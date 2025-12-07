// Database interaction module

use duckdb::arrow::record_batch::RecordBatch;
use crossbeam_channel::{unbounded, Sender, Receiver};
use tokio::sync::oneshot;
use tokio::task;
use tokio::task::JoinHandle;
use duckdb::Connection;
use anyhow::Result;
use prometheus::IntCounter;


pub enum DbCommand {
    Query(String),
    InsertBatch(RecordBatch, String),
    Flush,
    Shutdown
}

struct DbJob {
    command: DbCommand,
    response: tokio::sync::oneshot::Sender<anyhow::Result<DbResponse>>,
}

pub enum DbResponse {
    QueryResult,
    InsertResult,
    FlushResult,
    ShutdownResult,
}

#[derive(Clone)]
pub struct DbHandle {
    tx: Sender<DbJob>,
}

impl DbHandle {
    fn new(tx: Sender<DbJob>) -> Self {
        DbHandle { tx }
    }

    pub async fn insert_batch(&self, batch: RecordBatch, table: &str) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let job = DbJob {
            command: DbCommand::InsertBatch(batch, table.to_string()),
            response: tx,
        };
        self.tx.send(job).map_err(|e| anyhow::anyhow!("DB job send error: {}", e))?;
        rx.await.map_err(|e| anyhow::anyhow!("DB job response error: {}", e))??;
        Ok(())
    }

    pub async fn query(&self, query: String) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let job = DbJob {
            command: DbCommand::Query(query),
            response: tx,
        };
        self.tx.send(job).map_err(|e| anyhow::anyhow!("DB job send error: {}", e))?;
        rx.await.map_err(|e| anyhow::anyhow!("DB job response error: {}", e))??;
        Ok(())
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let job = DbJob {
            command: DbCommand::Flush,
            response: tx,
        };
        self.tx.send(job).map_err(|e| anyhow::anyhow!("DB job send error: {}", e))?;
        rx.await.map_err(|e| anyhow::anyhow!("DB job response error: {}", e))??;
        Ok(())
    }

    pub async fn shutdown(&self) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        let job = DbJob {
            command: DbCommand::Shutdown,
            response: tx,
        };
        self.tx.send(job).map_err(|e| anyhow::anyhow!("DB job send error: {}", e))?;
        
        match rx.await.map_err(|e| anyhow::anyhow!("DB job response error: {}", e))?? {
            DbResponse::ShutdownResult => Ok(()),
            _ => Ok(()),
        }
    }
}

/// Start the DB worker thread which owns a DuckDB connection and executes jobs.
/// If `path` is `Some`, opens that file, otherwise uses an in-memory DB.
pub fn start_db_worker(path: Option<String>, mqtt_messages_not_flushed_to_db: IntCounter) -> (DbHandle, JoinHandle<()>) {
    let (tx, rx): (Sender<DbJob>, Receiver<DbJob>) = unbounded();
    let handle = DbHandle::new(tx.clone());

    // Spawn a blocking thread that owns the DuckDB connection.
    // TODO: Handle connection errors more gracefully - currently panics on failure which is not OK
    let join = task::spawn_blocking(move || {
        let conn = match path.as_deref() {
            Some(p) => Connection::open(p).expect("open duckdb file"),
            None => Connection::open_in_memory().expect("open in-memory duckdb"),
        };

        while let Ok(job) = rx.recv() {
            match job.command {
                DbCommand::InsertBatch(batch, table) => {
                    let res: Result<()> = (|| {
                        let mut appender = conn.appender(&table)?;                        
                        appender.append_record_batch(batch)?;
                        appender.flush()?;                        
                        Ok(())
                    })();
                    let _ = job.response.send(res.map(|_| DbResponse::InsertResult));
                }
                DbCommand::Query(sql) => {
                    let res = conn.execute(&sql, []);
                    let _ = job.response.send(res.map(|_| DbResponse::QueryResult).map_err(|e| anyhow::anyhow!(e)));
                }
                DbCommand::Flush => {
                    let res = conn.execute("CHECKPOINT", []);
                    let _ = job.response.send(res.map(|_| DbResponse::FlushResult).map_err(|e| anyhow::anyhow!(e)));
                    // Reset the unflushed messages counter after a successful flush
                    mqtt_messages_not_flushed_to_db.reset();
                }
                DbCommand::Shutdown => {
                    // Perform any necessary cleanup here
                    // This skips retrying on close errors - ugly                
                    let res = conn.close();
                    let _ = job.response.send(res.map(|_| DbResponse::ShutdownResult).map_err(|e| anyhow::anyhow!(e.1)));
                    break; // Exit the loop to terminate the thread
                }
            }
        }
    });

    (handle.clone(), join)
}


#[cfg(test)]
mod tests {
    use super::*;
    use crossbeam_channel::unbounded;
    use std::thread;
    use std::sync::Arc;
    use duckdb::arrow::array::Int32Array;
    use duckdb::arrow::datatypes::{Field, Schema, DataType};

    fn make_dummy_batch() -> RecordBatch {
        let a = Int32Array::from(vec![1i32, 2, 3]);
        let field = Field::new("v", DataType::Int32, false);
        let schema = Schema::new(vec![field]);
        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)]).unwrap()
    }

    #[tokio::test]
    async fn test_insert_batch_roundtrip() {
        let (tx, rx) = unbounded::<DbJob>();
        let handle = DbHandle::new(tx.clone());

        // Spawn a mock worker thread that receives one job and replies OK
        thread::spawn(move || {
            if let Ok(job) = rx.recv() {
                match job.command {
                    DbCommand::InsertBatch(_batch, _table) => {
                        let _ = job.response.send(Ok(DbResponse::InsertResult));
                    }
                    _ => {
                        let _ = job.response.send(Ok(DbResponse::QueryResult));
                    }
                }
            }
        });

        let batch = make_dummy_batch();
        let res = handle.insert_batch(batch, "test_table").await;
        assert!(res.is_ok(), "insert_batch should succeed");
    }

    #[tokio::test]
    async fn test_query_roundtrip() {
        let (tx, rx) = unbounded::<DbJob>();
        let handle = DbHandle::new(tx.clone());

        thread::spawn(move || {
            if let Ok(job) = rx.recv() {
                match job.command {
                    DbCommand::Query(_q) => {
                        let _ = job.response.send(Ok(DbResponse::QueryResult));
                    }
                    _ => {
                        let _ = job.response.send(Ok(DbResponse::QueryResult));
                    }
                }
            }
        });

        let res = handle.query("SELECT 1".to_string()).await;
        assert!(res.is_ok(), "query should succeed");
    }

    #[tokio::test]
    async fn test_flush_roundtrip() {
        let (tx, rx) = unbounded::<DbJob>();
        let handle = DbHandle::new(tx.clone());

        thread::spawn(move || {
            if let Ok(job) = rx.recv() {
                let _ = job.response.send(Ok(DbResponse::FlushResult));
            }
        });

        let res = handle.flush().await;
        assert!(res.is_ok(), "flush should succeed");
    }
}