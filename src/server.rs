// `server.rs` composes the HTTP application: it loads initial state,
// registers Prometheus metrics, starts the MQTT background task, and
// mounts HTTP handlers and middleware.
use crate::{handlers, mqtt, db, state::{load_mappings, Store}};
use axum::{routing::{get, put}, Router, Extension};
use prometheus::{Registry, IntCounter};
use std::sync::Arc;
use tokio::task;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::http::{Request, Method, HeaderValue, StatusCode};
use tokio::signal::unix::{signal, SignalKind};

// TODO
// - IDEA: reread config/mappings on SIGHUP?
// - Centralized database handler shared between MQTT task and HTTP handlers
// - Persist mappings to database

pub async fn run() -> anyhow::Result<()> {
    let initial = load_mappings().await.unwrap_or_default();
    let store: Store = Arc::new(tokio::sync::RwLock::new(initial));

    let registry = Arc::new(Registry::new());
    let mqtt_messages_received_counter = IntCounter::new("mqtt_messages_total", "Total MQTT messages received").unwrap();
    let mqtt_messages_not_flushed_to_db = IntCounter::new("mqtt_unflushed_total", "Total unflushed MQTT messages in WAL").unwrap();
    registry.register(Box::new(mqtt_messages_received_counter.clone())).ok();
    registry.register(Box::new(mqtt_messages_not_flushed_to_db.clone())).ok();



    // Start DB worker and pass handle into background tasks
    let mqtt_messages_not_flushed_to_db_handle = mqtt_messages_not_flushed_to_db.clone();
    let db_path = std::env::var("DUCKDB_PATH").ok();
    let (db_handle, _db_join) = db::start_db_worker(db_path, mqtt_messages_not_flushed_to_db_handle);

    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let shutdown_notify_task = shutdown_notify.clone();



    // Start MQTT background task
    let mqtt_messages_received_counter_task = mqtt_messages_received_counter.clone();
    let mqtt_messages_not_flushed_to_db_task = mqtt_messages_not_flushed_to_db.clone();
    let db_for_task = db_handle.clone();
    let mqtt_join = mqtt::start_mqtt_worker(
        mqtt_messages_received_counter_task, 
        mqtt_messages_not_flushed_to_db_task, 
        db_for_task, 
        shutdown_notify_task
    ).await.unwrap();

    // Spawn a task to handle Unix signals for graceful shutdown
    let shutdown_notify_task2 = shutdown_notify.clone();
    let signal_task = task::spawn(async move {
        let mut sighup = signal(SignalKind::hangup()).unwrap();
        let mut sigterm = signal(SignalKind::terminate()).unwrap();
        let mut sigint = signal(SignalKind::interrupt()).unwrap();

        let handle_shutdown = async |signal_name: String| {
            println!("Received {}, shutting down...", signal_name);
            // Notify MQTT task to shut down. It will flush and shut down the DB.
            shutdown_notify_task2.notify_waiters();

            println!("Waiting for MQTT task and DB thread to finish...");

            // REFACTOR: refactor http handlers and mqtt task to share db handle properly
            // also refactor http handler into its own module and create start_http_server function

            // Await MQTT task completion
            mqtt_join.await.unwrap_or_else(|e| {
                eprintln!("Error joining MQTT task on shutdown: {}", e);
            });

            db_handle.shutdown().await.unwrap_or_else(|e| {
                eprintln!("Error shutting down DB on shutdown: {}", e);
            });

            // Join DB thread
            _db_join.await.unwrap_or_else(|e| {
                eprintln!("Error joining DB thread on shutdown: {:?}", e);
            });

            println!("Shutdown complete.");            
        };

        // Handle signals for SIGHUP (checkpoint), SIGINT and SIGTERM (graceful shutdown)
        // Ugly and should be refactored to reduce duplication
        // As it is now, does affect 
        loop {
            tokio::select! {
                _ = sighup.recv() => {
                    println!("Received SIGHUP, CHECKPOINTING database...");

                    db_handle.flush().await.unwrap_or_else(|e| {
                        eprintln!("Error flushing DB on SIGHUP: {}", e);
                    });
                }
                _ = sigint.recv() => {
                    handle_shutdown("SIGINT".to_string()).await;
                    break;
                },
                _ = sigterm.recv() => {
                    handle_shutdown("SIGTERM".to_string()).await;
                    break;
                }
            }
        }

        println!("Signal handling task exiting cleanly.");
    });

    // Build the HTTP app. Layers are applied from bottom -> top: the
    // `Extension` layers provide shared state (Store and Registry) to
    // handlers. The CORS middleware is mounted last so it can ensure
    // headers are applied to all responses.
    let app = Router::new()
        .route("/mapping", put(handlers::put_mapping).get(handlers::list_mappings))
        .route("/metrics", get(handlers::metrics_handler))
        .route("/health", get(|| async { "ok" }))
        .fallback_service(get(handlers::spa_handler))
        .layer(Extension(store))
        .layer(Extension(registry))
        //.layer(Extension(db_handle))
        .layer(middleware::from_fn(cors_middleware));

    let bind_addr = "0.0.0.0:3000";
    println!("listening on {}", bind_addr);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    let server = axum::serve(listener, app);

    let shutdown_future = {
        let shutdown_notify_task3 = shutdown_notify.clone();
        async move {
            shutdown_notify_task3.notified().await;
            println!("HTTP server shutdown signal received.");
        }
    };

    server.with_graceful_shutdown(shutdown_future).await?;
    signal_task.await.unwrap();

    Ok(())
}

async fn cors_middleware(req: Request<axum::body::Body>, next: Next) -> Response {
    let allow_headers = HeaderValue::from_static("*");
    let allow_methods = HeaderValue::from_static("GET,PUT,POST,OPTIONS");
    let allow_origin = HeaderValue::from_static("*");

    if req.method() == &Method::OPTIONS {
        let mut res = Response::new(axum::body::Body::empty());
        *res.status_mut() = StatusCode::NO_CONTENT;
        let headers = res.headers_mut();
        // Common preflight response headers. For production, consider
        // restricting `allow_origin` to your frontend domain and only
        // allowing the specific headers you need.
        headers.insert("access-control-allow-origin", allow_origin.clone());
        headers.insert("access-control-allow-methods", allow_methods.clone());
        headers.insert("access-control-allow-headers", allow_headers.clone());
        return res;
    }

    let mut res = next.run(req).await;
    let headers = res.headers_mut();
    // Attach the same CORS headers to the normal responses so the browser
    // accepts the responses from the API.
    headers.insert("access-control-allow-origin", allow_origin);
    headers.insert("access-control-allow-methods", allow_methods);
    headers.insert("access-control-allow-headers", allow_headers);
    res
}
