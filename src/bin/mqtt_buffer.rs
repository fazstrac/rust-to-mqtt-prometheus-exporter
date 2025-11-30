use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use duckdb::arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema};
use duckdb::arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[allow(deprecated)]
fn parse_time(s: &str) -> Option<i64> {
    if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(DateTime::<Utc>::from_utc(ndt, Utc).timestamp_micros());
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc).timestamp_micros());
    }
    None
}

#[allow(dead_code)]
fn main() {
    println!("This is a library module; run the appender binary to use it.");
}

pub struct MqttBuffer {
    received_at: Vec<i64>,
    model: Vec<String>,
    id: Vec<String>,
    battery_ok: Vec<Option<bool>>,
    quantity_code: Vec<i64>,
    measurement: Vec<f64>,
    raw: Vec<String>,
}

impl MqttBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            received_at: Vec::with_capacity(capacity),
            model: Vec::with_capacity(capacity),
            id: Vec::with_capacity(capacity),
            battery_ok: Vec::with_capacity(capacity),
            quantity_code: Vec::with_capacity(capacity),
            measurement: Vec::with_capacity(capacity),
            raw: Vec::with_capacity(capacity),
        }
    }

    pub fn len(&self) -> usize {
        self.measurement.len()
    }

    pub fn is_empty(&self) -> bool {
        self.measurement.is_empty()
    }

    pub fn clear(&mut self) {
        self.received_at.clear();
        self.model.clear();
        self.id.clear();
        self.battery_ok.clear();
        self.quantity_code.clear();
        self.measurement.clear();
        self.raw.clear();
    }

    pub fn push_json(&mut self, raw_json: &str) -> Result<()> {
        let v: serde_json::Value = serde_json::from_str(raw_json)?;
        if let Some(obj) = v.as_object() {
            let received_at = obj
                .get("time")
                .and_then(|t| t.as_str())
                .and_then(|s| parse_time(s))
                .unwrap_or_else(|| Utc::now().timestamp_micros());

            let model = obj.get("model").and_then(|m| m.as_str()).unwrap_or("").to_string();
            let id = obj.get("id").map(|idv| idv.to_string()).unwrap_or_default();
            let battery_ok = obj.get("battery_ok").and_then(|b| {
                if b.is_boolean() {
                    b.as_bool()
                } else {
                    b.as_i64().map(|i| i != 0)
                }
            });

            let raw_s = raw_json.to_string();

            if let Some(temp) = obj.get("temperature_C").and_then(|t| t.as_f64()) {
                self.received_at.push(received_at);
                self.model.push(model.clone());
                self.id.push(id.clone());
                self.battery_ok.push(battery_ok);
                self.quantity_code.push(1);
                self.measurement.push(temp);
                self.raw.push(raw_s.clone());
            }
            if let Some(h) = obj.get("humidity").and_then(|t| t.as_f64()) {
                self.received_at.push(received_at);
                self.model.push(model.clone());
                self.id.push(id.clone());
                self.battery_ok.push(battery_ok);
                self.quantity_code.push(2);
                self.measurement.push(h);
                self.raw.push(raw_s.clone());
            }
            if let Some(p) = obj.get("pressure_kPa").and_then(|t| t.as_f64()) {
                self.received_at.push(received_at);
                self.model.push(model.clone());
                self.id.push(id.clone());
                self.battery_ok.push(battery_ok);
                self.quantity_code.push(3);
                self.measurement.push(p);
                self.raw.push(raw_s.clone());
            }
        }
        Ok(())
    }

    pub fn flush_to_duckdb(&mut self, conn: &duckdb::Connection, table: &str) -> Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        let ra = Int64Array::from(self.received_at.clone());
        let model_arr = StringArray::from(self.model.clone());
        let id_arr = StringArray::from(self.id.clone());
        let bools: Vec<Option<bool>> = self.battery_ok.clone();
        let bool_arr = BooleanArray::from(bools);
        let q_arr = Int64Array::from(self.quantity_code.clone());
        let meas_arr = Float64Array::from(self.measurement.clone());
        let raw_arr = StringArray::from(self.raw.clone());

        let schema = Arc::new(Schema::new(vec![
            Field::new("received_at", DataType::Int64, false),
            Field::new("model", DataType::Utf8, false),
            Field::new("id", DataType::Utf8, false),
            Field::new("battery_ok", DataType::Boolean, true),
            Field::new("quantity_code", DataType::Int64, false),
            Field::new("measurement", DataType::Float64, false),
            Field::new("raw", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(ra),
                Arc::new(model_arr),
                Arc::new(id_arr),
                Arc::new(bool_arr),
                Arc::new(q_arr),
                Arc::new(meas_arr),
                Arc::new(raw_arr),
            ],
        )?;

        let mut appender = conn.appender(table)?;
        appender.append_record_batch(batch)?;
        appender.flush()?;

        self.clear();
        Ok(())
    }
}
