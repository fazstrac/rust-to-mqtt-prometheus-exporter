use anyhow::Result;
use chrono::{NaiveDateTime, TimeZone, Local, Utc};
use duckdb::arrow::array::{TimestampMicrosecondArray, Float64Array, UInt32Array, StringArray};
use duckdb::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use duckdb::arrow::record_batch::RecordBatch;
use std::sync::Arc;

use serde::Deserialize;

const MEASUREMENT_KEYS: &[&str] = &["temperature_C", "humidity", "pressure_kPa", "battery_ok"];

#[derive(Debug, Deserialize)]
struct RawMessage {
    #[serde(default)]
    id: serde_json::Value,
    #[serde(default)]
    model: serde_json::Value,
    #[serde(flatten)]
    measurements: serde_json::Value, // catch-all for dynamic fields
}

#[derive(Debug)]
enum MeasurementType {
}

impl MeasurementType {
    fn from_key(key: &str) -> u8 {
        match key {
            "temperature_C" => 0,
            "humidity" => 1,
            "pressure_kPa" => 2,
            "battery_ok" => 3,
            _ => 255, // unknown
        }
    }
}

#[derive(Debug, Clone)]
pub struct NormalizedRow {
    // microseconds since epoch; suitable for Arrow Timestamp(Microsecond)
    timestamp: i64,
    sensor_id: String,
    model: String,
    measurement_type: u8,
    value: f32,
    raw_json: Option<String>,
}

pub fn normalize_one_message(json_str: &str) -> Vec<NormalizedRow> {
    let raw: RawMessage = serde_json::from_str(json_str).unwrap();
    let mut rows = Vec::new();
    let ts = parse_time(raw.measurements.get("time"));

    // normalize id and model into strings
    let sensor_id = match &raw.id {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    let model = match &raw.model {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };

    if let Some(obj) = raw.measurements.as_object() {
        for (key, val) in obj {
            if let Some(num) = val.as_f64() {
                if MEASUREMENT_KEYS.contains(&key.as_str()) {
                    rows.push(NormalizedRow {
                        timestamp: ts,
                        sensor_id: sensor_id.clone(),
                        model: model.clone(),
                        measurement_type: MeasurementType::from_key(key),
                        value: num as f32,
                        raw_json: Some(raw.measurements.to_string()),
                    });                    
                }
            }
        }
    }
    rows
}

pub fn flush_to_duckdb(rows: Vec<NormalizedRow>, conn: &duckdb::Connection, table: &str) -> Result<()> {
    let batch = create_arrow_record_batch(&rows)?;

    let mut appender = conn.appender(table)?;
    appender.append_record_batch(batch)?;
    appender.flush()?;

    Ok(())
}

pub fn create_table(conn: &duckdb::Connection, table: &str) -> Result<()> {
    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {} (
            timestamp TIMESTAMP,
            model VARCHAR,
            sensor_id VARCHAR,
            measurement_type INTEGER,
            value DOUBLE,
            raw_json VARCHAR
        )",
        table
    );

    conn.execute(&create_table_sql, [])?;
    Ok(())
}


fn parse_time(val_opt: Option<&serde_json::Value>) -> i64 {
    if let Some(v) = val_opt {
        if let Some(s) = v.as_str() {
            if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                if let Some(local_dt) = Local.from_local_datetime(&ndt).single() {
                    return local_dt.with_timezone(&Utc).timestamp_micros();
                }
            }
        }
    }
    Utc::now().timestamp_micros()
}


fn create_arrow_record_batch(rows: &[NormalizedRow]) -> Result<RecordBatch> {
    let ra = TimestampMicrosecondArray::from(rows.iter().map(|r| r.timestamp).collect::<Vec<i64>>());
    let model_arr = StringArray::from(rows.iter().map(|r| r.model.clone()).collect::<Vec<String>>());
    let id_arr = StringArray::from(rows.iter().map(|r| r.sensor_id.clone()).collect::<Vec<String>>());
    let q_arr = UInt32Array::from(rows.iter().map(|r| r.measurement_type as u32).collect::<Vec<u32>>());
    let meas_arr = Float64Array::from(rows.iter().map(|r| r.value as f64).collect::<Vec<f64>>());
    let raw_arr = StringArray::from(
        rows.iter()
            .map(|r| r.raw_json.clone().unwrap_or_default())
            .collect::<Vec<String>>(),
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None), false),
        Field::new("model", DataType::Utf8, false),
        Field::new("sensor_id", DataType::Utf8, false),
        Field::new("measurement_type", DataType::UInt32, false),
        Field::new("value", DataType::Float64, false),
        Field::new("raw_json", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ra),
            Arc::new(model_arr),
            Arc::new(id_arr),
            Arc::new(q_arr),
            Arc::new(meas_arr),
            Arc::new(raw_arr),
        ],
    )?;

    Ok(batch)
}


//   TESTS
//

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_JSON: &str = r#"[
    {
        "time": "2025-11-29 22:00:39",
        "model": "LaCrosse-TX29IT",
        "id": 19,
        "battery_ok": 1,
        "newbattery": 0,
        "temperature_C": 20.9,
        "mic": "CRC"
    },
    {
        "time": "2025-11-29 22:00:59",
        "model": "LaCrosse-TX141Bv3",
        "id": 246,
        "channel": 1,
        "battery_ok": 1,
        "temperature_C": 22.0,
        "test": "No"
    },
    {
        "time": "2025-11-29 22:02:28",
        "model": "Toyota",
        "type": "TPMS",
        "id": "f1c7e267",
        "status": 128,
        "pressure_kPa": 246.48758,
        "temperature_C": 6.0,
        "mic": "CRC"
    },
    {
        "time": "2025-11-29 22:02:33",
        "model": "Ambientweather-F007TH",
        "id": 141,
        "channel": 1,
        "battery_ok": 0,
        "temperature_C": 18.72223,
        "humidity": 40,
        "mic": "CRC"
    }  
    ]"#;

    #[test]
    fn test_normalize_message() {
        let v: serde_json::Value = serde_json::from_str(TEST_JSON).expect("parse test json");
        let arr = v.as_array().expect("expected json array");

        let mut all_rows = Vec::new();
        for item in arr {
            let s = serde_json::to_string(item).unwrap();
            let rows = normalize_one_message(&s);
            all_rows.extend(rows);
        }

        // Expected rows: temperatures in 4 messages, humidity 1, pressure 1, battery_ok 3 => total 9
        assert_eq!(all_rows.len(), 9, "expected 9 normalized rows");

        // Count measurement types
        let mut temps = 0usize;
        let mut hum = 0usize;
        let mut pres = 0usize;
        let mut batt = 0usize;
        for r in &all_rows {
            match r.measurement_type {
                0 => temps += 1,
                1 => hum += 1,
                2 => pres += 1,
                3 => batt += 1,
                _ => {}
            }
        }
        assert_eq!(temps, 4, "expected 4 temperature measurements");
        assert_eq!(hum, 1, "expected 1 humidity measurement");
        assert_eq!(pres, 1, "expected 1 pressure measurement");
        assert_eq!(batt, 3, "expected 3 battery_ok measurements");
    }

    #[test]
    fn test_create_arrow_record_batch() {
        let v: serde_json::Value = serde_json::from_str(TEST_JSON).expect("parse test json");
        let arr = v.as_array().expect("expected json array");

        let mut all_rows = Vec::new();
        for item in arr {
            let s = serde_json::to_string(item).unwrap();
            let rows = normalize_one_message(&s);
            all_rows.extend(rows);
        }

        let batch = create_arrow_record_batch(&all_rows).expect("create arrow record batch");

        assert_eq!(batch.num_rows(), all_rows.len(), "record batch row count");
        assert_eq!(batch.num_columns(), 6, "record batch column count");
    }
}
