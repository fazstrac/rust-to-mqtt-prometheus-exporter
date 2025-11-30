use duckdb::Connection;

mod mqtt_buffer;

fn main() -> duckdb::Result<()> {
    // Open (or create) DB and ensure target table exists.
    let conn = Connection::open("example.duckdb")?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS measurements(received_at BIGINT, model TEXT, id TEXT, battery_ok BOOLEAN, quantity_code INTEGER, measurement DOUBLE, raw JSON)",
        [],
    )?;

    // Example: accumulate two MQTT payloads (in real use you'd push from the MQTT callback)
    let mut buf = mqtt_buffer::MqttBuffer::new(64);
    let js1 = r#"{"time":"2025-11-29 22:00:39","model":"ESP32","id":"dev1","battery_ok":1,"temperature_C":21.5,"humidity":45.2}"#;
    let js2 = r#"{"time":"2025-11-29 22:05:39","model":"ESP32","id":"dev1","battery_ok":1,"pressure_kPa":101.3}"#;

    buf.push_json(js1).unwrap();
    buf.push_json(js2).unwrap();

    // When ready (size threshold, timer, or shutdown), flush to DuckDB
    buf.flush_to_duckdb(&conn, "measurements").unwrap();

    println!("Flushed {} rows into DuckDB", buf.len());
    Ok(())
}
