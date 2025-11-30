// MQTT background task. This connects to the broker using `rumqttc` and
// subscribes to the configured topic namespace. For each incoming message
// we increment the provided `IntCounter` and print the event. In a real
// implementation you'd persist raw messages to DuckDB/DuckLake and perform
// structured parsing/validation.
use prometheus::IntCounter;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use tokio::time::{self, Duration};

use crate::mqtt_buffer;

/// Start a long-running MQTT loop. This function never returns unless an
/// unrecoverable error occurs. It is intended to be spawned with
/// `tokio::task::spawn` from `server::run()` so it runs in the background.
pub async fn start_mqtt_loop(counter_tot_msg: IntCounter, counter_unflushed_msg: IntCounter) -> anyhow::Result<()> {
    // Create MQTT options from environment variables. Check for host,
    // port, username, and password; use defaults if not provided.
    // Not all fields are required; we default to localhost:1883
    // with no authentication if env vars are missing.

    let mut mqttoptions: MqttOptions;

    // Read credentials from environment and set them if both present.
    // This keeps defaults simple (no auth) while enabling secure
    // deployments by setting the env vars.
    let mqtt_host = std::env::var("MQTT_HOST").ok();
    let mqtt_port = std::env::var("MQTT_PORT").ok();
    let mqtt_user = std::env::var("MQTT_USER").ok();
    let mqtt_pass = std::env::var("MQTT_PASS").ok();
    let mqtt_topic = std::env::var("MQTT_TOPIC").ok();

    match (mqtt_host, mqtt_port) {
        // No host or port: default to localhost:1883
        (None, None) => {
            mqttoptions = MqttOptions::new("rust_exporter_client", "localhost", 1883);
            println!("Connecting to MQTT broker at localhost:1883");
        }
        // Host and port provided, use both
        (Some(host), Some(port)) => {
            match port.trim().parse::<u16>() {
                Ok(p) => {
                    mqttoptions = MqttOptions::new("rust_exporter_client", &host, p);
                    println!("Connecting to MQTT broker at {}:{}", host, p);
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("Invalid MQTT_PORT value, expected a number, got: {}", e));
                }
            }
            println!("Connecting to MQTT broker at {}:{}", host, port);
        }
        // Only host provided, use default port 1883
        (Some(host), None) => {
            mqttoptions = MqttOptions::new("rust_exporter_client", &host, 1883);
            println!("Connecting to MQTT broker at {}:1883", host);
        }
        (None, Some(_)) => {
            return Err(anyhow::anyhow!("MQTT_HOST must be set if MQTT_PORT is provided"));
        }
    }

    mqttoptions.set_keep_alive(std::time::Duration::from_secs(5));

    match (mqtt_user, mqtt_pass) {
        (Some(user), Some(pass)) => {            
            mqttoptions.set_credentials(&user, &pass);
            println!("Using MQTT credentials from environment {}:*******", user);
        }
        (Some(_), None) | (None, Some(_)) => {
            // Warn but continue without credentials if only one is set.
            eprintln!("MQTT credentials incomplete: both MQTT_USER and MQTT_PASS must be set to enable auth");
        }
        (None, None) => {
            // No credentials configured; proceed unauthenticated.
            println!("No MQTT credentials provided; connecting without authentication");
        }
    }

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    match mqtt_topic {
        Some(topic) => {
            client.subscribe(&topic, QoS::AtLeastOnce).await?;
            println!("Subscribing to MQTT topic: {}", topic);
        }
        None => {
            return Err(anyhow::anyhow!("MQTT_TOPIC environment variable must be set to subscribe to topics"));
        }
    }

    let mut all_rows: Vec<mqtt_buffer::NormalizedRow> = Vec::new();
    let conn = duckdb::Connection::open("measurements.db").unwrap();

    mqtt_buffer::create_table(&conn, "measurements").unwrap();

    // Timer for periodic flush and checkpoint
    // Use prime numbers to avoid alignment with other periodic tasks
    let mut interval_flush = time::interval(Duration::from_secs(113));

    loop {
        tokio::select! {
            // General idea:
            // Handle incoming MQTT messages and process them
            // Flush to DuckDB periodically or based on message count if there is a burst
            // Checkpoint DuckDB periodically to ensure data is persisted

            // MQTT event
            ev = eventloop.poll() => {
                match ev {
                    // increase unflushed count and store normalized rows
                    // on receiving a publish
                    // If unflushed count exceeds threshold, flush to DuckDB
                    // that happens most likely during bursts of messages (over 500 msgs per 113 seconds)
                    Ok(Event::Incoming(Incoming::Publish(p))) => {                
                        counter_tot_msg.inc();
                        counter_unflushed_msg.inc();
                        println!("Got topic: {}, Count: {}, Unflushed: {}", p.topic, counter_tot_msg.get(), counter_unflushed_msg.get());

                        let payload_str = String::from_utf8_lossy(&p.payload);
                        let rows = mqtt_buffer::normalize_one_message(&payload_str);
                        all_rows.extend(rows);

                        // check if we should flush to DuckDB
                        if counter_unflushed_msg.get() >= 500 {
                            // Every 500 hits, flush to DuckDB
                            match mqtt_buffer::flush_to_duckdb(all_rows.clone(), &conn, "measurements") {
                                Ok(_) => {
                                    println!("Flushed {} rows to DuckDB", all_rows.len());
                                    all_rows.clear();
                                }
                                Err(e) => {
                                    eprintln!("Error flushing to DuckDB: {}", e);
                                }
                            }
                            counter_unflushed_msg.reset();
                        }
                    }
                    Ok(Event::Incoming(i)) => {
                        println!("Incoming = {i:?}");
                    }
                    Ok(Event::Outgoing(o)) => {
                        println!("Outgoing = {o:?}");
                    }
                    Err(e) => {
                        // Back off on errors to avoid busy loops.
                        eprintln!("mqtt loop error: {}", e);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                    }
                }
            }
            // Timer tick
            _ = interval_flush.tick() => {
                // Periodic flush and checkpoint to DuckDB
                if !all_rows.is_empty() {
                    match mqtt_buffer::flush_to_duckdb(all_rows.clone(), &conn, "measurements") {
                        Ok(_) => {
                            println!("Periodic flush: Flushed {} rows to DuckDB", all_rows.len());
                            all_rows.clear();
                            counter_unflushed_msg.reset();
                        }
                        Err(e) => {
                            eprintln!("Error during periodic flush to DuckDB: {}", e);
                        }
                    }

                    conn.execute("CHECKPOINT;", []).unwrap();
                } 
            }
        }
    }
}
