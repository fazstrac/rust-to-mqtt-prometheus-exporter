// `main.rs` is intentionally tiny: it only declares modules and delegates
// execution to `server::run()`. The real implementation lives in the
// `server`, `state`, `handlers`, and `mqtt` modules under `src/` so each
// responsibility is isolated and easier to navigate / test.
mod state;
mod handlers;
mod mqtt;
mod mqtt_buffer;
mod db;
mod server;

/// Start the service. Keep `main` minimal so hot-reloads, tests, and
/// integration points can import `server::run()` directly if needed.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    server::run().await
}
