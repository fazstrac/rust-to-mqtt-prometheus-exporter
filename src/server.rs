// `server.rs` composes the HTTP application: it loads initial state,
// registers Prometheus metrics, starts the MQTT background task, and
// mounts HTTP handlers and middleware.
use crate::{handlers, mqtt, state::{load_mappings, Store}};
use axum::{routing::{get, put}, Router, Extension};
use prometheus::{Registry, IntCounter};
use std::sync::Arc;
use tokio::task;
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::http::{Request, Method, HeaderValue, StatusCode};



pub async fn run() -> anyhow::Result<()> {
    let initial = load_mappings().await.unwrap_or_default();
    let store: Store = Arc::new(tokio::sync::RwLock::new(initial));

    let registry = Arc::new(Registry::new());
    let messages_counter = IntCounter::new("mqtt_messages_total", "Total MQTT messages received").unwrap();
    registry.register(Box::new(messages_counter.clone())).ok();

    let mqtt_counter = messages_counter.clone();
    task::spawn(async move {
        if let Err(e) = mqtt::start_mqtt_loop(mqtt_counter).await {
            eprintln!("MQTT task ended: {}", e);
        }
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
        .layer(middleware::from_fn(cors_middleware));

    let bind_addr = "0.0.0.0:3000";
    println!("listening on {}", bind_addr);

    let listener = tokio::net::TcpListener::bind(bind_addr).await?;
    axum::serve(listener, app).await?;

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
