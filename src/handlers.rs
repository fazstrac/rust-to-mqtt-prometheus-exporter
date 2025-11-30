// HTTP handlers for the service. These are thin wrappers around the shared
// `Store` and the Prometheus `Registry`. They intentionally do minimal
// validation to keep the example concise — add validation as needed.
use crate::state::{key_for, save_mappings, Mapping, Store};
use axum::{body::Body, extract::Extension, http::{HeaderMap, Request, StatusCode, header::CONTENT_TYPE, HeaderValue}, response::IntoResponse, Json};
use prometheus::{Encoder, Registry, TextEncoder};
use std::sync::Arc;

/// Return all mappings as JSON array. This performs a read-lock and clones the
/// values so the handler does not keep the lock across await points.
pub async fn list_mappings(Extension(store): Extension<Store>) -> Json<Vec<Mapping>> {
    let map = store.read().await;
    let vec = map.values().cloned().collect();
    Json(vec)
}

/// Insert or update a mapping. Expects a JSON body matching `Mapping`.
/// Returns `201 Created` on success. In a production service you'd validate
/// fields and possibly return `400 Bad Request` for invalid payloads.
pub async fn put_mapping(Extension(store): Extension<Store>, Json(payload): Json<Mapping>) -> Result<StatusCode, (StatusCode, String)> {
    let key = key_for(&payload.sensor_id, &payload.manufacturer);
    {
        let mut map = store.write().await;
        map.insert(key, payload);
    }
    // Persist immediately for this simple example. Consider batching in
    // high-throughput scenarios or moving persistence to a DB.
    save_mappings(&store).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::CREATED)
}

/// Expose Prometheus text-format metrics gathered from the provided
/// `Registry` extension. This returns the body and an (empty) header map so
/// the caller can set the appropriate `Content-Type` if needed.
pub async fn metrics_handler(Extension(registry): Extension<Arc<Registry>>) -> (HeaderMap, String) {
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap_or_default();
    let body = String::from_utf8_lossy(&buffer).to_string();
    (HeaderMap::new(), body)
}

/// Serve the built single-page app under `ui/dist`. The handler maps `/` to
/// `ui/dist/index.html` and otherwise attempts to read the requested file.
/// This is intentionally small — for production you might use a static file
/// server or embed assets in the binary.
pub async fn spa_handler(req: Request<Body>) -> impl IntoResponse {
    let path = req.uri().path();
    let rel = if path == "/" { "ui/dist/index.html".to_string() } else { format!("ui/dist{}", path) };

    match tokio::fs::read(&rel).await {
        Ok(bytes) => {
            let content_type = if rel.ends_with(".html") {
                "text/html; charset=utf-8"
            } else if rel.ends_with(".js") {
                "application/javascript; charset=utf-8"
            } else if rel.ends_with(".css") {
                "text/css; charset=utf-8"
            } else if rel.ends_with(".json") {
                "application/json; charset=utf-8"
            } else if rel.ends_with(".wasm") {
                "application/wasm"
            } else {
                "application/octet-stream"
            };
            let mut headers = HeaderMap::new();
            headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
            (headers, bytes).into_response()
        }
        Err(_) => (StatusCode::NOT_FOUND, "Not Found").into_response(),
    }
}
