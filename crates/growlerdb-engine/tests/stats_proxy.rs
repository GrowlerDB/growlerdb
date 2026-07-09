//! The **metrics proxy** (task-48): `/v1/stats/*` on the gateway forwards to a Prometheus-
//! compatible backend so the UI's SLI panels query same-origin (no CORS). Point `stats_router`
//! at a stub upstream and check the path + query string are forwarded and the body passes
//! through unchanged.

use axum::body::{to_bytes, Body};
use axum::extract::RawQuery;
use axum::http::{Request as HttpRequest, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use growlerdb_engine::rest;
use tokio::net::TcpListener;
use tower::ServiceExt;

/// A stub Prometheus that echoes the received query string in its JSON response.
async fn stub_prometheus() -> String {
    let app = Router::new()
        .route(
            "/api/v1/query_range",
            get(|RawQuery(q): RawQuery| async move {
                Json(serde_json::json!({
                    "status": "success",
                    "echo": q.unwrap_or_default(),
                    "data": { "resultType": "matrix", "result": [] },
                }))
            }),
        )
        .route(
            "/api/v1/alerts",
            get(|| async {
                Json(serde_json::json!({ "status": "success", "data": { "alerts": [] } }))
            }),
        );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// A stub Prometheus whose `/api/v1/alerts` returns a firing, a pending, and an inactive alert
/// in the real Prometheus shape — to exercise the `/v1/alerts` normalizer (task-111).
async fn stub_prometheus_alerts() -> String {
    let app = Router::new().route(
        "/api/v1/alerts",
        get(|| async {
            Json(serde_json::json!({
                "status": "success",
                "data": { "alerts": [
                    {
                        "labels": { "alertname": "HighQueryErrorRate", "severity": "critical" },
                        "annotations": { "summary": "Query error rate 0.12/s exceeds 0.05/s" },
                        "state": "firing",
                        "value": "0.12",
                    },
                    {
                        "labels": { "alertname": "HighQueryLatency", "severity": "warning" },
                        "annotations": { "description": "p99 latency above target" },
                        "state": "pending",
                    },
                    {
                        "labels": { "alertname": "StaleLocatorChurn", "severity": "warning" },
                        "annotations": { "summary": "churn" },
                        "state": "inactive",
                    },
                ] },
            }))
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

async fn text(resp: axum::response::Response) -> String {
    String::from_utf8(to_bytes(resp.into_body(), 1 << 20).await.unwrap().to_vec()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_range_forwards_path_and_query() {
    let base = stub_prometheus().await;
    let app = rest::stats_router(base);

    let req = HttpRequest::builder()
        .uri("/v1/stats/query_range?query=up&step=15s")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    // The upstream echoed our query string back → the proxy forwarded it verbatim.
    assert!(body.contains("\"status\":\"success\""));
    assert!(body.contains("query=up") && body.contains("step=15s"));

    // Alerts passthrough.
    let resp = app
        .oneshot(
            HttpRequest::builder()
                .uri("/v1/stats/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(text(resp).await.contains("\"alerts\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alerts_normalizes_firing_and_pending_only() {
    let base = stub_prometheus_alerts().await;
    let app = rest::stats_router(base);

    let resp = app
        .oneshot(
            HttpRequest::builder()
                .uri("/v1/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    let json: serde_json::Value = serde_json::from_str(&body).unwrap();
    let alerts = json["alerts"].as_array().unwrap();
    // The inactive alert is dropped; firing + pending survive.
    assert_eq!(alerts.len(), 2);

    let firing = &alerts[0];
    assert_eq!(firing["name"], "HighQueryErrorRate");
    assert_eq!(firing["severity"], "critical");
    assert_eq!(firing["state"], "firing");
    assert_eq!(firing["summary"], "Query error rate 0.12/s exceeds 0.05/s");
    assert_eq!(firing["value"], "0.12");

    // The pending alert falls back to `description` for its summary; no `value` field.
    let pending = &alerts[1];
    assert_eq!(pending["name"], "HighQueryLatency");
    assert_eq!(pending["state"], "pending");
    assert_eq!(pending["summary"], "p99 latency above target");
    assert!(pending.get("value").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn alerts_returns_502_when_backend_unreachable() {
    // No backend listening at this port → the proxy reports a bad gateway, so the console can
    // fall back to its local SLI checks.
    let app = rest::stats_router("http://127.0.0.1:1");
    let resp = app
        .oneshot(
            HttpRequest::builder()
                .uri("/v1/alerts")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
}
