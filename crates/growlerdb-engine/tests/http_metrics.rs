//! REST **RED metrics** middleware: [`rest::track_http_metrics`] records a per-route
//! request counter (labelled by the matched route template + status) and a duration histogram, so
//! the console's Runtime "API …" panels and the Search "query status codes" panel have data. This
//! drives a request through a layered router and asserts the metrics render on `/metrics`.

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use axum::routing::{get, post};
use axum::Router;
use growlerdb_engine::rest;
use tower::ServiceExt;

#[tokio::test]
async fn middleware_records_route_template_and_status() {
    // Install the Prometheus recorder so `metrics_text()` renders what the middleware emits.
    growlerdb_telemetry::init("test");

    // A tiny router with the same layer main.rs applies to the merged `/v1/*` router.
    let app = Router::new()
        .route("/v1/search", post(|| async { "ok" }))
        .route(
            "/v1/boom",
            get(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
        )
        .layer(axum::middleware::from_fn(rest::track_http_metrics));

    // A 200 on the query endpoint…
    let ok = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("POST")
                .uri("/v1/search")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(ok.status(), StatusCode::OK);

    // …a 500 on another route…
    let err = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .uri("/v1/boom")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(err.status(), StatusCode::INTERNAL_SERVER_ERROR);

    // …and a 404 for an unmatched path, which must bucket as "<unmatched>" (bounded cardinality).
    let missing = app
        .oneshot(
            HttpRequest::builder()
                .uri("/nope")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(missing.status(), StatusCode::NOT_FOUND);

    let metrics = growlerdb_telemetry::metrics_text();
    // The counter is labelled by the route TEMPLATE (not the raw path) and the status code.
    assert!(
        metrics.contains("growlerdb_http_requests_total")
            && metrics.contains("route=\"/v1/search\""),
        "search request counted by route template:\n{metrics}"
    );
    assert!(metrics.contains("status=\"200\""), "status code labelled");
    assert!(metrics.contains("status=\"500\""), "server error labelled");
    assert!(
        metrics.contains("route=\"<unmatched>\""),
        "unmatched path bucketed, not labelled by raw URL:\n{metrics}"
    );
    // The duration histogram is emitted too (summary quantiles for the p95 panel).
    assert!(metrics.contains("growlerdb_http_request_duration_seconds"));
}
