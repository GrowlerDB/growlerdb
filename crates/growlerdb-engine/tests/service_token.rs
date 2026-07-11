//! End-to-end service-token gate: a control plane wrapped with the service-token interceptor
//! rejects an RPC with a missing/wrong token and accepts the matching one; an unconfigured control
//! plane stays open. Exercises the real gRPC path (interceptor on the server, the token-stamping
//! client) rather than the interceptor in isolation.

use growlerdb_engine::{intercept_service_token, ControlPlaneService};
use growlerdb_proto::service_token::connect;
use growlerdb_proto::v1::ListIndexesRequest;
use growlerdb_source::IcebergConfig;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

/// Spawn a control plane gated with `token` (or open when `None`); return its `http://` endpoint.
async fn spawn_cp(token: Option<&str>) -> String {
    let tmp = tempfile::tempdir().unwrap();
    let registry =
        Arc::new(growlerdb_controlplane::Registry::open(tmp.path().join("registry.json")).unwrap());
    // Keep the tempdir alive for the server's lifetime.
    std::mem::forget(tmp);
    let svc = ControlPlaneService::new(registry, IcebergConfig::local());
    let service = intercept_service_token(svc.into_server(), token.map(str::to_string));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(service)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    format!("http://{addr}")
}

async fn list(endpoint: &str, token: Option<&str>) -> Result<(), tonic::Status> {
    // Retry the initial connect so the test isn't racing the server's bind.
    let mut client = None;
    for _ in 0..50 {
        if let Ok(c) = connect(endpoint.to_string(), None, token).await {
            client = Some(c);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    client
        .expect("control plane never came up")
        .list_indexes(ListIndexesRequest {})
        .await
        .map(|_| ())
}

#[tokio::test(flavor = "multi_thread")]
async fn configured_token_rejects_missing_and_wrong_accepts_right() {
    let ep = spawn_cp(Some("s3cr3t")).await;

    // Missing token ⇒ unauthenticated.
    let err = list(&ep, None).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // Wrong token ⇒ unauthenticated.
    let err = list(&ep, Some("nope")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // Matching token ⇒ allowed.
    list(&ep, Some("s3cr3t"))
        .await
        .expect("right token accepted");
}

#[tokio::test(flavor = "multi_thread")]
async fn unconfigured_control_plane_is_open() {
    let ep = spawn_cp(None).await;
    // No token configured ⇒ a call with no token succeeds (bare local dev).
    list(&ep, None)
        .await
        .expect("open control plane accepts a tokenless call");
}
