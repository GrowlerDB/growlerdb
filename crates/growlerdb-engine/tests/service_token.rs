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

// ---- Node data plane -------------------------------------------------------------------------
//
// The Node's whole gRPC surface (here: Search + Admin, representative of the layered server the
// CLI serves) is gated by `service_token_layer`, and `RemoteNode` stamps the token per request —
// the two halves the distributed mesh relies on. Without the layer a directly-reachable Node port
// bypassed the Gateway's authn/RBAC entirely.

fn data_plane_shard(root: &std::path::Path) -> Arc<growlerdb_index::Shard> {
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
        SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
    };
    use growlerdb_index::{LocalIndexStore, ShardId};
    let src = SourceSchema::new(
        vec![SourceField::new("id", SourceType::String)],
        vec![],
        vec!["id".into()],
    );
    let idx = IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap();
    let shard = LocalIndexStore::open(root)
        .unwrap()
        .create_shard(&ShardId::single("docs"), &idx)
        .unwrap();
    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("1"))]);
    let mut f = std::collections::BTreeMap::new();
    f.insert("id".to_string(), Value::from("1"));
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();
    Arc::new(shard)
}

/// Spawn a Node data plane (Search + Admin over a real shard) behind
/// [`service_token_layer`](growlerdb_engine::service_token_layer); return its endpoint.
async fn spawn_node(token: Option<&str>) -> String {
    use growlerdb_engine::{AdminService, SearchService};
    let tmp = tempfile::tempdir().unwrap();
    let shard = data_plane_shard(tmp.path());
    std::mem::forget(tmp);
    let search = SearchService::new(shard.clone());
    let admin = AdminService::new(shard.clone(), "docs");
    let write = growlerdb_engine::WriteService::new(shard, "docs", 4);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .layer(growlerdb_engine::service_token_layer(
                token.map(str::to_string),
            ))
            .add_service(search.into_server())
            .add_service(admin.into_server())
            .add_service(write.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    format!("http://{addr}")
}

/// A raw `Write.GetCheckpoint` against the layered Node, with/without the token — Write is the
/// data-plane RPC whose exposure mattered most (an open Node accepted arbitrary writes), and the
/// connector's `WriteClient` stamps the same header this test does.
async fn write_checkpoint(endpoint: &str, token: Option<&str>) -> Result<(), tonic::Status> {
    use growlerdb_proto::v1::write_client::WriteClient;
    let mut client = None;
    for _ in 0..50 {
        if let Ok(channel) = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .unwrap()
            .connect()
            .await
        {
            client = Some(WriteClient::with_interceptor(
                channel,
                growlerdb_proto::service_token::ServiceTokenInterceptor::new(token),
            ));
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    client
        .expect("node never came up")
        .get_checkpoint(growlerdb_proto::v1::GetCheckpointRequest { window: 0 })
        .await
        .map(|_| ())
}

async fn describe(
    endpoint: &str,
    token: Option<&str>,
) -> Result<growlerdb_proto::v1::DescribeIndexResponse, tonic::Status> {
    // Retry the initial connect so the test isn't racing the server's bind.
    let mut node = None;
    for _ in 0..50 {
        if let Ok(n) = growlerdb_engine::RemoteNode::connect(endpoint.to_string(), token).await {
            node = Some(n);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    use growlerdb_engine::Node;
    node.expect("node never came up")
        .describe_index(tonic::Request::new(
            growlerdb_proto::v1::DescribeIndexRequest {
                index: "docs".into(),
                window: 0,
            },
        ))
        .await
        .map(tonic::Response::into_inner)
}

#[tokio::test(flavor = "multi_thread")]
async fn node_data_plane_enforces_the_token_and_remote_node_stamps_it() {
    let ep = spawn_node(Some("mesh")).await;

    // Missing / wrong token ⇒ unauthenticated, on every service behind the layer.
    let err = describe(&ep, None).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    let err = describe(&ep, Some("nope")).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);

    // Matching token ⇒ the same RPC succeeds (RemoteNode stamped it on the request).
    let resp = describe(&ep, Some("mesh")).await.expect("token accepted");
    assert_eq!(resp.stats.expect("stats present").num_docs, 1);

    // The Write service sits behind the SAME layer: tokenless ⇒ unauthenticated, right token ⇒
    // allowed — the auth-denied coverage for Write the endpoint audit flagged as missing.
    let err = write_checkpoint(&ep, None).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
    write_checkpoint(&ep, Some("mesh"))
        .await
        .expect("the tokened write client reaches Write");
}

#[tokio::test(flavor = "multi_thread")]
async fn unconfigured_node_data_plane_is_open() {
    let ep = spawn_node(None).await;
    // No token configured ⇒ single-node dev stays open.
    describe(&ep, None)
        .await
        .expect("open node accepts a tokenless call");
}
