//! **mTLS between internal services** (task-35 slice 5): stand up the Node's gRPC services on a
//! tonic server configured for mutual TLS, then prove a [`RemoteNode`] connects only when it
//! presents a client certificate signed by the cluster CA — an anonymous (no-client-cert) peer
//! is rejected at the handshake, before any RPC. Uses throwaway test certs under
//! `tests/testdata/tls/` (a CA, a `localhost` server cert, and a client cert).

use std::collections::BTreeMap;
use std::sync::Arc;

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_engine::{
    tls, AdminService, LookupService, RemoteNode, SearchService, SuggestService,
};
use growlerdb_index::{LocalIndexStore, Shard, ShardId};
use growlerdb_proto::v1::DescribeIndexRequest;
use growlerdb_source::IcebergConfig;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{ClientTlsConfig, Server};
use tonic::Request;

const CA: &[u8] = include_bytes!("testdata/tls/ca.crt");
const SERVER_CRT: &[u8] = include_bytes!("testdata/tls/server.crt");
const SERVER_KEY: &[u8] = include_bytes!("testdata/tls/server.key");
const CLIENT_CRT: &[u8] = include_bytes!("testdata/tls/client.crt");
const CLIENT_KEY: &[u8] = include_bytes!("testdata/tls/client.key");

fn shard(root: &std::path::Path) -> Arc<Shard> {
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
    let mut f = BTreeMap::new();
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

/// Spawn the Node services over `shard` with **mutual TLS** required; return the `https://…`
/// endpoint.
async fn spawn_mtls_node(shard: Arc<Shard>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let search = SearchService::new(shard.clone());
    let suggest = SuggestService::new(shard.clone());
    let lookup = LookupService::new(shard.clone(), IcebergConfig::local(), "g.docs");
    let admin = AdminService::new(shard, "docs");
    tokio::spawn(
        Server::builder()
            .tls_config(tls::server_mtls(SERVER_CRT, SERVER_KEY, CA))
            .unwrap()
            .add_service(search.into_server())
            .add_service(suggest.into_server())
            .add_service(lookup.into_server())
            .add_service(admin.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    format!("https://{addr}")
}

/// Try to connect with `tls` and describe the index; `Ok` only if both the handshake and the
/// RPC succeed. Retries the connect so the spawned server has time to bind.
async fn try_describe(endpoint: &str, tls: ClientTlsConfig) -> Result<(), String> {
    let mut last = String::new();
    for _ in 0..50 {
        match RemoteNode::connect_with_tls(endpoint.to_string(), tls.clone()).await {
            Ok(node) => {
                let gw = growlerdb_engine::Gateway::new(Arc::new(node));
                return gw
                    .describe_index(Request::new(DescribeIndexRequest {
                        window: 0,
                        index: String::new(),
                    }))
                    .await
                    .map(|_| ())
                    .map_err(|e| format!("rpc: {e}"));
            }
            Err(e) => {
                last = format!("connect: {e}");
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    Err(last)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_with_a_valid_client_cert_is_admitted() {
    let tmp = tempfile::tempdir().unwrap();
    let endpoint = spawn_mtls_node(shard(tmp.path())).await;

    let tls = tls::client_mtls(CA, CLIENT_CRT, CLIENT_KEY, "localhost");
    assert!(
        try_describe(&endpoint, tls).await.is_ok(),
        "a client presenting a CA-signed cert should be admitted over mTLS"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_peer_with_no_client_cert_is_rejected() {
    let tmp = tempfile::tempdir().unwrap();
    let endpoint = spawn_mtls_node(shard(tmp.path())).await;

    // Trusts the server CA but presents NO client identity — the server requires one, so the
    // mutual handshake fails (whether surfaced at connect or first RPC).
    let tls = ClientTlsConfig::new()
        .ca_certificate(tonic::transport::Certificate::from_pem(CA))
        .domain_name("localhost");
    assert!(
        try_describe(&endpoint, tls).await.is_err(),
        "a client with no certificate must be rejected by a mutual-TLS server"
    );
}
