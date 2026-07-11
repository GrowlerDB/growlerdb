//! Cross-process test of `growlerdb serve`: spawn the real binary, write a batch
//! through the Write gRPC service, and confirm it commits + is searchable.

use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexReader, LocatedDoc, ResolvedIndex,
    SearchParams, SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_proto::v1::write_client::WriteClient;
use growlerdb_proto::v1::{DocBatch, GetCheckpointRequest, WriteRequest};
use tonic::transport::Channel;

fn docs_index() -> ResolvedIndex {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("body", SourceType::String),
        ],
        vec![],
        vec!["id".into()],
    );
    IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: ALL }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap()
}

fn batch() -> CommitBatch {
    let doc = |id: &str, body: &str, row: u64| {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), Value::from(id));
        fields.insert("body".to_string(), Value::from(body));
        LocatedDoc {
            doc: Document::new(key, fields),
            iceberg_file: "data/f0.parquet".into(),
            row_position: row,
        }
    };
    CommitBatch::from_upserts(
        vec![doc("doc-1", "served over grpc", 0)],
        SourceCheckpoint::iceberg(1),
        "b1",
    )
}

/// A spawned `growlerdb serve` that is killed on drop.
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn growlerdb_serve_hosts_write_grpc() {
    let tmp = tempfile::tempdir().unwrap();
    let resolved = docs_index();

    // Define the index on disk (schema only) — `growlerdb serve` opens it.
    std::fs::create_dir_all(tmp.path().join("docs")).unwrap();
    std::fs::write(
        tmp.path().join("docs/index.json"),
        serde_json::to_vec(&resolved).unwrap(),
    )
    .unwrap();

    // Pick a free port, then spawn the real binary on it.
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");
    let server = Server(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                tmp.path().to_str().unwrap(),
                "serve",
                "docs",
                "--addr",
                &addr,
            ])
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn growlerdb serve"),
    );

    // Write a batch over gRPC → committed snapshot.
    let mut client = connect(&format!("http://{addr}")).await;
    let wire: DocBatch = batch().into();
    let snapshot = client
        .write(WriteRequest { batch: Some(wire) })
        .await
        .expect("write rpc")
        .into_inner()
        .snapshot;
    assert_eq!(snapshot, 1, "first commit → snapshot 1");

    // Idempotent: same batch_id → no new generation.
    let again = client
        .write(WriteRequest {
            batch: Some(batch().into()),
        })
        .await
        .expect("re-write")
        .into_inner()
        .snapshot;
    assert_eq!(again, 1);

    // The connector's resume point: GetCheckpoint reports the committed
    // checkpoint (Iceberg snapshot 1) + the current index snapshot, over gRPC.
    let cp = client
        .get_checkpoint(GetCheckpointRequest::default())
        .await
        .expect("get_checkpoint rpc")
        .into_inner();
    assert_eq!(cp.snapshot, 1);
    assert!(
        matches!(
            cp.checkpoint.and_then(|c| c.kind),
            Some(growlerdb_proto::v1::source_checkpoint::Kind::IcebergSnapshot(1))
        ),
        "checkpoint should be Iceberg snapshot 1",
    );

    // Search over gRPC: the just-written doc is queryable by body text,
    // returning its coordinates.
    let mut search =
        growlerdb_proto::v1::search_client::SearchClient::connect(format!("http://{addr}"))
            .await
            .expect("connect search");
    let resp = search
        .search(growlerdb_proto::v1::SearchRequest {
            query: "body:grpc".into(),
            limit: 10,
            offset: 0,
            sort: Vec::new(),
            search_after: Vec::new(),
            collapse: String::new(),
            pit_id: 0,
            ..Default::default()
        })
        .await
        .expect("search rpc")
        .into_inner();
    assert_eq!(resp.hits.len(), 1, "doc-1 searchable over the Search gRPC");
    let id = resp.hits[0]
        .coordinates
        .as_ref()
        .and_then(|c| c.identifier.iter().find(|f| f.name == "id"))
        .and_then(|f| f.value.clone())
        .and_then(|v| v.kind);
    assert_eq!(
        id,
        Some(growlerdb_proto::v1::value::Kind::Str("doc-1".to_string()))
    );

    // Stop the server (release the store lock), then confirm the data persisted
    // and is searchable on the shard the binary wrote to.
    drop(server);
    // Give the OS a moment to release the file lock.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let store = LocalIndexStore::open(tmp.path()).unwrap();
    let shard = store
        .open_shard(&ShardId::single("docs"), &resolved)
        .unwrap();
    let hits = IndexReader::search(&shard, &SearchParams::parse("body:grpc", 10).unwrap()).unwrap();
    assert_eq!(hits.total, 1);
    assert_eq!(
        hits.hits[0].key.get("id").unwrap().to_index_string(),
        "doc-1"
    );
}

async fn connect(url: &str) -> WriteClient<Channel> {
    for _ in 0..80 {
        if let Ok(client) = WriteClient::connect(url.to_string()).await {
            return client;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("growlerdb serve did not come up at {url}");
}
