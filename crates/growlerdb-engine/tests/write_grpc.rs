//! Round-trips a write batch through the Node `Write` gRPC service over a local
//! socket and asserts the committed snapshot + that the data is searchable.

use std::collections::BTreeMap;
use std::sync::Arc;

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexReader, LocatedDoc, ResolvedIndex,
    SearchParams, SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_engine::WriteService;
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_proto::v1::write_client::WriteClient;
use growlerdb_proto::v1::{DocBatch, WriteRequest};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};

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
        vec![
            doc("doc-1", "hello over the wire", 0),
            doc("doc-2", "grpc write path", 1),
        ],
        SourceCheckpoint::iceberg(7),
        "b1",
    )
}

#[tokio::test]
async fn write_batch_round_trips_over_grpc() {
    let tmp = tempfile::tempdir().unwrap();
    let store = LocalIndexStore::open(tmp.path()).unwrap();
    let shard = Arc::new(
        store
            .create_shard(&ShardId::single("docs"), &docs_index())
            .unwrap(),
    );

    // Serve the Write service over an ephemeral socket.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let service = WriteService::new(shard.clone(), "docs", 8);
    tokio::spawn(async move {
        Server::builder()
            .add_service(service.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });

    let mut client = connect(&format!("http://{addr}")).await;

    // Client → wire: a CommitBatch becomes a DocBatch.
    let wire: DocBatch = batch().into();
    let resp = client
        .write(WriteRequest { batch: Some(wire) })
        .await
        .expect("write rpc")
        .into_inner();
    assert_eq!(resp.snapshot, 1, "first commit → snapshot 1");

    // The shard now serves the written docs.
    let hits =
        IndexReader::search(&*shard, &SearchParams::parse("body:grpc", 10).unwrap()).unwrap();
    assert_eq!(hits.total, 1);
    assert_eq!(
        hits.hits[0].key.get("id").unwrap().to_index_string(),
        "doc-2"
    );

    // Idempotent: re-writing the same batch_id is a no-op, same snapshot.
    let again = client
        .write(WriteRequest {
            batch: Some(batch().into()),
        })
        .await
        .expect("re-write")
        .into_inner();
    assert_eq!(again.snapshot, 1, "same batch_id → no new generation");
}

async fn connect(url: &str) -> WriteClient<Channel> {
    for _ in 0..40 {
        if let Ok(client) = WriteClient::connect(url.to_string()).await {
            return client;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("gRPC server did not come up");
}
