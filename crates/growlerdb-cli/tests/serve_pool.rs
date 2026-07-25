//! Cross-process test of `growlerdb serve-pool` (D52): spawn the real binary serving **two** windowed
//! indexes from one process, and confirm a search dispatches per `(index, window)` to the right
//! index's shard — the multi-index-per-node property that kills the node-per-index wall.

use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc, ResolvedIndex,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_proto::v1::search_client::SearchClient;
use growlerdb_proto::v1::SearchRequest;
use tonic::transport::Channel;

/// A minimal windowed index (`id` KEYWORD + a `ts` window field) named `index`.
fn windowed_index(index: &str) -> ResolvedIndex {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("ts", SourceType::Long),
        ],
        vec![],
        vec!["id".into()],
    );
    IndexDefinition::from_yaml(&format!(
        "name: {index}\nsource: {{ iceberg: {{ catalog: g, table: g.{index} }} }}\nwindowing: {{ field: ts, granularity: daily }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD, fast: true }}, {{ path: ts, format: epoch_ms, fast: true }} ] }}\n",
    ))
    .unwrap()
    .resolve(&src)
    .unwrap()
}

/// Define `index` on disk (its `index.json`) and build one window shard (`w10`) holding a single doc
/// whose `id` is `only` — the pre-built windowed index a pool node serves.
fn build_windowed_index(data_dir: &std::path::Path, index: &str, only: &str) {
    let resolved = windowed_index(index);
    std::fs::create_dir_all(data_dir.join(index)).unwrap();
    std::fs::write(
        data_dir.join(index).join("index.json"),
        serde_json::to_vec(&resolved).unwrap(),
    )
    .unwrap();
    let store = LocalIndexStore::open(data_dir).unwrap();
    let shard = store
        .create_shard(&ShardId::window(index, 10), &resolved)
        .unwrap();
    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(only))]);
    let mut fields = BTreeMap::new();
    fields.insert("id".to_string(), Value::from(only));
    fields.insert("ts".to_string(), Value::from(10_i64));
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![LocatedDoc {
                doc: Document::new(key, fields),
                iceberg_file: "f0".into(),
                row_position: 0,
            }],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();
}

/// A spawned `growlerdb` process that is killed on drop.
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

async fn connect(url: &str) -> SearchClient<Channel> {
    for _ in 0..120 {
        if let Ok(client) = SearchClient::connect(url.to_string()).await {
            return client;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("growlerdb serve-pool did not come up at {url}");
}

/// Search `(index, window)` and return the matched `id`s.
async fn search_ids(client: &mut SearchClient<Channel>, index: &str, window: i64) -> Vec<String> {
    let resp = client
        .search(SearchRequest {
            query: "*".into(),
            limit: 10,
            index: index.into(),
            window,
            ..Default::default()
        })
        .await
        .expect("search rpc")
        .into_inner();
    resp.hits
        .iter()
        .filter_map(|h| {
            h.coordinates
                .as_ref()
                .and_then(|c| c.identifier.iter().find(|f| f.name == "id"))
                .and_then(|f| f.value.clone())
                .and_then(|v| match v.kind {
                    Some(growlerdb_proto::v1::value::Kind::Str(s)) => Some(s),
                    _ => None,
                })
        })
        .collect()
}

#[tokio::test]
async fn serve_pool_dispatches_search_across_two_indexes() {
    let tmp = tempfile::tempdir().unwrap();
    // Two independent windowed indexes, each pre-built on disk with its own window-10 doc.
    build_windowed_index(tmp.path(), "alpha", "alphadoc");
    build_windowed_index(tmp.path(), "beta", "betadoc");

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");
    // One process, both indexes — the node-per-index wall removed.
    let _server = Server(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                tmp.path().to_str().unwrap(),
                "serve-pool",
                "--index",
                "alpha",
                "--index",
                "beta",
                "--addr",
                &addr,
            ])
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn growlerdb serve-pool"),
    );

    let mut client = connect(&format!("http://{addr}")).await;
    // Each (index, window) reaches exactly its own index's shard on the one node.
    assert_eq!(
        search_ids(&mut client, "alpha", 10).await,
        vec!["alphadoc"],
        "alpha/w10 → alpha's doc"
    );
    assert_eq!(
        search_ids(&mut client, "beta", 10).await,
        vec!["betadoc"],
        "beta/w10 → beta's doc"
    );
    // An index this node doesn't serve is a loud InvalidArgument (not a silent empty result).
    let unserved = client
        .search(SearchRequest {
            query: "*".into(),
            limit: 10,
            index: "gamma".into(),
            window: 10,
            ..Default::default()
        })
        .await;
    assert_eq!(
        unserved.unwrap_err().code(),
        tonic::Code::InvalidArgument,
        "an unserved index is rejected, not silently empty"
    );
}
