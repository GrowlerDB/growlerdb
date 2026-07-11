//! Cross-process smoke test of the **Rust SDK** ([`growlerdb-client`]): seed
//! an index on disk, spawn the real `growlerdb serve`, and drive Search / Suggest /
//! Admin through the first-party client — the same shape as the connector e2e.
//!
//! GetByKey is part of the client surface but hydrates from Iceberg, so it needs the
//! live stack and is exercised by the engine integration path, not here.

use std::process::{Child, Command, Stdio};

use growlerdb_client::{Client, SearchQuery};
use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_index::{LocalIndexStore, ShardId};
use std::collections::BTreeMap;

/// A spawned `growlerdb serve` that is killed on drop.
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Seed `docs` on disk: persist the definition + a committed shard (id/body/city/rank),
/// then drop the store so `serve` can open it exclusively.
fn seed(root: &std::path::Path) {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("body", SourceType::String),
            SourceField::new("city", SourceType::String),
            SourceField::new("rank", SourceType::Long),
        ],
        vec![],
        vec!["id".into()],
    );
    let resolved = IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: city, type: KEYWORD }, { path: rank, type: LONG, fast: true } ] }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap();

    std::fs::create_dir_all(root.join("docs")).unwrap();
    std::fs::write(
        root.join("docs/index.json"),
        serde_json::to_vec(&resolved).unwrap(),
    )
    .unwrap();

    let store = LocalIndexStore::open(root).unwrap();
    let shard = store
        .create_shard(&ShardId::single("docs"), &resolved)
        .unwrap();
    let doc = |id: &str, body: &str, city: &str, rank: i64| {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("body".to_string(), Value::from(body));
        f.insert("city".to_string(), Value::from(city));
        f.insert("rank".to_string(), Value::Int(rank));
        LocatedDoc {
            doc: Document::new(key, f),
            iceberg_file: "data/f0.parquet".into(),
            row_position: 0,
        }
    };
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![
                doc("doc-1", "iceberg search engine", "berlin", 30),
                doc("doc-2", "iceberg lakehouse", "bern", 10),
            ],
            SourceCheckpoint::iceberg(5),
            "b1",
        ),
    )
    .unwrap();
    // `shard`/`store` drop here, releasing the redb lock before `serve` opens it.
}

async fn client(url: &str) -> Client {
    for _ in 0..80 {
        if let Ok(c) = Client::connect(url.to_string()).await {
            return c;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("growlerdb serve did not come up at {url}");
}

fn id_of(hit: &growlerdb_client::proto::SearchHit) -> String {
    hit.coordinates
        .as_ref()
        .and_then(|c| c.identifier.iter().find(|f| f.name == "id"))
        .and_then(|f| f.value.clone())
        .and_then(|v| match v.kind {
            Some(growlerdb_client::proto::value::Kind::Str(s)) => Some(s),
            _ => None,
        })
        .unwrap()
}

#[tokio::test]
async fn rust_sdk_drives_search_suggest_and_admin_against_a_live_server() {
    let tmp = tempfile::tempdir().unwrap();
    seed(tmp.path());

    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");
    let _server = Server(
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

    let client = client(&format!("http://{addr}")).await;

    // Search: rank desc → doc-1 (30) before doc-2 (10).
    let hits = client
        .search(
            SearchQuery::new("body:iceberg")
                .limit(10)
                .sort("rank", true),
        )
        .await
        .expect("search");
    assert_eq!(hits.total, 2);
    let ids: Vec<String> = hits.hits.iter().map(id_of).collect();
    assert_eq!(ids, vec!["doc-1", "doc-2"]);

    // Autocomplete: city prefix "ber" → berlin, bern.
    let sug = client
        .suggest_prefix("city", "ber", 10)
        .await
        .expect("suggest");
    let terms: Vec<&str> = sug.suggestions.iter().map(|s| s.text.as_str()).collect();
    assert_eq!(terms, vec!["berlin", "bern"]);

    // Did-you-mean: "bostom" has no near city here, "bern" is distance 2 from "ber n"…
    // use a real near-miss: "berlim" → berlin (distance 1).
    let dym = client
        .suggest_fuzzy("city", "berlim", 10, 1)
        .await
        .expect("fuzzy");
    let terms: Vec<&str> = dym.suggestions.iter().map(|s| s.text.as_str()).collect();
    assert_eq!(terms, vec!["berlin"]);

    // Admin: describe the served index.
    let stats = client.describe_index("").await.expect("describe");
    assert_eq!(stats.name, "docs");
    assert_eq!(stats.num_docs, 2);
    assert_eq!(stats.checkpoint, "iceberg_snapshot:5");

    // A bad request surfaces the gRPC status through the client error.
    let err = client.suggest_prefix("nope", "x", 10).await.unwrap_err();
    assert!(
        matches!(&err, growlerdb_client::ClientError::Rpc(s) if s.code() == tonic::Code::InvalidArgument),
        "unexpected error: {err:?}"
    );
}
