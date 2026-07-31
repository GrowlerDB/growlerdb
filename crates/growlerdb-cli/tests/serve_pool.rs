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
use growlerdb_proto::v1::write_client::WriteClient;
use growlerdb_proto::v1::{DocBatch, SearchRequest, WriteRequest};
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

/// Define `index` on disk (its `index.json`) but build **no** window shard — a pool node then serves
/// zero windows for it at boot, and the first write must create the window lazily.
fn define_windowed_index(data_dir: &std::path::Path, index: &str) {
    let resolved = windowed_index(index);
    std::fs::create_dir_all(data_dir.join(index)).unwrap();
    std::fs::write(
        data_dir.join(index).join("index.json"),
        serde_json::to_vec(&resolved).unwrap(),
    )
    .unwrap();
}

/// A minimal **hash** (non-windowed) index (`id` KEYWORD) named `index` — its units are ordinal
/// shards, not time windows.
fn hash_index(index: &str) -> ResolvedIndex {
    let src = SourceSchema::new(
        vec![SourceField::new("id", SourceType::String)],
        vec![],
        vec!["id".into()],
    );
    IndexDefinition::from_yaml(&format!(
        "name: {index}\nsource: {{ iceberg: {{ catalog: g, table: g.{index} }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD, fast: true }} ] }}\n",
    ))
    .unwrap()
    .resolve(&src)
    .unwrap()
}

/// Define hash `index` on disk and build one **empty** ordinal shard per entry in `ordinals`
/// (`{index}/{ordinal}`) — the pre-placed ordinals a pool node serves and the connector streams into.
fn build_hash_ordinals(data_dir: &std::path::Path, index: &str, ordinals: &[u32]) {
    let resolved = hash_index(index);
    std::fs::create_dir_all(data_dir.join(index)).unwrap();
    std::fs::write(
        data_dir.join(index).join("index.json"),
        serde_json::to_vec(&resolved).unwrap(),
    )
    .unwrap();
    let store = LocalIndexStore::open(data_dir).unwrap();
    for &o in ordinals {
        store
            .create_shard(&ShardId::shard(index, o), &resolved)
            .unwrap();
    }
}

/// A one-doc changelog batch for a hash index: upsert `id` (no window field), as a wire `DocBatch`.
fn hash_doc_batch(id: &str, batch_id: &str) -> DocBatch {
    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
    let mut fields = BTreeMap::new();
    fields.insert("id".to_string(), Value::from(id));
    CommitBatch::from_upserts(
        vec![LocatedDoc {
            doc: Document::new(key, fields),
            iceberg_file: "f0".into(),
            row_position: 0,
        }],
        SourceCheckpoint::iceberg(1),
        batch_id,
    )
    .into()
}

/// Search hash `(index, ordinal)` and return the matched `id`s — routes on the `shard` selector.
async fn search_ids_shard(
    client: &mut SearchClient<Channel>,
    index: &str,
    shard: u32,
) -> Vec<String> {
    let resp = client
        .search(SearchRequest {
            query: "*".into(),
            limit: 10,
            index: index.into(),
            shard,
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
        // A bare TCP connect can land in the bind-:0-then-release window where the port is
        // transiently contested by a sibling test's dying server; only a gRPC-level reply
        // (Ok or an application Status, never a transport error) proves OUR server is up.
        if let Ok(mut client) = SearchClient::connect(url.to_string()).await {
            match client.search(SearchRequest::default()).await {
                Ok(_) => return client,
                Err(status) if std::error::Error::source(&status).is_none() => return client,
                Err(_) => {}
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("growlerdb serve-pool did not come up at {url}");
}

async fn connect_write(url: &str) -> WriteClient<Channel> {
    for _ in 0..120 {
        if let Ok(client) = WriteClient::connect(url.to_string()).await {
            return client;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("growlerdb serve-pool Write did not come up at {url}");
}

/// A one-doc changelog batch: upsert `id` with window field `ts` (epoch-ms), as a wire `DocBatch`.
fn one_doc_batch(id: &str, ts_ms: i64, batch_id: &str) -> DocBatch {
    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
    let mut fields = BTreeMap::new();
    fields.insert("id".to_string(), Value::from(id));
    fields.insert("ts".to_string(), Value::from(ts_ms));
    CommitBatch::from_upserts(
        vec![LocatedDoc {
            doc: Document::new(key, fields),
            iceberg_file: "f0".into(),
            row_position: 0,
        }],
        SourceCheckpoint::iceberg(1),
        batch_id,
    )
    .into()
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
    // An index this node doesn't serve is the structured not-served refusal (not a silent empty
    // result) — FailedPrecondition with the UNIT_NOT_SERVED detail, surviving the wire hop.
    let unserved = client
        .search(SearchRequest {
            query: "*".into(),
            limit: 10,
            index: "gamma".into(),
            window: 10,
            ..Default::default()
        })
        .await;
    let err = unserved.unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::FailedPrecondition,
        "an unserved index is rejected, not silently empty"
    );
    assert!(
        growlerdb_engine::is_unit_not_served(&err),
        "the structured UNIT_NOT_SERVED detail survives the wire: {err:?}"
    );
}

/// HA-G4: one corrupt window shard must be QUARANTINED at boot — logged and skipped — not fail the
/// whole multi-index pool process. The healthy index keeps serving; the poisoned unit answers the
/// structured `UNIT_NOT_SERVED` refusal (the stale-route signal a registered pool's CP/gateway
/// walk past), exactly as if the node never held it.
#[tokio::test]
async fn serve_pool_quarantines_a_corrupt_window_shard_and_serves_the_rest() {
    let tmp = tempfile::tempdir().unwrap();
    build_windowed_index(tmp.path(), "alpha", "alphadoc");
    build_windowed_index(tmp.path(), "beta", "betadoc");
    // Poison alpha/w10: garbage over the Tantivy meta so the shard cannot open.
    std::fs::write(
        tmp.path()
            .join("alpha")
            .join("w10")
            .join("index")
            .join("meta.json"),
        b"{ definitely not tantivy meta",
    )
    .unwrap();

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

    // The process came up despite the corrupt shard, and the healthy index serves.
    let mut client = connect(&format!("http://{addr}")).await;
    assert_eq!(
        search_ids(&mut client, "beta", 10).await,
        vec!["betadoc"],
        "the healthy index serves despite the sibling's corrupt shard"
    );
    // The quarantined unit is a structured refusal, not a crash and not a silent empty page.
    let err = client
        .search(SearchRequest {
            query: "*".into(),
            limit: 10,
            index: "alpha".into(),
            window: 10,
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        growlerdb_engine::is_unit_not_served(&err),
        "a quarantined shard reads as not-served: {err:?}"
    );
}

/// HA-G4: plain Kubernetes stops a pod with SIGTERM (only the Helm preStop sends SIGINT) — the
/// node must drain and exit cleanly on it, not die on the default disposition mid-request.
#[cfg(unix)]
#[tokio::test]
async fn serve_pool_shuts_down_cleanly_on_sigterm() {
    use std::io::Read;

    let tmp = tempfile::tempdir().unwrap();
    build_windowed_index(tmp.path(), "alpha", "alphadoc");
    let port = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    };
    let addr = format!("127.0.0.1:{port}");
    let mut server = Server(
        Command::new(env!("CARGO_BIN_EXE_growlerdb"))
            .args([
                "--data-dir",
                tmp.path().to_str().unwrap(),
                "serve-pool",
                "--index",
                "alpha",
                "--addr",
                &addr,
            ])
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn growlerdb serve-pool"),
    );
    // Fully up and serving before the signal.
    let mut client = connect(&format!("http://{addr}")).await;
    assert_eq!(search_ids(&mut client, "alpha", 10).await, vec!["alphadoc"]);

    let status = Command::new("kill")
        .args(["-TERM", &server.0.id().to_string()])
        .status()
        .expect("send SIGTERM");
    assert!(status.success(), "kill -TERM delivered");
    // Graceful drain: the process exits ZERO within a bounded wait (an unhandled SIGTERM would be
    // a signal death — non-zero / signaled).
    let exited = {
        let mut exited = None;
        for _ in 0..200 {
            if let Some(st) = server.0.try_wait().expect("try_wait") {
                exited = Some(st);
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        exited.expect("serve-pool did not exit within 10s of SIGTERM")
    };
    assert!(exited.success(), "clean zero exit on SIGTERM: {exited:?}");
    let mut out = String::new();
    server
        .0
        .stdout
        .take()
        .expect("piped stdout")
        .read_to_string(&mut out)
        .unwrap();
    assert!(
        out.contains("shut down cleanly"),
        "the graceful-shutdown path ran (stdout: {out:?})"
    );
}

#[tokio::test]
async fn serve_pool_dispatches_writes_across_two_indexes_and_creates_windows_lazily() {
    // One day in epoch-ms / canonical-micros: a `ts` of `n` days lands in daily window `n * DAY_US`.
    const DAY_MS: i64 = 86_400_000;
    const DAY_US: i64 = 86_400_000_000;

    let tmp = tempfile::tempdir().unwrap();
    // Two windowed indexes DEFINED but not built — no window shards on disk yet.
    define_windowed_index(tmp.path(), "alpha");
    define_windowed_index(tmp.path(), "beta");

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

    let url = format!("http://{addr}");
    let mut writer = connect_write(&url).await;

    // Write one doc into each index at day 10 — the (index, window) selector dispatches to that
    // index's writer, which creates the day-10 window on first write.
    writer
        .write(WriteRequest {
            batch: Some(one_doc_batch("alphadoc", 10 * DAY_MS, "a1")),
            index: "alpha".into(),
            shard: 0,
        })
        .await
        .expect("write to alpha");
    writer
        .write(WriteRequest {
            batch: Some(one_doc_batch("betadoc", 10 * DAY_MS, "b1")),
            index: "beta".into(),
            shard: 0,
        })
        .await
        .expect("write to beta");

    // Each write landed in the CORRECT index's freshly-created day-10 window — queryable with no
    // restart (the writer shares the read pool's window maps).
    let mut client = connect(&url).await;
    assert_eq!(
        search_ids(&mut client, "alpha", 10 * DAY_US).await,
        vec!["alphadoc"],
        "alpha write → alpha's day-10 window"
    );
    assert_eq!(
        search_ids(&mut client, "beta", 10 * DAY_US).await,
        vec!["betadoc"],
        "beta write → beta's day-10 window"
    );

    // A write to an index this node doesn't serve is the structured not-served refusal (as on the
    // read path).
    let unserved = writer
        .write(WriteRequest {
            batch: Some(one_doc_batch("gammadoc", 10 * DAY_MS, "g1")),
            index: "gamma".into(),
            shard: 0,
        })
        .await;
    assert_eq!(
        unserved.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "a write to an unserved index is rejected"
    );
}

/// A pool node serves a **hash** index (D52): it opens the on-disk ordinal shards, routes reads AND
/// writes on the `shard` ordinal selector (not `window`), and a write to an ordinal it doesn't hold is
/// the structured not-served refusal — the hash counterpart to the windowed dispatch above.
#[tokio::test]
async fn serve_pool_serves_a_hash_index_by_ordinal() {
    let tmp = tempfile::tempdir().unwrap();
    // A hash index `docs` with two ordinal shards pre-built empty (as `growlerdb index --shards`
    // would place them); the connector then streams docs to their ordinal.
    build_hash_ordinals(tmp.path(), "docs", &[0, 1]);

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
                "serve-pool",
                "--index",
                "docs",
                "--addr",
                &addr,
            ])
            .stdout(Stdio::null())
            .spawn()
            .expect("spawn growlerdb serve-pool"),
    );

    let url = format!("http://{addr}");
    let mut writer = connect_write(&url).await;

    // A write tagged to ordinal 1 dispatches to ordinal 1's shard (ordinal-routed, not partitioned).
    writer
        .write(WriteRequest {
            batch: Some(hash_doc_batch("d1", "b1")),
            index: "docs".into(),
            shard: 1,
        })
        .await
        .expect("write to ordinal 1");

    // Queryable on ordinal 1 with no restart; ordinal 0 stayed empty (the write reached the RIGHT
    // ordinal). The pool routes the read on `shard`, not `window`.
    let mut client = connect(&url).await;
    assert_eq!(
        search_ids_shard(&mut client, "docs", 1).await,
        vec!["d1"],
        "ordinal-1 write → ordinal 1's shard"
    );
    assert!(
        search_ids_shard(&mut client, "docs", 0).await.is_empty(),
        "ordinal 0 stayed empty"
    );

    // A write to an ordinal this node doesn't hold is the structured not-served refusal (a stale
    // route the connector re-resolves), exactly as an unserved index/window is.
    let unserved = writer
        .write(WriteRequest {
            batch: Some(hash_doc_batch("d9", "b9")),
            index: "docs".into(),
            shard: 9,
        })
        .await;
    assert_eq!(
        unserved.unwrap_err().code(),
        tonic::Code::FailedPrecondition,
        "a write to an unheld ordinal is rejected"
    );
}
