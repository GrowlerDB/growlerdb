//! **Walking-skeleton** end-to-end test.
//!
//! Proves the whole spine against the **real** Compose stack (MinIO + Polaris +
//! the seeded `growlerdb.docs` table — no lakehouse mocks): index → search → assert
//! coordinates (ranked) → hydrate → assert the authoritative row.
//!
//! Prereqs: `just up` (brings up the stack and seeds `growlerdb.docs`) and
//! `127.0.0.1 minio` in `/etc/hosts` (see `deploy/compose/README.md`).
//! Run: `cargo test -p growlerdb-engine --test e2e -- --ignored`.

use std::collections::BTreeMap;

use growlerdb_core::{
    CommitBatch, CompositeKey, DocOp, Document, IndexWriter, LocatedDoc, Projection, ResolvedIndex,
    RowLocator, ShardRouter, SourceCheckpoint, Value,
};
use growlerdb_engine::{get_by_key, Engine, SearchOutcome};
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_source::{IcebergConfig, IcebergReader};

/// The composite key for a `docs` row by its `id`.
fn doc_key(id: &str) -> CompositeKey {
    CompositeKey::new(vec![], vec![("id".into(), Value::from(id))])
}

/// Read a hit/row's `id` coordinate as a string.
fn id_of(key: &CompositeKey) -> String {
    key.get("id").expect("id coordinate").to_index_string()
}

#[tokio::test]
#[ignore = "requires the local dev stack (just up) + `127.0.0.1 minio` in /etc/hosts"]
async fn walking_skeleton_index_search_hydrate() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();

    // 1. INDEX the seeded table (3 rows: doc-1, doc-2, doc-3).
    let indexed = engine
        .index("growlerdb.docs", None, None)
        .await
        .expect("index growlerdb.docs");
    assert_eq!(indexed.name, "docs");
    assert_eq!(indexed.doc_count, 3, "all seeded rows indexed");

    // Idempotent re-run: same Iceberg snapshot → no-op, same index snapshot.
    let reindexed = engine
        .index("growlerdb.docs", None, None)
        .await
        .expect("re-index");
    assert_eq!(
        reindexed.snapshot, indexed.snapshot,
        "re-indexing the same snapshot is a no-op"
    );

    // 2. SEARCH → expected coordinates. `body:iceberg` appears only in doc-2.
    let one = engine
        .search("docs", "body:iceberg", 10, false, Projection::All)
        .await
        .expect("search");
    assert_eq!(one.hits.len(), 1);
    assert_eq!(id_of(&one.hits[0].key), "doc-2");
    assert!(one.hits[0].score > 0.0);
    assert!(one.rows.is_none());

    // Ranked multi-hit: `body:search` is in doc-2 and doc-3; hits are score-ordered.
    let many = engine
        .search("docs", "body:search", 10, false, Projection::All)
        .await
        .expect("search");
    let mut ids: Vec<String> = many.hits.iter().map(|h| id_of(&h.key)).collect();
    ids.sort();
    assert_eq!(ids, vec!["doc-2".to_string(), "doc-3".to_string()]);
    assert!(
        many.hits[0].score >= many.hits[1].score,
        "hits returned in descending score order"
    );

    // 3. HYDRATE → the authoritative row for doc-2, fetched from Iceberg by key.
    let hydrated = engine
        .search("docs", "body:iceberg", 10, true, Projection::All)
        .await
        .expect("hydrate");
    let rows = hydrated.rows.expect("hydrated rows");
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(id_of(&row.key), "doc-2");
    assert_eq!(row.fields["title"].to_index_string(), "iceberg search");
    assert_eq!(
        row.fields["body"].to_index_string(),
        "fast full text search over apache iceberg"
    );

    // Projection narrows the returned columns.
    let projected = engine
        .search(
            "docs",
            "body:iceberg",
            10,
            true,
            Projection::Columns(vec!["title".into()]),
        )
        .await
        .expect("projected hydrate");
    let prow = &projected.rows.expect("rows")[0];
    assert_eq!(prow.fields.keys().collect::<Vec<_>>(), vec!["title"]);
}

/// **Sharded-build gate** — building two shards of the real seeded table partitions it
/// disjointly, so a multi-shard cluster sees every document exactly once.
///
/// Builds shard 0 and shard 1 of `growlerdb.docs` with `index_shard(.., shards=2, ordinal=K)`
/// (the per-node sharded build) into two stores, then asserts the two shards' documents are a
/// **disjoint partition** of the full table, each landing on the shard the shared
/// [`ShardRouter`] routes it to — so the Gateway's broadcast-and-merge over the shards (proven
/// in-process) returns each doc once, with no cross-shard duplicates.
#[tokio::test]
#[ignore = "requires the local dev stack (just up) + `127.0.0.1 minio` in /etc/hosts"]
async fn sharded_build_partitions_the_table_disjointly() {
    let (t0, t1) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
    let e0 = Engine::open(t0.path(), IcebergConfig::local()).unwrap();
    let e1 = Engine::open(t1.path(), IcebergConfig::local()).unwrap();

    // Each node builds only its partition from source (shards=2).
    let b0 = e0
        .index_shard("growlerdb.docs", None, None, 2, 0)
        .await
        .expect("build shard 0");
    let b1 = e1
        .index_shard("growlerdb.docs", None, None, 2, 1)
        .await
        .expect("build shard 1");
    assert_eq!(
        b0.doc_count + b1.doc_count,
        3,
        "every seeded row built on exactly one shard"
    );

    let ids = |o: &SearchOutcome| {
        let mut v: Vec<String> = o.hits.iter().map(|h| id_of(&h.key)).collect();
        v.sort();
        v
    };
    let ids0 = ids(&e0
        .search("docs", "*:*", 10, false, Projection::All)
        .await
        .expect("search shard 0"));
    let ids1 = ids(&e1
        .search("docs", "*:*", 10, false, Projection::All)
        .await
        .expect("search shard 1"));

    // Disjoint partition covering the whole table — a broadcast search sees each doc once.
    let mut all: Vec<String> = ids0.iter().chain(&ids1).cloned().collect();
    all.sort();
    assert_eq!(
        all,
        vec!["doc-1", "doc-2", "doc-3"],
        "the shards' union is the full table"
    );
    assert!(
        ids0.iter().all(|id| !ids1.contains(id)),
        "a document is on both shards (would be double-counted)"
    );

    // Each shard holds exactly the docs the shared router routes to it (read routing == build split).
    let router = ShardRouter::hashed(2);
    assert!(ids0.iter().all(|id| router.route(&doc_key(id)) == 0));
    assert!(ids1.iter().all(|id| router.route(&doc_key(id)) == 1));
}

/// **Empty-shard health** — a shard that owns **zero** of the source rows (a sparse
/// multi-shard build: more shards than the 3-row table can fill) must still record the source
/// snapshot it caught up to. Otherwise it never commits a checkpoint and reports `uninitialized`
/// forever — a grey "unknown" health pill for the whole index — even though it is genuinely in sync.
#[tokio::test]
#[ignore = "requires the local dev stack (just up) + `127.0.0.1 minio` in /etc/hosts"]
async fn a_sparse_shard_with_no_rows_records_the_source_snapshot() {
    // 3 seeded rows across 5 shards ⇒ at least one ordinal owns no rows.
    let shards = 5u32;
    let router = ShardRouter::hashed(shards);
    let owned: std::collections::HashSet<u32> = ["doc-1", "doc-2", "doc-3"]
        .iter()
        .map(|id| router.route(&doc_key(id)))
        .collect();
    let empty = (0..shards)
        .find(|o| !owned.contains(o))
        .expect("some ordinal owns no rows across 5 shards");

    let t = tempfile::tempdir().unwrap();
    let e = Engine::open(t.path(), IcebergConfig::local()).unwrap();
    let out = e
        .index_shard("growlerdb.docs", None, None, shards, empty)
        .await
        .expect("build the empty shard");
    assert_eq!(
        out.doc_count, 0,
        "this ordinal owns none of the seeded rows"
    );

    // Re-open the built shard and assert it recorded the source checkpoint — so the ingestion
    // view reports it `in_sync` (green), not `uninitialized`.
    let json =
        std::fs::read_to_string(t.path().join("docs").join("index.json")).expect("index.json");
    let resolved: ResolvedIndex = serde_json::from_str(&json).expect("parse index.json");
    let store = LocalIndexStore::open(t.path()).expect("open store");
    let shard = store
        .open_shard(&ShardId::single("docs"), &resolved)
        .expect("open the built shard");
    assert!(
        shard
            .current_checkpoint()
            .expect("read checkpoint")
            .is_some(),
        "an empty shard must record the source snapshot it caught up to"
    );
}

/// An update + a delete round-trip reflected in search.
///
/// Builds the index from the real seeded table, then applies a changelog-style
/// [`DocOp`] batch (the same ops the Spark connector commits over gRPC, here applied
/// in-process): **update** doc-3's content and **delete** doc-2. Asserts that search
/// reflects both — the new content is found, the superseded/deleted content is gone —
/// proving updates & deletes (`key_to_doc` supersede + merge-on-read) end-to-end on
/// Compose. The JVM↔Rust gRPC path for the same is covered by the connector
/// cross-process test.
#[tokio::test]
#[ignore = "requires the local dev stack (just up) + `127.0.0.1 minio` in /etc/hosts"]
async fn update_and_delete_round_trip_reflected_in_search() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();
    engine
        .index("growlerdb.docs", None, None)
        .await
        .expect("index");

    // Baseline: `body:iceberg` → doc-2; `body:search` → doc-2 + doc-3.
    let before = engine
        .search("docs", "body:search", 10, false, Projection::All)
        .await
        .expect("search");
    let mut ids: Vec<String> = before.hits.iter().map(|h| id_of(&h.key)).collect();
    ids.sort();
    assert_eq!(ids, vec!["doc-2".to_string(), "doc-3".to_string()]);

    // Apply an UPDATE (doc-3 → new distinctive body) + a DELETE (doc-2), as one
    // committed changelog batch. Scoped so the shard's redb handle drops (releasing
    // the file lock) before the engine reopens it to search.
    {
        let resolved: ResolvedIndex =
            serde_json::from_slice(&std::fs::read(tmp.path().join("docs/index.json")).unwrap())
                .unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .open_shard(&ShardId::single("docs"), &resolved)
            .unwrap();

        let mut updated_fields = BTreeMap::new();
        updated_fields.insert("id".to_string(), Value::from("doc-3"));
        updated_fields.insert("body".to_string(), Value::from("supernovae cosmology"));
        let updated = LocatedDoc {
            doc: Document::new(doc_key("doc-3"), updated_fields),
            iceberg_file: "data/mutation.parquet".into(),
            row_position: 0,
        };

        IndexWriter::write(
            &shard,
            &CommitBatch::new(
                vec![DocOp::Upsert(updated), DocOp::Delete(doc_key("doc-2"))],
                // A checkpoint past the indexed snapshot; exact value is unimportant here.
                SourceCheckpoint::iceberg(i64::MAX),
                "m1-update-delete",
            ),
        )
        .expect("commit update + delete");
    }

    let search = |q: &'static str| {
        let engine = &engine;
        async move {
            let mut ids: Vec<String> = engine
                .search("docs", q, 10, false, Projection::All)
                .await
                .expect("search")
                .hits
                .iter()
                .map(|h| id_of(&h.key))
                .collect();
            ids.sort();
            ids
        }
    };

    // DELETE reflected: doc-2 was the sole `body:iceberg` match → now none.
    assert!(
        search("body:iceberg").await.is_empty(),
        "deleted doc-2 no longer matches"
    );
    // UPDATE reflected — new content is searchable…
    assert_eq!(
        search("body:supernovae").await,
        vec!["doc-3".to_string()],
        "doc-3's updated body is searchable"
    );
    // …and the prior versions are gone: `body:search` matched doc-2 (deleted) and
    // doc-3 (superseded by the update) → now none.
    assert!(
        search("body:search").await.is_empty(),
        "superseded + deleted content no longer matches"
    );
}

/// A stale locator (as if Iceberg rewrote the data file) must **verify and fall
/// back** — re-find the row by key, return the correct content (never a phantom),
/// and **refresh** the locator so it self-heals.
#[tokio::test]
#[ignore = "requires the local dev stack (just up) + `127.0.0.1 minio` in /etc/hosts"]
async fn stale_locator_self_heals_via_verify_and_fall_back() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();
    engine
        .index("growlerdb.docs", None, None)
        .await
        .expect("index");

    // Reopen the shard directly to corrupt a locator entry, simulating a rewrite
    // that moved the row to a file/position the locator no longer points at.
    let resolved: ResolvedIndex =
        serde_json::from_slice(&std::fs::read(tmp.path().join("docs/index.json")).unwrap())
            .unwrap();
    let store = LocalIndexStore::open(tmp.path()).unwrap();
    let shard = store
        .open_shard(&ShardId::single("docs"), &resolved)
        .unwrap();

    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-2"))]);
    let bogus = "data/rewritten-away.parquet";
    shard
        .refresh_locators(&[(
            key.clone(),
            RowLocator {
                iceberg_file: bogus.into(),
                row_position: 999,
            },
        )])
        .unwrap();

    // Hydrate through the engine path → fallback re-finds doc-2 by key.
    let reader = IcebergReader::connect(&IcebergConfig::local())
        .await
        .unwrap();
    let rows = get_by_key(
        &shard,
        &reader,
        "growlerdb.docs",
        std::slice::from_ref(&key),
        &Projection::All,
    )
    .await
    .expect("hydrate with fallback");
    assert_eq!(rows.len(), 1, "row re-found despite the stale locator");
    assert_eq!(id_of(&rows[0].key), "doc-2");
    assert_eq!(rows[0].fields["title"].to_index_string(), "iceberg search");

    // The locator self-healed — it no longer points at the bogus file, and a
    // second hydrate (now via the fast located path) still returns the row.
    let healed = shard.locate(&key).unwrap().expect("locator");
    assert_ne!(healed.iceberg_file, bogus, "locator refreshed");
    let again = get_by_key(&shard, &reader, "growlerdb.docs", &[key], &Projection::All)
        .await
        .unwrap();
    assert_eq!(again[0].fields["title"].to_index_string(), "iceberg search");
}

/// **Reconcile backstop** — the detect-and-repair promise, end-to-end against
/// the real seeded table. Injects drift in **both** directions into the built index, then asserts a
/// single [`Engine::reconcile`] cycle repairs it all, and that re-running is a no-op (idempotent).
///
/// The two *missing* injections stand for two provenances: an **artificially-deleted
/// indexed row** (indexed, then lost from the index while still in the source — the
/// silent-loss class) and an **artificially-skipped source row** (a source row ingest never
/// applied). Both leave the index in the same "source has the key, index doesn't" state, which
/// reconcile repairs *regardless of cause* — the whole point of the backstop. A third **stale**
/// injection (an indexed key the source never held) exercises the delete direction, so one cycle is
/// shown to close drift both ways.
#[tokio::test]
#[ignore = "requires the local dev stack (just up) + `127.0.0.1 minio` in /etc/hosts"]
async fn reconcile_backstop_detects_and_repairs_drift_both_ways() {
    let tmp = tempfile::tempdir().unwrap();
    let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();

    // Build the index from the real seeded table (doc-1, doc-2, doc-3).
    let built = engine
        .index("growlerdb.docs", None, None)
        .await
        .expect("index growlerdb.docs");
    assert_eq!(built.doc_count, 3, "all seeded rows indexed");

    // Inject drift by reopening the shard directly. Scoped so the redb handle drops (releasing the
    // file lock) before `reconcile` reopens the shard. Delete doc-1 and doc-2 (they remain in the
    // source): the two *missing* provenances. Upsert a doc-99 the source never held: a *stale* row.
    {
        let resolved: ResolvedIndex =
            serde_json::from_slice(&std::fs::read(tmp.path().join("docs/index.json")).unwrap())
                .unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .open_shard(&ShardId::single("docs"), &resolved)
            .unwrap();

        let mut phantom_fields = BTreeMap::new();
        phantom_fields.insert("id".to_string(), Value::from("doc-99"));
        let phantom = LocatedDoc {
            doc: Document::new(doc_key("doc-99"), phantom_fields),
            iceberg_file: "data/never-in-source.parquet".into(),
            row_position: 0,
        };

        IndexWriter::write(
            &shard,
            &CommitBatch::new(
                vec![
                    DocOp::Delete(doc_key("doc-1")),
                    DocOp::Delete(doc_key("doc-2")),
                    DocOp::Upsert(phantom),
                ],
                // A checkpoint past the indexed snapshot; the exact value is unimportant here.
                SourceCheckpoint::iceberg(i64::MAX),
                "task195-inject-drift",
            ),
        )
        .expect("inject drift");
    }

    let all_ids = || {
        let engine = &engine;
        async move {
            let mut ids: Vec<String> = engine
                .search("docs", "*:*", 10, false, Projection::All)
                .await
                .expect("search")
                .hits
                .iter()
                .map(|h| id_of(&h.key))
                .collect();
            ids.sort();
            ids
        }
    };

    // Pre-reconcile the index is drifted: doc-1/doc-2 dropped, the doc-99 phantom present.
    assert_eq!(
        all_ids().await,
        vec!["doc-3".to_string(), "doc-99".to_string()],
        "index drifted from the source before reconcile"
    );

    // One reconcile cycle closes BOTH directions: re-index the two missing rows, delete the stale one.
    let report = engine.reconcile("docs").await.expect("reconcile");
    assert_eq!(
        report.reindexed, 2,
        "doc-1 + doc-2 re-indexed from the source"
    );
    assert_eq!(
        report.deleted, 1,
        "phantom doc-99 removed (absent from the source)"
    );
    assert!(
        !report.deletes_skipped,
        "no concurrent ingest, so the stale-delete ran this cycle"
    );

    // Search now reflects the repaired index — back in sync with the source's three rows.
    assert_eq!(
        all_ids().await,
        vec![
            "doc-1".to_string(),
            "doc-2".to_string(),
            "doc-3".to_string()
        ],
        "reconcile repaired the index to match the source"
    );

    // Idempotent: a second cycle against the now-in-sync index repairs nothing.
    let again = engine.reconcile("docs").await.expect("re-reconcile");
    assert!(again.is_clean(), "second reconcile is a no-op: {again:?}");
}

/// **MCP Streamable HTTP transport e2e** — an HTTP MCP client's flow against the real stack:
/// build the index from the seeded `growlerdb.docs` Iceberg table, mount the composed REST
/// surface + `/mcp` exactly as the CLI fronts do, then drive `initialize` → `tools/list` →
/// `tools/call search` (with inline hydration) over the transport and assert the authoritative
/// row comes back from the real lakehouse — tool calls riding the same admitted, governed
/// `/v1` surface as any other query.
#[tokio::test]
#[ignore = "requires the local dev stack (just up) + `127.0.0.1 minio` in /etc/hosts"]
async fn mcp_http_transport_searches_and_hydrates_the_real_stack() {
    use axum::body::{to_bytes, Body};
    use axum::http::Request as HttpRequest;
    use growlerdb_engine::{
        mcp_router, rest, AdminService, Gateway, LocalNode, LookupService, SearchService,
        SuggestService,
    };
    use growlerdb_index::Shard;
    use std::sync::Arc;
    use tower::ServiceExt;

    // Author the same shape the walking skeleton indexes, so the shard can be reopened
    // outside the Engine (open_shard needs the resolved definition).
    let def = "name: docs\n\
               source: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\n\
               key: { identifier_fields: [id] }\n\
               mapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, \
               { path: title, type: TEXT }, { path: body, type: TEXT } ] }\n";

    let tmp = tempfile::tempdir().unwrap();
    let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();
    let indexed = engine
        .index("growlerdb.docs", Some(def), None)
        .await
        .expect("index growlerdb.docs");
    assert_eq!(indexed.doc_count, 3);

    // Reopen the built shard and front it with the real services + Gateway + REST + /mcp.
    let reader = IcebergReader::connect(&IcebergConfig::local())
        .await
        .unwrap();
    let schema = reader.read_source_schema("growlerdb.docs").await.unwrap();
    let resolved = growlerdb_core::IndexDefinition::from_yaml(def)
        .unwrap()
        .resolve(&schema)
        .unwrap();
    let shard: Arc<Shard> = Arc::new(
        LocalIndexStore::open(tmp.path())
            .unwrap()
            .open_shard(&ShardId::single("docs"), &resolved)
            .unwrap(),
    );
    let auth = growlerdb_engine::auth::default_auth();
    let node = LocalNode::new(
        SearchService::with_auth(shard.clone(), auth.clone()),
        SuggestService::with_auth(shard.clone(), auth.clone()),
        LookupService::with_auth(
            shard.clone(),
            IcebergConfig::local(),
            "growlerdb.docs",
            auth.clone(),
        ),
        AdminService::with_auth(shard, "docs", auth),
    );
    let gw = Arc::new(Gateway::new(node.shared()));
    let v1 = rest::router(gw.clone());
    let app = v1.clone().merge(mcp_router(v1, gw));

    let post = |body: serde_json::Value| {
        let app = app.clone();
        async move {
            let req = HttpRequest::builder()
                .method("POST")
                .uri("/mcp")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap();
            let resp = app.oneshot(req).await.unwrap();
            let status = resp.status();
            let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
            (
                status,
                serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
            )
        }
    };

    // initialize → tools/list → search(hydrate) — the client flow, over the HTTP transport.
    let (status, init) = post(serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "initialize",
        "params": { "protocolVersion": "2025-06-18" }
    }))
    .await;
    assert_eq!(status, 200);
    assert_eq!(init["result"]["serverInfo"]["name"], "growlerdb");

    let (status, list) = post(serde_json::json!({
        "jsonrpc": "2.0", "id": 2, "method": "tools/list"
    }))
    .await;
    assert_eq!(status, 200);
    // Assert the tool NAMES, not just the count: a bare count silently missed `more_like_this`
    // when TASK-319 added it (this real-stack test runs only in nightly, so the PR CI stayed green).
    let tools: std::collections::BTreeSet<&str> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        tools,
        std::collections::BTreeSet::from([
            "aggregate",
            "describe_index",
            "hydrate",
            "list_indexes",
            "more_like_this",
            "search",
        ])
    );

    let (status, resp) = post(serde_json::json!({
        "jsonrpc": "2.0", "id": 3, "method": "tools/call",
        "params": { "name": "search",
                    "arguments": { "query": "body:iceberg", "hydrate": true } }
    }))
    .await;
    assert_eq!(status, 200);
    assert_eq!(resp["result"]["isError"], false);
    let payload: serde_json::Value =
        serde_json::from_str(resp["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    let hit = &payload["hits"][0];
    assert_eq!(hit["coordinates"]["identifier"][0]["value"], "doc-2");
    // The authoritative row, hydrated from the real Iceberg table through the one tool call.
    assert_eq!(hit["row"]["title"], "iceberg search");
    assert_eq!(
        hit["row"]["body"],
        "fast full text search over apache iceberg"
    );
}
