//! Replica via segment shipping: a replica **pulls sealed segments** from the
//! primary's backup instead of re-indexing, so its segments are byte-identical and it scores
//! exactly like the primary. Covers: byte-identical pull, no duplicate indexing (the replica never
//! writes), incremental refresh (only new segments transfer), and consistent scoring across a
//! commit that changes BM25 statistics.

use std::collections::BTreeMap;

use growlerdb_backup::{backup, fs_store, refresh, refresh_and_reopen};
use growlerdb_core::{
    CommitBatch, CompositeKey, Document, Hit, IndexDefinition, IndexWriter, LocatedDoc, Query,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_index::{LocalIndexStore, Shard, ShardId};

fn docs_index() -> growlerdb_core::ResolvedIndex {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("body", SourceType::String),
        ],
        vec![],
        vec!["id".into()],
    );
    IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT } ] }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap()
}

fn doc(id: &str, body: &str) -> LocatedDoc {
    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
    let mut f = BTreeMap::new();
    f.insert("id".to_string(), Value::from(id));
    f.insert("body".to_string(), Value::from(body));
    LocatedDoc {
        doc: Document::new(key, f),
        iceberg_file: "f".into(),
        row_position: 0,
    }
}

/// (key, score) pairs for a query — equal across nodes iff segments are byte-identical.
fn scored(shard: &Shard, q: &str) -> Vec<(String, f32)> {
    shard
        .search_all(&Query::parse(q).unwrap(), 10)
        .unwrap()
        .iter()
        .map(|h: &Hit| (format!("{:?}", h.key), h.score))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replica_pulls_segments_and_scores_identically() {
    let idx = docs_index();
    let id = ShardId::single("docs");

    // --- Primary: index 3 docs (snapshot 1), back up.
    let p_root = tempfile::tempdir().unwrap();
    let p_store = LocalIndexStore::open(p_root.path()).unwrap();
    let primary = p_store.create_shard(&id, &idx).unwrap();
    IndexWriter::write(
        &primary,
        &CommitBatch::from_upserts(
            vec![
                doc("d1", "alpha"),
                doc("d2", "beta alpha"),
                doc("d3", "gamma"),
            ],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();

    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    let staging = p_root.path().join(".staging");
    let prefix = "backups/docs";
    backup(&primary, "docs", "docs", &staging, &store, prefix, None)
        .await
        .unwrap();

    // --- Replica: first refresh downloads everything (nothing skipped).
    let r_root = tempfile::tempdir().unwrap();
    let r_store = LocalIndexStore::open(r_root.path()).unwrap();
    let dest = r_store.shard_path(&id);
    let s1 = refresh(&store, prefix, &dest).await.unwrap();
    assert!(
        s1.downloaded > 0 && s1.skipped == 0,
        "cold refresh downloads all: {s1:?}"
    );

    let replica = r_store.open_shard(&id, &idx).unwrap();
    assert_eq!(
        scored(&replica, "body:alpha"),
        scored(&primary, "body:alpha"),
        "replica scores identically to the primary (byte-identical segments)"
    );

    // --- Primary: commit 2 more docs (snapshot 2 → a new segment; BM25 stats shift), back up.
    IndexWriter::write(
        &primary,
        &CommitBatch::from_upserts(
            vec![doc("d4", "alpha delta"), doc("d5", "epsilon")],
            SourceCheckpoint::iceberg(2),
            "b2",
        ),
    )
    .unwrap();
    backup(&primary, "docs", "docs", &staging, &store, prefix, None)
        .await
        .unwrap();

    // --- Replica: incremental refresh — the snapshot-1 segment is reused, only the new one ships.
    let s2 = refresh(&store, prefix, &dest).await.unwrap();
    assert!(
        s2.skipped > 0,
        "incremental refresh reuses existing segments: {s2:?}"
    );
    assert_eq!(s2.manifest.snapshot, 2);

    let replica = r_store.open_shard(&id, &idx).unwrap();
    let p = scored(&primary, "body:alpha");
    let r = scored(&replica, "body:alpha");
    assert_eq!(p.len(), 3, "alpha now matches d1, d2, d4");
    assert_eq!(
        r, p,
        "after incremental refresh the replica still matches the primary exactly"
    );
}

/// Live read-replica: `refresh_and_reopen` re-opens the shard only when the pull changed
/// something, and the re-opened shard reflects the primary's new segments — the unit a `serve
/// --replica` poll loop calls before a hot-swap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_and_reopen_reopens_only_on_change() {
    let idx = docs_index();
    let id = ShardId::single("docs");

    // Primary: index 2 docs, back up.
    let p_root = tempfile::tempdir().unwrap();
    let p_store = LocalIndexStore::open(p_root.path()).unwrap();
    let primary = p_store.create_shard(&id, &idx).unwrap();
    IndexWriter::write(
        &primary,
        &CommitBatch::from_upserts(
            vec![doc("d1", "alpha"), doc("d2", "beta")],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();
    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    let staging = p_root.path().join(".staging");
    let prefix = "backups/docs";
    backup(&primary, "docs", "docs", &staging, &store, prefix, None)
        .await
        .unwrap();

    // Replica: first cycle (serving nothing yet, snapshot 0) downloads everything → re-opens, and
    // searches like the primary.
    let r_root = tempfile::tempdir().unwrap();
    let r_store = LocalIndexStore::open(r_root.path()).unwrap();
    let (shard, s1) = refresh_and_reopen(&store, prefix, &r_store, &id, &idx, None, 0)
        .await
        .unwrap();
    assert!(s1.downloaded > 0, "cold pull downloads: {s1:?}");
    let shard = shard.expect("a changed pull re-opens the shard");
    assert_eq!(scored(&shard, "body:alpha"), scored(&primary, "body:alpha"));

    // Second cycle, now serving snapshot 1, primary unchanged → no re-open (cheap steady-state poll),
    // even though the mutable meta/locator files always re-download.
    let (none, _) = refresh_and_reopen(
        &store,
        prefix,
        &r_store,
        &id,
        &idx,
        None,
        s1.manifest.snapshot,
    )
    .await
    .unwrap();
    assert!(
        none.is_none(),
        "an unchanged primary does not re-open the shard"
    );

    // Primary commits a new doc + re-backs up → next cycle (still serving snapshot 1) re-opens.
    IndexWriter::write(
        &primary,
        &CommitBatch::from_upserts(
            vec![doc("d3", "alpha gamma")],
            SourceCheckpoint::iceberg(2),
            "b2",
        ),
    )
    .unwrap();
    backup(&primary, "docs", "docs", &staging, &store, prefix, None)
        .await
        .unwrap();
    let (shard, s3) = refresh_and_reopen(
        &store,
        prefix,
        &r_store,
        &id,
        &idx,
        None,
        s1.manifest.snapshot,
    )
    .await
    .unwrap();
    assert_eq!(s3.manifest.snapshot, 2);
    let shard = shard.expect("a new primary commit re-opens the replica");
    assert_eq!(
        scored(&shard, "body:alpha").len(),
        2,
        "alpha now matches d1 + d3 after the swap-worthy refresh"
    );
}
