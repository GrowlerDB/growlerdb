//! Backup → restore round-trip (task-32) over the filesystem object-store backend (no MinIO
//! needed): build a populated shard, back it up, restore into a fresh location, and assert the
//! restored shard returns the same results and carries the same snapshot + checkpoint (so
//! ingestion can resume the tail exactly-once). Also covers the no-backup case.

use std::collections::BTreeMap;

use growlerdb_backup::{
    backup, cold_park, fs_store, park, promote_cold, read_manifest, refresh, restore, revive,
    BackupError, FileEntry, Manifest, MANIFEST_FORMAT,
};
use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc, Query,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_index::RangeCache;
use growlerdb_index::{LocalIndexStore, ShardId};

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_then_restore_round_trips_data_and_checkpoint() {
    let idx = docs_index();
    let id = ShardId::single("docs");

    // --- Build + populate the source shard.
    let src_root = tempfile::tempdir().unwrap();
    let src_store = LocalIndexStore::open(src_root.path()).unwrap();
    let shard = src_store.create_shard(&id, &idx).unwrap();
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![
                doc("doc-1", "alpha"),
                doc("doc-2", "beta alpha"),
                doc("doc-3", "gamma"),
            ],
            SourceCheckpoint::iceberg(7),
            "b1",
        ),
    )
    .unwrap();
    let hits_before = shard
        .search_all(&Query::parse("body:alpha").unwrap(), 10)
        .unwrap();
    assert_eq!(hits_before.len(), 2);

    // --- Back up to a filesystem object store (staging on the same fs → hard-links).
    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    let staging = src_root.path().join(".backup-staging");
    let prefix = "backups/docs/snap";
    let def_json = serde_json::to_string(&idx).unwrap();
    let manifest = backup(
        &shard,
        "docs",
        "docs",
        &staging,
        &store,
        prefix,
        Some(def_json),
    )
    .await
    .unwrap();

    assert_eq!(manifest.snapshot, 1);
    // D30: format 1 IS the layered format — the backup ships the dense location array
    // alongside the segments + aux store.
    assert_eq!(manifest.format, MANIFEST_FORMAT);
    assert_eq!(manifest.format, 1, "format 1 is the layered format");
    assert_eq!(manifest.checkpoint, Some(SourceCheckpoint::iceberg(7)));
    assert!(manifest.files.iter().any(|f| f.path == "aux.redb"));
    assert!(manifest.files.iter().any(|f| f.path == "location.arr"));
    assert!(manifest.files.iter().any(|f| f.path == "index/meta.json"));
    assert!(manifest.files.iter().any(|f| f.path.starts_with("index/")));
    // The definition is carried in the manifest (not as a shard file).
    assert!(manifest.definition_json.is_some());
    assert!(!staging.exists(), "staging is cleaned up after backup");

    // --- Restore onto a fresh node location, open it, and verify it matches.
    let dest_root = tempfile::tempdir().unwrap();
    let dest_store = LocalIndexStore::open(dest_root.path()).unwrap();
    let dest = dest_store.shard_path(&id);
    let restored_manifest = restore(&store, prefix, &dest).await.unwrap();
    assert_eq!(restored_manifest.snapshot, 1);
    assert_eq!(
        restored_manifest.format, 1,
        "format 1 restores materialized"
    );
    // The location array is restored byte-identical.
    assert_eq!(
        std::fs::read(dest.join("location.arr")).unwrap(),
        std::fs::read(src_store.shard_path(&id).join("location.arr")).unwrap(),
        "location.arr round-trips byte-identical"
    );

    let restored = dest_store.open_shard(&id, &idx).unwrap();
    let hits_after = restored
        .search_all(&Query::parse("body:alpha").unwrap(), 10)
        .unwrap();
    assert_eq!(hits_after.len(), 2, "restored shard returns the same hits");
    assert_eq!(restored.current_snapshot().unwrap(), 1);
    assert_eq!(
        restored.current_checkpoint().unwrap(),
        Some(SourceCheckpoint::iceberg(7)),
        "restored checkpoint lets ingestion resume the tail exactly-once"
    );
    // …and hydrates through the layered path: key → `_locid` → restored array → intern.
    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-2"))]);
    let loc = restored.locate(&key).unwrap().expect("layered locate");
    assert_eq!(loc.iceberg_file, "f");
    assert_eq!(loc.row_position, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn park_evicts_local_window_then_revive_restores_it() {
    // Cold-tiering (task-80): parking a (window) shard backs it up and evicts the local dir;
    // reviving restores it, searchable again with the same checkpoint. Uses a window shard since
    // that's the tiering unit, though park/revive are shard-type agnostic.
    let idx = docs_index();
    let w: i64 = 1_700_000_000_000;
    let id = ShardId::window("docs", w);

    let root = tempfile::tempdir().unwrap();
    let local = LocalIndexStore::open(root.path()).unwrap();
    let shard = local.create_shard(&id, &idx).unwrap();
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![doc("doc-1", "alpha"), doc("doc-2", "beta alpha")],
            SourceCheckpoint::iceberg(7),
            "b1",
        ),
    )
    .unwrap();
    assert_eq!(
        shard
            .search_all(&Query::parse("body:alpha").unwrap(), 10)
            .unwrap()
            .len(),
        2
    );
    let shard_dir = local.shard_path(&id);
    assert!(shard_dir.exists());

    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    let staging = root.path().join(".park-staging");
    let prefix = "parked/docs/w1700000000000";

    // PARK: backup + evict the local dir.
    let manifest = park(
        shard,
        "docs",
        "w1700000000000",
        &shard_dir,
        &staging,
        &store,
        prefix,
        Some(serde_json::to_string(&idx).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(manifest.snapshot, 1);
    assert!(!shard_dir.exists(), "parked window's local dir is evicted");
    assert_eq!(
        read_manifest(&store, prefix).await.unwrap().snapshot,
        1,
        "the backup is restorable"
    );

    // REVIVE: restore back into the (now-absent) dir; search again.
    revive(&store, prefix, &shard_dir).await.unwrap();
    assert!(shard_dir.exists(), "revived window's dir is back");
    let revived = local.open_shard(&id, &idx).unwrap();
    assert_eq!(
        revived
            .search_all(&Query::parse("body:alpha").unwrap(), 10)
            .unwrap()
            .len(),
        2,
        "revived shard returns the same hits"
    );
    assert_eq!(
        revived.current_checkpoint().unwrap(),
        Some(SourceCheckpoint::iceberg(7)),
        "revived checkpoint lets ingestion resume the tail"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cold_park_evicts_bulk_keeps_aux_and_serves_read_through() {
    // Cold-tiering (task-80): cold_park backs up the bulk, evicts the local Tantivy `index/` but
    // keeps `aux.redb`, and drops a marker — then the window is served read-through in place.
    let idx = docs_index();
    let w: i64 = 1_700_000_000_000;
    let id = ShardId::window("docs", w);

    let root = tempfile::tempdir().unwrap();
    let local = LocalIndexStore::open(root.path()).unwrap();
    let shard = local.create_shard(&id, &idx).unwrap();
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![doc("doc-1", "alpha"), doc("doc-2", "beta alpha")],
            SourceCheckpoint::iceberg(7),
            "b1",
        ),
    )
    .unwrap();
    shard.set_event_bounds(Some(10), Some(99)).unwrap(); // a zone-map to carry into the marker
    let window_dir = local.shard_path(&id);

    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    let staging = root.path().join(".cold-staging");
    let prefix = "cold/docs/w1700000000000";
    let marker = cold_park(
        shard,
        "docs",
        w,
        &window_dir,
        &staging,
        &store,
        prefix,
        Some(serde_json::to_string(&idx).unwrap()),
    )
    .await
    .unwrap();

    // Bulk evicted; aux + location array + marker kept; marker carries prefix, zone-map, snapshot.
    assert!(!window_dir.join("index").exists(), "tantivy bulk evicted");
    assert!(window_dir.join("aux.redb").exists(), "aux kept local");
    assert!(
        window_dir.join("location.arr").exists(),
        "D30: the location array ships with the parked window and stays LOCAL (never read-through)"
    );
    assert!(window_dir.join("cold.json").exists(), "marker written");
    assert_eq!(marker.object_prefix, "cold/docs/w1700000000000/data/index");
    assert_eq!((marker.event_min, marker.event_max), (Some(10), Some(99)));
    assert_eq!(marker.snapshot, 1);
    // task-83: cold_park built + stored a hotcache sidecar (outside data/ so backup GC won't prune).
    assert_eq!(
        marker.hotcache_key.as_deref(),
        Some("cold/docs/w1700000000000/hotcache.bin")
    );
    // task-83 slice 2: it also bundled the index into ONE object + a layout manifest, and removed
    // the now-redundant individual index objects (no storage doubling).
    assert_eq!(
        marker.bundle_key.as_deref(),
        Some("cold/docs/w1700000000000/split.bundle")
    );
    assert_eq!(
        marker.bundle_manifest_key.as_deref(),
        Some("cold/docs/w1700000000000/split.manifest")
    );
    let leftover_index_objects = store
        .list_with("cold/docs/w1700000000000/data/index/")
        .recursive(true)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| !e.path().ends_with('/'))
        .count();
    assert_eq!(
        leftover_index_objects, 0,
        "individual index objects removed once bundled"
    );
    // task-150 / F7: the manifest is rewritten `bundled` (its index/* entries dropped), and a plain
    // restore of a bundled prefix refuses cleanly instead of 404-ing on the deleted objects.
    let bundled_manifest = read_manifest(&store, "cold/docs/w1700000000000")
        .await
        .unwrap();
    assert!(bundled_manifest.bundled, "manifest marked bundled");
    assert!(
        !bundled_manifest
            .files
            .iter()
            .any(|f| f.path.starts_with("index/")),
        "index/* entries dropped from the bundled manifest"
    );
    // D30: only `index/*` files were bundled/deleted — the aux + location-array entries (and
    // their data objects, part of the restorable backup) survive the bundling rewrite.
    assert!(bundled_manifest.files.iter().any(|f| f.path == "aux.redb"));
    assert!(bundled_manifest
        .files
        .iter()
        .any(|f| f.path == "location.arr"));
    let dest = tempfile::tempdir().unwrap();
    assert!(
        matches!(
            restore(&store, "cold/docs/w1700000000000", dest.path()).await,
            Err(BackupError::Bundled(_))
        ),
        "restore refuses a bundled cold prefix"
    );
    // Discovery sees it as cold.
    let discovered = local.cold_marker("docs", w).unwrap().unwrap();
    assert_eq!(discovered, marker);

    // The window is still searchable — now served from the single bundle object (+ hotcache).
    let cache = RangeCache::new(8 * 1024 * 1024);
    let store2 = store.clone();
    let wd = window_dir.clone();
    let object_prefix = marker.object_prefix.clone();
    let hotcache_key = marker.hotcache_key.clone();
    let bundle_key = marker.bundle_key.clone();
    let bundle_manifest_key = marker.bundle_manifest_key.clone();
    let (hits, cold_loc) = tokio::task::spawn_blocking(move || {
        let bundle = bundle_key.as_deref().zip(bundle_manifest_key.as_deref());
        let cold = local
            .open_cold_shard(
                &idx,
                &wd,
                store2,
                &object_prefix,
                cache,
                hotcache_key.as_deref(),
                bundle,
            )
            .unwrap();
        let hits = cold
            .search_all(&Query::parse("body:alpha").unwrap(), 10)
            .unwrap()
            .len();
        // D30: the layered locate works on the parked window — the key term resolves
        // through the read-through segments, the slot from the LOCAL array.
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
        let loc = cold.locate(&key).unwrap();
        (hits, loc)
    })
    .await
    .unwrap();
    assert_eq!(
        hits, 2,
        "cold-parked window still searchable via the hotcache"
    );
    let cold_loc = cold_loc.expect("cold layered locate");
    assert_eq!(cold_loc.iceberg_file, "f");
    assert_eq!(cold_loc.row_position, 0);

    // task-83 slice 3: PRE-WARM — a hot-again window is promoted back to a local hot shard.
    // (Re-open the store handle + index; the search block above moved its clones.)
    let local = LocalIndexStore::open(root.path()).unwrap();
    let idx = docs_index();
    promote_cold(&store, &marker, &window_dir).await.unwrap();
    assert!(
        !window_dir.join("cold.json").exists(),
        "cold marker dropped → window is hot again"
    );
    assert!(
        window_dir.join("index").join("meta.json").exists(),
        "index materialized locally from the bundle"
    );
    // Discovery no longer sees it as cold; it opens as a normal local hot shard and searches.
    assert!(local.cold_marker("docs", w).unwrap().is_none());
    let hot = local.open_shard(&id, &idx).unwrap();
    assert_eq!(
        hot.search_all(&Query::parse("body:alpha").unwrap(), 10)
            .unwrap()
            .len(),
        2,
        "promoted window serves from local NVMe (no cold latency)"
    );
    // task-150 / B9: promote reclaimed the window's object-storage copies — nothing left under its
    // backup prefix (no leaked bundle/hotcache/manifest).
    let leftover_objects = store
        .list_with("cold/docs/w1700000000000/")
        .recursive(true)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| !e.path().ends_with('/'))
        .count();
    assert_eq!(leftover_objects, 0, "promote reclaimed all object copies");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_gc_prunes_superseded_splits() {
    // task-33 backup GC: after the primary compacts (fusing many segments into one), a re-backup
    // to the same prefix must remove the now-orphaned old segment objects from the store, not just
    // add the new fused segment. Assert the store's data/ objects match the new manifest exactly.
    let idx = docs_index();
    let id = ShardId::single("docs");

    let src_root = tempfile::tempdir().unwrap();
    let src_store = LocalIndexStore::open(src_root.path()).unwrap();
    let shard = src_store.create_shard(&id, &idx).unwrap();
    // Three separate commits → three segments (each commit seals its own segment).
    for (n, d) in [
        ("b1", doc("doc-1", "alpha")),
        ("b2", doc("doc-2", "beta")),
        ("b3", doc("doc-3", "gamma")),
    ] {
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![d], SourceCheckpoint::iceberg(1), n),
        )
        .unwrap();
    }
    assert!(shard.segment_count().unwrap() >= 2, "multiple segments");

    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    let staging = src_root.path().join(".backup-staging");
    let prefix = "backups/docs/snap";

    // First backup: uploads every segment.
    backup(&shard, "docs", "docs", &staging, &store, prefix, None)
        .await
        .unwrap();
    let after_first = store
        .list_with(&format!("{prefix}/data/"))
        .recursive(true)
        .await
        .unwrap()
        .into_iter()
        .filter(|e| !e.path().ends_with('/'))
        .count();

    // Compact on the primary → the many segments fuse into one (new file names).
    shard
        .compact(&growlerdb_index::CompactionPolicy::default())
        .unwrap();
    assert_eq!(
        shard.segment_count().unwrap(),
        1,
        "compacted to one segment"
    );

    // Second backup to the SAME prefix: uploads the fused segment AND GCs the orphaned old ones.
    let manifest = backup(&shard, "docs", "docs", &staging, &store, prefix, None)
        .await
        .unwrap();

    let remaining: std::collections::BTreeSet<String> = store
        .list_with(&format!("{prefix}/data/"))
        .recursive(true)
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.path().to_string())
        .filter(|p| !p.ends_with('/'))
        .map(|p| p.trim_start_matches(&format!("{prefix}/data/")).to_string())
        .collect();
    let wanted: std::collections::BTreeSet<String> =
        manifest.files.iter().map(|f| f.path.clone()).collect();
    assert_eq!(
        remaining, wanted,
        "store holds exactly the manifest's files — superseded splits pruned"
    );
    assert!(
        remaining.len() < after_first,
        "GC actually removed orphaned splits (was {after_first}, now {})",
        remaining.len()
    );

    // And it's still restorable end-to-end after GC.
    let dest_root = tempfile::tempdir().unwrap();
    let dest_store = LocalIndexStore::open(dest_root.path()).unwrap();
    let dest = dest_store.shard_path(&id);
    restore(&store, prefix, &dest).await.unwrap();
    let restored = dest_store.open_shard(&id, &idx).unwrap();
    assert_eq!(
        restored
            .search_all(&Query::parse("body:beta").unwrap(), 10)
            .unwrap()
            .len(),
        1,
        "restored-after-GC shard still returns hits"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refresh_re_reads_the_manifest_on_a_mid_flight_404() {
    // task-149 / I7: a replica's refresh reads the manifest, then downloads each listed file. If a
    // concurrent backup's GC pruned a file this (now-stale) manifest still names, the download 404s;
    // refresh must re-read the manifest and retry rather than fail on the race. Here the ghost file
    // is *persistently* absent, so after the re-read+retry it surfaces cleanly as NotFound (proving
    // the retry path runs and terminates instead of hanging or panicking).
    let idx = docs_index();
    let id = ShardId::single("docs");
    let src_root = tempfile::tempdir().unwrap();
    let src_store = LocalIndexStore::open(src_root.path()).unwrap();
    let shard = src_store.create_shard(&id, &idx).unwrap();
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![doc("doc-1", "alpha")],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();

    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    let staging = src_root.path().join(".backup-staging");
    let prefix = "backups/docs/snap";
    backup(&shard, "docs", "docs", &staging, &store, prefix, None)
        .await
        .unwrap();

    // Simulate the race: rewrite the manifest to reference a file the store doesn't have.
    let mut manifest = read_manifest(&store, prefix).await.unwrap();
    manifest.files.push(FileEntry {
        path: "index/ghost.term".into(),
        len: 4,
    });
    store
        .write(
            &format!("{prefix}/manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .await
        .unwrap();

    let dest_root = tempfile::tempdir().unwrap();
    let dest = LocalIndexStore::open(dest_root.path())
        .unwrap()
        .shard_path(&id);
    let err = refresh(&store, prefix, &dest).await.unwrap_err();
    assert!(
        matches!(err, BackupError::Store(_)),
        "a persistently-missing file surfaces as an object-store error after the retry, got {err:?}"
    );
}

#[test]
fn pre_versioning_manifest_defaults_to_format_1() {
    // D30 foundations: manifests written before the `format` field existed carry none — they must
    // deserialize as format 1 (same defaulting pattern as `bundled`).
    let legacy = r#"{
        "index": "docs",
        "shard": "docs",
        "snapshot": 1,
        "checkpoint": null,
        "files": [],
        "definition_json": null,
        "created_ms": 0
    }"#;
    let m: Manifest = serde_json::from_str(legacy).unwrap();
    assert_eq!(m.format, 1, "pre-versioning manifests are format 1");
    assert!(!m.bundled);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn legacy_manifest_without_format_field_still_restores() {
    // D30 foundations: a real backup whose manifest.json predates the `format` field (strip it
    // from a fresh backup's manifest) must restore exactly as today.
    let idx = docs_index();
    let id = ShardId::single("docs");
    let src_root = tempfile::tempdir().unwrap();
    let src_store = LocalIndexStore::open(src_root.path()).unwrap();
    let shard = src_store.create_shard(&id, &idx).unwrap();
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![doc("doc-1", "alpha")],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();

    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    let staging = src_root.path().join(".backup-staging");
    let prefix = "backups/docs/snap";
    backup(&shard, "docs", "docs", &staging, &store, prefix, None)
        .await
        .unwrap();

    // Rewrite the manifest as a legacy (pre-versioning) one: drop the `format` field.
    let raw = store
        .read(&format!("{prefix}/manifest.json"))
        .await
        .unwrap();
    let mut json: serde_json::Value = serde_json::from_slice(&raw.to_vec()).unwrap();
    assert!(json.as_object_mut().unwrap().remove("format").is_some());
    store
        .write(
            &format!("{prefix}/manifest.json"),
            serde_json::to_vec(&json).unwrap(),
        )
        .await
        .unwrap();

    let dest_root = tempfile::tempdir().unwrap();
    let dest_store = LocalIndexStore::open(dest_root.path()).unwrap();
    let dest = dest_store.shard_path(&id);
    let m = restore(&store, prefix, &dest).await.unwrap();
    assert_eq!(m.format, 1, "legacy manifest deserializes as format 1");
    let restored = dest_store.open_shard(&id, &idx).unwrap();
    assert_eq!(
        restored
            .search_all(&Query::parse("body:alpha").unwrap(), 10)
            .unwrap()
            .len(),
        1,
        "legacy-manifest backup restores as today"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn newer_manifest_format_is_refused_cleanly() {
    // D30 foundations: a manifest stamped with a format newer than this binary's MANIFEST_FORMAT
    // (written by a newer GrowlerDB — e.g. after the D30 locator layers bump it) must refuse with
    // a clear UnsupportedFormat on every consumer, not mis-restore.
    let idx = docs_index();
    let id = ShardId::single("docs");
    let src_root = tempfile::tempdir().unwrap();
    let src_store = LocalIndexStore::open(src_root.path()).unwrap();
    let shard = src_store.create_shard(&id, &idx).unwrap();
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![doc("doc-1", "alpha")],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();

    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    let staging = src_root.path().join(".backup-staging");
    let prefix = "backups/docs/snap";
    let mut manifest = backup(&shard, "docs", "docs", &staging, &store, prefix, None)
        .await
        .unwrap();

    // Stamp a future format, as a newer GrowlerDB would.
    manifest.format = 99;
    store
        .write(
            &format!("{prefix}/manifest.json"),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .await
        .unwrap();

    // restore refuses…
    let dest = tempfile::tempdir().unwrap();
    let err = restore(&store, prefix, dest.path()).await.unwrap_err();
    assert!(
        matches!(
            err,
            BackupError::UnsupportedFormat {
                found: 99,
                supported: MANIFEST_FORMAT,
            }
        ),
        "restore refuses a newer manifest format, got {err:?}"
    );
    // …with a message that tells the operator what to do.
    let msg = err.to_string();
    assert!(
        msg.contains("format 99") && msg.contains("newer GrowlerDB"),
        "error names the formats and points at the version mismatch: {msg}"
    );
    // …and so do the other manifest consumers (revive is restore; refresh reads the same funnel).
    assert!(matches!(
        read_manifest(&store, prefix).await.unwrap_err(),
        BackupError::UnsupportedFormat { found: 99, .. }
    ));
    let dest2 = tempfile::tempdir().unwrap();
    assert!(matches!(
        refresh(&store, prefix, dest2.path()).await.unwrap_err(),
        BackupError::UnsupportedFormat { found: 99, .. }
    ));
    assert!(
        !dest.path().join("aux.redb").exists(),
        "refusal happens before any file lands"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_backup_is_a_not_found() {
    let backup_root = tempfile::tempdir().unwrap();
    let store = fs_store(backup_root.path()).unwrap();
    // The caller treats NotFound as "no backup → rebuild from Iceberg".
    let err = read_manifest(&store, "backups/absent").await.unwrap_err();
    assert!(matches!(err, BackupError::NotFound(_)));
}
