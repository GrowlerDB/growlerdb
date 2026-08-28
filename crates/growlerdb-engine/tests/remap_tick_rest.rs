//! **Full `remap_tick` heal against a real REST catalog** (Polaris + MinIO, no cloud).
//!
//! Reproduces the live compaction re-map bug: after an Iceberg `rewrite_data_files` (a replace
//! snapshot) the poller marks the old files dead but the heal step re-points ZERO slots, so
//! hydration falls to the slow pass-2 fallback forever (`growlerdb_locator_remap_events_total`
//! stays 0 even though `growlerdb_locator_dead_files` is high). This drives the exact production
//! seam — `read_documents` boot-build → real shard with real `(iceberg_file, row_position)`
//! locators → `remap_tick` — in two scenarios:
//!
//!   A. control: an uninterrupted poll heals every slot end-to-end.
//!   B. **the bug**: the poller marked the rewritten files dead, then the process restarted
//!      (in-memory `RemapState` lost) before the heal finished. `mark_files_dead` is PERSISTED but
//!      the heal is not, so the next tick must re-derive and finish the heal. Pre-fix it returned
//!      `Ok(None)` (stuck) because `disappeared` is computed from `interned_live_files()`, which
//!      excludes the already-dead files → the heal never ran again.
//!
//! Stand up the fixture first (all local; see the tests/fixtures/gen_http_logs.py):
//!   docker compose -p remaprepro up -d minio createbuckets polaris-db polaris-bootstrap polaris
//!   POLARIS=http://localhost:8181 bash deploy/compose/setup-polaris.sh
//! The catalog vends the `minio:9000` S3 endpoint; the host either resolves `minio` or overrides
//! `GROWLERDB_S3_ENDPOINT`. Provide shell commands that (re)create + compact growlerdb.http_logs:
//!   GDB_SETUP_CMD='<drop+create+append growlerdb.http_logs>' \
//!   GDB_COMPACT_CMD='<overwrite growlerdb.http_logs — replace all data files>' \
//!   GROWLERDB_S3_ENDPOINT=http://localhost:9000 \
//!     cargo test -p growlerdb-engine --test remap_tick_rest -- --ignored --nocapture

use std::collections::HashSet;
use std::sync::Arc;

use growlerdb_core::{
    CommitBatch, CompositeKey, IndexDefinition, IndexWriter, ResolvedIndex, SourceCheckpoint,
    SourceField, SourceSchema, SourceType,
};
use growlerdb_engine::{remap_tick, RemapState};
use growlerdb_index::{LocalIndexStore, Shard, ShardId};
use growlerdb_source::{IcebergConfig, IcebergReader};

const TABLE: &str = "growlerdb.http_logs";

fn http_logs_index() -> ResolvedIndex {
    let src = SourceSchema::new(
        vec![
            SourceField::new("request_id", SourceType::String),
            SourceField::new("trace_id", SourceType::String),
            SourceField::new("ts", SourceType::Long),
            SourceField::new("status", SourceType::String),
        ],
        vec![],                    // partition empty (http_logs key shape)
        vec!["request_id".into()], // string identifier key
    );
    IndexDefinition::from_yaml(
        "name: http_logs\n\
         source: { iceberg: { catalog: growlerdb, table: growlerdb.http_logs } }\n\
         mapping:\n  selection: EXPLICIT\n  fields:\n\
         \x20   - { path: request_id, type: KEYWORD }\n\
         \x20   - { path: trace_id, type: TEXT }\n\
         \x20   - { path: status, type: KEYWORD }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap()
}

fn run_cmd(var: &str) {
    let cmd = std::env::var(var).unwrap_or_else(|_| panic!("set {var}"));
    let out = std::process::Command::new("sh")
        .arg("-c")
        .arg(&cmd)
        .output()
        .expect("run command");
    println!(
        "[{var}] status={} stdout:\n{}\nstderr:\n{}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(out.status.success(), "{var} failed");
}

/// Boot-build a fresh shard over the table's current snapshot with REAL locators.
async fn boot_build(
    reader: &IcebergReader,
    index: &ResolvedIndex,
) -> (Arc<Shard>, usize, Vec<CompositeKey>) {
    let batch = reader
        .read_documents(TABLE, index)
        .await
        .expect("read_documents");
    let n = batch.docs.len();
    assert!(n > 0);
    let tmp = Box::leak(Box::new(tempfile::tempdir().unwrap())); // keep the dir for the test's life
    let store = LocalIndexStore::open(tmp.path()).unwrap();
    let shard: Arc<Shard> = store
        .create_shard(&ShardId::single("http_logs"), index)
        .unwrap()
        .into();
    let keys: Vec<CompositeKey> = batch.docs.iter().map(|d| d.doc.key.clone()).collect();
    IndexWriter::write(
        &*shard,
        &CommitBatch::from_upserts(
            batch.docs,
            SourceCheckpoint::iceberg(batch.snapshot_id),
            "boot",
        ),
    )
    .unwrap();
    (shard, n, keys)
}

fn assert_all_point_at(shard: &Shard, keys: &[CompositeKey], live: &HashSet<String>) {
    for k in keys {
        let loc = shard.locate(k).unwrap().expect("healed locator");
        assert!(
            live.contains(&loc.iceberg_file),
            "slot must point at a live compacted file, got {}",
            loc.iceberg_file
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Polaris+MinIO REST catalog + GDB_SETUP_CMD/GDB_COMPACT_CMD (see tests/fixtures/gen_http_logs.py)"]
async fn remap_tick_control_heals_after_compaction() {
    growlerdb_telemetry::init("test");
    let index = http_logs_index();
    let key_fields = (
        index.key.partition_fields.as_slice(),
        index.key.identifier_fields.as_slice(),
    );
    let reader = IcebergReader::connect(&IcebergConfig::from_env())
        .await
        .unwrap();

    run_cmd("GDB_SETUP_CMD");
    let (shard, n, keys) = boot_build(&reader, &index).await;

    let pre_plan = reader.current_plan(TABLE).await.unwrap();
    let old_files: HashSet<String> = pre_plan
        .tasks
        .iter()
        .map(|t| t.data_file_path.clone())
        .collect();
    assert_eq!(
        shard
            .interned_live_files()
            .into_iter()
            .collect::<HashSet<_>>(),
        old_files,
        "shard interned exactly the plan's data files (path format matches)"
    );

    let mut state = RemapState::default();
    let pre = remap_tick(
        &reader,
        TABLE,
        key_fields,
        std::slice::from_ref(&shard),
        &mut state,
    )
    .await
    .unwrap();
    assert!(pre.is_none(), "no rewrite yet → Ok(None)");

    run_cmd("GDB_COMPACT_CMD");
    let post = remap_tick(
        &reader,
        TABLE,
        key_fields,
        std::slice::from_ref(&shard),
        &mut state,
    )
    .await
    .unwrap()
    .expect("Ok(Some): rewrite detected + healed");
    println!("control heal: {post:?}");
    assert_eq!(post.files_marked_dead as usize, old_files.len());
    assert_eq!(post.stats.remapped as usize, n, "every slot re-pointed");

    let new_files: HashSet<String> = reader
        .current_plan(TABLE)
        .await
        .unwrap()
        .tasks
        .iter()
        .map(|t| t.data_file_path.clone())
        .collect();
    assert!(new_files.is_disjoint(&old_files));
    assert_all_point_at(&shard, &keys, &new_files);
    println!("PASS control: uninterrupted poll heals end-to-end");
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a local Polaris+MinIO REST catalog + GDB_SETUP_CMD/GDB_COMPACT_CMD (see tests/fixtures/gen_http_logs.py)"]
async fn remap_tick_recovers_after_interrupted_heal() {
    growlerdb_telemetry::init("test");
    let index = http_logs_index();
    let key_fields = (
        index.key.partition_fields.as_slice(),
        index.key.identifier_fields.as_slice(),
    );
    let reader = IcebergReader::connect(&IcebergConfig::from_env())
        .await
        .unwrap();

    run_cmd("GDB_SETUP_CMD");
    let (shard, n, keys) = boot_build(&reader, &index).await;
    let old_files: Vec<String> = shard.interned_live_files();

    // The rewrite happens.
    run_cmd("GDB_COMPACT_CMD");

    // Simulate the poller that marked the rewritten files dead (this is PERSISTED — remap_tick's
    // own line 139) and then RESTARTED before the heal finished (in-memory RemapState is lost).
    let marked = shard.mark_files_dead(&old_files).unwrap();
    assert_eq!(marked as usize, old_files.len(), "old files persisted dead");
    assert_eq!(shard.dead_file_count() as usize, old_files.len());

    // A brand-new process: fresh state, no memory of the previous plan.
    let mut state = RemapState::default();
    let post = remap_tick(
        &reader,
        TABLE,
        key_fields,
        std::slice::from_ref(&shard),
        &mut state,
    )
    .await
    .expect("tick must not error");
    println!("recovery tick outcome: {post:?}");

    // THE BUG (pre-fix): `disappeared` = interned_live_files ∩ !current is EMPTY (the old files are
    // already dead → excluded), so the tick takes the Ok(None) early return and the heal never runs
    // — slots stay stale forever, hydration is stuck on pass-2. locator_remap_events_total stays 0.
    let post = post.expect(
        "Ok(Some): the heal must re-derive from the persisted dead set after a restart (pre-fix: Ok(None), stuck)",
    );
    assert_eq!(
        post.stats.remapped as usize, n,
        "every slot re-pointed after the interrupted-heal restart"
    );
    assert_eq!(
        post.stats.skipped_no_live_doc, 0,
        "no key-encoding mismatch"
    );

    let new_files: HashSet<String> = reader
        .current_plan(TABLE)
        .await
        .unwrap()
        .tasks
        .iter()
        .map(|t| t.data_file_path.clone())
        .collect();
    assert_all_point_at(&shard, &keys, &new_files);
    println!("PASS recovery: remap_tick finishes the heal after a restart");
}
