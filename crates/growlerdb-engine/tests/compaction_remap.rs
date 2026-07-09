//! **Compaction-under-hydration regression test** (task-184 slice 3 acceptance, [D30]):
//! an Iceberg compaction moves every row to new data files at new positions and deletes
//! the old files — with the background **re-map** on, hydration afterwards needs **zero**
//! lazy refreshes (`growlerdb_stale_locators_total` delta = 0) and every slot points at
//! the new files; with re-map off (**bitmap-only**), hydration is still correct via the
//! pass-2 fallback, the dead-file bitmap short-circuits every doomed pass-1 point read,
//! and the stale counter counts each fallback.
//!
//! There is no in-process Iceberg-catalog fixture, so the test builds the documented
//! minimal seam: real parquet data files on local disk (read through the same
//! column-projected reader production's re-map uses — `read_file_key_rows` over the
//! production `FileIO` stack), a real shard (ingest → `location.arr` slots), a simulated
//! `replace` (rewrite rows into a new parquet file, delete the old ones), and the diff
//! fed to the re-map entry point directly (`growlerdb_engine::remap_shard`). The
//! hydration harness mirrors the production `LookupService` flow at the same seams it
//! composes: `resolve_locators` → `apply_live_file_bitmap` → pass-1 verify against the
//! real parquet contents → pass-2 fallback by key → `refresh_locators` +
//! `sli::hydration` (which owns the stale counter).
//!
//! [D30]: ../../okf/system/decisions/d30-layered-locator.md

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc, ResolvedIndex,
    RowLocator, SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_engine::{apply_live_file_bitmap, remap_shard, resolve_locators};
use growlerdb_index::{LocalIndexStore, Shard, ShardId};
use growlerdb_source::{fs_file_io, read_file_key_rows};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

// ---------- fixture: a real shard over real parquet files ----------------------------

fn index() -> ResolvedIndex {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::Long),
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

fn key(id: i64) -> CompositeKey {
    CompositeKey::new(vec![], vec![("id".into(), Value::Int(id))])
}

/// Write `ids` as rows of a real parquet data file (`id` Int64 + `body` Utf8), row
/// groups of `group_size` — the shape of an Iceberg data file for the `docs` table.
fn write_parquet(path: &str, ids: &[i64], group_size: usize) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("body", DataType::Utf8, true),
    ]));
    let bodies: Vec<String> = ids
        .iter()
        .map(|id| format!("payload for row {id}"))
        .collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(bodies)),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_size(group_size)
        .build();
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(path).unwrap(), schema, Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

/// Ingest one doc per row of `(file, ids)` — locators exactly matching the parquet
/// layout, as the connector would record them.
fn ingest(shard: &Shard, file: &str, ids: &[i64], batch_id: &str, snapshot: i64) {
    let docs: Vec<LocatedDoc> = ids
        .iter()
        .enumerate()
        .map(|(pos, id)| {
            let mut fields = BTreeMap::new();
            fields.insert("id".to_string(), Value::Int(*id));
            fields.insert(
                "body".to_string(),
                Value::from(format!("payload for row {id}")),
            );
            LocatedDoc {
                doc: Document::new(key(*id), fields),
                iceberg_file: file.to_string(),
                row_position: pos as u64,
            }
        })
        .collect();
    IndexWriter::write(
        shard,
        &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(snapshot), batch_id),
    )
    .unwrap();
}

// ---------- the hydration harness (mirrors LookupService's get_by_key flow) ----------

/// What one simulated hydration did — the assertion surface.
struct Hydration {
    /// Keys authoritatively resolved (pass 1 or fallback).
    found: usize,
    /// Locators that needed the pass-2 fallback → written back via `refresh_locators`
    /// (exactly what feeds `growlerdb_stale_locators_total`).
    refreshed: usize,
    /// Pass-1 parquet reads actually attempted (the doomed-read count the bitmap
    /// short-circuit is supposed to zero out).
    pass1_reads: usize,
}

/// Hydrate `keys` against the table's current live `files`, mirroring the production
/// flow at the same seams `LookupService` composes: resolve → live-file bitmap →
/// pass-1 read-and-verify of each locator's `(file, position)` against the **real
/// parquet contents** (via the production column reader) → pass-2 fallback by key over
/// the live files → `refresh_locators` + `sli::hydration` (the stale counter's owner).
async fn hydrate(shard: &Shard, keys: &[CompositeKey], files: &[String]) -> Hydration {
    let fio = fs_file_io();
    let ident = ["id".to_string()];
    // The live table's rows, read through the production reader: file → rows, and the
    // pass-2 index key → (file, position).
    let mut rows_of: BTreeMap<String, Vec<(CompositeKey, u64)>> = BTreeMap::new();
    let mut by_key: BTreeMap<Vec<u8>, RowLocator> = BTreeMap::new();
    for file in files {
        let rows = read_file_key_rows(&fio, file, &[], &ident).await.unwrap();
        for (k, pos) in &rows {
            by_key.insert(
                k.encode(),
                RowLocator {
                    iceberg_file: file.clone(),
                    row_position: *pos,
                },
            );
        }
        rows_of.insert(file.clone(), rows);
    }

    let located = apply_live_file_bitmap(shard, resolve_locators(shard, keys).unwrap());
    let mut found = 0usize;
    let mut pass1_reads = 0usize;
    let mut refreshed: Vec<(CompositeKey, RowLocator)> = Vec::new();
    for (k, locator) in located {
        // Pass 1: a locator that survived the bitmap is read and key-verified.
        let verified = locator.as_ref().is_some_and(|loc| {
            let Some(rows) = rows_of.get(&loc.iceberg_file) else {
                return false; // file rewritten away — the read never happens
            };
            pass1_reads += 1;
            rows.iter()
                .any(|(rk, pos)| *pos == loc.row_position && *rk == k)
        });
        if verified {
            found += 1;
            continue;
        }
        // Pass 2: fall back to the current snapshot by key; a re-found row refreshes.
        if let Some(fresh) = by_key.get(&k.encode()) {
            found += 1;
            refreshed.push((k, fresh.clone()));
        }
    }
    shard.refresh_locators(&refreshed).unwrap();
    growlerdb_telemetry::sli::hydration(
        0.0,
        refreshed.len() as u64,
        keys.len() as u64,
        found as u64,
    );
    Hydration {
        found,
        refreshed: refreshed.len(),
        pass1_reads,
    }
}

/// The current value of `growlerdb_stale_locators_total` from the rendered metrics.
fn stale_locators_total() -> u64 {
    growlerdb_telemetry::metrics_text()
        .lines()
        .find(|l| l.starts_with("growlerdb_stale_locators_total"))
        .and_then(|l| l.split_whitespace().last())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn path_str(dir: &Path, name: &str) -> String {
    dir.join(name).to_str().unwrap().to_string()
}

// ---------- the regression test -------------------------------------------------------

const N: i64 = 200;

/// Both halves of the acceptance scenario run **sequentially in one test** because they
/// assert deltas of the same process-global stale counter.
#[tokio::test(flavor = "current_thread")]
async fn compaction_under_hydration_with_remap_and_bitmap_only() {
    growlerdb_telemetry::init("test"); // install the metrics recorder (idempotent)
    let keys: Vec<CompositeKey> = (0..N).map(key).collect();

    // ================= scenario 1: compaction + background re-map =================
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let f0 = path_str(data, "f0.parquet");
    let f1 = path_str(data, "f1.parquet");
    let ids0: Vec<i64> = (0..N / 2).collect();
    let ids1: Vec<i64> = (N / 2..N).collect();
    write_parquet(&f0, &ids0, 32);
    write_parquet(&f1, &ids1, 32);

    let store = LocalIndexStore::open(data).unwrap();
    let shard = store
        .create_shard(&ShardId::single("docs"), &index())
        .unwrap();
    ingest(&shard, &f0, &ids0, "b0", 1);
    ingest(&shard, &f1, &ids1, "b1", 2);

    // Baseline: every key hydrates through pass 1, nothing refreshes.
    let live = vec![f0.clone(), f1.clone()];
    let before = stale_locators_total();
    let base = hydrate(&shard, &keys, &live).await;
    assert_eq!(base.found, N as usize, "baseline: all keys hydrate");
    assert_eq!(base.refreshed, 0, "baseline: no locator is stale");
    assert_eq!(
        stale_locators_total(),
        before,
        "baseline: counter untouched"
    );

    // Simulated `rewrite_data_files`: all rows move into ONE new file in a different
    // order (every position changes), and the old files are gone.
    let compacted = path_str(data, "compacted.parquet");
    let mut shuffled: Vec<i64> = (0..N).map(|i| (i * 7 + 3) % N).collect();
    shuffled.dedup(); // 7 is coprime with 200 → already a permutation; keep it honest
    assert_eq!(shuffled.len(), N as usize);
    write_parquet(&compacted, &shuffled, 32);
    std::fs::remove_file(&f0).unwrap();
    std::fs::remove_file(&f1).unwrap();

    // The re-map, fed the diff directly (the seam remap_tick drives in production):
    // disappeared = {f0, f1}; moved = the added file's key column + positions, read
    // through the production column-projected scan.
    let moved: Vec<(CompositeKey, RowLocator)> =
        read_file_key_rows(&fs_file_io(), &compacted, &[], &["id".to_string()])
            .await
            .unwrap()
            .into_iter()
            .map(|(k, pos)| {
                (
                    k,
                    RowLocator {
                        iceberg_file: compacted.clone(),
                        row_position: pos,
                    },
                )
            })
            .collect();
    let (marked_dead, stats) = remap_shard(&shard, &[f0.clone(), f1.clone()], &moved).unwrap();
    assert_eq!(marked_dead, 2, "both rewritten files flagged dead");
    assert_eq!(stats.remapped, N as u64, "every row's slot re-pointed");
    assert_eq!(stats.skipped_no_live_doc, 0);
    assert_eq!(stats.skipped_already_live, 0);

    // Hydration after the re-map: everything resolves in pass 1 — ZERO lazy
    // refreshes, so the stale counter does not move.
    let before = stale_locators_total();
    let healed = hydrate(&shard, &keys, std::slice::from_ref(&compacted)).await;
    assert_eq!(
        healed.found, N as usize,
        "all keys resolve after compaction"
    );
    assert_eq!(
        healed.refreshed, 0,
        "re-map healed every slot — no per-read refresh tax"
    );
    assert_eq!(
        stale_locators_total(),
        before,
        "growlerdb_stale_locators_total delta = 0 under compaction WITH re-map"
    );
    // And the slots really point at the new file, at the rewritten positions.
    for (i, id) in shuffled.iter().enumerate() {
        let loc = shard.locate(&key(*id)).unwrap().expect("locator");
        assert_eq!(loc.iceberg_file, compacted);
        assert_eq!(loc.row_position, i as u64);
    }

    // ============== scenario 2: same compaction, bitmap only (no re-map) ==============
    let tmp = tempfile::tempdir().unwrap();
    let data = tmp.path();
    let f0 = path_str(data, "f0.parquet");
    let f1 = path_str(data, "f1.parquet");
    write_parquet(&f0, &ids0, 32);
    write_parquet(&f1, &ids1, 32);
    let store = LocalIndexStore::open(data).unwrap();
    let shard = store
        .create_shard(&ShardId::single("docs"), &index())
        .unwrap();
    ingest(&shard, &f0, &ids0, "b0", 1);
    ingest(&shard, &f1, &ids1, "b1", 2);

    let compacted = path_str(data, "compacted.parquet");
    write_parquet(&compacted, &shuffled, 32);
    std::fs::remove_file(&f0).unwrap();
    std::fs::remove_file(&f1).unwrap();

    // Only the live-file bitmap engages — no re-map.
    assert_eq!(shard.mark_files_dead(&[f0.clone(), f1.clone()]).unwrap(), 2);

    // The bitmap short-circuit: every locator points into a dead file, so locator
    // resolution strips them all — zero doomed pass-1 reads are even attempted.
    let located = apply_live_file_bitmap(&shard, resolve_locators(&shard, &keys).unwrap());
    assert!(
        located.iter().all(|(_, loc)| loc.is_none()),
        "dead-file locators go straight to the fallback"
    );

    // Hydration stays CORRECT via the fallback, and every key pays the lazy refresh
    // once — the stale counter counts all of them (it still works under the bitmap).
    let before = stale_locators_total();
    let lazy = hydrate(&shard, &keys, std::slice::from_ref(&compacted)).await;
    assert_eq!(
        lazy.found, N as usize,
        "bitmap-only hydration is still correct"
    );
    assert_eq!(lazy.pass1_reads, 0, "no wasted point read into dead files");
    assert_eq!(
        lazy.refreshed, N as usize,
        "every key fell back and refreshed"
    );
    assert_eq!(
        stale_locators_total(),
        before + N as u64,
        "the stale counter increments without the re-map"
    );

    // The lazy refreshes healed the slots too — the next hydration is clean again.
    let again = hydrate(&shard, &keys, &[compacted]).await;
    assert_eq!(again.found, N as usize);
    assert_eq!(again.refreshed, 0, "verify-and-refresh healed lazily");
}
