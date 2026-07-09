//! The embedded **engine façade** ([embedded topology]) — wires source → index →
//! search → hydrate into one in-process unit, driven by the CLI (task-9). No
//! server, auth, sharding, or UI; those are M2/M3.
//!
//! [embedded topology]: ../../../design/06-service-architecture.md

use std::path::PathBuf;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use growlerdb_core::{
    CommitBatch, CompositeKey, Hit, HydratedRow, IcebergSource, IndexDefinition, IndexReader,
    IndexWriter, KeySpec, LocatedDoc, Mapping, Projection, ResolvedIndex, ScanMode, SearchParams,
    ShardRouter, Snapshot, Source, SourceCheckpoint, Value,
};
use growlerdb_index::{LocalIndexStore, Shard, ShardId};
use growlerdb_source::{IcebergConfig, IcebergReader};

use crate::{hydrate, EngineError};

/// Result of an append fast-path [`sync`](Engine::sync).
#[derive(Debug)]
pub struct SyncOutcome {
    /// Documents appended since the prior checkpoint.
    pub added: usize,
    /// The committed snapshot after the append.
    pub snapshot: Snapshot,
    /// The source snapshot the index now reflects.
    pub checkpoint: i64,
}

/// Result of a drift [`reconcile`](Engine::reconcile): how the index differed from
/// the source's current state and what was repaired.
#[derive(Debug, PartialEq, Eq)]
pub struct DriftReport {
    /// Live indexed docs before reconciliation (in the reconciled scope).
    pub index_count: usize,
    /// Docs the source currently holds (in the reconciled scope).
    pub source_count: usize,
    /// Stale docs removed (indexed but absent from the source).
    pub deleted: usize,
    /// Missing docs re-indexed (in the source but absent from the index).
    pub reindexed: usize,
    /// The stale-delete pass was **skipped** because a concurrent ingest advanced the shard during
    /// the source scan (task-195 TOCTOU guard). Missing-repair still ran; the next reconcile retries
    /// the deletes once the shard is momentarily quiescent.
    pub deletes_skipped: bool,
}

impl DriftReport {
    /// Whether the index already matched the source (nothing repaired).
    pub fn is_clean(&self) -> bool {
        self.deleted == 0 && self.reindexed == 0
    }
}

/// Max affected keys to log on a nonzero repair (task-195) — bounded so a large drift can't flood
/// the log; the counts (and the `drift_*` metrics) carry the full magnitude.
const DRIFT_LOG_KEYS: usize = 20;

/// Reconcile a shard scope against the source's current `source_docs` (task-18 drift
/// repair): drop indexed keys the source no longer has (via partition reconciliation)
/// and re-index source docs the index is missing. Pure over the store + the provided
/// source docs, so it is exercised without a live catalog. `partition` empty ⇒ the
/// whole index.
///
/// For the **sharded** backstop (task-195) the caller filters `source_docs` to the keys this shard
/// owns before calling, so the stale-set (indexed keys absent from `source_docs`) can't sweep away
/// another shard's keys — the placement-destroying bug of the old whole-table reconcile.
/// `expected_checkpoint` is the shard's checkpoint captured **before** the source scan that produced
/// `source_docs`; it fences the stale-delete against a concurrent ingest (task-195 TOCTOU guard).
/// `None` opts out for callers with no concurrent writer (CLI/tests).
pub(crate) fn apply_drift(
    shard: &Shard,
    partition: &[(String, Value)],
    source_docs: Vec<LocatedDoc>,
    expected_checkpoint: Option<SourceCheckpoint>,
) -> Result<DriftReport, EngineError> {
    let index_count = shard.key_count(partition)?;
    let source_count = source_docs.len();

    // Stale: indexed keys in this scope that the source no longer carries.
    let source_keys: Vec<CompositeKey> = source_docs.iter().map(|d| d.doc.key.clone()).collect();
    let delete_outcome =
        shard.reconcile_partition(partition, &source_keys, expected_checkpoint.as_ref())?;
    let deleted = delete_outcome.deleted;
    let deletes_skipped = delete_outcome.skipped_concurrent_write;

    // Missing: source docs the index doesn't hold → re-index as upserts. The batch id
    // is derived from the missing key set, so repairing the *same* drift twice is a
    // no-op while a *different* repair is never wrongly deduped.
    let mut missing = Vec::new();
    for doc in source_docs {
        if !shard.contains_key(&doc.doc.key)? {
            missing.push(doc);
        }
    }
    let reindexed = missing.len();
    // A nonzero repair is a real index↔source divergence — log it (bounded) so the affected keys are
    // recoverable from logs, not just a metric count (task-195). Clean runs stay silent.
    if deleted > 0 || reindexed > 0 || deletes_skipped {
        let missing_keys: Vec<String> = missing
            .iter()
            .take(DRIFT_LOG_KEYS)
            .map(|d| format!("{:?}", d.doc.key))
            .collect();
        tracing::warn!(
            deleted,
            reindexed,
            deletes_skipped,
            index_count,
            source_count,
            missing_sample = ?missing_keys,
            truncated = reindexed > DRIFT_LOG_KEYS,
            "reconcile repaired drift"
        );
    }
    if !missing.is_empty() {
        let mut hasher = DefaultHasher::new();
        for doc in &missing {
            doc.doc.key.encode().hash(&mut hasher);
        }
        let checkpoint = shard
            .current_checkpoint()?
            .unwrap_or(SourceCheckpoint::iceberg(0));
        IndexWriter::write(
            shard,
            &CommitBatch::from_upserts(
                missing,
                checkpoint,
                format!("reconcile-{:x}", hasher.finish()),
            ),
        )?;
    }

    Ok(DriftReport {
        index_count,
        source_count,
        deleted,
        reindexed,
        deletes_skipped,
    })
}

/// Result of building an index.
#[derive(Debug)]
pub struct IndexOutcome {
    /// The index name (used for later `search`).
    pub name: String,
    /// The committed snapshot.
    pub snapshot: Snapshot,
    /// Number of documents indexed.
    pub doc_count: usize,
}

/// Result of a search: ranked hits and, if requested, hydrated rows (aligned to
/// `hits` by position).
#[derive(Debug)]
pub struct SearchOutcome {
    /// Ranked coordinates + scores.
    pub hits: Vec<Hit>,
    /// Authoritative rows, if `--hydrate` was requested.
    pub rows: Option<Vec<HydratedRow>>,
}

/// The embedded engine: a local index store under `root`, plus the Iceberg
/// connection settings used for indexing and hydration.
pub struct Engine {
    store: LocalIndexStore,
    root: PathBuf,
    iceberg: IcebergConfig,
}

impl Engine {
    /// Open (creating if absent) an engine rooted at `root`, reading from the
    /// Iceberg catalog described by `iceberg`.
    pub fn open(root: impl Into<PathBuf>, iceberg: IcebergConfig) -> Result<Self, EngineError> {
        let root = root.into();
        let store = LocalIndexStore::open(&root)?;
        Ok(Self {
            store,
            root,
            iceberg,
        })
    }

    /// Build (or rebuild) a local index from `table`. With `def_yaml`, the index
    /// definition is authored; otherwise a default is derived (auto-map all
    /// fields; identifier from the source, else an `id` column, else the sole
    /// column). Re-running with the same Iceberg snapshot is a **no-op**.
    pub async fn index(
        &self,
        table: &str,
        def_yaml: Option<&str>,
        name: Option<&str>,
    ) -> Result<IndexOutcome, EngineError> {
        self.index_shard(table, def_yaml, name, 1, 0).await
    }

    /// Build **one ordinal shard** of an index from `table` (task-77): like [`index`](Self::index),
    /// but keeps only the documents shard `shard_ordinal` of `shards` owns under the index's routing
    /// strategy. This is how a node in a sharded cluster builds *its* partition from source — so a
    /// broadcast search over the shards sees each document exactly once (no cross-shard duplicates).
    /// `shards <= 1` is a normal full build. Errors on a windowed index (it shards by time window).
    pub async fn index_shard(
        &self,
        table: &str,
        def_yaml: Option<&str>,
        name: Option<&str>,
        shards: u32,
        shard_ordinal: u32,
    ) -> Result<IndexOutcome, EngineError> {
        let reader = IcebergReader::connect(&self.iceberg).await?;
        let source_schema = reader.read_source_schema(table).await?;

        let resolved = match def_yaml {
            Some(yaml) => IndexDefinition::from_yaml(yaml)?.resolve(&source_schema)?,
            None => self
                .default_definition(table, name, &source_schema)?
                .resolve(&source_schema)?,
        };
        let index_name = resolved.name.clone();

        // The per-shard build filter: keep only docs this ordinal owns. None for a single-shard
        // (full) build. Windowed indexes shard by time window, so ordinal sharding doesn't apply.
        let filter = shard_build_filter(&resolved, shards, shard_ordinal)?;

        self.persist_definition(&index_name, &resolved)?;

        let (snapshot, doc_count) = self
            .build_from_source(&reader, table, &resolved, filter.as_ref())
            .await?;
        Ok(IndexOutcome {
            name: index_name,
            snapshot,
            doc_count,
        })
    }

    /// Persist an index's **definition only** — resolve `table`'s schema into `index.json` and write
    /// it, building **no shards/windows** (task-223). This is how a **windowed** node starts truly
    /// empty on k8s: `serve` needs the resolved `index.json` on disk, but a windowed node must NOT
    /// batch-build windows from the source — with no ordinal filter every node would build *all*
    /// windows locally, replicating them across the pool and defeating control-plane placement (the
    /// windows a node serves are the ones the connector streams to it). Reads the source schema (so
    /// the definition resolves) but never reads rows. Returns an [`IndexOutcome`] with `doc_count = 0`.
    pub async fn define_index(
        &self,
        table: &str,
        def_yaml: Option<&str>,
        name: Option<&str>,
    ) -> Result<IndexOutcome, EngineError> {
        let reader = IcebergReader::connect(&self.iceberg).await?;
        let source_schema = reader.read_source_schema(table).await?;
        let resolved = match def_yaml {
            Some(yaml) => IndexDefinition::from_yaml(yaml)?.resolve(&source_schema)?,
            None => self
                .default_definition(table, name, &source_schema)?
                .resolve(&source_schema)?,
        };
        let index_name = resolved.name.clone();
        self.persist_definition(&index_name, &resolved)?;
        Ok(IndexOutcome {
            name: index_name,
            snapshot: Snapshot(0),
            doc_count: 0,
        })
    }

    /// Read `table` into the index. **Non-windowed:** stream the source in bounded chunks into the
    /// single shard, so peak memory is independent of table size (task-84 — the old whole-table read
    /// OOM'd on large tables). **Windowed:** buffer + [`write_windowed`](LocalIndexStore::write_windowed)
    /// (streamed windowing is a follow-up). Returns `(snapshot, doc_count)`.
    async fn build_from_source(
        &self,
        reader: &IcebergReader,
        table: &str,
        resolved: &ResolvedIndex,
        // Per-shard build filter (task-77): keep only docs this `(router, ordinal)` owns. `None`
        // for a normal full build.
        shard_filter: Option<&(ShardRouter, u32)>,
    ) -> Result<(Snapshot, usize), EngineError> {
        let (snapshot, doc_count) = if resolved.windowing.is_some() {
            let batch = reader.read_documents(table, resolved).await?;
            let doc_count = batch.docs.len();
            let snapshot = self.write_build(
                resolved,
                CommitBatch::from_upserts(
                    batch.docs,
                    SourceCheckpoint::iceberg(batch.snapshot_id),
                    format!("snapshot-{}", batch.snapshot_id),
                ),
            )?;
            (snapshot, doc_count)
        } else {
            // Stream into the single shard: one bounded chunk → one commit, so memory stays flat.
            let shard = self
                .store
                .create_shard(&ShardId::single(&resolved.name), resolved)?;
            let (snapshot_id, _) = reader.current_snapshot(table).await?;
            let mut chunk = 0u64;
            // Count docs **written** (post-filter), not source rows read — so a sharded build
            // reports its shard's doc count, not the whole table's (task-77).
            let mut written = 0usize;
            reader
                .read_documents_streamed(table, resolved, |mut docs| {
                    // Sharded build (task-77): drop docs this shard doesn't own, so the shard holds
                    // only its partition and a broadcast search can't double-count.
                    if let Some((router, ordinal)) = shard_filter {
                        docs.retain(|d| router.owns(&d.doc.key, *ordinal));
                    }
                    written += docs.len();
                    chunk += 1;
                    let batch = CommitBatch::from_upserts(
                        docs,
                        SourceCheckpoint::iceberg(snapshot_id),
                        format!("snapshot-{snapshot_id}-{chunk}"),
                    );
                    IndexWriter::write(&shard, &batch)
                        .map(|_| ())
                        .map_err(|e| e.to_string())
                })
                .await?;
            // Anchor the built index to its source's Iceberg `table-uuid` (task-114) so a later
            // `serve` can detect a drop+recreate of the source and refuse to serve stale data.
            shard.set_source_uuid(&reader.table_uuid(table).await?)?;
            (Snapshot(shard.current_snapshot()?), written)
        };

        // Never silently commit an empty index from a non-empty source (task-85): if we read 0 docs
        // but the snapshot reports rows, the read is broken (e.g. a delete in the table's history).
        // Skipped for a sharded build, where a shard may legitimately own no docs.
        if doc_count == 0 && shard_filter.is_none() {
            if let Some(records) = reader.current_snapshot_records(table).await? {
                if records > 0 {
                    return Err(EngineError::EmptyReadFromNonEmptySource {
                        table: table.to_string(),
                        records,
                    });
                }
            }
        }
        Ok((snapshot, doc_count))
    }

    /// Write a freshly-read build/rebuild batch to the index: one single shard, or — for a
    /// **windowed** index — per-window shards via the time-window router
    /// ([`write_windowed`](LocalIndexStore::write_windowed), task-81). Returns a representative
    /// committed [`Snapshot`]: the single shard's, or the latest window's (windowed shards commit
    /// independently, so the outcome's lone snapshot is informational — the per-window state is the
    /// source of truth). The single shard is created lazily here so a windowed build never leaves an
    /// empty `ShardId::single` dir beside the `w<window>` shards.
    fn write_build(
        &self,
        resolved: &ResolvedIndex,
        commit: CommitBatch,
    ) -> Result<Snapshot, EngineError> {
        if resolved.windowing.is_some() {
            let windows = self.store.write_windowed(resolved, &commit)?;
            let snap = match windows.iter().max() {
                Some(&w) => self
                    .store
                    .open_shard(&ShardId::window(&resolved.name, w), resolved)?
                    .current_snapshot()?,
                None => 0,
            };
            Ok(Snapshot(snap))
        } else {
            let shard = self
                .store
                .create_shard(&ShardId::single(&resolved.name), resolved)?;
            Ok(IndexWriter::write(&shard, &commit)?)
        }
    }

    /// **Append fast-path sync** (task-18): for an `APPEND_FAST_PATH` index, read only
    /// the files added since the committed checkpoint and index them (no delete/update
    /// handling). Cheaper than the changelog scan for immutable tables; resumes from
    /// the shard's checkpoint. Errors on a changelog-mode index — that's the
    /// connector's job, not this incremental path.
    pub async fn sync(&self, index: &str) -> Result<SyncOutcome, EngineError> {
        let resolved = self.load_definition(index)?;
        if scan_mode(&resolved) != ScanMode::AppendFastPath {
            return Err(EngineError::NotAppendFastPath(index.to_string()));
        }
        let shard = self.store.open_shard(&ShardId::single(index), &resolved)?;
        let table = source_table(&resolved);
        let since = shard.current_checkpoint()?.map(|cp| cp.snapshot_id());

        let reader = IcebergReader::connect(&self.iceberg).await?;
        let batch = reader
            .read_documents_appended_since(&table, &resolved, since)
            .await?;
        let added = batch.docs.len();
        let checkpoint = batch.snapshot_id;
        let snapshot = IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                batch.docs,
                SourceCheckpoint::iceberg(checkpoint),
                format!("append-{checkpoint}"),
            ),
        )?;
        Ok(SyncOutcome {
            added,
            snapshot,
            checkpoint,
        })
    }

    /// **Drift reconciliation** (task-18): compare the index against the source's
    /// current snapshot and repair discrepancies — delete indexed keys the source
    /// dropped, re-index keys it gained. The periodic backstop that keeps the index
    /// consistent with Iceberg regardless of sync mode or delete encoding. M1
    /// reconciles the whole index; per-partition scoping is the scaling refinement.
    pub async fn reconcile(&self, index: &str) -> Result<DriftReport, EngineError> {
        let resolved = self.load_definition(index)?;
        let shard = self.store.open_shard(&ShardId::single(index), &resolved)?;
        let table = source_table(&resolved);
        // Capture the checkpoint before the source read so the stale-delete can fence against a
        // concurrent ingest (task-195 TOCTOU guard); harmless for the CLI single-shard path.
        let expected_checkpoint = shard.current_checkpoint()?;
        let reader = IcebergReader::connect(&self.iceberg).await?;
        let source = reader.read_documents(&table, &resolved).await?;
        apply_drift(&shard, &[], source.docs, expected_checkpoint)
    }

    /// **Rebuild from Iceberg** (task-18): the hard-reset backstop — drop the index's
    /// on-disk state and re-index the current snapshot from scratch, reusing the
    /// persisted definition. Always available because the index is rebuildable from
    /// the source.
    pub async fn rebuild(&self, index: &str) -> Result<IndexOutcome, EngineError> {
        let resolved = self.load_definition(index)?;
        let table = source_table(&resolved);

        // Drop all shard state (segments + redb + persisted def), then re-create on write.
        std::fs::remove_dir_all(self.root.join(index))?;
        self.persist_definition(index, &resolved)?;

        let reader = IcebergReader::connect(&self.iceberg).await?;
        let (snapshot, doc_count) = self
            .build_from_source(&reader, &table, &resolved, None)
            .await?;
        Ok(IndexOutcome {
            name: index.to_string(),
            snapshot,
            doc_count,
        })
    }

    /// Search `index`, returning ranked coordinates + scores. When `hydrate` is
    /// set, also fetch the authoritative rows (projected by `projection`) from
    /// Iceberg via the locator.
    pub async fn search(
        &self,
        index: &str,
        query: &str,
        k: usize,
        hydrate: bool,
        projection: Projection,
    ) -> Result<SearchOutcome, EngineError> {
        let resolved = self.load_definition(index)?;
        let shard = self.store.open_shard(&ShardId::single(index), &resolved)?;

        let params = SearchParams::parse(query, k)?;
        let hits = IndexReader::search(&shard, &params)?.hits;

        let rows = if hydrate {
            let keys: Vec<CompositeKey> = hits.iter().map(|h| h.key.clone()).collect();
            let table = source_table(&resolved);
            let reader = IcebergReader::connect(&self.iceberg).await?;
            Some(hydrate::get_by_key(&shard, &reader, &table, &keys, &projection).await?)
        } else {
            None
        };

        Ok(SearchOutcome { hits, rows })
    }

    /// Build a default index definition for `table` (auto-map all fields).
    fn default_definition(
        &self,
        table: &str,
        name: Option<&str>,
        source: &growlerdb_core::SourceSchema,
    ) -> Result<IndexDefinition, EngineError> {
        let identifier_fields = if !source.identifier_fields.is_empty() {
            source.identifier_fields.clone()
        } else if source.has_field("id") {
            vec!["id".to_string()]
        } else if source.fields.len() == 1 {
            vec![source.fields[0].path.clone()]
        } else {
            return Err(EngineError::NoIdentifier);
        };

        let name = name
            .map(str::to_string)
            .unwrap_or_else(|| table.rsplit('.').next().unwrap_or(table).to_string());

        Ok(IndexDefinition {
            name,
            source: Source::Iceberg(IcebergSource {
                catalog: self.iceberg.warehouse.clone(),
                table: table.to_string(),
                scan: ScanMode::default(),
            }),
            key: KeySpec {
                partition_fields: source.partition_fields.clone(),
                identifier_fields,
            },
            mapping: Mapping::default(),
            shard_count: 1,     // the embedded engine builds a single shard
            tenant_field: None, // auto-mapped indexes aren't tenant-scoped (set it explicitly)
            windowing: None,    // auto-mapped indexes aren't time-windowed (set it explicitly)
            // The universal default (task-184 / D30); PREDICATE is an explicit choice.
            location_strategy: growlerdb_core::LocationStrategy::default(),
        })
    }

    /// Path to the persisted definition for `index`.
    fn definition_path(&self, index: &str) -> PathBuf {
        self.root.join(index).join("index.json")
    }

    /// Persist a resolved definition so `search` can reopen the shard later.
    fn persist_definition(&self, index: &str, resolved: &ResolvedIndex) -> Result<(), EngineError> {
        let path = self.definition_path(index);
        // `index.json` lives at the index root, which may not exist yet — the (windowed) build now
        // persists the definition *before* creating any shard dir (task-81 4e-1). Ensure the parent.
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(path, serde_json::to_vec_pretty(resolved)?)?;
        Ok(())
    }

    /// Load a previously persisted definition.
    fn load_definition(&self, index: &str) -> Result<ResolvedIndex, EngineError> {
        let path = self.definition_path(index);
        if !path.exists() {
            return Err(EngineError::NotIndexed(index.to_string()));
        }
        Ok(serde_json::from_slice(&std::fs::read(path)?)?)
    }
}

/// The source table identifier of a resolved index.
fn source_table(resolved: &ResolvedIndex) -> String {
    match &resolved.source {
        Source::Iceberg(s) => s.table.clone(),
    }
}

/// The per-shard build filter (task-77) for building shard `ordinal` of `shards`: a `(router,
/// ordinal)` whose [`owns`](ShardRouter::owns) keeps only this shard's docs. `shards <= 1` ⇒ `None`
/// (full build). Errors on a windowed index — it shards by time window, not by ordinal.
fn shard_build_filter(
    resolved: &ResolvedIndex,
    shards: u32,
    ordinal: u32,
) -> Result<Option<(ShardRouter, u32)>, EngineError> {
    if shards <= 1 {
        return Ok(None);
    }
    if resolved.windowing.is_some() {
        return Err(EngineError::ShardingWindowedUnsupported(
            resolved.name.clone(),
        ));
    }
    Ok(Some((
        ShardRouter::new(shards, resolved.routing_strategy()),
        ordinal,
    )))
}

/// The scan mode of a resolved index's source.
fn scan_mode(resolved: &ResolvedIndex) -> ScanMode {
    match &resolved.source {
        Source::Iceberg(s) => s.scan,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{
        Document, IndexDefinition, LocatedDoc, SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;
    use std::path::Path;

    fn resolved() -> ResolvedIndex {
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

    #[test]
    fn shard_build_filter_partitions_or_full_builds() {
        let r = resolved();
        // Single shard ⇒ no filter (a normal full build).
        assert!(shard_build_filter(&r, 1, 0).unwrap().is_none());

        // Multi-shard ⇒ a filter that keeps exactly this ordinal's docs.
        let (router, ordinal) = shard_build_filter(&r, 3, 2).unwrap().unwrap();
        assert_eq!(ordinal, 2);
        assert_eq!(router.shards(), 3);

        // Across the shards the filters partition the key space — every key is built by exactly
        // one shard, so a broadcast search sees it once (no cross-shard duplicates, no gaps).
        for id in ["a", "b", "c", "d", "e", "f", "g", "h"] {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let owners: Vec<u32> = (0..3)
                .filter(|o| {
                    let (rt, ord) = shard_build_filter(&r, 3, *o).unwrap().unwrap();
                    rt.owns(&key, ord)
                })
                .collect();
            assert_eq!(
                owners.len(),
                1,
                "key {id} must be built by exactly one shard"
            );
        }
    }

    /// Set up an indexed shard the way `index()` would, but without Iceberg, so
    /// the local `search()` path can be exercised in-process.
    fn seed_index(root: &Path) {
        let store = LocalIndexStore::open(root).unwrap();
        let resolved = resolved();
        let shard = store
            .create_shard(&ShardId::single("docs"), &resolved)
            .unwrap();
        std::fs::write(
            root.join("docs").join("index.json"),
            serde_json::to_vec_pretty(&resolved).unwrap(),
        )
        .unwrap();

        let doc = |id: &str, body: &str, pos: u64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut fields = BTreeMap::new();
            fields.insert("id".to_string(), Value::from(id));
            fields.insert("body".to_string(), Value::from(body));
            LocatedDoc {
                doc: Document::new(key, fields),
                iceberg_file: "data/f0.parquet".into(),
                row_position: pos,
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![
                    doc("doc-1", "hello world welcome", 0),
                    doc("doc-2", "full text search over iceberg", 1),
                ],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn search_returns_ranked_coordinates() {
        let tmp = tempfile::tempdir().unwrap();
        seed_index(tmp.path());
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();

        let out = engine
            .search("docs", "body:search", 10, false, Projection::All)
            .await
            .unwrap();
        assert_eq!(out.hits.len(), 1);
        assert_eq!(out.hits[0].key.get("id"), Some(&Value::from("doc-2")));
        assert!(out.rows.is_none());
    }

    fn doc(id: &str, body: &str, pos: u64) -> LocatedDoc {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), Value::from(id));
        fields.insert("body".to_string(), Value::from(body));
        LocatedDoc {
            doc: Document::new(key, fields),
            iceberg_file: "data/f0.parquet".into(),
            row_position: pos,
        }
    }

    fn resolved_windowed() -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("ingest", SourceType::Long),
                SourceField::new("event", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            "name: events\nsource: { iceberg: { catalog: g, table: g.events } }\nwindowing: { field: ingest, granularity: daily, event_time_field: event }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: ingest, format: epoch_us, fast: true }, { path: event, format: epoch_us, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn wdoc(id: &str, ingest: i64, event: i64) -> LocatedDoc {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), Value::from(id));
        fields.insert("ingest".to_string(), Value::Int(ingest));
        fields.insert("event".to_string(), Value::Int(event));
        LocatedDoc {
            doc: Document::new(key, fields),
            iceberg_file: "f".into(),
            row_position: 0,
        }
    }

    #[test]
    fn write_build_routes_windowed_to_window_shards_else_single() {
        const DAY: i64 = 86_400_000_000; // micros (canonical window scale, task-116)
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();

        // Windowed build → per-window shards, no empty single shard, representative snapshot.
        let win = resolved_windowed();
        let snap = engine
            .write_build(
                &win,
                CommitBatch::from_upserts(
                    vec![
                        wdoc("a", 10 * DAY + 1, 10 * DAY),
                        wdoc("b", 11 * DAY + 1, 11 * DAY),
                    ],
                    SourceCheckpoint::iceberg(1),
                    "b1",
                ),
            )
            .unwrap();
        assert_eq!(
            engine.store.window_shards("events").unwrap(),
            vec![10 * DAY, 11 * DAY]
        );
        assert!(
            !engine.store.shard_path(&ShardId::single("events")).exists(),
            "windowed build must not leave an empty single shard"
        );
        assert!(snap.0 > 0, "representative snapshot from the latest window");

        // Non-windowed build → one single shard, no window dirs.
        engine
            .write_build(
                &resolved(),
                CommitBatch::from_upserts(
                    vec![doc("d1", "hello", 0)],
                    SourceCheckpoint::iceberg(1),
                    "b2",
                ),
            )
            .unwrap();
        assert!(engine.store.window_shards("docs").unwrap().is_empty());
        assert!(engine.store.shard_path(&ShardId::single("docs")).exists());
    }

    #[test]
    fn persist_definition_creates_index_root_when_absent() {
        // index()/rebuild() persist the definition *before* any shard dir is created (4e-1), so the
        // index root may not exist yet — persist_definition must create it (regression for the e2e
        // `NotFound` writing index.json onto a missing dir).
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();
        engine.persist_definition("fresh", &resolved()).unwrap();
        assert!(tmp.path().join("fresh").join("index.json").exists());
    }

    #[test]
    fn chunked_writes_equal_a_single_batch() {
        // The streamed build (task-84) writes the source in many bounded chunks (one commit each,
        // unique batch ids) instead of one giant batch. Assert the resulting index is identical:
        // every doc searchable across the per-chunk segments, same counts as a single write.
        let docs: Vec<LocatedDoc> = (0..30)
            .map(|i| {
                let body = if i % 3 == 0 { "alpha beta" } else { "beta" };
                doc(&format!("d{i}"), body, i)
            })
            .collect();
        let count = |shard: &Shard, q: &str| {
            shard
                .search_all(&growlerdb_core::Query::parse(q).unwrap(), 100)
                .unwrap()
                .len()
        };

        // One batch.
        let t1 = tempfile::tempdir().unwrap();
        let s1 = LocalIndexStore::open(t1.path()).unwrap();
        let shard1 = s1
            .create_shard(&ShardId::single("docs"), &resolved())
            .unwrap();
        IndexWriter::write(
            &shard1,
            &CommitBatch::from_upserts(docs.clone(), SourceCheckpoint::iceberg(1), "snapshot-1"),
        )
        .unwrap();

        // Same docs in three chunks of ten (the streamed path), unique batch id per chunk.
        let t2 = tempfile::tempdir().unwrap();
        let s2 = LocalIndexStore::open(t2.path()).unwrap();
        let shard2 = s2
            .create_shard(&ShardId::single("docs"), &resolved())
            .unwrap();
        for (c, chunk) in docs.chunks(10).enumerate() {
            IndexWriter::write(
                &shard2,
                &CommitBatch::from_upserts(
                    chunk.to_vec(),
                    SourceCheckpoint::iceberg(1),
                    format!("snapshot-1-{c}"),
                ),
            )
            .unwrap();
        }

        assert_eq!(
            (count(&shard1, "body:alpha"), count(&shard1, "body:beta")),
            (10, 30)
        );
        assert_eq!(
            (count(&shard2, "body:alpha"), count(&shard2, "body:beta")),
            (count(&shard1, "body:alpha"), count(&shard1, "body:beta")),
            "chunked build == single-batch build"
        );
    }

    #[test]
    fn apply_drift_deletes_stale_reindexes_missing_and_is_idempotent() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &resolved())
            .unwrap();
        // Index doc-1, doc-2.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![doc("doc-1", "a", 0), doc("doc-2", "b", 1)],
                SourceCheckpoint::iceberg(1),
                "seed",
            ),
        )
        .unwrap();

        // Source now has doc-1 (unchanged) + doc-3 (new); doc-2 is gone.
        let source = vec![doc("doc-1", "a", 0), doc("doc-3", "c", 2)];
        let report = apply_drift(&shard, &[], source.clone(), None).unwrap();
        assert_eq!(
            report,
            DriftReport {
                index_count: 2,
                source_count: 2,
                deleted: 1,   // doc-2 removed
                reindexed: 1, // doc-3 added
                deletes_skipped: false,
            }
        );
        assert!(!shard.contains_key(&key("doc-2")).unwrap());
        assert!(shard.contains_key(&key("doc-3")).unwrap());

        // Idempotent: reconciling against the same source again repairs nothing.
        let again = apply_drift(&shard, &[], source, None).unwrap();
        assert!(again.is_clean(), "second reconcile is a no-op: {again:?}");
    }

    fn key(id: &str) -> CompositeKey {
        CompositeKey::new(vec![], vec![("id".into(), Value::from(id))])
    }

    #[test]
    fn sharded_reconcile_repairs_only_this_shards_owned_keys() {
        // The task-195 guarantee: reconcile scoped to a shard's owned keys repairs its own drift
        // (delete stale, re-index missing) WITHOUT pulling another shard's keys into it — the
        // placement-destroying bug of the old whole-table, `ShardId::single` reconcile.
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard0 = store
            .create_shard(&ShardId::single("docs"), &resolved())
            .unwrap();

        // A 2-shard hash router; split a pool of ids into what ordinal 0 owns vs. the other shard.
        let router = growlerdb_core::ShardRouter::hashed(2);
        let pool: Vec<String> = (0..40).map(|i| format!("doc-{i}")).collect();
        let owned: Vec<&String> = pool.iter().filter(|id| router.owns(&key(id), 0)).collect();
        let foreign: Vec<&String> = pool.iter().filter(|id| !router.owns(&key(id), 0)).collect();
        assert!(
            owned.len() >= 3 && !foreign.is_empty(),
            "need a usable split: {} owned / {} foreign",
            owned.len(),
            foreign.len()
        );
        let (stay, stale, missing) = (owned[0].as_str(), owned[1].as_str(), owned[2].as_str());

        // Seed shard 0 with an owned key that stays + an owned key that will go stale.
        IndexWriter::write(
            &shard0,
            &CommitBatch::from_upserts(
                vec![doc(stay, "a", 0), doc(stale, "b", 1)],
                SourceCheckpoint::iceberg(1),
                "seed",
            ),
        )
        .unwrap();

        // The FULL source (every shard's keys): `stay` + a new owned `missing` + ALL foreign keys;
        // `stale` is gone. This is what a whole-table read would hand a single node.
        let mut full_source = vec![doc(stay, "a", 0), doc(missing, "c", 2)];
        for (i, f) in foreign.iter().enumerate() {
            full_source.push(doc(f, "x", 100 + i as u64));
        }

        // Shard-scope: keep only the docs THIS shard owns before reconciling (what the RPC does).
        let owned_source: Vec<LocatedDoc> = full_source
            .into_iter()
            .filter(|d| router.owns(&d.doc.key, 0))
            .collect();
        let report = apply_drift(&shard0, &[], owned_source, None).unwrap();
        assert_eq!(report.deleted, 1, "the owned stale key was removed");
        assert_eq!(report.reindexed, 1, "the owned missing key was re-indexed");

        assert!(shard0.contains_key(&key(stay)).unwrap());
        assert!(shard0.contains_key(&key(missing)).unwrap());
        assert!(!shard0.contains_key(&key(stale)).unwrap());
        // The guarantee: no other shard's key was pulled into this shard.
        for f in &foreign {
            assert!(
                !shard0.contains_key(&key(f)).unwrap(),
                "foreign key {f} must not be indexed into this shard"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn sync_requires_append_fast_path() {
        // The seeded `docs` index is changelog mode → sync refuses (before any
        // Iceberg connection), pointing at the connector instead.
        let tmp = tempfile::tempdir().unwrap();
        seed_index(tmp.path());
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();
        let err = engine.sync("docs").await.unwrap_err();
        assert!(matches!(err, EngineError::NotAppendFastPath(i) if i == "docs"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rebuild_unknown_index_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();
        let err = engine.rebuild("nope").await.unwrap_err();
        assert!(matches!(err, EngineError::NotIndexed(i) if i == "nope"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn search_unknown_index_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();
        let err = engine
            .search("nope", "x", 10, false, Projection::All)
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::NotIndexed(i) if i == "nope"));
    }
}
