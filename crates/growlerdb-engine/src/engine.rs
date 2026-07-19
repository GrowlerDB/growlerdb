//! The embedded **engine façade** — wires source → index → search → hydrate into
//! one in-process unit, driven by the CLI. No server, auth, sharding, or UI.

use std::path::PathBuf;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use growlerdb_core::{
    CommitBatch, CompositeKey, Hit, HydratedRow, IcebergSource, IndexDefinition, IndexReader,
    IndexWriter, KeySpec, LocatedDoc, Mapping, Projection, Query, ResolvedIndex, ScanMode,
    SearchParams, ShardRouter, Snapshot, Source, SourceCheckpoint, Value,
};
// Ingest embeds via the BGE-capable factory (real local model, or the hash-embedder fallback),
// not core's built-in `default_embedder`.
use growlerdb_embed::{embed_located_docs, embedder_for};
use growlerdb_index::{IndexSchema, LocalIndexStore, Shard, ShardId};
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
    /// the source scan (TOCTOU guard). Missing-repair still ran; the next reconcile retries
    /// the deletes once the shard is momentarily quiescent.
    pub deletes_skipped: bool,
}

impl DriftReport {
    /// Whether the index already matched the source (nothing repaired).
    pub fn is_clean(&self) -> bool {
        self.deleted == 0 && self.reindexed == 0
    }
}

/// Max affected keys to log on a nonzero repair — bounded so a large drift can't flood
/// the log; the counts (and the `drift_*` metrics) carry the full magnitude.
const DRIFT_LOG_KEYS: usize = 20;

/// Reconcile a shard scope against the source's current `source_docs`: drop indexed
/// keys the source no longer has (via partition reconciliation) and re-index source
/// docs the index is missing. Pure over the store + the provided source docs, so it is
/// exercised without a live catalog. `partition` empty ⇒ the whole index.
///
/// For the **sharded** backstop the caller filters `source_docs` to the keys this shard
/// owns before calling, so the stale-set (indexed keys absent from `source_docs`) can't sweep away
/// another shard's keys.
/// `expected_checkpoint` is the shard's checkpoint captured **before** the source scan that produced
/// `source_docs`; it fences the stale-delete against a concurrent ingest (TOCTOU guard).
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
    // recoverable from logs, not just a metric count. Clean runs stay silent.
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

    /// Build **one ordinal shard** of an index from `table`: like [`index`](Self::index),
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

        // A `growlerdb index` run is a full build. If a previously-built index for this name persists
        // on disk with a *different* schema (a mapped field added/removed/renamed, or a type/fast-ness
        // change), reopening it and writing new-schema documents would corrupt Tantivy's fast-field
        // writer (a field-count mismatch panic). Detect that up front and reindex from scratch — wipe
        // the stale dir so the build below creates a fresh index with the new schema.
        self.reindex_on_schema_change(&index_name, &resolved)?;

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
    /// it, building **no shards/windows**. This is how a **windowed** node starts truly
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
    /// single shard, so peak memory is independent of table size. **Windowed:** buffer +
    /// [`write_windowed`](LocalIndexStore::write_windowed) (streamed windowing is a follow-up).
    /// Returns `(snapshot, doc_count)`.
    async fn build_from_source(
        &self,
        reader: &IcebergReader,
        table: &str,
        resolved: &ResolvedIndex,
        // Per-shard build filter: keep only docs this `(router, ordinal)` owns. `None` for a
        // normal full build.
        shard_filter: Option<&(ShardRouter, u32)>,
    ) -> Result<(Snapshot, usize), EngineError> {
        let (snapshot, doc_count) = if resolved.windowing.is_some() {
            let mut batch = reader.read_documents(table, resolved).await?;
            // Fill in each LOCAL vector field's embedding before the docs are written.
            embed_located_docs(resolved, &mut batch.docs);
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
            // reports its shard's doc count, not the whole table's.
            let mut written = 0usize;
            reader
                .read_documents_streamed(table, resolved, |mut docs| {
                    // Sharded build: drop docs this shard doesn't own, so the shard holds
                    // only its partition and a broadcast search can't double-count.
                    if let Some((router, ordinal)) = shard_filter {
                        docs.retain(|d| router.owns(&d.doc.key, *ordinal));
                    }
                    // Fill in each LOCAL vector field's embedding before the chunk is written.
                    embed_located_docs(resolved, &mut docs);
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
            // A shard that caught up to the source snapshot but wrote **zero rows** — a sparse
            // shard in a multi-shard build that owns none of the keys, or a currently-empty source —
            // must still record the snapshot it reflects. Otherwise it never commits a checkpoint and
            // reports `uninitialized` forever (a grey "unknown" health pill for the whole index),
            // even though it is genuinely in sync. If nothing above advanced the
            // checkpoint, anchor it with a checkpoint-only commit. Guarded on a real snapshot
            // (`snapshot_id != 0`) so a source with no snapshot stays honestly uninitialized.
            if snapshot_id != 0 && shard.current_checkpoint()?.is_none() {
                IndexWriter::write(
                    &shard,
                    &CommitBatch::from_upserts(
                        vec![],
                        SourceCheckpoint::iceberg(snapshot_id),
                        format!("snapshot-{snapshot_id}-anchor"),
                    ),
                )?;
            }
            // Anchor the built index to its source's Iceberg `table-uuid` so a later `serve` can
            // detect a drop+recreate of the source and refuse to serve stale data.
            shard.set_source_uuid(&reader.table_uuid(table).await?)?;
            (Snapshot(shard.current_snapshot()?), written)
        };

        // Never silently commit an empty index from a non-empty source: if we read 0 docs
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
    /// ([`write_windowed`](LocalIndexStore::write_windowed)). Returns a representative
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

    /// **Append fast-path sync**: for an `APPEND_FAST_PATH` index, read only
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

    /// **Drift reconciliation**: compare the index against the source's
    /// current snapshot and repair discrepancies — delete indexed keys the source
    /// dropped, re-index keys it gained. The periodic backstop that keeps the index
    /// consistent with Iceberg regardless of sync mode or delete encoding. Reconciles
    /// the whole index; per-partition scoping is the scaling refinement.
    pub async fn reconcile(&self, index: &str) -> Result<DriftReport, EngineError> {
        let resolved = self.load_definition(index)?;
        let shard = self.store.open_shard(&ShardId::single(index), &resolved)?;
        let table = source_table(&resolved);
        // Capture the checkpoint before the source read so the stale-delete can fence against a
        // concurrent ingest (TOCTOU guard); harmless for the CLI single-shard path.
        let expected_checkpoint = shard.current_checkpoint()?;
        let reader = IcebergReader::connect(&self.iceberg).await?;
        let source = reader.read_documents(&table, &resolved).await?;
        apply_drift(&shard, &[], source.docs, expected_checkpoint)
    }

    /// **Rebuild from Iceberg**: the hard-reset backstop — drop the index's
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

    /// **Semantic (KNN) search** over a VECTOR field: embed `query_text` with the field's
    /// configured embedder (the same [`embedder_for`] factory used at ingest, so the query and the
    /// documents share one embedding space), then run a top-level [`Query::Knn`] returning the `k` nearest
    /// documents' coordinates + KNN scores. When `hydrate` is set, also fetch the authoritative
    /// rows from Iceberg (projected by `projection`), exactly as [`search`](Self::search) does.
    ///
    /// This is the native, end-to-end semantic path (embed → KNN → nearest coordinates); the
    /// query-string / REST DSL surface for it is a later task. `field` must be a VECTOR field on
    /// the index.
    #[allow(clippy::too_many_arguments)] // a cohesive search-request surface, not an extractable cluster
    pub async fn semantic_search(
        &self,
        index: &str,
        field: &str,
        query_text: &str,
        k: usize,
        tenant: Option<&str>,
        hydrate: bool,
        projection: Projection,
        // Opt-in reranking (D21): reorder the retrieved top-K by a cross-encoder over
        // `query_text` and each hit's cached source-field text. `rerank_top_k` is the candidate
        // pool to fetch + rerank (0 ⇒ exactly `k`). Off by default.
        rerank: bool,
        rerank_top_k: usize,
    ) -> Result<SearchOutcome, EngineError> {
        let resolved = self.load_definition(index)?;
        // The named field must be a VECTOR field — its `VectorSpec` drives the query embedder.
        let spec = resolved
            .fields
            .iter()
            .find(|f| f.path == field)
            .and_then(|f| f.vector.as_ref())
            .ok_or_else(|| EngineError::NotVectorField(field.to_string()))?;

        // The KNN fetch depth: the rerank candidate pool when reranking, else just `k`.
        let fetch = if rerank { rerank_top_k.max(k) } else { k };

        // Embed the query text with the SAME factory ingest uses (real BGE when a model is
        // provisioned, else the deterministic hash fallback) — the query and the stored document
        // vectors must come from the same model or KNN compares across incompatible spaces.
        let embedder = embedder_for(spec);
        let mut vectors = embedder.embed(&[query_text.to_string()])?;
        let vector = vectors.pop().unwrap_or_default();

        let mut query = Query::Knn {
            field: field.to_string(),
            vector,
            k: fetch,
            filter: None,
        };
        // Tenant enforcement (filtered KNN): on a tenant-scoped index the caller MUST
        // present a claim; the `tenant = <claim>` Term rides inside the KNN as its filter, so the
        // neighbor set is intersected with that tenant's docs. Still fail **closed** when the claim
        // is missing — a tenant-scoped index with no claim must refuse, never return nearest
        // neighbors across tenants. A non-tenant-scoped index ignores `tenant`.
        if let Some(tenant_field) = resolved.tenant_field() {
            let Some(claim) = tenant else {
                return Err(EngineError::SemanticTenantClaimRequired(index.to_string()));
            };
            query = query.with_knn_filter(Query::Term {
                field: Some(tenant_field.to_string()),
                value: claim.to_string(),
            });
        }

        let shard = self.store.open_shard(&ShardId::single(index), &resolved)?;
        let hits = shard.search_all(&query, fetch)?;
        // Reranking reorders the retrieved pool by (query, cached source-field text) relevance and
        // returns the top `k`; off by default it's a plain KNN top-`k`.
        let hits = if rerank {
            crate::search_service::rerank_hits(hits, query_text, &spec.source_field, k)
        } else {
            hits
        };

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

    /// **Hybrid search** — run the query as BOTH a lexical (BM25) query and a semantic (KNN) query,
    /// then **Reciprocal-Rank-Fuse** the two result lists into one ranking ([`rrf_fuse`]). The
    /// lexical arm finds exact-term matches; the vector arm finds paraphrases/synonyms the lexical
    /// arm misses; RRF combines their ranks so a doc strong in *either* modality surfaces, and one
    /// strong in *both* rises to the top. `field` must be a VECTOR field on the index.
    ///
    /// Tenant scoping is enforced on **both** arms: the lexical query gets a non-widenable
    /// `and_filter(tenant, claim)` and the KNN query gets the same constraint as its
    /// [`with_knn_filter`](Query::with_knn_filter). On a tenant-scoped index a missing claim fails
    /// **closed** (same as [`semantic_search`](Self::semantic_search)); a non-tenant-scoped index
    /// ignores `tenant`.
    #[allow(clippy::too_many_arguments)] // a cohesive search-request surface, not an extractable cluster
    pub async fn hybrid_search(
        &self,
        index: &str,
        field: &str,
        query_text: &str,
        k: usize,
        tenant: Option<&str>,
        hydrate: bool,
        projection: Projection,
        // Opt-in reranking (D21): the semantic arm reorders its candidates by a cross-encoder
        // before RRF, so cross-encoder relevance carries into the fused ranks. Off by default.
        rerank: bool,
        rerank_top_k: usize,
    ) -> Result<SearchOutcome, EngineError> {
        let resolved = self.load_definition(index)?;
        let spec = resolved
            .fields
            .iter()
            .find(|f| f.path == field)
            .and_then(|f| f.vector.as_ref())
            .ok_or_else(|| EngineError::NotVectorField(field.to_string()))?;

        // Resolve the tenant claim once, failing closed on a tenant-scoped index with no claim.
        let tenant_claim = match resolved.tenant_field() {
            Some(tf) => {
                let Some(claim) = tenant else {
                    return Err(EngineError::SemanticTenantClaimRequired(index.to_string()));
                };
                Some((tf.to_string(), claim.to_string()))
            }
            None => None,
        };

        // Over-fetch each arm so the fusion has depth to work with (a doc ranked past `k` in one
        // arm can still win once the other arm's rank is added in).
        let k_each = k.max(10) * 2;
        // When reranking, fetch the larger of the fusion depth and the requested rerank pool for
        // the semantic arm, so a caller can rerank a deeper candidate set than the fusion depth.
        let knn_k = if rerank {
            k_each.max(rerank_top_k)
        } else {
            k_each
        };

        // Lexical arm: parse the query text (an empty/unparseable query falls back to match-all so
        // hybrid still returns the semantic arm). Tenant-scope with the non-widenable filter.
        let mut lexical = Query::parse(query_text).unwrap_or(Query::MatchAll);
        if let Some((tf, claim)) = &tenant_claim {
            lexical = lexical.and_filter(tf.clone(), claim.clone());
        }

        // Vector arm: embed with the field's configured embedder (same factory as ingest), then a
        // top-level KNN, tenant-scoped via `with_knn_filter`.
        let embedder = embedder_for(spec);
        let mut vectors = embedder.embed(&[query_text.to_string()])?;
        let vector = vectors.pop().unwrap_or_default();
        let mut knn = Query::Knn {
            field: field.to_string(),
            vector,
            k: knn_k,
            filter: None,
        };
        if let Some((tf, claim)) = &tenant_claim {
            knn = knn.with_knn_filter(Query::Term {
                field: Some(tf.clone()),
                value: claim.clone(),
            });
        }

        let shard = self.store.open_shard(&ShardId::single(index), &resolved)?;
        let lexical_hits = shard.search_all(&lexical, k_each)?;
        let vector_hits = shard.search_all(&knn, knn_k)?;
        // Reranking reorders the semantic arm's candidates by (query, cached source-field text)
        // relevance before fusion, so the cross-encoder signal carries into the fused RRF ranks.
        let vector_hits = if rerank {
            crate::search_service::rerank_hits(vector_hits, query_text, &spec.source_field, k_each)
        } else {
            vector_hits
        };

        let hits = rrf_fuse(&[&lexical_hits, &vector_hits], RRF_K, k);

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
            // The universal default; PREDICATE is an explicit choice.
            location_strategy: growlerdb_core::LocationStrategy::default(),
        })
    }

    /// Path to the persisted definition for `index`.
    fn definition_path(&self, index: &str) -> PathBuf {
        self.root.join(index).join("index.json")
    }

    /// If an index for `index` was already built and its persisted definition derives a **different**
    /// Tantivy schema than `resolved` (a mapped-field add/remove/rename or a field type/fast-ness
    /// change — anything that changes the field set the segment writer expects), wipe its on-disk
    /// state so the build reindexes from scratch against the new schema. A schema change invalidates
    /// the old segments+locator+checkpoint, so a fresh build is the correct behavior (same hard-reset
    /// as [`rebuild`](Self::rebuild)). Without this, reopening the stale narrower index and adding
    /// wider-schema documents panics Tantivy's fast-field writer (a field-count mismatch). No prior
    /// definition (a fresh volume), or a schema-compatible re-run, is a no-op.
    fn reindex_on_schema_change(
        &self,
        index: &str,
        resolved: &ResolvedIndex,
    ) -> Result<(), EngineError> {
        let path = self.definition_path(index);
        if !path.exists() {
            return Ok(()); // nothing built yet — a fresh index
        }
        let old: ResolvedIndex = serde_json::from_slice(&std::fs::read(&path)?)?;
        // Compare the *derived Tantivy schemas*, not the definitions: that is exactly the field set
        // (count/types/fast-ness) the segment writer is built against, so it flags precisely the
        // changes that would corrupt the writer while ignoring cosmetic definition edits.
        let old_schema = IndexSchema::from_resolved(&old);
        let new_schema = IndexSchema::from_resolved(resolved);
        if old_schema.tantivy_schema() != new_schema.tantivy_schema() {
            tracing::info!(
                index,
                "index schema changed since the last build — reindexing from scratch"
            );
            let dir = self.root.join(index);
            if dir.exists() {
                std::fs::remove_dir_all(&dir)?;
            }
        }
        Ok(())
    }

    /// Persist a resolved definition so `search` can reopen the shard later.
    fn persist_definition(&self, index: &str, resolved: &ResolvedIndex) -> Result<(), EngineError> {
        let path = self.definition_path(index);
        // `index.json` lives at the index root, which may not exist yet — the (windowed) build
        // persists the definition *before* creating any shard dir. Ensure the parent.
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

/// The per-shard build filter for building shard `ordinal` of `shards`: a `(router,
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

/// The standard **RRF constant** (`k = 60`): it dampens how much a top rank in any single list
/// contributes, so no one modality dominates and a doc must rank well across lists to rise. This is
/// the value from Cormack et al., "Reciprocal Rank Fusion outperforms Condorcet and individual Rank
/// Learning Methods" (SIGIR 2009), and the de-facto default in hybrid-search systems.
const RRF_K: usize = 60;

/// **Reciprocal Rank Fusion** of several ranked hit lists into one ranking. For each list, the hit
/// at 0-based `rank` contributes `1 / (k_rrf + rank + 1)` to that document's fused score (keyed by
/// its composite key); the contributions sum across lists. The fused list is sorted by score
/// descending with the encoded composite key as a stable tiebreaker (matching the KNN top-`k`
/// tiebreak), then truncated to `limit`.
///
/// A document present in only one list still appears (it just accrues one contribution); a document
/// high in *both* lists outranks one high in only one. Each fused [`Hit`] keeps its key and its
/// `score` becomes the RRF score; for `fields`/`highlight` it prefers a representative that carries
/// display data (the lexical hit) over a bare vector hit.
fn rrf_fuse(lists: &[&[Hit]], k_rrf: usize, limit: usize) -> Vec<Hit> {
    use std::collections::HashMap;
    // encoded key -> (accumulated fused score, representative hit)
    let mut acc: HashMap<Vec<u8>, (f32, Hit)> = HashMap::new();
    for list in lists {
        for (rank, hit) in list.iter().enumerate() {
            let contribution = 1.0 / (k_rrf as f32 + rank as f32 + 1.0);
            let entry = acc
                .entry(hit.key.encode())
                .or_insert_with(|| (0.0, hit.clone()));
            entry.0 += contribution;
            // Prefer a representative that carries display fields/highlights (a lexical hit) over one
            // that doesn't (a bare vector hit), so the fused hit renders without hydration.
            if entry.1.fields.is_empty()
                && entry.1.highlight.is_empty()
                && (!hit.fields.is_empty() || !hit.highlight.is_empty())
            {
                entry.1 = hit.clone();
            }
        }
    }
    let mut fused: Vec<Hit> = acc
        .into_values()
        .map(|(score, mut hit)| {
            hit.score = score;
            hit
        })
        .collect();
    fused.sort_by(|a, b| {
        b.score
            .partial_cmp(&a.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.key.encode().cmp(&b.key.encode()))
    });
    fused.truncate(limit);
    fused
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

    /// Seed a `docs` index that adds a LOCAL VECTOR field (`body_vec`), ingesting the docs with
    /// their bodies embedded exactly as ingest would (via `embed_located_docs`), so a query
    /// embedded by the same model shares their space.
    fn seed_vector_index(root: &Path) {
        use growlerdb_core::embed_located_docs;
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let resolved = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: body_vec, type: VECTOR, vector: { dims: 64, source_field: body } } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();

        let store = LocalIndexStore::open(root).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &resolved)
            .unwrap();
        std::fs::write(
            root.join("docs").join("index.json"),
            serde_json::to_vec_pretty(&resolved).unwrap(),
        )
        .unwrap();

        let mut docs = vec![
            doc("doc-1", "apache iceberg lakehouse tables", 0),
            doc("doc-2", "full text search relevance ranking", 1),
            doc("doc-3", "vector embeddings semantic retrieval", 2),
        ];
        // Embed each doc's `body` into `body_vec`, exactly as the ingest transform does.
        embed_located_docs(&resolved, &mut docs);
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn semantic_search_returns_nearest_coordinates_end_to_end() {
        let tmp = tempfile::tempdir().unwrap();
        seed_vector_index(tmp.path());
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();

        // A query whose tokens overlap doc-3's body embeds nearest to it (HashEmbedder is a
        // bag-of-tokens, so shared tokens ⇒ high cosine).
        let out = engine
            .semantic_search(
                "docs",
                "body_vec",
                "semantic retrieval embeddings",
                1,
                None,
                false,
                Projection::All,
                false,
                0,
            )
            .await
            .unwrap();
        assert_eq!(out.hits.len(), 1);
        assert_eq!(out.hits[0].key.get("id"), Some(&Value::from("doc-3")));
        assert!(out.rows.is_none());

        // Naming a non-vector field is a clear error.
        let err = engine
            .semantic_search(
                "docs",
                "body",
                "x",
                1,
                None,
                false,
                Projection::All,
                false,
                0,
            )
            .await
            .unwrap_err();
        assert!(matches!(err, EngineError::NotVectorField(f) if f == "body"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn semantic_search_is_refused_fail_closed_on_a_tenant_scoped_index() {
        // KNN doesn't yet enforce the tenant filter, so it must refuse rather than risk a
        // cross-tenant result — the guard fires before any shard read.
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let resolved = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\ntenant_field: id\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: body_vec, type: VECTOR, vector: { dims: 64, source_field: body } } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        assert!(resolved.tenant_field().is_some());
        let store = LocalIndexStore::open(root).unwrap();
        store
            .create_shard(&ShardId::single("docs"), &resolved)
            .unwrap();
        std::fs::write(
            root.join("docs").join("index.json"),
            serde_json::to_vec_pretty(&resolved).unwrap(),
        )
        .unwrap();

        let engine = Engine::open(root, IcebergConfig::local()).unwrap();
        // A tenant-scoped index with NO claim still fails closed — never returns cross-tenant rows.
        let err = engine
            .semantic_search(
                "docs",
                "body_vec",
                "x",
                1,
                None,
                false,
                Projection::All,
                false,
                0,
            )
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            EngineError::SemanticTenantClaimRequired(i) if i == "docs"
        ));
    }

    /// Seed a tenant-scoped vector index (`tenant` KEYWORD + `body_vec` LOCAL VECTOR), embedding
    /// each doc's body as ingest would, so a query embedded by the same model shares their space.
    fn seed_tenant_vector_index(root: &Path) {
        use growlerdb_core::embed_located_docs;
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("tenant", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let resolved = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\ntenant_field: tenant\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: tenant, type: KEYWORD }, { path: body, type: TEXT }, { path: body_vec, type: VECTOR, vector: { dims: 64, source_field: body } } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        assert!(resolved.tenant_field().is_some());

        let store = LocalIndexStore::open(root).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &resolved)
            .unwrap();
        std::fs::write(
            root.join("docs").join("index.json"),
            serde_json::to_vec_pretty(&resolved).unwrap(),
        )
        .unwrap();

        let tdoc = |id: &str, tenant: &str, body: &str, pos: u64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut fields = BTreeMap::new();
            fields.insert("id".to_string(), Value::from(id));
            fields.insert("tenant".to_string(), Value::from(tenant));
            fields.insert("body".to_string(), Value::from(body));
            LocatedDoc {
                doc: Document::new(key, fields),
                iceberg_file: "f".into(),
                row_position: pos,
            }
        };
        let mut docs = vec![
            tdoc("t1-a", "t1", "vector embeddings semantic retrieval", 0),
            tdoc("t1-b", "t1", "apache iceberg lakehouse tables", 1),
            // Same body as t1-a but a DIFFERENT tenant — the nearest neighbor to a matching query,
            // so it would surface first without the tenant filter.
            tdoc("t2-a", "t2", "vector embeddings semantic retrieval", 2),
        ];
        embed_located_docs(&resolved, &mut docs);
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn tenant_scoped_semantic_search_returns_only_the_claimed_tenant() {
        let tmp = tempfile::tempdir().unwrap();
        seed_tenant_vector_index(tmp.path());
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();

        // A query nearest to the shared "vector embeddings semantic retrieval" body: t2-a is an
        // equally-near neighbor, but the tenant claim t1 must exclude it — only t1 docs come back.
        let out = engine
            .semantic_search(
                "docs",
                "body_vec",
                "semantic retrieval embeddings",
                5,
                Some("t1"),
                false,
                Projection::All,
                false,
                0,
            )
            .await
            .unwrap();
        assert!(!out.hits.is_empty());
        for h in &out.hits {
            assert!(
                h.key
                    .get("id")
                    .unwrap()
                    .to_index_string()
                    .starts_with("t1-"),
                "every hit must belong to tenant t1, got {:?}",
                h.key.get("id")
            );
        }
        // The other tenant's doc is absent even though it's an equally-near neighbor.
        assert!(!out
            .hits
            .iter()
            .any(|h| h.key.get("id") == Some(&Value::from("t2-a"))));
    }

    /// Seed a NON-tenant vector index tuned for the hybrid test: `both` matches the query
    /// lexically AND semantically; `other` matches neither lexically (so it's absent from the BM25
    /// arm) but is still returned by the KNN arm.
    fn seed_hybrid_index(root: &Path) {
        use growlerdb_core::embed_located_docs;
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let resolved = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: body_vec, type: VECTOR, vector: { dims: 64, source_field: body } } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let store = LocalIndexStore::open(root).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &resolved)
            .unwrap();
        std::fs::write(
            root.join("docs").join("index.json"),
            serde_json::to_vec_pretty(&resolved).unwrap(),
        )
        .unwrap();
        let mut docs = vec![
            doc("both", "apple banana cherry", 0),
            doc("other", "xylophone yak zebra", 1),
        ];
        embed_located_docs(&resolved, &mut docs);
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hybrid_search_fuses_and_the_both_modality_doc_wins() {
        let tmp = tempfile::tempdir().unwrap();
        seed_hybrid_index(tmp.path());
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();

        // "apple banana cherry" matches `both` lexically (all three terms) AND semantically; `other`
        // shares no query term (absent from the BM25 arm) but is still a KNN neighbor. RRF sums the
        // two arms, so `both` (in both lists) outranks `other` (in one) — and both appear.
        let out = engine
            .hybrid_search(
                "docs",
                "body_vec",
                "apple banana cherry",
                10,
                None,
                false,
                Projection::All,
                false,
                0,
            )
            .await
            .unwrap();
        let ids: Vec<String> = out
            .hits
            .iter()
            .map(|h| h.key.get("id").unwrap().to_index_string())
            .collect();
        assert!(ids.contains(&"both".to_string()) && ids.contains(&"other".to_string()));
        assert_eq!(
            ids[0], "both",
            "the both-modality doc fuses to the top: {ids:?}"
        );
    }

    fn hit(id: &str, score: f32) -> Hit {
        Hit {
            key: CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]),
            score,
            fields: BTreeMap::new(),
            highlight: BTreeMap::new(),
        }
    }

    #[test]
    fn rrf_fuse_ranks_overlap_first_keeps_singletons_and_is_deterministic() {
        // Two ranked lists. `b` is high in BOTH; `a` tops only list 1; `c` tops only list 2.
        let list1 = vec![hit("a", 9.0), hit("b", 8.0), hit("d", 7.0)];
        let list2 = vec![hit("b", 0.9), hit("c", 0.8)];

        let fused = rrf_fuse(&[&list1, &list2], RRF_K, 10);
        let ids: Vec<String> = fused
            .iter()
            .map(|h| h.key.get("id").unwrap().to_index_string())
            .collect();
        // `b` appears in both lists → highest fused score → first.
        assert_eq!(ids[0], "b", "the doc in both lists sorts first: {ids:?}");
        // A doc in only one list still appears.
        for want in ["a", "b", "c", "d"] {
            assert!(
                ids.contains(&want.to_string()),
                "{want} missing from {ids:?}"
            );
        }
        // `b`'s fused score sums its reciprocal ranks: rank 1 (0-based) in list1, rank 0 in list2.
        let b = fused
            .iter()
            .find(|h| h.key.get("id") == Some(&Value::from("b")))
            .unwrap();
        let expected = 1.0 / (RRF_K as f32 + 1.0 + 1.0) + 1.0 / (RRF_K as f32 + 0.0 + 1.0);
        assert!((b.score - expected).abs() < 1e-6, "b.score = {}", b.score);
        // `a` (rank 0 in list1 only) beats `c` (rank 1 in list2 only): 1/61 > 1/62.
        let pos = |id: &str| ids.iter().position(|x| x == id).unwrap();
        assert!(pos("a") < pos("c"));

        // Determinism: re-fusing the same input yields the identical order.
        let again = rrf_fuse(&[&list1, &list2], RRF_K, 10);
        let ids2: Vec<String> = again
            .iter()
            .map(|h| h.key.get("id").unwrap().to_index_string())
            .collect();
        assert_eq!(ids, ids2);

        // The limit truncates.
        assert_eq!(rrf_fuse(&[&list1, &list2], RRF_K, 2).len(), 2);
    }

    #[test]
    fn rrf_fuse_prefers_a_representative_with_display_fields() {
        // The vector arm's hit for `x` carries no fields; the lexical arm's does. The fused hit must
        // keep the one that renders without hydration.
        let mut lex = hit("x", 1.0);
        lex.fields.insert("title".into(), Value::from("Hello"));
        let vec_only = hit("x", 0.5); // no fields
        let fused = rrf_fuse(&[&[vec_only], &[lex]], RRF_K, 10);
        assert_eq!(fused.len(), 1);
        assert_eq!(fused[0].fields.get("title"), Some(&Value::from("Hello")));
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

    #[test]
    fn reindex_on_schema_change_wipes_a_stale_index_but_leaves_an_unchanged_one() {
        // A previously-built `docs` index (schema A: id, body). A `growlerdb index` re-run with a
        // *widened* definition (adds a mapped `title` field) must reindex from scratch rather than
        // reopen the stale narrower index and panic Tantivy's fast-field writer. A re-run
        // with the SAME schema must be a no-op (the built data is preserved).
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();
        seed_index(root); // builds `docs` (schema A) + persists its index.json
        let engine = Engine::open(root, IcebergConfig::local()).unwrap();
        let docs_dir = root.join("docs");
        assert!(docs_dir.join("index.json").exists());

        // Same schema ⇒ no-op: the built index (and its data) survive.
        engine
            .reindex_on_schema_change("docs", &resolved())
            .unwrap();
        assert!(
            docs_dir.exists(),
            "an unchanged schema must not wipe the index"
        );

        // Widened schema ⇒ the stale on-disk index is wiped so the build reindexes from scratch.
        let widened = {
            let src = SourceSchema::new(
                vec![
                    SourceField::new("id", SourceType::String),
                    SourceField::new("body", SourceType::String),
                    SourceField::new("title", SourceType::String),
                ],
                vec![],
                vec!["id".into()],
            );
            IndexDefinition::from_yaml(
                "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: title, type: TEXT } ] }\n",
            )
            .unwrap()
            .resolve(&src)
            .unwrap()
        };
        engine.reindex_on_schema_change("docs", &widened).unwrap();
        assert!(
            !docs_dir.exists(),
            "a changed schema reindexes from scratch (wipes the stale dir)"
        );

        // The wiped dir now builds cleanly against the widened schema — a fresh index whose writer
        // schema matches the wider docs, so the historical field-count panic is unreachable.
        let shard = engine
            .store
            .create_shard(&ShardId::single("docs"), &widened)
            .unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), Value::from("doc-1"));
        fields.insert("body".to_string(), Value::from("hello world"));
        fields.insert("title".to_string(), Value::from("greeting"));
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![LocatedDoc {
                    doc: Document::new(key, fields),
                    iceberg_file: "data/f0.parquet".into(),
                    row_position: 0,
                }],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        assert_eq!(shard.num_docs().unwrap(), 1);
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
        const DAY: i64 = 86_400_000_000; // micros (canonical window scale)
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
        // index()/rebuild() persist the definition *before* any shard dir is created, so the
        // index root may not exist yet — persist_definition must create it (regression for the e2e
        // `NotFound` writing index.json onto a missing dir).
        let tmp = tempfile::tempdir().unwrap();
        let engine = Engine::open(tmp.path(), IcebergConfig::local()).unwrap();
        engine.persist_definition("fresh", &resolved()).unwrap();
        assert!(tmp.path().join("fresh").join("index.json").exists());
    }

    #[test]
    fn chunked_writes_equal_a_single_batch() {
        // The streamed build writes the source in many bounded chunks (one commit each,
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
        // Reconcile scoped to a shard's owned keys repairs its own drift (delete stale, re-index
        // missing) WITHOUT pulling another shard's keys into it.
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

    /// **Real-model eval (AC#3), `#[ignore]`d.** Builds a small corpus + paraphrase queries whose
    /// relevant doc shares MEANING but no exact terms (so a strict-AND BM25 misses it), then shows
    /// hybrid RRF strictly beats lexical-only by **MRR**. Requires the provisioned BGE model
    /// (`GROWLERDB_MODEL_DIR` default `~/.cache/growlerdb/models/bge-small-en-v1.5/`); never runs in
    /// CI. Run:
    /// `cargo test -p growlerdb-engine --release -- --ignored hybrid_beats_lexical_on_eval_set --nocapture`
    #[tokio::test(flavor = "current_thread")]
    #[ignore = "requires a provisioned GROWLERDB_MODEL_DIR bge-small-en-v1.5 model"]
    async fn hybrid_beats_lexical_on_eval_set() {
        use growlerdb_core::embed_located_docs;
        let tmp = tempfile::tempdir().unwrap();
        let root = tmp.path();

        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        // `vector: { source_field: body }` takes the defaults → dims 384, model bge-small-en-v1.5,
        // provider LOCAL — the real model when provisioned.
        let resolved = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: body_vec, type: VECTOR, vector: { source_field: body } } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        // ~18 short docs across distinct topics. The five relevant docs (cats/dogs/markets/cooking/
        // space) are phrased WITHOUT the query's content words, so BM25 alone can't retrieve them.
        let corpus: &[(&str, &str)] = &[
            ("cats", "feline predators nap in warm sunbeams"),
            ("dogs", "canines fetch and chase a ball across the meadow"),
            ("markets", "equities rallied as wall street traders bought"),
            ("cooking", "a chef seared the salmon fillet in butter"),
            ("space", "astronomers observed a faint distant galaxy"),
            ("cars", "a mechanic replaced the worn brake pads"),
            ("music", "the orchestra performed a sweeping symphony"),
            ("garden", "she planted crimson roses along the fence"),
            ("weather", "a storm flooded the low river valley"),
            ("software", "the engineer debugged a race condition"),
            ("travel", "the jet touched down on the coastal runway"),
            ("health", "vaccines build immunity against infection"),
            ("history", "the museum archived medieval manuscripts"),
            ("sports", "the striker scored in the final minute"),
            ("farming", "workers harvested golden wheat at dusk"),
            ("art", "the toddler scribbled with bright crayons"),
            ("mountain", "hikers ascended the steep granite ridge"),
            ("ocean", "divers explored a vivid coral reef"),
        ];
        let mut docs: Vec<LocatedDoc> = corpus
            .iter()
            .enumerate()
            .map(|(i, (id, body))| doc(id, body, i as u64))
            .collect();
        embed_located_docs(&resolved, &mut docs);
        // Seed in a scope so the store/shard (holding the redb) drops before `Engine::open` reopens
        // the same root — otherwise redb refuses the second open (`DatabaseAlreadyOpen`).
        {
            let store = LocalIndexStore::open(root).unwrap();
            let shard = store
                .create_shard(&ShardId::single("docs"), &resolved)
                .unwrap();
            std::fs::write(
                root.join("docs").join("index.json"),
                serde_json::to_vec_pretty(&resolved).unwrap(),
            )
            .unwrap();
            IndexWriter::write(
                &shard,
                &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(1), "b1"),
            )
            .unwrap();
        }

        let engine = Engine::open(root, IcebergConfig::local()).unwrap();

        // (paraphrase query, relevant doc id) — each query shares MEANING, not terms, with its doc.
        let evalset: &[(&str, &str)] = &[
            ("cats sleeping in the sun", "cats"),
            ("puppies playing outdoors", "dogs"),
            ("shares rose on the stock exchange", "markets"),
            ("preparing fish for dinner", "cooking"),
            ("studying stars in the cosmos", "space"),
        ];

        // Reciprocal rank of the relevant id in a hit list (0 if absent).
        let rr = |hits: &[Hit], rel: &str| -> f32 {
            hits.iter()
                .position(|h| h.key.get("id") == Some(&Value::from(rel)))
                .map(|p| 1.0 / (p as f32 + 1.0))
                .unwrap_or(0.0)
        };

        let (mut lex_mrr, mut hyb_mrr) = (0.0f32, 0.0f32);
        for (q, rel) in evalset {
            let lex = engine
                .search("docs", q, 10, false, Projection::All)
                .await
                .unwrap();
            let hyb = engine
                .hybrid_search(
                    "docs",
                    "body_vec",
                    q,
                    10,
                    None,
                    false,
                    Projection::All,
                    false,
                    0,
                )
                .await
                .unwrap();
            lex_mrr += rr(&lex.hits, rel);
            hyb_mrr += rr(&hyb.hits, rel);
        }
        let n = evalset.len() as f32;
        lex_mrr /= n;
        hyb_mrr /= n;
        println!("EVAL MRR  lexical = {lex_mrr:.4}   hybrid = {hyb_mrr:.4}");
        assert!(
            hyb_mrr > lex_mrr,
            "hybrid MRR ({hyb_mrr:.4}) must strictly beat lexical MRR ({lex_mrr:.4})"
        );
    }
}
