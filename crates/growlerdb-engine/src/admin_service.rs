//! The Node **Admin** gRPC service over the served shard.
//! `DescribeIndex` reports stats. `AlterIndex` resolves a candidate definition against the
//! source schema and returns the [`AlterPlan`](growlerdb_core::AlterPlan) (detect + guide);
//! with `apply` set it accepts only a true **no-op** live — every real change (reindex-requiring,
//! rename, or a restart-required read-time policy like `sensitive`/`max_bytes`) is rejected with a
//! clear status, since the running shard keeps its built schema. `ReindexIndex` rebuilds
//! from the source and durably swaps the shard live, fencing writes for the rebuild so the
//! checkpoint can't regress. Create / drop / list pair with the multi-index server + Control Plane.

use std::path::Path;
use std::sync::{Arc, RwLock};

use growlerdb_core::{
    BucketMap, CommitBatch, CompositeKey, IndexDefinition, IndexWriter, LocatedDoc, ResolvedIndex,
    ShardRouter, SourceCheckpoint, SourceSchema,
};
use growlerdb_index::{LocalIndexStore, Shard, ShardId, StoreError};
use growlerdb_proto::v1::{
    AlterIndexRequest, AlterIndexResponse, AlterPlan as WireAlterPlan, BackupIndexRequest,
    BackupIndexResponse, BackupStatusRequest, BackupStatusResponse, CompactIndexRequest,
    CompactIndexResponse, DescribeIndexRequest, DescribeIndexResponse, IndexStats,
    ReconcileIndexRequest, ReconcileIndexResponse, ReindexIndexRequest, ReindexIndexResponse,
    VectorFieldStat,
};
use growlerdb_proto::{Admin, AdminServer};
use growlerdb_source::{IcebergConfig, IcebergReader};
use tonic::{Request, Response, Status};

use crate::auth::{self, default_auth, SharedAuth};
use crate::fence::{ReindexFence, ReindexGuard};
use crate::service_util::{check_served, internal, run_blocking};
use crate::shard_handle::ShardHandle;

/// The source context an [`AdminService`] needs to **execute** admin operations against the
/// index's Iceberg source: the served definition (the alter diff baseline, updated in place
/// when an alter is applied), the store + shard id (for a durable reindex swap), how to reach
/// the source, and where the definition is persisted. Absent ⇒ `AlterIndex` / `ReindexIndex`
/// are `Unimplemented`.
struct SourceContext {
    /// The current definition — the diff baseline. Behind a lock so an applied in-place alter
    /// updates it for subsequent alters/reindexes without a restart.
    resolved: RwLock<ResolvedIndex>,
    store: LocalIndexStore,
    shard_id: ShardId,
    iceberg: IcebergConfig,
    table: String,
    /// Shared reindex write-fence: engaged for the duration of a reindex so the Write
    /// service rejects new writes (no rebuild-window delta to drop) and a second reindex is
    /// refused (single-flight). Shared with the Node's [`WriteService`](crate::WriteService).
    fence: ReindexFence,
}

/// The accumulated outcome of a count-gated reconcile: counts over the divergent
/// partitions it row-reconciled plus how many partitions it scanned vs. skipped as in-sync.
#[derive(Default)]
struct GateResult {
    index_count: u64,
    source_count: u64,
    stale: u64,
    missing: u64,
    deletes_skipped: bool,
    partitions_scanned: u64,
    partitions_skipped: u64,
}

impl GateResult {
    fn into_response(self) -> ReconcileIndexResponse {
        ReconcileIndexResponse {
            index_count: self.index_count,
            source_count: self.source_count,
            stale: self.stale,
            missing: self.missing,
            deletes_skipped: self.deletes_skipped,
            partitions_scanned: self.partitions_scanned,
            partitions_skipped: self.partitions_skipped,
        }
    }
}

/// Object-storage backup target: the [`Operator`](opendal::Operator) + prefix a
/// node-triggered backup writes to / reads status from. Absent ⇒ `BackupIndex` is `Unimplemented`
/// and `BackupStatus` reports `configured = false`.
#[derive(Clone)]
struct BackupCfg {
    store: opendal::Operator,
    prefix: String,
}

/// An `Admin` service over the served shard. Stats reads touch redb + the Tantivy
/// segments, so they run on the blocking pool. Every RPC consults the
/// [auth hook](SharedAuth) first.
#[derive(Clone)]
pub struct AdminService {
    shard: ShardHandle,
    index: String,
    auth: SharedAuth,
    /// Present when the Node has source access, enabling [`AlterIndex`](Admin::alter_index)
    /// planning and [`ReindexIndex`](Admin::reindex_index) execution.
    source: Option<Arc<SourceContext>>,
    /// Present when the Node was started with an object-storage backup target.
    backup: Option<BackupCfg>,
}

impl AdminService {
    /// An Admin service describing `index` (served by `shard`), with the default no-op
    /// auth hook and no alter capability ([`with_alter`](Self::with_alter) adds it). Accepts
    /// an `Arc<Shard>` (fresh handle) or a shared [`ShardHandle`].
    pub fn new(shard: impl Into<ShardHandle>, index: impl Into<String>) -> Self {
        Self::with_auth(shard, index, default_auth())
    }

    /// As [`new`](Self::new), with a specific [auth hook](SharedAuth).
    pub fn with_auth(
        shard: impl Into<ShardHandle>,
        index: impl Into<String>,
        auth: SharedAuth,
    ) -> Self {
        Self {
            shard: shard.into(),
            index: index.into(),
            auth,
            source: None,
            backup: None,
        }
    }

    /// Enable node-triggered backups: `store` is the object-storage target and `prefix`
    /// the key prefix backups are written under. Without this, `BackupIndex` is `Unimplemented` and
    /// `BackupStatus` reports `configured = false`.
    pub fn with_backup(mut self, store: opendal::Operator, prefix: impl Into<String>) -> Self {
        self.backup = Some(BackupCfg {
            store,
            prefix: prefix.into(),
        });
        self
    }

    /// Enable [`AlterIndex`](Admin::alter_index) planning and
    /// [`ReindexIndex`](Admin::reindex_index) by giving the service source access: the served
    /// index's current definition, the store + shard id (for a durable reindex swap), and how
    /// to reach the Iceberg source. Without this, both return `Unimplemented`. The `fence` is
    /// shared with the Node's [`WriteService`](crate::WriteService) so a reindex fences writes.
    pub fn with_source(
        mut self,
        resolved: ResolvedIndex,
        store: LocalIndexStore,
        shard_id: ShardId,
        iceberg: IcebergConfig,
        table: impl Into<String>,
        fence: ReindexFence,
    ) -> Self {
        self.source = Some(Arc::new(SourceContext {
            resolved: RwLock::new(resolved),
            store,
            shard_id,
            iceberg,
            table: table.into(),
            fence,
        }));
        self
    }

    /// Wrap as a mountable tonic [`AdminServer`].
    pub fn into_server(self) -> AdminServer<Self> {
        AdminServer::new(self)
    }

    /// Per-partition count-gated reconcile. Returns `Some(result)` when the gate applied
    /// (the source is cleanly identity-partitioned on the index's partition-key fields), having
    /// row-reconciled only the partitions whose source `record_count` differs from the index
    /// key-count and skipped the rest. Returns `None` when the gate doesn't apply (not
    /// identity-partitioned, misaligned fields, or an empty source) so the caller falls back to a
    /// whole-shard scan. Reads rows only for divergent partitions; peak memory is O(one partition).
    ///
    /// Note: it iterates SOURCE partitions, so a whole partition dropped from the source (all its
    /// keys now stale) isn't caught here — the periodic `full` sweep is the completeness backstop.
    async fn count_gated_reconcile(
        &self,
        reader: &IcebergReader,
        table: &str,
        resolved: &ResolvedIndex,
        owner_filter: &Option<(ShardRouter, u32)>,
        expected_checkpoint: Option<SourceCheckpoint>,
    ) -> Result<Option<GateResult>, Status> {
        // Cheap detection: per-partition source record counts from manifest metadata (no row reads).
        let plan = reader.current_plan(table).await.map_err(internal)?;
        let Some(parts) = growlerdb_source::partition_record_counts(&plan.tasks) else {
            return Ok(None); // not cleanly identity-partitioned → whole-shard fallback
        };
        if parts.is_empty() {
            return Ok(None); // empty source → let the full path handle any delete-all
        }
        // Alignment: the source identity-partition fields must be exactly the index's partition-key
        // fields, in order, so a partition tuple builds the index key prefix directly.
        let part_fields: Vec<&str> = parts[0].0.iter().map(|(n, _)| n.as_str()).collect();
        let index_fields: Vec<&str> = resolved
            .key
            .partition_fields
            .iter()
            .map(String::as_str)
            .collect();
        if part_fields != index_fields {
            return Ok(None);
        }

        let mut result = GateResult::default();
        for (partition, source_records) in parts {
            // Ownership: only the partitions this shard owns (partition routing co-locates a partition
            // on one shard). Another shard's partition is silently skipped.
            if let Some((router, ord)) = owner_filter {
                if !router.owns(&CompositeKey::new(partition.clone(), Vec::new()), *ord) {
                    continue;
                }
            }
            result.source_count += source_records;

            // Cheap index count for the partition (term enumeration, no row reads).
            let shard = self.shard.current();
            let part_for_count = partition.clone();
            let index_records = run_blocking(move || shard.key_count(&part_for_count))
                .await?
                .map_err(internal)? as u64;
            result.index_count += index_records;
            if index_records == source_records {
                result.partitions_skipped += 1;
                continue; // in sync — no row read
            }

            // Divergent: read only this partition's rows, keep owned, reconcile the partition scope.
            result.partitions_scanned += 1;
            let mut owned: Vec<LocatedDoc> = Vec::new();
            reader
                .read_documents_in_partition(table, resolved, &partition, |docs| {
                    for d in docs {
                        if owner_filter
                            .as_ref()
                            .is_none_or(|(router, o)| router.owns(&d.doc.key, *o))
                        {
                            owned.push(d);
                        }
                    }
                    Ok(())
                })
                .await
                .map_err(internal)?;

            let shard = self.shard.current();
            let ck = expected_checkpoint.clone();
            let report =
                run_blocking(move || crate::engine::apply_drift(&shard, &partition, owned, ck))
                    .await?
                    .map_err(internal)?;
            result.stale += report.deleted as u64;
            result.missing += report.reindexed as u64;
            result.deletes_skipped |= report.deletes_skipped;
        }
        Ok(Some(result))
    }
}

/// Resolve `candidate_yaml` against `source`. A candidate that fails to parse/resolve is an
/// `InvalidArgument` (the operator's definition is wrong, not the server).
fn resolve_candidate(candidate_yaml: &str, source: &SourceSchema) -> Result<ResolvedIndex, Status> {
    IndexDefinition::from_yaml(candidate_yaml)
        .map_err(|e| Status::invalid_argument(format!("invalid candidate definition: {e}")))?
        .resolve(source)
        .map_err(|e| Status::invalid_argument(format!("candidate does not resolve: {e}")))
}

/// Diff `candidate` against `current` into the wire [`AlterPlan`](WireAlterPlan).
fn wire_plan(current: &ResolvedIndex, candidate: &ResolvedIndex) -> WireAlterPlan {
    let plan = current.alter_to(candidate);
    WireAlterPlan {
        is_noop: plan.is_noop(),
        requires_reindex: plan.requires_reindex(),
        reindex_reasons: plan.reindex_reasons,
        in_place_changes: plan.in_place,
    }
}

/// Validate an in-place alter and return `candidate` as the new baseline when it's a true no-op.
/// Everything else is rejected with `FailedPrecondition`:
///
/// * a **no-op** → `Ok` (the only safe live apply; nothing changes, nothing is written);
/// * a **reindex-requiring** change → run `ReindexIndex`;
/// * a **rename** → identity/paths are fixed on a single-index node;
/// * any other in-place change — i.e. a **read-time policy** declaration (`sensitive`/`max_bytes`)
///   — is **restart-required**: the running shard keeps the schema it was built with, so applying
///   it live would silently not take effect. The operator changes the definition
///   (`index.json`, the boot source of truth) and restarts. Pure / no side effects — the apply
///   path never writes, so it's never exposed to the file-write hazard.
fn apply_in_place(
    current: &ResolvedIndex,
    candidate: ResolvedIndex,
) -> Result<ResolvedIndex, Status> {
    let plan = current.alter_to(&candidate);
    if plan.is_noop() {
        return Ok(candidate);
    }
    if plan.requires_reindex() {
        return Err(Status::failed_precondition(format!(
            "alter requires a reindex ({}); update the definition and run ReindexIndex",
            plan.reindex_reasons.join(", ")
        )));
    }
    if candidate.name != current.name {
        return Err(Status::failed_precondition(
            "renaming the served index is not supported on a single-index node",
        ));
    }
    Err(Status::failed_precondition(format!(
        "alter changes ({}) are restart-required: the running shard keeps its built schema, so they \
         would not take effect live — update the definition and restart the Node to apply them",
        plan.in_place.join(", ")
    )))
}

#[tonic::async_trait]
impl Admin for AdminService {
    async fn describe_index(
        &self,
        request: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        auth::authorize(&self.auth, "DescribeIndex", &request)?;
        let req = request.into_inner();

        // Single-index server: an empty name means "the served index"; any other name
        // is not found here.
        check_served(&req.index, &self.index)?;

        let shard = self.shard.current();
        let name = self.index.clone();
        let stats = run_blocking(move || -> Result<IndexStats, _> {
            Ok::<_, growlerdb_index::StoreError>(IndexStats {
                name,
                snapshot: shard.current_snapshot()?,
                num_docs: shard.num_docs()?,
                generation_count: shard.segment_count()?,
                checkpoint: render_checkpoint(shard.current_checkpoint()?),
                size_bytes: shard.size_bytes(), // per-shard on-disk size (skew signal)
                time_fields: shard.date_fields(), // DATE columns for the console time filter
                sort_fields: shard.sort_fields(), // sortable fast fields for the console sort menu
                // VECTOR fields for the console's semantic/hybrid vector-field picker, each
                // with its KNN coverage so a partially-embedded index is visible next to
                // `num_docs` instead of silently unsearchable.
                vector_fields: {
                    let mut vfs = Vec::new();
                    for v in shard.vector_fields() {
                        let docs_with_vector = shard.vector_coverage(&v.name)?;
                        vfs.push(VectorFieldStat {
                            name: v.name,
                            source_field: v.source_field,
                            model: v.model,
                            dims: v.dims as u32,
                            docs_with_vector,
                        });
                    }
                    vfs
                },
                // The full mapping (type + capability flags) so clients compose valid
                // queries from the schema — console pickers, MCP agents self-teaching.
                fields: shard
                    .mapped_fields()
                    .into_iter()
                    .map(|f| growlerdb_proto::v1::MappedFieldStat {
                        name: f.name,
                        r#type: f.ty,
                        fast: f.fast,
                        indexed: f.indexed,
                        cached: f.cached,
                    })
                    .collect(),
            })
        })
        .await?
        .map_err(internal)?;

        Ok(Response::new(DescribeIndexResponse {
            stats: Some(stats.clone()),
            failed_shards: 0, // a Node serves one shard; the Gateway sets this on merge
            // A Node is one shard, so its own stats are the (single) per-shard breakdown.
            per_shard: vec![stats],
        }))
    }

    async fn alter_index(
        &self,
        request: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        auth::authorize(&self.auth, "AlterIndex", &request)?;
        let req = request.into_inner();

        check_served(&req.index, &self.index)?;

        let ctx = self.source.as_ref().ok_or_else(|| {
            Status::unimplemented("this node was started without source access for alter planning")
        })?;

        // Resolve the candidate against the *current* source schema, then diff. Reading the
        // schema reconnects to Iceberg (the served definition is a snapshot from build time).
        let reader = IcebergReader::connect(&ctx.iceberg)
            .await
            .map_err(internal)?;
        let source = reader
            .read_source_schema(&ctx.table)
            .await
            .map_err(internal)?;

        let baseline = ctx
            .resolved
            .read()
            .expect("definition lock not poisoned")
            .clone();
        let candidate = resolve_candidate(&req.definition_yaml, &source)?;
        let plan = wire_plan(&baseline, &candidate);

        if req.apply {
            // Apply in-place changes durably, then advance the in-memory baseline so later
            // alters/reindexes see the new definition.
            let applied = apply_in_place(&baseline, candidate)?;
            *ctx.resolved.write().expect("definition lock not poisoned") = applied;
        }
        Ok(Response::new(AlterIndexResponse { plan: Some(plan) }))
    }

    async fn reindex_index(
        &self,
        request: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        auth::authorize(&self.auth, "ReindexIndex", &request)?;
        let req = request.into_inner();

        check_served(&req.index, &self.index)?;

        let ctx = self.source.as_ref().ok_or_else(|| {
            Status::unimplemented("this node was started without source access for reindex")
        })?;

        // Fence: engaging is the single-flight guard (a second reindex is refused) AND
        // it makes the Write service reject new writes for the duration — so the connector can't
        // advance the shard past the rebuild snapshot, a delta the swap would otherwise drop.
        if !ctx.fence.engage() {
            return Err(Status::failed_precondition(
                "a reindex is already in progress for this index",
            ));
        }
        let _fence_guard = ReindexGuard::new(ctx.fence.clone());

        // Rebuild against the current definition (an applied in-place alter may have moved it).
        let resolved = ctx
            .resolved
            .read()
            .expect("definition lock not poisoned")
            .clone();

        // Reshard filter: a non-empty bucket map means rebuild this shard keeping only the
        // docs it owns under the new map — the per-node data step of an online reshard. Empty ⇒ a
        // normal full reindex. A filtered shard may legitimately end up empty (it owns no populated
        // buckets), so the empty-rebuild guard is skipped when filtering.
        let reshard_filter: Option<(ShardRouter, u32)> = if req.bucket_owners.is_empty() {
            None
        } else {
            let map = BucketMap::from_owners(req.bucket_owners.clone())
                .map_err(|e| Status::invalid_argument(format!("bucket map: {e}")))?;
            Some((
                ShardRouter::bucketed(resolved.routing_strategy(), map),
                req.shard_ordinal,
            ))
        };
        let filtering = reshard_filter.is_some();

        // Stream the source into the rebuild: the staging shard is populated chunk by
        // chunk straight off the Iceberg scan, so peak memory is O(one streamed chunk), not
        // O(table).
        let reader = IcebergReader::connect(&ctx.iceberg)
            .await
            .map_err(internal)?;
        // Fix the rebuild snapshot from table metadata (cheap, no scan) before streaming, so every
        // chunk commits at one checkpoint. The rebuilt shard cannot regress the live one:
        // writes are fenced, so the live checkpoint is an ancestor of the head read here. There is
        // no numeric `max(old, snapshot_id)` "monotonicity belt" — snapshot ids are random longs, so
        // a numeric max could actually PICK the stale side; the fence is the guarantee, and the
        // sequence number stamped below is the order.
        let (snapshot_id, sequence) = reader
            .current_snapshot_ordered(&ctx.table)
            .await
            .map_err(internal)?
            .unwrap_or((0, 0));
        let checkpoint = snapshot_id;

        // The source's reported row count — lets us refuse to swap in an empty rebuild if the
        // streamed read came back empty from a non-empty table (data-loss guard).
        let records = reader
            .current_snapshot_records(&ctx.table)
            .await
            .map_err(internal)?;

        // Fail fast if there isn't plausibly enough free disk for the rebuild (old + staging +
        // backup coexist mid-swap) — better than discovering it hours into a multi-GB rebuild.
        let canonical = ctx.store.shard_path(&ctx.shard_id);
        if let Err(msg) = precheck_free_disk(&canonical) {
            return Err(Status::failed_precondition(msg));
        }
        tracing::info!(
            index = %self.index,
            snapshot = snapshot_id,
            checkpoint = %checkpoint,
            "reindex: rebuilding from source (streaming)"
        );

        // The rebuild + atomic on-disk swap is blocking I/O → the blocking pool. The async Iceberg
        // read is driven on that same blocking thread via `block_on`; its sync sink writes each
        // bounded chunk straight into the staging shard (mirrors `Engine::build_from_source`), so
        // peak rebuild memory is O(one chunk), not O(table).
        let store = ctx.store.clone();
        let shard_id = ctx.shard_id.clone();
        let table = ctx.table.clone();
        let index_name = self.index.clone();
        let handle = tokio::runtime::Handle::current();
        let (promoted, doc_count) =
            run_blocking(move || -> Result<(Shard, u64), StoreError> {
                let mut doc_count = 0u64;
                let promoted = store.reindex(&shard_id, &resolved, |shard| {
                    // Sub-commit each streamed chunk in bounded slices with progress — a single
                    // giant commit balloons peak memory and gives no signal on a long op.
                    // Every commit carries the rebuild checkpoint; a per-commit seq keeps each
                    // batch_id unique.
                    let mut seq = 0u64;
                    handle
                        .block_on(
                            reader.read_documents_streamed(&table, &resolved, |mut docs| {
                                // Reshard: drop docs this shard no longer owns under the new
                                // map, so the rebuilt shard holds only its post-reshard buckets.
                                if let Some((router, ordinal)) = &reshard_filter {
                                    docs.retain(|d| router.owns(&d.doc.key, *ordinal));
                                }
                                doc_count += docs.len() as u64;
                                for chunk in docs.chunks(REINDEX_COMMIT_CHUNK) {
                                    seq += 1;
                                    IndexWriter::write(
                                        shard,
                                        &reindex_commit(chunk.to_vec(), checkpoint, sequence, seq),
                                    )
                                    .map_err(|e| e.to_string())?;
                                }
                                // Per-chunk progress at debug (off by default) so a big rebuild
                                // doesn't spam unbounded stderr.
                                tracing::debug!(index = %index_name, doc_count, "reindex: committed chunk");
                                Ok(())
                            }),
                        )
                        .map_err(|e| StoreError::Source(e.to_string()))?;
                    // A 0-doc read from a non-empty source is a broken read — abort before the swap
                    // so the live index is never replaced by an empty one. Skipped for a
                    // reshard rebuild, where a shard may legitimately own no populated buckets.
                    if !filtering {
                        if let Some(reason) = empty_rebuild_abort_reason(doc_count, records) {
                            return Err(StoreError::Source(reason));
                        }
                    }
                    if doc_count == 0 {
                        // Genuinely empty source: still stamp the checkpoint so the shard isn't behind.
                        IndexWriter::write(
                            shard,
                            &reindex_commit(Vec::new(), checkpoint, sequence, 0),
                        )?;
                    }
                    Ok(())
                })?;
                Ok((promoted, doc_count))
            })
            .await?
            .map_err(internal)?;

        let snapshot = promoted.current_snapshot().map_err(internal)?;
        // Install the rebuilt shard as the live one — every service sees it at once.
        self.shard.swap(Arc::new(promoted));
        tracing::info!(index = %self.index, snapshot, "reindex: promoted the rebuilt shard");

        Ok(Response::new(ReindexIndexResponse {
            doc_count,
            snapshot,
        }))
    }

    #[tracing::instrument(name = "admin.reconcile_index", skip_all, err)]
    async fn reconcile_index(
        &self,
        request: Request<ReconcileIndexRequest>,
    ) -> Result<Response<ReconcileIndexResponse>, Status> {
        auth::authorize(&self.auth, "ReconcileIndex", &request)?;
        let req = request.into_inner();
        check_served(&req.index, &self.index)?;
        let ctx = self.source.as_ref().ok_or_else(|| {
            Status::unimplemented(
                "reconcile requires source access (the node was started without a source)",
            )
        })?;
        let resolved = ctx
            .resolved
            .read()
            .expect("definition lock not poisoned")
            .clone();

        // Shard scope: restrict the comparison to the keys THIS shard owns, so the
        // stale-set (indexed keys absent from the read) can't sweep away another shard's keys.
        // Empty `bucket_owners` ⇒ the whole index (single-shard / unsharded).
        let owner_filter: Option<(ShardRouter, u32)> = if req.bucket_owners.is_empty() {
            None
        } else {
            let map = BucketMap::from_owners(req.bucket_owners.clone())
                .map_err(|e| Status::invalid_argument(format!("bucket map: {e}")))?;
            Some((
                ShardRouter::bucketed(resolved.routing_strategy(), map),
                req.shard_ordinal,
            ))
        };
        let ordinal = owner_filter.as_ref().map_or(0, |(_, o)| *o);

        let reader = IcebergReader::connect(&ctx.iceberg)
            .await
            .map_err(internal)?;
        let table = ctx.table.clone();

        // Whole-index count-gate phase (`count_only`): report this shard's live doc count +
        // the source table's total record count, WITHOUT reconciling. The cluster driver aggregates
        // Σ index_count across shards and compares to the source total to skip the row-level reconcile
        // when the index is already in sync — routing-agnostic (covers hash-routed indexes the
        // per-partition gate can't). Metadata + one cheap key enumeration; no rows read or written.
        if req.count_only {
            let source_total = reader
                .current_snapshot_records(&table)
                .await
                .map_err(internal)?
                .unwrap_or(0)
                .max(0) as u64;
            let shard = self.shard.current();
            let index_count = run_blocking(move || shard.key_count(&[]))
                .await?
                .map_err(internal)? as u64;
            return Ok(Response::new(ReconcileIndexResponse {
                index_count,
                source_count: source_total,
                ..Default::default()
            }));
        }

        // Capture the shard's checkpoint BEFORE any source read: the stale-delete only runs if the
        // shard hasn't advanced since (TOCTOU guard), so a concurrent ingest that lands a
        // new key during the scan can't have that key mistaken for stale and deleted.
        let expected_checkpoint = self
            .shard
            .current()
            .current_checkpoint()
            .map_err(internal)?;

        // Per-partition count-gate: unless `full` forces a whole-shard scan, try to
        // reconcile only the partitions whose source record-count differs from the index key-count,
        // skipping the in-sync majority without reading their rows.
        if !req.full {
            if let Some(report) = self
                .count_gated_reconcile(
                    &reader,
                    &table,
                    &resolved,
                    &owner_filter,
                    expected_checkpoint.clone(),
                )
                .await?
            {
                growlerdb_telemetry::sli::drift_reconcile(
                    &self.index,
                    ordinal,
                    report.stale,
                    report.missing,
                );
                return Ok(Response::new(report.into_response()));
            }
        }

        // Full-scan fallback: stream the source, keep only owned docs (peak memory O(owned keys)),
        // and reconcile the whole shard. The path for hash-routed / non-identity-partitioned indexes,
        // an empty source, or a forced `full` sweep.
        let mut owned: Vec<LocatedDoc> = Vec::new();
        reader
            .read_documents_streamed(&table, &resolved, |docs| {
                for d in docs {
                    if owner_filter
                        .as_ref()
                        .is_none_or(|(router, o)| router.owns(&d.doc.key, *o))
                    {
                        owned.push(d);
                    }
                }
                Ok(())
            })
            .await
            .map_err(internal)?;

        let shard = self.shard.current();
        let report = run_blocking(move || {
            crate::engine::apply_drift(&shard, &[], owned, expected_checkpoint)
        })
        .await?
        .map_err(internal)?;

        growlerdb_telemetry::sli::drift_reconcile(
            &self.index,
            ordinal,
            report.deleted as u64,
            report.reindexed as u64,
        );

        Ok(Response::new(ReconcileIndexResponse {
            index_count: report.index_count as u64,
            source_count: report.source_count as u64,
            stale: report.deleted as u64,
            missing: report.reindexed as u64,
            deletes_skipped: report.deletes_skipped,
            ..Default::default()
        }))
    }

    async fn compact_index(
        &self,
        request: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        auth::authorize(&self.auth, "CompactIndex", &request)?;
        let req = request.into_inner();
        check_served(&req.index, &self.index)?;
        // Compaction is local + blocking (Tantivy merge); report the live segment count before/after.
        let shard = self.shard.current();
        let (before, after) = run_blocking(move || -> Result<(u64, u64), StoreError> {
            let before = shard.compaction_health()?.segments;
            shard.compact(&growlerdb_index::CompactionPolicy::default())?;
            let after = shard.compaction_health()?.segments;
            Ok((before, after))
        })
        .await?
        .map_err(internal)?;
        // Segments·merges metric: count the compaction + record the post-merge segments.
        growlerdb_telemetry::sli::compaction(&self.index, before, after);
        Ok(Response::new(CompactIndexResponse {
            segments_before: before,
            segments_after: after,
        }))
    }

    async fn backup_index(
        &self,
        request: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        auth::authorize(&self.auth, "BackupIndex", &request)?;
        let req = request.into_inner();
        check_served(&req.index, &self.index)?;
        let cfg = self.backup.as_ref().ok_or_else(|| {
            Status::unimplemented(
                "this node has no object-storage backup target (start it with GROWLERDB_BACKUP_BUCKET set)",
            )
        })?;
        let prefix = if req.prefix.is_empty() {
            cfg.prefix.clone()
        } else {
            req.prefix
        };
        let shard = self.shard.current();
        // Stage beside the shard dir so segment files hard-link (instant), not copy.
        let staging = shard
            .index_dir()
            .parent()
            .map(|p| p.join(format!(".backup-staging-{}", self.index)))
            .unwrap_or_else(|| std::path::PathBuf::from(format!(".backup-staging-{}", self.index)));
        let m = growlerdb_backup::backup(
            shard.as_ref(),
            &self.index,
            &self.index,
            &staging,
            &cfg.store,
            &prefix,
            None,
        )
        .await
        .map_err(internal)?;
        Ok(Response::new(BackupIndexResponse {
            snapshot: m.snapshot,
            file_count: m.files.len() as u64,
            created_ms: m.created_ms as u64,
            prefix,
        }))
    }

    async fn backup_status(
        &self,
        request: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        auth::authorize(&self.auth, "BackupStatus", &request)?;
        let Some(cfg) = &self.backup else {
            return Ok(Response::new(BackupStatusResponse::default())); // configured = false
        };
        match growlerdb_backup::read_manifest(&cfg.store, &cfg.prefix).await {
            Ok(m) => Ok(Response::new(BackupStatusResponse {
                configured: true,
                present: true,
                snapshot: m.snapshot,
                created_ms: m.created_ms as u64,
                file_count: m.files.len() as u64,
            })),
            // No manifest yet (or unreadable) → configured but nothing backed up.
            Err(_) => Ok(Response::new(BackupStatusResponse {
                configured: true,
                present: false,
                ..Default::default()
            })),
        }
    }
}

/// Docs per reindex commit — each streamed source chunk is sub-committed in bounded slices so a
/// rebuild commits incrementally with progress instead of one giant commit. Peak *read*
/// memory is bounded by the streamed chunk size, independent of table size.
const REINDEX_COMMIT_CHUNK: usize = 10_000;

/// Build the durable commit for one reindex chunk — pure, no I/O, so
/// it's unit-testable without a stack. Every chunk carries the rebuild `checkpoint` (with its
/// lineage sequence number when the table has one — v2); the per-chunk `seq` keeps
/// each `batch_id` unique (commits are idempotent/replayable by id).
fn reindex_commit(docs: Vec<LocatedDoc>, checkpoint: i64, sequence: i64, seq: u64) -> CommitBatch {
    let cp = if sequence > 0 {
        SourceCheckpoint::iceberg_ordered(checkpoint, sequence)
    } else {
        SourceCheckpoint::iceberg(checkpoint)
    };
    CommitBatch::from_upserts(docs, cp, format!("reindex-{checkpoint}-{seq}"))
}

/// Guard against a reindex silently rebuilding an **empty** index over a live shard:
/// if the streamed read produced 0 docs but the source snapshot reports rows, the read is broken
/// (e.g. a delete-in-history the changelog read mishandles) — so the swap must be aborted rather
/// than destroy the served data. `records` is the source's reported row count (`None` ⇒ unknown ⇒
/// allow). Returns the abort reason, or `None` when the empty rebuild is legitimate (genuinely
/// empty source) or non-empty. Mirrors `Engine::build_from_source`'s `EmptyReadFromNonEmptySource`
/// guard, but on the reindex path the stakes are higher: the swap replaces a *live* index.
fn empty_rebuild_abort_reason(doc_count: u64, records: Option<i64>) -> Option<String> {
    match records {
        Some(n) if doc_count == 0 && n > 0 => Some(format!(
            "reindex read 0 docs from a source that reports {n} rows — refusing to swap in an empty \
             index (a delete-in-history read bug); the live index is unchanged"
        )),
        _ => None,
    }
}

/// Headroom multiplier over the current index size for the free-disk precheck: the old, staging,
/// and (briefly) backup copies coexist during the swap.
const REINDEX_DISK_HEADROOM: u64 = 3;

/// Refuse a reindex up front if free disk plausibly can't hold the rebuild — better than failing
/// hours in. Compares ≈`REINDEX_DISK_HEADROOM`× the current index size to the free space
/// at the shard's parent dir. A probe failure (`None`) skips the check rather than blocking.
fn precheck_free_disk(shard_dir: &Path) -> Result<(), String> {
    let size = dir_size(shard_dir);
    let need = size.saturating_mul(REINDEX_DISK_HEADROOM);
    let probe_at = shard_dir.parent().unwrap_or(shard_dir);
    match free_disk_bytes(probe_at) {
        Some(free) if free < need => Err(format!(
            "insufficient free disk for reindex: need ~{need} bytes (≈{REINDEX_DISK_HEADROOM}× the \
             {size}-byte index), only {free} free at {}",
            probe_at.display()
        )),
        _ => Ok(()),
    }
}

/// Total size in bytes of the files under `dir` (recursive); 0 if absent/unreadable.
fn dir_size(dir: &Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        match entry.metadata() {
            Ok(meta) if meta.is_dir() => total += dir_size(&entry.path()),
            Ok(meta) => total += meta.len(),
            Err(_) => {}
        }
    }
    total
}

/// Free bytes available to an unprivileged process at `path` (via `statvfs`); `None` if the probe
/// fails, in which case the [`precheck_free_disk`] is skipped rather than blocking a reindex.
fn free_disk_bytes(path: &Path) -> Option<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).ok()?;
    // SAFETY: `stat` is fully written by `statvfs` when it returns 0; we read it only then.
    let mut stat: libc::statvfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statvfs(c_path.as_ptr(), &mut stat) } != 0 {
        return None;
    }
    Some((stat.f_bavail as u64).saturating_mul(stat.f_frsize as u64))
}

/// Render a source checkpoint for the stats line (`"none"` before any commit).
fn render_checkpoint(checkpoint: Option<SourceCheckpoint>) -> String {
    match checkpoint {
        Some(cp) => format!("iceberg_snapshot:{}", cp.snapshot_id()),
        None => "none".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc, SourceField,
        SourceSchema, SourceType, Value,
    };
    use growlerdb_index::{LocalIndexStore, ShardId};
    use std::collections::BTreeMap;
    use tonic::Code;

    fn service(root: &std::path::Path) -> AdminService {
        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let put = |id: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }
        };
        // Two commits → two generations; checkpoint at iceberg snapshot 7.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![put("a"), put("b")],
                SourceCheckpoint::iceberg(3),
                "b1",
            ),
        )
        .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![put("c")], SourceCheckpoint::iceberg(7), "b2"),
        )
        .unwrap();
        AdminService::new(Arc::new(shard), "docs")
    }

    // The streamed-rebuild commit builder is pure, so its checkpoint/batch-id contract is
    // testable without a stack (the end-to-end streaming reindex stays `#[ignore]`-gated on Polaris).
    #[test]
    fn reindex_commit_carries_checkpoint_and_unique_ids() {
        let doc = |id: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }
        };

        // Every chunk of one rebuild commits at the same checkpoint (with the table's
        // lineage sequence number when it has one — v2; 0 ⇒ unknown, plain)...
        let c1 = reindex_commit(vec![doc("a"), doc("b")], 42, 7, 1);
        let c2 = reindex_commit(vec![doc("c")], 42, 7, 2);
        assert_eq!(c1.checkpoint, SourceCheckpoint::iceberg_ordered(42, 7));
        assert_eq!(c2.checkpoint, SourceCheckpoint::iceberg_ordered(42, 7));
        assert_eq!(
            reindex_commit(vec![doc("d")], 42, 0, 3).checkpoint,
            SourceCheckpoint::iceberg(42),
            "a v1 table (sequence 0) stamps no order"
        );
        assert_eq!(c1.upserts().count(), 2);
        assert_eq!(c2.upserts().count(), 1);
        // ...but each carries a distinct, replayable batch id (seq-keyed), so a crash-replay of one
        // chunk can't collide with another.
        assert_eq!(c1.batch_id, "reindex-42-1");
        assert_eq!(c2.batch_id, "reindex-42-2");
        assert_ne!(c1.batch_id, c2.batch_id);

        // The empty-source checkpoint commit (seq 0) is well-formed and carries no docs.
        let empty = reindex_commit(Vec::new(), 7, 0, 0);
        assert_eq!(empty.upserts().count(), 0);
        assert_eq!(empty.batch_id, "reindex-7-0");
        assert_eq!(empty.checkpoint, SourceCheckpoint::iceberg(7));
    }

    // A reindex that reads 0 docs from a *non-empty* source must abort before the swap so
    // the live index is never replaced by an empty one. Pure, so testable without a stack.
    #[test]
    fn empty_rebuild_aborts_only_on_nonempty_source() {
        // 0 docs but the source reports rows → abort (the broken-read / data-loss case).
        let reason = empty_rebuild_abort_reason(0, Some(2_000_000)).expect("should abort");
        assert!(
            reason.contains("2000000"),
            "names the source row count: {reason}"
        );
        assert!(
            reason.contains("unchanged"),
            "promises the live index is preserved: {reason}"
        );

        // Legitimate empty rebuilds and normal rebuilds proceed.
        assert_eq!(empty_rebuild_abort_reason(0, Some(0)), None); // genuinely empty source
        assert_eq!(empty_rebuild_abort_reason(0, None), None); // unknown count ⇒ allow
        assert_eq!(empty_rebuild_abort_reason(5, Some(5)), None); // read matches
        assert_eq!(empty_rebuild_abort_reason(5, Some(9)), None); // non-empty read, never blocked
    }

    #[tokio::test(flavor = "current_thread")]
    async fn compact_merges_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()); // two commits ⇒ two segments under NoMergePolicy
        let resp = svc
            .compact_index(Request::new(CompactIndexRequest {
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.segments_before >= 2, "before={}", resp.segments_before);
        assert_eq!(resp.segments_after, 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backup_unconfigured_is_unimplemented_and_status_reports_it() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()); // no with_backup
        let err = svc
            .backup_index(Request::new(BackupIndexRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented);
        let st = svc
            .backup_status(Request::new(BackupStatusRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(!st.configured);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn backup_to_object_store_round_trips_status() {
        let tmp = tempfile::tempdir().unwrap();
        let store = growlerdb_backup::fs_store(tmp.path().join("bk")).unwrap();
        let svc = service(tmp.path()).with_backup(store, "node-backups");

        // Configured, but nothing backed up yet.
        let st0 = svc
            .backup_status(Request::new(BackupStatusRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(st0.configured && !st0.present);

        // Run a backup → files written; status now present at the same snapshot.
        let b = svc
            .backup_index(Request::new(BackupIndexRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(b.file_count > 0);
        let st1 = svc
            .backup_status(Request::new(BackupStatusRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(st1.configured && st1.present);
        assert_eq!(st1.snapshot, b.snapshot);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn describe_reports_index_stats() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let stats = svc
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .stats
            .unwrap();
        assert_eq!(stats.name, "docs");
        assert_eq!(stats.num_docs, 3); // a, b, c
        assert!(stats.generation_count >= 1); // ≥1 Tantivy segment (merge policy varies)
        assert_eq!(stats.checkpoint, "iceberg_snapshot:7");
        assert!(stats.snapshot >= 2);

        // The full mapping rides describe: name, type, and capability flags per field — the
        // schema clients (console pickers, MCP agents) compose queries from.
        assert_eq!(stats.fields.len(), 1);
        let f = &stats.fields[0];
        assert_eq!((f.name.as_str(), f.r#type.as_str()), ("id", "KEYWORD"));
        assert!(f.indexed, "KEYWORD is term-queryable by default");
        assert!(!f.fast);
        assert!(!f.cached);

        // Naming the served index explicitly also works.
        assert!(svc
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: "docs".into(),
            }))
            .await
            .is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn describe_surfaces_date_fields_for_the_time_filter() {
        // Describe reports the index's DATE columns so the console time filter can list them.
        let tmp = tempfile::tempdir().unwrap();
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("ts", SourceType::Date),
                SourceField::new("city", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: ts, type: DATE, fast: true }, { path: city, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(tmp.path())
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let svc = AdminService::new(std::sync::Arc::new(shard), "docs");
        let stats = svc
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .stats
            .unwrap();
        // Only the DATE column is a time field (not the KEYWORD `id`/`city`).
        assert_eq!(stats.time_fields, vec!["ts".to_string()]);
        // A non-vector index reports no vector fields.
        assert!(stats.vector_fields.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn describe_surfaces_vector_fields_for_semantic_search() {
        // Describe reports the index's VECTOR fields (path + embedding config) so the console can
        // offer a semantic/hybrid vector-field picker.
        let tmp = tempfile::tempdir().unwrap();
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: body_vec, type: VECTOR, vector: { dims: 8, model: test-model, source_field: body } } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(tmp.path())
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let svc = AdminService::new(std::sync::Arc::new(shard), "docs");
        let stats = svc
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .stats
            .unwrap();
        assert_eq!(stats.vector_fields.len(), 1);
        let vf = &stats.vector_fields[0];
        assert_eq!(vf.name, "body_vec");
        assert_eq!(vf.source_field, "body");
        assert_eq!(vf.model, "test-model");
        assert_eq!(vf.dims, 8);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn describing_another_index_is_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let err = svc
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: "other".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn auth_gates_describe() {
        use crate::auth::{AuthContext, AuthDenied, AuthHook};
        struct DenyAll;
        impl AuthHook for DenyAll {
            fn authorize(&self, _ctx: &AuthContext) -> Result<(), AuthDenied> {
                Err(AuthDenied::new("nope"))
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let base = service(tmp.path());
        let svc = AdminService::with_auth(base.shard.clone(), "docs", Arc::new(DenyAll));
        let err = svc
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
    }

    /// A source schema (id/city/rank) + a current EXPLICIT definition (id, city) for the
    /// alter planner — no Iceberg needed, so the alter logic is fully unit-tested.
    fn alter_fixtures() -> (ResolvedIndex, SourceSchema) {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("city", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let current = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        (current, src)
    }

    /// Plan via the split helpers: resolve the candidate, then diff it.
    fn plan(current: &ResolvedIndex, yaml: &str, src: &SourceSchema) -> WireAlterPlan {
        wire_plan(current, &resolve_candidate(yaml, src).unwrap())
    }

    #[test]
    fn plan_alter_detects_noop_reindex_and_in_place() {
        let (current, src) = alter_fixtures();

        // Identical definition → no-op.
        let same = "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD } ] }\n";
        let p = plan(&current, same, &src);
        assert!(p.is_noop);
        assert!(!p.requires_reindex);

        // Adding a field changes the segment schema → requires a reindex, with a reason.
        let add = "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD }, { path: rank, type: LONG, fast: true } ] }\n";
        let p = plan(&current, add, &src);
        assert!(p.requires_reindex);
        assert!(!p.is_noop);
        assert!(p.reindex_reasons.iter().any(|r| r.contains("rank")));

        // A rename is metadata only → in-place, no reindex.
        let rename = "name: docs2\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD } ] }\n";
        let p = plan(&current, rename, &src);
        assert!(!p.requires_reindex);
        assert!(!p.is_noop);
        assert!(p.in_place_changes.iter().any(|c| c.contains("renamed")));

        // A candidate that fails to resolve is the operator's error → InvalidArgument.
        let err = resolve_candidate("name: docs\nmapping: [not valid", &src).unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[test]
    fn apply_in_place_rejects_reindex_rename_and_restart_required_policy() {
        let (current, src) = alter_fixtures();

        // A read-time policy change (flip `sensitive`) is in-place per `alter_to`, but the running
        // shard keeps its built schema → restart-required, rejected. Nothing persisted.
        let policy = resolve_candidate(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD, sensitive: true } ] }\n",
            &src,
        )
        .unwrap();
        let err = apply_in_place(&current, policy).unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(err.message().contains("restart"), "{}", err.message());

        // A reindex-requiring change (add a field) is refused → FailedPrecondition.
        let add = resolve_candidate(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD }, { path: rank, type: LONG, fast: true } ] }\n",
            &src,
        )
        .unwrap();
        let err = apply_in_place(&current, add).unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);

        // A rename is refused even though it is in-place per `alter_to`.
        let rename = resolve_candidate(
            "name: docs2\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD } ] }\n",
            &src,
        )
        .unwrap();
        let err = apply_in_place(&current, rename).unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[test]
    fn apply_in_place_noop_succeeds_without_error() {
        // Applying the identical definition is the one safe live apply: a no-op returns the
        // baseline (and, being pure, writes nothing).
        let (current, _src) = alter_fixtures();
        let applied = apply_in_place(&current, current.clone()).unwrap();
        assert_eq!(applied.name, current.name);
        assert_eq!(applied.fields.len(), current.fields.len());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn second_concurrent_reindex_is_rejected() {
        // A reindex already in flight holds the fence; a second is rejected rather than
        // trampling the shared staging dirs.
        let (resolved, _src) = alter_fixtures();
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard_id = ShardId::single("docs");
        let shard = store.create_shard(&shard_id, &resolved).unwrap();
        let svc = AdminService::new(Arc::new(shard), "docs").with_source(
            resolved,
            store,
            shard_id,
            IcebergConfig::local(),
            "g.docs",
            ReindexFence::new(),
        );

        // Simulate a reindex already running (the fence is engaged).
        assert!(svc.source.as_ref().unwrap().fence.engage());
        let err = svc
            .reindex_index(Request::new(ReindexIndexRequest {
                index: String::new(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alter_without_source_access_is_unimplemented() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()); // built via `new` ⇒ no alter context
        let err = svc
            .alter_index(Request::new(AlterIndexRequest {
                index: String::new(),
                definition_yaml: "name: docs".into(),
                apply: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn altering_another_index_is_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        // The served-index guard runs before the source check, so this is NotFound, not
        // Unimplemented, even though this service has no alter context.
        let err = svc
            .alter_index(Request::new(AlterIndexRequest {
                index: "other".into(),
                definition_yaml: String::new(),
                apply: true,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reindex_without_source_access_is_unimplemented() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()); // built via `new` ⇒ no source context
        let err = svc
            .reindex_index(Request::new(ReindexIndexRequest {
                index: String::new(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reindexing_another_index_is_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let err = svc
            .reindex_index(Request::new(ReindexIndexRequest {
                index: "other".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reconcile_without_source_access_is_unimplemented() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()); // built via `new` ⇒ no source context
        let err = svc
            .reconcile_index(Request::new(ReconcileIndexRequest {
                index: String::new(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reconciling_another_index_is_not_found() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let err = svc
            .reconcile_index(Request::new(ReconcileIndexRequest {
                index: "other".into(),
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }
}
