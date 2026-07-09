//! The Node **Write** gRPC service ([Design 06]) — the Rust side of the JVM↔Rust
//! ingestion boundary. Adapts the in-process [`IndexWriter`] (stage/commit +
//! `DocOp`, task-59) to the wire `Write` service (task-60 proto), so the Spark
//! changelog connector (task-11) can apply batches to a shard over gRPC.
//!
//! [Design 06]: ../../../design/06-service-architecture.md

use std::sync::Arc;

use growlerdb_core::{CommitBatch, IndexWriter};
use growlerdb_proto::v1::{
    Error as WireError, GetCheckpointRequest, GetCheckpointResponse, WriteRequest, WriteResponse,
};
use growlerdb_proto::{to_status, Write, WriteServer};
use tonic::{Code, Request, Response, Status};

use crate::fence::ReindexFence;
use crate::shard_handle::ShardHandle;

/// Max decoded size of an inbound `Write` request (task-113). The ingestion connector commits a
/// whole changelog window in one request, so a catch-up after downtime — or an initial backfill of
/// an existing table — can far exceed tonic's default **4 MiB** decode cap (which surfaces as
/// `OUT_OF_RANGE: decoded message length too large` and kills the streaming query). 256 MiB is a
/// generous safety margin; the durable fix is to **bound the per-commit window** in the connector so
/// a single batch can't grow without limit (the rest of task-113).
const MAX_WRITE_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

/// Map an index [`StoreError`](growlerdb_index::StoreError) from a commit to a gRPC status. A
/// [`CheckpointGap`](growlerdb_index::StoreError::CheckpointGap) is a **non-retryable**
/// `FAILED_PRECONDITION` (task-194): the batch doesn't continue from the shard's checkpoint, so the
/// connector must resolve the discontinuity (reindex/reconcile) rather than retry the same batch.
/// Everything else is an internal failure.
fn store_error_to_status(e: growlerdb_index::StoreError) -> Status {
    match &e {
        growlerdb_index::StoreError::CheckpointGap { .. } => to_status(
            Code::FailedPrecondition,
            WireError::new("CHECKPOINT_GAP", e.to_string()),
        ),
        _ => to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())),
    }
}

/// A `Write` service over one shard, with bounded **admission control**: at most
/// `max_inflight` writes run concurrently; further requests get
/// `RESOURCE_EXHAUSTED` rather than queueing unboundedly (backpressure to the
/// connector). Index work is blocking, so it runs on the blocking pool.
#[derive(Clone)]
pub struct WriteService {
    shard: ShardHandle,
    /// The index this node serves — used only to label the ingestion-throughput metric so the
    /// console can chart per-index throughput (task-136).
    index_name: String,
    inflight: Arc<tokio::sync::Semaphore>,
    /// The admission ceiling (`inflight`'s initial permits) — kept so the write-queue-depth gauge can
    /// report in-flight commits (`max_inflight - available_permits`) as a backpressure signal (task-233).
    max_inflight: usize,
    /// Shared reindex write-fence (task-71): while a reindex rebuilds, writes are rejected with a
    /// retryable status so the connector can't advance the shard past the rebuild snapshot (a
    /// delta the swap would drop). Default-open; wired to the Admin service via [`with_fence`].
    ///
    /// [`with_fence`]: Self::with_fence
    fence: ReindexFence,
    /// Set when the node booted against a **recreated source** (task-114): the source table's
    /// `table-uuid` no longer matches the one this index was built from, so the index is stale.
    /// Writes and checkpoint reads are then refused with a non-retryable `FAILED_PRECONDITION`
    /// (`SOURCE_RECREATED`) — so the connector stops advancing a stale index and the control-plane
    /// renders the shard `source_recreated`. Search still serves the (stale) index read-only for
    /// inspection; a reindex re-anchors and clears it. Set once at startup via
    /// [`with_source_recreated`](Self::with_source_recreated).
    source_recreated: bool,
}

impl WriteService {
    /// A Write service over `shard`, admitting at most `max_inflight` concurrent writes.
    /// Accepts an `Arc<Shard>` (fresh handle) or a shared [`ShardHandle`]. Writes are unfenced
    /// until [`with_fence`](Self::with_fence) shares the reindex fence.
    pub fn new(
        shard: impl Into<ShardHandle>,
        index_name: impl Into<String>,
        max_inflight: usize,
    ) -> Self {
        Self {
            shard: shard.into(),
            index_name: index_name.into(),
            inflight: Arc::new(tokio::sync::Semaphore::new(max_inflight.max(1))),
            max_inflight: max_inflight.max(1),
            fence: ReindexFence::new(),
            source_recreated: false,
        }
    }

    /// Share the reindex [`ReindexFence`] with this service so writes are rejected while a reindex
    /// is in progress (task-71). Wire the same fence into the Admin service's `with_source`.
    pub fn with_fence(mut self, fence: ReindexFence) -> Self {
        self.fence = fence;
        self
    }

    /// Mark this node as serving a **stale index over a recreated source** (task-114): writes and
    /// checkpoint reads are refused so the index can't advance and the drift is surfaced. Set at
    /// serve startup when the recorded source `table-uuid` doesn't match the live table.
    pub fn with_source_recreated(mut self, recreated: bool) -> Self {
        self.source_recreated = recreated;
        self
    }

    /// The non-retryable status returned while the source is recreated (the index is stale).
    fn source_recreated_status() -> Status {
        to_status(
            Code::FailedPrecondition,
            WireError::new(
                "SOURCE_RECREATED",
                "source table was recreated; this index is stale (its keys won't hydrate) — reindex it",
            ),
        )
    }

    /// Wrap as a mountable tonic [`WriteServer`], raising the inbound decode cap to
    /// [`MAX_WRITE_MESSAGE_BYTES`] so large catch-up/backfill commits aren't rejected (task-113).
    pub fn into_server(self) -> WriteServer<Self> {
        WriteServer::new(self).max_decoding_message_size(MAX_WRITE_MESSAGE_BYTES)
    }

    /// Admit a write or signal backpressure: refuse (`RESOURCE_EXHAUSTED`) rather
    /// than queue unboundedly when all in-flight slots are taken.
    fn admit(&self) -> Result<tokio::sync::OwnedSemaphorePermit, Status> {
        self.inflight.clone().try_acquire_owned().map_err(|_| {
            to_status(
                Code::ResourceExhausted,
                WireError::new("RESOURCE_EXHAUSTED", "write admission buffer full"),
            )
        })
    }
}

#[tonic::async_trait]
impl Write for WriteService {
    #[tracing::instrument(name = "node.write", skip_all, err)]
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        // Recreated-source guard (task-114): refuse to advance a stale index — non-retryable, so
        // the connector stops rather than retrying forever (it must reindex).
        if self.source_recreated {
            return Err(Self::source_recreated_status());
        }
        // Reindex fence (task-71): reject writes while a reindex rebuilds, with a retryable
        // status, so the connector can't advance the shard past the rebuild snapshot (the swap
        // would drop that delta and regress the checkpoint). The connector retries and resumes
        // once the reindex completes.
        if self.fence.is_engaged() {
            return Err(to_status(
                Code::Unavailable,
                WireError::new(
                    "REINDEX_IN_PROGRESS",
                    "a reindex is rebuilding this index; retry shortly",
                ),
            ));
        }

        // Admission control: refuse rather than queue when saturated.
        let permit = self.admit()?;
        // Backpressure signal (task-233): in-flight commits after admitting this write. It pins at
        // max_inflight when the connector out-runs the commit path — at which point further writes get
        // RESOURCE_EXHAUSTED (visible as the connector's write_retries), so lag climbs. Sampled here on
        // each admitted write (a streaming workload writes continuously).
        growlerdb_telemetry::sli::write_queue_depth(
            &self.index_name,
            (self.max_inflight - self.inflight.available_permits()) as u64,
        );

        let batch: CommitBatch = request
            .into_inner()
            .batch
            .ok_or_else(|| {
                to_status(
                    Code::InvalidArgument,
                    WireError::new("INVALID_ARGUMENT", "WriteRequest.batch is required"),
                )
            })?
            .try_into()
            .map_err(|e: growlerdb_proto::MissingField| {
                to_status(
                    Code::InvalidArgument,
                    WireError::new("INVALID_ARGUMENT", e.to_string()),
                )
            })?;

        // The write is blocking (redb + Tantivy); keep it off the async runtime.
        let ops = batch.ops.len() as u64;
        let shard = self.shard.current();
        let started = std::time::Instant::now();
        let snapshot = tokio::task::spawn_blocking(move || {
            // Hold the admission permit for the TRUE duration of the blocking commit (task-194).
            // `spawn_blocking` tasks can't be cancelled, so when a client gives up (its deadline
            // fires and tonic drops this handler future), the commit keeps running. Moving the permit
            // in — rather than leaving it on the dropped future's frame, where it releases early —
            // keeps that orphaned commit occupying its slot until it actually finishes. New writes
            // then get RESOURCE_EXHAUSTED (retryable backpressure) and back off, instead of the Node
            // spawning unbounded concurrent commits that fight the compaction I/O storm and thrash
            // the shard's write lock — the contention that detonated the silent-loss event.
            let _permit = permit;
            IndexWriter::write(&*shard, &batch)
        })
        .await
        .map_err(|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())))?
        .map_err(store_error_to_status)?;
        // Ingestion throughput + write-latency SLIs (task-39/136/233): committed doc-ops per write and
        // the wall-clock stage+commit time, both labelled by index — the latency localizes an ingest
        // ceiling to the commit path when node CPU stays flat.
        growlerdb_telemetry::sli::ingested_docs(&self.index_name, ops);
        growlerdb_telemetry::sli::write(&self.index_name, started.elapsed().as_secs_f64());

        Ok(Response::new(WriteResponse {
            snapshot: snapshot.0,
        }))
    }

    async fn get_checkpoint(
        &self,
        _request: Request<GetCheckpointRequest>,
    ) -> Result<Response<GetCheckpointResponse>, Status> {
        // Recreated-source guard (task-114): a stale index must not hand back a resume point — the
        // `FAILED_PRECONDITION` both stops the connector and lets the control-plane render the shard
        // `source_recreated` (distinct from a transport-level `unreachable`).
        if self.source_recreated {
            return Err(Self::source_recreated_status());
        }
        // A cheap redb read — not admission-controlled (it isn't index work and
        // must stay available so a restarting connector can always resume).
        let shard = self.shard.current();
        let (checkpoint, snapshot) =
            tokio::task::spawn_blocking(move || -> Result<_, growlerdb_index::StoreError> {
                Ok((shard.current_checkpoint()?, shard.current_snapshot()?))
            })
            .await
            .map_err(|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())))?
            .map_err(|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())))?;

        Ok(Response::new(GetCheckpointResponse {
            checkpoint: checkpoint.map(Into::into),
            snapshot,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{IndexDefinition, SourceField, SourceSchema, SourceType};
    use growlerdb_index::{LocalIndexStore, Shard, ShardId};

    fn shard(dir: &std::path::Path) -> Arc<Shard> {
        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: ALL }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let store = LocalIndexStore::open(dir).unwrap();
        Arc::new(store.create_shard(&ShardId::single("docs"), &idx).unwrap())
    }

    #[tokio::test]
    async fn get_checkpoint_reflects_the_committed_batch() {
        use growlerdb_core::{CompositeKey, Document, LocatedDoc, SourceCheckpoint, Value};
        use growlerdb_proto::v1::{source_checkpoint::Kind, GetCheckpointRequest};

        let tmp = tempfile::tempdir().unwrap();
        let svc = WriteService::new(shard(tmp.path()), "docs", 4);

        // Before any write: no checkpoint, snapshot 0.
        let before = svc
            .get_checkpoint(Request::new(GetCheckpointRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert!(
            before.checkpoint.is_none(),
            "no checkpoint before first commit"
        );
        assert_eq!(before.snapshot, 0);

        // Commit a batch checkpointed at Iceberg snapshot 42.
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("id".to_string(), Value::from("doc-1"));
        let doc = LocatedDoc {
            doc: Document::new(key, fields),
            iceberg_file: "data/f0.parquet".into(),
            row_position: 0,
        };
        let batch = CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(42), "b1");
        svc.write(Request::new(WriteRequest {
            batch: Some(batch.into()),
        }))
        .await
        .unwrap();

        // After: the checkpoint the connector resumes from is snapshot 42.
        let after = svc
            .get_checkpoint(Request::new(GetCheckpointRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(after.snapshot, 1, "one commit → index snapshot 1");
        match after.checkpoint.and_then(|c| c.kind) {
            Some(Kind::IcebergSnapshot(id)) => assert_eq!(id, 42),
            other => panic!("expected IcebergSnapshot(42), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn writes_are_rejected_while_the_reindex_fence_is_engaged() {
        use growlerdb_core::{CompositeKey, Document, LocatedDoc, SourceCheckpoint, Value};

        let tmp = tempfile::tempdir().unwrap();
        let fence = ReindexFence::new();
        let svc = WriteService::new(shard(tmp.path()), "docs", 4).with_fence(fence.clone());

        let batch = || {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("id".to_string(), Value::from("doc-1"));
            let doc = LocatedDoc {
                doc: Document::new(key, fields),
                iceberg_file: "f".into(),
                row_position: 0,
            };
            WriteRequest {
                batch: Some(
                    CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(1), "b1").into(),
                ),
            }
        };

        // Fence open → the write goes through.
        svc.write(Request::new(batch())).await.unwrap();

        // A reindex engages the fence → writes are refused with a retryable status.
        fence.engage();
        let err = svc.write(Request::new(batch())).await.unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);

        // After the reindex releases it, writes resume.
        fence.release();
        svc.write(Request::new(batch())).await.unwrap();
    }

    #[tokio::test]
    async fn source_recreated_refuses_writes_and_checkpoint_non_retryably() {
        use growlerdb_core::{CompositeKey, Document, LocatedDoc, SourceCheckpoint, Value};

        let tmp = tempfile::tempdir().unwrap();
        // A node booted against a recreated source (task-114) serves DEGRADED: writes and checkpoint
        // reads are refused with FAILED_PRECONDITION (non-retryable — the connector must reindex,
        // and the control-plane renders the shard `source_recreated`, not `unreachable`).
        let svc = WriteService::new(shard(tmp.path()), "docs", 4).with_source_recreated(true);

        let req = || {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("id".to_string(), Value::from("doc-1"));
            let doc = LocatedDoc {
                doc: Document::new(key, fields),
                iceberg_file: "f".into(),
                row_position: 0,
            };
            WriteRequest {
                batch: Some(
                    CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(1), "b1").into(),
                ),
            }
        };

        let err = svc.write(Request::new(req())).await.unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        let err = svc
            .get_checkpoint(Request::new(GetCheckpointRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn continuity_guard_rejects_a_gap_batch_non_retryably() {
        use growlerdb_core::{CompositeKey, Document, LocatedDoc, SourceCheckpoint, Value};

        let tmp = tempfile::tempdir().unwrap();
        let svc = WriteService::new(shard(tmp.path()), "docs", 4);

        let batch = |from: Option<i64>, to: i64, id: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("id".to_string(), Value::from("doc-1"));
            let doc = LocatedDoc {
                doc: Document::new(key, fields),
                iceberg_file: "f".into(),
                row_position: 0,
            };
            WriteRequest {
                batch: Some(
                    CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(to), id)
                        .with_from_checkpoint(from.map(SourceCheckpoint::iceberg))
                        .into(),
                ),
            }
        };

        // Bootstrap (from = None) commits: the shard is now at checkpoint 1.
        svc.write(Request::new(batch(None, 1, "b1"))).await.unwrap();

        // A NEW batch that resumes from 99 — a checkpoint this shard never reached — is a gap:
        // refused with FAILED_PRECONDITION (non-retryable), so the connector can't advance the
        // checkpoint forward over unapplied data.
        let err = svc
            .write(Request::new(batch(Some(99), 100, "b-gap")))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(
            err.message().contains("checkpoint gap"),
            "{}",
            err.message()
        );

        // The contiguous batch (from = 1, the current checkpoint) is accepted and advances to 2.
        svc.write(Request::new(batch(Some(1), 2, "b2")))
            .await
            .unwrap();
        let after = svc
            .get_checkpoint(Request::new(GetCheckpointRequest::default()))
            .await
            .unwrap()
            .into_inner();
        match after.checkpoint.and_then(|c| c.kind) {
            Some(growlerdb_proto::v1::source_checkpoint::Kind::IcebergSnapshot(id)) => {
                assert_eq!(id, 2)
            }
            other => panic!("expected IcebergSnapshot(2), got {other:?}"),
        }
    }

    #[tokio::test]
    async fn empty_batch_advances_the_checkpoint_in_lockstep() {
        use growlerdb_core::{CompositeKey, Document, LocatedDoc, SourceCheckpoint, Value};
        use growlerdb_proto::v1::source_checkpoint::Kind;

        let tmp = tempfile::tempdir().unwrap();
        let svc = WriteService::new(shard(tmp.path()), "docs", 4);

        // A data batch → checkpoint 1, index snapshot 1.
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
        let mut fields = std::collections::BTreeMap::new();
        fields.insert("id".to_string(), Value::from("doc-1"));
        let doc = LocatedDoc {
            doc: Document::new(key, fields),
            iceberg_file: "f".into(),
            row_position: 0,
        };
        svc.write(Request::new(WriteRequest {
            batch: Some(
                CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(1), "b1").into(),
            ),
        }))
        .await
        .unwrap();

        // An EMPTY batch (no ops) that resumes from checkpoint 1 still advances the shard's
        // checkpoint to 2 (lockstep) — but adds no new index snapshot.
        svc.write(Request::new(WriteRequest {
            batch: Some(
                CommitBatch::new(vec![], SourceCheckpoint::iceberg(2), "b2-empty")
                    .with_from_checkpoint(Some(SourceCheckpoint::iceberg(1)))
                    .into(),
            ),
        }))
        .await
        .unwrap();

        let after = svc
            .get_checkpoint(Request::new(GetCheckpointRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(after.snapshot, 1, "empty batch adds no index snapshot");
        match after.checkpoint.and_then(|c| c.kind) {
            Some(Kind::IcebergSnapshot(id)) => assert_eq!(id, 2, "checkpoint advanced on empty"),
            other => panic!("expected IcebergSnapshot(2), got {other:?}"),
        }

        // Idempotent replay of the empty advance is a no-op (batch_id dedup): still at 2.
        svc.write(Request::new(WriteRequest {
            batch: Some(
                CommitBatch::new(vec![], SourceCheckpoint::iceberg(2), "b2-empty")
                    .with_from_checkpoint(Some(SourceCheckpoint::iceberg(1)))
                    .into(),
            ),
        }))
        .await
        .unwrap();
        let again = svc
            .get_checkpoint(Request::new(GetCheckpointRequest::default()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(again.snapshot, 1);
    }

    #[test]
    fn admit_signals_backpressure_when_saturated() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = WriteService::new(shard(tmp.path()), "docs", 1);

        let held = svc.admit().expect("first admit succeeds");
        let err = svc.admit().expect_err("second admit refused");
        assert_eq!(err.code(), Code::ResourceExhausted);

        drop(held); // releasing a slot lets the next write in
        assert!(svc.admit().is_ok(), "admission recovers after release");
    }
}
