//! **Dynamic windowed streaming ingest**: a node-side `Write` gRPC service that routes
//! each incoming doc to its **time-window shard** — creating the window shard on first write and
//! publishing it so it becomes **live-queryable** (mux + in-process gateway) with no restart. This is
//! the streaming counterpart to the batch [`write_windowed`](growlerdb_index::LocalIndexStore) that
//! builds all windows up front; here windows form continuously as the ingest timeline advances.
//!
//! Placement is decided elsewhere (the control plane): the connector resolves a
//! window's owning node and streams that window's rows here, so a given node only ever receives —
//! and creates — the windows the control plane assigned to it.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use growlerdb_core::{CommitBatch, IndexWriter, ResolvedIndex, TimeWindowing};
use growlerdb_index::{LocalIndexStore, ShardId, StoreError};
use growlerdb_proto::v1::{
    Error as WireError, GetCheckpointRequest, GetCheckpointResponse, ServedWindow, WriteRequest,
    WriteResponse,
};
use growlerdb_proto::{to_status, Write, WriteServer};
use growlerdb_source::IcebergConfig;
use tonic::{Code, Request, Response, Status};

use crate::gateway::Gateway;
use crate::shard_handle::ShardHandle;
use crate::windowed_routing::{
    SharedAdminWindows, SharedLookupWindows, SharedSearchWindows, SharedSuggestWindows,
};
use crate::{AdminService, LocalNode, LookupService, Node, SearchService, SuggestService};

/// Max decoded size of an inbound windowed `Write` (mirrors [`write_service`](crate::write_service)):
/// a catch-up commit spanning many windows can be large.
const MAX_WRITE_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

/// One window's authoritative serving state on this node: the swappable shard handle, the
/// in-process node fronting it (for the node's own REST gateway scatter), and its event-time
/// zone-map (widened as the window ingests). The search/suggest *services* live in the shared mux
/// maps ([`SharedSearchWindows`]/[`SharedSuggestWindows`]) the write path keeps in sync.
struct WindowState {
    handle: ShardHandle,
    node: Arc<dyn Node>,
    zone: Option<(i64, i64)>,
}

/// The shared, mutable set of windows a node serves (`window-id → state`). The windowed write path is
/// the sole writer; the gateway rebuild + control-plane registration are readers.
type WindowStates = Arc<RwLock<BTreeMap<i64, WindowState>>>;

/// The boot seed for one window: its swappable handle, in-process node, and event zone-map — what
/// [`serve_windowed`] already builds for each window present at startup.
pub type WindowSeed = (ShardHandle, Arc<dyn Node>, Option<(i64, i64)>);

/// One window's routing descriptor for [`Gateway::swap_windowed`](crate::Gateway): `(window id,
/// event zone-map)`.
type WindowDescriptor = (i64, Option<(i64, i64)>);

/// Called once per **newly-created** window so the CLI can attach its process-level
/// concerns — auto-compaction of the new hot shard, log lines — that the engine can't own.
pub type OnNewWindow = Arc<dyn Fn(i64, ShardHandle) + Send + Sync>;

/// A node-side windowed `Write` service. Serializes writes on the node (so window creation
/// can't race), commits each window's sub-batch to its own shard, and publishes a new window to the
/// query paths. Cheap to clone (all shared state is `Arc`).
#[derive(Clone)]
pub struct WindowedWriteService {
    store: LocalIndexStore,
    resolved: ResolvedIndex,
    windowing: TimeWindowing,
    index_name: String,
    table: String,
    iceberg: IcebergConfig,
    windows: WindowStates,
    search: SharedSearchWindows,
    suggest: SharedSuggestWindows,
    lookup: SharedLookupWindows,
    admin: SharedAdminWindows,
    gateway: Arc<Gateway>,
    on_new_window: OnNewWindow,
    /// Serializes windowed writes on this node so two concurrent commits can't both create the same
    /// window (or fight one shard's writer). The connector streams a node's windows from one worker,
    /// so serialization is the natural model and keeps creation single-threaded.
    write_lock: Arc<tokio::sync::Mutex<()>>,
}

impl WindowedWriteService {
    /// Build a windowed write service sharing the mux maps + in-process gateway that
    /// [`serve_windowed`] wires up. `windows`/`search`/`suggest` are seeded with the windows present
    /// at boot; `on_new_window` attaches compaction (+ logging) to each window created at runtime.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store: LocalIndexStore,
        resolved: ResolvedIndex,
        table: impl Into<String>,
        iceberg: IcebergConfig,
        windows_seed: BTreeMap<i64, WindowSeed>,
        search: SharedSearchWindows,
        suggest: SharedSuggestWindows,
        lookup: SharedLookupWindows,
        admin: SharedAdminWindows,
        gateway: Arc<Gateway>,
        on_new_window: OnNewWindow,
    ) -> Result<Self, StoreError> {
        let windowing = resolved
            .windowing
            .clone()
            .ok_or_else(|| StoreError::NotWindowed(resolved.name.clone()))?;
        let index_name = resolved.name.clone();
        let states = windows_seed
            .into_iter()
            .map(|(w, (handle, node, zone))| (w, WindowState { handle, node, zone }))
            .collect();
        Ok(Self {
            store,
            resolved,
            windowing,
            index_name,
            table: table.into(),
            iceberg,
            windows: Arc::new(RwLock::new(states)),
            search,
            suggest,
            lookup,
            admin,
            gateway,
            on_new_window,
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Wrap as a mountable tonic [`WriteServer`] with the large-commit decode cap.
    pub fn into_server(self) -> WriteServer<Self> {
        WriteServer::new(self).max_decoding_message_size(MAX_WRITE_MESSAGE_BYTES)
    }

    fn read_windows(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<i64, WindowState>> {
        self.windows.read().unwrap_or_else(|e| e.into_inner())
    }

    /// The [`TimeFormat`](growlerdb_core::TimeFormat) declared for a window/event field, to normalize
    /// its values to canonical micros before bucketing (mirrors `write_windowed`).
    fn format_of(&self, name: &str) -> Option<growlerdb_core::TimeFormat> {
        self.resolved
            .fields
            .iter()
            .find(|f| f.path == name)
            .and_then(|f| f.format)
    }

    /// The handle for `window`, creating + publishing the window shard (mux services + authoritative
    /// state) on first write. Returns `(handle, created)`. Only ever called under [`write_lock`], so
    /// there is no concurrent creator to race.
    fn ensure_window(&self, window: i64) -> Result<(ShardHandle, bool), StoreError> {
        if let Some(st) = self.read_windows().get(&window) {
            return Ok((st.handle.clone(), false));
        }
        let shard = Arc::new(
            self.store
                .create_shard(&ShardId::window(&self.index_name, window), &self.resolved)?,
        );
        let handle = ShardHandle::new(shard);
        let node = LocalNode::new(
            SearchService::new(handle.clone()),
            SuggestService::new(handle.clone()),
            LookupService::new(handle.clone(), self.iceberg.clone(), self.table.clone()),
            AdminService::new(handle.clone(), &self.index_name),
        )
        .shared();
        // Publish the query services first (a request can only reach the mux via the gateway, which we
        // swap after the commit — so an early mux entry is never routed to prematurely).
        self.search
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(window, SearchService::new(handle.clone()));
        self.suggest
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(window, SuggestService::new(handle.clone()));
        // Hydration: a new window must also be reachable by keys:get + describe, or a doc
        // just streamed into it can't be opened in the console and it's invisible to the index total.
        self.lookup
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                window,
                LookupService::new(handle.clone(), self.iceberg.clone(), self.table.clone()),
            );
        self.admin
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(window, AdminService::new(handle.clone(), &self.index_name));
        self.windows
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                window,
                WindowState {
                    handle: handle.clone(),
                    node,
                    zone: None,
                },
            );
        Ok((handle, true))
    }

    /// Commit one windowed batch (blocking): route upserts to per-window shards (creating windows as
    /// needed), widen each window's zone-map, and broadcast deletes to every window. Returns the max
    /// index snapshot and the ids of any windows created this call.
    fn commit_windowed(&self, batch: &CommitBatch) -> Result<(u64, Vec<i64>), StoreError> {
        let (window_batches, deletes) = self.windowing.partition_batch(
            batch,
            self.format_of(&self.windowing.field),
            self.windowing.event_time_field.as_deref(),
            self.windowing
                .event_time_field
                .as_deref()
                .and_then(|f| self.format_of(f)),
        );
        let mut max_snapshot = 0u64;
        let mut created = Vec::new();
        let mut ingested = 0u64;
        for wb in &window_batches {
            let (handle, is_new) = self.ensure_window(wb.window)?;
            if is_new {
                created.push(wb.window);
            }
            ingested += wb.batch.ops.len() as u64;
            let shard = handle.current();
            let snapshot = IndexWriter::write(&*shard, &wb.batch)?;
            max_snapshot = max_snapshot.max(snapshot.0);
            shard.set_event_bounds(wb.event_min, wb.event_max)?;
            // Record the widened zone-map for the gateway descriptor + registration.
            let zone = shard.event_bounds()?;
            if let Some(st) = self
                .windows
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .get_mut(&wb.window)
            {
                st.zone = zone;
            }
        }
        // Deletes carry only a key (no window value), so broadcast to every window — only the owner
        // holds the key. Rare for the append-mostly sources windowing targets.
        if !deletes.is_empty() {
            let del_batch = CommitBatch::new(
                deletes,
                batch.checkpoint.clone(),
                format!("{}#del", batch.batch_id),
            );
            let handles: Vec<ShardHandle> = self
                .read_windows()
                .values()
                .map(|s| s.handle.clone())
                .collect();
            for handle in handles {
                let snapshot = IndexWriter::write(&*handle.current(), &del_batch)?;
                max_snapshot = max_snapshot.max(snapshot.0);
            }
        }
        // Ingestion throughput SLI: committed doc-ops this commit, labelled by index
        // — the ordinal write path emits this too, so the console Ingestion throughput + the Grafana
        // "index docs/s" panel light up for a windowed index instead of reading a flat 0.
        growlerdb_telemetry::sli::ingested_docs(&self.index_name, ingested);
        Ok((max_snapshot, created))
    }

    /// After a commit created new windows: rebuild the in-process windowed gateway so the node's own
    /// REST surface serves them, and hand each new window's handle to `on_new_window` (compaction).
    fn publish_new_windows(&self, created: &[i64]) {
        for &w in created {
            if let Some(handle) = self.read_windows().get(&w).map(|s| s.handle.clone()) {
                (self.on_new_window)(w, handle);
            }
        }
        // One gateway swap covers all new windows: rebuild (nodes, descriptors) from the current set.
        let (nodes, descriptors): (Vec<Arc<dyn Node>>, Vec<WindowDescriptor>) = {
            let map = self.read_windows();
            map.iter()
                .map(|(w, st)| (st.node.clone(), (*w, st.zone)))
                .unzip()
        };
        self.gateway
            .swap_windowed(nodes, self.windowing.clone(), descriptors);
    }

    /// The windows this node currently serves, as [`ServedWindow`]s for control-plane registration —
    /// read fresh each announce so a window created since boot is advertised.
    pub fn served_windows(&self) -> Vec<ServedWindow> {
        self.read_windows()
            .iter()
            .map(|(w, st)| ServedWindow {
                window: *w,
                event_min: st.zone.map(|(lo, _)| lo).unwrap_or(0),
                event_max: st.zone.map(|(_, hi)| hi).unwrap_or(0),
                has_event_bounds: st.zone.is_some(),
            })
            .collect()
    }
}

#[tonic::async_trait]
impl Write for WindowedWriteService {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
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

        // Serialize windowed writes on this node so window creation is single-threaded, then run the
        // blocking commit off the async runtime.
        let _guard = self.write_lock.lock().await;
        let svc = self.clone();
        let started = std::time::Instant::now();
        let (snapshot, created) = tokio::task::spawn_blocking(move || svc.commit_windowed(&batch))
            .await
            .map_err(|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())))?
            .map_err(store_error_to_status)?;
        // Write-latency SLI: the wall-clock per-commit time across this batch's windows —
        // the latency counterpart to the `ingested_docs` throughput counted inside `commit_windowed`.
        growlerdb_telemetry::sli::write(&self.index_name, started.elapsed().as_secs_f64());

        if !created.is_empty() {
            self.publish_new_windows(&created);
        }
        Ok(Response::new(WriteResponse { snapshot }))
    }

    async fn get_checkpoint(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<GetCheckpointResponse>, Status> {
        // Per-window resume: the connector reads each window's checkpoint independently.
        let window = request.into_inner().window;
        let Some(handle) = self.read_windows().get(&window).map(|s| s.handle.clone()) else {
            // A window this node hasn't created yet → no checkpoint (the connector starts it fresh).
            return Ok(Response::new(GetCheckpointResponse {
                checkpoint: None,
                snapshot: 0,
            }));
        };
        let (checkpoint, snapshot) =
            tokio::task::spawn_blocking(move || -> Result<_, StoreError> {
                let shard = handle.current();
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

/// Map a commit [`StoreError`] to a gRPC status — a checkpoint gap is a non-retryable
/// `FAILED_PRECONDITION` (the connector must reconcile), everything else internal. Mirrors
/// [`write_service`](crate::write_service).
fn store_error_to_status(e: StoreError) -> Status {
    match &e {
        StoreError::CheckpointGap { .. } => to_status(
            Code::FailedPrecondition,
            WireError::new("CHECKPOINT_GAP", e.to_string()),
        ),
        _ => to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WindowedSearchService;
    use growlerdb_core::{
        CompositeKey, DocOp, Document, IndexDefinition, LocatedDoc, SourceCheckpoint, SourceField,
        SourceSchema, SourceType, Value,
    };
    use growlerdb_proto::v1::{SearchRequest, WriteRequest};
    use growlerdb_proto::Search;

    const DAY: i64 = 86_400_000_000; // one day in canonical micros

    fn windowed_index() -> ResolvedIndex {
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
            "name: logs\nsource: { iceberg: { catalog: g, table: g.logs } }\nwindowing: { field: ingest, granularity: daily, event_time_field: event }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD, fast: true }, { path: ingest, format: epoch_us, fast: true }, { path: event, format: epoch_us, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn upsert(id: &str, ingest: i64) -> DocOp {
        let mut f = std::collections::BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("ingest".to_string(), Value::Int(ingest));
        f.insert("event".to_string(), Value::Int(ingest));
        DocOp::Upsert(LocatedDoc {
            doc: Document::new(
                CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]),
                f,
            ),
            iceberg_file: "f".into(),
            row_position: 0,
        })
    }

    fn service() -> (WindowedWriteService, SharedSearchWindows, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let resolved = windowed_index();
        let windowing = resolved.windowing.clone().unwrap();
        let search: SharedSearchWindows = Arc::new(RwLock::new(BTreeMap::new()));
        let suggest: SharedSuggestWindows = Arc::new(RwLock::new(BTreeMap::new()));
        let lookup: SharedLookupWindows = Arc::new(RwLock::new(BTreeMap::new()));
        let admin: SharedAdminWindows = Arc::new(RwLock::new(BTreeMap::new()));
        // An empty windowed gateway; the write path swaps in real windows on first write. The test
        // queries the mux directly, so this gateway is only exercised by the swap.
        let gw = Arc::new(Gateway::windowed(vec![], windowing, vec![]));
        let svc = WindowedWriteService::new(
            store,
            resolved,
            "g.logs",
            IcebergConfig::local(),
            BTreeMap::new(),
            search.clone(),
            suggest.clone(),
            lookup,
            admin,
            gw,
            Arc::new(|_w, _h| {}),
        )
        .unwrap();
        (svc, search, tmp)
    }

    async fn ids_in_window(search: &SharedSearchWindows, window: i64) -> Vec<String> {
        let mux = WindowedSearchService::new(search.clone());
        let resp = Search::search(
            &mux,
            Request::new(SearchRequest {
                query: "*".into(),
                limit: 10,
                window,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let mut ids: Vec<String> = resp
            .hits
            .iter()
            .map(|h| {
                let key: CompositeKey = h.coordinates.clone().unwrap().try_into().unwrap();
                key.identifier[0].1.to_index_string()
            })
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn streaming_write_creates_windows_and_makes_them_queryable() {
        let (svc, search, _tmp) = service();
        let day = |n: i64| n * DAY;

        // A batch spanning two days → two windows created on first write.
        let batch = growlerdb_core::CommitBatch::new(
            vec![upsert("a", day(10) + 5), upsert("b", day(11) + 7)],
            SourceCheckpoint::iceberg(1),
            "b1",
        );
        svc.write(Request::new(WriteRequest {
            batch: Some(batch.into()),
        }))
        .await
        .unwrap();

        // Each window's shard is live-queryable through the (shared) mux with the right doc.
        assert_eq!(ids_in_window(&search, day(10)).await, vec!["a"]);
        assert_eq!(ids_in_window(&search, day(11)).await, vec!["b"]);

        // The node advertises both windows (with event zone-maps) for control-plane registration.
        let served: std::collections::BTreeMap<i64, bool> = svc
            .served_windows()
            .into_iter()
            .map(|w| (w.window, w.has_event_bounds))
            .collect();
        assert_eq!(served.len(), 2);
        assert!(served[&day(10)], "window 10 reports an event zone-map");

        // A second batch extends window 11 and opens window 12 — no restart, still queryable.
        let batch2 = growlerdb_core::CommitBatch::new(
            vec![upsert("c", day(11) + 9), upsert("d", day(12) + 1)],
            SourceCheckpoint::iceberg(2),
            "b2",
        );
        svc.write(Request::new(WriteRequest {
            batch: Some(batch2.into()),
        }))
        .await
        .unwrap();
        assert_eq!(ids_in_window(&search, day(11)).await, vec!["b", "c"]);
        assert_eq!(ids_in_window(&search, day(12)).await, vec!["d"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn per_window_checkpoint_resumes_each_window_independently() {
        use growlerdb_proto::v1::source_checkpoint::Kind;
        let (svc, _search, _tmp) = service();
        let day = |n: i64| n * DAY;

        svc.write(Request::new(WriteRequest {
            batch: Some(
                growlerdb_core::CommitBatch::new(
                    vec![upsert("a", day(10))],
                    SourceCheckpoint::iceberg(42),
                    "b1",
                )
                .into(),
            ),
        }))
        .await
        .unwrap();

        // The window we wrote resumes from its committed checkpoint (Iceberg snapshot 42).
        let cp = svc
            .get_checkpoint(Request::new(GetCheckpointRequest { window: day(10) }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(cp.snapshot, 1);
        assert!(matches!(
            cp.checkpoint.and_then(|c| c.kind),
            Some(Kind::IcebergSnapshot(42))
        ));

        // A window this node never created has no checkpoint → the connector starts it from scratch.
        let none = svc
            .get_checkpoint(Request::new(GetCheckpointRequest { window: day(99) }))
            .await
            .unwrap()
            .into_inner();
        assert!(none.checkpoint.is_none());
        assert_eq!(none.snapshot, 0);
    }
}
