//! **Dynamic windowed streaming ingest**: a node-side `Write` gRPC service that routes each incoming
//! doc to its **time-window shard** — creating the window shard on first write and publishing it so
//! it becomes **live-queryable** (mux + in-process gateway) with no restart. The streaming
//! counterpart to the batch [`write_windowed`](growlerdb_index::LocalIndexStore); here windows form
//! as the timeline advances. Placement is the control plane's, so a node only receives — and creates
//! — the windows assigned to it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, RwLock};

use growlerdb_core::{CommitBatch, IndexWriter, ResolvedIndex, TimeWindowing};
use growlerdb_index::{LocalIndexStore, ShardId, StoreError};
use growlerdb_proto::v1::{
    unit_assignment, Error as WireError, GetCheckpointRequest, GetCheckpointResponse, ServedWindow,
    UnitAssignment, WriteRequest, WriteResponse,
};
use growlerdb_proto::{to_status, Write, WriteServer};
use growlerdb_source::IcebergConfig;
use tonic::{Code, Request, Response, Status};

use crate::fence::ReindexFence;
use crate::gateway::Gateway;
use crate::shard_handle::ShardHandle;
use crate::windowed_routing::{
    not_primary, SharedAdminWindows, SharedLookupWindows, SharedSearchWindows, SharedSuggestWindows,
};
use crate::{AdminService, LocalNode, LookupService, Node, SearchService, SuggestService};

/// Max decoded size of an inbound windowed `Write` (mirrors [`write_service`](crate::write_service)):
/// a catch-up commit spanning many windows can be large. `pub(crate)` so the pool write dispatcher
/// mounts its `WriteServer` with the same cap.
pub(crate) const MAX_WRITE_MESSAGE_BYTES: usize = 256 * 1024 * 1024;

/// The node-side **write fence** (D53's one-writer-per-unit): an atomically-swapped view of the
/// `(index, window)` units this node holds **as primary**, per the control plane's assignment pushes.
/// The windowed write path consults it before committing or answering a checkpoint — a `Write`/
/// `GetCheckpoint` addressed to a unit outside the primary set is refused with the structured
/// [`NOT_PRIMARY`](crate::windowed_routing::NOT_PRIMARY) `FAILED_PRECONDITION`, so a misrouted or
/// split-brain connector can never commit to (or fabricate a resume point on) a non-primary holder.
/// When a unit's primary moves away, the next snapshot makes this node start refusing — closing its
/// half of the split-brain window without waiting for the unit to unload.
///
/// **Fencing applies only when the node runs from CP assignments** (`serve-pool --register`):
///
/// * [`PrimaryFence::unrestricted`] (the `Default`) never refuses — classic `serve` / standalone
///   `serve-pool` have no assignment stream, so create-on-first-write is unchanged.
/// * [`PrimaryFence::fenced`] enforces from the **first applied snapshot** on; before it arrives the
///   fence stays permissive (the CP's seed snapshot is in flight — refusing here would fail connector
///   streams for a startup race, and `ensure_window`'s read-mux check still protects replica windows).
///
/// Cheap to clone; one instance is shared by every per-index writer on the node.
#[derive(Clone, Default)]
pub struct PrimaryFence {
    /// `None` = unrestricted (no assignment stream). Inner `None` = fenced but no snapshot applied
    /// yet (permissive until the CP's seed snapshot lands).
    view: Option<Arc<RwLock<Option<PrimaryUnits>>>>,
}

/// The primary-held window set, per index: `index → windows this node is the primary writer for`.
type PrimaryUnits = BTreeMap<String, BTreeSet<i64>>;

impl PrimaryFence {
    /// A fence that never refuses — for serve modes with no CP assignment stream.
    pub fn unrestricted() -> Self {
        Self { view: None }
    }

    /// A fence that enforces the primary-holder set from the first [`apply_snapshot`] on.
    ///
    /// [`apply_snapshot`]: PrimaryFence::apply_snapshot
    pub fn fenced() -> Self {
        Self {
            view: Some(Arc::new(RwLock::new(None))),
        }
    }

    /// Atomically replace the primary-held view from a full CP assignment snapshot. Only **window**
    /// units matter here (the windowed write path is what's fenced); replica-role units are excluded.
    /// A no-op on an unrestricted fence.
    pub fn apply_snapshot(&self, units: &[UnitAssignment]) {
        let Some(view) = &self.view else { return };
        let mut primaries = PrimaryUnits::new();
        for u in units {
            if !u.primary {
                continue;
            }
            if let Some(unit_assignment::Unit::Window(w)) = u.unit {
                primaries.entry(u.index.clone()).or_default().insert(w);
            }
        }
        *view.write().unwrap_or_else(|e| e.into_inner()) = Some(primaries);
    }

    /// Whether this node may accept a write / answer a checkpoint for `(index, window)`: `true` when
    /// unrestricted, when no snapshot has been applied yet, or when the unit is in the primary set.
    pub fn allows(&self, index: &str, window: i64) -> bool {
        let Some(view) = &self.view else { return true };
        match view.read().unwrap_or_else(|e| e.into_inner()).as_ref() {
            None => true, // fenced, but the CP's seed snapshot hasn't landed yet
            Some(primaries) => primaries.get(index).is_some_and(|ws| ws.contains(&window)),
        }
    }
}

/// One window's authoritative serving state on this node: the swappable shard handle, the in-process
/// node fronting it (for the node's own REST gateway scatter), and its event-time zone-map. The
/// search/suggest *services* live in the shared mux maps the write path keeps in sync.
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
/// event zone-map, cold)` — `cold` = served read-through from object storage (parked).
type WindowDescriptor = (i64, Option<(i64, i64)>, bool);

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
    /// The node's primary-holder write fence ([`PrimaryFence`]); unrestricted by default, so
    /// non-pool serve modes are unchanged. Set via [`with_primary_fence`](Self::with_primary_fence).
    fence: PrimaryFence,
    /// Serializes windowed writes on this node so two concurrent commits can't both create the same
    /// window (or fight one shard's writer). The connector streams a node's windows from one worker,
    /// so serialization is the natural model.
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
            fence: PrimaryFence::unrestricted(),
            write_lock: Arc::new(tokio::sync::Mutex::new(())),
        })
    }

    /// Attach the node's primary-holder write [`PrimaryFence`] (serve-pool with CP assignments).
    /// Without this, the service is unrestricted — classic `serve` behavior.
    pub fn with_primary_fence(mut self, fence: PrimaryFence) -> Self {
        self.fence = fence;
        self
    }

    /// Wrap as a mountable tonic [`WriteServer`] with the large-commit decode cap.
    pub fn into_server(self) -> WriteServer<Self> {
        WriteServer::new(self).max_decoding_message_size(MAX_WRITE_MESSAGE_BYTES)
    }

    /// A snapshot of the windows this node serves and their swappable handles — the **live** set (boot
    /// windows plus any created at runtime). The background cold-tier park loop reads this each tick;
    /// swapping a returned handle to a cold read-through shard is immediately visible to every query
    /// path sharing it. Ascending by window id, matching [`window_shards`](LocalIndexStore::window_shards).
    pub fn window_handles(&self) -> Vec<(i64, ShardHandle)> {
        self.read_windows()
            .iter()
            .map(|(w, st)| (*w, st.handle.clone()))
            .collect()
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
    ///
    /// **Replica-held guard (HA-A2, defense in depth under the [`PrimaryFence`]):** a window present
    /// in the shared read mux but absent from this writer's states was published **read-through**
    /// (cold) by the assignment reconcile — a parked snapshot this node serves as a replica. Creating
    /// a fresh shard here would overwrite the served entries with an empty one; refuse instead (the
    /// same non-retryable `WINDOW_PARKED` a late write into a locally-parked window gets).
    fn ensure_window(&self, window: i64) -> Result<(ShardHandle, bool), Status> {
        if let Some(st) = self.read_windows().get(&window) {
            return Ok((st.handle.clone(), false));
        }
        if self
            .search
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&window)
        {
            return Err(to_status(
                Code::FailedPrecondition,
                WireError::new(
                    "WINDOW_PARKED",
                    format!(
                        "window {window} of `{}` is served read-through (cold) on this node — \
                         refusing to overwrite the parked snapshot with a fresh shard (route the \
                         write to the window's primary, or revive the window)",
                        self.index_name
                    ),
                ),
            ));
        }
        let shard = Arc::new(
            self.store
                .create_shard(&ShardId::window(&self.index_name, window), &self.resolved)
                .map_err(store_error_to_status)?,
        );
        let handle = ShardHandle::new(shard);
        let node = LocalNode::new(
            SearchService::new(handle.clone()),
            SuggestService::new(handle.clone()),
            LookupService::new(
                handle.clone(),
                self.iceberg.clone(),
                self.table.clone(),
                self.resolved.clone(),
            ),
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
        // A new window must also be reachable by keys:get + describe, or a doc just streamed into it
        // can't be opened in the console and is invisible to the index total.
        self.lookup
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                window,
                LookupService::new(
                    handle.clone(),
                    self.iceberg.clone(),
                    self.table.clone(),
                    self.resolved.clone(),
                ),
            );
        // Source-capable admin for this (hot) window, so a coordinated per-window reindex can rebuild
        // + swap it. Fresh per-window ReindexFence (windowed cutover relies on connector replay, not a
        // write-fence — see serve_windowed's `build`). ShardId::window ties the reindex to this window.
        self.admin
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                window,
                AdminService::new(handle.clone(), &self.index_name).with_source(
                    self.resolved.clone(),
                    self.store.clone(),
                    ShardId::window(&self.index_name, window),
                    self.iceberg.clone(),
                    self.table.clone(),
                    ReindexFence::new(),
                ),
            );
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
    ///
    /// **Write fencing (357.12):** every target window is validated against the [`PrimaryFence`]
    /// *before anything commits* — a batch touching even one window this node doesn't hold as primary
    /// is refused whole (structured `NOT_PRIMARY`), with no shard created and no partial commit, so
    /// the refusal is safely replayable against the right holder.
    fn commit_windowed(&self, batch: &CommitBatch) -> Result<(u64, Vec<i64>), Status> {
        let (window_batches, deletes) = self.windowing.partition_batch(
            batch,
            self.format_of(&self.windowing.field),
            self.windowing.event_time_field.as_deref(),
            self.windowing
                .event_time_field
                .as_deref()
                .and_then(|f| self.format_of(f)),
        );
        for wb in &window_batches {
            if !self.fence.allows(&self.index_name, wb.window) {
                return Err(not_primary(format!(
                    "this node does not hold window {} of `{}` as primary — the write belongs on \
                     the unit's primary holder (re-resolve placement)",
                    wb.window, self.index_name
                )));
            }
        }
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
            let snapshot = IndexWriter::write(&*shard, &wb.batch).map_err(store_error_to_status)?;
            max_snapshot = max_snapshot.max(snapshot.0);
            shard
                .set_event_bounds(wb.event_min, wb.event_max)
                .map_err(store_error_to_status)?;
            // Record the widened zone-map for the gateway descriptor + registration.
            let zone = shard.event_bounds().map_err(store_error_to_status)?;
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
        // holds the key.
        if !deletes.is_empty() {
            // Carry the connector's resume floor so each window prunes its idempotency records
            // (`from` stays absent by design — see `TimeWindowing::partition_batch`).
            let del_batch = CommitBatch::new(
                deletes,
                batch.checkpoint.clone(),
                format!("{}#del", batch.batch_id),
            )
            .with_safe_checkpoint(batch.safe_checkpoint.clone());
            let handles: Vec<(i64, ShardHandle)> = self
                .read_windows()
                .iter()
                .map(|(w, s)| (*w, s.handle.clone()))
                .collect();
            let mut skipped_parked = Vec::new();
            for (window, handle) in handles {
                // A window this node no longer holds as primary must not take the broadcast either —
                // its primary applies the same delete, so nothing is lost.
                if !self.fence.allows(&self.index_name, window) {
                    continue;
                }
                let shard = handle.current();
                // A parked (cold read-through) window is immutable: failing the whole batch would
                // wedge ingest behind an un-applicable delete. Skip it — a key in a parked window
                // outlives its delete until the window is dropped/revived (surfaced via the warning below).
                if shard.is_read_only() {
                    skipped_parked.push(window);
                    continue;
                }
                let snapshot =
                    IndexWriter::write(&*shard, &del_batch).map_err(store_error_to_status)?;
                max_snapshot = max_snapshot.max(snapshot.0);
            }
            if !skipped_parked.is_empty() {
                tracing::warn!(
                    index = %self.index_name,
                    windows = ?skipped_parked,
                    "delete broadcast skipped parked (read-only cold) windows — a deleted key \
                     residing in one stays visible there until the window is revived or dropped"
                );
            }
        }
        // Ingestion throughput SLI: committed doc-ops this commit, labelled by index (the ordinal
        // write path emits this too, so a windowed index lights up the same console/Grafana panels).
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
                .map(|(w, st)| {
                    (
                        st.node.clone(),
                        (*w, st.zone, st.handle.current().is_read_only()),
                    )
                })
                .unzip()
        };
        self.gateway
            .swap_windowed(nodes, self.windowing.clone(), descriptors);
    }

    /// **Unload** `window` from this node's serving set (D53 de-assignment, HA-G1): remove its
    /// entries from the shared read mux maps (the same Arcs the pool muxes front, so the window stops
    /// being routable immediately) and from this writer's window states, then rebuild the in-process
    /// gateway without it. Safe for in-flight requests: the muxes **clone the service (an `Arc`'d
    /// shard handle) out from under the read lock** before dispatching, so the last clone keeps the
    /// shard alive until those requests finish. A replica-held (read-through) window has mux entries
    /// but no writer state; both shapes are handled. Returns `true` iff the node was actually serving
    /// the window.
    pub fn unload_window(&self, window: i64) -> bool {
        let mut any = false;
        any |= self
            .search
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&window)
            .is_some();
        any |= self
            .suggest
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&window)
            .is_some();
        any |= self
            .lookup
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&window)
            .is_some();
        any |= self
            .admin
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&window)
            .is_some();
        let had_state = self
            .windows
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&window)
            .is_some();
        if had_state {
            // The node's own in-process gateway (REST scatter) must drop the window too.
            self.publish_new_windows(&[]);
        }
        any || had_state
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
                // Live tier: read-through (no writer) ⇒ cold. Reported each heartbeat, so a
                // park/pre-warm swap surfaces in the cluster gateway's /v1/cold.
                cold: st.handle.current().is_read_only(),
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

        // Serialize windowed writes so window creation is single-threaded, then commit off the runtime.
        let _guard = self.write_lock.lock().await;
        let svc = self.clone();
        let started = std::time::Instant::now();
        let (snapshot, created) = tokio::task::spawn_blocking(move || svc.commit_windowed(&batch))
            .await
            .map_err(|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())))??;
        // Write-latency SLI: wall-clock per-commit time across this batch's windows (the latency
        // counterpart to `ingested_docs`).
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
        // Write fence (HA-A3): a checkpoint read on a non-primary holder must be the structured
        // NOT_PRIMARY refusal, never a fabricated "no checkpoint, snapshot 0" — that would instruct a
        // misrouted connector to re-ingest the window from scratch here.
        if !self.fence.allows(&self.index_name, window) {
            return Err(not_primary(format!(
                "this node does not hold window {} of `{}` as primary — resolve the unit's \
                 primary holder for its resume checkpoint",
                window, self.index_name
            )));
        }
        let Some(handle) = self.read_windows().get(&window).map(|s| s.handle.clone()) else {
            // Defense in depth (as in `ensure_window`): a window in the read mux but absent from the
            // write states is a parked snapshot published read-through — refuse rather than fabricate
            // an empty checkpoint over served data.
            if self
                .search
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(&window)
            {
                return Err(to_status(
                    Code::FailedPrecondition,
                    WireError::new(
                        "WINDOW_PARKED",
                        format!(
                            "window {window} of `{}` is served read-through (cold) on this node — \
                             its resume checkpoint lives on the window's primary",
                            self.index_name
                        ),
                    ),
                ));
            }
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

/// Map a commit [`StoreError`] to a gRPC status — a checkpoint gap and a write into a parked window
/// are non-retryable `FAILED_PRECONDITION`s (retrying can't succeed; the operator must reconcile /
/// revive), everything else internal. Mirrors [`write_service`](crate::write_service).
fn store_error_to_status(e: StoreError) -> Status {
    match &e {
        StoreError::CheckpointGap { .. } => to_status(
            Code::FailedPrecondition,
            WireError::new("CHECKPOINT_GAP", e.to_string()),
        ),
        // Late data routed to a window already parked to cold: loud + actionable rather than an
        // infinite-retry INTERNAL trap — revive (pre-warm) the window or widen `hot_windows`.
        StoreError::ReadOnlyShard(_) => to_status(
            Code::FailedPrecondition,
            WireError::new(
                "WINDOW_PARKED",
                format!("{e} (late data arrived for a cold-parked window — revive it or widen `hot_windows`)"),
            ),
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

    fn service() -> (
        WindowedWriteService,
        SharedSearchWindows,
        LocalIndexStore,
        tempfile::TempDir,
    ) {
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
            store.clone(),
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
        (svc, search, store, tmp)
    }

    /// Park `window` under the service's live handle, exactly like the CLI cold-park loop: copy
    /// the window's tantivy bulk into a local-fs "object store", evict the local copy, open a
    /// read-through cold shard **sharing the live `aux.redb` handle**, and hot-swap it in.
    /// Returns the object-store tempdir (kept alive by the caller).
    async fn park_window(
        svc: &WindowedWriteService,
        store: &LocalIndexStore,
        window: i64,
    ) -> tempfile::TempDir {
        let resolved = windowed_index();
        let window_dir = store.shard_path(&ShardId::window(&resolved.name, window));
        let index_dir = window_dir.join("index");
        let store_root = tempfile::tempdir().unwrap();
        let cold = store_root.path().join("cold");
        std::fs::create_dir_all(&cold).unwrap();
        for entry in std::fs::read_dir(&index_dir).unwrap() {
            let e = entry.unwrap();
            if e.file_type().unwrap().is_file() {
                std::fs::copy(e.path(), cold.join(e.file_name())).unwrap();
            }
        }
        // Evict the local copy. Unlike the CLI park loop (which only parks windows past their
        // write horizon), the test parks a just-written window, so a tantivy merge thread can
        // still drop a segment file into the dir mid-removal — retry briefly until it quiesces.
        for attempt in 0.. {
            match std::fs::remove_dir_all(&index_dir) {
                Ok(()) => break,
                Err(_) if attempt < 50 => {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                Err(e) => panic!("evicting {index_dir:?}: {e}"),
            }
        }
        let op = opendal::Operator::new(
            opendal::services::Fs::default().root(&store_root.path().to_string_lossy()),
        )
        .unwrap();
        let handle = svc
            .window_handles()
            .into_iter()
            .find(|(w, _)| *w == window)
            .expect("window exists")
            .1;
        let reuse_db = handle.current().db_handle();
        let cache = growlerdb_index::RangeCache::new(8 * 1024 * 1024);
        let (store2, aux) = (store.clone(), window_dir.clone());
        let shard = tokio::task::spawn_blocking(move || {
            store2.open_cold_shard(
                &resolved,
                &aux,
                op,
                "cold",
                cache,
                None,
                None,
                Some(reuse_db),
            )
        })
        .await
        .unwrap()
        .unwrap();
        assert!(shard.is_read_only());
        handle.swap(Arc::new(shard));
        store_root
    }

    fn delete(id: &str) -> DocOp {
        DocOp::Delete(CompositeKey::new(
            vec![],
            vec![("id".into(), Value::from(id))],
        ))
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
        let (svc, search, _store, _tmp) = service();
        let day = |n: i64| n * DAY;

        // A batch spanning two days → two windows created on first write.
        let batch = growlerdb_core::CommitBatch::new(
            vec![upsert("a", day(10) + 5), upsert("b", day(11) + 7)],
            SourceCheckpoint::iceberg(1),
            "b1",
        );
        svc.write(Request::new(WriteRequest {
            batch: Some(batch.into()),
            index: String::new(),
            shard: 0,
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
            index: String::new(),
            shard: 0,
        }))
        .await
        .unwrap();
        assert_eq!(ids_in_window(&search, day(11)).await, vec!["b", "c"]);
        assert_eq!(ids_in_window(&search, day(12)).await, vec!["d"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn per_window_checkpoint_resumes_each_window_independently() {
        use growlerdb_proto::v1::source_checkpoint::Kind;
        let (svc, _search, _store, _tmp) = service();
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
            index: String::new(),
            shard: 0,
        }))
        .await
        .unwrap();

        // The window we wrote resumes from its committed checkpoint (Iceberg snapshot 42).
        let cp = svc
            .get_checkpoint(Request::new(GetCheckpointRequest {
                window: day(10),
                index: String::new(),
                shard: 0,
            }))
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
            .get_checkpoint(Request::new(GetCheckpointRequest {
                window: day(99),
                index: String::new(),
                shard: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(none.checkpoint.is_none());
        assert_eq!(none.snapshot, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn delete_broadcast_skips_parked_windows_instead_of_wedging_ingest() {
        let (svc, search, store, _tmp) = service();
        let day = |n: i64| n * DAY;

        // Two hot windows, then window 10 ages out and parks to the cold tier.
        svc.write(Request::new(WriteRequest {
            batch: Some(
                growlerdb_core::CommitBatch::new(
                    vec![upsert("a", day(10) + 5), upsert("b", day(11) + 7)],
                    SourceCheckpoint::iceberg(1),
                    "b1",
                )
                .into(),
            ),
            index: String::new(),
            shard: 0,
        }))
        .await
        .unwrap();
        let _cold_store = park_window(&svc, &store, day(10)).await;

        // A batch with an upsert + deletes commits cleanly: the broadcast applies to the hot window
        // and skips the parked one.
        svc.write(Request::new(WriteRequest {
            batch: Some(
                growlerdb_core::CommitBatch::new(
                    vec![upsert("c", day(11) + 9), delete("a"), delete("b")],
                    SourceCheckpoint::iceberg(2),
                    "b2",
                )
                .into(),
            ),
            index: String::new(),
            shard: 0,
        }))
        .await
        .expect("a delete broadcast must not fail on a parked window");

        // Hot window 11: the delete applied ("b" gone, "c" landed).
        assert_eq!(ids_in_window(&search, day(11)).await, vec!["c"]);
        // Parked window 10 is immutable: "a" outlives its delete until revive/drop — the
        // documented trade-off (and still served read-through).
        assert_eq!(ids_in_window(&search, day(10)).await, vec!["a"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn late_upsert_into_a_parked_window_is_a_clean_window_parked_error() {
        let (svc, _search, store, _tmp) = service();
        let day = |n: i64| n * DAY;

        svc.write(Request::new(WriteRequest {
            batch: Some(
                growlerdb_core::CommitBatch::new(
                    vec![upsert("a", day(10) + 5)],
                    SourceCheckpoint::iceberg(1),
                    "b1",
                )
                .into(),
            ),
            index: String::new(),
            shard: 0,
        }))
        .await
        .unwrap();
        let _cold_store = park_window(&svc, &store, day(10)).await;

        // Late data routed to the parked window: a loud, non-retryable FAILED_PRECONDITION.
        let status = svc
            .write(Request::new(WriteRequest {
                batch: Some(
                    growlerdb_core::CommitBatch::new(
                        vec![upsert("late", day(10) + 9)],
                        SourceCheckpoint::iceberg(2),
                        "b2",
                    )
                    .into(),
                ),
                index: String::new(),
                shard: 0,
            }))
            .await
            .expect_err("a write into a parked window must be refused");
        assert_eq!(status.code(), Code::FailedPrecondition);
        let detail = growlerdb_proto::error_details(&status).expect("structured error");
        assert_eq!(detail.code, "WINDOW_PARKED");
    }

    /// A CP assignment snapshot marking this node primary for `primaries` and replica for
    /// `replicas` (all on index `logs`).
    fn snapshot_units(primaries: &[i64], replicas: &[i64]) -> Vec<UnitAssignment> {
        primaries
            .iter()
            .map(|&w| (w, true))
            .chain(replicas.iter().map(|&w| (w, false)))
            .map(|(w, primary)| UnitAssignment {
                index: "logs".into(),
                unit: Some(unit_assignment::Unit::Window(w)),
                primary,
            })
            .collect()
    }

    async fn write_batch(
        svc: &WindowedWriteService,
        ops: Vec<DocOp>,
        seq: i64,
        batch_id: &str,
    ) -> Result<u64, Status> {
        let batch = growlerdb_core::CommitBatch::new(ops, SourceCheckpoint::iceberg(seq), batch_id);
        svc.write(Request::new(WriteRequest {
            batch: Some(batch.into()),
            index: String::new(),
            shard: 0,
        }))
        .await
        .map(|r| r.into_inner().snapshot)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fenced_write_outside_the_primary_set_is_refused_without_creating_a_shard() {
        // 357.12 / HA-A1 (node side): with a CP assignment snapshot applied, a write addressed to a
        // window this node does not hold as PRIMARY is the structured NOT_PRIMARY refusal — and it
        // must not create a shard, touch the read mux, or partially commit.
        let (svc, search, store, _tmp) = service();
        let day = |n: i64| n * DAY;
        let fence = PrimaryFence::fenced();
        // Primary for day 10; day 11 held only as a REPLICA (in the snapshot, but not primary).
        fence.apply_snapshot(&snapshot_units(&[day(10)], &[day(11)]));
        let svc = svc.with_primary_fence(fence);

        // A write into the replica-held window is refused with the structured detail...
        let status = write_batch(&svc, vec![upsert("r", day(11) + 1)], 1, "b1")
            .await
            .expect_err("a write to a non-primary window must be refused");
        assert!(
            crate::windowed_routing::is_not_primary(&status),
            "structured NOT_PRIMARY detail: {status:?}"
        );
        // ...with no shard created and no read-mux entry published.
        assert!(svc.served_windows().is_empty(), "no window state created");
        assert!(store.window_shards("logs").unwrap().is_empty());
        assert!(search.read().unwrap().is_empty(), "read mux untouched");

        // A batch spanning an allowed AND a refused window is refused WHOLE — pre-validation, so
        // nothing (not even the allowed window) commits and the batch replays cleanly elsewhere.
        let status = write_batch(
            &svc,
            vec![upsert("a", day(10) + 1), upsert("r", day(11) + 2)],
            1,
            "b2",
        )
        .await
        .expect_err("a mixed batch must be refused whole");
        assert!(crate::windowed_routing::is_not_primary(&status));
        assert!(svc.served_windows().is_empty(), "no partial commit");

        // A write into the primary-held window proceeds and creates it as before.
        assert!(
            write_batch(&svc, vec![upsert("a", day(10) + 1)], 1, "b3")
                .await
                .unwrap()
                > 0
        );
        assert_eq!(ids_in_window(&search, day(10)).await, vec!["a"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fenced_get_checkpoint_on_a_non_primary_window_is_the_structured_refusal() {
        // HA-A3: a checkpoint read on a non-primary holder must never answer "no checkpoint,
        // snapshot 0" — that instructs a misrouted connector to re-ingest the window from scratch
        // on the wrong node. With the fence it's the NOT_PRIMARY refusal.
        let (svc, _search, _store, _tmp) = service();
        let day = |n: i64| n * DAY;
        let fence = PrimaryFence::fenced();
        fence.apply_snapshot(&snapshot_units(&[day(10)], &[day(11)]));
        let svc = svc.with_primary_fence(fence);

        let status = svc
            .get_checkpoint(Request::new(GetCheckpointRequest {
                window: day(11),
                index: String::new(),
                shard: 0,
            }))
            .await
            .expect_err("a checkpoint read on a non-primary window must be refused");
        assert!(
            crate::windowed_routing::is_not_primary(&status),
            "structured NOT_PRIMARY detail: {status:?}"
        );

        // The primary-held window still answers normally (fresh here → honest empty checkpoint).
        let cp = svc
            .get_checkpoint(Request::new(GetCheckpointRequest {
                window: day(10),
                index: String::new(),
                shard: 0,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(cp.checkpoint.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn write_to_a_replica_published_window_refuses_and_keeps_the_served_snapshot() {
        // HA-A2 (defense in depth, independent of the fence): a window present in the READ mux but
        // absent from the write states is a replica-held parked snapshot published read-through by
        // the assignment reconcile. A write addressed to it must refuse rather than create a fresh
        // empty shard that overwrites the served entries — even with a permissive fence.
        let (svc, search, store, _tmp) = service();
        let day = |n: i64| n * DAY;
        let svc = svc.with_primary_fence(PrimaryFence::fenced()); // fenced, no snapshot yet

        // Simulate the reconcile's publication: a shard holding the "parked" doc, inserted into the
        // shared search mux only (exactly what reconcile_replica_windows does — read paths, not the
        // writer's states).
        let replica_tmp = tempfile::tempdir().unwrap();
        let replica_store = LocalIndexStore::open(replica_tmp.path()).unwrap();
        let shard = replica_store
            .create_shard(&ShardId::window("logs", day(10)), &windowed_index())
            .unwrap();
        IndexWriter::write(
            &shard,
            &growlerdb_core::CommitBatch::new(
                vec![upsert("parked", day(10) + 1)],
                SourceCheckpoint::iceberg(7),
                "p1",
            ),
        )
        .unwrap();
        search
            .write()
            .unwrap()
            .insert(day(10), crate::SearchService::new(Arc::new(shard)));

        // The write is refused (non-retryable, WINDOW_PARKED-style)...
        let status = write_batch(&svc, vec![upsert("late", day(10) + 5)], 1, "b1")
            .await
            .expect_err("a write to a replica-published window must be refused");
        assert_eq!(status.code(), Code::FailedPrecondition);
        let detail = growlerdb_proto::error_details(&status).expect("structured error");
        assert_eq!(detail.code, "WINDOW_PARKED");
        // ...the served snapshot still answers through the mux (nothing overwritten)...
        assert_eq!(ids_in_window(&search, day(10)).await, vec!["parked"]);
        // ...and no shard was created in this node's own store.
        assert!(store.window_shards("logs").unwrap().is_empty());
        assert!(svc.served_windows().is_empty());

        // GetCheckpoint on the replica-published window refuses too — never a fabricated
        // "no checkpoint, snapshot 0" over served data.
        let status = svc
            .get_checkpoint(Request::new(GetCheckpointRequest {
                window: day(10),
                index: String::new(),
                shard: 0,
            }))
            .await
            .expect_err("a checkpoint read on a replica-published window must be refused");
        let detail = growlerdb_proto::error_details(&status).expect("structured error");
        assert_eq!(detail.code, "WINDOW_PARKED");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fence_is_permissive_before_the_first_snapshot_and_unrestricted_without_one() {
        // Backward compatibility: classic (non-pool) serve constructs the service without a fence →
        // unrestricted forever (covered throughout this module); a FENCED node is permissive only
        // until the CP's seed snapshot lands, then enforces.
        let (svc, search, _store, _tmp) = service();
        let day = |n: i64| n * DAY;
        let fence = PrimaryFence::fenced();
        let svc = svc.with_primary_fence(fence.clone());

        // Before the first snapshot: create-on-first-write works (the seed snapshot is in flight).
        assert!(
            write_batch(&svc, vec![upsert("a", day(10) + 1)], 1, "b1")
                .await
                .unwrap()
                > 0
        );
        assert_eq!(ids_in_window(&search, day(10)).await, vec!["a"]);

        // The snapshot lands without day 10 (the primary moved away): the node refuses immediately.
        fence.apply_snapshot(&snapshot_units(&[day(11)], &[]));
        let status = write_batch(&svc, vec![upsert("b", day(10) + 2)], 2, "b2")
            .await
            .expect_err("primary moved away → the next write is refused");
        assert!(crate::windowed_routing::is_not_primary(&status));
        // The already-served window still answers reads (unloading is a separate concern, 357.24).
        assert_eq!(ids_in_window(&search, day(10)).await, vec!["a"]);
    }
}
