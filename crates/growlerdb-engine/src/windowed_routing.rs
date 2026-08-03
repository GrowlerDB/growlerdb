//! **Distributed per-window routing**: the two halves that let a standalone
//! [Gateway](crate::gateway::Gateway) address per-window requests when the windows live on remote
//! Nodes. [`WindowNode`] is the **client** half (stamps its window id onto each request before
//! delegating); [`WindowedSearchService`] / [`WindowedSuggestService`] are the **server** halves
//! (dispatch each request to the shard its selector names). Embedded mode needs no selector — each
//! window is its own in-process `LocalNode`.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use growlerdb_proto::v1::Error as WireError;
use growlerdb_proto::v1::{
    AggregateRequest, AggregateResponse, AlterIndexRequest, AlterIndexResponse, BackupIndexRequest,
    BackupIndexResponse, BackupStatusRequest, BackupStatusResponse, CancelReindexRequest,
    CancelReindexResponse, ClosePitRequest, ClosePitResponse, CompactIndexRequest,
    CompactIndexResponse, DescribeIndexRequest, DescribeIndexResponse, ExplainRequest,
    ExplainResponse, ExportRequest, GetByKeyRequest, GetByKeyResponse, OpenPitRequest,
    OpenPitResponse, ReconcileIndexRequest, ReconcileIndexResponse, ReindexIndexRequest,
    ReindexIndexResponse, ReindexStatusRequest, ReindexStatusResponse, SearchRequest,
    SearchResponse, SemanticSearchRequest, SuggestRequest, SuggestResponse,
};
use growlerdb_proto::{
    error_details, to_status, Admin, AdminServer, Lookup, LookupServer, Search, SearchServer,
    Suggest, SuggestServer,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request, Response, Status};

use crate::{AdminService, LookupService, Node, SearchService, SuggestService};

/// Structured wire code for "this node does not serve the addressed unit" — a **routing-staleness**
/// signal, not a request error: the CP may have re-placed the unit or a replica may not have warmed
/// it yet, so another holder can serve it. Responders attach it as the structured
/// [`Error`](growlerdb_proto::v1::Error) detail on a `FailedPrecondition`; read failover
/// ([`FailoverNode`](crate::FailoverNode)) matches it to try the next holder instead of aborting.
pub const UNIT_NOT_SERVED: &str = "UNIT_NOT_SERVED";

/// A `FailedPrecondition` carrying the [`UNIT_NOT_SERVED`] structured code.
pub fn unit_not_served(message: impl Into<String>) -> Status {
    to_status(
        Code::FailedPrecondition,
        WireError::new(UNIT_NOT_SERVED, message),
    )
}

/// Whether `status` is a responder's [`UNIT_NOT_SERVED`] answer — matched on the structured detail,
/// not the message text, so it survives rewording and the wire hop.
pub fn is_unit_not_served(status: &Status) -> bool {
    status.code() == Code::FailedPrecondition
        && error_details(status).is_some_and(|e| e.code == UNIT_NOT_SERVED)
}

/// Structured wire code for "this node holds the addressed unit, but **not as its primary**" — the
/// write-path fencing refusal (D53: one writer per unit). Distinct from [`UNIT_NOT_SERVED`] so a
/// caller can tell *wrong node for writes* (re-resolve the primary) from *not serving at all* (try
/// the next holder). Emitted by the windowed write path
/// ([`WindowedWriteService`](crate::windowed_ingest::WindowedWriteService)) when a `Write` /
/// `GetCheckpoint` addresses a `(index, window)` outside the node's primary set.
/// `FAILED_PRECONDITION`, so the connector re-resolves placement instead of hammering the wrong node.
pub const NOT_PRIMARY: &str = "NOT_PRIMARY";

/// A `FailedPrecondition` carrying the [`NOT_PRIMARY`] structured code.
pub fn not_primary(message: impl Into<String>) -> Status {
    to_status(
        Code::FailedPrecondition,
        WireError::new(NOT_PRIMARY, message),
    )
}

/// Whether `status` is a responder's [`NOT_PRIMARY`] refusal — matched on the structured detail,
/// not the message text, so it survives rewording and the wire hop.
pub fn is_not_primary(status: &Status) -> bool {
    status.code() == Code::FailedPrecondition
        && error_details(status).is_some_and(|e| e.code == NOT_PRIMARY)
}

/// Route a window selector to its entry in a shared `window id → service` map. `window == 0`
/// (unset) is a request-level `InvalidArgument` (a windowed node always expects a selector); a **set
/// but unserved** window is [`unit_not_served`] — a stale route, try-the-next-holder territory.
fn route_window<T: Clone>(
    windows: &Arc<RwLock<BTreeMap<i64, T>>>,
    window: i64,
) -> Result<T, Status> {
    if window == 0 {
        return Err(Status::invalid_argument(
            "a windowed node requires a window selector",
        ));
    }
    windows
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&window)
        .cloned()
        .ok_or_else(|| unit_not_served(format!("window {window} is not served by this node")))
}

/// A node's live `window id → SearchService` map behind a shared lock (dynamic windowed ingest):
/// the write path **inserts** a new window's service on first write and the multiplexer reads it,
/// so a freshly-created window becomes queryable with no restart. The service is `Clone` (an `Arc`
/// handle), so `route` clones it out from under the read lock.
pub type SharedSearchWindows = Arc<RwLock<BTreeMap<i64, SearchService>>>;
/// The suggest counterpart to [`SharedSearchWindows`].
pub type SharedSuggestWindows = Arc<RwLock<BTreeMap<i64, SuggestService>>>;
/// The lookup (GetByKey hydration) counterpart to [`SharedSearchWindows`]: a key can live
/// in any window (its coordinate carries no time), so the Gateway **broadcasts** a hydration to every
/// window and each node's [`WindowedLookupService`] dispatches the one it serves.
pub type SharedLookupWindows = Arc<RwLock<BTreeMap<i64, LookupService>>>;
/// The admin (DescribeIndex) counterpart to [`SharedSearchWindows`]: the Gateway fans a
/// describe to every window and sums the per-window stats into the index total.
pub type SharedAdminWindows = Arc<RwLock<BTreeMap<i64, AdminService>>>;

/// The **server** half of distributed windowed routing: a `Search` service fronting a node's window
/// shards (`window id → SearchService`) that dispatches each request to the shard its
/// [`SearchRequest::window`] selector names. Unserved window → [`unit_not_served`] (a stale route
/// failover walks past); `window = 0` (unset) → `InvalidArgument`. The map is **shared + mutable**
/// ([`SharedSearchWindows`]) so the write path can add a new window without rebuilding the service.
pub struct WindowedSearchService {
    windows: SharedSearchWindows,
}

impl WindowedSearchService {
    /// A multiplexer over the shared `windows` map (`window id → SearchService`).
    pub fn new(windows: SharedSearchWindows) -> Self {
        Self { windows }
    }

    /// Wrap as a mountable tonic [`SearchServer`].
    pub fn into_server(self) -> SearchServer<Self> {
        SearchServer::new(self)
    }

    /// A clone of the [`SearchService`] for `window` ([`unit_not_served`] when unserved). Cloned out
    /// of the read lock so the request runs without holding it.
    fn route(&self, window: i64) -> Result<SearchService, Status> {
        route_window(&self.windows, window)
    }
}

#[tonic::async_trait]
impl Search for WindowedSearchService {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        // The window selector picks the shard; the inner SearchService runs the query against it.
        let window = request.get_ref().window;
        let svc = self.route(window)?;
        Search::search(&svc, request).await
    }

    async fn semantic_search(
        &self,
        request: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let window = request.get_ref().window;
        let svc = self.route(window)?;
        Search::semantic_search(&svc, request).await
    }

    async fn aggregate(
        &self,
        request: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        // Same window-selector dispatch as `search`.
        let window = request.get_ref().window;
        let svc = self.route(window)?;
        Search::aggregate(&svc, request).await
    }

    async fn open_pit(
        &self,
        _request: Request<OpenPitRequest>,
    ) -> Result<Response<OpenPitResponse>, Status> {
        Err(Status::unimplemented(
            "distributed windowed point-in-time is not yet supported",
        ))
    }

    async fn close_pit(
        &self,
        _request: Request<ClosePitRequest>,
    ) -> Result<Response<ClosePitResponse>, Status> {
        Err(Status::unimplemented(
            "distributed windowed point-in-time is not yet supported",
        ))
    }

    async fn explain(
        &self,
        _request: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        // Explain names a doc by coordinate, not a window selector; finding its window would need a
        // scatter. Not supported over a distributed windowed index yet.
        Err(Status::unimplemented(
            "explain is not yet supported over a distributed windowed index",
        ))
    }

    type ExportStream = ReceiverStream<Result<SearchResponse, Status>>;

    async fn export(
        &self,
        _request: Request<ExportRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        Err(Status::unimplemented(
            "distributed windowed export is not yet supported",
        ))
    }
}

/// The suggest counterpart to [`WindowedSearchService`]: a `Suggest` service over a node's
/// `window id → SuggestService` map, dispatching each request to the shard its
/// [`SuggestRequest::window`] selector names. Suggest fans out to *every* window (a term can live in
/// any — no time pruning). Unserved window → [`unit_not_served`]; unset → `InvalidArgument`.
pub struct WindowedSuggestService {
    windows: SharedSuggestWindows,
}

impl WindowedSuggestService {
    /// A multiplexer over the shared `windows` map (`window id → SuggestService`).
    pub fn new(windows: SharedSuggestWindows) -> Self {
        Self { windows }
    }

    /// Wrap as a mountable tonic [`SuggestServer`].
    pub fn into_server(self) -> SuggestServer<Self> {
        SuggestServer::new(self)
    }

    fn route(&self, window: i64) -> Result<SuggestService, Status> {
        route_window(&self.windows, window)
    }
}

#[tonic::async_trait]
impl Suggest for WindowedSuggestService {
    async fn suggest(
        &self,
        request: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        let window = request.get_ref().window;
        let svc = self.route(window)?;
        Suggest::suggest(&svc, request).await
    }
}

/// The **lookup** (GetByKey hydration) counterpart to [`WindowedSearchService`]: a `Lookup` service
/// over a node's `window id → LookupService` map, dispatching each hydration to the shard its
/// [`GetByKeyRequest::window`] selector names. No time pruning (a key's coordinate carries no
/// window), so the Gateway **broadcasts** to every window and the owner returns its row, the rest
/// nothing. Unserved window → [`unit_not_served`]; unset → `InvalidArgument`.
pub struct WindowedLookupService {
    windows: SharedLookupWindows,
}

impl WindowedLookupService {
    /// A multiplexer over the shared `windows` map (`window id → LookupService`).
    pub fn new(windows: SharedLookupWindows) -> Self {
        Self { windows }
    }

    /// Wrap as a mountable tonic [`LookupServer`].
    pub fn into_server(self) -> LookupServer<Self> {
        LookupServer::new(self)
    }

    fn route(&self, window: i64) -> Result<LookupService, Status> {
        route_window(&self.windows, window)
    }
}

#[tonic::async_trait]
impl Lookup for WindowedLookupService {
    async fn get_by_key(
        &self,
        request: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        let window = request.get_ref().window;
        let svc = self.route(window)?;
        Lookup::get_by_key(&svc, request).await
    }
}

/// The **admin** (DescribeIndex) counterpart to [`WindowedSearchService`]: an `Admin` service over a
/// node's `window id → AdminService` map, dispatching a describe to the window its
/// [`DescribeIndexRequest::window`] selector names, so the Gateway can fan a describe to every window
/// and sum the per-window stats. Alter/reindex are cluster-shape ops that don't apply per-window →
/// `Unimplemented`.
pub struct WindowedAdminService {
    windows: SharedAdminWindows,
}

impl WindowedAdminService {
    /// A multiplexer over the shared `windows` map (`window id → AdminService`).
    pub fn new(windows: SharedAdminWindows) -> Self {
        Self { windows }
    }

    /// Wrap as a mountable tonic [`AdminServer`].
    pub fn into_server(self) -> AdminServer<Self> {
        AdminServer::new(self)
    }

    fn route(&self, window: i64) -> Result<AdminService, Status> {
        route_window(&self.windows, window)
    }
}

#[tonic::async_trait]
impl Admin for WindowedAdminService {
    async fn describe_index(
        &self,
        request: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        let window = request.get_ref().window;
        let svc = self.route(window)?;
        Admin::describe_index(&svc, request).await
    }

    async fn alter_index(
        &self,
        _request: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        Err(Status::unimplemented(
            "alter is not supported over a distributed windowed index",
        ))
    }

    async fn reindex_index(
        &self,
        _request: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        Err(Status::unimplemented(
            "reindex is not supported over a distributed windowed index",
        ))
    }

    async fn reindex_status(
        &self,
        _request: Request<ReindexStatusRequest>,
    ) -> Result<Response<ReindexStatusResponse>, Status> {
        Err(Status::unimplemented(
            "reindex is not supported over a distributed windowed index",
        ))
    }

    async fn cancel_reindex(
        &self,
        _request: Request<CancelReindexRequest>,
    ) -> Result<Response<CancelReindexResponse>, Status> {
        Err(Status::unimplemented(
            "reindex is not supported over a distributed windowed index",
        ))
    }

    async fn reconcile_index(
        &self,
        _request: Request<ReconcileIndexRequest>,
    ) -> Result<Response<ReconcileIndexResponse>, Status> {
        Err(Status::unimplemented(
            "reconcile is not supported over a distributed windowed index",
        ))
    }

    async fn compact_index(
        &self,
        _request: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        Err(Status::unimplemented(
            "compact is not supported over a distributed windowed index (windows self-compact)",
        ))
    }

    async fn backup_index(
        &self,
        _request: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        Err(Status::unimplemented(
            "backup is not supported over a distributed windowed index",
        ))
    }

    async fn backup_status(
        &self,
        _request: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        Err(Status::unimplemented(
            "backup status is not supported over a distributed windowed index",
        ))
    }
}

/// The **client** half of distributed windowed routing: a [`Node`] wrapping the remote node serving
/// a window that **stamps the `(index, window)` unit** onto every search/suggest before delegating,
/// so the receiving multiplexer knows which shard to hit. The Gateway holds one per window and
/// scatters to them as plain shards, so [`Gateway::windowed`](crate::gateway::Gateway::windowed) and
/// its pruning are untouched.
///
/// Stamping the **index** too is what lets the Gateway front a **pool node** (D52): a node serving
/// windows of many indexes routes on the index selector first
/// ([`PoolSearchService`](crate::pool_routing::PoolSearchService)), then the window. A single-index
/// node ignores the index selector, so stamping it is harmless.
pub struct WindowNode {
    inner: Arc<dyn Node>,
    index: String,
    window: i64,
}

impl WindowNode {
    /// Front `inner` (a `RemoteNode` to the unit's serving endpoint) as the node for `(index,
    /// window)`.
    pub fn new(inner: Arc<dyn Node>, index: impl Into<String>, window: i64) -> Self {
        Self {
            inner,
            index: index.into(),
            window,
        }
    }

    /// Erase to a shared `dyn Node` for the [Gateway](crate::gateway::Gateway).
    pub fn shared(self) -> Arc<dyn Node> {
        Arc::new(self)
    }

    /// Stamp this node's `(index, window)` unit onto a request selector pair.
    fn stamp(&self, index: &mut String, window: &mut i64) {
        index.clone_from(&self.index);
        *window = self.window;
    }
}

#[tonic::async_trait]
impl Node for WindowNode {
    async fn search(
        &self,
        mut req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.window);
        self.inner.search(req).await
    }

    async fn semantic_search(
        &self,
        mut req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.window);
        self.inner.semantic_search(req).await
    }

    async fn suggest(
        &self,
        mut req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.window);
        self.inner.suggest(req).await
    }

    async fn get_by_key(
        &self,
        mut req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.window);
        self.inner.get_by_key(req).await
    }

    async fn describe_index(
        &self,
        mut req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.window);
        self.inner.describe_index(req).await
    }

    async fn aggregate(
        &self,
        mut req: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.window);
        self.inner.aggregate(req).await
    }

    async fn explain(
        &self,
        mut req: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.explain(req).await
    }

    async fn reindex_index(
        &self,
        mut req: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.reindex_index(req).await
    }

    async fn alter_index(
        &self,
        mut req: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.alter_index(req).await
    }

    async fn compact_index(
        &self,
        mut req: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.compact_index(req).await
    }

    async fn backup_index(
        &self,
        mut req: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.backup_index(req).await
    }

    async fn backup_status(
        &self,
        mut req: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.backup_status(req).await
    }
}

/// The **client** half of distributed **hash-shard** routing (D52) — the hash counterpart to
/// [`WindowNode`]: a [`Node`] wrapping the remote node that holds an **ordinal shard**, stamping the
/// `(index, ordinal)` unit onto every request's `index` + `shard` selectors before delegating, so a
/// pool node fronting many indexes' ordinals dispatches to the right one
/// ([`PoolSearchService`](crate::pool_routing::PoolSearchService) routes a hash index on `shard`).
/// The Gateway holds one per ordinal and scatters as plain shards; a single-shard node ignores the
/// selectors, so stamping is harmless.
pub struct ShardNode {
    inner: Arc<dyn Node>,
    index: String,
    ordinal: u32,
}

impl ShardNode {
    /// Front `inner` (a `RemoteNode` to the unit's serving endpoint) as the node for `(index,
    /// ordinal)`.
    pub fn new(inner: Arc<dyn Node>, index: impl Into<String>, ordinal: u32) -> Self {
        Self {
            inner,
            index: index.into(),
            ordinal,
        }
    }

    /// Erase to a shared `dyn Node` for the [Gateway](crate::gateway::Gateway).
    pub fn shared(self) -> Arc<dyn Node> {
        Arc::new(self)
    }

    /// Stamp this node's `(index, ordinal)` unit onto a request selector pair.
    fn stamp(&self, index: &mut String, shard: &mut u32) {
        index.clone_from(&self.index);
        *shard = self.ordinal;
    }
}

#[tonic::async_trait]
impl Node for ShardNode {
    async fn search(
        &self,
        mut req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.shard);
        self.inner.search(req).await
    }

    async fn semantic_search(
        &self,
        mut req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.shard);
        self.inner.semantic_search(req).await
    }

    async fn suggest(
        &self,
        mut req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.shard);
        self.inner.suggest(req).await
    }

    async fn get_by_key(
        &self,
        mut req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.shard);
        self.inner.get_by_key(req).await
    }

    async fn describe_index(
        &self,
        mut req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.shard);
        self.inner.describe_index(req).await
    }

    async fn aggregate(
        &self,
        mut req: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        let r = req.get_mut();
        self.stamp(&mut r.index, &mut r.shard);
        self.inner.aggregate(req).await
    }

    async fn explain(
        &self,
        mut req: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.explain(req).await
    }

    async fn reindex_index(
        &self,
        mut req: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.reindex_index(req).await
    }

    async fn alter_index(
        &self,
        mut req: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.alter_index(req).await
    }

    async fn compact_index(
        &self,
        mut req: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.compact_index(req).await
    }

    async fn backup_index(
        &self,
        mut req: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.backup_index(req).await
    }

    async fn backup_status(
        &self,
        mut req: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        let r = req.get_mut();
        r.index.clone_from(&self.index);
        self.inner.backup_status(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
        SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
    };
    use growlerdb_index::{LocalIndexStore, Shard, ShardId};
    use growlerdb_proto::v1::SuggestKind;
    use std::sync::Mutex;

    /// A fresh single-doc shard (id = `only`, a KEYWORD field) at `root` — searchable and
    /// suggestable on `id`.
    fn one_doc_shard(root: &std::path::Path, only: &str) -> Arc<Shard> {
        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(only))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(only));
        let doc = LocatedDoc {
            doc: Document::new(key, f),
            iceberg_file: "f".into(),
            row_position: 0,
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
        Arc::new(shard)
    }

    fn one_doc_service(root: &std::path::Path, only: &str) -> SearchService {
        SearchService::new(one_doc_shard(root, only))
    }

    async fn hit_ids(mux: &WindowedSearchService, window: i64) -> Result<Vec<String>, tonic::Code> {
        let req = SearchRequest {
            query: "*".into(),
            limit: 10,
            window,
            ..Default::default()
        };
        match Search::search(mux, Request::new(req)).await {
            Ok(resp) => Ok(resp
                .into_inner()
                .hits
                .iter()
                .map(|h| {
                    let key: CompositeKey = h.coordinates.clone().unwrap().try_into().unwrap();
                    key.identifier[0].1.to_index_string()
                })
                .collect()),
            Err(s) => Err(s.code()),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windowed_search_dispatches_by_selector() {
        let t1 = tempfile::tempdir().unwrap();
        let t2 = tempfile::tempdir().unwrap();
        let mux = WindowedSearchService::new(Arc::new(RwLock::new(BTreeMap::from([
            (10, one_doc_service(t1.path(), "win10doc")),
            (20, one_doc_service(t2.path(), "win20doc")),
        ]))));

        // Each selector reaches exactly its own window's shard...
        assert_eq!(hit_ids(&mux, 10).await.unwrap(), vec!["win10doc"]);
        assert_eq!(hit_ids(&mux, 20).await.unwrap(), vec!["win20doc"]);
        // ...a window this node doesn't serve is the structured not-served refusal (failover walks
        // past it), and the unset `0` is a request-level InvalidArgument.
        assert_eq!(
            hit_ids(&mux, 99).await.unwrap_err(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            hit_ids(&mux, 0).await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
    }

    /// The unserved-window refusal carries the structured [`UNIT_NOT_SERVED`] detail — the signal
    /// [`is_unit_not_served`] (and so `FailoverNode`) matches on, robust to message rewording.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unserved_window_is_the_structured_unit_not_served_refusal() {
        let t1 = tempfile::tempdir().unwrap();
        let mux = WindowedSearchService::new(Arc::new(RwLock::new(BTreeMap::from([(
            10,
            one_doc_service(t1.path(), "win10doc"),
        )]))));
        let err = Search::search(
            &mux,
            Request::new(SearchRequest {
                query: "*".into(),
                window: 99,
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        assert!(
            is_unit_not_served(&err),
            "structured detail attached: {err:?}"
        );
        // The unset selector is NOT a not-served answer — no holder could ever satisfy it.
        let err = Search::search(
            &mux,
            Request::new(SearchRequest {
                query: "*".into(),
                window: 0,
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        assert!(!is_unit_not_served(&err));
    }

    /// A [`Node`] that records the `(index, window)` selector of the last request it received.
    struct RecordingNode(Mutex<(String, i64)>);

    impl RecordingNode {
        fn record(&self, index: &str, window: i64) {
            *self.0.lock().unwrap() = (index.to_string(), window);
        }
        fn seen(&self) -> (String, i64) {
            self.0.lock().unwrap().clone()
        }
    }

    #[tonic::async_trait]
    impl Node for RecordingNode {
        async fn search(
            &self,
            req: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            self.record(&req.get_ref().index, req.get_ref().window);
            Ok(Response::new(SearchResponse::default()))
        }
        async fn semantic_search(
            &self,
            req: Request<SemanticSearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            self.record(&req.get_ref().index, req.get_ref().window);
            Ok(Response::new(SearchResponse::default()))
        }
        async fn suggest(
            &self,
            req: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            self.record(&req.get_ref().index, req.get_ref().window);
            Ok(Response::new(SuggestResponse::default()))
        }
        async fn get_by_key(
            &self,
            req: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            self.record(&req.get_ref().index, req.get_ref().window);
            Ok(Response::new(GetByKeyResponse::default()))
        }
        async fn describe_index(
            &self,
            req: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            self.record(&req.get_ref().index, req.get_ref().window);
            Ok(Response::new(DescribeIndexResponse::default()))
        }
    }

    #[tokio::test]
    async fn window_node_stamps_the_unit_onto_search() {
        let rec = Arc::new(RecordingNode(Mutex::new((String::new(), -1))));
        let node = WindowNode::new(rec.clone(), "logs", 7);
        // A request the Gateway scatters carries neither index nor window; the WindowNode stamps its
        // own (index, window) unit so a pool node dispatches on both.
        node.search(Request::new(SearchRequest {
            query: "*".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
        assert_eq!(rec.seen(), ("logs".to_string(), 7));
    }

    #[tokio::test]
    async fn window_node_stamps_the_unit_onto_semantic_search() {
        let rec = Arc::new(RecordingNode(Mutex::new((String::new(), -1))));
        let node = WindowNode::new(rec.clone(), "logs", 7);
        // Regression guard: semantic_search must stamp the (index, window) unit and forward like
        // `search`, not fall through to the Node trait's Unimplemented default.
        node.semantic_search(Request::new(SemanticSearchRequest {
            query_text: "hello".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
        assert_eq!(rec.seen(), ("logs".to_string(), 7));
    }

    /// A [`Node`] that records the `(index, shard)` selector of the last request it received — the
    /// hash-shard counterpart to [`RecordingNode`], for asserting [`ShardNode`] stamping.
    struct ShardRecordingNode(Mutex<(String, u32)>);

    impl ShardRecordingNode {
        fn seen(&self) -> (String, u32) {
            self.0.lock().unwrap().clone()
        }
    }

    #[tonic::async_trait]
    impl Node for ShardRecordingNode {
        async fn search(
            &self,
            req: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            *self.0.lock().unwrap() = (req.get_ref().index.clone(), req.get_ref().shard);
            Ok(Response::new(SearchResponse::default()))
        }
        async fn semantic_search(
            &self,
            req: Request<SemanticSearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            *self.0.lock().unwrap() = (req.get_ref().index.clone(), req.get_ref().shard);
            Ok(Response::new(SearchResponse::default()))
        }
        async fn suggest(
            &self,
            _req: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Ok(Response::new(SuggestResponse::default()))
        }
        async fn get_by_key(
            &self,
            _req: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Ok(Response::new(GetByKeyResponse::default()))
        }
        async fn describe_index(
            &self,
            _req: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Ok(Response::new(DescribeIndexResponse::default()))
        }
    }

    #[tokio::test]
    async fn shard_node_stamps_the_unit_onto_semantic_search() {
        let rec = Arc::new(ShardRecordingNode(Mutex::new((String::new(), u32::MAX))));
        let node = ShardNode::new(rec.clone(), "catalog", 3);
        // Same regression guard for the hash-shard wrapper: semantic_search must stamp the
        // (index, ordinal) unit onto `index` + `shard` and forward, not 501 on the trait default.
        node.semantic_search(Request::new(SemanticSearchRequest {
            query_text: "hello".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
        assert_eq!(rec.seen(), ("catalog".to_string(), 3));
    }

    async fn suggest_texts(
        mux: &WindowedSuggestService,
        window: i64,
        prefix: &str,
    ) -> Result<Vec<String>, tonic::Code> {
        let req = SuggestRequest {
            field: "id".into(),
            text: prefix.into(),
            limit: 10,
            kind: SuggestKind::Prefix as i32,
            window,
            ..Default::default()
        };
        match Suggest::suggest(mux, Request::new(req)).await {
            Ok(resp) => Ok(resp
                .into_inner()
                .suggestions
                .into_iter()
                .map(|s| s.text)
                .collect()),
            Err(s) => Err(s.code()),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windowed_suggest_dispatches_by_selector() {
        let t1 = tempfile::tempdir().unwrap();
        let t2 = tempfile::tempdir().unwrap();
        let mux = WindowedSuggestService::new(Arc::new(RwLock::new(BTreeMap::from([
            (
                10,
                SuggestService::new(one_doc_shard(t1.path(), "win10doc")),
            ),
            (
                20,
                SuggestService::new(one_doc_shard(t2.path(), "win20doc")),
            ),
        ]))));

        // The `win1` prefix autocompletes only in window 10 (its `win10doc` term)...
        assert_eq!(
            suggest_texts(&mux, 10, "win1").await.unwrap(),
            vec!["win10doc"]
        );
        assert!(suggest_texts(&mux, 20, "win1").await.unwrap().is_empty());
        // ...and `win2` only in window 20 — so the selector is dispatching, not broadcasting.
        assert_eq!(
            suggest_texts(&mux, 20, "win2").await.unwrap(),
            vec!["win20doc"]
        );
        // An unserved window is the structured not-served refusal; the unset `0` stays a
        // request-level InvalidArgument.
        assert_eq!(
            suggest_texts(&mux, 99, "win").await.unwrap_err(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            suggest_texts(&mux, 0, "win").await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn window_node_stamps_the_selector_onto_suggest() {
        let rec = Arc::new(RecordingNode(Mutex::new((String::new(), -1))));
        let node = WindowNode::new(rec.clone(), "logs", 9);
        node.suggest(Request::new(SuggestRequest {
            field: "id".into(),
            text: "x".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
        assert_eq!(rec.seen(), ("logs".to_string(), 9));
    }

    async fn agg_ids(mux: &WindowedSearchService, window: i64) -> Result<String, tonic::Code> {
        let req = AggregateRequest {
            query: "*".into(),
            aggs: r#"{"ids": {"Terms": {"field": "id", "size": 10}}}"#.into(),
            window,
            ..Default::default()
        };
        match Search::aggregate(mux, Request::new(req)).await {
            Ok(resp) => Ok(resp.into_inner().results),
            Err(s) => Err(s.code()),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windowed_aggregate_dispatches_by_selector() {
        let t1 = tempfile::tempdir().unwrap();
        let t2 = tempfile::tempdir().unwrap();
        let mux = WindowedSearchService::new(Arc::new(RwLock::new(BTreeMap::from([
            (10, one_doc_service(t1.path(), "win10doc")),
            (20, one_doc_service(t2.path(), "win20doc")),
        ]))));

        // A terms agg over each window reflects only that window's docs — proof of dispatch, not
        // broadcast.
        let r10 = agg_ids(&mux, 10).await.unwrap();
        assert!(r10.contains("win10doc"), "window 10 agg: {r10}");
        assert!(
            !r10.contains("win20doc"),
            "window 10 agg leaked window 20: {r10}"
        );
        assert!(agg_ids(&mux, 20).await.unwrap().contains("win20doc"));
        // A window this node doesn't serve is the structured not-served refusal.
        assert_eq!(
            agg_ids(&mux, 99).await.unwrap_err(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test]
    async fn window_node_stamps_the_selector_onto_get_by_key() {
        let rec = Arc::new(RecordingNode(Mutex::new((String::new(), -1))));
        let node = WindowNode::new(rec.clone(), "logs", 11);
        // The Gateway broadcasts a hydration with no window; the WindowNode stamps its own so the
        // receiving multiplexer knows which window's shard to hit.
        node.get_by_key(Request::new(GetByKeyRequest::default()))
            .await
            .unwrap();
        assert_eq!(rec.seen(), ("logs".to_string(), 11));
    }

    #[tokio::test]
    async fn window_node_stamps_the_selector_onto_describe() {
        let rec = Arc::new(RecordingNode(Mutex::new((String::new(), -1))));
        let node = WindowNode::new(rec.clone(), "logs", 13);
        node.describe_index(Request::new(DescribeIndexRequest::default()))
            .await
            .unwrap();
        assert_eq!(rec.seen(), ("logs".to_string(), 13));
    }

    async fn describe_num_docs(
        mux: &WindowedAdminService,
        window: i64,
    ) -> Result<u64, tonic::Code> {
        let req = DescribeIndexRequest {
            window,
            ..Default::default()
        };
        match Admin::describe_index(mux, Request::new(req)).await {
            Ok(resp) => Ok(resp.into_inner().stats.unwrap().num_docs),
            Err(s) => Err(s.code()),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windowed_describe_dispatches_by_selector() {
        let t1 = tempfile::tempdir().unwrap();
        let t2 = tempfile::tempdir().unwrap();
        let mux = WindowedAdminService::new(Arc::new(RwLock::new(BTreeMap::from([
            (10, AdminService::new(one_doc_shard(t1.path(), "a"), "docs")),
            (20, AdminService::new(one_doc_shard(t2.path(), "b"), "docs")),
        ]))));
        // Each window reports its own one doc (dispatch, not a broadcast sum); an unserved window
        // is the structured not-served refusal, the unset `0` an InvalidArgument.
        assert_eq!(describe_num_docs(&mux, 10).await.unwrap(), 1);
        assert_eq!(describe_num_docs(&mux, 20).await.unwrap(), 1);
        assert_eq!(
            describe_num_docs(&mux, 99).await.unwrap_err(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(
            describe_num_docs(&mux, 0).await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
    }

    async fn lookup_code(mux: &WindowedLookupService, window: i64) -> tonic::Code {
        // Empty keys → the routed LookupService returns Ok with no rows (no Iceberg read), isolating
        // the *dispatch*: a served window succeeds; unserved → not-served, unset → InvalidArgument.
        let req = GetByKeyRequest {
            window,
            ..Default::default()
        };
        match Lookup::get_by_key(mux, Request::new(req)).await {
            Ok(_) => tonic::Code::Ok,
            Err(s) => s.code(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windowed_lookup_dispatches_by_selector() {
        use growlerdb_source::IcebergConfig;
        let t1 = tempfile::tempdir().unwrap();
        let t2 = tempfile::tempdir().unwrap();
        let mux = WindowedLookupService::new(Arc::new(RwLock::new(BTreeMap::from([
            (
                10,
                LookupService::new(
                    one_doc_shard(t1.path(), "a"),
                    IcebergConfig::local(),
                    "g.docs",
                    crate::test_docs_resolved(),
                ),
            ),
            (
                20,
                LookupService::new(
                    one_doc_shard(t2.path(), "b"),
                    IcebergConfig::local(),
                    "g.docs",
                    crate::test_docs_resolved(),
                ),
            ),
        ]))));
        assert_eq!(lookup_code(&mux, 10).await, tonic::Code::Ok);
        assert_eq!(lookup_code(&mux, 20).await, tonic::Code::Ok);
        assert_eq!(lookup_code(&mux, 99).await, tonic::Code::FailedPrecondition);
        assert_eq!(lookup_code(&mux, 0).await, tonic::Code::InvalidArgument);
    }
}
