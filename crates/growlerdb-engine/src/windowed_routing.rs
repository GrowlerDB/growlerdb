//! **Distributed per-window routing** (task-82): the two halves that let a standalone
//! [Gateway](crate::gateway::Gateway) prune a time-filtered search to the owning windows when those
//! windows live on remote Nodes — the distributed mirror of the embedded `serve_windowed`.
//!
//! A windowed index is served by **one process fronting many window shards** (so they share one
//! read-through cold cache, task-80), reachable on a single gRPC endpoint. So a per-window request
//! must say *which* window it targets:
//!
//! * [`WindowNode`] is the **client** half — the Gateway holds one per window (all over the same
//!   remote endpoint), and each stamps its window id onto every search/suggest before delegating,
//!   so the Gateway's existing scatter/prune ([`Gateway::windowed`](crate::gateway::Gateway::windowed))
//!   needs no change.
//! * [`WindowedSearchService`] / [`WindowedSuggestService`] are the **server** halves — `Search` /
//!   `Suggest` services over `window id → service` maps that dispatch each request to the shard the
//!   selector names.
//!
//! Embedded mode is unaffected: there each window is its own in-process `LocalNode`, so no selector
//! is needed.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use growlerdb_proto::v1::{
    AggregateRequest, AggregateResponse, AlterIndexRequest, AlterIndexResponse, BackupIndexRequest,
    BackupIndexResponse, BackupStatusRequest, BackupStatusResponse, ClosePitRequest,
    ClosePitResponse, CompactIndexRequest, CompactIndexResponse, DescribeIndexRequest,
    DescribeIndexResponse, ExplainRequest, ExplainResponse, ExportRequest, GetByKeyRequest,
    GetByKeyResponse, OpenPitRequest, OpenPitResponse, ReconcileIndexRequest,
    ReconcileIndexResponse, ReindexIndexRequest, ReindexIndexResponse, SearchRequest,
    SearchResponse, SuggestRequest, SuggestResponse,
};
use growlerdb_proto::{
    Admin, AdminServer, Lookup, LookupServer, Search, SearchServer, Suggest, SuggestServer,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::{AdminService, LookupService, Node, SearchService, SuggestService};

/// A node's live `window id → SearchService` map behind a shared lock (task-219 dynamic windowed
/// ingest): the windowed write path **inserts** a new window's service on first write, and the
/// multiplexer reads it — so a freshly-created window becomes queryable with no restart. The service
/// is `Clone` (an `Arc` handle), so `route` clones it out from under the read lock.
pub type SharedSearchWindows = Arc<RwLock<BTreeMap<i64, SearchService>>>;
/// The suggest counterpart to [`SharedSearchWindows`].
pub type SharedSuggestWindows = Arc<RwLock<BTreeMap<i64, SuggestService>>>;
/// The lookup (GetByKey hydration) counterpart to [`SharedSearchWindows`] (task-225): a key can live
/// in any window (its coordinate carries no time), so the Gateway **broadcasts** a hydration to every
/// window and each node's [`WindowedLookupService`] dispatches the one it serves.
pub type SharedLookupWindows = Arc<RwLock<BTreeMap<i64, LookupService>>>;
/// The admin (DescribeIndex) counterpart to [`SharedSearchWindows`] (task-225): the Gateway fans a
/// describe to every window and sums the per-window stats into the index total.
pub type SharedAdminWindows = Arc<RwLock<BTreeMap<i64, AdminService>>>;

/// The **server** half of distributed windowed routing (task-82): a `Search` service that fronts a
/// node's many window shards (`window id → SearchService`) and dispatches each request to the shard
/// its [`SearchRequest::window`] selector names. A request for a window this node doesn't serve is
/// an `InvalidArgument` (the Gateway only routes a window to a node that owns it, so this means a
/// stale shard map). `window = 0` (unset) also fails — a windowed node always expects a selector.
///
/// The window map is **shared + mutable** ([`SharedSearchWindows`], task-219) so the windowed write
/// path can add a newly-created window without rebuilding the service.
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

    /// A clone of the [`SearchService`] for `window`, or `InvalidArgument` if this node doesn't serve
    /// it. Cloned out of the read lock so the request runs without holding it.
    fn route(&self, window: i64) -> Result<SearchService, Status> {
        self.windows
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&window)
            .cloned()
            .ok_or_else(|| {
                Status::invalid_argument(format!("window {window} is not served by this node"))
            })
    }
}

#[tonic::async_trait]
impl Search for WindowedSearchService {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        // The window selector picks the shard; the inner SearchService runs the (auth'd) query
        // against it exactly as a single-shard node would (it ignores the now-satisfied selector).
        let window = request.get_ref().window;
        let svc = self.route(window)?;
        Search::search(&svc, request).await
    }

    async fn aggregate(
        &self,
        request: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        // Aggregate dispatches by the same window selector as search (task-82): the Gateway prunes a
        // time-filtered aggregation to the overlapping windows, then scatters a partial to each.
        let window = request.get_ref().window;
        let svc = self.route(window)?;
        Search::aggregate(&svc, request).await
    }

    async fn open_pit(
        &self,
        _request: Request<OpenPitRequest>,
    ) -> Result<Response<OpenPitResponse>, Status> {
        Err(Status::unimplemented(
            "distributed windowed point-in-time is not yet supported (task-82 follow-on)",
        ))
    }

    async fn close_pit(
        &self,
        _request: Request<ClosePitRequest>,
    ) -> Result<Response<ClosePitResponse>, Status> {
        Err(Status::unimplemented(
            "distributed windowed point-in-time is not yet supported (task-82 follow-on)",
        ))
    }

    async fn explain(
        &self,
        _request: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        // Explain names a doc by coordinate, not a window selector; finding its window would need a
        // scatter. Not supported over a distributed windowed index yet (task-102 follow-on).
        Err(Status::unimplemented(
            "explain is not yet supported over a distributed windowed index (task-102 follow-on)",
        ))
    }

    type ExportStream = ReceiverStream<Result<SearchResponse, Status>>;

    async fn export(
        &self,
        _request: Request<ExportRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        Err(Status::unimplemented(
            "distributed windowed export is not yet supported (task-82 follow-on)",
        ))
    }
}

/// The suggest counterpart to [`WindowedSearchService`] (task-82): a `Suggest` service over a
/// node's `window id → SuggestService` map that dispatches each request to the shard its
/// [`SuggestRequest::window`] selector names. Suggest fans out to *every* window (a term can live in
/// any of them — no time pruning), so the Gateway scatters to all windows and each is dispatched
/// here. Unknown/unset window → `InvalidArgument`.
pub struct WindowedSuggestService {
    windows: SharedSuggestWindows,
}

impl WindowedSuggestService {
    /// A multiplexer over the shared `windows` map (`window id → SuggestService`, task-219).
    pub fn new(windows: SharedSuggestWindows) -> Self {
        Self { windows }
    }

    /// Wrap as a mountable tonic [`SuggestServer`].
    pub fn into_server(self) -> SuggestServer<Self> {
        SuggestServer::new(self)
    }

    fn route(&self, window: i64) -> Result<SuggestService, Status> {
        self.windows
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&window)
            .cloned()
            .ok_or_else(|| {
                Status::invalid_argument(format!("window {window} is not served by this node"))
            })
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

/// The **lookup** (GetByKey hydration) counterpart to [`WindowedSearchService`] (task-225): a `Lookup`
/// service over a node's `window id → LookupService` map that dispatches each hydration to the shard
/// its [`GetByKeyRequest::window`] selector names. Unlike search there's no time pruning — a key's
/// coordinate carries no window — so the Gateway **broadcasts** to every window and each is dispatched
/// here; the window that owns the key returns its row, the rest return nothing. Unknown/unset window
/// → `InvalidArgument` (the Gateway only routes a window to a node that serves it).
pub struct WindowedLookupService {
    windows: SharedLookupWindows,
}

impl WindowedLookupService {
    /// A multiplexer over the shared `windows` map (`window id → LookupService`, task-219/225).
    pub fn new(windows: SharedLookupWindows) -> Self {
        Self { windows }
    }

    /// Wrap as a mountable tonic [`LookupServer`].
    pub fn into_server(self) -> LookupServer<Self> {
        LookupServer::new(self)
    }

    fn route(&self, window: i64) -> Result<LookupService, Status> {
        self.windows
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&window)
            .cloned()
            .ok_or_else(|| {
                Status::invalid_argument(format!("window {window} is not served by this node"))
            })
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

/// The **admin** (DescribeIndex) counterpart to [`WindowedSearchService`] (task-225): an `Admin`
/// service over a node's `window id → AdminService` map that dispatches a describe to the window its
/// [`DescribeIndexRequest::window`] selector names, so the Gateway can fan a describe to every window
/// and sum the per-window stats into the index total. Alter/reindex are cluster-shape ops that don't
/// apply per-window over a distributed windowed index — they return `Unimplemented`.
pub struct WindowedAdminService {
    windows: SharedAdminWindows,
}

impl WindowedAdminService {
    /// A multiplexer over the shared `windows` map (`window id → AdminService`, task-219/225).
    pub fn new(windows: SharedAdminWindows) -> Self {
        Self { windows }
    }

    /// Wrap as a mountable tonic [`AdminServer`].
    pub fn into_server(self) -> AdminServer<Self> {
        AdminServer::new(self)
    }

    fn route(&self, window: i64) -> Result<AdminService, Status> {
        self.windows
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&window)
            .cloned()
            .ok_or_else(|| {
                Status::invalid_argument(format!("window {window} is not served by this node"))
            })
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
            "alter is not supported over a distributed windowed index (task-225 follow-on)",
        ))
    }

    async fn reindex_index(
        &self,
        _request: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        Err(Status::unimplemented(
            "reindex is not supported over a distributed windowed index (task-225 follow-on)",
        ))
    }

    async fn reconcile_index(
        &self,
        _request: Request<ReconcileIndexRequest>,
    ) -> Result<Response<ReconcileIndexResponse>, Status> {
        Err(Status::unimplemented(
            "reconcile is not supported over a distributed windowed index (task-225 follow-on)",
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
            "backup is not supported over a distributed windowed index (task-225 follow-on)",
        ))
    }

    async fn backup_status(
        &self,
        _request: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        Err(Status::unimplemented(
            "backup status is not supported over a distributed windowed index (task-225 follow-on)",
        ))
    }
}

/// The **client** half of distributed windowed routing (task-82): a [`Node`] that wraps the remote
/// node serving a window and **stamps that window's id** onto every search/suggest before
/// delegating, so the receiving multiplexer knows which shard to hit. The Gateway holds one per window
/// (often several over the same endpoint) and scatters to them exactly as it would plain shards —
/// the window addressing is localized here, so [`Gateway::windowed`](crate::gateway::Gateway::windowed)
/// and its pruning are untouched.
pub struct WindowNode {
    inner: Arc<dyn Node>,
    window: i64,
}

impl WindowNode {
    /// Front `inner` (a `RemoteNode` to the window's serving endpoint) as the node for `window`.
    pub fn new(inner: Arc<dyn Node>, window: i64) -> Self {
        Self { inner, window }
    }

    /// Erase to a shared `dyn Node` for the [Gateway](crate::gateway::Gateway).
    pub fn shared(self) -> Arc<dyn Node> {
        Arc::new(self)
    }
}

#[tonic::async_trait]
impl Node for WindowNode {
    async fn search(
        &self,
        mut req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        req.get_mut().window = self.window;
        self.inner.search(req).await
    }

    async fn suggest(
        &self,
        mut req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        req.get_mut().window = self.window;
        self.inner.suggest(req).await
    }

    async fn get_by_key(
        &self,
        mut req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        req.get_mut().window = self.window;
        self.inner.get_by_key(req).await
    }

    async fn describe_index(
        &self,
        mut req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        req.get_mut().window = self.window;
        self.inner.describe_index(req).await
    }

    async fn aggregate(
        &self,
        mut req: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        req.get_mut().window = self.window;
        self.inner.aggregate(req).await
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
        // ...and a window this node doesn't serve (incl. the unset `0`) is a loud InvalidArgument.
        assert_eq!(
            hit_ids(&mux, 99).await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            hit_ids(&mux, 0).await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
    }

    /// A [`Node`] that records the `window` selector of the last search/suggest it received.
    struct RecordingNode(Mutex<i64>);

    #[tonic::async_trait]
    impl Node for RecordingNode {
        async fn search(
            &self,
            req: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            *self.0.lock().unwrap() = req.get_ref().window;
            Ok(Response::new(SearchResponse::default()))
        }
        async fn suggest(
            &self,
            req: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            *self.0.lock().unwrap() = req.get_ref().window;
            Ok(Response::new(SuggestResponse::default()))
        }
        async fn get_by_key(
            &self,
            req: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            *self.0.lock().unwrap() = req.get_ref().window;
            Ok(Response::new(GetByKeyResponse::default()))
        }
        async fn describe_index(
            &self,
            req: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            *self.0.lock().unwrap() = req.get_ref().window;
            Ok(Response::new(DescribeIndexResponse::default()))
        }
    }

    #[tokio::test]
    async fn window_node_stamps_the_selector_onto_search() {
        let rec = Arc::new(RecordingNode(Mutex::new(-1)));
        let node = WindowNode::new(rec.clone(), 7);
        // A request the Gateway scatters carries no window; the WindowNode stamps its own.
        node.search(Request::new(SearchRequest {
            query: "*".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
        assert_eq!(*rec.0.lock().unwrap(), 7);
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
        // A window this node doesn't serve (incl. unset `0`) is a loud InvalidArgument.
        assert_eq!(
            suggest_texts(&mux, 99, "win").await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            suggest_texts(&mux, 0, "win").await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn window_node_stamps_the_selector_onto_suggest() {
        let rec = Arc::new(RecordingNode(Mutex::new(-1)));
        let node = WindowNode::new(rec.clone(), 9);
        node.suggest(Request::new(SuggestRequest {
            field: "id".into(),
            text: "x".into(),
            ..Default::default()
        }))
        .await
        .unwrap();
        assert_eq!(*rec.0.lock().unwrap(), 9);
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
        // broadcast (aggregate now routes by selector instead of returning Unimplemented).
        let r10 = agg_ids(&mux, 10).await.unwrap();
        assert!(r10.contains("win10doc"), "window 10 agg: {r10}");
        assert!(
            !r10.contains("win20doc"),
            "window 10 agg leaked window 20: {r10}"
        );
        assert!(agg_ids(&mux, 20).await.unwrap().contains("win20doc"));
        // A window this node doesn't serve is a loud InvalidArgument.
        assert_eq!(
            agg_ids(&mux, 99).await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test]
    async fn window_node_stamps_the_selector_onto_get_by_key() {
        let rec = Arc::new(RecordingNode(Mutex::new(-1)));
        let node = WindowNode::new(rec.clone(), 11);
        // The Gateway broadcasts a hydration with no window; the WindowNode stamps its own so the
        // receiving multiplexer knows which window's shard to hit (task-225).
        node.get_by_key(Request::new(GetByKeyRequest::default()))
            .await
            .unwrap();
        assert_eq!(*rec.0.lock().unwrap(), 11);
    }

    #[tokio::test]
    async fn window_node_stamps_the_selector_onto_describe() {
        let rec = Arc::new(RecordingNode(Mutex::new(-1)));
        let node = WindowNode::new(rec.clone(), 13);
        node.describe_index(Request::new(DescribeIndexRequest::default()))
            .await
            .unwrap();
        assert_eq!(*rec.0.lock().unwrap(), 13);
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
        // Each window reports its own one doc (dispatch, not a broadcast sum); an unserved/unset
        // window is a loud InvalidArgument.
        assert_eq!(describe_num_docs(&mux, 10).await.unwrap(), 1);
        assert_eq!(describe_num_docs(&mux, 20).await.unwrap(), 1);
        assert_eq!(
            describe_num_docs(&mux, 99).await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            describe_num_docs(&mux, 0).await.unwrap_err(),
            tonic::Code::InvalidArgument
        );
    }

    async fn lookup_code(mux: &WindowedLookupService, window: i64) -> tonic::Code {
        // Empty keys → the routed LookupService returns Ok with no rows (no Iceberg read), isolating
        // the *dispatch*: a served window succeeds; an unserved/unset one is InvalidArgument.
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
                ),
            ),
            (
                20,
                LookupService::new(
                    one_doc_shard(t2.path(), "b"),
                    IcebergConfig::local(),
                    "g.docs",
                ),
            ),
        ]))));
        assert_eq!(lookup_code(&mux, 10).await, tonic::Code::Ok);
        assert_eq!(lookup_code(&mux, 20).await, tonic::Code::Ok);
        assert_eq!(lookup_code(&mux, 99).await, tonic::Code::InvalidArgument);
        assert_eq!(lookup_code(&mux, 0).await, tonic::Code::InvalidArgument);
    }
}
