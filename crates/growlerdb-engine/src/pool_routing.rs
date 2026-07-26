//! **Per-index dispatch** for a universal-placement-pool node (D52): the outer half of a node that
//! serves units from *many* indexes over one gRPC endpoint.
//!
//! A pool node hosts a CP-assigned set of `(index, window)` units drawn from several indexes. The
//! existing [windowed multiplexers](crate::windowed_routing) already dispatch a request to the right
//! **window** within one index; these [`PoolSearchService`] / … wrap them with an **index** layer:
//! each request first routes on its [`SearchRequest::index`](growlerdb_proto::v1::SearchRequest)
//! selector to that index's window map, then the inner windowed mux routes on the window. So one
//! process fronts many indexes' windows — the multi-index-per-node property that kills node-per-index.
//!
//! The two layers compose cleanly: the index map holds one [`SharedSearchWindows`] per served index
//! (the same shared, mutable maps the windowed write path already grows), and the inner
//! [`WindowedSearchService`] is rebuilt per call — cheap, it just wraps an `Arc`. An index this node
//! doesn't serve is the structured [`unit_not_served`] refusal, exactly as an unserved window is —
//! a stale-route signal read failover walks past; an **empty** index selector defaults to the sole
//! served index when there is exactly one (so a pool service is a drop-in even for a node that
//! happens to serve a single index).

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use growlerdb_proto::v1::{
    AggregateRequest, AggregateResponse, AlterIndexRequest, AlterIndexResponse, BackupIndexRequest,
    BackupIndexResponse, BackupStatusRequest, BackupStatusResponse, ClosePitRequest,
    ClosePitResponse, CompactIndexRequest, CompactIndexResponse, DescribeIndexRequest,
    DescribeIndexResponse, ExplainRequest, ExplainResponse, ExportRequest, GetByKeyRequest,
    GetByKeyResponse, GetCheckpointRequest, GetCheckpointResponse, OpenPitRequest, OpenPitResponse,
    ReconcileIndexRequest, ReconcileIndexResponse, ReindexIndexRequest, ReindexIndexResponse,
    SearchRequest, SearchResponse, SemanticSearchRequest, SuggestRequest, SuggestResponse,
    WriteRequest, WriteResponse,
};
use growlerdb_proto::{
    Admin, AdminServer, Lookup, LookupServer, Search, SearchServer, Suggest, SuggestServer, Write,
    WriteServer,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::windowed_ingest::{WindowedWriteService, MAX_WRITE_MESSAGE_BYTES};
use crate::windowed_routing::{
    unit_not_served, SharedAdminWindows, SharedLookupWindows, SharedSearchWindows,
    SharedSuggestWindows, WindowedAdminService, WindowedLookupService, WindowedSearchService,
    WindowedSuggestService,
};

/// A pool node's live `index → SharedSearchWindows` map behind a shared lock: an index this node is
/// assigned units for is inserted with its window map; the multiplexer reads it, so a freshly-assigned
/// index becomes queryable with no restart (the same dynamic-growth property the windowed maps have,
/// lifted one level to the index).
pub type SharedSearchIndexes = Arc<RwLock<BTreeMap<String, SharedSearchWindows>>>;
/// The suggest counterpart to [`SharedSearchIndexes`].
pub type SharedSuggestIndexes = Arc<RwLock<BTreeMap<String, SharedSuggestWindows>>>;
/// The lookup (GetByKey hydration) counterpart to [`SharedSearchIndexes`].
pub type SharedLookupIndexes = Arc<RwLock<BTreeMap<String, SharedLookupWindows>>>;
/// The admin (DescribeIndex) counterpart to [`SharedSearchIndexes`].
pub type SharedAdminIndexes = Arc<RwLock<BTreeMap<String, SharedAdminWindows>>>;
/// A pool node's live `index → WindowedWriteService` map: the write counterpart to
/// [`SharedSearchIndexes`]. Each entry is a full single-index windowed writer (it owns that index's
/// windows and creates them on first write); the [`PoolWriteService`] routes each `Write` on its
/// `index` selector to the right one. Behind a lock so a dynamically-assigned index's writer can be
/// inserted without a restart (the read maps grow the same way, one level down at the window).
pub type SharedWriteIndexes = Arc<RwLock<BTreeMap<String, WindowedWriteService>>>;

/// Route an index selector to its per-index shared map `T` (one of the `SharedX` window maps), or
/// the structured [`unit_not_served`] refusal when this node doesn't serve it (a stale route —
/// try-the-next-holder territory, like an unserved window). An **empty** selector resolves to the
/// sole served index when there is exactly one — a pool service then works unchanged for a
/// single-index node whose caller didn't stamp the index; ambiguous (multi-index) it stays a
/// request-level `InvalidArgument` — no holder could satisfy it either.
fn route_index<T: Clone>(
    by_index: &Arc<RwLock<BTreeMap<String, T>>>,
    index: &str,
) -> Result<T, Status> {
    let map = by_index.read().unwrap_or_else(|e| e.into_inner());
    if index.is_empty() {
        if map.len() == 1 {
            return Ok(map.values().next().cloned().expect("len == 1"));
        }
        return Err(Status::invalid_argument(
            "a pool node serving multiple indexes requires an index selector",
        ));
    }
    map.get(index)
        .cloned()
        .ok_or_else(|| unit_not_served(format!("index `{index}` is not served by this node")))
}

/// The **index-dispatch** `Search` service for a pool node: routes each request on its
/// [`SearchRequest::index`] selector to that index's window map, then delegates to a
/// [`WindowedSearchService`] which routes on the window. Both layers are pure selectors — the inner
/// [`SearchService`](crate::SearchService) runs the (auth'd) query once the unit is resolved.
pub struct PoolSearchService {
    by_index: SharedSearchIndexes,
}

impl PoolSearchService {
    /// A multiplexer over the shared `index → windows` map.
    pub fn new(by_index: SharedSearchIndexes) -> Self {
        Self { by_index }
    }

    /// Wrap as a mountable tonic [`SearchServer`].
    pub fn into_server(self) -> SearchServer<Self> {
        SearchServer::new(self)
    }

    /// The inner windowed mux for `index`, or [`unit_not_served`] if unserved.
    fn windowed(&self, index: &str) -> Result<WindowedSearchService, Status> {
        Ok(WindowedSearchService::new(route_index(
            &self.by_index,
            index,
        )?))
    }
}

#[tonic::async_trait]
impl Search for PoolSearchService {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let svc = self.windowed(&request.get_ref().index)?;
        Search::search(&svc, request).await
    }

    async fn semantic_search(
        &self,
        request: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let svc = self.windowed(&request.get_ref().index)?;
        Search::semantic_search(&svc, request).await
    }

    async fn aggregate(
        &self,
        request: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        let svc = self.windowed(&request.get_ref().index)?;
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

/// The suggest counterpart to [`PoolSearchService`]: routes on the index selector, then delegates to
/// a [`WindowedSuggestService`].
pub struct PoolSuggestService {
    by_index: SharedSuggestIndexes,
}

impl PoolSuggestService {
    /// A multiplexer over the shared `index → windows` map.
    pub fn new(by_index: SharedSuggestIndexes) -> Self {
        Self { by_index }
    }

    /// Wrap as a mountable tonic [`SuggestServer`].
    pub fn into_server(self) -> SuggestServer<Self> {
        SuggestServer::new(self)
    }
}

#[tonic::async_trait]
impl Suggest for PoolSuggestService {
    async fn suggest(
        &self,
        request: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        let svc =
            WindowedSuggestService::new(route_index(&self.by_index, &request.get_ref().index)?);
        Suggest::suggest(&svc, request).await
    }
}

/// The lookup (GetByKey hydration) counterpart to [`PoolSearchService`]: routes on the index selector,
/// then delegates to a [`WindowedLookupService`].
pub struct PoolLookupService {
    by_index: SharedLookupIndexes,
}

impl PoolLookupService {
    /// A multiplexer over the shared `index → windows` map.
    pub fn new(by_index: SharedLookupIndexes) -> Self {
        Self { by_index }
    }

    /// Wrap as a mountable tonic [`LookupServer`].
    pub fn into_server(self) -> LookupServer<Self> {
        LookupServer::new(self)
    }
}

#[tonic::async_trait]
impl Lookup for PoolLookupService {
    async fn get_by_key(
        &self,
        request: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        let svc =
            WindowedLookupService::new(route_index(&self.by_index, &request.get_ref().index)?);
        Lookup::get_by_key(&svc, request).await
    }
}

/// The admin (DescribeIndex) counterpart to [`PoolSearchService`]: routes on the index selector, then
/// delegates to a [`WindowedAdminService`]. Alter/reindex/reconcile/compact/backup stay
/// `Unimplemented` (cluster-shape ops that don't apply per-unit on a pool node), as on the windowed
/// mux it wraps.
pub struct PoolAdminService {
    by_index: SharedAdminIndexes,
}

impl PoolAdminService {
    /// A multiplexer over the shared `index → windows` map.
    pub fn new(by_index: SharedAdminIndexes) -> Self {
        Self { by_index }
    }

    /// Wrap as a mountable tonic [`AdminServer`].
    pub fn into_server(self) -> AdminServer<Self> {
        AdminServer::new(self)
    }
}

#[tonic::async_trait]
impl Admin for PoolAdminService {
    async fn describe_index(
        &self,
        request: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        let svc = WindowedAdminService::new(route_index(&self.by_index, &request.get_ref().index)?);
        Admin::describe_index(&svc, request).await
    }

    async fn alter_index(
        &self,
        _request: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        Err(Status::unimplemented(
            "alter is not supported on a pool node",
        ))
    }

    async fn reindex_index(
        &self,
        _request: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        Err(Status::unimplemented(
            "reindex is not supported on a pool node",
        ))
    }

    async fn reconcile_index(
        &self,
        _request: Request<ReconcileIndexRequest>,
    ) -> Result<Response<ReconcileIndexResponse>, Status> {
        Err(Status::unimplemented(
            "reconcile is not supported on a pool node",
        ))
    }

    async fn compact_index(
        &self,
        _request: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        Err(Status::unimplemented(
            "compact is not supported on a pool node (windows self-compact)",
        ))
    }

    async fn backup_index(
        &self,
        _request: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        Err(Status::unimplemented(
            "backup is not supported on a pool node",
        ))
    }

    async fn backup_status(
        &self,
        _request: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        Err(Status::unimplemented(
            "backup status is not supported on a pool node",
        ))
    }
}

/// The **index-dispatch** `Write` service for a pool node (D52): routes each `Write` / `GetCheckpoint`
/// on its `index` selector to that index's [`WindowedWriteService`], which then routes on the window
/// (creating the window shard on first write and publishing it to the query paths). The write
/// counterpart to [`PoolSearchService`]; the same empty-selector-defaults-to-sole-index rule holds, so
/// a single-index pool node is writable by a connector that doesn't stamp the index.
pub struct PoolWriteService {
    by_index: SharedWriteIndexes,
}

impl PoolWriteService {
    /// A write multiplexer over the shared `index → writer` map.
    pub fn new(by_index: SharedWriteIndexes) -> Self {
        Self { by_index }
    }

    /// Wrap as a mountable tonic [`WriteServer`] with the large-commit decode cap (a catch-up batch
    /// spanning many windows can be large — matching the single-index windowed writer).
    pub fn into_server(self) -> WriteServer<Self> {
        WriteServer::new(self).max_decoding_message_size(MAX_WRITE_MESSAGE_BYTES)
    }

    /// The single-index windowed writer for `index`, or [`unit_not_served`] if unserved (an empty
    /// selector resolves to the sole served index when there is exactly one).
    fn writer(&self, index: &str) -> Result<WindowedWriteService, Status> {
        route_index(&self.by_index, index)
    }
}

#[tonic::async_trait]
impl Write for PoolWriteService {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let svc = self.writer(&request.get_ref().index)?;
        Write::write(&svc, request).await
    }

    async fn get_checkpoint(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<GetCheckpointResponse>, Status> {
        let svc = self.writer(&request.get_ref().index)?;
        Write::get_checkpoint(&svc, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SearchService;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
        SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
    };
    use growlerdb_index::{LocalIndexStore, Shard, ShardId};

    /// A fresh single-doc shard for `index` (a KEYWORD `id` field), holding `only`.
    fn one_doc_shard(root: &std::path::Path, index: &str, only: &str) -> Arc<Shard> {
        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(&format!(
            "name: {index}\nsource: {{ iceberg: {{ catalog: g, table: g.{index} }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD, fast: true }} ] }}\n",
        ))
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::window(index, 10), &idx)
            .unwrap();
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(only))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(only));
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![LocatedDoc {
                    doc: Document::new(key, f),
                    iceberg_file: "f".into(),
                    row_position: 0,
                }],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        Arc::new(shard)
    }

    /// A one-index window map holding a single window (10) with one doc.
    fn windows_for(root: &std::path::Path, index: &str, only: &str) -> SharedSearchWindows {
        Arc::new(RwLock::new(BTreeMap::from([(
            10,
            SearchService::new(one_doc_shard(root, index, only)),
        )])))
    }

    async fn hit_ids(
        mux: &PoolSearchService,
        index: &str,
        window: i64,
    ) -> Result<Vec<String>, tonic::Code> {
        let req = SearchRequest {
            query: "*".into(),
            limit: 10,
            index: index.into(),
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

    /// A windowed index def (`id` KEYWORD + a `ts` Long window field), for the pool write path.
    fn windowed_resolved(index: &str) -> growlerdb_core::ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("ts", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(&format!(
            "name: {index}\nsource: {{ iceberg: {{ catalog: g, table: g.{index} }} }}\nwindowing: {{ field: ts, granularity: daily }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD, fast: true }}, {{ path: ts, format: epoch_us, fast: true }} ] }}\n",
        ))
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    /// A single-index [`WindowedWriteService`] for `index` sharing `search` (so a written doc is
    /// queryable through the same map a [`PoolSearchService`] reads) — the per-index writer a pool
    /// node holds. Returns the writer and its tempdir (kept alive by the caller).
    fn writer_for(
        index: &str,
        search: SharedSearchWindows,
    ) -> (WindowedWriteService, tempfile::TempDir) {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let resolved = windowed_resolved(index);
        let windowing = resolved.windowing.clone().unwrap();
        let suggest: SharedSuggestWindows = Arc::new(RwLock::new(BTreeMap::new()));
        let lookup: SharedLookupWindows = Arc::new(RwLock::new(BTreeMap::new()));
        let admin: SharedAdminWindows = Arc::new(RwLock::new(BTreeMap::new()));
        let gw = Arc::new(crate::Gateway::windowed(vec![], windowing, vec![]));
        let svc = WindowedWriteService::new(
            store,
            resolved,
            format!("g.{index}"),
            growlerdb_source::IcebergConfig::local(),
            BTreeMap::new(),
            search,
            suggest,
            lookup,
            admin,
            gw,
            Arc::new(|_w, _h| {}),
        )
        .unwrap();
        (svc, tmp)
    }

    /// An upsert of `id` into daily window `day` (canonical micros).
    fn win_upsert(id: &str, day: i64) -> growlerdb_core::DocOp {
        const DAY: i64 = 86_400_000_000;
        let ts = day * DAY + 5;
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("ts".to_string(), Value::Int(ts));
        growlerdb_core::DocOp::Upsert(LocatedDoc {
            doc: Document::new(
                CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]),
                f,
            ),
            iceberg_file: "f".into(),
            row_position: 0,
        })
    }

    async fn pool_write(
        mux: &PoolWriteService,
        index: &str,
        batch: CommitBatch,
    ) -> Result<u64, tonic::Code> {
        let req = WriteRequest {
            batch: Some(batch.into()),
            index: index.into(),
        };
        match Write::write(mux, Request::new(req)).await {
            Ok(resp) => Ok(resp.into_inner().snapshot),
            Err(s) => Err(s.code()),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pool_write_dispatches_by_index_and_creates_windows_lazily() {
        const DAY: i64 = 86_400_000_000;
        // The read-side maps shared with the writers: a write into an index's window must be
        // queryable through the same map a PoolSearchService fronts.
        let search_a: SharedSearchWindows = Arc::new(RwLock::new(BTreeMap::new()));
        let search_b: SharedSearchWindows = Arc::new(RwLock::new(BTreeMap::new()));
        let (wa, _ta) = writer_for("alpha", search_a.clone());
        let (wb, _tb) = writer_for("beta", search_b.clone());

        // One pool writer fronting BOTH indexes' writers — the multi-index-per-node write path.
        let writes = PoolWriteService::new(Arc::new(RwLock::new(BTreeMap::from([
            ("alpha".to_string(), wa),
            ("beta".to_string(), wb),
        ]))));
        // ...and a pool reader over the same shared window maps.
        let reads = PoolSearchService::new(Arc::new(RwLock::new(BTreeMap::from([
            ("alpha".to_string(), search_a),
            ("beta".to_string(), search_b),
        ]))));

        // Each write is dispatched to its index's writer, which lazily creates the day-10 window.
        assert!(
            pool_write(
                &writes,
                "alpha",
                CommitBatch::new(
                    vec![win_upsert("a", 10)],
                    SourceCheckpoint::iceberg(1),
                    "b1"
                )
            )
            .await
            .unwrap()
                > 0
        );
        assert!(
            pool_write(
                &writes,
                "beta",
                CommitBatch::new(
                    vec![win_upsert("b", 10)],
                    SourceCheckpoint::iceberg(1),
                    "b1"
                )
            )
            .await
            .unwrap()
                > 0
        );

        // The doc landed in the CORRECT index's window (dispatch went to the right writer).
        assert_eq!(hit_ids(&reads, "alpha", 10 * DAY).await.unwrap(), vec!["a"]);
        assert_eq!(hit_ids(&reads, "beta", 10 * DAY).await.unwrap(), vec!["b"]);
        assert!(hit_ids(&reads, "alpha", 10 * DAY).await.unwrap() != vec!["b".to_string()]);

        // GetCheckpoint dispatches by index too: alpha's day-10 window resumes from its checkpoint.
        let cp = Write::get_checkpoint(
            &writes,
            Request::new(GetCheckpointRequest {
                window: 10 * DAY,
                index: "alpha".into(),
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(cp.snapshot, 1);

        // An index this node doesn't serve is the structured not-served refusal (as on the read
        // path — a stale route, not a malformed request).
        assert_eq!(
            pool_write(
                &writes,
                "gamma",
                CommitBatch::new(
                    vec![win_upsert("c", 10)],
                    SourceCheckpoint::iceberg(1),
                    "b1"
                )
            )
            .await
            .unwrap_err(),
            tonic::Code::FailedPrecondition
        );
        // An empty selector with >1 index served is ambiguous → InvalidArgument.
        assert_eq!(
            pool_write(
                &writes,
                "",
                CommitBatch::new(
                    vec![win_upsert("d", 10)],
                    SourceCheckpoint::iceberg(1),
                    "b1"
                )
            )
            .await
            .unwrap_err(),
            tonic::Code::InvalidArgument
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pool_search_dispatches_by_index_then_window() {
        let ta = tempfile::tempdir().unwrap();
        let tb = tempfile::tempdir().unwrap();
        // One node, TWO indexes — the multi-index-per-node property (kills node-per-index).
        let mux = PoolSearchService::new(Arc::new(RwLock::new(BTreeMap::from([
            (
                "alpha".to_string(),
                windows_for(ta.path(), "alpha", "alphadoc"),
            ),
            (
                "beta".to_string(),
                windows_for(tb.path(), "beta", "betadoc"),
            ),
        ]))));

        // Each (index, window) reaches exactly its own shard...
        assert_eq!(hit_ids(&mux, "alpha", 10).await.unwrap(), vec!["alphadoc"]);
        assert_eq!(hit_ids(&mux, "beta", 10).await.unwrap(), vec!["betadoc"]);
        // ...a served index but an unserved window is the inner windowed mux's not-served refusal...
        assert_eq!(
            hit_ids(&mux, "alpha", 99).await.unwrap_err(),
            tonic::Code::FailedPrecondition
        );
        // ...and an index this node doesn't serve is the same refusal at the outer layer.
        assert_eq!(
            hit_ids(&mux, "gamma", 10).await.unwrap_err(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_index_defaults_only_when_exactly_one_served() {
        let ta = tempfile::tempdir().unwrap();
        let tb = tempfile::tempdir().unwrap();

        // Two indexes served ⇒ an empty selector is ambiguous → InvalidArgument.
        let two = PoolSearchService::new(Arc::new(RwLock::new(BTreeMap::from([
            (
                "alpha".to_string(),
                windows_for(ta.path(), "alpha", "alphadoc"),
            ),
            (
                "beta".to_string(),
                windows_for(tb.path(), "beta", "betadoc"),
            ),
        ]))));
        assert_eq!(
            hit_ids(&two, "", 10).await.unwrap_err(),
            tonic::Code::InvalidArgument
        );

        // Exactly one served ⇒ an empty selector defaults to it (drop-in for a single-index node).
        let tc = tempfile::tempdir().unwrap();
        let one = PoolSearchService::new(Arc::new(RwLock::new(BTreeMap::from([(
            "solo".to_string(),
            windows_for(tc.path(), "solo", "solodoc"),
        )]))));
        assert_eq!(hit_ids(&one, "", 10).await.unwrap(), vec!["solodoc"]);
    }
}
