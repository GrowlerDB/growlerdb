//! **Per-index dispatch** for a universal-placement-pool node (D52): the outer half of a node
//! serving units from *many* indexes over one endpoint. Each request routes first on its
//! [`SearchRequest::index`](growlerdb_proto::v1::SearchRequest) selector to that index's window
//! map, then the inner [windowed mux](crate::windowed_routing) routes on the window — one process
//! fronts many indexes' windows (the property that kills node-per-index). An index this node
//! doesn't serve is the structured [`unit_not_served`] refusal; an **empty** index selector
//! defaults to the sole served index when there is exactly one.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use growlerdb_proto::v1::{
    AggregateRequest, AggregateResponse, AlterIndexRequest, AlterIndexResponse, BackupIndexRequest,
    BackupIndexResponse, BackupStatusRequest, BackupStatusResponse, CancelReindexRequest,
    CancelReindexResponse, ClosePitRequest, ClosePitResponse, CompactIndexRequest,
    CompactIndexResponse, DescribeIndexRequest, DescribeIndexResponse, ExplainRequest,
    ExplainResponse, ExportRequest, GetByKeyRequest, GetByKeyResponse, GetCheckpointRequest,
    GetCheckpointResponse, OpenPitRequest, OpenPitResponse, ReconcileIndexRequest,
    ReconcileIndexResponse, ReindexIndexRequest, ReindexIndexResponse, ReindexPrecheckRequest,
    ReindexPrecheckResponse, ReindexStatusRequest, ReindexStatusResponse, SearchRequest,
    SearchResponse, SemanticSearchRequest, SuggestRequest, SuggestResponse, WriteRequest,
    WriteResponse,
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
    SharedSuggestWindows,
};
use crate::write_service::WriteService;

/// A pool node's live `index → SharedSearchWindows` map behind a shared lock: an assigned index is
/// inserted with its window map, so a freshly-assigned index becomes queryable with no restart
/// (the windowed maps' dynamic growth, lifted one level up to the index).
pub type SharedSearchIndexes = Arc<RwLock<BTreeMap<String, SharedSearchWindows>>>;
/// The suggest counterpart to [`SharedSearchIndexes`].
pub type SharedSuggestIndexes = Arc<RwLock<BTreeMap<String, SharedSuggestWindows>>>;
/// The lookup (GetByKey hydration) counterpart to [`SharedSearchIndexes`].
pub type SharedLookupIndexes = Arc<RwLock<BTreeMap<String, SharedLookupWindows>>>;
/// The admin (DescribeIndex) counterpart to [`SharedSearchIndexes`].
pub type SharedAdminIndexes = Arc<RwLock<BTreeMap<String, SharedAdminWindows>>>;
/// A pool node's live `index → WindowedWriteService` map: the write counterpart to
/// [`SharedSearchIndexes`]. Each entry is a full single-index windowed writer; the
/// [`PoolWriteService`] routes each `Write` on its `index` selector. Behind a lock so a
/// dynamically-assigned index's writer is inserted without a restart.
pub type SharedWriteIndexes = Arc<RwLock<BTreeMap<String, WindowedWriteService>>>;

/// A pool node's live `index → (ordinal → WriteService)` map for **hash-sharded** indexes (D52):
/// the write counterpart to a hash index's ordinal read maps. Each ordinal is a single-shard
/// [`WriteService`]; the [`PoolWriteService`] routes each `Write` on its `(index, shard)` selector.
/// Ordinals are keyed `ordinal as i64` to share the pool's generic `(index, unit)` routing. Both
/// levels behind locks so a dynamically-assigned ordinal registers without a restart.
pub type SharedHashWriteUnits = Arc<RwLock<BTreeMap<i64, WriteService>>>;
/// The `index → ordinal writers` map behind the hash half of [`PoolWriteService`] — see
/// [`SharedHashWriteUnits`].
pub type SharedHashWriteIndexes = Arc<RwLock<BTreeMap<String, SharedHashWriteUnits>>>;

/// A pool node's live `index → is-hash?` map (D52): `true` when the index is **hash/partition-
/// sharded** (units are ordinal shards, routed on `shard`), `false`/absent for a **windowed** index
/// (units are windows, routed on `window`). Read to pick the unit selector per index, so one
/// endpoint serves both kinds. Behind a lock for restart-free registration.
pub type SharedIndexKinds = Arc<RwLock<BTreeMap<String, bool>>>;

/// The generic shape shared by the four `SharedX Indexes` read maps: `index → (unit → leaf service)`,
/// both levels behind locks. Lets [`route_unit`] be generic over the leaf service `T`.
type SharedIndexUnits<T> = Arc<RwLock<BTreeMap<String, Arc<RwLock<BTreeMap<i64, T>>>>>>;

/// Whether `index` is hash-sharded on this pool node (route units on `shard`, not `window`). An empty
/// selector on a node serving exactly one index resolves to that index's kind — the sole-index
/// drop-in, matching [`route_index`].
fn index_is_hash(kinds: &SharedIndexKinds, index: &str) -> bool {
    let k = kinds.read().unwrap_or_else(|e| e.into_inner());
    if index.is_empty() {
        return k.len() == 1 && *k.values().next().expect("len == 1");
    }
    k.get(index).copied().unwrap_or(false)
}

/// Resolve `(index, unit)` to its leaf service on a pool node: route the index to its per-unit map,
/// then the unit — the request's **`shard`** ordinal for a hash index or its **`window`** for a
/// windowed one (per [`index_is_hash`]). Maps are `i64`-keyed either way, so both kinds share one
/// storage type; a hash index routes on `shard` directly (ordinal 0 is valid — unlike a windowed
/// node where `window == 0` means "no selector"). An unserved index/unit → [`unit_not_served`].
fn route_unit<T: Clone>(
    by_index: &SharedIndexUnits<T>,
    kinds: &SharedIndexKinds,
    index: &str,
    window: i64,
    shard: u32,
) -> Result<T, Status> {
    let units = route_index(by_index, index)?;
    let hash = index_is_hash(kinds, index);
    let key = if hash {
        shard as i64
    } else if window == 0 {
        return Err(Status::invalid_argument(
            "a windowed node requires a window selector",
        ));
    } else {
        window
    };
    let found = units
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&key)
        .cloned();
    found.ok_or_else(|| {
        unit_not_served(if hash {
            format!("shard {key} is not served by this node")
        } else {
            format!("window {key} is not served by this node")
        })
    })
}

/// Route an index selector to its per-index shared map `T`, or the structured [`unit_not_served`]
/// refusal when this node doesn't serve it (a stale route). An **empty** selector resolves to the
/// sole served index when there is exactly one (drop-in for a single-index node whose caller didn't
/// stamp the index); ambiguous (multi-index) it stays a request-level `InvalidArgument`.
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
/// [`WindowedSearchService`] which routes on the window.
pub struct PoolSearchService {
    by_index: SharedSearchIndexes,
    kinds: SharedIndexKinds,
}

impl PoolSearchService {
    /// A multiplexer over the shared `index → units` map, routing each index's units on `window`
    /// (windowed) or `shard` (hash) per `kinds`.
    pub fn new(by_index: SharedSearchIndexes, kinds: SharedIndexKinds) -> Self {
        Self { by_index, kinds }
    }

    /// Wrap as a mountable tonic [`SearchServer`].
    pub fn into_server(self) -> SearchServer<Self> {
        SearchServer::new(self)
    }
}

#[tonic::async_trait]
impl Search for PoolSearchService {
    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let r = request.get_ref();
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, r.window, r.shard)?;
        Search::search(&svc, request).await
    }

    async fn semantic_search(
        &self,
        request: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let r = request.get_ref();
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, r.window, r.shard)?;
        Search::semantic_search(&svc, request).await
    }

    async fn aggregate(
        &self,
        request: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        let r = request.get_ref();
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, r.window, r.shard)?;
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
        request: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        // Explain names a doc by coordinate, not a window/shard selector. For a HASH index the
        // gateway already routed to the owner, so delegate to shard 0 (the sole unit in the common
        // single-shard case). A distributed WINDOWED index can't pick the window from a coordinate
        // without a scatter, so it stays unimplemented.
        let r = request.get_ref();
        if !index_is_hash(&self.kinds, &r.index) {
            return Err(Status::unimplemented(
                "explain is not yet supported over a distributed windowed index",
            ));
        }
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, 0, 0)?;
        Search::explain(&svc, request).await
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
    kinds: SharedIndexKinds,
}

impl PoolSuggestService {
    /// A multiplexer over the shared `index → units` map (window- or shard-routed per `kinds`).
    pub fn new(by_index: SharedSuggestIndexes, kinds: SharedIndexKinds) -> Self {
        Self { by_index, kinds }
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
        let r = request.get_ref();
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, r.window, r.shard)?;
        Suggest::suggest(&svc, request).await
    }
}

/// The lookup (GetByKey hydration) counterpart to [`PoolSearchService`]: routes on the index selector,
/// then delegates to a [`WindowedLookupService`].
pub struct PoolLookupService {
    by_index: SharedLookupIndexes,
    kinds: SharedIndexKinds,
}

impl PoolLookupService {
    /// A multiplexer over the shared `index → units` map (window- or shard-routed per `kinds`).
    pub fn new(by_index: SharedLookupIndexes, kinds: SharedIndexKinds) -> Self {
        Self { by_index, kinds }
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
        let r = request.get_ref();
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, r.window, r.shard)?;
        Lookup::get_by_key(&svc, request).await
    }
}

/// The admin (DescribeIndex) counterpart to [`PoolSearchService`]: routes on the index + unit
/// selectors to the owning window/shard's [`AdminService`]. The **reindex lifecycle**
/// (`reindex_index` / `reindex_status` / `cancel_reindex` / `reindex_precheck`) routes per-unit so the
/// control plane's coordinated reindex reaches this node's shard/window. Alter/reconcile/compact/backup
/// stay `Unimplemented` — cluster-shape ops the CP drives at the registry / not per-unit here (alter is
/// applied CP-side then rebuilt via `reindex_index`), as on the windowed mux.
pub struct PoolAdminService {
    by_index: SharedAdminIndexes,
    kinds: SharedIndexKinds,
}

impl PoolAdminService {
    /// A multiplexer over the shared `index → units` map (window- or shard-routed per `kinds`).
    pub fn new(by_index: SharedAdminIndexes, kinds: SharedIndexKinds) -> Self {
        Self { by_index, kinds }
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
        let r = request.get_ref();
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, r.window, r.shard)?;
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
        request: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        // Route the coordinated reindex to the owning unit (hash: `shard_ordinal`; windowed: `window`)
        // and delegate to its per-unit AdminService, which enforces the write-fence + single-flight.
        let r = request.get_ref();
        let svc = route_unit(
            &self.by_index,
            &self.kinds,
            &r.index,
            r.window,
            r.shard_ordinal,
        )?;
        Admin::reindex_index(&svc, request).await
    }

    async fn reindex_status(
        &self,
        request: Request<ReindexStatusRequest>,
    ) -> Result<Response<ReindexStatusResponse>, Status> {
        // Status/cancel/precheck carry only `index` + `window` (window-oriented); a hash index's sole
        // unit resolves via the shard-0 default, matching the coordinated driver's per-window polling.
        let r = request.get_ref();
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, r.window, 0)?;
        Admin::reindex_status(&svc, request).await
    }

    async fn cancel_reindex(
        &self,
        request: Request<CancelReindexRequest>,
    ) -> Result<Response<CancelReindexResponse>, Status> {
        let r = request.get_ref();
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, r.window, 0)?;
        Admin::cancel_reindex(&svc, request).await
    }

    async fn reindex_precheck(
        &self,
        request: Request<ReindexPrecheckRequest>,
    ) -> Result<Response<ReindexPrecheckResponse>, Status> {
        let r = request.get_ref();
        let svc = route_unit(&self.by_index, &self.kinds, &r.index, r.window, 0)?;
        Admin::reindex_precheck(&svc, request).await
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

/// The **index-dispatch** `Write` service for a pool node (D52): routes each `Write` /
/// `GetCheckpoint` on its `index` selector. A **windowed** index dispatches to its
/// [`WindowedWriteService`] (which routes on the window, creating the shard on first write); a
/// **hash-sharded** index (per [`kinds`](SharedIndexKinds)) dispatches on the `shard` ordinal to
/// that ordinal's [`WriteService`]. The write counterpart to [`PoolSearchService`], with the same
/// empty-selector-defaults-to-sole-index rule.
pub struct PoolWriteService {
    windowed: SharedWriteIndexes,
    hash: SharedHashWriteIndexes,
    kinds: SharedIndexKinds,
}

impl PoolWriteService {
    /// A write multiplexer over the shared windowed `index → writer` map and the hash
    /// `index → ordinal → writer` map, picking the unit selector per index via `kinds`.
    pub fn new(
        windowed: SharedWriteIndexes,
        hash: SharedHashWriteIndexes,
        kinds: SharedIndexKinds,
    ) -> Self {
        Self {
            windowed,
            hash,
            kinds,
        }
    }

    /// Wrap as a mountable tonic [`WriteServer`] with the large-commit decode cap (a catch-up batch
    /// spanning many windows can be large).
    pub fn into_server(self) -> WriteServer<Self> {
        WriteServer::new(self).max_decoding_message_size(MAX_WRITE_MESSAGE_BYTES)
    }

    /// The single-index windowed writer for `index`, or [`unit_not_served`] if unserved (an empty
    /// selector resolves to the sole served index when there is exactly one).
    fn writer(&self, index: &str) -> Result<WindowedWriteService, Status> {
        route_index(&self.windowed, index)
    }

    /// The hash ordinal writer for `(index, shard)`, or [`unit_not_served`] if the node holds no such
    /// ordinal (a stale route — the connector re-resolves the ordinal's owner).
    fn hash_writer(&self, index: &str, shard: u32) -> Result<WriteService, Status> {
        route_unit(&self.hash, &self.kinds, index, 0, shard)
    }
}

#[tonic::async_trait]
impl Write for PoolWriteService {
    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let r = request.get_ref();
        if index_is_hash(&self.kinds, &r.index) {
            let svc = self.hash_writer(&r.index, r.shard)?;
            return Write::write(&svc, request).await;
        }
        let svc = self.writer(&r.index)?;
        Write::write(&svc, request).await
    }

    async fn get_checkpoint(
        &self,
        request: Request<GetCheckpointRequest>,
    ) -> Result<Response<GetCheckpointResponse>, Status> {
        let r = request.get_ref();
        if index_is_hash(&self.kinds, &r.index) {
            let svc = self.hash_writer(&r.index, r.shard)?;
            return Write::get_checkpoint(&svc, request).await;
        }
        let svc = self.writer(&r.index)?;
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
        hit_ids_req(
            mux,
            SearchRequest {
                query: "*".into(),
                limit: 10,
                index: index.into(),
                window,
                ..Default::default()
            },
        )
        .await
    }

    /// As [`hit_ids`] but routing on the **`shard`** (ordinal) selector — a hash index's unit.
    async fn hit_ids_shard(
        mux: &PoolSearchService,
        index: &str,
        shard: u32,
    ) -> Result<Vec<String>, tonic::Code> {
        hit_ids_req(
            mux,
            SearchRequest {
                query: "*".into(),
                limit: 10,
                index: index.into(),
                shard,
                ..Default::default()
            },
        )
        .await
    }

    async fn hit_ids_req(
        mux: &PoolSearchService,
        req: SearchRequest,
    ) -> Result<Vec<String>, tonic::Code> {
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
    async fn pool_explain_delegates_for_hash_index_and_refuses_windowed() {
        use growlerdb_proto::v1::Coordinates;
        let tmp = tempfile::tempdir().unwrap();
        // A hash (non-windowed) index `docs` with one doc `d1`; its unit is keyed on shard ordinal 0.
        let unit = SearchService::new(one_doc_shard(tmp.path(), "docs", "d1"));
        let by_index: SharedSearchIndexes = Arc::new(RwLock::new(BTreeMap::from([(
            "docs".to_string(),
            Arc::new(RwLock::new(BTreeMap::from([(0i64, unit)]))),
        )])));
        let kinds: SharedIndexKinds =
            Arc::new(RwLock::new(BTreeMap::from([("docs".to_string(), true)])));
        let mux = PoolSearchService::new(by_index, kinds);

        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("d1"))]);
        let req = ExplainRequest {
            query: "id:d1".into(),
            coordinates: Some(Coordinates::from(&key)),
            index: "docs".into(),
            ..Default::default()
        };
        // Regression: a hash pool index delegates to the unit's SearchService::explain (was 501).
        let resp = Search::explain(&mux, Request::new(req)).await;
        assert!(
            resp.is_ok(),
            "hash-index explain should delegate, got {:?}",
            resp.err().map(|s| s.code())
        );

        // A distributed WINDOWED index still can't pick the window from a coordinate → Unimplemented.
        let kinds_w: SharedIndexKinds =
            Arc::new(RwLock::new(BTreeMap::from([("evt".to_string(), false)])));
        let mux_w = PoolSearchService::new(Arc::new(RwLock::new(BTreeMap::new())), kinds_w);
        let req_w = ExplainRequest {
            query: "*".into(),
            coordinates: Some(Coordinates::from(&key)),
            index: "evt".into(),
            ..Default::default()
        };
        let err = Search::explain(&mux_w, Request::new(req_w))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
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
            shard: 0,
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
        let writes = PoolWriteService::new(
            Arc::new(RwLock::new(BTreeMap::from([
                ("alpha".to_string(), wa),
                ("beta".to_string(), wb),
            ]))),
            Default::default(),
            Default::default(), // all windowed
        );
        // ...and a pool reader over the same shared window maps.
        let reads = PoolSearchService::new(
            Arc::new(RwLock::new(BTreeMap::from([
                ("alpha".to_string(), search_a),
                ("beta".to_string(), search_b),
            ]))),
            Default::default(), // all windowed (route on window)
        );

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
                shard: 0,
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
        let mux = PoolSearchService::new(
            Arc::new(RwLock::new(BTreeMap::from([
                (
                    "alpha".to_string(),
                    windows_for(ta.path(), "alpha", "alphadoc"),
                ),
                (
                    "beta".to_string(),
                    windows_for(tb.path(), "beta", "betadoc"),
                ),
            ]))),
            Default::default(),
        );

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
        let two = PoolSearchService::new(
            Arc::new(RwLock::new(BTreeMap::from([
                (
                    "alpha".to_string(),
                    windows_for(ta.path(), "alpha", "alphadoc"),
                ),
                (
                    "beta".to_string(),
                    windows_for(tb.path(), "beta", "betadoc"),
                ),
            ]))),
            Default::default(),
        );
        assert_eq!(
            hit_ids(&two, "", 10).await.unwrap_err(),
            tonic::Code::InvalidArgument
        );

        // Exactly one served ⇒ an empty selector defaults to it (drop-in for a single-index node).
        let tc = tempfile::tempdir().unwrap();
        let one = PoolSearchService::new(
            Arc::new(RwLock::new(BTreeMap::from([(
                "solo".to_string(),
                windows_for(tc.path(), "solo", "solodoc"),
            )]))),
            Default::default(),
        );
        assert_eq!(hit_ids(&one, "", 10).await.unwrap(), vec!["solodoc"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pool_routes_a_hash_index_on_the_shard_selector() {
        // A HASH index's units are ordinal shards (keyed by ordinal-as-i64); with the index marked
        // hash in `kinds`, the pool routes each read on the request's `shard` selector, not `window`.
        let t0 = tempfile::tempdir().unwrap();
        let t1 = tempfile::tempdir().unwrap();
        let ordinals: SharedSearchWindows = Arc::new(RwLock::new(BTreeMap::from([
            (
                0_i64,
                SearchService::new(one_doc_shard(t0.path(), "h", "ord0")),
            ),
            (
                1_i64,
                SearchService::new(one_doc_shard(t1.path(), "h", "ord1")),
            ),
        ])));
        let kinds: SharedIndexKinds =
            Arc::new(RwLock::new(BTreeMap::from([("h".to_string(), true)])));
        let mux = PoolSearchService::new(
            Arc::new(RwLock::new(BTreeMap::from([("h".to_string(), ordinals)]))),
            kinds,
        );

        // Route on `shard`: ordinal 0 → its doc, ordinal 1 → its doc.
        assert_eq!(hit_ids_shard(&mux, "h", 0).await.unwrap(), vec!["ord0"]);
        assert_eq!(hit_ids_shard(&mux, "h", 1).await.unwrap(), vec!["ord1"]);
        // An unserved ordinal is the structured UNIT_NOT_SERVED refusal (FailedPrecondition — a
        // stale-route signal the gateway fails past), not a silent empty result.
        assert_eq!(
            hit_ids_shard(&mux, "h", 9).await.unwrap_err(),
            tonic::Code::FailedPrecondition
        );
        // A hash index ignores the `window` selector — routing keys off `shard`, so window=1 with
        // shard unset (0) reaches ordinal 0, not window 1.
        assert_eq!(hit_ids(&mux, "h", 1).await.unwrap(), vec!["ord0"]);
    }

    /// A fresh **empty** single-shard for `index` (KEYWORD `id`), writable through a [`WriteService`].
    fn empty_shard(root: &std::path::Path, index: &str) -> Arc<Shard> {
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
        Arc::new(
            LocalIndexStore::open(root)
                .unwrap()
                .create_shard(&ShardId::single(index), &idx)
                .unwrap(),
        )
    }

    /// A single-doc upsert batch for the KEYWORD `id` field (a hash write is one flat batch — no
    /// window bucketing).
    fn id_upsert(id: &str, n: i64) -> CommitBatch {
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        CommitBatch::from_upserts(
            vec![LocatedDoc {
                doc: Document::new(
                    CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]),
                    f,
                ),
            }],
            SourceCheckpoint::iceberg(n),
            "b1",
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pool_hash_write_dispatches_by_ordinal_and_is_queryable() {
        use crate::shard_handle::ShardHandle;
        use crate::write_service::WriteService;

        let t0 = tempfile::tempdir().unwrap();
        let t1 = tempfile::tempdir().unwrap();
        // Two ordinal shards of one hash index `h`; each ordinal's read services and its per-ordinal
        // WriteService share ONE handle, so a written doc is queryable through the same map.
        let h0 = ShardHandle::new(empty_shard(t0.path(), "h"));
        let h1 = ShardHandle::new(empty_shard(t1.path(), "h"));
        let reads_ord: SharedSearchWindows = Arc::new(RwLock::new(BTreeMap::from([
            (0_i64, SearchService::new(h0.clone())),
            (1_i64, SearchService::new(h1.clone())),
        ])));
        let writes_ord: SharedHashWriteUnits = Arc::new(RwLock::new(BTreeMap::from([
            (0_i64, WriteService::new(h0.clone(), "h", 4)),
            (1_i64, WriteService::new(h1.clone(), "h", 4)),
        ])));
        let kinds: SharedIndexKinds =
            Arc::new(RwLock::new(BTreeMap::from([("h".to_string(), true)])));

        let reads = PoolSearchService::new(
            Arc::new(RwLock::new(BTreeMap::from([("h".to_string(), reads_ord)]))),
            kinds.clone(),
        );
        let writes = PoolWriteService::new(
            Default::default(), // no windowed indexes on this node
            Arc::new(RwLock::new(BTreeMap::from([("h".to_string(), writes_ord)]))),
            kinds,
        );

        // A write tagged to ordinal 1 dispatches to ordinal 1's shard.
        let req = WriteRequest {
            batch: Some(id_upsert("x", 1).into()),
            index: "h".into(),
            shard: 1,
        };
        assert!(
            Write::write(&writes, Request::new(req))
                .await
                .unwrap()
                .into_inner()
                .snapshot
                > 0
        );
        // Ordinal 1 has the doc; ordinal 0 stayed empty (dispatch went to the RIGHT ordinal, not
        // window-partitioned across both).
        assert_eq!(hit_ids_shard(&reads, "h", 1).await.unwrap(), vec!["x"]);
        assert!(hit_ids_shard(&reads, "h", 0).await.unwrap().is_empty());

        // GetCheckpoint dispatches by ordinal too: ordinal 1 resumes from its committed checkpoint.
        let cp = Write::get_checkpoint(
            &writes,
            Request::new(GetCheckpointRequest {
                window: 0,
                index: "h".into(),
                shard: 1,
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(cp.snapshot, 1);

        // An ordinal this node doesn't hold is the structured not-served refusal (a stale route —
        // FailedPrecondition, not a malformed request), so the connector re-resolves the owner.
        let req9 = WriteRequest {
            batch: Some(id_upsert("y", 2).into()),
            index: "h".into(),
            shard: 9,
        };
        assert_eq!(
            Write::write(&writes, Request::new(req9))
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pool_admin_routes_reindex_lifecycle_and_keeps_alter_unimplemented() {
        use crate::AdminService;
        let tmp = tempfile::tempdir().unwrap();
        // A hash (non-windowed) index `catalog` with one unit keyed on shard ordinal 0 — the
        // single-shard pool index shape that used to 501 on reindex (the coordinated driver dials the
        // pool node per unit). The test AdminService has no source context, so it fails *past* the
        // pool layer with its own message — the point is it no longer blanket-rejects "on a pool node".
        let unit = AdminService::new(one_doc_shard(tmp.path(), "catalog", "c1"), "catalog");
        let by_index: SharedAdminIndexes = Arc::new(RwLock::new(BTreeMap::from([(
            "catalog".to_string(),
            Arc::new(RwLock::new(BTreeMap::from([(0i64, unit)]))),
        )])));
        let kinds: SharedIndexKinds =
            Arc::new(RwLock::new(BTreeMap::from([("catalog".to_string(), true)])));
        let mux = PoolAdminService::new(by_index, kinds);

        // Regression: reindex_status routes to the served unit's AdminService (not the pool reject).
        let err = Admin::reindex_status(
            &mux,
            Request::new(ReindexStatusRequest {
                index: "catalog".into(),
                window: 0,
            }),
        )
        .await
        .unwrap_err();
        assert!(
            !err.message().contains("pool node"),
            "reindex_status should route to the unit, got: {}",
            err.message()
        );

        // An unserved index is refused by the unit router (FailedPrecondition), not blanket Unimplemented.
        for code in [
            Admin::reindex_index(
                &mux,
                Request::new(ReindexIndexRequest {
                    index: "missing".into(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap_err()
            .code(),
            Admin::reindex_status(
                &mux,
                Request::new(ReindexStatusRequest {
                    index: "missing".into(),
                    window: 0,
                }),
            )
            .await
            .unwrap_err()
            .code(),
        ] {
            assert_eq!(code, tonic::Code::FailedPrecondition);
        }

        // Alter stays Unimplemented on a pool node — the CP applies it at the registry, then rebuilds
        // via reindex_index (a pool node can't durably change the registry).
        let err = Admin::alter_index(
            &mux,
            Request::new(AlterIndexRequest {
                index: "catalog".into(),
                ..Default::default()
            }),
        )
        .await
        .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("pool node"));
    }
}
