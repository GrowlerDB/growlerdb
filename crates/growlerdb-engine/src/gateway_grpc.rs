//! The **public gRPC Engine-API front**: tonic service adapters that
//! implement the Engine-API service traits by routing through the [`Gateway`] — the gRPC
//! counterpart to the [REST front](crate::rest), so native gRPC clients (and the
//! [SDK](https://docs.rs/growlerdb-client)) can target the Gateway instead of a Node directly.
//!
//! Scope mirrors the REST front: the **query + describe + aggregate** surface (`search`,
//! `suggest`, `get_by_key`, `describe_index`, `aggregate`). PIT/export and admin *mutations*
//! are intentionally **not** routed here — single-shard scroll + cross-shard affinity are
//! not yet routed, and alter / reindex are Node-direct / Control-Plane operations. Those methods
//! return `Unimplemented` with a pointer, rather than silently degrading.

use std::sync::Arc;

use growlerdb_proto::v1::{
    AggregateRequest, AggregateResponse, AlterIndexRequest, AlterIndexResponse, BackupIndexRequest,
    BackupIndexResponse, BackupStatusRequest, BackupStatusResponse, ClosePitRequest,
    ClosePitResponse, CompactIndexRequest, CompactIndexResponse, DescribeIndexRequest,
    DescribeIndexResponse, ExplainRequest, ExplainResponse, ExportRequest, GetByKeyRequest,
    GetByKeyResponse, OpenPitRequest, OpenPitResponse, ReconcileIndexRequest,
    ReconcileIndexResponse, ReindexIndexRequest, ReindexIndexResponse, SearchRequest,
    SearchResponse, SemanticSearchRequest, SuggestRequest, SuggestResponse,
};
use growlerdb_proto::{
    Admin, AdminServer, Lookup, LookupServer, Search, SearchServer, Suggest, SuggestServer,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::gateway::Gateway;

/// Reason returned for Engine-API methods the Gateway does not route yet.
fn not_routed(what: &str) -> Status {
    Status::unimplemented(format!(
        "{what} is not routed through the Gateway yet — call the Node directly \
         (cross-shard routing lands later; admin mutations are Node/Control-Plane)"
    ))
}

/// Mount all Gateway-backed Engine-API gRPC services for `gw` on a tonic server builder.
/// Returns the four servers ready to `.add_service(...)`.
pub fn servers(
    gw: Arc<Gateway>,
) -> (
    SearchServer<GatewaySearch>,
    SuggestServer<GatewaySuggest>,
    LookupServer<GatewayLookup>,
    AdminServer<GatewayAdmin>,
) {
    (
        SearchServer::new(GatewaySearch(gw.clone())),
        SuggestServer::new(GatewaySuggest(gw.clone())),
        LookupServer::new(GatewayLookup(gw.clone())),
        AdminServer::new(GatewayAdmin(gw)),
    )
}

/// `Search` over the Gateway: routes `search`; PIT/export are not routed here.
#[derive(Clone)]
pub struct GatewaySearch(Arc<Gateway>);

#[tonic::async_trait]
impl Search for GatewaySearch {
    async fn search(
        &self,
        req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.0.search(req).await
    }

    async fn semantic_search(
        &self,
        req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.0.semantic_search(req).await
    }

    async fn open_pit(
        &self,
        _req: Request<OpenPitRequest>,
    ) -> Result<Response<OpenPitResponse>, Status> {
        Err(not_routed("OpenPit"))
    }

    async fn close_pit(
        &self,
        _req: Request<ClosePitRequest>,
    ) -> Result<Response<ClosePitResponse>, Status> {
        Err(not_routed("ClosePit"))
    }

    type ExportStream = ReceiverStream<Result<SearchResponse, Status>>;

    async fn export(
        &self,
        _req: Request<ExportRequest>,
    ) -> Result<Response<Self::ExportStream>, Status> {
        Err(not_routed("Export"))
    }

    async fn aggregate(
        &self,
        req: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        self.0.aggregate(req).await
    }

    async fn explain(
        &self,
        req: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        self.0.explain(req).await
    }
}

/// `Suggest` over the Gateway.
#[derive(Clone)]
pub struct GatewaySuggest(Arc<Gateway>);

#[tonic::async_trait]
impl Suggest for GatewaySuggest {
    async fn suggest(
        &self,
        req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        self.0.suggest(req).await
    }
}

/// `Lookup` (hydration) over the Gateway.
#[derive(Clone)]
pub struct GatewayLookup(Arc<Gateway>);

#[tonic::async_trait]
impl Lookup for GatewayLookup {
    async fn get_by_key(
        &self,
        req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        self.0.get_by_key(req).await
    }
}

/// `Admin` over the Gateway: routes `describe_index`; mutations are Node/Control-Plane.
#[derive(Clone)]
pub struct GatewayAdmin(Arc<Gateway>);

#[tonic::async_trait]
impl Admin for GatewayAdmin {
    async fn describe_index(
        &self,
        req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        self.0.describe_index(req).await
    }

    async fn alter_index(
        &self,
        _req: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        Err(not_routed("AlterIndex"))
    }

    async fn reindex_index(
        &self,
        _req: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        Err(not_routed("ReindexIndex"))
    }

    async fn reconcile_index(
        &self,
        _req: Request<ReconcileIndexRequest>,
    ) -> Result<Response<ReconcileIndexResponse>, Status> {
        // Per-shard op: a reconcile is scoped to a shard's owned keys, so it must be
        // driven directly against each node (with that node's ordinal + bucket map), not fanned
        // through the gateway.
        Err(not_routed("ReconcileIndex"))
    }

    async fn compact_index(
        &self,
        _req: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        Err(not_routed("CompactIndex"))
    }

    async fn backup_index(
        &self,
        _req: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        Err(not_routed("BackupIndex"))
    }

    async fn backup_status(
        &self,
        _req: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        Err(not_routed("BackupStatus"))
    }
}
