//! The **Node client seam**: the [`Node`] trait is the Gateway's
//! view of one Node's query/admin surface, and [`LocalNode`] is the in-process
//! implementation that delegates straight to this process's services (embedded mode);
//! [`RemoteNode`] implements the same trait over a gRPC channel for distributed mode, so the
//! [Gateway](crate::gateway::Gateway) routes through `dyn Node` without caring whether the Node is
//! in-process or across the network.
//!
//! Scope is the surface the Engine API terminates — search, suggest, hydrate
//! (`get_by_key`), and `describe_index`. Writes go connector → Node `Write` gRPC directly
//! (not through the Gateway).

use std::sync::Arc;
use std::time::Duration;

use growlerdb_proto::v1::admin_client::AdminClient;
use growlerdb_proto::v1::lookup_client::LookupClient;
use growlerdb_proto::v1::search_client::SearchClient;
use growlerdb_proto::v1::suggest_client::SuggestClient;
use growlerdb_proto::v1::{
    AggregateRequest, AggregateResponse, AlterIndexRequest, AlterIndexResponse, BackupIndexRequest,
    BackupIndexResponse, BackupStatusRequest, BackupStatusResponse, CompactIndexRequest,
    CompactIndexResponse, DescribeIndexRequest, DescribeIndexResponse, ExplainRequest,
    ExplainResponse, GetByKeyRequest, GetByKeyResponse, ReindexIndexRequest, ReindexIndexResponse,
    SearchRequest, SearchResponse, SemanticSearchRequest, SuggestRequest, SuggestResponse,
};
use growlerdb_proto::{Admin, Lookup, Search, Suggest};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status};

use crate::{AdminService, LookupService, SearchService, SuggestService};

/// Time to establish a TCP+HTTP/2 connection to a Node before giving up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Per-request ceiling for a Node RPC — bounds a slow shard at the transport layer, under the
/// Gateway's own scatter deadline.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// A Node's query/admin surface as the [Gateway](crate::gateway::Gateway) sees it:
/// transport-agnostic RPCs (proto bodies in a tonic [`Request`] so auth metadata flows
/// through unchanged). [`LocalNode`] implements it in-process; [`RemoteNode`] implements it over
/// gRPC. Each call targets one Node — one shard; cross-shard scatter-gather lands in the Gateway.
#[tonic::async_trait]
pub trait Node: Send + Sync {
    /// Run a search against the Node's shard.
    async fn search(&self, req: Request<SearchRequest>)
        -> Result<Response<SearchResponse>, Status>;

    /// Semantic (KNN) search against the Node's shard: the Node embeds the request's `query_text`
    /// with the vector field's embedder and returns the nearest documents' coordinates + KNN
    /// scores. Defaults to `Unimplemented` so simple Node impls (and test stubs / windowed nodes
    /// that don't yet serve vector search) need not provide it; [`LocalNode`]/[`RemoteNode`]
    /// override it.
    async fn semantic_search(
        &self,
        _req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Err(Status::unimplemented("semantic_search"))
    }
    /// Term suggestions (autocomplete / did-you-mean).
    async fn suggest(
        &self,
        req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status>;
    /// Hydrate keys back to source rows (PK lookup).
    async fn get_by_key(
        &self,
        req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status>;
    /// Index stats/status.
    async fn describe_index(
        &self,
        req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status>;

    /// Aggregate over the docs a query matches. Defaults to `Unimplemented` so simple Node
    /// impls (and test stubs) need not provide it; [`LocalNode`]/`RemoteNode` override it.
    async fn aggregate(
        &self,
        _req: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        Err(Status::unimplemented("aggregate"))
    }

    /// Explain how a query scores one document. Defaults to `Unimplemented` so test
    /// stubs need not provide it; [`LocalNode`]/[`RemoteNode`] override it.
    async fn explain(
        &self,
        _req: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        Err(Status::unimplemented("explain"))
    }

    /// Rebuild this Node's index from source and durably swap it live. A write-fenced
    /// **mutation** — unlike the read RPCs the Gateway scatters, this targets the single owning
    /// Node. Defaults to `Unimplemented` so test stubs need not provide it; [`LocalNode`] and
    /// [`RemoteNode`] override it.
    async fn reindex_index(
        &self,
        _req: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        Err(Status::unimplemented("reindex_index"))
    }

    /// Plan (and optionally apply in-place) an index-definition change against the owning Node:
    /// diff a candidate definition vs the served one — in-place metadata changes vs
    /// changes that force a reindex — and, with `apply`, persist the in-place ones live. A
    /// write-targeted **mutation** like reindex. Defaults to `Unimplemented`; [`LocalNode`] and
    /// [`RemoteNode`] override it.
    async fn alter_index(
        &self,
        _req: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        Err(Status::unimplemented("alter_index"))
    }

    /// Compact the owning Node's segments. Defaults to `Unimplemented`.
    async fn compact_index(
        &self,
        _req: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        Err(Status::unimplemented("compact_index"))
    }

    /// Back up the owning Node's shard. Defaults to `Unimplemented`.
    async fn backup_index(
        &self,
        _req: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        Err(Status::unimplemented("backup_index"))
    }

    /// Read the owning Node's backup status. Defaults to `Unimplemented`.
    async fn backup_status(
        &self,
        _req: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        Err(Status::unimplemented("backup_status"))
    }
}

/// The **in-process** Node (embedded mode): delegates straight to this process's services
/// over the shared [`Arc<Shard>`](growlerdb_index::Shard) — no network hop. Those services
/// keep mounting on the Node's own gRPC server too; `LocalNode` just hands the Gateway a
/// `dyn Node` view of the same instances, so embedded mode collapses Gateway + Node into
/// one process with zero serialization between them.
#[derive(Clone)]
pub struct LocalNode {
    search: SearchService,
    suggest: SuggestService,
    lookup: LookupService,
    admin: AdminService,
}

impl LocalNode {
    /// Build an in-process Node over this process's services (they share the shard).
    pub fn new(
        search: SearchService,
        suggest: SuggestService,
        lookup: LookupService,
        admin: AdminService,
    ) -> Self {
        Self {
            search,
            suggest,
            lookup,
            admin,
        }
    }

    /// Erase to a shared `dyn Node` for the [Gateway](crate::gateway::Gateway).
    pub fn shared(self) -> Arc<dyn Node> {
        Arc::new(self)
    }
}

#[tonic::async_trait]
impl Node for LocalNode {
    async fn search(
        &self,
        req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Search::search(&self.search, req).await
    }

    async fn semantic_search(
        &self,
        req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Search::semantic_search(&self.search, req).await
    }

    async fn suggest(
        &self,
        req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        Suggest::suggest(&self.suggest, req).await
    }

    async fn get_by_key(
        &self,
        req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        Lookup::get_by_key(&self.lookup, req).await
    }

    async fn describe_index(
        &self,
        req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        Admin::describe_index(&self.admin, req).await
    }

    async fn aggregate(
        &self,
        req: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        Search::aggregate(&self.search, req).await
    }

    async fn explain(
        &self,
        req: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        Search::explain(&self.search, req).await
    }

    async fn reindex_index(
        &self,
        req: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        Admin::reindex_index(&self.admin, req).await
    }

    async fn alter_index(
        &self,
        req: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        Admin::alter_index(&self.admin, req).await
    }

    async fn compact_index(
        &self,
        req: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        Admin::compact_index(&self.admin, req).await
    }

    async fn backup_index(
        &self,
        req: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        Admin::backup_index(&self.admin, req).await
    }

    async fn backup_status(
        &self,
        req: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        Admin::backup_status(&self.admin, req).await
    }
}

/// A **remote** Node (distributed mode): implements [`Node`] over a gRPC channel to a Node
/// server's Search/Suggest/Lookup/Admin services. The four generated clients multiplex one
/// HTTP/2 [`Channel`], and each call forwards the tonic [`Request`] verbatim — so the auth
/// metadata set by the Engine API travels over the wire to the Node's [auth seam](crate::auth).
/// This is the half of the seam that makes the [Gateway](crate::gateway::Gateway) work
/// across a real network hop; embedded mode uses [`LocalNode`] instead.
#[derive(Clone)]
pub struct RemoteNode {
    search: SearchClient<Channel>,
    suggest: SuggestClient<Channel>,
    lookup: LookupClient<Channel>,
    admin: AdminClient<Channel>,
}

impl RemoteNode {
    /// Connect to a Node's gRPC endpoint (e.g. `"http://127.0.0.1:50051"`). Sets a connect and a
    /// per-request timeout so a hung/slow shard surfaces as a call error (counted as a failed
    /// shard → `partial`) rather than blocking forever, complementing the Gateway's scatter
    /// deadline.
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect()
            .await?;
        Ok(Self::with_channel(channel))
    }

    /// Connect over **mutual TLS** ([`tls`](crate::tls)): like [`connect`](Self::connect), but
    /// the channel presents this service's client identity and verifies the Node's server cert
    /// against the configured CA/domain. The internal-trust transport for a distributed cluster.
    pub async fn connect_with_tls(
        endpoint: impl Into<String>,
        tls: tonic::transport::ClientTlsConfig,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?
            .tls_config(tls)?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect()
            .await?;
        Ok(Self::with_channel(channel))
    }

    /// Like [`connect`](Self::connect) but **lazy**: build the channel without establishing the
    /// connection now. The connection opens on first use and — crucially for resilience —
    /// **re-resolves DNS on every (re)connect attempt**, so a shard whose pod crashed and came back
    /// at a *new* IP is reached again automatically, and a still-down shard fails fast at
    /// [`CONNECT_TIMEOUT`] (→ a `partial` query) instead of blocking on a stale connection. Building
    /// never fails on an unreachable node, so a Gateway can front a partially-down cluster.
    pub fn connect_lazy(endpoint: impl Into<String>) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect_lazy();
        Ok(Self::with_channel(channel))
    }

    /// [`connect_lazy`](Self::connect_lazy) over mutual TLS (cf. [`connect_with_tls`](Self::connect_with_tls)).
    pub fn connect_lazy_with_tls(
        endpoint: impl Into<String>,
        tls: tonic::transport::ClientTlsConfig,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?
            .tls_config(tls)?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect_lazy();
        Ok(Self::with_channel(channel))
    }

    /// Build over an existing channel — all four clients share the one connection.
    pub fn with_channel(channel: Channel) -> Self {
        Self {
            search: SearchClient::new(channel.clone()),
            suggest: SuggestClient::new(channel.clone()),
            lookup: LookupClient::new(channel.clone()),
            admin: AdminClient::new(channel),
        }
    }
}

#[tonic::async_trait]
impl Node for RemoteNode {
    async fn search(
        &self,
        req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        // tonic clients take `&mut self`; cloning is cheap (it shares the channel).
        self.search.clone().search(req).await
    }

    async fn semantic_search(
        &self,
        req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        self.search.clone().semantic_search(req).await
    }

    async fn suggest(
        &self,
        req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        self.suggest.clone().suggest(req).await
    }

    async fn get_by_key(
        &self,
        req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        self.lookup.clone().get_by_key(req).await
    }

    async fn describe_index(
        &self,
        req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        self.admin.clone().describe_index(req).await
    }

    async fn aggregate(
        &self,
        req: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        self.search.clone().aggregate(req).await
    }

    async fn explain(
        &self,
        req: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        self.search.clone().explain(req).await
    }

    async fn compact_index(
        &self,
        req: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        self.admin.clone().compact_index(req).await
    }

    async fn backup_index(
        &self,
        req: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        self.admin.clone().backup_index(req).await
    }

    async fn backup_status(
        &self,
        req: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        self.admin.clone().backup_status(req).await
    }

    async fn reindex_index(
        &self,
        req: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        self.admin.clone().reindex_index(req).await
    }

    async fn alter_index(
        &self,
        req: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        self.admin.clone().alter_index(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Lazy connect must **build without dialing** — so a Gateway can front a shard whose
    /// node is currently down (the build doesn't fail), and the channel reconnects/re-resolves later.
    #[tokio::test]
    async fn connect_lazy_builds_for_an_unreachable_endpoint() {
        // 198.51.100.0/24 (TEST-NET-2) is non-routable; an eager connect would fail, lazy must not.
        assert!(RemoteNode::connect_lazy("http://198.51.100.1:50051").is_ok());
    }

    /// A malformed endpoint is still a build error (not silently accepted).
    #[test]
    fn connect_lazy_rejects_a_bad_endpoint() {
        assert!(RemoteNode::connect_lazy("not a url").is_err());
    }
}
