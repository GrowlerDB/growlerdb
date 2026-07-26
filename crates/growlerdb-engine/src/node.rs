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
use tonic::{Code, Request, Response, Status};

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
/// A Node channel with the cluster's shared service token stamped on every request (a no-op
/// stamp when no token is configured, so the same type serves open single-node dev). The
/// server-side counterpart is [`service_token_layer`](crate::service_token_layer) on the Node.
type NodeChannel = tonic::service::interceptor::InterceptedService<
    Channel,
    growlerdb_proto::service_token::ServiceTokenInterceptor,
>;

#[derive(Clone)]
pub struct RemoteNode {
    search: SearchClient<NodeChannel>,
    suggest: SuggestClient<NodeChannel>,
    lookup: LookupClient<NodeChannel>,
    admin: AdminClient<NodeChannel>,
}

impl RemoteNode {
    /// Connect to a Node's gRPC endpoint (e.g. `"http://127.0.0.1:50051"`). Sets a connect and a
    /// per-request timeout so a hung/slow shard surfaces as a call error (counted as a failed
    /// shard → `partial`) rather than blocking forever, complementing the Gateway's scatter
    /// deadline.
    pub async fn connect(
        endpoint: impl Into<String>,
        token: Option<&str>,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect()
            .await?;
        Ok(Self::with_channel(channel, token))
    }

    /// Connect over **mutual TLS** ([`tls`](crate::tls)): like [`connect`](Self::connect), but
    /// the channel presents this service's client identity and verifies the Node's server cert
    /// against the configured CA/domain. The internal-trust transport for a distributed cluster.
    pub async fn connect_with_tls(
        endpoint: impl Into<String>,
        tls: tonic::transport::ClientTlsConfig,
        token: Option<&str>,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?
            .tls_config(tls)?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect()
            .await?;
        Ok(Self::with_channel(channel, token))
    }

    /// Like [`connect`](Self::connect) but **lazy**: build the channel without establishing the
    /// connection now. The connection opens on first use and — crucially for resilience —
    /// **re-resolves DNS on every (re)connect attempt**, so a shard whose pod crashed and came back
    /// at a *new* IP is reached again automatically, and a still-down shard fails fast at
    /// [`CONNECT_TIMEOUT`] (→ a `partial` query) instead of blocking on a stale connection. Building
    /// never fails on an unreachable node, so a Gateway can front a partially-down cluster.
    pub fn connect_lazy(
        endpoint: impl Into<String>,
        token: Option<&str>,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect_lazy();
        Ok(Self::with_channel(channel, token))
    }

    /// [`connect_lazy`](Self::connect_lazy) over mutual TLS (cf. [`connect_with_tls`](Self::connect_with_tls)).
    pub fn connect_lazy_with_tls(
        endpoint: impl Into<String>,
        tls: tonic::transport::ClientTlsConfig,
        token: Option<&str>,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = Endpoint::from_shared(endpoint.into())?
            .tls_config(tls)?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(REQUEST_TIMEOUT)
            .connect_lazy();
        Ok(Self::with_channel(channel, token))
    }

    /// Build over an existing channel — all four clients share the one connection, each
    /// stamping the cluster service `token` on every request (`None` ⇒ no stamp, open dev).
    pub fn with_channel(channel: Channel, token: Option<&str>) -> Self {
        let stamp = growlerdb_proto::service_token::ServiceTokenInterceptor::new(token);
        Self {
            search: SearchClient::with_interceptor(channel.clone(), stamp.clone()),
            suggest: SuggestClient::with_interceptor(channel.clone(), stamp.clone()),
            lookup: LookupClient::with_interceptor(channel.clone(), stamp.clone()),
            admin: AdminClient::with_interceptor(channel, stamp),
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

/// Whether a Node error means "this holder is down/unreachable" — so failing over to another holder
/// can help — rather than a request-level error that would recur on every holder. Matches the
/// connector's transient-transport set (`Unavailable`/`DeadlineExceeded`).
fn is_holder_down(status: &Status) -> bool {
    matches!(status.code(), Code::Unavailable | Code::DeadlineExceeded)
}

/// Try each holder in order for a **read** RPC, failing over past a down holder to the next and
/// returning the first success (or the last transport error when every holder is down). The request
/// body is cloned per attempt (proto messages are `Clone`).
macro_rules! failover_read {
    ($self:expr, $method:ident, $req:expr) => {{
        let msg = $req.into_inner();
        let mut last: Option<Status> = None;
        for holder in &$self.holders {
            match holder.$method(Request::new(msg.clone())).await {
                Ok(resp) => return Ok(resp),
                // The holder is down/unreachable — remember the error and try the next holder.
                Err(status) if is_holder_down(&status) => last = Some(status),
                // A request-level error recurs on every holder — surface it now, don't burn replicas.
                Err(status) => return Err(status),
            }
        }
        Err(last.unwrap_or_else(|| Status::unavailable("no holders for unit")))
    }};
}

/// A [`Node`] that fronts a unit's **holders** — its primary first, then read replicas (D53) — and
/// serves each read from a live one: it tries the holders in order and, on a **retriable transport
/// error** ([`is_holder_down`] — the holder is down or unreachable), fails over to the next, returning
/// the first success (or the last error when every holder is down). So a single node loss is a
/// **zero-gap read failover** instead of the honest-`partial` degradation a single-holder route gives.
///
/// **Reads fail over; mutations don't.** The write-fenced RPCs (reindex / alter / compact / backup)
/// target the sole writer, so they go to the **primary** (the first holder) with no failover — a read
/// replica is read-only and must never accept them.
pub struct FailoverNode {
    /// Primary first, then replicas. Always non-empty (a primary is required).
    holders: Vec<Arc<dyn Node>>,
}

impl FailoverNode {
    /// Front `primary` (the writer + preferred read target) plus zero or more read `replicas`.
    pub fn new(primary: Arc<dyn Node>, replicas: Vec<Arc<dyn Node>>) -> Self {
        let mut holders = Vec::with_capacity(1 + replicas.len());
        holders.push(primary);
        holders.extend(replicas);
        Self { holders }
    }

    /// Erase to a shared `dyn Node` for the [Gateway](crate::gateway::Gateway).
    pub fn shared(self) -> Arc<dyn Node> {
        Arc::new(self)
    }
}

#[tonic::async_trait]
impl Node for FailoverNode {
    async fn search(
        &self,
        req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        failover_read!(self, search, req)
    }

    async fn semantic_search(
        &self,
        req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        failover_read!(self, semantic_search, req)
    }

    async fn suggest(
        &self,
        req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        failover_read!(self, suggest, req)
    }

    async fn get_by_key(
        &self,
        req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        failover_read!(self, get_by_key, req)
    }

    async fn describe_index(
        &self,
        req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        failover_read!(self, describe_index, req)
    }

    async fn aggregate(
        &self,
        req: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        failover_read!(self, aggregate, req)
    }

    async fn explain(
        &self,
        req: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        failover_read!(self, explain, req)
    }

    // Mutations target the sole writer → the primary, never a read replica. No failover.
    async fn reindex_index(
        &self,
        req: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        self.holders[0].reindex_index(req).await
    }

    async fn alter_index(
        &self,
        req: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        self.holders[0].alter_index(req).await
    }

    async fn compact_index(
        &self,
        req: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        self.holders[0].compact_index(req).await
    }

    async fn backup_index(
        &self,
        req: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        self.holders[0].backup_index(req).await
    }

    async fn backup_status(
        &self,
        req: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        self.holders[0].backup_status(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A stub Node that records how many times it was called and answers by `mode`: `Ok` succeeds,
    /// `Err(code)` returns that status. Overrides `search` (a read) + `reindex_index` (a mutation).
    struct MockNode {
        mode: Result<(), Code>,
        calls: Arc<AtomicUsize>,
    }

    impl MockNode {
        fn up(calls: Arc<AtomicUsize>) -> Arc<Self> {
            Arc::new(Self {
                mode: Ok(()),
                calls,
            })
        }
        fn erroring(code: Code, calls: Arc<AtomicUsize>) -> Arc<Self> {
            Arc::new(Self {
                mode: Err(code),
                calls,
            })
        }
    }

    #[tonic::async_trait]
    impl Node for MockNode {
        async fn search(
            &self,
            _req: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                Ok(()) => Ok(Response::new(SearchResponse::default())),
                Err(code) => Err(Status::new(code, "mock")),
            }
        }
        async fn suggest(
            &self,
            _req: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _req: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _req: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("describe_index"))
        }
        async fn reindex_index(
            &self,
            _req: Request<ReindexIndexRequest>,
        ) -> Result<Response<ReindexIndexResponse>, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            match self.mode {
                Ok(()) => Ok(Response::new(ReindexIndexResponse::default())),
                Err(code) => Err(Status::new(code, "mock")),
            }
        }
    }

    fn count() -> Arc<AtomicUsize> {
        Arc::new(AtomicUsize::new(0))
    }

    #[tokio::test]
    async fn failover_skips_a_down_primary_to_a_replica() {
        let (p, r) = (count(), count());
        let fo = FailoverNode::new(
            MockNode::erroring(Code::Unavailable, p.clone()),
            vec![MockNode::up(r.clone())],
        );
        Node::search(&fo, Request::new(SearchRequest::default()))
            .await
            .expect("a down primary fails over to the replica");
        assert_eq!(p.load(Ordering::SeqCst), 1, "primary was tried first");
        assert_eq!(r.load(Ordering::SeqCst), 1, "replica answered");
    }

    #[tokio::test]
    async fn failover_prefers_the_primary_when_up() {
        let (p, r) = (count(), count());
        let fo = FailoverNode::new(MockNode::up(p.clone()), vec![MockNode::up(r.clone())]);
        Node::search(&fo, Request::new(SearchRequest::default()))
            .await
            .unwrap();
        assert_eq!(p.load(Ordering::SeqCst), 1);
        assert_eq!(
            r.load(Ordering::SeqCst),
            0,
            "the primary answered; the replica isn't queried"
        );
    }

    #[tokio::test]
    async fn failover_errors_when_every_holder_is_down() {
        let (p, r) = (count(), count());
        let fo = FailoverNode::new(
            MockNode::erroring(Code::Unavailable, p),
            vec![MockNode::erroring(Code::DeadlineExceeded, r)],
        );
        let err = Node::search(&fo, Request::new(SearchRequest::default()))
            .await
            .unwrap_err();
        // The last holder's transport error surfaces — the unit is genuinely unavailable.
        assert_eq!(err.code(), Code::DeadlineExceeded);
    }

    #[tokio::test]
    async fn a_request_error_is_not_retried_on_replicas() {
        let (p, r) = (count(), count());
        let fo = FailoverNode::new(
            MockNode::erroring(Code::InvalidArgument, p.clone()),
            vec![MockNode::up(r.clone())],
        );
        let err = Node::search(&fo, Request::new(SearchRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::InvalidArgument,
            "a request error surfaces as-is"
        );
        assert_eq!(
            r.load(Ordering::SeqCst),
            0,
            "the replica isn't burned on a request error"
        );
    }

    #[tokio::test]
    async fn mutations_pin_to_the_primary_and_do_not_fail_over() {
        // A reindex targets the writer: it goes to the primary and, even when the primary errors,
        // never falls through to a read replica.
        let (p, r) = (count(), count());
        let fo = FailoverNode::new(
            MockNode::erroring(Code::Unavailable, p.clone()),
            vec![MockNode::up(r.clone())],
        );
        let err = Node::reindex_index(&fo, Request::new(ReindexIndexRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unavailable);
        assert_eq!(p.load(Ordering::SeqCst), 1, "the mutation hit the primary");
        assert_eq!(
            r.load(Ordering::SeqCst),
            0,
            "a mutation never touches a replica"
        );
    }

    /// Lazy connect must **build without dialing** — so a Gateway can front a shard whose
    /// node is currently down (the build doesn't fail), and the channel reconnects/re-resolves later.
    #[tokio::test]
    async fn connect_lazy_builds_for_an_unreachable_endpoint() {
        // 198.51.100.0/24 (TEST-NET-2) is non-routable; an eager connect would fail, lazy must not.
        assert!(RemoteNode::connect_lazy("http://198.51.100.1:50051", None).is_ok());
    }

    /// A malformed endpoint is still a build error (not silently accepted).
    #[test]
    fn connect_lazy_rejects_a_bad_endpoint() {
        assert!(RemoteNode::connect_lazy("not a url", None).is_err());
    }
}
