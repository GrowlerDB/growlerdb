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
use tonic::{Code, Extensions, Request, Response, Status};

use crate::windowed_routing::is_unit_not_served;
use crate::{AdminService, LookupService, SearchService, SuggestService};

/// Time to establish a TCP+HTTP/2 connection to a Node before giving up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Per-request ceiling for a Node RPC — bounds a slow shard at the transport layer, under the
/// Gateway's own scatter deadline. [`FailoverNode`] divides this budget across a unit's holders
/// (see [`failover_read!`]) so a timed-out primary still leaves room for a replica attempt.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
/// HTTP/2 keepalive ping interval on a Node channel — detects a hung/blackholed peer (a dead TCP
/// path that never RSTs) instead of waiting out the request timeout on every read.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(10);
/// How long to wait for a keepalive ping ack before the connection is declared dead.
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(5);

/// The shared [`Endpoint`] shape for a Node channel: connect + per-request timeouts, and HTTP/2
/// keepalive (pinging while idle too) so an established channel to a silently-dead peer fails fast
/// enough for read failover to reach the next holder within the request budget.
fn node_endpoint(endpoint: String) -> Result<Endpoint, tonic::transport::Error> {
    Ok(Endpoint::from_shared(endpoint)?
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .http2_keep_alive_interval(KEEP_ALIVE_INTERVAL)
        .keep_alive_timeout(KEEP_ALIVE_TIMEOUT)
        .keep_alive_while_idle(true))
}

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
        let channel = node_endpoint(endpoint.into())?.connect().await?;
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
        let channel = node_endpoint(endpoint.into())?
            .tls_config(tls)?
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
        let channel = node_endpoint(endpoint.into())?.connect_lazy();
        Ok(Self::with_channel(channel, token))
    }

    /// [`connect_lazy`](Self::connect_lazy) over mutual TLS (cf. [`connect_with_tls`](Self::connect_with_tls)).
    pub fn connect_lazy_with_tls(
        endpoint: impl Into<String>,
        tls: tonic::transport::ClientTlsConfig,
        token: Option<&str>,
    ) -> Result<Self, tonic::transport::Error> {
        let channel = node_endpoint(endpoint.into())?
            .tls_config(tls)?
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
/// can help — rather than a request-level error that would recur on every holder.
/// `Unavailable`/`DeadlineExceeded` are the plain transport set (the connector's transient set).
/// tonic maps its client-side channel timeout to `Cancelled` — retried unconditionally, since these
/// are idempotent reads a spurious retry can't corrupt — and a mid-request transport reset to
/// `Unknown`/`Internal`. Those two are ambiguous (a remote handler returns them for request-level
/// failures too), so they count as down only when **transport-shaped**: a local error `source`
/// (a status decoded off the wire never carries one) or tonic's transport/connection error text.
fn is_holder_down(status: &Status) -> bool {
    match status.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => true,
        Code::Unknown | Code::Internal => {
            std::error::Error::source(status).is_some()
                || status.message().contains("transport error")
                || status.message().contains("connection")
        }
        _ => false,
    }
}

/// Try each holder in order for a **read** RPC, failing over past a holder that is down
/// ([`is_holder_down`]) or that answers "unit not served" ([`is_unit_not_served`] — a stale route or
/// a not-yet-warmed replica; another holder may well serve it), returning the first success.
///
/// The gateway-stamped metadata (verified tenant/principal claims, `grpc-timeout`) is preserved on
/// **every** attempt: the request is split once via `into_parts` and each attempt rebuilt from the
/// same metadata + message ([`Extensions`] aren't `Clone`; nothing on this path carries any).
///
/// Each attempt runs under a slice of the per-request budget — [`REQUEST_TIMEOUT`] divided by the
/// holder count — so a hung primary can't exhaust the Gateway's scatter deadline (which equals
/// [`REQUEST_TIMEOUT`]) before a replica is tried; a single-holder unit keeps the full budget.
///
/// When every holder is exhausted, the last transport error surfaces as-is (an honest
/// `Unavailable`/`DeadlineExceeded`); an all-holders-not-serving run maps to `Unavailable` — the
/// unit is unreachable, and the per-holder "not served" is a routing detail, not the caller's error.
macro_rules! failover_read {
    ($self:expr, $method:ident, $req:expr) => {{
        let (meta, _ext, msg) = $req.into_parts();
        let per_attempt = REQUEST_TIMEOUT / $self.holders.len().max(1) as u32;
        let mut last: Option<Status> = None;
        for holder in &$self.holders {
            let attempt = Request::from_parts(meta.clone(), Extensions::default(), msg.clone());
            match tokio::time::timeout(per_attempt, holder.$method(attempt)).await {
                Ok(Ok(resp)) => return Ok(resp),
                // The holder is down/unreachable — remember the error and try the next holder.
                Ok(Err(status)) if is_holder_down(&status) => last = Some(status),
                // The holder doesn't serve this unit — try the next; if none does, that is
                // unavailability, not a request error.
                Ok(Err(status)) if is_unit_not_served(&status) => {
                    last = Some(Status::unavailable(format!(
                        "no holder serves the unit: {}",
                        status.message()
                    )));
                }
                // A request-level error recurs on every holder — surface it now, don't burn replicas.
                Ok(Err(status)) => return Err(status),
                // The attempt overran its slice of the budget — a hung holder, treated as down.
                Err(_elapsed) => {
                    last = Some(Status::deadline_exceeded(format!(
                        "holder did not answer within the {per_attempt:?} failover slice"
                    )));
                }
            }
        }
        Err(last.unwrap_or_else(|| Status::unavailable("no holders for unit")))
    }};
}

/// A [`Node`] that fronts a unit's **holders** — its primary first, then read replicas (D53) — and
/// serves each read from a live one: it tries the holders in order and, on a **retriable transport
/// error** ([`is_holder_down`]) or a **"unit not served" answer** ([`is_unit_not_served`]), fails
/// over to the next, returning the first success (see [`failover_read!`] for the metadata, budget,
/// and exhaustion rules). So a single node loss is a **zero-gap read failover** instead of the
/// honest-`partial` degradation a single-holder route gives.
///
/// **Reads fail over; mutations don't.** The write-fenced RPCs (reindex / alter / compact / backup)
/// target the sole writer, so they go to the **primary** (the first holder) with no failover — a read
/// replica is read-only and must never accept them.
///
/// **`require_complete` pins to the primary** (D53): a replica trails the primary by its snapshot
/// advance, so a caller that opted out of any degradation gets the sole writer's answer or an
/// honest error — never a possibly-stale replica answer dressed as complete.
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
        // `require_complete` pins to the primary: no replica failover, zero read-your-writes lag.
        if req.get_ref().require_complete {
            return self.holders[0].search(req).await;
        }
        failover_read!(self, search, req)
    }

    async fn semantic_search(
        &self,
        req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        if req.get_ref().require_complete {
            return self.holders[0].semantic_search(req).await;
        }
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
    use std::sync::Mutex;

    /// How a [`MockNode`] answers each call.
    enum Mode {
        /// Succeed with a default response.
        Up,
        /// Return `Status::new(code, "mock")` — a plain status with no source and no details.
        Err(Code),
        /// Return the responder-shaped "unit not served" status (structured detail attached).
        NotServed,
        /// Return `Unknown` with tonic's transport-error message shape (a mid-request reset).
        TransportUnknown,
        /// Never answer — stands in for a hung/blackholed holder.
        Hang,
    }

    /// A stub Node that records how many times it was called (and the last `x-growlerdb-tenant`
    /// metadata it saw) and answers by [`Mode`]. Overrides `search` (a read) + `reindex_index`
    /// (a mutation).
    struct MockNode {
        mode: Mode,
        calls: Arc<AtomicUsize>,
        tenant_seen: Mutex<Option<String>>,
    }

    impl MockNode {
        fn with_mode(mode: Mode, calls: Arc<AtomicUsize>) -> Arc<Self> {
            Arc::new(Self {
                mode,
                calls,
                tenant_seen: Mutex::new(None),
            })
        }
        fn up(calls: Arc<AtomicUsize>) -> Arc<Self> {
            Self::with_mode(Mode::Up, calls)
        }
        fn erroring(code: Code, calls: Arc<AtomicUsize>) -> Arc<Self> {
            Self::with_mode(Mode::Err(code), calls)
        }
        fn tenant_seen(&self) -> Option<String> {
            self.tenant_seen.lock().unwrap().clone()
        }
        async fn answer<T: Default>(&self) -> Result<Response<T>, Status> {
            match self.mode {
                Mode::Up => Ok(Response::new(T::default())),
                Mode::Err(code) => Err(Status::new(code, "mock")),
                Mode::NotServed => Err(crate::windowed_routing::unit_not_served(
                    "window 7 is not served by this node",
                )),
                Mode::TransportUnknown => Err(Status::new(Code::Unknown, "transport error")),
                Mode::Hang => {
                    tokio::time::sleep(Duration::from_secs(3600)).await;
                    Err(Status::internal("unreachable"))
                }
            }
        }
    }

    #[tonic::async_trait]
    impl Node for MockNode {
        async fn search(
            &self,
            req: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.tenant_seen.lock().unwrap() = req
                .metadata()
                .get("x-growlerdb-tenant")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            self.answer().await
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
            self.answer().await
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

    /// The failover rebuild must carry the gateway-stamped metadata to EVERY attempt — the node's
    /// tenant scoping is fail-closed, so dropping `x-growlerdb-tenant` on the replica attempt would
    /// turn every failover on a tenant-scoped index into PermissionDenied.
    #[tokio::test]
    async fn failover_preserves_request_metadata_on_the_replica_attempt() {
        let (p, r) = (count(), count());
        let replica = MockNode::up(r);
        let fo = FailoverNode::new(
            MockNode::erroring(Code::Unavailable, p),
            vec![replica.clone()],
        );
        let mut req = Request::new(SearchRequest::default());
        req.metadata_mut()
            .insert("x-growlerdb-tenant", "acme".parse().unwrap());
        Node::search(&fo, req).await.unwrap();
        assert_eq!(
            replica.tenant_seen().as_deref(),
            Some("acme"),
            "the replica attempt carries the verified tenant claim"
        );
    }

    /// tonic surfaces its client-side channel timeout as `Cancelled` and a mid-request transport
    /// reset as `Unknown` with a transport-shaped message — both mean "this holder is down", so
    /// both must fail over instead of aborting the read.
    #[tokio::test]
    async fn cancelled_and_transport_shaped_errors_fail_over() {
        for mode in [Mode::Err(Code::Cancelled), Mode::TransportUnknown] {
            let (p, r) = (count(), count());
            let fo = FailoverNode::new(MockNode::with_mode(mode, p), vec![MockNode::up(r.clone())]);
            Node::search(&fo, Request::new(SearchRequest::default()))
                .await
                .expect("a transport-shaped error fails over to the replica");
            assert_eq!(r.load(Ordering::SeqCst), 1, "replica answered");
        }
    }

    /// A plain (non-transport-shaped) `Unknown` is a remote handler's own error — request-level,
    /// never retried on a replica.
    #[tokio::test]
    async fn a_plain_unknown_is_not_retried_on_replicas() {
        let (p, r) = (count(), count());
        let fo = FailoverNode::new(
            MockNode::erroring(Code::Unknown, p),
            vec![MockNode::up(r.clone())],
        );
        let err = Node::search(&fo, Request::new(SearchRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unknown);
        assert_eq!(r.load(Ordering::SeqCst), 0, "the replica isn't burned");
    }

    /// A holder answering "unit not served" (stale route / not-yet-warmed replica) is skipped to the
    /// next holder; when NO holder serves the unit the caller sees `Unavailable`, not the routing
    /// detail.
    #[tokio::test]
    async fn a_not_served_holder_is_skipped_and_exhaustion_is_unavailable() {
        let (p, r) = (count(), count());
        let fo = FailoverNode::new(
            MockNode::with_mode(Mode::NotServed, p.clone()),
            vec![MockNode::up(r.clone())],
        );
        Node::search(&fo, Request::new(SearchRequest::default()))
            .await
            .expect("a not-serving holder fails over to the replica");
        assert_eq!(p.load(Ordering::SeqCst), 1);
        assert_eq!(r.load(Ordering::SeqCst), 1);

        let fo = FailoverNode::new(
            MockNode::with_mode(Mode::NotServed, count()),
            vec![MockNode::with_mode(Mode::NotServed, count())],
        );
        let err = Node::search(&fo, Request::new(SearchRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            Code::Unavailable,
            "exhausting not-serving holders is unavailability, not FailedPrecondition"
        );
    }

    /// `require_complete` pins the read to the primary (D53): a replica may trail the sole writer,
    /// so a down primary is an honest error — the replica is never consulted.
    #[tokio::test]
    async fn require_complete_pins_search_to_the_primary() {
        let (p, r) = (count(), count());
        let fo = FailoverNode::new(
            MockNode::erroring(Code::Unavailable, p.clone()),
            vec![MockNode::up(r.clone())],
        );
        let err = Node::search(
            &fo,
            Request::new(SearchRequest {
                require_complete: true,
                ..Default::default()
            }),
        )
        .await
        .expect_err("a down primary refuses under require_complete");
        assert_eq!(err.code(), Code::Unavailable);
        assert_eq!(p.load(Ordering::SeqCst), 1, "the primary was tried");
        assert_eq!(
            r.load(Ordering::SeqCst),
            0,
            "a pinned read never falls to a replica"
        );
    }

    /// A hung primary is abandoned at its slice of the request budget (REQUEST_TIMEOUT / holders),
    /// leaving the replica attempt inside the Gateway's scatter deadline. Paused-clock test: the
    /// timeout fires virtually, so this asserts the real budget split without waiting 15 s.
    #[tokio::test(start_paused = true)]
    async fn a_hung_primary_leaves_budget_for_the_replica() {
        let (p, r) = (count(), count());
        let fo = FailoverNode::new(
            MockNode::with_mode(Mode::Hang, p),
            vec![MockNode::up(r.clone())],
        );
        let started = tokio::time::Instant::now();
        Node::search(&fo, Request::new(SearchRequest::default()))
            .await
            .expect("the replica answers after the hung primary's slice");
        assert_eq!(
            started.elapsed(),
            REQUEST_TIMEOUT / 2,
            "two holders split the request budget evenly"
        );
        assert_eq!(r.load(Ordering::SeqCst), 1, "replica answered");
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
