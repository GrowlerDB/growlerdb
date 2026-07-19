//! The **Gateway** terminates the Engine API
//! ([REST](crate::rest) + [gRPC](crate::gateway_grpc)) and routes each call to the Nodes
//! through the [`Node`](crate::node::Node) seam. A Node is a
//! [`LocalNode`](crate::node::LocalNode) (embedded) or a gRPC client (distributed) — the
//! surface is identical either way.
//!
//! A Gateway fronts one Node per shard. With a single shard it forwards verbatim; with many
//! it **scatter-gathers** the query to every shard concurrently and **merges** the results:
//! search by score (top-k), suggest by count, key lookups routed by key, describe by summed
//! stats, and **aggregations** by merging each shard's mergeable partial (terms/stats/range/
//! date_histogram exact; HLL/DDSketch approximate but correctly merged). A shard that fails to
//! respond is a **flagged gap, never a silent one**: `search` sets `SearchResponse.partial`, and
//! `suggest`/`get_by_key`/`describe_index`/`aggregate` set a `failed_shards` count on their
//! responses; if **every** shard fails the call returns `UNAVAILABLE` rather than a
//! success-shaped empty result.
//!
//! **Paging is merged correctly across shards**: `offset` via offset-merge (each shard
//! returns rank 0 .. `offset+limit`, the Gateway applies the global window once), and
//! `search_after` keyset scrolling via a **global** cursor — the Gateway sends the same cursor to
//! every shard and composes the next one from the last returned hit's (sort tuple, key), so a
//! full scroll visits every doc exactly once. Keyset paging needs a sort (scores aren't a stable
//! keyset). **`collapse`** is folded across shards: each shard collapses locally and the Gateway
//! merges groups by value, summing `group_count` and keeping each group's global top hit. A
//! single-shard Gateway forwards verbatim and is unaffected by any of this.
//!
//! **Resiliency** ([`GatewayLimits`]): each scatter-gather runs under a **deadline** — a
//! hung/slow shard is aborted and flagged `partial` rather than stalling the query forever (Nodes
//! also carry transport connect/request timeouts) — and a search's **`offset + limit`** is capped
//! at the boundary so an unbounded `limit` can't OOM the Gateway.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use growlerdb_core::{
    cmp_sort_value, Agg, CompositeKey, Query, SearchAfter, ShardRouter, SortOrder, SortValue,
    TimeWindowing, Value, SCORE_SORT_KEY,
};
use growlerdb_proto::v1::{
    AggregateRequest, AggregateResponse, AlterIndexRequest, AlterIndexResponse, BackupIndexRequest,
    BackupIndexResponse, BackupStatusRequest, BackupStatusResponse, CompactIndexRequest,
    CompactIndexResponse, Coordinates, DescribeIndexRequest, DescribeIndexResponse, ExplainRequest,
    ExplainResponse, GetByKeyRequest, GetByKeyResponse, HybridSearchRequest, IndexStats,
    ReindexIndexRequest, ReindexIndexResponse, SearchHit, SearchRequest, SearchResponse,
    SemanticSearchRequest, SuggestRequest, SuggestResponse, Suggestion,
};
use tonic::{Extensions, Request, Response, Status};

use crate::auth::SharedAuth;
use crate::authn::SharedAuthn;
use crate::node::Node;

/// Resiliency limits for a [`Gateway`]'s scatter-gather: a per-query **deadline**
/// (a slow shard can't stall a query forever) and a **max page fetch** (`offset + limit`, so a
/// huge `limit` can't make every shard build a giant page and OOM the Gateway).
#[derive(Debug, Clone, Copy)]
pub struct GatewayLimits {
    /// Wall-clock budget for a scatter-gather; on expiry the Gateway aborts the outstanding
    /// shard tasks, flags `partial`, and returns what arrived. `None` = wait indefinitely.
    pub deadline: Option<Duration>,
    /// Max `offset + limit` a search may request. Oversized → `InvalidArgument`. `0` = unbounded.
    pub max_fetch: usize,
    /// Max concurrent per-shard RPCs in flight across all scatter-gathers. At hundreds of
    /// shards an unbounded fan-out would open hundreds of simultaneous connections per burst of
    /// queries and exhaust the Gateway's socket/fd budget; a semaphore caps it (excess tasks queue
    /// under the deadline). `0` = unbounded.
    pub max_concurrent_fanout: usize,
}

impl Default for GatewayLimits {
    fn default() -> Self {
        // A 30s budget and a 10k page ceiling — generous for real queries, a firm wall on a
        // hung shard or a `limit=1_000_000` DoS. 256 concurrent shard RPCs bounds fan-out.
        Self {
            deadline: Some(Duration::from_secs(30)),
            max_fetch: 10_000,
            max_concurrent_fanout: 256,
        }
    }
}

/// Build the fan-out semaphore for `max_concurrent_fanout`; `0` = effectively unbounded.
fn fanout_semaphore(max: usize) -> Arc<tokio::sync::Semaphore> {
    Arc::new(tokio::sync::Semaphore::new(if max == 0 {
        tokio::sync::Semaphore::MAX_PERMITS
    } else {
        max
    }))
}

/// Routes Engine-API calls to one [`Node`] per shard. Holds `dyn Node`s, so routing is
/// identical whether the Nodes are in-process (embedded) or remote (distributed). A
/// [`ShardRouter`] decides which shard owns a key (for [`get_by_key`](Self::get_by_key)).
///
/// **Multi-index**: a Gateway can front *many* indexes at once. Each named index has its
/// own [`IndexRoute`] (its shard-set + router + partition fields, each independently hot-reloadable),
/// stored in `routes` keyed by resolved index name. A request names its target index; the Gateway
/// [`resolve_route`](Self::resolve_route)s it at entry and operates on that route. Built via the
/// single-index constructors ([`new`](Self::new)/[`sharded`](Self::sharded)/…) the Gateway holds one
/// static route (`single`); built via
/// [`multi_index`](Self::multi_index) with a [`RouteResolver`] it lazily resolves each named index
/// through the control plane on first use.
#[derive(Clone)]
pub struct Gateway {
    /// The single static route, when the Gateway was built via a single-index constructor
    /// (`new`/`sharded`/`windowed`/…). Present iff `resolver` is `None`. Every read routes to it;
    /// `served_index` (below) still gates the request's `index` field. Its inner swap cell backs
    /// [`swap_routing`](Self::swap_routing)/[`swap_windowed`](Self::swap_windowed) hot-reload.
    single: Option<Arc<IndexRoute>>,
    /// Lazily-populated per-index routes for a **multi-index** Gateway: resolved-index-name
    /// → its [`IndexRoute`]. Empty in single-index mode. Populated on first request for an index via
    /// `resolver`, then hot-reloaded by a per-index reloader the resolver spawns.
    routes: Arc<std::sync::RwLock<HashMap<String, Arc<IndexRoute>>>>,
    /// Resolves a named index into an [`IndexRoute`] (fetch its `GetIndex`, connect nodes). `Some`
    /// only in multi-index mode; drives lazy population of `routes`. `None` = single-index (static).
    resolver: Option<Arc<dyn RouteResolver>>,
    /// Brief negative cache of index names the resolver reported as absent: name → when it
    /// was cached. A flood of requests for a nonexistent index shouldn't hammer the control plane; a
    /// `NOT_FOUND` is remembered for [`NEGATIVE_CACHE_TTL`] before we ask the CP again.
    missing: Arc<std::sync::RwLock<HashMap<String, Instant>>>,
    /// The index a request with an **empty** `index` field resolves to. `Some` = that
    /// index; `None` = no default (empty `index` errors unless exactly one index is served — see
    /// [`resolve_route`](Self::resolve_route)). In single-index mode this is the served index name.
    default_index: Option<String>,
    limits: GatewayLimits,
    authn: Option<SharedAuthn>,
    /// Built-in (no-IdP) password login is available — advertised on `/v1/config` so the
    /// console shows a username/password form rather than an OIDC redirect.
    password_login: bool,
    authz: Option<SharedAuth>,
    cold: Option<ColdTier>,
    /// Each temporal field's declared unit (by path), so the `_search` adapter converts a range/exact
    /// bound written in that unit to canonical micros before planning — keeping window pruning and
    /// segment execution (both micros-native) consistent. Populated from the served definition
    /// ([`with_date_formats`](Self::with_date_formats)); empty ⇒ no conversion (bounds are micros).
    date_formats: crate::opensearch::FieldFormats,
    /// The index name this Gateway serves, in **single-index** mode. A request whose `index`
    /// is non-empty and names a *different* index is answered `NOT_FOUND` instead of silently searching
    /// this one. `None` (the default, and all tests) means "serve any request" — no index scoping. In
    /// multi-index mode this is unused (scoping is per-`routes` membership).
    served_index: Option<String>,
    /// Bounds concurrent per-shard RPCs across all scatter-gathers — see
    /// [`GatewayLimits::max_concurrent_fanout`].
    fanout: Arc<tokio::sync::Semaphore>,
}

/// How long a `NOT_FOUND` from the [`RouteResolver`] is remembered before the control plane is asked
/// again: short, so an index created moments ago becomes queryable quickly,
/// but long enough that a burst of requests for a bad name doesn't storm the control plane.
const NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(5);

/// One index's routing on a [`Gateway`]: the shard-set + key router (+ optional windowed
/// descriptors) behind a hot-swap cell, plus that
/// index's keyword partition fields for fan-out pruning. A multi-index Gateway holds one per served
/// index; a single-index Gateway holds exactly one (its `single`). Cloneable-cheap (`Arc` inner).
pub struct IndexRoute {
    /// The shard set + key router, behind a swap so a running route can **hot-reload** its topology
    /// after a reshard cutover: [`swap`](Self::swap) installs a new `(shards, router)`
    /// atomically; each request reads a snapshot via [`routing`](Self::routing).
    routing: std::sync::RwLock<Arc<RoutingState>>,
    /// This index's **keyword** partition-key fields, for search fan-out pruning. When a
    /// search AND-pins all of them to values, every matching key routes to a single shard, so the
    /// query goes there instead of broadcasting. Empty = no pruning.
    partition_fields: Vec<String>,
}

impl IndexRoute {
    /// A route over a shard set + router (+ optional windowed descriptors) and its keyword partition
    /// fields. `partition_fields` should be only keyword-typed partitions (the caller filters them).
    pub fn new(
        shards: Vec<Arc<dyn Node>>,
        router: ShardRouter,
        window_routing: Option<WindowRouting>,
        partition_fields: Vec<String>,
    ) -> Arc<Self> {
        Arc::new(Self {
            routing: std::sync::RwLock::new(Arc::new(RoutingState {
                shards,
                router,
                window_routing,
            })),
            partition_fields,
        })
    }

    /// A snapshot of this route's current routing (`Arc` clone), consistent across a concurrent swap.
    fn routing(&self) -> Arc<RoutingState> {
        self.routing
            .read()
            .expect("routing lock not poisoned")
            .clone()
    }

    /// **Hot-reload** this route's ordinal topology: atomically replace shard set + router.
    /// Skips an empty/count-mismatched swap (keeps the current servable topology), like the Gateway
    /// method it backs.
    pub fn swap(&self, shards: Vec<Arc<dyn Node>>, router: ShardRouter) {
        if shards.is_empty() || router.shards() as usize != shards.len() {
            eprintln!(
                "gateway: ignoring an invalid routing swap ({} shards, router covers {}) — keeping current topology",
                shards.len(),
                router.shards()
            );
            return;
        }
        *self.routing.write().expect("routing lock not poisoned") = Arc::new(RoutingState {
            shards,
            router,
            window_routing: None,
        });
    }

    /// **Hot-swap** this route's windowed window set. Skips an empty/mismatched swap.
    pub fn swap_windowed(
        &self,
        shards: Vec<Arc<dyn Node>>,
        windowing: TimeWindowing,
        windows: Vec<WindowDescriptor>,
    ) {
        if shards.is_empty() || shards.len() != windows.len() {
            eprintln!(
                "gateway: ignoring an invalid windowed swap ({} shards, {} window descriptors) — keeping current topology",
                shards.len(),
                windows.len()
            );
            return;
        }
        let router = ShardRouter::hashed(shards.len() as u32);
        *self.routing.write().expect("routing lock not poisoned") = Arc::new(RoutingState {
            shards,
            router,
            window_routing: Some(WindowRouting { windowing, windows }),
        });
    }
}

/// Resolves a named index into an [`IndexRoute`] for a **multi-index** Gateway: fetch the
/// index's shard map (e.g. a control-plane `GetIndex`), connect a [`Node`] per shard, and build the
/// route (with a per-index hot-reloader wired to its [`IndexRoute::swap`]). Returns `Ok(None)` when
/// the index doesn't exist (→ `NOT_FOUND`, negative-cached), `Err` on a transient failure (→ the
/// caller surfaces `Unavailable`, not cached, so it retries).
#[tonic::async_trait]
pub trait RouteResolver: Send + Sync {
    /// Resolve `index` into its route, or `Ok(None)` if no such index is registered.
    async fn resolve(&self, index: &str) -> Result<Option<Arc<IndexRoute>>, String>;
}

/// The Gateway's hot-swappable routing: one [`Node`] per shard + the [`ShardRouter`] that places
/// keys, plus (for a windowed index) the [`WindowRouting`] descriptors. Snapshotted per request
/// (cheap `Arc` clone) so an in-flight scatter sees a consistent topology even as a reshard — or a
/// **new window** — swaps in a new one. Keeping `window_routing`
/// *inside* the swap cell (rather than a fixed Gateway field) is what lets a running windowed gateway
/// learn a new window's id + zone-map atomically with its node, via [`swap_windowed`](Gateway::swap_windowed).
pub struct RoutingState {
    shards: Vec<Arc<dyn Node>>,
    router: ShardRouter,
    /// `Some` on a windowed gateway; `None` on hash/partition. Aligned 1:1 with `shards`.
    window_routing: Option<WindowRouting>,
}

/// Windowed-index routing: the [`TimeWindowing`] config plus, **aligned 1:1 with
/// `shards`**, each shard's window id and event-time zone-map. Lets the Gateway prune a
/// time-filtered search to the windows that can match before scatter-gather. `None` on a normal
/// (hash/partition) Gateway.
/// One window's routing descriptor: `(window id, event zone-map, cold)` — `cold` = served
/// read-through from object storage (parked). Aligned 1:1 with a route's `shards`.
pub type WindowDescriptor = (i64, Option<(i64, i64)>, bool);

#[derive(Clone)]
pub struct WindowRouting {
    windowing: TimeWindowing,
    /// Per-shard [`WindowDescriptor`] for `shards[i]`.
    windows: Vec<WindowDescriptor>,
}

impl WindowRouting {
    /// A windowed routing descriptor: the [`TimeWindowing`] config + per-shard
    /// `(window id, event zone-map)`, for building an [`IndexRoute`] over a windowed index.
    pub fn new(windowing: TimeWindowing, windows: Vec<WindowDescriptor>) -> Self {
        Self { windowing, windows }
    }
}

/// Collect `field → value` for [`Query::Term`] leaves that are **ANDed** (in `must`/`filter`,
/// possibly nested) and target one of `fields` — the partition equalities a search pins.
/// Only AND clauses force the field to a single value for *every* match, so `should`/OR, `must_not`,
/// and negation are ignored: pruning on them could drop shards that legitimately hold matches.
fn collect_and_pins(
    q: &Query,
    fields: &[String],
    out: &mut std::collections::HashMap<String, String>,
) {
    match q {
        Query::Term {
            field: Some(f),
            value,
        } if fields.iter().any(|pf| pf == f) => {
            out.insert(f.clone(), value.clone());
        }
        Query::Bool { must, filter, .. } => {
            for c in must.iter().chain(filter.iter()) {
                collect_and_pins(c, fields, out);
            }
        }
        _ => {}
    }
}

/// The shared read-through byte-range cache of an **in-process** (node) windowed Gateway — kept so
/// `/v1/cold` can report its hit/miss/byte stats. Per-window hot/cold tier is NOT here: it comes from
/// the routing descriptors (a node's live shards, or the tiers each node reports to the cluster
/// gateway), so a runtime park/pre-warm is reflected without re-wiring this.
#[derive(Clone)]
struct ColdTier {
    cache: growlerdb_index::RangeCache,
}

/// Cold-tier status — per-window hot/cold tier + the shared cache's stats. Serialized at
/// `GET /v1/cold` so the console can show warm/cold state and the cold-read efficiency.
#[derive(serde::Serialize)]
pub struct ColdStatus {
    /// Each window's tier + event zone-map (oldest first).
    pub windows: Vec<WindowStatus>,
    /// Shared read-through cache stats, or `None` when no window is cold.
    pub cache: Option<growlerdb_index::CacheStats>,
    /// How many windows are hot (local) vs cold (read-through).
    pub hot: usize,
    /// Cold (read-through) window count.
    pub cold: usize,
}

/// One window's tier in [`ColdStatus`].
#[derive(serde::Serialize)]
pub struct WindowStatus {
    /// Window id (epoch-ms of the window start).
    pub window: i64,
    /// `true` if served read-through from object storage; `false` if hot (local).
    pub cold: bool,
    /// Event-time zone-map lower bound, if known.
    pub event_min: Option<i64>,
    /// Event-time zone-map upper bound.
    pub event_max: Option<i64>,
}

impl Gateway {
    /// Assemble a single-index Gateway from its routing + the optional features (the shared
    /// constructor body). Holds one static [`IndexRoute`]; multi-index gateways use
    /// [`multi_index`](Self::multi_index) instead.
    fn with_routing(
        shards: Vec<Arc<dyn Node>>,
        router: ShardRouter,
        window_routing: Option<WindowRouting>,
    ) -> Self {
        Self {
            single: Some(IndexRoute::new(shards, router, window_routing, Vec::new())),
            routes: Arc::new(std::sync::RwLock::new(HashMap::new())),
            resolver: None,
            missing: Arc::new(std::sync::RwLock::new(HashMap::new())),
            default_index: None,
            limits: GatewayLimits::default(),
            authn: None,
            password_login: false,
            authz: None,
            cold: None,
            date_formats: crate::opensearch::FieldFormats::default(),
            served_index: None,
            fanout: fanout_semaphore(GatewayLimits::default().max_concurrent_fanout),
        }
    }

    /// A **multi-index** Gateway: serves *many* indexes over one endpoint, resolving each
    /// named index lazily through `resolver` (typically a control-plane `GetIndex` builder) and
    /// hot-reloading each independently. A request with an empty `index` uses `default_index` (or, if
    /// `None` and exactly one index has been resolved, that one; else `InvalidArgument`). Readiness is
    /// the control plane's reachability, not any one index resolving.
    pub fn multi_index(resolver: Arc<dyn RouteResolver>, default_index: Option<String>) -> Self {
        Self {
            single: None,
            routes: Arc::new(std::sync::RwLock::new(HashMap::new())),
            resolver: Some(resolver),
            missing: Arc::new(std::sync::RwLock::new(HashMap::new())),
            default_index,
            limits: GatewayLimits::default(),
            authn: None,
            password_login: false,
            authz: None,
            cold: None,
            date_formats: crate::opensearch::FieldFormats::default(),
            served_index: None,
            fanout: fanout_semaphore(GatewayLimits::default().max_concurrent_fanout),
        }
    }

    /// Declare the (single) index's **keyword** partition-key fields so a search that pins them prunes
    /// its fan-out to the owning shard. Pass only keyword-typed partition fields — the
    /// caller (the sharded serve path) filters them from the resolved definition; a non-keyword
    /// partition field is omitted so pruning never routes a mistyped value to the wrong shard. Only
    /// meaningful in single-index mode; multi-index routes carry their own partition fields.
    pub fn with_partition_fields(mut self, fields: Vec<String>) -> Self {
        if let Some(single) = &self.single {
            self.single = Some(IndexRoute::new(
                single.routing().shards.clone(),
                single.routing().router.clone(),
                single.routing().window_routing.clone(),
                fields,
            ));
        }
        self
    }

    /// The single static [`IndexRoute`] — present iff this is a single-index Gateway. Every
    /// single-index handler resolves to this.
    fn single(&self) -> &Arc<IndexRoute> {
        self.single
            .as_ref()
            .expect("single-index gateway has a static route")
    }

    /// A snapshot of the single-index route's current routing (`Arc` clone). Only valid in
    /// single-index mode (the swap methods below back the CLI reloaders that operate on `self.single`).
    fn routing(&self) -> Arc<RoutingState> {
        self.single().routing()
    }

    /// **Hot-reload** the (single) topology: atomically replace the shard set + router, e.g.
    /// after a reshard cutover the control plane committed a new bucket map and added nodes. In-flight
    /// requests finish against their snapshot; subsequent ones route through the new topology. The
    /// router's shard count must match the node count. (Ordinal indexes only — not windowed.)
    pub fn swap_routing(&self, shards: Vec<Arc<dyn Node>>, router: ShardRouter) {
        self.single().swap(shards, router);
    }

    /// **Hot-swap** the (single) windowed gateway's window set:
    /// atomically install a new `(shards, window descriptors)` so a running windowed gateway can serve
    /// a **newly-created** window (or an updated zone-map) without a restart — the windowed analog of
    /// [`swap_routing`]. `windows` aligns 1:1 with `shards` (one `(window id, event zone-map)` each);
    /// the key router is regenerated as `hashed(n)` (windowed fan-out never key-routes). In-flight
    /// requests finish against their snapshot. Skips an empty/mismatched swap, like `swap_routing`.
    pub fn swap_windowed(
        &self,
        shards: Vec<Arc<dyn Node>>,
        windowing: TimeWindowing,
        windows: Vec<WindowDescriptor>,
    ) {
        self.single().swap_windowed(shards, windowing, windows);
    }

    /// Resolve a request's target `index` field to the [`IndexRoute`] that answers it — the
    /// per-request routing decision every read/write handler makes at entry.
    ///
    /// The empty-index rule: an empty `index` uses `default_index` if set; else, if exactly one index
    /// is currently served (a lone `single` route, or a single resolved multi-index route with no
    /// resolver ambiguity), that one; else `InvalidArgument` ("index required; endpoint serves N
    /// indexes"). A non-empty `index` names its target directly.
    ///
    /// Single-index mode preserves index scoping exactly: a non-empty name that differs from the
    /// served index is `NOT_FOUND`; empty or the served name resolves to the static route. Multi-index
    /// mode resolves the named index through the `resolver` (lazily populating `routes` and spawning
    /// that index's hot-reloader), negative-caching a `NOT_FOUND` briefly.
    async fn resolve_route(&self, index: &str) -> Result<Arc<IndexRoute>, Status> {
        let want = index.trim();
        // ---- Single-index (static) mode. --------------------------
        if let Some(single) = &self.single {
            if let Some(served) = &self.served_index {
                if !want.is_empty() && want != served {
                    return Err(Status::not_found(format!(
                        "index `{want}` is not served by this endpoint (serving `{served}`)"
                    )));
                }
            }
            return Ok(single.clone());
        }

        // ---- Multi-index mode: resolve the named index (empty → default / sole). ------------------
        let target = if !want.is_empty() {
            want.to_string()
        } else if let Some(def) = &self.default_index {
            def.clone()
        } else {
            // No default: only unambiguous if exactly one index is currently served.
            let routes = self.routes.read().expect("routes lock not poisoned");
            if routes.len() == 1 {
                return Ok(routes.values().next().expect("one route").clone());
            }
            return Err(Status::invalid_argument(format!(
                "index required; endpoint serves {} indexes",
                routes.len()
            )));
        };

        // Fast path: already resolved.
        if let Some(route) = self
            .routes
            .read()
            .expect("routes lock not poisoned")
            .get(&target)
            .cloned()
        {
            return Ok(route);
        }

        // Negative cache: recently reported absent → NOT_FOUND without touching the control plane.
        if let Some(at) = self
            .missing
            .read()
            .expect("missing lock not poisoned")
            .get(&target)
            .copied()
        {
            if at.elapsed() < NEGATIVE_CACHE_TTL {
                return Err(Status::not_found(format!(
                    "index `{target}` is not served by this endpoint"
                )));
            }
        }

        let resolver = self
            .resolver
            .as_ref()
            .expect("multi-index gateway has a resolver");
        match resolver.resolve(&target).await {
            Ok(Some(route)) => {
                // Insert (another concurrent request may have won the race — keep the first).
                let mut routes = self.routes.write().expect("routes lock not poisoned");
                let route = routes.entry(target.clone()).or_insert(route).clone();
                self.missing
                    .write()
                    .expect("missing lock not poisoned")
                    .remove(&target);
                Ok(route)
            }
            Ok(None) => {
                self.missing
                    .write()
                    .expect("missing lock not poisoned")
                    .insert(target.clone(), Instant::now());
                Err(Status::not_found(format!(
                    "index `{target}` is not served by this endpoint"
                )))
            }
            // A transient failure (control plane down, node unreachable) is Unavailable, NOT cached —
            // the next request retries rather than being stuck on a stale miss.
            Err(e) => Err(Status::unavailable(format!(
                "resolving index `{target}`: {e}"
            ))),
        }
    }

    /// A single-shard Gateway fronting `node` (requests forward verbatim).
    pub fn new(node: Arc<dyn Node>) -> Self {
        Self::with_routing(vec![node], ShardRouter::hashed(1), None)
    }

    /// A multi-shard Gateway over one Node per shard, with **hash** key routing (the default
    /// for an unpartitioned index). Queries scatter-gather across the shards and merge.
    pub fn sharded(shards: Vec<Arc<dyn Node>>) -> Self {
        let router = ShardRouter::hashed(shards.len() as u32);
        Self::sharded_with(shards, router)
    }

    /// A multi-shard Gateway with an explicit key [`ShardRouter`] (e.g. partition routing).
    /// The router's shard count must match the node count.
    pub fn sharded_with(shards: Vec<Arc<dyn Node>>, router: ShardRouter) -> Self {
        // Hard invariants: a Gateway with 0 shards (or a count-mismatched router)
        // can't serve and would panic on `shards[…]` — fail loudly at construction, not later.
        assert!(!shards.is_empty(), "a Gateway needs at least one shard");
        assert_eq!(
            router.shards() as usize,
            shards.len(),
            "router shard count must match node count"
        );
        Self::with_routing(shards, router, None)
    }

    /// A **windowed** Gateway: one Node per time-window shard, each tagged with its
    /// window id + event-time zone-map (`windows` aligns 1:1 with `shards`). A search carrying a
    /// range filter on the window or event-time field is pruned to the overlapping windows before
    /// scatter-gather; an unfiltered search fans out to all. The cross-shard merge is the same as
    /// any multi-shard Gateway — windows are just shards the query may skip.
    pub fn windowed(
        shards: Vec<Arc<dyn Node>>,
        windowing: TimeWindowing,
        windows: Vec<WindowDescriptor>,
    ) -> Self {
        debug_assert_eq!(
            shards.len(),
            windows.len(),
            "one window descriptor per shard"
        );
        let router = ShardRouter::hashed(shards.len().max(1) as u32);
        Self::with_routing(shards, router, Some(WindowRouting { windowing, windows }))
    }

    /// The shards a search must touch on `route`. Normally every shard; for a windowed route, only the
    /// windows whose id + event zone-map overlap the query's time range. A query that
    /// doesn't parse or carries no relevant range bound prunes nothing (fans out to all) — pruning
    /// only ever *removes* windows that provably can't match, so results never change.
    fn target_shards(route: &IndexRoute, body: &SearchRequest) -> Vec<Arc<dyn Node>> {
        // Partition-prune: on a non-windowed, partition-routed index, a search that
        // AND-pins every (keyword) partition field can only match keys owned by one shard — route
        // there instead of broadcasting to all. Correct because Partition routing depends solely on
        // the partition (the identifier is dropped), so no matching key lives on another shard.
        let rs = route.routing();
        if rs.window_routing.is_none() {
            if let Some(ord) = Self::partition_prune(route, &body.query, &rs) {
                return vec![rs.shards[ord].clone()];
            }
        }
        Self::windows_matching(&rs, &body.query)
    }

    /// The single shard a search must touch when its filter AND-pins **all** of `route`'s keyword
    /// partition fields — else `None` (fan out). Builds the pinned partition into a [`CompositeKey`]
    /// and routes it (reusing the same [`ShardRouter`] as [`get_by_key`](Self::get_by_key)).
    fn partition_prune(route: &IndexRoute, query_str: &str, rs: &RoutingState) -> Option<usize> {
        if route.partition_fields.is_empty() {
            return None;
        }
        let query = Query::parse(query_str).ok()?;
        let mut pinned: std::collections::HashMap<String, String> =
            std::collections::HashMap::new();
        collect_and_pins(&query, &route.partition_fields, &mut pinned);
        // Every partition field must be pinned, else the partition isn't determined → fan out.
        let partition: Vec<(String, Value)> = route
            .partition_fields
            .iter()
            .map(|f| pinned.remove(f).map(|v| (f.clone(), Value::Str(v))))
            .collect::<Option<Vec<_>>>()?;
        let ord = rs.router.route(&CompositeKey::new(partition, Vec::new())) as usize;
        (ord < rs.shards.len()).then_some(ord)
    }

    /// The shards a `query_str` must touch on routing snapshot `rs` (shared by search and aggregate):
    /// all shards on a normal route; on a windowed route only the windows whose id + event
    /// zone-map overlap the query's time range. An unparseable / unfiltered query prunes nothing.
    /// Pruning only removes windows that can't match, so a windowed aggregate over a time range gives
    /// the same result as scanning all windows — just cheaper.
    fn windows_matching(rs: &RoutingState, query_str: &str) -> Vec<Arc<dyn Node>> {
        let Some(wr) = &rs.window_routing else {
            return rs.shards.clone();
        };
        let Ok(query) = Query::parse(query_str) else {
            return rs.shards.clone();
        };
        rs.shards
            .iter()
            .zip(&wr.windows)
            .filter(|(_, (w, zone, _cold))| wr.windowing.keeps(*w, *zone, &query))
            .map(|(s, _)| s.clone())
            .collect()
    }

    /// Override the [resiliency limits](GatewayLimits) (deadline + max page fetch + fan-out cap).
    pub fn with_limits(mut self, limits: GatewayLimits) -> Self {
        self.fanout = fanout_semaphore(limits.max_concurrent_fanout);
        self.limits = limits;
        self
    }

    /// Run a single-shard forward under the same per-query deadline the scatter-gather uses. The
    /// multi-shard path bounds itself via [`gather_responses`], but a direct single-shard forward
    /// otherwise had no ceiling, so one slow legal query could pin a blocking worker indefinitely.
    /// On expiry the caller gets `DEADLINE_EXCEEDED`.
    async fn under_deadline<T>(
        &self,
        fut: impl std::future::Future<Output = Result<T, Status>>,
    ) -> Result<T, Status> {
        match self.limits.deadline {
            Some(d) => tokio::time::timeout(d, fut).await.map_err(|_| {
                Status::deadline_exceeded("query exceeded the per-request deadline")
            })?,
            None => fut.await,
        }
    }

    /// Declare the index name this Gateway serves. A search whose `index` is non-empty and
    /// names a different index is then rejected with `NOT_FOUND` — the console can scope a query to a
    /// named index and trust it won't be silently answered by the wrong one. Without this the Gateway
    /// ignores `SearchRequest.index` and serves every request.
    pub fn serving(mut self, index: impl Into<String>) -> Self {
        let name = index.into();
        // The served index also becomes the empty-`index` default, so a bare request resolves to it.
        self.default_index = Some(name.clone());
        self.served_index = Some(name);
        self
    }

    /// Install an [authenticator](crate::authn) — the Gateway is where authentication
    /// terminates (wiki/22-security). Once set, every query-surface call must carry a valid
    /// credential: the Gateway authenticates it, stamps the *verified* principal/tenant into
    /// the request (dropping any caller-asserted identity), then routes. Without this the
    /// Gateway stays open and forwards caller-supplied identity verbatim.
    pub fn with_authn(mut self, authn: SharedAuthn) -> Self {
        self.authn = Some(authn);
        self
    }

    /// Mark that built-in password login is available — surfaced via `/v1/config` so the
    /// console renders a username/password form.
    pub fn with_password_login(mut self, on: bool) -> Self {
        self.password_login = on;
        self
    }

    /// Whether built-in password login is available, for `/v1/config`.
    pub fn password_login(&self) -> bool {
        self.password_login
    }

    /// Install an [authorization hook](crate::auth::AuthHook) — typically an
    /// [`RbacPolicy`](crate::rbac::RbacPolicy) — enforced at the Gateway after authentication.
    /// A call whose verified roles don't grant the method's scope is rejected with
    /// `PermissionDenied` *before* any shard is touched. Without this, only AuthN runs.
    pub fn with_authz(mut self, authz: SharedAuth) -> Self {
        self.authz = Some(authz);
        self
    }

    /// Tag a windowed Gateway with its **cold-tier** state: the set of `cold_windows`
    /// served read-through + the shared read-through `cache`, surfaced by [`cold_status`](Self::cold_status).
    /// Wire the shared read-through cache so `/v1/cold` can report its stats (an in-process node
    /// gateway). The per-window tier is carried by the routing descriptors, not here.
    pub fn with_cold_tier(mut self, cache: growlerdb_index::RangeCache) -> Self {
        self.cold = Some(ColdTier { cache });
        self
    }

    /// Declare the served index's temporal-field units so the `_search` adapter converts range/exact
    /// bounds written in those units to canonical micros before planning. From the served definition
    /// ([`ResolvedIndex::date_formats`](growlerdb_core::ResolvedIndex::date_formats), or the field
    /// mappings on `GetIndex` for a live-CP gateway).
    pub fn with_date_formats(mut self, formats: crate::opensearch::FieldFormats) -> Self {
        self.date_formats = formats;
        self
    }

    /// The served index's temporal-field units, for the `_search` adapter's bound conversion.
    pub fn date_formats(&self) -> &crate::opensearch::FieldFormats {
        &self.date_formats
    }

    /// Cold-tier status: each window's hot/cold tier + event zone-map, plus the shared
    /// read-through cache's hit/miss/byte stats. `None` on a non-windowed Gateway.
    pub fn cold_status(&self) -> Option<ColdStatus> {
        let rs = self.routing();
        let wr = rs.window_routing.as_ref()?;
        // The per-window tier comes from the routing descriptors — for the in-process node gateway
        // that is its live shard set; for the cluster gateway it is the tier each node reports every
        // heartbeat (so a runtime park/pre-warm shows up here, not just the boot snapshot). The shared
        // read-through cache (and its hit/miss stats) is local to a node, so only an in-process
        // gateway that was wired [`with_cold_tier`] reports cache stats.
        let windows: Vec<WindowStatus> = wr
            .windows
            .iter()
            .map(|(w, zone, cold)| WindowStatus {
                window: *w,
                cold: *cold,
                event_min: zone.map(|z| z.0),
                event_max: zone.map(|z| z.1),
            })
            .collect();
        let cold_count = windows.iter().filter(|w| w.cold).count();
        Some(ColdStatus {
            cache: self.cold.as_ref().map(|c| c.cache.stats()),
            hot: windows.len() - cold_count,
            cold: cold_count,
            windows,
        })
    }

    /// Authenticate + authorize + **resolve the target route** for a read/write `method` in one step:
    /// guards the request against the target index (so per-index RBAC sees it), then hands
    /// back the [`IndexRoute`] the handler operates on. `index` is the request's `index` field.
    ///
    /// Order: **authn → authz(target index) → resolve**. Authenticating first stamps the verified
    /// identity; authorizing *before* resolution means a token whose allowlist forbids the index is
    /// `PermissionDenied` without a control-plane round-trip and without revealing whether the index
    /// exists (no shard is ever touched for a denied index). The authz target is the request's explicit
    /// name, or — when empty — the endpoint's default index (so an allowlist still binds the default).
    async fn guard_and_resolve<T>(
        &self,
        method: &'static str,
        index: &str,
        req: &mut Request<T>,
    ) -> Result<Arc<IndexRoute>, Status> {
        match &self.authn {
            Some(authn) => {
                crate::authn::authenticate(authn, req)?;
            }
            None => crate::authn::strip_identity(req),
        }
        if let Some(authz) = &self.authz {
            let target = if index.trim().is_empty() {
                self.default_index.as_deref()
            } else {
                Some(index.trim())
            };
            crate::auth::authorize_index(authz, method, target, req)?;
        }
        self.resolve_route(index).await
    }

    /// The **verified identity** of the caller of `req`, for `GET /v1/me`. Authenticates
    /// (but does not authorize — identity is not a gated operation) and returns the trusted
    /// principal/roles/profile. On an **open** gateway (no authenticator) returns the
    /// [anonymous](crate::authn::Verified::anonymous) shape, so the console shows "not signed in"
    /// rather than erroring; a configured gateway with a missing/invalid token returns the authn
    /// error (401), which the console also treats as anonymous.
    pub fn identity<T>(&self, req: &mut Request<T>) -> Result<crate::authn::Verified, Status> {
        match &self.authn {
            Some(authn) => crate::authn::authenticate(authn, req),
            None => Ok(crate::authn::Verified::anonymous()),
        }
    }

    /// Whether this gateway **requires authentication** ("closed mode") — true iff an authenticator
    /// is configured. The console reads this from `GET /v1/config` to decide whether to gate the app
    /// behind a login screen; an open gateway (no authenticator) returns false and the
    /// console runs un-gated, the zero-config trial/POC path.
    pub fn auth_required(&self) -> bool {
        self.authn.is_some()
    }

    /// Number of shards this Gateway fronts for its single index. In multi-index mode there
    /// is no single shard count (each resolved index has its own), so this reports `0` — callers that
    /// log a shard count use it only for the single-index CLI paths.
    pub fn shard_count(&self) -> usize {
        match &self.single {
            Some(single) => single.routing().shards.len(),
            None => 0,
        }
    }

    /// Run a search. Single-shard: forward verbatim. Multi-shard: scatter to every shard,
    /// merge the hits into one global order (by sort tuple when sorted, else by score), apply
    /// the global `offset`/`limit`, and flag `partial` if any shard failed. Records the query
    /// SLI (rate/errors/duration) around the whole call, so both the gRPC and REST
    /// fronts are covered through this one chokepoint.
    pub async fn search(
        &self,
        req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let start = std::time::Instant::now();
        let result = self.search_inner(req).await;
        growlerdb_telemetry::sli::query(start.elapsed().as_secs_f64(), result.is_err());
        result
    }

    #[tracing::instrument(skip_all, fields(shards = self.shard_count()), err)]
    async fn search_inner(
        &self,
        mut req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        // Authenticate + resolve the target index + authorize against it (index scoping,
        // multi-index routing + per-index RBAC), all before any routing — the shards must only ever
        // see a trusted identity, and a request naming an index this endpoint doesn't serve is
        // NOT_FOUND rather than silently answered by the wrong one. Empty `index` = the default index.
        let index = req.get_ref().index.clone();
        let route = self.guard_and_resolve("Search", &index, &mut req).await?;
        // Page-fetch ceiling: reject a huge `offset + limit` at the boundary before any
        // shard builds the page — an unbounded `limit` is an easy OOM/DoS (S shards × a giant
        // page, buffered + sorted at the Gateway). Applies to single- and multi-shard alike.
        let fetch = (req.get_ref().offset as usize).saturating_add(req.get_ref().limit as usize);
        if self.limits.max_fetch > 0 && fetch > self.limits.max_fetch {
            return Err(Status::invalid_argument(format!(
                "offset+limit ({fetch}) exceeds the maximum page fetch ({}); request a smaller page",
                self.limits.max_fetch
            )));
        }
        // Target shards: all of them, or — for a windowed index — only the windows whose time
        // range can match (window pruning). A time filter outside every window matches nothing.
        let shards_total = route.routing().shards.len() as u32;
        let shards = Self::target_shards(&route, req.get_ref());
        if shards.is_empty() {
            // A time filter that prunes every window matches nothing — 0 shards scanned, but the
            // index still has `shards_total` (so the console shows e.g. "0/64", not a blank).
            return Ok(Response::new(SearchResponse {
                hits: Vec::new(),
                total: 0,
                next_cursor: Vec::new(),
                partial: false,
                shards_scanned: 0,
                shards_total,
            }));
        }
        if shards.len() == 1 {
            let mut resp = self.under_deadline(shards[0].search(req)).await?;
            let r = resp.get_mut();
            r.shards_scanned = 1;
            r.shards_total = shards_total;
            return Ok(resp);
        }
        let (meta, _ext, body) = req.into_parts();
        let offset = body.offset as usize;
        let limit = body.limit as usize;

        // `offset` paging (offset-merge) and `search_after` keyset scrolling (below) are both
        // supported across shards. Keyset paging needs a sort to define the keyset — a
        // score-ranked scroll has no stable cursor (scores aren't a keyset), so reject that —
        // whether the sort is empty (pure relevance) or carries an explicit `_score` key.
        let sort_by_score = body.sort.iter().any(|s| s.field == SCORE_SORT_KEY);
        if !body.search_after.is_empty() && (body.sort.is_empty() || sort_by_score) {
            return Err(Status::invalid_argument(
                "search_after requires a non-`_score` sort on a multi-shard index: score-ranked \
                 keyset paging is unsupported because scores aren't a stable keyset.",
            ));
        }
        // Collapse folds groups across shards on its own scatter/merge path — it ignores
        // offset/keyset paging, so it doesn't share the offset-merge logic below.
        if !body.collapse.is_empty() {
            return self
                .search_collapsed_merge(&shards, shards_total, meta, body)
                .await;
        }

        // Offset-merge (design/09 §9): a shard can't apply the *global* offset, so ask each for
        // the page from rank 0 deep enough to cover it — `offset + limit` hits — and apply the
        // global `offset`/`limit` once, at the merge. A `search_after` cursor encodes the global
        // position directly, so `offset` is ignored on the keyset path (each shard resumes
        // strictly after the cursor and returns up to `limit`). `limit == 0` means unbounded.
        let effective_offset = if body.search_after.is_empty() {
            offset
        } else {
            0
        };
        let per_shard_limit = if limit == 0 {
            0
        } else {
            effective_offset
                .saturating_add(limit)
                .min(u32::MAX as usize) as u32
        };
        // The same `search_after` goes to every shard verbatim — it's the *global* cursor, so
        // each shard resumes after the same position in its local order; the merge stays total.
        let shard_body = SearchRequest {
            offset: 0,
            limit: per_shard_limit,
            ..body.clone()
        };

        let total_shards = shards.len();
        let mut set = tokio::task::JoinSet::new();
        for shard in &shards {
            let shard = shard.clone();
            let r = Request::from_parts(meta.clone(), Extensions::default(), shard_body.clone());
            let permit = self.fanout.clone();
            set.spawn(async move {
                let _permit = permit.acquire_owned().await; // bound concurrent fan-out
                shard.search(r).await
            });
        }

        // Gather under the deadline; a shard that errors, panics, or runs past the deadline (and
        // is aborted) simply doesn't contribute a body — `failed` counts those.
        let Fanout { bodies, errors } = gather_responses(set, self.limits.deadline).await;
        let failed = total_shards - bodies.len();
        // Every shard failed/timed out ⇒ an honest error, not a success-shaped empty page a
        // client could mistake for "no matches". A uniform client error (e.g. a bad query shape)
        // surfaces verbatim as a 4xx instead of an opaque 500 — see `all_shards_failed`.
        if bodies.is_empty() {
            return Err(all_shards_failed(
                format!("all {total_shards} shards failed to respond"),
                errors,
            ));
        }
        // Each shard reports its true match count; sum them for the global total. Shards are
        // normally disjoint, but during a **reshard** a moved bucket briefly lives on both
        // its old and new shard, so the merged hits are deduped by key below. `total` still
        // sums per-shard counts — it can over-count during that window, like the `partial` flag.
        let mut hits = Vec::new();
        let mut total_matches = 0u64;
        for r in bodies {
            total_matches += r.total;
            hits.extend(r.hits);
        }

        // Merge into one globally-ordered sequence, both paths using the encoded composite key
        // as the final, deterministic tiebreaker (a total order independent of shard
        // *completion* order). A **field-sorted** query orders by the sort tuple (the same
        // comparator the store uses across generations, lifted across shards); a
        // **score-ranked** query orders by score desc. Cross-shard keyset paging isn't defined
        // yet, so the page carries no cursor.
        if body.sort.is_empty() {
            let mut decorated: Vec<(Vec<u8>, growlerdb_proto::v1::SearchHit)> =
                hits.into_iter().map(|h| (hit_key(&h), h)).collect();
            decorated.sort_by(|a, b| {
                b.1.score
                    .partial_cmp(&a.1.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            hits = decorated.into_iter().map(|(_, h)| h).collect();
        } else {
            hits = merge_field_sorted(hits, &body.sort);
        }
        // Dedupe by composite key: keep the first (best-ranked) occurrence of each key, so
        // a doc that a reshard has on two shards mid-cutover is returned once. A no-op when shards
        // are disjoint (every key appears once).
        let mut seen = std::collections::HashSet::new();
        hits.retain(|h| seen.insert(hit_key(h)));
        // Apply the global window to the merged order: drop the first `effective_offset`, then
        // keep `limit` (0 = keep all). This is the step a single shard couldn't do.
        if effective_offset > 0 {
            hits.drain(..effective_offset.min(hits.len()));
        }
        if limit > 0 && hits.len() > limit {
            hits.truncate(limit);
        }
        // `total` is the global match count (summed across shards), not the page size.
        let total = total_matches;

        // Compose the **global** keyset cursor: for a sorted query that returned a
        // full page, the next page resumes strictly after the last returned hit's (sort tuple,
        // key). A short page means every shard is exhausted at this position → no cursor.
        // Score-ranked queries — pure relevance or an explicit `_score` key — have no stable
        // keyset, so they never carry a cursor.
        let next_cursor =
            if !body.sort.is_empty() && !sort_by_score && limit > 0 && hits.len() == limit {
                hits.last()
                    .and_then(search_after_from_hit)
                    .map(crate::search_service::encode_cursor)
                    .unwrap_or_default()
            } else {
                Vec::new()
            };
        Ok(Response::new(SearchResponse {
            hits,
            total,
            next_cursor,
            partial: failed > 0,
            shards_scanned: total_shards as u32,
            shards_total,
        }))
    }

    /// Multi-shard **collapse**: each shard collapses locally and returns its top-`limit` groups,
    /// each carrying the group's top-hit sort values. The Gateway **folds** groups by
    /// value across shards — summing each group's `group_count` and keeping the globally-best hit
    /// (the first in the merged sort order) — then orders the folded groups and truncates to
    /// `limit`. Collapse ignores offset/keyset paging, so no cursor is produced.
    ///
    /// Recall caveat (documented, same as distributed terms aggs / Elasticsearch field collapse):
    /// a group present on a shard but outside that shard's local top-`limit` can be missed; the
    /// fold is exact for the groups every relevant shard surfaced.
    async fn search_collapsed_merge(
        &self,
        shards: &[Arc<dyn Node>],
        shards_total: u32,
        meta: tonic::metadata::MetadataMap,
        body: SearchRequest,
    ) -> Result<Response<SearchResponse>, Status> {
        // Collapse defines each group's "top" by the sort, so a sort is required (mirrors the
        // Node). Check here so the client gets a clear error, not an all-shards-failed status.
        if body.sort.is_empty() {
            return Err(Status::invalid_argument(
                "collapse requires a non-empty sort on a multi-shard index",
            ));
        }
        let limit = body.limit as usize;

        // Collapse ignores offset/search_after; send each shard a clean window for its top groups.
        let shard_body = SearchRequest {
            offset: 0,
            search_after: Vec::new(),
            ..body.clone()
        };
        let total_shards = shards.len();
        let mut set = tokio::task::JoinSet::new();
        for shard in shards {
            let shard = shard.clone();
            let r = Request::from_parts(meta.clone(), Extensions::default(), shard_body.clone());
            let permit = self.fanout.clone();
            set.spawn(async move {
                let _permit = permit.acquire_owned().await; // bound concurrent fan-out
                shard.search(r).await
            });
        }

        let Fanout { bodies, errors } = gather_responses(set, self.limits.deadline).await;
        let failed = total_shards - bodies.len();
        if bodies.is_empty() {
            return Err(all_shards_failed(
                format!("all {total_shards} shards failed to respond"),
                errors,
            ));
        }
        let reps: Vec<growlerdb_proto::v1::SearchHit> =
            bodies.into_iter().flat_map(|r| r.hits).collect();

        // Sum each group's count across shards (every rep carries its shard's local count).
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        for h in &reps {
            if let Some(g) = group_key(h) {
                *counts.entry(g).or_default() += h.group_count;
            }
        }

        // Order all reps by the global sort; the first rep seen per group is its global top hit.
        let ordered = merge_field_sorted(reps, &body.sort);
        let mut seen: HashSet<String> = HashSet::new();
        let mut hits = Vec::new();
        for mut h in ordered {
            let Some(g) = group_key(&h) else { continue };
            if !seen.insert(g.clone()) {
                continue;
            }
            h.group_count = counts.get(&g).copied().unwrap_or(h.group_count);
            hits.push(h);
            if limit > 0 && hits.len() == limit {
                break;
            }
        }
        let total = hits.len() as u64;
        Ok(Response::new(SearchResponse {
            hits,
            total,
            next_cursor: Vec::new(),
            partial: failed > 0,
            shards_scanned: total_shards as u32,
            shards_total,
        }))
    }

    /// Run a **semantic (KNN) search** ([TASK-302]). The Gateway is pure orchestration: it
    /// resolves + authorizes the index, then scatters the `SemanticSearchRequest` to each shard —
    /// **each Node embeds the query text itself** (the Gateway carries no ML model) — and merges the
    /// KNN hits into one global top-`k` by score (the same score-merge + dedupe the lexical path
    /// uses). Single-shard forwards verbatim. Tenant scoping is enforced Node-side, fail-closed.
    pub async fn semantic_search(
        &self,
        mut req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let index = req.get_ref().index.clone();
        let route = self.guard_and_resolve("Search", &index, &mut req).await?;
        // Page-fetch ceiling on `k`, mirroring the lexical path's `offset+limit` guard: an
        // unbounded `k` would make every shard build a giant top-k and OOM the Gateway.
        let k = req.get_ref().k as usize;
        if self.limits.max_fetch > 0 && k > self.limits.max_fetch {
            return Err(Status::invalid_argument(format!(
                "k ({k}) exceeds the maximum page fetch ({}); request fewer neighbors",
                self.limits.max_fetch
            )));
        }
        let rs = route.routing();
        let shards_total = rs.shards.len() as u32;
        let shards = rs.shards.clone();
        if shards.len() == 1 {
            let mut resp = self.under_deadline(shards[0].semantic_search(req)).await?;
            let r = resp.get_mut();
            r.shards_scanned = 1;
            r.shards_total = shards_total;
            return Ok(resp);
        }
        let (meta, _ext, body) = req.into_parts();

        let total_shards = shards.len();
        let mut set = tokio::task::JoinSet::new();
        for shard in &shards {
            let shard = shard.clone();
            let r = Request::from_parts(meta.clone(), Extensions::default(), body.clone());
            let permit = self.fanout.clone();
            set.spawn(async move {
                let _permit = permit.acquire_owned().await; // bound concurrent fan-out
                shard.semantic_search(r).await
            });
        }

        let Fanout { bodies, errors } = gather_responses(set, self.limits.deadline).await;
        let failed = total_shards - bodies.len();
        if bodies.is_empty() {
            return Err(all_shards_failed(
                format!("all {total_shards} shards failed to respond"),
                errors,
            ));
        }
        let mut hits = Vec::new();
        let mut total_matches = 0u64;
        for r in bodies {
            total_matches += r.total;
            hits.extend(r.hits);
        }
        // KNN hits are score-ranked (no field sort) — merge by score desc + key tiebreak, dedupe
        // (a reshard can briefly place a key on two shards), and keep the global top-`k`. Identical
        // to the lexical `search_inner` score branch.
        let hits = merge_score_topk(hits, k);
        Ok(Response::new(SearchResponse {
            hits,
            total: total_matches,
            next_cursor: Vec::new(),
            partial: failed > 0,
            shards_scanned: total_shards as u32,
            shards_total,
        }))
    }

    /// Run a **hybrid search** ([TASK-302]): fan out BOTH a lexical (BM25) [`search`](Self::search)
    /// over `query_text` AND a [`semantic_search`](Self::semantic_search) over `vector_field`, each
    /// already merged into a ranked list across shards, then **Reciprocal-Rank-Fuse** the two lists
    /// at the Gateway. The Gateway embeds nothing — each Node embeds its own semantic arm. A doc
    /// strong in *both* modalities fuses above one strong in only one. The two arms carry the same
    /// auth metadata, so authz/tenant scoping binds each independently.
    pub async fn hybrid_search(
        &self,
        req: Request<HybridSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let (meta, _ext, body) = req.into_parts();
        let k = body.k as usize;
        let rrf_k = if body.rrf_k == 0 {
            HYBRID_RRF_K
        } else {
            body.rrf_k as usize
        };
        // Over-fetch each arm so the fusion has depth: a doc ranked past `k` in one arm can still
        // win once the other arm's rank is added in.
        let k_each = k.max(10).saturating_mul(2);
        if self.limits.max_fetch > 0 && k_each > self.limits.max_fetch {
            return Err(Status::invalid_argument(format!(
                "hybrid k ({k}) over-fetches {k_each} per arm, exceeding the maximum page fetch ({})",
                self.limits.max_fetch
            )));
        }

        // Lexical arm: the query text as a BM25 query.
        let lexical_req = Request::from_parts(
            meta.clone(),
            Extensions::default(),
            SearchRequest {
                query: body.query_text.clone(),
                limit: k_each as u32,
                index: body.index.clone(),
                syntax: body.syntax,
                ..Default::default()
            },
        );
        // Semantic arm: the same text embedded Node-side over the vector field, same filter.
        let semantic_req = Request::from_parts(
            meta.clone(),
            Extensions::default(),
            SemanticSearchRequest {
                index: body.index.clone(),
                vector_field: body.vector_field.clone(),
                query_text: body.query_text.clone(),
                k: k_each as u32,
                filter: body.filter.clone(),
                syntax: body.syntax,
                window: 0,
            },
        );

        // The semantic arm defines the request (its vector field, index resolution, and authz), so
        // surface its error verbatim. The lexical arm tolerates an unparseable/empty query — the
        // fusion then just reflects the semantic ranking (mirroring the engine's MatchAll-fallback
        // intent), rather than failing the whole hybrid on a query the vector arm handled fine.
        let semantic = self.semantic_search(semantic_req).await?.into_inner();
        let lexical = self.search(lexical_req).await.map(Response::into_inner);

        let empty: &[SearchHit] = &[];
        let lex_hits = lexical.as_ref().map(|r| r.hits.as_slice()).unwrap_or(empty);
        let partial = semantic.partial || lexical.as_ref().map(|r| r.partial).unwrap_or(false);
        let shards_scanned = semantic
            .shards_scanned
            .max(lexical.as_ref().map(|r| r.shards_scanned).unwrap_or(0));
        let shards_total = semantic
            .shards_total
            .max(lexical.as_ref().map(|r| r.shards_total).unwrap_or(0));

        let fused = rrf_fuse_hits(&[lex_hits, semantic.hits.as_slice()], rrf_k, k);
        let total = fused.len() as u64;
        Ok(Response::new(SearchResponse {
            hits: fused,
            total,
            next_cursor: Vec::new(),
            partial,
            shards_scanned,
            shards_total,
        }))
    }

    /// Run a suggest. Multi-shard: merge suggestions by text (summing counts), best (highest
    /// count) first, truncated to `limit`.
    pub async fn suggest(
        &self,
        mut req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        // Resolve the target index: suggest honors `SuggestRequest.index`, so a
        // multi-index endpoint suggests over the named index (and per-index RBAC applies).
        let index = req.get_ref().index.clone();
        let route = self.guard_and_resolve("Suggest", &index, &mut req).await?;
        let rs = route.routing();
        if rs.shards.len() == 1 {
            return rs.shards[0].suggest(req).await;
        }
        let (meta, _ext, body) = req.into_parts();
        let limit = body.limit as usize;

        let total_shards = rs.shards.len();
        let mut set = tokio::task::JoinSet::new();
        for shard in &rs.shards {
            let shard = shard.clone();
            let r = Request::from_parts(meta.clone(), Extensions::default(), body.clone());
            let permit = self.fanout.clone();
            set.spawn(async move {
                let _permit = permit.acquire_owned().await; // bound concurrent fan-out
                shard.suggest(r).await
            });
        }

        let Fanout { bodies, errors } = gather_responses(set, self.limits.deadline).await;
        let failed = total_shards - bodies.len();
        if bodies.is_empty() {
            return Err(all_shards_failed(
                format!("all {total_shards} shards failed to respond"),
                errors,
            ));
        }
        let mut counts: BTreeMap<String, u64> = BTreeMap::new();
        for body in bodies {
            for s in body.suggestions {
                *counts.entry(s.text).or_default() += s.count;
            }
        }
        let mut suggestions: Vec<Suggestion> = counts
            .into_iter()
            .map(|(text, count)| Suggestion { text, count })
            .collect();
        suggestions.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.text.cmp(&b.text)));
        if limit > 0 && suggestions.len() > limit {
            suggestions.truncate(limit);
        }
        Ok(Response::new(SuggestResponse {
            suggestions,
            failed_shards: failed as u32,
        }))
    }

    /// Hydrate keys. Multi-shard: **route** each key to its owning shard, send each shard only
    /// the keys it owns (not a broadcast), and concatenate the rows.
    pub async fn get_by_key(
        &self,
        mut req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        // Bound the batch before any routing/fan-out — mirrors the Node's self-defense so the
        // gateway rejects an oversized request up front rather than scattering it.
        let n_keys = req.get_ref().keys.len();
        if n_keys > crate::lookup_service::MAX_KEYS {
            return Err(Status::invalid_argument(format!(
                "keys ({n_keys}) exceeds the maximum ({})",
                crate::lookup_service::MAX_KEYS
            )));
        }
        // Resolve the target index: hydration honors `GetByKeyRequest.index`.
        let index = req.get_ref().index.clone();
        let route = self.guard_and_resolve("GetByKey", &index, &mut req).await?;
        let rs = route.routing();
        if rs.shards.len() == 1 {
            return rs.shards[0].get_by_key(req).await;
        }
        let (meta, _ext, body) = req.into_parts();

        // Windowed: a key's coordinate carries no window selector, so we can't route it to a
        // single shard the way ordinal hashing does. Broadcast every key to every window shard — each
        // WindowNode stamps its window id, the node's WindowedLookupService dispatches locally, and the
        // window that owns a key returns its row (others return none). Under the default COORDINATES
        // locator a non-owning window returns just the subset it holds; under PREDICATE it answers a
        // missing key with NotFound, folded into an empty contribution here (correct for single-key
        // hydration; a multi-key request spanning windows under PREDICATE would need per-key window
        // routing — a follow-on).
        if rs.window_routing.is_some() {
            let total = rs.shards.len();
            let mut set = tokio::task::JoinSet::new();
            for shard in &rs.shards {
                let shard = shard.clone();
                let r = Request::from_parts(
                    meta.clone(),
                    Extensions::default(),
                    GetByKeyRequest {
                        keys: body.keys.clone(),
                        columns: body.columns.clone(),
                        index: String::new(), // already routed to this index's shard
                        window: 0,            // stamped per-shard by the WindowNode
                    },
                );
                let permit = self.fanout.clone();
                set.spawn(async move {
                    let _permit = permit.acquire_owned().await;
                    match shard.get_by_key(r).await {
                        // "key not in this window" is expected under broadcast — not a shard failure.
                        Err(s) if s.code() == tonic::Code::NotFound => {
                            Ok(Response::new(GetByKeyResponse::default()))
                        }
                        other => other,
                    }
                });
            }
            let Fanout { bodies, errors } = gather_responses(set, self.limits.deadline).await;
            let failed = total - bodies.len();
            if !body.keys.is_empty() && bodies.is_empty() {
                return Err(all_shards_failed(
                    format!("all {total} windows failed to respond to the hydration"),
                    errors,
                ));
            }
            let rows: Vec<growlerdb_proto::v1::HydratedRow> =
                bodies.into_iter().flat_map(|r| r.rows).collect();
            return Ok(Response::new(GetByKeyResponse {
                rows,
                failed_shards: failed as u32,
            }));
        }

        // Group requested keys by owning shard. A malformed coordinate is rejected loudly
        // — routing it to an arbitrary shard would surface as a spurious
        // "not found" that hides the real cause (a bad key, not a missing row).
        let mut per_shard: Vec<Vec<Coordinates>> = vec![Vec::new(); rs.shards.len()];
        for coord in body.keys {
            let key = CompositeKey::try_from(coord.clone())
                .map_err(|e| Status::invalid_argument(format!("malformed key coordinate: {e}")))?;
            let shard = rs.router.route(&key) as usize;
            // `route` returns an ordinal in `0..router.shards()`, which equals the node
            // count (enforced in `sharded_with`); this never indexes out of bounds.
            debug_assert!(
                shard < rs.shards.len(),
                "router returned an out-of-range shard"
            );
            per_shard[shard].push(coord);
        }

        // Scatter only to the shards that actually own requested keys.
        let mut queried = 0usize;
        let mut set = tokio::task::JoinSet::new();
        for (i, keys) in per_shard.into_iter().enumerate() {
            if keys.is_empty() {
                continue;
            }
            queried += 1;
            let shard = rs.shards[i].clone();
            let r = Request::from_parts(
                meta.clone(),
                Extensions::default(),
                GetByKeyRequest {
                    keys,
                    columns: body.columns.clone(),
                    index: String::new(), // already routed to this index's shard
                    window: 0,
                },
            );
            let permit = self.fanout.clone();
            set.spawn(async move {
                let _permit = permit.acquire_owned().await; // bound concurrent fan-out
                shard.get_by_key(r).await
            });
        }

        let Fanout { bodies, errors } = gather_responses(set, self.limits.deadline).await;
        let failed = queried - bodies.len();
        // Every shard we needed failed/timed out ⇒ error, not an empty (success-shaped) hydration.
        if queried > 0 && bodies.is_empty() {
            return Err(all_shards_failed(
                format!("all {queried} shards holding requested keys failed to respond"),
                errors,
            ));
        }
        let rows: Vec<growlerdb_proto::v1::HydratedRow> =
            bodies.into_iter().flat_map(|r| r.rows).collect();
        Ok(Response::new(GetByKeyResponse {
            rows,
            failed_shards: failed as u32,
        }))
    }

    /// Explain how a query scores one document. Routes to the **single owning shard**
    /// (by key), then fills the per-stage timings + shard counts the leaf can't know: `index_ms` is
    /// the Node's; `hydration_ms` is a best-effort PK lookup here; `total_ms` is the gateway wall
    /// time. `shards_scanned = 1` (only the owner is touched) of `shards_total`.
    pub async fn explain(
        &self,
        mut req: Request<ExplainRequest>,
    ) -> Result<Response<ExplainResponse>, Status> {
        // Resolve the target index (scoping + routing/RBAC), authorized as a read.
        let index = req.get_ref().index.clone();
        let route = self.guard_and_resolve("Search", &index, &mut req).await?;
        let started = std::time::Instant::now();
        let coord =
            req.get_ref().coordinates.clone().ok_or_else(|| {
                Status::invalid_argument("explain requires a document coordinate")
            })?;
        let meta = req.metadata().clone();

        let rs = route.routing();
        let owner = if rs.shards.len() == 1 {
            0
        } else {
            let key = CompositeKey::try_from(coord.clone())
                .map_err(|e| Status::invalid_argument(format!("malformed key coordinate: {e}")))?;
            rs.router.route(&key) as usize
        };
        let mut resp = rs.shards[owner].explain(req).await?.into_inner();
        resp.shards_total = rs.shards.len() as u32;

        // Best-effort hydration timing — the authoritative row the console shows alongside the
        // explanation (forwarding auth metadata so a tenant-scoped read still resolves). Carry the
        // resolved index so the internal hydration routes to the same index.
        let gk = Request::from_parts(
            meta,
            Extensions::default(),
            GetByKeyRequest {
                keys: vec![coord],
                columns: Vec::new(),
                window: 0,
                index: index.clone(),
            },
        );
        let h0 = std::time::Instant::now();
        let hydrated = self.get_by_key(gk).await.is_ok();
        let timings = resp.timings.get_or_insert_with(Default::default);
        timings.hydration_ms = if hydrated {
            h0.elapsed().as_secs_f64() * 1000.0
        } else {
            0.0
        };
        timings.total_ms = started.elapsed().as_secs_f64() * 1000.0;
        Ok(Response::new(resp))
    }

    /// Aggregate over the matched docs. Single-shard: forward verbatim. Multi-shard: scatter
    /// with `partial` set so each shard returns its **mergeable** partial, then `merge` the
    /// partials and finalize. Additive aggs (terms/stats/range/date_histogram) merge exactly;
    /// HLL/DDSketch sketches are approximate but correctly merged (see [`merge_aggregations`]).
    ///
    /// [`merge_aggregations`]: growlerdb_index::merge_aggregations
    pub async fn aggregate(
        &self,
        mut req: Request<AggregateRequest>,
    ) -> Result<Response<AggregateResponse>, Status> {
        // Resolve the target index: aggregate honors `AggregateRequest.index`.
        let index = req.get_ref().index.clone();
        let route = self
            .guard_and_resolve("Aggregate", &index, &mut req)
            .await?;
        {
            let rs = route.routing();
            if rs.shards.len() == 1 {
                return self.under_deadline(rs.shards[0].aggregate(req)).await;
            }
        }
        let (meta, _ext, body) = req.into_parts();

        // Parse the agg spec once (the Gateway needs it to finalize the merge).
        let aggs: std::collections::BTreeMap<String, Agg> = if body.aggs.trim().is_empty() {
            std::collections::BTreeMap::new()
        } else {
            serde_json::from_str(&body.aggs)
                .map_err(|e| Status::invalid_argument(format!("aggs: {e}")))?
        };
        let aggs: Vec<(String, Agg)> = aggs.into_iter().collect();
        // Reject a malformed spec at the boundary, before fanning out.
        growlerdb_core::validate_aggs(&aggs).map_err(Status::invalid_argument)?;

        // Windowed: prune to the windows whose time range can match the query; non-windowed
        // keeps every shard. A windowed query filtered beyond *all* windows prunes to none → a real,
        // empty aggregation (zero counts), not a failure.
        let shards = Self::windows_matching(&route.routing(), &body.query);
        let total_shards = shards.len();
        if total_shards == 0 {
            let results = tokio::task::spawn_blocking(move || {
                growlerdb_index::merge_aggregations(&[], &aggs)
            })
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .map_err(|e| Status::internal(e.to_string()))?;
            let results =
                serde_json::to_string(&results).map_err(|e| Status::internal(e.to_string()))?;
            return Ok(Response::new(AggregateResponse {
                results,
                partial: Vec::new(),
                failed_shards: 0,
            }));
        }

        // Scatter: ask each shard for its partial (mergeable intermediate) result.
        let mut set = tokio::task::JoinSet::new();
        for shard in &shards {
            let shard = shard.clone();
            let r = Request::from_parts(
                meta.clone(),
                Extensions::default(),
                AggregateRequest {
                    query: body.query.clone(),
                    aggs: body.aggs.clone(),
                    index: String::new(), // already routed to this index's shard
                    partial: true,
                    window: 0, // a WindowNode stamps the real selector; ignored otherwise
                },
            );
            let permit = self.fanout.clone();
            set.spawn(async move {
                let _permit = permit.acquire_owned().await; // bound concurrent fan-out
                shard.aggregate(r).await
            });
        }
        let Fanout { bodies, errors } = gather_responses(set, self.limits.deadline).await;
        let failed = total_shards - bodies.len();
        // Every shard failed/timed out ⇒ error, not a zero-count aggregation that reads as a
        // real (but empty) result.
        if bodies.is_empty() {
            return Err(all_shards_failed(
                format!("all {total_shards} shards failed to respond"),
                errors,
            ));
        }
        let partials: Vec<Vec<u8>> = bodies.into_iter().map(|r| r.partial).collect();

        // Merge the partials and finalize (blocking — Tantivy merge/finalize).
        let results = tokio::task::spawn_blocking(move || {
            growlerdb_index::merge_aggregations(&partials, &aggs)
        })
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .map_err(|e| Status::internal(e.to_string()))?;
        let results =
            serde_json::to_string(&results).map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(AggregateResponse {
            results,
            partial: Vec::new(),
            failed_shards: failed as u32,
        }))
    }

    /// Describe an index. Multi-shard: aggregate the per-shard stats (sum `num_docs` +
    /// `generation_count`, max `snapshot`).
    pub async fn describe_index(
        &self,
        mut req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        // Resolve the target index: describe routes by `DescribeIndexRequest.index`.
        let index = req.get_ref().index.clone();
        let route = self
            .guard_and_resolve("DescribeIndex", &index, &mut req)
            .await?;
        let rs = route.routing();
        if rs.shards.len() == 1 {
            return rs.shards[0].describe_index(req).await;
        }
        let (meta, _ext, body) = req.into_parts();

        let total_shards = rs.shards.len();
        let mut set = tokio::task::JoinSet::new();
        for shard in &rs.shards {
            let shard = shard.clone();
            let r = Request::from_parts(meta.clone(), Extensions::default(), body.clone());
            let permit = self.fanout.clone();
            set.spawn(async move {
                let _permit = permit.acquire_owned().await; // bound concurrent fan-out
                shard.describe_index(r).await
            });
        }

        let Fanout { bodies, errors } = gather_responses(set, self.limits.deadline).await;
        let failed = total_shards - bodies.len();
        if bodies.is_empty() {
            return Err(all_shards_failed(
                format!("all {total_shards} shards failed to respond"),
                errors,
            ));
        }
        let mut merged = IndexStats::default();
        let mut any = false;
        // Keep each shard's stats as the per-shard breakdown so load skew is observable.
        let mut per_shard = Vec::with_capacity(bodies.len());
        for body in bodies {
            if let Some(s) = body.stats {
                if !any {
                    merged.name = s.name.clone();
                    merged.checkpoint = s.checkpoint.clone();
                    merged.time_fields = s.time_fields.clone(); // same mapping on every shard
                    merged.sort_fields = s.sort_fields.clone(); // same mapping on every shard
                    merged.vector_fields = s.vector_fields.clone(); // same mapping on every shard
                    any = true;
                }
                merged.num_docs += s.num_docs;
                merged.generation_count += s.generation_count;
                merged.size_bytes += s.size_bytes;
                merged.snapshot = merged.snapshot.max(s.snapshot);
                per_shard.push(s);
            }
        }
        Ok(Response::new(DescribeIndexResponse {
            stats: Some(merged),
            failed_shards: failed as u32,
            per_shard,
        }))
    }

    /// Reindex an index: rebuild it from source and durably swap it live. Unlike the
    /// scatter-gather read RPCs, reindex is a **write-fenced mutation** that must run on the single
    /// Node owning the shard. We route it only for a **single-shard** gateway (the embedded
    /// `serve` deployment the console fronts); a distributed multi-shard reindex needs orchestration
    /// (fence + rebuild + swap per shard) we don't do yet, so it surfaces an honest `Unimplemented`
    /// rather than silently reindexing one shard. The owning Node still enforces the write-fence and
    /// the single-flight guard (a second concurrent reindex → `FailedPrecondition`).
    pub async fn reindex_index(
        &self,
        mut req: Request<ReindexIndexRequest>,
    ) -> Result<Response<ReindexIndexResponse>, Status> {
        let index = req.get_ref().index.clone();
        let route = self
            .guard_and_resolve("ReindexIndex", &index, &mut req)
            .await?;
        let rs = route.routing();
        if rs.shards.len() != 1 {
            return Err(Status::unimplemented(format!(
                "reindex over a {}-shard gateway is not supported; reindex each shard's Node \
                 directly (distributed reindex orchestration is future work)",
                rs.shards.len()
            )));
        }
        rs.shards[0].reindex_index(req).await
    }

    /// Plan (and optionally apply in-place) an index-definition change: diff a candidate
    /// definition vs the served one, reporting reindex-forcing vs in-place changes and, with
    /// `apply`, persisting the in-place ones live. A write-targeted mutation like reindex, so it
    /// routes only for a **single-shard** gateway (the embedded `serve` the console fronts); a
    /// multi-shard alter needs per-shard orchestration we don't do yet → honest `Unimplemented`.
    pub async fn alter_index(
        &self,
        mut req: Request<AlterIndexRequest>,
    ) -> Result<Response<AlterIndexResponse>, Status> {
        let index = req.get_ref().index.clone();
        let route = self
            .guard_and_resolve("AlterIndex", &index, &mut req)
            .await?;
        let rs = route.routing();
        if rs.shards.len() != 1 {
            return Err(Status::unimplemented(format!(
                "alter over a {}-shard gateway is not supported; alter each shard's Node directly \
                 (distributed alter orchestration is future work)",
                rs.shards.len()
            )));
        }
        rs.shards[0].alter_index(req).await
    }

    /// Compact the served shard's segments. Single-shard only (like reindex); a
    /// multi-shard gateway returns `Unimplemented` (compact each shard's Node directly).
    pub async fn compact_index(
        &self,
        mut req: Request<CompactIndexRequest>,
    ) -> Result<Response<CompactIndexResponse>, Status> {
        let index = req.get_ref().index.clone();
        let route = self
            .guard_and_resolve("CompactIndex", &index, &mut req)
            .await?;
        let rs = route.routing();
        if rs.shards.len() != 1 {
            return Err(Status::unimplemented(format!(
                "compact over a {}-shard gateway is not supported; compact each shard's Node directly",
                rs.shards.len()
            )));
        }
        rs.shards[0].compact_index(req).await
    }

    /// Run a backup of the served shard. Single-shard only.
    pub async fn backup_index(
        &self,
        mut req: Request<BackupIndexRequest>,
    ) -> Result<Response<BackupIndexResponse>, Status> {
        let index = req.get_ref().index.clone();
        let route = self
            .guard_and_resolve("BackupIndex", &index, &mut req)
            .await?;
        let rs = route.routing();
        if rs.shards.len() != 1 {
            return Err(Status::unimplemented(format!(
                "backup over a {}-shard gateway is not supported; back up each shard's Node directly",
                rs.shards.len()
            )));
        }
        rs.shards[0].backup_index(req).await
    }

    /// Read the served shard's backup status. Single-shard.
    pub async fn backup_status(
        &self,
        mut req: Request<BackupStatusRequest>,
    ) -> Result<Response<BackupStatusResponse>, Status> {
        let index = req.get_ref().index.clone();
        let route = self
            .guard_and_resolve("BackupStatus", &index, &mut req)
            .await?;
        let rs = route.routing();
        rs.shards[0].backup_status(req).await
    }
}

/// Drain a scatter's [`JoinSet`](tokio::task::JoinSet) into the successful response bodies,
/// enforcing an optional **deadline**: on expiry, abort the outstanding shard tasks
/// and return whatever arrived. A shard that errors, panics, or is aborted simply doesn't
/// contribute a body — so the caller derives `failed = spawned - bodies.len()` and flags
/// `partial` (or returns `UNAVAILABLE` when nothing arrived).
/// The outcome of a scatter/gather fan-out: the bodies of the shards that answered, plus the
/// error [`Status`]es of the shards that answered with an error. A shard that panicked or was
/// aborted at the deadline contributes to neither (there is no status to carry). The errors let
/// the caller distinguish a **bad request** (every shard rejects the same query the same way)
/// from a **transient/server failure** — see [`all_shards_failed`].
struct Fanout<T> {
    bodies: Vec<T>,
    errors: Vec<Status>,
}

async fn gather_responses<T: Send + 'static>(
    mut set: tokio::task::JoinSet<Result<Response<T>, Status>>,
    deadline: Option<Duration>,
) -> Fanout<T> {
    let mut bodies = Vec::new();
    let mut errors = Vec::new();
    let until = deadline.map(|d| tokio::time::Instant::now() + d);
    loop {
        let joined = match until {
            Some(at) => match tokio::time::timeout_at(at, set.join_next()).await {
                Ok(Some(joined)) => joined,
                Ok(None) => break, // every shard finished
                Err(_) => {
                    set.abort_all(); // deadline hit — drop the slow shards, return what we have
                    break;
                }
            },
            None => match set.join_next().await {
                Some(joined) => joined,
                None => break,
            },
        };
        match joined {
            Ok(Ok(resp)) => bodies.push(resp.into_inner()),
            Ok(Err(status)) => errors.push(status), // a shard rejected/failed the RPC
            Err(_join_err) => {}                    // panicked or aborted — no status to surface
        }
    }
    Fanout { bodies, errors }
}

/// gRPC codes that map to a **4xx**: the request itself is at fault, so a retry won't help.
/// Exactly the codes the OpenSearch adapter renders as 4xx (`rest.rs` code→HTTP) — notably
/// **not** `Unimplemented` (501) or the `_ => 500` fallthrough (e.g. `OutOfRange`), which are
/// server-side, so a fan-out that saw only those keeps the retryable `unavailable`.
fn is_client_error(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::InvalidArgument
            | tonic::Code::FailedPrecondition
            | tonic::Code::NotFound
            | tonic::Code::PermissionDenied
            | tonic::Code::Unauthenticated
    )
}

/// The error to return when a fan-out produced **no usable body**. Every shard ran the *same*
/// request against the *same* schema, so a bad query fails them all identically: when every
/// reported failure is a client error (4xx), surface the first verbatim so the caller learns
/// *why* (e.g. `ip_cidr requires an IP field`, `sort needs a … fast field`) instead of an
/// opaque, retryable 500. If any failure is server-side/transient — or shards vanished without a
/// status (panic/deadline) leaving no reported error at all — keep the retryable `unavailable`,
/// which is honest about "the query might be fine; the cluster couldn't answer it".
fn all_shards_failed(fallback: String, errors: Vec<Status>) -> Status {
    if !errors.is_empty() && errors.iter().all(|s| is_client_error(s.code())) {
        return errors.into_iter().next().unwrap();
    }
    Status::unavailable(fallback)
}

/// A collapse group's stable key — the canonical index string of the hit's `group` value (the
/// same `to_index_string` the store folds by). `None` if the hit carries no decodable group.
fn group_key(h: &growlerdb_proto::v1::SearchHit) -> Option<String> {
    let g = h.group.clone()?;
    growlerdb_core::Value::try_from(g)
        .ok()
        .map(|v| v.to_index_string())
}

/// Build the keyset cursor that resumes strictly after `h`: its sort tuple plus its composite
/// key (the unique tiebreaker). `None` if the hit has no decodable coordinates — without a key
/// the cursor wouldn't be a total position, so we'd rather emit no cursor than an ambiguous one.
fn search_after_from_hit(h: &growlerdb_proto::v1::SearchHit) -> Option<SearchAfter> {
    let key = CompositeKey::try_from(h.coordinates.clone()?).ok()?;
    let sort_values = h
        .sort_values
        .iter()
        .cloned()
        .map(|v| SortValue::try_from(v).unwrap_or(SortValue::Missing))
        .collect();
    Some(SearchAfter { sort_values, key })
}

/// The encoded composite key of a hit — the cross-shard merge's final, unique tiebreaker
/// (the same total order the store applies intra-shard). A hit with no/undecodable
/// coordinates yields an empty key (sorts first), never a panic.
fn hit_key(h: &growlerdb_proto::v1::SearchHit) -> Vec<u8> {
    h.coordinates
        .clone()
        .and_then(|c| CompositeKey::try_from(c).ok())
        .map(|k| k.encode())
        .unwrap_or_default()
}

/// Merge field-sorted hits from many shards into one globally-ordered page. Orders by
/// the request's sort tuple via the shared [`cmp_sort_value`] (so the cross-shard order
/// matches the store's cross-generation order), with the encoded composite key as the
/// final tiebreaker — a total, deterministic order, the same rule the store applies
/// intra-shard. A hit missing a value for a key (e.g. from an older Node that didn't
/// populate `sort_values`) is treated as `Missing` (sorts last), never a panic.
fn merge_field_sorted(
    hits: Vec<growlerdb_proto::v1::SearchHit>,
    sort: &[growlerdb_proto::v1::Sort],
) -> Vec<growlerdb_proto::v1::SearchHit> {
    let orders: Vec<SortOrder> = sort
        .iter()
        .map(|s| {
            if s.descending {
                SortOrder::Desc
            } else {
                SortOrder::Asc
            }
        })
        .collect();
    // Decode each hit's sort values + encoded key once, not per comparison.
    let mut decorated: Vec<(Vec<SortValue>, Vec<u8>, growlerdb_proto::v1::SearchHit)> = hits
        .into_iter()
        .map(|h| {
            let vals: Vec<SortValue> = h
                .sort_values
                .iter()
                .cloned()
                .map(|v| SortValue::try_from(v).unwrap_or(SortValue::Missing))
                .collect();
            let key_enc = hit_key(&h);
            (vals, key_enc, h)
        })
        .collect();
    decorated.sort_by(|a, b| {
        for (i, order) in orders.iter().enumerate() {
            let av = a.0.get(i).unwrap_or(&SortValue::Missing);
            let bv = b.0.get(i).unwrap_or(&SortValue::Missing);
            let c = cmp_sort_value(av, bv, *order);
            if c != std::cmp::Ordering::Equal {
                return c;
            }
        }
        a.1.cmp(&b.1) // composite key ascending — the final, unique tiebreaker
    });
    decorated.into_iter().map(|(_, _, h)| h).collect()
}

/// The standard **RRF constant** (`k = 60`, Cormack et al. SIGIR 2009) for the Gateway's hybrid
/// fusion — the same value the engine's embedded `rrf_fuse` uses. Dampens how much a top rank in
/// any single arm contributes, so a doc must rank well across arms to rise.
const HYBRID_RRF_K: usize = 60;

/// Merge score-ranked hits from many shards into one global top-`k`: order by score descending
/// with the encoded composite key as the deterministic tiebreaker, dedupe by key (a reshard can
/// briefly place a key on two shards), and keep the first `k` (0 = keep all). This is the KNN /
/// pure-relevance merge — the same rule the lexical `search_inner` score branch applies.
fn merge_score_topk(hits: Vec<SearchHit>, k: usize) -> Vec<SearchHit> {
    let mut decorated: Vec<(Vec<u8>, SearchHit)> =
        hits.into_iter().map(|h| (hit_key(&h), h)).collect();
    decorated.sort_by(|a, b| {
        b.1.score
            .partial_cmp(&a.1.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    let mut seen = std::collections::HashSet::new();
    let mut out: Vec<SearchHit> = Vec::new();
    for (key, hit) in decorated {
        if !seen.insert(key) {
            continue;
        }
        out.push(hit);
        if k > 0 && out.len() == k {
            break;
        }
    }
    out
}

/// **Reciprocal Rank Fusion** of several ranked [`SearchHit`] lists into one ranking — the
/// Gateway-side analogue of the engine's `rrf_fuse` (over `Hit`), operating on the already
/// cross-shard-merged wire hits. For each list, the hit at 0-based `rank` contributes
/// `1 / (k_rrf + rank + 1)` to that document's fused score (keyed by its encoded composite key);
/// contributions sum across lists. The fused list sorts by score descending with the encoded key
/// as a stable tiebreaker, then truncates to `limit` (0 = keep all). A doc present in only one arm
/// still appears; one high in *both* outranks it. The representative hit prefers one carrying
/// display `fields`/`highlight` (a lexical hit) over a bare vector hit, so it renders without
/// hydration.
fn rrf_fuse_hits(lists: &[&[SearchHit]], k_rrf: usize, limit: usize) -> Vec<SearchHit> {
    let mut acc: HashMap<Vec<u8>, (f64, SearchHit)> = HashMap::new();
    for list in lists {
        for (rank, hit) in list.iter().enumerate() {
            let contribution = 1.0 / (k_rrf as f64 + rank as f64 + 1.0);
            let entry = acc
                .entry(hit_key(hit))
                .or_insert_with(|| (0.0, hit.clone()));
            entry.0 += contribution;
            if entry.1.fields.is_empty()
                && entry.1.highlight.is_empty()
                && (!hit.fields.is_empty() || !hit.highlight.is_empty())
            {
                entry.1 = hit.clone();
            }
        }
    }
    let mut fused: Vec<(Vec<u8>, SearchHit)> = acc
        .into_iter()
        .map(|(key, (score, mut hit))| {
            hit.score = score;
            (key, hit)
        })
        .collect();
    fused.sort_by(|a, b| {
        b.1.score
            .partial_cmp(&a.1.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    if limit > 0 {
        fused.truncate(limit);
    }
    fused.into_iter().map(|(_, h)| h).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_proto::v1::IndexStats;

    /// A stand-in Node that answers `describe_index` and leaves the rest unimplemented —
    /// enough to prove the Gateway routes through `dyn Node` and propagates results/errors
    /// verbatim, without standing up a real shard.
    struct FakeNode;

    #[tonic::async_trait]
    impl Node for FakeNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            Err(Status::unimplemented("search"))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            req: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Ok(Response::new(DescribeIndexResponse {
                stats: Some(IndexStats {
                    name: req.into_inner().index,
                    ..Default::default()
                }),
                failed_shards: 0,
                per_shard: Vec::new(),
            }))
        }
        async fn reindex_index(
            &self,
            _: Request<ReindexIndexRequest>,
        ) -> Result<Response<ReindexIndexResponse>, Status> {
            // A sentinel doc_count proves the Gateway routed reindex to this Node.
            Ok(Response::new(ReindexIndexResponse {
                doc_count: 7,
                snapshot: 42,
            }))
        }
        async fn alter_index(
            &self,
            _: Request<AlterIndexRequest>,
        ) -> Result<Response<AlterIndexResponse>, Status> {
            // A sentinel in-place change proves the Gateway routed alter to this Node.
            Ok(Response::new(AlterIndexResponse {
                plan: Some(growlerdb_proto::v1::AlterPlan {
                    is_noop: false,
                    requires_reindex: false,
                    reindex_reasons: vec![],
                    in_place_changes: vec!["sentinel".into()],
                }),
            }))
        }
    }

    /// A Node returning a fixed set of `(id, score)` search hits and `num_docs`.
    struct ShardNode {
        hits: Vec<(&'static str, f64)>,
        num_docs: u64,
    }

    #[tonic::async_trait]
    impl Node for ShardNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            use growlerdb_proto::v1::{value::Kind, Coordinates, Field, SearchHit, Value};
            let hits = self
                .hits
                .iter()
                .map(|(id, score)| SearchHit {
                    coordinates: Some(Coordinates {
                        partition: vec![],
                        identifier: vec![Field {
                            name: "id".into(),
                            value: Some(Value {
                                kind: Some(Kind::Str((*id).into())),
                            }),
                        }],
                    }),
                    score: *score,
                    group: None,
                    group_count: 0,
                    sort_values: Vec::new(),
                    fields: vec![],
                    highlight: Default::default(),
                })
                .collect::<Vec<_>>();
            let total = hits.len() as u64;
            Ok(Response::new(SearchResponse {
                hits,
                total,
                next_cursor: Vec::new(),
                partial: false,
                ..Default::default()
            }))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Ok(Response::new(DescribeIndexResponse {
                stats: Some(IndexStats {
                    name: "docs".into(),
                    num_docs: self.num_docs,
                    ..Default::default()
                }),
                failed_shards: 0,
                per_shard: Vec::new(),
            }))
        }
    }

    fn id_of(hit: &growlerdb_proto::v1::SearchHit) -> String {
        match hit.coordinates.as_ref().unwrap().identifier[0]
            .value
            .as_ref()
            .unwrap()
            .kind
            .clone()
            .unwrap()
        {
            growlerdb_proto::v1::value::Kind::Str(s) => s,
            other => panic!("unexpected id kind: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gateway_routes_to_the_node() {
        let gw = Gateway::new(Arc::new(FakeNode));

        let resp = gw
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: "docs".into(),
            }))
            .await
            .unwrap();
        assert_eq!(resp.into_inner().stats.unwrap().name, "docs");

        let err = gw
            .search(Request::new(SearchRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reindex_routes_for_a_single_shard_but_refuses_multi_shard() {
        // Single-shard gateway → reindex is routed to the owning Node (sentinel doc_count).
        let one = Gateway::new(Arc::new(FakeNode));
        let resp = one
            .reindex_index(Request::new(ReindexIndexRequest::default()))
            .await
            .unwrap();
        assert_eq!(resp.into_inner().doc_count, 7);

        // Multi-shard gateway → honest Unimplemented (no silent single-shard reindex).
        let many = Gateway::sharded(vec![Arc::new(FakeNode), Arc::new(FakeNode)]);
        let err = many
            .reindex_index(Request::new(ReindexIndexRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("2-shard"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alter_routes_for_a_single_shard_but_refuses_multi_shard() {
        // Single-shard gateway → alter is routed to the owning Node (sentinel in-place change).
        let one = Gateway::new(Arc::new(FakeNode));
        let resp = one
            .alter_index(Request::new(AlterIndexRequest::default()))
            .await
            .unwrap();
        assert_eq!(
            resp.into_inner().plan.unwrap().in_place_changes,
            ["sentinel"]
        );

        // Multi-shard gateway → honest Unimplemented (no silent single-shard alter).
        let many = Gateway::sharded(vec![Arc::new(FakeNode), Arc::new(FakeNode)]);
        let err = many
            .alter_index(Request::new(AlterIndexRequest::default()))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
        assert!(err.message().contains("2-shard"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn search_scatter_gathers_and_merges_by_score() {
        // Two shards; interleaved scores. The merged top-3 is globally score-sorted.
        let a = Arc::new(ShardNode {
            hits: vec![("a1", 9.0), ("a2", 3.0)],
            num_docs: 2,
        });
        let b = Arc::new(ShardNode {
            hits: vec![("b1", 7.0), ("b2", 5.0), ("b3", 1.0)],
            num_docs: 3,
        });
        let gw = Gateway::sharded(vec![a, b]);
        assert_eq!(gw.shard_count(), 2);

        let resp = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 3,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        // Global top-3 by score: a1(9), b1(7), b2(5).
        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["a1", "b1", "b2"]);
        // total is the global match count (2 + 3 across shards), not the page size (3).
        assert_eq!(resp.total, 5);
        assert!(!resp.partial);
        // Both shards were scanned (no pruning on an unfiltered query), out of 2 total.
        assert_eq!(resp.shards_scanned, 2);
        assert_eq!(resp.shards_total, 2);

        // Describe aggregates num_docs across shards.
        let stats = gw
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .stats
            .unwrap();
        assert_eq!(stats.num_docs, 5);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn serving_scopes_search_to_the_named_index() {
        // A Gateway that declares it serves `events`.
        let node = Arc::new(ShardNode {
            hits: vec![("a1", 9.0)],
            num_docs: 1,
        });
        let gw = Gateway::new(node).serving("events");
        let q = |index: &str| {
            Request::new(SearchRequest {
                query: "x".into(),
                limit: 5,
                index: index.into(),
                ..Default::default()
            })
        };

        // Empty index → the served index; matching name → served.
        assert_eq!(gw.search(q("")).await.unwrap().into_inner().hits.len(), 1);
        assert_eq!(
            gw.search(q("events"))
                .await
                .unwrap()
                .into_inner()
                .hits
                .len(),
            1
        );

        // A different index → NOT_FOUND, not a silent search of the wrong index.
        let err = gw.search(q("other")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        assert!(err.message().contains("other"));

        // Without `serving`, the index field is ignored.
        let open = Gateway::new(Arc::new(ShardNode {
            hits: vec![("a1", 9.0)],
            num_docs: 1,
        }));
        assert_eq!(
            open.search(q("anything"))
                .await
                .unwrap()
                .into_inner()
                .hits
                .len(),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_down_shard_flags_partial_not_a_silent_gap() {
        // One healthy shard + one that errors every search.
        let healthy = Arc::new(ShardNode {
            hits: vec![("a1", 9.0), ("a2", 4.0)],
            num_docs: 2,
        });
        let gw = Gateway::sharded(vec![healthy, Arc::new(FakeNode)]);

        let resp = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 10,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        // The healthy shard's hits are returned, and the page is flagged incomplete.
        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["a1", "a2"]);
        assert!(resp.partial);
    }

    /// A Node that fails **every** RPC with a fixed status — models a shard that rejects a
    /// query (a bad query shape ⇒ `InvalidArgument`) or is genuinely down (`Unavailable`).
    struct RejectNode {
        code: tonic::Code,
        message: &'static str,
    }

    #[tonic::async_trait]
    impl Node for RejectNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            Err(Status::new(self.code, self.message))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::new(self.code, self.message))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::new(self.code, self.message))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::new(self.code, self.message))
        }
    }

    /// TASK-209 regression: on a multi-shard index a genuinely-unsupported query makes *every*
    /// shard reject it with the same client error (a bad query shape is uniform — same schema on
    /// each shard). The fan-out must surface that **4xx verbatim**, not collapse it into an
    /// opaque, retryable `unavailable` (which the OpenSearch adapter renders as a 500). Both the
    /// http_logs `cidr_clientip` and `topk_hydrated` shapes that failed on the live scale run are
    /// this case: `ip_cidr` on a mis-mapped TEXT field, and a sort on a non-`fast` field.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_shards_rejecting_a_bad_query_surface_the_client_4xx_not_a_500() {
        // cidr_clientip against a field the deployed index auto-mapped to TEXT instead of IP.
        let cidr_msg = "ip_cidr requires an IP field, got `client_ip`";
        let gw = Gateway::sharded(vec![
            Arc::new(RejectNode {
                code: tonic::Code::InvalidArgument,
                message: cidr_msg,
            }),
            Arc::new(RejectNode {
                code: tonic::Code::InvalidArgument,
                message: cidr_msg,
            }),
        ]);
        let err = gw
            .search(Request::new(SearchRequest {
                query: "client_ip:211.0.0.0/8".into(),
                limit: 0,
                ..Default::default()
            }))
            .await
            .expect_err("a bad query must be an error, not an empty page");
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "4xx, not 5xx");
        assert_eq!(
            err.message(),
            cidr_msg,
            "the shard's reason is surfaced verbatim"
        );

        // topk_hydrated: a sort on a field the deployed index built non-`fast`.
        let sort_msg = "sort needs a numeric/date/keyword fast field, got `response_time_ms`";
        let gw = Gateway::sharded(vec![
            Arc::new(RejectNode {
                code: tonic::Code::InvalidArgument,
                message: sort_msg,
            }),
            Arc::new(RejectNode {
                code: tonic::Code::InvalidArgument,
                message: sort_msg,
            }),
        ]);
        let err = gw
            .search(Request::new(SearchRequest {
                query: "user_agent:Chrome".into(),
                limit: 20,
                sort: rank_sort(true),
                ..Default::default()
            }))
            .await
            .expect_err("a non-fast sort must be an error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert_eq!(err.message(), sort_msg);
    }

    /// The 4xx surfacing is gated on *every* failure being a client error. A single server-side
    /// or transient failure in the mix (or all of them) keeps the retryable `unavailable`, so a
    /// real outage is never mislabeled as the client's fault.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_server_error_among_the_failures_keeps_unavailable() {
        // One shard rejects the query (client error), the other is genuinely down (server error).
        let gw = Gateway::sharded(vec![
            Arc::new(RejectNode {
                code: tonic::Code::InvalidArgument,
                message: "bad query",
            }),
            Arc::new(RejectNode {
                code: tonic::Code::Internal,
                message: "boom",
            }),
        ]);
        let err = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 10,
                ..Default::default()
            }))
            .await
            .expect_err("all shards failed");
        assert_eq!(
            err.code(),
            tonic::Code::Unavailable,
            "a server error must not be masked as a 4xx"
        );
        assert!(err.message().contains("shards failed to respond"));
    }

    /// A Node returning **field-sorted** hits, each carrying its `sort_values` (numeric;
    /// an empty inner vec ⇒ no value ⇒ `Missing`). Exercises the Gateway's cross-shard
    /// field-sort merge — the shard ranks locally and reports the sort cells.
    struct SortNode {
        hits: Vec<(&'static str, Vec<f64>)>,
    }

    #[tonic::async_trait]
    impl Node for SortNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            use growlerdb_proto::v1::{
                sort_value, value::Kind, Coordinates, Field, SearchHit, SortValue, Value,
            };
            let hits = self
                .hits
                .iter()
                .map(|(id, vals)| SearchHit {
                    coordinates: Some(Coordinates {
                        partition: vec![],
                        identifier: vec![Field {
                            name: "id".into(),
                            value: Some(Value {
                                kind: Some(Kind::Str((*id).into())),
                            }),
                        }],
                    }),
                    score: 0.0,
                    group: None,
                    group_count: 0,
                    fields: vec![],
                    sort_values: vals
                        .iter()
                        .map(|v| SortValue {
                            kind: Some(sort_value::Kind::Num(*v)),
                        })
                        .collect(),
                    highlight: Default::default(),
                })
                .collect::<Vec<_>>();
            Ok(Response::new(SearchResponse {
                hits,
                total: 0,
                next_cursor: Vec::new(),
                partial: false,
                ..Default::default()
            }))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("describe"))
        }
    }

    fn rank_sort(descending: bool) -> Vec<growlerdb_proto::v1::Sort> {
        vec![growlerdb_proto::v1::Sort {
            field: "rank".into(),
            descending,
        }]
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn field_sort_merges_across_shards_by_sort_value_not_score() {
        // Each shard is locally sorted by `rank` asc; the globally-merged page interleaves
        // them by sort value (NOT by score — every hit has score 0).
        let a = Arc::new(SortNode {
            hits: vec![("a1", vec![1.0]), ("a3", vec![3.0])],
        });
        let b = Arc::new(SortNode {
            hits: vec![("b2", vec![2.0]), ("b4", vec![4.0])],
        });
        let gw = Gateway::sharded(vec![a, b]);

        let resp = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 3,
                sort: rank_sort(false),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["a1", "b2", "a3"]); // global asc top-3

        // Descending flips the order.
        let resp = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 3,
                sort: rank_sort(true),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["b4", "a3", "b2"]); // global desc top-3
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn field_sort_missing_sorts_last_and_ties_break_by_key() {
        // `bx` has no sort value (Missing → last in either direction); `a` and `z` tie on
        // rank 5 → broken by composite key ascending ("a" < "z").
        let a = Arc::new(SortNode {
            hits: vec![("z", vec![5.0]), ("bx", vec![])],
        });
        let b = Arc::new(SortNode {
            hits: vec![("a", vec![5.0])],
        });
        let gw = Gateway::sharded(vec![a, b]);

        let resp = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 10,
                sort: rank_sort(false),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["a", "z", "bx"]); // tie→key asc, then missing last
    }

    /// A Node that records the ids of the keys it was asked to hydrate (and returns no rows),
    /// so a test can assert which keys the Gateway routed to which shard.
    struct RecordingNode {
        seen: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[tonic::async_trait]
    impl Node for RecordingNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            Err(Status::unimplemented("search"))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            req: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            let req = req.into_inner();
            let mut seen = self.seen.lock().unwrap();
            for c in &req.keys {
                seen.push(coord_id(c));
            }
            Ok(Response::new(GetByKeyResponse {
                rows: Vec::new(),
                failed_shards: 0,
            }))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("describe"))
        }
    }

    fn coord_id(c: &Coordinates) -> String {
        match c.identifier[0]
            .value
            .as_ref()
            .unwrap()
            .kind
            .clone()
            .unwrap()
        {
            growlerdb_proto::v1::value::Kind::Str(s) => s,
            other => panic!("unexpected id kind: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_by_key_routes_each_key_to_its_owning_shard() {
        use growlerdb_core::Value;
        use std::sync::Mutex;

        let router = ShardRouter::hashed(2);
        let seen0 = Arc::new(Mutex::new(Vec::new()));
        let seen1 = Arc::new(Mutex::new(Vec::new()));
        let gw = Gateway::sharded_with(
            vec![
                Arc::new(RecordingNode {
                    seen: seen0.clone(),
                }),
                Arc::new(RecordingNode {
                    seen: seen1.clone(),
                }),
            ],
            router.clone(),
        );

        let key = |id: &str| CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let ids: Vec<String> = (0..20).map(|i| format!("k{i}")).collect();

        // Expected placement, computed with the same router.
        let (mut expect0, mut expect1) = (Vec::new(), Vec::new());
        for id in &ids {
            if router.route(&key(id)) == 0 {
                expect0.push(id.clone());
            } else {
                expect1.push(id.clone());
            }
        }
        // The split is meaningful (both shards own some keys).
        assert!(!expect0.is_empty() && !expect1.is_empty());

        let keys: Vec<Coordinates> = ids.iter().map(|id| (&key(id)).into()).collect();
        gw.get_by_key(Request::new(GetByKeyRequest {
            window: 0,
            keys,
            columns: Vec::new(),
            index: String::new(),
        }))
        .await
        .unwrap();

        let mut got0 = seen0.lock().unwrap().clone();
        let mut got1 = seen1.lock().unwrap().clone();
        got0.sort();
        got1.sort();
        expect0.sort();
        expect1.sort();
        assert_eq!(got0, expect0);
        assert_eq!(got1, expect1);
    }

    /// A LocalNode over a fresh shard holding `(id, cat)` rows (cat is a KEYWORD fast field).
    fn agg_node(root: &std::path::Path, rows: &[(&str, &str)]) -> Arc<dyn Node> {
        use growlerdb_core::{
            CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
            SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
        };
        use growlerdb_index::{LocalIndexStore, ShardId};
        use std::collections::BTreeMap;

        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("cat", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: cat, type: KEYWORD, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let docs: Vec<LocatedDoc> = rows
            .iter()
            .map(|(id, cat)| {
                let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(*id))]);
                let mut f = BTreeMap::new();
                f.insert("id".to_string(), Value::from(*id));
                f.insert("cat".to_string(), Value::from(*cat));
                LocatedDoc {
                    doc: Document::new(key, f),
                    iceberg_file: "f".into(),
                    row_position: 0,
                }
            })
            .collect();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
        let shard = Arc::new(shard);
        crate::LocalNode::new(
            crate::SearchService::new(shard.clone()),
            crate::SuggestService::new(shard.clone()),
            crate::LookupService::new(
                shard.clone(),
                growlerdb_source::IcebergConfig::local(),
                "g.docs",
            ),
            crate::AdminService::new(shard, "docs"),
        )
        .shared()
    }

    /// A LocalNode over a shard holding exactly `ids` (KEYWORD `id`) — one shard of a layout.
    fn id_shard_node(root: &std::path::Path, ids: &[String]) -> Arc<dyn Node> {
        use growlerdb_core::{
            CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
            SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
        };
        use growlerdb_index::{LocalIndexStore, ShardId};
        use std::collections::BTreeMap;

        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let docs: Vec<LocatedDoc> = ids
            .iter()
            .map(|id| {
                let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id.as_str()))]);
                let mut f = BTreeMap::new();
                f.insert("id".to_string(), Value::from(id.as_str()));
                LocatedDoc {
                    doc: Document::new(key, f),
                    iceberg_file: "f".into(),
                    row_position: 0,
                }
            })
            .collect();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
        let shard = Arc::new(shard);
        crate::LocalNode::new(
            crate::SearchService::new(shard.clone()),
            crate::SuggestService::new(shard.clone()),
            crate::LookupService::new(
                shard.clone(),
                growlerdb_source::IcebergConfig::local(),
                "g.docs",
            ),
            crate::AdminService::new(shard, "docs"),
        )
        .shared()
    }

    /// Build a [`Gateway`] over a `router`'s layout: split `ids` into one shard per ordinal by
    /// `router.route` (the same split a reshard rebuild applies), seed a real shard per group, and
    /// front them. `dirs` must outlive the gateway (owns the on-disk shards).
    fn layout_gateway(
        dirs: &[tempfile::TempDir],
        ids: &[String],
        router: growlerdb_core::ShardRouter,
    ) -> Gateway {
        use growlerdb_core::{CompositeKey, Value};
        let n = router.shards() as usize;
        assert_eq!(dirs.len(), n);
        let mut groups: Vec<Vec<String>> = vec![Vec::new(); n];
        for id in ids {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id.as_str()))]);
            groups[router.route(&key) as usize].push(id.clone());
        }
        let nodes: Vec<Arc<dyn Node>> = dirs
            .iter()
            .zip(&groups)
            .map(|(d, g)| id_shard_node(d.path(), g))
            .collect();
        Gateway::sharded_with(nodes, router)
    }

    #[test]
    fn partition_pinned_search_prunes_to_one_shard() {
        use growlerdb_core::{CompositeKey, ShardRouter, Value};
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let router = ShardRouter::partitioned(2);
        let gw =
            layout_gateway(&dirs, &[], router.clone()).with_partition_fields(vec!["region".into()]);
        let route = gw.single().clone();
        let rs = route.routing();

        // A search pinning the partition field routes to exactly the shard the router picks — the
        // same routing get_by_key would use for a key with that partition.
        for region in ["us", "eu", "apac", "sa"] {
            let owner = router.route(&CompositeKey::new(
                vec![("region".into(), Value::Str(region.into()))],
                Vec::new(),
            )) as usize;
            assert_eq!(
                Gateway::partition_prune(&route, &format!("region:{region}"), &rs),
                Some(owner)
            );
            // Pinned via an AND clause alongside another predicate — still routable.
            assert_eq!(
                Gateway::partition_prune(&route, &format!("region:{region} AND body:x"), &rs),
                Some(owner)
            );
        }
        // No partition pin → fan out (None).
        assert_eq!(Gateway::partition_prune(&route, "body:x", &rs), None);
        // Partition field only under OR (should) doesn't pin every match → fan out.
        assert_eq!(
            Gateway::partition_prune(&route, "region:us OR body:x", &rs),
            None
        );

        // target_shards reflects it: a pinned search hits one shard, an unpinned one all.
        let pinned = SearchRequest {
            query: "region:us".into(),
            ..Default::default()
        };
        let unpinned = SearchRequest {
            query: "body:x".into(),
            ..Default::default()
        };
        assert_eq!(Gateway::target_shards(&route, &pinned).len(), 1);
        assert_eq!(Gateway::target_shards(&route, &unpinned).len(), 2);
    }

    #[test]
    fn partition_prune_off_without_declared_fields() {
        // Without with_partition_fields (or on a hash index), pruning never engages — fan out.
        let dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let gw = layout_gateway(&dirs, &[], growlerdb_core::ShardRouter::partitioned(2));
        let route = gw.single().clone();
        let rs = route.routing();
        assert_eq!(Gateway::partition_prune(&route, "region:us", &rs), None);
    }

    async fn all_ids(gw: &Gateway, limit: u32) -> Vec<String> {
        let resp = gw
            .search(Request::new(SearchRequest {
                query: "*:*".into(),
                limit,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!resp.partial, "search came back partial");
        let mut ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        ids.sort();
        ids
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn swap_routing_hot_reloads_the_topology() {
        use growlerdb_core::{BucketMap, CompositeKey, RoutingStrategy, ShardRouter, Value};
        // A running Gateway picks up a reshard's new map + node set without a restart.
        let (t0, t1) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let ids: Vec<String> = (0..200).map(|i| format!("k{i}")).collect();
        let mut expected = ids.clone();
        expected.sort();

        // Start as a 2-shard cluster (balanced(2)).
        let gw = layout_gateway(
            &[t0, t1],
            &ids,
            ShardRouter::bucketed(RoutingStrategy::Hash, BucketMap::balanced(2)),
        );
        assert_eq!(gw.shard_count(), 2);
        assert_eq!(all_ids(&gw, 1000).await, expected);

        // Build a fresh 3-shard topology and hot-swap it in — same `gw`, no restart.
        let three: Vec<tempfile::TempDir> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let plan = BucketMap::balanced(2).reassign(3);
        let new_router = ShardRouter::bucketed(RoutingStrategy::Hash, plan.map.clone());
        let new_nodes = {
            // Re-derive each new shard's docs (the post-reshard split).
            let mut groups: Vec<Vec<String>> = vec![Vec::new(); 3];
            for id in &ids {
                let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id.as_str()))]);
                groups[new_router.route(&key) as usize].push(id.clone());
            }
            three
                .iter()
                .zip(&groups)
                .map(|(d, g)| id_shard_node(d.path(), g))
                .collect::<Vec<_>>()
        };
        gw.swap_routing(new_nodes, new_router);

        // The same Gateway now fronts 3 shards and still returns every doc exactly once.
        assert_eq!(gw.shard_count(), 3);
        assert_eq!(
            all_ids(&gw, 1000).await,
            expected,
            "post-swap search lost/dup'd a doc"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn overlapping_shards_dedupe_during_a_reshard_window() {
        // Mid-cutover a moved bucket's docs live on both its old and new shard. A broadcast
        // search hits both; the Gateway must return each doc once, not twice.
        let (ta, tb) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let shared: Vec<String> = vec!["x".into(), "y".into(), "z".into()];
        let gw = Gateway::sharded(vec![
            id_shard_node(ta.path(), &shared), // both shards hold the SAME docs
            id_shard_node(tb.path(), &shared),
        ]);
        let resp = gw
            .search(Request::new(SearchRequest {
                query: "*:*".into(),
                limit: 100,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let mut ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        ids.sort();
        assert_eq!(
            ids,
            vec!["x", "y", "z"],
            "duplicates across shards weren't deduped"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reshard_2_to_3_keeps_every_doc_findable_exactly_once() {
        use growlerdb_core::{BucketMap, RoutingStrategy, ShardRouter};
        // The in-process cutover: a real multi-shard Gateway over real shards. Build the
        // 2-shard layout, reshard to 3 by rebuilding shards split under the reassigned map, and
        // assert every doc is searchable exactly once before AND after — no lost/duplicate/missing
        // reads across the cutover.
        let ids: Vec<String> = (0..300).map(|i| format!("k{i}")).collect();
        let mut expected = ids.clone();
        expected.sort();

        // Before: 2 shards under balanced(2).
        let before = BucketMap::balanced(2);
        let old_dirs: Vec<_> = (0..2).map(|_| tempfile::tempdir().unwrap()).collect();
        let gw2 = layout_gateway(
            &old_dirs,
            &ids,
            ShardRouter::bucketed(RoutingStrategy::Hash, before.clone()),
        );
        assert_eq!(gw2.shard_count(), 2);
        assert_eq!(
            all_ids(&gw2, 1000).await,
            expected,
            "2-shard layout lost/dup'd a doc"
        );

        // Cutover: reshard 2 → 3 over the bounded reassignment, rebuilding each shard's docs.
        let plan = before.reassign(3);
        let new_dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
        let gw3 = layout_gateway(
            &new_dirs,
            &ids,
            ShardRouter::bucketed(RoutingStrategy::Hash, plan.map),
        );
        assert_eq!(gw3.shard_count(), 3);
        assert_eq!(
            all_ids(&gw3, 1000).await,
            expected,
            "3-shard layout lost/dup'd a doc"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aggregate_merges_term_buckets_across_shards() {
        use std::collections::BTreeMap;
        let ta = tempfile::tempdir().unwrap();
        let tb = tempfile::tempdir().unwrap();
        // cat values overlap on "y": A={x,x,y}, B={y,z}.
        let gw = Gateway::sharded(vec![
            agg_node(ta.path(), &[("1", "x"), ("2", "x"), ("3", "y")]),
            agg_node(tb.path(), &[("4", "y"), ("5", "z")]),
        ]);

        let resp = gw
            .aggregate(Request::new(AggregateRequest {
                query: "cat:x OR cat:y OR cat:z".into(),
                aggs: r#"{"by_cat": {"Terms": {"field": "cat", "size": 10}}}"#.into(),
                partial: false,
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        // The Gateway scattered with partial=true, merged the partials, and finalized.
        let results: serde_json::Value = serde_json::from_str(&resp.results).unwrap();
        let mut counts = BTreeMap::new();
        for b in results["by_cat"]["buckets"].as_array().unwrap() {
            counts.insert(
                b["key"].as_str().unwrap().to_string(),
                b["doc_count"].as_u64().unwrap(),
            );
        }
        assert_eq!(counts.get("x"), Some(&2));
        assert_eq!(counts.get("y"), Some(&2)); // 1 from each shard
        assert_eq!(counts.get("z"), Some(&1));
    }

    // ---- fail-loud multi-shard guards ----------------------------------

    fn search_req() -> SearchRequest {
        SearchRequest {
            query: "x".into(),
            limit: 10,
            ..Default::default()
        }
    }

    /// A Node that **honors** `offset`/`limit` over its (pre-sorted, score-desc) hits and
    /// records the window it was asked for — so a test can prove the Gateway rewrites the
    /// per-shard request to `offset=0, limit=offset+limit` (offset-merge), not forward
    /// the global window verbatim (which a real shard would apply locally → wrong page).
    struct PagingNode {
        hits: Vec<(&'static str, f64)>,
        seen: std::sync::Arc<std::sync::Mutex<(u32, u32)>>,
    }

    #[tonic::async_trait]
    impl Node for PagingNode {
        async fn search(
            &self,
            req: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            use growlerdb_proto::v1::{value::Kind, Coordinates, Field, SearchHit, Value};
            let r = req.into_inner();
            *self.seen.lock().unwrap() = (r.offset, r.limit);
            let off = r.offset as usize;
            let lim = if r.limit == 0 {
                usize::MAX
            } else {
                r.limit as usize
            };
            let hits = self
                .hits
                .iter()
                .skip(off)
                .take(lim)
                .map(|(id, score)| SearchHit {
                    coordinates: Some(Coordinates {
                        partition: vec![],
                        identifier: vec![Field {
                            name: "id".into(),
                            value: Some(Value {
                                kind: Some(Kind::Str((*id).into())),
                            }),
                        }],
                    }),
                    score: *score,
                    group: None,
                    group_count: 0,
                    sort_values: Vec::new(),
                    fields: vec![],
                    highlight: Default::default(),
                })
                .collect::<Vec<_>>();
            Ok(Response::new(SearchResponse {
                total: hits.len() as u64,
                hits,
                next_cursor: Vec::new(),
                partial: false,
                ..Default::default()
            }))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("describe"))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn offset_merge_returns_global_page_and_rewrites_per_shard_window() {
        use std::sync::Mutex;
        // Global score-desc order: a1(9) b1(8) a2(6) b2(5) a3(3) b3(2). offset=2 limit=2 → a2,b2.
        let sa = Arc::new(Mutex::new((0, 0)));
        let sb = Arc::new(Mutex::new((0, 0)));
        let a = Arc::new(PagingNode {
            hits: vec![("a1", 9.0), ("a2", 6.0), ("a3", 3.0)],
            seen: sa.clone(),
        });
        let b = Arc::new(PagingNode {
            hits: vec![("b1", 8.0), ("b2", 5.0), ("b3", 2.0)],
            seen: sb.clone(),
        });
        let gw = Gateway::sharded(vec![a, b]);

        let resp = gw
            .search(Request::new(SearchRequest {
                offset: 2,
                limit: 2,
                ..search_req()
            }))
            .await
            .unwrap()
            .into_inner();

        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["a2", "b2"]); // global ranks 2..4, no gaps/dupes
                                           // Each shard was asked for the page from rank 0, deep enough to cover offset+limit.
        assert_eq!(*sa.lock().unwrap(), (0, 4));
        assert_eq!(*sb.lock().unwrap(), (0, 4));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn offset_merge_on_field_sorted_page() {
        // Global asc-by-rank order: a1(1) b2(2) a3(3) b4(4). offset=1 limit=2 → b2, a3.
        let a = Arc::new(SortNode {
            hits: vec![("a1", vec![1.0]), ("a3", vec![3.0])],
        });
        let b = Arc::new(SortNode {
            hits: vec![("b2", vec![2.0]), ("b4", vec![4.0])],
        });
        let gw = Gateway::sharded(vec![a, b]);

        let resp = gw
            .search(Request::new(SearchRequest {
                sort: rank_sort(false),
                offset: 1,
                limit: 2,
                ..search_req()
            }))
            .await
            .unwrap()
            .into_inner();

        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["b2", "a3"]);
    }

    /// A Node that faithfully honors a `search_after` keyset over its `(id, rank)` hits: it
    /// sorts locally by the rank key (composite-key tiebreaker), resumes strictly after the
    /// decoded cursor, and returns up to `limit` hits carrying their sort values — exactly what
    /// the Gateway's cross-shard keyset scroll relies on.
    struct KeysetNode {
        hits: Vec<(&'static str, f64)>,
    }

    impl KeysetNode {
        fn coords(id: &str) -> Coordinates {
            use growlerdb_proto::v1::{value::Kind, Field, Value};
            Coordinates {
                partition: vec![],
                identifier: vec![Field {
                    name: "id".into(),
                    value: Some(Value {
                        kind: Some(Kind::Str(id.into())),
                    }),
                }],
            }
        }
    }

    #[tonic::async_trait]
    impl Node for KeysetNode {
        async fn search(
            &self,
            req: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            use growlerdb_core::Value;
            use growlerdb_proto::v1::{sort_value, SearchHit, SortValue as WireSortValue};

            let r = req.into_inner();
            let order = if r.sort.first().map(|s| s.descending).unwrap_or(false) {
                SortOrder::Desc
            } else {
                SortOrder::Asc
            };
            let lim = if r.limit == 0 {
                usize::MAX
            } else {
                r.limit as usize
            };
            // Local order: by rank (with the request's direction), then composite key ascending.
            let mut items: Vec<(f64, Vec<u8>, &'static str)> = self
                .hits
                .iter()
                .map(|(id, rank)| {
                    let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(*id))]);
                    (*rank, key.encode(), *id)
                })
                .collect();
            items.sort_by(|a, b| {
                cmp_sort_value(&SortValue::Num(a.0), &SortValue::Num(b.0), order)
                    .then_with(|| a.1.cmp(&b.1))
            });
            // Resume strictly after the cursor's (rank, key) position, if any.
            let start = if r.search_after.is_empty() {
                0
            } else {
                let c = crate::search_service::decode_cursor(&r.search_after)
                    .map_err(Status::invalid_argument)?;
                let cv = c.sort_values.first().cloned().unwrap_or(SortValue::Missing);
                let ck = c.key.encode();
                items
                    .iter()
                    .position(|(rank, key, _)| {
                        cmp_sort_value(&SortValue::Num(*rank), &cv, order)
                            .then_with(|| key.cmp(&ck))
                            == std::cmp::Ordering::Greater
                    })
                    .unwrap_or(items.len())
            };
            let hits = items[start..]
                .iter()
                .take(lim)
                .map(|(rank, _, id)| SearchHit {
                    coordinates: Some(Self::coords(id)),
                    score: 0.0,
                    group: None,
                    group_count: 0,
                    fields: vec![],
                    sort_values: vec![WireSortValue {
                        kind: Some(sort_value::Kind::Num(*rank)),
                    }],
                    highlight: Default::default(),
                })
                .collect::<Vec<_>>();
            Ok(Response::new(SearchResponse {
                total: hits.len() as u64,
                hits,
                next_cursor: Vec::new(),
                partial: false,
                ..Default::default()
            }))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("describe"))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keyset_scroll_visits_every_doc_once() {
        // Global asc-by-rank order across 2 shards: a1(1) b2(2) b3(3) a4(4) a5(5) b6(6).
        let a = Arc::new(KeysetNode {
            hits: vec![("a1", 1.0), ("a4", 4.0), ("a5", 5.0)],
        });
        let b = Arc::new(KeysetNode {
            hits: vec![("b2", 2.0), ("b3", 3.0), ("b6", 6.0)],
        });
        let gw = Gateway::sharded(vec![a, b]);

        // Scroll a page of 2 at a time, following the Gateway's composite cursor to exhaustion.
        let mut cursor: Vec<u8> = Vec::new();
        let mut got: Vec<String> = Vec::new();
        for _ in 0..10 {
            let resp = gw
                .search(Request::new(SearchRequest {
                    sort: rank_sort(false),
                    limit: 2,
                    search_after: cursor.clone(),
                    ..search_req()
                }))
                .await
                .unwrap()
                .into_inner();
            if resp.hits.is_empty() {
                break;
            }
            got.extend(resp.hits.iter().map(id_of));
            if resp.next_cursor.is_empty() {
                break;
            }
            cursor = resp.next_cursor;
        }

        // Every matching doc, in global order, exactly once — no gaps, no dupes.
        assert_eq!(got, vec!["a1", "b2", "b3", "a4", "a5", "b6"]);
    }

    /// A Node that returns pre-collapsed groups: one hit per `(group, top_rank, count)`, carrying
    /// the group value, its local count, and the top hit's sort value — what a real shard emits
    /// for a collapse query. Honors the request `limit` (local top-k groups).
    struct CollapseNode {
        groups: Vec<(&'static str, f64, u64)>,
    }

    #[tonic::async_trait]
    impl Node for CollapseNode {
        async fn search(
            &self,
            req: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            use growlerdb_proto::v1::{sort_value, value::Kind, Field, SearchHit, Value};
            let r = req.into_inner();
            let order = if r.sort.first().map(|s| s.descending).unwrap_or(false) {
                SortOrder::Desc
            } else {
                SortOrder::Asc
            };
            let lim = if r.limit == 0 {
                usize::MAX
            } else {
                r.limit as usize
            };
            let mut gs = self.groups.clone();
            gs.sort_by(|a, b| cmp_sort_value(&SortValue::Num(a.1), &SortValue::Num(b.1), order));
            let hits = gs
                .iter()
                .take(lim)
                .map(|(g, rank, count)| SearchHit {
                    coordinates: Some(Coordinates {
                        partition: vec![],
                        identifier: vec![Field {
                            name: "id".into(),
                            value: Some(Value {
                                kind: Some(Kind::Str((*g).into())),
                            }),
                        }],
                    }),
                    score: 0.0,
                    group: Some(Value {
                        kind: Some(Kind::Str((*g).into())),
                    }),
                    group_count: *count,
                    fields: vec![],
                    sort_values: vec![growlerdb_proto::v1::SortValue {
                        kind: Some(sort_value::Kind::Num(*rank)),
                    }],
                    highlight: Default::default(),
                })
                .collect::<Vec<_>>();
            Ok(Response::new(SearchResponse {
                total: hits.len() as u64,
                hits,
                next_cursor: Vec::new(),
                partial: false,
                ..Default::default()
            }))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("describe"))
        }
    }

    fn group_of(hit: &growlerdb_proto::v1::SearchHit) -> String {
        match hit.group.as_ref().unwrap().kind.clone().unwrap() {
            growlerdb_proto::v1::value::Kind::Str(s) => s,
            other => panic!("unexpected group kind: {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn collapse_folds_groups_across_shards() {
        // Group "x" appears on BOTH shards. Folded: count 2+4=6, top hit = the better rank
        // (A's x@1 < B's x@3). Other groups: y@5 (A), z@2 (B). Global asc order: x(1) z(2) y(5).
        let a = Arc::new(CollapseNode {
            groups: vec![("x", 1.0, 2), ("y", 5.0, 1)],
        });
        let b = Arc::new(CollapseNode {
            groups: vec![("x", 3.0, 4), ("z", 2.0, 1)],
        });
        let gw = Gateway::sharded(vec![a, b]);

        let resp = gw
            .search(Request::new(SearchRequest {
                collapse: "cat".into(),
                sort: rank_sort(false),
                limit: 10,
                ..search_req()
            }))
            .await
            .unwrap()
            .into_inner();

        let groups: Vec<String> = resp.hits.iter().map(group_of).collect();
        assert_eq!(groups, vec!["x", "z", "y"]); // one merged group per value, ordered by top hit
                                                 // "x" merged into a single group with the summed count and its global-top hit (id "x").
        let x = &resp.hits[0];
        assert_eq!(x.group_count, 6);
        assert_eq!(id_of(x), "x");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn collapse_without_sort_is_rejected() {
        let gw = Gateway::sharded(vec![Arc::new(FakeNode), Arc::new(FakeNode)]);
        let err = gw
            .search(Request::new(SearchRequest {
                collapse: "cat".into(),
                limit: 10,
                ..search_req()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_shard_rejects_score_ranked_search_after() {
        // search_after WITHOUT a sort (score-ranked keyset) is unsupported — scores aren't a
        // stable keyset. With a sort it is supported (see keyset_scroll_visits_every_doc_once).
        let gw = Gateway::sharded(vec![Arc::new(FakeNode), Arc::new(FakeNode)]);
        let err = gw
            .search(Request::new(SearchRequest {
                search_after: vec![1, 2, 3],
                ..search_req()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        // An explicit `_score` sort key is also not a stable keyset, so
        // search_after over it is rejected the same way.
        let err = gw
            .search(Request::new(SearchRequest {
                search_after: vec![1, 2, 3],
                sort: vec![growlerdb_proto::v1::Sort {
                    field: "_score".into(),
                    descending: true,
                }],
                ..search_req()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn single_shard_forwards_offset_unchanged() {
        // A single-shard Gateway forwards verbatim: the guards don't apply, so the request
        // reaches the Node (Unimplemented here), it is not rejected with InvalidArgument.
        let gw = Gateway::new(Arc::new(FakeNode));
        let err = gw
            .search(Request::new(SearchRequest {
                offset: 5,
                ..search_req()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn all_shards_down_search_errors_not_empty() {
        // Both shards fail → an honest UNAVAILABLE, never a success-shaped empty page.
        let gw = Gateway::sharded(vec![Arc::new(FakeNode), Arc::new(FakeNode)]);
        let err = gw.search(Request::new(search_req())).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unavailable);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn score_ties_break_by_key_deterministically() {
        // Equal scores across shards → ordered by composite key ascending ("a" < "z"),
        // not by shard completion order.
        let a = Arc::new(ShardNode {
            hits: vec![("z", 5.0)],
            num_docs: 1,
        });
        let b = Arc::new(ShardNode {
            hits: vec![("a", 5.0)],
            num_docs: 1,
        });
        let gw = Gateway::sharded(vec![a, b]);
        let resp = gw
            .search(Request::new(search_req()))
            .await
            .unwrap()
            .into_inner();
        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["a", "z"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_flags_failed_shards() {
        // ShardNode answers describe; SortNode fails it (one healthy + one failing).
        let healthy = Arc::new(ShardNode {
            hits: vec![],
            num_docs: 7,
        });
        let failing = Arc::new(SortNode { hits: vec![] });
        let gw = Gateway::sharded(vec![healthy, failing]);
        let resp = gw
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.failed_shards, 1);
        assert_eq!(resp.stats.unwrap().num_docs, 7); // the healthy shard still counts
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn describe_exposes_per_shard_stats_for_skew() {
        // Two lopsided shards; describe sums the total AND keeps the per-shard breakdown so the
        // skew (100 vs 5) is observable.
        let a = Arc::new(ShardNode {
            hits: vec![],
            num_docs: 100,
        });
        let b = Arc::new(ShardNode {
            hits: vec![],
            num_docs: 5,
        });
        let gw = Gateway::sharded(vec![a, b]);
        let resp = gw
            .describe_index(Request::new(DescribeIndexRequest {
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.stats.unwrap().num_docs, 105); // summed total
        assert_eq!(resp.per_shard.len(), 2);
        let counts: Vec<u64> = resp.per_shard.iter().map(|s| s.num_docs).collect();
        assert!(
            counts.contains(&100) && counts.contains(&5),
            "per-shard skew visible: {counts:?}"
        );
    }

    /// A Node that returns fixed suggestions (the others fail), to test the suggest merge's
    /// `failed_shards` flag.
    struct SuggestingNode {
        suggestions: Vec<(&'static str, u64)>,
    }

    #[tonic::async_trait]
    impl Node for SuggestingNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            Err(Status::unimplemented("search"))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Ok(Response::new(SuggestResponse {
                suggestions: self
                    .suggestions
                    .iter()
                    .map(|(text, count)| Suggestion {
                        text: (*text).into(),
                        count: *count,
                    })
                    .collect(),
                failed_shards: 0,
            }))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("describe"))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn suggest_flags_failed_shards() {
        let healthy = Arc::new(SuggestingNode {
            suggestions: vec![("foo", 3)],
        });
        let gw = Gateway::sharded(vec![healthy, Arc::new(FakeNode)]); // FakeNode.suggest fails
        let resp = gw
            .suggest(Request::new(SuggestRequest {
                field: "f".into(),
                text: "fo".into(),
                limit: 10,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.failed_shards, 1);
        assert_eq!(resp.suggestions.len(), 1); // the healthy shard's suggestion survives
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_by_key_flags_failed_shards() {
        use growlerdb_core::Value;
        use std::sync::Mutex;

        let seen = Arc::new(Mutex::new(Vec::new()));
        let gw = Gateway::sharded_with(
            vec![
                Arc::new(RecordingNode { seen: seen.clone() }), // shard 0: succeeds
                Arc::new(FakeNode),                             // shard 1: get_by_key fails
            ],
            ShardRouter::hashed(2),
        );
        let key = |id: &str| CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        // 20 keys hash across both shards (see get_by_key_routes_each_key_to_its_owning_shard),
        // so shard 1 (FakeNode) is queried and fails its slice.
        let keys: Vec<Coordinates> = (0..20).map(|i| (&key(&format!("k{i}"))).into()).collect();
        let resp = gw
            .get_by_key(Request::new(GetByKeyRequest {
                window: 0,
                keys,
                columns: Vec::new(),
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.failed_shards, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn get_by_key_rejects_malformed_coordinate() {
        use growlerdb_proto::v1::Field;
        let gw = Gateway::sharded(vec![Arc::new(FakeNode), Arc::new(FakeNode)]);
        // A coordinate whose identifier field carries no value can't decode to a key.
        let bad = Coordinates {
            partition: vec![],
            identifier: vec![Field {
                name: "id".into(),
                value: None,
            }],
        };
        let err = gw
            .get_by_key(Request::new(GetByKeyRequest {
                window: 0,
                keys: vec![bad],
                columns: Vec::new(),
                index: String::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn aggregate_flags_failed_shards() {
        let ta = tempfile::tempdir().unwrap();
        let gw = Gateway::sharded(vec![
            agg_node(ta.path(), &[("1", "x"), ("2", "y")]),
            Arc::new(FakeNode), // aggregate → Unimplemented (the Node trait default)
        ]);
        let resp = gw
            .aggregate(Request::new(AggregateRequest {
                query: "cat:x OR cat:y".into(),
                aggs: r#"{"by_cat": {"Terms": {"field": "cat", "size": 10}}}"#.into(),
                partial: false,
                window: 0,
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.failed_shards, 1);
        // The healthy shard's buckets still come through.
        let results: serde_json::Value = serde_json::from_str(&resp.results).unwrap();
        assert!(!results["by_cat"]["buckets"].as_array().unwrap().is_empty());
    }

    // ---- deadlines + limit guards --------------------------------------

    /// A Node whose `search` sleeps past any sane deadline — stands in for a hung/slow shard.
    struct SlowNode {
        delay: Duration,
    }

    #[tonic::async_trait]
    impl Node for SlowNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            tokio::time::sleep(self.delay).await;
            Ok(Response::new(SearchResponse::default()))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("describe"))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn slow_shard_hits_the_deadline_and_returns_partial() {
        // One fast shard, one that would take 10s; a 150ms deadline drops the slow one.
        let fast = Arc::new(ShardNode {
            hits: vec![("a1", 9.0)],
            num_docs: 1,
        });
        let slow = Arc::new(SlowNode {
            delay: Duration::from_secs(10),
        });
        let gw = Gateway::sharded(vec![fast, slow]).with_limits(GatewayLimits {
            deadline: Some(Duration::from_millis(150)),
            max_fetch: 10_000,
            ..GatewayLimits::default()
        });

        let started = tokio::time::Instant::now();
        let resp = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 10,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        // Returned well before the slow shard would have (proves the deadline fired + aborted).
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "deadline did not fire"
        );
        assert!(resp.partial, "a dropped slow shard must flag partial");
        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(ids, vec!["a1"]); // the fast shard's hit still returned
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn oversized_page_fetch_is_rejected() {
        let a = Arc::new(ShardNode {
            hits: vec![("a1", 9.0)],
            num_docs: 1,
        });
        let b = Arc::new(ShardNode {
            hits: vec![("b1", 8.0)],
            num_docs: 1,
        });
        let gw = Gateway::sharded(vec![a, b]).with_limits(GatewayLimits {
            deadline: None,
            max_fetch: 100,
            ..GatewayLimits::default()
        });

        // limit over the ceiling → InvalidArgument (before any shard builds a giant page).
        let err = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 1000,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        // offset + limit over the ceiling is also rejected (that's the real per-shard fetch).
        let err = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                offset: 80,
                limit: 30,
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);

        // Within the ceiling is served normally (not rejected).
        let resp = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 50,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.hits.len(), 2);
    }

    // ---- AuthN at the Gateway boundary ------------------------------

    use std::sync::Mutex;

    use crate::authn::{Authenticator, AuthnError, SharedAuthn, Verified};

    /// The `(principal, tenant)` each call arrived with, captured per request.
    type SeenIdentities = Arc<Mutex<Vec<(Option<String>, Option<String>)>>>;

    /// Records the identity metadata each `describe_index` arrives with, so a test can
    /// prove what identity actually reached the shard.
    struct RecordIdentityNode {
        seen: SeenIdentities,
    }

    #[tonic::async_trait]
    impl Node for RecordIdentityNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            Err(Status::unimplemented("search"))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            req: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            let read = |k: &str| {
                req.metadata()
                    .get(k)
                    .and_then(|v| v.to_str().ok())
                    .map(str::to_string)
            };
            self.seen
                .lock()
                .unwrap()
                .push((read("x-growlerdb-principal"), read("x-growlerdb-tenant")));
            Ok(Response::new(DescribeIndexResponse {
                stats: Some(IndexStats {
                    name: req.into_inner().index,
                    ..Default::default()
                }),
                failed_shards: 0,
                per_shard: Vec::new(),
            }))
        }
    }

    /// A stand-in authenticator: `Bearer good` → a fixed verified identity, anything else
    /// (including a missing credential) → `Missing`. Keeps the Gateway test about the wiring
    /// — JWT validation itself is covered in `authn`.
    struct StubAuthn;
    impl Authenticator for StubAuthn {
        fn authenticate(&self, authorization: Option<&str>) -> Result<Verified, AuthnError> {
            match authorization {
                Some("Bearer good") => Ok(Verified {
                    principal: "alice".to_string(),
                    tenant: Some("acme".to_string()),
                    roles: Vec::new(),
                    ..Default::default()
                }),
                _ => Err(AuthnError::Missing),
            }
        }
    }

    fn describe_req(
        authorization: Option<&str>,
        forged_principal: Option<&str>,
    ) -> Request<DescribeIndexRequest> {
        let mut req = Request::new(DescribeIndexRequest {
            window: 0,
            index: "docs".into(),
        });
        if let Some(a) = authorization {
            req.metadata_mut()
                .insert("authorization", a.parse().unwrap());
        }
        if let Some(p) = forged_principal {
            req.metadata_mut()
                .insert("x-growlerdb-principal", p.parse().unwrap());
        }
        req
    }

    #[test]
    fn auth_required_reflects_whether_an_authenticator_is_configured() {
        // /v1/config's source — open (no authenticator) is not gated; configured is.
        assert!(!Gateway::new(Arc::new(FakeNode)).auth_required());
        assert!(Gateway::new(Arc::new(FakeNode))
            .with_authn(Arc::new(StubAuthn))
            .auth_required());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn identity_is_anonymous_when_open_and_verified_with_authn() {
        // /v1/me's source. Open gateway → anonymous (empty principal).
        let open = Gateway::new(Arc::new(FakeNode));
        assert!(open
            .identity(&mut Request::new(()))
            .unwrap()
            .principal
            .is_empty());

        // With an authenticator: a valid credential resolves the identity; a missing one is rejected.
        let gw = Gateway::new(Arc::new(FakeNode)).with_authn(Arc::new(StubAuthn));
        let mut ok = Request::new(());
        ok.metadata_mut()
            .insert("authorization", "Bearer good".parse().unwrap());
        assert_eq!(gw.identity(&mut ok).unwrap().principal, "alice");
        assert!(gw.identity(&mut Request::new(())).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn with_authn_a_valid_credential_overrides_a_forged_identity() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let node = Arc::new(RecordIdentityNode { seen: seen.clone() });
        let authn: SharedAuthn = Arc::new(StubAuthn);
        let gw = Gateway::new(node).with_authn(authn);

        // A forged principal accompanies a valid credential — the shard must see the
        // verified identity, never the forgery.
        gw.describe_index(describe_req(Some("Bearer good"), Some("attacker")))
            .await
            .unwrap();
        assert_eq!(
            seen.lock().unwrap().as_slice(),
            [(Some("alice".to_string()), Some("acme".to_string()))]
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn with_authn_a_missing_credential_is_rejected_before_the_shard() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let node = Arc::new(RecordIdentityNode { seen: seen.clone() });
        let gw = Gateway::new(node).with_authn(Arc::new(StubAuthn));

        let err = gw
            .describe_index(describe_req(None, Some("attacker")))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
        // The request never reached the shard.
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn open_gateway_strips_caller_asserted_identity() {
        // With no authenticator the Gateway must NOT trust caller-supplied identity —
        // a forged principal/tenant is stripped, so it can't drive tenant scoping or RBAC. (Closed
        // mode stamps the *verified* identity instead — see `with_authn_a_valid_credential_...`.)
        let seen = Arc::new(Mutex::new(Vec::new()));
        let node = Arc::new(RecordIdentityNode { seen: seen.clone() });
        let gw = Gateway::new(node);

        gw.describe_index(describe_req(None, Some("attacker")))
            .await
            .unwrap();
        assert_eq!(
            seen.lock().unwrap()[0].0,
            None,
            "forged principal is stripped"
        );
    }

    // ---- RBAC at the Gateway ------------------------------------------------

    /// An authenticator that admits any credential as `alice` with fixed roles.
    struct RolesAuthn(Vec<String>);
    impl Authenticator for RolesAuthn {
        fn authenticate(&self, authorization: Option<&str>) -> Result<Verified, AuthnError> {
            authorization.ok_or(AuthnError::Missing)?;
            Ok(Verified {
                principal: "alice".to_string(),
                tenant: None,
                roles: self.0.clone(),
                ..Default::default()
            })
        }
    }

    fn rbac_gw(roles: &[&str], seen: SeenIdentities) -> Gateway {
        let authn: SharedAuthn =
            Arc::new(RolesAuthn(roles.iter().map(|r| r.to_string()).collect()));
        Gateway::new(Arc::new(RecordIdentityNode { seen }))
            .with_authn(authn)
            .with_authz(Arc::new(crate::rbac::RbacPolicy::with_default_roles()))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rbac_admits_a_role_that_grants_the_methods_scope() {
        // DescribeIndex requires `index.read`, which `viewer` holds → reaches the shard.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let gw = rbac_gw(&["viewer"], seen.clone());
        gw.describe_index(describe_req(Some("Bearer x"), None))
            .await
            .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rbac_rejects_an_unprivileged_caller_before_the_shard() {
        // An authenticated caller with no granting role → PermissionDenied, shard untouched.
        let seen = Arc::new(Mutex::new(Vec::new()));
        let gw = rbac_gw(&["bogus"], seen.clone());
        let err = gw
            .describe_index(describe_req(Some("Bearer x"), None))
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn windowed_gateway_prunes_scatter_to_matching_windows() {
        use growlerdb_core::WindowGranularity;
        const DAY: i64 = 86_400_000_000; // one day in micros (canonical window scale)
        let day = |n: i64| n * DAY;

        // One Node per window, each returning a distinct hit. Because ShardNode ignores the query,
        // a window's hit appears *iff* its node was queried — so pruning is directly observable
        // (no per-shard filter masking it, unlike the embedded store path).
        let node = |id: &'static str| {
            Arc::new(ShardNode {
                hits: vec![(id, 1.0)],
                num_docs: 1,
            }) as Arc<dyn Node>
        };
        let shards = vec![node("d10"), node("d11"), node("d20")];
        let windowing =
            TimeWindowing::new("ingest", WindowGranularity::Daily).with_event_time("event");
        // Window 10 carries a late event (zone widened down to day 2); 11 and 20 are tight.
        let windows = vec![
            (day(10), Some((day(2), day(10))), false),
            (day(11), Some((day(11), day(11))), false),
            (day(20), Some((day(20), day(20))), false),
        ];
        let gw = Gateway::windowed(shards, windowing, windows);

        let search = |q: String| {
            let gw = &gw;
            async move {
                let mut ids: Vec<String> = gw
                    .search(Request::new(SearchRequest {
                        query: q,
                        limit: 10,
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner()
                    .hits
                    .iter()
                    .map(id_of)
                    .collect();
                ids.sort();
                ids
            }
        };

        // No time filter → fan out to every window.
        assert_eq!(search("foo".into()).await, vec!["d10", "d11", "d20"]);

        // Ingest range inside window 11 → only that window's node is queried.
        assert_eq!(
            search(format!("foo AND ingest:[{} TO {}]", day(11), day(11) + 100)).await,
            vec!["d11"]
        );

        // Event-time range in the late-data band → only window 10's widened zone-map overlaps.
        assert_eq!(
            search(format!("foo AND event:[{} TO {}]", day(2), day(3))).await,
            vec!["d10"]
        );

        // A time filter beyond every window → empty page (valid query, no window can match).
        assert!(
            search(format!("foo AND ingest:[{} TO {}]", day(40), day(41)))
                .await
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn swap_windowed_makes_a_new_window_queryable_and_prunable() {
        // A running windowed gateway learns a newly-created window via swap_windowed —
        // the new window becomes queryable + time-prunable with no restart.
        use growlerdb_core::WindowGranularity;
        const DAY: i64 = 86_400_000_000;
        let day = |n: i64| n * DAY;
        let node = |id: &'static str| {
            Arc::new(ShardNode {
                hits: vec![(id, 1.0)],
                num_docs: 1,
            }) as Arc<dyn Node>
        };
        let windowing =
            TimeWindowing::new("ingest", WindowGranularity::Daily).with_event_time("event");
        // Start with two windows (10, 11).
        let gw = Gateway::windowed(
            vec![node("d10"), node("d11")],
            windowing.clone(),
            vec![
                (day(10), Some((day(10), day(10))), false),
                (day(11), Some((day(11), day(11))), false),
            ],
        );
        let search = |q: String| {
            let gw = &gw;
            async move {
                let mut ids: Vec<String> = gw
                    .search(Request::new(SearchRequest {
                        query: q,
                        limit: 10,
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner()
                    .hits
                    .iter()
                    .map(id_of)
                    .collect();
                ids.sort();
                ids
            }
        };
        assert_eq!(search("foo".into()).await, vec!["d10", "d11"]);
        // A query in a not-yet-existing window 12 matches nothing.
        assert!(
            search(format!("foo AND ingest:[{} TO {}]", day(12), day(12) + 100))
                .await
                .is_empty()
        );

        // A new window 12 is created + swapped in (the dynamic-ingest path does this on first write).
        gw.swap_windowed(
            vec![node("d10"), node("d11"), node("d12")],
            windowing,
            vec![
                (day(10), Some((day(10), day(10))), false),
                (day(11), Some((day(11), day(11))), false),
                (day(12), Some((day(12), day(12))), false),
            ],
        );
        // Now it fans out to all three, and a time filter prunes precisely to the new window.
        assert_eq!(search("foo".into()).await, vec!["d10", "d11", "d12"]);
        assert_eq!(
            search(format!("foo AND ingest:[{} TO {}]", day(12), day(12) + 100)).await,
            vec!["d12"]
        );
        assert_eq!(gw.shard_count(), 3);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cold_status_reports_per_window_tier_and_cache() {
        use growlerdb_core::WindowGranularity;
        use growlerdb_index::RangeCache;
        const DAY: i64 = 86_400_000_000; // one day in micros (canonical window scale)
        let day = |n: i64| n * DAY;
        let node = |id: &'static str| {
            Arc::new(ShardNode {
                hits: vec![(id, 1.0)],
                num_docs: 1,
            }) as Arc<dyn Node>
        };
        let shards = vec![node("d10"), node("d11"), node("d12")];
        let windowing =
            TimeWindowing::new("ingest", WindowGranularity::Daily).with_event_time("event");
        // Per-window tier travels in the descriptors: windows 10 and 11 are parked (read-through),
        // 12 is hot. `with_cold_tier` only wires the shared cache for its stats.
        let windows = vec![
            (day(10), Some((day(10), day(10) + 5)), true),
            (day(11), Some((day(11), day(11))), true),
            (day(12), None, false),
        ];
        let gw = Gateway::windowed(shards, windowing, windows)
            .with_cold_tier(RangeCache::new(1024 * 1024));

        let status = gw.cold_status().expect("windowed gateway has cold status");
        assert_eq!((status.hot, status.cold), (1, 2));
        assert_eq!(status.windows.len(), 3);
        let w = |id: i64| status.windows.iter().find(|w| w.window == id).unwrap();
        assert!(
            w(day(10)).cold && w(day(11)).cold,
            "parked windows are cold"
        );
        assert!(!w(day(12)).cold, "the recent window is hot");
        assert_eq!(w(day(10)).event_min, Some(day(10)));
        // The shared cache is present, fresh (no reads yet).
        assert_eq!(status.cache.unwrap().hits, 0);

        // A non-windowed Gateway has no cold tier.
        assert!(Gateway::sharded(vec![node("a"), node("b")])
            .cold_status()
            .is_none());
    }

    // ---- Multi-index routing -----------------------------------------------

    /// A Node whose `search`/`describe` report a fixed index tag, so a test can tell *which* index's
    /// route answered a request. `describe` echoes the tag as the stats name; `search` returns one hit
    /// whose id is the tag.
    struct TaggedNode(&'static str);

    #[tonic::async_trait]
    impl Node for TaggedNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            use growlerdb_proto::v1::{value::Kind, Coordinates, Field, SearchHit, Value};
            Ok(Response::new(SearchResponse {
                hits: vec![SearchHit {
                    coordinates: Some(Coordinates {
                        partition: vec![],
                        identifier: vec![Field {
                            name: "id".into(),
                            value: Some(Value {
                                kind: Some(Kind::Str(self.0.into())),
                            }),
                        }],
                    }),
                    score: 1.0,
                    group: None,
                    group_count: 0,
                    sort_values: Vec::new(),
                    fields: vec![],
                    highlight: Default::default(),
                }],
                total: 1,
                ..Default::default()
            }))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Ok(Response::new(DescribeIndexResponse {
                stats: Some(IndexStats {
                    name: self.0.into(),
                    ..Default::default()
                }),
                failed_shards: 0,
                per_shard: Vec::new(),
            }))
        }
    }

    /// A resolver backed by a fixed name → tag map: index `name` resolves to a one-shard route over a
    /// [`TaggedNode`] echoing `tag`; an unknown name is `Ok(None)` (→ NOT_FOUND). Records how many
    /// times it was asked to resolve each name, so a test can prove routes are cached (resolved once).
    struct MapResolver {
        map: std::collections::HashMap<&'static str, &'static str>,
        calls: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[tonic::async_trait]
    impl RouteResolver for MapResolver {
        async fn resolve(&self, index: &str) -> Result<Option<Arc<IndexRoute>>, String> {
            self.calls.lock().unwrap().push(index.to_string());
            match self.map.get(index) {
                Some(tag) => Ok(Some(IndexRoute::new(
                    vec![Arc::new(TaggedNode(tag)) as Arc<dyn Node>],
                    ShardRouter::hashed(1),
                    None,
                    Vec::new(),
                ))),
                None => Ok(None),
            }
        }
    }

    fn multi_gw() -> (Gateway, std::sync::Arc<std::sync::Mutex<Vec<String>>>) {
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let resolver = Arc::new(MapResolver {
            map: std::collections::HashMap::from([("a", "index-a"), ("b", "index-b")]),
            calls: calls.clone(),
        });
        (Gateway::multi_index(resolver, None), calls)
    }

    fn describe(index: &str) -> Request<DescribeIndexRequest> {
        Request::new(DescribeIndexRequest {
            window: 0,
            index: index.into(),
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_index_routes_each_request_to_its_named_index() {
        let (gw, calls) = multi_gw();
        // Each named index is answered by its own route.
        let a = gw.describe_index(describe("a")).await.unwrap().into_inner();
        assert_eq!(a.stats.unwrap().name, "index-a");
        let b = gw.describe_index(describe("b")).await.unwrap().into_inner();
        assert_eq!(b.stats.unwrap().name, "index-b");
        // A search routes by index too.
        let hit = gw
            .search(Request::new(SearchRequest {
                query: "x".into(),
                limit: 1,
                index: "b".into(),
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(id_of(&hit.hits[0]), "index-b");
        // Each distinct index was resolved once (routes are cached, not re-resolved per request).
        let repeat = gw.describe_index(describe("a")).await.unwrap().into_inner();
        assert_eq!(repeat.stats.unwrap().name, "index-a");
        let resolved = calls.lock().unwrap().clone();
        assert_eq!(resolved.iter().filter(|n| *n == "a").count(), 1);
        assert_eq!(resolved.iter().filter(|n| *n == "b").count(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_index_unknown_index_is_not_found() {
        let (gw, _) = multi_gw();
        let err = gw.describe_index(describe("nope")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::NotFound);
        // A second request for the same unknown index is served from the negative cache (still NF).
        let err2 = gw.describe_index(describe("nope")).await.unwrap_err();
        assert_eq!(err2.code(), tonic::Code::NotFound);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_index_empty_index_without_a_default_is_invalid_argument() {
        // With no default and >1 index served, an empty `index` can't be disambiguated. Resolve both
        // first so the map holds two routes, then a bare request is InvalidArgument.
        let (gw, _) = multi_gw();
        gw.describe_index(describe("a")).await.unwrap();
        gw.describe_index(describe("b")).await.unwrap();
        let err = gw.describe_index(describe("")).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("index required"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multi_index_empty_index_uses_the_default() {
        // A multi-index gateway with a default resolves an empty `index` to it.
        let calls = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let resolver = Arc::new(MapResolver {
            map: std::collections::HashMap::from([("a", "index-a"), ("b", "index-b")]),
            calls,
        });
        let gw = Gateway::multi_index(resolver, Some("a".into()));
        let resp = gw.describe_index(describe("")).await.unwrap().into_inner();
        assert_eq!(resp.stats.unwrap().name, "index-a");
    }

    // ---- Per-index RBAC + tenant isolation through multi-index --------------

    /// An authenticator that admits any credential as `alice` with fixed roles + an index allowlist —
    /// exercises per-index RBAC end to end (the allowlist stamped into metadata, read back by authz).
    struct ScopedAuthn {
        roles: Vec<String>,
        indexes: Vec<String>,
    }
    impl crate::authn::Authenticator for ScopedAuthn {
        fn authenticate(
            &self,
            authorization: Option<&str>,
        ) -> Result<crate::authn::Verified, crate::authn::AuthnError> {
            authorization.ok_or(crate::authn::AuthnError::Missing)?;
            Ok(crate::authn::Verified {
                principal: "alice".to_string(),
                roles: self.roles.clone(),
                indexes: self.indexes.clone(),
                ..Default::default()
            })
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn per_index_rbac_denies_a_token_scoped_to_another_index() {
        // A reader whose token allows only index `a`: search `a` is allowed, `b` is PermissionDenied —
        // the resolved target index is checked against the allowlist before any shard is touched.
        let authn: SharedAuthn = Arc::new(ScopedAuthn {
            roles: vec!["reader".into()],
            indexes: vec!["a".into()],
        });
        let (gw, _) = multi_gw();
        let gw = gw
            .with_authn(authn)
            .with_authz(Arc::new(crate::rbac::RbacPolicy::with_default_roles()));

        let search = |index: &str| {
            Request::new(SearchRequest {
                query: "x".into(),
                limit: 1,
                index: index.into(),
                ..Default::default()
            })
        };
        // Allowed index → the request reaches the (tagged) shard and answers. (ScopedAuthn requires a
        // non-empty credential, so a Bearer header is set.)
        let mut req_a = search("a");
        req_a
            .metadata_mut()
            .insert("authorization", "Bearer t".parse().unwrap());
        assert_eq!(
            id_of(&gw.search(req_a).await.unwrap().into_inner().hits[0]),
            "index-a"
        );
        // A different index the token doesn't allow → PermissionDenied, shard untouched.
        let mut req_b = search("b");
        req_b
            .metadata_mut()
            .insert("authorization", "Bearer t".parse().unwrap());
        let err = gw.search(req_b).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn per_index_rbac_unrestricted_token_reaches_any_index() {
        // A token with no index allowlist (empty) may reach any resolved index.
        let authn: SharedAuthn = Arc::new(ScopedAuthn {
            roles: vec!["reader".into()],
            indexes: Vec::new(),
        });
        let (gw, _) = multi_gw();
        let gw = gw
            .with_authn(authn)
            .with_authz(Arc::new(crate::rbac::RbacPolicy::with_default_roles()));
        for (ix, tag) in [("a", "index-a"), ("b", "index-b")] {
            let mut req = Request::new(SearchRequest {
                query: "x".into(),
                limit: 1,
                index: ix.into(),
                ..Default::default()
            });
            req.metadata_mut()
                .insert("authorization", "Bearer t".parse().unwrap());
            assert_eq!(
                id_of(&gw.search(req).await.unwrap().into_inner().hits[0]),
                tag
            );
        }
    }

    // ---- Semantic + hybrid orchestration -------------------------------------

    /// Build a wire [`SearchHit`] for `id` at `score` (KNN or BM25 score — the field is
    /// modality-agnostic).
    fn mk_hit(id: &str, score: f64) -> SearchHit {
        use growlerdb_proto::v1::{value::Kind, Coordinates, Field, Value};
        SearchHit {
            coordinates: Some(Coordinates {
                partition: vec![],
                identifier: vec![Field {
                    name: "id".into(),
                    value: Some(Value {
                        kind: Some(Kind::Str(id.into())),
                    }),
                }],
            }),
            score,
            group: None,
            group_count: 0,
            sort_values: Vec::new(),
            fields: vec![],
            highlight: Default::default(),
        }
    }

    /// A Node with canned **lexical** (`search`) and **semantic** (`semantic_search`) hit sets —
    /// enough to prove the Gateway's cross-shard semantic merge and hybrid RRF fusion without a
    /// real shard/embedder.
    struct VectorNode {
        lexical: Vec<(&'static str, f64)>,
        semantic: Vec<(&'static str, f64)>,
    }

    #[tonic::async_trait]
    impl Node for VectorNode {
        async fn search(
            &self,
            _: Request<SearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            let hits: Vec<SearchHit> = self.lexical.iter().map(|(id, s)| mk_hit(id, *s)).collect();
            let total = hits.len() as u64;
            Ok(Response::new(SearchResponse {
                hits,
                total,
                ..Default::default()
            }))
        }
        async fn semantic_search(
            &self,
            _: Request<SemanticSearchRequest>,
        ) -> Result<Response<SearchResponse>, Status> {
            let hits: Vec<SearchHit> = self.semantic.iter().map(|(id, s)| mk_hit(id, *s)).collect();
            let total = hits.len() as u64;
            Ok(Response::new(SearchResponse {
                hits,
                total,
                ..Default::default()
            }))
        }
        async fn suggest(
            &self,
            _: Request<SuggestRequest>,
        ) -> Result<Response<SuggestResponse>, Status> {
            Err(Status::unimplemented("suggest"))
        }
        async fn get_by_key(
            &self,
            _: Request<GetByKeyRequest>,
        ) -> Result<Response<GetByKeyResponse>, Status> {
            Err(Status::unimplemented("get_by_key"))
        }
        async fn describe_index(
            &self,
            _: Request<DescribeIndexRequest>,
        ) -> Result<Response<DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("describe_index"))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn semantic_search_merges_multi_shard_to_global_top_k() {
        // Two shards' KNN candidates; the global top-2 by score spans both shards.
        let a = Arc::new(VectorNode {
            lexical: vec![],
            semantic: vec![("a1", 0.9), ("a2", 0.2)],
        });
        let b = Arc::new(VectorNode {
            lexical: vec![],
            semantic: vec![("b1", 0.8), ("b2", 0.7)],
        });
        let gw = Gateway::sharded(vec![a, b]);
        let resp = gw
            .semantic_search(Request::new(SemanticSearchRequest {
                vector_field: "body_vec".into(),
                query_text: "q".into(),
                k: 2,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        // Global top-2 by score: a1 (0.9) then b1 (0.8) — a2/b2 drop off.
        assert_eq!(ids, vec!["a1", "b1"]);
        assert_eq!(resp.shards_scanned, 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hybrid_search_rrf_fuses_both_arms_and_the_both_modality_doc_wins() {
        // `both` appears in BOTH arms; `lex` only lexically; `vec` only semantically. RRF sums the
        // two arms so `both` outranks the single-modality docs, and all three appear.
        let node = Arc::new(VectorNode {
            lexical: vec![("both", 5.0), ("lex", 4.0)],
            semantic: vec![("both", 0.9), ("vec", 0.8)],
        });
        let gw = Gateway::new(node);
        let resp = gw
            .hybrid_search(Request::new(HybridSearchRequest {
                vector_field: "body_vec".into(),
                query_text: "q".into(),
                k: 10,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let ids: Vec<String> = resp.hits.iter().map(id_of).collect();
        assert_eq!(
            ids[0], "both",
            "the both-modality doc fuses to the top: {ids:?}"
        );
        for want in ["both", "lex", "vec"] {
            assert!(
                ids.contains(&want.to_string()),
                "{want} missing from {ids:?}"
            );
        }
    }
}
