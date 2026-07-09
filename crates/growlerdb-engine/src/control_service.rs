//! The **Control Plane** gRPC service ([task-28], Design 06) — cluster-wide index lifecycle
//! over the [`Registry`](growlerdb_controlplane::Registry): create / drop / list (completing
//! task-26's lifecycle). `CreateIndex` resolves the candidate definition against its source
//! schema before registering it (status `building`); the registry mutations are durable.
//! Lightweight and off the search/write hot path. Every RPC consults the
//! [auth hook](SharedAuth) first.
//!
//! [task-28]: ../../../design/06-service-architecture.md

use std::sync::Arc;

use std::collections::BTreeMap;

use growlerdb_controlplane::{
    ApiToken, IndexEntry, Registry, RegistryError, SavedQuery, ShardAssignment,
};
use growlerdb_core::{BucketMap, IndexDefinition, Reassignment, ResolvedIndex, Source};
use growlerdb_proto::v1::admin_client::AdminClient;
use growlerdb_proto::v1::{
    ActivityEvent as WireActivity, AliasEntry, ApiTokenMeta, ApplyReshardRequest,
    ApplyReshardResponse, BucketMove, CreateIndexRequest, CreateIndexResponse, CreateTokenRequest,
    CreateTokenResponse, DeleteSavedQueryRequest, DeleteSavedQueryResponse, DescribeSourceRequest,
    DescribeSourceResponse, DropAliasRequest, DropAliasResponse, DropIndexRequest,
    DropIndexResponse, Error as WireError, FieldMapping, GetCheckpointRequest, GetIndexRequest,
    GetIndexResponse, IndexIngestion, IndexSummary as WireSummary, IngestionStatusRequest,
    IngestionStatusResponse, ListActivityRequest, ListActivityResponse, ListAliasesRequest,
    ListAliasesResponse, ListIndexesRequest, ListIndexesResponse, ListRolesRequest,
    ListRolesResponse, ListSavedQueriesRequest, ListSavedQueriesResponse, ListTokensRequest,
    ListTokensResponse, ListUsersRequest, ListUsersResponse, LoginRequest, LoginResponse,
    MoveBucketRequest, MoveBucketResponse, PlanReshardRequest, PlanReshardResponse,
    RegisterNodeRequest, RegisterNodeResponse, RegisterServedIndexRequest,
    RegisterServedIndexResponse, ReindexIndexRequest, ResolveWindowOwnerRequest,
    ResolveWindowOwnerResponse, RevokeTokenRequest, RevokeTokenResponse, RoleBinding,
    RoutingStrategy as WireRouting, SaveSavedQueryRequest, SaveSavedQueryResponse,
    SavedQuery as WireSavedQuery, SetAliasRequest, SetAliasResponse, SetUserRolesRequest,
    SetUserRolesResponse, ShardIngestion, ShardStatus, SourceFieldInfo, WindowingConfig,
};
use growlerdb_proto::{to_status, ControlPlane, ControlPlaneServer, WriteClient};
use growlerdb_source::{IcebergConfig, IcebergReader};
use tonic::{Code, Request, Response, Status};

use crate::auth::{self, default_auth, AuthContext, SharedAuth};
use crate::authn::SharedAuthn;

/// Consecutive failures before an account is locked out (task-147 / B3).
const LOGIN_FAILURES_BEFORE_LOCKOUT: u32 = 5;
/// Base lockout window; doubles per failure past the threshold, capped at [`LOGIN_LOCKOUT_MAX_SECS`].
const LOGIN_LOCKOUT_BASE_SECS: u64 = 1;
const LOGIN_LOCKOUT_MAX_SECS: u64 = 300;
/// Max concurrent Argon2 verifications — bounds the CPU an unauthenticated `/v1/login` flood can burn.
const MAX_CONCURRENT_LOGINS: usize = 8;
/// Cap on tracked accounts, so a username-spray can't grow the throttle map unbounded.
const MAX_TRACKED_ACCOUNTS: usize = 10_000;

/// Per-account login throttle (task-147 / B3): tracks consecutive failures and locks an account for
/// an exponentially-growing window, plus a global concurrency permit. A locked account skips the
/// (CPU-heavy) Argon2 verify entirely, so online guessing is rate-limited and the unauthenticated
/// CPU-exhaustion amplifier is closed. Keyed by the *submitted* username (existing or not), so it
/// leaks no account existence.
struct LoginThrottle {
    accounts: std::sync::Mutex<std::collections::HashMap<String, Attempt>>,
    concurrency: tokio::sync::Semaphore,
}

#[derive(Default)]
struct Attempt {
    failures: u32,
    locked_until: Option<std::time::Instant>,
}

impl LoginThrottle {
    fn new() -> Self {
        Self {
            accounts: std::sync::Mutex::new(std::collections::HashMap::new()),
            concurrency: tokio::sync::Semaphore::new(MAX_CONCURRENT_LOGINS),
        }
    }

    /// Remaining lockout for `subject`, or `None` if it may attempt a login now.
    fn locked_for(&self, subject: &str) -> Option<std::time::Duration> {
        let now = std::time::Instant::now();
        self.accounts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(subject)
            .and_then(|a| a.locked_until)
            .filter(|&until| until > now)
            .map(|until| until - now)
    }

    fn record_failure(&self, subject: &str) {
        let mut map = self.accounts.lock().unwrap_or_else(|e| e.into_inner());
        // Prune expired, unlocked entries before growing, so a spray can't balloon the map.
        if map.len() >= MAX_TRACKED_ACCOUNTS {
            let now = std::time::Instant::now();
            map.retain(|_, a| a.locked_until.is_some_and(|u| u > now));
        }
        if map.len() >= MAX_TRACKED_ACCOUNTS && !map.contains_key(subject) {
            return; // still full of live lockouts — the concurrency cap remains the hard limit
        }
        let a = map.entry(subject.to_string()).or_default();
        a.failures = a.failures.saturating_add(1);
        if a.failures >= LOGIN_FAILURES_BEFORE_LOCKOUT {
            let shift = (a.failures - LOGIN_FAILURES_BEFORE_LOCKOUT).min(20);
            let secs = (LOGIN_LOCKOUT_BASE_SECS << shift).min(LOGIN_LOCKOUT_MAX_SECS);
            a.locked_until = Some(std::time::Instant::now() + std::time::Duration::from_secs(secs));
        }
    }

    fn record_success(&self, subject: &str) {
        self.accounts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(subject);
    }
}

/// A `ControlPlane` service over a shared [`Registry`]. `CreateIndex` resolves against the
/// index's Iceberg source (`iceberg`); drop/list are pure registry operations.
#[derive(Clone)]
pub struct ControlPlaneService {
    registry: Arc<Registry>,
    iceberg: IcebergConfig,
    auth: SharedAuth,
    /// Optional authenticator (task-104): when set, the control plane validates the forwarded bearer
    /// itself (rather than trusting gateway-stamped metadata) before authorizing — needed so local
    /// role bindings are merged against a *verified* subject. `None` keeps the pre-task-104 behavior
    /// (trust the gateway-stamped principal/roles).
    authn: Option<SharedAuthn>,
    /// Built-in login signing key (task-128): `Some` enables the `Login` RPC, which verifies a
    /// password against the registry credential store and mints an HS256 session JWT signed with
    /// this secret (the gateway validates it with the same secret). `None` ⇒ login is `UNIMPLEMENTED`.
    session_secret: Option<Vec<u8>>,
    /// Shared login throttle (task-147 / B3) — `Arc` so the per-connection clones share lockout state.
    login_throttle: Arc<LoginThrottle>,
}

impl ControlPlaneService {
    /// A Control-Plane service over `registry`, resolving new indexes against `iceberg`,
    /// with the default no-op auth hook.
    pub fn new(registry: Arc<Registry>, iceberg: IcebergConfig) -> Self {
        Self::with_auth(registry, iceberg, default_auth())
    }

    /// As [`new`](Self::new), with a specific [auth hook](SharedAuth).
    pub fn with_auth(registry: Arc<Registry>, iceberg: IcebergConfig, auth: SharedAuth) -> Self {
        Self {
            registry,
            iceberg,
            auth,
            authn: None,
            session_secret: None,
            login_throttle: Arc::new(LoginThrottle::new()),
        }
    }

    /// Install an [authenticator](crate::authn) so the control plane validates the bearer itself
    /// (task-104) — required for role-binding enforcement that doesn't trust forwarded identity.
    pub fn with_authn(mut self, authn: SharedAuthn) -> Self {
        self.authn = Some(authn);
        self
    }

    /// Enable built-in credential login (task-128): the `Login` RPC verifies a password against the
    /// registry credential store and mints a session JWT signed with `secret`. Without this, `Login`
    /// returns `UNIMPLEMENTED`.
    pub fn with_session_secret(mut self, secret: Vec<u8>) -> Self {
        self.session_secret = Some(secret);
        self
    }

    /// Wrap as a mountable tonic [`ControlPlaneServer`].
    pub fn into_server(self) -> ControlPlaneServer<Self> {
        ControlPlaneServer::new(self)
    }

    /// Authorize `method` for the caller of `request` (task-104). Resolves the caller's identity —
    /// validating the bearer when an [authenticator](Self::with_authn) is set, else trusting the
    /// gateway-stamped principal/roles — then **merges the subject's local role bindings** before
    /// the policy check. So an admin granting a role takes effect on that subject's next call.
    fn gate<T>(&self, method: &'static str, request: &Request<T>) -> Result<AuthContext, Status> {
        let meta = request.metadata();
        let hdr = |k: &str| {
            meta.get(k)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        let (principal, mut roles, tenant) = match &self.authn {
            Some(authn) => {
                let v = authn
                    .authenticate(hdr("authorization").as_deref())
                    .map_err(|e| {
                        to_status(
                            Code::Unauthenticated,
                            WireError::new("UNAUTHENTICATED", e.to_string()),
                        )
                    })?;
                // Session revocation (task-147 / B4): reject a token minted before the subject's
                // session epoch — set when their roles change or credential is removed. Forces
                // re-authentication with the current roles rather than riding a stale embedded set.
                // Compared at second granularity (`iat` is floored seconds; the epoch is ms): since
                // floor is monotonic, a token minted at-or-after the epoch is never wrongly rejected —
                // at worst a token from the same second as the change survives <1s.
                if let Some(iat) = v.issued_at {
                    let epoch_secs = self.registry.session_epoch(&v.principal) / 1000;
                    if (iat as i64) < epoch_secs {
                        return Err(to_status(
                            Code::Unauthenticated,
                            WireError::new(
                                "UNAUTHENTICATED",
                                "session superseded by a role change — please sign in again",
                            ),
                        ));
                    }
                }
                (v.principal, v.roles, v.tenant)
            }
            None => {
                let roles = hdr(auth::ROLES_KEY)
                    .map(|s| {
                        s.split(',')
                            .map(str::trim)
                            .filter(|r| !r.is_empty())
                            .map(str::to_string)
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                (
                    hdr(auth::PRINCIPAL_KEY).unwrap_or_default(),
                    roles,
                    hdr(auth::TENANT_KEY),
                )
            }
        };
        // Merge admin-managed local role bindings (task-104), keyed by the verified subject.
        for r in self.registry.roles_for(&principal) {
            if !roles.contains(&r) {
                roles.push(r);
            }
        }
        let ctx = AuthContext {
            method,
            principal: Some(principal).filter(|p| !p.is_empty()),
            tenant,
            roles,
        };
        self.auth.authorize(&ctx).map_err(|denied| {
            to_status(
                Code::PermissionDenied,
                WireError::new("PERMISSION_DENIED", denied.reason),
            )
        })?;
        Ok(ctx)
    }
}

/// The verified subject that owns saved queries (task-106): the authenticated principal, or `""`
/// (anonymous) on an open gateway — in which case the console keeps queries in localStorage instead.
fn subject_of<T>(req: &Request<T>) -> String {
    req.metadata()
        .get(auth::PRINCIPAL_KEY)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string()
}

/// Build per-field [`FieldMapping`]s for the console's Mapping tab (task-107) from the resolved
/// definition: type / analyzer / fast / cached, key role, and the D23 reason a field can't be cached.
fn field_mappings(def: &ResolvedIndex) -> Vec<FieldMapping> {
    use growlerdb_core::FieldType::{Bool, Date, Double, Ip, Keyword, Long, Text};
    let is_pk = |path: &str| {
        def.key.identifier_fields.iter().any(|p| p == path)
            || def.key.partition_fields.iter().any(|p| p == path)
    };
    def.fields
        .iter()
        .map(|f| {
            let ty = match f.ty {
                Text => "TEXT",
                Keyword => "KEYWORD",
                Long => "LONG",
                Double => "DOUBLE",
                Bool => "BOOL",
                Date => "DATE",
                Ip => "IP",
            };
            // D23: a field that can't be cached — sensitive (never) or big text (over the cap).
            let blocked = if f.sensitive {
                "sensitive (D23)".to_string()
            } else if f
                .max_bytes
                .is_some_and(|n| n > growlerdb_core::MAX_CACHED_FIELD_BYTES)
            {
                "big text (D23)".to_string()
            } else {
                String::new()
            };
            FieldMapping {
                path: f.path.clone(),
                r#type: ty.to_string(),
                analyzer: f.analyzer.clone().unwrap_or_default(),
                fast: f.fast,
                cached: f.cached,
                pk: is_pk(&f.path),
                blocked,
            }
        })
        .collect()
}

/// Per-shard placement + coarse state for the console's Shards tab (task-108): the control-plane
/// shard map (primary + replicas per ordinal, or per window for a windowed index). A shard with an
/// assigned primary is `active`; one still awaiting assignment is `building`.
fn shard_statuses(entry: &IndexEntry) -> Vec<ShardStatus> {
    // `bounds` is the window's event-time zone-map (task-219) — carried so the live-CP gateway can
    // prune; `None`/absent for an ordinal shard.
    let from =
        |ordinal: u32, window: i64, a: &ShardAssignment, bounds: Option<(i64, i64)>| ShardStatus {
            ordinal,
            window,
            primary: a.primary.as_ref().map(|n| n.0.clone()).unwrap_or_default(),
            replicas: a.replicas.iter().map(|n| n.0.clone()).collect(),
            state: if a.is_assigned() {
                "active"
            } else {
                "building"
            }
            .to_string(),
            event_min: bounds.map(|(lo, _)| lo).unwrap_or(0),
            event_max: bounds.map(|(_, hi)| hi).unwrap_or(0),
            has_event_bounds: bounds.is_some(),
        };
    if entry.windows.is_empty() {
        entry
            .shards
            .iter()
            .map(|(ord, a)| from(*ord, 0, a, None))
            .collect()
    } else {
        // Windowed index: one cell per time window (oldest first), ordinal is its position; carry the
        // per-window event-time zone-map so a live-CP windowed gateway can prune (task-219).
        entry
            .windows
            .iter()
            .enumerate()
            .map(|(i, (w, wa))| from(i as u32, *w, &wa.assignment, wa.event_min.zip(wa.event_max)))
            .collect()
    }
}

/// The windowing config carried on `GetIndexResponse` (task-219): `Some` iff the index is windowed,
/// so a live-CP gateway can build a window router + prune without reading the registry file. Mirrors
/// `growlerdb_core::TimeWindowing`.
fn windowing_config(def: &ResolvedIndex) -> Option<WindowingConfig> {
    use growlerdb_core::WindowGranularity::{Daily, Hourly, Weekly};
    def.windowing.as_ref().map(|w| WindowingConfig {
        field: w.field.clone(),
        granularity: match w.granularity {
            Hourly => "hourly",
            Daily => "daily",
            Weekly => "weekly",
        }
        .to_string(),
        event_time_field: w.event_time_field.clone().unwrap_or_default(),
        hot_windows: w.hot_windows.map(|n| n as u32).unwrap_or(0),
        has_hot_windows: w.hot_windows.is_some(),
        // The window field's format, so the connector normalizes each row's window value to canonical
        // micros exactly as the engine does (task-219). "" = a native DATE already in micros.
        field_format: def
            .fields
            .iter()
            .find(|f| f.path == w.field)
            .and_then(|f| f.format)
            .map(time_format_str)
            .unwrap_or_default()
            .to_string(),
    })
}

/// A [`TimeFormat`](growlerdb_core::TimeFormat) as its serde snake_case wire name (task-219) — what
/// the connector maps back to a normalization when computing a row's window id.
fn time_format_str(f: growlerdb_core::TimeFormat) -> &'static str {
    use growlerdb_core::TimeFormat::*;
    match f {
        EpochSeconds => "epoch_seconds",
        EpochMillis => "epoch_millis",
        EpochMicros => "epoch_micros",
        EpochNanos => "epoch_nanos",
        Rfc3339 => "rfc3339",
        DateOnly => "date",
    }
}

/// API-token → wire metadata (task-105): never includes the hash or secret.
fn token_meta(t: ApiToken) -> ApiTokenMeta {
    ApiTokenMeta {
        id: t.id,
        label: t.label,
        prefix: t.prefix,
        roles: t.roles,
        owner: t.owner,
        created_at_ms: t.created_at_ms,
    }
}

fn wire_saved(q: SavedQuery) -> WireSavedQuery {
    WireSavedQuery {
        id: q.id,
        name: q.name,
        owner: q.owner,
        query: q.query,
        state: q.state,
        shared: q.shared,
        created_at_ms: q.created_at_ms,
    }
}

fn core_saved(q: WireSavedQuery) -> SavedQuery {
    SavedQuery {
        id: q.id,
        name: q.name,
        owner: q.owner,
        query: q.query,
        state: q.state,
        shared: q.shared,
        created_at_ms: q.created_at_ms,
    }
}

/// Map a registry error to a gRPC status.
fn registry_status(e: RegistryError) -> Status {
    match e {
        RegistryError::AlreadyExists(name) => to_status(
            Code::AlreadyExists,
            WireError::new("ALREADY_EXISTS", format!("index `{name}` already exists")),
        ),
        RegistryError::NotFound(name) => to_status(
            Code::NotFound,
            WireError::new("NOT_FOUND", format!("index `{name}` not found")),
        ),
        RegistryError::AliasNotFound(name) => to_status(
            Code::NotFound,
            WireError::new("NOT_FOUND", format!("alias `{name}` not found")),
        ),
        RegistryError::SavedQueryNotFound(id) => to_status(
            Code::NotFound,
            WireError::new("NOT_FOUND", format!("saved query `{id}` not found")),
        ),
        RegistryError::AliasNameClash(name) => to_status(
            Code::InvalidArgument,
            WireError::new(
                "INVALID_ARGUMENT",
                format!("alias `{name}` clashes with an existing index name"),
            ),
        ),
        other => to_status(
            Code::Internal,
            WireError::new("INTERNAL", other.to_string()),
        ),
    }
}

/// The control plane's wall clock in epoch ms — the authority for windowed-node heartbeat liveness
/// (task-219), so a node's own (possibly skewed) clock never decides whether it's in the pool.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A planned **growth** reshard (task-77): the new bucket map plus which shards to (re)build from
/// source before the cutover and which to trim after it. `(ordinal, endpoint)` pairs name the node
/// serving each shard.
struct GrowthReshard {
    /// The bucket map to commit at the cutover.
    map: BucketMap,
    /// New shards (ordinals `current..new`) to build from source filtered to their buckets, before
    /// the cutover — old shards stay complete until then, so reads never miss.
    build: Vec<(u32, String)>,
    /// Old shards (ordinals `0..current`) whose now-dead buckets are trimmed after the cutover.
    trim: Vec<(u32, String)>,
}

/// Plan a growth reshard, or reject it. Growth-only so existing shards keep their data and reads
/// stay correct: `reassign` must move buckets **only onto new shards** (`to >= current_count`), and
/// every ordinal `0..new_count` must already have a serving node (the new shards registered with
/// their ordinal, task-77 slice 3c-c). Pure over the plan + shard map, so it's unit-tested without
/// a cluster.
fn plan_growth_reshard(
    reassignment: &Reassignment,
    shard_map: &BTreeMap<u32, ShardAssignment>,
    current_count: u32,
    new_count: u32,
) -> Result<GrowthReshard, String> {
    if new_count <= current_count {
        return Err(format!(
            "apply-reshard grows an index; new shard count {new_count} must exceed the current {current_count} \
             (shrink/rebalance is not supported online)"
        ));
    }
    // Growth invariant: every relocated bucket lands on a *new* shard, so no existing shard needs a
    // bucket it doesn't already hold (which would force a pre-cutover rebuild → a read gap).
    if let Some((bucket, _, to)) = reassignment
        .moved
        .iter()
        .find(|(_, _, to)| *to < current_count)
    {
        return Err(format!(
            "reassignment moves bucket {bucket} onto existing shard {to} (a rebalance); apply-reshard \
             supports growth only"
        ));
    }
    let endpoint = |ord: u32| -> Result<String, String> {
        shard_map
            .get(&ord)
            .and_then(|a| a.primary.as_ref())
            .map(|n| n.0.clone())
            .ok_or_else(|| {
                format!(
                    "shard {ord} has no assigned node — bring up + register shards \
                     {current_count}..{new_count} (`serve --shards {new_count} --shard-ordinal K`) first"
                )
            })
    };
    let mut build = Vec::new();
    let mut trim = Vec::new();
    for ord in 0..new_count {
        let ep = endpoint(ord)?;
        if ord >= current_count {
            build.push((ord, ep));
        } else {
            trim.push((ord, ep));
        }
    }
    Ok(GrowthReshard {
        map: reassignment.map.clone(),
        build,
        trim,
    })
}

/// Drive a **filtered reindex** on the node serving one shard (task-77): connect its Admin gRPC and
/// rebuild the shard from source keeping only the buckets it owns under `owners`. The per-node data
/// step of a reshard, reusing the write-fenced reindex (slice 3b).
async fn reindex_shard_on_node(
    endpoint: &str,
    index: &str,
    owners: &[u32],
    ordinal: u32,
) -> Result<(), Status> {
    let mut client = AdminClient::connect(endpoint.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("connecting to node `{endpoint}`: {e}")))?;
    client
        .reindex_index(ReindexIndexRequest {
            index: index.to_string(),
            bucket_owners: owners.to_vec(),
            shard_ordinal: ordinal,
        })
        .await?;
    Ok(())
}

#[tonic::async_trait]
impl ControlPlane for ControlPlaneService {
    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<CreateIndexResponse>, Status> {
        self.gate("CreateIndex", &request)?;
        let req = request.into_inner();

        // Parse first — the name is needed before resolving, so a duplicate is rejected
        // (cheaply, no source connect) ahead of an Iceberg round-trip.
        let def = IndexDefinition::from_yaml(&req.definition_yaml)
            .map_err(|e| Status::invalid_argument(format!("invalid definition: {e}")))?;
        let name = def.name.clone();
        if self.registry.get(&name).is_some() {
            return Err(registry_status(RegistryError::AlreadyExists(name)));
        }

        // Resolve against the source schema, then register.
        let Source::Iceberg(src) = &def.source;
        let table = src.table.clone();
        let reader = IcebergReader::connect(&self.iceberg)
            .await
            .map_err(|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())))?;
        let source = reader
            .read_source_schema(&table)
            .await
            .map_err(|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())))?;
        let resolved = def
            .resolve(&source)
            .map_err(|e| Status::invalid_argument(format!("definition does not resolve: {e}")))?;
        // Surface non-fatal resolution warnings in the response *and* the log — e.g. the
        // `PREDICATE` location strategy's honest-scope note (hydration latency depends on
        // the source layout; task-184 / D30), or an equality-delete reconcile fallback.
        let warnings = resolved.warnings.clone();
        for w in &warnings {
            eprintln!("create index `{name}`: warning: {w}");
        }
        self.registry.create(resolved).map_err(registry_status)?;
        self.registry
            .record_activity(&name, "index.created", format!("index `{name}` created"));

        Ok(Response::new(CreateIndexResponse { name, warnings }))
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        self.gate("DropIndex", &request)?;
        let req = request.into_inner();
        self.registry
            .drop_index(&req.name)
            .map_err(registry_status)?;
        Ok(Response::new(DropIndexResponse {}))
    }

    async fn get_index(
        &self,
        request: Request<GetIndexRequest>,
    ) -> Result<Response<GetIndexResponse>, Status> {
        self.gate("GetIndex", &request)?;
        let name = request.into_inner().name;
        let entry = self
            .registry
            .get(&name)
            .ok_or_else(|| registry_status(RegistryError::NotFound(name.clone())))?;
        // Routing config the connector must match: shard count from the shard map, strategy
        // resolved from the definition (the same source the Gateway's router uses) — task-69.
        let routing = match entry.definition.routing_strategy() {
            growlerdb_core::RoutingStrategy::Hash => WireRouting::RoutingHash,
            growlerdb_core::RoutingStrategy::Partition => WireRouting::RoutingPartition,
        };
        Ok(Response::new(GetIndexResponse {
            name,
            status: status_str(entry.status).to_string(),
            shard_count: entry.shards.len() as u32,
            routing: routing as i32,
            // Empty ⇒ legacy routing (task-77); present ⇒ writers/readers route through this map.
            bucket_owners: entry.bucket_owners.clone(),
            // Per-field mapping for the console's Mapping tab (task-107).
            fields: field_mappings(&entry.definition),
            // Per-shard placement for the Shards tab (task-108).
            shard_status: shard_statuses(&entry),
            // Windowing config for a windowed index (task-219) — lets a live-CP gateway build a
            // window router + prune; `None` for an ordinal index.
            windowing: windowing_config(&entry.definition),
        }))
    }

    async fn plan_reshard(
        &self,
        request: Request<PlanReshardRequest>,
    ) -> Result<Response<PlanReshardResponse>, Status> {
        self.gate("PlanReshard", &request)?;
        let req = request.into_inner();
        // Read-only: compute the bounded bucket→shard reassignment to reach the new count without
        // applying it. The move list is the migration work for the online cutover (task-77).
        let plan = self
            .registry
            .plan_reshard(&req.index, req.new_shard_count)
            .map_err(registry_status)?;
        Ok(Response::new(PlanReshardResponse {
            bucket_count: growlerdb_core::routing::NUM_BUCKETS,
            moved: plan
                .moved
                .into_iter()
                .map(|(bucket, from_shard, to_shard)| BucketMove {
                    bucket,
                    from_shard,
                    to_shard,
                })
                .collect(),
        }))
    }

    async fn apply_reshard(
        &self,
        request: Request<ApplyReshardRequest>,
    ) -> Result<Response<ApplyReshardResponse>, Status> {
        self.gate("ApplyReshard", &request)?;
        let req = request.into_inner();

        // 1. Plan the reassignment and validate it as a safe growth reshard.
        let plan = self
            .registry
            .plan_reshard(&req.index, req.new_shard_count)
            .map_err(registry_status)?;
        let shard_map = self
            .registry
            .shard_map(&req.index)
            .ok_or_else(|| registry_status(RegistryError::NotFound(req.index.clone())))?;
        // The count the data is **currently routed over** — the stored bucket map's shard count, not
        // the registered-node count (which already includes the new shards). A legacy index has no
        // map; its first reshard adopts a balanced map over its current shard count (task-77).
        let current_count = self
            .registry
            .bucket_map(&req.index)
            .map(|m| m.shards())
            .unwrap_or(shard_map.len() as u32);
        let growth = plan_growth_reshard(&plan, &shard_map, current_count, req.new_shard_count)
            .map_err(Status::failed_precondition)?;
        let owners = growth.map.owners().to_vec();

        // 2. Build the new shards from source (filtered) BEFORE the cutover — the old shards are
        //    untouched and still complete, so reads via the current map never miss.
        for (ord, endpoint) in &growth.build {
            reindex_shard_on_node(endpoint, &req.index, &owners, *ord).await?;
        }

        // 3. Cutover: commit the new bucket map atomically. Reads/writes now route through it.
        self.registry
            .set_bucket_map(&req.index, &growth.map)
            .map_err(registry_status)?;

        // 4. Trim the old shards' now-dead buckets (best-effort — the index is already correct; this
        //    only reclaims space). Safe post-cutover: those buckets no longer route to old shards.
        let mut trimmed = Vec::new();
        for (ord, endpoint) in &growth.trim {
            match reindex_shard_on_node(endpoint, &req.index, &owners, *ord).await {
                Ok(()) => trimmed.push(*ord),
                Err(e) => eprintln!(
                    "apply-reshard `{}`: post-cutover trim of shard {ord} failed (non-fatal): {e}",
                    req.index
                ),
            }
        }

        // Record the reshard on the index's activity log (task-135) — a material lifecycle event,
        // alongside index.created / alias.swapped. (Per-document ingestion is intentionally not
        // logged here; the Activity tab is the index's lifecycle/admin audit trail.)
        self.registry.record_activity(
            &req.index,
            "reshard",
            format!("resharded to {} shards", req.new_shard_count),
        );

        Ok(Response::new(ApplyReshardResponse {
            bucket_count: growlerdb_core::routing::NUM_BUCKETS,
            moved: plan
                .moved
                .into_iter()
                .map(|(bucket, from_shard, to_shard)| BucketMove {
                    bucket,
                    from_shard,
                    to_shard,
                })
                .collect(),
            built_shards: growth.build.iter().map(|(o, _)| *o).collect(),
            trimmed_shards: trimmed,
        }))
    }

    async fn move_bucket(
        &self,
        request: Request<MoveBucketRequest>,
    ) -> Result<Response<MoveBucketResponse>, Status> {
        self.gate("MoveBucket", &request)?;
        let req = request.into_inner();

        // Skew relief applies to a **bucketed** index (a legacy index has no buckets to move).
        let map = self.registry.bucket_map(&req.index).ok_or_else(|| {
            Status::failed_precondition(format!(
                "index `{}` is not bucketed (legacy routing); a reshard establishes buckets first",
                req.index
            ))
        })?;
        let from_shard = map.owner(req.bucket);
        if from_shard == req.to_shard {
            return Err(Status::failed_precondition(format!(
                "bucket {} already lives on shard {}",
                req.bucket, req.to_shard
            )));
        }
        // The new map with just this bucket relocated (validates ranges + non-emptying).
        let new_map = map
            .with_owner(req.bucket, req.to_shard)
            .map_err(Status::invalid_argument)?;
        let owners = new_map.owners().to_vec();

        let shard_map = self
            .registry
            .shard_map(&req.index)
            .ok_or_else(|| registry_status(RegistryError::NotFound(req.index.clone())))?;
        let endpoint = |ord: u32| -> Result<String, Status> {
            shard_map
                .get(&ord)
                .and_then(|a| a.primary.as_ref())
                .map(|n| n.0.clone())
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "shard {ord} of `{}` has no node",
                        req.index
                    ))
                })
        };
        let to_endpoint = endpoint(req.to_shard)?;
        let from_endpoint = endpoint(from_shard)?;

        // 1. Build the target shard to **include** the bucket — the source shard is untouched and
        //    still serves it, so reads never miss; the brief overlap is deduped by the Gateway.
        reindex_shard_on_node(&to_endpoint, &req.index, &owners, req.to_shard).await?;
        // 2. Cutover: commit the relocated map. The bucket now routes to the target.
        self.registry
            .set_bucket_map(&req.index, &new_map)
            .map_err(registry_status)?;
        // 3. Trim the source shard (best-effort) — it no longer owns the bucket.
        if let Err(e) = reindex_shard_on_node(&from_endpoint, &req.index, &owners, from_shard).await
        {
            eprintln!(
                "move-bucket `{}`: post-cutover trim of shard {from_shard} failed (non-fatal): {e}",
                req.index
            );
        }

        Ok(Response::new(MoveBucketResponse {
            bucket: req.bucket,
            from_shard,
            to_shard: req.to_shard,
        }))
    }

    async fn describe_source(
        &self,
        request: Request<DescribeSourceRequest>,
    ) -> Result<Response<DescribeSourceResponse>, Status> {
        self.gate("DescribeSource", &request)?;
        let table = request.into_inner().table;
        let reader = IcebergReader::connect(&self.iceberg)
            .await
            .map_err(|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())))?;
        let schema = reader
            .read_source_schema(&table)
            .await
            .map_err(|e| to_status(Code::Internal, WireError::new("INTERNAL", e.to_string())))?;
        let fields = schema
            .fields
            .into_iter()
            .map(|f| SourceFieldInfo {
                path: f.path,
                r#type: source_type_str(f.ty).to_string(),
            })
            .collect();
        Ok(Response::new(DescribeSourceResponse {
            fields,
            partition_fields: schema.partition_fields,
            identifier_fields: schema.identifier_fields,
        }))
    }

    async fn list_indexes(
        &self,
        request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        self.gate("ListIndexes", &request)?;
        let indexes = self
            .registry
            .list()
            .into_iter()
            .map(|s| WireSummary {
                name: s.name,
                status: status_str(s.status).to_string(),
            })
            .collect();
        Ok(Response::new(ListIndexesResponse { indexes }))
    }

    async fn set_alias(
        &self,
        request: Request<SetAliasRequest>,
    ) -> Result<Response<SetAliasResponse>, Status> {
        self.gate("SetAlias", &request)?;
        let req = request.into_inner();
        if req.alias.is_empty() {
            return Err(Status::invalid_argument("alias name is required"));
        }
        let alias = req.alias.clone();
        let targets = req.targets.clone();
        self.registry
            .set_alias(&req.alias, req.targets)
            .map_err(registry_status)?;
        // Record on each target so the alias swap shows in that index's activity (task-110).
        for target in &targets {
            self.registry.record_activity(
                target,
                "alias.swapped",
                format!("alias `{alias}` → `{target}` swapped"),
            );
        }
        Ok(Response::new(SetAliasResponse {}))
    }

    async fn drop_alias(
        &self,
        request: Request<DropAliasRequest>,
    ) -> Result<Response<DropAliasResponse>, Status> {
        self.gate("DropAlias", &request)?;
        self.registry
            .drop_alias(&request.into_inner().alias)
            .map_err(registry_status)?;
        Ok(Response::new(DropAliasResponse {}))
    }

    async fn list_activity(
        &self,
        request: Request<ListActivityRequest>,
    ) -> Result<Response<ListActivityResponse>, Status> {
        self.gate("ListActivity", &request)?;
        let req = request.into_inner();
        let events = self
            .registry
            .list_activity(&req.index, req.limit as usize)
            .into_iter()
            .map(|e| WireActivity {
                ts_ms: e.ts_ms,
                kind: e.kind,
                message: e.message,
            })
            .collect();
        Ok(Response::new(ListActivityResponse { events }))
    }

    /// Built-in credential login (task-128): verify the password against the registry store and mint
    /// a session JWT. **Unauthenticated** — it establishes auth, so no `gate()`. `UNIMPLEMENTED` when
    /// the deployment isn't running built-in auth (no signing secret configured).
    async fn login(
        &self,
        request: Request<LoginRequest>,
    ) -> Result<Response<LoginResponse>, Status> {
        let Some(secret) = &self.session_secret else {
            return Err(Status::unimplemented(
                "built-in login is not enabled on this deployment",
            ));
        };
        let req = request.into_inner();
        if req.username.is_empty() {
            return Err(Status::invalid_argument("username is required"));
        }
        // Rate-limit online guessing (task-147 / B3): a locked account is rejected *before* the
        // CPU-heavy Argon2 verify, so lockout also caps the unauthenticated CPU cost.
        if let Some(remaining) = self.login_throttle.locked_for(&req.username) {
            growlerdb_telemetry::sli::login("locked");
            return Err(Status::unavailable(format!(
                "too many failed attempts; retry in {}s",
                remaining.as_secs().max(1)
            )));
        }
        // Bound concurrent Argon2 verifications so a burst can't exhaust CPU.
        let _permit = match self.login_throttle.concurrency.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                growlerdb_telemetry::sli::login("busy");
                return Err(Status::unavailable("login is busy; retry shortly"));
            }
        };
        // Constant-ish failure: an unknown subject and a wrong password are indistinguishable (I10).
        if !self
            .registry
            .verify_credential(&req.username, &req.password)
        {
            self.login_throttle.record_failure(&req.username);
            growlerdb_telemetry::sli::login("bad_credential");
            return Err(Status::unauthenticated("invalid username or password"));
        }
        self.login_throttle.record_success(&req.username);
        growlerdb_telemetry::sli::login("success");
        let roles = self.registry.roles_for(&req.username);
        let token = crate::authn::mint_session_jwt(
            secret,
            &req.username,
            &roles,
            crate::authn::BUILTIN_SESSION_ISSUER,
            crate::authn::BUILTIN_SESSION_AUDIENCE,
            crate::authn::BUILTIN_SESSION_TTL_SECS,
            None,
        )
        .map_err(|e| Status::internal(e.to_string()))?;
        let expires_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0)
            + (crate::authn::BUILTIN_SESSION_TTL_SECS as i64) * 1000;
        Ok(Response::new(LoginResponse {
            token,
            expires_at_ms,
            roles,
        }))
    }

    async fn list_aliases(
        &self,
        request: Request<ListAliasesRequest>,
    ) -> Result<Response<ListAliasesResponse>, Status> {
        self.gate("ListAliases", &request)?;
        let aliases = self
            .registry
            .list_aliases()
            .into_iter()
            .map(|(alias, targets)| AliasEntry { alias, targets })
            .collect();
        Ok(Response::new(ListAliasesResponse { aliases }))
    }

    async fn list_saved_queries(
        &self,
        request: Request<ListSavedQueriesRequest>,
    ) -> Result<Response<ListSavedQueriesResponse>, Status> {
        self.gate("ListSavedQueries", &request)?;
        let owner = subject_of(&request);
        let queries = self
            .registry
            .list_saved_queries(&owner)
            .into_iter()
            .map(wire_saved)
            .collect();
        Ok(Response::new(ListSavedQueriesResponse { queries }))
    }

    async fn save_saved_query(
        &self,
        request: Request<SaveSavedQueryRequest>,
    ) -> Result<Response<SaveSavedQueryResponse>, Status> {
        self.gate("SaveSavedQuery", &request)?;
        let owner = subject_of(&request);
        let q = request
            .into_inner()
            .query
            .ok_or_else(|| Status::invalid_argument("query is required"))?;
        if q.name.trim().is_empty() && q.query.trim().is_empty() {
            return Err(Status::invalid_argument(
                "a saved query needs a name or a query",
            ));
        }
        let saved = self
            .registry
            .save_saved_query(core_saved(q), &owner)
            .map_err(registry_status)?;
        Ok(Response::new(SaveSavedQueryResponse {
            query: Some(wire_saved(saved)),
        }))
    }

    async fn delete_saved_query(
        &self,
        request: Request<DeleteSavedQueryRequest>,
    ) -> Result<Response<DeleteSavedQueryResponse>, Status> {
        self.gate("DeleteSavedQuery", &request)?;
        let owner = subject_of(&request);
        self.registry
            .delete_saved_query(&request.into_inner().id, &owner)
            .map_err(registry_status)?;
        Ok(Response::new(DeleteSavedQueryResponse {}))
    }

    async fn list_users(
        &self,
        request: Request<ListUsersRequest>,
    ) -> Result<Response<ListUsersResponse>, Status> {
        self.gate("ListUsers", &request)?;
        let users = self
            .registry
            .list_role_bindings()
            .into_iter()
            .map(|(subject, roles)| RoleBinding { subject, roles })
            .collect();
        Ok(Response::new(ListUsersResponse { users }))
    }

    async fn set_user_roles(
        &self,
        request: Request<SetUserRolesRequest>,
    ) -> Result<Response<SetUserRolesResponse>, Status> {
        let ctx = self.gate("SetUserRoles", &request)?;
        let req = request.into_inner();
        if req.subject.trim().is_empty() {
            return Err(Status::invalid_argument("subject is required"));
        }
        // Prevent privilege escalation (task-147 / F3): a caller can only assign roles that are
        // assignable and whose scopes it already holds.
        crate::rbac::check_assignable(&ctx.roles, &req.roles).map_err(|reason| {
            to_status(
                Code::PermissionDenied,
                WireError::new("PERMISSION_DENIED", reason),
            )
        })?;
        self.registry
            .set_user_roles(&req.subject, req.roles)
            .map_err(registry_status)?;
        let roles = self.registry.roles_for(&req.subject);
        Ok(Response::new(SetUserRolesResponse {
            user: Some(RoleBinding {
                subject: req.subject,
                roles,
            }),
        }))
    }

    async fn list_roles(
        &self,
        request: Request<ListRolesRequest>,
    ) -> Result<Response<ListRolesResponse>, Status> {
        self.gate("ListRoles", &request)?;
        Ok(Response::new(ListRolesResponse {
            roles: crate::rbac::ASSIGNABLE_ROLES
                .iter()
                .map(|s| s.to_string())
                .collect(),
        }))
    }

    async fn create_token(
        &self,
        request: Request<CreateTokenRequest>,
    ) -> Result<Response<CreateTokenResponse>, Status> {
        let ctx = self.gate("CreateToken", &request)?;
        let req = request.into_inner();
        if req.label.trim().is_empty() {
            return Err(Status::invalid_argument("a token needs a label"));
        }
        // Prevent privilege escalation (task-147 / F3): a token can't carry roles/scopes the
        // minting caller doesn't already hold.
        crate::rbac::check_assignable(&ctx.roles, &req.roles).map_err(|reason| {
            to_status(
                Code::PermissionDenied,
                WireError::new("PERMISSION_DENIED", reason),
            )
        })?;
        let owner = ctx.principal.clone().unwrap_or_default();
        // Mint the secret + hash here; only the hash is persisted (the secret is returned once).
        let (secret, hash) = crate::authn::mint_api_token();
        let prefix: String = secret.chars().take(13).collect();
        let token = ApiToken {
            id: self.registry.next_token_id(),
            label: req.label,
            prefix,
            hash,
            roles: req.roles,
            owner,
            created_at_ms: 0,    // the registry stamps this on create
            expires_at_ms: None, // no expiry by default (B13); a request-supplied TTL is a follow-up
        };
        let token = self.registry.create_token(token).map_err(registry_status)?;
        Ok(Response::new(CreateTokenResponse {
            token: Some(token_meta(token)),
            secret,
        }))
    }

    async fn list_tokens(
        &self,
        request: Request<ListTokensRequest>,
    ) -> Result<Response<ListTokensResponse>, Status> {
        self.gate("ListTokens", &request)?;
        Ok(Response::new(ListTokensResponse {
            tokens: self
                .registry
                .list_tokens()
                .into_iter()
                .map(token_meta)
                .collect(),
        }))
    }

    async fn revoke_token(
        &self,
        request: Request<RevokeTokenRequest>,
    ) -> Result<Response<RevokeTokenResponse>, Status> {
        self.gate("RevokeToken", &request)?;
        self.registry
            .revoke_token(&request.into_inner().id)
            .map_err(registry_status)?;
        Ok(Response::new(RevokeTokenResponse {}))
    }

    async fn register_served_index(
        &self,
        request: Request<RegisterServedIndexRequest>,
    ) -> Result<Response<RegisterServedIndexResponse>, Status> {
        self.gate("RegisterServedIndex", &request)?;
        let req = request.into_inner();
        if req.endpoint.is_empty() {
            return Err(Status::invalid_argument("endpoint is required"));
        }
        // The node ships its already-resolved definition (its `index.json`), so registration is a
        // pure registry op — no source round-trip (unlike CreateIndex, which resolves YAML).
        let resolved: ResolvedIndex = serde_json::from_str(&req.definition_json)
            .map_err(|e| Status::invalid_argument(format!("invalid definition_json: {e}")))?;
        let name = resolved.name.clone();
        let shard_count = req.shard_count.max(1);

        // Classify by the DEFINITION, not by whether `windows` is populated (task-219): a windowed
        // node that starts **empty** (streaming-first — it creates windows on first write) still
        // reports zero windows, and must register as a *windowed* entry so `ResolveWindowOwner` can
        // place windows on it — not be misclassified as an ordinal single-shard index.
        let is_windowed = resolved.windowing.is_some();
        // Upsert: create on first announce, idempotent on restart (a re-announce just re-points
        // the shard/window map at the — possibly new — endpoint below).
        if self.registry.get(&name).is_none() {
            self.registry.create(resolved).map_err(registry_status)?;
        }
        if !is_windowed {
            // Ordinal shard map. A node serving specific ordinals (task-77 multi-node sharding)
            // claims only those; otherwise (single-node default) it claims all 0..count.
            let owned: Vec<u32> = if req.shard_ordinals.is_empty() {
                (0..shard_count).collect()
            } else {
                req.shard_ordinals.clone()
            };
            for &shard in &owned {
                if shard >= shard_count {
                    return Err(Status::invalid_argument(format!(
                        "shard ordinal {shard} is out of range for a {shard_count}-shard index"
                    )));
                }
            }
            // One persist for all this node's ordinals (task-202), not one rewrite per ordinal.
            self.registry
                .assign_primaries(&name, &owned, req.endpoint.clone())
                .map_err(registry_status)?;
        } else {
            // Windowed (task-81/219): place each served window on this node and record its event-time
            // zone-map so the gateway can prune. `windows` may be empty (an empty streaming node) —
            // the entry still exists + activates below so placement can proceed.
            for w in &req.windows {
                self.registry
                    .assign_window(&name, w.window, req.endpoint.clone())
                    .map_err(registry_status)?;
                if w.has_event_bounds {
                    self.registry
                        .set_window_bounds(&name, w.window, Some(w.event_min), Some(w.event_max))
                        .map_err(registry_status)?;
                }
            }
        }
        self.registry.activate(&name).map_err(registry_status)?;
        Ok(Response::new(RegisterServedIndexResponse { name }))
    }

    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        self.gate("RegisterNode", &request)?;
        let req = request.into_inner();
        if req.index.is_empty() || req.endpoint.is_empty() {
            return Err(Status::invalid_argument("index and endpoint are required"));
        }
        // A liveness heartbeat into the CP placement pool (task-219); the CP stamps its own clock so a
        // skewed node clock can't fake liveness. In-memory only — no persist.
        self.registry
            .register_node(&req.index, &req.endpoint, now_ms());
        Ok(Response::new(RegisterNodeResponse {}))
    }

    async fn resolve_window_owner(
        &self,
        request: Request<ResolveWindowOwnerRequest>,
    ) -> Result<Response<ResolveWindowOwnerResponse>, Status> {
        self.gate("ResolveWindowOwner", &request)?;
        let req = request.into_inner();
        let (endpoint, created) = self
            .registry
            .resolve_window_owner(&req.index, req.window, now_ms())
            .map_err(|e| match e {
                // No node has heartbeated yet (a transient bring-up state) — retryable, so the
                // connector backs off and re-asks rather than failing the ingest batch.
                RegistryError::NoLiveNode { .. } => Status::unavailable(e.to_string()),
                other => registry_status(other),
            })?;
        Ok(Response::new(ResolveWindowOwnerResponse {
            endpoint,
            created,
        }))
    }

    async fn ingestion_status(
        &self,
        request: Request<IngestionStatusRequest>,
    ) -> Result<Response<IngestionStatusResponse>, Status> {
        self.gate("IngestionStatus", &request)?;
        let filter = request.into_inner().index;
        let names: Vec<String> = if filter.is_empty() {
            self.registry.list().into_iter().map(|s| s.name).collect()
        } else {
            vec![filter]
        };

        Ok(Response::new(IngestionStatusResponse {
            items: self.collect_ingestion(names).await,
        }))
    }
}

impl ControlPlaneService {
    /// Build the per-index ingestion status for `names` (source head vs each shard's committed
    /// checkpoint) AND export the `growlerdb_ingest_lag_ms` / `growlerdb_shards_up|total` gauges
    /// (task-143). Gate-free so both the `IngestionStatus` RPC and the background metrics sampler
    /// ([`spawn_ingestion_metrics_sampler`](Self::spawn_ingestion_metrics_sampler)) reuse it.
    async fn collect_ingestion(&self, names: Vec<String>) -> Vec<IndexIngestion> {
        // One catalog connection for all source-head reads. Best-effort: if the source can't be
        // read, lag is reported "unknown" rather than failing the whole status call.
        let reader = IcebergReader::connect(&self.iceberg).await.ok();

        let mut items = Vec::with_capacity(names.len());
        for name in names {
            let Some(entry) = self.registry.get(&name) else {
                continue;
            };
            let Source::Iceberg(src) = &entry.definition.source;
            let source_table = src.table.clone();
            // A windowed index has no ordinal shards — its placement lives in the `windows` map, so the
            // ingestion probe iterates windows instead (task-226).
            let windowed = entry.definition.windowing.is_some();
            let routing = match entry.definition.routing_strategy() {
                growlerdb_core::RoutingStrategy::Hash => WireRouting::RoutingHash,
                growlerdb_core::RoutingStrategy::Partition => WireRouting::RoutingPartition,
            };

            // Source head (the position ingestion is racing to catch up to).
            let (source_snapshot_id, source_timestamp_ms, source_readable) = match &reader {
                Some(r) => match r.current_snapshot(&source_table).await {
                    Ok((id, ts)) => (id, ts, true),
                    Err(_) => (0, 0, false),
                },
                None => (0, 0, false),
            };
            // Snapshot id → commit-timestamp, to measure how far behind each shard's committed
            // checkpoint is in wall-clock terms (task-137). Best-effort: empty map ⇒ lag unknown.
            let snapshot_ts = match &reader {
                Some(r) => r
                    .snapshot_timestamps(&source_table)
                    .await
                    .unwrap_or_default(),
                None => std::collections::HashMap::new(),
            };

            // Source-health gauges (task-197): diagnose a source that wants Iceberg maintenance
            // (small files / long snapshot history slow GrowlerDB's O(files) query path). Read from
            // snapshot metadata only — best-effort, so a failed read just skips this tick's sample.
            if let Some(r) = &reader {
                if let Ok(h) = r.source_health(&source_table).await {
                    growlerdb_telemetry::sli::source_health(
                        &name,
                        h.data_files,
                        h.bytes,
                        h.delete_files,
                        h.records,
                        h.snapshots,
                    );
                }
                // Partition skew (task-208.2): one `current_plan` (manifest read, then cached) per
                // index — O(indexes), same order as the per-index metadata this loop already does.
                // Only emitted for identity-partitioned sources; best-effort.
                if let Ok(Some(skew)) = r.partition_skew(&source_table).await {
                    growlerdb_telemetry::sli::source_partition_skew(&name, skew);
                }
            }

            // Each shard's committed checkpoint, via its primary's Write.GetCheckpoint — fetched
            // CONCURRENTLY (task-202). The old serial loop did one fresh connect + RPC per shard in
            // sequence, so at hundreds of shards a single sample took hundreds of round-trips and fell
            // behind its own cadence. A bounded JoinSet runs them in parallel; the state/lag math is
            // then a cheap synchronous pass.
            let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(SHARD_POLL_CONCURRENCY));
            let mut set = tokio::task::JoinSet::new();
            // Probe the index's shard set: ordinal shards, or the **time windows** for a windowed index
            // (task-226) — its `shards` map is empty, its `windows` map holds the placement. `ordinal`
            // and `window` are 0 for the axis that doesn't apply; the row carries the one that does.
            if windowed {
                for (window, wa) in &entry.windows {
                    let window = *window;
                    let node = wa.assignment.primary.as_ref().map(|n| n.0.clone());
                    let sem = sem.clone();
                    set.spawn(async move {
                        let Some(endpoint) = node else {
                            return (0u32, window, String::new(), Err("no_primary"));
                        };
                        let _permit = sem.acquire_owned().await;
                        let res = shard_checkpoint(&endpoint, window).await;
                        (0u32, window, endpoint, res)
                    });
                }
            } else {
                for (ordinal, assignment) in &entry.shards {
                    let ordinal = *ordinal;
                    // `node` is the primary endpoint, or "" when the shard has no primary yet.
                    let node = assignment.primary.as_ref().map(|n| n.0.clone());
                    let sem = sem.clone();
                    set.spawn(async move {
                        let Some(endpoint) = node else {
                            return (ordinal, 0i64, String::new(), Err("no_primary"));
                        };
                        let _permit = sem.acquire_owned().await;
                        let res = shard_checkpoint(&endpoint, 0).await;
                        (ordinal, 0i64, endpoint, res)
                    });
                }
            }
            let mut raw: Vec<ShardProbe> = Vec::new();
            while let Some(joined) = set.join_next().await {
                if let Ok(t) = joined {
                    raw.push(t);
                }
            }
            // Stable order for the console table: by window id (windowed) or ordinal (otherwise).
            raw.sort_by_key(|(ordinal, window, _, _)| (*window, *ordinal));

            let mut shards = Vec::with_capacity(raw.len());
            for (ordinal, window, node, res) in raw {
                let (committed, snapshot, state, lag_ms) = match res {
                    Err(state) => (0i64, 0u64, state, 0i64),
                    Ok((committed, snapshot)) => {
                        let (state, lag_ms) = ingestion_state(
                            committed,
                            source_snapshot_id,
                            source_readable,
                            snapshot_ts.get(&committed).copied(),
                            source_timestamp_ms,
                            INGESTION_LAG_TOLERANCE_MS,
                        );
                        (committed, snapshot, state, lag_ms)
                    }
                };
                shards.push(ShardIngestion {
                    ordinal,
                    node,
                    committed_snapshot_id: committed,
                    index_snapshot: snapshot,
                    state: state.to_string(),
                    lag_ms,
                    window,
                });
            }

            // Export the ingestion-lag + shard-availability gauges (task-143) so the Observability
            // grid, Grafana, and alerts can see them. `up` = shards with a reachable primary; lag =
            // the worst shard's wall-clock staleness.
            let lag_ms = shards.iter().map(|s| s.lag_ms).max().unwrap_or(0);
            let up = shards
                .iter()
                .filter(|s| s.state != "no_primary" && s.state != "unreachable")
                .count() as u64;
            growlerdb_telemetry::sli::ingest_lag_ms(&name, lag_ms);
            growlerdb_telemetry::sli::shard_availability(&name, up, shards.len() as u64);

            items.push(IndexIngestion {
                name,
                status: status_str(entry.status).to_string(),
                source_table,
                routing: routing as i32,
                shard_count: if windowed {
                    entry.windows.len() as u32
                } else {
                    entry.shards.len() as u32
                },
                source_snapshot_id,
                source_timestamp_ms,
                source_readable,
                shards,
            });
        }
        items
    }

    /// Spawn a background task that recomputes the ingestion-lag + shard-availability gauges
    /// (task-143) every `interval_secs`, independent of any console poll — so Prometheus always
    /// scrapes a fresh value even when nobody has the Ingestion page open. Cheap: it reuses the
    /// same source-head read the `IngestionStatus` RPC does. Returns immediately.
    pub fn spawn_ingestion_metrics_sampler(&self, interval_secs: u64) {
        let svc = self.clone();
        tokio::spawn(async move {
            let mut tick =
                tokio::time::interval(std::time::Duration::from_secs(interval_secs.max(1)));
            loop {
                tick.tick().await;
                let names = svc.registry.list().into_iter().map(|s| s.name).collect();
                let _ = svc.collect_ingestion(names).await; // side effect: sets the gauges
            }
        });
    }
}

/// In_sync tolerance for the Ingestion view (task-137): a shard within this much wall-clock lag of
/// the source head still reads `in_sync`. A live source commits a fresh snapshot every few seconds,
/// so a healthy connector is always momentarily a snapshot behind; without a tolerance the badge
/// flaps in_sync↔behind. Generous enough to absorb normal pipeline latency, small enough to surface
/// a genuine backlog.
const INGESTION_LAG_TOLERANCE_MS: i64 = 15_000;

/// Max concurrent per-shard `GetCheckpoint` probes in one ingestion sample (task-202). Bounds the
/// fan-out so a many-shard index doesn't open every connection at once, while still turning the old
/// serial hundreds-of-round-trips sweep into a handful of parallel batches.
const SHARD_POLL_CONCURRENCY: usize = 32;

/// One shard's concurrent checkpoint probe (task-202): `(ordinal, primary endpoint, checkpoint or
/// error state)` — `Ok((committed_snapshot, index_snapshot))`, or `Err(state)` for no-primary /
/// unreachable / source-recreated.
// (ordinal, window, node-endpoint, checkpoint result). `window` is 0 for an ordinal shard; `ordinal`
// is 0 for a windowed index's window (task-226).
type ShardProbe = (u32, i64, String, Result<(i64, u64), &'static str>);

/// Classify a shard's ingestion `state` + its `lag_ms` vs the source head (task-137). `in_sync` when
/// the shard has committed the head, or is within `tolerance_ms` of it (measured as a wall-clock
/// delta `head_ts − committed_ts` — Iceberg snapshot ids are random, so an id delta is meaningless).
/// A committed snapshot no longer in the source history (`committed_ts == None`, e.g. expired by
/// maintenance) can't be measured, so it reports `behind`. Pure, for unit testing.
fn ingestion_state(
    committed: i64,
    head_id: i64,
    source_readable: bool,
    committed_ts: Option<i64>,
    head_ts: i64,
    tolerance_ms: i64,
) -> (&'static str, i64) {
    if committed == 0 {
        return ("uninitialized", 0);
    }
    if !source_readable {
        return ("unknown", 0);
    }
    if committed == head_id {
        return ("in_sync", 0);
    }
    match committed_ts {
        Some(ts) => {
            let lag = (head_ts - ts).max(0);
            if lag <= tolerance_ms {
                ("in_sync", lag)
            } else {
                ("behind", lag)
            }
        }
        None => ("behind", 0),
    }
}

/// Read one shard's committed checkpoint (the source snapshot it reflects) + its index snapshot
/// from the shard primary's `Write.GetCheckpoint`. A fresh connect per call — the Ingestion view
/// polls at human cadence, so a pooled client isn't worth the bookkeeping.
async fn shard_checkpoint(endpoint: &str, window: i64) -> Result<(i64, u64), &'static str> {
    let mut client = WriteClient::connect(endpoint.to_string())
        .await
        .map_err(|_| "unreachable")?;
    let resp = client
        // `window` selects the time-window shard on a windowed node (task-226); 0 on an ordinal node,
        // which ignores it.
        .get_checkpoint(GetCheckpointRequest { window })
        .await
        // A node serving a stale index over a recreated source refuses the checkpoint with
        // FAILED_PRECONDITION (task-114) — surface that as a distinct `source_recreated` state, not
        // a generic transport `unreachable`.
        .map_err(|s| {
            if s.code() == tonic::Code::FailedPrecondition {
                "source_recreated"
            } else {
                "unreachable"
            }
        })?
        .into_inner();
    let committed = match resp.checkpoint.and_then(|c| c.kind) {
        Some(growlerdb_proto::v1::source_checkpoint::Kind::IcebergSnapshot(id)) => id,
        None => 0,
    };
    Ok((committed, resp.snapshot))
}

/// Render a coarse source type for the wire (the create-form introspection, task-47).
fn source_type_str(ty: growlerdb_core::SourceType) -> &'static str {
    use growlerdb_core::SourceType::*;
    match ty {
        String => "string",
        Long => "long",
        Double => "double",
        Bool => "bool",
        Date => "date",
        Binary => "binary",
        Other => "other",
    }
}

/// Render an index status for the wire.
fn status_str(status: growlerdb_controlplane::IndexStatus) -> &'static str {
    match status {
        growlerdb_controlplane::IndexStatus::Building => "building",
        growlerdb_controlplane::IndexStatus::Active => "active",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{ResolvedIndex, SourceField, SourceSchema, SourceType};
    use growlerdb_proto::v1::ServedWindow;

    #[test]
    fn ingestion_state_tolerates_a_fresh_lag_so_the_badge_doesnt_flap() {
        let tol = 15_000;
        // Caught up to the head → in_sync, no lag.
        assert_eq!(
            ingestion_state(100, 100, true, Some(5_000), 5_000, tol),
            ("in_sync", 0)
        );
        // Behind by a fresh snapshot (8s) within tolerance → in_sync (no flap), but lag is reported.
        assert_eq!(
            ingestion_state(90, 100, true, Some(2_000), 10_000, tol),
            ("in_sync", 8_000)
        );
        // Genuinely behind (30s > tolerance) → behind, with the lag.
        assert_eq!(
            ingestion_state(90, 100, true, Some(0), 30_000, tol),
            ("behind", 30_000)
        );
        // A committed snapshot expired from the source history can't be measured → behind.
        assert_eq!(
            ingestion_state(90, 100, true, None, 10_000, tol),
            ("behind", 0)
        );
        // Edge states are unchanged.
        assert_eq!(
            ingestion_state(0, 100, true, None, 10_000, tol),
            ("uninitialized", 0)
        );
        assert_eq!(
            ingestion_state(90, 100, false, Some(0), 10_000, tol),
            ("unknown", 0)
        );
    }

    fn resolved(name: &str) -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(&format!(
            "name: {name}\nsource: {{ iceberg: {{ catalog: g, table: g.{name} }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD }} ] }}\n",
        ))
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn resolved_windowed(name: &str) -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("ingest", SourceType::Long),
                SourceField::new("event", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(&format!(
            "name: {name}\nsource: {{ iceberg: {{ catalog: g, table: g.{name} }} }}\nwindowing: {{ field: ingest, granularity: daily, event_time_field: event }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD }}, {{ path: ingest, format: epoch_us, fast: true }}, {{ path: event, format: epoch_us, fast: true }} ] }}\n",
        ))
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn service(root: &std::path::Path) -> ControlPlaneService {
        let registry = Arc::new(Registry::open(root.join("registry.json")).unwrap());
        ControlPlaneService::new(registry, IcebergConfig::local())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn builtin_login_mints_a_session_token_the_gateway_accepts() {
        // task-128: /v1/login verifies a credential and mints an HS256 session JWT validatable by the
        // gateway's JwtAuthenticator with the same secret.
        let tmp = tempfile::tempdir().unwrap();
        let registry = Arc::new(Registry::open(tmp.path().join("registry.json")).unwrap());
        registry.set_credential("alice", "pw").unwrap();
        registry
            .set_user_roles("alice", vec!["admin".to_string()])
            .unwrap();
        let secret = b"shared-deployment-secret".to_vec();
        let svc = ControlPlaneService::new(registry.clone(), IcebergConfig::local())
            .with_session_secret(secret.clone());

        // Wrong password → Unauthenticated.
        assert!(svc
            .login(Request::new(LoginRequest {
                username: "alice".into(),
                password: "nope".into(),
            }))
            .await
            .is_err());

        // Correct password → a token the HS256 authenticator validates to the right subject + roles.
        let resp = svc
            .login(Request::new(LoginRequest {
                username: "alice".into(),
                password: "pw".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(resp.roles.contains(&"admin".to_string()));
        use crate::authn::Authenticator as _;
        let authn = crate::authn::JwtAuthenticator::from_hs256_secret(
            &secret,
            crate::authn::BUILTIN_SESSION_ISSUER,
            crate::authn::BUILTIN_SESSION_AUDIENCE,
        );
        let v = authn
            .authenticate(Some(&format!("Bearer {}", resp.token)))
            .unwrap();
        assert_eq!(v.principal, "alice");
        assert!(v.roles.contains(&"admin".to_string()));

        // Without a session secret configured, login is UNIMPLEMENTED.
        let open = ControlPlaneService::new(registry, IcebergConfig::local());
        assert!(open
            .login(Request::new(LoginRequest {
                username: "alice".into(),
                password: "pw".into(),
            }))
            .await
            .is_err());
    }

    #[test]
    fn login_throttle_locks_after_repeated_failures() {
        // task-147 / B3: an account is unlocked until it crosses the failure threshold, then locked
        // for a positive window; a success clears it.
        let t = LoginThrottle::new();
        for _ in 0..LOGIN_FAILURES_BEFORE_LOCKOUT - 1 {
            t.record_failure("alice");
            assert!(t.locked_for("alice").is_none(), "not yet locked");
        }
        t.record_failure("alice"); // crosses the threshold
        assert!(
            t.locked_for("alice").is_some(),
            "locked after the threshold"
        );
        // A different account is independent.
        assert!(t.locked_for("bob").is_none());
        // Success clears the lock.
        t.record_success("alice");
        assert!(t.locked_for("alice").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn role_change_revokes_outstanding_sessions() {
        // task-147 / B4: a session JWT minted before a subject's roles change (which bumps the
        // session epoch) is rejected by the control-plane gate, forcing re-authentication.
        use jsonwebtoken::{encode, get_current_timestamp, Algorithm, EncodingKey, Header};
        let tmp = tempfile::tempdir().unwrap();
        let registry = Arc::new(Registry::open(tmp.path().join("registry.json")).unwrap());
        // Start with no revocation (epoch 0). The token carries its own `reader` role, so authz
        // passes without a registry binding — and no initial set_user_roles bumps the epoch early.
        let secret = b"shared-deployment-secret".to_vec();
        let authn: crate::authn::SharedAuthn =
            Arc::new(crate::authn::JwtAuthenticator::from_hs256_secret(
                &secret,
                crate::authn::BUILTIN_SESSION_ISSUER,
                crate::authn::BUILTIN_SESSION_AUDIENCE,
            ));
        let svc = ControlPlaneService::with_auth(
            registry.clone(),
            IcebergConfig::local(),
            Arc::new(crate::rbac::RbacPolicy::with_default_roles()),
        )
        .with_authn(authn);

        // A session minted 100s ago (iat in the past), signed with the deployment secret.
        let now = get_current_timestamp();
        let claims = serde_json::json!({
            "sub": "alice", "roles": ["reader"],
            "iss": crate::authn::BUILTIN_SESSION_ISSUER,
            "aud": crate::authn::BUILTIN_SESSION_AUDIENCE,
            "exp": now + 3600, "iat": now - 100,
        });
        let stale = encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(&secret),
        )
        .unwrap();

        let call = |token: &str| {
            let mut req = Request::new(ListIndexesRequest {});
            req.metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            req
        };
        // Before any revocation the (valid, non-expired) token is accepted.
        assert!(svc.list_indexes(call(&stale)).await.is_ok());

        // Change alice's roles → bumps her session epoch to now, which is after the token's iat.
        registry
            .set_user_roles("alice", vec!["admin".to_string()])
            .unwrap();
        let err = svc.list_indexes(call(&stale)).await.unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
        assert!(err.message().contains("superseded"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn list_and_drop_over_the_registry() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());

        // Seed the registry directly (the create happy-path needs a live source).
        svc.registry.create(resolved("docs")).unwrap();
        svc.registry.create(resolved("logs")).unwrap();

        let listed = svc
            .list_indexes(Request::new(ListIndexesRequest {}))
            .await
            .unwrap()
            .into_inner()
            .indexes;
        let names: Vec<&str> = listed.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, vec!["docs", "logs"]);
        assert!(listed.iter().all(|s| s.status == "building"));

        // Drop one over the service.
        svc.drop_index(Request::new(DropIndexRequest {
            name: "logs".into(),
        }))
        .await
        .unwrap();
        assert!(svc.registry.get("logs").is_none());

        // Dropping a missing index → NotFound.
        let err = svc
            .drop_index(Request::new(DropIndexRequest {
                name: "logs".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_index_returns_rich_field_mapping() {
        // task-107: per-field type/analyzer/fast/cached/PK + a D23 block reason.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("city", SourceType::String),
                SourceField::new("body", SourceType::String),
                SourceField::new("ssn", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let def = IndexDefinition::from_yaml(
            "name: rich\nsource: { iceberg: { catalog: g, table: g.rich } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD, fast: true }, { path: body, type: TEXT, cached: true }, { path: ssn, type: KEYWORD, sensitive: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        svc.registry.create(def).unwrap();

        let resp = svc
            .get_index(Request::new(GetIndexRequest {
                name: "rich".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let f = |p: &str| resp.fields.iter().find(|x| x.path == p).expect(p).clone();
        assert!(f("id").pk, "id is the identifier key");
        assert_eq!(f("id").r#type, "KEYWORD");
        assert!(f("city").fast);
        assert!(f("body").cached);
        assert_eq!(f("body").r#type, "TEXT");
        // A sensitive field can't be cached (D23) → a block reason, not cached.
        assert!(!f("ssn").cached);
        assert!(f("ssn").blocked.contains("sensitive"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_index_returns_shard_count_and_routing() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());

        // A hash index (no partition fields) with two assigned shards; shard 0 also has a replica.
        svc.registry.create(resolved("docs")).unwrap();
        svc.registry.assign_primary("docs", 0, "node-a").unwrap();
        svc.registry.add_replica("docs", 0, "node-a2").unwrap();
        svc.registry.assign_primary("docs", 1, "node-b").unwrap();

        let resp = svc
            .get_index(Request::new(GetIndexRequest {
                name: "docs".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.name, "docs");
        assert_eq!(resp.shard_count, 2);
        assert_eq!(resp.routing, WireRouting::RoutingHash as i32);
        assert_eq!(resp.status, "building");
        // Legacy index ⇒ no bucket map vended (task-77).
        assert!(resp.bucket_owners.is_empty());

        // Per-shard placement (task-108): primary + replica + active state.
        let s0 = resp.shard_status.iter().find(|s| s.ordinal == 0).unwrap();
        assert_eq!(s0.primary, "node-a");
        assert_eq!(s0.replicas, vec!["node-a2".to_string()]);
        assert_eq!(s0.state, "active");
        assert_eq!(
            resp.shard_status
                .iter()
                .find(|s| s.ordinal == 1)
                .unwrap()
                .primary,
            "node-b"
        );

        // A missing index → NotFound.
        let err = svc
            .get_index(Request::new(GetIndexRequest {
                name: "nope".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_index_carries_windowing_config_and_event_bounds() {
        // task-219: a live-CP gateway needs the windowing config + per-window event zone-map on the
        // wire so it can build a window router and prune time-filtered queries.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        svc.registry.create(resolved_windowed("logs")).unwrap();
        // Two windows on two nodes: one with an event-time zone-map, one not bounded yet.
        let (w0, w1) = (1_700_000_000_000_i64, 1_700_086_400_000_i64);
        svc.registry.assign_window("logs", w0, "node-a").unwrap();
        svc.registry
            .set_window_bounds("logs", w0, Some(10), Some(99))
            .unwrap();
        svc.registry.assign_window("logs", w1, "node-b").unwrap();

        let resp = svc
            .get_index(Request::new(GetIndexRequest {
                name: "logs".into(),
            }))
            .await
            .unwrap()
            .into_inner();

        // Windowing config mirrors the definition (daily, ingest bucketed, event zone-map).
        let wc = resp
            .windowing
            .expect("windowed index carries a windowing config");
        assert_eq!(wc.field, "ingest");
        assert_eq!(wc.granularity, "daily");
        assert_eq!(wc.event_time_field, "event");

        // Per-window placement + zone-map: w0 has bounds, w1 doesn't yet (has_event_bounds=false).
        let s0 = resp.shard_status.iter().find(|s| s.window == w0).unwrap();
        assert_eq!(s0.primary, "node-a");
        assert!(s0.has_event_bounds);
        assert_eq!((s0.event_min, s0.event_max), (10, 99));
        let s1 = resp.shard_status.iter().find(|s| s.window == w1).unwrap();
        assert_eq!(s1.primary, "node-b");
        assert!(!s1.has_event_bounds);

        // An ordinal index carries no windowing config.
        svc.registry.create(resolved("docs")).unwrap();
        let ord = svc
            .get_index(Request::new(GetIndexRequest {
                name: "docs".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(ord.windowing.is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cp_driven_window_placement_via_rpc() {
        // task-219: nodes heartbeat into the pool (RegisterNode), the connector resolves each window's
        // owner (ResolveWindowOwner, placing on first ask), and GetIndex reflects the placement.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        svc.registry.create(resolved_windowed("logs")).unwrap();

        let resolve = |w: i64| {
            svc.resolve_window_owner(Request::new(ResolveWindowOwnerRequest {
                index: "logs".into(),
                window: w,
            }))
        };

        // With no node registered yet, placement is retryable (Unavailable), not a hard failure.
        assert_eq!(resolve(10).await.unwrap_err().code(), Code::Unavailable);

        // Two nodes register as available.
        for ep in ["http://node-a:50051", "http://node-b:50051"] {
            svc.register_node(Request::new(RegisterNodeRequest {
                index: "logs".into(),
                endpoint: ep.into(),
            }))
            .await
            .unwrap();
        }

        // Resolving four windows places each (created=true), spread evenly across the two nodes.
        let mut owners = Vec::new();
        for w in [10_i64, 20, 30, 40] {
            let r = resolve(w).await.unwrap().into_inner();
            assert!(r.created, "window {w} placed on first ask");
            owners.push(r.endpoint);
        }
        assert_eq!(
            owners
                .iter()
                .filter(|e| *e == "http://node-a:50051")
                .count(),
            2
        );
        assert_eq!(
            owners
                .iter()
                .filter(|e| *e == "http://node-b:50051")
                .count(),
            2
        );

        // Idempotent: re-resolving a placed window returns the same owner, created=false.
        let again = resolve(10).await.unwrap().into_inner();
        assert_eq!(again.endpoint, owners[0]);
        assert!(!again.created);

        // GetIndex reflects the durable placement (windowed shard_status carries window→primary) —
        // exactly what the live-CP gateway (stage 1) reads to build its window router.
        let gi = svc
            .get_index(Request::new(GetIndexRequest {
                name: "logs".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let placed: std::collections::BTreeMap<i64, String> = gi
            .shard_status
            .iter()
            .filter(|s| s.window != 0)
            .map(|s| (s.window, s.primary.clone()))
            .collect();
        assert_eq!(placed.len(), 4);
        assert_eq!(placed[&10], owners[0]);

        // Resolving a window of an unregistered index is NotFound (not a placement retry).
        assert_eq!(
            svc.resolve_window_owner(Request::new(ResolveWindowOwnerRequest {
                index: "ghost".into(),
                window: 1,
            }))
            .await
            .unwrap_err()
            .code(),
            Code::NotFound
        );
    }

    /// A shard map staffing ordinals `0..n`, each with a distinct node endpoint.
    fn staffed(n: u32) -> BTreeMap<u32, ShardAssignment> {
        (0..n)
            .map(|o| {
                (
                    o,
                    ShardAssignment {
                        primary: Some(format!("http://node{o}:50051").into()),
                        replicas: vec![],
                    },
                )
            })
            .collect()
    }

    #[test]
    fn plan_growth_reshard_accepts_growth_builds_new_and_trims_old() {
        let plan = BucketMap::balanced(2).reassign(3); // growth: buckets move only onto shard 2

        let g = plan_growth_reshard(&plan, &staffed(3), 2, 3).expect("growth plan");
        // Build the new shard (2) before the cutover; trim the old shards (0, 1) after it.
        assert_eq!(g.build.iter().map(|(o, _)| *o).collect::<Vec<_>>(), vec![2]);
        assert_eq!(
            g.trim.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(g.build[0].1, "http://node2:50051");
        assert_eq!(g.map, plan.map);
    }

    #[test]
    fn plan_growth_reshard_rejects_non_growth_and_unready_topology() {
        let plan = BucketMap::balanced(2).reassign(3);
        // new == current and new < current are both not growth.
        assert!(plan_growth_reshard(&plan, &staffed(3), 3, 3).is_err());
        assert!(plan_growth_reshard(&plan, &staffed(4), 4, 3).is_err());
        // The new shard (ordinal 2) isn't staffed yet → topology not ready.
        assert!(plan_growth_reshard(&plan, &staffed(2), 2, 3).is_err());
    }

    #[test]
    fn plan_growth_reshard_rejects_a_rebalance_onto_existing_shards() {
        // A reassignment that moves a bucket onto an existing shard (0) is a rebalance, not growth —
        // existing shard 0 would need a bucket it doesn't hold, forcing a pre-cutover read gap.
        let plan = Reassignment {
            map: BucketMap::balanced(3),
            moved: vec![(5, 2, 0)],
        };
        assert!(plan_growth_reshard(&plan, &staffed(3), 2, 3).is_err());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn move_bucket_validates_before_touching_nodes() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        svc.registry.create(resolved("docs")).unwrap();
        svc.registry.assign_primary("docs", 0, "node-a").unwrap();
        svc.registry.assign_primary("docs", 1, "node-b").unwrap();

        // A legacy index (no stored bucket map) has no buckets to move.
        let err = svc
            .move_bucket(Request::new(MoveBucketRequest {
                index: "docs".into(),
                bucket: 5,
                to_shard: 1,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);

        // Now bucketed: moving a bucket onto the shard it already lives on is rejected too.
        svc.registry
            .set_bucket_map("docs", &BucketMap::balanced(2))
            .unwrap();
        let here = svc.registry.bucket_map("docs").unwrap().owner(4);
        let err = svc
            .move_bucket(Request::new(MoveBucketRequest {
                index: "docs".into(),
                bucket: 4,
                to_shard: here,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plan_reshard_returns_a_bounded_move_list() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        svc.registry.create(resolved("docs")).unwrap();
        svc.registry.assign_primary("docs", 0, "node-a").unwrap();
        svc.registry.assign_primary("docs", 1, "node-b").unwrap();

        // Plan growing 2 → 3 shards: the response carries NUM_BUCKETS and a bounded move list.
        let resp = svc
            .plan_reshard(Request::new(PlanReshardRequest {
                index: "docs".into(),
                new_shard_count: 3,
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.bucket_count, growlerdb_core::routing::NUM_BUCKETS);
        assert!(!resp.moved.is_empty());
        assert!(resp.moved.len() < (growlerdb_core::routing::NUM_BUCKETS / 2) as usize);
        // Each move names a real destination shard in the new range.
        for m in &resp.moved {
            assert!(m.to_shard < 3, "move targets a shard outside the new count");
        }
        // Planning didn't mutate routing — the index is still legacy.
        assert!(svc.registry.bucket_map("docs").is_none());

        // Unknown index → NotFound.
        let err = svc
            .plan_reshard(Request::new(PlanReshardRequest {
                index: "nope".into(),
                new_shard_count: 3,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_index_reports_partition_routing() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());

        // A partitioned index resolves to partition routing.
        let src = SourceSchema::new(
            vec![
                SourceField::new("region", SourceType::String),
                SourceField::new("id", SourceType::String),
            ],
            vec![],
            vec![],
        );
        let part = IndexDefinition::from_yaml(
            "name: ptd\nsource: { iceberg: { catalog: g, table: g.ptd } }\nkey: { partition_fields: [region], identifier_fields: [id] }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        svc.registry.create(part).unwrap();

        let resp = svc
            .get_index(Request::new(GetIndexRequest { name: "ptd".into() }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.routing, WireRouting::RoutingPartition as i32);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn register_served_index_assigns_only_its_ordinal_for_multi_node() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();

        // Two nodes each register **one** ordinal of a 2-shard index (task-77 multi-node sharding).
        for (ord, ep) in [(0u32, "http://node-a:50051"), (1u32, "http://node-b:50051")] {
            svc.register_served_index(Request::new(RegisterServedIndexRequest {
                definition_json: def_json.clone(),
                endpoint: ep.into(),
                shard_count: 2,
                shard_ordinals: vec![ord],
                windows: vec![],
            }))
            .await
            .unwrap();
        }
        // The shard map places each node at exactly its ordinal — a correct multi-node topology.
        let entry = svc.registry.get("docs").unwrap();
        assert_eq!(entry.shards.len(), 2);
        assert_eq!(
            entry.shards.get(&0).unwrap().primary.as_ref().unwrap().0,
            "http://node-a:50051"
        );
        assert_eq!(
            entry.shards.get(&1).unwrap().primary.as_ref().unwrap().0,
            "http://node-b:50051"
        );

        // An ordinal outside `0..shard_count` is rejected.
        let err = svc
            .register_served_index(Request::new(RegisterServedIndexRequest {
                definition_json: def_json,
                endpoint: "http://node-c:50051".into(),
                shard_count: 2,
                shard_ordinals: vec![5],
                windows: vec![],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn register_served_index_upserts_assigns_and_activates() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());

        // A node announces an index it serves (definition_json = its resolved `index.json`).
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();
        let resp = svc
            .register_served_index(Request::new(RegisterServedIndexRequest {
                definition_json: def_json.clone(),
                endpoint: "http://node-a:50051".into(),
                shard_count: 1,
                shard_ordinals: vec![],
                windows: vec![],
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(resp.name, "docs");

        // It's now in the registry, active, with shard 0 assigned to the announced endpoint.
        let entry = svc.registry.get("docs").unwrap();
        assert_eq!(entry.status, growlerdb_controlplane::IndexStatus::Active);
        assert_eq!(
            entry.shards.get(&0).unwrap().primary.as_ref().unwrap().0,
            "http://node-a:50051"
        );

        // Re-announcing (a restart at a new endpoint) is idempotent and re-points the primary.
        svc.register_served_index(Request::new(RegisterServedIndexRequest {
            definition_json: def_json,
            endpoint: "http://node-b:50051".into(),
            shard_count: 1,
            shard_ordinals: vec![],
            windows: vec![],
        }))
        .await
        .unwrap();
        let entry = svc.registry.get("docs").unwrap();
        assert_eq!(
            entry.shards.get(&0).unwrap().primary.as_ref().unwrap().0,
            "http://node-b:50051"
        );

        // A missing endpoint is rejected.
        let err = svc
            .register_served_index(Request::new(RegisterServedIndexRequest {
                definition_json: serde_json::to_string(&resolved("logs")).unwrap(),
                endpoint: String::new(),
                shard_count: 1,
                shard_ordinals: vec![],
                windows: vec![],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn register_served_index_records_windows_and_zone_maps() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());

        // A node serving a windowed index announces its windows (+ event-time zone-maps) instead of
        // ordinal shards.
        svc.register_served_index(Request::new(RegisterServedIndexRequest {
            definition_json: serde_json::to_string(&resolved_windowed("events")).unwrap(),
            endpoint: "http://node-a:50051".into(),
            shard_count: 1, // ignored when windows is set
            shard_ordinals: vec![],
            windows: vec![
                ServedWindow {
                    window: 100,
                    event_min: 5,
                    event_max: 80,
                    has_event_bounds: true,
                },
                ServedWindow {
                    window: 200, // no docs yet → no zone-map reported
                    event_min: 0,
                    event_max: 0,
                    has_event_bounds: false,
                },
            ],
        }))
        .await
        .unwrap();

        let entry = svc.registry.get("events").unwrap();
        assert_eq!(entry.status, growlerdb_controlplane::IndexStatus::Active);
        assert!(entry.shards.is_empty(), "windowed → no ordinal shards");

        let wm = svc.registry.window_map("events").unwrap();
        assert_eq!(wm.len(), 2);
        let w100 = wm.get(&100).unwrap();
        assert_eq!(
            w100.assignment.primary.as_ref().unwrap().0,
            "http://node-a:50051"
        );
        assert_eq!((w100.event_min, w100.event_max), (Some(5), Some(80)));
        // No reported bounds → zone-map stays None (gateway conservatively always queries it).
        let w200 = wm.get(&200).unwrap();
        assert_eq!((w200.event_min, w200.event_max), (None, None));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ingestion_status_reports_binding_and_per_shard_state() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());

        // One index with a primary at an unreachable endpoint, plus an unassigned shard.
        svc.registry.create(resolved("docs")).unwrap();
        svc.registry
            .assign_primary("docs", 0, "http://127.0.0.1:1")
            .unwrap();
        svc.registry
            .add_replica("docs", 1, "http://127.0.0.1:1")
            .ok();

        let items = svc
            .ingestion_status(Request::new(IngestionStatusRequest {
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .items;
        assert_eq!(items.len(), 1);
        let docs = &items[0];
        assert_eq!(docs.name, "docs");
        assert_eq!(docs.source_table, "g.docs");
        // The local-dev catalog isn't up in this unit test, so the source head is unknown.
        assert!(!docs.source_readable);

        // Shard 0's primary is unreachable here; the status surfaces that rather than failing.
        let s0 = docs.shards.iter().find(|s| s.ordinal == 0).unwrap();
        assert_eq!(s0.node, "http://127.0.0.1:1");
        assert_eq!(s0.state, "unreachable");

        // Filtering to a missing index yields no items (not an error).
        let none = svc
            .ingestion_status(Request::new(IngestionStatusRequest {
                index: "nope".into(),
            }))
            .await
            .unwrap()
            .into_inner()
            .items;
        assert!(none.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ingestion_status_reports_windows_for_a_windowed_index() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());

        // A windowed index has no ordinal shards — its placement lives in the `windows` map. The
        // ingestion feed must report those windows (task-226), not the old "0 of 0 shards".
        svc.registry.create(resolved_windowed("wdocs")).unwrap();
        svc.registry
            .assign_window("wdocs", 86_400_000_000, "http://127.0.0.1:1")
            .unwrap();

        let items = svc
            .ingestion_status(Request::new(IngestionStatusRequest {
                index: String::new(),
            }))
            .await
            .unwrap()
            .into_inner()
            .items;
        let w = items.iter().find(|i| i.name == "wdocs").unwrap();
        // The window is reported as a shard row carrying its window id; shard_count = window count.
        assert_eq!(w.shard_count, 1);
        assert_eq!(w.shards.len(), 1);
        assert_eq!(w.shards[0].window, 86_400_000_000);
        assert_eq!(w.shards[0].ordinal, 0);
        assert_eq!(w.shards[0].node, "http://127.0.0.1:1");
        assert_eq!(w.shards[0].state, "unreachable"); // no live node behind the endpoint
    }

    #[tokio::test(flavor = "current_thread")]
    async fn create_rejects_bad_yaml_and_duplicates_before_connecting() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());

        // Unparseable definition → InvalidArgument (before any source connect).
        let err = svc
            .create_index(Request::new(CreateIndexRequest {
                definition_yaml: "name: docs\nmapping: [not valid".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        // A name already registered → AlreadyExists, rejected before the Iceberg round-trip.
        svc.registry.create(resolved("docs")).unwrap();
        let err = svc
            .create_index(Request::new(CreateIndexRequest {
                definition_yaml: "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::AlreadyExists);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alias_rpcs_set_list_and_drop() {
        use growlerdb_proto::v1::{DropAliasRequest, ListAliasesRequest, SetAliasRequest};
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        svc.registry.create(resolved("events_v1")).unwrap();

        // Set an alias → list reflects it.
        svc.set_alias(Request::new(SetAliasRequest {
            alias: "events".into(),
            targets: vec!["events_v1".into()],
        }))
        .await
        .unwrap();
        let aliases = svc
            .list_aliases(Request::new(ListAliasesRequest {}))
            .await
            .unwrap()
            .into_inner()
            .aliases;
        assert_eq!(aliases.len(), 1);
        assert_eq!(aliases[0].alias, "events");
        assert_eq!(aliases[0].targets, vec!["events_v1".to_string()]);

        // Empty alias name → InvalidArgument; a missing target → InvalidArgument (name clash maps
        // similarly; unknown target maps via registry NotFound).
        let err = svc
            .set_alias(Request::new(SetAliasRequest {
                alias: String::new(),
                targets: vec![],
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        // Drop the alias → list is empty; dropping a missing alias → NotFound.
        svc.drop_alias(Request::new(DropAliasRequest {
            alias: "events".into(),
        }))
        .await
        .unwrap();
        assert!(svc
            .list_aliases(Request::new(ListAliasesRequest {}))
            .await
            .unwrap()
            .into_inner()
            .aliases
            .is_empty());
        let err = svc
            .drop_alias(Request::new(DropAliasRequest {
                alias: "nope".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);
    }
}
