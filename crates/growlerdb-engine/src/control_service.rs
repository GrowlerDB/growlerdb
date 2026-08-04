//! The **Control Plane** gRPC service — cluster-wide index lifecycle over the
//! [`Registry`](growlerdb_controlplane::Registry). `CreateIndex` resolves the definition against
//! its source schema before registering it (status `building`); every RPC gates on the auth hook first.

use std::sync::Arc;

use std::collections::BTreeMap;

use growlerdb_controlplane::{
    ApiToken, IndexEntry, JobKind, JobState, Registry, RegistryError, ReindexJob, SavedQuery,
    ShardAssignment, ShardPhase,
};
use growlerdb_core::{BucketMap, IndexDefinition, Reassignment, ResolvedIndex, Source};
use growlerdb_proto::v1::admin_client::AdminClient;
use growlerdb_proto::v1::{
    ActivityEvent as WireActivity, AliasEntry, AlterControlRequest, AlterControlResponse,
    ApiTokenMeta, ApplyReshardRequest, ApplyReshardResponse, BucketMove, CancelReindexJobRequest,
    CancelReindexRequest, CreateIndexRequest, CreateIndexResponse, CreateTokenRequest,
    CreateTokenResponse, DeleteSavedQueryRequest, DeleteSavedQueryResponse, DescribeSourceRequest,
    DescribeSourceResponse, DropAliasRequest, DropAliasResponse, DropIndexRequest,
    DropIndexResponse, Error as WireError, FieldMapping, GetCheckpointRequest, GetIndexRequest,
    GetIndexResponse, GetLicenseRequest, GetLicenseResponse, GetReindexJobRequest, IndexIngestion,
    IndexSummary as WireSummary, IngestionStatusRequest, IngestionStatusResponse,
    ListActivityRequest, ListActivityResponse, ListAliasesRequest, ListAliasesResponse,
    ListIndexesRequest, ListIndexesResponse, ListReindexJobsRequest, ListReindexJobsResponse,
    ListRolesRequest, ListRolesResponse, ListSavedQueriesRequest, ListSavedQueriesResponse,
    ListTokensRequest, ListTokensResponse, ListUsersRequest, ListUsersResponse, LoginRequest,
    LoginResponse, MoveBucketRequest, MoveBucketResponse, NodeAssignments, PlanReshardRequest,
    PlanReshardResponse, RegisterNodeRequest, RegisterNodeResponse, RegisterServedIndexRequest,
    RegisterServedIndexResponse, ReindexControlRequest, ReindexControlResponse,
    ReindexIndexRequest, ReindexIndexResponse, ReindexJobShard, ReindexJobStatus, ReindexPhase,
    ReindexPrecheckRequest, ReindexStatusRequest, ResolveUnitOwnerRequest,
    ResolveUnitOwnerResponse, RevokeTokenRequest, RevokeTokenResponse, RoleBinding,
    RoutingStrategy as WireRouting, SaveSavedQueryRequest, SaveSavedQueryResponse,
    SavedQuery as WireSavedQuery, SetAliasRequest, SetAliasResponse, SetUserRolesRequest,
    SetUserRolesResponse, ShardIngestion, ShardStatus, SourceFieldInfo, StartJobResponse,
    StartReindexJobRequest, SubscribeAssignmentsRequest, UnitAssignment, WindowingConfig,
};
use growlerdb_proto::{to_status, ControlPlane, ControlPlaneServer, WriteClient};
use growlerdb_source::{IcebergConfig, IcebergReader};
use tonic::{Code, Request, Response, Status};

use crate::auth::{self, default_auth, AuthContext, SharedAuth};
use crate::authn::SharedAuthn;

/// Consecutive failures before an account is locked out.
const LOGIN_FAILURES_BEFORE_LOCKOUT: u32 = 5;
/// Base lockout window; doubles per failure past the threshold, capped at [`LOGIN_LOCKOUT_MAX_SECS`].
const LOGIN_LOCKOUT_BASE_SECS: u64 = 1;
const LOGIN_LOCKOUT_MAX_SECS: u64 = 300;
/// Max concurrent Argon2 verifications — bounds the CPU an unauthenticated `/v1/login` flood can burn.
const MAX_CONCURRENT_LOGINS: usize = 8;
/// Cap on tracked accounts, so a username-spray can't grow the throttle map unbounded.
const MAX_TRACKED_ACCOUNTS: usize = 10_000;

/// Per-account login throttle: tracks consecutive failures and locks an account for an
/// exponentially-growing window, plus a global concurrency permit. A locked account skips the
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

/// Per-node assignment-push subscribers (D53): `node endpoint → watch sender` holding each node's
/// latest snapshot. `watch` coalesces to the latest value, so a slow node never wedges the CP and
/// converges on the current set (snapshots are idempotent). `Arc<Mutex<…>>` so every clone of the
/// [`ControlPlaneService`] shares one subscriber set.
#[derive(Clone, Default)]
struct AssignmentHub {
    senders: Arc<
        std::sync::Mutex<
            std::collections::HashMap<String, tokio::sync::watch::Sender<NodeAssignments>>,
        >,
    >,
}

impl AssignmentHub {
    /// Subscribe `endpoint`, seeding the stream with its current snapshot. Register-then-compute-then-send
    /// (HA-D4a): the receiver is created before the snapshot is computed and the hub lock held across
    /// compute+send (here and in [`notify_all`](Self::notify_all)), so no placement change lands in a gap
    /// and gets clobbered by a stale seed. The registry read inside the lock never re-takes the hub lock,
    /// so the lock order (hub → registry) is acyclic.
    fn subscribe(
        &self,
        endpoint: &str,
        registry: &Registry,
    ) -> tokio::sync::watch::Receiver<NodeAssignments> {
        let mut map = self.senders.lock().unwrap_or_else(|e| e.into_inner());
        let tx = map
            .entry(endpoint.to_string())
            .or_insert_with(|| tokio::sync::watch::channel(NodeAssignments::default()).0);
        let rx = tx.subscribe();
        let snap = node_assignments_wire(registry.node_assignments(endpoint));
        let _ = tx.send(snap);
        rx
    }

    /// Push a fresh snapshot to every subscribed node — invoked by the registry's placement-change
    /// hook after any persisted mutation, so every change path pushes without remembering to. Senders
    /// whose receivers have all dropped are evicted (HA-D4b): a disconnected node re-subscribes for a
    /// fresh full snapshot, so nothing is lost and the hub can't grow with dead endpoints.
    fn notify_all(&self, registry: &Registry) {
        let mut map = self.senders.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|ep, tx| {
            if tx.receiver_count() == 0 {
                return false; // every stream for this endpoint dropped — evict
            }
            let _ = tx.send(node_assignments_wire(registry.node_assignments(ep)));
            true
        });
    }
}

/// Convert the registry's `(index, unit, is_primary)` rows into the wire [`NodeAssignments`] snapshot.
fn node_assignments_wire(
    units: Vec<(String, growlerdb_controlplane::Unit, bool)>,
) -> NodeAssignments {
    use growlerdb_controlplane::Unit;
    use growlerdb_proto::v1::unit_assignment::Unit as WireUnit;
    NodeAssignments {
        units: units
            .into_iter()
            .map(|(index, unit, primary)| UnitAssignment {
                index,
                primary,
                unit: Some(match unit {
                    Unit::Shard(o) => WireUnit::Shard(o),
                    Unit::Window(w) => WireUnit::Window(w),
                }),
            })
            .collect(),
    }
}

/// A `ControlPlane` service over a shared [`Registry`]. `CreateIndex` resolves against the
/// index's Iceberg source (`iceberg`); drop/list are pure registry operations.
#[derive(Clone)]
pub struct ControlPlaneService {
    registry: Arc<Registry>,
    iceberg: IcebergConfig,
    auth: SharedAuth,
    /// Optional authenticator: when set, the control plane validates the forwarded bearer itself
    /// (not trusting gateway-stamped metadata) before authorizing — so local role bindings merge
    /// against a *verified* subject. `None` trusts the gateway-stamped principal/roles.
    authn: Option<SharedAuthn>,
    /// Built-in login signing key: `Some` enables the `Login` RPC, which verifies a password against
    /// the registry credential store and mints an HS256 session JWT signed with this secret (the
    /// gateway validates it with the same secret). `None` ⇒ login is `UNIMPLEMENTED`.
    session_secret: Option<Vec<u8>>,
    /// Shared login throttle — `Arc` so the per-connection clones share lockout state.
    login_throttle: Arc<LoginThrottle>,
    /// Optional scale-limit [license](crate::license). `None` ⇒ the free tier
    /// ([`FREE_NODE_LIMIT`](crate::license::FREE_NODE_LIMIT)); a valid license raises the node cap.
    license: Option<crate::license::License>,
    /// Cluster-wide **replication factor** R (D53): holders the CP places per unit — 1 primary +
    /// R−1 read replicas. `1` (the default) is primary-only; `> 1` engages
    /// [`resolve_unit_holders`](Registry::resolve_unit_holders) so a resolve also places replicas.
    replication_factor: usize,
    /// D53 assignment-push subscribers: nodes subscribe (`SubscribeAssignments`) and a placement
    /// change pushes each its current holder set, so a placed replica starts serving.
    assignments: AssignmentHub,
}

impl ControlPlaneService {
    /// A Control-Plane service over `registry`, resolving new indexes against `iceberg`,
    /// with the default no-op auth hook.
    pub fn new(registry: Arc<Registry>, iceberg: IcebergConfig) -> Self {
        Self::with_auth(registry, iceberg, default_auth())
    }

    /// As [`new`](Self::new), with a specific [auth hook](SharedAuth).
    pub fn with_auth(registry: Arc<Registry>, iceberg: IcebergConfig, auth: SharedAuth) -> Self {
        let assignments = AssignmentHub::default();
        // Placement-change hook (HA-D1): wired at the registry's persist boundary so every persisted
        // placement mutation pushes each subscribed node its fresh snapshot, and no new mutation can
        // forget to notify. `Weak` breaks the registry → listener → registry cycle.
        {
            let hub = assignments.clone();
            let weak = Arc::downgrade(&registry);
            registry.set_placement_listener(move || {
                if let Some(registry) = weak.upgrade() {
                    hub.notify_all(&registry);
                }
            });
        }
        Self {
            registry,
            iceberg,
            auth,
            authn: None,
            session_secret: None,
            login_throttle: Arc::new(LoginThrottle::new()),
            license: None,
            replication_factor: 1,
            assignments,
        }
    }

    /// Install a verified scale-limit [license](crate::license), raising the node cap above the free
    /// tier. `None` keeps the free tier.
    pub fn with_license(mut self, license: Option<crate::license::License>) -> Self {
        self.license = license;
        self
    }

    /// Set the cluster-wide **replication factor** R (D53): the CP then places 1 primary + R−1 read
    /// replicas per unit on `ResolveUnitOwner`. Clamped to at least 1 (1 = primary-only, the default).
    pub fn with_replication_factor(mut self, r: usize) -> Self {
        self.replication_factor = r.max(1);
        self
    }

    /// The entitled cap in **distinct primary-holding nodes** (D38/D53, Option A): the license's
    /// `max_nodes` claim (replicas are free), or [`FREE_NODE_LIMIT`](crate::license::FREE_NODE_LIMIT).
    fn entitled_nodes(&self) -> usize {
        self.license
            .as_ref()
            .map(|l| l.max_nodes as usize)
            .unwrap_or(crate::license::FREE_NODE_LIMIT)
    }

    /// Install an [authenticator](crate::authn) so the control plane validates the bearer itself —
    /// required for role-binding enforcement that doesn't trust forwarded identity.
    pub fn with_authn(mut self, authn: SharedAuthn) -> Self {
        self.authn = Some(authn);
        self
    }

    /// Enable built-in credential login: the `Login` RPC verifies a password against the registry
    /// credential store and mints a session JWT signed with `secret`. Without this, `Login` returns
    /// `UNIMPLEMENTED`.
    pub fn with_session_secret(mut self, secret: Vec<u8>) -> Self {
        self.session_secret = Some(secret);
        self
    }

    /// Wrap as a mountable tonic [`ControlPlaneServer`].
    ///
    /// # Panics
    /// Fails closed at wiring time if an authorizing policy is installed without an authenticator:
    /// the control plane would then enforce roles against caller-asserted (forgeable) metadata, so a
    /// forged admin role would escalate. A misconfiguration to catch before serving, not at runtime.
    pub fn into_server(self) -> ControlPlaneServer<Self> {
        assert!(
            !(self.authn.is_none() && self.auth.is_authorizing()),
            "control plane installed an authorizing policy without an authenticator: \
             identity would be caller-asserted and forgeable — install an authenticator",
        );
        ControlPlaneServer::new(self)
    }

    /// Authorize `method` for the caller of `request`. Resolves the caller's identity — validating
    /// the bearer when an [authenticator](Self::with_authn) is set, else trusting the gateway-stamped
    /// principal/roles — then **merges the subject's local role bindings** before the policy check.
    /// So an admin granting a role takes effect on that subject's next call.
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
                // Session revocation: reject a token minted before the subject's session epoch (set
                // when roles change or a credential is removed), forcing re-auth with current roles.
                // Compared at second granularity (`iat` floored seconds, epoch ms): floor is monotonic,
                // so a token at-or-after the epoch is never wrongly rejected — at worst one from the
                // same second survives <1s.
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
        // Merge admin-managed local role bindings, keyed by the verified subject.
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
            // Control-plane ops authorize by role/scope, not a resolved target index: the per-index
            // allowlist is enforced on the Gateway read/write path. Index-agnostic here.
            index: None,
            allowed_indexes: Vec::new(),
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

/// The verified subject that owns saved queries: the [gated](Self::gate) principal, or `""`
/// (anonymous) on an open gateway — in which case the console keeps queries in localStorage
/// instead. Taken from the authorized context, never a re-read of caller-asserted metadata.
fn subject_of(ctx: &AuthContext) -> String {
    ctx.principal.clone().unwrap_or_default()
}

/// Build per-field [`FieldMapping`]s for the console's Mapping tab from the resolved definition:
/// type / analyzer / fast / cached, key role, and the reason a field can't be cached.
fn field_mappings(def: &ResolvedIndex) -> Vec<FieldMapping> {
    use growlerdb_core::FieldType::{Bool, Date, Double, Ip, Keyword, Long, Text, Variant, Vector};
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
                Vector => "VECTOR",
                Variant => "VARIANT",
            };
            // A field that can't be cached — sensitive (never) or big text (over the cap).
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
                // DATE fields carry their source unit so a live-CP gateway's `_search` adapter can
                // convert a bound written in that unit to canonical micros (every temporal field).
                field_format: f
                    .format
                    .map(time_format_str)
                    .unwrap_or_default()
                    .to_string(),
            }
        })
        .collect()
}

/// Per-shard placement + coarse state for the console's Shards tab: the control-plane shard map
/// (primary + replicas per ordinal, or per window for a windowed index). A shard with an assigned
/// primary is `active`; one still awaiting assignment is `building`.
fn shard_statuses(entry: &IndexEntry) -> Vec<ShardStatus> {
    // `bounds` is the window's event-time zone-map — carried so the live-CP gateway can prune;
    // `None`/absent for an ordinal shard.
    let from =
        |ordinal: u32, window: i64, a: &ShardAssignment, bounds: Option<(i64, i64)>, cold: bool| {
            ShardStatus {
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
                cold,
            }
        };
    if entry.windows.is_empty() {
        entry
            .shards
            .iter()
            .map(|(ord, a)| from(*ord, 0, a, None, false))
            .collect()
    } else {
        // Windowed index: one cell per time window (oldest first), ordinal is its position; carry the
        // per-window event-time zone-map (pruning) + the live cold/hot tier (for /v1/cold).
        entry
            .windows
            .iter()
            .enumerate()
            .map(|(i, (w, wa))| {
                from(
                    i as u32,
                    *w,
                    &wa.assignment,
                    wa.event_min.zip(wa.event_max),
                    wa.cold,
                )
            })
            .collect()
    }
}

/// The windowing config carried on `GetIndexResponse`: `Some` iff the index is windowed, so a
/// live-CP gateway can build a window router + prune without reading the registry file. Mirrors
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
        // micros exactly as the engine does. "" = a native DATE already in micros.
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

/// A [`TimeFormat`](growlerdb_core::TimeFormat) as its serde snake_case wire name — what the
/// connector maps back to a normalization when computing a row's window id.
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

/// API-token → wire metadata: never includes the hash or secret.
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
        RegistryError::InvalidDefinition(detail) => to_status(
            Code::InvalidArgument,
            WireError::new("INVALID_ARGUMENT", detail),
        ),
        RegistryError::PlacementConflict(detail) => to_status(
            Code::FailedPrecondition,
            WireError::new(
                "PLACEMENT_CONFLICT",
                format!("placement conflict: {detail}"),
            ),
        ),
        // Scale entitlement (D38/D53): a NEW placement past the cap. RESOURCE_EXHAUSTED so callers
        // distinguish "buy/raise the limit" from a transient failure.
        RegistryError::EntitlementExceeded { nodes, entitled } => to_status(
            Code::ResourceExhausted,
            WireError::new(
                "RESOURCE_EXHAUSTED",
                format!(
                    "scale limit reached: {nodes} primary-serving nodes in use, entitlement is \
                     {entitled} (free tier is {}). Read replicas and additional indexes co-located \
                     on already-counted nodes are free; an Enterprise license raises the limit — \
                     see COMM-LICENSE.md.",
                    crate::license::FREE_NODE_LIMIT,
                ),
            ),
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
        RegistryError::JobNotFound(id) => to_status(
            Code::NotFound,
            WireError::new("NOT_FOUND", format!("job `{id}` not found")),
        ),
        RegistryError::AliasNameClash(name) => to_status(
            Code::InvalidArgument,
            WireError::new(
                "INVALID_ARGUMENT",
                format!("alias `{name}` clashes with an existing index name"),
            ),
        ),
        // A write reached a standby (or a deposed leader): the store is healthy, this replica just
        // may not write — FAILED_PRECONDITION, not Internal, so the caller re-resolves the leader
        // (in k8s the Service already routes to it) and retries.
        RegistryError::NotLeader(detail) => to_status(
            Code::FailedPrecondition,
            WireError::new("NOT_LEADER", format!("not the registry leader: {detail}")),
        ),
        other => to_status(
            Code::Internal,
            WireError::new("INTERNAL", other.to_string()),
        ),
    }
}

/// Validate a node-supplied `endpoint` (HA-D6): non-empty and shaped like `[http[s]://]host:port`
/// with a numeric port. The pool, the shard map, and the assignment hub key on this string and the
/// gateway dials it, so a malformed value (e.g. a field decoded from an incompatible old binary)
/// must fail loudly at the RPC boundary instead of seeding placement with a garbage node.
fn validate_endpoint(endpoint: &str) -> Result<(), Status> {
    let bad = |why: &str| {
        Status::invalid_argument(format!(
            "invalid endpoint `{endpoint}`: {why} (expected [http[s]://]host:port)"
        ))
    };
    if endpoint.is_empty() {
        return Err(Status::invalid_argument("endpoint is required"));
    }
    if endpoint.chars().any(char::is_whitespace) {
        return Err(bad("contains whitespace"));
    }
    let rest = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    if rest.contains("://") || rest.contains('/') {
        return Err(bad("unsupported scheme or path"));
    }
    let (host, port) = rest
        .rsplit_once(':')
        .ok_or_else(|| bad("missing `:port`"))?;
    if host.is_empty() {
        return Err(bad("empty host"));
    }
    if port.parse::<u16>().is_err() {
        return Err(bad("port is not a number in 0-65535"));
    }
    Ok(())
}

/// The control plane's wall clock in epoch ms — the authority for windowed-node heartbeat liveness,
/// so a node's own (possibly skewed) clock never decides whether it's in the pool.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A planned **growth** reshard: the new bucket map plus which shards to (re)build from source
/// before the cutover and which to trim after it. `(ordinal, endpoint)` pairs name the node serving
/// each shard.
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
/// their ordinal). Pure over the plan + shard map, so it's unit-tested without a cluster.
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

/// Drive a **filtered reindex** on the node serving one shard: connect its Admin gRPC and rebuild
/// the shard from source keeping only the buckets it owns under `owners`. The per-node data step of
/// a reshard, reusing the write-fenced reindex.
#[allow(clippy::too_many_arguments)]
async fn reindex_shard_on_node(
    endpoint: &str,
    index: &str,
    owners: &[u32],
    ordinal: u32,
    window: i64,
    phase: ReindexPhase,
    definition_json: &str,
) -> Result<ReindexIndexResponse, Status> {
    // Mesh dial: stamp the shared service token (env) — the node's data plane enforces it.
    let (channel, stamp) = growlerdb_proto::service_token::node_channel(endpoint.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("connecting to node `{endpoint}`: {e}")))?;
    let mut client = AdminClient::with_interceptor(channel, stamp);
    let resp = client
        .reindex_index(ReindexIndexRequest {
            index: index.to_string(),
            bucket_owners: owners.to_vec(),
            shard_ordinal: ordinal,
            phase: phase as i32,
            definition_json: definition_json.to_string(),
            window,
        })
        .await?
        .into_inner();
    Ok(resp)
}

/// How often the coordinated driver polls a building shard's node for live doc-level progress.
const REINDEX_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(500);

/// Poll one node's live reindex build progress (docs done/total) over its Admin gRPC.
async fn reindex_status_on_node(
    endpoint: &str,
    index: &str,
    window: i64,
) -> Result<growlerdb_proto::v1::ReindexStatusResponse, Status> {
    let (channel, stamp) = growlerdb_proto::service_token::node_channel(endpoint.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("connecting to node `{endpoint}`: {e}")))?;
    let mut client = AdminClient::with_interceptor(channel, stamp);
    Ok(client
        .reindex_status(ReindexStatusRequest {
            index: index.to_string(),
            window,
        })
        .await?
        .into_inner())
}

/// Trip one node's reindex cancel flag over its Admin gRPC — the in-flight build aborts.
async fn cancel_reindex_on_node(endpoint: &str, index: &str, window: i64) -> Result<(), Status> {
    let (channel, stamp) = growlerdb_proto::service_token::node_channel(endpoint.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("connecting to node `{endpoint}`: {e}")))?;
    let mut client = AdminClient::with_interceptor(channel, stamp);
    client
        .cancel_reindex(CancelReindexRequest {
            index: index.to_string(),
            window,
        })
        .await?;
    Ok(())
}

/// Ask one node whether it has the free disk to reindex its shard/window over its Admin gRPC.
async fn reindex_precheck_on_node(
    endpoint: &str,
    index: &str,
    window: i64,
) -> Result<growlerdb_proto::v1::ReindexPrecheckResponse, Status> {
    let (channel, stamp) = growlerdb_proto::service_token::node_channel(endpoint.to_string())
        .await
        .map_err(|e| Status::unavailable(format!("connecting to node `{endpoint}`: {e}")))?;
    let mut client = AdminClient::with_interceptor(channel, stamp);
    Ok(client
        .reindex_precheck(ReindexPrecheckRequest {
            index: index.to_string(),
            window,
        })
        .await?
        .into_inner())
}

/// **Pre-run free-disk check** across every unit a reindex will build: ask each unit's node whether
/// it has room (≈headroom × the current shard size), and refuse the whole reindex with ONE clear,
/// up-front error naming the short nodes — instead of a job that fails hours into a multi-GB rebuild
/// on a single shard. Called before the job is created. A node unreachable for the probe is treated as
/// OK (don't block a reindex on a transient precheck hiccup; its BUILD does the authoritative check).
async fn precheck_reindex_disk(index: &str, units: &[(u32, i64, String)]) -> Result<(), Status> {
    let unit_label = |ordinal: u32, window: i64| {
        if window != 0 {
            format!("window {window}")
        } else {
            format!("shard {ordinal}")
        }
    };
    let mut short: Vec<String> = Vec::new();
    for (ordinal, window, endpoint) in units {
        match reindex_precheck_on_node(endpoint, index, *window).await {
            Ok(r) if r.probed && !r.ok => short.push(format!(
                "{} on {endpoint}: need ~{} bytes, only {} free",
                unit_label(*ordinal, *window),
                r.needed_bytes,
                r.free_bytes
            )),
            Ok(_) => {}
            Err(e) => {
                // A probe hiccup shouldn't block the reindex — the node's BUILD re-checks disk anyway.
                tracing::warn!(index = %index, endpoint = %endpoint, error = %e,
                    "reindex disk precheck probe failed — proceeding (the node's build re-checks)");
            }
        }
    }
    if !short.is_empty() {
        return Err(Status::failed_precondition(format!(
            "insufficient free disk to reindex `{index}` on {} node(s): {}",
            short.len(),
            short.join("; ")
        )));
    }
    Ok(())
}

/// Validate that `index` can be coordinated-reindexed and return one unit per shard/window as
/// `(ordinal, window, primary endpoint)` — the driver's build plan. Fails **before** any job is
/// created so a bad request never leaves a stray job behind.
///
/// A **windowed** index enumerates its `window_map` (one unit per HOT window; `ordinal = 0`, `window`
/// = the window id); **cold/parked** windows are skipped (they have no local writer — reindexing them
/// needs a revive→build→re-park cycle, a follow-up). An **ordinal** index enumerates its `shard_map`
/// (one unit per shard; `window = 0`). Either way each unit needs a primary to drive.
fn plan_reindex_shards(
    registry: &Registry,
    index: &str,
) -> Result<Vec<(u32, i64, String)>, Status> {
    let entry = registry
        .get(index)
        .ok_or_else(|| registry_status(RegistryError::NotFound(index.to_string())))?;
    let primary_endpoint = |assignment: &ShardAssignment, unit: &str| {
        assignment
            .primary
            .as_ref()
            .map(|n| n.0.clone())
            .filter(|e| !e.is_empty())
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "{unit} of `{index}` has no primary; cannot reindex"
                ))
            })
    };
    if windowing_config(&entry.definition).is_some() {
        let window_map = registry
            .window_map(index)
            .ok_or_else(|| registry_status(RegistryError::NotFound(index.to_string())))?;
        let mut units: Vec<(u32, i64, String)> = Vec::new();
        let mut cold_skipped = 0usize;
        for (window, wa) in &window_map {
            if wa.cold {
                // A parked window is read-through with no local writer; skip it (see the doc above).
                cold_skipped += 1;
                continue;
            }
            let endpoint = primary_endpoint(&wa.assignment, &format!("window {window}"))?;
            units.push((0, *window, endpoint));
        }
        if units.is_empty() {
            return Err(Status::failed_precondition(format!(
                "index `{index}` has no hot windows to reindex ({cold_skipped} cold windows skipped; \
                 cold-window reindex is a follow-up)"
            )));
        }
        if cold_skipped > 0 {
            tracing::info!(index = %index, cold_skipped,
                "windowed reindex: skipping cold (parked) windows — reindexing hot windows only");
        }
        return Ok(units);
    }
    let shard_map = registry
        .shard_map(index)
        .ok_or_else(|| registry_status(RegistryError::NotFound(index.to_string())))?;
    if shard_map.is_empty() {
        return Err(Status::failed_precondition(format!(
            "index `{index}` has no assigned shards to reindex"
        )));
    }
    let mut shards: Vec<(u32, i64, String)> = Vec::with_capacity(shard_map.len());
    for (ord, assignment) in &shard_map {
        let endpoint = primary_endpoint(assignment, &format!("shard {ord}"))?;
        shards.push((*ord, 0, endpoint));
    }
    Ok(shards)
}

/// Whether the job still wants to proceed — `false` once a cancel is requested (or the job vanished,
/// so there is nothing left to drive).
fn job_wants_to_continue(registry: &Registry, job_id: &str) -> bool {
    registry
        .get_job(job_id)
        .map(|j| !j.cancel_requested)
        .unwrap_or(false)
}

/// Set one unit's phase in a job (no-op if the `(ordinal, window)` isn't present). A windowed job's
/// rows all have `ordinal = 0`, so the window disambiguates them; an ordinal job's have `window = 0`.
fn set_shard_phase(job: &mut ReindexJob, ordinal: u32, window: i64, phase: ShardPhase) {
    if let Some(s) = job
        .shards
        .iter_mut()
        .find(|s| s.ordinal == ordinal && s.window == window)
    {
        s.phase = phase;
    }
}

/// The outcome of a single shard's build, distinguishing a deliberate cancel from a real failure so
/// the driver can end the job Canceled vs Failed.
enum BuildOutcome {
    Ok,
    Canceled,
    Failed(Status),
}

/// Run one shard's BUILD while polling its node for live progress (folded into the job) and honoring
/// a mid-build cancel. Returns once the build resolves.
#[allow(clippy::too_many_arguments)]
async fn build_shard_with_progress(
    registry: &Arc<Registry>,
    job_id: &str,
    index: &str,
    owners: &[u32],
    ordinal: u32,
    window: i64,
    endpoint: &str,
    def_json: &str,
) -> BuildOutcome {
    let build = reindex_shard_on_node(
        endpoint,
        index,
        owners,
        ordinal,
        window,
        ReindexPhase::Build,
        def_json,
    );
    tokio::pin!(build);
    loop {
        tokio::select! {
            res = &mut build => {
                return match res {
                    Ok(_) => BuildOutcome::Ok,
                    Err(status) if status.code() == Code::Cancelled => BuildOutcome::Canceled,
                    // A build that failed while a cancel was in flight is a cancel, not a fault.
                    Err(status) => {
                        if !job_wants_to_continue(registry, job_id) {
                            BuildOutcome::Canceled
                        } else {
                            BuildOutcome::Failed(status)
                        }
                    }
                };
            }
            _ = tokio::time::sleep(REINDEX_POLL_INTERVAL) => {
                if let Ok(st) = reindex_status_on_node(endpoint, index, window).await {
                    registry.mutate_job(job_id, |j| {
                        if let Some(s) = j.shards.iter_mut().find(|s| s.ordinal == ordinal && s.window == window) {
                            s.docs_done = st.docs_done;
                            s.docs_total = st.docs_total;
                        }
                    });
                }
                // A cancel requested mid-build: ping the node so the in-flight build aborts promptly
                // (the CancelReindexJob handler also pings, but the driver re-pings in case of a race).
                if !job_wants_to_continue(registry, job_id) {
                    let _ = cancel_reindex_on_node(endpoint, index, window).await;
                }
            }
        }
    }
}

/// DISCARD every already-built shard's staged generation (releasing its fence), marking each row
/// Discarded — the shared unwind for an aborted or canceled job. Best-effort: a discard failure is
/// logged, since the old generation is intact regardless (no shard was promoted).
async fn discard_built(
    registry: &Arc<Registry>,
    job_id: &str,
    index: &str,
    owners: &[u32],
    built: &[(u32, i64, String)],
) {
    for (ord, window, endpoint) in built {
        if let Err(e) = reindex_shard_on_node(
            endpoint,
            index,
            owners,
            *ord,
            *window,
            ReindexPhase::Discard,
            "",
        )
        .await
        {
            tracing::warn!(index = %index, shard = ord, window, error = %e,
                "reindex: discard of a staged generation failed — its write-fence may need a manual clear");
        }
        registry.mutate_job(job_id, |j| {
            set_shard_phase(j, *ord, *window, ShardPhase::Discarded)
        });
    }
}

/// Drive a created reindex/alter **job** to a terminal state: BUILD every shard's next generation
/// (folding live per-shard progress into the job), then PROMOTE all and bump the routing generation
/// (the atomic cutover) — updating the job as it advances. Any build failure DISCARDs every staged
/// generation and fails the job (never a half-swap); a cancel request DISCARDs + cancels. `def_json`
/// (serde of the new `ResolvedIndex`) rebuilds from a schema-changing alter's new definition; empty ⇒
/// a plain reindex against each shard's served definition. Runs inline (synchronous `ReindexIndex` /
/// `AlterIndex`) or spawned (`StartReindexJob`).
async fn drive_reindex_job(registry: Arc<Registry>, job_id: String, def_json: String) {
    let Some(job) = registry.get_job(&job_id) else {
        return;
    };
    let index = job.index.clone();
    // The generation we plan the cutover CAS from, and the CURRENT bucket owners (identity filter:
    // each shard keeps exactly its docs — no topology change).
    let Some(entry) = registry.get(&index) else {
        registry.mutate_job(&job_id, |j| {
            j.state = JobState::Failed;
            j.error = format!("index `{index}` vanished before the reindex started");
        });
        return;
    };
    let current_gen = entry.generation;
    // Windowed indexes have no routing-generation epoch — each window promotes as a node-local swap
    // and the gateway converges by placement fingerprint. So the cutover CAS is skipped for windowed.
    let windowed = windowing_config(&entry.definition).is_some();
    let owners = registry
        .bucket_map(&index)
        .map(|m| m.owners().to_vec())
        .unwrap_or_default();
    // Each unit: (ordinal, window, endpoint). Ordinal jobs have window = 0; windowed jobs ordinal = 0.
    let units: Vec<(u32, i64, String)> = job
        .shards
        .iter()
        .map(|s| (s.ordinal, s.window, s.node.clone()))
        .collect();

    let fail = |err: String| {
        registry.mutate_job(&job_id, |j| {
            j.state = JobState::Failed;
            j.error = err;
        });
    };
    let cancel = |registry: &Arc<Registry>| {
        registry.mutate_job(&job_id, |j| {
            j.state = JobState::Canceled;
            j.error = "canceled by request".to_string();
        });
    };
    // How a unit reads in a log/error message ("shard 2" or "window 17…").
    let unit_label = |ordinal: u32, window: i64| {
        if windowed {
            format!("window {window}")
        } else {
            format!("shard {ordinal}")
        }
    };

    // Phase 1 — BUILD every unit's next generation (staged, NOT promoted).
    registry.mutate_job(&job_id, |j| j.state = JobState::Building);
    let mut built: Vec<(u32, i64, String)> = Vec::new();
    for (ordinal, window, endpoint) in &units {
        if !job_wants_to_continue(&registry, &job_id) {
            discard_built(&registry, &job_id, &index, &owners, &built).await;
            cancel(&registry);
            return;
        }
        registry.mutate_job(&job_id, |j| {
            set_shard_phase(j, *ordinal, *window, ShardPhase::Building)
        });
        match build_shard_with_progress(
            &registry, &job_id, &index, &owners, *ordinal, *window, endpoint, &def_json,
        )
        .await
        {
            BuildOutcome::Ok => {
                built.push((*ordinal, *window, endpoint.clone()));
                // Publish the final count for this unit and mark it built (awaiting cutover).
                registry.mutate_job(&job_id, |j| {
                    if let Some(s) = j
                        .shards
                        .iter_mut()
                        .find(|s| s.ordinal == *ordinal && s.window == *window)
                    {
                        s.phase = ShardPhase::Built;
                        if s.docs_total > 0 {
                            s.docs_done = s.docs_total;
                        }
                    }
                });
            }
            BuildOutcome::Canceled => {
                // The in-flight build aborted itself; discard it too, plus the already-built ones.
                let _ = reindex_shard_on_node(
                    endpoint,
                    &index,
                    &owners,
                    *ordinal,
                    *window,
                    ReindexPhase::Discard,
                    "",
                )
                .await;
                registry.mutate_job(&job_id, |j| {
                    set_shard_phase(j, *ordinal, *window, ShardPhase::Discarded)
                });
                discard_built(&registry, &job_id, &index, &owners, &built).await;
                cancel(&registry);
                return;
            }
            BuildOutcome::Failed(status) => {
                discard_built(&registry, &job_id, &index, &owners, &built).await;
                fail(format!(
                    "build of {} failed (no cutover; old generation intact): {status}",
                    unit_label(*ordinal, *window)
                ));
                return;
            }
        }
    }

    // A cancel between the last build and the cutover still aborts cleanly (nothing is promoted yet).
    if !job_wants_to_continue(&registry, &job_id) {
        discard_built(&registry, &job_id, &index, &owners, &built).await;
        cancel(&registry);
        return;
    }

    // Phase 2 — PROMOTE every unit (brief fence-drain + atomic swap).
    registry.mutate_job(&job_id, |j| j.state = JobState::CuttingOver);
    for (i, (ordinal, window, endpoint)) in units.iter().enumerate() {
        registry.mutate_job(&job_id, |j| {
            set_shard_phase(j, *ordinal, *window, ShardPhase::Promoting)
        });
        match reindex_shard_on_node(
            endpoint,
            &index,
            &owners,
            *ordinal,
            *window,
            ReindexPhase::Promote,
            &def_json,
        )
        .await
        {
            Ok(resp) => {
                // Record the unit's authoritative post-promote doc count (the definitive number a
                // finished job reports; the live BUILD poll was only an estimate).
                registry.mutate_job(&job_id, |j| {
                    if let Some(s) = j
                        .shards
                        .iter_mut()
                        .find(|s| s.ordinal == *ordinal && s.window == *window)
                    {
                        s.phase = ShardPhase::Promoted;
                        s.docs_done = resp.doc_count;
                        if s.docs_total < resp.doc_count {
                            s.docs_total = resp.doc_count;
                        }
                    }
                });
            }
            Err(e) => {
                // Rare promote-phase partial failure: discard the not-yet-promoted remainder so the op
                // is retryable; already-promoted units keep the new generation (queryable, mixed;
                // a retry converges). The common BUILD-phase failure never reaches here, so it never
                // half-swaps.
                for (o, w, ep) in &units[i..] {
                    let _ = reindex_shard_on_node(
                        ep,
                        &index,
                        &owners,
                        *o,
                        *w,
                        ReindexPhase::Discard,
                        "",
                    )
                    .await;
                }
                fail(format!(
                    "promote of {} failed after {i}/{} units; discarded the rest \
                     (index is queryable, mixed-generation; retry to converge): {e}",
                    unit_label(*ordinal, *window),
                    units.len()
                ));
                return;
            }
        }
    }

    // Phase 3 — cutover marker. Ordinal: bump the routing generation (CAS vs the planned generation,
    // so a concurrent reindex/reshard is a loud PLACEMENT_CONFLICT; gateways converge on poll).
    // Windowed: no generation epoch — each window promoted as a node-local swap already, so just
    // finalize the job.
    if windowed {
        registry.record_activity(
            &index,
            "reindex",
            format!("reindexed {} windows (job {job_id})", units.len()),
        );
        registry.mutate_job(&job_id, |j| j.state = JobState::Done);
        return;
    }
    match registry.set_generation(&index, current_gen, current_gen + 1) {
        Ok(generation) => {
            registry.record_activity(
                &index,
                "reindex",
                format!("reindexed {} shards (job {job_id})", units.len()),
            );
            registry.mutate_job(&job_id, |j| {
                j.state = JobState::Done;
                j.generation = generation;
            });
        }
        Err(e) => fail(format!("cutover failed at the generation CAS: {e}")),
    }
}

/// Run a coordinated reindex/alter **synchronously**: create the job, drive it inline to a terminal
/// state, then map that outcome to the legacy `ReindexControlResponse`. This is the synchronous
/// `ReindexIndex` / `AlterIndex` path over the same driver `StartReindexJob` spawns — one
/// orchestration implementation, two doors.
async fn run_coordinated_reindex(
    registry: &Arc<Registry>,
    index: &str,
    def_json: &str,
) -> Result<ReindexControlResponse, Status> {
    let shards = plan_reindex_shards(registry, index)?;
    // Up-front free-disk check across every unit — fail fast with one clear error, not mid-rebuild.
    precheck_reindex_disk(index, &shards).await?;
    let kind = if def_json.is_empty() {
        JobKind::Reindex
    } else {
        JobKind::Alter
    };
    let job = registry.create_job(kind, index, shards);
    drive_reindex_job(registry.clone(), job.id.clone(), def_json.to_string()).await;
    let done = registry
        .get_job(&job.id)
        .ok_or_else(|| Status::internal("reindex job vanished mid-run"))?;
    match done.state {
        JobState::Done => Ok(ReindexControlResponse {
            generation: done.generation,
            shards: done.shards.len() as u32,
            doc_count: done.docs_done(),
        }),
        JobState::Canceled => Err(Status::cancelled(done.error)),
        _ => Err(Status::internal(if done.error.is_empty() {
            format!("reindex of `{index}` failed")
        } else {
            done.error
        })),
    }
}

/// Map a registry [`ReindexJob`] to its wire status.
fn job_to_wire(job: &ReindexJob) -> ReindexJobStatus {
    ReindexJobStatus {
        id: job.id.clone(),
        index: job.index.clone(),
        kind: job_kind_str(job.kind).to_string(),
        state: job_state_str(job.state).to_string(),
        shards: job
            .shards
            .iter()
            .map(|s| ReindexJobShard {
                ordinal: s.ordinal,
                node: s.node.clone(),
                phase: shard_phase_str(s.phase).to_string(),
                docs_done: s.docs_done,
                docs_total: s.docs_total,
                window: s.window,
            })
            .collect(),
        docs_done: job.docs_done(),
        docs_total: job.docs_total(),
        generation: job.generation,
        cancel_requested: job.cancel_requested,
        error: job.error.clone(),
        created_ms: job.created_ms,
        updated_ms: job.updated_ms,
    }
}

fn job_kind_str(k: JobKind) -> &'static str {
    match k {
        JobKind::Reindex => "reindex",
        JobKind::Alter => "alter",
    }
}

fn job_state_str(s: JobState) -> &'static str {
    match s {
        JobState::Pending => "pending",
        JobState::Building => "building",
        JobState::CatchingUp => "catching_up",
        JobState::CuttingOver => "cutting_over",
        JobState::Done => "done",
        JobState::Failed => "failed",
        JobState::Canceled => "canceled",
    }
}

fn shard_phase_str(p: ShardPhase) -> &'static str {
    match p {
        ShardPhase::Pending => "pending",
        ShardPhase::Building => "building",
        ShardPhase::Built => "built",
        ShardPhase::Promoting => "promoting",
        ShardPhase::Promoted => "promoted",
        ShardPhase::Discarded => "discarded",
    }
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

        // Resolve against the source schema, then register. A variant table's introspection routes
        // through Trino (D49): released iceberg-rust can't parse a v3 variant schema, so a variant def
        // reads its columns from Trino `information_schema`; non-variant defs keep the native reader.
        let Source::Iceberg(src) = &def.source;
        let table = src.table.clone();
        let internal = |e: String| to_status(Code::Internal, WireError::new("INTERNAL", e));
        let source = if def.declares_variant() {
            growlerdb_source::shared_hydrator()
                .read_source_schema(&table)
                .await
                .map_err(|e| internal(e.to_string()))?
        } else {
            let reader = IcebergReader::connect(&self.iceberg)
                .await
                .map_err(|e| internal(e.to_string()))?;
            reader
                .read_source_schema(&table)
                .await
                .map_err(|e| internal(e.to_string()))?
        };
        let resolved = def
            .resolve(&source)
            .map_err(|e| Status::invalid_argument(format!("definition does not resolve: {e}")))?;
        // Surface non-fatal resolution warnings in the response *and* the log — e.g. the
        // `PREDICATE` location strategy's honest-scope note (hydration latency depends on
        // the source layout), or an equality-delete reconcile fallback.
        let warnings = resolved.warnings.clone();
        for w in &warnings {
            tracing::warn!(index = %name, "create index warning: {w}");
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
        // resolved from the definition (the same source the Gateway's router uses).
        let routing = match entry.definition.routing_strategy() {
            growlerdb_core::RoutingStrategy::Hash => WireRouting::RoutingHash,
            growlerdb_core::RoutingStrategy::Partition => WireRouting::RoutingPartition,
        };
        Ok(Response::new(GetIndexResponse {
            name,
            status: status_str(entry.status).to_string(),
            shard_count: entry.shards.len() as u32,
            routing: routing as i32,
            // Empty ⇒ legacy routing; present ⇒ writers/readers route through this map.
            bucket_owners: entry.bucket_owners.clone(),
            // Per-field mapping for the console's Mapping tab.
            fields: field_mappings(&entry.definition),
            // Per-shard placement for the Shards tab.
            shard_status: shard_statuses(&entry),
            // Windowing config for a windowed index — lets a live-CP gateway build a window router +
            // prune; `None` for an ordinal index.
            windowing: windowing_config(&entry.definition),
            // Routing generation (reindex cutover epoch) + definition version (in-place alter),
            // so a gateway/node converges on the current build + served definition via this poll.
            generation: entry.generation,
            definition_version: entry.definition_version,
            // The authoritative resolved definition, so a booting node loads it (in cluster mode)
            // instead of a stale local def — opening the on-disk index at the definition a durable
            // alter last committed. Serialization can't realistically fail (the registry round-trips
            // this same value as JSON); on the impossible error, fall back to an empty string
            // (the node then keeps its local def) rather than fail the whole GetIndex.
            definition_json: serde_json::to_string(&entry.definition).unwrap_or_default(),
        }))
    }

    async fn plan_reshard(
        &self,
        request: Request<PlanReshardRequest>,
    ) -> Result<Response<PlanReshardResponse>, Status> {
        self.gate("PlanReshard", &request)?;
        let req = request.into_inner();
        // Read-only: compute the bounded bucket→shard reassignment to reach the new count without
        // applying it. The move list is the migration work for the online cutover.
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
        // The count the data is **currently routed over** — the stored bucket map's shard count,
        // NEVER the registered-node count: registration already includes the new (empty) build
        // targets, so deriving current from it makes growth impossible (current == new → rejected).
        // Registration adopts a map on first announce, so a registered ordinal index always has one;
        // the fallback covers only a CP-created index no node announced (apply then fails on missing
        // endpoints below anyway).
        let current_map = self.registry.bucket_map(&req.index);
        let current_count = current_map
            .as_ref()
            .map(|m| m.shards())
            .unwrap_or(shard_map.len() as u32);
        let growth = plan_growth_reshard(&plan, &shard_map, current_count, req.new_shard_count)
            .map_err(Status::failed_precondition)?;
        let owners = growth.map.owners().to_vec();

        // 2. Build the new shards from source (filtered) BEFORE the cutover — the old shards are
        //    untouched and still complete, so reads via the current map never miss.
        for (ord, endpoint) in &growth.build {
            // Reshard uses the one-shot node reindex (fence + build + promote in one call); the
            // phased flow is for a coordinated whole-index reindex (ControlPlane::reindex_index).
            reindex_shard_on_node(
                endpoint,
                &req.index,
                &owners,
                *ord,
                0,
                ReindexPhase::Full,
                "",
            )
            .await?;
        }

        // 3. Cutover: commit the new bucket map atomically — compare-and-swap against the map
        //    this plan was derived from, so a concurrent placement op (another reshard, a bucket
        //    move) that committed during the minutes-long build turns this into a loud
        //    FAILED_PRECONDITION instead of silently reverting its ownership.
        self.registry
            .set_bucket_map(&req.index, current_map.as_ref(), &growth.map)
            .map_err(registry_status)?;

        // 4. Trim the old shards' now-dead buckets (best-effort — the index is already correct; this
        //    only reclaims space). Safe post-cutover: those buckets no longer route to old shards.
        let mut trimmed = Vec::new();
        for (ord, endpoint) in &growth.trim {
            match reindex_shard_on_node(
                endpoint,
                &req.index,
                &owners,
                *ord,
                0,
                ReindexPhase::Full,
                "",
            )
            .await
            {
                Ok(_) => trimmed.push(*ord),
                Err(e) => tracing::warn!(
                    index = %req.index,
                    shard = ord,
                    error = %e,
                    "apply-reshard: post-cutover trim of shard failed (non-fatal)"
                ),
            }
        }

        // Record the reshard on the index's activity log — a material lifecycle event,
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

    async fn reindex_index(
        &self,
        request: Request<ReindexControlRequest>,
    ) -> Result<Response<ReindexControlResponse>, Status> {
        self.gate("ReindexIndex", &request)?;
        let index = request.into_inner().index;
        // A plain reindex rebuilds against each shard's currently-served definition (no def passed).
        run_coordinated_reindex(&self.registry, &index, "")
            .await
            .map(Response::new)
    }

    async fn start_reindex_job(
        &self,
        request: Request<StartReindexJobRequest>,
    ) -> Result<Response<StartJobResponse>, Status> {
        self.gate("StartReindexJob", &request)?;
        let index = request.into_inner().index;
        // One coordinated reindex per index at a time: a second would race the first's build + cutover
        // CAS. Refuse loudly rather than let the loser fail deep in the driver.
        if self
            .registry
            .list_jobs()
            .iter()
            .any(|j| j.index == index && !j.state.is_terminal())
        {
            return Err(Status::failed_precondition(format!(
                "a reindex job is already running for `{index}`"
            )));
        }
        // Validate + resolve the shard plan up front so a bad request fails before a job exists.
        let shards = plan_reindex_shards(&self.registry, &index)?;
        // Up-front free-disk check across every unit — fail fast (202 never returned) with one clear
        // error naming the short nodes, rather than a job that dies mid-rebuild on one shard.
        precheck_reindex_disk(&index, &shards).await?;
        let job = self.registry.create_job(JobKind::Reindex, &index, shards);
        // Drive it in the background; the caller polls GetReindexJob.
        tokio::spawn(drive_reindex_job(
            self.registry.clone(),
            job.id.clone(),
            String::new(),
        ));
        Ok(Response::new(StartJobResponse { job_id: job.id }))
    }

    async fn get_reindex_job(
        &self,
        request: Request<GetReindexJobRequest>,
    ) -> Result<Response<ReindexJobStatus>, Status> {
        self.gate("GetReindexJob", &request)?;
        let id = request.into_inner().job_id;
        let job = self
            .registry
            .get_job(&id)
            .ok_or_else(|| registry_status(RegistryError::JobNotFound(id)))?;
        Ok(Response::new(job_to_wire(&job)))
    }

    async fn list_reindex_jobs(
        &self,
        request: Request<ListReindexJobsRequest>,
    ) -> Result<Response<ListReindexJobsResponse>, Status> {
        self.gate("ListReindexJobs", &request)?;
        let jobs = self.registry.list_jobs().iter().map(job_to_wire).collect();
        Ok(Response::new(ListReindexJobsResponse { jobs }))
    }

    async fn cancel_reindex_job(
        &self,
        request: Request<CancelReindexJobRequest>,
    ) -> Result<Response<ReindexJobStatus>, Status> {
        self.gate("CancelReindexJob", &request)?;
        let id = request.into_inner().job_id;
        // Flag the job; the driver observes it between phases and unwinds cleanly.
        let job = self
            .registry
            .request_job_cancel(&id)
            .map_err(registry_status)?;
        // Proactively interrupt any in-flight build so the cancel takes effect promptly rather than
        // waiting for the current shard's full rebuild.
        for s in &job.shards {
            if s.phase == ShardPhase::Building {
                let _ = cancel_reindex_on_node(&s.node, &job.index, s.window).await;
            }
        }
        Ok(Response::new(job_to_wire(&job)))
    }

    async fn alter_index(
        &self,
        request: Request<AlterControlRequest>,
    ) -> Result<Response<AlterControlResponse>, Status> {
        self.gate("AlterIndex", &request)?;
        let req = request.into_inner();

        // Resolve the candidate against the source schema (same path as create_index), then diff it
        // against the registry's current definition to build the alter plan.
        let entry = self
            .registry
            .get(&req.index)
            .ok_or_else(|| registry_status(RegistryError::NotFound(req.index.clone())))?;
        let candidate_def = IndexDefinition::from_yaml(&req.definition_yaml)
            .map_err(|e| Status::invalid_argument(format!("invalid definition: {e}")))?;
        let Source::Iceberg(src) = &candidate_def.source;
        let table = src.table.clone();
        let internal = |e: String| to_status(Code::Internal, WireError::new("INTERNAL", e));
        let source = if candidate_def.declares_variant() {
            growlerdb_source::shared_hydrator()
                .read_source_schema(&table)
                .await
                .map_err(|e| internal(e.to_string()))?
        } else {
            IcebergReader::connect(&self.iceberg)
                .await
                .map_err(|e| internal(e.to_string()))?
                .read_source_schema(&table)
                .await
                .map_err(|e| internal(e.to_string()))?
        };
        let candidate = candidate_def
            .resolve(&source)
            .map_err(|e| Status::invalid_argument(format!("definition does not resolve: {e}")))?;
        let plan = entry.definition.alter_to(&candidate);

        let mut resp = AlterControlResponse {
            is_noop: plan.is_noop(),
            requires_reindex: plan.requires_reindex(),
            reindex_reasons: plan.reindex_reasons.clone(),
            in_place_changes: plan.in_place.clone(),
            applied: false,
            reindex_triggered: false,
            generation: entry.generation,
        };
        // Dry-run (or a no-op): just return the plan.
        if !req.apply || plan.is_noop() {
            return Ok(Response::new(resp));
        }

        // Apply: update the registry definition durably (CAS on the definition version), so it
        // survives restart and every shard reindexes/serves against the same new definition.
        self.registry
            .set_definition(&req.index, entry.definition_version, candidate.clone())
            .map_err(registry_status)?;
        resp.applied = true;

        // A reindex-requiring change: run a coordinated reindex from the NEW definition, cutting
        // over atomically to the new-schema generation. In-place-only changes just take the durable
        // definition update (nodes reload it; some are restart-scoped, as on the single-shard path).
        if plan.requires_reindex() {
            let def_json = serde_json::to_string(&candidate)
                .map_err(|e| internal(format!("serialize definition: {e}")))?;
            let out = run_coordinated_reindex(&self.registry, &req.index, &def_json).await?;
            resp.reindex_triggered = true;
            resp.generation = out.generation;
        }
        self.registry.record_activity(
            &req.index,
            "index.altered",
            format!(
                "altered `{}` (reindex={})",
                req.index, resp.reindex_triggered
            ),
        );
        Ok(Response::new(resp))
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
        reindex_shard_on_node(
            &to_endpoint,
            &req.index,
            &owners,
            req.to_shard,
            0,
            ReindexPhase::Full,
            "",
        )
        .await?;
        // 2. Cutover: commit the relocated map — CAS against the map this move was planned from
        //    (see apply_reshard), so a reshard finishing mid-move can't be silently reverted.
        self.registry
            .set_bucket_map(&req.index, Some(&map), &new_map)
            .map_err(registry_status)?;
        // 3. Trim the source shard (best-effort) — it no longer owns the bucket.
        if let Err(e) = reindex_shard_on_node(
            &from_endpoint,
            &req.index,
            &owners,
            from_shard,
            0,
            ReindexPhase::Full,
            "",
        )
        .await
        {
            tracing::warn!(
                index = %req.index,
                shard = from_shard,
                error = %e,
                "move-bucket: post-cutover trim of shard failed (non-fatal)"
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
        // Record on each target so the alias swap shows in that index's activity.
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

    /// Built-in credential login: verify the password against the registry store and mint
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
        // Rate-limit online guessing: a locked account is rejected *before* the
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
        // Constant-ish failure: an unknown subject and a wrong password are indistinguishable.
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
        // Per-index scope: if the subject has an index binding, stamp it into the token's
        // `indexes` claim so per-index RBAC restricts them; empty = unrestricted.
        let indexes = self.registry.indexes_for(&req.username);
        let token = crate::authn::mint_session_jwt(
            secret,
            &req.username,
            &roles,
            &indexes,
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
        let ctx = self.gate("ListSavedQueries", &request)?;
        let owner = subject_of(&ctx);
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
        let ctx = self.gate("SaveSavedQuery", &request)?;
        let owner = subject_of(&ctx);
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
        let ctx = self.gate("DeleteSavedQuery", &request)?;
        let owner = subject_of(&ctx);
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
        // Prevent privilege escalation: a caller can only assign roles that are
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
        // Prevent privilege escalation: a token can't carry roles/scopes the
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
            expires_at_ms: None, // no expiry by default
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
        validate_endpoint(&req.endpoint)?;
        // Heartbeat the owner into the liveness pool: a classic `serve --index X` node only announces
        // via RegisterServedIndex (never RegisterNode), so without this the dead-owner sweeper would
        // see its owner as dead and steal its self-declared units onto a pool node. Records LIVENESS
        // ONLY — the endpoint isn't made placement-eligible, so the CP never assigns POOL units to it.
        self.registry.touch_node_liveness(&req.endpoint, now_ms());
        // The node ships its already-resolved definition (its `index.json`), so registration is a
        // pure registry op — no source round-trip (unlike CreateIndex, which resolves YAML).
        let resolved: ResolvedIndex = serde_json::from_str(&req.definition_json)
            .map_err(|e| Status::invalid_argument(format!("invalid definition_json: {e}")))?;
        let name = resolved.name.clone();
        let shard_count = req.shard_count.max(1);

        // Classify by the DEFINITION, not by whether `windows` is populated: a windowed
        // node that starts **empty** (streaming-first — it creates windows on first write) still
        // reports zero windows, and must register as a *windowed* entry so `ResolveUnitOwner` can
        // place windows on it — not be misclassified as an ordinal single-shard index.
        let is_windowed = resolved.windowing.is_some();
        // Upsert: create on first announce, idempotent on restart (a re-announce just re-points
        // the shard/window map at the — possibly new — endpoint below).
        if self.registry.get(&name).is_none() {
            // Idempotent under a race: two nodes serving the same index (D53 replication) can both
            // find it absent and try to create it — treat the loser's `AlreadyExists` as success.
            match self.registry.create(resolved) {
                Ok(_) | Err(RegistryError::AlreadyExists(_)) => {}
                Err(e) => return Err(registry_status(e)),
            }
        }
        if !is_windowed {
            // Ordinal shard map. An empty list means "claim all 0..count" for a classic single node,
            // but "claim none" for a **placement-pool** node (D52) — its ordinals are placed by
            // `ResolveUnitOwner`, so a replica-only node registers without grabbing every shard as
            // primary (which would conflict with every peer serving the same index).
            let owned: Vec<u32> = if req.pool_managed {
                req.shard_ordinals.clone()
            } else if req.shard_ordinals.is_empty() {
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
            // One persist for all this node's ordinals. The announce is guarded (HA-D3/D7): idempotent
            // for this endpoint's own shards, a serving report for shards it replicates, a takeover for
            // confidently-dead primaries — but a shard held by a live foreign primary is
            // PLACEMENT_CONFLICT (first-wins), and fresh primaries are entitlement-checked (fail-closed).
            self.registry
                .announce_primaries(
                    &name,
                    &owned,
                    &req.endpoint,
                    now_ms(),
                    self.entitled_nodes(),
                )
                .map_err(registry_status)?;
            // Every ordinal index is bucketed from its first announce (no long-lived legacy
            // routing): adopt a balanced map over the DECLARED total once. A later announce —
            // in particular a growth build target registering with `--shards N+k` mid-reshard —
            // finds the map present and leaves live routing untouched until the cutover.
            self.registry
                .adopt_bucket_map_if_absent(&name, shard_count)
                .map_err(registry_status)?;
        } else {
            // Windowed: place the served windows on this node and record their event-time zone-maps
            // + hot/cold tier in one guarded, batched mutation. Same first-wins semantics as the
            // ordinal path. `windows` may be empty (an empty streaming node) — the entry still exists
            // + activates below so placement can proceed.
            let announces: Vec<growlerdb_controlplane::WindowAnnounce> = req
                .windows
                .iter()
                .map(|w| growlerdb_controlplane::WindowAnnounce {
                    window: w.window,
                    bounds: w.has_event_bounds.then_some((w.event_min, w.event_max)),
                    cold: w.cold,
                })
                .collect();
            self.registry
                .announce_windows(
                    &name,
                    &req.endpoint,
                    &announces,
                    now_ms(),
                    self.entitled_nodes(),
                )
                .map_err(registry_status)?;
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
        // A malformed endpoint must fail LOUDLY here, not seed the pool with a garbage entry that
        // least-loaded placement would then prefer (HA-D6 — an old-binary heartbeat is the classic
        // source; the proto also `reserved` the repurposed field so it can't decode as an endpoint).
        validate_endpoint(&req.endpoint)?;
        // A liveness heartbeat into the CP placement pool; the CP stamps its own clock so a
        // skewed node clock can't fake liveness. In-memory only — no persist. The pool is
        // index-agnostic (D52): a node registers once as an interchangeable shard host.
        //
        // Node registration is **uncapped** (D53/D38 Option A): the scale entitlement counts distinct
        // primary-holding nodes, so registering a node adds capacity for free; the cap is enforced at
        // unit placement (`ResolveUnitOwner` / the `RegisterServedIndex` announce) instead.
        //
        // The replica-capability declaration rides the heartbeat (HA-G2): only a node with an object
        // store can serve replica windows read-through, so replica placement filters on it. Absent
        // (old binary) decodes false — the safe default (no replicas placed there).
        self.registry
            .register_node_with_capability(&req.endpoint, req.replica_capable, now_ms());
        Ok(Response::new(RegisterNodeResponse {}))
    }

    async fn get_license(
        &self,
        request: Request<GetLicenseRequest>,
    ) -> Result<Response<GetLicenseResponse>, Status> {
        self.gate("GetLicense", &request)?;
        Ok(Response::new(GetLicenseResponse {
            licensed: self.license.is_some(),
            licensee: self
                .license
                .as_ref()
                .map(|l| l.licensee.clone())
                .unwrap_or_default(),
            // The `*_nodes` fields mean literal distinct **primary-holding nodes** (D38/D53, Option
            // A): `max_nodes` is the entitlement cap, `current_nodes` the current usage — replicas
            // don't count (HA is free), and packing many indexes' primaries on one node counts once.
            max_nodes: self.entitled_nodes() as u32,
            current_nodes: self.registry.count_entitlement_nodes(now_ms()) as u32,
        }))
    }

    async fn resolve_unit_owner(
        &self,
        request: Request<ResolveUnitOwnerRequest>,
    ) -> Result<Response<ResolveUnitOwnerResponse>, Status> {
        use growlerdb_proto::v1::resolve_unit_owner_request::Unit as WireUnit;
        self.gate("ResolveUnitOwner", &request)?;
        let req = request.into_inner();
        // Map the wire unit oneof to the registry's `(shard | window)` unit — one placement path.
        let unit = match req.unit {
            Some(WireUnit::Shard(ordinal)) => growlerdb_controlplane::Unit::Shard(ordinal),
            Some(WireUnit::Window(window)) => growlerdb_controlplane::Unit::Window(window),
            None => {
                return Err(Status::invalid_argument(
                    "ResolveUnitOwnerRequest.unit is required (shard or window)",
                ))
            }
        };
        // No node has heartbeated yet (a transient bring-up state) → retryable, so the connector
        // backs off and re-asks rather than failing the ingest batch.
        let to_status = |e: RegistryError| match e {
            RegistryError::NoLiveNode { .. } => Status::unavailable(e.to_string()),
            other => registry_status(other),
        };
        // One placement path for every R (D53): resolve places 1 primary + R−1 read replicas. The
        // scale entitlement (distinct live primary-holding nodes, D38/D53) is enforced ATOMICALLY in
        // the registry's placement critical section — beyond the cap is RESOURCE_EXHAUSTED (at the cap
        // a fresh unit packs onto an already-primary node); re-resolves and dead-owner re-placement
        // always pass, and replicas are free. Pushes ride the placement-change hook.
        let holders = self
            .registry
            .resolve_unit_holders(
                &req.index,
                unit,
                self.replication_factor,
                self.entitled_nodes(),
                now_ms(),
            )
            .map_err(to_status)?;
        Ok(Response::new(ResolveUnitOwnerResponse {
            endpoint: holders.primary,
            // Proto contract: true iff this call MADE or MOVED the primary assignment — replica
            // churn (prune/top-up/trim) alone doesn't set it (HA-D7).
            created: holders.moved,
        }))
    }

    type SubscribeAssignmentsStream =
        std::pin::Pin<Box<dyn tokio_stream::Stream<Item = Result<NodeAssignments, Status>> + Send>>;

    async fn subscribe_assignments(
        &self,
        request: Request<SubscribeAssignmentsRequest>,
    ) -> Result<Response<Self::SubscribeAssignmentsStream>, Status> {
        self.gate("SubscribeAssignments", &request)?;
        let endpoint = request.into_inner().endpoint;
        validate_endpoint(&endpoint)?;
        // Identity gate (HA-D4d): the endpoint claim is only accepted for a currently-registered
        // pool node — an ops-scoped caller can't read an arbitrary endpoint's stream by naming it.
        // Residual trust: within the mesh, a caller could still heartbeat any endpoint first; the
        // internal RPCs are mesh-trusted (service token) — documented on the proto.
        if !self.registry.node_alive(&endpoint, now_ms()) {
            return Err(Status::failed_precondition(format!(
                "`{endpoint}` is not a registered pool node — RegisterNode first, then subscribe"
            )));
        }
        // Subscribe, seeded with the node's current snapshot (register-then-compute-then-send inside
        // the hub, so no placement change can slip between the seed and the first push). The stream
        // then yields a fresh snapshot on every placement change; the node reconciles idempotently.
        let rx = self.assignments.subscribe(&endpoint, &self.registry);
        use tokio_stream::StreamExt;
        let stream = tokio_stream::wrappers::WatchStream::new(rx).map(Ok);
        Ok(Response::new(Box::pin(stream)))
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
    /// Gate-free so both the `IngestionStatus` RPC and the background metrics sampler
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
            // ingestion probe iterates windows instead.
            let windowed = entry.definition.windowing.is_some();
            let routing = match entry.definition.routing_strategy() {
                growlerdb_core::RoutingStrategy::Hash => WireRouting::RoutingHash,
                growlerdb_core::RoutingStrategy::Partition => WireRouting::RoutingPartition,
            };

            // A **variant** index's source head can't be probed natively: released iceberg-rust can't
            // plan a v3 variant schema (D49), so the connector reads it via the Trino lane instead.
            // `source_probeable` tells the console this null head + "unknown" lag is expected — not an
            // outage — so a healthy variant index never rolls the cluster health up as a source failure.
            let source_probeable = !entry.definition.has_variant_field();

            // Source head (the position ingestion is racing to catch up to). Skipped (no native read)
            // for a non-probeable variant source.
            let (source_snapshot_id, source_timestamp_ms, source_readable) = match &reader {
                Some(r) if source_probeable => match r.current_snapshot(&source_table).await {
                    Ok((id, ts)) => (id, ts, true),
                    Err(_) => (0, 0, false),
                },
                _ => (0, 0, false),
            };
            // Snapshot id → commit-timestamp, to measure how far behind each shard's committed
            // checkpoint is in wall-clock terms. Best-effort: empty map ⇒ lag unknown.
            let snapshot_ts = match &reader {
                Some(r) => r
                    .snapshot_timestamps(&source_table)
                    .await
                    .unwrap_or_default(),
                None => std::collections::HashMap::new(),
            };

            // Source-health gauges: diagnose a source that wants Iceberg maintenance
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
                // Partition skew: one `current_plan` (manifest read, then cached) per
                // index — O(indexes), same order as the per-index metadata this loop already does.
                // Only emitted for identity-partitioned sources; best-effort.
                if let Ok(Some(skew)) = r.partition_skew(&source_table).await {
                    growlerdb_telemetry::sli::source_partition_skew(&name, skew);
                }
            }

            // Each shard's committed checkpoint, via its primary's Write.GetCheckpoint — fetched
            // CONCURRENTLY. A serial loop would do one fresh connect + RPC per shard in sequence, so
            // at hundreds of shards a single sample would take hundreds of round-trips and fall
            // behind its own cadence. A bounded JoinSet runs them in parallel; the state/lag math is
            // then a cheap synchronous pass.
            let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(SHARD_POLL_CONCURRENCY));
            let mut set = tokio::task::JoinSet::new();
            // Probe the index's shard set: ordinal shards, or the **time windows** for a windowed index
            // — its `shards` map is empty, its `windows` map holds the placement. `ordinal`
            // and `window` are 0 for the axis that doesn't apply; the row carries the one that does.
            if windowed {
                for (window, wa) in &entry.windows {
                    let window = *window;
                    let node = wa.assignment.primary.as_ref().map(|n| n.0.clone());
                    let sem = sem.clone();
                    let index = name.clone();
                    set.spawn(async move {
                        let Some(endpoint) = node else {
                            return (0u32, window, String::new(), Err("no_primary"));
                        };
                        let _permit = sem.acquire_owned().await;
                        // Windowed unit: routed by `window`; ordinal is 0. Thread the index so a
                        // multi-index pool node can resolve which index's window to probe.
                        let res = shard_checkpoint(&endpoint, &index, 0, window).await;
                        (0u32, window, endpoint, res)
                    });
                }
            } else {
                for (ordinal, assignment) in &entry.shards {
                    let ordinal = *ordinal;
                    // `node` is the primary endpoint, or "" when the shard has no primary yet.
                    let node = assignment.primary.as_ref().map(|n| n.0.clone());
                    let sem = sem.clone();
                    let index = name.clone();
                    set.spawn(async move {
                        let Some(endpoint) = node else {
                            return (ordinal, 0i64, String::new(), Err("no_primary"));
                        };
                        let _permit = sem.acquire_owned().await;
                        // Hash unit: routed by `shard` ordinal; window is 0. Thread the index so a
                        // multi-index pool node can resolve which index's shard to probe.
                        let res = shard_checkpoint(&endpoint, &index, ordinal, 0).await;
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

            // Export the ingestion-lag + shard-availability gauges so the Observability
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
                source_probeable,
                shards,
            });
        }
        items
    }

    /// Spawn a background task that recomputes the ingestion-lag + shard-availability gauges
    /// every `interval_secs`, independent of any console poll — so Prometheus always
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

    /// Spawn the **dead-owner sweeper** (HA-D2): every TTL/2, re-place units whose primary is
    /// confidently dead through the same path a write-driven resolve takes
    /// ([`Registry::sweep_dead_primaries`] — idempotent, entitlement-aware, persist + push), so
    /// quiescent units on a dead node become available again without waiting for a write. Only the
    /// **leader** sweeps (standbys skip each tick); the liveness grace window after boot/promotion
    /// is respected inside the sweep itself.
    pub fn spawn_dead_owner_sweeper(&self) {
        use growlerdb_controlplane::NODE_HEARTBEAT_TTL_MS;
        let registry = self.registry.clone();
        let replication_factor = self.replication_factor;
        let entitled = self.entitled_nodes();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(
                (NODE_HEARTBEAT_TTL_MS / 2).max(1000) as u64,
            ));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if !registry.is_leader() {
                    continue;
                }
                match registry.sweep_dead_primaries(replication_factor, entitled, now_ms()) {
                    Ok(0) => {}
                    Ok(moved) => {
                        tracing::info!(moved, "dead-owner sweep re-placed unit primaries")
                    }
                    Err(e) => tracing::warn!(error = %e, "dead-owner sweep failed; will retry"),
                }
            }
        });
    }

    /// Spawn the **placement sweeper** (HA-D8 / 357.26): every TTL/2, drive every unit to
    /// `replication_factor` live holders via the idempotent resolve path
    /// ([`Registry::ensure_placement`]) — place a primary for each declared hash ordinal lacking one
    /// (round-robin; nodes build/load on assignment) and fill replicas. Makes the pool self-organize
    /// and read HA independent of write activity. Counterpart to
    /// [`spawn_dead_owner_sweeper`](Self::spawn_dead_owner_sweeper). Only the leader sweeps; the grace
    /// window is honored inside the sweep.
    pub fn spawn_placement_sweeper(&self) {
        use growlerdb_controlplane::NODE_REANNOUNCE_INTERVAL_MS;
        let registry = self.registry.clone();
        let replication_factor = self.replication_factor;
        let entitled = self.entitled_nodes();
        tokio::spawn(async move {
            // Sweep several times per heartbeat interval so the cold-start pool self-organizes within
            // a few seconds of the initial settle (build-on-assignment then serves), instead of on a
            // coarse TTL/2 tick. A no-op sweep is a cheap read, so a tight cadence is fine; dead-owner
            // re-placement still keys off the TTL, not this interval.
            let mut tick = tokio::time::interval(std::time::Duration::from_millis(
                (NODE_REANNOUNCE_INTERVAL_MS / 4).max(1000) as u64,
            ));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                if !registry.is_leader() {
                    continue;
                }
                match registry.ensure_placement(replication_factor, entitled, now_ms()) {
                    Ok(0) => {}
                    Ok(n) => {
                        tracing::info!(placed = n, "placement sweep placed primaries/replicas")
                    }
                    Err(e) => tracing::warn!(error = %e, "placement sweep failed; will retry"),
                }
            }
        });
    }
}

/// In_sync tolerance for the Ingestion view: a shard within this much wall-clock lag of
/// the source head still reads `in_sync`. A live source commits a fresh snapshot every few seconds,
/// so a healthy connector is always momentarily a snapshot behind; without a tolerance the badge
/// flaps in_sync↔behind. Generous enough to absorb normal pipeline latency, small enough to surface
/// a genuine backlog.
const INGESTION_LAG_TOLERANCE_MS: i64 = 15_000;

/// Max concurrent per-shard `GetCheckpoint` probes in one ingestion sample. Bounds the
/// fan-out so a many-shard index doesn't open every connection at once, while still collapsing
/// a serial hundreds-of-round-trips sweep into a handful of parallel batches.
const SHARD_POLL_CONCURRENCY: usize = 32;

/// One shard's concurrent checkpoint probe: `(ordinal, primary endpoint, checkpoint or
/// error state)` — `Ok((committed_snapshot, index_snapshot))`, or `Err(state)` for no-primary /
/// unreachable / source-recreated.
// (ordinal, window, node-endpoint, checkpoint result). `window` is 0 for an ordinal shard; `ordinal`
// is 0 for a windowed index's window.
type ShardProbe = (u32, i64, String, Result<(i64, u64), &'static str>);

/// Classify a shard's ingestion `state` + its `lag_ms` vs the source head. `in_sync` when
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
async fn shard_checkpoint(
    endpoint: &str,
    index: &str,
    shard: u32,
    window: i64,
) -> Result<(i64, u64), &'static str> {
    let (channel, stamp) = growlerdb_proto::service_token::node_channel(endpoint.to_string())
        .await
        .map_err(|_| "unreachable")?;
    let mut client = WriteClient::with_interceptor(channel, stamp);
    let resp = client
        // `window` selects the time-window shard on a windowed node; `shard` the ordinal on a hash
        // node. A single-index node ignores all three, but a MULTI-index pool node REQUIRES the index
        // (+ ordinal) to route the probe — without them it answers InvalidArgument, read back as
        // `unreachable`, reporting every pool-served shard as DOWN.
        .get_checkpoint(GetCheckpointRequest {
            window,
            index: index.to_string(),
            shard,
        })
        .await
        // A node serving a stale index over a recreated source refuses the checkpoint with
        // FAILED_PRECONDITION — surface that as a distinct `source_recreated` state, not
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

/// Render a coarse source type for the wire (the create-form introspection).
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
        // /v1/login verifies a credential and mints an HS256 session JWT validatable by the
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
        // An account is unlocked until it crosses the failure threshold, then locked
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
        // A session JWT minted before a subject's roles change (which bumps the
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
        // Per-field type/analyzer/fast/cached/PK + a block reason.
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
        // A sensitive field can't be cached → a block reason, not cached.
        assert!(!f("ssn").cached);
        assert!(f("ssn").blocked.contains("sensitive"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_index_carries_the_authoritative_definition_json() {
        // A booting node loads this to open the on-disk index at the definition a durable alter last
        // committed, instead of a stale local def — so definition_json must round-trip to exactly the
        // registered ResolvedIndex.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let def = IndexDefinition::from_yaml(
            "name: reload\nsource: { iceberg: { catalog: g, table: g.reload } }\nmapping: { selection: ALL }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        svc.registry.create(def.clone()).unwrap();

        let resp = svc
            .get_index(Request::new(GetIndexRequest {
                name: "reload".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(
            !resp.definition_json.is_empty(),
            "the authoritative definition is carried for boot-time reload"
        );
        let parsed: growlerdb_core::ResolvedIndex =
            serde_json::from_str(&resp.definition_json).expect("definition_json parses");
        assert_eq!(parsed, def, "round-trips to the registered definition");
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
        // Legacy index ⇒ no bucket map vended.
        assert!(resp.bucket_owners.is_empty());

        // Per-shard placement: primary + replica + active state.
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
        // A live-CP gateway needs the windowing config + per-window event zone-map on the
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
    async fn windowed_reindex_plan_enumerates_hot_windows_and_skips_cold() {
        // A windowed reindex plan enumerates one unit per HOT window (ordinal 0, the window id, its
        // primary) and skips cold/parked windows (deferred — no local writer).
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        svc.registry.create(resolved_windowed("logs")).unwrap();
        let (w0, w1, w2) = (
            1_700_000_000_000_i64,
            1_700_086_400_000_i64,
            1_700_172_800_000_i64,
        );
        svc.registry.assign_window("logs", w0, "node-a").unwrap();
        svc.registry.assign_window("logs", w1, "node-b").unwrap();
        svc.registry.assign_window("logs", w2, "node-c").unwrap();
        // Park w1 → the plan must skip it.
        svc.registry.set_window_cold("logs", w1, true).unwrap();

        let plan = plan_reindex_shards(&svc.registry, "logs").expect("windowed plan");
        let mut windows: Vec<i64> = plan
            .iter()
            .map(|(ord, w, _)| {
                assert_eq!(*ord, 0, "windowed units carry ordinal 0");
                *w
            })
            .collect();
        windows.sort();
        assert_eq!(
            windows,
            vec![w0, w2],
            "hot windows enumerated, cold w1 skipped"
        );
        let endpoint = |w: i64| {
            plan.iter()
                .find(|(_, ww, _)| *ww == w)
                .map(|(_, _, e)| e.clone())
        };
        assert_eq!(endpoint(w0).as_deref(), Some("node-a"));
        assert_eq!(endpoint(w2).as_deref(), Some("node-c"));

        // All windows cold → nothing to reindex (FailedPrecondition, not a silent empty plan).
        svc.registry.set_window_cold("logs", w0, true).unwrap();
        svc.registry.set_window_cold("logs", w2, true).unwrap();
        let err = plan_reindex_shards(&svc.registry, "logs").unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_places_replicas_when_replication_factor_gt_1() {
        // D53: with R=2 a resolve places a primary + one read replica per unit. ResolveUnitOwner
        // still returns the primary (the write target); GetIndex.shard_status carries the replica,
        // which is what the gateway reads to fail a read over to a live holder.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()).with_replication_factor(2);
        svc.registry.create(resolved_windowed("logs")).unwrap();
        for ep in ["http://node-a:50051", "http://node-b:50051"] {
            svc.register_node(Request::new(RegisterNodeRequest {
                endpoint: ep.into(),
                replica_capable: true,
            }))
            .await
            .unwrap();
        }

        let r = svc
            .resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                index: "logs".into(),
                unit: Some(growlerdb_proto::v1::resolve_unit_owner_request::Unit::Window(10)),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(r.created);
        let primary = r.endpoint;

        let gi = svc
            .get_index(Request::new(GetIndexRequest {
                name: "logs".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let w = gi
            .shard_status
            .iter()
            .find(|s| s.window == 10)
            .expect("window 10 placed");
        assert_eq!(w.primary, primary, "the write target is the primary");
        assert_eq!(w.replicas.len(), 1, "R=2 ⇒ one read replica");
        assert_ne!(w.replicas[0], primary, "the replica is a distinct node");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resolve_places_replicas_only_on_replica_capable_nodes() {
        // HA-G2 over the wire: a node that heartbeats WITHOUT `replica_capable` (an old binary, or
        // `serve-pool --register` with no object store) must never be handed replica units it
        // could not serve read-through — while primaries still place on it by load alone.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()).with_replication_factor(2);
        svc.registry.create(resolved_windowed("logs")).unwrap();
        let cap = "http://cap:50051";
        let nocap = "http://nocap:50051";
        for (ep, capable) in [(cap, true), (nocap, false)] {
            svc.register_node(Request::new(RegisterNodeRequest {
                endpoint: ep.into(),
                replica_capable: capable,
            }))
            .await
            .unwrap();
        }
        let resolve = |w: i64| {
            svc.resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                index: "logs".into(),
                unit: Some(growlerdb_proto::v1::resolve_unit_owner_request::Unit::Window(w)),
            }))
        };
        // First unit: primary lands on the capable node (least-loaded tie → lexicographic first);
        // the only other node is incapable, so at R=2 the unit holds ZERO replicas rather than
        // placing one on a node that could never serve it.
        assert_eq!(resolve(10).await.unwrap().into_inner().endpoint, cap);
        // Second unit: load now favors the incapable node for the PRIMARY (capability never gates
        // primaries), and the capable node takes the replica slot.
        assert_eq!(resolve(20).await.unwrap().into_inner().endpoint, nocap);
        let gi = svc
            .get_index(Request::new(GetIndexRequest {
                name: "logs".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        let replicas_of = |w: i64| {
            gi.shard_status
                .iter()
                .find(|s| s.window == w)
                .expect("window placed")
                .replicas
                .clone()
        };
        assert!(
            replicas_of(10).is_empty(),
            "no capable second node ⇒ no replica — never the incapable node"
        );
        assert_eq!(
            replicas_of(20),
            vec![cap.to_string()],
            "the capable node takes the replica of the incapable node's primary"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn entitlement_counts_distinct_primary_nodes_not_units_or_processes() {
        // D53/D38 (Option A): the scale cap is on distinct live **primary-holding nodes**, enforced
        // atomically at placement — node registration is uncapped, a windowed index accumulating
        // windows on already-primary nodes costs nothing new, and at the cap a fresh unit (of the
        // same OR a different index) packs onto an already-primary node rather than bricking.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()); // R=1, no license → 3-node free tier
        svc.registry.create(resolved_windowed("logs")).unwrap();
        // Register MORE nodes than the cap — registration is uncapped.
        for i in 0..6 {
            svc.register_node(Request::new(RegisterNodeRequest {
                endpoint: format!("http://n{i}:50051"),
                replica_capable: true,
            }))
            .await
            .unwrap();
        }
        let resolve = |index: &'static str, w: i64| {
            svc.resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                index: index.into(),
                unit: Some(growlerdb_proto::v1::resolve_unit_owner_request::Unit::Window(w)),
            }))
        };
        let counted = ["http://n0:50051", "http://n1:50051", "http://n2:50051"];
        // Three windows spread onto three distinct nodes → the free-tier cap of 3 nodes.
        for w in [10_i64, 20, 30] {
            assert!(resolve("logs", w).await.is_ok(), "window {w} within cap");
        }
        // The 4th-day scenario (lifetime-brick fix): a 4th window does NOT light up a new node —
        // at the cap it packs onto a node already holding a primary instead of a fresh node.
        let w40 = resolve("logs", 40).await.expect("4th window still places");
        assert!(
            counted.contains(&w40.into_inner().endpoint.as_str()),
            "at the cap a new window packs onto an already-primary node"
        );
        // Re-resolving an already-placed unit passes too (never a new node).
        assert!(resolve("logs", 10).await.is_ok(), "idempotent re-resolve");
        // A FRESH index at the cap also packs onto an already-primary node (node semantics: a new
        // index co-located on a counted node is free — this is the resolve path's soft-pack, not a
        // brick). The hard refusal lives on the fixed-endpoint announce path (its own test).
        svc.registry.create(resolved_windowed("audit")).unwrap();
        let a10 = resolve("audit", 10)
            .await
            .expect("fresh index packs at cap");
        assert!(
            counted.contains(&a10.into_inner().endpoint.as_str()),
            "a fresh index at the cap co-locates on an already-counted node"
        );
        // GetLicense reports NODES (3), not units (5 windows) and not the 6 registered nodes.
        let lic = svc
            .get_license(Request::new(GetLicenseRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            lic.current_nodes, 3,
            "3 distinct primary-holding nodes serving"
        );
        assert_eq!(lic.max_nodes, 3, "free-tier entitlement");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn replicas_are_free_against_the_node_entitlement() {
        // AC#1: enabling replication (R>1) doesn't reduce the allowance — a replica is no new primary.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()).with_replication_factor(2);
        svc.registry.create(resolved_windowed("logs")).unwrap();
        for i in 0..4 {
            svc.register_node(Request::new(RegisterNodeRequest {
                endpoint: format!("http://n{i}:50051"),
                replica_capable: true,
            }))
            .await
            .unwrap();
        }
        // 3 units at R=2 = 3 primaries + 3 replicas = 6 holder slots, but the primaries land on only
        // 2 distinct nodes (see below).
        for w in [10_i64, 20, 30] {
            assert!(
                svc.resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                    index: "logs".into(),
                    unit: Some(growlerdb_proto::v1::resolve_unit_owner_request::Unit::Window(w)),
                }))
                .await
                .is_ok(),
                "unit {w} places its primary + replica within the free tier"
            );
        }
        let lic = svc
            .get_license(Request::new(GetLicenseRequest {}))
            .await
            .unwrap()
            .into_inner();
        // Deterministic placement over n0..n3: w10 → primary n0 (+replica n1), w20 → primary n2
        // (+replica n3), w30 → primary n0 again (all loads tied → smallest endpoint). That's 6
        // holder slots but only TWO distinct primary-holding nodes — replicas never count, and
        // re-using a primary node is free.
        assert_eq!(
            lic.current_nodes, 2,
            "2 distinct primary-holding nodes — the 3 read replicas don't consume the entitlement"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn subscribe_assignments_pushes_placement_to_the_holder_node() {
        // D53 push: a node subscribes and, when the CP places a unit that lands on it (as primary or
        // replica), the node's stream is pushed a fresh snapshot carrying that unit — so a placed
        // replica knows to open + serve it.
        use tokio_stream::StreamExt;
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()).with_replication_factor(2);
        svc.registry.create(resolved_windowed("logs")).unwrap();
        for ep in ["http://node-a:50051", "http://node-b:50051"] {
            svc.register_node(Request::new(RegisterNodeRequest {
                endpoint: ep.into(),
                replica_capable: true,
            }))
            .await
            .unwrap();
        }

        // node-b subscribes; its first snapshot is empty (it holds nothing yet).
        let mut stream = svc
            .subscribe_assignments(Request::new(SubscribeAssignmentsRequest {
                endpoint: "http://node-b:50051".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(
            stream.next().await.unwrap().unwrap().units.is_empty(),
            "initial snapshot: node-b holds nothing"
        );

        // Resolving window 10 at R=2 lands a primary on one node and a replica on the other, so
        // node-b holds it either way — and the push delivers node-b its updated snapshot.
        let r = svc
            .resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                index: "logs".into(),
                unit: Some(growlerdb_proto::v1::resolve_unit_owner_request::Unit::Window(10)),
            }))
            .await
            .unwrap()
            .into_inner();

        let pushed = stream.next().await.unwrap().unwrap();
        let held: Vec<_> = pushed
            .units
            .iter()
            .filter(|u| {
                matches!(
                    u.unit,
                    Some(growlerdb_proto::v1::unit_assignment::Unit::Window(10))
                )
            })
            .collect();
        assert_eq!(held.len(), 1, "node-b was pushed its window-10 assignment");
        assert_eq!(
            held[0].primary,
            r.endpoint == "http://node-b:50051",
            "node-b's role matches whether it's the resolved primary"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn placement_pushes_on_every_mutation_path() {
        // HA-D1: pushes are wired at the registry's persist boundary, so EVERY placement mutation —
        // an announce (RegisterServedIndex), remove_node, promote_replica, drop_index — pushes the
        // affected node a fresh snapshot, not just ResolveUnitOwner.
        use tokio_stream::StreamExt;
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let b = "http://node-b:50051";
        svc.register_node(Request::new(RegisterNodeRequest {
            endpoint: b.into(),
            replica_capable: true,
        }))
        .await
        .unwrap();
        let mut stream = svc
            .subscribe_assignments(Request::new(SubscribeAssignmentsRequest {
                endpoint: b.into(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(stream.next().await.unwrap().unwrap().units.is_empty());

        // 1. RegisterServedIndex (announce) → push with the shard this node now primaries.
        svc.register_served_index(Request::new(RegisterServedIndexRequest {
            definition_json: serde_json::to_string(&resolved("docs")).unwrap(),
            endpoint: b.into(),
            shard_count: 1,
            shard_ordinals: vec![],
            windows: vec![],
            pool_managed: false,
        }))
        .await
        .unwrap();
        let snap = stream.next().await.unwrap().unwrap();
        assert_eq!(snap.units.len(), 1, "announce pushed the new assignment");
        assert!(snap.units[0].primary);

        // 2. remove_node (a fencing/failover mutation that never pushed before) → push without it.
        svc.registry
            .remove_node("docs", 0, &growlerdb_controlplane::NodeId::from(b))
            .unwrap();
        assert!(
            stream.next().await.unwrap().unwrap().units.is_empty(),
            "remove_node pushed the loss — the node stops serving what it no longer holds"
        );

        // 3. add_replica + promote_replica → each pushes; after promotion node-b is primary again.
        svc.registry.add_replica("docs", 0, b).unwrap();
        let snap = stream.next().await.unwrap().unwrap();
        assert!(!snap.units[0].primary, "replica assignment pushed");
        svc.registry.promote_replica("docs", 0).unwrap();
        let snap = stream.next().await.unwrap().unwrap();
        assert!(snap.units[0].primary, "promotion pushed");

        // 4. drop_index → push with the unit gone.
        svc.registry.drop_index("docs").unwrap();
        assert!(
            stream.next().await.unwrap().unwrap().units.is_empty(),
            "drop_index pushed the removal"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn resubscribe_seeds_fresh_and_dropped_streams_are_evicted() {
        // HA-D4: (a/c) the seed is computed under the hub lock AFTER the receiver registers, so a
        // re-subscribe can only re-send the CURRENT truth to an endpoint's other live receivers —
        // never reset them to a stale seed; (b) senders whose receivers all dropped are evicted.
        use tokio_stream::StreamExt;
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()).with_replication_factor(2);
        svc.registry.create(resolved_windowed("logs")).unwrap();
        let b = "http://node-b:50051";
        for ep in ["http://node-a:50051", b] {
            svc.register_node(Request::new(RegisterNodeRequest {
                endpoint: ep.into(),
                replica_capable: true,
            }))
            .await
            .unwrap();
        }
        let subscribe = || async {
            svc.subscribe_assignments(Request::new(SubscribeAssignmentsRequest {
                endpoint: b.into(),
            }))
            .await
            .unwrap()
            .into_inner()
        };
        let mut s1 = subscribe().await;
        assert!(s1.next().await.unwrap().unwrap().units.is_empty());
        // Place a unit (R=2 ⇒ node-b holds it) → s1 sees it.
        svc.resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
            index: "logs".into(),
            unit: Some(growlerdb_proto::v1::resolve_unit_owner_request::Unit::Window(10)),
        }))
        .await
        .unwrap();
        assert_eq!(s1.next().await.unwrap().unwrap().units.len(), 1);
        // Re-subscribe: the new stream's seed carries the CURRENT assignment, and the re-seed that
        // reaches s1 is that same fresh snapshot — not an empty stale one.
        let mut s2 = subscribe().await;
        assert_eq!(
            s2.next().await.unwrap().unwrap().units.len(),
            1,
            "the re-subscribe seed is current"
        );
        assert_eq!(
            s1.next().await.unwrap().unwrap().units.len(),
            1,
            "the surviving stream was not reset to a stale seed"
        );
        // Drop both streams; the next placement change evicts the endpoint's sender entirely.
        drop(s1);
        drop(s2);
        svc.resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
            index: "logs".into(),
            unit: Some(growlerdb_proto::v1::resolve_unit_owner_request::Unit::Window(20)),
        }))
        .await
        .unwrap();
        assert!(
            svc.assignments.senders.lock().unwrap().is_empty(),
            "senders with no live receiver are evicted on the next push"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn subscribe_rejects_an_unregistered_endpoint() {
        // HA-D4d: the endpoint claim is only honored for a currently-registered pool node.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let ep = "http://ghost:50051";
        match svc
            .subscribe_assignments(Request::new(SubscribeAssignmentsRequest {
                endpoint: ep.into(),
            }))
            .await
        {
            Err(err) => assert_eq!(err.code(), Code::FailedPrecondition),
            Ok(_) => panic!("an unregistered endpoint must not subscribe"),
        }
        // After RegisterNode the same subscribe succeeds.
        svc.register_node(Request::new(RegisterNodeRequest {
            endpoint: ep.into(),
            replica_capable: true,
        }))
        .await
        .unwrap();
        assert!(svc
            .subscribe_assignments(Request::new(SubscribeAssignmentsRequest {
                endpoint: ep.into(),
            }))
            .await
            .is_ok());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn register_node_rejects_malformed_endpoints() {
        // HA-D6: a garbage endpoint (e.g. a field decoded from an incompatible old binary) must
        // fail loudly, never seed the pool with an entry least-loaded placement would prefer.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        for bad in [
            "",
            "logs",
            "http://",
            "host:notaport",
            "http://host",
            "ho st:1",
            "ftp://h:1",
        ] {
            let err = svc
                .register_node(Request::new(RegisterNodeRequest {
                    endpoint: bad.into(),
                    replica_capable: true,
                }))
                .await
                .expect_err(&format!("`{bad}` must be rejected"));
            assert_eq!(err.code(), Code::InvalidArgument, "`{bad}`");
        }
        for good in ["http://n1:50051", "https://n1:443", "n1:1", "[::1]:50051"] {
            assert!(
                svc.register_node(Request::new(RegisterNodeRequest {
                    endpoint: good.into(),
                    replica_capable: true,
                }))
                .await
                .is_ok(),
                "`{good}` must be accepted"
            );
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn register_served_index_conflicts_on_a_live_foreign_primary() {
        // HA-D7: announces are first-wins. A shard primaried by a LIVE node refuses a foreign
        // announce (PLACEMENT_CONFLICT / FAILED_PRECONDITION); once the holder is confidently dead
        // the takeover re-points; idempotent re-announce always passes.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let (a, b) = ("http://node-a:50051", "http://node-b:50051");
        let announce = |ep: &str| {
            let def = serde_json::to_string(&resolved("docs")).unwrap();
            let ep = ep.to_string();
            svc.register_served_index(Request::new(RegisterServedIndexRequest {
                definition_json: def,
                endpoint: ep,
                shard_count: 1,
                shard_ordinals: vec![],
                windows: vec![],
                pool_managed: false,
            }))
        };
        // node-a heartbeats (tracked, live) and announces; disarm the startup grace so liveness
        // verdicts are immediate.
        svc.registry.register_node(a, now_ms());
        svc.registry.set_placement_grace_anchor(
            now_ms() - growlerdb_controlplane::NODE_HEARTBEAT_TTL_MS - 1,
        );
        announce(a).await.unwrap();
        // A live foreign announce is refused — no last-write-wins re-point.
        let err = announce(b).await.unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert_eq!(
            svc.registry.shard_map("docs").unwrap()[&0].primary,
            Some(growlerdb_controlplane::NodeId::from(a))
        );
        // Idempotent re-announce by the holder passes (the D53-blessed upsert).
        announce(a).await.unwrap();
        // node-a's heartbeat lapses past the TTL → node-b's announce is a takeover.
        svc.registry.register_node(
            a,
            now_ms() - growlerdb_controlplane::NODE_HEARTBEAT_TTL_MS - 1,
        );
        announce(b).await.unwrap();
        assert_eq!(
            svc.registry.shard_map("docs").unwrap()[&0].primary,
            Some(growlerdb_controlplane::NodeId::from(b))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn register_served_index_enforces_the_entitlement() {
        // HA-D3a: RegisterServedIndex is no longer an entitlement bypass — a primary on the 4th
        // distinct node on the free tier is RESOURCE_EXHAUSTED, fail-closed even though nobody
        // heartbeats.
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path()); // no license → FREE_NODE_LIMIT (3) nodes
        for (i, name) in ["docs", "logs", "events"].iter().enumerate() {
            svc.register_served_index(Request::new(RegisterServedIndexRequest {
                definition_json: serde_json::to_string(&resolved(name)).unwrap(),
                endpoint: format!("http://n{i}:50051"),
                shard_count: 1,
                shard_ordinals: vec![],
                windows: vec![],
                pool_managed: false,
            }))
            .await
            .unwrap();
        }
        let err = svc
            .register_served_index(Request::new(RegisterServedIndexRequest {
                definition_json: serde_json::to_string(&resolved("audit")).unwrap(),
                endpoint: "http://n3:50051".into(),
                shard_count: 1,
                shard_ordinals: vec![],
                windows: vec![],
                pool_managed: false,
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::ResourceExhausted);
        // The refused announce placed nothing.
        assert!(svc.registry.shard_map("audit").unwrap().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cp_driven_window_placement_via_rpc() {
        // Nodes heartbeat into the pool (RegisterNode), the connector resolves each window's
        // owner (ResolveUnitOwner, placing on first ask), and GetIndex reflects the placement.
        let tmp = tempfile::tempdir().unwrap();
        // A license raises the unit entitlement above the free tier so this 4-window placement
        // isn't scale-gated (the free-tier unit cap is covered by its own test).
        let svc = service(tmp.path()).with_license(Some(crate::license::License {
            licensee: "test".into(),
            max_nodes: 100,
            expires_at: None,
        }));
        svc.registry.create(resolved_windowed("logs")).unwrap();

        let resolve = |w: i64| {
            svc.resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                index: "logs".into(),
                unit: Some(growlerdb_proto::v1::resolve_unit_owner_request::Unit::Window(w)),
            }))
        };

        // With no node registered yet, placement is retryable (Unavailable), not a hard failure.
        assert_eq!(resolve(10).await.unwrap_err().code(), Code::Unavailable);

        // Two nodes register into the (index-agnostic) pool.
        for ep in ["http://node-a:50051", "http://node-b:50051"] {
            svc.register_node(Request::new(RegisterNodeRequest {
                endpoint: ep.into(),
                replica_capable: true,
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
            svc.resolve_unit_owner(Request::new(ResolveUnitOwnerRequest {
                index: "ghost".into(),
                unit: Some(growlerdb_proto::v1::resolve_unit_owner_request::Unit::Window(1)),
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
            .set_bucket_map("docs", None, &BucketMap::balanced(2))
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

        // Two nodes each register **one** ordinal of a 2-shard index.
        for (ord, ep) in [(0u32, "http://node-a:50051"), (1u32, "http://node-b:50051")] {
            svc.register_served_index(Request::new(RegisterServedIndexRequest {
                definition_json: def_json.clone(),
                endpoint: ep.into(),
                shard_count: 2,
                shard_ordinals: vec![ord],
                windows: vec![],
                pool_managed: false,
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
                pool_managed: false,
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
                pool_managed: false,
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
            pool_managed: false,
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
                pool_managed: false,
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
                    cold: false,
                },
                ServedWindow {
                    window: 200, // no docs yet → no zone-map reported
                    event_min: 0,
                    event_max: 0,
                    has_event_bounds: false,
                    cold: true, // parked
                },
            ],
            pool_managed: false,
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
        // The reported per-window tier round-trips through the registry (drives the gateway's
        // /v1/cold): window 100 was reported hot, 200 parked.
        assert!(!w100.cold, "window 100 reported hot");
        assert!(w200.cold, "window 200 reported cold");
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
        // ingestion feed must report those windows, not "0 of 0 shards".
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

    // ---- online reshard, end to end against stub nodes ---------------------------------------

    /// A stub Node `Admin` that records reindex calls (and can block on a gate) — stands in for
    /// the real per-node rebuild so the FULL grow path (register → plan → apply → cutover → trim)
    /// runs without Iceberg. Every other RPC is unimplemented.
    #[derive(Clone, Default)]
    struct StubNodeAdmin {
        /// `(shard_ordinal, owners.len())` per reindex call, in arrival order.
        reindexed: Arc<std::sync::Mutex<Vec<(u32, usize)>>>,
        /// `(shard_ordinal, phase i32)` per reindex call — lets the coordinated-reindex tests assert
        /// the BUILD → PROMOTE (or DISCARD-on-abort) sequence.
        phases: Arc<std::sync::Mutex<Vec<(u32, i32)>>>,
        /// When true, a BUILD-phase reindex returns an error — to drive the abort path.
        fail_build: bool,
        /// When set: signal `entered` on a reindex call, then wait for `release` — lets a test
        /// deterministically interleave a concurrent placement commit mid-build.
        gate: Option<(Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>)>,
        /// Tripped by `CancelReindex` (and it releases a gated build): a gated BUILD then returns
        /// CANCELLED, mirroring a real node's populate-loop abort so the cancel path can be tested.
        cancel: Arc<std::sync::atomic::AtomicBool>,
        /// When true, `ReindexPrecheck` reports `ok = false` (disk-short) — drives the CP's up-front
        /// disk-precheck refusal.
        disk_short: bool,
    }

    #[tonic::async_trait]
    impl growlerdb_proto::Admin for StubNodeAdmin {
        async fn describe_index(
            &self,
            _r: Request<growlerdb_proto::v1::DescribeIndexRequest>,
        ) -> Result<Response<growlerdb_proto::v1::DescribeIndexResponse>, Status> {
            Err(Status::unimplemented("stub"))
        }
        async fn alter_index(
            &self,
            _r: Request<growlerdb_proto::v1::AlterIndexRequest>,
        ) -> Result<Response<growlerdb_proto::v1::AlterIndexResponse>, Status> {
            Err(Status::unimplemented("stub"))
        }
        async fn reindex_index(
            &self,
            r: Request<growlerdb_proto::v1::ReindexIndexRequest>,
        ) -> Result<Response<growlerdb_proto::v1::ReindexIndexResponse>, Status> {
            let req = r.into_inner();
            self.reindexed
                .lock()
                .unwrap()
                .push((req.shard_ordinal, req.bucket_owners.len()));
            self.phases
                .lock()
                .unwrap()
                .push((req.shard_ordinal, req.phase));
            if self.fail_build && req.phase == growlerdb_proto::v1::ReindexPhase::Build as i32 {
                return Err(Status::internal("stub build failure"));
            }
            // Gate only the build phases (FULL / BUILD) — a PROMOTE/DISCARD the driver sends after
            // releasing must not block, or the cancel/abort unwind would deadlock.
            let is_build = req.phase == growlerdb_proto::v1::ReindexPhase::Full as i32
                || req.phase == growlerdb_proto::v1::ReindexPhase::Build as i32;
            if is_build {
                if let Some((entered, release)) = &self.gate {
                    entered.notify_one();
                    release.notified().await;
                }
            }
            // A cancel that arrived during a gated BUILD aborts it with CANCELLED, as a real node
            // does when its populate loop observes the flag.
            if req.phase == growlerdb_proto::v1::ReindexPhase::Build as i32
                && self.cancel.load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(Status::cancelled("stub build canceled"));
            }
            Ok(Response::new(growlerdb_proto::v1::ReindexIndexResponse {
                doc_count: 3,
                snapshot: 1,
            }))
        }
        async fn reindex_status(
            &self,
            _r: Request<growlerdb_proto::v1::ReindexStatusRequest>,
        ) -> Result<Response<growlerdb_proto::v1::ReindexStatusResponse>, Status> {
            Ok(Response::new(growlerdb_proto::v1::ReindexStatusResponse {
                building: !self.cancel.load(std::sync::atomic::Ordering::SeqCst),
                docs_done: 1,
                docs_total: 3,
                cancel_requested: self.cancel.load(std::sync::atomic::Ordering::SeqCst),
            }))
        }
        async fn cancel_reindex(
            &self,
            _r: Request<growlerdb_proto::v1::CancelReindexRequest>,
        ) -> Result<Response<growlerdb_proto::v1::CancelReindexResponse>, Status> {
            self.cancel.store(true, std::sync::atomic::Ordering::SeqCst);
            // Wake a gated build so it observes the cancel and returns promptly.
            if let Some((_, release)) = &self.gate {
                release.notify_one();
            }
            Ok(Response::new(
                growlerdb_proto::v1::CancelReindexResponse::default(),
            ))
        }
        async fn reindex_precheck(
            &self,
            _r: Request<growlerdb_proto::v1::ReindexPrecheckRequest>,
        ) -> Result<Response<growlerdb_proto::v1::ReindexPrecheckResponse>, Status> {
            Ok(Response::new(
                growlerdb_proto::v1::ReindexPrecheckResponse {
                    ok: !self.disk_short,
                    free_bytes: if self.disk_short { 10 } else { 1_000_000 },
                    needed_bytes: 100,
                    index_bytes: 33,
                    probed: true,
                },
            ))
        }
        async fn reconcile_index(
            &self,
            _r: Request<growlerdb_proto::v1::ReconcileIndexRequest>,
        ) -> Result<Response<growlerdb_proto::v1::ReconcileIndexResponse>, Status> {
            Err(Status::unimplemented("stub"))
        }
        async fn compact_index(
            &self,
            _r: Request<growlerdb_proto::v1::CompactIndexRequest>,
        ) -> Result<Response<growlerdb_proto::v1::CompactIndexResponse>, Status> {
            Err(Status::unimplemented("stub"))
        }
        async fn backup_index(
            &self,
            _r: Request<growlerdb_proto::v1::BackupIndexRequest>,
        ) -> Result<Response<growlerdb_proto::v1::BackupIndexResponse>, Status> {
            Err(Status::unimplemented("stub"))
        }
        async fn backup_status(
            &self,
            _r: Request<growlerdb_proto::v1::BackupStatusRequest>,
        ) -> Result<Response<growlerdb_proto::v1::BackupStatusResponse>, Status> {
            Err(Status::unimplemented("stub"))
        }
    }

    /// Serve `stub` on an ephemeral port; return its routable endpoint.
    async fn spawn_stub_node(stub: StubNodeAdmin) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(growlerdb_proto::AdminServer::new(stub))
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        format!("http://{addr}")
    }

    async fn register(svc: &ControlPlaneService, def_json: &str, ep: &str, total: u32, ord: u32) {
        svc.register_served_index(Request::new(RegisterServedIndexRequest {
            definition_json: def_json.to_string(),
            endpoint: ep.into(),
            shard_count: total,
            shard_ordinals: vec![ord],
            windows: vec![],
            pool_managed: false,
        }))
        .await
        .unwrap();
    }

    /// The full online grow for a **normally-created** index: nodes announce (adopting the
    /// bucket map), the growth build target registers with the new total WITHOUT flipping live
    /// routing, and apply builds → cuts over → trims.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_normally_registered_index_completes_an_online_grow() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let stub = |log: &Arc<std::sync::Mutex<Vec<(u32, usize)>>>| StubNodeAdmin {
            reindexed: log.clone(),
            gate: None,
            ..Default::default()
        };

        // The index's original two nodes announce → the balanced(2) map is adopted.
        let ep0 = spawn_stub_node(stub(&log)).await;
        let ep1 = spawn_stub_node(stub(&log)).await;
        register(&svc, &def_json, &ep0, 2, 0).await;
        register(&svc, &def_json, &ep1, 2, 1).await;
        assert_eq!(
            svc.registry.bucket_map("docs"),
            Some(BucketMap::balanced(2))
        );

        // Bring up + register the growth build target with the NEW total (as apply requires).
        // Live routing must NOT change: the map still covers 2 shards.
        let ep2 = spawn_stub_node(stub(&log)).await;
        register(&svc, &def_json, &ep2, 3, 2).await;
        assert_eq!(
            svc.registry.bucket_map("docs"),
            Some(BucketMap::balanced(2))
        );

        // Plan derives from the STORED map (2 shards), not the registered count (3): real moves.
        let plan = svc
            .plan_reshard(Request::new(PlanReshardRequest {
                index: "docs".into(),
                new_shard_count: 3,
            }))
            .await
            .unwrap()
            .into_inner();
        assert!(!plan.moved.is_empty(), "growth plan has buckets to move");

        // Apply: build the new shard, cut over, trim the old ones.
        let resp = svc
            .apply_reshard(Request::new(ApplyReshardRequest {
                index: "docs".into(),
                new_shard_count: 3,
            }))
            .await
            .expect("a normally-registered index must be able to grow")
            .into_inner();
        assert_eq!(resp.built_shards, vec![2]);
        assert_eq!(resp.trimmed_shards, vec![0, 1]);
        assert_eq!(svc.registry.bucket_map("docs").unwrap().shards(), 3);

        // The stubs really received the rebuild (shard 2 first) then the trims, each with the
        // full owner map.
        let calls = log.lock().unwrap().clone();
        assert_eq!(
            calls.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
            vec![2, 0, 1]
        );
        assert!(calls
            .iter()
            .all(|(_, n)| *n == growlerdb_core::routing::NUM_BUCKETS as usize));
    }

    /// Coordinated whole-index reindex: every shard runs BUILD for its next generation, then every
    /// shard runs PROMOTE (all builds precede any promote), and the routing generation is bumped
    /// once at cutover.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coordinated_reindex_builds_all_then_promotes_all_and_bumps_generation() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();
        // Both shards record into ONE shared phase log so we can assert the global ordering.
        let phaselog = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mk = || StubNodeAdmin {
            phases: phaselog.clone(),
            ..Default::default()
        };
        let ep0 = spawn_stub_node(mk()).await;
        let ep1 = spawn_stub_node(mk()).await;
        register(&svc, &def_json, &ep0, 2, 0).await;
        register(&svc, &def_json, &ep1, 2, 1).await;
        assert_eq!(svc.registry.generation("docs"), Some(0));

        let resp = svc
            .reindex_index(Request::new(ReindexControlRequest {
                index: "docs".into(),
            }))
            .await
            .expect("coordinated reindex succeeds")
            .into_inner();
        assert_eq!(resp.shards, 2);
        assert_eq!(resp.generation, 1, "routing generation bumped at cutover");
        assert_eq!(
            resp.doc_count, 6,
            "aggregates promote doc_count (3 per shard)"
        );
        assert_eq!(svc.registry.generation("docs"), Some(1));

        // BUILD both shards, THEN PROMOTE both — never a mixed build/promote interleave, no DISCARD.
        let build = ReindexPhase::Build as i32;
        let promote = ReindexPhase::Promote as i32;
        let seq = phaselog.lock().unwrap().clone();
        assert_eq!(
            seq,
            vec![(0, build), (1, build), (0, promote), (1, promote)]
        );
    }

    /// A build-phase failure on any shard aborts the reindex: every already-built shard is DISCARDed
    /// (releasing its held fence), the generation is NOT bumped, and no shard is promoted — so the
    /// old generation is intact everywhere (never a half-swap).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn coordinated_reindex_aborts_and_discards_when_a_build_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();
        let phase0 = Arc::new(std::sync::Mutex::new(Vec::new()));
        // Shard 0 builds fine (and records its phases); shard 1 fails its build.
        let s0 = StubNodeAdmin {
            phases: phase0.clone(),
            ..Default::default()
        };
        let s1 = StubNodeAdmin {
            fail_build: true,
            ..Default::default()
        };
        let ep0 = spawn_stub_node(s0).await;
        let ep1 = spawn_stub_node(s1).await;
        register(&svc, &def_json, &ep0, 2, 0).await;
        register(&svc, &def_json, &ep1, 2, 1).await;

        let err = svc
            .reindex_index(Request::new(ReindexControlRequest {
                index: "docs".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Internal);
        assert_eq!(
            svc.registry.generation("docs"),
            Some(0),
            "no cutover on a build failure"
        );
        // Shard 0 built, then got a DISCARD (fence released); it was never promoted.
        let build = ReindexPhase::Build as i32;
        let discard = ReindexPhase::Discard as i32;
        let phases: Vec<i32> = phase0.lock().unwrap().iter().map(|(_, p)| *p).collect();
        assert_eq!(phases, vec![build, discard]);
    }

    /// A disk-short node fails the reindex **up front** (before any build/job) with one clear error
    /// naming it — the CP pre-run free-disk check.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reindex_refused_up_front_when_a_node_is_disk_short() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();
        let phase0 = Arc::new(std::sync::Mutex::new(Vec::new()));
        // Shard 0 has room (records phases so we can assert it never built); shard 1 is disk-short.
        let s0 = StubNodeAdmin {
            phases: phase0.clone(),
            ..Default::default()
        };
        let s1 = StubNodeAdmin {
            disk_short: true,
            ..Default::default()
        };
        let ep0 = spawn_stub_node(s0).await;
        let ep1 = spawn_stub_node(s1).await;
        register(&svc, &def_json, &ep0, 2, 0).await;
        register(&svc, &def_json, &ep1, 2, 1).await;

        let err = svc
            .reindex_index(Request::new(ReindexControlRequest {
                index: "docs".into(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(
            err.message().contains("free disk") && err.message().contains(&ep1),
            "the error names the disk-short node: {}",
            err.message()
        );
        // Refused before building anything and before any cutover: no phases recorded, no generation
        // bump, and no job left behind.
        assert!(phase0.lock().unwrap().is_empty(), "no shard was built");
        assert_eq!(svc.registry.generation("docs"), Some(0));
        assert!(svc.registry.list_jobs().is_empty(), "no job created");
    }

    /// Poll a reindex job by id until it reaches a terminal state (or time out the test).
    async fn poll_job_to_terminal(svc: &ControlPlaneService, id: &str) -> ReindexJobStatus {
        for _ in 0..200 {
            let j = svc
                .get_reindex_job(Request::new(GetReindexJobRequest {
                    job_id: id.to_string(),
                }))
                .await
                .unwrap()
                .into_inner();
            if matches!(j.state.as_str(), "done" | "failed" | "canceled") {
                return j;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        panic!("job {id} did not reach a terminal state");
    }

    /// The async path: StartReindexJob returns a job id immediately; the background driver builds all
    /// shards, promotes all, and bumps the generation — pollable to `done` with the doc count.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_reindex_job_starts_polls_and_completes() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();
        let ep0 = spawn_stub_node(StubNodeAdmin::default()).await;
        let ep1 = spawn_stub_node(StubNodeAdmin::default()).await;
        register(&svc, &def_json, &ep0, 2, 0).await;
        register(&svc, &def_json, &ep1, 2, 1).await;

        let start = svc
            .start_reindex_job(Request::new(StartReindexJobRequest {
                index: "docs".into(),
            }))
            .await
            .expect("start returns immediately")
            .into_inner();
        assert!(!start.job_id.is_empty());

        // A second start is refused while the first is running or done-recorded is fine; here we just
        // poll the first to completion.
        let done = poll_job_to_terminal(&svc, &start.job_id).await;
        assert_eq!(done.state, "done");
        assert_eq!(done.generation, 1);
        assert_eq!(done.docs_done, 6, "Σ per-shard promoted doc counts");
        assert!(done.shards.iter().all(|s| s.phase == "promoted"));
        assert_eq!(svc.registry.generation("docs"), Some(1));
    }

    /// Canceling an in-flight job discards every staged generation (releasing the fences), never cuts
    /// over (generation unchanged), and ends the job `canceled`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn async_reindex_job_cancels_mid_build_and_discards() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();
        // Shard 0's build blocks on the gate (records its phases) so we can cancel mid-build; shard 1
        // is never reached.
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let phase0 = Arc::new(std::sync::Mutex::new(Vec::new()));
        let s0 = StubNodeAdmin {
            phases: phase0.clone(),
            gate: Some((entered.clone(), release.clone())),
            ..Default::default()
        };
        let ep0 = spawn_stub_node(s0).await;
        let ep1 = spawn_stub_node(StubNodeAdmin::default()).await;
        register(&svc, &def_json, &ep0, 2, 0).await;
        register(&svc, &def_json, &ep1, 2, 1).await;

        let start = svc
            .start_reindex_job(Request::new(StartReindexJobRequest {
                index: "docs".into(),
            }))
            .await
            .unwrap()
            .into_inner();
        // Wait until shard 0's build is in flight, then cancel — the handler pings the node, which
        // releases the gated build with CANCELLED.
        entered.notified().await;
        svc.cancel_reindex_job(Request::new(CancelReindexJobRequest {
            job_id: start.job_id.clone(),
        }))
        .await
        .unwrap();

        let done = poll_job_to_terminal(&svc, &start.job_id).await;
        assert_eq!(done.state, "canceled");
        assert_eq!(
            svc.registry.generation("docs"),
            Some(0),
            "no cutover on cancel"
        );
        // Shard 0 built, then got a DISCARD (staged generation dropped, fence released).
        let build = ReindexPhase::Build as i32;
        let discard = ReindexPhase::Discard as i32;
        let phases: Vec<i32> = phase0.lock().unwrap().iter().map(|(_, p)| *p).collect();
        assert_eq!(phases, vec![build, discard]);
    }

    /// A real bucket move end to end: build on the target, cut the map over, trim the source — and
    /// routing reflects the relocation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn move_bucket_relocates_and_trims() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = service(tmp.path());
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let stub = |log: &Arc<std::sync::Mutex<Vec<(u32, usize)>>>| StubNodeAdmin {
            reindexed: log.clone(),
            gate: None,
            ..Default::default()
        };
        let ep0 = spawn_stub_node(stub(&log)).await;
        let ep1 = spawn_stub_node(stub(&log)).await;
        register(&svc, &def_json, &ep0, 2, 0).await;
        register(&svc, &def_json, &ep1, 2, 1).await;

        let owner0 = svc.registry.bucket_map("docs").unwrap().owner(0);
        let target = 1 - owner0;
        let resp = svc
            .move_bucket(Request::new(MoveBucketRequest {
                index: "docs".into(),
                bucket: 0,
                to_shard: target,
            }))
            .await
            .expect("a staffed bucketed index must support a bucket move")
            .into_inner();
        assert_eq!(
            (resp.bucket, resp.from_shard, resp.to_shard),
            (0, owner0, target)
        );

        // The stored map routes bucket 0 to the target now; every other bucket is untouched.
        let map = svc.registry.bucket_map("docs").unwrap();
        assert_eq!(map.owner(0), target);
        assert_eq!(map.shards(), 2);

        // Build hit the target first, then the source trim — each with the full owner map.
        let calls = log.lock().unwrap().clone();
        assert_eq!(
            calls.iter().map(|(o, _)| *o).collect::<Vec<_>>(),
            vec![target, owner0]
        );
        assert!(calls
            .iter()
            .all(|(_, n)| *n == growlerdb_core::routing::NUM_BUCKETS as usize));
    }

    /// The cutover CAS: a placement op whose map changed under it mid-build is refused loudly
    /// instead of last-write-wins reverting the concurrent op's ownership.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_placement_op_racing_another_cutover_is_refused() {
        let tmp = tempfile::tempdir().unwrap();
        let svc = Arc::new(service(tmp.path()));
        let def_json = serde_json::to_string(&resolved("docs")).unwrap();

        // Both stubs block their reindex until released — the deterministic interleave point
        // (the move's build lands on whichever shard is the target).
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let gated = || StubNodeAdmin {
            reindexed: Arc::default(),
            gate: Some((entered.clone(), release.clone())),
            ..Default::default()
        };
        let ep0 = spawn_stub_node(gated()).await;
        let ep1 = spawn_stub_node(gated()).await;
        register(&svc, &def_json, &ep0, 2, 0).await;
        register(&svc, &def_json, &ep1, 2, 1).await;

        // Start a bucket move onto shard 1; it reads the map, then blocks in the build.
        let mover = {
            let svc = svc.clone();
            let owner0 = svc.registry.bucket_map("docs").unwrap().owner(0);
            tokio::spawn(async move {
                svc.move_bucket(Request::new(MoveBucketRequest {
                    index: "docs".into(),
                    bucket: 0,
                    to_shard: 1 - owner0, // whichever shard doesn't own bucket 0
                }))
                .await
            })
        };
        entered.notified().await;

        // A concurrent placement op commits while the move is mid-build.
        let current = svc.registry.bucket_map("docs").unwrap();
        let other = current.with_owner(5, 1 - current.owner(5)).unwrap();
        svc.registry
            .set_bucket_map("docs", Some(&current), &other)
            .unwrap();

        // The mover finishes its build and hits the CAS: loud FAILED_PRECONDITION, and the
        // concurrent commit survives untouched.
        release.notify_one();
        let err = mover.await.unwrap().unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert_eq!(svc.registry.bucket_map("docs").unwrap(), other);
    }
}
