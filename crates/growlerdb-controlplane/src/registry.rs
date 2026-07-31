//! The index **registry**: the cluster's catalog of index definitions + lifecycle status,
//! durably persisted so create / drop / list survive restarts and a crash never leaves a
//! half-written catalog.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use growlerdb_core::routing::{BucketMap, Reassignment};
use growlerdb_core::ResolvedIndex;
use serde::{Deserialize, Serialize};

use crate::backend::{JsonFileBackend, PersistedState, RegistryBackend, RegistrySnapshot};

/// A fixed dummy argon2 PHC hash that makes [`Registry::verify_credential`] do equivalent work for
/// an unknown subject as for a real one, closing a username-enumeration timing oracle. Computed
/// once; the salt is fixed (it protects nothing — the hash is never authenticated against). A
/// parse/hash failure yields an empty string, which `PasswordHash::new` rejects → the
/// unknown-subject path still returns `false`.
static DUMMY_CREDENTIAL_HASH: std::sync::LazyLock<String> = std::sync::LazyLock::new(|| {
    use argon2::password_hash::{PasswordHasher, SaltString};
    use argon2::Argon2;
    let Ok(salt) = SaltString::encode_b64(b"growlerdb-dummy!") else {
        return String::new();
    };
    // Stringify while `salt` is still alive (the PasswordHash borrows it).
    Argon2::default()
        .hash_password(b"growlerdb-dummy-password", &salt)
        .map(|h| h.to_string())
        .unwrap_or_default()
});

/// Lifecycle status of a registered index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IndexStatus {
    /// Registered, being built / provisioned (shards not yet serving).
    Building,
    /// Built and serving.
    Active,
}

/// A node's stable cluster identity (e.g. a StatefulSet pod hostname). Serializes as a
/// bare string.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(pub String);

impl<S: Into<String>> From<S> for NodeId {
    fn from(s: S) -> Self {
        NodeId(s.into())
    }
}

/// A **placement unit** (D52): the atom the control plane places on a pool node. Either an ordinal
/// **shard** of a hash/partition-sharded index or a **window** of a windowed index — the two live in
/// the same [`IndexEntry`] (`shards` vs `windows`) and go through one placement path
/// ([`Registry::resolve_unit_owner`]). `Copy` so it threads freely through the placement logic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unit {
    /// Ordinal shard of a hash/partition-sharded index (`IndexEntry::shards[ordinal]`).
    Shard(u32),
    /// Time window of a windowed index (`IndexEntry::windows[window]`).
    Window(i64),
}

impl std::fmt::Display for Unit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Unit::Shard(o) => write!(f, "shard {o}"),
            Unit::Window(w) => write!(f, "window {w}"),
        }
    }
}

/// The **R holders** of a placement unit (D53): the sole-writer **primary** plus its read
/// **replicas**, as node endpoints. Returned by
/// [`resolve_unit_holders`](Registry::resolve_unit_holders); the write path targets `primary`, and
/// the gateway scatters reads across `primary` + `replicas`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnitHolders {
    /// The primary node endpoint (accepts writes + reads).
    pub primary: String,
    /// Read-replica node endpoints (never includes the primary).
    pub replicas: Vec<String>,
    /// True iff this call changed the placement (placed, promoted, pruned, trimmed, or topped up a
    /// holder) — i.e. whether anything persisted.
    pub changed: bool,
    /// True iff this call **made or moved the primary assignment** (fresh placement, promotion, or
    /// dead-owner re-placement) — the proto `created` flag. Replica-only churn (prune / top-up /
    /// trim) sets [`changed`](Self::changed) but not this.
    pub moved: bool,
}

/// One window of a node's `RegisterServedIndex` announce, as consumed by
/// [`Registry::announce_windows`]: the window id, its reported event-time zone-map (if any), and
/// whether the node currently serves it cold (read-through).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WindowAnnounce {
    /// Window id (epoch-ms of the window start).
    pub window: i64,
    /// Reported event-time `[min, max]`, or `None` when the window has no bounds yet.
    pub bounds: Option<(i64, i64)>,
    /// True when the node serves this window read-through from object storage (parked).
    pub cold: bool,
}

/// Which nodes serve one shard: the **primary** (accepts writes + reads) and zero or more
/// read **replicas**. The shard map is `shard ordinal → ShardAssignment`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardAssignment {
    /// The node currently serving as primary, if assigned.
    pub primary: Option<NodeId>,
    /// Read replicas (do not include the primary).
    pub replicas: Vec<NodeId>,
}

impl ShardAssignment {
    /// Whether a primary is assigned.
    pub fn is_assigned(&self) -> bool {
        self.primary.is_some()
    }

    /// Every node serving this shard — primary first, then replicas.
    pub fn nodes(&self) -> Vec<&NodeId> {
        self.primary.iter().chain(self.replicas.iter()).collect()
    }
}

/// One time-window shard of a windowed index: its node placement plus the event-time
/// **zone-map** the serving node reports, so the Gateway can prune windows by event time without a
/// fan-out. Used in [`IndexEntry::windows`] (`window-id → WindowAssignment`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowAssignment {
    /// Node placement for this window's shard (primary + replicas).
    #[serde(flatten)]
    pub assignment: ShardAssignment,
    /// Min event-time this window covers (`None` until the node reports it).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_min: Option<i64>,
    /// Max event-time this window covers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_max: Option<i64>,
    /// True when the serving node currently holds this window **cold** (read-through from object
    /// storage, parked). Reported per-heartbeat, so it flips as park/pre-warm swap the tier — the
    /// cluster Gateway reads it for `/v1/cold`. Defaults false (hot).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cold: bool,
}

/// A registered index: its resolved definition, lifecycle status, and **shard map**
/// (primary/replica per shard). (Connector config joins this entry in a later slice.)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexEntry {
    /// The resolved definition (the same shape persisted as a Node's `index.json`).
    pub definition: ResolvedIndex,
    /// Where the index is in its lifecycle.
    pub status: IndexStatus,
    /// `shard ordinal → ShardAssignment`. Empty until shards are assigned. `#[serde(default)]`
    /// so registries without a shard map load cleanly.
    #[serde(default)]
    pub shards: BTreeMap<u32, ShardAssignment>,
    /// `window-id → WindowAssignment` for a **time-windowed** index: which node serves
    /// each window + its event-time zone-map. Empty for ordinal (hash/partition) indexes.
    #[serde(default)]
    pub windows: BTreeMap<i64, WindowAssignment>,
    /// Virtual-bucket map: `bucket_owners[b]` = the shard owning bucket `b`, length
    /// [`NUM_BUCKETS`](growlerdb_core::routing::NUM_BUCKETS). **Empty ⇒ legacy `fnv % shards`
    /// routing**, so registries without this field load as legacy. When present, writers and
    /// readers route `key → bucket → shard` through it, and a reshard
    /// ([`Registry::plan_reshard`]) moves whole buckets.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bucket_owners: Vec<u32>,
}

/// A compact listing row (name + status) for [`Registry::list`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSummary {
    pub name: String,
    pub status: IndexStatus,
}

/// How often a node re-heartbeats into the CP placement pool ([`NODE_HEARTBEAT_TTL_MS`] must be a
/// comfortable multiple of this). The CLI's registration loop derives its re-announce interval from
/// this constant so the two can never silently diverge (HA-D5).
pub const NODE_REANNOUNCE_INTERVAL_MS: i64 = 10_000;

/// How long a node's heartbeat is trusted before it drops out of the CP placement pool.
/// Sized at 3× the re-announce interval ([`NODE_REANNOUNCE_INTERVAL_MS`], 10 s) so a missed or
/// jittered (±20%) heartbeat doesn't eject a healthy node, while a genuinely dead node's units get
/// re-placed within ~30 s (the sweeper runs at TTL/2).
pub const NODE_HEARTBEAT_TTL_MS: i64 = 3 * NODE_REANNOUNCE_INTERVAL_MS;

/// A brief settle window after the placement-grace anchor during which even **initial** placement
/// holds off — much shorter than the full liveness grace ([`NODE_HEARTBEAT_TTL_MS`]). It gives
/// co-booting pool nodes a moment to register so the first primaries round-robin **balanced** across
/// the pool, instead of all landing on whichever node registered first. Never-placed units then
/// place as soon as this clears (a few seconds), rather than waiting a full grace window.
pub const INITIAL_PLACEMENT_SETTLE_MS: i64 = NODE_REANNOUNCE_INTERVAL_MS / 2;

/// A persisted API token: long-lived programmatic credential. Only the secret's hash is stored —
/// the raw secret is shown once at creation and never persisted.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiToken {
    /// Server-assigned id (the handle for revoke).
    pub id: String,
    /// Human label.
    pub label: String,
    /// Display prefix of the secret (e.g. `gdb_live_a1b2`) — safe to show; not the secret.
    pub prefix: String,
    /// SHA-256 (base64url) of the secret. Looked up at authentication; never returned.
    pub hash: String,
    /// Roles the token authenticates with.
    pub roles: Vec<String>,
    /// The subject that created it.
    pub owner: String,
    /// Creation time (epoch ms).
    #[serde(default)]
    pub created_at_ms: i64,
    /// Optional expiry (epoch ms). `None` = never expires (via serde default). Past this instant the
    /// token no longer authenticates and is pruned on the next `create_token`, so the token map
    /// can't grow without bound.
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
}

impl ApiToken {
    /// Whether this token is past its expiry at `now_ms`. A token with no expiry is never expired.
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms.is_some_and(|exp| now_ms >= exp)
    }
}

/// A persisted saved search: the full query state the console can restore, scoped to its `owner`
/// (the verified subject). `state` is an opaque JSON blob the UI round-trips.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavedQuery {
    /// Server-assigned stable id (the handle for update/delete).
    pub id: String,
    /// Human label.
    pub name: String,
    /// The verified subject that owns it.
    pub owner: String,
    /// The raw query string (display + a fallback when `state` is empty).
    pub query: String,
    /// Opaque JSON: the full search state to restore (index, filters, time range, sort, syntax).
    #[serde(default)]
    pub state: String,
    /// Workspace-visible (read-only to non-owners) when true.
    #[serde(default)]
    pub shared: bool,
    /// Server-set creation time (epoch ms).
    #[serde(default)]
    pub created_at_ms: i64,
}

/// Errors from registry operations.
#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    /// `create` of a name that is already registered.
    #[error("index `{0}` already exists")]
    AlreadyExists(String),
    /// `create` with a definition the registry refuses (e.g. an index name unusable as a
    /// filesystem path component).
    #[error("invalid definition: {0}")]
    InvalidDefinition(String),
    /// A placement compare-and-set lost: a [`set_bucket_map`](Registry::set_bucket_map) whose
    /// expected prior map no longer matches (another reshard committed in between), or a
    /// [`RegisterServedIndex` announce](Registry::announce_primaries) claiming a unit whose primary is
    /// a different, not-confidently-dead node (first-wins). Maps to gRPC `FAILED_PRECONDITION`.
    #[error("placement conflict: {0}")]
    PlacementConflict(String),
    /// An operation named an index that is not registered.
    #[error("index `{0}` not found")]
    NotFound(String),
    /// `promote_replica` for a shard with no replica to promote.
    #[error("shard {shard} of `{index}` has no replica to promote")]
    NoReplica { index: String, shard: u32 },
    /// `promote_replica` while a primary is still assigned — the caller must fence/clear the old
    /// primary first, or two nodes serve as primary for one shard (split brain).
    #[error(
        "shard {shard} of `{index}` still has primary `{primary}`; fence/clear it before promoting"
    )]
    PrimaryStillAssigned {
        index: String,
        shard: u32,
        primary: String,
    },
    /// [`resolve_unit_owner`](Registry::resolve_unit_owner) when no node has heartbeated within the
    /// TTL — the pool is empty, so there is nowhere to place the unit. The caller retries once a node
    /// registers.
    #[error("no live node to place {unit} of `{index}` (none heartbeated within the TTL)")]
    NoLiveNode { index: String, unit: String },
    /// Placing a **new** unit would grow the deployment's entitlement usage past its cap (D38: the
    /// metric is distinct live nodes holding a primary of any index — see
    /// [`count_entitlement_nodes`](Registry::count_entitlement_nodes)). Existing units are never
    /// disrupted: re-resolves and dead-owner re-placement always pass. Maps to gRPC
    /// `RESOURCE_EXHAUSTED`.
    #[error(
        "scale limit reached: {nodes} primary-serving nodes in use, entitlement is {entitled}"
    )]
    EntitlementExceeded { nodes: usize, entitled: usize },
    /// Another process holds the registry's exclusive lock — single-writer is enforced, so a
    /// second control plane fails fast rather than last-writer-wins clobbering.
    #[error("registry `{0}` is locked by another process")]
    Locked(PathBuf),
    /// Reading or writing the persisted registry failed.
    #[error("registry io: {0}")]
    Io(#[from] std::io::Error),
    /// An externalized registry backend (e.g. Postgres, D51) failed — connect, schema, query, or the
    /// single-writer lock. Carries the store's error text.
    #[error("registry backend: {0}")]
    Backend(String),
    /// A write reached a control plane that is not (or no longer) the registry leader — a standby
    /// got a mis-routed write, or the store's version check showed another writer took over.
    /// Retryable once the caller re-resolves the leader; maps to gRPC `FAILED_PRECONDITION`
    /// (never `Internal` — the store is fine, this replica just may not write).
    #[error("not the registry leader: {0}")]
    NotLeader(String),
    /// Encoding/decoding the persisted registry failed.
    #[error("registry codec: {0}")]
    Codec(#[from] serde_json::Error),
    /// `set_alias` used a name that's already a registered index — an alias and an index can't share
    /// a name, or routing would be ambiguous.
    #[error("alias `{0}` clashes with an existing index name")]
    AliasNameClash(String),
    /// An operation named an alias that doesn't exist.
    #[error("alias `{0}` not found")]
    AliasNotFound(String),
    /// An update/delete named a saved query that doesn't exist (or isn't the caller's).
    #[error("saved query `{0}` not found")]
    SavedQueryNotFound(String),
    /// A stored bucket map failed validation — wrong length or a gap. Indicates a corrupt/hand-edited
    /// registry, since maps are only ever written through validated paths.
    #[error("invalid bucket map: {0}")]
    InvalidBucketMap(String),
    /// A built-in credential could not be hashed — an argon2 failure, not a wrong password.
    #[error("credential hashing failed: {0}")]
    Credential(String),
}

/// One entry in a per-index **activity log**: a timestamped lifecycle event. Stored append-only
/// and bounded; the `kind` is a stable machine tag, the `message` human-readable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityEvent {
    /// Event time (epoch ms).
    pub ts_ms: i64,
    /// Stable event tag, e.g. `index.created`, `alias.swapped`, `reshard.applied`.
    pub kind: String,
    /// Human-readable description.
    pub message: String,
}

/// Max events retained per index in the activity log — oldest are dropped.
const ACTIVITY_RETAIN: usize = 200;

/// Debounce window for activity-sidecar flushes. An isolated event flushes immediately (synchronous
/// durability for the common case); a burst coalesces into a single off-lock write instead of one
/// fsync per event. The tail flushes on graceful shutdown ([`Registry`]'s `Drop`); a hard crash
/// within the window may lose the last few events — acceptable for a non-critical audit log.
const ACTIVITY_FLUSH_DEBOUNCE_MS: i64 = 1000;

/// Coalescing state for the debounced activity-sidecar flush, behind its own mutex so a flush never
/// holds the activity data lock across the fsync.
#[derive(Default)]
struct ActivityFlush {
    /// Epoch ms of the last completed sidecar write (`0` = never written this session).
    last_flush_ms: i64,
    /// In-memory events exist that a debounce window skipped writing — flush them on shutdown.
    dirty: bool,
}

/// Registry result alias.
pub type Result<T> = std::result::Result<T, RegistryError>;

/// The index **registry**: `name → `[`IndexEntry`]. Reads are served from memory; every mutation
/// persists durably through a [`RegistryBackend`]. Cheap to share across threads — internally
/// `RwLock`-guarded.
///
/// **Persistence backend:** the default [`open`](Self::open) uses the local single-writer
/// [`JsonFileBackend`]; a replicated backend (D51) swaps in via
/// [`with_backend`](Self::with_backend) without changing any logic below.
///
/// **Lock-order invariant:** each data map has its own `RwLock`. A mutation holds
/// **only the one map it changes**, drops it, then calls [`persist_snapshot`](Self::persist_snapshot)
/// (which re-reads every map off-lock). The only places that hold two+ data locks at once do so in
/// this fixed order — `indexes → aliases → saved_queries → role_bindings → tokens → credentials`:
/// [`persist_snapshot`](Self::persist_snapshot) (all read locks, for the snapshot),
/// [`drop_index`](Self::drop_index) and [`set_alias`](Self::set_alias) (`indexes` before `aliases`).
/// The derived `token_by_hash` index and the `activity`/`session_epochs` sidecars are independent,
/// always taken one-at-a-time. Keep new lock acquisitions on this order — never the reverse.
pub struct Registry {
    /// Where durable state lives. Every persist path goes through here, off any data lock.
    backend: Box<dyn RegistryBackend>,
    indexes: RwLock<BTreeMap<String, IndexEntry>>,
    /// Index aliases: `alias → member index names`. A separate lock from `indexes`; every code path
    /// acquires **`indexes` before `aliases`** to avoid deadlock.
    aliases: RwLock<BTreeMap<String, BTreeSet<String>>>,
    /// Saved searches: `id → `[`SavedQuery`]. Lock order is **indexes → aliases → saved_queries**
    /// everywhere, to avoid deadlock.
    saved_queries: RwLock<BTreeMap<String, SavedQuery>>,
    /// Monotonic suffix for generated saved-query ids; combined with a millisecond timestamp it is
    /// unique across restarts too.
    next_saved: std::sync::atomic::AtomicU64,
    /// Local role bindings: `subject → roles`. Lock order is **indexes → aliases → saved_queries →
    /// role_bindings → tokens**.
    role_bindings: RwLock<BTreeMap<String, Vec<String>>>,
    /// API tokens: `id → `[`ApiToken`].
    tokens: RwLock<BTreeMap<String, ApiToken>>,
    /// Secret-hash → token-id lookup: makes `find_token` (every authenticated request) O(1) instead
    /// of a linear scan. **Derived** from `tokens` (not persisted); rebuilt on open and after every
    /// token mutation. Never nested under the `tokens` lock, so no deadlock.
    token_by_hash: RwLock<std::collections::HashMap<String, String>>,
    /// Monotonic suffix for generated token ids.
    next_token: std::sync::atomic::AtomicU64,
    /// Built-in local credentials: `subject → argon2 PHC hash`. Lock order is **last**, after
    /// `tokens` (indexes → aliases → saved_queries → role_bindings → tokens → credentials).
    credentials: RwLock<BTreeMap<String, String>>,
    /// Per-subject index allowlist for built-in login: `subject → allowed index names`. Threaded
    /// into the session JWT's `indexes` claim so per-index RBAC restricts the subject. Lock order is
    /// **after `credentials`**.
    index_bindings: RwLock<BTreeMap<String, Vec<String>>>,
    /// Per-index activity log: `index → events`, bounded + append-only. Persisted to a separate
    /// `activity.json` (non-critical) so the registry's atomic envelope stays small. Lock is
    /// independent (acquired last).
    activity: RwLock<BTreeMap<String, Vec<ActivityEvent>>>,
    /// Debounce/coalescing state for the activity-sidecar flush. Its own mutex also serializes
    /// concurrent flushes (last snapshot wins the file) — like `flush_lock` for the main registry —
    /// and is never nested under the activity data lock.
    activity_flush: std::sync::Mutex<ActivityFlush>,
    /// Per-subject **session epoch** (epoch ms): sessions issued before this instant are stale.
    /// Bumped when a subject's roles change or credential is removed, giving revocation / immediate
    /// role-downgrade for outstanding session JWTs. Persisted to a separate `sessions.json` sidecar.
    session_epochs: RwLock<BTreeMap<String, i64>>,
    /// Serializes session-epoch sidecar writes so two concurrent revokes can't persist out of order
    /// and lose one bump. Taken **after** the `session_epochs` guard is released, off the auth hot path.
    sessions_flush: std::sync::Mutex<()>,
    /// Serializes registry-file writes, taken off-lock after a mutation releases its data lock — so
    /// routing reads never block on the fsync, and two concurrent persists can't lose a change (each
    /// snapshots the latest memory; last write wins with the full state).
    flush_lock: std::sync::Mutex<()>,
    /// Set when a failed persist's rollback could not restore memory from the store either: memory may
    /// still hold an unpersisted change, so every further persist is refused until a
    /// [`reload`](Self::reload) succeeds — the failed change must never ride out on the next snapshot.
    rollback_failed: std::sync::atomic::AtomicBool,
    /// The **placement pool** (D52): `node endpoint → last-heartbeat epoch-ms`. One flat pool of
    /// interchangeable shard hosts, **not** keyed by index — any live node can be assigned units from
    /// any index, which lets one node process serve many indexes. **In-memory only** — liveness is
    /// ephemeral runtime state, re-registered within a heartbeat interval after a restart; unit
    /// *assignments* stay durable in [`IndexEntry`].
    node_pool: RwLock<BTreeMap<String, i64>>,
    /// The endpoints whose **latest heartbeat declared replica capability** (HA-G2): an object store
    /// is configured, so they can open replica windows read-through (D53). Replica top-up places ONLY
    /// on these, so a node that never declares never receives replica units it could not serve.
    /// In-memory; refreshed every heartbeat, so a node that loses its object store stops attracting
    /// new replicas within one re-announce.
    replica_capable: RwLock<BTreeSet<String>>,
    /// The endpoints admitted as **placement-eligible pool nodes** (D52): those that registered via
    /// `RegisterNode`. A classic fixed-endpoint node (seen only through `RegisterServedIndex`) is
    /// deliberately ABSENT — its liveness is kept in [`node_pool`](Self::node_pool) so the sweeper
    /// leaves its self-declared units alone, but the CP never *places* a pool unit onto it. Placement
    /// draws targets from [`placement_nodes`](Self::placement_nodes) = live ∩ this set.
    pool_eligible: RwLock<BTreeSet<String>>,
    /// **Placement-change hook** (HA-D1): invoked after any successful persist/reload whose placement
    /// fingerprint differs from the last — the single choke point that pushes fresh assignment
    /// snapshots to subscribed nodes. At the persist boundary (not per mutation) so a new placement
    /// mutation cannot forget to notify. Invoked with **no** data lock held.
    placement_listener: RwLock<Option<Box<dyn Fn() + Send + Sync>>>,
    /// Fingerprint of the placement state at the last listener check, so non-placement persists
    /// (tokens, aliases, …) don't fire spurious pushes.
    last_placement_hash: std::sync::Mutex<u64>,
    /// The **liveness grace anchor** (HA-D5): epoch-ms of the first heartbeat observed after
    /// boot/promotion, `-1` = none yet. For one [`NODE_HEARTBEAT_TTL_MS`] after it, owner liveness is
    /// *unknown* (laggard nodes haven't re-registered with this possibly-restarted control plane yet),
    /// so dead-owner actions are suppressed and entitlement counting fails closed. Re-armed to `-1` on
    /// leadership promotion.
    grace_anchor_ms: std::sync::atomic::AtomicI64,
}

impl Registry {
    /// Open the registry at `path` over the default local [`JsonFileBackend`]: load the existing
    /// catalog if present, taking the exclusive single-writer lock first (fails fast with
    /// [`Locked`](RegistryError::Locked) if another process holds it).
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        Self::with_backend(Box::new(JsonFileBackend::open(path)?))
    }

    /// Build a registry over an arbitrary persistence [`backend`](RegistryBackend) — the seam that
    /// lets the control plane run on the local JSON store or a replicated external store (D51) with
    /// identical in-memory logic. Loads persisted state into memory; the derived `token_by_hash` index
    /// is rebuilt here.
    pub fn with_backend(backend: Box<dyn RegistryBackend>) -> Result<Self> {
        let PersistedState {
            indexes,
            aliases,
            saved_queries,
            role_bindings,
            tokens,
            credentials,
            index_bindings,
            activity,
            session_epochs,
        } = backend.load()?;
        // Build the hash→id lookup from the loaded tokens (derived, not persisted).
        let token_by_hash: std::collections::HashMap<String, String> = tokens
            .iter()
            .map(|(id, t)| (t.hash.clone(), id.clone()))
            .collect();
        let reg = Self {
            backend,
            indexes: RwLock::new(indexes),
            aliases: RwLock::new(aliases),
            saved_queries: RwLock::new(saved_queries),
            next_saved: std::sync::atomic::AtomicU64::new(0),
            role_bindings: RwLock::new(role_bindings),
            tokens: RwLock::new(tokens),
            token_by_hash: RwLock::new(token_by_hash),
            next_token: std::sync::atomic::AtomicU64::new(0),
            credentials: RwLock::new(credentials),
            index_bindings: RwLock::new(index_bindings),
            activity: RwLock::new(activity),
            activity_flush: std::sync::Mutex::new(ActivityFlush::default()),
            session_epochs: RwLock::new(session_epochs),
            sessions_flush: std::sync::Mutex::new(()),
            flush_lock: std::sync::Mutex::new(()),
            rollback_failed: std::sync::atomic::AtomicBool::new(false),
            node_pool: RwLock::new(BTreeMap::new()),
            replica_capable: RwLock::new(BTreeSet::new()),
            pool_eligible: RwLock::new(BTreeSet::new()),
            placement_listener: RwLock::new(None),
            last_placement_hash: std::sync::Mutex::new(0),
            grace_anchor_ms: std::sync::atomic::AtomicI64::new(-1),
        };
        // Seed the placement fingerprint from the loaded state so the first mutation only notifies
        // if it actually changed placement (there's no listener yet at construction anyway).
        *reg.last_placement_hash.lock().unwrap() = placement_hash(&reg.read_map());
        Ok(reg)
    }

    /// Read the catalog under the lock, recovering from poisoning: a panic elsewhere while holding
    /// the lock must not take down every subsequent create/drop/list/route call.
    fn read_map(&self) -> RwLockReadGuard<'_, BTreeMap<String, IndexEntry>> {
        self.indexes.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Write the catalog under the lock, recovering from poisoning (see [`read_map`](Self::read_map)).
    fn write_map(&self) -> RwLockWriteGuard<'_, BTreeMap<String, IndexEntry>> {
        self.indexes.write().unwrap_or_else(|e| e.into_inner())
    }

    fn read_aliases(&self) -> RwLockReadGuard<'_, BTreeMap<String, BTreeSet<String>>> {
        self.aliases.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_aliases(&self) -> RwLockWriteGuard<'_, BTreeMap<String, BTreeSet<String>>> {
        self.aliases.write().unwrap_or_else(|e| e.into_inner())
    }

    fn read_saved(&self) -> RwLockReadGuard<'_, BTreeMap<String, SavedQuery>> {
        self.saved_queries.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_saved(&self) -> RwLockWriteGuard<'_, BTreeMap<String, SavedQuery>> {
        self.saved_queries
            .write()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn read_bindings(&self) -> RwLockReadGuard<'_, BTreeMap<String, Vec<String>>> {
        self.role_bindings.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_bindings(&self) -> RwLockWriteGuard<'_, BTreeMap<String, Vec<String>>> {
        self.role_bindings
            .write()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn read_tokens(&self) -> RwLockReadGuard<'_, BTreeMap<String, ApiToken>> {
        self.tokens.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_tokens(&self) -> RwLockWriteGuard<'_, BTreeMap<String, ApiToken>> {
        self.tokens.write().unwrap_or_else(|e| e.into_inner())
    }

    fn read_credentials(&self) -> RwLockReadGuard<'_, BTreeMap<String, String>> {
        self.credentials.read().unwrap_or_else(|e| e.into_inner())
    }

    fn write_credentials(&self) -> RwLockWriteGuard<'_, BTreeMap<String, String>> {
        self.credentials.write().unwrap_or_else(|e| e.into_inner())
    }

    fn read_index_bindings(&self) -> RwLockReadGuard<'_, BTreeMap<String, Vec<String>>> {
        self.index_bindings
            .read()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn write_index_bindings(&self) -> RwLockWriteGuard<'_, BTreeMap<String, Vec<String>>> {
        self.index_bindings
            .write()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Snapshot every core map under brief read locks and write the registry file **off any data
    /// lock**, so routing reads never block on the fsync and mutations aren't serialized behind disk
    /// I/O. `flush_lock` serializes the writes so a concurrent pair can't lose a change: each snapshot
    /// reads the latest memory, last write wins with the full state. Must be called with **no**
    /// registry data lock held (it re-acquires them briefly).
    ///
    /// **Rollback on persist failure.** A failed persist must not leave the mutation applied in
    /// memory, or the *next* successful mutation's full snapshot silently commits it. Rollback here
    /// restores the core maps from the store's durable state (the pre-mutation state, since persists
    /// serialize under `flush_lock`) — one undo path since every mutation funnels through this point.
    /// If the restore itself fails, `rollback_failed` latches and every further persist is refused
    /// until a restore succeeds — stale memory can never reach the store. On the Postgres backend a
    /// persist failure also demotes the writer, so a mutation that raced past the rollback is refused
    /// (`NotLeader`) rather than silently re-committing it.
    fn persist_snapshot(&self) -> Result<()> {
        let _flush = self.flush_lock.lock().unwrap_or_else(|e| e.into_inner());
        if self
            .rollback_failed
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            // A previous failed persist is still un-rolled-back in memory. Restore first; that also
            // sweeps away the *current* mutation's change, so this call must fail (retryable) rather
            // than report success for a change memory no longer holds.
            self.restore_core()?;
            self.rollback_failed
                .store(false, std::sync::atomic::Ordering::SeqCst);
            self.notify_if_placement_changed();
            return Err(RegistryError::Backend(
                "registry memory was rolled back after an earlier persist failure; retry".into(),
            ));
        }
        // Clone each map under a brief read lock; the guards are temporaries released at the end of
        // this statement, before any I/O the backend does.
        let snapshot = RegistrySnapshot {
            indexes: self.read_map().clone(),
            aliases: self.read_aliases().clone(),
            saved_queries: self.read_saved().clone(),
            role_bindings: self.read_bindings().clone(),
            tokens: self.read_tokens().clone(),
            credentials: self.read_credentials().clone(),
            index_bindings: self.read_index_bindings().clone(),
        };
        match self.backend.persist_registry(snapshot) {
            Ok(()) => {
                // The single placement-notification choke point (HA-D1): every placement mutation
                // funnels through here, so none can forget to push; non-placement persists are
                // filtered by the fingerprint. Fired off every data lock (only `flush_lock` is held,
                // which the listener never takes).
                self.notify_if_placement_changed();
                Ok(())
            }
            Err(e) => {
                if self.restore_core().is_err() {
                    self.rollback_failed
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                } else {
                    // The rollback may have undone a placement change some node already heard about
                    // through an earlier push; re-notify so subscribers converge on the truth.
                    self.notify_if_placement_changed();
                }
                Err(e)
            }
        }
    }

    /// Install the **placement-change listener** (HA-D1): called (with no data lock held) whenever a
    /// successful persist or [`reload`](Self::reload) changed which nodes hold which units. The
    /// control-plane service points this at its assignment hub so every placement mutation — resolve,
    /// announce re-point, drop-index, promote, remove-node, sweeper move — pushes fresh snapshots.
    pub fn set_placement_listener(&self, listener: impl Fn() + Send + Sync + 'static) {
        *self
            .placement_listener
            .write()
            .unwrap_or_else(|e| e.into_inner()) = Some(Box::new(listener));
    }

    /// Compare the current placement fingerprint against the last notified one; on change, store it
    /// and invoke the listener. Must be called with no data lock held (takes a brief `indexes` read).
    fn notify_if_placement_changed(&self) {
        let hash = placement_hash(&self.read_map());
        {
            let mut last = self
                .last_placement_hash
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if *last == hash {
                return;
            }
            *last = hash;
        }
        let listener = self
            .placement_listener
            .read()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(f) = listener.as_ref() {
            f();
        }
    }

    /// Restore the seven core catalog maps (and the derived token index) from the backend's durable
    /// state — the rollback path for a failed persist. The sidecars (activity, session epochs) are
    /// left alone: they persist through their own paths with their own rollback.
    fn restore_core(&self) -> Result<()> {
        let s = self.backend.load()?;
        *self.write_map() = s.indexes;
        *self.write_aliases() = s.aliases;
        *self.write_saved() = s.saved_queries;
        *self.write_bindings() = s.role_bindings;
        *self.write_tokens() = s.tokens;
        *self.write_credentials() = s.credentials;
        *self.write_index_bindings() = s.index_bindings;
        self.rebuild_token_index();
        Ok(())
    }

    /// Reload the whole in-memory catalog from the backend — a **standby** control plane calls this
    /// when the store's [`backend_version`](Self::backend_version) advances (the leader wrote), so it
    /// stays warm for a fast failover. Each map is replaced under its own write lock (never two at
    /// once). The process-local **node heartbeats are left untouched** — they are this replica's own
    /// liveness view, and nodes re-register with a new leader within a heartbeat after failover
    /// ([D33](/system/decisions/d33-windowed-topology.md)).
    pub fn reload(&self) -> Result<()> {
        let s = self.backend.load()?;
        *self.write_map() = s.indexes;
        *self.write_aliases() = s.aliases;
        *self.write_saved() = s.saved_queries;
        *self.write_bindings() = s.role_bindings;
        *self.write_tokens() = s.tokens;
        *self.write_credentials() = s.credentials;
        *self.write_index_bindings() = s.index_bindings;
        *self.activity.write().unwrap_or_else(|e| e.into_inner()) = s.activity;
        *self
            .session_epochs
            .write()
            .unwrap_or_else(|e| e.into_inner()) = s.session_epochs;
        self.rebuild_token_index();
        // Memory now mirrors the store exactly — any change a failed persist's rollback couldn't
        // undo is gone, so persists are safe again.
        self.rollback_failed
            .store(false, std::sync::atomic::Ordering::SeqCst);
        // A reload can change placement too (the leader wrote, or this standby just promoted over a
        // dead leader's writes) — push subscribers the fresh truth.
        self.notify_if_placement_changed();
        Ok(())
    }

    /// The backend's monotonic registry version, if it supports change-polling — a standby's reload
    /// signal. `None` for the local JSON file (single-writer, no standbys).
    pub fn backend_version(&self) -> Result<Option<i64>> {
        self.backend.poll_version()
    }

    /// Attempt to acquire write **leadership** over the backend — a standby promoting itself when
    /// the previous leader dies. `Ok(true)` if this control plane is now the leader.
    ///
    /// Ordering is load-bearing: acquire the writer lock → [`reload`](Self::reload) → only then
    /// confirm writership. Writes are gated on the confirmed flag, so a promoted leader can never
    /// overwrite the dead leader's last writes with its stale pre-promotion catalog. If the reload
    /// fails, the lock is resigned and this replica stays a standby (retry next tick).
    pub fn try_become_leader(&self) -> Result<bool> {
        if self.backend.is_leader() {
            return Ok(true);
        }
        if !self.backend.try_become_leader()? {
            return Ok(false);
        }
        match self.reload() {
            Ok(()) => {
                self.backend.confirm_leadership();
                // Re-arm the liveness grace (HA-D5): this replica's heartbeat view starts empty, so
                // dead-owner actions stay suppressed for one TTL — early resolves can't mass-re-place
                // laggards' units onto the first re-registrant.
                self.grace_anchor_ms
                    .store(-1, std::sync::atomic::Ordering::SeqCst);
                Ok(true)
            }
            Err(e) => {
                self.backend.resign_leadership();
                Err(e)
            }
        }
    }

    /// Whether leadership is **still** held — the store session owning the writer lock is alive. A
    /// leader's run loop calls this every tick; `Ok(false)` (or `Err`) means demoted: stop serving
    /// writes, withdraw readiness, and fall back to the standby path.
    pub fn verify_leadership(&self) -> Result<bool> {
        self.backend.verify_leadership()
    }

    /// Give up write leadership (releasing the store's writer lock if possible) — the demotion path
    /// after [`verify_leadership`](Self::verify_leadership) fails. No-op on the local JSON backend.
    pub fn resign_leadership(&self) {
        self.backend.resign_leadership()
    }

    /// Whether this control plane currently holds write leadership (always `true` on the local JSON
    /// backend). A non-leader's mutations are refused at the persist boundary.
    pub fn is_leader(&self) -> bool {
        self.backend.is_leader()
    }

    /// Register a new index (status [`Building`](IndexStatus::Building)). Errors if the name
    /// is already taken.
    pub fn create(&self, definition: ResolvedIndex) -> Result<()> {
        let name = definition.name.clone();
        // Re-check the name: the registry also accepts pre-resolved definitions that bypass
        // `from_yaml`'s validation, and the name becomes a node-side shard directory + object prefix.
        growlerdb_core::validate_index_name(&name)
            .map_err(|e| RegistryError::InvalidDefinition(e.to_string()))?;
        let mut map = self.write_map();
        if map.contains_key(&name) {
            return Err(RegistryError::AlreadyExists(name));
        }
        map.insert(
            name,
            IndexEntry {
                definition,
                status: IndexStatus::Building,
                shards: BTreeMap::new(),
                windows: BTreeMap::new(),
                bucket_owners: Vec::new(),
            },
        );
        drop(map); // release the data lock before the fsync
        self.persist_snapshot()
    }

    /// Mark an index [`Active`](IndexStatus::Active) (provisioning complete). Errors if absent.
    /// Already-active is a no-op without a persist — nodes re-announce (and re-activate) every
    /// heartbeat, and an idempotent re-announce must not rewrite the registry.
    pub fn activate(&self, name: &str) -> Result<()> {
        let mut map = self.write_map();
        let entry = map
            .get_mut(name)
            .ok_or_else(|| RegistryError::NotFound(name.to_string()))?;
        if entry.status == IndexStatus::Active {
            return Ok(());
        }
        entry.status = IndexStatus::Active;
        drop(map);
        self.persist_snapshot()
    }

    /// Remove an index, returning its definition. Errors if absent. Also prunes the index from any
    /// aliases that point at it (dropping aliases left empty), so an alias never dangles.
    pub fn drop_index(&self, name: &str) -> Result<ResolvedIndex> {
        let mut map = self.write_map();
        let entry = map
            .remove(name)
            .ok_or_else(|| RegistryError::NotFound(name.to_string()))?;
        // indexes-write already held → acquire aliases-write (indexes-before-aliases order).
        let mut aliases = self.write_aliases();
        for targets in aliases.values_mut() {
            targets.remove(name);
        }
        aliases.retain(|_, targets| !targets.is_empty());
        drop(aliases);
        drop(map);
        self.persist_snapshot()?;
        Ok(entry.definition)
    }

    /// Point an `alias` at `targets`, replacing any existing target set. The atomic reindex-and-swap
    /// is just re-pointing an alias here — one durable write. Errors if the alias name collides with
    /// an index, or any target index isn't registered.
    pub fn set_alias(
        &self,
        alias: &str,
        targets: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<()> {
        let targets: BTreeSet<String> = targets.into_iter().map(Into::into).collect();
        let indexes = self.read_map(); // indexes-before-aliases
        if indexes.contains_key(alias) {
            return Err(RegistryError::AliasNameClash(alias.to_string()));
        }
        for t in &targets {
            if !indexes.contains_key(t) {
                return Err(RegistryError::NotFound(t.clone()));
            }
        }
        let mut aliases = self.write_aliases();
        aliases.insert(alias.to_string(), targets);
        drop(aliases);
        drop(indexes);
        self.persist_snapshot()
    }

    /// Remove an alias. Errors if it doesn't exist.
    pub fn drop_alias(&self, alias: &str) -> Result<()> {
        {
            let mut aliases = self.write_aliases();
            if aliases.remove(alias).is_none() {
                return Err(RegistryError::AliasNotFound(alias.to_string()));
            }
        } // hold only the map we mutate; persist_snapshot re-reads everything off-lock
        self.persist_snapshot()
    }

    /// The member indexes an `alias` points at, if it is an alias.
    pub fn alias_targets(&self, alias: &str) -> Option<Vec<String>> {
        self.read_aliases()
            .get(alias)
            .map(|s| s.iter().cloned().collect())
    }

    /// All aliases as `alias → sorted member names`.
    pub fn list_aliases(&self) -> BTreeMap<String, Vec<String>> {
        self.read_aliases()
            .iter()
            .map(|(a, t)| (a.clone(), t.iter().cloned().collect()))
            .collect()
    }

    /// Saved searches visible to `owner`: the owner's own rows plus any `shared` ones, newest first.
    /// An empty `owner` (anonymous/open gateway) sees only shared rows.
    pub fn list_saved_queries(&self, owner: &str) -> Vec<SavedQuery> {
        let mut out: Vec<SavedQuery> = self
            .read_saved()
            .values()
            .filter(|q| q.owner == owner || q.shared)
            .cloned()
            .collect();
        out.sort_by_key(|q| std::cmp::Reverse(q.created_at_ms));
        out
    }

    /// Create (empty `id`) or update (existing own `id`) a saved query for `owner`. The server
    /// stamps `id`/`owner`/`created_at_ms` on create; an update of another subject's row (or
    /// a missing id) is [`SavedQueryNotFound`](RegistryError::SavedQueryNotFound). Returns the row.
    pub fn save_saved_query(&self, mut q: SavedQuery, owner: &str) -> Result<SavedQuery> {
        let indexes = self.read_map();
        let aliases = self.read_aliases();
        let mut saved = self.write_saved();
        if q.id.is_empty() {
            let id = format!(
                "sq-{}-{}",
                now_ms(),
                self.next_saved
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            );
            q.id = id.clone();
            q.owner = owner.to_string();
            q.created_at_ms = now_ms();
            saved.insert(id, q.clone());
        } else {
            match saved.get(&q.id) {
                Some(existing) if existing.owner == owner => {
                    // Preserve immutable server fields; the caller can change name/query/state/shared.
                    q.owner = owner.to_string();
                    q.created_at_ms = existing.created_at_ms;
                    saved.insert(q.id.clone(), q.clone());
                }
                _ => return Err(RegistryError::SavedQueryNotFound(q.id.clone())),
            }
        }
        drop(saved);
        drop(aliases);
        drop(indexes);
        self.persist_snapshot()?;
        Ok(q)
    }

    /// Delete `owner`'s saved query `id`. Deleting a non-existent or non-owned row is
    /// [`SavedQueryNotFound`](RegistryError::SavedQueryNotFound).
    pub fn delete_saved_query(&self, id: &str, owner: &str) -> Result<()> {
        let indexes = self.read_map();
        let aliases = self.read_aliases();
        let mut saved = self.write_saved();
        match saved.get(id) {
            Some(q) if q.owner == owner => {
                saved.remove(id);
            }
            _ => return Err(RegistryError::SavedQueryNotFound(id.to_string())),
        }
        drop(saved);
        drop(aliases);
        drop(indexes);
        self.persist_snapshot()?;
        Ok(())
    }

    /// All local role bindings as `subject → roles`, sorted by subject.
    pub fn list_role_bindings(&self) -> BTreeMap<String, Vec<String>> {
        self.read_bindings().clone()
    }

    /// The locally-bound roles for `subject` — merged into the caller's token roles when the control
    /// plane authorizes. Empty for an unknown/empty subject.
    pub fn roles_for(&self, subject: &str) -> Vec<String> {
        if subject.is_empty() {
            return Vec::new();
        }
        self.read_bindings()
            .get(subject)
            .cloned()
            .unwrap_or_default()
    }

    /// Set (replace) `subject`'s local roles. Empty `roles` removes the binding. Roles are
    /// de-duplicated and order-stable; an empty `subject` is rejected.
    pub fn set_user_roles(&self, subject: &str, roles: Vec<String>) -> Result<()> {
        if subject.trim().is_empty() {
            return Err(RegistryError::SavedQueryNotFound("(empty subject)".into()));
        }
        let mut deduped: Vec<String> = Vec::new();
        for r in roles {
            let r = r.trim().to_string();
            if !r.is_empty() && !deduped.contains(&r) {
                deduped.push(r);
            }
        }
        {
            // Hold only `role_bindings` — persist_snapshot re-reads every map off-lock.
            let mut bindings = self.write_bindings();
            if deduped.is_empty() {
                bindings.remove(subject);
            } else {
                bindings.insert(subject.to_string(), deduped);
            }
        }
        self.persist_snapshot()?;
        // A role change takes effect immediately: invalidate outstanding sessions so the subject
        // re-authenticates with the new roles rather than riding an old token's embedded set. A
        // revocation persist failure fails the call (retryable) — the binding is durable, the session
        // downgrade is not yet.
        self.revoke_sessions(subject)?;
        Ok(())
    }

    /// The index allowlist bound to `subject` for built-in login — threaded into the session JWT's
    /// `indexes` claim so per-index RBAC restricts them. Empty (no binding) = unrestricted across
    /// indexes. Empty for an unknown/empty subject.
    pub fn indexes_for(&self, subject: &str) -> Vec<String> {
        if subject.is_empty() {
            return Vec::new();
        }
        self.read_index_bindings()
            .get(subject)
            .cloned()
            .unwrap_or_default()
    }

    /// Set (replace) `subject`'s index allowlist. Empty `indexes` removes the binding (making the
    /// subject unrestricted). Entries are de-duplicated and order-stable; an empty `subject` is
    /// rejected. Like a role change, this bumps the subject's session epoch so an outstanding token
    /// minted with the old scope is superseded and they re-authenticate.
    pub fn set_user_indexes(&self, subject: &str, indexes: Vec<String>) -> Result<()> {
        if subject.trim().is_empty() {
            return Err(RegistryError::SavedQueryNotFound("(empty subject)".into()));
        }
        let mut deduped: Vec<String> = Vec::new();
        for i in indexes {
            let i = i.trim().to_string();
            if !i.is_empty() && !deduped.contains(&i) {
                deduped.push(i);
            }
        }
        {
            // Hold only `index_bindings` — persist_snapshot re-reads every map off-lock.
            let mut bindings = self.write_index_bindings();
            if deduped.is_empty() {
                bindings.remove(subject);
            } else {
                bindings.insert(subject.to_string(), deduped);
            }
        }
        self.persist_snapshot()?;
        // A scope change takes effect immediately: supersede outstanding sessions (like a role change).
        self.revoke_sessions(subject)?;
        Ok(())
    }

    /// All API tokens, newest first. The caller strips the `hash` before returning to a client —
    /// only metadata leaves the control plane.
    pub fn list_tokens(&self) -> Vec<ApiToken> {
        let mut out: Vec<ApiToken> = self.read_tokens().values().cloned().collect();
        out.sort_by_key(|t| std::cmp::Reverse(t.created_at_ms));
        out
    }

    /// Persist a new API token. The caller has minted the secret + hash + id; the registry stamps
    /// `created_at_ms` and returns the stored token.
    pub fn create_token(&self, mut token: ApiToken) -> Result<ApiToken> {
        let now = now_ms();
        token.created_at_ms = now;
        {
            let mut tokens = self.write_tokens();
            // Prune expired tokens so the map (and its persisted copy) can't grow without bound.
            tokens.retain(|_, t| !t.is_expired(now));
            tokens.insert(token.id.clone(), token.clone());
        } // release the tokens write lock before rebuilding the index / persisting off-lock
        self.rebuild_token_index();
        self.persist_snapshot()?;
        Ok(token)
    }

    /// Revoke an API token by id — effective immediately. Errors if it doesn't exist.
    pub fn revoke_token(&self, id: &str) -> Result<()> {
        {
            let mut tokens = self.write_tokens();
            if tokens.remove(id).is_none() {
                return Err(RegistryError::SavedQueryNotFound(id.to_string()));
            }
        }
        self.rebuild_token_index();
        self.persist_snapshot()
    }

    /// Set (or replace) a subject's built-in password: salted-argon2-hash it and persist. Never
    /// stores plaintext. Errors only on a hashing/persist failure, not on a re-set.
    pub fn set_credential(&self, subject: &str, password: &str) -> Result<()> {
        use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
        use argon2::Argon2;
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| RegistryError::Credential(e.to_string()))?
            .to_string();
        {
            // Hold only `credentials` — persist_snapshot re-reads every map off-lock.
            let mut creds = self.write_credentials();
            creds.insert(subject.to_string(), hash);
        }
        self.persist_snapshot()
    }

    /// Verify a subject's password against its stored argon2 hash. `false` for an unknown subject or
    /// a wrong password — the caller can't distinguish the two. To avoid a **username-enumeration
    /// timing oracle**, an unknown subject is verified against a fixed dummy hash so both paths
    /// perform equivalent Argon2 work before returning `false`.
    pub fn verify_credential(&self, subject: &str, password: &str) -> bool {
        use argon2::password_hash::{PasswordHash, PasswordVerifier};
        use argon2::Argon2;
        let creds = self.read_credentials();
        let stored = creds.get(subject).cloned();
        // Real hash when the subject exists, else the dummy — so timing doesn't leak existence.
        let hash_str = stored
            .as_deref()
            .unwrap_or_else(|| DUMMY_CREDENTIAL_HASH.as_str());
        let Ok(parsed) = PasswordHash::new(hash_str) else {
            return false;
        };
        let matched = Argon2::default()
            .verify_password(password.as_bytes(), &parsed)
            .is_ok();
        // Never authenticate against the dummy hash — an unknown subject is always `false`.
        matched && stored.is_some()
    }

    /// Remove a subject's built-in credential. No-op if absent.
    pub fn remove_credential(&self, subject: &str) -> Result<()> {
        {
            // Hold only `credentials` — persist_snapshot re-reads every map off-lock.
            let mut creds = self.write_credentials();
            creds.remove(subject);
        }
        self.persist_snapshot()?;
        // Deprovision: kill outstanding sessions so a removed user can't keep riding a live JWT.
        self.revoke_sessions(subject)?;
        Ok(())
    }

    /// Whether any built-in credential exists — decides whether to seed an initial admin on first
    /// closed-mode boot.
    pub fn has_credentials(&self) -> bool {
        !self.read_credentials().is_empty()
    }

    /// Whether `subject` has a built-in credential — lets a seeder be idempotent about a single
    /// account (e.g. the demo user) without clobbering an operator-changed password on restart.
    pub fn has_credential(&self, subject: &str) -> bool {
        self.read_credentials().contains_key(subject)
    }

    /// Look up a token by its secret's `hash` — used by the authenticator on every authenticated
    /// request, so O(1) via the derived index rather than a linear scan. `None` if no such **live**
    /// token, so a revoked or **expired** token fails authentication. The two locks are taken
    /// one-at-a-time (index, released, then tokens) so this never nests with the writer's order.
    pub fn find_token(&self, hash: &str) -> Option<ApiToken> {
        let id = self
            .token_by_hash
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(hash)
            .cloned()?;
        let token = self.read_tokens().get(&id).cloned()?;
        // An expired token doesn't authenticate (it's pruned on the next create_token).
        (!token.is_expired(now_ms())).then_some(token)
    }

    /// Rebuild the hash→id index from the current token map. Cheap (tokens are few and change
    /// rarely), and rebuilding wholesale after each mutation avoids incremental-sync bugs. Must be
    /// called with **no** `tokens` write lock held — it takes `tokens` read then `token_by_hash` write.
    fn rebuild_token_index(&self) {
        let index: std::collections::HashMap<String, String> = self
            .read_tokens()
            .iter()
            .map(|(id, t)| (t.hash.clone(), id.clone()))
            .collect();
        *self
            .token_by_hash
            .write()
            .unwrap_or_else(|e| e.into_inner()) = index;
    }

    /// A monotonic-ish token id: `tok-<ms>-<counter>`.
    pub fn next_token_id(&self) -> String {
        format!(
            "tok-{}-{}",
            now_ms(),
            self.next_token
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }

    /// Append a lifecycle event to `index`'s activity log, trimmed to the retention cap.
    /// Best-effort persist — a sidecar write failure never fails the mutation that recorded it.
    pub fn record_activity(
        &self,
        index: &str,
        kind: impl Into<String>,
        message: impl Into<String>,
    ) {
        let event = ActivityEvent {
            ts_ms: now_ms(),
            kind: kind.into(),
            message: message.into(),
        };
        {
            let mut log = self.activity.write().unwrap_or_else(|e| e.into_inner());
            let events = log.entry(index.to_string()).or_default();
            events.push(event);
            if events.len() > ACTIVITY_RETAIN {
                let drop = events.len() - ACTIVITY_RETAIN;
                events.drain(0..drop);
            }
        } // drop the data lock before any I/O — the fsync no longer blocks reads/appends.
        self.flush_activity();
    }

    /// Persist the activity sidecar off the data lock, coalescing bursts. An isolated event flushes
    /// immediately (synchronous durability preserved); events arriving within
    /// [`ACTIVITY_FLUSH_DEBOUNCE_MS`] of the last write are marked `dirty` and folded into the next
    /// flush — a later event past the window, or the [`Drop`] shutdown flush. Best-effort: a write
    /// failure never fails the mutation that recorded the event.
    fn flush_activity(&self) {
        // The flush mutex serializes concurrent writers (last snapshot wins the file) and is taken
        // *before* the brief activity read lock — a consistent order that never nests the other way.
        let mut flush = self
            .activity_flush
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let now = now_ms();
        if flush.last_flush_ms != 0 && now - flush.last_flush_ms < ACTIVITY_FLUSH_DEBOUNCE_MS {
            flush.dirty = true;
            return;
        }
        // Snapshot under a brief read lock, released before the fsync so routing/list reads and
        // further appends never wait on disk I/O.
        let snapshot = self
            .activity
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        flush.last_flush_ms = now;
        flush.dirty = false;
        if let Err(e) = self.backend.persist_activity(&snapshot) {
            tracing::warn!(error = %e, "failed to persist activity log");
        }
    }

    /// The subject's **session epoch** (epoch ms): a session JWT with `iat` before this is stale and
    /// must be rejected. `0` means no revocation is in effect for this subject.
    pub fn session_epoch(&self, subject: &str) -> i64 {
        self.session_epochs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(subject)
            .copied()
            .unwrap_or(0)
    }

    /// Invalidate all of `subject`'s outstanding sessions by advancing its session epoch to now —
    /// called when the subject's roles change or its credential is removed, so a role downgrade /
    /// deprovision takes effect immediately (the next call with a stale session is rejected and must
    /// re-authenticate with the current roles).
    ///
    /// A revocation that isn't durable isn't a revocation — it would silently un-revoke on the next
    /// failover — so a persist failure rolls the in-memory bump back and is a **hard error** to the
    /// caller. The persist runs off the `session_epochs` write guard (the crate's persist-off-lock
    /// invariant: auth-path `session_epoch` reads never wait on a store round-trip), serialized
    /// under `sessions_flush` so concurrent revokes can't persist out of order and lose a bump.
    pub fn revoke_sessions(&self, subject: &str) -> Result<()> {
        let bumped = now_ms();
        let prior = {
            let mut epochs = self
                .session_epochs
                .write()
                .unwrap_or_else(|e| e.into_inner());
            epochs.insert(subject.to_string(), bumped)
        };
        let _flush = self
            .sessions_flush
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        // Snapshot under the flush lock (after the bump) so each serialized persist writes the
        // latest map — a pair of concurrent revokes can't overwrite one bump with a stale clone.
        let snapshot = self
            .session_epochs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Err(e) = self.backend.persist_sessions(&snapshot) {
            // Roll back this bump (only if no later revoke already superseded it) so memory never
            // claims a revocation the store doesn't hold.
            let mut epochs = self
                .session_epochs
                .write()
                .unwrap_or_else(|e| e.into_inner());
            if epochs.get(subject) == Some(&bumped) {
                match prior {
                    Some(p) => epochs.insert(subject.to_string(), p),
                    None => epochs.remove(subject),
                };
            }
            return Err(e);
        }
        Ok(())
    }

    /// `index`'s activity events, **newest first**, capped at `limit` (0 = all retained).
    pub fn list_activity(&self, index: &str, limit: usize) -> Vec<ActivityEvent> {
        let log = self.activity.read().unwrap_or_else(|e| e.into_inner());
        let Some(events) = log.get(index) else {
            return Vec::new();
        };
        let take = if limit == 0 { events.len() } else { limit };
        events.iter().rev().take(take).cloned().collect()
    }

    /// Resolve a `name` to the concrete indexes a search/route should touch: an **alias**
    /// → its members; an exact **index** name → just itself; an **index pattern** (a glob like
    /// `events-*`) → every registered index whose name matches, sorted; anything else → empty.
    /// Patterns are resolved here at read time, so a growing set needs no maintained alias.
    pub fn resolve(&self, name: &str) -> Vec<String> {
        let indexes = self.read_map();
        let aliases = self.read_aliases();
        if let Some(targets) = aliases.get(name) {
            return targets.iter().cloned().collect();
        }
        if indexes.contains_key(name) {
            return vec![name.to_string()];
        }
        if name.contains('*') {
            // `BTreeMap` keys iterate sorted → deterministic member order.
            return indexes
                .keys()
                .filter(|n| glob_match(name, n))
                .cloned()
                .collect();
        }
        Vec::new()
    }

    /// The full definition + status for `name`, if registered.
    pub fn get(&self, name: &str) -> Option<IndexEntry> {
        self.read_map().get(name).cloned()
    }

    /// All registered indexes as compact summaries, name-sorted.
    pub fn list(&self) -> Vec<IndexSummary> {
        self.read_map()
            .iter()
            .map(|(name, e)| IndexSummary {
                name: name.clone(),
                status: e.status,
            })
            .collect()
    }

    // ---- shard map -------------------------------------------------------------

    /// Set the **primary** for `shard` of `index` (creating the assignment if absent).
    /// Errors if the index is unregistered.
    pub fn assign_primary(&self, index: &str, shard: u32, node: impl Into<NodeId>) -> Result<()> {
        self.with_shard(index, shard, |a| a.primary = Some(node.into()))
    }

    /// Set `node` as the primary for **all** of `shards` of `index` in a **single** persist — the
    /// batched form of [`assign_primary`]. Per-ordinal `assign_primary` would rewrite the whole
    /// `registry.json` each time (O(N²) bytes to bring up an N-shard index); this mutates all in
    /// memory under one lock, then persists once. Errors if the index is unregistered.
    pub fn assign_primaries(
        &self,
        index: &str,
        shards: &[u32],
        node: impl Into<NodeId>,
    ) -> Result<()> {
        if shards.is_empty() {
            return Ok(());
        }
        let node = node.into();
        let mut map = self.write_map();
        let entry = map
            .get_mut(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        for &shard in shards {
            entry.shards.entry(shard).or_default().primary = Some(node.clone());
        }
        drop(map);
        self.persist_snapshot()
    }

    /// A node's **announce** of the ordinal shards it serves (`RegisterServedIndex`) — the guarded,
    /// entitlement-checked form of [`assign_primaries`](Self::assign_primaries) (HA-D3/HA-D7).
    /// Under one write-lock acquisition:
    ///
    /// - **Idempotent:** announcing shards this endpoint already primaries is a no-op re-point.
    /// - **First-wins, not last-write-wins:** a shard whose current primary is a **different**
    ///   endpoint that is *not confidently dead* refuses the whole announce with
    ///   [`PlacementConflict`](RegistryError::PlacementConflict) (gRPC `FAILED_PRECONDITION`) —
    ///   unless the announcer is a listed **replica** of that shard, in which case its announce is a
    ///   serving report and the primary is left untouched. A confidently-dead primary is taken over
    ///   (the node-restart-at-a-new-endpoint flow).
    /// - **Entitlement (fail-closed):** taking primaries of shards that never had one is gated on
    ///   `entitled_nodes` exactly like `resolve` — admitting a primary on a node that is not already
    ///   primary-holding, once the cap is reached, is
    ///   [`EntitlementExceeded`](RegistryError::EntitlementExceeded) (`RESOURCE_EXHAUSTED`).
    pub fn announce_primaries(
        &self,
        index: &str,
        shards: &[u32],
        endpoint: &str,
        now_ms: i64,
        entitled_nodes: usize,
    ) -> Result<()> {
        if shards.is_empty() {
            return Ok(());
        }
        let node = NodeId::from(endpoint);
        let mut map = self.write_map();
        let entry = map
            .get(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        // Classify each announced ordinal (no mutation yet).
        let mut replica_report: BTreeSet<u32> = BTreeSet::new();
        let mut any_fresh = false;
        for &shard in shards {
            match entry.shards.get(&shard).and_then(|sa| sa.primary.as_ref()) {
                None => any_fresh = true,
                Some(cur) if *cur == node => {}
                Some(cur) => {
                    if self.owner_confidently_dead(&cur.0, now_ms) || !self.is_pool_eligible(&cur.0)
                    {
                        // Takeover of a dead primary (allowed even for a warm replica announcing
                        // itself), OR of a **classic** (non-pool-eligible) owner: a classic index has
                        // a single self-declared owner, so a fresh announce re-points it
                        // (last-write-wins). A CP-assigned **pool** primary stays first-wins (needs its
                        // owner confidently dead) so a node can't steal an assigned unit.
                    } else if entry
                        .shards
                        .get(&shard)
                        .is_some_and(|sa| sa.replicas.contains(&node))
                    {
                        replica_report.insert(shard); // holder reporting, not a primary claim
                    } else {
                        return Err(RegistryError::PlacementConflict(format!(
                            "shard {shard} of `{index}` is held by `{}`",
                            cur.0
                        )));
                    }
                }
            }
        }
        // Fresh primaries past the cap fail closed — checked in the same critical section.
        if any_fresh {
            let nodes = self.entitlement_nodes(&map, now_ms);
            if !nodes.contains(endpoint) && nodes.len() >= entitled_nodes {
                return Err(RegistryError::EntitlementExceeded {
                    nodes: nodes.len(),
                    entitled: entitled_nodes,
                });
            }
        }
        let entry = map.get_mut(index).expect("presence checked above");
        let mut changed = false;
        for &shard in shards {
            if replica_report.contains(&shard) {
                continue;
            }
            let sa = entry.shards.entry(shard).or_default();
            if sa.primary.as_ref() != Some(&node) {
                sa.primary = Some(node.clone());
                changed = true;
            }
            let before = sa.replicas.len();
            sa.replicas.retain(|n| n != &node); // never primary + replica at once
            changed |= sa.replicas.len() != before;
        }
        drop(map);
        if changed {
            self.persist_snapshot()?; // an idempotent re-announce (10 s cadence) skips the rewrite
        }
        Ok(())
    }

    /// A windowed node's **announce** of the windows it serves (+ zone-maps and hot/cold tier) —
    /// the guarded, entitlement-checked, **batched** counterpart of
    /// [`assign_window`](Self::assign_window)/[`set_window_bounds`](Self::set_window_bounds)/
    /// [`set_window_cold`](Self::set_window_cold): one lock, one persist for the whole announce.
    /// The same first-wins semantics as [`announce_primaries`](Self::announce_primaries): a window
    /// primaried by a live foreign node refuses the announce (`PlacementConflict`) unless the
    /// announcer is a listed replica (serving report — bounds/tier from a replica are ignored; the
    /// primary's report is authoritative). All fresh windows of one announce cost at most **one**
    /// new primary-holding node (`endpoint`), and none at all when the endpoint already holds a
    /// primary, so an accumulating windowed index never grows its entitlement footprint.
    pub fn announce_windows(
        &self,
        index: &str,
        endpoint: &str,
        windows: &[WindowAnnounce],
        now_ms: i64,
        entitled_nodes: usize,
    ) -> Result<()> {
        if windows.is_empty() {
            return Ok(());
        }
        let node = NodeId::from(endpoint);
        let mut map = self.write_map();
        let entry = map
            .get(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        let mut replica_report: BTreeSet<i64> = BTreeSet::new();
        let mut any_fresh = false;
        for w in windows {
            match entry
                .windows
                .get(&w.window)
                .and_then(|wa| wa.assignment.primary.as_ref())
            {
                None => any_fresh = true,
                Some(cur) if *cur == node => {}
                Some(cur) => {
                    if self.owner_confidently_dead(&cur.0, now_ms) || !self.is_pool_eligible(&cur.0)
                    {
                        // Takeover of a dead primary (allowed even for a warm replica), or of a
                        // classic (non-pool-eligible) owner re-pointing — last-write-wins; a
                        // CP-assigned pool primary stays first-wins. See the hash path above.
                    } else if entry
                        .windows
                        .get(&w.window)
                        .is_some_and(|wa| wa.assignment.replicas.contains(&node))
                    {
                        replica_report.insert(w.window);
                    } else {
                        return Err(RegistryError::PlacementConflict(format!(
                            "window {} of `{index}` is held by `{}`",
                            w.window, cur.0
                        )));
                    }
                }
            }
        }
        if any_fresh {
            let nodes = self.entitlement_nodes(&map, now_ms);
            if !nodes.contains(endpoint) && nodes.len() >= entitled_nodes {
                return Err(RegistryError::EntitlementExceeded {
                    nodes: nodes.len(),
                    entitled: entitled_nodes,
                });
            }
        }
        let entry = map.get_mut(index).expect("presence checked above");
        let mut changed = false;
        for w in windows {
            if replica_report.contains(&w.window) {
                continue; // a replica's report never re-points or overwrites the primary's metadata
            }
            let is_new = !entry.windows.contains_key(&w.window);
            let wa = entry.windows.entry(w.window).or_default();
            changed |= is_new;
            if wa.assignment.primary.as_ref() != Some(&node) {
                wa.assignment.primary = Some(node.clone());
                changed = true;
            }
            let before = wa.assignment.replicas.len();
            wa.assignment.replicas.retain(|n| n != &node);
            changed |= wa.assignment.replicas.len() != before;
            if let Some((min, max)) = w.bounds {
                let widened = (
                    Some(wa.event_min.map_or(min, |m| m.min(min))),
                    Some(wa.event_max.map_or(max, |m| m.max(max))),
                );
                if (wa.event_min, wa.event_max) != widened {
                    (wa.event_min, wa.event_max) = widened;
                    changed = true;
                }
            }
            if wa.cold != w.cold {
                wa.cold = w.cold;
                changed = true;
            }
        }
        drop(map);
        if changed {
            self.persist_snapshot()?; // an idempotent re-announce (10 s cadence) skips the rewrite
        }
        Ok(())
    }

    /// Add a read **replica** for `shard` of `index` (idempotent; never duplicates, and never
    /// adds the current primary as a replica). Errors if the index is unregistered.
    pub fn add_replica(&self, index: &str, shard: u32, node: impl Into<NodeId>) -> Result<()> {
        let node = node.into();
        self.with_shard(index, shard, |a| {
            if a.primary.as_ref() != Some(&node) && !a.replicas.contains(&node) {
                a.replicas.push(node);
            }
        })
    }

    /// Remove `node` from `shard` of `index`, whether it was the primary or a replica. Errors
    /// if the index is unregistered.
    pub fn remove_node(&self, index: &str, shard: u32, node: &NodeId) -> Result<()> {
        self.with_shard(index, shard, |a| {
            if a.primary.as_ref() == Some(node) {
                a.primary = None;
            }
            a.replicas.retain(|n| n != node);
        })
    }

    /// **Promote** the first replica of `shard` to primary (the mechanism leader election runs on
    /// primary loss), returning the promoted node. Errors if the index is unregistered, the shard
    /// has no replica, or — the **fencing precondition** — a primary is still assigned: the caller
    /// must fence/clear the old primary first (`remove_node`), so promotion can't produce two
    /// primaries for one shard (split brain).
    pub fn promote_replica(&self, index: &str, shard: u32) -> Result<NodeId> {
        let mut map = self.write_map();
        let entry = map
            .get_mut(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        let assignment = entry.shards.entry(shard).or_default();
        // Fencing: refuse to promote over a still-assigned primary — clear it first.
        if let Some(primary) = &assignment.primary {
            return Err(RegistryError::PrimaryStillAssigned {
                index: index.to_string(),
                shard,
                primary: primary.0.clone(),
            });
        }
        if assignment.replicas.is_empty() {
            return Err(RegistryError::NoReplica {
                index: index.to_string(),
                shard,
            });
        }
        let promoted = assignment.replicas.remove(0);
        assignment.primary = Some(promoted.clone());
        drop(map);
        self.persist_snapshot()?;
        Ok(promoted)
    }

    /// The shard map for `index` (`shard → assignment`), if registered. A clone, for routing.
    pub fn shard_map(&self, index: &str) -> Option<BTreeMap<u32, ShardAssignment>> {
        self.read_map().get(index).map(|e| e.shards.clone())
    }

    // ---- virtual-bucket map ----------------------------------------------------

    /// The stored [`BucketMap`] for `index`, or `None` when it routes **legacy** (`fnv % shards`).
    /// The single source of truth both the connector (writes) and the Gateway (reads) route
    /// through, so placement can't drift. Returns `None` for an unknown index too.
    pub fn bucket_map(&self, index: &str) -> Option<BucketMap> {
        let map = self.read_map();
        let entry = map.get(index)?;
        if entry.bucket_owners.is_empty() {
            None
        } else {
            // Stored maps are always written via `set_bucket_map`/`apply_reshard`, so they're valid.
            BucketMap::from_owners(entry.bucket_owners.clone()).ok()
        }
    }

    /// Store `map` as `index`'s bucket→shard assignment, then persist — **compare-and-swap**:
    /// `expected` must match the stored map (`None` = expects no map yet) or the write is refused
    /// with [`PlacementConflict`](RegistryError::PlacementConflict). Placement ops (apply-reshard,
    /// move-bucket) read the map, run a minutes-long source rebuild, and commit here; without the CAS
    /// two concurrent ops would last-write-wins clobber each other — e.g. a finished move-bucket
    /// committing a pre-cutover map would revert a reshard's ownership while the data already lives
    /// under the new map.
    pub fn set_bucket_map(
        &self,
        index: &str,
        expected: Option<&BucketMap>,
        map: &BucketMap,
    ) -> Result<()> {
        let mut indexes = self.write_map();
        let entry = indexes
            .get_mut(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        let expected_owners = expected.map(|m| m.owners()).unwrap_or(&[]);
        if entry.bucket_owners != expected_owners {
            return Err(RegistryError::PlacementConflict(format!(
                "the bucket map of `{index}` changed while this operation ran — re-plan and retry"
            )));
        }
        entry.bucket_owners = map.owners().to_vec();
        drop(indexes);
        self.persist_snapshot()
    }

    /// Adopt a [balanced](BucketMap::balanced) bucket map over `shard_count` if the index has none
    /// yet; a no-op (Ok(false)) when a map is already stored. Called on every served-index
    /// registration, so **every ordinal index is bucketed from its first announce**. Check-and-set
    /// under one write lock, so concurrent first-registrations can't race and a **growth build
    /// target** (`--shards N+k` during a reshard) finds the map present and leaves live routing
    /// untouched until cutover.
    pub fn adopt_bucket_map_if_absent(&self, index: &str, shard_count: u32) -> Result<bool> {
        let mut indexes = self.write_map();
        let entry = indexes
            .get_mut(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        if !entry.bucket_owners.is_empty() {
            return Ok(false);
        }
        entry.bucket_owners = BucketMap::balanced(shard_count.max(1)).owners().to_vec();
        drop(indexes);
        self.persist_snapshot()?;
        Ok(true)
    }

    /// The bucket map `index` routes through **today** — its stored map, or the
    /// [balanced](BucketMap::balanced) default over its current shard count for a legacy index
    /// that hasn't adopted buckets yet (so the first reshard transparently moves it onto buckets).
    fn current_bucket_map(&self, index: &str) -> Result<BucketMap> {
        let map = self.read_map();
        let entry = map
            .get(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        if entry.bucket_owners.is_empty() {
            Ok(BucketMap::balanced(entry.shards.len().max(1) as u32))
        } else {
            BucketMap::from_owners(entry.bucket_owners.clone())
                .map_err(RegistryError::InvalidBucketMap)
        }
    }

    /// **Plan** a reshard of `index` to `new_shard_count`: the bounded, balanced bucket→shard
    /// reassignment to reach the new count — computed, **not applied**. The returned
    /// move list is the migration work-list for the online cutover (a later slice). Read-only and
    /// safe to call anytime; errors only if the index is unknown.
    pub fn plan_reshard(&self, index: &str, new_shard_count: u32) -> Result<Reassignment> {
        Ok(self.current_bucket_map(index)?.reassign(new_shard_count))
    }

    /// Mutate one shard's assignment under the write lock, then persist.
    fn with_shard(
        &self,
        index: &str,
        shard: u32,
        f: impl FnOnce(&mut ShardAssignment),
    ) -> Result<()> {
        let mut map = self.write_map();
        let entry = map
            .get_mut(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        f(entry.shards.entry(shard).or_default());
        drop(map);
        self.persist_snapshot()
    }

    // ---- window map ------------------------------------------------------------

    /// Set the **primary** node for a time `window` of `index` (creating the assignment if absent).
    /// The node calls this when it begins serving a window shard. Errors if the index is absent.
    pub fn assign_window(&self, index: &str, window: i64, node: impl Into<NodeId>) -> Result<()> {
        self.with_window(index, window, |w| w.assignment.primary = Some(node.into()))
    }

    /// **Widen** a window's event-time zone-map `[min, max]` (the serving node reports its bounds as
    /// it ingests). The Gateway prunes a window whose `[min, max]` can't overlap an event-time
    /// filter. A no-op when both bounds are `None`. Errors if the index is absent.
    pub fn set_window_bounds(
        &self,
        index: &str,
        window: i64,
        min: Option<i64>,
        max: Option<i64>,
    ) -> Result<()> {
        self.with_window(index, window, |w| {
            if let (Some(min), Some(max)) = (min, max) {
                w.event_min = Some(w.event_min.map_or(min, |m| m.min(min)));
                w.event_max = Some(w.event_max.map_or(max, |m| m.max(max)));
            }
        })
    }

    /// Record whether the serving node currently holds `window` **cold** (read-through from object
    /// storage). Reported every heartbeat, so it tracks park/pre-warm tier swaps; the Gateway reads
    /// it for `/v1/cold`. Errors if the index is absent.
    pub fn set_window_cold(&self, index: &str, window: i64, cold: bool) -> Result<()> {
        self.with_window(index, window, |w| w.cold = cold)
    }

    /// The window map for `index` (`window-id → WindowAssignment`), if registered. A clone, for the
    /// Gateway to route + prune time-windowed queries.
    pub fn window_map(&self, index: &str) -> Option<BTreeMap<i64, WindowAssignment>> {
        self.read_map().get(index).map(|e| e.windows.clone())
    }

    /// Mutate one window's assignment under the write lock, then persist.
    fn with_window(
        &self,
        index: &str,
        window: i64,
        f: impl FnOnce(&mut WindowAssignment),
    ) -> Result<()> {
        let mut map = self.write_map();
        let entry = map
            .get_mut(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        f(entry.windows.entry(window).or_default());
        drop(map);
        self.persist_snapshot()
    }

    // ---- CP-driven universal placement pool (D52) ------------------------------

    /// Record a node's liveness **heartbeat** into the [placement pool](Self::node_pool): the node
    /// calls this on registration + on an interval to stay eligible for unit placement. In-memory
    /// only; `now_ms` is the control plane's wall clock. The pool is index-agnostic — a node is an
    /// interchangeable shard host, not bound to one index.
    ///
    /// Convenience form that declares the node **replica-capable**. The RPC handler goes through
    /// [`register_node_with_capability`](Self::register_node_with_capability) with the flag the node
    /// actually sent (HA-G2), so an object-store-less node never attracts replica placements.
    pub fn register_node(&self, endpoint: &str, now_ms: i64) {
        self.register_node_with_capability(endpoint, true, now_ms);
    }

    /// [`register_node`](Self::register_node) with an explicit **replica capability** declaration
    /// (HA-G2): `replica_capable = true` iff the node has an object store configured and can open
    /// replica windows read-through (D53). Replica top-up in
    /// [`resolve_unit_holders`](Self::resolve_unit_holders) places only on currently-capable nodes;
    /// primaries are unaffected. Refreshed every heartbeat — a `false` after a `true` (the node lost
    /// its object store config) removes it from the capable set.
    pub fn register_node_with_capability(
        &self,
        endpoint: &str,
        replica_capable: bool,
        now_ms: i64,
    ) {
        self.node_pool
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(endpoint.to_string(), now_ms);
        {
            let mut cap = self
                .replica_capable
                .write()
                .unwrap_or_else(|e| e.into_inner());
            if replica_capable {
                cap.insert(endpoint.to_string());
            } else {
                cap.remove(endpoint);
            }
        }
        // A `RegisterNode` heartbeat admits the node to the placement-eligible pool: the CP may now
        // assign it units. (A classic served-index owner never lands here — see `touch_node_liveness`.)
        self.pool_eligible
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(endpoint.to_string());
        // First heartbeat since boot/promotion arms the liveness grace anchor (HA-D5): for one TTL
        // from here, owners that haven't re-registered yet are treated live-unknown.
        let _ = self.grace_anchor_ms.compare_exchange(
            -1,
            now_ms,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    /// Refresh a **served-index owner's liveness** WITHOUT admitting it to the placement pool — the
    /// [`RegisterServedIndex`] counterpart to [`register_node`](Self::register_node). A classic
    /// fixed-endpoint node (`serve --index X`) heartbeats here on every announce; tracking its
    /// liveness in [`node_pool`](Self::node_pool) keeps the dead-owner sweeper from stealing its
    /// self-declared units. It is NOT added to [`pool_eligible`](Self::pool_eligible), so the CP never
    /// *places* a pool unit onto it — see [`placement_nodes`](Self::placement_nodes). Arms the
    /// liveness grace like any heartbeat.
    pub fn touch_node_liveness(&self, endpoint: &str, now_ms: i64) {
        self.node_pool
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(endpoint.to_string(), now_ms);
        let _ = self.grace_anchor_ms.compare_exchange(
            -1,
            now_ms,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        );
    }

    /// Whether the **liveness grace window** is active at `now_ms` (HA-D5): the first
    /// [`NODE_HEARTBEAT_TTL_MS`] after the first heartbeat this (possibly freshly started or promoted)
    /// control plane observed. While active, an assigned owner that hasn't re-registered yet is
    /// **live-unknown** — dead-owner re-placement, announce re-points over it, and the
    /// [sweeper](Self::sweep_dead_primaries) are all suppressed, so laggards' units aren't
    /// mass-re-placed onto the first re-registrant. With no heartbeat ever observed the window is
    /// **not** active (the ordinal, non-pool shape with no liveness tracking).
    pub fn placement_grace_active(&self, now_ms: i64) -> bool {
        let anchor = self
            .grace_anchor_ms
            .load(std::sync::atomic::Ordering::SeqCst);
        anchor >= 0 && now_ms - anchor <= NODE_HEARTBEAT_TTL_MS
    }

    /// Override the grace anchor — for tests and operational tooling that need dead-owner actions
    /// enabled/suppressed deterministically (`-1` disarms; any epoch-ms re-anchors the window).
    pub fn set_placement_grace_anchor(&self, anchor_ms: i64) {
        self.grace_anchor_ms
            .store(anchor_ms, std::sync::atomic::Ordering::SeqCst);
    }

    /// Whether the brief **initial-placement settle** ([`INITIAL_PLACEMENT_SETTLE_MS`]) is still
    /// active — the first few seconds after the grace anchor, during which even a *never-placed*
    /// unit is held back so co-booting nodes can register and the first primaries place **balanced**.
    /// Strictly shorter than [`placement_grace_active`](Self::placement_grace_active), which
    /// additionally suppresses *re-placement* of already-held units for a full
    /// [`NODE_HEARTBEAT_TTL_MS`].
    pub fn initial_placement_settling(&self, now_ms: i64) -> bool {
        let anchor = self
            .grace_anchor_ms
            .load(std::sync::atomic::Ordering::SeqCst);
        anchor >= 0 && now_ms - anchor < INITIAL_PLACEMENT_SETTLE_MS
    }

    /// Whether `endpoint` has a live pool heartbeat (within [`NODE_HEARTBEAT_TTL_MS`] of `now_ms`).
    /// Gate for `SubscribeAssignments` identity: only a currently-registered node may subscribe to
    /// an endpoint's assignment stream.
    pub fn node_alive(&self, endpoint: &str, now_ms: i64) -> bool {
        self.node_pool
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(endpoint)
            .is_some_and(|&t| now_ms - t <= NODE_HEARTBEAT_TTL_MS)
    }

    /// Whether an assigned owner `endpoint` is **confidently dead** at `now_ms`: an untracked owner ⇒
    /// dead unless the grace window is active (announce-only deployments have no heartbeats, so
    /// re-announce is the only takeover mechanism); a tracked owner ⇒ dead iff its heartbeat is past
    /// the TTL and the grace window is inactive.
    fn owner_confidently_dead(&self, endpoint: &str, now_ms: i64) -> bool {
        let pool = self.node_pool.read().unwrap_or_else(|e| e.into_inner());
        match pool.get(endpoint) {
            Some(&t) => now_ms - t > NODE_HEARTBEAT_TTL_MS && !self.placement_grace_active(now_ms),
            None => !self.placement_grace_active(now_ms),
        }
    }

    /// Whether `endpoint` is a **placement-eligible pool node** (registered via `RegisterNode`), as
    /// opposed to a classic served-index owner tracked only for liveness
    /// ([`touch_node_liveness`](Self::touch_node_liveness) / [`pool_eligible`](Self::pool_eligible)).
    fn is_pool_eligible(&self, endpoint: &str) -> bool {
        self.pool_eligible
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains(endpoint)
    }

    /// Whether an assigned owner `endpoint` is **tracked in the pool and stale past the TTL** —
    /// the only state in which its node stops counting toward entitlement. Stricter than
    /// [`owner_confidently_dead`](Self::owner_confidently_dead): an owner the pool has *never*
    /// tracked (announce-only deployments, or a pre-restart corpse) keeps **counting** — the
    /// entitlement fails closed on unknown liveness — even though dead-owner *actions* may treat it
    /// as replaceable (availability first; a re-placement moves the primary, never multiplies nodes).
    fn owner_tracked_stale(&self, endpoint: &str, now_ms: i64) -> bool {
        self.node_pool
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(endpoint)
            .is_some_and(|&t| now_ms - t > NODE_HEARTBEAT_TTL_MS)
            && !self.placement_grace_active(now_ms)
    }

    /// Like [`register_node`](Self::register_node), but **refuses to admit a *new* node** once
    /// `max_nodes` distinct live endpoints are already in the pool — the scale limit. Re-registering
    /// an already-live endpoint always succeeds (it's a heartbeat, not new capacity), so an existing
    /// cluster is never disrupted. Atomic under the write lock. On rejection returns the current
    /// distinct live node count.
    pub fn register_node_capped(
        &self,
        endpoint: &str,
        now_ms: i64,
        max_nodes: usize,
    ) -> std::result::Result<(), usize> {
        let mut pool = self.node_pool.write().unwrap_or_else(|e| e.into_inner());
        let known = pool
            .get(endpoint)
            .is_some_and(|&t| now_ms - t <= NODE_HEARTBEAT_TTL_MS);
        if !known {
            let live = pool
                .values()
                .filter(|&&t| now_ms - t <= NODE_HEARTBEAT_TTL_MS)
                .count();
            if live >= max_nodes {
                return Err(live);
            }
        }
        pool.insert(endpoint.to_string(), now_ms);
        drop(pool);
        // Same default as `register_node`: the capped convenience form declares capability.
        self.replica_capable
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(endpoint.to_string());
        Ok(())
    }

    /// Count of distinct live node endpoints in the pool — the deployment's raw node usage.
    pub fn distinct_live_nodes(&self, now_ms: i64) -> usize {
        self.node_pool
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|&&t| now_ms - t <= NODE_HEARTBEAT_TTL_MS)
            .count()
    }

    /// The deployment's **entitlement usage** (D38/D53, Option A): the number of distinct **live
    /// nodes that hold ≥1 primary of any index**. This caps *concurrent scale* by node, never lifetime
    /// usage: a windowed index accumulating windows on one node costs **one** node forever, and
    /// packing primaries of many indexes onto one node still costs **one**. Read replicas are free
    /// (never a primary). A node **tracked in the pool and stale past the TTL** stops counting (its
    /// primaries are about to be re-placed onto a live node — net constant). **Unknown liveness
    /// counts** (grace window, or never-tracked announce-only owners) so the metric fails closed.
    pub fn count_entitlement_nodes(&self, now_ms: i64) -> usize {
        let map = self.read_map();
        self.entitlement_nodes(&map, now_ms).len()
    }

    /// The set of distinct live **primary-holding node endpoints** behind
    /// [`count_entitlement_nodes`](Self::count_entitlement_nodes), deduped across indexes. Takes the
    /// already-held `indexes` guard so placement paths can count **inside the same critical section as
    /// the mutation** (the HA-D3 TOCTOU fix). Takes `node_pool` briefly per owner — an independent
    /// lock, never taken in the reverse order.
    fn entitlement_nodes(
        &self,
        map: &BTreeMap<String, IndexEntry>,
        now_ms: i64,
    ) -> BTreeSet<String> {
        let mut nodes = BTreeSet::new();
        for e in map.values() {
            let mut add = |primary: Option<&NodeId>| {
                if let Some(p) = primary {
                    if !self.owner_tracked_stale(&p.0, now_ms) {
                        nodes.insert(p.0.clone());
                    }
                }
            };
            for sa in e.shards.values() {
                add(sa.primary.as_ref());
            }
            for wa in e.windows.values() {
                add(wa.assignment.primary.as_ref());
            }
        }
        nodes
    }

    /// Every endpoint whose heartbeat is within [`NODE_HEARTBEAT_TTL_MS`] of `now_ms` — pool nodes
    /// **and** classic served-index owners (sorted, since `node_pool` is a `BTreeMap`). This is the
    /// **liveness** view (used by the dead-owner sweeper + entitlement); for *placement targets* use
    /// [`placement_nodes`](Self::placement_nodes), which is this ∩ [`pool_eligible`](Self::pool_eligible).
    pub fn live_nodes(&self, now_ms: i64) -> Vec<String> {
        self.node_pool
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(_, &t)| now_ms - t <= NODE_HEARTBEAT_TTL_MS)
            .map(|(ep, _)| ep.clone())
            .collect()
    }

    /// The **placement-eligible** live pool: [`live_nodes`](Self::live_nodes) ∩
    /// [`pool_eligible`](Self::pool_eligible) — the nodes the control plane may assign units to. It
    /// excludes classic fixed-endpoint owners (which are live for the sweeper but must never receive
    /// a pool unit they can't build/serve). Placement (`resolve_unit_holders`) picks targets here.
    pub fn placement_nodes(&self, now_ms: i64) -> Vec<String> {
        let eligible = self.pool_eligible.read().unwrap_or_else(|e| e.into_inner());
        self.node_pool
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .filter(|(ep, &t)| now_ms - t <= NODE_HEARTBEAT_TTL_MS && eligible.contains(*ep))
            .map(|(ep, _)| ep.clone())
            .collect()
    }

    /// Resolve the node that owns a placement [`Unit`] (a shard or a window) of `index`, **placing it
    /// on first ask** — the CP-driven universal-pool assignment (D52), at `R = 1` (primary only).
    /// The single-holder view of [`resolve_unit_holders`](Self::resolve_unit_holders) — one placement
    /// path, so promotion of a warm replica, the liveness grace window, and the atomic entitlement
    /// check all apply here too. Returns `(primary endpoint, moved)` where `moved` is true iff this
    /// call made or moved the primary assignment.
    pub fn resolve_unit_owner(
        &self,
        index: &str,
        unit: Unit,
        entitled_nodes: usize,
        now_ms: i64,
    ) -> Result<(String, bool)> {
        let h = self.resolve_unit_holders(index, unit, 1, entitled_nodes, now_ms)?;
        Ok((h.primary, h.moved))
    }

    /// Resolve the owner of a **window** — the windowed special case of
    /// [`resolve_unit_owner`](Self::resolve_unit_owner). Kept as the connector's window entry point.
    pub fn resolve_window_owner(
        &self,
        index: &str,
        window: i64,
        entitled_nodes: usize,
        now_ms: i64,
    ) -> Result<(String, bool)> {
        self.resolve_unit_owner(index, Unit::Window(window), entitled_nodes, now_ms)
    }

    /// The units `endpoint` currently holds, as `(index, unit, is_primary)` — a node's **assignment
    /// snapshot** (D53), which the control plane pushes so the node opens/serves exactly these units
    /// (`is_primary = false` ⇒ a read replica it opens read-through). Scans every index's shard +
    /// window maps; a node that holds neither the primary nor a replica slot of a unit doesn't appear.
    pub fn node_assignments(&self, endpoint: &str) -> Vec<(String, Unit, bool)> {
        let role = |sa: &ShardAssignment| -> Option<bool> {
            if sa.primary.as_ref().is_some_and(|p| p.0 == endpoint) {
                Some(true)
            } else if sa.replicas.iter().any(|r| r.0 == endpoint) {
                Some(false)
            } else {
                None
            }
        };
        let map = self.read_map();
        let mut out = Vec::new();
        for (name, entry) in map.iter() {
            for (ordinal, sa) in &entry.shards {
                if let Some(is_primary) = role(sa) {
                    out.push((name.clone(), Unit::Shard(*ordinal), is_primary));
                }
            }
            for (window, wa) in &entry.windows {
                if let Some(is_primary) = role(&wa.assignment) {
                    out.push((name.clone(), Unit::Window(*window), is_primary));
                }
            }
        }
        out
    }

    /// Resolve the **R holders** of a unit (D53): one **primary** (sole writer) + up to
    /// `replication_factor − 1` read **replicas** over the live [placement pool](Self::node_pool).
    ///
    /// Idempotent when the unit already has a live primary and a full replica set; otherwise, in one
    /// pass, it:
    /// 1. **prunes dead holders** (drops replicas whose node has no live heartbeat);
    /// 2. **promotes** a live replica to primary if the primary died (else places a fresh primary on
    ///    the least-loaded live node **not already a replica of this unit**) — so a node kill
    ///    re-primaries onto a warm holder and a primary is never co-located on its own replica;
    /// 3. **tops up** replicas toward `R − 1` from the least-loaded live nodes not already holding the
    ///    unit, counting each node's holder load (primary + replica) across **all** indexes so the pool
    ///    bin-packs replicas too;
    /// 4. **trims** excess replicas past `R − 1` (a since-lowered `R` releases its extra holders).
    ///
    /// `replication_factor` is clamped to at least 1 (`R = 1` ⇒ primary only — the D52 behavior).
    /// Persists once iff anything changed; the returned [`moved`](UnitHolders::moved) is true iff the
    /// **primary** assignment was made or moved (replica churn alone doesn't set it).
    ///
    /// **Liveness grace (HA-D5):** while [`placement_grace_active`](Self::placement_grace_active),
    /// an assigned owner is live-unknown — the current holder set is returned untouched, so a
    /// freshly (re)started or promoted control plane never mass-re-places laggards' units onto the
    /// first re-registrant. Fresh (never-assigned) units still place normally.
    ///
    /// **Entitlement (HA-D3, atomic):** placing a **fresh** unit is checked against `entitled_nodes`
    /// (distinct live primary-holding nodes — see
    /// [`count_entitlement_nodes`](Self::count_entitlement_nodes)) inside the same write-lock
    /// critical section as the mutation, so concurrent resolves can't race past the cap. At the cap,
    /// placement falls back to a live node **already holding a primary of any index** (no new node
    /// counted — a windowed index keeps accumulating windows, and a new index co-locates, at constant
    /// entitlement cost); with no such node it fails
    /// [`EntitlementExceeded`](RegistryError::EntitlementExceeded). Re-resolves and dead-owner
    /// re-placement are never entitlement-gated (existing capacity, not new).
    ///
    /// Errors if the index is unregistered or no node is live (retryable). Re-placing a dead owner's
    /// unit only moves the *assignment* — the new owner rebuilds that unit's data from source / cold
    /// tier on demand.
    pub fn resolve_unit_holders(
        &self,
        index: &str,
        unit: Unit,
        replication_factor: usize,
        entitled_nodes: usize,
        now_ms: i64,
    ) -> Result<UnitHolders> {
        let r = replication_factor.max(1);
        let live = self.live_nodes(now_ms);
        let mut map = self.write_map();
        if !map.contains_key(index) {
            return Err(RegistryError::NotFound(index.to_string()));
        }
        // Liveness grace: owners are live-unknown → an assigned unit is returned as-is, untouched.
        if self.placement_grace_active(now_ms) {
            if let Some(a) = unit_assignment(&map[index], unit) {
                if let Some(p) = &a.primary {
                    return Ok(UnitHolders {
                        primary: p.0.clone(),
                        replicas: a.replicas.iter().map(|n| n.0.clone()).collect(),
                        changed: false,
                        moved: false,
                    });
                }
            }
        }
        if live.is_empty() {
            return Err(RegistryError::NoLiveNode {
                index: index.to_string(),
                unit: unit.to_string(),
            });
        }
        let live_set: BTreeSet<String> = live.iter().cloned().collect();
        // Entitlement inputs, computed under the SAME write-lock acquisition as the mutation below
        // (the HA-D3 TOCTOU fix). Only a fresh (never-assigned) unit is gated, so only then compute.
        let fresh_unit = unit_primary(&map[index], unit).is_none();
        let (node_count, primary_nodes): (usize, BTreeSet<String>) = if fresh_unit {
            let n = self.entitlement_nodes(&map, now_ms);
            (n.len(), n)
        } else {
            (0, BTreeSet::new())
        };
        // Placement targets are the **pool-eligible** live nodes only — a classic served-index owner
        // is live (so the sweeper leaves its units alone) but must never be assigned a pool unit it
        // can't build/serve. Holder load counts across ALL indexes so top-up bin-packs the pool.
        let placement = self.placement_nodes(now_ms);
        let mut load: BTreeMap<String, usize> =
            placement.iter().map(|e| (e.clone(), 0usize)).collect();
        for e in map.values() {
            for sa in e.shards.values() {
                for n in sa.nodes() {
                    if let Some(c) = load.get_mut(&n.0) {
                        *c += 1;
                    }
                }
            }
            for wa in e.windows.values() {
                for n in wa.assignment.nodes() {
                    if let Some(c) = load.get_mut(&n.0) {
                        *c += 1;
                    }
                }
            }
        }

        let entry = map.get_mut(index).expect("index presence checked above");
        let a = unit_assignment_mut(entry, unit);
        let mut changed = false;
        let mut moved = false;

        // 1. Prune dead replicas.
        let before = a.replicas.len();
        a.replicas.retain(|n| live_set.contains(&n.0));
        changed |= a.replicas.len() != before;

        // 2. Dead primary → clear, then promote a live (warm) replica or place a fresh primary.
        if a.primary.as_ref().is_some_and(|p| !live_set.contains(&p.0)) {
            a.primary = None;
            changed = true;
        }
        if a.primary.is_none() {
            if !a.replicas.is_empty() {
                a.primary = Some(a.replicas.remove(0)); // promote a warm replica (already live)
            } else {
                // Never co-locate the primary on a node still listed as a replica of this unit.
                let exclude: BTreeSet<String> = a.replicas.iter().map(|n| n.0.clone()).collect();
                // `load` is now the pool-eligible nodes only, which can be empty (e.g. all pool
                // nodes down but a classic owner still live) — so this is a retryable NoLiveNode,
                // not a panic.
                let mut pick = pick_least_loaded(&load, &exclude).ok_or_else(|| {
                    RegistryError::NoLiveNode {
                        index: index.to_string(),
                        unit: unit.to_string(),
                    }
                })?;
                // Atomic entitlement gate for a FRESH unit: at the cap, only nodes already holding a
                // primary of any index (no new node counted) may take it.
                if fresh_unit && node_count >= entitled_nodes && !primary_nodes.contains(&pick) {
                    let allowed: BTreeMap<String, usize> = load
                        .iter()
                        .filter(|(ep, _)| primary_nodes.contains(*ep))
                        .map(|(ep, c)| (ep.clone(), *c))
                        .collect();
                    pick = pick_least_loaded(&allowed, &exclude).ok_or(
                        RegistryError::EntitlementExceeded {
                            nodes: node_count,
                            entitled: entitled_nodes,
                        },
                    )?;
                }
                *load.get_mut(&pick).expect("picked a live node") += 1;
                a.primary = Some(pick.into());
            }
            changed = true;
            moved = true;
        }

        // 3. Top up replicas toward R−1 on least-loaded live **replica-capable** nodes not already
        //    holding the unit. Capability (HA-G2) filters REPLICAS only: a replica is served
        //    read-through from the object store, so a node without one could never answer for it —
        //    placing there would silently absent HA. Primaries (step 2) are unaffected.
        let incapable: BTreeSet<String> = {
            let cap = self
                .replica_capable
                .read()
                .unwrap_or_else(|e| e.into_inner());
            live_set
                .iter()
                .filter(|e| !cap.contains(*e))
                .cloned()
                .collect()
        };
        while a.replicas.len() + 1 < r {
            let mut exclude: BTreeSet<String> = a.replicas.iter().map(|n| n.0.clone()).collect();
            if let Some(p) = &a.primary {
                exclude.insert(p.0.clone());
            }
            exclude.extend(incapable.iter().cloned());
            let Some(pick) = pick_least_loaded(&load, &exclude) else {
                break; // fewer live capable nodes than R — hold what we can
            };
            *load.get_mut(&pick).expect("picked a live node") += 1;
            a.replicas.push(pick.into());
            changed = true;
        }

        // 4. Trim excess replicas past R−1 (R was lowered since they were placed).
        if a.replicas.len() + 1 > r {
            a.replicas.truncate(r - 1);
            changed = true;
        }

        let holders = UnitHolders {
            primary: a.primary.as_ref().expect("primary set above").0.clone(),
            replicas: a.replicas.iter().map(|n| n.0.clone()).collect(),
            changed,
            moved,
        };
        drop(map);
        if changed {
            self.persist_snapshot()?;
        }
        Ok(holders)
    }

    /// The units whose assigned **primary is confidently dead** at `now_ms` — the sweeper's
    /// work-list. Empty while the [grace window](Self::placement_grace_active) is active or nothing
    /// is dead. Read-only.
    pub fn dead_primary_units(&self, now_ms: i64) -> Vec<(String, Unit)> {
        if self.placement_grace_active(now_ms) {
            return Vec::new();
        }
        let map = self.read_map();
        let mut out = Vec::new();
        for (name, e) in map.iter() {
            for (ordinal, sa) in &e.shards {
                if sa
                    .primary
                    .as_ref()
                    .is_some_and(|p| self.owner_confidently_dead(&p.0, now_ms))
                {
                    out.push((name.clone(), Unit::Shard(*ordinal)));
                }
            }
            for (window, wa) in &e.windows {
                if wa
                    .assignment
                    .primary
                    .as_ref()
                    .is_some_and(|p| self.owner_confidently_dead(&p.0, now_ms))
                {
                    out.push((name.clone(), Unit::Window(*window)));
                }
            }
        }
        out
    }

    /// **Dead-owner sweep** (HA-D2): re-place every unit whose primary is confidently dead through
    /// the SAME path a write-driven resolve takes ([`resolve_unit_holders`](Self::resolve_unit_holders)
    /// — idempotent, entitlement-aware, persist + notify), so quiescent units on a dead node become
    /// available again without waiting for a write. Returns the number of primaries moved.
    ///
    /// Respects the [liveness grace window](Self::placement_grace_active) and no-ops on an empty pool.
    /// The **caller** gates on leadership (only the leader sweeps); a demotion race is still safe — the
    /// persist boundary refuses non-leader writes (`NotLeader`).
    pub fn sweep_dead_primaries(
        &self,
        replication_factor: usize,
        entitled_nodes: usize,
        now_ms: i64,
    ) -> Result<usize> {
        if self.live_nodes(now_ms).is_empty() {
            return Ok(0); // nowhere to re-place (also: nothing heartbeats ⇒ nothing is *dead*)
        }
        let mut swept = 0usize;
        for (index, unit) in self.dead_primary_units(now_ms) {
            match self.resolve_unit_holders(
                &index,
                unit,
                replication_factor,
                entitled_nodes,
                now_ms,
            ) {
                Ok(h) if h.moved => swept += 1,
                Ok(_) => {}
                // The index was dropped, or a concurrent resolve already handled it — skip.
                Err(RegistryError::NotFound(_)) => {}
                // The pool emptied mid-sweep: stop, retry next tick.
                Err(RegistryError::NoLiveNode { .. }) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(swept)
    }

    /// The units that need placement work at `now_ms` — the **placement sweeper's** work-list (HA-D8 /
    /// 357.26), which lets the pool self-organize over each index's declared units. A unit needs work
    /// when:
    ///
    /// - **Hash index** — for every ordinal `0..shard_count`: it has no primary yet (**place one**,
    ///   round-robin least-loaded, so a node need not have pre-built it), or it's placed but has fewer
    ///   than `R` live holders (**top up replicas** / replace a dead one).
    /// - **Windowed index** — windows are created on demand by the connector's write-driven resolve,
    ///   so this only **tops up replicas** for windows *already* placed; it never creates a window.
    ///
    /// Live-holder count is the primary + replicas whose node still heartbeats. A *never-placed* unit
    /// is placed as soon as the brief [initial settle](Self::initial_placement_settling) clears; only
    /// *re-placement* / replica top-up of an already-held unit waits out the full
    /// [grace window](Self::placement_grace_active) (HA-D5 anti-flap). Read-only.
    pub fn units_needing_placement(
        &self,
        replication_factor: usize,
        now_ms: i64,
    ) -> Vec<(String, Unit)> {
        let r = replication_factor.max(1);
        // Hold everything for the brief initial settle so co-booting nodes register first.
        if self.initial_placement_settling(now_ms) {
            return Vec::new();
        }
        // Past the settle: place never-placed primaries even inside the grace window; only
        // re-placement / replica top-up of an already-held unit still waits the full grace.
        let grace = self.placement_grace_active(now_ms);
        let live: std::collections::BTreeSet<String> =
            self.live_nodes(now_ms).into_iter().collect();
        let live_holders =
            |sa: &ShardAssignment| sa.nodes().iter().filter(|n| live.contains(&n.0)).count();
        let map = self.read_map();
        let mut out = Vec::new();
        for (name, e) in map.iter() {
            if e.definition.windowing.is_none() {
                // Hash: proactively place + replicate every declared ordinal.
                for ordinal in 0..e.definition.shard_count {
                    let needs = match e.shards.get(&ordinal) {
                        None => true,                               // never placed → needs a primary
                        Some(sa) if sa.primary.is_none() => true,   // no primary → place one
                        Some(sa) => !grace && live_holders(sa) < r, // under-replicated → not during grace
                    };
                    if needs {
                        out.push((name.clone(), Unit::Shard(ordinal)));
                    }
                }
            } else {
                // Windowed: only top up replicas for windows the connector already placed (never
                // proactively created), and not while the grace window suppresses re-placement.
                for (window, wa) in &e.windows {
                    if !grace && wa.assignment.primary.is_some() && live_holders(&wa.assignment) < r
                    {
                        out.push((name.clone(), Unit::Window(*window)));
                    }
                }
            }
        }
        out
    }

    /// **Placement sweep** (HA-D8 / 357.26): drive every unit in
    /// [`units_needing_placement`](Self::units_needing_placement) to `R` live holders through the SAME
    /// idempotent path a write-driven resolve takes ([`resolve_unit_holders`](Self::resolve_unit_holders)
    /// — entitlement-aware, least-loaded, prunes dead replicas, persist + push). Places a primary for
    /// each declared hash ordinal that has none and fills replicas for placed units, so read HA does
    /// not depend on write activity. Returns the number of units whose holder set changed.
    ///
    /// The counterpart to [`sweep_dead_primaries`](Self::sweep_dead_primaries) (which promotes on a
    /// dead *primary*). Places never-placed primaries once the brief initial settle clears, respects
    /// the full grace window for *re-placement*, and no-ops on an empty pool. The **caller** gates on
    /// leadership; the persist boundary refuses a non-leader write (`NotLeader`), so a demotion race is
    /// safe.
    pub fn ensure_placement(
        &self,
        replication_factor: usize,
        entitled_nodes: usize,
        now_ms: i64,
    ) -> Result<usize> {
        if self.live_nodes(now_ms).is_empty() {
            return Ok(0);
        }
        let mut topped = 0usize;
        for (index, unit) in self.units_needing_placement(replication_factor, now_ms) {
            match self.resolve_unit_holders(
                &index,
                unit,
                replication_factor,
                entitled_nodes,
                now_ms,
            ) {
                Ok(h) if h.changed => topped += 1,
                Ok(_) => {}
                // The index was dropped, or a concurrent resolve already handled it — skip.
                Err(RegistryError::NotFound(_)) => {}
                // The pool emptied mid-sweep: stop, retry next tick.
                Err(RegistryError::NoLiveNode { .. }) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(topped)
    }
}

/// The mutable [`ShardAssignment`] for a [`Unit`] within an [`IndexEntry`], creating an empty one on
/// first placement — the write counterpart to [`unit_primary`]. A window's assignment is nested under
/// its [`WindowAssignment`]; a shard's is the entry directly.
fn unit_assignment_mut(entry: &mut IndexEntry, unit: Unit) -> &mut ShardAssignment {
    match unit {
        Unit::Shard(ordinal) => entry.shards.entry(ordinal).or_default(),
        Unit::Window(window) => &mut entry.windows.entry(window).or_default().assignment,
    }
}

/// The read-only [`ShardAssignment`] for a [`Unit`], if it exists — the read counterpart to
/// [`unit_assignment_mut`].
fn unit_assignment(entry: &IndexEntry, unit: Unit) -> Option<&ShardAssignment> {
    match unit {
        Unit::Shard(ordinal) => entry.shards.get(&ordinal),
        Unit::Window(window) => entry.windows.get(&window).map(|w| &w.assignment),
    }
}

/// A fingerprint of the catalog's **placement state** — every `(index, unit, primary, replicas)`
/// tuple — so the [placement listener](Registry::set_placement_listener) fires exactly when holder
/// sets change and stays quiet for non-placement persists (tokens, aliases, zone-maps, …).
/// `BTreeMap` iteration is deterministic, so equal placements hash equal.
fn placement_hash(map: &BTreeMap<String, IndexEntry>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for (name, e) in map.iter() {
        for (ordinal, sa) in &e.shards {
            (name, ordinal, &sa.primary, &sa.replicas).hash(&mut h);
        }
        for (window, wa) in &e.windows {
            (
                name,
                window,
                &wa.assignment.primary,
                &wa.assignment.replicas,
            )
                .hash(&mut h);
        }
    }
    h.finish()
}

/// The least-loaded live node not in `exclude`, or `None` when every candidate is excluded. `load` is
/// a `BTreeMap`, so it iterates endpoints sorted and `min_by_key` returns the first minimum — the
/// smallest endpoint on a tie, keeping placement deterministic.
fn pick_least_loaded(load: &BTreeMap<String, usize>, exclude: &BTreeSet<String>) -> Option<String> {
    load.iter()
        .filter(|(ep, _)| !exclude.contains(*ep))
        .min_by_key(|(_, c)| **c)
        .map(|(ep, _)| ep.clone())
}

/// The current primary owner (if any) of a [`Unit`] within an [`IndexEntry`] — the shared read both
/// the idempotency check and the placement write key off.
fn unit_primary(entry: &IndexEntry, unit: Unit) -> Option<String> {
    match unit {
        Unit::Shard(ordinal) => entry
            .shards
            .get(&ordinal)
            .and_then(|s| s.primary.as_ref())
            .map(|n| n.0.clone()),
        Unit::Window(window) => entry
            .windows
            .get(&window)
            .and_then(|w| w.assignment.primary.as_ref())
            .map(|n| n.0.clone()),
    }
}

impl Drop for Registry {
    /// Flush any activity events a debounce window left in memory so a graceful shutdown doesn't
    /// lose the tail of a burst. Best-effort; runs before `_lock`'s flock releases.
    fn drop(&mut self) {
        let dirty = self
            .activity_flush
            .get_mut()
            .map(|f| f.dirty)
            .unwrap_or(false);
        if dirty {
            let log = self.activity.get_mut().unwrap_or_else(|e| e.into_inner());
            if let Err(e) = self.backend.persist_activity(log) {
                tracing::warn!(error = %e, "failed to flush activity log on shutdown");
            }
        }
    }
}

/// Match an index `pattern` (a `*`-glob like `events-*` / `*-2025` / `a*b`) against `name`.
/// `*` matches any (possibly empty) run of characters; there is no `?`. Index names are ASCII
/// identifiers, so byte-slicing on the literal segments is safe. Public so clients filtering a
/// listed index set (e.g. CLI retention) match patterns the same way `resolve` does.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == name; // no wildcard → exact
    }
    let (first, last) = (parts[0], parts[parts.len() - 1]);
    // The literal before the first `*` must be a prefix; the one after the last `*`, a suffix.
    if !name.starts_with(first) || !name.ends_with(last) {
        return false;
    }
    let mut pos = first.len();
    let end = name.len() - last.len();
    if pos > end {
        return false; // prefix and suffix overlap
    }
    // Interior literals must appear in order within the remaining window.
    for mid in &parts[1..parts.len() - 1] {
        if mid.is_empty() {
            continue;
        }
        match name[pos..end].find(mid) {
            Some(i) => pos += i + mid.len(),
            None => return false,
        }
    }
    true
}

/// Epoch milliseconds.
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::{PersistedState, RegistryBackend, RegistrySnapshot};
    use growlerdb_core::{IndexDefinition, SourceField, SourceSchema, SourceType};
    use std::sync::{Arc, Mutex};

    /// A non-file [`RegistryBackend`] backed by a shared in-memory store — proves the seam is real:
    /// the registry drives its whole lifecycle over a backend that touches no disk, and two
    /// registries over the *same* store round-trip (a simulated "reopen" without a JSON file). This
    /// is the shape a replicated store (D51) plugs in as.
    #[derive(Default)]
    struct SharedStore {
        indexes: BTreeMap<String, IndexEntry>,
        aliases: BTreeMap<String, BTreeSet<String>>,
        saved_queries: BTreeMap<String, SavedQuery>,
        role_bindings: BTreeMap<String, Vec<String>>,
        tokens: BTreeMap<String, ApiToken>,
        credentials: BTreeMap<String, String>,
        index_bindings: BTreeMap<String, Vec<String>>,
        activity: BTreeMap<String, Vec<ActivityEvent>>,
        session_epochs: BTreeMap<String, i64>,
    }

    #[derive(Clone, Default)]
    struct InMemoryBackend(Arc<Mutex<SharedStore>>);

    impl RegistryBackend for InMemoryBackend {
        fn load(&self) -> Result<PersistedState> {
            let s = self.0.lock().unwrap();
            Ok(PersistedState {
                indexes: s.indexes.clone(),
                aliases: s.aliases.clone(),
                saved_queries: s.saved_queries.clone(),
                role_bindings: s.role_bindings.clone(),
                tokens: s.tokens.clone(),
                credentials: s.credentials.clone(),
                index_bindings: s.index_bindings.clone(),
                activity: s.activity.clone(),
                session_epochs: s.session_epochs.clone(),
            })
        }
        fn persist_registry(&self, snap: RegistrySnapshot) -> Result<()> {
            let mut s = self.0.lock().unwrap();
            s.indexes = snap.indexes;
            s.aliases = snap.aliases;
            s.saved_queries = snap.saved_queries;
            s.role_bindings = snap.role_bindings;
            s.tokens = snap.tokens;
            s.credentials = snap.credentials;
            s.index_bindings = snap.index_bindings;
            Ok(())
        }
        fn persist_activity(&self, activity: &BTreeMap<String, Vec<ActivityEvent>>) -> Result<()> {
            self.0.lock().unwrap().activity = activity.clone();
            Ok(())
        }
        fn persist_sessions(&self, sessions: &BTreeMap<String, i64>) -> Result<()> {
            self.0.lock().unwrap().session_epochs = sessions.clone();
            Ok(())
        }
    }

    #[test]
    fn registry_runs_over_a_custom_backend_and_round_trips() {
        // The whole registry lifecycle works over a non-file backend, and a second registry over the
        // SAME store loads the first's writes — the seam a replicated backend (D51) plugs into.
        let store = InMemoryBackend::default();
        {
            let reg = Registry::with_backend(Box::new(store.clone())).unwrap();
            reg.create(resolved("docs")).unwrap();
            reg.activate("docs").unwrap();
            reg.set_alias("d", ["docs"]).unwrap();
            reg.set_credential("alice", "pw").unwrap();
            reg.create_token(ApiToken {
                id: "tok1".into(),
                label: "l".into(),
                prefix: "gdb".into(),
                hash: "H1".into(),
                roles: vec!["reader".into()],
                owner: "alice".into(),
                created_at_ms: 0,
                expires_at_ms: None,
            })
            .unwrap();
            reg.record_activity("docs", "index.created", "created");
        }
        // "Reopen" over the same in-memory store: every persisted map is loaded back, and the derived
        // token-by-hash index is rebuilt so find_token works.
        let reg2 = Registry::with_backend(Box::new(store.clone())).unwrap();
        assert_eq!(reg2.get("docs").unwrap().status, IndexStatus::Active);
        assert_eq!(reg2.alias_targets("d"), Some(vec!["docs".to_string()]));
        assert!(reg2.verify_credential("alice", "pw"));
        assert_eq!(reg2.find_token("H1").unwrap().id, "tok1");
        assert_eq!(reg2.list_activity("docs", 0).len(), 1);

        // A custom backend does NOT enforce the file's single-writer flock — two live registries over
        // one store is exactly what a replicated backend supports (single-writer becomes the store's
        // concern, not the file lock's).
        let reg3 = Registry::with_backend(Box::new(store)).unwrap();
        assert!(reg3.get("docs").is_some());
    }

    /// An [`InMemoryBackend`] with fault + leadership toggles: persist/load failures on demand and
    /// the standby → lock → confirm promotion protocol — the shapes the HA fixes are tested against
    /// without a real store.
    #[derive(Clone)]
    struct FlakyBackend(Arc<FlakyInner>);

    struct FlakyInner {
        store: InMemoryBackend,
        fail_registry: std::sync::atomic::AtomicBool,
        fail_sessions: std::sync::atomic::AtomicBool,
        fail_load: std::sync::atomic::AtomicBool,
        lock_available: std::sync::atomic::AtomicBool,
        leader: std::sync::atomic::AtomicBool,
    }

    impl FlakyBackend {
        fn new(store: InMemoryBackend, leader: bool) -> Self {
            Self(Arc::new(FlakyInner {
                store,
                fail_registry: false.into(),
                fail_sessions: false.into(),
                fail_load: false.into(),
                lock_available: false.into(),
                leader: leader.into(),
            }))
        }
        fn set(flag: &std::sync::atomic::AtomicBool, v: bool) {
            flag.store(v, std::sync::atomic::Ordering::SeqCst);
        }
    }

    impl RegistryBackend for FlakyBackend {
        fn load(&self) -> Result<PersistedState> {
            if self.0.fail_load.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(RegistryError::Backend("injected load failure".into()));
            }
            self.0.store.load()
        }
        fn persist_registry(&self, snap: RegistrySnapshot) -> Result<()> {
            if !self.is_leader() {
                return Err(RegistryError::NotLeader("standby refuses writes".into()));
            }
            if self
                .0
                .fail_registry
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(RegistryError::Backend("injected persist failure".into()));
            }
            self.0.store.persist_registry(snap)
        }
        fn persist_activity(&self, activity: &BTreeMap<String, Vec<ActivityEvent>>) -> Result<()> {
            self.0.store.persist_activity(activity)
        }
        fn persist_sessions(&self, sessions: &BTreeMap<String, i64>) -> Result<()> {
            if self
                .0
                .fail_sessions
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(RegistryError::Backend("injected sessions failure".into()));
            }
            self.0.store.persist_sessions(sessions)
        }
        fn try_become_leader(&self) -> Result<bool> {
            Ok(self
                .0
                .lock_available
                .load(std::sync::atomic::Ordering::SeqCst))
        }
        fn confirm_leadership(&self) {
            Self::set(&self.0.leader, true);
        }
        fn resign_leadership(&self) {
            Self::set(&self.0.leader, false);
        }
        fn is_leader(&self) -> bool {
            self.0.leader.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[test]
    fn failed_persist_rolls_back_the_in_memory_change() {
        // HA-C3: a failed persist must not leave the mutation in memory, or the next successful
        // mutation's full snapshot silently commits it.
        let flaky = FlakyBackend::new(InMemoryBackend::default(), true);
        let reg = Registry::with_backend(Box::new(flaky.clone())).unwrap();
        reg.create(resolved("docs")).unwrap();

        FlakyBackend::set(&flaky.0.fail_registry, true);
        assert!(reg.create(resolved("logs")).is_err());
        assert!(
            reg.get("logs").is_none(),
            "the failed create was rolled back from memory"
        );

        FlakyBackend::set(&flaky.0.fail_registry, false);
        reg.create(resolved("extra")).unwrap();
        // The store never saw the failed change ride out on the later successful snapshot.
        let check = Registry::with_backend(Box::new(flaky.clone())).unwrap();
        assert!(check.get("docs").is_some());
        assert!(check.get("extra").is_some());
        assert!(
            check.get("logs").is_none(),
            "the failed change must never become durable"
        );
    }

    #[test]
    fn unrollbackable_persist_failure_refuses_writes_until_resynced() {
        // If the rollback restore ALSO fails (store unreachable), persists latch off: the next
        // attempt restores first and fails retryably, rather than silently committing the stale
        // change from memory.
        let flaky = FlakyBackend::new(InMemoryBackend::default(), true);
        let reg = Registry::with_backend(Box::new(flaky.clone())).unwrap();
        reg.create(resolved("docs")).unwrap();

        FlakyBackend::set(&flaky.0.fail_registry, true);
        FlakyBackend::set(&flaky.0.fail_load, true);
        assert!(reg.create(resolved("logs")).is_err());

        // Store is healthy again: the first write is refused (it restores memory — sweeping both
        // the stale change and its own — and reports retryable), the retry succeeds.
        FlakyBackend::set(&flaky.0.fail_registry, false);
        FlakyBackend::set(&flaky.0.fail_load, false);
        assert!(reg.create(resolved("extra")).is_err());
        assert!(
            reg.get("logs").is_none(),
            "stale change swept by the restore"
        );
        assert!(reg.get("extra").is_none(), "refused write swept too");
        reg.create(resolved("extra")).unwrap();

        let check = Registry::with_backend(Box::new(flaky.clone())).unwrap();
        assert!(
            check.get("logs").is_none(),
            "the stale change never persisted"
        );
        assert!(check.get("extra").is_some());
    }

    #[test]
    fn standby_refusal_is_not_leader_and_leaves_memory_clean() {
        // HA-C6: a standby's refused write surfaces as NotLeader (FAILED_PRECONDITION at the gRPC
        // seam, not Internal) and must not linger in the standby's memory serving stale reads.
        let flaky = FlakyBackend::new(InMemoryBackend::default(), false);
        let reg = Registry::with_backend(Box::new(flaky.clone())).unwrap();
        assert!(!reg.is_leader());
        assert!(matches!(
            reg.create(resolved("phantom")),
            Err(RegistryError::NotLeader(_))
        ));
        assert!(
            reg.get("phantom").is_none(),
            "the refused write was rolled back from standby memory"
        );
    }

    #[test]
    fn promotion_reloads_before_confirming_writership() {
        // HA-C2: acquire lock → reload → confirm. A failed reload leaves the replica a standby;
        // a successful promotion already sees the dead leader's last writes before any write can
        // be accepted.
        let store = InMemoryBackend::default();
        let leader =
            Registry::with_backend(Box::new(FlakyBackend::new(store.clone(), true))).unwrap();
        leader.create(resolved("docs")).unwrap();

        let flaky = FlakyBackend::new(store, false);
        let standby = Registry::with_backend(Box::new(flaky.clone())).unwrap();
        leader.create(resolved("late-write")).unwrap(); // after the standby's load

        // Lock is winnable but the reload fails → stays standby, no writership confirmed.
        FlakyBackend::set(&flaky.0.lock_available, true);
        FlakyBackend::set(&flaky.0.fail_load, true);
        assert!(standby.try_become_leader().is_err());
        assert!(
            !standby.is_leader(),
            "a failed promotion reload must not confirm writership"
        );

        // Reload healthy → promoted, and the dead leader's last write is already visible.
        FlakyBackend::set(&flaky.0.fail_load, false);
        assert!(standby.try_become_leader().unwrap());
        assert!(standby.is_leader());
        assert!(
            standby.get("late-write").is_some(),
            "promotion reloaded before confirming writership"
        );
    }

    #[test]
    fn revoke_sessions_persist_failure_is_a_hard_error_and_rolls_back() {
        // HA-C4: a revocation that isn't durable isn't a revocation — the epoch bump must not stay
        // in memory (it would silently un-revoke on failover) and the caller must see the failure.
        let flaky = FlakyBackend::new(InMemoryBackend::default(), true);
        let reg = Registry::with_backend(Box::new(flaky.clone())).unwrap();

        FlakyBackend::set(&flaky.0.fail_sessions, true);
        assert!(reg.revoke_sessions("alice").is_err());
        assert_eq!(
            reg.session_epoch("alice"),
            0,
            "the failed bump was rolled back"
        );
        // The failure propagates through the mutations that revoke as a side effect.
        assert!(reg.set_user_roles("alice", vec!["admin".into()]).is_err());

        FlakyBackend::set(&flaky.0.fail_sessions, false);
        reg.revoke_sessions("alice").unwrap();
        assert!(reg.session_epoch("alice") > 0);
        // Durable: a fresh replica over the same store sees the revocation.
        let check = Registry::with_backend(Box::new(flaky.clone())).unwrap();
        assert!(check.session_epoch("alice") > 0);
    }

    #[test]
    fn scale_limit_caps_new_nodes_but_allows_reheartbeats() {
        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::open(dir.path().join("registry.json")).unwrap();
        let t = 1_000_000;
        // Limit of 2 for the test: two distinct nodes are admitted.
        assert!(reg.register_node_capped("node-a:50051", t, 2).is_ok());
        assert!(reg.register_node_capped("node-b:50051", t, 2).is_ok());
        assert_eq!(reg.distinct_live_nodes(t), 2);
        // A third *new* node is rejected (returns the current count).
        assert_eq!(reg.register_node_capped("node-c:50051", t, 2), Err(2));
        // But an already-live node re-heartbeats fine — no new capacity, never disrupt the cluster.
        assert!(reg.register_node_capped("node-a:50051", t + 100, 2).is_ok());
        // Re-heartbeats never grow the count (the pool is a flat endpoint set — a node isn't
        // double-counted no matter how many indexes' units it serves).
        assert!(reg.register_node_capped("node-a:50051", t + 100, 2).is_ok());
        assert_eq!(reg.distinct_live_nodes(t + 100), 2);
    }

    #[test]
    fn credentials_hash_verify_and_persist() {
        // Built-in credentials are salted-argon2-hashed, verified, persisted, and the plaintext is
        // never written to disk.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        {
            let reg = Registry::open(&path).unwrap();
            assert!(!reg.has_credentials());
            reg.set_credential("alice", "s3cr3t-pw").unwrap();
            assert!(reg.has_credentials());
            assert!(reg.verify_credential("alice", "s3cr3t-pw"));
            assert!(!reg.verify_credential("alice", "wrong"));
            assert!(!reg.verify_credential("bob", "s3cr3t-pw")); // unknown subject
            let raw = std::fs::read_to_string(&path).unwrap();
            assert!(
                !raw.contains("s3cr3t-pw"),
                "plaintext password must never be persisted"
            );
        } // drop releases the exclusive flock
          // Reopen: the credential survives a restart; remove clears it.
        let reg2 = Registry::open(&path).unwrap();
        assert!(reg2.verify_credential("alice", "s3cr3t-pw"));
        reg2.remove_credential("alice").unwrap();
        assert!(!reg2.verify_credential("alice", "s3cr3t-pw"));
        assert!(!reg2.has_credentials());
    }

    #[test]
    fn activity_debounces_bursts_but_persists_isolated_events_and_survives_reload() {
        // An isolated activity event flushes to the sidecar immediately (durability preserved), a
        // same-window burst coalesces (later events aren't fsynced per-event), and the debounced
        // tail is flushed on graceful shutdown so nothing is lost across a restart.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        let activity_path = dir.path().join("activity.json");
        {
            let reg = Registry::open(&path).unwrap();
            // First event: no prior flush this session → written immediately.
            reg.record_activity("docs", "index.created", "index `docs` created");
            let on_disk = std::fs::read_to_string(&activity_path).unwrap();
            assert!(
                on_disk.contains("index.created"),
                "an isolated event must be durable immediately"
            );
            // Burst within the debounce window: coalesced, so the sidecar still holds only the first.
            for i in 0..4 {
                reg.record_activity("docs", "reshard", format!("resharded pass {i}"));
            }
            let on_disk = std::fs::read_to_string(&activity_path).unwrap();
            assert!(
                !on_disk.contains("resharded pass 3"),
                "a same-window burst must coalesce, not fsync per event"
            );
            // In memory the full history is always current regardless of flush timing.
            assert_eq!(reg.list_activity("docs", 0).len(), 5);
        } // drop → graceful-shutdown flush of the coalesced tail
        let reg2 = Registry::open(&path).unwrap();
        let events = reg2.list_activity("docs", 0);
        assert_eq!(events.len(), 5, "the debounced tail must survive a restart");
        assert!(events
            .iter()
            .any(|e| e.message.contains("resharded pass 3")));
    }

    #[test]
    fn persist_snapshot_captures_every_map_across_mutations() {
        // Each mutation persists the FULL snapshot (all maps), not just the one it changed — so
        // interleaved mutations to different maps all survive a restart.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        {
            let reg = Registry::open(&path).unwrap();
            reg.create(resolved("docs")).unwrap(); // indexes
            reg.set_alias("d", ["docs"]).unwrap(); // aliases
            reg.set_user_roles("alice", vec!["admin".into()]).unwrap(); // role_bindings
            reg.set_credential("alice", "pw").unwrap(); // credentials
            reg.set_user_indexes("alice", vec!["docs".into(), "catalog".into()])
                .unwrap(); // index_bindings
        }
        let reg2 = Registry::open(&path).unwrap();
        assert!(reg2.get("docs").is_some(), "index survived");
        assert_eq!(
            reg2.alias_targets("d"),
            Some(vec!["docs".to_string()]),
            "alias survived"
        );
        assert_eq!(
            reg2.roles_for("alice"),
            vec!["admin".to_string()],
            "binding survived"
        );
        assert!(reg2.verify_credential("alice", "pw"), "credential survived");
        assert_eq!(
            reg2.indexes_for("alice"),
            vec!["docs".to_string(), "catalog".to_string()],
            "index binding survived"
        );
    }

    #[test]
    fn index_bindings_scope_a_subject_and_revoke_sessions_on_change() {
        // A subject's index allowlist is de-duplicated, revokes outstanding sessions on change (so a
        // re-scoped session must re-authenticate), and clears when set empty.
        let dir = tempfile::tempdir().unwrap();
        let reg = Registry::open(dir.path().join("registry.json")).unwrap();
        assert!(
            reg.indexes_for("demo").is_empty(),
            "no binding = unrestricted"
        );
        reg.set_user_indexes("demo", vec!["docs".into(), "docs".into(), "catalog".into()])
            .unwrap();
        assert_eq!(
            reg.indexes_for("demo"),
            vec!["docs".to_string(), "catalog".to_string()],
            "de-duplicated, order-stable"
        );
        let epoch = reg.session_epoch("demo");
        assert!(epoch > 0, "a scope change bumps the session epoch");
        // Clearing the allowlist removes the binding (subject becomes unrestricted again).
        reg.set_user_indexes("demo", vec![]).unwrap();
        assert!(reg.indexes_for("demo").is_empty());
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

    #[test]
    fn window_placement_is_least_loaded_idempotent_and_deterministic() {
        // CP-driven placement spreads windows evenly over live nodes, deterministically, and is
        // idempotent for an already-placed live window.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("logs")).unwrap();
        let t0 = 1_000_000;
        for n in ["node-a", "node-b", "node-c"] {
            reg.register_node(n, t0);
        }
        // Placing 6 windows round-robins evenly across the 3 live nodes (least-loaded each step).
        let mut owners = Vec::new();
        for w in 0..6 {
            let (ep, created) = reg.resolve_window_owner("logs", w, usize::MAX, t0).unwrap();
            assert!(created, "first ask places window {w}");
            owners.push(ep);
        }
        for n in ["node-a", "node-b", "node-c"] {
            assert_eq!(
                owners.iter().filter(|e| *e == n).count(),
                2,
                "{n} should own 2 of 6 windows"
            );
        }
        // Deterministic: with all loads equal, the smallest endpoint wins the tie.
        assert_eq!(owners[0], "node-a");

        // Idempotent: re-resolving an assigned window with a live owner returns it, no re-placement.
        let (ep, created) = reg.resolve_window_owner("logs", 0, usize::MAX, t0).unwrap();
        assert_eq!(ep, "node-a");
        assert!(!created);
    }

    #[test]
    fn window_placement_reaps_a_dead_owner_and_re_places() {
        // A window whose owner stops heartbeating (past the TTL) is re-placed on a live node; with
        // no live node at all, placement errors so the caller retries.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("logs")).unwrap();
        let t0 = 1_000_000;
        reg.register_node("node-a", t0);
        let (ep, _) = reg.resolve_window_owner("logs", 7, usize::MAX, t0).unwrap();
        assert_eq!(ep, "node-a");

        // node-a goes silent; only node-b heartbeats, past node-a's TTL → node-a is dead.
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        reg.register_node("node-b", t1);
        let (ep, created) = reg.resolve_window_owner("logs", 7, usize::MAX, t1).unwrap();
        assert_eq!(
            ep, "node-b",
            "the dead owner's window re-places on the live node"
        );
        assert!(created, "re-placing a dead owner is a new assignment");
        // The durable window map reflects the move.
        assert_eq!(
            reg.window_map("logs").unwrap()[&7]
                .assignment
                .primary
                .as_ref()
                .unwrap()
                .0,
            "node-b"
        );

        // Once every node is stale, resolving a fresh window errors (caller retries on next heartbeat).
        let t2 = t1 + NODE_HEARTBEAT_TTL_MS + 1;
        assert!(matches!(
            reg.resolve_window_owner("logs", 99, usize::MAX, t2),
            Err(RegistryError::NoLiveNode { .. })
        ));
    }

    #[test]
    fn resolve_unit_holders_places_promotes_and_tops_up() {
        // D53: the CP assigns R holders per unit — one primary + R−1 replicas over the pool —
        // idempotent, with promote-on-primary-death + replica top-up (least-loaded, distinct).
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("idx")).unwrap();
        let t0 = 1_000_000;
        for n in ["node-a", "node-b", "node-c"] {
            reg.register_node(n, t0);
        }

        // R=1 ⇒ primary only (the D52 behavior), no replicas.
        let solo = reg
            .resolve_unit_holders("idx", Unit::Shard(9), 1, usize::MAX, t0)
            .unwrap();
        assert!(solo.changed);
        assert!(solo.replicas.is_empty());

        // R=3 ⇒ 1 primary + 2 replicas, all distinct live nodes.
        let h = reg
            .resolve_unit_holders("idx", Unit::Shard(0), 3, usize::MAX, t0)
            .unwrap();
        assert!(h.changed);
        assert_eq!(h.replicas.len(), 2);
        let mut all = h.replicas.clone();
        all.push(h.primary.clone());
        all.sort();
        assert_eq!(
            all,
            vec!["node-a", "node-b", "node-c"],
            "all three nodes hold the unit"
        );

        // Idempotent: with every holder live, a re-resolve changes nothing.
        let again = reg
            .resolve_unit_holders("idx", Unit::Shard(0), 3, usize::MAX, t0)
            .unwrap();
        assert!(!again.changed);
        assert_eq!(again.primary, h.primary);
        assert_eq!(again.replicas, h.replicas);

        // Kill the primary: its two replicas keep heartbeating; a warm replica is promoted, the dead
        // node drops out, and with only two live nodes the set holds 1 primary + 1 replica (< R).
        let dead = h.primary.clone();
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        for n in ["node-a", "node-b", "node-c"] {
            if n != dead {
                reg.register_node(n, t1);
            }
        }
        let after = reg
            .resolve_unit_holders("idx", Unit::Shard(0), 3, usize::MAX, t1)
            .unwrap();
        assert!(after.changed);
        assert_ne!(after.primary, dead, "the dead primary is replaced");
        assert!(
            h.replicas.contains(&after.primary),
            "a warm replica was promoted to primary"
        );
        assert!(
            !after.replicas.contains(&dead),
            "the dead node is pruned from replicas"
        );
        assert_eq!(
            after.replicas.len(),
            1,
            "only two live nodes ⇒ can't reach R=3"
        );
    }

    #[test]
    fn ensure_placement_tops_up_replicas_of_a_read_served_index() {
        // HA-D8 / 357.26: a placed PRIMARY with no write-driven resolve — a batch-built, read-served
        // index (no connector) — is topped up to R live holders by the sweep, so read HA does not
        // depend on write activity.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("idx")).unwrap();
        let t = 1_000_000;
        for n in ["node-a", "node-b"] {
            reg.register_node(n, t);
        }
        // Disarm the liveness grace so the sweep acts deterministically (the first heartbeat armed it).
        reg.set_placement_grace_anchor(-1);

        // Announce ONLY a primary for ordinal 0 (as register_served_index's announce_primaries does
        // for a pool node holding the ordinal hot) — no replicas yet.
        reg.announce_primaries("idx", &[0], "node-a", t, usize::MAX)
            .unwrap();
        // Under-replicated at R=2: primary node-a + 0 replicas = 1 live holder < 2.
        assert_eq!(
            reg.units_needing_placement(2, t),
            vec![("idx".to_string(), Unit::Shard(0))]
        );

        // The sweep tops it up to 2 holders (node-b becomes the replica) via the idempotent resolve.
        assert_eq!(reg.ensure_placement(2, usize::MAX, t).unwrap(), 1);
        let h = reg
            .resolve_unit_holders("idx", Unit::Shard(0), 2, usize::MAX, t)
            .unwrap();
        assert_eq!(h.primary, "node-a");
        assert_eq!(h.replicas, vec!["node-b".to_string()]);

        // Idempotent: fully replicated → empty work-list, the sweep no-ops.
        assert!(reg.units_needing_placement(2, t).is_empty());
        assert_eq!(reg.ensure_placement(2, usize::MAX, t).unwrap(), 0);

        // A dead replica re-opens the work-list: node-b stops heartbeating; only node-a is live, so
        // ordinal 0 is under-replicated again (and there's no spare node to top up onto).
        let t1 = t + NODE_HEARTBEAT_TTL_MS + 1;
        reg.register_node("node-a", t1); // node-b left to die
        reg.set_placement_grace_anchor(-1); // the re-register re-armed grace; disarm for determinism
        assert_eq!(
            reg.units_needing_placement(2, t1),
            vec![("idx".to_string(), Unit::Shard(0))]
        );
    }

    #[test]
    fn ensure_placement_proactively_places_primaries_round_robin_over_declared_ordinals() {
        // The self-organizing pool: an index is registered with its DEFINITION (shard_count) but NO
        // node announces holding any ordinal (nodes start empty — build/load on assignment). The sweep
        // places a primary for every declared ordinal round-robin across the pool, with no per-node
        // designation and no write ever arriving.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        let mut def = resolved("idx");
        def.shard_count = 4; // a 4-ordinal hash index
        reg.create(def).unwrap();
        let t = 1_000_000;
        for n in ["node-a", "node-b"] {
            reg.register_node(n, t);
        }
        reg.set_placement_grace_anchor(-1);

        // Nothing announced → all 4 ordinals are unplaced and need a primary.
        assert_eq!(
            reg.units_needing_placement(1, t),
            (0..4)
                .map(|o| ("idx".to_string(), Unit::Shard(o)))
                .collect::<Vec<_>>()
        );

        // The sweep places all four primaries (R=1); least-loaded ⇒ they spread 2/2 across the pool.
        assert_eq!(reg.ensure_placement(1, usize::MAX, t).unwrap(), 4);
        let owners: Vec<String> = (0..4)
            .map(|o| {
                reg.resolve_unit_holders("idx", Unit::Shard(o), 1, usize::MAX, t)
                    .unwrap()
                    .primary
            })
            .collect();
        let a = owners.iter().filter(|p| *p == "node-a").count();
        let b = owners.iter().filter(|p| *p == "node-b").count();
        assert_eq!(
            (a, b),
            (2, 2),
            "primaries balance round-robin across the pool"
        );

        // Idempotent once every ordinal is placed at R=1.
        assert!(reg.units_needing_placement(1, t).is_empty());
        assert_eq!(reg.ensure_placement(1, usize::MAX, t).unwrap(), 0);
    }

    #[test]
    fn cold_start_places_never_placed_primaries_after_the_settle_but_topup_waits_the_grace() {
        // Cold-start fast path: a fresh CP places never-placed primaries as soon as the brief
        // initial settle clears (a few seconds), NOT after the full ~30 s liveness grace — while
        // replica top-up of an already-held unit still waits the grace out (HA-D5 anti-flap).
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        let mut def = resolved("idx");
        def.shard_count = 2;
        reg.create(def).unwrap();
        let t0 = 1_000_000;
        for n in ["node-a", "node-b"] {
            reg.register_node(n, t0);
        }
        reg.set_placement_grace_anchor(t0); // a fresh CP arms the grace at its first heartbeat

        // (1) During the initial settle: place nothing, so co-booting nodes register first.
        assert!(reg.initial_placement_settling(t0));
        assert!(
            reg.units_needing_placement(1, t0).is_empty(),
            "no placement during the initial settle"
        );

        // (2) Settle cleared but STILL inside the grace window: never-placed primaries place now.
        let t1 = t0 + INITIAL_PLACEMENT_SETTLE_MS;
        assert!(!reg.initial_placement_settling(t1));
        assert!(
            reg.placement_grace_active(t1),
            "still within the grace window"
        );
        assert_eq!(
            reg.units_needing_placement(1, t1),
            vec![
                ("idx".to_string(), Unit::Shard(0)),
                ("idx".to_string(), Unit::Shard(1)),
            ],
            "never-placed primaries place during grace, once settled"
        );
        assert_eq!(reg.ensure_placement(1, usize::MAX, t1).unwrap(), 2);

        // (3) Now placed at R=1; asking R=2 makes them under-replicated — but replica top-up must
        //     NOT fire during the grace window.
        assert!(reg.placement_grace_active(t1));
        assert!(
            reg.units_needing_placement(2, t1).is_empty(),
            "replica top-up of a held unit waits out the grace"
        );

        // (4) Once the grace clears (nodes still heartbeating), top-up resumes.
        let t2 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        for n in ["node-a", "node-b"] {
            reg.register_node(n, t2); // re-heartbeat; does not re-arm the (already-armed) anchor
        }
        assert!(!reg.placement_grace_active(t2));
        assert_eq!(
            reg.units_needing_placement(2, t2).len(),
            2,
            "both units want a replica once the grace clears"
        );
    }

    #[test]
    fn classic_owner_is_live_for_the_sweeper_but_never_a_placement_target() {
        // A classic `serve --index events` node heartbeats via RegisterServedIndex only
        // (`touch_node_liveness`) — it never calls RegisterNode. It must (a) stay live so the
        // dead-owner sweeper doesn't steal its self-declared unit, yet (b) never be a target for
        // pool units it can't build/serve.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("docs")).unwrap(); // a pool (CP-placed) index
        reg.create(resolved("events")).unwrap(); // classic-served
        let cap = usize::MAX;
        let t0 = 1_000_000;
        reg.register_node("pool-a", t0); // serve-pool → placement-eligible
        reg.register_node("pool-b", t0);
        reg.touch_node_liveness("node-events", t0); // classic owner → liveness only
        reg.announce_primaries("events", &[0], "node-events", t0, cap)
            .unwrap();
        reg.set_placement_grace_anchor(-1); // deterministic placement

        // (b) The classic owner is LIVE but NOT placement-eligible, so a pool primary never lands on it.
        assert!(reg.live_nodes(t0).contains(&"node-events".to_string()));
        assert_eq!(reg.placement_nodes(t0), vec!["pool-a", "pool-b"]);
        let docs = reg
            .resolve_unit_owner("docs", Unit::Shard(0), cap, t0)
            .unwrap()
            .0;
        assert!(docs == "pool-a" || docs == "pool-b");
        assert_ne!(
            docs, "node-events",
            "a pool primary never lands on a classic node"
        );

        // (a) While it keeps heartbeating (past the grace), its unit is NOT swept.
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        for n in ["pool-a", "pool-b"] {
            reg.register_node(n, t1);
        }
        reg.touch_node_liveness("node-events", t1);
        reg.set_placement_grace_anchor(-1);
        assert_eq!(
            reg.resolve_unit_owner("events", Unit::Shard(0), cap, t1)
                .unwrap()
                .0,
            "node-events",
            "the live classic owner keeps its unit — not stolen by the sweeper"
        );

        // Liveness, not a permanent exemption: once it truly goes silent past the TTL, it drops out.
        let t2 = t1 + NODE_HEARTBEAT_TTL_MS + 1;
        assert!(!reg.live_nodes(t2).contains(&"node-events".to_string()));
    }

    #[test]
    fn universal_pool_places_shard_and_window_units_across_indexes() {
        // D52: one flat pool of interchangeable nodes serves UNITS (shards + windows) from MANY
        // indexes through one resolve_unit_owner path; load is counted cross-index, so placement
        // bin-packs the whole pool.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("hashidx")).unwrap();
        reg.create(resolved("winidx")).unwrap();
        let t0 = 1_000_000;
        // A node registers ONCE into the pool (not per-index) and is eligible for any index's units.
        for n in ["node-a", "node-b"] {
            reg.register_node(n, t0);
        }

        // Hash-shard units of one index and window units of another go through the SAME path and land
        // on the durable `shards` / `windows` maps respectively.
        let (s0, c0) = reg
            .resolve_unit_owner("hashidx", Unit::Shard(0), usize::MAX, t0)
            .unwrap();
        assert!(c0);
        let (s1, _) = reg
            .resolve_unit_owner("hashidx", Unit::Shard(1), usize::MAX, t0)
            .unwrap();
        let (w0, _) = reg
            .resolve_unit_owner("winidx", Unit::Window(100), usize::MAX, t0)
            .unwrap();
        let (w1, _) = reg
            .resolve_unit_owner("winidx", Unit::Window(200), usize::MAX, t0)
            .unwrap();
        // Cross-index least-loaded ⇒ the 4 units spread 2/2 over the pool (each node hosts units from
        // BOTH indexes — the multi-index-per-node property that kills node-per-index).
        let owners = [s0.clone(), s1, w0, w1];
        for n in ["node-a", "node-b"] {
            assert_eq!(
                owners.iter().filter(|e| *e == n).count(),
                2,
                "{n} should host 2 of the 4 units across both indexes"
            );
        }
        // Recorded on the right durable map per unit kind.
        assert_eq!(
            reg.shard_map("hashidx").unwrap()[&0]
                .primary
                .as_ref()
                .unwrap()
                .0,
            s0
        );
        assert!(reg.window_map("winidx").unwrap().contains_key(&100));

        // Idempotent for a live owner, and a shard unit re-places on a dead owner just like a window.
        let (again, created) = reg
            .resolve_unit_owner("hashidx", Unit::Shard(0), usize::MAX, t0)
            .unwrap();
        assert_eq!(again, s0);
        assert!(!created);
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        reg.register_node("node-c", t1); // only node-c is live now; a/b are stale
        let (moved, created) = reg
            .resolve_unit_owner("hashidx", Unit::Shard(0), usize::MAX, t1)
            .unwrap();
        assert_eq!(
            moved, "node-c",
            "dead owner's shard re-places on the live node"
        );
        assert!(created);
    }

    #[test]
    fn create_get_list_and_drop() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();

        reg.create(resolved("docs")).unwrap();
        reg.create(resolved("logs")).unwrap();

        // get returns the definition + Building status.
        let entry = reg.get("docs").unwrap();
        assert_eq!(entry.definition.name, "docs");
        assert_eq!(entry.status, IndexStatus::Building);
        assert!(reg.get("missing").is_none());

        // list is name-sorted with status.
        assert_eq!(
            reg.list(),
            vec![
                IndexSummary {
                    name: "docs".into(),
                    status: IndexStatus::Building
                },
                IndexSummary {
                    name: "logs".into(),
                    status: IndexStatus::Building
                },
            ]
        );

        // activate flips status.
        reg.activate("docs").unwrap();
        assert_eq!(reg.get("docs").unwrap().status, IndexStatus::Active);

        // drop returns the definition and removes it.
        let def = reg.drop_index("logs").unwrap();
        assert_eq!(def.name, "logs");
        assert!(reg.get("logs").is_none());
        assert_eq!(reg.list().len(), 1);
    }

    #[test]
    fn duplicate_create_and_missing_ops_error() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("docs")).unwrap();

        assert!(matches!(
            reg.create(resolved("docs")),
            Err(RegistryError::AlreadyExists(_))
        ));
        assert!(matches!(
            reg.drop_index("nope"),
            Err(RegistryError::NotFound(_))
        ));
        assert!(matches!(
            reg.activate("nope"),
            Err(RegistryError::NotFound(_))
        ));
    }

    #[test]
    fn registry_is_durable_across_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        {
            let reg = Registry::open(&path).unwrap();
            reg.create(resolved("docs")).unwrap();
            reg.activate("docs").unwrap();
            reg.create(resolved("logs")).unwrap();
        }
        // A fresh handle over the same file sees the persisted catalog + statuses.
        let reg = Registry::open(&path).unwrap();
        assert_eq!(reg.get("docs").unwrap().status, IndexStatus::Active);
        assert_eq!(reg.get("logs").unwrap().status, IndexStatus::Building);
        assert_eq!(reg.list().len(), 2);
    }

    /// Give `index` `n` ordinal shards (primary only), so `shards.len()` reflects the count.
    fn assign_shards(reg: &Registry, index: &str, n: u32) {
        for s in 0..n {
            reg.assign_primary(index, s, format!("node-{s}")).unwrap();
        }
    }

    #[test]
    fn bucket_map_defaults_to_legacy_and_persists_when_set() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        {
            let reg = Registry::open(&path).unwrap();
            reg.create(resolved("docs")).unwrap();
            assign_shards(&reg, "docs", 4);

            // No stored map ⇒ legacy routing.
            assert!(reg.bucket_map("docs").is_none());

            // Storing a balanced(4) map adopts buckets; it reads back identically. The CAS
            // expectation is `None` — no map stored yet.
            let map = BucketMap::balanced(4);
            reg.set_bucket_map("docs", None, &map).unwrap();
            assert_eq!(reg.bucket_map("docs"), Some(map.clone()));

            // CAS: a writer whose expectation is stale (still `None`, or a different map) is
            // refused — a concurrent placement op committed in between.
            let stale = reg.set_bucket_map("docs", None, &BucketMap::balanced(5));
            assert!(matches!(stale, Err(RegistryError::PlacementConflict(_))));
            let other = BucketMap::balanced(2);
            let stale = reg.set_bucket_map("docs", Some(&other), &BucketMap::balanced(5));
            assert!(matches!(stale, Err(RegistryError::PlacementConflict(_))));
            // The matching expectation commits.
            reg.set_bucket_map("docs", Some(&map), &BucketMap::balanced(5))
                .unwrap();
            reg.set_bucket_map("docs", Some(&BucketMap::balanced(5)), &map)
                .unwrap();
        }
        // Survives a reopen (persisted in registry.json).
        let reg = Registry::open(&path).unwrap();
        assert_eq!(reg.bucket_map("docs"), Some(BucketMap::balanced(4)));
        // Unknown index ⇒ None, not an error.
        assert!(reg.bucket_map("nope").is_none());
    }

    #[test]
    fn adopt_bucket_map_is_first_registration_only() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("docs")).unwrap();

        // First announce (a 2-shard index) adopts balanced(2).
        assert!(reg.adopt_bucket_map_if_absent("docs", 2).unwrap());
        assert_eq!(reg.bucket_map("docs"), Some(BucketMap::balanced(2)));

        // A re-announce is a no-op, and — the reshard-critical case — a growth build target
        // registering with the NEW total must not touch live routing before the cutover.
        assert!(!reg.adopt_bucket_map_if_absent("docs", 2).unwrap());
        assert!(!reg.adopt_bucket_map_if_absent("docs", 3).unwrap());
        assert_eq!(reg.bucket_map("docs"), Some(BucketMap::balanced(2)));
    }

    #[test]
    fn plan_reshard_from_legacy_grows_with_bounded_movement() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("docs")).unwrap();
        assign_shards(&reg, "docs", 4); // legacy index, 4 shards, no stored map

        // First reshard transparently adopts the balanced(4) map, then grows to 5.
        let plan = reg.plan_reshard("docs", 5).unwrap();
        assert_eq!(plan.map.shards(), 5);
        let counts = plan.map.counts();
        assert!(counts.iter().max().unwrap() - counts.iter().min().unwrap() <= 1);
        // Bounded: ~1/5 of buckets move (the new shard's share), nowhere near re-routing everything.
        assert!(plan.moved.len() < (growlerdb_core::routing::NUM_BUCKETS / 2) as usize);
        assert!(!plan.moved.is_empty());

        // Planning is read-only: the registry still routes legacy until a cutover applies it.
        assert!(reg.bucket_map("docs").is_none());
    }

    #[test]
    fn plan_reshard_unknown_index_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        assert!(matches!(
            reg.plan_reshard("nope", 4),
            Err(RegistryError::NotFound(_))
        ));
    }

    #[test]
    fn aliases_resolve_swap_prune_and_persist() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        {
            let reg = Registry::open(&path).unwrap();
            reg.create(resolved("events_v1")).unwrap();
            reg.create(resolved("events_v2")).unwrap();

            // Point a stable alias at v1; resolve an alias → members, an index → itself.
            reg.set_alias("events", ["events_v1"]).unwrap();
            assert_eq!(reg.resolve("events"), vec!["events_v1".to_string()]);
            assert_eq!(reg.resolve("events_v2"), vec!["events_v2".to_string()]);
            assert!(reg.resolve("ghost").is_empty());

            // Atomic reindex-and-swap: re-point the alias to v2 in one write.
            reg.set_alias("events", ["events_v2"]).unwrap();
            assert_eq!(reg.resolve("events"), vec!["events_v2".to_string()]);

            // A multi-target alias resolves to all members, sorted (search-and-merge precursor).
            reg.set_alias("all", ["events_v2", "events_v1"]).unwrap();
            assert_eq!(reg.resolve("all"), vec!["events_v1", "events_v2"]);
            assert_eq!(reg.list_aliases().len(), 2);

            // Validation.
            assert!(matches!(
                reg.set_alias("events_v1", ["events_v2"]),
                Err(RegistryError::AliasNameClash(_))
            ));
            assert!(matches!(
                reg.set_alias("bad", ["missing"]),
                Err(RegistryError::NotFound(_))
            ));
            assert!(matches!(
                reg.drop_alias("nope"),
                Err(RegistryError::AliasNotFound(_))
            ));
        }
        // Aliases persist across reopen.
        let reg = Registry::open(&path).unwrap();
        assert_eq!(reg.resolve("events"), vec!["events_v2".to_string()]);
        // Dropping a target prunes it from aliases; an alias left empty disappears.
        reg.drop_index("events_v2").unwrap();
        assert_eq!(reg.alias_targets("events"), None, "empty alias pruned");
        assert_eq!(
            reg.resolve("all"),
            vec!["events_v1".to_string()],
            "all keeps v1"
        );
    }

    #[test]
    fn glob_match_handles_star_patterns() {
        assert!(glob_match("events-*", "events-2025"));
        assert!(glob_match("events-*", "events-"));
        assert!(!glob_match("events-*", "events")); // the literal `events-` must be present
        assert!(!glob_match("events-*", "logs-2025"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*-2025", "events-2025"));
        assert!(!glob_match("*-2025", "events-2024"));
        assert!(glob_match("a*b", "axxb"));
        assert!(glob_match("a*b", "ab"));
        assert!(!glob_match("a*b", "axx"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "exacto"));
    }

    #[test]
    fn resolve_matches_index_patterns() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("events-2025-01")).unwrap();
        reg.create(resolved("events-2025-02")).unwrap();
        reg.create(resolved("logs-2025-01")).unwrap();

        // A pattern resolves to matching index names, sorted.
        assert_eq!(
            reg.resolve("events-*"),
            vec!["events-2025-01", "events-2025-02"]
        );
        assert_eq!(
            reg.resolve("*-2025-01"),
            vec!["events-2025-01", "logs-2025-01"]
        );
        // Matching nothing → empty; an exact index name short-circuits the pattern path.
        assert!(reg.resolve("nope-*").is_empty());
        assert_eq!(reg.resolve("events-2025-01"), vec!["events-2025-01"]);
    }

    #[test]
    fn shard_map_tracks_primary_and_replicas() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("docs")).unwrap();

        reg.assign_primary("docs", 0, "node-a").unwrap();
        reg.add_replica("docs", 0, "node-b").unwrap();
        reg.add_replica("docs", 0, "node-c").unwrap();
        reg.add_replica("docs", 0, "node-b").unwrap(); // idempotent
        reg.add_replica("docs", 0, "node-a").unwrap(); // the primary is never a replica

        let map = reg.shard_map("docs").unwrap();
        let a = &map[&0];
        assert_eq!(a.primary, Some(NodeId::from("node-a")));
        assert_eq!(
            a.replicas,
            vec![NodeId::from("node-b"), NodeId::from("node-c")]
        );
        assert!(a.is_assigned());
        assert_eq!(
            a.nodes(),
            vec![
                &NodeId::from("node-a"),
                &NodeId::from("node-b"),
                &NodeId::from("node-c")
            ]
        );

        // Assigning to a missing index errors.
        assert!(matches!(
            reg.assign_primary("nope", 0, "n"),
            Err(RegistryError::NotFound(_))
        ));
    }

    #[test]
    fn assign_primaries_batches_all_shards() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        let reg = Registry::open(&path).unwrap();
        reg.create(resolved("docs")).unwrap();

        // One call assigns every ordinal to the node (batched bring-up, one persist).
        reg.assign_primaries("docs", &[0, 1, 2], "node-a").unwrap();
        let entry = reg.get("docs").unwrap();
        for ord in 0..3u32 {
            assert_eq!(
                entry.shards.get(&ord).and_then(|a| a.primary.as_ref()),
                Some(&NodeId::from("node-a")),
                "shard {ord} assigned",
            );
        }

        // Empty is a no-op; a missing index errors.
        reg.assign_primaries("docs", &[], "x").unwrap();
        assert!(matches!(
            reg.assign_primaries("nope", &[0], "n"),
            Err(RegistryError::NotFound(_))
        ));

        // Persisted: reopening sees the assignments.
        drop(reg);
        let reg2 = Registry::open(&path).unwrap();
        assert!(reg2.get("docs").unwrap().shards[&2].primary.is_some());
    }

    #[test]
    fn promote_replica_on_primary_loss() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        {
            let reg = Registry::open(&path).unwrap();
            reg.create(resolved("docs")).unwrap();
            reg.assign_primary("docs", 0, "node-a").unwrap();
            reg.add_replica("docs", 0, "node-b").unwrap();
            reg.add_replica("docs", 0, "node-c").unwrap();

            // Primary node-a is lost; promote the first replica.
            reg.remove_node("docs", 0, &NodeId::from("node-a")).unwrap();
            let promoted = reg.promote_replica("docs", 0).unwrap();
            assert_eq!(promoted, NodeId::from("node-b"));
        }
        // Durable: the new assignment survives reopen — node-b primary, node-c replica.
        let reg = Registry::open(&path).unwrap();
        let a = reg.shard_map("docs").unwrap().remove(&0).unwrap();
        assert_eq!(a.primary, Some(NodeId::from("node-b")));
        assert_eq!(a.replicas, vec![NodeId::from("node-c")]);

        // Promoting with a primary still assigned is refused — fence it first.
        assert!(matches!(
            reg.promote_replica("docs", 0),
            Err(RegistryError::PrimaryStillAssigned { .. })
        ));

        // Clear the primary, then with no replica left promotion errors with NoReplica.
        reg.remove_node("docs", 0, &NodeId::from("node-c")).unwrap();
        reg.remove_node("docs", 0, &NodeId::from("node-b")).unwrap();
        assert!(matches!(
            reg.promote_replica("docs", 0),
            Err(RegistryError::NoReplica { .. })
        ));
    }

    #[test]
    fn promote_replica_fences_against_split_brain() {
        // The split-brain-avoidance precondition: with a live primary assigned, promote_replica
        // refuses — the lease driver must clear/fence the old primary first.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("docs")).unwrap();
        reg.assign_primary("docs", 0, "node-a").unwrap();
        reg.add_replica("docs", 0, "node-b").unwrap();

        // Primary still node-a → refused (no second primary created).
        assert!(matches!(
            reg.promote_replica("docs", 0),
            Err(RegistryError::PrimaryStillAssigned { .. })
        ));
        assert_eq!(
            reg.shard_map("docs").unwrap()[&0].primary,
            Some(NodeId::from("node-a")),
            "the live primary is untouched"
        );

        // After fencing the old primary, promotion succeeds.
        reg.remove_node("docs", 0, &NodeId::from("node-a")).unwrap();
        assert_eq!(
            reg.promote_replica("docs", 0).unwrap(),
            NodeId::from("node-b")
        );
    }

    #[test]
    fn open_is_single_writer() {
        // A second open of the same registry fails fast while the first holds the lock.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        let first = Registry::open(&path).unwrap();
        assert!(matches!(
            Registry::open(&path),
            Err(RegistryError::Locked(_))
        ));
        // Releasing the first lets a new open acquire the lock.
        drop(first);
        assert!(Registry::open(&path).is_ok());
    }

    #[test]
    fn locks_recover_from_poisoning() {
        // A panic while holding the write lock must not wedge the registry.
        use std::sync::Arc;
        let tmp = tempfile::tempdir().unwrap();
        let reg = Arc::new(Registry::open(tmp.path().join("registry.json")).unwrap());
        reg.create(resolved("docs")).unwrap();

        let r2 = reg.clone();
        let _ = std::thread::spawn(move || {
            let _guard = r2.indexes.write().unwrap();
            panic!("poison the lock");
        })
        .join();

        // Despite the poisoned lock, reads/writes still work (recover via into_inner).
        assert!(reg.get("docs").is_some());
        reg.create(resolved("logs")).unwrap();
        assert_eq!(reg.list().len(), 2);
    }

    #[test]
    fn persists_a_versioned_envelope() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        let reg = Registry::open(&path).unwrap();
        reg.create(resolved("docs")).unwrap();

        // On disk: a `{ version, indexes }` envelope, not a bare map.
        let raw: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(raw["version"], 1);
        assert!(raw["indexes"]["docs"].is_object());

        // Reopen parses the envelope back (drop the first to release the single-writer lock).
        drop(reg);
        assert!(Registry::open(&path).unwrap().get("docs").is_some());
    }

    #[test]
    fn falls_back_to_prev_on_a_corrupt_registry() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        let reg = Registry::open(&path).unwrap();
        reg.create(resolved("docs")).unwrap(); // first write: no .prev yet
        reg.create(resolved("logs")).unwrap(); // second write: .prev now holds {docs}
        drop(reg);

        // Corrupt the live file; the .prev copy is still a valid envelope holding {docs}.
        std::fs::write(&path, b"{ not valid json").unwrap();
        let reopened = Registry::open(&path).unwrap();
        assert!(reopened.get("docs").is_some()); // recovered from .prev
        assert!(reopened.get("logs").is_none()); // .prev predates the logs create
    }

    #[test]
    fn window_map_assigns_nodes_widens_bounds_and_persists() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        let reg = Registry::open(&path).unwrap();
        reg.create(resolved("events")).unwrap();

        // Assign two window shards to nodes + record their event-time zone-maps.
        reg.assign_window("events", 10, "node-a").unwrap();
        reg.set_window_bounds("events", 10, Some(200), Some(900))
            .unwrap();
        reg.assign_window("events", 11, "node-b").unwrap();
        reg.set_window_bounds("events", 11, Some(1000), Some(1100))
            .unwrap();
        // A late event widens window 10's bound down — never shrinks.
        reg.set_window_bounds("events", 10, Some(50), Some(300))
            .unwrap();

        let map = reg.window_map("events").unwrap();
        assert_eq!(map.len(), 2);
        let w10 = &map[&10];
        assert_eq!(w10.assignment.primary.as_ref().unwrap().0, "node-a");
        assert_eq!((w10.event_min, w10.event_max), (Some(50), Some(900))); // widened both ways
        assert_eq!(map[&11].assignment.primary.as_ref().unwrap().0, "node-b");

        // Assigning a window of a missing index errors; the map survives a reopen.
        assert!(reg.assign_window("nope", 0, "x").is_err());
        drop(reg);
        let reopened = Registry::open(&path).unwrap();
        let map = reopened.window_map("events").unwrap();
        assert_eq!(map[&10].event_min, Some(50));
        assert_eq!(map[&11].assignment.primary.as_ref().unwrap().0, "node-b");
    }

    #[test]
    fn tokens_are_found_by_hash_expire_and_survive_reload() {
        // O(1) hash lookup, expiry enforcement + pruning, and a derived index that's rebuilt on
        // open.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("registry.json");
        let mk = |id: &str, hash: &str, expires: Option<i64>| ApiToken {
            id: id.into(),
            label: "l".into(),
            prefix: "gdb".into(),
            hash: hash.into(),
            roles: vec!["reader".into()],
            owner: "svc".into(),
            created_at_ms: 0,
            expires_at_ms: expires,
        };
        let now = now_ms();
        {
            let reg = Registry::open(&path).unwrap();
            reg.create_token(mk("live", "H_LIVE", None)).unwrap();
            reg.create_token(mk("future", "H_FUT", Some(now + 60_000)))
                .unwrap();
            // O(1) lookup by hash returns the token; a bogus hash is None.
            assert_eq!(reg.find_token("H_LIVE").unwrap().id, "live");
            assert_eq!(reg.find_token("H_FUT").unwrap().id, "future");
            assert!(reg.find_token("nope").is_none());
            // An already-expired token never authenticates...
            reg.create_token(mk("stale", "H_STALE", Some(now - 1)))
                .unwrap();
            assert!(reg.find_token("H_STALE").is_none());
            // ...and the next create prunes it from the store (bounds growth).
            reg.create_token(mk("another", "H_OTHER", None)).unwrap();
            assert!(reg.list_tokens().iter().all(|t| t.id != "stale"));
            // Revoke drops it from the O(1) index too.
            reg.revoke_token("live").unwrap();
            assert!(reg.find_token("H_LIVE").is_none());
        }
        // The hash index is derived (not persisted) — it must be rebuilt on open so find still works.
        let reopened = Registry::open(&path).unwrap();
        assert_eq!(reopened.find_token("H_FUT").unwrap().id, "future");
        assert!(reopened.find_token("H_LIVE").is_none()); // revoked, so not persisted
    }

    #[test]
    fn concurrent_auth_mutations_dont_deadlock_and_persist() {
        // Every mutation holds only the map it changes (persist_snapshot re-reads the rest
        // off-lock), so hammering different auth maps concurrently completes without a lock-order
        // deadlock and each change is durably persisted.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        let reg = Registry::open(&path).unwrap();
        reg.create(resolved("docs")).unwrap(); // a target for set_alias
        std::thread::scope(|s| {
            for i in 0..8 {
                let reg = &reg;
                s.spawn(move || {
                    let who = format!("user{i}");
                    // Touch a different lock on each call; interleaving across threads exercises the
                    // acquisition order. A dead multi-lock hold in the reverse order would hang here.
                    reg.set_credential(&who, "pw").unwrap();
                    reg.set_user_roles(&who, vec!["reader".into()]).unwrap();
                    reg.create_token(ApiToken {
                        id: format!("tok{i}"),
                        label: "l".into(),
                        prefix: "gdb".into(),
                        hash: format!("H{i}"),
                        roles: vec!["reader".into()],
                        owner: who.clone(),
                        created_at_ms: 0,
                        expires_at_ms: None,
                    })
                    .unwrap();
                    reg.set_alias(&format!("a{i}"), ["docs"]).unwrap();
                    reg.remove_credential(&who).unwrap(); // removes the credential, keeps roles/token
                });
            }
        });
        drop(reg); // graceful shutdown flushes any debounced activity tail
                   // Reopen: roles, tokens and aliases all survived; credentials were removed.
        let reg2 = Registry::open(&path).unwrap();
        assert_eq!(reg2.list_tokens().len(), 8);
        assert_eq!(reg2.list_aliases().len(), 8);
        assert!(!reg2.has_credentials());
        for i in 0..8 {
            assert_eq!(
                reg2.roles_for(&format!("user{i}")),
                vec!["reader".to_string()]
            );
            assert_eq!(
                reg2.find_token(&format!("H{i}")).unwrap().id,
                format!("tok{i}")
            );
        }
    }

    #[test]
    fn heartbeat_ttl_has_reannounce_headroom() {
        // HA-D5: the liveness TTL must give at least 3 heartbeat opportunities (with ±20% jitter
        // room), or healthy nodes flap out of the pool — the two constants were once both 30 s.
        // Read through black_box so the sanity check survives constant-folding lints.
        let (ttl, reannounce) = (
            std::hint::black_box(NODE_HEARTBEAT_TTL_MS),
            std::hint::black_box(NODE_REANNOUNCE_INTERVAL_MS),
        );
        assert!(
            ttl >= 3 * reannounce,
            "TTL ({ttl} ms) must be ≥ 3× the re-announce interval ({reannounce} ms)"
        );
    }

    #[test]
    fn sweeper_re_places_dead_owners_idempotently_and_respects_grace() {
        // HA-D2: quiescent units on a dead node are re-placed by the sweep — no write required —
        // through the same resolve path; idempotent; suppressed while the grace window is active;
        // and dead-owner re-placement is never entitlement-bricked (availability first).
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("logs")).unwrap();
        let t0 = 1_000_000;
        reg.register_node("node-a", t0);
        for w in [1_i64, 2] {
            let (ep, _) = reg
                .resolve_unit_owner("logs", Unit::Window(w), usize::MAX, t0)
                .unwrap();
            assert_eq!(ep, "node-a");
        }
        // node-a dies; node-b heartbeats past its TTL. Simulate a fresh promotion: grace re-anchored.
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        reg.register_node("node-b", t1);
        reg.set_placement_grace_anchor(t1);
        assert_eq!(
            reg.sweep_dead_primaries(1, usize::MAX, t1).unwrap(),
            0,
            "no sweeping while owner liveness is unknown (grace)"
        );
        assert_eq!(
            reg.window_map("logs").unwrap()[&1].assignment.primary,
            Some(NodeId::from("node-a")),
            "grace leaves the laggard's assignment untouched"
        );
        // Grace over: both units move to the live node — even at entitled=1 (a re-placement is
        // existing capacity, never gated).
        reg.set_placement_grace_anchor(t1 - NODE_HEARTBEAT_TTL_MS - 1);
        assert_eq!(reg.sweep_dead_primaries(1, 1, t1).unwrap(), 2);
        for w in [1_i64, 2] {
            assert_eq!(
                reg.window_map("logs").unwrap()[&w].assignment.primary,
                Some(NodeId::from("node-b")),
                "window {w} re-placed on the live node"
            );
        }
        // Idempotent: a second sweep has nothing to move; the node count stayed constant.
        assert_eq!(reg.sweep_dead_primaries(1, 1, t1).unwrap(), 0);
        assert_eq!(reg.count_entitlement_nodes(t1), 1);
    }

    #[test]
    fn entitlement_check_is_atomic_under_concurrent_resolves() {
        // HA-D3b: check+place happen under ONE write-lock acquisition, so N racing resolves of
        // fresh units never spread primaries over more than `entitled` distinct nodes — past the
        // cap they pack onto already-primary-holding nodes. Without the atomic check two resolves
        // could each see "1 node used" and each light up a fresh node, blowing the cap.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Arc::new(Registry::open(tmp.path().join("registry.json")).unwrap());
        let t0 = 1_000_000;
        for i in 0..8 {
            reg.create(resolved(&format!("idx{i}"))).unwrap();
        }
        // A pool wide enough that, unchecked, the race could scatter primaries over 8 nodes.
        for i in 0..8 {
            reg.register_node(&format!("node-{i}"), t0);
        }
        let admitted = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|s| {
            for i in 0..8 {
                let (reg, admitted) = (&reg, &admitted);
                s.spawn(move || {
                    match reg.resolve_unit_owner(&format!("idx{i}"), Unit::Shard(0), 2, t0) {
                        Ok(_) => admitted.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
                        Err(e) => panic!("unexpected error: {e}"),
                    };
                });
            }
        });
        // Every resolve succeeds (packing never bricks), but only `entitled` = 2 distinct nodes
        // ever hold a primary despite the 8-way race.
        assert_eq!(admitted.load(std::sync::atomic::Ordering::SeqCst), 8);
        assert_eq!(reg.count_entitlement_nodes(t0), 2);
    }

    #[test]
    fn windowed_index_costs_constant_entitlement_over_time() {
        // The 4th-day scenario (HA-D3c): windows accumulate forever, but a windowed index never
        // lights up a second node — at the cap, new windows pack onto the node already primarying
        // the index instead of a fresh node.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("logs")).unwrap();
        let t0 = 1_000_000;
        reg.register_node("node-a", t0);
        reg.register_node("node-b", t0);
        for w in 0..10_i64 {
            let (ep, created) = reg
                .resolve_unit_owner("logs", Unit::Window(w), 1, t0)
                .unwrap();
            assert!(created, "window {w} placed");
            assert_eq!(ep, "node-a", "every window packs onto the one primary node");
        }
        assert_eq!(
            reg.count_entitlement_nodes(t0),
            1,
            "10 windows ⇒ still one primary-holding node"
        );
    }

    #[test]
    fn fresh_index_at_cap_packs_onto_an_already_primary_node() {
        // Option A (b): at the node cap, a *fresh index's* primary lands on a node that already
        // holds a primary (of any index) rather than lighting up an unused live node.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("docs")).unwrap();
        reg.create(resolved("logs")).unwrap();
        let t0 = 1_000_000;
        reg.register_node("node-a", t0);
        reg.register_node("node-b", t0);
        // docs' primary lights up node-a — the one node the cap of 1 allows.
        let (ep, _) = reg
            .resolve_unit_owner("docs", Unit::Shard(0), 1, t0)
            .unwrap();
        assert_eq!(ep, "node-a");
        // A *different* index at the cap must co-locate on node-a, never take the idle node-b.
        let (ep, _) = reg
            .resolve_unit_owner("logs", Unit::Shard(0), 1, t0)
            .unwrap();
        assert_eq!(
            ep, "node-a",
            "fresh index packs onto the already-primary node"
        );
        assert_eq!(reg.count_entitlement_nodes(t0), 1, "still one node");
    }

    #[test]
    fn r1_resolve_promotes_a_warm_replica_and_trims_excess() {
        // HA-D7: on primary death at R=1 a warm replica is PROMOTED (never a cold fresh placement
        // that ignores it), and lowering R trims the excess replicas.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("idx")).unwrap();
        let t0 = 1_000_000;
        for n in ["node-a", "node-b", "node-c"] {
            reg.register_node(n, t0);
        }
        // Disarm the startup grace so trims/promotions act on TTL liveness immediately.
        reg.set_placement_grace_anchor(t0 - NODE_HEARTBEAT_TTL_MS - 1);
        let h3 = reg
            .resolve_unit_holders("idx", Unit::Shard(0), 3, usize::MAX, t0)
            .unwrap();
        assert_eq!(h3.replicas.len(), 2);
        // R shrank to 2: the excess replica is trimmed; the primary did not move.
        let h2 = reg
            .resolve_unit_holders("idx", Unit::Shard(0), 2, usize::MAX, t0)
            .unwrap();
        assert!(h2.changed && !h2.moved);
        assert_eq!(h2.primary, h3.primary);
        assert_eq!(h2.replicas, h3.replicas[..1].to_vec());
        // The primary dies; the survivors keep heartbeating. An R=1 resolve promotes the warm
        // replica — and reports it as a moved assignment (`created` on the wire).
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        for n in ["node-a", "node-b", "node-c"] {
            if *n != h2.primary {
                reg.register_node(n, t1);
            }
        }
        let (ep, moved) = reg
            .resolve_unit_owner("idx", Unit::Shard(0), usize::MAX, t1)
            .unwrap();
        assert!(moved);
        assert_eq!(ep, h2.replicas[0], "the warm replica was promoted");
        assert!(
            reg.shard_map("idx").unwrap()[&0].replicas.is_empty(),
            "R=1 holds no replicas after promotion + trim"
        );
    }

    #[test]
    fn replicas_place_only_on_replica_capable_nodes() {
        // HA-G2: a node without an object store can never serve a replica window read-through, so
        // replica selection must skip it — placing there would silently absent HA. Primaries are
        // unaffected by capability.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("idx")).unwrap();
        let t0 = 1_000_000;
        reg.register_node_with_capability("node-a", true, t0);
        reg.register_node_with_capability("node-b", true, t0);
        reg.register_node_with_capability("node-c", false, t0);
        let h = reg
            .resolve_unit_holders("idx", Unit::Window(1), 3, usize::MAX, t0)
            .unwrap();
        assert_eq!(h.primary, "node-a", "least-loaded primary, tie → first");
        assert_eq!(
            h.replicas,
            vec!["node-b".to_string()],
            "R=3 with one other CAPABLE node holds what it can — never the incapable node"
        );
    }

    #[test]
    fn replica_top_up_skips_incapable_nodes_and_capability_refreshes_per_heartbeat() {
        // The top-up path (an existing unit re-resolved at a higher R) filters on capability too,
        // and capability follows the LATEST heartbeat: a node that loses its object store stops
        // attracting new replicas, while an incapable node can still take a primary.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("idx")).unwrap();
        let t0 = 1_000_000;
        reg.register_node_with_capability("node-a", true, t0);
        reg.register_node_with_capability("node-b", false, t0);
        reg.register_node_with_capability("node-c", true, t0);
        // Disarm the startup grace so the assigned-unit re-resolve below isn't frozen by it.
        reg.set_placement_grace_anchor(t0 - NODE_HEARTBEAT_TTL_MS - 1);
        let h1 = reg
            .resolve_unit_holders("idx", Unit::Window(1), 1, usize::MAX, t0)
            .unwrap();
        assert_eq!((h1.primary.as_str(), h1.replicas.len()), ("node-a", 0));
        // R raised to 3: top-up may only pick node-c (node-b is live but incapable).
        let h3 = reg
            .resolve_unit_holders("idx", Unit::Window(1), 3, usize::MAX, t0)
            .unwrap();
        assert_eq!(h3.primary, "node-a", "top-up never moves the primary");
        assert_eq!(
            h3.replicas,
            vec!["node-c".to_string()],
            "the top-up placed the capable node and skipped the incapable one"
        );
        // The next heartbeats withdraw capability everywhere (object stores lost) → a fresh unit
        // finds NO replica slot at all (without the filter, top-up would happily place two), but
        // still gets a primary — which may be an incapable node (least-loaded is node-b here).
        reg.register_node_with_capability("node-a", false, t0);
        reg.register_node_with_capability("node-c", false, t0);
        let h = reg
            .resolve_unit_holders("idx", Unit::Window(2), 3, usize::MAX, t0)
            .unwrap();
        assert_eq!(
            h.primary, "node-b",
            "primaries are placed by load alone — capability never gates them"
        );
        assert!(
            h.replicas.is_empty(),
            "no capable node left ⇒ hold what we can (zero replicas), never a bogus placement"
        );
    }

    #[test]
    fn register_node_defaults_to_replica_capable() {
        // The convenience heartbeat (tests, in-process callers) keeps the historical intent:
        // a plainly-registered node is a full holder, so R>1 fixtures keep placing replicas.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("idx")).unwrap();
        let t0 = 1_000_000;
        reg.register_node("node-a", t0);
        reg.register_node("node-b", t0);
        let h = reg
            .resolve_unit_holders("idx", Unit::Window(1), 2, usize::MAX, t0)
            .unwrap();
        assert_eq!(h.replicas.len(), 1, "default-capable nodes take replicas");
    }

    #[test]
    fn grace_window_treats_assigned_owners_as_live_unknown() {
        // HA-D5: for one TTL after (re)start/promotion, a resolve over a not-yet-re-registered
        // owner returns it untouched instead of mass-re-placing onto the first re-registrant.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("logs")).unwrap();
        let t0 = 1_000_000;
        reg.register_node("node-a", t0);
        reg.resolve_unit_owner("logs", Unit::Window(7), usize::MAX, t0)
            .unwrap();
        // Fresh promotion at t1: node-b re-registers first; node-a is a laggard.
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        reg.register_node("node-b", t1);
        reg.set_placement_grace_anchor(t1);
        let (ep, created) = reg
            .resolve_unit_owner("logs", Unit::Window(7), usize::MAX, t1)
            .unwrap();
        assert_eq!((ep.as_str(), created), ("node-a", false), "owner untouched");
        // Grace over and node-a still silent → the dead owner re-places.
        reg.set_placement_grace_anchor(t1 - NODE_HEARTBEAT_TTL_MS - 1);
        let (ep, created) = reg
            .resolve_unit_owner("logs", Unit::Window(7), usize::MAX, t1)
            .unwrap();
        assert_eq!((ep.as_str(), created), ("node-b", true));
    }

    #[test]
    fn announce_primaries_is_first_wins_with_dead_takeover_and_replica_reports() {
        // HA-D7: RegisterServedIndex announces are no longer last-write-wins. Legacy re-point (no
        // liveness tracking at all) still works; a LIVE foreign primary conflicts; a listed replica's
        // announce is a serving report; a confidently-dead primary is taken over.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("docs")).unwrap();
        let t0 = 1_000_000;
        // Announce-only mode (nobody heartbeats): re-pointing to a new endpoint stays allowed —
        // the restart-at-a-new-endpoint flow the D53 idempotent upsert blesses.
        reg.announce_primaries("docs", &[0], "node-a", t0, usize::MAX)
            .unwrap();
        reg.announce_primaries("docs", &[0], "node-b", t0, usize::MAX)
            .unwrap();
        assert_eq!(
            reg.shard_map("docs").unwrap()[&0].primary,
            Some(NodeId::from("node-b"))
        );
        // node-b heartbeats → its primaries are protected: a foreign announce now conflicts.
        reg.register_node("node-b", t0);
        assert!(matches!(
            reg.announce_primaries("docs", &[0], "node-c", t0, usize::MAX),
            Err(RegistryError::PlacementConflict(_))
        ));
        // An idempotent re-announce by the current primary always passes.
        reg.announce_primaries("docs", &[0], "node-b", t0, usize::MAX)
            .unwrap();
        // A listed replica's announce is a serving report — accepted, primary untouched.
        reg.add_replica("docs", 0, "node-c").unwrap();
        reg.announce_primaries("docs", &[0], "node-c", t0, usize::MAX)
            .unwrap();
        assert_eq!(
            reg.shard_map("docs").unwrap()[&0].primary,
            Some(NodeId::from("node-b")),
            "a replica's announce never steals the primary"
        );
        // Once node-b is confidently dead (tracked, stale past the TTL, out of grace) a takeover
        // announce succeeds — and the promoted node leaves the replica list.
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        reg.announce_primaries("docs", &[0], "node-c", t1, usize::MAX)
            .unwrap();
        let a = reg.shard_map("docs").unwrap().remove(&0).unwrap();
        assert_eq!(a.primary, Some(NodeId::from("node-c")));
        assert!(!a.replicas.contains(&NodeId::from("node-c")));
    }

    #[test]
    fn announces_enforce_the_entitlement_fail_closed() {
        // HA-D3a: RegisterServedIndex is no longer an entitlement bypass — a fresh primary an
        // announce creates on a *new* node is capped exactly like resolve-placed ones, and
        // untracked (announce-only) owners COUNT (fail-closed), so the cap can't be dodged by never
        // heartbeating. Co-locating on an already-primary node is free (node semantics, Option A).
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        for name in ["docs", "logs"] {
            reg.create(resolved(name)).unwrap();
        }
        let t0 = 1_000_000;
        reg.announce_primaries("docs", &[0], "node-a", t0, 1)
            .unwrap();
        // A second index on a *different* node is a new primary-holding node — over the cap of 1.
        assert!(matches!(
            reg.announce_primaries("logs", &[0], "node-b", t0, 1),
            Err(RegistryError::EntitlementExceeded { .. })
        ));
        assert!(matches!(
            reg.announce_windows(
                "logs",
                "node-b",
                &[WindowAnnounce {
                    window: 10,
                    bounds: None,
                    cold: false
                }],
                t0,
                1
            ),
            Err(RegistryError::EntitlementExceeded { .. })
        ));
        // But a *different index* whose primary co-locates on the already-counted node-a is free —
        // node count, not (index, node) pairs, is the metric.
        reg.announce_primaries("logs", &[0], "node-a", t0, 1)
            .unwrap();
        // More shards of an already-primary node are free; so is an idempotent re-announce.
        reg.announce_primaries("docs", &[1, 2], "node-a", t0, 1)
            .unwrap();
        reg.announce_primaries("docs", &[0], "node-a", t0, 1)
            .unwrap();
        assert_eq!(reg.count_entitlement_nodes(t0), 1);
    }

    #[test]
    fn entitlement_node_cap_allows_demo_shape_and_refuses_a_fourth_node() {
        // Option A (c)+(d): the free tier is 3 *nodes*. The 4-index demo shape (docs, catalog,
        // movies on the pool + events on its own node = 3 primary-holding nodes) fits under 3, and a
        // primary landing on a 4th distinct node is refused — while co-locating a 5th index on an
        // already-counted node stays free.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        for name in ["docs", "catalog", "movies", "events", "extra"] {
            reg.create(resolved(name)).unwrap();
        }
        let t0 = 1_000_000;
        let cap = 3;
        // Three distinct primary-holding nodes across four indexes — the marketed demo, allowed.
        reg.announce_primaries("docs", &[0], "node-pool-a", t0, cap)
            .unwrap();
        reg.announce_primaries("catalog", &[0], "node-pool-a", t0, cap)
            .unwrap(); // co-located on pool-a — free
        reg.announce_primaries("movies", &[0], "node-pool-b", t0, cap)
            .unwrap();
        reg.announce_primaries("events", &[0], "node-events", t0, cap)
            .unwrap();
        assert_eq!(
            reg.count_entitlement_nodes(t0),
            3,
            "4 indexes over 3 nodes fits the free tier"
        );
        // A primary on a 4th distinct node is refused.
        assert!(matches!(
            reg.announce_primaries("extra", &[0], "node-four", t0, cap),
            Err(RegistryError::EntitlementExceeded { .. })
        ));
        // But co-locating that 5th index on an already-counted node is free.
        reg.announce_primaries("extra", &[0], "node-pool-b", t0, cap)
            .unwrap();
        assert_eq!(reg.count_entitlement_nodes(t0), 3);
    }

    #[test]
    fn announce_windows_batches_metadata_and_ignores_replica_reports() {
        // One announce = one mutation: primaries + zone-maps + tier land together; a replica's
        // report neither re-points nor overwrites the primary's metadata; a non-holder conflicts.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("events")).unwrap();
        let t0 = 1_000_000;
        reg.register_node("node-a", t0);
        reg.register_node("node-b", t0);
        // Disarm the startup grace so the R=2 top-up below acts immediately.
        reg.set_placement_grace_anchor(t0 - NODE_HEARTBEAT_TTL_MS - 1);
        reg.announce_windows(
            "events",
            "node-a",
            &[
                WindowAnnounce {
                    window: 10,
                    bounds: Some((5, 80)),
                    cold: false,
                },
                WindowAnnounce {
                    window: 20,
                    bounds: None,
                    cold: true,
                },
            ],
            t0,
            usize::MAX,
        )
        .unwrap();
        let wm = reg.window_map("events").unwrap();
        assert_eq!(wm[&10].assignment.primary, Some(NodeId::from("node-a")));
        assert_eq!((wm[&10].event_min, wm[&10].event_max), (Some(5), Some(80)));
        assert!(!wm[&10].cold && wm[&20].cold);
        // node-b becomes window 10's replica (R=2 top-up), then reports serving it: accepted, but
        // the primary and the primary-reported zone-map/tier stand.
        reg.resolve_unit_holders("events", Unit::Window(10), 2, usize::MAX, t0)
            .unwrap();
        reg.announce_windows(
            "events",
            "node-b",
            &[WindowAnnounce {
                window: 10,
                bounds: Some((0, 999)),
                cold: true,
            }],
            t0,
            usize::MAX,
        )
        .unwrap();
        let wm = reg.window_map("events").unwrap();
        assert_eq!(wm[&10].assignment.primary, Some(NodeId::from("node-a")));
        assert_eq!((wm[&10].event_min, wm[&10].event_max), (Some(5), Some(80)));
        assert!(!wm[&10].cold, "a replica's tier report doesn't clobber");
        // A live non-holder claiming the window is a conflict.
        reg.register_node("node-c", t0);
        assert!(matches!(
            reg.announce_windows(
                "events",
                "node-c",
                &[WindowAnnounce {
                    window: 10,
                    bounds: None,
                    cold: false
                }],
                t0,
                usize::MAX
            ),
            Err(RegistryError::PlacementConflict(_))
        ));
    }

    #[test]
    fn entitlement_drops_tracked_stale_nodes_but_counts_untracked_owners() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("logs")).unwrap();
        let t0 = 1_000_000;
        reg.register_node("node-a", t0);
        reg.resolve_unit_owner("logs", Unit::Window(1), usize::MAX, t0)
            .unwrap();
        assert_eq!(reg.count_entitlement_nodes(t0), 1);
        // Tracked and stale past the TTL (grace long over): the node stops counting — its primary is
        // about to be re-placed, which lands it on a live node.
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        assert_eq!(reg.count_entitlement_nodes(t1), 0);
        // An owner the pool never tracked (announce-only) counts at ANY time — fail-closed.
        reg.create(resolved("docs")).unwrap();
        reg.announce_primaries("docs", &[0], "node-x", t1, usize::MAX)
            .unwrap();
        assert_eq!(reg.count_entitlement_nodes(t1), 1);
    }

    #[test]
    fn placement_listener_fires_on_placement_mutations_only() {
        // HA-D1: the notification choke point sits at the persist boundary — ANY mutation that
        // changes holder sets fires it (resolve, announce, remove_node, promote, drop), and
        // non-placement mutations (aliases, credentials) stay silent.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        let fired = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = fired.clone();
        reg.set_placement_listener(move || {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        });
        let count = || fired.load(std::sync::atomic::Ordering::SeqCst);
        reg.create(resolved("docs")).unwrap();
        assert_eq!(count(), 0, "an index with no assignments isn't placement");
        reg.announce_primaries("docs", &[0], "node-a", 1_000, usize::MAX)
            .unwrap();
        assert_eq!(count(), 1, "announce fired");
        reg.announce_primaries("docs", &[0], "node-a", 1_000, usize::MAX)
            .unwrap();
        assert_eq!(count(), 1, "an idempotent re-announce changes nothing");
        reg.add_replica("docs", 0, "node-b").unwrap();
        assert_eq!(count(), 2, "replica add fired");
        reg.remove_node("docs", 0, &NodeId::from("node-a")).unwrap();
        assert_eq!(count(), 3, "remove_node fired");
        reg.promote_replica("docs", 0).unwrap();
        assert_eq!(count(), 4, "promotion fired");
        reg.set_alias("d", ["docs"]).unwrap();
        reg.set_credential("alice", "pw").unwrap();
        assert_eq!(count(), 4, "non-placement mutations stay silent");
        reg.drop_index("docs").unwrap();
        assert_eq!(count(), 5, "drop_index fired");
    }
}
