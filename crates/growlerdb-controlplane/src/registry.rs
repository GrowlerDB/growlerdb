//! The index **registry**: the cluster's catalog of index definitions + lifecycle status,
//! durably persisted so create / drop / list survive restarts and a crash never leaves a
//! half-written catalog.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::PathBuf;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use fs2::FileExt;
use growlerdb_core::routing::{BucketMap, Reassignment};
use growlerdb_core::ResolvedIndex;
use serde::{Deserialize, Serialize};

/// A fixed dummy argon2 PHC hash used to make [`Registry::verify_credential`] do equivalent work for
/// an unknown subject as for a real one, closing a username-enumeration timing oracle (task-147 /
/// I10). Computed once; the salt is fixed (it protects nothing — the hash is never authenticated
/// against). A parse/hash failure yields an empty string, which `PasswordHash::new` rejects → the
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

/// One time-window shard of a windowed index (task-81): its node placement plus the event-time
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
    /// so registries written before the shard map load cleanly.
    #[serde(default)]
    pub shards: BTreeMap<u32, ShardAssignment>,
    /// `window-id → WindowAssignment` for a **time-windowed** index (task-81): which node serves
    /// each window + its event-time zone-map. Empty for ordinal (hash/partition) indexes.
    #[serde(default)]
    pub windows: BTreeMap<i64, WindowAssignment>,
    /// Virtual-bucket map (task-77): `bucket_owners[b]` = the shard owning bucket `b`, length
    /// [`NUM_BUCKETS`](growlerdb_core::routing::NUM_BUCKETS). **Empty ⇒ legacy `fnv % shards`
    /// routing** (every pre-bucket index), so registries written before this field load as legacy.
    /// When present, writers and readers route `key → bucket → shard` through it, and a reshard
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

/// On-disk schema version for `registry.json` (task-70 L2). A versioned envelope means a future
/// format change can be detected and migrated instead of mis-parsed.
const REGISTRY_VERSION: u32 = 1;

/// How long a windowed node's heartbeat is trusted before it drops out of the CP placement pool
/// (task-219). Sized at ~3× a node's re-register interval (~10 s) so one missed heartbeat doesn't
/// eject a healthy node, while a genuinely dead node's windows get re-placed within ~30 s.
pub const NODE_HEARTBEAT_TTL_MS: i64 = 30_000;

/// The persisted `registry.json` document: a `{ version, indexes }` envelope around the catalog,
/// rather than a bare map — so the format is self-describing and evolvable (task-70).
#[derive(Debug, Serialize, Deserialize)]
struct RegistryFile {
    version: u32,
    indexes: BTreeMap<String, IndexEntry>,
    /// Index **aliases** (task-52): `alias → set of member index names`. A stable name a client can
    /// search/route through; re-pointing it is the atomic reindex-and-swap. Defaulted so older
    /// registry files (no aliases) load cleanly.
    #[serde(default)]
    aliases: BTreeMap<String, BTreeSet<String>>,
    /// Saved searches (task-106): `id → `[`SavedQuery`]. Per-subject persisted query state.
    /// Defaulted so older registry files load cleanly.
    #[serde(default)]
    saved_queries: BTreeMap<String, SavedQuery>,
    /// Local role bindings (task-104): `subject → roles`. Admin-managed grants that augment the
    /// roles a token carries; the control plane merges them when authorizing. Defaulted for older
    /// registry files.
    #[serde(default)]
    role_bindings: BTreeMap<String, Vec<String>>,
    /// API tokens (task-105): `id → `[`ApiToken`]. Only the SHA-256 hash is stored, never the secret.
    /// Defaulted for older registry files.
    #[serde(default)]
    tokens: BTreeMap<String, ApiToken>,
    /// Built-in local credentials (task-128): `subject → argon2 PHC hash` (salt embedded in the
    /// string). Never the plaintext password. Powers `/v1/login` for closed mode without an external
    /// IdP. Defaulted for older registry files.
    #[serde(default)]
    credentials: BTreeMap<String, String>,
    /// Per-subject **index allowlist** for built-in login (task-244, extending task-240): `subject →
    /// allowed index names`. When a subject has a binding, their minted session JWT carries these as
    /// the `indexes` claim, so per-index RBAC restricts them to exactly this set. Absent/empty =
    /// unrestricted (the pre-task-244 default). Defaulted for older registry files.
    #[serde(default)]
    index_bindings: BTreeMap<String, Vec<String>>,
}

/// A persisted API token (task-105): long-lived programmatic credential. Only the secret's hash is
/// stored — the raw secret is shown once at creation and never persisted.
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
    /// Optional expiry (epoch ms), task-151 / B13. `None` = never expires (all pre-B13 tokens, via
    /// serde default). Past this instant the token no longer authenticates and is pruned on the next
    /// `create_token`, so the token map can't grow without bound.
    #[serde(default)]
    pub expires_at_ms: Option<i64>,
}

impl ApiToken {
    /// Whether this token is past its expiry at `now_ms` (task-151 / B13). A token with no expiry is
    /// never expired.
    pub fn is_expired(&self, now_ms: i64) -> bool {
        self.expires_at_ms.is_some_and(|exp| now_ms >= exp)
    }
}

/// A persisted saved search (task-106): the full query state the console can restore, scoped to its
/// `owner` (the verified subject). `state` is an opaque JSON blob the UI round-trips.
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
    /// An operation named an index that is not registered.
    #[error("index `{0}` not found")]
    NotFound(String),
    /// `promote_replica` for a shard with no replica to promote.
    #[error("shard {shard} of `{index}` has no replica to promote")]
    NoReplica { index: String, shard: u32 },
    /// `promote_replica` while a primary is still assigned — the caller must fence/clear the old
    /// primary first, or two nodes serve as primary for one shard (split brain). Task-74 (M1).
    #[error(
        "shard {shard} of `{index}` still has primary `{primary}`; fence/clear it before promoting"
    )]
    PrimaryStillAssigned {
        index: String,
        shard: u32,
        primary: String,
    },
    /// `resolve_window_owner` when no node has heartbeated within the TTL — there is nowhere to place
    /// the window (task-219). The caller retries once a node registers.
    #[error(
        "no live node to place window {window} of `{index}` (none heartbeated within the TTL)"
    )]
    NoLiveNode { index: String, window: i64 },
    /// Another process holds the registry's exclusive lock — single-writer is enforced, so a
    /// second control plane fails fast rather than last-writer-wins clobbering (task-74, H5).
    #[error("registry `{0}` is locked by another process")]
    Locked(PathBuf),
    /// Reading or writing the persisted registry failed.
    #[error("registry io: {0}")]
    Io(#[from] std::io::Error),
    /// Encoding/decoding the persisted registry failed.
    #[error("registry codec: {0}")]
    Codec(#[from] serde_json::Error),
    /// `set_alias` used a name that's already a registered index (task-52) — an alias and an index
    /// can't share a name, or routing would be ambiguous.
    #[error("alias `{0}` clashes with an existing index name")]
    AliasNameClash(String),
    /// An operation named an alias that doesn't exist (task-52).
    #[error("alias `{0}` not found")]
    AliasNotFound(String),
    /// An update/delete named a saved query that doesn't exist (or isn't the caller's) (task-106).
    #[error("saved query `{0}` not found")]
    SavedQueryNotFound(String),
    /// A stored bucket map failed validation (task-77) — wrong length or a gap. Indicates a
    /// corrupt/hand-edited registry, since maps are only ever written through validated paths.
    #[error("invalid bucket map: {0}")]
    InvalidBucketMap(String),
    /// A built-in credential could not be hashed (task-128) — an argon2 failure, not a wrong password.
    #[error("credential hashing failed: {0}")]
    Credential(String),
}

/// One entry in a per-index **activity log** (task-110): a timestamped lifecycle event. Stored
/// append-only and bounded; the `kind` is a stable machine tag, the `message` human-readable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityEvent {
    /// Event time (epoch ms).
    pub ts_ms: i64,
    /// Stable event tag, e.g. `index.created`, `alias.swapped`, `reshard.applied`.
    pub kind: String,
    /// Human-readable description.
    pub message: String,
}

/// Max events retained per index in the activity log (task-110) — oldest are dropped.
const ACTIVITY_RETAIN: usize = 200;

/// Debounce window for activity-sidecar flushes (task-151 / B11). An isolated event (none within
/// this window) flushes immediately — preserving synchronous durability for the common case — while
/// a burst (e.g. a multi-target alias swap looping `record_activity` per target) coalesces into a
/// single off-lock write instead of one full-file fsync per event. The burst tail is flushed on
/// graceful shutdown ([`Registry`]'s `Drop`); a hard crash within the window may lose the last few
/// events, which is acceptable — the log is a non-critical audit convenience, not catalog state.
const ACTIVITY_FLUSH_DEBOUNCE_MS: i64 = 1000;

/// Coalescing state for the debounced activity-sidecar flush (task-151 / B11), behind its own mutex
/// so a flush never holds the activity data lock across the fsync.
#[derive(Default)]
struct ActivityFlush {
    /// Epoch ms of the last completed sidecar write (`0` = never written this session).
    last_flush_ms: i64,
    /// In-memory events exist that a debounce window skipped writing — flush them on shutdown.
    dirty: bool,
}

/// Registry result alias.
pub type Result<T> = std::result::Result<T, RegistryError>;

/// The index **registry**: `name → `[`IndexEntry`], persisted to a JSON document. Reads are
/// served from memory; every mutation persists **atomically** (write a temp file, then
/// rename over the target) so a crash never leaves a partially-written registry. Cheap to
/// share across threads — internally `RwLock`-guarded.
///
/// **Single-writer (task-74):** [`open`](Self::open) takes an exclusive advisory `flock` on a
/// `.lock` sibling, held for the registry's lifetime, so a second control-plane process fails
/// fast instead of last-writer-wins clobbering a stale in-memory map. (See design/06.)
///
/// **Lock-order invariant (task-151 / B5):** each data map has its own `RwLock`. A mutation holds
/// **only the one map it changes**, drops it, then calls [`persist_snapshot`](Self::persist_snapshot)
/// (which re-reads every map off-lock). The only places that hold two+ data locks at once do so in
/// this fixed order — `indexes → aliases → saved_queries → role_bindings → tokens → credentials`:
/// [`persist_snapshot`](Self::persist_snapshot) (all read locks, for the snapshot),
/// [`drop_index`](Self::drop_index) and [`set_alias`](Self::set_alias) (`indexes` before `aliases`).
/// The derived `token_by_hash` index and the `activity`/`session_epochs` sidecars are independent,
/// always taken one-at-a-time. Keep new lock acquisitions on this order — never the reverse.
pub struct Registry {
    path: PathBuf,
    indexes: RwLock<BTreeMap<String, IndexEntry>>,
    /// Index aliases (task-52): `alias → member index names`. A separate lock from `indexes`;
    /// every code path acquires **`indexes` before `aliases`** to avoid deadlock.
    aliases: RwLock<BTreeMap<String, BTreeSet<String>>>,
    /// Saved searches (task-106): `id → `[`SavedQuery`]. Lock order is **indexes → aliases →
    /// saved_queries** everywhere, to avoid deadlock.
    saved_queries: RwLock<BTreeMap<String, SavedQuery>>,
    /// Monotonic suffix for generated saved-query ids (uniqueness within a process; combined with a
    /// millisecond timestamp it is unique across restarts too).
    next_saved: std::sync::atomic::AtomicU64,
    /// Local role bindings (task-104): `subject → roles`. Lock order is **indexes → aliases →
    /// saved_queries → role_bindings → tokens**.
    role_bindings: RwLock<BTreeMap<String, Vec<String>>>,
    /// API tokens (task-105): `id → `[`ApiToken`].
    tokens: RwLock<BTreeMap<String, ApiToken>>,
    /// Secret-hash → token-id lookup index (task-151 / B13): makes `find_token` — on every
    /// authenticated request — O(1) instead of a linear scan of `tokens`. **Derived** from `tokens`
    /// (not persisted); rebuilt on open and after every token mutation. Never held together with the
    /// `tokens` lock in a nested way (see `find_token` / `rebuild_token_index`), so no deadlock.
    token_by_hash: RwLock<std::collections::HashMap<String, String>>,
    /// Monotonic suffix for generated token ids.
    next_token: std::sync::atomic::AtomicU64,
    /// Built-in local credentials (task-128): `subject → argon2 PHC hash`. Lock order is **last**,
    /// after `tokens` (indexes → aliases → saved_queries → role_bindings → tokens → credentials).
    credentials: RwLock<BTreeMap<String, String>>,
    /// Per-subject index allowlist for built-in login (task-244): `subject → allowed index names`.
    /// Threaded into the session JWT's `indexes` claim so per-index RBAC (task-240) restricts the
    /// subject. Lock order is **after `credentials`** (… → credentials → index_bindings).
    index_bindings: RwLock<BTreeMap<String, Vec<String>>>,
    /// Per-index activity log (task-110): `index → events`, bounded + append-only. Persisted to a
    /// **separate** `activity.json` (non-critical, lossy-on-corruption) so the registry's atomic
    /// envelope stays small. Lock is independent (acquired last).
    activity: RwLock<BTreeMap<String, Vec<ActivityEvent>>>,
    /// Path of the activity sidecar file.
    activity_path: PathBuf,
    /// Debounce/coalescing state for the activity-sidecar flush (task-151 / B11). Its own mutex also
    /// serializes concurrent flushes (last snapshot wins the file) — like `flush_lock` for the main
    /// registry — and is never nested under the activity data lock.
    activity_flush: std::sync::Mutex<ActivityFlush>,
    /// Per-subject **session epoch** (epoch ms): sessions issued before this instant are stale
    /// (task-147 / B4). Bumped when a subject's roles change or credential is removed, giving
    /// revocation / immediate role-downgrade for outstanding session JWTs. Persisted to a separate
    /// `sessions.json` sidecar (independent lock, acquired last).
    session_epochs: RwLock<BTreeMap<String, i64>>,
    /// Path of the session-epoch sidecar file.
    session_epochs_path: PathBuf,
    /// Serializes registry-file writes (task-151 / F10): a mutation applies its change in memory,
    /// releases its data lock, then persists off-lock under this — so routing reads never block on the
    /// fsync, and two concurrent persists can't lose a change from the file (each snapshots the latest
    /// memory; the last write wins with the full state).
    flush_lock: std::sync::Mutex<()>,
    /// Per-index **node inventory** for CP-driven windowed placement (task-219): `index → (node
    /// endpoint → last-heartbeat epoch-ms)`. **In-memory only** (like `token_by_hash`) — liveness is
    /// ephemeral runtime state, not durable topology: after a control-plane restart every node
    /// re-registers within a heartbeat interval, so persisting it would only add write amplification.
    /// Window *assignments* (which node owns a window) stay durable in [`IndexEntry::windows`].
    node_heartbeats: RwLock<BTreeMap<String, BTreeMap<String, i64>>>,
    /// Held for the process lifetime to keep the exclusive `flock`; released on drop / exit.
    _lock: File,
}

impl Registry {
    /// Open the registry at `path`, loading the existing catalog if present (else empty). Takes an
    /// exclusive lock first (fails fast with [`Locked`](RegistryError::Locked) if another process
    /// holds it — single-writer, task-74). If the file fails to parse, fall back to the
    /// last-known-good `.prev` copy with a loud warning rather than hard-failing startup (task-70).
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Exclusive single-writer lock on a stable `.lock` sibling (the data file is renamed over,
        // so locking it directly wouldn't be stable across writes).
        let lock_path = path.with_extension("lock");
        let lock = File::create(&lock_path)?;
        lock.try_lock_exclusive()
            .map_err(|_| RegistryError::Locked(lock_path))?;

        let (indexes, aliases, saved_queries, role_bindings, tokens, credentials, index_bindings) =
            if path.exists() {
                load(&path)?
            } else {
                (
                    BTreeMap::new(),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    BTreeMap::new(),
                    BTreeMap::new(),
                )
            };
        // Activity log sidecar (task-110): best-effort — a missing/corrupt log starts empty rather
        // than failing registry startup (it's an audit convenience, not catalog state).
        let activity_path = path.with_file_name("activity.json");
        let activity = std::fs::read(&activity_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        // Session-epoch sidecar (task-147 / B4): best-effort load, like the activity log. A missing
        // file means no revocations are in effect (all outstanding sessions valid until `exp`).
        let session_epochs_path = path.with_file_name("sessions.json");
        let session_epochs = std::fs::read(&session_epochs_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        // Build the B13 hash→id lookup index from the loaded tokens (derived, not persisted).
        let token_by_hash: std::collections::HashMap<String, String> = tokens
            .iter()
            .map(|(id, t)| (t.hash.clone(), id.clone()))
            .collect();
        Ok(Self {
            path,
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
            activity_path,
            activity_flush: std::sync::Mutex::new(ActivityFlush::default()),
            session_epochs: RwLock::new(session_epochs),
            session_epochs_path,
            flush_lock: std::sync::Mutex::new(()),
            node_heartbeats: RwLock::new(BTreeMap::new()),
            _lock: lock,
        })
    }

    /// Read the catalog under the lock, recovering from poisoning (task-74): a panic elsewhere
    /// while holding the lock must not take down every subsequent create/drop/list/route call.
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
    /// lock** (task-151 / F10). A mutation applies its change in memory, releases its write lock, then
    /// calls this — so routing reads (`resolve`/`shard_map`/`get`/`list`) never block on the fsync,
    /// and mutations aren't globally serialized behind disk I/O. The `flush_lock` serializes the
    /// writes themselves (they're rare) so a concurrent pair can't lose a change from the file: each
    /// snapshot reads the latest memory, and the last write wins with the full state. Must be called
    /// with **no** registry data lock held (it re-acquires them briefly).
    fn persist_snapshot(&self) -> Result<()> {
        let _flush = self.flush_lock.lock().unwrap_or_else(|e| e.into_inner());
        // Clone each map under a brief read lock; the guards are temporaries released at the end of
        // this statement, before any I/O.
        let file = RegistryFile {
            version: REGISTRY_VERSION,
            indexes: self.read_map().clone(),
            aliases: self.read_aliases().clone(),
            saved_queries: self.read_saved().clone(),
            role_bindings: self.read_bindings().clone(),
            tokens: self.read_tokens().clone(),
            credentials: self.read_credentials().clone(),
            index_bindings: self.read_index_bindings().clone(),
        };
        let json = serde_json::to_vec_pretty(&file)?;
        growlerdb_core::durable::write_keeping_prev(&self.path, &json)?;
        Ok(())
    }

    /// Register a new index (status [`Building`](IndexStatus::Building)). Errors if the name
    /// is already taken.
    pub fn create(&self, definition: ResolvedIndex) -> Result<()> {
        let name = definition.name.clone();
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
        drop(map); // release the data lock before the fsync (task-151 / F10)
        self.persist_snapshot()
    }

    /// Mark an index [`Active`](IndexStatus::Active) (provisioning complete). Errors if absent.
    pub fn activate(&self, name: &str) -> Result<()> {
        let mut map = self.write_map();
        let entry = map
            .get_mut(name)
            .ok_or_else(|| RegistryError::NotFound(name.to_string()))?;
        entry.status = IndexStatus::Active;
        drop(map);
        self.persist_snapshot()
    }

    /// Remove an index, returning its definition. Errors if absent. Also prunes the index from any
    /// aliases that point at it (dropping aliases left empty), so an alias never dangles (task-52).
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

    /// Point an `alias` at `targets`, replacing any existing target set (task-52). The atomic
    /// reindex-and-swap is just re-pointing an alias here — one durable write. Errors if the alias
    /// name collides with an index, or any target index isn't registered.
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

    /// Remove an alias (task-52). Errors if it doesn't exist.
    pub fn drop_alias(&self, alias: &str) -> Result<()> {
        {
            let mut aliases = self.write_aliases();
            if aliases.remove(alias).is_none() {
                return Err(RegistryError::AliasNotFound(alias.to_string()));
            }
        } // hold only the map we mutate; persist_snapshot re-reads everything off-lock (B5/F10)
        self.persist_snapshot()
    }

    /// The member indexes an `alias` points at, if it is an alias (task-52).
    pub fn alias_targets(&self, alias: &str) -> Option<Vec<String>> {
        self.read_aliases()
            .get(alias)
            .map(|s| s.iter().cloned().collect())
    }

    /// All aliases as `alias → sorted member names` (task-52).
    pub fn list_aliases(&self) -> BTreeMap<String, Vec<String>> {
        self.read_aliases()
            .iter()
            .map(|(a, t)| (a.clone(), t.iter().cloned().collect()))
            .collect()
    }

    /// Saved searches visible to `owner` (task-106): the owner's own rows plus any `shared` ones,
    /// newest first. An empty `owner` (anonymous/open gateway) sees only shared rows.
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

    /// Create (empty `id`) or update (existing own `id`) a saved query for `owner` (task-106). The
    /// server stamps `id`/`owner`/`created_at_ms` on create; an update of another subject's row (or
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

    /// Delete `owner`'s saved query `id` (task-106). Deleting a non-existent or non-owned row is
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

    /// All local role bindings as `subject → roles` (task-104), sorted by subject.
    pub fn list_role_bindings(&self) -> BTreeMap<String, Vec<String>> {
        self.read_bindings().clone()
    }

    /// The locally-bound roles for `subject` (task-104) — merged into the caller's token roles when
    /// the control plane authorizes. Empty for an unknown/empty subject.
    pub fn roles_for(&self, subject: &str) -> Vec<String> {
        if subject.is_empty() {
            return Vec::new();
        }
        self.read_bindings()
            .get(subject)
            .cloned()
            .unwrap_or_default()
    }

    /// Set (replace) `subject`'s local roles (task-104). Empty `roles` removes the binding. Roles
    /// are de-duplicated and order-stable; an empty `subject` is rejected.
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
            // Hold only `role_bindings` — persist_snapshot re-reads every map off-lock (B5/F10).
            let mut bindings = self.write_bindings();
            if deduped.is_empty() {
                bindings.remove(subject);
            } else {
                bindings.insert(subject.to_string(), deduped);
            }
        }
        self.persist_snapshot()?;
        // A role change must take effect immediately: invalidate outstanding sessions so the subject
        // re-authenticates with the new roles rather than riding an old token's embedded set (B4).
        self.revoke_sessions(subject);
        Ok(())
    }

    /// The index allowlist bound to `subject` for built-in login (task-244) — threaded into the
    /// session JWT's `indexes` claim so per-index RBAC (task-240) restricts them. Empty (no binding)
    /// = unrestricted across indexes. Empty for an unknown/empty subject.
    pub fn indexes_for(&self, subject: &str) -> Vec<String> {
        if subject.is_empty() {
            return Vec::new();
        }
        self.read_index_bindings()
            .get(subject)
            .cloned()
            .unwrap_or_default()
    }

    /// Set (replace) `subject`'s index allowlist (task-244). Empty `indexes` removes the binding
    /// (making the subject unrestricted). Entries are de-duplicated and order-stable; an empty
    /// `subject` is rejected. Like a role change, this bumps the subject's session epoch so an
    /// outstanding token minted with the old scope is superseded and they re-authenticate.
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
            // Hold only `index_bindings` — persist_snapshot re-reads every map off-lock (B5/F10).
            let mut bindings = self.write_index_bindings();
            if deduped.is_empty() {
                bindings.remove(subject);
            } else {
                bindings.insert(subject.to_string(), deduped);
            }
        }
        self.persist_snapshot()?;
        // A scope change takes effect immediately: supersede outstanding sessions (like a role change).
        self.revoke_sessions(subject);
        Ok(())
    }

    /// All API tokens (task-105), newest first. The caller strips the `hash` before returning to a
    /// client — only metadata leaves the control plane.
    pub fn list_tokens(&self) -> Vec<ApiToken> {
        let mut out: Vec<ApiToken> = self.read_tokens().values().cloned().collect();
        out.sort_by_key(|t| std::cmp::Reverse(t.created_at_ms));
        out
    }

    /// Persist a new API token (task-105). The caller has minted the secret + hash + id; the registry
    /// stamps `created_at_ms` and returns the stored token.
    pub fn create_token(&self, mut token: ApiToken) -> Result<ApiToken> {
        let now = now_ms();
        token.created_at_ms = now;
        {
            let mut tokens = self.write_tokens();
            // Prune expired tokens so the map (and its persisted copy) can't grow without bound (B13).
            tokens.retain(|_, t| !t.is_expired(now));
            tokens.insert(token.id.clone(), token.clone());
        } // release the tokens write lock before rebuilding the index / persisting off-lock (F10)
        self.rebuild_token_index();
        self.persist_snapshot()?;
        Ok(token)
    }

    /// Revoke an API token by id (task-105) — effective immediately. Errors if it doesn't exist.
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

    /// Set (or replace) a subject's built-in password (task-128): salted-argon2-hash it and persist.
    /// Never stores plaintext. Errors only on a hashing/persist failure, not on a re-set.
    pub fn set_credential(&self, subject: &str, password: &str) -> Result<()> {
        use argon2::password_hash::{rand_core::OsRng, PasswordHasher, SaltString};
        use argon2::Argon2;
        let salt = SaltString::generate(&mut OsRng);
        let hash = Argon2::default()
            .hash_password(password.as_bytes(), &salt)
            .map_err(|e| RegistryError::Credential(e.to_string()))?
            .to_string();
        {
            // Hold only `credentials` — persist_snapshot re-reads every map off-lock (B5/F10).
            let mut creds = self.write_credentials();
            creds.insert(subject.to_string(), hash);
        }
        self.persist_snapshot()
    }

    /// Verify a subject's password against its stored argon2 hash (task-128). `false` for an unknown
    /// subject or a wrong password — the caller can't distinguish the two. To avoid a
    /// **username-enumeration timing oracle** (task-147 / I10), an unknown subject is verified against
    /// a fixed dummy hash so both paths perform equivalent Argon2 work before returning `false`.
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

    /// Remove a subject's built-in credential (task-128). No-op if absent.
    pub fn remove_credential(&self, subject: &str) -> Result<()> {
        {
            // Hold only `credentials` — persist_snapshot re-reads every map off-lock (B5/F10).
            let mut creds = self.write_credentials();
            creds.remove(subject);
        }
        self.persist_snapshot()?;
        // Deprovision: kill outstanding sessions so a removed user can't keep riding a live JWT (B4).
        self.revoke_sessions(subject);
        Ok(())
    }

    /// Whether any built-in credential exists (task-128) — decides whether to seed an initial admin
    /// on first closed-mode boot.
    pub fn has_credentials(&self) -> bool {
        !self.read_credentials().is_empty()
    }

    /// Whether `subject` has a built-in credential (task-244) — lets a seeder be idempotent about a
    /// single account (e.g. the demo user) without clobbering an operator-changed password on restart.
    pub fn has_credential(&self, subject: &str) -> bool {
        self.read_credentials().contains_key(subject)
    }

    /// Look up a token by its secret's `hash` (task-105) — used by the authenticator on every
    /// authenticated request, so O(1) via the B13 index rather than a linear scan. `None` if no such
    /// **live** token, so a revoked or **expired** (B13) token fails authentication. The two locks are
    /// taken one-at-a-time (index, released, then tokens) so this never nests with the writer's order.
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

    /// Rebuild the B13 hash→id index from the current token map. Cheap (tokens are few and change
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

    /// A monotonic-ish token id (task-105): `tok-<ms>-<counter>`.
    pub fn next_token_id(&self) -> String {
        format!(
            "tok-{}-{}",
            now_ms(),
            self.next_token
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        )
    }

    /// Append a lifecycle event to `index`'s activity log (task-110), trimmed to the retention cap.
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
        } // drop the data lock before any I/O — the fsync no longer blocks reads/appends (B11).
        self.flush_activity();
    }

    /// Persist the activity sidecar off the data lock, coalescing bursts (task-151 / B11). An
    /// isolated event flushes immediately (synchronous durability preserved); events arriving within
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
        if let Err(e) = persist_activity(&self.activity_path, &snapshot) {
            eprintln!("registry: failed to persist activity log ({e})");
        }
    }

    /// The subject's **session epoch** (epoch ms): a session JWT with `iat` before this is stale and
    /// must be rejected (task-147 / B4). `0` means no revocation is in effect for this subject.
    pub fn session_epoch(&self, subject: &str) -> i64 {
        self.session_epochs
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(subject)
            .copied()
            .unwrap_or(0)
    }

    /// Invalidate all of `subject`'s outstanding sessions by advancing its session epoch to now
    /// (task-147 / B4) — called when the subject's roles change or its credential is removed, so a
    /// role downgrade / deprovision takes effect immediately (the next call with a stale session is
    /// rejected and must re-authenticate with the current roles).
    pub fn revoke_sessions(&self, subject: &str) {
        let mut epochs = self
            .session_epochs
            .write()
            .unwrap_or_else(|e| e.into_inner());
        epochs.insert(subject.to_string(), now_ms());
        if let Err(e) = persist_sessions(&self.session_epochs_path, &epochs) {
            eprintln!("registry: failed to persist session epochs ({e})");
        }
    }

    /// `index`'s activity events, **newest first**, capped at `limit` (0 = all retained, task-110).
    pub fn list_activity(&self, index: &str, limit: usize) -> Vec<ActivityEvent> {
        let log = self.activity.read().unwrap_or_else(|e| e.into_inner());
        let Some(events) = log.get(index) else {
            return Vec::new();
        };
        let take = if limit == 0 { events.len() } else { limit };
        events.iter().rev().take(take).cloned().collect()
    }

    /// Resolve a `name` to the concrete indexes a search/route should touch (task-52): an **alias**
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

    /// Set `node` as the primary for **all** of `shards` of `index` in a **single** persist (task-202)
    /// — the batched form of [`assign_primary`]. A node serving K ordinals used to call
    /// `assign_primary` K times, each a full `registry.json` rewrite, so bringing up an N-shard index
    /// was O(N) rewrites of an O(N)-sized file = O(N²) bytes. This mutates all K in memory under one
    /// lock, then persists once. Errors if the index is unregistered.
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
    /// has no replica, or — the **fencing precondition** (task-74) — a primary is still assigned:
    /// the caller must fence/clear the old primary first (`remove_node`), so promotion can't
    /// produce two primaries for one shard (split brain).
    pub fn promote_replica(&self, index: &str, shard: u32) -> Result<NodeId> {
        let mut map = self.write_map();
        let entry = map
            .get_mut(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        let assignment = entry.shards.entry(shard).or_default();
        // Fencing (M1): refuse to promote over a still-assigned primary — clear it first.
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

    // ---- virtual-bucket map (task-77) ------------------------------------------

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

    /// Store `map` as `index`'s bucket→shard assignment (adopting bucketed routing), then persist.
    /// Errors if the index is unknown.
    pub fn set_bucket_map(&self, index: &str, map: &BucketMap) -> Result<()> {
        let mut indexes = self.write_map();
        let entry = indexes
            .get_mut(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        entry.bucket_owners = map.owners().to_vec();
        drop(indexes);
        self.persist_snapshot()
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

    /// **Plan** a reshard of `index` to `new_shard_count` (task-77): the bounded, balanced
    /// bucket→shard reassignment to reach the new count — computed, **not applied**. The returned
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

    // ---- window map (task-81) --------------------------------------------------

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

    // ---- CP-driven windowed placement (task-219) -------------------------------

    /// Record a node's liveness **heartbeat** for a windowed `index` (task-219): the node calls this
    /// on registration + on an interval to stay in the placement pool. In-memory only (see
    /// [`node_heartbeats`](Self::node_heartbeats)); `now_ms` is the control plane's wall clock. No
    /// index existence check — a node may heartbeat before its first window is placed.
    pub fn register_node(&self, index: &str, endpoint: &str, now_ms: i64) {
        self.node_heartbeats
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .entry(index.to_string())
            .or_default()
            .insert(endpoint.to_string(), now_ms);
    }

    /// The endpoints of nodes whose heartbeat is within [`NODE_HEARTBEAT_TTL_MS`] of `now_ms` — the
    /// **live** placement pool for `index` (sorted, so tie-breaks are deterministic).
    pub fn live_nodes(&self, index: &str, now_ms: i64) -> Vec<String> {
        self.node_heartbeats
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(index)
            .into_iter()
            .flatten()
            .filter(|(_, hb)| now_ms - **hb <= NODE_HEARTBEAT_TTL_MS)
            .map(|(ep, _)| ep.clone())
            .collect()
    }

    /// Resolve the node that owns `window` of `index`, **placing it on first ask** — the CP-driven
    /// windowed assignment (task-219). The connector/writer calls this to learn where to route a row
    /// whose window it just computed.
    ///
    /// - **Idempotent:** a window already assigned to a **live** node returns that node (`created =
    ///   false`), so repeated asks are stable.
    /// - **Placement:** an unassigned window — or one whose owner is **dead** (no heartbeat within the
    ///   TTL) — is placed on the **least-loaded** live node (fewest windows currently owned; ties
    ///   broken by endpoint, so placement is deterministic), recorded via the durable window map, and
    ///   returned with `created = true`.
    ///
    /// Errors if the index is unregistered, or if **no live node** is available to place a needed
    /// window (the caller retries once a node heartbeats). Re-placing a dead owner's window only moves
    /// the *assignment* — the new owner rebuilds that window's data from source on demand (a later
    /// stage); this method is the placement authority, not the data mover.
    pub fn resolve_window_owner(
        &self,
        index: &str,
        window: i64,
        now_ms: i64,
    ) -> Result<(String, bool)> {
        let live = self.live_nodes(index, now_ms);
        let mut map = self.write_map();
        let entry = map
            .get_mut(index)
            .ok_or_else(|| RegistryError::NotFound(index.to_string()))?;
        // A live current owner is authoritative — idempotent, no write.
        if let Some(primary) = entry
            .windows
            .get(&window)
            .and_then(|w| w.assignment.primary.as_ref())
        {
            if live.iter().any(|e| e == &primary.0) {
                return Ok((primary.0.clone(), false));
            }
        }
        // Needs placement (unassigned or dead owner). Count each live node's current window load,
        // then pick the least-loaded (BTreeMap iterates endpoints sorted, so the first minimum is the
        // smallest endpoint — deterministic ties).
        if live.is_empty() {
            return Err(RegistryError::NoLiveNode {
                index: index.to_string(),
                window,
            });
        }
        let mut load: BTreeMap<&str, usize> = live.iter().map(|e| (e.as_str(), 0usize)).collect();
        for wa in entry.windows.values() {
            if let Some(p) = &wa.assignment.primary {
                if let Some(c) = load.get_mut(p.0.as_str()) {
                    *c += 1;
                }
            }
        }
        let chosen = load
            .iter()
            .min_by_key(|(_, c)| **c)
            .map(|(ep, _)| ep.to_string())
            .expect("live pool non-empty");
        entry.windows.entry(window).or_default().assignment.primary = Some(chosen.clone().into());
        drop(map);
        self.persist_snapshot()?;
        Ok((chosen, true))
    }
}

impl Drop for Registry {
    /// Flush any activity events a debounce window left in memory (task-151 / B11) so a graceful
    /// shutdown doesn't lose the tail of a burst. Best-effort; runs before `_lock`'s flock releases.
    fn drop(&mut self) {
        let dirty = self
            .activity_flush
            .get_mut()
            .map(|f| f.dirty)
            .unwrap_or(false);
        if dirty {
            let log = self.activity.get_mut().unwrap_or_else(|e| e.into_inner());
            if let Err(e) = persist_activity(&self.activity_path, log) {
                eprintln!("registry: failed to flush activity log on shutdown ({e})");
            }
        }
    }
}

/// Load the catalog from `path`, parsing the `{ version, indexes }` envelope. On a parse failure
/// (a corrupt file), fall back to the last-known-good `.prev` copy with a loud warning instead of
/// hard-failing — bricking the control plane on a single bad file would be worse (task-70).
/// Match an index `pattern` (a `*`-glob like `events-*` / `*-2025` / `a*b`) against `name`.
/// `*` matches any (possibly empty) run of characters; there is no `?`. Index names are ASCII
/// identifiers, so byte-slicing on the literal segments is safe. Public so clients filtering a
/// listed index set (e.g. CLI retention, task-52) match patterns the same way `resolve` does.
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

#[allow(clippy::type_complexity)]
type LoadedRegistry = (
    BTreeMap<String, IndexEntry>,
    BTreeMap<String, BTreeSet<String>>,
    BTreeMap<String, SavedQuery>,
    BTreeMap<String, Vec<String>>,
    BTreeMap<String, ApiToken>,
    BTreeMap<String, String>,
    BTreeMap<String, Vec<String>>,
);

fn load(path: &std::path::Path) -> Result<LoadedRegistry> {
    fn parse(bytes: &[u8]) -> Result<LoadedRegistry> {
        let f = serde_json::from_slice::<RegistryFile>(bytes)?;
        Ok((
            f.indexes,
            f.aliases,
            f.saved_queries,
            f.role_bindings,
            f.tokens,
            f.credentials,
            f.index_bindings,
        ))
    }
    match std::fs::read(path)
        .map_err(RegistryError::from)
        .and_then(|b| parse(&b))
    {
        Ok(loaded) => Ok(loaded),
        Err(primary) => {
            let prev = growlerdb_core::durable::prev_path(path);
            if prev.exists() {
                eprintln!(
                    "warning: registry `{}` failed to load ({primary}); falling back to `{}`",
                    path.display(),
                    prev.display()
                );
                parse(&std::fs::read(&prev)?)
            } else {
                Err(primary)
            }
        }
    }
}

/// Persist the activity sidecar (task-110) durably (temp + rename + fsync).
fn persist_activity(
    path: &std::path::Path,
    log: &BTreeMap<String, Vec<ActivityEvent>>,
) -> Result<()> {
    let json = serde_json::to_vec_pretty(log)?;
    growlerdb_core::durable::write_keeping_prev(path, &json)?;
    Ok(())
}

fn persist_sessions(path: &std::path::Path, epochs: &BTreeMap<String, i64>) -> Result<()> {
    let json = serde_json::to_vec_pretty(epochs)?;
    growlerdb_core::durable::write_keeping_prev(path, &json)?;
    Ok(())
}

/// Epoch milliseconds (task-106 saved-query timestamps).
fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{IndexDefinition, SourceField, SourceSchema, SourceType};

    #[test]
    fn credentials_hash_verify_and_persist() {
        // task-128: built-in credentials are salted-argon2-hashed, verified, persisted, and the
        // plaintext is never written to disk.
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
        // task-151 / B11: an isolated activity event flushes to the sidecar immediately (durability
        // preserved), a same-window burst coalesces (later events aren't fsynced per-event), and the
        // debounced tail is flushed on graceful shutdown so nothing is lost across a restart.
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
        // task-151 / F10: each mutation persists the FULL snapshot (all maps), not just the one it
        // changed — so interleaved mutations to different maps all survive a restart.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("registry.json");
        {
            let reg = Registry::open(&path).unwrap();
            reg.create(resolved("docs")).unwrap(); // indexes
            reg.set_alias("d", ["docs"]).unwrap(); // aliases
            reg.set_user_roles("alice", vec!["admin".into()]).unwrap(); // role_bindings
            reg.set_credential("alice", "pw").unwrap(); // credentials
            reg.set_user_indexes("alice", vec!["docs".into(), "catalog".into()])
                .unwrap(); // index_bindings (task-244)
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
        // task-244: a subject's index allowlist is de-duplicated, revokes outstanding sessions on
        // change (so a re-scoped session must re-authenticate), and clears when set empty.
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
        assert!(
            epoch > 0,
            "a scope change bumps the session epoch (task-147 / B4)"
        );
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
        // task-219: CP-driven placement spreads windows evenly over live nodes, deterministically,
        // and is idempotent for an already-placed live window.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("logs")).unwrap();
        let t0 = 1_000_000;
        for n in ["node-a", "node-b", "node-c"] {
            reg.register_node("logs", n, t0);
        }
        // Placing 6 windows round-robins evenly across the 3 live nodes (least-loaded each step).
        let mut owners = Vec::new();
        for w in 0..6 {
            let (ep, created) = reg.resolve_window_owner("logs", w, t0).unwrap();
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
        let (ep, created) = reg.resolve_window_owner("logs", 0, t0).unwrap();
        assert_eq!(ep, "node-a");
        assert!(!created);
    }

    #[test]
    fn window_placement_reaps_a_dead_owner_and_re_places() {
        // task-219: a window whose owner stops heartbeating (past the TTL) is re-placed on a live
        // node; with no live node at all, placement errors so the caller retries.
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("logs")).unwrap();
        let t0 = 1_000_000;
        reg.register_node("logs", "node-a", t0);
        let (ep, _) = reg.resolve_window_owner("logs", 7, t0).unwrap();
        assert_eq!(ep, "node-a");

        // node-a goes silent; only node-b heartbeats, past node-a's TTL → node-a is dead.
        let t1 = t0 + NODE_HEARTBEAT_TTL_MS + 1;
        reg.register_node("logs", "node-b", t1);
        let (ep, created) = reg.resolve_window_owner("logs", 7, t1).unwrap();
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
            reg.resolve_window_owner("logs", 99, t2),
            Err(RegistryError::NoLiveNode { .. })
        ));
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

            // Storing a balanced(4) map adopts buckets; it reads back identically.
            let map = BucketMap::balanced(4);
            reg.set_bucket_map("docs", &map).unwrap();
            assert_eq!(reg.bucket_map("docs"), Some(map.clone()));
        }
        // Survives a reopen (persisted in registry.json).
        let reg = Registry::open(&path).unwrap();
        assert_eq!(reg.bucket_map("docs"), Some(BucketMap::balanced(4)));
        // Unknown index ⇒ None, not an error.
        assert!(reg.bucket_map("nope").is_none());
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

        // One call assigns every ordinal to the node (task-202 batched bring-up, one persist).
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

        // Promoting with a primary still assigned is refused — fence it first (task-74).
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
        // The split-brain-avoidance precondition (task-74 M1): with a live primary assigned,
        // promote_replica refuses — the lease driver must clear/fence the old primary first.
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
        // A second open of the same registry fails fast while the first holds the lock (task-74).
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
        // A panic while holding the write lock must not wedge the registry (task-74 M4).
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
        // task-151 / B13: O(1) hash lookup, expiry enforcement + pruning, and a derived index that's
        // rebuilt on open.
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
        // task-151 / B5: every mutation holds only the map it changes (persist_snapshot re-reads the
        // rest off-lock), so hammering different auth maps concurrently completes without a
        // lock-order deadlock and each change is durably persisted.
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
}
