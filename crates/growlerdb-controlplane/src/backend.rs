//! The registry **persistence backend** seam.
//!
//! [`Registry`](crate::Registry) holds all in-memory state + logic; *where* that state is durably
//! stored sits behind [`RegistryBackend`]. The default [`JsonFileBackend`] is a single-writer JSON
//! store; a replicated backend (Postgres/etcd, so the control plane can run as N stateless replicas
//! — D51) implements the same trait without touching the registry's logic.
//!
//! The interface is deliberately **snapshot-shaped** — a mutation applies in memory, then hands the
//! backend the full core snapshot to persist — which maps cleanly onto a transactional upsert.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::registry::{
    ActivityEvent, ApiToken, IndexEntry, RegistryError, ReindexJob, Result, SavedQuery,
};

/// On-disk schema version for the registry envelope, so a future format change can be detected and
/// migrated instead of mis-parsed. Shared by every backend.
pub(crate) const REGISTRY_VERSION: u32 = 1;

/// The full state loaded from a backend on [`Registry`](crate::Registry) open: the core catalog maps
/// plus the two sidecar maps (activity log, session epochs).
#[derive(Debug, Default)]
pub struct PersistedState {
    pub indexes: BTreeMap<String, IndexEntry>,
    pub aliases: BTreeMap<String, BTreeSet<String>>,
    pub saved_queries: BTreeMap<String, SavedQuery>,
    pub role_bindings: BTreeMap<String, Vec<String>>,
    pub tokens: BTreeMap<String, ApiToken>,
    pub credentials: BTreeMap<String, String>,
    pub index_bindings: BTreeMap<String, Vec<String>>,
    pub activity: BTreeMap<String, Vec<ActivityEvent>>,
    pub session_epochs: BTreeMap<String, i64>,
    /// Coordinated reindex/alter jobs: `id → `[`ReindexJob`]. A separate `jobs.json` sidecar (like
    /// the activity log) — progress/observability state, not catalog truth.
    pub jobs: BTreeMap<String, ReindexJob>,
}

/// The core catalog snapshot a mutation hands the backend to persist — the seven durable maps of the
/// `registry.json` envelope. The activity log and session epochs persist through their own backend
/// methods, as independent sidecars with different durability rules.
#[derive(Debug)]
pub struct RegistrySnapshot {
    pub indexes: BTreeMap<String, IndexEntry>,
    pub aliases: BTreeMap<String, BTreeSet<String>>,
    pub saved_queries: BTreeMap<String, SavedQuery>,
    pub role_bindings: BTreeMap<String, Vec<String>>,
    pub tokens: BTreeMap<String, ApiToken>,
    pub credentials: BTreeMap<String, String>,
    pub index_bindings: BTreeMap<String, Vec<String>>,
}

/// Where the registry's durable state lives. The default [`JsonFileBackend`] is the local
/// single-writer JSON store; a replicated backend (D51) implements the same trait against an external
/// store.
///
/// **Single-writer** is the backend's responsibility: [`JsonFileBackend::open`] takes an exclusive
/// advisory `flock`; a store-backed backend enforces it with a lease / conditional write.
pub trait RegistryBackend: Send + Sync {
    /// Load the full persisted state (empty when nothing is stored yet).
    fn load(&self) -> Result<PersistedState>;

    /// Durably persist the core catalog snapshot (the `registry.json` envelope). Called off any
    /// registry data lock, serialized by the registry's `flush_lock`.
    fn persist_registry(&self, snapshot: RegistrySnapshot) -> Result<()>;

    /// Durably persist the activity-log sidecar. Best-effort at the call site (a failure is logged,
    /// never fails the mutation that recorded the event).
    fn persist_activity(&self, activity: &BTreeMap<String, Vec<ActivityEvent>>) -> Result<()>;

    /// Durably persist the session-epoch sidecar. Best-effort at the call site.
    fn persist_sessions(&self, sessions: &BTreeMap<String, i64>) -> Result<()>;

    /// Durably persist the reindex-jobs sidecar. Best-effort at the call site (a failure is logged,
    /// never fails the reindex the job records). Default no-op: a backend that doesn't persist jobs
    /// keeps them in-memory only — on that backend a control-plane restart simply forgets in-flight
    /// jobs, which is safe (the reindex's own generation CAS is the durable source of truth).
    fn persist_jobs(&self, _jobs: &BTreeMap<String, ReindexJob>) -> Result<()> {
        Ok(())
    }

    // ---- leadership & change-polling (for N-replica HA over a shared store) -------------------
    // The defaults model the local single-writer file: unconditionally the leader (its `flock` made
    // it the sole writer) with no standbys. A replicated backend overrides these so a read-only
    // standby can detect the leader's writes and take over on its death.

    /// The store's monotonic registry version, when the backend supports change-polling. A **standby**
    /// polls this and calls [`Registry::reload`](crate::Registry::reload) when it advances — the
    /// leader wrote. `None` = unsupported (the local JSON file has one writer and no standbys).
    fn poll_version(&self) -> Result<Option<i64>> {
        Ok(None)
    }

    /// Attempt to acquire the store's writer lock. `Ok(true)` means this backend now **holds the
    /// lock but is not yet the writer**: the caller must first reload from the store and only then
    /// [`confirm_leadership`](RegistryBackend::confirm_leadership) (or
    /// [`resign_leadership`](RegistryBackend::resign_leadership) if that reload fails) — so a
    /// promoted leader can never persist a snapshot built from a stale catalog and overwrite the
    /// dead leader's last writes. Default: unconditionally `true` (the [`JsonFileBackend`]'s `flock`
    /// leaves no other writer to be stale against).
    fn try_become_leader(&self) -> Result<bool> {
        Ok(true)
    }

    /// Mark this backend the active writer. Called only after
    /// [`try_become_leader`](RegistryBackend::try_become_leader) returned `true` **and** the caller
    /// reloaded from the store. Default no-op (the local backend is always leader).
    fn confirm_leadership(&self) {}

    /// Give up writer status, releasing the store's writer lock if the session is still alive —
    /// after a failed promotion reload, a persist that revealed another writer, or any signal the
    /// lock-holding store session died. Default no-op (the `flock` cannot be resigned mid-process).
    fn resign_leadership(&self) {}

    /// Cheap per-tick check that leadership is still held — i.e. the store session that owns the
    /// writer lock is alive. `Ok(false)` = leadership lost: the caller must stop serving writes and
    /// fall back to the standby path. Default: [`is_leader`](RegistryBackend::is_leader) (the local
    /// `flock` cannot be lost while the process runs).
    fn verify_leadership(&self) -> Result<bool> {
        Ok(self.is_leader())
    }

    /// Whether this backend currently holds write leadership. Default `true` (see
    /// [`try_become_leader`](RegistryBackend::try_become_leader)). A backend that is **not** leader
    /// must refuse every persist so a standby can never corrupt the shared store.
    fn is_leader(&self) -> bool {
        true
    }
}

/// The persisted registry **envelope**: a `{ version, indexes, … }` document around the catalog,
/// rather than a bare map — so the format is self-describing and evolvable. `#[serde(default)]` on
/// every non-core map means older files (written before a field existed) load cleanly. Every backend
/// serializes exactly this envelope, so the JSON file and the Postgres JSONB document are
/// byte-for-byte interchangeable.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct RegistryFile {
    version: u32,
    indexes: BTreeMap<String, IndexEntry>,
    #[serde(default)]
    aliases: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    saved_queries: BTreeMap<String, SavedQuery>,
    #[serde(default)]
    role_bindings: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    tokens: BTreeMap<String, ApiToken>,
    #[serde(default)]
    credentials: BTreeMap<String, String>,
    #[serde(default)]
    index_bindings: BTreeMap<String, Vec<String>>,
}

impl RegistryFile {
    /// An empty envelope at the current version (a store with nothing persisted yet).
    pub(crate) fn empty() -> Self {
        Self {
            version: REGISTRY_VERSION,
            indexes: BTreeMap::new(),
            aliases: BTreeMap::new(),
            saved_queries: BTreeMap::new(),
            role_bindings: BTreeMap::new(),
            tokens: BTreeMap::new(),
            credentials: BTreeMap::new(),
            index_bindings: BTreeMap::new(),
        }
    }

    /// Wrap a core [`RegistrySnapshot`] as a versioned envelope, ready to serialize.
    pub(crate) fn from_snapshot(snapshot: RegistrySnapshot) -> Self {
        Self {
            version: REGISTRY_VERSION,
            indexes: snapshot.indexes,
            aliases: snapshot.aliases,
            saved_queries: snapshot.saved_queries,
            role_bindings: snapshot.role_bindings,
            tokens: snapshot.tokens,
            credentials: snapshot.credentials,
            index_bindings: snapshot.index_bindings,
        }
    }

    /// Combine this core envelope with the separately-stored sidecars into the full
    /// [`PersistedState`] the registry loads into memory.
    pub(crate) fn into_persisted(
        self,
        activity: BTreeMap<String, Vec<ActivityEvent>>,
        session_epochs: BTreeMap<String, i64>,
        jobs: BTreeMap<String, ReindexJob>,
    ) -> PersistedState {
        PersistedState {
            indexes: self.indexes,
            aliases: self.aliases,
            saved_queries: self.saved_queries,
            role_bindings: self.role_bindings,
            tokens: self.tokens,
            credentials: self.credentials,
            index_bindings: self.index_bindings,
            activity,
            session_epochs,
            jobs,
        }
    }
}

/// The default backend: the local **single-writer JSON store**. Holds the exclusive `flock` for the
/// process lifetime, writes the `registry.json` envelope and the `activity.json` / `sessions.json`
/// sidecars with atomic temp+rename (keeping a `.prev` fallback).
pub struct JsonFileBackend {
    path: PathBuf,
    activity_path: PathBuf,
    session_epochs_path: PathBuf,
    jobs_path: PathBuf,
    /// Held for the backend's lifetime to keep the exclusive `flock`; released on drop / exit.
    _lock: File,
}

impl JsonFileBackend {
    /// Open the JSON store at `path`, taking the exclusive single-writer lock first (fails fast with
    /// [`Locked`](RegistryError::Locked) if another process holds it). The sidecars live alongside
    /// `registry.json` (`activity.json`, `sessions.json`).
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        // Lock a stable `.lock` sibling, not the data file: the data file is renamed over, so a lock
        // on it wouldn't be stable across writes.
        let lock_path = path.with_extension("lock");
        let lock = File::create(&lock_path)?;
        lock.try_lock_exclusive()
            .map_err(|_| RegistryError::Locked(lock_path))?;
        let activity_path = path.with_file_name("activity.json");
        let session_epochs_path = path.with_file_name("sessions.json");
        let jobs_path = path.with_file_name("jobs.json");
        Ok(Self {
            path,
            activity_path,
            session_epochs_path,
            jobs_path,
            _lock: lock,
        })
    }
}

impl RegistryBackend for JsonFileBackend {
    fn load(&self) -> Result<PersistedState> {
        let core = if self.path.exists() {
            load_core(&self.path)?
        } else {
            RegistryFile::empty()
        };
        // Sidecars are best-effort: a missing/corrupt log or session file starts empty rather than
        // failing startup — they're audit convenience / revocation state, not catalog truth.
        let activity = std::fs::read(&self.activity_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let session_epochs = std::fs::read(&self.session_epochs_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let jobs = std::fs::read(&self.jobs_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        Ok(core.into_persisted(activity, session_epochs, jobs))
    }

    fn persist_registry(&self, snapshot: RegistrySnapshot) -> Result<()> {
        let json = serde_json::to_vec_pretty(&RegistryFile::from_snapshot(snapshot))?;
        growlerdb_core::durable::write_keeping_prev(&self.path, &json)?;
        Ok(())
    }

    fn persist_activity(&self, activity: &BTreeMap<String, Vec<ActivityEvent>>) -> Result<()> {
        let json = serde_json::to_vec_pretty(activity)?;
        growlerdb_core::durable::write_keeping_prev(&self.activity_path, &json)?;
        Ok(())
    }

    fn persist_sessions(&self, sessions: &BTreeMap<String, i64>) -> Result<()> {
        let json = serde_json::to_vec_pretty(sessions)?;
        growlerdb_core::durable::write_keeping_prev(&self.session_epochs_path, &json)?;
        Ok(())
    }

    fn persist_jobs(&self, jobs: &BTreeMap<String, ReindexJob>) -> Result<()> {
        let json = serde_json::to_vec_pretty(jobs)?;
        growlerdb_core::durable::write_keeping_prev(&self.jobs_path, &json)?;
        Ok(())
    }
}

/// Load the core catalog from `path`. On a parse failure (a corrupt file), fall back to the
/// last-known-good `.prev` copy with a loud warning rather than bricking the control plane.
fn load_core(path: &Path) -> Result<RegistryFile> {
    fn parse(bytes: &[u8]) -> Result<RegistryFile> {
        Ok(serde_json::from_slice::<RegistryFile>(bytes)?)
    }
    match std::fs::read(path)
        .map_err(RegistryError::from)
        .and_then(|b| parse(&b))
    {
        Ok(loaded) => Ok(loaded),
        Err(primary) => {
            let prev = growlerdb_core::durable::prev_path(path);
            if prev.exists() {
                tracing::warn!(
                    path = %path.display(),
                    fallback = %prev.display(),
                    error = %primary,
                    "registry failed to load; falling back to the previous snapshot"
                );
                parse(&std::fs::read(&prev)?)
            } else {
                Err(primary)
            }
        }
    }
}
