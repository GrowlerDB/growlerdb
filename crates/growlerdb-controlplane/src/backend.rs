//! The registry **persistence backend** seam.
//!
//! [`Registry`](crate::Registry) holds all in-memory state + logic; *where* that state is durably
//! stored sits behind [`RegistryBackend`]. The default [`JsonFileBackend`] is the current
//! single-writer JSON store — a `flock`'d `registry.json` envelope plus `activity.json` /
//! `sessions.json` sidecars, atomic temp+rename writes — byte-for-byte the pre-seam behavior. A
//! replicated backend (Postgres/etcd, so the control plane can run as N stateless replicas — see
//! `okf/system/decisions/d51-controlplane-ha.md`) implements the same trait without touching the
//! registry's logic.
//!
//! The interface is deliberately **snapshot-shaped** — a mutation applies in memory, then hands the
//! backend the full core snapshot to persist — which mirrors how the JSON store already rewrites the
//! whole envelope each write, and maps cleanly onto a transactional upsert for an external store.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::path::{Path, PathBuf};

use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::registry::{ActivityEvent, ApiToken, IndexEntry, RegistryError, Result, SavedQuery};

/// On-disk schema version for `registry.json`. A versioned envelope means a future format change
/// can be detected and migrated instead of mis-parsed.
const REGISTRY_VERSION: u32 = 1;

/// The full state loaded from a backend on [`Registry`](crate::Registry) open: the core catalog maps
/// plus the two sidecar maps (activity log, session epochs). The registry moves each field into its
/// own `RwLock` from here.
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
}

/// The core catalog snapshot a mutation hands the backend to persist — the seven durable maps of the
/// `registry.json` envelope (the activity log and session epochs persist through their own backend
/// methods, since they're independent sidecars with different durability rules). Owned, so the JSON
/// backend serializes it without a second clone.
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
/// single-writer JSON store; a replicated backend (D51) implements the same three persist paths plus
/// [`load`](RegistryBackend::load) against an external store.
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
}

/// The persisted `registry.json` document: a `{ version, indexes, … }` envelope around the catalog,
/// rather than a bare map — so the format is self-describing and evolvable. `#[serde(default)]` on
/// every non-core map means older registry files (written before a field existed) load cleanly.
#[derive(Debug, Serialize, Deserialize)]
struct RegistryFile {
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

/// The default backend: the local **single-writer JSON store**. Holds the exclusive `flock` for the
/// process lifetime, writes the `registry.json` envelope and the `activity.json` / `sessions.json`
/// sidecars with atomic temp+rename (keeping a `.prev` fallback), exactly as the control plane did
/// before the backend seam.
pub struct JsonFileBackend {
    path: PathBuf,
    activity_path: PathBuf,
    session_epochs_path: PathBuf,
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
        // Exclusive single-writer lock on a stable `.lock` sibling (the data file is renamed over,
        // so locking it directly wouldn't be stable across writes).
        let lock_path = path.with_extension("lock");
        let lock = File::create(&lock_path)?;
        lock.try_lock_exclusive()
            .map_err(|_| RegistryError::Locked(lock_path))?;
        let activity_path = path.with_file_name("activity.json");
        let session_epochs_path = path.with_file_name("sessions.json");
        Ok(Self {
            path,
            activity_path,
            session_epochs_path,
            _lock: lock,
        })
    }
}

impl RegistryBackend for JsonFileBackend {
    fn load(&self) -> Result<PersistedState> {
        let core = if self.path.exists() {
            load_core(&self.path)?
        } else {
            RegistryFile {
                version: REGISTRY_VERSION,
                indexes: BTreeMap::new(),
                aliases: BTreeMap::new(),
                saved_queries: BTreeMap::new(),
                role_bindings: BTreeMap::new(),
                tokens: BTreeMap::new(),
                credentials: BTreeMap::new(),
                index_bindings: BTreeMap::new(),
            }
        };
        // Sidecars are best-effort: a missing/corrupt log or session file starts empty rather than
        // failing registry startup (they're a non-critical audit convenience / revocation state, not
        // catalog truth).
        let activity = std::fs::read(&self.activity_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let session_epochs = std::fs::read(&self.session_epochs_path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        Ok(PersistedState {
            indexes: core.indexes,
            aliases: core.aliases,
            saved_queries: core.saved_queries,
            role_bindings: core.role_bindings,
            tokens: core.tokens,
            credentials: core.credentials,
            index_bindings: core.index_bindings,
            activity,
            session_epochs,
        })
    }

    fn persist_registry(&self, snapshot: RegistrySnapshot) -> Result<()> {
        let file = RegistryFile {
            version: REGISTRY_VERSION,
            indexes: snapshot.indexes,
            aliases: snapshot.aliases,
            saved_queries: snapshot.saved_queries,
            role_bindings: snapshot.role_bindings,
            tokens: snapshot.tokens,
            credentials: snapshot.credentials,
            index_bindings: snapshot.index_bindings,
        };
        let json = serde_json::to_vec_pretty(&file)?;
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
}

/// Load the core catalog from `path`, parsing the `{ version, indexes, … }` envelope. On a parse
/// failure (a corrupt file), fall back to the last-known-good `.prev` copy with a loud warning
/// instead of hard-failing — bricking the control plane on a single bad file would be worse.
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
