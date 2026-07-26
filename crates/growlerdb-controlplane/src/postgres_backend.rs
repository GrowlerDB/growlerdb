//! The **externalized Postgres registry backend** (D51) — behind the `postgres` feature.
//!
//! Stores the same versioned registry envelope the [`JsonFileBackend`](crate::JsonFileBackend) writes
//! to disk, but as JSONB rows in a shared Postgres, so the control plane can run as **N stateless
//! replicas** over one durable store. Single-writer is the store's job: the **leader** holds a
//! **session-level advisory lock** — the direct successor to the JSON backend's `flock` — so a
//! second control plane over the same database cannot also become writer.
//!
//! Both roles live here: [`open`](PostgresBackend::open) connects and takes leadership (or errors if
//! held); [`open_standby`](PostgresBackend::open_standby) connects read-only. A standby serves reads,
//! [polls the version](RegistryBackend::poll_version) and [reloads](crate::Registry::reload) when the
//! leader writes, refuses every persist ([`is_leader`](RegistryBackend::is_leader) guard), and — when
//! the leader dies and Postgres releases the lock — promotes itself via
//! [`try_become_leader`](RegistryBackend::try_become_leader). The `growlerdb control-plane` run-loop
//! that drives these over a live gRPC server (write-routing + readiness) is the follow-on slice.
//!
//! The write-coordination model is **leader-writer + reloading standbys**: exactly one replica holds
//! the advisory lock and writes; two concurrent placement ops can only ever run on the one leader,
//! where the existing in-memory expected-map check serializes them. The advisory lock alone is not
//! trusted as the last line of defense, though — every persist also carries an **expected-version
//! guard** (optimistic concurrency per `cp_state` row), so a replica whose leadership silently
//! lapsed (dead session, partition) gets a [`NotLeader`](crate::RegistryError::NotLeader) refusal
//! from the store instead of overwriting the real leader's writes (D51: the CAS maps to the store).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Sender};
use std::sync::Mutex;

use postgres::{Client, NoTls};
use serde_json::Value;

use crate::backend::{PersistedState, RegistryBackend, RegistryFile, RegistrySnapshot};
use crate::registry::{ActivityEvent, RegistryError, Result};

/// A unit of work handed to the backend's dedicated Postgres thread: it borrows the one `Client`,
/// runs the query, and returns its own typed result to the caller over a private channel.
type Job = Box<dyn FnOnce(&mut Client) + Send>;

/// Fixed key for the session-level advisory lock that enforces single-writer across replicas — the
/// Postgres successor to the JSON backend's `flock`. Any constant `bigint`; scoped to the database.
const WRITER_LOCK_KEY: i64 = 0x4744_425F_4350_4C4B; // "GDB_CPLK"

/// Fixed key for the transaction-scoped advisory lock that serializes schema DDL across replicas
/// booting concurrently — `CREATE TABLE IF NOT EXISTS` from N connections at once races on
/// `pg_type`/`pg_class` duplicate keys. Distinct from [`WRITER_LOCK_KEY`]: standbys must be able to
/// migrate while a leader holds the writer lock.
const SCHEMA_LOCK_KEY: i64 = 0x4744_425F_4350_4444; // "GDB_CPDD"

/// Idempotent schema: one row per logical document (`registry` / `activity` / `sessions`), each a
/// JSONB envelope plus a monotonic `version` a standby polls to detect a leader's write.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS cp_state ( \
  key TEXT PRIMARY KEY, \
  version BIGINT NOT NULL DEFAULT 0, \
  doc JSONB NOT NULL \
);";

/// The externalized Postgres [`RegistryBackend`]. The blocking `postgres` client drives its **own**
/// tokio runtime under the hood, so it must never be called from a thread that is already inside the
/// control plane's async runtime (its gRPC handlers) — that panics with *"cannot start a runtime from
/// within a runtime"*. So the `Client` is confined to one **dedicated worker thread** with no ambient
/// runtime; every op is marshaled to it over [`jobs`](Self::jobs) and blocks for the reply. That one
/// thread also holds the single session that owns the single-writer advisory lock, and serializes all
/// access (no `Mutex<Client>` needed). `is_writer` tracks leadership so a standby refuses every
/// persist until it is promoted.
///
/// **Leadership is only as alive as the session.** If the session dies (Postgres restart, network
/// drop, a job panic unwinding the worker thread), Postgres releases the advisory lock and another
/// replica may promote — so any signal the session is gone demotes this backend immediately
/// (`is_writer = false`): a worker send/recv failure in [`on_worker`](Self::on_worker), a failed
/// [`verify_leadership`](RegistryBackend::verify_leadership) probe, or a persist error. A demoted
/// backend re-enters the standby race with a **fresh connection**
/// ([`try_become_leader`](RegistryBackend::try_become_leader) revives the worker if the old session
/// died).
pub struct PostgresBackend {
    /// Connection string, kept so a dead session can be replaced by a fresh worker.
    url: String,
    /// Channel to the current worker thread. Behind a mutex only so
    /// [`revive_worker`](Self::revive_worker) can swap in a fresh sender; held just long enough to
    /// clone.
    jobs: Mutex<Sender<Job>>,
    is_writer: AtomicBool,
    /// The `version` each `cp_state` row had when last loaded or persisted — the expected-version
    /// guard for optimistic concurrency (D51: "the placement CAS maps to the store"). A persist
    /// whose expected version no longer matches means another writer exists → demote.
    versions: Mutex<BTreeMap<String, i64>>,
}

impl PostgresBackend {
    /// Connect to `url` as the **leader**: ensure the schema and acquire the single-writer advisory
    /// lock (the Postgres equivalent of the JSON backend taking the `flock`). Errors with
    /// [`Backend`](RegistryError::Backend) if the lock is already held by another control plane
    /// (single-writer enforced by the store) or on any connect/DDL failure. Use
    /// [`open_standby`](Self::open_standby) for a warm read replica.
    pub fn open(url: &str) -> Result<Self> {
        let backend = Self::open_standby(url)?;
        if !backend.try_become_leader()? {
            return Err(RegistryError::Backend(
                "registry postgres single-writer lock is held by another control plane".into(),
            ));
        }
        // Safe to confirm without the promotion reload: no registry memory exists yet — the caller
        // ([`Registry::with_backend`]) loads fresh from the store before any mutation is possible.
        backend.confirm_leadership();
        Ok(backend)
    }

    /// Connect to `url` as a **standby**: ensure the schema but do **not** take the writer lock. The
    /// backend can [`load`](RegistryBackend::load) and [`poll_version`](RegistryBackend::poll_version)
    /// (serve reads + watch for the leader's writes) but refuses every persist until promoted via
    /// [`try_become_leader`](RegistryBackend::try_become_leader).
    ///
    /// Spawns the dedicated worker thread and connects **on it** (off any runtime); the connect +
    /// schema result is handed back so this returns the same errors the inline connect used to.
    pub fn open_standby(url: &str) -> Result<Self> {
        let jobs = spawn_worker(url.to_string())?;
        Ok(Self {
            url: url.to_string(),
            jobs: Mutex::new(jobs),
            is_writer: AtomicBool::new(false),
            versions: Mutex::new(BTreeMap::new()),
        })
    }

    /// Run `f` against the confined `Client` on the worker thread and block for its result — the sole
    /// path to the database, so no query ever runs on a runtime worker thread.
    ///
    /// A send/recv failure means the worker thread is **gone** (a job panicked and unwound it): its
    /// session — and the advisory lock with it — is released, so this demotes immediately rather
    /// than leaving a lockless "leader" serving a frozen catalog.
    fn on_worker<R, F>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut Client) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let (tx, rx) = channel();
        let jobs = self.jobs.lock().unwrap_or_else(|e| e.into_inner()).clone();
        if jobs
            .send(Box::new(move |client| {
                let _ = tx.send(f(client));
            }))
            .is_err()
        {
            self.is_writer.store(false, Ordering::SeqCst);
            return Err(backend_err("control-plane postgres worker is gone"));
        }
        match rx.recv() {
            Ok(result) => result,
            Err(_) => {
                self.is_writer.store(false, Ordering::SeqCst);
                Err(backend_err(
                    "control-plane postgres worker dropped the reply",
                ))
            }
        }
    }

    /// Whether the worker's session still answers — the liveness of the session **is** the liveness
    /// of any advisory lock it holds (a session-level lock survives exactly as long as the session).
    fn session_alive(&self) -> bool {
        self.on_worker(|client| {
            client.simple_query("SELECT 1").map_err(backend_err)?;
            Ok(())
        })
        .is_ok()
    }

    /// Replace a dead worker/session with a freshly connected one, so a demoted replica can rejoin
    /// the standby race (poll + re-acquire) after its original connection died with the old tenure.
    fn revive_worker(&self) -> Result<()> {
        let fresh = spawn_worker(self.url.clone())?;
        // Swapping the sender drops the old one; a still-running old worker exits its recv loop and
        // its client (and any stale lock) is released.
        *self.jobs.lock().unwrap_or_else(|e| e.into_inner()) = fresh;
        Ok(())
    }

    /// The store-wide monotonic version: the **sum** of every row's version. Each write bumps
    /// exactly one row by one, so the sum strictly increases on any write — a sessions-only bump (a
    /// revocation) is visible to polling standbys, not just registry-envelope writes.
    fn store_version(&self) -> Result<i64> {
        self.on_worker(|client| {
            client
                .query_one(
                    "SELECT COALESCE(SUM(version), 0)::BIGINT FROM cp_state",
                    &[],
                )
                .map_err(backend_err)?
                .try_get(0)
                .map_err(backend_err)
        })
    }

    /// Refuse a write from a non-leader, so a standby can never corrupt the shared store even if a
    /// mis-routed write RPC reaches its registry.
    fn ensure_leader(&self) -> Result<()> {
        if self.is_writer.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(RegistryError::NotLeader(
                "this control plane is a standby; retry against the current leader".into(),
            ))
        }
    }

    /// Persist one document row under the **expected-version** guard: the row is only written if
    /// its `version` still equals what this backend last loaded/persisted (D51's store-level CAS).
    /// A mismatch means another writer bumped it — this replica's leadership is stale no matter
    /// what its advisory-lock flag says — so it **fail-stops**: resign leadership (no further stale
    /// persist can reach the store) and surface retryable [`NotLeader`](RegistryError::NotLeader);
    /// the leadership loop reloads and re-races as a standby. Any store error on the write path
    /// demotes the same way — the sync client does not recover a broken session, and a leader that
    /// cannot write durably must not keep claiming writership.
    fn persist_doc(&self, key: &'static str, doc: Value) -> Result<()> {
        self.ensure_leader()?;
        let expected = self
            .versions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(key)
            .copied()
            .unwrap_or(0);
        let updated = match self.on_worker(move |client| {
            client
                .execute(
                    "INSERT INTO cp_state (key, version, doc) VALUES ($1, $2::BIGINT + 1, $3) \
                     ON CONFLICT (key) DO UPDATE SET version = cp_state.version + 1, \
                     doc = EXCLUDED.doc WHERE cp_state.version = $2::BIGINT",
                    &[&key, &expected, &doc],
                )
                .map_err(backend_err)
        }) {
            Ok(n) => n,
            Err(e) => {
                self.resign_leadership();
                return Err(e);
            }
        };
        if updated == 1 {
            self.versions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(key.to_string(), expected + 1);
            Ok(())
        } else {
            self.resign_leadership();
            Err(RegistryError::NotLeader(format!(
                "registry row `{key}` version moved past {expected} — another writer took over"
            )))
        }
    }
}

/// Spawn the dedicated Postgres worker thread: connect + migrate **on it** (off any runtime) and
/// hand back the job channel once the connection is up (or the connect error).
fn spawn_worker(url: String) -> Result<Sender<Job>> {
    let (jobs, rx) = channel::<Job>();
    let (ready_tx, ready_rx) = channel::<Result<()>>();
    std::thread::Builder::new()
        .name("cp-postgres".into())
        .spawn(move || {
            let mut client = match connect_and_migrate(&url) {
                Ok(c) => {
                    let _ = ready_tx.send(Ok(()));
                    c
                }
                Err(e) => {
                    let _ = ready_tx.send(Err(e));
                    return;
                }
            };
            // Serve marshaled jobs until the backend is dropped; then the sender closes, this
            // loop ends, the `Client` drops, and Postgres releases the advisory lock — the exact
            // signal a standby waits on to promote itself after the leader dies.
            while let Ok(job) = rx.recv() {
                job(&mut client);
            }
        })
        .map_err(backend_err)?;
    ready_rx
        .recv()
        .map_err(|_| backend_err("control-plane postgres worker exited during connect"))??;
    Ok(jobs)
}

/// Connect to `url` and ensure the schema — run **only** on the worker thread (the sync client makes
/// its own runtime, so this must be off any ambient runtime). The DDL runs in a transaction under a
/// [`SCHEMA_LOCK_KEY`] advisory xact-lock: N replicas booting at once would otherwise race
/// `CREATE TABLE IF NOT EXISTS` into Postgres's known duplicate-key failure on `pg_type`/`pg_class`
/// and crash-loop until the restarts de-interleave.
fn connect_and_migrate(url: &str) -> Result<Client> {
    let mut client = Client::connect(url, NoTls).map_err(backend_err)?;
    let mut tx = client.transaction().map_err(backend_err)?;
    tx.execute("SELECT pg_advisory_xact_lock($1)", &[&SCHEMA_LOCK_KEY])
        .map_err(backend_err)?;
    tx.batch_execute(SCHEMA).map_err(backend_err)?;
    tx.commit().map_err(backend_err)?;
    Ok(client)
}

impl RegistryBackend for PostgresBackend {
    fn load(&self) -> Result<PersistedState> {
        // One statement = one MVCC snapshot: the three documents (and their versions, remembered as
        // the expected-version baseline for the next persists) are read consistently — a concurrent
        // leader write can't tear the registry envelope from its sidecars.
        let (mut docs, versions) = self.on_worker(|client| {
            let rows = client
                .query("SELECT key, version, doc FROM cp_state", &[])
                .map_err(backend_err)?;
            let mut docs: BTreeMap<String, Value> = BTreeMap::new();
            let mut versions: BTreeMap<String, i64> = BTreeMap::new();
            for row in rows {
                let key: String = row.try_get(0).map_err(backend_err)?;
                versions.insert(key.clone(), row.try_get(1).map_err(backend_err)?);
                docs.insert(key, row.try_get(2).map_err(backend_err)?);
            }
            Ok((docs, versions))
        })?;
        *self.versions.lock().unwrap_or_else(|e| e.into_inner()) = versions;
        let core = match docs.remove("registry") {
            Some(v) => serde_json::from_value::<RegistryFile>(v)?,
            None => RegistryFile::empty(),
        };
        let activity = docs
            .remove("activity")
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        let session_epochs = docs
            .remove("sessions")
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        Ok(core.into_persisted(activity, session_epochs))
    }

    fn persist_registry(&self, snapshot: RegistrySnapshot) -> Result<()> {
        let doc = serde_json::to_value(RegistryFile::from_snapshot(snapshot))?;
        self.persist_doc("registry", doc)
    }

    fn persist_activity(&self, activity: &BTreeMap<String, Vec<ActivityEvent>>) -> Result<()> {
        self.persist_doc("activity", serde_json::to_value(activity)?)
    }

    fn persist_sessions(&self, sessions: &BTreeMap<String, i64>) -> Result<()> {
        self.persist_doc("sessions", serde_json::to_value(sessions)?)
    }

    fn poll_version(&self) -> Result<Option<i64>> {
        Ok(Some(self.store_version()?))
    }

    fn try_become_leader(&self) -> Result<bool> {
        // Already the confirmed writer → idempotent success (the session still holds the lock).
        if self.is_writer.load(Ordering::SeqCst) {
            return Ok(true);
        }
        // A demoted replica's session may have died with its old tenure — the lock can only be won
        // on a live session, so replace a dead one before racing.
        if !self.session_alive() {
            self.revive_worker()?;
        }
        self.on_worker(|client| {
            client
                .query_one("SELECT pg_try_advisory_lock($1)", &[&WRITER_LOCK_KEY])
                .map_err(backend_err)?
                .try_get(0)
                .map_err(backend_err)
        })
        // Deliberately does NOT set `is_writer`: the caller reloads from the store first and then
        // calls `confirm_leadership`, so a write can never slip in between lock acquisition and the
        // reload and persist a stale pre-promotion snapshot.
    }

    fn confirm_leadership(&self) {
        self.is_writer.store(true, Ordering::SeqCst);
    }

    fn resign_leadership(&self) {
        self.is_writer.store(false, Ordering::SeqCst);
        // Best-effort unlock so the standby race is immediate; if the session already died the lock
        // is gone with it and this call harmlessly fails.
        let _ = self.on_worker(|client| {
            client
                .query_one("SELECT pg_advisory_unlock($1)", &[&WRITER_LOCK_KEY])
                .map_err(backend_err)?;
            Ok(())
        });
    }

    fn verify_leadership(&self) -> Result<bool> {
        if !self.is_writer.load(Ordering::SeqCst) {
            return Ok(false);
        }
        if self.session_alive() {
            return Ok(true);
        }
        // The lock-holding session is gone: Postgres already released the advisory lock and another
        // replica may be leader by now. Demote — never keep serving a frozen catalog as "ready".
        self.is_writer.store(false, Ordering::SeqCst);
        Ok(false)
    }

    fn is_leader(&self) -> bool {
        self.is_writer.load(Ordering::SeqCst)
    }
}

/// Wrap a store error as [`RegistryError::Backend`].
fn backend_err<E: std::fmt::Display>(e: E) -> RegistryError {
    RegistryError::Backend(e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ApiToken, IndexStatus, Registry};
    use growlerdb_core::{IndexDefinition, SourceField, SourceSchema, SourceType};

    /// The live Postgres to test against. Unset ⇒ the test **skips** (default `cargo test --features
    /// postgres` stays green without a database); CI's integration lane sets it. E.g.
    /// `postgresql://postgres:pw@127.0.0.1:55432/cp`.
    fn test_url() -> Option<String> {
        std::env::var("GROWLERDB_TEST_POSTGRES_URL").ok()
    }

    fn resolved(name: &str) -> growlerdb_core::ResolvedIndex {
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

    fn reset(url: &str) {
        let mut c = Client::connect(url, NoTls).unwrap();
        c.batch_execute(SCHEMA).unwrap();
        c.execute("DELETE FROM cp_state", &[]).unwrap();
    }

    /// Read the store-wide version (sum across rows — what `poll_version` reports) without acquiring
    /// the writer advisory lock — the shape a read-only standby uses to poll for a leader's write.
    fn registry_version_readonly(url: &str) -> i64 {
        let mut c = Client::connect(url, NoTls).unwrap();
        c.query_one(
            "SELECT COALESCE(SUM(version), 0)::BIGINT FROM cp_state",
            &[],
        )
        .unwrap()
        .get::<_, i64>(0)
    }

    /// The whole HA story end-to-end against a real Postgres — one advisory-lock timeline (the two
    /// halves can't be separate `#[test]`s: cargo runs tests concurrently, and they share one
    /// database, one `cp_state` table, and one global advisory lock key). Covers: leader
    /// round-trip + store-enforced single-writer, standby read + refuse-write + version-poll reload,
    /// and leader-death → standby promotion → durable write.
    #[test]
    fn postgres_backend_ha_lifecycle() {
        use std::time::Duration;
        let Some(url) = test_url() else {
            eprintln!("skipping: set GROWLERDB_TEST_POSTGRES_URL to run the Postgres backend test");
            return;
        };
        reset(&url);

        // ---- leader: full lifecycle + single-writer enforced by the store ----
        let leader =
            Registry::with_backend(Box::new(PostgresBackend::open(&url).unwrap())).unwrap();
        assert!(leader.is_leader());
        leader.create(resolved("docs")).unwrap();
        leader.activate("docs").unwrap();
        leader.assign_primary("docs", 0, "node-a").unwrap();
        leader.set_credential("alice", "pw").unwrap();
        leader
            .create_token(ApiToken {
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
        leader.record_activity("docs", "index.created", "created");
        // A second control plane cannot become writer while the leader holds the advisory lock.
        assert!(
            matches!(PostgresBackend::open(&url), Err(RegistryError::Backend(_))),
            "a second writer must be refused while the leader holds the advisory lock"
        );

        // ---- standby: serves reads, refuses writes, reloads on the leader's version bump ----
        let standby =
            Registry::with_backend(Box::new(PostgresBackend::open_standby(&url).unwrap())).unwrap();
        assert!(!standby.is_leader());
        assert_eq!(standby.get("docs").unwrap().status, IndexStatus::Active);
        assert_eq!(standby.find_token("H1").unwrap().id, "tok1"); // derived index rebuilt on load
        let v1 = standby.backend_version().unwrap().unwrap();
        assert!(v1 > 0);

        // A non-leader write is refused as NotLeader (FAILED_PRECONDITION at the gRPC seam), nothing
        // reaches the store, AND the refused mutation is rolled back out of standby memory.
        assert!(
            matches!(
                standby.create(resolved("phantom")),
                Err(RegistryError::NotLeader(_))
            ),
            "a non-leader must refuse to persist"
        );
        assert_eq!(
            registry_version_readonly(&url),
            v1,
            "the refused standby write never touched the store"
        );
        assert!(
            standby.get("phantom").is_none(),
            "the refused write was rolled back from standby memory immediately"
        );

        // The leader writes more; the standby's version poll advances and reload() picks it up.
        leader.create(resolved("logs")).unwrap();
        let v2 = standby.backend_version().unwrap().unwrap();
        assert!(v2 > v1, "the leader's write bumped the store version");
        standby.reload().unwrap();
        assert!(
            standby.get("logs").is_some(),
            "standby sees the leader's write after reload"
        );
        assert!(
            standby.get("phantom").is_none(),
            "reload resynced away the phantom in-memory entry from the refused write"
        );

        // ---- failover: leader dies → Postgres releases the lock → standby promotes and writes ----
        // A last write the standby has NOT polled yet: promotion must reload it before confirming
        // writership (HA-C2), or the promoted leader's first snapshot would erase it.
        leader.create(resolved("late-write")).unwrap();
        drop(leader);
        let mut promoted = false;
        for _ in 0..60 {
            if standby.try_become_leader().unwrap() {
                promoted = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50)); // Postgres notices the closed connection
        }
        assert!(
            promoted,
            "the standby acquires leadership after the leader dies"
        );
        assert!(standby.is_leader());
        assert!(
            standby.get("late-write").is_some(),
            "promotion reloads the dead leader's last writes before confirming writership"
        );
        standby.create(resolved("after-failover")).unwrap();

        // Durable: a fresh replica loads the whole persisted state, including the promoted write.
        let checker =
            Registry::with_backend(Box::new(PostgresBackend::open_standby(&url).unwrap())).unwrap();
        assert!(checker.get("after-failover").is_some());
        assert!(checker.get("logs").is_some());
        assert!(checker.get("late-write").is_some());
        assert!(checker.verify_credential("alice", "pw"));
        assert_eq!(checker.list_activity("docs", 0).len(), 1);
        drop(checker);

        // ---- version conflict: another writer bumps a row → refuse + demote (store-level CAS) ----
        let mut meddler = Client::connect(&url, NoTls).unwrap();
        meddler
            .execute(
                "UPDATE cp_state SET version = version + 1 WHERE key = 'registry'",
                &[],
            )
            .unwrap();
        assert!(
            matches!(
                standby.create(resolved("conflicted")),
                Err(RegistryError::NotLeader(_))
            ),
            "an expected-version mismatch means another writer — refused as NotLeader"
        );
        assert!(
            !standby.is_leader(),
            "a version conflict fail-stops the writer"
        );
        assert!(
            standby.get("conflicted").is_none(),
            "the refused write was rolled back from memory"
        );
        // Recovery: re-promote (reload refreshes the version baseline) and write again.
        assert!(standby.try_become_leader().unwrap());
        standby.create(resolved("recovered")).unwrap();

        // ---- demotion: the lock-holding session dies → leadership is detected as lost ----
        let killed = meddler
            .query(
                "SELECT pg_terminate_backend(pid) FROM pg_locks \
                 WHERE locktype = 'advisory' AND ((classid::BIGINT << 32) | objid::BIGINT) = $1",
                &[&WRITER_LOCK_KEY],
            )
            .unwrap();
        assert_eq!(killed.len(), 1, "found and killed the lock-holding session");
        assert!(
            !standby.verify_leadership().unwrap(),
            "a dead store session is a lost leadership"
        );
        assert!(!standby.is_leader());
        assert!(
            matches!(
                standby.create(resolved("zombie")),
                Err(RegistryError::NotLeader(_))
            ),
            "a deposed leader refuses writes instead of serving a frozen catalog"
        );
        // Recovery: try_become_leader revives the dead session with a fresh connection, reloads,
        // and re-acquires the released lock.
        let mut revived = false;
        for _ in 0..60 {
            if standby.try_become_leader().unwrap() {
                revived = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
        assert!(
            revived,
            "a demoted replica rejoins the race on a fresh session"
        );
        standby.create(resolved("after-revive")).unwrap();
        assert!(
            standby.get("zombie").is_none(),
            "the refused write never resurfaced after recovery"
        );

        // Everything that should be durable is; nothing refused ever became durable.
        let final_check =
            Registry::with_backend(Box::new(PostgresBackend::open_standby(&url).unwrap())).unwrap();
        assert!(final_check.get("recovered").is_some());
        assert!(final_check.get("after-revive").is_some());
        assert!(final_check.get("conflicted").is_none());
        assert!(final_check.get("zombie").is_none());
    }
}
