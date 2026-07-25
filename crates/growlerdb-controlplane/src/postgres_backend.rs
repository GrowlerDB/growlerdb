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
//! the advisory lock and writes; because writes are single-writer, the placement compare-and-swap
//! ([`set_bucket_map`](crate::Registry::set_bucket_map)) stays race-free across replicas without any
//! extra store-side CAS — two concurrent placement ops can only ever run on the one leader, where the
//! existing in-memory expected-map check already serializes them.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use postgres::{Client, NoTls};
use serde_json::Value;

use crate::backend::{PersistedState, RegistryBackend, RegistryFile, RegistrySnapshot};
use crate::registry::{ActivityEvent, RegistryError, Result};

/// Fixed key for the session-level advisory lock that enforces single-writer across replicas — the
/// Postgres successor to the JSON backend's `flock`. Any constant `bigint`; scoped to the database.
const WRITER_LOCK_KEY: i64 = 0x4744_425F_4350_4C4B; // "GDB_CPLK"

/// Idempotent schema: one row per logical document (`registry` / `activity` / `sessions`), each a
/// JSONB envelope plus a monotonic `version` a standby polls to detect a leader's write.
const SCHEMA: &str = "\
CREATE TABLE IF NOT EXISTS cp_state ( \
  key TEXT PRIMARY KEY, \
  version BIGINT NOT NULL DEFAULT 0, \
  doc JSONB NOT NULL \
);";

/// The externalized Postgres [`RegistryBackend`]. Holds one blocking client for the backend's
/// lifetime; the same session is what holds the single-writer advisory lock once this replica is
/// leader. `is_writer` tracks leadership so a standby refuses every persist until it is promoted.
pub struct PostgresBackend {
    client: Mutex<Client>,
    is_writer: AtomicBool,
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
        Ok(backend)
    }

    /// Connect to `url` as a **standby**: ensure the schema but do **not** take the writer lock. The
    /// backend can [`load`](RegistryBackend::load) and [`poll_version`](RegistryBackend::poll_version)
    /// (serve reads + watch for the leader's writes) but refuses every persist until promoted via
    /// [`try_become_leader`](RegistryBackend::try_become_leader).
    pub fn open_standby(url: &str) -> Result<Self> {
        let mut client = Client::connect(url, NoTls).map_err(backend_err)?;
        client.batch_execute(SCHEMA).map_err(backend_err)?;
        Ok(Self {
            client: Mutex::new(client),
            is_writer: AtomicBool::new(false),
        })
    }

    /// The registry envelope's monotonic `version` in the store (`0` when nothing is persisted yet).
    fn registry_version(&self) -> Result<i64> {
        let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
        let row = client
            .query_opt("SELECT version FROM cp_state WHERE key = 'registry'", &[])
            .map_err(backend_err)?;
        Ok(row.map(|r| r.get::<_, i64>(0)).unwrap_or(0))
    }

    /// Refuse a write from a non-leader, so a standby can never corrupt the shared store even if a
    /// mis-routed write RPC reaches its registry.
    fn ensure_leader(&self) -> Result<()> {
        if self.is_writer.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(RegistryError::Backend(
                "control plane is not the registry leader; refusing to write".into(),
            ))
        }
    }
}

impl RegistryBackend for PostgresBackend {
    fn load(&self) -> Result<PersistedState> {
        let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
        let core = match read_doc(&mut client, "registry")? {
            Some(v) => serde_json::from_value::<RegistryFile>(v)?,
            None => RegistryFile::empty(),
        };
        let activity = read_doc(&mut client, "activity")?
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        let session_epochs = read_doc(&mut client, "sessions")?
            .map(serde_json::from_value)
            .transpose()?
            .unwrap_or_default();
        Ok(core.into_persisted(activity, session_epochs))
    }

    fn persist_registry(&self, snapshot: RegistrySnapshot) -> Result<()> {
        self.ensure_leader()?;
        let doc = serde_json::to_value(RegistryFile::from_snapshot(snapshot))?;
        let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
        upsert(&mut client, "registry", &doc)
    }

    fn persist_activity(&self, activity: &BTreeMap<String, Vec<ActivityEvent>>) -> Result<()> {
        self.ensure_leader()?;
        let doc = serde_json::to_value(activity)?;
        let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
        upsert(&mut client, "activity", &doc)
    }

    fn persist_sessions(&self, sessions: &BTreeMap<String, i64>) -> Result<()> {
        self.ensure_leader()?;
        let doc = serde_json::to_value(sessions)?;
        let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
        upsert(&mut client, "sessions", &doc)
    }

    fn poll_version(&self) -> Result<Option<i64>> {
        Ok(Some(self.registry_version()?))
    }

    fn try_become_leader(&self) -> Result<bool> {
        // Already leader → idempotent success (the session still holds the advisory lock).
        if self.is_writer.load(Ordering::SeqCst) {
            return Ok(true);
        }
        let acquired: bool = {
            let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
            client
                .query_one("SELECT pg_try_advisory_lock($1)", &[&WRITER_LOCK_KEY])
                .map_err(backend_err)?
                .get(0)
        };
        if acquired {
            self.is_writer.store(true, Ordering::SeqCst);
        }
        Ok(acquired)
    }

    fn is_leader(&self) -> bool {
        self.is_writer.load(Ordering::SeqCst)
    }
}

/// Wrap a store error as [`RegistryError::Backend`].
fn backend_err<E: std::fmt::Display>(e: E) -> RegistryError {
    RegistryError::Backend(e.to_string())
}

/// Read a document row's JSONB, if present.
fn read_doc(client: &mut Client, key: &str) -> Result<Option<Value>> {
    let row = client
        .query_opt("SELECT doc FROM cp_state WHERE key = $1", &[&key])
        .map_err(backend_err)?;
    Ok(row.map(|r| r.get::<_, Value>(0)))
}

/// Upsert a document, bumping its monotonic `version` on every write.
fn upsert(client: &mut Client, key: &str, doc: &Value) -> Result<()> {
    client
        .execute(
            "INSERT INTO cp_state (key, version, doc) VALUES ($1, 1, $2) \
             ON CONFLICT (key) DO UPDATE SET version = cp_state.version + 1, doc = EXCLUDED.doc",
            &[&key, &doc],
        )
        .map_err(backend_err)?;
    Ok(())
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

    /// Read the registry envelope's version without acquiring the writer advisory lock — the shape a
    /// read-only standby uses to poll for a leader's write.
    fn registry_version_readonly(url: &str) -> i64 {
        let mut c = Client::connect(url, NoTls).unwrap();
        c.query_one("SELECT version FROM cp_state WHERE key = 'registry'", &[])
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

        // A non-leader write is refused AND nothing reaches the store.
        assert!(
            matches!(
                standby.create(resolved("phantom")),
                Err(RegistryError::Backend(_))
            ),
            "a non-leader must refuse to persist"
        );
        assert_eq!(
            registry_version_readonly(&url),
            v1,
            "the refused standby write never touched the store"
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
        standby.create(resolved("after-failover")).unwrap();

        // Durable: a fresh replica loads the whole persisted state, including the promoted write.
        let checker =
            Registry::with_backend(Box::new(PostgresBackend::open_standby(&url).unwrap())).unwrap();
        assert!(checker.get("after-failover").is_some());
        assert!(checker.get("logs").is_some());
        assert!(checker.verify_credential("alice", "pw"));
        assert_eq!(checker.list_activity("docs", 0).len(), 1);
    }
}
