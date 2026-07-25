//! The **externalized Postgres registry backend** (D51) — behind the `postgres` feature.
//!
//! Stores the same versioned registry envelope the [`JsonFileBackend`](crate::JsonFileBackend) writes
//! to disk, but as JSONB rows in a shared Postgres, so the control plane can run as **N stateless
//! replicas** over one durable store. Single-writer is the store's job: [`open`](PostgresBackend::open)
//! takes a **session-level advisory lock** — the direct successor to the JSON backend's `flock` — so a
//! second control plane over the same database cannot also become writer. On the writer's death its
//! session ends and Postgres releases the lock, so a standby can take over (the standby run-loop is a
//! follow-on slice; this backend is the writer path + the version hook a standby polls).
//!
//! The write-coordination model is **leader-writer + reloading standbys**: exactly one replica holds
//! the advisory lock and writes; because writes are single-writer, the placement compare-and-swap
//! ([`set_bucket_map`](crate::Registry::set_bucket_map)) stays race-free across replicas without any
//! extra store-side CAS — two concurrent placement ops can only ever run on the one leader, where the
//! existing in-memory expected-map check already serializes them.

use std::collections::BTreeMap;
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
/// lifetime — which is also what holds the single-writer advisory lock.
pub struct PostgresBackend {
    client: Mutex<Client>,
}

impl PostgresBackend {
    /// Connect to `url`, ensure the schema, and **acquire the single-writer advisory lock** — the
    /// Postgres equivalent of the JSON backend taking the `flock`. Errors with
    /// [`Backend`](RegistryError::Backend) if the lock is already held by another control plane
    /// (single-writer enforced by the store) or on any connect/DDL failure.
    pub fn open(url: &str) -> Result<Self> {
        let mut client = Client::connect(url, NoTls).map_err(backend_err)?;
        client.batch_execute(SCHEMA).map_err(backend_err)?;
        let acquired: bool = client
            .query_one("SELECT pg_try_advisory_lock($1)", &[&WRITER_LOCK_KEY])
            .map_err(backend_err)?
            .get(0);
        if !acquired {
            return Err(RegistryError::Backend(
                "registry postgres single-writer lock is held by another control plane".into(),
            ));
        }
        Ok(Self {
            client: Mutex::new(client),
        })
    }

    /// The registry envelope's monotonic `version` in the store (`0` when nothing is persisted yet).
    /// A standby polls this to detect that the leader wrote and it should reload; exposed for the
    /// standby run-loop (a follow-on slice).
    pub fn registry_version(&self) -> Result<i64> {
        let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
        let row = client
            .query_opt("SELECT version FROM cp_state WHERE key = 'registry'", &[])
            .map_err(backend_err)?;
        Ok(row.map(|r| r.get::<_, i64>(0)).unwrap_or(0))
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
        let doc = serde_json::to_value(RegistryFile::from_snapshot(snapshot))?;
        let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
        upsert(&mut client, "registry", &doc)
    }

    fn persist_activity(&self, activity: &BTreeMap<String, Vec<ActivityEvent>>) -> Result<()> {
        let doc = serde_json::to_value(activity)?;
        let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
        upsert(&mut client, "activity", &doc)
    }

    fn persist_sessions(&self, sessions: &BTreeMap<String, i64>) -> Result<()> {
        let doc = serde_json::to_value(sessions)?;
        let mut client = self.client.lock().unwrap_or_else(|e| e.into_inner());
        upsert(&mut client, "sessions", &doc)
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

    #[test]
    fn postgres_backend_round_trips_and_is_single_writer() {
        let Some(url) = test_url() else {
            eprintln!("skipping: set GROWLERDB_TEST_POSTGRES_URL to run the Postgres backend test");
            return;
        };
        reset(&url);

        // A control plane over Postgres drives the full registry lifecycle...
        {
            let reg =
                Registry::with_backend(Box::new(PostgresBackend::open(&url).unwrap())).unwrap();
            reg.create(resolved("docs")).unwrap();
            reg.activate("docs").unwrap();
            reg.assign_primary("docs", 0, "node-a").unwrap();
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

            // ...and while it holds the advisory lock, a SECOND control plane over the same database
            // cannot become writer — single-writer enforced by the store, not a local flock.
            assert!(
                matches!(PostgresBackend::open(&url), Err(RegistryError::Backend(_))),
                "a second writer must be refused while the first holds the advisory lock"
            );
        } // drop → connection closes → Postgres releases the advisory lock

        // A fresh control plane (a failed-over replica) loads the persisted state from Postgres.
        let reg2 = Registry::with_backend(Box::new(PostgresBackend::open(&url).unwrap())).unwrap();
        assert_eq!(reg2.get("docs").unwrap().status, IndexStatus::Active);
        assert_eq!(
            reg2.shard_map("docs").unwrap()[&0]
                .primary
                .as_ref()
                .unwrap()
                .0,
            "node-a"
        );
        assert!(reg2.verify_credential("alice", "pw"));
        assert_eq!(reg2.find_token("H1").unwrap().id, "tok1"); // derived index rebuilt on load
        assert_eq!(reg2.list_activity("docs", 0).len(), 1);

        // The monotonic version advanced across the writes (a standby's reload signal). Read it with
        // a plain connection — opening another PostgresBackend would try to take the writer lock that
        // reg2 still holds.
        assert!(registry_version_readonly(&url) > 0);
    }

    /// Read the registry envelope's version without acquiring the writer advisory lock — the shape a
    /// read-only standby uses to poll for a leader's write.
    fn registry_version_readonly(url: &str) -> i64 {
        let mut c = Client::connect(url, NoTls).unwrap();
        c.query_one("SELECT version FROM cp_state WHERE key = 'registry'", &[])
            .unwrap()
            .get::<_, i64>(0)
    }
}
