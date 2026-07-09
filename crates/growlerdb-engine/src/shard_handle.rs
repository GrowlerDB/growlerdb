//! [`ShardHandle`] — a live, **swappable** handle to a Node's shard ([task-30] B1, the
//! reindex foundation). Every Node service reads the current shard through
//! [`current`](ShardHandle::current); a reindex atomically replaces it with
//! [`swap`](ShardHandle::swap). A request that already loaded the `Arc<Shard>` keeps reading
//! it through completion, so a swap never tears an in-flight search — and open PITs on the
//! retired shard stay valid until their readers release it.
//!
//! Backed by an `RwLock<Arc<Shard>>`: reads only clone the `Arc` under a brief read lock,
//! and swaps are rare, so the lock is not a contention point (a lock-free `arc-swap` would
//! be an over-optimization here).
//!
//! [task-30]: ../../../design/06-service-architecture.md

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use growlerdb_index::Shard;

/// Shared inner state: the swappable shard plus a search counter.
struct Inner {
    shard: RwLock<Arc<Shard>>,
    /// Searches served (task-83 pre-warm signal, corrected in task-153 / I3). Bumped **only** by the
    /// search path via [`record_search`](ShardHandle::record_search) — NOT by every `current()`,
    /// which every service (suggest/lookup/admin/health) calls, so the promotion decision reflects
    /// real query load rather than incidental access.
    searches: AtomicU64,
}

/// A shared, swappable pointer to the live [`Shard`]. Cloning shares the same underlying
/// cell, so every service built from one handle observes the same swaps.
#[derive(Clone)]
pub struct ShardHandle(Arc<Inner>);

impl ShardHandle {
    /// Wrap a shard in a fresh handle.
    pub fn new(shard: Arc<Shard>) -> Self {
        Self(Arc::new(Inner {
            shard: RwLock::new(shard),
            searches: AtomicU64::new(0),
        }))
    }

    /// The shard currently live, as an owned `Arc` (clones under a brief read lock). Hold
    /// the returned `Arc` for the duration of a request so a concurrent [`swap`] can't pull
    /// the shard out from under it.
    ///
    /// [`swap`]: Self::swap
    pub fn current(&self) -> Arc<Shard> {
        self.0
            .shard
            .read()
            .expect("shard handle not poisoned")
            .clone()
    }

    /// Record one search against this shard — the pre-warm signal (task-153 / I3). Called by the
    /// search path only, so a cold window is promoted for real query load, not describe/health traffic.
    pub fn record_search(&self) {
        self.0.searches.fetch_add(1, Ordering::Relaxed);
    }

    /// Total searches served since this handle was created (task-83 pre-warm). Monotonic; the
    /// pre-warm loop watches its delta over time.
    pub fn search_count(&self) -> u64 {
        self.0.searches.load(Ordering::Relaxed)
    }

    /// Atomically replace the live shard (reindex), returning the previous one so the caller
    /// can drop it (and GC its files) once its last reader releases it.
    pub fn swap(&self, shard: Arc<Shard>) -> Arc<Shard> {
        std::mem::replace(
            &mut self.0.shard.write().expect("shard handle not poisoned"),
            shard,
        )
    }
}

impl From<Arc<Shard>> for ShardHandle {
    fn from(shard: Arc<Shard>) -> Self {
        Self::new(shard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SearchService;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
        SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
    };
    use growlerdb_index::{LocalIndexStore, ShardId};
    use growlerdb_proto::v1::SearchRequest;
    use growlerdb_proto::Search;
    use std::collections::BTreeMap;
    use tonic::Request;

    /// Build a shard under `root` holding `ids` (each a doc whose body is "doc").
    fn shard_with(root: &std::path::Path, ids: &[&str]) -> Arc<Shard> {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT } ] }\n",
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
                let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(*id))]);
                let mut f = BTreeMap::new();
                f.insert("id".to_string(), Value::from(*id));
                f.insert("body".to_string(), Value::from("doc"));
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
        Arc::new(shard)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn swap_is_visible_through_the_handle_and_its_services() {
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        let a = shard_with(tmp_a.path(), &["1", "2"]);
        let b = shard_with(tmp_b.path(), &["10", "20", "30"]);

        let handle = ShardHandle::new(a.clone());
        // A service built from a clone of the handle shares the same swappable cell.
        let search = SearchService::new(handle.clone());
        let count = |svc: &SearchService| {
            let svc = svc.clone();
            async move {
                svc.search(Request::new(SearchRequest {
                    query: "body:doc".into(),
                    limit: 100,
                    ..Default::default()
                }))
                .await
                .unwrap()
                .into_inner()
                .total
            }
        };

        assert_eq!(handle.current().num_docs().unwrap(), 2);
        assert_eq!(count(&search).await, 2);

        // Reindex: swap in shard B. The handle and the pre-built service both see it.
        let prev = handle.swap(b.clone());
        assert_eq!(prev.num_docs().unwrap(), 2); // the retired shard is returned for GC
        assert_eq!(handle.current().num_docs().unwrap(), 3);
        assert_eq!(count(&search).await, 3);
    }

    #[test]
    fn only_record_search_moves_the_prewarm_signal() {
        // task-153 / I3: the pre-warm signal counts SEARCHES, not every `current()` — so incidental
        // access (describe/health/suggest) doesn't promote a cold window.
        let tmp = tempfile::tempdir().unwrap();
        let handle = ShardHandle::new(shard_with(tmp.path(), &["1"]));
        assert_eq!(handle.search_count(), 0);
        for _ in 0..5 {
            let _ = handle.current(); // non-search access must NOT bump the signal
        }
        assert_eq!(handle.search_count(), 0);
        for _ in 0..3 {
            handle.record_search();
        }
        assert_eq!(handle.search_count(), 3);
    }
}
