//! Snapshot-pinned **plan cache** for hydration (task-184 / D30 foundations).
//!
//! Planning a hydration pass-1 read is `load_table` (one catalog REST call) plus
//! `scan().plan_files()` (manifest-list + manifest GETs from object storage). The catalog
//! call is cheap and unavoidable — it is how we learn the *current snapshot id* — but the
//! manifest reads only change when the snapshot does. At the target lookup rates
//! (~10–50 batches/s) re-planning per batch would dominate hydration p99, so the planned
//! task set is cached **pinned to the snapshot it was planned at**: same `(table,
//! snapshot)` → reuse; snapshot advanced → replan and replace.
//!
//! The cache is generic over the plan type `T` (production uses
//! `Arc<Vec<FileScanTask>>`) so the keyed-by-snapshot behavior is unit-testable without a
//! catalog. It is a small LRU keyed by table ident — the engine serves few tables, so the
//! cap ([`IcebergReader`](crate::IcebergReader) uses [`PLAN_CACHE_CAP`]) only guards an
//! operator pointing many indexes at one node. Locking is a short `std::sync::Mutex`
//! critical section around map access only — never held across the planning `await` — so
//! concurrent misses at the same snapshot may **duplicate a replan** (benign: last writer
//! wins, both plans are identical) rather than serializing all hydrations.

use std::collections::HashMap;
use std::sync::Mutex;

/// Table-count cap for the per-reader plan cache: an engine node serves a handful of
/// tables, so this only bounds the pathological many-indexes-one-node case.
pub const PLAN_CACHE_CAP: usize = 16;

/// A plan cached for one table, valid only at the snapshot it was planned at.
struct Entry<T> {
    snapshot_id: i64,
    plan: T,
    /// LRU tick — bumped on every hit/insert; the smallest is evicted at the cap.
    last_used: u64,
}

struct Inner<T> {
    tick: u64,
    entries: HashMap<String, Entry<T>>,
}

/// A snapshot-pinned plan cache: `table ident → (snapshot id, plan)`, LRU-bounded.
/// See the [module docs](self) for the invalidation and concurrency story.
pub struct PlanCache<T> {
    cap: usize,
    inner: Mutex<Inner<T>>,
}

impl<T: Clone> PlanCache<T> {
    /// An empty cache holding at most `cap` tables (one plan per table).
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            inner: Mutex::new(Inner {
                tick: 0,
                entries: HashMap::new(),
            }),
        }
    }

    /// The cached plan for `table`, only if it was planned at exactly `snapshot_id`
    /// (an advanced snapshot is a miss — the caller replans and [`put`](Self::put)s).
    pub fn get(&self, table: &str, snapshot_id: i64) -> Option<T> {
        let mut inner = self.inner.lock().expect("plan cache poisoned");
        inner.tick += 1;
        let tick = inner.tick;
        let entry = inner.entries.get_mut(table)?;
        if entry.snapshot_id != snapshot_id {
            return None;
        }
        entry.last_used = tick;
        Some(entry.plan.clone())
    }

    /// Cache `plan` for `table` at `snapshot_id`, replacing any previous snapshot's plan
    /// for the table and evicting the least-recently-used table beyond the cap.
    pub fn put(&self, table: &str, snapshot_id: i64, plan: T) {
        let mut inner = self.inner.lock().expect("plan cache poisoned");
        inner.tick += 1;
        let tick = inner.tick;
        inner.entries.insert(
            table.to_string(),
            Entry {
                snapshot_id,
                plan,
                last_used: tick,
            },
        );
        while inner.entries.len() > self.cap {
            let Some(evict) = inner
                .entries
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(t, _)| t.clone())
            else {
                break;
            };
            inner.entries.remove(&evict);
        }
    }

    /// The cached plan at `(table, snapshot_id)`, or run `plan` and cache its result.
    /// Returns `(plan, hit)` — `hit` feeds the `growlerdb_plan_cache_{hits,misses}_total`
    /// counters. A failed `plan` caches nothing (the next call retries). The lock is not
    /// held across the `plan` await (see the [module docs](self)).
    pub async fn get_or_plan<F, Fut, E>(
        &self,
        table: &str,
        snapshot_id: i64,
        plan: F,
    ) -> Result<(T, bool), E>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, E>>,
    {
        if let Some(cached) = self.get(table, snapshot_id) {
            return Ok((cached, true));
        }
        let planned = plan().await?;
        self.put(table, snapshot_id, planned.clone());
        Ok((planned, false))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// `get_or_plan` with a counting planner returning `plan` — the seam the tests use to
    /// prove how many planning passes actually ran.
    async fn plan_counted(
        cache: &PlanCache<u32>,
        table: &str,
        snapshot: i64,
        plan: u32,
        plans_run: &AtomicUsize,
    ) -> (u32, bool) {
        cache
            .get_or_plan(table, snapshot, || async {
                plans_run.fetch_add(1, Ordering::SeqCst);
                Ok::<_, String>(plan)
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn same_snapshot_plans_once_and_hits_after() {
        let cache = PlanCache::new(4);
        let runs = AtomicUsize::new(0);
        // Two hydrates at the same snapshot → exactly one planning pass.
        assert_eq!(
            plan_counted(&cache, "g.docs", 1, 10, &runs).await,
            (10, false)
        );
        assert_eq!(
            plan_counted(&cache, "g.docs", 1, 99, &runs).await,
            (10, true)
        );
        assert_eq!(runs.load(Ordering::SeqCst), 1, "manifest reads happen once");
    }

    #[tokio::test]
    async fn snapshot_advance_replans_and_replaces() {
        let cache = PlanCache::new(4);
        let runs = AtomicUsize::new(0);
        assert_eq!(
            plan_counted(&cache, "g.docs", 1, 10, &runs).await,
            (10, false)
        );
        // The snapshot advanced → the pinned plan is stale → a fresh planning pass.
        assert_eq!(
            plan_counted(&cache, "g.docs", 2, 20, &runs).await,
            (20, false)
        );
        assert_eq!(runs.load(Ordering::SeqCst), 2);
        // The old snapshot's plan was *replaced*, not kept alongside (one plan per table).
        assert_eq!(cache.get("g.docs", 1), None);
        assert_eq!(cache.get("g.docs", 2), Some(20));
    }

    #[tokio::test]
    async fn tables_are_cached_independently() {
        let cache = PlanCache::new(4);
        cache.put("g.a", 1, 10);
        cache.put("g.b", 7, 20);
        assert_eq!(cache.get("g.a", 1), Some(10));
        assert_eq!(cache.get("g.b", 7), Some(20));
        assert_eq!(cache.get("g.b", 1), None, "snapshot is part of the key");
    }

    #[tokio::test]
    async fn cap_evicts_the_least_recently_used_table() {
        let cache = PlanCache::new(2);
        cache.put("g.a", 1, 10);
        cache.put("g.b", 1, 20);
        assert_eq!(cache.get("g.a", 1), Some(10)); // touch `a` → `b` is now LRU
        cache.put("g.c", 1, 30);
        assert_eq!(cache.get("g.b", 1), None, "LRU table evicted at the cap");
        assert_eq!(cache.get("g.a", 1), Some(10));
        assert_eq!(cache.get("g.c", 1), Some(30));
    }

    #[tokio::test]
    async fn a_failed_plan_is_not_cached_so_the_next_call_retries() {
        let cache: PlanCache<u32> = PlanCache::new(4);
        let runs = AtomicUsize::new(0);
        let err = cache
            .get_or_plan("g.docs", 1, || async {
                runs.fetch_add(1, Ordering::SeqCst);
                Err::<u32, _>("catalog down".to_string())
            })
            .await
            .unwrap_err();
        assert_eq!(err, "catalog down");
        // Nothing poisoned: the retry plans again and its result is cached.
        assert_eq!(
            plan_counted(&cache, "g.docs", 1, 10, &runs).await,
            (10, false)
        );
        assert_eq!(
            plan_counted(&cache, "g.docs", 1, 10, &runs).await,
            (10, true)
        );
        assert_eq!(
            runs.load(Ordering::SeqCst),
            2,
            "failed run + successful retry"
        );
    }
}
