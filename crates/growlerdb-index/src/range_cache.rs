//! A **bounded byte-range cache** for read-through cold-window serving. Wraps the ranged
//! object-store reads an [`ObjectDirectory`](crate::ObjectDirectory) makes so repeated reads — the
//! term dictionary, the same postings, structural metadata — stay local instead of re-fetching from
//! object storage. The "warm" behaviour is emergent: query a cold window once and the bytes it
//! touched are cached; query it again and it's fast.
//!
//! Keyed by `(object, [start, end))` (the exact ranges tantivy re-reads), evicted **least-recently
//! used** once a total-bytes cap is exceeded. One cache is shared across every cold window on a
//! node. Hit/miss/byte counters feed the cold-tier metrics.

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tantivy::directory::OwnedBytes;

/// Cache key: an object's key plus the half-open byte range read from it.
type Key = (Arc<str>, u64, u64);

/// A byte-bounded LRU over cached ranges. Recency is a monotonic sequence; eviction pops the
/// smallest (oldest) sequence until under the cap.
struct ByteLru {
    map: HashMap<Key, (OwnedBytes, u64)>,
    order: BTreeMap<u64, Key>,
    seq: u64,
    bytes: usize,
    cap: usize,
}

impl ByteLru {
    fn get(&mut self, k: &Key) -> Option<OwnedBytes> {
        let (bytes, old_seq) = self.map.get(k).map(|(b, s)| (b.clone(), *s))?;
        self.seq += 1;
        let new_seq = self.seq;
        self.order.remove(&old_seq);
        self.order.insert(new_seq, k.clone());
        self.map.get_mut(k).expect("present").1 = new_seq;
        Some(bytes)
    }

    fn put(&mut self, k: Key, v: OwnedBytes) {
        if v.len() > self.cap {
            return; // a single range bigger than the whole cache is never worth holding
        }
        if let Some((old, old_seq)) = self.map.remove(&k) {
            self.bytes -= old.len();
            self.order.remove(&old_seq);
        }
        self.seq += 1;
        let seq = self.seq;
        self.bytes += v.len();
        self.order.insert(seq, k.clone());
        self.map.insert(k, (v, seq));
        while self.bytes > self.cap {
            let Some((&lru_seq, _)) = self.order.iter().next() else {
                break;
            };
            if let Some(key) = self.order.remove(&lru_seq) {
                if let Some((ev, _)) = self.map.remove(&key) {
                    self.bytes -= ev.len();
                }
            }
        }
    }
}

/// A snapshot of cache activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize)]
pub struct CacheStats {
    /// Range reads served from the cache.
    pub hits: u64,
    /// Range reads that missed and were fetched from object storage.
    pub misses: u64,
    /// Total bytes fetched from object storage (the cold-read cost).
    pub fetched_bytes: u64,
    /// Bytes currently held in the cache.
    pub cached_bytes: u64,
    /// Ranges dropped on insert because a single range exceeded the whole cache cap:
    /// such a range is re-fetched cold on *every* read, so a non-zero count signals an undersized
    /// cache relative to the segment size, not a normal miss.
    pub oversize_drops: u64,
}

/// A shared, byte-bounded LRU range cache (clone to share one cache across cold windows).
#[derive(Clone)]
pub struct RangeCache(Arc<Inner>);

struct Inner {
    lru: Mutex<ByteLru>,
    hits: AtomicU64,
    misses: AtomicU64,
    fetched_bytes: AtomicU64,
    oversize_drops: AtomicU64,
}

impl RangeCache {
    /// A cache holding up to `capacity_bytes` of cached ranges before LRU eviction.
    pub fn new(capacity_bytes: usize) -> Self {
        Self(Arc::new(Inner {
            lru: Mutex::new(ByteLru {
                map: HashMap::new(),
                order: BTreeMap::new(),
                seq: 0,
                bytes: 0,
                cap: capacity_bytes,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            fetched_bytes: AtomicU64::new(0),
            oversize_drops: AtomicU64::new(0),
        }))
    }

    /// Look up a cached range, recording a hit or miss.
    pub fn get(&self, object: &Arc<str>, range: &Range<usize>) -> Option<OwnedBytes> {
        let key = (object.clone(), range.start as u64, range.end as u64);
        let hit = self
            .0
            .lru
            .lock()
            .expect("range cache not poisoned")
            .get(&key);
        // Prometheus counters for the cold-tier cache-hit rate. Emitted at the source
        // (this node-wide LRU has no index in scope — a `tier` label suffices); the panel computes
        // hit% = rate(hits) / (rate(hits) + rate(misses)). No-op unless a metrics recorder is
        // installed, so the `growlerdb-index` crate needn't pull the telemetry facade — just the
        // lightweight `metrics` facade.
        if hit.is_some() {
            self.0.hits.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("growlerdb_cold_cache_hits_total", "tier" => "cold").increment(1);
        } else {
            self.0.misses.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("growlerdb_cold_cache_misses_total", "tier" => "cold").increment(1);
        }
        hit
    }

    /// Insert a freshly-fetched range, accounting the bytes fetched from object storage.
    pub fn put(&self, object: &Arc<str>, range: &Range<usize>, bytes: OwnedBytes) {
        self.0
            .fetched_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let key = (object.clone(), range.start as u64, range.end as u64);
        let mut lru = self.0.lru.lock().expect("range cache not poisoned");
        if bytes.len() > lru.cap {
            // A single range bigger than the whole cache never fits → dropped, and re-fetched cold on
            // every read. Count it so this is distinguishable from a normal miss.
            drop(lru);
            self.0.oversize_drops.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("growlerdb_cold_cache_oversize_drops_total").increment(1);
            return;
        }
        lru.put(key, bytes);
    }

    /// Snapshot every cached `(object-key, start, end, bytes)` currently held — used to build a
    /// precomputed **hotcache**: warm a cold window through a fresh cache, then capture the
    /// exact ranges tantivy touched so a later cold open can preload them in one GET. Recency/counters
    /// are untouched (this is an out-of-band read for packaging, not a query).
    pub fn snapshot_entries(&self) -> Vec<(Arc<str>, u64, u64, OwnedBytes)> {
        let lru = self.0.lru.lock().expect("range cache not poisoned");
        lru.map
            .iter()
            .map(|((obj, start, end), (bytes, _))| (obj.clone(), *start, *end, bytes.clone()))
            .collect()
    }

    /// A snapshot of hit/miss/byte counters.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.0.hits.load(Ordering::Relaxed),
            misses: self.0.misses.load(Ordering::Relaxed),
            fetched_bytes: self.0.fetched_bytes.load(Ordering::Relaxed),
            cached_bytes: self.0.lru.lock().expect("range cache not poisoned").bytes as u64,
            oversize_drops: self.0.oversize_drops.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(n: usize) -> OwnedBytes {
        OwnedBytes::new(vec![0u8; n])
    }

    #[test]
    fn miss_then_hit_on_repeat_read() {
        let cache = RangeCache::new(1024);
        let obj: Arc<str> = Arc::from("cold/w1/file");
        let r = 0..100;
        assert!(cache.get(&obj, &r).is_none(), "first read misses");
        cache.put(&obj, &r, bytes(100));
        assert!(cache.get(&obj, &r).is_some(), "second read hits");

        let s = cache.stats();
        assert_eq!((s.hits, s.misses), (1, 1));
        assert_eq!(s.fetched_bytes, 100);
        assert_eq!(s.cached_bytes, 100);
    }

    #[test]
    fn oversize_range_is_counted_not_cached() {
        // A range bigger than the whole cache is dropped (re-fetched cold every read),
        // and counted distinctly from a normal miss.
        let cache = RangeCache::new(100);
        let obj: Arc<str> = Arc::from("file");
        cache.put(&obj, &(0..200), bytes(200)); // 200 > 100 cap → dropped
        assert!(
            cache.get(&obj, &(0..200)).is_none(),
            "oversize range not cached"
        );
        assert_eq!(cache.stats().oversize_drops, 1);
        // A fitting range still caches normally.
        cache.put(&obj, &(0..50), bytes(50));
        assert!(cache.get(&obj, &(0..50)).is_some());
        assert_eq!(
            cache.stats().oversize_drops,
            1,
            "only the oversize insert counted"
        );
    }

    #[test]
    fn evicts_least_recently_used_over_cap() {
        let cache = RangeCache::new(250); // holds ~2 of these 100-byte ranges
        let obj: Arc<str> = Arc::from("file");
        let (a, b, c) = (0..100, 100..200, 200..300);
        cache.put(&obj, &a, bytes(100));
        cache.put(&obj, &b, bytes(100));
        // Touch `a` so `b` is now the least-recently-used.
        assert!(cache.get(&obj, &a).is_some());
        cache.put(&obj, &c, bytes(100)); // 300 > 250 → evict the LRU (`b`)
        assert!(cache.get(&obj, &a).is_some(), "a retained (recently used)");
        assert!(cache.get(&obj, &c).is_some(), "c retained (just inserted)");
        assert!(cache.get(&obj, &b).is_none(), "b evicted as LRU");
        assert!(cache.stats().cached_bytes <= 250);
    }

    #[test]
    fn distinct_ranges_of_same_object_are_distinct_entries() {
        let cache = RangeCache::new(1024);
        let obj: Arc<str> = Arc::from("file");
        cache.put(&obj, &(0..50), bytes(50));
        assert!(
            cache.get(&obj, &(50..100)).is_none(),
            "a different range misses"
        );
        assert!(cache.get(&obj, &(0..50)).is_some());
    }
}
