//! **Precomputed hotcache** for cold windows (task-83, a refinement on task-80's read-through cold
//! tier). Opening a parked window read-through normally costs a burst of small object-store
//! round-trips *before the first hit is even scored*: the two atomic files (`meta.json`,
//! `.managed.json`), a `stat` per segment file for its length, and the structural byte ranges every
//! [`SegmentReader`](crate::SegmentReader) reads (term-dictionary index, fast-field codecs, store
//! footers). That's the "cold open" latency tax, paid on every node restart / cache-cold query.
//!
//! The hotcache pays it **once, at park time**: [`build`] warms the just-parked index through a
//! recording [`ObjectDirectory`] and captures exactly those structural reads into a single small
//! sidecar object. A later cold open ([`preload`]) fetches that one object and serves all of it
//! locally — atomic bodies + file lengths from an in-memory [`HotState`], byte ranges from the
//! shared [`RangeCache`] — so `Index::open` + reader setup issue **zero** object round-trips. Only
//! the postings a specific query actually touches are fetched cold (and then cached as usual).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tantivy::directory::OwnedBytes;
use tantivy::Index;

use crate::object_directory::{HotState, ObjectDirectory};
use crate::range_cache::RangeCache;
use crate::store::{Result, StoreError};

/// The serialized sidecar (postcard): the structural reads a cold open performs, keyed by path
/// relative to the index object prefix. Bytes are the actual object contents, so the whole thing is
/// self-contained — one GET reconstitutes it.
#[derive(Serialize, Deserialize, Default)]
struct HotCache {
    /// `(relative file, range start, bytes)` — the structural byte ranges tantivy reads on open.
    ranges: Vec<(String, u64, Vec<u8>)>,
    /// `(relative file, length)` — segment file lengths, so `get_file_handle` skips its `stat`.
    lens: Vec<(String, u64)>,
    /// `(relative file, bytes)` — full bodies of the tiny atomic files (`meta.json`, `.managed.json`).
    atomic: Vec<(String, Vec<u8>)>,
}

/// Build a hotcache for the cold index rooted at `object_prefix` in `op`: warm it through a
/// recording directory and capture the structural reads. Returns the serialized sidecar to store
/// next to the window (see [`preload`]). **Synchronous** — [`ObjectDirectory`] reads `block_on` the
/// current tokio runtime, so call this from a blocking context (e.g. `spawn_blocking`), same as
/// [`open_cold_shard`](crate::LocalIndexStore::open_cold_shard).
pub fn build(op: opendal::Operator, object_prefix: &str) -> Result<Vec<u8>> {
    // A private cache captures the ranges of this one warm-up (not the node's shared serving cache).
    let cache = RangeCache::new(256 * 1024 * 1024);
    let dir = ObjectDirectory::open(op, object_prefix)
        .map_err(|e| StoreError::Cold(e.to_string()))?
        .with_cache(cache.clone())
        .recording();
    let index = Index::open(dir.clone()).map_err(|e| StoreError::Segment(e.into()))?;
    // Opening the live reader's searcher forces every segment reader open → the structural reads.
    let reader = index.reader().map_err(|e| StoreError::Segment(e.into()))?;
    let _ = reader.searcher();

    let recorded = dir.take_recorded();
    let base = format!("{}/", object_prefix.trim_end_matches('/'));
    let ranges = cache
        .snapshot_entries()
        .into_iter()
        .filter_map(|(obj, start, _end, bytes)| {
            obj.strip_prefix(base.as_str())
                .map(|rel| (rel.to_string(), start, bytes.to_vec()))
        })
        .collect();
    let hc = HotCache {
        ranges,
        lens: recorded.lens.into_iter().collect(),
        atomic: recorded.atomic.into_iter().collect(),
    };
    // Frame with a magic + version so a later format change degrades instead of mis-parsing
    // (task-150 / F5).
    Ok(crate::sidecar::frame(
        crate::sidecar::HOTCACHE_MAGIC,
        postcard::to_stdvec(&hc)?,
    ))
}

/// Preload a hotcache `bytes` (from [`build`]) into a [`HotState`] to hand to
/// [`ObjectDirectory::with_hot`](crate::ObjectDirectory). Atomic bodies, file lengths, **and** the
/// structural byte ranges are all pinned in the returned state (task-150 / B7) — the ranges no longer
/// go into the shared evictable cache — so opening the window needs no object round-trips and they
/// can't be evicted out from under it. Errors on an unrecognized/incompatible sidecar (task-150 / F5)
/// so the caller can fall back to plain read-through.
pub(crate) fn preload(bytes: &[u8]) -> Result<HotState> {
    let payload = crate::sidecar::unframe(crate::sidecar::HOTCACHE_MAGIC, bytes)?;
    let hc: HotCache = postcard::from_bytes(payload)?;
    let mut ranges: HashMap<String, Vec<(u64, OwnedBytes)>> = HashMap::new();
    for (rel, start, b) in hc.ranges {
        ranges
            .entry(rel)
            .or_default()
            .push((start, OwnedBytes::new(b)));
    }
    Ok(HotState {
        lens: hc.lens.into_iter().collect(),
        atomic: hc.atomic.into_iter().collect(),
        ranges,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tantivy::collector::TopDocs;
    use tantivy::query::QueryParser;
    use tantivy::schema::{Schema, TEXT};
    use tantivy::{doc, Index};

    /// Copy a freshly-built local tantivy index into an fs-backed opendal store under `prefix`.
    fn stage_index(store_root: &std::path::Path, prefix: &str) -> opendal::Operator {
        let mut sb = Schema::builder();
        let body = sb.add_text_field("body", TEXT);
        let local = tempfile::tempdir().unwrap();
        let index = Index::create_in_dir(local.path(), sb.build()).unwrap();
        let mut writer = index.writer(50_000_000).unwrap();
        for w in ["alpha beta", "beta gamma", "delta", "alpha delta"] {
            writer.add_document(doc!(body => w)).unwrap();
        }
        writer.commit().unwrap();

        let prefix_dir = store_root.join(prefix);
        std::fs::create_dir_all(&prefix_dir).unwrap();
        for entry in std::fs::read_dir(local.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                std::fs::copy(entry.path(), prefix_dir.join(entry.file_name())).unwrap();
            }
        }
        opendal::Operator::new(opendal::services::Fs::default().root(&store_root.to_string_lossy()))
            .unwrap()
            .finish()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hotcache_open_needs_zero_object_round_trips() {
        let store_root = tempfile::tempdir().unwrap();
        let op = stage_index(store_root.path(), "cold/w1");

        // Build the hotcache from the parked index.
        let op_b = op.clone();
        let hot_bytes = tokio::task::spawn_blocking(move || build(op_b, "cold/w1").unwrap())
            .await
            .unwrap();
        assert!(!hot_bytes.is_empty(), "hotcache captured structural bytes");
        // Persist it into the store like cold_park would (a sidecar object).
        op.write("cold/hotcache.bin", hot_bytes.clone())
            .await
            .unwrap();

        // Now DELETE the index objects, leaving only the hotcache sidecar — proving a preloaded open
        // touches object storage zero times for structural reads (there's nothing left to touch).
        std::fs::remove_dir_all(store_root.path().join("cold/w1")).unwrap();

        let cache = RangeCache::new(64 * 1024 * 1024);
        let opened = tokio::task::spawn_blocking(move || {
            let hot = preload(&hot_bytes).unwrap();
            let dir = ObjectDirectory::open(op, "cold/w1")
                .unwrap()
                .with_cache(cache.clone())
                .with_hot(Arc::new(hot));
            // Index::open + reader + searcher = the full structural open; must succeed with the
            // backing objects gone → every structural byte came from the hotcache.
            let index = Index::open(dir).unwrap();
            let searcher = index.reader().unwrap().searcher();
            searcher.num_docs()
        })
        .await
        .unwrap();
        assert_eq!(
            opened, 4,
            "cold window opens + counts purely from the hotcache"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn hotcache_preload_still_answers_queries() {
        // With the objects present (normal case), a hotcache-preloaded open still serves real
        // queries: structural reads are free, and postings are fetched cold + cached as usual.
        let store_root = tempfile::tempdir().unwrap();
        let op = stage_index(store_root.path(), "cold/w1");
        let op_b = op.clone();
        let hot_bytes = tokio::task::spawn_blocking(move || build(op_b, "cold/w1").unwrap())
            .await
            .unwrap();

        let cache = RangeCache::new(64 * 1024 * 1024);
        let hits = tokio::task::spawn_blocking(move || {
            let hot = preload(&hot_bytes).unwrap();
            let dir = ObjectDirectory::open(op, "cold/w1")
                .unwrap()
                .with_cache(cache.clone())
                .with_hot(Arc::new(hot));
            let index = Index::open(dir).unwrap();
            let body = index.schema().get_field("body").unwrap();
            let qp = QueryParser::for_index(&index, vec![body]);
            index
                .reader()
                .unwrap()
                .searcher()
                .search(
                    &qp.parse_query("alpha").unwrap(),
                    &TopDocs::with_limit(10).order_by_score(),
                )
                .unwrap()
                .len()
        })
        .await
        .unwrap();
        assert_eq!(hits, 2, "preloaded cold window answers a real term query");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unusable_hotcache_sidecar_is_rejected() {
        // task-150 / F5: an unframed/incompatible sidecar is rejected (so `open_cold_shard` falls
        // back to plain read-through) rather than mis-parsed.
        assert!(preload(b"not a framed sidecar at all").is_err());
        // A real sidecar with a corrupted magic byte is rejected too.
        let store_root = tempfile::tempdir().unwrap();
        let op = stage_index(store_root.path(), "cold/w1");
        let mut bytes = tokio::task::spawn_blocking(move || build(op, "cold/w1").unwrap())
            .await
            .unwrap();
        assert!(preload(&bytes).is_ok(), "the freshly-built sidecar loads");
        bytes[0] ^= 0xFF; // flip a magic byte
        assert!(preload(&bytes).is_err(), "a bad magic is rejected");
    }
}
