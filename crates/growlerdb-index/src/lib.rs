//! `growlerdb-index` — the index store (Tantivy segments + the D30 layered locator +
//! a slim redb aux store) and the in-process Index API (writer / reader / store).
//!
//! See the Index API and on-disk schema design docs.

pub mod bundle;
pub mod hotcache;
pub mod location;
pub mod object_directory;
pub mod range_cache;
pub mod segment;
mod sidecar;
pub mod store;
pub mod vector;

pub use location::{LocationStore, ENTRY_BYTES, LOCATION_FILE};
pub use object_directory::ObjectDirectory;
pub use range_cache::{CacheStats, RangeCache};
pub use segment::{
    ExplainHit, IndexError, IndexSchema, Result, SegmentReader, TantivySegmentCore, KEY_FIELD,
};
pub use store::{
    merge_aggregations, BackupSnapshot, ColdMarker, CompactionHealth, CompactionPolicy,
    LocalIndexStore, PreWarmPolicy, RemapStats, SealedSegment, Shard, ShardId, StoreError,
    COLD_MARKER,
};
pub use vector::{BruteForceIndex, SegmentAnn, VectorIndex, VectorIndexError, ANN_SUFFIX};

/// Crate version, from Cargo metadata.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
