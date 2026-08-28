//! `growlerdb-index` — the index store (Tantivy segments + a slim redb aux store) and the
//! in-process Index API (writer / reader / store).
//!
//! See the Index API and on-disk schema design docs.

pub mod bundle;
pub mod completion;
pub mod hotcache;
pub mod object_directory;
pub mod range_cache;
pub mod segment;
mod sidecar;
pub mod store;
pub mod vector;

pub use completion::{
    SegmentCompletion, COMPLETION_PREFIX_DEPTH, COMPLETION_SUFFIX, COMPLETION_TOP_K,
};
pub use object_directory::ObjectDirectory;
pub use range_cache::{CacheStats, RangeCache};
pub use segment::{
    ExplainHit, IndexError, IndexSchema, MappedFieldSummary, Result, SegmentReader,
    TantivySegmentCore, VectorFieldSummary, KEY_FIELD,
};
pub use store::{
    merge_aggregations, BackupSnapshot, ColdMarker, CompactionHealth, CompactionPolicy,
    LocalIndexStore, PreWarmPolicy, SealedSegment, Shard, ShardId, StoreError, COLD_MARKER,
};
pub use vector::{
    BruteForceIndex, HnswIndex, SegmentAnn, StoredAnnIndex, VectorIndex, VectorIndexError,
    ANN_SUFFIX, HNSW_MIN_VECTORS,
};

/// Crate version, from Cargo metadata.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
