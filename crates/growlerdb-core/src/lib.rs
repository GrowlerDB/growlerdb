//! `growlerdb-core` — shared types, traits, and errors for GrowlerDB.
//!
//! GrowlerDB is an open-source **text search engine over Apache Iceberg** (and other
//! datastores). This crate holds the vocabulary the other crates build on; types
//! are fleshed out as the M0 walking-skeleton tasks land (see the project backlog).

pub mod api;
pub mod doc;
pub mod durable;
pub mod index_def;
pub mod query;
pub mod routing;
pub mod timestamp;
pub mod window;

pub use api::{
    cmp_sort_value, sort_has_score, validate_aggs, Agg, AggRange, CollapsedHit, CommitBatch, DocOp,
    Highlight, HighlightFragment, HighlightSegment, Hit, HydratedRow, IndexReader, IndexWriter,
    LocatedDoc, Projection, RowLocator, SearchAfter, SearchParams, ShardHits, Snapshot, Sort,
    SortOrder, SortValue, DEFAULT_HIGHLIGHT_FRAGMENT_SIZE, DEFAULT_HIGHLIGHT_MAX_FRAGMENTS,
    SCORE_SORT_KEY,
};
pub use doc::{CompositeKey, DocBatch, Document, KeyDecodeError, SourceCheckpoint, Value};
pub use index_def::{
    AlterPlan, DefError, EqualityDeleteHandling, FieldMapping, FieldType, IcebergSource,
    IndexDefinition, KeySpec, LocationStrategy, Mapping, ResolvedField, ResolvedIndex, ResolvedKey,
    ScanMode, Selection, Source, SourceField, SourceSchema, SourceType, TextRecord,
    MAX_CACHED_FIELD_BYTES,
};
pub use query::{MatchOp, ParseError, Query, Syntax};
pub use routing::{BucketMap, Reassignment, RoutingStrategy, ShardRouter};
pub use timestamp::{TimeFormat, TimeParseError};
pub use window::{TimeWindowing, WindowGranularity};

/// The GrowlerDB ASCII banner (brand art), shared across binaries' startup output.
pub const BANNER: &str = include_str!("banner.txt");

/// Crate version, from Cargo metadata.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// The startup splash: the [`BANNER`] art plus a name/version/tagline line,
/// printed by binaries (CLI now, server later) when they start.
pub fn startup_banner() -> String {
    format!(
        "{BANNER}\n    GrowlerDB v{}  ·  open-source text search over Apache Iceberg\n",
        version()
    )
}

/// GrowlerDB's top-level error. Variants are added as functionality lands.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A feature that is planned but not yet implemented.
    #[error("not yet implemented: {0}")]
    Unimplemented(&'static str),
}

/// Convenience result alias used across GrowlerDB crates.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    #[test]
    fn version_is_reported() {
        assert!(!super::version().is_empty());
    }
}
