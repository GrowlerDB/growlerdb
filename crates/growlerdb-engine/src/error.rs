//! The engine's error type.

use growlerdb_core::{DefError, ParseError};
use growlerdb_index::StoreError;
use growlerdb_source::SourceError;

/// Errors from the engine façade (indexing, search, hydration).
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// A key had no locator entry — it was never indexed, or the index is behind.
    #[error("no locator for key: {0}")]
    MissingLocator(String),
    /// Search/hydrate referenced an index that has not been built.
    #[error("index `{0}` does not exist — run `growlerdb index` first")]
    NotIndexed(String),
    /// A default index could not pick an identifier field; supply a definition.
    #[error("no identifier field: provide a definition with `key.identifier_fields`")]
    NoIdentifier,
    /// `sync` (append fast-path) was called on a changelog-mode index — that's the
    /// connector's job, not the embedded incremental path.
    #[error("index `{0}` is not APPEND_FAST_PATH — changelog sync is the connector's job")]
    NotAppendFastPath(String),
    /// The index definition was invalid.
    #[error(transparent)]
    Definition(#[from] DefError),
    /// The query string could not be parsed.
    #[error(transparent)]
    Query(#[from] ParseError),
    /// The aux store / index operation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Reading the source (Iceberg) failed.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// A build read **0 documents from a non-empty source** (task-85) — the source's current
    /// snapshot reports `records` rows but the read produced none (e.g. a delete-in-history the
    /// changelog read mishandles). Fail loudly instead of committing a silently-empty index.
    #[error(
        "indexed 0 documents from `{table}`, but its current snapshot reports {records} records — \
         the source read is broken (e.g. a delete in the table's history); refusing to commit an empty index"
    )]
    EmptyReadFromNonEmptySource {
        /// The source table identifier.
        table: String,
        /// `total-records` from the current snapshot's summary.
        records: i64,
    },
    /// A **sharded build** (task-77) was asked for on a windowed index. Windowed indexes shard by
    /// time window, not by ordinal/bucket, so the two sharding models don't compose.
    #[error(
        "index `{0}` is windowed — it shards by time window, not by `--shards`/`--shard-ordinal`"
    )]
    ShardingWindowedUnsupported(String),
    /// A filesystem operation failed (reading/writing the persisted definition).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Encoding/decoding the persisted index definition failed.
    #[error("definition codec: {0}")]
    Codec(#[from] serde_json::Error),
}
