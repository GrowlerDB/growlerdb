//! `LocalIndexStore` — one Tantivy index per shard plus a small redb aux store, with an
//! incremental, crash-safe commit. Crash safety: the Tantivy commit lands before the redb
//! checkpoint txn; a crash re-applies the batch on resume, idempotent on the key
//! (delete-then-add). Commits are idempotent on `batch_id`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use growlerdb_core::{
    cmp_sort_value, durable, sort_has_score, Agg, CollapsedHit, CommitBatch, CompositeKey, DocOp,
    Document, Highlight, Hit, IndexReader, IndexWriter, Query, ResolvedIndex, SearchAfter,
    SearchParams, ShardHits, Snapshot, Sort, SortOrder, SortValue, SourceCheckpoint, Value,
};
use redb::{
    Database, MultimapTableDefinition, ReadTransaction, ReadableDatabase, ReadableMultimapTable,
    TableDefinition,
};
use serde::{Deserialize, Serialize};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::merge_policy::NoMergePolicy;
use tantivy::Term;

use crate::object_directory::ObjectDirectory;
use crate::range_cache::RangeCache;
use crate::segment::{
    ExplainHit, IndexError, IndexSchema, SegmentReader, TantivySegmentCore, WRITER_HEAP_BYTES,
};

/// A page of hits, each paired with its per-key sort values, plus the next-page keyset
/// cursor — the value-carrying counterpart of `(Vec<Hit>, Option<SearchAfter>)`. Lets the
/// Engine API put `sort_values` on every wire hit for cross-shard merge.
type ValuedPage = (Vec<(Hit, Vec<SortValue>)>, Option<SearchAfter>);

// `aux.redb` holds META + BATCH_KEYS (+ its BATCH_CKPT prune index). The live-key set is
// enumerated from the index with per-term liveness.
/// Small store metadata: `checkpoint` (JSON), `snapshot` (u64 LE).
const META: TableDefinition<&str, &[u8]> = TableDefinition::new("meta");
/// `batch_id` → commit snapshot; idempotent-retry guard (the presence of the key, not the
/// value, is what dedup consults). Pruned once a batch falls at/below the connector's
/// resume floor and can never be re-sent (keyed through [`BATCH_CKPT`]).
const BATCH_KEYS: TableDefinition<&str, u64> = TableDefinition::new("batch_keys");
/// Prune index over [`BATCH_KEYS`]: the checkpoint's **Iceberg sequence number** → the
/// `batch_id`s committed at it (a multimap — many batches can share one checkpoint). Lets the
/// write path **range-delete** every `batch_id` at/below the connector's resume floor instead of
/// scanning the whole table; written in the same redb txn as the `BATCH_KEYS` insert so they
/// never diverge.
///
/// The key **must be lineage-ordered** for the range prune to be sound (snapshot ids are random
/// longs, so a raw-id key could delete records still ahead of the floor).
/// [`migrate_batch_index`](LocalIndexStore::migrate_batch_index) clears any misordered generation
/// once at open (safe: the window-covering guard makes these records an optimization, not
/// correctness). A checkpoint with no sequence number gets no entry and is never pruned.
const BATCH_CKPT: MultimapTableDefinition<i64, &str> = MultimapTableDefinition::new("batch_ckpt");

const META_CHECKPOINT: &str = "checkpoint";
const META_SNAPSHOT: &str = "snapshot";

/// Max documents applied per Tantivy commit inside one [`Shard::commit_staged`]. Bounding it caps
/// per-commit apply+fsync cost (unbounded, one big batch is O(batch), ~4.5s @150k rows) and makes
/// early docs searchable mid-batch, while the checkpoint still advances once per batch. Env
/// `GROWLERDB_WRITE_COMMIT_CHUNK` overrides; `0` commits the whole batch at once. Default ~25k
/// (~1s/commit) balances commit latency against segment count.
fn commit_chunk_docs() -> usize {
    static CHUNK: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CHUNK.get_or_init(|| {
        std::env::var("GROWLERDB_WRITE_COMMIT_CHUNK")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(25_000)
    })
}
/// Marker that [`BATCH_CKPT`] is keyed by sequence number. Absent while any
/// misordered rows might exist ⇒ that generation is cleared once at open.
const META_BATCH_CKPT_ORDER: &str = "batch_ckpt_order";
/// Event-time zone-map bounds (i64 LE) for a windowed shard.
const META_EVENT_MIN: &str = "event_min";
const META_EVENT_MAX: &str = "event_max";
/// The source Iceberg `table-uuid` this index was built from — its lineage anchor. A
/// mismatch with the live table means the source was recreated and the index is stale.
const META_SOURCE_UUID: &str = "source_uuid";

/// Filename of the cold-park marker dropped in a parked window-shard dir. Public so the
/// backup/pre-warm layer can drop it when promoting a window back to hot.
pub const COLD_MARKER: &str = "cold.json";

/// Marker left in a window-shard dir when its Tantivy bulk has been **parked** to object storage.
/// The `aux.redb` stays local beside it; the index is served read-through from
/// `object_prefix` (see [`open_cold_shard`](LocalIndexStore::open_cold_shard)). Its presence is how
/// discovery tells a **cold** window from a **hot** one (which has a local `index/` dir instead).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdMarker {
    /// Object-store key prefix the Tantivy index files live under.
    pub object_prefix: String,
    /// Event-time zone-map lower bound — lets the gateway prune a cold window *without* opening it.
    #[serde(default)]
    pub event_min: Option<i64>,
    /// Event-time zone-map upper bound.
    #[serde(default)]
    pub event_max: Option<i64>,
    /// The committed snapshot this cold copy reflects.
    pub snapshot: u64,
    /// Object key of the precomputed **hotcache** sidecar, or `None` if one wasn't built.
    /// When present, [`open_cold_shard`](LocalIndexStore::open_cold_shard) preloads it so the cold
    /// open needs zero object round-trips.
    #[serde(default)]
    pub hotcache_key: Option<String>,
    /// Object key of the **split bundle** — the window's index files concatenated into one
    /// object — or `None` if unbundled. When present, cold reads issue ranged GETs against this one
    /// object instead of one object per file.
    #[serde(default)]
    pub bundle_key: Option<String>,
    /// Object key of the bundle's [`BundleLayout`](crate::bundle::BundleLayout) manifest,
    /// paired with `bundle_key`.
    #[serde(default)]
    pub bundle_manifest_key: Option<String>,
    /// Object key of the parked window's `aux.redb`. The primary reads its local copy; a replica on
    /// another node ([D53](/system/decisions/d53-unit-replication.md)) has none, so it fetches this
    /// key and opens read-through ([`open_cold_replica`](crate::LocalIndexStore::open_cold_replica)).
    /// A parked window is immutable, so the snapshot is frozen. `None` if parked before replica support.
    #[serde(default)]
    pub aux_key: Option<String>,
}

/// Cap on concurrently-open point-in-time handles per shard. Each held
/// [`ReadView`] pins a redb read version (retained pages until it closes), so the cap
/// bounds worst-case space amplification from a burst of *active* handles —
/// complementing [`expire_pits`](Shard::expire_pits), which only reaps *idle* ones.
pub const MAX_OPEN_PITS: usize = 64;

/// Errors from the local index store.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// Building or opening the Tantivy segment failed.
    #[error(transparent)]
    Segment(#[from] IndexError),
    /// A filesystem operation failed (staging/publish).
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// A redb operation failed.
    #[error("aux store: {0}")]
    Redb(#[from] redb::Error),
    /// Encoding/decoding a stored value failed.
    #[error("codec: {0}")]
    Codec(#[from] serde_json::Error),
    /// Encoding/decoding an aggregation **partial** (the mergeable intermediate form) failed.
    #[error("aggregation codec: {0}")]
    AggCodec(#[from] postcard::Error),
    /// A point-in-time handle was unknown or had expired (closed / TTL-evicted).
    #[error("unknown or expired point-in-time handle: {0}")]
    UnknownPit(u64),
    /// Too many open point-in-time handles ([`MAX_OPEN_PITS`]); close some first. The
    /// cap bounds redb space amplification from many concurrently-held read versions.
    #[error("too many open point-in-time handles (max {0}); close some first")]
    TooManyPits(usize),
    /// A windowed-write was requested for an index without `windowing` in its definition.
    #[error("index `{0}` is not windowed (its definition has no `windowing`)")]
    NotWindowed(String),
    /// Opening a cold shard's object-storage directory failed.
    #[error("cold store: {0}")]
    Cold(String),
    /// The source read that streams a reindex rebuild failed. The detail is the
    /// underlying `growlerdb-source` error, stringified to keep this crate independent of it.
    #[error("source read: {0}")]
    Source(String),
    /// A batch's `from` checkpoint doesn't continue from the shard's current checkpoint. Refused so
    /// the shard never overwrites its checkpoint forward over unapplied data (the structural
    /// silent-loss window). Non-retryable; the connector must resolve the discontinuity (typically a
    /// reindex/reconcile). Carries `(from, current)` for the operator.
    #[error("checkpoint gap: batch resumes from {from} but this shard is at {current}")]
    CheckpointGap { from: String, current: String },
    /// A write-path operation reached a **read-only cold** shard (a parked window served
    /// read-through, no writer). Routing normally shields cold shards, but late data or an admin
    /// call can still land here — refused cleanly, never a panic. Remedy: pre-warm the window first.
    #[error(
        "cannot {0} a read-only cold shard: the window is parked — revive (pre-warm) it first"
    )]
    ReadOnlyShard(&'static str),
    /// The on-disk index was built with a **different schema** than the definition now opening it
    /// (a mapped field added/removed/renamed, or a type/fast-ness change), so the derived Tantivy
    /// schema no longer matches the segment's `meta.json`. Writing new-schema docs into the stale
    /// index would corrupt the fast-field writer (a doc referencing a field ordinal the writer lacks),
    /// so refuse up front. Reindex from scratch (drop the index / fresh data dir, or `growlerdb index`).
    #[error("index `{index}` was built with a different schema; drop it (or use a fresh data dir / reindex) before rebuilding")]
    SchemaChanged { index: String },
}

/// Convenience result alias for the store.
pub type Result<T> = std::result::Result<T, StoreError>;

/// Outcome of a [`reconcile_partition`](Shard::reconcile_partition) stale-delete pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ReconcileDelete {
    /// Indexed docs removed as stale (absent from the source scan).
    pub deleted: usize,
    /// The stale-delete was **skipped** because the shard's checkpoint advanced during the source
    /// scan (a concurrent ingest committed) — deleting could have dropped a legitimately newer row
    /// (TOCTOU guard). Missing-repair still ran; the next reconcile retries the deletes.
    pub skipped_concurrent_write: bool,
}

impl ReconcileDelete {
    /// Nothing stale to remove (a clean partition).
    fn none() -> Self {
        Self::default()
    }
    /// Deletes were skipped by the TOCTOU guard.
    fn skipped() -> Self {
        Self {
            deleted: 0,
            skipped_concurrent_write: true,
        }
    }
}

/// Decode a little-endian `i64` from a stored meta value (0 if malformed/short).
fn i64_le(b: &[u8]) -> i64 {
    b.try_into().map(i64::from_le_bytes).unwrap_or(0)
}

// redb's call-site errors all convert into `redb::Error`; bridge them so `?` works.
macro_rules! redb_from {
    ($($t:ty),+ $(,)?) => {$(
        impl From<$t> for StoreError {
            fn from(e: $t) -> Self { StoreError::Redb(e.into()) }
        }
    )+};
}
redb_from!(
    redb::DatabaseError,
    redb::TransactionError,
    redb::TableError,
    redb::StorageError,
    redb::CommitError,
);

/// Identifies one shard (a node-local index partition). A single-shard index uses one shard; a
/// windowed index addresses shards by time-window id instead of ordinal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardId {
    /// Index name.
    pub index: String,
    /// Shard ordinal within the index (ignored when `window` is set).
    pub shard: u32,
    /// Time-window id (epoch ms of the window start) when the index is windowed; `None` = an
    /// ordinal shard.
    pub window: Option<i64>,
}

impl ShardId {
    /// A single-shard id for `index` (shard 0).
    pub fn single(index: impl Into<String>) -> Self {
        Self {
            index: index.into(),
            shard: 0,
            window: None,
        }
    }

    /// A **time-window** shard id for `index` at window-start `window`.
    pub fn window(index: impl Into<String>, window: i64) -> Self {
        Self {
            index: index.into(),
            shard: 0,
            window: Some(window),
        }
    }

    /// An **ordinal-shard** id for a hash/partition-sharded `index` (D52 pool): ordinal `ordinal`
    /// lives at its own path `{index}/{ordinal}`, so one pool node can hold several ordinals of the
    /// same index without collision. Ordinal 0 is the single-shard path — `shard(index, 0)` and
    /// [`single`](Self::single) are the same id.
    pub fn shard(index: impl Into<String>, ordinal: u32) -> Self {
        Self {
            index: index.into(),
            shard: ordinal,
            window: None,
        }
    }

    /// Relative on-disk path segment: `{index}/{shard}`, or `{index}/w{window}` when windowed.
    fn rel_path(&self) -> PathBuf {
        let seg = match self.window {
            Some(w) => format!("w{w}"),
            None => self.shard.to_string(),
        };
        PathBuf::from(&self.index).join(seg)
    }
}

/// The local index store: a root directory under which each shard lives.
#[derive(Clone)]
pub struct LocalIndexStore {
    root: PathBuf,
}

impl LocalIndexStore {
    /// Use `root` as the store base (e.g. `/data/growlerdb`). Created if absent.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    /// Create (or open) a shard for `index`, deriving its schema. Opens the shard's
    /// single Tantivy index (creating it if absent; Tantivy recovers any uncommitted
    /// writer state on open) and its redb aux store.
    pub fn create_shard(&self, id: &ShardId, index: &ResolvedIndex) -> Result<Shard> {
        self.build_shard(self.root.join(id.rel_path()), index)
    }

    /// Open an existing shard (create-if-absent, as [`create_shard`](Self::create_shard)).
    pub fn open_shard(&self, id: &ShardId, index: &ResolvedIndex) -> Result<Shard> {
        self.create_shard(id, index)
    }

    /// Open a hot shard **reusing an already-open `aux.redb` handle** — the pre-warm (cold→hot)
    /// transition passes the retiring cold shard's [`db_handle`](Shard::db_handle) so promoting a
    /// window in place never opens a second redb handle on the same file (redb allows only one). The
    /// window's `aux.redb` was already local while cold, so it is the same handle either way.
    pub fn open_shard_reusing_db(
        &self,
        id: &ShardId,
        index: &ResolvedIndex,
        db: Arc<Database>,
    ) -> Result<Shard> {
        self.build_shard_with_db(self.root.join(id.rel_path()), index, Some(db))
    }

    /// Open a **cold** window shard **read-through**: its Tantivy index is served from
    /// object storage under `prefix` in `op` (via [`ObjectDirectory`] + the shared range `cache`),
    /// while its `aux.redb` (checkpoints + event-time zone-map) stays **local** under `aux_dir` — kept
    /// in the cold footprint when the index bulk was parked. The shard is **read-only** (no writer);
    /// the gateway routes only searches to it, and every search method works unchanged over the
    /// read-through reader. Must be called from within a tokio runtime, since `ObjectDirectory`
    /// reads `block_on` it (the Search service already runs query execution on `spawn_blocking`).
    #[allow(clippy::too_many_arguments)]
    pub fn open_cold_shard(
        &self,
        index: &ResolvedIndex,
        aux_dir: &Path,
        op: opendal::Operator,
        prefix: &str,
        cache: RangeCache,
        hotcache_key: Option<&str>,
        bundle: Option<(&str, &str)>,
        reuse_db: Option<Arc<Database>>,
    ) -> Result<Shard> {
        let schema = IndexSchema::from_resolved(index);
        let mut dir = ObjectDirectory::open(op.clone(), prefix)
            .map_err(|e| StoreError::Cold(e.to_string()))?
            .with_cache(cache.clone());
        // Blocking operator for the small sidecar/manifest reads (op is async; this open path is sync).
        let bop =
            opendal::blocking::Operator::new(op).map_err(|e| StoreError::Cold(e.to_string()))?;
        // Precomputed hotcache: one GET preloads the structural reads so the window opens with zero
        // further round-trips. Missing/unreadable/incompatible → fall back to plain read-through,
        // never fail the open on a stale hotcache.
        if let Some(key) = hotcache_key {
            match bop.read(key) {
                Ok(buf) => match crate::hotcache::preload(&buf.to_vec()) {
                    Ok(hot) => dir = dir.with_hot(std::sync::Arc::new(hot)),
                    Err(e) => {
                        tracing::warn!(hotcache = %key, error = %e, "cold open: ignoring unusable hotcache; reading through")
                    }
                },
                Err(e) if e.kind() == opendal::ErrorKind::NotFound => {}
                Err(e) => return Err(StoreError::Cold(e.to_string())),
            }
        }
        // Split bundle: read the layout manifest, then serve each file read as a ranged GET of the
        // one bundle object instead of one object per file.
        if let Some((bundle_key, manifest_key)) = bundle {
            let manifest = bop
                .read(manifest_key)
                .map_err(|e| StoreError::Cold(e.to_string()))?;
            let state = crate::bundle::BundleState::from_bytes(bundle_key, &manifest.to_vec())?;
            dir = dir.with_bundle(std::sync::Arc::new(state));
        }
        let tantivy = tantivy::Index::open(dir).map_err(|e| StoreError::Segment(e.into()))?;
        // No local index files; point the reader at `aux_dir` so the ANN KNN path has a directory to
        // probe (it finds no local sidecar and returns no vector hits — cold-tier KNN is a later step).
        let core =
            SegmentReader::live(&tantivy, aux_dir.to_path_buf()).map_err(StoreError::Segment)?;
        // Reuse the retiring shard's `aux.redb` handle when parking in place (redb allows one open
        // per file); otherwise open fresh.
        let db = match reuse_db {
            Some(db) => db,
            None => Arc::new(Database::open(aux_dir.join("aux.redb"))?),
        };
        Ok(Shard {
            index: tantivy,
            // No local Tantivy dir; the search path never reads `index_dir` (only backup/replica
            // does, which a cold shard isn't part of).
            index_dir: aux_dir.to_path_buf(),
            core,
            schema,
            db,
            writer: None,
            pits: Mutex::new(HashMap::new()),
            next_pit: AtomicU64::new(1),
            commit_chunk: commit_chunk_docs(),
            #[cfg(test)]
            commit_trace: Mutex::new(Vec::new()),
        })
    }

    /// Open a **parked window as a cross-node replica** (D53): unlike [`open_cold_shard`], which
    /// reads the window's `aux.redb` from a **local** `aux_dir` (the primary keeps it beside the
    /// marker), a replica on another node has no local copy — so this fetches the sidecar from
    /// object storage (the [`aux_key`](ColdMarker::aux_key) `backup()` uploaded) into `scratch_dir`,
    /// then opens the window read-through exactly like a cold open. The window is immutable, so the
    /// fetched snapshot is consistent and never re-synced. Errors if the marker predates replica
    /// support (no sidecar key — re-park to enable it).
    pub fn open_cold_replica(
        &self,
        index: &ResolvedIndex,
        scratch_dir: &Path,
        op: opendal::Operator,
        marker: &ColdMarker,
        cache: RangeCache,
    ) -> Result<Shard> {
        let aux_key = match &marker.aux_key {
            Some(a) => a,
            None => {
                return Err(StoreError::Cold(
                    "parked window has no replica sidecar key (aux_key) — re-park it \
                     to enable cross-node replica read-through"
                        .into(),
                ))
            }
        };
        std::fs::create_dir_all(scratch_dir)?;
        // Blocking operator for the one-shot sidecar download (this open path is synchronous).
        let bop = opendal::blocking::Operator::new(op.clone())
            .map_err(|e| StoreError::Cold(e.to_string()))?;
        let aux = bop
            .read(aux_key)
            .map_err(|e| StoreError::Cold(e.to_string()))?;
        std::fs::write(scratch_dir.join("aux.redb"), aux.to_vec())?;
        let bundle = marker
            .bundle_key
            .as_deref()
            .zip(marker.bundle_manifest_key.as_deref());
        // Fresh aux.redb (no live hot shard to reuse a handle from — this is a replica node).
        self.open_cold_shard(
            index,
            scratch_dir,
            op,
            &marker.object_prefix,
            cache,
            marker.hotcache_key.as_deref(),
            bundle,
            None,
        )
    }

    /// The cold-park [`ColdMarker`] for window `w` of `index`, or `None` if the window is **hot**
    /// (still local) or absent. Discovery uses this to tell hot windows (local `index/`)
    /// from cold ones (parked: this marker + its event zone-map for pruning).
    pub fn cold_marker(&self, index: &str, w: i64) -> Result<Option<ColdMarker>> {
        let path = self
            .shard_path(&ShardId::window(index, w))
            .join(COLD_MARKER);
        match std::fs::read(&path) {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// The on-disk path of the cold-park marker for window `w` of `index` (written by `cold_park`).
    pub fn cold_marker_path(&self, index: &str, w: i64) -> PathBuf {
        self.shard_path(&ShardId::window(index, w))
            .join(COLD_MARKER)
    }

    /// The canonical on-disk directory for `id` — e.g. for a free-disk precheck before a reindex.
    /// The directory may not exist yet.
    pub fn shard_path(&self, id: &ShardId) -> PathBuf {
        self.root.join(id.rel_path())
    }

    /// The existing time-window ids for `index` — its `w<window>` shard directories, ascending.
    /// Empty when the index has no window shards yet.
    pub fn window_shards(&self, index: &str) -> Result<Vec<i64>> {
        let dir = self.root.join(index);
        let mut windows = Vec::new();
        if dir.exists() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    if let Some(w) = entry
                        .file_name()
                        .to_str()
                        .and_then(|n| n.strip_prefix('w'))
                        .and_then(|s| s.parse::<i64>().ok())
                    {
                        windows.push(w);
                    }
                }
            }
        }
        windows.sort_unstable();
        Ok(windows)
    }

    /// The **ordinal shards** of a hash/partition-sharded `index` present on disk — the numeric
    /// `{index}/{ordinal}` dirs (D52 pool). A pool node opens exactly these ordinals it holds. Windowed
    /// dirs (`w{window}`) are skipped, so a mixed store never confuses the two. Ascending by ordinal.
    pub fn ordinal_shards(&self, index: &str) -> Result<Vec<u32>> {
        let dir = self.root.join(index);
        let mut ordinals = Vec::new();
        if dir.exists() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                if entry.file_type()?.is_dir() {
                    if let Some(o) = entry
                        .file_name()
                        .to_str()
                        .and_then(|n| n.parse::<u32>().ok())
                    {
                        ordinals.push(o);
                    }
                }
            }
        }
        ordinals.sort_unstable();
        Ok(ordinals)
    }

    /// Apply a [`CommitBatch`] to a **windowed** index: route upserts to per-window
    /// shards by ingest-time (creating windows as needed), widen each window's event-time
    /// zone-map, and **broadcast deletes** to every existing window (only the owner has the key —
    /// rare for the append-mostly sources windowing targets). Returns the window ids upserted to.
    /// Errors with [`StoreError::NotWindowed`] if `index` has no `windowing`.
    pub fn write_windowed(&self, index: &ResolvedIndex, batch: &CommitBatch) -> Result<Vec<i64>> {
        let windowing = index
            .windowing
            .as_ref()
            .ok_or_else(|| StoreError::NotWindowed(index.name.clone()))?;
        // Pass each field's `format` so values normalize to canonical micros before bucketing —
        // matching the unit the index/range path stores.
        let format_of = |name: &str| {
            index
                .fields
                .iter()
                .find(|f| f.path == name)
                .and_then(|f| f.format)
        };
        let (window_batches, deletes) = windowing.partition_batch(
            batch,
            format_of(&windowing.field),
            windowing.event_time_field.as_deref(),
            windowing.event_time_field.as_deref().and_then(format_of),
        );

        let mut written = Vec::with_capacity(window_batches.len());
        for wb in &window_batches {
            let shard = self.create_shard(&ShardId::window(&index.name, wb.window), index)?;
            IndexWriter::write(&shard, &wb.batch)?;
            shard.set_event_bounds(wb.event_min, wb.event_max)?;
            written.push(wb.window);
        }
        if !deletes.is_empty() {
            // Carry the connector's resume floor so each window's idempotency records prune
            // (`from` stays absent by design — see `partition_batch`).
            let del_batch = CommitBatch::new(
                deletes,
                batch.checkpoint.clone(),
                format!("{}#del", batch.batch_id),
            )
            .with_safe_checkpoint(batch.safe_checkpoint.clone());
            for w in self.window_shards(&index.name)? {
                let shard = self.create_shard(&ShardId::window(&index.name, w), index)?;
                IndexWriter::write(&shard, &del_batch)?;
            }
        }
        Ok(written)
    }

    /// Search a **windowed** index: prune to the window shards a time-filtered `query` can
    /// match — by ingest-window id (cheap, *before* opening the shard) then by each survivor's
    /// event-time zone-map — search only those, and merge the global top-`k` by score (ties broken
    /// by encoded key, matching the gateway's cross-shard merge). A query without a relevant range
    /// bound fans out to every window — never wrong, only un-pruned. Errors with
    /// [`StoreError::NotWindowed`] if `index` has no `windowing`.
    pub fn search_windowed(
        &self,
        index: &ResolvedIndex,
        query: &Query,
        k: usize,
    ) -> Result<Vec<Hit>> {
        let windowing = index
            .windowing
            .as_ref()
            .ok_or_else(|| StoreError::NotWindowed(index.name.clone()))?;
        let mut hits = Vec::new();
        for w in self.window_shards(&index.name)? {
            // Ingest-window prune before paying to open the shard (zone unknown yet → `None`).
            if !windowing.keeps(w, None, query) {
                continue;
            }
            let shard = self.create_shard(&ShardId::window(&index.name, w), index)?;
            // Event-time zone-map prune now that the shard's bounds are readable.
            if !windowing.keeps(w, shard.event_bounds()?, query) {
                continue;
            }
            hits.extend(shard.search_all(query, k)?);
        }
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.encode().cmp(&b.key.encode()))
        });
        hits.truncate(k);
        Ok(hits)
    }

    /// One-time re-key of the batch-idempotency prune index: misordered [`BATCH_CKPT`] rows keyed by
    /// raw snapshot id (a random long) make the range prune unsound. Ordering can't be recovered from
    /// the stored key, so the old generation — the prune index plus the [`BATCH_KEYS`] records it
    /// indexes — is cleared once and the [`META_BATCH_CKPT_ORDER`] marker set. Safe: the
    /// window-covering guard makes these records an optimization, not correctness.
    fn migrate_batch_index(db: &Database) -> Result<()> {
        let read = db.begin_read()?;
        let migrated = match read.open_table(META) {
            Ok(meta) => meta.get(META_BATCH_CKPT_ORDER)?.is_some(),
            Err(redb::TableError::TableDoesNotExist(_)) => false,
            Err(e) => return Err(e.into()),
        };
        drop(read);
        if migrated {
            return Ok(());
        }
        let txn = db.begin_write()?;
        {
            let mut batches = txn.open_table(BATCH_KEYS)?;
            batches.retain(|_, _| false)?;
            let mut ckpt = txn.open_multimap_table(BATCH_CKPT)?;
            // Collect keys first: the iterator borrows the table.
            let keys: Vec<i64> = ckpt
                .iter()?
                .map(|e| e.map(|(k, _)| k.value()))
                .collect::<std::result::Result<_, _>>()?;
            for k in keys {
                ckpt.remove_all(k)?;
            }
            let mut meta = txn.open_table(META)?;
            meta.insert(META_BATCH_CKPT_ORDER, b"seq".as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    fn build_shard(&self, dir: PathBuf, index: &ResolvedIndex) -> Result<Shard> {
        self.build_shard_with_db(dir, index, None)
    }

    /// Build a hot shard, optionally **reusing** an already-open `aux.redb` handle (`reuse_db`)
    /// instead of opening a second one — the in-process pre-warm (cold→hot) transition passes the
    /// retiring cold shard's handle so the two never race redb's one-open-per-file rule. `None`
    /// opens/creates the db fresh (the normal build/create path).
    fn build_shard_with_db(
        &self,
        dir: PathBuf,
        index: &ResolvedIndex,
        reuse_db: Option<Arc<Database>>,
    ) -> Result<Shard> {
        std::fs::create_dir_all(&dir)?;
        let schema = IndexSchema::from_resolved(index);
        let index_dir = dir.join("index");
        let tantivy = TantivySegmentCore
            .open_or_create_index(&schema, &index_dir)
            .map_err(StoreError::Segment)?;
        // Guardrail: an existing index's persisted (`meta.json`) schema must match the one derived
        // from `index`. Writing new-schema docs through a stale index's writer corrupts the
        // fast-field writer (see [`StoreError::SchemaChanged`]), so refuse before any doc is written.
        if tantivy.schema() != *schema.tantivy_schema() {
            return Err(StoreError::SchemaChanged {
                index: index.name.clone(),
            });
        }
        let core = SegmentReader::live(&tantivy, index_dir.clone()).map_err(StoreError::Segment)?;
        let db = match reuse_db {
            Some(db) => db, // already open + migrated (shared across a tier swap)
            None => {
                let db = Arc::new(Database::create(dir.join("aux.redb"))?);
                Self::migrate_batch_index(&db)?;
                db
            }
        };
        let writer: tantivy::IndexWriter = tantivy
            .writer(WRITER_HEAP_BYTES)
            .map_err(|e| StoreError::Segment(e.into()))?;
        writer.set_merge_policy(Box::new(NoMergePolicy));
        Ok(Shard {
            index: tantivy,
            index_dir,
            core,
            schema,
            db,
            writer: Some(Mutex::new(writer)),
            pits: Mutex::new(HashMap::new()),
            next_pit: AtomicU64::new(1),
            commit_chunk: commit_chunk_docs(),
            #[cfg(test)]
            commit_trace: Mutex::new(Vec::new()),
        })
    }

    /// **Reindex** a shard durably in one shot: [`build_staging_generation`] then
    /// [`promote_generation`]. Builds a fresh replacement at a staging sibling, populates it via
    /// `populate` (e.g. a full source read), then atomically swaps it in and reopens it at the
    /// canonical path, returning the promoted shard. A crash leaves recoverable state that
    /// [`recover_reindex`] resolves on the next open. Retired shards' in-flight readers/PITs keep
    /// their files alive via open-fd inode refs, so dropping the backup is safe.
    ///
    /// Multi-shard reindex calls the two halves separately so it can build every shard's next
    /// generation and only cut over once all succeed (see [`build_staging_generation`]).
    ///
    /// [`build_staging_generation`]: Self::build_staging_generation
    /// [`promote_generation`]: Self::promote_generation
    /// [`recover_reindex`]: Self::recover_reindex
    pub fn reindex<F>(&self, id: &ShardId, index: &ResolvedIndex, populate: F) -> Result<Shard>
    where
        F: FnOnce(&Shard) -> Result<()>,
    {
        self.build_staging_generation(id, index, populate)?;
        self.promote_generation(id, index)
    }

    /// Build the **next generation** of a shard into its staging sibling directory and populate it
    /// via `populate` (a full source read, optionally filtered), leaving it **durable but not
    /// promoted** — the live canonical shard is untouched and still serves. A later
    /// [`promote_generation`] cuts over, or [`discard_staging`] drops it.
    ///
    /// This is the build half of [`reindex`], split out so a multi-shard reindex can build every
    /// shard's next generation and cut over only once **all** succeed — the atomic cutover is the
    /// control plane's single decision, not a per-node one, so this must NOT auto-promote. No commit
    /// marker is written here, so a crash between build and promote is a torn attempt that
    /// [`recover_reindex`] discards: the old generation stays authoritative and the reindex is
    /// retried (identical crash semantics to the one-shot path, whose commit marker is likewise only
    /// written at promote).
    ///
    /// [`promote_generation`]: Self::promote_generation
    /// [`discard_staging`]: Self::discard_staging
    /// [`recover_reindex`]: Self::recover_reindex
    pub fn build_staging_generation<F>(
        &self,
        id: &ShardId,
        index: &ResolvedIndex,
        populate: F,
    ) -> Result<()>
    where
        F: FnOnce(&Shard) -> Result<()>,
    {
        let canonical = self.root.join(id.rel_path());
        let staging = sibling(&canonical, "reindex");
        let marker = sibling(&canonical, "commit");
        let parent = canonical
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.root.clone());

        // Clear leftovers from a prior interrupted attempt (recover_reindex normally ran first).
        if staging.exists() {
            std::fs::remove_dir_all(&staging)?;
        }
        if marker.exists() {
            std::fs::remove_file(&marker)?;
        }

        // Build + populate the replacement, then drop it so its files are flushed and
        // closed. The rebuild re-adds every doc from the source through the normal commit path.
        {
            let staged = self.build_shard(staging.clone(), index)?;
            populate(&staged)?;
        }

        // Make staging durable. The commit marker is deliberately NOT written here — it is written
        // by `promote_generation` at cutover, so recovery never promotes an uncommitted build.
        durable::sync_dir(&staging)?;
        durable::sync_dir(&parent)?;
        Ok(())
    }

    /// Cut over to a staged next generation built by [`build_staging_generation`]: write the durable
    /// `*.commit` marker (the promise [`recover_reindex`] rolls forward on), swap the old shard aside
    /// to a `*.old` backup and the staging shard into the canonical path, reopen it, then drop the
    /// backup. Returns the promoted shard so the caller can install it. The swap ordering + marker
    /// are identical to the one-shot [`reindex`], so a crash mid-swap recovers exactly as before.
    ///
    /// [`build_staging_generation`]: Self::build_staging_generation
    /// [`recover_reindex`]: Self::recover_reindex
    pub fn promote_generation(&self, id: &ShardId, index: &ResolvedIndex) -> Result<Shard> {
        let canonical = self.root.join(id.rel_path());
        let staging = sibling(&canonical, "reindex");
        let backup = sibling(&canonical, "old");
        let marker = sibling(&canonical, "commit");
        let parent = canonical
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.root.clone());

        // The marker's existence means "staging is safe to promote" — write it durably before any
        // rename (the following parent fsyncs persist its dir entry), so recovery rolls forward from
        // any point in the swap below.
        durable::write(&marker, REINDEX_COMMIT_MARKER)?;

        // Swap: old → backup, new → canonical; fsync the parent after each rename so the dir
        // entries are durable.
        std::fs::rename(&canonical, &backup)?;
        durable::sync_dir(&parent)?;
        std::fs::rename(&staging, &canonical)?;
        durable::sync_dir(&parent)?;
        let promoted = self.build_shard(canonical.clone(), index)?;

        // Promotion complete: drop the backup first, then the marker — the marker must outlive
        // the backup so a crash here still recovers to the promoted index.
        let _ = std::fs::remove_dir_all(&backup);
        let _ = std::fs::remove_file(&marker);
        durable::sync_dir(&parent)?;
        Ok(promoted)
    }

    /// Discard a staged next generation built by [`build_staging_generation`] without promoting it —
    /// e.g. a multi-shard reindex the control plane aborted because a sibling shard failed, or a
    /// cancelled job. The live canonical shard is untouched; only the staging sibling is removed. No
    /// commit marker exists for an unpromoted build, so nothing else needs unwinding.
    ///
    /// [`build_staging_generation`]: Self::build_staging_generation
    pub fn discard_staging(&self, id: &ShardId) -> Result<()> {
        let canonical = self.root.join(id.rel_path());
        let staging = sibling(&canonical, "reindex");
        if staging.exists() {
            std::fs::remove_dir_all(&staging)?;
        }
        Ok(())
    }

    /// Open the **staged next generation** built by [`build_staging_generation`] (not yet promoted)
    /// as a writable [`Shard`], so a reindex **write catch-up** can apply the source delta that
    /// accumulated during the unfenced build onto the staged generation before the cutover — leaving
    /// it stamped at ~head so the promoted generation doesn't regress the live checkpoint. The
    /// canonical live shard is untouched; a later [`promote_generation`](Self::promote_generation)
    /// swaps the caught-up staging in. Errors if no staged generation exists (nothing to catch up).
    ///
    /// [`build_staging_generation`]: Self::build_staging_generation
    pub fn open_staging(&self, id: &ShardId, index: &ResolvedIndex) -> Result<Shard> {
        let canonical = self.root.join(id.rel_path());
        let staging = sibling(&canonical, "reindex");
        if !staging.exists() {
            return Err(StoreError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "no staged generation to catch up",
            )));
        }
        self.build_shard(staging, index)
    }

    /// Resolve an interrupted [`reindex`](Self::reindex) for `id` — call before opening the
    /// shard at startup. Keyed off the durable `*.commit` marker:
    ///
    /// * **marker present** ⇒ the staged index was fully durable, so roll the promotion
    ///   **forward**: ensure the canonical path holds the new (staging) index, then drop the
    ///   backup + marker. A safety net restores the `*.old` backup if neither the new nor the
    ///   old index is at the canonical path, so recovery never leaves the shard without an index.
    /// * **no marker** ⇒ a torn, pre-commit attempt: the canonical index is authoritative, so
    ///   discard any stray staging/backup. Never promotes a half-built staging dir.
    pub fn recover_reindex(&self, id: &ShardId) -> Result<()> {
        let canonical = self.root.join(id.rel_path());
        let staging = sibling(&canonical, "reindex");
        let backup = sibling(&canonical, "old");
        let marker = sibling(&canonical, "commit");
        let parent = canonical
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.root.clone());

        if marker.exists() {
            // Roll forward: promote the durable staging index if it hasn't been swapped in yet.
            if staging.exists() {
                if canonical.exists() {
                    if backup.exists() {
                        std::fs::remove_dir_all(&backup)?;
                    }
                    std::fs::rename(&canonical, &backup)?;
                    durable::sync_dir(&parent)?;
                }
                std::fs::rename(&staging, &canonical)?;
                durable::sync_dir(&parent)?;
            }
            // Safety net: never leave the canonical path without an index — restore the old
            // backup if the new index isn't there (a pathological mid-swap state).
            if !canonical.exists() && backup.exists() {
                std::fs::rename(&backup, &canonical)?;
                durable::sync_dir(&parent)?;
            }
            if backup.exists() {
                std::fs::remove_dir_all(&backup)?;
            }
            std::fs::remove_file(&marker)?;
            durable::sync_dir(&parent)?;
        } else {
            // No commit ⇒ canonical is authoritative; discard any half-built strays.
            if staging.exists() {
                std::fs::remove_dir_all(&staging)?;
            }
            if backup.exists() {
                std::fs::remove_dir_all(&backup)?;
            }
        }
        Ok(())
    }
}

/// Marker file contents written once staging is durable, signalling a reindex is safe to
/// promote on recovery. Any non-empty payload works; the *presence* is the signal.
const REINDEX_COMMIT_MARKER: &[u8] = b"reindex-committed\n";

/// Total size in bytes of the files under `dir` (recursive); 0 if absent/unreadable. Best-effort
/// disk-usage for the per-shard size signal.
fn dir_size_bytes(dir: &Path) -> u64 {
    let mut total = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        match entry.metadata() {
            Ok(meta) if meta.is_dir() => total += dir_size_bytes(&entry.path()),
            Ok(meta) => total += meta.len(),
            Err(_) => {}
        }
    }
    total
}

/// A shard's on-disk index size split by component — see [`Shard::index_size_breakdown`]. The inverted index is reported by its Tantivy file kind so
/// storage work (dropping positions, fast-only numerics, compact key terms) is attributable to the
/// structure it actually shrinks, not a lump "inverted" total.
#[derive(Debug, Clone, Copy, Default)]
pub struct IndexSizeBreakdown {
    /// Term dictionaries (`.term`) — one entry per unique term per segment.
    pub term: u64,
    /// Postings lists (`.idx`) — doc ids (+ freqs) per term.
    pub postings: u64,
    /// Token positions (`.pos`) — phrase-query support on analyzed TEXT fields.
    pub positions: u64,
    /// Fieldnorms (`.fieldnorm`) — per-doc field lengths for BM25.
    pub fieldnorms: u64,
    /// Fast fields — columnar values for sort/aggregation/range ("fast cache").
    pub fast: u64,
    /// Stored-document data.
    pub store: u64,
    /// Segment/index metadata, deletes, and anything else under `index_dir`, plus the sibling
    /// `aux.redb` (checkpoint / batch idempotency / zone-map).
    pub other: u64,
}

impl IndexSizeBreakdown {
    /// The classic **inverted index** total: term dicts + postings + positions + fieldnorms.
    pub fn inverted(&self) -> u64 {
        self.term + self.postings + self.positions + self.fieldnorms
    }

    /// Every component summed — the shard's full on-disk index footprint (Tantivy files
    /// **plus** the sibling `aux.redb`). This is what `growlerdb_index_bytes` reports, so the
    /// total gauge and `sum(growlerdb_index_bytes_component)` reconcile exactly.
    pub fn total(&self) -> u64 {
        self.inverted() + self.fast + self.store + self.other
    }
}

/// `{dir}` → a sibling path `{dir}.{suffix}` (e.g. `…/docs/0` → `…/docs/0.reindex`).
fn sibling(path: &Path, suffix: &str) -> PathBuf {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("shard");
    path.with_file_name(format!("{name}.{suffix}"))
}

/// Build the Tantivy aggregation request from the typed [`Agg`]s (via JSON).
fn build_aggregations(aggs: &[(String, Agg)]) -> Result<Aggregations> {
    let mut req = serde_json::Map::new();
    for (name, agg) in aggs {
        let body = match agg {
            Agg::Terms { field, size } => {
                // `show_term_doc_count_error` surfaces Tantivy's `doc_count_error_upper_bound` as
                // an accuracy signal: cross-shard top-N is exact only within Tantivy's
                // implicit `size×10` over-fetch window — a globally-top term below that window on
                // several shards would otherwise be silently folded into `sum_other_doc_count`.
                serde_json::json!({
                    "terms": { "field": field, "size": size, "show_term_doc_count_error": true }
                })
            }
            Agg::Stats { field } => serde_json::json!({ "stats": { "field": field } }),
            Agg::DateHistogram {
                field,
                fixed_interval,
            } => serde_json::json!({
                "date_histogram": { "field": field, "fixed_interval": fixed_interval }
            }),
            Agg::Range { field, ranges } => {
                // Emit only the present bounds so an open end stays open.
                let ranges: Vec<serde_json::Value> = ranges
                    .iter()
                    .map(|r| {
                        let mut o = serde_json::Map::new();
                        if let Some(from) = r.from {
                            o.insert("from".into(), serde_json::json!(from));
                        }
                        if let Some(to) = r.to {
                            o.insert("to".into(), serde_json::json!(to));
                        }
                        serde_json::Value::Object(o)
                    })
                    .collect();
                serde_json::json!({ "range": { "field": field, "ranges": ranges } })
            }
            Agg::Cardinality { field } => {
                serde_json::json!({ "cardinality": { "field": field } })
            }
            Agg::Percentiles { field, percents } => serde_json::json!({
                "percentiles": { "field": field, "percents": percents }
            }),
        };
        req.insert(name.clone(), body);
    }
    Ok(serde_json::from_value(serde_json::Value::Object(req))?)
}

/// Finalize intermediate aggregation results against `request` → a `name → result` JSON map.
fn finalize_aggregations(
    inter: IntermediateAggregationResults,
    request: Aggregations,
) -> Result<BTreeMap<String, serde_json::Value>> {
    let results = inter
        .into_final_result(request, Default::default())
        .map_err(IndexError::Tantivy)?;
    match serde_json::to_value(&results)? {
        serde_json::Value::Object(m) => Ok(m.into_iter().collect()),
        _ => Ok(BTreeMap::new()),
    }
}

/// **Merge** per-shard [`aggregate_partial`](Shard::aggregate_partial) results into one
/// finalized `name → result` map (the Gateway's cross-shard aggregation merge):
/// deserialize each partial's intermediate results, `merge_fruits` them, then finalize. Empty
/// input ⇒ an empty map.
///
/// The **additive** aggregations — `terms` (summed bucket counts), `stats`, `range`,
/// `date_histogram` — merge **exactly** across shards. **`cardinality` (HLL) and `percentiles`
/// (DDSketch)** are **approximate but correctly merged**: `merge_fruits` unions the
/// deserialized sketches (`HllUnion` / `DDSketch::merge`), so a cross-shard distinct/percentile
/// carries only the sketch's own error — it is NOT under-counted. (Verified by
/// `cross_shard_cardinality_and_percentiles_match_single_shard`: the merged HLL equals a single
/// shard over all data, percentiles within ~5%.) `terms` top-N is exact only within Tantivy's
/// `size×10` over-fetch window — see `doc_count_error_upper_bound` for the accuracy signal.
pub fn merge_aggregations(
    partials: &[Vec<u8>],
    aggs: &[(String, Agg)],
) -> Result<BTreeMap<String, serde_json::Value>> {
    let mut merged: Option<IntermediateAggregationResults> = None;
    for partial in partials {
        let inter: IntermediateAggregationResults = postcard::from_bytes(partial)?;
        match &mut merged {
            None => merged = Some(inter),
            Some(acc) => acc.merge_fruits(inter).map_err(IndexError::Tantivy)?,
        }
    }
    match merged {
        Some(inter) => finalize_aggregations(inter, build_aggregations(aggs)?),
        None => Ok(BTreeMap::new()),
    }
}

/// Live fragmentation signals that drive **health-driven auto-compaction**: how many
/// segments the shard has and how much delete debt they carry, read from committed segment metadata.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CompactionHealth {
    /// Number of searchable segments (fragmentation / merge pressure).
    pub segments: u64,
    /// Total `max_doc` across segments (live + deleted) — the delete-ratio denominator.
    pub max_doc: u64,
    /// Total deleted-but-unpurged docs across segments (purge pressure).
    pub deleted: u64,
}

impl CompactionHealth {
    /// Fraction of docs that are deleted-but-not-yet-purged (`0.0` for an empty shard).
    pub fn deleted_ratio(&self) -> f64 {
        if self.max_doc == 0 {
            0.0
        } else {
            self.deleted as f64 / self.max_doc as f64
        }
    }
}

/// When a shard is fragmented enough to be worth compacting: too many segments, or too
/// much delete debt. Pure of any scheduling — a serving loop reads [`Shard::compaction_health`] on a
/// timer and calls [`reason_to_compact`](Self::reason_to_compact); a `Some` triggers
/// [`Shard::compact`].
#[derive(Debug, Clone, Copy)]
pub struct CompactionPolicy {
    /// Compact once the shard has at least this many segments.
    pub min_segments: u64,
    /// Compact once deleted-but-unpurged docs reach this fraction of all docs (`0.0`–`1.0`).
    pub max_deleted_ratio: f64,
    /// Max segments merged **per bounded pass**. Compaction never merges the whole shard
    /// at once — each pass merges up to this many *similar-sized* segments and releases the writer
    /// lock, so a single merge is O(a size tier), not O(shard), and ingest interleaves between passes.
    pub merge_factor: usize,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        // Compact at ≥8 segments OR ≥20% deleted — enough churn to pay back the merge I/O without
        // thrashing on every small commit. Merge ≤8 same-tier segments per bounded pass.
        Self {
            min_segments: 8,
            max_deleted_ratio: 0.2,
            merge_factor: 8,
        }
    }
}

/// Size ratio between compaction tiers: segments whose live-doc counts are within this
/// factor are the "same size" and merge together; a merged group jumps to the next tier. Bounds
/// segment count to ~`merge_factor` per tier × `log_TIER_RATIO(shard docs)`.
const TIER_RATIO: u64 = 4;

/// Max bounded passes one [`compact`](Shard::compact) call runs before returning. Each
/// pass merges one tier group under the lock, then releases it; the cap keeps a single call from
/// monopolizing the shard when a huge backlog needs many passes — the next poll continues.
const MAX_COMPACTION_PASSES: usize = 32;

/// The bounded group of segments to merge this pass — the **smallest size tier that has ≥2
/// segments**, up to `merge_factor` of them. Cheapest merges first; each group is
/// same-tier so the merge output is bounded to one tier's size, never the whole shard. Returns
/// `< 2` ids when nothing should merge (each segment is a lone size → already tiered). Pure, so the
/// tiering is unit-tested without a live index.
fn select_tiered_merge(
    segs: &[(tantivy::index::SegmentId, u64)],
    merge_factor: usize,
) -> Vec<tantivy::index::SegmentId> {
    // Tier a segment by its live-doc magnitude in base TIER_RATIO — same-tier segments are ~same size.
    let tier = |docs: u64| -> u32 {
        let mut d = docs.max(1);
        let mut t = 0u32;
        while d >= TIER_RATIO {
            d /= TIER_RATIO;
            t += 1;
        }
        t
    };
    let mut by_tier: std::collections::BTreeMap<u32, Vec<tantivy::index::SegmentId>> =
        Default::default();
    for (id, docs) in segs {
        by_tier.entry(tier(*docs)).or_default().push(*id); // BTreeMap iterates smallest tier first
    }
    for (_t, mut ids) in by_tier {
        if ids.len() >= 2 {
            ids.truncate(merge_factor.max(2));
            return ids;
        }
    }
    Vec::new()
}

impl CompactionPolicy {
    /// Why this shard should be compacted now, or `None` if it's healthy enough to leave alone.
    /// Never asks to merge a single segment ([`Shard::compact`] is itself a no-op at ≤1).
    pub fn reason_to_compact(&self, health: &CompactionHealth) -> Option<String> {
        if health.segments <= 1 {
            return None;
        }
        if health.segments >= self.min_segments {
            return Some(format!(
                "{} segments ≥ {}",
                health.segments, self.min_segments
            ));
        }
        if health.deleted_ratio() >= self.max_deleted_ratio {
            return Some(format!(
                "{:.0}% deleted ≥ {:.0}%",
                health.deleted_ratio() * 100.0,
                self.max_deleted_ratio * 100.0
            ));
        }
        None
    }
}

/// Access-driven pre-warm policy: promote a **cold** (read-through) window back to a local
/// hot shard once it sees at least `min_accesses` reads within a sampling interval — a window getting
/// sustained traffic stops paying cold-tier latency. Pure of scheduling: a serving loop samples each
/// cold window's [`ShardHandle`](../../growlerdb_engine/struct.ShardHandle.html) access delta on a
/// timer and calls [`should_promote`](Self::should_promote). `min_accesses == 0` disables pre-warm.
#[derive(Debug, Clone, Copy)]
pub struct PreWarmPolicy {
    /// Promote once a cold window sees at least this many reads within one sampling interval.
    pub min_accesses: u64,
}

impl Default for PreWarmPolicy {
    fn default() -> Self {
        // A window that fields ≥16 reads in an interval is clearly hot-again — promote it.
        Self { min_accesses: 16 }
    }
}

impl PreWarmPolicy {
    /// Whether a cold window with `accesses_in_interval` reads this interval should be promoted.
    pub fn should_promote(&self, accesses_in_interval: u64) -> bool {
        self.min_accesses > 0 && accesses_in_interval >= self.min_accesses
    }
}

/// A **sealed segment** of a shard's index — the immutable unit of backup and
/// replica shipping. Returned by [`Shard::sealed_segments`]; its [`files`] are
/// relative to [`Shard::index_dir`] and content-stable once sealed, so backups dedupe
/// unchanged segments and replicas open the bytes without re-indexing.
///
/// [`files`]: Self::files
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedSegment {
    /// Stable segment id (Tantivy UUID string) — also the file-name stem of its files.
    pub id: String,
    /// Live (non-deleted) docs in the segment.
    pub num_docs: u32,
    /// Total docs ever written to the segment (live + deleted); `num_deleted_docs` of
    /// these are superseded/deleted and get purged on the next merge.
    pub max_doc: u32,
    /// Docs deleted/superseded since the segment was sealed (a high ratio is compaction
    /// pressure — a health signal for the Compactor).
    pub num_deleted_docs: u32,
    /// The segment's files, **relative to** [`Shard::index_dir`], sorted for stable
    /// manifests. Read bytes by joining each onto `index_dir`.
    pub files: Vec<PathBuf>,
}

/// A consistent on-disk snapshot of a shard, captured for backup: the snapshot/
/// checkpoint it reflects and the files written under the staging dir.
#[derive(Debug, Clone)]
pub struct BackupSnapshot {
    /// The committed index snapshot the backup reflects.
    pub snapshot: u64,
    /// The source checkpoint at that snapshot — a restored node resumes the tail from here.
    pub checkpoint: Option<SourceCheckpoint>,
    /// Files written under the staging dir, relative to it: segment files + `meta.json` under
    /// `index/`, plus `aux.redb`. (The index definition lives at the index root, not the shard,
    /// so it's carried by the backup orchestration layer, not here.)
    pub files: Vec<PathBuf>,
}

/// A single shard's store: its **one** Tantivy index (+ a live reader), the derived
/// schema and a small redb aux store (checkpoint / batch
/// ids / file interns).
pub struct Shard {
    index: tantivy::Index,
    /// The Tantivy index directory on disk — the root that [`SealedSegment::files`] paths
    /// are relative to (the backup/replica layer reads segment bytes from here).
    index_dir: PathBuf,
    /// The live reader (auto-reloads on commit); all non-PIT reads go through it.
    core: SegmentReader,
    schema: IndexSchema,
    /// The redb aux store (checkpoints + batch idempotency + zone-map). Behind an `Arc` because redb
    /// permits only **one** open handle per file per process, yet an in-process hot↔cold tier swap
    /// (park / pre-warm) has both the retiring and the arriving shard live at once — they **share**
    /// this one handle across the swap (`aux.redb` stays local and unchanged through it) rather than
    /// racing a second `Database::open`.
    db: Arc<Database>,
    /// The **single, long-lived** Tantivy writer, behind a `Mutex` (Tantivy allows one
    /// writer; the lock also serializes commits). Created with [`NoMergePolicy`] once at
    /// open so segments accumulate per commit and only [`compact`](Shard::compact) merges
    /// them — avoiding a per-writer-creation race with the default merge policy. **`None` for a
    /// read-only cold shard** ([`open_cold_shard`](LocalIndexStore::open_cold_shard)): its
    /// tantivy index is served read-through from object storage, which can't be written.
    writer: Option<Mutex<tantivy::IndexWriter>>,
    /// Open **point-in-time** handles: each holds a pinned Tantivy
    /// [`SegmentReader`] snapshot (its segment ref-counting keeps the files alive
    /// through compaction) + a redb read txn (as-of-`S` snapshot). Bounded by
    /// [TTL expiry](Shard::expire_pits) + [`MAX_OPEN_PITS`].
    pits: Mutex<HashMap<u64, PitEntry>>,
    /// Monotonic source of opaque PIT ids.
    next_pit: AtomicU64,
    /// Max docs per Tantivy commit inside [`commit_staged`](Shard::commit_staged). Set from
    /// [`commit_chunk_docs`] (env) at open; tests set it directly. `0` ⇒ commit the whole batch at once.
    commit_chunk: usize,
    /// Test-only recorder of the commit's durability ordering (Tantivy commit → redb txn)
    /// so the crash contract is asserted, not just documented.
    #[cfg(test)]
    commit_trace: Mutex<Vec<&'static str>>,
}

/// A live point-in-time handle: the pinned Tantivy snapshot (shared so concurrent PIT
/// reads don't serialize) + the redb view (as-of-`S` snapshot), and the
/// last-used time for [idle TTL expiry](Shard::expire_pits).
struct PitEntry {
    core: Arc<SegmentReader>,
    view: Arc<ReadView>,
    last_used: Instant,
}

/// A point-in-time handle returned by [`open_pit`](Shard::open_pit): its opaque `id`
/// (echo it back to read against the snapshot) and the `snapshot` it pinned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Pit {
    /// Opaque id; pass to PIT-scoped reads / [`close_pit`](Shard::close_pit).
    pub id: u64,
    /// The monotonic snapshot the PIT observes.
    pub snapshot: u64,
}

/// A consistent **redb** read snapshot (one MVCC `ReadTransaction`) for the aux state
/// a read needs: the [`snapshot`](ReadView::snapshot) id. A PIT holds one open so its
/// snapshot is as-of-`S`.
pub struct ReadView {
    txn: ReadTransaction,
}

impl ReadView {
    /// The monotonic snapshot this view observes (0 before the first commit).
    fn snapshot(&self) -> Result<u64> {
        let meta = match self.txn.open_table(META) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(0),
            Err(e) => return Err(e.into()),
        };
        Ok(meta
            .get(META_SNAPSHOT)?
            .and_then(|v| v.value().try_into().ok().map(u64::from_le_bytes))
            .unwrap_or(0))
    }
}

/// Staged-but-uncommitted work from [`stage`](Shard::stage_batch): the effective
/// per-key upserts (document) and deleted keys that
/// [`commit`](Shard::commit_staged) applies to the Tantivy index. `already_applied`
/// marks a batch whose `batch_id` was already committed (an idempotent no-op).
pub struct StagedRef {
    checkpoint: SourceCheckpoint,
    /// The window's resume point — drives the continuity decision, re-taken at commit.
    /// `None` = a bootstrap batch (from the start of the changelog).
    from_checkpoint: Option<SourceCheckpoint>,
    batch_id: String,
    /// `(enc(key), document)` for each upserted doc.
    upserts: Vec<(Vec<u8>, Document)>,
    /// `enc(key)` of deleted docs.
    deletes: Vec<Vec<u8>>,
    already_applied: bool,
    /// The connector's resume floor for this batch's trigger: the idempotency
    /// records for batches at/below it can never be re-sent, so the commit prunes them.
    /// `None` = no floor supplied (prune nothing). Same value across a trigger's sub-batches.
    safe_checkpoint: Option<SourceCheckpoint>,
}

impl StagedRef {
    /// Whether this staged batch carries document work (as opposed to a pure
    /// checkpoint advance).
    fn has_content(&self) -> bool {
        !self.upserts.is_empty() || !self.deletes.is_empty()
    }
}

/// The continuity decision for one batch against a shard's committed checkpoint
/// ([`Shard::continuity`] — the window-covering relaxation of the exact-match guard).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Continuity {
    /// The window strictly extends this shard: apply fully and advance. Rows at/behind the
    /// current checkpoint are byte-identical re-applies of committed ops (idempotent
    /// delete-then-add, LWW within the batch), so covering overlap is content-safe.
    Apply,
    /// The window ends at/behind this shard: an idempotent replay — nothing to apply,
    /// nothing to advance (in particular the checkpoint must NOT regress to the window end).
    NoOp,
    /// The window starts strictly ahead of this shard (or coverage can't be proven):
    /// applying would advance the checkpoint over unapplied data — refuse.
    Gap,
}

impl Shard {
    /// Whether this shard is a **read-only cold read-through** shard (served from object storage
    /// via [`open_cold_shard`](LocalIndexStore::open_cold_shard), no local writer). A hot shard
    /// has a writer; a cold one does not. Background writers (auto-compaction)
    /// key off this to stand down the moment a window is parked underneath a live handle.
    pub fn is_read_only(&self) -> bool {
        self.writer.is_none()
    }

    /// A clone of this shard's shared `aux.redb` handle, to hand to the shard arriving in an
    /// in-process tier swap ([`open_cold_shard`](LocalIndexStore::open_cold_shard) /
    /// [`open_shard_reusing_db`](LocalIndexStore::open_shard_reusing_db)) so the two never race a
    /// second `Database::open` on the same file during the overlap.
    pub fn db_handle(&self) -> Arc<Database> {
        self.db.clone()
    }

    /// The **checkpoint-continuity guard** (window-covering): decide what a batch whose window
    /// is `(from, end]` means for a shard sitting at `current`.
    ///
    /// With lineage order available ([`SourceCheckpoint::lineage_cmp`], Iceberg sequence
    /// numbers): `end ≤ current` ⇒ [`NoOp`](Continuity::NoOp) (idempotent replay — never
    /// regress); `from ≤ current < end` ⇒ [`Apply`](Continuity::Apply) (the window covers the
    /// shard's position; the overlap re-applies committed ops byte-identically, content-safe);
    /// `from > current` ⇒ [`Gap`](Continuity::Gap) — advancing would overwrite the checkpoint
    /// forward over unapplied data, the structural silent-loss window this guard closes. A
    /// bootstrap batch (`from = None`) covers from the start of the changelog, i.e. `from =
    /// -∞` — which also closes the old unconditional bootstrap bypass: a stale bootstrap
    /// window now no-ops instead of regressing the checkpoint.
    ///
    /// Without order on the pair (a legacy checkpoint with no sequence number —
    /// snapshot ids are random longs and must not be compared numerically): fall back to
    /// exact semantics — same position ⇒ `NoOp`, `from` at the shard's position ⇒
    /// `Apply`, else `Gap`; `from = None` keeps its legacy exemption.
    fn continuity(
        current: Option<&SourceCheckpoint>,
        from: Option<&SourceCheckpoint>,
        end: &SourceCheckpoint,
    ) -> Continuity {
        use std::cmp::Ordering;
        let Some(current) = current else {
            // A shard with no committed checkpoint accepts any window (first write wins).
            return Continuity::Apply;
        };
        match end.lineage_cmp(current) {
            // Ends exactly where the shard is. A windowed batch (`from` present) is the
            // idempotent re-send of the window that produced `current` — nothing to do. A
            // bootstrap batch is different: bulk build / reindex / reconcile commit MANY
            // chunks at one fixed checkpoint with no `from`, each carrying distinct
            // content — they must all apply.
            Some(Ordering::Equal) => match from {
                None => Continuity::Apply,
                Some(_) => Continuity::NoOp,
            },
            // Ends strictly behind (by sequence number): a stale replay — never apply, never
            // regress (this also catches a stale `from = None` bootstrap that would rewind).
            Some(Ordering::Less) => Continuity::NoOp,
            Some(Ordering::Greater) => match from {
                None => Continuity::Apply, // covers from the start of the changelog
                Some(f) => match f.lineage_cmp(current) {
                    Some(Ordering::Less | Ordering::Equal) => Continuity::Apply,
                    // `from` ahead of the shard, or incomparable (coverage unprovable).
                    Some(Ordering::Greater) | None => Continuity::Gap,
                },
            },
            // End incomparable with current: legacy exact-match semantics.
            None => match from {
                None => Continuity::Apply,
                Some(f) if f.same_position(current) => Continuity::Apply,
                Some(_) => Continuity::Gap,
            },
        }
    }

    /// **Stage** a batch: reduce its ops to last-write-wins per key and collect the
    /// effective upserts (documents) and deleted keys. No Tantivy write yet —
    /// [`commit`](Self::commit_staged) applies it. An already-applied batch (by
    /// `batch_id`) stages as a no-op.
    ///
    /// The continuity guard runs here **advisorily** (fail fast, before any staging work)
    /// and again **authoritatively** in [`commit_staged`](Self::commit_staged) under the
    /// writer mutex — this read of the current checkpoint is lock-free, so a concurrent
    /// writer could move it between stage and commit.
    pub fn stage_batch(&self, batch: &CommitBatch) -> Result<StagedRef> {
        let already_applied = self.batch_snapshot(&batch.batch_id)?.is_some();
        if already_applied {
            // Idempotent replay: the connector re-sent a batch this shard already committed —
            // benign, but a rising rate is the "retry storm / boundary re-read" signal.
            // Counted here where it's detected.
            metrics::counter!("growlerdb_dedup_hits_total").increment(1);
        }

        let mut stage_noop = false;
        if !already_applied {
            match Self::continuity(
                self.current_checkpoint()?.as_ref(),
                batch.from_checkpoint.as_ref(),
                &batch.checkpoint,
            ) {
                Continuity::Apply => {}
                Continuity::NoOp => stage_noop = true, // stage no content; commit re-decides
                Continuity::Gap => {
                    // A rejected gap is a correctness event — count it so an alert can fire on
                    // any nonzero rate, not just find it in (rotatable) logs.
                    metrics::counter!("growlerdb_checkpoint_gap_total").increment(1);
                    return Err(StoreError::CheckpointGap {
                        from: format!("{:?}", batch.from_checkpoint),
                        current: format!("{:?}", self.current_checkpoint()?),
                    });
                }
            }
        }

        // Last-write-wins per key within the batch (changelog order).
        let mut effective: HashMap<Vec<u8>, DocOp> = HashMap::new();
        for op in &batch.ops {
            let enc = match op {
                DocOp::Upsert(d) => d.doc.key.encode(),
                DocOp::Delete(k) => k.encode(),
            };
            effective.insert(enc, op.clone());
        }

        let mut upserts: Vec<(Vec<u8>, Document)> = Vec::new();
        let mut deletes: Vec<Vec<u8>> = Vec::new();
        // A stage-time NoOp (window ends at/behind this shard) stages no content: the
        // checkpoint only advances, so commit's authoritative re-decision can't flip a
        // NoOp back to Apply — it would need the current checkpoint to regress.
        if !already_applied && !stage_noop {
            for (enc, op) in effective {
                match op {
                    DocOp::Upsert(d) => upserts.push((enc, d.doc.clone())),
                    DocOp::Delete(_) => deletes.push(enc),
                }
            }
        }

        Ok(StagedRef {
            checkpoint: batch.checkpoint.clone(),
            from_checkpoint: batch.from_checkpoint.clone(),
            batch_id: batch.batch_id.clone(),
            upserts,
            deletes,
            already_applied,
            safe_checkpoint: batch.safe_checkpoint.clone(),
        })
    }

    /// **Commit** staged work: apply native-delete upserts/deletes to the single Tantivy
    /// index and commit it (durable first), reload the live reader, then in one redb txn
    /// advance the checkpoint/snapshot and record the batch ids. A crash between the two
    /// commits re-applies the batch on resume — idempotent on the key (delete-then-add),
    /// so exactly-once holds.
    ///
    /// Hydration is store-less (PREDICATE): the commit stores no source-location data —
    /// only the Tantivy doc + key/checkpoint/batch bookkeeping — so this is the plain
    /// two-phase Tantivy-then-redb ordering.
    pub fn commit_staged(&self, staged: &[StagedRef]) -> Result<Snapshot> {
        let mut writer = self
            .writer
            .as_ref()
            .ok_or(StoreError::ReadOnlyShard("commit a batch to"))?
            .lock()
            .expect("writer not poisoned");
        // Authoritative continuity decision: the stage-time check ran lock-free, so a concurrent
        // writer may have advanced this shard between stage and commit. Re-deciding here against the
        // live checkpoint — under the writer mutex, before touching Tantivy — turns the loser of the
        // race into a NoOp or a loud Gap, never a silent regression.
        let mut position = self.current_checkpoint()?;
        let mut apply: Vec<&StagedRef> = Vec::new();
        for s in staged {
            if s.already_applied {
                continue; // recorded batch id — idempotent replay, no re-apply, no advance
            }
            match Self::continuity(position.as_ref(), s.from_checkpoint.as_ref(), &s.checkpoint) {
                Continuity::Apply => {
                    position = Some(s.checkpoint.clone());
                    apply.push(s);
                }
                Continuity::NoOp => {
                    // Ends at/behind the live checkpoint (e.g. staged before a racing commit
                    // landed): drop it — its content is already covered.
                    metrics::counter!("growlerdb_checkpoint_noop_total").increment(1);
                }
                Continuity::Gap => {
                    metrics::counter!("growlerdb_checkpoint_gap_total").increment(1);
                    return Err(StoreError::CheckpointGap {
                        from: format!("{:?}", s.from_checkpoint),
                        current: format!("{position:?}"),
                    });
                }
            }
        }

        let live: Vec<&StagedRef> = apply.iter().copied().filter(|s| s.has_content()).collect();
        if live.is_empty() {
            // No document work — but an empty batch still advances the source checkpoint. A window
            // routing no rows here must not leave its checkpoint behind, or shards drift out of
            // lockstep (inflating the resume re-read and breaking the continuity guard). Redb-only:
            // record the batch ids for idempotent replay and move the checkpoint.
            let snapshot = self.current_snapshot()?;
            if let Some(last) = apply.last() {
                let txn = self.db.begin_write()?;
                {
                    let mut meta = txn.open_table(META)?;
                    meta.insert(
                        META_CHECKPOINT,
                        serde_json::to_vec(&last.checkpoint)?.as_slice(),
                    )?;
                }
                // Record the batch ids (idempotent replay) and prune any that fell at/below the
                // connector's resume floor.
                Self::record_and_prune_batches(&txn, &apply, snapshot)?;
                txn.commit()?;
                self.trace("redb_checkpoint_advance");
            }
            return Ok(Snapshot(snapshot));
        }

        // 1) Apply to Tantivy, then commit it (the durable point).
        let key_enc = self.schema.key_enc_field();
        // Per-phase write latency (apply / tantivy_commit / redb) so a high
        // `growlerdb_write_duration_seconds` is attributable to a phase.
        let phase_secs = |name: &'static str, secs: f64| {
            metrics::histogram!("growlerdb_write_phase_duration_seconds", "phase" => name)
                .record(secs);
        };
        // Bound each Tantivy commit to `chunk` docs (`0` = one commit). The redb checkpoint still
        // advances exactly once at the end, so the crash invariant holds: intermediate commits
        // leave the index ahead of the un-advanced checkpoint, and a crash before the redb txn
        // replays the whole batch idempotently.
        let chunk = self.commit_chunk;
        macro_rules! flush_chunk {
            () => {{
                let t = std::time::Instant::now();
                writer.commit().map_err(|e| StoreError::Segment(e.into()))?;
                self.trace("tantivy_commit");
                self.core.reload().map_err(StoreError::Segment)?;
                phase_secs("tantivy_commit", t.elapsed().as_secs_f64());
            }};
        }
        let mut t_apply = std::time::Instant::now();
        let mut apply_secs = 0f64;
        let mut docs_since_flush = 0usize;
        for s in &live {
            for enc in &s.deletes {
                writer.delete_term(Term::from_field_bytes(key_enc, enc));
            }
            for (enc, doc) in &s.upserts {
                // delete-then-add: the new doc out-opstamps the delete and survives,
                // while the prior committed version is removed.
                writer.delete_term(Term::from_field_bytes(key_enc, enc));
                let td = self.schema.to_tantivy(doc);
                writer
                    .add_document(td)
                    .map_err(|e| StoreError::Segment(e.into()))?;
                docs_since_flush += 1;
                if chunk != 0 && docs_since_flush >= chunk {
                    apply_secs += t_apply.elapsed().as_secs_f64();
                    flush_chunk!();
                    docs_since_flush = 0;
                    t_apply = std::time::Instant::now();
                }
            }
        }
        apply_secs += t_apply.elapsed().as_secs_f64();
        phase_secs("apply", apply_secs);
        // Final flush — the durable barrier before the checkpoint advance below. Committing an empty
        // writer (last upsert on a chunk boundary) is a harmless no-op fsync.
        flush_chunk!();

        // Build the per-segment ANN + completion sidecars over the just-committed segments.
        // Idempotent + skipped when the index has no such field; run after the final flush so the
        // reloaded searcher sees every new segment.
        self.build_ann_sidecars()?;
        self.build_completion_sidecars()?;

        // 2) redb: checkpoint + snapshot + batch ids.
        let t_redb = std::time::Instant::now();
        let snapshot = self.current_snapshot()? + 1;
        let txn = self.db.begin_write()?;
        {
            let mut meta = txn.open_table(META)?;
            // The furthest APPLIED end — `apply` may trail an empty (advance-only) batch
            // behind the last content batch; the checkpoint reflects it.
            meta.insert(
                META_CHECKPOINT,
                serde_json::to_vec(&apply.last().unwrap().checkpoint)?.as_slice(),
            )?;
            meta.insert(META_SNAPSHOT, snapshot.to_le_bytes().as_slice())?;
        }
        // Record each committed batch id (idempotent replay) and prune any now at/below the
        // connector's resume floor — those batches can never be re-sent.
        Self::record_and_prune_batches(&txn, &apply, snapshot)?;
        txn.commit()?;
        self.trace("redb_commit");
        phase_secs("redb", t_redb.elapsed().as_secs_f64());
        Ok(Snapshot(snapshot))
    }

    /// The comparable key the [`BATCH_CKPT`] prune index is ordered by — the checkpoint's
    /// **lineage-monotone Iceberg sequence number**. Snapshot ids are random longs, so a
    /// numeric range over them is meaningless (a snapshot-id key could prune
    /// the record inserted in the same txn). `None` (legacy sender / v1 table) ⇒ the record
    /// is not indexed and never pruned: bounded over-retention, the safe direction.
    fn checkpoint_key(cp: &SourceCheckpoint) -> Option<i64> {
        cp.sequence_number()
    }

    /// Record every committed `batch_id` in `BATCH_KEYS` (the idempotent-replay guard) and index it
    /// under its checkpoint in [`BATCH_CKPT`], then drop every record at or below the connector's
    /// resume floor. Runs inside the caller's redb write txn so the two tables never diverge; the
    /// caller must not hold `BATCH_KEYS`/`BATCH_CKPT` open when calling.
    ///
    /// **Soundness.** `safe_checkpoint` is the connector's resume floor (monotonic; it never resumes
    /// before it), so no batch with checkpoint `<= floor` — in **lineage order**, never the random
    /// snapshot id — can be re-sent. And the window-covering guard no-ops a replay by *position*, so
    /// a dropped record can't turn a replay into a spurious `CheckpointGap` — the records are an
    /// optimization, the guard is the correctness. `None` prunes nothing; the **max** floor is used
    /// across differing staged batches.
    fn record_and_prune_batches(
        txn: &redb::WriteTransaction,
        committed: &[&StagedRef],
        snapshot: u64,
    ) -> Result<()> {
        {
            let mut batches = txn.open_table(BATCH_KEYS)?;
            let mut ckpt = txn.open_multimap_table(BATCH_CKPT)?;
            for s in committed {
                batches.insert(s.batch_id.as_str(), snapshot)?;
                // Only sequence-numbered checkpoints are indexed for pruning: the key must
                // be lineage-ordered for the range delete to be sound.
                if let Some(seq) = Self::checkpoint_key(&s.checkpoint) {
                    ckpt.insert(seq, s.batch_id.as_str())?;
                }
            }
        }
        let floor = committed
            .iter()
            .filter_map(|s| s.safe_checkpoint.as_ref().and_then(Self::checkpoint_key))
            .max();
        if let Some(floor) = floor {
            Self::prune_batches_at_or_below(txn, floor)?;
        }
        Ok(())
    }

    /// Range-delete every `batch_id` whose checkpoint is `<= floor` from both the [`BATCH_CKPT`]
    /// prune index and `BATCH_KEYS`. `floor` is the connector's resume floor, so a pruned batch can
    /// never be re-sent. O(pruned), not O(table): usually 0 (the floor only advances as
    /// the laggiest shard catches up), and each prune touches only the newly-safe tail.
    fn prune_batches_at_or_below(txn: &redb::WriteTransaction, floor: i64) -> Result<()> {
        let mut ckpt = txn.open_multimap_table(BATCH_CKPT)?;
        // Collect first: the range iterator borrows `ckpt`, and we mutate both tables below.
        let mut stale: Vec<(i64, String)> = Vec::new();
        for entry in ckpt.range(..=floor)? {
            let (cp, ids) = entry?;
            let cp = cp.value();
            for id in ids {
                stale.push((cp, id?.value().to_string()));
            }
        }
        if stale.is_empty() {
            return Ok(());
        }
        let mut batches = txn.open_table(BATCH_KEYS)?;
        for (cp, id) in &stale {
            ckpt.remove(*cp, id.as_str())?;
            batches.remove(id.as_str())?;
        }
        Ok(())
    }

    /// **Partition-scoped reconciliation** — the equality-delete fallback
    /// for a non-key predicate ([equality deletes](../../../okf/product/functional/ingestion/index.md)).
    /// Given the keys a fresh source scan found **live** in `partition`, drop every
    /// indexed doc in that partition whose key is absent. Bounded to the partition
    /// and robust regardless of delete encoding (it never needs a pre-image).
    ///
    /// `expected_checkpoint` is the shard's checkpoint captured **before** the source scan that
    /// produced `live_keys`; the stale-delete only runs if the shard hasn't advanced since (the
    /// TOCTOU guard). `None` disables the guard for callers with no concurrent writer.
    ///
    /// Removing the keys from the index (native Tantivy delete) hides the docs
    /// immediately. An empty `partition` reconciles the whole index.
    pub fn reconcile_partition(
        &self,
        partition: &[(String, Value)],
        live_keys: &[CompositeKey],
        expected_checkpoint: Option<&SourceCheckpoint>,
    ) -> Result<ReconcileDelete> {
        let live: HashSet<Vec<u8>> = live_keys.iter().map(|k| k.encode()).collect();
        // All keys in `partition` share this byte prefix (encode is partition-first,
        // length-prefixed, so no cross-partition false matches); an empty partition
        // yields an empty prefix that matches every key.
        let prefix = CompositeKey::new(partition.to_vec(), Vec::new()).encode();

        // The indexed live-key set for the partition (D30: enumerated from the term
        // dictionary with per-term liveness — deleted-but-unmerged keys are excluded,
        // so the set equals the live keys exactly, even under delete debt).
        let stale: Vec<Vec<u8>> = self
            .core
            .live_keys_with_prefix(&prefix)
            .map_err(StoreError::Segment)?
            .into_iter()
            .filter(|k| !live.contains(k))
            .collect();
        if stale.is_empty() {
            return Ok(ReconcileDelete::none());
        }

        let mut writer = self
            .writer
            .as_ref()
            .ok_or(StoreError::ReadOnlyShard("reconcile-delete on"))?
            .lock()
            .expect("writer not poisoned");

        // TOCTOU guard: `live_keys` came from a source snapshot read *before* this lock. A key a
        // concurrent ingest added during that read is live in the index but absent from `live_keys`,
        // so it looks "stale" and we'd delete a legitimately newer row. The writer lock serializes
        // commits, so with it held `current_checkpoint == expected` proves NO commit happened across
        // the scan — safe to delete. A mismatch means the shard advanced: skip the deletes this cycle
        // (missing-repair still runs). `None` opts out (no concurrent writer — CLI/tests).
        if let Some(expected) = expected_checkpoint {
            if self.current_checkpoint()?.as_ref() != Some(expected) {
                return Ok(ReconcileDelete::skipped());
            }
        }

        let key_enc = self.schema.key_enc_field();
        for k in &stale {
            writer.delete_term(Term::from_field_bytes(key_enc, k));
        }
        writer.commit().map_err(|e| StoreError::Segment(e.into()))?;
        self.core.reload().map_err(StoreError::Segment)?;
        Ok(ReconcileDelete {
            deleted: stale.len(),
            skipped_concurrent_write: false,
        })
    }

    /// The number of **live** indexed keys, optionally scoped to a `partition` prefix — the cheap
    /// half of a drift check.
    ///
    /// D30: counted from the `_keyenc` term dictionary over the partition's raw-bytes prefix range,
    /// with a per-term liveness probe. Prefix enumeration (rather than a partition-field `Count`)
    /// preserves partition scoping exactly whether or not the partition fields are indexed, and the
    /// empty prefix ("whole shard") falls out naturally. O(keys in partition).
    pub fn key_count(&self, partition: &[(String, Value)]) -> Result<usize> {
        let prefix = CompositeKey::new(partition.to_vec(), Vec::new()).encode();
        Ok(self
            .core
            .live_keys_with_prefix(&prefix)
            .map_err(StoreError::Segment)?
            .len())
    }

    /// Whether `key` currently has a **live** indexed doc — the presence half of a
    /// drift check. A term probe filtered by the alive bitset, so a
    /// deleted-but-unmerged doc never reads as present.
    pub fn contains_key(&self, key: &CompositeKey) -> Result<bool> {
        self.core
            .live_key_exists(&key.encode())
            .map_err(StoreError::Segment)
    }

    /// Open a fresh, consistent [`ReadView`] of the shard metadata — one redb read
    /// transaction (MVCC snapshot). All read paths route through a view so a single
    /// search observes one consistent snapshot; a PIT holds one open.
    pub fn read_view(&self) -> Result<ReadView> {
        Ok(ReadView {
            txn: self.db.begin_read()?,
        })
    }

    /// Open a **point-in-time** handle: capture the current [`ReadView`] and
    /// hold it so every read against the returned [`Pit`] observes that one snapshot —
    /// unchanged by later commits/updates/deletes — until [`close_pit`](Self::close_pit)
    /// (or [TTL expiry](Self::expire_pits)). The pin is **Tantivy segment ref-counting**:
    /// the held [`SegmentReader`] snapshot keeps its segment files alive through later
    /// commits and [`compact`](Self::compact), so the as-of-`S` view never tears.
    pub fn open_pit(&self) -> Result<Pit> {
        let core = SegmentReader::snapshot(&self.index, self.index_dir.clone())
            .map_err(StoreError::Segment)?;
        let view = self.read_view()?;
        let snapshot = view.snapshot()?;
        let entry = PitEntry {
            core: Arc::new(core),
            view: Arc::new(view),
            last_used: Instant::now(),
        };
        let mut pits = self.pits.lock().expect("pit registry not poisoned");
        if pits.len() >= MAX_OPEN_PITS {
            return Err(StoreError::TooManyPits(MAX_OPEN_PITS));
        }
        let id = self.next_pit.fetch_add(1, Ordering::Relaxed);
        pits.insert(id, entry);
        Ok(Pit { id, snapshot })
    }

    /// Release a PIT handle, dropping its pinned snapshot (Tantivy can then reclaim
    /// any segments held only for it). Returns whether a handle was open. Idempotent.
    pub fn close_pit(&self, id: u64) -> bool {
        self.pits
            .lock()
            .expect("pit registry not poisoned")
            .remove(&id)
            .is_some()
    }

    /// The number of currently-open PIT handles.
    pub fn open_pit_count(&self) -> usize {
        self.pits.lock().expect("pit registry not poisoned").len()
    }

    /// Evict PITs idle (not read) for longer than `ttl`, dropping their pinned
    /// snapshots. Returns the number evicted — the bound on retained-segment growth.
    pub fn expire_pits(&self, ttl: Duration) -> usize {
        let now = Instant::now();
        let mut pits = self.pits.lock().expect("pit registry not poisoned");
        let before = pits.len();
        pits.retain(|_, e| now.duration_since(e.last_used) < ttl);
        before - pits.len()
    }

    /// Clone out a PIT's pinned reader + view (touching its last-used time), or error if
    /// the handle is unknown/expired. The `Arc` clones let the read run without holding
    /// the registry lock, so concurrent PIT reads don't serialize.
    fn pit(&self, id: u64) -> Result<(Arc<SegmentReader>, Arc<ReadView>)> {
        let mut pits = self.pits.lock().expect("pit registry not poisoned");
        let entry = pits.get_mut(&id).ok_or(StoreError::UnknownPit(id))?;
        entry.last_used = Instant::now();
        Ok((entry.core.clone(), entry.view.clone()))
    }

    /// [`search_page`](Self::search_page) against an open **PIT** — the same paging over
    /// the PIT's pinned snapshot, so a scroll/export sees a stable result set even under
    /// concurrent writes. Errors if the handle is unknown/expired.
    pub fn search_page_pit(
        &self,
        pit: u64,
        query: &Query,
        k: usize,
        sort: &[Sort],
        offset: usize,
        after: Option<&SearchAfter>,
    ) -> Result<(Vec<Hit>, Option<SearchAfter>)> {
        let (core, _) = self.pit(pit)?;
        self.page_in(&core, query, k, sort, offset, after)
    }

    /// The number of Tantivy **segments** in the index (compaction pressure / stats).
    pub fn segment_count(&self) -> Result<u64> {
        Ok(self
            .index
            .searchable_segment_ids()
            .map_err(IndexError::from)?
            .len() as u64)
    }

    /// **Compact** the shard: fuse all current segments into one via Tantivy's
    /// `merge`, which **physically purges** superseded/deleted docs and improves both
    /// matching and cache locality. Atomic and **non-disruptive to readers** — in-flight
    /// searches (and open PITs) keep reading the segments they opened; the merge is visible
    /// only to new searches, and Tantivy keeps the old segment files until those readers
    /// release them (so PITs are safe by construction). A no-op when ≤1 segment.
    pub fn compact(&self, policy: &CompactionPolicy) -> Result<()> {
        // Bounded, lock-releasing compaction: each pass merges one **size tier** (up to
        // `merge_factor` similar-sized segments) under the writer lock, then releases it before the
        // next — so a single lock-hold is O(a tier), never O(shard), and ingest interleaves between
        // passes. Repeats up to `MAX_COMPACTION_PASSES`; the poll re-runs to drain any remainder.
        for _ in 0..MAX_COMPACTION_PASSES {
            let mut writer = self
                .writer
                .as_ref()
                .ok_or(StoreError::ReadOnlyShard("compact"))?
                .lock()
                .expect("writer not poisoned");
            let metas = self
                .index
                .searchable_segment_metas()
                .map_err(IndexError::from)?;
            let segs: Vec<(tantivy::index::SegmentId, u64)> = metas
                .iter()
                .map(|m| {
                    let live = u64::from(m.max_doc().saturating_sub(m.num_deleted_docs()));
                    (m.id(), live)
                })
                .collect();
            let group = select_tiered_merge(&segs, policy.merge_factor);
            if group.len() < 2 {
                return Ok(()); // nothing left to merge under the tiering
            }
            writer
                .merge(&group)
                .wait()
                .map_err(|e| StoreError::Segment(e.into()))?;
            // Reclaim merged-away segment files not held by any live reader/PIT.
            writer
                .garbage_collect_files()
                .wait()
                .map_err(|e| StoreError::Segment(e.into()))?;
            drop(writer); // release the lock so ingest can commit before the next bounded pass
            self.core.reload().map_err(StoreError::Segment)?;
            // A merge produced a new segment id → build its ANN + completion sidecars. The
            // merged-away segments' orphaned sidecars may linger (harmless — `sealed_segments` lists
            // only current segments, so backup never carries an orphan).
            self.build_ann_sidecars()?;
            self.build_completion_sidecars()?;
        }
        Ok(())
    }

    /// Current [`CompactionHealth`] signals — segment count + delete debt, summed from the
    /// committed segment metadata. A serving loop reads this on a timer and consults a
    /// [`CompactionPolicy`] to decide whether to [`compact`](Self::compact).
    pub fn compaction_health(&self) -> Result<CompactionHealth> {
        let segs = self.sealed_segments()?;
        Ok(CompactionHealth {
            segments: segs.len() as u64,
            max_doc: segs.iter().map(|s| s.max_doc as u64).sum(),
            deleted: segs.iter().map(|s| s.num_deleted_docs as u64).sum(),
        })
    }

    /// The tenant-scoping field, if this shard's index is tenant-scoped. Reads inject a mandatory
    /// `tenant_field = <verified claim>` filter; `None` = not scoped.
    pub fn tenant_field(&self) -> Option<&str> {
        self.schema.tenant_field()
    }

    /// The [`VectorSpec`](growlerdb_core::VectorSpec) of the VECTOR field named `field`, or `None`
    /// if `field` is not a VECTOR field on this shard's index. The node's semantic-search handler
    /// reads this to embed a query with the field's configured embedder (the same space its
    /// documents were embedded in at ingest).
    pub fn vector_spec(&self, field: &str) -> Option<&growlerdb_core::VectorSpec> {
        self.schema.vector_spec(field)
    }

    /// Whether this shard's index maps an Iceberg v3 **variant** column (D47) — the stats
    /// pollers use this to skip a variant table, whose metadata released
    /// iceberg-rust can't parse (D49).
    pub fn has_variant_fields(&self) -> bool {
        self.schema.has_variant_fields()
    }

    /// The mapped DATE field names — the time columns a console time filter can range a
    /// query on; a query ranging the windowing field additionally prunes windows.
    pub fn date_fields(&self) -> Vec<String> {
        self.schema
            .date_fields()
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    /// Read each key's live-doc **fast values** for `fields` — the sort-key **prune hints** the
    /// hydration fallback AND-s onto its pass-2 key predicate so a sorted source table prunes files
    /// by manifest min/max (the heal for an otherwise-unprunable random key). Reuses the identity
    /// layer's key→doc resolution; a key with no live doc, or a non-fast field, contributes nothing
    /// (never an error). Returns a vec aligned 1:1 with `keys`. See [`SegmentReader::fast_values`].
    pub fn prune_values(
        &self,
        keys: &[CompositeKey],
        fields: &[String],
    ) -> Result<Vec<Vec<(String, growlerdb_core::Value)>>> {
        self.core
            .fast_values(keys, fields)
            .map_err(StoreError::Segment)
    }

    /// The field names a query can sort by — numeric/date/keyword fields declared `fast`. The
    /// console lists exactly these so it never offers a non-sortable field (see
    /// `IndexSchema::sort_fields`).
    pub fn sort_fields(&self) -> Vec<String> {
        self.schema
            .sort_fields()
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    /// The index's VECTOR fields — each a [`VectorFieldSummary`](crate::VectorFieldSummary) of the
    /// field path plus its embedding config. The describe path surfaces these so a console can
    /// offer a semantic/hybrid vector-field picker. Empty for a non-vector index.
    pub fn vector_fields(&self) -> Vec<crate::VectorFieldSummary> {
        self.schema.vector_fields()
    }

    /// Per-VECTOR-field **KNN coverage**: how many vectors the live segments' ANN sidecars hold
    /// for `field` (see [`SegmentReader::vector_coverage`]). The describe path pairs this with
    /// `num_docs` so a partially-embedded index — documents ingested without an embedding — is
    /// observable instead of silently unsearchable.
    pub fn vector_coverage(&self, field: &str) -> Result<u64> {
        self.core
            .vector_coverage(field)
            .map_err(StoreError::Segment)
    }

    /// Every mapped field's describe-facing summary (name, type, `fast`/`indexed`/`cached`),
    /// in definition order — see [`IndexSchema::mapped_fields`](crate::MappedFieldSummary).
    pub fn mapped_fields(&self) -> Vec<crate::MappedFieldSummary> {
        self.schema.mapped_fields()
    }

    pub fn index_dir(&self) -> &Path {
        &self.index_dir
    }

    /// The shard's full on-disk index footprint in bytes — every component of
    /// [`index_size_breakdown`](Self::index_size_breakdown) summed, i.e. the Tantivy files **plus**
    /// the sibling `aux.redb`. Used by the serving loop to emit `growlerdb_index_bytes`; computed
    /// from the breakdown so the total gauge and the per-component gauge always reconcile.
    pub fn index_size_bytes(&self) -> u64 {
        self.index_size_breakdown().total()
    }

    /// On-disk index size broken into components, so the
    /// index:source ratio can be attributed to the structure that drives it — term dictionaries vs
    /// postings vs positions vs fieldnorms vs the **fast-field cache** vs the **doc store** —
    /// rather than a lump total. Tantivy files are classified by extension; the sibling `aux.redb`
    /// counts under `other`. Best-effort — unreadable entries count as 0.
    pub fn index_size_breakdown(&self) -> IndexSizeBreakdown {
        let mut b = IndexSizeBreakdown::default();
        if let Ok(entries) = std::fs::read_dir(&self.index_dir) {
            for entry in entries.flatten() {
                let Ok(meta) = entry.metadata() else { continue };
                if meta.is_dir() {
                    b.other += dir_size_bytes(&entry.path());
                    continue;
                }
                let len = meta.len();
                match entry.path().extension().and_then(|e| e.to_str()) {
                    Some("term") => b.term += len,            // term dictionaries
                    Some("idx") => b.postings += len,         // postings lists
                    Some("pos") => b.positions += len,        // token positions (phrase support)
                    Some("fieldnorm") => b.fieldnorms += len, // per-doc field lengths (BM25)
                    Some("fast") => b.fast += len, // columnar fast fields (sort/agg/range) — "fast cache"
                    Some("store") => b.store += len, // stored-document data
                    _ => b.other += len,           // meta.json, .managed.json, .del, lock, …
                }
            }
        }
        // The sibling `aux.redb` sits beside `index_dir`.
        if let Some(parent) = self.index_dir.parent() {
            if let Ok(meta) = std::fs::metadata(parent.join("aux.redb")) {
                b.other += meta.len();
            }
        }
        b
    }

    /// Total on-disk size of the shard, in bytes — a per-shard skew/health signal exposed via
    /// `DescribeIndex`. Same computation as [`index_size_bytes`](Self::index_size_bytes),
    /// so the API-reported size and the `growlerdb_index_bytes` gauge agree.
    /// Best-effort: unreadable entries count as 0.
    pub fn size_bytes(&self) -> u64 {
        self.index_size_bytes()
    }

    /// Enumerate the shard's **sealed segments** — the immutable committed segments, each with its
    /// on-disk files (relative to [`index_dir`]). The shipping/backup unit: files are content-stable
    /// (each name embeds the content/opstamp), so backup uploads incrementally (unchanged segments
    /// dedupe by name) and replicas open the bytes without re-indexing. Reflects the *current*
    /// committed view; call after a commit/[`compact`](Self::compact) for the post-merge set. The
    /// index-level `meta.json` is captured by the restore manifest separately.
    ///
    /// [`index_dir`]: Self::index_dir
    ///
    /// Build the per-segment **ANN sidecars** ([D19]) for the newly-sealed segments — one
    /// `<segment-uuid>.ann` per segment over its stored vectors. Idempotent and a no-op for a
    /// non-vector index. Must run with the live reader already reloaded onto the target commit.
    ///
    /// [D19]: ../../../okf/system/decisions/d19-ann-library.md
    fn build_ann_sidecars(&self) -> Result<()> {
        if !self.schema.has_vector_fields() {
            return Ok(());
        }
        self.core
            .build_ann_sidecars(&self.schema, &self.index_dir)
            .map_err(StoreError::Segment)
    }

    /// Build the per-segment completion sidecars (`<uuid>.cmp`) for the `suggest` fields. Idempotent
    /// and a no-op when no field opts in. Must run with the live reader reloaded onto the target
    /// commit, so it sees every new segment — same discipline as [`build_ann_sidecars`](Self::build_ann_sidecars).
    fn build_completion_sidecars(&self) -> Result<()> {
        if !self.schema.has_suggest_fields() {
            return Ok(());
        }
        self.core
            .build_completion_sidecars(&self.schema, &self.index_dir)
            .map_err(StoreError::Segment)
    }

    pub fn sealed_segments(&self) -> Result<Vec<SealedSegment>> {
        let segments = self.index.searchable_segments().map_err(IndexError::from)?;
        Ok(segments
            .iter()
            .map(|seg| {
                let meta = seg.meta();
                // `list_files` is a superset — one path per possible component, some of which never
                // hit disk. Filter to the bytes that exist so callers get a precise copy manifest.
                let mut files: Vec<PathBuf> = meta
                    .list_files()
                    .into_iter()
                    .filter(|f| self.index_dir.join(f).exists())
                    .collect();
                // The `.ann`/`.cmp` sidecars aren't listed by `list_files` but belong to this
                // segment — register them so backup/restore carries them (content-stable, dedupes
                // like the rest).
                for suffix in ["ann", "cmp"] {
                    let side = PathBuf::from(format!("{}.{suffix}", meta.id().uuid_string()));
                    if self.index_dir.join(&side).exists() {
                        files.push(side);
                    }
                }
                files.sort();
                SealedSegment {
                    id: meta.id().uuid_string(),
                    num_docs: meta.num_docs(),
                    max_doc: meta.max_doc(),
                    num_deleted_docs: meta.num_deleted_docs(),
                    files,
                }
            })
            .collect())
    }

    /// Snapshot the shard's committed state into `staging` for backup. Taken under the
    /// writer lock so no commit/compaction races: the live segment files, the `aux.redb` store,
    /// and (when present) the `index.json` definition. Segment files are **hard-linked** when
    /// `staging` shares the filesystem (instant, and the link keeps the bytes alive even if a
    /// later compaction unlinks the original) and copied otherwise. Returns the snapshot +
    /// checkpoint captured and the file list (relative to `staging`).
    pub fn backup_snapshot(&self, staging: &Path) -> Result<BackupSnapshot> {
        // Serialize against commits (and thus compaction) for a consistent file set + aux copy.
        let _guard = self
            .writer
            .as_ref()
            .ok_or(StoreError::ReadOnlyShard("backup"))?
            .lock()
            .expect("writer lock poisoned");
        let snapshot = self.current_snapshot()?;
        let checkpoint = self.current_checkpoint()?;

        let staging_index = staging.join("index");
        std::fs::create_dir_all(&staging_index)?;
        let mut files = Vec::new();
        for seg in self.sealed_segments()? {
            for rel in seg.files {
                let src = self.index_dir.join(&rel);
                let dst = staging_index.join(&rel);
                if let Some(parent) = dst.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                // Hard-link when possible (same fs); fall back to a byte copy across devices.
                if std::fs::hard_link(&src, &dst).is_err() {
                    std::fs::copy(&src, &dst)?;
                }
                files.push(Path::new("index").join(&rel));
            }
        }
        // Tantivy's index-level metadata lists the live segments — without `meta.json` a restored
        // index sees no segments at all. These are small + mutable (rewritten on commit), so copy
        // them (not hard-link); the writer lock guarantees they're quiescent here. `.managed.json`
        // tracks files for GC and is included when present.
        for name in ["meta.json", ".managed.json"] {
            let src = self.index_dir.join(name);
            if src.exists() {
                std::fs::copy(&src, staging_index.join(name))?;
                files.push(Path::new("index").join(name));
            }
        }

        // aux.redb sits beside `index/`; a byte copy under the writer lock = a consistent committed
        // view. (The index *definition* lives at the index root, carried by the orchestration layer.)
        let shard_dir = self.index_dir.parent().unwrap_or(&self.index_dir);
        std::fs::copy(shard_dir.join("aux.redb"), staging.join("aux.redb"))?;
        files.push(PathBuf::from("aux.redb"));
        Ok(BackupSnapshot {
            snapshot,
            checkpoint,
            files,
        })
    }

    /// Search returning the merged top-`k` by score.
    pub fn search_all(&self, query: &Query, k: usize) -> Result<Vec<Hit>> {
        self.search_paged(query, k, &[], 0)
    }

    /// **Explain** `query`'s score for the document with composite key `key`.
    pub fn explain(&self, query: &Query, key: &CompositeKey) -> Result<ExplainHit> {
        Ok(self.core.explain(query, &key.encode())?)
    }

    /// Search across all live generations with multi-key fast-field `sort` and
    /// `offset` (from/size paging), returning the merged page of `k` hits.
    ///
    /// **Merge-on-read:** a hit from generation `g` is dropped unless
    /// `key_to_doc[key] == g`, so superseded (updated) and deleted docs never surface.
    /// Each segment returns its top `offset+k`; the merged set is re-sorted by the
    /// **full sort tuple** (each key in priority order, then the composite key as a
    /// trailing tiebreaker so the order is total/deterministic) and the page is sliced.
    pub fn search_paged(
        &self,
        query: &Query,
        k: usize,
        sort: &[Sort],
        offset: usize,
    ) -> Result<Vec<Hit>> {
        Ok(self
            .page_with_values(&self.core, query, k, sort, offset, None)?
            .into_iter()
            .map(|(hit, _)| hit)
            .collect())
    }

    /// **Keyset paging** ([`search_after`](growlerdb_core::SearchParams::after)): the
    /// page of `k` hits strictly after `after` in the [total order](Sort) of `sort`,
    /// plus the cursor for the *next* page (the last hit of this one, or `None` when
    /// the page is empty — the end of the result set). `sort` must be non-empty.
    ///
    /// Unlike [`search_paged`](Self::search_paged)'s `offset`, this scans no skipped
    /// prefix: each generation's segment is asked only for its top-`k` docs *after the
    /// cursor* (the keyset predicate is pushed into the query), so paging stays O(k)
    /// however deep the cursor is. Merge-on-read liveness is applied as elsewhere.
    pub fn search_after(
        &self,
        query: &Query,
        k: usize,
        sort: &[Sort],
        after: Option<&SearchAfter>,
    ) -> Result<(Vec<Hit>, Option<SearchAfter>)> {
        if sort.is_empty() {
            return Err(StoreError::Segment(IndexError::QueryType(
                "search_after requires at least one sort key".into(),
            )));
        }
        let (hits, next) = self.search_page(query, k, sort, 0, after)?;
        Ok((hits, next))
    }

    /// The unified read path behind [`search_paged`](Self::search_paged) and
    /// [`search_after`](Self::search_after): returns the page of `k` hits **plus the
    /// cursor for the next page** (the last hit's sort values + key) when `sort` is
    /// non-empty, so a caller can hand the client an opaque token and switch to
    /// keyset deep-paging at any point. `after` (a keyset cursor) takes precedence
    /// over `offset`.
    pub fn search_page(
        &self,
        query: &Query,
        k: usize,
        sort: &[Sort],
        offset: usize,
        after: Option<&SearchAfter>,
    ) -> Result<(Vec<Hit>, Option<SearchAfter>)> {
        self.page_in(&self.core, query, k, sort, offset, after)
    }

    /// Like [`search_page`](Self::search_page) but keeps **each hit's sort values**
    /// (not just the cursor's): returns `(Vec<(Hit, Vec<SortValue>)>, next_cursor)`. The
    /// Engine API uses this to put `sort_values` on every wire hit so the Gateway can
    /// merge field-sorted pages across shards. Values are empty per hit when
    /// `sort` is empty (score ranking).
    pub fn search_page_values(
        &self,
        query: &Query,
        k: usize,
        sort: &[Sort],
        offset: usize,
        after: Option<&SearchAfter>,
        highlight: Option<&Highlight>,
    ) -> Result<ValuedPage> {
        self.page_in_values(&self.core, query, k, sort, offset, after, highlight)
    }

    /// [`search_page_values`](Self::search_page_values) against a PIT-frozen snapshot.
    #[allow(clippy::too_many_arguments)]
    pub fn search_page_pit_values(
        &self,
        pit: u64,
        query: &Query,
        k: usize,
        sort: &[Sort],
        offset: usize,
        after: Option<&SearchAfter>,
        highlight: Option<&Highlight>,
    ) -> Result<ValuedPage> {
        let (core, _) = self.pit(pit)?;
        self.page_in_values(&core, query, k, sort, offset, after, highlight)
    }

    /// Total documents matching `query` (live; the single index excludes superseded/deleted) —
    /// the search response's `total`, distinct from the returned page size. A Gateway sums these
    /// across shards for a true global match count.
    pub fn search_count(&self, query: &Query) -> Result<u64> {
        Ok(self.core.count(query)?)
    }

    /// [`search_count`](Self::search_count) against a PIT-frozen snapshot.
    pub fn search_count_pit(&self, pit: u64, query: &Query) -> Result<u64> {
        let (core, _) = self.pit(pit)?;
        Ok(core.count(query)?)
    }

    /// [`search_page`](Self::search_page) against a specific reader — the page of `k`
    /// hits plus the next-page cursor. Shared by the live path and the PIT-scoped path
    /// ([`search_page_pit`](Self::search_page_pit)).
    fn page_in(
        &self,
        core: &SegmentReader,
        query: &Query,
        k: usize,
        sort: &[Sort],
        offset: usize,
        after: Option<&SearchAfter>,
    ) -> Result<(Vec<Hit>, Option<SearchAfter>)> {
        let (page, next) = self.page_in_values(core, query, k, sort, offset, after, None)?;
        Ok((page.into_iter().map(|(hit, _)| hit).collect(), next))
    }

    /// The shared paging core that keeps per-hit sort values: the page as
    /// `(Hit, Vec<SortValue>)` plus the next-page cursor (the last hit's values + key,
    /// when `sort` is non-empty). [`page_in`](Self::page_in) strips the values for
    /// callers that don't need them; [`search_page_values`](Self::search_page_values)
    /// keeps them for the wire.
    #[allow(clippy::too_many_arguments)]
    fn page_in_values(
        &self,
        core: &SegmentReader,
        query: &Query,
        k: usize,
        sort: &[Sort],
        offset: usize,
        after: Option<&SearchAfter>,
        highlight: Option<&Highlight>,
    ) -> Result<ValuedPage> {
        let mut page = self.page_with_values(core, query, k, sort, offset, after)?;
        // Server-side highlighting: fill each hit's per-field fragments from the
        // analyzed match, against the same reader that produced the page. Off unless opted in.
        if let Some(hl) = highlight {
            let mut hits: Vec<Hit> = page.iter().map(|(h, _)| h.clone()).collect();
            core.highlight_hits(query, &mut hits, hl)?;
            for ((hit, _), filled) in page.iter_mut().zip(hits) {
                hit.highlight = filled.highlight;
            }
        }
        // No keyset cursor for an unsorted query, or for a `_score` sort (relevance isn't
        // a stable keyset key): those page by offset only.
        let next = if sort.is_empty() || sort_has_score(sort) {
            None
        } else {
            page.last().map(|(hit, sort_values)| SearchAfter {
                sort_values: sort_values.clone(),
                key: hit.key.clone(),
            })
        };
        Ok((page, next))
    }

    /// **Field collapsing** ([collapse](growlerdb_core::SearchParams)): reduce
    /// the result set to one entry per distinct value of `collapse` — the **top hit**
    /// of each group (by the `sort` order) plus the group's **member count** — and
    /// return the top-`k` groups (ordered by their top hit). `sort` must be non-empty.
    ///
    /// Every matching doc is scanned (grouping/counting need all members), merged
    /// across generations with merge-on-read liveness, ordered by the full sort tuple,
    /// then folded by group. Docs lacking the collapse field are skipped. `O(matches)`
    /// — search-support scope (D24); heavy grouping pushes to Trino/Spark.
    pub fn search_collapsed(
        &self,
        query: &Query,
        k: usize,
        sort: &[Sort],
        collapse: &str,
        highlight: Option<&Highlight>,
    ) -> Result<Vec<CollapsedHit>> {
        self.collapsed_in(&self.core, query, k, sort, collapse, highlight)
    }

    /// [`search_collapsed`](Self::search_collapsed) against an open **PIT** — collapse
    /// over the PIT's frozen snapshot. Errors if the handle is unknown/expired.
    pub fn search_collapsed_pit(
        &self,
        pit: u64,
        query: &Query,
        k: usize,
        sort: &[Sort],
        collapse: &str,
        highlight: Option<&Highlight>,
    ) -> Result<Vec<CollapsedHit>> {
        let (core, _) = self.pit(pit)?;
        self.collapsed_in(&core, query, k, sort, collapse, highlight)
    }

    /// Field collapsing against a specific reader — shared by the live and PIT paths.
    fn collapsed_in(
        &self,
        core: &SegmentReader,
        query: &Query,
        k: usize,
        sort: &[Sort],
        collapse: &str,
        highlight: Option<&Highlight>,
    ) -> Result<Vec<CollapsedHit>> {
        if sort.is_empty() {
            return Err(StoreError::Segment(IndexError::QueryType(
                "collapse requires at least one sort key".into(),
            )));
        }
        let mut entries: Vec<(Hit, Value, Vec<SortValue>)> =
            core.collapse_scan(query, sort, collapse)?;
        // Order by the full sort tuple so the first doc seen for a group is its top hit.
        entries.sort_by(|a, b| cmp_hits(&a.0, &a.2, &b.0, &b.2, sort));

        // Fold into groups in first-appearance order, counting members. The first entry of a group
        // is its top hit (entries are sorted above), so its sort values represent the group —
        // carried out so a Gateway can order groups across shards.
        let mut order: Vec<String> = Vec::new();
        let mut groups: HashMap<String, (Hit, Value, Vec<SortValue>, usize)> = HashMap::new();
        for (hit, group, sort_values) in entries {
            let gkey = group.to_index_string();
            match groups.get_mut(&gkey) {
                Some(g) => g.3 += 1,
                None => {
                    order.push(gkey.clone());
                    groups.insert(gkey, (hit, group, sort_values, 1));
                }
            }
        }
        let mut collapsed: Vec<CollapsedHit> = order
            .into_iter()
            .take(k)
            .map(|gk| {
                let (hit, group, sort_values, count) =
                    groups.remove(&gk).expect("group recorded above");
                CollapsedHit {
                    hit,
                    group,
                    count,
                    sort_values,
                }
            })
            .collect();
        // Server-side highlighting: fill each group's top hit against the same reader.
        if let Some(hl) = highlight {
            let mut hits: Vec<Hit> = collapsed.iter().map(|c| c.hit.clone()).collect();
            core.highlight_hits(query, &mut hits, hl)?;
            for (c, filled) in collapsed.iter_mut().zip(hits) {
                c.hit.highlight = filled.highlight;
            }
        }
        Ok(collapsed)
    }

    /// Gather the merged, liveness-filtered, fully-sorted page as `(hit, sort_values)`
    /// pairs. With a keyset `after`, each segment yields its top-`k` *after the cursor*
    /// and the page is the first `k`; otherwise each yields its top-`offset+k` and the
    /// page is `[offset, offset+k)`. Merge-on-read drops superseded/deleted docs.
    fn page_with_values(
        &self,
        core: &SegmentReader,
        query: &Query,
        k: usize,
        sort: &[Sort],
        offset: usize,
        after: Option<&SearchAfter>,
    ) -> Result<Vec<(Hit, Vec<SortValue>)>> {
        // Multi-key sort: the top-`k`-by-primary window is wrong when many docs tie on the primary,
        // so scan all matches and full-sort. Score / single-key sort: the windowed top-`k` is
        // correct and cheap. A `_score` key needs the exact scan EXCEPT a sole `_score desc` (its
        // window is `order_by_score`); `_score asc` can't come from the descending window, so it scans.
        let score_asc_primary =
            sort.len() == 1 && sort[0].is_score() && sort[0].order == SortOrder::Asc;
        let mut hits = if sort.len() > 1 || score_asc_primary {
            core.scan_sorted(query, sort, after)?
        } else {
            let want = if after.is_some() {
                k
            } else {
                k.saturating_add(offset)
            };
            core.search_sorted(query, want, sort, after)?
        };
        hits.sort_by(|a, b| cmp_hits(&a.0, &a.1, &b.0, &b.1, sort));
        let skip = if after.is_some() { 0 } else { offset };
        Ok(hits.into_iter().skip(skip).take(k).collect())
    }

    /// Top-`k` by score with **server-side highlights** filled per hit: each returned
    /// [`Hit`] carries `highlight` (field → matched fragments) for the requested highlightable TEXT
    /// fields, reflecting the analyzed match. A thin wrapper over [`search_page_values`](
    /// Self::search_page_values) with a highlight opt-in; used by tests and the highlight fast path.
    pub fn search_highlighted(&self, query: &Query, k: usize, hl: &Highlight) -> Result<Vec<Hit>> {
        let (page, _) = self.page_in_values(&self.core, query, k, &[], 0, None, Some(hl))?;
        Ok(page.into_iter().map(|(hit, _)| hit).collect())
    }

    /// **Prefix autocomplete**: the top-`limit` indexed terms of `field`
    /// starting with `prefix`, ordered by descending document frequency (ties broken
    /// by term, ascending). Merges the per-generation term dictionaries, summing
    /// frequencies for a term seen in several generations.
    ///
    /// Frequencies are **approximate** — not merge-on-read liveness-filtered (a term
    /// only in superseded docs may still surface), which is the accepted contract for a
    /// suggester hint. `field` must be an indexed string (TEXT/KEYWORD) field.
    pub fn suggest_prefix(
        &self,
        field: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, u64)>> {
        // Completion fast path: for a `suggest`-flagged field with a short prefix, answer from the
        // precomputed per-segment top-K table (no term-dict scan). `None` = the sidecar can't answer
        // (cold, over-`P`/empty prefix, or partial coverage) → the live seek below, unchanged.
        if self.schema.is_suggest_field(field) {
            if let Some(ranked) = self.core.suggest_prefix_sidecar(field, prefix, limit)? {
                return Ok(ranked);
            }
        }
        // Bounded multiple of the page so a broad prefix can't walk the whole vocabulary, while
        // leaving room to rank. `prefix_terms` already sums across segments.
        let scan_cap = limit.saturating_mul(64).max(1024);
        let mut totals: HashMap<String, u64> = HashMap::new();
        for (term, freq) in self.core.prefix_terms(field, prefix, scan_cap)? {
            *totals.entry(term).or_insert(0) += freq;
        }
        let mut ranked: Vec<(String, u64)> = totals.into_iter().collect();
        // Descending frequency, then ascending term for a deterministic order.
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        ranked.truncate(limit);
        Ok(ranked)
    }

    /// **Did-you-mean**: the top-`limit` indexed terms of `field` within edit
    /// distance `max_dist` of `term` (excluding `term` itself), ranked by **closeness**
    /// (edit distance ascending) then **frequency** (descending), ties broken by term.
    /// Merges the per-generation term dictionaries. Returns `(term, doc_freq)`.
    ///
    /// Frequencies are **approximate** (not liveness-filtered) — the suggester contract.
    /// `field` must be an indexed string (TEXT/KEYWORD) field; `max_dist` is typically
    /// 1–2. Cost is a bounded dictionary scan per generation (a Levenshtein automaton is
    /// a future optimization).
    pub fn suggest_fuzzy(
        &self,
        field: &str,
        term: &str,
        max_dist: u8,
        limit: usize,
    ) -> Result<Vec<(String, u64)>> {
        // A full-dictionary scan, capped to bound a pathologically large vocabulary.
        const SCAN_CAP: usize = 50_000;
        // term → (min edit distance, summed doc frequency); `fuzzy_terms` already sums
        // across the index's segments.
        let mut best: HashMap<String, (u8, u64)> = HashMap::new();
        for (cand, dist, freq) in self.core.fuzzy_terms(field, term, max_dist, SCAN_CAP)? {
            let e = best.entry(cand).or_insert((u8::MAX, 0));
            e.0 = e.0.min(dist);
            e.1 += freq;
        }
        let mut ranked: Vec<(String, u8, u64)> =
            best.into_iter().map(|(t, (d, f))| (t, d, f)).collect();
        // Closest first, then most frequent, then term for determinism.
        ranked.sort_by(|a, b| {
            a.1.cmp(&b.1)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked.truncate(limit);
        Ok(ranked.into_iter().map(|(t, _, f)| (t, f)).collect())
    }

    /// Run search-support **aggregations** over the docs matching `query`, returning
    /// each named result as JSON. Liveness-correct for free: the single
    /// index's searcher already excludes superseded/deleted docs (Tantivy deletes).
    pub fn aggregate(
        &self,
        query: &Query,
        aggs: &[(String, Agg)],
    ) -> Result<BTreeMap<String, serde_json::Value>> {
        let request = build_aggregations(aggs)?;
        // One pass over the single index (no tombstone exclusion needed — Tantivy already
        // drops deleted docs).
        let inter = self.core.aggregate_intermediate(query, &request)?;
        finalize_aggregations(inter, request)
    }

    /// Run `aggs` over the matched docs and return the **intermediate** (mergeable) result as
    /// opaque bytes — the per-shard partial the Gateway merges for a distributed aggregation
    /// ([`merge_aggregations`]). HLL/DDSketch sketches survive in this form, so a cross-shard
    /// merge unions them (approximate, not under-counted); finalized results could not be merged.
    /// Encoded with `postcard` (binary) because the sketches' non-string map keys aren't
    /// JSON-representable.
    pub fn aggregate_partial(&self, query: &Query, aggs: &[(String, Agg)]) -> Result<Vec<u8>> {
        let request = build_aggregations(aggs)?;
        let inter = self.core.aggregate_intermediate(query, &request)?;
        Ok(postcard::to_allocvec(&inter)?)
    }

    /// Total live documents (Tantivy excludes deleted/superseded docs).
    pub fn num_docs(&self) -> Result<u64> {
        Ok(self.core.num_docs())
    }

    /// The commit snapshot a `batch_id` was applied at, if any (idempotency guard).
    fn batch_snapshot(&self, batch_id: &str) -> Result<Option<u64>> {
        let txn = self.db.begin_read()?;
        let batches = match txn.open_table(BATCH_KEYS) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(batches.get(batch_id)?.map(|v| v.value()))
    }

    /// The source checkpoint the shard currently reflects, if any.
    pub fn current_checkpoint(&self) -> Result<Option<SourceCheckpoint>> {
        self.meta_bytes(META_CHECKPOINT)?
            .map(|b| serde_json::from_slice(&b).map_err(StoreError::from))
            .transpose()
    }

    /// The current monotonic snapshot (0 before the first commit).
    pub fn current_snapshot(&self) -> Result<u64> {
        self.read_view()?.snapshot()
    }

    /// The event-time zone-map `[min, max]` this (windowed) shard has seen, or `None`
    /// if it carries no event-time stats. The gateway prunes a window whose `[min, max]` can't
    /// overlap an event-time query filter.
    pub fn event_bounds(&self) -> Result<Option<(i64, i64)>> {
        match (
            self.meta_bytes(META_EVENT_MIN)?,
            self.meta_bytes(META_EVENT_MAX)?,
        ) {
            (Some(lo), Some(hi)) => Ok(Some((i64_le(&lo), i64_le(&hi)))),
            _ => Ok(None),
        }
    }

    /// **Widen** this shard's event-time zone-map to include `[min, max]`. A no-op when
    /// both bounds are `None` (e.g. a batch with no event-time values). Late events naturally
    /// widen the bound, which is what keeps them findable by event-time queries.
    pub fn set_event_bounds(&self, min: Option<i64>, max: Option<i64>) -> Result<()> {
        let (Some(min), Some(max)) = (min, max) else {
            return Ok(());
        };
        let (lo, hi) = match self.event_bounds()? {
            Some((l, h)) => (l.min(min), h.max(max)),
            None => (min, max),
        };
        let txn = self.db.begin_write()?;
        {
            let mut meta = txn.open_table(META)?;
            meta.insert(META_EVENT_MIN, lo.to_le_bytes().as_slice())?;
            meta.insert(META_EVENT_MAX, hi.to_le_bytes().as_slice())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// The source Iceberg `table-uuid` this index was built from, or `None` for an index
    /// built before lineage was recorded (so the guard simply can't check it — never a false alarm).
    pub fn source_uuid(&self) -> Result<Option<String>> {
        Ok(self
            .meta_bytes(META_SOURCE_UUID)?
            .map(|b| String::from_utf8_lossy(&b).into_owned()))
    }

    /// Anchor this index to its source's `table-uuid`, recorded at build/reindex so a
    /// later `serve` can detect a recreated source. Idempotent — re-recording the same uuid is fine.
    pub fn set_source_uuid(&self, uuid: &str) -> Result<()> {
        let txn = self.db.begin_write()?;
        {
            let mut meta = txn.open_table(META)?;
            meta.insert(META_SOURCE_UUID, uuid.as_bytes())?;
        }
        txn.commit()?;
        Ok(())
    }

    /// Record a commit-ordering event (test builds only) — see `commit_trace`.
    #[cfg(test)]
    fn trace(&self, event: &'static str) {
        self.commit_trace.lock().expect("trace lock").push(event);
    }

    #[cfg(not(test))]
    fn trace(&self, _event: &'static str) {}

    /// Drain the recorded commit-ordering events (test builds only).
    #[cfg(test)]
    fn take_commit_trace(&self) -> Vec<&'static str> {
        std::mem::take(&mut *self.commit_trace.lock().expect("trace lock"))
    }

    fn meta_bytes(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let txn = self.db.begin_read()?;
        let meta = match txn.open_table(META) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(e.into()),
        };
        Ok(meta.get(key)?.map(|v| v.value().to_vec()))
    }
}

// ---- Index API (Design 02) -------------------------------------------------
// The shard fulfils the in-process write/read seam; methods delegate to the
// inherent implementation above.

impl IndexWriter for Shard {
    type Error = StoreError;
    type Staged = StagedRef;

    fn stage(&self, batch: &CommitBatch) -> Result<StagedRef> {
        self.stage_batch(batch)
    }

    fn commit(&self, staged: &[StagedRef]) -> Result<Snapshot> {
        self.commit_staged(staged)
    }
}

impl IndexReader for Shard {
    type Error = StoreError;

    fn search(&self, params: &SearchParams) -> Result<ShardHits> {
        // A keyset cursor takes precedence over `offset` (O(k) deep paging). The
        // next-page cursor is available via the concrete [`Shard::search_after`];
        // the trait surface returns just the page. When a highlight opt-in is present,
        // page through the values path so each hit carries its fragments.
        let after = params.search_after.as_ref();
        let (page, _) = self.page_in_values(
            &self.core,
            &params.query,
            params.k,
            &params.sort,
            params.offset,
            after,
            params.highlight.as_ref(),
        )?;
        let hits: Vec<Hit> = page.into_iter().map(|(hit, _)| hit).collect();
        // `total` is the true match count (not the page size), consistent with the wire
        // `SearchResponse.total` and summable across shards.
        let total = self.search_count(&params.query)? as usize;
        Ok(ShardHits { hits, total })
    }

    fn snapshot(&self) -> Snapshot {
        // Infallible per Design 02; an unreadable aux store reads as snapshot 0.
        Snapshot(self.current_snapshot().unwrap_or(0))
    }
}

#[cfg(test)]
mod sort_tests {
    use super::*;
    use growlerdb_core::{
        IndexDefinition, LocatedDoc, Sort, SortOrder, SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;

    fn ranked_shard(tmp: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: rank, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let store = LocalIndexStore::open(tmp).unwrap();
        let shard = store.create_shard(&ShardId::single("docs"), &idx).unwrap();
        let mk = |id: &str, rank: i64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("rank".to_string(), Value::Int(rank));
            LocatedDoc {
                doc: Document::new(key, f),
            }
        };
        // Two commits → two generations, so the merge sort spans segments.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("a", 30), mk("b", 10)],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![mk("c", 20)], SourceCheckpoint::iceberg(2), "b2"),
        )
        .unwrap();
        shard
    }

    fn id_order(hits: Vec<Hit>) -> Vec<String> {
        hits.iter()
            .map(|h| h.key.get("id").unwrap().to_index_string())
            .collect()
    }

    #[test]
    fn prune_values_reads_each_key_live_fast_value_typed_and_aligned() {
        // The sort-key prune-hint read: resolve each key's live doc and read its `rank` fast field,
        // typed to the field's LONG mapping. A key with no live doc contributes an empty row; a
        // non-fast/unknown field is skipped — never an error (the predicate is a pure prune).
        let tmp = tempfile::tempdir().unwrap();
        let shard = ranked_shard(tmp.path());
        let k = |id: &str| CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let out = shard
            .prune_values(
                &[k("a"), k("absent"), k("c")],
                &["rank".to_string(), "id".to_string()],
            )
            .unwrap();
        assert_eq!(out.len(), 3, "aligned 1:1 with keys");
        assert_eq!(
            out[0],
            vec![("rank".to_string(), Value::Int(30))],
            "a → rank 30"
        );
        assert!(out[1].is_empty(), "no live doc → no hint, not an error");
        assert_eq!(
            out[2],
            vec![("rank".to_string(), Value::Int(20))],
            "c → rank 20"
        );
        // `id` is a KEYWORD identifier but not declared `fast` here, so it yields no hint (skipped).
        assert!(
            out[0].iter().all(|(n, _)| n != "id"),
            "non-fast field contributes nothing"
        );
    }

    // --- chunked commit -------------------------------------------------------------
    fn chunk_shard(tmp: &std::path::Path, chunk: usize) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::Long),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: LONG, fast: true }, { path: body, type: TEXT } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let store = LocalIndexStore::open(tmp).unwrap();
        let mut shard = store.create_shard(&ShardId::single("docs"), &idx).unwrap();
        shard.commit_chunk = chunk; // force intra-batch chunk commits
        shard
    }

    fn ck_doc(id: i64) -> LocatedDoc {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::Int(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::Int(id));
        f.insert("body".to_string(), Value::from("shared token"));
        LocatedDoc {
            doc: Document::new(key, f),
        }
    }

    #[test]
    fn commit_chunking_same_result_and_single_checkpoint_advance() {
        // 50 docs in ONE batch with chunk=7 → many internal Tantivy commits, but ONE checkpoint
        // advance; result identical to committing the whole batch at once (chunk=0).
        let tmp_c = tempfile::tempdir().unwrap();
        let chunked = chunk_shard(tmp_c.path(), 7);
        let tmp_w = tempfile::tempdir().unwrap();
        let whole = chunk_shard(tmp_w.path(), 0);

        let docs: Vec<LocatedDoc> = (0..50).map(ck_doc).collect();
        for s in [&chunked, &whole] {
            IndexWriter::write(
                s,
                &CommitBatch::from_upserts(docs.clone(), SourceCheckpoint::iceberg(100), "b1"),
            )
            .unwrap();
        }

        let q = Query::parse("body:shared").unwrap();
        let mut a = id_order(chunked.search_all(&q, 200).unwrap());
        let mut b = id_order(whole.search_all(&q, 200).unwrap());
        a.sort();
        b.sort();
        assert_eq!(a.len(), 50, "every chunk's docs are committed + searchable");
        assert_eq!(a, b, "chunked commit == whole commit");
        // One batch = one snapshot advance, regardless of the number of internal chunk commits.
        assert_eq!(chunked.current_snapshot().unwrap(), 1);
        assert_eq!(whole.current_snapshot().unwrap(), 1);
    }

    #[test]
    fn commit_chunking_replay_is_idempotent() {
        // Crash mid-batch leaves the index ahead of the un-advanced checkpoint; the connector
        // re-sends the whole batch. A re-apply at the same checkpoint (fresh batch-id, `from = None`
        // ⇒ Apply) must not duplicate — delete-then-add by key across chunks is idempotent.
        let tmp = tempfile::tempdir().unwrap();
        let shard = chunk_shard(tmp.path(), 7);
        let docs: Vec<LocatedDoc> = (0..50).map(ck_doc).collect();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs.clone(), SourceCheckpoint::iceberg(100), "b1"),
        )
        .unwrap();
        // Re-send the same content at the same checkpoint under a NEW batch id (bypasses the
        // batch-id short-circuit, exercising the chunked re-apply path).
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(100), "b1-replay"),
        )
        .unwrap();
        let hits = shard
            .search_all(&Query::parse("body:shared").unwrap(), 200)
            .unwrap();
        assert_eq!(
            hits.len(),
            50,
            "replay across chunks is idempotent — no duplicates"
        );
    }

    #[test]
    fn commit_chunking_cross_chunk_key_reuse() {
        // The SAME key upserted twice in one batch, split across a chunk boundary (chunk=3): the
        // second upsert (in a later chunk) must reuse/replace the first — one live doc, not two.
        let tmp = tempfile::tempdir().unwrap();
        let shard = chunk_shard(tmp.path(), 3);
        let mut docs: Vec<LocatedDoc> = (0..10).map(ck_doc).collect();
        docs.push(ck_doc(0)); // key 0 again, at a new row — lands in a later chunk
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(100), "b1"),
        )
        .unwrap();
        let hits = shard
            .search_all(&Query::parse("body:shared").unwrap(), 200)
            .unwrap();
        assert_eq!(
            hits.len(),
            10,
            "10 distinct keys — the re-upsert of key 0 replaced, not duplicated"
        );
    }

    fn sort(field: &str, order: SortOrder) -> Sort {
        Sort {
            field: field.into(),
            order,
        }
    }

    #[test]
    fn sorts_by_fast_field_across_generations_and_pages() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = ranked_shard(tmp.path());
        let all = Query::MatchAll;
        let asc = [sort("rank", SortOrder::Asc)];
        let desc = [sort("rank", SortOrder::Desc)];

        // Ascending by rank (10,20,30) and descending (30,20,10), across segments.
        assert_eq!(
            id_order(shard.search_paged(&all, 10, &asc, 0).unwrap()),
            vec!["b", "c", "a"]
        );
        assert_eq!(
            id_order(shard.search_paged(&all, 10, &desc, 0).unwrap()),
            vec!["a", "c", "b"]
        );
        // Offset paging: ascending, skip 1, take 1 → the middle rank (c=20).
        assert_eq!(
            id_order(shard.search_paged(&all, 1, &asc, 1).unwrap()),
            vec!["c"]
        );
        // Sorting on a non-fast field is a clear error.
        let id_sort = [sort("id", SortOrder::Asc)];
        assert!(shard.search_paged(&all, 10, &id_sort, 0).is_err());
    }

    #[test]
    fn fast_only_long_matches_indexed_long_and_is_smaller() {
        // Two shards over the same docs: `rank` fast-only (the default for a fast
        // field) vs `rank` fast **and** inverted-indexed (`indexed: true`). Every query shape
        // the engine routes at a numeric field must return identical results — Tantivy serves
        // range/exact/sort/exists from the columnar store when the field is fast — and the
        // fast-only shard's inverted index (term dicts + postings) must actually be smaller.
        let tmp_fast = tempfile::tempdir().unwrap();
        let fast_only = ranked_shard(tmp_fast.path()); // `rank` fast: true → columnar-only default

        let tmp_both = tempfile::tempdir().unwrap();
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: rank, type: LONG, fast: true, indexed: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let store = LocalIndexStore::open(tmp_both.path()).unwrap();
        let both = store.create_shard(&ShardId::single("docs"), &idx).unwrap();
        let mk = |id: &str, rank: i64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("rank".to_string(), Value::Int(rank));
            LocatedDoc {
                doc: Document::new(key, f),
            }
        };
        IndexWriter::write(
            &both,
            &CommitBatch::from_upserts(
                vec![mk("a", 30), mk("b", 10)],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        IndexWriter::write(
            &both,
            &CommitBatch::from_upserts(vec![mk("c", 20)], SourceCheckpoint::iceberg(2), "b2"),
        )
        .unwrap();

        // Exact match, range, sort (asc/desc), and exists agree between the two shapes.
        let queries = [
            Query::parse("rank:30").unwrap(),
            Query::Range {
                field: "rank".into(),
                lower: Some("15".into()),
                lower_inclusive: true,
                upper: Some("30".into()),
                upper_inclusive: true,
            },
            Query::Exists {
                field: "rank".into(),
            },
        ];
        for q in &queries {
            assert_eq!(
                id_order(fast_only.search_paged(q, 10, &[], 0).unwrap()),
                id_order(both.search_paged(q, 10, &[], 0).unwrap()),
                "results diverge for {q:?}"
            );
        }
        for order in [SortOrder::Asc, SortOrder::Desc] {
            let s = [sort("rank", order)];
            assert_eq!(
                id_order(fast_only.search_paged(&Query::MatchAll, 10, &s, 0).unwrap()),
                id_order(both.search_paged(&Query::MatchAll, 10, &s, 0).unwrap()),
                "sort order diverges ({order:?})"
            );
        }

        // The saving is real: no `rank` terms → smaller term dicts + postings; the columnar
        // side is the same either way.
        let a = fast_only.index_size_breakdown();
        let b = both.index_size_breakdown();
        assert!(
            a.term < b.term && a.postings < b.postings,
            "fast-only should shed inverted bytes: fast-only {a:?} vs indexed {b:?}"
        );
        assert_eq!(a.fast, b.fast, "columnar bytes identical");
    }

    #[test]
    fn term_query_on_a_numeric_field_is_an_exact_match() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = ranked_shard(tmp.path()); // id KEYWORD, rank LONG; a@30 b@10 c@20
                                              // A bare `rank:30` term on a LONG field is an exact-value match (doc a) — not a
                                              // "non-searchable field" error. This is what clicking a numeric field value does.
        assert_eq!(
            id_order(
                shard
                    .search_paged(&Query::parse("rank:30").unwrap(), 10, &[], 0)
                    .unwrap()
            ),
            vec!["a"]
        );
        // A value no doc has matches nothing — a clean empty page, still not an error.
        assert!(shard
            .search_paged(&Query::parse("rank:999").unwrap(), 10, &[], 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn quoted_phrase_on_a_non_text_field_is_an_exact_match() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = ranked_shard(tmp.path()); // id KEYWORD, rank LONG; a@30 b@10 c@20
                                              // The facet / filter chips emit a *quoted* `field:"value"`, which parses as a Phrase.
                                              // On a KEYWORD field it's an exact keyword match (doc a) and on a numeric field an
                                              // exact-value match (doc a) — not a "phrase requires an analyzed TEXT field" /
                                              // "non-searchable field" error. This is what clicking a facet value does.
        assert_eq!(
            id_order(
                shard
                    .search_paged(&Query::parse("id:\"a\"").unwrap(), 10, &[], 0)
                    .unwrap()
            ),
            vec!["a"]
        );
        assert_eq!(
            id_order(
                shard
                    .search_paged(&Query::parse("rank:\"30\"").unwrap(), 10, &[], 0)
                    .unwrap()
            ),
            vec!["a"]
        );
    }

    /// A shard whose docs all match `body:alpha` but with different term frequencies
    /// (same field length, so BM25 is monotonic in `tf`): `b` (tf 3) > `c` (tf 2) > `a`
    /// (tf 1). `grp` ties `a`/`b` so a `_score` tiebreaker is observable.
    fn scored_shard(tmp: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("grp", SourceType::Long),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: grp, type: LONG, fast: true }, { path: body, type: TEXT } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let store = LocalIndexStore::open(tmp).unwrap();
        let shard = store.create_shard(&ShardId::single("docs"), &idx).unwrap();
        let mk = |id: &str, grp: i64, body: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("grp".to_string(), Value::Int(grp));
            f.insert("body".to_string(), Value::from(body));
            LocatedDoc {
                doc: Document::new(key, f),
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![
                    mk("a", 1, "alpha pad pad"),     // tf 1 → lowest score
                    mk("b", 1, "alpha alpha alpha"), // tf 3 → highest score
                    mk("c", 2, "alpha alpha pad"),   // tf 2 → middle score
                ],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        shard
    }

    #[test]
    fn sorts_by_score_key_alone_and_among_other_keys() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = scored_shard(tmp.path());
        let q = Query::parse("body:alpha").unwrap();

        // `_score desc` alone: pure relevance, high→low (windowed order_by_score path).
        let desc = [sort("_score", SortOrder::Desc)];
        assert_eq!(
            id_order(shard.search_paged(&q, 10, &desc, 0).unwrap()),
            vec!["b", "c", "a"]
        );
        // `_score asc` alone: low→high (the exact-scan path — can't come from the
        // descending score window).
        let asc = [sort("_score", SortOrder::Asc)];
        assert_eq!(
            id_order(shard.search_paged(&q, 10, &asc, 0).unwrap()),
            vec!["a", "c", "b"]
        );
        // `grp asc, _score desc`: grp 1 ordered by score desc (b before a), then grp 2 (c).
        let multi = [sort("grp", SortOrder::Asc), sort("_score", SortOrder::Desc)];
        assert_eq!(
            id_order(shard.search_paged(&q, 10, &multi, 0).unwrap()),
            vec!["b", "a", "c"]
        );
        // Offset paging over a `_score` sort still slices the total order.
        assert_eq!(
            id_order(shard.search_paged(&q, 1, &desc, 1).unwrap()),
            vec!["c"]
        );
    }

    #[test]
    fn score_sort_is_offset_only_no_keyset() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = scored_shard(tmp.path());
        let q = Query::parse("body:alpha").unwrap();
        let desc = [sort("_score", SortOrder::Desc)];

        // A `_score` sort hands out no keyset cursor (relevance isn't a stable keyset).
        let (hits, next) = shard.search_after(&q, 10, &desc, None).unwrap();
        assert_eq!(id_order(hits), vec!["b", "c", "a"]);
        assert!(next.is_none());

        // And an incoming `search_after` cursor with a `_score` sort is a clear error.
        let cursor = SearchAfter {
            sort_values: vec![SortValue::Num(1.0)],
            key: CompositeKey::new(vec![], vec![("id".into(), Value::from("b"))]),
        };
        assert!(shard.search_after(&q, 10, &desc, Some(&cursor)).is_err());
    }

    /// Two commits, ties on the primary key resolved by a secondary key, then by the
    /// composite key — a deterministic total order across generations.
    fn tied_shard(tmp: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("grp", SourceType::Long),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: grp, type: LONG, fast: true }, { path: rank, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let store = LocalIndexStore::open(tmp).unwrap();
        let shard = store.create_shard(&ShardId::single("docs"), &idx).unwrap();
        let mk = |id: &str, grp: i64, rank: i64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("grp".to_string(), Value::Int(grp));
            f.insert("rank".to_string(), Value::Int(rank));
            LocatedDoc {
                doc: Document::new(key, f),
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                // grp ties: (a,1,30) (b,1,10); (d,1,30) shares grp+rank with a → key tiebreak.
                vec![mk("a", 1, 30), mk("b", 1, 10), mk("e", 2, 5)],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("d", 1, 30), mk("c", 1, 20)],
                SourceCheckpoint::iceberg(2),
                "b2",
            ),
        )
        .unwrap();
        shard
    }

    #[test]
    fn multi_key_sort_breaks_ties_then_by_composite_key() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = tied_shard(tmp.path());
        let all = Query::MatchAll;
        // grp asc, then rank desc: grp 1 first (rank 30,30,20,10 → a/d tie at 30
        // broken by key asc → a,d), then grp 2 (e).
        let keys = [sort("grp", SortOrder::Asc), sort("rank", SortOrder::Desc)];
        assert_eq!(
            id_order(shard.search_paged(&all, 10, &keys, 0).unwrap()),
            vec!["a", "d", "c", "b", "e"]
        );
        // The total order is stable under paging: page 2 (skip 2, take 2) is a slice.
        assert_eq!(
            id_order(shard.search_paged(&all, 2, &keys, 2).unwrap()),
            vec!["c", "b"]
        );
    }

    /// Page through the whole result set with `search_after`, k=2, and confirm the
    /// concatenation equals the single-page total order — no gaps, no duplicates,
    /// including across the `a`/`d` tie bucket and across the two generations.
    #[test]
    fn search_after_keyset_paging_covers_total_order() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = tied_shard(tmp.path());
        let all = Query::MatchAll;
        let keys = [sort("grp", SortOrder::Asc), sort("rank", SortOrder::Desc)];

        let mut got: Vec<String> = Vec::new();
        let mut cursor: Option<SearchAfter> = None;
        loop {
            let (page, next) = shard.search_after(&all, 2, &keys, cursor.as_ref()).unwrap();
            if page.is_empty() {
                break;
            }
            got.extend(id_order(page));
            cursor = next;
        }
        assert_eq!(got, vec!["a", "d", "c", "b", "e"]);

        // A single-key cursor (rank desc, key tiebreak) also covers everything exactly.
        let rank = [sort("rank", SortOrder::Desc)];
        let mut got1: Vec<String> = Vec::new();
        let mut cur1: Option<SearchAfter> = None;
        loop {
            let (page, next) = shard.search_after(&all, 2, &rank, cur1.as_ref()).unwrap();
            if page.is_empty() {
                break;
            }
            got1.extend(id_order(page));
            cur1 = next;
        }
        // rank: a=30,d=30 (key tiebreak a,d), c=20, b=10, e=5.
        assert_eq!(got1, vec!["a", "d", "c", "b", "e"]);

        // search_after with no sort key is a clear error.
        assert!(shard.search_after(&all, 2, &[], None).is_err());
    }

    /// A shard with a KEYWORD-fast `cat` group field + a LONG-fast `rank`, in two
    /// generations, with one doc superseded — to prove collapse is liveness-correct.
    fn collapse_shard(tmp: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("cat", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: cat, type: KEYWORD, fast: true }, { path: rank, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(tmp)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let mk = |id: &str, cat: &str, rank: i64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("cat".to_string(), Value::from(cat));
            f.insert("rank".to_string(), Value::Int(rank));
            LocatedDoc {
                doc: Document::new(key, f),
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("a", "red", 30), mk("b", "blue", 10), mk("c", "red", 20)],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                // d (blue,25); supersede a → (red,5), so a's gen-1 red/30 is dead.
                vec![mk("d", "blue", 25), mk("a", "red", 5)],
                SourceCheckpoint::iceberg(2),
                "b2",
            ),
        )
        .unwrap();
        shard
    }

    #[test]
    fn collapse_returns_top_hit_and_count_per_group_liveness_correct() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = collapse_shard(tmp.path());
        let all = Query::MatchAll;
        let rank_desc = [sort("rank", SortOrder::Desc)];

        let groups = shard
            .search_collapsed(&all, 10, &rank_desc, "cat", None)
            .unwrap();
        // Live by rank desc: d(blue,25), c(red,20), b(blue,10), a(red,5).
        // Groups in first-appearance order: blue (top d), red (top c). Counts 2/2.
        let summary: Vec<(String, String, usize)> = groups
            .iter()
            .map(|g| {
                (
                    g.group.to_index_string(),
                    g.hit.key.get("id").unwrap().to_index_string(),
                    g.count,
                )
            })
            .collect();
        assert_eq!(
            summary,
            vec![
                ("blue".to_string(), "d".to_string(), 2),
                ("red".to_string(), "c".to_string(), 2),
            ]
        );

        // top-k limits the number of groups, not the per-group count.
        let one = shard
            .search_collapsed(&all, 1, &rank_desc, "cat", None)
            .unwrap();
        assert_eq!(one.len(), 1);
        assert_eq!(one[0].group, Value::from("blue"));
        assert_eq!(one[0].count, 2);

        // Collapsing on a non-fast field is a clear error.
        assert!(shard
            .search_collapsed(&all, 10, &rank_desc, "id", None)
            .is_err());
        // Collapse needs a sort key.
        assert!(shard.search_collapsed(&all, 10, &[], "cat", None).is_err());
    }

    /// A shard with a KEYWORD-**fast** `name` field for string sorting, in two
    /// generations (one doc superseded), to prove lexicographic sort + keyset paging.
    fn named_shard(tmp: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("name", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: name, type: KEYWORD, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(tmp)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let mk = |id: &str, name: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("name".to_string(), Value::from(name));
            LocatedDoc {
                doc: Document::new(key, f),
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("1", "delta"), mk("2", "alpha"), mk("3", "charlie")],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("4", "bravo"), mk("2", "zulu")], // supersede id 2: alpha → zulu
                SourceCheckpoint::iceberg(2),
                "b2",
            ),
        )
        .unwrap();
        shard
    }

    #[test]
    fn sorts_by_keyword_fast_field_lexicographically() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = named_shard(tmp.path());
        let all = Query::MatchAll;
        let by_name = [sort("name", SortOrder::Asc)];

        // Live names: 1=delta, 2=zulu (superseded alpha is gone), 3=charlie, 4=bravo.
        // Ascending → bravo(4), charlie(3), delta(1), zulu(2).
        let ids: Vec<String> = id_order(shard.search_paged(&all, 10, &by_name, 0).unwrap());
        assert_eq!(ids, vec!["4", "3", "1", "2"]);

        // Descending.
        let desc = [sort("name", SortOrder::Desc)];
        let ids: Vec<String> = id_order(shard.search_paged(&all, 10, &desc, 0).unwrap());
        assert_eq!(ids, vec!["2", "1", "3", "4"]);
    }

    #[test]
    fn search_after_keyset_paging_on_a_string_field() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = named_shard(tmp.path());
        let all = Query::MatchAll;
        let by_name = [sort("name", SortOrder::Asc)];

        // Page the whole set by `name` asc, one at a time, via the keyset cursor.
        let mut got: Vec<String> = Vec::new();
        let mut cursor: Option<SearchAfter> = None;
        loop {
            let (page, next) = shard
                .search_after(&all, 1, &by_name, cursor.as_ref())
                .unwrap();
            if page.is_empty() {
                break;
            }
            got.extend(id_order(page));
            cursor = next;
        }
        // bravo(4), charlie(3), delta(1), zulu(2) — full coverage, no gaps/dupes.
        assert_eq!(got, vec!["4", "3", "1", "2"]);
    }
}

#[cfg(test)]
mod pit_tests {
    //! `ReadView` is the seam strict point-in-time reads are built on: a
    //! held view must keep observing one frozen snapshot regardless of later commits.
    use super::*;
    use growlerdb_core::{
        IndexDefinition, LocatedDoc, Sort, SortOrder, SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;

    fn shard(tmp: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: rank, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        LocalIndexStore::open(tmp)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap()
    }

    fn write(shard: &Shard, id: &str, rank: i64, snap: i64, batch: &str) {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("rank".to_string(), Value::Int(rank));
        let doc = LocatedDoc {
            doc: Document::new(key, f),
        };
        IndexWriter::write(
            shard,
            &CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(snap), batch),
        )
        .unwrap();
    }

    /// A held `ReadView` is a consistent snapshot: a superseding commit after it opens
    /// is invisible to it (old generation, old liveness, old snapshot id), while a
    /// fresh view sees the new state. This isolation is exactly what a PIT will hold.
    fn ids(hits: &[Hit]) -> Vec<String> {
        hits.iter()
            .map(|h| h.key.get("id").unwrap().to_index_string())
            .collect()
    }

    /// The headline: a PIT opened at `S` returns the exact as-of-`S` result set on every
    /// read, even after a superseding commit — while a fresh search sees the new state.
    #[test]
    fn pit_holds_the_as_of_snapshot_result_set_across_writes() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());
        let all = Query::MatchAll;
        let rank_desc = [Sort {
            field: "rank".into(),
            order: SortOrder::Desc,
        }];

        // gen 1: a@10, b@20.
        write(&shard, "a", 10, 1, "b1a");
        write(&shard, "b", 20, 1, "b1b");
        let pit = shard.open_pit().unwrap();

        // Superseding commits after the PIT opened: a→100, plus a new c@30.
        write(&shard, "a", 100, 2, "b2a");
        write(&shard, "c", 30, 2, "b2c");

        // The PIT still sees the as-of-S world: a=10 (not 100), b=20, no c.
        let (pit_hits, _) = shard
            .search_page_pit(pit.id, &all, 10, &rank_desc, 0, None)
            .unwrap();
        assert_eq!(ids(&pit_hits), vec!["b", "a"]); // rank desc: 20, 10

        // A fresh search reflects the new state: a=100, c=30, b=20.
        let (fresh_hits, _) = shard.search_page(&all, 10, &rank_desc, 0, None).unwrap();
        assert_eq!(ids(&fresh_hits), vec!["a", "c", "b"]); // 100, 30, 20

        // Re-reading the PIT is still stable.
        let (pit_again, _) = shard
            .search_page_pit(pit.id, &all, 10, &rank_desc, 0, None)
            .unwrap();
        assert_eq!(ids(&pit_again), vec!["b", "a"]);
    }

    #[test]
    fn pit_lifecycle_pins_generations_closes_and_rejects_unknown() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());
        let all = Query::MatchAll;
        let sort = [Sort {
            field: "rank".into(),
            order: SortOrder::Asc,
        }];

        write(&shard, "a", 10, 1, "b1");
        let pit = shard.open_pit().unwrap();
        write(&shard, "b", 20, 2, "b2"); // adds gen 2, not pinned by this PIT

        assert_eq!(shard.open_pit_count(), 1);

        // Closing frees the handle; a second close is a no-op.
        assert!(shard.close_pit(pit.id));
        assert!(!shard.close_pit(pit.id));
        assert_eq!(shard.open_pit_count(), 0);

        // Reading a closed/unknown handle is a clear error.
        let err = shard
            .search_page_pit(pit.id, &all, 10, &sort, 0, None)
            .unwrap_err();
        assert!(matches!(err, StoreError::UnknownPit(id) if id == pit.id));
    }

    #[test]
    fn expire_pits_evicts_idle_handles() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());
        write(&shard, "a", 10, 1, "b1");

        let pit = shard.open_pit().unwrap();
        assert_eq!(shard.open_pit_count(), 1);
        // A generous TTL keeps a just-used handle.
        assert_eq!(shard.expire_pits(Duration::from_secs(3600)), 0);
        assert_eq!(shard.open_pit_count(), 1);
        // A zero TTL evicts it (idle since open).
        assert_eq!(shard.expire_pits(Duration::ZERO), 1);
        assert_eq!(shard.open_pit_count(), 0);
        assert!(shard
            .search_page_pit(pit.id, &Query::MatchAll, 1, &[], 0, None)
            .is_err());
    }

    /// The cap bounds *active* handles: opening past `MAX_OPEN_PITS` errors, and
    /// closing one frees a slot. The bound on space amplification `expire_pits` (idle
    /// only) can't provide.
    #[test]
    fn open_pit_is_capped_and_a_close_frees_a_slot() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());
        write(&shard, "a", 10, 1, "b1");

        let mut open = Vec::new();
        for _ in 0..MAX_OPEN_PITS {
            open.push(shard.open_pit().unwrap());
        }
        assert_eq!(shard.open_pit_count(), MAX_OPEN_PITS);
        // One past the cap is a clear error, not an unbounded held view.
        assert!(matches!(
            shard.open_pit().unwrap_err(),
            StoreError::TooManyPits(m) if m == MAX_OPEN_PITS
        ));
        // Closing one frees a slot.
        assert!(shard.close_pit(open.pop().unwrap().id));
        assert!(shard.open_pit().is_ok());
    }

    /// Collapse honors a PIT: it groups the as-of-`S` world, not the latest.
    #[test]
    fn collapse_against_a_pit_uses_the_frozen_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        // A shard with a KEYWORD-fast `cat` group field + LONG-fast `rank`.
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("cat", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: cat, type: KEYWORD, fast: true }, { path: rank, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(tmp.path())
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let put = |id: &str, cat: &str, rank: i64, snap: i64, batch: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("cat".to_string(), Value::from(cat));
            f.insert("rank".to_string(), Value::Int(rank));
            let doc = LocatedDoc {
                doc: Document::new(key, f),
            };
            IndexWriter::write(
                &shard,
                &CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(snap), batch),
            )
            .unwrap();
        };
        let rank_desc = [Sort {
            field: "rank".into(),
            order: SortOrder::Desc,
        }];

        // as-of S: red={a@30}, blue={b@10}.
        put("a", "red", 30, 1, "b1a");
        put("b", "blue", 10, 1, "b1b");
        let pit = shard.open_pit().unwrap();

        // After S: a leaves red for green@5, and red gains c@99.
        put("a", "green", 5, 2, "b2a");
        put("c", "red", 99, 2, "b2c");

        // The PIT collapse sees the as-of-S groups: red top a@30 (count 1), blue b@10.
        let g = shard
            .search_collapsed_pit(pit.id, &Query::MatchAll, 10, &rank_desc, "cat", None)
            .unwrap();
        let summary: Vec<(String, String, usize)> = g
            .iter()
            .map(|c| {
                (
                    c.group.to_index_string(),
                    c.hit.key.get("id").unwrap().to_index_string(),
                    c.count,
                )
            })
            .collect();
        assert_eq!(
            summary,
            vec![
                ("red".to_string(), "a".to_string(), 1),
                ("blue".to_string(), "b".to_string(), 1),
            ]
        );

        // A fresh collapse reflects the new world. Live by rank desc: c(red,99),
        // b(blue,10), a(green,5) → groups red(top c), blue(top b), green(top a).
        let fresh = shard
            .search_collapsed(&Query::MatchAll, 10, &rank_desc, "cat", None)
            .unwrap();
        let fresh_groups: Vec<(String, String)> = fresh
            .iter()
            .map(|c| {
                (
                    c.group.to_index_string(),
                    c.hit.key.get("id").unwrap().to_index_string(),
                )
            })
            .collect();
        assert_eq!(
            fresh_groups,
            vec![
                ("red".to_string(), "c".to_string()),
                ("blue".to_string(), "b".to_string()),
                ("green".to_string(), "a".to_string()),
            ]
        );
    }
}

#[cfg(test)]
mod highlight_tests {
    use super::*;
    use growlerdb_core::{
        IndexDefinition, LocatedDoc, SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;

    #[test]
    fn highlights_matched_terms_in_cached_text_across_generations() {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT, cached: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store.create_shard(&ShardId::single("docs"), &idx).unwrap();
        let mk = |id: &str, body: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("body".to_string(), Value::from(body));
            LocatedDoc {
                doc: Document::new(key, f),
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("a", "the quick brown fox")],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("b", "a lazy brown dog"), mk("c", "no match here")],
                SourceCheckpoint::iceberg(2),
                "b2",
            ),
        )
        .unwrap();

        // Marked-segment text of a hit's `body` highlight, joined for assertion.
        let marked = |h: &Hit| -> Vec<String> {
            h.highlight
                .get("body")
                .into_iter()
                .flatten()
                .flat_map(|frag| frag.segments.iter())
                .filter(|s| s.marked)
                .map(|s| s.text.clone())
                .collect()
        };

        let q = Query::parse("body:brown").unwrap();
        let hl = Highlight::new(vec!["body".into()], 0, 100);
        let by_id: BTreeMap<String, Hit> = shard
            .search_highlighted(&q, 10, &hl)
            .unwrap()
            .into_iter()
            .map(|h| (h.key.get("id").unwrap().to_index_string(), h))
            .collect();
        // a + b match "brown" across the two generations, each with "brown" marked in its snippet.
        assert_eq!(by_id.len(), 2);
        assert_eq!(marked(&by_id["a"]), vec!["brown".to_string()]);
        assert_eq!(marked(&by_id["b"]), vec!["brown".to_string()]);
        // The un-marked context is preserved (XSS-safe segments, not HTML).
        let has_context = by_id["a"].highlight["body"][0]
            .segments
            .iter()
            .any(|s| !s.marked && s.text.contains("fox"));
        assert!(has_context, "fragment keeps surrounding context");

        // Highlighting a non-TEXT (keyword) field is silently skipped, not an error.
        let hl_kw = Highlight::new(vec!["id".into()], 0, 100);
        let kw_hits = shard.search_highlighted(&q, 10, &hl_kw).unwrap();
        assert!(kw_hits.iter().all(|h| h.highlight.is_empty()));

        // An empty `fields` request defaults to the highlightable TEXT fields (here `body`).
        let hl_default = Highlight::new(vec![], 0, 100);
        let default_hits = shard.search_highlighted(&q, 10, &hl_default).unwrap();
        assert!(default_hits
            .iter()
            .any(|h| h.highlight.contains_key("body")));

        // An analyzed (stemmed-style folding) match still highlights: the query lowercases,
        // so an uppercase query term marks the lowercased indexed token.
        let q_upper = Query::parse("body:BROWN").unwrap();
        let upper_hits = shard.search_highlighted(&q_upper, 10, &hl).unwrap();
        assert!(
            upper_hits.iter().any(|h| !marked(h).is_empty()),
            "analyzed match highlights"
        );
    }
}

#[cfg(test)]
mod agg_tests {
    use super::*;
    use growlerdb_core::{
        Agg, IndexDefinition, LocatedDoc, SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;

    #[test]
    fn aggregations_exclude_superseded_docs_across_generations() {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("category", SourceType::String),
                SourceField::new("amount", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: category, type: KEYWORD, fast: true }, { path: amount, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store.create_shard(&ShardId::single("docs"), &idx).unwrap();
        let mk = |id: &str, cat: &str, amt: i64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("category".to_string(), Value::from(cat));
            f.insert("amount".to_string(), Value::Int(amt));
            LocatedDoc {
                doc: Document::new(key, f),
            }
        };
        // gen 1: A(red, 10), B(blue, 20).
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("A", "red", 10), mk("B", "blue", 20)],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        // gen 2: update A → (green, 100). A's old (red, 10) is now tombstoned.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![mk("A", "green", 100)],
                SourceCheckpoint::iceberg(2),
                "b2",
            ),
        )
        .unwrap();

        let result = shard
            .aggregate(
                &Query::MatchAll,
                &[
                    (
                        "cats".to_string(),
                        Agg::Terms {
                            field: "category".into(),
                            size: 10,
                        },
                    ),
                    (
                        "amts".to_string(),
                        Agg::Stats {
                            field: "amount".into(),
                        },
                    ),
                ],
            )
            .unwrap();

        // terms: green + blue (each 1) — the superseded `red` is NOT counted.
        let buckets = result["cats"]["buckets"].as_array().unwrap();
        let mut keys: Vec<&str> = buckets.iter().map(|b| b["key"].as_str().unwrap()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["blue", "green"]);
        assert!(buckets
            .iter()
            .all(|b| b["doc_count"].as_u64().unwrap() == 1));

        // stats: 2 live docs, sum 120 (100 + 20) — not the old 10.
        let stats = &result["amts"];
        assert_eq!(stats["count"].as_u64().unwrap(), 2);
        assert_eq!(stats["sum"].as_f64().unwrap(), 120.0);
    }

    /// A shard with `cat` (KEYWORD fast), `amount` (LONG fast), `ts` (DATE fast), and a
    /// non-fast `id`, plus `n` docs from `rows` (id, cat, amount, ts-micros).
    fn agg_shard(tmp: &std::path::Path, rows: &[(&str, &str, i64, i64)]) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("cat", SourceType::String),
                SourceField::new("amount", SourceType::Long),
                SourceField::new("ts", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: cat, type: KEYWORD, fast: true }, { path: amount, type: LONG, fast: true }, { path: ts, type: DATE, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(tmp)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let docs: Vec<LocatedDoc> = rows
            .iter()
            .map(|(id, cat, amount, ts)| {
                let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(*id))]);
                let mut f = BTreeMap::new();
                f.insert("id".to_string(), Value::from(*id));
                f.insert("cat".to_string(), Value::from(*cat));
                f.insert("amount".to_string(), Value::Int(*amount));
                f.insert("ts".to_string(), Value::Int(*ts));
                LocatedDoc {
                    doc: Document::new(key, f),
                }
            })
            .collect();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
        shard
    }

    const DAY_US: i64 = 86_400_000_000; // one day in microseconds

    #[test]
    fn range_buckets_a_numeric_fast_field() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = agg_shard(
            tmp.path(),
            &[("a", "x", 10, 0), ("b", "x", 20, 0), ("c", "x", 100, 0)],
        );
        let r = shard
            .aggregate(
                &Query::MatchAll,
                &[(
                    "r".to_string(),
                    Agg::Range {
                        field: "amount".into(),
                        ranges: vec![
                            growlerdb_core::AggRange {
                                from: None,
                                to: Some(50.0),
                            },
                            growlerdb_core::AggRange {
                                from: Some(50.0),
                                to: None,
                            },
                        ],
                    },
                )],
            )
            .unwrap();
        let buckets = r["r"]["buckets"].as_array().unwrap();
        let counts: Vec<u64> = buckets
            .iter()
            .map(|b| b["doc_count"].as_u64().unwrap())
            .collect();
        // < 50 → {10, 20} = 2; >= 50 → {100} = 1.
        assert_eq!(counts, vec![2, 1]);
    }

    #[test]
    fn cardinality_counts_distinct_values() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = agg_shard(
            tmp.path(),
            &[("a", "red", 1, 0), ("b", "blue", 1, 0), ("c", "blue", 1, 0)],
        );
        let r = shard
            .aggregate(
                &Query::MatchAll,
                &[(
                    "c".to_string(),
                    Agg::Cardinality {
                        field: "cat".into(),
                    },
                )],
            )
            .unwrap();
        // Two distinct cats (red, blue); HLL is exact at this size.
        assert_eq!(r["c"]["value"].as_f64().unwrap(), 2.0);
    }

    #[test]
    fn percentiles_over_a_numeric_field_within_tolerance() {
        let tmp = tempfile::tempdir().unwrap();
        // 101 docs valued 0..=100; p50 ≈ 50, p90 ≈ 90.
        let owned: Vec<(String, i64)> = (0..=100).map(|v| (format!("d{v}"), v)).collect();
        let rows: Vec<(&str, &str, i64, i64)> = owned
            .iter()
            .map(|(id, v)| (id.as_str(), "x", *v, 0i64))
            .collect();
        let shard = agg_shard(tmp.path(), &rows);
        let r = shard
            .aggregate(
                &Query::MatchAll,
                &[(
                    "p".to_string(),
                    Agg::Percentiles {
                        field: "amount".into(),
                        percents: vec![50.0, 90.0],
                    },
                )],
            )
            .unwrap();
        let values = r["p"]["values"].as_object().unwrap();
        let p50 = values.values().next().unwrap().as_f64().unwrap();
        assert!((45.0..=55.0).contains(&p50), "p50 ~50, got {p50}");
        let p90 = values
            .get("90")
            .or_else(|| values.get("90.0"))
            .unwrap()
            .as_f64()
            .unwrap();
        assert!((85.0..=95.0).contains(&p90), "p90 ~90, got {p90}");
    }

    #[test]
    fn cross_shard_cardinality_and_percentiles_match_single_shard() {
        // Settles the "under-merge" question: merging two shards' sketch partials must
        // match a single shard over all the data within the sketch's error bound — i.e. the
        // cross-shard merge is *approximate but correct*, not silently under-counted.
        let make = |lo: i64, hi: i64| -> Vec<(String, String, i64, i64)> {
            (lo..hi)
                .map(|v| (format!("d{v}"), format!("c{}", v % 20), v, 0i64))
                .collect()
        };
        fn rows(o: &[(String, String, i64, i64)]) -> Vec<(&str, &str, i64, i64)> {
            o.iter()
                .map(|(id, cat, amt, ts)| (id.as_str(), cat.as_str(), *amt, *ts))
                .collect()
        }
        let (ta, tb, tall) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        let a = make(0, 100);
        let b = make(100, 200);
        let all = make(0, 200);
        let sa = agg_shard(ta.path(), &rows(&a));
        let sb = agg_shard(tb.path(), &rows(&b));
        let sall = agg_shard(tall.path(), &rows(&all));

        let aggs = vec![
            (
                "card".to_string(),
                Agg::Cardinality {
                    field: "cat".into(),
                },
            ),
            (
                "pct".to_string(),
                Agg::Percentiles {
                    field: "amount".into(),
                    percents: vec![50.0, 90.0],
                },
            ),
        ];

        let pa = sa.aggregate_partial(&Query::MatchAll, &aggs).unwrap();
        let pb = sb.aggregate_partial(&Query::MatchAll, &aggs).unwrap();
        let merged = merge_aggregations(&[pa, pb], &aggs).unwrap();
        let single = sall.aggregate(&Query::MatchAll, &aggs).unwrap();

        // Cardinality: 20 distinct cats; the merged HLL matches single-shard (exact at this size).
        let merged_card = merged["card"]["value"].as_f64().unwrap();
        let single_card = single["card"]["value"].as_f64().unwrap();
        assert_eq!(
            merged_card, single_card,
            "merged HLL must match single-shard"
        );
        assert!(
            (merged_card - 20.0).abs() < 1.0,
            "≈20 distinct, got {merged_card}"
        );

        // Percentiles: the merged DDSketch is within a small relative error of single-shard.
        let (mp, sp) = (
            merged["pct"]["values"].as_object().unwrap(),
            single["pct"]["values"].as_object().unwrap(),
        );
        for key in sp.keys() {
            let m = mp[key].as_f64().unwrap();
            let s = sp[key].as_f64().unwrap();
            let rel = (m - s).abs() / s.max(1.0);
            assert!(
                rel < 0.05,
                "pct {key}: merged {m} vs single {s} (rel {rel})"
            );
        }
    }

    #[test]
    fn date_histogram_buckets_by_day() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = agg_shard(
            tmp.path(),
            &[
                ("a", "x", 1, 0),          // day 0
                ("b", "x", 1, DAY_US),     // day 1
                ("c", "x", 1, DAY_US + 5), // day 1
            ],
        );
        let r = shard
            .aggregate(
                &Query::MatchAll,
                &[(
                    "d".to_string(),
                    Agg::DateHistogram {
                        field: "ts".into(),
                        fixed_interval: "1d".into(),
                    },
                )],
            )
            .unwrap();
        let buckets = r["d"]["buckets"].as_array().unwrap();
        let total: u64 = buckets
            .iter()
            .map(|b| b["doc_count"].as_u64().unwrap())
            .sum();
        let max = buckets
            .iter()
            .map(|b| b["doc_count"].as_u64().unwrap())
            .max()
            .unwrap();
        assert_eq!(total, 3); // all docs counted
        assert_eq!(max, 2); // the day-1 bucket holds two
    }

    #[test]
    fn aggregating_a_non_fast_field_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = agg_shard(tmp.path(), &[("a", "x", 1, 0)]);
        // `id` is KEYWORD but not a fast field → aggregation is a clear error.
        let err = shard.aggregate(
            &Query::MatchAll,
            &[(
                "t".to_string(),
                Agg::Terms {
                    field: "id".into(),
                    size: 10,
                },
            )],
        );
        assert!(err.is_err());
    }

    #[test]
    fn merge_aggregations_across_shards_sums_buckets_and_stats() {
        // Two shards with overlapping `cat` values (y is on both).
        let ta = tempfile::tempdir().unwrap();
        let tb = tempfile::tempdir().unwrap();
        let a = agg_shard(
            ta.path(),
            &[("1", "x", 10, 0), ("2", "x", 20, 0), ("3", "y", 5, 0)],
        );
        let b = agg_shard(tb.path(), &[("4", "y", 7, 0), ("5", "z", 9, 0)]);

        let aggs = vec![
            (
                "by_cat".to_string(),
                Agg::Terms {
                    field: "cat".into(),
                    size: 10,
                },
            ),
            (
                "amt".to_string(),
                Agg::Stats {
                    field: "amount".into(),
                },
            ),
        ];

        // Per-shard partials (the mergeable intermediate form) → merged + finalized.
        let pa = a.aggregate_partial(&Query::MatchAll, &aggs).unwrap();
        let pb = b.aggregate_partial(&Query::MatchAll, &aggs).unwrap();
        let merged = merge_aggregations(&[pa, pb], &aggs).unwrap();

        // Term buckets are summed across shards: x:2, y:2 (1+1), z:1.
        let mut counts = BTreeMap::new();
        for bucket in merged["by_cat"]["buckets"].as_array().unwrap() {
            counts.insert(
                bucket["key"].as_str().unwrap().to_string(),
                bucket["doc_count"].as_u64().unwrap(),
            );
        }
        assert_eq!(counts.get("x"), Some(&2));
        assert_eq!(counts.get("y"), Some(&2)); // 1 from each shard
        assert_eq!(counts.get("z"), Some(&1));

        // Stats combine exactly across shards (amounts 10,20,5 + 7,9).
        let s = &merged["amt"];
        assert_eq!(s["count"].as_u64().unwrap(), 5);
        assert_eq!(s["min"].as_f64().unwrap(), 5.0);
        assert_eq!(s["max"].as_f64().unwrap(), 20.0);
        assert_eq!(s["sum"].as_f64().unwrap(), 51.0);
    }

    #[test]
    fn merge_aggregations_of_a_single_partial_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = agg_shard(tmp.path(), &[("1", "x", 10, 0), ("2", "y", 20, 0)]);
        let aggs = vec![(
            "amt".to_string(),
            Agg::Stats {
                field: "amount".into(),
            },
        )];
        let partial = shard.aggregate_partial(&Query::MatchAll, &aggs).unwrap();
        let merged = merge_aggregations(&[partial], &aggs).unwrap();
        // Same as a direct single-shard aggregate.
        assert_eq!(merged, shard.aggregate(&Query::MatchAll, &aggs).unwrap());

        // No partials ⇒ an empty result map.
        assert!(merge_aggregations(&[], &aggs).unwrap().is_empty());
    }
}

/// Compare two hits for the merged ranking under the full sort `keys`. With no keys,
/// ranks by **score** descending. Otherwise compares each key's value in priority
/// order; remaining ties (and the no-key score ties) are broken by the **composite
/// key** ascending, so the order is **total and deterministic** — the same hit set
/// always pages identically, the foundation for `search_after`.
fn cmp_hits(
    a: &Hit,
    a_vals: &[SortValue],
    b: &Hit,
    b_vals: &[SortValue],
    keys: &[Sort],
) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if keys.is_empty() {
        let by_score = b.score.partial_cmp(&a.score).unwrap_or(Ordering::Equal);
        if by_score != Ordering::Equal {
            return by_score;
        }
    } else {
        for (i, key) in keys.iter().enumerate() {
            let c = cmp_sort_value(&a_vals[i], &b_vals[i], key.order);
            if c != Ordering::Equal {
                return c;
            }
        }
    }
    // Trailing tiebreaker: composite key ascending (a total, stable order).
    a.key.encode().cmp(&b.key.encode())
}

#[cfg(test)]
mod suggest_tests {
    use super::*;
    use growlerdb_core::{
        IndexDefinition, LocatedDoc, SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;

    fn shard(tmp: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("city", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD }, { path: body, type: TEXT } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        LocalIndexStore::open(tmp)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap()
    }

    fn put(shard: &Shard, id: &str, city: &str, body: &str, snap: i64, batch: &str) {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("city".to_string(), Value::from(city));
        f.insert("body".to_string(), Value::from(body));
        let doc = LocatedDoc {
            doc: Document::new(key, f),
        };
        IndexWriter::write(
            shard,
            &CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(snap), batch),
        )
        .unwrap();
    }

    #[test]
    fn autocomplete_returns_prefix_terms_ranked_by_frequency_across_generations() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());
        // gen 1: two docs in "berlin", one in "bern", one in "boston".
        put(&shard, "1", "berlin", "the quick fox", 1, "b1a");
        put(&shard, "2", "berlin", "quick brown dog", 1, "b1b");
        put(&shard, "3", "bern", "brown bear", 1, "b1c");
        // gen 2 (separate generation): another "berlin".
        put(&shard, "4", "berlin", "lazy fox", 2, "b2");

        // KEYWORD `city`, prefix "ber": berlin (freq 3 across gens) before bern (1);
        // "boston" is excluded.
        let cities = shard.suggest_prefix("city", "ber", 10).unwrap();
        assert_eq!(
            cities,
            vec![("berlin".to_string(), 3), ("bern".to_string(), 1)]
        );

        // limit caps the suggestions.
        let top1 = shard.suggest_prefix("city", "b", 1).unwrap();
        assert_eq!(top1, vec![("berlin".to_string(), 3)]);

        // TEXT `body`, analyzed: prefix lowercased to match tokens. "quick" (2 docs)
        // ranks above... only "quick" starts with "qu".
        let qu = shard.suggest_prefix("body", "QU", 10).unwrap();
        assert_eq!(qu, vec![("quick".to_string(), 2)]);

        // A no-match prefix is empty; a non-string field errors.
        assert!(shard.suggest_prefix("city", "zzz", 10).unwrap().is_empty());
        assert!(shard.suggest_prefix("id", "x", 10).is_ok()); // id is KEYWORD → fine
    }

    /// A shard whose `city` field opts into the completion sidecar.
    fn suggest_shard(tmp: &Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("city", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD, suggest: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        LocalIndexStore::open(tmp)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap()
    }

    fn put_city(shard: &Shard, id: &str, city: &str, snap: i64, batch: &str) {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("city".to_string(), Value::from(city));
        let doc = LocatedDoc {
            doc: Document::new(key, f),
        };
        IndexWriter::write(
            shard,
            &CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(snap), batch),
        )
        .unwrap();
    }

    #[test]
    fn suggest_sidecar_built_and_answers_top_k_by_frequency() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = suggest_shard(tmp.path());
        put_city(&shard, "1", "berlin", 1, "b1");
        put_city(&shard, "2", "berlin", 1, "b2");
        put_city(&shard, "3", "bern", 1, "b3");
        put_city(&shard, "4", "boston", 1, "b4");

        // A `.cmp` sidecar exists (the fast path has data to read).
        let has_cmp = std::fs::read_dir(shard.index_dir())
            .unwrap()
            .filter_map(|e| e.ok())
            .any(|e| e.path().extension().is_some_and(|x| x == "cmp"));
        assert!(has_cmp, "completion sidecar was not written");

        // Same frequency-ranked contract as the live path: berlin(2) before bern(1).
        assert_eq!(
            shard.suggest_prefix("city", "ber", 10).unwrap(),
            vec![("berlin".to_string(), 2), ("bern".to_string(), 1)]
        );
        // limit truncates the merged top-K: berlin(2) outranks bern(1)/boston(1).
        assert_eq!(
            shard.suggest_prefix("city", "b", 1).unwrap(),
            vec![("berlin".to_string(), 2)]
        );
    }

    #[test]
    fn suggest_sidecar_merges_across_segments_for_global_top_k() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = suggest_shard(tmp.path());
        // Each put is a separate generation → a separate segment (NoMergePolicy). "berlin" lands in
        // three segments (freq 1 each) and must sum to 3 across their per-segment sidecars.
        put_city(&shard, "1", "berlin", 1, "b1");
        put_city(&shard, "2", "berlin", 2, "b2");
        put_city(&shard, "3", "bern", 3, "b3");
        put_city(&shard, "4", "berlin", 4, "b4");

        assert_eq!(
            shard.suggest_prefix("city", "ber", 10).unwrap(),
            vec![("berlin".to_string(), 3), ("bern".to_string(), 1)]
        );
    }

    #[test]
    fn suggest_falls_back_for_long_prefix_and_unflagged_field() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = suggest_shard(tmp.path());
        // Two cities sharing a >P-byte prefix; a prefix longer than P isn't in the table → live seek.
        put_city(&shard, "1", "abcdefghij_one", 1, "b1");
        put_city(&shard, "2", "abcdefghij_two", 1, "b2");
        put_city(&shard, "3", "abcdefghij_two", 1, "b3");
        // 10-byte prefix (> P=8) — served correctly by the fallback live path.
        assert_eq!(
            shard.suggest_prefix("city", "abcdefghij", 10).unwrap(),
            vec![
                ("abcdefghij_two".to_string(), 2),
                ("abcdefghij_one".to_string(), 1),
            ]
        );
        // A short prefix (≤ P) is served by the sidecar, same answer.
        assert_eq!(
            shard.suggest_prefix("city", "abc", 10).unwrap(),
            vec![
                ("abcdefghij_two".to_string(), 2),
                ("abcdefghij_one".to_string(), 1),
            ]
        );
        // The unflagged `id` field has no sidecar → live path (never errors).
        assert!(shard.suggest_prefix("id", "1", 10).is_ok());
    }

    #[test]
    fn did_you_mean_ranks_by_edit_distance_then_frequency_across_generations() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());
        // body terms: house(1), mouse(2 across gens), horse(1).
        put(&shard, "1", "x", "house", 1, "b1a");
        put(&shard, "2", "x", "mouse", 1, "b1b");
        put(&shard, "3", "x", "horse", 1, "b1c");
        put(&shard, "4", "x", "mouse", 2, "b2"); // second generation

        // "house" (typed correctly) → exclude it; within edit distance 1 are mouse
        // (d1, freq 2) and horse (d1, freq 1); ranked by closeness then frequency.
        let s = shard.suggest_fuzzy("body", "house", 1, 10).unwrap();
        assert_eq!(s, vec![("mouse".to_string(), 2), ("horse".to_string(), 1)]);

        // A misspelling "hous" → "house" (one insertion); mouse/horse are distance 2.
        let s = shard.suggest_fuzzy("body", "hous", 1, 10).unwrap();
        assert_eq!(s, vec![("house".to_string(), 1)]);

        // limit caps; nothing within distance is empty.
        assert_eq!(shard.suggest_fuzzy("body", "house", 1, 1).unwrap().len(), 1);
        assert!(shard
            .suggest_fuzzy("body", "xyzzy", 1, 10)
            .unwrap()
            .is_empty());

        // KEYWORD field works too: city has only "x"; "zz" is distance 2 → empty.
        assert!(shard.suggest_fuzzy("city", "zz", 1, 10).unwrap().is_empty());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{IndexDefinition, LocatedDoc, SourceField, SourceSchema, SourceType};
    use std::collections::BTreeMap;

    fn index() -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            r#"
name: docs
source: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }
mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - { path: body, type: TEXT }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn located(id: i64, body: &str) -> LocatedDoc {
        let key = CompositeKey::new(vec![], vec![("id".into(), id.into())]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), id.into());
        fields.insert("body".to_string(), body.into());
        LocatedDoc {
            doc: Document::new(key, fields),
        }
    }

    fn batch() -> CommitBatch {
        CommitBatch::from_upserts(
            vec![
                located(1, "the quick brown fox"),
                located(2, "a lazy brown dog"),
            ],
            SourceCheckpoint::iceberg(100),
            "b1",
        )
    }

    fn ids(hits: &[Hit]) -> Vec<i64> {
        let mut v: Vec<i64> = hits
            .iter()
            .filter_map(|h| match h.key.get("id") {
                Some(growlerdb_core::Value::Int(i)) => Some(*i),
                _ => None,
            })
            .collect();
        v.sort_unstable();
        v
    }

    fn key(id: i64) -> CompositeKey {
        CompositeKey::new(vec![], vec![("id".into(), id.into())])
    }

    /// A widened schema for `docs`: the same fields as [`index`] plus a new mapped `title` TEXT
    /// field. Its derived Tantivy schema has one more field than [`index`]'s, so opening an index
    /// built with [`index`] against this definition is exactly the schema-change the guardrail
    /// catches.
    fn index_widened() -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
                SourceField::new("title", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            r#"
name: docs
source: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }
mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - { path: body, type: TEXT }
    - { path: title, type: TEXT }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    #[test]
    fn reopening_with_a_changed_schema_errors_instead_of_panicking() {
        // Build an index with schema A (id, body), then reopen the SAME data dir with schema B that
        // adds a mapped `title` field (a wider derived Tantivy schema). Reopening the stale narrower
        // index and writing wider-schema docs into it corrupts Tantivy's fast-field writer (the
        // historical `index out of bounds: the len is N but the index is N` panic). The store must
        // DETECT the change and refuse — never open the stale index — so the panic is unreachable.
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();

        // Schema A: build + commit real docs, so a built index (with meta.json) persists on disk.
        let shard_a = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();
        IndexWriter::write(&shard_a, &batch()).unwrap();
        drop(shard_a);

        // Schema B on the same dir: detected as a schema change, refused with the typed error —
        // BEFORE any writer touches the stale index. Not a panic, not a silent stale-open.
        let err = store
            .create_shard(&ShardId::single("docs"), &index_widened())
            .err()
            .expect("reopening with a changed schema must error, not open the stale index");
        assert!(
            matches!(&err, StoreError::SchemaChanged { index } if index == "docs"),
            "expected SchemaChanged, got {err:?}"
        );

        // The engine's reindex-from-scratch remedy (wipe the stale dir) lets schema B build cleanly:
        // a fresh index with the wider schema accepts a doc carrying the new `title` field.
        std::fs::remove_dir_all(tmp.path().join("docs")).unwrap();
        let shard_b = store
            .create_shard(&ShardId::single("docs"), &index_widened())
            .unwrap();
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), 1i64.into());
        fields.insert("body".to_string(), "hello".into());
        fields.insert("title".to_string(), "greeting".into());
        let doc = LocatedDoc {
            doc: Document::new(key(1), fields),
        };
        IndexWriter::write(
            &shard_b,
            &CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(1), "b1"),
        )
        .unwrap();
        assert_eq!(
            shard_b
                .search_all(&Query::parse("title:greeting").unwrap(), 10)
                .unwrap()
                .len(),
            1
        );
    }

    fn open_committed(tmp: &std::path::Path) -> Shard {
        let store = LocalIndexStore::open(tmp).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();
        IndexWriter::write(&shard, &batch()).unwrap(); // gen 1: ids 1,2 ("brown")
        shard
    }

    /// An empty (uncommitted) shard — every shard is D30-layered.
    fn open_empty(tmp: &std::path::Path) -> Shard {
        let store = LocalIndexStore::open(tmp).unwrap();
        store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap()
    }

    fn search(shard: &Shard, q: &str) -> Vec<i64> {
        ids(&shard.search_all(&Query::parse(q).unwrap(), 10).unwrap())
    }

    #[test]
    fn delete_by_key_excludes_doc_from_search() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_committed(tmp.path());
        assert_eq!(search(&shard, "body:brown"), vec![1, 2]);

        // Delete id 1 → logically removed from search.
        IndexWriter::write(
            &shard,
            &CommitBatch::new(
                vec![DocOp::Delete(key(1))],
                SourceCheckpoint::iceberg(101),
                "del-1",
            ),
        )
        .unwrap();

        assert_eq!(
            search(&shard, "body:brown"),
            vec![2],
            "deleted doc excluded"
        );
        assert!(
            !shard.contains_key(&key(1)).unwrap(),
            "key no longer indexed"
        );
    }

    /// A `safe_checkpoint` (the connector's resume floor) prunes every idempotency record
    /// at/below it — but only those; records above the floor stay so a resume-driven replay still
    /// dedups. Without a floor, nothing is pruned.
    #[test]
    fn safe_checkpoint_prunes_batch_ids_at_or_below_the_resume_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_empty(tmp.path());

        // Checkpoints carry their sequence number: the prune index is keyed by
        // it — a floor without one prunes nothing (snapshot ids are unordered).
        // These tests use seq == id for readability.
        let ordered = |cp: i64| SourceCheckpoint::iceberg_ordered(cp, cp);
        let commit = |id: i64, cp: i64, from: Option<i64>, safe: Option<i64>| {
            let batch = CommitBatch::from_upserts(
                vec![located(id, "brown")],
                ordered(cp),
                format!("b{cp}"),
            )
            .with_from_checkpoint(from.map(ordered))
            .with_safe_checkpoint(safe.map(ordered));
            IndexWriter::write(&shard, &batch).unwrap();
        };

        // No floor yet → every record retained (dedup must keep working across the whole chain).
        commit(1, 100, None, None);
        commit(2, 200, Some(100), None);
        commit(3, 300, Some(200), None);
        for cp in [100, 200, 300] {
            assert!(
                shard.batch_snapshot(&format!("b{cp}")).unwrap().is_some(),
                "b{cp} retained while no resume floor has been supplied"
            );
        }

        // Resume floor 200: batches at checkpoint <= 200 can never be re-sent → b100, b200 pruned;
        // b300 (above the floor) and b400 (current) stay.
        commit(4, 400, Some(300), Some(200));
        assert!(
            shard.batch_snapshot("b100").unwrap().is_none(),
            "b100 pruned (below floor 200)"
        );
        assert!(
            shard.batch_snapshot("b200").unwrap().is_none(),
            "b200 pruned (at floor 200)"
        );
        assert!(
            shard.batch_snapshot("b300").unwrap().is_some(),
            "b300 retained (above floor 200)"
        );
        assert!(
            shard.batch_snapshot("b400").unwrap().is_some(),
            "b400 retained (current)"
        );

        // A retained batch still dedups on replay (no CheckpointGap, no double-apply): re-sending
        // b300 is a benign no-op that leaves the shard where it is.
        let before = shard.current_snapshot().unwrap();
        let replay = CommitBatch::from_upserts(vec![located(3, "brown")], ordered(300), "b300")
            .with_from_checkpoint(Some(ordered(200)));
        IndexWriter::write(&shard, &replay).unwrap();
        assert_eq!(
            shard.current_snapshot().unwrap(),
            before,
            "replay of a retained batch is a dedup no-op"
        );
    }

    /// The continuity decision matrix — window-covering with sequence numbers,
    /// exact-match fallback without them.
    #[test]
    fn continuity_decision_matrix() {
        let ord = SourceCheckpoint::iceberg_ordered;
        let plain = SourceCheckpoint::iceberg;
        let c = Shard::continuity;
        // A shard with no checkpoint accepts anything (first write wins).
        assert_eq!(c(None, None, &ord(5, 5)), Continuity::Apply);
        // Covering: from ≤ current < end — the overlap re-applies committed ops, content-safe.
        assert_eq!(
            c(Some(&ord(20, 2)), Some(&ord(10, 1)), &ord(30, 3)),
            Continuity::Apply
        );
        // Exact continuation still applies.
        assert_eq!(
            c(Some(&ord(20, 2)), Some(&ord(20, 2)), &ord(30, 3)),
            Continuity::Apply
        );
        // A replay ending exactly at / strictly behind the shard no-ops — never regresses.
        assert_eq!(
            c(Some(&ord(30, 3)), Some(&ord(10, 1)), &ord(30, 3)),
            Continuity::NoOp
        );
        assert_eq!(
            c(Some(&ord(30, 3)), Some(&ord(10, 1)), &ord(20, 2)),
            Continuity::NoOp
        );
        // `from` strictly ahead: a hole — the loss signal the guard exists for.
        assert_eq!(
            c(Some(&ord(20, 2)), Some(&ord(30, 3)), &ord(40, 4)),
            Continuity::Gap
        );
        // Bootstrap batch (`from = None`) onto an ahead shard: stale end no-ops — the old
        // unconditional bypass could REGRESS the checkpoint here.
        assert_eq!(c(Some(&ord(30, 3)), None, &ord(20, 2)), Continuity::NoOp);
        // Bootstrap batches at one fixed checkpoint (bulk build / reindex chunks) all apply.
        assert_eq!(c(Some(&ord(42, 7)), None, &ord(42, 7)), Continuity::Apply);
        assert_eq!(c(Some(&plain(42)), None, &plain(42)), Continuity::Apply);
        // Order comes from the sequence number alone — ids here are numerically backwards.
        assert_eq!(
            c(Some(&ord(900, 2)), Some(&ord(999, 1)), &ord(50, 3)),
            Continuity::Apply
        );
        // Legacy (no sequence numbers): exact semantics.
        assert_eq!(
            c(Some(&plain(20)), Some(&plain(20)), &plain(30)),
            Continuity::Apply
        );
        assert_eq!(
            c(Some(&plain(20)), Some(&plain(10)), &plain(30)),
            Continuity::Gap
        );
        // Upgrade seam: legacy stored current, seq-stamped batch resuming from that position.
        assert_eq!(
            c(Some(&plain(20)), Some(&ord(20, 2)), &ord(30, 3)),
            Continuity::Apply
        );
    }

    /// Recovery after a partial fan-out + head advance no longer wedges. A re-send
    /// whose window COVERS the shard's checkpoint (from behind it, end past it, by sequence
    /// number) applies under a NEW batch id instead of tripping `CheckpointGap`; a stale
    /// replay ending at the shard's position no-ops without regressing.
    #[test]
    fn window_covering_absorbs_resume_skew() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_empty(tmp.path());
        let ord = SourceCheckpoint::iceberg_ordered;

        // Trigger 1 committed (R=100/seq1 → A=111/seq2] here; a sibling shard failed, the
        // stream restarted, and the head moved to H2=222/seq3 before the re-send.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![located(1, "brown")], ord(111, 2), "t1")
                .with_from_checkpoint(Some(ord(100, 1))),
        )
        .unwrap();

        // The re-send: window (100 → 222], a NEW id (the old exact guard wedged here forever:
        // from=100 ≠ current=111 and the id "t2" was never recorded).
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![
                    located(1, "brown"), // overlap: re-apply, idempotent
                    located(2, "quick"), // the rows past the old head
                ],
                ord(222, 3),
                "t2",
            )
            .with_from_checkpoint(Some(ord(100, 1))),
        )
        .unwrap();
        assert_eq!(shard.current_checkpoint().unwrap(), Some(ord(222, 3)));
        assert_eq!(search(&shard, "body:brown"), vec![1], "overlap deduped");
        assert_eq!(search(&shard, "body:quick"), vec![2], "new rows landed");

        // A stale full replay ending at the OLD head no-ops — checkpoint stays at 222.
        let before = shard.current_snapshot().unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![located(1, "brown")], ord(111, 2), "t3-stale")
                .with_from_checkpoint(Some(ord(100, 1))),
        )
        .unwrap();
        assert_eq!(
            shard.current_checkpoint().unwrap(),
            Some(ord(222, 3)),
            "stale replay never regresses the checkpoint"
        );
        assert_eq!(shard.current_snapshot().unwrap(), before);
    }

    /// The silent-loss signature, reproduced at the node guard:
    /// one connector trigger's writes land on *some* shards but not others while the checkpoint
    /// advances. Three shards start caught up at `R`; trigger 1 `(R → H1]` lands on shard 0 only
    /// (shards 1 & 2 "missed" their writes — the compaction-race loss); trigger 2 `(H1 → H2]`, which
    /// the connector stamps `from = H1` on *every* sub-batch, then arrives. Shard 0's window covers
    /// it and applies; shards 1 & 2 are still at `R`, so `from = H1` is a **hole** ahead of them and
    /// the write is refused as `CheckpointGap` — the loss is now LOUD, not silently sealed. (In the
    /// live pipeline the connector never reaches trigger 2: a partial fan-out throws so the Spark
    /// offset doesn't advance — [`ShardFanOut`]. This isolates the node's last-line guard.)
    #[test]
    fn task194_missed_shard_write_trips_a_loud_checkpoint_gap() {
        let ord = SourceCheckpoint::iceberg_ordered;
        let (t0, t1, t2) = (
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
            tempfile::tempdir().unwrap(),
        );
        let shards = [
            open_empty(t0.path()),
            open_empty(t1.path()),
            open_empty(t2.path()),
        ];

        // Trigger 0 (bootstrap → R=100/seq1): every shard is caught up at R.
        for s in &shards {
            IndexWriter::write(
                s,
                &CommitBatch::from_upserts(vec![located(0, "seed")], ord(100, 1), "t0"),
            )
            .unwrap();
        }

        // Trigger 1 (R=100 → H1=111/seq2): only shard 0's write lands; shards 1 & 2 miss theirs.
        IndexWriter::write(
            &shards[0],
            &CommitBatch::from_upserts(vec![located(1, "brown")], ord(111, 2), "t1")
                .with_from_checkpoint(Some(ord(100, 1))),
        )
        .unwrap();
        assert_eq!(shards[0].current_checkpoint().unwrap(), Some(ord(111, 2)));
        assert_eq!(
            shards[1].current_checkpoint().unwrap(),
            Some(ord(100, 1)),
            "shard 1 never got trigger 1 — still at R"
        );

        // Trigger 2 (H1=111 → H2=222/seq3), `from = H1` stamped on every sub-batch.
        let t2_batch = || {
            CommitBatch::from_upserts(vec![located(2, "quick")], ord(222, 3), "t2")
                .with_from_checkpoint(Some(ord(111, 2)))
        };

        // Shard 0 is at H1 → the window covers it → applies.
        IndexWriter::write(&shards[0], &t2_batch()).unwrap();
        assert_eq!(shards[0].current_checkpoint().unwrap(), Some(ord(222, 3)));

        // Shards 1 & 2 are behind at R: `from = H1` is strictly ahead — a hole. The old silent bug
        // jumped them to H2 and skipped trigger 1's row forever; now the write is refused, LOUDLY,
        // and the checkpoint never advances over the gap.
        for s in &shards[1..] {
            let err = IndexWriter::write(s, &t2_batch()).unwrap_err();
            assert!(
                matches!(err, StoreError::CheckpointGap { .. }),
                "a missed-write shard must trip CheckpointGap, not silently seal the loss: {err:?}"
            );
            assert_eq!(
                s.current_checkpoint().unwrap(),
                Some(ord(100, 1)),
                "the refused write left the checkpoint at R — no forward jump over the hole"
            );
        }
    }

    /// The continuity guard is re-decided at COMMIT under the writer mutex. Two
    /// batches staged against the same `current` can no longer both advance blindly — the
    /// staged-earlier/committed-later one resolves by position (NoOp here; Gap in the legacy
    /// no-sequence flavor), never a silent checkpoint regression.
    #[test]
    fn commit_recheck_prevents_checkpoint_regression() {
        let ord = SourceCheckpoint::iceberg_ordered;
        {
            let tmp = tempfile::tempdir().unwrap();
            let shard = open_empty(tmp.path());
            IndexWriter::write(
                &shard,
                &CommitBatch::from_upserts(vec![located(1, "brown")], ord(10, 1), "base"),
            )
            .unwrap();
            // Both writers stage against current = seq1 (the lock-free advisory check passes
            // for both), then commit in the "wrong" order.
            let short = shard
                .stage_batch(
                    &CommitBatch::from_upserts(vec![located(2, "quick")], ord(20, 2), "w-short")
                        .with_from_checkpoint(Some(ord(10, 1))),
                )
                .unwrap();
            let long = shard
                .stage_batch(
                    &CommitBatch::from_upserts(vec![located(3, "lazy")], ord(30, 3), "w-long")
                        .with_from_checkpoint(Some(ord(10, 1))),
                )
                .unwrap();
            shard.commit_staged(&[long]).unwrap();
            assert_eq!(shard.current_checkpoint().unwrap(), Some(ord(30, 3)));
            // Committing `short` blindly would regress the checkpoint 30 → 20.
            shard.commit_staged(&[short]).unwrap();
            assert_eq!(
                shard.current_checkpoint().unwrap(),
                Some(ord(30, 3)),
                "commit-time re-check turns the loser into a NoOp, not a regression"
            );
        }
        {
            // Legacy flavor (no sequence numbers): the loser can't be proven covered, so it
            // fails LOUDLY at commit instead of silently regressing.
            let tmp = tempfile::tempdir().unwrap();
            let shard = open_empty(tmp.path());
            let plain = SourceCheckpoint::iceberg;
            IndexWriter::write(
                &shard,
                &CommitBatch::from_upserts(vec![located(1, "brown")], plain(10), "base"),
            )
            .unwrap();
            let short = shard
                .stage_batch(
                    &CommitBatch::from_upserts(vec![located(2, "quick")], plain(20), "w-short")
                        .with_from_checkpoint(Some(plain(10))),
                )
                .unwrap();
            let long = shard
                .stage_batch(
                    &CommitBatch::from_upserts(vec![located(3, "lazy")], plain(30), "w-long")
                        .with_from_checkpoint(Some(plain(10))),
                )
                .unwrap();
            shard.commit_staged(&[long]).unwrap();
            let err = shard.commit_staged(&[short]).unwrap_err();
            assert!(
                matches!(err, StoreError::CheckpointGap { .. }),
                "legacy loser fails loudly at commit, got {err:?}"
            );
            assert_eq!(shard.current_checkpoint().unwrap(), Some(plain(30)));
        }
    }

    /// Pruning orders by sequence number, never by the random snapshot id. A
    /// numeric range over snapshot ids could delete the record inserted in the SAME txn whenever
    /// the new checkpoint's id happened to sort below the floor's id (~50% per boundary).
    #[test]
    fn prune_orders_by_sequence_not_snapshot_id() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_empty(tmp.path());
        let ord = SourceCheckpoint::iceberg_ordered;

        // Lineage A → B → C with ids deliberately numerically backwards: 900, 50, 30.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![located(1, "brown")], ord(900, 1), "bA"),
        )
        .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![located(2, "quick")], ord(50, 2), "bB")
                .with_from_checkpoint(Some(ord(900, 1))),
        )
        .unwrap();
        // bC ends at id 30 — numerically BELOW its own floor's id 50: the old `..= id` range
        // pruned bC's just-inserted record here. By sequence (3 > 2) it must survive, while
        // bA (seq 1) and bB (seq 2, at the floor) are pruned.
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![located(3, "lazy")], ord(30, 3), "bC")
                .with_from_checkpoint(Some(ord(50, 2)))
                .with_safe_checkpoint(Some(ord(50, 2))),
        )
        .unwrap();
        assert!(shard.batch_snapshot("bA").unwrap().is_none(), "bA pruned");
        assert!(shard.batch_snapshot("bB").unwrap().is_none(), "bB pruned");
        assert!(
            shard.batch_snapshot("bC").unwrap().is_some(),
            "bC survives its own floor despite its numerically-small id"
        );
    }

    /// A shard whose FIRST commit is an empty batch (0 rows routed here, e.g. a sparse
    /// multi-shard build) must still record the source checkpoint — else it reports
    /// `uninitialized` forever. Advances from `None`, not just N -> M.
    #[test]
    fn first_empty_batch_records_the_source_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_empty(tmp.path());
        assert_eq!(
            shard.current_checkpoint().unwrap(),
            None,
            "fresh shard has no checkpoint"
        );
        // The build commits an empty batch (this shard owns 0 of the source rows).
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![], SourceCheckpoint::iceberg(5), "snapshot-5-1"),
        )
        .unwrap();
        assert_eq!(
            shard.current_checkpoint().unwrap(),
            Some(SourceCheckpoint::iceberg(5)),
            "an empty first build must still record the source snapshot it caught up to"
        );
    }

    /// The empty-batch checkpoint advance (the redb-only path) prunes too — its
    /// `safe_checkpoint` reaches the same helper as a document commit.
    #[test]
    fn empty_batch_advance_also_prunes() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_empty(tmp.path());
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![located(1, "brown")],
                SourceCheckpoint::iceberg_ordered(100, 100),
                "b100",
            ),
        )
        .unwrap();
        assert!(shard.batch_snapshot("b100").unwrap().is_some());

        // An empty window advancing 100 → 200, stamped with resume floor 100, prunes b100.
        let empty = CommitBatch::new(vec![], SourceCheckpoint::iceberg_ordered(200, 200), "b200")
            .with_from_checkpoint(Some(SourceCheckpoint::iceberg_ordered(100, 100)))
            .with_safe_checkpoint(Some(SourceCheckpoint::iceberg_ordered(100, 100)));
        IndexWriter::write(&shard, &empty).unwrap();
        assert!(
            shard.batch_snapshot("b100").unwrap().is_none(),
            "b100 pruned via the empty-batch advance"
        );
        assert!(
            shard.batch_snapshot("b200").unwrap().is_some(),
            "the empty advance still records its own batch id"
        );
    }

    #[test]
    fn upsert_supersedes_prior_version() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();

        // gen 1: id 7 = "old release notes"
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![located(7, "old release notes")],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        assert!(shard.contains_key(&key(7)).unwrap());

        // re-upsert id 7 = "new shiny notes" (supersedes the prior version)
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![located(7, "new shiny notes")],
                SourceCheckpoint::iceberg(2),
                "b2",
            ),
        )
        .unwrap();

        // The new version supersedes the old (Tantivy delete-then-add).
        assert!(shard.contains_key(&key(7)).unwrap());
        assert_eq!(search(&shard, "body:shiny"), vec![7], "new version is live");
        assert_eq!(
            search(&shard, "body:old"),
            Vec::<i64>::new(),
            "old version superseded"
        );
        // Exactly one live hit for id 7 across a term in both versions ("notes").
        assert_eq!(search(&shard, "body:notes"), vec![7]);
    }

    #[test]
    fn re_upserting_a_deleted_key_revives_it() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_committed(tmp.path());
        // delete id 1, then upsert it again
        IndexWriter::write(
            &shard,
            &CommitBatch::new(
                vec![DocOp::Delete(key(1))],
                SourceCheckpoint::iceberg(2),
                "d1",
            ),
        )
        .unwrap();
        assert_eq!(search(&shard, "body:brown"), vec![2]);
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![located(1, "brown again")],
                SourceCheckpoint::iceberg(3),
                "u1",
            ),
        )
        .unwrap();
        assert_eq!(
            search(&shard, "body:brown"),
            vec![1, 2],
            "re-upsert revives the key"
        );
    }

    #[test]
    fn within_batch_last_write_wins() {
        // A changelog micro-batch with upsert-then-delete of the same key in one
        // commit nets to a delete (last op wins) — no stray live doc.
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::new(
                vec![
                    DocOp::Upsert(located(5, "brown transient")),
                    DocOp::Delete(key(5)),
                ],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        assert_eq!(search(&shard, "body:brown"), Vec::<i64>::new());
        assert!(!shard.contains_key(&key(5)).unwrap());
    }

    #[test]
    fn index_api_traits_drive_the_shard() {
        // Exercise the shard purely through the Design-02 seam (IndexWriter /
        // IndexReader), as the engine will.
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();

        assert_eq!(IndexReader::snapshot(&shard), Snapshot(0));
        let snap = IndexWriter::write(&shard, &batch()).unwrap();
        assert_eq!(snap, Snapshot(1));
        assert_eq!(IndexReader::snapshot(&shard), Snapshot(1));

        // search → coordinates + score on the default TEXT field.
        let hits = IndexReader::search(&shard, &SearchParams::parse("brown", 10).unwrap()).unwrap();
        assert_eq!(hits.total, 2);
        assert!(hits.hits.iter().all(|h| h.score > 0.0));
        assert!(hits.hits.iter().all(|h| h.key.get("id").is_some()));

        // field-scoped search.
        let scoped =
            IndexReader::search(&shard, &SearchParams::parse("body:fox", 10).unwrap()).unwrap();
        assert_eq!(scoped.total, 1);
    }

    #[test]
    fn record_freq_matches_position_results_and_sheds_positions() {
        // Two shards over the same docs: `body` at the default `record: POSITION` vs
        // `record: FREQ`. Term/match results AND scores are identical (BM25 needs freqs +
        // fieldnorms, both present); a phrase query works on POSITION and fails with the
        // remedy on FREQ; and the FREQ shard writes zero position bytes.
        let mk_shard = |tmp: &std::path::Path, body_spec: &str| {
            let src = SourceSchema::new(
                vec![
                    SourceField::new("id", SourceType::String),
                    SourceField::new("body", SourceType::String),
                ],
                vec![],
                vec!["id".into()],
            );
            let idx = IndexDefinition::from_yaml(&format!(
                "name: docs\nsource: {{ iceberg: {{ catalog: g, table: g.docs }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD }}, {body_spec} ] }}\n",
            ))
            .unwrap()
            .resolve(&src)
            .unwrap();
            let store = LocalIndexStore::open(tmp).unwrap();
            let shard = store.create_shard(&ShardId::single("docs"), &idx).unwrap();
            // Enough multi-token docs that real position data dwarfs the fixed per-segment
            // `.pos` footer tantivy writes even for a positionless field.
            let bodies = [
                "the quick brown fox jumps over the lazy dog tonight",
                "a lazy brown dog sleeps under the quick warm sun",
                "quick quick slow steady wins the long race every time",
            ];
            let docs: Vec<LocatedDoc> = (0..300)
                .map(|i| located(i, bodies[i as usize % 3]))
                .collect();
            IndexWriter::write(
                &shard,
                &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(100), "b1"),
            )
            .unwrap();
            shard
        };

        let tmp_pos = tempfile::tempdir().unwrap();
        let with_positions = mk_shard(tmp_pos.path(), "{ path: body, type: TEXT }");
        let tmp_freq = tempfile::tempdir().unwrap();
        let freq_only = mk_shard(tmp_freq.path(), "{ path: body, type: TEXT, record: FREQ }");

        // Identical hits AND identical BM25 scores — freqs and fieldnorms are both present.
        // Fetch every match: same-body docs tie exactly, so a top-k cutoff would compare
        // arbitrary tie-break order, not ranking.
        for q in ["body:brown", "body:quick"] {
            let a = with_positions
                .search_all(&Query::parse(q).unwrap(), 500)
                .unwrap();
            let b = freq_only
                .search_all(&Query::parse(q).unwrap(), 500)
                .unwrap();
            assert_eq!(ids(&a), ids(&b), "results diverge for {q}");
            let scores = |hits: &[Hit]| {
                let mut s: Vec<f32> = hits.iter().map(|h| h.score).collect();
                s.sort_by(|x, y| x.total_cmp(y));
                s
            };
            assert_eq!(scores(&a), scores(&b), "scores diverge for {q}");
        }

        // Phrase: works with positions, clear remedy without them.
        let phrase = Query::parse("body:\"quick brown\"").unwrap();
        let phrase_hits = with_positions.search_all(&phrase, 500).unwrap();
        assert_eq!(phrase_hits.len(), 100, "every 'quick brown fox' doc");
        let err = freq_only.search_all(&phrase, 10).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("body") && msg.contains("record: POSITION"),
            "error must name the field and the fix, got: {msg}"
        );

        // The saving: the freq shard's `.pos` is the fixed per-segment footer only —
        // a fraction of the position shard's real data.
        let (a, b) = (
            with_positions.index_size_breakdown(),
            freq_only.index_size_breakdown(),
        );
        assert!(
            b.positions * 4 < a.positions,
            "freq shard should carry footers only: with={a:?} without={b:?}"
        );
    }

    #[test]
    fn stored_key_round_trips_through_a_real_shard() {
        // Hit identity is the stored `enc(key)` bytes decoded back — assert the full
        // write→search→decode loop over realistic hex-string keys (the scale-run shape).
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();
        let hex = |i: u64| format!("{:032x}", i.wrapping_mul(0x9e3779b97f4a7c15));
        let docs: Vec<LocatedDoc> = (0..500)
            .map(|i| {
                let key = CompositeKey::new(vec![], vec![("id".into(), hex(i).into())]);
                let mut fields = BTreeMap::new();
                fields.insert("id".to_string(), hex(i).into());
                fields.insert("body".to_string(), "the quick brown fox".into());
                LocatedDoc {
                    doc: Document::new(key, fields),
                }
            })
            .collect();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(100), "b1"),
        )
        .unwrap();

        let hits = shard
            .search_all(&Query::parse("body:fox").unwrap(), 500)
            .unwrap();
        assert_eq!(hits.len(), 500);
        let mut got: Vec<String> = hits
            .iter()
            .map(|h| h.key.get("id").unwrap().to_index_string())
            .collect();
        got.sort_unstable();
        let mut want: Vec<String> = (0..500).map(hex).collect();
        want.sort_unstable();
        assert_eq!(got, want, "every stored key decodes back exactly");
    }

    #[test]
    fn size_breakdown_components_are_populated_and_reconcile() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_committed(tmp.path());

        // Every Tantivy structure a committed TEXT+KEYWORD segment writes is attributed: term
        // dicts, postings, positions (body is analyzed TEXT), fieldnorms, fast fields, the doc
        // store (`_key` is STORED), and metadata (plus the sibling `aux.redb`, counted in `other`).
        let b = shard.index_size_breakdown();
        assert!(b.term > 0, "term dict: {b:?}");
        assert!(b.postings > 0, "postings: {b:?}");
        assert!(b.positions > 0, "positions: {b:?}");
        assert!(b.fieldnorms > 0, "fieldnorms: {b:?}");
        assert!(b.fast > 0, "fast fields: {b:?}");
        assert!(b.store > 0, "doc store: {b:?}");
        assert!(b.other > 0, "metadata + aux.redb: {b:?}");
        assert_eq!(
            b.inverted(),
            b.term + b.postings + b.positions + b.fieldnorms
        );

        // The total gauge is the breakdown's sum by construction, and DescribeIndex's
        // per-shard size agrees — one number, however you ask for it.
        assert_eq!(shard.index_size_bytes(), b.total());
        assert_eq!(shard.size_bytes(), b.total());
    }

    #[test]
    fn commit_is_durable_tantivy_then_redb() {
        // Store-less ingest collapses to the two-phase ordering: the Tantivy commit is
        // the durable point, then the redb checkpoint txn lands last.
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_empty(tmp.path());
        IndexWriter::write(&shard, &batch()).unwrap();
        assert_eq!(
            shard.take_commit_trace(),
            vec!["tantivy_commit", "redb_commit"]
        );
    }

    #[test]
    fn source_uuid_records_and_reads_back_for_the_lineage_guard() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();
        // A fresh shard has no recorded lineage → the guard can't (and won't) check it: never a
        // false alarm on an index with no recorded lineage.
        assert_eq!(shard.source_uuid().unwrap(), None);
        // Recording the source table-uuid round-trips; re-recording (e.g. on reindex) is idempotent.
        shard.set_source_uuid("aaaaaaaa-1111").unwrap();
        assert_eq!(
            shard.source_uuid().unwrap().as_deref(),
            Some("aaaaaaaa-1111")
        );
        shard.set_source_uuid("bbbbbbbb-2222").unwrap();
        assert_eq!(
            shard.source_uuid().unwrap().as_deref(),
            Some("bbbbbbbb-2222")
        );
    }

    #[test]
    fn commit_appends_a_generation_and_advances_the_checkpoint() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();

        let snap = IndexWriter::write(&shard, &batch()).unwrap();
        assert_eq!(snap, Snapshot(1));

        // The committed docs are searchable.
        assert_eq!(shard.num_docs().unwrap(), 2);
        let q = Query::parse("body:brown").unwrap();
        assert_eq!(shard.search_all(&q, 10).unwrap().len(), 2);

        // Checkpoint advanced to the committed batch's source checkpoint.
        assert_eq!(
            shard.current_checkpoint().unwrap(),
            Some(SourceCheckpoint::iceberg(100))
        );
    }

    #[test]
    fn second_commit_appends_a_segment_searchable_alongside_the_first() {
        // Two distinct commits accumulate; both are searchable.
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();

        IndexWriter::write(&shard, &batch()).unwrap(); // gen 1: ids 1,2 ("brown")
        let snap2 = IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![located(3, "a brown bear")],
                SourceCheckpoint::iceberg(200),
                "b2",
            ),
        )
        .unwrap();
        assert_eq!(snap2, Snapshot(2));
        assert_eq!(shard.num_docs().unwrap(), 3);

        // "brown" spans both generations → all three docs.
        let hits = shard
            .search_all(&Query::parse("body:brown").unwrap(), 10)
            .unwrap();
        assert_eq!(ids(&hits), vec![1, 2, 3]);
    }

    #[test]
    fn re_committing_same_batch_is_a_noop() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();

        assert_eq!(IndexWriter::write(&shard, &batch()).unwrap(), Snapshot(1));
        // Same batch_id again → no-op: no new generation, same snapshot.
        assert_eq!(IndexWriter::write(&shard, &batch()).unwrap(), Snapshot(1));
        assert_eq!(shard.num_docs().unwrap(), 2);
    }

    #[test]
    fn empty_before_any_commit() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &index())
            .unwrap();
        assert_eq!(shard.num_docs().unwrap(), 0);
        assert_eq!(shard.current_snapshot().unwrap(), 0);
    }

    // ---- partition-scoped reconciliation (equality-delete fallback) ----

    /// An index keyed by (partition `region`, identifier `id`).
    fn partitioned_index() -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("region", SourceType::String),
                SourceField::new("id", SourceType::Long),
                SourceField::new("body", SourceType::String),
            ],
            vec!["region".into()],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: ALL }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn pkey(region: &str, id: i64) -> CompositeKey {
        CompositeKey::new(
            vec![("region".into(), region.into())],
            vec![("id".into(), id.into())],
        )
    }

    fn plocated(region: &str, id: i64, body: &str) -> LocatedDoc {
        let mut fields = BTreeMap::new();
        fields.insert("region".to_string(), region.into());
        fields.insert("id".to_string(), id.into());
        fields.insert("body".to_string(), body.into());
        LocatedDoc {
            doc: Document::new(pkey(region, id), fields),
        }
    }

    #[test]
    fn reconcile_partition_drops_absent_keys_within_partition_only() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &partitioned_index())
            .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![
                    plocated("us", 1, "alpha"),
                    plocated("us", 2, "beta"),
                    plocated("eu", 3, "gamma"),
                ],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();

        // A fresh scan of region=us finds only id 1 live (id 2 was equality-deleted
        // by a non-key predicate). Reconcile us → id 2 dropped; id 1 and the eu
        // partition untouched.
        let removed = shard
            .reconcile_partition(&[("region".into(), "us".into())], &[pkey("us", 1)], None)
            .unwrap()
            .deleted;
        assert_eq!(removed, 1, "only the stale us key is removed");

        assert!(shard.contains_key(&pkey("us", 1)).unwrap(), "live key kept");
        assert!(
            !shard.contains_key(&pkey("us", 2)).unwrap(),
            "stale key dropped"
        );
        assert!(
            shard.contains_key(&pkey("eu", 3)).unwrap(),
            "other partition untouched"
        );

        // Idempotent: reconciling again with the same live set is a no-op.
        let again = shard
            .reconcile_partition(&[("region".into(), "us".into())], &[pkey("us", 1)], None)
            .unwrap()
            .deleted;
        assert_eq!(again, 0);
    }

    #[test]
    fn reconcile_empty_partition_covers_whole_index() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_committed(tmp.path()); // unpartitioned: ids 1, 2 live
                                                // A whole-index scan finds only id 1 → id 2 reconciled away.
        let removed = shard
            .reconcile_partition(&[], &[key(1)], None)
            .unwrap()
            .deleted;
        assert_eq!(removed, 1);
        assert_eq!(search(&shard, "body:brown"), vec![1]);
    }

    #[test]
    fn reconcile_skips_deletes_when_checkpoint_advanced_during_scan() {
        // TOCTOU guard: if the shard's checkpoint advanced since the source scan that
        // produced the live-key set, a concurrently-ingested key would look "stale" — so the guard
        // skips the deletes rather than dropping a legitimately newer row.
        let tmp = tempfile::tempdir().unwrap();
        let shard = open_committed(tmp.path()); // ids 1,2 live at checkpoint 100
        let current = shard.current_checkpoint().unwrap();
        assert_eq!(current, Some(SourceCheckpoint::iceberg(100)));

        // Empty live set ⇒ everything is "stale". With a MISMATCHED expected checkpoint (the shard
        // appears to have advanced under the scan), the guard skips the delete — nothing removed.
        let stale = SourceCheckpoint::iceberg(999_999);
        let out = shard.reconcile_partition(&[], &[], Some(&stale)).unwrap();
        assert!(
            out.skipped_concurrent_write,
            "guard fires on checkpoint mismatch"
        );
        assert_eq!(out.deleted, 0);
        assert!(
            shard.contains_key(&key(1)).unwrap(),
            "no key deleted under the guard"
        );
        assert!(shard.contains_key(&key(2)).unwrap());

        // With the CURRENT checkpoint (no advance), the same reconcile deletes the stale keys.
        let out = shard
            .reconcile_partition(&[], &[], current.as_ref())
            .unwrap();
        assert!(!out.skipped_concurrent_write);
        assert_eq!(
            out.deleted, 2,
            "both stale keys removed once the checkpoint matches"
        );
        assert!(!shard.contains_key(&key(1)).unwrap());
        assert!(!shard.contains_key(&key(2)).unwrap());
    }

    // ---- live-key enumeration under delete debt: the live-key set comes from the
    // term dictionary + per-term liveness.
    // The store runs NoMergePolicy, so a deleted/superseded doc's key term stays in
    // the dictionary until compaction — raw enumeration would over-report.

    /// A partitioned shard with us/{1,2}, eu/{3} committed, then us/2 deleted and
    /// us/1 superseded (updated) **without compacting** — real delete debt in every
    /// segment: the dictionary still names us/2 and two versions of us/1.
    fn shard_with_delete_debt(tmp: &std::path::Path) -> Shard {
        let store = LocalIndexStore::open(tmp).unwrap();
        let shard = store
            .create_shard(&ShardId::single("docs"), &partitioned_index())
            .unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![
                    plocated("us", 1, "alpha"),
                    plocated("us", 2, "beta"),
                    plocated("eu", 3, "gamma"),
                ],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        // Fuse into ONE segment first, so the deletes below land *inside* a surviving
        // segment (Tantivy drops a fully-deleted segment outright, which would erase
        // the debt this fixture exists to create).
        shard.compact(&CompactionPolicy::default()).unwrap();
        IndexWriter::write(
            &shard,
            &CommitBatch::new(
                vec![
                    DocOp::Delete(pkey("us", 2)),
                    DocOp::Upsert(plocated("us", 1, "alpha-v2")),
                ],
                SourceCheckpoint::iceberg(2),
                "b2",
            ),
        )
        .unwrap();
        // Prove the debt is real: deleted docs still sit unpurged in the segments.
        let health = shard.compaction_health().unwrap();
        assert!(
            health.deleted >= 2,
            "expected unmerged delete debt (old us/1 + deleted us/2), got {health:?}"
        );
        shard
    }

    #[test]
    fn key_count_excludes_deleted_and_superseded_keys_under_delete_debt() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard_with_delete_debt(tmp.path());

        // Live keys: us/1 (once, despite two versions in the dictionary) and eu/3.
        assert_eq!(shard.key_count(&[]).unwrap(), 2, "whole shard");
        assert_eq!(
            shard.key_count(&[("region".into(), "us".into())]).unwrap(),
            1,
            "deleted us/2 not counted; superseded us/1 counted once"
        );
        assert_eq!(
            shard.key_count(&[("region".into(), "eu".into())]).unwrap(),
            1,
            "partition scoping is exact (prefix range)"
        );
        assert!(!shard.contains_key(&pkey("us", 2)).unwrap());
        assert!(shard.contains_key(&pkey("us", 1)).unwrap());

        // Compaction purges the debt without changing the counts.
        shard.compact(&CompactionPolicy::default()).unwrap();
        assert_eq!(shard.key_count(&[]).unwrap(), 2);
        assert_eq!(
            shard.key_count(&[("region".into(), "us".into())]).unwrap(),
            1
        );
    }

    #[test]
    fn reconcile_does_not_see_deleted_keys_under_delete_debt() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard_with_delete_debt(tmp.path());

        // The source's live set for `us` is exactly {us/1}. The enumerated indexed
        // set must equal the live-key set: the deleted-but-unmerged us/2 must NOT be
        // enumerated (it would show up as "stale" and inflate `removed`).
        let removed = shard
            .reconcile_partition(&[("region".into(), "us".into())], &[pkey("us", 1)], None)
            .unwrap()
            .deleted;
        assert_eq!(
            removed, 0,
            "nothing stale: the deleted key was not enumerated"
        );

        // An empty live set removes exactly the one live key — the superseded us/1
        // is one key, not two versions.
        let removed = shard
            .reconcile_partition(&[("region".into(), "us".into())], &[], None)
            .unwrap()
            .deleted;
        assert_eq!(removed, 1, "exactly the live us/1, counted once");
        assert!(!shard.contains_key(&pkey("us", 1)).unwrap());
        assert!(
            shard.contains_key(&pkey("eu", 3)).unwrap(),
            "other partition untouched"
        );
    }
}

#[cfg(test)]
mod compact_tests {
    use super::*;
    use growlerdb_core::{
        IndexDefinition, LocatedDoc, SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;

    fn shard(tmp: &std::path::Path) -> Shard {
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
        LocalIndexStore::open(tmp)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap()
    }

    fn put(shard: &Shard, id: &str, body: &str, snap: i64, batch: &str) {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("body".to_string(), Value::from(body));
        let doc = LocatedDoc {
            doc: Document::new(key, f),
        };
        IndexWriter::write(
            shard,
            &CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(snap), batch),
        )
        .unwrap();
    }

    fn search_ids(shard: &Shard, q: &str) -> Vec<String> {
        let mut ids: Vec<String> = shard
            .search_all(&Query::parse(q).unwrap(), 10)
            .unwrap()
            .iter()
            .map(|h| h.key.get("id").unwrap().to_index_string())
            .collect();
        ids.sort();
        ids
    }

    #[test]
    fn select_tiered_merge_picks_smallest_tier_and_bounds_the_group() {
        use tantivy::index::SegmentId as Sid;
        let id = Sid::generate_random;

        // TIER_RATIO=4: docs 1..3 → tier 0, 4..15 → tier 1, 16..63 → tier 2, … Two tier-0 segments
        // plus lone larger ones: the smallest tier with ≥2 (tier 0) is merged.
        let (a, b) = (id(), id());
        let segs = vec![(a, 2u64), (b, 3), (id(), 20), (id(), 500)];
        let g = select_tiered_merge(&segs, 8);
        assert_eq!(g.len(), 2);
        assert!(
            g.contains(&a) && g.contains(&b),
            "merged the two smallest same-tier segments"
        );

        // merge_factor caps the group: 10 same-tier segments, factor 4 → merge exactly 4 (bounded).
        let many: Vec<_> = (0..10).map(|_| (id(), 2u64)).collect();
        assert_eq!(select_tiered_merge(&many, 4).len(), 4);

        // One segment per tier (already tiered) → nothing to merge; and ≤1 segment → empty.
        assert!(select_tiered_merge(&[(id(), 2u64), (id(), 20), (id(), 200)], 8).is_empty());
        assert!(select_tiered_merge(&[(id(), 5u64)], 8).is_empty());
        assert!(select_tiered_merge(&[], 8).is_empty());
    }

    #[test]
    fn compact_fuses_segments_and_purges_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());

        // Single-doc commits accumulate as distinct segments (auto-merge is disabled).
        put(&shard, "a", "aaa", 1, "b1");
        put(&shard, "b", "bbb", 2, "b2");
        put(&shard, "c", "ccc", 3, "b3");
        put(&shard, "d", "ddd", 4, "b4");
        put(&shard, "a", "arev", 5, "b5"); // supersede a (its single-doc segment is dropped)

        assert!(shard.segment_count().unwrap() >= 4);
        assert_eq!(shard.num_docs().unwrap(), 4); // live: a(rev), b, c, d
        assert!(search_ids(&shard, "body:aaa").is_empty()); // superseded
        assert_eq!(search_ids(&shard, "body:arev"), vec!["a"]);

        shard.compact(&CompactionPolicy::default()).unwrap();

        // One fused segment; identical live result set, with superseded bytes reclaimed.
        assert_eq!(shard.segment_count().unwrap(), 1);
        assert_eq!(shard.num_docs().unwrap(), 4);
        assert!(search_ids(&shard, "body:aaa").is_empty());
        assert_eq!(search_ids(&shard, "body:arev"), vec!["a"]);
        assert_eq!(search_ids(&shard, "body:bbb"), vec!["b"]);
        assert_eq!(search_ids(&shard, "body:ccc"), vec!["c"]);
        assert_eq!(search_ids(&shard, "body:ddd"), vec!["d"]);
    }

    #[test]
    fn compact_is_a_noop_with_one_or_zero_segments() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());
        shard.compact(&CompactionPolicy::default()).unwrap(); // empty index
        assert_eq!(shard.num_docs().unwrap(), 0);
        put(&shard, "a", "alpha", 1, "b1");
        shard.compact(&CompactionPolicy::default()).unwrap(); // one segment
        shard.compact(&CompactionPolicy::default()).unwrap(); // already merged → still fine
        assert_eq!(shard.segment_count().unwrap(), 1);
        assert_eq!(search_ids(&shard, "body:alpha"), vec!["a"]);
    }

    #[test]
    fn compaction_policy_thresholds() {
        let p = CompactionPolicy::default(); // ≥8 segments OR ≥20% deleted
        let h = |segments, max_doc, deleted| CompactionHealth {
            segments,
            max_doc,
            deleted,
        };

        // Healthy: few segments, little delete debt → leave alone.
        assert!(p.reason_to_compact(&h(3, 1000, 50)).is_none());
        // Never compacts ≤1 segment, even with heavy "deletes" (compact() is a no-op there anyway).
        assert!(p.reason_to_compact(&h(1, 100, 90)).is_none());
        assert!(p.reason_to_compact(&h(0, 0, 0)).is_none());
        // Segment-count pressure crosses the threshold.
        assert!(p
            .reason_to_compact(&h(8, 1000, 0))
            .unwrap()
            .contains("segments"));
        // Delete-debt pressure (30% ≥ 20%) below the segment threshold.
        assert!(p
            .reason_to_compact(&h(3, 1000, 300))
            .unwrap()
            .contains("deleted"));
        // Just under the delete threshold (19%) and few segments → still healthy.
        assert!(p.reason_to_compact(&h(3, 1000, 190)).is_none());
        assert_eq!(h(0, 0, 0).deleted_ratio(), 0.0);
    }

    #[test]
    fn prewarm_policy_thresholds() {
        let p = PreWarmPolicy { min_accesses: 16 };
        assert!(!p.should_promote(0), "idle cold window stays cold");
        assert!(!p.should_promote(15), "below the threshold stays cold");
        assert!(p.should_promote(16), "at the threshold → promote");
        assert!(p.should_promote(100), "well over → promote");
        // min_accesses == 0 disables pre-warm entirely.
        let off = PreWarmPolicy { min_accesses: 0 };
        assert!(!off.should_promote(1_000_000));
    }

    #[test]
    fn compaction_health_aggregates_segment_metadata() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());
        // Single-doc commits accumulate as distinct segments (auto-merge is disabled) — segment
        // count is the fragmentation signal the policy keys on.
        put(&shard, "a", "alpha", 1, "b1");
        put(&shard, "b", "beta", 2, "b2");
        put(&shard, "c", "gamma", 3, "b3");

        // `compaction_health` faithfully aggregates the committed segment metadata.
        let h = shard.compaction_health().unwrap();
        let segs = shard.sealed_segments().unwrap();
        assert_eq!(h.segments, shard.segment_count().unwrap());
        assert_eq!(
            h.max_doc,
            segs.iter().map(|s| s.max_doc as u64).sum::<u64>()
        );
        assert_eq!(
            h.deleted,
            segs.iter().map(|s| s.num_deleted_docs as u64).sum::<u64>()
        );
        assert!(h.segments >= 3, "single-doc commits fragment: {h:?}");

        // Compaction fuses everything into one segment with no delete debt.
        shard.compact(&CompactionPolicy::default()).unwrap();
        let h = shard.compaction_health().unwrap();
        assert_eq!(h.segments, 1);
        assert_eq!(h.deleted, 0);
    }

    #[test]
    fn compaction_is_pit_safe() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());

        put(&shard, "a", "alphaold", 1, "b1");
        put(&shard, "b", "betakeep", 2, "b2");
        let pit = shard.open_pit().unwrap();

        // Supersede a + add c, then compact — while the PIT is open.
        put(&shard, "a", "alphanew", 3, "b3");
        put(&shard, "c", "gammanew", 4, "b4");
        shard.compact(&CompactionPolicy::default()).unwrap();
        assert_eq!(shard.segment_count().unwrap(), 1);

        // The PIT still sees the as-of-open world (its held searcher kept those segments
        // alive through the merge): old a is present, c is not.
        let pit_search = |q: &str| {
            let (hits, _) = shard
                .search_page_pit(pit.id, &Query::parse(q).unwrap(), 10, &[], 0, None)
                .unwrap();
            hits.iter()
                .map(|h| h.key.get("id").unwrap().to_index_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(pit_search("body:alphaold"), vec!["a"]); // old version still visible
        assert!(pit_search("body:gammanew").is_empty()); // c committed after the PIT

        // A fresh search reflects the compacted state, with the old version purged.
        assert_eq!(search_ids(&shard, "body:alphanew"), vec!["a"]);
        assert!(search_ids(&shard, "body:alphaold").is_empty()); // old a purged on merge
        assert_eq!(search_ids(&shard, "body:gammanew"), vec!["c"]);
        assert_eq!(search_ids(&shard, "body:betakeep"), vec!["b"]);
    }

    #[test]
    fn sealed_segments_enumerate_files_and_track_deletes() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = shard(tmp.path());
        assert!(shard.sealed_segments().unwrap().is_empty()); // nothing committed yet

        // Four docs (each commit's tiny batch lands in its own segment under the
        // multi-threaded writer), then fuse them into one multi-doc segment.
        put(&shard, "a", "aaa", 1, "b1");
        put(&shard, "b", "bbb", 2, "b2");
        put(&shard, "c", "ccc", 3, "b3");
        put(&shard, "d", "ddd", 4, "b4");
        shard.compact(&CompactionPolicy::default()).unwrap();
        let segs = shard.sealed_segments().unwrap();
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].num_docs, 4);
        assert_eq!(segs[0].num_deleted_docs, 0);

        // Supersede a key *inside* that fused segment: it stays alive (b, c, d) carrying
        // one delete, while the new version lands in a fresh segment.
        put(&shard, "a", "arev", 5, "b5");
        let segs = shard.sealed_segments().unwrap();
        assert_eq!(segs.len() as u64, shard.segment_count().unwrap());
        assert_eq!(segs.len(), 2);

        // Exactly one delete is tracked across the sealed segments; live count matches.
        let deleted: u32 = segs.iter().map(|s| s.num_deleted_docs).sum();
        assert_eq!(deleted, 1); // old a, superseded within the fused segment
        let live: u32 = segs.iter().map(|s| s.num_docs).sum();
        assert_eq!(u64::from(live), shard.num_docs().unwrap()); // arev, b, c, d

        // Ids are unique and every listed file is a real, relative path under index_dir.
        let mut ids: Vec<&str> = segs.iter().map(|s| s.id.as_str()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 2);
        for seg in &segs {
            assert!(!seg.files.is_empty());
            for f in &seg.files {
                assert!(f.is_relative());
                assert!(shard.index_dir().join(f).exists(), "missing {f:?}");
            }
        }

        // Compaction purges the delete into one sealed, delete-free segment.
        shard.compact(&CompactionPolicy::default()).unwrap();
        let segs = shard.sealed_segments().unwrap();
        assert_eq!(segs.len(), 1);
        assert_eq!(segs[0].num_deleted_docs, 0);
        assert_eq!(segs[0].max_doc, segs[0].num_docs);
        assert_eq!(segs[0].num_docs, 4);
    }
}

#[cfg(test)]
mod reindex_tests {
    use super::*;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
        SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;

    fn resolved() -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn write(shard: &Shard, ids: &[&str], snap: i64) {
        let docs: Vec<LocatedDoc> = ids
            .iter()
            .map(|id| {
                let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(*id))]);
                let mut f = BTreeMap::new();
                f.insert("id".to_string(), Value::from(*id));
                f.insert("body".to_string(), Value::from("doc"));
                LocatedDoc {
                    doc: Document::new(key, f),
                }
            })
            .collect();
        IndexWriter::write(
            shard,
            &CommitBatch::from_upserts(docs, SourceCheckpoint::iceberg(snap), format!("b{snap}")),
        )
        .unwrap();
    }

    #[test]
    fn reindex_rebuilds_promotes_and_is_durable() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let id = ShardId::single("docs");
        let resolved = resolved();

        // Original index has a, b.
        let shard = store.create_shard(&id, &resolved).unwrap();
        write(&shard, &["a", "b"], 1);
        assert_eq!(shard.num_docs().unwrap(), 2);
        drop(shard);

        // Reindex to a fresh document set (c, d, e), populated via the closure.
        let promoted = store
            .reindex(&id, &resolved, |s| {
                write(s, &["c", "d", "e"], 2);
                Ok(())
            })
            .unwrap();
        assert_eq!(promoted.num_docs().unwrap(), 3);
        drop(promoted);

        // Durable: reopening from disk sees the reindexed set; staging/backup dirs are gone.
        let reopened = store.open_shard(&id, &resolved).unwrap();
        assert_eq!(reopened.num_docs().unwrap(), 3);
        let canonical = tmp.path().join("docs").join("0");
        assert!(canonical.exists());
        assert!(!sibling(&canonical, "reindex").exists());
        assert!(!sibling(&canonical, "old").exists());
    }

    #[test]
    fn two_phase_build_then_promote_matches_reindex() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let id = ShardId::single("docs");
        let resolved = resolved();
        let shard = store.create_shard(&id, &resolved).unwrap();
        write(&shard, &["a", "b"], 1);
        drop(shard);

        // Build the next generation without cutting over — the live shard still serves the old set.
        store
            .build_staging_generation(&id, &resolved, |s| {
                write(s, &["c", "d", "e"], 2);
                Ok(())
            })
            .unwrap();
        let canonical = tmp.path().join("docs").join("0");
        assert!(sibling(&canonical, "reindex").exists(), "staging built");
        assert!(
            !sibling(&canonical, "commit").exists(),
            "no commit marker before promote"
        );
        let live = store.open_shard(&id, &resolved).unwrap();
        assert_eq!(
            live.num_docs().unwrap(),
            2,
            "old generation still serves pre-cutover"
        );
        drop(live);

        // Cut over.
        let promoted = store.promote_generation(&id, &resolved).unwrap();
        assert_eq!(promoted.num_docs().unwrap(), 3);
        drop(promoted);
        let reopened = store.open_shard(&id, &resolved).unwrap();
        assert_eq!(reopened.num_docs().unwrap(), 3);
        assert!(!sibling(&canonical, "reindex").exists());
        assert!(!sibling(&canonical, "old").exists());
        assert!(!sibling(&canonical, "commit").exists());
    }

    #[test]
    fn discard_staging_leaves_the_live_generation_untouched() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let id = ShardId::single("docs");
        let resolved = resolved();
        let shard = store.create_shard(&id, &resolved).unwrap();
        write(&shard, &["a", "b"], 1);
        drop(shard);

        store
            .build_staging_generation(&id, &resolved, |s| {
                write(s, &["c", "d", "e"], 2);
                Ok(())
            })
            .unwrap();
        store.discard_staging(&id).unwrap();

        let canonical = tmp.path().join("docs").join("0");
        assert!(
            !sibling(&canonical, "reindex").exists(),
            "staging discarded"
        );
        let reopened = store.open_shard(&id, &resolved).unwrap();
        assert_eq!(reopened.num_docs().unwrap(), 2, "old generation intact");
    }

    #[test]
    fn recover_discards_a_built_but_unpromoted_generation() {
        // A crash after build_staging but before promote: no commit marker, so recovery treats the
        // staging as a torn attempt and discards it — the old generation stays authoritative and the
        // reindex is retried. This is why build_staging must not auto-promote.
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let id = ShardId::single("docs");
        let resolved = resolved();
        let shard = store.create_shard(&id, &resolved).unwrap();
        write(&shard, &["a", "b"], 1);
        drop(shard);

        store
            .build_staging_generation(&id, &resolved, |s| {
                write(s, &["c", "d", "e"], 2);
                Ok(())
            })
            .unwrap();
        let canonical = tmp.path().join("docs").join("0");
        assert!(sibling(&canonical, "reindex").exists());

        store.recover_reindex(&id).unwrap();
        assert!(
            !sibling(&canonical, "reindex").exists(),
            "uncommitted staging discarded on recovery"
        );
        let reopened = store.open_shard(&id, &resolved).unwrap();
        assert_eq!(reopened.num_docs().unwrap(), 2, "old generation preserved");
    }

    #[test]
    fn recover_restores_an_interrupted_swap() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let id = ShardId::single("docs");
        let resolved = resolved();
        let shard = store.create_shard(&id, &resolved).unwrap();
        write(&shard, &["a", "b"], 1);
        drop(shard);

        // Simulate a crash right after `rename(canonical → backup)`: the commit marker is
        // present (written before the swap), the canonical path is gone, the backup holds the
        // index, and staging is gone. Recovery's safety net restores the backup.
        let canonical = tmp.path().join("docs").join("0");
        let backup = sibling(&canonical, "old");
        let marker = sibling(&canonical, "commit");
        std::fs::write(&marker, b"x").unwrap();
        std::fs::rename(&canonical, &backup).unwrap();
        assert!(!canonical.exists());

        store.recover_reindex(&id).unwrap();
        assert!(canonical.exists()); // restored
        assert!(!backup.exists()); // cleaned
        assert!(!marker.exists()); // cleaned
        let reopened = store.open_shard(&id, &resolved).unwrap();
        assert_eq!(reopened.num_docs().unwrap(), 2);
    }

    // --- crash-window recovery: hand-build each on-disk state and assert recovery ---

    fn tag_dir(dir: &Path, tag: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("tag"), tag).unwrap();
    }

    fn tag_of(dir: &Path) -> String {
        std::fs::read_to_string(dir.join("tag")).unwrap()
    }

    /// Build an on-disk reindex state via `setup`, run `recover_reindex`, assert staging/backup/
    /// marker are cleaned up, and return the canonical index's tag (None if it's absent). Uses
    /// plain tagged dirs (recovery only renames/removes by existence + marker, no Tantivy).
    fn recover_and_tag(setup: impl FnOnce(&Path, &Path, &Path, &Path)) -> Option<String> {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let id = ShardId::single("docs");
        let canonical = tmp.path().join("docs").join("0");
        std::fs::create_dir_all(canonical.parent().unwrap()).unwrap();
        let staging = sibling(&canonical, "reindex");
        let backup = sibling(&canonical, "old");
        let marker = sibling(&canonical, "commit");
        setup(&canonical, &staging, &backup, &marker);

        store.recover_reindex(&id).unwrap();

        assert!(!staging.exists(), "staging not cleaned up");
        assert!(!backup.exists(), "backup not cleaned up");
        assert!(!marker.exists(), "marker not cleaned up");
        canonical.exists().then(|| tag_of(&canonical))
    }

    #[test]
    fn recover_rolls_forward_a_committed_unswapped_reindex() {
        // Marker present, canonical=old, staging=new, no backup (crash before the first rename).
        let tag = recover_and_tag(|canonical, staging, _backup, marker| {
            tag_dir(canonical, "old");
            tag_dir(staging, "new");
            std::fs::write(marker, b"x").unwrap();
        });
        assert_eq!(tag.as_deref(), Some("new")); // staging promoted
    }

    #[test]
    fn recover_completes_a_swap_interrupted_after_the_first_rename() {
        // Marker present, canonical gone, staging=new, backup=old (crash between the two renames).
        let tag = recover_and_tag(|_canonical, staging, backup, marker| {
            tag_dir(staging, "new");
            tag_dir(backup, "old");
            std::fs::write(marker, b"x").unwrap();
        });
        assert_eq!(tag.as_deref(), Some("new")); // staging promoted
    }

    #[test]
    fn recover_keeps_the_new_index_after_a_full_swap() {
        // The bug case: marker present, canonical=new, backup=old, no staging (crash after the
        // second rename). Recovery must keep the NEW canonical and drop the backup — never
        // restore the stale backup over it or delete the only good copy.
        let tag = recover_and_tag(|canonical, _staging, backup, marker| {
            tag_dir(canonical, "new");
            tag_dir(backup, "old");
            std::fs::write(marker, b"x").unwrap();
        });
        assert_eq!(tag.as_deref(), Some("new"));
    }

    #[test]
    fn recover_rolls_back_a_torn_precommit_reindex() {
        // No marker: a torn attempt (staging half-built before the commit). Keep the canonical
        // old index; discard the staging — never promote an uncommitted dir.
        let tag = recover_and_tag(|canonical, staging, _backup, _marker| {
            tag_dir(canonical, "old");
            tag_dir(staging, "partial");
        });
        assert_eq!(tag.as_deref(), Some("old"));
    }

    #[test]
    fn recover_restores_the_backup_when_canonical_is_missing() {
        // Safety net: marker present but neither canonical nor staging exist — restore the old
        // backup rather than leave the shard with no index.
        let tag = recover_and_tag(|_canonical, _staging, backup, marker| {
            tag_dir(backup, "old");
            std::fs::write(marker, b"x").unwrap();
        });
        assert_eq!(tag.as_deref(), Some("old"));
    }
}

#[cfg(test)]
mod window_store_tests {
    use super::*;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, LocatedDoc, SourceCheckpoint,
        SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;

    const DAY: i64 = 86_400_000_000; // one day in **micros** (the canonical window scale)

    fn events_index() -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("ingest", SourceType::Long),
                SourceField::new("event", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            "name: events\nsource: { iceberg: { catalog: g, table: g.events } }\nwindowing: { field: ingest, granularity: daily, event_time_field: event }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: ingest, format: epoch_us, fast: true }, { path: event, format: epoch_us, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn doc(id: &str, ingest: i64, event: i64) -> DocOp {
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("ingest".to_string(), Value::Int(ingest));
        f.insert("event".to_string(), Value::Int(event));
        DocOp::Upsert(LocatedDoc {
            doc: Document::new(
                CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]),
                f,
            ),
        })
    }

    #[test]
    fn windowed_write_routes_to_window_shards_with_event_bounds_and_broadcast_delete() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let idx = events_index();
        let day = |n: i64| n * DAY;

        let batch = CommitBatch::new(
            vec![
                doc("d1", day(10) + 5, day(10)), // ingest win 10, event day 10
                doc("d2", day(10) + 9, day(2)),  // ingest win 10, event day 2 (late by 8d)
                doc("d3", day(11) + 1, day(11)), // ingest win 11
                DocOp::Delete(CompositeKey::new(
                    vec![],
                    vec![("id".into(), Value::from("d1"))],
                )),
            ],
            SourceCheckpoint::iceberg(1),
            "b1",
        );
        let written = store.write_windowed(&idx, &batch).unwrap();
        assert_eq!(written, vec![day(10), day(11)], "two ingest windows");
        assert_eq!(
            store.window_shards("events").unwrap(),
            vec![day(10), day(11)]
        );

        // Window 10: d1 was upserted then the broadcast delete removed it → only d2 remains.
        let w10 = store
            .create_shard(&ShardId::window("events", day(10)), &idx)
            .unwrap();
        assert_eq!(w10.num_docs().unwrap(), 1, "d1 deleted, d2 remains");
        assert!(
            w10.search_all(&growlerdb_core::Query::parse("id:d2").unwrap(), 10)
                .unwrap()
                .len()
                == 1
        );
        // The late event widened window 10's zone-map down to day 2 — and a delete doesn't shrink
        // it (zone-maps are conservative: may over-include, never under).
        assert_eq!(w10.event_bounds().unwrap(), Some((day(2), day(10))));

        // Window 11: just d3.
        let w11 = store
            .create_shard(&ShardId::window("events", day(11)), &idx)
            .unwrap();
        assert_eq!(w11.num_docs().unwrap(), 1);
        assert_eq!(w11.event_bounds().unwrap(), Some((day(11), day(11))));
    }

    #[test]
    fn ordinal_shards_enumerate_pool_ordinals_at_distinct_paths() {
        // D52 pool: one node can hold several ORDINAL shards of a hash index, each at its own path.
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        store
            .create_shard(&ShardId::shard("docs", 0), &idx)
            .unwrap();
        store
            .create_shard(&ShardId::shard("docs", 2), &idx)
            .unwrap();
        // Ordinal 0 is the single-shard path; distinct ordinals get distinct dirs (no collision).
        assert_eq!(
            store.shard_path(&ShardId::shard("docs", 0)),
            store.shard_path(&ShardId::single("docs"))
        );
        assert_ne!(
            store.shard_path(&ShardId::shard("docs", 0)),
            store.shard_path(&ShardId::shard("docs", 2))
        );
        assert_eq!(store.ordinal_shards("docs").unwrap(), vec![0, 2]);
        // A window dir (`w{...}`) is not mistaken for an ordinal shard.
        store
            .create_shard(&ShardId::window("docs", 999), &idx)
            .unwrap();
        assert_eq!(
            store.ordinal_shards("docs").unwrap(),
            vec![0, 2],
            "window dirs are excluded from ordinal enumeration"
        );
    }

    #[test]
    fn search_windowed_prunes_and_merges_across_windows() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let idx = events_index();
        let day = |n: i64| n * DAY;
        let parse = |q: &str| growlerdb_core::Query::parse(q).unwrap();
        let ids = |hits: &[Hit]| {
            let mut v: Vec<String> = hits
                .iter()
                .map(|h| match h.key.get("id").unwrap() {
                    Value::Str(s) => s.clone(),
                    _ => unreachable!(),
                })
                .collect();
            v.sort();
            v
        };

        store
            .write_windowed(
                &idx,
                &CommitBatch::new(
                    vec![
                        doc("d1", day(10) + 5, day(10)),
                        doc("d2", day(10) + 9, day(2)), // late: event 8d before ingest
                        doc("d3", day(11) + 1, day(11)),
                        doc("d4", day(20), day(20)),
                    ],
                    SourceCheckpoint::iceberg(1),
                    "b1",
                ),
            )
            .unwrap();

        // No range filter → fan out to every window, merged.
        let all = store
            .search_windowed(&idx, &parse("id:d1 OR id:d3 OR id:d4"), 10)
            .unwrap();
        assert_eq!(ids(&all), vec!["d1", "d3", "d4"]);

        // Ingest range inside window 11 → only that window contributes.
        let q = format!(
            "(id:d1 OR id:d3 OR id:d4) AND ingest:[{} TO {}]",
            day(11),
            day(11) + 100
        );
        assert_eq!(
            ids(&store.search_windowed(&idx, &parse(&q), 10).unwrap()),
            vec!["d3"]
        );

        // Late-data query by EVENT time: d2 (event day 2) is found in ingest-window 10 via that
        // window's widened event zone-map; d1 (event day 10) is filtered out by the event range.
        let qe = format!("(id:d1 OR id:d2) AND event:[{} TO {}]", day(2), day(3));
        assert_eq!(
            ids(&store.search_windowed(&idx, &parse(&qe), 10).unwrap()),
            vec!["d2"]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_cold_shard_serves_search_read_through_with_local_aux() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let idx = events_index();
        let w = 10 * DAY;
        let id = ShardId::window("events", w);

        // Build a window shard with docs, capture its hits, then drop it (release redb + tantivy).
        {
            let shard = store.create_shard(&id, &idx).unwrap();
            IndexWriter::write(
                &shard,
                &CommitBatch::new(
                    vec![doc("d1", w + 1, w), doc("d2", w + 2, w)],
                    SourceCheckpoint::iceberg(1),
                    "b1",
                ),
            )
            .unwrap();
            assert_eq!(
                shard
                    .search_all(&growlerdb_core::Query::parse("id:d1").unwrap(), 10)
                    .unwrap()
                    .len(),
                1
            );
        }

        // Park the bulk: copy the tantivy `index/` files into an object store, evict the local
        // copy, keep `aux.redb` (the cold footprint).
        let window_dir = store.shard_path(&id);
        let index_dir = window_dir.join("index");
        let store_root = tempfile::tempdir().unwrap();
        let cold = store_root.path().join("cold");
        std::fs::create_dir_all(&cold).unwrap();
        for entry in std::fs::read_dir(&index_dir).unwrap() {
            let e = entry.unwrap();
            if e.file_type().unwrap().is_file() {
                std::fs::copy(e.path(), cold.join(e.file_name())).unwrap();
            }
        }
        std::fs::remove_dir_all(&index_dir).unwrap();
        assert!(!index_dir.exists(), "tantivy bulk evicted locally");

        let op = opendal::Operator::new(
            opendal::services::Fs::default().root(&store_root.path().to_string_lossy()),
        )
        .unwrap();
        let cache = crate::range_cache::RangeCache::new(8 * 1024 * 1024);
        let aux_dir = window_dir.clone();
        let counts = tokio::task::spawn_blocking(move || {
            let cold = store
                .open_cold_shard(&idx, &aux_dir, op, "cold", cache, None, None, None)
                .unwrap();
            let d1 = cold
                .search_all(&growlerdb_core::Query::parse("id:d1").unwrap(), 10)
                .unwrap()
                .len();
            let d2 = cold
                .search_all(&growlerdb_core::Query::parse("id:d2").unwrap(), 10)
                .unwrap()
                .len();
            // Write-path calls against the read-only cold shard are refused cleanly —
            // never a panic (late data / an admin call can still reach a parked window).
            let write_err = IndexWriter::write(
                &cold,
                &CommitBatch::new(vec![doc("d9", 0, 0)], SourceCheckpoint::iceberg(9), "b9"),
            )
            .expect_err("a cold shard must refuse writes");
            assert!(matches!(write_err, StoreError::ReadOnlyShard(_)));
            let backup_err = cold
                .backup_snapshot(std::path::Path::new("/nonexistent-staging"))
                .expect_err("a cold shard must refuse backup");
            assert!(matches!(backup_err, StoreError::ReadOnlyShard(_)));
            (d1, d2)
        })
        .await
        .unwrap();
        assert_eq!(
            counts,
            (1, 1),
            "cold shard searches read-through from object storage with local aux"
        );
    }

    #[test]
    fn windowed_batches_prune_idempotency_records_at_the_safe_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let store = LocalIndexStore::open(tmp.path()).unwrap();
        let idx = events_index();
        let day = |n: i64| n * DAY;
        let w10 = format!("b1#w{}", day(10));

        // Batch 1 (no floor yet): its idempotency record is retained on the window shard.
        store
            .write_windowed(
                &idx,
                &CommitBatch::new(
                    vec![doc("d1", day(10) + 1, day(10))],
                    SourceCheckpoint::iceberg_ordered(101, 1),
                    "b1",
                ),
            )
            .unwrap();
        {
            // Scoped: redb allows one open handle per file, and batch 2 reopens this shard.
            let shard = store
                .create_shard(&ShardId::window("events", day(10)), &idx)
                .unwrap();
            assert!(shard.batch_snapshot(&w10).unwrap().is_some());
        }

        // Batch 2 carries the connector's resume floor (≥ batch 1's checkpoint): the window
        // sub-batch propagates it, so batch 1's record prunes. Without the carry, windowed
        // shards' idempotency tables grew without bound.
        store
            .write_windowed(
                &idx,
                &CommitBatch::new(
                    vec![doc("d2", day(10) + 2, day(10))],
                    SourceCheckpoint::iceberg_ordered(102, 2),
                    "b2",
                )
                .with_safe_checkpoint(Some(SourceCheckpoint::iceberg_ordered(101, 1))),
            )
            .unwrap();
        let shard = store
            .create_shard(&ShardId::window("events", day(10)), &idx)
            .unwrap();
        assert!(
            shard.batch_snapshot(&w10).unwrap().is_none(),
            "the record at the floor is pruned"
        );
        assert!(
            shard
                .batch_snapshot(&format!("b2#w{}", day(10)))
                .unwrap()
                .is_some(),
            "the batch above the floor is retained"
        );
    }
}

#[cfg(test)]
mod ann_tests {
    use super::*;
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, LocatedDoc, Query, SourceCheckpoint,
        SourceField, SourceSchema, SourceType, Value,
    };
    use std::collections::BTreeMap;

    /// A `docs` shard with a VECTOR field (`body_vec`, dims 3, cosine).
    fn vector_shard(tmp: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: body_vec, type: VECTOR, vector: { dims: 3, metric: COSINE, source_field: body } } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        LocalIndexStore::open(tmp)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap()
    }

    fn put_vec(shard: &Shard, id: &str, v: Vec<f32>, snap: i64) {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("body".to_string(), Value::from(id));
        f.insert("body_vec".to_string(), Value::Vector(v));
        let doc = LocatedDoc {
            doc: Document::new(key, f),
        };
        IndexWriter::write(
            shard,
            &CommitBatch::from_upserts(
                vec![doc],
                SourceCheckpoint::iceberg(snap),
                format!("b{snap}"),
            ),
        )
        .unwrap();
    }

    fn knn_ids(hits: &[Hit]) -> Vec<String> {
        hits.iter()
            .map(|h| h.key.get("id").unwrap().to_index_string())
            .collect()
    }

    #[test]
    fn knn_over_shard_returns_nearest_and_sidecar_survives_backup() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = vector_shard(tmp.path());
        put_vec(&shard, "x", vec![1.0, 0.0, 0.0], 1);
        put_vec(&shard, "y", vec![0.0, 1.0, 0.0], 2);
        put_vec(&shard, "xy", vec![0.9, 0.1, 0.0], 3);
        put_vec(&shard, "z", vec![0.0, 0.0, 1.0], 4);
        // Fuse the per-commit segments so a single merged segment (a fresh id) must rebuild its
        // ANN sidecar — exercising the compaction path.
        shard.compact(&CompactionPolicy::default()).unwrap();

        // A top-level KNN query returns the nearest documents by vector, best-first.
        let hits = shard
            .search_all(
                &Query::Knn {
                    field: "body_vec".into(),
                    vector: vec![1.0, 0.0, 0.0],
                    k: 2,
                    filter: None,
                },
                2,
            )
            .unwrap();
        assert_eq!(knn_ids(&hits), vec!["x", "xy"]);

        // Coverage pairs with `num_docs`: every doc here embedded, so the ANN sidecars hold a
        // vector for each — the describe path surfaces a shortfall (docs ingested without an
        // embedding, invisible to KNN) instead of leaving it silent (TASK-323).
        assert_eq!(
            shard.vector_coverage("body_vec").unwrap(),
            shard.num_docs().unwrap(),
            "every embedded doc counts toward KNN coverage"
        );
        assert_eq!(
            shard.vector_coverage("no_such_field").unwrap(),
            0,
            "an unknown/never-embedded field has zero coverage"
        );

        // Every current segment lists its `.ann` sidecar among its backup files.
        let segs = shard.sealed_segments().unwrap();
        let ann_files: Vec<_> = segs
            .iter()
            .flat_map(|s| s.files.iter())
            .filter(|f| f.extension().map(|e| e == "ann").unwrap_or(false))
            .collect();
        assert_eq!(ann_files.len(), 1, "the merged segment has one ANN sidecar");
        for f in &ann_files {
            assert!(shard.index_dir().join(f).exists());
        }

        // Backup carries the sidecar (registered in the sealed file set).
        let staging = tempfile::tempdir().unwrap();
        let snap = shard.backup_snapshot(staging.path()).unwrap();
        assert!(
            snap.files
                .iter()
                .any(|f| f.extension().map(|e| e == "ann").unwrap_or(false)),
            "backup file list includes the ANN sidecar"
        );

        // The backed-up bytes reconstruct a working KNN index: open the staged index dir and
        // re-run the query straight off the restored sidecar.
        let restored = TantivySegmentCore
            .open(&staging.path().join("index"))
            .unwrap();
        let restored_hits = restored
            .knn_search("body_vec", &[0.0, 0.0, 1.0], 1, None)
            .unwrap();
        assert_eq!(knn_ids(&restored_hits), vec!["z"]);
    }

    #[test]
    fn knn_respects_supersede_via_alive_bitset() {
        let tmp = tempfile::tempdir().unwrap();
        let shard = vector_shard(tmp.path());
        put_vec(&shard, "a", vec![1.0, 0.0, 0.0], 1);
        // Supersede `a` with a vector pointing the other way; the old version is deleted in its
        // segment, so KNN toward +x must NOT surface the stale embedding.
        put_vec(&shard, "a", vec![0.0, 0.0, 1.0], 2);
        let hits = shard
            .search_all(
                &Query::Knn {
                    field: "body_vec".into(),
                    vector: vec![1.0, 0.0, 0.0],
                    k: 5,
                    filter: None,
                },
                5,
            )
            .unwrap();
        // Only the live `a` remains, and its current (+z) embedding scores low against +x — but it
        // is the sole live doc, so it's the only hit (the superseded +x version is filtered out).
        assert_eq!(knn_ids(&hits), vec!["a"]);
        assert_eq!(hits.len(), 1);
    }

    /// A vector shard that also carries a `lang` KEYWORD fast field, for filtered-KNN tests.
    fn vector_shard_with_lang(tmp: &std::path::Path) -> Shard {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
                SourceField::new("lang", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT }, { path: lang, type: KEYWORD, fast: true }, { path: body_vec, type: VECTOR, vector: { dims: 3, metric: COSINE, source_field: body } } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        LocalIndexStore::open(tmp)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap()
    }

    fn put_vec_lang(shard: &Shard, id: &str, v: Vec<f32>, lang: &str, snap: i64) {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("body".to_string(), Value::from(id));
        f.insert("lang".to_string(), Value::from(lang));
        f.insert("body_vec".to_string(), Value::Vector(v));
        let doc = LocatedDoc {
            doc: Document::new(key, f),
        };
        IndexWriter::write(
            shard,
            &CommitBatch::from_upserts(
                vec![doc],
                SourceCheckpoint::iceberg(snap),
                format!("b{snap}"),
            ),
        )
        .unwrap();
    }

    #[test]
    fn filtered_knn_returns_only_docs_matching_the_filter() {
        // The nearest doc to +x is `en_near` (lang=en); the very nearest overall is `de_near`
        // (lang=de). A KNN filtered to `lang:en` must skip `de_near` and return the `en` docs even
        // though a `de` doc is closer — the filter intersects the candidate set.
        let tmp = tempfile::tempdir().unwrap();
        let shard = vector_shard_with_lang(tmp.path());
        put_vec_lang(&shard, "de_near", vec![1.0, 0.0, 0.0], "de", 1);
        put_vec_lang(&shard, "en_near", vec![0.95, 0.05, 0.0], "en", 2);
        put_vec_lang(&shard, "en_mid", vec![0.5, 0.5, 0.0], "en", 3);
        put_vec_lang(&shard, "de_far", vec![0.0, 1.0, 0.0], "de", 4);

        let knn = Query::Knn {
            field: "body_vec".into(),
            vector: vec![1.0, 0.0, 0.0],
            k: 2,
            filter: Some(Box::new(Query::Term {
                field: Some("lang".into()),
                value: "en".into(),
            })),
        };
        let hits = shard.search_all(&knn, 2).unwrap();
        assert_eq!(knn_ids(&hits), vec!["en_near", "en_mid"]);
        // The closer `de` doc was excluded by the filter.
        assert!(!knn_ids(&hits).contains(&"de_near".to_string()));
    }
}
