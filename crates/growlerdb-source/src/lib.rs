//! Source connectors for GrowlerDB: an Iceberg batch reader (current snapshot,
//! append-only — no delete handling) that tracks each batch's source data file
//! and starting row position, so the index can build a primary-key locator for
//! hydration.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::{
    Array, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeStringArray, RecordBatch, StringArray, StructArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt8Array,
};
use arrow_schema::{DataType, Fields, Schema, SchemaRef, TimeUnit};
use futures::{StreamExt, TryStreamExt};
use growlerdb_core::{
    CompositeKey, Document, HydratedRow, LocatedDoc, Projection, ResolvedIndex, RowLocator,
    SourceField, SourceSchema, SourceType, Value,
};
use iceberg::arrow::{schema_to_arrow_schema, ArrowReaderBuilder};
use iceberg::expr::{Predicate, Reference};
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    Datum, Literal, PrimitiveLiteral, PrimitiveType, Schema as IcebergSchema, Transform,
};
use iceberg::table::Table;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalog;
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalStorageFactory;

mod key_scan;
mod plan_cache;
mod point_read;
mod shared_reader;

pub use key_scan::read_file_key_rows;
pub use plan_cache::{PlanCache, PLAN_CACHE_CAP};
pub use shared_reader::SharedReader;

// The table IO handle [`read_file_key_rows`] (and [`TablePlan`]) hands around — re-exported so
// callers (the engine's re-map driver, its tests) needn't depend on the `iceberg` crate.
pub use iceberg::io::FileIO;

/// A **local-filesystem** [`FileIO`] over the same opendal storage factory the S3 path
/// uses — for reading table/data files off local disk (fixtures, tools, tests).
pub fn fs_file_io() -> FileIO {
    iceberg::io::FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build()
}

/// Errors from reading a source.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    #[error(transparent)]
    Iceberg(#[from] iceberg::Error),

    /// A targeted parquet point read failed (hydration pass 1 reads parquet directly for
    /// row-group + row-selection scoping; see [`point_read`]).
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),

    /// A locator referenced a data file absent from the current table plan
    /// (e.g. compacted away — a stale locator).
    #[error("data file not found in current table plan: {0}")]
    FileNotFound(String),

    /// A locator's row position was out of range for its data file (stale).
    #[error("row position {position} out of range in {file}")]
    RowOutOfRange {
        /// The data file path.
        file: String,
        /// The offending row position.
        position: u64,
    },

    /// The streamed-read sink (the caller's per-chunk write) failed. Carries the caller's error
    /// rendered as a string, so the source crate needn't depend on the engine's error type.
    #[error("sink: {0}")]
    Sink(String),
}

pub type Result<T> = std::result::Result<T, SourceError>;

/// Connection settings for an Iceberg REST catalog backed by S3-compatible storage.
#[derive(Debug, Clone)]
pub struct IcebergConfig {
    /// REST catalog base URI (e.g. Polaris `http://host:8181/api/catalog`).
    pub uri: String,
    /// Warehouse — for Polaris this is the **catalog name** (e.g. `growlerdb`).
    pub warehouse: String,
    /// OAuth2 client credential `client_id:secret` (Polaris), if required.
    pub credential: Option<String>,
    /// OAuth2 scope (Polaris uses `PRINCIPAL_ROLE:ALL`).
    pub scope: Option<String>,
    pub s3_endpoint: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_region: String,
}

impl IcebergConfig {
    /// Defaults matching the local dev stack (`deploy/compose`: Polaris + MinIO).
    pub fn local() -> Self {
        Self {
            uri: "http://localhost:8181/api/catalog".to_string(),
            warehouse: "growlerdb".to_string(),
            credential: Some("root:s3cr3t".to_string()),
            scope: Some("PRINCIPAL_ROLE:ALL".to_string()),
            s3_endpoint: "http://minio:9000".to_string(),
            s3_access_key: "minioadmin".to_string(),
            s3_secret_key: "minioadmin".to_string(),
            s3_region: "us-east-1".to_string(),
        }
    }

    /// As [`local`](Self::local), but each field overridable from the environment — so the same
    /// binary runs on a dev host (defaults: `localhost`/`minio`) and in a container/cluster
    /// pointed at in-network Polaris + object storage. Recognized vars (all optional):
    /// `GROWLERDB_CATALOG_URI`, `GROWLERDB_WAREHOUSE`, `GROWLERDB_CATALOG_CREDENTIAL`,
    /// `GROWLERDB_CATALOG_SCOPE`, `GROWLERDB_S3_ENDPOINT`, `GROWLERDB_S3_ACCESS_KEY`,
    /// `GROWLERDB_S3_SECRET_KEY`, `GROWLERDB_S3_REGION`. An empty value clears the optional
    /// credential/scope (anonymous catalog).
    pub fn from_env() -> Self {
        let base = Self::local();
        let var = |key: &str| std::env::var(key).ok();
        let opt = |key: &str, default: Option<String>| match std::env::var(key) {
            Ok(v) if v.is_empty() => None,
            Ok(v) => Some(v),
            Err(_) => default,
        };
        Self {
            uri: var("GROWLERDB_CATALOG_URI").unwrap_or(base.uri),
            warehouse: var("GROWLERDB_WAREHOUSE").unwrap_or(base.warehouse),
            credential: opt("GROWLERDB_CATALOG_CREDENTIAL", base.credential),
            scope: opt("GROWLERDB_CATALOG_SCOPE", base.scope),
            s3_endpoint: var("GROWLERDB_S3_ENDPOINT").unwrap_or(base.s3_endpoint),
            s3_access_key: var("GROWLERDB_S3_ACCESS_KEY").unwrap_or(base.s3_access_key),
            s3_secret_key: var("GROWLERDB_S3_SECRET_KEY").unwrap_or(base.s3_secret_key),
            s3_region: var("GROWLERDB_S3_REGION").unwrap_or(base.s3_region),
        }
    }

    fn props(&self) -> HashMap<String, String> {
        let mut p = HashMap::from([
            ("uri".to_string(), self.uri.clone()),
            ("warehouse".to_string(), self.warehouse.clone()),
            ("s3.endpoint".to_string(), self.s3_endpoint.clone()),
            ("s3.access-key-id".to_string(), self.s3_access_key.clone()),
            (
                "s3.secret-access-key".to_string(),
                self.s3_secret_key.clone(),
            ),
            ("s3.region".to_string(), self.s3_region.clone()),
            ("s3.path-style-access".to_string(), "true".to_string()),
        ]);
        if let Some(c) = &self.credential {
            p.insert("credential".to_string(), c.clone());
        }
        if let Some(s) = &self.scope {
            p.insert("scope".to_string(), s.clone());
        }
        p
    }
}

/// A record batch tagged with where its rows live in the source, for the locator.
pub struct LocatedBatch {
    pub batch: RecordBatch,
    /// Source data-file path the rows came from.
    pub data_file: String,
    /// Row position (within `data_file`) of the first row of `batch`.
    pub start_row: u64,
}

/// The result of reading a table snapshot: its Arrow schema and located batches.
pub struct ReadResult {
    pub schema: SchemaRef,
    pub batches: Vec<LocatedBatch>,
}

impl ReadResult {
    /// Total number of rows across all batches.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.batch.num_rows()).sum()
    }
}

/// Docs per chunk for the streamed read: bounds peak memory while keeping the per-chunk
/// commit count (and thus segment count) reasonable. ~50k telemetry docs ≈ a few MB.
const STREAM_CHUNK: usize = 50_000;

/// Reads Apache Iceberg tables via a REST catalog.
pub struct IcebergReader {
    catalog: RestCatalog,
    /// Snapshot-pinned plan cache for [hydration](Self::hydrate)'s pass-1 unpredicated
    /// current-snapshot plan: only effective when the reader itself is long-lived — hold it
    /// via [`SharedReader`] rather than connecting per call.
    plans: PlanCache<Arc<Vec<FileScanTask>>>,
}

impl IcebergReader {
    /// Connect to the catalog described by `cfg`.
    pub async fn connect(cfg: &IcebergConfig) -> Result<Self> {
        // Object-store retry: `OpenDalStorageFactory` wraps every operator it builds — including
        // this S3 path — in an opendal `RetryLayer` internally, so the source read path (scans +
        // hydration) already retries transient 5xx/SlowDown. No separate layer is attached here.
        // The built-in uses opendal's default retry (3 attempts, no jitter); a tuned
        // `with_max_times(4).with_jitter()` would need a hand-rolled StorageFactory, judged not
        // worth it for a single-reader-per-index source.
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
                configured_scheme: "s3".to_string(),
                customized_credential_load: None,
            }))
            .load("growlerdb", cfg.props())
            .await?;
        Ok(Self {
            catalog,
            plans: PlanCache::new(PLAN_CACHE_CAP),
        })
    }

    /// Read a table's current snapshot (append-only), returning each batch
    /// tagged with its source data file and starting row position.
    ///
    /// `table` is a dotted identifier, e.g. `growlerdb.docs`.
    pub async fn read_current(&self, table: &str) -> Result<ReadResult> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(tbl.metadata().current_schema())?);
        let tasks: Vec<FileScanTask> = tbl
            .scan()
            .select_all()
            .build()?
            .plan_files()
            .await?
            .try_collect()
            .await?;
        let batches = read_tasks(tbl.file_io().clone(), tasks, &HashSet::new()).await?;
        Ok(ReadResult { schema, batches })
    }

    /// The source table's **current snapshot** — its id and commit timestamp (epoch ms) —
    /// read from table metadata only (no scan). This is the cheap "source head" the Ingestion
    /// view compares each shard's committed checkpoint against. Returns `(0, 0)` when
    /// the table has no snapshots yet.
    pub async fn current_snapshot(&self, table: &str) -> Result<(i64, i64)> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        Ok(match tbl.metadata().current_snapshot() {
            Some(snap) => (snap.snapshot_id(), snap.timestamp_ms()),
            None => (0, 0),
        })
    }

    /// The current snapshot's `(id, sequence-number)` from table metadata only (no scan), or
    /// `None` when the table has no snapshots. The sequence number is the lineage-monotone
    /// order over snapshots — snapshot ids are random longs and carry none.
    pub async fn current_snapshot_ordered(&self, table: &str) -> Result<Option<(i64, i64)>> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        Ok(tbl
            .metadata()
            .current_snapshot()
            .map(|snap| (snap.snapshot_id(), snap.sequence_number())))
    }

    /// Every snapshot's `(id → commit-timestamp-ms)`, from table metadata only (no scan). The
    /// Ingestion view looks up a shard's committed snapshot to measure how far *behind*
    /// the source head it is in wall-clock terms — Iceberg snapshot ids are random, not sequential,
    /// so an id delta is meaningless; a time delta is what's comparable.
    pub async fn snapshot_timestamps(
        &self,
        table: &str,
    ) -> Result<std::collections::HashMap<i64, i64>> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        Ok(tbl
            .metadata()
            .snapshots()
            .map(|snap| (snap.snapshot_id(), snap.timestamp_ms()))
            .collect())
    }

    /// The source table's **Iceberg `table-uuid`** — the stable identity of *this* table, distinct
    /// from its name. A drop+recreate (or an in-memory catalog reset) mints a new uuid even at the
    /// same name, so comparing the build-time uuid recorded in the index to the live one detects a
    /// **recreated source** whose rows the index no longer matches — the lineage guard.
    pub async fn table_uuid(&self, table: &str) -> Result<String> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        Ok(tbl.metadata().uuid().to_string())
    }

    /// **Append fast-path**: read only the data files **added since** `since_snapshot` —
    /// for opt-in immutable/append-only tables, the cheap incremental scan (no delete/update
    /// handling). Files already present at `since_snapshot` are skipped; `None` reads the whole
    /// current snapshot (the initial backfill). Returns the located batches plus the current
    /// snapshot id they bring the index up to.
    ///
    /// Correct for append-only tables (files are only added). It is **not** safe on a
    /// table with deletes/rewrites — those need [changelog mode](IcebergReader);
    /// hence the fast path is opt-in per [`ScanMode::AppendFastPath`].
    pub async fn read_appended_since(
        &self,
        table: &str,
        since_snapshot: Option<i64>,
    ) -> Result<(ReadResult, i64)> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(tbl.metadata().current_schema())?);
        let current_snapshot = tbl.metadata().current_snapshot_id().unwrap_or(0);

        // Files already present at the checkpoint snapshot are excluded; what remains
        // in the current plan is exactly what was appended after it.
        let prior: HashSet<String> = match since_snapshot {
            Some(s) if s == current_snapshot => {
                return Ok((
                    ReadResult {
                        schema,
                        batches: Vec::new(),
                    },
                    current_snapshot,
                ));
            }
            Some(s) => {
                let tasks: Vec<FileScanTask> = tbl
                    .scan()
                    .snapshot_id(s)
                    .select_all()
                    .build()?
                    .plan_files()
                    .await?
                    .try_collect()
                    .await?;
                tasks.into_iter().map(|t| t.data_file_path).collect()
            }
            None => HashSet::new(),
        };

        let tasks: Vec<FileScanTask> = tbl
            .scan()
            .select_all()
            .build()?
            .plan_files()
            .await?
            .try_collect()
            .await?;
        let batches = read_tasks(tbl.file_io().clone(), tasks, &prior).await?;
        Ok((ReadResult { schema, batches }, current_snapshot))
    }

    /// Read a table's [`SourceSchema`] — its top-level leaf fields plus the key
    /// hints (partition + identifier field names) GrowlerDB derives the composite key
    /// from. Struct/list/map leaves map to [`SourceType::Other`].
    ///
    /// `table` is a dotted identifier, e.g. `growlerdb.docs`.
    pub async fn read_source_schema(&self, table: &str) -> Result<SourceSchema> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        let meta = tbl.metadata();
        let schema = meta.current_schema();

        // Partition / identifier field *names*, resolved from their source field ids.
        let partition_fields = meta
            .default_partition_spec()
            .fields()
            .iter()
            .filter_map(|pf| schema.field_by_id(pf.source_id).map(|f| f.name.clone()))
            .collect();
        let identifier_fields = schema
            .identifier_field_ids()
            .filter_map(|id| schema.field_by_id(id).map(|f| f.name.clone()))
            .collect();

        let arrow = schema_to_arrow_schema(schema)?;
        Ok(arrow_schema_to_source(
            &arrow,
            partition_fields,
            identifier_fields,
        ))
    }

    /// **Hydration with verify-and-fall-back** ([Flow 2], [keeping the locator
    /// valid]): resolve `(key, locator)` pairs to authoritative rows.
    ///
    /// For each request the located `(file, position)` is read — a **targeted parquet point
    /// read** ([`point_read`]) that fetches only the row group(s) holding the requested positions
    /// instead of streaming the file from row 0 — and **verified**: the
    /// row's key must match the requested key. If the locator is **stale** (the file was
    /// rewritten away, the position is out of range, or the row no longer carries that key),
    /// GrowlerDB **falls back** to a partition-scoped scan of the current snapshot to re-find
    /// the row by key, and returns a **refreshed** locator for it. So a phantom row is never
    /// returned (verification), and a lagging locator self-heals (fallback + refresh). Keys
    /// sharing a file are coalesced; rows come back in input order (genuinely-absent keys
    /// omitted).
    ///
    /// A request may carry **no locator** (`None`) — a *known-stale* key whose locator points
    /// into a file the index has flagged dead (the live-file bitmap): it skips the doomed pass-1
    /// point read entirely and goes straight to the fallback (whose
    /// result refreshes the slot).
    ///
    /// [Flow 2]: ../../../design/07-data-flows.md
    /// [keeping the locator valid]: ../../../wiki/07-query-execution.md#keeping-the-locator-valid-as-iceberg-changes
    pub async fn hydrate(
        &self,
        table: &str,
        requests: &[(CompositeKey, Option<RowLocator>)],
        projection: &Projection,
    ) -> Result<HydrationResult> {
        if requests.is_empty() {
            return Ok(HydrationResult::default());
        }
        // One catalog REST call to learn the current snapshot; the pass-1 unpredicated plan
        // (manifest-list + manifest GETs) is then reused from the snapshot-pinned cache while
        // the snapshot is unchanged, and replanned (replacing the entry) once it advances.
        // Pass 2 below is per-request-predicated and stays uncached.
        let (tbl, tasks, plan_cache_hit) = self.load_and_plan(table).await?;
        let file_io = tbl.file_io().clone();

        // Pass 1 — located read + verify (point reads, coalesced by file); a `None` is a stale
        // entry (missing file/position or key mismatch), deferred to the fallback.
        let mut resolved = resolve_pass1(&file_io, &tasks, requests).await?;
        let any_stale = resolved.iter().any(Option::is_none);

        // Pass 2 — fallback: re-resolve stale rows by scanning the current snapshot. To keep this
        // cheap we push an equality predicate over the stale keys' partition + identifier
        // fields, so Iceberg prunes to the relevant partitions/files instead of reading the whole
        // table — the point of declaring partition fields. Correctness doesn't depend on the
        // predicate: every candidate row is re-verified against the exact key below, so a superset
        // (or, on any predicate/scan error, an unfiltered read) is always safe.
        let mut refreshed: Vec<(CompositeKey, RowLocator)> = Vec::new();
        let mut duplicate_pks = 0u64;
        if any_stale {
            let stale_keys: Vec<&CompositeKey> = requests
                .iter()
                .zip(&resolved)
                .filter(|(_, r)| r.is_none())
                .map(|((k, _), _)| k)
                .collect();
            let predicate = key_predicate(tbl.metadata().current_schema(), &stale_keys);
            let (partition_names, identifier_names) = key_field_names(&requests[0].0);
            // Only the stale keys are re-resolved, and the scan streams with early-exit — so even the
            // unfiltered (`None` predicate) path is bounded in memory and cost.
            let wanted: HashSet<Vec<u8>> = stale_keys.iter().map(|k| k.encode()).collect();
            let (index, duplicates) = match scan_stale_index(
                &tbl,
                predicate.clone(),
                &wanted,
                &partition_names,
                &identifier_names,
            )
            .await
            {
                Ok(found) => found,
                // A pruned scan that errored (e.g. an unexpected type binding) must never turn a
                // present row into a miss — fall back to the full unfiltered scan.
                Err(_) if predicate.is_some() => {
                    scan_stale_index(&tbl, None, &wanted, &partition_names, &identifier_names)
                        .await?
                }
                Err(e) => return Err(e),
            };
            duplicate_pks = duplicates;
            for (i, (key, _)) in requests.iter().enumerate() {
                if resolved[i].is_some() {
                    continue;
                }
                if let Some((full, fresh)) = index.get(&key.encode()) {
                    resolved[i] = Some(full.clone());
                    refreshed.push((key.clone(), fresh.clone()));
                }
            }
        }

        let rows = assemble_rows(requests, resolved, projection);
        Ok(HydrationResult {
            rows,
            refreshed,
            plan_cache_hit: Some(plan_cache_hit),
            duplicate_pks,
        })
    }

    /// `load_table` + the snapshot-pinned pass-1 plan for its current snapshot (cached per
    /// snapshot, replanned on advance) — shared by [`hydrate`](Self::hydrate) and
    /// [`current_plan`](Self::current_plan). Returns `(table, tasks, cache_hit)`.
    async fn load_and_plan(&self, table: &str) -> Result<(Table, Arc<Vec<FileScanTask>>, bool)> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        let snapshot_id = tbl.metadata().current_snapshot_id().unwrap_or(0);
        let (tasks, cache_hit) = self
            .plans
            .get_or_plan(table, snapshot_id, || async {
                let planned: Vec<FileScanTask> = tbl
                    .scan()
                    .select_all()
                    .build()?
                    .plan_files()
                    .await?
                    .try_collect()
                    .await?;
                Ok::<_, SourceError>(Arc::new(planned))
            })
            .await?;
        Ok((tbl, tasks, cache_hit))
    }

    /// The table's **current-snapshot plan** — snapshot id, file-scan tasks, and the
    /// `FileIO` to read them with — served from the same snapshot-pinned [`PlanCache`]
    /// hydration uses (one catalog call; manifest reads only on snapshot advance). The
    /// compaction re-map poller diffs its live data-file set against
    /// the index's interned files each tick, so the steady-state poll costs one REST
    /// call and a cache hit. Observing table metadata is read-only — it imposes nothing
    /// on the source.
    pub async fn current_plan(&self, table: &str) -> Result<TablePlan> {
        let (tbl, tasks, cache_hit) = self.load_and_plan(table).await?;
        Ok(TablePlan {
            snapshot_id: tbl.metadata().current_snapshot_id().unwrap_or(0),
            tasks,
            file_io: tbl.file_io().clone(),
            cache_hit,
        })
    }

    /// Read a table's current snapshot and map every row to a [`LocatedDoc`] —
    /// the composite key + indexed fields (per `index`) plus the row's source
    /// location (data file + position) for the locator. Full snapshot, append-only.
    pub async fn read_documents(
        &self,
        table: &str,
        index: &ResolvedIndex,
    ) -> Result<DocumentBatch> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        let snapshot_id = tbl.metadata().current_snapshot_id().unwrap_or(0);

        let read = self.read_current(table).await?;
        let mut docs = Vec::with_capacity(read.row_count());
        for lb in &read.batches {
            batch_to_docs(index, &lb.batch, &lb.data_file, lb.start_row, &mut docs);
        }
        Ok(DocumentBatch { docs, snapshot_id })
    }

    /// The `total-records` the current snapshot's summary reports, if present. Lets a
    /// build catch the case where it read **0 documents from a non-empty table** — a stale/broken
    /// read (e.g. a delete-in-history that the changelog read mishandles) — instead of silently
    /// committing an empty index.
    pub async fn current_snapshot_records(&self, table: &str) -> Result<Option<i64>> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        Ok(tbl.metadata().current_snapshot().and_then(|s| {
            s.summary()
                .additional_properties
                .get("total-records")
                .and_then(|v| v.parse::<i64>().ok())
        }))
    }

    /// Cheap **source-health** signals for the Ingestion/Observability view, read from table
    /// metadata only — **no scan**. GrowlerDB reads O(files) on the query path (scan planning
    /// and hydration), so a source that accumulates small files or a long snapshot history silently
    /// slows GrowlerDB down with nothing pointing at the real cause. These gauges let operators
    /// *diagnose* that; the remedy (Iceberg compaction / `expire_snapshots`) stays the user's, never
    /// GrowlerDB's — GrowlerDB never manages the source table.
    ///
    /// Everything comes from the current snapshot's `summary` (the `total-*` properties an Iceberg
    /// writer populates by convention) plus the retained-snapshot count — one catalog load, no
    /// manifest reads. A property the writer omitted reads as 0.
    pub async fn source_health(&self, table: &str) -> Result<SourceHealth> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        let meta = tbl.metadata();
        let snapshots = meta.snapshots().len() as u64;
        let Some(snap) = meta.current_snapshot() else {
            return Ok(SourceHealth {
                snapshots,
                ..Default::default()
            });
        };
        let prop = |key: &str| -> u64 {
            snap.summary()
                .additional_properties
                .get(key)
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
        };
        Ok(SourceHealth {
            data_files: prop("total-data-files"),
            bytes: prop("total-files-size"),
            delete_files: prop("total-delete-files"),
            records: prop("total-records"),
            snapshots,
        })
    }

    /// A cheap **partition-skew** ratio for the source's current snapshot: the largest
    /// identity partition's record count over the mean across partitions, from manifest metadata
    /// ([`partition_record_counts`] — no row reads). `1.0` means partitions are evenly sized; a
    /// higher value means one partition is a hotspot (lopsided ingest / a hot key). Returns `None`
    /// when the source isn't cleanly identity-partitioned (nothing to skew-check) or has fewer than
    /// two partitions. Costs one `current_plan` (manifest read on a new snapshot, then cached),
    /// unlike [`source_health`](Self::source_health) which is summary-only.
    pub async fn partition_skew(&self, table: &str) -> Result<Option<f64>> {
        let plan = self.current_plan(table).await?;
        let Some(counts) = partition_record_counts(&plan.tasks) else {
            return Ok(None);
        };
        if counts.len() < 2 {
            return Ok(None);
        }
        let total: u64 = counts.iter().map(|(_, n)| *n).sum();
        let max = counts.iter().map(|(_, n)| *n).max().unwrap_or(0);
        let mean = total as f64 / counts.len() as f64;
        Ok((mean > 0.0).then_some(max as f64 / mean))
    }

    /// **Streamed** full-snapshot read: map the current snapshot to documents and hand
    /// them to `sink` in **bounded chunks** (≈[`STREAM_CHUNK`] docs), reading one data file at a
    /// time, so peak memory is independent of table size — a table larger than RAM can be indexed
    /// (the non-streamed [`read_documents`](Self::read_documents) buffers the whole table). Returns
    /// `(snapshot_id, total_docs)`. The caller writes each chunk and is responsible for unique
    /// commit/batch ids per chunk.
    pub async fn read_documents_streamed<F>(
        &self,
        table: &str,
        index: &ResolvedIndex,
        mut sink: F,
    ) -> Result<(i64, usize)>
    where
        F: FnMut(Vec<LocatedDoc>) -> std::result::Result<(), String>,
    {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        let snapshot_id = tbl.metadata().current_snapshot_id().unwrap_or(0);
        let tasks: Vec<FileScanTask> = tbl
            .scan()
            .select_all()
            .build()?
            .plan_files()
            .await?
            .try_collect()
            .await?;
        let file_io = tbl.file_io().clone();

        let mut total = 0usize;
        let mut chunk: Vec<LocatedDoc> = Vec::new();
        for task in tasks {
            let data_file = task.data_file_path.clone();
            let reader = ArrowReaderBuilder::new(file_io.clone()).build();
            let task_stream =
                futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) })
                    .boxed();
            let mut stream = reader.read(task_stream)?;
            let mut pos = 0u64;
            while let Some(batch) = stream.try_next().await? {
                let n = batch.num_rows() as u64;
                batch_to_docs(index, &batch, &data_file, pos, &mut chunk);
                pos += n;
                if chunk.len() >= STREAM_CHUNK {
                    total += chunk.len();
                    sink(std::mem::take(&mut chunk)).map_err(SourceError::Sink)?;
                }
            }
        }
        if !chunk.is_empty() {
            total += chunk.len();
            sink(chunk).map_err(SourceError::Sink)?;
        }
        Ok((snapshot_id, total))
    }

    /// Map only the rows from files matching `partition` (an identity-partition tuple as
    /// [`partition_record_counts`] reports it) to documents, streamed in bounded chunks — the
    /// **partition-scoped** read the count-gate uses to reconcile only a divergent partition without
    /// scanning the whole table. Reads exactly the data files whose partition equals
    /// `partition`; returns `(snapshot_id, docs_read)`.
    pub async fn read_documents_in_partition<F>(
        &self,
        table: &str,
        index: &ResolvedIndex,
        partition: &[(String, Value)],
        mut sink: F,
    ) -> Result<(i64, usize)>
    where
        F: FnMut(Vec<LocatedDoc>) -> std::result::Result<(), String>,
    {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        let snapshot_id = tbl.metadata().current_snapshot_id().unwrap_or(0);
        let all: Vec<FileScanTask> = tbl
            .scan()
            .select_all()
            .build()?
            .plan_files()
            .await?
            .try_collect()
            .await?;
        let file_io = tbl.file_io().clone();

        let mut total = 0usize;
        let mut chunk: Vec<LocatedDoc> = Vec::new();
        for task in all {
            // Only the files in the requested identity partition; a task whose partition doesn't
            // extract (non-identity/unsupported) never matches, so it's simply not read here.
            if identity_partition_of(&task).as_deref() != Some(partition) {
                continue;
            }
            let data_file = task.data_file_path.clone();
            let reader = ArrowReaderBuilder::new(file_io.clone()).build();
            let task_stream =
                futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) })
                    .boxed();
            let mut stream = reader.read(task_stream)?;
            let mut pos = 0u64;
            while let Some(batch) = stream.try_next().await? {
                let n = batch.num_rows() as u64;
                batch_to_docs(index, &batch, &data_file, pos, &mut chunk);
                pos += n;
                if chunk.len() >= STREAM_CHUNK {
                    total += chunk.len();
                    sink(std::mem::take(&mut chunk)).map_err(SourceError::Sink)?;
                }
            }
        }
        if !chunk.is_empty() {
            total += chunk.len();
            sink(chunk).map_err(SourceError::Sink)?;
        }
        Ok((snapshot_id, total))
    }

    /// Map only the rows from files **appended since** `since_snapshot` to documents
    /// (the append fast-path's document read). `since_snapshot = None` is a
    /// full backfill. The returned `snapshot_id` is the current snapshot the index is
    /// brought up to (the new checkpoint). See [`read_appended_since`](Self::read_appended_since).
    pub async fn read_documents_appended_since(
        &self,
        table: &str,
        index: &ResolvedIndex,
        since_snapshot: Option<i64>,
    ) -> Result<DocumentBatch> {
        let (read, snapshot_id) = self.read_appended_since(table, since_snapshot).await?;
        let mut docs = Vec::with_capacity(read.row_count());
        for lb in &read.batches {
            batch_to_docs(index, &lb.batch, &lb.data_file, lb.start_row, &mut docs);
        }
        Ok(DocumentBatch { docs, snapshot_id })
    }
}

/// Cheap **source-health** signals ([`IcebergReader::source_health`]) — all read from
/// the current snapshot's summary + the retained-snapshot count, no scan. Diagnostic only: they
/// tell an operator the *source* table wants Iceberg maintenance (compaction / `expire_snapshots`),
/// which stays the user's responsibility, outside GrowlerDB.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SourceHealth {
    /// Data files in the current snapshot (`total-data-files`) — the O(files) scan-planning driver.
    pub data_files: u64,
    /// Total data-file bytes (`total-files-size`). With `data_files` this gives the average file
    /// size — the small-file signal (many tiny files ⇒ a low average ⇒ the source wants compaction).
    pub bytes: u64,
    /// Delete files in the current snapshot (`total-delete-files`) — merge-on-read read overhead.
    pub delete_files: u64,
    /// Rows in the current snapshot (`total-records`).
    pub records: u64,
    /// Retained snapshot count — metadata history depth. Unbounded growth ⇒ fat metadata that wants
    /// `expire_snapshots`.
    pub snapshots: u64,
}

/// A table's current-snapshot plan as [`IcebergReader::current_plan`] returns it: the
/// snapshot it was planned at, the file-scan tasks (shared with the hydration plan
/// cache), and the `FileIO` that reads their data files.
pub struct TablePlan {
    /// The Iceberg snapshot id the plan reflects (0 for an empty table).
    pub snapshot_id: i64,
    /// The snapshot's file-scan tasks (one per data file), from the snapshot-pinned cache.
    pub tasks: Arc<Vec<FileScanTask>>,
    /// The table's IO stack — reads the plan's data files (e.g. the re-map's key scan).
    pub file_io: iceberg::io::FileIO,
    /// Whether the plan came from the snapshot-pinned cache (no manifest reads).
    pub cache_hit: bool,
}

/// A table snapshot mapped to documents, tagged with the snapshot it reflects
/// (the source checkpoint for an exactly-once commit).
pub struct DocumentBatch {
    /// The documents, each with its source location.
    pub docs: Vec<LocatedDoc>,
    /// The Iceberg snapshot id these documents were read from.
    pub snapshot_id: i64,
}

/// Read each `FileScanTask` into [`LocatedBatch`]es, skipping any whose data file is
/// in `exclude` (the append fast-path's already-seen files). Each output batch is
/// attributed to one data file + a row position within it (plan-then-read, so files
/// aren't merged and the locator stays exact).
async fn read_tasks(
    file_io: iceberg::io::FileIO,
    tasks: Vec<FileScanTask>,
    exclude: &HashSet<String>,
) -> Result<Vec<LocatedBatch>> {
    let mut batches = Vec::new();
    for task in tasks {
        if exclude.contains(&task.data_file_path) {
            continue;
        }
        let data_file = task.data_file_path.clone();
        let reader = ArrowReaderBuilder::new(file_io.clone()).build();
        let task_stream =
            futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) }).boxed();
        let mut stream = reader.read(task_stream)?;

        let mut pos = 0u64;
        while let Some(batch) = stream.try_next().await? {
            let n = batch.num_rows() as u64;
            batches.push(LocatedBatch {
                batch,
                data_file: data_file.clone(),
                start_row: pos,
            });
            pos += n;
        }
    }
    Ok(batches)
}

/// Map each row of `batch` to a [`LocatedDoc`] per the resolved `index`, appending
/// to `out`. `start_row` is the batch's absolute row offset within `data_file`.
fn batch_to_docs(
    index: &ResolvedIndex,
    batch: &RecordBatch,
    data_file: &str,
    start_row: u64,
    out: &mut Vec<LocatedDoc>,
) {
    let extract = |names: &[String], row: usize| -> Vec<(String, Value)> {
        names
            .iter()
            .filter_map(|name| Some((name.clone(), nested_value(batch, name, row)?)))
            .collect()
    };

    for row in 0..batch.num_rows() {
        let key = CompositeKey::new(
            extract(&index.key.partition_fields, row),
            extract(&index.key.identifier_fields, row),
        );
        let mut fields = BTreeMap::new();
        for f in &index.fields {
            if let Some(value) = nested_value(batch, &f.path, row) {
                fields.insert(f.path.clone(), value);
            }
        }
        out.push(LocatedDoc {
            doc: Document::new(key, fields),
            iceberg_file: data_file.to_string(),
            row_position: start_row + row as u64,
        });
    }
}

/// The outcome of [hydration](IcebergReader::hydrate): the resolved rows plus any
/// **refreshed** locator entries (stale entries that fell back and re-found their
/// row) for the caller to write back into the index store.
#[derive(Debug, Clone, Default)]
pub struct HydrationResult {
    /// The hydrated rows, in request order (genuinely-absent keys omitted).
    pub rows: Vec<HydratedRow>,
    /// `(key, fresh locator)` for entries whose `(file, position)` had moved.
    pub refreshed: Vec<(CompositeKey, RowLocator)>,
    /// Whether pass 1's plan came from the snapshot-pinned [`PlanCache`] (`Some(true)`),
    /// was freshly planned (`Some(false)`), or no planning happened at all (`None` — an
    /// empty request). Feeds the `growlerdb_plan_cache_{hits,misses}_total` counters.
    pub plan_cache_hit: Option<bool>,
    /// **Duplicate primary keys** the key scan detected: extra distinct
    /// source rows matching an already-matched key. The result stays deterministic —
    /// per key, the row with the **highest `(file, position)`** among the scanned rows
    /// wins (see [`index_batch`]) — but a duplicate means the source table holds more
    /// than one row for a "unique" key. Feeds `growlerdb_duplicate_pks_total`.
    pub duplicate_pks: u64,
}

/// Pass 1 of [hydration](IcebergReader::hydrate): resolve each request's `(file, position)` and
/// **verify** the row's key, returning per-request `Some(full row)` or `None` (stale — the caller
/// falls back). Requests are coalesced by file; within a file one parquet footer read serves all
/// requested positions via a **targeted point read** ([`point_read`]) — the row group(s) holding
/// the positions plus a `RowSelection` to the exact rows, instead of streaming the file from row
/// 0. A file absent from `tasks` (rewritten away) yields `None` for all its positions, and a
/// request with **no locator** (known-stale via the live-file bitmap) is `None` without any read
/// at all.
async fn resolve_pass1(
    file_io: &iceberg::io::FileIO,
    tasks: &[FileScanTask],
    requests: &[(CompositeKey, Option<RowLocator>)],
) -> Result<Vec<Option<BTreeMap<String, Value>>>> {
    let mut by_file: BTreeMap<String, Vec<u64>> = BTreeMap::new();
    for (_key, loc) in requests {
        let Some(loc) = loc else {
            continue; // known stale — straight to the pass-2 fallback
        };
        by_file
            .entry(loc.iceberg_file.clone())
            .or_default()
            .push(loc.row_position);
    }
    // Index the plan by data-file path once (O(files)) so the per-file lookup below is O(1). A
    // linear `tasks.iter().find` per requested file would be O(files × requested-files), which —
    // at the large small-file counts a continuously-appended table accumulates between
    // compactions — dominates hydration planning.
    let by_path: HashMap<&str, &FileScanTask> = tasks
        .iter()
        .map(|t| (t.data_file_path.as_str(), t))
        .collect();
    let mut located: HashMap<(String, u64), BTreeMap<String, Value>> = HashMap::new();
    for (file, positions) in &by_file {
        let Some(task) = by_path.get(file.as_str()).map(|t| (*t).clone()) else {
            continue; // file rewritten away → all its positions fall back
        };
        // Locator positions were recorded against the ingest-time stream. For a delete-free file
        // that equals the **physical** row position, so the direct parquet point read is an exact
        // drop-in. A file carrying delete files/DVs must instead go through the iceberg reader
        // (which applies them): its stream positions are delete-shifted — matching what ingest
        // recorded — and a physical read there could return a since-deleted row whose key still
        // verifies. Either way, a row that fails the key verify goes stale → pass 2, unchanged.
        let rows = if task.deletes.is_empty() {
            point_read::read_file_rows(file_io, file, positions).await?
        } else {
            stream_file_rows(file_io.clone(), task, positions).await?
        };
        for (pos, full) in rows {
            located.insert((file.clone(), pos), full);
        }
    }
    // Verify: the located row must carry the requested key, else the entry is stale — a phantom
    // row from a moved `(file, position)` is never returned.
    Ok(requests
        .iter()
        .map(|(key, loc)| {
            let loc = loc.as_ref()?;
            located
                .get(&(loc.iceberg_file.clone(), loc.row_position))
                .filter(|full| row_matches_key(full, key))
                .cloned()
        })
        .collect())
}

/// The streaming pass-1 read, kept for **delete-bearing files** (see [`resolve_pass1`]): the
/// iceberg Arrow reader applies the file's delete files while streaming from row 0, stopping past
/// `max(positions)` rather than reading the whole file. Returns `position → full row`;
/// out-of-range positions are simply absent.
async fn stream_file_rows(
    file_io: iceberg::io::FileIO,
    task: FileScanTask,
    positions: &[u64],
) -> Result<BTreeMap<u64, BTreeMap<String, Value>>> {
    let reader = ArrowReaderBuilder::new(file_io).build();
    let task_stream =
        futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) }).boxed();
    let mut stream = reader.read(task_stream)?;
    let max_pos = positions.iter().copied().max().unwrap_or(0);
    let mut batches = Vec::new();
    let mut rows_seen = 0u64;
    while let Some(batch) = stream.try_next().await? {
        rows_seen += batch.num_rows() as u64;
        batches.push(batch);
        if rows_seen > max_pos {
            break; // every requested position for this file is now covered
        }
    }
    Ok(extract_full_rows(&batches, positions))
}

/// Final assembly of [hydration](IcebergReader::hydrate): the resolved rows back in **request
/// order**, genuinely-absent keys omitted, each row narrowed to `projection`.
fn assemble_rows(
    requests: &[(CompositeKey, Option<RowLocator>)],
    resolved: Vec<Option<BTreeMap<String, Value>>>,
    projection: &Projection,
) -> Vec<HydratedRow> {
    requests
        .iter()
        .zip(resolved)
        .filter_map(|((key, _), full)| {
            full.map(|full| HydratedRow {
                key: key.clone(),
                fields: project_row(&full, projection),
            })
        })
        .collect()
}

/// Extract the **full** rows (all columns) at the requested `positions` (absolute
/// row offsets across a single file's `batches`). Out-of-range positions are
/// simply omitted (a stale-locator signal, not an error).
fn extract_full_rows(
    batches: &[RecordBatch],
    positions: &[u64],
) -> BTreeMap<u64, BTreeMap<String, Value>> {
    let want: BTreeSet<u64> = positions.iter().copied().collect();
    let mut result: BTreeMap<u64, BTreeMap<String, Value>> = BTreeMap::new();
    let mut offset = 0u64;
    for batch in batches {
        let n = batch.num_rows() as u64;
        for &p in want.range(offset..offset + n) {
            result.insert(p, full_row(batch, (p - offset) as usize));
        }
        offset += n;
    }
    result
}

/// Extract every column of `batch` at `row` as a field map (scalar subset).
fn full_row(batch: &RecordBatch, row: usize) -> BTreeMap<String, Value> {
    let schema = batch.schema();
    let mut fields = BTreeMap::new();
    for (i, field) in schema.fields().iter().enumerate() {
        if let Some(value) = array_value(batch.column(i).as_ref(), row) {
            fields.insert(field.name().clone(), value);
        }
    }
    fields
}

/// Narrow a full row to the requested projection.
fn project_row(full: &BTreeMap<String, Value>, projection: &Projection) -> BTreeMap<String, Value> {
    match projection {
        Projection::All => full.clone(),
        Projection::Columns(_) => full
            .iter()
            .filter(|(name, _)| projection.includes(name))
            .map(|(n, v)| (n.clone(), v.clone()))
            .collect(),
    }
}

/// Whether `full` carries the values of every field in `key` — the verification
/// that prevents returning a phantom row from a stale `(file, position)`.
fn row_matches_key(full: &BTreeMap<String, Value>, key: &CompositeKey) -> bool {
    key.partition
        .iter()
        .chain(key.identifier.iter())
        .all(|(name, value)| full.get(name) == Some(value))
}

/// The partition + identifier field names of a composite key.
fn key_field_names(key: &CompositeKey) -> (Vec<String>, Vec<String>) {
    let names = |fields: &[(String, Value)]| fields.iter().map(|(n, _)| n.clone()).collect();
    (names(&key.partition), names(&key.identifier))
}

/// Scan a table's current snapshot (optionally pruned by `predicate`) into located batches — the
/// hydration-fallback read. Reuses the already-loaded [`Table`] so no extra catalog round-trip.
/// Stream the current snapshot (optionally pruned by `predicate`) and index **only** the `wanted`
/// stale keys → `(full row, fresh locator)`, stopping as soon as they're all found. Bounds both
/// memory and cost: batches are processed one at a time (never the whole snapshot in RAM), and the
/// result map is capped at the stale set — critical when `predicate` is `None` (a DATE key / type
/// mismatch) forces an unfiltered scan.
///
/// Also returns the number of **duplicate PKs** seen (see [`index_batch`]).
/// Note the early exit bounds detection too: once every wanted key has a match the
/// scan stops, so a duplicate lurking in a not-yet-scanned file goes unreported —
/// detection is honest within what the scan read, not a full-table uniqueness audit.
async fn scan_stale_index(
    tbl: &Table,
    predicate: Option<Predicate>,
    wanted: &HashSet<Vec<u8>>,
    partition_names: &[String],
    identifier_names: &[String],
) -> Result<(HashMap<Vec<u8>, (BTreeMap<String, Value>, RowLocator)>, u64)> {
    let mut builder = tbl.scan().select_all();
    if let Some(p) = predicate {
        builder = builder.with_filter(p);
    }
    let tasks: Vec<FileScanTask> = builder.build()?.plan_files().await?.try_collect().await?;
    let file_io = tbl.file_io().clone();
    let mut index = HashMap::new();
    let mut duplicates = 0u64;
    'files: for task in tasks {
        let data_file = task.data_file_path.clone();
        let reader = ArrowReaderBuilder::new(file_io.clone()).build();
        let task_stream =
            futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) }).boxed();
        let mut stream = reader.read(task_stream)?;
        let mut start_row = 0u64;
        while let Some(batch) = stream.try_next().await? {
            let n = batch.num_rows() as u64;
            duplicates += index_batch(
                &mut index,
                &batch,
                &data_file,
                start_row,
                wanted,
                partition_names,
                identifier_names,
            );
            start_row += n;
            if index.len() == wanted.len() {
                break 'files; // every stale key located → stop scanning
            }
        }
    }
    Ok((index, duplicates))
}

/// Build an `OR`-of-`AND` equality predicate over the partition + identifier fields of `keys`, so a
/// hydration fallback prunes the Iceberg scan to the partitions/files that can hold them.
///
/// Datums are typed to match the source schema; any field whose type can't be mapped safely (a
/// value/column-type mismatch, or a timestamp that can't be an exact DATE) makes the whole
/// predicate `None` so the caller reads unfiltered. This must never *exclude* a matching row — the
/// fallback re-verifies each candidate against the exact key — so `None` (read everything) is the
/// safe default, and pruning is a pure speed-up. Returns `None` for an empty key set.
fn key_predicate(schema: &IcebergSchema, keys: &[&CompositeKey]) -> Option<Predicate> {
    let mut per_key = Vec::with_capacity(keys.len());
    for key in keys {
        let mut conj: Option<Predicate> = None;
        for (name, value) in key.partition.iter().chain(key.identifier.iter()) {
            let datum = value_to_datum(schema, name, value)?;
            let eq = Reference::new(name.clone()).equal_to(datum);
            conj = Some(match conj {
                Some(c) => c.and(eq),
                None => eq,
            });
        }
        per_key.push(conj?); // identifier is always non-empty, so conj is Some
    }
    let mut it = per_key.into_iter();
    let first = it.next()?;
    Some(it.fold(first, |acc, p| acc.or(p)))
}

/// Microseconds per UTC day — the Date32 (days-since-epoch) ↔ canonical-micros scale factor.
const MICROS_PER_DAY: i64 = 86_400_000_000;

/// The identity-partition tuple of a data file, in partition-spec order, or `None` when it can't be
/// safely extracted: no partition/spec, a **non-identity** transform (`day`/`bucket`/…),
/// a temporal/float/decimal partition column (whose literal encoding we won't risk mis-mapping to the
/// index key form), a null partition value, or a type mismatch. Metadata only — no row reads. Used to
/// group files by partition for the count-gate and to scope a partition read; anything it can't
/// extract makes the caller fall back to a whole-shard reconcile (safe, just not optimized).
fn identity_partition_of(task: &FileScanTask) -> Option<Vec<(String, Value)>> {
    let spec = task.partition_spec.as_ref()?;
    let part = task.partition.as_ref()?;
    let fields = spec.fields();
    if fields.is_empty() || fields.len() != part.fields().len() {
        return None;
    }
    let mut out = Vec::with_capacity(fields.len());
    for (field, lit) in fields.iter().zip(part.fields().iter()) {
        if field.transform != Transform::Identity {
            return None;
        }
        let Some(Literal::Primitive(prim)) = lit else {
            return None; // null / nested partition value — unsupported
        };
        let col_ty = task
            .schema
            .field_by_id(field.source_id)
            .and_then(|f| f.field_type.as_primitive_type())?;
        // Only the types whose literal maps 1:1 to the index key `Value`. Temporal (days/micros),
        // float, decimal, and binary are excluded so a wrong prefix can never silently mis-count.
        let value = match (col_ty, prim) {
            (PrimitiveType::String, PrimitiveLiteral::String(s)) => Value::Str(s.clone()),
            (PrimitiveType::Long, PrimitiveLiteral::Long(i)) => Value::Int(*i),
            (PrimitiveType::Int, PrimitiveLiteral::Int(i)) => Value::Int(i64::from(*i)),
            (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(b)) => Value::Bool(*b),
            _ => return None,
        };
        out.push((field.name.clone(), value));
    }
    Some(out)
}

/// One identity partition's tuple (`field → value`, in spec order) paired with its source record
/// count summed from manifest metadata (the count-gate).
pub type PartitionCount = (Vec<(String, Value)>, u64);

/// Per-partition source record counts from file **metadata** (manifest `record_count`), grouped by
/// identity partition — the cheap detection half of the count-gate, zero row reads. Each
/// entry is `(partition tuple, Σ record_count)`. Returns `None` if the table isn't cleanly
/// identity-partitioned (any file whose partition can't be [extracted](identity_partition_of), or any
/// missing `record_count`) so the caller reconciles the whole shard instead. An empty table is
/// `Some(empty)`.
pub fn partition_record_counts(tasks: &[FileScanTask]) -> Option<Vec<PartitionCount>> {
    // Group by the partition's canonical key encoding — `Value` isn't `Ord`/`Hash` (it carries a
    // float variant), but its byte encoding is a stable map key.
    let mut counts: std::collections::HashMap<Vec<u8>, PartitionCount> =
        std::collections::HashMap::new();
    for task in tasks {
        let part = identity_partition_of(task)?;
        let records = task.record_count?;
        let enc = CompositeKey::new(part.clone(), Vec::new()).encode();
        let entry = counts.entry(enc).or_insert((part, 0));
        entry.1 += records;
    }
    Some(counts.into_values().collect())
}

/// Map a key [`Value`] to an Iceberg [`Datum`] typed to the source column, or `None` when the column
/// type isn't one we prune on (float keys are already rejected at definition time; unmapped types
/// fall back to an unfiltered read rather than risk a mis-typed predicate dropping the row).
fn value_to_datum(schema: &IcebergSchema, name: &str, value: &Value) -> Option<Datum> {
    let ty = schema.field_by_name(name)?.field_type.as_primitive_type()?;
    match (ty, value) {
        (PrimitiveType::String, Value::Str(s)) => Some(Datum::string(s)),
        (PrimitiveType::Long, Value::Int(i)) => Some(Datum::long(*i)),
        (PrimitiveType::Int, Value::Int(i)) => i32::try_from(*i).ok().map(Datum::int),
        (PrimitiveType::Boolean, Value::Bool(b)) => Some(Datum::bool(*b)),
        // Temporal keys: `Ts` is canonical epoch micros UTC. A DATE column only gets a
        // predicate when the micros are an exact UTC-midnight day — a lossy division could build a
        // predicate that *excludes* the matching row, and `None` is the safe unfiltered read.
        (PrimitiveType::Date, Value::Ts(micros)) if micros % MICROS_PER_DAY == 0 => {
            i32::try_from(micros / MICROS_PER_DAY).ok().map(Datum::date)
        }
        (PrimitiveType::Timestamp, Value::Ts(micros)) => Some(Datum::timestamp_micros(*micros)),
        (PrimitiveType::Timestamptz, Value::Ts(micros)) => Some(Datum::timestamptz_micros(*micros)),
        _ => None,
    }
}

/// Index the rows of one `batch` whose composite key is in `wanted` → `enc(key) → (full row, fresh
/// locator)`, for the verify-and-fall-back re-find. Filtering to `wanted` (the stale keys) is what
/// bounds the fallback's memory — an unfiltered scan of a large snapshot doesn't materialize an
/// entry per row. `start_row` is the batch's absolute offset within `data_file`.
///
/// **Duplicate-PK detection**: a second distinct source row matching an
/// already-matched key means the table holds >1 row for a "unique" key. Each extra row
/// counts toward the returned total (→ `growlerdb_duplicate_pks_total`) and emits a
/// [rate-limited warning](warn_duplicate_pk) naming the key. The result stays
/// deterministic: per key, the row with the **highest `(file, position)`** among the
/// rows scanned wins — never the silent scan-order last-wins of a plain map insert.
/// (The caller's early exit means rows past the point where every wanted key matched
/// aren't scanned; detection and the winner rule apply to what the scan read.)
fn index_batch(
    index: &mut HashMap<Vec<u8>, (BTreeMap<String, Value>, RowLocator)>,
    batch: &RecordBatch,
    data_file: &str,
    start_row: u64,
    wanted: &HashSet<Vec<u8>>,
    partition_names: &[String],
    identifier_names: &[String],
) -> u64 {
    let schema = batch.schema();
    let field = |names: &[String], row: usize| -> Vec<(String, Value)> {
        names
            .iter()
            .filter_map(|name| {
                let col = schema.index_of(name).ok()?;
                Some((name.clone(), array_value(batch.column(col).as_ref(), row)?))
            })
            .collect()
    };
    let mut duplicates = 0u64;
    for row in 0..batch.num_rows() {
        let partition = field(partition_names, row);
        let key = CompositeKey::new(partition, field(identifier_names, row));
        let enc = key.encode();
        if !wanted.contains(&enc) {
            continue; // only re-resolve the stale keys, not every row in the snapshot
        }
        let locator = RowLocator {
            iceberg_file: data_file.to_string(),
            row_position: start_row + row as u64,
        };
        match index.entry(enc) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert((full_row(batch, row), locator));
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                // A second distinct row for this key — a genuine duplicate PK (one
                // scan never visits the same (file, position) twice). Deterministic
                // winner: highest (file, position).
                duplicates += 1;
                let held = &slot.get().1;
                let keep_new = (locator.iceberg_file.as_str(), locator.row_position)
                    > (held.iceberg_file.as_str(), held.row_position);
                let (winner, loser) = if keep_new {
                    (&locator, held)
                } else {
                    (held, &locator)
                };
                warn_duplicate_pk(&key, winner, loser);
                if keep_new {
                    slot.insert((full_row(batch, row), locator));
                }
            }
        }
    }
    duplicates
}

/// Minimum seconds between duplicate-PK warnings — keeps a badly duplicated table
/// from flooding the log while the counter still records every occurrence.
const DUP_WARN_INTERVAL_SECS: u64 = 10;

/// Warn (rate-limited, at most one per [`DUP_WARN_INTERVAL_SECS`] process-wide) that
/// the key scan found a **duplicate primary key**: `key` matched more than one distinct
/// source row. Names the key and both rows, and states the deterministic winner rule.
/// Returns whether a line was actually emitted (for tests).
fn warn_duplicate_pk(key: &CompositeKey, winner: &RowLocator, loser: &RowLocator) -> bool {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    /// Epoch seconds of the last emitted warning (0 = never).
    static LAST_WARN_SECS: AtomicU64 = AtomicU64::new(0);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST_WARN_SECS.load(Ordering::Relaxed);
    if last != 0 && now.saturating_sub(last) < DUP_WARN_INTERVAL_SECS {
        return false; // within the rate-limit window — counted, not logged
    }
    if LAST_WARN_SECS
        .compare_exchange(last, now.max(1), Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return false; // a concurrent scan won the window
    }
    let describe = |fields: &[(String, Value)]| {
        fields
            .iter()
            .map(|(n, v)| format!("{n}={}", v.to_index_string()))
            .collect::<Vec<_>>()
            .join(",")
    };
    eprintln!(
        "WARNING: duplicate primary key [{}|{}] in source scan: >1 distinct row matches — keeping \
         {}:{} over {}:{} (deterministic: highest (file, position) wins). The source table is not \
         unique on this key; further duplicates are counted (growlerdb_duplicate_pks_total) but \
         this warning is rate-limited.",
        describe(&key.partition),
        describe(&key.identifier),
        winner.iceberg_file,
        winner.row_position,
        loser.iceberg_file,
        loser.row_position,
    );
    true
}

/// Extract a scalar [`Value`] from an Arrow array at `row` (scalar subset).
/// `None` for nulls and unsupported (nested/decimal/binary) types.
fn array_value(array: &dyn Array, row: usize) -> Option<Value> {
    if array.is_null(row) {
        return None;
    }
    macro_rules! get {
        ($ty:ty) => {
            array.as_any().downcast_ref::<$ty>()
        };
    }
    match array.data_type() {
        DataType::Utf8 => get!(StringArray).map(|a| Value::Str(a.value(row).to_string())),
        DataType::LargeUtf8 => get!(LargeStringArray).map(|a| Value::Str(a.value(row).to_string())),
        DataType::Boolean => get!(BooleanArray).map(|a| Value::Bool(a.value(row))),
        DataType::Int8 => get!(Int8Array).map(|a| Value::Int(a.value(row) as i64)),
        DataType::Int16 => get!(Int16Array).map(|a| Value::Int(a.value(row) as i64)),
        DataType::Int32 => get!(Int32Array).map(|a| Value::Int(a.value(row) as i64)),
        DataType::Int64 => get!(Int64Array).map(|a| Value::Int(a.value(row))),
        DataType::UInt8 => get!(UInt8Array).map(|a| Value::Int(a.value(row) as i64)),
        DataType::UInt16 => get!(UInt16Array).map(|a| Value::Int(a.value(row) as i64)),
        DataType::UInt32 => get!(UInt32Array).map(|a| Value::Int(a.value(row) as i64)),
        DataType::Float32 => get!(Float32Array).map(|a| Value::Float(a.value(row) as f64)),
        DataType::Float64 => get!(Float64Array).map(|a| Value::Float(a.value(row))),
        // Temporal columns normalize to canonical **epoch micros UTC** (`Value::Ts`).
        // Arrow timestamps store the instant since the epoch regardless of the tz annotation
        // (the tz is display metadata), so any tz normalizes the same way.
        DataType::Date32 => {
            get!(Date32Array).map(|a| Value::Ts(a.value(row) as i64 * MICROS_PER_DAY))
        }
        DataType::Date64 => get!(Date64Array).map(|a| Value::Ts(a.value(row) * 1_000)),
        DataType::Timestamp(TimeUnit::Second, _) => {
            get!(TimestampSecondArray).map(|a| Value::Ts(a.value(row) * 1_000_000))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            get!(TimestampMillisecondArray).map(|a| Value::Ts(a.value(row) * 1_000))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            get!(TimestampMicrosecondArray).map(|a| Value::Ts(a.value(row)))
        }
        // Nanos → micros floors (div_euclid, consistent with `TimeFormat::EpochNanos`) — sub-µs
        // precision is truncated; micros is the canonical unit.
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            get!(TimestampNanosecondArray).map(|a| Value::Ts(a.value(row).div_euclid(1_000)))
        }
        _ => None,
    }
}

/// Build a [`SourceSchema`] from an Arrow schema and pre-resolved key hints.
///
/// **Nested struct fields flatten to dotted leaf paths** (`actor.user`):
/// a `Struct` is recursed into and each leaf becomes a `SourceField` at its dotted
/// path with the leaf's coarse [`SourceType`]. List/Map values are scalar-valued in
/// GrowlerDB's wire form today, so their elements are not flattened (the field maps
/// to [`SourceType::Other`] and isn't extracted).
pub fn arrow_schema_to_source(
    arrow: &Schema,
    partition_fields: Vec<String>,
    identifier_fields: Vec<String>,
) -> SourceSchema {
    let mut fields = Vec::new();
    flatten_arrow_fields("", arrow.fields(), &mut fields);
    SourceSchema::new(fields, partition_fields, identifier_fields)
}

/// Recurse `fields` (under dotted `prefix`), emitting one [`SourceField`] per leaf;
/// `Struct` children are descended into, everything else is a leaf.
fn flatten_arrow_fields(prefix: &str, fields: &Fields, out: &mut Vec<SourceField>) {
    for f in fields {
        let path = if prefix.is_empty() {
            f.name().clone()
        } else {
            format!("{prefix}.{}", f.name())
        };
        match f.data_type() {
            DataType::Struct(children) => flatten_arrow_fields(&path, children, out),
            dt => out.push(SourceField::new(path, arrow_type_to_source(dt))),
        }
    }
}

/// Resolve a (possibly dotted) field `path` to its scalar [`Value`] at `row`,
/// descending nested `Struct` columns. `None` if any segment is missing, the path
/// doesn't resolve to a scalar, or a struct along the way is null at `row`.
fn nested_value(batch: &RecordBatch, path: &str, row: usize) -> Option<Value> {
    let mut segments = path.split('.');
    let top = segments.next()?;
    let mut array: &dyn Array = batch.column(batch.schema().index_of(top).ok()?).as_ref();
    for segment in segments {
        let st = array.as_any().downcast_ref::<StructArray>()?;
        if st.is_null(row) {
            return None;
        }
        array = st.column_by_name(segment)?.as_ref();
    }
    array_value(array, row)
}

/// Map an Arrow data type onto GrowlerDB's coarse [`SourceType`].
fn arrow_type_to_source(dt: &DataType) -> SourceType {
    use DataType::*;
    match dt {
        Utf8 | LargeUtf8 | Utf8View => SourceType::String,
        Boolean => SourceType::Bool,
        Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 => SourceType::Long,
        Float16 | Float32 | Float64 => SourceType::Double,
        Date32 | Date64 | Timestamp(_, _) => SourceType::Date,
        Binary | LargeBinary | BinaryView | FixedSizeBinary(_) => SourceType::Binary,
        _ => SourceType::Other,
    }
}

#[cfg(test)]
pub(crate) mod test_util {
    //! Shared parquet fixture for the point-read tests: a real multi-row-group `docs` file on
    //! local disk, read back through the same opendal `FileIO` stack production uses (`fs` scheme).
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use iceberg::io::FileIO;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    /// Write `rows` docs rows (`id = base + i` Int64, `body = body_for(id)` Utf8) to `path` with
    /// row groups of `group_size` rows, returning the file's length in bytes.
    pub(crate) fn write_docs_parquet(path: &str, base: i64, rows: usize, group_size: usize) -> u64 {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
        ]));
        let ids: Vec<i64> = (0..rows as i64).map(|i| base + i).collect();
        let bodies: Vec<String> = ids.iter().map(|id| body_for(*id)).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(bodies)),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_size(group_size)
            .build();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        std::fs::metadata(path).unwrap().len()
    }

    /// The deterministic body of row `id` — a mixed-in hash keeps the payload from compressing
    /// away, so byte-count assertions stay meaningful.
    pub(crate) fn body_for(id: i64) -> String {
        format!(
            "body-{id}-{:016x}",
            (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        )
    }

    /// A local-filesystem `FileIO` (the crate-level [`fs_file_io`](crate::fs_file_io)).
    pub(crate) fn fs_file_io() -> FileIO {
        crate::fs_file_io()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PartitionSpec, Struct, Type};

    /// An identity `PartitionSpec` on `site` over [`ice_schema`] (count-gate tests).
    fn site_spec() -> PartitionSpec {
        PartitionSpec::builder(Arc::new(ice_schema()))
            .add_partition_field("site", "site", Transform::Identity)
            .unwrap()
            .build()
            .unwrap()
    }

    /// A `FileScanTask` partitioned by identity `site = <site>` with `records` manifest rows.
    fn partitioned_task(site: &str, records: Option<u64>) -> FileScanTask {
        let mut t = docs_task("data/f.parquet");
        t.record_count = records;
        t.partition_spec = Some(Arc::new(site_spec()));
        t.partition = Some(Struct::from_iter([Some(Literal::Primitive(
            PrimitiveLiteral::String(site.to_string()),
        ))]));
        t
    }

    #[test]
    fn partition_record_counts_sums_by_identity_partition() {
        let tasks = vec![
            partitioned_task("us", Some(3)),
            partitioned_task("us", Some(2)),
            partitioned_task("eu", Some(5)),
        ];
        let by_site: std::collections::HashMap<String, u64> = partition_record_counts(&tasks)
            .expect("identity-partitioned → Some")
            .into_iter()
            .map(|(p, n)| match &p[0].1 {
                Value::Str(s) => (s.clone(), n),
                other => panic!("expected string partition, got {other:?}"),
            })
            .collect();
        assert_eq!(by_site["us"], 5, "us files summed by metadata record_count");
        assert_eq!(by_site["eu"], 5);
    }

    #[test]
    fn partition_record_counts_falls_back_when_not_cleanly_partitioned() {
        // A file with no manifest record_count can't be counted from metadata → None (full scan).
        assert!(partition_record_counts(&[partitioned_task("us", None)]).is_none());
        // An unpartitioned file (no partition/spec) → None.
        assert!(partition_record_counts(&[docs_task("data/f.parquet")]).is_none());
    }

    /// A source schema `site:String, id:String, n:Long` for predicate-builder tests.
    fn ice_schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_fields([
                Arc::new(NestedField::required(
                    1,
                    "site",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    2,
                    "id",
                    Type::Primitive(PrimitiveType::String),
                )),
                Arc::new(NestedField::required(
                    3,
                    "n",
                    Type::Primitive(PrimitiveType::Long),
                )),
            ])
            .build()
            .unwrap()
    }

    fn ckey(partition: Vec<(&str, Value)>, identifier: Vec<(&str, Value)>) -> CompositeKey {
        let own = |v: Vec<(&str, Value)>| v.into_iter().map(|(n, x)| (n.to_string(), x)).collect();
        CompositeKey::new(own(partition), own(identifier))
    }

    #[test]
    fn key_predicate_prunes_by_partition_and_identifier() {
        let schema = ice_schema();
        let k = ckey(
            vec![("site", Value::Str("plant-1".into()))],
            vec![("id", Value::Str("doc-10".into()))],
        );
        let p = key_predicate(&schema, &[&k]).expect("predicate");
        let s = p.to_string();
        assert!(
            s.contains("site") && s.contains("plant-1"),
            "partition pruned: {s}"
        );
        assert!(
            s.contains("id") && s.contains("doc-10"),
            "identifier pruned: {s}"
        );
    }

    #[test]
    fn key_predicate_ors_multiple_keys() {
        let schema = ice_schema();
        let a = ckey(vec![], vec![("id", Value::Str("a".into()))]);
        let b = ckey(vec![], vec![("id", Value::Str("b".into()))]);
        let p = key_predicate(&schema, &[&a, &b]).expect("predicate");
        let s = p.to_string();
        assert!(
            s.contains("\"a\"") && s.contains("\"b\""),
            "both keys present: {s}"
        );
    }

    #[test]
    fn key_predicate_is_none_on_type_mismatch_so_the_read_is_unfiltered() {
        let schema = ice_schema();
        // `id` is a String column but the key value is an Int — can't safely prune, so no predicate
        // (the caller reads unfiltered; the exact in-memory match still guarantees correctness).
        let k = ckey(vec![], vec![("id", Value::Int(5))]);
        assert!(key_predicate(&schema, &[&k]).is_none());
        // An unknown/absent column likewise yields None.
        let missing = ckey(vec![], vec![("nope", Value::Str("x".into()))]);
        assert!(key_predicate(&schema, &[&missing]).is_none());
    }

    #[test]
    fn key_predicate_is_none_for_no_keys() {
        assert!(key_predicate(&ice_schema(), &[]).is_none());
    }

    /// A temporal source schema `day:Date (partition), ts:Timestamp, tstz:Timestamptz, id:String`
    /// for the temporal-key predicate tests.
    fn temporal_ice_schema() -> IcebergSchema {
        IcebergSchema::builder()
            .with_fields([
                Arc::new(NestedField::required(
                    1,
                    "day",
                    Type::Primitive(PrimitiveType::Date),
                )),
                Arc::new(NestedField::required(
                    2,
                    "ts",
                    Type::Primitive(PrimitiveType::Timestamp),
                )),
                Arc::new(NestedField::required(
                    3,
                    "tstz",
                    Type::Primitive(PrimitiveType::Timestamptz),
                )),
                Arc::new(NestedField::required(
                    4,
                    "id",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn value_to_datum_types_temporal_keys_to_the_column() {
        let schema = temporal_ice_schema();
        let midnight = 20_625 * MICROS_PER_DAY; // 2026-06-21T00:00:00Z as canonical micros
        let instant = 1_782_000_123_456_789_i64;
        assert_eq!(
            value_to_datum(&schema, "day", &Value::Ts(midnight)),
            Some(Datum::date(20_625))
        );
        assert_eq!(
            value_to_datum(&schema, "ts", &Value::Ts(instant)),
            Some(Datum::timestamp_micros(instant))
        );
        assert_eq!(
            value_to_datum(&schema, "tstz", &Value::Ts(instant)),
            Some(Datum::timestamptz_micros(instant))
        );
    }

    #[test]
    fn key_predicate_prunes_on_temporal_keys() {
        // A date-partitioned, timestamp-identified key builds a real predicate — the
        // hydration fallback prunes instead of scanning the whole table.
        let schema = temporal_ice_schema();
        let k = ckey(
            vec![("day", Value::Ts(20_625 * MICROS_PER_DAY))],
            vec![("ts", Value::Ts(1_782_000_123_456_789))],
        );
        let p = key_predicate(&schema, &[&k]).expect("temporal predicate");
        let s = p.to_string();
        assert!(s.contains("day"), "date key pruned: {s}");
        assert!(s.contains("ts"), "timestamp key pruned: {s}");
    }

    #[test]
    fn key_predicate_is_none_for_a_date_key_with_intraday_micros() {
        // A DATE column can only be pruned by an exact UTC-midnight value; anything else could
        // build a predicate that *excludes* the row. None ⇒ safe unfiltered read.
        let schema = temporal_ice_schema();
        let not_midnight = 20_625 * MICROS_PER_DAY + 1;
        assert_eq!(
            value_to_datum(&schema, "day", &Value::Ts(not_midnight)),
            None
        );
        let k = ckey(
            vec![("day", Value::Ts(not_midnight))],
            vec![("id", Value::Str("x".into()))],
        );
        assert!(key_predicate(&schema, &[&k]).is_none());
    }

    #[test]
    fn array_value_normalizes_temporal_columns_to_canonical_micros() {
        use arrow_array::TimestampMicrosecondArray;
        let days = 20_625_i32; // 2026-06-21
        let micros_at_midnight = days as i64 * MICROS_PER_DAY;
        let instant_micros = 1_782_000_123_456_789_i64;

        let date32 = Date32Array::from(vec![days]);
        assert_eq!(array_value(&date32, 0), Some(Value::Ts(micros_at_midnight)));

        let date64 = Date64Array::from(vec![micros_at_midnight / 1_000]);
        assert_eq!(array_value(&date64, 0), Some(Value::Ts(micros_at_midnight)));

        let secs = TimestampSecondArray::from(vec![1_782_000_000_i64]);
        assert_eq!(
            array_value(&secs, 0),
            Some(Value::Ts(1_782_000_000_000_000))
        );

        let millis = TimestampMillisecondArray::from(vec![instant_micros / 1_000]);
        assert_eq!(
            array_value(&millis, 0),
            Some(Value::Ts(instant_micros / 1_000 * 1_000))
        );

        let micros = TimestampMicrosecondArray::from(vec![instant_micros]);
        assert_eq!(array_value(&micros, 0), Some(Value::Ts(instant_micros)));

        // The tz annotation is display metadata — the stored instant is already since-epoch.
        let micros_tz = TimestampMicrosecondArray::from(vec![instant_micros])
            .with_timezone("Europe/Madrid".to_string());
        assert_eq!(array_value(&micros_tz, 0), Some(Value::Ts(instant_micros)));

        // Nanos floor to micros (div_euclid) — including pre-epoch values.
        let nanos = TimestampNanosecondArray::from(vec![instant_micros * 1_000 + 999, -1_500]);
        assert_eq!(array_value(&nanos, 0), Some(Value::Ts(instant_micros)));
        assert_eq!(array_value(&nanos, 1), Some(Value::Ts(-2)));

        // Nulls are still None.
        let with_null = Date32Array::from(vec![None, Some(days)]);
        assert_eq!(array_value(&with_null, 0), None);
    }

    /// A two-batch `docs` file: ids 10,11 | 12 across `id` (Int64) + `body` (Utf8).
    fn docs_batches() -> Vec<RecordBatch> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
        ]));
        let b0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 11])),
                Arc::new(StringArray::from(vec!["alpha", "bravo"])),
            ],
        )
        .unwrap();
        let b1 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![12])),
                Arc::new(StringArray::from(vec!["charlie"])),
            ],
        )
        .unwrap();
        vec![b0, b1]
    }

    fn key_id(id: i64) -> CompositeKey {
        CompositeKey::new(vec![], vec![("id".into(), Value::Int(id))])
    }

    #[test]
    fn extract_full_rows_reads_positions_across_batches() {
        let batches = docs_batches();
        let rows = extract_full_rows(&batches, &[0, 2]);
        assert_eq!(rows[&0]["id"], Value::Int(10));
        assert_eq!(rows[&0]["body"], Value::Str("alpha".into()));
        assert_eq!(rows[&2]["body"], Value::Str("charlie".into()));
    }

    #[test]
    fn extract_full_rows_omits_out_of_range() {
        let batches = docs_batches(); // 3 rows total (0..=2)
        let rows = extract_full_rows(&batches, &[1, 3]);
        assert!(rows.contains_key(&1));
        assert!(!rows.contains_key(&3), "out-of-range omitted, not an error");
    }

    #[test]
    fn project_row_narrows_columns() {
        let full = full_row(&docs_batches()[0], 1);
        let narrowed = project_row(&full, &Projection::Columns(vec!["body".into()]));
        assert_eq!(narrowed.keys().collect::<Vec<_>>(), vec!["body"]);
        assert_eq!(narrowed["body"], Value::Str("bravo".into()));
    }

    #[test]
    fn row_matches_key_verifies_identity() {
        let full = full_row(&docs_batches()[0], 0); // id=10
        assert!(row_matches_key(&full, &key_id(10)), "matching key verifies");
        assert!(
            !row_matches_key(&full, &key_id(99)),
            "a different key at this position is a phantom — rejected"
        );
    }

    /// A minimal delete-free [`FileScanTask`] for `path` — pass 1 only reads its
    /// `data_file_path` + `deletes` on the point-read path; the rest is inert.
    fn docs_task(path: &str) -> FileScanTask {
        FileScanTask {
            file_size_in_bytes: 0,
            start: 0,
            length: 0,
            record_count: None,
            data_file_path: path.to_string(),
            data_file_format: iceberg::spec::DataFileFormat::Parquet,
            schema: Arc::new(ice_schema()),
            project_field_ids: vec![],
            predicate: None,
            deletes: vec![],
            partition: None,
            partition_spec: None,
            name_mapping: None,
            case_sensitive: false,
        }
    }

    fn locator(file: &str, pos: u64) -> RowLocator {
        RowLocator {
            iceberg_file: file.to_string(),
            row_position: pos,
        }
    }

    /// Hydration pass 1 over **real parquet files**: a batch mixing two files
    /// resolves via targeted point reads and comes back in **input order**; a wrong-key position
    /// (phantom), an out-of-range position, and a rewritten-away file all verify-fail to `None`
    /// (→ the pass-2 fallback, unchanged) and are **omitted** from the assembled rows.
    #[tokio::test]
    async fn pass1_resolves_mixed_files_in_input_order_and_marks_stale() {
        use crate::test_util::{body_for, fs_file_io, write_docs_parquet};

        let dir = tempfile::tempdir().unwrap();
        let f0 = dir.path().join("f0.parquet").to_str().unwrap().to_string();
        let f1 = dir.path().join("f1.parquet").to_str().unwrap().to_string();
        write_docs_parquet(&f0, 0, 300, 50); // ids 0..300, 6 row groups
        write_docs_parquet(&f1, 1000, 300, 50); // ids 1000..1300

        let key = |id: i64| CompositeKey::new(vec![], vec![("id".into(), Value::Int(id))]);
        let requests = vec![
            (key(1005), Some(locator(&f1, 5))),         // resolves (file 1)
            (key(7), Some(locator(&f0, 7))), // resolves (file 0) — interleaved input order
            (key(999), Some(locator(&f0, 8))), // wrong key at position 8 (id=8) → stale
            (key(1299), Some(locator(&f1, 299))), // last row of file 1 → resolves
            (key(42), Some(locator(&f0, 9_999))), // past EOF → stale
            (key(1), Some(locator("gone.parquet", 1))), // file rewritten away → stale
            (key(2), None),                  // known stale (dead-file bitmap) → no read at all
        ];
        let tasks = vec![docs_task(&f0), docs_task(&f1)];

        let resolved = resolve_pass1(&fs_file_io(), &tasks, &requests)
            .await
            .expect("pass 1");
        assert_eq!(
            resolved.iter().map(Option::is_some).collect::<Vec<_>>(),
            vec![true, true, false, true, false, false, false],
            "verify: matches resolve; phantoms/OOR/missing-file/known-stale go stale"
        );

        let rows = assemble_rows(&requests, resolved, &Projection::All);
        let ids: Vec<&Value> = rows.iter().map(|r| &r.fields["id"]).collect();
        assert_eq!(
            ids,
            vec![&Value::Int(1005), &Value::Int(7), &Value::Int(1299)],
            "rows come back in input order, absent keys omitted"
        );
        assert_eq!(rows[1].key, key(7), "row carries its request key");
        assert_eq!(
            rows[0].fields["body"],
            Value::Str(body_for(1005)),
            "full-row equality with the source row"
        );
    }

    /// The hydration hot path composed with the snapshot-pinned [`PlanCache`] over **real
    /// parquet files**: two hydrates at the same snapshot run exactly one
    /// planning pass; an appended file (snapshot advance) forces a fresh plan whose rows
    /// then resolve correctly — the cached stale plan alone could not see them.
    #[tokio::test]
    async fn plan_cache_reuses_at_same_snapshot_and_replans_on_advance() {
        use crate::test_util::{fs_file_io, write_docs_parquet};
        use std::sync::atomic::{AtomicUsize, Ordering};

        let dir = tempfile::tempdir().unwrap();
        let f0 = dir.path().join("f0.parquet").to_str().unwrap().to_string();
        let f1 = dir.path().join("f1.parquet").to_str().unwrap().to_string();
        write_docs_parquet(&f0, 0, 100, 50); // ids 0..100 — present at snapshot 1
        write_docs_parquet(&f1, 1000, 100, 50); // ids 1000..1100 — appended at snapshot 2

        // The table's plan per snapshot, exactly as `hydrate` would learn it from
        // `load_table` + `plan_files`: snapshot 1 sees f0; snapshot 2 sees f0 + f1.
        let plan_at = |snapshot: i64| match snapshot {
            1 => vec![docs_task(&f0)],
            _ => vec![docs_task(&f0), docs_task(&f1)],
        };
        let cache: PlanCache<Arc<Vec<FileScanTask>>> = PlanCache::new(PLAN_CACHE_CAP);
        let plans_run = AtomicUsize::new(0);

        // One "hydrate" = hydrate()'s pass 1 with the cached-or-planned task set.
        let hydrate_at = |snapshot: i64, requests: Vec<(CompositeKey, Option<RowLocator>)>| {
            let (cache, plans_run) = (&cache, &plans_run);
            async move {
                let (tasks, hit) = cache
                    .get_or_plan("g.docs", snapshot, || async {
                        plans_run.fetch_add(1, Ordering::SeqCst);
                        Ok::<_, SourceError>(Arc::new(plan_at(snapshot)))
                    })
                    .await
                    .unwrap();
                let resolved = resolve_pass1(&fs_file_io(), &tasks, &requests)
                    .await
                    .unwrap();
                (assemble_rows(&requests, resolved, &Projection::All), hit)
            }
        };

        // Two hydrates at snapshot 1 → correct rows, one planning pass (the second is a hit).
        let (rows, hit) = hydrate_at(1, vec![(key_id(7), Some(locator(&f0, 7)))]).await;
        assert_eq!(rows[0].fields["id"], Value::Int(7));
        assert!(!hit);
        let (rows, hit) = hydrate_at(1, vec![(key_id(42), Some(locator(&f0, 42)))]).await;
        assert_eq!(rows[0].fields["id"], Value::Int(42));
        assert!(hit, "same snapshot → cached plan reused");
        assert_eq!(plans_run.load(Ordering::SeqCst), 1, "manifests read once");

        // Snapshot advances (f1 appended) → replan → the appended row hydrates correctly,
        // and rows from the old plan's file still do.
        let (rows, hit) = hydrate_at(
            2,
            vec![
                (key_id(1005), Some(locator(&f1, 5))),
                (key_id(7), Some(locator(&f0, 7))),
            ],
        )
        .await;
        assert!(!hit, "snapshot advance → fresh plan");
        assert_eq!(plans_run.load(Ordering::SeqCst), 2);
        assert_eq!(
            rows.len(),
            2,
            "appended file visible only via the fresh plan"
        );
        assert_eq!(rows[0].fields["id"], Value::Int(1005));
        assert_eq!(rows[1].fields["id"], Value::Int(7));
    }

    #[test]
    fn index_batch_indexes_only_wanted_keys_for_fallback() {
        // A stale locator (wrong file/position) is re-found by key from a scan — and only the wanted
        // (stale) keys are indexed, so an unfiltered fallback scan doesn't materialize every row.
        let batch = docs_batches()[0].clone(); // ids 10, 11 in data/x at rows 0, 1
        let wanted: HashSet<Vec<u8>> = [key_id(11).encode()].into_iter().collect();
        let mut index = HashMap::new();
        index_batch(
            &mut index,
            &batch,
            "data/x.parquet",
            0,
            &wanted,
            &[],
            &["id".to_string()],
        );
        assert_eq!(
            index.len(),
            1,
            "only the wanted key is indexed, not every row"
        );
        assert!(
            !index.contains_key(&key_id(10).encode()),
            "unwanted key skipped"
        );
        let (full, locator) = index.get(&key_id(11).encode()).expect("found by key");
        assert_eq!(full["body"], Value::Str("bravo".into()));
        assert_eq!(locator.iceberg_file, "data/x.parquet");
        assert_eq!(locator.row_position, 1);
    }

    #[test]
    fn index_batch_detects_duplicate_pks_deterministically() {
        // A fixture with a GENUINE duplicate key: id 11 appears on three distinct rows
        // (twice in data/x, once more in data/y), id 10 once. The scan must count each
        // extra row, warn (rate-limited), keep the deterministic winner — highest
        // (file, position) — and still produce exactly one entry per requested key, so
        // the caller's found/requested accounting is unaffected.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch_x = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![10, 11, 11])),
                Arc::new(StringArray::from(vec!["alpha", "first-11", "second-11"])),
            ],
        )
        .unwrap();
        let wanted: HashSet<Vec<u8>> = [key_id(10).encode(), key_id(11).encode()]
            .into_iter()
            .collect();
        let mut index = HashMap::new();

        // Same file: row 2 out-positions row 1 → last (highest position) wins.
        let dups = index_batch(
            &mut index,
            &batch_x,
            "data/x.parquet",
            0,
            &wanted,
            &[],
            &["id".to_string()],
        );
        assert_eq!(dups, 1, "one extra row for id 11");
        assert_eq!(
            index.len(),
            2,
            "still one entry per key — accounting intact"
        );
        let (full, loc) = &index[&key_id(11).encode()];
        assert_eq!(full["body"], Value::Str("second-11".into()));
        assert_eq!(
            (loc.iceberg_file.as_str(), loc.row_position),
            ("data/x.parquet", 2)
        );

        // A later file that sorts HIGHER wins even at a lower row position...
        let batch_y = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![11])),
                Arc::new(StringArray::from(vec!["third-11"])),
            ],
        )
        .unwrap();
        let dups = index_batch(
            &mut index,
            &batch_y,
            "data/y.parquet",
            0,
            &wanted,
            &[],
            &["id".to_string()],
        );
        assert_eq!(dups, 1);
        let (full, loc) = &index[&key_id(11).encode()];
        assert_eq!(full["body"], Value::Str("third-11".into()));
        assert_eq!(
            (loc.iceberg_file.as_str(), loc.row_position),
            ("data/y.parquet", 0)
        );

        // ... and a lower-sorting file NEVER displaces it (deterministic, not scan-order).
        let batch_w = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![11])),
                Arc::new(StringArray::from(vec!["loser-11"])),
            ],
        )
        .unwrap();
        let dups = index_batch(
            &mut index,
            &batch_w,
            "data/w.parquet",
            5,
            &wanted,
            &[],
            &["id".to_string()],
        );
        assert_eq!(dups, 1, "counted even though the held row wins");
        let (full, loc) = &index[&key_id(11).encode()];
        assert_eq!(
            full["body"],
            Value::Str("third-11".into()),
            "winner unchanged"
        );
        assert_eq!(loc.iceberg_file, "data/y.parquet");

        // Warning path: the detections above went through `warn_duplicate_pk`, which
        // consumed the process-wide rate-limit window — a direct call inside the same
        // window is suppressed (returns false) while the count above still recorded
        // every occurrence.
        assert!(
            !warn_duplicate_pk(
                &key_id(11),
                &locator("data/y.parquet", 0),
                &locator("data/w.parquet", 5),
            ),
            "rate limit engaged: the scan's own warning consumed the window"
        );
    }

    #[test]
    fn index_batch_refinds_a_temporal_key_for_fallback() {
        // A timestamp-keyed row is re-found by key: the Arrow timestamp column extracts
        // to `Value::Ts` (canonical micros) whose encoding matches a wanted key built from `Ts` —
        // so the verify-and-fall-back path no longer silently drops temporal key fields.
        use arrow_array::TimestampMicrosecondArray;
        let micros = [1_782_000_000_000_000_i64, 1_782_000_123_456_789];
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("body", DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(micros.to_vec())),
                Arc::new(StringArray::from(vec!["alpha", "bravo"])),
            ],
        )
        .unwrap();
        let key = |m: i64| CompositeKey::new(vec![], vec![("ts".into(), Value::Ts(m))]);
        let wanted: HashSet<Vec<u8>> = [key(micros[1]).encode()].into_iter().collect();
        let mut index = HashMap::new();
        index_batch(
            &mut index,
            &batch,
            "data/t.parquet",
            0,
            &wanted,
            &[],
            &["ts".to_string()],
        );
        assert_eq!(index.len(), 1, "only the wanted temporal key is indexed");
        let (full, locator) = index
            .get(&key(micros[1]).encode())
            .expect("found by ts key");
        assert_eq!(full["body"], Value::Str("bravo".into()));
        assert_eq!(full["ts"], Value::Ts(micros[1]));
        assert_eq!(locator.row_position, 1);
    }

    #[test]
    fn batch_to_docs_builds_keyed_located_documents() {
        use growlerdb_core::{IndexDefinition, SourceField, SourceSchema, SourceType};

        // Index: identifier `id` (KEYWORD), fields id + body.
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let index = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: ALL }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();

        let batches = docs_batches(); // id (Int64) 10,11 | 12 ; body strings
        let mut docs = Vec::new();
        batch_to_docs(&index, &batches[0], "data/f0.parquet", 0, &mut docs);
        batch_to_docs(&index, &batches[1], "data/f0.parquet", 2, &mut docs);

        assert_eq!(docs.len(), 3);
        // Key carries the identifier; locator carries file + absolute position.
        assert_eq!(docs[0].doc.key.get("id"), Some(&Value::Int(10)));
        assert_eq!(docs[0].row_position, 0);
        assert_eq!(docs[2].doc.key.get("id"), Some(&Value::Int(12)));
        assert_eq!(docs[2].row_position, 2);
        assert_eq!(docs[2].iceberg_file, "data/f0.parquet");
        // Fields include the mapped columns.
        assert_eq!(docs[1].doc.fields["body"], Value::Str("bravo".into()));
    }

    #[test]
    fn array_value_maps_scalar_types_and_nulls() {
        let ints = Int64Array::from(vec![Some(7), None]);
        assert_eq!(array_value(&ints, 0), Some(Value::Int(7)));
        assert_eq!(array_value(&ints, 1), None); // null → None
        let bools = BooleanArray::from(vec![true]);
        assert_eq!(array_value(&bools, 0), Some(Value::Bool(true)));
        let floats = Float64Array::from(vec![1.5]);
        assert_eq!(array_value(&floats, 0), Some(Value::Float(1.5)));
    }

    #[test]
    fn arrow_types_map_to_source_types() {
        let arrow = Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("count", DataType::Int64, true),
            Field::new("ratio", DataType::Float64, true),
            Field::new("ok", DataType::Boolean, true),
            Field::new("day", DataType::Date32, true),
            Field::new("blob", DataType::Binary, true),
        ]);
        let src = arrow_schema_to_source(&arrow, vec!["day".into()], vec!["count".into()]);

        assert_eq!(src.partition_fields, vec!["day".to_string()]);
        assert_eq!(src.identifier_fields, vec!["count".to_string()]);
        let ty = |p: &str| src.field(p).unwrap().ty;
        assert_eq!(ty("body"), SourceType::String);
        assert_eq!(ty("count"), SourceType::Long);
        assert_eq!(ty("ratio"), SourceType::Double);
        assert_eq!(ty("ok"), SourceType::Bool);
        assert_eq!(ty("day"), SourceType::Date);
        assert_eq!(ty("blob"), SourceType::Binary);
    }

    #[test]
    fn arrow_schema_resolves_an_index_definition() {
        // End-to-end at the source seam: an Arrow schema → SourceSchema → a
        // resolved index, exercising derive-from-source key + ALL auto-mapping.
        let arrow = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, true),
        ]);
        let src = arrow_schema_to_source(&arrow, vec![], vec!["id".into()]);
        let def = growlerdb_core::IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\n",
        )
        .unwrap();
        let resolved = def.resolve(&src).expect("resolve");
        assert_eq!(resolved.key.identifier_fields, vec!["id".to_string()]);
        assert_eq!(resolved.fields.len(), 2);
    }

    /// A batch with a top-level `id` and a nested `actor: { user, id }` struct.
    fn nested_batch() -> RecordBatch {
        use arrow_array::ArrayRef;
        let actor_fields = Fields::from(vec![
            Field::new("user", DataType::Utf8, true),
            Field::new("id", DataType::Int64, true),
        ]);
        let actor = StructArray::new(
            actor_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])) as ArrayRef,
                Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef,
            ],
            None,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("actor", DataType::Struct(actor_fields), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10_i64, 11])),
                Arc::new(actor),
            ],
        )
        .unwrap()
    }

    #[test]
    fn nested_struct_schema_flattens_to_dotted_paths() {
        let src =
            arrow_schema_to_source(nested_batch().schema().as_ref(), vec![], vec!["id".into()]);
        let paths: Vec<&str> = src.fields.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, vec!["id", "actor.user", "actor.id"]);
        assert_eq!(src.field("actor.user").unwrap().ty, SourceType::String);
        assert_eq!(src.field("actor.id").unwrap().ty, SourceType::Long);
    }

    #[test]
    fn batch_to_docs_extracts_nested_struct_values() {
        let batch = nested_batch();
        let src = arrow_schema_to_source(batch.schema().as_ref(), vec![], vec!["id".into()]);
        let idx = growlerdb_core::IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: ALL }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();

        let mut out = Vec::new();
        batch_to_docs(&idx, &batch, "data/f.parquet", 0, &mut out);
        assert_eq!(out.len(), 2);

        // Row 0: the nested leaves resolve to their dotted paths, and the top-level
        // key field resolves too.
        let d0 = &out[0].doc;
        assert_eq!(
            d0.fields.get("actor.user").unwrap().to_index_string(),
            "alice"
        );
        assert_eq!(d0.fields.get("actor.id"), Some(&Value::Int(1)));
        assert_eq!(d0.key.get("id"), Some(&Value::Int(10)));
        assert_eq!(
            out[1]
                .doc
                .fields
                .get("actor.user")
                .unwrap()
                .to_index_string(),
            "bob"
        );
    }

    #[test]
    fn local_config_has_expected_endpoints() {
        let c = IcebergConfig::local();
        assert!(c.uri.contains(":8181"));
        assert!(c.props().contains_key("s3.endpoint"));
    }

    #[test]
    fn from_env_overrides_defaults_and_clears_optional_on_empty() {
        // Defaults when unset (these vars aren't set elsewhere in the suite).
        assert_eq!(IcebergConfig::from_env().uri, IcebergConfig::local().uri);

        std::env::set_var("GROWLERDB_CATALOG_URI", "http://polaris:8181/api/catalog");
        std::env::set_var("GROWLERDB_S3_ENDPOINT", "http://minio:9000");
        std::env::set_var("GROWLERDB_CATALOG_CREDENTIAL", ""); // empty → anonymous
        let c = IcebergConfig::from_env();
        assert_eq!(c.uri, "http://polaris:8181/api/catalog");
        assert_eq!(c.s3_endpoint, "http://minio:9000");
        assert_eq!(c.credential, None);
        std::env::remove_var("GROWLERDB_CATALOG_URI");
        std::env::remove_var("GROWLERDB_S3_ENDPOINT");
        std::env::remove_var("GROWLERDB_CATALOG_CREDENTIAL");
    }

    /// Live read against the local dev stack. Prereqs:
    ///   `just up` (brings up MinIO + Polaris and seeds growlerdb.docs), and
    ///   `127.0.0.1 minio` in /etc/hosts (see deploy/compose/README.md).
    /// Then: `cargo test -p growlerdb-source -- --ignored`
    #[tokio::test]
    #[ignore = "requires the local dev stack (just up) + `127.0.0.1 minio` in /etc/hosts"]
    async fn reads_seeded_docs_table() {
        let reader = IcebergReader::connect(&IcebergConfig::local())
            .await
            .expect("connect");
        let res = reader.read_current("growlerdb.docs").await.expect("read");
        assert!(res.row_count() >= 1, "expected seeded rows");
        for b in &res.batches {
            assert!(!b.data_file.is_empty(), "every batch carries a source file");
        }
    }

    /// Regression (no stack): read a **real Spark merge-on-read** table off local disk via
    /// `StaticTable` and assert iceberg-rust honors its history delete *correctly*. The table is
    /// `append(r0..r4) → DELETE r2 (writes a positional delete file) → append(r5..r9)`, so the
    /// current snapshot has **9** live rows; a correct reader returns 9 (not 10 — that would mean a
    /// deleted row was resurrected; not 0 — a mis-scoped history delete). pyiceberg can't produce
    /// this shape (copy-on-write writes no delete file), so a pyiceberg fixture couldn't exercise
    /// the delete path at all.
    ///
    /// Generate the fixture first (Spark, in `connector/`):
    ///   `T85_WAREHOUSE=/tmp/t85wh mvn test -Dgroups=fixturegen -Dtest.excludedGroups= \
    ///      -Dtest=T85DeleteHistoryFixtureTest`
    /// Then: `cargo test -p growlerdb-source -- --ignored reads_real_mor_delete_in_history`
    #[tokio::test]
    #[ignore = "requires the Spark MoR fixture at /tmp/t85wh (see connector T85DeleteHistoryFixtureTest)"]
    async fn reads_real_mor_delete_in_history() {
        use iceberg::io::FileIOBuilder;
        use iceberg::table::StaticTable;
        use iceberg_storage_opendal::OpenDalStorageFactory;

        let meta = "/tmp/t85wh/ns/t85/metadata/v4.metadata.json";
        let file_io = FileIOBuilder::new(std::sync::Arc::new(OpenDalStorageFactory::Fs)).build();
        let ident = TableIdent::from_strs(["ns", "t85"]).unwrap();
        let tbl = StaticTable::from_metadata_file(meta, ident, file_io)
            .await
            .expect("static table")
            .into_table();

        let tasks: Vec<FileScanTask> = tbl
            .scan()
            .select_all()
            .build()
            .unwrap()
            .plan_files()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        // Honor source deletes (production behavior).
        let batches = read_tasks(tbl.file_io().clone(), tasks, &HashSet::new())
            .await
            .expect("read");
        let rows: usize = batches.iter().map(|b| b.batch.num_rows()).sum();
        assert_eq!(
            rows, 9,
            "MoR history delete must be honored (9 live rows, not 10 or 0)"
        );
    }
}
