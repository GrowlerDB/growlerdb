//! Source connectors for GrowlerDB: an Iceberg batch reader that maps a table's rows to
//! documents, and store-less hydration that re-finds a document's source row by key.

use std::collections::{BTreeMap, HashMap, HashSet};
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
    CompositeKey, Document, HydrateRequest, HydratedRow, LocatedDoc, Projection, ResolvedIndex,
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

mod plan_cache;
mod shared_reader;
mod trino;

pub use plan_cache::{PlanCache, PLAN_CACHE_CAP};
pub use shared_reader::SharedReader;
pub use trino::{shared_hydrator, TrinoConfig, TrinoHydrator};

// The table IO handle [`TablePlan`] and [`fs_file_io`] hand around — re-exported so callers
// needn't depend on the `iceberg` crate.
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

    /// A parquet read failed.
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),

    /// A referenced data file was absent from the current table plan (e.g. compacted away).
    #[error("data file not found in current table plan: {0}")]
    FileNotFound(String),

    /// A row position was out of range for its data file.
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

    /// The **interim Trino hydration lane** (D48) failed — Trino unreachable, a bad HTTP response,
    /// or a query error. Surfaced as a loud error (D45), never a silent empty/partial hydration.
    #[error("trino: {0}")]
    Trino(String),
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

/// The result of reading a table snapshot: its Arrow schema and record batches.
pub struct ReadResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

impl ReadResult {
    /// Total number of rows across all batches.
    pub fn row_count(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

/// Docs per chunk for the streamed read: bounds peak memory while keeping the per-chunk
/// commit count (and thus segment count) reasonable. ~50k telemetry docs ≈ a few MB.
const STREAM_CHUNK: usize = 50_000;

/// Reads Apache Iceberg tables via a REST catalog.
pub struct IcebergReader {
    catalog: RestCatalog,
    /// Snapshot-pinned plan cache for [hydration](Self::hydrate)'s unpredicated
    /// current-snapshot plan: only effective when the reader itself is long-lived — hold it
    /// via [`SharedReader`] rather than connecting per call.
    plans: PlanCache<Arc<Vec<FileScanTask>>>,
}

impl IcebergReader {
    /// Connect to the catalog described by `cfg`.
    pub async fn connect(cfg: &IcebergConfig) -> Result<Self> {
        // `OpenDalStorageFactory` wraps every operator in an opendal `RetryLayer` internally, so
        // scans + hydration already retry transient 5xx/SlowDown — no separate layer here. Its
        // default (3 attempts, no jitter) is fine for a single-reader-per-index source.
        let catalog = RestCatalogBuilder::default()
            .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
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
    ) -> Result<(ReadResult, i64, Option<i64>)> {
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        let schema = Arc::new(schema_to_arrow_schema(tbl.metadata().current_schema())?);
        let current_snapshot = tbl.metadata().current_snapshot_id().unwrap_or(0);
        // The head's lineage sequence number, captured in the same table load so a catch-up can stamp
        // an ordered checkpoint without a second metadata read (no TOCTOU). `None` on a v1 table / an
        // empty table (no order available).
        let current_sequence = tbl
            .metadata()
            .current_snapshot()
            .map(|s| s.sequence_number());

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
                    current_sequence,
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
        Ok((
            ReadResult { schema, batches },
            current_snapshot,
            current_sequence,
        ))
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

    /// The source table's **sort-order column names** usable for equality pruning — each
    /// `default_sort_order` field with an **Identity** transform, resolved to its schema column
    /// name. A sorted table's compaction lays rows out by these columns, so a hydration fallback can
    /// AND the row's own sort-key values onto its key predicate and let Iceberg prune files by
    /// manifest min/max — the heal for an unpartitioned, hash-routed random key. Non-identity
    /// transforms (`day`/`bucket`/…) don't preserve equality, so they're excluded (a hint on one
    /// could exclude the matching row). Empty for an unsorted table. One catalog metadata load.
    pub async fn sort_field_names(&self, table: &str) -> Result<Vec<String>> {
        // Read fresh each call — NOT cached: the sort order is declared by compaction (WRITE ORDERED
        // BY) *after* a table is first hydrated (the cold-sync convergence sample), so a cache would
        // pin the pre-compaction empty order and never prune. The extra `load_table` lands only on the
        // hydrated path (already an Iceberg round-trip), so its cost is in the noise.
        let ident = TableIdent::from_strs(table.split('.'))?;
        let tbl = self.catalog.load_table(&ident).await?;
        Ok(sort_field_names_of(&tbl))
    }

    /// **Store-less hydration** ([Flow 2]): resolve each request's composite key to its
    /// authoritative row by a single **pruned key-equality scan** of the current snapshot.
    ///
    /// There is no stored `(file, position)` locator — every request re-finds its row by key.
    /// The scan pushes an equality predicate over each key's partition + identifier fields (plus
    /// its **sort-key prune hints** — the row's own fast `ts`, etc.) so a sorted/partitioned
    /// source prunes by manifest min/max to the files that can hold the row. Correctness doesn't
    /// depend on the predicate: every candidate is re-verified against the exact key, so a
    /// superset (or an unfiltered read on any predicate/scan error) is always safe. Rows come
    /// back in input order (genuinely-absent keys omitted).
    ///
    /// [Flow 2]: ../../../okf/system/architecture.md
    pub async fn hydrate(
        &self,
        table: &str,
        requests: &[HydrateRequest],
        projection: &Projection,
    ) -> Result<HydrationResult> {
        // Max bytes the key scan reads per hydration call (decoded, row-group-granular). A
        // well-pruned plan is far under this (a sort/partition-clustered key → a file or two → every
        // hit resolved); an *unprunable* key (large, unclustered, unpartitioned, so per-file min/max
        // spans the space) is capped here instead of scanning the whole snapshot per hit — it serves
        // what it cheaply finds and omits the rest (graceful, assemble_rows). 256 MiB keeps a
        // worst-case scan to a couple of seconds even against few-but-huge (~1 GB) compacted files.
        const FALLBACK_MAX_SCAN_BYTES: u64 = 256 * 1024 * 1024;
        if requests.is_empty() {
            return Ok(HydrationResult::default());
        }
        // One catalog REST call to learn the current snapshot; the unpredicated plan is reused from
        // the snapshot-pinned cache until the snapshot advances. The key scan below is
        // per-request-predicated and stays uncached.
        let (tbl, _tasks, plan_cache_hit) = self.load_and_plan(table).await?;

        // Each request carries its key plus its **sort-key prune hints** — both go into the
        // predicate so a sorted table prunes by manifest min/max on the sort key, not just the
        // (unprunable) random identifier.
        let entries: Vec<(&CompositeKey, &[(String, Value)])> = requests
            .iter()
            .map(|req| (&req.key, req.prune.as_slice()))
            .collect();
        let predicate = key_predicate(tbl.metadata().current_schema(), &entries);
        let (partition_names, identifier_names) = key_field_names(&requests[0].key);
        // Only the wanted keys are indexed and the scan streams with early-exit, so even the
        // unfiltered (`None` predicate) path is bounded in memory and cost.
        let wanted: HashSet<Vec<u8>> = entries.iter().map(|(k, _)| k.encode()).collect();
        let (index, duplicate_pks) = match scan_stale_index(
            &tbl,
            predicate.clone(),
            &wanted,
            &partition_names,
            &identifier_names,
            FALLBACK_MAX_SCAN_BYTES,
        )
        .await
        {
            Ok(found) => found,
            // A pruned scan that errored (e.g. an unexpected type binding) must never turn a
            // present row into a miss — fall back to the unfiltered scan (same file budget).
            Err(_) if predicate.is_some() => {
                scan_stale_index(
                    &tbl,
                    None,
                    &wanted,
                    &partition_names,
                    &identifier_names,
                    FALLBACK_MAX_SCAN_BYTES,
                )
                .await?
            }
            Err(e) => return Err(e),
        };
        let resolved: Vec<Option<BTreeMap<String, Value>>> = requests
            .iter()
            .map(|req| index.get(&req.key.encode()).map(|(full, _)| full.clone()))
            .collect();

        let rows = assemble_rows(requests, resolved, projection);
        Ok(HydrationResult {
            rows,
            plan_cache_hit: Some(plan_cache_hit),
            duplicate_pks,
        })
    }

    /// `load_table` + the snapshot-pinned plan for its current snapshot (cached per
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
    /// hydration uses (one catalog call; manifest reads only on snapshot advance), so the
    /// steady-state fetch costs one REST call and a cache hit. Observing table metadata is
    /// read-only — it imposes nothing on the source.
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
    /// the composite key + indexed fields (per `index`). Full snapshot, append-only.
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
        for batch in &read.batches {
            batch_to_docs(index, batch, &mut docs);
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

    /// Cheap **source-health** signals for the Ingestion/Observability view, from table metadata
    /// only — **no scan**. GrowlerDB reads O(files) on the query path, so a source accumulating
    /// small files or a long snapshot history slows it down; these gauges let operators diagnose
    /// that (the remedy — Iceberg compaction / `expire_snapshots` — stays the user's).
    ///
    /// Everything comes from the current snapshot's `summary` (`total-*` properties) plus the
    /// retained-snapshot count — one catalog load, no manifest reads. An omitted property reads as 0.
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
            let reader =
                ArrowReaderBuilder::new(file_io.clone(), iceberg::Runtime::current()).build();
            let task_stream =
                futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) })
                    .boxed();
            let mut stream = reader.read(task_stream)?.stream();
            while let Some(batch) = stream.try_next().await? {
                batch_to_docs(index, &batch, &mut chunk);
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
            let reader =
                ArrowReaderBuilder::new(file_io.clone(), iceberg::Runtime::current()).build();
            let task_stream =
                futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) })
                    .boxed();
            let mut stream = reader.read(task_stream)?.stream();
            while let Some(batch) = stream.try_next().await? {
                batch_to_docs(index, &batch, &mut chunk);
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
        let (read, snapshot_id, _seq) = self.read_appended_since(table, since_snapshot).await?;
        let mut docs = Vec::with_capacity(read.row_count());
        for batch in &read.batches {
            batch_to_docs(index, batch, &mut docs);
        }
        Ok(DocumentBatch { docs, snapshot_id })
    }

    /// As [`read_documents_appended_since`](Self::read_documents_appended_since), but also carrying
    /// the head snapshot's lineage **sequence number** — so a reindex **write catch-up** (append
    /// fast-path) can stamp the staged generation with an *ordered* [`SourceCheckpoint`] at the exact
    /// head it caught up to. The sequence is captured in the same table load as the read (no TOCTOU);
    /// `sequence_number = None` on a v1 / empty table, where the checkpoint carries no order.
    pub async fn read_documents_appended_since_ordered(
        &self,
        table: &str,
        index: &ResolvedIndex,
        since_snapshot: Option<i64>,
    ) -> Result<OrderedDocumentBatch> {
        let (read, snapshot_id, sequence_number) =
            self.read_appended_since(table, since_snapshot).await?;
        let mut docs = Vec::with_capacity(read.row_count());
        for batch in &read.batches {
            batch_to_docs(index, batch, &mut docs);
        }
        Ok(OrderedDocumentBatch {
            docs,
            snapshot_id,
            sequence_number,
        })
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
    /// The table's IO stack — reads the plan's data files (the pruned hydration key scan).
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

/// As [`DocumentBatch`], but carrying the head snapshot's lineage **sequence number** — so a reindex
/// catch-up can stamp an *ordered* [`SourceCheckpoint`](growlerdb_core::SourceCheckpoint) at the head
/// it read to. See [`IcebergReader::read_documents_appended_since_ordered`].
pub struct OrderedDocumentBatch {
    /// The documents, each with its source location.
    pub docs: Vec<LocatedDoc>,
    /// The Iceberg snapshot id these documents were read from.
    pub snapshot_id: i64,
    /// The head snapshot's lineage sequence number (`None` = v1 / empty table, no order).
    pub sequence_number: Option<i64>,
}

/// Read each `FileScanTask` into record batches, skipping any whose data file is in
/// `exclude` (the append fast-path's already-seen files).
async fn read_tasks(
    file_io: iceberg::io::FileIO,
    tasks: Vec<FileScanTask>,
    exclude: &HashSet<String>,
) -> Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    for task in tasks {
        if exclude.contains(&task.data_file_path) {
            continue;
        }
        let reader = ArrowReaderBuilder::new(file_io.clone(), iceberg::Runtime::current()).build();
        let task_stream =
            futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) }).boxed();
        let mut stream = reader.read(task_stream)?.stream();
        while let Some(batch) = stream.try_next().await? {
            batches.push(batch);
        }
    }
    Ok(batches)
}

/// Map each row of `batch` to a [`LocatedDoc`] per the resolved `index`, appending to `out`.
/// Hydration re-finds rows by key, so nothing but the document is carried (store-less).
fn batch_to_docs(index: &ResolvedIndex, batch: &RecordBatch, out: &mut Vec<LocatedDoc>) {
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
        });
    }
}

/// The outcome of [hydration](IcebergReader::hydrate): the resolved rows in request order.
#[derive(Debug, Clone, Default)]
pub struct HydrationResult {
    /// The hydrated rows, in request order (genuinely-absent keys omitted).
    pub rows: Vec<HydratedRow>,
    /// Whether the plan came from the snapshot-pinned [`PlanCache`] (`Some(true)`),
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

/// Final assembly of [hydration](IcebergReader::hydrate): the resolved rows back in **request
/// order**, genuinely-absent keys omitted, each row narrowed to `projection`.
fn assemble_rows(
    requests: &[HydrateRequest],
    resolved: Vec<Option<BTreeMap<String, Value>>>,
    projection: &Projection,
) -> Vec<HydratedRow> {
    requests
        .iter()
        .zip(resolved)
        .filter_map(|(req, full)| {
            full.map(|full| HydratedRow {
                key: req.key.clone(),
                fields: project_row(&full, projection),
            })
        })
        .collect()
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

/// The partition + identifier field names of a composite key.
fn key_field_names(key: &CompositeKey) -> (Vec<String>, Vec<String>) {
    let names = |fields: &[(String, Value)]| fields.iter().map(|(n, _)| n.clone()).collect();
    (names(&key.partition), names(&key.identifier))
}

/// A scanned row's source coordinates — `(data file, row position)`. Used purely as the
/// deterministic duplicate-PK tiebreak key (the highest one wins); no locator is stored.
struct ScanLoc {
    file: String,
    position: u64,
}

/// Stream the current snapshot (optionally pruned by `predicate`, reusing the already-loaded
/// [`Table`]) and index **only** the `wanted` keys → `full row`, stopping once all are found —
/// the store-less hydration read. Batches are processed one at a time and the result map is
/// capped at the wanted set, so even an unfiltered scan (`predicate` = `None`, from a DATE key /
/// type mismatch) is bounded.
///
/// Also returns the number of **duplicate PKs** seen (see [`index_batch`]). The early exit bounds
/// detection too: a duplicate in a not-yet-scanned file goes unreported — detection is honest
/// within what the scan read, not a full-table uniqueness audit.
async fn scan_stale_index(
    tbl: &Table,
    predicate: Option<Predicate>,
    wanted: &HashSet<Vec<u8>>,
    partition_names: &[String],
    identifier_names: &[String],
    max_scan_bytes: u64,
) -> Result<(HashMap<Vec<u8>, (BTreeMap<String, Value>, ScanLoc)>, u64)> {
    let mut builder = tbl.scan().select_all();
    if let Some(p) = predicate {
        builder = builder.with_filter(p);
    }
    let tasks: Vec<FileScanTask> = builder.build()?.plan_files().await?.try_collect().await?;
    let file_io = tbl.file_io().clone();
    let mut index = HashMap::new();
    let mut duplicates = 0u64;
    // Byte budget (not a file count): when the predicate can't prune — a random high-cardinality
    // identifier, a secondary-sorted key whose per-file min/max spans the whole space, or a `None`
    // plan — this scan would otherwise read the *whole current snapshot* on every hydration call
    // (O(snapshot)/hit → 30s stalls post-compaction, TASK-339). A file *count* budget is useless when
    // files are few-but-huge (a handful of ~1 GB parquet = the whole table), so bound the bytes
    // *read*, checked at row-group granularity. A well-pruned plan is far under budget and resolves
    // every key; an unpruned one returns what it cheaply found and omits the rest. Unresolved rows
    // are omitted, not errored (assemble_rows) — graceful.
    let mut bytes_scanned = 0u64;
    'files: for task in tasks.into_iter() {
        if bytes_scanned >= max_scan_bytes {
            break 'files;
        }
        let data_file = task.data_file_path.clone();
        let reader = ArrowReaderBuilder::new(file_io.clone(), iceberg::Runtime::current()).build();
        let task_stream =
            futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) }).boxed();
        let mut stream = reader.read(task_stream)?.stream();
        let mut start_row = 0u64;
        while let Some(batch) = stream.try_next().await? {
            bytes_scanned += batch.get_array_memory_size() as u64;
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
            if bytes_scanned >= max_scan_bytes {
                break 'files; // budget exhausted mid-file → serve what we found, omit the rest
            }
        }
    }
    Ok((index, duplicates))
}

/// Build an `OR`-of-`AND` equality predicate over each entry's key fields **plus its sort-key prune
/// hints**, so a hydration fallback prunes the Iceberg scan to the files that can hold the row. The
/// hint is the row's *own* sort-key value (e.g. `ts = <that row's ts>`); on a **sorted** source
/// table it lets Iceberg prune by manifest min/max on the sort key — the heal for an unpartitioned,
/// hash-routed random identifier whose per-file min/max spans the whole space.
///
/// Datums are typed to match the source schema. A **key** field that can't map safely (a
/// value/column-type mismatch, or a timestamp that can't be an exact DATE) makes the whole
/// predicate `None` so the caller reads unfiltered. A **hint** field that can't map is simply
/// *skipped* (the row's key still constrains it). Neither can *exclude* a matching row — the
/// fallback re-verifies each candidate against the exact key — so pruning is a pure speed-up, and
/// `None` (read everything) is the safe default. Returns `None` for an empty entry set.
fn key_predicate(
    schema: &IcebergSchema,
    entries: &[(&CompositeKey, &[(String, Value)])],
) -> Option<Predicate> {
    let mut per_key = Vec::with_capacity(entries.len());
    for (key, hints) in entries {
        let mut conj: Option<Predicate> = None;
        let push = |conj: &mut Option<Predicate>, name: &str, datum: Datum| {
            let eq = Reference::new(name.to_string()).equal_to(datum);
            *conj = Some(match conj.take() {
                Some(c) => c.and(eq),
                None => eq,
            });
        };
        // Every term only NARROWS a pure prune — each candidate row is re-verified against the exact
        // composite key downstream — so an un-typeable term (key field OR hint) is SKIPPED, never
        // fatal. Bailing to `None` on an un-typeable KEY field (the old `?`) discarded the whole
        // predicate incl. the sort-key hint, so a table whose random key doesn't cleanly map to an
        // Iceberg datum lost all pruning — the `ts` hint must survive that.
        for (name, value) in key
            .partition
            .iter()
            .chain(key.identifier.iter())
            .chain(hints.iter())
        {
            if let Some(datum) = value_to_datum(schema, name, value) {
                push(&mut conj, name, datum);
            }
        }
        // A key that mapped no term at all can't prune — drop it from the OR (an empty per-key clause
        // would match everything). If every key drops out, the caller reads unfiltered (bounded).
        if let Some(c) = conj {
            per_key.push(c);
        }
    }
    let mut it = per_key.into_iter();
    let first = it.next()?;
    Some(it.fold(first, |acc, p| acc.or(p)))
}

/// The identity sort-order columns of `tbl` — see [`IcebergReader::sort_field_names`]. Free helper
/// so a caller already holding the [`Table`] needn't re-load it.
fn sort_field_names_of(tbl: &Table) -> Vec<String> {
    let meta = tbl.metadata();
    let schema = meta.current_schema();
    meta.default_sort_order()
        .fields
        .iter()
        .filter(|sf| sf.transform == Transform::Identity)
        .filter_map(|sf| schema.field_by_id(sf.source_id).map(|f| f.name.clone()))
        .collect()
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

/// Index the rows of one `batch` whose composite key is in `wanted` → `enc(key) → (full row,
/// [`ScanLoc`])`, for the store-less re-find. Filtering to `wanted` bounds the scan's memory.
/// `start_row` is the batch's absolute offset within `data_file`.
///
/// **Duplicate-PK detection**: a second distinct source row for an already-matched key means the
/// table holds >1 row for a "unique" key. Each extra row counts toward the returned total (→
/// `growlerdb_duplicate_pks_total`) and emits a [rate-limited warning](warn_duplicate_pk). The
/// winner is deterministic — per key, the highest `(file, position)` scanned, not scan-order
/// last-wins — but bounded to what the scan read (the caller's early exit stops at all-matched).
fn index_batch(
    index: &mut HashMap<Vec<u8>, (BTreeMap<String, Value>, ScanLoc)>,
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
            continue; // only re-resolve the wanted keys, not every row in the snapshot
        }
        let loc = ScanLoc {
            file: data_file.to_string(),
            position: start_row + row as u64,
        };
        match index.entry(enc) {
            std::collections::hash_map::Entry::Vacant(slot) => {
                slot.insert((full_row(batch, row), loc));
            }
            std::collections::hash_map::Entry::Occupied(mut slot) => {
                // A second distinct row for this key — a genuine duplicate PK (one
                // scan never visits the same (file, position) twice). Deterministic
                // winner: highest (file, position).
                duplicates += 1;
                let held = &slot.get().1;
                let keep_new =
                    (loc.file.as_str(), loc.position) > (held.file.as_str(), held.position);
                let (winner, loser) = if keep_new { (&loc, held) } else { (held, &loc) };
                warn_duplicate_pk(&key, winner, loser);
                if keep_new {
                    slot.insert((full_row(batch, row), loc));
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
fn warn_duplicate_pk(key: &CompositeKey, winner: &ScanLoc, loser: &ScanLoc) -> bool {
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
    tracing::warn!(
        "duplicate primary key [{}|{}] in source scan: >1 distinct row matches — keeping \
         {}:{} over {}:{} (deterministic: highest (file, position) wins). The source table is not \
         unique on this key; further duplicates are counted (growlerdb_duplicate_pks_total) but \
         this warning is rate-limited.",
        describe(&key.partition),
        describe(&key.identifier),
        winner.file,
        winner.position,
        loser.file,
        loser.position,
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
mod tests {
    use super::*;
    use arrow_schema::Field;
    use std::sync::Arc;

    use iceberg::spec::{NestedField, PartitionSpec, Struct, Type};

    /// A minimal delete-free [`FileScanTask`] for `path`; only `data_file_path` + `deletes` are
    /// meaningful to the tests, the rest is inert.
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

    /// `key_predicate` entries with no prune hints — the key-only predicate (the prior behavior).
    fn no_hint<'a>(keys: &[&'a CompositeKey]) -> Vec<(&'a CompositeKey, &'a [(String, Value)])> {
        keys.iter().map(|k| (*k, &[][..])).collect()
    }

    #[test]
    fn key_predicate_prunes_by_partition_and_identifier() {
        let schema = ice_schema();
        let k = ckey(
            vec![("site", Value::Str("plant-1".into()))],
            vec![("id", Value::Str("doc-10".into()))],
        );
        let p = key_predicate(&schema, &no_hint(&[&k])).expect("predicate");
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
        let p = key_predicate(&schema, &no_hint(&[&a, &b])).expect("predicate");
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
        assert!(key_predicate(&schema, &no_hint(&[&k])).is_none());
        // An unknown/absent column likewise yields None.
        let missing = ckey(vec![], vec![("nope", Value::Str("x".into()))]);
        assert!(key_predicate(&schema, &no_hint(&[&missing])).is_none());
    }

    #[test]
    fn key_predicate_is_none_for_no_keys() {
        assert!(key_predicate(&ice_schema(), &[]).is_none());
    }

    #[test]
    fn key_predicate_ands_a_sort_key_hint_onto_the_key() {
        // The sort-key prune hint (`n = 42`, the row's own value) is AND-ed onto the key's own
        // `id` equality — on a sorted table this prunes files by manifest min/max on `n`, which the
        // random `id` alone can't. Correctness rests on the exact key re-verify, so the extra
        // equality on the row's true value is always safe.
        let schema = ice_schema();
        let k = ckey(vec![], vec![("id", Value::Str("doc-10".into()))]);
        let hints = [("n".to_string(), Value::Int(42))];
        let p = key_predicate(&schema, &[(&k, &hints)]).expect("predicate");
        let s = p.to_string();
        assert!(
            s.contains("id") && s.contains("doc-10"),
            "key term present: {s}"
        );
        assert!(
            s.contains('n') && s.contains("42"),
            "sort-key hint AND-ed in: {s}"
        );
    }

    #[test]
    fn key_predicate_absent_hint_degrades_to_the_key_only_predicate() {
        // No hint (or an unmappable one) leaves exactly the prior key-only predicate — a hint never
        // widens or narrows incorrectly. An `n = <string>` hint can't type against the Long column,
        // so it's skipped, and the result matches the no-hint predicate verbatim.
        let schema = ice_schema();
        let k = ckey(vec![], vec![("id", Value::Str("doc-10".into()))]);
        let key_only = key_predicate(&schema, &no_hint(&[&k]))
            .expect("predicate")
            .to_string();
        let bad = [("n".to_string(), Value::Str("not-a-long".into()))];
        let with_bad_hint = key_predicate(&schema, &[(&k, &bad)])
            .expect("predicate")
            .to_string();
        assert_eq!(
            with_bad_hint, key_only,
            "unmappable hint skipped → key-only"
        );
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
        let p = key_predicate(&schema, &no_hint(&[&k])).expect("temporal predicate");
        let s = p.to_string();
        assert!(s.contains("day"), "date key pruned: {s}");
        assert!(s.contains("ts"), "timestamp key pruned: {s}");
    }

    #[test]
    fn key_predicate_skips_an_intraday_date_field_but_keeps_the_rest() {
        // A DATE column can only be pruned by an exact UTC-midnight value; anything else could build
        // a predicate that *excludes* the row, so that term is SKIPPED (value_to_datum → None). But
        // skipping one term must not discard the whole predicate — the other key fields still prune
        // (a pure superset filter, re-verified by the exact key), so `id="x"` survives.
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
        // Not None: the intraday `day` is skipped, `id="x"` still prunes.
        assert!(key_predicate(&schema, &no_hint(&[&k])).is_some());
        // A key whose ONLY field is the un-mappable intraday date maps no term → unfiltered (None).
        let only_date = ckey(vec![("day", Value::Ts(not_midnight))], vec![]);
        assert!(key_predicate(&schema, &no_hint(&[&only_date])).is_none());
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
    fn project_row_narrows_columns() {
        let full = full_row(&docs_batches()[0], 1);
        let narrowed = project_row(&full, &Projection::Columns(vec!["body".into()]));
        assert_eq!(narrowed.keys().collect::<Vec<_>>(), vec!["body"]);
        assert_eq!(narrowed["body"], Value::Str("bravo".into()));
    }

    #[test]
    fn index_batch_indexes_only_wanted_keys_for_fallback() {
        // A key is re-found from a scan — and only the wanted keys are indexed, so an unfiltered
        // scan doesn't materialize every row.
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
        let (full, loc) = index.get(&key_id(11).encode()).expect("found by key");
        assert_eq!(full["body"], Value::Str("bravo".into()));
        assert_eq!(loc.file, "data/x.parquet");
        assert_eq!(loc.position, 1);
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
        assert_eq!((loc.file.as_str(), loc.position), ("data/x.parquet", 2));

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
        assert_eq!((loc.file.as_str(), loc.position), ("data/y.parquet", 0));

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
        assert_eq!(loc.file, "data/y.parquet");

        // Warning path: the detections above went through `warn_duplicate_pk`, which
        // consumed the process-wide rate-limit window — a direct call inside the same
        // window is suppressed (returns false) while the count above still recorded
        // every occurrence.
        assert!(
            !warn_duplicate_pk(
                &key_id(11),
                &ScanLoc {
                    file: "data/y.parquet".into(),
                    position: 0
                },
                &ScanLoc {
                    file: "data/w.parquet".into(),
                    position: 5
                },
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
        let (full, loc) = index
            .get(&key(micros[1]).encode())
            .expect("found by ts key");
        assert_eq!(full["body"], Value::Str("bravo".into()));
        assert_eq!(full["ts"], Value::Ts(micros[1]));
        assert_eq!(loc.position, 1);
    }

    #[test]
    fn batch_to_docs_builds_keyed_documents() {
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
        batch_to_docs(&index, &batches[0], &mut docs);
        batch_to_docs(&index, &batches[1], &mut docs);

        assert_eq!(docs.len(), 3);
        // Key carries the identifier.
        assert_eq!(docs[0].doc.key.get("id"), Some(&Value::Int(10)));
        assert_eq!(docs[2].doc.key.get("id"), Some(&Value::Int(12)));
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
        batch_to_docs(&idx, &batch, &mut out);
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
        assert!(
            !res.batches.is_empty(),
            "seeded rows come back as record batches"
        );
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
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            rows, 9,
            "MoR history delete must be honored (9 live rows, not 10 or 0)"
        );
    }

    /// Reproduction (no cloud): the **sort-key hydration prune**, end to end. A local Iceberg table
    /// with a declared sort order `[ts, request_id]` (identity — what Spark `WRITE ORDERED BY ts,
    /// request_id` records) and 6 ts-disjoint data files, each a tight `ts` manifest min/max — the
    /// post-compaction, ts-clustered layout the bench measured via Trino readable_metrics. The key
    /// (`request_id`) is written to span the whole hex space in every file, so a request_id-only
    /// predicate can never prune.
    ///
    /// Proves the mechanism: the hydration scan's [`key_predicate`], once the row's own `ts`
    /// sort-key value is AND-ed on as a prune hint, makes `plan_files` prune to the **one** file that
    /// can hold the row (vs the full 6-file scan on the key alone). Also proves the *dependency* on a
    /// declared sort order: [`sort_field_names_of`] reads `default_sort_order()` — the sorted table
    /// yields the `ts` hint field, its unsorted twin (same data, same clustering, no sort order)
    /// yields none → no hint → full scan, which is the live symptom's cause.
    ///
    /// Generate the fixture first (any pyiceberg + pyarrow venv):
    ///   `NS_WAREHOUSE=/tmp/prunewh python3 tests/fixtures/gen_prune_fixture.py`
    /// Then: `cargo test -p growlerdb-source -- --ignored sort_key_hint_prunes_hydration_scan`
    #[tokio::test]
    #[ignore = "requires the pyiceberg prune fixture at /tmp/prunewh (see gen_prune_fixture.py)"]
    async fn sort_key_hint_prunes_hydration_scan() {
        use growlerdb_core::{CompositeKey, Value};
        use iceberg::io::FileIOBuilder;
        use iceberg::table::StaticTable;
        use iceberg_storage_opendal::OpenDalStorageFactory;

        let wh = std::env::var("GDB_PRUNE_WAREHOUSE").unwrap_or_else(|_| "/tmp/prunewh".into());
        async fn load(wh: &str, name: &str) -> Table {
            let dir = format!("{wh}/ns/{name}/metadata");
            let mut v: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
                .unwrap_or_else(|e| panic!("read fixture dir {dir}: {e}"))
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|x| x == "json"))
                .collect();
            v.sort(); // version-prefixed (00000-, 00001-, …) → last is the current metadata
            let meta = v
                .last()
                .expect("a metadata json")
                .to_string_lossy()
                .into_owned();
            let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build();
            let ident = TableIdent::from_strs(["ns", name]).unwrap();
            StaticTable::from_metadata_file(&meta, ident, file_io)
                .await
                .expect("static table")
                .into_table()
        }
        async fn plan_count(tbl: &Table, predicate: Option<Predicate>) -> usize {
            let mut b = tbl.scan().select_all();
            if let Some(p) = predicate {
                b = b.with_filter(p);
            }
            let tasks: Vec<FileScanTask> = b
                .build()
                .unwrap()
                .plan_files()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            tasks.len()
        }

        let sorted = load(&wh, "sorted").await;
        let unsorted = load(&wh, "unsorted").await;

        // (1) default_sort_order() IS read: the identity sort columns resolve to their names.
        assert_eq!(
            sort_field_names_of(&sorted),
            vec!["ts".to_string(), "request_id".to_string()],
            "declared sort order [ts, request_id] must surface as hint fields"
        );
        assert!(
            sort_field_names_of(&unsorted).is_empty(),
            "no declared sort order → no hint fields → the live full-scan symptom"
        );

        // The stale key + its own ts sort-key value (the prune hint attach_prune_hints would build).
        let key = CompositeKey::new(
            vec![],
            vec![(
                "request_id".into(),
                Value::Str("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
            )],
        );
        let no_hint: &[(String, Value)] = &[];
        let ts_hint: Vec<(String, Value)> = vec![("ts".into(), Value::Int(4500))];
        let schema = sorted.metadata().current_schema();
        let key_only = key_predicate(schema, &[(&key, no_hint)]);
        let key_and_ts = key_predicate(schema, &[(&key, ts_hint.as_slice())]);
        assert!(
            key_only.is_some() && key_and_ts.is_some(),
            "predicates build"
        );

        // (2) THE PROOF — the file-count delta.
        let full = plan_count(&sorted, None).await;
        let key_scan = plan_count(&sorted, key_only).await;
        let hint_scan = plan_count(&sorted, key_and_ts).await;
        assert_eq!(full, 6, "6 ts-disjoint data files in the snapshot");
        assert_eq!(
            key_scan, 6,
            "the random key spans every file's min/max → key alone can't prune (the wall)"
        );
        assert_eq!(
            hint_scan, 1,
            "AND-ing the row's ts sort-key value prunes to the single file that can hold it"
        );
    }

    /// Reproduction (no cloud) of the **live** sort-key prune path — over a real **REST catalog**
    /// (Polaris), the one difference from [`sort_key_hint_prunes_hydration_scan`] (which loads via
    /// `StaticTable` off a metadata file). Settles the two candidate live root causes:
    ///
    /// - **(A) RestCatalog gap** — does `iceberg-rust`'s `RestCatalog::load_table` surface
    ///   `default_sort_order()` (identity `ts`/`request_id`) the same as `StaticTable`? If Polaris /
    ///   the REST response didn't round-trip the sort order, [`sort_field_names`] returns `[]` live →
    ///   no hint → full scan. Asserted directly below.
    /// - **(B) Key typing** — for the real `request_id` (a string identifier) key, does
    ///   [`key_predicate`] emit a predicate that still carries the `ts` term? Asserted via the
    ///   predicate's rendered form.
    ///
    /// Then the end-to-end proof: `plan_files` over the REST-loaded [`Table`] prunes 6 → 1 once the
    /// `ts` hint is AND-ed on (vs 6 on the key alone).
    ///
    /// Stand the fixture up first (Polaris + MinIO, no cloud):
    ///   `docker compose -p prunerepro up -d minio createbuckets polaris-db polaris-bootstrap polaris`
    ///   `POLARIS=http://localhost:8181 bash deploy/compose/setup-polaris.sh`  (creates the `growlerdb` catalog)
    ///   then create `growlerdb.{sorted,unsorted}` THROUGH the REST catalog with a declared
    ///   `sort_order=[ts, request_id]` (see the investigation's `gen_rest_prune.py`).
    /// Run it in-network (the catalog vends the `minio:9000` endpoint):
    ///   `GROWLERDB_CATALOG_URI=http://polaris:8181/api/catalog cargo test -p growlerdb-source \
    ///      -- --ignored rest_catalog_sort_key_hint_prunes_hydration_scan`
    #[tokio::test]
    #[ignore = "requires a local Polaris+MinIO REST catalog with growlerdb.{sorted,unsorted} (see gen_rest_prune.py)"]
    async fn rest_catalog_sort_key_hint_prunes_hydration_scan() {
        use growlerdb_core::{CompositeKey, Value};

        const TARGET_RID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const TARGET_TS: i64 = 4500; // the target row's ts — inside only file 3's manifest range

        let reader = IcebergReader::connect(&IcebergConfig::from_env())
            .await
            .expect("connect to REST catalog");

        // (A) — the REST-loaded table's declared sort order surfaces as hint fields (metadata only, no
        // S3): if this were empty live, there'd be no `ts` hint and the fallback would full-scan.
        assert_eq!(
            reader.sort_field_names("growlerdb.sorted").await.unwrap(),
            vec!["ts".to_string(), "request_id".to_string()],
            "(A) RestCatalog::load_table must surface default_sort_order() over REST — else no hint → full scan"
        );
        assert!(
            reader
                .sort_field_names("growlerdb.unsorted")
                .await
                .unwrap()
                .is_empty(),
            "the unsorted twin has no declared order → no hint (the live full-scan symptom's cause)"
        );

        let ident = TableIdent::from_strs(["growlerdb", "sorted"]).unwrap();
        let tbl = reader.catalog.load_table(&ident).await.expect("load_table");
        let schema = tbl.metadata().current_schema();

        // (B) — the real string `request_id` key: the predicate types AND still carries the `ts` term.
        let key = CompositeKey::new(
            vec![],
            vec![("request_id".into(), Value::Str(TARGET_RID.into()))],
        );
        let ts_hint: Vec<(String, Value)> = vec![("ts".into(), Value::Int(TARGET_TS))];
        let key_only = key_predicate(schema, &[(&key, &[])]).expect("key predicate");
        let key_and_ts =
            key_predicate(schema, &[(&key, ts_hint.as_slice())]).expect("key+ts predicate");
        let rendered = format!("{key_and_ts}");
        assert!(
            rendered.contains("ts") && rendered.contains("request_id"),
            "(B) predicate must carry BOTH the ts hint and the request_id key term, got: {rendered}"
        );

        async fn plan_count(tbl: &Table, predicate: Option<Predicate>) -> usize {
            let mut b = tbl.scan().select_all();
            if let Some(p) = predicate {
                b = b.with_filter(p);
            }
            let tasks: Vec<FileScanTask> = b
                .build()
                .unwrap()
                .plan_files()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            tasks.len()
        }

        // The end-to-end proof over REST — file-count delta (reads manifests from object storage).
        let full = plan_count(&tbl, None).await;
        let key_scan = plan_count(&tbl, Some(key_only)).await;
        let hint_scan = plan_count(&tbl, Some(key_and_ts)).await;
        assert_eq!(full, 6, "6 ts-disjoint data files in the snapshot");
        assert_eq!(
            key_scan, 6,
            "the random request_id spans every file's min/max → key alone can't prune"
        );
        assert_eq!(
            hint_scan, 1,
            "over REST too, AND-ing the row's ts sort-key value prunes to the one file that holds it"
        );

        // The live differentiator: a topk hydrates a *batch* of keys at once → ONE OR-of-AND
        // predicate. The ts hint only prunes when the batch's keys cluster in a narrow ts window.
        // A clustered batch (both keys in file 3's ts range) still prunes to 1; a batch whose keys
        // are spread across the whole timeline (one distinct key per file, each ts in a different
        // file's range) matches EVERY file — the OR spans the space, so nothing prunes and the scan
        // reads all 6 files. That is the live full-scan: a broad topk, not a narrow/recency one.
        let clustered_keys = [
            (
                CompositeKey::new(
                    vec![],
                    vec![("request_id".into(), Value::Str(TARGET_RID.into()))],
                ),
                vec![("ts".into(), Value::Int(TARGET_TS))],
            ),
            (
                CompositeKey::new(
                    vec![],
                    vec![("request_id".into(), Value::Str(format!("c{:031x}", 3)))],
                ),
                vec![("ts".into(), Value::Int(4600))], // file 3 range [4000,4900]
            ),
        ];
        let clustered: Vec<(&CompositeKey, &[(String, Value)])> = clustered_keys
            .iter()
            .map(|(k, h)| (k, h.as_slice()))
            .collect();
        let clustered_scan = plan_count(&tbl, key_predicate(schema, &clustered)).await;
        assert_eq!(
            clustered_scan, 1,
            "a topk batch clustered in one ts window still prunes to its file"
        );

        // One distinct key per file, ts spread across all 6 file ranges.
        let spread_keys: Vec<(CompositeKey, Vec<(String, Value)>)> = (0..6)
            .map(|i| {
                (
                    CompositeKey::new(
                        vec![],
                        vec![("request_id".into(), Value::Str(format!("c{i:031x}")))],
                    ),
                    vec![("ts".into(), Value::Int(1000 + i * 1000 + 600))],
                )
            })
            .collect();
        let spread: Vec<(&CompositeKey, &[(String, Value)])> =
            spread_keys.iter().map(|(k, h)| (k, h.as_slice())).collect();
        let spread_scan = plan_count(&tbl, key_predicate(schema, &spread)).await;
        assert_eq!(
            spread_scan, 6,
            "a topk batch whose keys span the whole timeline can't prune — the OR matches every file (the live 30s full-scan)"
        );
    }

    /// The decisive measurement the file-count sibling tests never took: does iceberg-rust prune at
    /// the **row-group** level *within* one data file? This settles whether store-less `PREDICATE`
    /// hydration can serve a **scattered** top-k (the real `topk_hydrated`: `sort response_time desc`
    /// → 20 hits with `ts` spread across the whole timeline) in sub-second.
    ///
    /// The sibling REST test showed a scattered batch can't prune at the FILE level (`spread_scan`
    /// = every file). But the compacted files are few-but-huge (many row groups each). This loads a
    /// single 40-row-group file — `ts` strictly increasing (tight, disjoint per-row-group min/max),
    /// `request_id` md5-uniform (spans the whole space in EVERY row group, so it prunes neither file
    /// nor row group; iceberg-rust 0.10.1 has no bloom support) — and measures the bytes iceberg
    /// actually fetches (`ScanMetrics::bytes_read`) for the real hydration read (mirrors
    /// [`scan_stale_index`]) in three modes:
    ///   * no filter          → whole file (the baseline);
    ///   * request_id-only    → the CONTROL: an unselective key can't skip any row group → ~whole
    ///                          file (this is also why the parquet request_id bloom is dead weight);
    ///   * request_id AND ts  → the scattered batch's 5 keys, each with its `ts` prune hint.
    ///
    /// If the ts+key read fetches ~5/40 of the file → row-group pruning carries the scattered batch
    /// → PREDICATE is sub-second with NO stored locators, NO remap, NO staleness. That is the whole
    /// question behind dropping the fragile `(file,position)` locator.
    ///
    /// Generate first (any pyiceberg + pyarrow venv):
    ///   `GDB_RG_WAREHOUSE=/tmp/rgwh python3 tests/fixtures/gen_rowgroup_prune.py`
    /// Then: `cargo test -p growlerdb-source -- --ignored rowgroup_prune --nocapture`
    #[tokio::test]
    #[ignore = "requires the pyiceberg row-group fixture at /tmp/rgwh (see gen_rowgroup_prune.py)"]
    async fn rowgroup_prune_reads_only_the_matching_row_groups() {
        use growlerdb_core::{CompositeKey, Value};
        use iceberg::io::FileIOBuilder;
        use iceberg::table::StaticTable;
        use iceberg_storage_opendal::OpenDalStorageFactory;

        // The 5 scattered targets the fixture prints (md5("req-<i>"), ts = i), one per row group
        // ~9 apart across the 40-group file.
        const TARGETS: [(&str, i64); 5] = [
            ("697999c93cb1611f2bbd5b10610416f0", 2_500),
            ("d2b947b49b2beba62b6f5f861a6fae54", 11_500),
            ("d691c64dd2248551c44ec631b0c9b078", 20_500),
            ("bb9a07ef8619e182e3e74fac490f7d59", 29_500),
            ("e6e9a115791ff9c51ddc55af0acd5a4a", 38_500),
        ];

        let wh = std::env::var("GDB_RG_WAREHOUSE").unwrap_or_else(|_| "/tmp/rgwh".into());
        let dir = format!("{wh}/ns/rowgroups/metadata");
        let mut metas: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("read fixture dir {dir}: {e} — run gen_rowgroup_prune.py"))
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "json"))
            .collect();
        metas.sort(); // version-prefixed → last is current
        let meta = metas
            .last()
            .expect("a metadata json")
            .to_string_lossy()
            .into_owned();
        let file_io = FileIOBuilder::new(Arc::new(OpenDalStorageFactory::Fs)).build();
        let ident = TableIdent::from_strs(["ns", "rowgroups"]).unwrap();
        let tbl = StaticTable::from_metadata_file(&meta, ident, file_io)
            .await
            .expect("static table")
            .into_table();

        // The declared sort order surfaces `ts` as a hint field (the production dependency).
        assert_eq!(
            sort_field_names_of(&tbl),
            vec!["ts".to_string(), "request_id".to_string()],
            "declared WRITE ORDERED BY (ts, request_id) must surface as hint fields"
        );

        // Run the REAL hydration read (mirrors `scan_stale_index`: plan_files with the filter, then the
        // iceberg ArrowReader per task) and return (matched request_ids, bytes fetched from storage).
        async fn scan_read(tbl: &Table, predicate: Option<Predicate>) -> (Vec<String>, u64) {
            let mut b = tbl.scan().select_all();
            if let Some(p) = predicate {
                b = b.with_filter(p);
            }
            let tasks: Vec<FileScanTask> = b
                .build()
                .unwrap()
                .plan_files()
                .await
                .unwrap()
                .try_collect()
                .await
                .unwrap();
            let file_io = tbl.file_io().clone();
            let mut rids = Vec::new();
            let mut bytes = 0u64;
            for task in tasks {
                let reader =
                    ArrowReaderBuilder::new(file_io.clone(), iceberg::Runtime::current()).build();
                let task_stream =
                    futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) })
                        .boxed();
                let scan = reader.read(task_stream).unwrap();
                let metrics = scan.metrics().clone(); // shares the Arc byte counter
                let mut stream = scan.stream();
                while let Some(batch) = stream.try_next().await.unwrap() {
                    for r in 0..batch.num_rows() {
                        if let Some(Value::Str(rid)) = full_row(&batch, r).get("request_id") {
                            rids.push(rid.clone());
                        }
                    }
                }
                bytes += metrics.bytes_read();
            }
            (rids, bytes)
        }

        let key = |rid: &str| {
            CompositeKey::new(vec![], vec![("request_id".into(), Value::Str(rid.into()))])
        };
        let keys: Vec<CompositeKey> = TARGETS.iter().map(|(rid, _)| key(rid)).collect();
        let ts_hints: Vec<Vec<(String, Value)>> = TARGETS
            .iter()
            .map(|(_, ts)| vec![("ts".to_string(), Value::Int(*ts))])
            .collect();
        let schema = tbl.metadata().current_schema();
        let no_hint: &[(String, Value)] = &[];
        let key_only: Vec<(&CompositeKey, &[(String, Value)])> =
            keys.iter().map(|k| (k, no_hint)).collect();
        let key_and_ts: Vec<(&CompositeKey, &[(String, Value)])> = keys
            .iter()
            .zip(&ts_hints)
            .map(|(k, h)| (k, h.as_slice()))
            .collect();

        let (full_rows, full_bytes) = scan_read(&tbl, None).await;
        let (ctrl_rids, ctrl_bytes) = scan_read(&tbl, key_predicate(schema, &key_only)).await;
        let (hit_rids, hit_bytes) = scan_read(&tbl, key_predicate(schema, &key_and_ts)).await;

        eprintln!(
            "row-group prune bytes_read: full={full_bytes} request_id_only={ctrl_bytes} \
             request_id+ts={hit_bytes} (full rows={})",
            full_rows.len()
        );

        // Both filtered reads return exactly the 5 scattered targets (the RowFilter is applied) —
        // so pruning is a pure speed-up, never a correctness change.
        let want: std::collections::BTreeSet<String> =
            TARGETS.iter().map(|(rid, _)| rid.to_string()).collect();
        assert_eq!(
            ctrl_rids
                .iter()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>(),
            want,
            "request_id-only still returns the 5 targets (correctness)"
        );
        assert_eq!(
            hit_rids.iter().cloned().collect::<std::collections::BTreeSet<_>>(),
            want,
            "request_id+ts returns exactly the 5 targets — the ts prune term never drops a real hit"
        );

        // CONTROL: `request_id` alone cannot skip a row group (its per-group min/max spans the whole
        // space, and iceberg-rust 0.10.1 uses no parquet bloom filter), so it reads every row group's
        // key column — but parquet's RowFilter still late-materializes, reading the fat `payload` only
        // for the 5 matched rows, so `request_id`-only lands well below the unfiltered full read. The
        // exact "touches all 40 row groups" control is asserted by the sibling
        // `tests/rowgroup_bytes.rs` (which logs per-row-group reads); here we only need it as the
        // no-ts baseline the `ts` hint must beat.
        assert!(
            ctrl_bytes < full_bytes,
            "request_id-only ({ctrl_bytes}) reads less than the unfiltered full scan ({full_bytes}) \
             via RowFilter late-materialization"
        );

        // THE PROOF: AND-ing each hit's `ts` prunes to the 5 of 40 row groups the scattered keys fall
        // in — even though the batch spans the whole timeline (file-level pruning was useless here).
        // The residual is the ~5 row groups' data plus a fixed ~512 KiB footer/page-index prefetch,
        // so the ratio is generous on this small file and shrinks toward ~5/40 at production 8 MiB
        // row groups. The sibling `rowgroup_bytes.rs` proves the exact 5-group touch.
        assert!(
            (hit_bytes as f64) < 0.35 * full_bytes as f64,
            "scattered ts+key must read only the matching row groups ({hit_bytes} vs full \
             {full_bytes}) — the row-group pruning that makes PREDICATE hydration sub-second"
        );
        assert!(
            (hit_bytes as f64) < 0.6 * ctrl_bytes as f64,
            "the ts hint is what prunes: ts+key ({hit_bytes}) must be well below request_id-only \
             ({ctrl_bytes})"
        );
    }
}
