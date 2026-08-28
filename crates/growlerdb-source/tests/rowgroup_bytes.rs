//! Definitive row-group-touch measurement for store-less (`PREDICATE`) hydration.
//!
//! The lib.rs `rowgroup_prune` unit test shows the byte *ratio* iceberg fetches for a scattered
//! top-k. This test answers the sharper question — **exactly which row groups does iceberg read?**
//! — by wrapping the iceberg `FileIO` in a logging `Storage` that records every byte range the
//! parquet reader fetches, then bucketing those ranges into the file's row groups.
//!
//! It settles whether iceberg-rust 0.10.1 skips the non-matching row groups of a single large file
//! for a SCATTERED OR-of-AND predicate (`(rid=R1 AND ts=T1) OR … OR (rid=R5 AND ts=T5)`, five keys
//! whose `ts` land in five widely-separated row groups — the real `topk_hydrated` shape). If it
//! touches ~5 of the 40 row groups → row-group pruning carries the scattered batch → `PREDICATE`
//! hydration needs no stored `(file,position)` locators. The control (`request_id` alone, no `ts`)
//! touches every row group — iceberg has no bloom-filter support, so an unselective key can't skip.
//!
//! Generate the fixture first (any pyiceberg + pyarrow venv):
//!   `GDB_RG_WAREHOUSE=/tmp/rgwh python3 tests/fixtures/gen_rowgroup_prune.py`
//! Then: `cargo test -p growlerdb-source --test rowgroup_bytes -- --ignored --nocapture`

use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::Array;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use iceberg::expr::{Predicate, Reference};
use iceberg::io::{
    FileIO, FileIOBuilder, FileMetadata, FileRead, InputFile, LocalFsStorage, OutputFile, Storage,
    StorageConfig, StorageFactory,
};
use iceberg::scan::FileScanTask;
use iceberg::spec::Datum;
use iceberg::table::{StaticTable, Table};
use iceberg::TableIdent;
use serde::{Deserialize, Serialize};

/// The five scattered targets `gen_rowgroup_prune.py` prints: md5("req-<i>"), ts = i, one per row
/// group ~9 apart across the 40-group file.
const TARGETS: [(&str, i64); 5] = [
    ("697999c93cb1611f2bbd5b10610416f0", 2_500),
    ("d2b947b49b2beba62b6f5f861a6fae54", 11_500),
    ("d691c64dd2248551c44ec631b0c9b078", 20_500),
    ("bb9a07ef8619e182e3e74fac490f7d59", 29_500),
    ("e6e9a115791ff9c51ddc55af0acd5a4a", 38_500),
];

/// One logged read: the file path and the byte range fetched.
type ReadRecord = (String, Range<u64>);

/// Global read log: (path, range) for every `FileRead::read` the scan issues.
fn read_log() -> &'static Mutex<Vec<ReadRecord>> {
    static LOG: OnceLock<Mutex<Vec<ReadRecord>>> = OnceLock::new();
    LOG.get_or_init(|| Mutex::new(Vec::new()))
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LoggingFs;

#[async_trait]
#[typetag::serde]
impl Storage for LoggingFs {
    async fn exists(&self, path: &str) -> iceberg::Result<bool> {
        LocalFsStorage::new().exists(path).await
    }
    async fn metadata(&self, path: &str) -> iceberg::Result<FileMetadata> {
        LocalFsStorage::new().metadata(path).await
    }
    async fn read(&self, path: &str) -> iceberg::Result<Bytes> {
        LocalFsStorage::new().read(path).await
    }
    async fn reader(&self, path: &str) -> iceberg::Result<Box<dyn FileRead>> {
        let inner = LocalFsStorage::new().reader(path).await?;
        Ok(Box::new(LoggingRead {
            inner,
            path: path.to_string(),
        }))
    }
    async fn write(&self, path: &str, bs: Bytes) -> iceberg::Result<()> {
        LocalFsStorage::new().write(path, bs).await
    }
    async fn writer(&self, path: &str) -> iceberg::Result<Box<dyn iceberg::io::FileWrite>> {
        LocalFsStorage::new().writer(path).await
    }
    async fn delete(&self, path: &str) -> iceberg::Result<()> {
        LocalFsStorage::new().delete(path).await
    }
    async fn delete_prefix(&self, path: &str) -> iceberg::Result<()> {
        LocalFsStorage::new().delete_prefix(path).await
    }
    async fn delete_stream(&self, paths: BoxStream<'static, String>) -> iceberg::Result<()> {
        LocalFsStorage::new().delete_stream(paths).await
    }
    fn new_input(&self, path: &str) -> iceberg::Result<InputFile> {
        // Return an InputFile bound to THIS storage so InputFile::reader() logs.
        Ok(InputFile::new(Arc::new(LoggingFs), path.to_string()))
    }
    fn new_output(&self, path: &str) -> iceberg::Result<OutputFile> {
        LocalFsStorage::new().new_output(path)
    }
}

struct LoggingRead {
    inner: Box<dyn FileRead>,
    path: String,
}

#[async_trait]
impl FileRead for LoggingRead {
    async fn read(&self, range: Range<u64>) -> iceberg::Result<Bytes> {
        read_log()
            .lock()
            .unwrap()
            .push((self.path.clone(), range.clone()));
        self.inner.read(range).await
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct LoggingFsFactory;

#[typetag::serde]
impl StorageFactory for LoggingFsFactory {
    fn build(&self, _config: &StorageConfig) -> iceberg::Result<Arc<dyn Storage>> {
        Ok(Arc::new(LoggingFs))
    }
}

fn logging_file_io() -> FileIO {
    FileIOBuilder::new(Arc::new(LoggingFsFactory)).build()
}

/// Row-group byte spans `[start, end)` of the data file, from the parquet footer.
fn row_group_spans(data_path: &str) -> Vec<Range<u64>> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let file = std::fs::File::open(data_path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();
    let md = reader.metadata();
    (0..md.num_row_groups())
        .map(|i| {
            let rg = md.row_group(i);
            let mut start = u64::MAX;
            let mut end = 0u64;
            for c in 0..rg.num_columns() {
                let (s, len) = rg.column(c).byte_range();
                start = start.min(s);
                end = end.max(s + len);
            }
            start..end
        })
        .collect()
}

/// Which row groups the logged data-file **data-page** reads overlap. Excludes the parquet
/// footer/page-index prefetch — a single large read of the file tail that reaches EOF (past the
/// last row group's data into the footer); it overlaps the tail groups' byte spans without
/// decoding their data, so counting it would mask the real pruning. Data-page reads always end at
/// or before the last row group's data end.
fn touched_row_groups(data_file_suffix: &str, spans: &[Range<u64>]) -> BTreeSet<usize> {
    let data_end = spans.last().map(|s| s.end).unwrap_or(0);
    let log = read_log().lock().unwrap();
    let mut touched = BTreeSet::new();
    for (path, r) in log.iter() {
        if !path.ends_with(data_file_suffix) {
            continue; // ignore metadata / manifest reads
        }
        if r.end > data_end {
            continue; // the footer / page-index prefetch, not a data-page read
        }
        for (i, span) in spans.iter().enumerate() {
            if r.start < span.end && span.start < r.end {
                touched.insert(i);
            }
        }
    }
    touched
}

async fn load_table(wh: &str) -> Table {
    let dir = format!("{wh}/ns/rowgroups/metadata");
    let mut metas: Vec<std::path::PathBuf> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read fixture dir {dir}: {e} — run gen_rowgroup_prune.py"))
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    metas.sort();
    let meta = metas
        .last()
        .expect("a metadata json")
        .to_string_lossy()
        .into_owned();
    StaticTable::from_metadata_file(
        &meta,
        TableIdent::from_strs(["ns", "rowgroups"]).unwrap(),
        logging_file_io(),
    )
    .await
    .expect("static table")
    .into_table()
}

async fn scan(tbl: &Table, predicate: Option<Predicate>) -> (Vec<String>, u64) {
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
    let mut rids = Vec::new();
    let mut bytes = 0u64;
    for task in tasks {
        let reader = iceberg::arrow::ArrowReaderBuilder::new(
            tbl.file_io().clone(),
            iceberg::Runtime::current(),
        )
        .build();
        let stream =
            futures::stream::once(async move { Ok::<FileScanTask, iceberg::Error>(task) }).boxed();
        let result = reader.read(stream).unwrap();
        let metrics = result.metrics().clone();
        let mut s = result.stream();
        while let Some(batch) = s.try_next().await.unwrap() {
            let col = batch
                .column_by_name("request_id")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for r in 0..batch.num_rows() {
                if col.is_valid(r) {
                    rids.push(col.value(r).to_string());
                }
            }
        }
        bytes += metrics.bytes_read();
    }
    (rids, bytes)
}

fn scattered(with_ts: bool) -> Predicate {
    let disj = |rid: &str, ts: i64| {
        let key = Reference::new("request_id").equal_to(Datum::string(rid));
        if with_ts {
            key.and(Reference::new("ts").equal_to(Datum::long(ts)))
        } else {
            key
        }
    };
    TARGETS
        .iter()
        .skip(1)
        .fold(disj(TARGETS[0].0, TARGETS[0].1), |acc, (rid, ts)| {
            acc.or(disj(rid, *ts))
        })
}

#[tokio::test]
#[ignore = "requires the pyiceberg row-group fixture at /tmp/rgwh (see gen_rowgroup_prune.py)"]
async fn scattered_topk_touches_only_matching_row_groups() {
    let wh = std::env::var("GDB_RG_WAREHOUSE").unwrap_or_else(|_| "/tmp/rgwh".into());
    let data_path = format!("{wh}/data.parquet");
    let spans = row_group_spans(&data_path);
    assert!(
        spans.len() >= 20,
        "fixture should have many row groups, got {}",
        spans.len()
    );

    let want: BTreeSet<String> = TARGETS.iter().map(|(rid, _)| rid.to_string()).collect();

    // (1) scattered request_id + ts — the real store-less hydration read.
    read_log().lock().unwrap().clear();
    let tbl = load_table(&wh).await;
    let (hit_rids, hit_bytes) = scan(&tbl, Some(scattered(true))).await;
    let hit_groups = touched_row_groups("data.parquet", &spans);
    assert_eq!(
        hit_rids.iter().cloned().collect::<BTreeSet<_>>(),
        want,
        "returns exactly the 5 scattered targets"
    );

    // (2) control: request_id alone (no ts) — no bloom, unselective → every row group.
    read_log().lock().unwrap().clear();
    let tbl2 = load_table(&wh).await;
    let (_ctrl_rids, ctrl_bytes) = scan(&tbl2, Some(scattered(false))).await;
    let ctrl_groups = touched_row_groups("data.parquet", &spans);

    eprintln!(
        "row groups (data-page reads): total={} | request_id+ts touched {} {:?} ({hit_bytes} B incl. \
         footer prefetch) | request_id-only touched {} ({ctrl_bytes} B)",
        spans.len(),
        hit_groups.len(),
        hit_groups,
        ctrl_groups.len(),
    );

    // THE PROOF: a scattered 5-key OR-of-AND reads the DATA of EXACTLY the 5 row groups its `ts`
    // hints fall in — of 40. Row-group pruning fully carries the scattered top-k the file-level
    // plan can't prune. (The extra ~512 KB in `hit_bytes` is the fixed footer/page-index prefetch,
    // independent of key count — negligible against production 8 MiB row groups.)
    let want_groups: BTreeSet<usize> = TARGETS
        .iter()
        .map(|(_, ts)| (*ts as usize) / 1_000) // ts == row index; 1000 rows/group
        .collect();
    assert_eq!(
        hit_groups,
        want_groups,
        "scattered ts+key must read exactly the 5 matching row groups' data, of {}",
        spans.len()
    );
    // CONTROL: `request_id` alone touches EVERY row group — an unselective key prunes nothing, and
    // iceberg-rust 0.10.1 has no parquet bloom-filter support (so the corpus request_id bloom is
    // dead weight for this reader; only column min/max — i.e. the `ts` sort key — prunes).
    assert_eq!(
        ctrl_groups.len(),
        spans.len(),
        "request_id-only must touch all {} row groups (no bloom, unselective min/max)",
        spans.len()
    );
}
