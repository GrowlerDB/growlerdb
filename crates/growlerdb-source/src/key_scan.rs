//! Column-projected **key scans** for the compaction re-map (task-184 slice 3, D30
//! `coordinates` strategy — `okf/system/decisions/d30-layered-locator.md`).
//!
//! When an Iceberg rewrite (`replace` snapshot) moves rows into new data files, every
//! location slot pointing into the rewritten files goes stale at once. The re-map heals
//! them in the background by reading **only the key columns + row positions** of the
//! snapshot's *added* files — the minimum needed to recompute `key → (file, position)`
//! — instead of the full rows. The read mirrors [`point_read`](crate::point_read)'s
//! stack: the `parquet` crate directly over the table's `FileIO` (opendal + retry),
//! with a [`ProjectionMask`] narrowing IO/decode to the root columns of the key paths.
//!
//! Positions are **physical** row positions, matching what ingest records for
//! delete-free files — which freshly-compacted files are. A file already carrying
//! delete files is *not* re-mapped from here (the caller skips it; ingest-recorded
//! positions for such files are delete-shifted), leaving those slots to the lazy
//! verify-and-refresh safety net.

use futures::TryStreamExt;
use growlerdb_core::{CompositeKey, Value};
use iceberg::arrow::ArrowFileReader;
use iceberg::io::FileIO;
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};

use crate::{nested_value, Result};

/// Read `(composite key, row position)` for **every row** of the parquet data file at
/// `path`, projecting only the root columns of the key paths (partition + identifier
/// fields). The re-map's per-file read: its output feeds the shard's
/// `remap_locations` (batched, key-sorted term lookups → slot patches). A key field
/// absent from the file simply doesn't contribute to that row's key (the resulting
/// key then matches no live doc and is skipped downstream — never a wrong patch);
/// if *no* key field resolves, the file yields nothing.
pub async fn read_file_key_rows(
    file_io: &FileIO,
    path: &str,
    partition_fields: &[String],
    identifier_fields: &[String],
) -> Result<Vec<(CompositeKey, u64)>> {
    let input = file_io.new_input(path)?;
    let meta = input.metadata().await?;
    let reader = ArrowFileReader::new(meta, input.reader().await?);
    read_key_rows_at(reader, partition_fields, identifier_fields).await
}

/// As [`read_file_key_rows`], over any parquet [`AsyncFileReader`] — separated so tests
/// can wrap the reader to count the bytes the projection actually fetches.
pub(crate) async fn read_key_rows_at<R>(
    reader: R,
    partition_fields: &[String],
    identifier_fields: &[String],
) -> Result<Vec<(CompositeKey, u64)>>
where
    R: AsyncFileReader + Unpin + Send + 'static,
{
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    // Project the ROOT column of each key path (a dotted nested key projects its root
    // struct — `nested_value` descends the rest), deduplicated, in schema order.
    let schema = builder.schema().clone();
    let mut roots: Vec<usize> = Vec::new();
    for name in partition_fields.iter().chain(identifier_fields.iter()) {
        let root = name.split('.').next().unwrap_or(name);
        if let Ok(idx) = schema.index_of(root) {
            if !roots.contains(&idx) {
                roots.push(idx);
            }
        }
    }
    if roots.is_empty() {
        return Ok(Vec::new()); // no key column in this file → nothing to re-map
    }
    let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
    let mut stream = builder.with_projection(mask).build()?;

    let mut rows: Vec<(CompositeKey, u64)> = Vec::new();
    let mut pos = 0u64;
    while let Some(batch) = stream.try_next().await? {
        let extract = |names: &[String], row: usize| -> Vec<(String, Value)> {
            names
                .iter()
                .filter_map(|name| Some((name.clone(), nested_value(&batch, name, row)?)))
                .collect()
        };
        for row in 0..batch.num_rows() {
            let key = CompositeKey::new(
                extract(partition_fields, row),
                extract(identifier_fields, row),
            );
            rows.push((key, pos + row as u64));
        }
        pos += batch.num_rows() as u64;
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{fs_file_io, write_docs_parquet};

    fn id_key(id: i64) -> CompositeKey {
        CompositeKey::new(vec![], vec![("id".into(), Value::Int(id))])
    }

    #[tokio::test]
    async fn key_scan_yields_every_row_with_its_physical_position() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.parquet");
        let path = path.to_str().unwrap();
        write_docs_parquet(path, 100, 250, 64); // ids 100..350 across 4 row groups

        let rows = read_file_key_rows(&fs_file_io(), path, &[], &["id".to_string()])
            .await
            .expect("key scan");
        assert_eq!(rows.len(), 250, "one entry per row");
        assert_eq!(rows[0], (id_key(100), 0));
        assert_eq!(rows[63], (id_key(163), 63), "row-group edge");
        assert_eq!(rows[249], (id_key(349), 249), "last row");
    }

    #[tokio::test]
    async fn key_scan_without_any_key_column_yields_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.parquet");
        let path = path.to_str().unwrap();
        write_docs_parquet(path, 0, 10, 10);
        let rows = read_file_key_rows(&fs_file_io(), path, &[], &["nope".to_string()])
            .await
            .expect("key scan");
        assert!(rows.is_empty(), "no key column → nothing to re-map");
    }

    /// The projection must actually narrow IO: scanning keys of a file whose payload
    /// column dominates reads far less than the file (the re-map is priced on key
    /// bytes, not row bytes).
    #[tokio::test]
    async fn key_scan_reads_far_less_than_the_file() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        struct CountingReader {
            inner: ArrowFileReader,
            bytes: Arc<AtomicU64>,
        }
        impl AsyncFileReader for CountingReader {
            fn get_bytes(
                &mut self,
                range: std::ops::Range<u64>,
            ) -> futures::future::BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
                self.bytes
                    .fetch_add(range.end - range.start, Ordering::Relaxed);
                self.inner.get_bytes(range)
            }
            fn get_byte_ranges(
                &mut self,
                ranges: Vec<std::ops::Range<u64>>,
            ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>>
            {
                for r in &ranges {
                    self.bytes.fetch_add(r.end - r.start, Ordering::Relaxed);
                }
                self.inner.get_byte_ranges(ranges)
            }
            fn get_metadata<'a>(
                &'a mut self,
                options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
            ) -> futures::future::BoxFuture<
                'a,
                parquet::errors::Result<std::sync::Arc<parquet::file::metadata::ParquetMetaData>>,
            > {
                self.inner.get_metadata(options)
            }
        }

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.parquet");
        let path = path.to_str().unwrap();
        let file_len = write_docs_parquet(path, 0, 20_000, 1_000); // bodies dominate

        let file_io = fs_file_io();
        let input = file_io.new_input(path).unwrap();
        let meta = input.metadata().await.unwrap();
        let bytes = Arc::new(AtomicU64::new(0));
        let reader = CountingReader {
            inner: ArrowFileReader::new(meta, input.reader().await.unwrap()),
            bytes: bytes.clone(),
        };

        let rows = read_key_rows_at(reader, &[], &["id".to_string()])
            .await
            .expect("key scan");
        assert_eq!(rows.len(), 20_000);
        assert_eq!(rows[19_999].0, id_key(19_999));

        let read = bytes.load(Ordering::Relaxed);
        assert!(read > 0, "data bytes were fetched through the reader");
        assert!(
            read < file_len / 2,
            "key projection read {read} of {file_len} bytes — expected far less than the file"
        );
    }
}
