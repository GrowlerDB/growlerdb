//! Positional parquet **point reads** for hydration pass 1 (the layered locator —
//! `okf/system/decisions/d30-layered-locator.md`).
//!
//! Resolving a `(file, row_position)` locator by streaming the data file's Arrow reader from
//! row 0 would decode every batch until it passed `max(row_position)` — for a late row in a large
//! file that decodes nearly the whole file per lookup batch. Instead, one parquet **footer
//! metadata** read per file uses the per-row-group row counts to identify the row group(s) holding
//! the requested positions, and the Arrow reader is scoped with `with_row_groups` + a
//! [`RowSelection`] that skips to the exact
//! rows. IO and decode are bounded by the touched row groups (and, where the writer emitted an
//! offset index, the touched pages — the iceberg [`ArrowFileReader`] preloads offset indexes),
//! not by the file prefix. All columns are projected — hydration returns the full row; the win is
//! skipping rows, not columns.
//!
//! The read goes to the `parquet` crate directly over the **same `FileIO` stack** the iceberg
//! scan path uses (opendal S3 + built-in retry, coalesced concurrent range fetches), because
//! iceberg-rust's scan API has no positional selection. That makes positions **physical** row
//! positions — an exact drop-in for delete-free files, where the ingest-time stream position
//! equals the physical position. Files carrying delete files keep the iceberg streaming read
//! (see [`hydrate`](crate::IcebergReader::hydrate)'s pass 1), so verify-and-fall-back semantics
//! are unchanged: a targeted read that yields a row whose key doesn't match goes stale exactly
//! as before.

use std::collections::{BTreeMap, BTreeSet};

use futures::TryStreamExt;
use growlerdb_core::Value;
use iceberg::arrow::ArrowFileReader;
use iceberg::io::FileIO;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

use crate::{full_row, Result};

/// The targeted-read plan for one data file: which row groups to read and, within the
/// concatenation of those groups' rows, which exact rows to select.
#[derive(Debug)]
pub(crate) struct PointReadPlan {
    /// Indexes of the row groups holding at least one requested position.
    pub(crate) row_groups: Vec<usize>,
    /// Row selection **relative to the concatenated rows of the selected row groups** (rows of
    /// skipped groups don't appear — the `parquet` crate's contract for
    /// `with_row_groups` + `with_row_selection`).
    pub(crate) selection: RowSelection,
    /// The in-range requested positions, ascending and deduplicated — exactly the order the
    /// selected rows emerge from the reader.
    pub(crate) positions: Vec<u64>,
}

/// Compute the [`PointReadPlan`] for `positions` (absolute row positions within the data file,
/// as recorded at ingest) from the footer's per-row-group row counts. Positions at/past EOF are
/// dropped — a stale-locator signal, not an error: the caller's verify → fallback handles them
/// exactly as the old stream-past-the-end read did. `None` when no position is in range.
pub(crate) fn plan_point_selection(
    row_group_rows: &[u64],
    positions: &[u64],
) -> Option<PointReadPlan> {
    let total: u64 = row_group_rows.iter().sum();
    let want: BTreeSet<u64> = positions.iter().copied().filter(|&p| p < total).collect();
    if want.is_empty() {
        return None;
    }
    let mut row_groups = Vec::new();
    let mut selectors: Vec<RowSelector> = Vec::new();
    let mut cursor = 0u64; // rows covered by `selectors` so far (within selected groups)
    let mut selected_rows = 0u64; // total rows of the groups selected so far
    let mut group_start = 0u64; // absolute position of the current group's first row
    for (idx, &rows) in row_group_rows.iter().enumerate() {
        let in_group: Vec<u64> = want
            .range(group_start..group_start + rows)
            .copied()
            .collect();
        if !in_group.is_empty() {
            row_groups.push(idx);
            for p in in_group {
                let rel = selected_rows + (p - group_start);
                if rel > cursor {
                    selectors.push(RowSelector::skip((rel - cursor) as usize));
                }
                selectors.push(RowSelector::select(1));
                cursor = rel + 1;
            }
            selected_rows += rows;
        }
        group_start += rows;
    }
    if selected_rows > cursor {
        // Trailing skip to the end of the last selected group, per the documented shape.
        selectors.push(RowSelector::skip((selected_rows - cursor) as usize));
    }
    Some(PointReadPlan {
        row_groups,
        selection: RowSelection::from(selectors),
        positions: want.into_iter().collect(),
    })
}

/// Point-read the **full rows** (all columns) at `positions` from the parquet data file at
/// `path`, over the given `FileIO` — the same opendal-backed IO (retry, coalesced range fetches)
/// the scan path uses. One footer read serves every position; only the containing row groups are
/// fetched and decoded. Returns `position → full row`; positions past EOF are simply absent
/// (stale, deferred to the fallback — not an error).
pub(crate) async fn read_file_rows(
    file_io: &FileIO,
    path: &str,
    positions: &[u64],
) -> Result<BTreeMap<u64, BTreeMap<String, Value>>> {
    let input = file_io.new_input(path)?;
    let meta = input.metadata().await?;
    let reader = ArrowFileReader::new(meta, input.reader().await?);
    read_rows_at(reader, positions).await
}

/// As [`read_file_rows`], over any parquet [`AsyncFileReader`] — separated so tests can wrap the
/// reader to count the bytes actually fetched.
pub(crate) async fn read_rows_at<R>(
    reader: R,
    positions: &[u64],
) -> Result<BTreeMap<u64, BTreeMap<String, Value>>>
where
    R: AsyncFileReader + Unpin + Send + 'static,
{
    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let row_group_rows: Vec<u64> = builder
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as u64)
        .collect();
    let Some(plan) = plan_point_selection(&row_group_rows, positions) else {
        return Ok(BTreeMap::new()); // every position past EOF → stale → caller falls back
    };
    let mut stream = builder
        .with_row_groups(plan.row_groups)
        .with_row_selection(plan.selection)
        .build()?;
    // The selection emits exactly the planned rows, in ascending-position order.
    let mut rows = BTreeMap::new();
    let mut planned = plan.positions.into_iter();
    while let Some(batch) = stream.try_next().await? {
        for row in 0..batch.num_rows() {
            let Some(pos) = planned.next() else {
                break; // defensive: never more rows than planned
            };
            rows.insert(pos, full_row(&batch, row));
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{body_for, fs_file_io, write_docs_parquet};

    fn select(n: usize) -> RowSelector {
        RowSelector::select(n)
    }
    fn skip(n: usize) -> RowSelector {
        RowSelector::skip(n)
    }

    #[test]
    fn plan_targets_only_the_containing_row_groups() {
        // Groups of 4|4|4 rows; positions 1 and 10 live in groups 0 and 2 — group 1 is never
        // read, and the selection is relative to the 8 rows of the two selected groups.
        let plan = plan_point_selection(&[4, 4, 4], &[10, 1]).expect("plan");
        assert_eq!(plan.row_groups, vec![0, 2]);
        assert_eq!(plan.positions, vec![1, 10]);
        assert_eq!(
            plan.selection,
            RowSelection::from(vec![skip(1), select(1), skip(4), select(1), skip(1)])
        );
    }

    #[test]
    fn plan_selects_first_and_last_rows() {
        let plan = plan_point_selection(&[5, 5], &[0, 9]).expect("plan");
        assert_eq!(plan.row_groups, vec![0, 1]);
        assert_eq!(
            plan.selection,
            RowSelection::from(vec![select(1), skip(8), select(1)])
        );
    }

    #[test]
    fn plan_drops_positions_past_eof_and_dedupes() {
        // 12 rows total: 100 is out of range (stale — omitted, not an error); 2 is requested
        // twice but selected once.
        let plan = plan_point_selection(&[6, 6], &[2, 100, 2]).expect("plan");
        assert_eq!(plan.row_groups, vec![0]);
        assert_eq!(plan.positions, vec![2]);
        assert_eq!(
            plan.selection,
            RowSelection::from(vec![skip(2), select(1), skip(3)])
        );
    }

    #[test]
    fn plan_is_none_when_everything_is_past_eof() {
        assert!(plan_point_selection(&[4, 4], &[8, 99]).is_none());
        assert!(plan_point_selection(&[], &[0]).is_none());
    }

    /// End-to-end over a real multi-row-group parquet file on local disk (the same `FileIO`
    /// stack production uses): first row, last row, middle rows, and rows spanning several row
    /// groups resolve in one call, with full-row equality against the written source rows.
    #[tokio::test]
    async fn point_read_resolves_rows_across_row_groups() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.parquet");
        let path = path.to_str().unwrap();
        // 1000 rows in row groups of 100 → 10 groups.
        write_docs_parquet(path, 0, 1000, 100);

        let positions = [0_u64, 99, 100, 555, 650, 999]; // first, group edges, middle, last
        let rows = point_read_rows(path, &positions).await;
        assert_eq!(rows.len(), positions.len());
        for &p in &positions {
            let full = &rows[&p];
            assert_eq!(full["id"], Value::Int(p as i64), "id at {p}");
            assert_eq!(full["body"], Value::Str(body_for(p as i64)), "body at {p}");
            assert_eq!(full.len(), 2, "all columns projected at {p}");
        }
    }

    #[tokio::test]
    async fn point_read_omits_positions_past_eof() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("docs.parquet");
        let path = path.to_str().unwrap();
        write_docs_parquet(path, 0, 30, 10);

        // 29 resolves; 30 and 9999 are stale signals → absent, not errors.
        let rows = point_read_rows(path, &[29, 30, 9999]).await;
        assert_eq!(rows.keys().copied().collect::<Vec<_>>(), vec![29]);

        let none = point_read_rows(path, &[30]).await;
        assert!(none.is_empty(), "all past EOF → empty (falls back)");
    }

    /// Efficiency signal: hydrating one **late** row must not read the whole file. A counting
    /// reader wraps the production [`ArrowFileReader`] and tallies every data-byte fetch (footer
    /// metadata reads go through the inner reader's own `get_metadata` and are excluded — the
    /// footer is fixed overhead either way); the tally must be a small fraction of the file.
    #[tokio::test]
    async fn point_read_of_a_late_row_reads_far_less_than_the_file() {
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
        // 20k rows in row groups of 500 → 40 groups; a late row touches exactly one.
        let file_len = write_docs_parquet(path, 0, 20_000, 500);

        let file_io = fs_file_io();
        let input = file_io.new_input(path).unwrap();
        let meta = input.metadata().await.unwrap();
        let bytes = Arc::new(AtomicU64::new(0));
        let reader = CountingReader {
            inner: ArrowFileReader::new(meta, input.reader().await.unwrap()),
            bytes: bytes.clone(),
        };

        let rows = read_rows_at(reader, &[19_999]).await.expect("point read");
        assert_eq!(rows[&19_999]["id"], Value::Int(19_999));

        let read = bytes.load(Ordering::Relaxed);
        assert!(read > 0, "data bytes were fetched through the reader");
        assert!(
            read < file_len / 4,
            "one late row read {read} of {file_len} bytes — expected far less than the file"
        );
    }

    /// Read `positions` from `path` through the production entry point.
    async fn point_read_rows(
        path: &str,
        positions: &[u64],
    ) -> BTreeMap<u64, BTreeMap<String, Value>> {
        read_file_rows(&fs_file_io(), path, positions)
            .await
            .expect("point read")
    }
}
