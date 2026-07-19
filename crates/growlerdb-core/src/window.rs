//! **Time-window (range) sharding** ([Service architecture]). Where [hash/partition
//! routing](crate::routing) spreads keys over a *fixed* shard count, time-windowing buckets a
//! **time field** into **contiguous windows** that grow over time — one shard per window. That
//! lets time-range queries **prune** to the overlapping windows and lets cold (older) windows be
//! **parked** to object storage and revived on demand ([Deployment ops]).
//!
//! Buckets are fixed-duration and aligned to the Unix epoch (no calendar library, fully
//! deterministic across processes/releases). A window's **id** is the epoch-**micros** of its start
//! — the same canonical scale the index, range queries, and the console time filter use,
//! so a `format`-declared timestamp can be the window field and pruning stays correct.
//!
//! [Service architecture]: ../../../okf/system/architecture.md
//! [Deployment ops]: ../../../okf/system/deployment/index.md

use serde::{Deserialize, Serialize};

use crate::api::{CommitBatch, DocOp};
use crate::doc::{Document, Value};
use crate::query::Query;
use crate::timestamp::TimeFormat;

/// Window granularity — a fixed duration, epoch-aligned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowGranularity {
    Hourly,
    Daily,
    Weekly,
}

impl WindowGranularity {
    /// The window length in milliseconds.
    pub fn millis(self) -> i64 {
        match self {
            WindowGranularity::Hourly => 3_600_000,
            WindowGranularity::Daily => 86_400_000,
            WindowGranularity::Weekly => 7 * 86_400_000,
        }
    }

    /// The window length in **microseconds** — the canonical unit window ids and zone-maps use.
    pub fn micros(self) -> i64 {
        self.millis() * 1_000
    }
}

/// Time-window routing for an index: the source **time field** (a `DATE` timestamp, canonical
/// micros) and the window [granularity](WindowGranularity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TimeWindowing {
    /// The **ingest-time** field whose value (epoch micros) places a document in a window. Windowing
    /// by ingest time keeps old windows immutable (late events land in the current window).
    pub field: String,
    /// Window length.
    pub granularity: WindowGranularity,
    /// Optional **event-time** field (epoch micros) to keep a per-window `[min, max]` **zone-map** on,
    /// so event-time queries can prune ingest-time windows. `None` = no event-time zone-map.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_time_field: Option<String>,
    /// Cold-tiering policy: the number of most-recent windows to keep **hot** (on local
    /// NVMe). Older windows are eligible for **parking** to object storage. `None` = keep every
    /// window hot (no parking).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hot_windows: Option<usize>,
}

impl TimeWindowing {
    /// New time-windowing over the ingest-time `field` at `granularity` (no event-time zone-map).
    pub fn new(field: impl Into<String>, granularity: WindowGranularity) -> Self {
        Self {
            field: field.into(),
            granularity,
            event_time_field: None,
            hot_windows: None,
        }
    }

    /// Set the event-time field to maintain the per-window zone-map on.
    pub fn with_event_time(mut self, field: impl Into<String>) -> Self {
        self.event_time_field = Some(field.into());
        self
    }

    /// Keep the `n` most-recent windows hot; older ones are parkable (cold-tiering).
    pub fn with_hot_windows(mut self, n: usize) -> Self {
        self.hot_windows = Some(n);
        self
    }

    /// The windows eligible for **parking** under the hot-window policy: all but the `keep` (or
    /// [`hot_windows`](Self::hot_windows)) most-recent. `windows` must be **ascending** — window ids
    /// are epoch-ms starts, so ascending = oldest first (the natural order of the store's
    /// `window_shards`). With no policy and no `keep`, nothing is cold. An explicit `keep` overrides
    /// the stored policy (e.g. a CLI `--keep-hot` flag).
    pub fn cold_windows<'a>(&self, windows: &'a [i64], keep: Option<usize>) -> &'a [i64] {
        match keep.or(self.hot_windows) {
            Some(hot) => &windows[..windows.len().saturating_sub(hot)],
            None => &[],
        }
    }

    /// The **window id** (epoch-micros of the window start) a timestamp falls in. `div_euclid` keeps
    /// pre-epoch timestamps bucketing correctly.
    pub fn window_of(&self, epoch_us: i64) -> i64 {
        let w = self.granularity.micros();
        epoch_us.div_euclid(w) * w
    }

    /// Whether the window starting at `window_start` overlaps the inclusive time range
    /// `[lo, hi]` (either bound `None` = unbounded). Used to prune which window shards a
    /// time-filtered query must touch: the window covers `[window_start, window_start + len)`.
    pub fn window_overlaps(&self, window_start: i64, lo: Option<i64>, hi: Option<i64>) -> bool {
        let window_end = window_start + self.granularity.micros(); // exclusive
        let after_lo = lo.is_none_or(|l| window_end > l);
        let before_hi = hi.is_none_or(|h| window_start <= h);
        after_lo && before_hi
    }

    /// The window-id bounds `[lo_window, hi_window]` overlapping a `[lo, hi]` query range — the
    /// caller intersects these with the index's *existing* windows (the registry's window→shard
    /// map) to get the shards to query. `None` bounds stay unbounded (scatter to all live windows).
    pub fn window_bounds(&self, lo: Option<i64>, hi: Option<i64>) -> (Option<i64>, Option<i64>) {
        (lo.map(|t| self.window_of(t)), hi.map(|t| self.window_of(t)))
    }

    /// Which windows a `query` must touch (pruning): those whose **ingest-window** id
    /// overlaps the query's range on [`field`](Self::field) **and** whose **event-time zone-map**
    /// overlaps the query's range on `event_time_field`. `windows` is `(window-id, zone-map)` from
    /// the registry; a window with **no** zone-map (`None`) is conservatively always included. An
    /// unfiltered query (no range on either field) returns every window. Result preserves order.
    pub fn prune<I>(&self, windows: I, query: &Query) -> Vec<i64>
    where
        I: IntoIterator<Item = (i64, Option<(i64, i64)>)>,
    {
        windows
            .into_iter()
            .filter(|(w, zone)| self.keeps(*w, *zone, query))
            .map(|(w, _)| w)
            .collect()
    }

    /// Whether a single window (its id + optional event `zone`-map) survives `query`'s pruning —
    /// the per-window predicate behind [`prune`](Self::prune). The store/embedded path uses this to
    /// prune **lazily**: test the ingest-window id cheaply before opening a shard, then re-test with
    /// the shard's event zone-map once it's read (the id check is idempotent). A `None` zone-map or
    /// a query without the relevant range bound is conservatively kept.
    pub fn keeps(&self, window: i64, zone: Option<(i64, i64)>, query: &Query) -> bool {
        let (ingest_lo, ingest_hi) = query.range_bounds(&self.field);
        if !self.window_overlaps(window, ingest_lo, ingest_hi) {
            return false;
        }
        match (self.event_time_field.as_deref(), zone) {
            (Some(field), Some((lo, hi))) => {
                let (event_lo, event_hi) = query.range_bounds(field);
                range_overlaps(lo, hi, event_lo, event_hi)
            }
            _ => true,
        }
    }

    /// Partition a [`CommitBatch`]'s **upserts** by the ingest-time window of [`field`](Self::field),
    /// computing each window's **event-time** `[min, max]` from `event_time_field` for the
    /// per-window zone-map. Returns `(window_batches, deletes)`:
    ///
    /// * **Upserts** carry their document, so each routes to `window_of(doc[field])`, **normalized to
    ///   canonical micros** via the field's declared [`TimeFormat`] (`window_format` / `event_format`;
    ///   `None` = a native `DATE`, already micros). An upsert whose window value is missing /
    ///   unparseable falls into window `0` (the connector should always set the ingest-time field).
    /// * **Deletes** carry only a key — no window value — so they can't be windowed here; the store
    ///   broadcasts them to all windows (rare for the append-mostly sources windowing targets).
    ///
    /// Each window's sub-batch keeps the same `checkpoint` and gets a window-unique `batch_id`
    /// (`{batch_id}#w{window}`) so idempotent retries stay per-window.
    ///
    /// **Checkpoint carrying is deliberately asymmetric** (the JVM `WindowedWriteClient`
    /// mirrors this):
    ///
    /// * `from_checkpoint` is **not** carried. Unlike ordinal shards (lockstep — every shard
    ///   gets a possibly-empty sub-batch each commit, so `from` always continues from `current`),
    ///   windows advance independently: a window receives a sub-batch only when rows route to it,
    ///   so its checkpoint legitimately skips batches. Carrying the stream's `from` would trip
    ///   that window's continuity guard with a false `CheckpointGap`. Windowed resume safety
    ///   comes from the connector instead: it resumes from the **server-derived min committed
    ///   checkpoint** across windows and replays idempotently (`batch_id` dedup).
    /// * `safe_checkpoint` **is** carried: the connector's resume floor is global (it never
    ///   resumes below it for any window), so each window can prune its idempotency records —
    ///   without it, a windowed shard's batch tables would grow without bound.
    pub fn partition_batch(
        &self,
        batch: &CommitBatch,
        window_format: Option<TimeFormat>,
        event_time_field: Option<&str>,
        event_format: Option<TimeFormat>,
    ) -> (Vec<WindowBatch>, Vec<DocOp>) {
        use std::collections::BTreeMap;
        // window-start → accumulator. BTreeMap keeps windows in time order.
        let mut by_window: BTreeMap<i64, WindowAcc> = BTreeMap::new();
        let mut deletes = Vec::new();
        for op in &batch.ops {
            match op {
                DocOp::Upsert(located) => {
                    let window = self.window_of(
                        field_micros(&located.doc, &self.field, window_format).unwrap_or(0),
                    );
                    let acc = by_window.entry(window).or_default();
                    if let Some(ef) = event_time_field {
                        if let Some(ev) = field_micros(&located.doc, ef, event_format) {
                            acc.event_min = Some(acc.event_min.map_or(ev, |m| m.min(ev)));
                            acc.event_max = Some(acc.event_max.map_or(ev, |m| m.max(ev)));
                        }
                    }
                    acc.ops.push(op.clone());
                }
                DocOp::Delete(_) => deletes.push(op.clone()),
            }
        }
        let windows = by_window
            .into_iter()
            .map(|(window, acc)| WindowBatch {
                window,
                batch: CommitBatch::new(
                    acc.ops,
                    batch.checkpoint.clone(),
                    format!("{}#w{window}", batch.batch_id),
                )
                .with_safe_checkpoint(batch.safe_checkpoint.clone()),
                event_min: acc.event_min,
                event_max: acc.event_max,
            })
            .collect();
        (windows, deletes)
    }
}

/// A sub-batch routed to one ingest-time window, plus the event-time span it covers (the window's
/// zone-map). Upserts only — see [`TimeWindowing::partition_batch`].
#[derive(Debug, Clone)]
pub struct WindowBatch {
    /// The window-start id (from the ingest-time field).
    pub window: i64,
    /// The upsert sub-batch for this window.
    pub batch: CommitBatch,
    /// Min event-time across this window's upserts (`None` if no event-time field / value).
    pub event_min: Option<i64>,
    /// Max event-time across this window's upserts.
    pub event_max: Option<i64>,
}

/// Per-window accumulator while partitioning a batch: the upsert ops + the event-time span seen.
#[derive(Default)]
struct WindowAcc {
    ops: Vec<DocOp>,
    event_min: Option<i64>,
    event_max: Option<i64>,
}

/// Whether the closed span `[min, max]` overlaps the (possibly open) range `[lo, hi]`.
fn range_overlaps(min: i64, max: i64, lo: Option<i64>, hi: Option<i64>) -> bool {
    lo.is_none_or(|l| max >= l) && hi.is_none_or(|h| min <= h)
}

/// Read a timestamp field off a document as **canonical epoch micros**. A `format`-declared field
/// (the source unit, e.g. `epoch_ms`) is normalized via [`TimeFormat::to_micros`]; a `None` format
/// is a native `DATE` whose value is already micros. Missing / unparseable → `None`.
fn field_micros(doc: &Document, field: &str, format: Option<TimeFormat>) -> Option<i64> {
    let v = doc.fields.get(field)?;
    match format {
        Some(fmt) => fmt.to_micros(field, v).ok(),
        None => match v {
            Value::Int(i) => Some(*i),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: i64 = 86_400_000_000; // one day in **micros** (the canonical window scale)

    #[test]
    fn buckets_align_to_the_window() {
        let w = TimeWindowing::new("ts", WindowGranularity::Daily);
        // 2021-01-01T00:00:00Z = 1_609_459_200_000_000 µs (an exact UTC day boundary).
        let day0 = 1_609_459_200_000_000;
        assert_eq!(w.window_of(day0), day0);
        assert_eq!(w.window_of(day0 + 1), day0); // same day
        assert_eq!(w.window_of(day0 + DAY - 1), day0); // last µs of the day
        assert_eq!(w.window_of(day0 + DAY), day0 + DAY); // next day → next window
    }

    #[test]
    fn granularity_lengths() {
        assert_eq!(WindowGranularity::Hourly.millis(), 3_600_000);
        assert_eq!(WindowGranularity::Daily.millis(), 86_400_000);
        assert_eq!(WindowGranularity::Weekly.millis(), 7 * 86_400_000);
        // The canonical window scale is micros (= millis × 1000).
        assert_eq!(WindowGranularity::Hourly.micros(), 3_600_000_000);
        assert_eq!(WindowGranularity::Daily.micros(), DAY);
        assert_eq!(WindowGranularity::Weekly.micros(), 7 * DAY);
    }

    #[test]
    fn overlap_prunes_windows_outside_the_range() {
        let w = TimeWindowing::new("ts", WindowGranularity::Daily);
        let day = |n: i64| n * DAY; // window starts
                                    // Query range fully inside day 10.
        let lo = Some(day(10) + 1000);
        let hi = Some(day(10) + 2000);
        assert!(!w.window_overlaps(day(9), lo, hi)); // day before → no
        assert!(w.window_overlaps(day(10), lo, hi)); // the day → yes
        assert!(!w.window_overlaps(day(11), lo, hi)); // day after → no
    }

    #[test]
    fn overlap_spans_multiple_windows_and_open_bounds() {
        let w = TimeWindowing::new("ts", WindowGranularity::Daily);
        let day = |n: i64| n * DAY;
        // [day8 .. day10] touches windows 8, 9, 10.
        let (lo, hi) = (Some(day(8) + 5), Some(day(10) + 5));
        for d in [8, 9, 10] {
            assert!(w.window_overlaps(day(d), lo, hi), "day {d} overlaps");
        }
        assert!(!w.window_overlaps(day(7), lo, hi));
        assert!(!w.window_overlaps(day(11), lo, hi));
        // Open upper bound: everything from day 8 on overlaps.
        assert!(w.window_overlaps(day(100), Some(day(8)), None));
        assert!(!w.window_overlaps(day(7), Some(day(8)), None));
        // Fully unbounded → every window overlaps.
        assert!(w.window_overlaps(day(0), None, None));
    }

    #[test]
    fn window_bounds_floor_to_the_window() {
        let w = TimeWindowing::new("ts", WindowGranularity::Daily);
        let day = |n: i64| n * DAY;
        assert_eq!(
            w.window_bounds(Some(day(3) + 999), Some(day(5) + 1)),
            (Some(day(3)), Some(day(5)))
        );
        assert_eq!(w.window_bounds(None, None), (None, None));
    }

    fn upsert(id: &str, ingest: i64, event: Option<i64>) -> DocOp {
        use crate::doc::CompositeKey;
        let mut f = std::collections::BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("ingest".to_string(), Value::Int(ingest));
        if let Some(e) = event {
            f.insert("event".to_string(), Value::Int(e));
        }
        DocOp::Upsert(crate::api::LocatedDoc {
            doc: Document::new(
                CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]),
                f,
            ),
            iceberg_file: "f".into(),
            row_position: 0,
        })
    }

    #[test]
    fn partition_batch_windows_upserts_and_tracks_event_bounds() {
        use crate::doc::{CompositeKey, SourceCheckpoint};
        let w = TimeWindowing::new("ingest", WindowGranularity::Daily);
        let day = |n: i64| n * DAY;
        let batch = CommitBatch::new(
            vec![
                upsert("a", day(10) + 5, Some(day(10) + 5)), // ingest win 10, event day 10
                // Ingested day 10 but the EVENT happened day 2 (late by 8 days):
                upsert("b", day(10) + 9, Some(day(2) + 1)),
                upsert("c", day(11) + 1, Some(day(11) + 1)), // ingest win 11
                DocOp::Delete(CompositeKey::new(
                    vec![],
                    vec![("id".into(), Value::from("z"))],
                )),
            ],
            SourceCheckpoint::iceberg(1),
            "b1",
        );
        // Native-DATE window/event fields (already micros) → no format normalization needed.
        let (windows, deletes) = w.partition_batch(&batch, None, Some("event"), None);

        assert_eq!(windows.len(), 2, "two ingest windows (10 and 11)");
        let w10 = windows.iter().find(|x| x.window == day(10)).unwrap();
        assert_eq!(w10.batch.ops.len(), 2, "a + b both ingested on day 10");
        // The late event widens window 10's event-time zone-map down to day 2 — this is what lets
        // an event-time query for day 2 still find `b` even though it lives in the day-10 window.
        assert_eq!(w10.event_min, Some(day(2) + 1));
        assert_eq!(w10.event_max, Some(day(10) + 5));
        let w11 = windows.iter().find(|x| x.window == day(11)).unwrap();
        assert_eq!(w11.event_min, Some(day(11) + 1));
        assert_eq!(w11.batch.batch_id, format!("b1#w{}", day(11)));

        assert_eq!(
            deletes.len(),
            1,
            "deletes are returned for broadcast, not windowed"
        );
    }

    #[test]
    fn partition_batch_carries_the_safe_floor_but_never_from() {
        use crate::doc::SourceCheckpoint;
        let w = TimeWindowing::new("ingest", WindowGranularity::Daily);
        let batch = CommitBatch::new(
            vec![upsert("a", 10 * DAY, None), upsert("b", 11 * DAY, None)],
            SourceCheckpoint::iceberg_ordered(9, 9),
            "b1",
        )
        .with_from_checkpoint(Some(SourceCheckpoint::iceberg_ordered(8, 8)))
        .with_safe_checkpoint(Some(SourceCheckpoint::iceberg_ordered(5, 5)));
        let (windows, _) = w.partition_batch(&batch, None, None, None);
        assert_eq!(windows.len(), 2);
        for wb in &windows {
            // The resume floor is global across windows → carried, so idempotency records prune.
            assert_eq!(
                wb.batch.safe_checkpoint,
                Some(SourceCheckpoint::iceberg_ordered(5, 5))
            );
            // `from` continuity is lockstep-only (ordinal shards); a window that skips batches
            // would false-Gap on it — deliberately absent here.
            assert_eq!(wb.batch.from_checkpoint, None);
        }
    }

    #[test]
    fn partition_batch_handles_missing_fields() {
        let w = TimeWindowing::new("ingest", WindowGranularity::Daily);
        let batch = CommitBatch::new(
            vec![
                upsert("a", 5_000, None), // window 0
                upsert("b", DAY, None),   // window 1 (one day in micros)
            ],
            crate::doc::SourceCheckpoint::iceberg(1),
            "b1",
        );
        // No event-time field requested → no zone-map bounds.
        let (windows, _) = w.partition_batch(&batch, None, None, None);
        assert_eq!(windows.len(), 2);
        assert!(windows
            .iter()
            .all(|x| x.event_min.is_none() && x.event_max.is_none()));
    }

    #[test]
    fn a_format_declared_millis_window_field_buckets_in_micros() {
        // The window field is an `epoch_ms` source column — its raw millis values must be
        // normalized to canonical micros before bucketing, so windows line up with the index/range
        // path (which also stores micros).
        let w = TimeWindowing::new("ingest", WindowGranularity::Daily).with_event_time("event");
        let day_ms = |n: i64| n * 86_400_000; // source values in **millis**
        let day_us = |n: i64| n * DAY; // expected window ids in **micros**
        let batch = CommitBatch::new(
            vec![
                upsert("a", day_ms(10) + 5, Some(day_ms(10) + 5)),
                upsert("b", day_ms(11) + 1, Some(day_ms(11) + 1)),
            ],
            crate::doc::SourceCheckpoint::iceberg(1),
            "b1",
        );
        let (windows, _) = w.partition_batch(
            &batch,
            Some(TimeFormat::EpochMillis),
            Some("event"),
            Some(TimeFormat::EpochMillis),
        );
        assert_eq!(windows.len(), 2);
        // The day-10 millis source landed in the day-10 **micros** window, and its zone-map is micros.
        let w10 = windows.iter().find(|x| x.window == day_us(10)).unwrap();
        assert_eq!(w10.event_min, Some(day_us(10) + 5_000)); // 5 ms == 5_000 µs
        assert!(windows.iter().any(|x| x.window == day_us(11)));
    }

    #[test]
    fn cold_windows_keeps_most_recent_hot() {
        let w = TimeWindowing::new("ingest", WindowGranularity::Daily);
        let days = [10, 11, 12, 13, 14]; // ascending = oldest first
                                         // No policy → nothing parks.
        assert_eq!(w.cold_windows(&days, None), &[] as &[i64]);
        // Policy keeps the 2 most-recent hot → the 3 oldest are cold.
        let w2 = w.clone().with_hot_windows(2);
        assert_eq!(w2.cold_windows(&days, None), &[10, 11, 12]);
        // Explicit `keep` overrides the stored policy.
        assert_eq!(w2.cold_windows(&days, Some(4)), &[10]);
        // Keeping more than exist → nothing cold (saturating).
        assert_eq!(w2.cold_windows(&days, Some(9)), &[] as &[i64]);
    }

    #[test]
    fn prune_selects_windows_by_ingest_and_event_range() {
        let w = TimeWindowing::new("ingest", WindowGranularity::Daily).with_event_time("event");
        let day = |n: i64| n * DAY;
        let windows = vec![
            (day(0), Some((day(0), day(0) + 100))),
            (day(1), Some((day(1), day(1) + 100))),
            (day(2), Some((day(2), day(2) + 100))),
            (day(3), None), // no zone-map → conservatively always kept
        ];

        // Ingest filter day1..day2 → only those ingest windows overlap (zone-maps irrelevant here).
        let q = Query::parse(&format!("ingest:[{} TO {}]", day(1), day(2) + 5)).unwrap();
        assert_eq!(w.prune(windows.clone(), &q), vec![day(1), day(2)]);

        // No range filter → every window.
        let none = Query::parse("status:active").unwrap();
        assert_eq!(
            w.prune(windows.clone(), &none),
            vec![day(0), day(1), day(2), day(3)]
        );

        // Event filter inside day 0's span → keep day0 (zone overlaps) + day3 (no zone-map);
        // day1/day2 zone-maps don't overlap, so they prune out even though ingest is unbounded.
        let qe = Query::parse(&format!("event:[{} TO {}]", day(0), day(0) + 50)).unwrap();
        assert_eq!(w.prune(windows, &qe), vec![day(0), day(3)]);
    }
}
