//! The minimal in-process **Index API** ([Design 02]) — the seam that the write
//! path (ingest) and read path (engine) depend on, with storage behind it.
//!
//! A single in-process shard, no gRPC, no admin/compaction.
//! The traits live here in `growlerdb-core` so the engine can be written against the
//! seam while `growlerdb-index` provides the [`LocalIndexStore`] implementation. The
//! vocabulary types ([`Snapshot`], [`RowLocator`], [`CommitBatch`], [`Hit`], …)
//! live here for the same reason.
//!
//! [Design 02]: ../../../design/02-index-api.md
//! [`LocalIndexStore`]: ../../growlerdb_index/store/struct.LocalIndexStore.html

use serde::{Deserialize, Serialize};

use crate::doc::{CompositeKey, Document, SourceCheckpoint, Value};
use crate::query::Query;

/// A monotonic index snapshot — the result of a successful [`write`](IndexWriter::write).
/// Pins a consistent read view: reads observe a committed snapshot, never a
/// partial one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Snapshot(pub u64);

/// A document paired with where its row lives in the source, for the locator.
#[derive(Debug, Clone)]
pub struct LocatedDoc {
    /// The document to index.
    pub doc: Document,
    /// Source data-file path the row came from.
    pub iceberg_file: String,
    /// Row position within `iceberg_file`.
    pub row_position: u64,
}

/// A single change to apply, keyed by the composite document key ([Design 02]).
/// A **changelog** reduces to these: `INSERT`/`UPDATE_AFTER` → [`Upsert`](DocOp::Upsert),
/// `DELETE`/`UPDATE_BEFORE` → [`Delete`](DocOp::Delete).
///
/// [Design 02]: ../../../design/02-index-api.md
#[derive(Debug, Clone)]
pub enum DocOp {
    /// Index (or replace) the document; carries its source location for the locator.
    Upsert(LocatedDoc),
    /// Remove the document for this key (logical delete).
    Delete(CompositeKey),
}

impl DocOp {
    /// The composite key this op applies to — the upserted document's key, or the deleted
    /// key. Both [shard routing](crate::ShardRouter) and idempotent apply key off this.
    pub fn key(&self) -> &CompositeKey {
        match self {
            DocOp::Upsert(located) => &located.doc.key,
            DocOp::Delete(key) => key,
        }
    }
}

/// A batch to commit: a sequence of [`DocOp`]s + the checkpoint they bring the
/// index to. The realization of [Design 02]'s `DocBatch` (the per-doc source
/// location is passed explicitly rather than derived during indexing).
///
/// [Design 02]: ../../../design/02-index-api.md
#[derive(Debug, Clone)]
pub struct CommitBatch {
    /// The ordered changes to apply.
    pub ops: Vec<DocOp>,
    /// The source position the index reflects after this commit.
    pub checkpoint: SourceCheckpoint,
    /// Opaque batch id, for idempotent retries.
    pub batch_id: String,
    /// The source position this batch resumes **from** (the prior checkpoint, exclusive); `None` =
    /// from the start of the changelog. The write path's continuity guard refuses a batch
    /// whose `from` doesn't equal the shard's current checkpoint, so a checkpoint can't advance over
    /// unapplied data. Defaults to `None` (unguarded) for callers that don't drive resumable ingest.
    pub from_checkpoint: Option<SourceCheckpoint>,
    /// The connector's **resume floor**: the position it would restart the changelog read from (the
    /// min committed checkpoint across all shards). The connector never resumes before it and reads
    /// the changelog from it *exclusive*, so a batch at or below it can never be re-sent — the write
    /// path prunes the idempotency records (`batch_id`s) for those batches to bound the local store.
    /// `None` = no floor yet, so nothing is pruned. Unlike [`from_checkpoint`] (this
    /// window's start, per sub-batch), the floor is identical across a trigger's sub-batches.
    ///
    /// [`from_checkpoint`]: Self::from_checkpoint
    pub safe_checkpoint: Option<SourceCheckpoint>,
}

impl CommitBatch {
    /// Build a batch from explicit ops.
    pub fn new(ops: Vec<DocOp>, checkpoint: SourceCheckpoint, batch_id: impl Into<String>) -> Self {
        Self {
            ops,
            checkpoint,
            batch_id: batch_id.into(),
            from_checkpoint: None,
            safe_checkpoint: None,
        }
    }

    /// Set the `from` checkpoint this batch resumes from (continuity guard). Builder-style
    /// so the common non-resumable callers keep the two/three-arg constructors.
    pub fn with_from_checkpoint(mut self, from: Option<SourceCheckpoint>) -> Self {
        self.from_checkpoint = from;
        self
    }

    /// Set the resume-floor [`safe_checkpoint`](Self::safe_checkpoint) that bounds the idempotency
    /// store. Builder-style; defaults to `None` (prune nothing) for callers that don't
    /// drive resumable ingest.
    pub fn with_safe_checkpoint(mut self, safe: Option<SourceCheckpoint>) -> Self {
        self.safe_checkpoint = safe;
        self
    }

    /// Build an upsert-only batch (the append/backfill path).
    pub fn from_upserts(
        docs: Vec<LocatedDoc>,
        checkpoint: SourceCheckpoint,
        batch_id: impl Into<String>,
    ) -> Self {
        Self::new(
            docs.into_iter().map(DocOp::Upsert).collect(),
            checkpoint,
            batch_id,
        )
    }

    /// The upserted documents, in order.
    pub fn upserts(&self) -> impl Iterator<Item = &LocatedDoc> {
        self.ops.iter().filter_map(|op| match op {
            DocOp::Upsert(d) => Some(d),
            DocOp::Delete(_) => None,
        })
    }

    /// The deleted keys, in order.
    pub fn deletes(&self) -> impl Iterator<Item = &CompositeKey> {
        self.ops.iter().filter_map(|op| match op {
            DocOp::Delete(k) => Some(k),
            DocOp::Upsert(_) => None,
        })
    }
}

/// A row's source coordinates: how to fetch the authoritative row for a key. An
/// **in-memory** bridge between the index's layered locate and the source's
/// hydrate — never persisted or sent on the wire. Locators are best-effort: hydration
/// verifies the row by key and falls back to a scan when the coordinates went stale
/// (Iceberg rewrote the file). Anything key-derived (partition values, field names)
/// travels with the [`CompositeKey`] it is paired with, not here.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RowLocator {
    /// Iceberg data-file path holding the row.
    pub iceberg_file: String,
    /// Row position within the data file.
    pub row_position: u64,
}

/// One search hit: the document's **coordinates** (composite key) and BM25 score.
/// Never carries sensitive/big-text fields.
#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    /// The document's composite key.
    pub key: CompositeKey,
    /// The BM25 score.
    pub score: f32,
    /// **Cached** display-field values returned with the hit — the fields
    /// marked `cached`/stored in the index, so a page renders without hydration.
    /// Empty when the index caches no display fields.
    pub fields: std::collections::BTreeMap<String, Value>,
    /// **Server-side highlights**: field name → matched fragments, populated only
    /// when the search opted in ([`SearchParams::highlight`]). Reflects the analyzed match
    /// (stemming/positions), unlike a client-side literal-term marker. Empty otherwise.
    pub highlight: std::collections::BTreeMap<String, Vec<HighlightFragment>>,
}

/// One matched **fragment** of a highlighted field: an ordered run of
/// [segments](HighlightSegment) — matched terms and their surrounding context. Carried as
/// segments (not pre-marked HTML) so a client renders `<mark>` with no `innerHTML`/XSS.
#[derive(Debug, Clone, PartialEq)]
pub struct HighlightFragment {
    /// The segments of this fragment, in order.
    pub segments: Vec<HighlightSegment>,
}

/// One run within a [fragment](HighlightFragment): a stretch of text and whether it is a
/// matched term (rendered inside a `<mark>`/`<em>`) or surrounding context.
#[derive(Debug, Clone, PartialEq)]
pub struct HighlightSegment {
    /// The literal text of this run.
    pub text: String,
    /// True ⇒ this run is a matched term (render it marked).
    pub marked: bool,
}

/// Server-side highlight options: which analyzed **TEXT** fields to snippet and the
/// per-hit bounds. Set on [`SearchParams::highlight`] to opt in; unset ⇒ no highlights.
#[derive(Debug, Clone, PartialEq)]
pub struct Highlight {
    /// TEXT fields to highlight. Empty = a sensible default: every highlightable (analyzed +
    /// stored) TEXT field. Non-highlightable names are silently skipped.
    pub fields: Vec<String>,
    /// Max fragments per field (bounds the per-hit payload).
    pub max_fragments: usize,
    /// Approximate max characters per fragment (the snippet window size).
    pub fragment_size: usize,
}

/// Default max fragments returned per highlighted field — a bounded payload.
pub const DEFAULT_HIGHLIGHT_MAX_FRAGMENTS: usize = 3;
/// Default approximate characters per highlight fragment — the snippet window.
pub const DEFAULT_HIGHLIGHT_FRAGMENT_SIZE: usize = 150;
/// Ceiling on `max_fragments` — bounds the per-hit highlight payload a request can force.
pub const MAX_HIGHLIGHT_MAX_FRAGMENTS: usize = 50;
/// Ceiling on `fragment_size` — bounds the snippet window a request can force.
pub const MAX_HIGHLIGHT_FRAGMENT_SIZE: usize = 2_000;

impl Highlight {
    /// A highlight request over `fields` (empty = the default highlightable set). A
    /// `max_fragments`/`fragment_size` of 0 means "use the default"; each is clamped to its
    /// ceiling so a request can't force an unbounded per-hit payload.
    pub fn new(fields: Vec<String>, max_fragments: usize, fragment_size: usize) -> Self {
        Self {
            fields,
            max_fragments: if max_fragments == 0 {
                DEFAULT_HIGHLIGHT_MAX_FRAGMENTS
            } else {
                max_fragments.min(MAX_HIGHLIGHT_MAX_FRAGMENTS)
            },
            fragment_size: if fragment_size == 0 {
                DEFAULT_HIGHLIGHT_FRAGMENT_SIZE
            } else {
                fragment_size.min(MAX_HIGHLIGHT_FRAGMENT_SIZE)
            },
        }
    }
}

/// Which columns [hydration](RowLocator) returns from the authoritative row.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Projection {
    /// All columns of the source row.
    #[default]
    All,
    /// Only the named columns (in this order).
    Columns(Vec<String>),
}

impl Projection {
    /// Whether `column` should be returned under this projection.
    pub fn includes(&self, column: &str) -> bool {
        match self {
            Projection::All => true,
            Projection::Columns(cols) => cols.iter().any(|c| c == column),
        }
    }
}

/// An authoritative row fetched from the source by [hydration](RowLocator):
/// the document's [`CompositeKey`] plus the projected column values.
#[derive(Debug, Clone, PartialEq)]
pub struct HydratedRow {
    /// The composite key this row was fetched for.
    pub key: CompositeKey,
    /// Projected column name → value (scalar columns).
    pub fields: std::collections::BTreeMap<String, Value>,
}

/// A search-support **aggregation** request over a fast field. Tantivy-backed
/// (terms + stats). Results
/// come back as JSON (`serde_json::Value`) keyed by the request name. Externally tagged on
/// the wire (`{"Terms": {"field": …, "size": …}}`), so the Engine API can carry an agg spec.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Agg {
    /// Top-`size` buckets of a field's values, by descending doc count.
    Terms {
        /// The fast field to bucket.
        field: String,
        /// Maximum number of buckets.
        size: usize,
    },
    /// `count` / `min` / `max` / `sum` / `avg` over a numeric fast field.
    Stats {
        /// The numeric fast field.
        field: String,
    },
    /// Time buckets of a DATE fast field at a fixed interval (e.g. `"1d"`, `"3600s"`).
    ///
    /// Buckets are **UTC-only** at a *fixed* interval — Tantivy's `fixed_interval` histogram has
    /// no timezone or calendar (month/quarter) intervals and no `offset`/`min_doc_count`. A
    /// client wanting local-time day boundaries must offset client-side.
    DateHistogram {
        /// The DATE fast field.
        field: String,
        /// Bucket width as a duration string (Tantivy `fixed_interval`).
        fixed_interval: String,
    },
    /// Buckets of a numeric fast field over user-defined `[from, to)` ranges.
    Range {
        /// The numeric fast field.
        field: String,
        /// The ranges (open-ended when `from`/`to` is `None`).
        ranges: Vec<AggRange>,
    },
    /// Approximate **distinct count** (HyperLogLog) over a fast field.
    Cardinality {
        /// The fast field.
        field: String,
    },
    /// Approximate **percentiles** of a numeric fast field. Backed by **DDSketch** (Tantivy
    /// 0.26.1) — a *relative-error* sketch (not t-digest) — so results are approximate; a
    /// cross-shard merge unions the sketches (still approximate, correctly merged).
    Percentiles {
        /// The numeric fast field.
        field: String,
        /// The percentiles to compute (e.g. `[50.0, 95.0, 99.0]`).
        percents: Vec<f64>,
    },
}

/// One `[from, to)` bucket of a [`Range`](Agg::Range) aggregation; either bound may be
/// open (`None`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggRange {
    /// Inclusive lower bound (open when `None`).
    pub from: Option<f64>,
    /// Exclusive upper bound (open when `None`).
    pub to: Option<f64>,
}

/// Largest `size` a [`Terms`](Agg::Terms) aggregation may request. Caps the bucket cardinality one
/// aggregation can force the store to build and merge across shards.
pub const MAX_TERMS_SIZE: usize = 10_000;

/// Smallest [`DateHistogram`](Agg::DateHistogram) interval accepted. A sub-second interval over a
/// wide event-time span would materialize an unbounded number of buckets; a one-second floor bounds
/// the bucket count to something a full time range can plausibly need.
pub const MIN_DATE_HISTOGRAM_INTERVAL_SECS: u64 = 1;

/// Largest number of aggregations one request may carry. Bounds the total per-request aggregation
/// work independent of any single agg's own cap.
pub const MAX_AGGS: usize = 8;

/// Parse a Tantivy `fixed_interval` duration string (e.g. `"1d"`, `"3600s"`, `"90m"`) to whole
/// seconds. Accepts a bare number (seconds) or a number with an `s`/`m`/`h`/`d` suffix. Returns
/// `None` for an unparseable or zero-length interval.
fn interval_secs(s: &str) -> Option<u64> {
    let s = s.trim();
    let (num, mult) = match s.as_bytes().last() {
        Some(b's') => (&s[..s.len() - 1], 1),
        Some(b'm') => (&s[..s.len() - 1], 60),
        Some(b'h') => (&s[..s.len() - 1], 3_600),
        Some(b'd') => (&s[..s.len() - 1], 86_400),
        _ => (s, 1),
    };
    num.trim().parse::<u64>().ok()?.checked_mul(mult)
}

/// Validate an aggregation spec before it reaches Tantivy, so bad input is a clear
/// `InvalidArgument` rather than an opaque internal error (or an unbounded amount of work).
/// Checks: the number of aggregations ([`MAX_AGGS`]); each `terms` `size` ([`MAX_TERMS_SIZE`]);
/// each `date_histogram` interval ([`MIN_DATE_HISTOGRAM_INTERVAL_SECS`]); and `range` buckets
/// (each `[from, to)` well-formed, the list ascending and non-overlapping). This is the single
/// boundary both the Node and Gateway `Aggregate` paths call, so the caps hold over direct gRPC too.
/// Returns a human-readable message naming the offending aggregation.
pub fn validate_aggs(aggs: &[(String, Agg)]) -> std::result::Result<(), String> {
    if aggs.len() > MAX_AGGS {
        return Err(format!(
            "too many aggregations ({}); the maximum is {MAX_AGGS}",
            aggs.len()
        ));
    }
    for (name, agg) in aggs {
        match agg {
            Agg::Terms { size, .. } => {
                if *size > MAX_TERMS_SIZE {
                    return Err(format!(
                        "terms agg `{name}`: size ({size}) exceeds the maximum ({MAX_TERMS_SIZE})"
                    ));
                }
            }
            Agg::DateHistogram { fixed_interval, .. } => match interval_secs(fixed_interval) {
                None => {
                    return Err(format!(
                        "date_histogram agg `{name}`: unparseable interval `{fixed_interval}`"
                    ));
                }
                Some(secs) if secs < MIN_DATE_HISTOGRAM_INTERVAL_SECS => {
                    return Err(format!(
                        "date_histogram agg `{name}`: interval `{fixed_interval}` is below the \
                         minimum ({MIN_DATE_HISTOGRAM_INTERVAL_SECS}s)"
                    ));
                }
                Some(_) => {}
            },
            Agg::Range { ranges, .. } => {
                let mut prev_upper = f64::NEG_INFINITY;
                for r in ranges {
                    let from = r.from.unwrap_or(f64::NEG_INFINITY);
                    let to = r.to.unwrap_or(f64::INFINITY);
                    // `partial_cmp` returns `None` for a NaN bound, so a non-`Less` result rejects
                    // both `from >= to` and NaN.
                    if from.partial_cmp(&to) != Some(std::cmp::Ordering::Less) {
                        return Err(format!(
                            "range agg `{name}`: each bucket needs from < to (got [{:?}, {:?}))",
                            r.from, r.to
                        ));
                    }
                    if from.partial_cmp(&prev_upper) == Some(std::cmp::Ordering::Less) {
                        return Err(format!(
                            "range agg `{name}`: buckets must be ascending and non-overlapping"
                        ));
                    }
                    prev_upper = to;
                }
            }
            Agg::Stats { .. } | Agg::Cardinality { .. } | Agg::Percentiles { .. } => {}
        }
    }
    Ok(())
}

/// Sort order for a [`Sort`] key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    /// Ascending (smallest first).
    Asc,
    /// Descending (largest first).
    Desc,
}

/// The reserved sort-key name for **relevance score**. Used as a [`Sort`]
/// `field` it orders by BM25 `_score` instead of a fast field — alone (`[_score desc]`,
/// the explicit form of the default) or among other keys (`rank desc, _score desc`).
/// It is not a real field, so it is exempt from fast-field validation; because a score
/// isn't a stable, range-able key, a `_score` sort is **offset-paged only** —
/// `search_after` keyset paging over it is rejected.
pub const SCORE_SORT_KEY: &str = "_score";

/// One key of a [sort](SearchParams::sort): a numeric/date fast field plus a
/// direction. Multiple keys sort lexicographically — earlier keys
/// dominate, later keys break ties. A trailing **composite-key** tiebreaker is
/// applied implicitly so the resulting order is **total and deterministic** (the
/// basis for stable paging / `search_after`).
///
/// The field may be the reserved [`SCORE_SORT_KEY`] (`_score`) to sort by relevance
/// rather than a fast field.
#[derive(Debug, Clone)]
pub struct Sort {
    /// The fast field to sort by, or [`SCORE_SORT_KEY`] for relevance score.
    pub field: String,
    /// Ascending or descending.
    pub order: SortOrder,
}

impl Sort {
    /// Whether this key sorts by relevance [`_score`](SCORE_SORT_KEY) rather than a field.
    pub fn is_score(&self) -> bool {
        self.field == SCORE_SORT_KEY
    }
}

/// Whether any key in `sort` is the reserved [`_score`](SCORE_SORT_KEY) key — the
/// shared predicate that gates keyset paging off (score isn't a stable keyset key).
pub fn sort_has_score(sort: &[Sort]) -> bool {
    sort.iter().any(Sort::is_score)
}

/// One sort key's value for a hit: the cell the store orders by and the
/// keyset cursor round-trips. Numeric/date keys are [`Num`](SortValue::Num) (DATE as
/// epoch micros); KEYWORD keys are [`Str`](SortValue::Str); a doc lacking the field is
/// [`Missing`](SortValue::Missing), which sorts **last**.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SortValue {
    /// The doc has no value for this sort field (sorts last in either direction).
    Missing,
    /// A numeric/date value (DATE normalized to epoch microseconds).
    Num(f64),
    /// A KEYWORD value, compared lexicographically.
    Str(String),
}

/// Compare two [`SortValue`]s for a sorted ranking: present values by `order`
/// (numerically or lexicographically by kind), and [`Missing`](SortValue::Missing)
/// always sorts **last** regardless of direction. The single comparator shared by the
/// store's cross-generation merge and the Gateway's cross-shard merge (design/09), so
/// the two orderings can't drift.
pub fn cmp_sort_value(a: &SortValue, b: &SortValue, order: SortOrder) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let dir = |c: Ordering| {
        if order == SortOrder::Desc {
            c.reverse()
        } else {
            c
        }
    };
    match (a, b) {
        (SortValue::Missing, SortValue::Missing) => Ordering::Equal,
        (SortValue::Missing, _) => Ordering::Greater, // missing sorts last
        (_, SortValue::Missing) => Ordering::Less,
        (SortValue::Num(x), SortValue::Num(y)) => dir(x.partial_cmp(y).unwrap_or(Ordering::Equal)),
        (SortValue::Str(x), SortValue::Str(y)) => dir(x.cmp(y)),
        // Mixed kinds don't occur for a single typed field; order deterministically.
        (SortValue::Num(_), SortValue::Str(_)) => Ordering::Less,
        (SortValue::Str(_), SortValue::Num(_)) => Ordering::Greater,
    }
}

/// A keyset **cursor** for [`search_after`](SearchParams::search_after): the sort-key
/// values and composite key of the **last hit of the previous page**. The next page
/// returns the hits strictly after this point in the [total order](Sort) — O(k) deep
/// paging with no `offset` scan. Requires a non-empty fast-field [`sort`](SearchParams::sort).
///
/// It is `Serialize`/`Deserialize` so a transport can hand the client an **opaque
/// token** for the cursor and round-trip it back, without exposing the field values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SearchAfter {
    /// The cursor hit's [`SortValue`] for each sort key, aligned to `sort`.
    pub sort_values: Vec<SortValue>,
    /// The cursor hit's composite key — the final, unique tiebreaker.
    pub key: CompositeKey,
}

/// One **collapsed group** ([field collapsing](SearchParams)): the top hit
/// of a group of docs sharing the same value of the collapse field, plus that group's
/// value and how many docs it holds.
#[derive(Debug, Clone, PartialEq)]
pub struct CollapsedHit {
    /// The best hit in the group (by the search's sort order).
    pub hit: Hit,
    /// The collapse field's value that defines the group.
    pub group: Value,
    /// Number of docs in this group (over the matched, live result set).
    pub count: usize,
    /// The top hit's [`SortValue`] for each sort key, aligned to the search sort. Carried so
    /// a Gateway can fold and order collapse groups **across shards** (design/09) —
    /// the same role `sort_values` plays for ordinary field-sorted hits.
    pub sort_values: Vec<SortValue>,
}

/// Search parameters: a parsed [`Query`] AST, a top-K limit, a (possibly empty)
/// list of fast-field [`Sort`] keys (empty = relevance score, descending), and
/// paging — either an `offset` (from/size) or a [`search_after`](Self::search_after)
/// keyset cursor.
#[derive(Debug, Clone)]
pub struct SearchParams {
    /// The query to execute.
    pub query: Query,
    /// Top-K limit (page size).
    pub k: usize,
    /// Sort keys, in priority order; empty = by relevance score, descending.
    pub sort: Vec<Sort>,
    /// Skip this many leading results (from/size paging). Ignored when
    /// [`search_after`](Self::search_after) is set.
    pub offset: usize,
    /// Keyset cursor: return the page strictly after this point in the total order.
    /// When set, `offset` is ignored and `sort` must be non-empty.
    pub search_after: Option<SearchAfter>,
    /// **Highlight** opt-in: when `Some`, each returned [`Hit`] carries per-field
    /// [`highlight`](Hit::highlight) fragments of the analyzed match. `None` = no highlights
    /// (the default; highlighting is an extra per-hit cost).
    pub highlight: Option<Highlight>,
}

impl SearchParams {
    /// Search with a pre-built query AST, top-`k` (by score, no offset).
    pub fn new(query: Query, k: usize) -> Self {
        Self {
            query,
            k,
            sort: Vec::new(),
            offset: 0,
            search_after: None,
            highlight: None,
        }
    }

    /// Opt into [server-side highlighting](Highlight) for this search.
    pub fn with_highlight(mut self, highlight: Highlight) -> Self {
        self.highlight = Some(highlight);
        self
    }

    /// Parse `query` (Lucene/KQL grammar) and search top-`k` (by score, no offset).
    pub fn parse(query: &str, k: usize) -> Result<Self, crate::query::ParseError> {
        Ok(Self::new(Query::parse(query)?, k))
    }

    /// Set the **primary** sort key to `field` (a fast field) in `order`, replacing
    /// any existing keys. Chain [`then_sort`](Self::then_sort) for additional keys.
    pub fn with_sort(mut self, field: impl Into<String>, order: SortOrder) -> Self {
        self.sort = vec![Sort {
            field: field.into(),
            order,
        }];
        self
    }

    /// Append a lower-priority sort key (a tiebreaker for the keys before it).
    pub fn then_sort(mut self, field: impl Into<String>, order: SortOrder) -> Self {
        self.sort.push(Sort {
            field: field.into(),
            order,
        });
        self
    }

    /// Skip the first `offset` results (from/size paging).
    pub fn with_offset(mut self, offset: usize) -> Self {
        self.offset = offset;
        self
    }

    /// Page from a keyset `cursor` (the last hit of the previous page) instead of an
    /// `offset` — O(k) deep paging. Requires a non-empty [`sort`](Self::sort).
    pub fn after(mut self, cursor: SearchAfter) -> Self {
        self.search_after = Some(cursor);
        self
    }
}

/// Per-shard search results: ranked hits + a total count.
#[derive(Debug, Clone, Default)]
pub struct ShardHits {
    /// The ranked hits (this page).
    pub hits: Vec<Hit>,
    /// Total documents matching the query (the live match count), NOT the page size — so it
    /// can exceed `hits.len()` and be summed across shards.
    pub total: usize,
}

/// Write path ([Design 02]): **stage** a batch into a segment (not yet visible),
/// then **commit** staged work atomically (publish + advance checkpoint). Called
/// by ingest.
///
/// [Design 02]: ../../../design/02-index-api.md
pub trait IndexWriter {
    /// Implementation error type.
    type Error;
    /// Opaque handle to staged-but-uncommitted work.
    type Staged;

    /// Analyze + build segment(s) from `batch`; not visible until [`commit`](Self::commit).
    fn stage(&self, batch: &CommitBatch) -> Result<Self::Staged, Self::Error>;

    /// Atomically publish staged work AND advance the checkpoint AND update the
    /// PK locator — all-or-nothing. Returns the new snapshot. Idempotent: staged
    /// work for an already-applied batch is a no-op.
    fn commit(&self, staged: &[Self::Staged]) -> Result<Snapshot, Self::Error>;

    /// Convenience: stage + commit one batch.
    fn write(&self, batch: &CommitBatch) -> Result<Snapshot, Self::Error> {
        let staged = self.stage(batch)?;
        self.commit(std::slice::from_ref(&staged))
    }
}

/// Read path: lexical search + key→row locator resolution. Called by the engine.
pub trait IndexReader {
    /// Implementation error type.
    type Error;

    /// Lexical search; returns **coordinates + score** at the current snapshot.
    fn search(&self, params: &SearchParams) -> Result<ShardHits, Self::Error>;

    /// Resolve keys to their source-row [locators](RowLocator) (the hydration
    /// bridge). One result per input key, `None` if absent.
    fn get_by_key(&self, keys: &[CompositeKey]) -> Result<Vec<Option<RowLocator>>, Self::Error>;

    /// The snapshot this reader observes — pins a consistent read view.
    fn snapshot(&self) -> Snapshot;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn range(from: Option<f64>, to: Option<f64>) -> AggRange {
        AggRange { from, to }
    }

    fn range_agg(ranges: Vec<AggRange>) -> Vec<(String, Agg)> {
        vec![(
            "r".to_string(),
            Agg::Range {
                field: "amount".into(),
                ranges,
            },
        )]
    }

    #[test]
    fn validate_aggs_accepts_sorted_non_overlapping_ranges() {
        // Open-ended ends, contiguous middle — the well-formed shape Tantivy wants.
        let aggs = range_agg(vec![
            range(None, Some(10.0)),
            range(Some(10.0), Some(20.0)),
            range(Some(20.0), None),
        ]);
        assert!(validate_aggs(&aggs).is_ok());
        // A non-range agg is always fine.
        assert!(validate_aggs(&[(
            "t".into(),
            Agg::Terms {
                field: "cat".into(),
                size: 5
            }
        )])
        .is_ok());
    }

    #[test]
    fn validate_aggs_rejects_overlapping_unsorted_and_inverted_ranges() {
        // Overlap: [0,15) then [10,20).
        assert!(validate_aggs(&range_agg(vec![
            range(Some(0.0), Some(15.0)),
            range(Some(10.0), Some(20.0)),
        ]))
        .is_err());
        // Out of order: [10,20) then [0,5).
        assert!(validate_aggs(&range_agg(vec![
            range(Some(10.0), Some(20.0)),
            range(Some(0.0), Some(5.0)),
        ]))
        .is_err());
        // Inverted bounds: from >= to.
        assert!(validate_aggs(&range_agg(vec![range(Some(20.0), Some(10.0))])).is_err());
        // NaN bound.
        assert!(validate_aggs(&range_agg(vec![range(Some(f64::NAN), Some(10.0))])).is_err());
    }

    #[test]
    fn validate_aggs_caps_terms_size() {
        let ok = vec![(
            "t".to_string(),
            Agg::Terms {
                field: "cat".into(),
                size: MAX_TERMS_SIZE,
            },
        )];
        assert!(validate_aggs(&ok).is_ok());
        let over = vec![(
            "t".to_string(),
            Agg::Terms {
                field: "cat".into(),
                size: MAX_TERMS_SIZE + 1,
            },
        )];
        assert!(validate_aggs(&over).is_err());
    }

    #[test]
    fn validate_aggs_rejects_subsecond_date_histogram() {
        let mk = |iv: &str| {
            vec![(
                "d".to_string(),
                Agg::DateHistogram {
                    field: "ts".into(),
                    fixed_interval: iv.into(),
                },
            )]
        };
        assert!(validate_aggs(&mk("1d")).is_ok());
        assert!(validate_aggs(&mk("3600s")).is_ok());
        // Tantivy accepts a bare-number (seconds) or suffixed interval; a sub-second one is rejected.
        assert!(validate_aggs(&mk("0")).is_err());
        assert!(validate_aggs(&mk("nonsense")).is_err());
    }

    #[test]
    fn validate_aggs_caps_agg_count() {
        let many: Vec<(String, Agg)> = (0..=MAX_AGGS)
            .map(|i| {
                (
                    format!("s{i}"),
                    Agg::Stats {
                        field: "amount".into(),
                    },
                )
            })
            .collect();
        assert!(many.len() > MAX_AGGS);
        assert!(validate_aggs(&many).is_err());
    }

    #[test]
    fn highlight_new_clamps_bounds() {
        let h = Highlight::new(Vec::new(), MAX_HIGHLIGHT_MAX_FRAGMENTS + 100, 0);
        assert_eq!(h.max_fragments, MAX_HIGHLIGHT_MAX_FRAGMENTS);
        // A zero still means "use the default".
        assert_eq!(h.fragment_size, DEFAULT_HIGHLIGHT_FRAGMENT_SIZE);

        let h = Highlight::new(Vec::new(), 0, MAX_HIGHLIGHT_FRAGMENT_SIZE + 100);
        assert_eq!(h.max_fragments, DEFAULT_HIGHLIGHT_MAX_FRAGMENTS);
        assert_eq!(h.fragment_size, MAX_HIGHLIGHT_FRAGMENT_SIZE);
    }
}
