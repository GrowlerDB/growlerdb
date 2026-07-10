//! Segment build over [Tantivy] — the `SegmentCore` seam ([wiki 05]).
//!
//! [`TantivySegmentCore`] turns a [`DocBatch`] into an **immutable on-disk
//! segment set** in a local directory, and reopens it for BM25 search. Keeping
//! the core behind a small surface (build / open) leaves a future Lucene backend
//! possible, exactly as the design intends.
//!
//! M0 scope: TEXT fields are analyzed (Tantivy's default tokenizer — simple
//! tokenizer + lowercasing, i.e. "standard + lowercase"), KEYWORD fields are
//! indexed raw, and the [`CompositeKey`] is stored per document (JSON) so every
//! hit carries the coordinates the engine hydrates from Iceberg (task-8).
//!
//! [Tantivy]: https://github.com/quickwit-oss/tantivy
//! [wiki 05]: ../../../wiki/05-search-core.md

use std::net::{IpAddr, Ipv6Addr};
use std::ops::Bound;
use std::path::Path;

use growlerdb_core::{
    sort_has_score, CompositeKey, DocBatch, Document, FieldType, Hit, LocationStrategy, MatchOp,
    Query, ResolvedField, ResolvedIndex, SearchAfter, Sort, SortOrder, SortValue, TextRecord,
    TimeFormat, Value as GValue, SCORE_SORT_KEY,
};
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::{AggContextParams, DistributedAggregationCollector};
use tantivy::collector::{Count, DocSetCollector, TopDocs};
use tantivy::query::{
    AllQuery, BooleanQuery, BoostQuery, ConstScoreQuery, EmptyQuery, ExistsQuery, FuzzyTermQuery,
    Occur, PhraseQuery, Query as TantivyQuery, RangeQuery, RegexQuery, TermQuery, TermSetQuery,
};
use tantivy::schema::{
    DateOptions, Field, FieldType as TvFieldType, IndexRecordOption, IpAddrOptions, NumericOptions,
    Schema, TextFieldIndexing, TextOptions, Value, FAST, INDEXED, STORED, STRING,
};
use tantivy::{
    DateTime, DocSet, Index, IndexReader, ReloadPolicy, TantivyDocument, Term, TERMINATED,
};

/// Name of the stored field holding a doc's `enc(CompositeKey)` bytes — hit identity,
/// rebuilt via [`CompositeKey::decode`] (task-212). Same encoding as [`KEY_ENC_FIELD`],
/// computed once per doc.
pub const KEY_FIELD: &str = "_key";
/// Name of the **bytes-indexed** field holding `enc(CompositeKey)` — lets a reader
/// exclude a generation's tombstoned docs by key for liveness-correct aggregations
/// (task-24/64). Not stored (the stored `_key` carries the same bytes for hits).
const KEY_ENC_FIELD: &str = "_keyenc";
/// Name of the u64 **fast field** holding a doc's internal **locator ID** — the
/// immutable *reference* layer of the [D30] layered locator (task-184). The id indexes
/// the shard's dense location array ([`crate::location`]), which maps it to the row's
/// current `(file_id, row_position)`. Written on every upsert — the layered locator is
/// the only shard layout.
///
/// [D30]: ../../../okf/system/decisions/d30-layered-locator.md
pub const LOC_ID_FIELD: &str = "_locid";

/// Writer heap budget. Small for M0 batches; tuned later (task-33 compaction).
pub const WRITER_HEAP_BYTES: usize = 50_000_000;

/// Errors from building or reading a segment.
#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    /// An error from Tantivy (build, commit, open, search).
    #[error(transparent)]
    Tantivy(#[from] tantivy::TantivyError),

    /// The query string could not be parsed.
    #[error("query parse: {0}")]
    Query(#[from] tantivy::query::QueryParserError),

    /// A stored `enc(key)` failed to decode (corrupt bytes or a future format).
    #[error("stored key decode: {0}")]
    KeyDecode(#[from] growlerdb_core::KeyDecodeError),

    /// A hit was missing its stored composite key (corrupt segment).
    #[error("hit is missing its `{KEY_FIELD}` stored field")]
    MissingKey,

    /// The index defines no analyzed TEXT field to run a default search against.
    #[error("index schema has no TEXT field to search")]
    NoDefaultField,

    /// The query referenced a field that is not present/searchable in the index.
    #[error("unknown or non-searchable field: `{0}`")]
    UnknownField(String),

    /// A query operator was applied to an incompatible field type.
    #[error("query type error: {0}")]
    QueryType(String),

    /// A query was rejected by a cost guard (leading wildcard, broad regex, …).
    #[error("query rejected (cost guard): {0}")]
    CostGuard(String),
}

/// Convenience result alias for the index crate.
pub type Result<T> = std::result::Result<T, IndexError>;

/// One scanned doc for a [field collapse](SegmentReader::collapse_scan): its hit, the
/// collapse field's group value, and its sort-key values (for ordering in the store).
pub type CollapseEntry = (Hit, GValue, Vec<SortValue>);

/// A Tantivy schema derived from a [`ResolvedIndex`], plus the field handles
/// needed to build documents: the stored key field and each mapped field.
pub struct IndexSchema {
    schema: Schema,
    key_field: Field,
    /// Bytes-indexed `enc(key)` field for the aggregation liveness exclusion.
    key_enc_field: Field,
    /// u64 FAST locator-ID field ([`LOC_ID_FIELD`], the D30 reference layer), attached
    /// to every upsert by the store's commit path.
    loc_id_field: Field,
    /// (path, tantivy field, type, declared timestamp format) for each mapped field, in definition
    /// order. The optional [`TimeFormat`] (task-112) is set only for fields declared as timestamps;
    /// it tells [`add_typed_value`] to normalize the source epoch to canonical micros at build.
    fields: Vec<(String, Field, FieldType, Option<TimeFormat>)>,
    /// The tenant-scoping field (task-38), if the index is tenant-scoped.
    tenant_field: Option<String>,
    /// The index's **location strategy** (D30, task-184). Under
    /// [`Predicate`](LocationStrategy::Predicate) the store's commit path never
    /// populates [`LOC_ID_FIELD`] and writes no location slots — the schema **keeps**
    /// the field either way (see [`from_resolved`](Self::from_resolved)).
    location_strategy: LocationStrategy,
}

impl IndexSchema {
    /// Derive a Tantivy schema from a resolved index definition.
    ///
    /// TEXT → analyzed full-text; KEYWORD → raw; LONG/DOUBLE/BOOL/DATE/IP → typed,
    /// indexed (range-queryable) columns. The `fast` flag adds a columnar fast field
    /// (sort/filter/aggregate, task-23/24); `cached` stores the value for return with
    /// the hit (D23). The composite key is added as a `STORED`-only bytes field — the
    /// compact `enc(key)` (task-212).
    pub fn from_resolved(idx: &ResolvedIndex) -> Self {
        let mut builder = Schema::builder();
        let key_field = builder.add_bytes_field(KEY_FIELD, STORED);
        let key_enc_field = builder.add_bytes_field(KEY_ENC_FIELD, INDEXED);
        let mut fields = Vec::with_capacity(idx.fields.len());
        for f in &idx.fields {
            let handle = match f.ty {
                FieldType::Text => {
                    // Per-field indexing detail (task-216): record level (positions are the
                    // phrase-query slice, usually the largest part of a text field's inverted
                    // index) and fieldnorms (BM25 length normalization, ~1 byte/doc).
                    let indexing = TextFieldIndexing::default()
                        .set_tokenizer("default")
                        .set_index_option(record_option(f.record))
                        .set_fieldnorms(f.fieldnorms);
                    let mut opts = TextOptions::default().set_indexing_options(indexing);
                    if f.cached {
                        opts = opts.set_stored();
                    }
                    builder.add_text_field(&f.path, opts)
                }
                FieldType::Keyword => {
                    let mut opts = STRING;
                    if f.cached {
                        opts = opts | STORED;
                    }
                    if f.fast {
                        opts = opts | FAST;
                    }
                    builder.add_text_field(&f.path, opts)
                }
                FieldType::Long => builder.add_i64_field(&f.path, num_opts(f)),
                FieldType::Double => builder.add_f64_field(&f.path, num_opts(f)),
                FieldType::Bool => builder.add_bool_field(&f.path, num_opts(f)),
                FieldType::Date => builder.add_date_field(&f.path, date_opts(f)),
                FieldType::Ip => builder.add_ip_addr_field(&f.path, ip_opts(f)),
            };
            fields.push((f.path.clone(), handle, f.ty, f.format));
        }
        // The D30 locator-ID fast field (task-184), added after the mapped fields so
        // the internal handles never shift a user field's ordinal. It is declared for
        // **every** strategy — a `PREDICATE` index just never populates it (a missing
        // u64 fast value costs ~nothing bitpacked). Keeping the schema identical
        // across strategies avoids the slice-2 field-ordinal hazard: segments, backup,
        // reindex, and cold-open tooling see one schema shape, and a strategy never
        // shifts another field's ordinal.
        let loc_id_field = builder.add_u64_field(LOC_ID_FIELD, FAST);
        Self {
            schema: builder.build(),
            key_field,
            key_enc_field,
            loc_id_field,
            fields,
            tenant_field: idx.tenant_field().map(str::to_string),
            location_strategy: idx.location_strategy,
        }
    }

    /// The index's [location strategy](LocationStrategy) (D30, task-184) — how the
    /// store's commit path and the engine's hydration path locate source rows.
    pub fn location_strategy(&self) -> LocationStrategy {
        self.location_strategy
    }

    /// The tenant-scoping field (task-38), if this index is tenant-scoped — the field reads
    /// inject a mandatory `= <verified claim>` filter on.
    pub fn tenant_field(&self) -> Option<&str> {
        self.tenant_field.as_deref()
    }

    /// The mapped **DATE** fields, in definition order (task-101). These are the columns a console
    /// time filter can range-scope a query on; when one is also the windowing field, the gateway
    /// prunes non-overlapping windows (task-81). Stored as canonical **epoch microseconds**
    /// (task-112) — the unit Tantivy `DateTime::from_timestamp_micros` and the range/sort path use.
    pub fn date_fields(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|(_, _, ty, _)| *ty == FieldType::Date)
            .map(|(path, _, _, _)| path.as_str())
            .collect()
    }

    /// The bytes-indexed `enc(key)` field — used to **delete by key** (the Tantivy-native
    /// supersede/delete under the single-index model) and for the agg keyset exclusion.
    pub fn key_enc_field(&self) -> Field {
        self.key_enc_field
    }

    /// The u64 FAST **locator-ID** field ([`LOC_ID_FIELD`], D30 reference layer) — the
    /// store attaches each upsert's location-array id through this handle.
    pub fn loc_id_field(&self) -> Field {
        self.loc_id_field
    }

    /// Build the [`TantivyDocument`] for `doc`: the stored + indexed `enc(key)` (one
    /// encoding, computed once — hit identity and the delete term, task-212), and each
    /// mapped field's typed value (skipping absent fields).
    pub fn to_tantivy(&self, doc: &Document) -> TantivyDocument {
        let mut td = TantivyDocument::new();
        let enc = doc.key.encode();
        td.add_bytes(self.key_field, enc.as_slice());
        td.add_bytes(self.key_enc_field, enc.as_slice());
        for (path, field, ty, fmt) in &self.fields {
            if let Some(value) = doc.fields.get(path) {
                add_typed_value(&mut td, *field, *ty, *fmt, path, value);
            }
        }
        td
    }

    /// The underlying Tantivy schema.
    pub fn tantivy_schema(&self) -> &Schema {
        &self.schema
    }
}

/// The Tantivy [`IndexRecordOption`] for a TEXT field's [`TextRecord`] level (task-216).
fn record_option(record: TextRecord) -> IndexRecordOption {
    match record {
        TextRecord::Basic => IndexRecordOption::Basic,
        TextRecord::Freq => IndexRecordOption::WithFreqs,
        TextRecord::Position => IndexRecordOption::WithFreqsAndPositions,
    }
}

/// `NumericOptions` for a LONG/DOUBLE/BOOL field per the field's `indexed`/`fast`/`cached` flags.
/// A **fast-only** field (task-215, the default when `fast: true`) carries no inverted index —
/// range, exact-match (routed through Range), sort/search-after, and exists all run on the
/// columnar store (Tantivy's `RangeQuery` takes the fast path whenever the field is fast, and
/// `ExistsQuery` only ever reads fast fields), so the postings + term dict would be dead weight.
fn num_opts(f: &ResolvedField) -> NumericOptions {
    let mut o = NumericOptions::default();
    if f.indexed {
        o = o.set_indexed();
    }
    if f.fast {
        o = o.set_fast();
    }
    if f.cached {
        o = o.set_stored();
    }
    o
}

/// `DateOptions` for a DATE field (`indexed`/`fast`/`cached` — see [`num_opts`] on fast-only).
fn date_opts(f: &ResolvedField) -> DateOptions {
    let mut o = DateOptions::default();
    if f.indexed {
        o = o.set_indexed();
    }
    if f.fast {
        o = o.set_fast();
    }
    if f.cached {
        o = o.set_stored();
    }
    o
}

/// `IpAddrOptions` for an IP field (CIDR/range via inverted **or** fast — see [`num_opts`]).
fn ip_opts(f: &ResolvedField) -> IpAddrOptions {
    let mut o = IpAddrOptions::default();
    if f.indexed {
        o = o.set_indexed();
    }
    if f.fast {
        o = o.set_fast();
    }
    if f.cached {
        o = o.set_stored();
    }
    o
}

/// Add a wire [`Value`](growlerdb_core::Value) to the document as the field's typed
/// Tantivy value. A value whose kind doesn't match the field type is **skipped** (the
/// document still indexes its other fields) rather than failing the whole batch —
/// source-type validation is a richer concern handled at resolve time.
fn add_typed_value(
    td: &mut TantivyDocument,
    field: Field,
    ty: FieldType,
    format: Option<TimeFormat>,
    path: &str,
    value: &growlerdb_core::Value,
) {
    use growlerdb_core::Value as V;
    match ty {
        FieldType::Text | FieldType::Keyword => td.add_text(field, value.to_index_string()),
        FieldType::Long => {
            if let V::Int(i) = value {
                td.add_i64(field, *i);
            }
        }
        FieldType::Double => match value {
            V::Float(x) => td.add_f64(field, *x),
            V::Int(i) => td.add_f64(field, *i as f64),
            _ => {}
        },
        FieldType::Bool => {
            if let V::Bool(b) = value {
                td.add_bool(field, *b);
            }
        }
        // Dates are stored as canonical epoch **microseconds** (task-112). A field declared with a
        // `format` carries its source value in some other epoch unit (the demo's `ts` is millis), so
        // normalize it here; an unparseable value is **skipped** (the doc still indexes its other
        // fields) rather than wedging the batch or writing an off-by-10³ date. A field with no
        // declared format already arrives as canonical micros — a native source date/timestamp
        // extracts to `Ts` (task-184), and a pre-parsed epoch column may still arrive as `Int`
        // (`TimeFormat::to_micros` likewise passes a `Ts` through untouched).
        FieldType::Date => match format {
            Some(fmt) => {
                if let Ok(micros) = fmt.to_micros(path, value) {
                    td.add_date(field, DateTime::from_timestamp_micros(micros));
                }
            }
            None => {
                if let V::Int(micros) | V::Ts(micros) = value {
                    td.add_date(field, DateTime::from_timestamp_micros(*micros));
                }
            }
        },
        // IPs arrive as strings; Tantivy stores them as IPv6 (v4 mapped).
        FieldType::Ip => {
            if let V::Str(s) = value {
                if let Ok(ip) = s.parse::<IpAddr>() {
                    td.add_ip_addr(field, to_ipv6(ip));
                }
            }
        }
    }
}

/// Normalize an `IpAddr` to the IPv6 form Tantivy stores (IPv4 → v4-mapped v6).
fn to_ipv6(ip: IpAddr) -> Ipv6Addr {
    match ip {
        IpAddr::V4(v4) => v4.to_ipv6_mapped(),
        IpAddr::V6(v6) => v6,
    }
}

/// Lowercase `value` for an analyzed TEXT field (matching index-time analysis);
/// pass keyword values through unchanged.
fn fold(value: &str, is_text: bool) -> String {
    if is_text {
        value.to_lowercase()
    } else {
        value.to_string()
    }
}

/// Escape regex metacharacters in a literal so it matches verbatim.
fn regex_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if r".^$*+?()[]{}|\".contains(c) {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

/// Translate a glob (`*` = any run, `?` = any single char) to a regex, escaping all
/// other metacharacters. Tantivy anchors the pattern to the whole term.
fn glob_to_regex(glob: &str) -> String {
    let mut out = String::with_capacity(glob.len() + 4);
    for c in glob.chars() {
        match c {
            '*' => out.push_str(".*"),
            '?' => out.push('.'),
            c if r".^$+()[]{}|\".contains(c) => {
                out.push('\\');
                out.push(c);
            }
            c => out.push(c),
        }
    }
    out
}

/// Reject regexes that would scan the whole term dictionary (a leading `.*`/`.+`).
fn guard_regex(pattern: &str) -> Result<()> {
    if pattern.is_empty() {
        return Err(IndexError::CostGuard("empty regex".into()));
    }
    if pattern.starts_with(".*") || pattern.starts_with(".+") || pattern.starts_with(".?") {
        return Err(IndexError::CostGuard(
            "leading `.*`/`.+` scans every term".into(),
        ));
    }
    Ok(())
}

/// The Tantivy-backed implementation of the segment core seam.
#[derive(Debug, Default, Clone, Copy)]
pub struct TantivySegmentCore;

impl TantivySegmentCore {
    /// The settings a **new** index is created with (task-212): zstd doc-store compression.
    /// lz4 (the default) only match-copies, so high-entropy stored values — hex/UUID hit keys,
    /// random-ish cached fields — pass through nearly uncompressed; zstd entropy-codes them
    /// (~2x on hex). Per-index: the compressor persists in `meta.json`, so an existing index
    /// keeps whatever it was created with and its segments stay readable.
    fn new_index_settings() -> tantivy::IndexSettings {
        tantivy::IndexSettings {
            docstore_compression: tantivy::store::Compressor::Zstd(Default::default()),
            ..Default::default()
        }
    }

    /// Build an immutable segment set from `batch` into the (empty) directory
    /// `dir`, returning the number of documents written.
    pub fn build(&self, schema: &IndexSchema, batch: &DocBatch, dir: &Path) -> Result<u64> {
        let index = Index::builder()
            .schema(schema.schema.clone())
            .settings(Self::new_index_settings())
            .create_in_dir(dir)?;
        let mut writer: tantivy::IndexWriter = index.writer(WRITER_HEAP_BYTES)?;

        for doc in &batch.docs {
            writer.add_document(schema.to_tantivy(doc))?;
        }

        writer.commit()?;
        Ok(batch.docs.len() as u64)
    }

    /// Reopen a previously built segment set for reading.
    pub fn open(&self, dir: &Path) -> Result<SegmentReader> {
        let index = Index::open_in_dir(dir)?;
        let reader = index.reader()?;
        Ok(SegmentReader { index, reader })
    }

    /// Open the shard's **single** Tantivy index at `dir`, creating it empty if absent.
    /// All commits add segments to this one index (the single-index model); compaction
    /// is `IndexWriter::merge` over its segments (task-33). An existing index opens with
    /// the settings persisted in its `meta.json` (its original doc-store compressor);
    /// only a fresh create gets [`new_index_settings`](Self::new_index_settings).
    pub fn open_or_create_index(&self, schema: &IndexSchema, dir: &Path) -> Result<Index> {
        if dir.join("meta.json").exists() {
            Ok(Index::open_in_dir(dir)?)
        } else {
            std::fs::create_dir_all(dir).map_err(|e| IndexError::Tantivy(e.into()))?;
            Ok(Index::builder()
                .schema(schema.schema.clone())
                .settings(Self::new_index_settings())
                .create_in_dir(dir)?)
        }
    }
}

/// Rebuild a hit's [`CompositeKey`] from its stored `_key` bytes (task-212) — the
/// strict inverse of the `enc(key)` written at index time.
fn stored_key(doc: &TantivyDocument, key_field: Field) -> Result<CompositeKey> {
    let bytes = doc
        .get_first(key_field)
        .and_then(|v| v.as_bytes())
        .ok_or(IndexError::MissingKey)?;
    Ok(CompositeKey::decode(bytes)?)
}

/// The result of explaining one document's score for a query (task-102): Tantivy's BM25
/// score-explanation tree plus the post-analyzer tokens the query searched for.
#[derive(Debug, Clone)]
pub struct ExplainHit {
    /// The key resolved to a document in the index.
    pub found: bool,
    /// The query matches that document (false ⇒ score 0, no detail).
    pub matched: bool,
    /// Total BM25 score.
    pub score: f32,
    /// Tantivy's `Explanation` as JSON (`{value, description, details}`); null when unmatched.
    pub detail: serde_json::Value,
    /// Post-analyzer tokens the query searched for, as `(field, tokens)`.
    pub analyzed: Vec<(String, Vec<String>)>,
}

/// A read handle over a built segment set: document counts and BM25 search.
pub struct SegmentReader {
    index: Index,
    reader: IndexReader,
}

impl SegmentReader {
    /// A read handle over `index` that **auto-reloads on commit** — the shard's live
    /// reader; reads see each commit's new segment (and its native deletes).
    pub fn live(index: &Index) -> Result<Self> {
        Ok(SegmentReader {
            index: index.clone(),
            reader: index.reader()?,
        })
    }

    /// A read handle **pinned** to `index`'s current commit — never reloads, so its
    /// searcher is a stable snapshot and Tantivy keeps the referenced segment files
    /// alive even as later commits/compaction run. This is a point-in-time pin (task-65).
    pub fn snapshot(index: &Index) -> Result<Self> {
        Ok(SegmentReader {
            index: index.clone(),
            reader: index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?,
        })
    }

    /// Force the live reader to observe the latest commit (after a write).
    pub fn reload(&self) -> Result<()> {
        self.reader.reload()?;
        Ok(())
    }

    /// Total live documents (Tantivy excludes deleted/superseded docs).
    pub fn num_docs(&self) -> u64 {
        self.reader.searcher().num_docs()
    }

    /// The **locator ID** (`_locid` fast field) of the live doc carrying `enc(key)`,
    /// or `None` when no live doc has the key. This is the D30 write path's pre-commit
    /// **reuse lookup** (task-184): a key-term probe of each segment's dictionary + one
    /// fast-field read, ~1 µs warm per key (spike-measured, ≈1–2% of ingest CPU at bulk
    /// rates), which keeps the location array O(live keys) instead of O(all versions
    /// ever written).
    pub fn live_loc_id(&self, key_enc: &[u8]) -> Result<Option<u64>> {
        let schema = self.index.schema();
        let Ok(key_enc_field) = schema.get_field(KEY_ENC_FIELD) else {
            return Ok(None);
        };
        let term = Term::from_field_bytes(key_enc_field, key_enc);
        let searcher = self.reader.searcher();
        for segment in searcher.segment_readers() {
            let inverted = segment.inverted_index(key_enc_field)?;
            let Some(mut postings) = inverted
                .read_postings(&term, IndexRecordOption::Basic)
                .map_err(|e| IndexError::Tantivy(e.into()))?
            else {
                continue;
            };
            // Defensive: a segment with no `_locid` column can't contribute an id
            // (should be unreachable — every upsert writes the field).
            let Some(col) = segment.fast_fields().column_opt::<u64>(LOC_ID_FIELD)? else {
                continue;
            };
            let alive = segment.alive_bitset();
            let mut doc = postings.doc();
            while doc != TERMINATED {
                if alive.is_none_or(|b| b.is_alive(doc)) {
                    if let Some(id) = col.first(doc) {
                        return Ok(Some(id));
                    }
                }
                doc = postings.advance();
            }
        }
        Ok(None)
    }

    /// Whether any **live** doc carries `enc(key)` — a postings probe filtered by each
    /// segment's alive bitset. The presence half of a drift check: unlike a raw term
    /// lookup this never counts a deleted-but-unmerged doc (the store runs
    /// `NoMergePolicy`, so term dictionaries retain superseded/deleted keys until
    /// compaction).
    pub fn live_key_exists(&self, key_enc: &[u8]) -> Result<bool> {
        let key_enc_field = self.index.schema().get_field(KEY_ENC_FIELD)?;
        let term = Term::from_field_bytes(key_enc_field, key_enc);
        let searcher = self.reader.searcher();
        for segment in searcher.segment_readers() {
            let inverted = segment.inverted_index(key_enc_field)?;
            let Some(mut postings) = inverted
                .read_postings(&term, IndexRecordOption::Basic)
                .map_err(|e| IndexError::Tantivy(e.into()))?
            else {
                continue;
            };
            let alive = segment.alive_bitset();
            let mut doc = postings.doc();
            while doc != TERMINATED {
                if alive.is_none_or(|b| b.is_alive(doc)) {
                    return Ok(true);
                }
                doc = postings.advance();
            }
        }
        Ok(false)
    }

    /// Enumerate the **live-key set** under a raw-bytes `prefix` of the `_keyenc` term
    /// dictionary — the D30 replacement for the deleted keyed locator table's key
    /// range. `enc(CompositeKey)` is partition-first and length-prefixed, so a
    /// partition's encoded keys form one contiguous byte-prefix range: streaming the
    /// dictionary from `prefix` and stopping at the first non-matching term preserves
    /// partition scoping **exactly** (an empty prefix enumerates the whole shard).
    ///
    /// Per term, the key is counted only if it has a **live** doc (postings walk +
    /// alive bitset): under `NoMergePolicy` the dictionary retains
    /// deleted-but-unmerged keys, so raw term enumeration would over-report. A key's
    /// term can appear in several segments (superseded versions); the result set is
    /// deduplicated, and a key counts once however many segments name it.
    ///
    /// Cost: O(terms in range) dictionary streaming + one postings probe per candidate
    /// term, and O(live keys in range) memory for the returned set — the same order as
    /// the redb prefix range it replaces, minus the second copy of every key on disk.
    pub fn live_keys_with_prefix(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        let key_enc_field = self.index.schema().get_field(KEY_ENC_FIELD)?;
        let searcher = self.reader.searcher();
        let mut live: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for segment in searcher.segment_readers() {
            let inverted = segment.inverted_index(key_enc_field)?;
            let terms = inverted.terms();
            let mut stream = terms
                .range()
                .ge(prefix)
                .into_stream()
                .map_err(|e| IndexError::Tantivy(e.into()))?;
            let alive = segment.alive_bitset();
            while stream.advance() {
                let key = stream.key();
                if !key.starts_with(prefix) {
                    break; // sorted dictionary — past the contiguous prefix range
                }
                if live.contains(key) {
                    continue; // already proven live in another segment
                }
                let mut postings = inverted
                    .read_postings_from_terminfo(stream.value(), IndexRecordOption::Basic)
                    .map_err(|e| IndexError::Tantivy(e.into()))?;
                let mut doc = postings.doc();
                while doc != TERMINATED {
                    if alive.is_none_or(|b| b.is_alive(doc)) {
                        live.insert(key.to_vec());
                        break;
                    }
                    doc = postings.advance();
                }
            }
        }
        Ok(live.into_iter().collect())
    }

    /// Read the **cached** (stored) display fields of `doc` into a value map (D23,
    /// task-26) — every stored field except the internal key, typed back to a wire
    /// [`Value`](growlerdb_core::Value). These ride along on each [`Hit`] so a page
    /// renders without hydration.
    fn cached_fields(&self, doc: &TantivyDocument) -> std::collections::BTreeMap<String, GValue> {
        let schema = self.index.schema();
        let mut out = std::collections::BTreeMap::new();
        for (field, entry) in schema.fields() {
            if entry.name() == KEY_FIELD || !entry.is_stored() {
                continue;
            }
            let Some(v) = doc.get_first(field) else {
                continue;
            };
            let value = match entry.field_type() {
                TvFieldType::Str(_) => v.as_str().map(|s| GValue::Str(s.to_string())),
                TvFieldType::I64(_) => v.as_i64().map(GValue::Int),
                TvFieldType::F64(_) => v.as_f64().map(GValue::Float),
                TvFieldType::Bool(_) => v.as_bool().map(GValue::Bool),
                TvFieldType::Date(_) => v
                    .as_datetime()
                    .map(|d| GValue::Int(d.into_timestamp_micros())),
                TvFieldType::IpAddr(_) => v.as_ip_addr().map(|ip| GValue::Str(ip.to_string())),
                _ => None,
            };
            if let Some(value) = value {
                out.insert(entry.name().to_string(), value);
            }
        }
        out
    }

    /// Run `aggs` over the docs matching `query` and return the **intermediate** results for the
    /// store to merge/finalize (task-24). Under the single-index-per-shard model, Tantivy's own
    /// delete handling already excludes superseded/deleted docs, so there is no tombstone exclusion
    /// to apply here (task-75).
    pub fn aggregate_intermediate(
        &self,
        query: &Query,
        aggs: &Aggregations,
    ) -> Result<IntermediateAggregationResults> {
        let query = self.build(query)?;
        let searcher = self.reader.searcher();
        let collector =
            DistributedAggregationCollector::from_aggs(aggs.clone(), AggContextParams::default());
        Ok(searcher.search(query.as_ref(), &collector)?)
    }

    /// Count the documents matching `query` — the **live match total**, since the single
    /// index natively excludes superseded/deleted docs (same liveness as `num_docs`/aggregations).
    /// Cheap: no scoring, sorting, or doc materialization, just a `Count` over the matched docset.
    /// Used for the search response's `total` (the true match count, distinct from page size —
    /// task-68). Validates fields like [`search`](Self::search), so a bad query errors clearly.
    pub fn count(&self, query: &Query) -> Result<u64> {
        let tantivy_query = self.build(query)?;
        let searcher = self.reader.searcher();
        Ok(searcher.search(tantivy_query.as_ref(), &Count)? as u64)
    }

    /// Execute a [`Query`] AST as BM25, returning ranked **coordinates + scores**.
    /// Validates fields against the schema (unknown/non-searchable field →
    /// [`IndexError::UnknownField`]), so a bad query is a clear error, not a
    /// silent empty result.
    pub fn search(&self, query: &Query, k: usize) -> Result<Vec<Hit>> {
        Ok(self
            .search_sorted(query, k, &[], None)?
            .into_iter()
            .map(|(hit, _)| hit)
            .collect())
    }

    /// Execute `query` returning up to `limit` `(hit, sort_values)` pairs. With no
    /// `sort` keys the window is the top-`limit` by **score** (descending) and each
    /// hit's `sort_values` is empty (the store ranks by `Hit::score`). With one or
    /// more keys the window is the top-`limit` by the **primary** key, and each hit
    /// carries the value of **every** key (in key order) read from its columnar fast
    /// field — `None` when the doc lacks the field — so the store can do the full
    /// multi-key merge + page across generations (task-23). For key sort `Hit::score`
    /// is 0.0 (relevance isn't the ranking).
    pub fn search_sorted(
        &self,
        query: &Query,
        limit: usize,
        sort: &[Sort],
        after: Option<&SearchAfter>,
    ) -> Result<Vec<(Hit, Vec<SortValue>)>> {
        // With a keyset cursor, AND the user query with a predicate that admits only
        // docs strictly after the cursor in the total order (task-23). The cursor
        // needs a primary sort key to range over.
        let base = self.build(query)?;
        let tantivy_query: Box<dyn TantivyQuery> = match after {
            None => base,
            Some(cursor) => {
                if sort.is_empty() {
                    return Err(IndexError::QueryType(
                        "search_after requires at least one sort key".into(),
                    ));
                }
                let keyset = self.keyset_after(sort, cursor)?;
                Box::new(BooleanQuery::new(vec![
                    (Occur::Must, base),
                    (Occur::Must, keyset),
                ]))
            }
        };
        if limit == 0 {
            return Ok(Vec::new());
        }
        let searcher = self.reader.searcher();
        let collector = TopDocs::with_limit(limit);

        // The candidate window: top-`limit` by score (no keys) or by the primary key.
        let window: Vec<(f32, tantivy::DocAddress)> = match sort.first() {
            None => searcher.search(tantivy_query.as_ref(), &collector.order_by_score())?,
            Some(primary) => {
                let order = match primary.order {
                    SortOrder::Asc => tantivy::Order::Asc,
                    SortOrder::Desc => tantivy::Order::Desc,
                };
                self.windowed_by_field(
                    &searcher,
                    tantivy_query.as_ref(),
                    &primary.field,
                    order,
                    limit,
                )?
            }
        };

        // The window score is the ranking value for an unsorted query and for an
        // explicit `_score` primary (task-66); for a field-sorted query it's 0.0 (the
        // store ranks by the sort tuple, not relevance).
        let by_score = sort.is_empty() || sort.first().is_some_and(Sort::is_score);
        let key_field = self.index.schema().get_field(KEY_FIELD)?;
        let mut out = Vec::with_capacity(window.len());
        for (score, address) in window {
            let doc: TantivyDocument = searcher.doc(address)?;
            let key = stored_key(&doc, key_field)?;
            // For each sort key, this doc's value: the relevance score for a `_score`
            // key (only a primary reaches this windowed path), else the fast field.
            let mut sort_values = Vec::with_capacity(sort.len());
            for s in sort {
                sort_values.push(if s.is_score() {
                    SortValue::Num(score as f64)
                } else {
                    self.fast_value(&searcher, address, &s.field)?
                });
            }
            out.push((
                Hit {
                    key,
                    score: if by_score { score } else { 0.0 },
                    fields: self.cached_fields(&doc),
                },
                sort_values,
            ));
        }
        Ok(out)
    }

    /// **Exhaustively** scan every matching doc (honoring an optional keyset `after`),
    /// returning `(hit, sort_values)` for each. Unlike [`search_sorted`](
    /// Self::search_sorted)'s top-`limit`-by-primary window, this is correct for
    /// **multi-key** sort even when many docs tie on the primary key — the store does
    /// the full-tuple sort. `O(matches)`; used for the multi-key paging path.
    pub fn scan_sorted(
        &self,
        query: &Query,
        sort: &[Sort],
        after: Option<&SearchAfter>,
    ) -> Result<Vec<(Hit, Vec<SortValue>)>> {
        let base = self.build(query)?;
        let tantivy_query: Box<dyn TantivyQuery> = match after {
            None => base,
            Some(cursor) => {
                if sort.is_empty() {
                    return Err(IndexError::QueryType(
                        "search_after requires at least one sort key".into(),
                    ));
                }
                Box::new(BooleanQuery::new(vec![
                    (Occur::Must, base),
                    (Occur::Must, self.keyset_after(sort, cursor)?),
                ]))
            }
        };
        let searcher = self.reader.searcher();
        let docs = searcher.search(tantivy_query.as_ref(), &DocSetCollector)?;
        // When a `_score` key is present (task-66) we need each matching doc's relevance.
        // The exhaustive collector doesn't score, so score per doc via `explain` (the same
        // scorer as search, so it matches ranking exactly). `_score` rejects keyset paging,
        // so `tantivy_query` here is the unwrapped user query — the right thing to explain.
        let want_score = sort_has_score(sort);
        let key_field = self.index.schema().get_field(KEY_FIELD)?;
        let mut out = Vec::with_capacity(docs.len());
        for address in docs {
            let doc: TantivyDocument = searcher.doc(address)?;
            let key = stored_key(&doc, key_field)?;
            let score = if want_score {
                tantivy_query
                    .explain(&searcher, address)
                    .map(|e| e.value())
                    .unwrap_or(0.0)
            } else {
                0.0
            };
            let mut sort_values = Vec::with_capacity(sort.len());
            for s in sort {
                sort_values.push(if s.is_score() {
                    SortValue::Num(score as f64)
                } else {
                    self.fast_value(&searcher, address, &s.field)?
                });
            }
            out.push((
                Hit {
                    key,
                    score,
                    fields: self.cached_fields(&doc),
                },
                sort_values,
            ));
        }
        Ok(out)
    }

    /// Execute `query` (top-`k` by score) and, for each hit, generate a highlighted
    /// **snippet** of `field` (task-25) — matched terms wrapped in `<b>…</b>`, capped
    /// at `max_chars`. The snippet is `None` when the doc has no matching fragment.
    /// `field` must be an analyzed **TEXT** field that is **cached** (`STORED`) so its
    /// text is available to highlight.
    pub fn search_highlighted(
        &self,
        query: &Query,
        k: usize,
        field: &str,
        max_chars: usize,
    ) -> Result<Vec<(Hit, Option<String>)>> {
        let tantivy_query = self.build(query)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let (hl_field, is_text) = self.resolve_field(Some(field))?;
        if !is_text {
            return Err(IndexError::QueryType(format!(
                "highlight requires an analyzed TEXT field, got `{field}`"
            )));
        }
        let searcher = self.reader.searcher();
        let mut generator = tantivy::snippet::SnippetGenerator::create(
            &searcher,
            tantivy_query.as_ref(),
            hl_field,
        )?;
        generator.set_max_num_chars(max_chars);

        let key_field = self.index.schema().get_field(KEY_FIELD)?;
        let top = searcher.search(
            tantivy_query.as_ref(),
            &TopDocs::with_limit(k).order_by_score(),
        )?;
        let mut out = Vec::with_capacity(top.len());
        for (score, address) in top {
            let doc: TantivyDocument = searcher.doc(address)?;
            let key = stored_key(&doc, key_field)?;
            let snippet = generator.snippet_from_doc(&doc);
            let highlight = (!snippet.fragment().is_empty()).then(|| snippet.to_html());
            out.push((
                Hit {
                    key,
                    score,
                    fields: self.cached_fields(&doc),
                },
                highlight,
            ));
        }
        Ok(out)
    }

    /// **Prefix autocomplete** (task-25): the indexed terms of `field` that start with
    /// `prefix`, each with its document frequency, by scanning the field's term
    /// dictionary from `prefix` until a term no longer matches. `field` must be an
    /// indexed string field (TEXT → analyzed tokens, KEYWORD → raw values); the store
    /// merges these across generations and keeps the top suggestions. Collection is
    /// capped at `scan_cap` terms so a broad prefix can't scan an entire vocabulary.
    ///
    /// Frequencies are **approximate** — the term dictionary is not merge-on-read
    /// liveness-filtered, so a term present only in superseded docs may still appear
    /// (a hint, self-healing on compaction). The prefix is lowercased for TEXT fields
    /// to match the analyzer's lowercasing.
    pub fn prefix_terms(
        &self,
        field: &str,
        prefix: &str,
        scan_cap: usize,
    ) -> Result<Vec<(String, u64)>> {
        let (handle, is_text) = self.resolve_field(Some(field))?;
        let needle = if is_text {
            prefix.to_lowercase()
        } else {
            prefix.to_string()
        };
        let needle = needle.as_bytes();

        let searcher = self.reader.searcher();
        let mut out: Vec<(String, u64)> = Vec::new();
        for segment in searcher.segment_readers() {
            let inverted = segment.inverted_index(handle)?;
            let terms = inverted.terms();
            let mut stream = terms
                .range()
                .ge(needle)
                .into_stream()
                .map_err(|e| IndexError::Tantivy(e.into()))?;
            while stream.advance() {
                let key = stream.key();
                if !key.starts_with(needle) {
                    break; // sorted dictionary — past the prefix range
                }
                out.push((
                    String::from_utf8_lossy(key).into_owned(),
                    stream.value().doc_freq as u64,
                ));
                if out.len() >= scan_cap {
                    break;
                }
            }
        }
        Ok(out)
    }

    /// **Did-you-mean** candidates (task-25): indexed terms of `field` within edit
    /// distance `max_dist` of `term` (excluding `term` itself), each as `(term,
    /// distance, doc_freq)`. Scans the field's term dictionary, pruning by length and
    /// a distance-bounded Levenshtein, capped at `scan_cap` terms. The store merges
    /// across generations and ranks. `field` must be an indexed TEXT/KEYWORD field; the
    /// query is lowercased for TEXT to match the analyzer.
    ///
    /// A linear dictionary scan (a Levenshtein automaton is a future optimization);
    /// frequencies are **approximate** (not liveness-filtered), the suggester contract.
    pub fn fuzzy_terms(
        &self,
        field: &str,
        term: &str,
        max_dist: u8,
        scan_cap: usize,
    ) -> Result<Vec<(String, u8, u64)>> {
        let (handle, is_text) = self.resolve_field(Some(field))?;
        let needle = if is_text {
            term.to_lowercase()
        } else {
            term.to_string()
        };
        let q: Vec<char> = needle.chars().collect();

        let searcher = self.reader.searcher();
        let mut out: Vec<(String, u8, u64)> = Vec::new();
        let mut scanned = 0usize;
        'outer: for segment in searcher.segment_readers() {
            let inverted = segment.inverted_index(handle)?;
            let mut stream = inverted
                .terms()
                .stream()
                .map_err(|e| IndexError::Tantivy(e.into()))?;
            while stream.advance() {
                scanned += 1;
                if scanned > scan_cap {
                    break 'outer;
                }
                let Ok(cand) = std::str::from_utf8(stream.key()) else {
                    continue;
                };
                let chars: Vec<char> = cand.chars().collect();
                // Length prune: edit distance is ≥ the length gap, so skip early.
                if (chars.len() as i64 - q.len() as i64).unsigned_abs() > max_dist as u64 {
                    continue;
                }
                if let Some(d) = bounded_levenshtein(&q, &chars, max_dist) {
                    if d > 0 {
                        // Exclude the term as typed — "did you mean" wants alternatives.
                        out.push((cand.to_string(), d, stream.value().doc_freq as u64));
                    }
                }
            }
        }
        Ok(out)
    }

    /// The candidate **window**: the top-`limit` `(score=0, addr)` ordered by the
    /// numeric/date/**string** fast field `field`. Errors if the field isn't a fast
    /// field. The values themselves are read back per key by [`fast_value`](
    /// Self::fast_value) so multi-key ordering is resolved in the store.
    fn windowed_by_field(
        &self,
        searcher: &tantivy::Searcher,
        query: &dyn TantivyQuery,
        field: &str,
        order: tantivy::Order,
        limit: usize,
    ) -> Result<Vec<(f32, tantivy::DocAddress)>> {
        // `_score` as the primary key (task-66): the window IS the top-`limit` by score —
        // ordered by relevance, with the real per-doc score carried through (unlike fast
        // fields, whose window score is 0.0 and read back via `fast_value`). Descending is
        // the natural score order; an ascending `_score` is re-ordered by the store.
        if field == SCORE_SORT_KEY {
            return Ok(searcher.search(query, &TopDocs::with_limit(limit).order_by_score())?);
        }
        self.ensure_sortable(field)?;
        let (_, ftype) = self.resolve_typed_field(field)?;
        let collector = TopDocs::with_limit(limit);
        let addrs: Vec<tantivy::DocAddress> = match ftype {
            TvFieldType::I64(_) => searcher
                .search(query, &collector.order_by_fast_field::<i64>(field, order))?
                .into_iter()
                .map(|(_, a)| a)
                .collect(),
            TvFieldType::F64(_) => searcher
                .search(query, &collector.order_by_fast_field::<f64>(field, order))?
                .into_iter()
                .map(|(_, a)| a)
                .collect(),
            TvFieldType::Date(_) => searcher
                .search(
                    query,
                    &collector.order_by_fast_field::<DateTime>(field, order),
                )?
                .into_iter()
                .map(|(_, a): (Option<DateTime>, _)| a)
                .collect(),
            TvFieldType::Str(_) => searcher
                .search(query, &collector.order_by_string_fast_field(field, order))?
                .into_iter()
                .map(|(_, a): (Option<String>, _)| a)
                .collect(),
            _ => unreachable!("ensure_sortable validated the type"),
        };
        Ok(addrs.into_iter().map(|a| (0.0, a)).collect())
    }

    /// Read `field`'s [`SortValue`] for `address` from its columnar fast field
    /// ([`Missing`](SortValue::Missing) when the doc has no value). Numeric/date →
    /// [`Num`](SortValue::Num) (DATE as epoch micros); KEYWORD → [`Str`](SortValue::Str).
    fn fast_value(
        &self,
        searcher: &tantivy::Searcher,
        address: tantivy::DocAddress,
        field: &str,
    ) -> Result<SortValue> {
        self.ensure_sortable(field)?;
        let (_, ftype) = self.resolve_typed_field(field)?;
        let ff = searcher.segment_reader(address.segment_ord).fast_fields();
        let num = |v: Option<f64>| v.map(SortValue::Num).unwrap_or(SortValue::Missing);
        let v = match ftype {
            TvFieldType::I64(_) => num(ff.i64(field)?.first(address.doc_id).map(|x| x as f64)),
            TvFieldType::F64(_) => num(ff.f64(field)?.first(address.doc_id)),
            TvFieldType::Date(_) => num(ff
                .date(field)?
                .first(address.doc_id)
                .map(|d| d.into_timestamp_micros() as f64)),
            TvFieldType::Str(_) => {
                let col = ff.str(field)?.ok_or_else(|| not_a_sort_field(field))?;
                match col.ords().first(address.doc_id) {
                    Some(ord) => {
                        let mut s = String::new();
                        col.ord_to_str(ord, &mut s)
                            .map_err(|e| IndexError::QueryType(format!("sort read: {e}")))?;
                        SortValue::Str(s)
                    }
                    None => SortValue::Missing,
                }
            }
            _ => unreachable!("ensure_sortable validated the type"),
        };
        Ok(v)
    }

    /// Validate `field` is a **fast** field usable as a sort key — numeric, date, or a
    /// KEYWORD string fast field. The reserved [`SCORE_SORT_KEY`] (`_score`, task-66) is
    /// always sortable (relevance, not a field), so it is exempt from the fast check.
    fn ensure_sortable(&self, field: &str) -> Result<()> {
        if field == SCORE_SORT_KEY {
            return Ok(());
        }
        let (_, ftype) = self.resolve_typed_field(field)?;
        let fast = match &ftype {
            TvFieldType::I64(o) | TvFieldType::F64(o) => o.is_fast(),
            TvFieldType::Date(o) => o.is_fast(),
            TvFieldType::Str(o) => o.is_fast(),
            _ => false,
        };
        if fast {
            Ok(())
        } else {
            Err(not_a_sort_field(field))
        }
    }

    /// Scan **every** matching doc for a [field collapse](growlerdb_core::SearchParams)
    /// (grouping and counting need all members, not a top-`k` window), returning
    /// `(hit, group_value, sort_values)` for each doc that has the `collapse` field
    /// set. Docs lacking the collapse field are skipped (they can't be grouped). The
    /// store merges across generations (liveness), orders by `sort`, and reduces to the
    /// top hit + count per group. Cost is `O(matches)` — search-support scope (D24).
    pub fn collapse_scan(
        &self,
        query: &Query,
        sort: &[Sort],
        collapse: &str,
    ) -> Result<Vec<CollapseEntry>> {
        let tantivy_query = self.build(query)?;
        let searcher = self.reader.searcher();
        let docs = searcher.search(tantivy_query.as_ref(), &DocSetCollector)?;
        let key_field = self.index.schema().get_field(KEY_FIELD)?;
        let mut out = Vec::with_capacity(docs.len());
        for address in docs {
            let Some(group) = self.group_value(&searcher, address, collapse)? else {
                continue;
            };
            let doc: TantivyDocument = searcher.doc(address)?;
            let key = stored_key(&doc, key_field)?;
            let mut sort_values = Vec::with_capacity(sort.len());
            for s in sort {
                sort_values.push(self.fast_value(&searcher, address, &s.field)?);
            }
            out.push((
                Hit {
                    key,
                    score: 0.0,
                    fields: self.cached_fields(&doc),
                },
                group,
                sort_values,
            ));
        }
        Ok(out)
    }

    /// Read the **collapse group value** for `address` from `field`'s columnar fast
    /// field — a [`GValue`] for KEYWORD/LONG/DOUBLE/BOOL/DATE (DATE as epoch micros).
    /// `None` when the doc has no value. Errors if `field` is not a fast field.
    fn group_value(
        &self,
        searcher: &tantivy::Searcher,
        address: tantivy::DocAddress,
        field: &str,
    ) -> Result<Option<GValue>> {
        let (_, ftype) = self.resolve_typed_field(field)?;
        let not_fast =
            || IndexError::QueryType(format!("collapse needs a fast field, got `{field}`"));
        let ff = searcher.segment_reader(address.segment_ord).fast_fields();
        let v = match ftype {
            TvFieldType::Str(_) => {
                let col = ff.str(field)?.ok_or_else(not_fast)?;
                match col.ords().first(address.doc_id) {
                    Some(ord) => {
                        let mut s = String::new();
                        col.ord_to_str(ord, &mut s)
                            .map_err(|e| IndexError::QueryType(format!("collapse read: {e}")))?;
                        Some(GValue::Str(s))
                    }
                    None => None,
                }
            }
            TvFieldType::I64(o) if o.is_fast() => {
                ff.i64(field)?.first(address.doc_id).map(GValue::Int)
            }
            TvFieldType::F64(o) if o.is_fast() => {
                ff.f64(field)?.first(address.doc_id).map(GValue::Float)
            }
            TvFieldType::Bool(o) if o.is_fast() => {
                ff.bool(field)?.first(address.doc_id).map(GValue::Bool)
            }
            TvFieldType::Date(o) if o.is_fast() => ff
                .date(field)?
                .first(address.doc_id)
                .map(|d| GValue::Int(d.into_timestamp_micros())),
            _ => return Err(not_fast()),
        };
        Ok(v)
    }

    /// Build the **keyset predicate**: a query matching exactly the docs strictly
    /// *after* `cursor` in the [total order](growlerdb_core::Sort) defined by `sort`
    /// (then the composite key). It is the lexicographic "tuple > cursor" expressed as
    /// an OR of clauses — for each position `i`, *all earlier keys equal the cursor
    /// AND key `i` is strictly after the cursor*; plus a final clause where every key
    /// equals the cursor AND the composite key is greater. A missing field sorts last,
    /// so "strictly after a present value" also admits docs lacking that field, and
    /// "equal to a missing cursor value" means the field is absent. `sort` is
    /// non-empty (checked by the caller).
    fn keyset_after(&self, sort: &[Sort], cursor: &SearchAfter) -> Result<Box<dyn TantivyQuery>> {
        if sort_has_score(sort) {
            // A relevance score isn't a stable, range-able key, so it can't anchor a
            // keyset predicate (task-66). `_score` sorts are offset-paged only.
            return Err(IndexError::QueryType(
                "search_after (keyset paging) is not supported with a `_score` sort key; \
                 use offset paging"
                    .into(),
            ));
        }
        if cursor.sort_values.len() != sort.len() {
            return Err(IndexError::QueryType(
                "search_after cursor arity does not match the sort keys".into(),
            ));
        }
        // Resolve each sort key's field + type once (validates they're sortable).
        let mut cols = Vec::with_capacity(sort.len());
        for s in sort {
            self.ensure_sortable(&s.field)?;
            let (f, ft) = self.resolve_typed_field(&s.field)?;
            cols.push((s.field.as_str(), f, ft));
        }
        let key_enc = self.index.schema().get_field(KEY_ENC_FIELD)?;

        let mut shoulds: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::new();
        for i in 0..sort.len() {
            // Position `i` strictly after the cursor; skip if the cursor value is
            // missing there (nothing sorts strictly after "last").
            let Some(after_i) = self.kv_after(
                cols[i].0,
                cols[i].1,
                &cols[i].2,
                &cursor.sort_values[i],
                sort[i].order,
            )?
            else {
                continue;
            };
            let mut musts: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::new();
            for (j, col) in cols.iter().enumerate().take(i) {
                musts.push((
                    Occur::Must,
                    self.kv_exact(col.0, col.1, &col.2, &cursor.sort_values[j])?,
                ));
            }
            musts.push((Occur::Must, after_i));
            shoulds.push((Occur::Should, Box::new(BooleanQuery::new(musts))));
        }
        // Final clause: every key equals the cursor; break the tie by composite key.
        let mut musts: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::new();
        for (j, col) in cols.iter().enumerate() {
            musts.push((
                Occur::Must,
                self.kv_exact(col.0, col.1, &col.2, &cursor.sort_values[j])?,
            ));
        }
        musts.push((
            Occur::Must,
            Box::new(RangeQuery::new(
                Bound::Excluded(Term::from_field_bytes(key_enc, &cursor.key.encode())),
                Bound::Unbounded,
            )),
        ));
        shoulds.push((Occur::Should, Box::new(BooleanQuery::new(musts))));
        Ok(Box::new(BooleanQuery::new(shoulds)))
    }

    /// A query matching docs whose `field` **equals** the cursor value `val` — an
    /// inclusive point range for a present value, or "field absent" (`MustNot Exists`)
    /// when the cursor lacked the field ([`Missing`](SortValue::Missing)).
    fn kv_exact(
        &self,
        name: &str,
        field: Field,
        ftype: &TvFieldType,
        val: &SortValue,
    ) -> Result<Box<dyn TantivyQuery>> {
        match sort_term(field, ftype, val)? {
            Some(t) => Ok(Box::new(RangeQuery::new(
                Bound::Included(t.clone()),
                Bound::Included(t),
            ))),
            None => Ok(self.not_exists(name)),
        }
    }

    /// A query matching docs whose `field` is **strictly after** the cursor value
    /// `val` in `order` — i.e. greater (asc) / lesser (desc), *plus* docs missing the
    /// field (a missing value sorts last, after any present one). `None` when the
    /// cursor value is itself [`Missing`](SortValue::Missing) (nothing is after "last").
    fn kv_after(
        &self,
        name: &str,
        field: Field,
        ftype: &TvFieldType,
        val: &SortValue,
        order: SortOrder,
    ) -> Result<Option<Box<dyn TantivyQuery>>> {
        let Some(t) = sort_term(field, ftype, val)? else {
            return Ok(None);
        };
        let range: Box<dyn TantivyQuery> = match order {
            SortOrder::Asc => Box::new(RangeQuery::new(Bound::Excluded(t), Bound::Unbounded)),
            SortOrder::Desc => Box::new(RangeQuery::new(Bound::Unbounded, Bound::Excluded(t))),
        };
        // A missing value sorts after any present one, so it is "strictly after" too.
        Ok(Some(Box::new(BooleanQuery::new(vec![
            (Occur::Should, range),
            (Occur::Should, self.not_exists(name)),
        ]))))
    }

    /// A query matching docs that do **not** have `field` set (`MatchAll` minus
    /// `Exists`), used for the "equal to a missing cursor value" case.
    fn not_exists(&self, name: &str) -> Box<dyn TantivyQuery> {
        Box::new(BooleanQuery::new(vec![
            (Occur::Must, Box::new(AllQuery) as Box<dyn TantivyQuery>),
            (
                Occur::MustNot,
                Box::new(ExistsQuery::new(name.to_string(), false)),
            ),
        ]))
    }

    /// Compile a [`Query`] AST into a Tantivy query, validating field references.
    fn build(&self, query: &Query) -> Result<Box<dyn TantivyQuery>> {
        match query {
            Query::MatchAll => Ok(Box::new(AllQuery)),
            Query::Term { field, value } => {
                // A bare `field:value` on a numeric / date / bool / IP field is an **exact-value
                // match**, not a text term — those columns are indexed but not analyzed, so a text
                // `TermQuery` finds nothing and `resolve_field` (text-only) would reject the field as
                // "non-searchable". Reuse the typed, validated range path as an inclusive
                // `[value TO value]`. (A genuinely unknown field still falls through to the text path
                // below and errors as `UnknownField`.)
                if let Some(name) = field.as_deref() {
                    if let Ok((f, ftype)) = self.resolve_typed_field(name) {
                        // BOOL is exact-match, but Tantivy's `RangeQuery` rejects a `Bool` term
                        // ("Expected term with u64, i64, f64 or date"), so a `[true TO true]` range
                        // errors. Build the `TermQuery` directly instead.
                        if let TvFieldType::Bool(_) = ftype {
                            let b = value.parse::<bool>().map_err(|_| {
                                IndexError::QueryType(format!("bad bool value `{value}`"))
                            })?;
                            return Ok(Box::new(TermQuery::new(
                                Term::from_field_bool(f, b),
                                IndexRecordOption::Basic,
                            )));
                        }
                        if !matches!(ftype, TvFieldType::Str(_)) {
                            return self.build(&Query::Range {
                                field: name.to_string(),
                                lower: Some(value.clone()),
                                lower_inclusive: true,
                                upper: Some(value.clone()),
                                upper_inclusive: true,
                            });
                        }
                    }
                }
                let (field, is_text) = self.resolve_field(field.as_deref())?;
                // TEXT is analyzed (lowercased) at index time; match that. KEYWORD
                // is raw/exact. The record option must match how the field was
                // indexed (TEXT has freqs+positions; KEYWORD is basic).
                let (term, opt) = if is_text {
                    (
                        Term::from_field_text(field, &value.to_lowercase()),
                        IndexRecordOption::WithFreqsAndPositions,
                    )
                } else {
                    (
                        Term::from_field_text(field, value),
                        IndexRecordOption::Basic,
                    )
                };
                Ok(Box::new(TermQuery::new(term, opt)))
            }
            Query::Terms { field, values } => {
                let (field, is_text) = self.resolve_field(Some(field))?;
                let terms = values
                    .iter()
                    .map(|v| Term::from_field_text(field, &fold(v, is_text)))
                    .collect::<Vec<_>>();
                Ok(Box::new(TermSetQuery::new(terms)))
            }
            Query::Match { field, text, op } => {
                let (field, _) = self.resolve_field(field.as_deref())?;
                let tokens = self.analyze(field, text)?;
                if tokens.is_empty() {
                    return Ok(Box::new(EmptyQuery));
                }
                let occur = match op {
                    MatchOp::And => Occur::Must,
                    MatchOp::Or => Occur::Should,
                };
                let clauses = tokens
                    .iter()
                    .map(|t| {
                        let q: Box<dyn TantivyQuery> = Box::new(TermQuery::new(
                            Term::from_field_text(field, t),
                            IndexRecordOption::WithFreqs,
                        ));
                        (occur, q)
                    })
                    .collect::<Vec<_>>();
                Ok(Box::new(BooleanQuery::new(clauses)))
            }
            Query::Phrase { field, terms, slop } => {
                // A quoted `field:"value"` parses as a Phrase, but a positional phrase only means
                // something on an analyzed TEXT field. The facet / filter chips emit `field:"value"`
                // for *every* field type (KEYWORD, numeric, date, …), so on anything but analyzed
                // TEXT a phrase is an **exact-value match** — reuse the Term path (KEYWORD raw,
                // numeric/date exact via Range) rather than rejecting the field.
                if let Some(name) = field.as_deref() {
                    if let Ok((_, ftype)) = self.resolve_typed_field(name) {
                        // Non-Str (numeric / date / bool / IP): resolve_field (text-only) would
                        // reject it; delegate before resolving.
                        if !matches!(ftype, TvFieldType::Str(_)) {
                            return self.build(&Query::Term {
                                field: Some(name.to_string()),
                                value: terms.join(" "),
                            });
                        }
                    }
                }
                let name = field.clone();
                let (field, is_text) = self.resolve_field(field.as_deref())?;
                // KEYWORD (raw, non-analyzed Str): a phrase is an exact keyword match, not a
                // positional phrase — reuse Term so `field:"value"` matches the stored keyword.
                if !is_text {
                    return self.build(&Query::Term {
                        field: name,
                        value: terms.join(" "),
                    });
                }
                let mut tokens = Vec::new();
                for t in terms {
                    for tok in self.analyze(field, t)? {
                        tokens.push(Term::from_field_text(field, &tok));
                    }
                }
                match tokens.len() {
                    0 => Ok(Box::new(EmptyQuery)),
                    1 => Ok(Box::new(TermQuery::new(
                        tokens.pop().unwrap(),
                        IndexRecordOption::WithFreqsAndPositions,
                    ))),
                    _ => {
                        // A multi-token phrase needs positions. A field mapped with
                        // `record: BASIC|FREQ` (task-216) doesn't have them — fail with the
                        // fix, not tantivy's opaque weight error or silently-empty results.
                        let schema = self.index.schema();
                        let entry = schema.get_field_entry(field);
                        let has_positions = matches!(
                            entry.field_type(),
                            TvFieldType::Str(o) if o.get_indexing_options()
                                .is_some_and(|ix| ix.index_option().has_positions())
                        );
                        if !has_positions {
                            return Err(IndexError::QueryType(format!(
                                "phrase query on `{}` needs token positions, but the field is \
                                 mapped without them — set `record: POSITION` (and reindex) to \
                                 phrase-search it",
                                entry.name()
                            )));
                        }
                        let mut pq = PhraseQuery::new(tokens);
                        pq.set_slop(*slop);
                        Ok(Box::new(pq))
                    }
                }
            }
            Query::Prefix { field, prefix } => {
                let (field, is_text) = self.resolve_field(field.as_deref())?;
                if prefix.is_empty() {
                    return Err(IndexError::CostGuard("empty prefix".into()));
                }
                let pattern = format!("{}.*", regex_escape(&fold(prefix, is_text)));
                Ok(Box::new(RegexQuery::from_pattern(&pattern, field)?))
            }
            Query::Wildcard { field, pattern } => {
                let (field, is_text) = self.resolve_field(field.as_deref())?;
                if pattern.starts_with('*') || pattern.starts_with('?') {
                    return Err(IndexError::CostGuard(
                        "leading wildcard (`*`/`?`) scans every term".into(),
                    ));
                }
                let regex = glob_to_regex(&fold(pattern, is_text));
                Ok(Box::new(RegexQuery::from_pattern(&regex, field)?))
            }
            Query::Fuzzy {
                field,
                value,
                distance,
            } => {
                let (field, is_text) = self.resolve_field(field.as_deref())?;
                if *distance > 2 {
                    return Err(IndexError::CostGuard("fuzzy distance max is 2".into()));
                }
                let term = Term::from_field_text(field, &fold(value, is_text));
                Ok(Box::new(FuzzyTermQuery::new(term, *distance, true)))
            }
            Query::Regex { field, pattern } => {
                let (field, _) = self.resolve_field(field.as_deref())?;
                guard_regex(pattern)?;
                Ok(Box::new(RegexQuery::from_pattern(pattern, field)?))
            }
            Query::Exists { field } => {
                // Exists works on any indexed/fast field, so it skips the text-only
                // `resolve_field`; the field must exist and not be the stored key.
                if field == KEY_FIELD || self.index.schema().get_field(field).is_err() {
                    return Err(IndexError::UnknownField(field.clone()));
                }
                Ok(Box::new(ExistsQuery::new(field.clone(), false)))
            }
            Query::Range {
                field,
                lower,
                lower_inclusive,
                upper,
                upper_inclusive,
            } => {
                let (f, ftype) = self.resolve_typed_field(field)?;
                let lo = range_bound(lower.as_deref(), *lower_inclusive, f, &ftype)?;
                let hi = range_bound(upper.as_deref(), *upper_inclusive, f, &ftype)?;
                Ok(Box::new(RangeQuery::new(lo, hi)))
            }
            Query::IpCidr { field, cidr } => {
                let (f, ftype) = self.resolve_typed_field(field)?;
                if !matches!(ftype, TvFieldType::IpAddr(_)) {
                    return Err(IndexError::QueryType(format!(
                        "ip_cidr requires an IP field, got `{field}`"
                    )));
                }
                let (net, bcast) = cidr_range(cidr)
                    .ok_or_else(|| IndexError::QueryType(format!("invalid CIDR `{cidr}`")))?;
                Ok(Box::new(RangeQuery::new(
                    Bound::Included(Term::from_field_ip_addr(f, net)),
                    Bound::Included(Term::from_field_ip_addr(f, bcast)),
                )))
            }
            Query::Boost { query, boost } => {
                Ok(Box::new(BoostQuery::new(self.build(query)?, *boost)))
            }
            Query::Bool {
                must,
                should,
                must_not,
                filter,
            } => {
                let mut clauses: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::new();
                for q in must {
                    clauses.push((Occur::Must, self.build(q)?));
                }
                for q in should {
                    clauses.push((Occur::Should, self.build(q)?));
                }
                for q in must_not {
                    clauses.push((Occur::MustNot, self.build(q)?));
                }
                // `filter` constrains without scoring: a required clause forced to 0.
                for q in filter {
                    let inner = ConstScoreQuery::new(self.build(q)?, 0.0);
                    clauses.push((Occur::Must, Box::new(inner)));
                }
                // A purely-negative Bool (no positive base at all) needs match-all.
                if must.is_empty() && should.is_empty() && filter.is_empty() {
                    clauses.push((Occur::Must, Box::new(AllQuery)));
                }
                Ok(Box::new(BooleanQuery::new(clauses)))
            }
        }
    }

    /// Analyze `text` with `field`'s configured tokenizer into its indexed tokens
    /// (so a `Match`/`Phrase` token matches exactly what was indexed).
    fn analyze(&self, field: Field, text: &str) -> Result<Vec<String>> {
        let mut analyzer = self.index.tokenizer_for_field(field)?;
        let mut stream = analyzer.token_stream(text);
        let mut out = Vec::new();
        while stream.advance() {
            out.push(stream.token().text.clone());
        }
        Ok(out)
    }

    /// **Explain** how `query` scores the document identified by `key_enc` (task-102): locate the doc
    /// by its encoded composite key, then ask Tantivy for the per-clause BM25 explanation. Also
    /// returns the post-analyzer tokens the query searched for. `found = false` if the key isn't in
    /// the index; `matched = false` if the doc exists but the query doesn't select it.
    pub fn explain(&self, query: &Query, key_enc: &[u8]) -> Result<ExplainHit> {
        let analyzed = self.analyzed_terms(query);
        let searcher = self.reader.searcher();
        let key_enc_field = self.index.schema().get_field(KEY_ENC_FIELD)?;
        let key_q = TermQuery::new(
            Term::from_field_bytes(key_enc_field, key_enc),
            IndexRecordOption::Basic,
        );
        let found = searcher.search(&key_q, &TopDocs::with_limit(1).order_by_score())?;
        let Some((_, address)) = found.into_iter().next() else {
            return Ok(ExplainHit {
                found: false,
                matched: false,
                score: 0.0,
                detail: serde_json::Value::Null,
                analyzed,
            });
        };
        let tantivy_query = self.build(query)?;
        // `explain` errors when the doc doesn't match the query — that's a real, expected answer
        // ("matched = false"), not a failure.
        match tantivy_query.explain(&searcher, address) {
            Ok(exp) => Ok(ExplainHit {
                found: true,
                matched: true,
                score: exp.value(),
                detail: serde_json::to_value(&exp).unwrap_or(serde_json::Value::Null),
                analyzed,
            }),
            Err(_) => Ok(ExplainHit {
                found: true,
                matched: false,
                score: 0.0,
                detail: serde_json::Value::Null,
                analyzed,
            }),
        }
    }

    /// The post-analyzer tokens `query` searches for, as `(field, tokens)` (task-102). Walks the
    /// leaf clauses, running each field's analyzer so the console can show exactly what was matched.
    fn analyzed_terms(&self, query: &Query) -> Vec<(String, Vec<String>)> {
        let mut out = Vec::new();
        self.collect_analyzed(query, &mut out);
        out
    }

    fn collect_analyzed(&self, query: &Query, out: &mut Vec<(String, Vec<String>)>) {
        match query {
            Query::Term { field, value } => self.push_analyzed(field.as_deref(), value, out),
            Query::Match { field, text, .. } => self.push_analyzed(field.as_deref(), text, out),
            Query::Phrase { field, terms, .. } => {
                self.push_analyzed(field.as_deref(), &terms.join(" "), out)
            }
            Query::Terms { field, values } => {
                for v in values {
                    self.push_analyzed(Some(field), v, out);
                }
            }
            Query::Boost { query, .. } => self.collect_analyzed(query, out),
            Query::Bool {
                must,
                should,
                filter,
                ..
            } => {
                for sub in must.iter().chain(should).chain(filter) {
                    self.collect_analyzed(sub, out);
                }
            }
            _ => {}
        }
    }

    fn push_analyzed(&self, field: Option<&str>, text: &str, out: &mut Vec<(String, Vec<String>)>) {
        let name = field.unwrap_or("_default").to_string();
        // Run the field's analyzer when it's a TEXT field; otherwise the raw value is the token.
        let tokens = match self.resolve_field(field) {
            Ok((f, true)) => self
                .analyze(f, text)
                .unwrap_or_else(|_| vec![text.to_string()]),
            _ => vec![text.to_string()],
        };
        out.push((name, tokens));
    }

    /// Resolve a query field name to its Tantivy field + whether it is analyzed
    /// (TEXT). `None` resolves to the default TEXT field. The stored key field and
    /// any non-indexed field are rejected as unknown.
    fn resolve_field(&self, name: Option<&str>) -> Result<(Field, bool)> {
        let schema = self.index.schema();
        match name {
            Some(name) => {
                if name == KEY_FIELD {
                    return Err(IndexError::UnknownField(name.to_string()));
                }
                let field = schema
                    .get_field(name)
                    .map_err(|_| IndexError::UnknownField(name.to_string()))?;
                match field_kind(&schema, field) {
                    Some(is_text) => Ok((field, is_text)),
                    None => Err(IndexError::UnknownField(name.to_string())),
                }
            }
            None => default_text_field(&schema)
                .map(|f| (f, true))
                .ok_or(IndexError::NoDefaultField),
        }
    }

    /// Resolve a named field to its handle + Tantivy field type (for typed
    /// `Range`/`IpCidr`, which apply beyond text fields). Rejects the stored key.
    fn resolve_typed_field(&self, name: &str) -> Result<(Field, TvFieldType)> {
        if name == KEY_FIELD {
            return Err(IndexError::UnknownField(name.to_string()));
        }
        let schema = self.index.schema();
        let field = schema
            .get_field(name)
            .map_err(|_| IndexError::UnknownField(name.to_string()))?;
        let ftype = schema.get_field_entry(field).field_type().clone();
        Ok((field, ftype))
    }
}

/// A typed range bound: `None` → unbounded; otherwise parse `value` to the field's
/// type and wrap by inclusivity.
fn range_bound(
    value: Option<&str>,
    inclusive: bool,
    field: Field,
    ftype: &TvFieldType,
) -> Result<Bound<Term>> {
    let Some(v) = value else {
        return Ok(Bound::Unbounded);
    };
    let term = range_term(field, ftype, v)?;
    Ok(if inclusive {
        Bound::Included(term)
    } else {
        Bound::Excluded(term)
    })
}

/// Parse a range-bound string to a [`Term`] of the field's type (dates are epoch
/// microseconds; keyword ranges are lexicographic).
fn range_term(field: Field, ftype: &TvFieldType, v: &str) -> Result<Term> {
    let bad = |kind: &str| IndexError::QueryType(format!("bad {kind} range bound `{v}`"));
    Ok(match ftype {
        TvFieldType::I64(_) => Term::from_field_i64(field, v.parse().map_err(|_| bad("integer"))?),
        TvFieldType::F64(_) => Term::from_field_f64(field, v.parse().map_err(|_| bad("float"))?),
        TvFieldType::Bool(_) => Term::from_field_bool(field, v.parse().map_err(|_| bad("bool"))?),
        // A DATE bound is canonical epoch micros, but for authoring convenience it may also be
        // written as an ISO-8601 / RFC3339 datetime (`2024-01-01T00:00:00Z`) or a bare `YYYY-MM-DD`
        // date (UTC midnight); a raw integer stays epoch micros.
        TvFieldType::Date(_) => Term::from_field_date(
            field,
            DateTime::from_timestamp_micros(
                growlerdb_core::timestamp::parse_date_query_bound(v)
                    .ok_or_else(|| bad("date (epoch micros or ISO-8601)"))?,
            ),
        ),
        TvFieldType::Str(_) => Term::from_field_text(field, v),
        TvFieldType::IpAddr(_) => {
            Term::from_field_ip_addr(field, to_ipv6(v.parse().map_err(|_| bad("ip"))?))
        }
        _ => {
            return Err(IndexError::QueryType(
                "range unsupported for this field type".into(),
            ))
        }
    })
}

/// Levenshtein edit distance between `a` and `b`, short-circuiting to `None` once it
/// is known to exceed `max` (a whole DP row above `max` ⇒ no path back under it). Used
/// by the did-you-mean suggester to keep candidate scoring cheap.
fn bounded_levenshtein(a: &[char], b: &[char], max: u8) -> Option<u8> {
    let max = max as usize;
    let (n, m) = (a.len(), b.len());
    if n.abs_diff(m) > max {
        return None;
    }
    let mut prev: Vec<usize> = (0..=m).collect();
    let mut cur = vec![0usize; m + 1];
    for i in 1..=n {
        cur[0] = i;
        let mut row_min = cur[0];
        for j in 1..=m {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            cur[j] = (prev[j] + 1).min(cur[j - 1] + 1).min(prev[j - 1] + cost);
            row_min = row_min.min(cur[j]);
        }
        if row_min > max {
            return None;
        }
        std::mem::swap(&mut prev, &mut cur);
    }
    (prev[m] <= max).then_some(prev[m] as u8)
}

/// Build a keyset [`Term`] for a sort field from a cursor [`SortValue`], or `None` for
/// [`Missing`](SortValue::Missing). LONG/DATE round-trip through `i64` (DATE as epoch
/// micros, matching the `as f64` the cursor stored); KEYWORD uses the raw string term.
fn sort_term(field: Field, ftype: &TvFieldType, val: &SortValue) -> Result<Option<Term>> {
    let term = match (val, ftype) {
        (SortValue::Missing, _) => return Ok(None),
        (SortValue::Num(x), TvFieldType::I64(_)) => Term::from_field_i64(field, *x as i64),
        (SortValue::Num(x), TvFieldType::F64(_)) => Term::from_field_f64(field, *x),
        (SortValue::Num(x), TvFieldType::Date(_)) => {
            Term::from_field_date(field, DateTime::from_timestamp_micros(*x as i64))
        }
        (SortValue::Str(s), TvFieldType::Str(_)) => Term::from_field_text(field, s),
        _ => {
            return Err(IndexError::QueryType(
                "search_after cursor value does not match its sort field type".into(),
            ))
        }
    };
    Ok(Some(term))
}

/// The error for a field that isn't a usable fast sort field.
fn not_a_sort_field(field: &str) -> IndexError {
    IndexError::QueryType(format!(
        "sort needs a numeric/date/keyword fast field, got `{field}`"
    ))
}

/// Compute the inclusive `[network, broadcast]` IPv6 range of a CIDR block, mapping
/// IPv4 CIDRs into the v4-mapped v6 space Tantivy stores.
fn cidr_range(cidr: &str) -> Option<(Ipv6Addr, Ipv6Addr)> {
    let (addr, prefix) = cidr.split_once('/')?;
    let ip: IpAddr = addr.trim().parse().ok()?;
    let prefix: u32 = prefix.trim().parse().ok()?;
    let (v6, plen) = match ip {
        IpAddr::V4(v4) => (v4.to_ipv6_mapped(), prefix.checked_add(96)?),
        IpAddr::V6(v6) => (v6, prefix),
    };
    if plen > 128 {
        return None;
    }
    let bits = u128::from(v6);
    let mask = if plen == 0 {
        0
    } else {
        u128::MAX << (128 - plen)
    };
    Some((
        Ipv6Addr::from(bits & mask),
        Ipv6Addr::from((bits & mask) | !mask),
    ))
}

/// Whether `field` is indexed and, if so, analyzed (TEXT, tokenizer `default`)
/// vs raw (KEYWORD, tokenizer `raw`). `None` if the field isn't a searchable text
/// field (e.g. the STORED-only key field).
fn field_kind(schema: &Schema, field: Field) -> Option<bool> {
    match schema.get_field_entry(field).field_type() {
        tantivy::schema::FieldType::Str(opts) => opts
            .get_indexing_options()
            .map(|o| o.tokenizer() == "default"),
        _ => None,
    }
}

/// The first analyzed TEXT field in schema order (the default search field).
fn default_text_field(schema: &Schema) -> Option<Field> {
    schema.fields().find_map(|(field, entry)| {
        if entry.name() == KEY_FIELD {
            return None;
        }
        match field_kind(schema, field) {
            Some(true) => Some(field),
            _ => None,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::{
        CompositeKey, Document, IndexDefinition, MatchOp, Query, SourceField, SourceSchema,
        SourceType, Value,
    };
    use std::collections::BTreeMap;

    /// Parse a query string for the execution tests.
    fn q(s: &str) -> Query {
        Query::parse(s).unwrap()
    }

    /// A `docs` index: KEYWORD `id` (identifier) + TEXT `body`.
    fn docs_index() -> ResolvedIndex {
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

    fn doc(id: i64, body: &str) -> Document {
        let key = CompositeKey::new(vec![], vec![("id".into(), id.into())]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), id.into());
        fields.insert("body".to_string(), body.into());
        Document::new(key, fields)
    }

    fn batch() -> DocBatch {
        DocBatch::new(vec![
            doc(1, "the quick brown fox jumps"),
            doc(2, "a lazy brown dog sleeps"),
            doc(3, "unrelated content about cats"),
        ])
    }

    #[test]
    fn typed_fields_build_and_text_search_still_works() {
        use growlerdb_core::Value;
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
                SourceField::new("count", SourceType::Long),
                SourceField::new("active", SourceType::Bool),
                SourceField::new("when", SourceType::Date),
                SourceField::new("addr", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            r#"
name: docs
source: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }
mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD, fast: true }
    - { path: body, type: TEXT, cached: true }
    - { path: count, type: LONG, fast: true }
    - { path: active, type: BOOL }
    - { path: when, type: DATE, fast: true }
    - { path: addr, type: IP, fast: true }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap();

        let mk = |id: &str, body: &str, count: i64, active: bool, addr: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("body".to_string(), Value::from(body));
            f.insert("count".to_string(), Value::Int(count));
            f.insert("active".to_string(), Value::Bool(active));
            f.insert("when".to_string(), Value::Int(1_700_000_000_000_000)); // epoch micros
            f.insert("addr".to_string(), Value::from(addr));
            Document::new(key, f)
        };
        let batch = DocBatch::new(vec![
            mk("a", "quick brown fox", 10, true, "10.0.0.1"),
            mk("b", "lazy brown dog", 20, false, "192.168.1.5"),
        ]);

        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&idx);
        let core = TantivySegmentCore;
        // All typed columns (i64/bool/date/ip + fast/cached) index without error.
        assert_eq!(core.build(&schema, &batch, dir.path()).unwrap(), 2);

        let reader = core.open(dir.path()).unwrap();
        assert_eq!(reader.num_docs(), 2);
        // The analyzed text path still works alongside the typed columns.
        assert_eq!(reader.search(&q("body:brown"), 10).unwrap().len(), 2);

        // BOOL term match (task-247 / issue 1): `active:true`/`active:false` must select the right
        // doc without erroring (previously Internal "Expected term with u64/i64/f64/date, got Bool").
        assert_eq!(reader.search(&q("active:true"), 10).unwrap().len(), 1);
        assert_eq!(reader.search(&q("active:false"), 10).unwrap().len(), 1);

        // DATE range with an ISO-8601 bound (task-247 / issue 2): both docs are 2023-11-14
        // (1_700_000_000_000_000 micros). An ISO date-string bound must be accepted and select them;
        // it must match the equivalent epoch-micros bound exactly.
        assert_eq!(
            reader
                .search(&q("when:[2023-01-01 TO *]"), 10)
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            reader
                .search(&q("when:[2023-01-01T00:00:00Z TO *]"), 10)
                .unwrap()
                .len(),
            2
        );
        // A window that ends before the docs' date excludes them (ISO upper bound).
        assert_eq!(
            reader
                .search(&q("when:[* TO 2023-01-01]"), 10)
                .unwrap()
                .len(),
            0
        );
        // The ISO-date bound resolves to the same set as the raw epoch-micros bound.
        assert_eq!(
            reader
                .search(&q("when:[1700000000000000 TO *]"), 10)
                .unwrap()
                .len(),
            reader
                .search(&q("when:[2023-11-14 TO *]"), 10)
                .unwrap()
                .len(),
        );
    }

    #[test]
    fn a_declared_timestamp_format_normalizes_the_source_epoch_to_micros_at_build() {
        use growlerdb_core::Value;
        // `ts` is epoch-**millis** at the source (an int64 column) but declared as a timestamp via
        // `format: epoch_ms` — so it resolves to a DATE and must be indexed in canonical **micros**
        // (task-112). A doc whose `ts` can't be parsed is skipped, not allowed to wedge the build.
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("ts", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            r#"
name: events
source: { iceberg: { catalog: growlerdb, table: growlerdb.events } }
mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - { path: ts, format: epoch_ms, fast: true }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        // The format made it a DATE — so it shows up as a time field for the console (task-101).
        let schema = IndexSchema::from_resolved(&idx);
        assert_eq!(schema.date_fields(), vec!["ts"]);

        let mk = |id: &str, ts: Value| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("ts".to_string(), ts);
            Document::new(key, f)
        };
        // 1_782_000_000_000 ms == 1_782_000_000_000_000 µs (≈ 2026-06-29).
        let batch = DocBatch::new(vec![
            mk("a", Value::Int(1_782_000_000_000)), // valid epoch ms
            mk("b", Value::Str("not-a-timestamp".into())), // unparseable → ts skipped, doc still built
            // A `Ts` is *already* canonical micros (a native source timestamp, task-184) — the
            // declared epoch_ms format must pass it through untouched, not rescale it by 10³.
            mk("c", Value::Ts(1_782_000_000_000_000)),
        ]);

        let dir = tempfile::tempdir().unwrap();
        let core = TantivySegmentCore;
        assert_eq!(core.build(&schema, &batch, dir.path()).unwrap(), 3);
        let reader = core.open(dir.path()).unwrap();

        // A range query in **micros** (the canonical unit the console now sends) finds both the
        // normalized millis doc and the already-canonical Ts doc...
        assert_eq!(
            reader
                .search(&q("ts:[1781000000000000 TO 1783000000000000]"), 10)
                .unwrap()
                .len(),
            2,
            "millis source normalized to micros; Ts passed through as canonical micros"
        );
        // ...while the same window expressed in the **raw millis** value does NOT — proving the
        // value was scaled up by 10³, not stored as-is (which would be an off-by-10³ date).
        assert!(reader
            .search(&q("ts:[1781000000000 TO 1783000000000]"), 10)
            .unwrap()
            .is_empty());
        // The unparseable-`ts` doc indexed everything else and is findable by key.
        assert_eq!(reader.num_docs(), 3);
    }

    #[test]
    fn builds_reopens_and_searches() {
        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&docs_index());
        let core = TantivySegmentCore;

        // AC1/AC3: build a segment from a DocBatch, written to a local directory.
        let written = core.build(&schema, &batch(), dir.path()).unwrap();
        assert_eq!(written, 3);

        // AC3: reopen the directory and read it back.
        let reader = core.open(dir.path()).unwrap();
        assert_eq!(reader.num_docs(), 3);

        // AC4: BM25 is queryable. "brown" appears in docs 1 and 2.
        let hits = reader.search(&q("body:brown"), 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|h| h.score > 0.0));

        // AC2: the composite key is stored per doc and retrievable from a hit.
        let ids: Vec<i64> = hits
            .iter()
            .filter_map(|h| match h.key.get("id") {
                Some(growlerdb_core::Value::Int(i)) => Some(*i),
                _ => None,
            })
            .collect();
        assert!(ids.contains(&1) && ids.contains(&2));
        assert!(!ids.contains(&3));
    }

    #[test]
    fn boolean_operators_combine_clauses() {
        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&docs_index());
        let core = TantivySegmentCore;
        core.build(&schema, &batch(), dir.path()).unwrap();
        let reader = core.open(dir.path()).unwrap();

        let ids = |query: &str| -> Vec<i64> {
            let mut v: Vec<i64> = reader
                .search(&q(query), 10)
                .unwrap()
                .iter()
                .filter_map(|h| match h.key.get("id") {
                    Some(growlerdb_core::Value::Int(i)) => Some(*i),
                    _ => None,
                })
                .collect();
            v.sort_unstable();
            v
        };
        // AND: both terms (doc 2 has "brown" and "lazy").
        assert_eq!(ids("body:brown AND body:lazy"), vec![2]);
        // NOT: "brown" but not "lazy" (doc 1).
        assert_eq!(ids("body:brown AND NOT body:lazy"), vec![1]);
        // OR: either term (docs 1 and 3).
        assert_eq!(ids("body:fox OR body:cats"), vec![1, 3]);
    }

    #[test]
    fn text_is_analyzed_and_lowercased() {
        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&docs_index());
        let core = TantivySegmentCore;
        core.build(&schema, &batch(), dir.path()).unwrap();
        let reader = core.open(dir.path()).unwrap();

        // Uppercase query term matches lowercased analyzed text (standard+lowercase).
        let hits = reader.search(&q("body:QUICK"), 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.get("id"), Some(&growlerdb_core::Value::Int(1)));
    }

    #[test]
    fn keyword_is_exact_and_case_sensitive() {
        // A KEYWORD field matches the whole raw token, case-sensitively —
        // unlike a TEXT field, it is not lowercased.
        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&docs_index());
        let core = TantivySegmentCore;

        let key = CompositeKey::new(vec![], vec![("id".into(), "ERROR".into())]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), growlerdb_core::Value::Str("ERROR".into()));
        fields.insert("body".to_string(), "x".into());
        let d = Document::new(key, fields);
        core.build(&schema, &DocBatch::new(vec![d]), dir.path())
            .unwrap();
        let reader = core.open(dir.path()).unwrap();

        assert_eq!(reader.search(&q("id:ERROR"), 10).unwrap().len(), 1);
        // Lowercase does NOT match a raw keyword (would match a TEXT field).
        assert_eq!(reader.search(&q("id:error"), 10).unwrap().len(), 0);
    }

    #[test]
    fn unqualified_term_uses_default_text_field() {
        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&docs_index());
        let core = TantivySegmentCore;
        core.build(&schema, &batch(), dir.path()).unwrap();
        let reader = core.open(dir.path()).unwrap();

        // `fox` (no field) → default TEXT field `body`.
        let hits = reader.search(&q("fox"), 10).unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].key.get("id"), Some(&growlerdb_core::Value::Int(1)));
    }

    #[test]
    fn unknown_field_is_a_validation_error() {
        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&docs_index());
        let core = TantivySegmentCore;
        core.build(&schema, &batch(), dir.path()).unwrap();
        let reader = core.open(dir.path()).unwrap();

        // Unknown field → clear error, not a silent empty result.
        assert!(matches!(
            reader.search(&q("nope:x"), 10).unwrap_err(),
            IndexError::UnknownField(f) if f == "nope"
        ));
        // The stored key field is not searchable either.
        assert!(matches!(
            reader.search(&q("_key:x"), 10).unwrap_err(),
            IndexError::UnknownField(_)
        ));
    }

    // ---- task-21: the full query-type family ----------------------------------

    /// A reader over the standard `batch()` (ids 1 "quick brown fox", 2 "lazy brown
    /// dog", 3 "cats").
    fn reader_over_batch(dir: &std::path::Path) -> SegmentReader {
        let schema = IndexSchema::from_resolved(&docs_index());
        TantivySegmentCore.build(&schema, &batch(), dir).unwrap();
        TantivySegmentCore.open(dir).unwrap()
    }

    /// Sorted `id` coordinates of the hits.
    fn ids(hits: &[Hit]) -> Vec<i64> {
        let mut v: Vec<i64> = hits
            .iter()
            .filter_map(|h| match h.key.get("id") {
                Some(Value::Int(i)) => Some(*i),
                _ => None,
            })
            .collect();
        v.sort_unstable();
        v
    }

    #[test]
    fn search_returns_only_cached_fields_on_hits() {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        // body is cached; id is not.
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: body, type: TEXT, cached: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("doc-1"));
        f.insert("body".to_string(), Value::from("hello brown world"));
        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&idx);
        TantivySegmentCore
            .build(
                &schema,
                &DocBatch::new(vec![Document::new(key, f)]),
                dir.path(),
            )
            .unwrap();
        let r = TantivySegmentCore.open(dir.path()).unwrap();

        let hits = r.search(&q("body:brown"), 10).unwrap();
        assert_eq!(hits.len(), 1);
        // The cached `body` value rides along; uncached `id` and the internal key don't.
        assert_eq!(
            hits[0].fields.get("body").unwrap().to_index_string(),
            "hello brown world"
        );
        assert!(!hits[0].fields.contains_key("id"), "id is not cached");
        assert!(
            !hits[0].fields.contains_key(KEY_FIELD),
            "internal key excluded"
        );
    }

    #[test]
    fn full_query_family_executes() {
        let dir = tempfile::tempdir().unwrap();
        let r = reader_over_batch(dir.path());
        let run = |q: &Query| ids(&r.search(q, 10).unwrap());

        // Terms: id IN (1, 3) — exact keyword set membership.
        assert_eq!(
            run(&Query::Terms {
                field: "id".into(),
                values: vec!["1".into(), "3".into()],
            }),
            vec![1, 3]
        );
        // Match OR: brown (1,2) ∪ cats (3); AND: brown ∩ fox (1).
        assert_eq!(
            run(&Query::Match {
                field: Some("body".into()),
                text: "brown cats".into(),
                op: MatchOp::Or,
            }),
            vec![1, 2, 3]
        );
        assert_eq!(
            run(&Query::Match {
                field: Some("body".into()),
                text: "brown fox".into(),
                op: MatchOp::And,
            }),
            vec![1]
        );
        // Phrase "brown fox" (adjacent) → only doc 1.
        assert_eq!(
            run(&Query::Phrase {
                field: Some("body".into()),
                terms: vec!["brown".into(), "fox".into()],
                slop: 0,
            }),
            vec![1]
        );
        // Prefix / wildcard / fuzzy / regex all resolve "brown" → docs 1, 2.
        assert_eq!(
            run(&Query::Prefix {
                field: Some("body".into()),
                prefix: "bro".into(),
            }),
            vec![1, 2]
        );
        assert_eq!(
            run(&Query::Wildcard {
                field: Some("body".into()),
                pattern: "bro*".into(),
            }),
            vec![1, 2]
        );
        assert_eq!(
            run(&Query::Fuzzy {
                field: Some("body".into()),
                value: "brwn".into(),
                distance: 1,
            }),
            vec![1, 2]
        );
        assert_eq!(
            run(&Query::Regex {
                field: Some("body".into()),
                pattern: "br.wn".into(),
            }),
            vec![1, 2]
        );
    }

    #[test]
    fn field_grouped_or_set_matches_end_to_end() {
        // task-247 / issue 3: `field:(a OR b)` used to return 0 hits (the `field:` prefix wasn't
        // distributed over the group). It must now match the union, identically to the expanded
        // `field:a OR field:b`.
        let dir = tempfile::tempdir().unwrap();
        let r = reader_over_batch(dir.path());
        // body: 1="…fox…", 2="…dog…", 3="…cats". `fox OR cats` → docs 1, 3.
        assert_eq!(
            ids(&r.search(&q("body:(fox OR cats)"), 10).unwrap()),
            vec![1, 3]
        );
        assert_eq!(
            ids(&r.search(&q("body:(fox OR cats)"), 10).unwrap()),
            ids(&r.search(&q("body:fox OR body:cats"), 10).unwrap()),
        );
        // KEYWORD set membership via a grouped OR on the exact `id` field → docs 1, 3.
        assert_eq!(ids(&r.search(&q("id:(1 OR 3)"), 10).unwrap()), vec![1, 3]);
    }

    #[test]
    fn cost_guards_reject_leading_wildcard_and_broad_regex() {
        let dir = tempfile::tempdir().unwrap();
        let r = reader_over_batch(dir.path());
        assert!(matches!(
            r.search(
                &Query::Wildcard {
                    field: Some("body".into()),
                    pattern: "*own".into(),
                },
                10
            ),
            Err(IndexError::CostGuard(_))
        ));
        assert!(matches!(
            r.search(
                &Query::Regex {
                    field: Some("body".into()),
                    pattern: ".*".into(),
                },
                10
            ),
            Err(IndexError::CostGuard(_))
        ));
        // Phrase on a KEYWORD field (no positions) is not a positional phrase but an exact-value
        // match — the facet/filter chips emit `field:"value"` for every field type. `id:"1"` matches
        // doc 1 exactly; a value no doc carries matches nothing (clean, not an error).
        assert_eq!(
            ids(&r
                .search(
                    &Query::Phrase {
                        field: Some("id".into()),
                        terms: vec!["1".into()],
                        slop: 0,
                    },
                    10
                )
                .unwrap()),
            vec![1]
        );
        assert!(r
            .search(
                &Query::Phrase {
                    field: Some("id".into()),
                    terms: vec!["1".into(), "2".into()],
                    slop: 0,
                },
                10
            )
            .unwrap()
            .is_empty());
    }

    #[test]
    fn bool_filter_is_non_scoring_and_boost_scales() {
        let dir = tempfile::tempdir().unwrap();
        let r = reader_over_batch(dir.path());

        // Filter-only Bool: constrains to "brown" docs, contributing zero score.
        let filtered = r
            .search(
                &Query::Bool {
                    must: vec![],
                    should: vec![],
                    must_not: vec![],
                    filter: vec![Query::Term {
                        field: Some("body".into()),
                        value: "brown".into(),
                    }],
                },
                10,
            )
            .unwrap();
        assert_eq!(ids(&filtered), vec![1, 2]);
        assert!(
            filtered.iter().all(|h| h.score == 0.0),
            "filter is non-scoring"
        );

        // Boost scales the wrapped query's score by the factor.
        let term = Query::Term {
            field: Some("body".into()),
            value: "fox".into(),
        };
        let base = r.search(&term, 10).unwrap();
        let boosted = r
            .search(
                &Query::Boost {
                    query: Box::new(term.clone()),
                    boost: 4.0,
                },
                10,
            )
            .unwrap();
        assert!((boosted[0].score - base[0].score * 4.0).abs() < 1e-3);
    }

    #[test]
    fn exists_matches_only_docs_carrying_the_field() {
        // A `tag` keyword fast field present on doc 1, absent on doc 2.
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("tag", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: tag, type: KEYWORD, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();

        let mk = |id: &str, tag: Option<&str>| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            if let Some(t) = tag {
                f.insert("tag".to_string(), Value::from(t));
            }
            Document::new(key, f)
        };
        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&idx);
        TantivySegmentCore
            .build(
                &schema,
                &DocBatch::new(vec![mk("a", Some("x")), mk("b", None)]),
                dir.path(),
            )
            .unwrap();
        let r = TantivySegmentCore.open(dir.path()).unwrap();

        let hits = r
            .search(
                &Query::Exists {
                    field: "tag".into(),
                },
                10,
            )
            .unwrap();
        let tagged: Vec<String> = hits
            .iter()
            .map(|h| h.key.get("id").unwrap().to_index_string())
            .collect();
        assert_eq!(tagged, vec!["a"]);
        // Exists on a missing field is a clear validation error.
        assert!(matches!(
            r.search(
                &Query::Exists {
                    field: "nope".into()
                },
                10
            ),
            Err(IndexError::UnknownField(_))
        ));
    }

    #[test]
    fn parsed_query_strings_execute() {
        // task-22 → task-21: a wildcard / phrase / boolean *string* parses to the AST
        // and runs end to end over the corpus (ids 1,2 "brown").
        let dir = tempfile::tempdir().unwrap();
        let r = reader_over_batch(dir.path());
        assert_eq!(ids(&r.search(&q("body:bro*"), 10).unwrap()), vec![1, 2]);
        assert_eq!(
            ids(&r.search(&q(r#"body:"brown fox""#), 10).unwrap()),
            vec![1]
        );
        assert_eq!(
            ids(&r.search(&q("body:brown AND body:dog"), 10).unwrap()),
            vec![2]
        );
    }

    #[test]
    fn range_and_ip_cidr_execute() {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("count", SourceType::Long),
                SourceField::new("addr", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: count, type: LONG }, { path: addr, type: IP } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let mk = |id: &str, count: i64, addr: &str| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("count".to_string(), Value::Int(count));
            f.insert("addr".to_string(), Value::from(addr));
            Document::new(key, f)
        };
        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&idx);
        TantivySegmentCore
            .build(
                &schema,
                &DocBatch::new(vec![
                    mk("a", 5, "10.0.0.1"),
                    mk("b", 15, "10.1.2.3"),
                    mk("c", 25, "192.168.0.1"),
                ]),
                dir.path(),
            )
            .unwrap();
        let r = TantivySegmentCore.open(dir.path()).unwrap();
        let sids = |hits: &[Hit]| {
            let mut v: Vec<String> = hits
                .iter()
                .map(|h| h.key.get("id").unwrap().to_index_string())
                .collect();
            v.sort();
            v
        };

        // count in [10 TO 20] → only b (15).
        let in_range = Query::Range {
            field: "count".into(),
            lower: Some("10".into()),
            lower_inclusive: true,
            upper: Some("20".into()),
            upper_inclusive: true,
        };
        assert_eq!(sids(&r.search(&in_range, 10).unwrap()), vec!["b"]);

        // count > 10 (exclusive lower, unbounded upper) → b, c.
        let gt = Query::Range {
            field: "count".into(),
            lower: Some("10".into()),
            lower_inclusive: false,
            upper: None,
            upper_inclusive: true,
        };
        assert_eq!(sids(&r.search(&gt, 10).unwrap()), vec!["b", "c"]);

        // addr in 10.0.0.0/8 → a, b (not 192.168.x).
        let cidr = Query::IpCidr {
            field: "addr".into(),
            cidr: "10.0.0.0/8".into(),
        };
        assert_eq!(sids(&r.search(&cidr, 10).unwrap()), vec!["a", "b"]);

        // ip_cidr on a non-IP field, and a non-numeric range bound → type errors.
        assert!(matches!(
            r.search(
                &Query::IpCidr {
                    field: "count".into(),
                    cidr: "10.0.0.0/8".into(),
                },
                10
            ),
            Err(IndexError::QueryType(_))
        ));
        assert!(matches!(
            r.search(
                &Query::Range {
                    field: "count".into(),
                    lower: Some("oops".into()),
                    lower_inclusive: true,
                    upper: None,
                    upper_inclusive: true,
                },
                10
            ),
            Err(IndexError::QueryType(_))
        ));
    }
}
