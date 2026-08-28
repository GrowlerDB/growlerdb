//! Segment build over [Tantivy] — the `SegmentCore` seam ([wiki 05]).
//!
//! [`TantivySegmentCore`] builds a [`DocBatch`] into an immutable on-disk segment set and reopens
//! it for BM25 search. TEXT fields are analyzed (default tokenizer + lowercasing), KEYWORD raw, and
//! the [`CompositeKey`] is stored per doc so every hit carries its hydration coordinates.
//!
//! [Tantivy]: https://github.com/quickwit-oss/tantivy
//! [wiki 05]: ../../../okf/system/decisions/d22-search-core.md

use std::net::{IpAddr, Ipv6Addr};
use std::ops::Bound;
use std::path::{Path, PathBuf};

use crate::completion::{
    CompletionBuilder, SegmentCompletion, COMPLETION_PREFIX_DEPTH, COMPLETION_SUFFIX,
};
use crate::vector::{SegmentAnn, StoredAnnIndex, ANN_SUFFIX};

use growlerdb_core::{
    sort_has_score, CompositeKey, DocBatch, Document, FieldType, Highlight, HighlightFragment,
    HighlightSegment, Hit, LocationStrategy, MatchOp, Query, ResolvedField, ResolvedIndex,
    SearchAfter, Sort, SortOrder, SortValue, TextRecord, TimeFormat, Value as GValue,
    SCORE_SORT_KEY,
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
    DateTime, DocAddress, DocSet, Index, IndexReader, ReloadPolicy, TantivyDocument, Term,
    TERMINATED,
};

/// Stored field holding a doc's `enc(CompositeKey)` bytes — hit identity, rebuilt via
/// [`CompositeKey::decode`]. Same encoding as [`KEY_ENC_FIELD`].
pub const KEY_FIELD: &str = "_key";
/// Bytes-indexed `enc(CompositeKey)` field — delete-by-key plus the agg liveness keyset
/// exclusion. Not stored (`_key` carries the same bytes for hits).
const KEY_ENC_FIELD: &str = "_keyenc";
/// u64 fast field holding a doc's **locator ID** — the reference layer of the [D30] layered
/// locator. Indexes the shard's dense location array ([`crate::location`]) → the row's current
/// `(file_id, row_position)`. Written on every upsert.
///
/// [D30]: ../../../okf/system/decisions/d30-layered-locator.md
pub const LOC_ID_FIELD: &str = "_locid";

/// Writer heap budget.
pub const WRITER_HEAP_BYTES: usize = 50_000_000;

/// Separator between a variant flatten leaf's dotted path and its value in the `<column>#terms`
/// keyword token (D47). SOH (`0x01`) never appears in a path or rendered scalar, so the
/// `path`/`value` split is unambiguous.
const FLATTEN_TERM_SEP: char = '\u{1}';

/// The reserved Tantivy field name for a variant column's flatten **term** index (`path = value`
/// keyword tokens). The `#` is rejected in any user-declared variant path
/// ([`DefError::VariantReservedName`](growlerdb_core::DefError)), so this never collides.
fn flatten_terms_field_name(column: &str) -> String {
    format!("{column}#terms")
}

/// The reserved Tantivy field name for a variant column's analyzed TEXT **catch-all** (full-text
/// over the value's string leaves). Also the field a bare `<column>:query` full-text hits.
fn flatten_text_field_name(column: &str) -> String {
    format!("{column}#text")
}

/// Encode a flatten leaf `(path, value)` into its `<column>#terms` keyword token.
fn flatten_token(path: &str, value: &GValue) -> String {
    format!("{path}{FLATTEN_TERM_SEP}{}", value.to_index_string())
}

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

    /// Reading or writing a per-segment ANN sidecar (or any segment-adjacent file) failed.
    #[error("segment io: {0}")]
    Io(#[from] std::io::Error),

    /// An ANN sidecar could not be parsed (bad frame / postcard decode).
    #[error("ann sidecar: {0}")]
    Vector(#[from] crate::vector::VectorIndexError),

    /// A completion sidecar could not be parsed (bad frame / postcard decode).
    #[error("completion sidecar: {0}")]
    Sidecar(String),
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
    /// (path, tantivy field, type, declared timestamp format) per mapped field, in definition
    /// order. The [`TimeFormat`] is set only for timestamp fields; it tells [`add_typed_value`] to
    /// normalize the source epoch to canonical micros at build.
    fields: Vec<(String, Field, FieldType, Option<TimeFormat>)>,
    /// The fields a query can sort by — numeric/date/keyword fields declared `fast`, precomputed
    /// here (the tuple above drops `fast`). See [`sort_fields`](Self::sort_fields).
    sortable_fields: Vec<String>,
    /// Every mapped field's describe-facing summary, precomputed at build. See
    /// [`mapped_fields`](Self::mapped_fields).
    mapped_field_summaries: Vec<MappedFieldSummary>,
    /// The tenant-scoping field, if the index is tenant-scoped.
    tenant_field: Option<String>,
    /// The index's **location strategy** (D30). Under [`Predicate`](LocationStrategy::Predicate) the
    /// commit path never populates [`LOC_ID_FIELD`], yet the schema **keeps** the field either way
    /// (see [`from_resolved`](Self::from_resolved)).
    location_strategy: LocationStrategy,
    /// The VECTOR fields (ANN-build inputs), in definition order — each with its stored-bytes handle
    /// and [`VectorSpec`](growlerdb_core::VectorSpec). Empty for a non-vector index (ANN skipped).
    vector_fields: Vec<VectorFieldInfo>,
    /// The `suggest`-flagged KEYWORD/TEXT fields (completion-sidecar inputs) — `(path, handle)` in
    /// definition order. Empty when no field opts in (the completion sidecar is skipped).
    suggest_fields: Vec<(String, Field)>,
    /// The VARIANT columns' flatten fields (D47), keyed by column — the reserved
    /// `<column>#terms` / `<column>#text` handles (each present iff that flatten mode is on).
    variant_fields: Vec<VariantFieldInfo>,
}

/// One VARIANT column's flatten index handles ([`IndexSchema::variant_fields`]): the reserved
/// `<column>#terms` keyword field and/or the `<column>#text` analyzed catch-all, whichever the
/// [`FlattenConfig`](growlerdb_core::FlattenConfig) enabled.
struct VariantFieldInfo {
    /// The variant column name, e.g. `payload`.
    column: String,
    /// The `<column>#terms` keyword field — present iff flatten `terms` is on.
    terms: Option<Field>,
    /// The `<column>#text` analyzed catch-all — present iff flatten `text` is on.
    text: Option<Field>,
}

/// One VECTOR field's ANN-build inputs: path, stored-bytes handle, and the full
/// [`VectorSpec`](growlerdb_core::VectorSpec) (ANN build reads `dims`/`metric`; the query path
/// reads the whole spec to embed with the field's configured embedder).
struct VectorFieldInfo {
    path: String,
    field: Field,
    spec: growlerdb_core::VectorSpec,
}

/// One mapped field's describe-facing summary — name, type, and what a query can do with it
/// (`fast` = range/sort/aggregate, `indexed` = term-queryable, `cached` = returned with hits).
/// Surfaced by the describe/stats path so clients compose valid queries from the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedFieldSummary {
    /// Dotted field path.
    pub name: String,
    /// The GrowlerDB type name: `TEXT | KEYWORD | LONG | DOUBLE | BOOL | DATE | IP | VECTOR`.
    pub ty: String,
    pub fast: bool,
    pub indexed: bool,
    pub cached: bool,
    /// Whether the field has a prefix-completion sidecar (`/v1/suggest` fast path).
    pub suggest: bool,
}

/// A VECTOR field's describe-facing summary: field path plus the embedding config a console needs
/// to offer semantic/hybrid search (`source_field`, `model`, `dims`) — the owned, public
/// projection of the private [`VectorFieldInfo`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorFieldSummary {
    /// The VECTOR field path — what a semantic/hybrid request targets.
    pub name: String,
    /// The text field whose value is embedded to produce this vector.
    pub source_field: String,
    /// Embedding model id.
    pub model: String,
    /// Embedding dimensionality (vector length).
    pub dims: usize,
}

impl IndexSchema {
    /// Derive a Tantivy schema from a resolved index definition.
    ///
    /// TEXT → analyzed full-text; KEYWORD → raw; LONG/DOUBLE/BOOL/DATE/IP → typed,
    /// indexed (range-queryable) columns. The `fast` flag adds a columnar fast field
    /// (sort/filter/aggregate); `cached` stores the value for return with the hit (D23).
    /// The composite key is added as a `STORED`-only bytes field — the compact
    /// `enc(key)`.
    pub fn from_resolved(idx: &ResolvedIndex) -> Self {
        let mut builder = Schema::builder();
        let key_field = builder.add_bytes_field(KEY_FIELD, STORED);
        let key_enc_field = builder.add_bytes_field(KEY_ENC_FIELD, INDEXED);
        let mut fields = Vec::with_capacity(idx.fields.len());
        // Collected here while the `fast`/`cached` capability flags are still in scope.
        let mut sortable_fields = Vec::new();
        let mut mapped_field_summaries = Vec::with_capacity(idx.fields.len());
        let mut vector_fields = Vec::new();
        let mut suggest_fields = Vec::new();
        let mut variant_fields = Vec::new();
        for f in &idx.fields {
            // A VARIANT field carries no single typed Tantivy field — only its flatten index
            // (`<col>#terms` + optional `<col>#text`). Its declared shape leaves are separate
            // `ResolvedField`s handled by the normal arms.
            if f.ty == FieldType::Variant {
                let Some(v) = &f.variant else { continue };
                let terms = v.flatten.enabled && v.flatten.terms;
                let text = v.flatten.enabled && v.flatten.text;
                let terms_field = terms.then(|| {
                    // Raw, un-analyzed, indexed — exact `path=value` token match. Not fast/stored.
                    builder.add_text_field(&flatten_terms_field_name(&f.path), STRING)
                });
                let text_field = text.then(|| {
                    let indexing = TextFieldIndexing::default()
                        .set_tokenizer("default")
                        .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                        .set_fieldnorms(true);
                    builder.add_text_field(
                        &flatten_text_field_name(&f.path),
                        TextOptions::default().set_indexing_options(indexing),
                    )
                });
                variant_fields.push(VariantFieldInfo {
                    column: f.path.clone(),
                    terms: terms_field,
                    text: text_field,
                });
                mapped_field_summaries.push(MappedFieldSummary {
                    name: f.path.clone(),
                    ty: "VARIANT".to_string(),
                    fast: false,
                    // "indexed" for a variant = queryable via the flatten index.
                    indexed: terms || text,
                    cached: false,
                    suggest: false,
                });
                continue;
            }
            if f.fast
                && matches!(
                    f.ty,
                    FieldType::Long | FieldType::Double | FieldType::Date | FieldType::Keyword
                )
            {
                sortable_fields.push(f.path.clone());
            }
            let handle = match f.ty {
                FieldType::Text => {
                    // Record level drives positions (the phrase-query slice, usually the largest
                    // part of the inverted index); fieldnorms drive BM25 length normalization.
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
                // A vector is stored as raw LE-`f32` bytes: STORED so it round-trips per-doc,
                // FAST so the ANN build can read it columnar. Not sortable.
                FieldType::Vector => builder.add_bytes_field(&f.path, STORED | FAST),
                // Handled above (its flatten fields are added, then `continue`) — never reached.
                FieldType::Variant => unreachable!("variant field handled before the type match"),
            };
            if f.ty == FieldType::Vector {
                if let Some(spec) = &f.vector {
                    vector_fields.push(VectorFieldInfo {
                        path: f.path.clone(),
                        field: handle,
                        spec: spec.clone(),
                    });
                }
            }
            fields.push((f.path.clone(), handle, f.ty, f.format));
            // Only KEYWORD/TEXT fields have a term dictionary to complete over; `suggest` on any
            // other type is ignored (no sidecar), per the graceful-ignore contract.
            let suggest = f.suggest && matches!(f.ty, FieldType::Keyword | FieldType::Text);
            if suggest {
                suggest_fields.push((f.path.clone(), handle));
            }
            mapped_field_summaries.push(MappedFieldSummary {
                name: f.path.clone(),
                ty: format!("{:?}", f.ty).to_uppercase(),
                fast: f.fast,
                indexed: f.indexed,
                cached: f.cached,
                suggest,
            });
        }
        // Added after the mapped fields so internal handles never shift a user field's ordinal.
        // Declared for **every** strategy (a `PREDICATE` index just never populates it) so the
        // schema shape is identical across strategies — avoiding the field-ordinal hazard.
        let loc_id_field = builder.add_u64_field(LOC_ID_FIELD, FAST);
        Self {
            schema: builder.build(),
            key_field,
            key_enc_field,
            loc_id_field,
            fields,
            sortable_fields,
            mapped_field_summaries,
            tenant_field: idx.tenant_field().map(str::to_string),
            location_strategy: idx.location_strategy,
            vector_fields,
            suggest_fields,
            variant_fields,
        }
    }

    /// Whether this index has any VECTOR field — i.e. whether a per-segment ANN sidecar is built.
    /// When false the commit/compaction paths skip the ANN build entirely.
    pub fn has_vector_fields(&self) -> bool {
        !self.vector_fields.is_empty()
    }

    /// Whether any field opts into a prefix-completion sidecar — i.e. whether the commit/compaction
    /// paths build a per-segment `<uuid>.cmp`. False → the completion build is skipped entirely.
    pub fn has_suggest_fields(&self) -> bool {
        !self.suggest_fields.is_empty()
    }

    /// Whether `name` is a `suggest`-flagged KEYWORD/TEXT field — the gate the suggest path checks
    /// before attempting the completion-sidecar fast path.
    pub fn is_suggest_field(&self, name: &str) -> bool {
        self.suggest_fields.iter().any(|(p, _)| p == name)
    }

    /// Whether this index maps an Iceberg v3 **variant** column (D47) — the cheap, schema-only
    /// counterpart of [`ResolvedIndex::has_variant_field`](growlerdb_core::ResolvedIndex::has_variant_field)
    /// for the hydration fork on paths that hold only the [`IndexSchema`].
    pub fn has_variant_fields(&self) -> bool {
        !self.variant_fields.is_empty()
    }

    /// The index's [location strategy](LocationStrategy) (D30) — how the store's commit
    /// path and the engine's hydration path locate source rows.
    pub fn location_strategy(&self) -> LocationStrategy {
        self.location_strategy
    }

    /// The tenant-scoping field, if this index is tenant-scoped — the field reads inject
    /// a mandatory `= <verified claim>` filter on.
    pub fn tenant_field(&self) -> Option<&str> {
        self.tenant_field.as_deref()
    }

    /// The [`VectorSpec`](growlerdb_core::VectorSpec) of the VECTOR field named `path`, or `None`
    /// if `path` is not a VECTOR field. The semantic-search path reads this to embed a query in the
    /// same space its documents were embedded in at ingest.
    pub fn vector_spec(&self, path: &str) -> Option<&growlerdb_core::VectorSpec> {
        self.vector_fields
            .iter()
            .find(|vf| vf.path == path)
            .map(|vf| &vf.spec)
    }

    /// The mapped **DATE** fields, in definition order — the columns a console time filter can
    /// range-scope a query on. Stored as canonical **epoch microseconds**.
    pub fn date_fields(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|(_, _, ty, _)| *ty == FieldType::Date)
            .map(|(path, _, _, _)| path.as_str())
            .collect()
    }

    /// The fields a query can **sort** by — numeric/date/keyword fields declared `fast`, in
    /// definition order. Exactly matches the sort path's `ensure_sortable` check, so the console
    /// only ever offers a sort field the engine accepts.
    pub fn sort_fields(&self) -> Vec<&str> {
        self.sortable_fields.iter().map(String::as_str).collect()
    }

    /// Every mapped field's describe-facing summary, in definition order — the full schema the
    /// describe path surfaces so clients compose valid queries from it.
    pub fn mapped_fields(&self) -> Vec<MappedFieldSummary> {
        self.mapped_field_summaries.clone()
    }

    /// The index's VECTOR fields, in definition order — each a [`VectorFieldSummary`] (path +
    /// `source_field`/`model`/`dims`) for the describe path's semantic/hybrid picker. Empty for a
    /// non-vector index.
    pub fn vector_fields(&self) -> Vec<VectorFieldSummary> {
        self.vector_fields
            .iter()
            .map(|vf| VectorFieldSummary {
                name: vf.path.clone(),
                source_field: vf.spec.source_field.clone(),
                model: vf.spec.model.clone(),
                dims: vf.spec.dims,
            })
            .collect()
    }

    /// The bytes-indexed `enc(key)` field — used to **delete by key** and for the agg keyset
    /// exclusion.
    pub fn key_enc_field(&self) -> Field {
        self.key_enc_field
    }

    /// The u64 FAST **locator-ID** field ([`LOC_ID_FIELD`], D30 reference layer) — the store
    /// attaches each upsert's location-array id through this handle.
    pub fn loc_id_field(&self) -> Field {
        self.loc_id_field
    }

    /// Build the [`TantivyDocument`] for `doc`: the stored + indexed `enc(key)` and each mapped
    /// field's typed value (skipping absent fields).
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
        // Variant flatten leaves (D47): each `(path, value)` → a `path=value` token in
        // `<col>#terms`; each string leaf also feeds the analyzed `<col>#text` catch-all. Declared
        // shape leaves already rode `doc.fields` above.
        for vc in &doc.variants {
            let Some(info) = self.variant_fields.iter().find(|v| v.column == vc.field) else {
                continue; // a variant column not in this schema — ignore its leaves
            };
            for (path, value) in &vc.leaves {
                if let Some(terms) = info.terms {
                    td.add_text(terms, flatten_token(path, value));
                }
                if let Some(text) = info.text {
                    if let GValue::Str(s) = value {
                        td.add_text(text, s);
                    }
                }
            }
        }
        td
    }

    /// The underlying Tantivy schema.
    pub fn tantivy_schema(&self) -> &Schema {
        &self.schema
    }
}

/// The Tantivy [`IndexRecordOption`] for a TEXT field's [`TextRecord`] level.
fn record_option(record: TextRecord) -> IndexRecordOption {
    match record {
        TextRecord::Basic => IndexRecordOption::Basic,
        TextRecord::Freq => IndexRecordOption::WithFreqs,
        TextRecord::Position => IndexRecordOption::WithFreqsAndPositions,
    }
}

/// `NumericOptions` for a LONG/DOUBLE/BOOL field per its `indexed`/`fast`/`cached` flags. A
/// **fast-only** field carries no inverted index: range, exact-match, sort/search-after, and exists
/// all run on the columnar store, so postings + term dict would be dead weight.
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

/// Add a wire [`Value`](growlerdb_core::Value) to the document as the field's typed Tantivy value.
/// A value whose kind doesn't match the field type is **skipped** (the doc still indexes its other
/// fields) rather than failing the batch — source-type validation happens at resolve time.
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
        // Dates are stored as canonical epoch **microseconds**. A `format` field carries its source
        // in another epoch unit, so normalize here (an unparseable value is skipped, not wedged).
        // A format-less field already arrives as canonical micros (`Ts`, or a pre-parsed `Int`).
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
        // Vectors are stored as raw LE-`f32` bytes (see [`vec_f32_to_le_bytes`]).
        FieldType::Vector => {
            if let V::Vector(v) = value {
                td.add_bytes(field, &vec_f32_to_le_bytes(v));
            }
        }
        // A VARIANT field never appears in `self.fields` (its leaves are indexed in `to_tantivy`);
        // a declared shape leaf reaches here under its own concrete type, not `Variant`.
        FieldType::Variant => {}
    }
}

/// Encode a dense vector as its raw little-endian `f32` bytes (4 bytes/element) — the
/// stored form of a VECTOR field. Inverse of [`le_bytes_to_vec_f32`].
fn vec_f32_to_le_bytes(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Decode raw little-endian `f32` bytes back into a vector — inverse of [`vec_f32_to_le_bytes`]. A
/// trailing partial element (length not a multiple of 4) is dropped.
fn le_bytes_to_vec_f32(b: &[u8]) -> Vec<f32> {
    b.chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect()
}

/// The file name of a segment's ANN sidecar: `<segment-uuid>.ann`, beside the lexical segment
/// files in the index directory.
fn ann_sidecar_name(segment_uuid: &str) -> String {
    format!("{segment_uuid}.{ANN_SUFFIX}")
}

/// The file name of a segment's completion sidecar: `<segment-uuid>.cmp`, beside the lexical segment
/// files. One file per segment holds every `suggest` field's prefix table.
pub fn completion_sidecar_name(segment_uuid: &str) -> String {
    format!("{segment_uuid}.{COMPLETION_SUFFIX}")
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
    /// The settings a **new** index is created with: zstd doc-store compression. lz4 (the default)
    /// only match-copies, so high-entropy stored values (hex/UUID keys) pass through nearly
    /// uncompressed; zstd entropy-codes them (~2x on hex). The compressor persists in `meta.json`,
    /// so an existing index keeps whatever it was created with.
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
        // Build the per-segment ANN + completion sidecar(s) over the just-committed segment.
        if schema.has_vector_fields() || schema.has_suggest_fields() {
            let reader = self.open(dir)?;
            reader.build_ann_sidecars(schema, dir)?;
            reader.build_completion_sidecars(schema, dir)?;
        }
        Ok(batch.docs.len() as u64)
    }

    /// Reopen a previously built segment set for reading.
    pub fn open(&self, dir: &Path) -> Result<SegmentReader> {
        let index = Index::open_in_dir(dir)?;
        let reader = index.reader()?;
        Ok(SegmentReader {
            index,
            reader,
            index_dir: Some(dir.to_path_buf()),
        })
    }

    /// Open the shard's **single** Tantivy index at `dir`, creating it empty if absent. All commits
    /// add segments to this one index; compaction is `IndexWriter::merge` over its segments. An
    /// existing index opens with the settings persisted in its `meta.json`; only a fresh create gets
    /// [`new_index_settings`](Self::new_index_settings).
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

/// Rebuild a hit's [`CompositeKey`] from its stored `_key` bytes — the strict inverse of
/// the `enc(key)` written at index time.
fn stored_key(doc: &TantivyDocument, key_field: Field) -> Result<CompositeKey> {
    let bytes = doc
        .get_first(key_field)
        .and_then(|v| v.as_bytes())
        .ok_or(IndexError::MissingKey)?;
    Ok(CompositeKey::decode(bytes)?)
}

/// The result of explaining one document's score for a query: Tantivy's BM25
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
    /// The local directory holding the index's files (and the `<segment-uuid>.ann` sidecars), when
    /// known. `None` for a cold read-through object directory, where KNN finds no local sidecar and
    /// returns no vector hits.
    index_dir: Option<PathBuf>,
}

impl SegmentReader {
    /// A read handle over `index` that **auto-reloads on commit** — the shard's live reader; reads
    /// see each commit's new segment (and its native deletes).
    pub fn live(index: &Index, index_dir: impl Into<PathBuf>) -> Result<Self> {
        Ok(SegmentReader {
            index: index.clone(),
            reader: index.reader()?,
            index_dir: Some(index_dir.into()),
        })
    }

    /// A read handle **pinned** to `index`'s current commit — never reloads, so its searcher is a
    /// stable snapshot and Tantivy keeps the referenced segment files alive even as later
    /// commits/compaction run.
    pub fn snapshot(index: &Index, index_dir: impl Into<PathBuf>) -> Result<Self> {
        Ok(SegmentReader {
            index: index.clone(),
            reader: index
                .reader_builder()
                .reload_policy(ReloadPolicy::Manual)
                .try_into()?,
            index_dir: Some(index_dir.into()),
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

    /// The **locator ID** (`_locid` fast field) of the live doc carrying `enc(key)`, or `None` when
    /// no live doc has the key. The D30 write path's pre-commit **reuse lookup** — a key-term probe
    /// per segment + one fast-field read — which keeps the location array O(live keys), not O(all
    /// versions ever written).
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
            // Defensive: a segment with no `_locid` column can't contribute an id (should be
            // unreachable — every upsert writes the field).
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

    /// The [`DocAddress`](tantivy::DocAddress) of the live doc carrying `key_enc`, or `None` when no
    /// live doc has the key — the same identity-layer resolution [`live_loc_id`](Self::live_loc_id)
    /// does, but exposing the address so a caller reads arbitrary **fast fields** (the sort-key prune
    /// hints), not just `_locid`. The address is only valid for `searcher` (segment ords align).
    fn live_doc_address(
        &self,
        searcher: &tantivy::Searcher,
        key_enc: &[u8],
    ) -> Result<Option<tantivy::DocAddress>> {
        let schema = self.index.schema();
        let Ok(key_enc_field) = schema.get_field(KEY_ENC_FIELD) else {
            return Ok(None);
        };
        let term = Term::from_field_bytes(key_enc_field, key_enc);
        for (seg_ord, segment) in searcher.segment_readers().iter().enumerate() {
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
                    return Ok(Some(tantivy::DocAddress::new(seg_ord as u32, doc)));
                }
                doc = postings.advance();
            }
        }
        Ok(None)
    }

    /// Read each key's live-doc **fast values** for `fields` — the sort-key **prune hints** hydration
    /// AND-s onto the pass-2 predicate so a sorted source table prunes by manifest min/max. For each
    /// key: resolve its live doc ([`live_doc_address`](Self::live_doc_address)) then read each field's
    /// fast value, typed to the field's mapping. A key with no live doc, or a field that isn't
    /// fast/sortable, simply contributes nothing (never an error — the predicate is a pure prune).
    /// Returns a vec aligned 1:1 with `keys`.
    pub fn fast_values(
        &self,
        keys: &[CompositeKey],
        fields: &[String],
    ) -> Result<Vec<Vec<(String, GValue)>>> {
        let searcher = self.reader.searcher();
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            let mut row = Vec::new();
            if let Some(address) = self.live_doc_address(&searcher, &key.encode())? {
                for field in fields {
                    let Ok((_, ftype)) = self.resolve_typed_field(field) else {
                        continue; // unknown field — no hint
                    };
                    let Ok(sv) = self.fast_value(&searcher, address, field) else {
                        continue; // not a fast/sortable field — no hint
                    };
                    if let Some(v) = sort_value_to_value(&ftype, &sv) {
                        row.push((field.clone(), v));
                    }
                }
            }
            out.push(row);
        }
        Ok(out)
    }

    /// Whether any **live** doc carries `enc(key)` — a postings probe filtered by each segment's
    /// alive bitset. Unlike a raw term lookup this never counts a deleted-but-unmerged doc (under
    /// `NoMergePolicy` term dictionaries retain superseded/deleted keys until compaction).
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

    /// Enumerate the **live-key set** under a raw-bytes `prefix` of the `_keyenc` term dictionary
    /// (D30). `enc(CompositeKey)` is partition-first and length-prefixed, so a partition's keys form
    /// one contiguous byte-prefix range: streaming from `prefix` and stopping at the first
    /// non-matching term preserves partition scoping exactly (empty prefix → whole shard).
    ///
    /// A key is counted only if it has a **live** doc (postings walk + alive bitset): under
    /// `NoMergePolicy` the dictionary retains deleted-but-unmerged keys, so raw enumeration would
    /// over-report. A key can appear in several segments (superseded versions); the result is
    /// deduplicated so a key counts once.
    ///
    /// Cost: O(terms in range) streaming + one postings probe per candidate term, O(live keys)
    /// memory for the result.
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

    /// Read the **cached** (stored) display fields of `doc` into a value map (D23) — every stored
    /// field except the internal key, typed back to a wire [`Value`](growlerdb_core::Value). These
    /// ride along on each [`Hit`] so a page renders without hydration.
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
    /// store to merge/finalize. Under the single-index-per-shard model, Tantivy's own delete
    /// handling already excludes superseded/deleted docs, so there is no tombstone exclusion to
    /// apply here.
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

    /// Count the documents matching `query` — the **live match total** (the single index natively
    /// excludes superseded/deleted docs). No scoring/sorting/materialization; the search response's
    /// `total`. Validates fields like [`search`](Self::search), so a bad query errors clearly.
    pub fn count(&self, query: &Query) -> Result<u64> {
        // A top-level KNN query has no Tantivy representation — count its resolved neighbors.
        if let Query::Knn {
            field,
            vector,
            k,
            filter,
        } = query
        {
            return Ok(self.knn_search(field, vector, *k, filter.as_deref())?.len() as u64);
        }
        let tantivy_query = self.build(query)?;
        let searcher = self.reader.searcher();
        Ok(searcher.search(tantivy_query.as_ref(), &Count)? as u64)
    }

    /// Build the **per-segment ANN sidecar(s)** ([D19]) for every VECTOR field in `schema`, writing
    /// `<segment-uuid>.ann` beside each Tantivy segment in `dir`. Idempotent (an existing sidecar is
    /// skipped), so it can run after every commit. A sidecar covers **all** docs in a segment
    /// including deleted ones — deletes only write a `.del`, so [`knn_search`](Self::knn_search)
    /// filters by the live alive-bitset at query time instead.
    ///
    /// [D19]: ../../../okf/system/decisions/d19-ann-library.md
    pub fn build_ann_sidecars(&self, schema: &IndexSchema, dir: &Path) -> Result<()> {
        if schema.vector_fields.is_empty() {
            return Ok(());
        }
        let searcher = self.reader.searcher();
        for (seg_ord, seg) in searcher.segment_readers().iter().enumerate() {
            let path = dir.join(ann_sidecar_name(&seg.segment_id().uuid_string()));
            if path.exists() {
                continue; // already built (content-stable per segment id)
            }
            let mut sidecar = SegmentAnn::new();
            for vf in &schema.vector_fields {
                let mut items: Vec<(u32, Vec<f32>)> = Vec::new();
                for doc_id in 0..seg.max_doc() {
                    let doc: TantivyDocument =
                        searcher.doc(DocAddress::new(seg_ord as u32, doc_id))?;
                    if let Some(bytes) = doc.get_first(vf.field).and_then(|v| v.as_bytes()) {
                        let v = le_bytes_to_vec_f32(bytes);
                        if !v.is_empty() {
                            items.push((doc_id, v));
                        }
                    }
                }
                if !items.is_empty() {
                    // Auto-selects brute-force vs HNSW by this segment's vector count for the field.
                    sidecar.insert(
                        vf.path.clone(),
                        &StoredAnnIndex::build(vf.spec.dims, vf.spec.metric, &items),
                    );
                }
            }
            if !sidecar.is_empty() {
                std::fs::write(&path, sidecar.to_frame())?;
            }
        }
        Ok(())
    }

    /// Per-field count of vectors present in the live segments' ANN sidecars — the KNN coverage
    /// numerator the describe path pairs with `num_docs`. A shortfall means documents were indexed
    /// **without** an embedding (e.g. an ingest-time embed failure) and are invisible to semantic
    /// search — a gap nothing else surfaces. Counts sidecar entries, so recently deleted docs may
    /// still be included until compaction; a segment with no sidecar contributes 0.
    pub fn vector_coverage(&self, field: &str) -> Result<u64> {
        let Some(dir) = self.index_dir.as_ref() else {
            return Ok(0); // no local sidecar directory (e.g. cold read-through)
        };
        let searcher = self.reader.searcher();
        let mut vectors = 0u64;
        for seg in searcher.segment_readers() {
            let path = dir.join(ann_sidecar_name(&seg.segment_id().uuid_string()));
            let Ok(bytes) = std::fs::read(&path) else {
                continue; // no sidecar for this segment
            };
            if let Some(index) = SegmentAnn::from_frame(&bytes)?.field(field) {
                vectors += index.len() as u64;
            }
        }
        Ok(vectors)
    }

    /// Build the **per-segment completion sidecar(s)** for every `suggest` field in `schema`, writing
    /// `<segment-uuid>.cmp` beside each Tantivy segment in `dir`. Idempotent (an existing sidecar is
    /// skipped), so it can run after every commit; rebuilt for a new (merged) segment id on
    /// compaction, mirroring [`build_ann_sidecars`](Self::build_ann_sidecars). Frequencies are the
    /// dictionary's `doc_freq` (not liveness-filtered) — the accepted suggester contract.
    pub fn build_completion_sidecars(&self, schema: &IndexSchema, dir: &Path) -> Result<()> {
        if schema.suggest_fields.is_empty() {
            return Ok(());
        }
        let searcher = self.reader.searcher();
        for seg in searcher.segment_readers() {
            let path = dir.join(completion_sidecar_name(&seg.segment_id().uuid_string()));
            if path.exists() {
                continue; // already built (content-stable per segment id)
            }
            let mut sidecar = SegmentCompletion::new();
            for (name, handle) in &schema.suggest_fields {
                let inverted = seg.inverted_index(*handle)?;
                let mut builder = CompletionBuilder::new();
                let mut stream = inverted
                    .terms()
                    .stream()
                    .map_err(|e| IndexError::Tantivy(e.into()))?;
                while stream.advance() {
                    builder.add(stream.key(), stream.value().doc_freq as u64);
                }
                let table = builder.finish();
                if !table.is_empty() {
                    sidecar.insert(name.clone(), table);
                }
            }
            if !sidecar.is_empty() {
                std::fs::write(&path, sidecar.to_frame())?;
            }
        }
        Ok(())
    }

    /// **Prefix autocomplete from the completion sidecar**: the top-`limit` terms of `field` under
    /// `prefix` by descending doc frequency (ties by term ascending) — read from each segment's
    /// precomputed `<uuid>.cmp` instead of scanning the term dictionary. Returns `None` (fall back to
    /// the live seek) when the sidecar can't answer: no local index dir, an empty/over-`P` prefix, or
    /// **any** live segment missing its sidecar or a table for `field` (partial coverage would drop
    /// terms and mis-rank). Same summed-across-segments frequency contract as [`prefix_terms`](
    /// Self::prefix_terms), so a flagged field returns identical results to the live path.
    pub fn suggest_prefix_sidecar(
        &self,
        field: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Option<Vec<(String, u64)>>> {
        let Some(dir) = self.index_dir.as_ref() else {
            return Ok(None); // no local sidecar directory (e.g. cold read-through)
        };
        let (_, is_text) = self.resolve_field(Some(field))?;
        let needle = if is_text {
            prefix.to_lowercase()
        } else {
            prefix.to_string()
        };
        let needle = needle.as_bytes();
        // A too-long prefix already selects few terms (cheap live) and isn't in the table; an empty
        // prefix isn't keyed. Byte length matches the FST's byte-sorted prefix semantics.
        if needle.is_empty() || needle.len() > COMPLETION_PREFIX_DEPTH {
            return Ok(None);
        }
        let searcher = self.reader.searcher();
        let mut totals: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        for seg in searcher.segment_readers() {
            let path = dir.join(completion_sidecar_name(&seg.segment_id().uuid_string()));
            let Ok(bytes) = std::fs::read(&path) else {
                return Ok(None); // a live segment lacks its sidecar — fall back for completeness
            };
            let sidecar = SegmentCompletion::from_frame(&bytes).map_err(IndexError::Sidecar)?;
            let Some(table) = sidecar.field(field) else {
                return Ok(None); // sidecar doesn't cover this field — fall back
            };
            // An absent key means this segment holds no terms under the prefix (contributes nothing);
            // a present key sums into the cross-segment total, matching the live path.
            if let Some(entries) = table.get(needle) {
                for (term, freq) in entries {
                    *totals.entry(term.clone()).or_insert(0) += freq;
                }
            }
        }
        let mut ranked: Vec<(String, u64)> = totals.into_iter().collect();
        ranked.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        ranked.truncate(limit);
        Ok(Some(ranked))
    }

    pub fn knn_search(
        &self,
        field: &str,
        vector: &[f32],
        k: usize,
        filter: Option<&Query>,
    ) -> Result<Vec<Hit>> {
        let schema = self.index.schema();
        let tv_field = schema
            .get_field(field)
            .map_err(|_| IndexError::UnknownField(field.to_string()))?;
        // A VECTOR field is the only bytes field a user names — reject anything else.
        if !matches!(
            schema.get_field_entry(tv_field).field_type(),
            TvFieldType::Bytes(_)
        ) {
            return Err(IndexError::QueryType(format!(
                "field `{field}` is not a VECTOR field — KNN needs a vector field"
            )));
        }
        if k == 0 {
            return Ok(Vec::new());
        }
        let Some(dir) = self.index_dir.as_ref() else {
            return Ok(Vec::new()); // no local sidecar directory (e.g. cold read-through)
        };
        let key_field = schema.get_field(KEY_FIELD)?;
        let searcher = self.reader.searcher();
        // Filtered KNN: compile the sub-query once and collect its matching addresses; a neighbor is
        // admitted only if it is also in this set.
        let allowed: Option<std::collections::HashSet<DocAddress>> = match filter {
            Some(f) => {
                let tantivy_query = self.build(f)?;
                Some(searcher.search(tantivy_query.as_ref(), &DocSetCollector)?)
            }
            None => None,
        };
        let mut hits: Vec<Hit> = Vec::new();
        for (seg_ord, seg) in searcher.segment_readers().iter().enumerate() {
            let path = dir.join(ann_sidecar_name(&seg.segment_id().uuid_string()));
            let Ok(bytes) = std::fs::read(&path) else {
                continue; // no sidecar for this segment
            };
            let Some(index) = SegmentAnn::from_frame(&bytes)?.field(field) else {
                continue; // this segment's sidecar holds no index for the field
            };
            let alive = seg.alive_bitset();
            match allowed.as_ref() {
                // **Filtered KNN is exact** — the tenant-isolation path must not depend on the ANN
                // tier's recall. HNSW returns only ~ef_search candidates, so it could silently drop
                // filter-allowed matches ranking outside that window. Instead score the
                // alive+allowed subset **directly from stored vectors** with the field's metric —
                // exact regardless of tier, and cheap because the filter already limits the set.
                Some(a) => {
                    let metric = index.metric();
                    for address in a.iter().filter(|addr| addr.segment_ord as usize == seg_ord) {
                        let doc_id = address.doc_id;
                        if alive.is_some_and(|b| !b.is_alive(doc_id)) {
                            continue; // deleted/superseded
                        }
                        let doc: TantivyDocument = searcher.doc(*address)?;
                        let Some(bytes) = doc.get_first(tv_field).and_then(|v| v.as_bytes()) else {
                            continue; // this allowed doc has no vector for the field
                        };
                        let v = le_bytes_to_vec_f32(bytes);
                        if v.is_empty() {
                            continue;
                        }
                        hits.push(Hit {
                            key: stored_key(&doc, key_field)?,
                            score: crate::vector::score(metric, vector, &v),
                            fields: self.cached_fields(&doc),
                            highlight: Default::default(),
                        });
                    }
                }
                // Unfiltered fast path: the ANN index ranks; request enough that dropping
                // deleted docs still leaves `k` live ones.
                None => {
                    let want = k.saturating_add(seg.num_deleted_docs() as usize);
                    for (doc_id, score) in index.knn(vector, want) {
                        if alive.is_none_or(|b| b.is_alive(doc_id)) {
                            let address = DocAddress::new(seg_ord as u32, doc_id);
                            let doc: TantivyDocument = searcher.doc(address)?;
                            hits.push(Hit {
                                key: stored_key(&doc, key_field)?,
                                score,
                                fields: self.cached_fields(&doc),
                                highlight: Default::default(),
                            });
                        }
                    }
                }
            }
        }
        // Global top-`k`: descending score, composite key as a stable tiebreaker.
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.key.encode().cmp(&b.key.encode()))
        });
        hits.truncate(k);
        Ok(hits)
    }

    /// Execute a [`Query`] AST as BM25, returning ranked **coordinates + scores**. Validates fields
    /// against the schema (unknown/non-searchable → [`IndexError::UnknownField`]), so a bad query is
    /// a clear error, not a silent empty result.
    pub fn search(&self, query: &Query, k: usize) -> Result<Vec<Hit>> {
        Ok(self
            .search_sorted(query, k, &[], None)?
            .into_iter()
            .map(|(hit, _)| hit)
            .collect())
    }

    /// Execute `query` returning up to `limit` `(hit, sort_values)` pairs. With no `sort` keys the
    /// window is the top-`limit` by descending **score** and `sort_values` is empty. With keys the
    /// window is the top-`limit` by the **primary** key, and each hit carries every key's value (in
    /// key order) from its fast field, so the store can do the full multi-key merge + page across
    /// generations. For key sort `Hit::score` is 0.0.
    pub fn search_sorted(
        &self,
        query: &Query,
        limit: usize,
        sort: &[Sort],
        after: Option<&SearchAfter>,
    ) -> Result<Vec<(Hit, Vec<SortValue>)>> {
        // A top-level KNN query is resolved over the ANN sidecars, not compiled to a Tantivy query.
        // It ranks by KNN score (no field sort or keyset cursor), so hand back score-ranked hits
        // with empty sort values.
        if let Query::Knn {
            field,
            vector,
            k,
            filter,
        } = query
        {
            if !sort.is_empty() || after.is_some() {
                return Err(IndexError::QueryType(
                    "KNN search cannot be combined with a field sort or keyset cursor".into(),
                ));
            }
            let mut khits = self.knn_search(field, vector, *k, filter.as_deref())?;
            khits.truncate(limit);
            return Ok(khits.into_iter().map(|h| (h, Vec::new())).collect());
        }
        // With a keyset cursor, AND the user query with a predicate admitting only docs strictly
        // after the cursor in the total order.
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

        // The window score is the ranking value for an unsorted query or an explicit `_score`
        // primary; for a field-sorted query it's 0.0.
        let by_score = sort.is_empty() || sort.first().is_some_and(Sort::is_score);
        let key_field = self.index.schema().get_field(KEY_FIELD)?;
        let mut out = Vec::with_capacity(window.len());
        for (score, address) in window {
            let doc: TantivyDocument = searcher.doc(address)?;
            let key = stored_key(&doc, key_field)?;
            // Each key's value: the relevance score for a `_score` key, else the fast field.
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
                    highlight: Default::default(),
                },
                sort_values,
            ));
        }
        Ok(out)
    }

    /// **Exhaustively** scan every matching doc (honoring an optional keyset `after`), returning
    /// `(hit, sort_values)` for each. Unlike [`search_sorted`](Self::search_sorted)'s
    /// top-`limit`-by-primary window, this is correct for **multi-key** sort even when many docs tie
    /// on the primary key. `O(matches)`.
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
        // A `_score` key needs each doc's relevance, but the exhaustive collector doesn't score —
        // score per doc via `explain` (the same scorer as search). `_score` rejects keyset paging,
        // so `tantivy_query` here is the unwrapped user query.
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
                    highlight: Default::default(),
                },
                sort_values,
            ));
        }
        Ok(out)
    }

    /// The **highlightable** TEXT field names for a request: the requested `fields`, or — when none
    /// are listed — every stored analyzed TEXT field, in schema order. A requested field that isn't
    /// an analyzed stored TEXT field is silently dropped (highlighting is best-effort).
    fn highlightable_fields(&self, requested: &[String]) -> Vec<(String, Field)> {
        let schema = self.index.schema();
        let is_highlightable = |field: Field| -> bool {
            let entry = schema.get_field_entry(field);
            entry.name() != KEY_FIELD
                && entry.is_stored()
                && matches!(field_kind(&schema, field), Some(true))
        };
        if requested.is_empty() {
            schema
                .fields()
                .filter(|(field, _)| is_highlightable(*field))
                .map(|(field, entry)| (entry.name().to_string(), field))
                .collect()
        } else {
            requested
                .iter()
                .filter_map(|name| {
                    let field = schema.get_field(name).ok()?;
                    is_highlightable(field).then(|| (name.clone(), field))
                })
                .collect()
        }
    }

    /// Fill each hit's per-field [`highlight`](Hit::highlight) from the analyzed match: for every
    /// requested highlightable TEXT field, snippet the hit's own **cached** text with a Tantivy
    /// [`SnippetGenerator`](tantivy::snippet::SnippetGenerator), converting the highlighted ranges
    /// to XSS-safe [segments](HighlightSegment). Bounded by `hl.fragment_size`. A field with no
    /// matching fragment is absent from the hit's map.
    ///
    /// Cost: one generator per requested field (reused across the page) + one snippet per (hit,
    /// field). Off by default.
    pub fn highlight_hits(&self, query: &Query, hits: &mut [Hit], hl: &Highlight) -> Result<()> {
        let fields = self.highlightable_fields(&hl.fields);
        if fields.is_empty() || hits.is_empty() {
            return Ok(());
        }
        let tantivy_query = self.build(query)?;
        let searcher = self.reader.searcher();
        // Build a generator per field once; each snippets the per-hit cached text.
        let mut generators = Vec::with_capacity(fields.len());
        for (name, field) in &fields {
            let mut gen = tantivy::snippet::SnippetGenerator::create(
                &searcher,
                tantivy_query.as_ref(),
                *field,
            )?;
            gen.set_max_num_chars(hl.fragment_size);
            generators.push((name.clone(), gen));
        }
        for hit in hits.iter_mut() {
            for (name, gen) in &generators {
                // Highlight the field's cached text — no doc re-fetch.
                let Some(GValue::Str(text)) = hit.fields.get(name) else {
                    continue;
                };
                let snippet = gen.snippet(text);
                if snippet.is_empty() {
                    continue;
                }
                let fragment = snippet_to_fragment(snippet.fragment(), snippet.highlighted());
                if !fragment.segments.is_empty() {
                    // A single best fragment per field; `max_fragments` caps the vec.
                    let mut frags = vec![fragment];
                    frags.truncate(hl.max_fragments.max(1));
                    hit.highlight.insert(name.clone(), frags);
                }
            }
        }
        Ok(())
    }

    /// **Prefix autocomplete**: the indexed terms of `field` starting with `prefix`, each with its
    /// doc frequency, by scanning the term dictionary from `prefix` until a term no longer matches.
    /// `field` must be an indexed string field (TEXT → analyzed tokens, KEYWORD → raw). Capped at
    /// `scan_cap` terms so a broad prefix can't scan an entire vocabulary.
    ///
    /// Frequencies are **approximate** — the dictionary isn't liveness-filtered, so a term present
    /// only in superseded docs may still appear (self-healing on compaction). The prefix is
    /// lowercased for TEXT to match the analyzer.
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
                    break; // sorted dictionary — past the prefix
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

    /// **Did-you-mean** candidates: indexed terms of `field` within edit distance `max_dist` of
    /// `term` (excluding `term` itself), each as `(term, distance, doc_freq)`. Scans the term
    /// dictionary, pruning by length and a distance-bounded Levenshtein, capped at `scan_cap`.
    /// `field` must be an indexed TEXT/KEYWORD field; the query is lowercased for TEXT.
    ///
    /// Frequencies are **approximate** (not liveness-filtered).
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
    /// numeric/date/string fast field `field` (errors if it isn't fast). Values are read back per
    /// key by [`fast_value`](Self::fast_value) so multi-key ordering resolves in the store.
    fn windowed_by_field(
        &self,
        searcher: &tantivy::Searcher,
        query: &dyn TantivyQuery,
        field: &str,
        order: tantivy::Order,
        limit: usize,
    ) -> Result<Vec<(f32, tantivy::DocAddress)>> {
        // `_score` primary: the window IS the top-`limit` by score, with the real per-doc score
        // carried through (fast fields carry 0.0 and read back via `fast_value`). An ascending
        // `_score` is re-ordered by the store.
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

    /// Validate `field` is a **fast** field usable as a sort key — numeric, date, or KEYWORD.
    /// The reserved [`SCORE_SORT_KEY`] (`_score`) is always sortable, so it is exempt.
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

    /// Scan **every** matching doc for a [field collapse](growlerdb_core::SearchParams) (grouping
    /// needs all members, not a top-`k` window), returning `(hit, group_value, sort_values)` for
    /// each doc that has the `collapse` field set (docs lacking it are skipped). The store then
    /// merges across generations, orders by `sort`, and reduces to the top hit + count per group.
    /// `O(matches)` (D24).
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
                    highlight: Default::default(),
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

    /// Build the **keyset predicate**: a query matching exactly the docs strictly *after* `cursor`
    /// in the [total order](growlerdb_core::Sort) of `sort` (then the composite key). The
    /// lexicographic "tuple > cursor" as an OR of clauses — for each position `i`, *all earlier keys
    /// equal the cursor AND key `i` is strictly after*; plus a final clause where every key equals
    /// the cursor AND the composite key is greater. A missing field sorts last, so "strictly after a
    /// present value" also admits docs lacking the field. `sort` is non-empty (checked by caller).
    fn keyset_after(&self, sort: &[Sort], cursor: &SearchAfter) -> Result<Box<dyn TantivyQuery>> {
        if sort_has_score(sort) {
            // A relevance score isn't a stable, range-able key — `_score` is offset-paged only.
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
        // Resolve each sort key's field + type once (also validates sortability).
        let mut cols = Vec::with_capacity(sort.len());
        for s in sort {
            self.ensure_sortable(&s.field)?;
            let (f, ft) = self.resolve_typed_field(&s.field)?;
            cols.push((s.field.as_str(), f, ft));
        }
        let key_enc = self.index.schema().get_field(KEY_ENC_FIELD)?;

        let mut shoulds: Vec<(Occur, Box<dyn TantivyQuery>)> = Vec::new();
        for i in 0..sort.len() {
            // Position `i` strictly after the cursor; skip if its cursor value is missing (nothing
            // sorts after "last").
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

    /// A query matching docs whose `field` **equals** the cursor value `val` — an inclusive point
    /// range for a present value, or "field absent" when the cursor lacked it
    /// ([`Missing`](SortValue::Missing)).
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

    /// A query matching docs whose `field` is **strictly after** the cursor value `val` in `order`
    /// — greater (asc) / lesser (desc), *plus* docs missing the field (a missing value sorts last).
    /// `None` when the cursor value is itself [`Missing`](SortValue::Missing).
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
                // A dotted `<col>.<path>:value` on an **undeclared** variant sub-path is a flatten
                // exact-term match — rewrite to `<col>#terms` with the `path\u{1}value` token. A
                // declared shaped path is a direct field (below).
                if let Some(name) = field.as_deref() {
                    if let Some(terms) = self.flatten_terms_field(name) {
                        let token = format!("{name}{FLATTEN_TERM_SEP}{value}");
                        return Ok(Box::new(TermQuery::new(
                            Term::from_field_text(terms, &token),
                            IndexRecordOption::Basic,
                        )));
                    }
                }
                // A bare `field:value` on a numeric/date/bool/IP field is an **exact-value match**,
                // not a text term — those columns aren't analyzed, so a text `TermQuery` finds
                // nothing. Reuse the typed range path as an inclusive `[value TO value]`. (A truly
                // unknown field falls through to the text path below and errors as `UnknownField`.)
                if let Some(name) = field.as_deref() {
                    if let Ok((f, ftype)) = self.resolve_typed_field(name) {
                        // Tantivy's `RangeQuery` rejects a `Bool` term, so build the `TermQuery`
                        // directly for BOOL.
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
                // TEXT is analyzed (lowercased); KEYWORD is raw/exact. The record option must match
                // how the field was indexed (TEXT freqs+positions; KEYWORD basic).
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
                // `IN (...)` over an undeclared variant sub-path → a set of flatten tokens.
                if let Some(terms_field) = self.flatten_terms_field(field) {
                    let terms = values
                        .iter()
                        .map(|v| {
                            Term::from_field_text(
                                terms_field,
                                &format!("{field}{FLATTEN_TERM_SEP}{v}"),
                            )
                        })
                        .collect::<Vec<_>>();
                    return Ok(Box::new(TermSetQuery::new(terms)));
                }
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
                // something on analyzed TEXT. The facet/filter chips emit `field:"value"` for every
                // field type, so on anything but TEXT a phrase is an **exact-value match** — reuse
                // the Term path rather than rejecting the field.
                if let Some(name) = field.as_deref() {
                    if let Ok((_, ftype)) = self.resolve_typed_field(name) {
                        // Non-Str: resolve_field (text-only) would reject it; delegate first.
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
                // KEYWORD: a phrase is an exact keyword match, not positional — reuse Term.
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
                        // A multi-token phrase needs positions; a `record: BASIC|FREQ` field lacks
                        // them — fail with the fix, not an opaque error or empty results.
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
                // Exists works on any indexed/fast field, so it skips the text-only `resolve_field`.
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
            // KNN is a top-level retrieval clause resolved over the ANN sidecars, not compiled here.
            // Reaching this arm means it was nested in a lexical query (fusion — a later task).
            Query::Knn { .. } => Err(IndexError::QueryType(
                "KNN cannot be combined lexically — run it as a top-level KNN search (score \
                 fusion with lexical results is not yet supported)"
                    .into(),
            )),
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

    /// **Explain** how `query` scores the doc identified by `key_enc`: locate it by encoded key,
    /// then ask Tantivy for the per-clause BM25 explanation, plus the post-analyzer tokens searched
    /// for. `found = false` if the key isn't indexed; `matched = false` if it is but doesn't select.
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
        // `explain` errors when the doc doesn't match — that's an expected answer (matched = false).
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

    /// The post-analyzer tokens `query` searches for, as `(field, tokens)` — the leaf clauses run
    /// through each field's analyzer, so the console can show exactly what was matched.
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
        // TEXT fields run through the analyzer; otherwise the raw value is the token.
        let tokens = match self.resolve_field(field) {
            Ok((f, true)) => self
                .analyze(f, text)
                .unwrap_or_else(|_| vec![text.to_string()]),
            _ => vec![text.to_string()],
        };
        out.push((name, tokens));
    }

    /// Resolve a query field name to its Tantivy field + whether it is analyzed (TEXT). `None`
    /// resolves to the default TEXT field. The stored key and non-indexed fields are rejected.
    fn resolve_field(&self, name: Option<&str>) -> Result<(Field, bool)> {
        let schema = self.index.schema();
        match name {
            Some(name) => {
                if name == KEY_FIELD {
                    return Err(IndexError::UnknownField(name.to_string()));
                }
                let field = match schema.get_field(name) {
                    Ok(f) => f,
                    // A bare variant column name (`payload`) resolves to its analyzed flatten
                    // catch-all `<col>#text`, when that mode is enabled.
                    Err(_) => schema
                        .get_field(&flatten_text_field_name(name))
                        .map_err(|_| IndexError::UnknownField(name.to_string()))?,
                };
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

    /// If `name` is an **undeclared** dotted sub-path under a variant column whose `<col>#terms`
    /// index exists, return that field — the rewrite target for a flatten `path = value` term query.
    /// `None` for a declared field or when no variant column is a dotted prefix. Scans prefixes
    /// longest-first, so the most specific variant column wins.
    fn flatten_terms_field(&self, name: &str) -> Option<Field> {
        let schema = self.index.schema();
        if schema.get_field(name).is_ok() {
            return None; // a declared field — not a flatten rewrite
        }
        let mut end = name.len();
        while let Some(dot) = name[..end].rfind('.') {
            if let Ok(f) = schema.get_field(&flatten_terms_field_name(&name[..dot])) {
                return Some(f);
            }
            end = dot;
        }
        None
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
        // A DATE bound is canonical epoch micros, but may also be written as ISO-8601/RFC3339 or a
        // bare `YYYY-MM-DD` (UTC midnight) for authoring convenience.
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

/// Levenshtein edit distance between `a` and `b`, short-circuiting to `None` once it is known to
/// exceed `max` (a whole DP row above `max` ⇒ no path back under it).
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

/// Map a fast-field [`SortValue`] to a core [`GValue`] typed to the field's mapping — the sort-key
/// prune hint's value (LONG→[`Int`](GValue::Int), DOUBLE→[`Float`](GValue::Float), DATE→canonical
/// micros [`Ts`](GValue::Ts), KEYWORD→[`Str`](GValue::Str)). [`Missing`](SortValue::Missing) or a
/// type mismatch yields `None`, contributing no hint.
fn sort_value_to_value(ftype: &TvFieldType, sv: &SortValue) -> Option<GValue> {
    match (ftype, sv) {
        (TvFieldType::I64(_), SortValue::Num(x)) => Some(GValue::Int(*x as i64)),
        (TvFieldType::F64(_), SortValue::Num(x)) => Some(GValue::Float(*x)),
        (TvFieldType::Date(_), SortValue::Num(x)) => Some(GValue::Ts(*x as i64)),
        (TvFieldType::Str(_), SortValue::Str(s)) => Some(GValue::Str(s.clone())),
        _ => None,
    }
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

/// Convert a Tantivy snippet — a `fragment` string plus `highlighted` byte ranges — to an XSS-safe
/// [`HighlightFragment`] of alternating context/matched [segments](HighlightSegment). Overlapping
/// ranges are collapsed first; the fragment carries no HTML (the client wraps `marked` segments).
fn snippet_to_fragment(
    fragment: &str,
    highlighted: &[std::ops::Range<usize>],
) -> HighlightFragment {
    let mut segments = Vec::new();
    let mut cursor = 0usize;
    for range in tantivy::snippet::collapse_overlapped_ranges(highlighted) {
        // Skip a malformed/out-of-order range rather than panic on a bad slice.
        if range.start < cursor || range.end > fragment.len() || range.start >= range.end {
            continue;
        }
        if range.start > cursor {
            segments.push(HighlightSegment {
                text: fragment[cursor..range.start].to_string(),
                marked: false,
            });
        }
        segments.push(HighlightSegment {
            text: fragment[range.clone()].to_string(),
            marked: true,
        });
        cursor = range.end;
    }
    if cursor < fragment.len() {
        segments.push(HighlightSegment {
            text: fragment[cursor..].to_string(),
            marked: false,
        });
    }
    HighlightFragment { segments }
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
        CompositeKey, DocBatch, Document, IndexDefinition, MatchOp, Query, SourceField,
        SourceSchema, SourceType, Value,
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

    /// A `docs` index with an added VECTOR field (`body_vec`, dims 3, cosine).
    fn vector_index() -> ResolvedIndex {
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
    - { path: body_vec, type: VECTOR, vector: { dims: 3, metric: COSINE, source_field: body } }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn vec_doc(id: i64, body: &str, v: Vec<f32>) -> Document {
        let key = CompositeKey::new(vec![], vec![("id".into(), id.into())]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), id.into());
        fields.insert("body".to_string(), body.into());
        fields.insert("body_vec".to_string(), Value::Vector(v));
        Document::new(key, fields)
    }

    fn hit_id(hit: &Hit) -> i64 {
        match hit.key.get("id") {
            Some(Value::Int(i)) => *i,
            other => panic!("expected an int id, got {other:?}"),
        }
    }

    // ---- Variant flatten + shapes (D47, TASK-349/352) ---------------------

    /// An `events` index over a variant `payload` column: flatten (terms + text catch-all) plus
    /// two shapes (`pr` → LONG `number` fast + KEYWORD `action` fast; `issue` → LONG `number`).
    fn events_variant_index() -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("event_type", SourceType::String),
                SourceField::new("payload", SourceType::Other),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            r#"
name: events
source: { iceberg: { catalog: g, table: g.events } }
key: { identifier_fields: [id] }
mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - path: payload
      type: VARIANT
      variant:
        flatten: { terms: true, text: true }
        discriminator: event_type
        shapes:
          - name: pr
            when: [PullRequestEvent]
            fields:
              - { path: number, type: LONG, fast: true }
              - { path: action, type: KEYWORD, fast: true }
          - name: issue
            when: [IssuesEvent]
            fields:
              - { path: number, type: LONG, fast: true }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    /// Build an `events` document: `id`, a set of flatten `leaves` (dotted, column-qualified),
    /// declared shape `shaped` values (typed fields), and the discriminator value.
    fn event_doc(
        id: i64,
        discriminator: &str,
        leaves: Vec<(&str, Value)>,
        shaped: Vec<(&str, Value)>,
    ) -> Document {
        let key = CompositeKey::new(vec![], vec![("id".into(), id.into())]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), id.into());
        for (path, value) in shaped {
            fields.insert(path.to_string(), value);
        }
        Document::new(key, fields).with_variants(vec![growlerdb_core::VariantLeaves {
            field: "payload".into(),
            leaves: leaves
                .into_iter()
                .map(|(p, v)| (p.to_string(), v))
                .collect(),
            discriminator: Some(discriminator.to_string()),
        }])
    }

    #[test]
    fn flatten_term_filter_and_text_catch_all_match_seeded_docs() {
        // A GitHub-events-shaped batch. `count` is a **mixed-type** flatten leaf — Int in doc 1,
        // Str in doc 2 — exercising the untyped guarantee (D47): both index without error and both
        // are exact-term matchable, with no dynamic field growth.
        let idx = events_variant_index();
        let schema = IndexSchema::from_resolved(&idx);
        let batch = DocBatch::new(vec![
            event_doc(
                1,
                "PullRequestEvent",
                vec![
                    ("payload.user.login", Value::from("octocat")),
                    ("payload.title", Value::from("Add a shiny feature")),
                    ("payload.number", Value::Int(1347)),
                    ("payload.count", Value::Int(5)),
                ],
                vec![
                    ("payload.number", Value::Int(1347)),
                    ("payload.action", Value::from("opened")),
                ],
            ),
            event_doc(
                2,
                "IssuesEvent",
                vec![
                    ("payload.user.login", Value::from("defunkt")),
                    ("payload.title", Value::from("Fix a subtle bug")),
                    ("payload.number", Value::Int(99)),
                    ("payload.count", Value::from("many")),
                ],
                vec![("payload.number", Value::Int(99))],
            ),
        ]);
        let dir = tempfile::tempdir().unwrap();
        TantivySegmentCore
            .build(&schema, &batch, dir.path())
            .unwrap();
        let r = TantivySegmentCore.open(dir.path()).unwrap();
        let run = |s: &str| ids(&r.search(&q(s), 10).unwrap());

        // Flatten exact-term on an UNDECLARED path (`payload.user.login`) → the one doc.
        assert_eq!(run("payload.user.login:octocat"), vec![1]);
        assert_eq!(run("payload.user.login:defunkt"), vec![2]);
        // Full-text over the analyzed catch-all: `payload:<token>` matches string leaves.
        assert_eq!(run("payload:feature"), vec![1]);
        assert_eq!(run("payload:bug"), vec![2]);
        // Mixed-type untyped path: both the Int and the Str value are exact-term matchable.
        assert_eq!(run("payload.count:5"), vec![1]);
        assert_eq!(run("payload.count:many"), vec![2]);
        // An undeclared path that no doc has → no matches (not an error).
        assert!(run("payload.nope:x").is_empty());

        // No dynamic field growth: the Tantivy schema carries exactly the fixed fields —
        // id, the two flatten fields (payload#terms/#text), the shaped payload.number +
        // payload.action, plus the internal _key/_keyenc/_locid. Never a per-leaf field.
        let names: Vec<String> = schema
            .tantivy_schema()
            .fields()
            .map(|(_, e)| e.name().to_string())
            .collect();
        assert!(names.contains(&"payload#terms".to_string()));
        assert!(names.contains(&"payload#text".to_string()));
        assert!(names.contains(&"payload.number".to_string()));
        assert!(!names
            .iter()
            .any(|n| n == "payload.user.login" || n == "payload.count"));
    }

    #[test]
    fn shaped_paths_support_range_sort_and_exact_match() {
        // TASK-352: a shaped LONG (`payload.number`, fast) is range- and sort-capable; a shaped
        // KEYWORD (`payload.action`, fast) is exact-matchable — all via their typed fields, not
        // the flatten term rewrite.
        let idx = events_variant_index();
        let schema = IndexSchema::from_resolved(&idx);
        let batch = DocBatch::new(vec![
            event_doc(
                1,
                "PullRequestEvent",
                vec![("payload.number", Value::Int(1347))],
                vec![
                    ("payload.number", Value::Int(1347)),
                    ("payload.action", Value::from("opened")),
                ],
            ),
            event_doc(
                2,
                "PullRequestEvent",
                vec![("payload.number", Value::Int(50))],
                vec![
                    ("payload.number", Value::Int(50)),
                    ("payload.action", Value::from("closed")),
                ],
            ),
            event_doc(
                3,
                "IssuesEvent",
                vec![("payload.number", Value::Int(900))],
                vec![("payload.number", Value::Int(900))],
            ),
        ]);
        let dir = tempfile::tempdir().unwrap();
        TantivySegmentCore
            .build(&schema, &batch, dir.path())
            .unwrap();
        let r = TantivySegmentCore.open(dir.path()).unwrap();

        // Range on the shaped LONG.
        assert_eq!(
            ids(&r.search(&q("payload.number:[100 TO 2000]"), 10).unwrap()),
            vec![1, 3]
        );
        // Exact value on the shaped LONG (routed through the typed range path, not flatten).
        assert_eq!(
            ids(&r.search(&q("payload.number:50"), 10).unwrap()),
            vec![2]
        );
        // Exact match on the shaped KEYWORD.
        assert_eq!(
            ids(&r.search(&q("payload.action:opened"), 10).unwrap()),
            vec![1]
        );
        // The shaped LONG is registered sortable (fast + numeric) — the exact contract the sort
        // path's `ensure_sortable` accepts — so `sort=payload.number` is a valid, offered key.
        assert!(schema.sort_fields().contains(&"payload.number"));
    }

    #[test]
    fn knn_search_returns_nearest_docs_by_vector() {
        let schema = IndexSchema::from_resolved(&vector_index());
        let batch = DocBatch::new(vec![
            vec_doc(1, "x", vec![1.0, 0.0, 0.0]),
            vec_doc(2, "y", vec![0.0, 1.0, 0.0]),
            vec_doc(3, "xy", vec![0.9, 0.1, 0.0]),
            vec_doc(4, "z", vec![0.0, 0.0, 1.0]),
        ]);
        let dir = tempfile::tempdir().unwrap();
        TantivySegmentCore
            .build(&schema, &batch, dir.path())
            .unwrap();

        // The build wrote a per-segment ANN sidecar (the multi-threaded writer may split the batch
        // across several segments, one sidecar each).
        let anns: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".ann"))
            .collect();
        assert!(!anns.is_empty(), "at least one per-segment ANN sidecar");

        let reader = TantivySegmentCore.open(dir.path()).unwrap();

        // Nearest to +x → doc 1 (exact) then doc 3 (mostly +x), best-first.
        let hits = reader
            .knn_search("body_vec", &[1.0, 0.0, 0.0], 2, None)
            .unwrap();
        assert_eq!(hits.iter().map(hit_id).collect::<Vec<_>>(), vec![1, 3]);
        assert!(hits[0].score >= hits[1].score);

        // The same through the `Query::Knn` execution path resolves the right key.
        let via_query = reader
            .search(
                &Query::Knn {
                    field: "body_vec".into(),
                    vector: vec![0.0, 0.0, 1.0],
                    k: 1,
                    filter: None,
                },
                1,
            )
            .unwrap();
        assert_eq!(via_query.len(), 1);
        assert_eq!(hit_id(&via_query[0]), 4);

        // `count` of a KNN query is the number of resolved neighbors.
        assert_eq!(
            reader
                .count(&Query::Knn {
                    field: "body_vec".into(),
                    vector: vec![1.0, 0.0, 0.0],
                    k: 3,
                    filter: None,
                })
                .unwrap(),
            3
        );

        // A non-vector field is a clear type error, not an empty result.
        assert!(reader
            .knn_search("body", &[1.0, 0.0, 0.0], 1, None)
            .is_err());
    }

    /// Regression: **filtered KNN is exact on an HNSW-tier segment.** The approximate HNSW
    /// index returns only ~`ef_search` top-similarity candidates, so relying on it for the filtered
    /// path would silently drop filter-allowed matches that rank far down by similarity (the
    /// tenant-isolation hazard). We build one segment with > `HNSW_MIN_VECTORS` vectors (forcing the
    /// HNSW tier), where the only filter-matching docs sit *below* the top-similarity window, and
    /// assert the filtered result is the exact top-k over that allowed subset — never under-filled.
    #[test]
    fn filtered_knn_is_exact_on_hnsw_tier_segment() {
        let schema = IndexSchema::from_resolved(&vector_index());

        // 4096 "common" docs pointing at +x (cosine ≈ 1 — they own the top-similarity window) and
        // 50 "rare" docs pointing mostly at +y with a *tiny*, strictly-increasing +x component
        // (cosine ≈ 0.01·(j+1), well below any common doc). Only the rare docs match `body:rare`.
        let common = crate::vector::HNSW_MIN_VECTORS; // 4096 → total 4146, one HNSW-tier segment
        let rare = 50usize;
        let mut docs = Vec::with_capacity(common + rare);
        for i in 0..common {
            docs.push(vec_doc(i as i64, "common", vec![1.0, 0.0, 0.0]));
        }
        for j in 0..rare {
            let x = 0.01 * (j as f32 + 1.0);
            docs.push(vec_doc(10_000 + j as i64, "rare", vec![x, 1.0, 0.0]));
        }
        let batch = DocBatch::new(docs);

        // Force a SINGLE segment (single-threaded writer) so all 4146 vectors land in one sidecar
        // and it is built at the HNSW tier — a multi-threaded split could shard them into
        // sub-threshold brute-force segments and mask the bug.
        let dir = tempfile::tempdir().unwrap();
        let index = Index::builder()
            .schema(schema.schema.clone())
            .settings(TantivySegmentCore::new_index_settings())
            .create_in_dir(dir.path())
            .unwrap();
        let mut writer: tantivy::IndexWriter =
            index.writer_with_num_threads(1, WRITER_HEAP_BYTES).unwrap();
        for doc in &batch.docs {
            writer.add_document(schema.to_tantivy(doc)).unwrap();
        }
        writer.commit().unwrap();
        let reader = TantivySegmentCore.open(dir.path()).unwrap();
        reader.build_ann_sidecars(&schema, dir.path()).unwrap();

        // Confirm the single sidecar really is the HNSW tier (else the test proves nothing).
        let anns: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().ends_with(".ann"))
            .collect();
        assert_eq!(anns.len(), 1, "expected exactly one segment/sidecar");
        let frame = std::fs::read(anns[0].path()).unwrap();
        let stored = SegmentAnn::from_frame(&frame).unwrap().field("body_vec");
        assert!(
            matches!(stored, Some(StoredAnnIndex::Hnsw(_))),
            "sidecar must be HNSW tier for this regression to be meaningful"
        );

        // Filtered KNN for a +x query, constrained to `body:rare`. The exact top-10 of the allowed
        // subset are the 10 largest-x rare docs — ids 10049..10040, best-first.
        let k = 10;
        let hits = reader
            .knn_search("body_vec", &[1.0, 0.0, 0.0], k, Some(&q("body:rare")))
            .unwrap();
        let got: Vec<i64> = hits.iter().map(hit_id).collect();
        let expected: Vec<i64> = (0..k as i64).map(|n| 10_000 + 49 - n).collect();
        assert_eq!(
            got, expected,
            "filtered KNN must return the exact allowed top-k"
        );
        assert!(
            hits.windows(2).all(|w| w[0].score >= w[1].score),
            "results must be best-first"
        );
        // And it must not be under-filled: exactly `k` matches (50 allowed ≥ 10).
        assert_eq!(hits.len(), k);
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

        // BOOL term match: `active:true`/`active:false` must select the right doc without
        // erroring (Tantivy's range path rejects a Bool term with "Expected term with
        // u64/i64/f64/date, got Bool").
        assert_eq!(reader.search(&q("active:true"), 10).unwrap().len(), 1);
        assert_eq!(reader.search(&q("active:false"), 10).unwrap().len(), 1);

        // DATE range with an ISO-8601 bound: both docs are 2023-11-14
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
    fn sort_fields_lists_only_fast_numeric_date_keyword_fields() {
        // The console builds its sort menu from `sort_fields`; it must exactly match what the sort
        // path accepts (fast + numeric/date/keyword). A cached-but-not-fast keyword like `author`
        // must NOT appear — offering it produced a 400 at query time.
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("author", SourceType::String),
                SourceField::new("tag", SourceType::String),
                SourceField::new("views", SourceType::Long),
                SourceField::new("rating", SourceType::Double),
                SourceField::new("archived", SourceType::Bool),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            r#"
name: catalog
source: { iceberg: { catalog: growlerdb, table: growlerdb.catalog } }
mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - { path: author, type: KEYWORD }
    - { path: tag, type: KEYWORD, fast: true }
    - { path: views, type: LONG, fast: true }
    - { path: rating, type: DOUBLE, fast: true }
    - { path: archived, type: BOOL, fast: true }
    - { path: body, type: TEXT }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let schema = IndexSchema::from_resolved(&idx);
        // Included: fast keyword/long/double, in definition order. Excluded: non-fast keywords
        // (`id`, `author`), a fast BOOL (`archived`), and TEXT (`body`).
        assert_eq!(schema.sort_fields(), vec!["tag", "views", "rating"]);
    }

    #[test]
    fn vector_fields_reports_a_vector_field_with_its_embedding_config() {
        // A VECTOR index surfaces its vector field(s) with the embedding config the console needs
        // to offer semantic/hybrid search (source text field, model, dims).
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
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
    - { path: id, type: KEYWORD }
    - { path: body, type: TEXT }
    - { path: body_vec, type: VECTOR, vector: { dims: 8, model: test-model, source_field: body } }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let schema = IndexSchema::from_resolved(&idx);
        let vfs = schema.vector_fields();
        assert_eq!(vfs.len(), 1);
        assert_eq!(vfs[0].name, "body_vec");
        assert_eq!(vfs[0].source_field, "body");
        assert_eq!(vfs[0].model, "test-model");
        assert_eq!(vfs[0].dims, 8);
    }

    #[test]
    fn vector_fields_is_empty_for_a_non_vector_index() {
        // A plain lexical index reports no vector fields (the console then hides semantic/hybrid).
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            r#"
name: docs
source: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }
mapping: { selection: ALL }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let schema = IndexSchema::from_resolved(&idx);
        assert!(schema.vector_fields().is_empty());
        assert!(!schema.has_vector_fields());
    }

    #[test]
    fn a_declared_timestamp_format_normalizes_the_source_epoch_to_micros_at_build() {
        use growlerdb_core::Value;
        // `ts` is epoch-**millis** at the source (an int64 column) but declared as a timestamp via
        // `format: epoch_ms` — so it resolves to a DATE and must be indexed in canonical **micros**.
        // A doc whose `ts` can't be parsed is skipped, not allowed to wedge the build.
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
        // The format made it a DATE — so it shows up as a time field for the console.
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
            // A `Ts` is *already* canonical micros (a native source timestamp) — the
            // declared epoch_ms format must pass it through untouched, not rescale it by 10³.
            mk("c", Value::Ts(1_782_000_000_000_000)),
        ]);

        let dir = tempfile::tempdir().unwrap();
        let core = TantivySegmentCore;
        assert_eq!(core.build(&schema, &batch, dir.path()).unwrap(), 3);
        let reader = core.open(dir.path()).unwrap();

        // A range query in **micros** (the canonical unit the console sends) finds both the
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

        // Build a segment from a DocBatch, written to a local directory.
        let written = core.build(&schema, &batch(), dir.path()).unwrap();
        assert_eq!(written, 3);

        // Reopen the directory and read it back.
        let reader = core.open(dir.path()).unwrap();
        assert_eq!(reader.num_docs(), 3);

        // BM25 is queryable. "brown" appears in docs 1 and 2.
        let hits = reader.search(&q("body:brown"), 10).unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|h| h.score > 0.0));

        // The composite key is stored per doc and retrievable from a hit.
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

    // ---- the full query-type family ----------------------------------

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
        // `field:(a OR b)` must match the union, identically to the expanded
        // `field:a OR field:b` (the `field:` prefix distributes over the group).
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
        // A wildcard / phrase / boolean *string* parses to the AST and runs end to end
        // over the corpus (ids 1,2 "brown").
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

    #[test]
    fn vec_f32_le_bytes_round_trips() {
        let v = vec![0.0f32, 1.5, -2.25, 3.125, f32::MIN_POSITIVE, f32::MAX];
        assert_eq!(le_bytes_to_vec_f32(&vec_f32_to_le_bytes(&v)), v);
        // Empty vector → empty bytes → empty vector.
        assert!(le_bytes_to_vec_f32(&vec_f32_to_le_bytes(&[])).is_empty());
    }

    #[test]
    fn vector_field_stores_and_reads_back() {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
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
    - { path: id, type: KEYWORD }
    - { path: body, type: TEXT }
    - { path: body_vec, type: VECTOR, vector: { dims: 4, source_field: body } }
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap();

        let vector = vec![0.1f32, 0.2, 0.3, 0.4];
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("a"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("a"));
        f.insert("body".to_string(), Value::from("hello"));
        f.insert("body_vec".to_string(), Value::Vector(vector.clone()));
        let batch = DocBatch::new(vec![Document::new(key, f)]);

        let dir = tempfile::tempdir().unwrap();
        let schema = IndexSchema::from_resolved(&idx);
        let core = TantivySegmentCore;
        assert_eq!(core.build(&schema, &batch, dir.path()).unwrap(), 1);

        // Read the stored vector bytes back and decode — they must equal the input.
        let reader = core.open(dir.path()).unwrap();
        let searcher = reader.reader.searcher();
        let field = reader.index.schema().get_field("body_vec").unwrap();
        let addr = *searcher
            .search(&AllQuery, &DocSetCollector)
            .unwrap()
            .iter()
            .next()
            .unwrap();
        let doc: TantivyDocument = searcher.doc(addr).unwrap();
        // The tantivy `Value` trait (for `as_bytes`) — `_` avoids clashing with `growlerdb_core::Value`.
        use tantivy::schema::Value as _;
        let bytes = doc.get_first(field).and_then(|v| v.as_bytes()).unwrap();
        assert_eq!(le_bytes_to_vec_f32(bytes), vector);
    }
}
