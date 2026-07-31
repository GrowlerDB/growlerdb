//! **Index definition & field mapping** ([Design 04]).
//!
//! An [`IndexDefinition`] is the declarative YAML config describing one index: its
//! source, composite [key](KeySpec), and field [`Mapping`]. The server *resolves* it
//! against the [`SourceSchema`] (leaf fields + key hints) into a [`ResolvedIndex`] —
//! concrete key fields and a typed field list — validating every referenced path exists.
//! Field selection is **ALL** (auto-map, `fields[]` override), **EXPLICIT** (`fields[]`
//! allowlist), or **ALL_EXCEPT** (all but `exclude[]`); unknown YAML keys are ignored so a
//! fuller definition still parses.
//!
//! The [`SourceSchema`] is source-agnostic: the Iceberg/Arrow → GrowlerDB mapping lives in
//! `growlerdb-source`, so this crate stays connector-free.
//!
//! [Design 04]: ../../../okf/product/functional/index-management/create.md

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::routing::{RoutingStrategy, ShardRouter};

/// Errors from parsing or resolving an [`IndexDefinition`].
#[derive(Debug, thiserror::Error)]
pub enum DefError {
    /// The YAML failed to parse into an [`IndexDefinition`].
    #[error("invalid index definition: {0}")]
    Parse(#[from] serde_norway::Error),

    /// The index `name` is empty, too long, or contains characters outside
    /// `[a-zA-Z0-9_-]`. The name becomes an on-disk directory component (the shard path) and an
    /// object-storage prefix, so the charset is locked down — in particular `/` and `..` would
    /// escape the store root.
    #[error(
        "invalid index name `{0}`: use 1-128 characters from a-z, A-Z, 0-9, `_` and `-` \
         (the name becomes a filesystem path component)"
    )]
    InvalidName(String),

    /// `selection: EXPLICIT` was given without any `fields[]`.
    #[error("selection EXPLICIT requires a non-empty `fields` list")]
    EmptyExplicit,

    /// A path referenced by the definition is absent from the source schema.
    #[error("field `{path}` ({referenced_by}) does not exist in the source schema")]
    UnknownPath {
        /// The offending dotted path.
        path: String,
        /// What referenced it — e.g. `key.partition`, `mapping.fields`.
        referenced_by: &'static str,
    },

    /// No key fields could be determined (neither explicit nor derivable).
    #[error("no identifier fields: none given and the source declares none to derive from")]
    NoIdentifier,

    /// A key field (partition or identifier) has a floating-point source type. Floats are
    /// unstable identity/routing keys — `NaN != NaN`, NaN has no canonical bit pattern, and the
    /// bit encoding can diverge across languages (Rust `f64::to_bits` preserves the NaN payload;
    /// Java `Double.doubleToLongBits` canonicalizes it) — so the same key could route to
    /// different shards on the write and read sides. Use an integer, string, or date key.
    #[error("key field `{0}` is floating-point; floats can't be routing/identity keys (NaN is unstable and not cross-language canonical) — use an integer, string, or date key")]
    FloatKey(String),

    /// A `sensitive` field was marked `cached` — caching sensitive fields is
    /// hard-blocked (route them through Polaris-governed hydration instead).
    #[error("field `{0}` is sensitive and cannot be cached (D23)")]
    SensitiveCached(String),

    /// A **big-text** field (declared `max_bytes` over [`MAX_CACHED_FIELD_BYTES`]) was
    /// marked `cached` — big text is hydrate-only (storing it inline bloats the
    /// index and every hit page). Drop `cached` and fetch it via hydration instead.
    #[error("field `{path}` is big text ({max_bytes} > {cap} byte cap) and cannot be cached — fetch it via hydration (D23)")]
    BigTextCached {
        /// The offending field path.
        path: String,
        /// The field's declared maximum byte length.
        max_bytes: u64,
        /// The cache cap it exceeded.
        cap: u64,
    },

    /// `tenant_field` names a field that isn't in the index mapping. Tenant scoping
    /// injects an exact-match filter on this field, so it must be a mapped, filterable field.
    #[error("tenant_field `{0}` is not a mapped field — map it as a KEYWORD field to scope by it")]
    TenantFieldUnmapped(String),

    /// `tenant_field` is mapped but not a KEYWORD field. Tenant scoping matches the claim
    /// exactly; only a KEYWORD (raw, un-analyzed) field gives an exact, un-widenable match.
    #[error("tenant_field `{0}` must be a KEYWORD field (exact match); other types can't anchor a tenant filter")]
    TenantFieldNotKeyword(String),

    /// `windowing.field` isn't in the index mapping. Time-window sharding buckets
    /// documents by this field, so it must be a mapped, fast timestamp field.
    #[error(
        "windowing field `{0}` is not a mapped field — map it as a fast DATE (timestamp) field"
    )]
    WindowFieldUnmapped(String),

    /// `windowing.field` is mapped but not a `DATE`. Windows bucket on the **canonical micros**
    /// timestamp scale, so a window/event field must be a timestamp — a native Iceberg
    /// `date`/`timestamp`, or any column declared with a `format` (e.g. `epoch_ms`). A raw `LONG` is
    /// rejected because its unit is ambiguous (millis vs micros) and would silently misbucket.
    #[error(
        "windowing field `{0}` must be a DATE (timestamp) — declare a `format` (e.g. `epoch_ms`) on \
         the source column so it's stored as canonical micros"
    )]
    WindowFieldNotTime(String),

    /// `windowing.field` is a DATE but not `fast`. Time-range pruning needs the columnar
    /// fast field.
    #[error(
        "windowing field `{0}` must be a fast field (set `fast: true`) for time-range pruning"
    )]
    WindowFieldNotFast(String),

    /// A field declared a timestamp `format` but also an explicit non-DATE `type` — the
    /// format already makes it a DATE, so the type is contradictory.
    #[error("field `{path}` has a timestamp `format` but type `{ty:?}` — a `format` makes it a DATE; drop the type or set it to DATE")]
    TimestampFormatType {
        /// The offending field path.
        path: String,
        /// The contradictory declared type.
        ty: FieldType,
    },

    /// `indexed: false` on a TEXT/KEYWORD field. Text search and exact keyword match
    /// run on the inverted index — string terms have no columnar query fallback — so a
    /// non-indexed string field would be unsearchable.
    #[error("field `{0}` is TEXT/KEYWORD and cannot be `indexed: false` — string search has no columnar fallback")]
    IndexedFalseText(String),

    /// `indexed: false` without `fast: true`. With neither the inverted index nor a
    /// columnar fast field, no query path can reach the field at all.
    #[error("field `{0}` has `indexed: false` but not `fast: true` — the field would be unqueryable; set `fast: true` (columnar) or drop `indexed: false`")]
    IndexedFalseNotFast(String),

    /// `record:` on a non-TEXT field — the knob shapes the analyzed inverted
    /// index, which only TEXT has (KEYWORD is raw single-token; typed fields aren't analyzed).
    #[error("field `{0}` is not TEXT — `record:` only applies to analyzed TEXT fields")]
    RecordOnNonText(String),

    /// `fieldnorms:` on a non-TEXT field — length normalization only means
    /// something for analyzed multi-token TEXT.
    #[error("field `{0}` is not TEXT — `fieldnorms:` only applies to analyzed TEXT fields")]
    FieldnormsOnNonText(String),

    /// A `type: VECTOR` field carried no `vector` config — dims/model/source_field
    /// have no defaults for the source field to embed, so the config is required.
    #[error("VECTOR field `{0}` requires a `vector` config")]
    VectorMissingConfig(String),

    /// A `vector` config was declared on a field whose type isn't `VECTOR` — the
    /// embedding config only means something for a vector field.
    #[error("field `{0}` has a `vector` config but is not a VECTOR field")]
    VectorConfigOnNonVector(String),

    /// A VECTOR field's `dims` was 0 — a zero-length embedding is meaningless.
    #[error("VECTOR field `{0}` has `dims: 0` — embedding dimensionality must be > 0")]
    VectorZeroDims(String),

    /// A VECTOR field set a knob that doesn't apply to a vector (`fast`, `cached`,
    /// `sensitive`, `analyzer`, `record`, `fieldnorms`, or `format`) — vectors get
    /// no inverted index and no columnar/text representation.
    #[error("VECTOR field `{path}` cannot set `{option}` — it does not apply to a vector field")]
    VectorInvalidOption {
        /// The offending field path.
        path: String,
        /// The knob that isn't allowed on a vector field.
        option: &'static str,
    },

    /// A VECTOR field asked for the `EXTERNAL` embedding provider — only the
    /// in-process `LOCAL` provider is wired today.
    #[error("VECTOR field `{0}`: external embedding provider not yet supported (local only)")]
    VectorExternalProvider(String),

    /// A VECTOR field's `source_field` was empty or didn't name a mapped field —
    /// there is nothing to embed.
    #[error("VECTOR field `{path}` source_field `{source_field}` does not name a mapped field")]
    VectorSourceUnknown {
        /// The vector field's path.
        path: String,
        /// The unresolved `source_field` reference.
        source_field: String,
    },

    /// A `variant` config was declared on a field whose type isn't `VARIANT` — the flatten/shapes
    /// config only means something for a variant column.
    #[error("field `{0}` has a `variant` config but is not a VARIANT field")]
    VariantConfigOnNonVariant(String),

    /// A `type: VARIANT` field would index **nothing**: flatten is disabled and no shapes were
    /// declared. Enable flatten (the default) or declare at least one shape.
    #[error("VARIANT field `{0}` indexes nothing — enable `flatten` (the default) or declare at least one shape")]
    VariantEmpty(String),

    /// A VARIANT field declared `shapes` without a `discriminator` — shapes are selected per row
    /// by the discriminator value, so one is required.
    #[error("VARIANT field `{0}` declares shapes but no `discriminator` — a discriminator path selects the shape per row")]
    VariantShapesNoDiscriminator(String),

    /// The same path is declared in two shapes with **different** types — resolved shape field
    /// names are shared across shapes (dotted `column.path`), so a cross-shape type disagreement
    /// is a create-time error (D47).
    #[error("VARIANT field `{column}`: path `{path}` is `{first:?}` in shape `{first_shape}` but `{second:?}` in shape `{second_shape}` — a path shared across shapes must have one type")]
    VariantShapeTypeConflict {
        /// The variant column.
        column: String,
        /// The conflicting dotted `column.path`.
        path: String,
        /// The first-seen type and its shape.
        first: FieldType,
        first_shape: String,
        /// The disagreeing type and its shape.
        second: FieldType,
        second_shape: String,
    },

    /// A VARIANT column name (or a shape leaf path) contains the reserved `#` character. GrowlerDB
    /// derives the flatten term/text field names as `<column>#terms` / `<column>#text`, so `#` in a
    /// variant path would collide with the internal encoding.
    #[error("VARIANT path `{0}` contains the reserved `#` character (used for the `<column>#terms`/`#text` flatten fields)")]
    VariantReservedName(String),
}

/// Cache cap: a field whose declared `max_bytes` exceeds this is **big text** and
/// may not be `cached` (it is hydrate-only). 32 KiB — large enough for titles/summaries,
/// small enough that storing it inline on every hit stays cheap.
pub const MAX_CACHED_FIELD_BYTES: u64 = 32 * 1024;

/// A complete index definition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDefinition {
    /// Index name (aliasable later).
    pub name: String,
    /// Where the documents come from.
    pub source: Source,
    /// Composite document key. Defaults to derive-from-source.
    #[serde(default)]
    pub key: KeySpec,
    /// Which source fields to index, and how.
    #[serde(default)]
    pub mapping: Mapping,
    /// The number of shards the index is **routed and built at**. Placement is
    /// `route(key) % shard_count`, so changing it re-routes ~every document — it is a
    /// **reindex-only** operation ([`alter_to`](ResolvedIndex::alter_to) flags a change as
    /// reindex-required; online resharding via virtual buckets is the future path).
    /// The deployed shard map (Control Plane) and the connector fan-out must match this count.
    /// Defaults to 1 (single shard).
    #[serde(default = "default_shard_count")]
    pub shard_count: u32,
    /// The field carrying a row's **tenant id**, for tenant scoping. When set, the
    /// engine ANDs a mandatory `tenant_field = <claim>` filter into every read from the
    /// caller's verified tenant claim — so a caller can never read another tenant's rows.
    /// Must be a mapped **KEYWORD** field. **Independent of the routing/partition key**:
    /// isolation is a per-shard filter, so placement stays hash-uniform (no tenant hot spots);
    /// co-locating a tenant on a shard is a separate, opt-in routing choice. `None` = no scoping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_field: Option<String>,
    /// Time-window (range) sharding: when set, the index is partitioned by this time
    /// field into contiguous **window shards** (one per window), enabling time-range query pruning
    /// and cold-window parking. The field must be a mapped, **fast** `DATE`/`LONG` field
    /// (epoch ms). `None` = no time-windowing (hash/partition routing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub windowing: Option<crate::window::TimeWindowing>,
    /// How hydration locates a key's source row ([layered locator]): [`COORDINATES`]
    /// (the default — per-row location data kept in the index) or [`PREDICATE`]
    /// (store-less — re-find by a pruned key scan). See [`LocationStrategy`] for the
    /// trade-off and the predicate strategy's honest scope.
    ///
    /// [layered locator]: ../../../okf/system/decisions/d30-layered-locator.md
    /// [`COORDINATES`]: LocationStrategy::Coordinates
    /// [`PREDICATE`]: LocationStrategy::Predicate
    #[serde(default)]
    pub location_strategy: LocationStrategy,
}

/// Per-index **location strategy** ([layered locator]): how hydration resolves a
/// composite key back to its source row. Chosen by the author in the definition
/// (`location_strategy:`); the default is the universal [`Coordinates`](Self::Coordinates).
/// Auto-detection from table inspection (format version, sort order, partition spec)
/// is deferred until the `row_id` strategy exists — today the choice is explicit.
///
/// Key **verification** (a fetched row must carry the requested key) and the
/// predicate **fallback** stay on under every strategy — a strategy changes
/// performance and index size, never correctness.
///
/// [layered locator]: ../../../okf/system/decisions/d30-layered-locator.md
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum LocationStrategy {
    /// Per-row `(file, position)` kept in the index (the layered locator: `_locid`
    /// fast field + dense location array, ~13–15 B/row) — fast point reads on any
    /// table; healed in the background when Iceberg compaction rewrites files.
    #[default]
    Coordinates,
    /// **Store-less**: no per-row location data at all. Every hydration re-finds the
    /// row by a key-equality scan pruned by partition values + column stats — today's
    /// verify-and-fall-back pass promoted to the primary path. Zero location bytes and
    /// nothing to heal on source compaction, but **honest scope**: effective only
    /// where the key correlates with the table layout (partitioned on key fields, or
    /// clustered/sorted by the key). On an unclustered high-cardinality key, stats
    /// can't prune and hydration degrades to broad scans.
    Predicate,
}

/// Default [`shard_count`](IndexDefinition::shard_count): a single shard.
fn default_shard_count() -> u32 {
    1
}

/// Validate an index name for use as a filesystem path component / object-storage prefix:
/// non-empty, ≤128 chars, `[a-zA-Z0-9_-]` only. Rejecting `/`, `\` and `.` closes path
/// traversal (`../../evil`) at the single chokepoint every definition passes through
/// ([`IndexDefinition::from_yaml`]); the registry re-checks on create for defense-in-depth.
pub fn validate_index_name(name: &str) -> Result<(), DefError> {
    let ok = !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(DefError::InvalidName(name.to_string()))
    }
}

impl IndexDefinition {
    /// Parse an index definition from YAML.
    pub fn from_yaml(yaml: &str) -> Result<Self, DefError> {
        let def: Self = serde_norway::from_str(yaml)?;
        validate_index_name(&def.name)?;
        Ok(def)
    }

    /// Whether this **unresolved** definition declares a `type: VARIANT` field. Checked before
    /// [`resolve`](Self::resolve) so the create path routes schema introspection around released
    /// iceberg-rust (which can't parse a v3 variant schema) to the Trino lane
    /// ([D48](../../../okf/system/decisions/d48-variant-delivery.md),
    /// [D49](../../../okf/system/decisions/d49-variant-iceberg-rust-routing.md)) before `load_table`.
    /// The resolved counterpart is [`ResolvedIndex::has_variant_field`].
    pub fn declares_variant(&self) -> bool {
        self.mapping
            .fields
            .iter()
            .any(|f| f.ty == Some(FieldType::Variant))
    }

    /// Resolve this definition against a concrete `source` schema: derive the
    /// composite key, auto-map / override field types, and validate that every
    /// referenced path exists in the source.
    pub fn resolve(&self, source: &SourceSchema) -> Result<ResolvedIndex, DefError> {
        let key = self.key.resolve(source)?;
        let fields = self.mapping.resolve(source)?;
        // Cached-field policy, hard-blocked (not user-overridable):
        //   * sensitive fields are never cacheable;
        //   * big-text fields (declared over the cap) are hydrate-only.
        // Plus the `indexed` guardrails: a non-indexed field must have a columnar
        // query path (fast), and string types can't opt out at all.
        for f in &fields {
            // A VECTOR field is intentionally non-indexed and non-fast (it carries no inverted
            // index or columnar scalar), and a VARIANT field carries its own flatten index (not
            // the per-field inverted/columnar surface) — the `indexed`/`fast` guardrail below
            // doesn't apply to either.
            if !f.indexed && !matches!(f.ty, FieldType::Vector | FieldType::Variant) {
                if matches!(f.ty, FieldType::Text | FieldType::Keyword) {
                    return Err(DefError::IndexedFalseText(f.path.clone()));
                }
                if !f.fast {
                    return Err(DefError::IndexedFalseNotFast(f.path.clone()));
                }
            }
            if f.cached && f.sensitive {
                return Err(DefError::SensitiveCached(f.path.clone()));
            }
            if f.cached {
                if let Some(max_bytes) = f.max_bytes {
                    if max_bytes > MAX_CACHED_FIELD_BYTES {
                        return Err(DefError::BigTextCached {
                            path: f.path.clone(),
                            max_bytes,
                            cap: MAX_CACHED_FIELD_BYTES,
                        });
                    }
                }
            }
        }
        // A VECTOR field's `source_field` must name another mapped field — that's the text
        // whose value is embedded at ingest. Checked here, where the whole field set exists.
        for f in &fields {
            if let Some(spec) = &f.vector {
                if !fields.iter().any(|o| o.path == spec.source_field) {
                    return Err(DefError::VectorSourceUnknown {
                        path: f.path.clone(),
                        source_field: spec.source_field.clone(),
                    });
                }
            }
        }
        // Tenant scoping: the tenant field must be a mapped KEYWORD field, so the
        // injected `tenant = claim` filter is an exact, un-widenable match.
        if let Some(tf) = &self.tenant_field {
            match fields.iter().find(|f| &f.path == tf) {
                None => return Err(DefError::TenantFieldUnmapped(tf.clone())),
                Some(f) if f.ty != FieldType::Keyword => {
                    return Err(DefError::TenantFieldNotKeyword(tf.clone()))
                }
                Some(_) => {}
            }
        }
        // Time-window sharding: the ingest-time window field — and the optional
        // event-time zone-map field — must each be a mapped, fast **DATE** (canonical micros)
        // field, so documents bucket into windows on the same scale the index/range path
        // uses and time-range queries can prune by them. A raw LONG is rejected: declare a `format`.
        if let Some(w) = &self.windowing {
            let check_time_field = |name: &str| -> Result<(), DefError> {
                match fields.iter().find(|f| f.path == name) {
                    None => Err(DefError::WindowFieldUnmapped(name.to_string())),
                    Some(f) if f.ty != FieldType::Date => {
                        Err(DefError::WindowFieldNotTime(name.to_string()))
                    }
                    Some(f) if !f.fast => Err(DefError::WindowFieldNotFast(name.to_string())),
                    Some(_) => Ok(()),
                }
            };
            check_time_field(&w.field)?;
            if let Some(ef) = &w.event_time_field {
                check_time_field(ef)?;
            }
        }
        let (equality_deletes, mut warnings) =
            classify_equality_deletes(&source.equality_delete_fields, &key, &fields);
        // Honest-scope guardrail: a PREDICATE index stores no location data, so hydration
        // latency depends on the source layout. We don't inspect the layout (deferred with
        // auto-detection) — we warn, so the trade-off is a stated choice.
        if self.location_strategy == LocationStrategy::Predicate {
            warnings.push(
                "location_strategy PREDICATE stores no per-row location data: every hydration \
                 re-finds the row by a partition/stats-pruned key scan of the source table. \
                 Latency depends on the source layout — effective when the key correlates with \
                 it (partitioned on key fields, or clustered/sorted by the key); on an \
                 unclustered high-cardinality key the scan cannot prune and hydration degrades \
                 to broad scans. Use the default COORDINATES strategy if unsure."
                    .to_string(),
            );
        }
        Ok(ResolvedIndex {
            name: self.name.clone(),
            source: self.source.clone(),
            key,
            fields,
            equality_deletes,
            warnings,
            shard_count: self.shard_count.max(1),
            tenant_field: self.tenant_field.clone(),
            windowing: self.windowing.clone(),
            location_strategy: self.location_strategy,
        })
    }
}

/// Decide how a source's **equality deletes** apply to the index, and warn when
/// they need the costlier fallback ([equality deletes](../../../okf/product/functional/ingestion/index.md)):
///
/// * columns ⊆ the composite key → [`DeleteByKey`](EqualityDeleteHandling::DeleteByKey)
///   (an equality delete `c = K` *is* `delete_by_key(K)` — no pre-image needed);
/// * otherwise → [`Reconcile`](EqualityDeleteHandling::Reconcile): a non-key
///   predicate (e.g. `status = 'archived'`) can't be keyed, so the affected
///   partition is re-scanned and diffed against the index.
///
/// Validates the columns are a subset of key + indexed fields and **warns** (does not
/// fail) when they aren't — so the reconciliation path is a known choice, not a silent
/// surprise.
fn classify_equality_deletes(
    equality_delete_fields: &[String],
    key: &ResolvedKey,
    fields: &[ResolvedField],
) -> (EqualityDeleteHandling, Vec<String>) {
    if equality_delete_fields.is_empty() {
        return (EqualityDeleteHandling::None, Vec::new());
    }
    let in_key = |c: &String| key.partition_fields.contains(c) || key.identifier_fields.contains(c);
    let indexed = |c: &String| fields.iter().any(|f| &f.path == c);

    if equality_delete_fields.iter().all(in_key) {
        return (EqualityDeleteHandling::DeleteByKey, Vec::new());
    }

    // Non-key predicate → reconciliation. Columns outside key *and* indexed fields
    // are doubly costly (the diff can't even narrow by them) — call them out.
    let non_key: Vec<String> = equality_delete_fields
        .iter()
        .filter(|c| !in_key(c))
        .cloned()
        .collect();
    let uncovered: Vec<String> = non_key.iter().filter(|c| !indexed(c)).cloned().collect();
    let mut warnings = Vec::new();
    let detail = if uncovered.is_empty() {
        String::new()
    } else {
        format!(" (and outside the indexed fields: {uncovered:?})")
    };
    warnings.push(format!(
        "equality-delete columns {non_key:?} are not part of the composite key{detail}; \
         deletes on them fall back to partition-scoped reconciliation (a partition re-scan). \
         Add them to the key to get the cheaper delete-by-key path."
    ));
    (EqualityDeleteHandling::Reconcile { uncovered }, warnings)
}

/// The source the index is fed from. Iceberg only; the single-key-map
/// shape (`source: { iceberg: { … } }`) leaves room for `kafka`/`cdc`/`file`.
///
/// Serialized through [`SourceWire`] so the `{ kind: { … } }` authoring surface
/// from Design 04 round-trips without YAML `!tags` (which the maintained
/// `serde_*yaml` forks now require for externally tagged enums).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "SourceWire", into = "SourceWire")]
pub enum Source {
    /// An Apache Iceberg table read through a REST catalog.
    Iceberg(IcebergSource),
}

/// Wire form of [`Source`]: a map with one populated source-kind key.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SourceWire {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    iceberg: Option<IcebergSource>,
}

impl TryFrom<SourceWire> for Source {
    type Error = String;
    fn try_from(w: SourceWire) -> Result<Self, String> {
        match w.iceberg {
            Some(i) => Ok(Source::Iceberg(i)),
            None => Err("source must specify exactly one kind (e.g. `iceberg`)".to_string()),
        }
    }
}

impl From<Source> for SourceWire {
    fn from(s: Source) -> Self {
        match s {
            Source::Iceberg(i) => SourceWire { iceberg: Some(i) },
        }
    }
}

/// An Iceberg table source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IcebergSource {
    /// Catalog reference (Polaris REST catalog name).
    pub catalog: String,
    /// Table identifier, `namespace.table`.
    pub table: String,
    /// Scan mode. Reads the current snapshot append-only regardless; the
    /// field parses so a fuller definition round-trips.
    #[serde(default)]
    pub scan: ScanMode,
}

/// How the source is scanned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ScanMode {
    /// Changelog-first sync (the default).
    #[default]
    Changelog,
    /// Append-only fast path for immutable tables.
    AppendFastPath,
}

/// The composite document key: partition fields + identifier fields.
///
/// Empty lists mean *derive from the source* (its partition spec + identifier
/// fields); a non-empty list overrides that side explicitly.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct KeySpec {
    /// Partition fields — drive routing and hydration pruning. Empty ⇒ derive.
    #[serde(default)]
    pub partition_fields: Vec<String>,
    /// Identifier fields — uniquely identify a row within a partition. Empty ⇒ derive.
    #[serde(default)]
    pub identifier_fields: Vec<String>,
}

impl KeySpec {
    fn resolve(&self, source: &SourceSchema) -> Result<ResolvedKey, DefError> {
        let partition_fields = if self.partition_fields.is_empty() {
            source.partition_fields.clone()
        } else {
            self.partition_fields.clone()
        };
        let identifier_fields = if self.identifier_fields.is_empty() {
            source.identifier_fields.clone()
        } else {
            self.identifier_fields.clone()
        };

        if identifier_fields.is_empty() {
            return Err(DefError::NoIdentifier);
        }
        for p in partition_fields.iter().chain(identifier_fields.iter()) {
            match source.field(p) {
                None => {
                    return Err(DefError::UnknownPath {
                        path: p.clone(),
                        referenced_by: "key",
                    })
                }
                // Reject floating-point keys up front: a float can't be a stable
                // routing/identity key, and NaN encodes differently across languages.
                Some(f) if f.ty == SourceType::Double => return Err(DefError::FloatKey(p.clone())),
                Some(_) => {}
            }
        }
        Ok(ResolvedKey {
            partition_fields,
            identifier_fields,
        })
    }
}

/// Field selection strategy ([Design 04]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Selection {
    /// Index every source field with auto-derived types; `fields[]` are overrides.
    #[default]
    All,
    /// Index only the paths listed in `fields[]` (an allowlist).
    Explicit,
    /// Index every source field except `exclude[]`; `fields[]` are overrides.
    AllExcept,
}

/// Which source fields to index, and how to type them.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Mapping {
    /// `ALL` (default), `EXPLICIT`, or `ALL_EXCEPT`.
    #[serde(default)]
    pub selection: Selection,
    /// Per-path overrides (`ALL`/`ALL_EXCEPT`) or the allowlist (`EXPLICIT`).
    #[serde(default)]
    pub fields: Vec<FieldMapping>,
    /// Paths to drop under `ALL_EXCEPT` (the denylist). Ignored otherwise.
    #[serde(default)]
    pub exclude: Vec<String>,
}

impl Mapping {
    fn resolve(&self, source: &SourceSchema) -> Result<Vec<ResolvedField>, DefError> {
        for f in &self.fields {
            // A VECTOR field is a **derived** field (the embedding of `source_field`), not a
            // source column, so its `path` need not name a source leaf; every other field's must.
            if f.ty != Some(FieldType::Vector) && !source.has_field(&f.path) {
                return Err(DefError::UnknownPath {
                    path: f.path.clone(),
                    referenced_by: "mapping.fields",
                });
            }
            // A timestamp `format` declares the field a DATE; an explicit non-DATE `type`
            // alongside it is a contradiction — reject it loudly rather than silently pick one.
            // (Skipped for VECTOR, whose `resolve_vector_field` rejects `format` with its own error.)
            if f.ty != Some(FieldType::Vector)
                && f.format.is_some()
                && matches!(f.ty, Some(t) if t != FieldType::Date)
            {
                return Err(DefError::TimestampFormatType {
                    path: f.path.clone(),
                    ty: f.ty.unwrap(),
                });
            }
        }

        match self.selection {
            Selection::All | Selection::AllExcept => {
                let exclude: HashSet<&str> = if self.selection == Selection::AllExcept {
                    for p in &self.exclude {
                        if !source.has_field(p) {
                            return Err(DefError::UnknownPath {
                                path: p.clone(),
                                referenced_by: "mapping.exclude",
                            });
                        }
                    }
                    self.exclude.iter().map(String::as_str).collect()
                } else {
                    HashSet::new()
                };
                let mut fields = Vec::new();
                for sf in source
                    .fields
                    .iter()
                    .filter(|sf| !exclude.contains(sf.path.as_str()))
                {
                    let ovr = self.fields.iter().find(|f| f.path == sf.path);
                    // A VARIANT override turns this source column into the flatten index + its
                    // shape leaves (1+N fields); anything else resolves to a single field.
                    if ovr.is_some_and(|o| o.ty == Some(FieldType::Variant)) {
                        fields.extend(resolve_variant_field(ovr.unwrap())?);
                    } else {
                        fields.push(resolve_field(&sf.path, sf.ty, ovr)?);
                    }
                }
                // Append the derived VECTOR fields — they aren't source leaves, so the
                // source-driven pass above never emits them.
                for f in self
                    .fields
                    .iter()
                    .filter(|f| f.ty == Some(FieldType::Vector))
                {
                    fields.push(resolve_vector_field(f)?);
                }
                Ok(fields)
            }
            Selection::Explicit => {
                if self.fields.is_empty() {
                    return Err(DefError::EmptyExplicit);
                }
                let mut fields = Vec::new();
                for f in &self.fields {
                    if f.ty == Some(FieldType::Vector) {
                        fields.push(resolve_vector_field(f)?);
                    } else if f.ty == Some(FieldType::Variant) {
                        fields.extend(resolve_variant_field(f)?);
                    } else {
                        // Safe: presence validated above.
                        let sf = source.field(&f.path).expect("validated present");
                        fields.push(resolve_field(&f.path, sf.ty, Some(f))?);
                    }
                }
                Ok(fields)
            }
        }
    }
}

/// Build a [`ResolvedField`] from a source field + an optional override entry: type defaults
/// to [`auto_type`]; `analyzer`/`fast`/`indexed`/`record`/`fieldnorms`/`cached` come from the
/// override. Errors on the TEXT-only knobs (`record`, `fieldnorms`) applied to a non-TEXT
/// field — they shape the analyzed inverted index, which only TEXT has.
fn resolve_field(
    path: &str,
    source_ty: SourceType,
    ovr: Option<&FieldMapping>,
) -> Result<ResolvedField, DefError> {
    // A `vector` config only means something for a VECTOR field (which resolves through
    // `resolve_vector_field`, never here) — reject it on any other type.
    if ovr.is_some_and(|o| o.vector.is_some()) {
        return Err(DefError::VectorConfigOnNonVector(path.to_string()));
    }
    // Likewise a `variant` config only means something for a VARIANT field (resolved through
    // `resolve_variant_field`, never here).
    if ovr.is_some_and(|o| o.variant.is_some()) {
        return Err(DefError::VariantConfigOnNonVariant(path.to_string()));
    }
    let format = ovr.and_then(|o| o.format);
    // A declared timestamp `format` makes the field a DATE regardless of its source
    // Arrow type — that's the whole point (an int64/string epoch column becomes a timestamp).
    // The contradictory case (explicit non-DATE `type` + `format`) is rejected in `resolve`.
    let ty = if format.is_some() {
        FieldType::Date
    } else {
        ovr.and_then(|o| o.ty)
            .unwrap_or_else(|| auto_type(source_ty))
    };
    if !ty.is_text() {
        if ovr.is_some_and(|o| o.record.is_some()) {
            return Err(DefError::RecordOnNonText(path.to_string()));
        }
        if ovr.is_some_and(|o| o.fieldnorms.is_some()) {
            return Err(DefError::FieldnormsOnNonText(path.to_string()));
        }
    }
    let fast = ovr.is_some_and(|o| o.fast);
    Ok(ResolvedField {
        path: path.to_string(),
        ty,
        analyzer: ovr.and_then(|o| o.analyzer.clone()),
        format,
        fast,
        // Fast-only default: TEXT/KEYWORD are always inverted-indexed (string search has
        // no columnar fallback); a fast non-text field defaults to columnar-only (its range /
        // exact / sort / exists paths all run on the fast field — the inverted index would be
        // dead weight). An explicit `indexed:` wins; the invalid combinations (`false` on text,
        // `false` without `fast`) are rejected in `resolve`, not silently patched here.
        indexed: ovr.and_then(|o| o.indexed).unwrap_or(match ty {
            FieldType::Text | FieldType::Keyword => true,
            _ => !fast,
        }),
        // TEXT indexing detail: full fidelity by default — positions (phrase-safe)
        // and fieldnorms (BM25 length normalization). Meaningful only for TEXT (non-TEXT
        // rejected above); carried as concrete values so the derived schema never guesses.
        record: ovr.and_then(|o| o.record).unwrap_or(TextRecord::Position),
        fieldnorms: ovr.and_then(|o| o.fieldnorms).unwrap_or(true),
        cached: ovr.is_some_and(|o| o.cached),
        sensitive: ovr.is_some_and(|o| o.sensitive),
        max_bytes: ovr.and_then(|o| o.max_bytes),
        vector: None,
        variant: None,
    })
}

/// Resolve a `type: VECTOR` field mapping into its [`ResolvedField`]. Unlike a normal
/// field, a vector field is **derived** (its value is the embedding of `source_field`, not a
/// source column), so its `path` need not name a source leaf. The `vector` config is required;
/// the knobs that shape an inverted/columnar/text representation (`fast`, `cached`, `sensitive`,
/// `analyzer`, `record`, `fieldnorms`, `format`) don't apply and are rejected. The field is
/// forced non-`indexed` (vectors get no inverted index); `record`/`fieldnorms` stay at their
/// defaults. Cross-field validation (that `source_field` names a mapped field) happens in
/// [`resolve`](IndexDefinition::resolve) once the whole field set exists.
fn resolve_vector_field(f: &FieldMapping) -> Result<ResolvedField, DefError> {
    let opts = f
        .vector
        .as_ref()
        .ok_or_else(|| DefError::VectorMissingConfig(f.path.clone()))?;
    let reject = |set: bool, option: &'static str| -> Result<(), DefError> {
        if set {
            return Err(DefError::VectorInvalidOption {
                path: f.path.clone(),
                option,
            });
        }
        Ok(())
    };
    reject(f.fast, "fast")?;
    reject(f.cached, "cached")?;
    reject(f.sensitive, "sensitive")?;
    reject(f.analyzer.is_some(), "analyzer")?;
    reject(f.record.is_some(), "record")?;
    reject(f.fieldnorms.is_some(), "fieldnorms")?;
    reject(f.format.is_some(), "format")?;

    let dims = opts.dims.unwrap_or(DEFAULT_EMBED_DIMS);
    if dims == 0 {
        return Err(DefError::VectorZeroDims(f.path.clone()));
    }
    let provider = opts.provider.unwrap_or_default();
    if provider == EmbedProvider::External {
        return Err(DefError::VectorExternalProvider(f.path.clone()));
    }
    if opts.source_field.is_empty() {
        return Err(DefError::VectorSourceUnknown {
            path: f.path.clone(),
            source_field: opts.source_field.clone(),
        });
    }
    let spec = VectorSpec {
        dims,
        model: opts
            .model
            .clone()
            .unwrap_or_else(|| DEFAULT_EMBED_MODEL.to_string()),
        metric: opts.metric.unwrap_or_default(),
        provider,
        source_field: opts.source_field.clone(),
    };
    Ok(ResolvedField {
        path: f.path.clone(),
        ty: FieldType::Vector,
        analyzer: None,
        format: None,
        fast: false,
        // Vectors get no inverted index — the KNN/ANN path reads the stored bytes.
        indexed: false,
        record: TextRecord::Position,
        fieldnorms: true,
        cached: false,
        sensitive: false,
        max_bytes: None,
        vector: Some(spec),
        variant: None,
    })
}

/// Resolve a `type: VARIANT` field mapping into its [`ResolvedField`]s (D47): the variant field
/// itself (carrying the flatten index + the extraction spec for the connector) **plus** one
/// resolved leaf field per unique dotted `column.path` declared across the shapes. `f.path` is the
/// variant column (validated to exist in the source by the caller; its sub-paths are not — a
/// variant has no fixed leaf schema). Shape leaf paths are relative to the column and are prefixed
/// with it here; a path declared in two shapes with the same type is one field, a type
/// disagreement is a [create-time error](DefError::VariantShapeTypeConflict).
fn resolve_variant_field(f: &FieldMapping) -> Result<Vec<ResolvedField>, DefError> {
    let column = f.path.clone();
    if column.contains('#') {
        return Err(DefError::VariantReservedName(column));
    }
    // A `variant:` config is optional — a bare `type: VARIANT` gets the default flatten.
    let opts = f.variant.clone().unwrap_or_default();
    let flatten = opts.flatten.clone();

    // Shapes require a discriminator; a discriminator with no shapes is inert but harmless.
    if !opts.shapes.is_empty() && opts.discriminator.is_none() {
        return Err(DefError::VariantShapesNoDiscriminator(column));
    }
    // Nothing to index?
    let flatten_indexes = flatten.enabled && (flatten.terms || flatten.text);
    if !flatten_indexes && opts.shapes.is_empty() {
        return Err(DefError::VariantEmpty(column));
    }

    // Resolve each shape's leaves into typed fields at the dotted `column.path`, deduping across
    // shapes (same path+type = one field; a type disagreement is an error). `first_seen` remembers
    // which shape first declared each path, for the conflict message.
    let mut leaves: Vec<ResolvedField> = Vec::new();
    let mut first_seen: std::collections::HashMap<String, (FieldType, String)> =
        std::collections::HashMap::new();
    let mut resolved_shapes: Vec<ResolvedShape> = Vec::new();
    for shape in &opts.shapes {
        let mut shape_paths: Vec<(String, FieldType)> = Vec::new();
        for leaf in &shape.fields {
            if leaf.path.contains('#') {
                return Err(DefError::VariantReservedName(format!(
                    "{column}.{}",
                    leaf.path
                )));
            }
            // Reparent the leaf onto the column: its resolved name is `column.path`, and a VECTOR
            // leaf's `source_field` (which references a sibling leaf) is likewise column-qualified.
            let dotted = format!("{column}.{}", leaf.path);
            let mut reparented = leaf.clone();
            reparented.path = dotted.clone();
            if let Some(spec) = reparented.variant.as_ref() {
                // Nested variant inside a shape is not supported.
                let _ = spec;
                return Err(DefError::VariantConfigOnNonVariant(dotted));
            }
            if let Some(vspec) = reparented.vector.as_mut() {
                if !vspec.source_field.is_empty() && !vspec.source_field.contains('.') {
                    vspec.source_field = format!("{column}.{}", vspec.source_field);
                }
            }
            let resolved = if reparented.ty == Some(FieldType::Vector) {
                resolve_vector_field(&reparented)?
            } else {
                // A shape leaf is a declared path, not a source leaf — its type is whatever the
                // author declares (defaulting to KEYWORD, since a variant leaf has no source type
                // to auto-derive from). `resolve_field` with `SourceType::Other` auto-derives
                // KEYWORD when no `type` is given.
                resolve_field(&dotted, SourceType::Other, Some(&reparented))?
            };
            // Cross-shape type agreement on the shared path name.
            match first_seen.get(&dotted) {
                Some((ty, prior_shape)) if *ty != resolved.ty => {
                    return Err(DefError::VariantShapeTypeConflict {
                        column: column.clone(),
                        path: dotted.clone(),
                        first: *ty,
                        first_shape: prior_shape.clone(),
                        second: resolved.ty,
                        second_shape: shape.name.clone(),
                    });
                }
                Some(_) => {} // same path + same type = the same field; don't re-emit
                None => {
                    first_seen.insert(dotted.clone(), (resolved.ty, shape.name.clone()));
                    leaves.push(resolved.clone());
                }
            }
            shape_paths.push((dotted, resolved.ty));
        }
        resolved_shapes.push(ResolvedShape {
            name: shape.name.clone(),
            when: shape.when.clone(),
            paths: shape_paths,
        });
    }

    // The variant field itself: no Tantivy typed field, only the flatten index + extraction spec.
    let variant_field = ResolvedField {
        path: column,
        ty: FieldType::Variant,
        analyzer: flatten.analyzer.clone(),
        format: None,
        fast: false,
        indexed: false,
        record: TextRecord::Position,
        fieldnorms: true,
        cached: false,
        sensitive: false,
        max_bytes: None,
        vector: None,
        variant: Some(ResolvedVariant {
            flatten,
            discriminator: opts.discriminator.clone(),
            shapes: resolved_shapes,
        }),
    };

    let mut out = Vec::with_capacity(1 + leaves.len());
    out.push(variant_field);
    out.extend(leaves);
    Ok(out)
}

/// One field mapping entry: `path`, `type`, `analyzer`, and the `fast` / `cached`
/// flags. Other Design-04 keys (`sensitive`, `sub_fields`, …) are still accepted and
/// ignored so a fuller definition parses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldMapping {
    /// Dotted leaf path, e.g. `actor.user`.
    pub path: String,
    /// Field type; auto-derived from the source when omitted.
    #[serde(default, rename = "type", skip_serializing_if = "Option::is_none")]
    pub ty: Option<FieldType>,
    /// Analyzer name for TEXT fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analyzer: Option<String>,
    /// Timestamp source format. Declaring it makes the field a **DATE** regardless of its
    /// source Arrow type (so a plain `int64` epoch column or a digit string can be a real timestamp)
    /// and tells ingestion how to normalize the source value to canonical epoch micros.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<crate::timestamp::TimeFormat>,
    /// Columnar **fast** field — sortable / filterable / aggregatable in-index.
    /// Default false.
    #[serde(default)]
    pub fast: bool,
    /// Whether the field gets an **inverted index**. Defaults per type: TEXT/KEYWORD
    /// always `true` (string search has no columnar fallback); numeric/date/IP default to
    /// `!fast` — a fast field's range / exact-match / sort / exists paths all run on the
    /// columnar store, so its inverted index is dead weight (a per-doc-unique timestamp is the
    /// worst case). Set `indexed: true` alongside `fast: true` to keep both.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed: Option<bool>,
    /// TEXT-only: how much the inverted index records per posting —
    /// `BASIC` (doc ids), `FREQ` (+ term frequencies, full BM25), or `POSITION`
    /// (+ token positions, phrase queries). Default `POSITION` (full fidelity);
    /// drop to `FREQ` on text fields never phrase-searched — positions are usually
    /// the largest slice of a text field's inverted index. Rejected on non-TEXT.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record: Option<TextRecord>,
    /// TEXT-only: store per-doc **fieldnorms** (field lengths, the BM25
    /// length-normalization input). Default true; `false` drops ~1 byte/doc for text
    /// fields whose relevance ranking doesn't matter (pure filter/needle fields) —
    /// BM25 then scores without length normalization. Rejected on non-TEXT.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fieldnorms: Option<bool>,
    /// **Cached** display field — the value is stored in-index and returned with the
    /// hit, so a page renders without hydration. Default false.
    #[serde(default)]
    pub cached: bool,
    /// **Sensitive** field (eventually catalog/Polaris-marked) — hard-blocked
    /// from caching; requesting `cached` on it is a resolve error. Default false.
    #[serde(default)]
    pub sensitive: bool,
    /// Declared maximum byte length of the field's values, if known (from the catalog
    /// or the author). Drives the **big-text** rule: over [`MAX_CACHED_FIELD_BYTES`]
    /// the field is hydrate-only and cannot be `cached`. Default unknown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    /// VECTOR-only: embedding config (dims, model, metric, provider, source_field).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorMappingOpts>,
    /// VARIANT-only: flatten + shapes config (D47). Optional — a bare `type: VARIANT` gets the
    /// default flatten (untyped path terms, no text catch-all, no shapes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<VariantMappingOpts>,
}

/// GrowlerDB field type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FieldType {
    /// Analyzed full-text (BM25-searchable).
    Text,
    /// Exact-match token (not analyzed).
    Keyword,
    /// 64-bit signed integer — range, sort, numeric facets.
    Long,
    /// 64-bit float — range, sort.
    Double,
    /// Boolean.
    Bool,
    /// Date / timestamp — range, date-histogram, time pruning.
    Date,
    /// IP address — CIDR / range match (e.g. device/gateway IPs). Declared explicitly; never auto-derived
    /// (Iceberg/Arrow has no IP type — it arrives as a string the user types as `IP`).
    Ip,
    /// Dense embedding vector — KNN / semantic retrieval. Declared explicitly with a
    /// [`vector`](FieldMapping::vector) config; never auto-derived (the source has no vector
    /// column — the embedding is produced from a text `source_field` at ingest).
    Vector,
    /// An Iceberg v3 **variant** column — a semi-structured value indexed through two composable
    /// modes ([variant fields](../../../okf/product/functional/index-management/variant.md), D47):
    /// an untyped **flatten** catch-all (every leaf as a `path = value` term, plus an optional
    /// analyzed TEXT catch-all) and discriminator-selected **shapes** (typed sub-mappings). Declared
    /// explicitly via [`variant`](FieldMapping::variant) (never auto-derived — a variant has no fixed
    /// leaf schema); shape leaves resolve to their own dotted `column.path` fields, the variant field
    /// carrying only the flatten index.
    Variant,
}

impl FieldType {
    /// Whether this type is analyzed full-text (only [`Text`](FieldType::Text)).
    pub fn is_text(self) -> bool {
        matches!(self, FieldType::Text)
    }
}

/// Default embedding model id for a VECTOR field ([`VectorSpec::model`]) when the
/// author declares none — the small English BGE model the local runtime targets.
pub const DEFAULT_EMBED_MODEL: &str = "bge-small-en-v1.5";
/// Default embedding dimensionality ([`VectorSpec::dims`]) — the width of
/// [`DEFAULT_EMBED_MODEL`]'s output.
pub const DEFAULT_EMBED_DIMS: usize = 384;

/// Vector distance metric for KNN.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum VectorMetric {
    /// Cosine similarity (the default) — angle between vectors, scale-invariant.
    #[default]
    Cosine,
    /// Dot product — raw inner product (assumes normalized inputs for cosine parity).
    Dot,
    /// Euclidean (L2) distance.
    L2,
}

/// Where a vector field's embeddings are produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EmbedProvider {
    /// In-process, no egress (the default) — the built-in embedder.
    #[default]
    Local,
    /// An external embedding service — not yet supported.
    External,
}

/// Configuration for a VECTOR field: the embedding model, its dimensionality,
/// the distance metric, the provider, and the text field embedded to produce it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorSpec {
    /// Embedding dimensionality (vector length). Must be > 0.
    pub dims: usize,
    /// Embedding model id, recorded for reproducibility (a change ⇒ re-embedding reindex).
    pub model: String,
    /// Distance metric. Default COSINE.
    #[serde(default)]
    pub metric: VectorMetric,
    /// Embedding provider. Default LOCAL (in-process, no egress).
    #[serde(default)]
    pub provider: EmbedProvider,
    /// The text field path whose value is embedded to produce this vector.
    pub source_field: String,
}

/// Authoring DTO for a VECTOR field's [`vector`](FieldMapping::vector) config: every
/// tuning knob is optional so an author can write `vector: { source_field: body }` and
/// get the defaults; only [`source_field`](Self::source_field) is required. Resolved into
/// a concrete [`VectorSpec`] at [`resolve`](IndexDefinition::resolve).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorMappingOpts {
    /// Embedding dimensionality — defaults to [`DEFAULT_EMBED_DIMS`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dims: Option<usize>,
    /// Embedding model id — defaults to [`DEFAULT_EMBED_MODEL`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Distance metric — defaults to [`VectorMetric::Cosine`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<VectorMetric>,
    /// Embedding provider — defaults to [`EmbedProvider::Local`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<EmbedProvider>,
    /// Required: the text field to embed.
    pub source_field: String,
}

// ---- Variant field config (D47) --------------------------------------------

/// Authoring DTO for a VARIANT field's [`variant`](FieldMapping::variant) config: the two
/// composable modes of [D47](../../../okf/system/decisions/d47-variant-mapping.md) — schema-less
/// [`flatten`](Self::flatten) and declared [`shapes`](Self::shapes) selected by a
/// [`discriminator`](Self::discriminator). Both are optional and compose: flatten alone is fully
/// schema-less, shapes alone is schema-only, both gives typed declared paths plus a flattened
/// catch-all over the whole value. Resolved into a [`ResolvedVariant`] at
/// [`resolve`](IndexDefinition::resolve).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VariantMappingOpts {
    /// Flatten (schema-less) config. Defaults to **enabled** (path terms on, no text catch-all) —
    /// a bare `type: VARIANT` gets the untyped flatten index with no declaration, per D47.
    #[serde(default)]
    pub flatten: FlattenConfig,
    /// The **discriminator** path selecting a shape per row — dotted, relative to the variant
    /// column (a path inside the variant, e.g. `type`) or a sibling top-level column (e.g.
    /// `event_type`). Required iff [`shapes`](Self::shapes) is non-empty; ignored otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discriminator: Option<String>,
    /// Declared typed sub-schemas, selected per row by the discriminator. Empty ⇒ flatten-only.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shapes: Vec<VariantShape>,
}

/// Flatten (schema-less) config for a VARIANT column. Enabled by default; set `flatten: false`
/// (or `enabled: false`) for a shapes-only column. `terms` indexes every leaf as an exact
/// `path = value` term; `text` feeds string leaves into one analyzed TEXT catch-all for full-text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlattenConfig {
    /// Whether flatten is enabled at all. `false` ⇒ the column is shapes-only (no flatten index).
    pub enabled: bool,
    /// Index every leaf as an exact `path = value` term (the flatten term field). Default true.
    pub terms: bool,
    /// Feed the value's string leaves into one analyzed TEXT catch-all for full-text (BM25)
    /// search over the whole object. Default false.
    pub text: bool,
    /// Analyzer for the text catch-all (only meaningful when `text` is set). Default analyzer
    /// when unset.
    pub analyzer: Option<String>,
}

impl Default for FlattenConfig {
    fn default() -> Self {
        // A bare `type: VARIANT` (no `variant.flatten` key) ⇒ untyped flatten with path terms,
        // no text catch-all — "no declaration needed" (D47).
        Self {
            enabled: true,
            terms: true,
            text: false,
            analyzer: None,
        }
    }
}

/// Wire form of [`FlattenConfig`]: authored as either a toggle (`flatten: false` /
/// `flatten: true`) or an options map (`flatten: { text: true, analyzer: en }`). Absent ⇒ the
/// [`Default`] (enabled, terms-on). A toggle `true` is the default enabled config; `false`
/// disables flatten entirely.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
enum FlattenWire {
    Toggle(bool),
    Opts(FlattenOpts),
}

/// The options-map form of [`FlattenConfig`] on the wire — every knob optional.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FlattenOpts {
    #[serde(default = "default_true")]
    terms: bool,
    #[serde(default)]
    text: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    analyzer: Option<String>,
}

/// Default for a bare `flatten` toggle / an omitted `terms` — path terms on.
fn default_true() -> bool {
    true
}

impl Default for FlattenWire {
    fn default() -> Self {
        FlattenWire::Toggle(true)
    }
}

impl From<FlattenWire> for FlattenConfig {
    fn from(w: FlattenWire) -> Self {
        match w {
            FlattenWire::Toggle(true) => FlattenConfig::default(),
            FlattenWire::Toggle(false) => FlattenConfig {
                enabled: false,
                terms: false,
                text: false,
                analyzer: None,
            },
            FlattenWire::Opts(o) => FlattenConfig {
                enabled: true,
                terms: o.terms,
                text: o.text,
                analyzer: o.analyzer,
            },
        }
    }
}

impl From<FlattenConfig> for FlattenWire {
    fn from(c: FlattenConfig) -> Self {
        if !c.enabled {
            FlattenWire::Toggle(false)
        } else {
            FlattenWire::Opts(FlattenOpts {
                terms: c.terms,
                text: c.text,
                analyzer: c.analyzer,
            })
        }
    }
}

impl Serialize for FlattenConfig {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        FlattenWire::from(self.clone()).serialize(s)
    }
}

impl<'de> Deserialize<'de> for FlattenConfig {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(FlattenWire::deserialize(d)?.into())
    }
}

/// One declared **shape**: a named typed sub-mapping of paths under a variant column, selected
/// for a row when the [discriminator](VariantMappingOpts::discriminator) equals one of
/// [`when`](Self::when). Field `path`s are relative to the variant column; the resolved field
/// name is the dotted `<column>.<path>`, shared across shapes (same path + same type = one field;
/// a cross-shape type disagreement is a create-time error).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariantShape {
    /// Shape name — for diagnostics and the unmatched-discriminator ingest counter's labelling.
    pub name: String,
    /// Discriminator value(s) that select this shape. A row whose discriminator equals any of
    /// these matches; a row matching no shape skips typed extraction (counted, D45) but stays
    /// flatten-covered. Accepts a scalar or a list in YAML.
    #[serde(default, deserialize_with = "string_or_seq")]
    pub when: Vec<String>,
    /// Typed sub-mappings. Each entry's `path` is relative to the variant column and gets the full
    /// field-type/flag surface (TEXT/KEYWORD/LONG/DOUBLE/BOOL/DATE/IP/VECTOR, `fast`/`cached`/…).
    pub fields: Vec<FieldMapping>,
}

/// Deserialize a `Vec<String>` from either a scalar (`when: opened`) or a sequence
/// (`when: [opened, closed]`) — the ergonomic discriminator-match surface.
fn string_or_seq<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    Ok(match OneOrMany::deserialize(d)? {
        OneOrMany::One(s) => vec![s],
        OneOrMany::Many(v) => v,
    })
}

/// How much the inverted index records per TEXT-field posting — the
/// Elasticsearch `index_options` / Quickwit `record` knob. Each level up costs bytes:
/// positions are typically the largest slice of a text field's inverted index, and
/// exist only to serve phrase queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TextRecord {
    /// Doc ids only — filter-grade (BM25 scores with term frequency clamped to 1).
    Basic,
    /// Doc ids + term frequencies — full BM25, no phrase queries.
    Freq,
    /// Doc ids + frequencies + token positions — full BM25 and phrase queries.
    Position,
}

impl TextRecord {
    /// Whether this level records token positions (phrase-query support).
    pub fn has_positions(self) -> bool {
        matches!(self, TextRecord::Position)
    }
}

/// Auto-derive a GrowlerDB field type from a source type: textual sources become
/// full-text [`Text`](FieldType::Text), scalars map to their typed counterpart, and
/// anything else becomes an exact-match [`Keyword`](FieldType::Keyword) so every
/// field stays indexable. (`Ip` and `Vector` are never auto-derived — each needs an
/// explicit type.)
fn auto_type(ty: SourceType) -> FieldType {
    match ty {
        SourceType::String => FieldType::Text,
        SourceType::Long => FieldType::Long,
        SourceType::Double => FieldType::Double,
        SourceType::Bool => FieldType::Bool,
        SourceType::Date => FieldType::Date,
        SourceType::Binary | SourceType::Other => FieldType::Keyword,
    }
}

// ---- Source schema (source-agnostic) ---------------------------------------

/// A source-agnostic view of a source's schema: its leaf fields plus the key
/// hints (partition + identifier fields) used to derive the composite key.
///
/// The Iceberg/Arrow → `SourceSchema` mapping lives in `growlerdb-source`; other
/// connectors build the same shape, keeping this crate connector-free.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SourceSchema {
    /// Leaf fields, in source order.
    pub fields: Vec<SourceField>,
    /// Source-declared partition field paths (may be empty).
    pub partition_fields: Vec<String>,
    /// Source-declared identifier field paths (may be empty).
    pub identifier_fields: Vec<String>,
    /// Columns the source's **equality deletes** key on (Iceberg's equality field
    /// ids → paths), if any. Drives equality-delete handling at resolve time
    /// ([`classify_equality_deletes`]). Empty ⇒ none declared / not applicable.
    pub equality_delete_fields: Vec<String>,
}

impl SourceSchema {
    /// Build a schema from leaf fields and key hints. Equality-delete columns
    /// default to none; add them with [`with_equality_delete_fields`](Self::with_equality_delete_fields).
    pub fn new(
        fields: Vec<SourceField>,
        partition_fields: Vec<String>,
        identifier_fields: Vec<String>,
    ) -> Self {
        Self {
            fields,
            partition_fields,
            identifier_fields,
            equality_delete_fields: Vec::new(),
        }
    }

    /// Declare the columns the source's equality deletes key on (builder).
    pub fn with_equality_delete_fields(mut self, fields: Vec<String>) -> Self {
        self.equality_delete_fields = fields;
        self
    }

    /// The field at `path`, if present.
    pub fn field(&self, path: &str) -> Option<&SourceField> {
        self.fields.iter().find(|f| f.path == path)
    }

    /// Whether `path` is a known leaf.
    pub fn has_field(&self, path: &str) -> bool {
        self.field(path).is_some()
    }
}

/// One leaf field of a source schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceField {
    /// Dotted leaf path.
    pub path: String,
    /// Coarse source type, mapped to a GrowlerDB field type at resolution.
    pub ty: SourceType,
}

impl SourceField {
    /// Convenience constructor.
    pub fn new(path: impl Into<String>, ty: SourceType) -> Self {
        Self {
            path: path.into(),
            ty,
        }
    }
}

/// Coarse, connector-agnostic source type. Connectors map their native types
/// (Arrow, Iceberg, …) onto these; resolution maps these onto [`FieldType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceType {
    /// UTF-8 string / large-string.
    String,
    /// Integer.
    Long,
    /// Floating point.
    Double,
    /// Boolean.
    Bool,
    /// Date / timestamp.
    Date,
    /// Binary / bytes.
    Binary,
    /// Anything not in the M0 subset (struct/list/map/decimal/…).
    Other,
}

// ---- Resolved output -------------------------------------------------------

/// A fully resolved index: concrete key + typed fields, validated against the
/// source. This is what downstream tasks (segment build, locator) consume.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedIndex {
    /// Index name.
    pub name: String,
    /// The source (carried through unchanged).
    pub source: Source,
    /// The resolved composite key.
    pub key: ResolvedKey,
    /// The resolved, typed field list.
    pub fields: Vec<ResolvedField>,
    /// How this source's equality deletes apply to the index.
    #[serde(default)]
    pub equality_deletes: EqualityDeleteHandling,
    /// Non-fatal resolution warnings to surface (e.g. an equality-delete column
    /// that forces the reconciliation fallback). Empty when nothing to flag.
    #[serde(default)]
    pub warnings: Vec<String>,
    /// Shard count the index is routed/built at (carried from the definition). Changing
    /// it re-routes every document → reindex-only (see [`alter_to`](Self::alter_to)).
    #[serde(default = "default_shard_count")]
    pub shard_count: u32,
    /// The tenant-scoping field, validated to be a mapped KEYWORD field. When set,
    /// reads inject a mandatory `tenant_field = <verified claim>` filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tenant_field: Option<String>,
    /// Time-window sharding, validated to a mapped fast DATE/LONG field. `None` = none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub windowing: Option<crate::window::TimeWindowing>,
    /// The index's [location strategy](LocationStrategy), carried from the definition.
    #[serde(default)]
    pub location_strategy: LocationStrategy,
}

impl ResolvedIndex {
    /// Each temporal field's declared [`TimeFormat`] by path — so the `_search` adapter can convert a
    /// range/exact bound written in that unit (e.g. `epoch_s`) to canonical micros before the query is
    /// planned, keeping window pruning and segment execution (both micros-native) consistent.
    pub fn date_formats(&self) -> std::collections::HashMap<String, crate::timestamp::TimeFormat> {
        self.fields
            .iter()
            .filter_map(|f| f.format.map(|fmt| (f.path.clone(), fmt)))
            .collect()
    }

    /// The tenant-scoping field, if this index is tenant-scoped. When `Some`, every
    /// read must carry a verified tenant claim and is filtered to `tenant_field = claim`.
    pub fn tenant_field(&self) -> Option<&str> {
        self.tenant_field.as_deref()
    }
    /// How this index's documents map to shards: **partition** routing when
    /// the key has partition fields (co-locating a partition on one shard so partition-scoped
    /// queries hit fewer shards), else **hash** (a uniform spread). Both the read side
    /// (Gateway) and the write side (connector) derive their [`ShardRouter`] from this, so
    /// placement and lookup agree.
    pub fn routing_strategy(&self) -> RoutingStrategy {
        if self.key.partition_fields.is_empty() {
            RoutingStrategy::Hash
        } else {
            RoutingStrategy::Partition
        }
    }

    /// The [`ShardRouter`] this index uses over `shards` shards — its
    /// [`routing_strategy`](Self::routing_strategy) at the given width.
    pub fn shard_router(&self, shards: u32) -> ShardRouter {
        ShardRouter::new(shards, self.routing_strategy())
    }

    /// Whether this index maps an Iceberg v3 **variant** column (D47). The per-index **routing
    /// fork** ([D48](../../../okf/system/decisions/d48-variant-delivery.md)): a variant table routes
    /// schema introspection, hydration, stats, and snapshot polling around released iceberg-rust
    /// (which can't parse a v3 variant schema); a non-variant index keeps the native path.
    pub fn has_variant_field(&self) -> bool {
        self.fields.iter().any(|f| f.ty == FieldType::Variant)
    }

    /// The variant column name(s) mapped by this index, in definition order (usually one). Used by
    /// the connector (extraction) and the Trino hydration seam (render `CAST(col AS JSON)`).
    pub fn variant_columns(&self) -> Vec<&str> {
        self.fields
            .iter()
            .filter(|f| f.ty == FieldType::Variant)
            .map(|f| f.path.as_str())
            .collect()
    }
}

/// The impact of **altering** an index from one resolved definition to another:
/// whether the change needs a full **reindex** (existing immutable segments
/// lack the new shape) or is safe to apply **in place**, with human-readable reasons so
/// an operator is *guided*, not surprised.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct AlterPlan {
    /// Changes that force a rebuild — the on-disk segments don't carry the new schema
    /// (type/analyzer/fast/cached/field-set/key/source changes).
    pub reindex_reasons: Vec<String>,
    /// Changes safe to apply without rebuilding — metadata only (rename, the
    /// `sensitive` gate, a `max_bytes` declaration), no stored/indexed data differs.
    pub in_place: Vec<String>,
}

impl AlterPlan {
    /// Whether applying this alter requires a full reindex.
    pub fn requires_reindex(&self) -> bool {
        !self.reindex_reasons.is_empty()
    }

    /// Whether the two definitions are identical (no alter at all).
    pub fn is_noop(&self) -> bool {
        self.reindex_reasons.is_empty() && self.in_place.is_empty()
    }
}

impl ResolvedIndex {
    /// Compute the [`AlterPlan`] for changing **this** definition into `new`. Because
    /// segments are immutable and their Tantivy schema is fixed at build time, any
    /// change to the field set, a field's stored/indexed representation
    /// (type/analyzer/`fast`/`cached`), the composite key, or the source forces a
    /// reindex; a rename, a `sensitive` flip, or a `max_bytes` redeclaration is in-place.
    pub fn alter_to(&self, new: &ResolvedIndex) -> AlterPlan {
        let mut plan = AlterPlan::default();

        if self.name != new.name {
            plan.in_place
                .push(format!("renamed `{}` → `{}`", self.name, new.name));
        }
        if self.source != new.source {
            plan.reindex_reasons
                .push("source changed (different data)".to_string());
        }
        if self.shard_count != new.shard_count {
            // Routing is `route(key) % shard_count`, so a count change re-routes ~every doc —
            // a reindex-only operation (no in-place migration).
            plan.reindex_reasons.push(format!(
                "shard_count {} → {} (re-routes every doc; reshard via reindex)",
                self.shard_count, new.shard_count
            ));
        }
        if self.location_strategy != new.location_strategy {
            // PREDICATE docs carry no `_locid` value and no location slots; COORDINATES
            // needs both, and non-cached field values aren't stored in Tantivy, so the
            // location layers can't be rebuilt in place — either direction is a rebuild.
            plan.reindex_reasons.push(format!(
                "location_strategy {:?} → {:?} (location data must be rebuilt)",
                self.location_strategy, new.location_strategy
            ));
        }
        if self.key.partition_fields != new.key.partition_fields {
            plan.reindex_reasons
                .push("key.partition_fields changed (re-keys every doc)".to_string());
        }
        if self.key.identifier_fields != new.key.identifier_fields {
            plan.reindex_reasons
                .push("key.identifier_fields changed (re-keys every doc)".to_string());
        }

        // Compare fields by path over the union, in a deterministic order.
        let mut paths: Vec<&str> = self
            .fields
            .iter()
            .chain(new.fields.iter())
            .map(|f| f.path.as_str())
            .collect();
        paths.sort_unstable();
        paths.dedup();
        for path in paths {
            match (
                find_field(&self.fields, path),
                find_field(&new.fields, path),
            ) {
                (Some(_), None) => plan.reindex_reasons.push(format!("field `{path}` removed")),
                (None, Some(_)) => plan.reindex_reasons.push(format!("field `{path}` added")),
                (Some(a), Some(b)) => diff_field(path, a, b, &mut plan),
                (None, None) => unreachable!("path came from the union"),
            }
        }
        plan
    }
}

/// The resolved field at `path`, if present.
fn find_field<'a>(fields: &'a [ResolvedField], path: &str) -> Option<&'a ResolvedField> {
    fields.iter().find(|f| f.path == path)
}

/// Append the [`AlterPlan`] reasons for a field present in both definitions: a
/// stored/indexed-representation change forces a reindex; a policy/declaration change
/// is in-place.
fn diff_field(path: &str, old: &ResolvedField, new: &ResolvedField, plan: &mut AlterPlan) {
    if old.ty != new.ty {
        plan.reindex_reasons
            .push(format!("field `{path}` type {:?} → {:?}", old.ty, new.ty));
    }
    if old.analyzer != new.analyzer {
        plan.reindex_reasons.push(format!(
            "field `{path}` analyzer {:?} → {:?}",
            old.analyzer, new.analyzer
        ));
    }
    if old.record != new.record {
        // Postings carry the recorded detail (freqs/positions) physically — either
        // direction is a rebuild.
        plan.reindex_reasons.push(format!(
            "field `{path}` record {:?} → {:?}",
            old.record, new.record
        ));
    }
    if old.fieldnorms != new.fieldnorms {
        plan.reindex_reasons.push(format!(
            "field `{path}` fieldnorms {} → {}",
            old.fieldnorms, new.fieldnorms
        ));
    }
    if old.fast != new.fast {
        plan.reindex_reasons
            .push(format!("field `{path}` fast {} → {}", old.fast, new.fast));
    }
    if old.indexed != new.indexed {
        // The inverted index either exists in the segments or it doesn't — a flip in either
        // direction changes the on-disk shape and can only be honored by a rebuild.
        plan.reindex_reasons.push(format!(
            "field `{path}` indexed {} → {}",
            old.indexed, new.indexed
        ));
    }
    if old.cached != new.cached {
        plan.reindex_reasons.push(format!(
            "field `{path}` cached {} → {}",
            old.cached, new.cached
        ));
    }
    if old.sensitive != new.sensitive {
        plan.in_place.push(format!(
            "field `{path}` sensitive {} → {}",
            old.sensitive, new.sensitive
        ));
    }
    if old.max_bytes != new.max_bytes {
        plan.in_place
            .push(format!("field `{path}` max_bytes redeclared"));
    }
    if old.vector != new.vector {
        // The stored embeddings are produced from this exact config (model/dims/metric/
        // source_field) — any change means every vector must be regenerated → reindex.
        plan.reindex_reasons
            .push(format!("field `{path}` vector config changed (re-embed)"));
    }
    if old.variant != new.variant {
        // The flatten term/text fields and the extracted shape leaves are all physically written
        // per the variant config — any change (flatten toggle, discriminator, a shape's paths) is
        // a rebuild. Shape-leaf field add/remove is separately caught by the field-set diff.
        plan.reindex_reasons
            .push(format!("field `{path}` variant config changed"));
    }
}

/// How a source's **equality deletes** are applied to the index ([equality
/// deletes](../../../okf/product/functional/ingestion/index.md)). GrowlerDB is keyed by the
/// composite document key, so an equality delete on the key columns is just a
/// `delete_by_key`; anything else needs a partition re-scan.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EqualityDeleteHandling {
    /// The source declares no equality-delete columns — nothing to special-case.
    #[default]
    None,
    /// Equality-delete columns ⊆ the composite key → apply each as `delete_by_key`.
    DeleteByKey,
    /// At least one equality-delete column is outside the key → partition-scoped
    /// reconciliation. `uncovered` holds any column outside key *and* indexed
    /// fields (it can't even narrow the diff).
    Reconcile {
        /// Equality-delete columns outside both key and indexed fields.
        uncovered: Vec<String>,
    },
}

/// A resolved composite key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedKey {
    /// Partition field paths.
    pub partition_fields: Vec<String>,
    /// Identifier field paths.
    pub identifier_fields: Vec<String>,
}

/// A resolved field: a concrete path, type, and optional analyzer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedField {
    /// Dotted leaf path.
    pub path: String,
    /// Resolved GrowlerDB field type.
    pub ty: FieldType,
    /// Analyzer name (TEXT only).
    pub analyzer: Option<String>,
    /// Timestamp source format — present iff the field was declared a timestamp; tells
    /// ingestion how to normalize the source value to canonical epoch micros. `None` for a native
    /// DATE (already a date in the source) or a non-date field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<crate::timestamp::TimeFormat>,
    /// Columnar fast field — sortable / filterable / aggregatable.
    #[serde(default)]
    pub fast: bool,
    /// Whether the field gets an inverted index — see [`FieldMapping::indexed`]
    /// for the per-type default. Always explicit in the persisted form: the derived Tantivy
    /// schema hangs off it, so it must never be guessed at rehydration.
    pub indexed: bool,
    /// TEXT indexing detail: what each posting records — see
    /// [`FieldMapping::record`]. `POSITION` (and irrelevant) for non-TEXT fields.
    pub record: TextRecord,
    /// TEXT fieldnorms — see [`FieldMapping::fieldnorms`]. `true` (and
    /// irrelevant) for non-TEXT fields.
    pub fieldnorms: bool,
    /// Cached display field — value stored in-index and returned with the hit.
    #[serde(default)]
    pub cached: bool,
    /// Sensitive field — never cacheable. See [`FieldMapping::sensitive`].
    #[serde(default)]
    pub sensitive: bool,
    /// Declared max byte length, if known — drives the big-text caching rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<u64>,
    /// Present iff `ty == Vector`: the resolved embedding config.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vector: Option<VectorSpec>,
    /// Present iff `ty == Variant`: the resolved flatten + shape-extraction config. The variant
    /// field itself carries only the flatten index (the node builds `<col>#terms`/`<col>#text`
    /// from it); each shape leaf is a **separate** [`ResolvedField`] at its dotted `column.path`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<ResolvedVariant>,
}

/// The resolved config of a VARIANT field ([`ResolvedField::variant`]): the flatten index plus
/// the discriminator + shapes the **connector** uses to extract typed leaves (D47/D48). The
/// resolved typed leaves themselves are emitted as separate top-level [`ResolvedField`]s (so the
/// node's segment schema carries them); this struct is the extraction spec + flatten settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedVariant {
    /// The flatten (schema-less) index settings.
    pub flatten: FlattenConfig,
    /// The discriminator path (dotted, relative to the variant column or a sibling column), or
    /// `None` when the column is flatten-only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discriminator: Option<String>,
    /// The declared shapes, each a name + discriminator match values + the resolved leaf paths it
    /// extracts. Empty for a flatten-only column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub shapes: Vec<ResolvedShape>,
}

/// One resolved shape ([`ResolvedVariant::shapes`]): its name, the discriminator values that
/// select it, and the resolved leaf paths/types it extracts (dotted `column.path`, matching the
/// separate top-level [`ResolvedField`]s). The connector reads this to run `variant_get` for each
/// path on a row whose discriminator matches [`when`](Self::when).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedShape {
    /// Shape name.
    pub name: String,
    /// Discriminator values that select this shape.
    pub when: Vec<String>,
    /// The resolved leaf paths this shape extracts, as `(dotted column.path, type)`.
    pub paths: Vec<(String, FieldType)>,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small source: a text body, a string-ish id, plus partition/identifier hints.
    fn docs_schema() -> SourceSchema {
        SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
                SourceField::new("count", SourceType::Long),
                SourceField::new("day", SourceType::Date),
            ],
            vec!["day".into()],
            vec!["id".into()],
        )
    }

    /// The name becomes a shard directory + object prefix, so traversal/odd charsets are
    /// refused at parse time — the single chokepoint every definition passes through.
    #[test]
    fn index_names_unusable_as_path_components_are_rejected() {
        let yaml_with = |name: &str| {
            format!(
                "name: \"{name}\"\nsource: {{ iceberg: {{ catalog: g, table: g.docs }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD }} ] }}\n"
            )
        };
        for bad in [
            "../../evil",
            "a/b",
            "a\\b",
            "with space",
            "dot.dot",
            "",
            &"x".repeat(129),
        ] {
            let err = IndexDefinition::from_yaml(&yaml_with(bad)).unwrap_err();
            assert!(
                matches!(err, DefError::InvalidName(_)),
                "`{bad}` must be rejected, got: {err}"
            );
        }
        for good in ["docs", "http_logs-2026", "A1_b-C"] {
            IndexDefinition::from_yaml(&yaml_with(good)).expect(good);
        }
    }

    #[test]
    fn parses_minimal_yaml_with_key_and_fields() {
        let yaml = r#"
name: docs
source:
  iceberg: { catalog: growlerdb, table: growlerdb.docs }
key:
  partition_fields: [day]
  identifier_fields: [id]
mapping:
  selection: EXPLICIT
  fields:
    - { path: body, type: TEXT, analyzer: english }
    - { path: id, type: KEYWORD }
"#;
        let def = IndexDefinition::from_yaml(yaml).expect("parse");
        assert_eq!(def.name, "docs");
        assert_eq!(
            def.source,
            Source::Iceberg(IcebergSource {
                catalog: "growlerdb".into(),
                table: "growlerdb.docs".into(),
                scan: ScanMode::Changelog,
            })
        );
        assert_eq!(def.key.partition_fields, vec!["day".to_string()]);
        assert_eq!(def.mapping.selection, Selection::Explicit);
        assert_eq!(def.mapping.fields.len(), 2);
    }

    #[test]
    fn derives_composite_key_from_source() {
        // No explicit key → derive partition + identifier from the source hints.
        let def = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\n",
        )
        .unwrap();
        let r = def.resolve(&docs_schema()).expect("resolve");
        assert_eq!(r.key.partition_fields, vec!["day".to_string()]);
        assert_eq!(r.key.identifier_fields, vec!["id".to_string()]);
    }

    #[test]
    fn explicit_key_overrides_source_derivation() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nkey: { identifier_fields: [body] }\n";
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .expect("resolve");
        // identifier overridden to `body`; partition still derived from source.
        assert_eq!(r.key.identifier_fields, vec!["body".to_string()]);
        assert_eq!(r.key.partition_fields, vec!["day".to_string()]);
    }

    #[test]
    fn selection_all_auto_maps_every_field() {
        let def = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\n",
        )
        .unwrap();
        let r = def.resolve(&docs_schema()).unwrap();
        // One resolved field per source leaf.
        assert_eq!(r.fields.len(), 4);
        let ty = |p: &str| r.fields.iter().find(|f| f.path == p).unwrap().ty;
        assert_eq!(ty("body"), FieldType::Text); // string → TEXT
        assert_eq!(ty("count"), FieldType::Long); // long → LONG (typed)
        assert_eq!(ty("day"), FieldType::Date); // date → DATE (typed)
    }

    #[test]
    fn all_except_drops_excluded_fields() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: ALL_EXCEPT, exclude: [count, day] }\n";
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        let paths: Vec<&str> = r.fields.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, vec!["id", "body"], "count + day excluded");
    }

    #[test]
    fn all_except_unknown_exclude_path_is_rejected() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: ALL_EXCEPT, exclude: [nope] }\n";
        let err = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(matches!(
            err,
            DefError::UnknownPath {
                referenced_by: "mapping.exclude",
                ..
            }
        ));
    }

    #[test]
    fn fast_and_cached_flags_resolve_from_overrides() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping:\n  selection: ALL\n  fields:\n    - { path: count, fast: true }\n    - { path: body, cached: true }\n";
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        let f = |p: &str| r.fields.iter().find(|x| x.path == p).unwrap();
        assert!(f("count").fast && !f("count").cached);
        assert!(f("body").cached && !f("body").fast);
        assert!(
            !f("id").fast && !f("id").cached,
            "unflagged fields default false"
        );
    }

    #[test]
    fn fast_non_text_defaults_to_columnar_only() {
        // `count` is fast → no inverted index by default; `day` (DATE, not fast) keeps it;
        // `body` (TEXT) is always indexed, fast or not.
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping:\n  selection: ALL\n  fields:\n    - { path: count, fast: true }\n    - { path: body, fast: true }\n";
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        let f = |p: &str| r.fields.iter().find(|x| x.path == p).unwrap();
        assert!(
            f("count").fast && !f("count").indexed,
            "fast LONG → columnar-only"
        );
        assert!(
            f("day").indexed && !f("day").fast,
            "non-fast DATE stays indexed"
        );
        assert!(
            f("body").indexed && f("body").fast,
            "TEXT is always indexed"
        );
    }

    #[test]
    fn explicit_indexed_true_keeps_both_structures() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: count, fast: true, indexed: true } ] }\n";
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        assert!(r.fields[0].fast && r.fields[0].indexed);
    }

    #[test]
    fn indexed_false_on_text_is_rejected() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT, indexed: false, fast: true } ] }\n";
        let err = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(
            matches!(err, DefError::IndexedFalseText(p) if p == "body"),
            "text can't opt out"
        );
    }

    #[test]
    fn indexed_false_without_fast_is_rejected() {
        // Neither inverted nor columnar → unqueryable; loud error, not a silent dead field.
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: count, indexed: false } ] }\n";
        let err = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(matches!(err, DefError::IndexedFalseNotFast(p) if p == "count"));
    }

    #[test]
    fn text_record_and_fieldnorms_default_to_full_fidelity() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping:\n  selection: EXPLICIT\n  fields:\n    - { path: body, type: TEXT }\n    - { path: id, type: TEXT, record: FREQ, fieldnorms: false }\n";
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        let f = |p: &str| r.fields.iter().find(|x| x.path == p).unwrap();
        assert_eq!(
            f("body").record,
            TextRecord::Position,
            "default: phrase-safe"
        );
        assert!(f("body").fieldnorms, "default: BM25 length norm on");
        assert_eq!(f("id").record, TextRecord::Freq);
        assert!(!f("id").fieldnorms);
    }

    #[test]
    fn record_and_fieldnorms_are_rejected_on_non_text() {
        let record_on_long = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: count, record: BASIC } ] }\n";
        let err = IndexDefinition::from_yaml(record_on_long)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(matches!(err, DefError::RecordOnNonText(p) if p == "count"));

        let norms_on_keyword = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD, fieldnorms: false } ] }\n";
        let err = IndexDefinition::from_yaml(norms_on_keyword)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(matches!(err, DefError::FieldnormsOnNonText(p) if p == "id"));
    }

    #[test]
    fn flipping_record_or_fieldnorms_requires_reindex() {
        let old = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT } ] }\n",
        );
        let new = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT, record: FREQ, fieldnorms: false } ] }\n",
        );
        let plan = old.alter_to(&new);
        assert!(plan.requires_reindex());
        assert!(plan
            .reindex_reasons
            .iter()
            .any(|r| r.contains("`body` record Position → Freq")));
        assert!(plan
            .reindex_reasons
            .iter()
            .any(|r| r.contains("`body` fieldnorms true → false")));
    }

    #[test]
    fn explicit_ip_type_is_honoured() {
        // A string source field (`id`) typed explicitly as IP, marked fast.
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: IP, fast: true } ] }\n";
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        assert_eq!(r.fields[0].ty, FieldType::Ip);
        assert!(r.fields[0].fast);
    }

    #[test]
    fn timestamp_format_makes_an_int_column_a_date() {
        // `count` is a LONG source column; declaring `format: epoch_ms` makes it a DATE timestamp.
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: count, format: epoch_ms, fast: true } ] }\n";
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        assert_eq!(r.fields[0].ty, FieldType::Date);
        assert_eq!(
            r.fields[0].format,
            Some(crate::timestamp::TimeFormat::EpochMillis)
        );
        assert!(r.fields[0].fast);
    }

    #[test]
    fn a_format_with_a_contradictory_non_date_type_is_rejected() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: count, type: LONG, format: epoch_ms } ] }\n";
        let err = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema());
        assert!(
            matches!(err, Err(DefError::TimestampFormatType { .. })),
            "{err:?}"
        );
    }

    #[test]
    fn selection_all_applies_per_field_overrides() {
        let yaml = r#"
name: docs
source: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }
mapping:
  selection: ALL
  fields:
    - { path: id, type: KEYWORD }
    - { path: body, analyzer: english }
"#;
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        let f = |p: &str| r.fields.iter().find(|x| x.path == p).unwrap();
        // `id` is a string (would auto-map TEXT) but is overridden to KEYWORD.
        assert_eq!(f("id").ty, FieldType::Keyword);
        // `body` keeps its auto TEXT type but gains the analyzer override.
        assert_eq!(f("body").ty, FieldType::Text);
        assert_eq!(f("body").analyzer.as_deref(), Some("english"));
    }

    #[test]
    fn selection_explicit_indexes_only_listed_fields() {
        let yaml = r#"
name: docs
source: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }
mapping:
  selection: EXPLICIT
  fields:
    - { path: body, type: TEXT }
"#;
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        assert_eq!(r.fields.len(), 1);
        assert_eq!(r.fields[0].path, "body");
    }

    #[test]
    fn unknown_field_path_is_rejected() {
        let yaml = r#"
name: docs
source: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }
mapping:
  selection: EXPLICIT
  fields:
    - { path: nope, type: TEXT }
"#;
        let err = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(matches!(err, DefError::UnknownPath { .. }));
    }

    #[test]
    fn unknown_key_path_is_rejected() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nkey: { identifier_fields: [missing] }\n";
        let err = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(matches!(
            err,
            DefError::UnknownPath {
                referenced_by: "key",
                ..
            }
        ));
    }

    #[test]
    fn explicit_without_fields_is_rejected() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT }\n";
        let err = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(matches!(err, DefError::EmptyExplicit));
    }

    #[test]
    fn source_without_identifier_cannot_derive_key() {
        let schema = SourceSchema::new(
            vec![SourceField::new("body", SourceType::String)],
            vec![],
            vec![],
        );
        let def = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\n",
        )
        .unwrap();
        assert!(matches!(
            def.resolve(&schema).unwrap_err(),
            DefError::NoIdentifier
        ));
    }

    #[test]
    fn equality_delete_on_key_columns_is_delete_by_key() {
        // Equality deletes keyed on `id` (the identifier) → delete-by-key, no warning.
        let schema = docs_schema().with_equality_delete_fields(vec!["id".into()]);
        let r = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\n",
        )
        .unwrap()
        .resolve(&schema)
        .unwrap();
        assert_eq!(r.equality_deletes, EqualityDeleteHandling::DeleteByKey);
        assert!(r.warnings.is_empty(), "key-covered deletes need no warning");
    }

    #[test]
    fn no_equality_deletes_declared_is_none() {
        let r = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\n",
        )
        .unwrap()
        .resolve(&docs_schema())
        .unwrap();
        assert_eq!(r.equality_deletes, EqualityDeleteHandling::None);
        assert!(r.warnings.is_empty());
    }

    #[test]
    fn non_key_equality_delete_falls_back_to_reconcile_with_warning() {
        // `count` is indexed but not a key column → reconcile, and a warning, but
        // `count` is NOT in `uncovered` because it's still an indexed field.
        let schema = docs_schema().with_equality_delete_fields(vec!["count".into()]);
        let r = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\n",
        )
        .unwrap()
        .resolve(&schema)
        .unwrap();
        assert_eq!(
            r.equality_deletes,
            EqualityDeleteHandling::Reconcile { uncovered: vec![] }
        );
        assert_eq!(r.warnings.len(), 1);
        assert!(r.warnings[0].contains("count"));
    }

    #[test]
    fn equality_delete_outside_key_and_fields_is_flagged_uncovered() {
        // EXPLICIT mapping indexes only `body`; an equality delete on `count`
        // (not a key, not indexed) → reconcile with `count` flagged uncovered.
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body } ] }\n";
        let schema = docs_schema().with_equality_delete_fields(vec!["count".into()]);
        let r = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&schema)
            .unwrap();
        assert_eq!(
            r.equality_deletes,
            EqualityDeleteHandling::Reconcile {
                uncovered: vec!["count".into()]
            }
        );
        assert!(r.warnings[0].contains("indexed fields"));
    }

    #[test]
    fn sensitive_field_cannot_be_cached() {
        // Hard-block: marking a sensitive field `cached` is a resolve error.
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, cached: true, sensitive: true } ] }\n";
        let err = IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(matches!(err, DefError::SensitiveCached(f) if f == "body"));
    }

    #[test]
    fn big_text_field_cannot_be_cached() {
        // A field declared over the cache cap is hydrate-only — caching it errors.
        let over = MAX_CACHED_FIELD_BYTES + 1;
        let yaml = format!(
            "name: docs\nsource: {{ iceberg: {{ catalog: growlerdb, table: growlerdb.docs }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: body, cached: true, max_bytes: {over} }} ] }}\n"
        );
        let err = IndexDefinition::from_yaml(&yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap_err();
        assert!(matches!(
            err,
            DefError::BigTextCached { path, max_bytes, cap }
                if path == "body" && max_bytes == over && cap == MAX_CACHED_FIELD_BYTES
        ));
    }

    #[test]
    fn big_text_field_is_fine_when_not_cached_or_within_cap() {
        // Over the cap but not cached → allowed (it's hydrate-only, which is the point).
        let big_uncached = format!(
            "name: docs\nsource: {{ iceberg: {{ catalog: growlerdb, table: growlerdb.docs }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: body, max_bytes: {} }} ] }}\n",
            MAX_CACHED_FIELD_BYTES + 1
        );
        let r = IndexDefinition::from_yaml(&big_uncached)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap();
        let body = r.fields.iter().find(|f| f.path == "body").unwrap();
        assert!(!body.cached);

        // At the cap and cached → allowed (the rule triggers strictly above the cap).
        let at_cap = format!(
            "name: docs\nsource: {{ iceberg: {{ catalog: growlerdb, table: growlerdb.docs }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: body, cached: true, max_bytes: {MAX_CACHED_FIELD_BYTES} }} ] }}\n"
        );
        assert!(IndexDefinition::from_yaml(&at_cap)
            .unwrap()
            .resolve(&docs_schema())
            .is_ok());
    }

    #[test]
    fn float_key_field_is_rejected() {
        // A floating-point source field can't be a key — neither identifier nor partition:
        // floats are unstable identity/routing keys and NaN diverges cross-language.
        let id_src = SourceSchema::new(
            vec![
                SourceField::new("price", SourceType::Double),
                SourceField::new("name", SourceType::String),
            ],
            vec![],
            vec![],
        );
        let id_def = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nkey: { identifier_fields: [price] }\nmapping: { selection: EXPLICIT, fields: [ { path: name, type: KEYWORD } ] }\n",
        )
        .unwrap();
        assert!(matches!(id_def.resolve(&id_src), Err(DefError::FloatKey(f)) if f == "price"));

        let part_src = SourceSchema::new(
            vec![
                SourceField::new("bucket", SourceType::Double),
                SourceField::new("id", SourceType::String),
            ],
            vec![],
            vec![],
        );
        let part_def = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nkey: { partition_fields: [bucket], identifier_fields: [id] }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap();
        assert!(matches!(part_def.resolve(&part_src), Err(DefError::FloatKey(f)) if f == "bucket"));
    }

    #[test]
    fn temporal_key_fields_are_permitted() {
        // Date/timestamp source columns (both map to `SourceType::Date`) are legal key fields
        // in either role: they extract to `Value::Ts` (canonical epoch micros) and
        // encode/route deterministically, unlike floats.
        let src = SourceSchema::new(
            vec![
                SourceField::new("day", SourceType::Date),
                SourceField::new("created_at", SourceType::Date),
                SourceField::new("name", SourceType::String),
            ],
            vec![],
            vec![],
        );
        let def = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nkey: { partition_fields: [day], identifier_fields: [created_at] }\nmapping: { selection: EXPLICIT, fields: [ { path: name, type: KEYWORD } ] }\n",
        )
        .unwrap();
        let resolved = def.resolve(&src).unwrap();
        assert_eq!(resolved.key.partition_fields, vec!["day"]);
        assert_eq!(resolved.key.identifier_fields, vec!["created_at"]);
    }

    fn resolve_yaml(yaml: &str) -> ResolvedIndex {
        IndexDefinition::from_yaml(yaml)
            .unwrap()
            .resolve(&docs_schema())
            .unwrap()
    }

    #[test]
    fn tenant_field_must_be_a_mapped_keyword_field() {
        // Valid: a mapped KEYWORD field.
        let ok = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\ntenant_field: id\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&docs_schema())
        .unwrap();
        assert_eq!(ok.tenant_field(), Some("id"));

        // Not in the mapping → unmapped.
        let unmapped = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\ntenant_field: id\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT } ] }\n",
        )
        .unwrap()
        .resolve(&docs_schema());
        assert!(matches!(unmapped, Err(DefError::TenantFieldUnmapped(f)) if f == "id"));

        // Mapped, but not KEYWORD → rejected (exact match needs a raw field).
        let not_kw = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\ntenant_field: count\nmapping: { selection: EXPLICIT, fields: [ { path: count, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&docs_schema());
        assert!(matches!(not_kw, Err(DefError::TenantFieldNotKeyword(f)) if f == "count"));
    }

    #[test]
    fn windowing_field_must_be_a_mapped_fast_date_field() {
        // Valid: a mapped, fast **DATE** field (here a `count` LONG column declared `epoch_ms`, so it
        // resolves to a canonical-micros timestamp) → carried onto the resolved index.
        let ok = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nwindowing: { field: count, granularity: daily }\nmapping: { selection: EXPLICIT, fields: [ { path: count, format: epoch_ms, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&docs_schema())
        .unwrap();
        let w = ok.windowing.as_ref().expect("windowing carried");
        assert_eq!(w.field, "count");
        assert_eq!(w.granularity, crate::window::WindowGranularity::Daily);

        // Not in the mapping → unmapped.
        let unmapped = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nwindowing: { field: count, granularity: daily }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&docs_schema());
        assert!(matches!(unmapped, Err(DefError::WindowFieldUnmapped(f)) if f == "count"));

        // Mapped but not a timestamp (KEYWORD) → rejected.
        let not_time = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nwindowing: { field: id, granularity: daily }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap()
        .resolve(&docs_schema());
        assert!(matches!(not_time, Err(DefError::WindowFieldNotTime(f)) if f == "id"));

        // A raw LONG is rejected: its unit is ambiguous — declare a `format`.
        let raw_long = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nwindowing: { field: count, granularity: daily }\nmapping: { selection: EXPLICIT, fields: [ { path: count, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&docs_schema());
        assert!(matches!(raw_long, Err(DefError::WindowFieldNotTime(f)) if f == "count"));

        // DATE but not fast → rejected (range pruning needs the fast field).
        let not_fast = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nwindowing: { field: count, granularity: weekly }\nmapping: { selection: EXPLICIT, fields: [ { path: count, format: epoch_ms } ] }\n",
        )
        .unwrap()
        .resolve(&docs_schema());
        assert!(matches!(not_fast, Err(DefError::WindowFieldNotFast(f)) if f == "count"));
    }

    #[test]
    fn location_strategy_defaults_to_coordinates_and_round_trips() {
        // Omitted → COORDINATES, no honest-scope warning.
        let default_def = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\n",
        )
        .unwrap();
        assert_eq!(default_def.location_strategy, LocationStrategy::Coordinates);
        let resolved = default_def.resolve(&docs_schema()).unwrap();
        assert_eq!(resolved.location_strategy, LocationStrategy::Coordinates);
        assert!(resolved.warnings.is_empty());

        // Explicit PREDICATE parses, resolves through, and **round-trips** the wire
        // form (the definition travels as YAML over both gRPC `definition_yaml` and
        // the REST DTO, so serde round-trip *is* the wire round-trip).
        let yaml = "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nlocation_strategy: PREDICATE\n";
        let def = IndexDefinition::from_yaml(yaml).unwrap();
        assert_eq!(def.location_strategy, LocationStrategy::Predicate);
        let re = serde_norway::to_string(&def).unwrap();
        let back = IndexDefinition::from_yaml(&re).unwrap();
        assert_eq!(back, def, "definition round-trips through YAML");
        assert_eq!(back.location_strategy, LocationStrategy::Predicate);
    }

    #[test]
    fn predicate_strategy_resolves_with_an_honest_scope_warning() {
        let resolved = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nlocation_strategy: PREDICATE\n",
        )
        .unwrap()
        .resolve(&docs_schema())
        .unwrap();
        assert_eq!(resolved.location_strategy, LocationStrategy::Predicate);
        assert!(
            resolved
                .warnings
                .iter()
                .any(|w| w.contains("PREDICATE") && w.contains("degrades to broad scans")),
            "the honest-scope warning is emitted at resolve: {:?}",
            resolved.warnings
        );
        // And it JSON round-trips on the resolved form (the registry's stored shape).
        let json = serde_json::to_string(&resolved).unwrap();
        let back: ResolvedIndex = serde_json::from_str(&json).unwrap();
        assert_eq!(back.location_strategy, LocationStrategy::Predicate);
    }

    #[test]
    fn changing_location_strategy_requires_reindex() {
        let base = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT } ] }";
        let coords = resolve_yaml(&format!("{base}\n"));
        let pred = resolve_yaml(&format!("{base}\nlocation_strategy: PREDICATE\n"));
        let plan = coords.alter_to(&pred);
        assert!(plan.requires_reindex());
        assert!(plan
            .reindex_reasons
            .iter()
            .any(|r| r.contains("location_strategy")));
        assert!(
            !pred.alter_to(&pred).requires_reindex(),
            "same strategy: no-op"
        );
    }

    #[test]
    fn no_tenant_field_means_unscoped() {
        let idx = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        );
        assert_eq!(idx.tenant_field(), None);
    }

    #[test]
    fn alter_noop_when_definitions_are_identical() {
        let yaml = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT } ] }\n";
        let plan = resolve_yaml(yaml).alter_to(&resolve_yaml(yaml));
        assert!(plan.is_noop());
        assert!(!plan.requires_reindex());
    }

    #[test]
    fn changing_shard_count_requires_reindex() {
        // Resharding re-routes every doc, so a shard_count change is reindex-only —
        // never an in-place alter.
        let base = "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT } ] }";
        let old = resolve_yaml(&format!("{base}\nshard_count: 4\n"));
        let new = resolve_yaml(&format!("{base}\nshard_count: 8\n"));
        assert_eq!(old.shard_count, 4);
        let plan = old.alter_to(&new);
        assert!(plan.requires_reindex());
        assert!(plan
            .reindex_reasons
            .iter()
            .any(|r| r.contains("shard_count")));
        // Unspecified defaults to a single shard, and same-count is not a reindex.
        assert_eq!(resolve_yaml(&format!("{base}\n")).shard_count, 1);
        assert!(!old.alter_to(&old).requires_reindex());
    }

    #[test]
    fn altering_field_type_or_set_requires_reindex() {
        let old = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT } ] }\n",
        );
        // type TEXT → KEYWORD, plus a new `count` fast field.
        let new = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: KEYWORD }, { path: count, type: LONG, fast: true } ] }\n",
        );
        let plan = old.alter_to(&new);
        assert!(plan.requires_reindex());
        assert!(plan
            .reindex_reasons
            .iter()
            .any(|r| r.contains("`body` type")));
        assert!(plan
            .reindex_reasons
            .iter()
            .any(|r| r.contains("`count` added")));
    }

    #[test]
    fn flipping_fast_or_cached_requires_reindex() {
        let old = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: count, type: LONG } ] }\n",
        );
        let new = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: count, type: LONG, fast: true, cached: true } ] }\n",
        );
        let plan = old.alter_to(&new);
        assert!(plan.requires_reindex());
        assert!(plan.reindex_reasons.iter().any(|r| r.contains("fast")));
        assert!(plan.reindex_reasons.iter().any(|r| r.contains("cached")));
    }

    #[test]
    fn flipping_indexed_requires_reindex() {
        // Same YAML both sides except `indexed:` — the fast field defaults to columnar-only,
        // opting back in flips the segments' shape.
        let old = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: count, type: LONG, fast: true } ] }\n",
        );
        let new = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: count, type: LONG, fast: true, indexed: true } ] }\n",
        );
        let plan = old.alter_to(&new);
        assert!(plan.requires_reindex());
        assert!(plan
            .reindex_reasons
            .iter()
            .any(|r| r.contains("`count` indexed false → true")));
    }

    #[test]
    fn rename_and_sensitive_and_max_bytes_are_in_place() {
        let old = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT } ] }\n",
        );
        // rename + mark sensitive + declare max_bytes — none change stored data.
        let new = resolve_yaml(
            "name: docs_v2\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT, sensitive: true, max_bytes: 4096 } ] }\n",
        );
        let plan = old.alter_to(&new);
        assert!(!plan.requires_reindex(), "{:?}", plan.reindex_reasons);
        assert!(plan.in_place.iter().any(|r| r.contains("renamed")));
        assert!(plan.in_place.iter().any(|r| r.contains("sensitive")));
        assert!(plan.in_place.iter().any(|r| r.contains("max_bytes")));
    }

    #[test]
    fn altering_the_key_requires_reindex() {
        let old = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT } ] }\n",
        );
        // docs_schema identifiers default to [id]; force a different identifier set.
        let new = resolve_yaml(
            "name: docs\nsource: { iceberg: { catalog: growlerdb, table: growlerdb.docs } }\nkey: { identifier_fields: [body] }\nmapping: { selection: EXPLICIT, fields: [ { path: body, type: TEXT } ] }\n",
        );
        let plan = old.alter_to(&new);
        assert!(plan.requires_reindex());
        assert!(plan
            .reindex_reasons
            .iter()
            .any(|r| r.contains("identifier_fields")));
    }

    #[test]
    fn extra_design04_keys_are_ignored_not_rejected() {
        // A richer field entry (fast/cached/sub_fields) still parses.
        let yaml = r#"
name: docs
source: { iceberg: { catalog: growlerdb, table: growlerdb.docs, scan: APPEND_FAST_PATH } }
mapping:
  selection: ALL
  fields:
    - { path: id, type: KEYWORD, fast: true, cached: true, default_field: true }
"#;
        let def = IndexDefinition::from_yaml(yaml).expect("parse ignoring extra keys");
        assert!(def.resolve(&docs_schema()).is_ok());
    }

    #[test]
    fn routing_strategy_follows_partition_fields() {
        // A partitioned key → partition routing (co-locate a partition on a shard).
        let partitioned = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\n\
             key: { partition_fields: [day], identifier_fields: [id] }\n",
        )
        .unwrap()
        .resolve(&docs_schema())
        .unwrap();
        assert_eq!(partitioned.routing_strategy(), RoutingStrategy::Partition);
        let router = partitioned.shard_router(4);
        assert_eq!(router.shards(), 4);
        assert_eq!(router, ShardRouter::partitioned(4));

        // An unpartitioned key → hash routing (uniform spread).
        let no_partition = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("body", SourceType::String),
            ],
            vec![], // no partition hints
            vec!["id".into()],
        );
        let hashed = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\n\
             key: { identifier_fields: [id] }\n",
        )
        .unwrap()
        .resolve(&no_partition)
        .unwrap();
        assert_eq!(hashed.routing_strategy(), RoutingStrategy::Hash);
        assert_eq!(hashed.shard_router(2), ShardRouter::hashed(2));
    }

    // ---- VECTOR fields -----------------------------------------------------

    /// Resolve an EXPLICIT mapping with `body` + a `body_vec` VECTOR field over it.
    fn resolve_vector(vector_field_yaml: &str) -> Result<ResolvedIndex, DefError> {
        let yaml = format!(
            "name: docs\nsource: {{ iceberg: {{ catalog: g, table: g.docs }} }}\n\
             key: {{ identifier_fields: [id] }}\n\
             mapping:\n  selection: EXPLICIT\n  fields:\n\
             \x20   - {{ path: body, type: TEXT }}\n{vector_field_yaml}"
        );
        IndexDefinition::from_yaml(&yaml)
            .unwrap()
            .resolve(&docs_schema())
    }

    #[test]
    fn vector_field_resolves_with_explicit_config() {
        let r = resolve_vector(
            "    - { path: body_vec, type: VECTOR, \
             vector: { dims: 8, model: test-model, source_field: body } }\n",
        )
        .expect("resolve");
        let vf = r.fields.iter().find(|f| f.path == "body_vec").unwrap();
        assert_eq!(vf.ty, FieldType::Vector);
        assert!(!vf.indexed, "vectors get no inverted index");
        assert!(!vf.fast);
        let spec = vf.vector.as_ref().expect("vector spec");
        assert_eq!(spec.dims, 8);
        assert_eq!(spec.model, "test-model");
        assert_eq!(spec.source_field, "body");
        assert_eq!(spec.metric, VectorMetric::Cosine);
        assert_eq!(spec.provider, EmbedProvider::Local);
    }

    #[test]
    fn vector_field_applies_defaults() {
        let r = resolve_vector(
            "    - { path: body_vec, type: VECTOR, vector: { source_field: body } }\n",
        )
        .expect("resolve");
        let spec = r
            .fields
            .iter()
            .find(|f| f.path == "body_vec")
            .unwrap()
            .vector
            .as_ref()
            .unwrap();
        assert_eq!(spec.dims, DEFAULT_EMBED_DIMS);
        assert_eq!(spec.model, DEFAULT_EMBED_MODEL);
    }

    #[test]
    fn vector_field_without_config_is_rejected() {
        let err = resolve_vector("    - { path: body_vec, type: VECTOR }\n").unwrap_err();
        assert!(matches!(err, DefError::VectorMissingConfig(p) if p == "body_vec"));
    }

    #[test]
    fn vector_field_zero_dims_is_rejected() {
        let err = resolve_vector(
            "    - { path: body_vec, type: VECTOR, vector: { dims: 0, source_field: body } }\n",
        )
        .unwrap_err();
        assert!(matches!(err, DefError::VectorZeroDims(p) if p == "body_vec"));
    }

    #[test]
    fn vector_config_on_non_vector_field_is_rejected() {
        let err =
            resolve_vector("    - { path: count, vector: { source_field: body } }\n").unwrap_err();
        assert!(matches!(err, DefError::VectorConfigOnNonVector(p) if p == "count"));
    }

    #[test]
    fn vector_external_provider_is_rejected() {
        let err = resolve_vector(
            "    - { path: body_vec, type: VECTOR, \
             vector: { provider: EXTERNAL, source_field: body } }\n",
        )
        .unwrap_err();
        assert!(matches!(err, DefError::VectorExternalProvider(p) if p == "body_vec"));
    }

    #[test]
    fn vector_field_rejects_fast_and_cached() {
        let fast = resolve_vector(
            "    - { path: body_vec, type: VECTOR, fast: true, \
             vector: { source_field: body } }\n",
        )
        .unwrap_err();
        assert!(matches!(
            fast,
            DefError::VectorInvalidOption { option: "fast", .. }
        ));
        let cached = resolve_vector(
            "    - { path: body_vec, type: VECTOR, cached: true, \
             vector: { source_field: body } }\n",
        )
        .unwrap_err();
        assert!(matches!(
            cached,
            DefError::VectorInvalidOption {
                option: "cached",
                ..
            }
        ));
    }

    #[test]
    fn vector_unknown_source_field_is_rejected() {
        let err = resolve_vector(
            "    - { path: body_vec, type: VECTOR, vector: { source_field: nope } }\n",
        )
        .unwrap_err();
        assert!(
            matches!(err, DefError::VectorSourceUnknown { source_field, .. } if source_field == "nope")
        );
    }

    #[test]
    fn adding_a_vector_field_requires_reindex() {
        // The helper with no extra field resolves to just the `body` TEXT field.
        let without = resolve_vector("").unwrap();
        let with = resolve_vector(
            "    - { path: body_vec, type: VECTOR, vector: { source_field: body } }\n",
        )
        .unwrap();
        assert!(without.alter_to(&with).requires_reindex());

        // Changing the spec (dims) also forces a reindex (re-embed).
        let with_16 = resolve_vector(
            "    - { path: body_vec, type: VECTOR, vector: { dims: 16, source_field: body } }\n",
        )
        .unwrap();
        assert!(with.alter_to(&with_16).requires_reindex());
    }

    // ---- Variant fields (D47) ---------------------------------------------

    /// A GitHub-events-shaped source: scalar key/time/discriminator columns plus a variant
    /// `payload` column (opaque → `SourceType::Other`).
    fn events_schema() -> SourceSchema {
        SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("ts", SourceType::Long),
                SourceField::new("event_type", SourceType::String),
                SourceField::new("payload", SourceType::Other),
            ],
            vec![],
            vec!["id".into()],
        )
    }

    /// Resolve an `events` index whose EXPLICIT mapping is `mapping_fields` (the `fields:` YAML
    /// block body, already indented).
    fn resolve_events(mapping_fields: &str) -> Result<ResolvedIndex, DefError> {
        let yaml = format!(
            "name: events\nsource: {{ iceberg: {{ catalog: g, table: g.events }} }}\n\
             key: {{ identifier_fields: [id] }}\n\
             mapping:\n  selection: EXPLICIT\n  fields:\n{mapping_fields}"
        );
        IndexDefinition::from_yaml(&yaml)?.resolve(&events_schema())
    }

    /// Resolve an `events` index from a complete YAML `mapping:` block (correctly-indented raw
    /// string) — for the shape tests, whose nested `variant.shapes` are clearer written out.
    fn resolve_events_full(mapping_yaml: &str) -> Result<ResolvedIndex, DefError> {
        let yaml = format!(
            "name: events\nsource: {{ iceberg: {{ catalog: g, table: g.events }} }}\n\
             key: {{ identifier_fields: [id] }}\n{mapping_yaml}"
        );
        IndexDefinition::from_yaml(&yaml)?.resolve(&events_schema())
    }

    #[test]
    fn declares_variant_detects_the_unresolved_variant_mapping() {
        let with = IndexDefinition::from_yaml(
            "name: events\nsource: { iceberg: { catalog: g, table: g.events } }\nkey: { identifier_fields: [id] }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: payload, type: VARIANT } ] }\n",
        )
        .unwrap();
        assert!(with.declares_variant());
        let without = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD } ] }\n",
        )
        .unwrap();
        assert!(!without.declares_variant());
    }

    #[test]
    fn bare_variant_field_gets_default_flatten() {
        // `type: VARIANT` with no `variant:` config ⇒ flatten on (terms), no text, no shapes.
        let idx = resolve_events(
            "    - { path: id, type: KEYWORD }\n    - { path: payload, type: VARIANT }\n",
        )
        .expect("resolve");
        let v = idx.fields.iter().find(|f| f.path == "payload").unwrap();
        assert_eq!(v.ty, FieldType::Variant);
        let rv = v.variant.as_ref().unwrap();
        assert!(rv.flatten.enabled && rv.flatten.terms && !rv.flatten.text);
        assert!(rv.shapes.is_empty() && rv.discriminator.is_none());
        // No shaped leaves emitted, and the variant field is not a typed inverted/columnar field.
        assert_eq!(
            idx.fields
                .iter()
                .filter(|f| f.path.starts_with("payload."))
                .count(),
            0
        );
    }

    #[test]
    fn flatten_text_catch_all_and_disable_toggle() {
        // `flatten: { text: true }` turns on the catch-all with an analyzer.
        let idx = resolve_events(
            "    - { path: id, type: KEYWORD }\n    - { path: payload, type: VARIANT, variant: { flatten: { text: true, analyzer: en } } }\n",
        )
        .unwrap();
        let rv = idx
            .fields
            .iter()
            .find(|f| f.path == "payload")
            .unwrap()
            .variant
            .as_ref()
            .unwrap();
        assert!(rv.flatten.text);
        assert_eq!(rv.flatten.analyzer.as_deref(), Some("en"));

        // `flatten: false` with no shapes ⇒ nothing to index ⇒ error.
        let err = resolve_events(
            "    - { path: id, type: KEYWORD }\n    - { path: payload, type: VARIANT, variant: { flatten: false } }\n",
        )
        .unwrap_err();
        assert!(
            matches!(err, DefError::VariantEmpty(ref p) if p == "payload"),
            "got {err}"
        );
    }

    #[test]
    fn flatten_is_untyped_mixed_type_paths_are_no_declaration() {
        // Flatten declares no per-path types at all — resolution never inspects leaf types, so a
        // path that is a number in one row and a string in another simply cannot conflict. We
        // assert the *shape* of that guarantee: a flatten-only variant resolves with zero typed
        // fields under the column and no dynamic field growth.
        let idx = resolve_events(
            "    - { path: id, type: KEYWORD }\n    - { path: payload, type: VARIANT }\n",
        )
        .unwrap();
        // Exactly: id + payload (the variant field). No per-leaf typed fields.
        assert_eq!(idx.fields.len(), 2);
    }

    #[test]
    fn two_shapes_share_a_path_of_the_same_type_as_one_field() {
        // `payload.number` is LONG in both shapes ⇒ one resolved field; the shaped LONG is fast
        // (range/sort-capable). `payload.action` is TEXT in one shape only.
        let idx = resolve_events_full(
            r#"mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - path: payload
      type: VARIANT
      variant:
        discriminator: event_type
        shapes:
          - name: pr
            when: [PullRequestEvent]
            fields:
              - { path: number, type: LONG, fast: true }
              - { path: action, type: TEXT }
          - name: issue
            when: IssuesEvent
            fields:
              - { path: number, type: LONG, fast: true }
"#,
        )
        .expect("resolve");
        // One `payload.number` field (deduped across shapes), typed LONG + fast (sortable).
        let numbers: Vec<_> = idx
            .fields
            .iter()
            .filter(|f| f.path == "payload.number")
            .collect();
        assert_eq!(
            numbers.len(),
            1,
            "shared same-type path deduped to one field"
        );
        assert_eq!(numbers[0].ty, FieldType::Long);
        assert!(numbers[0].fast);
        assert!(idx
            .fields
            .iter()
            .any(|f| f.path == "payload.action" && f.ty == FieldType::Text));
        // Discriminator + shapes recorded on the variant field for the connector.
        let rv = idx
            .fields
            .iter()
            .find(|f| f.path == "payload")
            .unwrap()
            .variant
            .as_ref()
            .unwrap();
        assert_eq!(rv.discriminator.as_deref(), Some("event_type"));
        assert_eq!(rv.shapes.len(), 2);
        assert_eq!(rv.shapes[0].when, vec!["PullRequestEvent".to_string()]);
        assert_eq!(rv.shapes[1].when, vec!["IssuesEvent".to_string()]);
    }

    #[test]
    fn cross_shape_type_disagreement_is_a_create_time_error() {
        let err = resolve_events_full(
            r#"mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - path: payload
      type: VARIANT
      variant:
        discriminator: event_type
        shapes:
          - { name: a, when: A, fields: [ { path: n, type: LONG } ] }
          - { name: b, when: B, fields: [ { path: n, type: KEYWORD } ] }
"#,
        )
        .unwrap_err();
        assert!(
            matches!(&err, DefError::VariantShapeTypeConflict { path, .. } if path == "payload.n"),
            "got {err}"
        );
    }

    #[test]
    fn shapes_require_a_discriminator() {
        let err = resolve_events_full(
            r#"mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - path: payload
      type: VARIANT
      variant:
        shapes: [ { name: a, when: A, fields: [ { path: n, type: LONG } ] } ]
"#,
        )
        .unwrap_err();
        assert!(
            matches!(err, DefError::VariantShapesNoDiscriminator(ref p) if p == "payload"),
            "got {err}"
        );
    }

    #[test]
    fn shaped_vector_path_source_field_is_column_qualified() {
        // A VECTOR leaf inside a shape whose `source_field` is a sibling shaped TEXT path: both get
        // the `payload.` prefix, and the cross-field source_field check (over the full field set)
        // passes.
        let idx = resolve_events_full(
            r#"mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - path: payload
      type: VARIANT
      variant:
        discriminator: event_type
        shapes:
          - name: pr
            when: PullRequestEvent
            fields:
              - { path: body, type: TEXT }
              - { path: body_vec, type: VECTOR, vector: { source_field: body } }
"#,
        )
        .expect("resolve");
        let vec_field = idx
            .fields
            .iter()
            .find(|f| f.path == "payload.body_vec")
            .unwrap();
        assert_eq!(vec_field.ty, FieldType::Vector);
        assert_eq!(
            vec_field.vector.as_ref().unwrap().source_field,
            "payload.body"
        );
    }

    #[test]
    fn reserved_hash_in_variant_path_is_rejected() {
        let err = resolve_events_full(
            r#"mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - path: payload
      type: VARIANT
      variant:
        discriminator: event_type
        shapes: [ { name: a, when: A, fields: [ { path: "n#x", type: LONG } ] } ]
"#,
        )
        .unwrap_err();
        assert!(matches!(err, DefError::VariantReservedName(_)), "got {err}");
    }

    #[test]
    fn variant_config_on_non_variant_field_is_rejected() {
        let err = resolve_events(
            "    - { path: id, type: KEYWORD }\n    - { path: payload, type: KEYWORD, variant: { flatten: true } }\n",
        )
        .unwrap_err();
        assert!(
            matches!(err, DefError::VariantConfigOnNonVariant(ref p) if p == "payload"),
            "got {err}"
        );
    }

    #[test]
    fn variant_field_and_shapes_round_trip_through_yaml() {
        // The full resolved definition serializes and re-parses unchanged (persisted form).
        let idx = resolve_events_full(
            r#"mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - path: payload
      type: VARIANT
      variant:
        flatten: { text: true }
        discriminator: event_type
        shapes:
          - name: pr
            when: [PullRequestEvent]
            fields: [ { path: number, type: LONG, fast: true } ]
"#,
        )
        .unwrap();
        let json = serde_json::to_string(&idx).unwrap();
        let back: ResolvedIndex = serde_json::from_str(&json).unwrap();
        assert_eq!(idx, back);
    }

    #[test]
    fn flatten_toggle_wire_forms_parse() {
        // `flatten: true` / `false` / a map all parse to the right FlattenConfig.
        let parse = |v: &str| -> FlattenConfig {
            serde_norway::from_str::<VariantMappingOpts>(&format!("flatten: {v}"))
                .unwrap()
                .flatten
        };
        assert_eq!(parse("true"), FlattenConfig::default());
        assert!(!parse("false").enabled);
        let m = parse("{ terms: false, text: true }");
        assert!(m.enabled && !m.terms && m.text);
        // Absent ⇒ default (enabled).
        assert_eq!(
            serde_norway::from_str::<VariantMappingOpts>("discriminator: t")
                .unwrap()
                .flatten,
            FlattenConfig::default()
        );
    }
}
