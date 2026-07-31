//! **Interim Trino-backed hydration for variant tables** ([D48]). Released iceberg-rust can't parse
//! a v3 schema with a `variant` column (it errors in `Catalog::load_table`), which breaks every Rust
//! path over such a table. So a per-index fork
//! ([`ResolvedIndex::has_variant_field`](growlerdb_core::ResolvedIndex::has_variant_field)) routes
//! variant-table hydration through this module — key-predicated point `SELECT`s against Trino, the
//! variant column returned as JSON — while a non-variant index keeps the native [`IcebergReader`] path.
//!
//! [`TrinoHydrator::hydrate`] returns the same [`HydrationResult`] as the native
//! [`IcebergReader::hydrate`](crate::IcebergReader::hydrate), so the caller forks on
//! `has_variant_field` and is otherwise oblivious to the lane. Trino unreachable / a query error
//! **degrades loudly** ([D45]): [`hydrate`](TrinoHydrator::hydrate) returns an `Err`, never a silent
//! partial result.
//!
//! [D48]: ../../../okf/system/decisions/d48-variant-delivery.md
//! [D45]: ../../../okf/system/decisions/d45-degraded-vs-error.md

use std::collections::BTreeMap;

use growlerdb_core::{
    CompositeKey, FieldType, HydratedRow, Projection, ResolvedIndex, RowLocator, Source,
    SourceField, SourceSchema, SourceType, Value,
};
use serde::Deserialize;

use crate::{HydrationResult, Result, SourceError};

/// Connection settings for the interim Trino hydration lane. Defaults match the dev stack
/// (`deploy/compose`: `trinodb/trino`, Iceberg catalog `iceberg` over the same Polaris + MinIO).
#[derive(Debug, Clone)]
pub struct TrinoConfig {
    /// Coordinator base URL, e.g. `http://trino:8080` (host `http://localhost:8082` on the dev
    /// stack, where Trino's `8080` is published on `8082`).
    pub endpoint: String,
    /// The Trino **catalog** the Iceberg tables live under (`iceberg` in the dev stack's
    /// `trino/iceberg.properties`).
    pub catalog: String,
    /// The `X-Trino-User` to run queries as (audit only; no auth in the dev stack).
    pub user: String,
}

impl TrinoConfig {
    /// Dev-stack defaults (host-side): Trino published on `:8082`, catalog `iceberg`.
    pub fn local() -> Self {
        Self {
            endpoint: "http://localhost:8082".to_string(),
            catalog: "iceberg".to_string(),
            user: "growlerdb".to_string(),
        }
    }

    /// As [`local`](Self::local), overridable from the environment so the same binary runs on a dev
    /// host and in-cluster: `GROWLERDB_TRINO_ENDPOINT` (e.g. `http://trino:8080`),
    /// `GROWLERDB_TRINO_CATALOG`, `GROWLERDB_TRINO_USER`.
    pub fn from_env() -> Self {
        let base = Self::local();
        let var = |k: &str, d: String| std::env::var(k).ok().filter(|v| !v.is_empty()).unwrap_or(d);
        Self {
            endpoint: var("GROWLERDB_TRINO_ENDPOINT", base.endpoint),
            catalog: var("GROWLERDB_TRINO_CATALOG", base.catalog),
            user: var("GROWLERDB_TRINO_USER", base.user),
        }
    }
}

/// The process-wide interim Trino hydrator, built from the environment ([`TrinoConfig::from_env`]).
/// Lazily initialized on first variant-table hydration and reused (the reqwest client pools
/// connections), so the engine's per-index fork can reach it without threading a [`TrinoConfig`]
/// through every service constructor. Non-variant indexes never call this, so a stack with no
/// variant index never builds it.
pub fn shared_hydrator() -> &'static TrinoHydrator {
    static SHARED: std::sync::OnceLock<TrinoHydrator> = std::sync::OnceLock::new();
    SHARED.get_or_init(|| {
        // reqwest client construction only fails on a TLS-backend init error, which would fail
        // every request anyway — surface it loudly at first use rather than mask it.
        TrinoHydrator::new(TrinoConfig::from_env()).expect("build the Trino HTTP client")
    })
}

/// The interim variant-table hydrator (D48). Holds a Trino [`TrinoConfig`] + a reusable HTTP
/// client; `hydrate` runs one key-predicated point query per call.
pub struct TrinoHydrator {
    cfg: TrinoConfig,
    http: reqwest::Client,
}

impl TrinoHydrator {
    /// Build a hydrator for `cfg`.
    pub fn new(cfg: TrinoConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .build()
            .map_err(|e| SourceError::Trino(format!("building HTTP client: {e}")))?;
        Ok(Self { cfg, http })
    }

    /// **Interim hydration** for a variant-table index (D48): resolve `requests` to authoritative
    /// rows via a Trino key-predicated point `SELECT`, the variant column(s) returned as JSON. The
    /// `RowLocator`s are ignored — Trino re-finds each row by its composite key (there is no
    /// per-row locator lane through SQL); every returned row is still key-verified, so a phantom is
    /// never returned. Rows come back in `requests` order, genuinely-absent keys omitted — matching
    /// the native [`hydrate`](crate::IcebergReader::hydrate) contract. Trino unreachable / a query
    /// error is a loud `Err` (D45), never a silent miss.
    pub async fn hydrate(
        &self,
        index: &ResolvedIndex,
        requests: &[(CompositeKey, Option<RowLocator>)],
        projection: &Projection,
    ) -> Result<HydrationResult> {
        if requests.is_empty() {
            return Ok(HydrationResult::default());
        }
        let table = iceberg_table(index)?;
        let keys: Vec<&CompositeKey> = requests.iter().map(|(k, _)| k).collect();
        let cols = SelectColumns::for_index(index);
        let sql = point_query_sql(&self.cfg.catalog, table, &cols, &keys);
        let result = self.run_query(table, &sql).await?;
        let rows = assemble_rows(&result, requests, index, &cols, projection)?;
        Ok(HydrationResult {
            rows,
            // Trino re-finds by predicate every call: there is no per-row locator to refresh, and
            // no snapshot-pinned plan cache in this lane.
            refreshed: Vec::new(),
            plan_cache_hit: None,
            duplicate_pks: 0,
        })
    }

    /// **Route-around-iceberg-rust schema introspection** for a variant table (D48): read the
    /// table's columns from Trino `information_schema` and map them to a [`SourceSchema`] (the
    /// variant column → [`SourceType::Other`]). This replaces `read_source_schema` (which dies in
    /// `load_table`) at index-create for a variant table. Key hints (partition / identifier fields)
    /// are not exposed by `information_schema`, so they come from the definition's explicit `key:`.
    pub async fn read_source_schema(&self, table: &str) -> Result<SourceSchema> {
        let (schema_name, table_name) = split_table(table)?;
        let sql = format!(
            "SELECT column_name, data_type FROM {cat}.information_schema.columns \
             WHERE table_schema = {s} AND table_name = {t} ORDER BY ordinal_position",
            cat = ident(&self.cfg.catalog),
            s = string_literal(schema_name),
            t = string_literal(table_name),
        );
        let result = self.run_query(table, &sql).await?;
        let fields = result
            .rows
            .iter()
            .filter_map(|row| {
                let name = row.first()?.as_str()?;
                let ty = row.get(1)?.as_str()?;
                Some(SourceField::new(name.to_string(), trino_type_to_source(ty)))
            })
            .collect();
        Ok(SourceSchema::new(fields, Vec::new(), Vec::new()))
    }

    /// Run `sql` against Trino, following the REST `nextUri` chain until the result set is fully
    /// drained, accumulating `columns` + `data`. A Trino `error` payload — or any transport
    /// failure — is a loud `Err` (D45). `table` is only used to enrich error context.
    async fn run_query(&self, table: &str, sql: &str) -> Result<QueryResult> {
        let fail = |ctx: &str, e: String| {
            SourceError::Trino(format!(
                "{ctx} (table `{table}`, endpoint `{}`): {e}",
                self.cfg.endpoint
            ))
        };
        let mut resp: TrinoResponse = self
            .http
            .post(format!("{}/v1/statement", self.cfg.endpoint))
            .header("X-Trino-User", &self.cfg.user)
            .header("X-Trino-Catalog", &self.cfg.catalog)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| fail("posting statement", e.to_string()))?
            .json()
            .await
            .map_err(|e| fail("decoding statement response", e.to_string()))?;

        let mut out = QueryResult::default();
        loop {
            if let Some(err) = resp.error {
                return Err(fail("query failed", err.message));
            }
            if out.columns.is_empty() {
                if let Some(cols) = resp.columns {
                    out.columns = cols.into_iter().map(|c| c.name).collect();
                }
            }
            if let Some(data) = resp.data {
                out.rows.extend(data);
            }
            let Some(next) = resp.next_uri else { break };
            resp = self
                .http
                .get(&next)
                .header("X-Trino-User", &self.cfg.user)
                .send()
                .await
                .map_err(|e| fail("following nextUri", e.to_string()))?
                .json()
                .await
                .map_err(|e| fail("decoding nextUri response", e.to_string()))?;
        }
        Ok(out)
    }
}

/// The `namespace.table` identifier of a resolved index's Iceberg source.
fn iceberg_table(index: &ResolvedIndex) -> Result<&str> {
    match &index.source {
        Source::Iceberg(s) => Ok(&s.table),
    }
}

/// Split a `namespace.table` identifier into `(schema, table)` for Trino (`catalog.schema.table`).
fn split_table(table: &str) -> Result<(&str, &str)> {
    table
        .rsplit_once('.')
        .ok_or_else(|| SourceError::Trino(format!("table `{table}` is not `namespace.table`")))
}

/// The columns a point query selects for an index, split into the real Trino columns to read and
/// the subset that are variant columns (rendered `CAST(col AS JSON)`).
struct SelectColumns {
    /// Scalar (non-variant, non-shaped) columns — key columns + top-level scalar fields — read
    /// verbatim. Deduped, in a stable order.
    scalars: Vec<String>,
    /// Variant columns — read as `CAST(col AS JSON) AS col`.
    variants: Vec<String>,
}

impl SelectColumns {
    /// Derive the real Trino columns from a resolved index: the variant columns, plus every key
    /// column and top-level scalar field that is **not** a shaped sub-path of a variant column
    /// (those are virtual — they live inside the variant, not as table columns).
    fn for_index(index: &ResolvedIndex) -> Self {
        let variants: Vec<String> = index
            .variant_columns()
            .iter()
            .map(|s| s.to_string())
            .collect();
        let under_variant = |p: &str| {
            variants
                .iter()
                .any(|c| p.starts_with(&format!("{c}.")) || p == c)
        };
        let mut scalars: Vec<String> = Vec::new();
        let mut push = |p: &str| {
            if !under_variant(p) && !scalars.iter().any(|s| s == p) {
                scalars.push(p.to_string());
            }
        };
        for c in index
            .key
            .partition_fields
            .iter()
            .chain(index.key.identifier_fields.iter())
        {
            push(c);
        }
        for f in &index.fields {
            if f.ty != FieldType::Variant {
                push(&f.path);
            }
        }
        Self { scalars, variants }
    }
}

/// Build the key-predicated point-`SELECT`: the real columns (variant columns `CAST … AS JSON`),
/// `WHERE` an `OR` of per-key `AND` equalities over the key columns. `keys` is non-empty.
fn point_query_sql(
    catalog: &str,
    table: &str,
    cols: &SelectColumns,
    keys: &[&CompositeKey],
) -> String {
    let (schema, name) = table.rsplit_once('.').unwrap_or(("", table));
    let mut select: Vec<String> = cols.scalars.iter().map(|c| ident(c)).collect();
    for v in &cols.variants {
        // Render the variant as JSON text so the hydrated row carries the whole object (D48).
        select.push(format!("CAST({col} AS JSON) AS {col}", col = ident(v)));
    }
    let select = if select.is_empty() {
        "*".to_string()
    } else {
        select.join(", ")
    };
    let predicate = key_predicate_sql(keys);
    format!(
        "SELECT {select} FROM {cat}.{schema}.{name} WHERE {predicate}",
        cat = ident(catalog),
        schema = ident(schema),
        name = ident(name),
    )
}

/// An `OR` of per-key `AND` equalities over each key's fields — the pruning + match predicate.
fn key_predicate_sql(keys: &[&CompositeKey]) -> String {
    keys.iter()
        .map(|k| {
            let conj = k
                .partition
                .iter()
                .chain(k.identifier.iter())
                .map(|(n, v)| format!("{} = {}", ident(n), sql_literal(v)))
                .collect::<Vec<_>>()
                .join(" AND ");
            format!("({conj})")
        })
        .collect::<Vec<_>>()
        .join(" OR ")
}

/// Render a scalar [`Value`] as a Trino SQL literal. `Ts` is canonical epoch micros → a
/// `from_unixtime_nanos` timestamp; a `Vector` can never be a key value (rendered as `NULL`, which
/// simply matches nothing — a vector key is rejected far upstream).
fn sql_literal(v: &Value) -> String {
    match v {
        Value::Str(s) => string_literal(s),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Ts(micros) => format!("from_unixtime_nanos({})", micros.saturating_mul(1_000)),
        Value::Vector(_) => "NULL".to_string(),
    }
}

/// A single-quoted SQL string literal with embedded quotes doubled.
fn string_literal(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// A double-quoted SQL identifier with embedded quotes doubled (safe for column/table names).
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Map a Trino column `data_type` string to GrowlerDB's coarse [`SourceType`]. A `variant` (or an
/// unrecognized/complex type: row/array/map/json) maps to [`SourceType::Other`], matching how the
/// native Arrow path treats a non-scalar leaf.
fn trino_type_to_source(ty: &str) -> SourceType {
    let base = ty
        .split('(')
        .next()
        .unwrap_or(ty)
        .trim()
        .to_ascii_lowercase();
    match base.as_str() {
        "varchar" | "char" | "varbinary" => SourceType::String,
        "boolean" => SourceType::Bool,
        "tinyint" | "smallint" | "integer" | "int" | "bigint" => SourceType::Long,
        "real" | "double" | "decimal" => SourceType::Double,
        "date" | "timestamp" | "timestamp with time zone" => SourceType::Date,
        _ => SourceType::Other, // variant, row, array, map, json, …
    }
}

/// Assemble Trino result rows into [`HydratedRow`]s in `requests` order (absent keys omitted). Each
/// row's composite key is rebuilt from the key columns and **verified** against the request; a
/// variant column's JSON value is carried as a `Value::Str` of its compact JSON text.
fn assemble_rows(
    result: &QueryResult,
    requests: &[(CompositeKey, Option<RowLocator>)],
    index: &ResolvedIndex,
    cols: &SelectColumns,
    projection: &Projection,
) -> Result<Vec<HydratedRow>> {
    // Column name → position in the result.
    let pos: BTreeMap<&str, usize> = result
        .columns
        .iter()
        .enumerate()
        .map(|(i, c)| (c.as_str(), i))
        .collect();
    let key_cols: Vec<&str> = index
        .key
        .partition_fields
        .iter()
        .chain(index.key.identifier_fields.iter())
        .map(String::as_str)
        .collect();

    // Index the returned rows by their encoded composite key for O(1) request lookup.
    let mut by_key: BTreeMap<Vec<u8>, BTreeMap<String, Value>> = BTreeMap::new();
    for row in &result.rows {
        let full = row_to_fields(row, &pos, cols, index);
        // Rebuild the row's key from its key columns; skip a row missing any key field.
        let mut partition = Vec::new();
        let mut identifier = Vec::new();
        let mut complete = true;
        for c in &key_cols {
            match full.get(*c) {
                Some(v) if index.key.partition_fields.iter().any(|p| p == c) => {
                    partition.push((c.to_string(), v.clone()))
                }
                Some(v) => identifier.push((c.to_string(), v.clone())),
                None => {
                    complete = false;
                    break;
                }
            }
        }
        if complete {
            by_key.insert(CompositeKey::new(partition, identifier).encode(), full);
        }
    }

    Ok(requests
        .iter()
        .filter_map(|(key, _)| {
            let full = by_key.get(&key.encode())?;
            Some(HydratedRow {
                key: key.clone(),
                fields: project(full, projection),
            })
        })
        .collect())
}

/// Map one Trino result row (positional `serde_json` values) to a field map, typing scalars by the
/// index's declared field types and carrying each variant column as its JSON text.
fn row_to_fields(
    row: &[serde_json::Value],
    pos: &BTreeMap<&str, usize>,
    cols: &SelectColumns,
    index: &ResolvedIndex,
) -> BTreeMap<String, Value> {
    let mut fields = BTreeMap::new();
    for c in &cols.scalars {
        let Some(cell) = pos.get(c.as_str()).and_then(|i| row.get(*i)) else {
            continue;
        };
        if cell.is_null() {
            continue;
        }
        let ty = index.fields.iter().find(|f| &f.path == c).map(|f| f.ty);
        if let Some(v) = json_to_value(cell, ty) {
            fields.insert(c.clone(), v);
        }
    }
    for v in &cols.variants {
        if let Some(cell) = pos.get(v.as_str()).and_then(|i| row.get(*i)) {
            if !cell.is_null() {
                // Trino returns `CAST(col AS JSON)` as a JSON **string** in the REST `data` array, so
                // take the cell verbatim — `to_string()` on a JSON string double-encodes. A
                // non-string cell (some drivers hand back the parsed value) is compact-serialized.
                let json = match cell {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                fields.insert(v.clone(), Value::Str(json));
            }
        }
    }
    fields
}

/// Convert a Trino `serde_json` cell to a scalar [`Value`], guided by the field's declared type
/// (`ty`) where known. Trino renders scalars as JSON strings/numbers/bools; a temporal column
/// comes back as an ISO string, carried as a `Str` (hydration returns display values — the range/
/// sort semantics live in the index, not the hydrated payload).
fn json_to_value(cell: &serde_json::Value, ty: Option<FieldType>) -> Option<Value> {
    match cell {
        serde_json::Value::String(s) => Some(match ty {
            Some(FieldType::Long) => s
                .parse::<i64>()
                .map(Value::Int)
                .unwrap_or(Value::Str(s.clone())),
            Some(FieldType::Double) => s
                .parse::<f64>()
                .map(Value::Float)
                .unwrap_or(Value::Str(s.clone())),
            _ => Value::Str(s.clone()),
        }),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(Value::Int(i))
            } else {
                n.as_f64().map(Value::Float)
            }
        }
        serde_json::Value::Bool(b) => Some(Value::Bool(*b)),
        _ => None,
    }
}

/// Narrow a full field map to `projection`.
fn project(full: &BTreeMap<String, Value>, projection: &Projection) -> BTreeMap<String, Value> {
    match projection {
        Projection::All => full.clone(),
        Projection::Columns(_) => full
            .iter()
            .filter(|(n, _)| projection.includes(n))
            .map(|(n, v)| (n.clone(), v.clone()))
            .collect(),
    }
}

/// The accumulated result of a Trino query: column names + positional row cells.
#[derive(Default)]
struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Vec<serde_json::Value>>,
}

/// One page of the Trino REST protocol (`POST /v1/statement` then the `nextUri` chain).
#[derive(Deserialize)]
struct TrinoResponse {
    #[serde(default)]
    columns: Option<Vec<TrinoColumn>>,
    #[serde(default)]
    data: Option<Vec<Vec<serde_json::Value>>>,
    #[serde(default, rename = "nextUri")]
    next_uri: Option<String>,
    #[serde(default)]
    error: Option<TrinoError>,
}

#[derive(Deserialize)]
struct TrinoColumn {
    name: String,
}

#[derive(Deserialize)]
struct TrinoError {
    message: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::IndexDefinition;

    fn events_index() -> ResolvedIndex {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("ts", SourceType::Long),
                SourceField::new("event_type", SourceType::String),
                SourceField::new("payload", SourceType::Other),
            ],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(
            r#"
name: events
source: { iceberg: { catalog: growlerdb, table: growlerdb.events } }
key: { identifier_fields: [id] }
mapping:
  selection: EXPLICIT
  fields:
    - { path: id, type: KEYWORD }
    - { path: ts, type: LONG, fast: true }
    - { path: event_type, type: KEYWORD }
    - path: payload
      type: VARIANT
      variant:
        discriminator: event_type
        shapes:
          - name: pr
            when: [PullRequestEvent]
            fields: [ { path: number, type: LONG, fast: true } ]
"#,
        )
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    fn key(id: &str) -> CompositeKey {
        CompositeKey::new(vec![], vec![("id".into(), Value::from(id))])
    }

    #[test]
    fn select_columns_exclude_shaped_paths_and_keep_the_variant() {
        let idx = events_index();
        let cols = SelectColumns::for_index(&idx);
        // Real scalar columns: key `id` + top-level scalars; NOT the shaped `payload.number`.
        assert_eq!(cols.scalars, vec!["id", "ts", "event_type"]);
        assert_eq!(cols.variants, vec!["payload"]);
        assert!(!cols.scalars.iter().any(|c| c == "payload.number"));
    }

    #[test]
    fn point_query_sql_renders_variant_as_json_and_ors_keys() {
        let idx = events_index();
        let cols = SelectColumns::for_index(&idx);
        let k1 = key("evt-1");
        let k2 = key("evt-2");
        let sql = point_query_sql("iceberg", "growlerdb.events", &cols, &[&k1, &k2]);
        assert!(
            sql.contains("CAST(\"payload\" AS JSON) AS \"payload\""),
            "{sql}"
        );
        assert!(
            sql.contains("FROM \"iceberg\".\"growlerdb\".\"events\""),
            "{sql}"
        );
        assert!(
            sql.contains("(\"id\" = 'evt-1') OR (\"id\" = 'evt-2')"),
            "{sql}"
        );
    }

    #[test]
    fn sql_literal_escapes_and_types() {
        assert_eq!(sql_literal(&Value::from("a'b")), "'a''b'");
        assert_eq!(sql_literal(&Value::Int(7)), "7");
        assert_eq!(sql_literal(&Value::Bool(true)), "true");
        assert!(sql_literal(&Value::Ts(1_000_000)).starts_with("from_unixtime_nanos("));
    }

    #[test]
    fn trino_types_map_to_source_types() {
        assert_eq!(trino_type_to_source("varchar(255)"), SourceType::String);
        assert_eq!(trino_type_to_source("bigint"), SourceType::Long);
        assert_eq!(trino_type_to_source("double"), SourceType::Double);
        assert_eq!(trino_type_to_source("boolean"), SourceType::Bool);
        assert_eq!(trino_type_to_source("timestamp(6)"), SourceType::Date);
        // A variant (or any complex/unknown type) is opaque → Other.
        assert_eq!(trino_type_to_source("variant"), SourceType::Other);
        assert_eq!(trino_type_to_source("row(a integer)"), SourceType::Other);
    }

    #[test]
    fn assemble_rows_verifies_keys_carries_variant_json_and_preserves_request_order() {
        let idx = events_index();
        let cols = SelectColumns::for_index(&idx);
        // A synthetic Trino result: two rows, columns in SELECT order (scalars then variant).
        let result = QueryResult {
            columns: vec![
                "id".into(),
                "ts".into(),
                "event_type".into(),
                "payload".into(),
            ],
            rows: vec![
                vec![
                    serde_json::json!("evt-2"),
                    serde_json::json!(20),
                    serde_json::json!("IssuesEvent"),
                    serde_json::json!({ "number": 99 }),
                ],
                vec![
                    serde_json::json!("evt-1"),
                    serde_json::json!(10),
                    serde_json::json!("PullRequestEvent"),
                    serde_json::json!({ "user": { "login": "octocat" }, "number": 1347 }),
                ],
            ],
        };
        // Requested order is evt-1, evt-2, evt-missing → rows come back in that order, absent omitted.
        let requests = vec![
            (key("evt-1"), None),
            (key("evt-2"), None),
            (key("evt-missing"), None),
        ];
        let rows = assemble_rows(&result, &requests, &idx, &cols, &Projection::All).unwrap();
        assert_eq!(rows.len(), 2, "the missing key is omitted");
        assert_eq!(rows[0].key, key("evt-1"), "request order preserved");
        assert_eq!(rows[1].key, key("evt-2"));
        // Scalars typed by the schema; variant carried as JSON text.
        assert_eq!(rows[0].fields.get("ts"), Some(&Value::Int(10)));
        let payload = rows[0].fields.get("payload").unwrap().to_index_string();
        assert!(
            payload.contains("octocat") && payload.contains("1347"),
            "{payload}"
        );
    }

    #[test]
    fn projection_narrows_hydrated_fields() {
        let idx = events_index();
        let cols = SelectColumns::for_index(&idx);
        let result = QueryResult {
            columns: vec![
                "id".into(),
                "ts".into(),
                "event_type".into(),
                "payload".into(),
            ],
            rows: vec![vec![
                serde_json::json!("evt-1"),
                serde_json::json!(10),
                serde_json::json!("PullRequestEvent"),
                serde_json::json!({ "number": 1 }),
            ]],
        };
        let requests = vec![(key("evt-1"), None)];
        let rows = assemble_rows(
            &result,
            &requests,
            &idx,
            &cols,
            &Projection::Columns(vec!["payload".into()]),
        )
        .unwrap();
        assert_eq!(rows[0].fields.keys().collect::<Vec<_>>(), vec!["payload"]);
    }

    /// Live hydration against the seeded variant table (D48). Prereqs: `just variant` (creates
    /// `growlerdb.events` + brings up Trino on host `:8082`). Run:
    ///   `cargo test -p growlerdb-source -- --ignored trino_hydrates_the_live_events_table`
    #[tokio::test]
    #[ignore = "requires the local dev stack with the variant table (just variant)"]
    async fn trino_hydrates_the_live_events_table() {
        let hydrator = TrinoHydrator::new(TrinoConfig::local()).expect("hydrator");
        let idx = events_index();
        let requests = vec![(key("evt-1"), None), (key("evt-3"), None)];
        let result = hydrator
            .hydrate(&idx, &requests, &Projection::All)
            .await
            .expect("live Trino hydrate");
        assert_eq!(result.rows.len(), 2, "both keys hydrate, in request order");
        assert_eq!(result.rows[0].key, key("evt-1"));
        // Scalars typed by the schema.
        assert_eq!(
            result.rows[0].fields.get("event_type"),
            Some(&Value::from("PullRequestEvent"))
        );
        // The variant column comes back as JSON text carrying the nested object.
        let payload = result.rows[0]
            .fields
            .get("payload")
            .unwrap()
            .to_index_string();
        assert!(
            payload.contains("octocat") && payload.contains("1347"),
            "variant JSON: {payload}"
        );
    }
}
