//! Optional **OpenSearch-compatible `_search` adapter**. Translates a *documented
//! subset* of the OpenSearch Query DSL into GrowlerDB's native query string (which parses to the
//! canonical [`Query`](growlerdb_core::Query) AST), runs it through the [`Gateway`], and shapes the
//! results as OpenSearch documents: `_id` synthesized from the **composite key**, `_source` filled
//! by the Gateway's **inline hydration** (`SearchRequest.hydrate` — the governed PK-lookup path).
//! Read-path first — the native PK API stays primary; this is a thin migration/ecosystem
//! convenience, mounted only when the gateway is started with `--opensearch`.
//!
//! The supported subset and its caveats are documented in `docs/opensearch-adapter.md`; anything
//! outside it returns a clear error (`501` unsupported / `400` malformed) rather than silently
//! mis-translating.

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use serde::Deserialize;
use serde_json::{json, Map, Value as JsonValue};

use growlerdb_proto::v1::{
    self, value::Kind, Coordinates, HighlightField, HighlightRequest, SearchRequest,
    Sort as WireSort,
};

use crate::gateway::Gateway;
use crate::rest::grpc_request;

/// Build the OpenSearch-compatible router over the [`Gateway`]. Mount it alongside the `/v1`
/// router when `--opensearch` is set.
pub fn opensearch_router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        .route("/{index}/_search", post(search_handler))
        .route("/_search", post(search_all_handler))
        .with_state(gateway)
}

// ---- translation (pure; unit-tested incl. `Query::parse` of the output) ----------------------

/// Why a DSL request couldn't be served — surfaced as an OpenSearch-style error.
#[derive(Debug, PartialEq)]
pub struct AdapterError {
    pub kind: &'static str, // "unsupported" | "bad_request"
    pub reason: String,
}

impl AdapterError {
    fn unsupported(reason: impl Into<String>) -> Self {
        Self {
            kind: "unsupported",
            reason: reason.into(),
        }
    }
    fn bad(reason: impl Into<String>) -> Self {
        Self {
            kind: "bad_request",
            reason: reason.into(),
        }
    }
}

/// Query-string characters that are *structural* or *operators* and so can't sit bare in a value
/// without changing the parse (grouping, ranges, phrases, wildcards, fuzzy, boost, field-retarget
/// via `:`, the `&&`/`||`/`!` operators, or whitespace splitting the token). Everything else —
/// including `- . _ + @ /` — is fine mid-value (ids, dates, UUIDs, decimals), per the parser.
const SPECIAL: &[char] = &[
    '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\\', '&', '|', '!', ' ', '\t',
    '\n',
];

/// A value that can sit bare after `field:` (a single, unescaped token). Anything with whitespace
/// or a query-syntax metacharacter is rejected with a clear error rather than mis-encoded — the
/// adapter is a documented subset, not a best-effort guesser.
fn token(field: &str, value: &str) -> Result<String, AdapterError> {
    if value.is_empty() {
        return Err(AdapterError::bad(format!("empty value for `{field}`")));
    }
    if value.contains(SPECIAL) {
        return Err(AdapterError::unsupported(format!(
            "value `{value}` for `{field}` contains whitespace or query metacharacters; the adapter \
             supports simple token values (ids, numbers, dates, enums) — use the native /v1/search \
             for arbitrary text"
        )));
    }
    Ok(value.to_string())
}

/// Render a JSON scalar (string/number/bool) as a query token. Objects/arrays/null are rejected.
fn scalar_token(field: &str, v: &JsonValue) -> Result<String, AdapterError> {
    let s = match v {
        JsonValue::String(s) => s.clone(),
        JsonValue::Number(n) => n.to_string(),
        JsonValue::Bool(b) => b.to_string(),
        _ => {
            return Err(AdapterError::bad(format!(
                "`{field}` value must be a string/number/bool"
            )))
        }
    };
    token(field, &s)
}

/// Each temporal field's declared unit, by path — from the served index definition, so a
/// range/exact bound written in that unit (OpenSearch semantics) is converted to canonical micros
/// **here, once**, before planning. Both window pruning and segment execution are micros-native, so
/// converting at this boundary keeps them consistent. Empty for an index with no temporal fields.
pub type FieldFormats = std::collections::HashMap<String, growlerdb_core::TimeFormat>;

/// A range **bound** token. On a temporal field a numeric bound is in the field's declared unit
/// (e.g. `epoch_s` seconds) — convert it to canonical micros, exactly as ingestion normalized the
/// stored value, so the range and the data share a scale. A string bound (ISO-8601 / `YYYY-MM-DD`) is
/// absolute and passes through for the query parser to resolve; a non-temporal field is unchanged.
fn bound_token(field: &str, v: &JsonValue, formats: &FieldFormats) -> Result<String, AdapterError> {
    if let Some(fmt) = formats.get(field) {
        if let Some(n) = v.as_i64() {
            let micros = fmt
                .to_micros(field, &growlerdb_core::Value::Int(n))
                .map_err(|e| AdapterError::bad(format!("`range` on `{field}`: {e}")))?;
            return Ok(micros.to_string());
        }
    }
    scalar_token(field, v)
}

/// The single `{ field: spec }` of a leaf clause (e.g. `term`, `match`). Errors if not exactly one.
fn one_field(obj: &JsonValue, clause: &str) -> Result<(String, JsonValue), AdapterError> {
    let map = obj
        .as_object()
        .ok_or_else(|| AdapterError::bad(format!("`{clause}` must be an object")))?;
    if map.len() != 1 {
        return Err(AdapterError::bad(format!(
            "`{clause}` must name exactly one field"
        )));
    }
    let (k, v) = map.iter().next().unwrap();
    Ok((k.clone(), v.clone()))
}

/// `match`/`term` accept either `{field: value}` or `{field: {query|value: ...}}` — pull the value.
fn leaf_value<'a>(spec: &'a JsonValue, key: &str) -> &'a JsonValue {
    spec.get(key).unwrap_or(spec)
}

/// Translate one DSL query clause into a Lucene query-string fragment. Recursive (for `bool`).
pub fn translate_query(dsl: &JsonValue, formats: &FieldFormats) -> Result<String, AdapterError> {
    let obj = dsl
        .as_object()
        .ok_or_else(|| AdapterError::bad("query must be an object"))?;
    if obj.len() != 1 {
        return Err(AdapterError::bad(
            "a query clause must have exactly one type",
        ));
    }
    let (clause, body) = obj.iter().next().unwrap();
    match clause.as_str() {
        "match_all" => Ok("*:*".to_string()),

        "term" => {
            let (field, spec) = one_field(body, "term")?;
            // A temporal exact-match value is in the field's declared unit → canonical micros.
            let tok = bound_token(&field, leaf_value(&spec, "value"), formats)?;
            Ok(format!("{field}:{tok}"))
        }

        "terms" => {
            let (field, spec) = one_field(body, "terms")?;
            let arr = spec
                .as_array()
                .ok_or_else(|| AdapterError::bad("`terms` value must be an array"))?;
            if arr.is_empty() {
                return Err(AdapterError::bad("`terms` array must be non-empty"));
            }
            let toks = arr
                .iter()
                .map(|v| bound_token(&field, v, formats))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("{field}:({})", toks.join(" OR ")))
        }

        "match" => {
            let (field, spec) = one_field(body, "match")?;
            let text = leaf_value(&spec, "query");
            let text = text
                .as_str()
                .ok_or_else(|| AdapterError::bad("`match` query must be a string"))?;
            // OpenSearch `match` analyzes + ORs the tokens; we OR the whitespace-split tokens and
            // let the server analyze each (a token must be simple — see `token`).
            let toks = text
                .split_whitespace()
                .map(|t| token(&field, t))
                .collect::<Result<Vec<_>, _>>()?;
            if toks.is_empty() {
                return Err(AdapterError::bad("`match` query is empty"));
            }
            if toks.len() == 1 {
                Ok(format!("{field}:{}", toks[0]))
            } else {
                Ok(format!("{field}:({})", toks.join(" OR ")))
            }
        }

        "match_phrase" => {
            let (field, spec) = one_field(body, "match_phrase")?;
            let text = leaf_value(&spec, "query");
            let text = text
                .as_str()
                .ok_or_else(|| AdapterError::bad("`match_phrase` query must be a string"))?;
            if text.contains('"') || text.contains('\\') {
                return Err(AdapterError::unsupported(
                    "`match_phrase` text may not contain quotes or backslashes",
                ));
            }
            Ok(format!("{field}:\"{text}\""))
        }

        "multi_match" => {
            let q = body
                .get("query")
                .and_then(JsonValue::as_str)
                .ok_or_else(|| AdapterError::bad("`multi_match` needs a string `query`"))?;
            let fields = body
                .get("fields")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| AdapterError::bad("`multi_match` needs a `fields` array"))?;
            if fields.is_empty() {
                return Err(AdapterError::bad(
                    "`multi_match` `fields` must be non-empty",
                ));
            }
            // One analyzed token per field (whitespace text is rejected as a token, as above).
            let mut parts = Vec::new();
            for f in fields {
                let f = f
                    .as_str()
                    .ok_or_else(|| AdapterError::bad("`multi_match` fields must be strings"))?;
                parts.push(format!("{f}:{}", token(f, q)?));
            }
            Ok(format!("({})", parts.join(" OR ")))
        }

        "range" => {
            let (field, spec) = one_field(body, "range")?;
            translate_range(&field, &spec, formats)
        }

        "bool" => translate_bool(body, formats),

        other => Err(AdapterError::unsupported(format!(
            "query type `{other}` is not supported by the adapter (supported: match, match_phrase, \
             multi_match, term, terms, range, bool, match_all)"
        ))),
    }
}

fn translate_range(
    field: &str,
    spec: &JsonValue,
    formats: &FieldFormats,
) -> Result<String, AdapterError> {
    let get = |k: &str| spec.get(k);
    let tok = |v: &JsonValue| bound_token(field, v, formats);
    // Lower bound: gte (inclusive) or gt (exclusive); upper: lte / lt.
    let (lower, lower_inc) = match (get("gte"), get("gt")) {
        (Some(v), _) => (Some(tok(v)?), true),
        (None, Some(v)) => (Some(tok(v)?), false),
        (None, None) => (None, true),
    };
    let (upper, upper_inc) = match (get("lte"), get("lt")) {
        (Some(v), _) => (Some(tok(v)?), true),
        (None, Some(v)) => (Some(tok(v)?), false),
        (None, None) => (None, true),
    };
    if lower.is_none() && upper.is_none() {
        return Err(AdapterError::bad(format!(
            "`range` on `{field}` needs at least one of gte/gt/lte/lt"
        )));
    }
    let open = if lower_inc { '[' } else { '{' };
    let close = if upper_inc { ']' } else { '}' };
    let lo = lower.unwrap_or_default();
    let hi = upper.unwrap_or_default();
    Ok(format!("{field}:{open}{lo} TO {hi}{close}"))
}

fn translate_clauses(
    v: Option<&JsonValue>,
    formats: &FieldFormats,
) -> Result<Vec<String>, AdapterError> {
    let Some(v) = v else { return Ok(vec![]) };
    // A clause list accepts a single object or an array of objects (OpenSearch allows both).
    let items: Vec<&JsonValue> = match v {
        JsonValue::Array(a) => a.iter().collect(),
        JsonValue::Object(_) => vec![v],
        _ => return Err(AdapterError::bad("bool clause must be an object or array")),
    };
    items.iter().map(|c| translate_query(c, formats)).collect()
}

fn translate_bool(body: &JsonValue, formats: &FieldFormats) -> Result<String, AdapterError> {
    // `filter` is treated like `must` (a required conjunct) — the read-path adapter doesn't model
    // the non-scoring distinction. `must`/`filter` AND together; `must_not` negates. `should` is
    // honored for *matching* only when there is no must/filter (OpenSearch's default
    // minimum_should_match); with a must/filter present it is scoring-only and not expressible in
    // the query string, so it's dropped from the predicate (documented in the support matrix).
    let must = translate_clauses(body.get("must"), formats)?;
    let filter = translate_clauses(body.get("filter"), formats)?;
    let should = translate_clauses(body.get("should"), formats)?;
    let must_not = translate_clauses(body.get("must_not"), formats)?;

    let mut required: Vec<String> = must.into_iter().chain(filter).map(group).collect();
    if required.is_empty() && !should.is_empty() {
        // No must/filter → at least one should must match.
        let ored = should
            .into_iter()
            .map(group)
            .collect::<Vec<_>>()
            .join(" OR ");
        required.push(group(ored));
    }
    for mn in must_not {
        required.push(format!("NOT {}", group(mn)));
    }
    if required.is_empty() {
        // A purely-empty or must_not-only bool matches everything (then constrained by NOTs above).
        return Ok("*:*".to_string());
    }
    Ok(required.join(" AND "))
}

/// Parenthesize a fragment unless it's already a single bare token (keeps the string readable and
/// the precedence unambiguous when ANDed/ORed).
fn group(frag: String) -> String {
    if frag.starts_with('(') || !frag.contains(' ') {
        frag
    } else {
        format!("({frag})")
    }
}

/// Translate the OpenSearch `sort` clause to native sort keys. `_score` entries are dropped
/// (native ranks by score by default); a bare string or `{field: "asc"|"desc"}` / `{field:
/// {order}}` is accepted.
pub fn translate_sort(sort: &JsonValue) -> Result<Vec<WireSort>, AdapterError> {
    let items: Vec<&JsonValue> = match sort {
        JsonValue::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    let mut out = Vec::new();
    for item in items {
        match item {
            JsonValue::String(field) => {
                if field != "_score" {
                    out.push(WireSort {
                        field: field.clone(),
                        descending: false,
                    });
                }
            }
            JsonValue::Object(map) => {
                let (field, spec) = map
                    .iter()
                    .next()
                    .ok_or_else(|| AdapterError::bad("empty sort object"))?;
                if field == "_score" {
                    continue;
                }
                let order = match spec {
                    JsonValue::String(s) => s.clone(),
                    JsonValue::Object(o) => o
                        .get("order")
                        .and_then(JsonValue::as_str)
                        .unwrap_or("asc")
                        .to_string(),
                    _ => "asc".to_string(),
                };
                out.push(WireSort {
                    field: field.clone(),
                    descending: order == "desc",
                });
            }
            _ => return Err(AdapterError::bad("sort entries must be strings or objects")),
        }
    }
    Ok(out)
}

/// Translate an OpenSearch `highlight` clause into the native [`HighlightRequest`]. We
/// map the field set and the two bounds we support — `number_of_fragments` → `max_fragments` and
/// `fragment_size` → `fragment_size` (top-level or per-field; a per-field value wins). Everything
/// else in the OpenSearch highlight DSL (custom tags, `type`, `order`, …) is ignored: GrowlerDB
/// emits XSS-safe segments the client marks, so pre/post tags don't apply. An empty/absent `fields`
/// map highlights the default set (the index's highlightable TEXT fields).
pub fn translate_highlight(clause: &JsonValue) -> HighlightRequest {
    let obj = clause.as_object();
    let top_u32 = |key: &str| -> u32 {
        obj.and_then(|o| o.get(key))
            .and_then(JsonValue::as_u64)
            .unwrap_or(0) as u32
    };
    let (mut max_fragments, mut fragment_size) =
        (top_u32("number_of_fragments"), top_u32("fragment_size"));
    let mut fields = Vec::new();
    if let Some(fmap) = obj
        .and_then(|o| o.get("fields"))
        .and_then(JsonValue::as_object)
    {
        for (name, spec) in fmap {
            fields.push(name.clone());
            // A per-field override wins over the top-level default.
            if let Some(v) = spec.get("number_of_fragments").and_then(JsonValue::as_u64) {
                max_fragments = v as u32;
            }
            if let Some(v) = spec.get("fragment_size").and_then(JsonValue::as_u64) {
                fragment_size = v as u32;
            }
        }
    }
    HighlightRequest {
        fields,
        max_fragments,
        fragment_size,
    }
}

/// Render a wire [`HighlightField`]'s fragments to the OpenSearch response shape: a
/// vector of strings, one per fragment, with `marked` segments wrapped in `<em>…</em>` (the
/// OpenSearch default tag) and all text HTML-escaped so the fragment is safe to render.
fn highlight_field_html(field: &HighlightField) -> Vec<String> {
    field
        .fragments
        .iter()
        .map(|frag| {
            let mut s = String::new();
            for seg in &frag.segments {
                if seg.marked {
                    s.push_str("<em>");
                    s.push_str(&html_escape(&seg.text));
                    s.push_str("</em>");
                } else {
                    s.push_str(&html_escape(&seg.text));
                }
            }
            s
        })
        .collect()
}

/// Minimal HTML entity escaping for highlight fragment text (`&`, `<`, `>`).
fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// Synthesize an OpenSearch `_id` from the composite key: partition values then identifier values,
/// joined by `#`. Deterministic and round-trippable-ish (informational; hydration uses the full
/// coordinate, not this string).
pub fn compose_id(coords: &Coordinates) -> String {
    coords
        .partition
        .iter()
        .chain(coords.identifier.iter())
        .map(|f| f.value.as_ref().map(value_string).unwrap_or_default())
        .collect::<Vec<_>>()
        .join("#")
}

fn value_string(v: &v1::Value) -> String {
    match &v.kind {
        Some(Kind::Str(s)) => s.clone(),
        Some(Kind::Int(i)) => i.to_string(),
        Some(Kind::Float(f)) => f.to_string(),
        Some(Kind::Bool(b)) => b.to_string(),
        // Canonical epoch micros, rendered like an Int.
        Some(Kind::TsMicros(t)) => t.to_string(),
        None => String::new(),
    }
}

fn value_to_json(v: v1::Value) -> JsonValue {
    match v.kind {
        Some(Kind::Str(s)) => JsonValue::String(s),
        Some(Kind::Int(i)) => json!(i),
        Some(Kind::Float(f)) => json!(f),
        Some(Kind::Bool(b)) => JsonValue::Bool(b),
        // Canonical epoch micros, rendered like an Int.
        Some(Kind::TsMicros(t)) => json!(t),
        None => JsonValue::Null,
    }
}

// ---- HTTP handlers ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
struct OsSearchBody {
    #[serde(default)]
    query: Option<JsonValue>,
    #[serde(default)]
    from: Option<u32>,
    #[serde(default)]
    size: Option<u32>,
    #[serde(default)]
    sort: Option<JsonValue>,
    /// OpenSearch highlight clause: `{ "fields": { "body": {} } }`. Present ⇒ the search
    /// opts into server-side highlighting and the response carries a per-hit `highlight` object.
    #[serde(default)]
    highlight: Option<JsonValue>,
}

async fn search_all_handler(
    state: State<Arc<Gateway>>,
    headers: HeaderMap,
    body: Option<Json<OsSearchBody>>,
) -> Response {
    run_search(state, headers, "_all".to_string(), body).await
}

async fn search_handler(
    state: State<Arc<Gateway>>,
    axum::extract::Path(index): axum::extract::Path<String>,
    headers: HeaderMap,
    body: Option<Json<OsSearchBody>>,
) -> Response {
    run_search(state, headers, index, body).await
}

async fn run_search(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    index: String,
    body: Option<Json<OsSearchBody>>,
) -> Response {
    let start = std::time::Instant::now();
    let body = body.map(|Json(b)| b).unwrap_or_default();
    // OpenSearch `_all` (from `/_search`) means "no specific index": route to the endpoint's default
    // index. An empty `index` field triggers the Gateway's default/sole-index resolution
    // (a multi-index endpoint with no default answers `InvalidArgument`).
    let index = if index == "_all" {
        String::new()
    } else {
        index
    };

    // Translate the DSL (absent query => match_all). Temporal range/exact bounds are converted to
    // canonical micros here using the served index's declared field units.
    let query = match &body.query {
        Some(q) => match translate_query(q, gw.date_formats()) {
            Ok(s) => s,
            Err(e) => return adapter_error(e),
        },
        None => "*:*".to_string(),
    };
    let sort = match body.sort.as_ref().map(translate_sort).transpose() {
        Ok(s) => s.unwrap_or_default(),
        Err(e) => return adapter_error(e),
    };
    // Translate the OpenSearch `highlight` clause into the native opt-in. Present ⇒ the
    // response carries a per-hit `highlight` object of matched fragments.
    let highlight = body.highlight.as_ref().map(translate_highlight);

    let req = grpc_request(
        SearchRequest {
            query,
            limit: body.size.unwrap_or(10),
            offset: body.from.unwrap_or(0),
            sort,
            search_after: Vec::new(),
            collapse: String::new(),
            pit_id: 0,
            score_mode: v1::ScoreMode::ScoreLocal as i32,
            window: 0,
            // The adapter translates the DSL to a Lucene query string.
            syntax: v1::QuerySyntax::Lucene as i32,
            // OpenSearch semantics allow partial results by default (its
            // `allow_partial_search_results` is a query param, not a body clause); the adapter
            // keeps the native default and flags gaps via `_shards.failed`.
            require_complete: false,
            // Scope to the path's `{index}`; empty for `/_search` (the served index).
            index: index.clone(),
            highlight,
            // `_source` comes from the engine's **inline hydration**: rows attach to their
            // hits by coordinates at the Gateway (one admitted query), replacing the adapter's
            // old search-then-GetByKey pair. A hit whose row doesn't resolve (failed shard /
            // tenant-filtered / missing) carries `hydrate_error` and gets an empty `_source` —
            // hydration failure stays non-fatal, as before.
            hydrate: true,
            hydrate_columns: Vec::new(),
        },
        &headers,
    );

    let resp = match gw.search(req).await {
        Ok(r) => r.into_inner(),
        Err(status) => return status_error(status),
    };

    let mut hits = Vec::with_capacity(resp.hits.len());
    let mut max_score = f64::MIN;
    for hit in resp.hits.iter() {
        let coords = hit.coordinates.clone().unwrap_or_default();
        let id = compose_id(&coords);
        // Missing row → omit `_source` rather than shift another hit's row onto this `_id`.
        let source: Map<String, JsonValue> = hit
            .row
            .as_ref()
            .map(|row| {
                row.fields
                    .iter()
                    .filter_map(|f| f.value.clone().map(|v| (f.name.clone(), value_to_json(v))))
                    .collect()
            })
            .unwrap_or_default();
        max_score = max_score.max(hit.score);
        let mut doc = json!({
            "_index": index,
            "_id": id,
            "_score": hit.score,
            "_source": source,
        });
        // Server-side highlights: render the segment fragments into the OpenSearch
        // `highlight` shape — field → array of `<em>`-marked fragment strings. Only present when
        // the request carried a `highlight` clause and a field actually matched.
        if !hit.highlight.is_empty() {
            let hl: Map<String, JsonValue> = hit
                .highlight
                .iter()
                .map(|(field, hf)| (field.clone(), json!(highlight_field_html(hf))))
                .collect();
            if let Some(obj) = doc.as_object_mut() {
                obj.insert("highlight".to_string(), JsonValue::Object(hl));
            }
        }
        hits.push(doc);
    }

    let max_score = if hits.is_empty() {
        JsonValue::Null
    } else {
        json!(max_score)
    };
    let took = start.elapsed().as_millis() as u64;
    let body = json!({
        "took": took,
        "timed_out": false,
        "_shards": {
            "total": 1,
            "successful": if resp.partial { 0 } else { 1 },
            "skipped": 0,
            "failed": if resp.partial { 1 } else { 0 },
        },
        "hits": {
            "total": { "value": resp.total, "relation": "eq" },
            "max_score": max_score,
            "hits": hits,
        },
    });
    (StatusCode::OK, Json(body)).into_response()
}

fn adapter_error(e: AdapterError) -> Response {
    let status = if e.kind == "unsupported" {
        StatusCode::NOT_IMPLEMENTED
    } else {
        StatusCode::BAD_REQUEST
    };
    let body = json!({
        "error": { "type": e.kind, "reason": e.reason },
        "status": status.as_u16(),
    });
    (status, Json(body)).into_response()
}

fn status_error(status: tonic::Status) -> Response {
    let code = match status.code() {
        tonic::Code::InvalidArgument => StatusCode::BAD_REQUEST,
        tonic::Code::NotFound => StatusCode::NOT_FOUND,
        tonic::Code::PermissionDenied => StatusCode::FORBIDDEN,
        tonic::Code::Unauthenticated => StatusCode::UNAUTHORIZED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    let body = json!({
        "error": { "type": "search_error", "reason": status.message() },
        "status": code.as_u16(),
    });
    (code, Json(body)).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_core::Query;
    use serde_json::json;

    /// Translate a DSL clause and assert it parses into the canonical native AST — the AC
    /// "the `_search` DSL subset maps to the native AST", checked end-to-end.
    fn xlate(dsl: serde_json::Value) -> String {
        let s = translate_query(&dsl, &FieldFormats::default()).expect("should translate");
        Query::parse(&s).unwrap_or_else(|e| panic!("`{s}` should parse to the AST: {e}"));
        s
    }

    /// `xlate` with a declared temporal-field map, to assert the adapter converts range bounds.
    fn xlate_with(dsl: serde_json::Value, formats: &FieldFormats) -> String {
        let s = translate_query(&dsl, formats).expect("should translate");
        Query::parse(&s).unwrap_or_else(|e| panic!("`{s}` should parse to the AST: {e}"));
        s
    }

    #[test]
    fn adapter_converts_temporal_range_bounds_to_micros() {
        let mut fmts = FieldFormats::default();
        fmts.insert("ts".into(), growlerdb_core::TimeFormat::EpochSeconds);
        // A range written in the field's DECLARED unit (seconds) becomes canonical micros in the
        // query string — so window pruning + segment execution (both micros-native) stay consistent.
        assert_eq!(
            xlate_with(
                json!({ "range": { "ts": { "gte": 893964000, "lte": 894050400 } } }),
                &fmts
            ),
            "ts:[893964000000000 TO 894050400000000]"
        );
        // An exact `term` on the temporal field converts too.
        assert_eq!(
            xlate_with(json!({ "term": { "ts": { "value": 893964000 } } }), &fmts),
            "ts:893964000000000"
        );
        // A non-temporal field is untouched.
        assert_eq!(
            xlate(json!({ "range": { "age": { "gte": 18, "lt": 65 } } })),
            "age:[18 TO 65}"
        );
        // With NO declared format for the field, a numeric bound stays raw (native-micros contract).
        assert_eq!(
            xlate(json!({ "range": { "ts": { "gte": 893964000, "lte": 894050400 } } })),
            "ts:[893964000 TO 894050400]"
        );
    }

    #[test]
    fn match_all_maps_to_match_all() {
        // `*:*` is the universal match-all idiom; the parser maps it to the native `MatchAll` node
        // (a cheap AllQuery), not a cost-guarded term scan.
        let s = xlate(json!({ "match_all": {} }));
        assert_eq!(s, "*:*");
        assert_eq!(Query::parse(&s).unwrap(), Query::MatchAll);
    }

    #[test]
    fn term_maps_to_term() {
        // Both `{field: value}` and `{field: {value: ...}}` forms.
        for dsl in [
            json!({ "term": { "status": "active" } }),
            json!({ "term": { "status": { "value": "active" } } }),
        ] {
            let s = xlate(dsl);
            assert_eq!(s, "status:active");
            assert_eq!(
                Query::parse(&s).unwrap(),
                Query::Term {
                    field: Some("status".into()),
                    value: "active".into()
                }
            );
        }
        // A numeric term renders its scalar.
        assert_eq!(xlate(json!({ "term": { "age": 42 } })), "age:42");
    }

    #[test]
    fn terms_maps_to_an_or_of_terms() {
        let s = xlate(json!({ "terms": { "status": ["active", "pending"] } }));
        assert_eq!(s, "status:(active OR pending)");
    }

    /// The http_logs benchmark `cidr_clientip` workload query — a `term` whose value is a CIDR
    /// block on an IP field. `/` is not a query metacharacter, so it passes through the token
    /// filter and the parser recognizes the `addr/prefix` shape as a native `IpCidr` (routed to
    /// the field's IP range). On a correctly-mapped IP field this returns matches; the failure
    /// the scale run hit was an auto-mapped TEXT field, now surfaced as a clean 4xx by the gateway.
    #[test]
    fn cidr_term_on_an_ip_field_parses_to_ip_cidr() {
        let s = xlate(json!({ "term": { "client_ip": "211.0.0.0/8" } }));
        assert_eq!(s, "client_ip:211.0.0.0/8");
        assert_eq!(
            Query::parse(&s).unwrap(),
            Query::IpCidr {
                field: "client_ip".into(),
                cidr: "211.0.0.0/8".into()
            }
        );
    }

    /// The http_logs benchmark `topk_hydrated` workload sort — top-k by a numeric field, desc.
    /// Translates to a single descending `WireSort`; the server orders on the field's `fast`
    /// column (a non-`fast` field is the rejected-query case the gateway now reports as a 4xx).
    #[test]
    fn topk_hydrated_sort_translates_to_a_descending_wire_sort() {
        let out = translate_sort(&json!([{ "response_time_ms": "desc" }])).unwrap();
        assert_eq!(
            out,
            vec![WireSort {
                field: "response_time_ms".into(),
                descending: true
            }]
        );
    }

    #[test]
    fn match_single_and_multi_token() {
        assert_eq!(
            Query::parse(&xlate(json!({ "match": { "title": "hello" } }))).unwrap(),
            Query::Term {
                field: Some("title".into()),
                value: "hello".into()
            }
        );
        // multi-token OR; `{query: ...}` form.
        let s = xlate(json!({ "match": { "title": { "query": "hello world" } } }));
        assert_eq!(s, "title:(hello OR world)");
    }

    #[test]
    fn match_phrase_maps_to_phrase() {
        let s = xlate(json!({ "match_phrase": { "title": "hello world" } }));
        assert_eq!(s, "title:\"hello world\"");
        assert_eq!(
            Query::parse(&s).unwrap(),
            Query::Phrase {
                field: Some("title".into()),
                terms: vec!["hello".into(), "world".into()],
                slop: 0
            }
        );
    }

    #[test]
    fn range_bounds_and_inclusivity() {
        let s = xlate(json!({ "range": { "age": { "gte": 18, "lt": 65 } } }));
        assert_eq!(s, "age:[18 TO 65}");
        assert_eq!(
            Query::parse(&s).unwrap(),
            Query::Range {
                field: "age".into(),
                lower: Some("18".into()),
                lower_inclusive: true,
                upper: Some("65".into()),
                upper_inclusive: false,
            }
        );
        // Open upper bound (exclusive lower via `gt`; absent upper renders inclusive-empty).
        assert_eq!(
            xlate(json!({ "range": { "age": { "gt": 18 } } })),
            "age:{18 TO ]"
        );
    }

    #[test]
    fn multi_match_ors_across_fields() {
        let s = xlate(json!({ "multi_match": { "query": "alice", "fields": ["name", "email"] } }));
        assert_eq!(s, "(name:alice OR email:alice)");
    }

    #[test]
    fn bool_must_filter_must_not() {
        let s = xlate(json!({
            "bool": {
                "must": [{ "term": { "status": "active" } }],
                "filter": [{ "range": { "age": { "gte": "18" } } }],
                "must_not": [{ "term": { "deleted": "true" } }],
            }
        }));
        assert_eq!(s, "status:active AND (age:[18 TO ]) AND NOT deleted:true");
    }

    #[test]
    fn bool_should_only_ors() {
        let s = xlate(json!({
            "bool": { "should": [{ "term": { "a": "1" } }, { "term": { "b": "2" } }] }
        }));
        assert_eq!(s, "(a:1 OR b:2)");
    }

    #[test]
    fn unsupported_clause_is_a_clear_error() {
        let err = translate_query(&json!({ "fuzzy": { "x": "y" } }), &FieldFormats::default())
            .unwrap_err();
        assert_eq!(err.kind, "unsupported");
        assert!(err.reason.contains("fuzzy"));
    }

    #[test]
    fn common_value_charset_is_accepted() {
        // Hyphens / dots / dates / decimals are common in ids and must round-trip to a Term.
        for v in ["doc-2", "2024-01-01", "a_b.c", "1.5", "user@example.com"] {
            let s = xlate(json!({ "term": { "id": v } }));
            assert_eq!(s, format!("id:{v}"));
            assert_eq!(
                Query::parse(&s).unwrap(),
                Query::Term {
                    field: Some("id".into()),
                    value: v.into()
                }
            );
        }
    }

    #[test]
    fn unsafe_token_values_are_rejected() {
        // Whitespace / metacharacters in a term value → clear unsupported error, not mis-encoding.
        let err = translate_query(&json!({ "term": { "f": "a b" } }), &FieldFormats::default())
            .unwrap_err();
        assert_eq!(err.kind, "unsupported");
        let err = translate_query(&json!({ "term": { "f": "a:b" } }), &FieldFormats::default())
            .unwrap_err();
        assert_eq!(err.kind, "unsupported");
    }

    #[test]
    fn sort_translation() {
        let sort = json!(["created_at", { "age": "desc" }, { "_score": "desc" }, { "name": { "order": "asc" } }]);
        let out = translate_sort(&sort).unwrap();
        assert_eq!(out.len(), 3); // _score dropped
        assert_eq!(
            out[0],
            WireSort {
                field: "created_at".into(),
                descending: false
            }
        );
        assert_eq!(
            out[1],
            WireSort {
                field: "age".into(),
                descending: true
            }
        );
        assert_eq!(
            out[2],
            WireSort {
                field: "name".into(),
                descending: false
            }
        );
    }

    #[test]
    fn compose_id_joins_partition_then_identifier() {
        let coords = Coordinates {
            partition: vec![v1::Field {
                name: "tenant".into(),
                value: Some(v1::Value {
                    kind: Some(Kind::Int(42)),
                }),
            }],
            identifier: vec![v1::Field {
                name: "id".into(),
                value: Some(v1::Value {
                    kind: Some(Kind::Str("abc".into())),
                }),
            }],
        };
        assert_eq!(compose_id(&coords), "42#abc");
    }

    #[test]
    fn highlight_clause_maps_fields_and_bounds() {
        // `fields` + top-level bounds; a per-field bound overrides the top-level one.
        let req = translate_highlight(&json!({
            "number_of_fragments": 2,
            "fields": { "body": { "fragment_size": 80 }, "title": {} }
        }));
        assert!(req.fields.contains(&"body".to_string()));
        assert!(req.fields.contains(&"title".to_string()));
        assert_eq!(req.max_fragments, 2);
        assert_eq!(req.fragment_size, 80);

        // An empty highlight clause ⇒ default fields (none named) and default (0) bounds.
        let empty = translate_highlight(&json!({}));
        assert!(empty.fields.is_empty());
        assert_eq!(empty.max_fragments, 0);
        assert_eq!(empty.fragment_size, 0);
    }

    #[test]
    fn highlight_field_renders_em_marked_and_escaped_html() {
        // Marked segments wrap in `<em>`; all text is HTML-escaped so a `<script>` can't inject.
        let field = HighlightField {
            fragments: vec![v1::HighlightFragment {
                segments: vec![
                    v1::HighlightSegment {
                        text: "a <b>".into(),
                        marked: false,
                    },
                    v1::HighlightSegment {
                        text: "brown".into(),
                        marked: true,
                    },
                ],
            }],
        };
        assert_eq!(
            highlight_field_html(&field),
            vec!["a &lt;b&gt;<em>brown</em>".to_string()]
        );
    }
}
