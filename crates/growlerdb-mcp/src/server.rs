//! The MCP protocol core + the **stdio transport**.
//!
//! [`handle_message`] is the transport-agnostic JSON-RPC 2.0 dispatch: one parsed message in, an
//! optional response out, tools executed against any [`QueryBackend`]. [`serve_io`] wraps it in
//! the newline-delimited **stdio** loop (the local-agent path); the engine's gateway wraps it in
//! the **Streamable HTTP** transport at `POST /mcp`.
//!
//! Stdio transport contract: one JSON object per line on stdin/stdout, no embedded newlines.
//! **stdout carries ONLY JSON-RPC messages** — all diagnostics go to stderr (via `tracing`, which
//! is a no-op unless a stderr subscriber is installed by the host process).

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::backend::QueryBackend;
use crate::client::GatewayClient;
use crate::error::McpError;

/// The MCP protocol version we default to when a client's `initialize` omits one. The Streamable
/// HTTP transport exists since 2025-03-26, and the spec's guidance for a missing version header is
/// to assume this revision.
pub const DEFAULT_PROTOCOL_VERSION: &str = "2025-03-26";

/// Runtime configuration for [`serve`] (the stdio transport).
#[derive(Clone, Debug)]
pub struct McpConfig {
    /// Gateway origin, e.g. `http://127.0.0.1:8081`.
    pub gateway_url: String,
    /// Bearer token forwarded to the gateway. `None` ⇒ no `Authorization` header.
    pub token: Option<String>,
    /// Index used by a tool call that omits `index`. `None` ⇒ every call must pass `index`.
    pub default_index: Option<String>,
}

/// Serve the MCP protocol over the process's real stdin/stdout.
pub async fn serve(config: McpConfig) -> anyhow::Result<()> {
    serve_io(config, tokio::io::stdin(), tokio::io::stdout()).await
}

/// Serve the MCP protocol over arbitrary async byte streams. Factored out of [`serve`] so tests can
/// drive the loop with in-memory pipes instead of real stdio.
pub async fn serve_io<R, W>(config: McpConfig, reader: R, writer: W) -> anyhow::Result<()>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let client = GatewayClient::new(config.gateway_url.clone(), config.token.clone());
    let mut lines = BufReader::new(reader).lines();
    let mut writer = writer;

    // Read to EOF, staying alive across malformed lines and tool failures.
    while let Some(line) = lines.next_line().await? {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(response) = handle_line(line, &client, config.default_index.as_deref()).await {
            let mut bytes = serde_json::to_vec(&response)?;
            bytes.push(b'\n');
            writer.write_all(&bytes).await?;
            writer.flush().await?;
        }
    }
    Ok(())
}

/// Parse and dispatch a single JSON-RPC line. Returns `Some(response)` for a request and `None` for
/// a notification (no `id`) or an unparseable-but-idless message.
async fn handle_line<B: QueryBackend>(
    line: &str,
    backend: &B,
    default_index: Option<&str>,
) -> Option<Value> {
    let msg: Value = match serde_json::from_str(line) {
        Ok(v) => v,
        Err(e) => {
            // Parse errors carry a null id per JSON-RPC.
            return Some(error_response(
                Value::Null,
                -32700,
                &format!("parse error: {e}"),
            ));
        }
    };
    handle_message(msg, backend, default_index).await
}

/// Dispatch one parsed JSON-RPC message against `backend` — the transport-agnostic protocol
/// core shared by stdio and the gateway's Streamable HTTP route. Returns `Some(response)` for a
/// request, `None` for a notification (no `id`).
pub async fn handle_message<B: QueryBackend>(
    msg: Value,
    backend: &B,
    default_index: Option<&str>,
) -> Option<Value> {
    let method = msg
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let params = msg.get("params").cloned().unwrap_or(Value::Null);
    let id = msg.get("id").cloned();

    // A message without an `id` is a notification: act on it, but never reply.
    let Some(id) = id else {
        tracing::debug!(method, "notification received");
        return None;
    };

    let value = match method {
        "initialize" => initialize_result(&params),
        "ping" => json!({}),
        "tools/list" => json!({ "tools": tool_defs() }),
        "tools/call" => return Some(handle_tools_call(id, &params, backend, default_index).await),
        "resources/list" => json!({ "resources": resource_defs() }),
        "resources/read" => match resource_read(&params) {
            Ok(contents) => json!({ "contents": contents }),
            Err(msg) => return Some(error_response(id, -32002, &msg)),
        },
        _ => {
            return Some(error_response(
                id,
                -32601,
                &format!("method not found: {method}"),
            ))
        }
    };
    Some(success_response(id, value))
}

/// The condensed query-syntax reference, exposed as an MCP **resource** so an agent writes
/// valid Lucene/KQL on the first try instead of discovering the grammar through 400s.
const QUERY_SYNTAX_URI: &str = "growlerdb://query-syntax";
const QUERY_SYNTAX_MD: &str = include_str!("query_syntax.md");

/// The `resources/list` catalog.
fn resource_defs() -> Value {
    json!([{
        "uri": QUERY_SYNTAX_URI,
        "name": "query-syntax",
        "title": "GrowlerDB query syntax",
        "description": "Condensed Lucene/KQL grammar + what each field capability (indexed/fast/cached) \
            lets a query do, and how the search modes and inline hydration compose. Read this before \
            writing non-trivial queries; pair with `describe_index` for the target index's fields.",
        "mimeType": "text/markdown"
    }])
}

/// Resolve a `resources/read` request to its contents, or an error message for an unknown URI.
fn resource_read(params: &Value) -> Result<Value, String> {
    match params.get("uri").and_then(Value::as_str) {
        Some(QUERY_SYNTAX_URI) => Ok(json!([{
            "uri": QUERY_SYNTAX_URI,
            "mimeType": "text/markdown",
            "text": QUERY_SYNTAX_MD,
        }])),
        Some(other) => Err(format!(
            "unknown resource `{other}` (available: {QUERY_SYNTAX_URI})"
        )),
        None => Err("resources/read requires a `uri`".to_string()),
    }
}

/// Build the `initialize` result, echoing the client's requested `protocolVersion` when present.
fn initialize_result(params: &Value) -> Value {
    let protocol_version = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);
    json!({
        "protocolVersion": protocol_version,
        "capabilities": { "tools": {}, "resources": {} },
        "serverInfo": {
            "name": "growlerdb",
            "version": env!("CARGO_PKG_VERSION"),
        },
        // The MCP-official steering text a host injects into the agent's context. Field-tested:
        // an agent working inside a code checkout hears "what does the growlerdb catalog say…"
        // and greps FILES unless told these tools query the live, indexed data.
        "instructions": "GrowlerDB serves LIVE, indexed data — these tools query running search \
            indexes (in the demo: `docs`, `catalog`, `arxiv`), NOT files on disk. When asked what \
            GrowlerDB / an index / 'the catalog' says or contains, use `search` here instead of \
            file or grep tools. ALWAYS start an unfamiliar index with `describe_index`: it returns \
            the schema, example queries, and any `vector_fields` — and when vector fields exist, \
            search with `mode: hybrid` (lexical does NO stemming: a lexical `hydration` will not \
            match `hydrate`; hybrid catches both meaning and exact terms). Before trusting \
            semantic/hybrid, check the vector field's `docs_with_vector` against `num_docs` — a \
            shortfall means part of the corpus is invisible to KNN — and READ any `warnings` on a \
            search response: they flag in-band degradation (a failed hybrid arm, a fallback query \
            embed). Answers come back as governed rows with coordinates you can cite.",
    })
}

/// Dispatch a `tools/call`. Protocol-shape problems (missing name) return a JSON-RPC `-32602`;
/// tool/gateway failures return an MCP tool error (`isError: true`) so the agent can read them.
async fn handle_tools_call<B: QueryBackend>(
    id: Value,
    params: &Value,
    backend: &B,
    default_index: Option<&str>,
) -> Value {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return error_response(id, -32602, "tools/call requires a `name`");
    };
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let outcome = match name {
        "search" => tool_search(&args, backend, default_index).await,
        "hydrate" => tool_hydrate(&args, backend, default_index).await,
        "aggregate" => tool_aggregate(&args, backend, default_index).await,
        "list_indexes" => backend.list_indexes().await,
        "describe_index" => tool_describe(&args, backend, default_index).await,
        "more_like_this" => tool_more_like_this(&args, backend, default_index).await,
        other => Err(McpError::Config(format!("unknown tool: {other}"))),
    };

    let content = match &outcome {
        Ok(value) => serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
        // A failed retrieval is a result the agent should read and react to — append the
        // recovery move for the failure class instead of a bare error line.
        Err(e) => format!("{e}{}", error_hint(e)),
    };
    success_response(
        id,
        json!({
            "content": [{ "type": "text", "text": content }],
            "isError": outcome.is_err(),
        }),
    )
}

/// The actionable next step for a failed tool call, appended to the error text: what the agent
/// should *do* about this class of failure, not just what went wrong.
fn error_hint(e: &McpError) -> &'static str {
    match e {
        McpError::Gateway { status: 400, .. } => {
            "\n\nhint: the request didn't validate — call `describe_index` for this index's \
             fields (types + what each supports) and read the `growlerdb://query-syntax` \
             resource for the grammar, then rewrite the query."
        }
        McpError::Gateway { status: 404, .. } => {
            "\n\nhint: the index wasn't found here — call `list_indexes` to see what this \
             endpoint serves, and pass one of those as `index`."
        }
        McpError::Gateway {
            status: 401 | 403, ..
        } => {
            "\n\nhint: the caller's token doesn't grant this — it is scoped per index and \
             tenant. Work within the indexes `list_indexes` returns; a different scope needs a \
             different token, not a rephrased request."
        }
        _ => "",
    }
}

/// Resolve the target index for a tool call: the argument's `index`, else the configured default
/// (the gateway's own transport passes `Some("")` — empty routes to the endpoint's served index).
fn resolve_index(args: &Value, default_index: Option<&str>) -> Result<String, McpError> {
    if let Some(index) = args.get("index").and_then(Value::as_str) {
        if !index.is_empty() {
            return Ok(index.to_string());
        }
    }
    default_index.map(str::to_string).ok_or_else(|| {
        McpError::Config(
            "no index specified and no default index configured (pass `index`, or start the \
             server with --index)"
                .to_string(),
        )
    })
}

async fn tool_search<B: QueryBackend>(
    args: &Value,
    backend: &B,
    default_index: Option<&str>,
) -> Result<Value, McpError> {
    let index = resolve_index(args, default_index)?;
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let vector_field = args.get("vector_field").and_then(Value::as_str);
    // `k` and `limit` are aliases for the page size.
    let k = args
        .get("k")
        .or_else(|| args.get("limit"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let filter = args
        .get("filter")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let syntax = args
        .get("syntax")
        .and_then(Value::as_str)
        .unwrap_or_default();
    // Inline hydration: forwarded to the engine, which attaches each hit's authoritative
    // row via the governed keys:get path — the one-call form of search→hydrate.
    let hydrate = args
        .get("hydrate")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let hydrate_columns = args
        .get("hydrate_columns")
        .cloned()
        .unwrap_or_else(|| json!([]));
    // Default mode: hybrid when a vector field is given, else lexical.
    let mode = args
        .get("mode")
        .and_then(Value::as_str)
        .unwrap_or(if vector_field.is_some() {
            "hybrid"
        } else {
            "lexical"
        });

    // Context shaping: `highlight` returns matched snippets with each lexical hit; `max_chars`
    // is a response budget — hits are dropped from the tail until the payload fits.
    let highlight = args
        .get("highlight")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let max_chars = args.get("max_chars").and_then(Value::as_u64).unwrap_or(0) as usize;
    // Opt out of partial results: degradation errors instead of returning a flagged subset.
    let require_complete = args
        .get("require_complete")
        .and_then(Value::as_bool)
        .unwrap_or(false);

    let result = match mode {
        "lexical" => {
            let mut body = json!({
                "query": query,
                "limit": k,
                "syntax": syntax,
                "index": index,
                "hydrate": hydrate,
                "hydrate_columns": hydrate_columns,
                "require_complete": require_complete,
            });
            if highlight {
                // Server defaults: the index's highlightable TEXT fields, bounded fragments.
                body["highlight"] = json!({});
            }
            backend.search(body).await
        }
        "semantic" | "hybrid" => {
            let vector_field = vector_field.ok_or_else(|| {
                McpError::Config(format!(
                    "mode `{mode}` requires `vector_field` (the VECTOR field to search — \
                     `describe_index` lists them under `vector_fields`)"
                ))
            })?;
            let body = json!({
                "vector_field": vector_field,
                "query_text": query,
                "k": k,
                "filter": filter,
                "syntax": syntax,
                "index": index,
                "hydrate": hydrate,
                "hydrate_columns": hydrate_columns,
                "require_complete": require_complete,
            });
            if mode == "semantic" {
                backend.semantic_search(body).await
            } else {
                backend.hybrid_search(body).await
            }
        }
        other => {
            return Err(McpError::Config(format!(
                "unknown search mode `{other}` (use lexical|semantic|hybrid)"
            )))
        }
    };
    Ok(apply_response_budget(result?, max_chars))
}

/// Enforce the `max_chars` response budget: drop hits from the **tail** (lowest-ranked first)
/// until the serialized payload fits, and record how many were dropped as `truncated_hits` so
/// the agent knows the page was cut for size, not exhausted. `0` = no budget. Coordinates and
/// `total` are untouched — the agent can re-query with a smaller `k`, a projection, or
/// `aggregate` instead of a bigger window.
fn apply_response_budget(mut value: Value, max_chars: usize) -> Value {
    if max_chars == 0 {
        return value;
    }
    let over = |v: &Value| v.to_string().len() > max_chars;
    if !over(&value) {
        return value;
    }
    let mut dropped = 0u64;
    while over(&value) {
        match value.get_mut("hits").and_then(Value::as_array_mut) {
            Some(hits) if !hits.is_empty() => {
                hits.pop();
                dropped += 1;
            }
            _ => break,
        }
    }
    if dropped > 0 {
        if let Some(obj) = value.as_object_mut() {
            obj.insert("truncated_hits".to_string(), json!(dropped));
        }
    }
    value
}

async fn tool_hydrate<B: QueryBackend>(
    args: &Value,
    backend: &B,
    default_index: Option<&str>,
) -> Result<Value, McpError> {
    let index = resolve_index(args, default_index)?;
    let coordinates = args
        .get("coordinates")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let columns = args.get("columns").cloned().unwrap_or_else(|| json!([]));
    let body = json!({
        "keys": coordinates,
        "columns": columns,
        "index": index,
    });
    backend.hydrate(body).await
}

async fn tool_aggregate<B: QueryBackend>(
    args: &Value,
    backend: &B,
    default_index: Option<&str>,
) -> Result<Value, McpError> {
    let index = resolve_index(args, default_index)?;
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let fields = args.get("fields").cloned().unwrap_or_else(|| json!([]));
    let size = args.get("size").and_then(Value::as_u64).unwrap_or(0);
    let body = json!({
        "query": query,
        "fields": fields,
        "size": size,
        "index": index,
    });
    backend.facets(body).await
}

async fn tool_describe<B: QueryBackend>(
    args: &Value,
    backend: &B,
    default_index: Option<&str>,
) -> Result<Value, McpError> {
    let index = resolve_index(args, default_index)?;
    let mut stats = backend.describe(&index).await?;
    // Self-teaching schema: turn the returned mapping into ready-made example queries, so the
    // agent's first real query is composed from fields that exist with forms they support.
    let examples = example_queries(&stats);
    if let Some(obj) = stats.as_object_mut() {
        if !examples.is_empty() {
            obj.insert("example_queries".to_string(), json!(examples));
        }
        obj.insert(
            "guidance".to_string(),
            json!(
                "Query only the fields above (a term query needs `indexed`, a range/sort needs \
                 `fast`); `cached` fields return with hits — prefer them, and use hydrate for \
                 the rest. Grammar: read the `growlerdb://query-syntax` resource."
            ),
        );
    }
    Ok(stats)
}

/// Ready-made example queries for a described index, derived from its actual mapping — one per
/// query form the schema supports (term on the first indexed TEXT/KEYWORD, range on the first
/// `fast` DATE/numeric, semantic/hybrid when a VECTOR field exists).
fn example_queries(stats: &Value) -> Vec<Value> {
    let fields = stats
        .get("fields")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let first = |pred: &dyn Fn(&Value) -> bool| -> Option<String> {
        fields
            .iter()
            .find(|f| pred(f))
            .and_then(|f| f.get("name").and_then(Value::as_str))
            .map(str::to_string)
    };
    fn ty(f: &Value) -> &str {
        f.get("type").and_then(Value::as_str).unwrap_or("")
    }
    let flag = |f: &Value, k: &str| f.get(k).and_then(Value::as_bool).unwrap_or(false);

    let mut out = Vec::new();
    if let Some(f) = first(&|f| ty(f) == "TEXT" && flag(f, "indexed")) {
        out.push(json!({ "query": format!("{f}:(your search terms)"),
                          "note": "term/phrase match on the analyzed TEXT field" }));
    }
    if let Some(f) = first(&|f| ty(f) == "KEYWORD" && flag(f, "indexed")) {
        out.push(json!({ "query": format!("{f}:exact-value"),
                          "note": "exact match; also a good `aggregate` facet field if `fast`" }));
    }
    if let Some(f) = first(&|f| ty(f) == "DATE" && flag(f, "fast")) {
        out.push(json!({ "query": format!("{f}:[2024-01-01 TO *]"),
                          "note": "date range (ISO-8601); `fast` fields also sort" }));
    }
    if let Some(f) = first(&|f| matches!(ty(f), "LONG" | "DOUBLE") && flag(f, "fast")) {
        out.push(json!({ "query": format!("{f}:[10 TO 100]"), "note": "numeric range" }));
    }
    if let Some(v) = stats
        .get("vector_fields")
        .and_then(Value::as_array)
        .and_then(|vs| vs.first())
        .and_then(|v| v.get("name").and_then(Value::as_str))
    {
        out.push(json!({
            "arguments": { "mode": "hybrid", "vector_field": v,
                            "query": "a natural-language question", "hydrate": true },
            "note": "hybrid (lexical+semantic, fused) with authoritative rows inline — the \
                     strongest default when a VECTOR field exists"
        }));
    }
    out
}

/// **More-like-this**: given one document's coordinates, find its nearest neighbors — hydrate
/// the seed's text, embed-search it over `vector_field`, and drop the seed from the results.
async fn tool_more_like_this<B: QueryBackend>(
    args: &Value,
    backend: &B,
    default_index: Option<&str>,
) -> Result<Value, McpError> {
    let index = resolve_index(args, default_index)?;
    let seed = args
        .get("coordinates")
        .cloned()
        .ok_or_else(|| McpError::Config("more_like_this requires `coordinates` (one document's coordinates, as returned by `search`)".to_string()))?;
    let vector_field = args
        .get("vector_field")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            McpError::Config(
                "more_like_this requires `vector_field` (similarity is vector-based — \
                 `describe_index` lists them under `vector_fields`)"
                    .to_string(),
            )
        })?;
    let k = args.get("k").and_then(Value::as_u64).unwrap_or(10);
    let text_field = args.get("text_field").and_then(Value::as_str);

    // 1. Hydrate the seed's text (governed — the caller can only seed from rows it may read).
    let columns = match text_field {
        Some(f) => json!([f]),
        None => json!([]),
    };
    let hydrated = backend
        .hydrate(json!({ "keys": [seed.clone()], "columns": columns, "index": index }))
        .await?;
    let row_fields = hydrated
        .get("rows")
        .and_then(Value::as_array)
        .and_then(|rows| rows.first())
        .and_then(|row| row.get("fields"))
        .and_then(Value::as_object)
        .ok_or_else(|| {
            McpError::Config(
                "the seed document hydrated to no row — check the coordinates against a \
                 `search` hit for this index"
                    .to_string(),
            )
        })?;
    // The seed text: the named field, else every string field concatenated.
    let text = match text_field {
        Some(f) => row_fields
            .get(f)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| {
                McpError::Config(format!(
                    "seed row has no text in `{f}` — pass a `text_field` the row carries"
                ))
            })?,
        None => row_fields
            .values()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join(" "),
    };
    if text.trim().is_empty() {
        return Err(McpError::Config(
            "the seed row carries no text to match on — pass `text_field`".to_string(),
        ));
    }

    // 2. Nearest neighbors of the seed's text; over-fetch by one because the seed itself is
    //    almost always the top neighbor.
    let mut resp = backend
        .semantic_search(json!({
            "vector_field": vector_field,
            "query_text": text,
            "k": k + 1,
            "index": index,
        }))
        .await?;

    // 3. Drop the seed, then trim back to `k`.
    if let Some(hits) = resp.get_mut("hits").and_then(Value::as_array_mut) {
        let seed_ident = seed.get("identifier").cloned().unwrap_or(Value::Null);
        hits.retain(|h| {
            h.get("coordinates")
                .and_then(|c| c.get("identifier"))
                .map(|ident| *ident != seed_ident)
                .unwrap_or(true)
        });
        hits.truncate(k as usize);
    }
    Ok(resp)
}
/// The tool catalog returned by `tools/list`, each with an agent-facing description + JSON schema.
fn tool_defs() -> Value {
    let coordinates_schema = json!({
        "type": "array",
        "description": "Opaque document coordinates as returned by `search` hits. Pipe them straight into `hydrate`.",
        "items": {
            "type": "object",
            "properties": {
                "partition": {
                    "type": "array",
                    "items": { "type": "object", "properties": { "name": {"type": "string"}, "value": {} } }
                },
                "identifier": {
                    "type": "array",
                    "items": { "type": "object", "properties": { "name": {"type": "string"}, "value": {} } }
                }
            }
        }
    });

    json!([
        {
            "name": "search",
            "description": "Governed, tenant-scoped retrieval over a GrowlerDB index. Returns ranked hits as \
                document COORDINATES (partition + identifier) with relevance scores and any cached display \
                fields. Set `hydrate: true` to ALSO get each hit's authoritative source `row` in the same \
                call (the one-call form of search→hydrate; a row that fails to resolve carries a per-hit \
                `hydrate_error` instead). Without `hydrate`, pass a hit's `coordinates` to the `hydrate` \
                tool for the authoritative values. Modes: `lexical` (BM25 keyword, the default), `semantic` \
                (vector KNN — needs `vector_field`), `hybrid` (lexical+vector RRF fusion — needs \
                `vector_field`, the default when `vector_field` is given). Results are limited to what the \
                caller's bearer token is entitled to; you never see another tenant's data. Hits carry the                 index's `cached` fields — read those first and hydrate only what's missing (index authors:                 cache the fields agents read, so `search` alone answers). Call `describe_index` before                 composing non-trivial queries and read the `growlerdb://query-syntax` resource for the grammar.                 READ `warnings` when present: it names in-band degradation (a failed hybrid arm, a                 dev-fallback query embed) — treat those results as weaker than requested. `total` is the                 true corpus-wide match count in lexical mode only; semantic returns top-k (total = page                 size) and hybrid reports the lexical arm's match count when that arm succeeded.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": { "type": "string", "description": "Target index. Omit to use the server's default index." },
                    "query": { "type": "string", "description": "Query text (keyword query for lexical/hybrid; embedded text for semantic/hybrid)." },
                    "mode": { "type": "string", "enum": ["lexical", "semantic", "hybrid"], "description": "Retrieval mode. Defaults to hybrid when `vector_field` is set, else lexical." },
                    "vector_field": { "type": "string", "description": "The VECTOR field to embed+search. Required for semantic/hybrid." },
                    "k": { "type": "integer", "description": "Max results (alias: limit)." },
                    "limit": { "type": "integer", "description": "Max results (alias: k)." },
                    "filter": { "type": "string", "description": "Optional lexical/fast-field filter for the vector arm (semantic/hybrid)." },
                    "syntax": { "type": "string", "enum": ["lucene", "kql"], "description": "Query grammar. Defaults to lucene." },
                    "hydrate": { "type": "boolean", "description": "Also return each hit's authoritative source row inline (governed, same as the `hydrate` tool). Default false." },
                    "hydrate_columns": { "type": "array", "items": { "type": "string" }, "description": "Columns to hydrate when `hydrate` is set. Empty/omitted = all." },
                    "highlight": { "type": "boolean", "description": "Lexical mode: return matched snippets per hit (compact context instead of whole fields). Default false." },
                    "max_chars": { "type": "integer", "description": "Response budget: drop lowest-ranked hits until the payload fits (a `truncated_hits` count is set). 0/omitted = unlimited. Set this to protect your context window." },
                    "require_complete": { "type": "boolean", "description": "Opt out of partial results: any coverage degradation (a failed shard, a dropped hybrid arm) errors instead of returning a flagged subset. Default false (degradation is flagged via `partial`/`warnings`)." }
                },
                "required": ["query"]
            }
        },
        {
            "name": "hydrate",
            "description": "Resolve search-hit COORDINATES into authoritative, governed rows read live from the \
                index's source of truth. This is the second half of the search→hydrate pattern: `search` finds \
                what matches, `hydrate` returns the trustworthy field values. Accepts the exact `coordinates` \
                shape a `search` hit returns. Tenant-scoped by the caller's bearer token.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": { "type": "string", "description": "Target index. Omit to use the server's default index." },
                    "coordinates": coordinates_schema,
                    "columns": { "type": "array", "items": { "type": "string" }, "description": "Columns to return. Empty/omitted = all." }
                },
                "required": ["coordinates"]
            }
        },
        {
            "name": "aggregate",
            "description": "Compute term-facet counts (top values per field) over the documents matching an \
                optional query. Use it to summarize or break down a result set — e.g. counts by category, \
                status, or author — before drilling in with `search`. Tenant-scoped by the caller's token.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": { "type": "string", "description": "Target index. Omit to use the server's default index." },
                    "query": { "type": "string", "description": "Optional Lucene/KQL query to scope the facets. Empty = all documents." },
                    "fields": { "type": "array", "items": { "type": "string" }, "description": "Fields to facet on (aggregatable/keyword fields)." },
                    "size": { "type": "integer", "description": "Max buckets per field. 0/omitted = server default." }
                },
                "required": ["fields"]
            }
        },
        {
            "name": "list_indexes",
            "description": "List the indexes available to the caller (name + status). Tenant-scoped by the \
                caller's bearer token. Best-effort: served by the gateway's control-plane REST surface, which \
                a bare serving node may not expose.",
            "inputSchema": { "type": "object", "properties": {} }
        },
        {
            "name": "describe_index",
            "description": "Return one index's stats AND full schema: every mapped field with its type and \
                what a query can do with it (`indexed` = term-queryable, `fast` = range/sort/aggregate, \
                `cached` = returned with hits), the vector fields for semantic/hybrid, plus ready-made \
                `example_queries` composed from the actual mapping. CALL THIS FIRST when working with an \
                unfamiliar index. Tenant-scoped by the caller's token.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": { "type": "string", "description": "Target index. Omit to use the server's default index." }
                }
            }
        },
        {
            "name": "more_like_this",
            "description": "Find documents SIMILAR to one you already have: pass a hit's `coordinates` and a \
                `vector_field`, and get its nearest neighbors by meaning (the seed's text is hydrated — \
                governed — embedded, and searched; the seed itself is excluded). Use after a good hit to \
                expand coverage: 'find more rows like this one'. Tenant-scoped like every tool.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": { "type": "string", "description": "Target index. Omit to use the server's default index." },
                    "coordinates": {
                        "type": "object",
                        "description": "ONE document's coordinates, exactly as a `search` hit returned them.",
                        "properties": {
                            "partition": { "type": "array", "items": { "type": "object", "properties": { "name": {"type": "string"}, "value": {} } } },
                            "identifier": { "type": "array", "items": { "type": "object", "properties": { "name": {"type": "string"}, "value": {} } } }
                        }
                    },
                    "vector_field": { "type": "string", "description": "The VECTOR field to match in (see `describe_index` → `vector_fields`)." },
                    "text_field": { "type": "string", "description": "The seed row's text field to embed. Omitted = all of the row's string fields concatenated." },
                    "k": { "type": "integer", "description": "Max similar documents (default 10)." }
                },
                "required": ["coordinates", "vector_field"]
            }
        }
    ])
}

/// A JSON-RPC 2.0 success envelope.
fn success_response(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

/// A JSON-RPC 2.0 error envelope.
fn error_response(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}
