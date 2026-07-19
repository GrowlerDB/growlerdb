//! The MCP stdio server: a newline-delimited JSON-RPC 2.0 loop that exposes GrowlerDB's read-only
//! retrieval surface as MCP **tools**.
//!
//! Transport contract: one JSON object per line on stdin/stdout, no embedded newlines. **stdout
//! carries ONLY JSON-RPC messages** — all diagnostics go to stderr (via `tracing`, which is a no-op
//! unless a stderr subscriber is installed by the host process).

use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

use crate::client::GatewayClient;
use crate::error::McpError;

/// The MCP protocol version we default to when a client's `initialize` omits one.
const DEFAULT_PROTOCOL_VERSION: &str = "2024-11-05";

/// Runtime configuration for [`serve`].
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
        if let Some(response) = handle_line(line, &client, &config).await {
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
async fn handle_line(line: &str, client: &GatewayClient, config: &McpConfig) -> Option<Value> {
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
        "tools/call" => return Some(handle_tools_call(id, &params, client, config).await),
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

/// Build the `initialize` result, echoing the client's requested `protocolVersion` when present.
fn initialize_result(params: &Value) -> Value {
    let protocol_version = params
        .get("protocolVersion")
        .and_then(Value::as_str)
        .unwrap_or(DEFAULT_PROTOCOL_VERSION);
    json!({
        "protocolVersion": protocol_version,
        "capabilities": { "tools": {} },
        "serverInfo": {
            "name": "growlerdb",
            "version": env!("CARGO_PKG_VERSION"),
        },
    })
}

/// Dispatch a `tools/call`. Protocol-shape problems (missing name) return a JSON-RPC `-32602`;
/// tool/gateway failures return an MCP tool error (`isError: true`) so the agent can read them.
async fn handle_tools_call(
    id: Value,
    params: &Value,
    client: &GatewayClient,
    config: &McpConfig,
) -> Value {
    let Some(name) = params.get("name").and_then(Value::as_str) else {
        return error_response(id, -32602, "tools/call requires a `name`");
    };
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));

    let outcome = match name {
        "search" => tool_search(&args, client, config).await,
        "hydrate" => tool_hydrate(&args, client, config).await,
        "aggregate" => tool_aggregate(&args, client, config).await,
        "list_indexes" => client.list_indexes().await,
        "describe_index" => tool_describe(&args, client, config).await,
        other => Err(McpError::Config(format!("unknown tool: {other}"))),
    };

    let content = match &outcome {
        Ok(value) => serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string()),
        Err(e) => e.to_string(),
    };
    success_response(
        id,
        json!({
            "content": [{ "type": "text", "text": content }],
            "isError": outcome.is_err(),
        }),
    )
}

/// Resolve the target index for a tool call: the argument's `index`, else the configured default.
fn resolve_index(args: &Value, config: &McpConfig) -> Result<String, McpError> {
    if let Some(index) = args.get("index").and_then(Value::as_str) {
        if !index.is_empty() {
            return Ok(index.to_string());
        }
    }
    config.default_index.clone().ok_or_else(|| {
        McpError::Config(
            "no index specified and no default index configured (pass `index`, or start the \
             server with --index)"
                .to_string(),
        )
    })
}

fn tool_search(
    args: &Value,
    client: &GatewayClient,
    config: &McpConfig,
) -> impl std::future::Future<Output = Result<Value, McpError>> + Send {
    // Resolve everything up front so the returned future borrows nothing from `args`.
    let index = resolve_index(args, config);
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let vector_field = args
        .get("vector_field")
        .and_then(Value::as_str)
        .map(str::to_string);
    // `k` and `limit` are aliases for the page size.
    let k = args
        .get("k")
        .or_else(|| args.get("limit"))
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let filter = args
        .get("filter")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let syntax = args
        .get("syntax")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    // Default mode: hybrid when a vector field is given, else lexical.
    let mode = args
        .get("mode")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| {
            if vector_field.is_some() {
                "hybrid".to_string()
            } else {
                "lexical".to_string()
            }
        });
    let client = client.clone();

    async move {
        let index = index?;
        match mode.as_str() {
            "lexical" => {
                let body = json!({
                    "query": query,
                    "limit": k,
                    "syntax": syntax,
                    "index": index,
                });
                client.search(body).await
            }
            "semantic" | "hybrid" => {
                let vector_field = vector_field.ok_or_else(|| {
                    McpError::Config(format!(
                        "mode `{mode}` requires `vector_field` (the VECTOR field to search)"
                    ))
                })?;
                let body = json!({
                    "vector_field": vector_field,
                    "query_text": query,
                    "k": k,
                    "filter": filter,
                    "syntax": syntax,
                    "index": index,
                });
                if mode == "semantic" {
                    client.semantic_search(body).await
                } else {
                    client.hybrid_search(body).await
                }
            }
            other => Err(McpError::Config(format!(
                "unknown search mode `{other}` (use lexical|semantic|hybrid)"
            ))),
        }
    }
}

fn tool_hydrate(
    args: &Value,
    client: &GatewayClient,
    config: &McpConfig,
) -> impl std::future::Future<Output = Result<Value, McpError>> + Send {
    let index = resolve_index(args, config);
    let coordinates = args
        .get("coordinates")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let columns = args.get("columns").cloned().unwrap_or_else(|| json!([]));
    let client = client.clone();
    async move {
        let index = index?;
        let body = json!({
            "keys": coordinates,
            "columns": columns,
            "index": index,
        });
        client.hydrate(body).await
    }
}

fn tool_aggregate(
    args: &Value,
    client: &GatewayClient,
    config: &McpConfig,
) -> impl std::future::Future<Output = Result<Value, McpError>> + Send {
    let index = resolve_index(args, config);
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let fields = args.get("fields").cloned().unwrap_or_else(|| json!([]));
    let size = args.get("size").and_then(Value::as_u64).unwrap_or(0);
    let client = client.clone();
    async move {
        let index = index?;
        let body = json!({
            "query": query,
            "fields": fields,
            "size": size,
            "index": index,
        });
        client.facets(body).await
    }
}

fn tool_describe(
    args: &Value,
    client: &GatewayClient,
    config: &McpConfig,
) -> impl std::future::Future<Output = Result<Value, McpError>> + Send {
    let index = resolve_index(args, config);
    let client = client.clone();
    async move {
        let index = index?;
        client.describe(&index).await
    }
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
                fields — NOT authoritative rows. To read full/authoritative field values, pass a hit's \
                `coordinates` to `hydrate`. Modes: `lexical` (BM25 keyword, the default), `semantic` (vector \
                KNN — needs `vector_field`), `hybrid` (lexical+vector RRF fusion — needs `vector_field`, the \
                default when `vector_field` is given). Results are limited to what the caller's bearer token \
                is entitled to; you never see another tenant's data.",
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
                    "syntax": { "type": "string", "enum": ["lucene", "kql"], "description": "Query grammar. Defaults to lucene." }
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
            "description": "Return stats and schema hints for one index: document count, current snapshot, \
                generation/checkpoint, and the time/sort fields — useful to plan a `search` (which fields are \
                sortable or time-rangeable). Tenant-scoped by the caller's token.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "index": { "type": "string", "description": "Target index. Omit to use the server's default index." }
                }
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
