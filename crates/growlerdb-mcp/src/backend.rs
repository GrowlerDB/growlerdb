//! The **query backend** the MCP tools call — the gateway's read surface, REST-JSON-shaped.
//!
//! Two implementations exist: [`GatewayClient`](crate::GatewayClient) fronts a gateway **remotely**
//! over HTTP (the stdio server's path), and the engine's Streamable-HTTP transport fronts its own
//! gateway **in-process** (the `/mcp` route oneshots the `/v1` router). Both forward only the
//! caller's bearer — the backend never synthesizes an identity — so RBAC + tenant isolation are
//! always the gateway's.

use std::future::Future;

use serde_json::Value;

use crate::error::McpError;

/// The read endpoints the tools use. Bodies and results are the gateway's REST JSON shapes
/// (`/v1/search`, `/v1/keys:get`, …) verbatim — the tools compose requests and pass responses
/// through, so every implementation serves identical tool behavior.
pub trait QueryBackend: Send + Sync {
    /// `POST /v1/search` — lexical (BM25) search.
    fn search(&self, body: Value) -> impl Future<Output = Result<Value, McpError>> + Send;
    /// `POST /v1/search:semantic` — semantic (vector KNN) search.
    fn semantic_search(&self, body: Value) -> impl Future<Output = Result<Value, McpError>> + Send;
    /// `POST /v1/search:hybrid` — hybrid (lexical + vector, RRF-fused) search.
    fn hybrid_search(&self, body: Value) -> impl Future<Output = Result<Value, McpError>> + Send;
    /// `POST /v1/keys:get` — hydrate coordinates into authoritative rows.
    fn hydrate(&self, body: Value) -> impl Future<Output = Result<Value, McpError>> + Send;
    /// `POST /v1/facets` — term-facet aggregation.
    fn facets(&self, body: Value) -> impl Future<Output = Result<Value, McpError>> + Send;
    /// `POST /v1/index:describe` — index stats.
    fn describe(&self, index: &str) -> impl Future<Output = Result<Value, McpError>> + Send;
    /// `GET /v1/indexes` — list available indexes (best-effort: control-plane surface).
    fn list_indexes(&self) -> impl Future<Output = Result<Value, McpError>> + Send;
}

/// Interpret a gateway REST response: a 2xx parses as JSON, anything else becomes a typed
/// [`McpError::Gateway`] from the `{code, message}` error body — shared by every backend so a
/// tool error reads identically over stdio and HTTP.
pub fn interpret_response(status: u16, body: &[u8]) -> Result<Value, McpError> {
    if (200..300).contains(&status) {
        serde_json::from_slice(body).map_err(|e| {
            McpError::Config(format!(
                "gateway returned unparseable JSON (status {status}): {e}"
            ))
        })
    } else {
        let body: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        let code = body
            .get("code")
            .and_then(Value::as_str)
            .unwrap_or("UNKNOWN")
            .to_string();
        let message = body
            .get("message")
            .and_then(Value::as_str)
            .unwrap_or("gateway request failed")
            .to_string();
        Err(McpError::Gateway {
            status,
            code,
            message,
        })
    }
}
