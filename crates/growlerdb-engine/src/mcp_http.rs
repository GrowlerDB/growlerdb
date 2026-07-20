//! The MCP **Streamable HTTP transport**, served by the gateway at `POST /mcp`.
//!
//! A remote agent (Claude web/desktop connector, a hosted agent platform, CI) connects with just
//! a URL + bearer token — no local `growlerdb` binary, no stdio process. The route is a thin shell
//! around [`growlerdb_mcp::handle_message`] (the same JSON-RPC dispatch the stdio transport uses);
//! tool calls re-enter the gateway's own `/v1` router **in-process** (a `tower` oneshot, no
//! network hop), so authn, RBAC, the tenant filter, admission control, body limits, and timeouts
//! are enforced by the one existing query surface. The transport **synthesizes no identity** — it
//! forwards the caller's `Authorization` header verbatim.
//!
//! Protocol shape (spec 2025-03-26+):
//! - **Sessionless.** Every request is independent; no `Mcp-Session-Id` is issued, so the route
//!   scales horizontally behind a load balancer (the spec permits stateless servers).
//! - **POST only.** The server never initiates messages, so `GET /mcp` (the SSE stream) answers
//!   `405 Method Not Allowed`; responses are single JSON bodies, not event streams.
//! - **No batching.** JSON-RPC batch arrays were removed in spec 2025-06-18; an array is a 400.
//! - **`Origin` validation** rejects DNS-rebinding: a browser-sent `Origin` must be loopback or
//!   match the request's `Host`; non-browser clients (no `Origin`) pass.
//! - A closed gateway (authenticator configured) answers a missing/invalid bearer with
//!   `401 WWW-Authenticate: Bearer` before any JSON-RPC processing.

use std::sync::Arc;

use axum::body::{to_bytes, Body, Bytes};
use axum::extract::State;
use axum::http::{header, HeaderMap, HeaderValue, Method, Request as HttpRequest, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use growlerdb_mcp::{handle_message, interpret_response, McpError, QueryBackend};
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::gateway::Gateway;
use crate::rest::grpc_request;

/// Ceiling on an internal `/v1` response read. Matches the scale of the REST front's own
/// body limit — a tool result an agent reads should never approach this.
const MCP_RESPONSE_LIMIT: usize = 8 * 1024 * 1024;

#[derive(Clone)]
struct McpState {
    /// The gateway's composed `/v1` router — the surface tool calls re-enter in-process.
    v1: Router,
    /// For the up-front auth check (`auth_required` + bearer verification).
    gw: Arc<Gateway>,
}

/// Build the `/mcp` router over the gateway's composed `/v1` router. Mount it alongside that
/// router (`app.merge(mcp_router(app.clone(), gw))`); the clone is the surface tool calls
/// dispatch into, so everything mounted on it (search, keys:get, facets, `/v1/indexes` when the
/// control-plane proxy is wired) is reachable through MCP under the same enforcement.
pub fn mcp_router(v1: Router, gw: Arc<Gateway>) -> Router {
    // POST only: axum answers other methods on a matched path with 405 + `Allow` — exactly the
    // spec's required response for a server that doesn't offer the GET/SSE stream.
    Router::new()
        .route("/mcp", post(mcp_post))
        .with_state(McpState { v1, gw })
}

/// One Streamable-HTTP request: origin gate → auth gate → parse a single JSON-RPC message →
/// dispatch → JSON response (or `202 Accepted` for a notification).
async fn mcp_post(State(st): State<McpState>, headers: HeaderMap, body: Bytes) -> Response {
    if !origin_allowed(&headers) {
        return (
            StatusCode::FORBIDDEN,
            "origin not allowed (cross-origin MCP requests must come from this host)",
        )
            .into_response();
    }
    // Closed gateway: verify the bearer before any protocol processing, so a missing/invalid
    // token is an HTTP 401 (the MCP auth signal), not a per-tool error. An open gateway skips
    // this — the zero-config trial path — and per-request enforcement still happens on the
    // `/v1` surface every tool call re-enters.
    if st.gw.auth_required() {
        let mut probe = grpc_request((), &headers);
        if st.gw.identity(&mut probe).is_err() {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"))],
                "missing or invalid bearer token",
            )
                .into_response();
        }
    }
    let msg: Value = match serde_json::from_slice(&body) {
        Ok(m) => m,
        Err(e) => {
            let err = json!({ "jsonrpc": "2.0", "id": null,
                              "error": { "code": -32700, "message": format!("parse error: {e}") } });
            return (StatusCode::BAD_REQUEST, Json(err)).into_response();
        }
    };
    if msg.is_array() {
        let err = json!({ "jsonrpc": "2.0", "id": null,
                          "error": { "code": -32600,
                                     "message": "JSON-RPC batching is not supported (MCP spec 2025-06-18)" } });
        return (StatusCode::BAD_REQUEST, Json(err)).into_response();
    }

    let backend = InProcessBackend {
        v1: st.v1.clone(),
        authorization: headers.get(header::AUTHORIZATION).cloned(),
    };
    // Default index `""`: the REST surface routes an empty index to the endpoint's served
    // index, so a single-index gateway needs no `index` argument on tool calls.
    match handle_message(msg, &backend, Some("")).await {
        Some(response) => (StatusCode::OK, Json(response)).into_response(),
        // A notification produces no JSON-RPC response: 202, empty body, per the spec.
        None => StatusCode::ACCEPTED.into_response(),
    }
}

/// DNS-rebinding defense: a request carrying an `Origin` must originate from loopback or from
/// this server's own host (same-origin, e.g. a console page). Requests without an `Origin`
/// (curl, SDKs, non-browser MCP clients) pass — the header only exists to catch browsers whose
/// DNS answer was rebound to us.
fn origin_allowed(headers: &HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok()) else {
        return true;
    };
    // `scheme://authority` → authority ("null" and malformed origins fall through to deny).
    let authority = origin
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or("")
        .trim_end_matches('/');
    if authority.is_empty() {
        return false;
    }
    // Host without the port: `[v6]:port` keeps its brackets; `host:port` drops the port.
    let host_only = if let Some(v6) = authority.strip_prefix('[') {
        v6.split(']').next().map(|h| format!("[{h}]"))
    } else {
        Some(
            authority
                .rsplit_once(':')
                .map(|(h, _)| h)
                .unwrap_or(authority)
                .to_string(),
        )
    };
    if matches!(
        host_only.as_deref(),
        Some("localhost" | "127.0.0.1" | "[::1]")
    ) {
        return true;
    }
    headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(|host| host.eq_ignore_ascii_case(authority))
        .unwrap_or(false)
}

/// The in-process [`QueryBackend`]: tool calls become HTTP requests dispatched straight into the
/// gateway's own `/v1` router (`tower` oneshot — no socket), carrying only the caller's
/// forwarded `Authorization` header. One query surface, one enforcement path.
#[derive(Clone)]
struct InProcessBackend {
    v1: Router,
    authorization: Option<HeaderValue>,
}

impl InProcessBackend {
    async fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<Value, McpError> {
        let mut builder = HttpRequest::builder().method(method).uri(path);
        if let Some(auth) = &self.authorization {
            builder = builder.header(header::AUTHORIZATION, auth.clone());
        }
        let request = match body {
            Some(v) => builder
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(v.to_string())),
            None => builder.body(Body::empty()),
        }
        .map_err(|e| McpError::Config(format!("internal request build failed: {e}")))?;
        let response = self
            .v1
            .clone()
            .oneshot(request)
            .await
            .map_err(|e| McpError::Config(format!("internal dispatch failed: {e}")))?;
        let status = response.status().as_u16();
        let bytes = to_bytes(response.into_body(), MCP_RESPONSE_LIMIT)
            .await
            .map_err(|e| McpError::Config(format!("reading internal response failed: {e}")))?;
        interpret_response(status, &bytes)
    }
}

impl QueryBackend for InProcessBackend {
    async fn search(&self, body: Value) -> Result<Value, McpError> {
        self.call(Method::POST, "/v1/search", Some(body)).await
    }

    async fn semantic_search(&self, body: Value) -> Result<Value, McpError> {
        self.call(Method::POST, "/v1/search:semantic", Some(body))
            .await
    }

    async fn hybrid_search(&self, body: Value) -> Result<Value, McpError> {
        self.call(Method::POST, "/v1/search:hybrid", Some(body))
            .await
    }

    async fn hydrate(&self, body: Value) -> Result<Value, McpError> {
        self.call(Method::POST, "/v1/keys:get", Some(body)).await
    }

    async fn facets(&self, body: Value) -> Result<Value, McpError> {
        self.call(Method::POST, "/v1/facets", Some(body)).await
    }

    async fn describe(&self, index: &str) -> Result<Value, McpError> {
        self.call(
            Method::POST,
            "/v1/index:describe",
            Some(json!({ "index": index })),
        )
        .await
    }

    async fn list_indexes(&self) -> Result<Value, McpError> {
        self.call(Method::GET, "/v1/indexes", None).await
    }
}
