//! HTTP client that fronts the GrowlerDB **gateway** REST surface (`<gateway-url>/v1/...`) —
//! the remote [`QueryBackend`] the stdio server uses.
//!
//! The MCP server embeds no engine: it forwards the caller's bearer token to the gateway and lets
//! the gateway's existing RBAC + tenant isolation govern every read. We only ever *forward* the
//! token — we never synthesize an identity or a tenant — so an agent can never reach data the token
//! isn't already entitled to.

use serde_json::Value;

use crate::backend::{interpret_response, QueryBackend};
use crate::error::McpError;

/// A thin reqwest wrapper over the gateway's read endpoints. Cheap to clone (`reqwest::Client` is
/// an `Arc` internally); one per server is plenty.
#[derive(Clone)]
pub struct GatewayClient {
    http: reqwest::Client,
    /// Gateway origin, e.g. `http://127.0.0.1:8081` (no trailing slash, no `/v1`).
    base_url: String,
    /// The bearer token forwarded on every request. `None` ⇒ send no `Authorization` header
    /// (only useful against an unauthenticated dev gateway).
    token: Option<String>,
}

impl GatewayClient {
    /// Build a client for `base_url` (the gateway origin), forwarding `token` when present.
    pub fn new(base_url: impl Into<String>, token: Option<String>) -> Self {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        GatewayClient {
            http: reqwest::Client::new(),
            base_url,
            token,
        }
    }

    /// `POST <base>/v1<path>` with a JSON body, returning the parsed JSON response.
    async fn post(&self, path: &str, body: Value) -> Result<Value, McpError> {
        let url = format!("{}/v1{}", self.base_url, path);
        let mut req = self.http.post(&url).json(&body);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        Self::read(req.send().await?).await
    }

    /// `GET <base>/v1<path>`, returning the parsed JSON response.
    async fn get(&self, path: &str) -> Result<Value, McpError> {
        let url = format!("{}/v1{}", self.base_url, path);
        let mut req = self.http.get(&url);
        if let Some(token) = &self.token {
            req = req.bearer_auth(token);
        }
        Self::read(req.send().await?).await
    }

    /// Map a response into either its JSON body (2xx) or a typed [`McpError::Gateway`] so a
    /// tool call surfaces it as `isError` — via the backend-shared [`interpret_response`].
    async fn read(resp: reqwest::Response) -> Result<Value, McpError> {
        let status = resp.status().as_u16();
        let body = resp.bytes().await?;
        interpret_response(status, &body)
    }

    /// `POST /v1/login` — exchange credentials for a session token. Returns the token string.
    pub async fn login(&self, username: &str, password: &str) -> Result<String, McpError> {
        let resp = self
            .post(
                "/login",
                serde_json::json!({ "username": username, "password": password }),
            )
            .await?;
        resp.get("token")
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| McpError::Config("login response contained no token".to_string()))
    }
}

impl QueryBackend for GatewayClient {
    async fn search(&self, body: Value) -> Result<Value, McpError> {
        self.post("/search", body).await
    }

    async fn semantic_search(&self, body: Value) -> Result<Value, McpError> {
        self.post("/search:semantic", body).await
    }

    async fn hybrid_search(&self, body: Value) -> Result<Value, McpError> {
        self.post("/search:hybrid", body).await
    }

    async fn hydrate(&self, body: Value) -> Result<Value, McpError> {
        self.post("/keys:get", body).await
    }

    async fn facets(&self, body: Value) -> Result<Value, McpError> {
        self.post("/facets", body).await
    }

    async fn describe(&self, index: &str) -> Result<Value, McpError> {
        self.post("/index:describe", serde_json::json!({ "index": index }))
            .await
    }

    async fn list_indexes(&self) -> Result<Value, McpError> {
        self.get("/indexes").await
    }
}
