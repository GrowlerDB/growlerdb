//! The **Engine API over REST/JSON** ([Engine API]): an axum HTTP surface
//! mirroring the query/admin RPCs 1:1 under `/v1/...`. Each handler maps a JSON DTO to the
//! proto request and dispatches through the [Gateway](crate::gateway::Gateway) — which
//! routes to a Node ([in-process](crate::node::LocalNode) when embedded, gRPC when
//! distributed) — then maps the proto response back to JSON. gRPC `Status` codes map to
//! HTTP status codes.
//!
//! Auth parity: only the `authorization` bearer is forwarded into request metadata; the
//! [AuthN layer](crate::authn) stamps verified identity downstream, so caller-asserted
//! `x-growlerdb-*` identity headers are never propagated (they would be forgeable on a
//! control-plane path without an authenticator).
//!
//! [Engine API]: ../../../design/01-engine-api.md

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as JsonValue};
use tonic::{Code, Request, Status};

use growlerdb_proto::v1::{
    self, value::Kind, AggregateRequest, Coordinates, Field, GetByKeyRequest, SearchRequest,
    Sort as WireSort, SuggestKind, SuggestRequest,
};

use growlerdb_proto::ControlPlaneClient;

use crate::gateway::Gateway;

/// Ceiling on a REST request body — a query DTO is small, so this rejects an oversized upload
/// before it is buffered.
const REST_BODY_LIMIT: usize = 1 << 20; // 1 MiB

/// Wall-clock ceiling on a REST request, mirroring the [Gateway's per-query deadline]. Bounds a
/// slow request at the edge in addition to the Gateway's own single-shard timeout.
///
/// [Gateway's per-query deadline]: crate::gateway::GatewayLimits
const REST_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Build the `/v1/...` REST router over the [Gateway](crate::gateway::Gateway).
pub fn router(gateway: Arc<Gateway>) -> Router {
    Router::new()
        .route("/v1/config", get(config_handler))
        .route("/v1/me", get(me_handler))
        .route("/v1/search", post(search_handler))
        .route("/v1/explain", post(explain_handler))
        .route("/v1/facets", post(facets_handler))
        .route("/v1/suggest", post(suggest_handler))
        .route("/v1/keys:get", post(get_by_key_handler))
        .route("/v1/index:describe", post(describe_handler))
        .route("/v1/index:reindex", post(reindex_handler))
        .route("/v1/index:alter", post(alter_handler))
        .route("/v1/index:compact", post(compact_handler))
        .route("/v1/index:backup", post(backup_handler))
        .route("/v1/index:backup-status", post(backup_status_handler))
        .route("/v1/cold", get(cold_status_handler))
        .layer(axum::extract::DefaultBodyLimit::max(REST_BODY_LIMIT))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            REST_REQUEST_TIMEOUT,
        ))
        .with_state(gateway)
}

/// `GET /v1/cold` — cold-tier status: per-window hot/cold tier + the shared read-through
/// cache's hit/miss/byte stats. 404 on a non-windowed index (nothing to tier).
async fn cold_status_handler(State(gw): State<Arc<Gateway>>) -> axum::response::Response {
    use axum::response::IntoResponse;
    match gw.cold_status() {
        Some(status) => axum::Json(status).into_response(),
        None => (
            axum::http::StatusCode::NOT_FOUND,
            "not a windowed index (no cold tier)",
        )
            .into_response(),
    }
}

/// Axum middleware: record **RED metrics** for every REST request via
/// [`sli::http_request`](growlerdb_telemetry::sli::http_request) — the matched route *template*,
/// the response status code, and the wall-clock duration. Apply it **once** to the fully-merged
/// `/v1/*` router (after all `.merge`s) so a single layer covers every endpoint. Paths that matched
/// no route (404s) are bucketed as `"<unmatched>"` so a flood of bad URLs can't explode the label
/// set. Drives the Runtime "API …" panels + the Search "query status codes" panel.
pub async fn track_http_metrics(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Response {
    let route = req
        .extensions()
        .get::<axum::extract::MatchedPath>()
        .map(|m| m.as_str().to_owned())
        .unwrap_or_else(|| "<unmatched>".to_string());
    let start = std::time::Instant::now();
    let resp = next.run(req).await;
    growlerdb_telemetry::sli::http_request(
        &route,
        resp.status().as_u16(),
        start.elapsed().as_secs_f64(),
    );
    resp
}

/// As [`router`], but also serves the built **UI SPA** from `ui_dir`: static assets
/// directly, with `index.html` as the SPA fallback for client-side routes (e.g. `/indexes`).
/// The `/v1/...` API routes take precedence, so the SPA only handles paths the API doesn't —
/// this is what "served by the Engine binary" means (wiki/20-ui). `ui_dir` is the Vite `dist/`.
pub fn router_with_ui(gateway: Arc<Gateway>, ui_dir: &std::path::Path) -> Router {
    use tower_http::services::{ServeDir, ServeFile};
    let spa_fallback = ServeFile::new(ui_dir.join("index.html"));
    let assets = ServeDir::new(ui_dir).fallback(spa_fallback);
    router(gateway).fallback_service(assets)
}

/// The **control-plane** REST surface: index lifecycle (`/v1/indexes`) + source
/// introspection (`/v1/source:describe`), proxied to the Control Plane over gRPC. Merge into the
/// query [`router`] so the UI (and REST clients) can manage indexes, not just query them. Auth
/// headers are forwarded as metadata, so the Control Plane's RBAC seam governs these the same as
/// over gRPC.
pub fn control_router(client: ControlPlaneClient<tonic::transport::Channel>) -> Router {
    use axum::routing::get;
    Router::new()
        .route(
            "/v1/indexes",
            get(list_indexes_handler).post(create_index_handler),
        )
        .route(
            "/v1/indexes/{name}",
            get(get_index_handler).delete(drop_index_handler),
        )
        .route(
            "/v1/aliases",
            get(list_aliases_handler).post(set_alias_handler),
        )
        .route(
            "/v1/aliases/{alias}",
            axum::routing::delete(drop_alias_handler),
        )
        .route("/v1/source:describe", post(describe_source_handler))
        .route("/v1/index:activity", post(list_activity_handler))
        .route("/v1/ingestion", get(ingestion_status_handler))
        .route("/v1/ingestion/{name}", get(ingestion_status_one_handler))
        .route(
            "/v1/saved-queries",
            get(list_saved_queries_handler).post(save_saved_query_handler),
        )
        .route(
            "/v1/saved-queries/{id}",
            axum::routing::put(update_saved_query_handler).delete(delete_saved_query_handler),
        )
        .route("/v1/users", get(list_users_handler))
        .route(
            "/v1/users/{subject}/roles",
            axum::routing::put(set_user_roles_handler),
        )
        .route("/v1/roles", get(list_roles_handler))
        .route(
            "/v1/tokens",
            get(list_tokens_handler).post(create_token_handler),
        )
        .route(
            "/v1/tokens/{id}",
            axum::routing::delete(revoke_token_handler),
        )
        .route("/v1/login", post(login_handler))
        .with_state(client)
}

/// A **metrics proxy** to a Prometheus-compatible backend: the UI's native SLI panels
/// query `/v1/stats/...` **same-origin** (no CORS, no hardcoded Prometheus URL in the browser),
/// and the Engine forwards to Prometheus's query API. Read-only passthrough of `query`,
/// `query_range`, and `alerts`.
pub fn stats_router(prometheus_base: impl Into<String>) -> Router {
    use axum::routing::get;
    let proxy = Arc::new(StatsProxy {
        client: reqwest::Client::new(),
        base: prometheus_base.into().trim_end_matches('/').to_string(),
    });
    Router::new()
        .route("/v1/stats/query", get(stats_query_handler))
        .route("/v1/stats/query_range", get(stats_query_range_handler))
        .route("/v1/stats/alerts", get(stats_alerts_handler))
        .route("/v1/alerts", get(alerts_handler))
        .with_state(proxy)
}

struct StatsProxy {
    client: reqwest::Client,
    base: String,
}

impl StatsProxy {
    /// GET `{base}{path}?{query}` and pass the upstream status + JSON body straight through.
    async fn forward(&self, path: &str, query: Option<String>) -> Response {
        let url = match &query {
            Some(q) if !q.is_empty() => format!("{}{}?{}", self.base, path, q),
            _ => format!("{}{}", self.base, path),
        };
        match self.client.get(&url).send().await {
            Ok(resp) => {
                let status =
                    StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
                let body = resp.bytes().await.unwrap_or_default();
                Response::builder()
                    .status(status)
                    .header(axum::http::header::CONTENT_TYPE, "application/json")
                    .body(axum::body::Body::from(body))
                    .unwrap()
            }
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                format!("metrics backend error: {e}"),
            )
                .into_response(),
        }
    }
}

async fn stats_query_handler(
    State(proxy): State<Arc<StatsProxy>>,
    axum::extract::RawQuery(q): axum::extract::RawQuery,
) -> Response {
    proxy.forward("/api/v1/query", q).await
}

async fn stats_query_range_handler(
    State(proxy): State<Arc<StatsProxy>>,
    axum::extract::RawQuery(q): axum::extract::RawQuery,
) -> Response {
    proxy.forward("/api/v1/query_range", q).await
}

async fn stats_alerts_handler(State(proxy): State<Arc<StatsProxy>>) -> Response {
    proxy.forward("/api/v1/alerts", None).await
}

/// `GET /v1/alerts` — server-evaluated **firing alerts**, normalized from the metrics
/// backend's Prometheus alerting rules. A clean `{ alerts: [{ name, severity, summary, state }] }`
/// the console binds to directly (no client-side thresholds). `502` if the metrics backend is down,
/// so the console can fall back to its local SLI checks.
async fn alerts_handler(
    State(proxy): State<Arc<StatsProxy>>,
) -> Result<Json<AlertsDto>, StatusCode> {
    let url = format!("{}/api/v1/alerts", proxy.base);
    let resp = proxy
        .client
        .get(&url)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let parsed: PromAlertsResp = resp.json().await.map_err(|_| StatusCode::BAD_GATEWAY)?;
    let alerts = parsed
        .data
        .alerts
        .into_iter()
        .filter(|a| a.state == "firing" || a.state == "pending")
        .map(|a| {
            let name = a.labels.get("alertname").cloned().unwrap_or_default();
            let summary = a
                .annotations
                .get("summary")
                .or_else(|| a.annotations.get("description"))
                .cloned()
                .unwrap_or_else(|| name.clone());
            AlertDto {
                severity: a
                    .labels
                    .get("severity")
                    .cloned()
                    .unwrap_or_else(|| "warning".to_string()),
                name,
                summary,
                state: a.state,
                value: a.value,
            }
        })
        .collect();
    Ok(Json(AlertsDto { alerts }))
}

#[derive(Deserialize, Default)]
struct PromAlertsResp {
    #[serde(default)]
    data: PromAlertsData,
}
#[derive(Deserialize, Default)]
struct PromAlertsData {
    #[serde(default)]
    alerts: Vec<PromAlert>,
}
#[derive(Deserialize)]
struct PromAlert {
    #[serde(default)]
    labels: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    annotations: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    state: String,
    #[serde(default)]
    value: String,
}

#[derive(Serialize)]
struct AlertDto {
    name: String,
    severity: String,
    summary: String,
    state: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    value: String,
}
#[derive(Serialize)]
struct AlertsDto {
    alerts: Vec<AlertDto>,
}

type ControlClient = ControlPlaneClient<tonic::transport::Channel>;

async fn list_indexes_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
) -> Result<Json<IndexListDto>, ApiError> {
    let req = grpc_request(v1::ListIndexesRequest {}, &headers);
    let resp = client
        .clone()
        .list_indexes(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(IndexListDto::from(resp.into_inner())))
}

async fn get_index_handler(
    State(client): State<ControlClient>,
    axum::extract::Path(name): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Result<Json<IndexInfoDto>, ApiError> {
    let req = grpc_request(v1::GetIndexRequest { name }, &headers);
    let resp = client
        .clone()
        .get_index(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(IndexInfoDto::from(resp.into_inner())))
}

async fn create_index_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
    Json(dto): Json<CreateIndexDto>,
) -> Result<Json<CreateIndexRespDto>, ApiError> {
    let req = grpc_request(
        v1::CreateIndexRequest {
            definition_yaml: dto.definition,
        },
        &headers,
    );
    let resp = client
        .clone()
        .create_index(req)
        .await
        .map_err(ApiError::from)?;
    let resp = resp.into_inner();
    Ok(Json(CreateIndexRespDto {
        name: resp.name,
        warnings: resp.warnings,
    }))
}

async fn drop_index_handler(
    State(client): State<ControlClient>,
    axum::extract::Path(name): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let req = grpc_request(v1::DropIndexRequest { name }, &headers);
    client
        .clone()
        .drop_index(req)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn list_aliases_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
) -> Result<Json<AliasListDto>, ApiError> {
    let req = grpc_request(v1::ListAliasesRequest {}, &headers);
    let resp = client
        .clone()
        .list_aliases(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(AliasListDto::from(resp.into_inner())))
}

async fn set_alias_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
    Json(dto): Json<SetAliasDto>,
) -> Result<StatusCode, ApiError> {
    let req = grpc_request(
        v1::SetAliasRequest {
            alias: dto.alias,
            targets: dto.targets,
        },
        &headers,
    );
    client
        .clone()
        .set_alias(req)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn drop_alias_handler(
    State(client): State<ControlClient>,
    axum::extract::Path(alias): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let req = grpc_request(v1::DropAliasRequest { alias }, &headers);
    client
        .clone()
        .drop_alias(req)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- activity log --------------------------------------------------------------

#[derive(Deserialize)]
struct ActivityDto {
    #[serde(default)]
    index: String,
    #[serde(default)]
    limit: u32,
}
#[derive(Serialize)]
struct ActivityEventDto {
    ts_ms: i64,
    kind: String,
    message: String,
}
#[derive(Serialize)]
struct ActivityRespDto {
    events: Vec<ActivityEventDto>,
}

/// `POST /v1/index:activity` — the index's lifecycle/audit log, newest-first.
async fn list_activity_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
    Json(dto): Json<ActivityDto>,
) -> Result<Json<ActivityRespDto>, ApiError> {
    let req = grpc_request(
        v1::ListActivityRequest {
            index: dto.index,
            limit: dto.limit,
        },
        &headers,
    );
    let resp = client
        .clone()
        .list_activity(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(ActivityRespDto {
        events: resp
            .into_inner()
            .events
            .into_iter()
            .map(|e| ActivityEventDto {
                ts_ms: e.ts_ms,
                kind: e.kind,
                message: e.message,
            })
            .collect(),
    }))
}

// ---- API tokens ----------------------------------------------------------------

#[derive(Serialize)]
struct TokenMetaDto {
    id: String,
    label: String,
    prefix: String,
    roles: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty")]
    owner: String,
    created_at_ms: i64,
}
#[derive(Serialize)]
struct TokensDto {
    tokens: Vec<TokenMetaDto>,
}
#[derive(Serialize)]
struct CreateTokenRespDto {
    token: TokenMetaDto,
    /// The raw secret — present only on creation; never stored or listed.
    secret: String,
}
#[derive(Deserialize)]
struct CreateTokenDto {
    label: String,
    #[serde(default)]
    roles: Vec<String>,
}

impl From<v1::ApiTokenMeta> for TokenMetaDto {
    fn from(m: v1::ApiTokenMeta) -> Self {
        TokenMetaDto {
            id: m.id,
            label: m.label,
            prefix: m.prefix,
            roles: m.roles,
            owner: m.owner,
            created_at_ms: m.created_at_ms,
        }
    }
}

async fn list_tokens_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
) -> Result<Json<TokensDto>, ApiError> {
    let req = grpc_request(v1::ListTokensRequest {}, &headers);
    let resp = client
        .clone()
        .list_tokens(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(TokensDto {
        tokens: resp
            .into_inner()
            .tokens
            .into_iter()
            .map(TokenMetaDto::from)
            .collect(),
    }))
}

/// `POST /v1/login` — built-in credential login. **Unauthenticated** (it establishes
/// auth): verifies the username/password against the control-plane credential store and returns a
/// session JWT the console sends as `Authorization: Bearer`. Proxies the `Login` control-plane RPC.
async fn login_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
    Json(dto): Json<LoginDto>,
) -> Result<Json<LoginRespDto>, ApiError> {
    let req = grpc_request(
        v1::LoginRequest {
            username: dto.username,
            password: dto.password,
        },
        &headers,
    );
    let resp = client
        .clone()
        .login(req)
        .await
        .map_err(ApiError::from)?
        .into_inner();
    Ok(Json(LoginRespDto {
        token: resp.token,
        expires_at_ms: resp.expires_at_ms,
        roles: resp.roles,
    }))
}

#[derive(Deserialize)]
struct LoginDto {
    username: String,
    #[serde(default)]
    password: String,
}

#[derive(Serialize)]
struct LoginRespDto {
    token: String,
    expires_at_ms: i64,
    roles: Vec<String>,
}

async fn create_token_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
    Json(dto): Json<CreateTokenDto>,
) -> Result<Json<CreateTokenRespDto>, ApiError> {
    let req = grpc_request(
        v1::CreateTokenRequest {
            label: dto.label,
            roles: dto.roles,
        },
        &headers,
    );
    let resp = client
        .clone()
        .create_token(req)
        .await
        .map_err(ApiError::from)?
        .into_inner();
    Ok(Json(CreateTokenRespDto {
        token: resp.token.map(TokenMetaDto::from).unwrap_or(TokenMetaDto {
            id: String::new(),
            label: String::new(),
            prefix: String::new(),
            roles: Vec::new(),
            owner: String::new(),
            created_at_ms: 0,
        }),
        secret: resp.secret,
    }))
}

async fn revoke_token_handler(
    State(client): State<ControlClient>,
    axum::extract::Path(id): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let req = grpc_request(v1::RevokeTokenRequest { id }, &headers);
    client
        .clone()
        .revoke_token(req)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

// ---- users & roles -------------------------------------------------------------

#[derive(Serialize)]
struct UserDto {
    subject: String,
    roles: Vec<String>,
}
#[derive(Serialize)]
struct UsersDto {
    users: Vec<UserDto>,
}
#[derive(Serialize)]
struct RolesDto {
    roles: Vec<String>,
}
#[derive(Deserialize)]
struct SetRolesDto {
    #[serde(default)]
    roles: Vec<String>,
}

impl From<v1::RoleBinding> for UserDto {
    fn from(b: v1::RoleBinding) -> Self {
        UserDto {
            subject: b.subject,
            roles: b.roles,
        }
    }
}

/// `GET /v1/users` — local role bindings. Admin-gated at the control plane.
async fn list_users_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
) -> Result<Json<UsersDto>, ApiError> {
    let req = grpc_request(v1::ListUsersRequest {}, &headers);
    let resp = client
        .clone()
        .list_users(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(UsersDto {
        users: resp
            .into_inner()
            .users
            .into_iter()
            .map(UserDto::from)
            .collect(),
    }))
}

/// `PUT /v1/users/{subject}/roles` — replace a subject's local roles (empty clears). Admin-gated.
async fn set_user_roles_handler(
    State(client): State<ControlClient>,
    axum::extract::Path(subject): axum::extract::Path<String>,
    headers: HeaderMap,
    Json(dto): Json<SetRolesDto>,
) -> Result<Json<UserDto>, ApiError> {
    let req = grpc_request(
        v1::SetUserRolesRequest {
            subject,
            roles: dto.roles,
        },
        &headers,
    );
    let resp = client
        .clone()
        .set_user_roles(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(resp.into_inner().user.map(UserDto::from).unwrap_or(
        UserDto {
            subject: String::new(),
            roles: Vec::new(),
        },
    )))
}

/// `GET /v1/roles` — the assignable role names.
async fn list_roles_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
) -> Result<Json<RolesDto>, ApiError> {
    let req = grpc_request(v1::ListRolesRequest {}, &headers);
    let resp = client
        .clone()
        .list_roles(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(RolesDto {
        roles: resp.into_inner().roles,
    }))
}

// ---- saved searches ------------------------------------------------------------

#[derive(Serialize, Deserialize, Default)]
struct SavedQueryDto {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    owner: String,
    #[serde(default)]
    query: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    state: String,
    #[serde(default, skip_serializing_if = "is_false")]
    shared: bool,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    created_at_ms: i64,
}

fn is_zero_i64(n: &i64) -> bool {
    *n == 0
}

impl From<v1::SavedQuery> for SavedQueryDto {
    fn from(q: v1::SavedQuery) -> Self {
        SavedQueryDto {
            id: q.id,
            name: q.name,
            owner: q.owner,
            query: q.query,
            state: q.state,
            shared: q.shared,
            created_at_ms: q.created_at_ms,
        }
    }
}

impl From<SavedQueryDto> for v1::SavedQuery {
    fn from(d: SavedQueryDto) -> Self {
        v1::SavedQuery {
            id: d.id,
            name: d.name,
            owner: d.owner,
            query: d.query,
            state: d.state,
            shared: d.shared,
            created_at_ms: d.created_at_ms,
        }
    }
}

#[derive(Serialize)]
struct SavedQueriesDto {
    queries: Vec<SavedQueryDto>,
}

async fn list_saved_queries_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
) -> Result<Json<SavedQueriesDto>, ApiError> {
    let req = grpc_request(v1::ListSavedQueriesRequest {}, &headers);
    let resp = client
        .clone()
        .list_saved_queries(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(SavedQueriesDto {
        queries: resp
            .into_inner()
            .queries
            .into_iter()
            .map(SavedQueryDto::from)
            .collect(),
    }))
}

async fn save_saved_query_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
    Json(dto): Json<SavedQueryDto>,
) -> Result<Json<SavedQueryDto>, ApiError> {
    let req = grpc_request(
        v1::SaveSavedQueryRequest {
            query: Some(dto.into()),
        },
        &headers,
    );
    let resp = client
        .clone()
        .save_saved_query(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(
        resp.into_inner()
            .query
            .map(SavedQueryDto::from)
            .unwrap_or_default(),
    ))
}

async fn update_saved_query_handler(
    State(client): State<ControlClient>,
    axum::extract::Path(id): axum::extract::Path<String>,
    headers: HeaderMap,
    Json(mut dto): Json<SavedQueryDto>,
) -> Result<Json<SavedQueryDto>, ApiError> {
    dto.id = id; // the path id wins, so PUT always targets an existing row
    let req = grpc_request(
        v1::SaveSavedQueryRequest {
            query: Some(dto.into()),
        },
        &headers,
    );
    let resp = client
        .clone()
        .save_saved_query(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(
        resp.into_inner()
            .query
            .map(SavedQueryDto::from)
            .unwrap_or_default(),
    ))
}

async fn delete_saved_query_handler(
    State(client): State<ControlClient>,
    axum::extract::Path(id): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let req = grpc_request(v1::DeleteSavedQueryRequest { id }, &headers);
    client
        .clone()
        .delete_saved_query(req)
        .await
        .map_err(ApiError::from)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn describe_source_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
    Json(dto): Json<DescribeSourceDto>,
) -> Result<Json<SourceSchemaDto>, ApiError> {
    let req = grpc_request(v1::DescribeSourceRequest { table: dto.table }, &headers);
    let resp = client
        .clone()
        .describe_source(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(SourceSchemaDto::from(resp.into_inner())))
}

async fn ingestion_status_handler(
    State(client): State<ControlClient>,
    headers: HeaderMap,
) -> Result<Json<IngestionDto>, ApiError> {
    ingestion_status(client, headers, String::new()).await
}

async fn ingestion_status_one_handler(
    State(client): State<ControlClient>,
    axum::extract::Path(name): axum::extract::Path<String>,
    headers: HeaderMap,
) -> Result<Json<IngestionDto>, ApiError> {
    ingestion_status(client, headers, name).await
}

async fn ingestion_status(
    client: ControlClient,
    headers: HeaderMap,
    index: String,
) -> Result<Json<IngestionDto>, ApiError> {
    let req = grpc_request(v1::IngestionStatusRequest { index }, &headers);
    let resp = client
        .clone()
        .ingestion_status(req)
        .await
        .map_err(ApiError::from)?;
    Ok(Json(IngestionDto::from(resp.into_inner())))
}

// ---- control-plane DTOs --------------------------------------------------------

#[derive(Serialize)]
struct IndexListDto {
    indexes: Vec<IndexSummaryDto>,
}

#[derive(Serialize)]
struct IndexSummaryDto {
    name: String,
    status: String,
}

impl From<v1::ListIndexesResponse> for IndexListDto {
    fn from(r: v1::ListIndexesResponse) -> Self {
        IndexListDto {
            indexes: r
                .indexes
                .into_iter()
                .map(|s| IndexSummaryDto {
                    name: s.name,
                    status: s.status,
                })
                .collect(),
        }
    }
}

#[derive(Serialize)]
struct AliasDto {
    alias: String,
    targets: Vec<String>,
}

#[derive(Serialize)]
struct AliasListDto {
    aliases: Vec<AliasDto>,
}

impl From<v1::ListAliasesResponse> for AliasListDto {
    fn from(r: v1::ListAliasesResponse) -> Self {
        AliasListDto {
            aliases: r
                .aliases
                .into_iter()
                .map(|a| AliasDto {
                    alias: a.alias,
                    targets: a.targets,
                })
                .collect(),
        }
    }
}

#[derive(Deserialize)]
struct SetAliasDto {
    alias: String,
    #[serde(default)]
    targets: Vec<String>,
}

#[derive(Serialize)]
struct IndexInfoDto {
    name: String,
    status: String,
    shard_count: u32,
    routing: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    fields: Vec<FieldMappingDto>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    shards: Vec<ShardStatusDto>,
}

#[derive(Serialize)]
struct ShardStatusDto {
    ordinal: u32,
    #[serde(skip_serializing_if = "is_zero_i64")]
    window: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    primary: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    replicas: Vec<String>,
    state: String,
}

impl From<v1::ShardStatus> for ShardStatusDto {
    fn from(s: v1::ShardStatus) -> Self {
        ShardStatusDto {
            ordinal: s.ordinal,
            window: s.window,
            primary: s.primary,
            replicas: s.replicas,
            state: s.state,
        }
    }
}

#[derive(Serialize)]
struct FieldMappingDto {
    path: String,
    #[serde(rename = "type")]
    ty: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    analyzer: String,
    fast: bool,
    cached: bool,
    pk: bool,
    #[serde(skip_serializing_if = "String::is_empty")]
    blocked: String,
}

impl From<v1::FieldMapping> for FieldMappingDto {
    fn from(f: v1::FieldMapping) -> Self {
        FieldMappingDto {
            path: f.path,
            ty: f.r#type,
            analyzer: f.analyzer,
            fast: f.fast,
            cached: f.cached,
            pk: f.pk,
            blocked: f.blocked,
        }
    }
}

impl From<v1::GetIndexResponse> for IndexInfoDto {
    fn from(r: v1::GetIndexResponse) -> Self {
        let routing = match v1::RoutingStrategy::try_from(r.routing) {
            Ok(v1::RoutingStrategy::RoutingPartition) => "partition",
            _ => "hash",
        };
        IndexInfoDto {
            name: r.name,
            status: r.status,
            shard_count: r.shard_count,
            routing: routing.to_string(),
            fields: r.fields.into_iter().map(FieldMappingDto::from).collect(),
            shards: r
                .shard_status
                .into_iter()
                .map(ShardStatusDto::from)
                .collect(),
        }
    }
}

#[derive(Deserialize)]
struct CreateIndexDto {
    /// The index-definition YAML (carries name + source + mapping).
    definition: String,
}

#[derive(Serialize)]
struct CreateIndexRespDto {
    name: String,
    /// Non-fatal resolution warnings (e.g. the `PREDICATE` location strategy's
    /// honest-scope note). Omitted when empty.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    warnings: Vec<String>,
}

#[derive(Deserialize)]
struct DescribeSourceDto {
    table: String,
}

#[derive(Serialize)]
struct SourceSchemaDto {
    fields: Vec<SourceFieldDto>,
    partition_fields: Vec<String>,
    identifier_fields: Vec<String>,
}

#[derive(Serialize)]
struct SourceFieldDto {
    path: String,
    #[serde(rename = "type")]
    ty: String,
}

impl From<v1::DescribeSourceResponse> for SourceSchemaDto {
    fn from(r: v1::DescribeSourceResponse) -> Self {
        SourceSchemaDto {
            fields: r
                .fields
                .into_iter()
                .map(|f| SourceFieldDto {
                    path: f.path,
                    ty: f.r#type,
                })
                .collect(),
            partition_fields: r.partition_fields,
            identifier_fields: r.identifier_fields,
        }
    }
}

#[derive(Serialize)]
struct IngestionDto {
    items: Vec<IndexIngestionDto>,
}

#[derive(Serialize)]
struct IndexIngestionDto {
    name: String,
    status: String,
    source_table: String,
    routing: String,
    shard_count: u32,
    /// The source table's current Iceberg snapshot (0 = none); `null` when unreadable.
    source_snapshot_id: Option<i64>,
    /// Commit time of that snapshot (epoch ms); `null` when unreadable/none.
    source_timestamp_ms: Option<i64>,
    shards: Vec<ShardIngestionDto>,
}

#[derive(Serialize)]
struct ShardIngestionDto {
    ordinal: u32,
    node: String,
    /// The source snapshot this shard reflects (0 = nothing committed yet).
    committed_snapshot_id: i64,
    index_snapshot: u64,
    state: String,
    /// Wall-clock staleness vs the source head, ms (0 when in_sync/unknown).
    lag_ms: i64,
    /// For a windowed index: the time-window id this row represents; 0 for an ordinal shard.
    window: i64,
}

impl From<v1::IngestionStatusResponse> for IngestionDto {
    fn from(r: v1::IngestionStatusResponse) -> Self {
        IngestionDto {
            items: r.items.into_iter().map(IndexIngestionDto::from).collect(),
        }
    }
}

impl From<v1::IndexIngestion> for IndexIngestionDto {
    fn from(i: v1::IndexIngestion) -> Self {
        let routing = match v1::RoutingStrategy::try_from(i.routing) {
            Ok(v1::RoutingStrategy::RoutingPartition) => "partition",
            _ => "hash",
        };
        IndexIngestionDto {
            name: i.name,
            status: i.status,
            source_table: i.source_table,
            routing: routing.to_string(),
            shard_count: i.shard_count,
            // Collapse the source-readable flag to nullable fields: the UI shows "—" for null.
            source_snapshot_id: i.source_readable.then_some(i.source_snapshot_id),
            source_timestamp_ms: i.source_readable.then_some(i.source_timestamp_ms),
            shards: i
                .shards
                .into_iter()
                .map(|s| ShardIngestionDto {
                    ordinal: s.ordinal,
                    node: s.node,
                    committed_snapshot_id: s.committed_snapshot_id,
                    index_snapshot: s.index_snapshot,
                    state: s.state,
                    lag_ms: s.lag_ms,
                    window: s.window,
                })
                .collect(),
        }
    }
}

// ---- handlers ------------------------------------------------------------------

async fn search_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<SearchDto>,
) -> Result<Json<SearchRespDto>, ApiError> {
    let req = grpc_request(dto.into_proto(), &headers);
    let resp = gw.search(req).await.map_err(ApiError::from)?;
    Ok(Json(SearchRespDto::from(resp.into_inner())))
}

/// `GET /v1/me` — the verified caller's identity + roles, for the console's header/Settings.
/// Authenticates the bearer at the gateway and returns the trusted `{ subject, display_name, email,
/// tenant, roles }`. On an open gateway (no `--oidc-issuer`) returns the anonymous shape; a
/// configured gateway with a missing/invalid token returns 401 (the console treats it as anonymous).
async fn me_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
) -> Result<Json<MeDto>, ApiError> {
    let mut req = grpc_request((), &headers);
    let id = gw.identity(&mut req).map_err(ApiError::from)?;
    Ok(Json(MeDto {
        authenticated: !id.principal.is_empty(),
        subject: id.principal,
        display_name: id.display_name,
        email: id.email,
        tenant: id.tenant,
        roles: id.roles,
    }))
}

/// `GET /v1/config` — **unauthenticated** runtime config the console needs *before* sign-in.
/// Always 200 (unlike `/v1/me`, which 401s for an anonymous caller on a closed gateway),
/// so the SPA can reliably learn whether to gate the app behind a login screen. `auth_required` is
/// true in closed mode (an authenticator is configured), false in the open trial/POC mode.
async fn config_handler(State(gw): State<Arc<Gateway>>) -> Json<ConfigDto> {
    Json(ConfigDto {
        auth_required: gw.auth_required(),
        password_login: gw.password_login(),
        grafana_url: grafana_url_from_env(),
    })
}

/// The deployment's Grafana base URL, from the gateway process's `GROWLERDB_GRAFANA_URL` env.
/// Runtime, not build-time, so the same static SPA points at *this* deployment's
/// Grafana — a cluster install sets the env; a bare install leaves it unset and the console simply
/// hides the "Open Grafana" link rather than sending users to a wrong/localhost dashboard.
fn grafana_url_from_env() -> Option<String> {
    std::env::var("GROWLERDB_GRAFANA_URL")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

#[derive(Serialize)]
struct ConfigDto {
    auth_required: bool,
    /// Built-in username/password login is available — the console shows a login form
    /// (vs an OIDC redirect).
    password_login: bool,
    /// The deployment's Grafana base URL, or omitted when unset — then the console hides
    /// the "Open Grafana" link instead of defaulting to a deceptive localhost URL.
    #[serde(skip_serializing_if = "Option::is_none")]
    grafana_url: Option<String>,
}

#[derive(Serialize)]
struct MeDto {
    authenticated: bool,
    subject: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tenant: Option<String>,
    roles: Vec<String>,
}

/// `POST /v1/explain` — explain how a query scores one document. Opt-in, per-hit: the
/// console's drawer calls it for a selected result. Returns the BM25 clause tree, analyzed terms,
/// per-stage timings, and shard counts.
async fn explain_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<ExplainDto>,
) -> Result<Json<ExplainRespDto>, ApiError> {
    let req = grpc_request(dto.into_proto()?, &headers);
    let resp = gw.explain(req).await.map_err(ApiError::from)?;
    Ok(Json(ExplainRespDto::from(resp.into_inner())))
}

/// `POST /v1/facets` — left-rail facets for the console. Computes, for each requested
/// field, a top-N **terms** aggregation over the docs the `query` matches, by **reusing the
/// distributed Aggregate path** — no parallel facet engine. Each field is aggregated
/// independently so a field that isn't a fast/aggregatable column is simply *skipped* (it returns
/// no group) rather than failing the whole rail. The query already carries any active filter
/// clauses, so facet counts reflect the current refinement.
async fn facets_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<FacetsDto>,
) -> Result<Json<FacetsRespDto>, ApiError> {
    const MAX_FIELDS: usize = 12;
    const DEFAULT_SIZE: u32 = 10;
    let size = if dto.size == 0 {
        DEFAULT_SIZE
    } else {
        dto.size.min(100)
    };

    let mut facets = Vec::new();
    let mut partial = false;
    for field in dto.fields.into_iter().take(MAX_FIELDS) {
        // One terms agg per field (externally-tagged `Agg`): `{"f": {"Terms": {"field", "size"}}}`.
        let aggs = serde_json::json!({
            "f": { "Terms": { "field": field, "size": size } }
        })
        .to_string();
        let req = grpc_request(
            AggregateRequest {
                query: dto.query.clone(),
                aggs,
                partial: false,
                window: 0,
                index: dto.index.clone(),
            },
            &headers,
        );
        // A non-fast/unknown field errors the agg — skip it, don't fail the whole request.
        let Ok(resp) = gw.aggregate(req).await else {
            continue;
        };
        let resp = resp.into_inner();
        if resp.failed_shards > 0 {
            partial = true;
        }
        let buckets = parse_terms_buckets(&resp.results);
        if !buckets.is_empty() {
            facets.push(FacetGroupDto { field, buckets });
        }
    }
    Ok(Json(FacetsRespDto { facets, partial }))
}

/// Extract `{value, count}` from a single-terms Aggregate result JSON shaped
/// `{"f": {"buckets": [{"key": <v>, "doc_count": <n>}, …]}}` (Tantivy terms result).
fn parse_terms_buckets(results: &str) -> Vec<FacetBucketDto> {
    let Ok(json) = serde_json::from_str::<JsonValue>(results) else {
        return Vec::new();
    };
    let Some(buckets) = json
        .get("f")
        .and_then(|f| f.get("buckets"))
        .and_then(|b| b.as_array())
    else {
        return Vec::new();
    };
    buckets
        .iter()
        .filter_map(|b| {
            let count = b.get("doc_count")?.as_u64()?;
            let value = match b.get("key")? {
                JsonValue::String(s) => s.clone(),
                other => other.to_string(),
            };
            Some(FacetBucketDto { value, count })
        })
        .collect()
}

async fn suggest_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<SuggestDto>,
) -> Result<Json<SuggestRespDto>, ApiError> {
    let req = grpc_request(dto.into_proto(), &headers);
    let resp = gw.suggest(req).await.map_err(ApiError::from)?;
    Ok(Json(SuggestRespDto::from(resp.into_inner())))
}

async fn describe_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<DescribeDto>,
) -> Result<Json<IndexStatsDto>, ApiError> {
    let req = grpc_request(
        v1::DescribeIndexRequest {
            index: dto.index,
            window: 0,
        },
        &headers,
    );
    let resp = gw.describe_index(req).await.map_err(ApiError::from)?;
    let stats = resp.into_inner().stats.unwrap_or_default();
    Ok(Json(IndexStatsDto::from(stats)))
}

/// `POST /v1/index:reindex` — rebuild an index from its source and durably swap it live.
/// The Engine-side trigger for the console's reindex button; the write-fence
/// and single-flight guard live on the owning Node, so a reindex already in progress surfaces as
/// `412 Precondition Failed`. Single-shard (embedded) deployments only — a multi-shard gateway
/// returns `501 Not Implemented` (distributed reindex orchestration is future work).
async fn reindex_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<ReindexDto>,
) -> Result<Json<ReindexRespDto>, ApiError> {
    let req = grpc_request(
        v1::ReindexIndexRequest {
            index: dto.index,
            ..Default::default()
        },
        &headers,
    );
    let resp = gw
        .reindex_index(req)
        .await
        .map_err(ApiError::from)?
        .into_inner();
    Ok(Json(ReindexRespDto {
        doc_count: resp.doc_count,
        snapshot: resp.snapshot,
    }))
}

/// `POST /v1/index:alter` — plan (and optionally apply in-place) an index-definition change.
/// Diffs the candidate `definition_yaml` against the served definition and returns the
/// plan: `requires_reindex` + `reindex_reasons` for changes that need a rebuild (which this does
/// **not** perform — use `/v1/index:reindex`), and `in_place_changes` for metadata-only changes,
/// applied live when `apply` is true. Single-shard (embedded) only — a multi-shard gateway returns
/// `501`; a node without source access returns `501`; an unserved index `404`.
async fn alter_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<AlterDto>,
) -> Result<Json<AlterPlanDto>, ApiError> {
    let req = grpc_request(
        v1::AlterIndexRequest {
            index: dto.index,
            definition_yaml: dto.definition_yaml,
            apply: dto.apply,
        },
        &headers,
    );
    let resp = gw
        .alter_index(req)
        .await
        .map_err(ApiError::from)?
        .into_inner();
    let plan = resp.plan.unwrap_or_default();
    Ok(Json(AlterPlanDto {
        is_noop: plan.is_noop,
        requires_reindex: plan.requires_reindex,
        reindex_reasons: plan.reindex_reasons,
        in_place_changes: plan.in_place_changes,
        applied: dto.apply && !plan.requires_reindex && !plan.is_noop,
    }))
}

/// `POST /v1/index:compact` — compact the served shard's segments. Reports the live
/// segment count before/after the merge.
async fn compact_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<ReindexDto>,
) -> Result<Json<CompactRespDto>, ApiError> {
    let req = grpc_request(v1::CompactIndexRequest { index: dto.index }, &headers);
    let resp = gw
        .compact_index(req)
        .await
        .map_err(ApiError::from)?
        .into_inner();
    Ok(Json(CompactRespDto {
        segments_before: resp.segments_before,
        segments_after: resp.segments_after,
    }))
}

/// `POST /v1/index:backup` — back up the served shard to object storage. `501` when the
/// node has no backup target configured.
async fn backup_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<BackupDto>,
) -> Result<Json<BackupRespDto>, ApiError> {
    let req = grpc_request(
        v1::BackupIndexRequest {
            index: dto.index,
            prefix: dto.prefix,
        },
        &headers,
    );
    let resp = gw
        .backup_index(req)
        .await
        .map_err(ApiError::from)?
        .into_inner();
    Ok(Json(BackupRespDto {
        snapshot: resp.snapshot,
        file_count: resp.file_count,
        created_ms: resp.created_ms,
        prefix: resp.prefix,
    }))
}

/// `POST /v1/index:backup-status` — last-backup status; `configured=false` when the node
/// has no backup target.
async fn backup_status_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<ReindexDto>,
) -> Result<Json<BackupStatusDto>, ApiError> {
    let req = grpc_request(v1::BackupStatusRequest { index: dto.index }, &headers);
    let resp = gw
        .backup_status(req)
        .await
        .map_err(ApiError::from)?
        .into_inner();
    Ok(Json(BackupStatusDto {
        configured: resp.configured,
        present: resp.present,
        snapshot: resp.snapshot,
        created_ms: resp.created_ms,
        file_count: resp.file_count,
    }))
}

#[derive(Serialize)]
struct CompactRespDto {
    segments_before: u64,
    segments_after: u64,
}

#[derive(Deserialize)]
struct BackupDto {
    #[serde(default)]
    index: String,
    #[serde(default)]
    prefix: String,
}

#[derive(Serialize)]
struct BackupRespDto {
    snapshot: u64,
    file_count: u64,
    created_ms: u64,
    prefix: String,
}

#[derive(Serialize)]
struct BackupStatusDto {
    configured: bool,
    present: bool,
    #[serde(skip_serializing_if = "is_zero")]
    snapshot: u64,
    #[serde(skip_serializing_if = "is_zero")]
    created_ms: u64,
    #[serde(skip_serializing_if = "is_zero")]
    file_count: u64,
}

async fn get_by_key_handler(
    State(gw): State<Arc<Gateway>>,
    headers: HeaderMap,
    Json(dto): Json<GetByKeyDto>,
) -> Result<Json<GetByKeyRespDto>, ApiError> {
    let req = grpc_request(dto.into_proto()?, &headers);
    let resp = gw.get_by_key(req).await.map_err(ApiError::from)?;
    Ok(Json(GetByKeyRespDto::from(resp.into_inner())))
}

// ---- request/response DTOs -----------------------------------------------------

#[derive(Deserialize)]
struct ExplainDto {
    query: String,
    coordinates: CoordinatesDto,
    #[serde(default)]
    syntax: String,
    #[serde(default)]
    index: String,
}

impl ExplainDto {
    fn into_proto(self) -> Result<growlerdb_proto::v1::ExplainRequest, ApiError> {
        Ok(growlerdb_proto::v1::ExplainRequest {
            query: self.query,
            coordinates: Some(dto_to_coords(self.coordinates)?),
            syntax: if self.syntax.eq_ignore_ascii_case("kql") {
                v1::QuerySyntax::Kql as i32
            } else {
                v1::QuerySyntax::Lucene as i32
            },
            index: self.index,
        })
    }
}

#[derive(Serialize)]
struct ExplainRespDto {
    found: bool,
    matched: bool,
    score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    detail: Option<ExplainClauseDto>,
    analyzed: Vec<AnalyzedFieldDto>,
    timings: TimingsDto,
    shards_scanned: u32,
    shards_total: u32,
}

#[derive(Serialize)]
struct ExplainClauseDto {
    description: String,
    score: f64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    details: Vec<ExplainClauseDto>,
}

#[derive(Serialize)]
struct AnalyzedFieldDto {
    field: String,
    terms: Vec<String>,
}

#[derive(Serialize)]
struct TimingsDto {
    index_ms: f64,
    hydration_ms: f64,
    total_ms: f64,
}

impl From<v1::ExplainClause> for ExplainClauseDto {
    fn from(c: v1::ExplainClause) -> Self {
        ExplainClauseDto {
            description: c.description,
            score: c.score,
            details: c.details.into_iter().map(ExplainClauseDto::from).collect(),
        }
    }
}

impl From<v1::ExplainResponse> for ExplainRespDto {
    fn from(r: v1::ExplainResponse) -> Self {
        let t = r.timings.unwrap_or_default();
        ExplainRespDto {
            found: r.found,
            matched: r.matched,
            score: r.score,
            detail: r.detail.map(ExplainClauseDto::from),
            analyzed: r
                .analyzed
                .into_iter()
                .map(|a| AnalyzedFieldDto {
                    field: a.field,
                    terms: a.terms,
                })
                .collect(),
            timings: TimingsDto {
                index_ms: t.index_ms,
                hydration_ms: t.hydration_ms,
                total_ms: t.total_ms,
            },
            shards_scanned: r.shards_scanned,
            shards_total: r.shards_total,
        }
    }
}

#[derive(Deserialize)]
struct FacetsDto {
    /// Lucene query the facets are scoped to (carries any active filter clauses).
    query: String,
    /// Fields to facet on (terms). Non-aggregatable fields are skipped. Capped server-side.
    #[serde(default)]
    fields: Vec<String>,
    /// Max buckets per field (0 ⇒ a server default).
    #[serde(default)]
    size: u32,
    /// Target index name. Empty = the endpoint's default index.
    #[serde(default)]
    index: String,
}

#[derive(Serialize)]
struct FacetsRespDto {
    facets: Vec<FacetGroupDto>,
    #[serde(skip_serializing_if = "is_false")]
    partial: bool,
}

#[derive(Serialize)]
struct FacetGroupDto {
    field: String,
    buckets: Vec<FacetBucketDto>,
}

#[derive(Serialize)]
struct FacetBucketDto {
    value: String,
    count: u64,
}

#[derive(Deserialize)]
struct SearchDto {
    query: String,
    #[serde(default)]
    limit: u32,
    #[serde(default)]
    offset: u32,
    #[serde(default)]
    sort: Vec<SortDto>,
    #[serde(default)]
    collapse: String,
    #[serde(default)]
    pit_id: u64,
    /// Opaque keyset cursor from a prior response's `next_cursor` (it is UTF-8 JSON).
    #[serde(default)]
    search_after: Option<String>,
    /// Query grammar: `"lucene"` (default) or `"kql"`.
    #[serde(default)]
    syntax: String,
    /// Target index name. Empty = the index this endpoint serves. A serving Gateway
    /// rejects a name that doesn't match the index it fronts (`404`).
    #[serde(default)]
    index: String,
    /// Opt into **server-side highlighting**. Present ⇒ each hit carries a `highlight`
    /// object of matched fragments per field. Absent (the default) ⇒ no highlights (a per-hit cost).
    #[serde(default)]
    highlight: Option<HighlightDto>,
}

#[derive(Deserialize)]
struct SortDto {
    field: String,
    #[serde(default)]
    desc: bool,
}

/// Server-side highlight options over REST. All fields optional: an empty `fields` list
/// highlights the index's default highlightable TEXT fields; `0`/omitted bounds use server defaults.
#[derive(Deserialize)]
struct HighlightDto {
    #[serde(default)]
    fields: Vec<String>,
    #[serde(default)]
    max_fragments: u32,
    #[serde(default)]
    fragment_size: u32,
}

/// Page size for a REST search that omits (or sends `0` for) `limit`. `limit = 0` on the wire still
/// means "unbounded" for advanced/gRPC callers, but over REST an omitted limit is a footgun (it
/// would stream the whole result set), so the REST front defaults it. For a full scan use the
/// scroll/export path, not an unbounded page.
const DEFAULT_PAGE_SIZE: u32 = 10;

impl SearchDto {
    fn into_proto(self) -> SearchRequest {
        SearchRequest {
            query: self.query,
            // A REST search with no `limit` gets a bounded page, not the entire result set.
            limit: if self.limit == 0 {
                DEFAULT_PAGE_SIZE
            } else {
                self.limit
            },
            offset: self.offset,
            sort: self
                .sort
                .into_iter()
                .map(|s| WireSort {
                    field: s.field,
                    descending: s.desc,
                })
                .collect(),
            collapse: self.collapse,
            pit_id: self.pit_id,
            search_after: self
                .search_after
                .map(String::into_bytes)
                .unwrap_or_default(),
            // REST doesn't expose scoring mode yet; default per-shard BM25 (design/09).
            score_mode: growlerdb_proto::v1::ScoreMode::ScoreLocal as i32,
            // The window selector is gateway-internal; a client request never sets it.
            window: 0,
            // Query grammar: `"kql"` → KQL, anything else → Lucene (the default).
            syntax: if self.syntax.eq_ignore_ascii_case("kql") {
                growlerdb_proto::v1::QuerySyntax::Kql as i32
            } else {
                growlerdb_proto::v1::QuerySyntax::Lucene as i32
            },
            // Per-index scoping: pass the target index through; the serving Gateway
            // validates it. Empty means "the index served here".
            index: self.index,
            // Server-side highlighting opt-in; absent ⇒ no highlights.
            highlight: self.highlight.map(|h| v1::HighlightRequest {
                fields: h.fields,
                max_fragments: h.max_fragments,
                fragment_size: h.fragment_size,
            }),
        }
    }
}

#[derive(Serialize)]
struct SearchRespDto {
    hits: Vec<HitDto>,
    total: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    next_cursor: Option<String>,
    /// At least one shard failed to respond, so `hits`/`total` under-count. Set
    /// by the Gateway; omitted when the result is complete, so callers can trust a missing flag.
    #[serde(skip_serializing_if = "is_false")]
    partial: bool,
    /// Shards the Gateway queried vs the index's total: a time/window filter prunes
    /// shards it can prove won't match, so the console shows a "scanned/total" ratio. Both omitted
    /// when `shards_total` is 0 (a bare Node with no shard scope).
    #[serde(skip_serializing_if = "is_zero_u32")]
    shards_scanned: u32,
    #[serde(skip_serializing_if = "is_zero_u32")]
    shards_total: u32,
}

#[derive(Serialize)]
struct HitDto {
    coordinates: CoordinatesDto,
    score: f64,
    /// Cached display fields returned with the hit — a results page renders
    /// document-like rows without hydration. Omitted when the index caches no display fields.
    #[serde(skip_serializing_if = "Map::is_empty")]
    fields: Map<String, JsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    group: Option<JsonValue>,
    #[serde(skip_serializing_if = "is_zero")]
    group_count: u64,
    /// **Server-side highlights**: field → fragments → XSS-safe `{text, marked}`
    /// segments of the analyzed match. Present only when the request opted in and a field matched;
    /// the console renders `marked` segments in `<mark>`. Omitted otherwise.
    #[serde(skip_serializing_if = "Map::is_empty")]
    highlight: Map<String, JsonValue>,
}

/// One XSS-safe highlight segment over REST: a run of text and whether it is a matched
/// term. Mirrors the console's `Segment` shape so the wire and the client-side fallback render alike.
#[derive(Serialize)]
struct SegmentDto {
    text: String,
    marked: bool,
}

impl From<v1::SearchResponse> for SearchRespDto {
    fn from(r: v1::SearchResponse) -> Self {
        SearchRespDto {
            total: r.total,
            partial: r.partial,
            shards_scanned: r.shards_scanned,
            shards_total: r.shards_total,
            next_cursor: (!r.next_cursor.is_empty())
                .then(|| String::from_utf8_lossy(&r.next_cursor).into_owned()),
            hits: r
                .hits
                .into_iter()
                .map(|h| HitDto {
                    coordinates: coords_to_dto(h.coordinates),
                    score: h.score,
                    fields: h
                        .fields
                        .into_iter()
                        .filter_map(|f| f.value.map(|v| (f.name, value_to_json(v))))
                        .collect(),
                    group: h.group.map(value_to_json),
                    group_count: h.group_count,
                    highlight: highlight_to_json(h.highlight),
                })
                .collect(),
        }
    }
}

#[derive(Deserialize)]
struct SuggestDto {
    field: String,
    text: String,
    #[serde(default)]
    limit: u32,
    #[serde(default)]
    fuzzy: bool,
    #[serde(default)]
    max_edits: u32,
    /// Target index name. Empty = the endpoint's default index. Lets the console's
    /// autocomplete suggest over the selected index on a multi-index endpoint.
    #[serde(default)]
    index: String,
}

impl SuggestDto {
    fn into_proto(self) -> SuggestRequest {
        SuggestRequest {
            field: self.field,
            text: self.text,
            limit: self.limit,
            kind: if self.fuzzy {
                SuggestKind::Fuzzy
            } else {
                SuggestKind::Prefix
            } as i32,
            max_edits: self.max_edits,
            // The window selector is gateway-internal; a client request never sets it.
            window: 0,
            index: self.index,
        }
    }
}

#[derive(Serialize)]
struct SuggestRespDto {
    suggestions: Vec<SuggestionDto>,
    /// Shards that failed to respond, so the merged suggestions under-count.
    /// Omitted when complete.
    #[serde(skip_serializing_if = "is_zero_u32")]
    failed_shards: u32,
}

#[derive(Serialize)]
struct SuggestionDto {
    text: String,
    count: u64,
}

impl From<v1::SuggestResponse> for SuggestRespDto {
    fn from(r: v1::SuggestResponse) -> Self {
        SuggestRespDto {
            failed_shards: r.failed_shards,
            suggestions: r
                .suggestions
                .into_iter()
                .map(|s| SuggestionDto {
                    text: s.text,
                    count: s.count,
                })
                .collect(),
        }
    }
}

#[derive(Deserialize, Default)]
struct DescribeDto {
    #[serde(default)]
    index: String,
}

#[derive(Deserialize, Default)]
struct ReindexDto {
    #[serde(default)]
    index: String,
}

#[derive(Serialize)]
struct ReindexRespDto {
    /// Documents in the rebuilt index.
    doc_count: u64,
    /// The rebuilt index's commit snapshot.
    snapshot: u64,
}

#[derive(Deserialize, Default)]
struct AlterDto {
    #[serde(default)]
    index: String,
    /// Candidate index-definition YAML to diff against the served definition.
    #[serde(default)]
    definition_yaml: String,
    /// `false` (default) ⇒ dry-run plan; `true` ⇒ apply the in-place changes live.
    #[serde(default)]
    apply: bool,
}

#[derive(Serialize)]
struct AlterPlanDto {
    /// The candidate is identical to the served definition — nothing to do.
    is_noop: bool,
    /// Some change needs a full rebuild; the plan guides but does not perform it.
    requires_reindex: bool,
    /// Human-readable reasons a reindex is required.
    reindex_reasons: Vec<String>,
    /// Metadata-only changes safe to apply live.
    in_place_changes: Vec<String>,
    /// Whether the in-place changes were applied (true only on `apply` for a non-reindex,
    /// non-noop plan).
    applied: bool,
}

#[derive(Serialize)]
struct IndexStatsDto {
    name: String,
    snapshot: u64,
    num_docs: u64,
    generation_count: u64,
    checkpoint: String,
    /// Mapped DATE columns — the console time filter ranges a query on one of these.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    time_fields: Vec<String>,
}

impl From<v1::IndexStats> for IndexStatsDto {
    fn from(s: v1::IndexStats) -> Self {
        IndexStatsDto {
            name: s.name,
            snapshot: s.snapshot,
            num_docs: s.num_docs,
            generation_count: s.generation_count,
            checkpoint: s.checkpoint,
            time_fields: s.time_fields,
        }
    }
}

#[derive(Deserialize)]
struct GetByKeyDto {
    keys: Vec<CoordinatesDto>,
    #[serde(default)]
    columns: Vec<String>,
    /// Target index name. Empty = the endpoint's default index.
    #[serde(default)]
    index: String,
}

impl GetByKeyDto {
    fn into_proto(self) -> Result<GetByKeyRequest, ApiError> {
        let keys = self
            .keys
            .into_iter()
            .map(dto_to_coords)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(GetByKeyRequest {
            keys,
            columns: self.columns,
            window: 0,
            index: self.index,
        })
    }
}

#[derive(Serialize)]
struct GetByKeyRespDto {
    rows: Vec<RowDto>,
    /// Shards that failed to resolve their keys, so some requested rows are missing.
    /// Omitted when complete.
    #[serde(skip_serializing_if = "is_zero_u32")]
    failed_shards: u32,
}

#[derive(Serialize)]
struct RowDto {
    key: CoordinatesDto,
    fields: Map<String, JsonValue>,
}

impl From<v1::GetByKeyResponse> for GetByKeyRespDto {
    fn from(r: v1::GetByKeyResponse) -> Self {
        GetByKeyRespDto {
            failed_shards: r.failed_shards,
            rows: r
                .rows
                .into_iter()
                .map(|row| RowDto {
                    key: coords_to_dto(row.key),
                    fields: row
                        .fields
                        .into_iter()
                        .filter_map(|f| f.value.map(|v| (f.name, value_to_json(v))))
                        .collect(),
                })
                .collect(),
        }
    }
}

// ---- coordinates / value conversions -------------------------------------------

#[derive(Serialize, Deserialize, Default)]
struct CoordinatesDto {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    partition: Vec<FieldDto>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    identifier: Vec<FieldDto>,
}

#[derive(Serialize, Deserialize)]
struct FieldDto {
    name: String,
    value: JsonValue,
}

fn coords_to_dto(c: Option<Coordinates>) -> CoordinatesDto {
    let c = c.unwrap_or_default();
    let map = |fields: Vec<Field>| {
        fields
            .into_iter()
            .filter_map(|f| {
                f.value.map(|v| FieldDto {
                    name: f.name,
                    value: value_to_json(v),
                })
            })
            .collect()
    };
    CoordinatesDto {
        partition: map(c.partition),
        identifier: map(c.identifier),
    }
}

fn dto_to_coords(dto: CoordinatesDto) -> Result<Coordinates, ApiError> {
    let map = |fields: Vec<FieldDto>| {
        fields
            .into_iter()
            .map(|f| {
                json_to_value(&f.value)
                    .map(|value| Field {
                        name: f.name.clone(),
                        value: Some(value),
                    })
                    .ok_or_else(|| {
                        ApiError::bad_request(format!(
                            "field `{}` must be a string/number/bool",
                            f.name
                        ))
                    })
            })
            .collect::<Result<Vec<_>, _>>()
    };
    Ok(Coordinates {
        partition: map(dto.partition)?,
        identifier: map(dto.identifier)?,
    })
}

/// A JSON scalar → a wire [`Value`](v1::Value); non-scalars (null/array/object) → `None`.
fn json_to_value(v: &JsonValue) -> Option<v1::Value> {
    let kind = match v {
        JsonValue::String(s) => Kind::Str(s.clone()),
        JsonValue::Bool(b) => Kind::Bool(*b),
        JsonValue::Number(n) if n.is_i64() => Kind::Int(n.as_i64()?),
        JsonValue::Number(n) if n.is_u64() => Kind::Int(n.as_u64()? as i64),
        JsonValue::Number(n) => Kind::Float(n.as_f64()?),
        _ => return None,
    };
    Some(v1::Value { kind: Some(kind) })
}

/// A wire [`Value`](v1::Value) → a JSON scalar (`null` if the value carried no kind).
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

/// Convert the wire `map<string, HighlightField>` to the REST highlight object: field →
/// fragments → `[{text, marked}]` segment runs. Skips empty (no-highlight) input.
fn highlight_to_json(
    highlight: std::collections::HashMap<String, v1::HighlightField>,
) -> Map<String, JsonValue> {
    highlight
        .into_iter()
        .map(|(field, hf)| {
            let fragments: Vec<JsonValue> = hf
                .fragments
                .into_iter()
                .map(|frag| {
                    let segs: Vec<SegmentDto> = frag
                        .segments
                        .into_iter()
                        .map(|s| SegmentDto {
                            text: s.text,
                            marked: s.marked,
                        })
                        .collect();
                    json!(segs)
                })
                .collect();
            (field, JsonValue::Array(fragments))
        })
        .collect()
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

fn is_zero_u32(n: &u32) -> bool {
    *n == 0
}

fn is_false(b: &bool) -> bool {
    !*b
}

// ---- request metadata + error mapping ------------------------------------------

/// Wrap a proto body in a tonic request, forwarding only the bearer credential so
/// authentication behaves the same over REST as over gRPC. Verified identity is stamped
/// downstream by the [AuthN layer](crate::authn); caller-asserted `x-growlerdb-principal` /
/// `x-growlerdb-tenant` / `x-growlerdb-roles` headers are never propagated, since a
/// control-plane path with no authenticator would otherwise trust a forged role claim.
pub(crate) fn grpc_request<T>(body: T, headers: &HeaderMap) -> Request<T> {
    let mut req = Request::new(body);
    if let Some(val) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        if let Ok(val) = val.parse() {
            req.metadata_mut().insert("authorization", val);
        }
    }
    req
}

/// A JSON error response `{ code, message }` with an HTTP status.
struct ApiError {
    status: StatusCode,
    code: String,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        ApiError {
            status: StatusCode::BAD_REQUEST,
            code: "INVALID_ARGUMENT".to_string(),
            message: message.into(),
        }
    }
}

impl From<Status> for ApiError {
    fn from(s: Status) -> Self {
        let status = match s.code() {
            Code::InvalidArgument => StatusCode::BAD_REQUEST,
            Code::NotFound => StatusCode::NOT_FOUND,
            Code::PermissionDenied => StatusCode::FORBIDDEN,
            Code::Unauthenticated => StatusCode::UNAUTHORIZED,
            Code::FailedPrecondition => StatusCode::PRECONDITION_FAILED,
            Code::Unimplemented => StatusCode::NOT_IMPLEMENTED,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        ApiError {
            status,
            code: format!("{:?}", s.code()),
            message: s.message().to_string(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({ "code": self.code, "message": self.message })),
        )
            .into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{AuthContext, AuthDenied, AuthHook};
    use crate::{AdminService, LookupService, SearchService, SuggestService};
    use axum::body::{to_bytes, Body};
    use axum::http::{Request as HttpRequest, StatusCode};
    use growlerdb_core::{
        CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
        SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
    };
    use growlerdb_index::{LocalIndexStore, Shard, ShardId};
    use growlerdb_source::IcebergConfig;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use tower::ServiceExt; // oneshot

    fn shard(root: &std::path::Path) -> Arc<Shard> {
        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("city", SourceType::String),
                SourceField::new("rank", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let idx = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: city, type: KEYWORD }, { path: rank, type: LONG, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let shard = LocalIndexStore::open(root)
            .unwrap()
            .create_shard(&ShardId::single("docs"), &idx)
            .unwrap();
        let put = |id: &str, city: &str, rank: i64| {
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
            let mut f = BTreeMap::new();
            f.insert("id".to_string(), Value::from(id));
            f.insert("city".to_string(), Value::from(city));
            f.insert("rank".to_string(), Value::Int(rank));
            LocatedDoc {
                doc: Document::new(key, f),
                iceberg_file: "f".into(),
                row_position: 0,
            }
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![put("1", "berlin", 30), put("2", "bern", 10)],
                SourceCheckpoint::iceberg(4),
                "b1",
            ),
        )
        .unwrap();
        Arc::new(shard)
    }

    fn app(shard: Arc<Shard>, auth: crate::SharedAuth) -> Router {
        let node = crate::node::LocalNode::new(
            SearchService::with_auth(shard.clone(), auth.clone()),
            SuggestService::with_auth(shard.clone(), auth.clone()),
            LookupService::with_auth(
                shard.clone(),
                IcebergConfig::local(),
                "g.docs",
                auth.clone(),
            ),
            AdminService::with_auth(shard, "docs", auth),
        );
        router(Arc::new(Gateway::new(node.shared())))
    }

    async fn post(app: &Router, path: &str, body: serde_json::Value) -> (StatusCode, JsonValue) {
        let req = HttpRequest::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
        (status, json)
    }

    #[tokio::test(flavor = "current_thread")]
    async fn search_over_rest_returns_ranked_coordinates() {
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), crate::auth::default_auth());
        let (status, body) = post(
            &app,
            "/v1/search",
            json!({ "query": "rank:[0 TO 100]", "limit": 10, "sort": [{ "field": "rank", "desc": true }] }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        // rank desc → city berlin(30) before bern(10); coordinates carry the id.
        let ids: Vec<&str> = body["hits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|h| h["coordinates"]["identifier"][0]["value"].as_str().unwrap())
            .collect();
        assert_eq!(ids, vec!["1", "2"]);
        assert_eq!(body["total"], 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rest_search_scopes_to_the_served_index() {
        // A REST front over a Gateway that declares it serves `docs` (per-index scoping).
        let tmp = tempfile::tempdir().unwrap();
        let sh = shard(tmp.path());
        let auth = crate::auth::default_auth();
        let node = crate::node::LocalNode::new(
            SearchService::with_auth(sh.clone(), auth.clone()),
            SuggestService::with_auth(sh.clone(), auth.clone()),
            LookupService::with_auth(sh.clone(), IcebergConfig::local(), "g.docs", auth.clone()),
            AdminService::with_auth(sh, "docs", auth),
        );
        let app = router(Arc::new(Gateway::new(node.shared()).serving("docs")));

        // Empty index and the served name both resolve.
        for ix in ["", "docs"] {
            let (status, _) = post(
                &app,
                "/v1/search",
                json!({ "query": "rank:[0 TO 100]", "index": ix }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "index `{ix}` should be served");
        }
        // A different index is a 404 — the request is not silently answered by `docs`.
        let (status, _) = post(
            &app,
            "/v1/search",
            json!({ "query": "rank:[0 TO 100]", "index": "other" }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn config_on_an_open_gateway_is_not_auth_required() {
        // An open gateway advertises auth_required=false (200, no token), so the console
        // runs un-gated. The closed case (auth_required=true) is covered by the gateway unit test.
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), crate::auth::default_auth());
        let req = HttpRequest::builder()
            .uri("/v1/config")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: JsonValue =
            serde_json::from_slice(&to_bytes(resp.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(body["auth_required"], false);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn me_on_an_open_gateway_is_anonymous() {
        // With no authenticator, /v1/me returns the "not signed in" shape (200, not 401).
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), crate::auth::default_auth());
        let req = HttpRequest::builder()
            .uri("/v1/me")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body: JsonValue =
            serde_json::from_slice(&to_bytes(resp.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(body["authenticated"], false);
        assert_eq!(body["subject"], "");
        assert_eq!(body["roles"].as_array().unwrap().len(), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn explain_returns_a_real_bm25_tree_for_a_hit() {
        // Explain a specific document's score for a query.
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), crate::auth::default_auth());
        let coord = json!({ "identifier": [{ "name": "id", "value": "1" }] });

        let (status, body) = post(
            &app,
            "/v1/explain",
            json!({ "query": "city:berlin", "coordinates": coord }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["found"], true);
        assert_eq!(body["matched"], true);
        assert!(body["score"].as_f64().unwrap() > 0.0);
        // Real explanation tree (Tantivy), not a fabricated term list.
        assert!(body["detail"]["description"].as_str().is_some());
        // Analyzed terms include the queried field.
        assert!(body["analyzed"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a["field"] == "city"));
        // Per-stage timings + shard counts.
        assert_eq!(body["shards_total"], 1);
        assert_eq!(body["shards_scanned"], 1);
        assert!(body["timings"]["index_ms"].as_f64().is_some());

        // The same doc, a non-matching query → found but not matched (score 0, no tree).
        let (_, body2) = post(
            &app,
            "/v1/explain",
            json!({ "query": "city:paris", "coordinates": coord }),
        )
        .await;
        assert_eq!(body2["found"], true);
        assert_eq!(body2["matched"], false);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn facets_reuse_aggregate_and_skip_non_fast_fields() {
        // The facet rail reuses the Aggregate path; each field is faceted independently
        // so a non-aggregatable field is skipped, never a 500.
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), crate::auth::default_auth());
        let (status, body) = post(
            &app,
            "/v1/facets",
            json!({ "query": "rank:[0 TO 100]", "fields": ["rank", "city"] }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let groups = body["facets"].as_array().unwrap();
        // `city` is KEYWORD-but-not-fast → terms errors → the field is skipped, not fatal.
        assert!(groups.iter().all(|g| g["field"] != "city"));
        // `rank` is a fast field → a terms facet with a bucket per distinct value (30, 10).
        let rank = groups
            .iter()
            .find(|g| g["field"] == "rank")
            .expect("rank facet present");
        assert_eq!(rank["buckets"].as_array().unwrap().len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn suggest_and_describe_over_rest() {
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), crate::auth::default_auth());

        let (status, body) = post(
            &app,
            "/v1/suggest",
            json!({ "field": "city", "text": "ber", "limit": 10 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let terms: Vec<&str> = body["suggestions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["text"].as_str().unwrap())
            .collect();
        assert_eq!(terms, vec!["berlin", "bern"]);

        let (status, body) = post(&app, "/v1/index:describe", json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["name"], "docs");
        assert_eq!(body["num_docs"], 2);
        assert_eq!(body["checkpoint"], "iceberg_snapshot:4");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn errors_map_to_http_status() {
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), crate::auth::default_auth());

        // Empty suggest text → 400 with the structured body.
        let (status, body) =
            post(&app, "/v1/suggest", json!({ "field": "city", "text": "" })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["code"], "InvalidArgument");

        // Describing another index → 404.
        let (status, _) = post(&app, "/v1/index:describe", json!({ "index": "nope" })).await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // GetByKey for an unindexed key → 404 (resolved before any Iceberg connect).
        let (status, _) = post(
            &app,
            "/v1/keys:get",
            json!({ "keys": [{ "identifier": [{ "name": "id", "value": "missing" }] }] }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn reindex_over_rest_is_wired_to_the_node() {
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), crate::auth::default_auth());

        // The served index reaches the Node's reindex, which (no source access in this harness)
        // returns Unimplemented → 501. That it's 501-with-a-structured-body, not a bare 404, proves
        // the route is mounted and dispatches to Admin.ReindexIndex (vs an unmatched path).
        let (status, body) = post(&app, "/v1/index:reindex", json!({})).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(body["code"], "Unimplemented");

        // Reindexing an index this node doesn't serve → 404 (name check precedes the source guard).
        let (status, body) = post(&app, "/v1/index:reindex", json!({ "index": "nope" })).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "NotFound");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn alter_over_rest_is_wired_to_the_node() {
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), crate::auth::default_auth());

        // The served index reaches the Node's alter, which (no source access in this harness)
        // returns Unimplemented → 501 — proving the route is mounted and dispatches to
        // Admin.AlterIndex (vs an unmatched 404).
        let (status, body) = post(&app, "/v1/index:alter", json!({ "definition_yaml": "" })).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(body["code"], "Unimplemented");

        // Altering an index this node doesn't serve → 404 (name check precedes the source guard).
        let (status, body) = post(&app, "/v1/index:alter", json!({ "index": "nope" })).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "NotFound");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn open_gateway_strips_caller_asserted_tenant_before_the_hook() {
        // On an open gateway (no authenticator) a caller-asserted `x-growlerdb-tenant`
        // is stripped before it can reach the auth seam or tenant scoping — so a forged tenant can't
        // be trusted. A hook that would block that tenant never sees it. (In closed mode the *verified*
        // tenant is stamped by the authenticator and does reach the hook.)
        struct DenyTenant(&'static str);
        impl AuthHook for DenyTenant {
            fn authorize(&self, ctx: &AuthContext) -> Result<(), AuthDenied> {
                match &ctx.tenant {
                    Some(t) if t == self.0 => Err(AuthDenied::new("blocked")),
                    _ => Ok(()),
                }
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let app = app(shard(tmp.path()), Arc::new(DenyTenant("blocked")));

        // No tenant header → allowed.
        let (status, _) = post(
            &app,
            "/v1/suggest",
            json!({ "field": "city", "text": "ber" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);

        // A forged "blocked" tenant header is stripped on the open gateway → the hook never sees it,
        // so the request is allowed.
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/suggest")
            .header("content-type", "application/json")
            .header("x-growlerdb-tenant", "blocked")
            .body(Body::from(
                json!({ "field": "city", "text": "ber" }).to_string(),
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// The Engine serves the built UI SPA: real assets directly, `index.html` as the
    /// SPA fallback for client routes, and the `/v1` API still wins for its own paths.
    #[tokio::test]
    async fn ui_spa_served_with_v1_api_precedence() {
        let data = tempfile::tempdir().unwrap();
        let ui = tempfile::tempdir().unwrap();
        std::fs::write(
            ui.path().join("index.html"),
            "<!doctype html><title>GrowlerDB</title>",
        )
        .unwrap();
        std::fs::create_dir(ui.path().join("assets")).unwrap();
        std::fs::write(ui.path().join("assets/app.js"), "console.log('hi')").unwrap();

        let sh = shard(data.path());
        let node = crate::node::LocalNode::new(
            SearchService::new(sh.clone()),
            SuggestService::new(sh.clone()),
            LookupService::new(sh.clone(), IcebergConfig::local(), "g.docs"),
            AdminService::new(sh, "docs"),
        );
        let app = router_with_ui(Arc::new(Gateway::new(node.shared())), ui.path());

        let get = |uri: &str| HttpRequest::builder().uri(uri).body(Body::empty()).unwrap();
        let body = |resp: axum::response::Response| async {
            String::from_utf8(to_bytes(resp.into_body(), 1 << 20).await.unwrap().to_vec()).unwrap()
        };

        // `/` → index.html.
        let resp = app.clone().oneshot(get("/")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(body(resp).await.contains("GrowlerDB"));

        // A real asset is served directly.
        let resp = app.clone().oneshot(get("/assets/app.js")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(body(resp).await.contains("console.log"));

        // A client route with no matching file falls back to index.html (SPA routing).
        let resp = app.clone().oneshot(get("/indexes")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(body(resp).await.contains("GrowlerDB"));

        // The `/v1` API still wins for its own paths (not swallowed by the SPA fallback).
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/search")
            .header("content-type", "application/json")
            .body(Body::from(
                json!({ "query": "city:berlin", "limit": 10 }).to_string(),
            ))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(body(resp).await.contains("\"total\""));
    }

    #[test]
    fn rest_search_defaults_the_page_size() {
        // An omitted limit gets a bounded page, not the whole result set.
        let dto: SearchDto = serde_json::from_str(r#"{"query":"x"}"#).unwrap();
        assert_eq!(dto.into_proto().limit, DEFAULT_PAGE_SIZE);
        // An explicit limit is honored.
        let dto: SearchDto = serde_json::from_str(r#"{"query":"x","limit":50}"#).unwrap();
        assert_eq!(dto.into_proto().limit, 50);
        // Explicit 0 is indistinguishable from omitted over JSON, so it's bounded too (full scans
        // use the scroll/export path, not an unbounded page).
        let dto: SearchDto = serde_json::from_str(r#"{"query":"x","limit":0}"#).unwrap();
        assert_eq!(dto.into_proto().limit, DEFAULT_PAGE_SIZE);
    }

    #[test]
    fn rest_surfaces_partial_and_failed_shards_only_when_incomplete() {
        let search = |partial| {
            serde_json::to_value(SearchRespDto::from(v1::SearchResponse {
                hits: vec![],
                total: 0,
                next_cursor: vec![],
                partial,
                ..Default::default()
            }))
            .unwrap()
        };
        assert_eq!(search(true)["partial"], serde_json::json!(true));
        assert!(
            search(false).get("partial").is_none(),
            "a complete search omits `partial` so a missing flag is trustworthy"
        );

        let suggest = |failed_shards| {
            serde_json::to_value(SuggestRespDto::from(v1::SuggestResponse {
                suggestions: vec![],
                failed_shards,
            }))
            .unwrap()
        };
        assert_eq!(suggest(2)["failed_shards"], serde_json::json!(2));
        assert!(suggest(0).get("failed_shards").is_none());

        let get_by_key = |failed_shards| {
            serde_json::to_value(GetByKeyRespDto::from(v1::GetByKeyResponse {
                rows: vec![],
                failed_shards,
            }))
            .unwrap()
        };
        assert_eq!(get_by_key(1)["failed_shards"], serde_json::json!(1));
        assert!(get_by_key(0).get("failed_shards").is_none());
    }

    #[test]
    fn rest_search_surfaces_cached_display_fields() {
        let hit = |fields: Vec<v1::Field>| v1::SearchHit {
            coordinates: None,
            score: 1.0,
            group: None,
            group_count: 0,
            sort_values: vec![],
            fields,
            highlight: Default::default(),
        };
        let resp = |hits| {
            serde_json::to_value(SearchRespDto::from(v1::SearchResponse {
                hits,
                total: 1,
                next_cursor: vec![],
                partial: false,
                ..Default::default()
            }))
            .unwrap()
        };

        // A cached display field renders inline on the hit — no hydration needed.
        let json = resp(vec![hit(vec![v1::Field {
            name: "city".into(),
            value: Some(v1::Value {
                kind: Some(Kind::Str("berlin".into())),
            }),
        }])]);
        assert_eq!(
            json["hits"][0]["fields"]["city"],
            serde_json::json!("berlin")
        );

        // A hit with no cached fields omits the `fields` object entirely.
        assert!(resp(vec![hit(vec![])])["hits"][0].get("fields").is_none());
    }

    #[test]
    fn rest_search_selects_the_query_syntax() {
        use growlerdb_proto::v1::QuerySyntax;
        let syntax = |body: &str| {
            serde_json::from_str::<SearchDto>(body)
                .unwrap()
                .into_proto()
                .syntax
        };
        // Default and unknown values are Lucene; `kql` (any case) selects KQL.
        assert_eq!(syntax(r#"{"query":"x"}"#), QuerySyntax::Lucene as i32);
        assert_eq!(
            syntax(r#"{"query":"x","syntax":"lucene"}"#),
            QuerySyntax::Lucene as i32
        );
        assert_eq!(
            syntax(r#"{"query":"x","syntax":"kql"}"#),
            QuerySyntax::Kql as i32
        );
        assert_eq!(
            syntax(r#"{"query":"x","syntax":"KQL"}"#),
            QuerySyntax::Kql as i32
        );
    }
}
