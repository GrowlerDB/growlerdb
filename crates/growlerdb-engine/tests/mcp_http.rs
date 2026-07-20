//! The MCP **Streamable HTTP transport** end to end: `POST /mcp` on the gateway's REST surface →
//! JSON-RPC dispatch → tool calls re-entering the real `/v1` router in-process. Covers the
//! protocol shape (sessionless, POST-only, no batching, 202 notifications), the `Origin`
//! DNS-rebinding gate, and auth: a closed gateway 401s a missing/invalid bearer up front and the
//! forwarded bearer is what the `/v1` surface authenticates — the transport synthesizes nothing.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{header, Request as HttpRequest, StatusCode};
use axum::Router;
use growlerdb_engine::{
    mcp_router, mint_session_jwt, rest, Gateway, JwtAuthenticator, Node, BUILTIN_SESSION_AUDIENCE,
    BUILTIN_SESSION_ISSUER, BUILTIN_SESSION_TTL_SECS,
};
use growlerdb_proto::v1::{
    value::Kind, Coordinates, DescribeIndexRequest, DescribeIndexResponse, Field, GetByKeyRequest,
    GetByKeyResponse, HydratedRow, SearchHit, SearchRequest, SearchResponse, SuggestRequest,
    SuggestResponse, Value,
};
use serde_json::{json, Value as JsonValue};
use tonic::{Request, Response, Status};
use tower::ServiceExt;

const SECRET: &[u8] = b"mcp-http-test-secret";

fn coords(id: &str) -> Coordinates {
    Coordinates {
        partition: vec![],
        identifier: vec![Field {
            name: "id".into(),
            value: Some(Value {
                kind: Some(Kind::Str(id.into())),
            }),
        }],
    }
}

/// One fixed hit (`doc-1`) that hydrates to one fixed authoritative row.
struct FakeNode;

#[tonic::async_trait]
impl Node for FakeNode {
    async fn search(
        &self,
        _req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Ok(Response::new(SearchResponse {
            hits: vec![SearchHit {
                coordinates: Some(coords("doc-1")),
                score: 1.5,
                ..Default::default()
            }],
            total: 1,
            ..Default::default()
        }))
    }

    async fn get_by_key(
        &self,
        req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        let rows = req
            .into_inner()
            .keys
            .into_iter()
            .map(|key| HydratedRow {
                key: Some(key),
                fields: vec![Field {
                    name: "body".into(),
                    value: Some(Value {
                        kind: Some(Kind::Str("authoritative".into())),
                    }),
                }],
            })
            .collect();
        Ok(Response::new(GetByKeyResponse {
            rows,
            failed_shards: 0,
        }))
    }

    async fn suggest(
        &self,
        _req: Request<SuggestRequest>,
    ) -> Result<Response<SuggestResponse>, Status> {
        Err(Status::unimplemented("suggest"))
    }

    async fn describe_index(
        &self,
        _req: Request<DescribeIndexRequest>,
    ) -> Result<Response<DescribeIndexResponse>, Status> {
        Err(Status::unimplemented("describe_index"))
    }
}

/// The composed app: `/v1` + `/mcp` mounted over it, exactly like the CLI fronts do.
fn app(gw: Arc<Gateway>) -> Router {
    let v1 = rest::router(gw.clone());
    v1.clone().merge(mcp_router(v1, gw))
}

fn open_app() -> Router {
    app(Arc::new(Gateway::new(Arc::new(FakeNode))))
}

/// A closed gateway (authenticator configured) + a valid signed bearer for it.
fn closed_app() -> (Router, String) {
    let authn = Arc::new(JwtAuthenticator::from_hs256_secret(
        SECRET,
        BUILTIN_SESSION_ISSUER,
        BUILTIN_SESSION_AUDIENCE,
    ));
    let gw = Arc::new(Gateway::new(Arc::new(FakeNode)).with_authn(authn));
    let jwt = mint_session_jwt(
        SECRET,
        "agent",
        &["admin".to_string()],
        &[],
        BUILTIN_SESSION_ISSUER,
        BUILTIN_SESSION_AUDIENCE,
        BUILTIN_SESSION_TTL_SECS,
        None,
    )
    .unwrap();
    (app(gw), format!("Bearer {jwt}"))
}

/// POST a JSON body to `/mcp` with optional extra headers; return status + headers + JSON body.
async fn post_mcp(
    app: &Router,
    body: &JsonValue,
    extra: &[(&str, &str)],
) -> (StatusCode, axum::http::HeaderMap, JsonValue) {
    let mut builder = HttpRequest::builder()
        .method("POST")
        .uri("/mcp")
        .header(header::CONTENT_TYPE, "application/json");
    for (k, v) in extra {
        builder = builder.header(*k, *v);
    }
    let resp = app
        .clone()
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let headers = resp.headers().clone();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let json = serde_json::from_slice(&bytes).unwrap_or(JsonValue::Null);
    (status, headers, json)
}

fn initialize_msg() -> JsonValue {
    json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": { "protocolVersion": "2025-06-18" } })
}

fn search_call(arguments: JsonValue) -> JsonValue {
    json!({ "jsonrpc": "2.0", "id": 7, "method": "tools/call",
            "params": { "name": "search", "arguments": arguments } })
}

/// Parse a tools/call response's content text as JSON.
fn tool_payload(resp: &JsonValue) -> JsonValue {
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn initialize_is_sessionless_and_echoes_the_protocol_version() {
    let app = open_app();
    let (status, headers, body) = post_mcp(&app, &initialize_msg(), &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["protocolVersion"], "2025-06-18");
    assert!(body["result"]["capabilities"]["tools"].is_object());
    // Sessionless: the server issues no session id — every request stands alone.
    assert!(headers.get("mcp-session-id").is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tools_flow_through_the_real_v1_surface() {
    let app = open_app();
    let (_, _, list) = post_mcp(
        &app,
        &json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
        &[],
    )
    .await;
    let tools: Vec<&str> = list["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        tools,
        vec![
            "search",
            "hydrate",
            "aggregate",
            "list_indexes",
            "describe_index",
            "more_like_this"
        ]
    );

    // A search with inline hydration: one tool call → coordinates + the authoritative row,
    // through the real REST DTOs and the real Gateway (admission, hydration merge and all).
    let (status, _, resp) = post_mcp(
        &app,
        &search_call(json!({ "query": "*:*", "hydrate": true })),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["result"]["isError"], false);
    let payload = tool_payload(&resp);
    assert_eq!(
        payload["hits"][0]["coordinates"]["identifier"][0]["value"],
        "doc-1"
    );
    assert_eq!(payload["hits"][0]["row"]["body"], "authoritative");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn notifications_get_202_batches_and_garbage_get_400_get_gets_405() {
    let app = open_app();

    // A notification (no id) produces no JSON-RPC response: 202, empty body.
    let (status, _, body) = post_mcp(
        &app,
        &json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert!(body.is_null());

    // JSON-RPC batching was removed in spec 2025-06-18.
    let (status, _, body) = post_mcp(&app, &json!([initialize_msg()]), &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["error"]["code"], -32600);

    // Unparseable body → 400 with a JSON-RPC parse error.
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/mcp")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from("{not json"))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    // No server-initiated stream: GET answers 405 (axum stamps the Allow header).
    let req = HttpRequest::builder()
        .method("GET")
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn origin_gate_blocks_rebinding_but_passes_loopback_and_same_host() {
    let app = open_app();

    // A foreign browser origin is refused before any protocol work.
    let (status, _, _) = post_mcp(
        &app,
        &initialize_msg(),
        &[("origin", "https://evil.example")],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Loopback origins pass (the local-agent + console path).
    let (status, _, _) = post_mcp(
        &app,
        &initialize_msg(),
        &[("origin", "http://localhost:5173")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Same-origin passes: the Origin's authority matches the request's Host.
    let (status, _, _) = post_mcp(
        &app,
        &initialize_msg(),
        &[
            ("origin", "https://search.example.com"),
            ("host", "search.example.com"),
        ],
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // No Origin (curl, SDKs, non-browser MCP clients) passes.
    let (status, _, _) = post_mcp(&app, &initialize_msg(), &[]).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn closed_gateway_401s_up_front_and_authenticates_the_forwarded_bearer() {
    let (app, bearer) = closed_app();

    // Missing bearer → 401 + the MCP auth signal, before any JSON-RPC processing.
    let (status, headers, _) = post_mcp(&app, &initialize_msg(), &[]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        headers.get(header::WWW_AUTHENTICATE).unwrap(),
        "Bearer",
        "401 carries WWW-Authenticate: Bearer"
    );

    // Garbage bearer → 401 too.
    let (status, _, _) = post_mcp(
        &app,
        &initialize_msg(),
        &[("authorization", "Bearer not-a-token")],
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // A valid signed bearer initializes AND its tool calls execute — the same forwarded
    // header is what the /v1 surface verifies (the transport adds no identity of its own).
    let (status, _, _) = post_mcp(&app, &initialize_msg(), &[("authorization", &bearer)]).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, resp) = post_mcp(
        &app,
        &search_call(json!({ "query": "*:*" })),
        &[("authorization", &bearer)],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(resp["result"]["isError"], false);
    let payload = tool_payload(&resp);
    assert_eq!(
        payload["hits"][0]["coordinates"]["identifier"][0]["value"],
        "doc-1"
    );
}
