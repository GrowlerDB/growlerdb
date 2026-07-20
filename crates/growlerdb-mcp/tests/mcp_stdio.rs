//! End-to-end tests: an in-process axum **mock gateway** serves canned JSON, and the MCP server is
//! driven over its stdio JSON-RPC via in-memory pipes. No network beyond `127.0.0.1` loopback.

use std::net::SocketAddr;

use axum::{
    extract::Json,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use growlerdb_mcp::{serve_io, GatewayClient, McpConfig, McpError};

const TOKEN: &str = "test-token";

/// Reject any request that doesn't forward `Authorization: Bearer test-token`.
fn require_bearer(headers: &HeaderMap) -> Result<(), (StatusCode, Json<Value>)> {
    let ok = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|v| v == format!("Bearer {TOKEN}"))
        .unwrap_or(false);
    if ok {
        Ok(())
    } else {
        Err((
            StatusCode::UNAUTHORIZED,
            Json(json!({ "code": "Unauthenticated", "message": "missing bearer" })),
        ))
    }
}

async fn search_handler(headers: HeaderMap, Json(body): Json<Value>) -> impl IntoResponse {
    if let Err(e) = require_bearer(&headers) {
        return e.into_response();
    }
    // Echo the index + hydrate opt-in back so tests can assert routing/forwarding.
    let index = body.get("index").cloned().unwrap_or(Value::Null);
    let hydrate = body.get("hydrate").cloned().unwrap_or(Value::Null);
    let hydrate_columns = body.get("hydrate_columns").cloned().unwrap_or(Value::Null);
    Json(json!({
        "hits": [{
            "coordinates": {
                "partition": [{ "name": "day", "value": "2026-07-18" }],
                "identifier": [{ "name": "id", "value": "doc-1" }]
            },
            "score": 1.5,
            "fields": { "title": "hello" }
        }],
        "total": 1,
        "shards_scanned": 1,
        "shards_total": 1,
        "_echo_index": index,
        "_echo_hydrate": hydrate,
        "_echo_hydrate_columns": hydrate_columns
    }))
    .into_response()
}

async fn semantic_handler(headers: HeaderMap, Json(_): Json<Value>) -> impl IntoResponse {
    if let Err(e) = require_bearer(&headers) {
        return e.into_response();
    }
    Json(json!({ "hits": [], "total": 0, "_endpoint": "semantic" })).into_response()
}

async fn keys_get_handler(headers: HeaderMap, Json(body): Json<Value>) -> impl IntoResponse {
    if let Err(e) = require_bearer(&headers) {
        return e.into_response();
    }
    // Round-trip the requested keys back as hydrated rows.
    let keys = body.get("keys").cloned().unwrap_or(json!([]));
    let key = keys.get(0).cloned().unwrap_or(Value::Null);
    Json(json!({
        "rows": [{ "key": key, "fields": { "title": "hello", "body": "authoritative" } }]
    }))
    .into_response()
}

async fn facets_handler(headers: HeaderMap, Json(_): Json<Value>) -> impl IntoResponse {
    if let Err(e) = require_bearer(&headers) {
        return e.into_response();
    }
    Json(json!({ "facets": [{ "field": "category", "buckets": [{ "value": "a", "count": 3 }] }] }))
        .into_response()
}

async fn describe_handler(headers: HeaderMap, Json(body): Json<Value>) -> impl IntoResponse {
    if let Err(e) = require_bearer(&headers) {
        return e.into_response();
    }
    // A 4xx path: describing `missing` returns a gateway error body.
    if body.get("index").and_then(Value::as_str) == Some("missing") {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "code": "NotFound", "message": "no such index" })),
        )
            .into_response();
    }
    Json(json!({ "name": "docs", "num_docs": 42, "snapshot": 7 })).into_response()
}

async fn indexes_handler(headers: HeaderMap) -> impl IntoResponse {
    if let Err(e) = require_bearer(&headers) {
        return e.into_response();
    }
    Json(json!({ "indexes": [{ "name": "docs", "status": "READY" }] })).into_response()
}

async fn login_handler(Json(body): Json<Value>) -> impl IntoResponse {
    let user = body.get("username").and_then(Value::as_str).unwrap_or("");
    let pass = body.get("password").and_then(Value::as_str).unwrap_or("");
    if user == "demo" && pass == "demo" {
        Json(json!({ "token": TOKEN, "expires_at_ms": 0, "roles": ["reader"] })).into_response()
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "code": "Unauthenticated", "message": "bad credentials" })),
        )
            .into_response()
    }
}

/// Spin the mock gateway on an ephemeral loopback port; return its base origin (`http://127.0.0.1:N`).
async fn spawn_mock_gateway() -> String {
    let app = Router::new()
        .route("/v1/search", post(search_handler))
        .route("/v1/search:semantic", post(semantic_handler))
        .route("/v1/search:hybrid", post(semantic_handler))
        .route("/v1/keys:get", post(keys_get_handler))
        .route("/v1/facets", post(facets_handler))
        .route("/v1/index:describe", post(describe_handler))
        .route("/v1/indexes", get(indexes_handler))
        .route("/v1/login", post(login_handler));

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Drive the MCP server over in-memory pipes: send each `request` line, then read the responses
/// (notifications produce none). Returns the parsed JSON-RPC responses in order.
async fn drive(config: McpConfig, requests: Vec<Value>) -> Vec<Value> {
    // to_server: test writes, server reads. from_server: server writes, test reads.
    let (mut client_w, server_r) = tokio::io::duplex(64 * 1024);
    let (server_w, client_r) = tokio::io::duplex(64 * 1024);

    let server = tokio::spawn(async move { serve_io(config, server_r, server_w).await });

    // Feed all requests, then close the write half so the server sees EOF and exits.
    let writer = tokio::spawn(async move {
        for req in requests {
            let mut line = serde_json::to_vec(&req).unwrap();
            line.push(b'\n');
            client_w.write_all(&line).await.unwrap();
        }
        client_w.flush().await.unwrap();
        drop(client_w);
    });

    let mut responses = Vec::new();
    let mut lines = BufReader::new(client_r).lines();
    while let Some(line) = lines.next_line().await.unwrap() {
        if line.trim().is_empty() {
            continue;
        }
        responses.push(serde_json::from_str::<Value>(&line).unwrap());
    }

    writer.await.unwrap();
    server.await.unwrap().unwrap();
    responses
}

fn config(base: &str) -> McpConfig {
    McpConfig {
        gateway_url: base.to_string(),
        token: Some(TOKEN.to_string()),
        default_index: Some("docs".to_string()),
    }
}

#[tokio::test]
async fn initialize_handshake_and_tools_list() {
    let base = spawn_mock_gateway().await;
    let responses = drive(
        config(&base),
        vec![
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": { "protocolVersion": "2025-06-18" } }),
            json!({ "jsonrpc": "2.0", "method": "notifications/initialized" }),
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list" }),
            json!({ "jsonrpc": "2.0", "id": 3, "method": "ping" }),
        ],
    )
    .await;

    // The notification produced no reply → 3 responses for 3 requests-with-id.
    assert_eq!(responses.len(), 3);

    let init = &responses[0];
    assert_eq!(init["id"], 1);
    // Echoes the client's requested protocol version.
    assert_eq!(init["result"]["protocolVersion"], "2025-06-18");
    assert_eq!(init["result"]["serverInfo"]["name"], "growlerdb");
    assert!(init["result"]["capabilities"]["tools"].is_object());

    let list = &responses[1];
    let tools = list["result"]["tools"].as_array().unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
    assert_eq!(
        names,
        vec![
            "search",
            "hydrate",
            "aggregate",
            "list_indexes",
            "describe_index"
        ]
    );
    // Every tool advertises an inputSchema.
    assert!(tools.iter().all(|t| t["inputSchema"].is_object()));

    assert_eq!(responses[2]["result"], json!({}));
}

#[tokio::test]
async fn default_protocol_version_when_client_omits_one() {
    let base = spawn_mock_gateway().await;
    let responses = drive(
        config(&base),
        vec![json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {} })],
    )
    .await;
    assert_eq!(responses[0]["result"]["protocolVersion"], "2024-11-05");
}

#[tokio::test]
async fn search_tool_hits_gateway_and_returns_coordinates() {
    let base = spawn_mock_gateway().await;
    let responses = drive(
        config(&base),
        vec![json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "search", "arguments": { "query": "hello", "index": "docs" } }
        })],
    )
    .await;

    let result = &responses[0]["result"];
    assert_eq!(result["isError"], false);
    let text = result["content"][0]["text"].as_str().unwrap();
    let payload: Value = serde_json::from_str(text).unwrap();
    // Coordinates flowed through, and the index routed correctly.
    assert_eq!(
        payload["hits"][0]["coordinates"]["identifier"][0]["value"],
        "doc-1"
    );
    assert_eq!(payload["_echo_index"], "docs");
}

/// The search tool's `hydrate` opt-in rides through to the engine (which does the governed
/// one-call search→hydrate); by default it is forwarded as false.
#[tokio::test]
async fn search_tool_forwards_the_hydrate_opt_in() {
    let base = spawn_mock_gateway().await;
    let responses = drive(
        config(&base),
        vec![
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": { "name": "search", "arguments": {
                        "query": "hello", "hydrate": true, "hydrate_columns": ["body"] } } }),
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": { "name": "search", "arguments": { "query": "hello" } } }),
        ],
    )
    .await;

    let opted_in: Value = serde_json::from_str(
        responses[0]["result"]["content"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(opted_in["_echo_hydrate"], true);
    assert_eq!(opted_in["_echo_hydrate_columns"], json!(["body"]));

    let default: Value = serde_json::from_str(
        responses[1]["result"]["content"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(default["_echo_hydrate"], false);
}

#[tokio::test]
async fn search_then_hydrate_round_trips_coordinates() {
    let base = spawn_mock_gateway().await;
    let responses = drive(
        config(&base),
        vec![
            json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": { "name": "search", "arguments": { "query": "hello" } } }),
            json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/call",
                    "params": { "name": "hydrate", "arguments": {
                        "coordinates": [{
                            "partition": [{ "name": "day", "value": "2026-07-18" }],
                            "identifier": [{ "name": "id", "value": "doc-1" }]
                        }]
                    } } }),
        ],
    )
    .await;

    let hydrate = &responses[1]["result"];
    assert_eq!(hydrate["isError"], false);
    let payload: Value =
        serde_json::from_str(hydrate["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(payload["rows"][0]["key"]["identifier"][0]["value"], "doc-1");
    assert_eq!(payload["rows"][0]["fields"]["body"], "authoritative");
}

#[tokio::test]
async fn unknown_method_returns_minus_32601() {
    let base = spawn_mock_gateway().await;
    let responses = drive(
        config(&base),
        vec![json!({ "jsonrpc": "2.0", "id": 9, "method": "no/such/method" })],
    )
    .await;
    assert_eq!(responses[0]["id"], 9);
    assert_eq!(responses[0]["error"]["code"], -32601);
}

#[tokio::test]
async fn malformed_json_returns_minus_32700() {
    let base = spawn_mock_gateway().await;
    // Feed a raw non-JSON line directly (bypassing `drive`'s serializer).
    let (mut client_w, server_r) = tokio::io::duplex(4096);
    let (server_w, client_r) = tokio::io::duplex(4096);
    let server = tokio::spawn(async move { serve_io(config(&base), server_r, server_w).await });
    client_w.write_all(b"this is not json\n").await.unwrap();
    drop(client_w);

    let mut lines = BufReader::new(client_r).lines();
    let line = lines.next_line().await.unwrap().unwrap();
    server.await.unwrap().unwrap();
    let resp: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(resp["error"]["code"], -32700);
    assert!(resp["id"].is_null());
}

#[tokio::test]
async fn gateway_4xx_surfaces_as_tool_error() {
    let base = spawn_mock_gateway().await;
    let responses = drive(
        config(&base),
        vec![json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "describe_index", "arguments": { "index": "missing" } }
        })],
    )
    .await;

    let result = &responses[0]["result"];
    assert_eq!(result["isError"], true);
    let text = result["content"][0]["text"].as_str().unwrap();
    assert!(
        text.contains("NotFound"),
        "expected gateway error text, got: {text}"
    );
    assert!(
        text.contains("404"),
        "expected status in error text, got: {text}"
    );
}

#[tokio::test]
async fn missing_index_without_default_is_a_tool_error() {
    let base = spawn_mock_gateway().await;
    let mut cfg = config(&base);
    cfg.default_index = None; // no fallback
    let responses = drive(
        cfg,
        vec![json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "describe_index", "arguments": {} }
        })],
    )
    .await;
    assert_eq!(responses[0]["result"]["isError"], true);
}

#[tokio::test]
async fn list_indexes_tool_forwards_and_returns() {
    let base = spawn_mock_gateway().await;
    let responses = drive(
        config(&base),
        vec![json!({ "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                     "params": { "name": "list_indexes", "arguments": {} } })],
    )
    .await;
    let payload: Value = serde_json::from_str(
        responses[0]["result"]["content"][0]["text"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(payload["indexes"][0]["name"], "docs");
}

// ---- GatewayClient unit tests against the mock -------------------------------

#[tokio::test]
async fn client_forwards_bearer_and_parses_ok() {
    let base = spawn_mock_gateway().await;
    let client = GatewayClient::new(base, Some(TOKEN.to_string()));
    let resp = client
        .search(json!({ "query": "x", "index": "docs" }))
        .await
        .unwrap();
    assert_eq!(resp["total"], 1);
}

#[tokio::test]
async fn client_without_token_is_rejected_as_gateway_error() {
    let base = spawn_mock_gateway().await;
    let client = GatewayClient::new(base, None); // forwards no bearer
    let err = client
        .search(json!({ "query": "x", "index": "docs" }))
        .await
        .unwrap_err();
    match err {
        McpError::Gateway { status, code, .. } => {
            assert_eq!(status, 401);
            assert_eq!(code, "Unauthenticated");
        }
        other => panic!("expected a gateway error, got {other:?}"),
    }
}

#[tokio::test]
async fn client_login_returns_token() {
    let base = spawn_mock_gateway().await;
    let client = GatewayClient::new(base, None);
    let token = client.login("demo", "demo").await.unwrap();
    assert_eq!(token, TOKEN);
}

/// The `aggregate` tool end to end: a tools/call reaches the gateway's `/v1/facets` and the
/// bucket payload flows back as the tool result (previously only name-asserted in tools/list).
#[tokio::test]
async fn aggregate_tool_returns_facet_buckets() {
    let base = spawn_mock_gateway().await;
    let responses = drive(
        config(&base),
        vec![json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "aggregate",
                        "arguments": { "query": "*", "index": "docs",
                                       "fields": ["category"], "size": 5 } }
        })],
    )
    .await;

    let result = &responses[0]["result"];
    assert_eq!(result["isError"], false);
    let text = result["content"][0]["text"].as_str().unwrap();
    let payload: Value = serde_json::from_str(text).unwrap();
    assert_eq!(payload["facets"][0]["field"], "category");
    assert_eq!(payload["facets"][0]["buckets"][0]["count"], 3);
}
