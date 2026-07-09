//! The **OpenSearch adapter** end-to-end over HTTP (task-50): drive `POST /<index>/_search` through
//! the real `opensearch_router` → `Gateway` → a capturing fake Node, and assert (a) the DSL was
//! translated to the native query the engine received, and (b) results are shaped as OpenSearch
//! documents — `_id` from the composite key, `_source` from hydration.

use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::{Request as HttpRequest, StatusCode};
use growlerdb_engine::{opensearch_router, Gateway, Node};
use growlerdb_proto::v1::{
    value::Kind, Coordinates, DescribeIndexRequest, DescribeIndexResponse, Field, GetByKeyRequest,
    GetByKeyResponse, HydratedRow, SearchHit, SearchRequest, SearchResponse, SuggestRequest,
    SuggestResponse, Value,
};
use serde_json::{json, Value as JsonValue};
use tonic::{Request, Response, Status};
use tower::ServiceExt;

fn str_val(s: &str) -> Value {
    Value {
        kind: Some(Kind::Str(s.into())),
    }
}
fn int_val(i: i64) -> Value {
    Value {
        kind: Some(Kind::Int(i)),
    }
}

/// A Node that records the query string it was asked to run, returns one fixed hit, and hydrates
/// that hit to a fixed row.
struct CaptureNode {
    seen_query: Arc<Mutex<String>>,
}

#[tonic::async_trait]
impl Node for CaptureNode {
    async fn search(
        &self,
        req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        *self.seen_query.lock().unwrap() = req.into_inner().query;
        Ok(Response::new(SearchResponse {
            hits: vec![SearchHit {
                coordinates: Some(Coordinates {
                    partition: vec![Field {
                        name: "tenant".into(),
                        value: Some(int_val(42)),
                    }],
                    identifier: vec![Field {
                        name: "id".into(),
                        value: Some(str_val("u1")),
                    }],
                }),
                score: 1.5,
                group: None,
                group_count: 0,
                sort_values: vec![],
                fields: vec![],
            }],
            total: 1,
            next_cursor: vec![],
            partial: false,
            ..Default::default()
        }))
    }

    async fn get_by_key(
        &self,
        _req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        Ok(Response::new(GetByKeyResponse {
            rows: vec![HydratedRow {
                key: None,
                fields: vec![
                    Field {
                        name: "title".into(),
                        value: Some(str_val("hello")),
                    },
                    Field {
                        name: "age".into(),
                        value: Some(int_val(30)),
                    },
                ],
            }],
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

async fn post(app: axum::Router, uri: &str, body: JsonValue) -> (StatusCode, JsonValue) {
    let req = HttpRequest::builder()
        .method("POST")
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&body).unwrap()))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn search_translates_dsl_and_shapes_documents() {
    let seen = Arc::new(Mutex::new(String::new()));
    let node = CaptureNode {
        seen_query: seen.clone(),
    };
    let gw = Arc::new(Gateway::new(Arc::new(node)));
    let app = opensearch_router(gw);

    let (status, body) = post(
        app,
        "/telemetry/_search",
        json!({
            "query": { "bool": {
                "must": [{ "match": { "title": "hello" } }],
                "filter": [{ "range": { "age": { "gte": "18" } } }]
            }},
            "size": 5
        }),
    )
    .await;

    assert_eq!(status, StatusCode::OK);

    // (a) The DSL reached the engine as the translated native query string.
    assert_eq!(*seen.lock().unwrap(), "title:hello AND (age:[18 TO ])");

    // (b) OpenSearch-shaped response: total, _id from the composite key, _source from hydration.
    assert_eq!(body["hits"]["total"]["value"], 1);
    let hit = &body["hits"]["hits"][0];
    assert_eq!(hit["_index"], "telemetry");
    assert_eq!(hit["_id"], "42#u1"); // partition#identifier
    assert_eq!(hit["_score"], 1.5);
    assert_eq!(hit["_source"]["title"], "hello");
    assert_eq!(hit["_source"]["age"], 30);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_clause_returns_clear_error() {
    let node = CaptureNode {
        seen_query: Arc::new(Mutex::new(String::new())),
    };
    let app = opensearch_router(Arc::new(Gateway::new(Arc::new(node))));

    let (status, body) = post(
        app,
        "/telemetry/_search",
        json!({ "query": { "wildcard": { "title": "hel*" } } }),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
    assert_eq!(body["error"]["type"], "unsupported");
    assert!(body["error"]["reason"]
        .as_str()
        .unwrap()
        .contains("wildcard"));
}
