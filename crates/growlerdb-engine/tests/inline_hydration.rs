//! **Inline hydration** end-to-end: `SearchRequest.hydrate` resolves the returned page's
//! coordinates through the Gateway's governed GetByKey path and attaches each row to its hit —
//! rows pair by **coordinates** (not response position), failures degrade **per-hit**, the
//! opt-in is stripped before the shard scatter, and an over-ceiling page is rejected up front.

use std::sync::{Arc, Mutex};

use axum::body::{to_bytes, Body};
use axum::http::{Request as HttpRequest, StatusCode};
use growlerdb_engine::{rest, Gateway, Node};
use growlerdb_proto::v1::{
    value::Kind, Coordinates, DescribeIndexRequest, DescribeIndexResponse, Field, GetByKeyRequest,
    GetByKeyResponse, HydratedRow, SearchHit, SearchRequest, SearchResponse, SemanticSearchRequest,
    SuggestRequest, SuggestResponse, Value,
};
use serde_json::{json, Value as JsonValue};
use tonic::{Code, Request, Response, Status};
use tower::ServiceExt;

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

/// A single-shard Node serving three fixed hits (`a`, `b`, `c`). Hydration returns rows for
/// `hydratable` ids only, in **reverse request order** (so positional pairing would mis-attribute
/// them), or fails wholesale with `fail_hydration`. Captures what the shard actually saw.
struct FakeNode {
    hydratable: Vec<&'static str>,
    fail_hydration: bool,
    /// The `hydrate` flag as it arrived at the shard — must always be false (Gateway-stripped).
    seen_search_hydrate: Mutex<Option<bool>>,
    /// The projection the hydration request carried.
    seen_columns: Mutex<Vec<String>>,
}

impl FakeNode {
    fn new(hydratable: Vec<&'static str>, fail_hydration: bool) -> Self {
        FakeNode {
            hydratable,
            fail_hydration,
            seen_search_hydrate: Mutex::new(None),
            seen_columns: Mutex::new(Vec::new()),
        }
    }

    fn hits() -> Vec<SearchHit> {
        [("a", 3.0), ("b", 2.0), ("c", 1.0)]
            .into_iter()
            .map(|(id, score)| SearchHit {
                coordinates: Some(coords(id)),
                score,
                fields: vec![Field {
                    name: "title".into(),
                    value: Some(Value {
                        kind: Some(Kind::Str(format!("cached-{id}"))),
                    }),
                }],
                ..Default::default()
            })
            .collect()
    }
}

#[tonic::async_trait]
impl Node for FakeNode {
    async fn search(
        &self,
        req: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        *self.seen_search_hydrate.lock().unwrap() = Some(req.get_ref().hydrate);
        Ok(Response::new(SearchResponse {
            hits: Self::hits(),
            total: 3,
            ..Default::default()
        }))
    }

    async fn semantic_search(
        &self,
        _req: Request<SemanticSearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        Ok(Response::new(SearchResponse {
            hits: Self::hits(),
            total: 3,
            ..Default::default()
        }))
    }

    async fn get_by_key(
        &self,
        req: Request<GetByKeyRequest>,
    ) -> Result<Response<GetByKeyResponse>, Status> {
        if self.fail_hydration {
            return Err(Status::unavailable("iceberg catalog unreachable"));
        }
        let req = req.into_inner();
        *self.seen_columns.lock().unwrap() = req.columns.clone();
        // Reverse order + dropped keys: exactly what a sharded/windowed hydration can produce.
        let rows = req
            .keys
            .iter()
            .rev()
            .filter_map(|key| {
                let id = key.identifier.first()?.value.as_ref()?;
                let id = match &id.kind {
                    Some(Kind::Str(s)) => s.as_str(),
                    _ => return None,
                };
                self.hydratable.contains(&id).then(|| HydratedRow {
                    key: Some(key.clone()),
                    fields: vec![Field {
                        name: "body".into(),
                        value: Some(Value {
                            kind: Some(Kind::Str(format!("row-{id}"))),
                        }),
                    }],
                })
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

fn search_req(hydrate: bool, columns: Vec<String>) -> Request<SearchRequest> {
    Request::new(SearchRequest {
        query: "*:*".into(),
        limit: 10,
        hydrate,
        hydrate_columns: columns,
        ..Default::default()
    })
}

fn row_body(hit: &SearchHit) -> Option<&str> {
    match &hit.row.as_ref()?.fields.first()?.value.as_ref()?.kind {
        Some(Kind::Str(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Rows attach to their hits **by coordinates** even when hydration returns them out of order
/// and drops one; the dropped hit degrades to a per-hit `hydrate_error`; the shard never sees
/// the `hydrate` flag; the projection reaches the lookup.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rows_attach_by_coordinates_and_failures_degrade_per_hit() {
    let node = Arc::new(FakeNode::new(vec!["a", "c"], false));
    let gw = Gateway::new(node.clone());

    let resp = gw
        .search(search_req(true, vec!["body".into()]))
        .await
        .unwrap()
        .into_inner();

    let by_id: Vec<(&str, Option<&str>, &str)> = resp
        .hits
        .iter()
        .map(|h| {
            let id = match &h.coordinates.as_ref().unwrap().identifier[0]
                .value
                .as_ref()
                .unwrap()
                .kind
            {
                Some(Kind::Str(s)) => s.as_str(),
                _ => panic!("string id"),
            };
            (id, row_body(h), h.hydrate_error.as_str())
        })
        .collect();
    assert_eq!(by_id[0].0, "a");
    assert_eq!(by_id[0].1, Some("row-a"), "row pairs to its own hit");
    assert_eq!(by_id[1].0, "b");
    assert_eq!(by_id[1].1, None, "missing row attaches to no hit");
    assert!(
        by_id[1].2.contains("row not returned"),
        "the missing row is a flagged per-hit gap: {}",
        by_id[1].2
    );
    assert_eq!(by_id[2].0, "c");
    assert_eq!(
        by_id[2].1,
        Some("row-c"),
        "reverse-order row still pairs right"
    );
    assert!(by_id[2].2.is_empty());

    // The opt-in is Gateway orchestration: the shard's SearchRequest arrived without it,
    // and the hydration carried the requested projection.
    assert_eq!(*node.seen_search_hydrate.lock().unwrap(), Some(false));
    assert_eq!(*node.seen_columns.lock().unwrap(), vec!["body".to_string()]);
}

/// A wholesale hydration failure (source down) never fails the search: every hit keeps its
/// coordinates + cached fields and carries the error per-hit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hydration_failure_degrades_per_hit_not_the_search() {
    let gw = Gateway::new(Arc::new(FakeNode::new(vec![], true)));
    let resp = gw
        .search(search_req(true, vec![]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.hits.len(), 3);
    for hit in &resp.hits {
        assert!(hit.row.is_none());
        assert!(
            hit.hydrate_error.contains("hydration failed"),
            "per-hit error: {}",
            hit.hydrate_error
        );
        assert!(!hit.fields.is_empty(), "cached fields survive");
    }
}

/// A hydrate page above the GetByKey batch ceiling is rejected up front — before any shard
/// work — with the hydrate-specific message (not a generic fetch-ceiling error).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hydrate_page_over_the_lookup_ceiling_is_rejected() {
    let gw = Gateway::new(Arc::new(FakeNode::new(vec![], false)));
    let mut req = search_req(true, vec![]);
    req.get_mut().limit = 1_001;
    let err = gw.search(req).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(
        err.message().contains("hydration batch maximum"),
        "{}",
        err.message()
    );
    // The same page without hydrate is fine (it's under the general fetch ceiling).
    let mut req = search_req(false, vec![]);
    req.get_mut().limit = 1_001;
    assert!(gw.search(req).await.is_ok());
}

/// Semantic and hybrid searches hydrate after their own merge/fusion, same per-hit contract.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn semantic_and_hybrid_hydrate_their_fused_hits() {
    let gw = Gateway::new(Arc::new(FakeNode::new(vec!["a", "b", "c"], false)));

    let resp = gw
        .semantic_search(Request::new(SemanticSearchRequest {
            vector_field: "vec".into(),
            query_text: "q".into(),
            k: 10,
            hydrate: true,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.hits.is_empty());
    assert!(
        resp.hits.iter().all(|h| h.row.is_some()),
        "semantic hits hydrate"
    );

    let resp = gw
        .hybrid_search(Request::new(growlerdb_proto::v1::HybridSearchRequest {
            vector_field: "vec".into(),
            query_text: "q".into(),
            k: 10,
            hydrate: true,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert!(!resp.hits.is_empty());
    assert!(
        resp.hits.iter().all(|h| h.row.is_some()),
        "fused hits hydrate"
    );
}

/// The REST shape: `hydrate: true` puts each hit's authoritative `row` (or `hydrate_error`)
/// on the wire; without the opt-in neither key exists.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rest_search_carries_row_and_hydrate_error() {
    let app = rest::router(Arc::new(Gateway::new(Arc::new(FakeNode::new(
        vec!["a", "c"],
        false,
    )))));
    let post = |app: axum::Router, body: JsonValue| async move {
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/search")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        (status, serde_json::from_slice::<JsonValue>(&bytes).unwrap())
    };

    let (status, body) = post(
        app.clone(),
        json!({ "query": "*:*", "hydrate": true, "hydrate_columns": ["body"] }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let hits = body["hits"].as_array().unwrap();
    assert_eq!(hits[0]["row"]["body"], "row-a");
    assert!(hits[1].get("row").is_none());
    assert!(hits[1]["hydrate_error"]
        .as_str()
        .unwrap()
        .contains("row not returned"));
    assert_eq!(hits[2]["row"]["body"], "row-c");
    assert_eq!(
        hits[0]["fields"]["title"], "cached-a",
        "cached fields still ride the hit"
    );

    // No opt-in ⇒ neither key on the wire.
    let (status, body) = post(app, json!({ "query": "*:*" })).await;
    assert_eq!(status, StatusCode::OK);
    let hit = &body["hits"].as_array().unwrap()[0];
    assert!(hit.get("row").is_none());
    assert!(hit.get("hydrate_error").is_none());
}
