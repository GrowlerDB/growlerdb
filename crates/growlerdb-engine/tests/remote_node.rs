//! The **distributed half of the Gateway/Node seam** (task-30 B1): stand up the Node's
//! Search/Suggest/Lookup/Admin gRPC services on a real tonic server, point a
//! [`RemoteNode`] at it over a channel, and drive a [`Gateway`] through it — proving that
//! routing, results, and error/auth propagation all hold across a network hop (the same
//! `Gateway` API that embedded mode serves over a [`LocalNode`](growlerdb_engine::LocalNode)).

use std::collections::BTreeMap;
use std::sync::Arc;

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_engine::{
    AdminService, Gateway, LookupService, RemoteNode, SearchService, SuggestService,
};
use growlerdb_index::{LocalIndexStore, Shard, ShardId};
use growlerdb_proto::v1::{
    Coordinates, DescribeIndexRequest, Field, GetByKeyRequest, SearchRequest, Sort as WireSort,
    SuggestRequest, Value as WireValue,
};
use growlerdb_source::IcebergConfig;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request};

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

/// Spawn the Node gRPC services over `shard` on an ephemeral port; return a `Gateway`
/// whose `RemoteNode` is connected to it.
async fn distributed_gateway(shard: Arc<Shard>) -> Gateway {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let search = SearchService::new(shard.clone());
    let suggest = SuggestService::new(shard.clone());
    let lookup = LookupService::new(shard.clone(), IcebergConfig::local(), "g.docs");
    let admin = AdminService::new(shard, "docs");
    tokio::spawn(
        Server::builder()
            .add_service(search.into_server())
            .add_service(suggest.into_server())
            .add_service(lookup.into_server())
            .add_service(admin.into_server())
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    // Retry connect until the spawned server is accepting (no fixed sleep).
    let endpoint = format!("http://{addr}");
    let mut last = None;
    for _ in 0..50 {
        match RemoteNode::connect(endpoint.clone()).await {
            Ok(node) => return Gateway::new(Arc::new(node)),
            Err(e) => {
                last = Some(e);
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
    panic!("remote node never came up: {last:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_routes_search_suggest_describe_over_the_wire() {
    let tmp = tempfile::tempdir().unwrap();
    let gw = distributed_gateway(shard(tmp.path())).await;

    // Search: rank desc → berlin(30) before bern(10); coordinates carry the id.
    let resp = gw
        .search(Request::new(SearchRequest {
            query: "rank:[0 TO 100]".into(),
            limit: 10,
            sort: vec![WireSort {
                field: "rank".into(),
                descending: true,
            }],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let ids: Vec<String> = resp
        .hits
        .iter()
        .map(|h| {
            let id = &h.coordinates.as_ref().unwrap().identifier[0];
            match id.value.as_ref().unwrap().kind.clone().unwrap() {
                growlerdb_proto::v1::value::Kind::Str(s) => s,
                other => panic!("unexpected id kind: {other:?}"),
            }
        })
        .collect();
    assert_eq!(ids, vec!["1", "2"]);
    assert_eq!(resp.total, 2);

    // Suggest: prefix "ber" over the city dictionary.
    let sresp = gw
        .suggest(Request::new(SuggestRequest {
            field: "city".into(),
            text: "ber".into(),
            limit: 10,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let terms: Vec<&str> = sresp.suggestions.iter().map(|s| s.text.as_str()).collect();
    assert_eq!(terms, vec!["berlin", "bern"]);

    // Describe: stats routed back from the Node.
    let stats = gw
        .describe_index(Request::new(DescribeIndexRequest {
            window: 0,
            index: String::new(),
        }))
        .await
        .unwrap()
        .into_inner()
        .stats
        .unwrap();
    assert_eq!(stats.name, "docs");
    assert_eq!(stats.num_docs, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn gateway_propagates_node_errors_over_the_wire() {
    let tmp = tempfile::tempdir().unwrap();
    let gw = distributed_gateway(shard(tmp.path())).await;

    // Describing another index → NotFound, surfaced verbatim through the channel.
    let err = gw
        .describe_index(Request::new(DescribeIndexRequest {
            window: 0,
            index: "nope".into(),
        }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::NotFound);

    // GetByKey for an unindexed key → NotFound (resolved before any Iceberg connect),
    // proving the lookup route + error mapping hold over the wire.
    let missing = GetByKeyRequest {
        window: 0,
        keys: vec![Coordinates {
            partition: vec![],
            identifier: vec![Field {
                name: "id".into(),
                value: Some(WireValue {
                    kind: Some(growlerdb_proto::v1::value::Kind::Str("missing".into())),
                }),
            }],
        }],
        columns: vec![],
    };
    let err = gw.get_by_key(Request::new(missing)).await.unwrap_err();
    assert_eq!(err.code(), Code::NotFound);
}
