//! End-to-end test of the **public gRPC Engine-API front** (task-30 B1): mount the
//! Gateway-backed Search/Suggest/Admin gRPC services over a `LocalNode`, connect real gRPC
//! clients, and confirm the query+describe surface routes through the Gateway — and that the
//! intentionally un-routed methods (PIT, admin mutations) surface `Unimplemented`.

use std::collections::BTreeMap;
use std::sync::Arc;

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_engine::{
    AdminService, Gateway, LocalNode, LookupService, SearchService, SuggestService,
};
use growlerdb_index::{LocalIndexStore, Shard, ShardId};
use growlerdb_proto::v1::admin_client::AdminClient;
use growlerdb_proto::v1::search_client::SearchClient;
use growlerdb_proto::v1::suggest_client::SuggestClient;
use growlerdb_proto::v1::{
    AlterIndexRequest, DescribeIndexRequest, OpenPitRequest, SearchRequest, Sort as WireSort,
    SuggestRequest,
};
use growlerdb_source::IcebergConfig;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::Code;

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

/// Mount the Gateway gRPC front over a LocalNode wrapping `shard`; return its address.
async fn gateway_front(shard: Arc<Shard>) -> std::net::SocketAddr {
    let node = LocalNode::new(
        SearchService::new(shard.clone()),
        SuggestService::new(shard.clone()),
        LookupService::new(shard.clone(), IcebergConfig::local(), "g.docs"),
        AdminService::new(shard, "docs"),
    );
    let gw = Arc::new(Gateway::new(node.shared()));
    let (search, suggest, lookup, admin) = growlerdb_engine::gateway_grpc::servers(gw);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(search)
            .add_service(suggest)
            .add_service(lookup)
            .add_service(admin)
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    addr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_front_routes_query_and_describe_through_the_gateway() {
    let tmp = tempfile::tempdir().unwrap();
    let addr = gateway_front(shard(tmp.path())).await;
    let url = format!("http://{addr}");

    // Search routes through the Gateway: rank desc → 1 (berlin) before 2 (bern).
    let mut search = loop {
        match SearchClient::connect(url.clone()).await {
            Ok(c) => break c,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
    };
    let resp = search
        .search(SearchRequest {
            query: "rank:[0 TO 100]".into(),
            limit: 10,
            sort: vec![WireSort {
                field: "rank".into(),
                descending: true,
            }],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let ids: Vec<String> = resp
        .hits
        .iter()
        .map(|h| {
            match h.coordinates.as_ref().unwrap().identifier[0]
                .value
                .as_ref()
                .unwrap()
                .kind
                .clone()
                .unwrap()
            {
                growlerdb_proto::v1::value::Kind::Str(s) => s,
                other => panic!("unexpected id kind: {other:?}"),
            }
        })
        .collect();
    assert_eq!(ids, vec!["1", "2"]);

    // Suggest routes through the Gateway.
    let mut suggest = SuggestClient::connect(url.clone()).await.unwrap();
    let sresp = suggest
        .suggest(SuggestRequest {
            field: "city".into(),
            text: "ber".into(),
            limit: 10,
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner();
    let terms: Vec<&str> = sresp.suggestions.iter().map(|s| s.text.as_str()).collect();
    assert_eq!(terms, vec!["berlin", "bern"]);

    // Admin describe routes; stats come back from the Node.
    let mut admin = AdminClient::connect(url.clone()).await.unwrap();
    let stats = admin
        .describe_index(DescribeIndexRequest {
            window: 0,
            index: String::new(),
        })
        .await
        .unwrap()
        .into_inner()
        .stats
        .unwrap();
    assert_eq!(stats.name, "docs");
    assert_eq!(stats.num_docs, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn grpc_front_reports_unrouted_methods_as_unimplemented() {
    let tmp = tempfile::tempdir().unwrap();
    let addr = gateway_front(shard(tmp.path())).await;
    let url = format!("http://{addr}");

    let mut search = loop {
        match SearchClient::connect(url.clone()).await {
            Ok(c) => break c,
            Err(_) => tokio::time::sleep(std::time::Duration::from_millis(20)).await,
        }
    };
    // PIT is not routed through the Gateway yet (task-29).
    let err = search.open_pit(OpenPitRequest {}).await.unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);

    // Admin mutations are Node/Control-Plane, not Gateway-routed.
    let mut admin = AdminClient::connect(url).await.unwrap();
    let err = admin
        .alter_index(AlterIndexRequest {
            index: String::new(),
            definition_yaml: "name: docs".into(),
            apply: false,
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::Unimplemented);
}
