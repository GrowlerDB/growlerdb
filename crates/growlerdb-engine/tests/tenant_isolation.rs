//! **End-to-end tenant isolation** (GA criterion, task-58). The per-seam pieces are unit-tested —
//! the authn boundary drops forged identity headers ([`authn`]), search injects a mandatory tenant
//! filter, hydration refuses a missing claim. This test composes them **through the `Gateway`**:
//! a real two-tenant index + an API-key authenticator, proving a caller scoped to one tenant can
//! never read another's rows — even while spoofing the tenant header or widening the query.

use std::collections::BTreeMap;
use std::sync::Arc;

use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_engine::{
    AdminService, ApiKeyStore, Gateway, KeyIdentity, LocalNode, LookupService, SearchService,
    ShardHandle, SuggestService,
};
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_proto::v1::SearchRequest;
use growlerdb_source::IcebergConfig;
use tonic::Request;

/// A `Gateway` over a tenant-scoped index (`tenant_field: tenant`) holding rows for two tenants —
/// acme (`a`, `c`) and globex (`b`) — fronted by an API-key authenticator. Returns the gateway and
/// an issued key whose verified claim scopes it to `acme`.
fn two_tenant_gateway(root: &std::path::Path) -> (Arc<Gateway>, String) {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("tenant", SourceType::String),
        ],
        vec![],
        vec!["id".into()],
    );
    let idx = IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\ntenant_field: tenant\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: tenant, type: KEYWORD } ] }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap();

    let shard = LocalIndexStore::open(root)
        .unwrap()
        .create_shard(&ShardId::single("docs"), &idx)
        .unwrap();
    let mk = |id: &str, tenant: &str| {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("tenant".to_string(), Value::from(tenant));
        LocatedDoc {
            doc: Document::new(key, f),
            iceberg_file: "f".into(),
            row_position: 0,
        }
    };
    IndexWriter::write(
        &shard,
        &CommitBatch::from_upserts(
            vec![mk("a", "acme"), mk("b", "globex"), mk("c", "acme")],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();

    let handle = ShardHandle::new(Arc::new(shard));
    let node = LocalNode::new(
        SearchService::new(handle.clone()),
        SuggestService::new(handle.clone()),
        LookupService::new(handle.clone(), IcebergConfig::local(), "g.docs"),
        AdminService::new(handle.clone(), "docs"),
    )
    .shared();

    let apikeys = Arc::new(ApiKeyStore::new());
    let key = apikeys.issue(KeyIdentity {
        principal: "acme-reader".into(),
        tenant: Some("acme".into()),
        roles: vec!["viewer".into()],
    });
    let gw = Arc::new(Gateway::new(node).with_authn(apikeys));
    (gw, key)
}

/// A search request authenticated by `api_key`, optionally carrying a (forged) caller-asserted
/// tenant header — which the authn boundary must drop in favor of the verified claim.
fn req(query: &str, api_key: &str, forged_tenant: Option<&str>) -> Request<SearchRequest> {
    let mut r = Request::new(SearchRequest {
        query: query.into(),
        limit: 10,
        ..Default::default()
    });
    let md = r.metadata_mut();
    md.insert(
        "authorization",
        format!("ApiKey {api_key}").parse().unwrap(),
    );
    if let Some(t) = forged_tenant {
        md.insert("x-growlerdb-tenant", t.parse().unwrap());
    }
    r
}

fn ids_of(resp: &growlerdb_proto::v1::SearchResponse) -> Vec<String> {
    let mut ids: Vec<String> = resp
        .hits
        .iter()
        .filter_map(|h| h.coordinates.as_ref())
        .flat_map(|c| c.identifier.iter())
        .filter_map(|f| {
            f.value.as_ref().and_then(|v| match &v.kind {
                Some(growlerdb_proto::v1::value::Kind::Str(s)) => Some(s.clone()),
                _ => None,
            })
        })
        .collect();
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forged_tenant_header_cannot_widen_past_the_verified_claim() {
    let tmp = tempfile::tempdir().unwrap();
    let (gw, acme_key) = two_tenant_gateway(tmp.path());

    // The query matches every row, AND the caller forges `x-growlerdb-tenant: globex`. The gateway
    // drops the forged header, stamps the verified `acme`, and ANDs `tenant:acme` into the query.
    let resp = gw
        .search(req("id:a OR id:b OR id:c", &acme_key, Some("globex")))
        .await
        .expect("authenticated search succeeds")
        .into_inner();

    assert_eq!(ids_of(&resp), vec!["a", "c"]); // globex's `b` never leaks
    assert_eq!(resp.total, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_query_clause_cannot_widen_past_the_tenant_filter() {
    let tmp = tempfile::tempdir().unwrap();
    let (gw, acme_key) = two_tenant_gateway(tmp.path());

    // Even an explicit `tenant:globex OR ...` can't widen — the injected `AND tenant:acme` binds.
    let resp = gw
        .search(req("tenant:globex OR id:a", &acme_key, None))
        .await
        .unwrap()
        .into_inner();

    assert_eq!(ids_of(&resp), vec!["a"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unauthenticated_request_is_rejected_before_the_shard() {
    let tmp = tempfile::tempdir().unwrap();
    let (gw, _key) = two_tenant_gateway(tmp.path());

    // No credential → the authenticator rejects it (Unauthenticated) before any shard read.
    let mut r = Request::new(SearchRequest {
        query: "id:a".into(),
        limit: 10,
        ..Default::default()
    });
    // A forged tenant header with no credential must not be honored either.
    r.metadata_mut()
        .insert("x-growlerdb-tenant", "acme".parse().unwrap());
    let err = gw.search(r).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}
