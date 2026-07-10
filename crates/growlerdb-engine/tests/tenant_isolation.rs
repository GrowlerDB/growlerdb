//! **End-to-end tenant isolation** (GA criterion, task-58; per-read-path coverage, task-246). The
//! per-seam pieces are unit-tested — the authn boundary drops forged identity headers ([`authn`]),
//! search injects a mandatory tenant filter, hydration refuses a missing claim. This test composes
//! them **through the `Gateway`**: a real two-tenant index + an API-key authenticator, proving a
//! caller scoped to one tenant can never read another's rows — even while spoofing the tenant header
//! or widening the query.
//!
//! task-246 broadens the end-to-end coverage from search alone to **every read path** the mandatory
//! tenant filter governs, so SECURITY.md's "verified" claim is backed by direct coverage:
//! - **search** — the original cases (forged header / widening clause / unauthenticated).
//! - **aggregate** — a tenant-scoped aggregation counts only the caller's docs; a forged header can't
//!   widen it and an unauthenticated one is rejected.
//! - **hydration (`get_by_key`)** — a missing verified claim fails closed with `PermissionDenied`
//!   before any Iceberg connect, an unauthenticated request is rejected, and a forged header can't
//!   inject a tenant. (The row itself is hydrated from Iceberg, so the authoritative-value drop is
//!   asserted as a unit in `lookup_service.rs`; here we assert the boundary that governs it.)
//! - **export** — the streaming scroll (a Node-only RPC, not Gateway-routed) applies the same tenant
//!   scope as search: only the caller's rows stream out, a widening clause can't widen, and a
//!   missing claim fails closed.
//! - **suggest** — a tenant-scoped index **fails closed** (`PermissionDenied`): term-dictionary
//!   suggestions aren't yet tenant-filtered, so they're refused rather than leaking other tenants'
//!   terms.

use std::collections::BTreeMap;
use std::sync::Arc;

use growlerdb_core::ShardRouter;
use growlerdb_core::{
    CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
    SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
};
use growlerdb_engine::{
    AdminService, ApiKeyStore, Gateway, IndexRoute, KeyIdentity, LocalNode, LookupService, Node,
    RouteResolver, SearchService, ShardHandle, SuggestService,
};
use growlerdb_index::{LocalIndexStore, ShardId};
use growlerdb_proto::v1::{
    AggregateRequest, Coordinates, ExportRequest, GetByKeyRequest, SearchRequest, Sort,
    SuggestKind, SuggestRequest,
};
use growlerdb_proto::Search as _;
use growlerdb_source::IcebergConfig;
use tokio_stream::StreamExt as _;
use tonic::Request;

/// A `Gateway` over a tenant-scoped index (`tenant_field: tenant`) holding rows for two tenants —
/// acme (`a`, `c`) and globex (`b`) — fronted by an API-key authenticator. Returns the gateway and
/// an issued key whose verified claim scopes it to `acme`.
fn two_tenant_gateway(root: &std::path::Path) -> (Arc<Gateway>, String) {
    let (gw, _apikeys, key) = two_tenant_gateway_with_store(root);
    (gw, key)
}

/// As [`two_tenant_gateway`], but also hands back the [`ApiKeyStore`] so a test can issue *other*
/// keys (a `globex`-scoped caller, or a claimless key) to exercise the tenant filter across callers.
fn two_tenant_gateway_with_store(
    root: &std::path::Path,
) -> (Arc<Gateway>, Arc<ApiKeyStore>, String) {
    let (node, apikeys, key) = two_tenant_node(root);
    let gw = Arc::new(Gateway::new(node).with_authn(apikeys.clone()));
    (gw, apikeys, key)
}

/// Issue an API key scoped to `tenant` (or claimless when `None`) against `apikeys`.
fn issue_key(apikeys: &ApiKeyStore, principal: &str, tenant: Option<&str>) -> String {
    apikeys.issue(KeyIdentity {
        principal: principal.into(),
        tenant: tenant.map(str::to_string),
        roles: vec!["viewer".into()],
        indexes: Vec::new(),
    })
}

/// The tenant-scoped Node + its API-key authenticator + a key scoped to `acme` — the shared build
/// used by both the single-index gateway and the multi-index routing variant (task-240).
fn two_tenant_node(root: &std::path::Path) -> (Arc<dyn Node>, Arc<ApiKeyStore>, String) {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("tenant", SourceType::String),
        ],
        vec![],
        vec!["id".into()],
    );
    // `tenant` is a fast field so the aggregate cases (task-246) can terms-bucket on it; this doesn't
    // change search/hydration behaviour, it only makes the column aggregatable.
    let idx = IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\ntenant_field: tenant\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: tenant, type: KEYWORD, fast: true } ] }\n",
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
        indexes: Vec::new(),
    });
    (node, apikeys, key)
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

/// Stamp the `Authorization: ApiKey …` header (+ an optional *forged* `x-growlerdb-tenant`) onto a
/// request — the shared credential-wiring for the non-search read paths (aggregate/hydrate/suggest).
/// The forged header, if present, is what the authn boundary must drop in favor of the verified claim.
fn auth<T>(r: &mut Request<T>, api_key: &str, forged_tenant: Option<&str>) {
    let md = r.metadata_mut();
    md.insert(
        "authorization",
        format!("ApiKey {api_key}").parse().unwrap(),
    );
    if let Some(t) = forged_tenant {
        md.insert("x-growlerdb-tenant", t.parse().unwrap());
    }
}

/// An `AggregateRequest` authenticated by `api_key`, optionally with a forged tenant header.
fn agg_req(
    query: &str,
    aggs: &str,
    api_key: &str,
    forged_tenant: Option<&str>,
) -> Request<AggregateRequest> {
    let mut r = Request::new(AggregateRequest {
        query: query.into(),
        aggs: aggs.into(),
        ..Default::default()
    });
    auth(&mut r, api_key, forged_tenant);
    r
}

/// A `GetByKeyRequest` for `ids`, authenticated by `api_key`, optionally with a forged tenant header.
fn get_req(ids: &[&str], api_key: &str, forged_tenant: Option<&str>) -> Request<GetByKeyRequest> {
    let keys: Vec<Coordinates> = ids
        .iter()
        .map(|id| (&CompositeKey::new(vec![], vec![("id".into(), Value::from(*id))])).into())
        .collect();
    let mut r = Request::new(GetByKeyRequest {
        keys,
        columns: vec![],
        index: String::new(),
        window: 0,
    });
    auth(&mut r, api_key, forged_tenant);
    r
}

/// A `SuggestRequest` (prefix) authenticated by `api_key`, optionally with a forged tenant header.
fn suggest_req(
    field: &str,
    text: &str,
    api_key: &str,
    forged_tenant: Option<&str>,
) -> Request<SuggestRequest> {
    let mut r = Request::new(SuggestRequest {
        field: field.into(),
        text: text.into(),
        limit: 10,
        kind: SuggestKind::Prefix as i32,
        ..Default::default()
    });
    auth(&mut r, api_key, forged_tenant);
    r
}

/// A tenant-scoped [`SearchService`] over the same two-tenant corpus (acme: `a`,`c`; globex: `b`),
/// for the **export** path — a Node-only streaming RPC the `Gateway` doesn't route, so it's driven
/// directly. The verified tenant claim is stamped on each request the way the authn boundary would.
fn two_tenant_search_service(root: &std::path::Path) -> SearchService {
    let src = SourceSchema::new(
        vec![
            SourceField::new("id", SourceType::String),
            SourceField::new("rank", SourceType::Long),
            SourceField::new("tenant", SourceType::String),
        ],
        vec![],
        vec!["id".into()],
    );
    // `rank` is a fast field so export can scroll in keyset order over it (export requires a sort).
    let idx = IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\ntenant_field: tenant\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD }, { path: rank, type: LONG, fast: true }, { path: tenant, type: KEYWORD } ] }\n",
    )
    .unwrap()
    .resolve(&src)
    .unwrap();
    let shard = LocalIndexStore::open(root)
        .unwrap()
        .create_shard(&ShardId::single("docs"), &idx)
        .unwrap();
    let mk = |id: &str, rank: i64, tenant: &str| {
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from(id));
        f.insert("rank".to_string(), Value::from(rank));
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
            vec![mk("a", 1, "acme"), mk("b", 2, "globex"), mk("c", 3, "acme")],
            SourceCheckpoint::iceberg(1),
            "b1",
        ),
    )
    .unwrap();
    SearchService::new(Arc::new(shard))
}

/// An `ExportRequest` (sorted by `rank`, required for the scroll), with the verified tenant stamped
/// as `x-growlerdb-tenant` (as the authn boundary would after validating a credential).
fn export_req(query: &str, verified_tenant: Option<&str>) -> Request<ExportRequest> {
    let mut r = Request::new(ExportRequest {
        query: query.into(),
        page_size: 10,
        sort: vec![Sort {
            field: "rank".into(),
            descending: false,
        }],
        pit_id: 0,
    });
    if let Some(t) = verified_tenant {
        r.metadata_mut()
            .insert("x-growlerdb-tenant", t.parse().unwrap());
    }
    r
}

/// Drain every streamed export page into the sorted set of hit ids.
async fn export_ids(svc: &SearchService, req: Request<ExportRequest>) -> Vec<String> {
    let mut stream = svc.export(req).await.expect("export starts").into_inner();
    let mut ids = Vec::new();
    while let Some(page) = stream.next().await {
        let page = page.expect("export page");
        for h in &page.hits {
            if let Some(c) = &h.coordinates {
                for f in &c.identifier {
                    if let Some(growlerdb_proto::v1::Value {
                        kind: Some(growlerdb_proto::v1::value::Kind::Str(s)),
                    }) = &f.value
                    {
                        ids.push(s.clone());
                    }
                }
            }
        }
    }
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

/// A [`RouteResolver`] that fronts the tenant-scoped node under index name `docs` (task-240).
struct DocsResolver(Arc<dyn Node>);

#[tonic::async_trait]
impl RouteResolver for DocsResolver {
    async fn resolve(&self, index: &str) -> Result<Option<Arc<IndexRoute>>, String> {
        if index == "docs" {
            Ok(Some(IndexRoute::new(
                vec![self.0.clone()],
                ShardRouter::hashed(1),
                None,
                Vec::new(),
            )))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_isolation_holds_through_multi_index_routing() {
    // task-240: routing a request through a resolved per-index route must not weaken the engine-level
    // tenant filter. The acme-scoped caller, even forging `x-growlerdb-tenant: globex` and widening
    // the query, only ever sees acme's rows — the shard applies the mandatory tenant filter exactly
    // as in the single-index path.
    let tmp = tempfile::tempdir().unwrap();
    let (node, apikeys, acme_key) = two_tenant_node(tmp.path());
    let resolver = Arc::new(DocsResolver(node));
    let gw = Arc::new(Gateway::multi_index(resolver, Some("docs".into())).with_authn(apikeys));

    // Explicit `index: docs` + a forged globex header + a query matching every row.
    let mut r = Request::new(SearchRequest {
        query: "id:a OR id:b OR id:c".into(),
        limit: 10,
        index: "docs".into(),
        ..Default::default()
    });
    let md = r.metadata_mut();
    md.insert(
        "authorization",
        format!("ApiKey {acme_key}").parse().unwrap(),
    );
    md.insert("x-growlerdb-tenant", "globex".parse().unwrap());
    let resp = gw.search(r).await.unwrap().into_inner();
    assert_eq!(ids_of(&resp), vec!["a", "c"]); // globex's `b` never leaks through the route
    assert_eq!(resp.total, 2);
}

// ---- Aggregate (task-246) ------------------------------------------------------------------------

/// A terms/stats aggregation over a tenant-scoped index sees only the caller's docs: the injected
/// `AND tenant:acme` binds before the agg runs, so acme's count is 2 (`a`,`c`), never globex's `b`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tenant_scoped_aggregation_counts_only_the_callers_docs() {
    let tmp = tempfile::tempdir().unwrap();
    let (gw, apikeys, acme_key) = two_tenant_gateway_with_store(tmp.path());
    let globex_key = issue_key(&apikeys, "globex-reader", Some("globex"));

    // acme: a match-all agg, even forging `globex`, counts only acme's 2 docs.
    let resp = gw
        .aggregate(agg_req(
            "id:a OR id:b OR id:c",
            r#"{"by_tenant": {"Terms": {"field": "tenant", "size": 10}}}"#,
            &acme_key,
            Some("globex"),
        ))
        .await
        .expect("authenticated aggregate")
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&resp.results).unwrap();
    let buckets = v["by_tenant"]["buckets"].as_array().expect("buckets");
    // Only the acme bucket exists, with 2 docs — globex's `b` is filtered before aggregation.
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["key"].as_str(), Some("acme"));
    assert_eq!(buckets[0]["doc_count"].as_u64(), Some(2));

    // A legitimate globex caller sees only its own single doc — the same filter, other tenant.
    let resp = gw
        .aggregate(agg_req(
            "id:a OR id:b OR id:c",
            r#"{"by_tenant": {"Terms": {"field": "tenant", "size": 10}}}"#,
            &globex_key,
            None,
        ))
        .await
        .expect("globex aggregate")
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&resp.results).unwrap();
    let buckets = v["by_tenant"]["buckets"].as_array().expect("buckets");
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["key"].as_str(), Some("globex"));
    assert_eq!(buckets[0]["doc_count"].as_u64(), Some(1));
}

/// A widening query clause can't widen a tenant-scoped aggregation past the injected filter, and an
/// unauthenticated aggregation is rejected before any shard is touched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregation_cannot_widen_and_denies_the_unauthenticated() {
    let tmp = tempfile::tempdir().unwrap();
    let (gw, acme_key) = two_tenant_gateway(tmp.path());

    // `tenant:globex OR id:a` — the mandatory AND tenant:acme still binds, so only acme's `a` counts.
    let resp = gw
        .aggregate(agg_req(
            "tenant:globex OR id:a",
            r#"{"by_tenant": {"Terms": {"field": "tenant", "size": 10}}}"#,
            &acme_key,
            None,
        ))
        .await
        .unwrap()
        .into_inner();
    let v: serde_json::Value = serde_json::from_str(&resp.results).unwrap();
    let buckets = v["by_tenant"]["buckets"].as_array().expect("buckets");
    assert_eq!(buckets.len(), 1);
    assert_eq!(buckets[0]["key"].as_str(), Some("acme"));
    assert_eq!(buckets[0]["doc_count"].as_u64(), Some(1)); // only `a`, not globex's `b`

    // No credential (+ a forged tenant header) → Unauthenticated before the shard.
    let mut r = Request::new(AggregateRequest {
        query: "id:a".into(),
        aggs: r#"{"by_tenant": {"Terms": {"field": "tenant", "size": 10}}}"#.into(),
        ..Default::default()
    });
    r.metadata_mut()
        .insert("x-growlerdb-tenant", "acme".parse().unwrap());
    let err = gw.aggregate(r).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

// ---- Hydration / get_by_key (task-246) -----------------------------------------------------------

/// Hydration on a tenant-scoped index **fails closed on a missing verified claim** (a key whose API
/// key carries no tenant) — `PermissionDenied`, *before* any Iceberg connect — and rejects the
/// unauthenticated caller. A forged `x-growlerdb-tenant` header can't inject a tenant either: with no
/// credential it's stripped and the request is `Unauthenticated`; with an acme credential it's
/// replaced by the verified `acme` claim, so the request proceeds under acme (not the forged tenant).
///
/// The authoritative-Iceberg-value drop (a coordinate that resolves to another tenant's row is
/// silently omitted) needs a live catalog to hydrate the row, so it's asserted as a unit in
/// `lookup_service.rs::post_hydration_filter_drops_foreign_tenant_rows`; here we prove the boundary.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hydration_fails_closed_without_a_verified_tenant_claim() {
    let tmp = tempfile::tempdir().unwrap();
    let (gw, apikeys, _acme_key) = two_tenant_gateway_with_store(tmp.path());

    // A claimless (tenant: None) key on a tenant-scoped index → the lookup service fails closed with
    // PermissionDenied before resolving locators or reaching the (absent) catalog.
    let claimless = issue_key(&apikeys, "no-tenant", None);
    let err = gw
        .get_by_key(get_req(&["a"], &claimless, None))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    // Even with a *forged* tenant header, a claimless key stays claimless (the forged header is
    // dropped by authn, not trusted) → still PermissionDenied.
    let err = gw
        .get_by_key(get_req(&["a", "b"], &claimless, Some("globex")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    // No credential at all (+ a forged tenant header) → Unauthenticated before the shard.
    let mut r = get_req(&["a"], "unused", None);
    r.metadata_mut().remove("authorization");
    r.metadata_mut()
        .insert("x-growlerdb-tenant", "acme".parse().unwrap());
    let err = gw.get_by_key(r).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unauthenticated);
}

// ---- Export (task-246) ---------------------------------------------------------------------------

/// Export is a Node-only streaming scroll (not Gateway-routed), and it applies the **same** tenant
/// scope as search: on a tenant-scoped index only the caller's rows stream out, even when the query
/// matches every row or explicitly ORs in another tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_scoped_export_streams_only_the_callers_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let svc = two_tenant_search_service(tmp.path());

    // A match-all export as acme → only acme's `a`,`c` scroll out; globex's `b` never streams.
    let ids = export_ids(&svc, export_req("rank:[0 TO 100]", Some("acme"))).await;
    assert_eq!(ids, vec!["a", "c"]);

    // A widening clause can't widen the export past the injected AND tenant:acme.
    let ids = export_ids(&svc, export_req("tenant:globex OR id:a", Some("acme"))).await;
    assert_eq!(ids, vec!["a"]);

    // A legitimate globex caller scrolls only its own row.
    let ids = export_ids(&svc, export_req("rank:[0 TO 100]", Some("globex"))).await;
    assert_eq!(ids, vec!["b"]);
}

/// Export **fails closed** on a tenant-scoped index when the request carries no verified claim — the
/// same PermissionDenied search/hydration give, so a full-scan export can't be the tenant-filter's
/// escape hatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tenant_scoped_export_requires_a_verified_claim() {
    let tmp = tempfile::tempdir().unwrap();
    let svc = two_tenant_search_service(tmp.path());
    let err = svc
        .export(export_req("rank:[0 TO 100]", None))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}

// ---- Suggest (task-246) --------------------------------------------------------------------------

/// Suggest **fails closed** on a tenant-scoped index: term-dictionary suggestions scan a field across
/// all docs and aren't yet tenant-filtered, so serving them would leak other tenants' terms. The
/// service refuses with `PermissionDenied` regardless of a valid claim or a forged header — the
/// conservative choice until a tenant-aware suggester lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn suggest_fails_closed_on_a_tenant_scoped_index() {
    let tmp = tempfile::tempdir().unwrap();
    let (gw, apikeys, acme_key) = two_tenant_gateway_with_store(tmp.path());

    // A properly-authenticated, tenant-scoped acme caller is still refused — fail closed.
    let err = gw
        .suggest(suggest_req("id", "a", &acme_key, None))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    // A forged `x-growlerdb-tenant` header doesn't change that (it's dropped by authn anyway).
    let err = gw
        .suggest(suggest_req("id", "a", &acme_key, Some("globex")))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);

    // A legitimate globex caller is refused just the same — the closure is per-index, not per-tenant.
    let globex_key = issue_key(&apikeys, "globex-reader", Some("globex"));
    let err = gw
        .suggest(suggest_req("id", "a", &globex_key, None))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::PermissionDenied);
}
