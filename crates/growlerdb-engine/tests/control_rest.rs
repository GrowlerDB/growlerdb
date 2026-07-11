//! The **control-plane REST surface**: the gateway's `/v1/indexes` REST routes proxy
//! to the Control Plane over gRPC. Stand up a real `ControlPlaneService` (over a temp registry)
//! on a tonic server, point the REST `control_router` at it via a gRPC client, and drive the
//! list/get/drop lifecycle over HTTP. `CreateIndex`/`DescribeSource` need a live Iceberg source,
//! so they're exercised against the Compose stack, not here.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request as HttpRequest, StatusCode};
use axum::Router;
use growlerdb_controlplane::Registry;
use growlerdb_core::{IndexDefinition, ResolvedIndex, SourceField, SourceSchema, SourceType};
use growlerdb_engine::{
    mint_session_jwt, rest, ControlPlaneService, JwtAuthenticator, RbacPolicy,
    BUILTIN_SESSION_AUDIENCE, BUILTIN_SESSION_ISSUER, BUILTIN_SESSION_TTL_SECS,
};
use growlerdb_proto::ControlPlaneServer;
use growlerdb_source::IcebergConfig;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tower::ServiceExt;

fn resolved(name: &str) -> ResolvedIndex {
    let src = SourceSchema::new(
        vec![SourceField::new("id", SourceType::String)],
        vec![],
        vec!["id".into()],
    );
    IndexDefinition::from_yaml(&format!(
        "name: {name}\nsource: {{ iceberg: {{ catalog: g, table: g.{name} }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD }} ] }}\n",
    ))
    .unwrap()
    .resolve(&src)
    .unwrap()
}

/// A `control_router` wired to a live Control Plane seeded with `seed` indexes.
async fn control_app(seed: &[&str], root: &std::path::Path) -> Router {
    let registry = Arc::new(Registry::open(root.join("registry.json")).unwrap());
    for name in seed {
        registry.create(resolved(name)).unwrap();
    }
    let svc = ControlPlaneService::new(registry, IcebergConfig::local());

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(ControlPlaneServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );

    let endpoint = format!("http://{addr}");
    for _ in 0..50 {
        if let Ok(client) =
            growlerdb_proto::service_token::connect(endpoint.clone(), None, None).await
        {
            return rest::control_router(client);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("control plane never came up");
}

/// Shared deployment secret the test authenticator validates bearer tokens against. The control
/// plane must verify identity itself over the REST→gRPC hop — caller-asserted `x-growlerdb-*`
/// headers are no longer forwarded — so tests carry identity in a signed bearer, not a header.
const TEST_SECRET: &[u8] = b"control-rest-test-secret";

/// A signed session bearer for `subject` carrying `roles` — the verified identity the control
/// plane's authenticator stamps into request metadata.
fn bearer(subject: &str, roles: &[&str]) -> String {
    let roles: Vec<String> = roles.iter().map(|r| r.to_string()).collect();
    let jwt = mint_session_jwt(
        TEST_SECRET,
        subject,
        &roles,
        &[],
        BUILTIN_SESSION_ISSUER,
        BUILTIN_SESSION_AUDIENCE,
        BUILTIN_SESSION_TTL_SECS,
        None,
    )
    .unwrap();
    format!("Bearer {jwt}")
}

/// A `control_router` wired to a Control Plane that authenticates the forwarded bearer itself
/// (`with_authn`) — the sound model, since nothing between the REST handler and the control plane
/// stamps identity. `enforce_rbac` also installs the RBAC policy for admin-gated routes.
async fn authn_control_app(root: &std::path::Path, enforce_rbac: bool) -> Router {
    let registry = Arc::new(Registry::open(root.join("registry.json")).unwrap());
    let authn = Arc::new(JwtAuthenticator::from_hs256_secret(
        TEST_SECRET,
        BUILTIN_SESSION_ISSUER,
        BUILTIN_SESSION_AUDIENCE,
    ));
    let svc = if enforce_rbac {
        ControlPlaneService::with_auth(
            registry,
            IcebergConfig::local(),
            Arc::new(RbacPolicy::with_default_roles()),
        )
    } else {
        ControlPlaneService::new(registry, IcebergConfig::local())
    }
    .with_authn(authn);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(ControlPlaneServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let endpoint = format!("http://{addr}");
    for _ in 0..50 {
        if let Ok(client) =
            growlerdb_proto::service_token::connect(endpoint.clone(), None, None).await
        {
            return rest::control_router(client);
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    panic!("control plane never came up");
}

fn get(uri: &str) -> HttpRequest<Body> {
    HttpRequest::builder().uri(uri).body(Body::empty()).unwrap()
}
fn delete(uri: &str) -> HttpRequest<Body> {
    HttpRequest::builder()
        .method("DELETE")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}
async fn text(resp: axum::response::Response) -> String {
    String::from_utf8(to_bytes(resp.into_body(), 1 << 20).await.unwrap().to_vec()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn list_get_drop_indexes_over_rest() {
    let tmp = tempfile::tempdir().unwrap();
    let app = control_app(&["docs", "logs"], tmp.path()).await;

    // List → both indexes.
    let resp = app.clone().oneshot(get("/v1/indexes")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    assert!(body.contains("docs") && body.contains("logs"));

    // Get one → routing config.
    let resp = app.clone().oneshot(get("/v1/indexes/docs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    assert!(body.contains("\"name\":\"docs\"") && body.contains("\"routing\""));

    // Unknown index → 404.
    let resp = app.clone().oneshot(get("/v1/indexes/nope")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);

    // Drop → 204, then it's gone from the list.
    let resp = app
        .clone()
        .oneshot(delete("/v1/indexes/logs"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = app.clone().oneshot(get("/v1/indexes")).await.unwrap();
    let body = text(resp).await;
    assert!(body.contains("docs") && !body.contains("logs"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingestion_status_over_rest() {
    let tmp = tempfile::tempdir().unwrap();
    let app = control_app(&["docs"], tmp.path()).await;

    // All-indexes ingestion status: `docs` appears with its source binding. No shards are
    // assigned here, and the local-dev catalog isn't up, so source freshness is null — but the
    // surface still resolves (the screen renders "—" for unknowns) rather than erroring.
    let resp = app.clone().oneshot(get("/v1/ingestion")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    assert!(body.contains("\"name\":\"docs\""));
    assert!(body.contains("\"source_table\":\"g.docs\""));
    assert!(body.contains("\"source_snapshot_id\":null"));

    // Single-index form.
    let resp = app
        .clone()
        .oneshot(get("/v1/ingestion/docs"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    assert!(body.contains("\"name\":\"docs\""));
}

#[tokio::test]
async fn saved_queries_persist_per_subject_with_sharing() {
    // Saved searches are scoped to the verified subject; `shared` makes one workspace-visible.
    let tmp = tempfile::tempdir().unwrap();
    let app = authn_control_app(tmp.path(), false).await;

    let list = |who: &str| {
        HttpRequest::builder()
            .uri("/v1/saved-queries")
            .header("authorization", bearer(who, &[]))
            .body(Body::empty())
            .unwrap()
    };

    // Alice creates a saved query (server stamps id + owner).
    let create = HttpRequest::builder()
        .method("POST")
        .uri("/v1/saved-queries")
        .header("content-type", "application/json")
        .header("authorization", bearer("alice", &[]))
        .body(Body::from(
            r#"{"name":"critical","query":"status:critical","state":"{\"index\":\"telemetry\"}"}"#,
        ))
        .unwrap();
    let resp = app.clone().oneshot(create).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let created: serde_json::Value = serde_json::from_str(&text(resp).await).unwrap();
    let id = created["id"].as_str().unwrap().to_string();
    assert_eq!(created["owner"], "alice");
    assert!(!id.is_empty());

    // Alice sees it; Bob does not (it isn't shared).
    assert!(text(app.clone().oneshot(list("alice")).await.unwrap())
        .await
        .contains("critical"));
    assert!(!text(app.clone().oneshot(list("bob")).await.unwrap())
        .await
        .contains("critical"));

    // Bob cannot delete Alice's query.
    let bob_del = HttpRequest::builder()
        .method("DELETE")
        .uri(format!("/v1/saved-queries/{id}"))
        .header("authorization", bearer("bob", &[]))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(bob_del).await.unwrap().status(),
        StatusCode::NOT_FOUND
    );

    // Alice shares it (PUT) → Bob now sees it.
    let share = HttpRequest::builder()
        .method("PUT")
        .uri(format!("/v1/saved-queries/{id}"))
        .header("content-type", "application/json")
        .header("authorization", bearer("alice", &[]))
        .body(Body::from(
            r#"{"name":"critical","query":"status:critical","shared":true}"#,
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(share).await.unwrap().status(),
        StatusCode::OK
    );
    assert!(text(app.clone().oneshot(list("bob")).await.unwrap())
        .await
        .contains("critical"));

    // Alice deletes it.
    let del = HttpRequest::builder()
        .method("DELETE")
        .uri(format!("/v1/saved-queries/{id}"))
        .header("authorization", bearer("alice", &[]))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(del).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
    assert!(!text(app.clone().oneshot(list("alice")).await.unwrap())
        .await
        .contains("critical"));
}

#[tokio::test]
async fn user_management_is_admin_gated_and_bindings_merge() {
    // Only admins manage users, and a granted role takes effect on the subject's next call.
    let tmp = tempfile::tempdir().unwrap();
    let app = authn_control_app(tmp.path(), true).await;

    let set_roles = |caller: &str, caller_roles: &[&str], subject: &str, body: &str| {
        HttpRequest::builder()
            .method("PUT")
            .uri(format!("/v1/users/{subject}/roles"))
            .header("content-type", "application/json")
            .header("authorization", bearer(caller, caller_roles))
            .body(Body::from(body.to_string()))
            .unwrap()
    };

    // A reader cannot manage users → 403 (before any mutation).
    let resp = app
        .clone()
        .oneshot(set_roles(
            "rita",
            &["reader"],
            "bob",
            r#"{"roles":["operator"]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    // An admin can.
    let resp = app
        .clone()
        .oneshot(set_roles(
            "ada",
            &["admin"],
            "bob",
            r#"{"roles":["operator"]}"#,
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // GET /v1/users (admin) shows the new binding.
    let list = HttpRequest::builder()
        .uri("/v1/users")
        .header("authorization", bearer("ada", &["admin"]))
        .body(Body::empty())
        .unwrap();
    let body = text(app.clone().oneshot(list).await.unwrap()).await;
    assert!(body.contains("bob") && body.contains("operator"));

    // Binding merge: grant carol `admin`; now carol — with a verified token carrying NO roles —
    // can manage users on her next call (the local binding grants admin).
    let grant = set_roles("ada", &["admin"], "carol", r#"{"roles":["admin"]}"#);
    assert_eq!(
        app.clone().oneshot(grant).await.unwrap().status(),
        StatusCode::OK
    );
    let carol_acts = HttpRequest::builder()
        .method("PUT")
        .uri("/v1/users/dave/roles")
        .header("content-type", "application/json")
        .header("authorization", bearer("carol", &[])) // no roles in the token
        .body(Body::from(r#"{"roles":["reader"]}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(carol_acts).await.unwrap().status(),
        StatusCode::OK
    );

    // GET /v1/roles lists the assignable catalog.
    let roles = HttpRequest::builder()
        .uri("/v1/roles")
        .header("authorization", bearer("ada", &["admin"]))
        .body(Body::empty())
        .unwrap();
    let body = text(app.clone().oneshot(roles).await.unwrap()).await;
    assert!(body.contains("admin") && body.contains("reader") && body.contains("operator"));
}

#[tokio::test]
async fn api_tokens_create_list_revoke_admin_gated() {
    // Tokens are admin-gated; the secret is returned once and never listed.
    let tmp = tempfile::tempdir().unwrap();
    let app = authn_control_app(tmp.path(), true).await;

    // A reader cannot create tokens.
    let reader = HttpRequest::builder()
        .method("POST")
        .uri("/v1/tokens")
        .header("content-type", "application/json")
        .header("authorization", bearer("rita", &["reader"]))
        .body(Body::from(r#"{"label":"ci","roles":["reader"]}"#))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(reader).await.unwrap().status(),
        StatusCode::FORBIDDEN
    );

    // An admin creates a token → the secret is returned exactly once.
    let create = HttpRequest::builder()
        .method("POST")
        .uri("/v1/tokens")
        .header("content-type", "application/json")
        .header("authorization", bearer("ada", &["admin"]))
        .body(Body::from(r#"{"label":"ci-pipeline","roles":["reader"]}"#))
        .unwrap();
    let resp = app.clone().oneshot(create).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let created: serde_json::Value = serde_json::from_str(&text(resp).await).unwrap();
    let secret = created["secret"].as_str().unwrap().to_string();
    let id = created["token"]["id"].as_str().unwrap().to_string();
    assert!(secret.starts_with("gdb_live_"));

    // List (admin) returns metadata only — label + prefix, never the secret or hash.
    let list = HttpRequest::builder()
        .uri("/v1/tokens")
        .header("authorization", bearer("ada", &["admin"]))
        .body(Body::empty())
        .unwrap();
    let body = text(app.clone().oneshot(list).await.unwrap()).await;
    assert!(body.contains("ci-pipeline") && body.contains("gdb_live_"));
    assert!(
        !body.contains(&secret),
        "the raw secret must never be listed"
    );

    // Revoke → 204.
    let revoke = HttpRequest::builder()
        .method("DELETE")
        .uri(format!("/v1/tokens/{id}"))
        .header("authorization", bearer("ada", &["admin"]))
        .body(Body::empty())
        .unwrap();
    assert_eq!(
        app.clone().oneshot(revoke).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn activity_log_records_lifecycle_events() {
    // A lifecycle mutation (alias swap) is recorded to the index's activity log + readable.
    let tmp = tempfile::tempdir().unwrap();
    let app = control_app(&["docs"], tmp.path()).await;

    // Point an alias at `docs` → records an `alias.swapped` event on `docs`.
    let set = HttpRequest::builder()
        .method("POST")
        .uri("/v1/aliases")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"alias":"live","targets":["docs"]}"#))
        .unwrap();
    let status = app.clone().oneshot(set).await.unwrap().status();
    assert!(
        status.is_success() || status == StatusCode::NO_CONTENT,
        "{status}"
    );

    // The activity read returns the event, newest-first.
    let read = HttpRequest::builder()
        .method("POST")
        .uri("/v1/index:activity")
        .header("content-type", "application/json")
        .body(Body::from(r#"{"index":"docs"}"#))
        .unwrap();
    let resp = app.clone().oneshot(read).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    assert!(body.contains("alias.swapped"), "{body}");
    assert!(body.contains("live"), "{body}");
}
