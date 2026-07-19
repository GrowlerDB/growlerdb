//! The **control-plane REST surface**: the gateway's `/v1/indexes` REST routes proxy
//! to the Control Plane over gRPC. Stand up a real `ControlPlaneService` (over a temp registry)
//! on a tonic server, point the REST `control_router` at it via a gRPC client, and drive the
//! list/get/drop lifecycle over HTTP. `CreateIndex`/`DescribeSource` need a live Iceberg source,
//! so only their validation/error paths run in default CI (inline in `control_service.rs`);
//! their happy paths currently have no automated coverage and are exercised manually against
//! the Compose stack.

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
    authn_control_app_seeded(root, enforce_rbac, &[]).await
}

/// [`authn_control_app`] with pre-registered indexes, for RBAC tests over index lifecycle routes.
async fn authn_control_app_seeded(
    root: &std::path::Path,
    enforce_rbac: bool,
    seed: &[&str],
) -> Router {
    let registry = Arc::new(Registry::open(root.join("registry.json")).unwrap());
    for name in seed {
        registry.create(resolved(name)).unwrap();
    }
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

/// The alias REST routes' read + delete halves (`POST /v1/aliases` is covered by the activity
/// test): list reflects a swap, delete removes it, and a missing alias 404s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aliases_list_and_delete_over_rest() {
    let tmp = tempfile::tempdir().unwrap();
    let app = control_app(&["events_v1"], tmp.path()).await;

    let set = HttpRequest::builder()
        .method("POST")
        .uri("/v1/aliases")
        .header("content-type", "application/json")
        .body(Body::from(
            r#"{"alias":"events","targets":["events_v1"]}"#.to_string(),
        ))
        .unwrap();
    assert_eq!(
        app.clone().oneshot(set).await.unwrap().status(),
        StatusCode::NO_CONTENT
    );

    // GET lists the swap.
    let resp = app.clone().oneshot(get("/v1/aliases")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    assert!(
        body.contains("\"events\"") && body.contains("events_v1"),
        "{body}"
    );

    // DELETE removes it; the list is empty; deleting again 404s.
    let resp = app
        .clone()
        .oneshot(delete("/v1/aliases/events"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    let resp = app.clone().oneshot(get("/v1/aliases")).await.unwrap();
    let body = text(resp).await;
    assert!(!body.contains("events_v1"), "{body}");
    let resp = app
        .clone()
        .oneshot(delete("/v1/aliases/events"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// `GET /v1/license` on an unlicensed (community) deployment: 200 with the honest state — the
/// console's License panel reads exactly this.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn license_over_rest_reports_the_unlicensed_state() {
    let tmp = tempfile::tempdir().unwrap();
    let app = control_app(&[], tmp.path()).await;
    let resp = app.clone().oneshot(get("/v1/license")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    assert!(body.contains("\"licensed\":false"), "{body}");
    assert!(body.contains("max_nodes"), "{body}");
}

/// `POST /v1/login` — the REAL REST handler (the MCP suite exercises login only against a mock):
/// a good credential mints a session token, a bad one is 401, and both ride the same route the
/// console uses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn login_over_rest_mints_a_token_and_rejects_bad_credentials() {
    let tmp = tempfile::tempdir().unwrap();
    // A CP configured for builtin login: session secret + one seeded credential.
    let registry = Arc::new(Registry::open(tmp.path().join("registry.json")).unwrap());
    registry.set_credential("alice", "pw").unwrap();
    registry
        .set_user_roles("alice", vec!["admin".to_string()])
        .unwrap();
    let svc = ControlPlaneService::new(registry, IcebergConfig::local())
        .with_session_secret(TEST_SECRET.to_vec());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(ControlPlaneServer::new(svc))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    let endpoint = format!("http://{addr}");
    let mut app = None;
    for _ in 0..50 {
        if let Ok(client) =
            growlerdb_proto::service_token::connect(endpoint.clone(), None, None).await
        {
            app = Some(rest::control_router(client));
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    let app = app.expect("control plane never came up");

    let login = |body: &str| {
        HttpRequest::builder()
            .method("POST")
            .uri("/v1/login")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    };

    // Good credential → 200 with a token + roles.
    let resp = app
        .clone()
        .oneshot(login(r#"{"username":"alice","password":"pw"}"#))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = text(resp).await;
    assert!(body.contains("\"token\""), "{body}");
    assert!(body.contains("admin"), "{body}");

    // Wrong password → 401, no token.
    let resp = app
        .clone()
        .oneshot(login(r#"{"username":"alice","password":"nope"}"#))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

/// RBAC over the index lifecycle REST routes: a reader can look but not drop an index or swap an
/// alias (403 before any mutation); an admin can. (The users/tokens routes have their own
/// admin-gate tests; this closes the same gap for `DropIndex`/`SetAlias`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn index_drop_and_alias_swap_are_admin_gated_over_rest() {
    let tmp = tempfile::tempdir().unwrap();
    let app = authn_control_app_seeded(tmp.path(), true, &["docs"]).await;

    let with_auth = |req: axum::http::request::Builder, who: &str, roles: &[&str]| {
        req.header("authorization", bearer(who, roles))
    };
    let drop_req = |who: &str, roles: &[&str]| {
        with_auth(
            HttpRequest::builder()
                .method("DELETE")
                .uri("/v1/indexes/docs"),
            who,
            roles,
        )
        .body(Body::empty())
        .unwrap()
    };
    let alias_req = |who: &str, roles: &[&str]| {
        with_auth(
            HttpRequest::builder()
                .method("POST")
                .uri("/v1/aliases")
                .header("content-type", "application/json"),
            who,
            roles,
        )
        .body(Body::from(
            r#"{"alias":"d","targets":["docs"]}"#.to_string(),
        ))
        .unwrap()
    };

    // Reader: reads work, mutations are 403 and change nothing.
    let list = with_auth(
        HttpRequest::builder().uri("/v1/indexes"),
        "rita",
        &["reader"],
    )
    .body(Body::empty())
    .unwrap();
    assert_eq!(
        app.clone().oneshot(list).await.unwrap().status(),
        StatusCode::OK
    );
    assert_eq!(
        app.clone()
            .oneshot(drop_req("rita", &["reader"]))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        app.clone()
            .oneshot(alias_req("rita", &["reader"]))
            .await
            .unwrap()
            .status(),
        StatusCode::FORBIDDEN
    );
    let list = with_auth(
        HttpRequest::builder().uri("/v1/indexes"),
        "rita",
        &["reader"],
    )
    .body(Body::empty())
    .unwrap();
    let body = text(app.clone().oneshot(list).await.unwrap()).await;
    assert!(
        body.contains("docs"),
        "the reader's denied drop mutated nothing: {body}"
    );

    // Admin: both mutations succeed.
    assert_eq!(
        app.clone()
            .oneshot(alias_req("ada", &["admin"]))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        app.clone()
            .oneshot(drop_req("ada", &["admin"]))
            .await
            .unwrap()
            .status(),
        StatusCode::NO_CONTENT
    );
}
