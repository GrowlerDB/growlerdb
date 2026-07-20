//! `growlerdb-engine` — search execution, primary-key hydration, and the engine façade
//! that the CLI (and, later, the gRPC/REST server) drive.
//!
//! The distributed split (M3 Phase B1) is taking shape: the [`gateway`]/[`node`] seam lets
//! the Engine API route through a `dyn Node` that is either in-process (embedded) or, in a
//! later slice, a gRPC client (distributed).

pub mod admin_service;
pub mod auth;
pub mod authn;
pub mod control_service;
pub mod engine;
pub mod error;
pub mod fence;
pub mod gateway;
pub mod gateway_grpc;
pub mod hydrate;
pub mod license;
pub mod lookup_service;
pub mod mcp_http;
pub mod node;
pub mod opensearch;
pub mod rbac;
pub mod remap;
pub mod rest;
pub mod search_service;
pub mod service_auth;
pub mod service_util;
pub mod shard_handle;
pub mod suggest_service;
pub mod tls;
pub mod topology;
pub mod windowed_ingest;
pub mod windowed_routing;
pub mod write_service;

pub use admin_service::AdminService;
pub use auth::{AllowAll, AuthContext, AuthDenied, AuthHook, SharedAuth};
pub use authn::{
    default_authn, hash_api_token, mint_api_token, mint_session_jwt, Anonymous, ApiKeyStore,
    Authenticator, AuthnError, ChainAuthenticator, ClaimMapping, JwksAuthenticator,
    JwtAuthenticator, KeyIdentity, RegistryTokenAuthenticator, SharedAuthn, Verified,
    BUILTIN_SESSION_AUDIENCE, BUILTIN_SESSION_ISSUER, BUILTIN_SESSION_TTL_SECS,
};
pub use control_service::ControlPlaneService;
pub use engine::{DriftReport, Engine, IndexOutcome, SearchOutcome, SyncOutcome};
pub use error::EngineError;
pub use fence::{ReindexFence, ReindexGuard};
pub use gateway::{Gateway, GatewayLimits, IndexRoute, RouteResolver, WindowRouting};
pub use hydrate::{apply_live_file_bitmap, get_by_key, resolve_locators};
pub use license::{License, LicenseError, FREE_NODE_LIMIT};
pub use lookup_service::LookupService;
pub use mcp_http::mcp_router;
pub use node::{LocalNode, Node, RemoteNode};
pub use opensearch::opensearch_router;
pub use rbac::{scope_for_method, RbacPolicy, Scope};
pub use remap::{remap_shard, remap_tick, RemapOutcome, RemapState};
pub use search_service::SearchService;
/// Serialize every test that mutates a process-global env var (`GROWLERDB_*_API_KEY`, ...).
/// `set_var`/`remove_var` are process-wide, so env-touching tests across modules race under
/// `cargo test`'s parallelism unless they share ONE crate-level lock (the same lesson the embed
/// crate learned; per-module locks re-introduce the cross-file race). Every such test takes this
/// guard — currently only `rest::tests::config_dto_has_no_secret_field`, but any future env test
/// must too.
#[cfg(test)]
pub(crate) fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

pub use service_auth::{
    intercept as intercept_service_token, layer as service_token_layer, ServiceTokenAuth,
};
pub use shard_handle::ShardHandle;
pub use suggest_service::SuggestService;
pub use topology::{shard_primaries, ShardTopologyError};
pub use windowed_ingest::{OnNewWindow, WindowSeed, WindowedWriteService};
pub use windowed_routing::{
    SharedAdminWindows, SharedLookupWindows, SharedSearchWindows, SharedSuggestWindows, WindowNode,
    WindowedAdminService, WindowedLookupService, WindowedSearchService, WindowedSuggestService,
};
pub use write_service::WriteService;

/// Crate version, from Cargo metadata.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
