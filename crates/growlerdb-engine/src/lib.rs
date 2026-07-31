//! `growlerdb-engine` — search execution, primary-key hydration, and the engine façade the CLI and
//! gRPC/REST server drive.
//!
//! The [`gateway`]/[`node`] seam lets the Engine API route through a `dyn Node` that is either
//! in-process (embedded) or a gRPC client (distributed).

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
pub mod pool_routing;
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
pub use node::{FailoverNode, LocalNode, Node, RemoteNode};
pub use opensearch::opensearch_router;
pub use rbac::{scope_for_method, RbacPolicy, Scope};
pub use remap::{remap_shard, remap_tick, RemapOutcome, RemapState};
pub use search_service::{heavy_reads_cap, IndexHeavyShare, SearchService};
/// Serialize every test that mutates a process-global env var (`GROWLERDB_*_API_KEY`, ...).
/// `set_var`/`remove_var` are process-wide, so env-touching tests across modules must share ONE
/// crate-level lock or they race under `cargo test`'s parallelism. Every such test takes this guard.
#[cfg(test)]
pub(crate) fn env_guard() -> std::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}

pub use pool_routing::{
    PoolAdminService, PoolLookupService, PoolSearchService, PoolSuggestService, PoolWriteService,
    SharedAdminIndexes, SharedHashWriteIndexes, SharedHashWriteUnits, SharedIndexKinds,
    SharedLookupIndexes, SharedSearchIndexes, SharedSuggestIndexes, SharedWriteIndexes,
};
pub use service_auth::{
    intercept as intercept_service_token, layer as service_token_layer, ServiceTokenAuth,
};
pub use shard_handle::ShardHandle;
pub use suggest_service::SuggestService;
pub use topology::{shard_primaries, ShardTopologyError};
pub use windowed_ingest::{OnNewWindow, PrimaryFence, WindowSeed, WindowedWriteService};
pub use windowed_routing::{
    is_not_primary, is_unit_not_served, not_primary, unit_not_served, ShardNode,
    SharedAdminWindows, SharedLookupWindows, SharedSearchWindows, SharedSuggestWindows, WindowNode,
    WindowedAdminService, WindowedLookupService, WindowedSearchService, WindowedSuggestService,
    NOT_PRIMARY, UNIT_NOT_SERVED,
};
pub use write_service::WriteService;

/// Crate version, from Cargo metadata.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// A minimal non-variant `docs` [`ResolvedIndex`](growlerdb_core::ResolvedIndex) for tests.
/// `LookupService` consults its resolved index only for the variant hydration fork
/// (`has_variant_field()`), so any non-variant index serves the non-variant test paths.
#[cfg(test)]
pub(crate) fn test_docs_resolved() -> growlerdb_core::ResolvedIndex {
    growlerdb_core::IndexDefinition::from_yaml(
        "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nmapping: { selection: EXPLICIT, fields: [{ path: id, type: KEYWORD }] }\n",
    )
    .unwrap()
    .resolve(&growlerdb_core::SourceSchema::new(
        vec![growlerdb_core::SourceField::new(
            "id",
            growlerdb_core::SourceType::String,
        )],
        vec![],
        vec!["id".into()],
    ))
    .unwrap()
}
