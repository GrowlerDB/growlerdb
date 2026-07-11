//! The **auth seam**: a pluggable authorization hook the gRPC/REST services consult
//! before serving a request, plus the request context it inspects. The default
//! [`AllowAll`] permits everything so the services are written against the seam and a
//! deployment can drop in policy (OIDC/JWT/RBAC + tenant scoping) later.

use std::sync::Arc;

use growlerdb_proto::to_status;
use growlerdb_proto::v1::Error as WireError;
use tonic::{Code, Request, Status};

/// Metadata key carrying the caller principal (an OIDC/JWT `sub`). The
/// [AuthN layer](crate::authn) stamps the *verified* principal here, overriding any
/// caller-asserted value, so this seam always reads a trusted identity once AuthN is on.
pub(crate) const PRINCIPAL_KEY: &str = "x-growlerdb-principal";
/// Metadata key carrying the tenant a request is scoped to (drives tenant filtering).
pub(crate) const TENANT_KEY: &str = "x-growlerdb-tenant";
/// Metadata key carrying the caller's verified roles (comma-separated). The
/// [AuthN layer](crate::authn) stamps these from validated token/key claims; an
/// [RBAC policy](crate::rbac) maps them to operation scopes.
pub(crate) const ROLES_KEY: &str = "x-growlerdb-roles";
/// Metadata key carrying the caller's **index allowlist** (comma-separated) for per-index RBAC.
/// The [AuthN layer](crate::authn) stamps this from a validated token's `indexes` claim;
/// when non-empty, an [RBAC policy](crate::rbac) restricts the caller to those indexes, so a token
/// scoped to index A cannot read index B. Empty/absent = unrestricted across indexes.
pub(crate) const INDEXES_KEY: &str = "x-growlerdb-indexes";

/// What an [`AuthHook`] inspects: the RPC method and the principal/tenant the
/// transport extracted from request metadata. `tenant` is the seam query execution
/// ANDs into the filter (tenant isolation); unused under [`AllowAll`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuthContext {
    /// The RPC method being called (e.g. `"Search"`, `"GetByKey"`).
    pub method: &'static str,
    /// Authenticated principal, if the request carried one.
    pub principal: Option<String>,
    /// Tenant the request is scoped to, if any.
    pub tenant: Option<String>,
    /// The caller's verified roles (empty if none) — what an [RBAC policy](crate::rbac)
    /// maps to operation scopes.
    pub roles: Vec<String>,
    /// The **resolved target index** of the request, if the caller is index-scoped.
    /// `Some` when the [`Gateway`](crate::gateway::Gateway) resolved the request's
    /// `index` field to a served index before authorizing; `None` for index-agnostic calls (cluster
    /// ops, or an un-routed call). A [per-index policy](crate::rbac) uses this to deny a token valid
    /// for one index from reading another.
    pub index: Option<String>,
    /// The caller's **index allowlist** from the token's `indexes` claim. When non-empty
    /// the caller may only operate on these indexes; empty = unrestricted. Enforced by
    /// an [RBAC policy](crate::rbac) against `index`.
    pub allowed_indexes: Vec<String>,
}

/// A denial returned by an [`AuthHook`] — carries a human-readable reason surfaced to
/// the caller as `PermissionDenied`.
#[derive(Debug, Clone)]
pub struct AuthDenied {
    /// Why the request was rejected.
    pub reason: String,
}

impl AuthDenied {
    /// Deny with `reason`.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

/// A pluggable authorization hook. `authorize` returns `Ok(())` to allow a request or
/// `Err(AuthDenied)` to reject it. Implementations must be cheap and side-effect-free
/// (run on every RPC). The default is [`AllowAll`].
pub trait AuthHook: Send + Sync {
    /// Authorize a request described by `ctx`.
    fn authorize(&self, ctx: &AuthContext) -> Result<(), AuthDenied>;

    /// Whether this hook makes decisions from caller identity (roles/principal). When `true`, a
    /// service must have a real authenticator installed, else the identity it enforces against is
    /// caller-asserted and forgeable. Defaults to `false` (the no-op [`AllowAll`]).
    fn is_authorizing(&self) -> bool {
        false
    }
}

/// The default no-op hook: permits every request.
#[derive(Debug, Clone, Default)]
pub struct AllowAll;

impl AuthHook for AllowAll {
    fn authorize(&self, _ctx: &AuthContext) -> Result<(), AuthDenied> {
        Ok(())
    }
}

/// A shared, type-erased auth hook the services hold. [`default_auth`] yields the
/// no-op [`AllowAll`].
pub type SharedAuth = Arc<dyn AuthHook>;

/// The default shared hook ([`AllowAll`]) used when a service is built without an
/// explicit policy.
pub fn default_auth() -> SharedAuth {
    Arc::new(AllowAll)
}

/// Build the [`AuthContext`] for `method` from a request's metadata, then run `auth`.
/// Maps a denial to a `PermissionDenied` status. Services call this at the top of each
/// RPC, before consuming the request. Index-agnostic (`ctx.index = None`); the
/// [`Gateway`](crate::gateway::Gateway) uses [`authorize_index`] to carry the resolved target index.
pub fn authorize<T>(
    auth: &SharedAuth,
    method: &'static str,
    request: &Request<T>,
) -> Result<(), Status> {
    authorize_index(auth, method, None, request)
}

/// As [`authorize`], but carrying the request's **resolved target index** (per-index RBAC):
/// the [`Gateway`](crate::gateway::Gateway) resolves a read/write's `index` field to a served index,
/// then authorizes against it so a [per-index policy](crate::rbac) can deny a token scoped to one
/// index from operating on another. `index = None` behaves exactly like [`authorize`].
pub fn authorize_index<T>(
    auth: &SharedAuth,
    method: &'static str,
    index: Option<&str>,
    request: &Request<T>,
) -> Result<(), Status> {
    let meta = request.metadata();
    let get = |key: &str| {
        meta.get(key)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let split_csv = |key: &str| -> Vec<String> {
        get(key)
            .map(|s| {
                s.split(',')
                    .map(str::trim)
                    .filter(|r| !r.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };
    let roles = split_csv(ROLES_KEY);
    let allowed_indexes = split_csv(INDEXES_KEY);
    let ctx = AuthContext {
        method,
        principal: get(PRINCIPAL_KEY),
        tenant: get(TENANT_KEY),
        roles,
        index: index.map(str::to_string),
        allowed_indexes,
    };
    auth.authorize(&ctx).map_err(|denied| {
        to_status(
            Code::PermissionDenied,
            WireError::new("PERMISSION_DENIED", denied.reason),
        )
    })
}

/// The verified tenant claim a request carries (the `x-growlerdb-tenant` the
/// [AuthN layer](crate::authn) stamps), or `None`. Drives [tenant scoping](crate::rbac) —
/// the mandatory per-read filter.
pub(crate) fn tenant_of<T>(request: &Request<T>) -> Option<String> {
    request
        .metadata()
        .get(TENANT_KEY)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A hook that denies a named tenant — exercises the seam end to end.
    struct DenyTenant(&'static str);
    impl AuthHook for DenyTenant {
        fn authorize(&self, ctx: &AuthContext) -> Result<(), AuthDenied> {
            match &ctx.tenant {
                Some(t) if t == self.0 => Err(AuthDenied::new(format!("tenant `{t}` blocked"))),
                _ => Ok(()),
            }
        }
    }

    fn request_with(principal: Option<&str>, tenant: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(p) = principal {
            req.metadata_mut().insert(PRINCIPAL_KEY, p.parse().unwrap());
        }
        if let Some(t) = tenant {
            req.metadata_mut().insert(TENANT_KEY, t.parse().unwrap());
        }
        req
    }

    #[test]
    fn allow_all_permits_everything() {
        let auth = default_auth();
        let req = request_with(Some("alice"), Some("acme"));
        assert!(authorize(&auth, "Search", &req).is_ok());
    }

    #[test]
    fn context_carries_principal_and_tenant_from_metadata() {
        use std::sync::Mutex;
        struct Capture(Arc<Mutex<AuthContext>>);
        impl AuthHook for Capture {
            fn authorize(&self, ctx: &AuthContext) -> Result<(), AuthDenied> {
                *self.0.lock().unwrap() = ctx.clone();
                Ok(())
            }
        }
        let seen = Arc::new(Mutex::new(AuthContext::default()));
        let auth: SharedAuth = Arc::new(Capture(seen.clone()));
        let req = request_with(Some("bob"), Some("globex"));
        authorize(&auth, "GetByKey", &req).unwrap();
        let ctx = seen.lock().unwrap().clone();
        assert_eq!(ctx.method, "GetByKey");
        assert_eq!(ctx.principal.as_deref(), Some("bob"));
        assert_eq!(ctx.tenant.as_deref(), Some("globex"));
    }

    #[test]
    fn a_denying_hook_maps_to_permission_denied() {
        let auth: SharedAuth = Arc::new(DenyTenant("blocked"));
        // An allowed tenant passes.
        assert!(authorize(&auth, "Search", &request_with(None, Some("ok"))).is_ok());
        // The blocked tenant is rejected with PermissionDenied.
        let err = authorize(&auth, "Search", &request_with(None, Some("blocked"))).unwrap_err();
        assert_eq!(err.code(), Code::PermissionDenied);
    }
}
