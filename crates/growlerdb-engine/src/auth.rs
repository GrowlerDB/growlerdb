//! The **auth seam** ([Engine API], task-19): a pluggable authorization hook the
//! gRPC/REST services consult before serving a request, plus the request context it
//! inspects. The default [`AllowAll`] permits everything — real AuthN/AuthZ
//! (OIDC/JWT/RBAC + tenant scoping) is **M4**; this milestone only stands up the seam
//! so the services are written against it and a deployment can drop in policy later.
//!
//! [Engine API]: ../../../design/01-engine-api.md

use std::sync::Arc;

use growlerdb_proto::to_status;
use growlerdb_proto::v1::Error as WireError;
use tonic::{Code, Request, Status};

/// Metadata key carrying the caller principal (M2: opaque; an OIDC/JWT `sub` in M4). The
/// [AuthN layer](crate::authn) stamps the *verified* principal here, overriding any
/// caller-asserted value, so this seam always reads a trusted identity once AuthN is on.
pub(crate) const PRINCIPAL_KEY: &str = "x-growlerdb-principal";
/// Metadata key carrying the tenant a request is scoped to (drives tenant filtering).
pub(crate) const TENANT_KEY: &str = "x-growlerdb-tenant";
/// Metadata key carrying the caller's verified roles (comma-separated). The
/// [AuthN layer](crate::authn) stamps these from validated token/key claims; an
/// [RBAC policy](crate::rbac) maps them to operation scopes.
pub(crate) const ROLES_KEY: &str = "x-growlerdb-roles";

/// What an [`AuthHook`] inspects: the RPC method and the principal/tenant the
/// transport extracted from request metadata. `tenant` is the seam future query
/// execution will AND into the filter (tenant isolation); unused under [`AllowAll`].
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
}

/// The default no-op hook: permits every request. Enforcement is M4.
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
/// RPC, before consuming the request.
pub fn authorize<T>(
    auth: &SharedAuth,
    method: &'static str,
    request: &Request<T>,
) -> Result<(), Status> {
    let meta = request.metadata();
    let get = |key: &str| {
        meta.get(key)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let roles = get(ROLES_KEY)
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|r| !r.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let ctx = AuthContext {
        method,
        principal: get(PRINCIPAL_KEY),
        tenant: get(TENANT_KEY),
        roles,
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
/// the mandatory per-read filter (task-38).
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
