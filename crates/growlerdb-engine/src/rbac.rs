//! **Control-plane RBAC**: a coarse, role-based [`AuthHook`](crate::auth::AuthHook)
//! that gates Engine operations by the caller's verified roles. Roles map to operation
//! **scopes**, each RPC method requires a scope, and a request is allowed iff one of the
//! caller's roles grants that scope.
//!
//! This is the *operation* tier ("what may you do"): coarse, about methods, not rows. Row- and
//! column-level access stays delegated to the lake (Polaris); tenant scoping
//! is enforced separately. The roles come from [authentication](crate::authn) — validated token
//! or API-key claims, never caller-asserted — and reach this hook via the request metadata that
//! the [`Gateway`](crate::gateway::Gateway) stamps and forwards.

use std::collections::{HashMap, HashSet};

use crate::auth::{AuthContext, AuthDenied, AuthHook};

/// A coarse class of Engine operations a role may be granted. Methods map to exactly one scope
/// ([`scope_for_method`]); roles are granted a set of scopes ([`RbacPolicy`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scope {
    /// Run queries: search, suggest, aggregate, key hydration.
    Search,
    /// Read index metadata/stats (describe).
    IndexRead,
    /// Ingest/mutate index contents (the connector write path).
    IndexWrite,
    /// Administer indexes: create / drop / alter / reindex.
    Admin,
    /// Operate the cluster: observability, shard/replica, connector management.
    Ops,
}

impl Scope {
    /// Stable lowercase name (`search`, `index.read`, …) for messages and config.
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Search => "search",
            Scope::IndexRead => "index.read",
            Scope::IndexWrite => "index.write",
            Scope::Admin => "admin",
            Scope::Ops => "ops",
        }
    }
}

impl std::fmt::Display for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The canonical role names an admin may assign via user management.
pub const ASSIGNABLE_ROLES: &[&str] = &["reader", "operator", "admin"];

/// The scope an RPC `method` requires, or `None` for an unrecognized method (which the policy
/// **denies** — fail closed, so a newly added method can't slip through ungated).
pub fn scope_for_method(method: &str) -> Option<Scope> {
    Some(match method {
        // Read/query surface — incl. explain, scrolling, and per-user saved queries:
        // any reader may run and manage these.
        "Search" | "Suggest" | "Aggregate" | "GetByKey" | "Explain" | "OpenPit" | "ClosePit"
        | "Export" | "ListSavedQueries" | "SaveSavedQuery" | "DeleteSavedQuery" => Scope::Search,
        // Index metadata / introspection / status reads + the assignable-role catalog (read-only).
        // `ListUsers` is NOT here: enumerating every subject + its role bindings is authorization-
        // topology disclosure, so it needs Admin, not any reader.
        "DescribeIndex" | "GetIndex" | "ListIndexes" | "ListAliases" | "DescribeSource"
        | "IngestionStatus" | "GetCheckpoint" | "ListRoles" | "ListActivity" => Scope::IndexRead,
        "Write" => Scope::IndexWrite,
        // Administer indexes + manage user role bindings + API tokens + list users.
        "CreateIndex" | "DropIndex" | "AlterIndex" | "ReindexIndex" | "SetAlias" | "DropAlias"
        | "SetUserRoles" | "CreateToken" | "ListTokens" | "RevokeToken" | "ListUsers" => {
            Scope::Admin
        }
        // Cluster operations: reshard/bucket moves + node self-registration + CP-driven windowed
        // placement (node heartbeat + window-owner resolution).
        "PlanReshard"
        | "ApplyReshard"
        | "MoveBucket"
        | "RegisterServedIndex"
        | "RegisterNode"
        | "ResolveWindowOwner" => Scope::Ops,
        _ => return None,
    })
}

/// A role-based authorization policy: each role grants a set of [`Scope`]s. Authorizes a
/// request when one of the caller's roles grants the scope its method requires.
#[derive(Debug, Clone, Default)]
pub struct RbacPolicy {
    role_scopes: HashMap<String, HashSet<Scope>>,
}

impl RbacPolicy {
    /// An empty policy: every authenticated call is denied until roles are granted scopes.
    pub fn new() -> Self {
        Self::default()
    }

    /// The default role catalog. The **canonical** console roles are:
    /// - `reader` — query + read index metadata (Search, IndexRead)
    /// - `operator` — reader + cluster ops (Search, IndexRead, Ops)
    /// - `admin` — full control: every scope
    ///
    /// Legacy roles remain as aliases so existing token bindings keep working:
    /// - `viewer` ≡ `reader`
    /// - `index-admin` — viewer + write + administer indexes
    /// - `service` — internal components: every scope (≡ `admin`)
    pub fn with_default_roles() -> Self {
        use Scope::*;
        Self::new()
            // Canonical roles.
            .grant("reader", [Search, IndexRead])
            .grant("operator", [Search, IndexRead, Ops])
            .grant("admin", [Search, IndexRead, IndexWrite, Admin, Ops])
            // Legacy aliases — kept for backward compatibility.
            .grant("viewer", [Search, IndexRead])
            .grant("index-admin", [Search, IndexRead, IndexWrite, Admin])
            .grant("service", [Search, IndexRead, IndexWrite, Admin, Ops])
    }

    /// Grant `role` the given `scopes` (additive; chainable).
    pub fn grant(mut self, role: &str, scopes: impl IntoIterator<Item = Scope>) -> Self {
        self.role_scopes
            .entry(role.to_string())
            .or_default()
            .extend(scopes);
        self
    }

    /// Whether any of `roles` grants `scope`.
    fn grants(&self, roles: &[String], scope: Scope) -> bool {
        roles
            .iter()
            .any(|r| self.role_scopes.get(r).is_some_and(|s| s.contains(&scope)))
    }

    /// The scopes a single `role` grants (empty if the role is unknown).
    fn scopes_of(&self, role: &str) -> HashSet<Scope> {
        self.role_scopes.get(role).cloned().unwrap_or_default()
    }

    /// The union of scopes across `roles` — the caller's effective scope set.
    fn effective_scopes(&self, roles: &[String]) -> HashSet<Scope> {
        roles
            .iter()
            .filter_map(|r| self.role_scopes.get(r))
            .flatten()
            .copied()
            .collect()
    }
}

/// Validate that a caller holding `caller_roles` may assign/mint `requested_roles`.
/// Every requested role must be in [`ASSIGNABLE_ROLES`] **and** its scopes must be a subset of the
/// caller's effective scopes — so an `Admin`-but-not-`Ops` `index-admin` cannot grant an `Ops`-bearing
/// role (`operator`/`admin`) and escalate to cluster operations. Evaluated against the canonical
/// default role catalog. `Err` carries a human-readable reason.
pub fn check_assignable(caller_roles: &[String], requested_roles: &[String]) -> Result<(), String> {
    let catalog = RbacPolicy::with_default_roles();
    let caller = catalog.effective_scopes(caller_roles);
    for role in requested_roles {
        if !ASSIGNABLE_ROLES.contains(&role.as_str()) {
            return Err(format!(
                "role `{role}` is not assignable (allowed: {})",
                ASSIGNABLE_ROLES.join(", ")
            ));
        }
        if !catalog.scopes_of(role).is_subset(&caller) {
            return Err(format!(
                "cannot grant role `{role}`: it carries scopes the caller does not hold"
            ));
        }
    }
    Ok(())
}

impl AuthHook for RbacPolicy {
    fn authorize(&self, ctx: &AuthContext) -> Result<(), AuthDenied> {
        let Some(required) = scope_for_method(ctx.method) else {
            // Fail closed: an unmapped method is denied rather than silently allowed.
            return Err(AuthDenied::new(format!(
                "operation `{}` is not permitted by policy",
                ctx.method
            )));
        };
        // Per-index RBAC: a token carrying an index allowlist may only touch those indexes.
        // Enforced *before* the scope check, and only when the request resolved to a concrete target
        // index (`ctx.index`) — cluster/admin ops that don't name an index (`index = None`) are gated
        // by scope alone. An empty allowlist = unrestricted.
        if !ctx.allowed_indexes.is_empty() {
            if let Some(target) = &ctx.index {
                if !ctx.allowed_indexes.iter().any(|i| i == target) {
                    return Err(AuthDenied::new(format!(
                        "index `{target}` is not in the caller's allowed indexes ({})",
                        ctx.allowed_indexes.join(", ")
                    )));
                }
            }
        }
        if self.grants(&ctx.roles, required) {
            return Ok(());
        }
        let held = if ctx.roles.is_empty() {
            "none".to_string()
        } else {
            ctx.roles.join(", ")
        };
        Err(AuthDenied::new(format!(
            "`{}` requires the `{required}` scope; caller roles ({held}) do not grant it",
            ctx.method
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(method: &'static str, roles: &[&str]) -> AuthContext {
        AuthContext {
            method,
            principal: Some("alice".to_string()),
            tenant: None,
            roles: roles.iter().map(|r| r.to_string()).collect(),
            index: None,
            allowed_indexes: Vec::new(),
        }
    }

    /// An [`AuthContext`] scoped to a resolved target `index` with an `allowed` index allowlist —
    /// exercises per-index RBAC.
    fn ctx_index(
        method: &'static str,
        roles: &[&str],
        index: Option<&str>,
        allowed: &[&str],
    ) -> AuthContext {
        AuthContext {
            method,
            principal: Some("alice".to_string()),
            tenant: None,
            roles: roles.iter().map(|r| r.to_string()).collect(),
            index: index.map(str::to_string),
            allowed_indexes: allowed.iter().map(|i| i.to_string()).collect(),
        }
    }

    #[test]
    fn check_assignable_prevents_privilege_escalation() {
        // An index-admin (Admin, but no Ops) can only grant `reader`.
        let index_admin = vec!["index-admin".to_string()];
        assert!(check_assignable(&index_admin, &["reader".to_string()]).is_ok());
        // Granting an Ops-bearing role it doesn't hold is rejected (the escalation path).
        assert!(check_assignable(&index_admin, &["operator".to_string()]).is_err());
        assert!(check_assignable(&index_admin, &["admin".to_string()]).is_err());
        // Non-assignable roles (privileged aliases) are rejected outright.
        assert!(check_assignable(&index_admin, &["service".to_string()]).is_err());
        assert!(check_assignable(&["admin".to_string()], &["index-admin".to_string()]).is_err());
        // A full admin may grant any assignable role.
        let admin = vec!["admin".to_string()];
        for r in ["reader", "operator", "admin"] {
            assert!(
                check_assignable(&admin, &[r.to_string()]).is_ok(),
                "admin may grant {r}"
            );
        }
        // No caller roles → can grant nothing.
        assert!(check_assignable(&[], &["reader".to_string()]).is_err());
    }

    #[test]
    fn viewer_may_query_but_not_administer() {
        let policy = RbacPolicy::with_default_roles();
        assert!(policy.authorize(&ctx("Search", &["viewer"])).is_ok());
        assert!(policy.authorize(&ctx("GetByKey", &["viewer"])).is_ok());
        assert!(policy.authorize(&ctx("DescribeIndex", &["viewer"])).is_ok());
        // No admin/write scope.
        assert!(policy.authorize(&ctx("ReindexIndex", &["viewer"])).is_err());
        assert!(policy.authorize(&ctx("Write", &["viewer"])).is_err());
    }

    #[test]
    fn canonical_roles_admin_operator_reader() {
        let policy = RbacPolicy::with_default_roles();
        // reader: query + read, no admin/ops.
        assert!(policy.authorize(&ctx("Search", &["reader"])).is_ok());
        assert!(policy.authorize(&ctx("DescribeIndex", &["reader"])).is_ok());
        assert!(policy.authorize(&ctx("CreateIndex", &["reader"])).is_err());
        // operator: read + ops, not index admin.
        assert!(policy.authorize(&ctx("PlanReshard", &["operator"])).is_ok());
        assert!(policy
            .authorize(&ctx("CreateIndex", &["operator"]))
            .is_err());
        // admin: everything.
        for m in ["Search", "Write", "CreateIndex", "ApplyReshard"] {
            assert!(policy.authorize(&ctx(m, &["admin"])).is_ok(), "{m}");
        }
    }

    #[test]
    fn reader_may_explain_and_manage_saved_queries() {
        // Explain + saved-query methods are read-tier: any reader can use them (not fail-closed).
        let policy = RbacPolicy::with_default_roles();
        for m in [
            "Explain",
            "ListSavedQueries",
            "SaveSavedQuery",
            "DeleteSavedQuery",
        ] {
            assert!(policy.authorize(&ctx(m, &["reader"])).is_ok(), "{m}");
        }
        // ...but not for an unauthenticated (no-roles) caller.
        assert!(policy.authorize(&ctx("SaveSavedQuery", &[])).is_err());
    }

    #[test]
    fn index_admin_may_administer_and_write() {
        let policy = RbacPolicy::with_default_roles();
        assert!(policy
            .authorize(&ctx("CreateIndex", &["index-admin"]))
            .is_ok());
        assert!(policy.authorize(&ctx("Write", &["index-admin"])).is_ok());
        assert!(policy.authorize(&ctx("Search", &["index-admin"])).is_ok());
    }

    #[test]
    fn service_role_may_do_everything() {
        let policy = RbacPolicy::with_default_roles();
        for m in ["Search", "Write", "CreateIndex", "DescribeIndex"] {
            assert!(policy.authorize(&ctx(m, &["service"])).is_ok(), "{m}");
        }
    }

    #[test]
    fn any_one_granting_role_suffices() {
        let policy = RbacPolicy::with_default_roles();
        // viewer can't write, but the caller also holds index-admin.
        assert!(policy
            .authorize(&ctx("Write", &["viewer", "index-admin"]))
            .is_ok());
    }

    #[test]
    fn no_roles_is_denied_with_a_clear_reason() {
        let policy = RbacPolicy::with_default_roles();
        let err = policy.authorize(&ctx("Search", &[])).unwrap_err();
        assert!(err.reason.contains("search"));
        assert!(err.reason.contains("none"));
    }

    #[test]
    fn unknown_role_grants_nothing() {
        let policy = RbacPolicy::with_default_roles();
        assert!(policy.authorize(&ctx("Search", &["wat"])).is_err());
    }

    #[test]
    fn unmapped_method_fails_closed() {
        let policy = RbacPolicy::with_default_roles();
        // Even a service role can't reach a method the policy doesn't recognize.
        let err = policy
            .authorize(&ctx("Frobnicate", &["service"]))
            .unwrap_err();
        assert!(err.reason.contains("not permitted"));
    }

    #[test]
    fn an_empty_policy_denies_everyone() {
        let policy = RbacPolicy::new();
        assert!(policy.authorize(&ctx("Search", &["viewer"])).is_err());
    }

    #[test]
    fn per_index_allowlist_denies_a_token_scoped_to_another_index() {
        // A reader whose token allows only index `a` may search `a` but NOT `b`.
        let policy = RbacPolicy::with_default_roles();
        // Allowed index → permitted (role still grants the scope).
        assert!(policy
            .authorize(&ctx_index("Search", &["reader"], Some("a"), &["a"]))
            .is_ok());
        // A different index → denied even though the role grants Search.
        let err = policy
            .authorize(&ctx_index("Search", &["reader"], Some("b"), &["a"]))
            .unwrap_err();
        assert!(err.reason.contains("not in the caller's allowed indexes"));
        assert!(err.reason.contains('b'));
    }

    #[test]
    fn per_index_allowlist_permits_any_listed_index() {
        let policy = RbacPolicy::with_default_roles();
        for ix in ["a", "b"] {
            assert!(
                policy
                    .authorize(&ctx_index("Search", &["reader"], Some(ix), &["a", "b"]))
                    .is_ok(),
                "index {ix} should be allowed"
            );
        }
        assert!(policy
            .authorize(&ctx_index("Search", &["reader"], Some("c"), &["a", "b"]))
            .is_err());
    }

    #[test]
    fn empty_allowlist_is_unrestricted_across_indexes() {
        // Back-compat: a token with no index allowlist may touch any resolved index.
        let policy = RbacPolicy::with_default_roles();
        assert!(policy
            .authorize(&ctx_index("Search", &["reader"], Some("anything"), &[]))
            .is_ok());
    }

    #[test]
    fn index_agnostic_calls_skip_the_allowlist() {
        // A call that resolved no target index (index = None) is gated by scope alone — the allowlist
        // check only applies to index-scoped operations.
        let policy = RbacPolicy::with_default_roles();
        assert!(policy
            .authorize(&ctx_index("Search", &["reader"], None, &["a"]))
            .is_ok());
    }

    #[test]
    fn per_index_allowlist_still_requires_the_scope() {
        // The allowlist is additive to (not a replacement for) the role→scope check: a reader allowed
        // on `a` still cannot administer it.
        let policy = RbacPolicy::with_default_roles();
        assert!(policy
            .authorize(&ctx_index("CreateIndex", &["reader"], Some("a"), &["a"]))
            .is_err());
    }
}
