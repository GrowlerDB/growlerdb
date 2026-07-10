//! The **AuthN core** (task-35, M4): validate a caller's bearer credential into a
//! *verified* identity ([`Verified`]) that the [auth seam](crate::auth) can trust.
//!
//! The M2 [auth seam](crate::auth) reads the principal/tenant straight off request
//! metadata — convenient, but any caller can assert any identity. AuthN closes that gap:
//! an [`Authenticator`] turns the raw `Authorization` header into a `Verified` identity
//! derived from a **validated** credential, which a later slice stamps into the request
//! (replacing, never trusting, caller-asserted headers).
//!
//! Authenticators: [`JwtAuthenticator`] (a fixed key — HS256/RS256 PEM, for simple/test
//! deploys), [`JwksAuthenticator`] (RS256 against an IdP's JWKS, selecting the key by `kid`
//! and refreshing to follow rotation — the production OIDC path), [`ApiKeyStore`] (issue/
//! revoke scoped keys for programmatic clients), and [`ChainAuthenticator`] (route by
//! `Authorization` scheme — `Bearer` vs `ApiKey`). The default [`Anonymous`] ignores
//! credentials and yields an anonymous identity, so dev and the existing suite stay open
//! until a deployment opts in. [`authenticate`] is the transport entry point the
//! [`Gateway`](crate::gateway::Gateway) calls to verify a request and stamp the trusted
//! identity. mTLS (service-to-service) is the remaining slice; see `backlog/docs/m4-plan.md`.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

use base64::Engine as _;
use growlerdb_proto::to_status;
use growlerdb_proto::v1::Error as WireError;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{
    decode, decode_header, encode, get_current_timestamp, Algorithm, DecodingKey, EncodingKey,
    Header, Validation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::{Digest as _, Sha256};
use tonic::{Code, Request, Status};

use crate::auth::{INDEXES_KEY, PRINCIPAL_KEY, ROLES_KEY, TENANT_KEY};

/// The request metadata key the credential is read from (HTTP `Authorization` header).
const AUTHORIZATION_KEY: &str = "authorization";

/// A verified caller identity, derived from a validated credential — **not** from
/// client-supplied headers. The request-path slice stamps this into metadata, overriding
/// any caller-asserted principal/tenant so downstream [authorization](crate::auth) and
/// tenant scoping act on a trusted identity.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Verified {
    /// The authenticated principal (an OIDC/JWT `sub`).
    pub principal: String,
    /// The tenant the caller is scoped to, from the configured tenant claim, if present.
    pub tenant: Option<String>,
    /// Coarse roles from the configured roles claim (drives control-plane RBAC in task-36).
    pub roles: Vec<String>,
    /// The caller's **index allowlist** from the configured `indexes` claim (task-240 per-index RBAC).
    /// When non-empty the caller may only operate on these indexes; empty = unrestricted (back-compat).
    pub indexes: Vec<String>,
    /// Human display name from the OIDC `name` claim, if present (task-103, for `GET /v1/me`).
    pub display_name: Option<String>,
    /// Email from the OIDC `email` claim, if present (task-103).
    pub email: Option<String>,
    /// The token's `iat` (issued-at, epoch seconds), if present — lets the control plane reject a
    /// session minted before a subject's roles were changed/revoked (task-147 / B4).
    pub issued_at: Option<u64>,
}

impl Verified {
    /// The identity on an **open** gateway (no authenticator): no principal, no roles. The console's
    /// `GET /v1/me` returns this as the "not signed in" shape (task-103).
    pub fn anonymous() -> Self {
        Self::default()
    }
}

/// Why authentication failed. The transport maps every variant to gRPC `Unauthenticated`
/// / HTTP 401 (request-path slice); a denial under [authorization](crate::auth) stays a
/// separate `PermissionDenied`.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AuthnError {
    /// No `Authorization` header was supplied.
    #[error("missing bearer credential")]
    Missing,
    /// The `Authorization` header was present but not a `Bearer <token>`.
    #[error("malformed authorization header")]
    Malformed,
    /// The token failed validation (signature, `exp`, `iss`, or `aud`).
    #[error("invalid token: {0}")]
    Invalid(String),
    /// The token validated but lacks a claim required to build the identity.
    #[error("token is missing the `{0}` claim")]
    MissingClaim(&'static str),
    /// JWKS/OIDC key material could not be fetched or parsed. An operational error from
    /// [`JwksAuthenticator::refresh`], not a per-request denial.
    #[error("OIDC/JWKS error: {0}")]
    Discovery(String),
}

/// Validates a caller's `Authorization` header into a [`Verified`] identity. Must be cheap
/// and side-effect-free — runs on every request. The default is [`Anonymous`].
pub trait Authenticator: Send + Sync {
    /// Authenticate the value of an `Authorization` header (e.g. `"Bearer eyJ…"`). `None`
    /// means the header was absent.
    fn authenticate(&self, authorization: Option<&str>) -> Result<Verified, AuthnError>;
}

/// The default no-op authenticator: ignores any credential and returns an anonymous
/// identity. Authentication is opt-in; until a deployment installs a real authenticator
/// the gateway stays open (as it was pre-M4).
#[derive(Debug, Clone, Default)]
pub struct Anonymous;

impl Authenticator for Anonymous {
    fn authenticate(&self, _authorization: Option<&str>) -> Result<Verified, AuthnError> {
        Ok(Verified {
            principal: "anonymous".to_string(),
            tenant: None,
            roles: Vec::new(),
            indexes: Vec::new(),
            display_name: None,
            email: None,
            issued_at: None,
        })
    }
}

/// A shared, type-erased authenticator the transport holds. [`default_authn`] yields the
/// no-op [`Anonymous`].
pub type SharedAuthn = Arc<dyn Authenticator>;

/// The default shared authenticator ([`Anonymous`]) used when none is configured.
pub fn default_authn() -> SharedAuthn {
    Arc::new(Anonymous)
}

/// Authenticate `request` with `authn` and rewrite its identity metadata to the **verified**
/// principal/tenant — the trust boundary. The credential is read from the `authorization`
/// metadata (the HTTP `Authorization` header over both gRPC and REST). Any caller-asserted
/// `x-growlerdb-principal`/`x-growlerdb-tenant` is **dropped first**, so the downstream
/// [authorization seam](crate::auth) (and the shards the Gateway forwards to) only ever sees
/// an identity this layer vouched for. A failure maps to `Unauthenticated`.
///
/// Called only when a deployment has installed an authenticator; with none, the request
/// passes through untouched (the pre-M4 internal-trust behavior). Returns the [`Verified`]
/// identity for callers that need the roles directly (e.g. control-plane RBAC, task-36).
pub fn authenticate<T>(authn: &SharedAuthn, request: &mut Request<T>) -> Result<Verified, Status> {
    let authorization = request
        .metadata()
        .get(AUTHORIZATION_KEY)
        .and_then(|v| v.to_str().ok());
    let verified = authn.authenticate(authorization).map_err(authn_status)?;

    let meta = request.metadata_mut();
    // Drop caller-asserted identity before stamping the verified one — a forged
    // `x-growlerdb-principal`/`-roles`/`-indexes` must never survive to the seam.
    meta.remove(PRINCIPAL_KEY);
    meta.remove(TENANT_KEY);
    meta.remove(ROLES_KEY);
    meta.remove(INDEXES_KEY);
    let principal = verified.principal.parse().map_err(|_| {
        Status::unauthenticated("authenticated principal is not a valid identifier")
    })?;
    meta.insert(PRINCIPAL_KEY, principal);
    if let Some(tenant) = &verified.tenant {
        let value = tenant
            .parse()
            .map_err(|_| Status::unauthenticated("tenant claim is not a valid identifier"))?;
        meta.insert(TENANT_KEY, value);
    }
    // Roles drive RBAC ([`crate::rbac`]); carry them as a comma-separated list so the
    // [authorization seam](crate::auth) and any downstream Node see the verified set.
    if !verified.roles.is_empty() {
        let value = verified
            .roles
            .join(",")
            .parse()
            .map_err(|_| Status::unauthenticated("a role claim is not a valid header value"))?;
        meta.insert(ROLES_KEY, value);
    }
    // The index allowlist scopes per-index RBAC (task-240); carry it comma-separated like roles so the
    // authorization seam restricts the caller to these indexes.
    if !verified.indexes.is_empty() {
        let value = verified.indexes.join(",").parse().map_err(|_| {
            Status::unauthenticated("an index allowlist entry is not a valid header value")
        })?;
        meta.insert(INDEXES_KEY, value);
    }
    Ok(verified)
}

/// Drop any caller-asserted identity metadata (`x-growlerdb-principal`/`-tenant`/`-roles`) from a
/// request. Used on an **open** gateway (no authenticator), where nothing verifies these headers —
/// without this, a client could forge `x-growlerdb-tenant` and read across tenants on a
/// tenant-scoped index (task-147 / F2). Leaving them stripped makes tenant scoping fail closed
/// (no verified tenant → the scoped index denies), which is the honest behaviour: a tenant-scoped
/// index can't be safely served without authentication.
pub fn strip_identity<T>(request: &mut Request<T>) {
    let meta = request.metadata_mut();
    meta.remove(PRINCIPAL_KEY);
    meta.remove(TENANT_KEY);
    meta.remove(ROLES_KEY);
    meta.remove(INDEXES_KEY);
}

/// Map an [`AuthnError`] to a gRPC `Unauthenticated` status carrying the structured wire
/// error (parallel to how the [authorization seam](crate::auth) maps a denial to
/// `PermissionDenied`).
fn authn_status(err: AuthnError) -> Status {
    to_status(
        Code::Unauthenticated,
        WireError::new("UNAUTHENTICATED", err.to_string()),
    )
}

/// Which JWT claims carry the tenant and roles. `sub` is always the principal (the
/// registered subject claim); these two are deployment-configurable because IdPs differ
/// (Keycloak nests roles under `realm_access.roles`; a flat `roles` claim is the simplest
/// shape and the default here).
#[derive(Debug, Clone)]
pub struct ClaimMapping {
    /// Claim carrying the tenant id (default `"tenant"`).
    pub tenant: String,
    /// Claim carrying roles — a JSON array of strings or a space-delimited string
    /// (default `"roles"`).
    pub roles: String,
    /// Claim carrying the caller's **index allowlist** for per-index RBAC (task-240) — a JSON array
    /// of strings or a space-delimited string (default `"indexes"`). Absent/empty = unrestricted.
    pub indexes: String,
}

impl Default for ClaimMapping {
    fn default() -> Self {
        Self {
            tenant: "tenant".to_string(),
            roles: "roles".to_string(),
            indexes: "indexes".to_string(),
        }
    }
}

/// Validates JWT bearer tokens (signature + `iss`/`aud`/`exp`) and maps their claims to a
/// [`Verified`] identity. RS256 (`from_rs256_pem`) is the OIDC default; HS256
/// (`from_hs256_secret`) suits simple or test deployments with a shared secret.
pub struct JwtAuthenticator {
    key: DecodingKey,
    validation: Validation,
    claims: ClaimMapping,
}

impl JwtAuthenticator {
    /// Build from a decoding `key` and a fully-configured `validation` (algorithm,
    /// issuer, audience). Use [`Self::from_hs256_secret`] / [`Self::from_rs256_pem`] for
    /// the common cases.
    pub fn new(key: DecodingKey, validation: Validation) -> Self {
        Self {
            key,
            validation,
            claims: ClaimMapping::default(),
        }
    }

    /// Validate HS256 tokens signed with a shared `secret`, requiring `issuer` and
    /// `audience` to match. Symmetric — for simple or test deployments, not multi-party
    /// OIDC.
    pub fn from_hs256_secret(secret: &[u8], issuer: &str, audience: &str) -> Self {
        Self::new(
            DecodingKey::from_secret(secret),
            validation_for(Algorithm::HS256, issuer, audience),
        )
    }

    /// Validate RS256 tokens against an RSA public key in PEM form, requiring `issuer` and
    /// `audience` to match. This is the OIDC shape; the key normally comes from the IdP's
    /// JWKS (fetched in a later slice). Errors if the PEM does not parse.
    pub fn from_rs256_pem(pem: &[u8], issuer: &str, audience: &str) -> Result<Self, AuthnError> {
        let key = DecodingKey::from_rsa_pem(pem).map_err(|e| AuthnError::Invalid(e.to_string()))?;
        Ok(Self::new(
            key,
            validation_for(Algorithm::RS256, issuer, audience),
        ))
    }

    /// Override which claims supply the tenant and roles (default: `tenant` / `roles`).
    pub fn with_claim_mapping(mut self, claims: ClaimMapping) -> Self {
        self.claims = claims;
        self
    }
}

impl Authenticator for JwtAuthenticator {
    fn authenticate(&self, authorization: Option<&str>) -> Result<Verified, AuthnError> {
        let token = bearer_token(authorization)?;
        decode_and_map(token, &self.key, &self.validation, &self.claims)
    }
}

/// Validate `token` against `key` + `validation` and map its claims to a [`Verified`]
/// identity per `mapping`. Shared by every JWT-based authenticator ([`JwtAuthenticator`]
/// with a fixed key, [`JwksAuthenticator`] with a per-`kid` key).
fn decode_and_map(
    token: &str,
    key: &DecodingKey,
    validation: &Validation,
    mapping: &ClaimMapping,
) -> Result<Verified, AuthnError> {
    let data = decode::<RawClaims>(token, key, validation)
        .map_err(|e| AuthnError::Invalid(e.to_string()))?;
    let claims = data.claims;
    let principal = claims.sub.ok_or(AuthnError::MissingClaim("sub"))?;
    let tenant = claims.extra.get(&mapping.tenant).and_then(claim_string);
    let roles = claims
        .extra
        .get(&mapping.roles)
        .map(claim_roles)
        .unwrap_or_default();
    // Per-index RBAC allowlist (task-240): same list shape as roles (JSON array or space-delimited).
    let indexes = claims
        .extra
        .get(&mapping.indexes)
        .map(claim_roles)
        .unwrap_or_default();
    // Standard OIDC profile claims for the console's identity (task-103); absent on minimal tokens.
    let display_name = claims.extra.get("name").and_then(claim_string);
    let email = claims.extra.get("email").and_then(claim_string);
    let issued_at = claims.extra.get("iat").and_then(|v| v.as_u64());
    Ok(Verified {
        principal,
        tenant,
        roles,
        indexes,
        display_name,
        email,
        issued_at,
    })
}

/// `iss`/`aud`-checked `exp`-validating config for `alg` (jsonwebtoken validates `exp` by
/// default).
fn validation_for(alg: Algorithm, issuer: &str, audience: &str) -> Validation {
    let mut validation = Validation::new(alg);
    validation.set_issuer(&[issuer]);
    validation.set_audience(&[audience]);
    validation
}

// ---- OIDC / JWKS (RS256 with key rotation) -------------------------------------------

/// Where a [`JwksAuthenticator`] finds the IdP's signing keys.
enum JwksSource {
    /// Discover the JWKS endpoint from the issuer's OIDC discovery document.
    Discover { issuer: String },
    /// A fixed JWKS endpoint URL.
    Uri(String),
}

/// Validates RS256 JWTs against an IdP's **JWKS** (JSON Web Key Set), selecting the signing
/// key by the token's `kid` header — the production OIDC path (Keycloak default). Keys rotate,
/// so the set is fetched from the IdP and [refreshed](Self::refresh): the Gateway runs an
/// initial fetch at startup and a periodic refresh thereafter. `authenticate` is synchronous
/// and reads the cached keys; refresh is the only async, network-touching step.
pub struct JwksAuthenticator {
    keys: Arc<RwLock<HashMap<String, DecodingKey>>>,
    validation: Validation,
    claims: ClaimMapping,
    source: JwksSource,
    http: reqwest::Client,
}

impl JwksAuthenticator {
    /// Validate RS256 tokens for `issuer`/`audience`, discovering the JWKS endpoint from the
    /// issuer's `.well-known/openid-configuration`. The key cache starts empty — call
    /// [`refresh`](Self::refresh) before serving (and periodically after).
    pub fn for_issuer(issuer: &str, audience: &str) -> Self {
        Self::with_source(
            JwksSource::Discover {
                issuer: issuer.to_string(),
            },
            issuer,
            audience,
        )
    }

    /// As [`for_issuer`](Self::for_issuer), but with an explicit `jwks_uri` (skips discovery).
    pub fn for_jwks_uri(issuer: &str, audience: &str, jwks_uri: &str) -> Self {
        Self::with_source(JwksSource::Uri(jwks_uri.to_string()), issuer, audience)
    }

    fn with_source(source: JwksSource, issuer: &str, audience: &str) -> Self {
        Self {
            keys: Arc::new(RwLock::new(HashMap::new())),
            validation: validation_for(Algorithm::RS256, issuer, audience),
            claims: ClaimMapping::default(),
            source,
            http: reqwest::Client::new(),
        }
    }

    /// Seed the key cache directly from an already-fetched [`JwkSet`] (a caller that fetches
    /// JWKS itself, or a test) — no network. Errors if the set has no usable keyed key.
    pub fn from_jwk_set(jwks: &JwkSet, issuer: &str, audience: &str) -> Result<Self, AuthnError> {
        let this = Self::with_source(JwksSource::Uri(String::new()), issuer, audience);
        *this.keys.write().expect("key lock") = build_key_map(jwks)?;
        Ok(this)
    }

    /// Override which claims supply the tenant and roles (default: `tenant` / `roles`).
    pub fn with_claim_mapping(mut self, claims: ClaimMapping) -> Self {
        self.claims = claims;
        self
    }

    /// Fetch the IdP's JWKS (discovering the endpoint first if configured by issuer) and
    /// replace the key cache. Call at startup and on a timer to follow key rotation. On failure
    /// the previous keys stay in place — an IdP blip must not blank authentication.
    pub async fn refresh(&self) -> Result<(), AuthnError> {
        let jwks_uri = match &self.source {
            JwksSource::Uri(uri) if !uri.is_empty() => uri.clone(),
            JwksSource::Uri(_) => {
                return Err(AuthnError::Discovery("no jwks_uri configured".to_string()))
            }
            JwksSource::Discover { issuer } => self.discover_jwks_uri(issuer).await?,
        };
        let jwks: JwkSet = self
            .http
            .get(&jwks_uri)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| AuthnError::Discovery(format!("fetching JWKS from {jwks_uri}: {e}")))?
            .json()
            .await
            .map_err(|e| AuthnError::Discovery(format!("parsing JWKS from {jwks_uri}: {e}")))?;
        let map = build_key_map(&jwks)?;
        *self.keys.write().expect("key lock") = map;
        Ok(())
    }

    /// Resolve the `jwks_uri` from the issuer's OIDC discovery document.
    async fn discover_jwks_uri(&self, issuer: &str) -> Result<String, AuthnError> {
        let url = format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        );
        let doc: JsonValue = self
            .http
            .get(&url)
            .send()
            .await
            .and_then(|r| r.error_for_status())
            .map_err(|e| AuthnError::Discovery(format!("fetching OIDC config from {url}: {e}")))?
            .json()
            .await
            .map_err(|e| AuthnError::Discovery(format!("parsing OIDC config from {url}: {e}")))?;
        doc.get("jwks_uri")
            .and_then(|v| v.as_str())
            .map(str::to_string)
            .ok_or_else(|| AuthnError::Discovery(format!("OIDC config at {url} has no jwks_uri")))
    }
}

impl Authenticator for JwksAuthenticator {
    fn authenticate(&self, authorization: Option<&str>) -> Result<Verified, AuthnError> {
        let token = bearer_token(authorization)?;
        let header = decode_header(token).map_err(|e| AuthnError::Invalid(e.to_string()))?;
        let kid = header
            .kid
            .ok_or_else(|| AuthnError::Invalid("token has no `kid` header".to_string()))?;
        let key = self
            .keys
            .read()
            .expect("key lock")
            .get(&kid)
            .cloned()
            .ok_or_else(|| {
                AuthnError::Invalid(format!(
                    "no signing key for kid `{kid}` (key rotated or JWKS not refreshed)"
                ))
            })?;
        decode_and_map(token, &key, &self.validation, &self.claims)
    }
}

/// Build a `kid → DecodingKey` map from a JWKS, skipping keys without a `kid`. Errors if no
/// usable key remains.
fn build_key_map(jwks: &JwkSet) -> Result<HashMap<String, DecodingKey>, AuthnError> {
    let mut map = HashMap::new();
    for jwk in &jwks.keys {
        let Some(kid) = jwk.common.key_id.clone() else {
            continue;
        };
        let key = DecodingKey::from_jwk(jwk)
            .map_err(|e| AuthnError::Discovery(format!("unusable JWK for kid `{kid}`: {e}")))?;
        map.insert(kid, key);
    }
    if map.is_empty() {
        return Err(AuthnError::Discovery(
            "JWKS contained no usable keys with a `kid`".to_string(),
        ));
    }
    Ok(map)
}

// ---- API keys (programmatic clients) -------------------------------------------------

/// The identity an API key resolves to — an API key stands in for a principal with a fixed
/// tenant/roles (rather than a token's claims).
#[derive(Debug, Clone)]
pub struct KeyIdentity {
    /// Principal the key authenticates as.
    pub principal: String,
    /// Tenant the key is scoped to, if any.
    pub tenant: Option<String>,
    /// Roles the key carries.
    pub roles: Vec<String>,
    /// The key's **index allowlist** for per-index RBAC (task-240). Non-empty = scoped to these
    /// indexes; empty = unrestricted (back-compat).
    pub indexes: Vec<String>,
}

/// An in-memory store of issued API keys for programmatic clients, presented as
/// `Authorization: ApiKey <key>`. Keys are **issued** (a fresh random secret, returned once)
/// and **revocable**; only their SHA-256 digest is stored, never the raw secret. Interior
/// locking means a shared handle can issue/revoke while the Gateway authenticates.
#[derive(Default)]
pub struct ApiKeyStore {
    /// `sha256(raw key)` (base64url) → identity.
    keys: RwLock<HashMap<String, KeyIdentity>>,
}

impl ApiKeyStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Issue a fresh key for `identity`, returning the raw secret — shown **once**; only its
    /// digest is retained. Present it as `Authorization: ApiKey <key>`.
    pub fn issue(&self, identity: KeyIdentity) -> String {
        let raw = generate_secret();
        self.keys
            .write()
            .expect("key lock")
            .insert(digest(&raw), identity);
        raw
    }

    /// Register a caller-supplied `raw` key for `identity` (e.g. loaded from config) — like
    /// [`issue`](Self::issue), but for a key minted elsewhere.
    pub fn insert(&self, raw: &str, identity: KeyIdentity) {
        self.keys
            .write()
            .expect("key lock")
            .insert(digest(raw), identity);
    }

    /// Revoke a key by its raw secret; returns whether one was removed. Effective immediately.
    pub fn revoke(&self, raw: &str) -> bool {
        self.keys
            .write()
            .expect("key lock")
            .remove(&digest(raw))
            .is_some()
    }
}

impl Authenticator for ApiKeyStore {
    fn authenticate(&self, authorization: Option<&str>) -> Result<Verified, AuthnError> {
        let key = api_key(authorization)?;
        let identity = self
            .keys
            .read()
            .expect("key lock")
            .get(&digest(key))
            .cloned()
            .ok_or_else(|| AuthnError::Invalid("unknown or revoked API key".to_string()))?;
        Ok(Verified {
            principal: identity.principal,
            tenant: identity.tenant,
            roles: identity.roles,
            indexes: identity.indexes,
            display_name: None,
            email: None,
            issued_at: None,
        })
    }
}

/// Extract the `<key>` from an `Authorization: ApiKey <key>` header value.
fn api_key(authorization: Option<&str>) -> Result<&str, AuthnError> {
    let header = authorization.ok_or(AuthnError::Missing)?;
    let key = header
        .strip_prefix("ApiKey ")
        .or_else(|| header.strip_prefix("apikey "))
        .ok_or(AuthnError::Malformed)?
        .trim();
    if key.is_empty() {
        return Err(AuthnError::Malformed);
    }
    Ok(key)
}

/// 32 bytes from the OS CSPRNG, base64url (no pad) — an opaque, unguessable API key.
fn generate_secret() -> String {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes).expect("OS RNG");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// SHA-256 digest of a raw key, base64url (no pad) — the at-rest form of an API key.
fn digest(raw: &str) -> String {
    let hash = Sha256::digest(raw.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash)
}

// ---- Composition ---------------------------------------------------------------------

/// Routes a request to the right authenticator by its `Authorization` **scheme**: `Bearer …`
/// to a JWT authenticator, `ApiKey …` to an [`ApiKeyStore`]. Lets a deployment accept both
/// human (OIDC) and programmatic (API-key) callers behind one [`Gateway::with_authn`].
///
/// [`Gateway::with_authn`]: crate::gateway::Gateway::with_authn
#[derive(Default)]
pub struct ChainAuthenticator {
    bearer: Option<SharedAuthn>,
    api_key: Option<SharedAuthn>,
}

impl ChainAuthenticator {
    /// An empty chain (accepts nothing until a scheme is configured).
    pub fn new() -> Self {
        Self::default()
    }

    /// Handle `Bearer` tokens with `authn` (a [`JwtAuthenticator`] / [`JwksAuthenticator`]).
    pub fn with_bearer(mut self, authn: SharedAuthn) -> Self {
        self.bearer = Some(authn);
        self
    }

    /// Handle `ApiKey` credentials with `authn` (an [`ApiKeyStore`]).
    pub fn with_api_keys(mut self, authn: SharedAuthn) -> Self {
        self.api_key = Some(authn);
        self
    }
}

impl Authenticator for ChainAuthenticator {
    fn authenticate(&self, authorization: Option<&str>) -> Result<Verified, AuthnError> {
        let header = authorization.ok_or(AuthnError::Missing)?;
        let scheme = header
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        let chosen = match scheme.as_str() {
            "bearer" => self.bearer.as_ref(),
            "apikey" => self.api_key.as_ref(),
            _ => return Err(AuthnError::Malformed),
        };
        chosen
            .ok_or(AuthnError::Malformed)?
            .authenticate(Some(header))
    }
}

/// Extract the `<token>` from an `Authorization: Bearer <token>` header value.
fn bearer_token(authorization: Option<&str>) -> Result<&str, AuthnError> {
    let header = authorization.ok_or(AuthnError::Missing)?;
    let token = header
        .strip_prefix("Bearer ")
        .or_else(|| header.strip_prefix("bearer "))
        .ok_or(AuthnError::Malformed)?
        .trim();
    if token.is_empty() {
        return Err(AuthnError::Malformed);
    }
    Ok(token)
}

/// A claim that should be a string (e.g. the tenant id), or `None` if absent/not a string.
fn claim_string(value: &JsonValue) -> Option<String> {
    value.as_str().map(str::to_string)
}

/// Roles claim, accepting either a JSON array of strings or a single space-delimited
/// string (both shapes appear across IdPs).
fn claim_roles(value: &JsonValue) -> Vec<String> {
    match value {
        JsonValue::Array(items) => items
            .iter()
            .filter_map(|v| v.as_str().map(str::to_string))
            .collect(),
        JsonValue::String(s) => s.split_whitespace().map(str::to_string).collect(),
        _ => Vec::new(),
    }
}

/// The claims we read off a validated token. `iss`/`aud`/`exp` are validated by
/// jsonwebtoken itself; `extra` captures everything else so the configured tenant/roles
/// claims can be looked up by name.
#[derive(Deserialize)]
struct RawClaims {
    sub: Option<String>,
    #[serde(flatten)]
    extra: BTreeMap<String, JsonValue>,
}

/// Fixed `iss`/`aud` + TTL for built-in session JWTs (task-128). The control-plane mints with these
/// and the gateway validates with the same, so they must agree — they're constants, not config.
pub const BUILTIN_SESSION_ISSUER: &str = "growlerdb";
pub const BUILTIN_SESSION_AUDIENCE: &str = "growlerdb-console";
/// Session lifetime — short, since logout is client-side and there's no per-session revocation yet.
pub const BUILTIN_SESSION_TTL_SECS: u64 = 12 * 3600;

/// Claims for a built-in **session JWT** (task-128). Subject + roles + the standard `iss`/`aud`/`exp`
/// so the existing [`JwtAuthenticator`] (HS256) validates it like any bearer; `name` feeds `/v1/me`.
#[derive(Serialize)]
struct SessionClaims<'a> {
    sub: &'a str,
    roles: &'a [String],
    /// The subject's **index allowlist** (task-244) — the same `indexes` claim shape the gateway's
    /// per-index RBAC reads (task-240). Omitted when empty (unrestricted across indexes), so a token
    /// for an unscoped subject is byte-for-byte the pre-task-244 session token.
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    indexes: &'a [String],
    iss: &'a str,
    aud: &'a str,
    exp: u64,
    /// Issued-at (epoch seconds) — the control plane compares this against the subject's session
    /// epoch to reject sessions minted before a role change/revocation (task-147 / B4).
    iat: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
}

/// Mint a short-lived HS256 **session JWT** for built-in (no external IdP) closed mode (task-128).
/// Carries the verified `subject` + `roles` (+ an optional `indexes` allowlist for per-index RBAC,
/// task-244), signed with the deployment's shared `secret`, expiring in `ttl_secs`. The gateway
/// accepts it via [`JwtAuthenticator::from_hs256_secret`]`(secret, issuer, audience)` — which already
/// checks `exp`, so the TTL needs no extra revocation store. (Logout is client-side token drop;
/// global invalidation is rotating the secret — see task-128 notes.)
// Each argument is a distinct, security-relevant claim (subject / roles / index scope / iss / aud /
// ttl / name); a struct would only rename them, so keep the explicit signature.
#[allow(clippy::too_many_arguments)]
pub fn mint_session_jwt(
    secret: &[u8],
    subject: &str,
    roles: &[String],
    indexes: &[String],
    issuer: &str,
    audience: &str,
    ttl_secs: u64,
    display_name: Option<&str>,
) -> Result<String, AuthnError> {
    let claims = SessionClaims {
        sub: subject,
        roles,
        indexes,
        iss: issuer,
        aud: audience,
        exp: get_current_timestamp() + ttl_secs,
        iat: get_current_timestamp(),
        name: display_name,
    };
    encode(
        &Header::new(Algorithm::HS256),
        &claims,
        &EncodingKey::from_secret(secret),
    )
    .map_err(|e| AuthnError::Invalid(e.to_string()))
}

/// Mint a fresh API-token secret + its stored hash (task-105). The secret (`gdb_live_<random>`) is
/// returned **once**; only the hash is persisted by the control plane.
pub fn mint_api_token() -> (String, String) {
    let raw = format!("gdb_live_{}", generate_secret());
    let hash = digest(&raw);
    (raw, hash)
}

/// SHA-256 (base64url) of an API-token secret — the value the control plane stores + looks up.
pub fn hash_api_token(raw: &str) -> String {
    digest(raw)
}

/// An [`Authenticator`] backed by the control-plane registry's API tokens (task-105): an
/// `Authorization: ApiKey <secret>` is hashed and looked up; a revoked/unknown token fails. The
/// token's stored roles become the verified roles. Chain with a JWT authenticator (which handles
/// `Bearer`) via [`ChainAuthenticator`] so a gateway accepts both.
pub struct RegistryTokenAuthenticator {
    registry: std::sync::Arc<growlerdb_controlplane::Registry>,
}

impl RegistryTokenAuthenticator {
    /// Authenticate API tokens against `registry`.
    pub fn new(registry: std::sync::Arc<growlerdb_controlplane::Registry>) -> Self {
        Self { registry }
    }
}

impl Authenticator for RegistryTokenAuthenticator {
    fn authenticate(&self, authorization: Option<&str>) -> Result<Verified, AuthnError> {
        let key = api_key(authorization)?;
        let token = self
            .registry
            .find_token(&digest(key))
            .ok_or_else(|| AuthnError::Invalid("unknown or revoked API token".to_string()))?;
        Ok(Verified {
            principal: token.owner,
            tenant: None,
            roles: token.roles,
            // Registry API tokens carry no index allowlist today (unrestricted across indexes); the
            // per-index allowlist (task-240) is delivered via a JWT `indexes` claim or a KeyIdentity.
            indexes: Vec::new(),
            display_name: Some(token.label),
            email: None,
            issued_at: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const SECRET: &[u8] = b"test-shared-secret";
    const ISSUER: &str = "https://idp.example/realms/growlerdb";
    const AUDIENCE: &str = "growlerdb";

    #[test]
    fn session_jwt_round_trips_through_the_hs256_authenticator() {
        // task-128: a login-minted session token must validate as a normal Bearer JWT.
        let roles = vec!["admin".to_string(), "reader".to_string()];
        let token = mint_session_jwt(
            SECRET,
            "alice",
            &roles,
            &[],
            ISSUER,
            AUDIENCE,
            3600,
            Some("Alice"),
        )
        .unwrap();
        let authn = JwtAuthenticator::from_hs256_secret(SECRET, ISSUER, AUDIENCE);
        let v = authn
            .authenticate(Some(&format!("Bearer {token}")))
            .unwrap();
        assert_eq!(v.principal, "alice");
        assert_eq!(v.roles, roles);
        assert_eq!(v.display_name.as_deref(), Some("Alice"));
        // No index allowlist → unrestricted across indexes (back-compat).
        assert!(v.indexes.is_empty());
        // A token signed with a different secret is rejected.
        let other = JwtAuthenticator::from_hs256_secret(b"different-secret", ISSUER, AUDIENCE);
        assert!(other
            .authenticate(Some(&format!("Bearer {token}")))
            .is_err());
    }

    #[test]
    fn session_jwt_carries_the_index_allowlist_claim() {
        // task-244: a scoped demo session must surface its `indexes` claim so per-index RBAC (task-240)
        // restricts the subject to exactly those indexes.
        let roles = vec!["reader".to_string(), "operator".to_string()];
        let indexes = vec!["docs".to_string(), "catalog".to_string()];
        let token = mint_session_jwt(
            SECRET, "demo", &roles, &indexes, ISSUER, AUDIENCE, 3600, None,
        )
        .unwrap();
        let authn = JwtAuthenticator::from_hs256_secret(SECRET, ISSUER, AUDIENCE);
        let v = authn
            .authenticate(Some(&format!("Bearer {token}")))
            .unwrap();
        assert_eq!(v.principal, "demo");
        assert_eq!(v.roles, roles);
        assert_eq!(v.indexes, indexes);
    }

    /// Sign `claims` as an HS256 JWT with [`SECRET`].
    fn hs256(claims: JsonValue) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(SECRET),
        )
        .unwrap()
    }

    /// A well-formed claim set: valid issuer/audience and a future expiry.
    fn good_claims() -> JsonValue {
        json!({
            "sub": "alice",
            "iss": ISSUER,
            "aud": AUDIENCE,
            "exp": get_current_timestamp() + 3600,
            "tenant": "acme",
            "roles": ["viewer", "index-admin"],
        })
    }

    fn authn() -> JwtAuthenticator {
        JwtAuthenticator::from_hs256_secret(SECRET, ISSUER, AUDIENCE)
    }

    fn bearer(token: &str) -> String {
        format!("Bearer {token}")
    }

    #[test]
    fn valid_token_yields_verified_identity() {
        let token = hs256(good_claims());
        let verified = authn().authenticate(Some(&bearer(&token))).unwrap();
        assert_eq!(verified.principal, "alice");
        assert_eq!(verified.tenant.as_deref(), Some("acme"));
        assert_eq!(verified.roles, vec!["viewer", "index-admin"]);
    }

    #[test]
    fn roles_as_space_delimited_string_are_split() {
        let mut claims = good_claims();
        claims["roles"] = json!("viewer operator");
        let token = hs256(claims);
        let verified = authn().authenticate(Some(&bearer(&token))).unwrap();
        assert_eq!(verified.roles, vec!["viewer", "operator"]);
    }

    #[test]
    fn absent_tenant_and_roles_default_empty() {
        let mut claims = good_claims();
        claims.as_object_mut().unwrap().remove("tenant");
        claims.as_object_mut().unwrap().remove("roles");
        let token = hs256(claims);
        let verified = authn().authenticate(Some(&bearer(&token))).unwrap();
        assert_eq!(verified.tenant, None);
        assert!(verified.roles.is_empty());
    }

    #[test]
    fn configurable_claim_names_are_honored() {
        let claims = json!({
            "sub": "svc",
            "iss": ISSUER,
            "aud": AUDIENCE,
            "exp": get_current_timestamp() + 3600,
            "org": "globex",
            "scope": "viewer",
        });
        let token = hs256(claims);
        let authn = authn().with_claim_mapping(ClaimMapping {
            tenant: "org".to_string(),
            roles: "scope".to_string(),
            indexes: "indexes".to_string(),
        });
        let verified = authn.authenticate(Some(&bearer(&token))).unwrap();
        assert_eq!(verified.tenant.as_deref(), Some("globex"));
        assert_eq!(verified.roles, vec!["viewer"]);
    }

    #[test]
    fn expired_token_is_rejected() {
        // Well past jsonwebtoken's default 60s `exp` leeway.
        let mut claims = good_claims();
        claims["exp"] = json!(get_current_timestamp() - 3600);
        let token = hs256(claims);
        let err = authn().authenticate(Some(&bearer(&token))).unwrap_err();
        assert!(matches!(err, AuthnError::Invalid(_)));
    }

    #[test]
    fn wrong_issuer_is_rejected() {
        let mut claims = good_claims();
        claims["iss"] = json!("https://evil.example");
        let token = hs256(claims);
        let err = authn().authenticate(Some(&bearer(&token))).unwrap_err();
        assert!(matches!(err, AuthnError::Invalid(_)));
    }

    #[test]
    fn wrong_audience_is_rejected() {
        let mut claims = good_claims();
        claims["aud"] = json!("someone-else");
        let token = hs256(claims);
        let err = authn().authenticate(Some(&bearer(&token))).unwrap_err();
        assert!(matches!(err, AuthnError::Invalid(_)));
    }

    #[test]
    fn bad_signature_is_rejected() {
        let token = encode(
            &Header::new(Algorithm::HS256),
            &good_claims(),
            &EncodingKey::from_secret(b"a-different-secret"),
        )
        .unwrap();
        let err = authn().authenticate(Some(&bearer(&token))).unwrap_err();
        assert!(matches!(err, AuthnError::Invalid(_)));
    }

    #[test]
    fn token_without_sub_is_rejected() {
        let mut claims = good_claims();
        claims.as_object_mut().unwrap().remove("sub");
        let token = hs256(claims);
        let err = authn().authenticate(Some(&bearer(&token))).unwrap_err();
        assert_eq!(err, AuthnError::MissingClaim("sub"));
    }

    #[test]
    fn missing_header_is_missing_error() {
        assert_eq!(authn().authenticate(None).unwrap_err(), AuthnError::Missing);
    }

    #[test]
    fn non_bearer_header_is_malformed() {
        let err = authn()
            .authenticate(Some("Basic dXNlcjpwYXNz"))
            .unwrap_err();
        assert_eq!(err, AuthnError::Malformed);
        let empty = authn().authenticate(Some("Bearer ")).unwrap_err();
        assert_eq!(empty, AuthnError::Malformed);
    }

    #[test]
    fn lowercase_bearer_scheme_is_accepted() {
        let token = hs256(good_claims());
        let verified = authn()
            .authenticate(Some(&format!("bearer {token}")))
            .unwrap();
        assert_eq!(verified.principal, "alice");
    }

    fn request_with(authorization: Option<&str>, forged_principal: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(a) = authorization {
            req.metadata_mut()
                .insert(AUTHORIZATION_KEY, a.parse().unwrap());
        }
        if let Some(p) = forged_principal {
            req.metadata_mut().insert(PRINCIPAL_KEY, p.parse().unwrap());
        }
        req
    }

    #[test]
    fn authenticate_stamps_verified_identity_over_a_forged_principal() {
        let token = hs256(good_claims());
        let shared: SharedAuthn = Arc::new(authn());
        let mut req = request_with(Some(&bearer(&token)), Some("attacker"));
        let verified = authenticate(&shared, &mut req).unwrap();
        assert_eq!(verified.principal, "alice");
        // The forged principal is gone; the metadata the seam reads is the verified identity.
        let meta = req.metadata();
        assert_eq!(meta.get(PRINCIPAL_KEY).unwrap().to_str().unwrap(), "alice");
        assert_eq!(meta.get(TENANT_KEY).unwrap().to_str().unwrap(), "acme");
    }

    #[test]
    fn authenticate_clears_a_forged_tenant_when_the_token_carries_none() {
        let mut claims = good_claims();
        claims.as_object_mut().unwrap().remove("tenant");
        let token = hs256(claims);
        let shared: SharedAuthn = Arc::new(authn());
        let mut req = request_with(Some(&bearer(&token)), None);
        req.metadata_mut()
            .insert(TENANT_KEY, "forged".parse().unwrap());
        authenticate(&shared, &mut req).unwrap();
        // A token with no tenant claim must clear the forged tenant, not leave it behind.
        assert!(req.metadata().get(TENANT_KEY).is_none());
    }

    #[test]
    fn strip_identity_drops_all_caller_asserted_headers() {
        // task-147 / F2: on an open gateway (no authenticator) the caller's identity headers must be
        // dropped so a forged tenant can't be trusted; tenant scoping then fails closed.
        let mut req = Request::new(());
        req.metadata_mut()
            .insert(PRINCIPAL_KEY, "attacker".parse().unwrap());
        req.metadata_mut()
            .insert(TENANT_KEY, "victim-tenant".parse().unwrap());
        req.metadata_mut()
            .insert(ROLES_KEY, "admin".parse().unwrap());
        strip_identity(&mut req);
        assert!(req.metadata().get(PRINCIPAL_KEY).is_none());
        assert!(req.metadata().get(TENANT_KEY).is_none());
        assert!(req.metadata().get(ROLES_KEY).is_none());
    }

    #[test]
    fn authenticate_maps_failure_to_unauthenticated() {
        let shared: SharedAuthn = Arc::new(authn());
        let mut req = request_with(None, Some("attacker"));
        let status = authenticate(&shared, &mut req).unwrap_err();
        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[test]
    fn anonymous_ignores_any_credential() {
        let anon = Anonymous;
        let verified = anon.authenticate(Some("Bearer whatever")).unwrap();
        assert_eq!(verified.principal, "anonymous");
        assert_eq!(verified.tenant, None);
        assert!(verified.roles.is_empty());
        // Absent credential is equally fine under the open default.
        assert_eq!(anon.authenticate(None).unwrap(), verified);
    }

    // ---- JWKS / RS256 (slice 3) -------------------------------------------------------

    /// A throwaway 2048-bit RSA test key (private PEM + the matching public JWK below).
    const RSA_PEM: &str = include_str!("testdata/jwt_rsa_test_key.pem");
    const RSA_KID: &str = "test-key";
    const RSA_N: &str = "xgJS0AHQWHRObIww_jDKrEDNu2oPcUaloAC3w8zkOySbhlKaUwhluF_lJzgQiNG048tQhyL_eGee7y4msIwRSy0S9pXKvSquHSAbAXDG1Abr1pHKVE2X0bYY4VgjS-Oe2ro00c0hwLSXV8AIYxlPzgwVDRokxdc4gkfYSf1UFoXaX1u-1aYg_UHkuY992ieg_PWgeZTX5phRJDBX02fp2Wvx2OSjjMekjuYQDgdoocE1-TRGUHV2Md42yCKRt3iQy0_DK55LN53TpJOlkocpeO_7KzKEDnoSLXmqUI5KF4nBKrV7TcPsiZsUxVEo0bJwZdTOfNUtmUT_Fj85Xdi8hQ";

    /// The JWKS exposing the test key (what an IdP's `jwks_uri` would return).
    fn test_jwks() -> JwkSet {
        serde_json::from_value(json!({
            "keys": [{
                "kty": "RSA", "use": "sig", "alg": "RS256",
                "kid": RSA_KID, "n": RSA_N, "e": "AQAB",
            }]
        }))
        .unwrap()
    }

    /// Sign `claims` as an RS256 JWT with the test key, stamping `kid`.
    fn rs256(claims: JsonValue, kid: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        encode(
            &header,
            &claims,
            &EncodingKey::from_rsa_pem(RSA_PEM.as_bytes()).unwrap(),
        )
        .unwrap()
    }

    fn jwks_authn() -> JwksAuthenticator {
        JwksAuthenticator::from_jwk_set(&test_jwks(), ISSUER, AUDIENCE).unwrap()
    }

    #[test]
    fn jwks_validates_an_rs256_token_by_kid() {
        let token = rs256(good_claims(), RSA_KID);
        let verified = jwks_authn().authenticate(Some(&bearer(&token))).unwrap();
        assert_eq!(verified.principal, "alice");
        assert_eq!(verified.tenant.as_deref(), Some("acme"));
        assert_eq!(verified.roles, vec!["viewer", "index-admin"]);
    }

    #[test]
    fn jwks_rejects_an_unknown_kid() {
        // Signed (validly) but with a kid the key set doesn't carry → no key to verify against.
        let token = rs256(good_claims(), "some-other-kid");
        let err = jwks_authn()
            .authenticate(Some(&bearer(&token)))
            .unwrap_err();
        assert!(matches!(err, AuthnError::Invalid(_)));
    }

    #[test]
    fn jwks_rejects_a_token_with_no_kid() {
        let token = encode(
            &Header::new(Algorithm::RS256),
            &good_claims(),
            &EncodingKey::from_rsa_pem(RSA_PEM.as_bytes()).unwrap(),
        )
        .unwrap();
        let err = jwks_authn()
            .authenticate(Some(&bearer(&token)))
            .unwrap_err();
        assert!(matches!(err, AuthnError::Invalid(_)));
    }

    #[test]
    fn jwks_still_checks_issuer_and_expiry() {
        let mut claims = good_claims();
        claims["iss"] = json!("https://evil.example");
        let token = rs256(claims, RSA_KID);
        let err = jwks_authn()
            .authenticate(Some(&bearer(&token)))
            .unwrap_err();
        assert!(matches!(err, AuthnError::Invalid(_)));
    }

    #[test]
    fn build_key_map_rejects_a_keyless_set() {
        // A JWKS whose only key has no `kid` yields nothing usable.
        let jwks: JwkSet = serde_json::from_value(json!({
            "keys": [{ "kty": "RSA", "use": "sig", "alg": "RS256", "n": RSA_N, "e": "AQAB" }]
        }))
        .unwrap();
        assert!(matches!(
            build_key_map(&jwks),
            Err(AuthnError::Discovery(_))
        ));
    }

    // ---- API keys (slice 4) -----------------------------------------------------------

    fn identity(principal: &str, tenant: Option<&str>, roles: &[&str]) -> KeyIdentity {
        KeyIdentity {
            principal: principal.to_string(),
            tenant: tenant.map(str::to_string),
            roles: roles.iter().map(|r| r.to_string()).collect(),
            indexes: Vec::new(),
        }
    }

    #[test]
    fn api_key_round_trip_issue_authenticate_revoke() {
        let store = ApiKeyStore::new();
        let key = store.issue(identity("svc-ingest", Some("acme"), &["service"]));

        let verified = store.authenticate(Some(&format!("ApiKey {key}"))).unwrap();
        assert_eq!(verified.principal, "svc-ingest");
        assert_eq!(verified.tenant.as_deref(), Some("acme"));
        assert_eq!(verified.roles, vec!["service"]);

        // Revocation is immediate.
        assert!(store.revoke(&key));
        let err = store
            .authenticate(Some(&format!("ApiKey {key}")))
            .unwrap_err();
        assert!(matches!(err, AuthnError::Invalid(_)));
        // Revoking again is a no-op.
        assert!(!store.revoke(&key));
    }

    #[test]
    fn registry_token_authenticator_validates_then_rejects_a_revoked_token() {
        // task-105: a registry-backed authenticator validates a live API token by its hash and
        // resolves its roles; a revoked token fails authentication.
        let tmp = tempfile::tempdir().unwrap();
        let reg = std::sync::Arc::new(
            growlerdb_controlplane::Registry::open(tmp.path().join("r.json")).unwrap(),
        );
        let (secret, hash) = mint_api_token();
        assert!(secret.starts_with("gdb_live_"));
        reg.create_token(growlerdb_controlplane::ApiToken {
            id: "t1".into(),
            label: "ci".into(),
            prefix: secret.chars().take(13).collect(),
            hash,
            roles: vec!["reader".into()],
            owner: "svc".into(),
            created_at_ms: 0,
            expires_at_ms: None,
        })
        .unwrap();
        let authn = RegistryTokenAuthenticator::new(reg.clone());

        let v = authn
            .authenticate(Some(&format!("ApiKey {secret}")))
            .unwrap();
        assert_eq!(v.principal, "svc");
        assert_eq!(v.roles, vec!["reader".to_string()]);

        // Revoke → the same secret no longer authenticates.
        reg.revoke_token("t1").unwrap();
        assert!(authn
            .authenticate(Some(&format!("ApiKey {secret}")))
            .is_err());
    }

    #[test]
    fn api_key_unknown_or_wrong_scheme_is_rejected() {
        let store = ApiKeyStore::new();
        // An unissued key → invalid.
        assert!(matches!(
            store.authenticate(Some("ApiKey never-issued")),
            Err(AuthnError::Invalid(_))
        ));
        // A bearer token presented to the key store → malformed (wrong scheme).
        assert_eq!(
            store.authenticate(Some("Bearer xyz")).unwrap_err(),
            AuthnError::Malformed
        );
        assert_eq!(store.authenticate(None).unwrap_err(), AuthnError::Missing);
    }

    #[test]
    fn api_keys_are_stored_hashed_not_in_the_clear() {
        // The raw secret never appears as a stored map key — only its digest.
        let store = ApiKeyStore::new();
        let key = store.issue(identity("svc", None, &[]));
        let stored: Vec<String> = store.keys.read().unwrap().keys().cloned().collect();
        assert_eq!(stored.len(), 1);
        assert_ne!(stored[0], key);
        assert_eq!(stored[0], digest(&key));
    }

    // ---- Chain (slice 4) --------------------------------------------------------------

    #[test]
    fn chain_routes_by_scheme() {
        let store = Arc::new(ApiKeyStore::new());
        let key = store.issue(identity("svc", Some("acme"), &["service"]));
        let jwt: SharedAuthn = Arc::new(authn());
        let chain = ChainAuthenticator::new()
            .with_bearer(jwt)
            .with_api_keys(store.clone());

        // Bearer → the JWT authenticator.
        let token = hs256(good_claims());
        assert_eq!(
            chain.authenticate(Some(&bearer(&token))).unwrap().principal,
            "alice"
        );
        // ApiKey → the key store.
        assert_eq!(
            chain
                .authenticate(Some(&format!("ApiKey {key}")))
                .unwrap()
                .principal,
            "svc"
        );
        // An unknown scheme is malformed.
        assert_eq!(
            chain.authenticate(Some("Basic abc")).unwrap_err(),
            AuthnError::Malformed
        );
    }

    #[test]
    fn chain_rejects_a_scheme_with_no_configured_authenticator() {
        // Only API keys configured; a Bearer token has nowhere to go.
        let chain = ChainAuthenticator::new().with_api_keys(Arc::new(ApiKeyStore::new()));
        assert_eq!(
            chain.authenticate(Some("Bearer xyz")).unwrap_err(),
            AuthnError::Malformed
        );
    }
}
