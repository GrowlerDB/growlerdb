//! **Service-credential enforcement for the control plane** (server side). When a service token is
//! configured, a tonic interceptor rejects any internal RPC lacking the matching
//! `x-growlerdb-service-token`, closing registration/placement to callers outside the cluster mesh —
//! a layer distinct from the per-user [auth hook](crate::auth)/RBAC, gating regardless of user-auth
//! mode. No token configured ⇒ a no-op (bare local dev stays open). Client counterpart:
//! [`growlerdb_proto::service_token`](growlerdb_proto::service_token).

use growlerdb_proto::service_token::SERVICE_TOKEN_KEY;
use sha2::{Digest, Sha256};
use tonic::service::interceptor::InterceptedService;
use tonic::{Request, Status};

/// Constant-time token comparison, so a network attacker can't recover the token byte-by-byte from
/// response timing. Both sides are hashed to a fixed-width digest (constant work regardless of
/// length), then compared with a branch-free bit accumulator.
fn tokens_match(presented: &[u8], expected: &[u8]) -> bool {
    let a = Sha256::digest(presented);
    let b = Sha256::digest(expected);
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// A tonic interceptor that requires every request to carry the shared service token. Built from
/// the process's configured token via [`intercept`]; with no token configured it lets every request
/// through (so the control plane stays open in bare dev).
#[derive(Clone)]
pub struct ServiceTokenAuth {
    expected: std::sync::Arc<Vec<u8>>,
}

impl tonic::service::Interceptor for ServiceTokenAuth {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        // Open control plane (no token configured): let every request through — bare local dev.
        if self.expected.is_empty() {
            return Ok(request);
        }
        let presented = request
            .metadata()
            .get(SERVICE_TOKEN_KEY)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        if presented.is_empty() || !tokens_match(presented.as_bytes(), &self.expected) {
            return Err(Status::unauthenticated(
                "this service requires a valid service token",
            ));
        }
        Ok(request)
    }
}

/// A whole-server tower layer applying the service-token requirement to **every** service a tonic
/// server mounts — the data-plane (Node) counterpart of [`intercept`] (which wraps one service). A
/// Node carries no per-user auth in distributed mode (that lives at the Gateway), so the token adds
/// defense-in-depth for a directly-reachable Node port. `None`/empty ⇒ a no-op layer (open dev).
pub fn layer(
    token: Option<String>,
) -> tonic::service::interceptor::InterceptorLayer<ServiceTokenAuth> {
    let expected = token.filter(|t| !t.is_empty()).map(|t| t.into_bytes());
    tonic::service::interceptor::InterceptorLayer::new(ServiceTokenAuth::from_expected(expected))
}

/// Wrap `service` so every RPC must present the matching service token, when `token` is set.
/// `None` ⇒ the service is returned unwrapped-in-behavior (a no-op interceptor), keeping the
/// control plane open for bare local dev. Returns an [`InterceptedService`] either way so the call
/// site has one type.
pub fn intercept<S>(service: S, token: Option<String>) -> InterceptedService<S, ServiceTokenAuth> {
    // Keep one return type for both modes: an unset/empty token installs an interceptor that allows
    // every request (open dev); a set token installs one that requires the match.
    let expected = token.filter(|t| !t.is_empty()).map(|t| t.into_bytes());
    InterceptedService::new(service, ServiceTokenAuth::from_expected(expected))
}

impl ServiceTokenAuth {
    fn from_expected(expected: Option<Vec<u8>>) -> Self {
        // Sentinel: an empty expected marks the open (no-token) control plane; the interceptor lets
        // every request through in that case, so bare dev needs no credential.
        Self {
            expected: std::sync::Arc::new(expected.unwrap_or_default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_proto::service_token::SERVICE_TOKEN_KEY;
    use tonic::service::Interceptor;

    fn req_with(token: Option<&str>) -> Request<()> {
        let mut req = Request::new(());
        if let Some(t) = token {
            req.metadata_mut()
                .insert(SERVICE_TOKEN_KEY, t.parse().unwrap());
        }
        req
    }

    #[test]
    fn constant_time_compare_agrees_with_equality() {
        assert!(tokens_match(b"abc", b"abc"));
        assert!(!tokens_match(b"abc", b"xyz")); // same length, differs
        assert!(!tokens_match(b"abc", b"abcd")); // differs in length
        assert!(!tokens_match(b"", b"x"));
    }

    #[test]
    fn open_control_plane_allows_missing_token() {
        // No configured token ⇒ every request passes, even without a token.
        let mut auth = ServiceTokenAuth::from_expected(None);
        assert!(auth.call(req_with(None)).is_ok());
        assert!(auth.call(req_with(Some("anything"))).is_ok());
    }

    #[test]
    fn configured_token_rejects_missing_and_wrong() {
        let mut auth = ServiceTokenAuth::from_expected(Some(b"right".to_vec()));
        assert_eq!(
            auth.call(req_with(None)).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert_eq!(
            auth.call(req_with(Some("wrong"))).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        assert!(auth.call(req_with(Some("right"))).is_ok());
    }
}
