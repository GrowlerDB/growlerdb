//! **Service-credential auth for the control plane** — the service-to-service layer that closes
//! the internal control-plane RPCs (registration, shard-map reads, placement) to callers that
//! don't share the cluster's `GROWLERDB_SERVICE_TOKEN`. It is *separate* from the per-user auth
//! (`--login-secret` / RBAC): mesh peers prove "I am a cluster service" with the shared token,
//! independent of any user identity a request carries.
//!
//! This module holds the **client** side (attach the token on every control-plane call) plus the
//! shared metadata key; the server-side verification lives with the control-plane service (it needs
//! a constant-time compare). When no token is configured the interceptor is a no-op, so bare local
//! dev stays open.

use tonic::metadata::MetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Request, Status};

use crate::ControlPlaneClient;

/// Metadata key carrying the shared service token on internal control-plane calls. Distinct from
/// the user-auth `authorization` bearer the gateway forwards, so the two layers never collide.
pub const SERVICE_TOKEN_KEY: &str = "x-growlerdb-service-token";

/// Time to establish a connection to the control plane before giving up.
const CONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// A tonic client interceptor that stamps the shared service token onto every request. `None` ⇒ a
/// no-op (open local dev), so the same client construction path serves both the closed mesh and a
/// bare-dev control plane.
#[derive(Clone)]
pub struct ServiceTokenInterceptor {
    token: Option<MetadataValue<tonic::metadata::Ascii>>,
}

impl ServiceTokenInterceptor {
    /// An interceptor stamping `token`, or a no-op when it is `None`/empty. Reads the process-wide
    /// [`GROWLERDB_SERVICE_TOKEN`](service_token_from_env) via the caller; here it just holds the value.
    pub fn new(token: Option<&str>) -> Self {
        let token = token
            .filter(|t| !t.is_empty())
            // A token that can't parse as an ASCII header value is treated as unset rather than
            // failing every call — the misconfiguration surfaces as an unauthenticated reject.
            .and_then(|t| MetadataValue::try_from(t).ok());
        Self { token }
    }
}

impl tonic::service::Interceptor for ServiceTokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = &self.token {
            request
                .metadata_mut()
                .insert(SERVICE_TOKEN_KEY, token.clone());
        }
        Ok(request)
    }
}

/// A control-plane client with the service-token interceptor installed — the single client type
/// every control-plane caller (node, gateway, CLI) uses, so the token rides every call.
pub type CpClient = ControlPlaneClient<InterceptedService<Channel, ServiceTokenInterceptor>>;

/// The shared service token from `GROWLERDB_SERVICE_TOKEN`, or `None` when unset/empty (open dev).
pub fn service_token_from_env() -> Option<String> {
    std::env::var("GROWLERDB_SERVICE_TOKEN")
        .ok()
        .filter(|t| !t.is_empty())
}

/// Build the control-plane [`Endpoint`] for `endpoint`, applying `tls` when set. Shared by the
/// eager and lazy connect paths.
fn cp_endpoint(
    endpoint: String,
    tls: Option<ClientTlsConfig>,
) -> Result<Endpoint, tonic::transport::Error> {
    let ep = Endpoint::from_shared(endpoint)?.connect_timeout(CONNECT_TIMEOUT);
    match tls {
        Some(tls) => ep.tls_config(tls),
        None => Ok(ep),
    }
}

/// Connect a [`CpClient`] to the control plane at `endpoint`, over `tls` when set, attaching
/// `token` on every call. Establishes the connection now (fails if unreachable).
pub async fn connect(
    endpoint: impl Into<String>,
    tls: Option<ClientTlsConfig>,
    token: Option<&str>,
) -> Result<CpClient, tonic::transport::Error> {
    let channel = cp_endpoint(endpoint.into(), tls)?.connect().await?;
    Ok(ControlPlaneClient::with_interceptor(
        channel,
        ServiceTokenInterceptor::new(token),
    ))
}

/// Like [`connect`], but **lazy**: build the channel without dialing now (opens on first use, and
/// re-resolves DNS on reconnect). Building never fails on an unreachable control plane.
pub fn connect_lazy(
    endpoint: impl Into<String>,
    tls: Option<ClientTlsConfig>,
    token: Option<&str>,
) -> Result<CpClient, tonic::transport::Error> {
    let channel = cp_endpoint(endpoint.into(), tls)?.connect_lazy();
    Ok(ControlPlaneClient::with_interceptor(
        channel,
        ServiceTokenInterceptor::new(token),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::service::Interceptor;

    #[test]
    fn interceptor_stamps_token_when_set() {
        let mut ic = ServiceTokenInterceptor::new(Some("s3cr3t"));
        let req = ic.call(Request::new(())).unwrap();
        assert_eq!(
            req.metadata()
                .get(SERVICE_TOKEN_KEY)
                .unwrap()
                .to_str()
                .unwrap(),
            "s3cr3t"
        );
    }

    #[test]
    fn interceptor_is_noop_when_unset() {
        let mut ic = ServiceTokenInterceptor::new(None);
        let req = ic.call(Request::new(())).unwrap();
        assert!(req.metadata().get(SERVICE_TOKEN_KEY).is_none());
        // Empty is treated as unset.
        let mut ic = ServiceTokenInterceptor::new(Some(""));
        let req = ic.call(Request::new(())).unwrap();
        assert!(req.metadata().get(SERVICE_TOKEN_KEY).is_none());
    }
}
