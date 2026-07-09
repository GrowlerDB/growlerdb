//! **mTLS for internal services** (task-35 slice 5, M4): the tonic TLS configs that secure the
//! Gateway↔Node hops. Internal traffic is *mutually* authenticated — a server presents its own
//! identity **and** requires the client to present a certificate signed by the shared cluster
//! CA, so only cluster peers (not arbitrary network clients) can reach a Node. This is the
//! transport-trust layer beneath the per-request [authentication](crate::authn): mTLS proves
//! "you are a cluster service", AuthN proves "you are this user/client".
//!
//! These are thin builders over [`tonic::transport`] TLS types from PEM material; the CLI reads
//! the PEM files and installs the result (server via `Server::builder().tls_config(...)`, client
//! via [`RemoteNode::connect_with_tls`](crate::node::RemoteNode::connect_with_tls)).

use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

/// Server-side mTLS: present `cert_pem`/`key_pem` as this service's identity and **require**
/// every client to present a certificate that chains to `client_ca_pem`. A client with no
/// certificate (or one signed by another CA) fails the handshake before any RPC runs.
pub fn server_mtls(cert_pem: &[u8], key_pem: &[u8], client_ca_pem: &[u8]) -> ServerTlsConfig {
    ServerTlsConfig::new()
        .identity(Identity::from_pem(cert_pem, key_pem))
        .client_ca_root(Certificate::from_pem(client_ca_pem))
}

/// Client-side mTLS: trust servers whose certificate chains to `ca_pem`, verify the server's
/// identity against `domain` (must match the server cert's SAN), and present `cert_pem`/
/// `key_pem` as this client's identity. Used by a [`RemoteNode`](crate::node::RemoteNode)
/// dialing a Node.
pub fn client_mtls(
    ca_pem: &[u8],
    cert_pem: &[u8],
    key_pem: &[u8],
    domain: &str,
) -> ClientTlsConfig {
    ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(ca_pem))
        .identity(Identity::from_pem(cert_pem, key_pem))
        .domain_name(domain)
}
