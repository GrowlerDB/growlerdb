//! Error type surfaced by the [`GatewayClient`](crate::client::GatewayClient).
//!
//! Every failure here is turned into an MCP **tool error** (`isError: true` with the message as
//! text) rather than a panic or a JSON-RPC protocol error — a failed retrieval is a normal result
//! an agent should read and react to, not a crash of the transport.

/// A failure talking to the GrowlerDB gateway.
#[derive(Debug, thiserror::Error)]
pub enum McpError {
    /// The gateway answered with a non-2xx status and a `{code, message}` error body.
    #[error("gateway {status} {code}: {message}")]
    Gateway {
        /// HTTP status code.
        status: u16,
        /// Machine-readable error code from the gateway body (e.g. `NotFound`).
        code: String,
        /// Human-readable message from the gateway body.
        message: String,
    },

    /// A transport-level failure (connection refused, TLS, timeout, malformed body).
    #[error("gateway transport error: {0}")]
    Transport(#[from] reqwest::Error),

    /// A caller/config problem detected before the request left the process
    /// (e.g. no index could be resolved, a required argument was missing).
    #[error("{0}")]
    Config(String),
}
