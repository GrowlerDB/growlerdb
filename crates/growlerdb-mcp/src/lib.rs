//! # growlerdb-mcp
//!
//! A **read-only** [Model Context Protocol](https://modelcontextprotocol.io) server for GrowlerDB.
//! It speaks JSON-RPC 2.0 over stdio (newline-delimited) and fronts the GrowlerDB **gateway** over
//! HTTP, forwarding the caller's bearer token so the gateway's existing RBAC + tenant isolation
//! govern every read — an agent can never reach another tenant's data.
//!
//! It exposes five tools — `search`, `hydrate`, `aggregate`, `list_indexes`, `describe_index` — and
//! embeds no engine (no ingest/write/admin surface). See [`serve`] for the entry point.

mod backend;
mod client;
mod error;
mod server;

pub use backend::{interpret_response, QueryBackend};
pub use client::GatewayClient;
pub use error::McpError;
pub use server::{handle_message, serve, serve_io, McpConfig, DEFAULT_PROTOCOL_VERSION};
