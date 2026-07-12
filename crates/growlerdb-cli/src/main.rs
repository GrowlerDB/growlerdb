//! Thin binary entrypoint. The command implementations live in the library
//! (`growlerdb_cli`) so they can be reused out-of-tree — e.g. an enterprise
//! build that injects its own authenticator into the gateway (see `GatewayConfig::authn`).
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    growlerdb_cli::run().await
}
