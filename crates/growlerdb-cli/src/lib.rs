//! `growlerdb` — the GrowlerDB CLI. In embedded mode (no server, auth, or sharding) it drives
//! the in-process [`Engine`](growlerdb_engine::Engine) over the local index store + Iceberg.

use clap::{Args, Parser, Subcommand};
use growlerdb_core::{CompositeKey, HydratedRow, Projection, Value};
use growlerdb_engine::Engine;
use growlerdb_source::IcebergConfig;

/// The version reported by `--version` and the System gRPC service. Baked by `build.rs` from the
/// release tag (`GROWLERDB_VERSION`) when set, else the in-tree workspace `0.0.0` (see RELEASING.md
/// — artifacts are tag-derived, the tree stays `0.0.0`).
const VERSION: &str = env!("GROWLERDB_BUILD_VERSION");

/// Server-side mutual-TLS for an internal gRPC service: present `--tls-cert`/`--tls-key` as this
/// service's identity and require every client to present a cert chaining to `--tls-client-ca`.
/// All three together enable mTLS; omit them to serve plaintext (dev).
#[derive(Args, Clone)]
struct ServerTlsArgs {
    /// PEM certificate for this service's TLS identity.
    #[arg(long, requires_all = ["tls_key", "tls_client_ca"])]
    tls_cert: Option<String>,
    /// PEM private key for `--tls-cert`.
    #[arg(long)]
    tls_key: Option<String>,
    /// PEM CA that client certificates must chain to (mutual TLS).
    #[arg(long)]
    tls_client_ca: Option<String>,
}

impl ServerTlsArgs {
    /// Build the [`ServerTlsConfig`](tonic::transport::ServerTlsConfig) when mTLS is requested
    /// (`--tls-cert` set), reading the PEM files; `None` means serve plaintext.
    fn load(&self) -> anyhow::Result<Option<tonic::transport::ServerTlsConfig>> {
        let Some(cert) = &self.tls_cert else {
            return Ok(None);
        };
        // clap's `requires_all` guarantees the other two are present alongside `--tls-cert`.
        let key = self.tls_key.as_ref().expect("clap requires_all");
        let ca = self.tls_client_ca.as_ref().expect("clap requires_all");
        Ok(Some(growlerdb_engine::tls::server_mtls(
            &std::fs::read(cert)
                .map_err(|e| anyhow::anyhow!("reading --tls-cert `{cert}`: {e}"))?,
            &std::fs::read(key).map_err(|e| anyhow::anyhow!("reading --tls-key `{key}`: {e}"))?,
            &std::fs::read(ca)
                .map_err(|e| anyhow::anyhow!("reading --tls-client-ca `{ca}`: {e}"))?,
        )))
    }
}

/// Client-side TLS for a Gateway dialing internal Nodes: verify Node server certs against
/// `--node-tls-ca` and present `--node-tls-cert`/`--node-tls-key` as the Gateway's client
/// identity (mutual TLS). Enabled by `--node-tls-ca`.
#[derive(Args, Clone)]
struct UpstreamTlsArgs {
    /// PEM CA used to verify Node server certificates (enables mutual TLS to Nodes; requires
    /// `--node-tls-cert`/`--node-tls-key`).
    #[arg(long)]
    node_tls_ca: Option<String>,
    /// PEM client certificate the Gateway presents to Nodes.
    #[arg(long, requires = "node_tls_ca")]
    node_tls_cert: Option<String>,
    /// PEM private key for `--node-tls-cert`.
    #[arg(long, requires = "node_tls_cert")]
    node_tls_key: Option<String>,
    /// Expected server-certificate domain (SAN) when connecting to Nodes (default `localhost`).
    #[arg(long, requires = "node_tls_ca", default_value = "localhost")]
    node_tls_domain: String,
}

impl UpstreamTlsArgs {
    /// Build the [`ClientTlsConfig`](tonic::transport::ClientTlsConfig) when TLS to Nodes is
    /// requested (`--node-tls-ca` set); `None` means connect plaintext. Internal traffic is
    /// mutual, so a client cert+key is required alongside the CA.
    fn load(&self) -> anyhow::Result<Option<tonic::transport::ClientTlsConfig>> {
        let Some(ca) = &self.node_tls_ca else {
            return Ok(None);
        };
        let cert = self.node_tls_cert.as_ref().ok_or_else(|| {
            anyhow::anyhow!("--node-tls-cert is required with --node-tls-ca (mutual TLS)")
        })?;
        let key = self
            .node_tls_key
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("--node-tls-key is required with --node-tls-cert"))?;
        let read = |label: &str, path: &str| {
            std::fs::read(path).map_err(|e| anyhow::anyhow!("reading {label} `{path}`: {e}"))
        };
        Ok(Some(growlerdb_engine::tls::client_mtls(
            &read("--node-tls-ca", ca)?,
            &read("--node-tls-cert", cert)?,
            &read("--node-tls-key", key)?,
            &self.node_tls_domain,
        )))
    }
}

/// Client-side TLS to the **control plane**, from the environment so every process that dials the
/// control plane (node, gateway, CLI) configures it uniformly without threading flags through each
/// call site. Enabled by `GROWLERDB_CP_TLS_CA` (PEM CA verifying the control-plane server cert);
/// `GROWLERDB_CP_TLS_CERT`/`_KEY` add a client identity for mTLS, and `GROWLERDB_CP_TLS_DOMAIN`
/// (default `localhost`) is the expected server SAN. Unset ⇒ plaintext (the loopback demo).
fn cp_client_tls_from_env() -> anyhow::Result<Option<tonic::transport::ClientTlsConfig>> {
    use tonic::transport::{Certificate, ClientTlsConfig, Identity};
    let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    let Some(ca) = var("GROWLERDB_CP_TLS_CA") else {
        return Ok(None);
    };
    let read = |label: &str, path: &str| {
        std::fs::read(path).map_err(|e| anyhow::anyhow!("reading {label} `{path}`: {e}"))
    };
    let domain = var("GROWLERDB_CP_TLS_DOMAIN").unwrap_or_else(|| "localhost".to_string());
    let mut tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(read("GROWLERDB_CP_TLS_CA", &ca)?))
        .domain_name(domain);
    // A client cert+key is optional (server-only TLS) unless the control plane requires mTLS.
    if let Some(cert) = var("GROWLERDB_CP_TLS_CERT") {
        let key = var("GROWLERDB_CP_TLS_KEY").ok_or_else(|| {
            anyhow::anyhow!("GROWLERDB_CP_TLS_KEY is required with GROWLERDB_CP_TLS_CERT")
        })?;
        tls = tls.identity(Identity::from_pem(
            read("GROWLERDB_CP_TLS_CERT", &cert)?,
            read("GROWLERDB_CP_TLS_KEY", &key)?,
        ));
    }
    Ok(Some(tls))
}

/// Connect a control-plane client to `endpoint`, attaching the shared service token
/// (`GROWLERDB_SERVICE_TOKEN`) and applying client TLS ([`cp_client_tls_from_env`]) when configured.
/// The single construction path for every control-plane caller. `lazy` builds the channel without
/// dialing now (for background reloaders that tolerate an unreachable control plane at boot).
async fn connect_cp(
    endpoint: &str,
    lazy: bool,
) -> anyhow::Result<growlerdb_proto::service_token::CpClient> {
    let tls = cp_client_tls_from_env()?;
    let token = growlerdb_proto::service_token::service_token_from_env();
    if lazy {
        growlerdb_proto::service_token::connect_lazy(endpoint, tls, token.as_deref())
            .map_err(|e| anyhow::anyhow!("connecting to control plane `{endpoint}`: {e}"))
    } else {
        growlerdb_proto::service_token::connect(endpoint, tls, token.as_deref())
            .await
            .map_err(|e| anyhow::anyhow!("connecting to control plane `{endpoint}`: {e}"))
    }
}

#[derive(Parser)]
#[command(
    name = "growlerdb",
    version = VERSION,
    about = "Open-source text search over Apache Iceberg"
)]
struct Cli {
    /// Local index store directory (env: `GROWLERDB_DATA_DIR`).
    #[arg(
        long,
        default_value = ".growlerdb",
        env = "GROWLERDB_DATA_DIR",
        global = true
    )]
    data_dir: String,

    /// Serve health/readiness probes (`/healthz`, `/readyz`) and Prometheus `/metrics` on this
    /// `host:port`. Applies to the long-running server commands; omit to disable.
    #[arg(long, global = true)]
    metrics_addr: Option<String>,

    /// Serve the built UI SPA (`ui/dist`) from the REST front — the GrowlerDB console at the same
    /// `host:port` as the Engine API. Omit to run API-only.
    #[arg(long, env = "GROWLERDB_UI_DIR", global = true)]
    ui_dir: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Build a local index from a source table.
    Index {
        /// Source table identifier (e.g. `namespace.table`).
        table: String,
        /// Path to an index-definition YAML (optional; otherwise auto-mapped).
        #[arg(long)]
        def: Option<String>,
        /// Index name (defaults to the table's last segment).
        #[arg(long)]
        name: Option<String>,
        /// Total shards in the cluster. `>1` builds only **this** node's partition, so a
        /// broadcast search over the shards sees each document once. Default 1 = full build.
        #[arg(long, default_value_t = 1)]
        shards: u32,
        /// This node's shard ordinal in `0..shards`. Pair with `--shards`.
        #[arg(long, default_value_t = 0)]
        shard_ordinal: u32,
        /// Write `index.json` (the resolved definition) only — build **no** shards/windows.
        /// A **windowed** node starts empty this way: it needs the definition on disk to `serve`, but
        /// must not batch-build windows from the source (that replicates every window onto every node
        /// and defeats control-plane placement). Ignores `--shards`/`--shard-ordinal`.
        #[arg(long, default_value_t = false)]
        define_only: bool,
    },
    /// Search an index and print ranked document coordinates.
    Search {
        /// Index name.
        index: String,
        /// Query string (Lucene/KQL-style).
        query: String,
        /// Maximum number of hits.
        #[arg(short = 'k', long, default_value_t = 10)]
        limit: usize,
        /// Also hydrate the authoritative rows from Iceberg.
        #[arg(long)]
        hydrate: bool,
        /// Comma-separated columns to return when hydrating (default: all).
        #[arg(long, value_delimiter = ',')]
        fields: Vec<String>,
    },
    /// Append fast-path sync: index files added since the last checkpoint
    /// (APPEND_FAST_PATH indexes only). Cheaper than changelog for immutable tables.
    Sync {
        /// Index name (must already be built).
        index: String,
    },
    /// Drift reconciliation: compare the index against Iceberg's current snapshot and
    /// repair discrepancies (delete vanished keys, re-index new ones).
    ///
    /// Without `--control-plane`, reconciles the local embedded index (single-shard dev path).
    /// With `--control-plane host:port`, drives the **cluster** backstop: fetch the index's shard
    /// map + bucket owners from the registry and fan a shard-scoped `ReconcileIndex` out to every
    /// shard's primary node — the form a scheduled CronJob runs.
    Reconcile {
        /// Index name.
        index: String,
        /// Control-plane `host:port`. Set ⇒ cluster mode (fan out to each shard's node).
        #[arg(long)]
        control_plane: Option<String>,
        /// Force a full row-level scan, bypassing the count-gate. Use for a periodic deep
        /// sweep that catches drift the count-gate can't (compensating stale+missing, or dup PKs).
        #[arg(long)]
        full: bool,
    },
    /// Hard reset: drop the index and rebuild it from Iceberg (the backstop).
    Rebuild {
        /// Index name.
        index: String,
    },
    /// Back up an index's shard to object storage (S3/MinIO) for restore on node loss.
    /// Reads credentials from `GROWLERDB_S3_*` and the bucket from `GROWLERDB_BACKUP_BUCKET`.
    Backup {
        /// Index name (must be built locally).
        index: String,
        /// Object-store key prefix (default: `backups/<index>`).
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Restore an index's shard from an object-storage backup; if none exists, rebuild from
    /// Iceberg (the backstop). The connector then resumes the tail from the backed-up checkpoint.
    Restore {
        /// Index name.
        index: String,
        /// Object-store key prefix (default: `backups/<index>`).
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Refresh a **replica** of an index from the primary's backup — incremental segment shipping:
    /// pulls only new sealed segments, byte-identical to the primary. Run on a timer (then `serve`
    /// the index) for a warm read-replica.
    RefreshReplica {
        /// Index name.
        index: String,
        /// Object-store key prefix (default: `backups/<index>`).
        #[arg(long)]
        prefix: Option<String>,
    },
    /// Move cold (time-aged) windows of a **windowed** index to object storage, evicting the local
    /// index bulk while keeping the window **searchable read-through**. Keeps the
    /// most-recent windows hot per the index's `hot_windows` policy (or `--keep-hot`). Reads
    /// `GROWLERDB_S3_*` + `GROWLERDB_BACKUP_BUCKET`; `growlerdb revive` promotes a window back to hot.
    Park {
        /// Index name (must be a built windowed index).
        index: String,
        /// Keep this many most-recent windows hot, overriding the index's `hot_windows` policy.
        #[arg(long)]
        keep_hot: Option<usize>,
    },
    /// Promote a cold window back to hot: restore its bulk to local NVMe so it serves locally
    /// again (a cold window is already searchable read-through; this pre-warms it). Reads
    /// `GROWLERDB_S3_*` + `GROWLERDB_BACKUP_BUCKET`.
    Revive {
        /// Index name.
        index: String,
        /// Window id (epoch-ms of the window start) to promote back to hot.
        window: i64,
    },
    /// Retention: drop the **oldest** indexes matching a `*`-glob `pattern` beyond `--keep`,
    /// e.g. roll off old daily indexes once you've rolled to a new one. Names sort
    /// chronologically when they embed a date (`events-2025-06-15`). Goes through the control plane.
    Retention {
        /// Index name pattern (a `*`-glob, e.g. `events-*`).
        pattern: String,
        /// Keep this many most-recent (highest-sorted) matching indexes; drop the rest.
        #[arg(long)]
        keep: usize,
        /// Control-plane gRPC endpoint (e.g. `http://controlplane:50071`).
        #[arg(long)]
        control_plane: String,
        /// List what would be dropped without dropping.
        #[arg(long)]
        dry_run: bool,
    },
    /// Run a Node server: host the Write (+ System) gRPC services for an index.
    Serve {
        /// Index name (must already be defined, e.g. via `growlerdb index`).
        index: String,
        /// Address to bind (`host:port`).
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
        /// Max concurrent in-flight writes before backpressure (`RESOURCE_EXHAUSTED`).
        #[arg(long, default_value_t = 32)]
        max_inflight: usize,
        /// Also serve the REST/JSON gateway (`/v1/...`) on this `host:port`. Omit to
        /// run gRPC only.
        #[arg(long)]
        rest_addr: Option<String>,
        /// Self-register this served index in the Control-Plane registry at this gRPC endpoint
        /// (e.g. `http://controlplane:50071`), so a node-built index is discoverable cluster-wide
        /// (the Indexes/Ingestion screens) instead of invisible until `CreateIndex`. The node
        /// announces its shard assignment at `--advertise-addr`.
        #[arg(long, requires = "advertise_addr")]
        register: Option<String>,
        /// The routable gRPC endpoint other services reach this node at (e.g.
        /// `http://node:50051`) — recorded as the shard primary when `--register` is set. Required
        /// with `--register` since `--addr` is often a bind-only wildcard (`0.0.0.0:...`).
        #[arg(long)]
        advertise_addr: Option<String>,
        /// Total ordinal shards in the index (multi-node sharding). With `--register`, this node
        /// registers as serving `--shard-ordinal` of `--shards`, so the Gateway's shard map places
        /// it at that ordinal. Default 1 = a single-shard index (this node serves it all).
        #[arg(long, default_value_t = 1)]
        shards: u32,
        /// This node's shard ordinal in `0..shards`. Pair with `--shards` (>1). The shard
        /// must already be built for this ordinal (`growlerdb index --shards N --shard-ordinal K`).
        #[arg(long, default_value_t = 0)]
        shard_ordinal: u32,
        /// Serve as a read-only **replica**: pull the primary's sealed segments from backup, serve
        /// search/lookup/suggest (no writes or reindex), and periodically re-pull + hot-swap new
        /// segments. The definition comes from the backup manifest. Needs the backup env
        /// (`GROWLERDB_BACKUP_BUCKET`, `GROWLERDB_S3_*`); single-shard indexes only.
        #[arg(long)]
        replica: bool,
        /// Backup prefix to replicate from (default `backups/<index>`). Only with `--replica`.
        #[arg(long, requires = "replica")]
        replica_prefix: Option<String>,
        /// Seconds between replica refresh polls (default 30).
        #[arg(long, default_value_t = 30)]
        replica_refresh_secs: u64,
        /// Seconds between **auto-compaction** health checks: when the shard is fragmented
        /// (≥8 segments) or carries delete debt (≥20%), segments are fused / deletes purged. `0`
        /// disables. Ignored for `--replica` (a replica must not compact). Default 60.
        #[arg(long, default_value_t = 60)]
        compact_interval_secs: u64,
        /// Seconds between **compaction re-map** polls: the node watches
        /// the source table's live data-file set; when Iceberg compaction rewrites files away, it
        /// marks them dead and re-points the affected locators at the new files in the background
        /// — so hydration doesn't pay a per-read refresh after every source compaction. `0`
        /// disables (the lazy verify-and-refresh path still heals). Ignored for `--replica` (it
        /// pulls the primary's already-healed locators). Default 45.
        #[arg(long, default_value_t = 45)]
        remap_interval_secs: u64,
        #[command(flatten)]
        tls: ServerTlsArgs,
    },
    /// Run a standalone Gateway: terminate the Engine API (gRPC + REST) and route to one or
    /// more remote Nodes over gRPC. The distributed counterpart to `serve`'s embedded gateway.
    ///
    /// Front a single Node with `--node-addr`; a **sharded** cluster from a registry **file** with
    /// `--registry` + `--index`; or a sharded cluster from the **live Control-Plane** with
    /// `--control-plane` + `--index` (no shared filesystem — what a Kubernetes deploy needs). In the
    /// sharded modes the Gateway fronts each shard's primary in ordinal order and hot-reloads on
    /// topology change.
    Gateway {
        /// A single Node's gRPC endpoint to front (e.g. `http://127.0.0.1:50051`). Mutually
        /// exclusive with `--registry`/`--index`.
        #[arg(long, conflicts_with_all = ["registry", "index"])]
        node_addr: Option<String>,
        /// Path to the Control-Plane `registry.json`; with `--index`, front that index's shards.
        #[arg(long, requires = "index")]
        registry: Option<String>,
        /// The index **or alias** to front. Pair with `--registry` (a registry.json file) **or**
        /// `--control-plane` (the live registry over gRPC). An alias (file mode) fronts the
        /// union of its members' shards. Each shard's `NodeId` is its gRPC endpoint.
        #[arg(long)]
        index: Option<String>,
        /// Front **every** registered index over one endpoint: each
        /// request routes to its named index's shard-set, resolved lazily from `--control-plane` on
        /// first use and hot-reloaded independently. Mutually exclusive with `--index`; requires
        /// `--control-plane`. Readiness flips when the control plane is reachable, not when an index
        /// resolves. Per-index RBAC still applies (a token scoped to index A can't read index B).
        #[arg(long, conflicts_with = "index", requires = "control_plane")]
        all_indexes: bool,
        /// Address to serve the Engine API over gRPC (`host:port`).
        #[arg(long, default_value = "127.0.0.1:50061")]
        addr: String,
        /// Address to serve the REST/JSON Engine API (`/v1/...`) on (`host:port`).
        #[arg(long, default_value = "127.0.0.1:8080")]
        rest_addr: String,
        /// Enable OIDC/JWT authentication: validate `Authorization: Bearer` tokens against
        /// this issuer's JWKS (e.g. `https://keycloak.example/realms/growlerdb`). Omit to
        /// leave the gateway open (no authentication).
        #[arg(long)]
        oidc_issuer: Option<String>,
        /// Expected `aud` claim for OIDC tokens (required with `--oidc-issuer`).
        #[arg(long, requires = "oidc_issuer")]
        oidc_audience: Option<String>,
        /// Built-in (no external IdP) password auth: validate the session JWTs that the
        /// control-plane's `/v1/login` mints, using a shared secret. Closed mode without OIDC.
        /// Mutually exclusive with `--oidc-issuer`; requires `--auth-secret`.
        #[arg(long, conflicts_with = "oidc_issuer", requires = "auth_secret")]
        builtin_auth: bool,
        /// Shared HMAC secret for built-in session JWTs — must match the control-plane's. Env:
        /// `GROWLERDB_AUTH_SECRET`.
        #[arg(long, env = "GROWLERDB_AUTH_SECRET")]
        auth_secret: Option<String>,
        /// Control-Plane gRPC endpoint (e.g. `http://controlplane:50071`). When set, the REST
        /// front exposes index management (`/v1/indexes`, `/v1/source:describe`) by
        /// proxying to it. With `--index` (and no `--registry`) it also drives **shard routing**:
        /// the Gateway reads the index's shard map from the live control-plane over gRPC and
        /// hot-reloads on change — the distributed (Kubernetes) deploy path.
        #[arg(long)]
        control_plane: Option<String>,
        /// Prometheus-compatible metrics URL (e.g. `http://lgtm:9090`). When set, the REST front
        /// proxies `/v1/stats/...` to it so the UI's SLI panels query same-origin.
        #[arg(long)]
        prometheus: Option<String>,
        /// Expose the optional OpenSearch-compatible `_search` adapter: a documented
        /// DSL subset translated to native queries, results as documents (`_id` from the key,
        /// `_source` via hydration). Off by default; the native PK API is primary.
        #[arg(long)]
        opensearch: bool,
        /// Poll the registry (file or control-plane) every N seconds and **hot-reload** the topology
        /// when it changes — after a reshard cutover the gateway picks up the new shard set
        /// + bucket map with no restart. Ordinal indexes only (not windowed). `0` disables.
        #[arg(long, default_value_t = 15)]
        reload_secs: u64,
        #[command(flatten)]
        node_tls: UpstreamTlsArgs,
    },
    /// Run the Control Plane: the cluster-wide index registry (create / drop / list) over
    /// gRPC, persisted under `{data_dir}/registry.json`.
    ControlPlane {
        /// Address to bind the Control-Plane gRPC service (`host:port`).
        #[arg(long, default_value = "127.0.0.1:50071")]
        addr: String,
        /// OIDC issuer URL. When set, the control plane validates bearers itself and enforces RBAC
        /// (admin-gated user management); without it the control plane is open.
        #[arg(long)]
        oidc_issuer: Option<String>,
        /// Expected JWT audience (required with `--oidc-issuer`).
        #[arg(long, requires = "oidc_issuer")]
        oidc_audience: Option<String>,
        /// Built-in (no external IdP) password auth: enable the `/v1/login` RPC (mints
        /// session JWTs from the registry credential store) and validate them. Mutually exclusive
        /// with `--oidc-issuer`; requires `--auth-secret` (shared with the gateway).
        #[arg(long, conflicts_with = "oidc_issuer", requires = "auth_secret")]
        builtin_auth: bool,
        /// Login-only mode (the `just stack` demo): enable the `/v1/login` RPC (mint session
        /// JWTs) and seed the demo/admin users, but leave the control plane's OWN authorization **open**
        /// — so the enforcement point is the gateway (`--builtin-auth`) on the public data plane, while
        /// the internal node/gateway control-plane RPCs (registration, shard-map reads) stay reachable
        /// without a service credential. Unlike `--builtin-auth`, this does NOT gate the control plane;
        /// it only turns on token minting. Requires `--auth-secret`; mutually exclusive with
        /// `--builtin-auth` / `--oidc-issuer`.
        #[arg(long, conflicts_with_all = ["oidc_issuer", "builtin_auth"], requires = "auth_secret")]
        login_secret: bool,
        /// Shared HMAC secret for built-in session JWTs — must match the gateway's. Env:
        /// `GROWLERDB_AUTH_SECRET`.
        #[arg(long, env = "GROWLERDB_AUTH_SECRET")]
        auth_secret: Option<String>,
        /// Initial admin username seeded on first built-in-auth boot (only if no credential exists).
        #[arg(long, default_value = "admin")]
        admin_user: String,
        /// Initial admin password to seed. If omitted, a random one is generated and printed once.
        /// Env: `GROWLERDB_ADMIN_PASSWORD`.
        #[arg(long, env = "GROWLERDB_ADMIN_PASSWORD")]
        admin_password: Option<String>,
        /// Shared service token gating the internal control-plane RPCs (registration, shard-map
        /// reads, placement). When set, every RPC must carry a matching token; unset ⇒ the control
        /// plane is open (bare local dev). Separate from user auth (`--login-secret` / RBAC) and
        /// enforced regardless of it. Node/gateway must present the same token. Env:
        /// `GROWLERDB_SERVICE_TOKEN`.
        #[arg(long, env = "GROWLERDB_SERVICE_TOKEN")]
        service_token: Option<String>,
        #[command(flatten)]
        tls: ServerTlsArgs,
    },
}

/// Cluster reconcile backstop: fetch the index's shard map + bucket owners from the
/// control plane, then fan a **shard-scoped** `ReconcileIndex` out to each shard's primary node —
/// each node compares only the keys it owns (via the same bucket map the gateway/connector route by),
/// so a reconcile can't pull another shard's keys into it. Prints per-shard drift + a total. Any
/// unreachable shard, missing primary, or shard-level error makes the whole run exit non-zero, so a
/// scheduled CronJob surfaces the failure instead of silently skipping a shard.
async fn reconcile_cluster(control_plane: &str, index: &str, full: bool) -> anyhow::Result<()> {
    use growlerdb_proto::v1::admin_client::AdminClient;
    use growlerdb_proto::v1::{GetIndexRequest, ReconcileIndexRequest, ReconcileIndexResponse};

    let mut cp = connect_cp(control_plane, false).await?;
    let idx = cp
        .get_index(GetIndexRequest {
            name: index.to_string(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("GetIndex(`{index}`): {e}"))?
        .into_inner();

    let owners = idx.bucket_owners;
    // Reconcile is bucket-ownership scoped, so it applies to ordinal shards (window == 0); a windowed
    // index shards by time, not ordinal, and isn't reconciled this way.
    let mut shards: Vec<_> = idx
        .shard_status
        .into_iter()
        .filter(|s| s.window == 0)
        .collect();
    shards.sort_by_key(|s| s.ordinal);
    if shards.is_empty() {
        anyhow::bail!("index `{index}` has no ordinal shards to reconcile");
    }

    // One shard-scoped ReconcileIndex call (or a counts-only probe when `count_only`).
    let call = |primary: String, ordinal: u32, owners: Vec<u32>, count_only: bool| async move {
        let mut client = AdminClient::connect(primary).await?;
        let resp = client
            .reconcile_index(ReconcileIndexRequest {
                index: index.to_string(),
                bucket_owners: owners,
                shard_ordinal: ordinal,
                full,
                count_only,
            })
            .await?
            .into_inner();
        Ok::<ReconcileIndexResponse, anyhow::Error>(resp)
    };

    // Whole-index count-gate: a cheap counts-only probe first. If Σ index docs across all
    // shards already equals the source table's total record count, the index is in sync — skip the
    // expensive row-level reconcile entirely. Routing-agnostic (covers hash-routed indexes the
    // per-partition gate can't). Any unreachable shard / missing primary / zero source total falls
    // through to a real reconcile (which surfaces the error). Skipped when `--full` forces a sweep.
    if !full && shards.iter().all(|s| !s.primary.is_empty()) {
        let mut index_total = 0u64;
        let mut source_total = 0u64;
        let mut ok = true;
        for s in &shards {
            match call(s.primary.clone(), s.ordinal, owners.clone(), true).await {
                Ok(r) => {
                    index_total += r.index_count;
                    source_total = source_total.max(r.source_count); // table-wide; same per shard
                }
                Err(_) => {
                    ok = false;
                    break;
                }
            }
        }
        if ok && source_total > 0 && index_total == source_total {
            println!(
                "`{index}` is in sync ({index_total} docs == source total) — skipped (use --full to force a sweep)"
            );
            return Ok(());
        }
    }

    let (mut total_stale, mut total_missing) = (0u64, 0u64);
    let (mut scanned, mut skipped) = (0u64, 0u64);
    let mut failures = 0usize;
    for s in &shards {
        if s.primary.is_empty() {
            eprintln!(
                "shard {} has no primary node (state `{}`) — skipping",
                s.ordinal, s.state
            );
            failures += 1;
            continue;
        }
        match call(s.primary.clone(), s.ordinal, owners.clone(), false).await {
            Ok(r) => {
                total_stale += r.stale;
                total_missing += r.missing;
                scanned += r.partitions_scanned;
                skipped += r.partitions_skipped;
                let gate = if r.partitions_scanned + r.partitions_skipped > 0 {
                    format!(
                        " [gate: {} scanned, {} skipped]",
                        r.partitions_scanned, r.partitions_skipped
                    )
                } else {
                    String::new()
                };
                println!(
                    "shard {} ({}): {} stale, {} missing repaired (index {} → source {}){gate}",
                    s.ordinal, s.primary, r.stale, r.missing, r.index_count, r.source_count
                );
            }
            Err(e) => {
                eprintln!("shard {} ({}) reconcile failed: {e}", s.ordinal, s.primary);
                failures += 1;
            }
        }
    }
    let gate = if scanned + skipped > 0 {
        format!(" [partitions: {scanned} scanned, {skipped} skipped by the count-gate]")
    } else {
        String::new()
    };
    println!(
        "reconciled `{index}` across {} shard(s): {total_stale} stale, {total_missing} missing repaired{gate}",
        shards.len()
    );
    if failures > 0 {
        anyhow::bail!("{failures} shard(s) failed to reconcile");
    }
    Ok(())
}

/// Parse the CLI arguments and dispatch the selected command. The binary's `main` is a thin
/// wrapper over this; exposing it (and [`gateway`]) lets an out-of-tree build reuse the CLI.
pub async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();
    // Structured JSON logging + the Prometheus metrics recorder.
    growlerdb_telemetry::init("growlerdb");
    // Startup splash to stderr (clap handles --help/--version before this, so it
    // only shows for real commands and never pollutes piped stdout).
    eprintln!("{}", growlerdb_core::startup_banner());

    // Embedded engine over the local store + the local-dev Iceberg/Polaris stack.
    let engine = Engine::open(&cli.data_dir, IcebergConfig::from_env())?;
    let metrics_addr = cli.metrics_addr.clone();
    let ui_dir = cli.ui_dir.clone();

    match cli.command {
        Command::Index {
            table,
            def,
            name,
            shards,
            shard_ordinal,
            define_only,
        } => {
            let def_yaml = def.map(std::fs::read_to_string).transpose()?;
            if define_only {
                // Definition only, no build — how a windowed node starts truly empty.
                let outcome = engine
                    .define_index(&table, def_yaml.as_deref(), name.as_deref())
                    .await?;
                println!(
                    "defined `{}`: index.json written, no shards built",
                    outcome.name
                );
            } else {
                let outcome = engine
                    .index_shard(
                        &table,
                        def_yaml.as_deref(),
                        name.as_deref(),
                        shards,
                        shard_ordinal,
                    )
                    .await?;
                let scope = if shards > 1 {
                    format!(" (shard {shard_ordinal}/{shards})")
                } else {
                    String::new()
                };
                println!(
                    "indexed `{}`{}: {} documents at snapshot {}",
                    outcome.name, scope, outcome.doc_count, outcome.snapshot.0
                );
            }
        }
        Command::Search {
            index,
            query,
            limit,
            hydrate,
            fields,
        } => {
            let projection = if fields.is_empty() {
                Projection::All
            } else {
                Projection::Columns(fields)
            };
            let outcome = engine
                .search(&index, &query, limit, hydrate, projection)
                .await?;
            print_results(&outcome.hits, outcome.rows.as_deref());
        }
        Command::Sync { index } => {
            let out = engine.sync(&index).await?;
            println!(
                "synced `{index}`: +{} doc(s) at snapshot {} (checkpoint {})",
                out.added, out.snapshot.0, out.checkpoint
            );
        }
        Command::Reconcile {
            index,
            control_plane,
            full,
        } => {
            if let Some(cp) = control_plane {
                reconcile_cluster(&cp, &index, full).await?;
            } else {
                let r = engine.reconcile(&index).await?;
                if r.is_clean() {
                    println!(
                        "`{index}` is consistent with the source ({} doc(s), no drift)",
                        r.source_count
                    );
                } else {
                    println!(
                        "reconciled `{index}`: deleted {} stale, re-indexed {} missing \
                         (index {} → source {})",
                        r.deleted, r.reindexed, r.index_count, r.source_count
                    );
                }
            }
        }
        Command::Rebuild { index } => {
            let out = engine.rebuild(&index).await?;
            println!(
                "rebuilt `{}`: {} documents at snapshot {}",
                out.name, out.doc_count, out.snapshot.0
            );
        }
        Command::Backup { index, prefix } => {
            backup_cmd(&cli.data_dir, &index, prefix.as_deref()).await?;
        }
        Command::Restore { index, prefix } => {
            restore_cmd(&engine, &cli.data_dir, &index, prefix.as_deref()).await?;
        }
        Command::RefreshReplica { index, prefix } => {
            refresh_replica_cmd(&cli.data_dir, &index, prefix.as_deref()).await?;
        }
        Command::Park { index, keep_hot } => {
            park_cmd(&cli.data_dir, &index, keep_hot).await?;
        }
        Command::Revive { index, window } => {
            revive_cmd(&cli.data_dir, &index, window).await?;
        }
        Command::Retention {
            pattern,
            keep,
            control_plane,
            dry_run,
        } => {
            retention_cmd(&control_plane, &pattern, keep, dry_run).await?;
        }
        Command::Serve {
            index,
            addr,
            max_inflight,
            rest_addr,
            register,
            advertise_addr,
            shards,
            shard_ordinal,
            replica,
            replica_prefix,
            replica_refresh_secs,
            compact_interval_secs,
            remap_interval_secs,
            tls,
        } => {
            if replica {
                serve_replica(
                    &cli.data_dir,
                    &index,
                    &addr,
                    rest_addr.as_deref(),
                    tls.load()?,
                    metrics_addr.as_deref(),
                    ui_dir.as_deref(),
                    replica_prefix.as_deref(),
                    replica_refresh_secs,
                )
                .await?;
            } else {
                serve(ServeConfig {
                    data_dir: &cli.data_dir,
                    index: &index,
                    addr: &addr,
                    max_inflight,
                    rest_addr: rest_addr.as_deref(),
                    tls: tls.load()?,
                    metrics_addr: metrics_addr.as_deref(),
                    ui_dir: ui_dir.as_deref(),
                    register: register.as_deref(),
                    advertise_addr: advertise_addr.as_deref(),
                    shards,
                    shard_ordinal,
                    compact_interval_secs,
                    remap_interval_secs,
                })
                .await?;
            }
        }
        Command::Gateway {
            node_addr,
            registry,
            index,
            all_indexes,
            addr,
            rest_addr,
            oidc_issuer,
            oidc_audience,
            builtin_auth,
            auth_secret,
            control_plane,
            prometheus,
            opensearch,
            reload_secs,
            node_tls,
        } => {
            gateway(GatewayConfig {
                node_addr: node_addr.as_deref(),
                registry: registry.as_deref(),
                index: index.as_deref(),
                all_indexes,
                addr: &addr,
                rest_addr: &rest_addr,
                oidc_issuer: oidc_issuer.as_deref(),
                oidc_audience: oidc_audience.as_deref(),
                builtin_auth,
                auth_secret: auth_secret.as_deref(),
                node_tls: node_tls.load()?,
                metrics_addr: metrics_addr.as_deref(),
                ui_dir: ui_dir.as_deref(),
                control_plane: control_plane.as_deref(),
                prometheus: prometheus.as_deref(),
                opensearch,
                reload_secs,
                authn: None,
            })
            .await?;
        }
        Command::ControlPlane {
            addr,
            oidc_issuer,
            oidc_audience,
            builtin_auth,
            login_secret,
            auth_secret,
            admin_user,
            admin_password,
            service_token,
            tls,
        } => {
            control_plane(
                &cli.data_dir,
                &addr,
                metrics_addr.as_deref(),
                oidc_issuer,
                oidc_audience,
                builtin_auth,
                login_secret,
                auth_secret,
                admin_user,
                admin_password,
                service_token,
                tls.load()?,
            )
            .await?;
        }
    }
    // Flush any buffered OTLP spans before exit (no-op when export is off).
    growlerdb_telemetry::shutdown();
    Ok(())
}

/// Spawn the health-driven **auto-compaction** loop for one shard `handle`: on a timer it
/// fuses segments / purges deletes when the live shard crosses the [`CompactionPolicy`] thresholds,
/// so segments don't accumulate unbounded under steady ingest. The merge is blocking I/O → the
/// blocking pool, non-disruptive to in-flight readers / open PITs, and always runs on the *current*
/// shard (a reindex swap is respected). `interval_secs == 0` disables it (spawns nothing). `label`
/// tags the log lines (the index name, or `index w<window>` for a windowed shard). Never called for
/// a replica or a cold read-through shard — neither has a writer to compact.
fn spawn_auto_compaction(handle: growlerdb_engine::ShardHandle, label: String, interval_secs: u64) {
    if interval_secs == 0 {
        return;
    }
    let policy = growlerdb_index::CompactionPolicy::default();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            let shard = handle.current();
            // If the window was parked underneath this handle (hot→cold demote by `spawn_park`),
            // the served shard is now a read-only cold read-through shard with no writer — there is
            // nothing to compact. Stop watching; a later `revive`/pre-warm re-spawns compaction.
            if shard.is_read_only() {
                break;
            }
            let health = match shard.compaction_health() {
                Ok(health) => health,
                Err(e) => {
                    eprintln!("compact `{label}`: health read failed ({e})");
                    growlerdb_telemetry::sli::background_failure("compaction");
                    continue;
                }
            };
            // Track live segments every tick so the segments panel shows growth between
            // merges, not just at compaction time — and the delete debt, so a size
            // sample taken between merges can be read in context (superseded docs still on disk).
            growlerdb_telemetry::sli::segments_live(&label, health.segments);
            growlerdb_telemetry::sli::index_deleted_docs(&label, health.deleted);
            // Live doc count: the index side of the source→index convergence check —
            // sum(growlerdb_index_docs) vs sum(growlerdb_source_records) must meet at steady state.
            if let Ok(docs) = shard.num_docs() {
                growlerdb_telemetry::sli::index_docs(&label, docs);
            }
            // One walk serves both gauges: the total is the breakdown's sum, so
            // `growlerdb_index_bytes` == sum over `growlerdb_index_bytes_component` by construction.
            let bd = shard.index_size_breakdown();
            growlerdb_telemetry::sli::index_bytes(&label, bd.total());
            growlerdb_telemetry::sli::index_bytes_component(&label, "term", bd.term);
            growlerdb_telemetry::sli::index_bytes_component(&label, "postings", bd.postings);
            growlerdb_telemetry::sli::index_bytes_component(&label, "positions", bd.positions);
            growlerdb_telemetry::sli::index_bytes_component(&label, "fieldnorms", bd.fieldnorms);
            growlerdb_telemetry::sli::index_bytes_component(&label, "fast", bd.fast);
            growlerdb_telemetry::sli::index_bytes_component(&label, "store", bd.store);
            growlerdb_telemetry::sli::index_bytes_component(&label, "locator", bd.locator);
            growlerdb_telemetry::sli::index_bytes_component(&label, "other", bd.other);
            growlerdb_telemetry::sli::background_success("compaction");
            if let Some(reason) = policy.reason_to_compact(&health) {
                eprintln!("compact `{label}`: {reason} — merging");
                let before = health.segments;
                let compact_shard = handle.current();
                match tokio::task::spawn_blocking(move || compact_shard.compact(&policy)).await {
                    Ok(Ok(())) => {
                        eprintln!("compact `{label}`: done");
                        // Count the merge + record the post-merge segment count.
                        if let Ok(after) = handle.current().compaction_health() {
                            growlerdb_telemetry::sli::compaction(&label, before, after.segments);
                        }
                    }
                    Ok(Err(e)) => {
                        eprintln!("compact `{label}`: failed ({e})");
                        growlerdb_telemetry::sli::background_failure("compaction");
                    }
                    Err(e) => {
                        eprintln!("compact `{label}`: task panicked ({e})");
                        growlerdb_telemetry::sli::background_failure("compaction");
                    }
                }
            }
        }
    });
}

/// Spawn the background **compaction re-map** loop (`coordinates` location
/// strategy) for the index's hot shard(s): each tick it polls the source table's current plan
/// (one catalog call; manifest reads only when the snapshot advanced — the reader's
/// snapshot-pinned plan cache) and diffs the live data-file set against the shards' interned
/// files. When Iceberg compaction rewrote files away, the disappeared files are marked **dead**
/// (hydration then skips their doomed point reads — the live-file bitmap) and the affected
/// location slots are re-pointed at the rewritten rows' new `(file, position)` in the
/// background — so locator staleness is a bounded background cost, not a per-hydration refresh
/// tax. It never blocks hydration or ingest (the shard takes the writer lock only per patch
/// chunk); interleaving safety is argued in [`growlerdb_engine::remap`]. `interval_secs == 0`
/// disables (spawns nothing; the lazy verify-and-refresh path still heals). A windowed index
/// passes every hot window's handle: one poll + one key scan serves them all (each window skips
/// the keys it doesn't hold). Never called for a replica (it pulls the primary's healed
/// locators) — and a cold window's shard has no writer but is single-writer by construction, so
/// hot handles only. A **`PREDICATE`** index spawns nothing: it stores no
/// location data, so source compaction leaves nothing to re-map or mark dead.
fn spawn_locator_remap(
    handles: Vec<growlerdb_engine::ShardHandle>,
    label: String,
    table: String,
    partition_fields: Vec<String>,
    identifier_fields: Vec<String>,
    interval_secs: u64,
) {
    if interval_secs == 0 || handles.is_empty() {
        return;
    }
    // Every shard of an index shares one location strategy (it's an index-definition
    // option), so the first handle speaks for all.
    if handles[0].current().location_strategy() == growlerdb_core::LocationStrategy::Predicate {
        println!(
            "remap `{label}`: not needed — PREDICATE location strategy (store-less; \
             hydration re-finds rows by pruned key scan)"
        );
        return;
    }
    tokio::spawn(async move {
        // Own shared reader: lazily connected, invalidated on failure so the next tick
        // reconnects (and its plan cache makes the steady-state poll one REST call).
        let reader = growlerdb_source::SharedReader::new(IcebergConfig::from_env());
        let mut state = growlerdb_engine::RemapState::default();
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            // Pin the current shard(s) per tick (a reindex swap is respected next tick). Skip any
            // window parked underneath its handle (hot→cold demote by `spawn_park`): a read-only
            // cold shard is served read-through from object storage and has no locators to patch.
            let shards: Vec<std::sync::Arc<growlerdb_index::Shard>> = handles
                .iter()
                .map(|h| h.current())
                .filter(|s| !s.is_read_only())
                .collect();
            let connected = match reader.get().await {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("remap `{label}`: catalog connect failed ({e})");
                    growlerdb_telemetry::sli::background_failure("locator-remap");
                    continue;
                }
            };
            match growlerdb_engine::remap_tick(
                &connected,
                &table,
                (&partition_fields, &identifier_fields),
                &shards,
                &mut state,
            )
            .await
            {
                Ok(Some(o)) => {
                    eprintln!(
                        "remap `{label}`: snapshot {} rewrote {} interned file(s) — {} row(s) \
                         re-mapped from {} added file(s) ({} skipped no-live-doc, {} already \
                         re-pointed, {} delete-bearing file(s) left to lazy refresh)",
                        o.snapshot_id,
                        o.files_marked_dead,
                        o.stats.remapped,
                        o.files_scanned,
                        o.stats.skipped_no_live_doc,
                        o.stats.skipped_already_live,
                        o.files_skipped_deletes,
                    );
                    growlerdb_telemetry::sli::locator_remap(&label, o.stats.remapped);
                    emit_dead_files(&label, &shards);
                    growlerdb_telemetry::sli::background_success("locator-remap");
                }
                Ok(None) => {
                    emit_dead_files(&label, &shards);
                    growlerdb_telemetry::sli::background_success("locator-remap");
                }
                Err(e) => {
                    eprintln!("remap `{label}`: poll failed ({e})");
                    // Drop the possibly-dead catalog client; the next tick reconnects.
                    reader.invalidate().await;
                    growlerdb_telemetry::sli::background_failure("locator-remap");
                }
            }
        }
    });

    fn emit_dead_files(label: &str, shards: &[std::sync::Arc<growlerdb_index::Shard>]) {
        let dead: u64 = shards.iter().map(|s| s.dead_file_count()).sum();
        growlerdb_telemetry::sli::locator_dead_files(label, dead);
    }
}

/// Background **pre-warm** loop for one cold window. Samples the window's **search** counter
/// each interval; when its per-interval search count crosses [`PreWarmPolicy`], the
/// window is promoted back to hot — its index is materialized locally (un-bundled from object storage)
/// and hot-swapped
/// into the live handle, after which it serves from local NVMe with no cold latency and the loop ends
/// (handing the now-hot window to auto-compaction). A no-op if the policy is disabled.
///
/// [`PreWarmPolicy`]: growlerdb_index::PreWarmPolicy
#[allow(clippy::too_many_arguments)]
fn spawn_prewarm(
    handle: growlerdb_engine::ShardHandle,
    store: growlerdb_index::LocalIndexStore,
    object_store: growlerdb_backup::Operator,
    resolved: growlerdb_core::ResolvedIndex,
    index: String,
    window: i64,
    compact_interval_secs: u64,
) {
    let policy = growlerdb_index::PreWarmPolicy::default();
    if policy.min_accesses == 0 {
        return;
    }
    // Sampling cadence for the read-rate signal; the promote itself is rare.
    const SAMPLE_SECS: u64 = 30;
    tokio::spawn(async move {
        use growlerdb_index::ShardId;
        let label = format!("{index} w{window}");
        let mut last = handle.search_count();
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(SAMPLE_SECS));
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            let now = handle.search_count();
            let delta = now.saturating_sub(last);
            last = now;
            if !policy.should_promote(delta) {
                continue;
            }
            let marker = match store.cold_marker(&index, window) {
                Ok(Some(m)) => m,
                // Genuinely no longer cold (already promoted) → stop watching.
                Ok(None) => break,
                // A transient marker-read error must NOT end the watcher forever:
                // log, count it, and retry next interval.
                Err(e) => {
                    eprintln!("pre-warm `{label}`: marker read failed ({e}) — retrying");
                    growlerdb_telemetry::sli::background_failure("pre-warm");
                    continue;
                }
            };
            let window_dir = store.shard_path(&ShardId::window(&index, window));
            eprintln!(
                "pre-warm `{label}`: {delta} searches/interval ≥ {} — promoting to hot",
                policy.min_accesses
            );
            if let Err(e) =
                growlerdb_backup::promote_cold(&object_store, &marker, &window_dir).await
            {
                eprintln!("pre-warm `{label}`: promote failed ({e}) — staying cold");
                growlerdb_telemetry::sli::background_failure("pre-warm");
                continue;
            }
            // Reuse the cold shard's already-open `aux.redb` handle: it is still live in `handle`
            // until the swap below, and redb allows only one open per file, so the arriving hot shard
            // must share it rather than race a second `Database::open`.
            let reuse_db = handle.current().db_handle();
            let (store2, resolved2, index2) = (store.clone(), resolved.clone(), index.clone());
            let opened = tokio::task::spawn_blocking(move || {
                store2.open_shard_reusing_db(
                    &ShardId::window(&index2, window),
                    &resolved2,
                    reuse_db,
                )
            })
            .await;
            match opened {
                Ok(Ok(shard)) => {
                    handle.swap(std::sync::Arc::new(shard));
                    eprintln!("pre-warm `{label}`: promoted — now serving from local NVMe");
                    growlerdb_telemetry::sli::background_success("pre-warm");
                    // Now hot → hand it to auto-compaction, and stop pre-warming this window.
                    spawn_auto_compaction(handle, label, compact_interval_secs);
                    break;
                }
                Ok(Err(e)) => {
                    eprintln!("pre-warm `{label}`: open-hot failed ({e})");
                    growlerdb_telemetry::sli::background_failure("pre-warm");
                }
                Err(e) => {
                    eprintln!("pre-warm `{label}`: open task panicked ({e})");
                    growlerdb_telemetry::sli::background_failure("pre-warm");
                }
            }
        }
    });
}

/// Background **park** loop for a windowed node — the hot→cold counterpart of [`spawn_prewarm`].
/// Each interval it reads the node's *live* window set (boot windows plus any created at runtime by
/// ingest), applies the index's `hot_windows` policy via [`cold_windows`], and demotes each aged
/// hot window to cold read-through: back it up through the live serving handle (no second writer),
/// swap the handle to a read-through shard, evict the local bulk, and start a pre-warm watcher so a
/// window that gets hot again auto-revives. Idempotent — an already-cold window is skipped. A no-op
/// when `interval_secs == 0`. Same discipline as the other background loops: a transient failure is
/// logged + counted and retried next interval; the loop never dies.
///
/// [`cold_windows`]: growlerdb_core::TimeWindowing::cold_windows
#[allow(clippy::too_many_arguments)]
fn spawn_park(
    write: growlerdb_engine::WindowedWriteService,
    store: growlerdb_index::LocalIndexStore,
    object_store: growlerdb_backup::Operator,
    cache: growlerdb_index::RangeCache,
    resolved: growlerdb_core::ResolvedIndex,
    windowing: growlerdb_core::TimeWindowing,
    index: String,
    compact_interval_secs: u64,
    interval_secs: u64,
) {
    if interval_secs == 0 {
        return;
    }
    // Serialize the definition once for the marker/backups; a failure here is fatal to parking (but
    // not to serving), so log and disable rather than retry a deterministic error every tick.
    let def_json = match serde_json::to_string(&resolved) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("park `{index}`: cannot serialize index definition ({e}) — park disabled");
            return;
        }
    };
    tokio::spawn(async move {
        use growlerdb_index::ShardId;
        use std::sync::Arc;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            // The live window set (ascending, oldest first) and each window's swappable handle.
            let live = write.window_handles();
            let ids: Vec<i64> = live.iter().map(|(w, _)| *w).collect();
            // Aged windows outside the `hot_windows` policy — exactly the manual `park` victims.
            let victims: Vec<i64> = windowing.cold_windows(&ids, None).to_vec();
            for w in victims {
                let Some(handle) = live.iter().find(|(x, _)| *x == w).map(|(_, h)| h.clone())
                else {
                    continue;
                };
                // Already parked (read-through, no writer) → nothing to do.
                if handle.current().is_read_only() {
                    continue;
                }
                let label = format!("{index} w{w}");
                let window_dir = store.shard_path(&ShardId::window(&index, w));
                // Staging sits beside the window dir (same filesystem → segment files hard-link).
                let staging = window_dir.with_file_name(format!(".cold-staging-{index}-w{w}"));
                let prefix = format!("cold/{index}/w{w}");
                // Back up + cold-tier THROUGH the live serving shard (borrow — no second writer). The
                // window keeps serving hot until the swap below.
                let hot = handle.current();
                let marker = match growlerdb_backup::cold_park_in_place(
                    &hot,
                    &index,
                    w,
                    &window_dir,
                    &staging,
                    &object_store,
                    &prefix,
                    Some(def_json.clone()),
                )
                .await
                {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("park `{label}`: cold-park failed ({e}) — staying hot");
                        growlerdb_telemetry::sli::background_failure("park");
                        continue;
                    }
                };
                // Keep the window's `aux.redb` handle to hand to the cold shard: redb allows one open
                // per file, and the handle still holds this hot shard until the swap below, so the
                // cold shard must SHARE the open handle, not race a second `Database::open`.
                let reuse_db = hot.db_handle();
                drop(hot);
                // Open the read-through cold shard (object-storage reads → blocking) and hot-swap it
                // in, so queries never see a gap; then evict the now-redundant local bulk.
                let object_prefix = marker.object_prefix.clone();
                let hotcache_key = marker.hotcache_key.clone();
                let bundle_key = marker.bundle_key.clone();
                let bundle_manifest_key = marker.bundle_manifest_key.clone();
                let (store2, resolved2, wdir2, op2, cache2) = (
                    store.clone(),
                    resolved.clone(),
                    window_dir.clone(),
                    object_store.clone(),
                    cache.clone(),
                );
                let opened = tokio::task::spawn_blocking(move || {
                    let bundle = bundle_key.as_deref().zip(bundle_manifest_key.as_deref());
                    store2.open_cold_shard(
                        &resolved2,
                        &wdir2,
                        op2,
                        &object_prefix,
                        cache2,
                        hotcache_key.as_deref(),
                        bundle,
                        Some(reuse_db),
                    )
                })
                .await;
                match opened {
                    Ok(Ok(shard)) => {
                        handle.swap(Arc::new(shard));
                        // Marker durable + read-through shard live → drop the local bulk. `aux.redb`
                        // stays as the cold footprint.
                        if let Err(e) = growlerdb_backup::evict_local_index(&window_dir) {
                            eprintln!("park `{label}`: local bulk evict failed ({e}) — parked, cleanup deferred");
                            growlerdb_telemetry::sli::background_failure("park");
                        }
                        eprintln!(
                            "park `{label}`: cold-parked (snapshot {}) — now serving read-through",
                            marker.snapshot
                        );
                        growlerdb_telemetry::sli::background_success("park");
                        // Let a re-heated window promote itself back to hot.
                        spawn_prewarm(
                            handle,
                            store.clone(),
                            object_store.clone(),
                            resolved.clone(),
                            index.clone(),
                            w,
                            compact_interval_secs,
                        );
                    }
                    Ok(Err(e)) => {
                        eprintln!("park `{label}`: open-cold failed after backup ({e}) — window still hot locally");
                        growlerdb_telemetry::sli::background_failure("park");
                    }
                    Err(e) => {
                        eprintln!("park `{label}`: open-cold task panicked ({e})");
                        growlerdb_telemetry::sli::background_failure("park");
                    }
                }
            }
        }
    });
}

/// Everything [`serve`] (and the windowed variant [`serve_windowed`]) needs to host a Node — bundled
/// into one struct instead of many positional args. Borrows the string config from the
/// dispatched `Command`; `tls` is owned (moved in). The windowed path ignores `max_inflight` /
/// `shards` / `shard_ordinal` (a windowed index shards by time window, not ordinal).
struct ServeConfig<'a> {
    data_dir: &'a str,
    index: &'a str,
    addr: &'a str,
    max_inflight: usize,
    rest_addr: Option<&'a str>,
    tls: Option<tonic::transport::ServerTlsConfig>,
    metrics_addr: Option<&'a str>,
    ui_dir: Option<&'a str>,
    register: Option<&'a str>,
    advertise_addr: Option<&'a str>,
    shards: u32,
    shard_ordinal: u32,
    compact_interval_secs: u64,
    remap_interval_secs: u64,
}

/// Host the gRPC services for the index over its address (and, if `rest_addr` is set, the
/// REST/JSON gateway over that address) until ^C.
async fn serve(cfg: ServeConfig<'_>) -> anyhow::Result<()> {
    use growlerdb_index::{LocalIndexStore, ShardId};
    use growlerdb_proto::{SystemServer, SystemService};
    use std::sync::Arc;
    use tonic::transport::Server;

    // Load the persisted index definition + open (creating if absent) its shard.
    let def_path = std::path::Path::new(cfg.data_dir)
        .join(cfg.index)
        .join("index.json");
    let def_bytes = std::fs::read(&def_path).map_err(|_| {
        anyhow::anyhow!(
            "index `{}` not found — run `growlerdb index` first",
            cfg.index
        )
    })?;
    // Parse the definition; if it's corrupt, fall back to the last-known-good `.prev` copy with a
    // loud warning rather than failing to boot the Node.
    let resolved: growlerdb_core::ResolvedIndex = match serde_json::from_slice(&def_bytes) {
        Ok(r) => r,
        Err(e) => {
            let prev = growlerdb_core::durable::prev_path(&def_path);
            if prev.exists() {
                eprintln!(
                    "warning: `{}` failed to parse ({e}); falling back to `{}`",
                    def_path.display(),
                    prev.display()
                );
                serde_json::from_slice(&std::fs::read(&prev)?)?
            } else {
                return Err(e.into());
            }
        }
    };
    // Surface resolution warnings (e.g. an equality-delete column that forces the
    // costlier partition-reconciliation fallback) so it's a known choice.
    for warning in &resolved.warnings {
        eprintln!("warning: {warning}");
    }
    let store = LocalIndexStore::open(cfg.data_dir)?;
    // A windowed index is served as many per-window shards behind a pruning Gateway, not
    // one single shard — a separate, REST-first path (reusing the same config).
    if resolved.windowing.is_some() {
        return serve_windowed(cfg, store, resolved).await;
    }
    // Non-windowed from here: destructure the config so the body reads with plain names.
    let ServeConfig {
        index,
        addr,
        max_inflight,
        rest_addr,
        tls,
        metrics_addr,
        ui_dir,
        register,
        advertise_addr,
        shards,
        shard_ordinal,
        compact_interval_secs,
        remap_interval_secs,
        data_dir: _,
    } = cfg;
    let shard_id = ShardId::single(index);
    // Complete or clean up any reindex that a prior process was interrupted mid-swap.
    store.recover_reindex(&shard_id)?;
    let shard = Arc::new(store.open_shard(&shard_id, &resolved)?);

    // Lineage guard: if this index recorded its source's Iceberg `table-uuid` at build,
    // verify the live table still carries it. A mismatch means the source was dropped+recreated (or
    // its catalog was reset) and the index is stale — its keys no longer exist in the table, so
    // search would return rows that fail to hydrate ("Row not found"). Rather than refuse to boot,
    // serve **DEGRADED**: search stays available read-only (for inspection) while writes + checkpoint
    // reads are refused (the WriteService below) — so the connector stops advancing the stale index
    // and the control-plane/console surface a distinct `source_recreated` state. A reindex re-anchors
    // the lineage and clears it. Best-effort: a transient catalog/uuid read error only warns, so a
    // catalog blip can't trip it — only a *confirmed* mismatch degrades.
    let mut source_recreated = false;
    if let Some(recorded) = shard.source_uuid()? {
        let table = match &resolved.source {
            growlerdb_core::Source::Iceberg(s) => s.table.clone(),
        };
        match growlerdb_source::IcebergReader::connect(&IcebergConfig::from_env()).await {
            Ok(reader) => match reader.table_uuid(&table).await {
                Ok(live) if live != recorded => {
                    eprintln!(
                        "WARNING: source `{table}` was recreated (table-uuid `{live}` != the index's \
                         `{recorded}`): serving `{index}` DEGRADED — read-only, writes refused. Its keys \
                         will not hydrate; reindex it (`growlerdb rebuild {index}`) to recover."
                    );
                    source_recreated = true;
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("warning: source lineage check skipped (uuid read failed): {e}")
                }
            },
            Err(e) => eprintln!("warning: source lineage check skipped (catalog unreachable): {e}"),
        }
    }

    // One swappable handle shared by every service, so a reindex swap is visible across the
    // whole Node at once.
    let handle = growlerdb_engine::ShardHandle::new(shard);

    // One reindex fence shared by Write (rejects writes while reindexing) and Admin (engages it
    // for the reindex) — so a rebuild can't lose the write delta or regress the checkpoint.
    let reindex_fence = growlerdb_engine::ReindexFence::new();
    let write = growlerdb_engine::WriteService::new(handle.clone(), index, max_inflight)
        .with_fence(reindex_fence.clone())
        .with_source_recreated(source_recreated);
    let search = growlerdb_engine::SearchService::new(handle.clone());
    // GetByKey hydrates coordinates back to rows from the index's Iceberg source.
    let table = match &resolved.source {
        growlerdb_core::Source::Iceberg(s) => s.table.clone(),
    };
    let lookup = growlerdb_engine::LookupService::new(
        handle.clone(),
        IcebergConfig::from_env(),
        table.clone(),
    );
    let suggest = growlerdb_engine::SuggestService::new(handle.clone());
    // Admin can plan alters and reindex: it resolves candidate definitions against the
    // index's Iceberg source, and rebuilds + durably swaps the shard for reindex.
    let mut admin = growlerdb_engine::AdminService::new(handle.clone(), index).with_source(
        resolved.clone(),
        store.clone(),
        shard_id.clone(),
        IcebergConfig::from_env(),
        table.clone(),
        reindex_fence.clone(),
    );
    // Enable console-/REST-triggered backups when an object-storage target is configured.
    if std::env::var("GROWLERDB_BACKUP_BUCKET").is_ok() {
        match backup_s3_config()
            .and_then(|cfg| growlerdb_backup::s3_store(&cfg).map_err(anyhow::Error::from))
        {
            Ok(backup_store) => {
                admin = admin.with_backup(backup_store, format!("backups/{index}"));
                println!("serve: backups enabled → object storage (prefix `backups/{index}`)");
            }
            Err(e) => eprintln!("serve: WARNING backups disabled ({e})"),
        }
    }
    let system = SystemService::new(VERSION);

    // Reap point-in-time handles clients opened but never closed, so a held
    // ReadView can't pin redb's read version (space amplification) indefinitely.
    const PIT_TTL: std::time::Duration = std::time::Duration::from_secs(300);
    const PIT_SWEEP: std::time::Duration = std::time::Duration::from_secs(60);
    let pit_handle = handle.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(PIT_SWEEP);
        loop {
            tick.tick().await;
            // Reap on the currently-live shard (a reindex swap moves PITs with it).
            let evicted = pit_handle.current().expire_pits(PIT_TTL);
            if evicted > 0 {
                eprintln!("pit: expired {evicted} idle point-in-time handle(s)");
            }
        }
    });

    // Health-driven **auto-compaction**, so segments don't accumulate unbounded under
    // steady ingest. (Only this primary path compacts — a `serve --replica` must never compact, or
    // it would diverge from the byte-identical segments it pulls.)
    spawn_auto_compaction(handle.clone(), index.to_string(), compact_interval_secs);

    // Background **compaction re-map**: heal locators in bulk when Iceberg
    // compaction rewrites the source's data files, instead of a per-hydration refresh tax.
    spawn_locator_remap(
        vec![handle.clone()],
        index.to_string(),
        table.clone(),
        resolved.key.partition_fields.clone(),
        resolved.key.identifier_fields.clone(),
        remap_interval_secs,
    );

    // Optionally stand up the Engine API over REST/JSON on a second listener. It routes
    // through the Gateway → an in-process LocalNode over clones of the same services, so
    // embedded mode collapses Gateway + Node into one process with no network hop.
    if let Some(rest_addr) = rest_addr {
        let rest_socket: std::net::SocketAddr = rest_addr
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid --rest-addr `{rest_addr}`: {e}"))?;
        let node = growlerdb_engine::LocalNode::new(
            search.clone(),
            suggest.clone(),
            lookup.clone(),
            admin.clone(),
        );
        let gateway =
            Arc::new(growlerdb_engine::Gateway::new(node.shared()).serving(resolved.name.clone()));
        let router = rest_router(gateway, ui_dir);
        let listener = tokio::net::TcpListener::bind(rest_socket).await?;
        println!("serving REST/JSON gateway on http://{rest_socket}/v1/...");
        tokio::spawn(async move {
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            if let Err(e) = axum::serve(listener, router)
                .with_graceful_shutdown(shutdown)
                .await
            {
                eprintln!("rest gateway error: {e}");
            }
        });
    }

    let socket: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --addr `{addr}`: {e}"))?;
    let mut builder = Server::builder();
    if let Some(tls) = tls {
        // mTLS required: clients must present a cert chaining to the configured CA.
        builder = builder.tls_config(tls)?;
        println!(
            "serving index `{index}` on {socket} over mutual TLS (clients must present a cert)"
        );
    } else {
        eprintln!("serve: WARNING TLS disabled (no --tls-cert); internal traffic is plaintext");
        println!(
            "serving index `{index}` on {socket} \
             (Write + Search + Lookup + Suggest + Admin + System gRPC)"
        );
    }

    // The shard is open and services are built. Readiness is gated below: a node
    // that registers with a control plane reports ready only once it's in the registry.
    let readiness = spawn_health(metrics_addr).await?;

    // Announce this served index to the Control-Plane registry so it's discoverable cluster-wide and
    // routable by the gateway. Retries until the CP is reachable and re-announces on an interval
    // in K8s the node pods routinely come up before the CP, so a one-shot attempt would
    // leave the shard serving but invisible to the gateway forever. `serve` hosts the single shard
    // `ShardId::single(index)`.
    if let (Some(cp), Some(endpoint)) = (register, advertise_addr) {
        // Multi-node sharding: with `--shards N > 1`, register as serving only this
        // node's `--shard-ordinal`; otherwise the single-node default (serve the whole index).
        let ordinals = if shards > 1 {
            vec![shard_ordinal]
        } else {
            vec![]
        };
        let label = if shards > 1 {
            format!("`{index}` at {endpoint} (shard {shard_ordinal}/{shards})")
        } else {
            format!("`{index}` at {endpoint}")
        };
        spawn_registration(
            cp.to_string(),
            endpoint.to_string(),
            resolved.clone(),
            shards.max(1),
            ordinals,
            vec![],
            readiness.clone(),
            label,
        );
    } else {
        // Standalone (no --register): ready as soon as the shard is open and services are built.
        readiness.mark_ready();
    }

    builder
        .add_service(write.into_server())
        .add_service(search.into_server())
        .add_service(lookup.into_server())
        .add_service(suggest.into_server())
        .add_service(admin.into_server())
        .add_service(SystemServer::new(system))
        .serve_with_shutdown(socket, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    println!("growlerdb serve: shut down cleanly");
    Ok(())
}

/// Serve an index as a read-only **replica**: pull the primary's sealed segments from
/// backup, serve the read surface (Search / Lookup / Suggest, plus Admin **describe** — no Write
/// and no reindex, so a replica can't diverge from the primary), and run a background poll that
/// re-pulls and **hot-swaps** new segments whenever the primary's backed-up snapshot advances. The
/// definition is taken from the backup manifest (falling back to a local `index.json`). Single-shard
/// only; a windowed replica is not yet supported.
#[allow(clippy::too_many_arguments)]
async fn serve_replica(
    data_dir: &str,
    index: &str,
    addr: &str,
    rest_addr: Option<&str>,
    tls: Option<tonic::transport::ServerTlsConfig>,
    metrics_addr: Option<&str>,
    ui_dir: Option<&str>,
    prefix: Option<&str>,
    refresh_secs: u64,
) -> anyhow::Result<()> {
    use growlerdb_index::{LocalIndexStore, ShardId};
    use growlerdb_proto::{SystemServer, SystemService};
    use std::sync::Arc;
    use tonic::transport::Server;

    let store = LocalIndexStore::open(data_dir)?;
    let backup = growlerdb_backup::s3_store(&backup_s3_config()?)?;
    let prefix = prefix
        .map(str::to_string)
        .unwrap_or_else(|| format!("backups/{index}"));
    let shard_id = ShardId::single(index);
    let def_path = std::path::Path::new(data_dir)
        .join(index)
        .join("index.json");

    // Initial pull — brings the primary's segments + (usually) the definition into this replica.
    let dest = store.shard_path(&shard_id);
    let stats = growlerdb_backup::refresh(&backup, &prefix, &dest).await?;
    let resolved: growlerdb_core::ResolvedIndex = match &stats.manifest.definition_json {
        Some(def) => {
            growlerdb_core::durable::write(&def_path, def.as_bytes())?;
            serde_json::from_str(def)?
        }
        None => {
            let bytes = std::fs::read(&def_path).map_err(|_| {
                anyhow::anyhow!(
                    "replica backup `{prefix}` carries no definition and no local `{}` exists",
                    def_path.display()
                )
            })?;
            serde_json::from_slice(&bytes)?
        }
    };
    if resolved.windowing.is_some() {
        anyhow::bail!("serving a windowed index as a replica is not yet supported");
    }
    let served_snapshot = stats.manifest.snapshot;
    let shard = Arc::new(store.open_shard(&shard_id, &resolved)?);
    let handle = growlerdb_engine::ShardHandle::new(shard);
    println!(
        "replica `{index}`: pulled snapshot {served_snapshot} ({} new, {} reused) from `{prefix}`",
        stats.downloaded, stats.skipped
    );

    // Read-only service surface: a replica never writes or reindexes (it must stay byte-identical to
    // the primary), so there's no Write service and Admin has **no source** (describe works; reindex
    // / alter return Unimplemented).
    let table = match &resolved.source {
        growlerdb_core::Source::Iceberg(s) => s.table.clone(),
    };
    let search = growlerdb_engine::SearchService::new(handle.clone());
    let lookup =
        growlerdb_engine::LookupService::new(handle.clone(), IcebergConfig::from_env(), table);
    let suggest = growlerdb_engine::SuggestService::new(handle.clone());
    let admin = growlerdb_engine::AdminService::new(handle.clone(), index);
    let system = SystemService::new(VERSION);

    // Background poll: re-pull + hot-swap when the primary's snapshot advances. The swap is atomic
    // across every service; in-flight readers keep their old segment files (open-fd refs) until done.
    {
        let (backup, prefix, store, resolved, def_path, index_s, swap_handle, shard_id) = (
            backup.clone(),
            prefix.clone(),
            store.clone(),
            resolved.clone(),
            def_path.clone(),
            index.to_string(),
            handle.clone(),
            shard_id.clone(),
        );
        tokio::spawn(async move {
            let mut served = served_snapshot;
            let mut tick =
                tokio::time::interval(std::time::Duration::from_secs(refresh_secs.max(1)));
            tick.tick().await; // consume the immediate first tick — the initial pull already ran
            loop {
                tick.tick().await;
                match growlerdb_backup::refresh_and_reopen(
                    &backup,
                    &prefix,
                    &store,
                    &shard_id,
                    &resolved,
                    Some(&def_path),
                    served,
                )
                .await
                {
                    Ok((Some(shard), s)) => {
                        served = s.manifest.snapshot;
                        swap_handle.swap(Arc::new(shard));
                        println!(
                            "replica `{index_s}`: refreshed to snapshot {served} ({} new); swapped",
                            s.downloaded
                        );
                        growlerdb_telemetry::sli::background_success("replica-refresh");
                    }
                    // No new snapshot is still a healthy poll — the replica is up to date.
                    Ok((None, _)) => {
                        growlerdb_telemetry::sli::background_success("replica-refresh")
                    }
                    Err(e) => {
                        eprintln!(
                            "replica `{index_s}`: refresh failed ({e}); keeping current segments"
                        );
                        growlerdb_telemetry::sli::background_failure("replica-refresh");
                    }
                }
            }
        });
    }

    // Optional REST front (mirrors `serve`): route through an in-process LocalNode over the read
    // services. Admin-without-source means management calls degrade to Unimplemented, never writes.
    if let Some(rest_addr) = rest_addr {
        let rest_socket: std::net::SocketAddr = rest_addr
            .parse()
            .map_err(|e| anyhow::anyhow!("invalid --rest-addr `{rest_addr}`: {e}"))?;
        let node = growlerdb_engine::LocalNode::new(
            search.clone(),
            suggest.clone(),
            lookup.clone(),
            admin.clone(),
        );
        let gateway =
            Arc::new(growlerdb_engine::Gateway::new(node.shared()).serving(resolved.name.clone()));
        let router = rest_router(gateway, ui_dir);
        let listener = tokio::net::TcpListener::bind(rest_socket).await?;
        println!("replica REST/JSON gateway on http://{rest_socket}/v1/...");
        tokio::spawn(async move {
            let shutdown = async {
                let _ = tokio::signal::ctrl_c().await;
            };
            if let Err(e) = axum::serve(listener, router)
                .with_graceful_shutdown(shutdown)
                .await
            {
                eprintln!("rest gateway error: {e}");
            }
        });
    }

    let socket: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --addr `{addr}`: {e}"))?;
    let mut builder = Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
        println!("serving replica `{index}` on {socket} over mutual TLS");
    } else {
        eprintln!(
            "serve --replica: WARNING TLS disabled (no --tls-cert); internal traffic is plaintext"
        );
        println!(
            "serving replica `{index}` on {socket} \
             (read-only: Search + Lookup + Suggest + Admin-describe + System gRPC)"
        );
    }

    let readiness = spawn_health(metrics_addr).await?;
    readiness.mark_ready();

    builder
        .add_service(search.into_server())
        .add_service(lookup.into_server())
        .add_service(suggest.into_server())
        .add_service(admin.into_server())
        .add_service(SystemServer::new(system))
        .serve_with_shutdown(socket, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    println!("growlerdb serve --replica: shut down cleanly");
    Ok(())
}

/// Serve a **windowed** index: open its per-window shards, front them with a windowed
/// [`Gateway`](growlerdb_engine::Gateway) that prunes a time-filtered search to the matching
/// windows, and expose search over REST. Windowed search is **REST-first** today — the gRPC Node
/// surface is per-shard, and distributed per-window addressing is a later slice — so `--rest-addr`
/// is required; the gRPC listener serves System (health/version) so the node still presents a
/// discoverable endpoint and registers its windows with the control plane.
async fn serve_windowed(
    cfg: ServeConfig<'_>,
    store: growlerdb_index::LocalIndexStore,
    resolved: growlerdb_core::ResolvedIndex,
) -> anyhow::Result<()> {
    use growlerdb_engine::{
        AdminService, Gateway, LocalNode, LookupService, Node, SearchService, ShardHandle,
        SuggestService,
    };
    use growlerdb_index::ShardId;
    use growlerdb_proto::{SystemServer, SystemService};
    use std::sync::Arc;
    use tonic::transport::Server;

    // A windowed index shards by time window, not ordinal, so `max_inflight`/`shards`/`shard_ordinal`
    // (and `data_dir`, already consumed to open `store`) don't apply here.
    let ServeConfig {
        index,
        addr,
        rest_addr,
        tls,
        metrics_addr,
        ui_dir,
        register,
        advertise_addr,
        compact_interval_secs,
        remap_interval_secs,
        ..
    } = cfg;

    let Some(rest_addr) = rest_addr else {
        anyhow::bail!("a windowed index is served over REST — pass --rest-addr");
    };
    let windowing = resolved
        .windowing
        .clone()
        .expect("serve_windowed requires a windowed definition");
    let table = match &resolved.source {
        growlerdb_core::Source::Iceberg(s) => s.table.clone(),
    };

    // A windowed node may start **empty** (streaming-first): it registers into the CP
    // placement pool and creates each window on the first write the connector streams to it. So an
    // empty window set is valid — the node serves zero windows until ingest populates them (the batch
    // `growlerdb index` build path still pre-populates them when used).
    let windows = store.window_shards(index)?;

    // Cold windows are served **read-through** from object storage; build the shared object store +
    // range cache when any window is already parked OR automatic parking is enabled (it will create
    // cold windows at runtime and must have somewhere to write + a cache to serve them read-through).
    let park_interval = park_interval_secs();
    let any_cold = windows
        .iter()
        .any(|&w| matches!(store.cold_marker(index, w), Ok(Some(_))));
    let object_store = if any_cold || park_interval > 0 {
        // Fail fast: parking with no backup bucket configured is a misconfiguration, not a silent
        // no-op (`backup_s3_config` errors when `GROWLERDB_BACKUP_BUCKET` is unset).
        Some(growlerdb_backup::s3_store(&backup_s3_config()?)?)
    } else {
        None
    };
    let cache = object_store
        .as_ref()
        .map(|_| growlerdb_index::RangeCache::new(cold_cache_bytes()));

    // One in-process Node per window — a local Shard for a hot window, a read-through cold Shard for
    // a parked one (tagged with the marker's zone-map so the Gateway prunes it without a fetch). The
    // opens run on a blocking thread because the cold path `block_on`s object-storage reads.
    let (
        nodes,
        descriptors,
        _served,
        cold_ids,
        windowed_search,
        windowed_suggest,
        windowed_lookup,
        windowed_admin,
        hot_handles,
        cold_handles,
    ) = {
        let (store, resolved, index_s, table) = (
            store.clone(),
            resolved.clone(),
            index.to_string(),
            table.clone(),
        );
        let (windows, object_store, cache) = (windows.clone(), object_store.clone(), cache.clone());
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            type Built = (
                Vec<Arc<dyn Node>>,
                Vec<(i64, Option<(i64, i64)>)>,
                Vec<growlerdb_proto::v1::ServedWindow>,
                Vec<i64>,
                std::collections::BTreeMap<i64, SearchService>,
                std::collections::BTreeMap<i64, SuggestService>,
                std::collections::BTreeMap<i64, LookupService>,
                std::collections::BTreeMap<i64, AdminService>,
                Vec<(i64, ShardHandle)>,
                Vec<(i64, ShardHandle)>,
            );
            // Each window shard backs an in-process `LocalNode` (the embedded REST Gateway) plus the
            // gRPC window multiplexers over the *same* swappable handle: `SearchService` + `SuggestService`
            // and `LookupService` (hydration) + `AdminService` (describe). The handle
            // is also returned so a HOT window can be auto-compacted.
            let build = |shard: Arc<growlerdb_index::Shard>| -> (
                Arc<dyn Node>,
                SearchService,
                SuggestService,
                LookupService,
                AdminService,
                ShardHandle,
            ) {
                let handle = ShardHandle::new(shard);
                let node = LocalNode::new(
                    SearchService::new(handle.clone()),
                    SuggestService::new(handle.clone()),
                    LookupService::new(handle.clone(), IcebergConfig::from_env(), table.clone()),
                    AdminService::new(handle.clone(), &index_s),
                )
                .shared();
                (
                    node,
                    SearchService::new(handle.clone()),
                    SuggestService::new(handle.clone()),
                    LookupService::new(handle.clone(), IcebergConfig::from_env(), table.clone()),
                    AdminService::new(handle.clone(), &index_s),
                    handle,
                )
            };
            let mut nodes: Vec<Arc<dyn Node>> = Vec::with_capacity(windows.len());
            let mut descriptors = Vec::with_capacity(windows.len());
            let mut served = Vec::with_capacity(windows.len());
            let mut cold_ids = Vec::new();
            let mut windowed_search = std::collections::BTreeMap::new();
            let mut windowed_suggest = std::collections::BTreeMap::new();
            let mut windowed_lookup = std::collections::BTreeMap::new();
            let mut windowed_admin = std::collections::BTreeMap::new();
            let mut hot_handles: Vec<(i64, ShardHandle)> = Vec::new();
            let mut cold_handles: Vec<(i64, ShardHandle)> = Vec::new();
            for &w in &windows {
                let (node, search_svc, suggest_svc, lookup_svc, admin_svc, zone) = match store
                    .cold_marker(&index_s, w)?
                {
                    Some(marker) => {
                        cold_ids.push(w);
                        let op = object_store
                            .clone()
                            .expect("object store present when cold");
                        let cache = cache.clone().expect("cache present when cold");
                        let window_dir = store.shard_path(&ShardId::window(&index_s, w));
                        let bundle = marker
                            .bundle_key
                            .as_deref()
                            .zip(marker.bundle_manifest_key.as_deref());
                        let shard = Arc::new(store.open_cold_shard(
                            &resolved,
                            &window_dir,
                            op,
                            &marker.object_prefix,
                            cache,
                            marker.hotcache_key.as_deref(),
                            bundle,
                            None, // startup: no hot shard holds this window's aux.redb yet
                        )?);
                        // Cold = read-through, no writer → never compacted, but its handle is kept so
                        // an access-driven pre-warm loop can promote it back to hot.
                        let (node, search, suggest, lookup, admin, handle) = build(shard);
                        cold_handles.push((w, handle));
                        (
                            node,
                            search,
                            suggest,
                            lookup,
                            admin,
                            marker.event_min.zip(marker.event_max),
                        )
                    }
                    None => {
                        let shard =
                            Arc::new(store.open_shard(&ShardId::window(&index_s, w), &resolved)?);
                        let zone = shard.event_bounds()?;
                        let (node, search, suggest, lookup, admin, handle) = build(shard);
                        hot_handles.push((w, handle)); // hot → eligible for auto-compaction
                        (node, search, suggest, lookup, admin, zone)
                    }
                };
                nodes.push(node);
                windowed_search.insert(w, search_svc);
                windowed_suggest.insert(w, suggest_svc);
                windowed_lookup.insert(w, lookup_svc);
                windowed_admin.insert(w, admin_svc);
                descriptors.push((w, zone));
                served.push(growlerdb_proto::v1::ServedWindow {
                    window: w,
                    event_min: zone.map(|(lo, _)| lo).unwrap_or(0),
                    event_max: zone.map(|(_, hi)| hi).unwrap_or(0),
                    has_event_bounds: zone.is_some(),
                });
            }
            Ok::<Built, anyhow::Error>((
                nodes,
                descriptors,
                served,
                cold_ids,
                windowed_search,
                windowed_suggest,
                windowed_lookup,
                windowed_admin,
                hot_handles,
                cold_handles,
            ))
        })
        .await??
    };
    let cold_count = cold_ids.len();
    let hot_count = windows.len() - cold_count;

    // Dynamic windowed ingest: the search/suggest mux maps become **shared + mutable** so
    // the windowed write path can add a window created at runtime, and we snapshot the boot windows
    // as the write service's seed (window → handle/node/zone). Built before `hot_handles`/`nodes` are
    // consumed below.
    let handle_by_window: std::collections::BTreeMap<i64, growlerdb_engine::ShardHandle> =
        hot_handles
            .iter()
            .chain(cold_handles.iter())
            .map(|(w, h)| (*w, h.clone()))
            .collect();
    let window_seed: std::collections::BTreeMap<i64, growlerdb_engine::WindowSeed> = nodes
        .iter()
        .zip(descriptors.iter())
        .map(|(node, (w, zone))| (*w, (handle_by_window[w].clone(), node.clone(), *zone)))
        .collect();
    let search_windows: growlerdb_engine::SharedSearchWindows =
        Arc::new(std::sync::RwLock::new(windowed_search));
    let suggest_windows: growlerdb_engine::SharedSuggestWindows =
        Arc::new(std::sync::RwLock::new(windowed_suggest));
    let lookup_windows: growlerdb_engine::SharedLookupWindows =
        Arc::new(std::sync::RwLock::new(windowed_lookup));
    let admin_windows: growlerdb_engine::SharedAdminWindows =
        Arc::new(std::sync::RwLock::new(windowed_admin));

    // Background **compaction re-map** across the HOT windows: one poll + one
    // key scan of the rewritten files serves every window (each skips keys it doesn't hold).
    // Cold read-through windows keep the lazy verify-and-refresh + dead-file short-circuit.
    spawn_locator_remap(
        hot_handles.iter().map(|(_, h)| h.clone()).collect(),
        index.to_string(),
        table.clone(),
        resolved.key.partition_fields.clone(),
        resolved.key.identifier_fields.clone(),
        remap_interval_secs,
    );

    // Auto-compact each HOT window shard: under steady ingest the current window
    // accumulates segments, so each hot window gets its own health-driven compaction loop. Cold
    // read-through windows have no writer and are skipped.
    for (w, handle) in hot_handles {
        spawn_auto_compaction(handle, format!("{index} w{w}"), compact_interval_secs);
    }

    // Access-driven pre-warm: each cold window watches its read rate; a parked window that
    // gets hot again is promoted back to a local hot shard (un-bundled from object storage) and
    // hot-swapped in, so it stops paying cold-tier latency. Needs the object store (present iff cold).
    if let Some(op) = &object_store {
        for (w, handle) in cold_handles {
            spawn_prewarm(
                handle,
                store.clone(),
                op.clone(),
                resolved.clone(),
                index.to_string(),
                w,
                compact_interval_secs,
            );
        }
    }

    // Tag the Gateway with cold-tier status (per-window tier + the shared cache) for `GET /v1/cold`.
    let mut gateway = Gateway::windowed(nodes, windowing.clone(), descriptors);
    if let Some(cache) = &cache {
        gateway = gateway.with_cold_tier(cold_ids, cache.clone());
    }
    let gateway = Arc::new(gateway);

    // The windowed **write** service: routes each streamed doc to its window shard,
    // creating the window on first write and publishing it (mux + this gateway) so it's immediately
    // queryable. A new window also gets its own auto-compaction loop via `on_new_window`.
    let on_new_window: growlerdb_engine::OnNewWindow = {
        let idx = index.to_string();
        let ci = compact_interval_secs;
        Arc::new(move |w, handle| spawn_auto_compaction(handle, format!("{idx} w{w}"), ci))
    };
    let write_service = growlerdb_engine::WindowedWriteService::new(
        store.clone(),
        resolved.clone(),
        table.clone(),
        IcebergConfig::from_env(),
        window_seed,
        search_windows.clone(),
        suggest_windows.clone(),
        lookup_windows.clone(),
        admin_windows.clone(),
        gateway.clone(),
        on_new_window,
    )?;

    // Automatic cold-tiering (opt-in via GROWLERDB_PARK_INTERVAL_SECS): demote aged windows past
    // the `hot_windows` policy to cold read-through in the background — the hot→cold counterpart of
    // the access-driven pre-warm above. Reads the write service's live window set so windows created
    // at runtime by ingest are parked as they age. Needs the shared object store + cache (both present
    // when park is enabled).
    if let (Some(op), Some(cache)) = (&object_store, &cache) {
        spawn_park(
            write_service.clone(),
            store.clone(),
            op.clone(),
            cache.clone(),
            resolved.clone(),
            windowing.clone(),
            index.to_string(),
            compact_interval_secs,
            park_interval,
        );
    }

    // REST listener — the windowed search surface.
    let rest_socket: std::net::SocketAddr = rest_addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --rest-addr `{rest_addr}`: {e}"))?;
    let router = rest_router(gateway.clone(), ui_dir);
    let listener = tokio::net::TcpListener::bind(rest_socket).await?;
    println!(
        "serving windowed index `{index}` ({} windows: {hot_count} hot, {cold_count} cold read-through) REST/JSON on http://{rest_socket}/v1/...",
        windows.len()
    );
    tokio::spawn(async move {
        let shutdown = async {
            let _ = tokio::signal::ctrl_c().await;
        };
        if let Err(e) = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            eprintln!("rest gateway error: {e}");
        }
    });

    let readiness = spawn_health(metrics_addr).await?;

    // Report the served windows (+ zone-maps) to the control plane so a cluster-level Gateway can
    // route to them. Retries until reachable and re-announces on an interval — same K8s
    // startup race as the sharded path; `/readyz` stays not-ready until registered.
    if let (Some(cp), Some(endpoint)) = (register, advertise_addr) {
        // Dynamic windowed registration: heartbeat into the CP placement pool (so new
        // windows can be placed here) AND re-announce the windows this node currently serves (+
        // zone-maps) each tick — so a window created since boot is advertised, not just the boot set.
        let label = format!("windowed `{index}` at {endpoint}");
        spawn_windowed_registration(
            cp.to_string(),
            endpoint.to_string(),
            resolved.clone(),
            write_service.clone(),
            readiness.clone(),
            label,
        );
    } else {
        readiness.mark_ready();
    }

    // gRPC listener: System (health/version) + the **window multiplexers** — `Search` and `Suggest`
    // plus `Lookup` (hydration) and `Admin` (describe) — over `window id → service`
    // maps that dispatch by the request's window selector, so a cluster Gateway can route per-window
    // requests to this one endpoint. (Aggregate/PIT over distributed windows are follow-ons.)
    let socket: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --addr `{addr}`: {e}"))?;
    let mut builder = Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
    }
    builder
        .add_service(
            growlerdb_engine::WindowedSearchService::new(search_windows.clone()).into_server(),
        )
        .add_service(
            growlerdb_engine::WindowedSuggestService::new(suggest_windows.clone()).into_server(),
        )
        // Hydration (keys:get) + describe over the windows: the Gateway broadcasts a
        // hydration to every window and fans a describe to each, dispatched by selector here.
        .add_service(
            growlerdb_engine::WindowedLookupService::new(lookup_windows.clone()).into_server(),
        )
        .add_service(
            growlerdb_engine::WindowedAdminService::new(admin_windows.clone()).into_server(),
        )
        // The windowed Write service — the connector streams each window's rows here.
        .add_service(write_service.into_server())
        .add_service(SystemServer::new(SystemService::new(VERSION)))
        .serve_with_shutdown(socket, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    println!("growlerdb serve: shut down cleanly");
    Ok(())
}

/// Everything [`gateway`] needs, bundled into one struct instead of many positional
/// args. Borrows its string config from the dispatched `Command`; `node_tls` is owned (moved in).
pub struct GatewayConfig<'a> {
    pub node_addr: Option<&'a str>,
    pub registry: Option<&'a str>,
    pub index: Option<&'a str>,
    /// Front every registered index over one endpoint. Mutually exclusive with `index`.
    pub all_indexes: bool,
    pub addr: &'a str,
    pub rest_addr: &'a str,
    pub oidc_issuer: Option<&'a str>,
    pub oidc_audience: Option<&'a str>,
    pub builtin_auth: bool,
    pub auth_secret: Option<&'a str>,
    pub node_tls: Option<tonic::transport::ClientTlsConfig>,
    pub metrics_addr: Option<&'a str>,
    pub ui_dir: Option<&'a str>,
    pub control_plane: Option<&'a str>,
    pub prometheus: Option<&'a str>,
    pub opensearch: bool,
    pub reload_secs: u64,
    /// Injected authenticator. When `Some`, it **takes precedence** over the flag-driven
    /// OIDC/built-in auth — an out-of-tree (e.g. enterprise) build supplies its own here, typically
    /// a [`ChainAuthenticator`](growlerdb_engine::ChainAuthenticator) combining enterprise + open
    /// methods. The binary always passes `None`.
    pub authn: Option<growlerdb_engine::SharedAuthn>,
}

/// Run a standalone Gateway: terminate the Engine API over **gRPC** (on `addr`) and **REST**
/// (on `rest_addr`), routing both to one or more remote Nodes. The same `Gateway` the embedded
/// `serve` uses, but over [`RemoteNode`]s instead of a `LocalNode` — either a single Node
/// (`node_addr`) or every shard primary of an index from the Control-Plane registry.
pub async fn gateway(cfg: GatewayConfig<'_>) -> anyhow::Result<()> {
    use std::sync::Arc;
    use tonic::transport::Server;

    let GatewayConfig {
        node_addr,
        registry,
        index,
        all_indexes,
        addr,
        rest_addr,
        oidc_issuer,
        oidc_audience,
        builtin_auth,
        auth_secret,
        node_tls,
        metrics_addr,
        ui_dir,
        control_plane,
        prometheus,
        opensearch,
        reload_secs,
        authn: injected_authn,
    } = cfg;

    let grpc_socket: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --addr `{addr}`: {e}"))?;
    let rest_socket: std::net::SocketAddr = rest_addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --rest-addr `{rest_addr}`: {e}"))?;

    // Serve /healthz + /readyz **before** building the gateway: if the gateway must WAIT
    // for the control-plane at boot (rolled together), /healthz stays up so liveness passes and the
    // pod isn't killed, while /readyz stays not-ready until the routing snapshot is in hand (marked
    // ready just before the gRPC serve below). Net: up-but-not-ready during the wait, never
    // CrashLoopBackOff.
    let readiness = spawn_health(metrics_addr).await?;

    // For `--registry` ordinal/alias indexes, remember what the hot-reload loop needs (registry
    // path, index, a TLS clone) — spawned after the gateway is `Arc`-wrapped below. Windowed indexes
    // aren't reloaded.
    let mut reload: Option<(String, String, Option<tonic::transport::ClientTlsConfig>)> = None;
    // Control-plane (gRPC) hot-reload: (cp endpoint, index, tls, startup fingerprint to seed `last`).
    let mut reload_cp: Option<(
        String,
        String,
        Option<tonic::transport::ClientTlsConfig>,
        RoutingFingerprint,
    )> = None;
    // The windowed analog: the cluster gateway re-polls `GetIndex` and swaps in windows
    // created/placed at runtime, so a temporal workload's new windows become queryable with no restart.
    let mut reload_cp_windowed: Option<(
        String,
        String,
        Option<tonic::transport::ClientTlsConfig>,
        WindowFingerprint,
    )> = None;
    let (gw, routed_to) = if all_indexes {
        // Multi-index: front EVERY registered index over one endpoint, resolving each
        // named index lazily from the live control-plane on first use and hot-reloading each
        // independently. Readiness (below) is the control plane's reachability — we don't block boot
        // on any one index resolving, so a fresh cluster with no indexes yet still serves.
        let cp = control_plane.ok_or_else(|| {
            anyhow::anyhow!(
                "--all-indexes requires --control-plane (the live registry to route from)"
            )
        })?;
        // Wait until the control plane is reachable: up but /readyz not-ready meanwhile,
        // rather than crash-looping when rolled alongside the control-plane.
        wait_for_control_plane(cp).await;
        let resolver = std::sync::Arc::new(CpRouteResolver {
            cp: cp.to_string(),
            node_tls: node_tls.clone(),
            reload_secs,
        });
        let gw = growlerdb_engine::Gateway::multi_index(resolver, None);
        (gw, format!("all indexes via control-plane {cp}"))
    } else {
        match (registry, index, node_addr) {
            (Some(registry), Some(index), _) => {
                let gw = gateway_from_registry(registry, index, node_tls.clone()).await?;
                let desc = format!("index `{index}` ({} shard(s))", gw.shard_count());
                let windowed = growlerdb_controlplane::Registry::open(registry)
                    .ok()
                    .and_then(|r| r.get(index))
                    .is_some_and(|e| e.definition.windowing.is_some());
                if reload_secs > 0 && !windowed {
                    reload = Some((registry.to_string(), index.to_string(), node_tls.clone()));
                }
                (gw, desc)
            }
            // Sharded routing from the **live control-plane** over gRPC (no registry file) — the
            // distributed/Kubernetes path: the control-plane and gateway are separate pods,
            // so there's no shared registry.json to read.
            (None, Some(index), _) => {
                let cp = control_plane.ok_or_else(|| {
                anyhow::anyhow!(
                    "--index without --registry requires --control-plane (the live registry to route from)"
                )
            })?;
                let (gw, reload) = gateway_from_control_plane(cp, index, node_tls.clone()).await;
                let desc = format!(
                    "index `{index}` ({} shard(s)) via control-plane {cp}",
                    gw.shard_count()
                );
                // Wire the reload matching the index kind: ordinal → swap_routing; windowed →
                // swap_windowed (so runtime-created windows are picked up).
                if reload_secs > 0 {
                    match reload {
                        CpReload::Ordinal(fp) => {
                            reload_cp =
                                Some((cp.to_string(), index.to_string(), node_tls.clone(), fp));
                        }
                        CpReload::Windowed(fp) => {
                            reload_cp_windowed =
                                Some((cp.to_string(), index.to_string(), node_tls.clone(), fp));
                        }
                    }
                }
                (gw, desc)
            }
            (_, _, Some(node_addr)) => {
                let node = connect_node(node_addr, node_tls).await?;
                (
                    growlerdb_engine::Gateway::new(Arc::new(node)),
                    format!("Node {node_addr}"),
                )
            }
            _ => {
                anyhow::bail!("provide --node-addr, --registry + --index, --control-plane + --index, or --all-indexes")
            }
        }
    };

    // An injected authenticator (out-of-tree/enterprise build) takes precedence over the
    // flag-driven auth below; it is authoritative and carries its own methods (typically a
    // ChainAuthenticator), so we simply install it plus the standard RBAC.
    let gw = if let Some(authn) = injected_authn {
        println!("gateway: authentication via an injected authenticator");
        gw.with_authn(authn)
            .with_password_login(builtin_auth)
            .with_authz(Arc::new(growlerdb_engine::RbacPolicy::with_default_roles()))
    }
    // Optional OIDC/JWT authentication. When enabled, fetch the issuer's JWKS up front (so a
    // misconfigured issuer fails fast at startup, not per request) and keep it fresh on a timer
    // to follow key rotation.
    else if let Some(issuer) = oidc_issuer {
        let audience = oidc_audience
            .ok_or_else(|| anyhow::anyhow!("--oidc-audience is required with --oidc-issuer"))?;
        let authn = Arc::new(growlerdb_engine::JwksAuthenticator::for_issuer(
            issuer, audience,
        ));
        authn
            .refresh()
            .await
            .map_err(|e| anyhow::anyhow!("fetching OIDC keys from `{issuer}`: {e}"))?;
        spawn_jwks_refresher(authn.clone());
        println!("gateway: OIDC/JWT authentication enabled (issuer `{issuer}`, aud `{audience}`)");
        // With authenticated roles in hand, enforce coarse control-plane RBAC:
        // map the verified roles to operation scopes and reject calls that lack them.
        println!("gateway: RBAC enabled (viewer / index-admin / operator / service roles)");
        gw.with_authn(authn)
            .with_authz(Arc::new(growlerdb_engine::RbacPolicy::with_default_roles()))
    } else if builtin_auth {
        // Built-in (no external IdP) closed mode: validate the HS256 session JWTs the
        // control-plane's /v1/login mints, using the shared secret. Same iss/aud as the minter.
        let secret = auth_secret
            .ok_or_else(|| anyhow::anyhow!("--auth-secret is required with --builtin-auth"))?;
        let authn = Arc::new(growlerdb_engine::JwtAuthenticator::from_hs256_secret(
            secret.as_bytes(),
            growlerdb_engine::BUILTIN_SESSION_ISSUER,
            growlerdb_engine::BUILTIN_SESSION_AUDIENCE,
        ));
        println!("gateway: built-in password authentication enabled (session JWTs via /v1/login)");
        gw.with_authn(authn)
            .with_password_login(true)
            .with_authz(Arc::new(growlerdb_engine::RbacPolicy::with_default_roles()))
    } else {
        eprintln!(
            "gateway: WARNING authentication disabled (no --oidc-issuer / --builtin-auth); the gateway is open"
        );
        gw
    };
    let gw = Arc::new(gw);

    // Hot-reload the topology after a reshard cutover: poll the registry and swap in the
    // new shard set + router with no restart.
    if let Some((registry_path, idx, tls)) = reload {
        spawn_registry_reloader(gw.clone(), registry_path, idx.clone(), tls, reload_secs);
        println!("gateway: topology hot-reload on for `{idx}` (every {reload_secs}s)");
    }
    if let Some((cp, idx, tls, fp)) = reload_cp {
        spawn_control_plane_reloader(gw.clone(), cp.clone(), idx.clone(), tls, reload_secs, fp);
        println!("gateway: topology hot-reload on for `{idx}` via control-plane {cp} (every {reload_secs}s)");
    }
    if let Some((cp, idx, tls, fp)) = reload_cp_windowed {
        spawn_windowed_control_plane_reloader(
            gw.clone(),
            cp.clone(),
            idx.clone(),
            tls,
            reload_secs,
            fp,
        );
        println!("gateway: windowed hot-reload on for `{idx}` via control-plane {cp} (every {reload_secs}s)");
    }

    // REST front on its own listener; with a Control Plane, also expose index management.
    let mut router = rest_router(gw.clone(), ui_dir);
    if let Some(cp) = control_plane {
        let client = connect_cp_with_retry(cp).await;
        router = router.merge(growlerdb_engine::rest::control_router(client));
        println!("gateway: index management on http://{rest_socket}/v1/indexes → {cp}");
    }
    if let Some(prom) = prometheus {
        router = router.merge(growlerdb_engine::rest::stats_router(prom));
        println!("gateway: SLI metrics proxy on http://{rest_socket}/v1/stats/... → {prom}");
    }
    if opensearch {
        router = router.merge(growlerdb_engine::opensearch_router(gw.clone()));
        println!("gateway: OpenSearch-compatible adapter on http://{rest_socket}/<index>/_search");
    }
    // RED metrics for every REST endpoint: one layer over the fully-merged router, so
    // `MatchedPath` resolves the route template for all `/v1/*` routes.
    router = router.layer(axum::middleware::from_fn(
        growlerdb_engine::rest::track_http_metrics,
    ));
    let rest_listener = tokio::net::TcpListener::bind(rest_socket).await?;
    println!("gateway: REST Engine API on http://{rest_socket}/v1/... → {routed_to}");
    tokio::spawn(async move {
        let shutdown = async {
            let _ = tokio::signal::ctrl_c().await;
        };
        if let Err(e) = axum::serve(rest_listener, router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            eprintln!("gateway REST error: {e}");
        }
    });

    // gRPC front: Gateway-backed Search/Suggest/Lookup/Admin routing to the Node(s).
    let (search, suggest, lookup, admin) = growlerdb_engine::gateway_grpc::servers(gw);
    println!("gateway: gRPC Engine API on {grpc_socket} → {routed_to}");
    // Routing snapshot in hand + fronts bound → ready. Health was
    // spawned early (above); only now do we flip /readyz to ready.
    readiness.mark_ready();
    Server::builder()
        .add_service(search)
        .add_service(suggest)
        .add_service(lookup)
        .add_service(admin)
        .serve_with_shutdown(grpc_socket, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    println!("growlerdb gateway: shut down cleanly");
    Ok(())
}

/// The REST Engine-API router, optionally also serving the built UI SPA from `ui_dir`.
fn rest_router(
    gateway: std::sync::Arc<growlerdb_engine::Gateway>,
    ui_dir: Option<&str>,
) -> axum::Router {
    match ui_dir {
        Some(dir) => {
            println!("serving UI from `{dir}` at the REST front");
            growlerdb_engine::rest::router_with_ui(gateway, std::path::Path::new(dir))
        }
        None => growlerdb_engine::rest::router(gateway),
    }
}

/// Spawn the health/readiness + Prometheus `/metrics` server on `addr`, returning a
/// [`Readiness`](growlerdb_telemetry::Readiness) the caller flips once warm. With no `addr` the
/// probe surface is disabled and the returned handle is already ready (nothing to gate).
async fn spawn_health(addr: Option<&str>) -> anyhow::Result<growlerdb_telemetry::Readiness> {
    let readiness = growlerdb_telemetry::Readiness::new();
    let Some(addr) = addr else {
        readiness.mark_ready();
        return Ok(readiness);
    };
    let socket: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --metrics-addr `{addr}`: {e}"))?;
    let listener = tokio::net::TcpListener::bind(socket).await?;
    let router = growlerdb_telemetry::health_router(readiness.clone());
    println!("telemetry: /healthz /readyz /metrics on http://{socket}");
    tokio::spawn(async move {
        let shutdown = async {
            let _ = tokio::signal::ctrl_c().await;
        };
        if let Err(e) = axum::serve(listener, router)
            .with_graceful_shutdown(shutdown)
            .await
        {
            eprintln!("telemetry server error: {e}");
        }
    });
    Ok(readiness)
}

/// Refresh the JWKS every 5 minutes so the gateway follows the IdP's key rotation. A failed
/// refresh logs and retains the previous keys — an IdP blip must not blank authentication.
fn spawn_jwks_refresher(authn: std::sync::Arc<growlerdb_engine::JwksAuthenticator>) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(300));
        tick.tick().await; // the immediate first tick — the startup fetch already ran
        loop {
            tick.tick().await;
            match authn.refresh().await {
                Ok(()) => growlerdb_telemetry::sli::background_success("jwks-refresh"),
                Err(e) => {
                    eprintln!("gateway: JWKS refresh failed (keeping current keys): {e}");
                    growlerdb_telemetry::sli::background_failure("jwks-refresh");
                }
            }
        }
    });
}

/// Build a **sharded** [`Gateway`] from an index's Control-Plane entry: read its shard map from
/// `registry.json`, validate it (a primary on every contiguous shard `0..N`) via
/// [`shard_primaries`](growlerdb_engine::shard_primaries), then connect a [`RemoteNode`] to each
/// shard's primary **in ordinal order** so the Gateway's shard `i` is the registry's shard `i`.
/// Each [`NodeId`](growlerdb_controlplane::NodeId) is taken to be that node's gRPC endpoint. The
/// key router is derived from the **index definition** (partition routing when the key is
/// partitioned, else hash), so reads land on the same shard the connector wrote to.
async fn gateway_from_registry(
    registry_path: &str,
    name: &str,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<growlerdb_engine::Gateway> {
    let registry = growlerdb_controlplane::Registry::open(registry_path)
        .map_err(|e| anyhow::anyhow!("opening registry `{registry_path}`: {e}"))?;
    // A single **windowed** index routes per time window, not over an ordinal shard map
    // (which it doesn't have) — front its windows over `WindowNode`s so a time-filtered search
    // prunes to the owning windows across nodes.
    if let Some(entry) = registry.get(name) {
        if let Some(windowing) = entry.definition.windowing.clone() {
            return gateway_windowed_from_registry(&registry, name, windowing, node_tls).await;
        }
    }
    // Otherwise `name` is an ordinal index or an **alias**: connect the shard primaries +
    // build the router. Factored out so the hot-reload loop ([`spawn_registry_reloader`]) can re-run
    // it on a topology change and swap the result in.
    let (nodes, router) = resolve_sharded_routing(&registry, name, node_tls).await?;
    // Search fan-out pruning: if the index is partition-routed on keyword fields, tell the
    // Gateway their names so a search pinning them routes to the owning shard instead of broadcasting.
    let partition_fields = registry
        .get(name)
        .map(|e| keyword_partition_fields(&e.definition))
        .unwrap_or_default();
    Ok(growlerdb_engine::Gateway::sharded_with(nodes, router)
        .with_partition_fields(partition_fields))
}

/// The index's partition-key field names **iff every one is a keyword** field — the
/// precondition for search fan-out pruning to route a string-valued query filter to the exact shard.
/// A non-keyword partition field (int/date/…) would route a string value to the wrong shard and drop
/// results, so a mixed partition disables pruning entirely (returns empty → the Gateway fans out).
fn keyword_partition_fields(def: &growlerdb_core::ResolvedIndex) -> Vec<String> {
    let all_keyword = !def.key.partition_fields.is_empty()
        && def.key.partition_fields.iter().all(|pf| {
            def.fields
                .iter()
                .any(|f| &f.path == pf && f.ty == growlerdb_core::FieldType::Keyword)
        });
    if all_keyword {
        def.key.partition_fields.clone()
    } else {
        Vec::new()
    }
}

/// (shard endpoints, bucket_owners) — the registry state that determines a sharded gateway's
/// topology. The hot-reload loop swaps the gateway only when this changes, so an unrelated
/// registry write (another index, an ingestion update) doesn't churn node connections.
type RoutingFingerprint = (Vec<String>, Vec<u32>);

/// The current routing fingerprint for `name` — read straight from the registry, **without**
/// connecting to any node (so the poll is cheap).
fn routing_fingerprint(
    registry: &growlerdb_controlplane::Registry,
    name: &str,
) -> anyhow::Result<RoutingFingerprint> {
    let (members, endpoints) = resolve_targets(registry, name)?;
    let owners = if members.len() == 1 {
        registry
            .get(&members[0])
            .map(|e| e.bucket_owners.clone())
            .unwrap_or_default()
    } else {
        Vec::new()
    };
    Ok((endpoints, owners))
}

/// Resolve `name`'s ordinal/alias shards into connected [`RemoteNode`]s + the key router. A single
/// index routes through its **virtual-bucket map** when present (the same map the connector
/// reads), else legacy `fnv % shards`; an **alias** hashes over the union of its members' nodes.
async fn resolve_sharded_routing(
    registry: &growlerdb_controlplane::Registry,
    name: &str,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<(
    Vec<std::sync::Arc<dyn growlerdb_engine::Node>>,
    growlerdb_core::ShardRouter,
)> {
    use std::sync::Arc;
    let (members, endpoints) = resolve_targets(registry, name)?;
    let mut nodes: Vec<Arc<dyn growlerdb_engine::Node>> = Vec::with_capacity(endpoints.len());
    for (i, endpoint) in endpoints.iter().enumerate() {
        // Lazy connect: tolerant of a down shard + re-resolves DNS on reconnect.
        let node = connect_node_lazy(endpoint, node_tls.clone())
            .map_err(|e| anyhow::anyhow!("shard {i} primary `{endpoint}`: {e}"))?;
        nodes.push(Arc::new(node));
    }
    let router = if members.len() == 1 {
        let entry = registry.get(&members[0]).expect("resolved member exists");
        growlerdb_core::ShardRouter::from_registry(
            entry.definition.routing_strategy(),
            &entry.bucket_owners,
            nodes.len() as u32,
        )
        .map_err(|e| anyhow::anyhow!("index `{}` bucket map: {e}", members[0]))?
    } else {
        growlerdb_core::ShardRouter::hashed(nodes.len() as u32)
    };
    Ok((nodes, router))
}

/// Poll the registry every `secs` and **hot-reload** the gateway's topology when it changes
/// after a reshard cutover (new bucket map plus nodes), the running gateway picks up the
/// new shard set and router with no restart. Each tick does a cheap fingerprint check; only a real
/// change reconnects nodes and swaps. A read error keeps the current topology (an outage must not
/// blank the gateway). Ordinal/alias indexes only — windowed gateways aren't reloaded.
fn spawn_registry_reloader(
    gateway: std::sync::Arc<growlerdb_engine::Gateway>,
    registry_path: String,
    index: String,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
    secs: u64,
) {
    tokio::spawn(async move {
        let open = |p: &str| growlerdb_controlplane::Registry::open(p);
        let mut last: Option<RoutingFingerprint> = open(&registry_path)
            .ok()
            .and_then(|r| routing_fingerprint(&r, &index).ok());
        // One-time phase offset so fleet-wide gateways don't all poll on the same tick; the
        // interval preserves the phase thereafter.
        tokio::time::sleep(jittered(std::time::Duration::from_secs(secs), 0.5)).await;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        tick.tick().await; // the immediate first tick is the startup state
        loop {
            tick.tick().await;
            let registry = match open(&registry_path) {
                Ok(r) => r,
                Err(e) => {
                    eprintln!("gateway: registry reopen failed (keeping topology): {e}");
                    growlerdb_telemetry::sli::background_failure("registry-reload");
                    continue;
                }
            };
            let fp = match routing_fingerprint(&registry, &index) {
                Ok(fp) => fp,
                Err(e) => {
                    eprintln!("gateway: registry read failed (keeping topology): {e}");
                    growlerdb_telemetry::sli::background_failure("registry-reload");
                    continue;
                }
            };
            if Some(&fp) == last.as_ref() {
                growlerdb_telemetry::sli::background_success("registry-reload");
                continue; // nothing relevant changed
            }
            match resolve_sharded_routing(&registry, &index, node_tls.clone()).await {
                Ok((nodes, router)) => {
                    gateway.swap_routing(nodes, router);
                    eprintln!(
                        "gateway: hot-reloaded `{index}` topology ({} shards)",
                        gateway.shard_count()
                    );
                    last = Some(fp);
                    growlerdb_telemetry::sli::background_success("registry-reload");
                }
                Err(e) => {
                    eprintln!("gateway: topology reload failed (keeping current): {e}");
                    growlerdb_telemetry::sli::background_failure("registry-reload");
                }
            }
        }
    });
}

/// Backoff between gateway startup attempts to reach the control-plane + resolve the index's shards
/// The gateway retries **unboundedly** (up but /readyz not-ready) rather than
/// exiting, so a gateway rolled alongside the control-plane waits instead of crash-looping.
const GW_CP_STARTUP_INTERVAL_SECS: u64 = 5;

/// The pure routing plan from a control-plane `GetIndex` response: each shard's primary endpoint
/// **in ordinal order**, the resolved routing strategy, and the virtual-bucket map. Split from the
/// network (connect) step so the validation is unit-testable without a gRPC server. Errors if the
/// index is windowed (routed elsewhere — see below), its ordinals aren't a contiguous
/// `0..shard_count`, or any shard has no assigned primary yet (still building / not registered).
fn routing_plan_from_get_index(
    index: &str,
    resp: &growlerdb_proto::v1::GetIndexResponse,
) -> anyhow::Result<(Vec<String>, growlerdb_core::RoutingStrategy, Vec<u32>)> {
    // A windowed index is fronted by [`windowed_gateway_from_get_index`], not the ordinal
    // planner, and its reloader is never wired — so reaching here with window shards is an internal
    // routing bug, not an unsupported case.
    if resp.shard_status.iter().any(|s| s.window != 0) || resp.windowing.is_some() {
        anyhow::bail!(
            "`{index}` is windowed but reached the ordinal routing planner — it must route through \
             the windowed gateway path"
        );
    }
    let mut shards: Vec<&growlerdb_proto::v1::ShardStatus> = resp.shard_status.iter().collect();
    shards.sort_by_key(|s| s.ordinal);
    if shards.len() as u32 != resp.shard_count {
        anyhow::bail!(
            "`{index}`: control-plane reports {} shard(s) but {} placement entries",
            resp.shard_count,
            shards.len()
        );
    }
    let mut endpoints = Vec::with_capacity(shards.len());
    for (pos, s) in shards.iter().enumerate() {
        if s.ordinal as usize != pos {
            anyhow::bail!(
                "`{index}`: shard ordinals are not a contiguous 0..{} (ordinal {} at position {pos})",
                resp.shard_count,
                s.ordinal
            );
        }
        if s.primary.is_empty() {
            anyhow::bail!("`{index}`: shard {} has no assigned primary yet", s.ordinal);
        }
        endpoints.push(s.primary.clone());
    }
    // ROUTING_PARTITION = 1 in the proto enum; anything else (incl. ROUTING_HASH = 0) → hash.
    let strategy = if resp.routing == growlerdb_proto::v1::RoutingStrategy::RoutingPartition as i32
    {
        growlerdb_core::RoutingStrategy::Partition
    } else {
        growlerdb_core::RoutingStrategy::Hash
    };
    Ok((endpoints, strategy, resp.bucket_owners.clone()))
}

/// Resolve `index`'s shards from the **live Control-Plane** over gRPC and connect each primary in
/// ordinal order — the gRPC analog of [`resolve_sharded_routing`] (which reads a registry file).
/// Returns the connected nodes, the key router, and the [`RoutingFingerprint`] for hot-reload.
async fn resolve_sharded_routing_cp(
    client: &mut growlerdb_proto::service_token::CpClient,
    index: &str,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<(
    Vec<std::sync::Arc<dyn growlerdb_engine::Node>>,
    growlerdb_core::ShardRouter,
    RoutingFingerprint,
)> {
    let resp = client
        .get_index(growlerdb_proto::v1::GetIndexRequest {
            name: index.to_string(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("control-plane GetIndex(`{index}`): {}", e.message()))?
        .into_inner();
    connect_sharded_from_get_index(index, &resp, node_tls)
}

/// (connected shard nodes, key router, routing fingerprint) — the resolved ordinal routing an
/// ordinal-index gateway is built from.
type CpShardRouting = (
    Vec<std::sync::Arc<dyn growlerdb_engine::Node>>,
    growlerdb_core::ShardRouter,
    RoutingFingerprint,
);

/// Connect an **ordinal** index's shard primaries from an already-fetched `GetIndex` response and
/// build the key router — the connect step shared by [`resolve_sharded_routing_cp`] (reloader) and
/// [`gateway_from_control_plane`] (startup, which fetches once and branches windowed vs ordinal).
fn connect_sharded_from_get_index(
    index: &str,
    resp: &growlerdb_proto::v1::GetIndexResponse,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<CpShardRouting> {
    use std::sync::Arc;
    let (endpoints, strategy, bucket_owners) = routing_plan_from_get_index(index, resp)?;
    let shard_count = endpoints.len() as u32;
    let mut nodes: Vec<Arc<dyn growlerdb_engine::Node>> = Vec::with_capacity(endpoints.len());
    for (ord, endpoint) in endpoints.iter().enumerate() {
        // Lazy connect: never fail the build on an unreachable shard, and re-resolve DNS
        // on reconnect so a returned-at-new-IP shard recovers and a still-down one fails fast → partial.
        let node = connect_node_lazy(endpoint, node_tls.clone())
            .map_err(|e| anyhow::anyhow!("shard {ord} primary `{endpoint}`: {e}"))?;
        nodes.push(Arc::new(node));
    }
    let router = growlerdb_core::ShardRouter::from_registry(strategy, &bucket_owners, shard_count)
        .map_err(|e| anyhow::anyhow!("index `{index}` bucket map: {e}"))?;
    Ok((nodes, router, (endpoints, bucket_owners)))
}

/// Reconstruct the [`TimeWindowing`](growlerdb_core::TimeWindowing) config from a control-plane
/// `GetIndex` response's [`WindowingConfig`](growlerdb_proto::v1::WindowingConfig) — so a
/// live-CP windowed gateway can prune exactly like the file-registry path (which reads it from the
/// stored definition).
fn windowing_from_get_index(
    index: &str,
    wc: &growlerdb_proto::v1::WindowingConfig,
) -> anyhow::Result<growlerdb_core::TimeWindowing> {
    use growlerdb_core::WindowGranularity::{Daily, Hourly, Weekly};
    let granularity = match wc.granularity.as_str() {
        "hourly" => Hourly,
        "daily" => Daily,
        "weekly" => Weekly,
        other => {
            anyhow::bail!("`{index}`: control-plane sent unknown window granularity `{other}`")
        }
    };
    Ok(growlerdb_core::TimeWindowing {
        field: wc.field.clone(),
        granularity,
        event_time_field: (!wc.event_time_field.is_empty()).then(|| wc.event_time_field.clone()),
        hot_windows: wc.has_hot_windows.then_some(wc.hot_windows as usize),
    })
}

/// Build a **windowed** Gateway from a live-CP `GetIndex` response — the gRPC analog of
/// [`gateway_windowed_from_registry`]. One [`WindowNode`](growlerdb_engine::WindowNode) per window
/// (deduped by endpoint, since a node fronts many windows on one channel), tagged with its id + the
/// event-time zone-map so a time-filtered search prunes to overlapping windows before scattering.
/// Not hot-reloaded yet (the window set is static under today's single-process serve); dynamic-window
/// reload is a follow-up (needs a windowed swap on the Gateway).
/// (window, primary endpoint) pairs identifying a windowed topology — the windowed analog of
/// [`RoutingFingerprint`], so the reloader logs only when the window→node set changes.
type WindowFingerprint = Vec<(i64, String)>;

/// The windowed routing resolved from a live-CP `GetIndex`: one [`WindowNode`] per window (deduped by
/// endpoint), the windowing config, the per-window zone-map descriptors, and the fingerprint.
type CpWindowedRouting = (
    Vec<std::sync::Arc<dyn growlerdb_engine::Node>>,
    growlerdb_core::TimeWindowing,
    Vec<(i64, Option<(i64, i64)>)>,
    WindowFingerprint,
);

/// Resolve a windowed index's routing from a live-CP `GetIndex` response: connect one
/// [`WindowNode`] per window (deduped by endpoint — a node fronts many windows on one channel), the
/// windowing config + per-window event-time zone-maps for pruning, and the topology fingerprint.
/// Shared by the startup build and the hot-reload loop (so a window created at runtime is picked up).
async fn resolve_windowed_routing_cp(
    index: &str,
    resp: &growlerdb_proto::v1::GetIndexResponse,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<CpWindowedRouting> {
    use std::collections::HashMap;
    use std::sync::Arc;
    let wc = resp
        .windowing
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("`{index}`: GetIndex carried no windowing config"))?;
    let windowing = windowing_from_get_index(index, wc)?;
    let windows: Vec<&growlerdb_proto::v1::ShardStatus> =
        resp.shard_status.iter().filter(|s| s.window != 0).collect();
    if windows.is_empty() {
        anyhow::bail!("windowed index `{index}` has no assigned windows yet");
    }
    let mut conns: HashMap<String, growlerdb_engine::RemoteNode> = HashMap::new();
    let mut nodes: Vec<Arc<dyn growlerdb_engine::Node>> = Vec::with_capacity(windows.len());
    let mut descriptors = Vec::with_capacity(windows.len());
    for s in &windows {
        if s.primary.is_empty() {
            anyhow::bail!(
                "window {} of `{index}` has no assigned primary yet",
                s.window
            );
        }
        let remote = match conns.get(&s.primary) {
            Some(r) => r.clone(),
            None => {
                let r = connect_node(&s.primary, node_tls.clone())
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("window {} primary `{}`: {e}", s.window, s.primary)
                    })?;
                conns.insert(s.primary.clone(), r.clone());
                r
            }
        };
        nodes.push(growlerdb_engine::WindowNode::new(Arc::new(remote), s.window).shared());
        descriptors.push((
            s.window,
            s.has_event_bounds.then_some((s.event_min, s.event_max)),
        ));
    }
    let mut fingerprint: WindowFingerprint = windows
        .iter()
        .map(|s| (s.window, s.primary.clone()))
        .collect();
    fingerprint.sort();
    Ok((nodes, windowing, descriptors, fingerprint))
}

/// Build a windowed [`Gateway`] + its [`WindowFingerprint`] from a live-CP `GetIndex`.
async fn windowed_gateway_from_get_index(
    index: &str,
    resp: &growlerdb_proto::v1::GetIndexResponse,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<(growlerdb_engine::Gateway, WindowFingerprint)> {
    let (nodes, windowing, descriptors, fp) =
        resolve_windowed_routing_cp(index, resp, node_tls).await?;
    Ok((
        growlerdb_engine::Gateway::windowed(nodes, windowing, descriptors),
        fp,
    ))
}

/// Build a sharded [`Gateway`](growlerdb_engine::Gateway) for `index` from the live Control-Plane at
/// `cp`. Bounded startup wait: nodes may still be registering their shards when the gateway boots
/// (the Kubernetes start race), so retry until every shard has a primary rather than crash-looping.
/// Returns the gateway and the startup [`RoutingFingerprint`] (seeds the reloader so it doesn't
/// redundantly re-apply the same topology on its first tick).
/// What hot-reload a live-CP gateway needs: an **ordinal** shard topology (reshard/primary
/// moves) vs a **windowed** window set (windows created/placed at runtime). Both poll `GetIndex`, but
/// swap differently — [`swap_routing`](growlerdb_engine::Gateway::swap_routing) vs
/// [`swap_windowed`](growlerdb_engine::Gateway::swap_windowed).
enum CpReload {
    Ordinal(RoutingFingerprint),
    Windowed(WindowFingerprint),
}

async fn gateway_from_control_plane(
    cp: &str,
    index: &str,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
) -> (growlerdb_engine::Gateway, CpReload) {
    // One build attempt: connect to the control plane, then resolve the index's topology. BOTH a
    // connection refusal (the CP pod isn't up yet) and shards-not-yet-registered (the
    // Kubernetes start race) are transient at boot, so retry_until_ok waits it out rather
    // than exit(1) → CrashLoopBackOff. The caller serves /healthz + a not-ready /readyz meanwhile.
    // A **windowed** index builds a window-pruning gateway (hot-reloaded via swap_windowed
    // so runtime-created windows are picked up); an ordinal index returns its routing fingerprint.
    let attempt = || {
        let node_tls = node_tls.clone();
        async move {
            let mut client = connect_cp(cp, false).await?;
            let resp = client
                .get_index(growlerdb_proto::v1::GetIndexRequest {
                    name: index.to_string(),
                })
                .await
                .map_err(|e| anyhow::anyhow!("control-plane GetIndex(`{index}`): {}", e.message()))?
                .into_inner();
            if resp.windowing.is_some() {
                let (gw, fp) = windowed_gateway_from_get_index(index, &resp, node_tls).await?;
                Ok::<_, anyhow::Error>((gw, CpReload::Windowed(fp)))
            } else {
                let (nodes, router, fp) = connect_sharded_from_get_index(index, &resp, node_tls)?;
                Ok((
                    growlerdb_engine::Gateway::sharded_with(nodes, router),
                    CpReload::Ordinal(fp),
                ))
            }
        }
    };
    let mut warned = false;
    retry_until_ok(
        attempt,
        std::time::Duration::from_secs(GW_CP_STARTUP_INTERVAL_SECS),
        |n, e| {
            if !warned {
                eprintln!(
                    "gateway: waiting for `{index}` via control-plane {cp} ({e}); retrying until \
                     reachable — up but /readyz not-ready"
                );
                warned = true;
            } else if n % 6 == 0 {
                eprintln!("gateway: still waiting for control-plane {cp} (attempt {n})");
            }
        },
    )
    .await
}

/// Wait (unboundedly) until the control-plane at `cp` accepts a gRPC connection, so a
/// `--all-indexes` gateway rolled alongside the control-plane stays up (not-ready) rather than
/// crash-looping. Multi-index readiness is CP reachability, *not* any one index resolving.
async fn wait_for_control_plane(cp: &str) {
    let mut warned = false;
    retry_until_ok(
        || async {
            connect_cp(cp, false).await.map(|_| ())
        },
        std::time::Duration::from_secs(GW_CP_STARTUP_INTERVAL_SECS),
        |n, e| {
            if !warned {
                eprintln!(
                    "gateway: waiting for control-plane {cp} ({e}); retrying — up but /readyz not-ready"
                );
                warned = true;
            } else if n % 6 == 0 {
                eprintln!("gateway: still waiting for control-plane {cp} (attempt {n})");
            }
        },
    )
    .await
}

/// A [`RouteResolver`](growlerdb_engine::RouteResolver) that resolves a named index into an
/// [`IndexRoute`](growlerdb_engine::IndexRoute) from the live control-plane: fetch its `GetIndex`,
/// connect a Node per shard (ordinal or windowed), and — if
/// `reload_secs > 0` — spawn a per-index hot-reloader so a reshard / new window is picked up with no
/// restart. Closes over the CP endpoint + node TLS so one resolver serves every index the gateway
/// fronts.
struct CpRouteResolver {
    cp: String,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
    reload_secs: u64,
}

#[tonic::async_trait]
impl growlerdb_engine::RouteResolver for CpRouteResolver {
    async fn resolve(
        &self,
        index: &str,
    ) -> Result<Option<std::sync::Arc<growlerdb_engine::IndexRoute>>, String> {
        let mut client = connect_cp(&self.cp, false)
            .await
            .map_err(|e| e.to_string())?;
        let resp = match client
            .get_index(growlerdb_proto::v1::GetIndexRequest {
                name: index.to_string(),
            })
            .await
        {
            Ok(r) => r.into_inner(),
            // A NOT_FOUND is "no such index" (→ Ok(None), negative-cached by the Gateway); any other
            // status is a transient failure the Gateway surfaces as Unavailable (retried next request).
            Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
            Err(status) => {
                return Err(format!("GetIndex(`{index}`): {}", status.message()));
            }
        };

        if resp.windowing.is_some() {
            let (nodes, windowing, descriptors, _fp) =
                resolve_windowed_routing_cp(index, &resp, self.node_tls.clone())
                    .await
                    .map_err(|e| e.to_string())?;
            let route = growlerdb_engine::IndexRoute::new(
                nodes,
                growlerdb_core::ShardRouter::hashed(descriptors.len().max(1) as u32),
                Some(growlerdb_engine::WindowRouting::new(windowing, descriptors)),
                Vec::new(),
            );
            if self.reload_secs > 0 {
                spawn_index_route_reloader(
                    route.clone(),
                    self.cp.clone(),
                    index.to_string(),
                    self.node_tls.clone(),
                    self.reload_secs,
                    true,
                );
            }
            Ok(Some(route))
        } else {
            let (nodes, router, _fp) =
                connect_sharded_from_get_index(index, &resp, self.node_tls.clone())
                    .map_err(|e| e.to_string())?;
            // The live-CP path carries no partition-field pruning hints (as the single-index live-CP
            // gateway also doesn't) — correct, just fans out instead of pruning.
            let route = growlerdb_engine::IndexRoute::new(nodes, router, None, Vec::new());
            if self.reload_secs > 0 {
                spawn_index_route_reloader(
                    route.clone(),
                    self.cp.clone(),
                    index.to_string(),
                    self.node_tls.clone(),
                    self.reload_secs,
                    false,
                );
            }
            Ok(Some(route))
        }
    }
}

/// Poll the control-plane every `secs` and **hot-reload** one multi-index route's topology:
/// the per-index analog of [`spawn_control_plane_reloader`], but swapping an
/// [`IndexRoute`](growlerdb_engine::IndexRoute) in place rather than the whole gateway. `windowed`
/// selects the swap kind (windows vs ordinal shards). A read error keeps the current topology (an
/// outage must not blank a route).
fn spawn_index_route_reloader(
    route: std::sync::Arc<growlerdb_engine::IndexRoute>,
    cp: String,
    index: String,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
    secs: u64,
    windowed: bool,
) {
    tokio::spawn(async move {
        let mut client: Option<growlerdb_proto::service_token::CpClient> = None;
        tokio::time::sleep(jittered(std::time::Duration::from_secs(secs), 0.5)).await;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        tick.tick().await; // the immediate first tick is the startup state
        loop {
            tick.tick().await;
            if client.is_none() {
                match connect_cp(&cp, false).await {
                    Ok(c) => client = Some(c),
                    Err(e) => {
                        eprintln!("gateway: route reloader reconnect to {cp} failed: {e}");
                        growlerdb_telemetry::sli::background_failure("route-reload");
                        continue;
                    }
                }
            }
            let c = client.as_mut().expect("client present");
            let resp = match c
                .get_index(growlerdb_proto::v1::GetIndexRequest {
                    name: index.clone(),
                })
                .await
            {
                Ok(r) => r.into_inner(),
                Err(e) => {
                    eprintln!(
                        "gateway: GetIndex(`{index}`) failed (keeping current route): {}",
                        e.message()
                    );
                    growlerdb_telemetry::sli::background_failure("route-reload");
                    client = None;
                    continue;
                }
            };
            if windowed {
                match resolve_windowed_routing_cp(&index, &resp, node_tls.clone()).await {
                    Ok((nodes, windowing, descriptors, _fp)) => {
                        route.swap_windowed(nodes, windowing, descriptors);
                        growlerdb_telemetry::sli::background_success("route-reload");
                    }
                    Err(e) => {
                        eprintln!("gateway: windowed route read failed (keeping current): {e}");
                        growlerdb_telemetry::sli::background_failure("route-reload");
                    }
                }
            } else {
                match connect_sharded_from_get_index(&index, &resp, node_tls.clone()) {
                    Ok((nodes, router, _fp)) => {
                        route.swap(nodes, router);
                        growlerdb_telemetry::sli::background_success("route-reload");
                    }
                    Err(e) => {
                        eprintln!("gateway: route topology read failed (keeping current): {e}");
                        growlerdb_telemetry::sli::background_failure("route-reload");
                    }
                }
            }
        }
    });
}

/// Retry `attempt` with a fixed `interval` backoff **until it succeeds**, returning its value;
/// `on_error(attempt_number, err)` runs on each failure (logging). Unbounded by design:
/// a gateway waiting for a not-yet-reachable control-plane at boot must stay up (not-ready), not
/// exit → CrashLoopBackOff. Pure control flow — unit-tested with a closure that fails then succeeds.
async fn retry_until_ok<T, F, Fut>(
    attempt: F,
    interval: std::time::Duration,
    mut on_error: impl FnMut(u32, &anyhow::Error),
) -> T
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let mut n = 0u32;
    loop {
        n += 1;
        match attempt().await {
            Ok(v) => return v,
            Err(e) => {
                on_error(n, &e);
                tokio::time::sleep(interval).await;
            }
        }
    }
}

/// Connect to the control plane over gRPC, retrying with backoff until it's reachable —
/// used for the REST index-management proxy so a gateway rolled alongside the control-plane waits
/// rather than exiting. (The routing build already waited for the CP, so this normally connects on
/// the first try; the retry only covers a CP blip in between.)
async fn connect_cp_with_retry(cp: &str) -> growlerdb_proto::service_token::CpClient {
    loop {
        match connect_cp(cp, false).await {
            Ok(c) => return c,
            Err(e) => {
                eprintln!("gateway: waiting for control plane `{cp}` (index management): {e}");
                tokio::time::sleep(std::time::Duration::from_secs(GW_CP_STARTUP_INTERVAL_SECS))
                    .await;
            }
        }
    }
}

/// Poll the live Control-Plane every `secs` and **hot-reload** the gateway's topology on change
/// (distributed): after a reshard cutover — or a shard primary moving to a new node — the
/// running gateway picks up the new shard set + bucket map with no restart. The gRPC analog of
/// [`spawn_registry_reloader`]. A read error keeps the current topology (a control-plane blip must
/// not blank the gateway). `startup_fp` seeds `last` so the first tick is a no-op if nothing changed.
fn spawn_control_plane_reloader(
    gateway: std::sync::Arc<growlerdb_engine::Gateway>,
    cp: String,
    index: String,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
    secs: u64,
    startup_fp: RoutingFingerprint,
) {
    tokio::spawn(async move {
        // Connect lazily and reconnect inside the loop: a single connect blip must
        // NOT end the reloader forever — otherwise topology freezes after one transient CP outage.
        let mut client: Option<growlerdb_proto::service_token::CpClient> = None;
        let mut last: Option<RoutingFingerprint> = Some(startup_fp);
        // One-time phase offset so fleet-wide gateways don't all poll the CP on the same tick.
        tokio::time::sleep(jittered(std::time::Duration::from_secs(secs), 0.5)).await;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        tick.tick().await; // immediate first tick is the startup state
        loop {
            tick.tick().await;
            // (Re)establish the client if we don't have one; a failure just retries next tick.
            if client.is_none() {
                match connect_cp(&cp, false).await {
                    Ok(c) => client = Some(c),
                    Err(e) => {
                        eprintln!("gateway: control-plane reloader reconnect to {cp} failed: {e}");
                        growlerdb_telemetry::sli::background_failure("cp-reload");
                        continue;
                    }
                }
            }
            let c = client.as_mut().expect("client present");
            match resolve_sharded_routing_cp(c, &index, node_tls.clone()).await {
                // Swap in a freshly-built routing **every** tick, not just on a topology change
                // the node channels are lazy, so rebuilding is cheap and — crucially — it
                // re-resolves each shard's DNS, so a shard pod that crashed and returned at a new IP
                // is reconnected within one interval with no manual gateway restart. The connect now
                // can't fail on a down shard (lazy), so a partially-down cluster still serves (partial)
                // and self-heals. Log only when the topology fingerprint actually changes.
                Ok((nodes, router, fp)) => {
                    gateway.swap_routing(nodes, router);
                    if Some(&fp) != last.as_ref() {
                        eprintln!(
                            "gateway: hot-reloaded `{index}` topology ({} shards) from {cp}",
                            gateway.shard_count()
                        );
                        last = Some(fp);
                    }
                    growlerdb_telemetry::sli::background_success("cp-reload");
                }
                Err(e) => {
                    eprintln!("gateway: control-plane topology read failed (keeping current): {e}");
                    growlerdb_telemetry::sli::background_failure("cp-reload");
                    // Drop the client so the next tick reconnects (the CP may have moved/restarted).
                    client = None;
                }
            }
        }
    });
}

/// The windowed analog of [`spawn_control_plane_reloader`]: poll `GetIndex` and
/// [`swap_windowed`](growlerdb_engine::Gateway::swap_windowed) so the cluster gateway picks up windows
/// **created/placed at runtime** — the temporal workload's timeline advances continuously, so new
/// windows must become queryable through the gateway with no restart. A read error keeps the current
/// window set; `startup_fp` seeds `last` so the first tick logs only on a real change.
fn spawn_windowed_control_plane_reloader(
    gateway: std::sync::Arc<growlerdb_engine::Gateway>,
    cp: String,
    index: String,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
    secs: u64,
    startup_fp: WindowFingerprint,
) {
    tokio::spawn(async move {
        let mut client: Option<growlerdb_proto::service_token::CpClient> = None;
        let mut last: Option<WindowFingerprint> = Some(startup_fp);
        tokio::time::sleep(jittered(std::time::Duration::from_secs(secs), 0.5)).await;
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        tick.tick().await; // immediate first tick is the startup state
        loop {
            tick.tick().await;
            if client.is_none() {
                match connect_cp(&cp, false).await {
                    Ok(c) => client = Some(c),
                    Err(e) => {
                        eprintln!("gateway: windowed reloader reconnect to {cp} failed: {e}");
                        growlerdb_telemetry::sli::background_failure("cp-reload-windowed");
                        continue;
                    }
                }
            }
            let c = client.as_mut().expect("client present");
            let resp = match c
                .get_index(growlerdb_proto::v1::GetIndexRequest {
                    name: index.clone(),
                })
                .await
            {
                Ok(r) => r.into_inner(),
                Err(e) => {
                    eprintln!(
                        "gateway: windowed GetIndex(`{index}`) failed (keeping current): {}",
                        e.message()
                    );
                    growlerdb_telemetry::sli::background_failure("cp-reload-windowed");
                    client = None; // reconnect next tick
                    continue;
                }
            };
            match resolve_windowed_routing_cp(&index, &resp, node_tls.clone()).await {
                Ok((nodes, windowing, descriptors, fp)) => {
                    gateway.swap_windowed(nodes, windowing, descriptors);
                    if Some(&fp) != last.as_ref() {
                        eprintln!(
                            "gateway: hot-reloaded `{index}` windows ({} live) from {cp}",
                            gateway.shard_count()
                        );
                        last = Some(fp);
                    }
                    growlerdb_telemetry::sli::background_success("cp-reload-windowed");
                }
                Err(e) => {
                    // A transient "no windows yet" during bring-up keeps the current set (no blank).
                    eprintln!("gateway: windowed topology read failed (keeping current): {e}");
                    growlerdb_telemetry::sli::background_failure("cp-reload-windowed");
                }
            }
        }
    });
}

/// Build a **windowed** Gateway for `name` from the registry window map: one
/// [`WindowNode`](growlerdb_engine::WindowNode) per assigned window, each over a `RemoteNode` to its
/// serving node's endpoint and tagged with its window id, plus the per-window event-time zone-map so
/// the Gateway prunes a time-filtered search to the overlapping windows before scattering. Remote
/// connections are deduped by endpoint — a windowed index is typically one process fronting all its
/// windows, so many windows share one channel.
async fn gateway_windowed_from_registry(
    registry: &growlerdb_controlplane::Registry,
    name: &str,
    windowing: growlerdb_core::TimeWindowing,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<growlerdb_engine::Gateway> {
    use std::collections::HashMap;
    use std::sync::Arc;

    let window_map = registry.window_map(name).ok_or_else(|| {
        anyhow::anyhow!("windowed index `{name}` has no window map in the registry")
    })?;
    if window_map.is_empty() {
        anyhow::bail!("windowed index `{name}` has no assigned windows yet");
    }

    let mut conns: HashMap<String, growlerdb_engine::RemoteNode> = HashMap::new();
    let mut nodes: Vec<Arc<dyn growlerdb_engine::Node>> = Vec::with_capacity(window_map.len());
    let mut descriptors = Vec::with_capacity(window_map.len());
    for (w, wa) in &window_map {
        let endpoint = wa
            .assignment
            .primary
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("window {w} of `{name}` has no assigned primary"))?
            .0
            .clone();
        let remote = match conns.get(&endpoint) {
            Some(r) => r.clone(),
            None => {
                let r = connect_node(&endpoint, node_tls.clone())
                    .await
                    .map_err(|e| anyhow::anyhow!("window {w} primary `{endpoint}`: {e}"))?;
                conns.insert(endpoint, r.clone());
                r
            }
        };
        nodes.push(growlerdb_engine::WindowNode::new(Arc::new(remote), *w).shared());
        descriptors.push((*w, wa.event_min.zip(wa.event_max)));
    }
    Ok(growlerdb_engine::Gateway::windowed(
        nodes,
        windowing,
        descriptors,
    ))
}

/// Resolve `name` — an **index** or an **alias** — to `(members, endpoints)`: the member
/// index names, and the gRPC endpoints to front (each member's shard primaries, in member then
/// ordinal order). A search over the resulting Gateway fans out across every member's shards and
/// merges. Errors if `name` is registered as neither, or a member's shards aren't (fully) assigned.
fn resolve_targets(
    registry: &growlerdb_controlplane::Registry,
    name: &str,
) -> anyhow::Result<(Vec<String>, Vec<String>)> {
    let members = registry.resolve(name);
    if members.is_empty() {
        anyhow::bail!("`{name}` is neither a registered index nor an alias");
    }
    let mut endpoints = Vec::new();
    for member in &members {
        let entry = registry
            .get(member)
            .ok_or_else(|| anyhow::anyhow!("alias member `{member}` is not registered"))?;
        let primaries = growlerdb_engine::shard_primaries(&entry.shards)
            .map_err(|e| anyhow::anyhow!("index `{member}` shard map: {e}"))?;
        endpoints.extend(primaries.into_iter().map(|n| n.0));
    }
    Ok((members, endpoints))
}

/// Connect a [`RemoteNode`] to `endpoint`, over mutual TLS when `tls` is set or
/// plaintext otherwise.
async fn connect_node(
    endpoint: &str,
    tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<growlerdb_engine::RemoteNode> {
    let node = match tls {
        Some(tls) => {
            growlerdb_engine::RemoteNode::connect_with_tls(endpoint.to_string(), tls).await
        }
        None => growlerdb_engine::RemoteNode::connect(endpoint.to_string()).await,
    };
    node.map_err(|e| anyhow::anyhow!("connecting to Node `{endpoint}`: {e}"))
}

/// Lazy variant of [`connect_node`] for sharded routing. Builds the Node channel without
/// dialing now, so (a) a down shard never fails the whole routing build — the Gateway can front a
/// partially-down cluster — and (b) the channel re-resolves DNS on each (re)connect, so a shard pod
/// that crashed and returned at a new IP is reached again, while a still-down shard fails fast at the
/// connect timeout (a `partial` query) instead of hanging on a stale connection.
fn connect_node_lazy(
    endpoint: &str,
    tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<growlerdb_engine::RemoteNode> {
    let node = match tls {
        Some(tls) => growlerdb_engine::RemoteNode::connect_lazy_with_tls(endpoint.to_string(), tls),
        None => growlerdb_engine::RemoteNode::connect_lazy(endpoint.to_string()),
    };
    node.map_err(|e| anyhow::anyhow!("preparing Node `{endpoint}`: {e}"))
}

/// Announce a node-served index to the Control-Plane registry: send its resolved definition (so
/// the control plane needn't re-resolve against the source) + the routable `endpoint` it serves
/// from. Idempotent — safe to call on every node restart.
/// Backoff bounds + heartbeat for control-plane registration.
const REGISTER_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
const REGISTER_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);
const REGISTER_REANNOUNCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Announce a served index to the control-plane registry in the background, **retrying until it
/// succeeds** and then re-announcing on an interval. In Kubernetes all pods start
/// together, so the control plane is routinely not reachable yet at node start; a one-shot
/// best-effort attempt would leave the shard serving but invisible to the gateway forever. The node
/// serves immediately, but `readiness` is marked ready only on the **first** successful registration — so
/// `/readyz` stays not-ready until the node is actually in the registry, the gateway never routes to
/// or waits on a half-joined shard, and a rolling restart re-registers. Re-announcing (an idempotent
/// upsert, control_service.rs) re-points the registry at this node after a control-plane restart.
#[allow(clippy::too_many_arguments)]
fn spawn_registration(
    control_plane: String,
    endpoint: String,
    resolved: growlerdb_core::ResolvedIndex,
    shard_count: u32,
    shard_ordinals: Vec<u32>,
    windows: Vec<growlerdb_proto::v1::ServedWindow>,
    readiness: growlerdb_telemetry::Readiness,
    label: String,
) {
    tokio::spawn(async move {
        registration_loop(
            || {
                // Own a clone per attempt so the returned future is 'static (no borrow of the
                // task's locals), which keeps the generic loop free of lifetime gymnastics.
                let control_plane = control_plane.clone();
                let endpoint = endpoint.clone();
                let resolved = resolved.clone();
                let shard_ordinals = shard_ordinals.clone();
                let windows = windows.clone();
                async move {
                    register_served_index(
                        &control_plane,
                        &endpoint,
                        &resolved,
                        shard_count,
                        shard_ordinals,
                        windows,
                    )
                    .await
                }
            },
            &readiness,
            &label,
            REGISTER_INITIAL_BACKOFF,
            REGISTER_MAX_BACKOFF,
            REGISTER_REANNOUNCE_INTERVAL,
        )
        .await;
    });
}

/// Apply ±`frac` jitter to `base` so fleet-wide loops don't fire in lockstep — in
/// Kubernetes every node starts together, and a synchronized re-announce/reload herd hammers the
/// control plane (each re-announce drives a full-registry rewrite). Uses the sub-second wall clock as
/// a cheap entropy source — no `rand` dependency, and only decorrelation (not unpredictability) is
/// needed. `frac` is clamped so the result never collapses below 10% of `base`.
fn jittered(base: std::time::Duration, frac: f64) -> std::time::Duration {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let unit = (nanos as f64 / 1_000_000_000.0) * 2.0 - 1.0; // ~[-1, 1)
    base.mul_f64((1.0 + unit * frac).max(0.1))
}

/// The retry/heartbeat loop behind [`spawn_registration`], factored out for testing: drive `attempt`
/// with capped exponential backoff until it first succeeds (then mark `readiness` ready and log
/// once), and thereafter re-run it every `reannounce` so a control-plane restart re-learns the node.
/// Failures are logged once until the next success (no per-retry log spam).
async fn registration_loop<F, Fut>(
    attempt: F,
    readiness: &growlerdb_telemetry::Readiness,
    label: &str,
    initial_backoff: std::time::Duration,
    max_backoff: std::time::Duration,
    reannounce: std::time::Duration,
) where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    let mut backoff = initial_backoff;
    let mut registered = false;
    let mut warned = false;
    loop {
        match attempt().await {
            Ok(()) => {
                if !registered {
                    registered = true;
                    println!("serve: registered {label} with the control plane");
                }
                backoff = initial_backoff;
                warned = false;
                readiness.mark_ready();
                // Jitter the heartbeat so a fleet doesn't re-announce in lockstep.
                tokio::time::sleep(jittered(reannounce, 0.2)).await;
            }
            Err(e) => {
                if !warned {
                    eprintln!(
                        "serve: WARNING control-plane registration failed ({e}); retrying until \
                         reachable — {label} serves but is not yet registered (/readyz not-ready)"
                    );
                    warned = true;
                }
                // Jitter the backoff so a CP restart doesn't trigger a synchronized retry storm.
                tokio::time::sleep(jittered(backoff, 0.2)).await;
                backoff = (backoff * 2).min(max_backoff);
            }
        }
    }
}

async fn register_served_index(
    control_plane: &str,
    endpoint: &str,
    resolved: &growlerdb_core::ResolvedIndex,
    shard_count: u32,
    shard_ordinals: Vec<u32>,
    windows: Vec<growlerdb_proto::v1::ServedWindow>,
) -> anyhow::Result<()> {
    let definition_json = serde_json::to_string(resolved)?;
    let mut client = connect_cp(control_plane, false).await?;
    client
        .register_served_index(growlerdb_proto::v1::RegisterServedIndexRequest {
            definition_json,
            endpoint: endpoint.to_string(),
            shard_count, // ignored when `windows` is set (a windowed index reports windows)
            shard_ordinals,
            windows,
        })
        .await
        .map_err(|e| anyhow::anyhow!("registering with control plane: {e}"))?;
    Ok(())
}

/// Heartbeat this windowed node into the control-plane **placement pool** so the CP can
/// place newly-seen windows on it (answering the connector's `ResolveWindowOwner`).
async fn register_node(control_plane: &str, index: &str, endpoint: &str) -> anyhow::Result<()> {
    let mut client = connect_cp(control_plane, false).await?;
    client
        .register_node(growlerdb_proto::v1::RegisterNodeRequest {
            index: index.to_string(),
            endpoint: endpoint.to_string(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("node heartbeat to control plane: {e}"))?;
    Ok(())
}

/// Dynamic windowed registration: on an interval, heartbeat into the placement pool and
/// re-announce the windows this node **currently** serves (+ zone-maps) — so a window created since
/// boot is advertised to the control plane (and thus the cluster gateway), not just the boot set. The
/// windowed counterpart to [`spawn_registration`] (which announces a fixed set).
fn spawn_windowed_registration(
    control_plane: String,
    endpoint: String,
    resolved: growlerdb_core::ResolvedIndex,
    write_service: growlerdb_engine::WindowedWriteService,
    readiness: growlerdb_telemetry::Readiness,
    label: String,
) {
    tokio::spawn(async move {
        registration_loop(
            || {
                let control_plane = control_plane.clone();
                let endpoint = endpoint.clone();
                let resolved = resolved.clone();
                let write_service = write_service.clone();
                async move {
                    // Heartbeat first (keeps the node in the placement pool), then re-announce the
                    // current served windows + their zone-maps for the cluster gateway to prune on.
                    register_node(&control_plane, &resolved.name, &endpoint).await?;
                    register_served_index(
                        &control_plane,
                        &endpoint,
                        &resolved,
                        1,
                        vec![],
                        write_service.served_windows(),
                    )
                    .await
                }
            },
            &readiness,
            &label,
            REGISTER_INITIAL_BACKOFF,
            REGISTER_MAX_BACKOFF,
            REGISTER_REANNOUNCE_INTERVAL,
        )
        .await;
    });
}

/// Seed the built-in users on first closed-mode / login boot. Idempotent:
/// - the **admin** (role `admin`) is seeded only if the registry has NO credentials yet, with the
///   supplied password or a generated one printed once;
/// - a **demo** user is seeded when `GROWLERDB_DEMO_USER` is set (the `just stack` demo) and it
///   doesn't already exist — a well-known, index-scoped credential so the walkthrough SHOWS login +
///   per-index RBAC, not open access. Roles `reader, operator` (query + read metadata +
///   ops read; NOT admin/write) and an `indexes` allowlist (default `docs,catalog`) that the minted
///   session JWT carries so the gateway restricts the demo user to exactly those indexes.
///
/// Shared by the `--builtin-auth` (closed) and `--login-secret` (demo, login-only) control-plane
/// modes so both establish the same accounts.
fn seed_builtin_users(
    registry: &growlerdb_controlplane::Registry,
    admin_user: &str,
    admin_password: Option<String>,
) -> anyhow::Result<()> {
    if !registry.has_credentials() {
        let password = admin_password.unwrap_or_else(|| {
            // No password supplied → generate a strong one and print it ONCE.
            let p = growlerdb_engine::mint_api_token().0;
            println!("control plane: seeded admin `{admin_user}` with a generated password:");
            println!("    {p}");
            println!(
                "control plane: (set --admin-password / GROWLERDB_ADMIN_PASSWORD to choose one)"
            );
            p
        });
        registry
            .set_credential(admin_user, &password)
            .map_err(|e| anyhow::anyhow!("seeding admin credential: {e}"))?;
        registry
            .set_user_roles(admin_user, vec!["admin".to_string()])
            .map_err(|e| anyhow::anyhow!("seeding admin roles: {e}"))?;
        println!("control plane: seeded built-in admin user `{admin_user}` (role: admin)");
    }
    if let Ok(demo_user) = std::env::var("GROWLERDB_DEMO_USER") {
        let demo_user = demo_user.trim().to_string();
        if !demo_user.is_empty() && !registry.has_credential(&demo_user) {
            let demo_password =
                std::env::var("GROWLERDB_DEMO_PASSWORD").unwrap_or_else(|_| "demo".to_string());
            let demo_roles = vec!["reader".to_string(), "operator".to_string()];
            let demo_indexes: Vec<String> = std::env::var("GROWLERDB_DEMO_INDEXES")
                .unwrap_or_else(|_| "docs,catalog".to_string())
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            registry
                .set_credential(&demo_user, &demo_password)
                .map_err(|e| anyhow::anyhow!("seeding demo credential: {e}"))?;
            registry
                .set_user_roles(&demo_user, demo_roles.clone())
                .map_err(|e| anyhow::anyhow!("seeding demo roles: {e}"))?;
            registry
                .set_user_indexes(&demo_user, demo_indexes.clone())
                .map_err(|e| anyhow::anyhow!("seeding demo index scope: {e}"))?;
            println!(
                "control plane: seeded demo user `{demo_user}` (roles: {}; indexes: {})",
                demo_roles.join(", "),
                demo_indexes.join(", ")
            );
        }
    }
    Ok(())
}

/// Run the Control Plane: serve the index registry (create / drop / list) over gRPC,
/// persisted at `{data_dir}/registry.json`.
#[allow(clippy::too_many_arguments)]
async fn control_plane(
    data_dir: &str,
    addr: &str,
    metrics_addr: Option<&str>,
    oidc_issuer: Option<String>,
    oidc_audience: Option<String>,
    builtin_auth: bool,
    login_secret: bool,
    auth_secret: Option<String>,
    admin_user: String,
    admin_password: Option<String>,
    service_token: Option<String>,
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> anyhow::Result<()> {
    use std::sync::Arc;
    use tonic::transport::Server;

    let socket: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --addr `{addr}`: {e}"))?;
    let registry_path = std::path::Path::new(data_dir).join("registry.json");
    std::fs::create_dir_all(data_dir)?;
    let registry = Arc::new(growlerdb_controlplane::Registry::open(&registry_path)?);

    // Optional scale-limit license from GROWLERDB_LICENSE (a signed entitlement). An invalid one
    // warns and falls back to the free tier rather than failing startup.
    let license = match std::env::var("GROWLERDB_LICENSE") {
        Ok(token) if !token.trim().is_empty() => {
            match growlerdb_engine::License::verify(token.trim()) {
                Ok(lic) => {
                    println!(
                        "control plane: Enterprise license for `{}` — node limit {}",
                        lic.licensee, lic.max_nodes
                    );
                    Some(lic)
                }
                Err(e) => {
                    eprintln!("control plane: WARNING ignoring invalid GROWLERDB_LICENSE ({e}); using the free tier");
                    None
                }
            }
        }
        _ => None,
    };

    // With OIDC, the control plane validates bearers itself and enforces RBAC — so
    // admin-gated user management is real, and local role bindings merge against a verified subject.
    let svc = if let Some(issuer) = oidc_issuer {
        let audience = oidc_audience
            .ok_or_else(|| anyhow::anyhow!("--oidc-audience is required with --oidc-issuer"))?;
        let jwks = Arc::new(growlerdb_engine::JwksAuthenticator::for_issuer(
            &issuer, &audience,
        ));
        jwks.refresh()
            .await
            .map_err(|e| anyhow::anyhow!("fetching OIDC keys from `{issuer}`: {e}"))?;
        spawn_jwks_refresher(jwks.clone());
        // Accept OIDC bearers *and* API tokens: `Bearer …` → JWKS, `ApiKey …` → the
        // registry's tokens (a revoked token fails immediately).
        let tokens = Arc::new(growlerdb_engine::RegistryTokenAuthenticator::new(
            registry.clone(),
        ));
        let chain = Arc::new(
            growlerdb_engine::ChainAuthenticator::new()
                .with_bearer(jwks)
                .with_api_keys(tokens),
        );
        println!(
            "control plane: OIDC/JWT + API tokens + RBAC enabled (issuer `{issuer}`, aud `{audience}`)"
        );
        growlerdb_engine::ControlPlaneService::with_auth(
            registry,
            IcebergConfig::from_env(),
            Arc::new(growlerdb_engine::RbacPolicy::with_default_roles()),
        )
        .with_authn(chain)
    } else if builtin_auth {
        // Built-in (no external IdP) closed mode: the /v1/login RPC mints session JWTs
        // from the registry credential store; the control plane validates them (+ API tokens) and
        // enforces RBAC. Seed the built-in users on first boot so the deployment is reachable.
        let secret = auth_secret
            .ok_or_else(|| anyhow::anyhow!("--auth-secret is required with --builtin-auth"))?;
        seed_builtin_users(&registry, &admin_user, admin_password)?;
        let tokens = Arc::new(growlerdb_engine::RegistryTokenAuthenticator::new(
            registry.clone(),
        ));
        let chain = Arc::new(
            growlerdb_engine::ChainAuthenticator::new()
                .with_bearer(Arc::new(
                    growlerdb_engine::JwtAuthenticator::from_hs256_secret(
                        secret.as_bytes(),
                        growlerdb_engine::BUILTIN_SESSION_ISSUER,
                        growlerdb_engine::BUILTIN_SESSION_AUDIENCE,
                    ),
                ))
                .with_api_keys(tokens),
        );
        println!("control plane: built-in password auth + API tokens + RBAC enabled");
        growlerdb_engine::ControlPlaneService::with_auth(
            registry,
            IcebergConfig::from_env(),
            Arc::new(growlerdb_engine::RbacPolicy::with_default_roles()),
        )
        .with_authn(chain)
        .with_session_secret(secret.into_bytes())
    } else if login_secret {
        // Login-only mode (the `just stack` demo): mint session JWTs via `/v1/login` and
        // seed the built-in users, but leave the control plane's OWN authorization **open**. The
        // enforcement point is the gateway (`--builtin-auth`) on the public data plane; the control
        // plane stays reachable for the internal node/gateway RPCs (registration, shard-map reads)
        // that carry no service credential. This does NOT gate the control plane — it only turns on
        // token minting — so it is not a regression from the open control plane, just login on top.
        let secret = auth_secret
            .ok_or_else(|| anyhow::anyhow!("--auth-secret is required with --login-secret"))?;
        seed_builtin_users(&registry, &admin_user, admin_password)?;
        println!(
            "control plane: login enabled (/v1/login mints session JWTs) — authorization OPEN \
             (enforcement is at the gateway); internal registration stays reachable"
        );
        growlerdb_engine::ControlPlaneService::new(registry, IcebergConfig::from_env())
            .with_session_secret(secret.into_bytes())
    } else {
        eprintln!("control plane: WARNING authorization disabled (no --oidc-issuer / --builtin-auth); it is open");
        growlerdb_engine::ControlPlaneService::new(registry, IcebergConfig::from_env())
    };
    let svc = svc.with_license(license);

    println!(
        "control plane: registry on {socket} (registry at {})",
        registry_path.display()
    );
    // Service-credential gate: closes the internal RPCs (registration, shard-map reads, placement)
    // to callers outside the mesh, independent of user auth — so `--login-secret` (open user-auth)
    // no longer leaves the internal RPCs reachable without a credential. Unset ⇒ open (bare dev).
    match &service_token {
        Some(t) if !t.is_empty() => {
            println!(
                "control plane: service-token gate ON (internal RPCs require the shared token)"
            )
        }
        _ => eprintln!(
            "control plane: WARNING no --service-token — internal RPCs are open (set \
             GROWLERDB_SERVICE_TOKEN to close them)"
        ),
    }
    if tls.is_some() {
        println!("control plane: serving over TLS");
    }
    // Keep the ingestion-lag + shard-availability gauges fresh for Prometheus regardless of console
    // polling.
    svc.spawn_ingestion_metrics_sampler(15);
    // Registry opened → ready.
    let readiness = spawn_health(metrics_addr).await?;
    readiness.mark_ready();
    let service = growlerdb_engine::intercept_service_token(svc.into_server(), service_token);
    let mut builder = Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
    }
    builder
        .add_service(service)
        .serve_with_shutdown(socket, async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    println!("growlerdb control-plane: shut down cleanly");
    Ok(())
}

/// Load a shard's persisted resolved definition (`<data_dir>/<index>/index.json`), falling back
/// to the last-known-good `.prev` copy if the live file is corrupt.
fn load_resolved(data_dir: &str, index: &str) -> anyhow::Result<growlerdb_core::ResolvedIndex> {
    let def_path = std::path::Path::new(data_dir)
        .join(index)
        .join("index.json");
    let bytes = std::fs::read(&def_path)
        .map_err(|_| anyhow::anyhow!("index `{index}` not found — run `growlerdb index` first"))?;
    match serde_json::from_slice(&bytes) {
        Ok(r) => Ok(r),
        Err(e) => {
            let prev = growlerdb_core::durable::prev_path(&def_path);
            if prev.exists() {
                Ok(serde_json::from_slice(&std::fs::read(&prev)?)?)
            } else {
                Err(e.into())
            }
        }
    }
}

/// Build the backup object-store config from the environment: the bucket from
/// `GROWLERDB_BACKUP_BUCKET` and credentials/endpoint from the same `GROWLERDB_S3_*` the source
/// uses (set the endpoint for MinIO; leave it unset for AWS S3).
fn backup_s3_config() -> anyhow::Result<growlerdb_backup::S3Config> {
    let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
    let bucket = var("GROWLERDB_BACKUP_BUCKET").ok_or_else(|| {
        anyhow::anyhow!("set GROWLERDB_BACKUP_BUCKET to the object-store bucket for backups")
    })?;
    Ok(growlerdb_backup::S3Config {
        bucket,
        region: var("GROWLERDB_S3_REGION").unwrap_or_else(|| "us-east-1".to_string()),
        endpoint: var("GROWLERDB_S3_ENDPOINT"),
        access_key_id: var("GROWLERDB_S3_ACCESS_KEY").unwrap_or_default(),
        secret_access_key: var("GROWLERDB_S3_SECRET_KEY").unwrap_or_default(),
    })
}

/// Local byte-range cache size for read-through cold windows, from
/// `GROWLERDB_COLD_CACHE_BYTES` (default 1 GiB). One cache is shared across all cold windows.
fn cold_cache_bytes() -> usize {
    std::env::var("GROWLERDB_COLD_CACHE_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1024 * 1024 * 1024)
}

/// How often a windowed node's background **park** loop demotes aged windows to cold read-through,
/// from `GROWLERDB_PARK_INTERVAL_SECS` (0 or unset = disabled — opt-in, like the reconcile backstop).
/// When enabled, `GROWLERDB_BACKUP_BUCKET` must be set (parking writes the cold bytes there); the
/// serve path errors at startup rather than silently no-op'ing. The `hot_windows` policy in the index
/// definition decides how many recent windows stay hot.
fn park_interval_secs() -> u64 {
    std::env::var("GROWLERDB_PARK_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

/// Back up an index's shard to object storage.
async fn backup_cmd(data_dir: &str, index: &str, prefix: Option<&str>) -> anyhow::Result<()> {
    use growlerdb_index::{LocalIndexStore, ShardId};
    let resolved = load_resolved(data_dir, index)?;
    let store_local = LocalIndexStore::open(data_dir)?;
    let shard = store_local.open_shard(&ShardId::single(index), &resolved)?;
    let store = growlerdb_backup::s3_store(&backup_s3_config()?)?;
    let prefix = prefix
        .map(str::to_string)
        .unwrap_or_else(|| format!("backups/{index}"));
    // Staging on the same filesystem as the shard → segment files hard-link (instant).
    let staging = std::path::Path::new(data_dir).join(format!(".backup-staging-{index}"));
    // The index definition lives at the index root (not the shard dir), so pass it for the manifest.
    let def_json = serde_json::to_string(&resolved)?;
    let m = growlerdb_backup::backup(
        &shard,
        index,
        index,
        &staging,
        &store,
        &prefix,
        Some(def_json),
    )
    .await?;
    println!(
        "backed up `{index}` snapshot {} ({} files) → bucket `{}` prefix `{prefix}`",
        m.snapshot,
        m.files.len(),
        backup_s3_config()?.bucket,
    );
    Ok(())
}

/// Cold-park windows of a windowed index: keep the most-recent `keep_hot` (or the index's
/// `hot_windows` policy) hot, cold-park the rest to `cold/<index>/w<window>` — evicting the local
/// bulk while keeping each window searchable read-through. `revive` promotes a window back to hot.
async fn park_cmd(data_dir: &str, index: &str, keep_hot: Option<usize>) -> anyhow::Result<()> {
    use growlerdb_index::{LocalIndexStore, ShardId};
    let resolved = load_resolved(data_dir, index)?;
    let windowing = resolved
        .windowing
        .clone()
        .ok_or_else(|| anyhow::anyhow!("`{index}` is not a windowed index — nothing to park"))?;

    let store_local = LocalIndexStore::open(data_dir)?;
    let windows = store_local.window_shards(index)?; // ascending (oldest first)
    let cold = windowing.cold_windows(&windows, keep_hot);
    if cold.is_empty() {
        println!(
            "park `{index}`: nothing to park ({} window(s), keeping {} hot)",
            windows.len(),
            keep_hot.or(windowing.hot_windows).unwrap_or(windows.len()),
        );
        return Ok(());
    }

    let store = growlerdb_backup::s3_store(&backup_s3_config()?)?;
    let def_json = serde_json::to_string(&resolved)?;
    for &w in cold {
        let id = ShardId::window(index, w);
        let shard = store_local.open_shard(&id, &resolved)?;
        let window_dir = store_local.shard_path(&id);
        let prefix = format!("cold/{index}/w{w}");
        let staging = std::path::Path::new(data_dir).join(format!(".cold-staging-{index}-w{w}"));
        // Cold-tier: evict the local bulk but keep the window searchable read-through.
        let marker = growlerdb_backup::cold_park(
            shard,
            index,
            w,
            &window_dir,
            &staging,
            &store,
            &prefix,
            Some(def_json.clone()),
        )
        .await?;
        println!(
            "cold-parked `{index}` window {w} (snapshot {}) → `{}` (still searchable read-through)",
            marker.snapshot, marker.object_prefix
        );
    }
    println!(
        "park `{index}`: cold-parked {} window(s), kept {} hot",
        cold.len(),
        windows.len() - cold.len(),
    );
    Ok(())
}

/// Promote a cold window back to hot: restore its bulk from `cold/<index>/w<window>` into
/// the local window-shard dir and drop the cold marker, so it's served locally again — the inverse
/// of cold-parking. (A cold window is *already* searchable read-through; this is for pre-warming a
/// window expecting heavy traffic.)
async fn revive_cmd(data_dir: &str, index: &str, window: i64) -> anyhow::Result<()> {
    use growlerdb_index::{LocalIndexStore, ShardId};
    let store_local = LocalIndexStore::open(data_dir)?;
    let store = growlerdb_backup::s3_store(&backup_s3_config()?)?;
    let shard_dir = store_local.shard_path(&ShardId::window(index, window));
    let prefix = format!("cold/{index}/w{window}");
    let m = growlerdb_backup::revive(&store, &prefix, &shard_dir)
        .await
        .map_err(|e| anyhow::anyhow!("reviving `{index}` window {window} from `{prefix}`: {e}"))?;
    // Drop the cold marker → the window is hot again (has a local `index/`).
    let _ = std::fs::remove_file(store_local.cold_marker_path(index, window));
    println!(
        "promoted `{index}` window {window} to hot (snapshot {}, {} files) from `{prefix}`",
        m.snapshot,
        m.files.len(),
    );
    Ok(())
}

/// The retention **victims** for a keep-last-N policy: of `names` matching `pattern`,
/// sorted, all **but** the most-recent `keep` (the oldest roll-off). Pure — the CLI applies it to
/// the index list it reads from the control plane.
fn retention_plan(names: &[String], pattern: &str, keep: usize) -> Vec<String> {
    let mut matching: Vec<String> = names
        .iter()
        .filter(|n| growlerdb_controlplane::glob_match(pattern, n))
        .cloned()
        .collect();
    matching.sort();
    matching.truncate(matching.len().saturating_sub(keep));
    matching
}

/// Drop the oldest indexes matching `pattern` beyond `keep`, via the control plane.
async fn retention_cmd(
    control_plane: &str,
    pattern: &str,
    keep: usize,
    dry_run: bool,
) -> anyhow::Result<()> {
    let mut client = connect_cp(control_plane, false).await?;
    let names: Vec<String> = client
        .list_indexes(growlerdb_proto::v1::ListIndexesRequest {})
        .await
        .map_err(|e| anyhow::anyhow!("listing indexes: {e}"))?
        .into_inner()
        .indexes
        .into_iter()
        .map(|s| s.name)
        .collect();

    let victims = retention_plan(&names, pattern, keep);
    if victims.is_empty() {
        let matched = names
            .iter()
            .filter(|n| growlerdb_controlplane::glob_match(pattern, n))
            .count();
        println!("retention `{pattern}`: nothing to drop ({matched} matching, keeping {keep})");
        return Ok(());
    }
    for v in &victims {
        if dry_run {
            println!("retention `{pattern}`: would drop `{v}` (dry-run)");
        } else {
            client
                .drop_index(growlerdb_proto::v1::DropIndexRequest { name: v.clone() })
                .await
                .map_err(|e| anyhow::anyhow!("dropping `{v}`: {e}"))?;
            println!("retention `{pattern}`: dropped `{v}`");
        }
    }
    println!(
        "retention `{pattern}`: {} index(es) {} (kept {keep} most-recent)",
        victims.len(),
        if dry_run {
            "would be dropped"
        } else {
            "dropped"
        },
    );
    Ok(())
}

/// Restore an index's shard from an object-storage backup, or rebuild from Iceberg when there is
/// none. After a backup restore, the connector resumes the tail from the checkpoint.
async fn restore_cmd(
    engine: &Engine,
    data_dir: &str,
    index: &str,
    prefix: Option<&str>,
) -> anyhow::Result<()> {
    use growlerdb_index::{LocalIndexStore, ShardId};
    let store_local = LocalIndexStore::open(data_dir)?;
    let dest = store_local.shard_path(&ShardId::single(index));
    let store = growlerdb_backup::s3_store(&backup_s3_config()?)?;
    let prefix = prefix
        .map(str::to_string)
        .unwrap_or_else(|| format!("backups/{index}"));
    match growlerdb_backup::restore(&store, &prefix, &dest).await {
        Ok(m) => {
            // restore() wrote the shard dir; the definition lives at the index root (one level up
            // from the shard's ordinal dir), so re-materialize it from the manifest.
            if let Some(def) = &m.definition_json {
                let def_path = std::path::Path::new(data_dir)
                    .join(index)
                    .join("index.json");
                growlerdb_core::durable::write(&def_path, def.as_bytes())?;
            }
            println!(
                "restored `{index}` snapshot {} from `{prefix}`; ingestion resumes from the checkpoint",
                m.snapshot
            );
        }
        Err(growlerdb_backup::BackupError::NotFound(_)) => {
            // The definition lives at the index root, not the shard's ordinal dir.
            let def_path = std::path::Path::new(data_dir)
                .join(index)
                .join("index.json");
            if def_path.exists() {
                let out = engine.rebuild(index).await?;
                println!(
                    "no backup at `{prefix}`; rebuilt `{index}` from Iceberg: {} documents at snapshot {}",
                    out.doc_count, out.snapshot.0
                );
            } else {
                anyhow::bail!(
                    "no backup at `{prefix}` and no local definition for `{index}` — \
                     run `growlerdb index <table> --name {index}` to build it from the source"
                );
            }
        }
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

/// Refresh a replica from the primary's backup: incremental segment shipping +
/// re-materialize the definition at the index root, so a subsequent `serve` is a read replica.
async fn refresh_replica_cmd(
    data_dir: &str,
    index: &str,
    prefix: Option<&str>,
) -> anyhow::Result<()> {
    use growlerdb_index::{LocalIndexStore, ShardId};
    let store_local = LocalIndexStore::open(data_dir)?;
    let dest = store_local.shard_path(&ShardId::single(index));
    let store = growlerdb_backup::s3_store(&backup_s3_config()?)?;
    let prefix = prefix
        .map(str::to_string)
        .unwrap_or_else(|| format!("backups/{index}"));
    let stats = growlerdb_backup::refresh(&store, &prefix, &dest).await?;
    if let Some(def) = &stats.manifest.definition_json {
        let def_path = std::path::Path::new(data_dir)
            .join(index)
            .join("index.json");
        growlerdb_core::durable::write(&def_path, def.as_bytes())?;
    }
    println!(
        "replica `{index}` at snapshot {} ({} new, {} reused, {} pruned)",
        stats.manifest.snapshot, stats.downloaded, stats.skipped, stats.removed
    );
    Ok(())
}

/// Print ranked hits (coordinates + score) and, if present, the hydrated rows.
fn print_results(hits: &[growlerdb_core::Hit], rows: Option<&[HydratedRow]>) {
    if hits.is_empty() {
        println!("no hits");
        return;
    }
    println!("{} hit(s):", hits.len());
    for (i, hit) in hits.iter().enumerate() {
        println!("  {:>6.3}  {}", hit.score, render_key(&hit.key));
        if let Some(rows) = rows {
            if let Some(row) = rows.get(i) {
                let mut cols: Vec<String> = row
                    .fields
                    .iter()
                    .map(|(k, v)| format!("{k}={}", render_value(v)))
                    .collect();
                cols.sort();
                println!("          ↳ {}", cols.join("  "));
            }
        }
    }
}

/// Render a composite key as `name=value …` over its partition + identifier.
fn render_key(key: &CompositeKey) -> String {
    key.partition
        .iter()
        .chain(key.identifier.iter())
        .map(|(name, value)| format!("{name}={}", render_value(value)))
        .collect::<Vec<_>>()
        .join(" ")
}

/// Render a value compactly for display.
fn render_value(value: &Value) -> String {
    value.to_index_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use growlerdb_controlplane::Registry;
    use growlerdb_core::{IndexDefinition, RoutingStrategy, SourceField, SourceSchema, SourceType};

    #[test]
    fn jittered_stays_within_bounds() {
        // Jitter must decorrelate without exploding or collapsing the interval.
        let base = std::time::Duration::from_secs(30);
        for _ in 0..1000 {
            let j = jittered(base, 0.2);
            assert!(
                j >= base.mul_f64(0.8) && j <= base.mul_f64(1.2),
                "±20% jitter of 30s stayed in [24s, 36s]: got {j:?}"
            );
        }
        // The 10% floor keeps even an aggressive fraction positive and non-trivial.
        assert!(jittered(base, 5.0) >= base.mul_f64(0.1));
    }

    /// The gateway's startup build must **retry until it succeeds** (CP unreachable / shards
    /// not yet registered) rather than exiting on the first failure → CrashLoopBackOff. Simulates a
    /// dependency down for the first two attempts then up: `retry_until_ok` keeps trying and returns
    /// the value once it succeeds; `on_error` fires once per failure.
    #[tokio::test]
    async fn gateway_startup_retries_until_the_control_plane_is_reachable() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let calls = Arc::new(AtomicUsize::new(0));
        let mut errors = 0u32;
        let calls_in = calls.clone();
        let got = retry_until_ok(
            || {
                let calls = calls_in.clone();
                async move {
                    // Fail the first two attempts (CP/shards not up yet), then succeed with 42.
                    if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                        anyhow::bail!("control plane unreachable")
                    } else {
                        Ok(42u32)
                    }
                }
            },
            Duration::from_millis(1),
            |_n, _e| errors += 1,
        )
        .await;

        assert_eq!(
            got, 42,
            "returns the value once the dependency is reachable"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3, "two failures then success");
        assert_eq!(errors, 2, "on_error fired once per failed attempt");
    }

    /// Registration must retry until the control plane is reachable, and the node must not
    /// report ready until it has registered. Simulates a CP that's unreachable for the first two
    /// attempts then comes up: the loop keeps trying, and once it succeeds readiness flips to ready.
    #[tokio::test]
    async fn registration_retries_until_the_control_plane_is_reachable() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::Duration;

        let attempts = Arc::new(AtomicUsize::new(0));
        let readiness = growlerdb_telemetry::Readiness::new();
        assert!(
            !readiness.is_ready(),
            "a node must not be ready before it has registered"
        );

        let attempts_in = attempts.clone();
        let readiness_in = readiness.clone();
        let handle = tokio::spawn(async move {
            registration_loop(
                || {
                    let attempts = attempts_in.clone();
                    async move {
                        // Fail the first two attempts (CP not up yet), then succeed.
                        if attempts.fetch_add(1, Ordering::SeqCst) < 2 {
                            anyhow::bail!("control plane unreachable")
                        } else {
                            Ok(())
                        }
                    }
                },
                &readiness_in,
                "`docs` (shard 0/3)",
                Duration::from_millis(5),
                Duration::from_millis(20),
                Duration::from_millis(50),
            )
            .await;
        });

        // The loop runs forever (it re-announces); wait until it has registered, then stop it.
        for _ in 0..400 {
            if readiness.is_ready() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        handle.abort();

        assert!(
            readiness.is_ready(),
            "node should be ready once the control plane becomes reachable"
        );
        assert!(
            attempts.load(Ordering::SeqCst) >= 3,
            "should have retried the two failures before the success that registered it"
        );
    }

    /// A `GetIndexResponse` with the given per-ordinal primary endpoints (in arbitrary order to
    /// exercise the sort), `shard_count`, routing strategy, and bucket map — for the CP-routing plan.
    fn get_index_resp(
        shards: &[(u32, &str)],
        shard_count: u32,
        routing: i32,
        bucket_owners: Vec<u32>,
    ) -> growlerdb_proto::v1::GetIndexResponse {
        growlerdb_proto::v1::GetIndexResponse {
            name: "events".into(),
            status: "active".into(),
            shard_count,
            routing,
            bucket_owners,
            shard_status: shards
                .iter()
                .map(|(ord, primary)| growlerdb_proto::v1::ShardStatus {
                    ordinal: *ord,
                    window: 0,
                    primary: primary.to_string(),
                    replicas: vec![],
                    state: "active".into(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    #[test]
    fn cp_routing_plan_orders_primaries_by_ordinal_and_resolves_strategy() {
        // Two shards out of ordinal order → endpoints come back ordered 0,1; default routing = hash.
        let resp = get_index_resp(
            &[(1, "http://n2:50051"), (0, "http://n1:50051")],
            2,
            0,
            vec![],
        );
        let (eps, strategy, owners) = routing_plan_from_get_index("events", &resp).unwrap();
        assert_eq!(eps, vec!["http://n1:50051", "http://n2:50051"]);
        assert_eq!(strategy, RoutingStrategy::Hash);
        assert!(owners.is_empty());

        // ROUTING_PARTITION (1) resolves to partition routing; a bucket map is carried through.
        let resp = get_index_resp(&[(0, "http://n1:50051")], 1, 1, vec![0; 8]);
        let (_eps, strategy, owners) = routing_plan_from_get_index("events", &resp).unwrap();
        assert_eq!(strategy, RoutingStrategy::Partition);
        assert_eq!(owners.len(), 8);
    }

    #[test]
    fn cp_routing_plan_rejects_incomplete_or_unsupported_topologies() {
        // A shard still building (empty primary) → not routable yet.
        let building = get_index_resp(&[(0, "http://n1:50051"), (1, "")], 2, 0, vec![]);
        assert!(routing_plan_from_get_index("events", &building).is_err());

        // shard_count disagrees with the number of placement entries.
        let mismatch = get_index_resp(&[(0, "http://n1:50051")], 2, 0, vec![]);
        assert!(routing_plan_from_get_index("events", &mismatch).is_err());

        // Non-contiguous ordinals (0 and 2, count 2).
        let gap = get_index_resp(
            &[(0, "http://n1:50051"), (2, "http://n3:50051")],
            2,
            0,
            vec![],
        );
        assert!(routing_plan_from_get_index("events", &gap).is_err());

        // A windowed index (window != 0) must route through the windowed gateway, not the ordinal
        // planner — the planner rejects it whether flagged by a window id …
        let mut windowed = get_index_resp(&[(0, "http://n1:50051")], 1, 0, vec![]);
        windowed.shard_status[0].window = 1_700_000_000_000_000;
        assert!(routing_plan_from_get_index("events", &windowed).is_err());
        // … or by the windowing config alone (defense in depth if a window id were 0).
        let mut wc = get_index_resp(&[(0, "http://n1:50051")], 1, 0, vec![]);
        wc.windowing = Some(growlerdb_proto::v1::WindowingConfig {
            field: "ts".into(),
            granularity: "daily".into(),
            ..Default::default()
        });
        assert!(routing_plan_from_get_index("events", &wc).is_err());
    }

    #[test]
    fn windowing_from_get_index_reconstructs_config() {
        // Granularity words map to the enum; an event-time field + hot_windows round-trip.
        let wc = growlerdb_proto::v1::WindowingConfig {
            field: "ingest".into(),
            granularity: "daily".into(),
            event_time_field: "event".into(),
            hot_windows: 3,
            has_hot_windows: true,
            field_format: "epoch_millis".into(),
        };
        let w = windowing_from_get_index("events", &wc).unwrap();
        assert_eq!(w.field, "ingest");
        assert_eq!(w.granularity, growlerdb_core::WindowGranularity::Daily);
        assert_eq!(w.event_time_field.as_deref(), Some("event"));
        assert_eq!(w.hot_windows, Some(3));

        // No event-time field ("") → None; no hot_windows → None (keep all hot); hourly/weekly parse.
        let w = windowing_from_get_index(
            "events",
            &growlerdb_proto::v1::WindowingConfig {
                field: "ts".into(),
                granularity: "hourly".into(),
                event_time_field: String::new(),
                hot_windows: 0,
                has_hot_windows: false,
                field_format: String::new(),
            },
        )
        .unwrap();
        assert_eq!(w.granularity, growlerdb_core::WindowGranularity::Hourly);
        assert!(w.event_time_field.is_none());
        assert!(w.hot_windows.is_none());

        // An unknown granularity is a hard error (a malformed/newer control-plane), not a silent default.
        assert!(windowing_from_get_index(
            "events",
            &growlerdb_proto::v1::WindowingConfig {
                field: "ts".into(),
                granularity: "yearly".into(),
                ..Default::default()
            },
        )
        .is_err());
    }

    fn resolved(name: &str) -> growlerdb_core::ResolvedIndex {
        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        IndexDefinition::from_yaml(&format!(
            "name: {name}\nsource: {{ iceberg: {{ catalog: g, table: g.{name} }} }}\nmapping: {{ selection: EXPLICIT, fields: [ {{ path: id, type: KEYWORD }} ] }}\n",
        ))
        .unwrap()
        .resolve(&src)
        .unwrap()
    }

    #[test]
    fn resolve_targets_unions_alias_member_primaries() {
        let tmp = tempfile::tempdir().unwrap();
        let reg = Registry::open(tmp.path().join("registry.json")).unwrap();
        reg.create(resolved("events_v1")).unwrap();
        reg.assign_primary("events_v1", 0, "http://n1:50051")
            .unwrap();
        reg.create(resolved("events_v2")).unwrap();
        reg.assign_primary("events_v2", 0, "http://n2:50051")
            .unwrap();
        reg.assign_primary("events_v2", 1, "http://n3:50051")
            .unwrap();
        reg.set_alias("events", ["events_v1", "events_v2"]).unwrap();

        // An index name fronts just its own shard primaries.
        let (members, eps) = resolve_targets(&reg, "events_v1").unwrap();
        assert_eq!(members, vec!["events_v1"]);
        assert_eq!(eps, vec!["http://n1:50051"]);

        // An alias fronts the union of all member primaries (members in order, then shard ordinal).
        let (members, eps) = resolve_targets(&reg, "events").unwrap();
        assert_eq!(members, vec!["events_v1", "events_v2"]);
        assert_eq!(
            eps,
            vec!["http://n1:50051", "http://n2:50051", "http://n3:50051"]
        );

        // A name that's neither an index nor an alias errors.
        assert!(resolve_targets(&reg, "ghost").is_err());
    }

    #[test]
    fn retention_plan_keeps_most_recent_drops_oldest() {
        let names: Vec<String> = [
            "events-2025-01",
            "events-2025-03",
            "events-2025-02",
            "logs-2025-01",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        // Keep the 1 most-recent `events-*` → the two oldest are dropped (sorted by name = date).
        assert_eq!(
            retention_plan(&names, "events-*", 1),
            vec!["events-2025-01", "events-2025-02"]
        );
        // Keep 2 → only the single oldest drops.
        assert_eq!(
            retention_plan(&names, "events-*", 2),
            vec!["events-2025-01"]
        );
        // Keeping more than match → nothing dropped; a pattern matching nothing → empty.
        assert!(retention_plan(&names, "events-*", 9).is_empty());
        assert!(retention_plan(&names, "metrics-*", 0).is_empty());
    }
}
