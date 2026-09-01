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

/// Load an index's **authoritative resolved definition** from the control-plane registry — the boot
/// source of truth in cluster mode (the definition a durable alter last committed, tracked by the
/// registry's `definition_version`). `Ok(None)` when the control plane doesn't have the index yet
/// (first boot: NOT_FOUND) or the registry row predates `definition_json` — the caller then keeps its
/// local / re-derived def. `Err` only on a real connection failure, so a misconfigured endpoint
/// surfaces loudly rather than silently booting a stale definition.
async fn fetch_cp_definition(
    cp: &str,
    name: &str,
) -> anyhow::Result<Option<growlerdb_core::ResolvedIndex>> {
    use growlerdb_proto::v1::GetIndexRequest;
    let mut client = connect_cp(cp, false).await?;
    let resp = match client
        .get_index(GetIndexRequest {
            name: name.to_string(),
        })
        .await
    {
        Ok(r) => r.into_inner(),
        Err(status) if status.code() == tonic::Code::NotFound => return Ok(None),
        Err(status) => {
            return Err(anyhow::anyhow!(
                "GetIndex(`{name}`) from control plane `{cp}`: {status}"
            ))
        }
    };
    if resp.definition_json.is_empty() {
        return Ok(None); // legacy registry row written before definition_json existed
    }
    let resolved = serde_json::from_str(&resp.definition_json).map_err(|e| {
        anyhow::anyhow!("control plane returned an unparseable definition for `{name}`: {e}")
    })?;
    Ok(Some(resolved))
}

#[derive(Parser)]
#[command(
    name = "growlerdb",
    version = VERSION,
    about = "Full-text, vector & hybrid search over your data"
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
        /// Control-plane `host:port`. On a cluster boot, load the index's **authoritative
        /// definition** from the registry (the definition a durable alter last committed) instead of
        /// re-deriving from the source — so the on-disk index opens/builds at the right schema and a
        /// node booting after an alter doesn't hit SchemaChanged. Falls back to the local/derived def
        /// when the control plane doesn't have the index yet (first boot). Requires `--name`.
        #[arg(long)]
        control_plane: Option<String>,
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
    /// Coordinated online reindex of a (multi-shard) index via the control plane: build every
    /// shard's next generation from source, then cut over atomically (bump the routing generation).
    /// A build failure on any shard aborts before cutover, leaving the old generation intact. For a
    /// single embedded shard use `rebuild`.
    Reindex {
        /// Index name.
        index: String,
        /// Control-plane `host:port`.
        #[arg(long)]
        control_plane: String,
        /// Start the job and return its id immediately instead of streaming progress to completion.
        /// Poll it later with `growlerdb jobs get <id>` or cancel with `growlerdb jobs cancel <id>`.
        #[arg(long)]
        detach: bool,
    },
    /// Inspect and control async reindex jobs on the control plane.
    Jobs {
        #[command(subcommand)]
        action: JobAction,
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
        /// Shared service token closing this Node's **data-plane gRPC**
        /// (Write/Search/Lookup/Suggest/Admin/System) to callers that don't present it. In
        /// distributed mode a Node carries no per-user auth of its own (authn/RBAC/tenant live at
        /// the Gateway), so set this everywhere in the cluster (gateway, control plane, connector
        /// read the same env) for defense-in-depth beyond network isolation. Unset ⇒ open
        /// (single-node dev). Env: `GROWLERDB_SERVICE_TOKEN`.
        #[arg(long, env = "GROWLERDB_SERVICE_TOKEN")]
        service_token: Option<String>,
        #[command(flatten)]
        tls: ServerTlsArgs,
    },
    /// Run a **pool node** (D52): one process serving the windows of **many** windowed indexes over
    /// one gRPC endpoint, instead of one `serve` per index. Each `--index` must already be built on
    /// disk (`growlerdb index`); reads are dispatched per `(index, window)`. This is the
    /// interchangeable shard-host that kills the node-per-index wall. (Writes + CP-driven dynamic
    /// placement are a follow-on; this serves pre-built windowed indexes read-only.)
    ServePool {
        /// A windowed index this node serves (repeatable). Each must already be defined + built on
        /// disk under `{data_dir}/{index}`.
        #[arg(long = "index", required = true)]
        indexes: Vec<String>,
        /// Address to bind the data-plane gRPC services (`host:port`).
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
        /// Register into the Control-Plane placement pool at this gRPC endpoint and announce the
        /// served windows of every `--index`, so a cluster gateway can route to this node. The node
        /// heartbeats into the pool once (index-agnostic) and re-announces each index's windows.
        #[arg(long, requires = "advertise_addr")]
        register: Option<String>,
        /// The routable gRPC endpoint other services reach this node at (recorded when `--register`
        /// is set, since `--addr` is often a bind-only wildcard).
        #[arg(long)]
        advertise_addr: Option<String>,
        /// Seconds between **auto-compaction** health checks per served window (as `serve`); `0`
        /// disables. Default 60.
        #[arg(long, default_value_t = 60)]
        compact_interval_secs: u64,
        /// Shared service token closing this node's data-plane gRPC to callers that don't present it —
        /// same gate as `serve`. Unset ⇒ open (single-node dev). Env: `GROWLERDB_SERVICE_TOKEN`.
        #[arg(long, env = "GROWLERDB_SERVICE_TOKEN")]
        service_token: Option<String>,
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
        /// **HA mode:** back the registry with a shared Postgres instead of the local
        /// `registry.json`, so the control plane runs as **N stateless replicas** over one durable
        /// store (D51). Each replica starts as a warm standby; exactly one holds the single-writer
        /// advisory lock (the leader) and reports ready, standbys reload on the leader's writes and
        /// promote on its death. Requires a build with `--features postgres`. Env:
        /// `GROWLERDB_REGISTRY_POSTGRES`.
        #[arg(long, env = "GROWLERDB_REGISTRY_POSTGRES")]
        registry_postgres: Option<String>,
        #[command(flatten)]
        tls: ServerTlsArgs,
    },
    /// Run a read-only Model Context Protocol (MCP) server on stdio for AI agents (Claude and any
    /// MCP client). It fronts the GrowlerDB **gateway** over HTTP — forwarding the caller's bearer
    /// token so the gateway's RBAC + tenant isolation govern every read — and exposes
    /// search/hydrate/aggregate/list/describe as MCP tools. HTTP-only: it needs no local data dir.
    Mcp {
        /// Gateway origin the MCP server fronts. Env: `GROWLERDB_GATEWAY_URL`.
        #[arg(
            long,
            default_value = "http://127.0.0.1:8081",
            env = "GROWLERDB_GATEWAY_URL"
        )]
        gateway_url: String,
        /// Bearer token forwarded to the gateway (carries tenant + RBAC). If absent, pass
        /// `--username`/`--password` to log in for one. Env: `GROWLERDB_TOKEN`.
        #[arg(long, env = "GROWLERDB_TOKEN")]
        token: Option<String>,
        /// Default index for a tool call that omits `index`.
        #[arg(long)]
        index: Option<String>,
        /// Username to log in with (via `POST /v1/login`) when `--token` is absent. Env:
        /// `GROWLERDB_USERNAME`.
        #[arg(long, env = "GROWLERDB_USERNAME")]
        username: Option<String>,
        /// Password paired with `--username`. Env: `GROWLERDB_PASSWORD`.
        #[arg(long, env = "GROWLERDB_PASSWORD")]
        password: Option<String>,
    },
}

/// `growlerdb jobs …` — inspect and control async reindex jobs on the control plane.
#[derive(Subcommand)]
enum JobAction {
    /// List reindex jobs, newest first.
    List {
        /// Control-plane `host:port`.
        #[arg(long)]
        control_plane: String,
    },
    /// Show one job's status (per-shard phase + progress).
    Get {
        /// Job id (from `growlerdb reindex --detach` or `jobs list`).
        id: String,
        /// Control-plane `host:port`.
        #[arg(long)]
        control_plane: String,
    },
    /// Request cancellation of a running job (staged generations are discarded; the old generation
    /// stays live).
    Cancel {
        /// Job id.
        id: String,
        /// Control-plane `host:port`.
        #[arg(long)]
        control_plane: String,
    },
}

/// Cluster reconcile backstop: fetch the index's shard map + bucket owners from the
/// control plane, then fan a **shard-scoped** `ReconcileIndex` out to each shard's primary node —
/// each node compares only the keys it owns (via the same bucket map the gateway/connector route by),
/// so a reconcile can't pull another shard's keys into it. Prints per-shard drift + a total. Any
/// unreachable shard, missing primary, or shard-level error makes the whole run exit non-zero, so a
/// scheduled CronJob surfaces the failure instead of silently skipping a shard.
/// Coordinated online reindex of a (multi-shard) index over the control plane's **async job** API:
/// `StartReindexJob` returns immediately, and the driver builds every shard's next generation from
/// source, then cuts over atomically (a build failure aborts before any cutover — the old generation
/// stays live). By default this streams per-shard progress to the terminal until the job finishes;
/// `--detach` starts it and prints the job id to poll later.
async fn reindex_cluster(control_plane: &str, index: &str, detach: bool) -> anyhow::Result<()> {
    use growlerdb_proto::v1::{GetReindexJobRequest, StartReindexJobRequest};
    let mut cp = connect_cp(control_plane, false).await?;
    let started = cp
        .start_reindex_job(StartReindexJobRequest {
            index: index.to_string(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("StartReindexJob(`{index}`): {e}"))?
        .into_inner();
    let job_id = started.job_id;

    if detach {
        println!(
            "started reindex job `{job_id}` for `{index}` (poll: growlerdb jobs get {job_id})"
        );
        return Ok(());
    }

    // Stream progress: poll the job until it reaches a terminal state.
    let mut last_line = String::new();
    loop {
        let job = cp
            .get_reindex_job(GetReindexJobRequest {
                job_id: job_id.clone(),
            })
            .await
            .map_err(|e| anyhow::anyhow!("GetReindexJob(`{job_id}`): {e}"))?
            .into_inner();
        // Print a status line only when it changes, so a fast rebuild doesn't spam the terminal.
        let line = format!(
            "  {} — {}/{} shards, {}/{} docs",
            job.state,
            job.shards
                .iter()
                .filter(|s| s.phase == "promoted" || s.phase == "built")
                .count(),
            job.shards.len(),
            job.docs_done,
            job.docs_total
        );
        if line != last_line {
            println!("{line}");
            last_line = line;
        }
        match job.state.as_str() {
            "done" => {
                println!(
                    "reindexed `{index}`: {} shard(s) cut over, generation {}, {} document(s)",
                    job.shards.len(),
                    job.generation,
                    job.docs_done
                );
                return Ok(());
            }
            "failed" => {
                anyhow::bail!("reindex of `{index}` failed: {}", job.error);
            }
            "canceled" => {
                anyhow::bail!("reindex of `{index}` was canceled: {}", job.error);
            }
            _ => tokio::time::sleep(std::time::Duration::from_millis(500)).await,
        }
    }
}

/// `growlerdb jobs …`: list / poll / cancel async reindex jobs over the control plane.
async fn jobs_cmd(action: JobAction) -> anyhow::Result<()> {
    use growlerdb_proto::v1::{
        CancelReindexJobRequest, GetReindexJobRequest, ListReindexJobsRequest, ReindexJobStatus,
    };

    /// One-line human summary of a job.
    fn summarize(job: &ReindexJobStatus) {
        println!(
            "{}  {}  {}  {}/{} docs  gen {}{}",
            job.id,
            job.index,
            job.state,
            job.docs_done,
            job.docs_total,
            job.generation,
            if job.error.is_empty() {
                String::new()
            } else {
                format!("  ({})", job.error)
            }
        );
    }

    match action {
        JobAction::List { control_plane } => {
            let mut cp = connect_cp(&control_plane, false).await?;
            let jobs = cp
                .list_reindex_jobs(ListReindexJobsRequest {})
                .await
                .map_err(|e| anyhow::anyhow!("ListReindexJobs: {e}"))?
                .into_inner()
                .jobs;
            if jobs.is_empty() {
                println!("no reindex jobs");
            }
            for job in &jobs {
                summarize(job);
            }
        }
        JobAction::Get { id, control_plane } => {
            let mut cp = connect_cp(&control_plane, false).await?;
            let job = cp
                .get_reindex_job(GetReindexJobRequest { job_id: id.clone() })
                .await
                .map_err(|e| anyhow::anyhow!("GetReindexJob(`{id}`): {e}"))?
                .into_inner();
            summarize(&job);
            for s in &job.shards {
                // A windowed job's units are identified by window (ordinal 0); an ordinal job's by shard.
                let unit = if s.window != 0 {
                    format!("window {}", s.window)
                } else {
                    format!("shard {}", s.ordinal)
                };
                println!(
                    "  {} @ {} — {} ({}/{} docs)",
                    unit, s.node, s.phase, s.docs_done, s.docs_total
                );
            }
        }
        JobAction::Cancel { id, control_plane } => {
            let mut cp = connect_cp(&control_plane, false).await?;
            let job = cp
                .cancel_reindex_job(CancelReindexJobRequest { job_id: id.clone() })
                .await
                .map_err(|e| anyhow::anyhow!("CancelReindexJob(`{id}`): {e}"))?
                .into_inner();
            println!("cancel requested for job `{}` ({})", job.id, job.state);
        }
    }
    Ok(())
}

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
        // Mesh dial: stamp the shared service token (env) — the node's data plane enforces it.
        let (channel, stamp) = growlerdb_proto::service_token::node_channel(primary).await?;
        let mut client = AdminClient::with_interceptor(channel, stamp);
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

    // Count-gate: if Σ index docs across shards equals the source total, skip the expensive
    // row-level reconcile. Any unreachable shard / zero source total falls through to a real
    // reconcile; skipped when `--full` forces a sweep.
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

    // `mcp` speaks JSON-RPC on stdout, so it must not init the telemetry stdout log layer (would
    // corrupt the protocol) and opens no embedded engine — handle it before both.
    if matches!(cli.command, Command::Mcp { .. }) {
        return run_mcp(cli.command).await;
    }

    // Structured JSON logging + the Prometheus metrics recorder.
    growlerdb_telemetry::init("growlerdb");
    // Startup splash to stderr — clap handles --help/--version before this, so it never pollutes
    // piped stdout.
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
            control_plane,
        } => {
            // Cluster boot: prefer the control plane's authoritative definition (the def a durable
            // alter last committed) over a locally re-derived one, so the on-disk index opens/builds
            // at the schema its reindexed segments were built with. `None` (first boot / not
            // registered / no --control-plane) falls back to the re-derive path below.
            let cp_def = match (control_plane.as_deref(), name.as_deref()) {
                (Some(cp), Some(n)) => fetch_cp_definition(cp, n).await?,
                _ => None,
            };
            if let Some(resolved) = cp_def {
                let index_name = resolved.name.clone();
                if define_only {
                    engine.adopt_resolved_definition(&resolved)?;
                    println!(
                        "defined `{index_name}` from the control-plane definition: index.json written, no shards built"
                    );
                } else {
                    let outcome = engine
                        .index_shard_with(resolved, &table, shards, shard_ordinal)
                        .await?;
                    let scope = if shards > 1 {
                        format!(" (shard {shard_ordinal}/{shards})")
                    } else {
                        String::new()
                    };
                    println!(
                        "indexed `{}`{} from the control-plane definition: {} documents at snapshot {}",
                        outcome.name, scope, outcome.doc_count, outcome.snapshot.0
                    );
                }
                return Ok(());
            }
            let def_yaml = def.map(std::fs::read_to_string).transpose()?;
            if define_only {
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
        Command::Reindex {
            index,
            control_plane,
            detach,
        } => {
            reindex_cluster(&control_plane, &index, detach).await?;
        }
        Command::Jobs { action } => {
            jobs_cmd(action).await?;
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
            service_token,
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
                    service_token.clone(),
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
                    service_token,
                })
                .await?;
            }
        }
        Command::ServePool {
            indexes,
            addr,
            register,
            advertise_addr,
            compact_interval_secs,
            service_token,
            tls,
        } => {
            serve_pool(
                &cli.data_dir,
                &indexes,
                &addr,
                register.as_deref(),
                advertise_addr.as_deref(),
                metrics_addr.as_deref(),
                compact_interval_secs,
                service_token,
                tls.load()?,
            )
            .await?;
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
            registry_postgres,
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
                registry_postgres,
                tls.load()?,
            )
            .await?;
        }
        // Handled before the engine opens (see the top of `run`), so it never reaches this match.
        Command::Mcp { .. } => {
            unreachable!("Command::Mcp is dispatched by run_mcp before Engine::open")
        }
    }
    // Flush any buffered OTLP spans before exit (no-op when export is off).
    growlerdb_telemetry::shutdown();
    Ok(())
}

/// Run the `mcp` subcommand: a read-only MCP stdio server fronting the gateway. Kept off the
/// telemetry/engine path in [`run`] because it owns stdout for JSON-RPC. When no `--token` is given
/// but `--username`/`--password` are, it logs in first (`POST /v1/login`) to obtain one.
async fn run_mcp(command: Command) -> anyhow::Result<()> {
    let Command::Mcp {
        gateway_url,
        token,
        index,
        username,
        password,
    } = command
    else {
        unreachable!("run_mcp is only called for Command::Mcp");
    };

    let token = match (token, username, password) {
        (Some(token), _, _) => Some(token),
        (None, Some(username), Some(password)) => {
            let client = growlerdb_mcp::GatewayClient::new(gateway_url.clone(), None);
            Some(client.login(&username, &password).await?)
        }
        (None, Some(_), None) | (None, None, Some(_)) => {
            anyhow::bail!("--username and --password must be provided together");
        }
        (None, None, None) => None,
    };

    let cfg = growlerdb_mcp::McpConfig {
        gateway_url,
        token,
        default_index: index,
    };
    growlerdb_mcp::serve(cfg).await
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
            // Parked underneath this handle (hot→cold by `spawn_park`) → read-only, no writer to
            // compact. Stop watching; a later `revive`/pre-warm re-spawns compaction.
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
            // Sample segments + delete debt every tick so the panel shows growth between merges,
            // not just at compaction time.
            growlerdb_telemetry::sli::segments_live(&label, health.segments);
            growlerdb_telemetry::sli::index_deleted_docs(&label, health.deleted);
            // Index side of the source→index convergence check: sum(index_docs) vs
            // sum(source_records) must meet at steady state.
            if let Ok(docs) = shard.num_docs() {
                growlerdb_telemetry::sli::index_docs(&label, docs);
            }
            // One walk serves both gauges: total == sum over the breakdown by construction.
            let bd = shard.index_size_breakdown();
            growlerdb_telemetry::sli::index_bytes(&label, bd.total());
            growlerdb_telemetry::sli::index_bytes_component(&label, "term", bd.term);
            growlerdb_telemetry::sli::index_bytes_component(&label, "postings", bd.postings);
            growlerdb_telemetry::sli::index_bytes_component(&label, "positions", bd.positions);
            growlerdb_telemetry::sli::index_bytes_component(&label, "fieldnorms", bd.fieldnorms);
            growlerdb_telemetry::sli::index_bytes_component(&label, "fast", bd.fast);
            growlerdb_telemetry::sli::index_bytes_component(&label, "store", bd.store);
            growlerdb_telemetry::sli::index_bytes_component(&label, "other", bd.other);
            growlerdb_telemetry::sli::background_success("compaction");
            if let Some(reason) = policy.reason_to_compact(&health) {
                eprintln!("compact `{label}`: {reason} — merging");
                let before = health.segments;
                let compact_shard = handle.current();
                match tokio::task::spawn_blocking(move || compact_shard.compact(&policy)).await {
                    Ok(Ok(())) => {
                        eprintln!("compact `{label}`: done");
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
            // redb allows one open per file, and the cold shard's `aux.redb` stays live in `handle`
            // until the swap below, so the arriving hot shard must share it, not reopen.
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

/// **One park pass** over a windowed node's live window set: demote every aged hot window (past the
/// `hot_windows` policy) among `live` to cold read-through — back it up through the live serving
/// handle (borrow, no second writer), swap the handle to a read-through shard that **shares** the
/// window's still-open `aux.redb`, and evict the local bulk. Returns the windows **newly** parked
/// this pass (already-cold windows are skipped), so the caller can start a pre-warm watcher on each.
/// A per-window failure is logged + counted and skipped; the pass never aborts. `live` is `(window
/// id, handle)`; `def_json` is the serialized index definition for the backup manifest.
///
/// The armed form of the [`park_once`] test seam: a callback run with the window id.
#[cfg(test)]
type ParkTestHook = Box<dyn Fn(i64) + Send>;
#[cfg(test)]
/// Test-only interleave seam for [`park_once`]: runs after a window's backup completes and
/// before its handle swaps to cold — exactly the window the write-race check closes. Armed by
/// the race test; `None` (always, in production builds) is a no-op.
static PARK_TEST_AFTER_BACKUP: std::sync::Mutex<Option<ParkTestHook>> = std::sync::Mutex::new(None);

/// [`cold_windows`]: growlerdb_core::TimeWindowing::cold_windows
#[allow(clippy::too_many_arguments)]
async fn park_once(
    live: &[(i64, growlerdb_engine::ShardHandle)],
    store: &growlerdb_index::LocalIndexStore,
    object_store: &growlerdb_backup::Operator,
    cache: &growlerdb_index::RangeCache,
    resolved: &growlerdb_core::ResolvedIndex,
    windowing: &growlerdb_core::TimeWindowing,
    index: &str,
    def_json: &str,
) -> Vec<i64> {
    use growlerdb_index::ShardId;
    use std::sync::Arc;
    let ids: Vec<i64> = live.iter().map(|(w, _)| *w).collect();
    // Aged windows outside the `hot_windows` policy — exactly the manual `park` victims.
    let victims: Vec<i64> = windowing.cold_windows(&ids, None).to_vec();
    let mut parked = Vec::new();
    for w in victims {
        let Some(handle) = live.iter().find(|(x, _)| *x == w).map(|(_, h)| h.clone()) else {
            continue;
        };
        // Already parked (read-through, no writer) → nothing to do.
        if handle.current().is_read_only() {
            continue;
        }
        let label = format!("{index} w{w}");
        let window_dir = store.shard_path(&ShardId::window(index, w));
        // Staging sits beside the window dir (same filesystem → segment files hard-link).
        let staging = window_dir.with_file_name(format!(".cold-staging-{index}-w{w}"));
        let prefix = format!("cold/{index}/w{w}");
        // Back up + cold-tier THROUGH the live serving shard (borrow — no second writer). The window
        // keeps serving hot until the swap below.
        let hot = handle.current();
        let marker = match growlerdb_backup::cold_park_in_place(
            &hot,
            index,
            w,
            &window_dir,
            &staging,
            object_store,
            &prefix,
            Some(def_json.to_string()),
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
        #[cfg(test)]
        if let Some(hook) = PARK_TEST_AFTER_BACKUP.lock().unwrap().as_ref() {
            hook(w);
        }
        // redb allows one open per file, and the hot shard stays live until the swap below, so the
        // cold shard must SHARE this `aux.redb` handle. Keep the hot `Arc` too: if a write raced the
        // backup (checked post-swap below), we swap it right back.
        let reuse_db = hot.db_handle();
        // Open the read-through cold shard (object-storage reads → blocking) and hot-swap it in, so
        // queries never see a gap; then evict the now-redundant local bulk.
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
                // Write-race check, AFTER the swap so it can't itself race (post-swap the window is
                // read-only → this comparison is stable). The window stayed writable until the swap;
                // a write in between advanced the kept `aux.redb` checkpoint while its segments live
                // only in the local bulk we're about to evict — serving the cold copy would silently
                // lose it. On a mismatch, swap the intact hot shard back and re-park next tick.
                let live_snapshot = handle.current().current_snapshot().unwrap_or(u64::MAX);
                if live_snapshot != marker.snapshot {
                    handle.swap(hot);
                    eprintln!(
                        "park `{label}`: a write raced the backup (snapshot {} → {live_snapshot}) \
                         — staying hot, re-parking next tick",
                        marker.snapshot
                    );
                    growlerdb_telemetry::sli::background_failure("park");
                    continue;
                }
                drop(hot);
                // Marker durable + read-through shard live → drop the local bulk (`aux.redb` stays).
                if let Err(e) = growlerdb_backup::evict_local_index(&window_dir) {
                    eprintln!(
                        "park `{label}`: local bulk evict failed ({e}) — parked, cleanup deferred"
                    );
                    growlerdb_telemetry::sli::background_failure("park");
                }
                eprintln!(
                    "park `{label}`: cold-parked (snapshot {}) — now serving read-through",
                    marker.snapshot
                );
                growlerdb_telemetry::sli::background_success("park");
                parked.push(w);
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
    parked
}

/// Background **park** loop for a windowed node — the hot→cold counterpart of [`spawn_prewarm`].
/// Each interval it reads the node's *live* window set (boot windows plus any created at runtime by
/// ingest) and runs one [`park_once`] pass, then starts a pre-warm watcher on each window it parked
/// so one that gets hot again auto-revives. A no-op when `interval_secs == 0`. Same discipline as the
/// other background loops: transient failures are logged + counted and retried next interval; the
/// loop never dies.
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
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        tick.tick().await; // skip the immediate first tick
        loop {
            tick.tick().await;
            let live = write.window_handles();
            let parked = park_once(
                &live,
                &store,
                &object_store,
                &cache,
                &resolved,
                &windowing,
                &index,
                &def_json,
            )
            .await;
            // Each newly-parked window gets a pre-warm watcher so it can promote itself back to hot.
            for w in parked {
                if let Some(handle) = live.iter().find(|(x, _)| *x == w).map(|(_, h)| h.clone()) {
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
    service_token: Option<String>,
}

/// Host the gRPC services for the index over its address (and, if `rest_addr` is set, the
/// REST/JSON gateway over that address) until ^C.
async fn serve(cfg: ServeConfig<'_>) -> anyhow::Result<()> {
    use growlerdb_index::{LocalIndexStore, ShardId};
    use growlerdb_proto::{SystemServer, SystemService};
    use std::sync::Arc;
    use tonic::transport::Server;

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
        service_token,
        data_dir: _,
    } = cfg;
    let shard_id = ShardId::single(index);
    // Complete or clean up any reindex that a prior process was interrupted mid-swap.
    store.recover_reindex(&shard_id)?;
    let shard = Arc::new(store.open_shard(&shard_id, &resolved)?);

    // Lineage guard: a live `table-uuid` differing from the one recorded at build means the source
    // was dropped+recreated and the index is stale (its keys no longer hydrate). Serve DEGRADED
    // (read-only search, writes refused) rather than refuse to boot, so the connector stops
    // advancing and the CP/console surface a `source_recreated` state; a reindex clears it.
    // Best-effort: a transient catalog/uuid read error only warns — only a confirmed mismatch degrades.
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
        .with_source_recreated(source_recreated)
        // Embed VECTOR fields on the streaming write path (D46) so a connector-fed index (which
        // can't cold-build — D49) still gets LOCAL embeddings. No-op for a non-vector index.
        .with_embedding(resolved.clone());
    let search = growlerdb_engine::SearchService::new(handle.clone());
    // GetByKey hydrates coordinates back to rows from the index's Iceberg source.
    let table = match &resolved.source {
        growlerdb_core::Source::Iceberg(s) => s.table.clone(),
    };
    let lookup = growlerdb_engine::LookupService::new(
        handle.clone(),
        IcebergConfig::from_env(),
        table.clone(),
        resolved.clone(),
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

    // Health-driven auto-compaction so segments don't accumulate under steady ingest. Only this
    // primary path compacts — a `--replica` must never compact or it diverges from pulled segments.
    spawn_auto_compaction(handle.clone(), index.to_string(), compact_interval_secs);

    // Optional Engine API over REST/JSON on a second listener: routes through the Gateway → an
    // in-process LocalNode, so embedded mode collapses Gateway + Node into one process, no hop.
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
        let gateway = Arc::new(
            growlerdb_engine::Gateway::new(node.shared())
                .with_limits(growlerdb_engine::GatewayLimits::from_env())
                .serving(resolved.name.clone()),
        );
        let router = with_mcp(rest_router(gateway.clone(), ui_dir), gateway);
        let listener = tokio::net::TcpListener::bind(rest_socket).await?;
        println!("serving REST/JSON gateway on http://{rest_socket}/v1/... (+ MCP on /mcp)");
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

    // Announce this served index to the CP registry so the gateway can route to it. Retries + re-
    // announces on an interval: node pods routinely start before the CP, so a one-shot attempt
    // would leave the shard serving but invisible forever.
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

    // Service-token gate over the whole data plane: a Node carries no per-user auth in distributed
    // mode, so the shared token is the defense-in-depth boundary. Unset ⇒ no-op (open dev).
    if service_token.is_none() {
        eprintln!(
            "serve: WARNING data-plane gRPC is open — any caller that can reach {socket} can \
             write/search/reindex (set GROWLERDB_SERVICE_TOKEN to close it)"
        );
    }
    builder
        .layer(growlerdb_engine::service_token_layer(service_token.clone()))
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
    service_token: Option<String>,
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

    // Read-only surface: a replica must stay byte-identical to the primary, so no Write service and
    // Admin has no source (describe works; reindex/alter return Unimplemented).
    let table = match &resolved.source {
        growlerdb_core::Source::Iceberg(s) => s.table.clone(),
    };
    let search = growlerdb_engine::SearchService::new(handle.clone());
    let lookup = growlerdb_engine::LookupService::new(
        handle.clone(),
        IcebergConfig::from_env(),
        table,
        resolved.clone(),
    );
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
        let gateway = Arc::new(
            growlerdb_engine::Gateway::new(node.shared())
                .with_limits(growlerdb_engine::GatewayLimits::from_env())
                .serving(resolved.name.clone()),
        );
        let router = with_mcp(rest_router(gateway.clone(), ui_dir), gateway);
        let listener = tokio::net::TcpListener::bind(rest_socket).await?;
        println!("replica REST/JSON gateway on http://{rest_socket}/v1/... (+ MCP on /mcp)");
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

    // Same data-plane service-token gate as `serve` (a replica's read surface is mesh-internal).
    if service_token.is_none() {
        eprintln!(
            "serve --replica: WARNING data-plane gRPC is open (set GROWLERDB_SERVICE_TOKEN to close it)"
        );
    }
    builder
        .layer(growlerdb_engine::service_token_layer(service_token.clone()))
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
        service_token,
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

    // A windowed node may start empty (streaming-first) and create each window on first write, so an
    // empty window set is valid. The batch `growlerdb index` path pre-populates them when used.
    let windows = store.window_shards(index)?;

    // Cold windows serve read-through from object storage; build the shared object store + range
    // cache when any window is already parked OR automatic parking is enabled (which creates cold
    // windows at runtime and needs somewhere to write + a cache to serve them).
    let park_interval = park_interval_secs();
    let any_cold = windows
        .iter()
        .any(|&w| matches!(store.cold_marker(index, w), Ok(Some(_))));
    let object_store = if any_cold || park_interval > 0 {
        // Fail fast: parking/cold with no object store configured is a misconfiguration, not a
        // silent no-op (`object_store_from_env` errors when neither env var is set).
        Some(object_store_from_env()?)
    } else {
        None
    };
    let cache = object_store
        .as_ref()
        .map(|_| growlerdb_index::RangeCache::new(cold_cache_bytes()));

    // One in-process Node per window — a local Shard for a hot window, a read-through cold Shard for
    // a parked one (tagged with the marker's zone-map so the Gateway prunes it without a fetch). Runs
    // on a blocking thread because the cold path `block_on`s object-storage reads.
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
                Vec<(i64, Option<(i64, i64)>, bool)>,
                Vec<growlerdb_proto::v1::ServedWindow>,
                Vec<i64>,
                std::collections::BTreeMap<i64, SearchService>,
                std::collections::BTreeMap<i64, SuggestService>,
                std::collections::BTreeMap<i64, LookupService>,
                std::collections::BTreeMap<i64, AdminService>,
                Vec<(i64, ShardHandle)>,
                Vec<(i64, ShardHandle)>,
            );
            // Each window shard backs an in-process `LocalNode` (embedded REST Gateway) plus the gRPC
            // window multiplexers over the *same* swappable handle. The handle is returned so a HOT
            // window can be auto-compacted.
            let build = |shard: Arc<growlerdb_index::Shard>,
                         w: i64,
                         hot: bool|
             -> (
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
                    LookupService::new(
                        handle.clone(),
                        IcebergConfig::from_env(),
                        table.clone(),
                        resolved.clone(),
                    ),
                    AdminService::new(handle.clone(), &index_s),
                )
                .shared();
                // The gRPC window-multiplexer's admin gets **source access on a HOT window**, so a
                // coordinated per-window reindex can rebuild that window's shard from source (filtered
                // to its ingest-time window) and swap it live. A COLD (read-through, no-writer) window
                // isn't reindexable in place — the planner skips it — so its admin stays source-less.
                // A fresh per-window ReindexFence backs the reindex's single-flight/RAII; windowed
                // cutover correctness comes from the connector's resume-and-replay (stamp at the build
                // snapshot), not a write-fence, so the fence needn't be shared with the write path.
                let admin = if hot {
                    AdminService::new(handle.clone(), &index_s).with_source(
                        resolved.clone(),
                        store.clone(),
                        ShardId::window(&index_s, w),
                        IcebergConfig::from_env(),
                        table.clone(),
                        growlerdb_engine::ReindexFence::new(),
                    )
                } else {
                    AdminService::new(handle.clone(), &index_s)
                };
                (
                    node,
                    SearchService::new(handle.clone()),
                    SuggestService::new(handle.clone()),
                    LookupService::new(
                        handle.clone(),
                        IcebergConfig::from_env(),
                        table.clone(),
                        resolved.clone(),
                    ),
                    admin,
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
                        let (node, search, suggest, lookup, admin, handle) = build(shard, w, false);
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
                        let (node, search, suggest, lookup, admin, handle) = build(shard, w, true);
                        hot_handles.push((w, handle)); // hot → eligible for auto-compaction
                        (node, search, suggest, lookup, admin, zone)
                    }
                };
                nodes.push(node);
                windowed_search.insert(w, search_svc);
                windowed_suggest.insert(w, suggest_svc);
                windowed_lookup.insert(w, lookup_svc);
                windowed_admin.insert(w, admin_svc);
                let is_cold = cold_ids.contains(&w);
                descriptors.push((w, zone, is_cold));
                served.push(growlerdb_proto::v1::ServedWindow {
                    window: w,
                    event_min: zone.map(|(lo, _)| lo).unwrap_or(0),
                    event_max: zone.map(|(_, hi)| hi).unwrap_or(0),
                    has_event_bounds: zone.is_some(),
                    cold: is_cold,
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

    // Dynamic windowed ingest: the mux maps become shared + mutable so the write path can add a
    // window created at runtime; snapshot the boot windows as the write service's seed. Built before
    // `hot_handles`/`nodes` are consumed below.
    let handle_by_window: std::collections::BTreeMap<i64, growlerdb_engine::ShardHandle> =
        hot_handles
            .iter()
            .chain(cold_handles.iter())
            .map(|(w, h)| (*w, h.clone()))
            .collect();
    let window_seed: std::collections::BTreeMap<i64, growlerdb_engine::WindowSeed> = nodes
        .iter()
        .zip(descriptors.iter())
        .map(|(node, (w, zone, _cold))| (*w, (handle_by_window[w].clone(), node.clone(), *zone)))
        .collect();
    let search_windows: growlerdb_engine::SharedSearchWindows =
        Arc::new(std::sync::RwLock::new(windowed_search));
    let suggest_windows: growlerdb_engine::SharedSuggestWindows =
        Arc::new(std::sync::RwLock::new(windowed_suggest));
    let lookup_windows: growlerdb_engine::SharedLookupWindows =
        Arc::new(std::sync::RwLock::new(windowed_lookup));
    let admin_windows: growlerdb_engine::SharedAdminWindows =
        Arc::new(std::sync::RwLock::new(windowed_admin));

    // Each HOT window gets its own health-driven compaction loop (the current window accumulates
    // segments under steady ingest). Cold read-through windows have no writer and are skipped.
    for (w, handle) in hot_handles {
        spawn_auto_compaction(handle, format!("{index} w{w}"), compact_interval_secs);
    }

    // Access-driven pre-warm: each cold window watches its read rate and, when it gets hot again, is
    // promoted back to a local hot shard so it stops paying cold-tier latency. Needs the object store.
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
    let mut gateway = Gateway::windowed(nodes, windowing.clone(), descriptors)
        .with_date_formats(resolved.date_formats());
    if let Some(cache) = &cache {
        gateway = gateway.with_cold_tier(cache.clone());
    }
    let gateway = Arc::new(gateway);

    // Windowed write service: routes each streamed doc to its window shard, creating + publishing the
    // window on first write so it's immediately queryable. A new window gets its own compaction loop
    // via `on_new_window`.
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

    // Automatic cold-tiering (opt-in via GROWLERDB_PARK_INTERVAL_SECS): background-demote aged
    // windows past `hot_windows` to cold read-through — the hot→cold counterpart of pre-warm above.
    // Reads the write service's live window set so runtime-created windows are parked as they age.
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

    let rest_socket: std::net::SocketAddr = rest_addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --rest-addr `{rest_addr}`: {e}"))?;
    let router = with_mcp(rest_router(gateway.clone(), ui_dir), gateway.clone());
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

    // Report the served windows (+ zone-maps) to the control plane so a cluster Gateway can route to
    // them. Retries + re-announces on an interval (same K8s startup race as the sharded path);
    // `/readyz` stays not-ready until registered.
    if let (Some(cp), Some(endpoint)) = (register, advertise_addr) {
        // Heartbeat into the CP placement pool (so new windows can be placed here) AND re-announce
        // the currently-served windows each tick, so a window created since boot is advertised too.
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

    // gRPC listener: System + the window multiplexers (Search/Suggest/Lookup/Admin) over
    // `window id → service` maps that dispatch by the request's window selector, so a cluster Gateway
    // can route per-window requests to this one endpoint.
    let socket: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --addr `{addr}`: {e}"))?;
    let mut builder = Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
    }
    // Same data-plane service-token gate as `serve` — the windowed Write service in particular
    // must not be writable by anything that merely reaches the pod port.
    if service_token.is_none() {
        eprintln!(
            "serve (windowed): WARNING data-plane gRPC is open (set GROWLERDB_SERVICE_TOKEN to close it)"
        );
    }
    builder
        .layer(growlerdb_engine::service_token_layer(service_token.clone()))
        .add_service(
            growlerdb_engine::WindowedSearchService::new(search_windows.clone()).into_server(),
        )
        .add_service(
            growlerdb_engine::WindowedSuggestService::new(suggest_windows.clone()).into_server(),
        )
        // Hydration (keys:get) + describe: the Gateway broadcasts to every window, dispatched here.
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

/// Run a **pool node** (D52): serve the windows of *many* windowed indexes from one process over one
/// gRPC endpoint. For each `--index` (already built on disk) this opens its window shards into a
/// per-index window map and mounts the [`Pool*` services](growlerdb_engine::PoolSearchService), which
/// dispatch each request first on its `index` selector to that index's windows, then on the window.
/// One process therefore fronts many indexes' windows — the interchangeable shard-host that removes
/// the node-per-index wall.
///
/// **Writes** dispatch the same way: a per-index [`WindowedWriteService`](growlerdb_engine::WindowedWriteService)
/// (sharing that index's live window maps, so a window it creates on first write is immediately
/// queryable) sits behind a [`PoolWriteService`](growlerdb_engine::PoolWriteService) that routes each
/// `Write` / `GetCheckpoint` on the `(index, window)` selector — so the connector streams ingest to a
/// pool node exactly as to a single-index windowed node, but for many indexes at once. Each served
/// window gets its own auto-compaction loop (boot windows now, runtime-created windows via
/// `on_new_window`). CP-driven **dynamic assignment** (loading a unit on demand rather than from the
/// static `--index` list) and cold read-through in pool mode remain follow-ons.
/// Open the on-disk **ordinal shards** of a hash/partition-sharded `index` for pool serving (D52):
/// build a per-ordinal read surface (Search / Suggest / Lookup / Admin) + a single-shard
/// [`WriteService`](growlerdb_engine::WriteService), publish them into the shared per-index maps keyed
/// by **ordinal-as-i64** (the same maps a windowed index keys by window), and start a per-ordinal
/// auto-compaction loop. Returns the ordinals this node holds.
///
/// A corrupt ordinal is **quarantined** (HA-G4) like a corrupt window: logged, skipped, and the
/// remaining ordinals served — the CP sees the unit unserved and re-places it. The held set is fixed
/// at boot (no create-on-first-write: a hash ordinal is built offline by `growlerdb index --shards`
/// and placed by the CP). The writer runs the D46 embed stage for a VECTOR index, so a connector-fed
/// hash index still gets its LOCAL embeddings — as the single-index `serve` path does.
#[allow(clippy::too_many_arguments)]
async fn open_pool_hash_index(
    store: &growlerdb_index::LocalIndexStore,
    resolved: &growlerdb_core::ResolvedIndex,
    table: &str,
    index_heavy: std::sync::Arc<growlerdb_engine::IndexHeavyShare>,
    compact_interval_secs: u64,
    search_idx: &growlerdb_engine::SharedSearchIndexes,
    suggest_idx: &growlerdb_engine::SharedSuggestIndexes,
    lookup_idx: &growlerdb_engine::SharedLookupIndexes,
    admin_idx: &growlerdb_engine::SharedAdminIndexes,
    write_hash_idx: &growlerdb_engine::SharedHashWriteIndexes,
) -> anyhow::Result<Vec<(u32, growlerdb_engine::ShardHandle)>> {
    use growlerdb_engine::{
        AdminService, LookupService, SearchService, ShardHandle, SuggestService, WriteService,
    };
    use growlerdb_index::ShardId;
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};

    let index = resolved.name.clone();
    let ordinals = store.ordinal_shards(&index)?;
    let (search_o, suggest_o, lookup_o, admin_o, write_o, handles) = {
        let (store, resolved, index_s, table, ordinals, index_heavy) = (
            store.clone(),
            resolved.clone(),
            index.clone(),
            table.to_string(),
            ordinals.clone(),
            index_heavy.clone(),
        );
        tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
            let mut search_o = BTreeMap::new();
            let mut suggest_o = BTreeMap::new();
            let mut lookup_o = BTreeMap::new();
            let mut admin_o = BTreeMap::new();
            let mut write_o = BTreeMap::new();
            let mut handles: Vec<(u32, ShardHandle)> = Vec::new();
            for &o in &ordinals {
                // Quarantine, don't crash (HA-G4): one corrupt ordinal must not take down the whole
                // multi-index pool process — log it, skip it, serve the rest.
                let opened = store
                    .open_shard(&ShardId::shard(&index_s, o), &resolved)
                    .map(Arc::new);
                let shard = match opened {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!(
                            "serve-pool: QUARANTINED {index_s}/{o} — ordinal shard failed to open \
                             ({e}); serving the remaining ordinals (repair or delete the shard dir; a \
                             registered pool's control plane will re-place the unit)"
                        );
                        continue;
                    }
                };
                let handle = ShardHandle::new(shard);
                let key = o as i64;
                search_o.insert(
                    key,
                    SearchService::new(handle.clone()).with_index_heavy_share(index_heavy.clone()),
                );
                suggest_o.insert(key, SuggestService::new(handle.clone()));
                lookup_o.insert(
                    key,
                    LookupService::new(
                        handle.clone(),
                        IcebergConfig::from_env(),
                        table.clone(),
                        resolved.clone(),
                    ),
                );
                // Source access so the primary can rebuild this ordinal on a coordinated reindex/alter
                // (mirrors `serve` + windowed-hot; a replica serves via the source-less replicate path).
                admin_o.insert(
                    key,
                    AdminService::new(handle.clone(), &index_s).with_source(
                        resolved.clone(),
                        store.clone(),
                        ShardId::shard(&index_s, o),
                        IcebergConfig::from_env(),
                        table.clone(),
                        growlerdb_engine::ReindexFence::new(),
                    ),
                );
                write_o.insert(
                    key,
                    WriteService::new(handle.clone(), index_s.clone(), POOL_HASH_MAX_INFLIGHT)
                        .with_embedding(resolved.clone()),
                );
                handles.push((o, handle));
            }
            Ok((search_o, suggest_o, lookup_o, admin_o, write_o, handles))
        })
        .await??
    };

    // Publish the read maps + the writer map so the Pool services dispatch to this index's ordinals,
    // then start a per-ordinal auto-compaction loop (as each served window gets one).
    search_idx
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(index.clone(), Arc::new(RwLock::new(search_o)));
    suggest_idx
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(index.clone(), Arc::new(RwLock::new(suggest_o)));
    lookup_idx
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(index.clone(), Arc::new(RwLock::new(lookup_o)));
    admin_idx
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(index.clone(), Arc::new(RwLock::new(admin_o)));
    write_hash_idx
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(index.clone(), Arc::new(RwLock::new(write_o)));
    for (o, handle) in &handles {
        spawn_auto_compaction(
            handle.clone(),
            format!("{index} s{o}"),
            compact_interval_secs,
        );
    }
    Ok(handles)
}

/// Seconds between a primary pool node's hash-ordinal snapshot publishes
/// (`GROWLERDB_REPLICATE_INTERVAL_SECS`, default 30; `0` disables). Coarse by design: re-publishing a
/// writable shard re-uploads it wholesale (immutable-first — incremental hot shipping is deferred), so
/// a replica trails the primary by at most one interval plus the upload.
fn pool_replicate_interval_secs() -> u64 {
    std::env::var("GROWLERDB_REPLICATE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(30)
}

/// Background **publish** loop for one HOT hash ordinal on its primary (D53): on boot and every
/// `interval_secs`, snapshot the shard and publish it to object storage
/// ([`backup_replica_snapshot`](growlerdb_backup::backup_replica_snapshot)) under `cold/{index}/{ordinal}`,
/// so a cross-node replica opens it read-through and a primary-node loss is a zero-gap read failover.
/// Only a node holding the ordinal HOT locally runs this (the sole publisher — a read-through replica
/// has no writable shard to snapshot). Same discipline as the [park loop](spawn_park): transient
/// failures are logged + counted and retried next tick; the loop never dies. A no-op when
/// `interval_secs == 0`.
fn spawn_shard_replicate(
    handle: growlerdb_engine::ShardHandle,
    store: growlerdb_index::LocalIndexStore,
    object_store: growlerdb_backup::Operator,
    resolved: growlerdb_core::ResolvedIndex,
    ordinal: u32,
    interval_secs: u64,
) {
    if interval_secs == 0 {
        return;
    }
    let index = resolved.name.clone();
    // Serialize the definition once for the marker; a failure here is deterministic, so log + disable
    // rather than retry it every tick (serving is unaffected — only replica feeding stops).
    let def_json = match serde_json::to_string(&resolved) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "serve-pool replicate `{index}/{ordinal}`: cannot serialize definition ({e}) — \
                 publish disabled"
            );
            return;
        }
    };
    tokio::spawn(async move {
        let staging = store
            .shard_path(&growlerdb_index::ShardId::shard(&index, ordinal))
            .join(".replicate-stg");
        let prefix = format!("cold/{index}/{ordinal}");
        // The first tick fires immediately → publish on boot, then every interval.
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(interval_secs));
        // Skip a re-upload when the shard hasn't committed since the last publish, so the coarse
        // re-upload cost is paid only when there's new data for a replica to catch up on.
        let mut last_published: Option<u64> = None;
        loop {
            tick.tick().await;
            let shard = handle.current();
            if let Ok(cur) = shard.current_snapshot() {
                if last_published == Some(cur) {
                    growlerdb_telemetry::sli::background_success("shard-replicate");
                    continue;
                }
            }
            match growlerdb_backup::backup_replica_snapshot(
                &shard,
                &index,
                &ordinal.to_string(),
                &staging,
                &object_store,
                &prefix,
                Some(def_json.clone()),
            )
            .await
            {
                Ok(m) => {
                    last_published = Some(m.snapshot);
                    growlerdb_telemetry::sli::background_success("shard-replicate");
                    eprintln!(
                        "serve-pool replicate `{index}/{ordinal}`: published snapshot {} for \
                         read-through replicas",
                        m.snapshot
                    );
                }
                Err(e) => {
                    growlerdb_telemetry::sli::background_failure("shard-replicate");
                    eprintln!(
                        "serve-pool replicate `{index}/{ordinal}`: publish failed ({e}) — retrying \
                         next interval"
                    );
                }
            }
        }
    });
}

/// Admission ceiling per ordinal writer on a pool node (matching `serve`'s default): refuse rather
/// than queue unboundedly when a connector out-runs one ordinal's commit path.
const POOL_HASH_MAX_INFLIGHT: usize = 32;

/// Open ONE already-built hash ordinal shard (`{index}/{ordinal}`) into the **already-published**
/// per-index pool maps + the writer map, keyed by ordinal-as-i64, and start its auto-compaction. The
/// per-index maps must already exist (the index was opened — possibly empty — at boot). This is how
/// **build-on-assignment** publishes a freshly cold-built primary ordinal, and it mirrors one
/// iteration of [`open_pool_hash_index`]. Returns the shard handle so the caller can start the publish
/// loop. Inserts only if the ordinal isn't already served (idempotent under a racing reconcile).
#[allow(clippy::too_many_arguments)]
async fn open_and_publish_ordinal(
    store: &growlerdb_index::LocalIndexStore,
    resolved: &growlerdb_core::ResolvedIndex,
    table: &str,
    ordinal: u32,
    index_heavy: std::sync::Arc<growlerdb_engine::IndexHeavyShare>,
    compact_interval_secs: u64,
    search_idx: &growlerdb_engine::SharedSearchIndexes,
    suggest_idx: &growlerdb_engine::SharedSuggestIndexes,
    lookup_idx: &growlerdb_engine::SharedLookupIndexes,
    admin_idx: &growlerdb_engine::SharedAdminIndexes,
    write_hash_idx: &growlerdb_engine::SharedHashWriteIndexes,
) -> anyhow::Result<growlerdb_engine::ShardHandle> {
    use growlerdb_engine::{
        AdminService, LookupService, SearchService, ShardHandle, SuggestService, WriteService,
    };
    use growlerdb_index::ShardId;
    use std::sync::Arc;

    let index = resolved.name.clone();
    let (store2, resolved2, index2) = (store.clone(), resolved.clone(), index.clone());
    let handle = tokio::task::spawn_blocking(move || -> anyhow::Result<ShardHandle> {
        let shard = store2.open_shard(&ShardId::shard(&index2, ordinal), &resolved2)?;
        Ok(ShardHandle::new(Arc::new(shard)))
    })
    .await??;
    let key = ordinal as i64;
    // Publish into each per-index map (the SAME Arcs the Pool services front). Replace any existing
    // entry: this runs only when the caller (reconcile_primary_builds) decided to (re)serve the
    // ordinal as PRIMARY — either it was unserved, or it was a read-through REPLICA being promoted —
    // and the freshly-built primary is authoritative over a stale cold replica. A second primary for
    // one ordinal on a node can't race here (the reconcile `building` in-flight guard prevents it).
    if let Some(m) = search_idx
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&index)
        .cloned()
    {
        m.write().unwrap_or_else(|e| e.into_inner()).insert(
            key,
            SearchService::new(handle.clone()).with_index_heavy_share(index_heavy.clone()),
        );
    }
    if let Some(m) = suggest_idx
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&index)
        .cloned()
    {
        m.write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key, SuggestService::new(handle.clone()));
    }
    if let Some(m) = lookup_idx
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&index)
        .cloned()
    {
        m.write().unwrap_or_else(|e| e.into_inner()).insert(
            key,
            LookupService::new(
                handle.clone(),
                IcebergConfig::from_env(),
                table.to_string(),
                resolved.clone(),
            ),
        );
    }
    if let Some(m) = admin_idx
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&index)
        .cloned()
    {
        // Source access so a coordinated reindex/alter can rebuild this primary ordinal from source
        // (the same wiring as the boot-time open path and the single-node `serve`).
        m.write().unwrap_or_else(|e| e.into_inner()).insert(
            key,
            AdminService::new(handle.clone(), &index).with_source(
                resolved.clone(),
                store.clone(),
                ShardId::shard(&index, ordinal),
                IcebergConfig::from_env(),
                table.to_string(),
                growlerdb_engine::ReindexFence::new(),
            ),
        );
    }
    if let Some(m) = write_hash_idx
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(&index)
        .cloned()
    {
        m.write().unwrap_or_else(|e| e.into_inner()).insert(
            key,
            WriteService::new(handle.clone(), index.clone(), POOL_HASH_MAX_INFLIGHT)
                .with_embedding(resolved.clone()),
        );
    }
    spawn_auto_compaction(
        handle.clone(),
        format!("{index} s{ordinal}"),
        compact_interval_secs,
    );
    Ok(handle)
}

/// Serve a **placement pool** node (D52): host CP-assigned units from many indexes over one gRPC
/// endpoint.
#[allow(clippy::too_many_arguments)]
async fn serve_pool(
    data_dir: &str,
    indexes: &[String],
    addr: &str,
    register: Option<&str>,
    advertise_addr: Option<&str>,
    metrics_addr: Option<&str>,
    compact_interval_secs: u64,
    service_token: Option<String>,
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> anyhow::Result<()> {
    use growlerdb_engine::{
        AdminService, Gateway, LocalNode, LookupService, Node, OnNewWindow, PoolAdminService,
        PoolLookupService, PoolSearchService, PoolSuggestService, PoolWriteService, SearchService,
        ShardHandle, SharedSearchWindows, SuggestService, WindowSeed, WindowedWriteService,
    };
    use growlerdb_index::ShardId;
    use growlerdb_proto::v1::ServedWindow;
    use growlerdb_proto::{SystemServer, SystemService};
    use std::collections::BTreeMap;
    use std::sync::{Arc, RwLock};
    use tonic::transport::Server;

    let store = growlerdb_index::LocalIndexStore::open(data_dir)?;
    // The per-index window maps behind the four Pool read multiplexers (index → window → service),
    // plus the per-index writers behind the Pool write multiplexer (index → windowed writer).
    let search_idx: growlerdb_engine::SharedSearchIndexes = Arc::new(RwLock::new(BTreeMap::new()));
    let suggest_idx: growlerdb_engine::SharedSuggestIndexes =
        Arc::new(RwLock::new(BTreeMap::new()));
    let lookup_idx: growlerdb_engine::SharedLookupIndexes = Arc::new(RwLock::new(BTreeMap::new()));
    let admin_idx: growlerdb_engine::SharedAdminIndexes = Arc::new(RwLock::new(BTreeMap::new()));
    let write_idx: growlerdb_engine::SharedWriteIndexes = Arc::new(RwLock::new(BTreeMap::new()));
    // The per-index `ordinal → WriteService` map behind the hash half of the Pool write multiplexer
    // (D52): a hash-sharded index's writes are ordinal-routed, not window-partitioned.
    let write_hash_idx: growlerdb_engine::SharedHashWriteIndexes =
        Arc::new(RwLock::new(BTreeMap::new()));
    // Per-index unit kind for the Pool services: `false` = windowed (routes on `window`), `true` =
    // hash-sharded (routes on the `shard` ordinal). Seeded `false`, resolved per index below.
    let kinds: growlerdb_engine::SharedIndexKinds = Arc::new(RwLock::new(
        indexes.iter().map(|i| (i.clone(), false)).collect(),
    ));

    // Node-side write fence (357.12): under CP assignments (`--register` + `--advertise-addr`) each
    // writer refuses writes/checkpoints for `(index, window)` units not assigned to it as PRIMARY
    // (NOT_PRIMARY) — updated from each pushed assignment snapshot. Standalone it stays unrestricted.
    let fence = if register.is_some() && advertise_addr.is_some() {
        growlerdb_engine::PrimaryFence::fenced()
    } else {
        growlerdb_engine::PrimaryFence::unrestricted()
    };
    // Per-index (resolved def, writer) for the CP announce when `--register` is set: the writer's
    // live `served_windows()` is read each re-announce, so a window created since boot is advertised.
    let mut announcements: Vec<(growlerdb_core::ResolvedIndex, WindowedWriteService)> = Vec::new();
    // Per hash index (def, total shard count, held ordinals) for the CP announce: reports served
    // ordinals, not windows, so the gateway can place a ShardNode per ordinal. Held set fixed at boot.
    let mut hash_announcements: Vec<(growlerdb_core::ResolvedIndex, u32, Vec<u32>)> = Vec::new();
    // Per hash index, the (def, held hot ordinal handles) served locally — each ordinal's primary
    // periodically publishes a frozen snapshot to object storage so replicas open it read-through
    // (D53). Publish loops spawn once the object store is known (below).
    #[allow(clippy::type_complexity)]
    let mut hash_hot_ordinals: Vec<(
        growlerdb_core::ResolvedIndex,
        Vec<(u32, growlerdb_engine::ShardHandle)>,
    )> = Vec::new();
    // Per-index (def, source table, heavy budget) the D53 replica reconcile needs to open an
    // assigned replica window read-through and publish it into this index's maps.
    let mut replica_meta: ReplicaIndexMeta = std::collections::HashMap::new();
    // Per-index fair share of the node-wide heavy-read budget (D52 pool fairness, 357.25): each
    // co-resident index gets an equal soft share so a flood on one can't starve another, while
    // staying work-conserving (overflow into free capacity). Denominator is the LIVE served-index
    // count, shared with the assignment reconcile so it tracks the dispatch map (D53), not boot.
    let live_indexes = Arc::new(std::sync::atomic::AtomicUsize::new(indexes.len().max(1)));
    let mut total_windows = 0usize;
    let mut total_ordinals = 0usize;
    for index in indexes {
        let resolved = load_resolved(data_dir, index)?;
        let table = match &resolved.source {
            growlerdb_core::Source::Iceberg(s) => s.table.clone(),
        };
        // One shared per-index heavy-read share across all of this index's unit services.
        let index_heavy = growlerdb_engine::IndexHeavyShare::new(
            growlerdb_engine::heavy_reads_cap(),
            live_indexes.clone(),
        );
        // A HASH/partition-sharded index (no `windowing`): its units are ordinal shards. Open the
        // held ordinals into the per-index read maps + a per-ordinal writer, mark it hash in `kinds`
        // so the Pool services route on `shard`, and skip the windowed wiring below.
        if resolved.windowing.is_none() {
            let handles = open_pool_hash_index(
                &store,
                &resolved,
                &table,
                index_heavy.clone(),
                compact_interval_secs,
                &search_idx,
                &suggest_idx,
                &lookup_idx,
                &admin_idx,
                &write_hash_idx,
            )
            .await?;
            total_ordinals += handles.len();
            kinds
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .insert(index.clone(), true);
            // Advertised total shard count comes from the definition (authoritative), not the held
            // set — a read-through-only node (D53 replica) still announces the true count, so the CP
            // can place every ordinal and the gateway builds the full router.
            let held: Vec<u32> = handles.iter().map(|(o, _)| *o).collect();
            hash_announcements.push((resolved.clone(), resolved.shard_count, held));
            // Ordinals held HOT (primary): schedule a periodic frozen-snapshot publish (below, once
            // the store is known) so a replica opens it read-through and a node loss is a zero-gap
            // read failover (D53). A read-through-only node holds none, so it never double-publishes.
            hash_hot_ordinals.push((resolved.clone(), handles));
            // The D53 reconcile needs the def/table/budget to open an assigned replica ordinal
            // read-through and publish it into this index's maps.
            replica_meta.insert(index.clone(), (resolved, table, index_heavy));
            continue;
        }
        let windowing = resolved
            .windowing
            .clone()
            .expect("windowing present (checked above)");
        // Open each window shard into the per-window read services + an in-process node (backing the
        // write service's gateway swap), keyed as the writer's boot seed. Cold read-through windows
        // are not yet handled in pool mode — pool serving targets hot windows.
        let windows = store.window_shards(index)?;
        let (search_w, suggest_w, lookup_w, admin_w, seed, served) = {
            let (store, resolved, index_s, table, windows, index_heavy) = (
                store.clone(),
                resolved.clone(),
                index.clone(),
                table.clone(),
                windows.clone(),
                index_heavy.clone(),
            );
            tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
                let mut search_w = BTreeMap::new();
                let mut suggest_w = BTreeMap::new();
                let mut lookup_w = BTreeMap::new();
                let mut admin_w = BTreeMap::new();
                let mut seed: BTreeMap<i64, WindowSeed> = BTreeMap::new();
                let mut served = Vec::with_capacity(windows.len());
                for &w in &windows {
                    // Quarantine, don't crash (HA-G4): one corrupt window shard must not take down
                    // the whole multi-index pool process — log it loudly, skip it, and serve the
                    // rest. The CP sees the unit unserved and (with the dead-owner sweeper /
                    // re-announce) re-places it, so the pool self-heals. Only the PER-UNIT open is
                    // quarantined; a misconfiguration (bad --data-dir, unreadable index dir,
                    // unresolvable definition) still fails the boot via the `?`s above this loop.
                    let opened = store
                        .open_shard(&ShardId::window(&index_s, w), &resolved)
                        .and_then(|shard| {
                            let shard = Arc::new(shard);
                            let zone = shard.event_bounds()?;
                            Ok((shard, zone))
                        });
                    let (shard, zone) = match opened {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!(
                                "serve-pool: QUARANTINED {index_s}/w{w} — window shard failed to \
                                 open ({e}); serving the remaining windows (repair or delete the \
                                 shard dir; a registered pool's control plane will re-place the \
                                 unit)"
                            );
                            continue;
                        }
                    };
                    let handle = ShardHandle::new(shard);
                    search_w.insert(
                        w,
                        SearchService::new(handle.clone())
                            .with_index_heavy_share(index_heavy.clone()),
                    );
                    suggest_w.insert(w, SuggestService::new(handle.clone()));
                    lookup_w.insert(
                        w,
                        LookupService::new(
                            handle.clone(),
                            IcebergConfig::from_env(),
                            table.clone(),
                            resolved.clone(),
                        ),
                    );
                    admin_w.insert(w, AdminService::new(handle.clone(), &index_s));
                    // In-process node fronting this window (the write service swaps its windowed
                    // gateway over these on new-window creation; not served over REST in pool mode).
                    let node: Arc<dyn Node> = LocalNode::new(
                        SearchService::new(handle.clone()),
                        SuggestService::new(handle.clone()),
                        LookupService::new(
                            handle.clone(),
                            IcebergConfig::from_env(),
                            table.clone(),
                            resolved.clone(),
                        ),
                        AdminService::new(handle.clone(), &index_s),
                    )
                    .shared();
                    seed.insert(w, (handle, node, zone));
                    served.push(ServedWindow {
                        window: w,
                        event_min: zone.map(|(lo, _)| lo).unwrap_or(0),
                        event_max: zone.map(|(_, hi)| hi).unwrap_or(0),
                        has_event_bounds: zone.is_some(),
                        cold: false,
                    });
                }
                Ok((search_w, suggest_w, lookup_w, admin_w, seed, served))
            })
            .await??
        };
        total_windows += seed.len();

        // The shared window maps: the SAME Arcs back both the Pool read services and this index's
        // writer, so a window the writer creates on first write is immediately queryable.
        let search_shared: SharedSearchWindows = Arc::new(RwLock::new(search_w));
        let suggest_shared = Arc::new(RwLock::new(suggest_w));
        let lookup_shared = Arc::new(RwLock::new(lookup_w));
        let admin_shared = Arc::new(RwLock::new(admin_w));
        search_idx
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(index.clone(), search_shared.clone());
        suggest_idx
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(index.clone(), suggest_shared.clone());
        lookup_idx
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(index.clone(), lookup_shared.clone());
        admin_idx
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(index.clone(), admin_shared.clone());

        // The write service's in-process windowed gateway (rebuilt on new-window creation) + a
        // per-window auto-compaction loop for the boot windows.
        let nodes: Vec<Arc<dyn Node>> = seed.values().map(|(_, n, _)| n.clone()).collect();
        // `(window, event-zone, cold=false)` descriptors for the writer's in-process gateway.
        let descriptors = seed
            .iter()
            .map(|(w, (_, _, z))| (*w, *z, false))
            .collect::<Vec<_>>();
        for (w, (handle, _, _)) in &seed {
            spawn_auto_compaction(
                handle.clone(),
                format!("{index} w{w}"),
                compact_interval_secs,
            );
        }
        let gateway = Arc::new(Gateway::windowed(nodes, windowing.clone(), descriptors));
        let on_new_window: OnNewWindow = {
            let idx = index.clone();
            let ci = compact_interval_secs;
            Arc::new(move |w, handle| spawn_auto_compaction(handle, format!("{idx} w{w}"), ci))
        };
        let write_service = WindowedWriteService::new(
            store.clone(),
            resolved.clone(),
            table.clone(),
            IcebergConfig::from_env(),
            seed,
            search_shared,
            suggest_shared,
            lookup_shared,
            admin_shared,
            gateway,
            on_new_window,
        )?
        .with_primary_fence(fence.clone());
        write_idx
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(index.clone(), write_service.clone());
        replica_meta.insert(
            index.clone(),
            (resolved.clone(), table.clone(), index_heavy.clone()),
        );
        announcements.push((resolved, write_service));
        let _ = served; // (per-window ServedWindow now re-read from the writer at announce time)
    }

    // D53 assignment subscription: every snapshot updates the write fence (needs only the CP); with
    // a backup object store it also opens each assigned *replica* window read-through, so a re-placed
    // holder answers without a rebuild. Absent the store, it fences writes but serves only its
    // local/primary windows.
    let mut replica_capable = false;
    // Backup object store (S3 or local fs), shared by the replica read-through path and the primary's
    // hash-ordinal publish loops below. `Err` when neither backup env var is set.
    let object_store = object_store_from_env();
    if let (Some(cp), Some(endpoint)) = (register, advertise_addr) {
        let replica_root = std::path::PathBuf::from(data_dir).join(".replica");
        let replica = match &object_store {
            Ok(op) => Some(ReplicaServing {
                meta: replica_meta.clone(),
                search_idx: search_idx.clone(),
                suggest_idx: suggest_idx.clone(),
                lookup_idx: lookup_idx.clone(),
                admin_idx: admin_idx.clone(),
                store: store.clone(),
                op: op.clone(),
                cache: growlerdb_index::RangeCache::new(cold_cache_bytes()),
                replica_root: replica_root.clone(),
                live_indexes: live_indexes.clone(),
            }),
            Err(e) => {
                eprintln!(
                    "serve-pool: replica failover disabled — no object store ({e}); the node \
                     registers as NOT replica-capable, so the control plane will not assign \
                     replica units to it. Set GROWLERDB_BACKUP_BUCKET (S3) or \
                     GROWLERDB_OBJECT_STORE_FS (local dir) to enable D53 replica serving (write \
                     fencing stays active)"
                );
                None
            }
        };
        // The heartbeat's replica-capability declaration (HA-G2) mirrors whether the replica
        // serving path above is actually wired.
        replica_capable = replica.is_some();
        // Primary side of D53 hash replication: for each hash ordinal held HOT, publish a frozen
        // snapshot to object storage on an interval so a replica opens it read-through (zero-gap read
        // failover). A read-through-only node holds no hot ordinals, so there's one publisher each.
        if let Ok(op) = &object_store {
            let interval = pool_replicate_interval_secs();
            for (resolved, handles) in &hash_hot_ordinals {
                for (ordinal, handle) in handles {
                    spawn_shard_replicate(
                        handle.clone(),
                        store.clone(),
                        op.clone(),
                        resolved.clone(),
                        *ordinal,
                        interval,
                    );
                }
            }
        }
        // Build-on-assignment (D52 dynamic assignment): a node assigned primary of a hash ordinal it
        // doesn't hold cold-builds it from source and serves it hot, so N interchangeable nodes run
        // one config and each builds the ordinals the CP gives it. Enabled whenever registered.
        let building = match growlerdb_engine::Engine::open(data_dir, IcebergConfig::from_env()) {
            Ok(engine) => Some(PrimaryBuilding {
                engine,
                store: store.clone(),
                meta: replica_meta,
                kinds: kinds.clone(),
                search_idx: search_idx.clone(),
                suggest_idx: suggest_idx.clone(),
                lookup_idx: lookup_idx.clone(),
                admin_idx: admin_idx.clone(),
                write_hash_idx: write_hash_idx.clone(),
                object_store: object_store.as_ref().ok().cloned(),
                compact_interval_secs,
                replicate_interval_secs: pool_replicate_interval_secs(),
                building: std::sync::Arc::new(std::sync::Mutex::new(
                    std::collections::HashSet::new(),
                )),
            }),
            Err(e) => {
                eprintln!(
                    "serve-pool: build-on-assignment disabled — engine open failed ({e}); the node \
                     serves only ordinals it holds locally"
                );
                None
            }
        };
        spawn_assignment_reconcile(
            cp.to_string(),
            endpoint.to_string(),
            fence.clone(),
            AssignmentUnload {
                write_idx: write_idx.clone(),
                replica_root,
            },
            replica,
            building,
        );
    }

    let socket: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --addr `{addr}`: {e}"))?;
    println!(
        "growlerdb serve-pool: {} index(es) [{}], {total_windows} window(s) + {total_ordinals} \
         ordinal shard(s) total, on {socket}",
        indexes.len(),
        indexes.join(", ")
    );
    if service_token.is_none() {
        eprintln!(
            "serve-pool: WARNING data-plane gRPC is open (set GROWLERDB_SERVICE_TOKEN to close it)"
        );
    }
    let readiness = spawn_health(metrics_addr).await?;
    // Register into the placement pool + announce every served index's windows so a cluster gateway
    // routes here; `/readyz` stays not-ready until the first registration. Without `--register` the
    // node serves immediately (standalone).
    if let (Some(cp), Some(endpoint)) = (register, advertise_addr) {
        let label = format!("pool node at {endpoint} [{}]", indexes.join(", "));
        spawn_pool_registration(
            cp.to_string(),
            endpoint.to_string(),
            announcements,
            hash_announcements,
            replica_capable,
            readiness.clone(),
            label,
        );
    } else {
        readiness.mark_ready();
    }

    let mut builder = Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
    }
    builder
        .layer(growlerdb_engine::service_token_layer(service_token))
        .add_service(PoolSearchService::new(search_idx, kinds.clone()).into_server())
        .add_service(PoolSuggestService::new(suggest_idx, kinds.clone()).into_server())
        .add_service(PoolLookupService::new(lookup_idx, kinds.clone()).into_server())
        .add_service(PoolAdminService::new(admin_idx, kinds.clone()).into_server())
        .add_service(PoolWriteService::new(write_idx, write_hash_idx, kinds).into_server())
        .add_service(SystemServer::new(SystemService::new(VERSION)))
        // SIGINT *or* SIGTERM (HA-G4): plain Kubernetes stops a pod with SIGTERM, so both must drain
        // gracefully. There is no deregistration RPC (a node leaves by ceasing to heartbeat and aging
        // out of the liveness TTL); the dead-owner sweeper re-places its units after the TTL.
        .serve_with_shutdown(socket, shutdown_signal())
        .await?;
    println!("growlerdb serve-pool: shut down cleanly");
    Ok(())
}

/// Resolve when the process receives **SIGINT (Ctrl-C) or SIGTERM** — the serve-pool shutdown
/// trigger. `ctrl_c` alone misses SIGTERM, which is what a plain Kubernetes pod stop (and most
/// process supervisors) send; missing it means an ungraceful kill mid-request. Non-unix targets
/// keep the Ctrl-C-only behavior (there is no SIGTERM there).
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        match signal(SignalKind::terminate()) {
            Ok(mut term) => {
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = term.recv() => {}
                }
            }
            // Installing the SIGTERM handler can only really fail in exotic environments; fall
            // back to Ctrl-C-only rather than refusing to serve.
            Err(_) => {
                let _ = ctrl_c.await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}

/// Per-index context a pool node needs to open an assigned **replica** window (D53): its resolved
/// definition, source table (for hydration), and the per-index heavy-read share the replica's
/// `SearchService` shares. Keyed by index name.
type ReplicaIndexMeta = std::collections::HashMap<
    String,
    (
        growlerdb_core::ResolvedIndex,
        String,
        std::sync::Arc<growlerdb_engine::IndexHeavyShare>,
    ),
>;

/// Everything the assignment loop needs to open + publish an assigned **replica** window (D53) —
/// `None` in [`spawn_assignment_reconcile`] when no backup object store is configured: the write
/// fence still updates from every snapshot, only the replica read-through serving is disabled.
struct ReplicaServing {
    meta: ReplicaIndexMeta,
    search_idx: growlerdb_engine::SharedSearchIndexes,
    suggest_idx: growlerdb_engine::SharedSuggestIndexes,
    lookup_idx: growlerdb_engine::SharedLookupIndexes,
    admin_idx: growlerdb_engine::SharedAdminIndexes,
    store: growlerdb_index::LocalIndexStore,
    op: growlerdb_backup::Operator,
    cache: growlerdb_index::RangeCache,
    replica_root: std::path::PathBuf,
    /// The heavy-read share denominator (357.25): refreshed to the dispatch map's size after each
    /// applied snapshot, so per-index fair shares track the LIVE served index set as assignments
    /// change (D53) instead of the boot-time index count.
    live_indexes: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// What the assignment loop needs to **unload de-assigned units** (HA-G1) — always present when the
/// node runs from CP assignments, independent of whether replica serving is enabled: even a
/// fence-only node must stop *reading* for a unit the CP moved away.
struct AssignmentUnload {
    /// The per-index writers: [`WindowedWriteService::unload_window`] removes a window from the
    /// shared read mux maps + the writer's states and rebuilds the in-process gateway.
    write_idx: growlerdb_engine::SharedWriteIndexes,
    /// `{data_dir}/.replica` — a de-assigned unit's read-through scratch is deleted from here
    /// (after a short drain), and orphans from previous runs are swept once the first snapshot
    /// arrives.
    replica_root: std::path::PathBuf,
}

/// What the assignment loop needs to **build a hash unit on assignment** (D52 dynamic assignment): a
/// pool node the CP assigns **primary** of a hash ordinal it doesn't hold cold-builds that ordinal
/// from source and serves it hot — so the operator points N interchangeable nodes at the pool with a
/// uniform config (no per-node build/primary designation) and each builds the ordinals the CP gives
/// it. `None` when the node has no Iceberg source configured to build from.
struct PrimaryBuilding {
    /// Builds one ordinal from source ([`Engine::index_shard_resolved`](growlerdb_engine::Engine::index_shard_resolved)).
    engine: growlerdb_engine::Engine,
    store: growlerdb_index::LocalIndexStore,
    /// Per served index: (resolved def, source table, heavy-read share) — the build + open inputs.
    meta: ReplicaIndexMeta,
    kinds: growlerdb_engine::SharedIndexKinds,
    search_idx: growlerdb_engine::SharedSearchIndexes,
    suggest_idx: growlerdb_engine::SharedSuggestIndexes,
    lookup_idx: growlerdb_engine::SharedLookupIndexes,
    admin_idx: growlerdb_engine::SharedAdminIndexes,
    write_hash_idx: growlerdb_engine::SharedHashWriteIndexes,
    /// The object store the built ordinal's publish loop ships snapshots to (so replicas can
    /// read-through). `None` disables publishing (no replicas can be fed).
    object_store: Option<growlerdb_backup::Operator>,
    compact_interval_secs: u64,
    replicate_interval_secs: u64,
    /// Ordinals a build task is in-flight for — so a repeated snapshot doesn't launch a second build
    /// of the same unit. Cleared when the build task finishes (success or failure).
    building: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<(String, u32)>>>,
}

/// Reconnect backoff for the assignment stream (HA-G4): jittered exponential, so a CP restart
/// doesn't re-subscribe the whole fleet in 3-second lockstep. Reset on every received snapshot.
const ASSIGN_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
const ASSIGN_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);
/// Drain grace before a de-assigned unit's `.replica` scratch is deleted: in-flight requests hold
/// the shard `Arc` (dropping the mux entry never aborts them), and a few seconds keeps the files
/// under any read that was already mid-flight when the unit unloaded. A re-assignment later simply
/// re-downloads the sidecars (`open_cold_replica` recreates the scratch dir).
const SCRATCH_DRAIN_GRACE: std::time::Duration = std::time::Duration::from_secs(3);
/// How often the assignment loop **re-attempts** building/opening assigned units the last snapshot
/// couldn't complete — a replica whose primary hadn't yet published its cold marker at the push, or a
/// build that failed. The CP pushes only on placement *changes*, so without this a unit that becomes
/// serveable *after* its push (the primary builds + publishes async) would wait for the next unrelated
/// placement change. The re-attempt is idempotent (already-served units are skipped).
const RECONCILE_RETRY_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Serve the units this node is assigned in `units`: build any hash **primary** ordinal it doesn't
/// hold ([`reconcile_primary_builds`], D52), open any assigned **replica** unit read-through
/// ([`reconcile_replica_units`], D53), and refresh the heavy-read share denominator. Idempotent — an
/// already-built/served unit is skipped — so it runs both on each pushed snapshot **and** on the
/// periodic [`RECONCILE_RETRY_INTERVAL`] retry.
async fn serve_assigned_units(
    units: &[growlerdb_proto::v1::UnitAssignment],
    building: &Option<PrimaryBuilding>,
    replica: &Option<ReplicaServing>,
) {
    if let Some(pb) = building {
        reconcile_primary_builds(units, pb);
    }
    if let Some(r) = replica {
        reconcile_replica_units(
            units,
            &r.meta,
            &r.search_idx,
            &r.suggest_idx,
            &r.lookup_idx,
            &r.admin_idx,
            &r.store,
            &r.op,
            &r.cache,
            &r.replica_root,
        )
        .await;
        // Refresh the heavy-read share denominator to the LIVE served index set (357.25): the dispatch
        // map is what actually serves reads, so per-index fair shares track it as assignments change.
        let live = r
            .search_idx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .len()
            .max(1);
        r.live_indexes
            .store(live, std::sync::atomic::Ordering::Release);
    }
}

/// Subscribe to CP **assignment pushes** (D53) and apply each pushed snapshot: first swap the
/// node's primary-holder **write fence** (357.12 — so a unit whose primary moved away is refused on
/// the very next snapshot), then **unload de-assigned units** (HA-G1 — window units this node
/// served *because of a previous snapshot* that the new snapshot no longer assigns to it, in either
/// role: drop their mux entries + writer state and delete their `.replica` scratch after a short
/// drain; boot windows the CP never assigned are left alone), and finally serve any newly-assigned
/// replica windows via [`reconcile_replica_units`]. The first snapshot also sweeps `.replica`
/// scratch orphaned by previous runs. Reconnects with jittered exponential backoff if the stream
/// drops (the CP re-sends a full snapshot on resubscribe, so nothing is lost).
fn spawn_assignment_reconcile(
    cp: String,
    endpoint: String,
    fence: growlerdb_engine::PrimaryFence,
    unload: AssignmentUnload,
    replica: Option<ReplicaServing>,
    building: Option<PrimaryBuilding>,
) {
    let replica_capable = replica.is_some();
    tokio::spawn(async move {
        // Window units assigned by a PREVIOUSLY applied snapshot. Only these are ever unloaded: a
        // boot window the CP never assigned must not be yanked by a snapshot that doesn't know it.
        // Survives reconnects (each snapshot is full).
        let mut assignment_seen: std::collections::HashSet<(String, i64)> =
            std::collections::HashSet::new();
        let mut orphans_swept = false;
        let mut backoff = ASSIGN_INITIAL_BACKOFF;
        loop {
            if let Err(e) = async {
                let mut client = connect_cp(&cp, false).await?;
                // Heartbeat FIRST (idempotent): `SubscribeAssignments` refuses an endpoint with no
                // live registration (FAILED_PRECONDITION), so racing the parallel registration loop
                // would burn this loop's backoff. The extra heartbeat is free (in-memory).
                client
                    .register_node(growlerdb_proto::v1::RegisterNodeRequest {
                        endpoint: endpoint.clone(),
                        replica_capable,
                    })
                    .await?;
                let mut stream = client
                    .subscribe_assignments(growlerdb_proto::v1::SubscribeAssignmentsRequest {
                        endpoint: endpoint.clone(),
                    })
                    .await?
                    .into_inner();
                // Full snapshot on subscribe + every placement change; reconcile each. Between pushes
                // a retry tick re-attempts units not yet serveable (a replica waiting on its primary's
                // marker, a failed build) — the CP pushes only on *changes*, so retry locally.
                let mut last_units: Vec<growlerdb_proto::v1::UnitAssignment> = Vec::new();
                let mut retry = tokio::time::interval(RECONCILE_RETRY_INTERVAL);
                retry.tick().await; // consume the immediate first tick
                loop {
                    tokio::select! {
                        msg = stream.message() => {
                            let Some(snapshot) = msg? else { break };
                            backoff = ASSIGN_INITIAL_BACKOFF; // a live stream re-arms the reconnect backoff
                            // Fence first: the write path must see the tightened primary set before (and
                            // regardless of whether) any replica unit is opened or unloaded.
                            fence.apply_snapshot(&snapshot.units);
                            // This node's current window-unit set (either role) from the snapshot.
                            use growlerdb_proto::v1::unit_assignment::Unit as WireUnit;
                            let current: std::collections::HashSet<(String, i64)> = snapshot
                                .units
                                .iter()
                                .filter_map(|u| match u.unit {
                                    Some(WireUnit::Window(w)) => Some((u.index.clone(), w)),
                                    _ => None,
                                })
                                .collect();
                            // HA-G1: unload units a previous snapshot assigned here that this one doesn't.
                            for (index, window) in assignment_seen.iter().filter(|u| !current.contains(u)) {
                                unload_unit(&unload, index, *window);
                            }
                            // The first snapshot is the authority on which PREVIOUS-run `.replica`
                            // scratch dirs are still assigned — sweep the rest (a blind startup sweep
                            // would race the subscription and delete still-assigned scratch).
                            if !orphans_swept {
                                orphans_swept = true;
                                let root = unload.replica_root.clone();
                                let keep = current.clone();
                                let _ = tokio::task::spawn_blocking(move || {
                                    sweep_orphan_replica_scratch(&root, &keep)
                                })
                                .await;
                            }
                            assignment_seen = current;
                            serve_assigned_units(&snapshot.units, &building, &replica).await;
                            last_units = snapshot.units;
                        }
                        _ = retry.tick() => {
                            // Retry not-yet-served assigned units (idempotent). No-op until the first
                            // snapshot arrives, and cheap once everything is served (all skipped).
                            if !last_units.is_empty() {
                                serve_assigned_units(&last_units, &building, &replica).await;
                            }
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .await
            {
                eprintln!(
                    "serve-pool replica: assignment subscription dropped ({e}); reconnecting"
                );
            }
            // Jittered exponential backoff (matches the registration loop's idiom) — a fixed sleep
            // would re-subscribe every node of a fleet in lockstep after a CP restart.
            tokio::time::sleep(jittered(backoff, 0.2)).await;
            backoff = (backoff * 2).min(ASSIGN_MAX_BACKOFF);
        }
    });
}

/// Unload one de-assigned `(index, window)` unit (HA-G1): remove it from the read mux maps and the
/// writer's window states (via [`WindowedWriteService::unload_window`] — in-flight requests hold
/// the shard `Arc`, so this only unpublishes), then delete its `.replica` read-through scratch
/// after [`SCRATCH_DRAIN_GRACE`]. An index this node was never started with has nothing loaded —
/// a no-op.
fn unload_unit(unload: &AssignmentUnload, index: &str, window: i64) {
    let writer = unload
        .write_idx
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .get(index)
        .cloned();
    if let Some(writer) = writer {
        if writer.unload_window(window) {
            println!(
                "serve-pool: unloaded {index}/w{window} — the control plane de-assigned it from \
                 this node"
            );
        }
    }
    let scratch = unload.replica_root.join(index).join(format!("w{window}"));
    if scratch.exists() {
        tokio::spawn(async move {
            tokio::time::sleep(SCRATCH_DRAIN_GRACE).await;
            let _ = tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&scratch)).await;
        });
    }
}

/// Delete `.replica/{index}/w{N}` scratch dirs for units NOT in `keep` — the first-snapshot sweep
/// of scratch orphaned by previous runs (a node restarted after de-assignments it never processed).
/// Best-effort: an unreadable root (usually: no replica ever served, so no dir) is a no-op.
fn sweep_orphan_replica_scratch(
    replica_root: &std::path::Path,
    keep: &std::collections::HashSet<(String, i64)>,
) {
    let Ok(indexes) = std::fs::read_dir(replica_root) else {
        return;
    };
    for index_entry in indexes.flatten() {
        let index = index_entry.file_name().to_string_lossy().to_string();
        let Ok(windows) = std::fs::read_dir(index_entry.path()) else {
            continue;
        };
        for w_entry in windows.flatten() {
            let name = w_entry.file_name().to_string_lossy().to_string();
            let Some(w) = name.strip_prefix('w').and_then(|s| s.parse::<i64>().ok()) else {
                continue;
            };
            if !keep.contains(&(index.clone(), w))
                && std::fs::remove_dir_all(w_entry.path()).is_ok()
            {
                println!("serve-pool: removed orphaned replica scratch {index}/w{w}");
            }
        }
    }
}

/// Reconcile one pushed assignment snapshot (D53): for each **replica unit** assigned to this node
/// that it isn't already serving, fetch the unit's [`ColdMarker`](growlerdb_index::ColdMarker) from
/// object storage and open it **read-through** ([`open_cold_replica`](growlerdb_index::LocalIndexStore::open_cold_replica)),
/// publishing it into the same per-index unit maps the Pool read services front — so the gateway's
/// failover routing reaches it. Handles **both** unit kinds: a windowed index's cold **windows** (park
/// prefix `cold/{index}/w{window}`) and a hash index's **ordinal shards** (`cold/{index}/{ordinal}`,
/// published by [`backup_replica_snapshot`](growlerdb_backup::backup_replica_snapshot)) — the maps are
/// `i64`-keyed either way, and an index is all one kind, so the open/publish path is shared. Snapshots
/// are idempotent: an already-served or not-yet-published unit is skipped (an over-served unit is
/// harmless — the CP just won't route to it). Returns the number of units newly served.
#[allow(clippy::too_many_arguments)]
async fn reconcile_replica_units(
    units: &[growlerdb_proto::v1::UnitAssignment],
    meta: &ReplicaIndexMeta,
    search_idx: &growlerdb_engine::SharedSearchIndexes,
    suggest_idx: &growlerdb_engine::SharedSuggestIndexes,
    lookup_idx: &growlerdb_engine::SharedLookupIndexes,
    admin_idx: &growlerdb_engine::SharedAdminIndexes,
    store: &growlerdb_index::LocalIndexStore,
    op: &growlerdb_backup::Operator,
    cache: &growlerdb_index::RangeCache,
    replica_root: &std::path::Path,
) -> usize {
    use growlerdb_engine::{
        AdminService, LookupService, SearchService, ShardHandle, SuggestService,
    };
    use growlerdb_proto::v1::unit_assignment::Unit as WireUnit;
    use std::sync::Arc;
    let mut served = 0usize;
    for u in units {
        // The unit's map key + object-store park prefix + `.replica` scratch subdir + label. A
        // windowed index's units are cold (parked) windows; a hash index's are ordinal shards (a
        // frozen `backup_replica_snapshot`). Both serve read-through here. A HOT window / unpublished
        // shard has no marker and is skipped, retried on the next snapshot.
        let (key, prefix, scratch_sub, label) = match u.unit {
            Some(WireUnit::Window(w)) => (
                w,
                format!("cold/{}/w{}", u.index, w),
                format!("w{w}"),
                format!("w{w}"),
            ),
            Some(WireUnit::Shard(o)) => (
                o as i64,
                format!("cold/{}/{}", u.index, o),
                o.to_string(),
                format!("s{o}"),
            ),
            None => continue,
        };
        // This node must have been started with `--index {u.index}` (so it holds the def + maps).
        let Some((resolved, table, heavy)) = meta.get(&u.index) else {
            continue;
        };
        let Some(search_units) = search_idx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&u.index)
            .cloned()
        else {
            continue;
        };
        if search_units
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .contains_key(&key)
        {
            continue; // already serving this unit
        }
        // Fetch the marker + open the unit read-through (blocking → off the async runtime).
        let marker = match growlerdb_backup::fetch_cold_marker(op, &prefix).await {
            Ok(Some(m)) => m,
            Ok(None) => continue, // not published yet — retry on the next snapshot
            Err(e) => {
                eprintln!("serve-pool replica: marker fetch {prefix}: {e}");
                continue;
            }
        };
        let scratch = replica_root.join(&u.index).join(&scratch_sub);
        let (store2, resolved2, op2, cache2) =
            (store.clone(), resolved.clone(), op.clone(), cache.clone());
        let opened = tokio::task::spawn_blocking(move || {
            store2.open_cold_replica(&resolved2, &scratch, op2, &marker, cache2)
        })
        .await;
        let shard = match opened {
            Ok(Ok(s)) => s,
            Ok(Err(e)) => {
                eprintln!("serve-pool replica: open {}/{label}: {e}", u.index);
                continue;
            }
            Err(e) => {
                eprintln!("serve-pool replica: open task {}/{label}: {e}", u.index);
                continue;
            }
        };
        let handle = ShardHandle::new(Arc::new(shard));
        // Publish into the four per-index maps (the SAME Arcs the Pool read services front). Insert
        // only if still absent (HA-A4): the slow cold open may have raced the write path creating this
        // unit HOT — the re-check under the write lock makes the hot unit win, discarding the cold one.
        {
            let mut sw = search_units.write().unwrap_or_else(|e| e.into_inner());
            if sw.contains_key(&key) {
                eprintln!(
                    "serve-pool replica: {}/{label} appeared (hot) during the cold open — \
                     keeping the live unit, discarding the stale cold replica",
                    u.index
                );
                continue;
            }
            sw.insert(
                key,
                SearchService::new(handle.clone()).with_index_heavy_share(heavy.clone()),
            );
        }
        if let Some(m) = suggest_idx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&u.index)
            .cloned()
        {
            m.write()
                .unwrap_or_else(|e| e.into_inner())
                .entry(key)
                .or_insert_with(|| SuggestService::new(handle.clone()));
        }
        if let Some(m) = lookup_idx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&u.index)
            .cloned()
        {
            m.write()
                .unwrap_or_else(|e| e.into_inner())
                .entry(key)
                .or_insert_with(|| {
                    LookupService::new(
                        handle.clone(),
                        IcebergConfig::from_env(),
                        table.clone(),
                        resolved.clone(),
                    )
                });
        }
        if let Some(m) = admin_idx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&u.index)
            .cloned()
        {
            m.write()
                .unwrap_or_else(|e| e.into_inner())
                .entry(key)
                .or_insert_with(|| AdminService::new(handle.clone(), &u.index));
        }
        served += 1;
        println!(
            "serve-pool replica: serving {}/{label} read-through (D53)",
            u.index
        );
    }
    served
}

/// Reconcile one pushed snapshot's **primary** assignments by **building on assignment** (D52 dynamic
/// assignment): for each hash ordinal the CP assigns **this node as primary** that it doesn't already
/// hold, spawn a task that cold-builds the ordinal from source
/// ([`Engine::index_shard_resolved`](growlerdb_engine::Engine::index_shard_resolved)), publishes it
/// into the pool maps + writer ([`open_and_publish_ordinal`]), and starts its snapshot publish loop so
/// replicas can read-through. The build runs off the reconcile loop (it reads the source and can be
/// slow); an in-flight guard ([`PrimaryBuilding::building`]) stops a repeated snapshot from
/// double-building. **Single-shard only** today (`shard_count == 1`, ordinal 0 — the demo case), since
/// the cold build writes to `ShardId::single`; a multi-ordinal build is a follow-up.
fn reconcile_primary_builds(units: &[growlerdb_proto::v1::UnitAssignment], pb: &PrimaryBuilding) {
    use growlerdb_proto::v1::unit_assignment::Unit as WireUnit;
    for u in units {
        // Only units the CP assigns THIS node as PRIMARY, and only hash ordinals.
        if !u.primary {
            continue;
        }
        let Some(WireUnit::Shard(ordinal)) = u.unit else {
            continue;
        };
        // The node must serve this index as a hash index (started with `--index`).
        let is_hash = pb
            .kinds
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&u.index)
            .copied()
            .unwrap_or(false);
        if !is_hash {
            continue;
        }
        let Some((resolved, table, heavy)) = pb.meta.get(&u.index) else {
            continue;
        };
        // Single-shard build-on-assignment only (see the doc): ordinal 0 of a 1-shard index.
        if resolved.shard_count != 1 || ordinal != 0 {
            continue;
        }
        // Already serving this ordinal? Skip — UNLESS we serve it only as a read-through REPLICA (a
        // read-only cold shard) while the CP now assigns us PRIMARY. That replica→primary role change
        // isn't covered by the de-assignment path (`spawn_assignment_reconcile` tracks only Window
        // units), so a hash ordinal's stale replica would otherwise never be superseded and the node
        // serves the stale cold snapshot forever — stale once the source advances (e.g. after
        // `just demo-data` reloads the corpus). Fall through to build; `open_and_publish_ordinal`
        // then replaces the replica service with the freshly-built primary. A primary we already hold
        // is left alone (idempotent).
        let serving_as_primary = pb
            .search_idx
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(&u.index)
            .and_then(|m| {
                m.read()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(&(ordinal as i64))
                    .map(|svc| !svc.serves_read_only())
            });
        if serving_as_primary == Some(true) {
            continue;
        }
        // In-flight guard: mark this ordinal building; if already marked, another task has it.
        {
            let mut b = pb.building.lock().unwrap_or_else(|e| e.into_inner());
            if !b.insert((u.index.clone(), ordinal)) {
                continue;
            }
        }
        // Build off the reconcile loop (source read; can be slow), then publish + start replication.
        let engine = pb.engine.clone();
        let store = pb.store.clone();
        let resolved = resolved.clone();
        let table = table.clone();
        let heavy = heavy.clone();
        let (search_idx, suggest_idx, lookup_idx, admin_idx, write_hash_idx) = (
            pb.search_idx.clone(),
            pb.suggest_idx.clone(),
            pb.lookup_idx.clone(),
            pb.admin_idx.clone(),
            pb.write_hash_idx.clone(),
        );
        let object_store = pb.object_store.clone();
        let (compact, replicate) = (pb.compact_interval_secs, pb.replicate_interval_secs);
        let building = pb.building.clone();
        let index = u.index.clone();
        tokio::spawn(async move {
            let key = (index.clone(), ordinal);
            let result = async {
                let outcome = engine.index_shard_resolved(&resolved, &table).await?;
                println!(
                    "serve-pool: built {index}/s{ordinal} on assignment ({} doc(s), snapshot {}) — \
                     now serving as primary",
                    outcome.doc_count, outcome.snapshot.0
                );
                let handle = open_and_publish_ordinal(
                    &store,
                    &resolved,
                    &table,
                    ordinal,
                    heavy,
                    compact,
                    &search_idx,
                    &suggest_idx,
                    &lookup_idx,
                    &admin_idx,
                    &write_hash_idx,
                )
                .await?;
                // Publish snapshots so replicas can open this ordinal read-through (D53).
                if let Some(op) = object_store {
                    spawn_shard_replicate(
                        handle,
                        store.clone(),
                        op,
                        resolved.clone(),
                        ordinal,
                        replicate,
                    );
                }
                Ok::<(), anyhow::Error>(())
            }
            .await;
            if let Err(e) = result {
                eprintln!("serve-pool: build-on-assignment {index}/s{ordinal} failed ({e}); will retry on the next snapshot");
            }
            building
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&key);
        });
    }
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

/// A truthy env value: `1` / `true` / `yes` / `on` (case-insensitive, trimmed).
fn env_truthy(v: &str) -> bool {
    matches!(
        v.trim().to_ascii_lowercase().as_str(),
        "1" | "true" | "yes" | "on"
    )
}

/// Whether `GROWLERDB_REQUIRE_AUTH` demands the gateway refuse to start without an authenticator.
/// A prod safety guard: opt-in so dev/CI stay open by default.
fn require_auth_env() -> bool {
    std::env::var("GROWLERDB_REQUIRE_AUTH").is_ok_and(|v| env_truthy(&v))
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

    // Serve /healthz + /readyz BEFORE building the gateway: if it must wait for the control-plane at
    // boot, /healthz stays up (liveness passes, pod isn't killed) while /readyz stays not-ready until
    // the routing snapshot is in hand — up-but-not-ready during the wait, never CrashLoopBackOff.
    let readiness = spawn_health(metrics_addr).await?;

    // For `--registry` ordinal/alias indexes, remember what the hot-reload loop needs (registry
    // path, index, TLS clone) — spawned after the gateway is `Arc`-wrapped below. Windowed not reloaded.
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
        let gw = growlerdb_engine::Gateway::multi_index(resolver, None)
            .with_limits(growlerdb_engine::GatewayLimits::from_env());
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
                    growlerdb_engine::Gateway::new(Arc::new(node))
                        .with_limits(growlerdb_engine::GatewayLimits::from_env()),
                    format!("Node {node_addr}"),
                )
            }
            _ => {
                anyhow::bail!("provide --node-addr, --registry + --index, --control-plane + --index, or --all-indexes")
            }
        }
    };

    // An injected authenticator (out-of-tree build) takes precedence over the flag-driven auth
    // below; it carries its own methods, so just install it plus the standard RBAC.
    let gw = if let Some(authn) = injected_authn {
        println!("gateway: authentication via an injected authenticator");
        gw.with_authn(authn)
            .with_password_login(builtin_auth)
            .with_authz(Arc::new(growlerdb_engine::RbacPolicy::with_default_roles()))
    }
    // Optional OIDC/JWT: fetch the issuer's JWKS up front (a misconfigured issuer fails fast at
    // startup, not per request) and keep it fresh on a timer for key rotation.
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
        // Map verified roles to operation scopes and reject calls that lack them.
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
    } else if require_auth_env() {
        // Prod safety guard: refuse to start open when GROWLERDB_REQUIRE_AUTH is set, so a
        // misconfigured deployment fails fast instead of silently serving open. Opt-in.
        anyhow::bail!(
            "gateway refused to start: GROWLERDB_REQUIRE_AUTH is set but no authentication is \
             configured — pass --oidc-issuer or --builtin-auth (or unset GROWLERDB_REQUIRE_AUTH)"
        );
    } else {
        // Open mode. Warn through tracing (not stderr) so the telemetry exporter captures it and an
        // "open gateway in prod" alert is possible.
        tracing::warn!(
            "authentication is disabled (no --oidc-issuer / --builtin-auth); the gateway is OPEN. \
             Set GROWLERDB_REQUIRE_AUTH=1 to refuse starting without authentication."
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
    // MCP Streamable HTTP transport, mounted over the fully-merged /v1 surface (so agent tool
    // calls can reach the control-plane proxy's /v1/indexes too, when wired).
    router = with_mcp(router, gw.clone());
    println!("gateway: MCP Streamable HTTP transport on http://{rest_socket}/mcp");
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

/// Mount the MCP **Streamable HTTP transport** (`POST /mcp`) over a composed REST router.
/// Tool calls re-enter `v1` in-process, so call this LAST — after every `/v1` merge — so
/// everything mounted there (search, keys:get, facets, `/v1/indexes` when the control-plane
/// proxy is wired) is reachable through MCP under the same enforcement.
fn with_mcp(v1: axum::Router, gateway: std::sync::Arc<growlerdb_engine::Gateway>) -> axum::Router {
    v1.clone().merge(growlerdb_engine::mcp_router(v1, gateway))
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
    // A single windowed index routes per time window, not an ordinal shard map — front its windows
    // over `WindowNode`s so a time-filtered search prunes to the owning windows across nodes.
    if let Some(entry) = registry.get(name) {
        if let Some(windowing) = entry.definition.windowing.clone() {
            return gateway_windowed_from_registry(&registry, name, windowing, node_tls).await;
        }
    }
    // Otherwise `name` is an ordinal index or alias: connect the shard primaries + build the router.
    // Factored out so the hot-reload loop can re-run it on a topology change and swap the result in.
    let (nodes, router) = resolve_sharded_routing(&registry, name, node_tls).await?;
    // Search fan-out pruning: if the index is partition-routed on keyword fields, tell the
    // Gateway their names so a search pinning them routes to the owning shard instead of broadcasting.
    let partition_fields = registry
        .get(name)
        .map(|e| keyword_partition_fields(&e.definition))
        .unwrap_or_default();
    Ok(growlerdb_engine::Gateway::sharded_with(nodes, router)
        .with_limits(growlerdb_engine::GatewayLimits::from_env())
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

/// Backoff between gateway startup attempts to reach the control-plane + resolve the index's
/// shards. The gateway retries **unboundedly** — staying up with `/readyz` reporting not-ready —
/// rather than exiting, so a gateway rolled alongside the control-plane waits instead of
/// crash-looping.
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
    // A windowed index is fronted by the windowed gateway path, not this ordinal planner — reaching
    // here with window shards is an internal routing bug, not an unsupported case.
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
    // Validate topology (contiguous ordinals, every primary assigned) + resolve strategy/bucket map;
    // `endpoints` are the per-ordinal primaries in ordinal order (the routing fingerprint).
    let (endpoints, strategy, bucket_owners) = routing_plan_from_get_index(index, resp)?;
    let shard_count = endpoints.len() as u32;
    // The ordinal shards, sorted (routing_plan already asserted a contiguous 0..shard_count).
    let mut shards: Vec<&growlerdb_proto::v1::ShardStatus> = resp.shard_status.iter().collect();
    shards.sort_by_key(|s| s.ordinal);
    // Dedupe endpoint → warm `RemoteNode`: a pool node hosts several ordinals on ONE endpoint, and a
    // tonic `Channel` is a cheap handle — each ordinal still gets its own `ShardNode` over it.
    let mut conns: std::collections::HashMap<String, growlerdb_engine::RemoteNode> =
        std::collections::HashMap::new();
    let mut nodes: Vec<Arc<dyn growlerdb_engine::Node>> = Vec::with_capacity(shards.len());
    for s in &shards {
        // This ordinal's holders (D53): primary first, then read replicas (deduped). Each connects
        // lazily (a down holder fails at query time, not build) and is wrapped in a `ShardNode`
        // stamping `(index, ordinal)`; a `FailoverNode` fails a dead primary over to a live replica.
        let mut holder_eps: Vec<String> = vec![s.primary.clone()];
        for r in &s.replicas {
            if !r.is_empty() && !holder_eps.contains(r) {
                holder_eps.push(r.clone());
            }
        }
        let mut holders: Vec<Arc<dyn growlerdb_engine::Node>> =
            Vec::with_capacity(holder_eps.len());
        for ep in &holder_eps {
            let remote = match conns.get(ep) {
                Some(r) => r.clone(),
                None => {
                    let r = connect_node_lazy(ep, node_tls.clone())
                        .map_err(|e| anyhow::anyhow!("shard {} holder `{ep}`: {e}", s.ordinal))?;
                    conns.insert(ep.clone(), r.clone());
                    r
                }
            };
            holders.push(
                growlerdb_engine::ShardNode::new(Arc::new(remote), index, s.ordinal).shared(),
            );
        }
        let mut holders = holders.into_iter();
        let primary = holders.next().expect("holder_eps starts with the primary");
        nodes.push(growlerdb_engine::FailoverNode::new(primary, holders.collect()).shared());
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
// `(window id, primary endpoint, cold)` per window. `cold` is in the fingerprint so a tier flip
// (park/pre-warm) — same window, same placement — still triggers a gateway topology reload, keeping
// `/v1/cold` live instead of frozen at the boot tier.
type WindowFingerprint = Vec<(i64, String, Vec<String>, bool)>;

/// The windowed routing resolved from a live-CP `GetIndex`: one [`WindowNode`] per window (deduped by
/// endpoint), the windowing config, the per-window zone-map descriptors, and the fingerprint.
type CpWindowedRouting = (
    Vec<std::sync::Arc<dyn growlerdb_engine::Node>>,
    growlerdb_core::TimeWindowing,
    Vec<(i64, Option<(i64, i64)>, bool)>,
    WindowFingerprint,
);

/// The [`WindowFingerprint`] of a `GetIndex` response — `(window, primary, sorted replicas, cold)`
/// per assigned window, sorted. Computed **without connecting anything**, so a reloader tick can
/// decide "routing unchanged → keep the live gateway" (warm channels + [`FailoverNode`]
/// (growlerdb_engine::FailoverNode) holder-health state intact) before building a single node.
fn window_fingerprint_from_get_index(
    resp: &growlerdb_proto::v1::GetIndexResponse,
) -> WindowFingerprint {
    let mut fingerprint: WindowFingerprint = resp
        .shard_status
        .iter()
        .filter(|s| s.window != 0)
        .map(|s| {
            let mut reps: Vec<String> = s
                .replicas
                .iter()
                .filter(|r| !r.is_empty())
                .cloned()
                .collect();
            reps.sort();
            (s.window, s.primary.clone(), reps, s.cold)
        })
        .collect();
    fingerprint.sort();
    fingerprint
}

/// Resolve a windowed index's routing from a live-CP `GetIndex` response: connect one
/// [`WindowNode`] per window (deduped by endpoint — a node fronts many windows on one channel), the
/// windowing config + per-window event-time zone-maps for pruning, and the topology fingerprint.
/// Shared by the startup build and the hot-reload loop (so a window created at runtime is picked up).
///
/// `conns` is the caller's endpoint → connection cache: endpoints already present are **reused**
/// (a tonic `Channel` is a cheap handle, so the warm connection carries over) and endpoints the new
/// routing no longer references are pruned. A reloader keeps one cache across ticks so a topology
/// change only dials the endpoints that actually changed; one-shot callers pass a fresh map.
async fn resolve_windowed_routing_cp(
    index: &str,
    resp: &growlerdb_proto::v1::GetIndexResponse,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
    conns: &mut std::collections::HashMap<String, growlerdb_engine::RemoteNode>,
) -> anyhow::Result<CpWindowedRouting> {
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
    let mut nodes: Vec<Arc<dyn growlerdb_engine::Node>> = Vec::with_capacity(windows.len());
    let mut descriptors = Vec::with_capacity(windows.len());
    let mut referenced: std::collections::HashSet<String> = std::collections::HashSet::new();
    for s in &windows {
        if s.primary.is_empty() {
            anyhow::bail!(
                "window {} of `{index}` has no assigned primary yet",
                s.window
            );
        }
        // This window's holders (D53): primary first, then read replicas (deduped). Each connects
        // lazily so a down holder doesn't fail the whole resolution; a `FailoverNode` serves each
        // read from a live holder, failing a down primary over to a replica with no gap.
        let mut holder_eps: Vec<String> = vec![s.primary.clone()];
        for r in &s.replicas {
            if !r.is_empty() && !holder_eps.contains(r) {
                holder_eps.push(r.clone());
            }
        }
        let mut holders: Vec<Arc<dyn growlerdb_engine::Node>> =
            Vec::with_capacity(holder_eps.len());
        for ep in &holder_eps {
            referenced.insert(ep.clone());
            let remote = match conns.get(ep) {
                Some(r) => r.clone(),
                None => {
                    let r = connect_node_lazy(ep, node_tls.clone())
                        .map_err(|e| anyhow::anyhow!("window {} holder `{ep}`: {e}", s.window))?;
                    conns.insert(ep.clone(), r.clone());
                    r
                }
            };
            holders.push(
                growlerdb_engine::WindowNode::new(Arc::new(remote), index, s.window).shared(),
            );
        }
        let mut holders = holders.into_iter();
        let primary = holders.next().expect("holder_eps starts with the primary");
        nodes.push(growlerdb_engine::FailoverNode::new(primary, holders.collect()).shared());
        descriptors.push((
            s.window,
            s.has_event_bounds.then_some((s.event_min, s.event_max)),
            s.cold,
        ));
    }
    // Prune connections no window references any more, so a long-lived reloader cache doesn't grow
    // without bound as pods churn endpoints.
    conns.retain(|ep, _| referenced.contains(ep));
    // Fingerprint the full holder set (primary + sorted replicas) so the routing loop re-resolves
    // when the CP moves or re-replicates a window, not just on a primary change.
    Ok((
        nodes,
        windowing,
        descriptors,
        window_fingerprint_from_get_index(resp),
    ))
}

/// Build a windowed [`Gateway`] + its [`WindowFingerprint`] from a live-CP `GetIndex`.
async fn windowed_gateway_from_get_index(
    index: &str,
    resp: &growlerdb_proto::v1::GetIndexResponse,
    node_tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<(growlerdb_engine::Gateway, WindowFingerprint)> {
    let (nodes, windowing, descriptors, fp) =
        resolve_windowed_routing_cp(index, resp, node_tls, &mut Default::default()).await?;
    // Temporal-field units from the CP's field mapping, so the gateway's `_search` adapter converts
    // range bounds to canonical micros (keeping pruning + node execution consistent).
    let date_formats = date_formats_from_get_index(resp);
    Ok((
        growlerdb_engine::Gateway::windowed(nodes, windowing, descriptors)
            .with_date_formats(date_formats),
        fp,
    ))
}

/// Every temporal field's declared unit, from a live-CP `GetIndex`'s field mappings — so the
/// gateway's `_search` adapter converts a range/exact bound on **any** date field (not just the
/// windowing one) to canonical micros before planning.
fn date_formats_from_get_index(
    resp: &growlerdb_proto::v1::GetIndexResponse,
) -> growlerdb_engine::opensearch::FieldFormats {
    resp.fields
        .iter()
        .filter_map(|f| {
            growlerdb_core::TimeFormat::from_wire(&f.field_format).map(|fmt| (f.path.clone(), fmt))
        })
        .collect()
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
    // One build attempt: connect to the CP, then resolve the index's topology. Both a connection
    // refusal and shards-not-yet-registered are transient at boot, so retry_until_ok waits it out
    // rather than exit(1) → CrashLoopBackOff. A windowed index builds a window-pruning gateway; an
    // ordinal index returns its routing fingerprint.
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
                    growlerdb_engine::Gateway::sharded_with(nodes, router)
                        .with_limits(growlerdb_engine::GatewayLimits::from_env()),
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
    fn control_plane(&self) -> Option<&str> {
        Some(&self.cp)
    }

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
            let (nodes, windowing, descriptors, _fp) = resolve_windowed_routing_cp(
                index,
                &resp,
                self.node_tls.clone(),
                &mut Default::default(),
            )
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
        // Windowed reload state: last-applied fingerprint (skip the swap when unchanged, keeping warm
        // channels) + the endpoint → connection cache reused across ticks (dial only new endpoints).
        let mut last: Option<WindowFingerprint> = None;
        let mut conns: std::collections::HashMap<String, growlerdb_engine::RemoteNode> =
            Default::default();
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
                // Unchanged routing → keep the live route: swapping would discard warm node channels
                // and the FailoverNode holder-health state for nothing.
                let fp = window_fingerprint_from_get_index(&resp);
                if last.as_ref() == Some(&fp) {
                    growlerdb_telemetry::sli::background_success("route-reload");
                    continue;
                }
                match resolve_windowed_routing_cp(&index, &resp, node_tls.clone(), &mut conns).await
                {
                    Ok((nodes, windowing, descriptors, fp)) => {
                        route.swap_windowed(nodes, windowing, descriptors);
                        last = Some(fp);
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
                // Swap in freshly-built routing EVERY tick (lazy channels make it cheap): it
                // re-resolves each shard's DNS, so a shard pod that returned at a new IP is
                // reconnected within one interval and a partially-down cluster self-heals. Log only
                // when the fingerprint actually changes.
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
/// window set; `startup_fp` seeds `last` so an unchanged-routing tick **skips the swap entirely**
/// (HA-B6): swapping every tick would replace warm node channels with fresh lazy ones and reset the
/// [`FailoverNode`](growlerdb_engine::FailoverNode) holder-health state for no topology gain. When
/// the routing does change, the endpoint → connection cache reuses the channels of every endpoint
/// that persists across the change (a tonic `Channel` is a cheap cloneable handle), so only genuinely
/// new endpoints dial.
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
        // Endpoint → connection cache, kept across ticks so a topology change reuses the warm
        // channel of every endpoint that persists (resolve prunes endpoints that drop out).
        let mut conns: std::collections::HashMap<String, growlerdb_engine::RemoteNode> =
            Default::default();
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
            // Routing unchanged → skip the swap: keep warm node channels + FailoverNode holder-health
            // state. Fingerprint is computed straight off the response — no node connects here.
            let fp = window_fingerprint_from_get_index(&resp);
            if last.as_ref() == Some(&fp) {
                growlerdb_telemetry::sli::background_success("cp-reload-windowed");
                continue;
            }
            match resolve_windowed_routing_cp(&index, &resp, node_tls.clone(), &mut conns).await {
                Ok((nodes, windowing, descriptors, fp)) => {
                    gateway.swap_windowed(nodes, windowing, descriptors);
                    eprintln!(
                        "gateway: hot-reloaded `{index}` windows ({} live) from {cp}",
                        gateway.shard_count()
                    );
                    last = Some(fp);
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
        nodes.push(growlerdb_engine::WindowNode::new(Arc::new(remote), name, *w).shared());
        descriptors.push((*w, wa.event_min.zip(wa.event_max), wa.cold));
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
/// plaintext otherwise. Stamps the cluster service token (`GROWLERDB_SERVICE_TOKEN`) on every
/// request when configured — required once the Node closes its data plane with the same token.
async fn connect_node(
    endpoint: &str,
    tls: Option<tonic::transport::ClientTlsConfig>,
) -> anyhow::Result<growlerdb_engine::RemoteNode> {
    let token = growlerdb_proto::service_token::service_token_from_env();
    let node = match tls {
        Some(tls) => {
            growlerdb_engine::RemoteNode::connect_with_tls(
                endpoint.to_string(),
                tls,
                token.as_deref(),
            )
            .await
        }
        None => growlerdb_engine::RemoteNode::connect(endpoint.to_string(), token.as_deref()).await,
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
    let token = growlerdb_proto::service_token::service_token_from_env();
    let node = match tls {
        Some(tls) => growlerdb_engine::RemoteNode::connect_lazy_with_tls(
            endpoint.to_string(),
            tls,
            token.as_deref(),
        ),
        None => growlerdb_engine::RemoteNode::connect_lazy(endpoint.to_string(), token.as_deref()),
    };
    node.map_err(|e| anyhow::anyhow!("preparing Node `{endpoint}`: {e}"))
}

/// Announce a node-served index to the Control-Plane registry: send its resolved definition (so
/// the control plane needn't re-resolve against the source) + the routable `endpoint` it serves
/// from. Idempotent — safe to call on every node restart.
/// Backoff bounds + heartbeat for control-plane registration.
const REGISTER_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
const REGISTER_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(30);
/// Derived from the control plane's own constant so the heartbeat cadence and liveness TTL
/// (`NODE_HEARTBEAT_TTL_MS` = 3× this) can't silently diverge and flap healthy nodes out of the
/// placement pool (HA-D5).
const REGISTER_REANNOUNCE_INTERVAL: std::time::Duration =
    std::time::Duration::from_millis(growlerdb_controlplane::NODE_REANNOUNCE_INTERVAL_MS as u64);

/// Announce a served index to the control-plane registry in the background, retrying until it
/// succeeds then re-announcing on an interval (pods start together, so the CP is routinely
/// unreachable at node start; a one-shot attempt would leave the shard invisible forever). The node
/// serves immediately, but `readiness` flips ready only on the FIRST successful registration, so
/// `/readyz` stays not-ready until the node is in the registry. Re-announcing is an idempotent
/// upsert that re-points the registry after a control-plane restart.
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
                        false, // classic sharded serve — empty ordinals ⇒ single node claims all
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

/// Apply ±`frac` jitter to `base` so fleet-wide loops don't fire in lockstep and herd the control
/// plane (each re-announce drives a full-registry rewrite). Uses the sub-second wall clock as cheap
/// entropy — only decorrelation, not unpredictability, is needed. Clamped so the result never drops
/// below 10% of `base`.
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

#[allow(clippy::too_many_arguments)]
async fn register_served_index(
    control_plane: &str,
    endpoint: &str,
    resolved: &growlerdb_core::ResolvedIndex,
    shard_count: u32,
    shard_ordinals: Vec<u32>,
    windows: Vec<growlerdb_proto::v1::ServedWindow>,
    pool_managed: bool,
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
            pool_managed,
        })
        .await
        .map_err(|e| anyhow::anyhow!("registering with control plane: {e}"))?;
    Ok(())
}

/// Heartbeat this node into the control-plane **placement pool** so the CP keeps it eligible for unit
/// placement. Index-agnostic (D52): the pool is a flat set of interchangeable shard hosts.
/// `replica_capable` declares whether this node can serve **replica** windows read-through (it has
/// an object store configured) — the CP places replicas only on capable nodes (HA-G2).
async fn register_node(
    control_plane: &str,
    endpoint: &str,
    replica_capable: bool,
) -> anyhow::Result<()> {
    let mut client = connect_cp(control_plane, false).await?;
    client
        .register_node(growlerdb_proto::v1::RegisterNodeRequest {
            endpoint: endpoint.to_string(),
            replica_capable,
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
                    // Heartbeat first, then re-announce the current served windows + zone-maps.
                    // Never replica-capable: the single-index windowed serve mode has no assignment
                    // reconcile, so it couldn't serve a replica window the CP placed on it.
                    register_node(&control_plane, &endpoint, false).await?;
                    register_served_index(
                        &control_plane,
                        &endpoint,
                        &resolved,
                        1,
                        vec![],
                        write_service.served_windows(),
                        false, // single-index windowed serve (not a pool node)
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

/// Pool-node registration (D52): heartbeat into the placement pool (once, index-agnostic) and
/// announce the served windows of **every** index this node hosts, so a cluster gateway can route to
/// it. The multi-index counterpart to [`spawn_windowed_registration`]; retries until reachable, marks
/// `readiness` ready on the first success, and re-announces on an interval (an idempotent upsert, so a
/// control-plane restart re-learns the node).
fn spawn_pool_registration(
    control_plane: String,
    endpoint: String,
    announcements: Vec<(
        growlerdb_core::ResolvedIndex,
        growlerdb_engine::WindowedWriteService,
    )>,
    hash_announcements: Vec<(growlerdb_core::ResolvedIndex, u32, Vec<u32>)>,
    replica_capable: bool,
    readiness: growlerdb_telemetry::Readiness,
    label: String,
) {
    tokio::spawn(async move {
        registration_loop(
            || {
                let control_plane = control_plane.clone();
                let endpoint = endpoint.clone();
                let announcements = announcements.clone();
                let hash_announcements = hash_announcements.clone();
                async move {
                    // Heartbeat first, then announce each served index's windows — read fresh from
                    // the writer each tick, so a window created since the last announce is advertised.
                    // The heartbeat carries replica-capability (HA-G2): true only with an object
                    // store, so the CP never places replica windows on a node that couldn't serve them.
                    register_node(&control_plane, &endpoint, replica_capable).await?;
                    for (resolved, writer) in &announcements {
                        register_served_index(
                            &control_plane,
                            &endpoint,
                            resolved,
                            1,
                            vec![],
                            writer.served_windows(),
                            true, // pool node
                        )
                        .await?;
                    }
                    // A hash index announces its served ordinals + total shard count (no windows):
                    // the gateway places a ShardNode per ordinal to route hash reads/writes to their
                    // holder. Held set fixed at boot.
                    for (resolved, shard_count, ordinals) in &hash_announcements {
                        register_served_index(
                            &control_plane,
                            &endpoint,
                            resolved,
                            *shard_count,
                            ordinals.clone(),
                            vec![],
                            true, // pool node — CP places ordinals; empty list claims none
                        )
                        .await?;
                    }
                    Ok(())
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
            let demo_password = std::env::var("GROWLERDB_DEMO_PASSWORD")
                .unwrap_or_else(|_| "demo-growlerdb".to_string());
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
    registry_postgres: Option<String>,
    tls: Option<tonic::transport::ServerTlsConfig>,
) -> anyhow::Result<()> {
    use std::sync::Arc;
    use tonic::transport::Server;

    let socket: std::net::SocketAddr = addr
        .parse()
        .map_err(|e| anyhow::anyhow!("invalid --addr `{addr}`: {e}"))?;
    let registry_path = std::path::Path::new(data_dir).join("registry.json");
    std::fs::create_dir_all(data_dir)?;

    // HA (D51): with `--registry-postgres`, every replica opens the registry as a warm standby over
    // the shared store and a background loop races for leadership; without it, the embedded
    // single-writer JSON store. `ha_managed` means the leadership loop owns readiness (only the
    // leader is ready), so the k8s Service routes to the one writer.
    #[cfg(feature = "postgres")]
    let (registry, ha_managed) = if let Some(url) = registry_postgres.as_deref() {
        let backend = growlerdb_controlplane::PostgresBackend::open_standby(url)?;
        (
            Arc::new(growlerdb_controlplane::Registry::with_backend(Box::new(
                backend,
            ))?),
            true,
        )
    } else {
        (
            Arc::new(growlerdb_controlplane::Registry::open(&registry_path)?),
            false,
        )
    };
    #[cfg(not(feature = "postgres"))]
    let (registry, ha_managed) = {
        if registry_postgres.is_some() {
            anyhow::bail!(
                "--registry-postgres needs a build with `--features postgres`; this binary was \
                 compiled without the Postgres registry backend"
            );
        }
        (
            Arc::new(growlerdb_controlplane::Registry::open(&registry_path)?),
            false,
        )
    };

    // The leadership loop needs its own registry handle: `registry` is moved into the gRPC service
    // below. (Only the HA path spawns the loop; cloned regardless in a postgres build, cheap.)
    #[cfg(feature = "postgres")]
    let ha_registry = registry.clone();

    // Optional scale-limit license from GROWLERDB_LICENSE (a signed entitlement). An invalid one
    // warns and falls back to the free tier rather than failing startup.
    let license = match std::env::var("GROWLERDB_LICENSE") {
        Ok(token) if !token.trim().is_empty() => {
            match growlerdb_engine::License::verify(token.trim()) {
                Ok(lic) => {
                    println!(
                        "control plane: Enterprise license for `{}` — unit limit {} (primary-held \
                         units; replicas free)",
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
        // Built-in closed mode: `/v1/login` mints session JWTs from the registry credential store;
        // the control plane validates them (+ API tokens) and enforces RBAC. Seed users on first boot.
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
        // Login-only mode (the `just stack` demo): mint session JWTs via `/v1/login` + seed users,
        // but leave the control plane's OWN authorization open. Enforcement is at the gateway
        // (`--builtin-auth`); the CP stays reachable for internal node/gateway RPCs that carry no
        // service credential. This only turns on token minting, it doesn't gate the control plane.
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
    let replication_factor = replication_factor_from_env();
    if replication_factor > 1 {
        println!(
            "control plane: replication factor R={replication_factor} — each unit gets 1 primary + \
             {} read replica(s) (D53)",
            replication_factor - 1
        );
    }
    let svc = svc
        .with_license(license)
        .with_replication_factor(replication_factor);

    if ha_managed {
        println!(
            "control plane: registry on {socket} (HA: externalized Postgres store, N-replica \
             leader/standby — starting as standby, racing for leadership)"
        );
    } else {
        println!(
            "control plane: registry on {socket} (registry at {})",
            registry_path.display()
        );
    }
    // Service-credential gate: closes the internal RPCs (registration, shard-map reads, placement)
    // to callers outside the mesh, independent of user auth. Unset ⇒ open (bare dev).
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
    // Dead-owner sweeper (357.18/HA-D2): re-place units whose primary stopped heartbeating, even
    // with no writes arriving — leader-only, grace-aware; the logic lives in the engine/registry.
    svc.spawn_dead_owner_sweeper();
    // Placement sweeper (357.26/HA-D8): self-organize the pool — round-robin a primary for each
    // declared hash ordinal (nodes build/load on assignment) and fill replicas to R, even with no
    // writes arriving, so a batch-built index gets placed with no connector and a join/loss
    // self-heals. Leader-only, grace-aware.
    svc.spawn_placement_sweeper();
    let readiness = spawn_health(metrics_addr).await?;
    // In HA mode the leadership loop owns readiness — only the writer-lock holder is ready, so the
    // Service routes to the single leader (active-passive). Otherwise the registry is ready at once.
    if ha_managed {
        #[cfg(feature = "postgres")]
        spawn_cp_leadership(ha_registry, readiness.clone());
    } else {
        readiness.mark_ready();
    }
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

/// The HA control-plane **leadership + standby-reload loop** (D51), spawned once per replica when
/// `--registry-postgres` is set. Runs on a dedicated OS thread because the Postgres backend is a
/// blocking client (its calls must not park a tokio worker). Active-passive: readiness tracks
/// leadership, so a k8s Service routes only to the single leader.
#[cfg(feature = "postgres")]
fn spawn_cp_leadership(
    registry: std::sync::Arc<growlerdb_controlplane::Registry>,
    readiness: growlerdb_telemetry::Readiness,
) {
    std::thread::spawn(move || {
        // Fast enough to promote a standby within a fraction of a second of the leader's death
        // (Postgres releases the advisory lock ~immediately on connection close), and cheap enough
        // (one `SELECT` per tick) to poll continuously.
        let interval = std::time::Duration::from_millis(250);
        let mut last_version: Option<i64> = None;
        loop {
            cp_leadership_tick(&registry, &readiness, &mut last_version);
            std::thread::sleep(interval);
        }
    });
}

/// One tick of the HA leadership loop — factored out of the spawned thread so the
/// promote / demote / ready transitions are unit-testable over a stub backend.
///
/// - **Leader** — revalidate leadership every tick: the advisory lock lives exactly as long as the
///   store session, so a dead session (Postgres restart, network drop, worker panic) means a standby
///   may already be leader. Verified → stay ready. Lost → **demote**: readiness off first (out of
///   the Service before anything else), resign writership, and fall back to the standby path.
/// - **Standby** — not-ready (out of the Service), then `try_become_leader`; still a standby → poll
///   the store version and [`reload`](growlerdb_controlplane::Registry::reload) when it advances
///   (the leader wrote — any row, including a sessions-only revocation bump), staying warm for a
///   fast failover.
/// - **Promotion** — `try_become_leader` succeeds: it reloaded from the store **before** confirming
///   writership (so the catalog is current before any write can be accepted); mark ready — this
///   replica joins the Service as the writer. A failed promotion reload surfaces as `Err` and the
///   replica stays a not-ready standby, retrying next tick.
#[cfg_attr(not(any(test, feature = "postgres")), allow(dead_code))]
fn cp_leadership_tick(
    registry: &growlerdb_controlplane::Registry,
    readiness: &growlerdb_telemetry::Readiness,
    last_version: &mut Option<i64>,
) {
    if registry.is_leader() {
        match registry.verify_leadership() {
            Ok(true) => readiness.mark_ready(),
            verdict => {
                readiness.mark_not_ready();
                registry.resign_leadership();
                match verdict {
                    Ok(_) => eprintln!(
                        "control plane: registry leadership lost (store session died) — demoting \
                         to standby"
                    ),
                    Err(e) => eprintln!(
                        "control plane: registry leadership could not be verified ({e}) — demoting \
                         to standby"
                    ),
                }
                // Force a resync on the next standby tick: this replica's catalog may be stale
                // against whichever replica took over.
                *last_version = None;
            }
        }
        return;
    }
    match registry.try_become_leader() {
        Ok(true) => {
            println!("control plane: promoted to registry leader — now serving writes");
            readiness.mark_ready();
        }
        Ok(false) => {
            // Still a standby: out of the Service, but keep the in-memory catalog warm.
            readiness.mark_not_ready();
            match registry.backend_version() {
                Ok(v) if v != *last_version => match registry.reload() {
                    Ok(()) => *last_version = v,
                    Err(e) => eprintln!("control plane: standby reload failed: {e}"),
                },
                Ok(_) => {}
                Err(e) => eprintln!("control plane: version poll failed: {e}"),
            }
        }
        Err(e) => {
            eprintln!("control plane: leadership acquisition failed: {e}");
            readiness.mark_not_ready();
        }
    }
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

/// The cluster-wide **replication factor** R from `GROWLERDB_REPLICATION_FACTOR` (D53): the CP places
/// 1 primary + R−1 read replicas per unit. Default/invalid ⇒ `1` (primary-only, the D52 behavior).
fn replication_factor_from_env() -> usize {
    std::env::var("GROWLERDB_REPLICATION_FACTOR")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&r| r >= 1)
        .unwrap_or(1)
}

/// Build the node's object store (for cold-tier / D53 replica read-through) from the environment.
/// `GROWLERDB_OBJECT_STORE_FS=<dir>` uses a **local filesystem** store — a single-host dev/test mode
/// (multiple processes on the box share the dir) that needs no S3/MinIO; otherwise the S3 store from
/// [`backup_s3_config`] (`GROWLERDB_BACKUP_BUCKET` + `GROWLERDB_S3_*`).
fn object_store_from_env() -> anyhow::Result<growlerdb_backup::Operator> {
    match std::env::var("GROWLERDB_OBJECT_STORE_FS")
        .ok()
        .filter(|v| !v.is_empty())
    {
        Some(dir) => growlerdb_backup::fs_store(&dir).map_err(anyhow::Error::from),
        None => growlerdb_backup::s3_store(&backup_s3_config()?).map_err(anyhow::Error::from),
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

    /// The HA opt-in flag parses (from the flag and its env var) and defaults to `None` — so the
    /// embedded JSON registry stays the default and `control_plane` only takes the Postgres branch
    /// when a URL is supplied.
    #[test]
    fn control_plane_registry_postgres_flag_parses() {
        use clap::Parser;
        let url = "postgresql://postgres:pw@127.0.0.1:55432/cp";

        let cli = Cli::try_parse_from(["growlerdb", "control-plane", "--registry-postgres", url])
            .expect("control-plane accepts --registry-postgres");
        let Command::ControlPlane {
            registry_postgres, ..
        } = cli.command
        else {
            panic!("expected the control-plane subcommand");
        };
        assert_eq!(registry_postgres.as_deref(), Some(url));

        // Default: no flag ⇒ None (embedded JSON registry).
        let cli = Cli::try_parse_from(["growlerdb", "control-plane"]).unwrap();
        let Command::ControlPlane {
            registry_postgres, ..
        } = cli.command
        else {
            panic!("expected the control-plane subcommand");
        };
        assert_eq!(registry_postgres, None);
    }

    /// A scripted [`growlerdb_controlplane::RegistryBackend`] simulating an HA store: the writer
    /// lock's availability and the lock-holding session's liveness are toggles, so
    /// [`cp_leadership_tick`]'s promote / demote / ready transitions are testable without Postgres.
    #[derive(Clone, Default)]
    struct ScriptedBackend(std::sync::Arc<ScriptedState>);

    #[derive(Default)]
    struct ScriptedState {
        leader: std::sync::atomic::AtomicBool,
        lock_available: std::sync::atomic::AtomicBool,
        session_dead: std::sync::atomic::AtomicBool,
    }

    impl growlerdb_controlplane::RegistryBackend for ScriptedBackend {
        fn load(&self) -> growlerdb_controlplane::Result<growlerdb_controlplane::PersistedState> {
            Ok(growlerdb_controlplane::PersistedState::default())
        }
        fn persist_registry(
            &self,
            _: growlerdb_controlplane::RegistrySnapshot,
        ) -> growlerdb_controlplane::Result<()> {
            Ok(())
        }
        fn persist_activity(
            &self,
            _: &std::collections::BTreeMap<String, Vec<growlerdb_controlplane::ActivityEvent>>,
        ) -> growlerdb_controlplane::Result<()> {
            Ok(())
        }
        fn persist_sessions(
            &self,
            _: &std::collections::BTreeMap<String, i64>,
        ) -> growlerdb_controlplane::Result<()> {
            Ok(())
        }
        fn poll_version(&self) -> growlerdb_controlplane::Result<Option<i64>> {
            Ok(Some(1))
        }
        fn try_become_leader(&self) -> growlerdb_controlplane::Result<bool> {
            use std::sync::atomic::Ordering;
            Ok(self.0.lock_available.load(Ordering::SeqCst)
                && !self.0.session_dead.load(Ordering::SeqCst))
        }
        fn confirm_leadership(&self) {
            self.0
                .leader
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
        fn resign_leadership(&self) {
            self.0
                .leader
                .store(false, std::sync::atomic::Ordering::SeqCst);
        }
        fn verify_leadership(&self) -> growlerdb_controlplane::Result<bool> {
            use std::sync::atomic::Ordering;
            if self.0.session_dead.load(Ordering::SeqCst) {
                self.0.leader.store(false, Ordering::SeqCst);
                return Ok(false);
            }
            Ok(self.0.leader.load(Ordering::SeqCst))
        }
        fn is_leader(&self) -> bool {
            self.0.leader.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    /// The HA leadership tick: a standby stays not-ready, promotes (ready) once the writer lock is
    /// winnable, **demotes within one tick** when the lock-holding session dies (readiness off, out
    /// of the Service — the HA-C1 fix: no eternal two-ready split), and re-promotes on recovery.
    #[test]
    fn cp_leadership_tick_promotes_demotes_and_repromotes() {
        use std::sync::atomic::Ordering;
        let backend = ScriptedBackend::default();
        let registry = Registry::with_backend(Box::new(backend.clone())).unwrap();
        let readiness = growlerdb_telemetry::Readiness::new();
        let mut last_version = None;

        // Standby: the lock is held elsewhere → not ready.
        cp_leadership_tick(&registry, &readiness, &mut last_version);
        assert!(!readiness.is_ready());
        assert!(!registry.is_leader());

        // Lock released (old leader died) → promoted and ready in one tick.
        backend.0.lock_available.store(true, Ordering::SeqCst);
        cp_leadership_tick(&registry, &readiness, &mut last_version);
        assert!(registry.is_leader());
        assert!(readiness.is_ready());

        // Steady state: leadership re-verified each tick, stays ready.
        cp_leadership_tick(&registry, &readiness, &mut last_version);
        assert!(readiness.is_ready());

        // The lock-holding session dies → demoted the same tick: readiness withdrawn, writership
        // resigned — the old leader can never sit READY serving a frozen catalog.
        backend.0.session_dead.store(true, Ordering::SeqCst);
        cp_leadership_tick(&registry, &readiness, &mut last_version);
        assert!(!readiness.is_ready());
        assert!(!registry.is_leader());

        // While the session is dead the standby can't re-acquire; still not ready.
        cp_leadership_tick(&registry, &readiness, &mut last_version);
        assert!(!readiness.is_ready());

        // Session revived and the lock still free → re-promotes.
        backend.0.session_dead.store(false, Ordering::SeqCst);
        cp_leadership_tick(&registry, &readiness, &mut last_version);
        assert!(registry.is_leader());
        assert!(readiness.is_ready());
    }

    #[test]
    fn env_truthy_parses_common_forms() {
        // The GROWLERDB_REQUIRE_AUTH guard treats these as "on".
        for v in ["1", "true", "TRUE", " yes ", "On"] {
            assert!(env_truthy(v), "{v:?} should be truthy");
        }
        // Everything else (including empty and "0") leaves the gateway able to run open.
        for v in ["0", "false", "no", "", "off", "2"] {
            assert!(!env_truthy(v), "{v:?} should not be truthy");
        }
    }

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

    /// A pool node hosts SEVERAL ordinals of one hash index over ONE endpoint, each replicated on a
    /// second pool node (D52). `connect_sharded_from_get_index` must wrap every ordinal in a
    /// `ShardNode` (stamping `(index, ordinal)`) over a `FailoverNode` across its holders, deduping
    /// the shared endpoint's channel — so it yields one routable node per ordinal, not one per
    /// endpoint. Lazy connects (nothing dials), so this runs with no live nodes.
    #[tokio::test]
    async fn cp_sharded_routing_wraps_pool_ordinals_sharing_one_endpoint() {
        use growlerdb_proto::v1::{GetIndexResponse, ShardStatus};
        let ordinal = |o: u32| ShardStatus {
            ordinal: o,
            window: 0,
            primary: "http://pool-a:50051".into(),
            replicas: vec!["http://pool-b:50051".into()],
            state: "active".into(),
            ..Default::default()
        };
        let resp = GetIndexResponse {
            name: "docs".into(),
            status: "active".into(),
            shard_count: 2,
            routing: 0, // hash
            shard_status: vec![ordinal(0), ordinal(1)],
            ..Default::default()
        };
        let (nodes, router, fp) = connect_sharded_from_get_index("docs", &resp, None).unwrap();
        // One routable node PER ORDINAL (not per endpoint), and the router spans both ordinals.
        assert_eq!(nodes.len(), 2);
        assert_eq!(router.shards(), 2);
        // The fingerprint is the per-ordinal primaries in ordinal order (both on the pool node).
        assert_eq!(
            fp.0,
            vec![
                "http://pool-a:50051".to_string(),
                "http://pool-a:50051".to_string()
            ]
        );
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconcile_serves_an_assigned_replica_window_read_through() {
        // D53 end-to-end (node side): a pushed REPLICA assignment makes the node fetch the parked
        // window's marker + open it read-through, publishing it into the pool maps so it's queryable
        // — with no local copy and no rebuild.
        use growlerdb_core::{
            CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
            SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
        };
        use growlerdb_index::{LocalIndexStore, ShardId};
        use growlerdb_proto::v1::unit_assignment::Unit as WireUnit;
        use growlerdb_proto::v1::UnitAssignment;
        use std::collections::{BTreeMap, HashMap};
        use std::sync::{Arc, RwLock};

        let src = SourceSchema::new(
            vec![
                SourceField::new("id", SourceType::String),
                SourceField::new("ts", SourceType::Long),
            ],
            vec![],
            vec!["id".into()],
        );
        let resolved = IndexDefinition::from_yaml(
            "name: logs\nsource: { iceberg: { catalog: g, table: g.logs } }\nwindowing: { field: ts, granularity: daily }\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD, fast: true }, { path: ts, format: epoch_ms, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let w: i64 = 1_700_000_000_000;
        let id = ShardId::window("logs", w);

        // --- primary node: build + park the window to shared object storage ---
        let primary_root = tempfile::tempdir().unwrap();
        let primary = LocalIndexStore::open(primary_root.path()).unwrap();
        let shard = primary.create_shard(&id, &resolved).unwrap();
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("doc-1"));
        f.insert("ts".to_string(), Value::from(1_i64));
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![LocatedDoc {
                    doc: Document::new(key, f),
                }],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        let window_dir = primary.shard_path(&id);
        let backup_root = tempfile::tempdir().unwrap();
        let op = growlerdb_backup::fs_store(backup_root.path()).unwrap();
        growlerdb_backup::cold_park(
            shard,
            "logs",
            w,
            &window_dir,
            &primary_root.path().join(".stg"),
            &op,
            &format!("cold/logs/w{w}"),
            Some(serde_json::to_string(&resolved).unwrap()),
        )
        .await
        .unwrap();

        // --- replica node: empty pool maps + meta; reconcile a replica assignment ---
        let replica_root = tempfile::tempdir().unwrap();
        let replica_store = LocalIndexStore::open(replica_root.path()).unwrap();
        // One served index ("logs") with an empty window map behind each Pool multiplexer.
        let search_idx: growlerdb_engine::SharedSearchIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("logs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let suggest_idx: growlerdb_engine::SharedSuggestIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("logs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let lookup_idx: growlerdb_engine::SharedLookupIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("logs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let admin_idx: growlerdb_engine::SharedAdminIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("logs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let mut meta: ReplicaIndexMeta = HashMap::new();
        meta.insert(
            "logs".to_string(),
            (
                resolved.clone(),
                "g.logs".to_string(),
                growlerdb_engine::IndexHeavyShare::new(
                    4,
                    Arc::new(std::sync::atomic::AtomicUsize::new(1)),
                ),
            ),
        );
        let cache = growlerdb_index::RangeCache::new(8 * 1024 * 1024);
        let replica_units = vec![UnitAssignment {
            index: "logs".into(),
            unit: Some(WireUnit::Window(w)),
            primary: false,
        }];

        let served = reconcile_replica_units(
            &replica_units,
            &meta,
            &search_idx,
            &suggest_idx,
            &lookup_idx,
            &admin_idx,
            &replica_store,
            &op,
            &cache,
            replica_root.path(),
        )
        .await;
        assert_eq!(served, 1, "the assigned replica window is opened + served");

        // Queryable through the pool read path — read-through, with no local copy.
        let mux = growlerdb_engine::PoolSearchService::new(
            search_idx.clone(),
            std::sync::Arc::new(std::sync::RwLock::new(std::collections::BTreeMap::new())),
        );
        let resp = growlerdb_proto::Search::search(
            &mux,
            tonic::Request::new(growlerdb_proto::v1::SearchRequest {
                query: "*".into(),
                limit: 10,
                index: "logs".into(),
                window: w,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(
            resp.hits.len(),
            1,
            "the replica serves the doc read-through via the pool"
        );

        // Idempotent: a second reconcile serves nothing new.
        assert_eq!(
            reconcile_replica_units(
                &replica_units,
                &meta,
                &search_idx,
                &suggest_idx,
                &lookup_idx,
                &admin_idx,
                &replica_store,
                &op,
                &cache,
                replica_root.path(),
            )
            .await,
            0,
            "an already-served window isn't re-opened"
        );

        // The same window assigned as PRIMARY is a no-op — it's already served (a parked window is
        // read-only, so it serves read-through for either role).
        let primary_units = vec![UnitAssignment {
            index: "logs".into(),
            unit: Some(WireUnit::Window(w)),
            primary: true,
        }];
        assert_eq!(
            reconcile_replica_units(
                &primary_units,
                &meta,
                &search_idx,
                &suggest_idx,
                &lookup_idx,
                &admin_idx,
                &replica_store,
                &op,
                &cache,
                replica_root.path(),
            )
            .await,
            0,
            "an already-served window isn't re-opened, whatever the role"
        );
    }

    #[tokio::test]
    async fn reconcile_serves_an_assigned_replica_hash_ordinal_read_through() {
        // D53 hash-shard parity (node side): a pushed REPLICA assignment for an ORDINAL shard makes
        // the node fetch the shard's `backup_replica_snapshot` marker + open it read-through, keyed by
        // ordinal-as-i64 in the same pool maps — no local copy, no rebuild. The hash counterpart to
        // `reconcile_serves_an_assigned_replica_window_read_through`.
        use growlerdb_core::{
            CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
            SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
        };
        use growlerdb_index::{LocalIndexStore, ShardId};
        use growlerdb_proto::v1::unit_assignment::Unit as WireUnit;
        use growlerdb_proto::v1::UnitAssignment;
        use std::collections::{BTreeMap, HashMap};
        use std::sync::{Arc, RwLock};

        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let resolved = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nshard_count: 2\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let ordinal: u32 = 1;
        let id = ShardId::shard("docs", ordinal);

        // --- primary node: build ordinal 1 + publish its frozen replica snapshot to object storage ---
        let primary_root = tempfile::tempdir().unwrap();
        let primary = LocalIndexStore::open(primary_root.path()).unwrap();
        let shard = primary.create_shard(&id, &resolved).unwrap();
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("doc-1"));
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![LocatedDoc {
                    doc: Document::new(key, f),
                }],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        let backup_root = tempfile::tempdir().unwrap();
        let op = growlerdb_backup::fs_store(backup_root.path()).unwrap();
        growlerdb_backup::backup_replica_snapshot(
            &shard,
            "docs",
            &ordinal.to_string(),
            &primary_root.path().join(".stg"),
            &op,
            &format!("cold/docs/{ordinal}"),
            Some(serde_json::to_string(&resolved).unwrap()),
        )
        .await
        .unwrap();

        // --- replica node: empty pool maps + meta; reconcile a replica Shard assignment ---
        let replica_root = tempfile::tempdir().unwrap();
        let replica_store = LocalIndexStore::open(replica_root.path()).unwrap();
        // One served index ("docs", hash) with an empty ordinal map behind each Pool multiplexer.
        let search_idx: growlerdb_engine::SharedSearchIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("docs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let suggest_idx: growlerdb_engine::SharedSuggestIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("docs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let lookup_idx: growlerdb_engine::SharedLookupIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("docs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let admin_idx: growlerdb_engine::SharedAdminIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("docs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let mut meta: ReplicaIndexMeta = HashMap::new();
        meta.insert(
            "docs".to_string(),
            (
                resolved.clone(),
                "g.docs".to_string(),
                growlerdb_engine::IndexHeavyShare::new(
                    4,
                    Arc::new(std::sync::atomic::AtomicUsize::new(1)),
                ),
            ),
        );
        let cache = growlerdb_index::RangeCache::new(8 * 1024 * 1024);
        let units = vec![UnitAssignment {
            index: "docs".into(),
            unit: Some(WireUnit::Shard(ordinal)),
            primary: false,
        }];
        let served = reconcile_replica_units(
            &units,
            &meta,
            &search_idx,
            &suggest_idx,
            &lookup_idx,
            &admin_idx,
            &replica_store,
            &op,
            &cache,
            replica_root.path(),
        )
        .await;
        assert_eq!(served, 1, "the assigned replica ordinal is opened + served");

        // Queryable through the pool read path — routed on the `shard` selector (kinds: docs is hash).
        let kinds: growlerdb_engine::SharedIndexKinds =
            Arc::new(RwLock::new(BTreeMap::from([("docs".to_string(), true)])));
        let mux = growlerdb_engine::PoolSearchService::new(search_idx.clone(), kinds);
        let resp = growlerdb_proto::Search::search(
            &mux,
            tonic::Request::new(growlerdb_proto::v1::SearchRequest {
                query: "*".into(),
                limit: 10,
                index: "docs".into(),
                shard: ordinal,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(
            resp.hits.len(),
            1,
            "the replica serves the ordinal's doc read-through via the pool"
        );

        // Idempotent: a second reconcile serves nothing new.
        assert_eq!(
            reconcile_replica_units(
                &units,
                &meta,
                &search_idx,
                &suggest_idx,
                &lookup_idx,
                &admin_idx,
                &replica_store,
                &op,
                &cache,
                replica_root.path(),
            )
            .await,
            0,
            "an already-served ordinal isn't re-opened"
        );
    }

    #[tokio::test]
    async fn build_on_assignment_promotes_a_read_through_replica_to_primary() {
        // A node serving a hash ORDINAL as a read-through REPLICA, then re-assigned PRIMARY for it,
        // must promote: the freshly-built primary replaces the stale read-through replica in the pool
        // maps. Without this the node serves the stale cold snapshot forever (the de-assignment path
        // tracks only Window units, so a hash ordinal's replica is never otherwise superseded). Here
        // we drive the promotion's core seam — `open_and_publish_ordinal` — directly.
        use growlerdb_core::{
            CommitBatch, CompositeKey, Document, IndexDefinition, IndexWriter, LocatedDoc,
            SourceCheckpoint, SourceField, SourceSchema, SourceType, Value,
        };
        use growlerdb_index::{LocalIndexStore, ShardId};
        use growlerdb_proto::v1::unit_assignment::Unit as WireUnit;
        use growlerdb_proto::v1::UnitAssignment;
        use std::collections::{BTreeMap, HashMap};
        use std::sync::atomic::AtomicUsize;
        use std::sync::{Arc, RwLock};

        let src = SourceSchema::new(
            vec![SourceField::new("id", SourceType::String)],
            vec![],
            vec!["id".into()],
        );
        let resolved = IndexDefinition::from_yaml(
            "name: docs\nsource: { iceberg: { catalog: g, table: g.docs } }\nshard_count: 2\nmapping: { selection: EXPLICIT, fields: [ { path: id, type: KEYWORD, fast: true } ] }\n",
        )
        .unwrap()
        .resolve(&src)
        .unwrap();
        let ordinal: u32 = 1;
        let id = ShardId::shard("docs", ordinal);

        // --- Build ordinal 1 in a local store + publish its replica snapshot to object storage. ---
        let primary_root = tempfile::tempdir().unwrap();
        let primary = LocalIndexStore::open(primary_root.path()).unwrap();
        let shard = primary.create_shard(&id, &resolved).unwrap();
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("doc-1"))]);
        let mut f = BTreeMap::new();
        f.insert("id".to_string(), Value::from("doc-1"));
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(
                vec![LocatedDoc {
                    doc: Document::new(key, f),
                }],
                SourceCheckpoint::iceberg(1),
                "b1",
            ),
        )
        .unwrap();
        let backup_root = tempfile::tempdir().unwrap();
        let op = growlerdb_backup::fs_store(backup_root.path()).unwrap();
        growlerdb_backup::backup_replica_snapshot(
            &shard,
            "docs",
            &ordinal.to_string(),
            &primary_root.path().join(".stg"),
            &op,
            &format!("cold/docs/{ordinal}"),
            Some(serde_json::to_string(&resolved).unwrap()),
        )
        .await
        .unwrap();
        // Release the writer lock so `open_and_publish_ordinal` can re-open the shard as primary.
        drop(shard);

        // --- Pool maps for a hash index "docs", each with an empty ordinal map. ---
        let search_idx: growlerdb_engine::SharedSearchIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("docs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let suggest_idx: growlerdb_engine::SharedSuggestIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("docs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let lookup_idx: growlerdb_engine::SharedLookupIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("docs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let admin_idx: growlerdb_engine::SharedAdminIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("docs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));
        let write_hash_idx: growlerdb_engine::SharedHashWriteIndexes = Arc::new(RwLock::new(
            BTreeMap::from([("docs".to_string(), Arc::new(RwLock::new(BTreeMap::new())))]),
        ));

        // --- 1) Serve ordinal 1 as a read-through REPLICA. ---
        let mut meta: ReplicaIndexMeta = HashMap::new();
        meta.insert(
            "docs".to_string(),
            (
                resolved.clone(),
                "g.docs".to_string(),
                growlerdb_engine::IndexHeavyShare::new(4, Arc::new(AtomicUsize::new(1))),
            ),
        );
        let cache = growlerdb_index::RangeCache::new(8 * 1024 * 1024);
        let replica_root = tempfile::tempdir().unwrap();
        let replica_store = LocalIndexStore::open(replica_root.path()).unwrap();
        let served = reconcile_replica_units(
            &[UnitAssignment {
                index: "docs".into(),
                unit: Some(WireUnit::Shard(ordinal)),
                primary: false,
            }],
            &meta,
            &search_idx,
            &suggest_idx,
            &lookup_idx,
            &admin_idx,
            &replica_store,
            &op,
            &cache,
            replica_root.path(),
        )
        .await;
        assert_eq!(served, 1, "the replica ordinal is opened read-through");
        let is_read_only = |m: &growlerdb_engine::SharedSearchIndexes| {
            m.read()
                .unwrap()
                .get("docs")
                .unwrap()
                .read()
                .unwrap()
                .get(&(ordinal as i64))
                .map(|svc| svc.serves_read_only())
        };
        assert_eq!(
            is_read_only(&search_idx),
            Some(true),
            "served as a read-only read-through replica"
        );

        // --- 2) Promote: build-on-assignment publishes the primary over the replica. ---
        open_and_publish_ordinal(
            &primary,
            &resolved,
            "g.docs",
            ordinal,
            growlerdb_engine::IndexHeavyShare::new(4, Arc::new(AtomicUsize::new(1))),
            0,
            &search_idx,
            &suggest_idx,
            &lookup_idx,
            &admin_idx,
            &write_hash_idx,
        )
        .await
        .unwrap();
        assert_eq!(
            is_read_only(&search_idx),
            Some(false),
            "the built primary REPLACED the read-through replica — no longer read-only"
        );
        // The write service now exists for the ordinal too (a replica has none), so the promoted
        // primary is writable.
        assert!(
            write_hash_idx
                .read()
                .unwrap()
                .get("docs")
                .unwrap()
                .read()
                .unwrap()
                .contains_key(&(ordinal as i64)),
            "promotion registers the primary's write service"
        );
    }

    #[tokio::test]
    async fn windowed_routing_builds_failover_holders_from_replicas() {
        // D53: each window resolves to a FailoverNode over its primary + replicas, and the routing
        // fingerprint carries the full (primary + sorted replicas) holder set so a replica-set change
        // re-resolves — not just a primary change. Lazy connect means the down holders don't dial.
        use growlerdb_proto::v1::{GetIndexResponse, ShardStatus, WindowingConfig};
        let resp = GetIndexResponse {
            windowing: Some(WindowingConfig {
                field: "ts".into(),
                granularity: "daily".into(),
                ..Default::default()
            }),
            shard_status: vec![
                ShardStatus {
                    window: 100,
                    primary: "http://p:50051".into(),
                    // Deliberately unsorted, to prove the fingerprint sorts them.
                    replicas: vec!["http://r2:50051".into(), "http://r1:50051".into()],
                    ..Default::default()
                },
                // A window with no replicas → a single-holder FailoverNode (the pre-D53 case).
                ShardStatus {
                    window: 200,
                    primary: "http://p2:50051".into(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let (nodes, _windowing, descriptors, fp) =
            resolve_windowed_routing_cp("logs", &resp, None, &mut Default::default())
                .await
                .unwrap();
        // One holder-group node (a FailoverNode) per window.
        assert_eq!(nodes.len(), 2);
        assert_eq!(descriptors.len(), 2);
        assert_eq!(
            fp,
            vec![
                (
                    100_i64,
                    "http://p:50051".to_string(),
                    vec!["http://r1:50051".to_string(), "http://r2:50051".to_string()],
                    false
                ),
                (200_i64, "http://p2:50051".to_string(), vec![], false),
            ]
        );
        // The cheap fingerprint (no connects) matches the resolved one — the reloader's skip-swap
        // decision (`fp == last` ⇒ keep the live gateway) keys off exactly this equality.
        assert_eq!(window_fingerprint_from_get_index(&resp), fp);
    }

    /// HA-B6 (reloader half): an unchanged `GetIndex` response fingerprints identically —
    /// regardless of shard/replica listing order — so a reloader tick skips the swap; any holder,
    /// placement, or tier change fingerprints differently and triggers a real swap.
    #[test]
    fn window_fingerprint_is_order_insensitive_and_change_sensitive() {
        use growlerdb_proto::v1::{GetIndexResponse, ShardStatus};
        let status = |window: i64, primary: &str, replicas: &[&str], cold: bool| ShardStatus {
            window,
            primary: primary.into(),
            replicas: replicas.iter().map(|r| r.to_string()).collect(),
            cold,
            ..Default::default()
        };
        let resp = |shards: Vec<ShardStatus>| GetIndexResponse {
            shard_status: shards,
            ..Default::default()
        };
        let base = resp(vec![
            status(100, "http://p:1", &["http://r1:1", "http://r2:1"], false),
            status(200, "http://p2:1", &[], true),
        ]);
        // Same routing, different wire order → same fingerprint → the reloader skips the swap.
        let reordered = resp(vec![
            status(200, "http://p2:1", &[], true),
            status(100, "http://p:1", &["http://r2:1", "http://r1:1"], false),
        ]);
        let fp = window_fingerprint_from_get_index(&base);
        assert_eq!(fp, window_fingerprint_from_get_index(&reordered));
        // A replica-set change, a primary move, and a tier flip each change the fingerprint.
        for changed in [
            resp(vec![
                status(100, "http://p:1", &["http://r1:1"], false),
                status(200, "http://p2:1", &[], true),
            ]),
            resp(vec![
                status(
                    100,
                    "http://elsewhere:1",
                    &["http://r1:1", "http://r2:1"],
                    false,
                ),
                status(200, "http://p2:1", &[], true),
            ]),
            resp(vec![
                status(100, "http://p:1", &["http://r1:1", "http://r2:1"], false),
                status(200, "http://p2:1", &[], false),
            ]),
        ] {
            assert_ne!(fp, window_fingerprint_from_get_index(&changed));
        }
    }

    /// The reloader's connection cache reuses an endpoint's connection across resolves and prunes
    /// endpoints the new routing no longer references (lazy connects — nothing dials here).
    #[tokio::test]
    async fn windowed_resolve_reuses_and_prunes_the_connection_cache() {
        use growlerdb_proto::v1::{GetIndexResponse, ShardStatus, WindowingConfig};
        let resp = |shards: Vec<(i64, &str, Vec<&str>)>| GetIndexResponse {
            windowing: Some(WindowingConfig {
                field: "ts".into(),
                granularity: "daily".into(),
                ..Default::default()
            }),
            shard_status: shards
                .into_iter()
                .map(|(window, primary, replicas)| ShardStatus {
                    window,
                    primary: primary.into(),
                    replicas: replicas.into_iter().map(String::from).collect(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let mut conns = std::collections::HashMap::new();
        resolve_windowed_routing_cp(
            "logs",
            &resp(vec![(100, "http://a:1", vec!["http://b:1"])]),
            None,
            &mut conns,
        )
        .await
        .unwrap();
        let mut eps: Vec<&String> = conns.keys().collect();
        eps.sort();
        assert_eq!(eps, ["http://a:1", "http://b:1"]);
        // The next topology drops `b` and adds `c`: `a`'s connection persists (warm-channel reuse),
        // `b` is pruned, `c` is dialed lazily.
        resolve_windowed_routing_cp(
            "logs",
            &resp(vec![(100, "http://a:1", vec!["http://c:1"])]),
            None,
            &mut conns,
        )
        .await
        .unwrap();
        let mut eps: Vec<&String> = conns.keys().collect();
        eps.sort();
        assert_eq!(
            eps,
            ["http://a:1", "http://c:1"],
            "persisting endpoints are kept, dropped ones pruned"
        );
    }

    #[test]
    fn date_formats_from_get_index_covers_every_temporal_field() {
        use growlerdb_core::TimeFormat;
        // Field mappings carry each date field's declared unit; non-date fields (blank format) and
        // unknown units are skipped. The windowing field is no longer special — every date field is
        // resolved so a `_search` bound on any of them converts to canonical micros.
        let field = |path: &str, ty: &str, fmt: &str| growlerdb_proto::v1::FieldMapping {
            path: path.into(),
            r#type: ty.into(),
            field_format: fmt.into(),
            ..Default::default()
        };
        let resp = growlerdb_proto::v1::GetIndexResponse {
            fields: vec![
                field("ingest", "date", "epoch_millis"),
                field("event", "date", "epoch_seconds"),
                field("native_ts", "date", ""), // native micros → no conversion
                field("body", "text", ""),      // non-temporal
                field("weird", "date", "furlongs"), // unparseable unit → skipped
            ],
            ..Default::default()
        };
        let formats = date_formats_from_get_index(&resp);
        assert_eq!(formats.get("ingest"), Some(&TimeFormat::EpochMillis));
        assert_eq!(formats.get("event"), Some(&TimeFormat::EpochSeconds));
        assert_eq!(formats.get("native_ts"), None);
        assert_eq!(formats.get("body"), None);
        assert_eq!(formats.get("weird"), None);
        assert_eq!(formats.len(), 2);
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

    /// Build a hot window shard under `store` (index `index`, window `w`) holding one doc keyed `id`,
    /// and return its swappable handle — the shape `park_once` operates on.
    fn hot_window(
        store: &growlerdb_index::LocalIndexStore,
        index: &str,
        idx: &growlerdb_core::ResolvedIndex,
        w: i64,
        id: &str,
    ) -> growlerdb_engine::ShardHandle {
        use growlerdb_core::{
            CommitBatch, CompositeKey, Document, IndexWriter, LocatedDoc, SourceCheckpoint, Value,
        };
        use growlerdb_index::ShardId;
        use std::collections::BTreeMap;
        use std::sync::Arc;
        let shard = store.create_shard(&ShardId::window(index, w), idx).unwrap();
        let key = CompositeKey::new(vec![], vec![("id".into(), Value::from(id))]);
        let mut fields = BTreeMap::new();
        fields.insert("id".to_string(), Value::from(id));
        let doc = LocatedDoc {
            doc: Document::new(key, fields),
        };
        IndexWriter::write(
            &shard,
            &CommitBatch::from_upserts(vec![doc], SourceCheckpoint::iceberg(1), "b"),
        )
        .unwrap();
        // A real windowed shard carries an event-time zone-map; set one so the cold marker does too.
        shard.set_event_bounds(Some(w), Some(w)).unwrap();
        growlerdb_engine::ShardHandle::new(Arc::new(shard))
    }

    /// End-to-end drive of a single `park_once` pass (the core the background loop runs each tick):
    /// aged windows past the `hot_windows` policy are demoted to cold read-through — the handle swaps
    /// to a read-only shard sharing the window's `aux.redb`, the local bulk is evicted, and the
    /// window stays searchable — while the most-recent window is left hot. Then it is idempotent.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn park_once_demotes_aged_windows_and_keeps_them_searchable_read_through() {
        use growlerdb_core::{Query, TimeWindowing, WindowGranularity};
        use growlerdb_index::ShardId;

        let idx = resolved("events");
        let root = tempfile::tempdir().unwrap();
        let store = growlerdb_index::LocalIndexStore::open(root.path()).unwrap();

        // Three windows oldest→newest, each a hot shard with one doc.
        let windows = [1_000i64, 2_000, 3_000];
        let live: Vec<(i64, growlerdb_engine::ShardHandle)> = windows
            .iter()
            .map(|&w| (w, hot_window(&store, "events", &idx, w, &format!("d{w}"))))
            .collect();

        let backup_root = tempfile::tempdir().unwrap();
        let object_store = growlerdb_backup::fs_store(backup_root.path()).unwrap();
        let cache = growlerdb_index::RangeCache::new(8 * 1024 * 1024);
        // Keep the most-recent 1 window hot → w1000 and w2000 are victims.
        let windowing = TimeWindowing::new("ts", WindowGranularity::Daily).with_hot_windows(1);
        let def_json = serde_json::to_string(&idx).unwrap();

        let parked = park_once(
            &live,
            &store,
            &object_store,
            &cache,
            &idx,
            &windowing,
            "events",
            &def_json,
        )
        .await;
        assert_eq!(
            parked,
            vec![1_000, 2_000],
            "the two oldest windows are demoted; the newest is kept hot"
        );

        // The two oldest now serve read-only (cold read-through); the newest stays hot.
        assert!(live[0].1.current().is_read_only());
        assert!(live[1].1.current().is_read_only());
        assert!(
            !live[2].1.current().is_read_only(),
            "the most-recent window (within the hot policy) is untouched"
        );

        // Each parked window: marker written, local bulk evicted, aux kept, still searchable.
        for &w in &[1_000i64, 2_000] {
            let wd = store.shard_path(&ShardId::window("events", w));
            assert!(
                store.cold_marker("events", w).unwrap().is_some(),
                "w{w}: cold marker written"
            );
            assert!(
                !wd.join("index").exists(),
                "w{w}: local Tantivy bulk evicted"
            );
            assert!(wd.join("aux.redb").exists(), "w{w}: aux.redb kept local");

            let handle = live.iter().find(|(x, _)| *x == w).unwrap().1.clone();
            let cold = handle.current();
            let hits = tokio::task::spawn_blocking(move || {
                cold.search_all(&Query::parse(&format!("id:d{w}")).unwrap(), 10)
                    .unwrap()
                    .len()
            })
            .await
            .unwrap();
            assert_eq!(hits, 1, "w{w}: still searchable read-through after parking");
        }

        // Idempotent: a second pass finds both victims already cold and parks nothing.
        let again = park_once(
            &live,
            &store,
            &object_store,
            &cache,
            &idx,
            &windowing,
            "events",
            &def_json,
        )
        .await;
        assert!(
            again.is_empty(),
            "second pass is a no-op (windows already cold)"
        );
    }

    /// The park↔write race, interleaved deterministically: a write that commits **between a
    /// window's backup and its cold swap** advances the kept `aux.redb` checkpoint while its
    /// segments exist only in the local bulk the park would evict — serving the cold copy would
    /// silently lose the write (the checkpoint claims it's covered, so the connector never
    /// re-sends). `park_once` must detect the snapshot mismatch post-swap, swap the hot shard
    /// back, and succeed on the next (un-raced) pass.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_racing_the_park_backup_keeps_the_window_hot_and_loses_nothing() {
        use growlerdb_core::{Query, TimeWindowing, WindowGranularity};

        let idx = resolved("events");
        let root = tempfile::tempdir().unwrap();
        let store = growlerdb_index::LocalIndexStore::open(root.path()).unwrap();
        // Two windows; hot policy keeps 1 → w1000 is the park victim.
        let live: Vec<(i64, growlerdb_engine::ShardHandle)> = [1_000i64, 2_000]
            .iter()
            .map(|&w| (w, hot_window(&store, "events", &idx, w, &format!("d{w}"))))
            .collect();
        let backup_root = tempfile::tempdir().unwrap();
        let object_store = growlerdb_backup::fs_store(backup_root.path()).unwrap();
        let cache = growlerdb_index::RangeCache::new(8 * 1024 * 1024);
        let windowing = TimeWindowing::new("ts", WindowGranularity::Daily).with_hot_windows(1);
        let def_json = serde_json::to_string(&idx).unwrap();

        // Arm the interleave: after w1000's backup, a late write commits through the still-hot
        // handle (exactly what a broadcast delete / late upsert does in production).
        let victim = live[0].1.clone();
        *PARK_TEST_AFTER_BACKUP.lock().unwrap() = Some(Box::new(move |w| {
            use growlerdb_core::{
                CommitBatch, CompositeKey, Document, IndexWriter, LocatedDoc, SourceCheckpoint,
                Value,
            };
            assert_eq!(w, 1_000);
            let key = CompositeKey::new(vec![], vec![("id".into(), Value::from("late"))]);
            let mut fields = std::collections::BTreeMap::new();
            fields.insert("id".to_string(), Value::from("late"));
            IndexWriter::write(
                &*victim.current(),
                &CommitBatch::from_upserts(
                    vec![LocatedDoc {
                        doc: Document::new(key, fields),
                    }],
                    SourceCheckpoint::iceberg(2),
                    "late-batch",
                ),
            )
            .unwrap();
        }));

        let parked = park_once(
            &live,
            &store,
            &object_store,
            &cache,
            &idx,
            &windowing,
            "events",
            &def_json,
        )
        .await;
        *PARK_TEST_AFTER_BACKUP.lock().unwrap() = None;

        // The raced window was NOT parked: the mismatch was detected, the hot shard swapped back,
        // and the raced write is still served.
        assert!(parked.is_empty(), "the raced park must abort");
        assert!(
            !live[0].1.current().is_read_only(),
            "the window stays hot after the aborted park"
        );
        let hits = live[0]
            .1
            .current()
            .search_all(&Query::parse("id:late").unwrap(), 10)
            .unwrap();
        assert_eq!(hits.len(), 1, "the raced write is not lost");

        // The next pass (no race) parks cleanly — and the cold copy includes the late write.
        let parked = park_once(
            &live,
            &store,
            &object_store,
            &cache,
            &idx,
            &windowing,
            "events",
            &def_json,
        )
        .await;
        assert_eq!(parked, vec![1_000]);
        assert!(live[0].1.current().is_read_only());
        // Cold read-through search = blocking object-store reads → off the async runtime.
        let cold = live[0].1.current();
        let hits = tokio::task::spawn_blocking(move || {
            cold.search_all(&Query::parse("id:late").unwrap(), 10)
                .unwrap()
                .len()
        })
        .await
        .unwrap();
        assert_eq!(hits, 1, "the re-parked cold copy serves the late write");
    }
}
