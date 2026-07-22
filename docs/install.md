---
title: Install & run modes
layout: default
nav_order: 3
---

# Install & run modes
{: .no_toc }

1. TOC
{:toc}

---

## Prerequisites

- Rust (stable). The toolchain is pinned via [mise](https://mise.jdx.dev).
- Docker and Compose, for the dependency stack (object storage plus Iceberg catalog).
- A source of truth: an Iceberg REST catalog (e.g. Apache Polaris) and S3-compatible object
  storage. The [Compose stack](deployment) provides MinIO and Polaris locally.

For the fastest path, one that skips all the build steps below, jump to
[Getting started](getting-started) (`just stack`).

## Prebuilt images & binaries (no build)

Don't want to build from source? Every release publishes signed, ready-to-run artifacts:

```sh
# Container image: multi-arch (amd64 + arm64), cosign-signed, with an SBOM.
docker pull ghcr.io/growlerdb/growlerdb:latest      # or pin a version, e.g. :0.2.0
docker run --rm ghcr.io/growlerdb/growlerdb:latest --help
```

- Release binaries: the `growlerdb` binary and checksums for each platform are attached to every
  [GitHub Release](https://github.com/GrowlerDB/growlerdb/releases).
- Helm chart: published to GHCR as an OCI artifact, `oci://ghcr.io/growlerdb/charts/growlerdb`
  (see [Deployment](deployment)).

The image tags follow SemVer: `:X.Y.Z` (immutable) plus moving `:X.Y`, `:X`, and `:latest`, so you
can pin exactly or float.

## Build from source

```sh
mise install            # install the pinned Rust toolchain
just setup              # add rustfmt + clippy
just build              # build the workspace (release: cargo build --release -p growlerdb-cli)
just check              # fmt + clippy + tests (the CI gate)
```

The single binary is `growlerdb` (`target/release/growlerdb`): one binary with four long-running
roles selected by subcommand, plus the offline index and maintenance commands.

```sh
growlerdb --help
```

## Connecting to the lakehouse

Every mode reads its Iceberg/object-store connection from the environment (defaults target the local
Compose stack). See [Configuration](configuration#environment) for the full list.

```sh
export GROWLERDB_CATALOG_URI=http://localhost:8181/api/catalog
export GROWLERDB_WAREHOUSE=growlerdb
export GROWLERDB_S3_ENDPOINT=http://localhost:9000
export GROWLERDB_CATALOG_CREDENTIAL='root:s3cr3t'
export GROWLERDB_S3_ACCESS_KEY=minioadmin
export GROWLERDB_S3_SECRET_KEY=minioadmin
```

## Run modes

### 1. Embedded (single binary)

Index a table, then search it, no servers needed. Best for laptops, CI, demos, and small corpora.

```sh
# Build the index from a source table (auto-maps the schema; --name defaults to the last segment).
growlerdb index growlerdb.docs --name docs

# Search it; --hydrate also fetches the authoritative rows from Iceberg.
growlerdb search docs 'title:iceberg' --limit 10 --hydrate
```

Maintenance commands operate on a local index:

| Command | What it does |
|---|---|
| `growlerdb sync <index>` | Append fast-path: index files added since the last checkpoint. |
| `growlerdb reconcile <index>` | Compare against Iceberg's current snapshot; fix drift. |
| `growlerdb rebuild <index>` | Hard reset: drop and rebuild from Iceberg (the backstop). |
| `growlerdb backup <index>` | Back up the shard (segments + locator/checkpoint + definition) to object storage. |
| `growlerdb restore <index>` | Restore the shard from a backup, or rebuild from Iceberg if none exists. |
| `growlerdb refresh-replica <index>` | Pull the latest sealed segments from the primary's backup (incremental) for a read replica. |

`backup`/`restore` read object-store credentials from `GROWLERDB_S3_*` and the bucket from
`GROWLERDB_BACKUP_BUCKET` (see [Configuration](configuration#environment)). After a restore the
connector resumes the tail from the backed-up checkpoint (exactly-once).

### 2. `serve` (a Node)

Host an already-built index over gRPC (Write + Search + Lookup + Suggest + Admin + System), and
optionally the REST API + console. Register with a control plane so it's cluster-visible.

```sh
growlerdb serve docs \
  --addr 0.0.0.0:50051 \
  --rest-addr 0.0.0.0:8080 \
  --metrics-addr 0.0.0.0:9102 \
  --register http://controlplane:50071 \
  --advertise-addr http://node:50051
```

### 3. `gateway` (the public Engine API)

Terminate the Engine API (gRPC + REST) and route to one or more nodes. This is the address clients
hit; it also serves the console UI and (optionally) the index-management, metrics, and OpenSearch
surfaces.

```sh
growlerdb gateway \
  --node-addr http://node:50051 \
  --addr 0.0.0.0:50061 \
  --rest-addr 0.0.0.0:8080 \
  --metrics-addr 0.0.0.0:9103 \
  --control-plane http://controlplane:50071 \
  --prometheus http://lgtm:9090 \
  --ui-dir /usr/share/growlerdb/ui \
  --opensearch
```

Front a sharded cluster instead of a single node with `--registry <registry.json> --index <name>`.
Enable authentication with `--oidc-issuer <url> --oidc-audience <aud>`. Without it the gateway is
open (see [Configuration → Auth](configuration#authentication--tenancy)).

### 4. `control-plane` (the registry)

The cluster-wide index registry (create / drop / list / ingestion status) over gRPC.

```sh
growlerdb control-plane --addr 0.0.0.0:50071 --metrics-addr 0.0.0.0:9101
```

## Health & metrics

Any long-running mode given `--metrics-addr` exposes `/healthz`, `/readyz`, and Prometheus
`/metrics` on that port. Use them for liveness/readiness gating.

```sh
curl -f localhost:9103/readyz
```

## Next

- [Configuration](configuration): flags, env, and the index-definition YAML.
- [Reference](reference): the query language and REST/gRPC API.
- [Deployment](deployment): Compose and Kubernetes (Helm).
