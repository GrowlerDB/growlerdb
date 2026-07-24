# Local dev stack

GrowlerDB's runtime dependencies for development and tests:

- **MinIO** — S3-compatible object storage (`:9000` API, `:9001` console; `minioadmin`/`minioadmin`).
- **Apache Polaris** — Iceberg REST catalog (`:8181`), backed by a persistent Postgres metastore
  (`polaris-db`).
- **seed** — creates the sample Iceberg tables: `growlerdb.docs` (3 rows), the richer
  `growlerdb.catalog` (10 rows), and `growlerdb.readings`.

## One-time host setup

The catalog hands clients an S3 endpoint of `minio:9000` (the in-network name).
For **host** clients/tests to reach object storage, map that name to localhost:

```sh
echo "127.0.0.1 minio" | sudo tee -a /etc/hosts
```

## Usage

From the repo root:

```sh
just up      # MinIO + Polaris, bootstrap the `growlerdb` catalog, and seed the sample tables
just seed    # re-bootstrap the catalog + re-seed (stack already up)
just down    # tear everything down (removes volumes)
```

`just up` runs, in order: `docker compose up` (minio/polaris) → `setup-polaris.sh`
(creates the `growlerdb` catalog + grants admin to root) → the `seed` service
(pyiceberg, in-network, writes `growlerdb.docs` + `growlerdb.catalog` + `growlerdb.readings`).

## Full stack — GrowlerDB + LGTM (Kubernetes alternative)

The `stack` profile additionally runs **GrowlerDB itself** and a full observability stack —
a single-host alternative to the Kubernetes deployment, and the environment integration tests
run against.

```sh
just stack        # deps + seed, then control-plane + node + node-catalog + node-movies + gateway + LGTM
just mcp-connect  # mint a demo token + print MCP connect snippets (Claude or any agent)
just demo-data    # optional: upgrade movies to the full Wikipedia movie-plots corpus
just stack-down   # tear it all down (removes volumes)
```

Once up, `just mcp-connect` makes the stack an **agent retrieval tool** over MCP (Streamable HTTP
at `/mcp` on the gateway, same port as the console — no extra binary; see getting-started §7). The
console **opens on `movies`**, the demo's `VECTOR` star (`GROWLERDB_DEFAULT_INDEX`), so semantic and
hybrid search work out of the box. `just stack` ships a small 300-row `movies` slice from a committed
parquet (no download); `just demo-data` **upgrades** it to the full 5000-film corpus (`demo-data`
compose profile: a loader reloads `growlerdb.movies` in Iceberg, then `node-movies` cold-rebuilds its
vector index — see [docs: Demo corpus](https://docs.growlerdb.com/demo-corpus)).

`just stack` activates three Compose profiles: `stack` (control-plane, node, gateway, LGTM), `catalog`
(the second `node-catalog`), and `demo-data` (the `node-movies` vector node + its one-shot loader). The
streaming demo (`just pipeline`) activates `stack` + `pipeline` and deliberately leaves `node-catalog`
and `node-movies` out — there is no seeded `growlerdb.catalog` / `growlerdb.movies` source there.

Services (all on the compose network; published to the host):

| Service | Role | Host endpoint |
|---|---|---|
| **gateway** | public Engine API + the **console UI** (built-in auth, all-indexes routing) | **console `http://localhost:8081`**, REST `/v1`, gRPC `:50061` |
| **node** | builds + serves the `docs` index | gRPC `:50051`, health `:9102` |
| **node-catalog** | builds + serves the richer `catalog` index (`catalog` profile) | gRPC `:50052`, health `:9104` |
| **node-movies** | builds + serves the `movies` **`VECTOR`** index — the console default (`demo-data` profile) | gRPC `:50053`, health `:9106` |
| **controlplane** | cluster index registry + `/v1/login` (seeds `demo`/`demo` + `admin`/`admin`) | gRPC `:50071`, health `:9101` |
| **lgtm** | Grafana + Loki/Tempo/Mimir + OTLP | Grafana `http://localhost:3000`, OTLP `:4318` |

Open the **console at http://localhost:8081** — the gateway serves the built Svelte UI
(`--ui-dir`) and backs its screens: **Search** (`/v1/search` + hydration), **Indexes**
(`--control-plane` proxy: `/v1/indexes`, `/v1/source:describe`), **Ingestion** (sync status:
`/v1/ingestion` — source head vs. each shard's committed checkpoint), and **Observability** (native
ECharts panels via `--prometheus` → the bundled Prometheus). Deep dashboards link to Grafana.

The `node` runs `growlerdb serve … --register http://controlplane:50071 --advertise-addr
http://node:50051`, so it **announces the `docs` index to the control-plane registry** — that's why
it appears in the Indexes + Ingestion screens (a node-built index is otherwise invisible to the
registry until something calls `CreateIndex`).

- The GrowlerDB image is built from `deploy/Dockerfile` (multi-stage; `.dockerignore` keeps the
  build context small). `just stack` builds it on first run.
- Each GrowlerDB service is pointed at the in-network Polaris + MinIO via `GROWLERDB_*` env
  (overriding `IcebergConfig`'s `localhost` defaults) and ships traces (OTLP/HTTP) to `lgtm`.
- **Observability is pre-wired** (`lgtm/`, mounted into the otel-lgtm container): the bundled
  OTel Collector scrapes each service's `/metrics` into Prometheus, and Grafana auto-loads the
  **GrowlerDB SLIs** dashboard (`http://localhost:3000` → Dashboards) — query RED (rate/errors/
  latency), ingestion throughput, and hydration latency + stale-locator rate. Traces land in
  Tempo (search by service `growlerdb`). The error/ingestion/stale panels populate under the
  matching traffic (failed queries / connector writes / locator refreshes).
- Health/readiness (`/healthz`, `/readyz`) and Prometheus `/metrics` are on each service's
  `--metrics-addr` port; Docker healthchecks gate `depends_on` (the gateway waits for a ready node).
- Smoke test once up: `curl localhost:9103/readyz` (gateway ready). REST queries need a **login
  token** and an **index name** now (the stack serves `movies` + `docs` + `catalog` with built-in
  auth) — see the
  [getting-started tutorial](../../docs/getting-started.md) §2–§3 for the `/v1/login` → `/v1/search`
  flow.

## Notes / gotchas (learned the hard way)

- **Named volume, not bind mount** for MinIO (Docker Desktop refuses to create
  host bind-mount dirs in some setups).
- **Polaris is INTERNAL** and writes the initial table metadata itself, so the
  catalog's storage endpoint must be the in-network `minio:9000` — hence the
  `/etc/hosts` line for host clients.
- **Seed runs in-network** (the `seed` compose service) so it resolves `minio`.
- **Polaris is persistent** (Postgres-backed `polaris-db`), so a restart no longer wipes the catalog
  or orphans the index. The `down`/`stack-down` recipes pass `-v` to drop volumes for an intentional
  clean reset; `setup-polaris.sh` re-bootstraps the catalog idempotently.
