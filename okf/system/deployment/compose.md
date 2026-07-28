---
type: Concept
title: Docker Compose
description: The single-host stack for dev and CI — dependencies + GrowlerDB + observability.
tags: [deployment, compose, dev]
resource: /deploy/compose
timestamp: 2026-07-20T00:00:00
---

# Docker Compose

A single-host stack (`deploy/compose`) that brings up the dependencies
([MinIO](/system/runtime/dependencies/object-storage/minio.md),
[Polaris](/system/runtime/dependencies/iceberg-catalog/polaris.md) + Postgres), and — in the `stack`
profile — GrowlerDB itself (control-plane + **two nodes** + gateway) plus the
[LGTM](/system/runtime/dependencies/lgtm.md) stack. The fastest path to a running GrowlerDB, and the
environment [CI e2e](/quality/ci-and-gates.md) runs against.

`just stack` is a thin wrapper over **`deploy/compose/stack-up.sh`**, which sequences the bring-up
(deps → catalog bootstrap → seed → core services → the `movies` and variant `events` demo indexes) and
prints numbered step headers rather than echoing the orchestration line by line.

## Notes

Profiles: `seed` (sample tables), `stack` (control plane + gateway + LGTM), `pool` (the two
interchangeable placement-pool nodes serving docs/catalog/movies at R=2), `demo-data` (the movies
source loader), `pipeline` (the streaming demo with Redpanda). Long-running services carry `restart:` policies
(self-heal); chaos drills exercise recovery ([reliability](/quality/reliability.md)).

The GrowlerDB services default to the **latest published release image** (`GROWLERDB_IMAGE`
overrides, e.g. to pin a version or point at a locally-built tag), so a first `just stack` is a pull,
not a ~10-minute source build. To run the **working checkout** end to end instead — engine binary +
console, so `/v1/config`, the UI, and search all reflect local changes — **`just stack-dev`** pins
`GROWLERDB_IMAGE` to a local-only tag, which makes the pull miss and builds the shared image from
`deploy/Dockerfile`.

**External lakehouse (`external.yml`):** a companion file (`deploy/compose/external.yml` + `.env`) runs
only GrowlerDB (control-plane + node + gateway, off the published image) against a user's **own**
external Iceberg REST catalog + S3 store — no bundled MinIO/Polaris/seed. It's the "day 2" step after
the demo; see the [getting-started site](/product/interfaces/website.md) *Connecting your own Iceberg
table* page for the walkthrough and limitations (REST-only catalog, static S3 keys, forced path-style).

**Demo indexes on an HA placement pool.** The `seed` profile writes `growlerdb.docs` (3 rows, the
minimal E2E table) *and* the richer `growlerdb.catalog` (10 rows — one field of every type); a
`demo-data` one-shot loads `growlerdb.movies`. All three are served **not per-index but on a
[placement pool](/system/decisions/d52-placement-pool.md)** ([D52](/system/decisions/d52-placement-pool.md)/[D53](/system/decisions/d53-unit-replication.md)):
**two interchangeable pool nodes** (`pool-a`/`pool-b`) with an **identical** config run
`serve-pool --index docs --index catalog --index movies` at **`GROWLERDB_REPLICATION_FACTOR=2`**, so the
demo is shaped like a production deploy. Neither node is designated anything — each `--define-only`s the
three indexes (schema only), and the control plane's placement sweep distributes each index's **primary
round-robin** across the pool while the other node opens it **read-through** from the shared cold store
(MinIO `growlerdb-cold`); a node made primary **builds that index from source on assignment**
(build-on-assignment). So killing either pool node is a **zero-gap read failover** — HA is "run two
nodes, the control plane does the rest", no per-node build/primary designation. The single
`--all-indexes` [gateway](/system/runtime/components/gateway.md) routes each request to its named index
([D35 multi-index routing](/system/decisions/d35-multi-index-routing.md)) and resolves lazily. The pool
lives in the `pool` profile — `just stack` co-activates `stack`+`pool`, but the streaming demo
(`just pipeline`, `stack`+`pipeline`) deliberately excludes it (it serves a single windowed
`telemetry_stream` node instead). The pool self-organizes after the ~30s placement grace, so `just
stack` waits for the first index to be queryable before reporting ready.
The [getting-started](/product/interfaces/website.md) **query playground** exercises the `catalog`
index through the gateway — every Lucene/KQL operator (term, phrase, keyword, set, numeric/float/date
range, CIDR, wildcard, prefix, fuzzy, boost, bool, `NOT`, match-all, regex) against known rows. With
`docs`, `catalog`, and `movies` served behind the `--all-indexes` gateway (no *served-default* index),
every search / `keys:get` request names its index; the **console's** default selection is separate — a
UI convenience set via `GROWLERDB_DEFAULT_INDEX` (→ `movies`, so a fresh visitor lands on a vector
index with semantic/hybrid a click away).

**Movie corpus (`movies` — small by default, full via `just demo-data`):** a slice of Wikipedia movie
plots (CC-BY-SA, decade-balanced) at the scale where retrieval *quality* shows — semantic vs lexical
vs hybrid visibly differ, facets are real (genre / origin / decade), and MCP agent Q&A has substance
the 10-row `catalog` can't give. **`just stack` ships a small 300-row slice** from a committed local
parquet (`demo-data/local/movies-300.parquet` — no download, ~1s embed at build) as the console's
default index, so all of that works out of the box. **`just demo-data` upgrades it to the full
corpus:** a loader one-shot downloads the pre-sliced parquet (a GitHub release asset;
`DEMO_DATA_URL`/`DEMO_DATA_FILE` overridable, `DEMO_DATA_SIZE` caps rows — default 5000) and writes
`growlerdb.movies` into Iceberg **first** (before the pool boots — its `--define-only` step blocks
until each source table resolves); the **pool** then builds + serves the vector-enabled index
(`movies.yaml` — `plot_vec` embedded locally from a short **synopsis** to keep embedding fast; full
`plot`/`title` **cached** so agents answer from `search` alone) on assignment, so the `--all-indexes`
gateway routes to it and the demo token (allowlist `docs,catalog,movies`) may query it. The slicer
(`demo-data/build_movies_slice.py`) regenerates the asset.

**Vector indexes cold-rebuild on (re)load.** `just demo-data` reloads `growlerdb.movies` to the full
corpus and then **wipes the pool's local data and recreates the two pool nodes**, so build-on-assignment
rebuilds all three indexes from source (movies now full). Why not an in-place refresh: a running node
that background-syncs a reloaded source refreshes the **lexical** segments but **not** the vector
sidecars (sync/reindex re-embed is TASK-326), so the ANN sidecars would go stale and **semantic hits
fail to hydrate** ("row not found") while lexical still works. The pool data volumes are a derived,
rebuildable store (the authoritative data is Iceberg), so wiping them is safe. This is a demo workaround
for the engine gap; a durable fix is TASK-326.

**Local-embeddings vector demo:** the `catalog` index carries a `body_vec`
[VECTOR field](/product/functional/search/vector.md) over its `body`, embedded at ingest with the local
**bge-small-en-v1.5** model — so the demo exercises **semantic + hybrid search** (inline in the console
Search screen's Semantic/Hybrid modes) and the [MCP server](/product/interfaces/mcp-server.md) against
real data, **keyless** (no API key, no egress). The model is provisioned **once per machine** by a `model-fetch` one-shot into a
host-bind-mounted `${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}` (idempotent — skipped when already
present, and shared with local `cargo`/eval runs), mounted on both **pool nodes** (which embed at
build/query time when they hold a VECTOR index; the gateway does not — [D43](/system/decisions/d43-node-local-query-embedding.md)).
The published image stays lean — the model is **not** baked in. Per [D42](/system/decisions/d42-retrieval-first.md)
the demo is retrieval-only: it returns governed coordinates + citations and never calls an LLM.

**Agent quick-connect:** `just mcp-connect` (→ `deploy/compose/mcp-connect.sh`) mints a demo bearer
via `/v1/login` and prints paste-ready snippets for connecting any HTTP-capable MCP client to the
gateway's [`/mcp` transport](/product/interfaces/mcp-server.md) — a Claude Code one-liner, a generic
HTTP config block, and a Claude Desktop bridge. The repo's checked-in `.mcp.json` points Claude Code
at the demo server automatically (auth via the `GROWLERDB_DEMO_TOKEN` env var the script prints), and
`just stack` ends by advertising the hookup. Tokens are session-scoped; re-run to re-mint.
