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

## Notes

Profiles: `seed` (sample tables), `stack` (GrowlerDB + LGTM), `catalog` (the second `node-catalog`),
`pipeline` (the streaming demo with Redpanda). Long-running services carry `restart:` policies
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

**Two demo indexes:** the `seed` profile writes `growlerdb.docs` (3 rows, the minimal
E2E table) *and* the richer `growlerdb.catalog` (10 rows — one field of every type). Each is served from
its own node (`node` → `docs`, `node-catalog` → `catalog`, built from `catalog.yaml`), and the single
`--all-indexes` [gateway](/system/runtime/components/gateway.md) routes each request to its named index
([D35 multi-index routing](/system/decisions/d35-multi-index-routing.md)). `node-catalog` lives in its
own `catalog` profile — `just stack` co-activates `stack`+`catalog`, but the streaming demo
(`just pipeline`, `stack`+`pipeline`) deliberately excludes it (no seeded `growlerdb.catalog` source
there), and the gateway resolves indexes lazily rather than hard-depending on it.
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
`growlerdb.movies` into Iceberg **first**; then `node-movies` builds + serves the vector-enabled index
(`movies.yaml` — `plot_vec` embedded locally from a short **synopsis** to keep embedding fast; full
`plot`/`title` **cached** so agents answer from `search` alone) and registers, so the `--all-indexes`
gateway routes to it and the demo token (allowlist `docs,catalog,movies`) may query it. The slicer
(`demo-data/build_movies_slice.py`) regenerates the asset.

**Vector indexes cold-rebuild on (re)load.** `node-catalog` and `node-movies` **wipe their index dir
and rebuild from scratch** on start, and `just stack` / `just demo-data` **force-recreate** them after
re-seeding their tables. Why: a running `serve` that background-syncs a reloaded source refreshes the
**lexical** segments but **not** the vector sidecars (sync/reindex re-embed is TASK-326), so on a re-run
the ANN sidecars would go stale and **semantic hits fail to hydrate** ("row not found") while lexical
still works. The index dir is a derived, rebuildable store (the authoritative data is Iceberg), so
wiping it is safe. This is a demo workaround for the engine gap; a durable fix is TASK-326.

**Local-embeddings vector demo:** the `catalog` index carries a `body_vec`
[VECTOR field](/product/functional/search/vector.md) over its `body`, embedded at ingest with the local
**bge-small-en-v1.5** model — so the demo exercises **semantic + hybrid search** (inline in the console
Search screen's Semantic/Hybrid modes) and the [MCP server](/product/interfaces/mcp-server.md) against
real data, **keyless** (no API key, no egress). The model is provisioned **once per machine** by a `model-fetch` one-shot into a
host-bind-mounted `${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}` (idempotent — skipped when already
present, and shared with local `cargo`/eval runs), mounted on `node` + `node-catalog` (which embed at
ingest and query time; the gateway does not — [D43](/system/decisions/d43-node-local-query-embedding.md)).
The published image stays lean — the model is **not** baked in. Per [D42](/system/decisions/d42-retrieval-first.md)
the demo is retrieval-only: it returns governed coordinates + citations and never calls an LLM.

**Agent quick-connect:** `just mcp-connect` (→ `deploy/compose/mcp-connect.sh`) mints a demo bearer
via `/v1/login` and prints paste-ready snippets for connecting any HTTP-capable MCP client to the
gateway's [`/mcp` transport](/product/interfaces/mcp-server.md) — a Claude Code one-liner, a generic
HTTP config block, and a Claude Desktop bridge. The repo's checked-in `.mcp.json` points Claude Code
at the demo server automatically (auth via the `GROWLERDB_DEMO_TOKEN` env var the script prints), and
`just stack` ends by advertising the hookup. Tokens are session-scoped; re-run to re-mint.
