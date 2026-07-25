#!/usr/bin/env bash
# Bring up the FULL single-host demo stack: deps (MinIO + Polaris) → catalog bootstrap → seed →
# GrowlerDB (control-plane / nodes / gateway) + the LGTM observability stack → the demo indexes
# (docs, catalog, movies, and the Iceberg v3 variant `events`). Driven by `just stack` /
# `just stack-dev`; the prose that used to live inline in the recipe (and got echoed line-by-line at
# runtime) is captured here as ordinary shell comments so startup shows clean step headers instead.
#
# Runs from anywhere (cd's to the repo root). Idempotent — safe to re-run.
set -euo pipefail
cd "$(dirname "$0")/../.." || exit 1

COMPOSE_FILE="deploy/compose/docker-compose.yml"
dc() { docker compose -f "$COMPOSE_FILE" "$@"; }

step() { printf '\n\033[1;36m==> [%s/%s] %s\033[0m\n' "$1" "$TOTAL" "$2"; }
TOTAL=6

# ---------------------------------------------------------------------------------------------------
step 1 6 "Dependencies + catalog (MinIO, Polaris) and seed tables"
# The local embedding model (bge-small-en-v1.5) is bind-mounted into the node containers; MODEL_HOST_DIR
# is resolved and exported by the justfile (compose can't expand `~`).
echo "    model dir: ${MODEL_HOST_DIR:-<unset>} (fetched once, reused)"
mkdir -p "${MODEL_HOST_DIR:?MODEL_HOST_DIR must be set — run via 'just stack'}"
dc up -d --quiet-pull minio createbuckets polaris
deploy/compose/setup-polaris.sh
# Writes growlerdb.docs (3-row minimal E2E table) and growlerdb.catalog (10 rows — one field of every type).
dc --profile seed run --rm --build seed

# ---------------------------------------------------------------------------------------------------
step 2 6 "GrowlerDB image (pull released, or build your checkout)"
# control-plane / node / gateway share one image: pull the latest official release once so the stack
# starts in a pull, not a ~10-minute source build. Falls back to building from your checkout when the
# image can't be pulled (developing GrowlerDB itself, or GROWLERDB_IMAGE=growlerdb-local:dev).
dc --profile stack pull -q node || dc build node

# ---------------------------------------------------------------------------------------------------
step 3 6 "Core services + catalog index"
dc --profile stack --profile catalog up -d --quiet-pull
# Force-recreate the VECTOR index node for a clean COLD rebuild against the freshly re-seeded `catalog`
# table. On a re-run `serve` background-syncs the new snapshot into the LEXICAL segments but not the
# vector sidecars (TASK-326), so without a rebuild semantic hits go stale ("row not found").
dc --profile stack --profile catalog up -d --force-recreate node-catalog

# ---------------------------------------------------------------------------------------------------
step 4 6 "Movies demo index (semantic + hybrid out of the box)"
# A SMALL 300-row Wikipedia movie-plots slice (CC-BY-SA) from the COMMITTED local parquet — no download,
# ~1s embed at build — so semantic/hybrid work immediately and the console lands here
# (GROWLERDB_DEFAULT_INDEX=movies). `just demo-data` upgrades it to the full corpus.
DEMO_DATA_SIZE=300 \
  dc --profile stack --profile demo-data run --rm --build demo-data
# `--force-recreate`: the run above re-seeds `movies`, so cold-rebuild the node against the current table.
dc --profile stack --profile demo-data up -d --force-recreate node-movies

# ---------------------------------------------------------------------------------------------------
step 5 6 "Variant index 'events' (Iceberg v3 — Spark-seeded, connector-fed, Trino-hydrated)"
# Unlike the pyiceberg-seeded, self-cold-building indexes above, a variant table is Spark-seeded
# (format-version=3), connector-fed (released iceberg-rust can't scan a variant table — D49 — so the
# node skips the native build) and Trino-hydrated. So: build the connector jar (needs JDK 21 via mise),
# bring up Trino, Spark-seed the table, serve `events`, then populate it via the connector. All variant
# commands run with the full profile set active so node-events' cross-profile deps resolve.
( cd connector && mise exec -- mvn -q -DskipTests package )
dc --profile trino up -d --quiet-pull trino
VARIANT_PROFILES=(--profile stack --profile catalog --profile demo-data --profile trino --profile variant)
dc "${VARIANT_PROFILES[@]}" run --rm seed-events
dc "${VARIANT_PROFILES[@]}" up -d --force-recreate node-events
dc "${VARIANT_PROFILES[@]}" run --rm connector-events

# ---------------------------------------------------------------------------------------------------
step 6 6 "Ready"
cat <<'EOF'

    Console:           http://localhost:8081  (demo/demo)  — opens on 'movies' (try Semantic/Hybrid)
    Indexes:           movies · catalog · docs · events (Iceberg v3 variant: flatten + shapes)
    Grafana:           http://localhost:3000
    Connect an agent:  just mcp-connect   (MCP over HTTP — Claude or any MCP client)
EOF
