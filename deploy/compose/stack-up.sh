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
# control-plane / pool nodes / gateway share one image: pull the latest official release once so the
# stack starts in a pull, not a ~10-minute source build. Falls back to building from your checkout when
# the image can't be pulled (developing GrowlerDB itself, or GROWLERDB_IMAGE=growlerdb-local:dev).
dc --profile stack pull -q controlplane || dc build controlplane

# ---------------------------------------------------------------------------------------------------
step 3 6 "Load the demo source tables (movies) before the pool"
# The HA pool serves docs/catalog/movies — it BUILDS each on assignment, so every source table must
# exist before the pool boots (its `--define-only` step blocks until each table resolves). `docs` +
# `catalog` were seeded in step 1; load `movies` (a SMALL 300-row Wikipedia movie-plots slice from the
# committed parquet — no download) here. `just demo-data` upgrades it to the full corpus.
DEMO_DATA_SIZE=300 \
  dc --profile demo-data run --rm --build demo-data

# ---------------------------------------------------------------------------------------------------
step 4 6 "HA placement pool (docs + catalog + movies at R=2)"
# Two interchangeable pool nodes with an IDENTICAL config, plus the control plane + gateway. Neither is
# designated anything: the CP places each index's primary round-robin across the pool and each node
# BUILDS the indexes it's made primary of from source (build-on-assignment); the other opens them
# read-through from the shared cold store. So kill either pool node and reads keep answering.
dc --profile stack --profile pool up -d --quiet-pull
# Convergence: once both pool nodes register, the control plane places each index's primary (after a
# brief settle so placement round-robins balanced) — the primaries then build + register and the
# replicas open read-through. Wait until `docs` is actually queryable through the gateway before
# declaring ready, so the console isn't hit mid-build.
printf '    converging (build-on-assignment; the pool self-organizes in a few seconds)'
for _ in $(seq 1 40); do
  if curl -fsS -m 5 http://localhost:8081/v1/search \
       -H 'content-type: application/json' -d '{"index":"docs","query":"*","limit":1}' 2>/dev/null \
       | grep -q '"total"'; then printf ' ready\n'; break; fi
  printf '.'; sleep 3
done

# ---------------------------------------------------------------------------------------------------
step 5 6 "Variant index 'events' (Iceberg v3 — Spark-seeded, connector-fed, Trino-hydrated)"
# Unlike the pyiceberg-seeded, self-cold-building indexes above, a variant table is Spark-seeded
# (format-version=3), connector-fed (released iceberg-rust can't scan a variant table — D49 — so the
# node skips the native build) and Trino-hydrated. So: build the connector jar (needs JDK 21 via mise),
# bring up Trino, Spark-seed the table, serve `events`, then populate it via the connector. All variant
# commands run with the full profile set active so node-events' cross-profile deps resolve.
( cd connector && mise exec -- mvn -q -DskipTests package )
dc --profile trino up -d --quiet-pull trino
VARIANT_PROFILES=(--profile stack --profile pool --profile trino --profile variant)
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
