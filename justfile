# GrowlerDB dev tasks. The toolchain is provided by mise (see mise.toml).
# Run `mise install` once, then these recipes. `just check` mirrors CI.

# Host directory holding the local embedding model (bge-small-en-v1.5), bind-mounted into the node
# containers at /models. Compose can't expand `~`, so resolve it HERE and export it to every recipe's
# environment as MODEL_HOST_DIR — the `stack` recipe's `model-fetch` one-shot writes the model here
# once per machine and node/node-catalog read it (GROWLERDB_MODEL_DIR=/models). Override the location
# with GROWLERDB_MODEL_DIR; it's reused across `just stack`/`just pipeline` and host `cargo test`.
export MODEL_HOST_DIR := env_var_or_default('GROWLERDB_MODEL_DIR', env_var('HOME') / '.cache/growlerdb/models')

# one-time: add toolchain components not in the minimal profile
setup:
    rustup component add rustfmt clippy

build:
    cargo build --workspace

test:
    cargo test --workspace

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

lint:
    cargo clippy --workspace --all-targets -- -D warnings

# non-Rust lint gates, mirrors the CI `lint` job. Needs: typos, shellcheck, actionlint,
# yamllint, and npx (node). Install: `brew install typos-cli shellcheck actionlint yamllint`.
lint-extra:
    typos
    shellcheck $(git ls-files '*.sh')
    actionlint
    yamllint .
    npx --yes markdownlint-cli2
    ./okf/check.sh

# OKF conformance: every concept .md carries a non-empty `type`
okf-check:
    ./okf/check.sh

# every lint gate: Rust clippy + the non-Rust linters
lint-all: lint lint-extra

# everything CI runs (Rust + UI)
check: fmt-check lint test ui-check

# Keep `target/` bounded the predictable way: if it exceeds CAP_GB (default 5), do a full
# `cargo clean`; otherwise leave it. Cargo never garbage-collects target/, and there is no SAFE
# selective GC — cargo-sweep's age/size modes prune the OLDEST files, which are your still-current
# third-party deps (tantivy/parquet/iceberg compile once and never get newer), forcing an expensive
# recompile. An all-or-nothing reset avoids that churn. A fresh build is ~2.5–3 GB (smaller now that
# profile.dev uses line-tables-only), so the cap is only hit after heavy rebuild-loop churn. Wire
# this into your loop or run it periodically.
trim CAP_GB="5":
    #!/usr/bin/env sh
    sz=$(du -sm target 2>/dev/null | cut -f1)
    cap=$(( {{ CAP_GB }} * 1024 ))
    if [ "${sz:-0}" -gt "$cap" ]; then
      echo "target/ is ${sz} MB > {{ CAP_GB }} GB cap — running cargo clean"
      cargo clean
    else
      echo "target/ is ${sz:-0} MB, within the {{ CAP_GB }} GB cap — nothing to do"
    fi

# Nuke ALL build artifacts (target/). The immediate full reclaim; the next build is from scratch
# (but with debug=line-tables-only, see Cargo.toml, the rebuilt target/ is much smaller).
clean:
    cargo clean

# --- UI (Svelte SPA under ui/) ------------------------------------------------
# install node deps (once)
ui-install:
    cd ui && npm install

# dev server with HMR (proxy /v1 to a running Engine, or set VITE_ENGINE_API)
ui-dev:
    cd ui && npm run dev

# production build → ui/dist (the Engine serves this via `serve/gateway --ui-dir ui/dist`)
ui-build:
    cd ui && npm run build

# type-check + unit tests
# Mirror the CI ui job's static gates (eslint → prettier → svelte-check → vitest); CI additionally
# builds and runs the Playwright smoke, which need the browser toolchain and stay CI-only.
ui-check:
    cd ui && npm run lint && npm run format:check && npm run check && npm run test

# bring up the full dev stack: MinIO + Polaris, bootstrap the catalog, seed growlerdb.docs
# NOTE: host clients/tests need `127.0.0.1 minio` in /etc/hosts (see deploy/compose/README.md)
up:
    docker compose -f deploy/compose/docker-compose.yml up -d minio createbuckets polaris
    deploy/compose/setup-polaris.sh
    # `run --rm` runs the seed as a one-off and returns its exit code, without the misleading
    # "Aborting on container exit" that `up --exit-code-from` prints when the one-shot job ends.
    docker compose -f deploy/compose/docker-compose.yml --profile seed run --rm --build seed

# Build the GrowlerDB container image once (lean `dist` profile, cached). Reused by stack/pipeline,
# so the heavy Rust build happens once — `up` below reuses it. Re-run this after changing Rust code.
build-image:
    docker compose -f deploy/compose/docker-compose.yml build node

# Safe UI-only reload: rebuild the image and hot-swap ONLY the gateway (which serves the SPA via
# `--ui-dir`). `--no-deps` means it never recreates `node` or bounces Polaris — recreating the node
# would rebuild the index, and an accidental Polaris (in-memory catalog) restart would recreate the
# source table and orphan the persisted index (stale keys → hydration "row not found").
# Use this after editing UI code instead of a blanket `up`.
ui-reload:
    docker compose -f deploy/compose/docker-compose.yml build node
    docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/pipeline.override.yml \
      --profile stack --profile pipeline up -d --no-deps --force-recreate gateway

# Streaming demo: generator → Redpanda → Iceberg → Spark connector → GrowlerDB index.
# Brings up the deps + bootstraps Polaris, builds the connector fat jar, then runs the full stack
# serving `telemetry_stream` plus the producer pipeline. The GrowlerDB image is built once and
# reused (no `--build`); run `just build-image` to rebuild after Rust changes. Watch ingest rate in
# Grafana (rate(growlerdb_ingested_docs_total)) and lag in the console Ingestion screen
# (<http://localhost:8081>). See deploy/compose/pipeline/README.md. Tear down with `just pipeline-down`.
pipeline:
    docker compose -f deploy/compose/docker-compose.yml up -d minio createbuckets polaris
    deploy/compose/setup-polaris.sh
    cd connector && mise exec -- mvn -q -DskipTests package
    docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/pipeline.override.yml \
      --profile stack --profile pipeline up -d

# Tear the streaming demo down, dropping ALL data volumes (`-v`) — MinIO, the index, AND the Polaris
# metastore (Postgres) — for a clean, consistent next `just pipeline`. (Polaris is persistent now, so
# an accidental restart no longer wipes the catalog/orphans the index; this full wipe is for an
# intentional from-scratch reset. For a keep-data stop, use `docker compose ... stop` instead.)
pipeline-down:
    docker compose -f deploy/compose/docker-compose.yml -f deploy/compose/pipeline.override.yml \
      --profile stack --profile pipeline down -v

# run the changelog-read demo in Spark local mode (builds the jar first).
# Self-contained (Hadoop catalog) — no MinIO/Polaris. Pass/fail via the exit code.
spark:
    cd connector && mise exec -- mvn -q -DskipTests package
    docker compose -f deploy/compose/docker-compose.yml --profile spark up --exit-code-from spark spark
    docker compose -f deploy/compose/docker-compose.yml --profile spark down

# connector pipeline integration test (Spark local mode, in-JVM, no cluster).
# changelog read → DocOp mapping → Write gRPC to an in-process Node stub.
connector-it:
    cd connector && mise exec -- mvn -q test -Dtest.excludedGroups= -Dgroups=integration

# cross-process e2e — the JVM connector writes to the real `growlerdb serve`
# (Rust) over gRPC and the committed docs are searchable. Builds the binary first.
connector-e2e:
    cargo build -p growlerdb-cli
    cd connector && mise exec -- mvn -q test -Dtest.excludedGroups= -Dgroups=e2e

# bring up the FULL single-host stack: deps + GrowlerDB (control-plane / node / gateway) +
# the LGTM observability stack — the Kubernetes-alternative deployment. Sequences deps →
# catalog setup → seed → the `stack` services (builds the growlerdb image on first run).
# Endpoints: Gateway REST http://localhost:8081/v1 · gRPC :50061 · Grafana http://localhost:3000
# NOTE: host clients/tests still need `127.0.0.1 minio` in /etc/hosts (see README).
stack:
    @echo "Model dir: {{ MODEL_HOST_DIR }} (bge-small-en-v1.5 fetched once, reused)"
    mkdir -p "$MODEL_HOST_DIR"
    docker compose -f deploy/compose/docker-compose.yml up -d minio createbuckets polaris
    deploy/compose/setup-polaris.sh
    docker compose -f deploy/compose/docker-compose.yml --profile seed run --rm --build seed
    # control-plane / node / gateway share one image (growlerdb-local:dev). `up --build` builds them
    # in parallel and they race to name the same tag ("image already exists") on Docker's containerd
    # store. Build the shared image ONCE, then start without --build.
    docker compose -f deploy/compose/docker-compose.yml build node
    docker compose -f deploy/compose/docker-compose.yml --profile stack --profile catalog up -d
    @echo ""
    @echo "Console:           http://localhost:8081  (demo/demo)"
    @echo "Grafana:           http://localhost:3000"
    @echo "Connect an agent:  just mcp-connect   (MCP over HTTP — Claude or any MCP client)"

# Mint a demo bearer and print paste-ready MCP connect snippets (Claude Code one-liner, the
# checked-in .mcp.json export, generic HTTP config, Claude Desktop bridge). Re-run to re-mint.
mcp-connect:
    deploy/compose/mcp-connect.sh

# Load the opt-in **movie demo corpus** (Wikipedia movie plots, CC-BY-SA) into the lakehouse and
# stand up its vector-enabled `movies` index. Stack must be up first (`just stack`); the index
# build embeds plot synopses locally with ONNX BGE (~500 docs/s in-container; default 5000 films
# ≈ ~45s including build + serve — raise `DEMO_DATA_SIZE`, or `=0`, for the full corpus).
demo-data:
    # `--profile stack` rides along on every invocation: node-movies depends on control-plane +
    # model-fetch (stack profile), and compose validates depends_on across the ACTIVE profile set.
    docker compose -f deploy/compose/docker-compose.yml --profile stack --profile demo-data build demo-data
    docker compose -f deploy/compose/docker-compose.yml --profile stack --profile demo-data run --rm demo-data
    docker compose -f deploy/compose/docker-compose.yml --profile stack --profile demo-data up -d node-movies
    @echo ""
    @echo "movies index building (local embedding; watch with:"
    @echo "  docker compose -f deploy/compose/docker-compose.yml --profile stack --profile demo-data logs -f node-movies)"
    @echo "Then search it — console index selector 'movies', or ask your MCP-connected agent."

# bring up Trino to explore the Iceberg source with SQL and compare with GrowlerDB
# Then: `docker compose -f deploy/compose/docker-compose.yml exec trino trino`
trino:
    docker compose -f deploy/compose/docker-compose.yml --profile trino up -d trino
    @echo 'Trino up on :8082. Query with: docker compose -f deploy/compose/docker-compose.yml exec trino trino'

# tear the full stack (and volumes) down
stack-down:
    docker compose -f deploy/compose/docker-compose.yml --profile stack --profile catalog --profile seed --profile trino down -v

# chaos drill: crash a core service on the running stack, assert it self-heals.
# SERVICE defaults to `node`; e.g. `just chaos gateway`. Requires `just stack` up first.
chaos SERVICE="node":
    deploy/compose/chaos/crash-recovery.sh {{ SERVICE }}

# chaos drill: kill the catalog (Polaris); assert search stays up + hydration recovers.
# Requires `just stack` up first and `jq`.
chaos-catalog:
    deploy/compose/chaos/catalog-outage.sh

# re-create the catalog + re-seed the sample table (stack already up)
seed:
    deploy/compose/setup-polaris.sh
    docker compose -f deploy/compose/docker-compose.yml --profile seed run --rm --build seed

down:
    docker compose -f deploy/compose/docker-compose.yml --profile seed down -v

# run the CLI, e.g. `just run search myidx 'hello'`
run *ARGS:
    cargo run -p growlerdb-cli -- {{ ARGS }}

# lint + render the Helm chart; ARGS pass through to `helm template` (e.g. --set ...)
helm-lint *ARGS:
    helm lint deploy/helm/growlerdb
    helm template gdb deploy/helm/growlerdb {{ ARGS }} >/dev/null && echo "helm template: OK"

# Build + push the images a scale run needs: server + connector + seed. CI does this via
# .github/workflows/scale-images.yml (which also builds the server `:dev` from merged main);
# this is the local (mini-PC) one-shot. `docker login ghcr.io` first. The signed multi-arch RELEASED
# server tags come from release.yml on a v* tag — this `:dev` push is the code-under-test build.
scale-images REGISTRY="ghcr.io/growlerdb" TAG="dev":
    docker build -t {{ REGISTRY }}/growlerdb:{{ TAG }} -f deploy/Dockerfile .
    docker push {{ REGISTRY }}/growlerdb:{{ TAG }}
    docker build -t {{ REGISTRY }}/growlerdb-connector:{{ TAG }} -f deploy/k8s/streaming/connector.Dockerfile .
    docker push {{ REGISTRY }}/growlerdb-connector:{{ TAG }}
    docker build -t {{ REGISTRY }}/growlerdb-seed:{{ TAG }} deploy/compose/seed
    docker push {{ REGISTRY }}/growlerdb-seed:{{ TAG }}

# Scale-harness smoke test: validate every workload offline (parse/schema), then — if a
# gateway is reachable at GROWLERDB_OS_URL — a tiny query round per workload. Catches workload-def +
# harness bugs before the cloud run. Full load→index→convergence needs the stack (see bench/scale).
smoke:
    bench/scale/smoke.sh

# Capture a scale/cluster-validation run BEFORE `terraform destroy`: metric time-series (Prometheus),
# pod logs (Loki), the harness results + run cost, and an audit record → a gitignored run dir under
# bench/scale/runs/, plus one compact row appended to the committed bench/scale/RUNLOG.md ledger.
# Needs PROM_URL (+ optional LOKI_URL/GRAFANA_URL) reachable (port-forwards). Screenshots are opt-in
# (`--screenshots`) and bounded. e.g. `just capture "cold-tier validation" --window-min 90 --cost '$5'`.
capture PURPOSE *ARGS:
    python bench/scale/capture.py --purpose {{ quote(PURPOSE) }} {{ ARGS }}
