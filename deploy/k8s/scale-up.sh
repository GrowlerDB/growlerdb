#!/usr/bin/env bash
# One-command workload-driven scale-test deploy: brings up the full GrowlerDB
# pipeline on a fresh k3s cluster (provisioned by deploy/iac) in the CORRECT order — the order
# matters and each step gates the next, which is easy to get wrong by hand:
#
#   render (workload → generator/connector manifests + index def)
#     → namespace + ghcr-pull secret
#       → deps (MinIO/Polaris/Postgres)                     [nodes crash without the catalog]
#         → generator (creates the workload's source table) [nodes crash building a missing table]
#           → GrowlerDB (helm, values-scale.yaml + the workload's index.yaml verbatim)
#             → streaming connector (--nodes sized to shards) [must equal shard count or it aborts]
#               → observability bundle
#                 → verify a query returns 200
#
# The WHOLE deploy derives from one workload definition (bench/scale/workloads/<name>/): its
# corpus.py streams the source, its index.yaml is the shard schema, its key/mapping become the
# connector's field lists. Switching workloads is configuration, never a manifest edit.
#
# Prereqs: KUBECONFIG points at the cluster; a GitHub PAT with read:packages (to pull the private
# images); python3 (pyyaml auto-installed into a scratch venv for the render step). Usage:
#   export KUBECONFIG=deploy/iac/kubeconfig.yaml
#   GHCR_PAT=ghp_xxx GH_USER=you deploy/k8s/scale-up.sh
# Knobs (env): WORKLOAD=http_logs, NAMESPACE=growlerdb, SHARDS=6, IMAGE_TAG=dev.
#
# IMAGE_TAG — the SERVER image the run deploys — must be built from the code under test. `release.yml`
# only builds `growlerdb:latest`/`:X.Y.Z` on a *release*, so after merging (pre-release) `latest` LAGS
# main; deploying it silently runs stale code. The `scale-images` workflow builds `growlerdb:dev`
# (+ commit SHA) from merged main — pin `IMAGE_TAG=dev` or the commit SHA, NOT `latest`. This script
# warns on `latest` and prints the deployed binary's `--version` after the nodes come up so you can
# confirm the code that's actually running.
set -euo pipefail

WORKLOAD="${WORKLOAD:-http_logs}"
NAMESPACE="${NAMESPACE:-growlerdb}"
SHARDS="${SHARDS:-6}"
GENERATORS="${GENERATORS:-1}"   # generator pod replicas — raise to parallelize ingest
# STAGE splits the bring-up so ingest can be measured as a clean COLD sync (comparison-plan.md):
#   deps    — deps + generator + observability, but NOT the GrowlerDB serving stack (nodes/connector),
#             so generation runs with nothing ingesting it. compare_run's generate phase uses this.
#   serving — the GrowlerDB serving stack ONLY (helm nodes + connector + verify), deployed AFTER the
#             source table is generated and settled, so the connector cold-backfills a static table
#             (mirrors bringing OpenSearch+Data Prepper up post-generation). This is the measured sync.
#   all     — the original single-shot bring-up (unchanged default; GrowlerDB streams during generation).
STAGE="${STAGE:-all}"
# DEFINE_ONLY: bring nodes up EMPTY (define the index, skip the boot-build from source) so the
# streaming connector performs the entire, measured cold backfill — symmetric with Data Prepper CDC and
# keeping ingest on the metric-emitting connector write path. Default off (nodes boot-build as before).
DEFINE_ONLY="${DEFINE_ONLY:-false}"
IMAGE_TAG="${IMAGE_TAG:-dev}"
GH_USER="${GH_USER:-}"
GHCR_PAT="${GHCR_PAT:-}"
HERE="$(cd "$(dirname "$0")" && pwd)"        # deploy/k8s
HELM_CHART="$HERE/../helm/growlerdb"
REPO_ROOT="$(cd "$HERE/../.." && pwd)"

: "${KUBECONFIG:?set KUBECONFIG to the cluster kubeconfig (deploy/iac/kubeconfig.yaml)}"
if [ -z "$GHCR_PAT" ] || [ -z "$GH_USER" ]; then
  echo "set GHCR_PAT (read:packages) + GH_USER for the ghcr-pull secret"; exit 1
fi

# `latest` is only rebuilt on a RELEASE, so pre-release it lags main → stale server code.
# The `scale-images` workflow builds `growlerdb:dev` (+ commit SHA) from merged main; pin one of those.
if [ "$IMAGE_TAG" = "latest" ]; then
  printf '\033[1;33m! IMAGE_TAG=latest is only rebuilt on a release — it may LAG merged main.\n' >&2
  printf '  Deploy the code under test: IMAGE_TAG=dev (or the commit SHA) built by the scale-images workflow.\033[0m\n' >&2
fi

say() { printf '\n\033[1;36m== %s ==\033[0m\n' "$*"; }

deploy_observability() {
  say "observability bundle (Prometheus / Grafana / node-exporter / kube-state / Trino / Loki+Promtail)"
  kubectl apply -f "$HERE/observability/"
}

say "0/7 render the '$WORKLOAD' workload → generator/connector manifests + index def"
# The render needs pyyaml; reuse the smoke venv pattern so the script runs on a bare host.
PY=python3
if ! $PY -c 'import yaml' >/dev/null 2>&1; then
  VENV="${SMOKE_VENV:-/tmp/gdb-scale-venv}"
  [ -x "$VENV/bin/python" ] || python3 -m venv "$VENV"
  "$VENV/bin/pip" install --quiet --disable-pip-version-check pyyaml >/dev/null
  PY="$VENV/bin/python"
fi
RENDER_DIR="$(mktemp -d)"
$PY "$REPO_ROOT/bench/scale/harness.py" render "$WORKLOAD" \
  --shards "$SHARDS" --namespace "$NAMESPACE" --generators "$GENERATORS" --out "$RENDER_DIR"
# TABLE / INDEX / INDEX_DEF for the gates + helm flags below.
# shellcheck source=/dev/null
source "$RENDER_DIR/workload.env"

# Detect a WINDOWED workload: its index.yaml declares `windowing:`. A windowed index uses a
# different node topology — nodes start EMPTY and serve control-plane-ASSIGNED time-window shards (no
# --shards/--shard-ordinal); the connector streams each row to its window's owning node (resolved from
# the live control plane) and the gateway hot-reloads windows. The chart renders that topology when
# index.windowed=true. See okf/quality/known-limitations/windowed-k8s-topology.md.
WINDOWED=false
if grep -qE '^[[:space:]]*windowing:' "$INDEX_DEF"; then
  WINDOWED=true
  say "workload '$WORKLOAD' is WINDOWED — deploying the time-windowed node topology"
fi

say "1/7 namespace + ghcr-pull image secret"
kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret docker-registry ghcr-pull \
  --docker-server=ghcr.io --docker-username="$GH_USER" --docker-password="$GHCR_PAT" \
  --dry-run=client -o yaml | kubectl apply -f -

# --- deps + generator: run for `deps`/`all`, skip for `serving` (the source table already exists). ---
if [ "$STAGE" != serving ]; then
say "2/7 deps (MinIO / Polaris / Postgres) — the Iceberg catalog + object store"
kubectl apply -k "$HERE/deps"   # kustomization pins namespace: growlerdb
kubectl -n "$NAMESPACE" rollout status deploy/minio --timeout=180s
kubectl -n "$NAMESPACE" rollout status deploy/polaris --timeout=180s
kubectl -n "$NAMESPACE" wait --for=condition=complete job/polaris-catalog-setup --timeout=240s

say "3/7 generator — creates $TABLE and starts feeding it"
kubectl apply -f "$RENDER_DIR/generator.yaml"
echo "waiting for the $TABLE table to be created ..."
# The "created <table>" line is emitted ONCE at generator startup, then batch lines stream every few
# seconds — so a small `--tail` scrolls past it and the grep never matches, burning the whole timeout
# (TASK-345). Read the full log (`--tail=-1`) so the one-time line is always found.
for _ in $(seq 1 40); do
  kubectl -n "$NAMESPACE" logs deploy/growlerdb-generator --tail=-1 2>/dev/null | grep -q "created $TABLE" && { echo "  table created"; break; }
  sleep 6
done
# Guardrail: the generator's append-retry MUST regenerate FRESH rows per attempt (`make_cols`). The
# pre-fix code re-committed the same batch on a false-negative commit → duplicate request_ids (~1.5%,
# breaks convergence-by-distinct; root-caused by local repro). A stale ConfigMap silently reintroduces
# it — assert the DEPLOYED code carries the fix before generating a whole corpus against it.
if ! kubectl -n "$NAMESPACE" get configmap growlerdb-workload-corpus \
     -o jsonpath='{.data.corpus\.py}' 2>/dev/null | grep -q "make_cols"; then
  echo "FATAL: deployed generator corpus.py lacks the idempotent append-retry (make_cols) — dup risk." >&2
  echo "  Re-render + re-apply the workload ConfigMap and rollout-restart the generator, then retry." >&2
  exit 1
fi
echo "  generator corpus.py carries the idempotent append-retry (make_cols) ✓"
fi  # end deps+generator (skipped for STAGE=serving)

# STAGE=deps stops here: bring up observability (so Prometheus scrapes generation + the later cold
# sync) and exit WITHOUT the GrowlerDB serving stack, so generation runs with nothing ingesting it.
if [ "$STAGE" = deps ]; then
  deploy_observability
  say "STAGE=deps complete — deps + generator + observability up; GrowlerDB serving stack NOT deployed"
  exit 0
fi

say "4/7 GrowlerDB (helm) — $SHARDS $([ "$WINDOWED" = true ] && echo 'windowed nodes' || echo 'shards') serving $INDEX"
# Cold-tiering (TASK-229): windowed runs exercise automatic park/revive. On by default for a windowed
# workload — backups land in the in-cluster MinIO `growlerdb-backups` bucket (created by minio.yaml),
# aged windows park per the index def's `hot_windows`. Disable with COLD_TIER=false.
COLD_TIER="${COLD_TIER:-true}"
COLD_ARGS=()
if [ "$WINDOWED" = true ] && [ "$COLD_TIER" = true ]; then
  COLD_ARGS=(--set coldTier.enabled=true
             --set coldTier.backupBucket="${COLD_BUCKET:-growlerdb-backups}"
             --set coldTier.parkIntervalSecs="${COLD_PARK_INTERVAL_SECS:-120}")
  say "cold-tiering ON — park every ${COLD_PARK_INTERVAL_SECS:-120}s → bucket ${COLD_BUCKET:-growlerdb-backups} (hot_windows from the index def)"
fi
# Scale-limit license (TASK-346): with $GROWLERDB_LICENSE set, the control plane admits all $SHARDS
# nodes deterministically instead of relying on the free-tier (3) registration leak. Passed as an env
# (never committed) via --set-string. Empty = free tier (works today only because the cap leaks).
LICENSE_ARGS=()
if [ -n "${GROWLERDB_LICENSE:-}" ]; then
  LICENSE_ARGS=(--set-string credentials.license="$GROWLERDB_LICENSE")
  say "scale-limit license provided — control plane will admit all $SHARDS nodes (no free-tier leak)"
fi
helm upgrade --install gdb "$HELM_CHART" -f "$HELM_CHART/values-scale.yaml" -n "$NAMESPACE" \
  --set image.tag="$IMAGE_TAG" --set index.shards="$SHARDS" \
  --set index.windowed="$WINDOWED" --set index.defineOnly="$DEFINE_ONLY" \
  --set index.name="$INDEX" --set index.sourceTable="$TABLE" \
  ${COLD_ARGS[@]+"${COLD_ARGS[@]}"} ${LICENSE_ARGS[@]+"${LICENSE_ARGS[@]}"} \
  --set-file index.definition="$INDEX_DEF"
echo "waiting for $SHARDS node $([ "$WINDOWED" = true ] && echo pods || echo shards) to become Ready ..."
for _ in $(seq 1 40); do
  ready=$(kubectl -n "$NAMESPACE" get pods --no-headers 2>/dev/null | grep -E "gdb-growlerdb-node-[0-9]" | grep -c "1/1" || true)
  echo "  shards ready: ${ready:-0}/$SHARDS"; [ "${ready:-0}" -ge "$SHARDS" ] && break; sleep 10
done

# Print the deployed server binary's version so a stale image (e.g. a floating `latest` that
# lags the code under test) can't hide behind a green rollout. The scale-images build stamps
# GROWLERDB_VERSION=dev-<sha>; an unstamped in-tree build reports 0.0.0.
echo "deployed server version (image tag '$IMAGE_TAG'):"
kubectl -n "$NAMESPACE" exec statefulset/gdb-growlerdb-node -- growlerdb --version 2>/dev/null \
  | sed 's/^/  /' || echo "  (could not read --version)"

# A WINDOWED gateway only becomes /readyz-ready once ≥1 window exists, and windows are created by the
# connector's streamed writes — so for a windowed index deploy the connector BEFORE the gateway-ready
# wait, else that wait always times out. For an ordinal index the gateway is ready as soon as the
# shards register, so deploy the connector after (its --nodes must equal the now-confirmed ready
# shard count).
deploy_connector() {
  say "5/7 streaming connector — fields from the workload's index.yaml, --nodes sized to $SHARDS shards"
  kubectl apply -f "$RENDER_DIR/connector.yaml"
}
[ "$WINDOWED" = true ] && deploy_connector
kubectl -n "$NAMESPACE" rollout status deploy/gdb-growlerdb-gateway --timeout=300s
[ "$WINDOWED" = true ] || deploy_connector

# For STAGE=all, deploy observability here (original order). For STAGE=serving it is already up from
# the deps stage, so skip it.
[ "$STAGE" = all ] && deploy_observability

say "7/7 verify — the gateway serves the $INDEX index"
kubectl -n "$NAMESPACE" run scaleupcheck --rm -i --restart=Never --image=curlimages/curl -- \
  -sS -w '\nHTTP %{http_code}\n' -X POST "http://gdb-growlerdb-gateway:8080/$INDEX/_search" \
  -H 'content-type: application/json' -d '{"query":{"match_all":{}},"size":0}' || true

cat <<EOF

Deployed workload '$WORKLOAD' (table $TABLE → index $INDEX). Port-forward to reach it:
  kubectl -n $NAMESPACE port-forward svc/gdb-growlerdb-gateway 8081:8080   # UI + REST + _search -> http://localhost:8081
  kubectl -n $NAMESPACE port-forward svc/grafana 3000:3000                 # Grafana -> http://localhost:3000
Then drive load: GROWLERDB_OS_URL=http://localhost:8081 python bench/scale/harness.py query $WORKLOAD --duration 120
EOF
