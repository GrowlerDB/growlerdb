#!/usr/bin/env bash
# Scale-harness smoke test (task-159): shake out the workloads + harness before the cloud run.
#
#   1. OFFLINE (always): parse + schema-check EVERY workload (`harness.py validate`). Catches a broken
#      index.yaml / queries.json / name mismatch without a cluster. Fails the smoke on any invalid.
#   2. OPTIONAL (if a gateway is up at GROWLERDB_OS_URL): a tiny query round per workload through the
#      OpenSearch adapter, to confirm the adapter + query bodies work end to end. Best-effort — an
#      un-built index just reports errors, it doesn't fail the smoke.
#
# The FULL pipeline smoke (load corpus → build index → convergence) needs the compose stack — see the
# runbook in bench/scale/README.md. Usage: `just smoke` or `bench/scale/smoke.sh`.
set -euo pipefail
cd "$(dirname "$0")"

OS_URL="${GROWLERDB_OS_URL:-http://localhost:8081}"

# A self-contained python with pyyaml (the harness's only offline dep), so the smoke runs anywhere.
VENV="${SMOKE_VENV:-/tmp/gdb-scale-venv}"
if [ ! -x "$VENV/bin/python" ]; then
  python3 -m venv "$VENV"
fi
"$VENV/bin/pip" install --quiet --disable-pip-version-check pyyaml >/dev/null 2>&1 || true
PY="$VENV/bin/python"

workloads=()
for d in workloads/*/; do
  [ -f "$d/workload.yaml" ] && workloads+=("$(basename "$d")")
done
[ "${#workloads[@]}" -gt 0 ] || { echo "no workloads found under workloads/"; exit 1; }

echo "== 1) offline validate ${#workloads[@]} workload(s) =="
fail=0
for wl in "${workloads[@]}"; do
  if "$PY" harness.py validate "$wl"; then :; else fail=1; fi
done
[ "$fail" -eq 0 ] || { echo "SMOKE FAILED: a workload is invalid"; exit 1; }

echo
echo "== 2) render the streaming k8s manifests (task-214) =="
# Every workload whose corpus has a stream() must render valid generator/connector manifests —
# the render itself yaml-parses its output, so this catches template/derivation breakage offline.
rendered=0
for wl in "${workloads[@]}"; do
  if "$PY" -c "
import sys; sys.path.insert(0, 'workloads/$wl')
import importlib.util
spec = importlib.util.spec_from_file_location('c', 'workloads/$wl/corpus.py')
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
sys.exit(0 if hasattr(m, 'stream') else 1)" 2>/dev/null; then
    "$PY" harness.py render "$wl" --shards 2 --out "/tmp/smoke-render-$wl" || { echo "SMOKE FAILED: render $wl"; exit 1; }
    rendered=$((rendered + 1))
  else
    echo "$wl: no corpus stream() — skipping render (bulk-load-only workload)"
  fi
done
[ "$rendered" -gt 0 ] || { echo "SMOKE FAILED: no streaming workload rendered"; exit 1; }

echo
echo "== 3) gateway query round (optional) =="
if curl -fsS -o /dev/null --max-time 3 "$OS_URL" 2>/dev/null; then
  echo "gateway reachable at $OS_URL — running a 3s query round per workload (best-effort)"
  for wl in "${workloads[@]}"; do
    echo "-- $wl --"
    GROWLERDB_OS_URL="$OS_URL" "$PY" harness.py query "$wl" \
      --duration 3 --concurrency 2 --out "/tmp/smoke-$wl.json" 2>&1 | tail -8 || true
  done
else
  echo "no gateway at $OS_URL — skipping the query round."
  echo "For the full pipeline smoke (load → index → convergence): bring up 'just stack' and follow"
  echo "the runbook in bench/scale/README.md."
fi

echo
echo "SMOKE OK: all workloads valid."
