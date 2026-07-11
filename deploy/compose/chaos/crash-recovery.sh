#!/usr/bin/env bash
# Chaos drill: process-crash / self-heal recovery on the Compose stack.
#
# Kills a core GrowlerDB service mid-flight and asserts it self-restarts (the `restart:` policy in
# docker-compose.yml — the Compose analogue of the k8s pods' always-restart + liveness probes) and
# returns to a serving state, with search still answering afterwards. This is the Compose-based
# variant of the resilience harness; the Kubernetes variant (pod/node faults via the Helm chart)
# is separate. Recovery objective: crash → auto-restart → /readyz green → search works, within RTO.
#
# Prerequisite: the full stack is up (`just stack`) and its host-side deps are mapped
# (`127.0.0.1 minio` in /etc/hosts — see deploy/compose/README.md).
#
# Usage:  deploy/compose/chaos/crash-recovery.sh [service]   # service defaults to `node`
set -euo pipefail
. "$(dirname "$0")/lib.sh"

SERVICE="${1:-node}"
# Where each service reports readiness (host-published metrics port → /readyz).
case "$SERVICE" in
  node)         READY_URL="http://localhost:9102/readyz" ;;
  gateway)      READY_URL="http://localhost:9103/readyz" ;;
  controlplane) READY_URL="http://localhost:9101/readyz" ;;
  *) fail "unknown service '$SERVICE' (expected node|gateway|controlplane)" ;;
esac
RECOVERY_TIMEOUT=90   # seconds; RTO bound for a single-container crash on the dev stack

cid="$(cid_of "$SERVICE")"
[ -n "$cid" ] || fail "service '$SERVICE' is not running — bring the stack up first (\`just stack\`)"

echo "==> baseline: '$SERVICE' ready?"
wait_http "$READY_URL" 30 || fail "'$SERVICE' not ready at baseline ($READY_URL)"
started_at="$(started_at_of "$cid")"

echo "==> injecting fault: docker kill (SIGKILL) $SERVICE ($cid)"
docker kill "$cid" >/dev/null

echo "==> asserting self-restart within ${RECOVERY_TIMEOUT}s"
wait_restart "$cid" "$started_at" "$RECOVERY_TIMEOUT" || fail "'$SERVICE' did not self-restart within ${RECOVERY_TIMEOUT}s"

echo "==> asserting readiness recovers ($READY_URL)"
wait_http "$READY_URL" "$RECOVERY_TIMEOUT" || fail "'$SERVICE' restarted but /readyz never recovered"

echo "==> asserting search still answers through the gateway"
curl -fsS -X POST "$GATEWAY/search" -H 'content-type: application/json' \
  -d '{"query":"*:*","limit":1}' >/dev/null || fail "search did not answer after '$SERVICE' recovery"

echo "DRILL PASSED: '$SERVICE' crash → self-restart → ready → search OK"
