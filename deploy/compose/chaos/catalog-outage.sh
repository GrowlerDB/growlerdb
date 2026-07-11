#!/usr/bin/env bash
# Chaos drill: catalog (Polaris) outage on the Compose stack.
#
# Kills Polaris and asserts:
#   1. SEARCH stays available during the outage — it serves the local index and never touches the
#      catalog, so a catalog failure must not take reads down.
#   2. Polaris SELF-RESTARTS (the `restart: unless-stopped` policy).
#   3. HYDRATION recovers AUTOMATICALLY once the catalog returns — `keys:get` reads the authoritative
#      Iceberg row through Polaris, so a hydrated row is end-to-end proof the catalog is back AND
#      (via the persistent Postgres metastore) survived the bounce with its tables intact,
#      i.e. no stale/orphaned index.
# Hydration is honestly degraded *during* the outage (not asserted here — Polaris restarts in seconds,
# so a "hydration fails now" check is inherently racy; the durable claims are #1–#3).
#
# Prerequisite: the full stack is up (`just stack`) and `127.0.0.1 minio` is in /etc/hosts.
#
# Usage:  deploy/compose/chaos/catalog-outage.sh
set -euo pipefail
. "$(dirname "$0")/lib.sh"
require jq

RECOVERY_TIMEOUT=120   # catalog cold JVM start is slower than a node restart

search()  { curl -fsS -X POST "$GATEWAY/search"   -H 'content-type: application/json' -d '{"query":"*:*","limit":1}'; }
hydrate() { curl -fsS -X POST "$GATEWAY/keys:get" -H 'content-type: application/json' \
              -d "$(jq -cn --argjson c "$1" '{keys:[$c],columns:[]}')"; }
row_count() { jq '.rows | length' 2>/dev/null; }

cid="$(cid_of polaris)"
[ -n "$cid" ] || fail "polaris is not running — bring the stack up first (\`just stack\`)"

echo "==> baseline: search a hit, then hydrate its authoritative row"
coords="$(search | jq -c '.hits[0].coordinates')" || fail "baseline search failed"
[ "$coords" != "null" ] || fail "baseline search returned no hits — is docs seeded?"
[ "$(hydrate "$coords" | row_count)" -ge 1 ] || fail "baseline hydration returned no rows"
started_at="$(started_at_of "$cid")"

echo "==> injecting fault: docker kill polaris ($cid)"
docker kill "$cid" >/dev/null

echo "==> asserting SEARCH stays available during the catalog outage"
search >/dev/null || fail "search must stay up while the catalog is down (it serves the local index)"

echo "==> asserting polaris self-restarts within ${RECOVERY_TIMEOUT}s"
wait_restart "$cid" "$started_at" "$RECOVERY_TIMEOUT" || fail "polaris did not self-restart"

echo "==> asserting HYDRATION recovers automatically once the catalog returns"
deadline=$(( SECONDS + RECOVERY_TIMEOUT ))
until n="$(hydrate "$coords" 2>/dev/null | row_count)" && [ "${n:-0}" -ge 1 ]; do
  (( SECONDS < deadline )) || fail "hydration did not recover after the catalog returned"
  sleep 3
done

echo "DRILL PASSED: catalog outage → search stayed up, polaris self-restarted, hydration recovered"
