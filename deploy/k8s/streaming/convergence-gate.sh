#!/usr/bin/env bash
# Exact-count-at-drain convergence gate for the streaming stack. Lag-based checks have two holes:
# (1) lag reaching ~0 doesn't prove convergence (an under-read window advances the cursor with the
# rows never applied), and (2) comparing to the raw row count is wrong when the generator re-emits
# duplicate ids (they collapse last-write-wins in the index, so index < source rows even with no
# loss). This gate closes both: it drains, then asserts the index doc count equals the source's
# DISTINCT-id count exactly. Optionally it runs Iceberg maintenance (compaction) CONCURRENTLY first,
# to exercise the changelog-read-vs-compaction race.
#
# Usage:
#   convergence-gate.sh [--with-maintenance] [--namespace growlerdb] [--index http_logs]
#                       [--table growlerdb.http_logs] [--id-col id] [--drain-timeout 900]
#
# Requires: kubectl (context pointing at the cluster), jq. Run after the streaming stack is up and has
# ingested for a while. Exits non-zero (CONVERGENCE FAILED) on a mismatch, so it doubles as a CI/soak
# regression gate.
set -euo pipefail

NS=growlerdb
INDEX=http_logs
TABLE=growlerdb.http_logs
ID_COL=id
DRAIN_TIMEOUT=900
WITH_MAINTENANCE=0

while [ $# -gt 0 ]; do
  case "$1" in
    --with-maintenance) WITH_MAINTENANCE=1 ;;
    --namespace) NS="$2"; shift ;;
    --index) INDEX="$2"; shift ;;
    --table) TABLE="$2"; shift ;;
    --id-col) ID_COL="$2"; shift ;;
    --drain-timeout) DRAIN_TIMEOUT="$2"; shift ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
  shift
done

fail() { echo "CONVERGENCE FAILED: $*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1"; }
need kubectl
need jq

kc() { kubectl -n "$NS" "$@"; }

# The index doc count = the gateway's match-all `total` (what search actually serves).
index_total() {
  kc exec deploy/growlerdb-connector -- sh -c \
    "curl -s -X POST http://gdb-growlerdb-gateway:8080/v1/search \
       -H 'Content-Type: application/json' \
       -d '{\"query\":\"*\",\"limit\":1}'" | jq -r '.total'
}

echo "convergence-gate: index=$INDEX table=$TABLE (namespace $NS)"

# 1) Optionally kick a maintenance (compaction) run so the drain races an Iceberg rewrite — the
#    changelog-read-vs-compaction window this whole guard exists for.
if [ "$WITH_MAINTENANCE" = 1 ]; then
  echo "convergence-gate: launching concurrent Iceberg maintenance"
  kc delete job convergence-maint --ignore-not-found >/dev/null 2>&1 || true
  kc create job --from=cronjob/growlerdb-iceberg-maintenance convergence-maint
fi

# 2) Stop the source so the connector can reach a fixed point.
echo "convergence-gate: stopping the generator"
kc scale deploy growlerdb-generator --replicas=0

# 3) Drain: wait until the index total is stable across several polls AND ingest lag is ~0. A stable
#    total alone isn't enough (a stalled connector is also "stable"), so require lag to have drained.
echo "convergence-gate: draining (timeout ${DRAIN_TIMEOUT}s)"
deadline=$(( SECONDS + DRAIN_TIMEOUT ))
stable=0
last=-1
until [ "$stable" -ge 6 ]; do
  (( SECONDS < deadline )) || fail "did not drain within ${DRAIN_TIMEOUT}s (last total=$last)"
  now="$(index_total || echo -1)"
  if [ "$now" = "$last" ] && [ "$now" != "-1" ]; then
    stable=$(( stable + 1 ))
  else
    stable=0
  fi
  last="$now"
  sleep 10
done
echo "convergence-gate: drained at index total=$last"

# 4) Source distinct-id count — the authoritative target. A one-shot spark-sql COUNT(DISTINCT) against
#    the same Polaris catalog the maintenance job uses (reuses the connector image's spark).
echo "convergence-gate: counting DISTINCT $ID_COL in $TABLE"
src_out="$(kc run convergence-count-$$ --rm -i --restart=Never \
  --image=ghcr.io/growlerdb/growlerdb-connector:dev --image-pull-policy=Always -- \
  /opt/spark/bin/spark-sql --master 'local[2]' \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
    --conf spark.sql.catalog.stream=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.stream.type=rest \
    --conf spark.sql.catalog.stream.uri=http://polaris:8181/api/catalog \
    --conf spark.sql.catalog.stream.warehouse=growlerdb \
    --conf spark.sql.catalog.stream.credential=root:s3cr3t \
    --conf spark.sql.catalog.stream.scope=PRINCIPAL_ROLE:ALL \
    --conf spark.sql.catalog.stream.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.sql.catalog.stream.s3.endpoint=http://minio:9000 \
    --conf spark.sql.catalog.stream.s3.path-style-access=true \
    -e "SELECT COUNT(DISTINCT ${ID_COL}) FROM stream.${TABLE};" 2>/dev/null)" \
  || fail "source distinct-id count query failed"

# spark-sql prints the scalar result on its own line; take the last all-digits line.
source_distinct="$(printf '%s\n' "$src_out" | grep -Eo '^[0-9]+$' | tail -1)"
[ -n "$source_distinct" ] || fail "could not parse a distinct-id count from spark-sql output"

# 5) The invariant: every distinct source id is searchable exactly once — no silent loss, no dup.
echo "convergence-gate: index total=$last  source distinct ids=$source_distinct"
if [ "$last" != "$source_distinct" ]; then
  fail "index ($last) != source distinct ids ($source_distinct) — $(( source_distinct - last )) row(s) diverged"
fi
echo "CONVERGENCE OK: index == source distinct ids ($last)"
