#!/usr/bin/env bash
set -e
# Run from the repo root regardless of where the script lives (bench/ is directly under it).
cd "$(dirname "$0")/.."
NET=growlerdb-dev_default
CENV=(-e GROWLERDB_CATALOG_URI=http://polaris:8181/api/catalog -e GROWLERDB_WAREHOUSE=growlerdb -e GROWLERDB_CATALOG_CREDENTIAL=root:s3cr3t -e GROWLERDB_S3_ENDPOINT=http://minio:9000)
IMG=growlerdb-local:dev

echo "### waiting for image build…"
until grep -qa "Built" /tmp/img.log 2>/dev/null; do sleep 5; done
echo "image ready: $(docker images "$IMG" --format '{{.CreatedSince}}')"

echo "### free memory for the run (stop non-essential stack)"
docker compose -f deploy/compose/docker-compose.yml stop lgtm node gateway controlplane >/dev/null 2>&1 || true

echo "### build telemetry (plain) + telemetry_cached @1M (streaming)"
docker rm -f bench-gdb-serve bench-gdbc-serve >/dev/null 2>&1 || true
docker volume rm bench-gdb >/dev/null 2>&1 || true
docker run --rm --network "$NET" "${CENV[@]}" -v bench-gdb:/data -v "$(pwd)/bench/telemetry.yaml:/telemetry.yaml:ro" "$IMG" --data-dir /data index growlerdb.telemetry --def /telemetry.yaml --name telemetry 2>&1 | grep -aiE "indexed|error" | head -1
docker run --rm --network "$NET" "${CENV[@]}" -v bench-gdb:/data -v "$(pwd)/bench/telemetry_cached.yaml:/c.yaml:ro" "$IMG" --data-dir /data index growlerdb.telemetry --def /c.yaml --name telemetry_cached 2>&1 | grep -aiE "indexed|error" | head -1
echo "index sizes:"; docker run --rm -v bench-gdb:/data alpine:3 sh -c 'du -sh /data/telemetry/0/index /data/telemetry_cached/0/index 2>/dev/null'

echo "### serve both"
docker run -d --name bench-gdb-serve --network "$NET" "${CENV[@]}" -p 8097:8097 -v bench-gdb:/data "$IMG" --data-dir /data serve telemetry --addr 0.0.0.0:50057 --rest-addr 0.0.0.0:8097 >/dev/null
docker run -d --name bench-gdbc-serve --network "$NET" "${CENV[@]}" -p 8098:8098 -v bench-gdb:/data "$IMG" --data-dir /data serve telemetry_cached --addr 0.0.0.0:50058 --rest-addr 0.0.0.0:8098 >/dev/null
for p in 8097 8098; do for _ in $(seq 1 30); do curl -sf "localhost:$p/v1/search" -H 'content-type: application/json' -d '{"query":"id:e1","limit":1}' >/dev/null 2>&1 && break; sleep 1; done; done

echo "### load ES @1M"
docker run --rm --network "$NET" -e POLARIS_URI=http://polaris:8181/api/catalog -e POLARIS_CATALOG=growlerdb -e POLARIS_CREDENTIAL=root:s3cr3t -e AWS_ENDPOINT_URL_S3=http://minio:9000 -e AWS_ACCESS_KEY_ID=minioadmin -e AWS_SECRET_ACCESS_KEY=minioadmin -e ES_URL=http://elasticsearch:9200 bench-load-es:dev 2>&1 | grep -aiE "ready|error" | tail -1

echo "### COUNT bench (filter latency, 1M)"; python3 bench/bench.py 2>&1 | tail -7
echo "### TOP-K DOCUMENTS bench (1M)"; python3 bench/bench_topk.py 2>&1 | tail -8
echo DONE_TOPK
