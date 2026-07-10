#!/usr/bin/env bash
# Bootstrap the local Polaris catalog for GrowlerDB: wait for Polaris, create the
# `growlerdb` catalog (S3/MinIO storage), and grant admin to the root principal.
# Idempotent — safe to re-run (catalog create returns 409 if it already exists).
set -euo pipefail

POLARIS="${POLARIS:-http://localhost:8181}"

echo "waiting for Polaris OAuth endpoint..."
TOK=""
for _ in $(seq 1 60); do
  # Parse the token with sed (no python3/jq dep — this runs on any minimal host). `curl -s` already
  # swallows the expected connection-refused noise while Polaris boots; we don't mask parse errors.
  TOK=$(curl -s -X POST "$POLARIS/api/catalog/v1/oauth/tokens" \
    -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
    -d scope=PRINCIPAL_ROLE:ALL \
    | sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
  [ -n "$TOK" ] && break
  sleep 1
done
[ -n "$TOK" ] || { echo "ERROR: Polaris did not become ready" >&2; exit 1; }

# Catalog storage endpoint is the in-network name (minio:9000). Host clients must
# resolve `minio` to 127.0.0.1 (one /etc/hosts line) to read data files.
code=$(curl -s -o /dev/null -w "%{http_code}" -X POST "$POLARIS/api/management/v1/catalogs" \
  -H "Authorization: Bearer $TOK" -H "Content-Type: application/json" \
  -d '{"catalog":{"name":"growlerdb","type":"INTERNAL","properties":{"default-base-location":"s3://growlerdb-warehouse/growlerdb"},"storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://growlerdb-warehouse/"],"roleArn":"arn:aws:iam::000000000000:role/polaris","endpoint":"http://minio:9000","pathStyleAccess":true}}}')
echo "create catalog 'growlerdb': http=$code  (201=created, 409=exists)"

# Grant catalog admin to the root principal (via the service_admin principal-role). Creating an
# INTERNAL catalog already auto-assigns `catalog_admin` to service_admin, so on a *persistent*
# metastore (Postgres, task-114 guardrail) this PUT hits a duplicate grant record and returns 500
# — harmless. Accept 2xx as granted; otherwise verify the assignment already exists before failing.
gcode=$(curl -s -o /dev/null -w "%{http_code}" -X PUT \
  "$POLARIS/api/management/v1/principal-roles/service_admin/catalog-roles/growlerdb" \
  -H "Authorization: Bearer $TOK" -H "Content-Type: application/json" \
  -d '{"catalogRole":{"name":"catalog_admin"}}')
if [ "$gcode" -ge 200 ] && [ "$gcode" -lt 300 ]; then
  echo "grant catalog_admin -> root: http=$gcode (granted)"
elif curl -s "$POLARIS/api/management/v1/principal-roles/service_admin/catalog-roles/growlerdb" \
       -H "Authorization: Bearer $TOK" | grep -q '"catalog_admin"'; then
  echo "grant catalog_admin -> root: http=$gcode (already granted — ok)"
else
  echo "ERROR: failed to grant catalog_admin to root (http=$gcode)" >&2
  exit 1
fi

echo "polaris ready"
