#!/usr/bin/env bash
# Push/restore benchmark corpus artifacts to a Hetzner Object Storage bucket, so a run's corpus is
# reusable and doesn't have to be regenerated. Stores two forms under <bucket>/<run-id>/:
#   iceberg/  — the generated Iceberg warehouse (Parquet + metadata), mirrored from in-cluster MinIO
#   ndjson/   — the portable raw logs as gzipped NDJSON (from corpus_export.py)
#   manifest.json — seed(s), span, rows, generator git SHA, validation report
#
# Uses `mc` (MinIO client) — direct cross-endpoint mirror, no local staging. If `mc` isn't on PATH it
# runs `minio/mc` via Docker. Cross-endpoint aliases are passed as MC_HOST_* env (no on-disk state).
#
# Env (defaults target the created bucket; creds auto-read from CRED_FILE):
#   HETZNER_S3_ENDPOINT   default https://nbg1.your-objectstorage.com
#   HETZNER_S3_BUCKET     default growlerdb
#   CRED_FILE             default ~/.ssh/hetzner-gdb-storage  (ACCESS_KEY=… / SECRET_KEY=…)
#   HETZNER_S3_KEY / HETZNER_S3_SECRET   override the CRED_FILE values if set
#   RUN_ID                run tag, e.g. 2026-08-25-50gb-run1   (REQUIRED)
#   MINIO_ENDPOINT        in-cluster MinIO as seen by mc; default http://host.docker.internal:9000
#                         (port-forward svc/minio 9000 first; use http://minio:9000 when run in-cluster)
#   MINIO_KEY / MINIO_SECRET   default minioadmin / minioadmin
#   ICEBERG_BUCKET        MinIO bucket holding the warehouse, default growlerdb-warehouse
#   NDJSON_DIR            local dir of *.ndjson.gz shards (push), default ./ndjson
#
# Note: with the Docker-mc fallback, NDJSON_DIR must be under a Docker-shared path (on macOS that's
# /Users, not /tmp) so the upload mount sees the files. Verified against the `growlerdb` bucket.
set -euo pipefail

cmd="${1:-}"
CRED_FILE="${CRED_FILE:-$HOME/.ssh/hetzner-gdb-storage}"
HETZNER_S3_ENDPOINT="${HETZNER_S3_ENDPOINT:-https://nbg1.your-objectstorage.com}"
HETZNER_S3_BUCKET="${HETZNER_S3_BUCKET:-growlerdb}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://host.docker.internal:9000}"
MINIO_KEY="${MINIO_KEY:-minioadmin}"; MINIO_SECRET="${MINIO_SECRET:-minioadmin}"
ICEBERG_BUCKET="${ICEBERG_BUCKET:-growlerdb-warehouse}"
NDJSON_DIR="${NDJSON_DIR:-./ndjson}"

# Load Hetzner creds from the file unless already in env.
if [ -z "${HETZNER_S3_KEY:-}" ] && [ -f "$CRED_FILE" ]; then
  HETZNER_S3_KEY="$(grep '^ACCESS_KEY' "$CRED_FILE" | cut -d= -f2- | tr -d ' "')"
  HETZNER_S3_SECRET="$(grep '^SECRET_KEY' "$CRED_FILE" | cut -d= -f2- | tr -d ' "')"
fi
: "${HETZNER_S3_KEY:?set HETZNER_S3_KEY or provide CRED_FILE}"
: "${HETZNER_S3_SECRET:?set HETZNER_S3_SECRET or provide CRED_FILE}"

# mc reads per-alias config from MC_HOST_<alias> = scheme://key:secret@host
export MC_HOST_gdbsrc="http://${MINIO_KEY}:${MINIO_SECRET}@${MINIO_ENDPOINT#http://}"
export MC_HOST_gdbdst="${HETZNER_S3_ENDPOINT%%//*}//${HETZNER_S3_KEY}:${HETZNER_S3_SECRET}@${HETZNER_S3_ENDPOINT#*//}"

# mc wrapper: local binary if present, else the minio/mc Docker image (passing MC_HOST_* through).
# mcc = remote↔remote ops (no local files); mc_put_local = upload a host file (mounts its dir for Docker).
if command -v mc >/dev/null; then
  mcc() { mc "$@"; }
  mc_put_local() { mc cp "$1" "$2"; }
else
  command -v docker >/dev/null || { echo "ERROR: need 'mc' or 'docker' to run mc"; exit 2; }
  mcc() { docker run --rm -e MC_HOST_gdbsrc -e MC_HOST_gdbdst minio/mc:latest "$@"; }
  mc_put_local() {
    local d b; d="$(cd "$(dirname "$1")" && pwd)"; b="$(basename "$1")"
    docker run --rm -e MC_HOST_gdbdst -v "$d":/data:ro minio/mc:latest cp "/data/$b" "$2"
  }
fi

case "$cmd" in
  push)
    : "${RUN_ID:?set RUN_ID}"
    dst="gdbdst/${HETZNER_S3_BUCKET}/${RUN_ID}"
    echo "==> mirror Iceberg warehouse  gdbsrc/${ICEBERG_BUCKET} -> ${dst}/iceberg"
    mcc mirror --overwrite "gdbsrc/${ICEBERG_BUCKET}" "${dst}/iceberg"
    if compgen -G "${NDJSON_DIR}/*.ndjson.gz" >/dev/null; then
      echo "==> upload NDJSON shards      ${NDJSON_DIR}/*.ndjson.gz -> ${dst}/ndjson"
      for f in "${NDJSON_DIR}"/*.ndjson.gz; do mc_put_local "$f" "${dst}/ndjson/"; done
    else
      echo "    (no ${NDJSON_DIR}/*.ndjson.gz — run corpus_export.py first to include raw logs)"
    fi
    [ -f manifest.json ] && mc_put_local manifest.json "${dst}/manifest.json" && echo "==> uploaded manifest.json"
    echo "done: s3://${HETZNER_S3_BUCKET}/${RUN_ID}/"
    ;;
  restore)
    : "${RUN_ID:?set RUN_ID}"
    src="gdbdst/${HETZNER_S3_BUCKET}/${RUN_ID}/iceberg"
    echo "==> restore Iceberg warehouse ${src} -> gdbsrc/${ICEBERG_BUCKET}"
    mcc mirror --overwrite "${src}" "gdbsrc/${ICEBERG_BUCKET}"
    echo "done — source table repopulated in MinIO; skip regeneration."
    ;;
  ls)
    echo "==> s3://${HETZNER_S3_BUCKET}/ contents:"; mcc ls --recursive "gdbdst/${HETZNER_S3_BUCKET}/" ;;
  *)
    echo "usage: artifacts.sh {push|restore|ls}   (see header for env; creds auto-read from CRED_FILE)"; exit 1 ;;
esac
