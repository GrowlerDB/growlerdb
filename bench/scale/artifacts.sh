#!/usr/bin/env bash
# Push/restore benchmark corpus artifacts to a Hetzner Object Storage bucket, so a run's corpus is
# reusable and doesn't have to be regenerated. Stores two forms under <bucket>/<run-id>/:
#   iceberg/  — the generated Iceberg warehouse (Parquet + metadata), mirrored from in-cluster MinIO
#   ndjson/   — the portable raw logs as gzipped NDJSON (from corpus_export.py)
#   manifest.json — seed(s), span, rows, generator git SHA, validation report
#
# Uses `mc` (MinIO client) — S3-compatible on both ends. Bucket-ready: set the HETZNER_* env once the
# bucket + S3 credentials exist. Idempotent mirrors.
#
# Env:
#   HETZNER_S3_ENDPOINT   e.g. https://nbg1.your-objectstorage.com   (REQUIRED)
#   HETZNER_S3_KEY / HETZNER_S3_SECRET                                (REQUIRED)
#   HETZNER_S3_BUCKET     e.g. growlerdb-bench-artifacts              (REQUIRED)
#   RUN_ID                run tag, e.g. 2026-08-25-50gb-run1          (REQUIRED for push/restore)
#   MINIO_ENDPOINT        default http://localhost:9000  (port-forward the in-cluster MinIO)
#   MINIO_KEY / MINIO_SECRET   default minioadmin / minioadmin
#   ICEBERG_BUCKET        MinIO bucket holding the warehouse, default growlerdb-warehouse
#   NDJSON_DIR            local dir of *.ndjson.gz shards (for push), default ./ndjson
set -euo pipefail

cmd="${1:-}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_KEY="${MINIO_KEY:-minioadmin}"; MINIO_SECRET="${MINIO_SECRET:-minioadmin}"
ICEBERG_BUCKET="${ICEBERG_BUCKET:-growlerdb-warehouse}"
NDJSON_DIR="${NDJSON_DIR:-./ndjson}"

require() { for v in "$@"; do [ -n "${!v:-}" ] || { echo "ERROR: \$$v is required (bucket not set up yet?)"; exit 2; }; done; }
have_mc() { command -v mc >/dev/null || { echo "ERROR: 'mc' (MinIO client) not found — install it or run this from a container that has it"; exit 2; }; }

setup_aliases() {
  have_mc
  require HETZNER_S3_ENDPOINT HETZNER_S3_KEY HETZNER_S3_SECRET HETZNER_S3_BUCKET
  mc alias set gdbsrc "$MINIO_ENDPOINT" "$MINIO_KEY" "$MINIO_SECRET" >/dev/null
  mc alias set gdbdst "$HETZNER_S3_ENDPOINT" "$HETZNER_S3_KEY" "$HETZNER_S3_SECRET" >/dev/null
}

case "$cmd" in
  push)
    require RUN_ID; setup_aliases
    dst="gdbdst/${HETZNER_S3_BUCKET}/${RUN_ID}"
    echo "==> mirror Iceberg warehouse  gdbsrc/${ICEBERG_BUCKET} -> ${dst}/iceberg"
    mc mirror --overwrite "gdbsrc/${ICEBERG_BUCKET}" "${dst}/iceberg"
    if compgen -G "${NDJSON_DIR}/*.ndjson.gz" >/dev/null; then
      echo "==> upload NDJSON shards      ${NDJSON_DIR}/*.ndjson.gz -> ${dst}/ndjson"
      mc cp "${NDJSON_DIR}"/*.ndjson.gz "${dst}/ndjson/"
    else
      echo "    (no ${NDJSON_DIR}/*.ndjson.gz — skipping raw-log upload; run corpus_export.py first)"
    fi
    [ -f manifest.json ] && mc cp manifest.json "${dst}/manifest.json" && echo "==> uploaded manifest.json"
    echo "done: s3://${HETZNER_S3_BUCKET}/${RUN_ID}/"
    ;;
  restore)
    require RUN_ID; setup_aliases
    src="gdbdst/${HETZNER_S3_BUCKET}/${RUN_ID}/iceberg"
    echo "==> restore Iceberg warehouse ${src} -> gdbsrc/${ICEBERG_BUCKET}"
    mc mirror --overwrite "${src}" "gdbsrc/${ICEBERG_BUCKET}"
    echo "done — the source table is repopulated in MinIO; skip regeneration."
    ;;
  *)
    echo "usage: artifacts.sh {push|restore}   (see header for required env)"; exit 1 ;;
esac
