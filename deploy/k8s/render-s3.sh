#!/usr/bin/env bash
# Render an S3-templated manifest against the active object-store target (deploy/k8s/s3-target.env),
# substituting ONLY the `S3_*` placeholders — other `$shell` in the file is left intact. Every apply
# site (scale-up.sh, comparison/up.sh, compare_run.py, observability) pipes its S3 manifests through
# this one helper so the target is resolved from a single place. Usage: render-s3.sh <manifest.yaml>
set -euo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
# shellcheck source=deploy/k8s/s3-target.env
. "$HERE/s3-target.env"   # resolves the profile (minio default | hetzner) and exports S3_* vars
# The single-quoted list is envsubst's SHELL-FORMAT — the literal var names to substitute (NOT a shell
# expansion); restricting to it leaves runtime `${…}` tokens in the manifests ($TOK, ${EXPIRE_TS}, …) intact.
# shellcheck disable=SC2016
exec envsubst \
  '$S3_ENDPOINT $S3_REGION $S3_PATH_STYLE $S3_SSL $S3_WAREHOUSE_BASE $S3_ALLOWED_LOCATION $S3_ACCESS_KEY $S3_SECRET_KEY' \
  < "$1"
