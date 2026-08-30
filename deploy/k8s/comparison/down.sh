#!/usr/bin/env bash
# Tear down the comparison stack (OpenSearch + Data Prepper), leaving deps/observability/Trino and the
# Iceberg source intact. Used between run phases and at the end. NS overridable via NAMESPACE.
set -euo pipefail
NS="${NAMESPACE:-growlerdb}"
HERE="$(cd "$(dirname "$0")" && pwd)"
echo "==> deleting OpenSearch + Data Prepper from namespace ${NS}"
kubectl -n "$NS" delete -f "$HERE/data-prepper.yaml" --ignore-not-found
kubectl -n "$NS" delete -f "$HERE/opensearch.yaml" --ignore-not-found
# StatefulSet PVCs are retained by default; drop them so a rerun starts clean.
kubectl -n "$NS" delete pvc -l app=opensearch --ignore-not-found
echo "done."
