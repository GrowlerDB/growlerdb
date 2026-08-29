#!/usr/bin/env bash
# Bring up the comparison stack (OpenSearch + Data Prepper Iceberg-CDC) in the growlerdb namespace.
# Deps (MinIO/Polaris), observability, and Trino are already up from deploy/k8s/scale-up.sh; this adds
# the OpenSearch-side systems for the OpenSearch phase of a comparison run. Reads the same Iceberg
# source table (growlerdb.http_logs) the generator populated. NS overridable via NAMESPACE.
set -euo pipefail
NS="${NAMESPACE:-growlerdb}"
HERE="$(cd "$(dirname "$0")" && pwd)"

echo "==> applying OpenSearch + Data Prepper manifests to namespace ${NS}"
kubectl -n "$NS" apply -f "$HERE/opensearch.yaml"
# Data Prepper reads the Iceberg source via S3 — render it against the active object-store target
# (deploy/k8s/render-s3.sh → s3-target.env) so it matches every other component's endpoint/creds.
"$HERE/../render-s3.sh" "$HERE/data-prepper.yaml" | kubectl -n "$NS" apply -f -

echo "==> waiting for OpenSearch statefulset to be ready"
kubectl -n "$NS" rollout status statefulset/opensearch --timeout=600s

echo "==> waiting for the index-setup job"
kubectl -n "$NS" wait --for=condition=complete job/opensearch-index-setup --timeout=300s

echo "==> waiting for Data Prepper deployment"
kubectl -n "$NS" rollout status deploy/data-prepper --timeout=300s

echo "done. OpenSearch: svc/opensearch:9200 ; Data Prepper: svc/data-prepper:4900 (CDC ingesting growlerdb.http_logs)"
