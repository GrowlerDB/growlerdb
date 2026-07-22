---
title: Deployment
layout: default
nav_order: 10
---

# Deployment

GrowlerDB supports two deployment paths.

## Local (Docker Compose)

GrowlerDB runs with its dependencies (MinIO object storage, Apache Polaris catalog) and a bundled
LGTM observability stack, all on one host. The integration tests run against this setup too.

```sh
just stack          # build + start everything; seeds a sample growlerdb.docs table
just stack-down     # tear it down (removes volumes)
```

Endpoints once up: console + REST API at <http://localhost:8081>, gRPC `:50061`, Grafana
<http://localhost:3000>. Full details + the deps-only `just up` flow are in the
[Compose README](https://github.com/GrowlerDB/growlerdb/blob/main/deploy/compose/README.md). See
[Getting started](getting-started) for the guided walkthrough.

## Kubernetes (Helm) {#kubernetes-helm}

The production sharded-cluster topology: a control-plane StatefulSet (registry), node StatefulSets
on local/NVMe PVs (the index store), and a gateway Deployment that fronts the cluster and serves
the console.

```sh
helm install gdb deploy/helm/growlerdb \
  --namespace growlerdb --create-namespace \
  --set image.repository=<your-registry>/growlerdb --set image.tag=<tag> \
  --set iceberg.catalogUri=https://catalog.example/api/catalog \
  --set iceberg.s3Endpoint=https://s3.example \
  --set credentials.existingSecret=growlerdb-creds
helm test gdb -n growlerdb
```

Prerequisites (push the image to a registry your cluster can pull; a reachable catalog + object
store), the full values surface, resilience (PDBs, anti-affinity, readiness gates), and the
single-shard/connector/compactor scope notes are in the
[Helm chart README](https://github.com/GrowlerDB/growlerdb/blob/main/deploy/helm/growlerdb/README.md).

## Configuration

Both paths take the same `GROWLERDB_*` connection environment and the same run-mode flags; see
[Configuration](configuration). For release artifacts (signed images, SBOM, the chart), see
[RELEASING](https://github.com/GrowlerDB/growlerdb/blob/main/RELEASING.md).
