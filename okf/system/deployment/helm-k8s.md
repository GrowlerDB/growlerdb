---
type: Concept
title: Helm / Kubernetes
description: The production deployment path — a Helm chart plus kustomize for in-cluster dependencies.
tags: [deployment, helm, kubernetes]
resource: /deploy/helm
timestamp: 2026-07-04T14:22:00
---

# Helm / Kubernetes

The production path (`deploy/helm/growlerdb`): a Helm chart deploying
[control-plane](/system/runtime/components/control-plane.md) + a
[node](/system/runtime/components/node.md) **StatefulSet** + a
[gateway](/system/runtime/components/gateway.md) Deployment, with Services/Ingress, liveness/readiness
probes, PodDisruptionBudgets, and anti-affinity. The gateway fronts the **live control plane** over
gRPC and hot-reloads routing.

## Topology

The node StatefulSet runs **one pod per shard** (ordinal = pod index; `replicas = shards`). Values
presets target the home-lab (`values-microk8s.yaml`), cloud (`values-hetzner.yaml`), and the
in-cluster scale test (`values-scale.yaml`, driven by `deploy/k8s/scale-up.sh`). In-cluster
dependencies (MinIO/Postgres/Polaris) are provided via a `deploy/k8s/deps` kustomize with an idempotent
bootstrap.

**Index schema.** By default each shard **auto-maps** the Iceberg columns (inferred types). When the
query mix needs a type the source can't carry — an `IP` field for CIDR, `fast` fields for sort/range,
per-field `record` levels — set `index.definition` to a **verbatim** GrowlerDB index definition
(`--set-file index.definition=path/to/index.yaml`, task-209/214): it mounts unchanged as a ConfigMap
and the node builds with `growlerdb index --def` instead of auto-mapping. Verbatim, so there is no
values-level reconstruction of the definition to drift from its source. Empty keeps the auto-map path.

**Scale-test deploys are workload-driven** (task-214): `WORKLOAD=<name> deploy/k8s/scale-up.sh`
derives the whole pipeline from one `bench/scale/workloads/<name>/` definition — `harness.py render`
produces the generator (the workload's own `corpus.py` mounted into a generic Deployment, its
`stream()` driven) and the connector (`--table/--identifier/--fields/--index` from `index.yaml`,
`--nodes` sized to the shard count), and the chart gets the same `index.yaml` verbatim. Switching
workloads is configuration, never a manifest edit.

**Maintenance cadences are values-driven** (task-214): `node.compactIntervalSecs` (auto-compaction
tick — also the sampling cadence of the per-shard size/docs/segments/delete-debt gauges, task-218)
and `node.remapIntervalSecs` (D30 locator re-map poll) pass through to `growlerdb serve` instead of
riding hidden binary defaults; `0` disables either loop.

The **connector** deploys outside the chart (`deploy/k8s/streaming/`): either the single-process
Deployment (rendered from `connector.template.yaml`, `replicas: 1`) or, for ingest scale-out, the
**connector-set StatefulSet** (`connector-set.yaml`, [D32](/system/decisions/d32-parallel-ingest.md))
— `W` worker pods, worker id = pod ordinal, `W ≤ shards`, never both on one table at once (the
streaming README carries the runbook).

## Notes

See [sharded HA](/system/deployment/sharded-ha.md) for the availability posture. Deploy-specific
console config (Grafana URL) is served at runtime, not baked in.
