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
presets target a local cluster (`values-microk8s.yaml`), cloud (`values-hetzner.yaml`), and the
in-cluster scale test (`values-scale.yaml`, driven by `deploy/k8s/scale-up.sh`). In-cluster
dependencies (MinIO/Postgres/Polaris) are provided via a `deploy/k8s/deps` kustomize with an idempotent
bootstrap.

**Control-plane HA** ([D51](/system/decisions/d51-controlplane-ha.md)). By default the control plane is
a **StatefulSet** (single-writer JSON registry on a PV). Setting `controlPlane.externalRegistry.enabled`
+ a `credentials.registryPostgresDsn` (or the same key in your `existingSecret`) switches it to a
**stateless Deployment** of `controlPlane.replicas` pods over an external Postgres — the image already
ships the `postgres` backend. Coordination is **active-passive**: only the leader (the pod holding the
store's advisory lock) reports `/readyz` 200, so the CP Service is a plain **ClusterIP** whose VIP
routes to that one ready endpoint and shifts to the promoted standby on failover, with no client
re-resolution. Pod anti-affinity (`controlPlane.spreadReplicas`) spreads replicas across hosts.
Leader-only readiness shapes the lifecycle machinery around it: the Deployment uses an explicit
`maxUnavailable: 100%` / `maxSurge: 1` rolling strategy (any smaller `maxUnavailable` wedges the
rollout forever — the controller can never scale down the old leader when only 1 pod is ever
available; expect a brief CP write gap per rollout) with the progress deadline effectively disabled;
a `preStop` SIGINT hook + 10s grace makes a terminating leader release the advisory lock promptly
(the binary is container PID 1 and handles SIGINT only, so Kubernetes' SIGTERM would otherwise be
ignored until SIGKILL); the CP deliberately has **no PodDisruptionBudget** (any budget requiring ≥1
healthy pod pins `disruptionsAllowed` to 0 and wedges drains — eviction is safe, a standby promotes
in ~250ms); and a `checksum/secret` pod annotation rolls the CP pods on DSN/service-token rotation.
Don't wait on `kubectl rollout status` for the HA CP (it never completes at replicas > 1) — NOTES.txt
gives the honest wait (`availableReplicas` = 1). Toggling `externalRegistry.enabled` on a live
release is refused at upgrade time (immutable Service `clusterIP`, and registry data does not
migrate between the embedded `registry.json` and Postgres — values.yaml documents the manual path).
The chart's default embedded path is unchanged. *(The universal node
**placement pool** — a capacity-sized node pool serving many indexes, [D52](/system/decisions/d52-placement-pool.md)
— is a separate slice; the node here is still the per-index StatefulSet.)*

**Index schema.** By default each shard **auto-maps** the Iceberg columns (inferred types). When the
query mix needs a type the source can't carry — an `IP` field for CIDR, `fast` fields for sort/range,
per-field `record` levels — set `index.definition` to a **verbatim** GrowlerDB index definition
(`--set-file index.definition=path/to/index.yaml`): it mounts unchanged as a ConfigMap
and the node builds with `growlerdb index --def` instead of auto-mapping. Verbatim, so there is no
values-level reconstruction of the definition to drift from its source. Empty keeps the auto-map path.

**Scale-test deploys are workload-driven:** `WORKLOAD=<name> deploy/k8s/scale-up.sh`
derives the whole pipeline from one `bench/scale/workloads/<name>/` definition — `harness.py render`
produces the generator (the workload's own `corpus.py` mounted into a generic Deployment, its
`stream()` driven) and the connector (`--table/--identifier/--fields/--index` from `index.yaml`,
`--nodes` sized to the shard count), and the chart gets the same `index.yaml` verbatim. Switching
workloads is configuration, never a manifest edit.

**Maintenance cadences are values-driven:** `node.compactIntervalSecs` (auto-compaction
tick — also the sampling cadence of the per-shard size/docs/segments/delete-debt gauges)
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
