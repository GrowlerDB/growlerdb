---
type: Concept
title: Sharded HA topology
description: The multi-shard, multi-node distributed topology and its availability posture.
tags: [deployment, ha, sharded, distributed]
timestamp: 2026-07-04T14:22:00
---

# Sharded HA topology

The genuinely distributed topology: a [gateway](/system/runtime/components/gateway.md) fronting the
live [control plane](/system/runtime/components/control-plane.md), and a node StatefulSet with **one
pod per shard**, spread by anti-affinity across nodes.

## Availability posture

Because searchers hold derived, rebuildable state, HA is met by **shards spread + PodDisruptionBudgets
+ PV self-heal**, with **honest partial results** during a shard's restart rather than a hard failure.
Online [resharding](/system/distribution.md) and gateway hot-reload make topology changes non-disruptive.
Ingest scales independently of the node topology via the connector-set
([D32](/system/decisions/d32-parallel-ingest.md)): W worker pods each own a disjoint shard group,
and a worker crash stalls only its own group's shards until the pod restarts and resumes.

## Security posture

Set `credentials.serviceToken` (→ `GROWLERDB_SERVICE_TOKEN` on every pod) in any multi-pod
deployment: it closes the control plane's internal RPCs **and every node's data-plane gRPC** to
callers without the token — see the [node's trust boundary](/system/runtime/components/node.md).
Node/CP ports must additionally never be exposed beyond the cluster network (only the gateway
terminates user traffic, with authn/RBAC/tenant enforcement); the token is defense-in-depth
behind network policy, not a substitute for it.

## Notes

Zero-downtime per-shard **replicas** (windowed/multi-shard segment shipping) are future work — see
[replicas](/product/functional/replicas.md) and
[known limitations](/quality/known-limitations/index.md). Availability under fault is validated by
[chaos drills](/quality/reliability.md).
