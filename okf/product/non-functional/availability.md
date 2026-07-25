---
type: Requirement
title: Availability
description: Search availability target and how it is met.
tags: [nfr, availability, ha]
timestamp: 2026-07-04T14:22:00
---

# Availability

- **Search availability:** 99.9% target. Searchers hold derived, rebuildable state, so availability is
  met with shards spread across nodes, [replicas](/product/functional/replicas.md), PodDisruptionBudgets,
  and PV self-heal — with **honest partial results** surfaced during a shard's restart rather than a
  hard failure.

**Status.** v1 **design target**; the self-healing behavior is exercised under
[reliability/chaos](/quality/reliability.md). Today the [control plane](/system/runtime/components/control-plane.md)
is still a single instance and nodes have no live replica (reads degrade to honest partial during a
restart). **True HA** — no SPOF, zero-gap node failover — is designed in
[high availability](/system/high-availability.md) (D51–D53, the `true-ha` epic): a replicated control
plane plus a placement pool of interchangeable, multi-index, replicated nodes; not yet built.
