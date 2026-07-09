---
type: Process
title: Reliability & resilience
description: How self-healing and recovery guarantees (RTO/RPO) are validated under fault injection.
tags: [quality, reliability, resilience, chaos]
timestamp: 2026-07-04T14:22:00
---

# Reliability & resilience

How GrowlerDB proves it is stable and self-healing — not by assertion but by **injecting faults and
asserting recovery**.

## Method

- **Self-heal by construction** — crashed/OOM-killed processes restart automatically (Compose
  `restart:` policies; k8s pod restartPolicy + liveness probes + PDBs), and the durable commit ordering
  means a crash never corrupts the index.
- **Chaos drills** — repeatable fault-injection: process crash (self-restart + `/readyz` recovery +
  search still answers), catalog outage (search stays up, ingestion resumes), source-recreated,
  streaming under node churn. Compose drills run against a live stack; a Kubernetes chaos harness runs
  on the cluster.
- **Recovery objectives** — RTO/RPO encoded as assertions so a regression in self-healing fails the
  suite.

## Notes

Backing guarantees: [durability](/product/non-functional/durability.md) (RPO = 0 for acked writes) and
[exactly-once](/product/functional/ingestion/checkpoints-exactly-once.md). A resilience posture doc
maps each failure mode → detection → automatic recovery → operator action.
