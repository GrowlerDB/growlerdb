---
type: Component
title: Compactor / maintenance
description: Background maintenance — segment compaction and Iceberg orphan-file reclamation.
tags: [component, maintenance, compaction]
timestamp: 2026-07-04T14:22:00
---

# Compactor / maintenance

Background maintenance that keeps the system healthy over time, in two forms:

- **In-process auto-compaction** — a loop inside serving [nodes](/system/runtime/components/node.md)
  that [compacts](/product/functional/index-management/compact.md) segments when a health policy trips
  (per shard; per hot window). Replicas and cold read-through windows do not compact.
- **Maintenance CronJob** — a scheduled Kubernetes job that reclaims Iceberg **orphan files** (and
  related housekeeping) so storage doesn't accumulate unreferenced data.

## Notes

Deployed via the Helm/k8s [manifests](/system/deployment/index.md). Size-band tiering + backup GC are
further maintenance work.
