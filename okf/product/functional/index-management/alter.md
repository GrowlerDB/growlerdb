---
type: Feature
title: Alter index
description: Change an index definition in place where safe; guide changes that require a reindex.
tags: [feature, index, alter, schema-evolution]
timestamp: 2026-07-04T14:22:00
---

# Alter index

Evolve an existing index. `POST /v1/index:alter` **dry-runs a plan** or **applies** a definition
change. A multi-shard index is applied by the [control plane](/system/runtime/components/control-plane.md)
(the gateway forwards it); a single embedded node applies its own.

- **In-place** — metadata-only changes (an index rename, the `sensitive` flag, a `max_bytes`
  redeclaration). Nothing stored or indexed differs.
- **Reindex-requiring** — adding/removing a mapped field, or a type/analyzer/`fast`/`cached`/`indexed`
  change: a segment's schema is fixed at build time, so these can't apply to existing segments.

**Apply is durable and drives the reindex.** On apply, the control plane updates the **registry
definition** (a compare-and-swap on its version), so the change survives restart — the registry is the
source of truth, not a node's local copy. A reindex-requiring apply then runs a coordinated
[reindex](/product/functional/index-management/reindex.md) **from the new definition** across every
shard, cutting over atomically to the new-schema generation; the response reports `applied`,
`reindex_triggered`, and the new `generation`.

## Notes

Schema-evolution rules live in the [data model](/system/storage/data-model.md). A node that boots
after a durable alter must reload the new definition from the registry (`definition_version`) rather
than rebuild from a stale local definition — a follow-up for full cross-restart durability.
