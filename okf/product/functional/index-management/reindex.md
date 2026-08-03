---
type: Feature
title: Reindex
description: Rebuild an index from its source across all shards, with an atomic generation cutover.
tags: [feature, index, reindex]
timestamp: 2026-07-04T14:22:00
---

# Reindex

Rebuild an index from its Iceberg source — after a definition change, a source recreation, or to move
to a new shard layout. `POST /v1/index:reindex` (CLI `growlerdb reindex --control-plane`), served by
the gateway (which forwards a multi-shard index to the control plane) or a single embedded node.

## Coordinated multi-shard reindex

A multi-shard reindex is orchestrated by the [control plane](/system/runtime/components/control-plane.md)
as **build-all → cut-over-all**, so a build failure never half-swaps the index:

1. **Build** every shard's *next generation* from source into a staging shard — durable but **not**
   promoted — while the live generation keeps serving. The build is filtered to each shard's current
   bucket owners (an identity rebuild; no topology change).
2. If **any** shard's build fails, **discard** every staged generation (releasing its write-fence) and
   abort — the old generation is intact everywhere. No cutover happens.
3. Once all builds succeed, **promote** every shard (a brief per-shard write-fence drain + atomic swap),
   then **bump the routing generation** ([`set_generation`](/system/distribution.md), a compare-and-swap
   epoch) — the atomic cutover marker. Gateways converge to the new generation on their next
   `GetIndex` poll.

Each node's phase is BUILD / PROMOTE / DISCARD; the write-fence is held across BUILD..PROMOTE so writes
can't advance a shard past its build snapshot. Reads stay up throughout; writes pause only for the brief
final drain, not the whole rebuild.

## Behavior

- **Per-node single-flight**: a node rejects a concurrent reindex on its shard (412). No-source → 501;
  wrong-index → 404; a windowed index → Unimplemented (event-time, not buckets — a follow-up).
- The source-streaming read path keeps peak rebuild memory bounded (O(one chunk)).
- A schema-changing [alter](/product/functional/index-management/alter.md) rebuilds from the **new**
  definition and cuts over to it; a plain reindex rebuilds against the served definition.
- Pair with an [alias swap](/product/functional/index-management/aliases-ilm.md) for a cross-index
  blue/green cutover (a differently-named index); the generation epoch serves the same-name path.

## Notes

**Remaining work:** an async job model (start → id, poll progress, cancel) and write catch-up (replay
the build→cutover delta so writes never pause, removing the brief final fence) are follow-ups; today
the trigger is synchronous and the final drain briefly fences writes.
