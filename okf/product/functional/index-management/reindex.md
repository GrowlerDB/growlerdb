---
type: Feature
title: Reindex
description: Rebuild an index from its source across all shards, with an atomic generation cutover.
tags: [feature, index, reindex]
timestamp: 2026-07-04T14:22:00
---

# Reindex

Rebuild an index from its Iceberg source — after a definition change, a source recreation, or to move
to a new shard layout. Run it **asynchronously as a job** (`POST /v1/jobs` → a job id, the first-class
path for a long-running multi-shard rebuild) or **synchronously** (`POST /v1/index:reindex`, served by
the gateway which forwards a multi-shard index to the control plane, or a single embedded node). Both
drive the same orchestration.

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

## Async jobs

A coordinated reindex is long-running, so the control plane models it as a durable **job**: `POST /v1/jobs`
returns `202` with a job id immediately, and the driver advances it through
`pending → building → cutting_over → done` (or `failed` / `canceled`), recording each shard's phase and
live `docs_done / docs_total` as it builds.

- **Poll** `GET /v1/jobs/{id}` (or `GET /v1/jobs` for the list) for per-shard progress; the CLI
  `growlerdb reindex --control-plane` streams it to the terminal, `--detach` returns the id, and
  `growlerdb jobs list|get|cancel` manage jobs.
- **Cancel** `DELETE /v1/jobs/{id}` trips a per-node flag the build's populate loop observes; the
  in-flight build aborts, every staged generation is discarded (fences released), and the old generation
  is left intact (no cutover).
- **Crash-safe**: the jobs registry is durable; a job found non-terminal after a control-plane restart is
  failed (its driver died), and since the cutover is a single generation compare-and-swap the index's old
  generation is always intact. One coordinated reindex per index runs at a time.

The synchronous `ReindexIndex` / `AlterIndex` RPCs create a job and await the same driver, so there is
exactly one orchestration implementation behind both doors.

## Behavior

- **Per-node single-flight**: a node rejects a concurrent reindex on its shard (412). No-source → 501;
  wrong-index → 404; a windowed index → Unimplemented (event-time, not buckets — a follow-up).
- The source-streaming read path keeps peak rebuild memory bounded (O(one chunk)).
- A schema-changing [alter](/product/functional/index-management/alter.md) rebuilds from the **new**
  definition and cuts over to it; a plain reindex rebuilds against the served definition.
- Pair with an [alias swap](/product/functional/index-management/aliases-ilm.md) for a cross-index
  blue/green cutover (a differently-named index); the generation epoch serves the same-name path.

## Notes

**Remaining work:** write catch-up (replay the build→cutover delta so writes never pause, removing the
brief final fence) and windowed-index reindex (event-time windows, not buckets) are follow-ups; today the
final drain briefly fences writes and a windowed index is Unimplemented.
