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
the gateway). The gateway forwards to the control plane's coordinated reindex whenever a control plane is
present — for pool-hosted and multi-shard indexes alike, since a single owning **pool** node rejects a
direct per-index reindex and must be unit-coordinated — and only reindexes a lone shard in-process on the
control-plane-less embedded `serve`. Both paths drive the same orchestration.

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

Each node's phase is BUILD / PROMOTE / DISCARD. Reads stay up throughout, and — see **write catch-up**
below — writes are **not** paused for the rebuild; the write-fence is engaged only for the brief cutover
swap.

## Windowed reindex

A **windowed** index shards by ingest-time window rather than ordinal, so it is reindexed **one window
at a time**: the driver enumerates the registry's `window_map` and drives BUILD → PROMOTE per window,
each rebuilt from source **filtered to that window's ingest-time window** (`window_of(doc[field])`, the
windowed analog of the reshard bucket filter). There is no routing-generation epoch for a windowed
index — each window promotes as a node-local shard swap and the gateway converges by placement
fingerprint — so the cutover skips the generation compare-and-swap. Windowed cutover uses the
connector-replay model (the staged window is stamped at the build snapshot; the connector resumes from
the server-min committed checkpoint and replays per window), so it needs no per-window write-fence.

**Cold/parked windows are skipped** (a read-through window has no local writer; the planner reports how
many it skipped). Reindexing a parked window — revive → build → promote → re-park — is a follow-up. So
after a schema-changing alter on a windowed index, already-parked windows stay at the old schema until
revived.

## Write catch-up (zero write-downtime)

The BUILD runs **unfenced**: writes keep flowing to the live generation while the (long) rebuild runs, so
there is no whole-op write pause. A per-node `staged` single-flight flag (not the write-fence) guards the
staging directory across BUILD..PROMOTE. The write-fence is engaged only for the **brief cutover** in
PROMOTE, so an in-flight write can't land on the generation being swapped aside.

Because the live generation advances during the unfenced build, the cutover must not lose or skip those
writes. Two paths, both **exactly-once** and **without a whole-op write pause**:

- **Changelog (delete-aware) indexes:** the staged generation is promoted at the **build snapshot**, so
  on cutover the live checkpoint rewinds to it. A batch connector (or the next scheduled run) resumes
  from that checkpoint and **replays the build-window delta through its normal delete-aware changelog**
  (`from ≤ current` ⇒ `Apply`; upserts idempotent, deletes applied). A *continuously streaming* connector
  holds an in-memory cursor ahead of the rewound checkpoint, so its next micro-batch trips one
  `CheckpointGap` — which is the **intended** signal, not a fault: it fails that micro-batch, and the
  connector's [restart loop](/system/runtime/components/connector.md) re-reads its resume point from the
  node's committed checkpoint (now the build snapshot) and replays from there. The `CheckpointGap` guard
  is deliberately **not** relaxed to tolerate the rewind: a node that genuinely missed a write looks
  identical locally, so silently accepting `from > current` would seal that loss. Net effect is a brief
  self-healing stream restart plus the delete-aware replay — not a crash, no manual offset surgery.
- **Append-only (`AppendFastPath`) indexes:** before the swap, the node **pre-applies the source rows
  appended since the build** onto the staged generation and stamps it at the caught-up **head**
  ([`read_documents_appended_since_ordered`](/system/runtime/components/node.md) under the cutover fence),
  so the promoted generation doesn't regress the live checkpoint and the connector has ~nothing to replay
  (seamless cutover, no `CheckpointGap`).

Stamping the staged generation *ahead* of the data it actually contains would silently skip rows, so the
append path only stamps at head after it has applied the delta; the changelog path stamps honestly at the
build snapshot and lets the connector's replay do the delete-aware catch-up.

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

- **Per-node single-flight**: a node rejects a concurrent reindex on its shard/window (412). No-source
  → 501; wrong-index → 404.
- **Up-front disk precheck**: before creating the job the control plane asks every unit's node — all of
  them **concurrently** — whether it has room, and refuses the whole reindex with one clear error naming
  the short nodes, rather than letting a rebuild fail hours in on a single shard. The bar is **≈2× the
  current shard size in free space**: the cutover swap `rename`s the old shard aside and the staging
  shard into place (both O(1), never a third physical copy), so the disk peak is ≈2× total — 1× for the
  staging copy plus headroom for a Tantivy segment merge's transient scratch. (The node's own build
  re-checks as a backstop.)
- The source-streaming read path keeps peak rebuild memory bounded (O(one chunk)).
- A schema-changing [alter](/product/functional/index-management/alter.md) rebuilds from the **new**
  definition and cuts over to it; a plain reindex rebuilds against the served definition.
- Pair with an [alias swap](/product/functional/index-management/aliases-ilm.md) for a cross-index
  blue/green cutover (a differently-named index); the generation epoch serves the same-name path.

## Notes

**Remaining work:** a node-side delete-aware bounded changelog reader would let **changelog** indexes also
skip the brief post-cutover replay (append-only indexes already do, via the append catch-up) — a latency
optimization, not a correctness gap; it is blocked on iceberg-rust gaining an incremental-changelog scan.
**Cold/parked windowed reindex** (revive → build → promote → re-park) is another follow-up; today a
windowed reindex covers hot windows and skips parked ones. **Multi-stage append catch-up** is a third:
for an append-only index whose build ran for hours on a high-throughput cluster, the appended-since delta
that the catch-up applies **under the cutover fence** can be large, extending the fenced window. A staged
catch-up — an unfenced pre-pass to within a few seconds of head, then a brief fence for a tiny final
pass — would keep the fence short regardless of build duration. A latency optimization, not a correctness
gap: today's single fenced pass is exactly-once and correct, just fenced for the whole delta.
