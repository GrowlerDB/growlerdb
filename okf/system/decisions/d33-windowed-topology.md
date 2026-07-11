---
type: Decision
title: D33. Distributed windowed topology — CP-driven placement, streaming-first
description: A windowed index is deployed as N interchangeable nodes that serve control-plane-assigned time windows (not fixed hash ordinals); nodes start empty and create each window on the first write the connector streams to it, resolved through the control plane on first ask. Enables the temporal (windowed + cold-tier) workload on k8s.
tags: [decision, adr, windowing, deployment, scale, controlplane]
timestamp: 2026-07-04T14:22:00
---

# D33. Distributed windowed topology — CP-driven placement, streaming-first

**Decision.** A **windowed** index is deployed distributed as **N
interchangeable nodes** that serve **control-plane-assigned time windows**, not fixed hash ordinals.
The control plane keeps a per-index **node inventory** (nodes heartbeat via `RegisterNode`;
in-memory, TTL'd — liveness is ephemeral, not durable topology) and **places each window on the
least-loaded live node on first ask** (`ResolveWindowOwner`, idempotent, dead-owner re-placement). It
is **streaming-first**: windowed nodes start **empty**, and the node creates a window's shard on the
**first write** the connector streams to it (`WindowedWriteService`, mirroring the batch
`write_windowed`), publishing it live — the search/suggest multiplexers read a shared, mutable window
map and the in-process gateway hot-swaps (`Gateway::swap_windowed`, window routing moved into the
swappable `RoutingState`). The connector computes each row's window id **byte-identically to the
engine** (`WindowRouter` ≡ `window_of ∘ field_micros`, the window field's `TimeFormat` carried on
`GetIndex`), resolves the owner, and streams the window's sub-batch with `from = None` (so a window
that skipped a batch isn't gap-rejected). The cluster gateway learns windows over the live control
plane and hot-reloads them; per-window checkpoints let the connector resume each window independently.

**Why.** Hash sharding (D12) fixes a shard set at build time and refuses a windowed index in-engine
(`ShardingWindowedUnsupported`); the temporal sweet spot needs windows that **form continuously** as
the timeline advances, so the topology must create + place + serve them at runtime. CP-driven
placement keeps nodes interchangeable (any node hosts any window) so the pool scales without a
build-time partition, and centralizes the window→node map that both the connector (writes) and gateway
(reads) route through — they can't drift. Streaming-first (create-on-first-write) avoids a build-time
window enumeration and matches how the source grows; `from = None` on window sub-batches (matching
`partition_batch`) keeps each window on its own checkpoint lattice, so a window that receives no rows
in a batch doesn't wedge the node's continuity guard (D31). The alternative — a fixed `window % N`
hash — reintroduces a build-time count and can't rebalance, and was rejected for exactly the
inflexibility that motivates windowing.

**Consequences.** The node inventory + placement are **in-memory** on the control plane: after a CP
restart nodes re-register within a heartbeat interval; window *assignments* stay durable in the
registry. Placement is **primary-only** — window read-HA/replicas remain future work
([windowed replica gap](/quality/known-limitations/windowed-replica-gap.md)). Re-placing a dead
owner's window moves the *assignment*; the new owner rebuilds that window's data from source on demand
(a follow-up — until then a dead node's windows are unavailable until it returns). Resume is the min
committed checkpoint across **committed** windows: correct (idempotent replay), but a cold restart can
re-read from the oldest active window — bounding this to active windows is a follow-up. Connector
**worker parallelism** (D32) and **distributed batch-build** with placement are out of scope (single
connector, streaming-only). Source-side Iceberg maintenance stays the user's concern (the scale-run
CronJob is hash-tuned; a window-partition sort key is a convenience follow-up). String-date window
fields aren't supported for connector-side routing yet (numeric-epoch only — a loud error, not a
silent misroute).

**Status.** Accepted. Engine + connector unit/integration tested (placement, `swap_windowed`,
dynamic mux, `WindowedWriteService` create-on-write + per-window checkpoint, `WindowRouter` parity,
`WindowedWriteClient` partition). Extends **D12** (adds a time-window sharding mode alongside hash),
**D9** (reconcile is per-shard, unaffected), and the windowing feature. Closes the
[windowed k8s topology](/quality/known-limitations/windowed-k8s-topology.md) gap; a live on-cluster
convergence run remains.
