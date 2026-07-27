---
type: Component
title: Control plane
description: The cluster registry — indexes, shards, routing, tokens, roles — and the source of routing truth.
tags: [component, control-plane, registry]
resource: /crates/growlerdb-controlplane
timestamp: 2026-07-26T00:00:00
---

# Control plane

A lightweight gRPC service (the `ControlPlane` API) holding the cluster's registry: index definitions,
shard/ordinal assignments, the [bucket routing map](/system/distribution.md), API
[tokens](/product/functional/auth/tokens.md), [role bindings](/product/functional/rbac-and-tenancy.md),
built-in credentials, session epochs, and the per-index activity log.

## Responsibilities

- **Vends routing** — [gateways](/system/runtime/components/gateway.md) build their shard routing from
  `GetIndex` (primaries + bucket map) and hot-reload on change.
- **Single writer** — an exclusive advisory lock; mutations apply in memory then persist **off the
  data lock** (registry JSON + `.prev` fallback + sidecars for activity/sessions), so routing reads
  never block on fsync.
- Serves auth-state lookups (O(1) token hash index) with a consistent lock order.

## Placement liveness & pushes (D52/D53)

- **Heartbeats & TTL** — pool nodes re-announce every **10 s** (±20% jitter;
  `NODE_REANNOUNCE_INTERVAL_MS`, from which the CLI derives its interval so the two can't diverge);
  liveness TTL is **3×** that (**30 s**, `NODE_HEARTBEAT_TTL_MS`), so one lost/jittered heartbeat
  never ejects a healthy node. `RegisterNode` validates the endpoint (`[http[s]://]host:port`) —
  a malformed value fails loudly instead of seeding placement with a garbage node.
- **Pushes on every placement change** — `SubscribeAssignments` snapshots are pushed from a hook at
  the registry's **persist boundary** (fingerprint-filtered), so *every* mutation path — resolve,
  `RegisterServedIndex` re-point, drop-index, promote/remove-node, the sweeper, a promoted leader's
  reload — notifies the affected nodes; no mutation can forget to. Subscribing requires a
  currently-registered node endpoint; dead subscriber channels are evicted on the next push.
- **Dead-owner sweeper** — a leader-only background task (every TTL/2) re-places units whose primary
  is confidently dead through the same idempotent, entitlement-aware resolve path, so quiescent
  units on a dead node recover without waiting for a write (at R=1 they'd otherwise be unavailable
  forever).
- **Replica top-up sweeper** — the counterpart at `R > 1` (also leader-only, every TTL/2): it brings
  every **placed** unit back up to `R` live holders through the same resolve path, so **read HA does
  not depend on write activity**. A batch-built, read-served index (no connector to drive a
  write-time resolve) still gets its replicas placed, and a node join or loss self-heals the replica
  set — the piece that makes "delete a pod and reads keep answering" hold for a quiescent index.
  Grace-aware, and a no-op at `R = 1`.
- **Startup/promotion grace** — for one TTL after the first post-boot/post-promotion heartbeat,
  assigned owners are treated live-unknown: dead-owner re-placement, announce takeovers, and the
  sweeper hold off, so a freshly (re)started CP never mass-re-places laggards' units onto the first
  re-registrant. Fresh placements and empty-pool retryable errors behave as before.
- **First-wins announces** — a `RegisterServedIndex` claim on a unit primaried by a live foreign
  node is `PLACEMENT_CONFLICT` (idempotent re-announce and dead-owner takeover stay allowed; a
  listed replica's announce is a serving report). The scale entitlement
  ([D38](/system/decisions/d38-scale-limit-entitlement.md): distinct live index × primary-node
  pairs) is enforced atomically inside placement on both the resolve and announce paths.

## Internal-RPC credential

The internal RPCs (registration, shard-map reads, placement) are a service-to-service layer, distinct
from the user [RBAC](/product/functional/rbac-and-tenancy.md) that governs data-plane requests. They can
be gated with a shared **service token** (`GROWLERDB_SERVICE_TOKEN`): when set, every RPC must carry the
matching token (constant-time checked) or is rejected — closing the internal RPCs to callers outside the
mesh, independent of the user-auth mode. Unset ⇒ open (local dev). The control plane can also serve over
[TLS/mTLS](/product/functional/auth/mtls.md), optional and off by default.

## Availability

By default the CP is a **single instance** (`replicas: 1`) — the cluster's one hard SPOF. HA
([D51](/system/decisions/d51-controlplane-ha.md)) moves durable state behind a **backend seam**: the
embedded single-writer JSON store stays the default for single-binary/Compose, and an **externalized
backend** (Postgres) holds all durable registry state so the CP runs as **N stateless replicas**
behind a Service, delegating consensus to the store. Reads are already off the data lock and the node
inventory is already ephemeral, so this is a store swap, not a new consensus engine.

**Running it:** `control-plane --registry-postgres <url>` (env `GROWLERDB_REGISTRY_POSTGRES`; needs a
build with the `postgres` feature) points every replica at one shared Postgres. Each starts as a warm
**standby**: it serves reads and continuously reloads its in-memory catalog when the store's
version advances (the leader wrote). Exactly one replica holds a **session-level advisory lock** — the
leader — and is the sole writer; a non-leader refuses every persist so it can never corrupt the store.
Coordination is **active-passive**: readiness tracks leadership, so `/readyz` is `200` only on the
leader and the Service routes all traffic to it while standbys stay warm but out of rotation. When the
leader dies, Postgres releases the lock, a standby wins it within a fraction of a second (`kill -9`
→ promotion verified live), flips to ready, and joins the Service. Standbys **serving reads** directly
(active-reads, per the D51 diagram) is a follow-up. See [high availability](/system/high-availability.md).

**Leadership is verified, ordered, and store-checked** (357.16/357.17):

- **Demotion** — the leader re-verifies each tick that the lock-holding store session is alive; a
  dead session (Postgres restart, network drop, worker panic) demotes it the same tick — readiness
  withdrawn, writership resigned — and it rejoins the standby race on a fresh connection. No deposed
  leader ever sits READY serving a frozen catalog.
- **Promotion order** — acquire lock → **reload from the store** → only then confirm writership and
  mark ready; a failed reload resigns the lock and stays standby. Writes are gated on the confirmed
  flag, so a promoted leader can never persist a stale pre-promotion snapshot over the dead leader's
  last writes.
- **Versioned persists** — every persist carries an expected-version guard (optimistic concurrency
  per store row; D51's "CAS maps to the store"): a mismatch means another writer took over → the
  persist is refused, the replica fail-stops (demote + reload), and the caller sees a retryable
  `NOT_LEADER` (`FAILED_PRECONDITION`, never `Internal`).
- **Rollback on persist failure** — a mutation whose persist fails is rolled back out of memory
  (restore from the store), so a failed change can never ride out on the next successful snapshot;
  if even the restore fails, persists latch off until a resync succeeds.
- **Session-epoch durability** — a revocation whose persist fails is a hard error (and rolled back),
  never a warn-and-continue; the standby version poll observes **all** store rows, so a sessions-only
  bump reloads standbys too and a revocation survives failover.
- Concurrent replica boots serialize their schema DDL under an advisory lock (no `CREATE TABLE IF
  NOT EXISTS` race).

## Notes

Implemented in `growlerdb-controlplane`. Its persistent state is small; durability is temp+fsync+rename.
