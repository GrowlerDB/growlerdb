---
type: Decision
title: 'D38. Scale-limit entitlement (offline license)'
description: The free tier serves up to a fixed number of nodes — distinct live nodes that hold a primary of any index (Option A); more requires an offline-verified Ed25519 license. Lighting up a new node is capped at placement; existing units are never disrupted, replicas are free, and additional indexes co-located on already-counted nodes are free (D53).
tags: [decision, adr]
timestamp: 2026-07-26T00:00:00
---

# D38. Scale-limit entitlement (offline license)

**Decision.** The open-source tier serves up to a fixed number of **nodes** per deployment
(`FREE_NODE_LIMIT`) at no cost. Beyond that, the control plane refuses to place capacity that would
light up a **new** primary-holding node until a valid **Enterprise license** raises the cap —
**existing units and data are never disrupted** (re-resolving an already-placed unit, or re-placing
its dead owner, always passes; only genuinely new node capacity is gated).

**The metric: distinct live nodes holding a primary of any index — Option A, concurrent scale, never
lifetime usage ([D53](/system/decisions/d53-unit-replication.md)).** The count is how many nodes
currently hold at least one primary (of any index) — the node is the scale lever, matching the
already-node-named license `max_nodes` claim, the proto `max_nodes`/`current_nodes` fields, and the
console's "Nodes (in use / limit)" label. This supersedes the interim per-`(index, primary node)`-pair
count: co-locating primaries of many indexes on one node now costs **one**, so the 4-index demo
(`docs`, `catalog`, `movies` on the pool + `events` on its own node = 3 primary-holding nodes) fits
under `FREE_NODE_LIMIT = 3`. It intentionally does **not** grow with time: a windowed index
accumulating daily windows on one node costs **one** node forever (the earlier per-`(index,
shard|window)`-unit count bricked a free-tier daily-windowed index in three days — windows are never
retired, so lifetime unit counts measured age, not scale). What costs more is genuine horizontal
scale: primaries spread across more nodes. At the cap, a fresh unit **packs onto a node already
holding a primary** (of any index) rather than lighting up a new one — dense per-node packing is
allowed, node count is the lever. A node tracked-in-pool but heartbeat-stale stops counting (its
primaries re-place onto a live node); unknown liveness (the post-boot grace window, or announce-only
deployments with no heartbeats) **counts — the metric fails closed**.

**Enforced at every placement path, atomically.** The cap is checked *inside* the registry's
placement critical section (no check-then-place race across lock acquisitions) on both paths that
create primaries: CP-driven placement (`ResolveUnitOwner` / the dead-owner sweeper) and node
announces (`RegisterServedIndex` — formerly an unlimited fail-open bypass, now `RESOURCE_EXHAUSTED`
past the cap). The two paths differ in remedy: CP-driven resolve can *soft-pack* a fresh unit onto an
already-counted node (its endpoint is a free choice), so it never bricks while any primary-holding
node exists; a node **announce** has a fixed endpoint, so a not-yet-counted node announcing a primary
past the cap is refused `RESOURCE_EXHAUSTED`. Node registration (`RegisterNode`) is **uncapped**: a
node is interchangeable pool capacity, and a **read replica is free** (it is never a primary), so
enabling replication (`R > 1`, more holder nodes) never consumes the allowance. The license claim's
`max_nodes` name is now literal; `GetLicense` reports current/entitled **nodes** under this metric.

The license is a compact **Ed25519-signed token** (`GROWLERDB_LICENSE` on the control plane), verified
**offline** against a public key baked into the binary — no phone-home
([D26](/system/decisions/d26-telemetry.md)). An invalid token falls back to the free tier with a
warning. **Expiry is deferred** until pre-expiry notification + a grace period exist, so a lapsed
license can never cause a sudden outage.

This is how the open-core scale line is enforced: paid *features* live out-of-tree in the commercial
crate ([D37](/system/decisions/d37-extension-seams.md)); paid *scale* is gated here, in the OSS core,
without removing any capability.

**Issuing (the ceremony).** Licenses are minted **offline** by GrowlerDB LLC with the Ed25519
**private key**, which is held privately and **never** enters this repo. `License::mint()` +
`cargo run -p growlerdb-engine --example mint_license` sign a token from a private-key PEM; the matching
**public** key is the only half embedded in the binary (`LICENSE_PUBLIC_KEY_PEM`). Deployments consume
the token via `credentials.license` in the Helm chart → the `GROWLERDB_LICENSE` env on the control
plane (only the control plane enforces the cap). See `COMM-LICENSE.md` for the runbook.

**Scale runs** should carry a license so all N nodes are admitted **deterministically**. The cap is on
distinct **primary-holding nodes**, counted inside the CP's placement write lock so it is exact — the
old node-registration leak under staggered pod startup does not apply (registration is uncapped). A
scale run spreading primaries beyond `FREE_NODE_LIMIT` nodes still needs a license ([TASK-346]).

**Status.** Accepted; the metric is **Option A — distinct live primary-holding nodes** (superseding the
interim per-`(index, primary node)`-pair count), because it matches the marketed "3 nodes" free tier,
keeps read replicas free, and allows dense per-node packing (co-locating any number of indexes on one
node is free — node count is the scale lever). The embedded `LICENSE_PUBLIC_KEY_PEM` is currently a
**placeholder** (its private key was discarded), so no license validates yet — installing the real
signing keypair + minting the scale license is the outstanding ceremony ([TASK-346]).
