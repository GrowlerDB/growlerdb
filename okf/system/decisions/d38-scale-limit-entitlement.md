---
type: Decision
title: 'D38. Scale-limit entitlement (offline license)'
description: The free tier serves up to a fixed number of entitlement units — distinct live (index, primary node) pairs; more requires an offline-verified Ed25519 license. New pairs are capped at placement; existing units are never disrupted, and read replicas are free (D53).
tags: [decision, adr]
timestamp: 2026-07-26T00:00:00
---

# D38. Scale-limit entitlement (offline license)

**Decision.** The open-source tier serves up to a fixed number of **entitlement units** per deployment
(`FREE_UNIT_LIMIT`) at no cost. Beyond that, the control plane refuses to place capacity that would
create a **new** unit until a valid **Enterprise license** raises the cap — **existing units and data
are never disrupted** (re-resolving an already-placed unit, or re-placing its dead owner, always
passes; only genuinely new capacity is gated).

**The metric: distinct live `(index, primary node)` pairs — concurrent scale, never lifetime usage
([D53](/system/decisions/d53-unit-replication.md)).** An entitlement unit is one index being
primary-served by one node. This intentionally does **not** grow with time for a small deployment: a
windowed index accumulating daily windows on one node costs **one** unit forever (the earlier
per-`(index, shard|window)`-unit count bricked a free-tier daily-windowed index in three days —
windows are never retired, so lifetime unit counts measured age, not scale). What costs more is
genuine horizontal scale: more indexes, or one index spread across more primary nodes. At the cap,
new units of an already-paired index **pack onto a node already primarying it** rather than being
refused. Pairs whose node is tracked-in-pool but heartbeat-stale stop counting (their units re-place,
moving the pair); unknown liveness (the post-boot grace window, or announce-only deployments with no
heartbeats) **counts — the metric fails closed**.

**Enforced at every placement path, atomically.** The cap is checked *inside* the registry's
placement critical section (no check-then-place race across lock acquisitions) on both paths that
create primaries: CP-driven placement (`ResolveUnitOwner` / the dead-owner sweeper) and node
announces (`RegisterServedIndex` — formerly an unlimited fail-open bypass, now `RESOURCE_EXHAUSTED`
past the cap). Node registration (`RegisterNode`) is **uncapped**: a node is interchangeable pool
capacity, and a **read replica is free** (it is never a pair's primary), so enabling replication
(`R > 1`, more holder nodes) never consumes the allowance. The license claim keeps its historical
`max_nodes` name, but its meaning is entitlement units; `GetLicense` reports current/entitled units
under the same metric.

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

**Scale runs** should carry a license so all N units are admitted **deterministically**. Since the cap
is now on **placed pairs** (not nodes registered), placement is serialized through the CP's write lock
so counting is exact — the old node-registration leak under staggered pod startup no longer applies
(nodes are uncapped). A scale run spreading an index beyond `FREE_UNIT_LIMIT` primary nodes still
needs a license ([TASK-346]).

**Status.** Accepted. The embedded `LICENSE_PUBLIC_KEY_PEM` is currently a **placeholder** (its private
key was discarded), so no license validates yet — installing the real signing keypair + minting the
scale license is the outstanding ceremony ([TASK-346]).
