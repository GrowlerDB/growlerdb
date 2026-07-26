---
type: Decision
title: 'D38. Scale-limit entitlement (offline license)'
description: The free tier serves up to a fixed number of primary-held units; more requires an offline-verified Ed25519 license. New units are capped at placement; existing units are never disrupted, and read replicas are free (D53).
tags: [decision, adr]
timestamp: 2026-07-11T00:00:00
---

# D38. Scale-limit entitlement (offline license)

**Decision.** The open-source tier serves up to a fixed number of **primary-held units** per deployment
(`FREE_UNIT_LIMIT`) at no cost. Beyond that, the control plane refuses to place a **new** primary unit
until a valid **Enterprise license** raises the cap — **existing units and data are never disrupted**
(re-resolving an already-placed unit, or re-placing its dead owner, always passes; only genuinely new
capacity is gated).

**Counting units, not node processes ([D53](/system/decisions/d53-unit-replication.md)).** The metric
is the number of `(index, shard|window)` units with an assigned **primary** — the served data
partitions — enforced at unit **placement** (`ResolveUnitOwner`). Node registration (`RegisterNode`) is
**uncapped**: a node is interchangeable pool capacity, and a **read replica is free** (it adds no new
primary unit), so enabling replication (`R > 1`, more holder nodes) never consumes the allowance. The
license claim keeps its historical `max_nodes` name, but its meaning is units; `GetLicense` reports
current/entitled units.

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
is now on **units placed** (not nodes registered), placement is serialized through the CP so counting is
exact — the old node-registration leak under staggered pod startup no longer applies (nodes are
uncapped). A scale run beyond `FREE_UNIT_LIMIT` units still needs a license ([TASK-346]).

**Status.** Accepted. The embedded `LICENSE_PUBLIC_KEY_PEM` is currently a **placeholder** (its private
key was discarded), so no license validates yet — installing the real signing keypair + minting the
scale license is the outstanding ceremony ([TASK-346]).
