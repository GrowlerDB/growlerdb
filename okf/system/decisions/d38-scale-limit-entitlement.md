---
type: Decision
title: 'D38. Scale-limit entitlement (offline license)'
description: The free tier runs up to a fixed node count; more requires an offline-verified Ed25519 license. New nodes are capped; existing nodes are never disrupted.
tags: [decision, adr]
timestamp: 2026-07-11T00:00:00
---

# D38. Scale-limit entitlement (offline license)

**Decision.** The open-source tier runs up to a fixed number of index nodes per deployment
(`FREE_NODE_LIMIT`) at no cost. Beyond that, the control plane refuses to admit **new** node
registrations until a valid **Enterprise license** raises the cap — **existing nodes and data are never
disrupted** (a re-heartbeat of a live node always passes; only genuinely new capacity is gated).

The license is a compact **Ed25519-signed token** (`GROWLERDB_LICENSE` on the control plane), verified
**offline** against a public key baked into the binary — no phone-home
([D26](/system/decisions/d26-telemetry.md)). An invalid token falls back to the free tier with a
warning. **Expiry is deferred** until pre-expiry notification + a grace period exist, so a lapsed
license can never cause a sudden outage.

This is how the open-core scale line is enforced: paid *features* live out-of-tree in the commercial
crate ([D37](/system/decisions/d37-extension-seams.md)); paid *scale* is gated here, in the OSS core,
without removing any capability.

**Status.** Accepted.
