---
type: Decision
title: D51. Control-plane HA — externalized replicated registry, N stateless replicas
description: The control plane's durable registry moves behind a backend seam; a new externalized backend (Postgres/etcd) holds all durable state so the CP runs as N stateless replicas behind a Service, delegating consensus to a mature store. The embedded single-writer JSON backend stays the default for single-binary/Compose. Closes the control-plane SPOF without putting the CP on the hot query path.
tags: [decision, adr, control-plane, ha, availability, registry]
timestamp: 2026-07-25T09:00:00
---

# D51. Control-plane HA — externalized replicated registry, N stateless replicas

**Decision.** The [control plane](/system/runtime/components/control-plane.md)'s durable state moves
behind a **registry backend seam**. Two backends, one API:

- **Embedded (default)** — the current single-writer JSON store (registry JSON + `.prev` + sidecars,
  temp+fsync+rename). Keeps [single-binary](/system/deployment/single-binary.md) / Compose
  zero-dependency. Single instance; no behavior change.
- **Externalized (HA)** — all durable registry state (index defs, `bucket_owners` maps, tokens, role
  bindings, built-in credentials, session epochs, activity log) lives in a **replicated external
  store** (Postgres — the [deps kustomize](/system/deployment/helm-k8s.md) already provisions it — or
  etcd). The CP then runs as **N stateless replicas** behind a Service; consensus/durability is the
  store's job.

With the externalized backend the **single-writer advisory lock becomes a store-level transaction /
conditional write**, and the placement compare-and-swap ([distribution](/system/distribution.md))
maps to optimistic concurrency: two concurrent placement ops still can't last-write-wins each other —
the loser gets `PLACEMENT_CONFLICT` (`FAILED_PRECONDITION`) and re-plans, now enforced by the store's
CAS instead of the in-process lock. Reads stay off any write path. The **node inventory stays
in-memory + TTL'd** ([D33](/system/decisions/d33-windowed-topology.md)) — it is ephemeral liveness,
not durable topology, and every replica rebuilds it from heartbeats; window/unit *assignments* are
durable in the store.

**Why.** The CP is the one hard [SPOF](/system/high-availability.md) (`replicas: 1`,
values.yaml: *"HA is a later milestone"*), and the placement pool ([D52](/system/decisions/d52-placement-pool.md))
leans on it harder, so it must close in the same effort. The CP is a **small, low-write registry**
whose reads are already off the data lock and whose inventory is already ephemeral — so the missing
piece is a durable, replicated store with fast leader failover, **not** a bespoke consensus engine.
Delegating to Postgres/etcd is the least new consensus code we own and test, and the deps stack
already ships Postgres. Chosen over an embedded Raft-replicated registry (self-contained but we'd own
consensus correctness) and over leader-election + object-store standby (lightest, but seconds-scale
lease-TTL failover with a write-freeze during handoff).

**Rejected — CP as query router.** Reaffirmed from [D35](/system/decisions/d35-multi-index-routing.md):
the CP stays a **registry** (`GetIndex`/`ListIndexes`/placement), never a routing proxy on the hot
query path. HA here is about registry availability, not moving query traffic through the CP.

**Consequences.** The externalized backend is an **optional deployment mode**, not a new hard
dependency — the default build and single-binary path keep the embedded backend, so the AGPL core
stays runnable with no external store. The registry API must be factored so both backends satisfy it
(the current in-memory-apply-then-persist shape maps to a transaction). Schema/migration for the
external store is owned in-repo (the store is an implementation detail, not a user contract). Backup
/ restore of the registry gains a store-native path alongside the JSON snapshot. Ties into the
[chaos](/quality/reliability.md) suite: a CP replica kill and a leader/store failover under live
mutation must assert routing + registration continuity.

The 2026-07-26 [adversarial audit](/quality/audits/2026-07-26-true-ha-adversarial-review.md)
(HA-C1..C7) hardened the implemented leader/standby protocol: leadership is **re-verified every
tick** (a dead lock-holding session demotes the leader immediately — readiness off, writership
resigned, standby race rejoined on a fresh connection); promotion is **lock → reload → confirm**, so
a new leader can never persist a stale snapshot; every persist carries an **expected-version guard**
— the store-level CAS this decision promised — refusing (retryable `NOT_LEADER`,
`FAILED_PRECONDITION`) and fail-stopping a replica whose leadership lapsed; a failed persist **rolls
the mutation back out of memory**; session-epoch revocations are **hard-fail durable** and the
standby poll observes all store rows so they survive failover.

**Status.** Accepted (design). Implementation staged in the `true-ha` epic; not yet built. Extends the
control-plane component and refines the availability posture in [high availability](/system/high-availability.md).
