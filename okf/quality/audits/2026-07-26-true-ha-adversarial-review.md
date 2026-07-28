---
type: Audit
title: Adversarial review — feat/true-ha (D51–D53), 2026-07-26
description: In-depth adversarial code audit of the true-HA feature branch (replicated control plane, universal placement pool, per-unit replication) against main. Records every verified finding with stable IDs (HA-A1…HA-T2), severity, and location, and maps them to the fix tasks under backlog epic 357.
tags: [quality, audit, ha, control-plane, node, gateway, connector, security]
timestamp: 2026-07-26T12:00:00
---

# Adversarial review — `feat/true-ha` (D51–D53), 2026-07-26

Scope: the full `feat/true-ha` diff vs `main` (~6,300 insertions, 25 commits) implementing
[D51](/system/decisions/d51-controlplane-ha.md) (replicated control plane),
[D52](/system/decisions/d52-placement-pool.md) (universal placement pool) and
[D53](/system/decisions/d53-unit-replication.md) (per-unit replication with cold-tier read-through
failover). Method: five parallel adversarial deep-dives (CP persistence, placement/entitlement,
engine pool/node, CLI serve-pool/gateway, connector + deploy), every finding verified end-to-end
against the code, the most severe re-verified independently. Fix work is tracked as subtasks of
backlog epic **357** (IDs noted per finding); this document is the durable record and stays put —
resolved findings get their task marked Done, not deleted here.

**Overall verdict.** Steady-state plumbing is solid: the pool dispatch layer is concurrency-clean
and leak-free, exactly-once mechanics hold within a stable topology, and auth/secret hygiene checked
out (no SQL injection, DSN never logged, service token on all new RPCs, `Scope::Ops` fail-closed,
path traversal fenced, non-root image). The risk concentrates exactly where the feature's value is
claimed — **topology change**: write fencing, failover error handling, CP leader handoff, connector
placement refresh, and the Helm/K8s lifecycle.

Severity: **C** critical, **H** high, **M** medium, **L** low.

## A. Write-path fencing — split-brain writes (task 357.12, connector side 357.13)

- **HA-A1 (C)** — No fencing anywhere on the write path. `resolve_unit_holders`
  (`growlerdb-controlplane/src/registry.rs`) prunes a "dead" primary and promotes a replica with no
  epoch/fence (bypassing the guarded `promote_replica` path and its split-brain test);
  `PoolWriteService::write` (`growlerdb-engine/src/pool_routing.rs`) routes on the `index` selector
  with zero holdership validation; the node never drops de-assigned units; the connector caches
  placement for process lifetime. A GC-paused-but-reachable primary keeps accepting commits after a
  replica is promoted → divergent data for one unit.
- **HA-A2 (C)** — A write addressed to a window held only as a cold replica bypasses the
  `WINDOW_PARKED` guard (replica windows are published to the read mux only, not to
  `WindowedWriteService.windows`), so `ensure_window` creates a fresh empty shard and **overwrites
  the served parked snapshot** — reads flip from full data to near-empty and the write is accepted.
- **HA-A3 (H)** — `get_checkpoint` on a non-primary holder returns "no checkpoint, snapshot 0"
  (`windowed_ingest.rs`), actively instructing a misrouted connector to re-ingest a window from
  scratch on the wrong node.
- **HA-A4 (L)** — Check-then-act race between `reconcile_replica_windows` and the write path: a hot
  window created during the slow cold-open gap is clobbered by the stale cold entry.

## B. Read failover — FailoverNode (task 357.14; health/push follow-on 357.15)

- **HA-B1 (C)** — The `failover_read!` macro (`growlerdb-engine/src/node.rs`) rebuilds the request
  with `Request::new(msg.clone())`, dropping the gateway-stamped `x-growlerdb-tenant` /
  `x-growlerdb-principal` and `grpc-timeout`. Node-side tenant scoping is fail-closed and every
  CP-routed window (even single-holder) is now wrapped in a FailoverNode → every windowed read on a
  tenant-scoped index returns `PermissionDenied`; a fail-open hook would silently lose tenant
  filtering instead.
- **HA-B2 (H)** — `is_holder_down` matches only `Unavailable|DeadlineExceeded`; tonic maps the
  endpoint timeout to `Cancelled` and mid-request resets to `Unknown`/`Internal` → hung/blackholed
  holders never fail over. No HTTP/2 keepalive configured.
- **HA-B3 (H)** — Per-attempt timeout (30 s) equals the gateway scatter deadline (30 s), so a
  timed-out primary exhausts the budget before the replica attempt starts.
- **HA-B4 (H)** — A not-yet-warmed replica answers `InvalidArgument("window not served")`, which
  failover treats as request-level and **aborts** — replica #2 is never tried; a hot-window node
  loss surfaces as client-facing `InvalidArgument` (should be `Unavailable`), since hot windows have
  no replica (cold-only read-through, HA-B7).
- **HA-B5 (M)** — `require_complete` is not pinned to the primary, contradicting D53; a replica
  answer counts as complete/fresh with no freshness signal (concrete via the promote-cold revive
  edge, where a replica can serve a frozen pre-revive snapshot).
- **HA-B6 (M)** — No health memory: every read re-probes the dead primary first (up to the 5 s
  connect timeout per read when blackholed) until the gateway's 15 s *poll* catches re-placement;
  the reloader rebuilds fresh channels every tick, discarding warm connections.
- **HA-B7 (M)** — Replica failover exists only for parked (cold) windows; a hot window has exactly
  one copy. D53 scopes this ("hot tail until warmed") but the headline "zero-gap node kill" holds
  only for the cold tier — documented as a limitation until hot-tail shipping lands.

## C. Control-plane HA — leadership & persistence (task 357.16; sessions 357.17)

- **HA-C1 (H)** — The leader never demotes: `is_writer.store(true, …)` is the only store call in
  `postgres_backend.rs`; nothing revalidates the advisory lock. A dropped PG session (PG failover,
  worker-thread panic) releases the lock, a standby promotes, and the old leader keeps
  `mark_ready()` every 250 ms → two READY replicas, one serving a frozen catalog.
- **HA-C2 (H)** — Promotion sets `is_writer=true` *before* `reload()`; a reload failure is only
  logged, then `mark_ready()` runs. With `persist_registry` an unconditional full-envelope overwrite
  (no store-level version CAS anywhere — D51's "CAS maps to the store" is not implemented), the
  promoted leader's first mutation durably erases writes the dead leader made after the standby's
  last 250 ms poll.
- **HA-C3 (M)** — Every mutation is apply-in-memory-then-persist with no rollback: a failed persist
  reports `Err` but the next successful mutation's snapshot silently commits the "failed" change
  (incl. resurrecting a revoke that reported failure).
- **HA-C4 (M, security)** — Session-epoch revocation is lossy across failover (`revoke_sessions`
  swallows persist failure; the sessions row's version bump is invisible to standbys polling the
  `registry` row) — a JWT revocation can resurrect on the new leader. `persist_sessions` also runs
  while the `session_epochs` write guard is held, putting a PG round-trip on the auth hot path.
- **HA-C5 (M)** — N replicas run `CREATE TABLE IF NOT EXISTS` concurrently at boot → known Postgres
  duplicate-type race → crash-loop until restarts de-interleave; standbys need DDL privileges.
- **HA-C6 (L)** — A standby's refused write maps to gRPC `Internal` (should be
  `FAILED_PRECONDITION`/`UNAVAILABLE`) and still mutates standby memory, which standby reads serve
  until the next version-triggered reload.
- **HA-C7 (L)** — `row.get` panics on schema drift (unwinds the worker thread, silently releasing
  the advisory lock — amplifies HA-C1); a single serialized PG connection blocks tokio workers on a
  sync channel.

## D. Placement & entitlement (tasks 357.18 notify/liveness, 357.19 entitlement, 357.20 hub, 357.21 proto)

- **HA-D1 (H)** — Assignment pushes fire from exactly one place (`ResolveUnitOwner`);
  `RegisterServedIndex` re-points, `drop_index`, `promote_replica`, `remove_node` never notify —
  contradicting the proto contract ("a full snapshot on every placement change").
- **HA-D2 (H)** — Dead-owner re-placement is write-driven only (no sweeper): quiescent units on a
  dead node point at the corpse indefinitely; at R=1 they are simply unavailable.
- **HA-D3 (H)** — Entitlement is bypassable and mis-scoped: enforcement lives solely in
  `ResolveUnitOwner` while `RegisterServedIndex` creates unlimited primary units (fail-open; the old
  node cap is gone and `register_node_capped` is dead code); the check is TOCTOU-racy across three
  lock acquisitions; and because windows are never retired, a daily-windowed free-tier index
  exhausts `FREE_UNIT_LIMIT = 3` in three days and bricks (`RESOURCE_EXHAUSTED` forever).
- **HA-D4 (M, security/perf)** — `SubscribeAssignments` hub: the seed snapshot is computed before
  the sender registers (a concurrent placement is clobbered by the stale seed); senders are never
  evicted (O(historical-endpoints × total-units) per placement change, unbounded growth); `endpoint`
  is an unauthenticated identity claim — any ops-scoped caller can read another node's stream.
- **HA-D5 (M)** — Node heartbeat interval (30 s, jittered to ~36 s) equals the liveness TTL
  (30 s) → healthy nodes flap out of the pool; and a freshly promoted/restarted CP has an empty
  in-memory pool with no grace period, so early resolves mass-re-place laggards' units onto the
  first re-registrant.
- **HA-D6 (M)** — `RegisterNodeRequest` silently repurposes proto field 1 (`index` → `endpoint`,
  no `reserved`): an old node's heartbeat decodes as a garbage endpoint that least-loaded placement
  then *prefers*. `ResolveWindowOwner` removal is at least a loud break.
- **HA-D7 (L)** — `resolve_unit_owner` (R=1) ignores existing replicas on primary death (fresh
  placement instead of promotion; can co-locate primary on its own replica); excess replicas are
  never trimmed; `created` conflates "assignment moved" with any holder change. Concurrent
  `RegisterServedIndex` is last-write-wins on the primary (no `PLACEMENT_CONFLICT`).

## E. Connector (task 357.13)

- **HA-E1 (H)** — Placement is resolved once per process (hash: at startup; windowed:
  `computeIfAbsent` cache, no invalidation) and the stream-restart loop reuses the same
  `BatchWriter` → after re-placement, ingest either halts forever against the dead endpoint or
  silently keeps committing to the alive-but-deposed node (write/read divergence, pairs with HA-A1).
- **HA-E2 (M)** — Resume is structurally broken against pool nodes: hash-path writes are
  index-tagged but `resumeMin`/`drainedTo` call `checkpoint(0L, "")` (non-retryable
  `InvalidArgument` on a multi-index node → crash-loop); the single-endpoint path drops the index
  tag entirely.
- **HA-E3 (M)** — `ControlPlaneClient` has no per-call deadline and no retry; `resolveWindowOwner`
  is on the write hot path — a force-killed CP pod freezes ingestion silently.
- **HA-E4 (L)** — `resolveShardOwner` is dead code; duplicate `ShardStatus` ordinals are silently
  last-wins.

## F. Deploy — Helm/K8s & CI (task 357.22; CI lane 357.23)

- **HA-F1 (H)** — Leader-only readiness + default Deployment strategy wedge every HA rolling update
  permanently (surged standby never Ready, `maxUnavailable=0` blocks scale-down of the old leader);
  `kubectl rollout status` (as instructed by NOTES.txt) never completes even on first install with
  replicas > 1.
- **HA-F2 (H)** — The CP PDB (`minAvailable: 1`) pins `disruptionsAllowed` to 0 for the leader —
  no preStop lock handover exists, so draining the leader's host blocks forever.
- **HA-F3 (M)** — Toggling `externalRegistry.enabled` on an existing release fails on the immutable
  Service `clusterIP`, and nothing migrates `registry.json` into Postgres — a forced switch boots an
  **empty registry**.
- **HA-F4 (M)** — The entire Postgres/HA test path never runs in CI: `postgres_backend_ha_lifecycle`
  is gated on `GROWLERDB_TEST_POSTGRES_URL`, which no workflow or justfile sets (the in-code claim
  "CI's integration lane sets it" is false).
- **HA-F5 (L)** — No secret-checksum pod annotation (DSN/token rotation never rolls CP pods);
  `registryPostgresEnv` lacks `optional: true` fail-fast nuance. Dockerfile itself is clean.

## G. serve-pool node operations (task 357.24; fairness 357.25)

- **HA-G1 (M)** — Windows are never unloaded (mmaps/compaction accumulate for every window ever
  served; the node keeps answering for units moved elsewhere) and `.replica/` scratch survives
  de-assignment and reboot.
- **HA-G2 (M)** — `--register` without an object store still joins the pool with no capability
  signal: the CP places replica windows the node can never serve — HA silently absent cluster-wide.
- **HA-G3 (M)** — Per-index heavy-read share is a static boot-time split charged per *window*
  sub-request: a single multi-window aggregate self-sheds on an idle cluster; idle indexes withhold
  budget (non-work-conserving).
- **HA-G4 (L)** — serve-pool handles only SIGINT (K8s SIGTERM = ungraceful kill, no deregistration);
  assignment-stream reconnect is a fixed 3 s sleep (fleet-synchronized herd); one corrupt window
  shard at boot fails the whole multi-index process; local-fs object store writes are non-atomic
  (torn `cold.json` is self-healing; re-park overwrites under live readers).

## T. Test posture (task 357.23)

- **HA-T1 (H)** — The chaos drill does not test its headline: one query pre-kill (no sustained
  load), a **fresh** gateway post-kill (sidesteps the established-channel mode of HA-B2), a retry
  loop whose `.ok()?` swallows a 5 s outage, and a no-`partial` assertion on the single-shard fast
  path that structurally never sets `partial`. It proves "eventual failover on connection-refused
  for a parked window", not "zero gap, no partial, under sustained query".
- **HA-T2 (M)** — Untested: `spawn_cp_leadership` (zero tests), old-primary-alive fencing, writes to
  replica-held windows, FailoverNode metadata pass-through, failover past a not-yet-serving replica,
  `require_complete` pinning, connector index-tag on the wire / re-placement mid-stream,
  entitlement lifetime/races, subscribe/notify interleaving, hub cleanup, cross-version proto
  decode, post-failover empty-pool churn. `gateway_pool.rs`/`serve_pool.rs` are happy-path only.

## Verified clean

No SQL injection (all queries use binds); Postgres DSN never logged, delivered via Secret; every new
mesh RPC behind the service token and `Scope::Ops` (fail-closed on unknown methods); object-store
path traversal fenced; scatter-gather cannot double-count a window (FailoverNode returns exactly one
holder's response per unit); exactly-once batch-id/lineage-min resume solid within a stable
topology; `admit_heavy` permit handling leak-free; pool dispatch lock discipline sound; Dockerfile
non-root with no baked secrets.
