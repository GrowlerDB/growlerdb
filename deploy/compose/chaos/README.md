# Chaos drills (Compose)

Fault-injection drills for the single-host Compose stack. Each drill injects a failure
against a **running** stack and asserts the system self-heals — the Compose analogue of the
Kubernetes chaos scenarios (pod/node faults via the Helm chart), which live separately.

## Prerequisites

- The full stack is up: `just stack`.
- Host-side deps mapped: `127.0.0.1 minio` in `/etc/hosts` (see `../README.md`).

## Drills

| Drill | Injects | Asserts |
|---|---|---|
| `crash-recovery.sh [node\|gateway\|controlplane]` | `docker kill` (SIGKILL) of a core service | it self-restarts (the `restart:` policy), `/readyz` recovers, and search still answers through the gateway — all within the RTO bound |
| `catalog-outage.sh` | `docker kill` of Polaris (the Iceberg catalog) | **search stays available** during the outage (local index, catalog-independent), Polaris self-restarts, and **hydration recovers automatically** when it returns (`keys:get` reads the authoritative Iceberg row — end-to-end proof the catalog survived the bounce with its tables). Requires `jq`. |

```sh
# crash a core service (default `node`)
just chaos                       # or: deploy/compose/chaos/crash-recovery.sh gateway
# catalog outage
just chaos-catalog               # or: deploy/compose/chaos/catalog-outage.sh
```

Shared bash helpers (compose handle, `wait_http`, `wait_restart`, …) live in `lib.sh`, sourced by
each drill.

A drill exits non-zero (with `DRILL FAILED: …`) if recovery doesn't happen within the timeout, so it
doubles as a regression gate for the self-healing policies — run it after any change to the stack's
`restart:`/healthcheck configuration.

## Self-heal posture

The core services (`controlplane`, `node`, `gateway`) and their long-running deps (`minio`,
`polaris`, `polaris-db`, `lgtm`, `redpanda`) carry `restart: unless-stopped` in
[`docker-compose.yml`](../docker-compose.yml), so a crash or OOM-kill brings them back automatically
instead of leaving the stack half-down. One-shot jobs (`createbuckets`, `polaris-bootstrap`, `seed`,
`spark`) intentionally do **not** restart. The Kubernetes deployment gets the same posture from pod
`restartPolicy` + liveness/readiness probes + PodDisruptionBudgets — see the
[Helm chart](../../helm/growlerdb/README.md).
