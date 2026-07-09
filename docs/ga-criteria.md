---
title: GA criteria
layout: default
nav_order: 9
---

# GA criteria & readiness

A living checklist of what GrowlerDB considers required for **General Availability**, with current
status. "Met" means there is shipped code/CI/docs evidence; "Partial"/"Pending" calls out the gap
honestly.

## Functionality

| Criterion | Status | Evidence |
|---|---|---|
| Index → search → hydrate (the core loop) | ✅ Met | `/v1/search` + `/v1/keys:get`; Compose E2E in CI (`e2e` job) |
| Distributed: control plane + sharded nodes + gateway | ✅ Met | M3; scatter-gather + top-K merge; node self-registration |
| Query language (native AST + Lucene/KQL) | ✅ Met | `growlerdb-core::query` |
| Console UI (search, indexes, ingestion, observability) | ✅ Met | M6 (`ui/`) |
| OpenSearch `_search` adapter (read, documented subset) | ✅ Met | [opensearch-adapter.md](opensearch-adapter) |

## Security

| Criterion | Status | Evidence |
|---|---|---|
| AuthN at the gateway (OIDC/JWT, API keys, mTLS) | ✅ Met | `growlerdb-engine::authn` |
| Control-plane RBAC | ✅ Met | `growlerdb-engine::rbac` |
| **Tenant isolation verified end-to-end** | ✅ Met | `tests/tenant_isolation.rs` — forged headers/query-widening can't cross tenants; unauth rejected before the shard |
| Data-plane authz delegated to the catalog | ⚠️ Partial | Hydration is catalog-governed; full Polaris policy enforcement is task-37 (P2) |
| Supply-chain gates (licenses, advisories, SBOM, signing) | ✅ Met | `cargo-deny` in CI; SBOM + cosign in `release.yml` |
| Independent security review | ⏳ Pending | Threat-model summary in [SECURITY.md](https://github.com/GrowlerDB/growlerdb/blob/main/SECURITY.md); external review not yet done |

## Stability & operations

| Criterion | Status | Evidence |
|---|---|---|
| Health/readiness probes + graceful shutdown | ✅ Met | `growlerdb-telemetry`; probes gate Compose/Helm |
| Observability (traces/metrics/logs, SLI dashboards) | ✅ Met | M4; LGTM + Grafana SLIs |
| Resource/DoS guards (page-fetch ceiling, cost guards) | ✅ Met | Gateway limits; segment cost guards |
| Backup/restore + node rebuild-from-Iceberg | ✅ Met | task-32 — shipped + live-verified; recovery is bounded by rebuild time, never data loss |
| Replica sync (segment shipping) | ✅ Met | task-31 — segment-shipping shipped + live-verified (single-shard; windowed / multi-shard replica sets are task-227, post-GA) |

## Performance

| Criterion | Status | Evidence |
|---|---|---|
| Representative benchmark suite + published numbers | ⚠️ Partial | Validated at scale on Hetzner k3s (task-159): empty-start windowed topology with CP-driven placement, **exact source↔index convergence** (Trino distinct == index docs), ingest keep-up to ~19k rows/s, sub-linear windowed top-K, and bounded commit latency under large snapshots. The **published GA benchmark numbers** (staged step-ups + storage milestones + Iceberg/Trino comparison) are task-185/186 — the one perf item before a 1.0 claim |

## Release & docs

| Criterion | Status | Evidence |
|---|---|---|
| SemVer + changelog + deprecation policy | ✅ Met | [RELEASING.md](https://github.com/GrowlerDB/growlerdb/blob/main/RELEASING.md), [CHANGELOG.md](https://github.com/GrowlerDB/growlerdb/blob/main/CHANGELOG.md) |
| Signed, multi-arch artifacts + SBOM; Helm chart published | ✅ Met | `release.yml` verified on v0.1.8 — `imagetools inspect` shows a linux/amd64 + linux/arm64 manifest list with cosign signature + SBOM; both arches run (task-157). Helm chart pushed to GHCR OCI |
| Getting-started + reference + migration docs | ✅ Met | [docs/](index) |

## Summary

The **P1 GA surface** — core search loop, distribution, security/multi-tenancy (incl. verified
tenant isolation), observability, the console, the OpenSearch adapter, the release pipeline, and
**backup/restore + single-shard replica sync** — is **in place, tested, and validated at scale on real
hardware**. The remaining items before a confident **1.0** are, honestly: the **published benchmark
numbers** (task-185/186 — the topology + convergence are validated; the numbers themselves are the
deliverable), **full Polaris data-plane authz** (task-37, P2), and an **independent security review**
(task-166). See the [public roadmap](roadmap) for the post-GA line (windowed replica HA, cold-tier
validation, connector parallelism). This is the go/no-go gate for cutting GA (task-163).
