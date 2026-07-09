---
title: Roadmap & known limitations
layout: default
nav_order: 10
---

# Roadmap & known limitations

GrowlerDB is honest about what it does and doesn't do yet. This page is the public counterpart to the
internal [GA criteria](ga-criteria) — what's shipped, what's known-limited, and what's next.

## What GA delivers

- **Text search over Apache Iceberg** — index → search → hydrate the authoritative rows, with a native
  query language and an [OpenSearch-compatible `_search` adapter](opensearch-adapter).
- **Distributed**: control plane + sharded/windowed nodes + gateway, with scatter-gather + top-K merge.
- **Time-windowed indexes** with event-time query pruning and [cold-tiering](storage-tiering) (aged
  windows served read-through from object storage).
- **Security**: gateway AuthN (OIDC/JWT, API keys, mTLS), control-plane RBAC, verified tenant isolation.
- **Operations**: health/readiness probes, an observability stack (metrics + logs + SLI dashboards),
  backup/restore, single-shard read replicas, and reconciliation that converges the index to the source.
- **Release**: SemVer, signed multi-arch (amd64 + arm64) images + SBOM, and a published Helm chart.

Validated at scale on real hardware (Hetzner k3s): empty-start windowed placement, **exact
source↔index convergence**, ingest keep-up, sub-linear windowed top-K, and bounded commit latency
under large source snapshots.

## Known limitations

- **Published benchmark numbers are pending.** The topology, convergence, and latency behaviour are
  validated at scale, but the formal staged benchmark suite (with an Iceberg/Trino baseline) is still
  being produced — treat performance figures as directional until then.
- **Read-HA for windowed / multi-shard indexes is limited.** Read replicas are single-shard today; a
  lost windowed-node's windows are unavailable until it recovers (its data is rebuildable from source).
  Zero-downtime windowed / multi-shard replica sets are post-GA.
- **Data-plane authz is catalog-delegated.** Hydration is governed by the Iceberg catalog and tenant
  isolation is enforced at the gateway; full Apache Polaris policy enforcement on the data plane is
  post-GA.
- **Vector / hybrid retrieval is not shipped.** Embeddings, ANN/KNN, reranking (the RAG path) are
  designed but deferred.
- **Non-windowed indexes have no cold tier** (disk-capacity bound); cold-tiering applies to windowed
  indexes. See the scale-ceiling notes for the honest map toward very large (100 TB) deployments.

## After GA

Near-term, in rough priority:

1. **Published scale benchmarks** — staged ingest step-ups + storage milestones, GrowlerDB search+hydrate
   vs an Iceberg/Trino table-scan baseline.
2. **Windowed read-HA** — window replicas so a node loss doesn't drop a window from results.
3. **Cold-tier validation at scale** + per-key hydration routing (drop the current broadcast fan-out).
4. **Ingest throughput** — parallel windowed connectors toward higher sustained rates.
5. **Vector + hybrid search** — embeddings, ANN, filtered KNN, reranking.
6. **More sources** — a second table format (Delta read) and a near-real-time hot tier.
7. **Full Polaris data-plane authz.**

Dates aren't promised; this is direction, not commitment. The [GA criteria](ga-criteria) page tracks
the go/no-go gate for the initial release.
