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

## Open source vs Enterprise

The **engine is open source** under **AGPL-3.0** — indexing, search, hydration, the query language,
distributed sharded/windowed search, the OpenSearch adapter, the console, basic security (OIDC login,
RBAC, verified tenant isolation, TLS), cold-tiering, and backup/restore + single-shard replicas. What's
shipped stays open.

A **commercial license** covers advanced operational and governance capabilities aimed at larger
deployments: **zero-downtime windowed / multi-shard replica HA**, cross-region DR, enterprise identity
(SSO/SAML/SCIM), audit logging, and managed multi-tenancy. A commercial license is also available for
**embedding GrowlerDB in a closed product** (an exception to AGPL's copyleft).

## Known limitations

- **Published benchmark numbers are pending.** The topology, convergence, and latency behaviour are
  validated at scale, but the formal staged benchmark suite (with an Iceberg/Trino baseline) is still
  being produced — treat performance figures as directional until then.
- **Read-HA for windowed / multi-shard indexes is limited.** Read replicas are single-shard today; a
  lost windowed-node's windows are unavailable until it recovers (its data is rebuildable from source).
  Zero-downtime windowed / multi-shard replica sets are part of the commercial offering (see
  [Open source vs Enterprise](#open-source-vs-enterprise)).
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
2. **Cold-tier validation at scale** + per-key hydration routing (drop the current broadcast fan-out).
3. **Ingest throughput** — parallel windowed connectors toward higher sustained rates.
4. **Vector + hybrid search** — embeddings, ANN, filtered KNN, reranking.
5. **More sources** — a second table format (Delta read) and a near-real-time hot tier.
6. **Full Polaris data-plane authz.**

Dates aren't promised; this is direction, not commitment. The [GA criteria](ga-criteria) page tracks
the go/no-go gate for the initial release.
