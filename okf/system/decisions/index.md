# Decisions (ADRs)

Architecture & product decision records — what was chosen and why. One concept per decision.

* [D1. Catalog: Apache Polaris](/system/decisions/d01-catalog.md) - Use Apache Polaris as the Iceberg REST catalog.
* [D2. UI framework: Svelte](/system/decisions/d02-ui-framework.md) - Build the console as a Svelte single-page app.
* [D3. Storage backend: local-first](/system/decisions/d03-storage-backend.md) - A local-only index store to start (Tantivy on NVMe + redb), durably backed up to object storage, for search-engine latency.
* [D4. OpenSearch API compatibility](/system/decisions/d04-opensearch-compat.md) - Offer a thin, optional OpenSearch-compatible query adapter rather than making it the core API; the native structured API stays canonical.
* [D5. Composite, partition-aware document keys](/system/decisions/d05-composite-keys.md) - Key every document by partition fields plus identifier fields, for routing, stable pagination, and hydration.
* [D6. AuthN / AuthZ](/system/decisions/d06-authn-authz.md) - OIDC/JWT at the gateway, control-plane RBAC, data-plane authz delegated to the catalog, and tenant scoping injected from token claims.
* [D7. Observability](/system/decisions/d07-observability.md) - OpenTelemetry traces/metrics/logs, Prometheus/Grafana, structured JSON logs, OTLP to any backend.
* [D8. Deployment](/system/decisions/d08-deployment.md) - Helm for Kubernetes plus Docker Compose; local and CI tests run on Compose.
* [D9. Sync model: changelog-first](/system/decisions/d09-sync-model.md) - Changelog scan by default, append-only opt-in, CDC where available, with a reconciliation backstop.
* [D10. Ingestion runtime: Spark for now](/system/decisions/d10-ingestion-runtime.md) - Use a single JVM engine (Spark Structured Streaming) until iceberg-rust matures, then migrate.
* [D11. Auxiliary KV store: redb](/system/decisions/d11-kv-store.md) - Use redb (pure-Rust) for the locator store; RocksDB is a fallback for write-heavy extreme scale.
* [D12. Sharding scheme: hash by default](/system/decisions/d12-sharding.md) - Hash on the key by default; range routing is opt-in and complements partition routing.
* [D13. Locator vs PK-clustering](/system/decisions/d13-locator.md) - Use a locator by default; prefer Iceberg pruning when the source is primary-key-clustered.
* [D14. Replica sync: segment shipping](/system/decisions/d14-replica-sync.md) - Replicas pull sealed segments shipped from the primary.
* [D15. Near-real-time hot tier](/system/decisions/d15-hot-tier.md) - Deferred; the searcher is built to merge multiple segment sources so a hot tier can slot in later.
* [D16. Query language: layered](/system/decisions/d16-query-language.md) - A native structured AST (canonical) plus a Lucene/KQL string parser plus SQL UDFs.
* [D17. Multi-table / joined search](/system/decisions/d17-joined-search.md) - Single-source core; joins are pushed to Trino/Spark, with an optional score-merge fan-out.
* [D18. Time-travel search](/system/decisions/d18-time-travel.md) - On-demand rebuild-from-snapshot, not continuous point-in-time.
* [D19. ANN library: Tantivy native KNN](/system/decisions/d19-ann-library.md) - Use Tantivy native KNN behind a trait for the deferred vector capability.
* [D20. Default embedding model](/system/decisions/d20-embedding-model.md) - Configurable, provider-agnostic BGE-family embedding models for the deferred vector capability.
* [D21. Reranker](/system/decisions/d21-reranker.md) - A pluggable reranker hook, off by default, for the deferred vector capability.
* [D22. Search core: Tantivy](/system/decisions/d22-search-core.md) - Commit to Tantivy as the search core; Lucene stays contingent and demand-driven.
* [D23. Cached-field policy](/system/decisions/d23-cached-field-policy.md) - Minimal-explicit caching; large text is always hydrated; catalog-sensitive fields are hard-blocked from cache; bounded staleness is accepted.
* [D24. Product scope: pure text search](/system/decisions/d24-product-scope.md) - A pure text search engine over Iceberg; non-goals are detection/alerting, analytics/OLAP, and being a datastore.
* [D25. API & format stability](/system/decisions/d25-api-stability.md) - SemVer with a versioned Engine API, wire protocol, and on-disk format, each with deprecation windows.
* [D26. Telemetry: no phone-home](/system/decisions/d26-telemetry.md) - No phone-home by default; optional anonymous opt-in only; queries and data are never collected.
* [D27. Governance & community](/system/decisions/d27-governance.md) - A pure open-source community project (Apache-2.0 + DCO); a managed-SaaS path is preserved; a foundation is deferred.
* [D28. Iceberg v3 adoption path](/system/decisions/d28-iceberg-v3.md) - A planned path to adopt Iceberg v3 types (variant to flattened dotted paths, nanosecond timestamps to date).
* [D29. Release versioning: tag-derived, auto-incremented](/system/decisions/d29-release-versioning.md) - The git tag is the source of truth; artifacts are stamped from it while the tree stays 0.0.0; auto-increment patch, explicit minor/major, 0.1.0 GA baseline.
* [D30. Layered locator: identity / reference / location](/system/decisions/d30-layered-locator.md) - Key terms + an internal locator-ID fast field + a dense-array location store, with per-index location strategies (coordinates / row_id / predicate); no constraints imposed on the source table.
* [D31. Ingest silent-loss guards](/system/decisions/d31-ingest-loss-guards.md) - Expected-row-count gate, node checkpoint-continuity guard, lockstep checkpoint advance, lineage-ordered head, and retryable admission backpressure so an under-read stalls loudly instead of sealing a permanent gap.
* [D32. Parallel ingest — shard-group connector sets](/system/decisions/d32-parallel-ingest.md) - W independent connector workers, each owning shards s % W == i; enabled by ordered checkpoints + the window-covering guard; the single connector remains for low-scale syncing.
* [D33. Distributed windowed topology — CP-driven placement, streaming-first](/system/decisions/d33-windowed-topology.md) - A windowed index is N interchangeable nodes serving control-plane-assigned time windows (not fixed hash ordinals); nodes start empty and create each window on the first streamed write, resolved through the control plane on first ask.
* [D34. Runner safety for a public repo — hosted PR CI + approval gate](/system/decisions/d34-runner-safety.md) - Untrusted fork-PR code never runs on the self-hosted runners: an org approval gate for all outside collaborators + pull_request CI on disposable GitHub-hosted runners (self-hosted reserved for push/nightly); least-privilege CI tokens.
* [D35. Multi-index routing from one gateway, with per-index RBAC](/system/decisions/d35-multi-index-routing.md) - One gateway fronts many indexes, routing each request to its named index's shard-set resolved lazily from the control plane and hot-reloaded per index; the CP stays a registry not a query router; empty index → default/sole else rejected; authorization sees the resolved index so a token scoped to one index can't read another, and the node-level tenant filter is preserved.
