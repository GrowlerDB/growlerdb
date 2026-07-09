# Non-functional

The quality attributes GrowlerDB targets, stated as requirements.

> **These are v1 design targets, not benchmarked results** (except where marked "verified"). They
> define what GrowlerDB aims to deliver; [scalability/benchmarking](/quality/scalability.md) and
> [reliability](/quality/reliability.md) are the methods that validate them.

* [Latency](/product/non-functional/latency.md) - warm query p50 < 20 ms / p99 < 100 ms; hydration on demand
* [Ingest throughput & freshness](/product/non-functional/throughput.md) - >= 250k docs/s; lag < 10-30 s
* [Scale & cost](/product/non-functional/scale.md) - tens of millions+ per index, long retention, object-storage economics
* [Availability](/product/non-functional/availability.md) - 99.9% search; honest partial results during restarts
* [Durability & recovery](/product/non-functional/durability.md) - no data loss; RPO seconds, RTO minutes
* [Consistency](/product/non-functional/consistency.md) - eventually consistent with Iceberg, bounded by lag
* [Multi-tenancy](/product/non-functional/tenancy.md) - strict per-tenant isolation (verified)
* [Security & compliance](/product/non-functional/security-posture.md) - encryption, cached-field policy, isolation
* [Openness](/product/non-functional/openness.md) - Apache-2.0, engine and index store open
