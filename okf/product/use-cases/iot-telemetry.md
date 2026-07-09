---
type: Use Case
title: IoT device telemetry text search (lead scenario)
description: High-scale, tenant-scoped text search over device telemetry in Iceberg, with governed evidence retrieval.
tags: [use-case, iot, telemetry, lead]
timestamp: 2026-07-04T14:22:00
---

# IoT device telemetry text search *(lead scenario)*

The highest-scale, most demanding scenario — it drives the
[non-functional envelope](/product/non-functional/index.md).

**Persona.** A reliability/platform engineer (or an MSP operating device fleets for many customer
tenants). See [search analyst](/product/actors/search-analyst.md) +
[platform admin](/product/actors/platform-admin.md).

**Context.** Device telemetry — sensor readings, health/status events, connectivity and firmware
logs, diagnostic messages — lands in Apache Iceberg (often via Kafka/MQTT), partitioned by `tenant_id`
(fleet/customer) and `event_date`, at **very high volume and long retention** (warranty/safety/
compliance often mandate 1 year+). Fields are high-cardinality: `device_id`, `site_id`, `firmware`,
`sensor`, `status`, `error_code`, `device_ip`, plus a free-text `message`.

**The job.** During an incident, search across all of it fast — *"every event from this device /
firmware / error code / site in the last 14 days"* — then pull the **full authoritative events** as
evidence. For an MSP, every query is **strictly tenant-scoped**.

**Scope.** GrowlerDB provides the **text search** over telemetry-shaped data. Anomaly detection,
alerting, dashboards, and remediation are the app layer *above* it — IoT appears here for the *data
type*, not to add device-management features.

**How GrowlerDB is used.**

- Index the telemetry tables; [ingest](/product/functional/ingestion/streaming.md) append-mostly from
  Kafka/Iceberg, exactly-once.
- Composite key `(tenant_id, event_id)`; shard/route by `tenant_id` so a tenant's search hits its own
  shards.
- Query a time-bounded window → `event_date` [window/partition pruning](/product/functional/windowing-time.md)
  skips most of the corpus → ranked coordinates → [hydrate](/product/functional/hydration.md) full
  events.
- [Tenant isolation](/product/functional/rbac-and-tenancy.md): the `tenant_id` predicate is injected
  from token claims, never user-supplied; hydration is catalog-governed.
- Long retention is cheap — the lake holds everything; GrowlerDB indexes searchable fields + a minimal
  cached projection; old windows use [cold tiering](/product/functional/cold-tiering.md).

**Why it fits.** One source of truth, object-storage economics for long retention, partition+time
pruning for fast device/error hunts, first-class fleet multi-tenancy, and governed evidence retrieval.

**Requirements exercised.** High ingest throughput · time/tenant pruning · tenant scoping · full-text
on high-cardinality fields · very large scale & retention · fast warm-window latency · governed
hydration.
