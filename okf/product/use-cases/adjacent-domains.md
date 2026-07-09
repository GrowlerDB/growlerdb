---
type: Use Case
title: Adjacent domains
description: Other domains the same machinery serves — observability, ticketing, catalog, e-discovery.
tags: [use-case, adjacent]
timestamp: 2026-07-04T14:22:00
---

# Adjacent domains

The same machinery serves many text-search-over-Iceberg shapes beyond the three lead use cases. Brief
sketches; each reuses the [functional capabilities](/product/functional/index.md).

| Domain | Shape | Key fit |
|---|---|---|
| Observability / log analytics | app/infra logs in Iceberg; free-text + field search over time windows | append fast path, time pruning, retention economics (like [IoT telemetry](/product/use-cases/iot-telemetry.md) without the multi-tenant fleet framing) |
| Customer-support / ticketing | tickets in Iceberg; agents search by text + status/customer | fast fields for filters, hydrate the full ticket |
| Product / catalog search | catalog in Iceberg; search-as-you-type + facets | a flavor of the [search-backed app](/product/use-cases/search-backed-app.md): facets/sort on fast fields, cached columns render the grid, returns product IDs |
| E-discovery / compliance | documents in Iceberg; legal search + provable retention/erasure | governed hydration, retention/erasure, audit |

These exercise the same [requirements](/product/non-functional/index.md) as the lead use cases.
