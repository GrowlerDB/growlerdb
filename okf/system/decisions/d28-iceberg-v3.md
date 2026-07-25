---
type: Decision
title: D28. Iceberg v3 adoption path
description: A planned path to adopt Iceberg v3 types (variant per D47, nanosecond timestamps to date).
tags: [decision, adr]
timestamp: 2026-07-24T12:00:00
---

# D28. Iceberg v3 adoption path

**Decision.** A planned path to adopt Iceberg v3 types (variant per
[D47](/system/decisions/d47-variant-mapping.md), nanosecond timestamps to date).

**Status.** Partially delivered. **Variant is shipped** — the original clause here ("variant to
flattened dotted paths") is superseded by [D47](/system/decisions/d47-variant-mapping.md) (untyped
flatten + discriminator-selected shapes), delivered end-to-end via the connector + interim Trino lane
([D48](/system/decisions/d48-variant-delivery.md)/[D49](/system/decisions/d49-variant-iceberg-rust-routing.md)),
with the native Rust read path pending the next iceberg-rust release. **Nanosecond timestamps** remain
planned. Scope note: this decision covers v3 **types** only; v3 **row-lineage**
adoption (locators) is tracked under [D30](/system/decisions/d30-layered-locator.md)'s `row_id`
strategy, gated on ecosystem support (iceberg-rust deletion-vector reads, Spark changelog
`_row_id`, Iceberg ≥1.10.3).
