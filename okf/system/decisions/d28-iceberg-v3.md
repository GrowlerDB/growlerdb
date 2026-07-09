---
type: Decision
title: D28. Iceberg v3 adoption path
description: A planned path to adopt Iceberg v3 types (variant to flattened dotted paths, nanosecond timestamps to date).
tags: [decision, adr]
timestamp: 2026-07-04T14:22:00
---

# D28. Iceberg v3 adoption path

**Decision.** A planned path to adopt Iceberg v3 types (variant to flattened dotted paths, nanosecond timestamps to date).

**Status.** Planned. Scope note: this decision covers v3 **types** only; v3 **row-lineage**
adoption (locators) is tracked under [D30](/system/decisions/d30-layered-locator.md)'s `row_id`
strategy, gated on ecosystem support (iceberg-rust deletion-vector reads, Spark changelog
`_row_id`, Iceberg ≥1.10.3).
