---
type: Decision
title: D24. Product scope: pure text search
description: A pure text search engine over Iceberg; non-goals are detection/alerting, analytics/OLAP, and being a datastore. Superseded by D44 (full-text, vector & hybrid retrieval over your data).
tags: [decision, adr]
timestamp: 2026-07-04T14:22:00
---

# D24. Product scope: pure text search

**Decision.** A pure text search engine over Iceberg; non-goals are detection/alerting, analytics/OLAP, and being a datastore.

**Status.** **Superseded by [D44](/system/decisions/d44-product-scope-retrieval.md)** — the product is
now full-text, vector, and hybrid retrieval over your data (not text-only), and the derived-index
thesis is source-agnostic (not Iceberg-only). The non-goals above (not a system of record / datastore,
not analytics/OLAP, not detection/alerting) are **retained** by D44.
