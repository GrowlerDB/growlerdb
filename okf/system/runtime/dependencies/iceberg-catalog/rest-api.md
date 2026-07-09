---
type: Dependency
title: Iceberg REST Catalog API
description: The catalog protocol GrowlerDB speaks; Polaris is one implementation.
tags: [dependency, catalog, iceberg, rest]
timestamp: 2026-07-04T14:22:00
---

# Iceberg REST Catalog API

The **Iceberg REST Catalog** protocol — the standard interface for resolving namespaces/tables and
their metadata. GrowlerDB targets this API (via iceberg-rust), so any conformant catalog can back it;
[Polaris](/system/runtime/dependencies/iceberg-catalog/polaris.md) is the one used and verified.

## Notes

Reads go through the catalog's storage config (object-store credentials + endpoint). The read path
wraps the object-store operator with retry for transient throttling.
