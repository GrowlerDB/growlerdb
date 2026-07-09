---
title: Reference
layout: default
nav_order: 5
has_children: true
---

# Reference

The GrowlerDB API and query surface:

- **[Query language](query-language)** — the Lucene/KQL string syntax and the field types it
  operates on.
- **[REST & gRPC API](rest-api)** — the Engine API endpoints the gateway serves.
- **[OpenSearch adapter](opensearch-adapter)** — the optional, read-path `_search` compatibility
  layer.

The console UI is a pure client of this same API — anything the UI does, a programmatic caller can
do over REST/gRPC.
