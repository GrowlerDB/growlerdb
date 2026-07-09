---
type: Interface
title: Console UI
description: The web console — a Svelte SPA over the Engine API, served by the engine itself.
tags: [interface, ui, console]
resource: /ui
timestamp: 2026-07-04T14:22:00
---

# Console UI

The **GrowlerDB console** — a Svelte single-page app (in `ui/`) served by the
[gateway](/system/runtime/components/gateway.md) at the same origin as the
[REST API](/product/interfaces/rest.md), of which it is a pure client. Implemented as the
[console-ui component](/system/runtime/components/console-ui.md).

## Screens

- **Search / Explore** — query, facets, sort/paging, highlighted results, hydrate a hit, export.
- **Indexes** — list/create/alter/drop/reindex/compact/backup, aliases, per-index detail.
- **Ingestion** — per-index sync status, lag, streaming charts.
- **Observability** — SLI dashboards (latency, ingest lag, shards, cold-cache), alerts.
- **Settings** — theme/locale/keyboard, identity, roles.
- **Login gate** — shown in closed mode when unauthenticated.

## Notes

Deploy-specific config (e.g. the Grafana link) is served at runtime via `/v1/config`, not baked in —
the SPA is built once and served by every deployment.
