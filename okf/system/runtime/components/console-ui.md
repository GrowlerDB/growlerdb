---
type: Component
title: Console UI (runtime)
description: The Svelte SPA served by the gateway as a static build.
tags: [component, ui, console]
resource: /ui
timestamp: 2026-07-04T14:22:00
---

# Console UI (runtime)

The [console](/product/interfaces/ui.md) as a deployed component: a Svelte SPA built to static assets
(`ui/`) and served by the [gateway](/system/runtime/components/gateway.md) from its `--ui-dir` at the
API origin. A pure client of the [REST API](/product/interfaces/rest.md).

## Notes

Built once, served by every deployment; deploy-specific config (e.g. the Grafana link) is fetched at
runtime from `/v1/config`, never baked in. Build/lint is part of the [CI](/quality/ci-and-gates.md)
`ui` job.
