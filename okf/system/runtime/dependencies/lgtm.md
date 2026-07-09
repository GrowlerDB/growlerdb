---
type: Dependency
title: LGTM observability stack
description: The telemetry backend — OTLP ingest, dashboards, and alerting.
tags: [dependency, observability, lgtm, grafana]
timestamp: 2026-07-04T14:22:00
---

# LGTM observability stack

The **LGTM** stack — **L**oki (logs), **G**rafana (dashboards), **T**empo (traces),
**M**imir/Prometheus (metrics) — is the telemetry backend. GrowlerDB exports
[OpenTelemetry](/system/observability.md) (OTLP) to it; it hosts the SLI dashboards and evaluates the
alert rules that back the [observability](/product/functional/observability.md) feature.

## Notes

The Compose stack bundles it as a single `grafana/otel-lgtm` image with an OTel collector; production
can point OTLP at any compatible backend. The console's Grafana deep-link is served at runtime on
`/v1/config`.
