---
type: Dependency
title: Kafka / Redpanda
description: The optional streaming transport for the ingestion pipeline.
tags: [dependency, streaming, kafka, redpanda]
timestamp: 2026-07-04T14:22:00
---

# Kafka / Redpanda

A Kafka-compatible broker is the **optional streaming transport** for the ingestion pipeline — events
flow through the broker to a sink that lands them in Iceberg, which the
[connector](/system/runtime/components/connector.md) then indexes. **Redpanda** (a single-binary
Kafka-compatible broker) is used in the demo pipeline.

## Notes

Not required for indexing an existing Iceberg table — only for the end-to-end streaming demo/topology
(generator → broker → sink → Iceberg → connector → index). Wired in the Compose `pipeline` profile.
