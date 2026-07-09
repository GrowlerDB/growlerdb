---
type: Concept
title: Single-binary (embedded)
description: One growlerdb binary serving a single index standalone.
tags: [deployment, embedded, single-binary]
timestamp: 2026-07-04T14:22:00
---

# Single-binary (embedded)

The simplest deployment: the [`growlerdb` binary](/system/runtime/components/cli-engine.md) builds and
serves a single index standalone (gRPC + REST), with no separate control plane or gateway. Good for
small/embedded use, local development, and demos.

## Notes

Scales up to the [sharded HA](/system/deployment/sharded-ha.md) topology by adding a control plane and
gateway; the same binary underlies both.
