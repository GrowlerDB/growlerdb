---
type: Component
title: CLI / engine binary
description: The single growlerdb binary that runs every role and the embedded single-node mode.
tags: [component, cli, binary, embedded]
resource: /crates/growlerdb-cli
timestamp: 2026-07-04T14:22:00
---

# CLI / engine binary

The `growlerdb` executable (`crates/growlerdb-cli`) — one binary that **is** every runtime role: it
builds indexes, and runs as a [node](/system/runtime/components/node.md),
[gateway](/system/runtime/components/gateway.md), or
[control plane](/system/runtime/components/control-plane.md) depending on subcommand. It can also serve
a single index standalone (the **embedded single-binary** deployment).

## Notes

The same binary underlies the container image; deployment picks the role via args. Subcommands are the
[CLI interface](/product/interfaces/cli.md).
