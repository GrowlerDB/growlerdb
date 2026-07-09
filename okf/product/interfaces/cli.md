---
type: Interface
title: CLI
description: The growlerdb binary — build indexes, run components, and query from the shell.
tags: [interface, cli]
resource: /crates/growlerdb-cli
timestamp: 2026-07-04T14:22:00
---

# CLI

The `growlerdb` binary (`crates/growlerdb-cli`) — the operational and embedded interface. The same
binary builds indexes, runs each cluster component, and can serve a single index standalone (the
embedded single-binary mode).

## Subcommands

- **`index`** — build an index from an Iceberg table (optionally sharded: `--shards N --shard-ordinal K`).
- **`serve`** / windowed serve — serve an index (or a shard/window) over gRPC + REST; `--replica` for
  a read-only replica; `--register` to announce to the control plane.
- **`gateway`** — front nodes as the public API (routing from the control plane, hot-reload).
- **`control-plane`** — run the cluster registry.
- **`search`** — query an index from the shell.
- **`backup`** / **`refresh-replica`** — back up an index / advance a replica from shipped segments.

## Notes

CLI config structs mirror the components in [system/runtime](/system/runtime/components/index.md).
Full flags: [docs/reference.md](/docs/reference.md).
