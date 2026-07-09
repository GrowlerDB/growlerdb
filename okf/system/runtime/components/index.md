# Components

The running processes that make up GrowlerDB.

* [Control plane](/system/runtime/components/control-plane.md) - the cluster registry and routing source of truth
* [Gateway](/system/runtime/components/gateway.md) - stateless public API; routing + scatter-gather
* [Node](/system/runtime/components/node.md) - builds and serves an index (or shard/window)
* [Connector](/system/runtime/components/connector.md) - Spark worker streaming the changelog into a node
* [Compactor / maintenance](/system/runtime/components/compactor-maintenance.md) - auto-compaction + Iceberg orphan reclaim
* [Console UI (runtime)](/system/runtime/components/console-ui.md) - the Svelte SPA served by the gateway
* [CLI / engine binary](/system/runtime/components/cli-engine.md) - the single binary that runs every role
