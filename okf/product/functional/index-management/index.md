# Index management

Creating, evolving, and operating indexes over Iceberg tables.

* [Create index](/product/functional/index-management/create.md) - define fields/key/flags/windowing and build
* [Alter index](/product/functional/index-management/alter.md) - in-place changes; guide reindex-requiring ones
* [Drop index](/product/functional/index-management/drop.md) - remove the derived index (source untouched)
* [Reindex](/product/functional/index-management/reindex.md) - rebuild from source, fenced; pairs with alias-swap
* [Compact](/product/functional/index-management/compact.md) - merge segments; health-driven auto-compaction
* [Backup & restore](/product/functional/index-management/backup-restore.md) - durable backup to object storage; rebuildable
* [Aliases & ILM](/product/functional/index-management/aliases-ilm.md) - atomic reindex-and-swap, retention
