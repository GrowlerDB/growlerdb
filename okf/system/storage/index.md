# Storage

How GrowlerDB stores the derived index and what it reads from Iceberg.

* [Index store](/system/storage/index-store.md) - Tantivy segments on NVMe + a redb locator store
* [Data model](/system/storage/data-model.md) - composite keys, field types, cached/fast fields
* [Locators & segments](/system/storage/locators-segments.md) - immutable segments + crash-consistent locators
* [Cold bundles](/system/storage/cold-bundles.md) - the split-bundle format for read-through cold windows
* [Backup format](/system/storage/backup-format.md) - what a backup contains; restore + replica shipping
* [Catalog metadata](/system/storage/catalog-metadata.md) - table UUID, snapshots, schema read from Iceberg
