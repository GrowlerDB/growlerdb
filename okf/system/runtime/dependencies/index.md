# Dependencies

The external systems GrowlerDB runs against.

* [Iceberg catalog](/system/runtime/dependencies/iceberg-catalog/) - the REST catalog governing tables + hydration (Polaris)
* [Object storage](/system/runtime/dependencies/object-storage/) - the durability/economics tier (S3 / MinIO)
* [Streaming](/system/runtime/dependencies/streaming/) - the optional transport (Kafka / Redpanda)
* [Metastore](/system/runtime/dependencies/metastore/) - the catalog's persistent store (Postgres)
* [Query engines](/system/runtime/dependencies/query-engines/) - for the connector + SQL UDFs (Spark / Trino)
* [LGTM observability stack](/system/runtime/dependencies/lgtm.md) - OTLP ingest, dashboards, alerting
