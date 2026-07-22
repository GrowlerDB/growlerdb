---
title: Connecting your own Iceberg table
layout: default
nav_order: 5
---

# Connecting your own Iceberg table
{: .no_toc }

The [getting-started demo](getting-started) runs GrowlerDB against a bundled MinIO + Polaris and a
seeded table. This page is the day-2 step: running GrowlerDB with Docker Compose against your own
external Iceberg table, on real AWS S3 (or any S3-compatible store) with a REST catalog you already
operate.

1. TOC
{:toc}

---

## The two moving parts

GrowlerDB reaches your lakehouse through two independent surfaces, and both must point at the same
catalog, bucket, and table:

| Surface | What it does | How it's configured |
|---|---|---|
| **Query / hydration** (control plane · node · gateway) | Builds + serves the index; reads Iceberg to hydrate matched keys back to rows | `GROWLERDB_*` environment variables |
| **Ingestion** (Spark connector) | Streams the Iceberg changelog into the index | `spark-submit --conf spark.sql.catalog.*` + `AWS_*` env |

The two authenticate to S3 differently: the engine uses `GROWLERDB_S3_ACCESS_KEY`/`SECRET_KEY`, and
the connector uses the AWS SDK's `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`. Set both.

## Before you start

You need an Iceberg table already registered in a REST catalog (Apache Polaris, Nessie, or any
Iceberg REST catalog), stored on S3-compatible object storage, with both reachable from where you run
Compose. See the [limitations](#limitations) below. In particular, the catalog must speak the Iceberg
REST protocol (AWS Glue and Hadoop catalogs are not supported by the engine).

## Part 1: run the query side against your table

An `external.yml` Compose file ships alongside the demo. Unlike `docker-compose.yml`, it bundles no
MinIO, Polaris, or seed data. It runs only GrowlerDB (control plane + node + gateway) off the
published image, with every connection setting coming from a `.env` file.

```sh
cd deploy/compose
cp .env.external.example .env
# edit .env: catalog URI, warehouse, S3 endpoint/keys/region, your table + index name
docker compose -f external.yml up
```

`.env` maps directly to the [configuration env vars](configuration#environment):

```sh
GROWLERDB_CATALOG_URI=https://your-catalog.example.com/api/catalog
GROWLERDB_WAREHOUSE=your_warehouse            # for Polaris, the catalog name
GROWLERDB_CATALOG_CREDENTIAL=client_id:client_secret   # empty if the catalog needs no auth
GROWLERDB_CATALOG_SCOPE=PRINCIPAL_ROLE:ALL             # Polaris; empty otherwise
GROWLERDB_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
GROWLERDB_S3_ACCESS_KEY=AKIA...
GROWLERDB_S3_SECRET_KEY=...
GROWLERDB_S3_REGION=us-east-1
GROWLERDB_SOURCE_TABLE=your_namespace.your_table
GROWLERDB_INDEX_NAME=your_index
```

On first boot the node builds an index from your table's current snapshot, auto-mapping every
column, and serves it. The gateway comes up on <http://localhost:8081> with the console. Log in
with the `GROWLERDB_LOGIN_USER` / `GROWLERDB_LOGIN_PASSWORD` you set.

To control the field mapping (types, the key, `tenant_field`, timestamps) instead of auto-mapping,
write an [index-definition YAML](configuration#the-index-definition), mount it into the `node` service,
and add `--def /index.yaml` to its command.

## Part 2: ingest ongoing changes with the connector

Part 1 indexes a snapshot. To keep the index current as your table changes, run the Spark
connector, a `spark-submit` job that streams the Iceberg changelog into the node. Build the fat jar
first (`cd connector && mise exec -- mvn -q -DskipTests package` → `target/growlerdb-connector-<version>.jar`),
then submit it pointed at your table:

```sh
spark-submit \
  --master 'local[2]' \
  --packages org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.0,org.apache.iceberg:iceberg-aws-bundle:1.10.0 \
  --conf spark.sql.catalog.mycat=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.mycat.type=rest \
  --conf spark.sql.catalog.mycat.cache-enabled=false \
  --conf spark.sql.catalog.mycat.uri=https://your-catalog.example.com/api/catalog \
  --conf spark.sql.catalog.mycat.warehouse=your_warehouse \
  --conf spark.sql.catalog.mycat.credential=client_id:client_secret \
  --conf spark.sql.catalog.mycat.scope=PRINCIPAL_ROLE:ALL \
  --conf spark.sql.catalog.mycat.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
  --conf spark.sql.catalog.mycat.s3.endpoint=https://s3.us-east-1.amazonaws.com \
  --conf spark.sql.catalog.mycat.s3.path-style-access=false \
  --class io.growlerdb.connector.ConnectorApp \
  target/growlerdb-connector-<version>.jar \
  --catalog mycat \
  --table your_namespace.your_table \
  --identifier id \
  --fields id,title,body,region,ts \
  --index your_index \
  --node 127.0.0.1:50051 \
  --control-plane 127.0.0.1:50071 \
  --stream
```

Provide the connector's S3 credentials via the AWS SDK env (or an IAM role, if you run it on AWS):

```sh
export AWS_ACCESS_KEY_ID=AKIA... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1
export GROWLERDB_SERVICE_TOKEN=...   # must match the value in your .env (mesh auth)
```

A few `--conf` notes: `type=rest` selects the REST catalog; `cache-enabled=false` is required for
streaming, so each trigger sees new snapshots; `io-impl=…S3FileIO` plus the `s3.*` settings point
Spark at your bucket. For AWS S3 use `s3.path-style-access=false` (virtual-hosted); for MinIO use
`true`. The `ConnectorApp` args (`--identifier`, `--fields`, `--table`, `--index`, `--node`,
`--control-plane`, `--stream`) tell it what to ingest and where. See the
[connector README](https://github.com/GrowlerDB/growlerdb/blob/main/connector/README.md).

## Limitations

These are constraints of the current engine. Plan around them:

- REST catalogs only. The engine builds an Iceberg `RestCatalog`; AWS Glue, Hadoop, and non-REST
  Nessie modes are not supported for the query/hydration side. (The Spark connector can read other
  catalog types, but hydration needs REST, so the end-to-end loop requires a REST catalog.)
- Static S3 keys only. The engine authenticates to S3 with a static access key plus secret key. There
  is no IAM instance-role, STS, or assume-role support on the engine side, so supply
  `GROWLERDB_S3_ACCESS_KEY`/`SECRET_KEY`.
- Path-style S3 access is forced on by the engine. It is required for MinIO and still works with
  AWS S3 today; strict virtual-hosted-only setups aren't supported.
- Rotate the secrets. `GROWLERDB_SERVICE_TOKEN` (mesh auth) and `GROWLERDB_AUTH_SECRET` (gateway
  login) default to placeholders in the template, so set real values.

## Troubleshooting

- Catalog `401`/`403`: check `GROWLERDB_CATALOG_CREDENTIAL` (`id:secret`) and, for Polaris,
  `GROWLERDB_CATALOG_SCOPE=PRINCIPAL_ROLE:ALL`. Both the node/gateway env and the connector `--conf`
  must carry them.
- S3 access denied / no such bucket: the engine and the connector authenticate separately, so verify
  both `GROWLERDB_S3_*` (engine) and `AWS_*` (connector), the endpoint, and the region.
- `table not found`: the node's `GROWLERDB_SOURCE_TABLE` and the connector's `--table` must be the
  same `namespace.table`, present in the warehouse you named.
- Connector commits nothing on a live table: confirm `spark.sql.catalog.<name>.cache-enabled=false`.
- Hydration errors from the node: the node must be able to reach the S3 endpoint by the same name
  it's configured with; check DNS/network from inside the container.

## Going to production

Compose is for local runs and experiments. For a production deployment, use the Helm chart, which is
built for external catalogs and object stores and takes credentials from a Kubernetes `Secret`. See the
[Helm README](https://github.com/GrowlerDB/growlerdb/blob/main/deploy/helm/growlerdb/README.md) and
[Deployment](deployment).
