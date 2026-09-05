---
type: Decision
title: 'D55. GrowlerDB-namespaced config is the sole public S3 contract'
description: The operator-facing surface for object-store credentials is GROWLERDB_S3_* across every GrowlerDB component (engine and Spark connector); AWS SDK env (AWS_*) and Spark s3.* confs are implementation-private. The connector maps GROWLERDB_S3_* to the catalog's Iceberg S3FileIO props itself; unset falls through to the AWS default chain (D56). Third-party demo/bench tools keep their native AWS config.
tags: [decision, adr, config, s3, connector]
timestamp: 2026-09-04T00:00:00
---

# D55. GrowlerDB-namespaced config is the sole public S3 contract

**Decision.** The public, operator-facing way to give a **GrowlerDB component** its object-store
credentials is the `GROWLERDB_S3_*` environment set (`GROWLERDB_S3_ACCESS_KEY`,
`GROWLERDB_S3_SECRET_KEY`, `GROWLERDB_S3_REGION`, `GROWLERDB_S3_ENDPOINT`) — and nothing else. This
holds for **both** GrowlerDB components: the Rust engine (control plane / node / gateway) already read
this namespace, and the **Spark connector** now does too. The connector reads `GROWLERDB_S3_*` and maps
it onto the catalog's Iceberg `S3FileIO` properties itself (`s3.access-key-id`, `s3.secret-access-key`,
`client.region`); the AWS SDK's `AWS_*` names and the `spark.sql.catalog.<name>.s3.*` `--conf` keys are
**implementation-private**, not part of the contract. Third-party tools bundled with the demo/bench
(Apache Polaris, MinIO, the pyiceberg/boto3 corpus + seed generators, the Kafka→Iceberg Spark sink, the
Elasticsearch loader, the query driver) are **not** GrowlerDB components and keep their own native AWS
SDK configuration.

**Why.** Exposing `AWS_*` on the connector welded the public contract to today's implementation (Spark
+ the AWS SDK for Java). An operator had to set the same secret twice under two names, and swapping the
connector's S3 client for a different one would silently break their config. One namespace means one
place to configure object storage, stable across whatever S3 client a component runs on — the engine
and connector translate the GrowlerDB contract into their runtime's native form internally.

**Consequences.** The connector applies the mapped `s3.*`/`client.region` props on the `SparkSession`
builder before the catalog is lazily instantiated. A blank/unset `GROWLERDB_S3_*` var is **omitted**,
so `S3FileIO` falls through to the AWS default credential chain — the fallback that makes instance
profiles, STS, and IRSA work without a user-facing `AWS_*` (that provider chain is
[D56](/system/decisions/d56-s3-credential-chain.md)). The demo Compose connector services and the k8s
connector templates carry `GROWLERDB_S3_*`; the docs stop instructing `AWS_*` for GrowlerDB processes.
External demo/bench tools are untouched — they are third-party AWS-SDK consumers with no GrowlerDB-cred
equivalent, so forcing them into the namespace would only obscure the boundary.

**Status.** Accepted. Companion to [D56](/system/decisions/d56-s3-credential-chain.md) (the engine's
non-static credential-provider chain), which this contract selects: `GROWLERDB_S3_*` set ⇒ static keys;
unset ⇒ the provider chain (IMDS / STS / IRSA).
