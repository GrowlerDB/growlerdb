---
type: Dependency
title: MinIO
description: The S3-compatible object store used in dev and self-hosted deployments.
tags: [dependency, object-storage, minio]
timestamp: 2026-07-04T14:22:00
---

# MinIO

MinIO is the **S3-compatible** object store used in the Compose/dev stack and self-hosted deployments —
a drop-in for [S3](/system/runtime/dependencies/object-storage/s3.md). It holds the warehouse bucket
for Iceberg data + index backups.

## Notes

Wired in the [Compose](/system/deployment/index.md) and k8s deps manifests; the catalog vends a
`minio:9000` endpoint (host clients map it in `/etc/hosts`). Production can use any S3-compatible
service.
