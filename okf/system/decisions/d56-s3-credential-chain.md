---
type: Decision
title: 'D56. Engine S3 credential-provider chain — static keys plus IMDS / STS / IRSA'
description: The engine resolves object-store credentials through opendal's default provider chain. GROWLERDB_S3_ACCESS_KEY/SECRET_KEY set ⇒ static keys; empty ⇒ the chain runs (env, profile, EC2/ECS instance profile via IMDS, STS assume-role, EKS IRSA web-identity, SSO). Applies to both S3 sites — the Iceberg read path and the backup/cold-tier path. Replaces the static-keys-only limitation.
tags: [decision, adr, s3, security, deployment]
timestamp: 2026-09-04T00:00:00
---

# D56. Engine S3 credential-provider chain — static keys plus IMDS / STS / IRSA

**Decision.** The engine resolves object-store credentials through a **provider chain**, not static
keys alone. When `GROWLERDB_S3_ACCESS_KEY` and `GROWLERDB_S3_SECRET_KEY` are set, they are used as
static keys (the front of the chain). When they are **empty**, they are omitted and the chain runs:
environment, shared-config profile, **EC2/ECS instance profile (IMDS)**, **STS assume-role**, **EKS
IRSA web-identity**, and SSO. This holds at **both** of the engine's S3 construction sites — the Iceberg
read/hydration path (`growlerdb-source`, via the `RestCatalog` `S3FileIO` props) and the
backup/cold-tier path (`growlerdb-backup::s3_store`). Static keys remain one option, not the only one.

**Why.** Static-keys-only ([the former limitation](/product/functional/configuration)) forced
long-lived access keys into config or Secrets and blocked the standard AWS patterns operators expect —
EC2/ECS instance profiles, and on EKS the IRSA (IAM Roles for Service Accounts) web-identity flow — with
no path for short-lived, rotating STS credentials. The engine already runs on **opendal 0.58**, whose
S3 backend implements this entire chain natively (via `reqsign-aws-v4`) and ignores empty static keys.
So this is configuration plumbing, not new credential machinery: the engine stops force-passing empty
static keys and lets the chain resolve.

**Consequences.** Provider **selection stays GrowlerDB-namespaced** ([D55](/system/decisions/d55-s3-config-contract.md))
— an empty `GROWLERDB_S3_*` selects the chain, with no user-facing `AWS_*`. On Kubernetes the Helm
chart's existing `serviceAccount.annotations` carries the IRSA role ARN
(`eks.amazonaws.com/role-arn`); leave the credential Secret's `s3AccessKey`/`s3SecretKey` empty and the
pod authenticates by its web-identity token. IRSA is the recommended production default. The Spark
connector reaches the same outcome automatically: its `S3FileIO` falls back to the AWS default chain
when `GROWLERDB_S3_*` is unset (the connector-side of [D55](/system/decisions/d55-s3-config-contract.md)).
**Unchanged:** path-style S3 addressing is still forced on (a separate limitation; it works with AWS S3
today), and the local dev default remains static `minioadmin` keys, so Compose and host runs are
unaffected.

**Status.** Accepted. Companion to [D55](/system/decisions/d55-s3-config-contract.md) (the config
contract that selects this chain). Resolves the static-S3-keys-only limitation.
