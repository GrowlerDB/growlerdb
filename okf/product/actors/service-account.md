---
type: Actor
title: Service account
description: A non-human principal (API token) for programmatic and automated access.
tags: [actor, service-account, token, automation]
timestamp: 2026-07-04T14:22:00
---

# Service account

A **non-human principal** — an application, pipeline, or automation — authenticating with an
[API token](/product/functional/auth/tokens.md) rather than an interactive session.

## Goals

- Call the Engine API programmatically (search / hydrate / ingest / admin) with a scoped token.
- Run under a bounded [role and tenant scope](/product/functional/rbac-and-tenancy.md) — the token
  carries the identity/claims the gateway enforces.

## Reaches it through

The [client SDKs](/product/interfaces/client-sdks.md),
[gRPC](/product/interfaces/grpc.md)/[REST](/product/interfaces/rest.md) API, and the
[connector](/system/runtime/components/connector.md) (which writes as a service account).
