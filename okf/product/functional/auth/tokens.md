---
type: Feature
title: API tokens
description: Long-lived, revocable tokens for non-interactive (service-account) access.
tags: [feature, auth, tokens, api]
timestamp: 2026-07-04T14:22:00
---

# API tokens

Long-lived bearer tokens for [service accounts](/product/actors/service-account.md) — apps, pipelines,
and the [connector](/system/runtime/components/connector.md) — instead of an interactive session.

## Behavior

- A token carries a subject + roles; the gateway authenticates each request by the token's hash
  (O(1) lookup) and enforces its scope.
- Tokens are **created and revoked** by id via the admin API; an optional **expiry** (TTL) makes a
  token stop authenticating and be pruned so the token store can't grow without bound.
- Only the token hash is stored; the secret is shown once at creation.

## Notes

Managed by [platform admins](/product/actors/platform-admin.md) (`/v1/users` / admin API). See
[RBAC & tenancy](/product/functional/rbac-and-tenancy.md) for scope enforcement.
