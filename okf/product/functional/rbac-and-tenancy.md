---
type: Feature
title: RBAC & tenant isolation
description: Role-based authorization at the control plane, and verified per-tenant data isolation.
tags: [feature, auth, rbac, tenancy, security]
timestamp: 2026-07-04T14:22:00
---

# RBAC & tenant isolation

What an authenticated principal is **allowed to do**, and how a multi-tenant deployment keeps tenants
apart.

## RBAC

- Roles are bound to subjects in the [control plane](/system/runtime/components/control-plane.md);
  operations (search, admin, ingest) require the appropriate role.
- A [session](/product/functional/auth/login.md) or [token](/product/functional/auth/tokens.md)
  carries the subject's roles; the gateway authorizes each request. Role changes revoke outstanding
  sessions immediately (see [logout](/product/functional/auth/logout.md)).

## Tenant isolation

- A tenant predicate is **injected from the caller's verified token claims**, never user-supplied, and
  ANDed into the query — so a forged header or query-widening cannot cross tenants, and an
  unauthenticated request is rejected before it reaches a shard.
- Hydration is catalog-governed, so authoritative-row retrieval respects the same boundary.
- This is verified end-to-end (a standing isolation test).

## Notes

Full data-plane authorization delegated to the catalog (Polaris policy) is a further, partial
capability — see [known limitations](/quality/known-limitations/index.md). Security posture is a
[quality](/quality/security/index.md) concern.
