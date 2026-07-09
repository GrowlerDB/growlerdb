---
type: Requirement
title: Multi-tenancy
description: Strict per-tenant isolation as a first-class requirement.
tags: [nfr, tenancy, isolation]
timestamp: 2026-07-04T14:22:00
---

# Multi-tenancy

GrowlerDB supports **strict multi-tenancy** as a first-class requirement — a tenant's queries reach
only that tenant's data. The tenant predicate is injected from verified token claims (never
user-supplied), routing/sharding can be keyed by tenant, and hydration is catalog-governed.

Realized by the [RBAC & tenant isolation](/product/functional/rbac-and-tenancy.md) capability;
enforcement is **verified end-to-end** (see [security](/quality/security/index.md)).

**Status.** Implemented and verified (a standing isolation test); central to the
[IoT telemetry / MSP](/product/use-cases/iot-telemetry.md) use case.
