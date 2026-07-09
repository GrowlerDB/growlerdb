---
type: Requirement
title: Security & compliance
description: Encryption in transit/at rest, cached-field policy, tenant isolation, and compliance posture.
tags: [nfr, security, compliance]
timestamp: 2026-07-04T14:22:00
---

# Security & compliance

- **Encryption** in transit ([TLS/mTLS](/product/functional/auth/mtls.md)) and at rest.
- **Cached-field policy** — sensitive fields are never cached, so they are only ever retrieved through
  governed [hydration](/product/functional/hydration.md).
- **Tenant isolation** enforced (see [tenancy](/product/non-functional/tenancy.md)).
- **Compliance** — provable erasure, an audit trail, and configurable retention, largely inherited
  from the lakehouse.

**Status.** AuthN/RBAC/tenant-isolation and supply-chain gates are in place; an independent security
review is pending — see [quality/security](/quality/security/index.md).
