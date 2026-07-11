---
type: Requirement
title: Security & compliance
description: Encryption in transit/at rest, cached-field policy, tenant isolation, and compliance posture.
tags: [nfr, security, compliance]
timestamp: 2026-07-04T14:22:00
---

# Security & compliance

- **Encryption** in transit ([TLS/mTLS](/product/functional/auth/mtls.md)) and at rest. The
  [control plane](/system/runtime/components/control-plane.md)'s internal RPCs can additionally be
  gated with a shared service credential and served over TLS.
- **Cached-field policy** — sensitive fields are never cached, so they are only ever retrieved through
  governed [hydration](/product/functional/hydration.md).
- **Tenant isolation** enforced (see [tenancy](/product/non-functional/tenancy.md)).
- **Verified identity only** — identity comes solely from the validated bearer token; caller-asserted
  `x-growlerdb-*` headers are never trusted on the public surface, and the control plane refuses to
  serve an authorizing policy without an authenticator (so roles can't be enforced against forgeable
  metadata).
- **Bounded request cost** — the public data plane caps result pages and key-batch size, bounds
  aggregation cardinality, times out long requests, limits the request body, and clamps highlight
  fragments, so a single request can't exhaust node/gateway resources.
- **Compliance** — provable erasure, an audit trail, and configurable retention, largely inherited
  from the lakehouse.

**Status.** AuthN/RBAC/tenant-isolation and supply-chain gates are in place; an independent security
review is pending — see [quality/security](/quality/security/index.md).
