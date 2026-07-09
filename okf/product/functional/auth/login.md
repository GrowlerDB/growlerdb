---
type: Feature
title: Login
description: Authenticate to GrowlerDB — external IdP (OIDC/JWT) or built-in credentials.
tags: [feature, auth, login]
timestamp: 2026-07-04T14:22:00
---

# Login

Authenticate to obtain a session the gateway trusts. Two modes:

- **External IdP (OIDC/JWT).** The gateway validates a bearer JWT (issuer/audience/exp) minted by an
  external identity provider — the default for organizations that already run an IdP.
- **Built-in credentials (no IdP).** For closed-mode deployments without an external IdP: a
  username/password `POST /v1/login` mints a session JWT; credentials are argon2-hashed and stored in
  the [control plane](/system/runtime/components/control-plane.md). Enabled with `--builtin-auth`.

## Modes

`/v1/config` advertises whether auth is **required** (closed mode) and whether **password login** is
available, so the [console](/product/interfaces/ui.md) shows the right login gate. Open mode serves
anonymously; closed mode gates every request.

## Notes

Login is rate-limited and the timing is constant to avoid a username-enumeration oracle. See
[RBAC & tenancy](/product/functional/rbac-and-tenancy.md) for what a session is authorized to do, and
[tokens](/product/functional/auth/tokens.md) for non-interactive access.
