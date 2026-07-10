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

A built-in session may also be **scoped to a set of indexes**: the control plane binds an index
allowlist to a subject and stamps it into the minted session token's `indexes` claim, so per-index
RBAC restricts that session to exactly those indexes (see
[RBAC & tenancy](/product/functional/rbac-and-tenancy.md)). Absent = unrestricted across indexes.

## The demo

The Compose demo (`just stack`) runs **authenticated, not open**: the gateway enforces built-in login
and the control plane mints session tokens. A well-known `demo` / `demo` credential — roles
`reader` + `operator`, scoped to the demo indexes (`docs`, `catalog`) — lets the walkthrough show
login and per-index scoping working end to end. It is a demo credential, not a production account.

## Notes

Login is rate-limited and the timing is constant to avoid a username-enumeration oracle. See
[RBAC & tenancy](/product/functional/rbac-and-tenancy.md) for what a session is authorized to do, and
[tokens](/product/functional/auth/tokens.md) for non-interactive access.
