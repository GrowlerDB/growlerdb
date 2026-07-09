---
type: Feature
title: Logout & session revocation
description: End a session; deprovision or role-change invalidates outstanding sessions immediately.
tags: [feature, auth, logout, revocation]
timestamp: 2026-07-04T14:22:00
---

# Logout & session revocation

Ending a session is client-side (drop the token), but GrowlerDB also enforces **server-side
revocation** so a deprovisioned user or a role downgrade takes effect immediately rather than riding
an outstanding JWT to expiry.

## Behavior

- A per-subject **session epoch** (in the control plane) is advanced when the subject's roles change
  or its credential is removed; any session issued before that instant is stale and rejected on its
  next request → the user must re-authenticate with current roles.
- In closed mode, a rejected session re-triggers the [login](/product/functional/auth/login.md) gate.

## Notes

This gives immediate role-downgrade / deprovision without a per-request revocation store.
[API tokens](/product/functional/auth/tokens.md) are revoked explicitly by id.
