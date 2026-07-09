---
type: Feature
title: User management
description: Manage subjects, their roles, and built-in credentials.
tags: [feature, users, roles, admin]
timestamp: 2026-07-04T14:22:00
---

# User management

[Platform admins](/product/actors/platform-admin.md) manage the principals of a deployment: subjects,
their [role bindings](/product/functional/rbac-and-tenancy.md), built-in
[credentials](/product/functional/auth/login.md), and [API tokens](/product/functional/auth/tokens.md).

## Behavior

- List/assign roles per subject (`/v1/users`, `/v1/roles`); a role change immediately revokes that
  subject's outstanding sessions.
- In built-in-auth deployments, set/remove a subject's password (argon2-hashed); the first closed-mode
  boot seeds an initial admin.
- Create/revoke API tokens.

## Notes

All principal state lives in the [control plane](/system/runtime/components/control-plane.md) registry.
The console surfaces users/roles in Settings.
