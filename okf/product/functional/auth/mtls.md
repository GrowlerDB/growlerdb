---
type: Feature
title: mTLS
description: Mutual-TLS transport authentication between clients and the gateway / between components.
tags: [feature, auth, mtls, tls]
timestamp: 2026-07-04T14:22:00
---

# mTLS

Mutual-TLS transport authentication — the client presents a certificate the server verifies (and vice
versa) — as an alternative/complement to bearer-token [login](/product/functional/auth/login.md) for
programmatic callers and inter-component traffic.

## Notes

mTLS authenticates the transport; [RBAC](/product/functional/rbac-and-tenancy.md) still governs what
the authenticated principal may do. Certificate provisioning is a
[deployment](/system/deployment/index.md) concern.

The internal mTLS builders pin the rustls **ring** crypto provider explicitly. Two provider
implementations are compiled into the binary — ring (via tonic) and aws-lc-rs (pulled transitively by
the object-store client's TLS) — so rustls cannot auto-select a process default and would otherwise
panic when the handshake config is built.
