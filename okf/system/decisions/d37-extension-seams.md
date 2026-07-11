---
type: Decision
title: 'D37. Extension seams for out-of-tree capabilities'
description: The core exposes pluggable behaviour as stable public trait seams; out-of-tree implementations attach without forking, and the default build stays 100% AGPL.
tags: [decision, adr]
timestamp: 2026-07-11T00:00:00
---

# D37. Extension seams for out-of-tree capabilities

**Decision.** GrowlerDB's core exposes its pluggable behaviour as **stable public trait seams**, so
capabilities can attach **out-of-tree without forking**. Today the seams are the identity/authorization
traits in `growlerdb-engine` — `Authenticator`/`SharedAuthn` and `AuthHook`/`SharedAuth` — which the
gateway builder injects (`with_authn`) and `ChainAuthenticator` composes. Audit, storage-tier/backup,
connector, and admin seams are added to the core **alongside the features that use them** (no
speculative interfaces).

Out-of-tree implementations (including commercial ones) live in a **separate crate** that depends on the
public crates and injects through these seams. The **public repository holds no such code; its default
build is 100% AGPL-3.0** ([D36](/system/decisions/d36-license-agplv3.md)). Combining the core with
non-AGPL code is done by the copyright holder under its own commercial license — a right retained via
the project's [contributor agreement](/system/decisions/d27-governance.md).

**Status.** Accepted.
