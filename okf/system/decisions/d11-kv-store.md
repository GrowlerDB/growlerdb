---
type: Decision
title: D11. Auxiliary KV store: redb
description: Use redb (pure-Rust) for the locator store; RocksDB is a fallback for write-heavy extreme scale.
tags: [decision, adr]
timestamp: 2026-07-04T14:22:00
---

# D11. Auxiliary KV store: redb

**Decision.** Use redb (pure-Rust) for the locator store; RocksDB is a fallback for write-heavy extreme scale.

**Status.** Accepted.
