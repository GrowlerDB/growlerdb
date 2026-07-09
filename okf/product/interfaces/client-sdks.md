---
type: Interface
title: Client SDKs
description: Programmatic clients for the Engine API — Python and Rust.
tags: [interface, sdk, python, rust]
resource: /clients/python
timestamp: 2026-07-04T14:22:00
---

# Client SDKs

Language clients over the [gRPC](/product/interfaces/grpc.md)/[REST](/product/interfaces/rest.md)
Engine API, for programmatic search, hydration, and index administration.

- **Python SDK** — `clients/python/growlerdb`: search / suggest / hydrate / admin, with bearer/API-token
  auth and bounded deadlines/retries.
- **Rust client** — `crates/growlerdb-client`: the in-tree client the CLI and tests use; a
  reconnecting, deadline-bounded gRPC client.

## Notes

The read clients carry deadlines, retries, and message-size limits so a slow or wedged node fails
loudly rather than hanging. Auth: a bearer token (JWT/session) or an API token.
