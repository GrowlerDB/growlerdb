---
type: Test Suite
title: Integration tests
description: Cross-crate and stack-gated tests against real dependencies; stack-dependent paths are marked ignored and factored so pure logic stays testable without a live stack.
tags: [quality]
timestamp: 2026-07-04T14:22:00
---

# Integration tests

Cross-crate and stack-gated tests against real dependencies; stack-dependent paths are marked ignored and factored so pure logic stays testable without a live stack.

## API endpoint coverage

Every gRPC RPC, REST route, OpenSearch route, and MCP tool has at least one functional test
through the service layer in default CI, **except** the paths that require a live Iceberg
source — `CreateIndex`/`DescribeSource` happy paths (validation/error paths run inline) and
`AlterIndex`/`ReindexIndex` successful mutations (their refusal/not-found paths run inline; the
reshard suite drives `ReindexIndex` against stub nodes) — which are exercised against the
Compose stack outside default CI. Auth-denial coverage exists for the admin-gated control REST
routes (users, tokens, index drop, alias swap) and for the Node data plane (service token,
including `Write`).
