---
type: Interface
title: MCP retrieval server
description: A read-only Model Context Protocol server that exposes GrowlerDB's governed retrieval to AI agents, scoped by the caller's token (RBAC + tenant).
tags: [interface, mcp, agents, retrieval, rag]
timestamp: 2026-07-19T00:00:00
---

# MCP retrieval server

`growlerdb mcp` runs a **[Model Context Protocol](https://modelcontextprotocol.io) server** so an AI
agent (Claude or any MCP client) can use GrowlerDB as a **governed retrieval tool**. It is the
agent-native face of [retrieval-first](/system/decisions/d42-retrieval-first.md): the agent retrieves
**coordinates** and hydrates authoritative Iceberg rows; generation stays with the agent.

## Shape

- **Read-only.** It exposes retrieval, not ingest or admin — those stay on the native
  [REST](/product/interfaces/rest.md) / [gRPC](/product/interfaces/grpc.md) API.
- **Transport: stdio** (JSON-RPC 2.0), the common local-agent path (e.g. Claude Desktop). An HTTP/SSE
  transport is a later addition.
- **Fronts the gateway.** The server is a thin adapter that calls the authenticated gateway over HTTP,
  forwarding the caller's `Authorization: Bearer <token>`. It **synthesizes no identity** — RBAC,
  per-index scope, and the non-widenable [tenant filter](/product/functional/rbac-and-tenancy.md) all
  ride the verified token and are enforced by the gateway, so **an agent physically cannot retrieve
  another tenant's data**. (Tenancy stays opt-in; a single-tenant deployment scopes by RBAC only.)

## Tools

- **`search`** — lexical, semantic, or **hybrid** ([RRF](/product/functional/search/vector.md)) retrieval;
  returns ranked **coordinates** + scores + cached fields.
- **`hydrate`** — resolves coordinates to authoritative, governed rows from Iceberg
  ([hydration](/product/functional/hydration.md)).
- **`aggregate`** — value counts / facets to narrow a search.
- **`list_indexes`** / **`describe_index`** — what's available and its shape.

The tool descriptions are written for an agent to read: retrieve coordinates, then hydrate them for the
authoritative answer.

## Notes

Open-core ([D41](/system/decisions/d41-vector-open-core.md)): the basic MCP server ships in the AGPL
engine; enterprise identity/audit around it is a commercial concern. The semantic/hybrid tools need a
VECTOR-indexed table; connecting an agent to a seeded vector demo is covered by the AI/RAG demo.
