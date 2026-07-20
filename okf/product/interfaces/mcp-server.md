---
type: Interface
title: MCP retrieval server
description: A read-only Model Context Protocol server that exposes GrowlerDB's governed retrieval to AI agents, scoped by the caller's token (RBAC + tenant).
tags: [interface, mcp, agents, retrieval, rag]
timestamp: 2026-07-20T00:00:00
---

# MCP retrieval server

`growlerdb mcp` runs a **[Model Context Protocol](https://modelcontextprotocol.io) server** so an AI
agent (Claude or any MCP client) can use GrowlerDB as a **governed retrieval tool**. It is the
agent-native face of [retrieval-first](/system/decisions/d42-retrieval-first.md): the agent retrieves
**coordinates** and hydrates authoritative Iceberg rows; generation stays with the agent.

## Shape

- **Read-only.** It exposes retrieval, not ingest or admin — those stay on the native
  [REST](/product/interfaces/rest.md) / [gRPC](/product/interfaces/grpc.md) API.
- **Two transports, one protocol core** (the same JSON-RPC 2.0 dispatch and tool set):
  - **Streamable HTTP at `POST /mcp`**, served by every REST front (gateway, bare node, replica,
    windowed) same-origin with the console — the remote-agent path: a URL + bearer token connects
    Claude web/desktop connectors, hosted agent platforms, or CI with no local binary.
    **Sessionless** (no `Mcp-Session-Id`; scales horizontally), **POST-only** (no server-initiated
    messages, so `GET /mcp` is 405 and responses are plain JSON, not SSE), no JSON-RPC batching
    (spec 2025-06-18), `Origin` validation against DNS rebinding, and on a closed deployment a
    missing/invalid bearer answers `401` + `WWW-Authenticate: Bearer` before any protocol work.
  - **stdio** (`growlerdb mcp`, newline-delimited), the local-agent path (e.g. Claude Desktop
    against a remote gateway) — a thin adapter calling the gateway's REST surface over HTTP.
- **Fronts the one query surface.** Both transports **synthesize no identity** — they forward the
  caller's `Authorization: Bearer <token>` verbatim (the HTTP transport re-enters the gateway's own
  `/v1` router in-process), so authn, RBAC, per-index scope, the non-widenable
  [tenant filter](/product/functional/rbac-and-tenancy.md), and admission control are enforced by
  the same path as every other query — **an agent physically cannot retrieve another tenant's
  data**. (Tenancy stays opt-in; a single-tenant deployment scopes by RBAC only.)

## Tools

- **`search`** — lexical, semantic, or **hybrid** ([RRF](/product/functional/search/vector.md)) retrieval;
  returns ranked **coordinates** + scores + cached fields. With `hydrate: true` it also returns each
  hit's **authoritative row** in the same call (the engine's
  [inline hydration](/product/functional/hydration.md) — one tool call instead of search-then-hydrate).
  **Context shaping:** `highlight` returns matched snippets instead of whole fields (lexical), and
  `max_chars` is a response budget — lowest-ranked hits are dropped until the payload fits, flagged
  via `truncated_hits` (never a silent cut).
- **`hydrate`** — resolves coordinates to authoritative, governed rows from Iceberg
  ([hydration](/product/functional/hydration.md)).
- **`more_like_this`** — nearest neighbors of one document: hydrates the seed's text (governed),
  embeds it over a `vector_field`, excludes the seed — "find more rows like this one".
- **`aggregate`** — value counts / facets to narrow a search.
- **`list_indexes`** / **`describe_index`** — what's available and its shape. `describe_index` is
  the **self-teaching** entry point: it returns every mapped field with its type + capability flags
  (`indexed`/`fast`/`cached` — the engine's describe now carries the full mapping), the vector
  fields, ready-made `example_queries` composed from that actual schema, and usage guidance.

## Resources

- **`growlerdb://query-syntax`** — a condensed Lucene/KQL grammar + field-capability reference
  (markdown), so an agent writes valid queries without discovering the grammar through 400s.

The tool descriptions are written for an agent to read: retrieve coordinates, then hydrate them for
the authoritative answer — and index authors should mark the fields agents read as `cached`, so
`search` alone answers. **Failed tool calls teach the recovery move**: a 400 points at
`describe_index` + the syntax resource, a 404 at `list_indexes`, a 401/403 at the token's
index/tenant scope.

## Notes

Open-core ([D41](/system/decisions/d41-vector-open-core.md)): the basic MCP server ships in the AGPL
engine; enterprise identity/audit around it is a commercial concern. The semantic/hybrid tools need a
VECTOR-indexed table; the [compose demo](/system/deployment/compose.md) seeds one and its
**`just mcp-connect`** prints paste-ready HTTP connect snippets (plus the repo's checked-in
`.mcp.json` for Claude Code auto-discovery) — the zero-install agent hookup.
