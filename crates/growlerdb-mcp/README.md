# growlerdb-mcp

A **read-only** [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server for
GrowlerDB. It lets an AI agent — Claude, or any MCP client — retrieve from GrowlerDB with the same
governance a human gets: it speaks JSON-RPC 2.0 over **stdio** and fronts the GrowlerDB **gateway**
over HTTP, forwarding the caller's bearer token so the gateway's existing RBAC + tenant isolation
apply to every read. The server embeds no engine and exposes no ingest/write/admin surface — an
agent can only search and read, and only what its token is already entitled to (it can never reach
another tenant's data).

## Tools

| Tool | What it does |
| --- | --- |
| `search` | Ranked retrieval over an index. Returns hits as document **coordinates** + scores + cached display fields. Modes: `lexical` (BM25, default), `semantic` (vector KNN — needs `vector_field`), `hybrid` (lexical + vector RRF fusion — needs `vector_field`, the default when one is given). |
| `hydrate` | Resolve search-hit **coordinates** into authoritative, governed rows. The second half of the search→hydrate pattern: `search` finds matches, `hydrate` returns the trustworthy field values. |
| `aggregate` | Term-facet counts (top values per field) over the documents matching an optional query. |
| `list_indexes` | List the indexes the caller can see (name + status). Best-effort — served by the gateway's control-plane REST surface. |
| `describe_index` | Stats + schema hints for one index (doc count, snapshot, time/sort fields). |

The agent-facing pattern is **search → hydrate**: `search` returns coordinates (not authoritative
rows), and the agent pipes a hit's `coordinates` straight into `hydrate` to read governed field
values.

## Run

```sh
growlerdb mcp --gateway-url http://localhost:8081 --token "$GROWLERDB_TOKEN" --index my_index
```

Flags (each also reads an env var):

- `--gateway-url` (`GROWLERDB_GATEWAY_URL`, default `http://127.0.0.1:8081`) — the gateway origin.
- `--token` (`GROWLERDB_TOKEN`) — bearer token forwarded to the gateway (carries tenant + RBAC).
- `--index` — default index for a tool call that omits `index`.
- `--username` / `--password` (`GROWLERDB_USERNAME` / `GROWLERDB_PASSWORD`) — if `--token` is
  absent, the server logs in (`POST /v1/login`) to obtain one.

All logging goes to **stderr**; stdout carries only JSON-RPC — so it's safe to pipe into an MCP
client.

## Get a demo token

Against the local demo stack (`just stack`, gateway at `http://localhost:8081`), log in with the
seeded `demo` / `demo` credentials:

```sh
curl -s http://localhost:8081/v1/login \
  -H 'content-type: application/json' \
  -d '{"username":"demo","password":"demo"}' | jq -r .token
```

Or let the server do it for you: `growlerdb mcp --username demo --password demo`.

## Claude Desktop config

Add to `claude_desktop_config.json` (macOS:
`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "growlerdb": {
      "command": "growlerdb",
      "args": [
        "mcp",
        "--gateway-url", "http://localhost:8081",
        "--index", "my_index"
      ],
      "env": {
        "GROWLERDB_TOKEN": "<your-bearer-token>"
      }
    }
  }
}
```

Use an absolute path for `command` (e.g. `/usr/local/bin/growlerdb`) if the binary isn't on
Claude Desktop's `PATH`.

## Notes / caveats

- **Semantic & hybrid search need a VECTOR-indexed table.** The current demo seeds none (that seed
  is a separate task), so the `semantic`/`hybrid` modes are exercised against a vector index you
  build via the vector-index API (TASK-302). **Lexical `search`, `hydrate`, `aggregate`,
  `list_indexes`, and `describe_index` work against the demo stack out of the box.**
- Transport is stdio only. HTTP/SSE transport is not implemented.
- Read-only by design: there is no ingest, write, or admin tool.
