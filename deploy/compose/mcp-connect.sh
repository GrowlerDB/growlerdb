#!/usr/bin/env bash
# One-command MCP hookup for the demo stack: wait for the gateway, mint a demo bearer via
# /v1/login, and print paste-ready connect snippets for HTTP-capable MCP clients (Claude Code,
# Claude Desktop, anything generic). No binary, no stdio subprocess — the gateway serves the MCP
# Streamable HTTP transport at POST /mcp on the same port as the console.
# Idempotent — re-run any time (session tokens expire; this just mints a fresh one).
set -euo pipefail

GATEWAY="${GATEWAY:-http://localhost:8081}"
MCP_USER="${MCP_USER:-demo}"
MCP_PASS="${MCP_PASS:-demo}"

echo "waiting for the gateway at $GATEWAY..."
up=""
for _ in $(seq 1 60); do
  # /v1/config is the unauthenticated liveness probe the console itself uses. Parse with sed
  # (no python3/jq dep — runs on any minimal host, same convention as setup-polaris.sh).
  up=$(curl -s "$GATEWAY/v1/config" | sed -n 's/.*"auth_required".*/ok/p') || true
  [ -n "$up" ] && break
  sleep 1
done
[ -n "$up" ] || {
  echo "ERROR: no gateway at $GATEWAY — is the stack up? (just stack)" >&2
  exit 1
}

TOKEN=$(curl -s -X POST "$GATEWAY/v1/login" \
  -H 'content-type: application/json' \
  -d "{\"username\":\"$MCP_USER\",\"password\":\"$MCP_PASS\"}" \
  | sed -n 's/.*"token":"\([^"]*\)".*/\1/p') || true
[ -n "$TOKEN" ] || {
  echo "ERROR: login as '$MCP_USER' failed at $GATEWAY/v1/login" >&2
  exit 1
}

cat <<EOF

Connect an AI agent to GrowlerDB (MCP over HTTP) ─────────────────────────────

The gateway serves MCP at  $GATEWAY/mcp  — governed retrieval over the demo's
Iceberg data, scoped to what '$MCP_USER' may see. Tokens expire; re-run this to
re-mint.

▸ Claude Code — one line (the remove first makes re-runs rotate the token:
  \`claude mcp add\` will NOT overwrite an existing server):

    claude mcp remove growlerdb 2>/dev/null; \\
    claude mcp add --transport http growlerdb $GATEWAY/mcp \\
      --header "Authorization: Bearer $TOKEN"

▸ Claude Code, via the repo's checked-in .mcp.json — export the token and start
  claude in this repo (the server is auto-discovered). Without the export the
  server fails SILENTLY (no growlerdb tools in the session — agents then fall
  back to grepping files); check with /mcp inside the session:

    export GROWLERDB_DEMO_TOKEN=$TOKEN

▸ Any HTTP-capable MCP client — generic config:

    { "type": "http",
      "url": "$GATEWAY/mcp",
      "headers": { "Authorization": "Bearer $TOKEN" } }

▸ Claude Desktop — bridge the header-authenticated endpoint with mcp-remote
  (Desktop's custom connectors don't send custom headers):

    { "mcpServers": { "growlerdb": {
        "command": "npx",
        "args": ["-y", "mcp-remote", "$GATEWAY/mcp",
                 "--header", "Authorization: Bearer $TOKEN"] } } }

Try asking your agent: "What does the catalog say about hydration?" — it will
search (hybrid/semantic/lexical) and answer from governed rows with citations.
──────────────────────────────────────────────────────────────────────────────
EOF
