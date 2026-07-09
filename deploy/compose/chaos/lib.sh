# shellcheck shell=bash
# shellcheck disable=SC2034  # vars/functions below are consumed by the drills that source this file
# Shared helpers for the Compose chaos drills (task-115). SOURCE this from a drill; don't execute it.
# Requires: docker, curl. (Individual drills may `require jq`.)

# Resolve paths/vars relative to this lib so drills work from any CWD.
CHAOS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$(cd "$CHAOS_DIR/.." && pwd)/docker-compose.yml"
DC=(docker compose -f "$COMPOSE_FILE" --profile stack)
GATEWAY="http://localhost:8081/v1"   # gateway REST — fronts the `docs` index (see docker-compose.yml)

fail() { echo "DRILL FAILED: $*" >&2; exit 1; }

# Assert a CLI tool is present, else fail with a clear message.
require() { command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1"; }

# Container id of a compose service ("" if not running).
cid_of() { "${DC[@]}" ps -q "$1"; }

# Poll a URL until it returns 2xx, or fail after `timeout` seconds.
wait_http() {
  local url="$1" deadline=$(( SECONDS + $2 ))
  until curl -fsS -o /dev/null "$url" 2>/dev/null; do
    (( SECONDS < deadline )) || return 1
    sleep 2
  done
}

# Wait until container `$1`'s StartedAt advances past `$2` and it is running again — proof the runtime
# self-restarted it (the container id is stable across a `restart:`, so an advanced StartedAt on the
# same id is unambiguous). Fail after `$3` seconds.
wait_restart() {
  local cid="$1" prev="$2" deadline=$(( SECONDS + $3 ))
  until [ "$(docker inspect -f '{{.State.StartedAt}}' "$cid" 2>/dev/null)" != "$prev" ] \
     && [ "$(docker inspect -f '{{.State.Running}}' "$cid" 2>/dev/null)" = "true" ]; do
    (( SECONDS < deadline )) || return 1
    sleep 2
  done
}

started_at_of() { docker inspect -f '{{.State.StartedAt}}' "$1"; }
