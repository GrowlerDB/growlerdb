#!/usr/bin/env bash
# Compute the next release version from the latest `v*` git tag (task-156).
#
# Usage: scripts/next-version.sh <patch|minor|major>
#
# - No prior `vX.Y.Z` tag → prints INITIAL_VERSION (default 0.1.0), the decided GA baseline
#   (see RELEASING.md / ADR D29); the bump argument is ignored for the very first release.
# - Otherwise → increments the requested component of the latest tag, resetting the lower ones
#   (minor → X.(Y+1).0, major → (X+1).0.0).
#
# Prints the bare `X.Y.Z` (no leading `v`) to stdout. Requires the full tag history
# (`git fetch --tags` / `actions/checkout` with `fetch-depth: 0`).
set -euo pipefail

bump="${1:?usage: next-version.sh <patch|minor|major>}"
initial="${INITIAL_VERSION:-0.1.0}"

latest="$(git tag -l 'v*.*.*' --sort=-v:refname | head -n1)"

if [ -z "$latest" ]; then
  printf '%s\n' "$initial"
  exit 0
fi

ver="${latest#v}"
major="${ver%%.*}"
rest="${ver#*.}"
minor="${rest%%.*}"
patch="${rest#*.}"

case "$bump" in
  major) major=$((major + 1)); minor=0; patch=0 ;;
  minor) minor=$((minor + 1)); patch=0 ;;
  patch) patch=$((patch + 1)) ;;
  *) echo "unknown bump: $bump (want patch|minor|major)" >&2; exit 2 ;;
esac

printf '%s.%s.%s\n' "$major" "$minor" "$patch"
