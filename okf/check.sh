#!/usr/bin/env bash
# OKF conformance check: every non-reserved concept .md must carry a non-empty `type`
# frontmatter field. The reserved index.md files (curated directory listings) intentionally carry none. Run from anywhere.
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1

fail=0
while IFS= read -r f; do
  case "${f##*/}" in
    index.md) continue ;;
  esac
  if ! grep -qE '^type:[[:space:]]*[^[:space:]]' "$f"; then
    echo "OKF: concept missing a non-empty 'type': $f" >&2
    fail=1
  fi
done < <(find okf -type f -name '*.md')

if [ "$fail" -eq 0 ]; then
  echo "OKF conformance: OK ($(find okf -type f -name '*.md' | wc -l | tr -d ' ') files)"
fi
exit "$fail"
