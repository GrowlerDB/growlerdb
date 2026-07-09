#!/usr/bin/env bash
# OKF conformance check (task-181): every non-reserved concept .md must carry a non-empty `type`
# frontmatter field. Reserved files (index.md, log.md) intentionally carry none. Run from anywhere.
set -euo pipefail
cd "$(dirname "$0")/.." || exit 1

fail=0
while IFS= read -r f; do
  case "${f##*/}" in
    index.md | log.md | pre-ga-log.md) continue ;;
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
