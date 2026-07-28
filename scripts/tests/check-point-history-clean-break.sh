#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
gate="$repo_root/scripts/check-point-history-clean-break.sh"

(cd "$repo_root" && bash "$gate")

for forbidden in \
  'load_projection_at' \
  'fn reconstruct(' \
  'rewrite_projection' \
  'mod rewrite'
do
  fixture=$(mktemp -d)
  mkdir -p \
    "$fixture/crates/temporal-storage/src" \
    "$fixture/crates/query-executor/src" \
    "$fixture/crates/distributed-query/src"
  printf '%s\n' "$forbidden" >"$fixture/crates/temporal-storage/src/injected.rs"
  if (cd "$fixture" && bash "$gate" >/dev/null 2>&1); then
    echo "clean-break gate accepted forbidden symbol: $forbidden" >&2
    exit 1
  fi
  rm -rf "$fixture"
done
