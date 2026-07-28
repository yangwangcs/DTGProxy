#!/usr/bin/env bash
set -euo pipefail

if rg -n 'load_projection_at|fn reconstruct\(|rewrite_projection|mod rewrite' \
  crates/temporal-storage/src crates/query-executor/src crates/distributed-query/src; then
  echo 'legacy point-history path remains' >&2
  exit 1
fi
