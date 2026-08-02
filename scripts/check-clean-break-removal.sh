#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

legacy_paths=(
  crates/adapter-memory
  crates/adapter-neo4j
  crates/adapter-postgres
  crates/adapter-registry
  crates/adapter-rocksdb
  crates/adapter-sidecar
  crates/storage-api
  crates/temporal-ir
  crates/physical-plan
  crates/query-executor
  crates/query-optimizer
  crates/distributed-query
  crates/procedure-runtime
  crates/cypher-engine
  crates/temporal-storage
  crates/raft-logstore
  crates/cluster-protocol
  crates/data-node
  crates/gateway-node
  crates/meta-node
  crates/controller
  crates/dtgproxy
  crates/storage/dtg-storage-neo4j
)

failed=0
for path in "${legacy_paths[@]}"; do
  if [[ -e $path ]]; then
    printf 'legacy path remains: %s\n' "$path" >&2
    failed=1
  fi
done

metadata="$(cargo metadata --locked --format-version 1 --no-deps)"
if jq -e '
  any(.packages[];
    (.name | test("^(adapter-|storage-api$|temporal-ir$|physical-plan$|query-executor$|query-optimizer$|distributed-query$|procedure-runtime$|cypher-engine$|temporal-storage$|raft-logstore$|cluster-protocol$|data-node$|gateway-node$|meta-node$|controller$|dtgproxy$)")))
' <<<"$metadata" >/dev/null; then
  printf 'legacy package remains in cargo metadata\n' >&2
  failed=1
fi

if cargo tree --workspace --edges normal 2>/dev/null | \
    rg -i 'rocksdb|adapter-sidecar|procedure-runtime|storage-api|temporal-ir|cluster-protocol v0'; then
  printf 'legacy dependency remains in workspace tree\n' >&2
  failed=1
fi

if rg -n --glob 'Cargo.toml' --glob '*.rs' --glob '*.proto' \
    'StorageAdapter|TemporalBackendMapping|ProcedureProvider|dtgproxy[.]cluster[.]v1' \
    crates Cargo.toml; then
  printf 'legacy source contract remains\n' >&2
  failed=1
fi

if rg -n -i \
    --glob '!docs/superpowers/specs/**' --glob '!docs/superpowers/plans/**' \
    --glob '!docs/audit/**' --glob '!docs/verification/**' --glob '!target/**' \
    'adapter-rocksdb|adapter-sidecar|procedure-runtime|dtgproxy[.]cluster[.]v1|backend[[:space:]]*=[[:space:]]*"rocksdb"' \
    README.md docs config examples .github 2>/dev/null; then
  printf 'current user-facing surface still names the legacy runtime\n' >&2
  failed=1
fi

exit "$failed"
