#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

test -x scripts/local-cluster.sh
rg -F 'start --managed-postgres' scripts/local-cluster.sh >/dev/null
rg -F 'start --postgres-url URL' scripts/local-cluster.sh >/dev/null
rg -F 'stop [--root PATH]' scripts/local-cluster.sh >/dev/null
rg -F 'status [--root PATH]' scripts/local-cluster.sh >/dev/null
rg -F '127.0.0.1' scripts/local-cluster.sh >/dev/null
rg -F 'umask 077' scripts/local-cluster.sh >/dev/null
rg -F 'pg_isready' scripts/local-cluster.sh >/dev/null
rg -F 'postgres_endpoint="host=127.0.0.1 port=$port dbname=dtgproxy sslmode=disable"' \
  scripts/local-cluster.sh >/dev/null
rg -F 'postgres_credential="user=dtgproxy password=$password application_name=dtgproxy-local"' \
  scripts/local-cluster.sh >/dev/null
rg -F 'DTG_DATA_POSTGRES_ENDPOINT="$postgres_endpoint"' scripts/local-cluster.sh >/dev/null
rg -F 'DTG_DATA_POSTGRES_CREDENTIAL="$postgres_credential"' scripts/local-cluster.sh >/dev/null
rg -F 'postgres_credential=""' scripts/local-cluster.sh >/dev/null
rg -F 'DTG_GATEWAY_SHARD_ENDPOINTS=' scripts/local-cluster.sh >/dev/null
rg -F '2:1:12:1:0:postgresql:1:1:local-postgres' scripts/local-cluster.sh >/dev/null
rg -F '3:1:13:1:0:kuzu:1:1:local-kuzu' scripts/local-cluster.sh >/dev/null
rg -F 'stop_managed_postgres' scripts/local-cluster.sh >/dev/null
! rg -F 'rm -f "$(runtime_path postgres/managed)"' scripts/local-cluster.sh >/dev/null
rg -F 'remove_runtime_root' scripts/local-cluster.sh >/dev/null
rg -F 'stop_process' scripts/local-cluster.sh >/dev/null
rg -F 'terminate_process_tree' scripts/local-cluster.sh >/dev/null
rg -F 'active_process_id' scripts/local-cluster.sh >/dev/null
rg -F 'cleanup_and_exit' scripts/local-cluster.sh >/dev/null

rg -F 'cargo test --locked --jobs 1 -p dtg-gateway --test live_provider_migrations' \
  scripts/test-provider-migrations-live.sh >/dev/null
rg -F 'terminate_process_tree' scripts/test-provider-migrations-live.sh >/dev/null
migration_trap_line="$(rg -n '^trap cleanup EXIT$' scripts/test-provider-migrations-live.sh | cut -d: -f1)"
migration_external_url_line="$(rg -n '^if \[\[ -n "\$\{DTG_POSTGRES_URL:-\}" \]\]; then$' scripts/test-provider-migrations-live.sh | cut -d: -f1)"
[[ $migration_trap_line -lt $migration_external_url_line ]]

runtime_root="target/local-cluster-contract-$$"
mkdir -p "$runtime_root/pids"
: >"$runtime_root/owned"
scripts/local-cluster.sh stop --root "$runtime_root"
test ! -e "$runtime_root"

printf '%s\n' 'local-cluster contract tests passed'
