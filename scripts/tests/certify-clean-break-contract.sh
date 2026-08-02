#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

test -x scripts/certify-clean-break.sh
scripts/certify-clean-break.sh --contract-only

rg -F "run_gate legacy_removal 'bash scripts/check-clean-break-removal.sh'" \
  scripts/certify-clean-break.sh >/dev/null
rg -F "run_gate official_provider_contracts 'cargo test --locked -p dtg-storage-fjall -p dtg-storage-postgres -p dtg-storage-kuzu'" \
  scripts/certify-clean-break.sh >/dev/null

test -r config/examples/clean-break-cluster/data-3.json
rg -F 'cluster_functionality' scripts/certify-clean-break.sh >/dev/null
rg -F 'provider_migrations_live' scripts/certify-clean-break.sh >/dev/null
rg -F 'bolt_typed_rows_summary' scripts/certify-clean-break.sh >/dev/null
rg -F 'raft_leader_failover_follower_read' scripts/certify-clean-break.sh >/dev/null
rg -F 'temporal_snapshot_isolation' scripts/certify-clean-break.sh >/dev/null
rg -F 'logical_snapshot_restart_continue' scripts/certify-clean-break.sh >/dev/null
rg -F 'snapshot_csr_builtin_async_analytics' scripts/certify-clean-break.sh >/dev/null
rg -F 'six_provider_migrations' scripts/certify-clean-break.sh >/dev/null
rg -F 'heterogeneous_data_node' scripts/certify-clean-break.sh >/dev/null
rg -F 'four_process_functional_probes' scripts/certify-clean-break.sh >/dev/null
rg -F 'evidence_root="$(cd "$evidence_root" && pwd -P)"' \
  scripts/certify-clean-break.sh >/dev/null
if rg -F 'record_success four_process_liveness' scripts/certify-clean-break.sh; then
  printf 'PID-only liveness is not certification evidence\n' >&2
  exit 1
fi
