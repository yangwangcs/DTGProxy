#!/usr/bin/env bash
set -euo pipefail

readonly SCHEMA_VERSION=1
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fixture_root="$repo_root/config/examples/clean-break-cluster"
mode=""
evidence_root="${DTG_CLEAN_BREAK_EVIDENCE_ROOT:-$repo_root/target/clean-break-certification}"

usage() {
  printf 'usage: %s --contract-only | --local | --live\n' "$0" >&2
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'missing required tool: %s\n' "$1" >&2
    exit 1
  }
}

digest_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

validate_contract() {
  require_tool cargo
  require_tool jq
  for file in meta-1.json controller-1.json gateway-1.json data-1.json data-2.json data-3.json; do
    test -r "$fixture_root/$file"
    jq -e '.version == 1 and .cluster_id == 9001' "$fixture_root/$file" >/dev/null
  done
  jq -e '.security.mode == "loopback_plaintext" and (.listen_addr | startswith("127.0.0.1:"))' \
    "$fixture_root/meta-1.json" "$fixture_root/controller-1.json" >/dev/null
  jq -e '.provider_class == "fjall"' "$fixture_root/data-1.json" >/dev/null
  jq -e '.provider_class == "postgresql"' "$fixture_root/data-2.json" >/dev/null
  jq -e '.provider_class == "kuzu"' "$fixture_root/data-3.json" >/dev/null
  jq -e '.cluster_endpoint | startswith("http://127.0.0.1:")' \
    "$fixture_root/gateway-1.json" >/dev/null
  test -f "$repo_root/crates/processes/dtg-gateway/tests/four_process_cluster.rs"
  test -f "$repo_root/crates/processes/dtg-gateway/tests/live_certification.rs"
  test -f "$repo_root/crates/processes/dtg-gateway/tests/runtime_certification.rs"
  test -f "$repo_root/crates/processes/dtg-gateway/tests/live_provider_migrations.rs"
  test -x "$repo_root/scripts/test-provider-migrations-live.sh"
  test -f "$repo_root/crates/processes/dtg-meta/src/main.rs"
  test -f "$repo_root/crates/processes/dtg-controller/src/main.rs"
  test -f "$repo_root/crates/processes/dtg-data/src/main.rs"
  test -f "$repo_root/crates/processes/dtg-gateway/src/main.rs"
  if rg -n -i 'rocksdb|adapter-sidecar|procedure-runtime|dtgproxy[.]cluster[.]v1' \
      "$fixture_root" "$repo_root/crates/processes/dtg-gateway/tests/four_process_cluster.rs" \
      "$repo_root/crates/processes/dtg-gateway/tests/live_certification.rs" \
      "$repo_root/crates/processes/dtg-gateway/tests/runtime_certification.rs"; then
    return 1
  fi
}

run_gate() {
  local name="$1"
  local command="$2"
  local log="$evidence_dir/$name.log"
  local started finished status digest
  started="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  set +e
  (cd "$repo_root" && bash -lc "$command") >"$log" 2>&1
  status=$?
  set -e
  finished="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  digest="$(digest_file "$log")"
  jq -cn \
    --arg name "$name" --arg command "$command" --arg started "$started" \
    --arg finished "$finished" --arg digest "$digest" --argjson status "$status" \
    '{name:$name,command:$command,started_at:$started,finished_at:$finished,exit_status:$status,output_digest:$digest}' \
    >>"$gates_file"
  if [[ $status -ne 0 ]]; then
    cat "$log" >&2
    return "$status"
  fi
}

record_success() {
  local name="$1"
  local command="$2"
  local artifact="$3"
  local timestamp digest
  timestamp="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  digest="$(digest_file "$artifact")"
  jq -cn --arg name "$name" --arg command "$command" --arg timestamp "$timestamp" \
    --arg digest "$digest" \
    '{name:$name,command:$command,started_at:$timestamp,finished_at:$timestamp,exit_status:0,output_digest:$digest}' \
    >>"$gates_file"
}

run_behavioral_certification() {
  functional_evidence="$evidence_dir/four-process-functional.json"
  runtime_evidence="$evidence_dir/runtime-behavior.json"
  provider_migration_evidence="$evidence_dir/provider-migrations-live.json"

  run_gate cluster_functionality \
    "DTG_CLEAN_BREAK_FUNCTIONAL_EVIDENCE='$functional_evidence' cargo test --locked -p dtg-gateway --test live_certification -- --nocapture"
  run_gate cluster_functionality_evidence \
    "jq -e '.four_process_functional_probes.gateway_bolt.typed_row.vertex_id == 37 and .four_process_functional_probes.gateway_bolt.summary.has_more == false' '$functional_evidence'"
  record_success four_process_functional_probes \
    'Meta catalog, Controller observation, Data apply, and Gateway Bolt probes' \
    "$functional_evidence"
  record_success bolt_typed_rows_summary \
    'typed Bolt fields, map row, integer vertex identifier, and summary' \
    "$functional_evidence"

  run_gate runtime_behavioral_certification \
    "DTG_CLEAN_BREAK_RUNTIME_EVIDENCE='$runtime_evidence' cargo test --locked -p dtg-gateway --test runtime_certification -- --nocapture"
  run_gate runtime_behavioral_evidence \
    "jq -e '.raft_leader_failover_follower_read.replication_factor == 3 and .temporal_snapshot_isolation.two_phase_commit.commands[-1] == \"finalize_commit\" and .logical_snapshot_restart_continue.restart_replayed_suffix == 1 and .snapshot_csr_builtin_async_analytics.tcypher_async.status == \"succeeded\" and .six_provider_migrations.count == 6 and .heterogeneous_data_node.hosted_shards == 3' '$runtime_evidence'"
  record_success raft_leader_failover_follower_read \
    'RF=3 election, failover, ReadIndex, and follower historical read proof' \
    "$runtime_evidence"
  record_success temporal_snapshot_isolation \
    'single-Shard fast path and Home-Shard two-phase commit' \
    "$runtime_evidence"
  record_success logical_snapshot_restart_continue \
    'logical snapshot install, active reopen, and WAL suffix continuation' \
    "$runtime_evidence"
  record_success snapshot_csr_builtin_async_analytics \
    'Snapshot CSR, PageRank, and T-Cypher analytics through a durable ledger' \
    "$runtime_evidence"
  record_success six_provider_migrations \
    'six directed provider migration control-state paths' \
    "$runtime_evidence"
  record_success heterogeneous_data_node \
    'one Data node hosting independent Fjall, PostgreSQL, and Kuzu bindings' \
    "$runtime_evidence"

  if [[ $mode == live ]]; then
    run_gate provider_migrations_live \
      "DTG_CLEAN_BREAK_PROVIDER_MIGRATION_EVIDENCE='$provider_migration_evidence' bash scripts/test-provider-migrations-live.sh"
    run_gate provider_migrations_live_evidence \
      "jq -e '.six_provider_migrations.count == 6 and all(.six_provider_migrations.directions[]; .source_content_digest == .target_canonical_digest)' '$provider_migration_evidence'"
  else
    jq -n \
      '{schema_version:1,status:"not_run",reason:"local mode excludes the external PostgreSQL service"}' \
      >"$provider_migration_evidence"
  fi
}

write_manifest() {
  local environment_digest content_digest status
  environment_digest="$(digest_file "$evidence_dir/environment.txt")"
  cat "$gates_file" "$functional_evidence" "$runtime_evidence" \
    "$provider_migration_evidence" >"$evidence_dir/content-input.txt"
  content_digest="$(digest_file "$evidence_dir/content-input.txt")"
  status="passed"
  if jq -s -e 'any(.[]; .exit_status != 0)' "$gates_file" >/dev/null; then
    status="failed"
  fi
  jq -n --argjson schema "$SCHEMA_VERSION" --arg status "$status" \
    --arg mode "$mode" --arg commit "$(git -C "$repo_root" rev-parse HEAD)" \
    --arg generated "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --arg environment_digest "$environment_digest" --arg content_digest "$content_digest" \
    --slurpfile gates "$gates_file" --slurpfile functional "$functional_evidence" \
    --slurpfile runtime "$runtime_evidence" \
    --slurpfile provider_migrations "$provider_migration_evidence" \
    '{schema_version:$schema,status:$status,mode:$mode,git_commit:$commit,generated_at:$generated,
      environment_fingerprint_digest:$environment_digest,content_digest:$content_digest,
      gates:$gates,
      cluster_functionality:{
        bolt_typed_rows_summary:$functional[0].four_process_functional_probes.gateway_bolt,
        four_process_functional_probes:$functional[0].four_process_functional_probes,
        raft_leader_failover_follower_read:$runtime[0].raft_leader_failover_follower_read,
        temporal_snapshot_isolation:$runtime[0].temporal_snapshot_isolation,
        logical_snapshot_restart_continue:$runtime[0].logical_snapshot_restart_continue,
        snapshot_csr_builtin_async_analytics:$runtime[0].snapshot_csr_builtin_async_analytics,
        six_provider_migrations:{
          control_state_machine:$runtime[0].six_provider_migrations,
          physical_live:$provider_migrations[0].six_provider_migrations
        },
        heterogeneous_data_node:$runtime[0].heterogeneous_data_node
      },
      provider_migrations_live:$provider_migrations[0]}' >"$evidence_dir/manifest.json"
  jq -e '.status == "passed" and ([.gates[].exit_status] | all(. == 0)) and
    .cluster_functionality.four_process_functional_probes.gateway_bolt.typed_row.vertex_id == 37 and
    .cluster_functionality.raft_leader_failover_follower_read.replication_factor == 3 and
    .cluster_functionality.six_provider_migrations.control_state_machine.count == 6' \
    "$evidence_dir/manifest.json" >/dev/null
  printf '%s\n' "$evidence_dir/manifest.json"
}

run_certification() {
  require_tool rustc
  require_tool git
  validate_contract
  mkdir -p "$evidence_root"
  evidence_root="$(cd "$evidence_root" && pwd -P)"
  evidence_dir="$evidence_root/$(date -u '+%Y%m%dT%H%M%SZ')-$(git -C "$repo_root" rev-parse --short HEAD)-$mode"
  mkdir -p "$evidence_dir"
  gates_file="$evidence_dir/gates.ndjson"
  : >"$gates_file"
  {
    uname -a
    rustc -Vv
    cargo -V
    git -C "$repo_root" rev-parse HEAD
  } >"$evidence_dir/environment.txt"

  run_gate format 'cargo fmt --all -- --check'
  run_gate architecture 'bash scripts/check-layered-architecture.sh'
  run_gate legacy_removal 'bash scripts/check-clean-break-removal.sh'
  run_gate process_contracts 'cargo test --locked -p dtg-language -p dtg-execution -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway'
  run_gate raft_and_consistent_reads 'cargo test --locked -p dtg-shard'
  run_gate temporal_transactions 'cargo test --locked -p dtg-transaction'
  run_gate snapshots_and_builtin_analytics 'cargo test --locked -p dtg-analytics'
  run_gate control_and_six_migrations 'cargo test --locked -p dtg-control'
  run_gate storage_and_remote_protocol 'cargo test --locked -p dtg-storage -p dtg-storage-fjall -p dtg-storage-remote'
  run_gate official_provider_contracts 'cargo test --locked -p dtg-storage-fjall -p dtg-storage-postgres -p dtg-storage-kuzu'
  run_gate strict_clippy 'cargo clippy --locked -p dtg-language -p dtg-execution -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway --all-targets -- -D warnings'
  run_gate process_binaries 'cargo build --locked -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway --bins'

  if [[ $mode == live ]]; then
    run_gate postgresql_live 'bash scripts/test-postgres-live.sh'
  fi
  run_behavioral_certification
  write_manifest
}

[[ $# -eq 1 ]] || { usage; exit 2; }
case "$1" in
  --contract-only) mode="contract" ;;
  --local) mode="local" ;;
  --live) mode="live" ;;
  *) usage; exit 2 ;;
esac

cd "$repo_root"
if [[ $mode == contract ]]; then
  validate_contract
  jq -n --argjson schema "$SCHEMA_VERSION" \
    '{schema_version:$schema,status:"contract_passed",role_count:4,fixture_process_count:6}'
else
  run_certification
fi
