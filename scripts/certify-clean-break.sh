#!/usr/bin/env bash
set -euo pipefail

readonly SCHEMA_VERSION=1
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fixture_root="$repo_root/config/examples/clean-break-cluster"
mode=""
evidence_root="${DTG_CLEAN_BREAK_EVIDENCE_ROOT:-$repo_root/target/clean-break-certification}"
target_dir="${CARGO_TARGET_DIR:-$repo_root/target}"
runtime_pids=()

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
  for file in meta-1.json controller-1.json gateway-1.json data-1.json data-2.json; do
    test -r "$fixture_root/$file"
    jq -e '.version == 1 and .cluster_id == 9001' "$fixture_root/$file" >/dev/null
  done
  jq -e '.security.mode == "loopback_plaintext" and (.listen_addr | startswith("127.0.0.1:"))' \
    "$fixture_root/meta-1.json" "$fixture_root/controller-1.json" >/dev/null
  jq -e '.provider_classes == ["fjall", "postgresql", "neo4j", "remote"]' \
    "$fixture_root/data-1.json" "$fixture_root/data-2.json" >/dev/null
  jq -e '.cluster_endpoint | startswith("http://127.0.0.1:")' \
    "$fixture_root/gateway-1.json" >/dev/null
  test -f "$repo_root/crates/processes/dtg-gateway/tests/four_process_cluster.rs"
  test -f "$repo_root/crates/processes/dtg-meta/src/main.rs"
  test -f "$repo_root/crates/processes/dtg-controller/src/main.rs"
  test -f "$repo_root/crates/processes/dtg-data/src/main.rs"
  test -f "$repo_root/crates/processes/dtg-gateway/src/main.rs"
  if rg -n -i 'rocksdb|adapter-sidecar|procedure-runtime|dtgproxy[.]cluster[.]v1' \
      "$fixture_root" "$repo_root/crates/processes/dtg-gateway/tests/four_process_cluster.rs"; then
    return 1
  fi
}

stop_processes() {
  local pid
  for pid in "${runtime_pids[@]:-}"; do
    if kill -0 "$pid" 2>/dev/null; then
      kill -INT "$pid" 2>/dev/null || true
    fi
  done
  for pid in "${runtime_pids[@]:-}"; do
    wait "$pid" 2>/dev/null || true
  done
  runtime_pids=()
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

record_live_prerequisites() {
  local output="$evidence_dir/live-backend-prerequisites.txt"
  local docker_pid=0
  {
    printf 'psql=%s\n' "$(command -v psql || echo unavailable)"
    printf 'docker=%s\n' "$(command -v docker || echo unavailable)"
    if command -v docker >/dev/null 2>&1; then
      docker info >/dev/null 2>&1 &
      docker_pid=$!
      local attempt
      for attempt in 1 2 3 4 5; do
        if ! kill -0 "$docker_pid" 2>/dev/null; then
          if wait "$docker_pid"; then
            printf 'docker_daemon=ready\n'
          else
            printf 'docker_daemon=unavailable\n'
          fi
          docker_pid=0
          break
        fi
        sleep 1
      done
      if [[ $docker_pid -ne 0 ]]; then
        kill "$docker_pid" 2>/dev/null || true
        wait "$docker_pid" 2>/dev/null || true
        printf 'docker_daemon=timeout\n'
      fi
    fi
  } >"$output"
  record_success live_backend_prerequisites \
    'bounded discovery of PostgreSQL client and Docker/Neo4j runtime availability' "$output"
}

start_processes() {
  local base runtime meta_config controller_config
  base=$((55000 + ($$ % 500) * 10))
  runtime="$evidence_dir/runtime"
  mkdir -p "$runtime/meta" "$runtime/controller" "$runtime/data-1/business" \
    "$runtime/data-1/raft" "$runtime/data-2/business" "$runtime/data-2/raft"
  meta_config="$runtime/meta.json"
  controller_config="$runtime/controller.json"
  jq --arg address "127.0.0.1:$((base + 1))" --arg directory "$runtime/meta" \
    '.listen_addr=$address | .data_directory=$directory' "$fixture_root/meta-1.json" >"$meta_config"
  jq --arg address "127.0.0.1:$((base + 2))" --arg directory "$runtime/controller" \
    '.listen_addr=$address | .data_directory=$directory' "$fixture_root/controller-1.json" >"$controller_config"

  "$target_dir/debug/dtgproxy-meta" --config "$meta_config" \
    >"$runtime/meta.log" 2>&1 & runtime_pids+=("$!")
  "$target_dir/debug/dtgproxy-controller" --config "$controller_config" \
    >"$runtime/controller.log" 2>&1 & runtime_pids+=("$!")
  env DTG_DATA_RPC_ADDR="127.0.0.1:$((base + 3))" \
    DTG_DATA_FJALL_ROOT="$runtime/data-1/business" \
    DTG_DATA_CONSENSUS_ROOT="$runtime/data-1/raft" \
    "$target_dir/debug/dtgproxy-data" >"$runtime/data-1.log" 2>&1 & runtime_pids+=("$!")
  env DTG_DATA_RPC_ADDR="127.0.0.1:$((base + 4))" \
    DTG_DATA_FJALL_ROOT="$runtime/data-2/business" \
    DTG_DATA_CONSENSUS_ROOT="$runtime/data-2/raft" \
    "$target_dir/debug/dtgproxy-data" >"$runtime/data-2.log" 2>&1 & runtime_pids+=("$!")

  sleep 2
  local pid
  for pid in "${runtime_pids[@]}"; do
    kill -0 "$pid"
  done

  env DTG_GATEWAY_BIND="127.0.0.1:$((base + 5))" DTG_GATEWAY_CLUSTER_ID=9001 \
    DTG_GATEWAY_REQUEST_TIMEOUT_MS=30000 \
    DTG_GATEWAY_CLUSTER_ENDPOINT="http://127.0.0.1:$((base + 3))" \
    "$target_dir/debug/dtgproxy-gateway" >"$runtime/gateway.log" 2>&1 & runtime_pids+=("$!")
  sleep 2
  kill -0 "${runtime_pids[4]}"

  jq -n --argjson base "$base" --argjson meta "${runtime_pids[0]}" \
    --argjson controller "${runtime_pids[1]}" --argjson data1 "${runtime_pids[2]}" \
    --argjson data2 "${runtime_pids[3]}" --argjson gateway "${runtime_pids[4]}" \
    '{role_count:4,process_count:5,base_port:$base,processes:[
      {role:"meta",pid:$meta},{role:"controller",pid:$controller},
      {role:"data",node_id:11,pid:$data1},{role:"data",node_id:12,pid:$data2},
      {role:"gateway",pid:$gateway}]}' >"$evidence_dir/processes.json"
  record_success four_process_liveness 'start Meta, Controller, two Data processes, and Gateway; verify every PID remains live' \
    "$evidence_dir/processes.json"
  stop_processes
}

write_manifest() {
  local environment_digest content_digest status
  environment_digest="$(digest_file "$evidence_dir/environment.txt")"
  cat "$gates_file" "$evidence_dir/processes.json" >"$evidence_dir/content-input.txt"
  content_digest="$(digest_file "$evidence_dir/content-input.txt")"
  status="passed"
  if jq -s -e 'any(.[]; .exit_status != 0)' "$gates_file" >/dev/null; then
    status="failed"
  fi
  jq -n --argjson schema "$SCHEMA_VERSION" --arg status "$status" \
    --arg mode "$mode" --arg commit "$(git -C "$repo_root" rev-parse HEAD)" \
    --arg generated "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" \
    --arg environment_digest "$environment_digest" --arg content_digest "$content_digest" \
    --slurpfile gates "$gates_file" --slurpfile processes "$evidence_dir/processes.json" \
    '{schema_version:$schema,status:$status,mode:$mode,git_commit:$commit,generated_at:$generated,
      environment_fingerprint_digest:$environment_digest,content_digest:$content_digest,
      gates:$gates,cluster:$processes[0]}' >"$evidence_dir/manifest.json"
  jq -e '.status == "passed" and ([.gates[].exit_status] | all(. == 0))' \
    "$evidence_dir/manifest.json" >/dev/null
  printf '%s\n' "$evidence_dir/manifest.json"
}

run_certification() {
  require_tool rustc
  require_tool git
  validate_contract
  mkdir -p "$evidence_root"
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
  trap stop_processes EXIT INT TERM

  run_gate format 'cargo fmt --all -- --check'
  run_gate architecture 'bash scripts/check-layered-architecture.sh'
  record_live_prerequisites
  run_gate process_contracts 'cargo test --locked -p dtg-language -p dtg-execution -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway'
  run_gate raft_and_consistent_reads 'cargo test --locked -p dtg-shard'
  run_gate temporal_transactions 'cargo test --locked -p dtg-transaction'
  run_gate snapshots_and_builtin_analytics 'cargo test --locked -p dtg-analytics'
  run_gate control_and_six_migrations 'cargo test --locked -p dtg-control'
  run_gate storage_and_remote_protocol 'cargo test --locked -p dtg-storage -p dtg-storage-fjall -p dtg-storage-remote'
  run_gate strict_clippy 'cargo clippy --locked -p dtg-language -p dtg-execution -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway --all-targets -- -D warnings'
  run_gate process_binaries 'cargo build --locked -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway --bins'
  start_processes

  if [[ $mode == live ]]; then
    run_gate postgresql_live 'bash scripts/test-postgres-live.sh'
    run_gate neo4j_live 'bash scripts/test-neo4j-live.sh'
  fi
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
    '{schema_version:$schema,status:"contract_passed",role_count:4,process_count:5}'
else
  run_certification
fi
