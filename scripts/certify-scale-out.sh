#!/usr/bin/env bash
set -euo pipefail

readonly SCHEMA_VERSION=1
readonly DATASET_SEED="dtgproxy-scale-out-v1"
readonly DEFAULT_QUERY="USE scale_graph FOR VALID_TIME AS OF 1000 MATCH (n) RETURN count(n) AS count"

mode="run"
report_path=""
report_directory=""
data_node_binary=""
gateway_binary=""
meta_node_binary=""
bolt_loadgen_binary=""
smoke=false

usage() {
  >&2 cat <<'EOF'
Usage:
  scripts/certify-scale-out.sh --validate-report FILE
  scripts/certify-scale-out.sh --validate-suite FILE
  scripts/certify-scale-out.sh --validate-threshold-suite FILE
  scripts/certify-scale-out.sh [--smoke] --report-dir DIR \
    --data-node-bin FILE --gateway-bin FILE --meta-node-bin FILE --bolt-loadgen-bin FILE
EOF
}

emit_error() {
  local code="$1"
  local message="$2"
  jq -n --arg code "$code" --arg message "$message" \
    '{schema_version: 1, status: "failed", error: {code: $code, message: $message}}'
}

fail() {
  emit_error "$1" "$2"
  exit 1
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || fail "missing_tool" "required tool '$1' is unavailable"
}

absolute_executable() {
  local path="$1"
  local directory
  directory=$(cd "$(dirname "$path")" && pwd)
  printf '%s/%s\n' "$directory" "$(basename "$path")"
}

readonly TOPOLOGY_VALIDATOR='def topology_error:
  (.node_count // null) as $node_count
  | (.processes // null) as $processes
  | (.workload // null) as $workload
  | if (($node_count | type) != "number" or ($node_count | floor) != $node_count or ($node_count != 1 and $node_count != 4 and $node_count != 8)) then
      "invalid_node_count"
    elif (($processes | type) != "array") then "missing_processes"
    elif (($workload | type) != "object") then "missing_workload"
    elif ([$processes[] | select(.role == "data-node")] | length) != $node_count then "data_node_process_count_mismatch"
    elif ([$processes[] | select(.role == "gateway")] | length) != 1 then "gateway_process_count_mismatch"
    elif any($processes[]; ((.pid // null) | type) != "number" or (.pid | floor) != .pid or .pid <= 1) then "invalid_process_pid"
    elif any($processes[] | select(.role == "data-node"); ((.node_id // null) | type) != "number" or (.node_id | floor) != .node_id or .node_id <= 0) then "invalid_data_node_id"
    elif ([$processes[] | select(.role == "data-node") | .pid] | unique | length) != $node_count then "duplicate_data_node_pid"
    elif ([$processes[] | select(.role == "data-node") | .node_id] | unique | length) != $node_count then "duplicate_data_node_id"
    elif any($processes[]; ((.listen_address // null) | type) != "string" or (.listen_address | test("^127[.]0[.]0[.]1:[1-9][0-9]{0,4}$") | not)) then "invalid_process_listener"
    elif ([$processes[] | select(.role == "data-node") | .listen_address] | unique | length) != $node_count then "duplicate_data_node_listener"
    elif any($processes[] | select(.role == "data-node"); ((.data_directory // null) | type) != "string" or (.data_directory | startswith("/") | not)) then "invalid_data_node_data_directory"
    elif ([$processes[] | select(.role == "data-node") | .data_directory] | unique | length) != $node_count then "duplicate_data_node_data_directory"
    elif any($processes[]; has("rss_bytes") | not) then "missing_rss_bytes"
    elif any($processes[]; ((.rss_bytes | type) != "number") or .rss_bytes < 0) then "invalid_rss_bytes"
    elif has("network_rx_bytes") | not then "missing_network_rx_bytes"
    elif ((.network_rx_bytes | type) != "number" or .network_rx_bytes < 0) then "invalid_network_rx_bytes"
    elif has("network_tx_bytes") | not then "missing_network_tx_bytes"
    elif ((.network_tx_bytes | type) != "number" or .network_tx_bytes < 0) then "invalid_network_tx_bytes"
    elif (($workload.dataset_seed // null) != "dtgproxy-scale-out-v1") then "invalid_dataset_seed"
    elif (($workload.query // null) != "USE scale_graph FOR VALID_TIME AS OF 1000 MATCH (n) RETURN count(n) AS count") then "invalid_query"
    elif (($workload.duration_seconds // null) | type) != "number" or $workload.duration_seconds <= 0 then "invalid_duration_seconds"
    elif (($workload.concurrency // null) | type) != "number" or ($workload.concurrency | floor) != $workload.concurrency or $workload.concurrency <= 0 then "invalid_concurrency"
    elif ($workload | has("result_digest") | not) then "missing_result_digest"
    elif (($workload.result_digest | type) != "string" or ($workload.result_digest | test("^[0-9a-f]{64}$") | not)) then "invalid_result_digest"
    elif ($workload | has("row_count") | not) then "missing_row_count"
    elif (($workload.row_count | type) != "number" or ($workload.row_count | floor) != $workload.row_count or $workload.row_count <= 0) then "invalid_row_count"
    elif ($workload | has("ttfr_ms") | not) then "missing_ttfr_ms"
    elif (($workload.ttfr_ms | type) != "number" or $workload.ttfr_ms < 0) then "invalid_ttfr_ms"
    elif ($workload | has("total_latency_ms") | not) then "missing_total_latency_ms"
    elif (($workload.total_latency_ms | type) != "number" or $workload.total_latency_ms < $workload.ttfr_ms) then "invalid_total_latency_ms"
    elif ($workload | has("throughput_ops_per_second") | not) then "missing_throughput"
    elif (($workload.throughput_ops_per_second | type) != "number" or $workload.throughput_ops_per_second <= 0) then "invalid_throughput"
    else ""
    end;
'

error_message() {
  case "$1" in
    duplicate_data_node_pid) echo "data-node PIDs must be distinct OS processes" ;;
    duplicate_data_node_listener) echo "data-node listen addresses must be distinct" ;;
    duplicate_data_node_data_directory) echo "data-node data directories must be distinct" ;;
    missing_result_digest) echo "external Bolt result digest is required" ;;
    missing_row_count) echo "external Bolt row count is required" ;;
    missing_ttfr_ms) echo "external Bolt TTFR is required" ;;
    missing_rss_bytes) echo "process RSS observation is required" ;;
    missing_network_rx_bytes|missing_network_tx_bytes) echo "process network byte observations are required" ;;
    result_identity_mismatch) echo "all topologies must return the same digest and row count" ;;
    four_node_scale_out_below_2_8x) echo "four-node throughput must be at least 2.8x the one-node baseline" ;;
    eight_node_scale_out_below_5_0x) echo "eight-node throughput must be at least 5.0x the one-node baseline" ;;
    *) echo "scale-out report failed validation: $1" ;;
  esac
}

validate_report() {
  local path="$1"
  [[ -r "$path" ]] || fail "report_unreadable" "report is not readable: $path"
  path=$(cd "$(dirname "$path")" && pwd)/$(basename "$path")
  jq -e . "$path" >/dev/null 2>&1 || fail "invalid_json" "report is not valid JSON: $path"
  local envelope_error
  envelope_error=$(jq -r '
    if .schema_version != 1 then "invalid_schema_version"
    elif .status != "passed" then "report_not_passed"
    elif (.topology | type) != "object" then "missing_topology"
    else "" end
  ' "$path")
  [[ -z "$envelope_error" ]] || fail "$envelope_error" "$(error_message "$envelope_error")"
  local topology_error
  topology_error=$(jq -r "$TOPOLOGY_VALIDATOR .topology | topology_error" "$path")
  [[ -z "$topology_error" ]] || fail "$topology_error" "$(error_message "$topology_error")"
  jq -n --arg path "$path" '{schema_version: 1, status: "validated", report: $path}'
}

validate_suite() {
  local path="$1"
  local enforce_thresholds="${2:-false}"
  [[ -r "$path" ]] || fail "report_unreadable" "suite report is not readable: $path"
  path=$(cd "$(dirname "$path")" && pwd)/$(basename "$path")
  jq -e . "$path" >/dev/null 2>&1 || fail "invalid_json" "suite report is not valid JSON: $path"
  local suite_error
  suite_error=$(jq -r --argjson enforce_thresholds "$enforce_thresholds" "${TOPOLOGY_VALIDATOR}"'
    if .schema_version != 1 then "invalid_schema_version"
    elif .status != "passed" then "report_not_passed"
    elif (.topologies | type) != "array" then "missing_topologies"
    elif ([.topologies[].node_count] | sort) != [1,4,8] then "topology_set_mismatch"
    elif ([.topologies[] | topology_error | select(length > 0)] | first // "") != "" then
      ([.topologies[] | topology_error | select(length > 0)] | first)
    elif ([.topologies[].workload | [.result_digest, .row_count]] | unique | length) != 1 then "result_identity_mismatch"
    elif $enforce_thresholds and
        ((first(.topologies[] | select(.node_count == 4) | .workload.throughput_ops_per_second) * 10) <
         (first(.topologies[] | select(.node_count == 1) | .workload.throughput_ops_per_second) * 28))
      then "four_node_scale_out_below_2_8x"
    elif $enforce_thresholds and
        ((first(.topologies[] | select(.node_count == 8) | .workload.throughput_ops_per_second) * 10) <
         (first(.topologies[] | select(.node_count == 1) | .workload.throughput_ops_per_second) * 50))
      then "eight_node_scale_out_below_5_0x"
    else "" end
  ' "$path")
  [[ -z "$suite_error" ]] || fail "$suite_error" "$(error_message "$suite_error")"
  if [[ $enforce_thresholds == true ]]; then
    jq -n --arg path "$path" --slurpfile suite "$path" '
      ($suite[0].topologies | map({key: (.node_count | tostring), value: .workload.throughput_ops_per_second}) | from_entries) as $throughput
      | {
          schema_version: 1,
          status: "threshold_validated",
          report: $path,
          throughput_ops_per_second: {
            one_node: $throughput["1"],
            four_nodes: $throughput["4"],
            eight_nodes: $throughput["8"]
          },
          speedup: {
            four_nodes: ($throughput["4"] / $throughput["1"]),
            eight_nodes: ($throughput["8"] / $throughput["1"])
          },
          required_speedup: {four_nodes: 2.8, eight_nodes: 5.0}
        }'
  else
    jq -n --arg path "$path" '{schema_version: 1, status: "diagnostic_validated", report: $path}'
  fi
}

run_certification() {
  require_tool jq
  [[ -n "$report_directory" ]] || fail "missing_report_directory" "--report-dir is required"
  [[ -n "$data_node_binary" ]] || fail "missing_data_node_binary" "--data-node-bin is required"
  [[ -n "$gateway_binary" ]] || fail "missing_gateway_binary" "--gateway-bin is required"
  [[ -n "$meta_node_binary" ]] || fail "missing_meta_node_binary" "--meta-node-bin is required"
  [[ -n "$bolt_loadgen_binary" ]] || fail "missing_bolt_loadgen_binary" "--bolt-loadgen-bin is required"
  for binary in "$data_node_binary" "$gateway_binary" "$meta_node_binary" "$bolt_loadgen_binary"; do
    [[ -x "$binary" ]] || fail "binary_not_executable" "required binary is not executable: $binary"
  done
  data_node_binary=$(absolute_executable "$data_node_binary")
  gateway_binary=$(absolute_executable "$gateway_binary")
  meta_node_binary=$(absolute_executable "$meta_node_binary")
  bolt_loadgen_binary=$(absolute_executable "$bolt_loadgen_binary")
  mkdir -p "$report_directory"
  report_directory=$(cd "$report_directory" && pwd)

  DTGPROXY_RUN_SCALE_OUT=1 \
  DTGPROXY_SCALE_OUT_REPORT_DIR="$report_directory" \
  DTGPROXY_SCALE_OUT_DURATION_SECONDS="${DTGPROXY_SCALE_OUT_DURATION_SECONDS:-1}" \
  DTGPROXY_SCALE_OUT_CONCURRENCY="${DTGPROXY_SCALE_OUT_CONCURRENCY:-1}" \
  DTGPROXY_SCALE_OUT_WARMUP_SECONDS="${DTGPROXY_SCALE_OUT_WARMUP_SECONDS:-0}" \
  DTGPROXY_SCALE_OUT_DATASET_ROWS="${DTGPROXY_SCALE_OUT_DATASET_ROWS:-4096}" \
  DTGPROXY_SCALE_OUT_TIMEOUT_MS="${DTGPROXY_SCALE_OUT_TIMEOUT_MS:-60000}" \
  DTGPROXY_SCALE_OUT_ENFORCE_THRESHOLDS="$([[ "$smoke" == true ]] && echo 0 || echo 1)" \
  DTGPROXY_META_BIN="$meta_node_binary" \
  DTGPROXY_DATA_BIN="$data_node_binary" \
  DTGPROXY_GATEWAY_BIN="$gateway_binary" \
  DTGPROXY_BOLT_LOADGEN_BIN="$bolt_loadgen_binary" \
    cargo test --locked -p gateway-node --test scale_out_process \
      real_process_scale_out_certification -- --exact --test-threads=1

  if [[ "$smoke" == true ]]; then
    for nodes in 1 4 8; do
      validate_report "$report_directory/scale-out-$nodes.json" >/dev/null
    done
    jq -e '
      [.topologies[].workload | [.result_digest, .row_count]] | unique | length == 1
    ' "$report_directory/scale-out-summary.json" >/dev/null \
      || fail "result_identity_mismatch" "$(error_message result_identity_mismatch)"
    jq -n --arg report "$report_directory/scale-out-summary.json" \
      '{schema_version: 1, status: "smoke_passed", report: $report}'
  else
    validate_suite "$report_directory/scale-out-summary.json" true
  fi
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --validate-report)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      mode="validate-report"
      report_path="$2"
      shift 2
      ;;
    --validate-suite)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      mode="validate-suite"
      report_path="$2"
      shift 2
      ;;
    --validate-threshold-suite)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      mode="validate-threshold-suite"
      report_path="$2"
      shift 2
      ;;
    --smoke)
      smoke=true
      shift
      ;;
    --report-dir)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      report_directory="$2"
      shift 2
      ;;
    --data-node-bin)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      data_node_binary="$2"
      shift 2
      ;;
    --gateway-bin)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      gateway_binary="$2"
      shift 2
      ;;
    --meta-node-bin)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      meta_node_binary="$2"
      shift 2
      ;;
    --bolt-loadgen-bin)
      [[ $# -ge 2 ]] || { usage; exit 2; }
      bolt_loadgen_binary="$2"
      shift 2
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

require_tool jq
case "$mode" in
  validate-report) validate_report "$report_path" ;;
  validate-suite) validate_suite "$report_path" false ;;
  validate-threshold-suite) validate_suite "$report_path" true ;;
  run) run_certification ;;
esac
