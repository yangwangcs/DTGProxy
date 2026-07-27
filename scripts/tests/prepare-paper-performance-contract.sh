#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
prepare="$root/scripts/prepare-paper-performance.sh"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-paper-prepare.XXXXXX")
pids=""

cleanup() {
  local pid
  for pid in $pids; do
    kill "$pid" >/dev/null 2>&1 || true
  done
  chmod -R u+w "$scratch" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup EXIT INT TERM

fail() {
  printf 'FAIL %s\n' "$*" >&2
  exit 1
}

expect_rejected() {
  local name=$1
  local expected=$2
  shift 2
  local output="$scratch/out-$name"
  local log="$scratch/$name.log"
  if invoke_prepare "$output" "$@" >"$log" 2>&1; then
    fail "$name unexpectedly passed"
  fi
  grep -F "$expected" "$log" >/dev/null || {
    cat "$log" >&2
    fail "$name failed for the wrong reason"
  }
  [[ ! -e $output ]] || fail "$name left a partial output directory"
  printf 'PASS rejected %s (%s)\n' "$name" "$expected"
}

make_fake_binary() {
  local path=$1
  cc -Os "$scratch/fake-process.c" -o "$path"
}

start_process() {
  "$1" 3600 &
  started_pid=$!
  pids="$pids $started_pid"
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

invoke_prepare() {
  local output=$1
  shift
  PATH="$scratch/bin:$PATH" \
  FAKE_SSH_LOG="$scratch/ssh.log" \
  FAKE_REMOTE_DATA_DIGEST="$(sha256_file "$scratch/bin/dtgproxy-data-node")" \
  FAKE_REMOTE_PROBE_DIGEST="$(sha256_file "$scratch/bin/dtgproxy-paper-cell-executor")" \
  "$prepare" \
    --backend rocksdb \
    --spec "$scratch/formal-spec.json" \
    --runtime-manifest "$scratch/runtime-manifest.json" \
    --dataset-evidence "$scratch/dataset-evidence.json" \
    --backend-evidence "$scratch/backend-evidence.json" \
    --gateway-build-evidence "$scratch/gateway-build-evidence.json" \
    --executor-bin "$scratch/bin/dtgproxy-paper-cell-executor" \
    --gateway-bin "$scratch/bin/dtgproxy-gateway" \
    --data-node-bin "$scratch/bin/dtgproxy-data-node" \
    --meta-node-bin "$scratch/bin/dtgproxy-meta-node" \
    --bolt-loadgen-bin "$scratch/bin/dtgproxy-bolt-loadgen" \
    --orchestrator-bin "$scratch/bin/dtgproxy-paper-benchmark" \
    --output-dir "$output" \
    "$@"
}

mkdir -p "$scratch/bin" "$scratch/rocks-snapshot"
cat >"$scratch/fake-process.c" <<'EOF'
#include <unistd.h>
int main(void) {
  sleep(3600);
  return 0;
}
EOF
for binary in \
  dtgproxy-paper-cell-executor \
  dtgproxy-gateway \
  dtgproxy-data-node \
  dtgproxy-meta-node \
  dtgproxy-bolt-loadgen; do
  make_fake_binary "$scratch/bin/$binary"
done
make_fake_binary "$scratch/bin/unrelated-process"
start_process "$scratch/bin/unrelated-process"
unrelated_pid=$started_pid
cat >"$scratch/bin/ssh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
expected=(
  -T
  -o BatchMode=yes
  -o ConnectTimeout=5
  -o ConnectionAttempts=1
  -o ServerAliveInterval=5
  -o ServerAliveCountMax=1
)
for option in "${expected[@]}"; do
  [[ ${1-} == "$option" ]] || {
    printf 'unexpected ssh option: %s (wanted %s)\n' "${1-}" "$option" >&2
    exit 90
  }
  shift
done
[[ ${1-} == -- ]] || { printf 'missing ssh option terminator\n' >&2; exit 90; }
shift
target=${1-}
shift
[[ $target =~ ^paper-node-([1-8])$ ]] || {
  printf 'unexpected ssh target: %s\n' "$target" >&2
  exit 90
}
node=${BASH_REMATCH[1]}
printf '%s\n' "$target $*" >>"$FAKE_SSH_LOG"
case ${1-} in
  sha256sum)
    [[ ${2-} == -- && ${3-} == /opt/dtgproxy/bin/dtgproxy-paper-cell-executor && $# == 3 ]] || exit 91
    printf '%s  %s\n' "$FAKE_REMOTE_PROBE_DIGEST" "$3"
    ;;
  /opt/dtgproxy/bin/dtgproxy-paper-cell-executor)
    [[ ${2-} == probe-process && ${3-} == --pid && ${5-} == --network-interface &&
       ${7-} == --executable && ${9-} == --listen-address &&
       ${11-} == --data-directory && $# == 12 ]] || exit 92
    pid=$4
    interface=$6
    jq -cn \
      --arg host_id "physical-host-$node" \
      --arg boot_id "boot-$node" \
      --arg process_start_id "process-start-$node-$pid" \
      --argjson pid "$pid" \
      --arg digest "$FAKE_REMOTE_DATA_DIGEST" \
      --arg probe_digest "$FAKE_REMOTE_PROBE_DIGEST" \
      --arg executable "$8" \
      --arg listen_address "${10}" \
      --arg data_directory "${12}" \
      --arg interface "$interface" \
      --argjson sampled_unix_ns "170000000000000000$node" \
      --argjson node "$node" '
      {
        schema_version: 1, sampled_unix_ns: $sampled_unix_ns,
        host_id: $host_id, boot_id: $boot_id, process_start_id: $process_start_id,
        pid: $pid, executable: $executable, executable_sha256: $digest,
        probe_binary_sha256: $probe_digest, listen_address: $listen_address,
        data_directory: $data_directory, cpu_time_ns: (1000000 * $node),
        rss_bytes: (1048576 * $node), peak_rss_bytes: (2097152 * $node),
        network_interface: $interface, network_rx_bytes: (10000 * $node),
        network_tx_bytes: (20000 * $node)
      }'
    ;;
  *)
    printf 'unexpected remote command: %s\n' "${1-}" >&2
    exit 93
    ;;
esac
EOF
chmod +x "$scratch/bin/ssh"
cat >"$scratch/bin/dtgproxy-paper-benchmark" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
[[ ${1-} == validate-spec && ${2-} == --spec && ${4-} == --executor && $# == 5 ]]
python3 - "$3" "$5" <<'PY'
import hashlib
import json
import sys
from pathlib import Path

spec = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
environment = spec["environment"]
expected = environment.pop("digest")
actual = hashlib.sha256(
    json.dumps(environment, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
).hexdigest()
assert actual == expected
assert environment["source"] == "captured"
assert environment["binaries"]["executor_sha256"] == hashlib.sha256(
    Path(sys.argv[2]).read_bytes()
).hexdigest()
PY
EOF
chmod +x "$scratch/bin/dtgproxy-paper-benchmark"

data_pids='[]'
data_dirs='[]'
for index in 1 2 3 4 5 6 7 8; do
  directory="$scratch/data-$index"
  mkdir -p "$directory"
  start_process "$scratch/bin/dtgproxy-data-node"
  pid=$started_pid
  data_pids=$(jq -c --argjson pid "$pid" '. + [$pid]' <<<"$data_pids")
  data_dirs=$(jq -c --arg path "$directory" '. + [$path]' <<<"$data_dirs")
done

gateway_pids='{}'
for nodes in 1 4 8; do
  start_process "$scratch/bin/dtgproxy-gateway"
  pid=$started_pid
  gateway_pids=$(jq -c --arg nodes "$nodes" --argjson pid "$pid" '. + {($nodes): $pid}' <<<"$gateway_pids")
done

python3 - "$scratch/gateway-1.sock" "$scratch/gateway-4.sock" "$scratch/gateway-8.sock" <<'PY' &
import signal
import socket
import sys
import time

sockets = []
for path in sys.argv[1:]:
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(path)
    server.listen(1)
    sockets.append(server)
signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
while True:
    time.sleep(60)
PY
socket_pid=$!
pids="$pids $socket_pid"
for socket_path in "$scratch/gateway-1.sock" "$scratch/gateway-4.sock" "$scratch/gateway-8.sock"; do
  for _ in 1 2 3 4 5 6 7 8 9 10; do
    [[ -S $socket_path ]] && break
    sleep 0.05
  done
  [[ -S $socket_path ]] || fail "fixture socket was not created: $socket_path"
done

revision=$(git -C "$root" rev-parse HEAD)
digest_a=$(printf 'a%.0s' {1..64})
digest_b=$(printf 'b%.0s' {1..64})
workload_ids='["comparison_count","native_pushdown_filter","column_batch_scan","lazy_paged_scan","parallel_fanout_count","batched_expand_gather","partition_parallel_scan"]'
workload_digests='[
  "1111111111111111111111111111111111111111111111111111111111111111",
  "2222222222222222222222222222222222222222222222222222222222222222",
  "3333333333333333333333333333333333333333333333333333333333333333",
  "4444444444444444444444444444444444444444444444444444444444444444",
  "5555555555555555555555555555555555555555555555555555555555555555",
  "6666666666666666666666666666666666666666666666666666666666666666",
  "7777777777777777777777777777777777777777777777777777777777777777"
]'
queries='[
  "MATCH (n) RETURN count(n)",
  "MATCH (n) WHERE n.active = true RETURN count(n)",
  "MATCH (n) RETURN count(n)",
  "MATCH (n) WHERE n.active = true RETURN n.id ORDER BY n.id",
  "MATCH (n) RETURN count(n)",
  "MATCH (n)-[r]->(m) RETURN count(m)",
  "MATCH (n) WHERE n.active = true WITH n.id AS id ORDER BY id LIMIT 4096 RETURN id"
]'

jq -n \
  --arg revision "$revision" \
  --arg dirty "$digest_a" \
  --arg dataset "$digest_b" \
  --argjson ids "$workload_ids" \
  --argjson digests "$workload_digests" \
  --argjson queries "$queries" '
  def workload($i): {
    manifest: {
      schema_version: 1,
      workload_id: $ids[$i],
      query: $queries[$i],
      parameters: {},
      available_paths: (if $i == 0 then ["backend_direct", "adapter_direct", "proxy"] else ["proxy"] end),
      digest: $digests[$i]
    },
    snapshot: "as_of:1000"
  };
  {
    schema_version: 1,
    run_id: "paper-formal-001",
    selected_backend: "rocksdb",
    revision: $revision,
    dirty_worktree_digest: $dirty,
    dataset: {
      schema_version: 1, dataset_id: "paper-1m-5m-v1", seed: 42,
      vertex_count: 1000000, edge_count: 5000000, temporal_update_count: 600000,
      content_digest: $dataset
    },
    workloads: [range(0; 7) as $i | workload($i)],
    matrix: {suites: [
      {kind: "comparison", backends: ["rocksdb"], paths: ["backend_direct","adapter_direct","proxy"], workloads: ["comparison_count"], data_nodes: [1], concurrencies: [1,8,32,64], ablations: ["production"], workload_ablations: {}},
      {kind: "scale", backends: ["rocksdb"], paths: ["proxy"], workloads: ["partition_parallel_scan"], data_nodes: [1,4,8], concurrencies: [1,8,32,64], ablations: ["production"], workload_ablations: {}},
      {kind: "ablation", backends: ["rocksdb"], paths: ["proxy"], workloads: $ids[1:6], data_nodes: [8], concurrencies: [32], ablations: ["production","no_native_pushdown","no_column_batch","no_lazy_pages","no_parallel_fanout","no_batched_gather"], workload_ablations: {
        native_pushdown_filter: ["production","no_native_pushdown"],
        column_batch_scan: ["production","no_column_batch"],
        lazy_paged_scan: ["production","no_lazy_pages"],
        parallel_fanout_count: ["production","no_parallel_fanout"],
        batched_expand_gather: ["production","no_batched_gather"]
      }}
    ]},
    protocol: {warmup_seconds: 30, measurement_seconds: 60, repetitions: 5},
    shuffle_seed: 99
  }' >"$scratch/formal-spec.json"

jq -n \
  --arg dataset "$digest_b" \
  --arg rocks "$scratch/rocks-snapshot" \
  --arg loadgen "$scratch/bin/dtgproxy-bolt-loadgen" \
  --arg gateway "$scratch/bin/dtgproxy-gateway" \
  --arg data "$scratch/bin/dtgproxy-data-node" \
  --arg data_digest "$(sha256_file "$scratch/bin/dtgproxy-data-node")" \
  --arg probe_digest "$(sha256_file "$scratch/bin/dtgproxy-paper-cell-executor")" \
  --arg scratch "$scratch" \
  --argjson ids "$workload_ids" \
  --argjson digests "$workload_digests" \
  --argjson data_pids "$data_pids" \
  --argjson data_dirs "$data_dirs" \
  --argjson gateway_pids "$gateway_pids" '
  def labels($nodes): if $nodes == 8 then ["production","no_native_pushdown","no_column_batch","no_lazy_pages","no_parallel_fanout","no_batched_gather"] else ["production"] end;
  def target($backend; $nodes; $label): {
    backend: $backend, data_nodes: $nodes, ablation: $label, deployment_mode: "remote_formal",
    bolt_address: ("127.0.0.1:" + (12000 + $nodes | tostring)),
    bolt_loadgen_binary: $loadgen,
    ablation_control_socket: ($scratch + "/gateway-" + ($nodes|tostring) + ".sock"),
    timeout_ms: 30000,
    data_node_processes: [range(0; $nodes) as $i | {
      pid: $data_pids[$i], executable: "/opt/dtgproxy/bin/dtgproxy-data-node",
      executable_sha256: $data_digest,
      listen_address: ("10.20.0." + ($i + 1 | tostring) + ":" + (13000 + $i | tostring)),
      data_directory: ("/var/lib/dtgproxy/data-" + ($i + 1 | tostring)),
      probe: {
        ssh_target: ("paper-node-" + ($i + 1 | tostring)),
        host_id: ("physical-host-" + ($i + 1 | tostring)),
        boot_id: ("boot-" + ($i + 1 | tostring)),
        probe_binary: "/opt/dtgproxy/bin/dtgproxy-paper-cell-executor",
        probe_binary_sha256: $probe_digest,
        data_interface: "eth-data", management_interface: "eth-management"
      }
    }],
    gateway_process: {
      pid: $gateway_pids[($nodes|tostring)], executable: $gateway,
      listen_address: ("127.0.0.1:" + (14000 + $nodes | tostring))
    }
  };
  {
    schema_version: 1, dataset_digest: $dataset, snapshot: "as_of:1000",
    workloads: [range(0; 7) as $i | {
      workload_digest: $digests[$i],
      kind: (if $i == 0 then "count_current_vertices" else null end),
      graph_id: (if $i == 0 then 1 else 0 end)
    } | with_entries(select(.value != null))],
    backends: {
      rocksdb: {status: "available", snapshot_paths: [$rocks]},
      postgresql: {status: "available", connection_string: "postgresql://paper@127.0.0.1:5432/dtgproxy", instance_id: "paper-v1", pool_size: 64},
      neo4j: {status: "available", endpoint: "http://127.0.0.1:7474", database: "neo4j", username: "neo4j", password: "paper-secret", instance_id: "paper-v1", timeout_seconds: 30}
    },
    proxy_targets: [
      ["rocksdb","postgresql","neo4j"][] as $backend |
      [1,4,8][] as $nodes | labels($nodes)[] as $label |
      target($backend; $nodes; $label)
    ]
  }' >"$scratch/runtime-manifest.json"

jq -n \
  --arg dataset "$digest_b" \
  --arg rocks "$scratch/rocks-snapshot" '
  def backend($name; $location; $endpoint): {backend: $name, dataset_location: $location, service_endpoint: $endpoint, logical_dataset_digest: $dataset};
  def topology($backend; $nodes): {
    backend: $backend, data_nodes: $nodes, logical_dataset_digest: $dataset,
    shards: [range(0; $nodes) as $i | {
      shard_id: ($i + 1), active_true_vertices: 20000, active_false_vertices: 20000,
      lazy_matching_vertices: 20000, visible_edges: 50000, expand_source_vertices: 20000
    }]
  };
  {
    schema_version: 1,
    backends: [
      backend("rocksdb"; {snapshot_paths: [$rocks]}; "embedded://rocksdb"),
      backend("postgresql"; {instance_id: "paper-v1"}; "postgresql://paper@127.0.0.1:5432/dtgproxy"),
      backend("neo4j"; {database: "neo4j", instance_id: "paper-v1"}; "http://127.0.0.1:7474")
    ],
    topologies: [["rocksdb","postgresql","neo4j"][] as $backend | [1,4,8][] as $nodes | topology($backend; $nodes)]
  }' >"$scratch/dataset-evidence.json"

jq -n '{schema_version: 1, backends: {rocksdb: {version: "9.7.4"}, postgresql: {version: "17.5"}, neo4j: {version: "2025.05"}}}' >"$scratch/backend-evidence.json"
jq -n \
  --arg digest "$(sha256_file "$scratch/bin/dtgproxy-gateway")" \
  '{schema_version: 1, binary_sha256: $digest, profile: "release", features: ["paper-benchmark-control"]}' \
  >"$scratch/gateway-build-evidence.json"

[[ -x $prepare ]] || {
  printf 'expected RED: prepare-paper-performance.sh is not implemented\n' >&2
  exit 1
}

output="$scratch/prepared"
invoke_prepare "$output"
for file in \
  formal-spec.json runtime-manifest.json dataset-evidence.json backend-evidence.json \
  gateway-build-evidence.json environment-fingerprint.json remote-node-evidence.json \
  READY.json SHA256SUMS; do
  [[ -f $output/$file ]] || fail "prepared bundle is missing $file"
done
jq -e '
  .schema_version == 1 and .status == "prepared" and
  .selected_backend == "rocksdb" and
  .formal_matrix_executed == false and .run_id == "paper-formal-001" and
  (.formal_run.argv | index("--simulate") | not) and
  (.formal_run.argv | index("--orchestrator-bin") != null) and
  .formal_run.environment.DTGPROXY_PAPER_RUNTIME_MANIFEST == "runtime-manifest.json" and
  (.formal_run.executor_sha256 | test("^[0-9a-f]{64}$")) and
  (.formal_run.orchestrator_sha256 | test("^[0-9a-f]{64}$"))
' "$output/READY.json" >/dev/null || fail "READY.json contract mismatch"
jq -e '
  .selected_backend == "rocksdb" and
  all(.matrix.suites[]; .backends == ["rocksdb"])
' "$output/formal-spec.json" >/dev/null || fail "formal spec selected_backend contract mismatch"
jq -e '
  (.backends | keys) == ["rocksdb"] and
  ([.proxy_targets[].backend] | unique) == ["rocksdb"] and
  (.proxy_targets | length) == 8
' "$output/runtime-manifest.json" >/dev/null || fail "runtime manifest backend filter mismatch"
jq -e '
  (.backends | length) == 1 and .backends[0].backend == "rocksdb" and
  (.topologies | length) == 3 and
  all(.topologies[]; .backend == "rocksdb")
' "$output/dataset-evidence.json" >/dev/null || fail "dataset evidence backend filter mismatch"
jq -e '
  (.backends | keys) == ["rocksdb"]
' "$output/backend-evidence.json" >/dev/null || fail "backend evidence filter mismatch"
jq -e '
  .schema_version == 1 and .source == "captured" and
  (.os_name | length > 0) and (.os_version | length > 0) and
  (.architecture | length > 0) and (.cpu_model | length > 0) and
  (.logical_cpu_count > 0) and (.total_memory_bytes > 0) and
  (.versions.rustc | length > 0) and (.versions.cargo | length > 0) and
  (.versions.rocksdb == "9.7.4") and
  (.versions.postgresql == "not-selected") and
  (.versions.neo4j == "not-selected") and
  ([.binaries[] | test("^[0-9a-f]{64}$")] | all) and
  (.digest | test("^[0-9a-f]{64}$"))
' "$output/environment-fingerprint.json" >/dev/null || fail "environment fingerprint contract mismatch"
jq -e --slurpfile environment "$output/environment-fingerprint.json" \
  '.environment == $environment[0]' "$output/formal-spec.json" >/dev/null ||
  fail "formal spec does not seal the captured environment fingerprint"
jq -e '
  .schema_version == 1 and .source == "remote_formal_preparation" and
  (.targets | length == 8) and
  all(.targets[]; .deployment_mode == "remote_formal") and
  all(.targets[]; (.processes | length) == .data_nodes) and
  all(.targets[].processes[];
    . as $process |
    ($process.snapshot.schema_version == 1) and
    ($process.snapshot.host_id == $process.claimed.probe.host_id) and
    ($process.snapshot.boot_id == $process.claimed.probe.boot_id) and
    ($process.snapshot.pid == $process.claimed.pid) and
    ($process.snapshot.executable_sha256 == $process.claimed.executable_sha256) and
    ($process.snapshot.network_interface == $process.claimed.probe.data_interface)
  )
' "$output/remote-node-evidence.json" >/dev/null || fail "remote node evidence contract mismatch"
(cd "$output" && shasum -a 256 -c SHA256SUMS >/dev/null) || fail "sealed checksums do not verify"
printf 'PASS prepared formal bundle without running a matrix\n'

cp "$scratch/formal-spec.json" "$scratch/formal-spec.valid.json"
jq '(.matrix.suites[] | select(.kind == "comparison") | .backends) = ["postgresql"]' \
  "$scratch/formal-spec.valid.json" >"$scratch/formal-spec.json"
expect_rejected mixed-backend "suite backend differs from selected_backend"
mv "$scratch/formal-spec.valid.json" "$scratch/formal-spec.json"

cp "$scratch/formal-spec.json" "$scratch/formal-spec.valid.json"
jq '(.matrix.suites[] | select(.kind == "scale") | .workloads) = ["comparison_count"]' \
  "$scratch/formal-spec.valid.json" >"$scratch/formal-spec.json"
expect_rejected scale-global-count "formal scale suite requires the partition_parallel_scan workload"
mv "$scratch/formal-spec.valid.json" "$scratch/formal-spec.json"

cp "$scratch/dataset-evidence.json" "$scratch/dataset-evidence.valid.json"
jq '(.topologies[] | select(.backend == "rocksdb" and .data_nodes == 8) | .shards[0].lazy_matching_vertices) = 16384' \
  "$scratch/dataset-evidence.valid.json" >"$scratch/dataset-evidence.json"
expect_rejected lazy-boundary "lazy_matching_vertices must be greater than 16384"
mv "$scratch/dataset-evidence.valid.json" "$scratch/dataset-evidence.json"

cp "$scratch/dataset-evidence.json" "$scratch/dataset-evidence.valid.json"
jq '(.topologies[] | select(.backend == "rocksdb" and .data_nodes == 4) | .logical_dataset_digest) = ("c" * 64)' \
  "$scratch/dataset-evidence.valid.json" >"$scratch/dataset-evidence.json"
expect_rejected topology-digest "topology logical dataset digest mismatch"
mv "$scratch/dataset-evidence.valid.json" "$scratch/dataset-evidence.json"

cp "$scratch/runtime-manifest.json" "$scratch/runtime-manifest.valid.json"
jq '
  (.proxy_targets[] | select(.backend == "rocksdb" and .data_nodes == 4 and .ablation == "production") | .data_node_processes[1].probe.ssh_target) = "paper-node-1" |
  (.proxy_targets[] | select(.backend == "rocksdb" and .data_nodes == 4 and .ablation == "production") | .data_node_processes[1].probe.host_id) = "physical-host-1" |
  (.proxy_targets[] | select(.backend == "rocksdb" and .data_nodes == 4 and .ablation == "production") | .data_node_processes[1].probe.boot_id) = "boot-1"
' \
  "$scratch/runtime-manifest.valid.json" >"$scratch/runtime-manifest.json"
expect_rejected duplicate-host-id "remote_formal data nodes must have unique host_id values"
mv "$scratch/runtime-manifest.valid.json" "$scratch/runtime-manifest.json"

cp "$scratch/runtime-manifest.json" "$scratch/runtime-manifest.valid.json"
jq '(.proxy_targets[] | select(.backend == "rocksdb" and .data_nodes == 1 and .ablation == "production") | .deployment_mode) = "local_diagnostic"' \
  "$scratch/runtime-manifest.valid.json" >"$scratch/runtime-manifest.json"
expect_rejected local-diagnostic-in-formal "formal preparation requires deployment_mode remote_formal"
mv "$scratch/runtime-manifest.valid.json" "$scratch/runtime-manifest.json"

cp "$scratch/runtime-manifest.json" "$scratch/runtime-manifest.valid.json"
jq '(.proxy_targets[] | select(.backend == "rocksdb" and .data_nodes == 1 and .ablation == "production") | .data_node_processes[0].executable_sha256) = ("c" * 64)' \
  "$scratch/runtime-manifest.valid.json" >"$scratch/runtime-manifest.json"
expect_rejected digest-mismatch "runtime data-node executable digest does not match --data-node-bin"
mv "$scratch/runtime-manifest.valid.json" "$scratch/runtime-manifest.json"

cp "$scratch/runtime-manifest.json" "$scratch/runtime-manifest.valid.json"
jq '(.proxy_targets[] | select(.backend == "rocksdb" and .data_nodes == 1 and .ablation == "production") | .data_node_processes[0].probe.management_interface) = "eth-data"' \
  "$scratch/runtime-manifest.valid.json" >"$scratch/runtime-manifest.json"
expect_rejected same-data-management-interface "remote_formal data and management interfaces must be distinct"
mv "$scratch/runtime-manifest.valid.json" "$scratch/runtime-manifest.json"

cp "$scratch/runtime-manifest.json" "$scratch/runtime-manifest.valid.json"
jq --arg socket "$scratch/gateway-1.sock" '
  (.proxy_targets[] | select(.data_nodes == 4) | .ablation_control_socket) = $socket
' "$scratch/runtime-manifest.valid.json" >"$scratch/runtime-manifest.json"
expect_rejected shared-control-socket "different Gateway processes must use different absolute Unix control sockets"
mv "$scratch/runtime-manifest.valid.json" "$scratch/runtime-manifest.json"

cp "$scratch/gateway-build-evidence.json" "$scratch/gateway-build-evidence.valid.json"
jq '.features = []' "$scratch/gateway-build-evidence.valid.json" >"$scratch/gateway-build-evidence.json"
expect_rejected missing-gateway-feature "Gateway build evidence must include paper-benchmark-control"
mv "$scratch/gateway-build-evidence.valid.json" "$scratch/gateway-build-evidence.json"

printf 'PASS prepare paper performance contract\n'
