#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/prepare-paper-performance.sh \
  --backend rocksdb|postgresql|neo4j \
  --spec FILE --runtime-manifest FILE --dataset-evidence FILE \
  --backend-evidence FILE --gateway-build-evidence FILE \
  --executor-bin FILE --gateway-bin FILE --data-node-bin FILE \
  --meta-node-bin FILE --bolt-loadgen-bin FILE --orchestrator-bin FILE \
  --output-dir DIR

Validates and seals formal experiment inputs. It never starts services, builds
binaries, connects to a backend, or runs the formal experiment matrix.
EOF
}

backend= spec= runtime_manifest= dataset_evidence= backend_evidence=
gateway_build_evidence= executor_bin= gateway_bin= data_node_bin=
meta_node_bin= bolt_loadgen_bin= output_dir=
orchestrator_bin=
while (($#)); do
  case "$1" in
    --backend|--spec|--runtime-manifest|--dataset-evidence|--backend-evidence|--gateway-build-evidence|--executor-bin|--gateway-bin|--data-node-bin|--meta-node-bin|--bolt-loadgen-bin|--orchestrator-bin|--output-dir)
      (($# >= 2)) || { printf 'missing value for %s\n' "$1" >&2; exit 2; }
      name=${1#--}; name=${name//-/_}; printf -v "$name" '%s' "$2"; shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) printf 'unknown argument: %s\n' "$1" >&2; usage >&2; exit 2 ;;
  esac
done

for name in backend spec runtime_manifest dataset_evidence backend_evidence gateway_build_evidence executor_bin gateway_bin data_node_bin meta_node_bin bolt_loadgen_bin orchestrator_bin output_dir; do
  [[ -n ${!name} ]] || { printf '%s is required\n' "--${name//_/-}" >&2; exit 2; }
done
case "$backend" in
  rocksdb|postgresql|neo4j) ;;
  *) printf '%s\n' '--backend must be one of rocksdb|postgresql|neo4j' >&2; exit 2 ;;
esac
command -v python3 >/dev/null || { printf 'python3 is required\n' >&2; exit 1; }
[[ ! -e $output_dir ]] || { printf 'output directory already exists: %s\n' "$output_dir" >&2; exit 1; }
parent=$(cd "$(dirname "$output_dir")" && pwd -P)
output_dir="$parent/$(basename "$output_dir")"
temporary=$(mktemp -d "$parent/.paper-prepare.XXXXXX")
trap 'rm -rf "$temporary"' EXIT

python3 - "$backend" "$spec" "$runtime_manifest" "$dataset_evidence" "$backend_evidence" \
  "$gateway_build_evidence" "$executor_bin" "$gateway_bin" "$data_node_bin" \
  "$meta_node_bin" "$bolt_loadgen_bin" "$orchestrator_bin" "$temporary" <<'PY'
import hashlib
import ipaddress
import json
import os
import platform
import shutil
import socket
import stat
import subprocess
import sys
from pathlib import Path

(selected_backend, spec_path, runtime_path, dataset_path, backend_path, gateway_evidence_path,
 executor_path, gateway_path, data_path, meta_path, loadgen_path, orchestrator_path,
 output_path) = sys.argv[1:]
spec_path, runtime_path, dataset_path, backend_path, gateway_evidence_path = map(
    Path, (spec_path, runtime_path, dataset_path, backend_path, gateway_evidence_path)
)
executor_path, gateway_path, data_path, meta_path, loadgen_path, orchestrator_path, output_path = map(
    Path, (executor_path, gateway_path, data_path, meta_path, loadgen_path, orchestrator_path, output_path)
)

def reject(message):
    raise SystemExit(f"prepare-paper-performance: {message}")

def read_json(path, label):
    if not path.is_absolute():
        reject(f"{label} path must be absolute")
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as error:
        reject(f"invalid {label}: {error}")

def digest_file(path):
    value = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()

def valid_digest(value):
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)

def require_executable(path, label):
    if not path.is_absolute() or not path.is_file() or not os.access(path, os.X_OK):
        reject(f"{label} must be an existing absolute executable file")

def live_process(pid, executable, label):
    if not isinstance(pid, int) or pid <= 1:
        reject(f"invalid {label} PID")
    try:
        os.kill(pid, 0)
    except Exception:
        reject(f"{label} PID {pid} is not live")
    try:
        if sys.platform == "linux":
            running = Path(os.readlink(f"/proc/{pid}/exe"))
        elif sys.platform == "darwin":
            running = Path(subprocess.check_output(
                ["ps", "-p", str(pid), "-o", "comm="],
                text=True,
            ).strip())
        else:
            reject(f"{label} process-image verification is unsupported on {sys.platform}")
        if running.resolve(strict=True) != executable.resolve(strict=True):
            reject(f"{label} PID executable mismatch")
    except SystemExit:
        raise
    except Exception as error:
        reject(f"failed to verify {label} PID executable: {error}")

def local_boot_identity():
    try:
        if sys.platform == "linux":
            value = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
        elif sys.platform == "darwin":
            value = subprocess.check_output(["sysctl", "-n", "kern.boottime"], text=True).strip()
        else:
            reject(f"local boot identity is unsupported on {sys.platform}")
    except SystemExit:
        raise
    except Exception as error:
        reject(f"failed to capture local boot identity: {error}")
    if not value:
        reject("local boot identity is empty")
    return value

def local_process_start_identity(pid):
    try:
        if sys.platform == "linux":
            stat_line = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
            closing = stat_line.rfind(")")
            fields = stat_line[closing + 2:].split()
            value = f"linux-start-ticks:{fields[19]}"
        elif sys.platform == "darwin":
            value = subprocess.check_output(
                ["ps", "-p", str(pid), "-o", "lstart="], text=True
            ).strip()
        else:
            reject(f"local process start identity is unsupported on {sys.platform}")
    except SystemExit:
        raise
    except Exception as error:
        reject(f"failed to capture local process start identity: {error}")
    if not value:
        reject(f"local process start identity is empty for PID {pid}")
    return value

def positive_integer(value):
    return type(value) is int and value > 0

def nonnegative_integer(value):
    return type(value) is int and value >= 0

def nonempty_string(value):
    return isinstance(value, str) and bool(value.strip())

def require_non_loopback_listener(value):
    if not isinstance(value, str):
        reject("remote_formal data-node listeners must be non-loopback IP addresses")
    try:
        if value.startswith("["):
            host, separator, port = value[1:].partition("]:")
            if not separator:
                raise ValueError("invalid bracketed listener")
        else:
            host, separator, port = value.rpartition(":")
            if not separator:
                raise ValueError("listener has no port")
        address = ipaddress.ip_address(host)
        port_number = int(port)
    except (TypeError, ValueError):
        reject("remote_formal data-node listeners must be non-loopback IP addresses")
    if address.is_loopback or not 1 <= port_number <= 65535:
        reject("remote_formal data-node listeners must be non-loopback IP addresses")

ssh_options = [
    "-T",
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=5",
    "-o", "ConnectionAttempts=1",
    "-o", "ServerAliveInterval=5",
    "-o", "ServerAliveCountMax=1",
]

def run_ssh(ssh_target, remote_argv, label):
    if (not nonempty_string(ssh_target) or ssh_target.startswith("-") or
            any(character.isspace() for character in ssh_target)):
        reject("remote probe ssh_target must be a non-option token without whitespace")
    try:
        completed = subprocess.run(
            ["ssh", *ssh_options, "--", ssh_target, *remote_argv],
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except subprocess.TimeoutExpired:
        reject(f"{label} exceeded the 15-second SSH bound")
    except Exception as error:
        reject(f"failed to invoke {label}: {error}")
    if completed.returncode != 0:
        detail = completed.stderr.strip()
        reject(f"{label} failed" + (f": {detail}" if detail else ""))
    if (not completed.stdout.endswith("\n") or completed.stdout.count("\n") != 1 or
            len(completed.stdout.encode("utf-8")) > 16 * 1024):
        reject(f"{label} must return one newline-terminated line of at most 16 KiB")
    return completed.stdout[:-1]

remote_probe_digests = {}
remote_probe_snapshots = {}

def validate_remote_probe_binary(probe):
    ssh_target = probe.get("ssh_target")
    probe_binary = probe.get("probe_binary")
    claimed_digest = probe.get("probe_binary_sha256")
    if (not nonempty_string(probe_binary) or not Path(probe_binary).is_absolute() or
            not valid_digest(claimed_digest)):
        reject("remote probe binary and SHA-256 claim are invalid")
    if claimed_digest != digest_file(executor_path):
        reject("remote probe binary digest does not match --executor-bin")
    key = (ssh_target, probe_binary)
    if key not in remote_probe_digests:
        line = run_ssh(
            ssh_target,
            ["sha256sum", "--", probe_binary],
            "remote probe binary digest check",
        )
        fields = line.split()
        if len(fields) != 2 or fields[1] != probe_binary or not valid_digest(fields[0]):
            reject("remote probe binary digest response is invalid")
        remote_probe_digests[key] = fields[0]
    if remote_probe_digests[key] != claimed_digest:
        reject("remote probe binary digest does not match the runtime claim")

def capture_remote_process(process):
    expected_process_keys = {
        "pid", "executable", "executable_sha256", "listen_address",
        "data_directory", "probe",
    }
    if set(process) != expected_process_keys:
        reject("remote_formal data-node process schema is invalid")
    pid = process.get("pid")
    executable = process.get("executable")
    executable_digest = process.get("executable_sha256")
    directory = process.get("data_directory")
    if not positive_integer(pid):
        reject("invalid remote_formal data-node PID")
    if not nonempty_string(executable) or not Path(executable).is_absolute():
        reject("remote_formal data-node executable must be an absolute path")
    if not valid_digest(executable_digest):
        reject("remote_formal data-node executable_sha256 is invalid")
    if executable_digest != digest_file(data_path):
        reject("runtime data-node executable digest does not match --data-node-bin")
    if not nonempty_string(directory) or not Path(directory).is_absolute():
        reject("remote_formal data-node data directories must be absolute")
    require_non_loopback_listener(process.get("listen_address"))

    probe = process.get("probe")
    expected_probe_keys = {
        "ssh_target", "host_id", "boot_id", "probe_binary",
        "probe_binary_sha256", "data_interface", "management_interface",
    }
    if not isinstance(probe, dict) or set(probe) != expected_probe_keys:
        reject("remote_formal probe schema is invalid")
    if any(not nonempty_string(probe.get(key)) for key in (
        "host_id", "boot_id", "data_interface", "management_interface"
    )):
        reject("remote_formal probe identity and interface claims must be non-empty")
    if probe["data_interface"] == probe["management_interface"]:
        reject("remote_formal data and management interfaces must be distinct")
    validate_remote_probe_binary(probe)

    cache_key = (
        probe["ssh_target"], probe["probe_binary"], pid, probe["data_interface"],
        executable, process["listen_address"], directory,
    )
    if cache_key not in remote_probe_snapshots:
        line = run_ssh(
            probe["ssh_target"],
            [
                probe["probe_binary"], "probe-process", "--pid", str(pid),
                "--network-interface", probe["data_interface"],
                "--executable", executable,
                "--listen-address", process["listen_address"],
                "--data-directory", directory,
            ],
            "remote process probe",
        )
        try:
            snapshot = json.loads(line)
        except Exception as error:
            reject(f"invalid remote process probe JSON: {error}")
        expected_snapshot_keys = {
            "schema_version", "sampled_unix_ns", "host_id", "boot_id",
            "process_start_id", "pid", "executable", "executable_sha256",
            "probe_binary_sha256", "listen_address", "data_directory",
            "cpu_time_ns", "rss_bytes", "peak_rss_bytes", "network_interface",
            "network_rx_bytes", "network_tx_bytes",
        }
        if not isinstance(snapshot, dict) or set(snapshot) != expected_snapshot_keys:
            reject("remote process probe JSON schema is invalid")
        if snapshot.get("schema_version") != 1:
            reject("remote process probe schema_version must be 1")
        if not positive_integer(snapshot.get("sampled_unix_ns")):
            reject("remote process probe sampled_unix_ns is invalid")
        if any(not nonnegative_integer(snapshot.get(key)) for key in (
            "cpu_time_ns", "rss_bytes", "peak_rss_bytes", "network_rx_bytes",
            "network_tx_bytes",
        )):
            reject("remote process probe counters must be nonnegative integers")
        if snapshot["peak_rss_bytes"] < snapshot["rss_bytes"]:
            reject("remote process probe peak_rss_bytes is below rss_bytes")
        remote_probe_snapshots[cache_key] = snapshot
    snapshot = remote_probe_snapshots[cache_key]

    claimed_fields = {
        "host_id": probe["host_id"],
        "boot_id": probe["boot_id"],
        "probe_binary_sha256": probe["probe_binary_sha256"],
        "pid": pid,
        "executable": executable,
        "executable_sha256": executable_digest,
        "listen_address": process["listen_address"],
        "data_directory": directory,
        "network_interface": probe["data_interface"],
    }
    for field, claimed in claimed_fields.items():
        if snapshot.get(field) != claimed:
            reject(f"remote probe {field} does not match the runtime claim")
    if not nonempty_string(snapshot.get("process_start_id")):
        reject("remote probe process_start_id must be non-empty")
    return snapshot

spec = read_json(spec_path, "formal spec")
runtime = read_json(runtime_path, "runtime manifest")
dataset = read_json(dataset_path, "dataset evidence")
backend = read_json(backend_path, "backend evidence")
gateway_evidence = read_json(gateway_evidence_path, "Gateway build evidence")

binaries = {
    "executor": executor_path, "gateway": gateway_path, "data_node": data_path,
    "meta_node": meta_path, "bolt_loadgen": loadgen_path,
}
for label, path in binaries.items():
    require_executable(path, label)

labels = ["production", "no_native_pushdown", "no_column_batch",
          "no_lazy_pages", "no_parallel_fanout", "no_batched_gather"]
if spec.get("schema_version") != 1:
    reject("formal spec schema_version must be 1")
if spec.get("selected_backend") != selected_backend:
    reject("formal spec selected_backend does not match --backend")
manifest = spec.get("dataset", {})
if [manifest.get(k) for k in ("vertex_count", "edge_count", "temporal_update_count")] != [1000000, 5000000, 600000]:
    reject("formal dataset must contain 1000000 vertices, 5000000 edges, and 600000 updates")
dataset_digest = manifest.get("content_digest")
if not valid_digest(dataset_digest):
    reject("invalid formal dataset digest")
protocol = spec.get("protocol", {})
if [protocol.get(k) for k in ("warmup_seconds", "measurement_seconds", "repetitions")] != [30, 60, 5]:
    reject("formal protocol must be 30s warmup, 60s measurement, and 5 repetitions")
suites = {item.get("kind"): item for item in spec.get("matrix", {}).get("suites", [])}
if set(suites) != {"comparison", "scale", "ablation"}:
    reject("formal spec must contain comparison, scale, and ablation suites")
if set(suites["scale"].get("data_nodes", [])) != {1, 4, 8}:
    reject("formal scale suite must contain 1/4/8 data-node topologies")
if set(suites["comparison"].get("concurrencies", [])) != {1, 8, 32, 64} or set(suites["scale"].get("concurrencies", [])) != {1, 8, 32, 64}:
    reject("formal comparison and scale concurrency axes must be 1/8/32/64")
if any(set(suites[name].get("backends", [])) != {selected_backend} for name in suites):
    reject("suite backend differs from selected_backend")
expected_ablations = {
    "native_pushdown_filter": "no_native_pushdown",
    "column_batch_scan": "no_column_batch",
    "lazy_paged_scan": "no_lazy_pages",
    "parallel_fanout_count": "no_parallel_fanout",
    "batched_expand_gather": "no_batched_gather",
}
actual = suites["ablation"].get("workload_ablations", {})
if any(actual.get(workload) != ["production", disabled] for workload, disabled in expected_ablations.items()):
    reject("formal workload ablation mapping is invalid")

workloads = spec.get("workloads", [])
workload_digests = {item.get("manifest", {}).get("digest") for item in workloads}
if len(workload_digests) != len(workloads) or any(not valid_digest(value) for value in workload_digests):
    reject("formal workload digests must be unique lowercase SHA-style digests")
workloads_by_id = {
    item.get("manifest", {}).get("workload_id"): item.get("manifest", {})
    for item in workloads
}
if len(workloads_by_id) != len(workloads):
    reject("formal workload IDs must be unique")
scale_workload = workloads_by_id.get("partition_parallel_scan")
expected_scale_query = (
    "MATCH (n) WHERE n.active = true WITH n.id AS id ORDER BY id LIMIT 4096 RETURN id"
)
if (scale_workload is None or
        scale_workload.get("query") != expected_scale_query or
        scale_workload.get("available_paths") != ["proxy"] or
        "partition_parallel_scan" not in suites["scale"].get("workloads", [])):
    reject("formal scale suite requires the partition_parallel_scan workload")
snapshots = {item.get("snapshot") for item in workloads}
if len(snapshots) != 1 or None in snapshots:
    reject("all formal workloads must use one pinned snapshot")

if runtime.get("schema_version") != 1 or runtime.get("dataset_digest") != dataset_digest:
    reject("runtime manifest dataset digest mismatch")
if runtime.get("snapshot") not in snapshots:
    reject("runtime manifest snapshot mismatch")
runtime_workloads = runtime.get("workloads", [])
if ({item.get("workload_digest") for item in runtime_workloads} != workload_digests or
        len(runtime_workloads) != len(workload_digests)):
    reject("runtime workload bindings do not match formal spec")
if not any(
    item.get("workload_digest") == scale_workload.get("digest")
    for item in runtime_workloads
):
    reject("runtime workload bindings must include partition_parallel_scan")
runtime_backends = runtime.get("backends", {})
if selected_backend not in runtime_backends:
    reject("runtime manifest does not configure selected_backend")
runtime["backends"] = {selected_backend: runtime_backends[selected_backend]}

targets = [
    item for item in runtime.get("proxy_targets", [])
    if item.get("backend") == selected_backend
]
runtime["proxy_targets"] = targets
expected_targets = {(selected_backend, nodes, label) for nodes in (1, 4, 8) for label in (labels if nodes == 8 else ["production"])}
actual_targets = {(item.get("backend"), item.get("data_nodes"), item.get("ablation")) for item in targets}
if actual_targets != expected_targets or len(targets) != len(expected_targets):
    reject("runtime Proxy targets do not cover the formal matrix exactly")
gateway_to_socket = {}
socket_to_gateway = {}
remote_evidence_targets = []
managed_gateways = {}
for target in targets:
    nodes = target["data_nodes"]
    processes = target.get("data_node_processes", [])
    deployment_mode = target.get("deployment_mode")
    if deployment_mode not in {"local_diagnostic", "remote_formal"}:
        reject("Proxy target deployment_mode must be local_diagnostic or remote_formal")
    if deployment_mode != "remote_formal":
        reject("formal preparation requires deployment_mode remote_formal")
    if len(processes) != nodes:
        reject("data-node process evidence must match the declared data-node count")
    gateway_process = target.get("gateway_process", {})
    gateway_pid = gateway_process.get("pid")
    socket_path = Path(target.get("ablation_control_socket", ""))
    if not socket_path.is_absolute():
        reject("Gateway control sockets must be absolute Unix paths")
    try:
        if not stat.S_ISSOCK(socket_path.stat().st_mode):
            reject("Gateway control socket is not a Unix socket")
    except OSError:
        reject("Gateway control socket does not exist")
    if gateway_pid in gateway_to_socket and gateway_to_socket[gateway_pid] != socket_path:
        reject("one Gateway process cannot declare multiple control sockets")
    if socket_path in socket_to_gateway and socket_to_gateway[socket_path] != gateway_pid:
        reject("different Gateway processes must use different absolute Unix control sockets")
    gateway_to_socket[gateway_pid] = socket_path
    socket_to_gateway[socket_path] = gateway_pid
    if Path(gateway_process.get("executable", "")) != gateway_path:
        reject("runtime Gateway executable does not match --gateway-bin")
    live_process(gateway_pid, gateway_path, "Gateway")
    gateway_identity = {
        "host_id": socket.gethostname(),
        "boot_id": local_boot_identity(),
        "process_start_id": local_process_start_identity(gateway_pid),
        "pid": gateway_pid,
    }
    existing_gateway = managed_gateways.get(gateway_pid)
    if existing_gateway is not None and existing_gateway["identity"] != gateway_identity:
        reject("Gateway PID identity changed during preparation")
    managed_gateways[gateway_pid] = {
        "backend": selected_backend,
        "role": "gateway",
        "identity": gateway_identity,
        "executable": str(gateway_path),
        "executable_sha256": digest_file(gateway_path),
        "probe": {"kind": "local"},
    }

    host_ids = [item.get("probe", {}).get("host_id") for item in processes]
    if len(set(host_ids)) != nodes:
        reject("remote_formal data nodes must have unique host_id values")
    listeners = [item.get("listen_address") for item in processes]
    if len(set(listeners)) != nodes:
        reject("remote_formal data-node listeners must be distinct")
    target_evidence = {
        "backend": target["backend"],
        "data_nodes": nodes,
        "ablation": target["ablation"],
        "deployment_mode": deployment_mode,
        "processes": [],
    }
    for process in processes:
        snapshot = capture_remote_process(process)
        target_evidence["processes"].append({
            "claimed": process,
            "snapshot": snapshot,
        })
    remote_evidence_targets.append(target_evidence)

if dataset.get("schema_version") != 1:
    reject("dataset evidence schema_version must be 1")
dataset_backends = dataset.get("backends", [])
dataset_backends = [item for item in dataset_backends if item.get("backend") == selected_backend]
if len(dataset_backends) != 1:
    reject("dataset evidence must identify the selected_backend location and endpoint")
dataset["backends"] = dataset_backends
for item in dataset_backends:
    if item.get("logical_dataset_digest") != dataset_digest or not item.get("dataset_location") or not item.get("service_endpoint"):
        reject("backend dataset location, endpoint, or digest is invalid")
topologies = [
    item for item in dataset.get("topologies", [])
    if item.get("backend") == selected_backend
]
dataset["topologies"] = topologies
expected_topologies = {(selected_backend, nodes) for nodes in (1, 4, 8)}
if {(item.get("backend"), item.get("data_nodes")) for item in topologies} != expected_topologies or len(topologies) != 3:
    reject("dataset evidence must contain selected_backend 1/4/8 topology exactly once")
for topology in topologies:
    if topology.get("logical_dataset_digest") != dataset_digest:
        reject("topology logical dataset digest mismatch")
    shards = topology.get("shards", [])
    if len(shards) != topology["data_nodes"] or len({item.get("shard_id") for item in shards}) != len(shards):
        reject("topology shard evidence does not match its data-node count")
    for shard in shards:
        if shard.get("active_true_vertices", 0) <= 0 or shard.get("active_false_vertices", 0) <= 0:
            reject("every shard must contain active=true and active=false vertices")
        if shard.get("lazy_matching_vertices", 0) <= 16384:
            reject("lazy_matching_vertices must be greater than 16384 on every shard")
        visible = shard.get("visible_edges", 0)
        sources = shard.get("expand_source_vertices", 0)
        if visible <= 0 or sources <= 0 or visible <= sources:
            reject("Expand evidence requires visible edges and fanout greater than 1")

backend_versions = backend.get("backends", {})
if backend.get("schema_version") != 1 or selected_backend not in backend_versions:
    reject("backend evidence must contain the selected_backend version")
if not str(backend_versions[selected_backend].get("version", "")).strip():
    reject("selected_backend version must be non-empty")
backend["backends"] = {selected_backend: backend_versions[selected_backend]}
if gateway_evidence.get("schema_version") != 1 or gateway_evidence.get("profile") != "release":
    reject("Gateway build evidence must describe a release binary")
if "paper-benchmark-control" not in gateway_evidence.get("features", []):
    reject("Gateway build evidence must include paper-benchmark-control")
if gateway_evidence.get("binary_sha256") != digest_file(gateway_path):
    reject("Gateway build evidence digest does not match --gateway-bin")

def command_version(program):
    try:
        return subprocess.check_output([program, "--version"], text=True, stderr=subprocess.STDOUT).strip()
    except Exception:
        reject(f"{program} --version is unavailable")

def sealed_backend_version(name):
    if name != selected_backend:
        return "not-selected"
    return str(backend_versions[name]["version"])

logical_cpus = os.cpu_count() or 0
if logical_cpus <= 0:
    reject("logical CPU count is unavailable")
if sys.platform == "darwin":
    cpu_model = subprocess.check_output(["sysctl", "-n", "machdep.cpu.brand_string"], text=True).strip()
    total_memory = int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip())
else:
    cpu_model = platform.processor().strip()
    if not cpu_model and Path("/proc/cpuinfo").exists():
        cpu_model = next((line.split(":", 1)[1].strip() for line in Path("/proc/cpuinfo").read_text().splitlines() if line.startswith("model name")), "")
    memory_kib = next((line.split()[1] for line in Path("/proc/meminfo").read_text().splitlines() if line.startswith("MemTotal:")), "0")
    total_memory = int(memory_kib) * 1024
if not cpu_model or total_memory <= 0:
    reject("CPU model or total memory is unavailable")
fingerprint = {
    "schema_version": 1,
    "source": "captured",
    "os_name": platform.system(),
    "os_version": platform.release(),
    "architecture": platform.machine(),
    "cpu_model": cpu_model,
    "logical_cpu_count": logical_cpus,
    "total_memory_bytes": total_memory,
    "versions": {
        "rustc": command_version("rustc"),
        "cargo": command_version("cargo"),
        "rocksdb": sealed_backend_version("rocksdb"),
        "postgresql": sealed_backend_version("postgresql"),
        "neo4j": sealed_backend_version("neo4j"),
    },
    "binaries": {
        "executor_sha256": digest_file(executor_path),
        "gateway_sha256": digest_file(gateway_path),
        "data_node_sha256": digest_file(data_path),
        "meta_node_sha256": digest_file(meta_path),
        "loadgen_sha256": digest_file(loadgen_path),
    },
}
canonical_fingerprint = json.dumps(
    fingerprint, ensure_ascii=False, separators=(",", ":")
).encode("utf-8")
fingerprint["digest"] = hashlib.sha256(canonical_fingerprint).hexdigest()
spec["environment"] = fingerprint

sealed_inputs = {
    "runtime-manifest.json": runtime,
    "dataset-evidence.json": dataset,
    "backend-evidence.json": backend,
}
for name, value in sealed_inputs.items():
    (output_path / name).write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
shutil.copyfile(gateway_evidence_path, output_path / "gateway-build-evidence.json")
(output_path / "remote-node-evidence.json").write_text(
    json.dumps({
        "schema_version": 1,
        "source": "remote_formal_preparation",
        "targets": remote_evidence_targets,
    }, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
managed_processes = list(managed_gateways.values())
seen_remote_identities = set()
for target in remote_evidence_targets:
    for process in target["processes"]:
        claimed = process["claimed"]
        snapshot = process["snapshot"]
        probe = claimed["probe"]
        identity_key = (
            probe["ssh_target"], snapshot["pid"], snapshot["host_id"],
            snapshot["boot_id"], snapshot["process_start_id"],
        )
        if identity_key in seen_remote_identities:
            continue
        seen_remote_identities.add(identity_key)
        managed_processes.append({
            "backend": selected_backend,
            "role": "data_node",
            "identity": {
                "host_id": snapshot["host_id"],
                "boot_id": snapshot["boot_id"],
                "process_start_id": snapshot["process_start_id"],
                "pid": snapshot["pid"],
            },
            "executable": snapshot["executable"],
            "executable_sha256": snapshot["executable_sha256"],
            "probe": {
                "kind": "remote",
                "ssh_target": probe["ssh_target"],
                "probe_binary": probe["probe_binary"],
                "network_interface": probe["data_interface"],
            },
        })
managed_processes.sort(key=lambda item: (
    item["role"], item["identity"]["host_id"], item["identity"]["pid"]
))
backend_service = (
    {"ownership": "embedded", "managed_by": "data_node"}
    if selected_backend == "rocksdb"
    else {
        "ownership": "external",
        "managed": False,
        "reason": "prepared runtime contains no exact backend service PID/start identity",
    }
)
(output_path / "managed-process-evidence.json").write_text(
    json.dumps({
        "schema_version": 1,
        "selected_backend": selected_backend,
        "lifecycle_contract": (
            "the sealed run command must retire every exact managed process before returning; "
            "the sequential runner never sends process signals"
        ),
        "managed_processes": managed_processes,
        "backend_service": backend_service,
    }, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
(output_path / "formal-spec.json").write_text(
    json.dumps(spec, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
)
(output_path / "environment-fingerprint.json").write_text(json.dumps(fingerprint, indent=2, sort_keys=True) + "\n", encoding="utf-8")
ready = {
    "schema_version": 1, "status": "prepared", "run_id": spec.get("run_id"),
    "selected_backend": selected_backend,
    "formal_matrix_executed": False,
    "formal_run": {
        "argv": [
            "scripts/run-paper-performance.sh",
            "--spec", "formal-spec.json",
            "--executor", str(executor_path),
            "--orchestrator-bin", str(orchestrator_path),
        ],
        "environment": {"DTGPROXY_PAPER_RUNTIME_MANIFEST": "runtime-manifest.json"},
        "executor_sha256": digest_file(executor_path),
        "orchestrator_sha256": digest_file(orchestrator_path),
    },
}
(output_path / "READY.json").write_text(json.dumps(ready, indent=2, sort_keys=True) + "\n", encoding="utf-8")
checksum_lines = []
for path in sorted(output_path.iterdir()):
    if path.is_file():
        checksum_lines.append(f"{digest_file(path)}  {path.name}\n")
(output_path / "SHA256SUMS").write_text("".join(checksum_lines), encoding="ascii")
for path in output_path.iterdir():
    path.chmod(0o400)
PY

"$orchestrator_bin" validate-spec \
  --spec "$temporary/formal-spec.json" \
  --executor "$executor_bin" >/dev/null

mv "$temporary" "$output_dir"
trap - EXIT
printf 'prepared formal experiment bundle: %s\n' "$output_dir"
