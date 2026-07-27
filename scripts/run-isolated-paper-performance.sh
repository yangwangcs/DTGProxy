#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/run-isolated-paper-performance.sh \
  --rocksdb-bundle DIR \
  --postgresql-bundle DIR \
  --neo4j-bundle DIR \
  --output-root DIR

Runs the three sealed one-backend experiments in rocksdb, postgresql, neo4j
order. Every artifact is verified and every exact managed PID is confirmed
absent or changed before the next backend starts. The runner never kills or
searches for processes by executable name.
EOF
}

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)
run_script="$root/scripts/run-paper-performance.sh"
verify_script="$root/scripts/verify-paper-performance.sh"
rocksdb_bundle= postgresql_bundle= neo4j_bundle= output_root=

while (($# > 0)); do
  case "$1" in
    --rocksdb-bundle|--postgresql-bundle|--neo4j-bundle|--output-root)
      (($# >= 2)) || { printf 'missing value for %s\n' "$1" >&2; exit 2; }
      name=${1#--}
      name=${name//-/_}
      printf -v "$name" '%s' "$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      printf 'unknown argument: %s\n' "$1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

for name in rocksdb_bundle postgresql_bundle neo4j_bundle output_root; do
  [[ -n ${!name} ]] || { printf '%s is required\n' "--${name//_/-}" >&2; exit 2; }
done
command -v python3 >/dev/null || { printf '%s\n' 'python3 is required' >&2; exit 1; }
command -v ssh >/dev/null || { printf '%s\n' 'ssh is required for managed process identity checks' >&2; exit 1; }
[[ ! -e $output_root/combined ]] || {
  printf 'combined output already exists: %s\n' "$output_root/combined" >&2
  exit 1
}
mkdir -p "$output_root"

scratch=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-paper-isolated.XXXXXX")
cleanup_scratch() {
  chmod -R u+w "$scratch" >/dev/null 2>&1 || true
  rm -rf "$scratch"
}
trap cleanup_scratch EXIT

bundle_metadata() {
  local expected_backend=$1
  local bundle=$2
  python3 - "$expected_backend" "$bundle" <<'PY'
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path, PurePosixPath

expected_backend, raw_bundle = sys.argv[1:]

def reject(message):
    raise SystemExit(f"run-isolated-paper-performance: {message}")

def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

def safe_relative(value):
    if not value or any(character in value for character in ("\n", "\r", "\0")):
        return False
    path = PurePosixPath(value)
    return not path.is_absolute() and all(part not in ("", ".", "..") for part in path.parts)

bundle = Path(raw_bundle)
if bundle.is_symlink() or not bundle.is_dir():
    reject(f"{expected_backend} prepared bundle must be a directory, not a symlink")
bundle = bundle.resolve(strict=True)
checksum_path = bundle / "SHA256SUMS"
if checksum_path.is_symlink() or not checksum_path.is_file():
    reject(f"{expected_backend} prepared bundle is missing SHA256SUMS")
entries = {}
for line in checksum_path.read_text(encoding="ascii").splitlines():
    match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
    if not match or not safe_relative(match.group(2)) or match.group(2) in entries:
        reject(f"{expected_backend} prepared bundle has invalid checksums")
    entries[match.group(2)] = match.group(1)
actual = set()
for directory, dirnames, filenames in os.walk(bundle, followlinks=False):
    directory_path = Path(directory)
    for name in dirnames:
        path = directory_path / name
        if path.is_symlink() or not path.is_dir():
            reject(f"{expected_backend} prepared bundle contains an unsupported entry")
    for name in filenames:
        path = directory_path / name
        relative = path.relative_to(bundle).as_posix()
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode) or not stat.S_ISREG(mode):
            reject(f"{expected_backend} prepared bundle contains an unsupported entry")
        if relative != "SHA256SUMS":
            actual.add(relative)
if actual != set(entries):
    reject(f"{expected_backend} prepared bundle checksum file set mismatch")
for relative, expected in entries.items():
    if digest_file(bundle / relative) != expected:
        reject(f"{expected_backend} prepared bundle checksum mismatch: {relative}")

try:
    ready = json.loads((bundle / "READY.json").read_text(encoding="utf-8"))
    spec = json.loads((bundle / "formal-spec.json").read_text(encoding="utf-8"))
    runtime = json.loads((bundle / "runtime-manifest.json").read_text(encoding="utf-8"))
    evidence = json.loads((bundle / "managed-process-evidence.json").read_text(encoding="utf-8"))
except Exception as error:
    reject(f"{expected_backend} prepared metadata is invalid: {error}")
if ready.get("status") != "prepared" or ready.get("selected_backend") != expected_backend:
    reject(f"{expected_backend} READY selected_backend mismatch")
if spec.get("selected_backend") != expected_backend:
    reject(f"{expected_backend} formal spec selected_backend mismatch")
run_id = ready.get("run_id")
if (not isinstance(run_id, str) or not run_id or run_id in (".", "..") or
        "/" in run_id or "\\" in run_id or any(character in run_id for character in "\n\r\0")):
    reject(f"{expected_backend} READY run_id is invalid")
processes = evidence.get("managed_processes")
roles = {process.get("role") for process in processes} if isinstance(processes, list) else set()
expected_lifecycle = (
    "the sealed run command must retire every exact managed process before returning; "
    "the sequential runner never sends process signals"
)
if (evidence.get("schema_version") != 1 or
        evidence.get("selected_backend") != expected_backend or
        evidence.get("lifecycle_contract") != expected_lifecycle or not processes or
        roles != {"gateway", "data_node"} or
        any(process.get("backend") != expected_backend for process in processes)):
    reject(f"{expected_backend} managed process evidence mismatch")
runtime_targets = runtime.get("proxy_targets")
if not isinstance(runtime_targets, list) or not runtime_targets:
    reject(f"{expected_backend} runtime process inventory is missing")
expected_gateways = {
    (target.get("gateway_process", {}).get("pid"),
     target.get("gateway_process", {}).get("executable"))
    for target in runtime_targets
}
expected_data_nodes = {
    (process.get("pid"), process.get("executable"),
     process.get("probe", {}).get("ssh_target"))
    for target in runtime_targets
    for process in target.get("data_node_processes", [])
}
actual_gateways = {
    (process.get("identity", {}).get("pid"), process.get("executable"))
    for process in processes if process.get("role") == "gateway"
}
actual_data_nodes = {
    (process.get("identity", {}).get("pid"), process.get("executable"),
     process.get("probe", {}).get("ssh_target"))
    for process in processes if process.get("role") == "data_node"
}
if (expected_gateways != actual_gateways or expected_data_nodes != actual_data_nodes or
        len(actual_gateways) != sum(process.get("role") == "gateway" for process in processes) or
        len(actual_data_nodes) != sum(process.get("role") == "data_node" for process in processes)):
    reject(f"{expected_backend} managed process evidence differs from runtime inventory")
backend_service = evidence.get("backend_service")
if expected_backend == "rocksdb":
    if backend_service != {"ownership": "embedded", "managed_by": "data_node"}:
        reject("rocksdb backend service ownership evidence is invalid")
elif (not isinstance(backend_service, dict) or
      backend_service != {
          "ownership": "lifecycle_runner", "runtime_role": "backend_service"
      }):
    reject(f"{expected_backend} backend service lifecycle ownership evidence is invalid")
formal = ready.get("formal_run")
argv = formal.get("argv") if isinstance(formal, dict) else None
if not isinstance(argv, list):
    reject(f"{expected_backend} formal argv is invalid")
positions = [index for index, value in enumerate(argv) if value == "--orchestrator-bin"]
if len(positions) != 1 or positions[0] + 1 >= len(argv):
    reject(f"{expected_backend} orchestrator is not sealed")
orchestrator = Path(argv[positions[0] + 1])
claimed = formal.get("orchestrator_sha256")
if (not orchestrator.is_absolute() or orchestrator.is_symlink() or
        not orchestrator.is_file() or not os.access(orchestrator, os.X_OK) or
        not isinstance(claimed, str) or not re.fullmatch(r"[0-9a-f]{64}", claimed) or
        digest_file(orchestrator) != claimed):
    reject(f"{expected_backend} sealed orchestrator identity mismatch")
lifecycle = formal.get("lifecycle_runner")
if not isinstance(lifecycle, dict) or set(lifecycle) != {"protocol_version", "path", "sha256"}:
    reject(f"{expected_backend} sealed lifecycle runner metadata is invalid")
lifecycle_runner = Path(lifecycle.get("path", ""))
lifecycle_digest = lifecycle.get("sha256")
if (lifecycle.get("protocol_version") != 1 or
        not lifecycle_runner.is_absolute() or lifecycle_runner.is_symlink() or
        not lifecycle_runner.is_file() or not os.access(lifecycle_runner, os.X_OK) or
        not isinstance(lifecycle_digest, str) or
        not re.fullmatch(r"[0-9a-f]{64}", lifecycle_digest) or
        digest_file(lifecycle_runner) != lifecycle_digest):
    reject(f"{expected_backend} sealed lifecycle runner identity mismatch")
print(run_id)
print(orchestrator)
print(claimed)
print(lifecycle_runner)
print(lifecycle_digest)
PY
}

verify_orchestrator_identity() {
  local orchestrator=$1
  local expected_digest=$2
  python3 - "$orchestrator" "$expected_digest" <<'PY'
import hashlib
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
expected = sys.argv[2]
if (not path.is_absolute() or path.is_symlink() or not path.is_file() or
        not os.access(path, os.X_OK)):
    raise SystemExit("run-isolated-paper-performance: sealed orchestrator identity changed before combine")
if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
    raise SystemExit("run-isolated-paper-performance: sealed orchestrator SHA-256 changed before combine")
PY
}

verify_lifecycle_identity() {
  local lifecycle_runner=$1
  local expected_digest=$2
  python3 - "$lifecycle_runner" "$expected_digest" <<'PY'
import hashlib
import os
import sys
from pathlib import Path

path = Path(sys.argv[1])
expected = sys.argv[2]
if (not path.is_absolute() or path.is_symlink() or not path.is_file() or
        not os.access(path, os.X_OK)):
    raise SystemExit("run-isolated-paper-performance: sealed lifecycle runner identity changed")
if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
    raise SystemExit("run-isolated-paper-performance: sealed lifecycle runner SHA-256 changed")
PY
}

normalize_runtime_evidence() {
  local expected_backend=$1
  local expected_run_id=$2
  local evidence_path=$3
  local normalized_path=$4
  python3 - "$expected_backend" "$expected_run_id" "$evidence_path" "$normalized_path" <<'PY'
import json
import os
import re
import sys
from pathlib import Path

expected_backend, expected_run_id, raw_evidence, raw_normalized = sys.argv[1:]
evidence_path = Path(raw_evidence)
normalized_path = Path(raw_normalized)

def reject(message):
    raise SystemExit(f"run-isolated-paper-performance: {message}")

def nonempty(value):
    return isinstance(value, str) and bool(value.strip())

if evidence_path.is_symlink() or not evidence_path.is_file():
    reject(f"{expected_backend} lifecycle runner did not emit a regular runtime evidence file")
try:
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
except Exception as error:
    reject(f"{expected_backend} runtime evidence is invalid JSON: {error}")
if not isinstance(evidence, dict) or set(evidence) != {
    "schema_version", "selected_backend", "run_id", "managed_processes"
} or evidence.get("schema_version") != 1:
    reject(f"{expected_backend} runtime evidence schema is invalid")
if evidence.get("selected_backend") != expected_backend:
    reject(f"{expected_backend} runtime evidence selected_backend mismatch")
if evidence.get("run_id") != expected_run_id:
    reject(f"{expected_backend} runtime evidence run_id mismatch")
processes = evidence.get("managed_processes")
if not isinstance(processes, list) or not processes:
    reject(f"{expected_backend} runtime evidence process inventory is empty")
role_counts = {"gateway": 0, "data_node": 0, "backend_service": 0}
identities = set()
for process in processes:
    if not isinstance(process, dict) or set(process) != {
        "backend", "role", "identity", "executable", "executable_sha256", "probe"
    }:
        reject(f"{expected_backend} runtime evidence process schema is invalid")
    role = process.get("role")
    if process.get("backend") != expected_backend or role not in role_counts:
        reject(f"{expected_backend} runtime evidence process backend or role is invalid")
    role_counts[role] += 1
    identity = process.get("identity")
    if not isinstance(identity, dict) or set(identity) != {
        "host_id", "boot_id", "process_start_id", "pid"
    } or any(not nonempty(identity.get(name)) for name in (
        "host_id", "boot_id", "process_start_id"
    )) or type(identity.get("pid")) is not int or identity["pid"] <= 1:
        reject(f"{expected_backend} runtime evidence process identity is invalid")
    identity_key = (
        identity["host_id"], identity["boot_id"],
        identity["process_start_id"], identity["pid"],
    )
    if identity_key in identities:
        reject(f"{expected_backend} runtime evidence contains duplicate process identities")
    identities.add(identity_key)
    executable = process.get("executable")
    digest = process.get("executable_sha256")
    if (not nonempty(executable) or not Path(executable).is_absolute() or
            not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest)):
        reject(f"{expected_backend} runtime evidence executable identity is invalid")
    probe = process.get("probe")
    if not isinstance(probe, dict):
        reject(f"{expected_backend} runtime evidence probe is invalid")
    if probe.get("kind") == "local":
        if set(probe) != {"kind"}:
            reject(f"{expected_backend} local runtime evidence probe is invalid")
    elif probe.get("kind") == "remote":
        if set(probe) != {"kind", "ssh_target", "probe_binary", "network_interface"}:
            reject(f"{expected_backend} remote runtime evidence probe is invalid")
        if (not nonempty(probe.get("ssh_target")) or
                not nonempty(probe.get("probe_binary")) or
                not Path(probe["probe_binary"]).is_absolute() or
                not nonempty(probe.get("network_interface"))):
            reject(f"{expected_backend} remote runtime evidence probe is invalid")
    else:
        reject(f"{expected_backend} runtime evidence probe kind is invalid")
if role_counts["gateway"] < 1 or role_counts["data_node"] < 1:
    reject(f"{expected_backend} runtime evidence requires gateway and data_node identities")
if expected_backend == "rocksdb" and role_counts["backend_service"] != 0:
    reject("rocksdb runtime evidence must not contain backend_service")
if expected_backend != "rocksdb" and role_counts["backend_service"] != 1:
    reject(f"{expected_backend} runtime evidence requires exactly one backend_service")
if normalized_path.exists():
    reject(f"{expected_backend} normalized runtime evidence already exists")
normalized_path.write_text(
    json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n",
    encoding="utf-8",
)
os.chmod(normalized_path, 0o400)
PY
}

validate_runtime_evidence_bindings() {
  local expected_backend=$1
  local bundle=$2
  local artifact=$3
  local evidence_path=$4
  python3 - "$expected_backend" "$bundle" "$artifact" "$evidence_path" <<'PY'
import json
import sys
from pathlib import Path

expected_backend, raw_bundle, raw_artifact, raw_evidence = sys.argv[1:]
bundle = Path(raw_bundle)
artifact = Path(raw_artifact)
evidence_path = Path(raw_evidence)

def reject(message):
    raise SystemExit(f"run-isolated-paper-performance: {message}")

try:
    runtime = json.loads((bundle / "runtime-manifest.json").read_text(encoding="utf-8"))
    prepared = json.loads(
        (bundle / "managed-process-evidence.json").read_text(encoding="utf-8")
    )
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
except Exception as error:
    reject(f"{expected_backend} runtime binding input is invalid: {error}")
processes = evidence["managed_processes"]
actual_gateways = {
    (process["identity"]["pid"], process["executable"], process["executable_sha256"])
    for process in processes if process["role"] == "gateway"
}
expected_gateways = {
    (target.get("gateway_process", {}).get("pid"),
     target.get("gateway_process", {}).get("executable"))
    for target in runtime.get("proxy_targets", [])
}
if (not expected_gateways or
        {(pid, executable) for pid, executable, _ in actual_gateways} != expected_gateways or
        len(actual_gateways) != sum(process["role"] == "gateway" for process in processes)):
    reject(f"{expected_backend} runtime gateway identity is not bound to the sealed runtime manifest")
prepared_gateways = {
    (process.get("identity", {}).get("pid"), process.get("executable"),
     process.get("executable_sha256"))
    for process in prepared.get("managed_processes", [])
    if process.get("role") == "gateway"
}
if actual_gateways != prepared_gateways:
    reject(f"{expected_backend} runtime gateway executable is not bound to preparation evidence")

manifest_data_nodes = {
    (
        process.get("pid"), process.get("executable"),
        process.get("executable_sha256"), process.get("probe", {}).get("host_id"),
        process.get("probe", {}).get("data_interface"),
    )
    for target in runtime.get("proxy_targets", [])
    for process in target.get("data_node_processes", [])
}
observed_data_nodes = set()
proxy_observations = 0
raw_directory = artifact / "raw"
if not raw_directory.is_dir() or raw_directory.is_symlink():
    reject(f"{expected_backend} verified artifact has no regular raw directory")
for path in sorted(raw_directory.iterdir()):
    if path.is_symlink() or not path.is_file():
        reject(f"{expected_backend} verified artifact raw entry is not a regular file")
    try:
        observation = json.loads(path.read_text(encoding="utf-8"))
    except Exception as error:
        reject(f"{expected_backend} verified raw observation is invalid: {error}")
    if observation.get("path") != "proxy":
        continue
    proxy_observations += 1
    if observation.get("backend") != expected_backend:
        reject(f"{expected_backend} verified Proxy observation backend mismatch")
    topology = observation.get("topology_evidence")
    if not isinstance(topology, dict) or topology.get("deployment_mode") != "remote_formal":
        reject(f"{expected_backend} verified Proxy observation lacks remote topology evidence")
    nodes = topology.get("data_nodes")
    if not isinstance(nodes, list) or not nodes:
        reject(f"{expected_backend} verified Proxy observation has no data-node identities")
    for node in nodes:
        observed_data_nodes.add((
            node.get("host_id"), node.get("boot_id"), node.get("process_start_id"),
            node.get("pid"), node.get("executable"), node.get("executable_sha256"),
            node.get("data_interface"),
        ))
if proxy_observations == 0:
    reject(f"{expected_backend} verified artifact has no Proxy observations")
actual_data_nodes = {
    (
        process["identity"]["host_id"], process["identity"]["boot_id"],
        process["identity"]["process_start_id"], process["identity"]["pid"],
        process["executable"], process["executable_sha256"],
        process["probe"].get("network_interface"),
    )
    for process in processes if process["role"] == "data_node"
}
if (actual_data_nodes != observed_data_nodes or
        len(actual_data_nodes) != sum(process["role"] == "data_node" for process in processes)):
    reject(f"{expected_backend} runtime data_node identity is not bound to verified Proxy observations")
for host_id, _, _, pid, executable, digest, interface in actual_data_nodes:
    if (pid, executable, digest, host_id, interface) not in manifest_data_nodes:
        reject(f"{expected_backend} runtime data_node identity is not bound to the sealed runtime manifest")
PY
}

verify_combined_package() {
  local combined=$1
  python3 - "$combined" <<'PY'
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path

combined = Path(sys.argv[1])
expected_files = {"combined-report.json", "combined-summary.csv", "isolation-evidence.json"}
checksum_path = combined / "SHA256SUMS"
if not combined.is_dir() or not checksum_path.is_file() or checksum_path.is_symlink():
    raise SystemExit("run-isolated-paper-performance: combined package is incomplete")
entries = {}
for line in checksum_path.read_text(encoding="ascii").splitlines():
    match = re.fullmatch(r"([0-9a-f]{64})  ([^/]+)", line)
    if not match or match.group(2) in entries:
        raise SystemExit("run-isolated-paper-performance: combined checksums are invalid")
    entries[match.group(2)] = match.group(1)
directory_entries = {path.name: path for path in combined.iterdir()}
expected_entries = expected_files | {"SHA256SUMS"}
if (set(entries) != expected_files or set(directory_entries) != expected_entries or
        any(not stat.S_ISREG(path.lstat().st_mode) for path in directory_entries.values())):
    raise SystemExit("run-isolated-paper-performance: combined file set is invalid")
for name, expected in entries.items():
    if hashlib.sha256((combined / name).read_bytes()).hexdigest() != expected:
        raise SystemExit(f"run-isolated-paper-performance: combined checksum mismatch: {name}")
try:
    report = json.loads((combined / "combined-report.json").read_text(encoding="utf-8"))
except Exception as error:
    raise SystemExit(f"run-isolated-paper-performance: combined report JSON is invalid: {error}")
if (report.get("schema_version") != 1 or
        [entry.get("backend") for entry in report.get("backends", [])] !=
        ["rocksdb", "postgresql", "neo4j"]):
    raise SystemExit("run-isolated-paper-performance: combined backend order/schema is invalid")
lines = (combined / "combined-summary.csv").read_text(encoding="utf-8").splitlines()
if not lines or not lines[0].startswith("backend_run,"):
    raise SystemExit("run-isolated-paper-performance: combined CSV lacks backend_run prefix")
try:
    isolation = json.loads((combined / "isolation-evidence.json").read_text(encoding="utf-8"))
except Exception as error:
    raise SystemExit(f"run-isolated-paper-performance: isolation evidence JSON is invalid: {error}")
runs = isolation.get("runs") if isinstance(isolation, dict) else None
if (set(isolation) != {"schema_version", "runs"} or isolation.get("schema_version") != 1 or
        not isinstance(runs, list) or
        [entry.get("backend") for entry in runs] != ["rocksdb", "postgresql", "neo4j"]):
    raise SystemExit("run-isolated-paper-performance: isolation evidence schema/order is invalid")
for entry in runs:
    if (not isinstance(entry, dict) or set(entry) != {
            "backend", "run_id", "artifact_sha256", "runtime_evidence_sha256"
        } or not isinstance(entry.get("run_id"), str) or not entry["run_id"] or
            not re.fullmatch(r"[0-9a-f]{64}", entry.get("artifact_sha256", "")) or
            not re.fullmatch(r"[0-9a-f]{64}", entry.get("runtime_evidence_sha256", ""))):
        raise SystemExit("run-isolated-paper-performance: isolation evidence binding is invalid")
for entry in report["backends"]:
    artifact = Path(entry.get("artifact", ""))
    if (not artifact.is_absolute() or artifact.is_symlink() or not artifact.is_dir() or
            not (artifact / "SHA256SUMS").is_file() or
            hashlib.sha256((artifact / "SHA256SUMS").read_bytes()).hexdigest() !=
            entry.get("artifact_sha256")):
        raise SystemExit("run-isolated-paper-performance: combined artifact path binding is invalid")
PY
}

sha256_file() {
  python3 - "$1" <<'PY'
import hashlib
import sys
from pathlib import Path

digest = hashlib.sha256()
with Path(sys.argv[1]).open("rb") as source:
    for block in iter(lambda: source.read(1024 * 1024), b""):
        digest.update(block)
print(digest.hexdigest())
PY
}

snapshot_verified_artifact() {
  local source=$1
  local destination=$2
  python3 - "$source" "$destination" <<'PY'
import os
import shutil
import stat
import sys
from pathlib import Path

source = Path(sys.argv[1])
destination = Path(sys.argv[2])
if destination.exists() or destination.is_symlink():
    raise SystemExit("run-isolated-paper-performance: verified artifact snapshot already exists")
for directory, dirnames, filenames in os.walk(source, followlinks=False):
    directory_path = Path(directory)
    for name in dirnames:
        path = directory_path / name
        if path.is_symlink() or not stat.S_ISDIR(path.lstat().st_mode):
            raise SystemExit("run-isolated-paper-performance: verified artifact contains an unsupported directory entry")
    for name in filenames:
        path = directory_path / name
        if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
            raise SystemExit("run-isolated-paper-performance: verified artifact contains an unsupported file entry")
shutil.copytree(source, destination, symlinks=True)
for directory, _, filenames in os.walk(destination, followlinks=False):
    for name in filenames:
        (Path(directory) / name).chmod(0o400)
    Path(directory).chmod(0o500)
PY
}

verify_artifact_checksum_inventory() {
  local artifact=$1
  python3 - "$artifact" <<'PY'
import hashlib
import os
import re
import stat
import sys
from pathlib import Path, PurePosixPath

artifact = Path(sys.argv[1])
checksum_path = artifact / "SHA256SUMS"
if checksum_path.is_symlink() or not checksum_path.is_file():
    raise SystemExit("run-isolated-paper-performance: artifact checksum inventory is missing")
entries = {}
for line in checksum_path.read_text(encoding="ascii").splitlines():
    match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
    if not match:
        raise SystemExit("run-isolated-paper-performance: artifact checksum inventory is invalid")
    relative = PurePosixPath(match.group(2))
    if (relative.is_absolute() or any(part in ("", ".", "..") for part in relative.parts) or
            relative.as_posix() in entries):
        raise SystemExit("run-isolated-paper-performance: artifact checksum inventory is invalid")
    entries[relative.as_posix()] = match.group(1)
actual = set()
for directory, dirnames, filenames in os.walk(artifact, followlinks=False):
    directory_path = Path(directory)
    for name in dirnames:
        path = directory_path / name
        if path.is_symlink() or not stat.S_ISDIR(path.lstat().st_mode):
            raise SystemExit("run-isolated-paper-performance: artifact contains an unsupported entry")
    for name in filenames:
        path = directory_path / name
        relative = path.relative_to(artifact).as_posix()
        if path.is_symlink() or not stat.S_ISREG(path.lstat().st_mode):
            raise SystemExit("run-isolated-paper-performance: artifact contains an unsupported entry")
        if relative != "SHA256SUMS":
            actual.add(relative)
if actual != set(entries):
    raise SystemExit("run-isolated-paper-performance: artifact checksum file set mismatch")
for relative, expected in entries.items():
    digest = hashlib.sha256((artifact / relative).read_bytes()).hexdigest()
    if digest != expected:
        raise SystemExit(f"run-isolated-paper-performance: artifact checksum mismatch: {relative}")
PY
}

remove_combined_output() {
  local combined=$1
  python3 - "$combined" <<'PY'
import shutil
import sys
from pathlib import Path

combined = Path(sys.argv[1])
if combined.name != "combined":
    raise SystemExit("refusing to remove an unexpected combined output path")
if combined.exists():
    shutil.rmtree(combined)
PY
}

publish_isolation_binding() {
  local combined=$1
  shift
  python3 - "$combined" "$@" <<'PY'
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path

combined = Path(sys.argv[1])
values = sys.argv[2:]
if len(values) != 18:
    raise SystemExit("run-isolated-paper-performance: isolation binding arguments are invalid")
expected_initial = {"combined-report.json", "combined-summary.csv"}
checksum_path = combined / "SHA256SUMS"
if not combined.is_dir() or checksum_path.is_symlink() or not checksum_path.is_file():
    raise SystemExit("run-isolated-paper-performance: combined package is incomplete before binding")
entries = {}
for line in checksum_path.read_text(encoding="ascii").splitlines():
    match = re.fullmatch(r"([0-9a-f]{64})  ([^/]+)", line)
    if not match or match.group(2) in entries:
        raise SystemExit("run-isolated-paper-performance: combined checksums are invalid")
    entries[match.group(2)] = match.group(1)
directory_entries = {path.name: path for path in combined.iterdir()}
expected_entries = expected_initial | {"SHA256SUMS"}
if (set(entries) != expected_initial or set(directory_entries) != expected_entries or
        any(not stat.S_ISREG(path.lstat().st_mode) for path in directory_entries.values())):
    raise SystemExit("run-isolated-paper-performance: combined file set is invalid")
for name, expected in entries.items():
    if hashlib.sha256((combined / name).read_bytes()).hexdigest() != expected:
        raise SystemExit(f"run-isolated-paper-performance: combined checksum mismatch: {name}")
isolation_path = combined / "isolation-evidence.json"
if isolation_path.exists() or isolation_path.is_symlink():
    raise SystemExit("run-isolated-paper-performance: isolation evidence already exists")
runs = []
try:
    report = json.loads((combined / "combined-report.json").read_text(encoding="utf-8"))
except Exception as error:
    raise SystemExit(f"run-isolated-paper-performance: combined report JSON is invalid: {error}")
report_backends = report.get("backends") if isinstance(report, dict) else None
if (not isinstance(report, dict) or report.get("schema_version") != 1 or
        not isinstance(report_backends, list) or
        len(report_backends) != 3):
    raise SystemExit("run-isolated-paper-performance: combined report schema is invalid before path binding")
for entry_index, index in enumerate(range(0, len(values), 6)):
    backend, run_id, artifact_digest, evidence_digest, sealed_path, persistent_path = \
        values[index:index + 6]
    if (backend not in ("rocksdb", "postgresql", "neo4j") or not run_id or
            not re.fullmatch(r"[0-9a-f]{64}", artifact_digest) or
            not re.fullmatch(r"[0-9a-f]{64}", evidence_digest)):
        raise SystemExit("run-isolated-paper-performance: isolation binding value is invalid")
    runs.append({
        "backend": backend,
        "run_id": run_id,
        "artifact_sha256": artifact_digest,
        "runtime_evidence_sha256": evidence_digest,
    })
    entry = report_backends[entry_index]
    sealed = Path(sealed_path)
    persistent = Path(persistent_path)
    try:
        sealed_canonical = sealed.resolve(strict=True)
        persistent_canonical = persistent.resolve(strict=True)
        report_canonical = Path(entry.get("artifact", "")).resolve(strict=True)
    except Exception as error:
        raise SystemExit(f"run-isolated-paper-performance: combined artifact path binding failed: {error}")
    if (entry.get("backend") != backend or
            entry.get("verification", {}).get("run_id") != run_id or
            entry.get("artifact_sha256") != artifact_digest or
            report_canonical != sealed_canonical or not persistent_canonical.is_dir() or
            hashlib.sha256((persistent_canonical / "SHA256SUMS").read_bytes()).hexdigest() !=
            artifact_digest):
        raise SystemExit("run-isolated-paper-performance: combined artifact path binding is invalid")
    entry["artifact"] = str(persistent_canonical)
if [entry["backend"] for entry in runs] != ["rocksdb", "postgresql", "neo4j"]:
    raise SystemExit("run-isolated-paper-performance: isolation binding backend order is invalid")
report_bytes = (json.dumps(report, indent=2, sort_keys=True) + "\n").encode("utf-8")
report_path = combined / "combined-report.json"
report_temp = combined / ".combined-report.path-binding.tmp"
try:
    descriptor = os.open(report_temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
except FileExistsError:
    raise SystemExit("run-isolated-paper-performance: combined report path-binding temp exists")
with os.fdopen(descriptor, "wb") as destination:
    destination.write(report_bytes)
    destination.flush()
    os.fsync(destination.fileno())
os.replace(report_temp, report_path)
isolation_bytes = (
    json.dumps({"schema_version": 1, "runs": runs}, indent=2, sort_keys=True) + "\n"
).encode("utf-8")
try:
    descriptor = os.open(isolation_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o400)
except FileExistsError:
    raise SystemExit("run-isolated-paper-performance: isolation evidence already exists")
with os.fdopen(descriptor, "wb") as destination:
    destination.write(isolation_bytes)
    destination.flush()
    os.fsync(destination.fileno())
checksum_lines = []
for path in sorted((combined / name for name in (
        "combined-report.json", "combined-summary.csv", "isolation-evidence.json"
)), key=lambda value: value.name):
    checksum_lines.append(f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n")
checksum_path.write_text("".join(checksum_lines), encoding="ascii")
PY
}

check_managed_processes_released() {
  local expected_backend=$1
  local evidence_path=$2
  python3 - "$expected_backend" "$evidence_path" <<'PY'
import json
import os
import socket
import subprocess
import sys
from pathlib import Path

expected_backend, evidence_path = sys.argv[1:]

def reject(message):
    raise SystemExit(f"run-isolated-paper-performance: {message}")

evidence = json.loads(Path(evidence_path).read_text(encoding="utf-8"))
ssh_options = [
    "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5",
    "-o", "ConnectionAttempts=1", "-o", "ServerAliveInterval=5",
    "-o", "ServerAliveCountMax=1",
]
seen = set()
for process in evidence.get("managed_processes", []):
    if process.get("backend") != expected_backend:
        reject(f"{expected_backend} managed process backend identity mismatch")
    previous = process.get("identity", {})
    probe = process.get("probe", {})
    pid = previous.get("pid")
    identity = (
        process.get("role"), pid, previous.get("host_id"), previous.get("boot_id"),
        previous.get("process_start_id"),
    )
    if identity in seen:
        continue
    seen.add(identity)
    if (process.get("role") not in ("gateway", "data_node", "backend_service") or
            not isinstance(pid, int) or pid <= 1 or
            not all(isinstance(value, str) and value for value in identity[0:1] + identity[2:])):
        reject(f"{expected_backend} managed process identity is invalid")
    if probe.get("kind") == "local":
        try:
            if sys.platform == "linux":
                stat_path = Path(f"/proc/{pid}/stat")
                if not stat_path.exists():
                    continue
                stat_line = stat_path.read_text(encoding="ascii")
                fields = stat_line[stat_line.rfind(")") + 2:].split()
                current_start = f"linux-start-ticks:{fields[19]}"
                current_boot = Path("/proc/sys/kernel/random/boot_id").read_text(
                    encoding="ascii"
                ).strip()
            elif sys.platform == "darwin":
                start = subprocess.run(
                    ["ps", "-p", str(pid), "-o", "lstart="],
                    capture_output=True, text=True, check=False,
                )
                if start.returncode == 1:
                    continue
                if start.returncode != 0:
                    reject(f"{expected_backend} cannot inspect exact local managed PID")
                current_start = start.stdout.strip()
                current_boot = subprocess.check_output(
                    ["sysctl", "-n", "kern.boottime"], text=True
                ).strip()
            else:
                reject(f"local managed PID identity is unsupported on {sys.platform}")
        except SystemExit:
            raise
        except Exception as error:
            reject(f"{expected_backend} local managed PID identity check failed: {error}")
        current = {
            "pid": pid,
            "host_id": socket.gethostname(),
            "boot_id": current_boot,
            "process_start_id": current_start,
        }
    elif probe.get("kind") == "remote":
        ssh_target = probe.get("ssh_target")
        probe_binary = probe.get("probe_binary")
        network_interface = probe.get("network_interface")
        if (not isinstance(ssh_target, str) or not ssh_target or
                not isinstance(probe_binary, str) or not probe_binary.startswith("/") or
                not isinstance(network_interface, str) or not network_interface):
            reject(f"{expected_backend} remote managed process probe is invalid")
        command = [
            "ssh", *ssh_options, "--", ssh_target, probe_binary, "probe-process",
            "--pid", str(pid), "--network-interface", network_interface,
        ]
        try:
            completed = subprocess.run(
                command, capture_output=True, text=True, timeout=20, check=False,
            )
        except subprocess.TimeoutExpired:
            reject(f"{expected_backend} managed PID identity probe timed out")
        if completed.returncode != 0:
            exact_pid = subprocess.run(
                ["ssh", *ssh_options, "--", ssh_target, "ps", "-p", str(pid), "-o", "pid="],
                capture_output=True, text=True, timeout=10, check=False,
            )
            if exact_pid.returncode == 0 and exact_pid.stdout.strip() == str(pid):
                reject(f"{expected_backend} managed PID is still live but its start identity cannot be verified")
            if exact_pid.returncode != 1:
                reject(f"{expected_backend} cannot prove the exact managed PID exited")
            continue
        try:
            current = json.loads(completed.stdout)
        except Exception as error:
            reject(f"{expected_backend} managed PID identity probe returned invalid JSON: {error}")
        if current.get("pid") != pid:
            reject(f"{expected_backend} managed PID identity probe returned a different PID")
    else:
        reject(f"{expected_backend} managed process probe kind is invalid")
    for name in ("host_id", "boot_id", "process_start_id"):
        if not isinstance(current.get(name), str) or not current[name].strip():
            reject(f"{expected_backend} managed PID identity probe returned invalid {name}")
    if current.get("host_id") != previous.get("host_id"):
        reject(f"{expected_backend} managed PID probe resolved to a different host identity")
    if (current.get("boot_id") == previous.get("boot_id") and
            current.get("process_start_id") == previous.get("process_start_id")):
        reject(f"{expected_backend} managed PID is still the same process")
PY
}

declare -a backends=(rocksdb postgresql neo4j)
declare -a bundles=("$rocksdb_bundle" "$postgresql_bundle" "$neo4j_bundle")
declare -a run_ids=()
declare -a orchestrators=()
declare -a orchestrator_digests=()
declare -a lifecycle_runners=()
declare -a lifecycle_digests=()
declare -a artifacts=()
declare -a sealed_artifacts=()
declare -a runtime_evidence_files=()
declare -a artifact_digests=()
declare -a runtime_evidence_digests=()

for index in 0 1 2; do
  backend=${backends[$index]}
  bundle=${bundles[$index]}
  metadata="$scratch/$backend.metadata"
  bundle_metadata "$backend" "$bundle" >"$metadata"
  {
    IFS= read -r run_id
    IFS= read -r orchestrator
    IFS= read -r orchestrator_digest
    IFS= read -r lifecycle_runner
    IFS= read -r lifecycle_digest
  } <"$metadata"
  run_ids+=("$run_id")
  orchestrators+=("$orchestrator")
  orchestrator_digests+=("$orchestrator_digest")
  lifecycle_runners+=("$lifecycle_runner")
  lifecycle_digests+=("$lifecycle_digest")
done
if [[ ${orchestrator_digests[0]} != "${orchestrator_digests[1]}" ||
      ${orchestrator_digests[0]} != "${orchestrator_digests[2]}" ]]; then
  printf '%s\n' 'all prepared bundles must seal the same orchestrator SHA-256' >&2
  exit 1
fi
python3 - \
  "${run_ids[0]}" "${run_ids[1]}" "${run_ids[2]}" <<'PY'
import sys

run_ids = sys.argv[1:4]
if len(set(run_ids)) != 3:
    raise SystemExit("run-isolated-paper-performance: prepared bundles must have distinct run_id values")
PY

preflight_all() {
  local index
  for index in 0 1 2; do
    verify_lifecycle_identity "${lifecycle_runners[$index]}" "${lifecycle_digests[$index]}"
    "${lifecycle_runners[$index]}" preflight --prepared-bundle "${bundles[$index]}"
  done
}

preflight_all

for index in 0 1 2; do
  backend=${backends[$index]}
  bundle=${bundles[$index]}
  run_id=${run_ids[$index]}
  artifact="$output_root/$run_id"
  [[ ! -e $artifact ]] || {
    printf 'artifact output already exists: %s\n' "$artifact" >&2
    exit 1
  }
  evidence_output="$scratch/$backend.runtime-evidence.json"
  normalized_evidence="$scratch/$backend.runtime-evidence.normalized.json"
  [[ ! -e $evidence_output && ! -e $normalized_evidence ]] || {
    printf 'runtime evidence output already exists for %s\n' "$backend" >&2
    exit 1
  }
  verify_lifecycle_identity "${lifecycle_runners[$index]}" "${lifecycle_digests[$index]}"
  "${lifecycle_runners[$index]}" run \
    --prepared-bundle "$bundle" \
    --output-root "$output_root" \
    --evidence-output "$evidence_output"
  normalize_runtime_evidence "$backend" "$run_id" "$evidence_output" "$normalized_evidence"
  [[ -d $artifact ]] || {
    printf 'sealed run did not create its declared artifact: %s\n' "$artifact" >&2
    exit 1
  }
  regenerate_dir="$output_root/verification/$run_id"
  "$verify_script" --prepared-bundle "$bundle" \
    --artifact "$artifact" --regenerate-dir "$regenerate_dir"
  validate_runtime_evidence_bindings "$backend" "$bundle" "$artifact" "$normalized_evidence"
  check_managed_processes_released "$backend" "$normalized_evidence"
  sealed_artifact="$scratch/sealed-artifacts/$run_id"
  mkdir -p "$scratch/sealed-artifacts"
  snapshot_verified_artifact "$artifact" "$sealed_artifact"
  runtime_evidence_files+=("$normalized_evidence")
  artifact_digests+=("$(sha256_file "$sealed_artifact/SHA256SUMS")")
  runtime_evidence_digests+=("$(sha256_file "$normalized_evidence")")
  artifacts+=("$artifact")
  sealed_artifacts+=("$sealed_artifact")
  preflight_all
done

verify_orchestrator_identity "${orchestrators[0]}" "${orchestrator_digests[0]}"
"${orchestrators[0]}" combine \
  --rocksdb "${sealed_artifacts[0]}" \
  --postgresql "${sealed_artifacts[1]}" \
  --neo4j "${sealed_artifacts[2]}" \
  --output "$output_root/combined"
for index in 0 1 2; do
  final_regenerate_dir="$output_root/verification-final/${run_ids[$index]}"
  if ! "$verify_script" --prepared-bundle "${bundles[$index]}" \
      --artifact "${sealed_artifacts[$index]}" --regenerate-dir "$final_regenerate_dir"; then
    printf '%s\n' "run-isolated-paper-performance: ${backends[$index]} artifact verification failed before publication" >&2
    remove_combined_output "$output_root/combined"
    exit 1
  fi
done
for index in 0 1 2; do
  if ! verify_artifact_checksum_inventory "${sealed_artifacts[$index]}"; then
    printf '%s\n' "run-isolated-paper-performance: ${backends[$index]} artifact checksum verification failed before publication" >&2
    remove_combined_output "$output_root/combined"
    exit 1
  fi
  if [[ $(sha256_file "${sealed_artifacts[$index]}/SHA256SUMS") != "${artifact_digests[$index]}" ]]; then
    printf '%s\n' "run-isolated-paper-performance: ${backends[$index]} artifact SHA256SUMS changed before publication" >&2
    remove_combined_output "$output_root/combined"
    exit 1
  fi
  if ! verify_artifact_checksum_inventory "${artifacts[$index]}"; then
    printf '%s\n' "run-isolated-paper-performance: ${backends[$index]} persistent artifact checksum verification failed before publication" >&2
    remove_combined_output "$output_root/combined"
    exit 1
  fi
  if [[ $(sha256_file "${artifacts[$index]}/SHA256SUMS") != "${artifact_digests[$index]}" ]]; then
    printf '%s\n' "run-isolated-paper-performance: ${backends[$index]} persistent artifact differs from the sealed snapshot" >&2
    remove_combined_output "$output_root/combined"
    exit 1
  fi
  if [[ $(sha256_file "${runtime_evidence_files[$index]}") != "${runtime_evidence_digests[$index]}" ]]; then
    printf '%s\n' "run-isolated-paper-performance: ${backends[$index]} runtime evidence changed before publication" >&2
    remove_combined_output "$output_root/combined"
    exit 1
  fi
done
if ! publish_isolation_binding "$output_root/combined" \
  rocksdb "${run_ids[0]}" "${artifact_digests[0]}" "${runtime_evidence_digests[0]}" \
    "${sealed_artifacts[0]}" "${artifacts[0]}" \
  postgresql "${run_ids[1]}" "${artifact_digests[1]}" "${runtime_evidence_digests[1]}" \
    "${sealed_artifacts[1]}" "${artifacts[1]}" \
  neo4j "${run_ids[2]}" "${artifact_digests[2]}" "${runtime_evidence_digests[2]}" \
    "${sealed_artifacts[2]}" "${artifacts[2]}"; then
  remove_combined_output "$output_root/combined"
  exit 1
fi
if ! verify_combined_package "$output_root/combined"; then
  remove_combined_output "$output_root/combined"
  exit 1
fi
for index in 0 1 2; do
  if ! verify_artifact_checksum_inventory "${artifacts[$index]}"; then
    printf '%s\n' "run-isolated-paper-performance: ${backends[$index]} persistent artifact checksum verification failed after combined validation" >&2
    remove_combined_output "$output_root/combined"
    exit 1
  fi
done

printf 'combined isolated backend report: %s\n' "$output_root/combined"
