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
trap 'rm -rf "$scratch"' EXIT

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
if (evidence.get("selected_backend") != expected_backend or not processes or
        roles != {"gateway", "data_node"} or
        any(process.get("backend") != expected_backend for process in processes)):
    reject(f"{expected_backend} managed process evidence mismatch")
backend_service = evidence.get("backend_service")
if expected_backend == "rocksdb":
    if backend_service != {"ownership": "embedded", "managed_by": "data_node"}:
        reject("rocksdb backend service ownership evidence is invalid")
elif (not isinstance(backend_service, dict) or
      backend_service.get("ownership") != "external" or
      backend_service.get("managed") is not False or
      not str(backend_service.get("reason", "")).strip()):
    reject(f"{expected_backend} external backend service ownership evidence is invalid")
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
print(run_id)
print(orchestrator)
print(claimed)
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

verify_combined_package() {
  local combined=$1
  python3 - "$combined" <<'PY'
import hashlib
import json
import re
import sys
from pathlib import Path

combined = Path(sys.argv[1])
expected_files = {"combined-report.json", "combined-summary.csv"}
checksum_path = combined / "SHA256SUMS"
if not combined.is_dir() or not checksum_path.is_file() or checksum_path.is_symlink():
    raise SystemExit("run-isolated-paper-performance: combined package is incomplete")
entries = {}
for line in checksum_path.read_text(encoding="ascii").splitlines():
    match = re.fullmatch(r"([0-9a-f]{64})  ([^/]+)", line)
    if not match or match.group(2) in entries:
        raise SystemExit("run-isolated-paper-performance: combined checksums are invalid")
    entries[match.group(2)] = match.group(1)
actual_files = {
    path.name for path in combined.iterdir()
    if path.is_file() and path.name != "SHA256SUMS"
}
if set(entries) != expected_files or actual_files != expected_files:
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
PY
}

check_managed_processes_released() {
  local expected_backend=$1
  local bundle=$2
  python3 - "$expected_backend" "$bundle/managed-process-evidence.json" <<'PY'
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
    if (process.get("role") not in ("gateway", "data_node") or
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
    same_process = (
        current.get("host_id") == previous.get("host_id") and
        current.get("boot_id") == previous.get("boot_id") and
        current.get("process_start_id") == previous.get("process_start_id")
    )
    if same_process:
        reject(f"{expected_backend} managed PID is still the same process")
PY
}

declare -a backends=(rocksdb postgresql neo4j)
declare -a bundles=("$rocksdb_bundle" "$postgresql_bundle" "$neo4j_bundle")
declare -a run_ids=()
declare -a orchestrators=()
declare -a orchestrator_digests=()
declare -a artifacts=()

for index in 0 1 2; do
  backend=${backends[$index]}
  bundle=${bundles[$index]}
  metadata="$scratch/$backend.metadata"
  bundle_metadata "$backend" "$bundle" >"$metadata"
  {
    IFS= read -r run_id
    IFS= read -r orchestrator
    IFS= read -r orchestrator_digest
  } <"$metadata"
  run_ids+=("$run_id")
  orchestrators+=("$orchestrator")
  orchestrator_digests+=("$orchestrator_digest")
done
if [[ ${orchestrator_digests[0]} != "${orchestrator_digests[1]}" ||
      ${orchestrator_digests[0]} != "${orchestrator_digests[2]}" ]]; then
  printf '%s\n' 'all prepared bundles must seal the same orchestrator SHA-256' >&2
  exit 1
fi

for index in 0 1 2; do
  backend=${backends[$index]}
  bundle=${bundles[$index]}
  run_id=${run_ids[$index]}
  artifact="$output_root/$run_id"
  [[ ! -e $artifact ]] || {
    printf 'artifact output already exists: %s\n' "$artifact" >&2
    exit 1
  }
  "$run_script" --prepared-bundle "$bundle" --output-root "$output_root"
  [[ -d $artifact ]] || {
    printf 'sealed run did not create its declared artifact: %s\n' "$artifact" >&2
    exit 1
  }
  regenerate_dir="$output_root/verification/$run_id"
  "$verify_script" --prepared-bundle "$bundle" \
    --artifact "$artifact" --regenerate-dir "$regenerate_dir"
  check_managed_processes_released "$backend" "$bundle"
  artifacts+=("$artifact")
done

verify_orchestrator_identity "${orchestrators[0]}" "${orchestrator_digests[0]}"
"${orchestrators[0]}" combine \
  --rocksdb "${artifacts[0]}" \
  --postgresql "${artifacts[1]}" \
  --neo4j "${artifacts[2]}" \
  --output "$output_root/combined"
if ! verify_combined_package "$output_root/combined"; then
  python3 - "$output_root/combined" <<'PY'
import shutil
import sys
from pathlib import Path

combined = Path(sys.argv[1])
if combined.name != "combined":
    raise SystemExit("refusing to remove an unexpected combined output path")
if combined.exists():
    shutil.rmtree(combined)
PY
  exit 1
fi

printf 'combined isolated backend report: %s\n' "$output_root/combined"
