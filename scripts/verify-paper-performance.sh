#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/verify-paper-performance.sh --prepared-bundle DIR \
    --artifact DIR --regenerate-dir DIR

Performs offline checksum, schema, completeness, identity, required-metric,
and statistical verification with the orchestrator sealed by preparation.
Regenerated JSON/CSV figure data is written outside the immutable artifact.
EOF
}

prepared_bundle=
artifact=
regenerate_dir=

while (($# > 0)); do
  case "$1" in
    --prepared-bundle|--artifact|--regenerate-dir)
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

[[ -n $prepared_bundle ]] || { printf '%s\n' '--prepared-bundle is required' >&2; exit 2; }
[[ -n $artifact ]] || { printf '%s\n' '--artifact is required' >&2; exit 2; }
[[ -n $regenerate_dir ]] || { printf '%s\n' '--regenerate-dir is required' >&2; exit 2; }
command -v python3 >/dev/null || { printf '%s\n' 'python3 is required' >&2; exit 1; }

metadata=$(mktemp "${TMPDIR:-/tmp}/dtgproxy-paper-bundle.XXXXXX")
trap 'rm -f "$metadata"' EXIT
python3 - "$prepared_bundle" >"$metadata" <<'PY'
import hashlib
import json
import os
import re
import stat
import sys
from pathlib import Path, PurePosixPath

def reject(message):
    raise SystemExit(f"verify-paper-performance: {message}")

def digest_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()

def safe_relative(value):
    if not value or "\n" in value or "\r" in value or "\0" in value:
        return False
    path = PurePosixPath(value)
    return not path.is_absolute() and all(part not in ("", ".", "..") for part in path.parts)

raw_bundle = Path(sys.argv[1])
if raw_bundle.is_symlink():
    reject("prepared bundle must not be a symlink")
if not raw_bundle.is_dir():
    reject("--prepared-bundle must be an existing directory")
bundle = raw_bundle.resolve(strict=True)
checksum_path = bundle / "SHA256SUMS"
if checksum_path.is_symlink() or not checksum_path.is_file():
    reject("prepared bundle SHA256SUMS must be a regular file, not a symlink")

entries = {}
try:
    lines = checksum_path.read_text(encoding="ascii").splitlines()
except Exception as error:
    reject(f"cannot read SHA256SUMS: {error}")
for line in lines:
    match = re.fullmatch(r"([0-9a-f]{64})  (.+)", line)
    if not match or not safe_relative(match.group(2)):
        reject("invalid SHA256SUMS entry")
    relative = match.group(2)
    if relative == "SHA256SUMS" or relative in entries:
        reject("invalid duplicate or self-referential SHA256SUMS entry")
    entries[relative] = match.group(1)

actual_files = set()
for directory, dirnames, filenames in os.walk(bundle, followlinks=False):
    directory_path = Path(directory)
    for name in list(dirnames):
        path = directory_path / name
        if path.is_symlink():
            reject(f"prepared bundle contains symlink: {path.relative_to(bundle).as_posix()}")
        if not path.is_dir():
            reject(f"prepared bundle contains special file: {path.relative_to(bundle).as_posix()}")
    for name in filenames:
        path = directory_path / name
        relative = path.relative_to(bundle).as_posix()
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            reject(f"prepared bundle contains symlink: {relative}")
        if not stat.S_ISREG(mode):
            reject(f"prepared bundle contains special file: {relative}")
        if relative != "SHA256SUMS":
            actual_files.add(relative)

if actual_files != set(entries):
    reject("prepared bundle checksum file set mismatch")
for relative, expected in entries.items():
    if digest_file(bundle / relative) != expected:
        reject(f"prepared bundle checksum mismatch: {relative}")

try:
    ready = json.loads((bundle / "READY.json").read_text(encoding="utf-8"))
except Exception as error:
    reject(f"invalid sealed READY.json: {error}")
if ready.get("schema_version") != 1 or ready.get("status") != "prepared":
    reject("READY.json status must be prepared")
if ready.get("formal_matrix_executed") is not False:
    reject("READY.json formal_matrix_executed must be false")
selected_backend = ready.get("selected_backend")
if selected_backend not in ("rocksdb", "postgresql", "neo4j"):
    reject("READY.json selected_backend is invalid")
formal = ready.get("formal_run")
if not isinstance(formal, dict):
    reject("READY.json formal_run is missing")
argv = formal.get("argv")
if not isinstance(argv, list) or not all(isinstance(value, str) for value in argv):
    reject("READY.json formal_run.argv is invalid")

def option(name):
    positions = [index for index, value in enumerate(argv) if value == name]
    if len(positions) != 1 or positions[0] + 1 >= len(argv):
        reject(f"READY.json must seal exactly one {name} value")
    return argv[positions[0] + 1]

spec_name = option("--spec")
executor_value = option("--executor")
orchestrator_value = option("--orchestrator-bin")
environment = formal.get("environment")
if not isinstance(environment, dict):
    reject("READY.json formal_run.environment is invalid")
runtime_name = environment.get("DTGPROXY_PAPER_RUNTIME_MANIFEST")
if spec_name != "formal-spec.json" or runtime_name != "runtime-manifest.json":
    reject("READY.json must seal formal-spec.json and runtime-manifest.json")
for name in (spec_name, runtime_name, "READY.json"):
    path = bundle / name
    if name not in entries or path.is_symlink() or not path.is_file():
        reject(f"sealed bundle file is missing or invalid: {name}")
try:
    spec = json.loads((bundle / spec_name).read_text(encoding="utf-8"))
    runtime = json.loads((bundle / runtime_name).read_text(encoding="utf-8"))
except Exception as error:
    reject(f"invalid sealed selected_backend document: {error}")
if spec.get("selected_backend") != selected_backend:
    reject("formal spec selected_backend differs from READY.json")
runtime_backends = runtime.get("backends")
if not isinstance(runtime_backends, dict) or set(runtime_backends) != {selected_backend}:
    reject("runtime manifest does not preserve selected_backend")

def executable(value, digest_name, label):
    path = Path(value)
    expected = formal.get(digest_name)
    if not path.is_absolute() or path.is_symlink() or not path.is_file() or not os.access(path, os.X_OK):
        reject(f"sealed {label} must be an existing absolute executable file, not a symlink")
    if not isinstance(expected, str) or not re.fullmatch(r"[0-9a-f]{64}", expected):
        reject(f"READY.json sealed {label} SHA-256 is invalid")
    if digest_file(path) != expected:
        reject(f"sealed {label} SHA-256 mismatch")
    if "\n" in str(path) or "\r" in str(path):
        reject(f"sealed {label} path contains a line break")
    return path

executable(executor_value, "executor_sha256", "executor")
orchestrator = executable(orchestrator_value, "orchestrator_sha256", "orchestrator")
print(bundle / runtime_name)
print(orchestrator)
print(selected_backend)
PY

{
  IFS= read -r runtime_manifest
  IFS= read -r orchestrator_bin
  IFS= read -r selected_backend
} <"$metadata"
rm -f "$metadata"
trap - EXIT

export DTGPROXY_PAPER_RUNTIME_MANIFEST=$runtime_manifest
"$orchestrator_bin" verify \
  --artifact "$artifact" \
  --regenerate-dir "$regenerate_dir"

python3 - "$artifact/manifest.json" "$selected_backend" <<'PY'
import json
import sys
from pathlib import Path

manifest_path, selected_backend = sys.argv[1:]
try:
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
except Exception as error:
    raise SystemExit(f"verify-paper-performance: verified artifact manifest is invalid: {error}")
if manifest.get("selected_backend") != selected_backend:
    raise SystemExit(
        "verify-paper-performance: artifact selected_backend differs from prepared bundle"
    )
PY
