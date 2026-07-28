#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/tests/paper-performance-fixtures.sh [--keep]

Runs only tiny, synthetic, one-second paper artifact fixtures. The fixture
never starts DTGProxy, PostgreSQL, or Neo4j and never writes under
artifacts/paper-performance.
EOF
}

keep=0
while (($# > 0)); do
  case "$1" in
    --keep)
      keep=1
      shift
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

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
scratch=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-paper-fixtures.XXXXXX")
if ((keep == 0)); then
  trap 'rm -rf "$scratch"' EXIT
else
  printf 'fixture scratch: %s\n' "$scratch"
fi

run_script="$root/scripts/run-paper-performance.sh"
verify_script="$root/scripts/verify-paper-performance.sh"
runs="$scratch/runs"
baseline="$runs/tiny-fixture"

reseal() {
  local artifact=$1
  python3 - "$artifact" <<'PY'
import hashlib
import os
import sys
from pathlib import Path

artifact = Path(sys.argv[1])
checksum_path = artifact / "SHA256SUMS"
if checksum_path.exists():
    checksum_path.unlink()
lines = []
for path in sorted(candidate for candidate in artifact.rglob("*") if candidate.is_file()):
    relative = path.relative_to(artifact).as_posix()
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    lines.append(f"{digest}  {relative}\n")
checksum_path.write_text("".join(lines), encoding="ascii")
PY
}

expect_rejected() {
  local name=$1
  local expected=$2
  local artifact="$scratch/$name"
  local regenerated="$scratch/regenerated-$name"
  local log="$scratch/$name.log"
  if "$verify_script" --artifact "$artifact" --regenerate-dir "$regenerated" >"$log" 2>&1; then
    printf 'fixture unexpectedly passed: %s\n' "$name" >&2
    exit 1
  fi
  if ! grep -F "$expected" "$log" >/dev/null; then
    printf 'fixture failed for the wrong reason: %s\n' "$name" >&2
    cat "$log" >&2
    exit 1
  fi
  printf 'PASS rejected %-28s (%s)\n' "$name" "$expected"
}

"$run_script" \
  --simulate \
  --output-root "$runs" \
  --run-id tiny-fixture \
  --concurrencies 1,2 \
  --repetitions 2 \
  --warmup-seconds 1 \
  --measurement-seconds 1
"$verify_script" \
  --artifact "$baseline" \
  --regenerate-dir "$scratch/regenerated-baseline"

cp -R "$baseline" "$scratch/incomplete-matrix"
python3 - "$scratch/incomplete-matrix" <<'PY'
import json
import sys
from pathlib import Path

raw = Path(sys.argv[1]) / "raw"
for path in raw.glob("*.json"):
    if json.loads(path.read_text(encoding="utf-8"))["concurrency"] == 2:
        path.unlink()
PY
reseal "$scratch/incomplete-matrix"
expect_rejected incomplete-matrix "incomplete matrix"

cp -R "$baseline" "$scratch/duplicate-cell"
first_raw=$(find "$scratch/duplicate-cell/raw" -name '*.json' -type f | sort | head -n 1)
cp "$first_raw" "$scratch/duplicate-cell/raw/duplicate.json"
reseal "$scratch/duplicate-cell"
expect_rejected duplicate-cell "duplicate raw observation"

cp -R "$baseline" "$scratch/missing-repetition"
python3 - "$scratch/missing-repetition" <<'PY'
import json
import sys
from pathlib import Path

raw = Path(sys.argv[1]) / "raw"
for path in sorted(raw.glob("*.json")):
    value = json.loads(path.read_text(encoding="utf-8"))
    if value["concurrency"] == 1 and value["path"] == "proxy" and value["repetition"] == 2:
        path.unlink()
        break
PY
reseal "$scratch/missing-repetition"
expect_rejected missing-repetition "missing repetition"

cp -R "$baseline" "$scratch/identity-mismatch"
python3 - "$scratch/identity-mismatch" <<'PY'
import json
import sys
from pathlib import Path

for path in sorted((Path(sys.argv[1]) / "raw").glob("*.json")):
    value = json.loads(path.read_text(encoding="utf-8"))
    if value["path"] == "proxy":
        value["identity"]["digest"] = "f" * 64
        value["identity"]["row_count"] += 1
        path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
        break
PY
reseal "$scratch/identity-mismatch"
expect_rejected identity-mismatch "result identity mismatch"

cp -R "$baseline" "$scratch/required-metric-unavailable"
python3 - "$scratch/required-metric-unavailable" <<'PY'
import json
import sys
from pathlib import Path

path = sorted((Path(sys.argv[1]) / "raw").glob("*.json"))[0]
value = json.loads(path.read_text(encoding="utf-8"))
value["resources"]["cpu_time_ns"] = {
    "status": "unavailable",
    "reason": "fixture deliberately removed the required metric",
}
path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
PY
reseal "$scratch/required-metric-unavailable"
expect_rejected required-metric-unavailable "required metric unavailable"

cp -R "$baseline" "$scratch/raw-tamper"
first_raw=$(find "$scratch/raw-tamper/raw" -name '*.json' -type f | sort | head -n 1)
printf ' ' >>"$first_raw"
expect_rejected raw-tamper "checksum mismatch"

cp -R "$baseline" "$scratch/summary-mismatch"
python3 - "$scratch/summary-mismatch" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1]) / "summary" / "summary.json"
value = json.loads(path.read_text(encoding="utf-8"))
value["cells"][0]["throughput"]["mean"] += 1.0
path.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
PY
reseal "$scratch/summary-mismatch"
expect_rejected summary-mismatch "summary is not reproducible from raw"

printf 'PASS paper performance fixture certification\n'
