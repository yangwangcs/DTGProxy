#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
runner="$repo_root/scripts/run-paper-diagnostic.sh"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-paper-diagnostic.XXXXXX")"
trap 'rm -rf "$scratch"' EXIT
mkdir -p "$scratch/bin"

fail() {
  echo "FAIL $*" >&2
  exit 1
}

cat >"$scratch/bin/cargo" <<'FAKE_CARGO'
#!/usr/bin/env bash
set -euo pipefail
[[ "$*" == "test -p paper-benchmark --test diagnostic_capture capture_selected_backend_diagnostic -- --exact --nocapture" ]] || exit 91
[[ ${DTGPROXY_DIAGNOSTIC_BACKEND-} =~ ^(rocksdb|postgresql|neo4j)$ ]] || exit 92
[[ -n ${DTGPROXY_DIAGNOSTIC_OUTPUT_DIR-} ]] || exit 93
mkdir -p "$DTGPROXY_DIAGNOSTIC_OUTPUT_DIR/raw"
printf '%s\n' "{\"schema_version\":1,\"mode\":\"diagnostic\",\"selected_backend\":\"$DTGPROXY_DIAGNOSTIC_BACKEND\",\"repetitions\":3,\"paths\":[\"backend_direct\",\"adapter_direct\"]}" >"$DTGPROXY_DIAGNOSTIC_OUTPUT_DIR/diagnostic-manifest.json"
FAKE_CARGO
chmod +x "$scratch/bin/cargo"

for backend in rocksdb postgresql neo4j; do
  output="$scratch/output-$backend"
  PATH="$scratch/bin:$PATH" "$runner" \
    --backend "$backend" \
    --output-dir "$output" \
    --warmup-seconds 1 \
    --measurement-seconds 3 \
    --repetitions 3
  [[ -f "$output/diagnostic-manifest.json" ]] || fail "$backend manifest missing"
  python3 - "$output/diagnostic-manifest.json" "$backend" <<'PY'
import json, pathlib, sys
manifest = json.loads(pathlib.Path(sys.argv[1]).read_text())
assert manifest == {
    "schema_version": 1,
    "mode": "diagnostic",
    "selected_backend": sys.argv[2],
    "repetitions": 3,
    "paths": ["backend_direct", "adapter_direct"],
}
PY
done

if PATH="$scratch/bin:$PATH" "$runner" --backend memory --output-dir "$scratch/invalid" 2>/dev/null; then
  fail "unsupported backend unexpectedly passed"
fi
if PATH="$scratch/bin:$PATH" "$runner" --backend rocksdb --output-dir "$scratch/output-rocksdb" 2>/dev/null; then
  fail "existing output unexpectedly passed"
fi

echo "PASS persistent single-backend diagnostic entry contract"
