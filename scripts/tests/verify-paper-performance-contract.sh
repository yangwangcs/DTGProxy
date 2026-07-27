#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
verify_script="$root/scripts/verify-paper-performance.sh"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-paper-verify.XXXXXX")
scratch=$(cd "$scratch" && pwd -P)
trap 'rm -rf "$scratch"' EXIT

fail() {
  printf 'FAIL %s\n' "$*" >&2
  exit 1
}

sha256_file() {
  shasum -a 256 "$1" | awk '{print $1}'
}

seal_bundle() {
  local path
  rm -f "$bundle/SHA256SUMS"
  : >"$bundle/SHA256SUMS"
  for path in "$bundle"/*; do
    [[ $(basename "$path") == SHA256SUMS ]] && continue
    printf '%s  %s\n' "$(sha256_file "$path")" "$(basename "$path")" \
      >>"$bundle/SHA256SUMS"
  done
}

mkdir -p "$scratch/bin" "$scratch/bundle" "$scratch/artifact"
bundle="$scratch/bundle"
executor="$scratch/executor"
orchestrator="$scratch/orchestrator"
printf '{"selected_backend":"rocksdb"}\n' >"$bundle/formal-spec.json"
printf '{"backends":{"rocksdb":{}}}\n' >"$bundle/runtime-manifest.json"
printf '{"selected_backend":"rocksdb"}\n' >"$scratch/artifact/manifest.json"
printf '#!/bin/sh\nexit 0\n' >"$executor"
chmod +x "$executor"
cat >"$orchestrator" <<'EOF'
#!/bin/sh
set -eu
[ "${DTGPROXY_PAPER_RUNTIME_MANIFEST-}" = "$EXPECTED_RUNTIME_MANIFEST" ] || exit 88
printf '%s\n' "$*" >>"$VERIFY_INVOCATION_LOG"
EOF
chmod +x "$orchestrator"
cat >"$scratch/bin/cargo" <<'EOF'
#!/bin/sh
printf '%s\n' 'formal verification must not invoke Cargo' >&2
exit 97
EOF
chmod +x "$scratch/bin/cargo"
jq -n \
  --arg executor "$executor" \
  --arg executor_sha256 "$(sha256_file "$executor")" \
  --arg orchestrator "$orchestrator" \
  --arg orchestrator_sha256 "$(sha256_file "$orchestrator")" '
  {
    schema_version: 1, status: "prepared", selected_backend: "rocksdb",
    run_id: "verify-rocksdb", formal_matrix_executed: false,
    formal_run: {
      argv: ["scripts/run-paper-performance.sh", "--spec", "formal-spec.json", "--executor", $executor, "--orchestrator-bin", $orchestrator],
      environment: {DTGPROXY_PAPER_RUNTIME_MANIFEST: "runtime-manifest.json"},
      executor_sha256: $executor_sha256,
      orchestrator_sha256: $orchestrator_sha256
    }
  }' >"$bundle/READY.json"
seal_bundle

invocation_log="$scratch/invocations.log"
regenerate="$scratch/regenerated"
VERIFY_INVOCATION_LOG="$invocation_log" \
EXPECTED_RUNTIME_MANIFEST="$bundle/runtime-manifest.json" \
PATH="$scratch/bin:$PATH" \
  "$verify_script" --prepared-bundle "$bundle" \
    --artifact "$scratch/artifact" --regenerate-dir "$regenerate"
expected="verify --artifact $scratch/artifact --regenerate-dir $regenerate"
[[ $(cat "$invocation_log") == "$expected" ]] || fail "sealed verifier invocation mismatch"
[[ $(wc -l <"$invocation_log" | tr -d ' ') == 1 ]] || fail "sealed verifier was not invoked exactly once"
printf '%s\n' 'PASS formal verification uses the sealed orchestrator without Cargo'

mkdir -p "$scratch/wrong-backend-artifact"
printf '{"selected_backend":"postgresql"}\n' >"$scratch/wrong-backend-artifact/manifest.json"
if VERIFY_INVOCATION_LOG="$scratch/wrong-backend-artifact-invocations.log" \
  EXPECTED_RUNTIME_MANIFEST="$bundle/runtime-manifest.json" \
  PATH="$scratch/bin:$PATH" \
  "$verify_script" --prepared-bundle "$bundle" \
    --artifact "$scratch/wrong-backend-artifact" \
    --regenerate-dir "$scratch/wrong-backend-artifact-regenerated" \
    >"$scratch/wrong-backend-artifact.log" 2>&1; then
  fail "artifact selected_backend mismatch unexpectedly passed"
fi
grep -F 'artifact selected_backend differs from prepared bundle' \
  "$scratch/wrong-backend-artifact.log" >/dev/null || {
  cat "$scratch/wrong-backend-artifact.log" >&2
  fail "artifact selected_backend mismatch failed for the wrong reason"
}
[[ $(wc -l <"$scratch/wrong-backend-artifact-invocations.log" | tr -d ' ') == 1 ]] ||
  fail "artifact backend binding did not run after exactly one cryptographic verification"
printf '%s\n' 'PASS verified artifact backend is bound to the sealed bundle backend'

printf 'tamper\n' >>"$bundle/formal-spec.json"
if VERIFY_INVOCATION_LOG="$scratch/tampered-invocations.log" \
  EXPECTED_RUNTIME_MANIFEST="$bundle/runtime-manifest.json" \
  PATH="$scratch/bin:$PATH" \
  "$verify_script" --prepared-bundle "$bundle" \
    --artifact "$scratch/artifact" --regenerate-dir "$scratch/tampered-regenerated" \
    >"$scratch/tampered.log" 2>&1; then
  fail "tampered verification bundle unexpectedly passed"
fi
grep -F 'checksum mismatch' "$scratch/tampered.log" >/dev/null || {
  cat "$scratch/tampered.log" >&2
  fail "tampered verification bundle failed for the wrong reason"
}
[[ ! -e $scratch/tampered-invocations.log ]] || fail "tampered bundle invoked the verifier"
printf '%s\n' 'PASS formal verification rejects a tampered prepared bundle'

printf '{"selected_backend":"postgresql"}\n' >"$bundle/formal-spec.json"
seal_bundle
if VERIFY_INVOCATION_LOG="$scratch/selected-backend-invocations.log" \
  EXPECTED_RUNTIME_MANIFEST="$bundle/runtime-manifest.json" \
  PATH="$scratch/bin:$PATH" \
  "$verify_script" --prepared-bundle "$bundle" \
    --artifact "$scratch/artifact" --regenerate-dir "$scratch/selected-backend-regenerated" \
    >"$scratch/selected-backend.log" 2>&1; then
  fail "selected_backend mismatch unexpectedly passed verification"
fi
grep -F 'selected_backend' "$scratch/selected-backend.log" >/dev/null || {
  cat "$scratch/selected-backend.log" >&2
  fail "selected_backend mismatch failed for the wrong reason"
}
[[ ! -e $scratch/selected-backend-invocations.log ]] || fail "selected_backend mismatch invoked verifier"
printf '%s\n' 'PASS formal verification preserves the sealed selected_backend identity'
