#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
run_script="$root/scripts/run-paper-performance.sh"
isolated_script="$root/scripts/run-isolated-paper-performance.sh"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-paper-run.XXXXXX")
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
  local bundle=$1
  local path
  rm -f "$bundle/SHA256SUMS"
  : >"$bundle/SHA256SUMS"
  for path in "$bundle"/*; do
    [[ $(basename "$path") == SHA256SUMS ]] && continue
    printf '%s  %s\n' "$(sha256_file "$path")" "$(basename "$path")" \
      >>"$bundle/SHA256SUMS"
  done
}

make_bundle() {
  local name=$1
  local selected_backend=${2:-rocksdb}
  local run_id=$name
  local bundle="$scratch/$name"
  local executor="$scratch/$name-executor"
  local orchestrator="$scratch/$name-orchestrator"
  mkdir -p "$bundle"
  jq -n --arg backend "$selected_backend" --arg run_id "$run_id" \
    '{selected_backend: $backend, run_id: $run_id}' >"$bundle/formal-spec.json"
  jq -n --arg backend "$selected_backend" '{backends: {($backend): {}}}' >"$bundle/runtime-manifest.json"
  printf '#!/bin/sh\nexit 0\n' >"$executor"
  chmod +x "$executor"
  cat >"$orchestrator" <<'EOF'
#!/bin/sh
set -eu
[ "${DTGPROXY_PAPER_RUNTIME_MANIFEST-}" = "$EXPECTED_RUNTIME_MANIFEST" ] || {
  printf '%s\n' 'sealed runtime manifest was not forwarded' >&2
  exit 88
}
printf '%s\n' "$*" >>"$FORMAL_INVOCATION_LOG"
EOF
  chmod +x "$orchestrator"
  jq -n \
    --arg executor "$executor" \
    --arg executor_sha256 "$(sha256_file "$executor")" \
    --arg orchestrator "$orchestrator" \
    --arg orchestrator_sha256 "$(sha256_file "$orchestrator")" \
    --arg selected_backend "$selected_backend" \
    --arg run_id "$name" '
    {
      schema_version: 1,
      status: "prepared",
      selected_backend: $selected_backend,
      run_id: $run_id,
      formal_matrix_executed: false,
      formal_run: {
        argv: [
          "scripts/run-paper-performance.sh",
          "--spec", "formal-spec.json",
          "--executor", $executor,
          "--orchestrator-bin", $orchestrator
        ],
        environment: {
          DTGPROXY_PAPER_RUNTIME_MANIFEST: "runtime-manifest.json"
        },
        executor_sha256: $executor_sha256,
        orchestrator_sha256: $orchestrator_sha256
      }
    }' >"$bundle/READY.json"
  seal_bundle "$bundle"
  printf '%s\n' "$bundle"
}

expect_rejected() {
  local name=$1
  local expected=$2
  local bundle=$3
  local log="$scratch/$name.log"
  local invocation_log="$scratch/$name-invocations.log"
  if FORMAL_INVOCATION_LOG="$invocation_log" \
    EXPECTED_RUNTIME_MANIFEST="$bundle/runtime-manifest.json" \
    PATH="$scratch/bin:$PATH" \
    "$run_script" --prepared-bundle "$bundle" \
      --output-root "$scratch/$name-output" >"$log" 2>&1; then
    fail "$name unexpectedly passed"
  fi
  grep -F "$expected" "$log" >/dev/null || {
    cat "$log" >&2
    fail "$name failed for the wrong reason"
  }
  [[ ! -e $invocation_log ]] || fail "$name invoked the orchestrator"
  printf 'PASS rejected %s (%s)\n' "$name" "$expected"
}

mkdir -p "$scratch/bin"
cat >"$scratch/bin/cargo" <<'EOF'
#!/bin/sh
printf '%s\n' 'formal execution must not invoke Cargo' >&2
exit 97
EOF
chmod +x "$scratch/bin/cargo"

bundle=$(make_bundle valid)
invocation_log="$scratch/valid-invocations.log"
output_root="$scratch/output"
FORMAL_INVOCATION_LOG="$invocation_log" \
EXPECTED_RUNTIME_MANIFEST="$bundle/runtime-manifest.json" \
PATH="$scratch/bin:$PATH" \
  "$run_script" --prepared-bundle "$bundle" --output-root "$output_root"
expected="run --spec $bundle/formal-spec.json --output-root $output_root --executor $scratch/valid-executor"
[[ $(cat "$invocation_log") == "$expected" ]] || fail "formal orchestrator invocation mismatch"
[[ $(wc -l <"$invocation_log" | tr -d ' ') == 1 ]] || fail "formal orchestrator was not invoked exactly once"
printf '%s\n' 'PASS formal run uses one sealed orchestrator invocation with runtime forwarding and no Cargo'

bundle=$(make_bundle tampered)
printf 'tamper\n' >>"$bundle/runtime-manifest.json"
expect_rejected tampered-bundle "checksum mismatch" "$bundle"

bundle=$(make_bundle wrong-executable-digest)
jq '.formal_run.executor_sha256 = ("0" * 64)' "$bundle/READY.json" >"$bundle/READY.json.new"
mv "$bundle/READY.json.new" "$bundle/READY.json"
seal_bundle "$bundle"
expect_rejected wrong-executable-digest "executor SHA-256 mismatch" "$bundle"

bundle=$(make_bundle symlinked)
rm "$bundle/runtime-manifest.json"
ln -s "$scratch/valid/runtime-manifest.json" "$bundle/runtime-manifest.json"
seal_bundle "$bundle"
expect_rejected symlinked-bundle "symlink" "$bundle"

bundle=$(make_bundle wrong-selected-backend)
jq '.selected_backend = "postgresql"' "$bundle/READY.json" >"$bundle/READY.json.new"
mv "$bundle/READY.json.new" "$bundle/READY.json"
seal_bundle "$bundle"
expect_rejected wrong-selected-backend "selected_backend" "$bundle"

make_isolated_bundle() {
  local selected_backend=$1
  local run_id=$2
  local bundle="$scratch/isolated-$selected_backend"
  local executor="$scratch/isolated-$selected_backend-executor"
  local orchestrator="$scratch/isolated-$selected_backend-orchestrator"
  local pid=$((4100 + ${#selected_backend}))
  mkdir -p "$bundle"
  printf '#!/bin/sh\nexit 0\n' >"$executor"
  chmod +x "$executor"
  cat >"$orchestrator" <<'EOF'
#!/bin/sh
set -eu
command=$1
shift
if [ "$command" = run ]; then
  spec=
  output_root=
  while [ $# -gt 0 ]; do
    if [ "$1" = --spec ]; then spec=$2; shift 2; continue; fi
    if [ "$1" = --output-root ]; then output_root=$2; shift 2; continue; fi
    shift
  done
  backend=$(jq -r .selected_backend "$spec")
  run_id=$(jq -r .run_id "$spec")
  printf '%s %s\n' "$command" "$backend" >>"$ISOLATED_EVENT_LOG"
  [ "${FAIL_BACKEND-}" != "$backend" ] || exit 71
  mkdir -p "$output_root/$run_id"
  jq -n --arg backend "$backend" '{selected_backend: $backend}' >"$output_root/$run_id/manifest.json"
elif [ "$command" = verify ]; then
  artifact=
  while [ $# -gt 0 ]; do
    if [ "$1" = --artifact ]; then artifact=$2; break; fi
    shift
  done
  backend=$(jq -r .selected_backend "$artifact/manifest.json")
  printf '%s %s\n' "$command" "$backend" >>"$ISOLATED_EVENT_LOG"
  [ "${TAMPER_ORCHESTRATOR_AFTER_VERIFY-}" != "$backend" ] ||
    printf '# tampered after verified run\n' >>"$0"
elif [ "$command" = combine ]; then
  backend=rocksdb
  printf '%s %s\n' "$command" "$backend" >>"$ISOLATED_EVENT_LOG"
  output=
  while [ $# -gt 0 ]; do
    if [ "$1" = --output ]; then output=$2; break; fi
    shift
  done
  mkdir -p "$output"
  printf '{"schema_version":1,"backends":[{"backend":"rocksdb"},{"backend":"postgresql"},{"backend":"neo4j"}]}\n' >"$output/combined-report.json"
  printf 'backend_run,backend\n' >"$output/combined-summary.csv"
  (cd "$output" && {
    shasum -a 256 combined-report.json combined-summary.csv |
      awk '{print $1 "  " $2}' >SHA256SUMS
  })
  [ "${BAD_COMBINED-}" != 1 ] || printf 'tamper\n' >>"$output/combined-report.json"
fi
EOF
  chmod +x "$orchestrator"
  jq -n --arg backend "$selected_backend" --arg run_id "$run_id" \
    '{selected_backend: $backend, run_id: $run_id}' >"$bundle/formal-spec.json"
  jq -n --arg backend "$selected_backend" '{backends: {($backend): {}}}' >"$bundle/runtime-manifest.json"
  jq -n \
    --arg backend "$selected_backend" \
    --arg host "paper-$selected_backend" \
    --arg start "start-$selected_backend" \
    --argjson pid "$pid" '
    {
      schema_version: 1,
      source: "remote_formal_preparation",
      targets: [{
        backend: $backend,
        data_nodes: 1,
        ablation: "production",
        deployment_mode: "remote_formal",
        processes: [{
          claimed: {
            pid: $pid,
            executable: "/opt/dtgproxy/data-node",
            executable_sha256: ("a" * 64),
            listen_address: "10.0.0.1:7000",
            data_directory: "/var/lib/dtgproxy",
            probe: {
              ssh_target: $host,
              host_id: $host,
              boot_id: "boot-1",
              probe_binary: "/opt/dtgproxy/probe",
              probe_binary_sha256: ("b" * 64),
              data_interface: "eth0",
              management_interface: "eth1"
            }
          },
          snapshot: {
            schema_version: 1,
            sampled_unix_ns: 1,
            host_id: $host,
            boot_id: "boot-1",
            process_start_id: $start,
            pid: $pid,
            executable: "/opt/dtgproxy/data-node",
            executable_sha256: ("a" * 64),
            probe_binary_sha256: ("b" * 64),
            listen_address: "10.0.0.1:7000",
            data_directory: "/var/lib/dtgproxy",
            cpu_time_ns: 1,
            rss_bytes: 1,
            peak_rss_bytes: 1,
            network_interface: "eth0",
            network_rx_bytes: 1,
            network_tx_bytes: 1
          }
        }]
      }]
    }' >"$bundle/remote-node-evidence.json"
  jq -n \
    --arg backend "$selected_backend" \
    --arg host "paper-$selected_backend" \
    --arg start "start-$selected_backend" \
    --argjson pid "$pid" '
    {
      schema_version: 1,
      selected_backend: $backend,
      lifecycle_contract: "the sealed run command must retire every exact managed process before returning; the sequential runner never sends process signals",
      managed_processes: [
        {
          backend: $backend,
          role: "data_node",
          identity: {host_id: $host, boot_id: "boot-1", process_start_id: $start, pid: $pid},
          executable: "/opt/dtgproxy/data-node",
          executable_sha256: ("a" * 64),
          probe: {
            kind: "remote", ssh_target: $host,
            probe_binary: "/opt/dtgproxy/probe", network_interface: "eth0"
          }
        },
        {
          backend: $backend,
          role: "gateway",
          identity: {host_id: "local-fixture", boot_id: "boot-local", process_start_id: "start-local", pid: 999999},
          executable: "/opt/dtgproxy/gateway",
          executable_sha256: ("c" * 64),
          probe: {kind: "local"}
        }
      ],
      backend_service: (if $backend == "rocksdb" then
        {ownership: "embedded", managed_by: "data_node"}
      else
        {ownership: "external", managed: false, reason: "fixture has no backend service PID"}
      end)
    }' >"$bundle/managed-process-evidence.json"
  jq -n \
    --arg backend "$selected_backend" \
    --arg run_id "$run_id" \
    --arg executor "$executor" \
    --arg executor_sha256 "$(sha256_file "$executor")" \
    --arg orchestrator "$orchestrator" \
    --arg orchestrator_sha256 "$(sha256_file "$orchestrator")" '
    {
      schema_version: 1,
      status: "prepared",
      selected_backend: $backend,
      run_id: $run_id,
      formal_matrix_executed: false,
      formal_run: {
        argv: [
          "scripts/run-paper-performance.sh",
          "--spec", "formal-spec.json",
          "--executor", $executor,
          "--orchestrator-bin", $orchestrator
        ],
        environment: {DTGPROXY_PAPER_RUNTIME_MANIFEST: "runtime-manifest.json"},
        executor_sha256: $executor_sha256,
        orchestrator_sha256: $orchestrator_sha256
      }
    }' >"$bundle/READY.json"
  seal_bundle "$bundle"
  printf '%s\n' "$bundle"
}

cat >"$scratch/bin/ssh" <<'EOF'
#!/bin/sh
set -eu
while [ "${1-}" != -- ]; do shift; done
shift
host=$1
shift
backend=${host#paper-}
if [ "${1-}" = true ]; then
  exit 0
fi
if [ "${1-}" = ps ]; then
  pid=$3
  [ "${ABSENT_BACKEND-}" = "$backend" ] && exit 1
  printf ' %s\n' "$pid"
  exit 0
fi
printf 'identity %s\n' "$backend" >>"$ISOLATED_EVENT_LOG"
[ "${ABSENT_BACKEND-}" != "$backend" ] || exit 3
[ "${BROKEN_PROBE_BACKEND-}" != "$backend" ] || exit 4
pid=
while [ $# -gt 0 ]; do
  if [ "$1" = --pid ]; then pid=$2; fi
  shift
done
start="start-$backend"
[ "${STICKY_BACKEND-}" = "$backend" ] || start="new-$start"
[ "${MALFORMED_IDENTITY_BACKEND-}" != "$backend" ] || {
  jq -cn --arg host "$host" --argjson pid "$pid" \
    '{host_id: $host, boot_id: "boot-1", pid: $pid}'
  exit 0
}
jq -cn --arg host "$host" --arg start "$start" --argjson pid "$pid" '
  {host_id: $host, boot_id: "boot-1", process_start_id: $start, pid: $pid}'
EOF
chmod +x "$scratch/bin/ssh"

rocksdb_bundle=$(make_isolated_bundle rocksdb isolated-rocksdb)
postgresql_bundle=$(make_isolated_bundle postgresql isolated-postgresql)
neo4j_bundle=$(make_isolated_bundle neo4j isolated-neo4j)

mismatched_orchestrator_bundle="$scratch/isolated-postgresql-mismatched-orchestrator"
cp -R "$postgresql_bundle" "$mismatched_orchestrator_bundle"
mismatched_orchestrator="$scratch/isolated-mismatched-orchestrator"
cp "$scratch/isolated-postgresql-orchestrator" "$mismatched_orchestrator"
printf '# different sealed identity\n' >>"$mismatched_orchestrator"
chmod +x "$mismatched_orchestrator"
jq --arg orchestrator "$mismatched_orchestrator" \
  --arg digest "$(sha256_file "$mismatched_orchestrator")" '
  (.formal_run.argv[.formal_run.argv | index("--orchestrator-bin") + 1]) = $orchestrator |
  .formal_run.orchestrator_sha256 = $digest
' "$mismatched_orchestrator_bundle/READY.json" >"$mismatched_orchestrator_bundle/READY.json.new"
mv "$mismatched_orchestrator_bundle/READY.json.new" "$mismatched_orchestrator_bundle/READY.json"
seal_bundle "$mismatched_orchestrator_bundle"
if ISOLATED_EVENT_LOG="$scratch/orchestrator-mismatch.events" PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$mismatched_orchestrator_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$scratch/orchestrator-mismatch-output" \
  >"$scratch/orchestrator-mismatch.log" 2>&1; then
  fail "isolated runner accepted different sealed orchestrator identities"
fi
grep -F 'must seal the same orchestrator SHA-256' "$scratch/orchestrator-mismatch.log" >/dev/null || {
  cat "$scratch/orchestrator-mismatch.log" >&2
  fail "orchestrator identity mismatch failed for the wrong reason"
}
[[ ! -e $scratch/orchestrator-mismatch.events ]] || fail "orchestrator mismatch launched a backend"
printf '%s\n' 'PASS isolated runner preflights one shared sealed orchestrator identity'

missing_service_bundle="$scratch/isolated-postgresql-missing-service-evidence"
cp -R "$postgresql_bundle" "$missing_service_bundle"
jq 'del(.backend_service)' "$missing_service_bundle/managed-process-evidence.json" \
  >"$missing_service_bundle/managed-process-evidence.json.new"
mv "$missing_service_bundle/managed-process-evidence.json.new" \
  "$missing_service_bundle/managed-process-evidence.json"
seal_bundle "$missing_service_bundle"
if ISOLATED_EVENT_LOG="$scratch/missing-service.events" PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$missing_service_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$scratch/missing-service-output" >"$scratch/missing-service.log" 2>&1; then
  fail "isolated runner accepted missing backend service ownership evidence"
fi
grep -F 'external backend service ownership evidence is invalid' "$scratch/missing-service.log" >/dev/null || {
  cat "$scratch/missing-service.log" >&2
  fail "missing backend service ownership failed for the wrong reason"
}
[[ ! -e $scratch/missing-service.events ]] || fail "missing service evidence launched a backend"
printf '%s\n' 'PASS isolated runner requires sealed backend service ownership evidence'

events="$scratch/isolated-success.events"
output="$scratch/isolated-output"
ISOLATED_EVENT_LOG="$events" PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output"
expected_events='run rocksdb
verify rocksdb
identity rocksdb
run postgresql
verify postgresql
identity postgresql
run neo4j
verify neo4j
identity neo4j
combine rocksdb'
[[ $(cat "$events") == "$expected_events" ]] || fail "isolated runner order mismatch"
[[ -f $output/combined/combined-report.json ]] || fail "isolated runner did not create combined output"
printf '%s\n' 'PASS isolated runner uses fixed verified process-isolated backend order'

events="$scratch/isolated-bad-combined.events"
output="$scratch/isolated-bad-combined-output"
if ISOLATED_EVENT_LOG="$events" BAD_COMBINED=1 PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-bad-combined.log" 2>&1; then
  fail "isolated runner accepted a corrupt combined package"
fi
grep -F 'combined checksum mismatch' "$scratch/isolated-bad-combined.log" >/dev/null || {
  cat "$scratch/isolated-bad-combined.log" >&2
  fail "corrupt combined package failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "corrupt combined package was retained as output"
printf '%s\n' 'PASS isolated runner verifies and removes an invalid combined package'

events="$scratch/isolated-absent.events"
output="$scratch/isolated-absent-output"
ISOLATED_EVENT_LOG="$events" ABSENT_BACKEND=rocksdb PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output"
[[ $(cat "$events") == "$expected_events" ]] || fail "absent managed PID order mismatch"
[[ -f $output/combined/combined-report.json ]] || fail "absent managed PID blocked combined output"
printf '%s\n' 'PASS isolated runner accepts an absent exact managed PID on a reachable host'

events="$scratch/isolated-broken-probe.events"
output="$scratch/isolated-broken-probe-output"
if ISOLATED_EVENT_LOG="$events" BROKEN_PROBE_BACKEND=rocksdb PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-broken-probe.log" 2>&1; then
  fail "isolated runner treated a broken identity probe as PID exit"
fi
grep -F 'managed PID is still live but its start identity cannot be verified' \
  "$scratch/isolated-broken-probe.log" >/dev/null || {
  cat "$scratch/isolated-broken-probe.log" >&2
  fail "broken identity probe failed for the wrong reason"
}
[[ $(cat "$events") == 'run rocksdb
verify rocksdb
identity rocksdb' ]] || fail "broken identity probe did not stop before the next backend"
[[ ! -e $output/combined ]] || fail "broken identity probe created combined output"
printf '%s\n' 'PASS isolated runner does not confuse a broken probe with exact PID exit'

events="$scratch/isolated-malformed-identity.events"
output="$scratch/isolated-malformed-identity-output"
if ISOLATED_EVENT_LOG="$events" MALFORMED_IDENTITY_BACKEND=rocksdb PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-malformed-identity.log" 2>&1; then
  fail "isolated runner accepted malformed process identity"
fi
grep -F 'managed PID identity probe returned invalid process_start_id' \
  "$scratch/isolated-malformed-identity.log" >/dev/null || {
  cat "$scratch/isolated-malformed-identity.log" >&2
  fail "malformed process identity failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "malformed process identity created combined output"
printf '%s\n' 'PASS isolated runner fails closed on malformed process identity'

gateway_leak_bundle="$scratch/isolated-rocksdb-gateway-leak"
cp -R "$rocksdb_bundle" "$gateway_leak_bundle"
python3 - "$gateway_leak_bundle/managed-process-evidence.json" "$$" <<'PY'
import json
import socket
import subprocess
import sys
from pathlib import Path

path = Path(sys.argv[1])
pid = int(sys.argv[2])
value = json.loads(path.read_text(encoding="utf-8"))
if sys.platform == "linux":
    stat_line = Path(f"/proc/{pid}/stat").read_text(encoding="ascii")
    fields = stat_line[stat_line.rfind(")") + 2:].split()
    start = f"linux-start-ticks:{fields[19]}"
    boot = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
elif sys.platform == "darwin":
    start = subprocess.check_output(
        ["ps", "-p", str(pid), "-o", "lstart="], text=True
    ).strip()
    boot = subprocess.check_output(["sysctl", "-n", "kern.boottime"], text=True).strip()
else:
    raise SystemExit(f"unsupported test platform: {sys.platform}")
gateway = next(item for item in value["managed_processes"] if item["role"] == "gateway")
gateway["identity"] = {
    "host_id": socket.gethostname(),
    "boot_id": boot,
    "process_start_id": start,
    "pid": pid,
}
path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
PY
seal_bundle "$gateway_leak_bundle"
events="$scratch/isolated-gateway-leak.events"
output="$scratch/isolated-gateway-leak-output"
if ISOLATED_EVENT_LOG="$events" PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$gateway_leak_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-gateway-leak.log" 2>&1; then
  fail "isolated runner accepted a still-live managed Gateway"
fi
grep -F 'managed PID is still the same process' "$scratch/isolated-gateway-leak.log" >/dev/null || {
  cat "$scratch/isolated-gateway-leak.log" >&2
  fail "Gateway leakage failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "Gateway leakage created combined output"
printf '%s\n' 'PASS isolated runner checks Gateway as well as DataNode managed identities'

events="$scratch/isolated-failure.events"
output="$scratch/isolated-failure-output"
if ISOLATED_EVENT_LOG="$events" FAIL_BACKEND=postgresql PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-failure.log" 2>&1; then
  fail "isolated runner continued after a failed backend"
fi
[[ $(cat "$events") == 'run rocksdb
verify rocksdb
identity rocksdb
run postgresql' ]] || fail "isolated runner failure-stop order mismatch"
[[ ! -e $output/combined ]] || fail "failed isolated run created combined output"
printf '%s\n' 'PASS isolated runner stops on failure without combined output'

events="$scratch/isolated-identity.events"
output="$scratch/isolated-identity-output"
if ISOLATED_EVENT_LOG="$events" STICKY_BACKEND=rocksdb PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-identity.log" 2>&1; then
  fail "isolated runner accepted the previous managed process identity"
fi
grep -F 'managed PID is still the same process' "$scratch/isolated-identity.log" >/dev/null || {
  cat "$scratch/isolated-identity.log" >&2
  fail "identity isolation failed for the wrong reason"
}
[[ $(cat "$events") == 'run rocksdb
verify rocksdb
identity rocksdb' ]] || fail "identity failure did not stop before the next backend"
[[ ! -e $output/combined ]] || fail "identity failure created combined output"
printf '%s\n' 'PASS isolated runner rejects a still-live exact managed process identity'

events="$scratch/isolated-orchestrator-recheck.events"
output="$scratch/isolated-orchestrator-recheck-output"
if ISOLATED_EVENT_LOG="$events" TAMPER_ORCHESTRATOR_AFTER_VERIFY=rocksdb \
  PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-orchestrator-recheck.log" 2>&1; then
  fail "isolated runner did not recheck orchestrator identity before combine"
fi
grep -F 'orchestrator SHA-256 changed before combine' \
  "$scratch/isolated-orchestrator-recheck.log" >/dev/null || {
  cat "$scratch/isolated-orchestrator-recheck.log" >&2
  fail "orchestrator recheck failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "orchestrator recheck failure created combined output"
printf '%s\n' 'PASS isolated runner rechecks the sealed orchestrator immediately before combine'

if grep -E '(^|[^[:alnum:]_])(pkill|pgrep|killall)([^[:alnum:]_]|$)' "$isolated_script" >/dev/null; then
  fail "isolated runner scans or kills processes by name"
fi
printf '%s\n' 'PASS isolated runner contains no process-name scan or kill command'
