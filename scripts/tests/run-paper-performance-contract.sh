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
  local gateway_pid=$((990000 + ${#selected_backend}))
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
  (cd "$output_root/$run_id" && \
    shasum -a 256 manifest.json | awk '{print $1 "  " $2}' >SHA256SUMS)
elif [ "$command" = verify ]; then
  artifact=
  while [ $# -gt 0 ]; do
    if [ "$1" = --artifact ]; then artifact=$2; break; fi
    shift
  done
  backend=$(jq -r .selected_backend "$artifact/manifest.json")
  printf '%s %s\n' "$command" "$backend" >>"$ISOLATED_EVENT_LOG"
  (cd "$artifact" && shasum -a 256 -c SHA256SUMS >/dev/null)
  verify_count=$(grep -c "^verify $backend$" "$ISOLATED_EVENT_LOG")
  if [ "${TAMPER_ARTIFACT_AFTER_FINAL_VERIFY-}" = "$backend" ] &&
     [ "$verify_count" -ge 2 ]; then
    chmod u+w "$artifact/raw/proxy.json"
    printf 'tampered after final verification\n' >>"$artifact/raw/proxy.json"
  fi
  [ "${TAMPER_ORCHESTRATOR_AFTER_VERIFY-}" != "$backend" ] ||
    printf '# tampered after verified run\n' >>"$0"
elif [ "$command" = combine ]; then
  backend=rocksdb
  printf '%s %s\n' "$command" "$backend" >>"$ISOLATED_EVENT_LOG"
  output=
  rocksdb_artifact=
  postgresql_artifact=
  neo4j_artifact=
  while [ $# -gt 0 ]; do
    if [ "$1" = --output ]; then output=$2; shift 2; continue; fi
    if [ "$1" = --rocksdb ]; then rocksdb_artifact=$2; shift 2; continue; fi
    if [ "$1" = --postgresql ]; then postgresql_artifact=$2; shift 2; continue; fi
    if [ "$1" = --neo4j ]; then neo4j_artifact=$2; shift 2; continue; fi
    shift
  done
  mkdir -p "$output"
  rocksdb_sha=$(shasum -a 256 "$rocksdb_artifact/SHA256SUMS" | awk '{print $1}')
  postgresql_sha=$(shasum -a 256 "$postgresql_artifact/SHA256SUMS" | awk '{print $1}')
  neo4j_sha=$(shasum -a 256 "$neo4j_artifact/SHA256SUMS" | awk '{print $1}')
  jq -n \
    --arg rocksdb "$rocksdb_artifact" \
    --arg postgresql "$postgresql_artifact" \
    --arg neo4j "$neo4j_artifact" \
    --arg rocksdb_sha "$rocksdb_sha" \
    --arg postgresql_sha "$postgresql_sha" \
    --arg neo4j_sha "$neo4j_sha" '
    {
      schema_version: 1,
      backends: [
        {backend: "rocksdb", artifact: $rocksdb,
         artifact_sha256: $rocksdb_sha, verification: {run_id: "isolated-rocksdb"}},
        {backend: "postgresql", artifact: $postgresql,
         artifact_sha256: $postgresql_sha, verification: {run_id: "isolated-postgresql"}},
        {backend: "neo4j", artifact: $neo4j,
         artifact_sha256: $neo4j_sha, verification: {run_id: "isolated-neo4j"}}
      ]
    }' >"$output/combined-report.json"
  printf 'backend_run,backend\n' >"$output/combined-summary.csv"
  (cd "$output" && {
    shasum -a 256 combined-report.json combined-summary.csv |
      awk '{print $1 "  " $2}' >SHA256SUMS
  })
  [ "${BAD_COMBINED-}" != 1 ] || printf 'tamper\n' >>"$output/combined-report.json"
  [ "${EXTRA_COMBINED_FILE-}" != 1 ] || printf 'extra\n' >"$output/unexpected.txt"
  [ "${EXTRA_COMBINED_DIRECTORY-}" != 1 ] || mkdir -p "$output/extra/payload"
  if [ "${SYMLINK_COMBINED_REPORT-}" = 1 ]; then
    cp "$output/combined-report.json" "$output/real-report.json"
    rm "$output/combined-report.json"
    ln -s real-report.json "$output/combined-report.json"
    rm "$output/SHA256SUMS"
    (cd "$output" && {
      shasum -a 256 combined-report.json combined-summary.csv |
        awk '{print $1 "  " $2}' >SHA256SUMS
    })
  fi
  [ "${TAMPER_ARTIFACT_DURING_COMBINE-}" != 1 ] || \
    { chmod u+w "$rocksdb_artifact/raw/proxy.json";
      printf 'tampered artifact payload\n' >>"$rocksdb_artifact/raw/proxy.json"; }
  [ "${TAMPER_PERSISTENT_ARTIFACT_DURING_COMBINE-}" != 1 ] || \
    printf 'tampered persistent artifact payload\n' \
      >>"$(dirname "$output")/isolated-rocksdb/raw/proxy.json"
fi
EOF
  chmod +x "$orchestrator"
  jq -n --arg backend "$selected_backend" --arg run_id "$run_id" \
    '{selected_backend: $backend, run_id: $run_id}' >"$bundle/formal-spec.json"
  jq -n \
    --arg backend "$selected_backend" \
    --arg host "paper-$selected_backend" \
    --argjson pid "$pid" \
    --argjson gateway_pid "$gateway_pid" '
    {
      schema_version: 1,
      backends: {($backend): {}},
      proxy_targets: [{
        backend: $backend,
        gateway_process: {
          pid: $gateway_pid,
          executable: "/opt/dtgproxy/gateway"
        },
        data_node_processes: [{
          pid: $pid,
          executable: "/opt/dtgproxy/data-node",
          executable_sha256: ("a" * 64),
          probe: {
            ssh_target: $host,
            host_id: $host,
            boot_id: "boot-1",
            probe_binary: "/opt/dtgproxy/probe",
            data_interface: "eth0"
          }
        }]
      }]
    }' >"$bundle/runtime-manifest.json"
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
    --argjson pid "$pid" \
    --argjson gateway_pid "$gateway_pid" '
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
          identity: {
            host_id: "local-fixture",
            boot_id: "boot-local",
            process_start_id: ("start-local-" + $backend),
            pid: $gateway_pid
          },
          executable: "/opt/dtgproxy/gateway",
          executable_sha256: ("c" * 64),
          probe: {kind: "local"}
        }
      ],
      backend_service: (if $backend == "rocksdb" then
        {ownership: "embedded", managed_by: "data_node"}
      else
        {ownership: "lifecycle_runner", runtime_role: "backend_service"}
      end)
    }' >"$bundle/managed-process-evidence.json"
  jq -n \
    --arg backend "$selected_backend" \
    --arg run_id "$run_id" \
    --arg executor "$executor" \
    --arg executor_sha256 "$(sha256_file "$executor")" \
    --arg orchestrator "$orchestrator" \
    --arg orchestrator_sha256 "$(sha256_file "$orchestrator")" \
    --arg lifecycle_runner "$scratch/bin/dtgproxy-lifecycle-runner" \
    --arg lifecycle_runner_sha256 "$(sha256_file "$scratch/bin/dtgproxy-lifecycle-runner")" '
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
        orchestrator_sha256: $orchestrator_sha256,
        lifecycle_runner: {
          protocol_version: 1,
          path: $lifecycle_runner,
          sha256: $lifecycle_runner_sha256
        }
      }
    }' >"$bundle/READY.json"
  seal_bundle "$bundle"
  printf '%s\n' "$bundle"
}

cat >"$scratch/bin/dtgproxy-lifecycle-runner" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail

command=${1-}
shift || true
bundle=
output_root=
evidence_output=
while (($#)); do
  case "$1" in
    --prepared-bundle) bundle=$2; shift 2 ;;
    --output-root) output_root=$2; shift 2 ;;
    --evidence-output) evidence_output=$2; shift 2 ;;
    *) printf 'unexpected lifecycle argument: %s\n' "$1" >&2; exit 90 ;;
  esac
done
backend=$(jq -r .selected_backend "$bundle/READY.json")
run_id=$(jq -r .run_id "$bundle/READY.json")
case "$command" in
  preflight)
    [[ -n $bundle && -z $output_root && -z $evidence_output ]]
    printf 'preflight %s\n' "$backend" >>"$ISOLATED_EVENT_LOG"
    [[ ${PREFLIGHT_FAIL_BACKEND-} != "$backend" ]] || exit 72
    ;;
  run)
    [[ -n $bundle && -n $output_root && -n $evidence_output ]]
    printf 'lifecycle-run %s\n' "$backend" >>"$ISOLATED_EVENT_LOG"
    "$LIFECYCLE_RUN_SCRIPT" --prepared-bundle "$bundle" --output-root "$output_root"
    gateway_pid=$(jq -r '.proxy_targets[0].gateway_process.pid' "$bundle/runtime-manifest.json")
    data_pid=$(jq -r '.proxy_targets[0].data_node_processes[0].pid' "$bundle/runtime-manifest.json")
    data_host=$(jq -r '.proxy_targets[0].data_node_processes[0].probe.host_id' "$bundle/runtime-manifest.json")
    artifact="$output_root/$run_id"
    mkdir -p "$artifact/raw"
    jq -n \
      --arg backend "$backend" \
      --arg host "$data_host" \
      --arg start "start-$backend" \
      --argjson pid "$data_pid" '
      {
        path: "proxy",
        backend: $backend,
        topology_evidence: {
          deployment_mode: "remote_formal",
          data_nodes: [{
            host_id: $host,
            boot_id: "boot-1",
            process_start_id: $start,
            pid: $pid,
            executable: "/opt/dtgproxy/data-node",
            executable_sha256: ("a" * 64),
            listen_address: "10.0.0.1:7000",
            data_interface: "eth0",
            management_interface: "eth1"
          }]
        }
      }' >"$artifact/raw/proxy.json"
    (cd "$artifact" && {
      shasum -a 256 manifest.json raw/proxy.json |
        awk '{print $1 "  " $2}' >SHA256SUMS
    })
    evidence_backend=$backend
    evidence_run_id=$run_id
    [[ ${WRONG_EVIDENCE_BACKEND-} != "$backend" ]] || evidence_backend=neo4j
    [[ ${WRONG_EVIDENCE_RUN_ID_BACKEND-} != "$backend" ]] || evidence_run_id=wrong-run
    include_service=false
    [[ $backend == rocksdb ]] || include_service=true
    [[ ${MISSING_SERVICE_BACKEND-} != "$backend" ]] || include_service=false
    [[ ${ROCKS_EXTERNAL_SERVICE-0} != 1 || $backend != rocksdb ]] || include_service=true
    jq -n \
      --arg backend "$evidence_backend" \
      --arg run_id "$evidence_run_id" \
      --arg actual_backend "$backend" \
      --arg data_host "$data_host" \
      --arg data_start "start-$backend" \
      --argjson gateway_pid "$gateway_pid" \
      --argjson data_pid "$data_pid" \
      --argjson include_service "$include_service" '
      def remote_process($role; $pid; $host; $boot; $start; $executable; $digest): {
        backend: $backend,
        role: $role,
        identity: {
          host_id: $host,
          boot_id: $boot,
          process_start_id: $start,
          pid: $pid
        },
        executable: $executable,
        executable_sha256: $digest,
        probe: {
          kind: "remote",
          ssh_target: $host,
          probe_binary: "/opt/dtgproxy/probe",
          network_interface: "eth0"
        }
      };
      [
        remote_process(
          "gateway"; $gateway_pid; ("runtime-" + $actual_backend + "-gateway");
          "runtime-boot-1"; ("runtime-start-" + $actual_backend + "-gateway");
          "/opt/dtgproxy/gateway"; ("c" * 64)
        ),
        remote_process(
          "data_node"; $data_pid; $data_host; "boot-1"; $data_start;
          "/opt/dtgproxy/data-node"; ("a" * 64)
        )
      ] +
      (if $include_service then [
        remote_process(
          "backend_service"; 5103; ("runtime-" + $actual_backend + "-backend_service");
          "runtime-boot-1"; ("runtime-start-" + $actual_backend + "-backend_service");
          ("/opt/dtgproxy/" + $actual_backend); ("d" * 64)
        )
      ] else [] end) |
      {
        schema_version: 1,
        selected_backend: $backend,
        run_id: $run_id,
        managed_processes: .
      }' >"$evidence_output"
    if [[ ${DUPLICATE_EVIDENCE_BACKEND-} == "$backend" ]]; then
      jq '.managed_processes[1].identity = .managed_processes[0].identity' \
        "$evidence_output" >"$evidence_output.new"
      mv "$evidence_output.new" "$evidence_output"
    fi
    if [[ ${UNBOUND_DATA_NODE_BACKEND-} == "$backend" ]]; then
      jq '(.managed_processes[] | select(.role == "data_node") |
        .identity.process_start_id) = "unbound-start"' \
        "$evidence_output" >"$evidence_output.new"
      mv "$evidence_output.new" "$evidence_output"
    fi
    if [[ ${UNBOUND_GATEWAY_BACKEND-} == "$backend" ]]; then
      jq '(.managed_processes[] | select(.role == "gateway") | .identity.pid) += 1' \
        "$evidence_output" >"$evidence_output.new"
      mv "$evidence_output.new" "$evidence_output"
    fi
    if [[ ${UNBOUND_GATEWAY_DIGEST_BACKEND-} == "$backend" ]]; then
      jq '(.managed_processes[] | select(.role == "gateway") |
        .executable_sha256) = ("e" * 64)' \
        "$evidence_output" >"$evidence_output.new"
      mv "$evidence_output.new" "$evidence_output"
    fi
    ;;
  *) exit 91 ;;
esac
EOF
chmod +x "$scratch/bin/dtgproxy-lifecycle-runner"
export LIFECYCLE_RUN_SCRIPT="$run_script"

cat >"$scratch/bin/ssh" <<'EOF'
#!/bin/sh
set -eu
while [ "${1-}" != -- ]; do shift; done
shift
host=$1
shift
identity_name=${host#runtime-}
backend=${identity_name%%-*}
role=${identity_name#*-}
if [ "$identity_name" = "$host" ]; then
  backend=${host#paper-}
  role=data_node
  boot_id=boot-1
  start="start-$backend"
else
  boot_id=runtime-boot-1
  start="runtime-start-$backend-$role"
fi
if [ "${1-}" = true ]; then
  exit 0
fi
if [ "${1-}" = ps ]; then
  pid=$3
  [ "${ABSENT_BACKEND-}" = "$backend" ] && exit 1
  printf ' %s\n' "$pid"
  exit 0
fi
printf 'identity %s %s\n' "$backend" "$role" >>"$ISOLATED_EVENT_LOG"
[ "${ABSENT_BACKEND-}" != "$backend" ] || exit 3
[ "${BROKEN_PROBE_BACKEND-}" != "$backend" ] || exit 4
pid=
while [ $# -gt 0 ]; do
  if [ "$1" = --pid ]; then pid=$2; fi
  shift
done
if [ "${STICKY_BACKEND-}" = "$backend" ] &&
   { [ -z "${STICKY_ROLE-}" ] || [ "${STICKY_ROLE-}" = "$role" ]; }; then
  :
else
  start="new-$start"
fi
[ "${WRONG_HOST_BACKEND-}" != "$backend" ] || host="wrong-$host"
[ "${MALFORMED_IDENTITY_BACKEND-}" != "$backend" ] || {
  jq -cn --arg host "$host" --argjson pid "$pid" \
    --arg boot_id "$boot_id" \
    '{host_id: $host, boot_id: $boot_id, pid: $pid}'
  exit 0
}
jq -cn --arg host "$host" --arg start "$start" --arg boot_id "$boot_id" \
  --argjson pid "$pid" '
  {host_id: $host, boot_id: $boot_id, process_start_id: $start, pid: $pid}'
EOF
chmod +x "$scratch/bin/ssh"

rocksdb_bundle=$(make_isolated_bundle rocksdb isolated-rocksdb)
postgresql_bundle=$(make_isolated_bundle postgresql isolated-postgresql)
neo4j_bundle=$(make_isolated_bundle neo4j isolated-neo4j)

expect_isolated_preflight_rejected() {
  local name=$1
  local expected=$2
  local rocksdb=$3
  local postgresql=$4
  local neo4j=$5
  local events="$scratch/$name.events"
  local log="$scratch/$name.log"
  if ISOLATED_EVENT_LOG="$events" PATH="$scratch/bin:$PATH" \
    "$isolated_script" \
    --rocksdb-bundle "$rocksdb" \
    --postgresql-bundle "$postgresql" \
    --neo4j-bundle "$neo4j" \
    --output-root "$scratch/$name-output" >"$log" 2>&1; then
    fail "$name unexpectedly passed"
  fi
  grep -F "$expected" "$log" >/dev/null || {
    cat "$log" >&2
    fail "$name failed for the wrong reason"
  }
  [[ ! -e $events ]] || fail "$name launched a backend"
}

missing_inventory_process_bundle="$scratch/isolated-postgresql-missing-inventory-process"
cp -R "$postgresql_bundle" "$missing_inventory_process_bundle"
jq '.proxy_targets[0].data_node_processes += [{
  pid: 424242,
  executable: "/opt/dtgproxy/data-node",
  executable_sha256: ("d" * 64),
  probe: {
    ssh_target: "paper-postgresql-extra",
    host_id: "paper-postgresql-extra",
    boot_id: "boot-extra",
    probe_binary: "/opt/dtgproxy/probe",
    data_interface: "eth0"
  }
}]' "$missing_inventory_process_bundle/runtime-manifest.json" \
  >"$missing_inventory_process_bundle/runtime-manifest.json.new"
mv "$missing_inventory_process_bundle/runtime-manifest.json.new" \
  "$missing_inventory_process_bundle/runtime-manifest.json"
seal_bundle "$missing_inventory_process_bundle"
expect_isolated_preflight_rejected \
  missing-inventory-process 'managed process evidence differs from runtime inventory' \
  "$rocksdb_bundle" "$missing_inventory_process_bundle" "$neo4j_bundle"
printf '%s\n' 'PASS isolated runner rejects runtime processes omitted from managed evidence before launch'

duplicate_run_bundle="$scratch/isolated-postgresql-duplicate-run-id"
cp -R "$postgresql_bundle" "$duplicate_run_bundle"
jq --arg run_id isolated-rocksdb '.run_id = $run_id' \
  "$duplicate_run_bundle/READY.json" >"$duplicate_run_bundle/READY.json.new"
mv "$duplicate_run_bundle/READY.json.new" "$duplicate_run_bundle/READY.json"
jq --arg run_id isolated-rocksdb '.run_id = $run_id' \
  "$duplicate_run_bundle/formal-spec.json" >"$duplicate_run_bundle/formal-spec.json.new"
mv "$duplicate_run_bundle/formal-spec.json.new" "$duplicate_run_bundle/formal-spec.json"
seal_bundle "$duplicate_run_bundle"
expect_isolated_preflight_rejected \
  duplicate-run-id 'prepared bundles must have distinct run_id values' \
  "$rocksdb_bundle" "$duplicate_run_bundle" "$neo4j_bundle"
printf '%s\n' 'PASS isolated runner rejects duplicate run IDs before launch'

wrong_schema_bundle="$scratch/isolated-postgresql-wrong-managed-schema"
cp -R "$postgresql_bundle" "$wrong_schema_bundle"
jq '.schema_version = 2' "$wrong_schema_bundle/managed-process-evidence.json" \
  >"$wrong_schema_bundle/managed-process-evidence.json.new"
mv "$wrong_schema_bundle/managed-process-evidence.json.new" \
  "$wrong_schema_bundle/managed-process-evidence.json"
seal_bundle "$wrong_schema_bundle"
expect_isolated_preflight_rejected \
  wrong-managed-schema 'managed process evidence mismatch' \
  "$rocksdb_bundle" "$wrong_schema_bundle" "$neo4j_bundle"
printf '%s\n' 'PASS isolated runner requires the managed evidence schema contract before launch'

wrong_lifecycle_bundle="$scratch/isolated-postgresql-wrong-lifecycle"
cp -R "$postgresql_bundle" "$wrong_lifecycle_bundle"
jq '.lifecycle_contract = "runner may terminate processes"' \
  "$wrong_lifecycle_bundle/managed-process-evidence.json" \
  >"$wrong_lifecycle_bundle/managed-process-evidence.json.new"
mv "$wrong_lifecycle_bundle/managed-process-evidence.json.new" \
  "$wrong_lifecycle_bundle/managed-process-evidence.json"
seal_bundle "$wrong_lifecycle_bundle"
expect_isolated_preflight_rejected \
  wrong-lifecycle 'managed process evidence mismatch' \
  "$rocksdb_bundle" "$wrong_lifecycle_bundle" "$neo4j_bundle"
printf '%s\n' 'PASS isolated runner requires the no-signal lifecycle contract before launch'

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

changed_lifecycle_bundle="$scratch/isolated-postgresql-changed-lifecycle"
cp -R "$postgresql_bundle" "$changed_lifecycle_bundle"
changed_lifecycle="$scratch/changed-lifecycle-runner"
cp "$scratch/bin/dtgproxy-lifecycle-runner" "$changed_lifecycle"
chmod +x "$changed_lifecycle"
claimed_lifecycle_digest=$(sha256_file "$changed_lifecycle")
printf '# changed after preparation\n' >>"$changed_lifecycle"
jq --arg path "$changed_lifecycle" --arg digest "$claimed_lifecycle_digest" '
  .formal_run.lifecycle_runner.path = $path |
  .formal_run.lifecycle_runner.sha256 = $digest
' "$changed_lifecycle_bundle/READY.json" >"$changed_lifecycle_bundle/READY.json.new"
mv "$changed_lifecycle_bundle/READY.json.new" "$changed_lifecycle_bundle/READY.json"
seal_bundle "$changed_lifecycle_bundle"
expect_isolated_preflight_rejected \
  changed-lifecycle-runner 'sealed lifecycle runner identity mismatch' \
  "$rocksdb_bundle" "$changed_lifecycle_bundle" "$neo4j_bundle"
printf '%s\n' 'PASS isolated runner rejects a changed sealed lifecycle runner before launch'

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
grep -F 'backend service lifecycle ownership evidence is invalid' "$scratch/missing-service.log" >/dev/null || {
  cat "$scratch/missing-service.log" >&2
  fail "missing backend service ownership failed for the wrong reason"
}
[[ ! -e $scratch/missing-service.events ]] || fail "missing service evidence launched a backend"
printf '%s\n' 'PASS isolated runner requires sealed backend service ownership evidence'

expect_runtime_evidence_rejected() {
  local name=$1
  local expected=$2
  local setting=$3
  local forbidden_backend=${4:-postgresql}
  local events="$scratch/$name.events"
  local output="$scratch/$name-output"
  local log="$scratch/$name.log"
  if env "$setting" ISOLATED_EVENT_LOG="$events" PATH="$scratch/bin:$PATH" \
    "$isolated_script" \
    --rocksdb-bundle "$rocksdb_bundle" \
    --postgresql-bundle "$postgresql_bundle" \
    --neo4j-bundle "$neo4j_bundle" \
    --output-root "$output" >"$log" 2>&1; then
    fail "$name unexpectedly passed"
  fi
  grep -F -- "$expected" "$log" >/dev/null || {
    cat "$log" >&2
    fail "$name failed for the wrong reason"
  }
  ! grep -F "lifecycle-run $forbidden_backend" "$events" >/dev/null 2>&1 || \
    fail "$name launched the next backend"
  [[ ! -e $output/combined ]] || fail "$name created combined output"
}

expect_runtime_evidence_rejected \
  wrong-runtime-backend 'runtime evidence selected_backend mismatch' \
  WRONG_EVIDENCE_BACKEND=rocksdb
expect_runtime_evidence_rejected \
  wrong-runtime-run-id 'runtime evidence run_id mismatch' \
  WRONG_EVIDENCE_RUN_ID_BACKEND=rocksdb
expect_runtime_evidence_rejected \
  duplicate-runtime-identity 'runtime evidence contains duplicate process identities' \
  DUPLICATE_EVIDENCE_BACKEND=rocksdb
expect_runtime_evidence_rejected \
  missing-runtime-service 'postgresql runtime evidence requires exactly one backend_service' \
  MISSING_SERVICE_BACKEND=postgresql neo4j
expect_runtime_evidence_rejected \
  rocksdb-external-service 'rocksdb runtime evidence must not contain backend_service' \
  ROCKS_EXTERNAL_SERVICE=1
expect_runtime_evidence_rejected \
  unbound-runtime-data-node 'runtime data_node identity is not bound to verified Proxy observations' \
  UNBOUND_DATA_NODE_BACKEND=rocksdb
expect_runtime_evidence_rejected \
  unbound-runtime-gateway 'runtime gateway identity is not bound to the sealed runtime manifest' \
  UNBOUND_GATEWAY_BACKEND=rocksdb
expect_runtime_evidence_rejected \
  unbound-runtime-gateway-digest 'runtime gateway executable is not bound to preparation evidence' \
  UNBOUND_GATEWAY_DIGEST_BACKEND=rocksdb
printf '%s\n' 'PASS isolated runner validates actual runtime evidence before advancing'

events="$scratch/preflight-failure.events"
output="$scratch/preflight-failure-output"
if ISOLATED_EVENT_LOG="$events" PREFLIGHT_FAIL_BACKEND=postgresql PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/preflight-failure.log" 2>&1; then
  fail "isolated runner continued after lifecycle preflight failure"
fi
[[ $(cat "$events") == 'preflight rocksdb
preflight postgresql' ]] || fail "preflight failure did not stop before backend launch"
[[ ! -e $output/combined ]] || fail "preflight failure created combined output"
printf '%s\n' 'PASS isolated runner fails closed when any lifecycle preflight fails'

events="$scratch/isolated-success.events"
output="$scratch/isolated-output"
ISOLATED_EVENT_LOG="$events" PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output"
expected_events='preflight rocksdb
preflight postgresql
preflight neo4j
lifecycle-run rocksdb
run rocksdb
verify rocksdb
identity rocksdb gateway
identity rocksdb data_node
preflight rocksdb
preflight postgresql
preflight neo4j
lifecycle-run postgresql
run postgresql
verify postgresql
identity postgresql gateway
identity postgresql data_node
identity postgresql backend_service
preflight rocksdb
preflight postgresql
preflight neo4j
lifecycle-run neo4j
run neo4j
verify neo4j
identity neo4j gateway
identity neo4j data_node
identity neo4j backend_service
preflight rocksdb
preflight postgresql
preflight neo4j
combine rocksdb
verify rocksdb
verify postgresql
verify neo4j'
[[ $(cat "$events") == "$expected_events" ]] || fail "isolated runner order mismatch"
[[ -f $output/combined/combined-report.json ]] || fail "isolated runner did not create combined output"
jq -e --arg output "$output" '
  all(.backends[];
    .artifact == ($output + "/" + .verification.run_id))
' "$output/combined/combined-report.json" >/dev/null || \
  fail "combined report does not retain persistent artifact paths"
while IFS= read -r artifact_path; do
  [[ -d $artifact_path ]] || fail "combined report artifact path does not exist after return"
done < <(jq -r '.backends[].artifact' "$output/combined/combined-report.json")
jq -e '
  .schema_version == 1 and
  [.runs[].backend] == ["rocksdb", "postgresql", "neo4j"] and
  all(.runs[];
    (.run_id | type == "string" and length > 0) and
    (.artifact_sha256 | test("^[0-9a-f]{64}$")) and
    (.runtime_evidence_sha256 | test("^[0-9a-f]{64}$")))
' "$output/combined/isolation-evidence.json" >/dev/null || \
  fail "isolated runner did not bind actual runtime evidence"
for backend in rocksdb postgresql neo4j; do
  run_id="isolated-$backend"
  expected_digest=$(sha256_file "$output/$run_id/SHA256SUMS")
  jq -e --arg backend "$backend" --arg run_id "$run_id" --arg digest "$expected_digest" '
    any(.runs[];
      .backend == $backend and .run_id == $run_id and
      .artifact_sha256 == $digest)
  ' "$output/combined/isolation-evidence.json" >/dev/null || \
    fail "$backend isolation binding does not match the verified artifact"
done
grep -E '^[0-9a-f]{64}  isolation-evidence\.json$' \
  "$output/combined/SHA256SUMS" >/dev/null || \
  fail "combined checksums omit isolation evidence"
(cd "$output/combined" && shasum -a 256 -c SHA256SUMS >/dev/null) || \
  fail "combined isolation binding checksum does not verify"
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

events="$scratch/isolated-artifact-recheck.events"
output="$scratch/isolated-artifact-recheck-output"
if ISOLATED_EVENT_LOG="$events" TAMPER_ARTIFACT_DURING_COMBINE=1 \
  PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-artifact-recheck.log" 2>&1; then
  fail "isolated runner accepted an artifact checksum changed during combine"
fi
grep -F 'artifact verification failed before publication' \
  "$scratch/isolated-artifact-recheck.log" >/dev/null || {
  cat "$scratch/isolated-artifact-recheck.log" >&2
  fail "artifact checksum recheck failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "changed artifact checksum retained combined output"
printf '%s\n' 'PASS isolated runner rechecks artifact bindings before publication'

events="$scratch/isolated-post-verify-tamper.events"
output="$scratch/isolated-post-verify-tamper-output"
if ISOLATED_EVENT_LOG="$events" TAMPER_ARTIFACT_AFTER_FINAL_VERIFY=rocksdb \
  PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-post-verify-tamper.log" 2>&1; then
  fail "isolated runner accepted an artifact changed after final verification"
fi
grep -F 'artifact checksum verification failed before publication' \
  "$scratch/isolated-post-verify-tamper.log" >/dev/null || {
  cat "$scratch/isolated-post-verify-tamper.log" >&2
  fail "post-verification artifact tamper failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "post-verification artifact tamper retained combined output"
printf '%s\n' 'PASS isolated runner rechecks full snapshot contents after final verification'

events="$scratch/isolated-persistent-tamper.events"
output="$scratch/isolated-persistent-tamper-output"
if ISOLATED_EVENT_LOG="$events" TAMPER_PERSISTENT_ARTIFACT_DURING_COMBINE=1 \
  PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-persistent-tamper.log" 2>&1; then
  fail "isolated runner published a report pointing to a tampered persistent artifact"
fi
grep -F 'persistent artifact checksum verification failed before publication' \
  "$scratch/isolated-persistent-tamper.log" >/dev/null || {
  cat "$scratch/isolated-persistent-tamper.log" >&2
  fail "persistent artifact tamper failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "persistent artifact tamper retained combined output"
printf '%s\n' 'PASS isolated runner fully rechecks persistent artifacts before path publication'

events="$scratch/isolated-extra-combined.events"
output="$scratch/isolated-extra-combined-output"
if ISOLATED_EVENT_LOG="$events" EXTRA_COMBINED_FILE=1 PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-extra-combined.log" 2>&1; then
  fail "isolated runner accepted an extra combined file"
fi
grep -F 'combined file set is invalid' "$scratch/isolated-extra-combined.log" >/dev/null || {
  cat "$scratch/isolated-extra-combined.log" >&2
  fail "extra combined file failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "extra combined file retained combined output"
printf '%s\n' 'PASS isolated runner rejects extra combined package files'

events="$scratch/isolated-extra-directory.events"
output="$scratch/isolated-extra-directory-output"
if ISOLATED_EVENT_LOG="$events" EXTRA_COMBINED_DIRECTORY=1 PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-extra-directory.log" 2>&1; then
  fail "isolated runner accepted an extra combined directory"
fi
grep -F 'combined file set is invalid' "$scratch/isolated-extra-directory.log" >/dev/null || {
  cat "$scratch/isolated-extra-directory.log" >&2
  fail "extra combined directory failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "extra combined directory retained combined output"
printf '%s\n' 'PASS isolated runner rejects extra combined directories'

events="$scratch/isolated-symlink-report.events"
output="$scratch/isolated-symlink-report-output"
if ISOLATED_EVENT_LOG="$events" SYMLINK_COMBINED_REPORT=1 PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-symlink-report.log" 2>&1; then
  fail "isolated runner accepted a symlinked combined report"
fi
grep -F 'combined file set is invalid' "$scratch/isolated-symlink-report.log" >/dev/null || {
  cat "$scratch/isolated-symlink-report.log" >&2
  fail "symlinked combined report failed for the wrong reason"
}
[[ ! -e $output/combined ]] || fail "symlinked combined report retained combined output"
printf '%s\n' 'PASS isolated runner rejects symlinked combined files'

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

rocksdb_first_probe_events='preflight rocksdb
preflight postgresql
preflight neo4j
lifecycle-run rocksdb
run rocksdb
verify rocksdb
identity rocksdb gateway'

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
[[ $(cat "$events") == "$rocksdb_first_probe_events" ]] || \
  fail "broken identity probe did not stop before the next backend"
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
[[ $(cat "$events") == "$rocksdb_first_probe_events" ]] || \
  fail "malformed process identity did not stop before the next backend"
printf '%s\n' 'PASS isolated runner fails closed on malformed process identity'

events="$scratch/isolated-wrong-host.events"
output="$scratch/isolated-wrong-host-output"
if ISOLATED_EVENT_LOG="$events" WRONG_HOST_BACKEND=rocksdb PATH="$scratch/bin:$PATH" \
  "$isolated_script" \
  --rocksdb-bundle "$rocksdb_bundle" \
  --postgresql-bundle "$postgresql_bundle" \
  --neo4j-bundle "$neo4j_bundle" \
  --output-root "$output" >"$scratch/isolated-wrong-host.log" 2>&1; then
  fail "isolated runner accepted a managed PID probe from a different host"
fi
grep -F 'managed PID probe resolved to a different host identity' \
  "$scratch/isolated-wrong-host.log" >/dev/null || {
  cat "$scratch/isolated-wrong-host.log" >&2
  fail "wrong-host identity failed for the wrong reason"
}
[[ $(cat "$events") == "$rocksdb_first_probe_events" ]] || \
  fail "wrong-host identity did not stop before the next backend"
[[ ! -e $output/combined ]] || fail "wrong-host identity created combined output"
printf '%s\n' 'PASS isolated runner fails closed when a probe resolves to another host'

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
expected_failure_events='preflight rocksdb
preflight postgresql
preflight neo4j
lifecycle-run rocksdb
run rocksdb
verify rocksdb
identity rocksdb gateway
identity rocksdb data_node
preflight rocksdb
preflight postgresql
preflight neo4j
lifecycle-run postgresql
run postgresql'
[[ $(cat "$events") == "$expected_failure_events" ]] || \
  fail "isolated runner failure-stop order mismatch"
[[ ! -e $output/combined ]] || fail "failed isolated run created combined output"
printf '%s\n' 'PASS isolated runner stops on failure without combined output'

events="$scratch/isolated-identity.events"
output="$scratch/isolated-identity-output"
if ISOLATED_EVENT_LOG="$events" STICKY_BACKEND=rocksdb STICKY_ROLE=gateway \
  PATH="$scratch/bin:$PATH" \
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
[[ $(cat "$events") == "$rocksdb_first_probe_events" ]] || \
  fail "identity failure did not stop before the next backend"
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

grep -F 'os.O_EXCL' "$isolated_script" >/dev/null || \
  fail "isolation evidence publication does not use no-replace creation"
printf '%s\n' 'PASS isolation evidence publication uses no-replace creation'
