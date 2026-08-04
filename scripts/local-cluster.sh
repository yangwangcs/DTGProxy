#!/usr/bin/env bash
set -euo pipefail

readonly repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly default_root="$repo_root/target/local-cluster"
readonly capabilities="adjacency,immutable-read-view,logical-snapshot,point"

cluster_root="$default_root"
postgres_url=""
postgres_endpoint=""
postgres_credential=""
managed_postgres=false
postgres_started=false
active_process_id=""
backend_kind="fjall"

usage() {
  cat >&2 <<'USAGE'
usage:
  scripts/local-cluster.sh start --backend <fjall|postgresql|kuzu> [--managed-postgres | --postgres-url URL] [--root PATH]
  scripts/local-cluster.sh stop [--root PATH]
  scripts/local-cluster.sh status [--root PATH]

`--managed-postgres` and `--postgres-url` are only valid with
`--backend postgresql`. The managed option creates a disposable, loopback-only
PostgreSQL instance owned by the selected runtime directory.
USAGE
}

fail() {
  printf '%s\n' "$*" >&2
  exit 1
}

require_tool() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required tool: $1"
}

validate_root() {
  local create_parent="${1:-false}"
  local parent target_root
  if [[ $cluster_root != /* ]]; then
    cluster_root="$repo_root/$cluster_root"
  fi
  [[ "/$cluster_root/" != *'/../'* && "/$cluster_root/" != *'/./'* ]] || \
    fail "runtime root must not contain relative path segments"
  [[ $cluster_root == "$repo_root/target/"* ]] || \
    fail "runtime root must be below $repo_root/target"
  parent="$(dirname "$cluster_root")"
  if [[ $create_parent == true ]]; then
    mkdir -p "$parent"
  else
    [[ -d $parent ]] || fail "runtime root parent does not exist: $parent"
  fi
  parent="$(cd "$(dirname "$cluster_root")" && pwd -P)"
  cluster_root="$parent/$(basename "$cluster_root")"
  target_root="$(cd "$repo_root/target" && pwd -P)"
  [[ $cluster_root == "$target_root/"* ]] || \
    fail "runtime root must be below $repo_root/target"
}

runtime_path() {
  printf '%s/%s\n' "$cluster_root" "$1"
}

port_is_listening() {
  lsof -nP -iTCP:"$1" -sTCP:LISTEN >/dev/null 2>&1
}

wait_for_port() {
  local port="$1"
  local name="$2"
  local process_id="$3"
  for _ in $(seq 1 150); do
    port_is_listening "$port" && return 0
    if ! kill -0 "$process_id" 2>/dev/null; then
      printf '%s exited before listening; log follows:\n' "$name" >&2
      cat "$(runtime_path "logs/$name.log")" >&2 || true
      return 1
    fi
    sleep 0.1
  done
  fail "$name did not listen on 127.0.0.1:$port"
}

wait_for_postgres() {
  local port="$1"
  for _ in $(seq 1 150); do
    pg_isready -h 127.0.0.1 -p "$port" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
  fail "managed PostgreSQL did not become ready on 127.0.0.1:$port"
}

terminate_process_tree() {
  local process_id="$1"
  local child_id
  [[ $process_id =~ ^[0-9]+$ ]] || return 0
  while IFS= read -r child_id; do
    [[ -n $child_id ]] || continue
    terminate_process_tree "$child_id"
  done < <(pgrep -P "$process_id" 2>/dev/null || true)
  kill -TERM "$process_id" 2>/dev/null || true
  for _ in 1 2 3 4 5; do
    kill -0 "$process_id" 2>/dev/null || return 0
    sleep 1
  done
  kill -KILL "$process_id" 2>/dev/null || true
}

find_free_port() {
  local port="$1"
  local maximum="$2"
  while (( port <= maximum )); do
    if ! port_is_listening "$port"; then
      printf '%s\n' "$port"
      return 0
    fi
    port=$((port + 1))
  done
  fail "no available TCP port in range"
}

write_config() {
  local path="$1"
  local body="$2"
  printf '%s\n' "$body" >"$path"
}

start_process() {
  local name="$1"
  local port="$2"
  shift 2
  local binary="$1"
  shift
  local log="$(runtime_path "logs/$name.log")"
  local pid_file="$(runtime_path "pids/$name.pid")"
  [[ -x $binary ]] || fail "required binary is absent: $binary"
  "$binary" "$@" >>"$log" 2>&1 &
  local process_id=$!
  printf '%s\n' "$process_id" >"$pid_file"
  wait_for_port "$port" "$name" "$process_id"
}

start_data() {
  local name="$1"
  local port="$2"
  local assignment="$3"
  local provider="$4"
  local data_root="$(runtime_path "$name")"
  local binary="$repo_root/target/debug/dtgproxy-data"
  local log="$(runtime_path "logs/$name.log")"
  local pid_file="$(runtime_path "pids/$name.pid")"
  mkdir -p "$data_root/business" "$data_root/kuzu" "$data_root/raft"
  case "$provider" in
    postgresql)
      DTG_DATA_RPC_ADDR="127.0.0.1:$port" \
      DTG_DATA_FJALL_ROOT="$data_root/business" \
      DTG_DATA_KUZU_ROOT="$data_root/kuzu" \
      DTG_DATA_CONSENSUS_ROOT="$data_root/raft" \
      DTG_DATA_BACKEND_KIND="$provider" \
      DTG_DATA_CAPABILITIES="$capabilities" \
      DTG_DATA_ASSIGNMENTS="$assignment" \
      DTG_DATA_POSTGRES_ENDPOINT="$postgres_endpoint" \
      DTG_DATA_POSTGRES_CREDENTIAL="$postgres_credential" \
      "$binary" >>"$log" 2>&1 &
      ;;
    *)
      DTG_DATA_RPC_ADDR="127.0.0.1:$port" \
      DTG_DATA_FJALL_ROOT="$data_root/business" \
      DTG_DATA_KUZU_ROOT="$data_root/kuzu" \
      DTG_DATA_CONSENSUS_ROOT="$data_root/raft" \
      DTG_DATA_BACKEND_KIND="$provider" \
      DTG_DATA_CAPABILITIES="$capabilities" \
      DTG_DATA_ASSIGNMENTS="$assignment" \
      "$binary" >>"$log" 2>&1 &
      ;;
  esac
  local process_id=$!
  printf '%s\n' "$process_id" >"$pid_file"
  wait_for_port "$port" "$name" "$process_id"
}

stop_process() {
  local name="$1"
  local pid_file="$(runtime_path "pids/$name.pid")"
  [[ -f $pid_file ]] || return 0
  local process_id
  process_id="$(<"$pid_file")"
  [[ $process_id =~ ^[0-9]+$ ]] || fail "invalid PID file: $pid_file"
  if kill -0 "$process_id" 2>/dev/null; then
    local command
    local binary_name
    case "$name" in
      data-*) binary_name="dtgproxy-data" ;;
      *) binary_name="dtgproxy-$name" ;;
    esac
    command="$(ps -p "$process_id" -o command= 2>/dev/null || true)"
    [[ $command == *"$binary_name"* ]] || \
      fail "refusing to stop PID $process_id: it is not the recorded $binary_name process"
    kill -TERM "$process_id"
    for _ in $(seq 1 50); do
      kill -0 "$process_id" 2>/dev/null || break
      sleep 0.1
    done
    kill -0 "$process_id" 2>/dev/null && kill -KILL "$process_id"
  fi
  rm -f "$pid_file"
}

stop_managed_postgres() {
  local data_directory="$(runtime_path postgres/data)"
  [[ $postgres_started == true || -f "$(runtime_path postgres/managed)" ]] || return 0
  [[ -d $data_directory ]] || fail "managed PostgreSQL has no data directory"
  pg_ctl -D "$data_directory" -m fast stop >/dev/null 2>&1 || true
}

stop_cluster() {
  if [[ -n $active_process_id ]]; then
    terminate_process_tree "$active_process_id"
    active_process_id=""
  fi
  for name in gateway data-3 data-2 data-1 controller meta; do
    stop_process "$name"
  done
  stop_managed_postgres
}

remove_runtime_root() {
  [[ -e $cluster_root ]] || return 0
  [[ -f "$(runtime_path owned)" ]] || \
    fail "refusing to remove runtime root without launcher ownership marker: $cluster_root"
  rm -rf "$cluster_root"
}

cleanup_and_exit() {
  local exit_code="$1"
  trap - ERR INT TERM
  stop_cluster
  remove_runtime_root
  exit "$exit_code"
}

start_managed_postgres() {
  require_tool initdb
  require_tool pg_ctl
  require_tool createdb
  require_tool pg_isready
  require_tool openssl
  local postgres_root="$(runtime_path postgres)"
  local data_directory="$postgres_root/data"
  local password_file="$postgres_root/password"
  local port
  port="$(find_free_port 55439 55539)"
  mkdir -p "$postgres_root"
  local password
  password="$(openssl rand -hex 24)"
  printf '%s\n' "$password" >"$password_file"
  chmod 600 "$password_file"
  initdb -D "$data_directory" -U dtgproxy --pwfile="$password_file" \
    --auth-local=trust --auth-host=scram-sha-256 --no-locale --encoding=UTF8 >/dev/null
  pg_ctl -D "$data_directory" -l "$postgres_root/postgres.log" \
    -o "-h 127.0.0.1 -p $port" start >/dev/null
  postgres_started=true
  wait_for_postgres "$port"
  PGPASSWORD="$password" createdb -h 127.0.0.1 -p "$port" -U dtgproxy dtgproxy
  postgres_endpoint="host=127.0.0.1 port=$port dbname=dtgproxy sslmode=disable"
  postgres_credential="user=dtgproxy password=$password application_name=dtgproxy-local"
  postgres_url="$postgres_endpoint $postgres_credential"
  printf '%s\n' "$postgres_url" >"$(runtime_path postgres/connection)"
  chmod 600 "$(runtime_path postgres/connection)"
  : >"$(runtime_path postgres/managed)"
}

prepare_binaries() {
  cargo build --locked --jobs 1 -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway --bins &
  active_process_id=$!
  wait "$active_process_id"
  active_process_id=""
}

start_cluster() {
  [[ ! -e $cluster_root ]] || fail "runtime root already exists; run stop or choose another --root"
  umask 077
  mkdir -p "$(runtime_path logs)" "$(runtime_path pids)"
  : >"$(runtime_path owned)"
  printf '%s\n' "$backend_kind" >"$(runtime_path backend-kind)"
  trap 'cleanup_and_exit $?' ERR
  trap 'cleanup_and_exit 130' INT
  trap 'cleanup_and_exit 143' TERM
  prepare_binaries
  if [[ $backend_kind == postgresql ]]; then
    if [[ $managed_postgres == true ]]; then
      start_managed_postgres
    else
      postgres_endpoint="$postgres_url"
      postgres_credential=""
      printf '%s\n' "$postgres_url" >"$(runtime_path postgres/connection)"
      chmod 600 "$(runtime_path postgres/connection)"
    fi
  fi

  local meta_port controller_port data_1_port data_2_port data_3_port gateway_port
  meta_port="$(find_free_port 55101 55120)"
  controller_port="$(find_free_port $((meta_port + 1)) 55130)"
  data_1_port="$(find_free_port $((controller_port + 1)) 55140)"
  data_2_port="$(find_free_port $((data_1_port + 1)) 55150)"
  data_3_port="$(find_free_port $((data_2_port + 1)) 55160)"
  gateway_port="$(find_free_port $((data_3_port + 1)) 55170)"
  write_config "$(runtime_path meta.json)" "$(jq -n --arg data "$(runtime_path meta)" --arg address "127.0.0.1:$meta_port" '{version:1,cluster_id:9001,node_id:1,listen_addr:$address,data_directory:$data,analytics_lease_duration:30,consensus_namespace:"local-cluster-meta",peers:[{node_id:1,raft_addr:$address,rpc_addr:$address}],security:{mode:"loopback_plaintext"}}')"
  write_config "$(runtime_path controller.json)" "$(jq -n --arg data "$(runtime_path controller)" --arg address "127.0.0.1:$controller_port" --arg meta "http://127.0.0.1:$meta_port" '{version:1,cluster_id:9001,node_id:2,listen_addr:$address,data_directory:$data,meta_endpoints:[$meta],security:{mode:"loopback_plaintext"}}')"

  start_process meta "$meta_port" "$repo_root/target/debug/dtgproxy-meta" --config "$(runtime_path meta.json)"
  start_process controller "$controller_port" "$repo_root/target/debug/dtgproxy-controller" --config "$(runtime_path controller.json)"
  start_data data-1 "$data_1_port" "9001:11:1:1:11:1:$backend_kind:1:1:local-$backend_kind-1" "$backend_kind"
  start_data data-2 "$data_2_port" "9001:11:2:1:12:1:$backend_kind:1:1:local-$backend_kind-2" "$backend_kind"
  start_data data-3 "$data_3_port" "9001:11:3:1:13:1:$backend_kind:1:1:local-$backend_kind-3" "$backend_kind"

  DTG_GATEWAY_BIND="127.0.0.1:$gateway_port" \
  DTG_GATEWAY_CLUSTER_ID=9001 \
  DTG_GATEWAY_REQUEST_TIMEOUT_MS=10000 \
  DTG_GATEWAY_CLUSTER_ENDPOINT="http://127.0.0.1:$data_1_port" \
  DTG_GATEWAY_SHARD_ENDPOINTS="1=http://127.0.0.1:$data_1_port,2=http://127.0.0.1:$data_2_port,3=http://127.0.0.1:$data_3_port" \
  DTG_GATEWAY_META_ENDPOINT="http://127.0.0.1:$meta_port" \
  DTG_GATEWAY_GRAPH_ID=11 \
  DTG_GATEWAY_CATALOG_VERSION=1 \
  DTG_GATEWAY_SCHEMA_VERSION=1 \
  DTG_GATEWAY_TRANSACTION_TIME=1 \
  DTG_GATEWAY_VALID_AT=1 \
  DTG_GATEWAY_LOGICAL_SCAN_BOUND=10 \
  DTG_GATEWAY_CAPABILITIES="$capabilities" \
  DTG_GATEWAY_SHARDS="1:1:11:1:0:$backend_kind:1:1:local-$backend_kind-1,2:1:12:1:0:$backend_kind:1:1:local-$backend_kind-2,3:1:13:1:0:$backend_kind:1:1:local-$backend_kind-3" \
  "$repo_root/target/debug/dtgproxy-gateway" >>"$(runtime_path logs/gateway.log)" 2>&1 &
  local gateway_process_id=$!
  printf '%s\n' "$gateway_process_id" >"$(runtime_path pids/gateway.pid)"
  wait_for_port "$gateway_port" gateway "$gateway_process_id"
  trap - ERR INT TERM
  printf 'local cluster is running: gateway bolt://127.0.0.1:%s\n' "$gateway_port"
  printf 'runtime root: %s\n' "$cluster_root"
}

status_cluster() {
  [[ -d $cluster_root ]] || fail "runtime root does not exist: $cluster_root"
  local configured_backend="unknown"
  if [[ -f "$(runtime_path backend-kind)" ]]; then
    configured_backend="$(<"$(runtime_path backend-kind)")"
  fi
  case "$configured_backend" in
    fjall|postgresql|kuzu) ;;
    *) configured_backend="unknown" ;;
  esac
  printf 'backend: %s\n' "$configured_backend"
  for name in meta controller data-1 data-2 data-3 gateway; do
    local pid_file="$(runtime_path "pids/$name.pid")"
    if [[ -f $pid_file ]] && kill -0 "$(<"$pid_file")" 2>/dev/null; then
      printf '%s: running (pid %s)\n' "$name" "$(<"$pid_file")"
    else
      printf '%s: stopped\n' "$name"
    fi
  done
  if [[ $configured_backend != postgresql ]]; then
    printf 'postgres: not selected\n'
  elif [[ -f "$(runtime_path postgres/managed)" ]]; then
    printf 'postgres: managed\n'
  elif [[ -f "$(runtime_path postgres/connection)" ]]; then
    printf 'postgres: external\n'
  else
    printf 'postgres: unknown\n'
  fi
}

[[ $# -ge 1 ]] || { usage; exit 2; }
command="$1"
shift
case "$command" in
  start)
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --backend)
          shift
          [[ $# -gt 0 ]] || { usage; exit 2; }
          backend_kind="$1"
          ;;
        --managed-postgres) managed_postgres=true ;;
        --postgres-url)
          shift
          [[ $# -gt 0 ]] || { usage; exit 2; }
          postgres_url="$1"
          ;;
        --root)
          shift
          [[ $# -gt 0 ]] || { usage; exit 2; }
          cluster_root="$1"
          ;;
        *) usage; exit 2 ;;
      esac
      shift
    done
    case "$backend_kind" in
      postgresql)
        [[ $managed_postgres == true && -z $postgres_url || $managed_postgres == false && -n $postgres_url ]] || \
          fail 'PostgreSQL start requires exactly one of --managed-postgres or --postgres-url URL'
        ;;
      fjall|kuzu)
        [[ $managed_postgres == false && -z $postgres_url ]] || \
          fail "--managed-postgres and --postgres-url require --backend postgresql"
        ;;
      *) fail '--backend must be fjall, postgresql, or kuzu' ;;
    esac
    validate_root true
    require_tool cargo
    require_tool jq
    require_tool lsof
    start_cluster
    ;;
  stop)
    [[ $# -eq 0 || $# -eq 2 && $1 == --root ]] || { usage; exit 2; }
    if [[ $# -eq 2 ]]; then cluster_root="$2"; fi
    validate_root
    stop_cluster
    remove_runtime_root
    ;;
  status)
    [[ $# -eq 0 || $# -eq 2 && $1 == --root ]] || { usage; exit 2; }
    if [[ $# -eq 2 ]]; then cluster_root="$2"; fi
    validate_root
    status_cluster
    ;;
  *) usage; exit 2 ;;
esac
