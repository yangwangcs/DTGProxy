#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

test_process_id=""
postgres_root=""
postgres_data=""
postgres_started=false

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

run_test() {
  (
    cd "$workspace_root"
    cargo test --locked --jobs 1 -p dtg-gateway --test live_provider_migrations -- \
      --ignored --test-threads=1 --nocapture
  ) &
  test_process_id=$!
  wait "$test_process_id"
  test_process_id=""
}

cleanup() {
  if [[ -n $test_process_id ]]; then
    terminate_process_tree "$test_process_id"
  fi
  if [[ $postgres_started == true ]]; then
    pg_ctl -D "$postgres_data" -m fast stop >/dev/null 2>&1 || true
  fi
  if [[ -n $postgres_root ]]; then
    find "$postgres_root" -depth -delete 2>/dev/null || true
  fi
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if [[ -n "${DTG_POSTGRES_URL:-}" ]]; then
  run_test
  exit 0
fi

for tool in initdb pg_ctl createdb pg_isready cargo openssl; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf '%s is required for simultaneous live provider migration certification\n' "$tool" >&2
    exit 78
  fi
done
postgres_root=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-provider-migrations.XXXXXX")
postgres_data="$postgres_root/postgres"
postgres_log="$postgres_root/postgres.log"
password_file="$postgres_root/password"
postgres_password=$(openssl rand -hex 24)

printf '%s\n' "$postgres_password" >"$password_file"
postgres_port=55540
while pg_isready -h 127.0.0.1 -p "$postgres_port" >/dev/null 2>&1; do
  postgres_port=$((postgres_port + 1))
  if ((postgres_port > 55640)); then
    printf 'no free local PostgreSQL test port in 55540..55640\n' >&2
    exit 78
  fi
done
initdb \
  -D "$postgres_data" \
  -U dtgproxy \
  --pwfile="$password_file" \
  --auth-local=trust \
  --auth-host=scram-sha-256 \
  --no-locale \
  --encoding=UTF8 >/dev/null
pg_ctl \
  -D "$postgres_data" \
  -l "$postgres_log" \
  -o "-h 127.0.0.1 -p $postgres_port" \
  start >/dev/null
postgres_started=true
PGPASSWORD="$postgres_password" createdb \
  -h 127.0.0.1 \
  -p "$postgres_port" \
  -U dtgproxy \
  dtgproxy

export DTG_POSTGRES_URL="host=127.0.0.1 port=$postgres_port user=dtgproxy password=$postgres_password dbname=dtgproxy sslmode=disable"
run_test
