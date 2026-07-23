#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

run_tests() {
  local cxx_bin=${CXX:-}
  local libclang_dir=${LIBCLANG_PATH:-}
  if [[ -z "$cxx_bin" && -x /opt/homebrew/opt/llvm/bin/clang++ ]]; then
    cxx_bin=/opt/homebrew/opt/llvm/bin/clang++
  fi
  if [[ -z "$libclang_dir" && -d /opt/homebrew/opt/llvm/lib ]]; then
    libclang_dir=/opt/homebrew/opt/llvm/lib
  fi
  (
    cd "$workspace_root"
    CXX="$cxx_bin" LIBCLANG_PATH="$libclang_dir" \
      cargo test -p adapter-postgres -- --test-threads=1
  )
}

if [[ -n "${DTGPROXY_POSTGRES_URL:-}" ]]; then
  run_tests
  exit 0
fi

for tool in initdb pg_ctl createdb pg_isready; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf 'DTGPROXY_POSTGRES_URL is unset and required PostgreSQL tool %s is unavailable\n' "$tool" >&2
    exit 78
  fi
done

postgres_root=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-postgres.XXXXXX")
postgres_data="$postgres_root/data"
postgres_log="$postgres_root/postgres.log"
postgres_password=$(openssl rand -hex 24)
password_file="$postgres_root/password"
printf '%s\n' "$postgres_password" >"$password_file"

cleanup() {
  pg_ctl -D "$postgres_data" -m fast stop >/dev/null 2>&1 || true
  rm -rf "$postgres_root"
}
trap cleanup EXIT INT TERM

postgres_port=55439
while pg_isready -h 127.0.0.1 -p "$postgres_port" >/dev/null 2>&1; do
  postgres_port=$((postgres_port + 1))
  if ((postgres_port > 55539)); then
    printf 'no free local PostgreSQL test port in 55439..55539\n' >&2
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
PGPASSWORD="$postgres_password" createdb \
  -h 127.0.0.1 \
  -p "$postgres_port" \
  -U dtgproxy \
  dtgproxy

export DTGPROXY_POSTGRES_URL="host=127.0.0.1 port=$postgres_port user=dtgproxy password=$postgres_password dbname=dtgproxy sslmode=disable"
run_tests
