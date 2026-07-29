#!/usr/bin/env bash
set -euo pipefail

workspace_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)

run_test() {
  (
    cd "$workspace_root"
    cargo test --locked -p dtg-gateway --test live_provider_migrations -- \
      --ignored --test-threads=1 --nocapture
  )
}

if [[ -n "${DTG_POSTGRES_URL:-}" || -n "${DTG_NEO4J_URL:-}" || -n "${DTG_NEO4J_PASSWORD:-}" ]]; then
  if [[ -z "${DTG_POSTGRES_URL:-}" || -z "${DTG_NEO4J_URL:-}" || -z "${DTG_NEO4J_PASSWORD:-}" ]]; then
    printf 'DTG_POSTGRES_URL, DTG_NEO4J_URL, and DTG_NEO4J_PASSWORD must be set together\n' >&2
    exit 78
  fi
  export DTG_NEO4J_USER="${DTG_NEO4J_USER:-neo4j}"
  run_test
  exit 0
fi

for tool in initdb pg_ctl createdb pg_isready docker curl cargo openssl; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf '%s is required for simultaneous live provider migration certification\n' "$tool" >&2
    exit 78
  fi
done
if ! docker info >/dev/null 2>&1; then
  printf 'the Docker daemon is required for simultaneous Neo4j migration certification\n' >&2
  exit 78
fi

postgres_root=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-provider-migrations.XXXXXX")
postgres_data="$postgres_root/postgres"
postgres_log="$postgres_root/postgres.log"
password_file="$postgres_root/password"
postgres_password=$(openssl rand -hex 24)
neo4j_password=$(openssl rand -hex 24)
neo4j_container="dtgproxy-provider-migrations-$$"
postgres_started=false
neo4j_started=false

cleanup() {
  if [[ $neo4j_started == true ]]; then
    docker rm -f "$neo4j_container" >/dev/null 2>&1 || true
  fi
  if [[ $postgres_started == true ]]; then
    pg_ctl -D "$postgres_data" -m fast stop >/dev/null 2>&1 || true
  fi
  find "$postgres_root" -depth -delete 2>/dev/null || true
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

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

docker run --detach --rm --name "$neo4j_container" \
  --publish 127.0.0.1::7474 \
  --env "NEO4J_AUTH=neo4j/$neo4j_password" \
  neo4j:5.26-community >/dev/null
neo4j_started=true
neo4j_port_mapping=$(docker port "$neo4j_container" 7474/tcp)
neo4j_port=${neo4j_port_mapping##*:}
if ! [[ "$neo4j_port" =~ ^[0-9]+$ ]]; then
  printf 'Docker did not report a numeric Neo4j HTTP host port\n' >&2
  exit 1
fi
readiness_attempts=${DTGPROXY_NEO4J_READINESS_ATTEMPTS:-45}
if ! [[ "$readiness_attempts" =~ ^[1-9][0-9]*$ ]]; then
  printf 'DTGPROXY_NEO4J_READINESS_ATTEMPTS must be a positive integer\n' >&2
  exit 64
fi
for ((attempt = 1; attempt <= readiness_attempts; attempt++)); do
  if curl --fail --silent --show-error --connect-timeout 1 --max-time 1 \
    "http://127.0.0.1:$neo4j_port" >/dev/null; then
    break
  fi
  if ((attempt == readiness_attempts)); then
    printf 'Neo4j did not become ready after %s bounded attempts\n' "$readiness_attempts" >&2
    exit 1
  fi
  sleep 1
done

export DTG_POSTGRES_URL="host=127.0.0.1 port=$postgres_port user=dtgproxy password=$postgres_password dbname=dtgproxy sslmode=disable"
export DTG_NEO4J_URL="http://127.0.0.1:$neo4j_port"
export DTG_NEO4J_USER=neo4j
export DTG_NEO4J_PASSWORD="$neo4j_password"
run_test
