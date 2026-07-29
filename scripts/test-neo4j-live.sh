#!/usr/bin/env bash
set -euo pipefail

for tool in docker curl cargo sleep; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    printf '%s is required for the disposable Neo4j live test\n' "$tool" >&2
    exit 78
  fi
done

readiness_attempts=${DTGPROXY_NEO4J_READINESS_ATTEMPTS:-30}
if ! [[ "$readiness_attempts" =~ ^[1-9][0-9]*$ ]]; then
  echo 'DTGPROXY_NEO4J_READINESS_ATTEMPTS must be a positive integer' >&2
  exit 64
fi

container="dtgproxy-neo4j-point-history-$$"
cleanup() { docker rm -f "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker run --detach --rm --name "$container" \
  --publish 127.0.0.1::7474 \
  --env NEO4J_AUTH=neo4j/dtgproxy-point-history-password \
  neo4j:5.26-community >/dev/null

port_mapping=$(docker port "$container" 7474/tcp)
port=${port_mapping##*:}
if ! [[ "$port" =~ ^[0-9]+$ ]]; then
  echo 'Docker did not report a numeric Neo4j HTTP host port' >&2
  exit 1
fi

for ((attempt = 1; attempt <= readiness_attempts; attempt++)); do
  if curl --fail --silent --show-error --connect-timeout 1 --max-time 1 \
    "http://127.0.0.1:$port" >/dev/null; then
    break
  fi
  if ((attempt == readiness_attempts)); then
    printf 'Neo4j did not become ready after %s bounded attempts\n' "$readiness_attempts" >&2
    exit 1
  fi
  sleep 1
done

DTG_NEO4J_URL="http://127.0.0.1:$port" \
DTG_NEO4J_USER=neo4j \
DTG_NEO4J_PASSWORD=dtgproxy-point-history-password \
cargo test --locked -p dtg-storage-neo4j --test live_tck -- --ignored --test-threads=1
