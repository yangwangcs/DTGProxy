#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
launcher="$root/scripts/test-neo4j-live.sh"
temporary=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-neo4j-launcher-contract.XXXXXX")
trap 'find "$temporary" -depth -delete 2>/dev/null || true' EXIT

[[ -f "$launcher" ]] || {
  echo 'missing Neo4j live-test launcher' >&2
  exit 1
}

bash -n "$launcher"
for required in \
  'for tool in docker curl cargo sleep' \
  'command -v "$tool"' \
  'dtgproxy-neo4j-point-history-' \
  '--publish 127.0.0.1::7474' \
  'docker rm -f' \
  '--connect-timeout 1' \
  '--max-time 1' \
  'DTG_NEO4J_URL=' \
  'cargo test --locked -p dtg-storage-neo4j --test live_tck -- --ignored --test-threads=1'
do
  rg -F -- "$required" "$launcher" >/dev/null || {
    echo "launcher is missing required contract: $required" >&2
    exit 1
  }
done

empty_bin="$temporary/empty-bin"
mkdir -p "$empty_bin"
set +e
PATH="$empty_bin" /bin/bash "$launcher" >"$temporary/docker-missing.out" 2>"$temporary/docker-missing.err"
status=$?
set -e
[[ "$status" == 78 ]]
rg -F 'docker is required' "$temporary/docker-missing.err" >/dev/null

prereq_bin="$temporary/prereq-bin"
mkdir -p "$prereq_bin"
printf '#!/bin/bash\nexit 0\n' >"$prereq_bin/docker"
chmod +x "$prereq_bin/docker"
set +e
PATH="$prereq_bin" /bin/bash "$launcher" >"$temporary/curl-missing.out" 2>"$temporary/curl-missing.err"
status=$?
set -e
[[ "$status" == 78 ]]
rg -F 'curl is required' "$temporary/curl-missing.err" >/dev/null

mock_bin="$temporary/mock-bin"
mock_log="$temporary/mock.log"
mkdir -p "$mock_bin"
printf '%s\n' '#!/bin/bash' \
  'case "$1" in' \
  '  run) printf "docker-run %s\\n" "$*" >>"$MOCK_LOG"; echo mock-container-id ;;' \
  '  port) printf "docker-port %s\\n" "$*" >>"$MOCK_LOG"; echo 127.0.0.1:49123 ;;' \
  '  rm) printf "docker-cleanup %s\\n" "$*" >>"$MOCK_LOG" ;;' \
  '  *) printf "docker-other %s\\n" "$*" >>"$MOCK_LOG" ;;' \
  'esac' >"$mock_bin/docker"
printf '%s\n' '#!/bin/bash' \
  'printf "curl %s\\n" "$*" >>"$MOCK_LOG"' \
  'if [[ "${MOCK_CURL_MODE:-success}" == slow ]]; then /bin/sleep 1; fi' \
  '[[ "${MOCK_CURL_MODE:-success}" == success ]]' >"$mock_bin/curl"
printf '%s\n' '#!/bin/bash' \
  'printf "cargo endpoint=%s username=%s password=%s args=%s\n" "${DTG_NEO4J_URL:-}" "${DTG_NEO4J_USER:-}" "${DTG_NEO4J_PASSWORD:-}" "$*" >>"$MOCK_LOG"' \
  'exit "${MOCK_CARGO_STATUS:-0}"' >"$mock_bin/cargo"
printf '#!/bin/bash\nexit 0\n' >"$mock_bin/sleep"
chmod +x "$mock_bin/docker" "$mock_bin/curl" "$mock_bin/cargo" "$mock_bin/sleep"

set +e
(cd "$root" && \
  PATH="$mock_bin" MOCK_LOG="$mock_log" MOCK_CARGO_STATUS=23 \
  /bin/bash "$launcher" >"$temporary/cargo-failure.out" 2>"$temporary/cargo-failure.err")
status=$?
set -e
[[ "$status" == 23 ]]
rg -F 'docker-run run --detach --rm --name dtgproxy-neo4j-point-history-' "$mock_log" >/dev/null
rg -F -- '--publish 127.0.0.1::7474' "$mock_log" >/dev/null
rg -F 'curl --fail --silent --show-error --connect-timeout 1 --max-time 1 http://127.0.0.1:49123' "$mock_log" >/dev/null
rg -F 'cargo endpoint=http://127.0.0.1:49123 username=neo4j password=dtgproxy-point-history-password args=test --locked -p dtg-storage-neo4j --test live_tck -- --ignored --test-threads=1' "$mock_log" >/dev/null
rg -F 'docker-cleanup rm -f dtgproxy-neo4j-point-history-' "$mock_log" >/dev/null

: >"$mock_log"
set +e
(cd "$root" && \
  PATH="$mock_bin" MOCK_LOG="$mock_log" MOCK_CURL_MODE=failure \
  DTGPROXY_NEO4J_READINESS_ATTEMPTS=2 \
  /bin/bash "$launcher" >"$temporary/timeout.out" 2>"$temporary/timeout.err")
status=$?
set -e
[[ "$status" == 1 ]]
rg -F 'Neo4j did not become ready after 2 bounded attempts' "$temporary/timeout.err" >/dev/null
[[ $(rg -c '^curl ' "$mock_log") == 2 ]]
if rg '^cargo ' "$mock_log" >/dev/null; then
  echo 'cargo must not run after readiness timeout' >&2
  exit 1
fi
rg -F 'docker-cleanup rm -f dtgproxy-neo4j-point-history-' "$mock_log" >/dev/null
