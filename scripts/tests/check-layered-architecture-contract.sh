#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

test -x scripts/check-layered-architecture.sh
scripts/check-layered-architecture.sh

for package in dtg-kernel dtg-language-ir dtg-language dtg-storage dtg-storage-fjall dtg-storage-postgres dtg-storage-kuzu dtg-storage-remote-protocol dtg-storage-remote dtg-plan dtg-query dtg-transaction dtg-shard dtg-analytics dtg-control dtg-cluster-protocol dtg-snapshot-csr dtg-execution dtg-gateway dtg-data dtg-meta dtg-controller
do
  cargo metadata --no-deps --format-version 1 |
    jq -e --arg package "$package" '.packages[] | select(.name == $package)' >/dev/null
done

checker="$repo_root/scripts/check-layered-architecture.sh"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/dtg-layered-architecture.XXXXXX")"
trap 'rm -rf "$fixture"' EXIT HUP INT TERM

write_crate() {
  name=$1
  dependencies=${2-}
  directory="$fixture/crates/$name"
  mkdir -p "$directory/src"
  cat > "$directory/Cargo.toml" <<EOF
[package]
name = "$name"
version = "1.0.0"
edition = "2024"

[dependencies]
$dependencies
EOF
  printf '%s\n' '#![forbid(unsafe_code)]' > "$directory/src/lib.rs"
}

reset_fixture() {
  rm -rf "$fixture"
  mkdir -p "$fixture/crates"

  members=''
  for name in \
    dtg-kernel dtg-language-ir dtg-language dtg-storage \
    dtg-storage-fjall dtg-storage-postgres dtg-storage-kuzu \
    dtg-storage-remote-protocol dtg-storage-remote dtg-plan dtg-query \
    dtg-transaction dtg-shard dtg-analytics dtg-control dtg-cluster-protocol \
    dtg-snapshot-csr \
    dtg-execution dtg-gateway dtg-data dtg-meta dtg-controller storage-api tokio
  do
    if [ -n "$members" ]; then
      members="$members, "
    fi
    members="$members\"crates/$name\""
  done

  cat > "$fixture/Cargo.toml" <<EOF
[workspace]
members = [$members]
resolver = "3"
EOF

  write_crate dtg-kernel
  write_crate dtg-language-ir 'dtg-kernel = { path = "../dtg-kernel" }'
  write_crate dtg-language 'dtg-kernel = { path = "../dtg-kernel" }
dtg-language-ir = { path = "../dtg-language-ir" }'
  write_crate dtg-storage 'dtg-kernel = { path = "../dtg-kernel" }'
  write_crate dtg-storage-fjall 'dtg-kernel = { path = "../dtg-kernel" }
dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-storage-postgres 'dtg-kernel = { path = "../dtg-kernel" }
dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-storage-kuzu 'dtg-kernel = { path = "../dtg-kernel" }
dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-storage-remote-protocol 'dtg-kernel = { path = "../dtg-kernel" }'
  write_crate dtg-storage-remote 'dtg-kernel = { path = "../dtg-kernel" }
dtg-storage = { path = "../dtg-storage" }
dtg-storage-remote-protocol = { path = "../dtg-storage-remote-protocol" }'
  write_crate dtg-plan 'dtg-kernel = { path = "../dtg-kernel" }
dtg-language-ir = { path = "../dtg-language-ir" }
dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-query 'dtg-kernel = { path = "../dtg-kernel" }
dtg-language-ir = { path = "../dtg-language-ir" }
dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-transaction 'dtg-kernel = { path = "../dtg-kernel" }
dtg-language-ir = { path = "../dtg-language-ir" }
dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-shard 'dtg-kernel = { path = "../dtg-kernel" }
dtg-language-ir = { path = "../dtg-language-ir" }
dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-analytics 'dtg-kernel = { path = "../dtg-kernel" }
dtg-language-ir = { path = "../dtg-language-ir" }
dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-control 'dtg-kernel = { path = "../dtg-kernel" }
dtg-language-ir = { path = "../dtg-language-ir" }
dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-cluster-protocol 'dtg-kernel = { path = "../dtg-kernel" }'
  write_crate dtg-snapshot-csr 'dtg-storage = { path = "../dtg-storage" }'
  write_crate dtg-execution 'dtg-kernel = { path = "../dtg-kernel" }
dtg-language = { path = "../dtg-language" }
dtg-language-ir = { path = "../dtg-language-ir" }
dtg-storage = { path = "../dtg-storage" }
dtg-plan = { path = "../dtg-plan" }
dtg-query = { path = "../dtg-query" }
dtg-transaction = { path = "../dtg-transaction" }
dtg-shard = { path = "../dtg-shard" }
dtg-analytics = { path = "../dtg-analytics" }
dtg-control = { path = "../dtg-control" }
dtg-cluster-protocol = { path = "../dtg-cluster-protocol" }
dtg-snapshot-csr = { path = "../dtg-snapshot-csr" }'
  write_crate dtg-gateway 'dtg-execution = { path = "../dtg-execution" }'
  write_crate dtg-data 'dtg-execution = { path = "../dtg-execution" }
dtg-storage-fjall = { path = "../dtg-storage-fjall" }
dtg-storage-postgres = { path = "../dtg-storage-postgres" }
dtg-storage-kuzu = { path = "../dtg-storage-kuzu" }
dtg-storage-remote = { path = "../dtg-storage-remote" }'
  write_crate dtg-meta 'dtg-execution = { path = "../dtg-execution" }'
  write_crate dtg-controller 'dtg-execution = { path = "../dtg-execution" }'
  write_crate storage-api
  write_crate tokio
}

run_gate() {
  LAYERED_ARCHITECTURE_ROOT="$fixture" "$checker" > "$fixture/output" 2>&1
}

expect_pass() {
  label=$1
  if ! run_gate; then
    printf 'expected pass: %s\n' "$label" >&2
    cat "$fixture/output" >&2
    exit 1
  fi
}

expect_fail() {
  label=$1
  expected=$2
  if run_gate; then
    printf 'expected failure: %s\n' "$label" >&2
    exit 1
  fi
  if ! rg -q --fixed-strings "$expected" "$fixture/output"; then
    printf 'wrong failure for %s; expected %s\n' "$label" "$expected" >&2
    cat "$fixture/output" >&2
    exit 1
  fi
}

reset_fixture
expect_pass 'approved dependency directions'

reset_fixture
printf '%s\n' 'tokio = { path = "../tokio" }' >> "$fixture/crates/dtg-kernel/Cargo.toml"
expect_fail 'kernel runtime dependency' 'dtg-kernel -> tokio'

reset_fixture
printf '%s\n' 'dtg-language = { path = "../dtg-language" }' >> "$fixture/crates/dtg-language-ir/Cargo.toml"
expect_fail 'language IR depends on compiler' 'dtg-language-ir -> dtg-language'

reset_fixture
printf '%s\n' 'dtg-query = { path = "../dtg-query" }' >> "$fixture/crates/dtg-plan/Cargo.toml"
expect_fail 'execution implementation dependency' 'dtg-plan -> dtg-query'

reset_fixture
printf '%s\n' 'dtg-kernel = { path = "../dtg-kernel" }' >> "$fixture/crates/dtg-gateway/Cargo.toml"
expect_fail 'gateway reaches below execution facade' 'dtg-gateway -> dtg-kernel'

reset_fixture
printf '%s\n' 'dtg-data = { path = "../dtg-data" }' >> "$fixture/crates/dtg-gateway/Cargo.toml"
expect_fail 'process-to-process dependency' 'dtg-gateway -> dtg-data'

reset_fixture
printf '%s\n' 'storage-api = { path = "../storage-api" }' >> "$fixture/crates/dtg-storage/Cargo.toml"
expect_fail 'legacy runtime dependency' 'dtg-storage -> storage-api'

printf '%s\n' 'check-layered-architecture contract tests passed'
