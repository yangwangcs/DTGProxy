#!/bin/sh
set -eu

checker=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)/check-tcypher-clean-break.sh
fixture=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-clean-break.XXXXXX")
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
  mkdir -p "$fixture/crates" "$fixture/docs"
  : > "$fixture/README.md"

  members=''
  for name in \
    temporal-types temporal-semantics storage-api adapter-demo \
    cypher-syntax cypher-ast cypher-sema cypher-compiler temporal-ir \
    physical-plan query-optimizer query-executor distributed-query cypher-engine
  do
    if [ -n "$members" ]; then
      members="$members, "
    fi
    members="${members}\"crates/$name\""
  done

  cat > "$fixture/Cargo.toml" <<EOF
[workspace]
members = [$members]
resolver = "3"
EOF

  write_crate temporal-types
  write_crate temporal-semantics 'temporal-types = { path = "../temporal-types" }'
  write_crate storage-api
  write_crate adapter-demo 'storage-api = { path = "../storage-api" }'
  write_crate cypher-syntax
  write_crate cypher-ast
  write_crate cypher-sema
  write_crate cypher-compiler
  write_crate temporal-ir
  write_crate physical-plan
  write_crate query-optimizer
  write_crate query-executor
  write_crate distributed-query
  write_crate cypher-engine

  (cd "$fixture" && cargo generate-lockfile --quiet)
}

run_gate() {
  TCYPHER_CLEAN_BREAK_ROOT="$fixture" "$checker" > "$fixture/output" 2>&1
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
expect_pass 'clean backend-independent boundary'

reset_fixture
printf '%s\n' 'query-executor = { path = "../query-executor" }' >> "$fixture/crates/adapter-demo/Cargo.toml"
(cd "$fixture" && cargo generate-lockfile --quiet)
expect_fail 'adapter direct execution dependency' 'adapter-demo -> query-executor'

reset_fixture
printf '%s\n' 'query-executor = { path = "../query-executor" }' >> "$fixture/crates/storage-api/Cargo.toml"
(cd "$fixture" && cargo generate-lockfile --quiet)
expect_fail 'adapter transitive execution dependency' 'adapter-demo -> storage-api -> query-executor'

reset_fixture
printf '%s\n' 'storage-api = { path = "../storage-api" }' >> "$fixture/crates/temporal-semantics/Cargo.toml"
(cd "$fixture" && cargo generate-lockfile --quiet)
expect_fail 'temporal-semantics direct closure expansion' 'temporal-semantics -> storage-api'

reset_fixture
printf '%s\n' 'storage-api = { path = "../storage-api" }' >> "$fixture/crates/temporal-types/Cargo.toml"
(cd "$fixture" && cargo generate-lockfile --quiet)
expect_fail 'temporal-semantics transitive closure expansion' 'temporal-semantics -> temporal-types -> storage-api'

reset_fixture
printf '%s\n' 'pub struct LegacyCompiler;' >> "$fixture/crates/cypher-compiler/src/lib.rs"
expect_fail 'legacy compiler residue' 'legacy/compat/fallback query interface remains in production source'

reset_fixture
printf '%s\n' 'pub struct FallbackExecutor;' >> "$fixture/crates/query-executor/src/lib.rs"
expect_fail 'fallback executor residue' 'legacy/compat/fallback query interface remains in production source'

reset_fixture
printf '%s\n' 'pub struct QueryExecutorV2;' >> "$fixture/crates/query-executor/src/lib.rs"
expect_fail 'versioned query interface residue' 'V1/V2 query interface remains in production source'

reset_fixture
rm "$fixture/Cargo.lock"
expect_fail 'missing locked dependency graph' 'failed to load the locked Cargo dependency graph'

printf '%s\n' 'check-tcypher-clean-break self-tests passed'
