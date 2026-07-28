#!/bin/sh
set -eu

default_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
root=${TCYPHER_CLEAN_BREAK_ROOT:-$default_root}

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

for command_name in cargo python3 rg; do
  command -v "$command_name" >/dev/null 2>&1 \
    || fail "required clean-break checker command is unavailable: $command_name"
done

cd "$root" || fail "clean-break root is unavailable: $root"
root=$(pwd -P)

[ -f Cargo.toml ] || fail "clean-break root has no Cargo.toml: $root"
[ -f Cargo.lock ] || fail "failed to load the locked Cargo dependency graph: Cargo.lock is missing"
[ -f README.md ] || fail "clean-break root has no README.md: $root"
[ -d docs ] || fail "clean-break root has no docs directory: $root"
[ -d crates ] || fail "clean-break root has no crates directory: $root"

scratch=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-clean-break-check.XXXXXX")
trap 'rm -rf "$scratch"' EXIT HUP INT TERM
metadata="$scratch/metadata.json"
metadata_error="$scratch/metadata.err"
matches="$scratch/matches"

if ! cargo metadata --locked --format-version 1 --no-deps >"$metadata" 2>"$metadata_error"; then
  printf '%s\n' 'failed to load the locked Cargo dependency graph:' >&2
  cat "$metadata_error" >&2
  exit 1
fi

python3 - "$root" "$metadata" <<'PY'
import json
import sys
from pathlib import Path


def abort(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


root = Path(sys.argv[1]).resolve()
metadata_path = Path(sys.argv[2])
try:
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
except (OSError, UnicodeError, json.JSONDecodeError) as error:
    abort(f"failed to parse Cargo dependency metadata: {error}")

try:
    metadata_root = Path(metadata["workspace_root"]).resolve()
    workspace_ids = set(metadata["workspace_members"])
    packages = [
        package for package in metadata["packages"] if package["id"] in workspace_ids
    ]
except (KeyError, TypeError, ValueError) as error:
    abort(f"Cargo dependency metadata is incomplete: {error}")

if metadata_root != root:
    abort(
        "Cargo metadata workspace root does not match the clean-break root: "
        f"{metadata_root} != {root}"
    )
if not packages:
    abort("Cargo dependency metadata contains no workspace packages")

by_manifest = {}
for package in packages:
    try:
        manifest = Path(package["manifest_path"]).resolve()
        name = package["name"]
    except (KeyError, TypeError, ValueError) as error:
        abort(f"Cargo package metadata is incomplete: {error}")
    if manifest in by_manifest:
        abort(f"duplicate workspace manifest in Cargo metadata: {manifest}")
    by_manifest[manifest] = package


def package_for_dependency(owner: dict, dependency: dict):
    path = dependency.get("path")
    if path is None:
        return None
    manifest = (Path(path).resolve() / "Cargo.toml").resolve()
    try:
        manifest.relative_to(root)
    except ValueError:
        return None
    package = by_manifest.get(manifest)
    if package is None:
        abort(
            f"local production dependency of {owner['name']} is absent from workspace metadata: "
            f"{manifest}"
        )
    return package


graph = {package["id"]: [] for package in packages}
for package in packages:
    dependencies = package.get("dependencies")
    if not isinstance(dependencies, list):
        abort(f"Cargo dependency list is missing for workspace package: {package['name']}")
    for dependency in dependencies:
        if dependency.get("kind") == "dev":
            continue
        dependency_package = package_for_dependency(package, dependency)
        if dependency_package is None:
            target = ("external", dependency.get("name", "<unnamed>"))
        else:
            target = ("workspace", dependency_package["id"])
        graph[package["id"]].append(target)

required_query_packages = {
    "cypher-syntax",
    "cypher-ast",
    "cypher-sema",
    "cypher-compiler",
    "temporal-ir",
    "physical-plan",
    "query-optimizer",
    "query-executor",
    "distributed-query",
    "cypher-engine",
}
packages_by_name = {}
for package in packages:
    packages_by_name.setdefault(package["name"], []).append(package)

for required in sorted(required_query_packages | {"temporal-semantics", "temporal-types"}):
    found = packages_by_name.get(required, [])
    if len(found) != 1:
        abort(
            f"expected exactly one {required} workspace package, found {len(found)}"
        )

adapter_manifests = {
    manifest.resolve() for manifest in root.glob("crates/adapter-*/Cargo.toml")
}
if not adapter_manifests:
    abort("expected at least one crates/adapter-* package, found 0")
for manifest in sorted(adapter_manifests):
    package = by_manifest.get(manifest)
    if package is None:
        abort(f"Adapter manifest is absent from workspace metadata: {manifest}")
    if not package["name"].startswith("adapter-"):
        abort(f"Adapter package name must start with adapter-: {package['name']}")

metadata_adapters = {
    Path(package["manifest_path"]).resolve()
    for package in packages
    if package["name"].startswith("adapter-")
}
if metadata_adapters != adapter_manifests:
    missing = sorted(str(path) for path in metadata_adapters ^ adapter_manifests)
    abort("Adapter package discovery is inconsistent: " + ", ".join(missing))

forbidden_adapter_dependencies = required_query_packages | {
    "analytics-runtime",
    "procedure-runtime",
}


def workspace_name(package_id: str) -> str:
    for package in packages:
        if package["id"] == package_id:
            return package["name"]
    abort(f"unknown workspace package id in dependency graph: {package_id}")


def forbidden_path(start: dict):
    pending = [(start["id"], [start["name"]])]
    visited = set()
    while pending:
        package_id, path = pending.pop(0)
        if package_id in visited:
            continue
        visited.add(package_id)
        for kind, target in graph[package_id]:
            if kind != "workspace":
                continue
            name = workspace_name(target)
            next_path = path + [name]
            if name in forbidden_adapter_dependencies:
                return next_path
            pending.append((target, next_path))
    return None


for manifest in sorted(adapter_manifests):
    adapter = by_manifest[manifest]
    path = forbidden_path(adapter)
    if path is not None:
        abort(
            "Adapter production dependency reaches a query-language or execution-internal "
            "crate: " + " -> ".join(path)
        )

semantics = packages_by_name["temporal-semantics"][0]
allowed_semantics_dependency = "temporal-types"
pending = [(semantics["id"], [semantics["name"]])]
visited = set()
seen_temporal_types = False
while pending:
    package_id, path = pending.pop(0)
    if package_id in visited:
        continue
    visited.add(package_id)
    for kind, target in graph[package_id]:
        if kind == "workspace":
            name = workspace_name(target)
            next_id = target
        else:
            name = target
            next_id = None
        next_path = path + [name]
        if name != allowed_semantics_dependency:
            abort(
                "temporal-semantics production dependency closure may contain only "
                f"temporal-types: {' -> '.join(next_path)}"
            )
        seen_temporal_types = True
        if next_id is not None:
            pending.append((next_id, next_path))

if not seen_temporal_types:
    abort("temporal-semantics must depend on temporal-types")
PY

reject_matches() {
  message=$1
  shift
  rg_status=0
  rg "$@" >"$matches" || rg_status=$?
  case $rg_status in
    0) ;;
    1) : >"$matches" ;;
    *) fail "clean-break source scan failed while checking: $message" ;;
  esac
  if [ -s "$matches" ]; then
    printf '%s\n' "$message" >&2
    cat "$matches" >&2
    exit 1
  fi
}

query_boundary_paths='crates/cypher-syntax/src
crates/cypher-ast/src
crates/cypher-sema/src
crates/cypher-compiler/src
crates/temporal-ir/src
crates/physical-plan/src
crates/query-optimizer/src
crates/query-executor/src
crates/distributed-query/src
crates/cypher-engine/src'

query_boundary_manifests='crates/cypher-syntax/Cargo.toml
crates/cypher-ast/Cargo.toml
crates/cypher-sema/Cargo.toml
crates/cypher-compiler/Cargo.toml
crates/temporal-ir/Cargo.toml
crates/physical-plan/Cargo.toml
crates/query-optimizer/Cargo.toml
crates/query-executor/Cargo.toml
crates/distributed-query/Cargo.toml
crates/cypher-engine/Cargo.toml'

language_and_spi_paths='crates/cypher-syntax/src
crates/cypher-ast/src
crates/cypher-sema/src
crates/cypher-compiler/src
crates/temporal-ir/src
crates/temporal-semantics/src
crates/storage-api/src
crates/cypher-syntax/Cargo.toml
crates/cypher-ast/Cargo.toml
crates/cypher-sema/Cargo.toml
crates/cypher-compiler/Cargo.toml
crates/temporal-ir/Cargo.toml
crates/temporal-semantics/Cargo.toml
crates/storage-api/Cargo.toml'

for required_path in $query_boundary_paths $query_boundary_manifests $language_and_spi_paths; do
  [ -e "$required_path" ] || fail "required clean-break boundary path is missing: $required_path"
done

reject_matches \
  'legacy T-Cypher syntax remains outside explicit rejection coverage:' \
  -n -i 'DIFF[[:space:]]+GRAPH|AT[[:space:]]+VALID_TIME|AT[[:space:]]+TRANSACTION_TIME' \
  crates --glob '*.rs' \
  --glob '!cypher-syntax/tests/parser_temporal.rs' \
  --glob '!crates/cypher-syntax/tests/parser_temporal.rs'

reject_matches \
  'legacy T-Cypher syntax remains in user documentation:' \
  -n -i 'DIFF[[:space:]]+GRAPH|AT[[:space:]]+VALID_TIME|AT[[:space:]]+TRANSACTION_TIME' \
  README.md docs --glob '*.md' \
  --glob '!superpowers/**' --glob '!docs/superpowers/**'

reject_matches \
  'removed T-Cypher AST/IR interface remains in production source:' \
  -n 'LogicalOperator::Diff|Statement::Diff|DiffStatement|TransactionTimeScope|ValidTimeScope' \
  crates/*/src --glob '*.rs'

reject_matches \
  'physical batch representation leaked into the language, semantics, or storage SPI:' \
  -n 'ColumnBatch|ColumnVector|query[_-]executor' \
  $language_and_spi_paths --glob '*.rs' --glob 'Cargo.toml'

reject_matches \
  'query-language or physical-runtime types leaked into an Adapter boundary:' \
  -n -i 'T-Cypher|TemporalScope|ResolvedTemporalScope|PhysicalPlan|PlanFragment|ColumnBatch|ColumnVector' \
  crates/adapter-*/src crates/adapter-*/Cargo.toml --glob '*.rs' --glob 'Cargo.toml'

legacy_interface_pattern='(?i)\b(legacy|compat(?:ibility)?|fallback)[_-]?(parser|compiler|executor|execution|planner|plan|ast|ir|query|cypher|temporal)[[:alnum:]_]*\b|\b(parser|compiler|executor|execution|planner|plan|ast|ir|query|cypher|temporal)[[:alnum:]_]*[_-]?(legacy|compat(?:ibility)?|fallback)\b'
reject_matches \
  'legacy/compat/fallback query interface remains in production source:' \
  -n "$legacy_interface_pattern" \
  $query_boundary_paths $query_boundary_manifests --glob '*.rs' --glob 'Cargo.toml'

versioned_interface_pattern='(?i)\b(parser|compiler|executor|execution|planner|plan|ast|ir|query|cypher|temporal)[[:alnum:]_]*[_-]?v[12]\b|\bv[12][_-]?(parser|compiler|executor|execution|planner|plan|ast|ir|query|cypher|temporal)[[:alnum:]_]*\b'
reject_matches \
  'V1/V2 query interface remains in production source:' \
  -n "$versioned_interface_pattern" \
  $query_boundary_paths $query_boundary_manifests --glob '*.rs' --glob 'Cargo.toml'

residue_files=$(find $query_boundary_paths -type f \( \
  -iname '*legacy*' -o -iname '*compat*' -o -iname '*fallback*' \
  -o -iname '*v1*' -o -iname '*v2*' \
\) -print)
if [ -n "$residue_files" ]; then
  printf '%s\n' 'legacy/compat/fallback/V1/V2 query boundary file remains:' >&2
  printf '%s\n' "$residue_files" >&2
  exit 1
fi

printf '%s\n' 'T-Cypher clean-break boundary checks passed'
