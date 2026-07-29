#!/usr/bin/env bash
set -euo pipefail

default_root="$(cd "$(dirname "$0")/.." && pwd)"
repo_root="${LAYERED_ARCHITECTURE_ROOT:-$default_root}"
cd "$repo_root"

metadata="$(mktemp)"
trap 'rm -f "$metadata"' EXIT HUP INT TERM
cargo metadata --no-deps --format-version 1 >"$metadata"

python3 - "$metadata" <<'PY'
import json
import sys
from pathlib import Path


def abort(message):
    print(message, file=sys.stderr)
    raise SystemExit(1)


def fail(owner, dependency, reason):
    abort(
        f"forbidden layered architecture dependency: {owner} -> {dependency} "
        f"({reason})"
    )


try:
    metadata = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    packages = metadata["packages"]
    workspace_members = set(metadata["workspace_members"])
except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
    abort(f"failed to read Cargo metadata: {error}")

new_packages = {
    "dtg-kernel",
    "dtg-language-ir",
    "dtg-language",
    "dtg-storage",
    "dtg-storage-fjall",
    "dtg-storage-postgres",
    "dtg-storage-neo4j",
    "dtg-storage-remote-protocol",
    "dtg-storage-remote",
    "dtg-plan",
    "dtg-query",
    "dtg-transaction",
    "dtg-shard",
    "dtg-analytics",
    "dtg-control",
    "dtg-cluster-protocol",
    "dtg-execution",
    "dtg-gateway",
    "dtg-data",
    "dtg-meta",
    "dtg-controller",
}
allowed_new_dependencies = {
    "dtg-kernel": set(),
    "dtg-language-ir": {"dtg-kernel"},
    "dtg-language": {"dtg-kernel", "dtg-language-ir"},
    "dtg-storage": {"dtg-kernel"},
    "dtg-storage-fjall": {"dtg-kernel", "dtg-storage"},
    "dtg-storage-postgres": {"dtg-kernel", "dtg-storage"},
    "dtg-storage-neo4j": {"dtg-kernel", "dtg-storage"},
    "dtg-storage-remote-protocol": {"dtg-kernel"},
    "dtg-storage-remote": {
        "dtg-kernel",
        "dtg-storage",
        "dtg-storage-remote-protocol",
    },
    "dtg-plan": {"dtg-kernel", "dtg-language-ir", "dtg-storage"},
    "dtg-query": {"dtg-kernel", "dtg-language-ir", "dtg-storage"},
    "dtg-transaction": {"dtg-kernel", "dtg-language-ir", "dtg-storage"},
    "dtg-shard": {"dtg-kernel", "dtg-language-ir", "dtg-storage"},
    "dtg-analytics": {"dtg-kernel", "dtg-language-ir", "dtg-storage"},
    "dtg-control": {"dtg-kernel", "dtg-language-ir", "dtg-storage"},
    "dtg-cluster-protocol": {"dtg-kernel"},
    "dtg-execution": {
        "dtg-kernel",
        "dtg-language",
        "dtg-language-ir",
        "dtg-storage",
        "dtg-plan",
        "dtg-query",
        "dtg-transaction",
        "dtg-shard",
        "dtg-analytics",
        "dtg-control",
        "dtg-cluster-protocol",
    },
    "dtg-gateway": {"dtg-execution"},
    "dtg-data": {
        "dtg-execution",
        "dtg-storage-fjall",
        "dtg-storage-postgres",
        "dtg-storage-neo4j",
        "dtg-storage-remote",
    },
    "dtg-meta": {"dtg-execution", "dtg-storage-fjall"},
    "dtg-controller": {"dtg-execution", "dtg-storage-fjall"},
}
legacy_runtime_packages = {
    "storage-api",
    "temporal-ir",
    "physical-plan",
    "query-executor",
    "query-optimizer",
    "distributed-query",
    "procedure-runtime",
    "cypher-engine",
    "temporal-storage",
    "raft-logstore",
    "cluster-protocol",
    "data-node",
    "gateway-node",
    "meta-node",
    "controller",
    "dtgproxy",
}

if set(allowed_new_dependencies) != new_packages:
    abort("layered architecture allowlist does not cover exactly the new packages")

workspace_packages = {
    package["id"]: package
    for package in packages
    if package.get("id") in workspace_members
}
workspace_package_names = {
    package["name"] for package in workspace_packages.values()
}
packages_by_manifest = {
    Path(package["manifest_path"]).resolve(): package
    for package in workspace_packages.values()
}


def dependency_name(dependency):
    path = dependency.get("path")
    if path is not None:
        package = packages_by_manifest.get((Path(path).resolve() / "Cargo.toml").resolve())
        if package is not None:
            return package["name"]
    name = dependency.get("name")
    if not isinstance(name, str):
        abort("Cargo metadata contains a dependency without a package name")
    return name


for package in workspace_packages.values():
    owner = package["name"]
    if owner not in new_packages:
        continue
    dependencies = package.get("dependencies")
    if not isinstance(dependencies, list):
        abort(f"Cargo dependency list is missing for {owner}")

    for dependency in dependencies:
        if dependency.get("kind") == "dev":
            continue
        target = dependency_name(dependency)

        if (
            target.startswith("adapter-")
            or target in legacy_runtime_packages
            or (target in workspace_package_names and target not in new_packages)
        ):
            fail(owner, target, "new packages must not depend on legacy runtime packages")
        if target in new_packages and target not in allowed_new_dependencies[owner]:
            fail(owner, target, "dependency is not in the explicit layer allowlist")
        if owner == "dtg-kernel" and target not in allowed_new_dependencies[owner]:
            fail(owner, target, "kernel permits no dependencies")
PY
