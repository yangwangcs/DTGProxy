#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

metadata="$(mktemp)"
trap 'rm -f "$metadata"' EXIT
cargo metadata --no-deps --format-version 1 >"$metadata"

python3 - "$metadata" <<'PY'
import json
import sys
from pathlib import Path


def fail(owner, dependency, reason):
    print(f"forbidden layered architecture dependency: {owner} -> {dependency} ({reason})", file=sys.stderr)
    raise SystemExit(1)


try:
    metadata = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    packages = metadata["packages"]
    workspace_members = set(metadata["workspace_members"])
except (OSError, json.JSONDecodeError, KeyError, TypeError) as error:
    print(f"failed to read Cargo metadata: {error}", file=sys.stderr)
    raise SystemExit(1)

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
processes = {"dtg-gateway", "dtg-data", "dtg-meta", "dtg-controller"}
concrete_storage_providers = {
    "dtg-storage-fjall",
    "dtg-storage-postgres",
    "dtg-storage-neo4j",
    "dtg-storage-remote",
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
workspace_package_names = {
    package["name"] for package in packages if package.get("id") in workspace_members
}


def is_language(name):
    return name.startswith("dtg-language")


def is_storage(name):
    return name.startswith("dtg-storage")


def is_execution(name):
    return name.startswith("dtg-") and name not in processes and not is_language(name) and not is_storage(name) and name != "dtg-kernel"


for package in packages:
    owner = package.get("name")
    if owner not in new_packages:
        continue
    dependencies = package.get("dependencies")
    if not isinstance(dependencies, list):
        print(f"missing dependency metadata for {owner}", file=sys.stderr)
        raise SystemExit(1)

    for dependency in dependencies:
        if dependency.get("kind") == "dev":
            continue
        target = dependency.get("name")
        if not isinstance(target, str):
            print(f"missing dependency name for {owner}", file=sys.stderr)
            raise SystemExit(1)

        if (
            target.startswith("adapter-")
            or target in legacy_runtime_packages
            or (target in workspace_package_names and target not in new_packages)
        ):
            fail(owner, target, "new packages must not depend on legacy runtime packages")
        if owner == "dtg-kernel" and target.startswith("dtg-"):
            fail(owner, target, "kernel may not depend on a dtg package")
        if is_language(owner) and (is_execution(target) or is_storage(target) or target in processes):
            fail(owner, target, "language may depend only on kernel among architectural layers")
        if is_storage(owner) and (is_language(target) or is_execution(target) or target in processes):
            fail(owner, target, "storage may not depend on language, execution, or processes")
        if is_execution(owner) and (target in processes or target in concrete_storage_providers):
            fail(owner, target, "execution may not depend on processes or concrete storage providers")
PY
