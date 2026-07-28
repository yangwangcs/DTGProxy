#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

test -x scripts/check-layered-architecture.sh
scripts/check-layered-architecture.sh

for package in dtg-kernel dtg-language-ir dtg-language dtg-storage dtg-storage-fjall dtg-storage-postgres dtg-storage-neo4j dtg-storage-remote-protocol dtg-storage-remote dtg-plan dtg-query dtg-transaction dtg-shard dtg-analytics dtg-control dtg-cluster-protocol dtg-execution dtg-gateway dtg-data dtg-meta dtg-controller
do
  cargo metadata --no-deps --format-version 1 |
    jq -e --arg package "$package" '.packages[] | select(.name == $package)' >/dev/null
done
