#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

test -x scripts/certify-clean-break.sh
scripts/certify-clean-break.sh --contract-only

rg -F "run_gate legacy_removal 'bash scripts/check-clean-break-removal.sh'" \
  scripts/certify-clean-break.sh >/dev/null
rg -F "run_gate official_provider_contracts 'cargo test --locked -p dtg-storage-fjall -p dtg-storage-postgres -p dtg-storage-neo4j'" \
  scripts/certify-clean-break.sh >/dev/null
