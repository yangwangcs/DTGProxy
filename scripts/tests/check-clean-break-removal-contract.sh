#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

test -x scripts/check-clean-break-removal.sh
scripts/check-clean-break-removal.sh
