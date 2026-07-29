#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

test -x scripts/certify-clean-break.sh
scripts/certify-clean-break.sh --contract-only
