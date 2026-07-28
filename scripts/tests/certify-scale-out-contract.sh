#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
certifier="$root/scripts/certify-scale-out.sh"
scratch=$(mktemp -d "${TMPDIR:-/tmp}/dtgproxy-scale-contract.XXXXXX")
trap 'rm -rf "$scratch"' EXIT

fail() {
  printf 'FAIL %s\n' "$1" >&2
  exit 1
}

write_suite() {
  local path=$1 one=$2 four=$3 eight=$4
  jq -n \
    --argjson one "$one" --argjson four "$four" --argjson eight "$eight" '
    def topology($nodes; $throughput): {
      node_count: $nodes,
      processes:
        ([range(1; $nodes + 1) as $node | {
          role: "data-node", node_id: $node, pid: (40000 + $node),
          listen_address: ("127.0.0.1:" + (7000 + $node | tostring)),
          data_directory: ("/tmp/dtgproxy-scale/node-" + ($node | tostring)),
          rss_bytes: 1024
        }] + [{
          role: "gateway", pid: 50000,
          listen_address: "127.0.0.1:7687", rss_bytes: 1024
        }]),
      network_rx_bytes: 2048,
      network_tx_bytes: 4096,
      workload: {
        dataset_seed: "dtgproxy-scale-out-v1",
        query: "USE scale_graph FOR VALID_TIME AS OF 1000 MATCH (n) RETURN count(n) AS count",
        duration_seconds: 60,
        concurrency: 32,
        result_digest: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        row_count: 1,
        ttfr_ms: 1,
        total_latency_ms: 2,
        throughput_ops_per_second: $throughput
      }
    };
    {
      schema_version: 1,
      status: "passed",
      topologies: [topology(1; $one), topology(4; $four), topology(8; $eight)]
    }' >"$path"
}

write_suite "$scratch/diagnostic.json" 100 100 100
bash "$certifier" --validate-suite "$scratch/diagnostic.json" >/dev/null ||
  fail "diagnostic suite must not claim formal thresholds"

write_suite "$scratch/four-low.json" 100 279 500
if bash "$certifier" --validate-threshold-suite "$scratch/four-low.json" >"$scratch/four.out"; then
  fail "formal suite accepted 4-node throughput below 2.8x"
fi
jq -e '.error.code == "four_node_scale_out_below_2_8x"' "$scratch/four.out" >/dev/null ||
  fail "formal 4-node failure code mismatch"

write_suite "$scratch/eight-low.json" 100 280 499
if bash "$certifier" --validate-threshold-suite "$scratch/eight-low.json" >"$scratch/eight.out"; then
  fail "formal suite accepted 8-node throughput below 5.0x"
fi
jq -e '.error.code == "eight_node_scale_out_below_5_0x"' "$scratch/eight.out" >/dev/null ||
  fail "formal 8-node failure code mismatch"

write_suite "$scratch/boundary.json" 100 280 500
bash "$certifier" --validate-threshold-suite "$scratch/boundary.json" >"$scratch/boundary.out" ||
  fail "formal suite rejected exact scale-out boundaries"
jq -e '.status == "threshold_validated" and .speedup.four_nodes == 2.8 and .speedup.eight_nodes == 5' \
  "$scratch/boundary.out" >/dev/null || fail "formal scale-out evidence mismatch"

printf 'PASS scale-out diagnostic and formal threshold contracts\n'
