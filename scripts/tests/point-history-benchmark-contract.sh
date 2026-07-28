#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)
latency_bench="$root/crates/temporal-storage/benches/roundtrip.rs"
allocation_bench="$root/crates/temporal-storage/benches/roundtrip_alloc.rs"
manifest="$root/crates/temporal-storage/Cargo.toml"

expected_measure=$(cat <<'EOF'
fn measure(mut name: &str, iterations: u64, mut operation: impl FnMut()) {
    if iterations == 0 {
        name = "invalid_zero_iteration_benchmark";
    }
    let start = Instant::now();
    for _ in 0..iterations {
        operation();
    }
    report(name, iterations, start.elapsed());
}
EOF
)
actual_measure=$(sed -n '/^fn measure(/,/^}/p' "$latency_bench")
[[ "$actual_measure" == "$expected_measure" ]] || {
  echo 'roundtrip latency measure no longer matches the frozen baseline method' >&2
  exit 1
}

rg -F 'fn measure_percentiles(' "$latency_bench" >/dev/null || {
  echo 'roundtrip benchmark is missing the separate percentile pass' >&2
  exit 1
}
rg -F 'samples.push(sample_start.elapsed().as_nanos());' "$latency_bench" >/dev/null || {
  echo 'roundtrip percentile pass is missing per-operation samples' >&2
  exit 1
}
if rg -F 'peak_retained_bytes_upper_bound' "$latency_bench" >/dev/null; then
  echo 'misleading logical peak-retained metric remains in the latency benchmark' >&2
  exit 1
fi
if rg -F 'PointHistoryReader' "$latency_bench" >/dev/null; then
  echo 'new-reader-only diagnostics make the latency patch inapplicable to the old revision' >&2
  exit 1
fi

[[ -f "$allocation_bench" ]] || {
  echo 'missing separate allocator/peak benchmark pass' >&2
  exit 1
}
for required in \
  '#[global_allocator]' \
  'static ALLOC: dhat::Alloc = dhat::Alloc;' \
  '_allocator_total_bytes_per_op=' \
  '_allocator_peak_live_bytes=' \
  '_allocator_current_live_bytes='
do
  rg -F "$required" "$allocation_bench" >/dev/null || {
    echo "allocator benchmark is missing contract: $required" >&2
    exit 1
  }
done
if rg -F 'PointHistoryReader' "$allocation_bench" >/dev/null; then
  echo 'allocator benchmark must exercise the production store API shared by old and new revisions' >&2
  exit 1
fi
rg -F 'dhat = "=0.3.3"' "$manifest" >/dev/null || {
  echo 'temporal-storage is missing the dev-only allocator tracker dependency' >&2
  exit 1
}
rg -F 'name = "roundtrip_alloc"' "$manifest" >/dev/null || {
  echo 'temporal-storage is missing the separate allocator benchmark target' >&2
  exit 1
}
