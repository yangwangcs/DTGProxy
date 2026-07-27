# Point History Clean-Break Performance Report

Date: 2026-07-28

## Revision and environment

- Revision: `479d799a115f12728d4f40c95f362bbd0b972427` (`docs: plan point history clean break`).
- Worktree: dirty before this baseline; `git status --porcelain=v1` reported 193 entries after the
  benchmark-fixture edit. The measurements are therefore evidence for this recorded revision plus
  the pre-existing worktree state, not for the commit alone.
- Host: Apple M2, arm64; macOS 27.0 (build 26A5388g).
- Toolchain: `rustc 1.93.0 (254b59607 2026-01-19)`, LLVM 21.1.8; `cargo 1.93.0`.
- Storage backend: a new temporary embedded RocksDB directory for each benchmark process.

## Dataset and fixed seed

The fixture has no pseudorandom input or seed. It deterministically commits one vertex 1,008 times,
with log indices 1 through 1,008, commit timestamps `100` through `100800` in steps of `100`,
valid interval `[0, 100)`, and payloads derived from the log index. The fixed point-in-time cells
query valid time `50` at transaction snapshots `99200`, `99300`, `100000`, and `100700` for replay
depths 0, 1, 8, and 15 respectively. The fixture then performs its existing deterministic
retroactive-write, adjacency, and transaction measurements.

## Commands

```bash
for run in 1 2 3; do
  DTGPROXY_BENCH_ITERS=10000 cargo bench --locked -p temporal-storage --features rocksdb-tests --bench roundtrip
done

cargo test --locked -p temporal-storage --test history_chain -- --test-threads=1
```

Each benchmark process constructs its own `tempfile::tempdir()` RocksDB instance. The three runs
were executed serially in the command order above, with no reduced iteration count or warm-up run
substituted for a recorded sample.

## Pre-change raw runs

All latency values are the benchmark's integer `elapsed.as_nanos() / iterations` output, in ns/op.

| Metric | Run 1 | Run 2 | Run 3 |
|---|---:|---:|---:|
| `current_point_lookup_ns_per_op` | 1,162 | 1,211 | 1,169 |
| `as_of_replay_depth_0_ns_per_op` | 10,584 | 10,955 | 10,483 |
| `as_of_replay_depth_1_ns_per_op` | 1,730 | 1,744 | 1,714 |
| `as_of_replay_depth_8_ns_per_op` | 7,245 | 7,394 | 7,198 |
| `as_of_replay_depth_15_ns_per_op` | 9,618 | 9,719 | 9,857 |
| `as_of_snapshot_age_1000_ns_per_op` | 6,231 | 6,238 | 6,226 |
| `history_value_bytes_total` | 100,827 | 100,827 | 100,827 |
| `history_value_bytes_average` | 100 | 100 | 100 |
| `full_anchor_value_bytes_hypothetical` | 148,077 | 148,077 | 148,077 |
| `retroactive_commit_sync_ns_per_op` | 125,093 | 133,885 | 122,161 |
| `expand_out_degree_32_ns_per_op` | 15,696 | 15,798 | 16,107 |
| `expand_out_as_of_partition_32_edges_ns_per_op` | 69,539 | 69,493 | 71,158 |
| `transaction_two_vertices_one_edge_sync_ns_per_op` | 108,652 | 107,737 | 108,682 |
| `rocksdb_bytes` | 36,429,298 | 36,429,304 | 36,429,300 |
| `iterations` | 10,000 | 10,000 | 10,000 |

## Pre-change median latency

The median is the middle of the three integer ns/op observations; it is not a mean or a
percentile estimate.

| Stable benchmark cell | Run 1 (ns/op) | Run 2 (ns/op) | Run 3 (ns/op) | Median (ns/op) |
|---|---:|---:|---:|---:|
| `as_of_replay_depth_0` | 10,584 | 10,955 | 10,483 | 10,584 |
| `as_of_replay_depth_1` | 1,730 | 1,744 | 1,714 | 1,730 |
| `as_of_replay_depth_8` | 7,245 | 7,394 | 7,198 | 7,245 |
| `as_of_replay_depth_15` | 9,618 | 9,719 | 9,857 | 9,719 |

## Allocation and retained-memory method

This fixture does not install an allocator sampler, RSS sampler, or retained-heap sampler, so it
does not claim allocation or retained-memory measurements. `rocksdb_bytes` is the recursive byte
size of the temporary RocksDB directory at the end of each benchmark process; it is retained
on-disk storage only and must not be interpreted as process memory. The three observed directory
sizes are recorded in the raw-runs table.

## Post-change raw runs

## Comparison

No post-change observations have been collected. Comparison is deferred until Task 8 reruns these
unchanged four stable benchmark cells under the same command and reports both raw observations and
the same median calculation.

## Remaining bottlenecks

The baseline demonstrates that the existing as-of path has distinct costs at the fixed replay
depths, but it does not isolate CPU, cache, allocation, or individual RocksDB operations. The
fixture also records a hypothetical all-anchor encoded-value total rather than a measured
post-change representation. Those distinctions must remain explicit when evaluating the
clean-break implementation.

## Conclusion

This document freezes the pre-change RocksDB point-history baseline, its deterministic fixture,
and the benchmark cell names that must be retained through the clean break. The focused
`history_chain` test passed 4/4 serially, including
`policy_writes_at_most_fifteen_deltas_before_a_new_anchor`.
