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

The original baseline did not install an allocator, RSS, or retained-heap sampler, so it has no
allocation/copy or retained-memory values. `rocksdb_bytes` is the recursive byte size of the
temporary RocksDB directory at the end of each benchmark process; it is retained on-disk storage
only and must not be interpreted as process memory.

The corrected Task-8 latency benchmark preserves the original `measure` function exactly: one
outer `Instant`, repeated calls to `operation()`, and one elapsed-time division after the loop.
Per-operation `Instant` reads, sample retention, and percentile sorting occur only in a separate
pass after the original point-read latency cells have completed. The same corrected benchmark
source can therefore be applied to the old and new revisions without changing the legacy
`*_ns_per_op` method, while the separate pass emits matching `*_p50_ns` and `*_p95_ns` fields.

Physical heap evidence is isolated in the separate `roundtrip_alloc` benchmark target. It uses the
dev-only external `dhat` allocator tracker through safe workspace code; no `unsafe` block or unsafe
allocator implementation exists in workspace source. The target exercises only the production
`TemporalStore::vertex_as_of` API that exists in both the recorded old revision and the new
revision. For each PointHistory cell it reports:

- `*_allocator_total_bytes_per_op`: allocator-observed bytes allocated during the profiled query
  window, divided by the configured positive iteration count;
- `*_allocator_peak_live_bytes`: the high-water mark of heap bytes allocated after the profiling
  window opened and still live at the same instant;
- `*_allocator_current_live_bytes`: those tracked bytes still live when the window closes; and
- `*_allocator_allocations_per_op`: allocator events divided by the iteration count.

These are physical heap-allocation observations, not logical history-byte counters and not RSS.
The tracker is process-wide, so RocksDB background-thread allocations that occur inside the narrow
query window are included; three isolated process runs and medians are required for old/new gate
evidence. Allocations already live before the profiling window are intentionally excluded, which
makes `allocator_peak_live_bytes` an incremental query-window heap peak rather than total process
resident memory.

The removed `history_bytes + payload_bytes_copied` field was neither peak retained memory nor an
upper bound on it. The new-reader-only `payload_bytes_copied` statistic also has no semantically
equivalent old-path counter, so it is not emitted as comparable benchmark evidence. It may still be
used as an internal non-gating PointHistory diagnostic, but it cannot support an old/new gate
without a separately reviewed counter on the historical production path.

## Post-change raw runs

Not run in this task. The required three 10,000-iteration latency samples and matching allocator
samples were explicitly deferred. No smoke output is presented as a performance result.

### Required replay procedure

1. Apply the corrected latency benchmark patch to the recorded pre-change implementation. Run the
   command below three times at 10,000 iterations and save complete raw output. The frozen
   historical `ns_per_op` rows remain continuity evidence, but matching old/new p50/p95 gates must
   use the new separate percentile pass on both revisions.
2. Run the same latency command three times against the post-change implementation and save every
   complete raw output:

   ```bash
   DTGPROXY_BENCH_ITERS=10000 cargo bench --locked -p temporal-storage --features rocksdb-tests --bench roundtrip
   ```

3. Apply the identical `roundtrip_alloc` source and dev-dependency patch to the recorded pre-change
   implementation. Run the following command in three fresh processes on both revisions, with the
   same positive allocation iteration count, and preserve raw output:

   ```bash
   DTGPROXY_BENCH_ALLOC_ITERS=1 cargo bench --locked -p temporal-storage --features rocksdb-tests --bench roundtrip_alloc
   ```

4. For each stable PointHistory cell, report the median of the three old and three new p50, p95,
   allocator-total-byte, and allocator-peak-live-byte observations. Preserve the historical and
   current `ns_per_op` rows as continuity evidence, but do not present them as percentiles.
5. Use `allocator_total_bytes_per_op` for the physical allocation gate and
   `allocator_peak_live_bytes` for the incremental query-window peak-live gate. Do not compare
   either field with `rocksdb_bytes`, RSS, the removed logical sum, or the unmatched new-reader
   payload-copy diagnostic.

## Comparison

No post-change 10,000-iteration observations have been collected. The latency gates require
matching old and new p50/p95 medians. The physical allocation and incremental peak-live gates now
have an old/new-compatible procedure, but neither revision has been sampled with it. The exact
payload-copy gate remains unresolved because the historical production path exposes no equivalent
counter. Therefore no Task-8 acceptance gate is claimed.

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
