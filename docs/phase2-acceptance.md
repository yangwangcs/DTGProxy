# Phase 2 acceptance evidence

Date: 2026-07-17. This is a single-machine engineering baseline, not a distributed production SLO.

## Environment

- Apple M2, 16 GiB RAM, arm64;
- macOS/Darwin 27.0.0;
- Rust/Cargo 1.93.0;
- RocksDB 10.4.2 through `rocksdb` crate 0.24.0;
- LLVM clang/libclang supplied explicitly for RocksDB native builds.

## Correctness evidence

`cargo test --workspace` passes the complete workspace, including:

- three real `dtgproxy-raft-node` processes over TCP;
- RF=3 write convergence, process exit, durable reopen on new addresses, new-term leader, second write, and final convergence;
- WAL crash between durable commit and Adapter apply;
- snapshot publication/install crash boundaries and 16-entry suffix replay;
- full Current/AS OF/DIFF, edge, and both adjacency semantic equality after snapshot recovery;
- leader/follower ReadIndex, term, epoch, applied-index, and safe-time fences;
- PrimaryReplica and SharedNothing routing with logical partition/physical Shard separation;
- SPI capability rejection, registry selection, secret redaction, and index-fenced backend cutover.

Exact commands:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test --workspace

CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

## Reproducible microbenchmark

Run:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
DTGPROXY_PHASE2_BENCH_ITERS=200 \
DTGPROXY_SNAPSHOT_BENCH_ITERS=3 \
cargo bench -p dtgproxy --bench phase2
```

Selected optimized results from this environment:

| Measurement | mean | p50 | p99 |
|---|---:|---:|---:|
| RF1 proposal→commit→apply | 36.8 µs | 36.4 µs | 81.5 µs |
| RF1 proposal→commit observed | 1.56 µs | 1.21 µs | 5.46 µs |
| RF1 commit→Adapter apply | 28.6 µs | 28.6 µs | 68.2 µs |
| RF3 proposal→commit→apply | 109 µs | 106 µs | 184 µs |
| RF3 proposal→commit observed | 10.9 µs | 10.3 µs | 22.0 µs |
| RF3 commit→Adapter apply | 27.6 µs | 26.5 µs | 49.0 µs |
| RF3 follower ReadIndex + temporal point read | 7.63 µs | 6.71 µs | 24.0 µs |
| RF3 replicated closed-timestamp tick | 82.5 µs | 81.9 µs | 179 µs |
| PrimaryReplica route | 35 ns | 42 ns | 42 ns |
| 128-Shard rendezvous route | 456 ns | 417 ns | 667 ns |
| checkpoint install + 16-entry suffix catch-up | 130 ms | 106 ms | sample too small |

The in-process Raft harness has no kernel network or disk WAL on each proposal, so these numbers isolate consensus/state-machine overhead and must not be extrapolated to a production cluster. The three-process smoke verifies process/network/durable behavior but is not yet a throughput benchmark.

## Unmet release gates

- persistent pooled async network transport with TLS and load shedding;
- multi-host chaos, disk-full/corruption, clock skew, packet loss, and long-duration tests;
- Meta/TSO/Balancer and complete cross-Shard transaction recovery;
- PostgreSQL and graph Sidecars plus their live backend conformance matrices;
- security review, dependency/SBOM audit, upgrade compatibility, backup/restore drills, and capacity envelopes.
