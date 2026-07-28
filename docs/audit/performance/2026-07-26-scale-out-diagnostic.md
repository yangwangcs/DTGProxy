# DTGProxy Scale-Out Diagnostic (2026-07-26)

## Scope

This is a short development diagnostic, not the formal paper experiment. It uses independent
operating-system data-node processes, one persistent external Bolt load generator per topology,
eight fixed logical shards, a one-second warmup, a three-second measurement, and eight concurrent
connections.

## Pre-Fix Evidence

The first attempt used 4,096 vertices. `MATCH (n) RETURN count(n)` failed before measurement with
`Distributed("distributed query protocol error: PayloadLimit")`. The shard fragments scan and
return materialized node rows while the coordinator performs the aggregate, so a scalar result can
exceed the exchange payload bound.

A 256-vertex diagnostic completed and produced the same result identity for every topology:

| Data nodes | Throughput (ops/s) | Relative to 1 node | Mean TTFR (ms) | Network RX+TX (bytes) | Process RSS (bytes) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 27.249 | 1.00x | 293.399 | 43,427 | 84,754,432 |
| 4 | 31.646 | 1.16x | 252.358 | 93,437 | 167,952,384 |
| 8 | 31.010 | 1.14x | 256.542 | 141,720 | 274,677,760 |

All cells returned digest
`d4bc6e359b4fb16377fa01e4b52c97e626e205e5fee5501357f0b436c799f566` and row count `1`.

Raw reports are under `target/scale-out-diagnostic-256-before-partial-v2/` and are development
evidence only.

## Diagnosis

The fixed eight-shard topology removes the previous deployment-mode and shard-count confounder.
The remaining evidence points to coordinator-side aggregation: increasing physical data nodes does
not reduce the number of rows crossing the exchange, while RPC, network, process memory, timestamp,
and coordinator costs increase. The 4,096-row `PayloadLimit` is the correctness-impacting form of
the same bottleneck.

The selected first data-plane fix is shard-local partial count with coordinator final sum. The
post-fix diagnostic must preserve the digest and row count, complete at 4,096 rows, and exchange no
more than one partial row per logical shard.

## Post-Fix Evidence

Shard-local partial count and coordinator final sum removed the exchange payload failure. The same
eight-shard deployment completed at 4,096 vertices with eight concurrent persistent Bolt sessions:

| Data nodes | Throughput (ops/s) | Relative to 1 node | Mean TTFR (ms) | Actual measured elapsed (s) | Network RX+TX (bytes) |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1.018 | 1.00x | 4471.149 | 7.857 | 72,378 |
| 4 | 1.349 | 1.33x | 3439.543 | 5.929 | 138,372 |
| 8 | 1.284 | 1.26x | 3778.631 | 6.229 | 205,005 |

All cells returned the exact expected digest
`3914e62ea65785c975f12b61eb5d6021dcced4e835a3f8768e69f672fcdbc6ca` for a single
`count(n) = 4096` row. Raw reports are under
`target/scale-out-diagnostic-4096-after-partial-v3/`.

This run demonstrates a correctness and bounded-exchange fix, not physical-node linear scaling.
The 1/4/8 data-node processes share one eight-core host and the one-node topology already owns all
eight logical shards, so it can use host-wide RocksDB and executor parallelism. Additional processes
do not add CPU, memory bandwidth, or storage bandwidth. Physical scale-out ratios require the final
1/4/8 host experiment; local process ratios must not be presented as that result.

## Formal Experiment Readiness Audit

The formal harness now has a sealed preparation contract, but the formal run has not started. The
workspace currently contains no formal experiment spec, runtime manifest, dataset/backend/build
evidence, prepared bundle, multi-host inventory, or `artifacts/paper-performance/<run-id>` output.
Only development-profile binaries are present. Consequently, no current file proves that a real
1/4/8-host environment or the fixed 1,000,000-vertex/5,000,000-edge dataset is available.

Before publishing `READY.json`, `scripts/prepare-paper-performance.sh` now requires live independent
data-node processes, distinct Gateway Unix control sockets, exact dataset evidence, three backend
versions, release binary digests, and a prebuilt `dtgproxy-paper-benchmark` validator. The validator
rechecks the Rust `ExperimentSpec` schema, workload and environment digests, formal matrix shape,
and executor SHA-256 without creating an artifact. The complete 30-second warmup/60-second
measurement/five-repetition matrix remains reserved for one invocation after those external inputs
exist.
The preparation gate also resolves every declared PID to its running executable image; a merely
live unrelated process is rejected. Formal execution invokes the same prebuilt orchestrator
directly and cannot run Cargo or silently substitute a newly built binary.

Current focused evidence:

- `paper-benchmark` library and both binaries pass `cargo check` with the system RocksDB development
  link path.
- Gateway library and `dtgproxy-gateway` with `paper-benchmark-control` pass `cargo check`.
- T-Cypher clean-break, formatting, diff, shell syntax, and preparation contract checks pass.
- Gateway and Bolt integration-test binaries did not finish linking within the development time
  budget; they are not recorded as passing. This does not authorize the formal run.
