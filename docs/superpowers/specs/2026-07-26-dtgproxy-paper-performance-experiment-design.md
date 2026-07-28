# DTGProxy Paper Performance Experiment Design

Date: 2026-07-26

Status: approved direction, implementation pending.

## Objective

Produce defensible paper evidence that DTGProxy provides a backend-independent temporal graph
language layer with bounded middleware overhead and useful multi-node scaling. The artifact must
separate backend cost, Adapter cost, and complete Proxy cost; quantify the contribution of each
major optimization; and preserve one reproducible final experiment run with raw observations.

## Current Measurement Defect

The existing scale-out harness starts one `dtgproxy-bolt-probe` operating-system process for every
query. Each process opens a TCP connection, negotiates Bolt, sends `HELLO`, executes one query, and
exits. Process creation, dynamic loading, connection establishment, and authentication setup are
therefore included in throughput even though the TTFR definition intentionally excludes them.

This fixed client overhead suppresses the visible benefit of additional data nodes and makes the
1/4/8 throughput comparison unsuitable for a paper. It must remain available as a connection-cold
measurement, but it must not be used as the steady-state throughput driver.

## Experiment Paths

Every path consumes the same generated dataset manifest, logical workload manifest, snapshot, and
query parameters. Every observation carries the same BLAKE3 result digest and row count.

### Backend Direct

Execute the backend-native operation without the DTGProxy language, planner, distributed runtime,
Gateway, or Bolt layers:

- RocksDB: snapshot iterator, MultiGet, prefix adjacency scan, and temporal-index scan.
- PostgreSQL: parameterized SQL over the managed native mapping tables.
- Neo4j: parameterized Cypher over the managed labels, relationships, and version records.

Backend Direct is not allowed to use a semantically weaker query. A workload without an equivalent
native operation is marked unavailable instead of being approximated.

### Adapter Direct

Execute the typed Storage SPI primitive through the production Adapter and pinned `ReadSnapshot`,
while bypassing T-Cypher compilation, optimization, distributed execution, Gateway, and Bolt. This
isolates Adapter translation, canonical page construction, bounds, continuation, and residual
preparation overhead.

### Proxy

Execute the complete external path: persistent Bolt connection, Gateway, T-Cypher compile and
optimize, distributed planning and execution, typed Adapter primitives, canonical residual
semantics, and Bolt `RECORD` decoding.

## Persistent Bolt Load Generator

Extend `bolt-server` with a reusable `BoltProbeSession` that owns one negotiated TCP connection and
`Receiver`. `connect` performs TCP setup, Bolt 5.8 negotiation, and `HELLO` once. `execute` sends one
`RUN`, consumes `RUN SUCCESS`, sends `PULL -1`, records TTFR at the first decoded `RECORD`, consumes
the summary, and leaves the connection ready for another auto-commit query.

Create `dtgproxy-bolt-loadgen` with these required arguments:

- `--address HOST:PORT`
- `--query QUERY`
- `--connections COUNT`
- `--warmup-seconds SECONDS`
- `--duration-seconds SECONDS`
- `--timeout-ms MILLISECONDS`
- `--output FILE`

The load generator creates exactly `connections` sessions, synchronizes their measured start,
reuses each session until the duration expires, and writes one JSON report. It records completed
operations, throughput, per-operation TTFR and total latency samples, result identity, connection
setup latency, errors, and exact timing boundaries. Any sample identity mismatch invalidates the
whole run.

The existing one-shot `dtgproxy-bolt-probe` remains the production TTFR and connection-cold probe.

## Multi-Node Optimization Method

Optimization proceeds from measured evidence, not threshold tuning:

1. Remove per-query client process and handshake cost from steady-state throughput.
2. Record Gateway admission wait, catalog wait, shard fanout start/completion, Adapter page count,
   exchange encoded/decoded bytes, and query CPU time where available.
3. Compare 1/4/8 profiles for the same result identity and concurrency.
4. Fix the largest serialized or duplicated data-plane cost.
5. Re-run only a short diagnostic workload until the final experiment.

The formal scale-out gate remains 4 nodes at or above 2.8x and 8 nodes at or above 5.0x the
one-node throughput for partition-parallel workloads. A global aggregation whose final combine is
inherently serial is reported separately and is not used as the only scale-out workload.

## Workload Suite

The fixed dataset contains 1,000,000 vertices, 5,000,000 edges, and temporal updates for 10% of
elements. Generation is deterministic from a versioned seed and records a manifest digest.

The minimum workload classes are:

1. Point lookup with property projection.
2. One-hop adjacency expansion.
3. Property-filtered current scan.
4. Historical `AS OF` lookup.
5. Temporal change scan.
6. Partition-parallel aggregation.
7. Multi-shard traversal or aggregation.
8. Bounded write and mixed read/write workload.

Each workload declares whether Backend Direct, Adapter Direct, and Proxy are all semantically
available. Comparisons use only paths with identical logical results.

## Ablation Matrix

Runtime benchmark configuration exposes these paper-only modes without changing language or SPI
contracts:

- `native_pushdown`: production typed primitives versus canonical residual-only reads.
- `column_batch`: internal column representation versus row/scalar conversion at each operator.
- `bounded_lazy_pages`: bounded lazy page pulls versus eager page collection.
- `parallel_shard_fanout`: bounded parallel shard starts versus one-at-a-time shard execution.
- `batched_property_gather`: one bounded gather request versus individual property reads.

The production configuration is the all-enabled mode. Ablations must be explicitly named in every
report and must never be selectable from the public T-Cypher language or Adapter SPI. Disabling an
optimization must preserve result identity.

## Statistical Protocol

The formal matrix uses node counts 1/4/8 and concurrency 1/8/32/64. Each cell uses 30 seconds of
warmup, 60 seconds of measured execution, and five independent repetitions. Run order is
deterministically shuffled to reduce thermal and cache-order bias.

Reports include throughput, p50/p95/p99 total latency, p50/p95/p99 TTFR, CPU time, peak RSS,
network bytes, operation count, error count, digest, and row count. The summarizer reports mean,
median, standard deviation, 95% confidence interval, and relative overhead or speedup. A cell with
errors, identity drift, missing repetitions, or unavailable required metrics is invalid.

The matrix is the union of three explicit suites, not a global Cartesian product:

- `comparison`: three paths, one data node, production only.
- `scale`: Proxy production only, 1/4/8 data nodes.
- `ablation`: Proxy only at a fixed representative node/concurrency anchor, production plus five
  single-disabled modes.

Each suite selects workloads for its purpose. Overlapping Proxy production cells run once. Direct
paths never inherit multi-node or ablation axes.

## Reproducible Artifact

The final artifact is rooted at `artifacts/paper-performance/<run-id>/` and contains:

- `manifest.json`: revision, dirty-worktree digest, toolchain, OS, CPU, memory, backend versions,
  dataset seed and digest, workload digest, and experiment protocol.
- `configs/`: exact topology and ablation configurations.
- `raw/`: immutable JSON output for every repetition.
- `summary/`: machine-generated CSV and JSON statistics.
- `figures/`: generated paper plots.
- `logs/`: process stdout/stderr and failure evidence.
- `SHA256SUMS`: checksums for every artifact file.

One command prepares services and binaries, one command runs the complete matrix once, and one
command verifies checksums and regenerates summaries and figures without rerunning experiments.

## Failure Rules

- No zero-test or zero-operation success.
- No mock, thread, or in-process worker may be counted as a data node.
- No formula-only performance result may be reported as a measurement.
- No Candidate result may be presented as Exact.
- No retry may silently replace a failed formal repetition.
- No final report is valid when Backend Direct, Adapter Direct, and Proxy return different result
  identities for a compared workload.

## Deferred Scope

The paper prototype does not require elastic online rebalancing, cross-region deployment, a public
benchmark control API, or complete production spill-to-disk. End-to-end materialization points are
reported as limitations and quantified by the eager/lazy ablation rather than hidden.
