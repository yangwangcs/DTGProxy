# DTGProxy Middleware Path Performance Optimization Design

Status: approved

Date: 2026-07-31

## Goal

Reduce DTGProxy's common-path fixed overhead without changing Fjall, PostgreSQL, or Neo4j internals.
The work targets both single-request latency and concurrency-eight throughput on the complete path:

```text
persistent Bolt client -> Gateway -> Data Node/Raft -> official Provider
```

The current Fjall baseline is functional but slow: concurrency-one p50 is 24.530 ms for CREATE,
38.758 ms for point lookup, and 37.030 ms for COUNT; concurrency-eight throughput is 60.6, 51.8,
and 56.8 operations per second respectively.

## Constraints

- Preserve durability, Raft commit, snapshot consistency, transaction replay, temporal semantics,
  result ordering, and Bolt RUN/PULL state-machine behavior.
- Do not change storage-provider data models or internal implementation.
- Do not add a benchmark-only production branch.
- Keep PostgreSQL, Neo4j, and Fjall on the same optimized middleware path.
- Run one backend at a time and do not run migration tests.
- Retain only optimizations supported by isolated before/after evidence.
- Do not claim a multiplicative improvement that the measured artifacts do not show.

## Acceptance Criteria

Against a fresh three-repetition Fjall baseline with one-second warmup and five-second measurement:

- concurrency-one p50 for every workload decreases by at least 30%;
- concurrency-eight throughput for every workload increases by at least 25%;
- p95 and p99 do not regress by more than 10%;
- every cell has zero errors and identical result identity and acknowledgements;
- the final implementation passes the focused suites, four-process Bolt certification, formatting,
  and workspace all-targets strict Clippy.

The corresponding initial targets are CREATE p50 <= 17.17 ms and c8 >= 75.75 ops/s; point lookup
p50 <= 27.13 ms and c8 >= 64.75 ops/s; COUNT p50 <= 25.92 ms and c8 >= 71.00 ops/s. These numbers
are recalculated from the pooled three-repetition baseline before judging the final result.

## Stage Metrics

Add an internal `RequestStageMetrics` facility with a fixed stage enum and fixed-size logarithmic
histogram buckets. Recording uses monotonic timestamps and relaxed atomic updates, performs no heap
allocation, and never records statements, parameter values, credentials, or result contents.

Gateway stages cover Bolt decode, compile, plan, internal RPC wait, local execution/materialization,
and Bolt encode. Data stages cover request validation, replica routing, Raft propose/Ready, and
Provider execution. Metrics expose an internal read-only snapshot for tests, diagnostics, and later
monitoring export. They do not introduce a user protocol or tune the process automatically.

Diagnostic cells snapshot stage counters at cell boundaries and bind them to the backend, workload,
concurrency, repetition, revision, and normal end-to-end samples. Stage totals must account for the
dominant end-to-end time before an optimization is accepted as causal evidence.

## Optimization Sequence

### 1. Bolt small-message latency

Set `TCP_NODELAY` immediately on every accepted Gateway Bolt socket and on the diagnostic client's
persistent socket. Failure to configure the server socket rejects only that connection. Preserve
the Bolt 5.4 handshake, RUN response, PULL records, summary, reset, and goodbye ordering.

Measure this change alone. It addresses delayed small-message delivery across the two mandatory
RUN/PULL exchanges and affects reads and writes on every backend.

### 2. Gateway concurrency

Replace the Gateway binary's current-thread Tokio runtime with a bounded multi-thread runtime. One
task continues to own and sequentially process each Bolt connection, while independent connections
may execute concurrently. Shared state remains behind existing synchronization boundaries and must
remain `Send + Sync` under compilation and process tests.

Measure this change separately after the Bolt change. Reject it if concurrency-eight throughput or
tail latency fails the acceptance rule.

### 3. Evidence-gated compile and plan cache

Implement caching only if compile plus planning consumes at least 10% of p50 or two milliseconds.
The compile key is the complete statement text. A physical plan key additionally contains Catalog,
Schema, Topology, Backend, and snapshot-fence fingerprints. Parameter values and results are never
cached. Capacity is 1,024 entries, and planning-context changes make old-fence entries unusable.

Cache corruption or an invalid fence is not accepted as a hit. Ordinary capacity eviction is a
miss. This stage is omitted when the evidence threshold is not met.

### 4. Evidence-gated write timestamp fusion

The current CREATE path performs sequential Meta start-time allocation, Meta commit-time
reservation, Data/Raft apply, and Meta committed resolution. If CREATE still misses the acceptance
target after the common-path stages, introduce one Meta operation that atomically returns start and
commit timestamps.

The operation must preserve `start_time < commit_time`, request idempotency, replay identity, and
fail-closed recovery. Data/Raft apply remains durable before committed resolution. Abort and retry
semantics remain unchanged. This does not combine the Data commit with Meta resolution and does not
bypass consensus.

## Error Handling

- Metrics use bounded storage and cannot change the request result.
- A metrics counter overflow saturates rather than wrapping.
- TCP socket configuration failure is connection-local and explicit.
- Cache misses and capacity eviction execute the original path; invalid fences still fail closed.
- No stage automatically weakens consistency or chooses a faster semantic mode.
- A performance change that breaks correctness or the tail-latency gate is reverted as a stage,
  instead of being hidden by later optimizations.

## Verification

Use red-green-refactor for every production change. Add focused tests for histogram boundaries,
concurrent metric recording, outcome accounting, TCP_NODELAY, Bolt ordering, multi-connection
parallelism, and single-connection ordering. Add cache and fused-timestamp tests only when those
evidence gates trigger.

Capture a three-repetition release baseline before changes. After each retained stage, rerun the
same six Fjall workload/concurrency groups with fresh four-process clusters and compare pooled
latencies, total measured duration, throughput, row identity, and errors. After the optimized Fjall
path passes, start PostgreSQL and Neo4j one at a time for the same end-to-end measurement. Do not run
migration tests. Report every retained and rejected stage, its isolated delta, and environmental
limitations.
