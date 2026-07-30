# DTGProxy Three-Backend End-to-End Diagnostic Design

Status: approved

Date: 2026-07-31

## Goal

Add and run a short, reproducible local diagnostic that compares Fjall, PostgreSQL, and Neo4j
through the complete production request path:

```text
persistent Bolt client -> Gateway -> Data Node -> official in-process storage provider
```

The result is development-host diagnostic evidence, not a formal paper benchmark or service SLO.

## Isolation and lifecycle

Run one backend at a time. Every backend receives a fresh single-Meta, single-Controller,
single-Data, single-Gateway cluster and a unique graph, Shard, replica namespace, and data
directory. Fjall uses a temporary local directory. PostgreSQL 17 and Neo4j 5.26 Community run in
disposable Docker containers with dynamically assigned loopback ports and generated credentials.

The runner records exact child process and container identities. Normal completion and failure both
stop the four DTGProxy processes, remove the two external backend containers, and remove temporary
database state. It never searches for or kills unrelated processes. Only the immutable JSON and CSV
diagnostic output remains.

## Workloads

Each backend executes the same workloads through persistent Bolt 5.4 connections:

1. `create_vertex`: auto-commit `CREATE (n:Bench {value: 1}) VALID FROM 1`, measuring a complete
   durable write acknowledgement.
2. `point_lookup`: a parameterized identifier lookup against a preloaded deterministic data set.
3. `count_vertices`: a full current vertex count against the same preloaded deterministic data set.

The read data set contains 4,096 vertices and is prepared before warmup. Write cells use a fresh
backend namespace so their growing state cannot affect read cells. Each workload/backend/concurrency
cell starts from fresh state; warmup mutations are discarded with that cell.

## Protocol

- Concurrency: 1 and 8 persistent Bolt sessions.
- Warmup: 1 second per cell.
- Measurement: 5 seconds per cell.
- Repetitions: 3.
- Backend order: deterministic seeded shuffle recorded in the manifest.
- Cell failure: no retry; record the error and stop publication of a successful combined summary.
- Correctness: every operation must complete without Bolt failure, read repetitions must return the
  expected row count and digest, and write cells must report successful empty-row acknowledgements.

The default matrix is 3 backends x 3 workloads x 2 concurrency values x 3 repetitions. With six
seconds of timed work per cell, it is intended to finish in several minutes after binaries and
container images are available.

## Metrics and artifacts

For every cell record:

- completed operations and errors;
- throughput in operations per second;
- p50, p95, and p99 end-to-end latency;
- warmup and measurement timestamps;
- backend, workload, concurrency, repetition, query digest, result digest, and row count;
- DTGProxy binary revision and host OS/architecture;
- external backend image and version where applicable.

Write a new output directory under `artifacts/backend-e2e-diagnostic/<run-id>/` containing:

```text
manifest.json
raw/*.json
summary.json
summary.csv
logs/
SHA256SUMS
```

Final files are written atomically and are never overwritten. The summary groups by backend,
workload, and concurrency; it pools measured latency samples across three repetitions and computes
throughput from total completed operations divided by total measured duration. It never averages
different backend families into one score.

## Implementation boundaries

Reuse the current clean-break four-process test support, provider bindings, and Bolt encoding rather
than reintroducing the removed Adapter SPI or legacy RocksDB benchmark packages. Add a dedicated
diagnostic load generator and lifecycle runner with contract tests. Production query, transaction,
storage, and process behavior must not gain benchmark-only branches.

## Verification

Before reporting measurements:

1. Run the diagnostic runner contract tests.
2. Run the existing four-process real Bolt certification.
3. Run focused Fjall, PostgreSQL, and Neo4j provider tests.
4. Execute the complete short matrix once.
5. Recompute summaries and checksums from raw observations and reject mismatches.
6. Confirm every managed child process and container has retired.

