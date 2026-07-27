# DTGProxy Single-Backend Diagnostic Baseline

Date: 2026-07-27

## Environment

- Host: Apple M2, 16 GiB RAM, arm64 macOS (Darwin 27.0.0).
- Toolchain: `rustc 1.93.0`, `cargo 1.93.0`.
- Code revision: `f20e44cb6f6fa4f060a045cf0756417264176b44` with a dirty worktree;
  the worktree status digest captured during audit was
  `2c1e13845893e6f555f195e0b7990b4e29785cfac559e09e9d35914d417bbc4d`.
- PostgreSQL evidence identifies image `postgres:17`; the live readiness check at
  `127.0.0.1:55432` accepted connections and the host client was PostgreSQL 17.10.
- Neo4j evidence identifies image `neo4j:5.26-community` and endpoint
  `http://127.0.0.1:57474`. The service was available when its baseline was captured but was not
  running during this documentation audit; the sealed raw observations remain independently
  hash-verified.

These are shortened local diagnostic measurements, not the formal 30-second warmup, 60-second
measurement, five-repetition, 1/4/8-node paper matrix.

## Backend isolation evidence

The three persistent manifests are stored separately under
`target-codex-paper-diagnostics/2026-07-27-single-backend/{rocksdb,postgresql,neo4j}/`.
Every manifest selects exactly one backend and marks the run mode `diagnostic`. Each contains six
raw observations: three Backend Direct and three Adapter Direct. All 18 raw files match the SHA-256
recorded in their manifest. Every observation reports zero errors and the same result identity:

```text
row_count = 1
digest = 3914e62ea65785c975f12b61eb5d6021dcced4e835a3f8768e69f672fcdbc6ca
```

RocksDB used an embedded temporary database. PostgreSQL and Neo4j used distinct real service
instances. Only the selected backend was marked available in each runtime manifest; the two other
backend entries were explicitly unavailable with reason `not selected`.

## Workload and protocol

- Workload: current vertex count at fixed snapshot (`count_current_vertices`).
- Dataset: graph 7 with 4,096 current vertices.
- Paths: semantically equivalent Backend Direct and production Adapter Direct.
- Concurrency: 1.
- Warmup: 1 second per observation.
- Measurement: 3 seconds per observation.
- Repetitions: 3 per path and backend.
- Correctness gate: identical result digest and row count, with zero errors.
- Summary method: pooled latency samples and total completed operations over nine measured seconds.

Reproduce a diagnostic in a new absolute directory with:

```bash
scripts/run-paper-diagnostic.sh \
  --backend rocksdb \
  --output-dir /absolute/path/to/diagnostics/rocksdb \
  --warmup-seconds 1 \
  --measurement-seconds 3 \
  --repetitions 3
```

PostgreSQL additionally requires `DTGPROXY_PAPER_TEST_POSTGRES_URL`; Neo4j requires
`DTGPROXY_PAPER_TEST_NEO4J_ENDPOINT`, `DTGPROXY_PAPER_TEST_NEO4J_USERNAME`,
`DTGPROXY_PAPER_TEST_NEO4J_PASSWORD`, and `DTGPROXY_PAPER_TEST_NEO4J_DATABASE`.

## RocksDB baseline

| Path | Operations | Throughput (ops/s) | p50 latency (ms) | p95 latency (ms) |
|---|---:|---:|---:|---:|
| Backend Direct | 2,499 | 277.666667 | 3.760625 | 3.970500 |
| Adapter Direct | 1,811 | 201.222222 | 4.906958 | 5.379333 |

The Adapter path reduced throughput by 27.53% and increased p50 latency by 30.48% relative to the
Backend Direct path in this diagnostic.

## PostgreSQL baseline

| Path | Operations | Throughput (ops/s) | p50 latency (ms) | p95 latency (ms) |
|---|---:|---:|---:|---:|
| Backend Direct | 12,892 | 1,432.444444 | 0.682917 | 0.888875 |
| Adapter Direct | 3 | 0.333333 | 2,650.087042 | 2,680.739625 |

The Adapter path was approximately 4,297 times lower-throughput and 3,881 times higher-latency
than Backend Direct. Only one Adapter operation completed in each three-second measurement window.

## Neo4j baseline

| Path | Operations | Throughput (ops/s) | p50 latency (ms) | p95 latency (ms) |
|---|---:|---:|---:|---:|
| Backend Direct | 1,599 | 177.666667 | 5.335000 | 7.179583 |
| Adapter Direct | 225 | 25.000000 | 39.480833 | 46.213334 |

The Adapter path reduced throughput by 85.93% and increased p50 latency by 640.04% relative to the
Backend Direct path.

## Stage timing and bottleneck ranking

The comparable middleware-controlled excess p50 time is approximately:

1. PostgreSQL Adapter Direct: 2,649.404 ms above Backend Direct.
2. Neo4j Adapter Direct: 34.146 ms above Backend Direct.
3. RocksDB Adapter Direct: 1.146 ms above Backend Direct.

Inspection of `PostgresReadSnapshot::scan_canonical_page` and the canonical scan SQL explains the
first result: the page query returns only logical keys, then `load_canonical_entry` issues another
database query for every selected key. Counting 4,096 current vertices therefore creates an N+1
query pattern. Current, History, and Opaque tables already store canonical Storage SPI values and
can return those bytes in the page query without changing ordering, continuation, snapshot, or
result semantics.

## Missing formal evidence

- Proxy-path, TTFR, CPU, RSS, network, and detailed Gateway stage metrics were not captured by this
  small diagnostic.
- The formal 1,000,000-vertex/5,000,000-edge/600,000-update dataset was not run.
- Concurrency 8/32/64 and real 1/4/8 Data Node topologies were not run.
- Formal 30-second warmup, 60-second measurement, and five repetitions were not run.
- No production SLO or universal backend ranking may be inferred from these results.

## Optimization target selection

The selected target is PostgreSQL canonical-page materialization because it is the largest measured
middleware cost by two orders of magnitude, has a direct code-level explanation, and can be changed
without weakening T-Cypher, temporal, persistence, ordering, or continuation semantics. The
implementation plan is
`docs/superpowers/plans/2026-07-27-dtgproxy-measured-performance-optimization.md`.

Acceptance requires the same 4,096-vertex, concurrency-1, 1/3/3 diagnostic protocol, zero errors,
and unchanged result identity. PostgreSQL Adapter Direct must improve from 0.333333 ops/s and
2,650.087042 ms p50; RocksDB and Neo4j are rerun in the same fixed order to expose environmental
drift rather than being silently carried forward.
