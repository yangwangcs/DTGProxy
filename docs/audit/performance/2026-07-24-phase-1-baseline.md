# DTGProxy 1.1 Phase 1 Performance Baseline

Date: 2026-07-24

Status: deterministic data-plane gates and query-lifecycle counters pass locally; real-backend
latency, throughput, TTFR, scale-out, and remote CI certification remain pending.

## Release CI Gates

`.github/workflows/quality-gates.yml` defines the following fail-closed checks in the current
workspace. It does not enforce a remote release gate until the workflow and referenced untracked
files are committed, pushed, and configured as required status checks.

| Gate | CI job | Command | Coverage |
|---|---|---|---|
| Paired semantic and overhead contract | `default-and-rocksdb` | `cargo test --locked -p cypher-engine --test direct_proxy_gate -- --test-threads=1` | Fixed fixture and snapshot; every-sample direct/proxy schema-value digest equality, real engine lifecycle counters, and fail-closed overhead threshold evaluation. |
| Cancellation and backpressure | `default-and-rocksdb` | `cargo test --locked -p distributed-query --features test-support --test performance_regressions -- --test-threads=1` | Source pull credit, no post-first-result requeue, decode-credit release, and cancellation cleanup. |
| Sidecar CanonicalScan | `default-and-rocksdb` | `cargo test --locked -p adapter-sidecar --test protocol --test client --test snapshot_protocol --test stateful_snapshot --test tcp -- --test-threads=1` | Feature negotiation, request/response codec, fixed snapshot index, page bounds, continuation, TCP read views, and fail-closed legacy peers. |
| PostgreSQL SQL contract | `default-and-rocksdb` | `cargo test --locked -p adapter-postgres --test sql_contract -- --test-threads=1` | Query shape and backend-neutral canonical scan contract without an external service. |
| macOS C++ and RocksDB | `macos-rocksdb-toolchain` | `scripts/macos-cxx` compile/link probe plus RocksDB roundtrip and adapter contract tests | SDK-resolved libc++ `<cstdint>` headers and native RocksDB linkage; the header probe does not execute its temporary binary. |
| PostgreSQL live adapter | `postgres-live` | `cargo test --locked -p adapter-postgres -- --test-threads=1` | Starts a disposable PostgreSQL 17 service and always runs the complete adapter package tests. |
| Neo4j live adapter | `neo4j-live` | `cargo test --locked -p adapter-neo4j -- --test-threads=1` | Starts a disposable Neo4j 5.26 Community service and always runs the complete adapter package tests. |

The live jobs use workflow-owned disposable credentials and do not skip when repository secrets are
absent. A release that claims real-backend verification must retain successful logs from both jobs
and from the separate three-backend migration/takeover matrix.

## Evidence Available

The following checks are reproducible on the current workspace:

- Base revision: `5edbfc66bd1850d5b1b27c79c644baae29ec2eae`; evidence was collected from
  the dirty DTGProxy 1.1 workspace and is not a claim about that base commit alone.
- Fixture source SHA-256: `77c265cf46cf13ad9c8abf86a2d7a454ba094b2d793400e400f89c10e9ad3832`.
- Toolchain: `rustc 1.93.0 (254b59607 2026-01-19)`, LLVM `21.1.8`.
- Host: Apple arm64, macOS `27.0` build `26A5388g`.
- Deterministic gates use one test thread. The observed direct/proxy lifecycle gate uses one paired
  sample; the compatibility semantic/latency fixture uses three paired samples.

| Area | Evidence | Result |
|---|---|---|
| Borrowed exchange values | `query-executor` ColumnBatch tests | passed |
| Lazy worker encoding | `distributed-query` local worker tests | passed |
| CandidateScan lazy execution | `query-executor` Candidate morsel tests | passed: 2 tests |
| Frame lifecycle | `distributed-query` performance regression tests | passed: 8 tests |
| Bounded canonical fallback | `temporal-storage` batched materialization tests | passed |
| Candidate page bounds | Memory, temporal-storage, and executor contract tests | passed |
| Snapshot and capability fencing | distributed query and executor tests | passed |
| Backend-neutral clean break | `scripts/check-tcypher-clean-break.sh` | passed |
| RocksDB C++ toolchain | SDK libc++ `<cstdint>` probe and default dependency build | passed: `librocksdb-sys`, `rocksdb`, and `adapter-rocksdb` compiled through the repository wrapper during the default `query-executor` test build |
| Fresh default DTGProxy/RocksDB build | `CARGO_INCREMENTAL=0 cargo build --locked -p dtgproxy --bin dtgproxy -vv` with a new target directory | passed in 18m51s; `librocksdb-sys`, `adapter-rocksdb`, and the final ARM64 `dtgproxy` binary were rebuilt and linked with no `<cstdint>` error |
| Direct/proxy semantic pairing and observed lifecycle gate | `cypher-engine` direct/proxy gate | passed: 9 tests |
| Release performance threshold calculations | `cargo test --locked -p cypher-engine --test system_performance_gate -- --test-threads=1` | passed: 6 tests; boundary and fail-closed cases cover point p50/p99, TTFR allowance, single-shard throughput, peak memory, exchange bytes, and 4/8-node scale-out |
| Cancellation/backpressure source gates | `distributed-query` performance regressions | passed: 8 tests |
| Distributed query package | `cargo test --locked -p distributed-query --features test-support --lib --tests -- --test-threads=1` | passed: 74 tests across unit and integration binaries |
| Borrowed column representation | `cargo test --locked -p query-executor --test column_batch -- --test-threads=1` | passed: 3 tests; default RocksDB dependency compiled and linked |
| Full workspace compile surface | `cargo check --locked --workspace --all-targets --all-features` | passed in 24m57s; includes RocksDB, PostgreSQL, Neo4j, Sidecar, node, gateway, and CLI targets |
| Strict affected-path lint | `cargo clippy --locked -p query-executor -p distributed-query -p cypher-engine --all-targets --all-features -- -D warnings` | passed in 11m34s |
| Gateway bounded shard fanout | `cargo test --locked -p gateway-node --features test-support --test read_fanout -- --test-threads=1` | passed: 2 tests; two independent shards start before either is released, output ordinal is restored, and first error aborts and drains a pending shard |
| Gateway all-feature lint | `cargo clippy --locked -p gateway-node --all-targets --all-features -- -D warnings` | passed in 5m32s |
| Gateway analytics restart regression | `cargo test --locked -p gateway-node --test primary_replica_analytics -- --test-threads=1` | initial run: 6 passed, 1 primary-replica ordered-restart timing failure; the failed test passed immediately when rerun alone. No deterministic regression claim is made. |
| PostgreSQL 17 live adapter | disposable PostgreSQL at `127.0.0.1:55432` | passed: 6 tests; canonical pagination and pinned-snapshot behavior exercised against the real service |
| Neo4j 5.26 live adapter | disposable Neo4j Community service | passed: 5 live tests plus 17 library tests; committed apply and pinned-snapshot behavior exercised against the real service |
| Six-direction backend migration | `cargo test --locked -p dtgproxy --features three-backend-certification --test three_backend_migration -- --test-threads=1` | passed: RocksDB, PostgreSQL, and Neo4j in all six source/destination directions |
| Three-backend deployment surface | exact `all_backends_are_equivalent_in_both_deployment_modes` certification | passed: all three backends in primary-replica and shared-nothing modes |
| Takeover fault authenticity | RocksDB primary-replica Degree `publish` exact certification | passed; the requested process-stop fault is observed before recovered output is accepted |
| Sidecar and full-stack interruption authenticity | RocksDB primary-replica exact restart certifications | passed; after the service/data outage exceeds the configured execution delay, the job must still be `RUNNING` before recovery |

The deterministic exchange suite contains gates for:

- one encode and one decode for a successful frame round trip;
- frame reservations are released after successful decode and rejected decode;
- decoded transport credit is released when a later source batch fails sequence validation;
- an `EXISTS` first-visible-row request does not poll or requeue its source after the first batch;
- pending morsel sources never exceed coordinator inflight credit, cancellation drops every pending
  source, and sources beyond the credit window are never started;
- peak retained frame bytes are observable and bounded by the configured codec limit;
- worker encoding is lazy and occurs only when the next morsel is requested;
- current-time CandidateScan with an empty overlay and a streamable unary pipeline fixes one
  snapshot and advances one bounded canonical page per visible pull; LIMIT stops further adapter
  reads. As-of reads fall back to canonical history reconstruction because the backend-neutral SPI
  does not yet expose an as-of candidate snapshot. External snapshot-owner bindings, overlays, and
  blocking operators still use the safe deferred eager path;
- coordinator exchange validation no longer retains a second query-wide `Vec<ColumnBatch>` and
  `execute_each_batch` waits for one consumer callback per canonical `RecordBatch`. Callback error
  or cancellation drops fan-in sources immediately, and a terminal frame is not re-polled. The
  compatibility `execute -> Vec<RecordBatch>` API still owns the final materialized result;
- bounded fallback materialization batches keys instead of issuing one read per result.
- gateway multi-get groups by shard, starts at most 16 shard reads concurrently, restores original
  request ordinals, validates each shard result count, and aborts plus drains outstanding work on
  the first adapter or task error. The two-shard fixture records a shard-start count of 2 before
  either read is released.
- direct and proxy executions over one fixed fixture and snapshot produce the same schema/value
  digest, row count, and semantic identity.
- query-scoped compile, optimize, real Storage SPI call, sent-frame, wire encoded/decoded,
  value-copy, and peak retained memory metrics have explicit `Observed` or `Unavailable(reason)`
  states; absence is never encoded as `0`, and threshold evaluation rejects an unavailable metric.
  Storage calls are counted at backend-independent canonical primitive/page boundaries, including
  reads performed through a pinned snapshot; worker source polls are not reported as adapter RPCs.

These cancellation and backpressure gates use source poll counts, explicit start notifications,
atomic pending-source counts, and retained-byte counters. They contain no elapsed-time assertion or
wall-clock threshold.

The cancellation acknowledgement gate snapshots query-scoped adapter calls, encoded bytes, and
sent-frame counts after the coordinator returns `Cancelled`, yields the scheduler three times, and
requires all three counters to remain unchanged. A separate positive gate requires one successfully
delivered morsel to record encoded bytes and exactly one sent frame, so a permanently-zero transport
counter cannot make cancellation pass accidentally.

The macOS preview runtime can pause new binaries in `_dyld_start` for tens of seconds. Serial waits
completed successfully for the commands recorded above; no timeout-based performance conclusion is
drawn from that environment behavior.

## Query-Scoped Overhead Gate

`cypher-engine::performance` defines `QueryScopedOverhead` for one query path. Each metric is an
independent availability-tagged value rather than an optional aggregate, so a benchmark can state
which observation is missing and why:

| Metric | Meaning | Current observation |
|---|---|---|
| Compile count | Calls that compile the statement for this query | Observed by `CypherQueryEngine::execute` |
| Optimize count | Calls that optimize the logical statement for this query | Observed by the engine lifecycle |
| Adapter RPC count | Backend-independent Storage SPI primitive/page calls for this query | Observed by `ObservedStorageAdapter`, including calls made through a pinned `ReadSnapshot`; worker source polls are not counted |
| Wire encoded bytes | Query-scoped bytes emitted to the exchange | Observed when worker frames are encoded |
| Wire decoded bytes | Query-scoped bytes accepted from the exchange | Observed when coordinator frames are decoded |
| Value-copy bytes | Explicit copied bytes of variable-width values | Observed at row/column conversion and decode materialization; fixed-width values do not contribute |
| Peak retained memory bytes | Maximum observed frame plus decode materialization owned by the query | Observed at the bounded exchange lifecycle |

`PairedMaterializedReport::evaluate_query_scoped_overhead` evaluates the proxy observation against
an explicit `QueryScopedOverheadLimits`. It fails closed on the first unavailable metric and also
fails closed when an observed value exceeds its bound. The compatibility materialized runner leaves
counters unavailable. The observed runner consumes the query-scoped snapshot returned by the engine
and takes the maximum per-query value across samples. Both runners validate every sample digest,
not only the final result. External TTFR remains `UnavailableMaterializedApi`; total materialized
latency is not substituted for first-result time.

The deterministic memory fixture now uses the real engine snapshot and evaluates explicit absolute
budgets. A release benchmark must additionally certify exact external adapter transport calls and
collect real-backend latency, TTFR, throughput, and process-memory data. It must not use fixture
constants, process-wide counters, or a post-hoc estimate.

## 2026-07-25 Certification Addendum

Real PostgreSQL 17 and Neo4j 5.26 services exposed two backend-specific defects that mock or SQL
shape tests did not catch. Neo4j required `WITH instance` between the snapshot-fence `SET` and the
following `OPTIONAL MATCH`. PostgreSQL required explicit integer casts for `to_hex` over smallint
columns. Both fixes were rerun against their real services before the six-direction migration and
three-backend deployment certifications.

The exact-test workflow now invokes `scripts/run-exact-cargo-test.sh`. The wrapper lists the target
first, requires the requested test name to occur exactly once, and only then runs `--exact`; a
missing test exits non-zero instead of allowing Cargo's zero-test success behavior. Takeover tests
also share an observable fault flag and reject completion unless the requested fault fired.

Algorithm and fault-point filters are now validated as a pair. `begin` is the only resumability
fault supported by WCC and PageRank in this matrix; Degree additionally supports its nine execution
boundaries and same-Gateway restart. An incompatible pair such as WCC plus `publish` is rejected
instead of silently executing a different case, and every filtered run must execute at least one
matching scenario.

Sidecar and ordered full-stack restart tests now require the interrupted job to remain `RUNNING`
after the outage has lasted longer than the fixture's execution delay. This prevents a job that
completed before shutdown from satisfying the recovery assertion by merely returning an already
published Result Artifact.

The release threshold formulas are executable in `cypher-engine::system_performance`. A paired
observation must provide non-empty total-latency and TTFR samples, non-zero throughput and memory,
query-scoped overhead, payload bytes, exchange bytes, and an explicit node count. The gate applies
the documented point p50/p99, single/multi-shard throughput, first-result, direct-plus-five-percent
memory, frame-plus-payload network, and 4/8-node scale-out limits without floating-point rounding.
Missing query metrics still fail through the existing query-scoped overhead gate.

This calculation gate is not a claim that external TTFR or scale-out has been measured. The current
production path still materializes complete remote Shard scans, complete PhysicalPlan fragment
outputs, `CypherQueryResponse`, and Bolt cursor records. A valid external TTFR sample requires the
bounded morsel source to remain live from the remote Shard stream through `query-executor`, the
full distributed PhysicalPlan, `cypher-engine`, and Bolt `PULL`; internal single-fragment callback
time must not be substituted.

## Not Yet Measured

No wall-clock or throughput claim is made here. The repository now contains a deterministic paired
direct/proxy runner with materialized latency percentiles and semantic-result validation, but it is
not yet a release benchmark with warm and cold caches, external first-result time, peak process
memory, network bytes, or real PostgreSQL/Neo4j service execution. External TTFR is explicitly
`UnavailableMaterializedApi`; it must not be approximated with total response latency.

In particular, the three production adapters currently advertise CandidateScan as
`Unsupported`. They therefore use the generic canonical path and must not be counted as indexed
property-pushdown performance results.

## Release Thresholds

The availability gate now exists. A release run is blocked until each required query-scoped metric
is observed. Once the real-backend paired benchmark supplies those values, it must also fail when
any of these conditions holds:

| Metric | Blocking threshold |
|---|---|
| Point lookup p50 overhead | greater than `max(75 us, direct * 15%)` |
| Point lookup p99 overhead | greater than `max(300 us, direct * 15%)` |
| Single-shard scan throughput | proxy/direct below `0.90` |
| Multi-shard or change throughput | proxy/direct below `0.85` |
| First-result overhead | single-shard above `max(250 us, 15%)`; multi-shard above `max(1 ms, 20%)` |
| Peak memory | above query budget, or above direct baseline by `5%` |
| Extra exchange bytes | above `frame_count * 256 B + payload * 3%` |
| Variable-width preflight copies | non-zero |
| Cancellation after acknowledgement | any new adapter RPC, encoded frame, or sent frame |
| Compile and optimize count | more than once per statement; hot prepared execution must be zero |
| Partitioned scale-out | 4 nodes below `2.8x` single-node throughput, or 8 nodes below `5x` |

These are gates, not claims about current measurements. A benchmark report must record fixture hash,
backend version, deployment mode, Rust version, CPU, cache mode, sample count, command output, and
the exact revision used for both direct and proxy paths.

## Reproduction Commands

```text
CARGO_INCREMENTAL=0 cargo test --locked -p distributed-query --features test-support \
  --test performance_regressions -- --test-threads=1
CARGO_INCREMENTAL=0 cargo test --locked -p query-executor --test column_batch
CARGO_INCREMENTAL=0 cargo test --locked -p temporal-storage --test batched_materialization \
  -- --test-threads=1
CARGO_INCREMENTAL=0 cargo test --locked -p cypher-engine --test direct_proxy_gate \
  -- --test-threads=1
CARGO_INCREMENTAL=0 cargo test --locked -p gateway-node --features test-support \
  --test read_fanout -- --test-threads=1
```
