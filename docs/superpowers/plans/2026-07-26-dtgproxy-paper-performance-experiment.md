# DTGProxy Paper Performance Experiment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to
> implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove client-side measurement pollution, optimize real 1/4/8-node execution, implement
Backend Direct/Adapter Direct/Proxy and ablation comparisons, and produce one statistically valid
reproducible paper artifact.

**Architecture:** Reuse the existing Bolt protocol implementation through persistent negotiated
sessions, add a dedicated paper benchmark crate for manifests, statistics, and path runners, and
keep benchmark-only ablations below the language and Storage SPI boundaries. The final orchestrator
uses the existing real process topology harness and writes immutable identity-checked observations.

**Tech Stack:** Rust 1.93, Tokio, Bolt 5.8, serde/serde_json, BLAKE3, RocksDB, PostgreSQL, Neo4j,
tonic, shell orchestration, jq.

## Global Constraints

- Language, temporal semantics, and Storage SPI remain backend-independent.
- A data node is an independent `dtgproxy-data` operating-system process.
- Backend Direct, Adapter Direct, and Proxy comparisons require equal digest and row count.
- Formal runs use 1/4/8 nodes, concurrency 1/8/32/64, 30-second warmup, 60-second measurement,
  and five repetitions.
- Development uses short focused diagnostics; the complete formal matrix runs once.
- Benchmark ablations are not public T-Cypher syntax or Adapter SPI capabilities.
- Do not stage, commit, push, revert unrelated changes, or modify `liblib.rlib`.

---

### Task 1: Persistent Bolt Session

**Files:**
- Modify: `crates/bolt-server/src/probe.rs`
- Modify: `crates/bolt-server/src/lib.rs`
- Modify: `crates/bolt-server/tests/external_ttfr.rs`

**Interfaces:**
- Produces: `BoltProbeSession::connect(config).await` and
  `BoltProbeSession::execute(query, parameters).await`.
- Preserves: `probe_external_ttfr` and the existing one-shot CLI behavior.

- [ ] **Step 1: Add a failing persistent-session test**

Add a loopback Bolt server that accepts one TCP connection and serves three RUN/PULL cycles. The
test calls `BoltProbeSession::connect` once, executes three samples, asserts one accepted connection,
three equal result identities, and TTFR measured from each RUN rather than from session creation.

- [ ] **Step 2: Verify RED**

Run:

```text
cargo test -p bolt-server --test external_ttfr persistent_session_reuses_one_negotiated_connection -- --exact
```

Expected: compile failure because `BoltProbeSession` does not exist.

- [ ] **Step 3: Implement the session state machine**

Move `TcpStream` and `Receiver` into a public session. `connect` performs connect, version
negotiation, and HELLO under the configured timeout. `execute` frames RUN/PULL, starts the clock
immediately before RUN, consumes through summary SUCCESS, and returns `ExternalTtfrSample`.
On protocol or timeout failure the session is unusable and the error is returned without retry.

- [ ] **Step 4: Reimplement one-shot probing through sessions**

For each one-shot warmup/sample, connect a new session, execute once, send GOODBYE, and preserve all
existing empty-result, failure, timeout, and identity checks.

- [ ] **Step 5: Run the focused target**

```text
cargo test -p bolt-server --test external_ttfr
```

Expected: all existing and persistent-session tests pass.

---

### Task 2: Persistent External Load Generator

**Files:**
- Create: `crates/bolt-server/src/loadgen.rs`
- Create: `crates/bolt-server/src/bin/dtgproxy-bolt-loadgen.rs`
- Create: `crates/bolt-server/tests/external_loadgen.rs`
- Modify: `crates/bolt-server/src/lib.rs`
- Modify: `crates/bolt-server/Cargo.toml`

**Interfaces:**
- Consumes: address, query, connection count, warmup duration, measurement duration, timeout.
- Produces: versioned JSON with operation samples, percentiles inputs, throughput, connection setup,
  errors, digest, and row count.

- [ ] **Step 1: Add failing bounded-load tests**

Use a real loopback Bolt listener. Assert two connections are accepted, both are reused, warmup
operations are excluded from measured count, every measured sample has the same identity, and one
mismatching server response invalidates the report.

- [ ] **Step 2: Verify RED**

```text
cargo test -p bolt-server --test external_loadgen
```

Expected: compile failure because loadgen APIs and binary do not exist.

- [ ] **Step 3: Implement load generation**

Create one Tokio task per connection. Use a barrier before measured execution, an absolute stop
instant, and per-operation timeout. Store integer nanosecond samples and merge only after all tasks
finish. Reject zero completed operations, any error, or identity drift.

- [ ] **Step 4: Implement machine-readable CLI output**

The CLI accepts the exact arguments from the design and writes JSON atomically to `--output`. JSON
contains schema version, timing definition, connection count, warmup/measured durations, completed
operations, throughput, TTFR samples, latency samples, connection setup samples, digest, row count,
and error count.

- [ ] **Step 5: Verify focused tests**

```text
cargo test -p bolt-server --test external_loadgen --test external_ttfr
```

Expected: all tests pass with no additional TCP connections per operation.

---

### Task 3: Real Scale-Out Driver And Bottleneck Evidence

**Files:**
- Modify: `crates/gateway-node/tests/scale_out_process.rs`
- Modify: `scripts/certify-scale-out.sh`
- Modify: `.github/workflows/quality-gates.yml`
- Create: `docs/audit/performance/2026-07-26-scale-out-diagnostic.md`

**Interfaces:**
- Consumes: `dtgproxy-bolt-loadgen` rather than one probe process per operation.
- Produces: 1/4/8 reports with identical result identity and diagnostic counters.

- [ ] **Step 1: Add a failing driver-authenticity test**

Add a test fixture whose fake loadgen increments a launch counter. Run a short workload and assert
the loadgen is launched once per topology, not once per query, and that its JSON is propagated into
the topology report.

- [ ] **Step 2: Verify RED**

```text
cargo test -p gateway-node --test scale_out_process persistent_driver_is_launched_once_per_topology -- --exact
```

Expected: failure because the harness still launches `dtgproxy-bolt-probe` per operation.

- [ ] **Step 3: Replace the workload loop**

Pass Bolt address, concurrency, warmup, duration, timeout, and report path to
`dtgproxy-bolt-loadgen`. Remove thread spawning and per-operation `Command::output`. Keep child
process lifecycle, topology authenticity, dataset seeding, RSS, network, digest, and row-count gates.

- [ ] **Step 4: Add query diagnostics**

Extend the benchmark report boundary with available query-scoped counters: Adapter calls, encoded
and decoded exchange bytes, sent frames, copied bytes, and peak retained bytes. Missing counters
remain explicit `Unavailable`; do not encode them as zero.

- [ ] **Step 5: Run one short diagnostic matrix**

Run 1/4/8 nodes for 3 seconds at concurrency 8. Record throughput ratios and the largest serialized
or duplicated cost in the diagnostic document. This is not the formal experiment.

- [ ] **Step 6: Fix the measured data-plane bottleneck**

Use the diagnostic evidence to select one implementation boundary. Candidate fixes include bounded
parallel fanout, avoiding repeated compile/catalog work, and removing duplicated exchange
materialization. Add a focused regression test that fails before the selected fix and passes after.

- [ ] **Step 7: Re-run only the short diagnostic**

Require identity equality and improved 4/8-node ratios. Do not run the final 30/60-second matrix.

---

### Task 4: Paper Benchmark Model And Three Comparison Paths

**Files:**
- Create: `crates/paper-benchmark/Cargo.toml`
- Create: `crates/paper-benchmark/src/lib.rs`
- Create: `crates/paper-benchmark/src/manifest.rs`
- Create: `crates/paper-benchmark/src/identity.rs`
- Create: `crates/paper-benchmark/src/statistics.rs`
- Create: `crates/paper-benchmark/src/path.rs`
- Create: `crates/paper-benchmark/tests/report_contract.rs`
- Modify: `Cargo.toml`

**Interfaces:**
- Produces: versioned dataset/workload/run manifests, `ExperimentPath`, raw observation schema,
  identity validation, percentile/statistics summarization.

- [ ] **Step 1: Write failing report-contract tests**

Tests reject missing path, backend, repetition, timing boundaries, identity, metrics, configuration
digest, and zero operations. Tests also reject identity differences between compared paths.

- [ ] **Step 2: Verify RED**

```text
cargo test -p paper-benchmark --test report_contract
```

Expected: package or API missing.

- [ ] **Step 3: Implement manifests and raw observations**

Use serde structs with `schema_version: 1`. `ExperimentPath` has exactly `backend_direct`,
`adapter_direct`, and `proxy`. Every observation contains backend, workload, topology, concurrency,
ablation, repetition, operations, samples, resource metrics, digest, and row count.

- [ ] **Step 4: Implement deterministic statistics**

Sort integer samples for percentiles. Compute mean, median, sample standard deviation, and a 95%
Student-t confidence interval over the five repetition-level values. Reject fewer than five formal
repetitions and all non-finite values.

- [ ] **Step 5: Implement path runner contracts**

Define backend-neutral request/result types for benchmark workloads. Backend Direct runners use
backend-native APIs, Adapter Direct runners use typed `ReadSnapshot` primitives, and Proxy consumes
loadgen JSON. Unsupported native equivalence returns an explicit unavailable reason.

- [ ] **Step 6: Verify report tests**

```text
cargo test -p paper-benchmark --test report_contract
```

Expected: all schema, identity, and statistics tests pass.

---

### Task 5: Benchmark-Only Ablations

**Files:**
- Create: `crates/paper-benchmark/src/ablation.rs`
- Modify: `crates/cypher-engine/src/lib.rs`
- Modify: `crates/query-executor/src/context.rs`
- Modify: `crates/distributed-query/src/coordinator.rs`
- Modify: `crates/temporal-storage/src/store.rs`
- Create: `crates/paper-benchmark/tests/ablation_identity.rs`

**Interfaces:**
- Produces: `BenchmarkAblationConfig` carried through internal execution context only.
- Preserves: public T-Cypher grammar, temporal semantics, and Storage SPI types.

- [ ] **Step 1: Add failing identity tests for every ablation**

Execute one deterministic fixture in production mode and each single-disabled mode. Assert schema,
digest, and row count equality while counters prove the requested optimization was actually disabled.

- [ ] **Step 2: Verify RED**

```text
cargo test -p paper-benchmark --test ablation_identity
```

Expected: missing ablation configuration and counters.

- [ ] **Step 3: Implement internal ablation configuration**

Add explicit booleans for native pushdown, column batches, bounded lazy pages, parallel shard
fanout, and batched property gather. The default is all enabled. Constructors used outside the
benchmark crate continue to produce the production default.

- [ ] **Step 4: Implement one true alternate path per switch**

Each disabled mode must execute a real semantically equivalent alternate path. A flag that only
changes a label is invalid. Add counters that demonstrate which path ran.

- [ ] **Step 5: Verify identity and clean-break boundaries**

```text
cargo test -p paper-benchmark --test ablation_identity
scripts/check-tcypher-clean-break.sh
```

Expected: all identities match and no benchmark type leaks into language or Adapter SPI crates.

---

### Task 6: Reproducible Experiment Orchestrator

**Files:**
- Create: `crates/paper-benchmark/src/bin/dtgproxy-paper-benchmark.rs`
- Create: `scripts/run-paper-performance.sh`
- Create: `scripts/verify-paper-performance.sh`
- Create: `scripts/tests/paper-performance-fixtures.sh`
- Create: `docs/paper-performance-artifact.md`

**Interfaces:**
- Produces: `artifacts/paper-performance/<run-id>/` with manifest, configs, raw, summary, figures,
  logs, and SHA256SUMS.

- [ ] **Step 1: Add failing artifact fixture tests**

Fixtures reject incomplete matrices, duplicate cells, missing repetitions, unequal identities,
missing metrics, modified raw files, and summaries not reproducible from raw JSON.

- [ ] **Step 2: Implement deterministic dataset/workload manifests**

Record the 1M/5M/10% dataset parameters and seed. Dataset generation is resumable for preparation,
but the completed manifest includes exact counts and a digest before experiments begin.

- [ ] **Step 3: Implement matrix orchestration**

Generate a deterministic shuffled order across backend, path, workload, node count, concurrency,
ablation, and repetition. Formal defaults are fixed and changing them marks the run non-formal.
Never retry a failed formal cell automatically.

- [ ] **Step 4: Implement immutable artifact writing**

Write each cell to a unique temporary file, fsync, rename to its final path, and refuse overwrite.
Generate summaries only after the complete raw matrix passes identity and completeness validation.

- [ ] **Step 5: Implement verification and figure regeneration**

`verify-paper-performance.sh` verifies checksums, schema, completeness, identities, and statistics,
then regenerates CSV and figures without starting DTGProxy or any backend.

- [ ] **Step 6: Run short fixture certification**

Use tiny generated fixtures and 1-second measurements to prove orchestration. This is not the formal
experiment and is written outside the final artifact path.

- [ ] **Step 7: Run the complete formal experiment exactly once**

Prepare disposable PostgreSQL and Neo4j services, build release binaries, record environment and
revision, then run the fixed 30/60-second, five-repetition matrix. Do not rerun failed cells.

- [ ] **Step 8: Verify and freeze evidence**

Run checksum and regeneration verification, update the paper performance audit with exact artifact
path and headline measurements, and only then evaluate the Goal completion audit.
