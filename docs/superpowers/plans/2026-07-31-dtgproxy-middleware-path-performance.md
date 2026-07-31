# DTGProxy Middleware Path Performance Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development
> (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce common DTGProxy Bolt/Gateway/Data fixed overhead with measured, independently
reversible optimizations while preserving all durability and temporal semantics.

**Architecture:** First make the quick four-process diagnostic publish complete three-repetition raw
samples and pooled summaries. Add bounded allocation-free stage histograms and periodic structured
process snapshots. Then measure `TCP_NODELAY` and a multi-thread Gateway runtime as separate stages;
use the stage evidence to decide whether a later cache or Meta timestamp-fusion plan is warranted.

**Tech Stack:** Rust 1.93, Tokio 1.53, Tonic 0.14, Bolt 5.4, fixed atomic histograms, Serde JSON,
four-process release diagnostics.

## Global Constraints

- The approved design is
  `docs/superpowers/specs/2026-07-31-dtgproxy-middleware-path-performance-design.md`.
- Preserve durability, Raft commit, snapshot consistency, transaction replay, temporal semantics,
  result ordering, and Bolt RUN/PULL behavior.
- Do not modify Fjall, PostgreSQL, or Neo4j internal implementation or data modeling.
- Do not add a benchmark-only production branch or record statements, parameters, credentials, or
  result contents in metrics.
- Run one backend at a time and do not run migration tests.
- Every retained optimization requires an isolated release before/after measurement with zero
  errors and identical result identity.
- Target every workload: concurrency-one p50 at least 30% lower, concurrency-eight throughput at
  least 25% higher, and p95/p99 no more than 10% worse.
- Every task follows red-green-refactor, commits only its task files, and leaves
  `.superpowers/sdd/progress.md` unstaged.

---

### Task 1: Publish Complete Quick-Diagnostic Raw Samples

**Files:**
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs`

**Interfaces:**
- Consumes: `RawObservation`, `Summary`, `summarize`, the selected-backend quick test.
- Produces: `QuickDiagnosticArtifact`, `write_quick_artifact`, and one immutable JSON artifact
  selected by `DTG_BACKEND_E2E_QUICK_OUTPUT`.

- [ ] **Step 1: Write failing artifact contract tests**

Add a synthetic three-repetition test requiring exactly 18 observations, all six
workload/concurrency groups, pooled latency samples, summed operations/duration, zero errors, and
refusal to overwrite an existing output. Require the serialized artifact to carry format version 1,
backend, revision, repetitions, raw observations, and six summaries.

```rust
#[test]
fn quick_artifact_requires_three_complete_repetitions_and_refuses_overwrite() {
    let observations = complete_quick_observations(Backend::Fjall, 3);
    let artifact = QuickDiagnosticArtifact::new("test-revision", observations).unwrap();
    assert_eq!(artifact.repetitions, 3);
    assert_eq!(artifact.observations.len(), 18);
    assert_eq!(artifact.summaries.len(), 6);
    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("quick.json");
    write_quick_artifact(&output, &artifact).unwrap();
    assert!(write_quick_artifact(&output, &artifact).is_err());
}
```

- [ ] **Step 2: Run RED**

Run:

```bash
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic quick_artifact_ -- --nocapture
```

Expected: FAIL because `QuickDiagnosticArtifact` and immutable publication do not exist.

- [ ] **Step 3: Implement the minimal artifact model**

Implement `QuickDiagnosticArtifact::new` with these exact validations:

- one backend;
- repetitions are exactly `0..repetitions` with no duplicate cell;
- three workloads and concurrency 1/8 exist in every repetition;
- every observation has zero errors and non-empty latency samples;
- read result digest/row count is stable within a group;
- writes acknowledge zero rows;
- pooled throughput is total operations divided by total measured duration;
- p50/p95/p99 use nearest-rank over the pooled raw samples.

Publish with `create_new(true)`, write JSON, `sync_all`, and sync the parent directory. Reject a
relative output path and an existing file.

- [ ] **Step 4: Make the live quick test run the exact requested repetitions**

Require:

```text
DTG_BACKEND_E2E_QUICK_REPETITIONS=3
DTG_BACKEND_E2E_QUICK_OUTPUT=/absolute/new/file.json
```

When the output variable is present, require repetitions to equal 3. Set `CellSpec.repetition` to
0, 1, and 2, collect all observations, and publish only after every cluster shut down successfully.
Keep the one-repetition stdout mode for a developer smoke run when no output path is supplied.

- [ ] **Step 5: Run GREEN**

```bash
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
cargo clippy --locked -p dtg-gateway --test backend_e2e_diagnostic -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 6: Commit**

```bash
git add crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs
git commit -m "test(perf): publish pooled quick diagnostics"
```

---

### Task 2: Add Bounded Request-Stage Histograms

**Files:**
- Create: `crates/execution/dtg-execution/src/request_metrics.rs`
- Modify: `crates/execution/dtg-execution/src/lib.rs`
- Modify: `crates/execution/dtg-execution/src/gateway.rs`
- Modify: `crates/processes/dtg-gateway/src/service.rs`
- Modify: `crates/processes/dtg-gateway/src/bolt.rs`
- Modify: `crates/processes/dtg-data/src/service.rs`
- Test: `crates/execution/dtg-execution/src/request_metrics.rs`
- Test: `crates/execution/dtg-execution/tests/gateway_process.rs`
- Test: `crates/processes/dtg-data/tests/process.rs`

**Interfaces:**
- Produces: `RequestStage`, `StageOutcome`, `RequestStageMetrics`, `StageTimer`,
  `RequestStageSnapshot`, and `RequestMetricsSnapshot`.
- Consumes: monotonic `Instant` durations at existing Gateway/Data boundaries.

- [ ] **Step 1: Write failing histogram tests**

Require 64 logarithmic nanosecond buckets, exact success/error/cancelled counts, saturating totals,
concurrent relaxed-atomic recording, and a dropped unfinished timer counted as cancelled.

```rust
#[test]
fn dropped_stage_timer_records_cancellation_once() {
    let metrics = Arc::new(RequestStageMetrics::default());
    drop(metrics.start(RequestStage::GatewayCompile));
    let stage = metrics.snapshot().stage(RequestStage::GatewayCompile);
    assert_eq!(stage.cancelled, 1);
    assert_eq!(stage.success, 0);
    assert_eq!(stage.error, 0);
}
```

- [ ] **Step 2: Run RED**

```bash
cargo test --locked -p dtg-execution request_metrics -- --nocapture
```

Expected: FAIL because the metrics module is absent.

- [ ] **Step 3: Implement the fixed metrics primitive**

Use this exact stage enum:

```rust
pub enum RequestStage {
    BoltDecode,
    GatewayCompile,
    GatewayPlan,
    GatewayInternalRpc,
    GatewayLocalExecution,
    BoltEncode,
    DataValidation,
    DataRouting,
    DataRaftApply,
    DataProviderExecution,
}
```

Each stage owns `[AtomicU64; 64]`, success/error/cancelled `AtomicU64`, and total/max nanoseconds.
Select bucket `min(63, 63 - nanos.max(1).leading_zeros() as usize)`. Use a CAS loop for saturating
addition. `StageTimer::finish(outcome)` records exactly once; `Drop` records cancellation when not
finished. Snapshot values are plain serializable integers and arrays/vectors.

- [ ] **Step 4: Instrument non-overlapping Gateway stages**

Attach one `Arc<RequestStageMetrics>` to `GatewayExecution` and expose a clone through
`GatewayService::request_metrics`. Time:

- PackStream decode in `serve_connection`;
- `GatewayExecution::compile`;
- planner execution and parameter lowering;
- the complete transport await;
- materialized local query execution;
- PackStream success/record/failure encoding.

Finish guards with `Success` or `Error`; cancellation/drop uses the guard default. Do not time
socket idle time between RUN and PULL as Gateway execution.

- [ ] **Step 5: Instrument Data stages**

Add `Arc<RequestStageMetrics>` to `DataRpcService`, surfaced through the existing service/node
metrics access pattern. Time validation/decode, `locate_replica`, `apply_transaction_command`, and
fragment Provider execution. Preserve existing `DataMetrics` counters.

- [ ] **Step 6: Verify instrumentation behavior**

Add focused fake-transport tests proving one successful process point query records compile, plan,
internal RPC, and local execution once; an invalid query records compile error; a cancelled future
increments cancelled. Add Data tests for successful and rejected requests.

Run:

```bash
cargo test --locked -p dtg-execution request_metrics -- --nocapture
cargo test --locked -p dtg-execution --test gateway_process -- --test-threads=1
cargo test --locked -p dtg-data --test process -- --test-threads=1
cargo clippy --locked -p dtg-execution -p dtg-data -p dtg-gateway --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add crates/execution/dtg-execution/src/request_metrics.rs \
  crates/execution/dtg-execution/src/lib.rs \
  crates/execution/dtg-execution/src/gateway.rs \
  crates/execution/dtg-execution/tests/gateway_process.rs \
  crates/processes/dtg-gateway/src/service.rs \
  crates/processes/dtg-gateway/src/bolt.rs \
  crates/processes/dtg-data/src/service.rs \
  crates/processes/dtg-data/tests/process.rs
git commit -m "feat(metrics): record middleware request stages"
```

---

### Task 3: Export Periodic Internal Metric Snapshots to Diagnostics

**Files:**
- Modify: `crates/processes/dtg-gateway/src/main.rs`
- Modify: `crates/processes/dtg-data/src/main.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/cluster.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`

**Interfaces:**
- Produces structured stderr lines `DTG_REQUEST_STAGE_METRICS=<json>` once per second.
- Extends `RawObservation` with the nearest bracketing Gateway/Data metrics snapshots and deltas.

- [ ] **Step 1: Write failing snapshot/log parsing tests**

Add tests requiring schema version 1, process role, Unix timestamp, monotonic sequence, and complete
stage snapshot. Reject duplicate sequence, role mismatch, snapshots outside the cell interval, and
counter regression. Compute delta only from one snapshot at or before measurement start and one at
or after measurement finish.

- [ ] **Step 2: Run RED**

```bash
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic stage_metrics_ -- --nocapture
```

Expected: FAIL because structured process snapshots and artifact deltas are absent.

- [ ] **Step 3: Add the permanent periodic exporter**

In Gateway and Data mains, spawn a one-second `tokio::time::interval` that writes one compact JSON
line to stderr. The exporter is always active, reports cumulative counters only, and never includes
request content. Use `MissedTickBehavior::Skip`; serialization or stderr failure must not affect
request execution. Stop the exporter when the process shutdown branch completes.

- [ ] **Step 4: Capture bracketing snapshots without extending measured duration**

Keep process log paths available after child retirement. During the one-second warmup there must be
a pre-measurement snapshot; after measurement, wait no more than 1,250 ms for a post-measurement
snapshot before shutting down. Parse only exact prefixed JSON lines. Store snapshot deltas beside
the raw end-to-end observation. A missing or invalid bracket rejects the diagnostic cell.

- [ ] **Step 5: Run GREEN**

```bash
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
cargo test --locked -p dtg-data --test process -- --test-threads=1
cargo clippy --locked -p dtg-data -p dtg-gateway --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 6: Commit the instrumentation**

```bash
git add crates/processes/dtg-gateway/src/main.rs \
  crates/processes/dtg-data/src/main.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_support/cluster.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs
git commit -m "feat(metrics): export middleware stage snapshots"
```

- [ ] **Step 7: Capture the immutable pre-optimization baseline**

Build release binaries and run exactly:

```bash
cargo build --release --locked -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway
DTG_BACKEND_E2E_BIN_DIR="$PWD/target/release" \
DTG_BACKEND_E2E_SELECTED_BACKEND=fjall \
DTG_BACKEND_E2E_QUICK_REPETITIONS=3 \
DTG_BACKEND_E2E_QUICK_OUTPUT="$PWD/artifacts/backend-e2e-diagnostic/middleware-before.json" \
cargo test --release --locked -p dtg-gateway --test backend_e2e_diagnostic \
  quick_selected_backend_e2e_comparison -- --ignored --exact --nocapture
```

The output path must be new, contain 18 raw observations, six pooled summaries, zero errors, and
valid bracketing metrics. This instrumentation-only committed revision is the comparison baseline
for later stages; retain the earlier uninstrumented log as overhead control. The artifact remains an
immutable local measurement output and is referenced by digest from the final committed report.

---

### Task 4: Eliminate Bolt Small-Message Delay

**Files:**
- Modify: `crates/processes/dtg-gateway/src/bolt.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/bolt.rs`
- Test: `crates/processes/dtg-gateway/src/bolt.rs`
- Test: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`

**Interfaces:**
- Consumes: accepted and connected `tokio::net::TcpStream` values.
- Produces: `configure_bolt_socket(&TcpStream) -> io::Result<()>`, applied before any Bolt bytes.

- [ ] **Step 1: Write failing socket-option tests**

Create a real loopback pair, assert the helper sets `nodelay() == true`, and assert the server calls
the helper before negotiation by using a test socket wrapper/hook that records ordering. Extend the persistent
diagnostic-client test to assert its connected socket has `TCP_NODELAY` enabled.

- [ ] **Step 2: Run RED**

```bash
cargo test --locked -p dtg-gateway bolt_socket -- --nocapture
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic bolt_session_ -- --nocapture
```

Expected: FAIL because neither side configures the option.

- [ ] **Step 3: Implement the minimal transport change**

Call `socket.set_nodelay(true)?` immediately after Gateway `accept` and immediately after diagnostic
client `connect`, before handshake or task dispatch. Do not change chunking, flush behavior, message
ordering, or PackStream encoding.

- [ ] **Step 4: Verify correctness**

```bash
cargo test --locked -p dtg-gateway --lib
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
cargo test --locked -p dtg-gateway --test live_certification \
  four_process_functional_probes_and_real_bolt_query -- --exact --nocapture
```

- [ ] **Step 5: Commit the candidate**

```bash
git add crates/processes/dtg-gateway/src/bolt.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_support/bolt.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs
git commit -m "perf(bolt): disable small-message coalescing"
```

- [ ] **Step 6: Capture the isolated after artifact**

Use the Task 3 command with the new absolute output
`artifacts/backend-e2e-diagnostic/middleware-after-nodelay.json`. Compare it against
`middleware-before.json`; require zero errors, identical result identity, and report the exact ratio
for all six groups. Retain the change only when it improves at least one primary target and does not
violate any tail-latency gate. If it fails that gate, create a non-interactive revert commit for the
candidate and preserve the rejected artifact for the audit report.

---

### Task 5: Allow Independent Bolt Sessions to Execute Concurrently

**Files:**
- Create: `crates/processes/dtg-gateway/src/runtime.rs`
- Modify: `crates/processes/dtg-gateway/src/lib.rs`
- Modify: `crates/processes/dtg-gateway/src/main.rs`
- Test: `crates/processes/dtg-gateway/src/runtime.rs`

**Interfaces:**
- Produces: `build_gateway_runtime() -> io::Result<tokio::runtime::Runtime>` with four workers.
- Preserves: one sequential owner task per Bolt connection.

- [ ] **Step 1: Write a failing process concurrency test**

Add a runtime unit test that starts two tasks on the returned runtime. A barrier releases both tasks,
each performs 100 ms of synchronous work, and total wall time must be below 175 ms. Also assert the
runtime handle reports `RuntimeFlavor::MultiThread`. A separate existing Bolt integration test
continues to prove one connection preserves request order.

- [ ] **Step 2: Run RED**

```bash
cargo test --locked -p dtg-gateway runtime::tests::gateway_runtime_runs_blocking_session_tasks_in_parallel \
  -- --exact --nocapture
```

Expected: FAIL because `build_gateway_runtime` and the runtime module do not exist.

- [ ] **Step 3: Change only the runtime topology**

Build the runtime with:

```rust
tokio::runtime::Builder::new_multi_thread()
    .worker_threads(4)
    .enable_all()
    .thread_name("dtg-gateway")
    .build()
```

Make `main` synchronous, build the runtime once, and `block_on` the existing async startup/service
body. Do not introduce per-request threads, change socket ownership, or widen existing locks.

- [ ] **Step 4: Run GREEN and regressions**

```bash
cargo test --locked -p dtg-gateway runtime::tests::gateway_runtime_runs_blocking_session_tasks_in_parallel \
  -- --exact --nocapture
cargo test --locked -p dtg-gateway --test live_certification \
  four_process_functional_probes_and_real_bolt_query -- --exact --nocapture
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
cargo test --locked -p dtg-execution --test gateway_process -- --test-threads=1
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 5: Commit the candidate**

```bash
git add crates/processes/dtg-gateway/src/runtime.rs \
  crates/processes/dtg-gateway/src/lib.rs \
  crates/processes/dtg-gateway/src/main.rs
git commit -m "perf(gateway): run Bolt sessions concurrently"
```

- [ ] **Step 6: Capture the cumulative and isolated effect**

Write `artifacts/backend-e2e-diagnostic/middleware-after-gateway-multithread.json`. Compare against
both the baseline and nodelay artifact. Require concurrency-eight throughput improvement without
violating the tail gate. The stage metric report must show whether queueing or Gateway-local work
changed. If it fails that gate, create a non-interactive revert commit for the candidate and preserve
the rejected artifact for the audit report.

---

### Task 6: Evaluate Evidence Gates and Publish the First Optimization Report

**Files:**
- Create: `docs/audit/performance/2026-07-31-middleware-path-optimization.md`
- Modify only if the evidence threshold triggers a follow-up: this plan file with fully specified
  cache or timestamp-fusion tasks in a later commit.

**Interfaces:**
- Consumes: before, nodelay, and multi-thread immutable artifacts.
- Produces: exact stage attribution, retained/rejected decisions, acceptance results, and the next
  evidence-gated task decision.

- [ ] **Step 1: Verify and compare artifacts**

Recompute each artifact's observation completeness and summaries from raw samples. Reject changed,
missing, non-finite, error-bearing, or identity-mismatched input. Report throughput and p50/p95/p99
absolute values, ratios, and percent changes for all six groups.

- [ ] **Step 2: Apply the cache gate**

Pool `GatewayCompile + GatewayPlan` stage time by workload. The cache gate triggers only if it is at
least 10% of p50 or two milliseconds. If it does not trigger, record `cache: skipped` with the exact
measurements. If it triggers, stop implementation and amend this plan with a separate bounded-cache
task before writing cache code.

- [ ] **Step 3: Apply the write-fusion gate**

If pooled CREATE p50 is above its 30% reduction target, report the time in Gateway internal RPC,
Data Raft apply, and Provider execution. Timestamp fusion triggers only when the two pre-Data Meta
RPCs are a measured dominant component. If it does not trigger, record `timestamp fusion: skipped`.
If it triggers, stop implementation and amend this plan with exact protocol/Meta tests before
changing the protocol.

- [ ] **Step 4: Write and verify the report**

Include host/revision, commands, raw artifact paths, three-repetition method, stage deltas, every
retained and rejected change, correctness gates, achieved ratios, unmet targets, and limitations.
Explicitly distinguish measured speedups from the requested aspirational tens/hundreds-of-times
goal.

```bash
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
cargo test --locked -p dtg-data --test process -- --test-threads=1
cargo test --locked -p dtg-execution --test gateway_process -- --test-threads=1
cargo clippy --locked --workspace --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 5: Commit**

```bash
git add docs/audit/performance/2026-07-31-middleware-path-optimization.md
git commit -m "docs: report middleware path optimization"
```

---

### Task 7: Reuse Exact Immutable Data Read Views

**Evidence:** On the real four-process Fjall point path, compile and planning are below 0.05 ms per
request while `DataProviderExecution` is about 37.5 ms. `FjallReadView::load` rebuilds an immutable
snapshot by decoding the complete history and temporal-change index for every fragment. The fix
belongs in the middleware lifecycle, not in a provider's data model.

**Files:**
- Modify: `crates/execution/dtg-execution/src/data.rs`
- Test: `crates/execution/dtg-execution/tests/facades.rs`

**Interface:** `DataExecution` owns at most one cached `Arc<dyn TemporalReadView>` per local
`ReplicaKey`. A hit requires the complete `ReadFence` to compare equal, including binding, applied
index, and capability digest.

- [ ] **Step 1: Write failing behavior tests**

Use a counting `ReplicaStateStore` through the real `DataExecution::execute_fragment` path. Require
two reads at the same fence to call `begin_read_view` once; a higher applied index to open and
publish a new view; a removed/re-added replica not to inherit the old view; and a provider-returned
view with a different fence to fail closed. Confirm the RED failure is the repeated construction,
not fixture setup.

- [ ] **Step 2: Implement the bounded exact-fence cache**

Look up the cache under a short mutex, release it before `begin_read_view().await`, validate that the
returned view carries the requested fence, convert it to `Arc`, then recheck before publication so
concurrent cold misses reuse an already-published exact view. Keep at most one entry per replica.
For one unchanged binding, an older request completing late must not displace a higher applied-index
entry. Verify that the runtime store is still the registered store before publishing a newly opened
view. Clear the entry after successful replica addition and on replica removal. Never hold the
store, ShardHost, or read-view cache mutex across an await.

- [ ] **Step 3: Verify correctness and isolation**

```bash
cargo test --locked -p dtg-execution --test facades read_view_cache_ -- --nocapture
cargo test --locked -p dtg-execution --test facades -- --test-threads=1
cargo test --locked -p dtg-data --test process -- --test-threads=1
cargo clippy --locked -p dtg-execution -p dtg-data --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
```

- [ ] **Step 4: Commit and measure**

Commit only the two task files. Build the four release processes, then produce a new three-repetition
Fjall artifact with a new absolute output path. Require 18 observations, six summaries, zero errors,
stable read identities, and valid metric brackets. Compare it to the post-Bolt/runtime artifact.
Only after Fjall is complete, start PostgreSQL and Neo4j one at a time for the same quick diagnostic;
do not run migration tests.
