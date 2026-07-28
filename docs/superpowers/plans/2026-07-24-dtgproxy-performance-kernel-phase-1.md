# DTGProxy Performance Kernel Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove fixed data-plane overhead that makes a query materialize, copy, and serialize complete results before downstream work can progress.

**Architecture:** Keep T-Cypher, canonical pages, SnapshotToken, and Adapter SPI unchanged. First replace owned exchange-cell access with borrowed views; then change fragments from whole-response `Vec<WorkerBatch>` values to bounded concurrent credit-accounted morsel sources. In parallel, batch canonical fallback reads by shard/page and make gateway fanout concurrent.

**Tech Stack:** Rust 2024, Tokio 1.53, canonical exchange codec, `ColumnBatch`, `StorageAdapter`, existing integration tests.

## Global Constraints

- `ColumnBatch` remains internal to `query-executor` and `distributed-query`; it must not enter `storage-api`, adapters, AST, IR, or physical-plan contracts.
- Adapter input/output remains typed bounded canonical primitives/pages. No raw T-Cypher, backend-private layout, or unchecked page crosses the SPI.
- Preserve snapshot, fencing, capability generation, schema, sequence, checksum, deadline, cancellation, and memory budget validation.
- `Candidate` always retains residual processing. Do not remove residuals for `Exact` in this phase.
- Every retained frame/batch reserves budget first and releases it on consumption, error, cancellation, and drop.
- Write and observe a failing test before production code. Never stage unrelated worktree changes.

---

## File Map

- `crates/query-executor/src/column_batch.rs`: borrowed typed column cell access.
- `crates/distributed-query/src/exchange_codec.rs`: borrowed preflight and encoding over existing column buffers.
- `crates/distributed-query/src/{worker.rs,coordinator.rs}`: morsel producer, concurrent fan-in, credit lifetime.
- `crates/temporal-storage/src/store.rs` and `crates/query-executor/src/temporal.rs`: canonical fallback key batching.
- `crates/gateway-node/src/service.rs`: bounded concurrent shard fanout.
- `crates/*/tests`: correctness plus deterministic counters. Wall-clock benchmarking is evidence, not a flaky unit test.

## Task 1: Add Deterministic Data-Plane Metrics

**Files:**
- Create: `crates/distributed-query/tests/performance_regressions.rs`
- Modify: `crates/distributed-query/src/lib.rs`

**Interfaces:**
- Produces test-only `ExchangeTestMetrics { encoded_frames, decoded_frames, retained_frame_bytes, retained_decoded_bytes }`.
- Produces an RAII installation guard so tests cannot leak global instrumentation.

- [ ] **Step 1: Write passing metrics-lifecycle tests**

```rust
#[tokio::test]
fn metrics_guard_restores_the_prior_test_sink_on_drop() {
    let metrics = install_exchange_test_metrics();
    metrics.reserve_frame(8);
    assert_eq!(metrics.retained_frame_bytes(), 8);
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p distributed-query --test performance_regressions --features test-support -- --test-threads=1`

Expected: compile failure because the opt-in test-support metrics and installation guard do not exist yet.

- [ ] **Step 3: Add only test-scoped atomic counters**

```rust
#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
pub(crate) struct ExchangeTestMetrics {
    encoded_frames: AtomicU64,
    decoded_frames: AtomicU64,
    retained_frame_bytes: AtomicU64,
    retained_decoded_bytes: AtomicU64,
}

#[cfg(any(test, feature = "test-support"))]
impl ExchangeTestMetrics {
    pub(crate) fn reserve_frame(&self, bytes: u64) {
        self.retained_frame_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(crate) fn release_frame(&self, bytes: u64) {
        self.retained_frame_bytes.fetch_sub(bytes, Ordering::Relaxed);
    }
}
```

Do not add production global state or timing assertions. The first-morsel and
credit-release behavior tests belong to Task 4, where the morsel API exists.

- [ ] **Step 4: Commit**

```bash
git add crates/distributed-query/src/lib.rs crates/distributed-query/tests/performance_regressions.rs
git commit -m "test(query): add exchange performance regression counters"
```

## Task 2: Borrow Exchange Values Instead of Cloning Them

**Files:**
- Modify: `crates/query-executor/src/column_batch.rs:15-300`
- Modify: `crates/distributed-query/src/exchange_codec.rs:441-655`
- Modify: `crates/query-executor/tests/column_batch.rs`
- Modify: `crates/distributed-query/tests/exchange_codec.rs`

**Interfaces:**
- Produces `pub enum ColumnValueRef<'a> { Null, Boolean(bool), Integer(i64), FloatBits(u64), TimestampMicros(i64), Utf8(&'a str), Bytes(&'a [u8]), Boundary(&'a RuntimeValue) }`.
- Produces `ColumnBatch::value_ref(column, row) -> Option<ColumnValueRef<'_>>`.
- Retains owned `ColumnBatch::value` only as a compatibility conversion.

- [ ] **Step 1: Write failing borrowed-view tests**

```rust
#[test]
fn value_ref_exposes_variable_values_without_owned_copies() {
    let batch = batch_with_string_and_bytes("large payload", b"binary payload");
    assert!(matches!(batch.value_ref(0, 0), Some(ColumnValueRef::Utf8("large payload"))));
    assert!(matches!(batch.value_ref(1, 0), Some(ColumnValueRef::Bytes(b"binary payload"))));
}

#[test]
fn value_ref_uses_validity_before_hidden_payload() {
    assert!(matches!(nullable_string_batch().value_ref(0, 1), Some(ColumnValueRef::Null)));
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p query-executor --test column_batch value_ref -- --exact`

Expected: compile failure because `ColumnValueRef` and `value_ref` do not exist.

- [ ] **Step 3: Implement the view and refactor both codec passes**

```rust
pub fn value_ref(&self, column: usize, row: usize) -> Option<ColumnValueRef<'_>> {
    (row < self.row_count).then(|| self.columns.get(column)?.value_ref(row))?
}
```

Match `ColumnValueRef` in `encoded_columns_len` and `encode_columns`. Use existing validity/offset/data buffers; do not collect `Vec<RuntimeValue>` or a second variable-width buffer. Keep canonical bytes, exact length, nesting limits, and validation identical.

- [ ] **Step 4: Add a round-trip regression and verify GREEN**

```rust
#[test]
fn large_variable_width_batch_round_trips_through_canonical_exchange() {
    let batch = large_string_bytes_and_node_batch();
    let frame = ExchangeFrame::encode(7, 0, false, snapshot(), &batch, limits()).unwrap();
    assert_eq!(frame.decode(expectation(), limits()).unwrap().into_batch(), batch);
}
```

Run: `cargo test -p query-executor --test column_batch && cargo test -p distributed-query --test exchange_codec`

Expected: PASS, including malformed/CRC/schema/nesting/resource-limit coverage.

- [ ] **Step 5: Commit**

```bash
git add crates/query-executor/src/column_batch.rs crates/query-executor/tests/column_batch.rs crates/distributed-query/src/exchange_codec.rs crates/distributed-query/tests/exchange_codec.rs
git commit -m "perf(query): avoid exchange value copies"
```

## Task 3: Replace Whole-Response Workers with Lazy Morsels

**Files:**
- Modify: `crates/distributed-query/src/worker.rs:20-24,248-278,414-443`
- Modify: `crates/distributed-query/src/lib.rs`
- Modify: `crates/distributed-query/tests/local_worker.rs`

**Interfaces:**
- Replaces worker `Future<Result<Vec<WorkerBatch>>>` with `WorkerMorselSource`.
- Defines `WorkerMorsel::next(&mut self) -> Future<Result<Option<WorkerBatch>, DistributedQueryError>>`.
- A worker encodes one current batch and at most one lookahead batch to set `has_more`.

- [ ] **Step 1: Write failing laziness tests**

```rust
#[tokio::test]
async fn local_worker_encodes_only_the_requested_morsel() {
    let mut source = worker_with_three_record_batches()
        .execute_fragment(&request(), &fragment(), now(), &ctx()).unwrap();
    assert_eq!(source.next().await.unwrap().unwrap().sequence(), 0);
    assert_eq!(worker_test_metrics().encoded_frames(), 1);
}

#[tokio::test]
async fn empty_worker_completes_without_an_empty_data_frame() {
    let mut source = empty_worker().execute_fragment(&request(), &fragment(), now(), &ctx()).unwrap();
    assert!(source.next().await.unwrap().is_none());
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p distributed-query --test local_worker morsel -- --test-threads=1`

Expected: compile failure because workers return vectors.

- [ ] **Step 3: Implement `LocalWorkerMorselSource`**

```rust
struct LocalWorkerMorselSource {
    shard_id: u32,
    request: FragmentRequest,
    batches: std::vec::IntoIter<RecordBatch>,
    next_sequence: u64,
    lookahead: Option<RecordBatch>,
}
```

Validate before construction. Rechunk/encode only requested work. Reserve the frame upper bound before encoding and release unconsumed reservations on drop. Keep a temporary drain adapter for unmigrated callers; delete it after Task 4.

- [ ] **Step 4: Verify GREEN and commit**

Run: `cargo test -p distributed-query --test local_worker morsel -- --test-threads=1`

```bash
git add crates/distributed-query/src/{lib.rs,worker.rs} crates/distributed-query/tests/local_worker.rs
git commit -m "perf(query): stream worker exchange morsels"
```

## Task 4: Concurrent Coordinator Fan-In and Credit Lifecycle

**Files:**
- Modify: `crates/distributed-query/src/coordinator.rs:100-250,1320-1510`
- Modify: `crates/distributed-query/tests/coordinator.rs`
- Modify: `crates/distributed-query/tests/performance_regressions.rs`

**Interfaces:**
- Produces `DistributedMorselStream::next() -> Result<Option<RetainedMorsel>, DistributedQueryError>`.
- `RetainedMorsel { batch: ColumnBatch, credit: CreditLease }` releases wire/decoded reservation exactly once on drop.
- `DistributedCoordinator::execute_morsels` starts all sources before awaiting one.

- [ ] **Step 1: Write failing concurrent dispatch and cancellation tests**

```rust
#[tokio::test]
async fn coordinator_starts_all_shards_before_waiting_for_response() {
    let starts = Arc::new(AtomicUsize::new(0));
    let mut stream = coordinator_with_counting_workers(starts.clone(), 3)
        .execute_morsels(request(), fragment()).await.unwrap();
    assert_eq!(starts.load(Ordering::SeqCst), 3);
    assert!(stream.next().await.unwrap().is_some());
}

#[tokio::test]
async fn dropping_stream_cancels_sources_and_releases_credit() {
    let metrics = install_exchange_test_metrics();
    drop(coordinator_with_workers(vec![blocked_worker()])
        .execute_morsels(request(), fragment()).await.unwrap());
    assert_eq!(metrics.retained_frame_bytes(), 0);
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p distributed-query --test coordinator concurrent -- --test-threads=1`

Expected: compile failure because coordinator is sequential and fully collected.

- [ ] **Step 3: Implement bounded fair fan-in**

Start every `FragmentWorker` source first. Keep at most one pending `next` future per source, poll with fair round-robin requeueing, reserve `frame_bytes + decoded_bytes_upper_bound` before decode, and attach the lease to the returned morsel. On error/cancellation/drop, stop all sources and release every lease. Do not retain `Vec<ColumnBatch>` in `TypedBatchMerger`.

- [ ] **Step 4: Verify GREEN and commit**

Run: `cargo test -p distributed-query --test coordinator -- --test-threads=1 && cargo test -p distributed-query --test local_worker -- --test-threads=1 && cargo test -p distributed-query --test exchange_codec`

```bash
git add crates/distributed-query/src/coordinator.rs crates/distributed-query/tests/{coordinator.rs,performance_regressions.rs}
git commit -m "perf(query): fan in shard morsels with credit backpressure"
```

## Task 5: Batch Canonical Fallback Materialization

**Files:**
- Modify: `crates/temporal-storage/src/store.rs:1160-1295`
- Modify: `crates/query-executor/src/temporal.rs:1010-1335`
- Create: `crates/temporal-storage/tests/batched_materialization.rs`
- Modify: `crates/query-executor/tests/temporal_scan.rs`

**Interfaces:**
- Produces internal `materialize_keys_batched(adapter, keys, max_items, max_request_bytes, snapshot) -> Result<Vec<Option<KeyValue>>, _>`.
- Preserves input ordinal and respects max keys, bytes, and in-flight pages.

- [ ] **Step 1: Write failing batching/order tests**

```rust
#[tokio::test]
async fn historical_scan_uses_bounded_multiget_pages() {
    let adapter = CountingAdapter::with_history_rows(9);
    let rows = TemporalStore::new(adapter.clone()).scan_historical_vertices(read(), page_of(3)).await.unwrap();
    assert_eq!(rows.len(), 9);
    assert_eq!(adapter.multi_get_call_sizes(), vec![3, 3, 3]);
}

#[tokio::test]
async fn expand_restores_input_order_after_batched_target_reads() {
    assert_eq!(executor_with_counting_adapter().expand(non_key_order_rows()).await.unwrap().ids(), vec![9, 2, 7]);
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p temporal-storage --test batched_materialization && cargo test -p query-executor --test temporal_scan expand_restores_input_order`

Expected: failure because generic fallback issues per-element reads.

- [ ] **Step 3: Implement bounded grouping**

After candidate filtering, deduplicate keys inside the existing read view, retain `(ordinal, key)`, partition by the existing scan/request item and byte limits as primitive `usize`/`u64` arguments, issue bounded-concurrent `multi_get`, validate response keys, and restore every original ordinal. Do not import `storage-api::QueryPageBounds` into `temporal-storage`, because it would introduce a dependency cycle. Keep temporal visibility, overlay, and missing-value behavior exactly unchanged.

- [ ] **Step 4: Verify GREEN and commit**

Run: `cargo test -p temporal-storage --test batched_materialization && cargo test -p query-executor --test temporal_scan && cargo test -p query-executor --test temporal_scope`

```bash
git add crates/temporal-storage/src/store.rs crates/temporal-storage/tests/batched_materialization.rs crates/query-executor/src/temporal.rs crates/query-executor/tests/temporal_scan.rs
git commit -m "perf(query): batch canonical graph materialization"
```

## Task 6: Concurrent Gateway Shard Fanout

**Files:**
- Modify: `crates/gateway-node/src/service.rs:300-375`
- Create: `crates/gateway-node/tests/read_fanout.rs`

**Interfaces:**
- Produces internal `multi_get_shards_concurrently(groups, max_inflight) -> Result<Vec<(usize, Option<Vec<u8>>)>, GatewayError>`.
- Each result is restored to original request ordinal; groups from different snapshot/security/deadline contexts never coalesce.

- [ ] **Step 1: Write a failing shard-start barrier test**

```rust
#[tokio::test]
async fn gateway_starts_independent_shards_before_releasing_the_first() {
    let gates = ShardCallGates::new(2);
    let task = tokio::spawn(gateway_with_gates(gates.clone()).multi_get(two_shard_request()));
    gates.wait_until_started(0).await;
    gates.wait_until_started(1).await;
    gates.release_all();
    assert_eq!(task.await.unwrap().unwrap(), expected_values());
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p gateway-node --test read_fanout gateway_starts_independent_shards -- --test-threads=1`

Expected: failure/timeout because the first shard is awaited before the second starts.

- [ ] **Step 3: Implement bounded concurrent fanout**

Use only a bounded Tokio task set or `tokio::sync::mpsc`; each task returns original ordinal and result. Fill preallocated output slots, abort/drain outstanding work on first error, and preserve existing per-shard ReadIndex/snapshot validation. Do not add a `futures` dependency for this task.

- [ ] **Step 4: Verify GREEN and commit**

Run: `cargo test -p gateway-node --test read_fanout -- --test-threads=1 && cargo test -p gateway-node --test primary_replica_analytics -- --test-threads=1`

```bash
git add crates/gateway-node/src/service.rs crates/gateway-node/tests/read_fanout.rs
git commit -m "perf(gateway): fan out shard reads concurrently"
```

## Task 7: Evidence Gate and Phase 2 Handoff

**Files:**
- Create: `docs/audit/performance/2026-07-24-phase-1-baseline.md`

- [ ] **Step 1: Record reproducible evidence**

Report fixture cardinality, deployment mode, revision, first-morsel readiness, maximum retained wire/decoded credit, frame count, fallback `multi_get` count, shard-start count, and actual command outputs. Mark unavailable values as unavailable; do not invent performance claims.

- [ ] **Step 2: Run final verification**

```bash
cargo test -p query-executor --test column_batch
cargo test -p distributed-query --all-targets -- --test-threads=1
cargo test -p temporal-storage --test batched_materialization -- --test-threads=1
cargo test -p gateway-node --test read_fanout -- --test-threads=1
cargo clippy -p query-executor -p distributed-query -p temporal-storage -p gateway-node --all-targets -- -D warnings
cargo fmt --all --check
git diff --check
scripts/check-tcypher-clean-break.sh
```

Expected: all phase-owned tests and static checks pass. Record unrelated pre-existing failures separately.

- [ ] **Step 3: Commit evidence only with matching code**

```bash
git add docs/audit/performance/2026-07-24-phase-1-baseline.md
git commit -m "docs(perf): record phase one query baseline"
```

## Phase 2 Entry Criteria

Start a separate plan only after Tasks 1-7 pass. Phase 2 owns immutable capability snapshots in `OptimizerContext`, physical primitive metadata, typed primitive execution, delayed `PropertyGather`, reusable read sessions, and prepared physical-template caching. It must not reopen Phase 1 stream interfaces without measured regression evidence or a semantic defect.
