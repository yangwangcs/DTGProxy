# DTGProxy Point History Clean-Break Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace full Anchor/Delta chain decoding and repeated full-projection rewriting with snapshot-bound, byte-budgeted point replay and a shared-payload interval materializer, then delete the old production path.

**Architecture:** `temporal-types` exposes borrowed canonical payload views; `temporal-storage` layers borrowed history-record views, a paged `HistoryCursor`, `PointHistoryReader`, and `IntervalHistoryMaterializer` on one `ReadSnapshot`. Public point and interval APIs migrate to those components before `reconstruct`, `rewrite_projection`, and their compatibility wrappers are removed.

**Tech Stack:** Rust 1.93, edition 2024, `storage-api::ReadSnapshot`, canonical page primitives, RocksDB/PostgreSQL/Neo4j adapters, fixed-seed temporal TCK, existing custom benchmark harness.

## Global Constraints

- Preserve the canonical persisted `Current`, Anchor/Delta, adjacency, and applied-index formats; they are the new authoritative storage contract, not compatibility code.
- Do not add legacy modules, feature flags, aliases, dual-read, dual-write, or fallback-to-old-path behavior.
- Every production history read must use a real `ReadSnapshot`, exact `applied_log_index`, item limits, and byte limits.
- Point replay must not call `ProjectionRecord::decode`, `reconstruct`, `rewrite_projection`, per-Delta full sort, or per-Delta full coalesce.
- Batch point replay must combine bounded backend requests and must not issue one RPC per element.
- Interval replay may materialize segments, but payloads must be shared during replay and copied only into the final public result.
- Keep `#![forbid(unsafe_code)]`; no `unsafe` parsing or unchecked length arithmetic.
- Preserve unrelated dirty-worktree changes and stage only files owned by the current task.
- The plan is complete only after old production symbols and call sites are deleted and the clean-break source scan passes.

---

## File Structure

- Create `crates/temporal-types/src/value_ref.rs`: borrowed, checked canonical-element parser and demanded-property projection.
- Modify `crates/temporal-types/src/lib.rs`: export `CanonicalElementRef` and `CanonicalPropertyRef`.
- Create `crates/temporal-types/tests/value_ref.rs`: corruption, lookup, projection, and allocation-boundary tests.
- Create `crates/temporal-storage/src/record_ref.rs`: borrowed Anchor, Delta, and Projection views.
- Create `crates/temporal-storage/src/history_reader.rs`: paged cursor, point replay, statistics, and budgets.
- Create `crates/temporal-storage/src/history_materializer.rs`: shared-payload interval replay and final projection creation.
- Modify `crates/temporal-storage/src/history.rs`: keep Anchor/Delta write policy only; remove old reconstruction.
- Delete `crates/temporal-storage/src/rewrite.rs`: old clone/sort/coalesce implementation.
- Modify `crates/temporal-storage/src/store.rs`: route all point, scan, segment, diff, expansion, and commit preparation call sites through the new readers/materializer.
- Modify `crates/temporal-storage/src/lib.rs`: export the new budget/stat types and remove the old module.
- Create `crates/temporal-storage/tests/point_history_reader.rs`: point fast-path and page/budget tests.
- Create `crates/temporal-storage/tests/history_materializer.rs`: full interval semantics and shared-payload replay tests.
- Modify `crates/temporal-storage/tests/query_adapter_metrics.rs`: assert bounded canonical pages rather than unbounded scans.
- Modify `crates/temporal-storage/benches/roundtrip.rs`: report replay depth, scanned bytes, decoded payloads, and copied payload bytes.
- Create `docs/audit/performance/2026-07-28-point-history-clean-break.md`: reproducible before/after method and measured results.

### Task 1: Capture the Existing Point-History Baseline

**Files:**
- Modify: `crates/temporal-storage/benches/roundtrip.rs`
- Create: `docs/audit/performance/2026-07-28-point-history-clean-break.md`

**Interfaces:**
- Consumes: existing `TemporalStore::vertex_as_of` and `DTGPROXY_BENCH_ITERS`.
- Produces: stable benchmark cell names used unchanged after the implementation.

- [ ] **Step 1: Add fixed benchmark cells before changing production history code**

Add these calls beside the existing replay-depth measurements:

```rust
for (name, snapshot) in [
    ("as_of_replay_depth_0", 99_200_i64),
    ("as_of_replay_depth_1", 99_300_i64),
    ("as_of_replay_depth_8", 100_000_i64),
    ("as_of_replay_depth_15", 100_700_i64),
] {
    measure(name, iterations, || {
        black_box(block_on(store.vertex_as_of(vertex, valid(50), tx(snapshot))).unwrap());
    });
}
```

Remove the duplicate old `as_of_replay_depth_1` and `as_of_replay_depth_16` cells so every name is emitted exactly once.

- [ ] **Step 2: Run the baseline three times**

Run:

```bash
for run in 1 2 3; do
  DTGPROXY_BENCH_ITERS=10000 cargo bench --locked -p temporal-storage --features rocksdb-tests --bench roundtrip
done
```

Expected: all three runs exit 0 and emit the four fixed `as_of_replay_depth_*` cells plus `iterations=10000`.

- [ ] **Step 3: Record the immutable baseline method and raw outputs**

Create the report with these exact headings and fill each table cell from the three command outputs:

```markdown
# Point History Clean-Break Performance Report

## Revision and environment
## Dataset and fixed seed
## Commands
## Pre-change raw runs
## Pre-change median latency
## Allocation and retained-memory method
## Post-change raw runs
## Comparison
## Remaining bottlenecks
## Conclusion
```

Do not enter post-change values yet; omit the post-change table body until Task 8 rather than adding placeholders.

- [ ] **Step 4: Verify the benchmark remains deterministic**

Run:

```bash
cargo test --locked -p temporal-storage --test history_chain -- --test-threads=1
```

Expected: PASS, including the fifteen-Delta/new-Anchor policy test.

- [ ] **Step 5: Commit the baseline fixture and report**

```bash
git add crates/temporal-storage/benches/roundtrip.rs docs/audit/performance/2026-07-28-point-history-clean-break.md
git commit -m "bench: capture point history clean-break baseline"
```

### Task 2: Add Borrowed Canonical-Element Views

**Files:**
- Create: `crates/temporal-types/src/value_ref.rs`
- Modify: `crates/temporal-types/src/lib.rs`
- Create: `crates/temporal-types/tests/value_ref.rs`

**Interfaces:**
- Consumes: canonical payload encoding from `crates/temporal-types/src/value.rs`.
- Produces: `CanonicalElementRef::parse`, `schema_version`, `property`, `project`, and `encoded`.

- [ ] **Step 1: Write failing lookup and corruption tests**

```rust
use std::collections::BTreeMap;
use temporal_types::{CanonicalElement, CanonicalElementRef, CodecError, GraphValue};

#[test]
fn borrowed_view_finds_one_property_without_materializing_the_map() {
    let encoded = CanonicalElement::new(
        7,
        BTreeMap::from([
            (1, GraphValue::Integer(11)),
            (9, GraphValue::String("kept".into())),
        ]),
    )
    .encode()
    .unwrap();
    let view = CanonicalElementRef::parse(&encoded).unwrap();
    assert_eq!(view.schema_version(), 7);
    assert_eq!(
        view.property(1).unwrap().unwrap().decode().unwrap(),
        GraphValue::Integer(11)
    );
    assert_eq!(view.property(2).unwrap(), None);
    assert_eq!(view.encoded(), encoded.as_slice());
}

#[test]
fn borrowed_view_rejects_truncated_values() {
    let encoded = CanonicalElement::new(1, BTreeMap::new()).encode().unwrap();
    assert_eq!(
        CanonicalElementRef::parse(&encoded[..encoded.len() - 1]),
        Err(CodecError::UnexpectedEnd)
    );
}
```

- [ ] **Step 2: Run the tests and confirm the new type is missing**

Run:

```bash
cargo test --locked -p temporal-types --test value_ref -- --test-threads=1
```

Expected: FAIL because `CanonicalElementRef` is not exported.

- [ ] **Step 3: Implement the borrowed API without unsafe code**

Add these public shapes in `value_ref.rs`:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalElementRef<'a> {
    encoded: &'a [u8],
    schema_version: u64,
    properties_offset: usize,
    property_count: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CanonicalPropertyRef<'a> {
    encoded_value: &'a [u8],
}

impl<'a> CanonicalElementRef<'a> {
    pub fn parse(encoded: &'a [u8]) -> Result<Self, CodecError>;
    pub const fn schema_version(self) -> u64;
    pub const fn encoded(self) -> &'a [u8];
    pub fn property(self, property_id: u32) -> Result<Option<CanonicalPropertyRef<'a>>, CodecError>;
    pub fn project(self, demanded: &[u32]) -> Result<CanonicalElement, CodecError>;
}

impl CanonicalPropertyRef<'_> {
    pub fn decode(self) -> Result<GraphValue, CodecError>;
}
```

Move the checked value-length walker and single-value decoder from `value.rs` into crate-private helpers shared by owned and borrowed decoding. Keep the wire format and `CodecError` variants unchanged.

- [ ] **Step 4: Add demanded-property ordering and nested-value tests**

Add tests asserting `project(&[9, 1, 9])` returns properties 1 and 9 once in canonical ID order, and that nested lists/bytes/string corruption is rejected before returning a view.

- [ ] **Step 5: Run temporal-types tests**

Run:

```bash
cargo test --locked -p temporal-types -- --test-threads=1
```

Expected: PASS with no wire-format fixture changes.

- [ ] **Step 6: Commit**

```bash
git add crates/temporal-types/src/value.rs crates/temporal-types/src/value_ref.rs crates/temporal-types/src/lib.rs crates/temporal-types/tests/value_ref.rs
git commit -m "feat: add borrowed canonical element views"
```

### Task 3: Add Borrowed History-Record Views

**Files:**
- Create: `crates/temporal-storage/src/record_ref.rs`
- Modify: `crates/temporal-storage/src/record.rs`
- Modify: `crates/temporal-storage/src/lib.rs`
- Create: `crates/temporal-storage/tests/record_ref.rs`

**Interfaces:**
- Consumes: `CanonicalElementRef` from Task 2 and existing record checksum/encoding.
- Produces: `HistoryEntryRef`, `HistoryAnchorRef`, `HistoryDeltaRef`, `ProjectionRecordRef`, and `HistoryOperationRef`.

- [ ] **Step 1: Write failing selective-parse tests**

```rust
#[test]
fn projection_ref_selects_only_the_segment_containing_valid_time() {
    let projection = projection_with_two_segments();
    let encoded = projection.encode().unwrap();
    let view = ProjectionRecordRef::parse(&encoded).unwrap();
    assert_eq!(
        view.visible_at(valid(2)).unwrap().unwrap().property(1).unwrap().unwrap().decode().unwrap(),
        value("old")
    );
    assert_eq!(view.visible_at(valid(5)).unwrap(), None);
    assert_eq!(
        view.visible_at(valid(8)).unwrap().unwrap().property(1).unwrap().unwrap().decode().unwrap(),
        value("new")
    );
}

#[test]
fn delta_ref_skips_non_matching_payload_but_still_checks_checksum() {
    let encoded = HistoryDelta::put(tx(3), interval(10, 20), payload("large")).encode().unwrap();
    let view = HistoryEntryRef::parse(&encoded).unwrap();
    assert!(!view.changed_valid().contains(valid(5)));
}
```

- [ ] **Step 2: Run the new test and verify it fails**

Run:

```bash
cargo test --locked -p temporal-storage --test record_ref -- --test-threads=1
```

Expected: FAIL because borrowed record types do not exist.

- [ ] **Step 3: Implement checked borrowed record shapes**

```rust
pub enum HistoryEntryRef<'a> {
    Anchor(HistoryAnchorRef<'a>),
    Delta(HistoryDeltaRef<'a>),
}

pub struct HistoryAnchorRef<'a> {
    commit_ts: TransactionTime,
    changed_valid: Interval<ValidTime>,
    projection: ProjectionRecordRef<'a>,
}

pub struct HistoryDeltaRef<'a> {
    commit_ts: TransactionTime,
    changed_valid: Interval<ValidTime>,
    operation: HistoryOperationRef<'a>,
}

pub enum HistoryOperationRef<'a> {
    Put(CanonicalElementRef<'a>),
    Delete,
}
```

Expose crate-private checked decoder helpers from `record.rs`; do not duplicate magic, interval, timestamp, length, or checksum rules.

- [ ] **Step 4: Add exact owned/borrowed equivalence tests**

For empty and multi-segment anchors plus Put/Delete deltas, assert borrowed fields equal `HistoryEntry::decode` fields and corrupted checksum/version/length returns the same `RecordCodecError` class.

- [ ] **Step 5: Run record and history tests**

Run:

```bash
cargo test --locked -p temporal-storage --test record_codec --test record_ref --test history_chain -- --test-threads=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/temporal-storage/src/record.rs crates/temporal-storage/src/record_ref.rs crates/temporal-storage/src/lib.rs crates/temporal-storage/tests/record_ref.rs
git commit -m "feat: add borrowed temporal record views"
```

### Task 4: Implement Snapshot-Bound PointHistoryReader

**Files:**
- Modify: `crates/storage-api/src/query.rs`
- Modify: `crates/storage-api/src/lib.rs`
- Modify: `crates/adapter-memory/src/lib.rs`
- Modify: `crates/adapter-rocksdb/src/lib.rs`
- Modify: `crates/adapter-postgres/src/lib.rs`
- Modify: `crates/adapter-neo4j/src/lib.rs`
- Modify: `crates/adapter-sidecar/proto/dtg_adapter_v1.proto`
- Modify: `crates/adapter-sidecar/src/lib.rs`
- Modify: `crates/adapter-sidecar/src/service.rs`
- Create: `crates/temporal-storage/src/history_reader.rs`
- Modify: `crates/temporal-storage/src/lib.rs`
- Create: `crates/temporal-storage/tests/point_history_reader.rs`
- Modify: `crates/temporal-storage/src/observed_adapter.rs`

**Interfaces:**
- Consumes: `ReadSnapshot::scan_canonical`, `CanonicalScanRequest`, `QueryPageBounds`, `HistoryEntryRef`, and history-key ordering.
- Produces: `CanonicalBatchScanRequest`, `CanonicalBatchScanPage`, `ReadSnapshot::scan_canonical_batch`, `HistoryReadBudget`, `HistoryReadStats`, `PointHistoryReader::read`, and `PointHistoryReader::read_batch`.

- [ ] **Step 1: Write failing depth, skip, and byte-budget tests**

```rust
#[test]
fn point_reader_replays_only_deltas_covering_the_requested_valid_time() {
    let fixture = history_fixture_with_disjoint_large_delta();
    let read = block_on(fixture.store.begin_read_snapshot()).unwrap();
    let outcome = block_on(PointHistoryReader::new(HistoryReadBudget::new(16, 128 * 1024, 64 * 1024).unwrap()).read(
        read.as_ref(),
        fixture.element,
        tx(400),
        valid(5),
        PropertyDemand::All,
    ))
    .unwrap();
    assert_eq!(outcome.value, Some(payload("visible")));
    assert_eq!(outcome.stats.history_records, 4);
    assert_eq!(outcome.stats.payloads_decoded, 2);
}

#[test]
fn point_reader_rejects_an_anchor_larger_than_the_record_budget() {
    let fixture = oversized_anchor_fixture();
    let read = block_on(fixture.store.begin_read_snapshot()).unwrap();
    assert_eq!(
        block_on(PointHistoryReader::new(HistoryReadBudget::new(16, 1024, 512).unwrap()).read(
            read.as_ref(), fixture.element, tx(200), valid(1), PropertyDemand::All,
        )),
        Err(TemporalStoreError::HistoryRecordByteLimit)
    );
}
```

- [ ] **Step 2: Run the tests and confirm missing types**

Run:

```bash
cargo test --locked -p temporal-storage --test point_history_reader -- --test-threads=1
```

Expected: FAIL because the reader and budget types are missing.

- [ ] **Step 3: Implement budgets, stats, and reader outcome**

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HistoryReadBudget {
    max_records: usize,
    max_total_bytes: u64,
    max_record_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistoryReadStats {
    pub history_records: usize,
    pub history_bytes: u64,
    pub payloads_decoded: usize,
    pub payload_bytes_copied: u64,
}

pub enum PropertyDemand<'a> {
    All,
    Selected(&'a [u32]),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PointHistoryRequest {
    pub element: ElementRef,
    pub transaction_time: TransactionTime,
    pub valid_time: ValidTime,
}

pub struct PointHistoryOutcome {
    pub value: Option<CanonicalElement>,
    pub stats: HistoryReadStats,
}

impl PointHistoryReader {
    pub async fn read_batch(
        &self,
        read: &dyn ReadSnapshot,
        requests: &[PointHistoryRequest],
        demand: PropertyDemand<'_>,
    ) -> Result<Vec<PointHistoryOutcome>, TemporalStoreError>;
}
```

Validate `max_records` in `1..=MAX_CHAIN_ENTRIES`, both byte limits as nonzero, and `max_record_bytes <= max_total_bytes`.

- [ ] **Step 4: Add one-call multi-range canonical scans to the Adapter SPI**

Add these bounded types to `storage-api`:

```rust
pub const MAX_CANONICAL_BATCH_RANGES: usize = 256;

pub struct CanonicalBatchScanRequest {
    scans: Vec<CanonicalScanRequest>,
    max_total_bytes: u64,
}

pub struct CanonicalBatchScanPage {
    applied_log_index: u64,
    pages: Vec<CanonicalScanPage>,
}
```

`CanonicalBatchScanRequest::new` rejects zero or more than 256 ranges, duplicate ranges, zero total bytes, and an aggregate request larger than `MAX_QUERY_PAGE_BYTES`. Add this required snapshot method:

```rust
fn scan_canonical_batch<'a>(
    &'a self,
    request: &'a CanonicalBatchScanRequest,
) -> AdapterFuture<'a, CanonicalBatchScanPage>;
```

Memory and RocksDB loop bounded iterators inside one adapter call. PostgreSQL executes one parameterized multi-range statement inside its repeatable-read transaction. Neo4j uses one `UNWIND $ranges` Query API statement inside its open transaction. Sidecar adds one versioned request/response RPC carrying all ranges and pages. No production implementation may satisfy this method by calling a remote single-range method once per range.

- [ ] **Step 5: Add batch primitive contract tests**

For Memory, RocksDB, PostgreSQL SQL contract, Neo4j protocol contract, and Sidecar protocol tests, assert input order is preserved, every subpage respects its bounds, total retained bytes respect `max_total_bytes`, and all pages carry the same `applied_log_index`.

- [ ] **Step 6: Implement paged point replay**

Implement `PointHistoryReader::read` with this state machine:

```rust
let expected_index = read.applied_log_index();
let mut pending = Vec::<OwnedPointDelta>::with_capacity(MAX_DELTAS_PER_ANCHOR);
let mut span = history_span_at(element, transaction_time);
loop {
    let page = read.scan_canonical(&CanonicalScanRequest::new(
        span.clone(),
        QueryPageBounds::new(remaining_records, remaining_page_bytes)?,
    )?).await?;
    require_same_applied_index(expected_index, page.applied_log_index())?;
    for entry in page.entries() {
        charge_record(entry, budget, &mut stats)?;
        match HistoryEntryRef::parse(entry.value())? {
            HistoryEntryRef::Delta(delta) => retain_only_matching_delta(delta, valid_time, demand, &mut pending, &mut stats)?,
            HistoryEntryRef::Anchor(anchor) => return replay_from_anchor(anchor, pending, valid_time, demand, stats),
        }
    }
    span = advance_history_span(span, page.next_start())?;
}
```

`OwnedPointDelta` stores only matching Put payload bytes or Delete, never a decoded full `HistoryDelta` and never a nonmatching payload.

- [ ] **Step 7: Implement bounded batch replay**

`read_batch` accepts `&[PointHistoryRequest]`, creates at most 256 history ranges per batch, calls `scan_canonical_batch` once per batch under one shared snapshot, and returns results in original input order. Assert with a counting adapter that nine requests use one adapter call and no page exceeds configured item/byte limits.

- [ ] **Step 8: Run reader, adapter, protocol, and corruption tests**

Run:

```bash
cargo test --locked -p temporal-storage --test point_history_reader --test query_adapter_metrics --test record_ref -- --test-threads=1
cargo test --locked -p storage-api --test query_primitives -- --test-threads=1
cargo test --locked -p adapter-sidecar --test protocol --test client -- --test-threads=1
```

Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/storage-api/src/query.rs crates/storage-api/src/lib.rs crates/adapter-memory/src/lib.rs crates/adapter-rocksdb/src/lib.rs crates/adapter-postgres/src/lib.rs crates/adapter-neo4j/src/lib.rs crates/adapter-sidecar/proto/dtg_adapter_v1.proto crates/adapter-sidecar/src/lib.rs crates/adapter-sidecar/src/service.rs crates/temporal-storage/src/history_reader.rs crates/temporal-storage/src/lib.rs crates/temporal-storage/src/observed_adapter.rs crates/temporal-storage/tests/point_history_reader.rs crates/temporal-storage/tests/query_adapter_metrics.rs
git commit -m "feat: add snapshot-bound point history reader"
```

### Task 5: Migrate Point APIs and Historical Expansions

**Files:**
- Modify: `crates/temporal-storage/src/store.rs`
- Modify: `crates/temporal-storage/tests/history_chain.rs`
- Modify: `crates/temporal-storage/tests/historical_expansion.rs`
- Modify: `crates/temporal-storage/tests/randomized_tck.rs`
- Modify: `crates/replica-snapshot/tests/temporal_equivalence.rs`

**Interfaces:**
- Consumes: `PointHistoryReader` from Task 4.
- Produces: snapshot-bound `vertex_as_of_in_snapshot`, `edge_as_of_in_snapshot`, and batched historical element materialization.

- [ ] **Step 1: Add failing snapshot-consistency tests**

Add a test that opens one `ReadSnapshot`, commits a newer value through another handle, and proves both `vertex_as_of_in_snapshot` calls still return the value at the snapshot applied index.

- [ ] **Step 2: Add the new public snapshot-bound methods**

```rust
pub fn vertex_as_of_in_snapshot<'a>(
    &'a self,
    read: &'a dyn ReadSnapshot,
    element: ElementRef,
    valid_time: ValidTime,
    transaction_time: TransactionTime,
    budget: HistoryReadBudget,
) -> TemporalStoreFuture<'a, Option<CanonicalElement>>;

pub fn edge_as_of_in_snapshot<'a>(
    &'a self,
    read: &'a dyn ReadSnapshot,
    element: ElementRef,
    valid_time: ValidTime,
    transaction_time: TransactionTime,
    budget: HistoryReadBudget,
) -> TemporalStoreFuture<'a, Option<CanonicalElement>>;
```

The existing `vertex_as_of` and `edge_as_of` names remain as the canonical convenience API, but their implementation must open one `ReadSnapshot` and delegate to the new reader. This is not a compatibility path because both names now have only the new implementation.

- [ ] **Step 3: Migrate point-view and historical expansion call sites**

Replace every point-style `load_projection_at(...).visible_at(valid_time)` call with `PointHistoryReader`. For graph scans and expansions, collect bounded element IDs and call `read_batch`; do not call the single-element method inside a loop.

- [ ] **Step 4: Run focused behavior tests**

Run:

```bash
cargo test --locked -p temporal-storage --test history_chain --test historical_expansion --test vertex_roundtrip_memory --test edge_roundtrip_memory -- --test-threads=1
```

Expected: PASS.

- [ ] **Step 5: Run fixed-seed Memory/RocksDB equivalence**

Run:

```bash
cargo test --locked -p temporal-storage --features rocksdb-tests --test randomized_tck -- --test-threads=1
cargo test --locked -p replica-snapshot --test temporal_equivalence -- --test-threads=1
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/temporal-storage/src/store.rs crates/temporal-storage/tests/history_chain.rs crates/temporal-storage/tests/historical_expansion.rs crates/temporal-storage/tests/randomized_tck.rs crates/replica-snapshot/tests/temporal_equivalence.rs
git commit -m "refactor: route historical point reads through point replay"
```

### Task 6: Implement Shared-Payload IntervalHistoryMaterializer

**Files:**
- Create: `crates/temporal-storage/src/history_materializer.rs`
- Create: `crates/temporal-storage/tests/history_materializer.rs`
- Modify: `crates/temporal-storage/src/store.rs`
- Modify: `crates/temporal-storage/src/lib.rs`

**Interfaces:**
- Consumes: `HistoryEntryRef`, `HistoryReadBudget`, and canonical payload bytes.
- Produces: `IntervalHistoryMaterializer::projection_at` and `ProjectionEditor::apply` for read and write paths.

- [ ] **Step 1: Write failing multi-correction and clone-count tests**

Use a test-only payload observer to assert fifteen corrections produce the same final segments as the temporal model while payload materialization count is bounded by final output segments plus matching Put deltas, not by `delta_count * segment_count`.

- [ ] **Step 2: Implement shared internal segments**

```rust
#[derive(Clone)]
struct SharedSegment {
    valid: Interval<ValidTime>,
    payload: Arc<CanonicalElement>,
}

pub(crate) struct ProjectionEditor {
    commit_ts: TransactionTime,
    segments: Vec<SharedSegment>,
}

impl ProjectionEditor {
    pub(crate) fn from_projection(projection: &ProjectionRecord) -> Self;
    pub(crate) fn apply(
        &mut self,
        commit_ts: TransactionTime,
        changed_valid: Interval<ValidTime>,
        replacement: Option<CanonicalElement>,
    ) -> Result<(), RecordCodecError>;
    pub(crate) fn finish(self) -> Result<ProjectionRecord, RecordCodecError>;
}
```

`apply` performs interval splitting with `Arc::clone`, inserts the replacement once, and preserves sorted non-overlapping order without sorting the entire vector. `finish` performs one adjacent-equality coalesce and converts shared payloads to owned final segments.

- [ ] **Step 3: Implement historical interval materialization**

`IntervalHistoryMaterializer::projection_at` reads the same bounded history pages, decodes the Anchor once, applies Deltas in chronological order through `ProjectionEditor`, and returns `Option<ProjectionRecord>` plus `HistoryReadStats`.

- [ ] **Step 4: Migrate write preparation**

Replace both commit-preparation calls to `rewrite_projection` in `store.rs` with `ProjectionEditor::from_projection(...).apply(...).finish()` so read and write interval semantics share one implementation.

- [ ] **Step 5: Migrate segment and diff APIs**

Replace interval-oriented `load_projection_at` callers, including segment scans and `diff_element`, with `IntervalHistoryMaterializer`. Keep point APIs on `PointHistoryReader`.

- [ ] **Step 6: Run interval and diff tests**

Run:

```bash
cargo test --locked -p temporal-storage --test history_materializer --test temporal_segments --test diff --test transaction -- --test-threads=1
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add crates/temporal-storage/src/history_materializer.rs crates/temporal-storage/src/store.rs crates/temporal-storage/src/lib.rs crates/temporal-storage/tests/history_materializer.rs
git commit -m "feat: add shared-payload interval history materializer"
```

### Task 7: Delete the Old History Reconstruction Path

**Files:**
- Modify: `crates/temporal-storage/src/history.rs`
- Delete: `crates/temporal-storage/src/rewrite.rs`
- Modify: `crates/temporal-storage/src/lib.rs`
- Modify: `crates/temporal-storage/src/store.rs`
- Modify: affected tests under `crates/temporal-storage/tests/`

**Interfaces:**
- Consumes: all migrated call sites from Tasks 5 and 6.
- Produces: one production history implementation with no legacy symbols.

- [ ] **Step 1: Prove all production call sites have migrated**

Run:

```bash
rg -n "load_projection_at|reconstruct\(|rewrite_projection\(" crates/temporal-storage/src crates/query-executor/src crates/distributed-query/src
```

Expected before deletion: only the old definitions/imports remain; no production caller remains.

- [ ] **Step 2: Remove old code**

Delete `reconstruct` from `history.rs`, delete `rewrite.rs`, remove `mod rewrite`, remove imports, remove obsolete errors that are not used by the new cursor/materializer, and remove test helpers that invoke the old path directly.

- [ ] **Step 3: Add a clean-break source gate**

Create `scripts/check-point-history-clean-break.sh` with:

```bash
#!/usr/bin/env bash
set -euo pipefail
if rg -n 'load_projection_at|fn reconstruct\(|rewrite_projection|mod rewrite' \
  crates/temporal-storage/src crates/query-executor/src crates/distributed-query/src; then
  echo 'legacy point-history path remains' >&2
  exit 1
fi
```

Add `scripts/tests/check-point-history-clean-break.sh` that injects each forbidden symbol into a temporary copied source tree and asserts the gate fails, then runs it on the real source and asserts success.

- [ ] **Step 4: Run formatting, source gate, and the temporal-storage suite**

Run:

```bash
cargo fmt --all -- --check
bash scripts/tests/check-point-history-clean-break.sh
cargo test --locked -p temporal-storage --all-features -- --test-threads=1
```

Expected: all commands exit 0.

- [ ] **Step 5: Commit deletion**

```bash
git add crates/temporal-storage/src crates/temporal-storage/tests scripts/check-point-history-clean-break.sh scripts/tests/check-point-history-clean-break.sh
git commit -m "refactor: remove legacy history reconstruction"
```

### Task 8: Verify Three Backends and Publish the Performance Result

**Files:**
- Modify: `crates/adapter-memory/tests/adapter_contract.rs`
- Modify: `crates/adapter-rocksdb/tests/adapter_contract.rs`
- Modify: `crates/adapter-postgres/tests/live_postgres.rs`
- Modify: `crates/adapter-neo4j/tests/live_neo4j.rs`
- Modify: `crates/temporal-storage/benches/roundtrip.rs`
- Create: `scripts/test-neo4j-live.sh`
- Modify: `docs/audit/performance/2026-07-28-point-history-clean-break.md`

**Interfaces:**
- Consumes: final PointHistoryReader/materializer and stable benchmark cell names.
- Produces: backend contract evidence and before/after performance report.

- [ ] **Step 1: Add adapter canonical-page continuation tests for history spans**

For each adapter, insert one Anchor and fifteen Delta records, request pages of 3 items and a small byte budget, advance with `next_start`, and assert concatenated keys equal canonical key order with one stable `applied_log_index`.

- [ ] **Step 2: Run Memory and RocksDB contracts**

Run:

```bash
cargo test --locked -p adapter-memory --test adapter_contract -- --test-threads=1
cargo test --locked -p adapter-rocksdb --test adapter_contract -- --test-threads=1
```

Expected: PASS.

- [ ] **Step 3: Add and run a self-contained Neo4j live-test launcher**

Create `scripts/test-neo4j-live.sh` with a unique container name, fixed loopback-only ports, readiness polling, and unconditional cleanup:

```bash
#!/usr/bin/env bash
set -euo pipefail
if ! command -v docker >/dev/null 2>&1; then
  echo 'docker is required for the disposable Neo4j live test' >&2
  exit 78
fi
container="dtgproxy-neo4j-point-history-$$"
cleanup() { docker rm -f "$container" >/dev/null 2>&1 || true; }
trap cleanup EXIT INT TERM
docker run --detach --rm --name "$container" \
  --publish 127.0.0.1::7474 \
  --env NEO4J_AUTH=neo4j/dtgproxy-point-history-password \
  neo4j:5.26-community >/dev/null
port=$(docker port "$container" 7474/tcp | awk -F: 'NR==1 {print $NF}')
for attempt in $(seq 1 60); do
  if curl --fail --silent "http://127.0.0.1:$port" >/dev/null; then break; fi
  if [[ "$attempt" == 60 ]]; then echo 'Neo4j did not become ready' >&2; exit 1; fi
  sleep 1
done
DTGPROXY_NEO4J_ENDPOINT="http://127.0.0.1:$port" \
DTGPROXY_NEO4J_USERNAME=neo4j \
DTGPROXY_NEO4J_PASSWORD=dtgproxy-point-history-password \
DTGPROXY_NEO4J_DATABASE=neo4j \
cargo test --locked -p adapter-neo4j -- --test-threads=1
```

Run:

```bash
scripts/test-postgres-live.sh
bash scripts/test-neo4j-live.sh
```

Expected: both commands exit 0. Exit 78 is recorded as unavailable evidence and does not permit claiming three-backend completion.

- [ ] **Step 4: Run the post-change benchmark three times**

Run the same command as Task 1 with `DTGPROXY_BENCH_ITERS=10000`. Add raw outputs and medians to the report without changing the baseline cells or method.

- [ ] **Step 5: Calculate the acceptance gates**

Record for replay depth 15:

```text
latency_improvement = (old_median_ns - new_median_ns) / old_median_ns
allocation_reduction = (old_allocated_bytes - new_allocated_bytes) / old_allocated_bytes
retained_reduction = (old_peak_retained - new_peak_retained) / old_peak_retained
```

Require at least 30% latency improvement in p50 or p95, 60% allocation/copy reduction, 50% peak-retained reduction, and no more than 5% p95 regression at depths 0/1.

- [ ] **Step 6: Run final verification**

Run:

```bash
bash scripts/check-point-history-clean-break.sh
cargo fmt --all -- --check
cargo clippy --locked -p temporal-types -p temporal-storage -p adapter-memory -p adapter-rocksdb --all-targets --all-features -- -D warnings
cargo test --locked -p temporal-types -- --test-threads=1
cargo test --locked -p temporal-storage --all-features -- --test-threads=1
cargo test --locked -p adapter-memory -p adapter-rocksdb -- --test-threads=1
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit verification artifacts**

```bash
git add crates/adapter-memory/tests/adapter_contract.rs crates/adapter-rocksdb/tests/adapter_contract.rs crates/adapter-postgres/tests/live_postgres.rs crates/adapter-neo4j/tests/live_neo4j.rs crates/temporal-storage/benches/roundtrip.rs scripts/test-neo4j-live.sh docs/audit/performance/2026-07-28-point-history-clean-break.md
git commit -m "perf: verify point history clean break"
```

## Follow-On Plans

After this plan passes, write and execute these separate plans against the resulting commit:

1. `2026-07-28-dtgproxy-snapshot-csr-core.md`: `snapshot-csr` crate, dictionary, directional CSR, memory reservation, Overlay, cache, and correctness tests.
2. `2026-07-28-dtgproxy-snapshot-csr-integration.md`: `GraphAccessPathSelector`, query/distributed integration, historical CSR candidates, three-backend traversal benchmarks, old Expand-path deletion, and final combined report.
