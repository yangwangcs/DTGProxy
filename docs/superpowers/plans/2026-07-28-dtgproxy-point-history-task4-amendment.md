# DTGProxy Point History Task 4 Amendment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish a reviewable prerequisite snapshot-query baseline, add one-call bounded canonical batch scans to every targeted backend, and implement a snapshot-bound point-history reader with deterministic budgets, pagination, key validation, and duplicate-range fan-out.

**Architecture:** A protected prerequisite checkpoint creates a clean parent for the currently uncommitted query/snapshot surface. Task 4A1 adds the batch request/page contract plus in-process implementations; Task 4A2 adds PostgreSQL, Neo4j, Sidecar, and observation support; Task 4B adds point replay. Unsupported snapshots fail closed and no implementation may issue N remote single-range calls.

**Tech Stack:** Rust 1.93, edition 2024, `storage-api::ReadSnapshot`, Memory/RocksDB/PostgreSQL/Neo4j adapters, protobuf Sidecar transport, fixed-seed temporal tests.

## Global Constraints

- Preserve the canonical persisted `Current`, Anchor/Delta, adjacency, and applied-index formats.
- Do not add legacy modules, aliases, feature flags, dual-read, dual-write, or fallback-to-old-path behavior.
- Every production history read uses one real `ReadSnapshot`, its exact `applied_log_index`, item limits, record-byte limits, total-byte limits, and validated continuations.
- Batch point replay groups duplicate history ranges and never issues one remote call per element.
- Point replay does not call `ProjectionRecord::decode`, `reconstruct`, `rewrite_projection`, per-Delta full sort, or per-Delta full coalesce.
- Keep `#![forbid(unsafe_code)]`; parsing and quota arithmetic use checked operations and checked slices.
- Preserve unrelated dirty-worktree changes and stage only the explicit path allow-list for each task.
- The Task 4 boundary amendment design at `docs/superpowers/specs/2026-07-28-dtgproxy-point-history-task4-boundary-amendment-design.md` overrides conflicting Task 4 text in the original plan.

---

## File Structure

- `crates/storage-api/src/query.rs`: canonical batch request/page validation and aggregate accounting.
- `crates/storage-api/src/lib.rs`: fail-closed `ReadSnapshot::scan_canonical_batch` method and exports.
- Backend adapter files: one physical multi-range operation per call.
- Sidecar proto/client/service: one versioned multi-range request/response and feature negotiation.
- `crates/temporal-storage/src/history_reader.rs`: budgets, replay state, grouping, paging, key validation, and public outcomes.
- `crates/temporal-storage/src/store.rs`: explicit point-history error variants only.
- New focused tests own Task 4 behavior; the protected preexisting `query_adapter_metrics.rs` is not modified.

### Task 0: Checkpoint the Protected Snapshot-Query Prerequisite

**Files:**
- Checkpoint current versions of:
  - `crates/storage-api/Cargo.toml`
  - `crates/storage-api/src/query.rs`
  - `crates/storage-api/src/lib.rs`
  - `crates/storage-api/src/mapping.rs`
  - `crates/storage-api/tests/query_primitives.rs`
  - `crates/adapter-memory/Cargo.toml`
  - `crates/adapter-memory/src/lib.rs`
  - `crates/adapter-memory/tests/adapter_contract.rs`
  - `crates/adapter-rocksdb/Cargo.toml`
  - `crates/adapter-rocksdb/src/lib.rs`
  - `crates/adapter-rocksdb/tests/adapter_contract.rs`
  - `crates/adapter-postgres/src/lib.rs`
  - `crates/adapter-postgres/tests/sql_contract.rs`
  - `crates/adapter-neo4j/Cargo.toml`
  - `crates/adapter-neo4j/src/lib.rs`
  - `crates/adapter-sidecar/Cargo.toml`
  - `crates/adapter-sidecar/proto/dtg_adapter_v1.proto`
  - `crates/adapter-sidecar/src/lib.rs`
  - `crates/adapter-sidecar/src/service.rs`
  - `crates/adapter-sidecar/tests/client.rs`
  - `crates/adapter-sidecar/tests/protocol.rs`
  - `crates/adapter-sidecar/tests/snapshot_protocol.rs`
  - `crates/adapter-sidecar/tests/stateful_snapshot.rs`
  - `crates/temporal-storage/src/key.rs`
  - `crates/temporal-storage/src/lib.rs`
  - `crates/temporal-storage/src/record.rs`
  - `crates/temporal-storage/src/store.rs`
  - `crates/temporal-storage/src/observed_adapter.rs`
  - `crates/temporal-storage/tests/key_codec.rs`
  - `crates/temporal-storage/tests/query_adapter_metrics.rs`
- Stage only dependency-lock hunks required by those files from `Cargo.lock`; do not stage unrelated workspace-member or paper-benchmark hunks.

**Interfaces:**
- Consumes: the protected current worktree snapshot/query implementation.
- Produces: a clean Git parent containing existing canonical scan primitives, backend snapshots, Sidecar snapshot protocol, observer, history keys, records, and store errors, but no Task 4 batch/reader symbols.

- [ ] **Step 1: Prove the checkpoint contains no Task 4 implementation**

Run:

```bash
rg -n 'CanonicalBatchScan|scan_canonical_batch|HistoryReadBudget|HistoryReadStats|PointHistoryReader|PointHistoryRequest|HistoryRecordByteLimit' \
  crates/storage-api crates/adapter-memory crates/adapter-rocksdb crates/adapter-postgres \
  crates/adapter-neo4j crates/adapter-sidecar crates/temporal-storage
```

Expected: no production matches. Test or plan-document matches do not qualify and must be excluded from the checkpoint diff.

- [ ] **Step 2: Verify the current prerequisite behavior**

Run:

```bash
cargo test --locked -p storage-api --test query_primitives -- --test-threads=1
cargo test --locked -p adapter-memory --test adapter_contract -- --test-threads=1
cargo test --locked -p adapter-rocksdb --test adapter_contract -- --test-threads=1
cargo test --locked -p adapter-postgres --test sql_contract -- --test-threads=1
cargo test --locked -p adapter-sidecar --test protocol --test client --test snapshot_protocol --test stateful_snapshot -- --test-threads=1
cargo test --locked -p temporal-storage --test key_codec --test query_adapter_metrics -- --test-threads=1
```

Expected: every command exits 0 with no warnings.

- [ ] **Step 3: Stage the exact checkpoint allow-list**

Stage the files listed above by exact path. Use `git add -p Cargo.lock` and accept only lock hunks required by the staged package manifests. Do not use `git add -A`, a directory glob, or a whole-file `Cargo.lock` add.

Run:

```bash
git diff --cached --name-only
git diff --cached --check
git diff --cached -U10
```

Expected: only the allow-list and relevant `Cargo.lock` hunks; no Task 4 symbols.

- [ ] **Step 4: Commit and clean-parent certify**

```bash
git commit -m "chore: checkpoint canonical snapshot query foundation"
```

Create a temporary clean worktree at the checkpoint commit and rerun the Step 2 commands there. Expected: PASS. If a dependency is missing, stop Task 4, add only the exact missing prerequisite in a follow-up checkpoint commit, and repeat the clean-worktree test before review.

- [ ] **Step 5: Independent checkpoint review**

Review the full checkpoint range for exact path ownership, absence of Task 4 symbols, canonical-format preservation, and clean-worktree test evidence. Do not begin Task 4A1 until this review is approved.

### Task 4A1: Add the Batch Contract, Memory, and RocksDB Implementations

**Files:**
- Modify: `crates/storage-api/src/query.rs`
- Modify: `crates/storage-api/src/lib.rs`
- Modify: `crates/storage-api/tests/query_primitives.rs`
- Modify: `crates/adapter-memory/src/lib.rs`
- Create: `crates/adapter-memory/tests/canonical_batch_scan.rs`
- Modify: `crates/adapter-rocksdb/src/lib.rs`
- Create: `crates/adapter-rocksdb/tests/canonical_batch_scan.rs`

**Interfaces:**
- Produces: `MAX_CANONICAL_BATCH_RANGES`, `CanonicalBatchScanRequest`, `CanonicalBatchScanPage`, and fail-closed `ReadSnapshot::scan_canonical_batch`.

- [ ] **Step 1: Write failing storage-api contract tests**

Add tests constructing two ordered `CanonicalScanRequest`s and asserting:

```rust
assert_eq!(
    CanonicalBatchScanRequest::new(Vec::new(), 1),
    Err(QueryPrimitiveError::EmptyInput)
);
assert!(matches!(
    CanonicalBatchScanRequest::new(vec![scan.clone(), scan], 1024),
    Err(QueryPrimitiveError::DuplicateCanonicalRange)
));
assert!(matches!(
    CanonicalBatchScanRequest::new(scans, MAX_QUERY_PAGE_BYTES + 1),
    Err(QueryPrimitiveError::ByteLimitTooLarge { .. })
));
```

Also assert aggregate subrequest item/byte limits, request-order preservation, page cardinality, same applied index, and aggregate retained key-plus-value bytes.

- [ ] **Step 2: Run RED**

```bash
cargo test --locked -p storage-api --test query_primitives -- --test-threads=1
```

Expected: FAIL because batch types and `DuplicateCanonicalRange` are missing.

- [ ] **Step 3: Implement the exact batch shapes**

Add:

```rust
pub const MAX_CANONICAL_BATCH_RANGES: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalBatchScanRequest {
    scans: Vec<CanonicalScanRequest>,
    max_total_bytes: u64,
}

impl CanonicalBatchScanRequest {
    pub fn new(
        scans: Vec<CanonicalScanRequest>,
        max_total_bytes: u64,
    ) -> Result<Self, QueryPrimitiveError>;
    pub fn scans(&self) -> &[CanonicalScanRequest];
    pub const fn max_total_bytes(&self) -> u64;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanonicalBatchScanPage {
    applied_log_index: u64,
    pages: Vec<CanonicalScanPage>,
}

impl CanonicalBatchScanPage {
    pub fn new(
        request: &CanonicalBatchScanRequest,
        applied_log_index: u64,
        pages: Vec<CanonicalScanPage>,
    ) -> Result<Self, QueryPrimitiveError>;
    pub const fn applied_log_index(&self) -> u64;
    pub fn pages(&self) -> &[CanonicalScanPage];
    pub fn into_pages(self) -> Vec<CanonicalScanPage>;
}
```

Validation is exactly the boundary-amendment design: `1..=256`, duplicate by `KeySpan`, positive capped total bytes, summed subrequest bytes/items within aggregate limits, exact page/request cardinality and order, one applied index, and aggregate retained bytes.

Add `QueryPrimitiveError::DuplicateCanonicalRange` and `QueryPrimitiveError::AppliedIndexMismatch { expected, actual }` with stable display text.

- [ ] **Step 4: Add the fail-closed snapshot method**

```rust
fn scan_canonical_batch<'a>(
    &'a self,
    _request: &'a CanonicalBatchScanRequest,
) -> AdapterFuture<'a, CanonicalBatchScanPage> {
    Box::pin(async move {
        Err(AdapterError::UnsupportedOperation {
            operation: "snapshot canonical batch scan",
        })
    })
}
```

Export all new types and constants from `storage-api`.

- [ ] **Step 5: Write Memory and RocksDB failing contract tests**

For each adapter, populate two disjoint ranges and assert one `scan_canonical_batch` call returns two pages in input order, every subpage respects bounds, aggregate retained bytes are bounded, and all pages use the snapshot index. Add duplicate and continuation cases through storage-api constructors rather than adapter-specific validation.

- [ ] **Step 6: Implement Memory and RocksDB batch scans**

Implement one adapter method that loops local bounded iterators inside the call, derives each page with the existing single-range canonical scan helper, accumulates retained key-plus-value bytes with checked arithmetic, and stops before exceeding `request.max_total_bytes()`. Do not invoke `self.scan_canonical` recursively and do not create a future/RPC per range.

- [ ] **Step 7: Verify and commit**

```bash
cargo test --locked -p storage-api --test query_primitives -- --test-threads=1
cargo test --locked -p adapter-memory --test canonical_batch_scan -- --test-threads=1
cargo test --locked -p adapter-rocksdb --test canonical_batch_scan -- --test-threads=1
cargo fmt --check
git diff --check
git add crates/storage-api/src/query.rs crates/storage-api/src/lib.rs \
  crates/storage-api/tests/query_primitives.rs crates/adapter-memory/src/lib.rs \
  crates/adapter-memory/tests/canonical_batch_scan.rs crates/adapter-rocksdb/src/lib.rs \
  crates/adapter-rocksdb/tests/canonical_batch_scan.rs
git commit -m "feat: add bounded canonical batch scans"
```

Expected: all tests pass and the commit contains only the seven listed files.

### Task 4A2: Add PostgreSQL, Neo4j, Sidecar, and Observer Batch Support

**Files:**
- Modify: `crates/adapter-postgres/src/lib.rs`
- Create: `crates/adapter-postgres/tests/canonical_batch_sql.rs`
- Modify: `crates/adapter-neo4j/src/lib.rs`
- Create: `crates/adapter-neo4j/tests/canonical_batch_protocol.rs`
- Modify: `crates/adapter-sidecar/proto/dtg_adapter_v1.proto`
- Modify: `crates/adapter-sidecar/src/lib.rs`
- Modify: `crates/adapter-sidecar/src/service.rs`
- Modify: `crates/adapter-sidecar/tests/protocol.rs`
- Modify: `crates/adapter-sidecar/tests/client.rs`
- Modify: `crates/temporal-storage/src/observed_adapter.rs`
- Create: `crates/temporal-storage/tests/canonical_batch_observer.rs`

**Interfaces:**
- Consumes: Task 4A1 batch contract.
- Produces: one-statement PostgreSQL/Neo4j implementations, one Sidecar request/response, and observed batch call accounting.

- [ ] **Step 1: Write failing SQL, Cypher, Sidecar, and observer tests**

Assert PostgreSQL uses one parameterized ordinal-preserving statement, Neo4j uses one `UNWIND $ranges` query in the open transaction, and Sidecar preserves request order in one envelope. Assert Sidecar rejects peers without the appended batch feature bit. Assert nine ranges increment the observed batch counter once and the single-range counter zero times.

- [ ] **Step 2: Run RED**

```bash
cargo test --locked -p adapter-postgres --test canonical_batch_sql -- --test-threads=1
cargo test --locked -p adapter-neo4j --test canonical_batch_protocol -- --test-threads=1
cargo test --locked -p adapter-sidecar --test protocol --test client -- --test-threads=1
cargo test --locked -p temporal-storage --test canonical_batch_observer -- --test-threads=1
```

Expected: missing batch implementations, protocol messages, feature bit, and observer counter.

- [ ] **Step 3: Implement PostgreSQL and Neo4j in one statement each**

PostgreSQL binds arrays for ordinal, keyspace, start, end, required prefix, max items, and max bytes; `unnest(... WITH ORDINALITY)` feeds one lateral bounded range scan and results are regrouped by ordinal. Neo4j sends one `ranges` parameter and one `UNWIND $ranges AS range` query inside the existing transaction; returned rows carry the ordinal and are regrouped without reordering. Both validate the final `CanonicalBatchScanPage` through storage-api constructors.

- [ ] **Step 4: Extend Sidecar without renumbering**

Append new proto messages and envelope fields for batch request/response, append one new negotiated feature bit after existing bits, update hand-maintained prost structs/codecs, and enforce both `max_total_bytes` and the existing 20 MiB frame cap. Client and service each perform one framed exchange for the batch.

- [ ] **Step 5: Extend observation**

Add `canonical_batch_scans` to `AdapterCallObserver`; forward `scan_canonical_batch` once and do not increment the single-range counter for subranges.

- [ ] **Step 6: Verify and commit**

```bash
cargo test --locked -p adapter-postgres --test sql_contract --test canonical_batch_sql -- --test-threads=1
cargo test --locked -p adapter-neo4j --test canonical_batch_protocol -- --test-threads=1
cargo test --locked -p adapter-sidecar --test protocol --test client -- --test-threads=1
cargo test --locked -p temporal-storage --test canonical_batch_observer -- --test-threads=1
cargo fmt --check
git diff --check
git add crates/adapter-postgres/src/lib.rs crates/adapter-postgres/tests/canonical_batch_sql.rs \
  crates/adapter-neo4j/src/lib.rs crates/adapter-neo4j/tests/canonical_batch_protocol.rs \
  crates/adapter-sidecar/proto/dtg_adapter_v1.proto crates/adapter-sidecar/src/lib.rs \
  crates/adapter-sidecar/src/service.rs crates/adapter-sidecar/tests/protocol.rs \
  crates/adapter-sidecar/tests/client.rs crates/temporal-storage/src/observed_adapter.rs \
  crates/temporal-storage/tests/canonical_batch_observer.rs
git commit -m "feat: batch canonical scans across remote adapters"
```

Expected: all tests pass; source scans show no per-range remote single-scan loop.

### Task 4B: Implement Snapshot-Bound PointHistoryReader

**Files:**
- Create: `crates/temporal-storage/src/history_reader.rs`
- Modify: `crates/temporal-storage/src/lib.rs`
- Modify: `crates/temporal-storage/src/store.rs`
- Create: `crates/temporal-storage/tests/point_history_reader.rs`

**Interfaces:**
- Consumes: `CanonicalBatchScanRequest`, `ReadSnapshot::scan_canonical_batch`, Task 3 borrowed record views, history-key ordering.
- Produces: `HistoryReadBudget`, `HistoryReadStats`, `PropertyDemand`, `PointHistoryRequest`, `PointHistoryOutcome`, `PointHistoryReader::read`, and `read_batch`.

- [ ] **Step 1: Write failing budget, replay, grouping, and corruption tests**

Add the two original Task 4 tests plus tests for:

```rust
assert_eq!(HistoryReadBudget::new(0, 1, 1), Err(TemporalStoreError::InvalidHistoryReadBudget));
assert_eq!(HistoryReadBudget::new(16, 512, 1024), Err(TemporalStoreError::InvalidHistoryReadBudget));
```

Add fixtures proving depth 0/1/8/15 equivalence, missing anchor, per-record and total byte limits, applied-index mismatch, nonadvancing continuation, unexpected history key, key/record timestamp mismatch, nine distinct one-page requests in one batch call, and two requests sharing one `(element, transaction_time)` range but different valid times.

- [ ] **Step 2: Run RED**

```bash
cargo test --locked -p temporal-storage --test point_history_reader -- --test-threads=1
```

Expected: missing reader, budget, stats, request, outcome, and error variants.

- [ ] **Step 3: Add exact public types and errors**

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PropertyDemand<'a> { All, Selected(&'a [u32]) }

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PointHistoryRequest {
    pub element: ElementRef,
    pub transaction_time: TransactionTime,
    pub valid_time: ValidTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PointHistoryOutcome {
    pub value: Option<CanonicalElement>,
    pub stats: HistoryReadStats,
}
```

Add store errors: `InvalidHistoryReadBudget`, `HistoryRecordByteLimit`, `HistoryTotalByteLimit`, `HistoryAppliedIndexMismatch { expected, actual }`, `HistoryContinuationNotAdvancing`, `UnexpectedHistoryKey`, and `HistoryKeyTimestampMismatch`. Reuse existing `MissingHistoryAnchor` and `HistoryChainTooDeep`.

- [ ] **Step 4: Implement grouped paged replay**

Group requests by `(element, transaction_time)`, retain original output ordinals, and create at most 256 distinct ranges per batch. Each unresolved range owns its validated continuation, charged record/byte totals, matching borrowed-or-copied point deltas, and the requested valid-time ordinals. One pagination round makes one `scan_canonical_batch` call.

For each entry, validate key type/element/transaction timestamp before `HistoryEntryRef::parse`, charge `entry.value().len()` with checked arithmetic, retain only matching Put bytes or Delete markers, and stop at the anchor. Reconstruct the point value from the anchor-visible payload plus matching deltas in chronological order. Apply `PropertyDemand::Selected` only when producing the final owned `CanonicalElement`.

- [ ] **Step 5: Implement single-read delegation**

`read` constructs one `PointHistoryRequest`, calls `read_batch`, and removes exactly one outcome. It does not maintain a second replay implementation.

- [ ] **Step 6: Verify and commit**

```bash
cargo test --locked -p temporal-storage --test point_history_reader --test query_adapter_metrics --test record_ref -- --test-threads=1
cargo test --locked -p storage-api --test query_primitives -- --test-threads=1
cargo test --locked -p adapter-sidecar --test protocol --test client -- --test-threads=1
cargo fmt --check
git diff --check
git add crates/temporal-storage/src/history_reader.rs crates/temporal-storage/src/lib.rs \
  crates/temporal-storage/src/store.rs crates/temporal-storage/tests/point_history_reader.rs
git commit -m "feat: add snapshot-bound point history reader"
```

Expected: all tests pass, nine one-page ranges use one batch call, duplicate ranges are scanned once, and no old reconstruction symbol appears in `history_reader.rs`.

### Task 4 Final Gate: Whole-Slice Review

- [ ] Generate one review package from the approved checkpoint head through Task 4B.
- [ ] Review all batch implementations for aggregate bounds, applied-index equality, one physical request per round, Sidecar field stability, checked arithmetic, and fail-closed unsupported snapshots.
- [ ] Review point replay for key/value timestamp equality, continuation progress, exact byte accounting, duplicate-range fan-out, record ordering, and absence of owned decoding for nonmatching deltas.
- [ ] Run the focused Task 4A1, 4A2, and 4B commands once on the reviewed head.
- [ ] Mark original Point History Task 4 complete only after this final gate is clean.
