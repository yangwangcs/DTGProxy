# Point History Budget Correction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve multi-range batching for legal large record budgets and ensure PointHistoryReader owns record-versus-total history byte-limit errors across pagination.

**Architecture:** Each unresolved history range reserves one maximum inspectable record plus its fixed history-key overhead per round, rather than reserving every remaining record. The reader maps a first-entry adapter byte rejection to the history record limit, while cumulative value-only accounting remains in `charge_record`. Tests use a byte-enforcing snapshot so adapter and reader quota boundaries are exercised rather than simulated away.

**Tech Stack:** Rust 2024, `storage-api` canonical batch snapshots, `temporal-storage`, Prost-compatible Sidecar framing, Neo4j Query API Cypher, Cargo tests.

## Global Constraints

- Preserve canonical Current, Anchor/Delta, adjacency, and applied-index formats.
- Use one real `ReadSnapshot` and one physical batch operation per pagination round.
- Group duplicate `(element, transaction_time)` requests and preserve original output order.
- Count history budgets using encoded record value bytes; adapter page bounds count key plus value bytes.
- Do not add aliases, feature flags, dual reads, dual writes, legacy fallbacks, or old reconstruction calls.
- Preserve unrelated dirty-worktree changes and stage only the paths listed by each task.
- Keep `#![forbid(unsafe_code)]` and checked quota arithmetic.

---

### Task 1: Correct PointHistory batch reservation and quota classification

**Files:**
- Modify: `crates/temporal-storage/tests/point_history_reader.rs`
- Modify: `crates/temporal-storage/src/history_reader.rs`

**Interfaces:**
- Consumes: `HistoryReadBudget`, `CanonicalBatchScanRequest`, `QueryPageBounds`, `AdapterError::ScanByteLimit`, `MAX_QUERY_PAGE_BYTES`.
- Produces: one-record-per-range round reservation and stable `HistoryRecordByteLimit` / `HistoryTotalByteLimit` outcomes.

- [ ] **Step 1: Make ScriptedSnapshot enforce byte-bounded pages**

Replace the item-only page slice in `scan_canonical_batch` with the same bounded accumulation used by production adapters:

```rust
let mut entries = Vec::new();
let mut retained = 0_u64;
let mut next_start = None;
for entry in matching {
    if entries.len() == self.page_items.min(scan.bounds().max_items()) {
        next_start = Some(entry.key().clone());
        break;
    }
    let entry_bytes = u64::try_from(entry.key().as_bytes().len())
        .unwrap()
        .checked_add(u64::try_from(entry.value().len()).unwrap())
        .unwrap();
    let required = retained.checked_add(entry_bytes).unwrap();
    if required > scan.bounds().max_bytes() {
        if entries.is_empty() {
            return Err(AdapterError::ScanByteLimit {
                limit: scan.bounds().max_bytes(),
                required,
            });
        }
        next_start = Some(entry.key().clone());
        break;
    }
    retained = required;
    entries.push(entry);
}
```

- [ ] **Step 2: Add failing batching and cross-page quota tests**

Change `nine_distinct_one_page_requests_use_one_batch_call` to use:

```rust
HistoryReadBudget::new(
    16,
    storage_api::MAX_QUERY_PAGE_BYTES,
    1024 * 1024,
)
```

Add two tests that force the reader onto a second page:

```rust
#[test]
fn oversized_record_after_continuation_is_a_history_record_limit() {
    let element = vertex(151);
    let snapshot = ScriptedSnapshot::new(vec![
        anchor(element, 100, vec![(interval(0, 10), payload(&"a".repeat(900)))]),
        put(element, 200, interval(20, 30), payload("small")),
    ])
    .with_page_items(1);

    assert_eq!(
        block_on(
            PointHistoryReader::new(HistoryReadBudget::new(16, 4096, 512).unwrap()).read(
                &snapshot,
                element,
                tx(200),
                valid(5),
                PropertyDemand::All,
            )
        ),
        Err(TemporalStoreError::HistoryRecordByteLimit)
    );
}

#[test]
fn cumulative_bytes_after_continuation_are_a_history_total_limit() {
    let element = vertex(152);
    let entries = vec![
        anchor(element, 100, vec![(interval(0, 10), payload(&"a".repeat(220)))]),
        put(element, 200, interval(20, 30), payload(&"b".repeat(220))),
    ];
    let total = entries
        .iter()
        .map(|entry| u64::try_from(entry.value().len()).unwrap())
        .sum::<u64>();
    let max_record = entries
        .iter()
        .map(|entry| u64::try_from(entry.value().len()).unwrap())
        .max()
        .unwrap();
    let snapshot = ScriptedSnapshot::new(entries).with_page_items(1);

    assert_eq!(
        block_on(
            PointHistoryReader::new(
                HistoryReadBudget::new(16, total - 1, max_record).unwrap(),
            )
            .read(&snapshot, element, tx(200), valid(5), PropertyDemand::All)
        ),
        Err(TemporalStoreError::HistoryTotalByteLimit)
    );
}
```

- [ ] **Step 3: Run RED tests**

Run:

```bash
cargo test --locked -p temporal-storage --test point_history_reader \
  nine_distinct_one_page_requests_use_one_batch_call \
  -- --exact --test-threads=1
cargo test --locked -p temporal-storage --test point_history_reader \
  oversized_record_after_continuation_is_a_history_record_limit \
  -- --exact --test-threads=1
cargo test --locked -p temporal-storage --test point_history_reader \
  cumulative_bytes_after_continuation_are_a_history_total_limit \
  -- --exact --test-threads=1
```

Expected: the first test reports more than one batch call; the quota tests return an adapter/scan byte-limit error instead of the expected history error.

- [ ] **Step 4: Reserve one inspectable record per range**

In `PointHistoryReader::build_batch`, retain `remaining_records` for the item bound, remove `key_overhead`, `remaining_value_bytes`, and multiplication by `remaining_records`, and compute:

```rust
let requested_bytes = self
    .budget
    .max_record_bytes
    .checked_add(key_bytes)
    .ok_or(TemporalStoreError::HistoryTotalByteLimit)?
    .min(MAX_QUERY_PAGE_BYTES);
```

Continue adding ranges until the checked aggregate would exceed `MAX_QUERY_PAGE_BYTES`; leave unfinished ranges for the next pagination round.

- [ ] **Step 5: Keep public quota classification in the reader**

Replace the direct `?` on `scan_canonical_batch` with:

```rust
let page = match read.scan_canonical_batch(&request).await {
    Ok(page) => page,
    Err(AdapterError::ScanByteLimit { .. }) => {
        return Err(TemporalStoreError::HistoryRecordByteLimit);
    }
    Err(error) => return Err(TemporalStoreError::Adapter(error)),
};
```

Because each subrequest now has one value-record allowance plus its key, aggregate retained bytes cannot exceed the sum of subrequest allowances; an adapter byte rejection on this path means the first entry cannot fit the record inspection allowance. Individually fitting entries reach `charge_record`, which retains cumulative total-byte ownership.

- [ ] **Step 6: Run GREEN and focused regressions**

Run:

```bash
cargo test --locked -p temporal-storage --test point_history_reader -- --test-threads=1
```

Expected: every point-history test passes, including one batch call for the 1 MiB nine-range case and the two exact history quota errors.

- [ ] **Step 7: Commit the reader correction**

```bash
git add crates/temporal-storage/src/history_reader.rs \
  crates/temporal-storage/tests/point_history_reader.rs
git diff --cached --check
git commit -m "fix: preserve point history quota semantics"
```

---

### Task 2: Harden Sidecar field and Neo4j query contracts

**Files:**
- Modify: `crates/adapter-sidecar/tests/protocol.rs`
- Modify: `crates/adapter-neo4j/src/lib.rs`
- Modify: `crates/adapter-neo4j/tests/canonical_batch_protocol.rs`

**Interfaces:**
- Consumes: Sidecar framed protobuf body layout and `CANONICAL_BATCH_SCAN_CYPHER`.
- Produces: exact top-level field-number assertions and a three-column Neo4j batch result.

- [ ] **Step 1: Add a top-level protobuf field parser and strengthen assertions**

Add a test helper that consumes protobuf keys and skips wire types 0, 1, 2, and 5 from the frame body (`frame[28..frame.len() - 4]`), returning top-level field numbers. Assert that the request contains field 21 and the response contains field 23:

```rust
assert!(top_level_fields(&request[28..request.len() - 4]).contains(&21));
assert!(top_level_fields(&response[28..response.len() - 4]).contains(&23));
```

The helper must fail the test on truncated varints, unsupported wire types, or out-of-range lengths rather than searching for marker bytes.

- [ ] **Step 2: Run the exact Sidecar protocol test**

Run:

```bash
cargo test --locked -p adapter-sidecar --test protocol \
  canonical_batch_feature_and_envelope_fields_are_append_only \
  -- --exact --test-threads=1
```

Expected: PASS with exact field parsing.

- [ ] **Step 3: Change the Neo4j contract assertion and verify RED**

Change the contract test to:

```rust
assert!(!query.contains("range.max_bytes"));
```

Run:

```bash
cargo test --locked -p adapter-neo4j --test canonical_batch_protocol \
  canonical_batch_scan_is_one_ordinal_preserving_unwind_query \
  -- --exact --test-threads=1
```

Expected: FAIL because the query still returns `range.max_bytes`.

- [ ] **Step 4: Remove the unused Neo4j result column**

Change `CANONICAL_BATCH_SCAN_CYPHER` to:

```cypher
RETURN range.ordinal AS ordinal, record.logical_key_hex AS logical_key_hex,
       record.value_base64 AS value_base64
...
RETURN ordinal, logical_key_hex, value_base64
```

Remove `"max_bytes": scan.bounds().max_bytes()` from the per-range JSON object.

The existing client-side `bounded_canonical_page` remains the actual byte-bound enforcement point.

- [ ] **Step 5: Run Neo4j and Sidecar focused tests**

Run:

```bash
cargo test --locked -p adapter-neo4j --test canonical_batch_protocol -- --test-threads=1
cargo test --locked -p adapter-sidecar --test protocol --test client -- --test-threads=1
```

Expected: all focused contract and client tests pass.

- [ ] **Step 6: Commit contract hardening**

```bash
git add crates/adapter-sidecar/tests/protocol.rs \
  crates/adapter-neo4j/src/lib.rs \
  crates/adapter-neo4j/tests/canonical_batch_protocol.rs
git diff --cached --check
git commit -m "test: harden canonical batch contracts"
```

---

### Task 3: Re-run the Task 4 whole-slice gate

**Files:**
- Modify: `.superpowers/sdd/point-history-task-4b-report.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Consumes: corrected Task 4 head and focused test suites.
- Produces: reproducible verification evidence for an independent whole-slice re-review.

- [ ] **Step 1: Run the complete focused gate**

Run:

```bash
cargo test --locked -p storage-api --test query_primitives -- --test-threads=1
cargo test --locked -p adapter-memory --test canonical_batch_scan -- --test-threads=1
cargo test --locked -p adapter-rocksdb --test canonical_batch_scan -- --test-threads=1
cargo test --locked -p adapter-postgres --lib --tests --no-fail-fast -- --test-threads=1
cargo test --locked -p adapter-neo4j --lib --tests --no-fail-fast -- --test-threads=1
cargo test --locked -p adapter-sidecar --test protocol --test client -- --test-threads=1
cargo test --locked -p temporal-storage --test point_history_reader \
  --test query_adapter_metrics --test record_ref -- --test-threads=1
cargo fmt --all -- --check
git diff --check
```

Expected: all focused tests pass; formatting and diff checks exit 0.

- [ ] **Step 2: Record exact evidence**

Append the commands, pass counts, commit range, and the two corrected whole-slice issues to `.superpowers/sdd/point-history-task-4b-report.md`. Change `.superpowers/sdd/progress.md` from the open whole-slice gate to “whole-slice re-review pending” with the corrected head SHA.

- [ ] **Step 3: Commit the verification record**

```bash
git add .superpowers/sdd/point-history-task-4b-report.md .superpowers/sdd/progress.md
git diff --cached --check
git commit -m "docs: record point history whole-slice fixes"
```

- [ ] **Step 4: Request independent whole-slice re-review**

Review from base `49823af` to the corrected head. Require a read-only assessment of batching, quota classification, Sidecar tags, Neo4j query shape, test evidence, path scope, and readiness to continue to the next PointHistory/Snapshot CSR task.
