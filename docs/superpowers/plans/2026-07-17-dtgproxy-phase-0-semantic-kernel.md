# DTGProxy Phase 0 Semantic Kernel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the first executable Rust vertical slice of DTGProxy: bitemporal primitives, a model-oracle for retroactive corrections, deterministic canonical property encoding, a versioned Storage Adapter contract, an atomic in-memory reference adapter, and a runnable `dtgproxy` binary.

**Architecture:** Keep temporal semantics independent from storage and networking. `temporal-types` owns pure value types and codecs; `temporal-model` is the executable semantic oracle; `storage-api` defines committed physical mutations; `adapter-memory` proves atomic/idempotent apply semantics; the CLI only exposes build identity in this phase. This is Phase 0 of the approved full design, not a substitute for RocksDB, Raft, 2PC, or distributed query phases.

**Tech Stack:** Rust 1.93, edition 2024, Cargo workspace resolver 3, standard library only for Phase 0, built-in Rust test harness.

## Global Constraints

- Product name is `DTGProxy`; Cargo package names use lowercase kebab-case.
- Rust is the only core implementation language.
- All valid-time intervals are half-open `[from, to)`; `None` represents `+infinity`.
- Transaction time is system-owned and ordered lexicographically by `(physical_micros, logical)`.
- No production function is added without first observing its behavior test fail.
- Production crates forbid `unsafe` code.
- Durable/canonical encoding is explicit and versioned; Rust memory layout and `HashMap` iteration order are never persisted.
- Phase 0 has no RocksDB, network, Raft, or async-runtime dependency.

---

## File Structure

```text
Cargo.toml                         workspace members, package defaults, lints
.gitignore                        Rust/editor artifacts
README.md                         product scope and commands
crates/temporal-types/            time, interval, visibility, canonical values
crates/temporal-model/            in-memory bitemporal model oracle
crates/storage-api/               adapter capability and mutation contract
crates/adapter-memory/            atomic/idempotent reference adapter
crates/dtgproxy/                   executable entry point
docs/superpowers/specs/            approved detailed design, renamed DTGProxy
```

### Task 1: Workspace, Branding, and Version-Control Baseline

**Files:**
- Create: `.gitignore`
- Create: `Cargo.toml`
- Create: `README.md`
- Create: each crate `Cargo.toml` and an empty `src/lib.rs` or `src/main.rs`
- Rename: `docs/superpowers/specs/2026-07-17-distributed-bitemporal-graph-middleware-design.md` to `docs/superpowers/specs/2026-07-17-dtgproxy-design.md`
- Modify: renamed design, replacing the old four-letter working name with `DTGProxy`

**Interfaces:**
- Consumes: approved design document.
- Produces: a buildable workspace containing packages `temporal-types`, `temporal-model`, `storage-api`, `adapter-memory`, and `dtgproxy`.

- [x] **Step 1: Initialize a feature branch because the directory is not yet a Git repository**

Run: `git init -b feature/dtgproxy-phase0`

Expected: a new repository whose current branch is `feature/dtgproxy-phase0`, never `main` or `master`.

- [x] **Step 2: Add workspace configuration**

```toml
[workspace]
members = [
    "crates/temporal-types",
    "crates/temporal-model",
    "crates/storage-api",
    "crates/adapter-memory",
    "crates/dtgproxy",
]
resolver = "3"

[workspace.package]
version = "0.1.0"
edition = "2024"
rust-version = "1.93"
license = "Apache-2.0"

[workspace.lints.rust]
unsafe_code = "forbid"

[workspace.lints.clippy]
all = "warn"
```

Each library manifest inherits workspace package fields and lints. `temporal-model` depends on `temporal-types`; `adapter-memory` depends on `storage-api`.

- [x] **Step 3: Add branding and ignore rules**

`.gitignore` contains `/target/`, `.DS_Store`, `.idea/`, and `.vscode/`. `README.md` states that DTGProxy is a distributed bitemporal property-graph middleware, lists the five Phase 0 crates, and documents `cargo test --workspace` and `cargo run -p dtgproxy -- --version`.

- [x] **Step 4: Rename the design and replace the old working name mechanically**

Run: `mv docs/superpowers/specs/2026-07-17-distributed-bitemporal-graph-middleware-design.md docs/superpowers/specs/2026-07-17-dtgproxy-design.md`, then replace every exact occurrence of the old four-letter working name with `DTGProxy`.

Expected: `rg -n '\bD[T]GM\b' .` returns no matches outside `.git`.

- [x] **Step 5: Verify the empty baseline builds**

Run: `cargo test --workspace`

Expected: exit 0 with zero behavioral tests.

- [x] **Step 6: Commit the baseline**

Run: `git add . && git commit -m "chore: initialize DTGProxy Rust workspace"`

### Task 2: Bitemporal Primitive Types

**Files:**
- Create: `crates/temporal-types/tests/interval_visibility.rs`
- Create: `crates/temporal-types/src/interval.rs`
- Create: `crates/temporal-types/src/time.rs`
- Create: `crates/temporal-types/src/version.rs`
- Modify: `crates/temporal-types/src/lib.rs`

**Interfaces:**
- Produces: `Interval<T>`, `IntervalError`, `ValidTime`, `TransactionTime`, and `BitemporalVersion<T>`.

- [x] **Step 1: Write the failing behavior tests**

```rust
use temporal_types::{BitemporalVersion, Interval, TransactionTime, ValidTime};

fn valid(value: i64) -> ValidTime { ValidTime::from_micros(value) }
fn tx(value: i64) -> TransactionTime { TransactionTime::new(value, 0) }

#[test]
fn interval_is_half_open_and_rejects_empty_ranges() {
    let interval = Interval::new(valid(10), Some(valid(20))).unwrap();
    assert!(interval.contains(valid(10)));
    assert!(interval.contains(valid(19)));
    assert!(!interval.contains(valid(20)));
    assert!(Interval::new(valid(10), Some(valid(10))).is_err());
}

#[test]
fn bitemporal_visibility_requires_both_dimensions() {
    let version = BitemporalVersion::new(
        "risk=high",
        Interval::new(valid(10), Some(valid(20))).unwrap(),
        Interval::new(tx(100), Some(tx(200))).unwrap(),
    );
    assert!(version.is_visible_at(valid(15), tx(150)));
    assert!(!version.is_visible_at(valid(20), tx(150)));
    assert!(!version.is_visible_at(valid(15), tx(200)));
}

#[test]
fn subtracting_an_inner_interval_returns_two_residuals() {
    let source = Interval::new(valid(1), Some(valid(10))).unwrap();
    let cut = Interval::new(valid(4), Some(valid(7))).unwrap();
    assert_eq!(source.subtract(&cut), vec![
        Interval::new(valid(1), Some(valid(4))).unwrap(),
        Interval::new(valid(7), Some(valid(10))).unwrap(),
    ]);
}
```

- [x] **Step 2: Run RED**

Run: `cargo test -p temporal-types --test interval_visibility`

Expected: compile failure because the public temporal types do not exist.

- [x] **Step 3: Implement the minimal types**

`Interval<T>` stores `start: T` and `end: Option<T>`, validates `end > start`, and implements the tested `contains` and `subtract` behavior. `ValidTime` is an ordered `i64` microsecond newtype. `TransactionTime` stores `physical_micros: i64` plus `logical: u32` and derives total ordering. `BitemporalVersion<T>` owns its value plus valid and transaction intervals and implements `is_visible_at` as the conjunction of both interval checks. Additional accessors are introduced only with the model-oracle tests that consume them.

- [x] **Step 4: Run GREEN and lint**

Run: `cargo test -p temporal-types --test interval_visibility && cargo clippy -p temporal-types --all-targets -- -D warnings`

Expected: 4 tests pass and Clippy exits 0.

- [x] **Step 5: Commit**

Run: `git add crates/temporal-types && git commit -m "feat: add bitemporal primitive types"`

### Task 3: Executable Bitemporal Model Oracle

**Files:**
- Create: `crates/temporal-model/tests/retroactive_correction.rs`
- Create: `crates/temporal-model/src/timeline.rs`
- Modify: `crates/temporal-model/src/lib.rs`

**Interfaces:**
- Consumes: `Interval<ValidTime>`, `TransactionTime`, `BitemporalVersion<T>`.
- Produces: `Timeline<T>`, `CorrectionSummary`, and `TimelineError`.

- [x] **Step 1: Write the failing correction test**

```rust
use temporal_model::Timeline;
use temporal_types::{Interval, TransactionTime, ValidTime};

fn valid(value: i64) -> ValidTime { ValidTime::from_micros(value) }
fn tx(value: i64) -> TransactionTime { TransactionTime::new(value, 0) }
fn interval(from: i64, to: i64) -> Interval<ValidTime> {
    Interval::new(valid(from), Some(valid(to))).unwrap()
}

#[test]
fn retroactive_correction_preserves_old_knowledge_and_splits_new_knowledge() {
    let mut timeline = Timeline::new();
    timeline.put_initial(interval(1, 10), "A".to_owned(), tx(100)).unwrap();
    let summary = timeline
        .correct(interval(4, 7), "B".to_owned(), tx(150), tx(200))
        .unwrap();

    assert_eq!(summary.closed_versions, 1);
    assert_eq!(summary.opened_versions, 3);
    assert_eq!(timeline.value_at(valid(5), tx(150)).unwrap().map(String::as_str), Some("A"));
    assert_eq!(timeline.value_at(valid(2), tx(250)).unwrap().map(String::as_str), Some("A"));
    assert_eq!(timeline.value_at(valid(5), tx(250)).unwrap().map(String::as_str), Some("B"));
    assert_eq!(timeline.value_at(valid(8), tx(250)).unwrap().map(String::as_str), Some("A"));
}

#[test]
fn stale_writer_conflicts_with_a_later_overlapping_commit() {
    let mut timeline = Timeline::new();
    timeline.put_initial(interval(1, 10), "A".to_owned(), tx(100)).unwrap();
    timeline.correct(interval(4, 7), "B".to_owned(), tx(150), tx(180)).unwrap();
    let error = timeline
        .correct(interval(5, 6), "C".to_owned(), tx(150), tx(200))
        .unwrap_err();
    assert_eq!(error.to_string(), "write conflict after transaction snapshot");
}
```

- [x] **Step 2: Run RED**

Run: `cargo test -p temporal-model --test retroactive_correction`

Expected: compile failure because `Timeline` is not implemented.

- [x] **Step 3: Implement the model**

`Timeline<T: Clone>` owns the versions needed by the semantic oracle. `put_initial` rejects overlap with a version visible at its commit timestamp. `correct` rejects `commit_ts <= read_ts`, detects any overlapping version whose transaction start is in `(read_ts, commit_ts)`, closes every overlapping version visible at `read_ts`, opens residual valid-time segments at `commit_ts`, and opens exactly one replacement segment over the correction interval. `value_at` returns an invariant error if more than one version is visible. Durable version IDs remain the responsibility of the later mutation protocol, so the Phase 0 oracle does not invent an untested ID API.

- [x] **Step 4: Run GREEN and all temporal tests**

Run: `cargo test -p temporal-model && cargo test -p temporal-types`

Expected: all tests pass.

- [ ] **Step 5: Commit**

Run: `git add crates/temporal-model && git commit -m "feat: add bitemporal model oracle"`

### Task 4: Deterministic Canonical Property Encoding

**Files:**
- Create: `crates/temporal-types/tests/canonical_codec.rs`
- Create: `crates/temporal-types/src/value.rs`
- Modify: `crates/temporal-types/src/lib.rs`

**Interfaces:**
- Produces: `GraphValue`, `CanonicalElement`, `CodecError`, `encode`, and `decode`.

- [ ] **Step 1: Write the failing round-trip tests**

```rust
use std::collections::BTreeMap;
use temporal_types::{CanonicalElement, GraphValue};

#[test]
fn canonical_payload_round_trips_without_type_loss() {
    let properties = BTreeMap::from([
        (1, GraphValue::Integer(-7)),
        (2, GraphValue::String("risk".to_owned())),
        (3, GraphValue::Bytes(vec![0, 1, 255])),
        (4, GraphValue::List(vec![GraphValue::Boolean(true), GraphValue::Null])),
    ]);
    let element = CanonicalElement::new(9, properties);
    let bytes = element.encode();
    assert_eq!(CanonicalElement::decode(&bytes).unwrap(), element);
}

#[test]
fn canonical_encoding_is_independent_of_insertion_order() {
    let left = CanonicalElement::new(1, BTreeMap::from([
        (7, GraphValue::Integer(8)),
        (2, GraphValue::String("x".to_owned())),
    ]));
    let right = CanonicalElement::new(1, BTreeMap::from([
        (2, GraphValue::String("x".to_owned())),
        (7, GraphValue::Integer(8)),
    ]));
    assert_eq!(left.encode(), right.encode());
}

#[test]
fn decoder_rejects_trailing_or_truncated_data() {
    let value = CanonicalElement::new(1, BTreeMap::new());
    let mut trailing = value.encode();
    trailing.push(0);
    assert!(CanonicalElement::decode(&trailing).is_err());
    assert!(CanonicalElement::decode(&value.encode()[..5]).is_err());
}
```

- [ ] **Step 2: Run RED**

Run: `cargo test -p temporal-types --test canonical_codec`

Expected: compile failure because canonical types do not exist.

- [ ] **Step 3: Implement versioned canonical encoding**

Use magic bytes `DTP1`, big-endian integers, a schema-version `u64`, a property-count `u32`, sorted property IDs, explicit one-byte value tags, and `u32` length prefixes. Supported values are Null, Boolean, Integer, FloatBits, String, Bytes, TimestampMicros, and recursive List. Decoder bounds-checks every read, validates UTF-8, rejects unknown tags, caps recursion at 64, and rejects trailing bytes.

- [ ] **Step 4: Run GREEN and regression tests**

Run: `cargo test -p temporal-types && cargo clippy -p temporal-types --all-targets -- -D warnings`

Expected: all temporal-type tests pass without warnings.

- [ ] **Step 5: Commit**

Run: `git add crates/temporal-types && git commit -m "feat: add canonical graph value codec"`

### Task 5: Storage Adapter Contract and Atomic In-Memory Adapter

**Files:**
- Create: `crates/storage-api/src/lib.rs`
- Create: `crates/adapter-memory/tests/adapter_contract.rs`
- Create: `crates/adapter-memory/src/lib.rs`

**Interfaces:**
- Produces from `storage-api`: `AdapterCapabilities`, `LogicalKey`, `Mutation`, `CommittedMutationBatch`, `ApplyReceipt`, `AdapterError`, `AdapterFuture`, and `StorageAdapter`.
- Produces from `adapter-memory`: `MemoryAdapter`.

- [ ] **Step 1: Define the contract types and write failing adapter tests**

The trait is object-safe and has these exact operations:

```rust
pub trait StorageAdapter: Send + Sync {
    fn capabilities(&self) -> AdapterCapabilities;
    fn apply_committed<'a>(
        &'a self,
        batch: CommittedMutationBatch,
    ) -> AdapterFuture<'a, ApplyReceipt>;
    fn multi_get<'a>(
        &'a self,
        keys: &'a [LogicalKey],
    ) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>>;
    fn applied_log_index(&self) -> Result<u64, AdapterError>;
}
```

Tests create a `MemoryAdapter`, apply log index 1 containing a Put, verify the value and applied index, replay the identical batch and require `duplicate == true`, reject the same `(txn_id, mutation_sequence)` with different bytes without changing data or index, and reject a log-index gap.

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-memory --test adapter_contract`

Expected: compile failure because `MemoryAdapter` and the contract implementation do not exist.

- [ ] **Step 3: Implement atomic and idempotent application**

`MemoryAdapter` stores a `Mutex<State>` containing the key-value map, current applied log index, per-log-index batch fingerprints, and per-`(txn_id, sequence)` mutation fingerprints. It validates a complete batch before cloning and mutating the map, swaps the new state only after every mutation succeeds, returns a duplicate receipt for an identical committed-log replay, and returns typed errors for mismatched replay, non-contiguous index, duplicate mutation sequence, or poisoned lock. Fingerprints use an explicit FNV-1a implementation over tagged length-delimited bytes; `DefaultHasher` is forbidden.

- [ ] **Step 4: Run GREEN and workspace regression**

Run: `cargo test -p adapter-memory && cargo test --workspace && cargo clippy --workspace --all-targets -- -D warnings`

Expected: all tests pass without warnings.

- [ ] **Step 5: Commit**

Run: `git add crates/storage-api crates/adapter-memory && git commit -m "feat: add storage adapter contract"`

### Task 6: Executable CLI and Phase 0 Acceptance

**Files:**
- Create: `crates/dtgproxy/tests/cli.rs`
- Modify: `crates/dtgproxy/src/main.rs`
- Modify: `README.md`

**Interfaces:**
- Produces: executable `dtgproxy --version` behavior.

- [ ] **Step 1: Write the failing CLI test**

```rust
use std::process::Command;

#[test]
fn version_reports_product_name_and_workspace_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_dtgproxy"))
        .arg("--version")
        .output()
        .unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "DTGProxy 0.1.0\n");
}
```

- [ ] **Step 2: Run RED**

Run: `cargo test -p dtgproxy --test cli`

Expected: failure because the binary does not yet return the required identity.

- [ ] **Step 3: Implement the minimal CLI**

`--version` and `-V` print `DTGProxy {CARGO_PKG_VERSION}` to stdout and exit 0. No arguments print a concise Phase 0 status plus usage. Unknown arguments print an error plus usage to stderr and exit 2.

- [ ] **Step 4: Run acceptance verification**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -q -p dtgproxy -- --version
rg -n '\bD[T]GM\b|T[B]D|T[O]DO|F[I]XME' --glob '!target/**' .
git status --short
```

Expected: formatting, Clippy, and tests exit 0; version output is `DTGProxy 0.1.0`; search has no matches; Git status only contains intentionally uncommitted Task 6 files before the commit.

- [ ] **Step 5: Commit**

Run: `git add crates/dtgproxy README.md docs && git commit -m "feat: deliver DTGProxy phase zero kernel"`

- [ ] **Step 6: Record actual evidence**

Update this plan's completed checkboxes only after each command has produced its expected result. Do not mark the overall DTGProxy goal complete: RocksDB persistence, consensus, distributed transactions, query execution, external adapters, analytics, and production operations remain Phase 1–6 work.
