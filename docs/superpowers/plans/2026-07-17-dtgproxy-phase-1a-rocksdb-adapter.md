# DTGProxy Phase 1A RocksDB Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the first durable DTGProxy backend: an atomic, idempotent, keyspace-aware RocksDB Storage Adapter with restart recovery and checkpoint support.

**Architecture:** Extend `LogicalKey` with a stable `Keyspace` so the same committed-mutation protocol can target the eight Column Families from the approved design. `RocksAdapter` serializes apply admission, validates durable replay metadata, writes logical changes plus `applied_log_index` in one cross-CF `WriteBatch`, and serves consistent multi-key reads from a RocksDB snapshot. Checkpoints use the RocksDB Checkpoint API and are verified by opening the resulting database.

**Tech Stack:** Rust 1.93, edition 2024, `rocksdb` 0.24.0 with `default-features = false` and features `lz4`, `multi-threaded-cf`, `bindgen-runtime`; `tempfile` 3 for integration tests.

## Global Constraints

- Phase 0 remains green throughout this plan.
- The eight durable keyspaces are Meta, Identity, Current, AdjOut, AdjIn, History, TemporalIndex, and Txn.
- Column Family names and keyspace tags are stable storage-format constants.
- WAL remains enabled; committed batches use synchronous writes.
- Business mutations and `applied_log_index` are committed in one `WriteBatch`.
- Identical log replay is a no-op; changed log or mutation replay is rejected.
- `unsafe` remains confined to the audited upstream RocksDB FFI crate; DTGProxy code forbids it.
- This plan delivers the durable adapter only. Temporal Current/History materialization and query APIs are Phase 1B, followed by Raft and distributed transactions.

Primary references:

- [rust-rocksdb 0.24.0](https://github.com/rust-rocksdb/rust-rocksdb/tree/v0.24.0)
- [RocksDB Basic Operations](https://github.com/facebook/rocksdb/wiki/Basic-Operations)
- [RocksDB Checkpoint API](https://github.com/rust-rocksdb/rust-rocksdb/blob/v0.24.0/src/checkpoint.rs)

---

### Task 1: Stable Keyspace Contract

**Files:**
- Create: `crates/storage-api/tests/keyspace.rs`
- Modify: `crates/storage-api/src/lib.rs`
- Modify: `crates/adapter-memory/src/lib.rs`
- Modify: `crates/adapter-memory/tests/adapter_contract.rs`

**Interfaces:**
- Produces: `Keyspace`, `Keyspace::tag`, `Keyspace::column_family`, `LogicalKey::in_keyspace`, and `LogicalKey::keyspace`.

- [x] **Step 1: Write failing keyspace tests**

```rust
use storage_api::{Keyspace, LogicalKey};

#[test]
fn keyspace_tags_and_column_families_are_stable() {
    assert_eq!(Keyspace::Meta.tag(), 0);
    assert_eq!(Keyspace::Current.column_family(), "current");
    assert_eq!(Keyspace::History.column_family(), "history");
    assert_eq!(Keyspace::Txn.column_family(), "txn");
}

#[test]
fn logical_key_defaults_to_current_and_can_select_history() {
    let current = LogicalKey::new(b"same".to_vec());
    let history = LogicalKey::in_keyspace(Keyspace::History, b"same".to_vec());
    assert_eq!(current.keyspace(), Keyspace::Current);
    assert_eq!(history.keyspace(), Keyspace::History);
    assert_ne!(current, history);
}
```

Add a memory-adapter contract test that stores the same byte key in Current and History, reads both independently, and proves replay fingerprints include the keyspace tag.

- [x] **Step 2: Run RED**

Run: `cargo test -p storage-api --test keyspace`

Expected: compile failure because `Keyspace` and the new constructors do not exist.

- [x] **Step 3: Implement the keyspace contract**

`Keyspace` is a `#[repr(u8)]` ordered enum with explicit tags 0 through 7 and stable CF names `meta`, `identity`, `current`, `adj_out`, `adj_in`, `history`, `temporal_index`, and `txn`. `LogicalKey::new` remains source-compatible and selects Current. Memory-adapter fingerprints prepend the keyspace tag before length-delimited key bytes.

- [x] **Step 4: Run GREEN and regressions**

Run: `cargo test -p storage-api && cargo test -p adapter-memory && cargo clippy --workspace --all-targets -- -D warnings`

- [x] **Step 5: Commit**

Run: `git add crates/storage-api crates/adapter-memory && git commit -m "feat: add stable storage keyspaces"`

### Task 2: RocksDB Bootstrap and Column Families

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/adapter-rocksdb/Cargo.toml`
- Create: `crates/adapter-rocksdb/src/lib.rs`
- Create: `crates/adapter-rocksdb/tests/open.rs`

**Interfaces:**
- Consumes: `StorageAdapter`, `Keyspace`.
- Produces: `RocksAdapter::open(path)` and `RocksAdapter::path()`.

- [x] **Step 1: Write the failing open test**

```rust
use adapter_rocksdb::RocksAdapter;
use storage_api::StorageAdapter;

#[test]
fn open_creates_all_required_column_families() {
    let directory = tempfile::tempdir().unwrap();
    let adapter = RocksAdapter::open(directory.path()).unwrap();
    assert_eq!(adapter.path(), directory.path());
    assert!(adapter.capabilities().local_atomic_batch);
    assert!(adapter.capabilities().idempotent_apply);
    assert_eq!(adapter.column_family_names().unwrap(), vec![
        "adj_in", "adj_out", "current", "default", "history", "identity",
        "meta", "temporal_index", "txn",
    ]);
}
```

- [x] **Step 2: Run RED**

Run: `cargo test -p adapter-rocksdb --test open`

Expected: package or import failure because the adapter crate does not exist.

- [x] **Step 3: Add the crate and minimal open implementation**

Use `DBWithThreadMode<MultiThreaded>`, `Options::create_if_missing(true)`, `create_missing_column_families(true)`, and one `ColumnFamilyDescriptor` per DTGProxy keyspace. Map all RocksDB errors to `AdapterError::Backend(String)` without inspecting English strings for retry policy.

- [x] **Step 4: Run GREEN**

Run: `cargo test -p adapter-rocksdb --test open`

Expected: the C++ dependency builds and the open test passes.

Local evidence: macOS 27 Command Line Tools could not resolve the standard C++ header `cstdint`; a direct compiler probe isolated the environment fault. The verified build used `CXX=/opt/homebrew/opt/llvm/bin/clang++` and `LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib`, after which both open tests passed.

- [ ] **Step 5: Commit**

Run: `git add Cargo.toml Cargo.lock crates/adapter-rocksdb && git commit -m "feat: bootstrap RocksDB adapter"`

### Task 3: Atomic Durable Apply and Snapshot Reads

**Files:**
- Create: `crates/adapter-rocksdb/tests/adapter_contract.rs`
- Modify: `crates/adapter-rocksdb/src/lib.rs`
- Modify: `crates/storage-api/src/lib.rs`

**Interfaces:**
- Implements: all `StorageAdapter` methods for `RocksAdapter`.
- Adds: `AdapterError::Backend(String)`.

- [ ] **Step 1: Write failing RocksDB contract tests**

Port the six Phase 0 memory-adapter contract behaviors to a temporary RocksDB directory and add a seventh keyspace-isolation case. Every test uses the real database and the standard-library `block_on` helper; no mocks are permitted.

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-rocksdb --test adapter_contract`

Expected: failures because apply, reads, replay metadata, and applied index are not implemented.

- [ ] **Step 3: Implement durable application**

Reserve these metadata keys:

```text
meta CF:  0x00 | "applied_log_index"
txn CF:   0x01 | log_index_be             -> batch_fingerprint_be
txn CF:   0x02 | txn_id_be | sequence_be  -> mutation_fingerprint_be
```

Under an apply mutex, read and validate replay metadata, validate the complete batch, then construct one `WriteBatch` containing every business Put/Delete, new replay fingerprints, and the new applied index. Commit with `WriteOptions::set_sync(true)` and WAL enabled. `multi_get` takes a RocksDB snapshot before reading keys so a single call cannot mix applied states.

- [ ] **Step 4: Run GREEN and cross-adapter tests**

Run: `cargo test -p adapter-rocksdb && cargo test -p adapter-memory && cargo clippy --workspace --all-targets -- -D warnings`

- [ ] **Step 5: Commit**

Run: `git add crates/storage-api crates/adapter-rocksdb && git commit -m "feat: apply committed batches to RocksDB"`

### Task 4: Restart Recovery and Checkpoint

**Files:**
- Create: `crates/adapter-rocksdb/tests/recovery.rs`
- Modify: `crates/adapter-rocksdb/src/lib.rs`

**Interfaces:**
- Produces: `RocksAdapter::checkpoint(destination)`.

- [ ] **Step 1: Write failing recovery tests**

Test A applies Current and History values, drops the adapter, reopens the same path, and verifies both values plus `applied_log_index`. Test B creates a checkpoint after log index 1, applies log index 2 to the source, opens the checkpoint as a separate adapter, and proves the checkpoint sees index 1 but not index 2.

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-rocksdb --test recovery`

Expected: restart may pass, while checkpoint compilation fails because the method is absent.

- [ ] **Step 3: Implement checkpoint creation**

Construct `rocksdb::checkpoint::Checkpoint` from the live DB and call `create_checkpoint(destination)`. Reject an existing destination through the typed backend error. Do not disable WAL or use a non-zero flush threshold.

- [ ] **Step 4: Run GREEN**

Run: `cargo test -p adapter-rocksdb --test recovery`

- [ ] **Step 5: Commit**

Run: `git add crates/adapter-rocksdb && git commit -m "feat: add RocksDB recovery checkpoints"`

### Task 5: Phase 1A Acceptance

**Files:**
- Modify: `README.md`
- Modify: this plan with actual evidence.

- [ ] **Step 1: Document the durable adapter and native build requirement**

README names RocksDB 0.24.0, explains the eight CFs, states that clang/libclang are required, and gives the exact adapter test command.

- [ ] **Step 2: Run complete verification**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo run -q -p dtgproxy -- --version
git diff --check
```

- [ ] **Step 3: Commit and record evidence**

Run: `git add README.md docs && git commit -m "docs: record RocksDB adapter verification"`.

Record exact test counts, tool results, implementation commit hashes, and the remaining Phase 1B–6 scope. Do not mark the overall DTGProxy goal complete.
