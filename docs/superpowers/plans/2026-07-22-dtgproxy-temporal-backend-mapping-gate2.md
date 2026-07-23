# DTGProxy Temporal Backend Mapping Gate 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Introduce the sole user-extensible `TemporalBackendMapping` SPI, validate versioned schema identities at registration/open time, route the RocksDB backend through the SPI, and prove the canonical mutation/read/snapshot contract with one reusable Mapping TCK.

**Architecture:** `StorageAdapter` remains the internal Shard runtime port so Raft, transactions, query execution, and snapshots do not change. A new `TemporalBackendMapping` is the only backend extension port. `MappingBackedAdapter` drives the required `prepare -> apply -> commit` lifecycle and forwards canonical getter/export operations; abort is mandatory on every pre-commit failure. Backend factories expose mappings through the existing registry, which validates both the runtime Adapter descriptor and the mapping schema descriptor.

**Tech Stack:** Rust 1.93, async boxed futures, existing `storage-api` canonical mutation/snapshot types, BLAKE3 schema fingerprints, RocksDB synchronous `WriteBatch`, existing Adapter Registry.

## Global Constraints

- Keep `#![forbid(unsafe_code)]` in every touched crate.
- `CommittedMutationBatch` is the only write input accepted by a Mapping.
- Acknowledgement occurs only after `commit` durably advances `applied_log_index` with replay metadata and business mutations atomically.
- No Mapping may return backend-native rows to callers; getter and export methods return canonical logical keys and bytes.
- Mapping names, versions, schema fingerprints, configuration, and capabilities fail closed before a Shard serves traffic.
- The Mapping SPI is current-only. Do not add `V2`, compatibility parsers, silent fallback, or a second backend extension interface.
- Gate 2 changes only RocksDB behavior. PostgreSQL and Neo4j keep compiling but are migrated in Gates 3 and 4.

---

## File Structure

- Create `crates/storage-api/src/mapping.rs`: descriptor, capabilities, validation errors, prepared transaction and mapping traits, and the runtime bridge.
- Modify `crates/storage-api/src/lib.rs`: expose the current Mapping API and keep canonical storage types in one crate.
- Create `crates/storage-api/tests/mapping_descriptor.rs`: version/name/fingerprint/capability validation.
- Create `crates/storage-api/tests/mapping_lifecycle.rs`: lifecycle ordering, abort, replay and canonical read/export forwarding through the bridge.
- Modify `crates/adapter-registry/src/lib.rs`: factories expose a mapping descriptor and Registry verifies it is stable before returning an opened backend.
- Modify `crates/adapter-registry/tests/registry.rs`: duplicate/unknown/incompatible mapping and descriptor-drift tests.
- Modify `crates/adapter-rocksdb/src/lib.rs`: implement `TemporalBackendMapping`; split deterministic preparation from durable commit; expose the schema fingerprint.
- Create `crates/adapter-rocksdb/tests/mapping_tck.rs`: invoke the shared Mapping TCK against RocksDB.
- Create `crates/storage-api/src/mapping_tck.rs` behind `cfg(any(test, feature = "mapping-tck"))`: reusable backend-neutral conformance cases.
- Modify `crates/storage-api/Cargo.toml` and `crates/adapter-rocksdb/Cargo.toml`: add the test-only feature wiring.
- Modify `docs/adapter-spi.md`: distinguish internal Adapter port from the public Mapping extension port and record RocksDB certification status.

### Task 1: Versioned Mapping Descriptor

**Files:**
- Create: `crates/storage-api/src/mapping.rs`
- Modify: `crates/storage-api/src/lib.rs`
- Test: `crates/storage-api/tests/mapping_descriptor.rs`

**Interfaces:**
- Produces: `MAPPING_SPI_VERSION`, `MappingCapabilities`, `MappingDescriptorV1`, `MappingRequirement`, `MappingCompatibilityError`.
- Consumed by: Registry validation, Mapping bridge, RocksDB descriptor, later PostgreSQL/Neo4j mappings.

- [ ] **Step 1: Write failing descriptor tests**

```rust
#[test]
fn mapping_descriptor_requires_current_version_and_nonzero_schema_fingerprint() {
    let descriptor = MappingDescriptorV1::new(
        "rocksdb-canonical",
        "1.0.0",
        BackendFamily::KeyValue,
        [0; 32],
        MappingCapabilities::canonical_durable(),
    );
    assert_eq!(descriptor.validate(MappingRequirement::ManagedReplica),
               Err(MappingCompatibilityError::ZeroSchemaFingerprint));
}
```

- [ ] **Step 2: Verify RED**

Run: `cargo test -p storage-api --test mapping_descriptor -- --nocapture`

Expected: compile failure because Mapping descriptor types do not exist.

- [ ] **Step 3: Implement the descriptor**

```rust
pub const MAPPING_SPI_VERSION: u16 = 1;

pub struct MappingDescriptorV1 {
    spi_version: u16,
    name: String,
    version: String,
    family: BackendFamily,
    schema_fingerprint: [u8; 32],
    capabilities: MappingCapabilities,
}
```

Validation must reject zero/unsupported versions, invalid canonical names, empty or control-containing versions, zero fingerprints, and missing managed/hot-pluggable capabilities.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p storage-api --test mapping_descriptor -- --nocapture`

Expected: every descriptor validation test passes.

### Task 2: Atomic Mapping Lifecycle and Runtime Bridge

**Files:**
- Modify: `crates/storage-api/src/mapping.rs`
- Test: `crates/storage-api/tests/mapping_lifecycle.rs`

**Interfaces:**
- Produces: `TemporalBackendMapping`, `PreparedMappingTransaction`, `MappingBackedAdapter`.
- Consumes: `CommittedMutationBatch`, `LogicalKey`, `KeySpan`, snapshot types, `AdapterFuture` and `AdapterError`.

- [ ] **Step 1: Write failing lifecycle tests**

```rust
#[test]
fn bridge_aborts_when_apply_fails_and_never_commits() {
    let mapping = RecordingMapping::fail_during_apply();
    let adapter = MappingBackedAdapter::new(Arc::new(mapping.clone())).unwrap();
    assert!(block_on(adapter.apply_committed(batch(1))).is_err());
    assert_eq!(mapping.events(), ["prepare", "apply", "abort"]);
}
```

Also test success ordering, prepare failure, commit failure, duplicate replay forwarding, getter ordering, exact scan error forwarding, export forwarding, and applied-index forwarding.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p storage-api --test mapping_lifecycle -- --nocapture`

Expected: compile failure because lifecycle traits and bridge do not exist.

- [ ] **Step 3: Implement object-safe lifecycle traits**

```rust
pub trait PreparedMappingTransaction: Send {
    fn apply<'a>(&'a mut self) -> MappingFuture<'a, ()>;
    fn commit<'a>(&'a mut self) -> MappingFuture<'a, ApplyReceipt>;
    fn abort<'a>(self: Box<Self>) -> MappingFuture<'a, ()> where Self: 'a;
}

pub trait CanonicalRestoreSession: Send {
    fn write_chunk<'a>(&'a mut self, chunk: LogicalSnapshotChunkV1)
        -> MappingFuture<'a, ()>;
    fn commit<'a>(&'a mut self, manifest: LogicalSnapshotManifestV1)
        -> MappingFuture<'a, ()>;
    fn abort<'a>(self: Box<Self>) -> MappingFuture<'a, ()> where Self: 'a;
}

pub trait TemporalBackendMapping: Send + Sync {
    fn describe_schema(&self) -> MappingDescriptorV1;
    fn validate_mapping(&self) -> Result<(), MappingError>;
    fn prepare<'a>(&'a self, batch: CommittedMutationBatch)
        -> MappingFuture<'a, Box<dyn PreparedMappingTransaction + 'a>>;
    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey])
        -> MappingFuture<'a, Vec<Option<Vec<u8>>>>;
    fn scan<'a>(&'a self, span: &'a KeySpan) -> MappingFuture<'a, Vec<KeyValue>>;
    fn export_canonical<'a>(&'a self, request: LogicalSnapshotExportRequest)
        -> MappingFuture<'a, Box<dyn LogicalSnapshotReader + 'a>>;
    fn restore_canonical<'a>(&'a self, header: LogicalSnapshotHeaderV1)
        -> MappingFuture<'a, Box<dyn CanonicalRestoreSession + 'a>>;
    fn applied_log_index(&self) -> Result<u64, MappingError>;
}
```

`MappingBackedAdapter::apply_committed` retains the transaction object through `commit`, calls
`abort` exactly once after any successful prepare whose apply or commit path fails, and drops it
without abort only after a successful durable commit.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p storage-api --test mapping_lifecycle -- --nocapture`

Expected: lifecycle event sequences and canonical forwarding pass.

### Task 3: Registry Mapping Validation

**Files:**
- Modify: `crates/adapter-registry/src/lib.rs`
- Test: `crates/adapter-registry/tests/registry.rs`

**Interfaces:**
- Produces: `AdapterFactory::mapping_descriptor`, `OpenedAdapter::mapping_descriptor`.
- Consumes: `MappingDescriptorV1::validate`.

- [ ] **Step 1: Write failing Registry tests**

Add tests proving open fails before publication for an unsupported Mapping SPI, zero schema fingerprint, mismatched backend family, and a descriptor that changes between factory declaration and opened mapping.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p adapter-registry --test registry mapping -- --nocapture`

Expected: compile failure because Registry does not expose Mapping descriptors.

- [ ] **Step 3: Add Registry checks**

The factory declares the descriptor it intends to open. Registry validates it before calling `open`, then validates the opened Adapter's Mapping descriptor and requires exact equality. `OpenedAdapter` retains this descriptor so migration fencing binds a generation to one schema mapping.

- [ ] **Step 4: Verify GREEN**

Run: `cargo test -p adapter-registry --test registry -- --nocapture`

Expected: all existing Registry and new Mapping validation tests pass.

### Task 4: RocksDB Mapping Migration

**Files:**
- Modify: `crates/adapter-rocksdb/src/lib.rs`
- Test: `crates/adapter-rocksdb/tests/mapping_tck.rs`

**Interfaces:**
- Produces: RocksDB implementation of `TemporalBackendMapping` with schema name `rocksdb-canonical`, version `1.0.0`, and a deterministic nonzero fingerprint over Keyspace/column-family/layout rules.
- Consumes: Task 2 lifecycle traits and Task 3 Registry validation.

- [ ] **Step 1: Write failing RocksDB lifecycle test**

Test that `prepare` performs all replay/sequence validation without changing the DB, `apply` builds/stages the WriteBatch without visibility, `abort` leaves the prior applied index and values untouched, and `commit` atomically publishes values, fingerprints and applied index.

- [ ] **Step 2: Verify RED**

Run: `cargo test -p adapter-rocksdb --test mapping_tck rocksdb_mapping_lifecycle -- --nocapture`

Expected: compile failure because RocksDB does not implement the Mapping SPI.

- [ ] **Step 3: Split RocksDB prepare and commit**

Move deterministic batch validation and `WriteBatch` construction into a prepared transaction
without backend side effects. `apply` completes staging. `commit` acquires the Adapter apply guard,
revalidates the durable applied index and replay fingerprints against the state observed during
prepare, then performs one synchronous WAL-enabled `write_opt`. `abort` drops the staged batch
without writing. No non-`Send` mutex guard is retained across an async boundary.

- [ ] **Step 4: Route the factory through `MappingBackedAdapter`**

The RocksDB factory returns the Mapping-backed runtime Adapter. No direct alternative factory path remains.

- [ ] **Step 5: Verify GREEN**

Run: `cargo test -p adapter-rocksdb -- --test-threads=1`

Expected: all pre-existing RocksDB tests and the new Mapping lifecycle tests pass.

### Task 5: Reusable Mapping TCK

**Files:**
- Create: `crates/storage-api/src/mapping_tck.rs`
- Modify: `crates/storage-api/Cargo.toml`
- Modify: `crates/adapter-rocksdb/Cargo.toml`
- Modify: `crates/adapter-rocksdb/tests/mapping_tck.rs`

**Interfaces:**
- Produces: `run_mapping_tck(open, reopen, restore)` test harness used unchanged by RocksDB, PostgreSQL and Neo4j.
- Consumes: `TemporalBackendMapping` only; it must not call backend-specific APIs.

- [ ] **Step 1: Add TCK cases**

The harness must cover exact replay, replay mismatch, non-contiguous index, duplicate mutation sequence, multi-get cardinality, bytewise scan order, keyspace isolation, delete/history retention, failed-batch invisibility, restart recovery, export/restore/continue, and byte-identical canonical snapshot output.

- [ ] **Step 2: Run TCK against RocksDB**

Run: `cargo test -p adapter-rocksdb --test mapping_tck -- --test-threads=1`

Expected: every shared case passes without a RocksDB-specific assertion branch.

- [ ] **Step 3: Run Gate 2 verification**

Run:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p storage-api -p adapter-registry -p adapter-rocksdb -- --test-threads=1
```

Then run strict Clippy for the same packages and targeted `rustfmt --check` for touched files.

Expected: tests and targeted checks pass; PostgreSQL and Neo4j continue compiling against the internal runtime port but are not yet Mapping-certified.

### Task 6: Documentation and Gate Evidence

**Files:**
- Modify: `docs/adapter-spi.md`
- Modify: `.superpowers/sdd/progress.md`

- [ ] **Step 1: Record exact status vocabulary**

Document three distinct states: `implemented`, `compiled`, and `live-certified`. Mark RocksDB Mapping implemented and contract-tested; PostgreSQL/Neo4j current canonical adapters compiled but not Mapping/live-certified.

- [ ] **Step 2: Self-review**

Search for placeholders, `V2`, parallel extension traits, direct RocksDB factory bypasses, and backend-specific branches in the shared TCK. Fix every result before declaring Gate 2 complete.
