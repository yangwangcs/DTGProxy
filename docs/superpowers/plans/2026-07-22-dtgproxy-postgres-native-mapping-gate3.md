# DTGProxy PostgreSQL Native Temporal Mapping Gate 3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the PostgreSQL canonical-table Adapter with a native temporal graph Mapping that atomically persists current entities, bitemporal history, adjacency, replay metadata, and applied-index state while reconstructing byte-identical canonical KV.

**Architecture:** `TemporalBackendMapping` remains the only backend extension port. The core temporal codec exposes a typed, lossless view of graph key/value pairs; PostgreSQL Mapping converts that view into fixed, parameterized native tables and reconstructs canonical bytes through the same codec. Replica metadata and every business mutation commit in one `SERIALIZABLE` PostgreSQL transaction with `synchronous_commit=on`; restore uses an unpublished generation and publishes only after the canonical manifest is verified.

**Tech Stack:** Rust 1.93, `postgres` 0.19 synchronous client behind the existing bounded pool, `temporal-storage` key/record codecs, BLAKE3 schema fingerprints, reusable `storage-api` Mapping TCK, local ephemeral PostgreSQL test instance.

## Global Constraints

- `TemporalBackendMapping` is the sole user-extensible backend SPI; do not add a second extension trait or a compatibility/V2 path.
- `CommittedMutationBatch` is the only Mapping write input; Raft, transaction, Cypher, analytics, and Shard code remain backend-neutral.
- Canonical key/value bytes are the interchange and verification contract; PostgreSQL native getters/exporters must reproduce the exact keyspace, key bytes, and value bytes.
- Acknowledgement follows only a durable commit that atomically advances replay metadata and `applied_log_index` with all business rows.
- Every SQL statement is static and parameterized; identifiers are compile-time schema names and user configuration never enters SQL text.
- PostgreSQL schema fingerprint, mapping capabilities, and opened-instance descriptor must match the factory declaration before a Shard serves traffic.
- Native tables are authoritative. Do not add a PostgreSQL `canonical_kv` shadow table to make tests pass.
- Failed apply, failed restore, divergent replay, non-contiguous log index, and schema drift are fail-closed and leave no partially visible business state.
- No `V2`, fallback parser, silent compatibility mode, or direct `StorageAdapter` extension path is introduced.

---

## File Structure

- Create `crates/temporal-storage/src/canonical.rs`: public lossless decoding/encoding helpers for graph key/value pairs and opaque non-graph keyspaces.
- Modify `crates/temporal-storage/src/lib.rs`: export the current canonical helper API.
- Create `crates/temporal-storage/tests/canonical_mapping.rs`: exact round-trip tests for identity, current, history, adjacency, and opaque keyspaces.
- Modify `crates/adapter-postgres/Cargo.toml`: depend on `temporal-storage` and the existing `blake3` workspace dependency if required by the fingerprint implementation.
- Modify `crates/adapter-postgres/src/lib.rs`: replace `canonical_kv` schema and operations with native Mapping implementation, prepared transaction lifecycle, native reads/scans, logical export/restore, and Mapping-backed factory output.
- Create `crates/adapter-postgres/tests/native_mapping.rs`: RED/GREEN tests for schema, lifecycle, getter reconstruction, history/adjacency, and atomic invisibility.
- Modify `crates/adapter-postgres/tests/sql_contract.rs`: assert native tables and reject the old canonical shadow table.
- Modify `crates/adapter-postgres/tests/live_postgres.rs`: remove `#[ignore]`, use the disposable local/CI server contract, run the shared Mapping TCK and native cross-backend checks.
- Create `crates/adapter-postgres/tests/support/ephemeral.rs` only if the existing repository test harness cannot share a server fixture without global state.
- Modify `crates/storage-api/src/mapping_tck.rs` only when a backend-neutral case exposes a real Gate 3 contract gap; do not add PostgreSQL branches.
- Modify `docs/postgresql-adapter.md`, `docs/adapter-spi.md`, and `.superpowers/sdd/progress.md` with exact implemented/compiled/live-certified evidence.

## Task 1: Lossless Canonical Graph Codec Surface

**Files:**

- Create `crates/temporal-storage/src/canonical.rs`
- Modify `crates/temporal-storage/src/lib.rs`
- Test `crates/temporal-storage/tests/canonical_mapping.rs`

**Interfaces:**

- Produces `CanonicalGraphKey`, `CanonicalGraphValue`, `decode_canonical_graph_entry`, and `encode_canonical_graph_entry` (current API only).
- Consumes existing `GraphKey`, `decode_graph_key`, `VertexIdentity`, `EdgeIdentity`, `ProjectionRecord`, `HistoryEntry`, and `LogicalKey`.
- Later PostgreSQL Mapping uses these helpers; the helper must never change canonical bytes.

- [ ] **Step 1: Write failing round-trip tests**

```rust
#[test]
fn graph_entries_decode_to_typed_records_and_reencode_byte_identically() {
    let key = current_vertex_key(vertex(7, 2, 11));
    let value = projection_bytes();
    let typed = decode_canonical_graph_entry(&key, &value).unwrap();
    assert_eq!(encode_canonical_graph_entry(&typed).unwrap(), (key, value));
}

#[test]
fn opaque_keyspaces_are_preserved_without_a_backend_specific_parser() {
    let key = LogicalKey::in_keyspace(Keyspace::Txn, b"opaque".to_vec());
    let value = vec![9, 8, 7];
    let typed = decode_canonical_graph_entry(&key, &value).unwrap();
    assert_eq!(encode_canonical_graph_entry(&typed).unwrap(), (key, value));
}
```

- [ ] **Step 2: Run RED**

Run `cargo test -p temporal-storage --test canonical_mapping -- --nocapture`.

Expected result: compilation fails because the current lossless canonical helper API does not exist.

- [ ] **Step 3: Implement the minimal typed view**

Use an enum with explicit variants for each graph key family and an `Opaque { keyspace, key, value }` variant. Decode only the existing current codecs; reject a mismatched value with a typed `CanonicalMappingError`. Encoding must call the existing record `encode` methods and return the original key bytes for every variant.

- [ ] **Step 4: Run GREEN**

Run `cargo test -p temporal-storage --test canonical_mapping -- --nocapture` and then `cargo test -p temporal-storage -- --test-threads=1`.

- [ ] **Step 5: Commit**

Commit as `feat(storage): expose lossless canonical mapping codec`.

## Task 2: PostgreSQL Native Schema and Descriptor

**Files:**

- Modify `crates/adapter-postgres/src/lib.rs`
- Modify `crates/adapter-postgres/tests/sql_contract.rs`
- Create `crates/adapter-postgres/tests/native_mapping.rs`

**Interfaces:**

- Produces the sole current PostgreSQL schema (`POSTGRES_SCHEMA_VERSION = 1`) and a deterministic `postgresql-native-temporal/1.0.0` Mapping descriptor.
- Consumes `MappingDescriptorV1`, `MappingCapabilities`, `BackendFamily::Sql`, and Task 1 codec.

- [ ] **Step 1: Write failing schema tests**

Assert that `POSTGRES_SCHEMA` creates these authoritative tables and indexes:

```text
schema_meta
adapter_instance
vertex_identity
edge_identity
vertex_current
edge_current
vertex_history
edge_history
out_adjacency
in_adjacency
opaque_records
replay_log
replay_mutation
```

Assert that the schema contains no `canonical_kv` table and that every table is scoped by `instance_id` with stable primary keys. Assert the descriptor fingerprint is nonzero and changes when the schema layout changes.

- [ ] **Step 2: Run RED**

Run `cargo test -p adapter-postgres --test sql_contract native -- --nocapture`.

Expected result: tests fail because the old schema has only `canonical_kv` and no Mapping descriptor.

- [ ] **Step 3: Implement static native schema**

Use byte-preserving `BYTEA` fields for canonical identifiers and payloads, typed `BIGINT/INTEGER/UUID-like BYTEA` fields only where the existing codec exposes fixed-width values, and explicit valid/transaction interval columns. Store history rows with their original canonical history key components and encoded value; store adjacency rows with direction, local/remote partition, endpoint, edge type, bucket, edge identity, and encoded projection. Store Meta/Txn/TemporalIndex entries in `opaque_records` keyed by `(instance_id, keyspace, logical_key)`; replay rows remain separate and are updated in the same transaction. Compute the schema fingerprint from a versioned static layout string. Delete the old `POSTGRES_SCHEMA_V1` symbol and direct canonical-table layout; no legacy schema parser or migration branch remains. Existing databases without the exact fingerprint fail startup and require explicit canonical export/restore into a fresh generation.

- [ ] **Step 4: Run GREEN**

Run the native SQL contract and descriptor tests.

- [ ] **Step 5: Commit**

Commit as `feat(postgres): define native temporal mapping schema`.

## Task 3: PostgreSQL Prepared Mapping Lifecycle

**Files:**

- Modify `crates/adapter-postgres/src/lib.rs`
- Test `crates/adapter-postgres/tests/native_mapping.rs`

**Interfaces:**

- Produces `PostgresTemporalMapping` implementing `TemporalBackendMapping` and a `PostgresPreparedTransaction` implementing `PreparedMappingTransaction`.
- Factory `open` returns `MappingBackedAdapter`; all writes go through `prepare -> apply -> commit -> abort`.

- [ ] **Step 1: Write failing lifecycle tests**

Cover:

```rust
fn apply_and_commit_make_native_rows_visible_and_advance_index_atomically() { /* ... */ }
fn abort_after_apply_failure_leaves_current_history_adjacency_and_index_unchanged() { /* ... */ }
fn exact_replay_is_duplicate_but_different_replay_is_rejected() { /* ... */ }
fn non_contiguous_index_and_duplicate_sequence_fail_before_any_native_row_changes() { /* ... */ }
```

Use a real disposable PostgreSQL connection for all tests that inspect row visibility; do not mock SQL execution.

- [ ] **Step 2: Run RED**

Run `cargo test -p adapter-postgres --test native_mapping -- --nocapture`.

Expected result: compilation or assertion failure because the Mapping implementation and native lifecycle are absent.

- [ ] **Step 3: Implement deterministic preparation**

During `prepare`, validate applied index, log fingerprint, mutation sequence, key/value decoding, and native row conflicts without writing business rows. Materialize a bounded `PreparedNativeMutation` list containing parameter values and the inverse/read-set needed to revalidate at commit. Reject malformed graph keys/values and unsupported opaque keys with `AdapterError::Backend`.

- [ ] **Step 4: Implement one-transaction apply/commit**

`apply` completes staging only. `commit` locks the instance row, revalidates the durable frontier and replay fingerprints, executes every native upsert/delete plus replay rows and applied index in one `SERIALIZABLE` transaction, sets local synchronous commit, and commits. `abort` only drops staged rows. No acknowledgement is returned before commit succeeds.

- [ ] **Step 5: Route the factory through `MappingBackedAdapter`**

Factory-declared and opened Mapping descriptors must compare exactly through Registry. Remove the direct `StorageAdapter` write/read path after the Mapping-backed path is live.

- [ ] **Step 6: Run GREEN**

Run the native lifecycle tests, the existing PostgreSQL unit tests, and the storage/registry Mapping tests.

- [ ] **Step 7: Commit**

Commit as `feat(postgres): implement native temporal mapping lifecycle`.

## Task 4: Native Getters, Ordered Scan, and Canonical Export/Restore

**Files:**

- Modify `crates/adapter-postgres/src/lib.rs`
- Test `crates/adapter-postgres/tests/native_mapping.rs`

**Interfaces:**

- `multi_get` and `scan` return only canonical `Vec<u8>` values through the Mapping SPI.
- `export_canonical` walks all keyspaces in strict `(keyspace, logical_key)` order.
- `restore_canonical` writes native rows into an unpublished instance and publishes only after manifest verification.

- [ ] **Step 1: Write failing getter/export tests**

Apply a vertex, edge, cross-partition edge, delete, and multiple history versions. Assert every original canonical key/value pair is returned byte-for-byte by `multi_get`, `scan`, and export. Restore into a fresh PostgreSQL instance and assert the exported manifest, query-visible rows, and continued log index are equal.

- [ ] **Step 2: Run RED**

Run `cargo test -p adapter-postgres --test native_mapping getters -- --nocapture`.

Expected result: failure because reads still target the removed canonical shadow table or native reconstruction is incomplete.

- [ ] **Step 3: Implement native reconstruction**

For each native row, rebuild its canonical `LogicalKey` with existing key constructors and rebuild its canonical value with existing record encoders. Merge opaque rows and replay/meta rows into the same ordered stream. `multi_get` preserves request cardinality and order; `scan` enforces `KeySpan` byte limits and ordering.

- [ ] **Step 4: Implement restore**

Restore chunks transactionally into the hidden instance, reject divergent duplicate content, verify snapshot ordinal/digests through `LogicalSnapshotAccumulator`, write the applied-index record, and publish once. Abort removes every row for the unpublished instance.

- [ ] **Step 5: Run GREEN**

Run the native getter/export/restore tests and the shared Mapping TCK unchanged against PostgreSQL.

- [ ] **Step 6: Commit**

Commit as `feat(postgres): reconstruct canonical snapshots from native tables`.

## Task 5: Repeated Graph Semantics and Migration Equivalence

**Files:**

- Modify `crates/adapter-postgres/tests/native_mapping.rs`
- Modify `crates/adapter-postgres/tests/live_postgres.rs`
- Modify `crates/adapter-rocksdb/tests/mapping_tck.rs` only for backend-neutral fixture improvements

- [ ] **Step 1: Add failing native semantic tests**

Assert current vertex/edge reads, valid-time history reconstruction, incoming/outgoing adjacency symmetry, cross-partition adjacency, deletion/history retention, restart recovery, and failed-batch invisibility. Compare canonical snapshots and fixed temporal query fixtures against RocksDB.

- [ ] **Step 2: Run RED**

Run the targeted live suite against a disposable PostgreSQL server.

- [ ] **Step 3: Fix only native mapping defects**

Do not add PostgreSQL branches to Temporal Cypher, transaction, analytics, or the shared TCK; fix the Mapping or core codec.

- [ ] **Step 4: Run GREEN**

Run RocksDB ↔ PostgreSQL export/restore in both directions, the shared TCK, and all current PostgreSQL tests.

- [ ] **Step 5: Commit**

Commit as `test(postgres): certify native temporal mapping equivalence`.

## Task 6: Real PostgreSQL Certification and Documentation

**Files:**

- Modify `crates/adapter-postgres/tests/live_postgres.rs`
- Create or modify repository CI/service configuration for a disposable PostgreSQL instance
- Modify `docs/postgresql-adapter.md`, `docs/adapter-spi.md`, `.superpowers/sdd/progress.md`

- [ ] **Step 1: Write the live certification command**

Use a fixed PostgreSQL major version and a disposable database. The test must fail loudly when the service is unavailable; it must not be marked `#[ignore]`. Local developers can start the same version with the repository-provided fixture command.

- [ ] **Step 2: Run the live suite**

Run the full PostgreSQL package, the shared Mapping TCK, and the RocksDB ↔ PostgreSQL migration matrix against the real instance.

- [ ] **Step 3: Record evidence**

Mark PostgreSQL `live-certified` only with command output showing the real instance, schema fingerprint, Mapping TCK, restart, export/restore, and migration checks. Keep `compiled` and `implemented` labels separate for any remaining Neo4j work.

- [ ] **Step 4: Commit**

Commit as `docs(postgres): record native mapping certification evidence`.

## Gate 3 Verification

Run:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p temporal-storage -p storage-api -p adapter-registry -p adapter-rocksdb -p adapter-postgres -- --test-threads=1
```

Then run strict Clippy for the same packages and `cargo fmt --check`. Finally run the real PostgreSQL certification command and confirm no PostgreSQL test remains ignored. Gate 3 is not complete until the opened factory Mapping descriptor, native schema, shared TCK, restart, logical migration, and live test all have fresh evidence.
