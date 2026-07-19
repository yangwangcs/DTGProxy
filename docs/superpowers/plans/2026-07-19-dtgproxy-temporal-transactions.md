# DTGProxy Cypher Temporal Transactions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** Implement Cypher read-your-writes, bitemporal update lowering, auto/explicit distributed transactions, MERGE/constraint correctness, serializable validation, and recovery over the existing home-shard 2PC protocol.

**Architecture:** Statements update a bounded canonical overlay. At commit, the overlay is validated against one schema/topology snapshot, split by shard, and lowered into existing temporal storage mutations. The current home-shard decision engine remains the durability authority; protocol records gain versioned dependency metadata without changing old record decoding.

**Tech Stack:** Rust 1.93, existing temporal-storage, txn-protocol, timestamp-oracle, Raft runtime, deterministic fault injection.

## Global Constraints

- TSO assigns commit time after successful prewrite proofs; clients cannot forge it.
- Backdated valid-time corrections append transaction history.
- Commit/abort/recovery are idempotent under duplicate delivery.
- Constraint and MERGE keys are globally deterministic and route to stable shards.
- Explicit transaction resources are bounded and cleaned on disconnect/timeout.

---

### Task 1: Canonical transaction overlay

**Files:**
- Create: `crates/temporal-storage/src/overlay.rs`
- Modify: `crates/temporal-storage/src/lib.rs`
- Test: `crates/temporal-storage/tests/{overlay,read_your_writes,backdated_update}.rs`

1. Write failing tests for create/update/delete composition, repeated SET/REMOVE, detach delete, relationship endpoint validation, read-your-writes, rollback, and backdated correction.
2. Implement `TransactionOverlay`, statement savepoints, stable element deltas, property patches, read composition, byte/item limits, and final canonical temporal transaction generation.
3. Run `cargo test -p temporal-storage --test overlay --test read_your_writes --test backdated_update`; expect pass.
4. Commit: `feat(txn): add bitemporal transaction overlay`.

### Task 2: Lower Cypher write operators

**Files:**
- Create: `crates/query-executor/src/v2/write.rs`
- Create: `crates/query-executor/src/v2/merge.rs`
- Test: `crates/query-executor/tests/{cypher_create,cypher_set_remove,cypher_delete,cypher_merge,foreach}.rs`

1. Write operator tests for CREATE, MERGE, SET, REMOVE, DELETE, DETACH DELETE, FOREACH, and CALL subquery writes.
2. Implement row-to-overlay lowering with stable generated identities, label/type checks, and statement atomicity.
3. Verify failed statements roll back only their savepoint while explicit transactions remain usable where Cypher rules allow.
4. Run write-operator tests; expect pass.
5. Commit: `feat(cypher): lower write clauses into temporal overlay`.

### Task 3: Constraint and dependency metadata

**Files:**
- Modify: `crates/txn-protocol/src/model.rs`
- Modify: `crates/txn-protocol/src/codec.rs`
- Modify: `crates/txn-protocol/src/participant.rs`
- Create: `crates/txn-protocol/src/dependency.rs`
- Test: `crates/txn-protocol/tests/{record_compat,constraint_conflict,serializable_dependencies}.rs`

1. Add failing v1 decode compatibility tests before extending records.
2. Add `PrewriteMetadataV2` with point-read versions, range fingerprints, constraint keys, and schema/topology fencing.
3. Validate uniqueness/constraint keys and serializable dependencies during prewrite inspection; keep TemporalSnapshot behavior unchanged.
4. Run `cargo test -p txn-protocol`; expect old and new tests pass.
5. Commit: `feat(txn): validate constraints and serializable dependencies`.

### Task 4: Session transaction coordinator integration

**Files:**
- Modify: `crates/dtgproxy/src/transaction.rs`
- Modify: `crates/gateway-node/src/session.rs`
- Modify: `crates/gateway-node/src/bookmark.rs`
- Test: `tests/{autocommit_temporal_tx,explicit_temporal_tx,bookmark_causal_read}.rs`

1. Write failing tests proving auto-commit is one transaction, explicit statements share an overlay/snapshot, rollback discards changes, and bookmarks fence later reads.
2. Split final overlay mutations by placement and call current single-shard fast path or multi-shard home 2PC.
3. Return commit-derived bookmarks and enforce bookmark catch-up/deadlines on later reads.
4. Run transaction integration tests; expect pass.
5. Commit: `feat(txn): connect Cypher sessions to distributed temporal commit`.

### Task 5: Crash/retry/rebalance matrix

**Files:**
- Create: `tests/temporal_tx_fault_matrix.rs`
- Modify: existing fault harnesses under `crates/dtgproxy/tests/`

1. Inject crash before/after participant prewrite, home decision, participant finalize, client acknowledgement, and cleanup.
2. Inject duplicate messages, timeout, leader transfer, stale placement epoch, schema change, and rebalance at every legal boundary.
3. Assert exactly one durable outcome, monotonic transaction time, no partial visible overlay, and recovery convergence.
4. Run the matrix for both isolation levels and both deployment modes.
5. Commit: `test(txn): certify temporal transaction recovery matrix`.

### Task 6: Backend semantic matrix

**Files:**
- Create: `tests/temporal_tx_backend_equivalence.rs`
- Modify: adapter-specific integration fixtures only where capability implementation is missing.

1. Execute identical creates, updates, deletes, MERGE races, uniqueness conflicts, backdated corrections, history reads, and retry sequences on memory/RocksDB/Neo4j/PostgreSQL.
2. Compare canonical current state, history, transaction records, and commit receipts.
3. Fix adapter deviations without adding backend-specific language semantics.
4. Commit: `test(txn): prove temporal write equivalence across backends`.
