# DTGProxy T-Cypher Canonical Event Log Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `CHANGES` read immutable, backend-independent canonical temporal events rather than reconstructed state.

**Architecture:** A committed temporal mutation writes a self-contained event record in the same storage batch as its identity, projection, history, and adjacency data. Event scans are bounded by graph, axis window, snapshot, rows, and bytes. The query executor receives typed event batches and applies all T-Cypher semantics; adapters only persist and scan canonical event keys.

**Tech Stack:** Rust 2024, `temporal-storage`, `storage-api`, `query-executor`, `distributed-query`, `cypher-engine`, `temporal-types`.

## Global Constraints

- `CHANGES` returns immutable PUT/DELETE events and never reconstructs state.
- Valid-time changes select `valid_from` in `[from,to)`; system-time changes select commit HLC in `[from,to)`.
- Every event carries graph, element identity, operation, valid interval, commit HLC, ordinal, and PUT payload or DELETE absence.
- The event write is atomic with the canonical temporal mutation and must be idempotent on replay.
- Backends do not receive raw T-Cypher and cannot choose a snapshot or event semantics.
- All scans and batches have explicit row and byte bounds.
- The existing history chain remains the state-reconstruction mechanism and is not reused as the event source.

---

### Task 1: Canonical Event Key and Codec

**Files:**
- Modify: `crates/temporal-storage/src/key.rs`
- Modify: `crates/temporal-storage/src/record.rs`
- Modify: `crates/temporal-storage/src/lib.rs`
- Test: `crates/temporal-storage/tests/event_codec.rs`

**Interfaces:**
- Produces `CanonicalTemporalEvent`, `TemporalOperationKind`, `temporal_event_key`, and `temporal_event_graph_prefix`.
- `CanonicalTemporalEvent::new(element, operation, valid, commit, ordinal, payload, metadata)` rejects an invalid element/payload combination.
- `CanonicalTemporalEvent::encode` and `decode` preserve byte-identical event data and verify checksums.

- [ ] Write failing codec tests for PUT, DELETE, edge endpoints, graph-prefixed ordering, checksum corruption, and invalid DELETE payloads.
- [ ] Run `cargo test -p temporal-storage --test event_codec`; expect missing event types and keys.
- [ ] Add a distinct event key tag and graph/axis prefixes. Encode `commit HLC + ordinal` in sortable order and include all event fields in the checksummed value.
- [ ] Re-export only the public event type and key helpers required by query execution.
- [ ] Run `cargo test -p temporal-storage --test event_codec`; expect all cases to pass.
- [ ] Commit `feat(storage): encode canonical temporal events`.

### Task 2: Atomic Event Emission on Temporal Writes

**Files:**
- Modify: `crates/temporal-storage/src/store.rs`
- Test: `crates/temporal-storage/tests/event_commit.rs`

**Interfaces:**
- Consumes `VertexMutation`, `EdgeMutation`, `CommitContext`, and the Task 1 event codec.
- Produces one canonical event per committed mutation with an ordinal stable across retry/replay.

- [ ] Write failing commit tests that commit vertex PUT, vertex DELETE, and edge PUT then inspect canonical storage entries for matching operation, valid interval, commit time, and payload.
- [ ] Run `cargo test -p temporal-storage --test event_commit`; expect no event entries.
- [ ] Allocate ordinals from the transaction operation order. Append each event to the existing `writes` vector before the prepared batch is applied, using the same replay-fingerprint path as current and history writes.
- [ ] Add a retry test proving a replay does not create a second event.
- [ ] Run `cargo test -p temporal-storage --test event_commit`; expect all cases to pass.
- [ ] Commit `feat(storage): persist immutable temporal events`.

### Task 3: Bounded Event Scan APIs

**Files:**
- Modify: `crates/temporal-storage/src/store.rs`
- Test: `crates/temporal-storage/tests/event_scan.rs`

**Interfaces:**
- Produces `scan_events_by_valid_from(graph, start, end, snapshot, max_rows, max_bytes)`.
- Produces `scan_events_by_commit(graph, start, end, max_rows, max_bytes)`.
- Both return ordered `Vec<CanonicalTemporalEvent>` and scanned byte counts.

- [ ] Write failing tests for half-open valid and commit windows, snapshot fencing, global ordering, DELETE visibility, row-limit rejection, and byte-limit rejection.
- [ ] Run `cargo test -p temporal-storage --test event_scan`; expect missing scan APIs.
- [ ] Implement key spans and decode validation. Valid scans must check `event.valid_from`; system scans must check commit HLC. Return stable bound errors before retaining an over-limit result.
- [ ] Run `cargo test -p temporal-storage --test event_scan`; expect all cases to pass.
- [ ] Commit `feat(storage): scan bounded canonical temporal events`.

### Task 4: Event Batches in the Universal Runtime

**Files:**
- Modify: `crates/query-executor/src/temporal.rs`
- Modify: `crates/query-executor/src/column_batch.rs`
- Modify: `crates/query-executor/src/lib.rs`
- Test: `crates/query-executor/tests/change_scan.rs`

**Interfaces:**
- Consumes the Task 3 event scan APIs and `PhysicalOperator::ChangeScan`.
- Produces a `ColumnBatch` whose entity slots contain the event entity and whose metadata projection reads operation and commit provenance without state reconstruction.

- [ ] Write failing valid-axis and system-axis tests for PUT/DELETE rows, snapshot cutoff, `operation`, `commit_seq`, and empty windows.
- [ ] Run `cargo test -p query-executor --test change_scan --no-default-features`; expect `ChangeScan` unsupported.
- [ ] Add a dedicated event execution branch. Do not route `ChangeScan` through `TemporalRead`, `scan_vertex_views_*`, or `scan_edges_*`.
- [ ] Convert event results to `ColumnBatch` at the storage boundary and retain rows only for output/protocol conversion.
- [ ] Run `cargo test -p query-executor --test change_scan --no-default-features`; expect all cases to pass.
- [ ] Commit `feat(runtime): execute canonical change events`.

### Task 5: Distributed and Engine Scope Fencing

**Files:**
- Modify: `crates/cypher-engine/src/lib.rs`
- Modify: `crates/distributed-query/src/worker.rs`
- Modify: `crates/distributed-query/src/coordinator.rs`
- Test: `crates/cypher-engine/tests/end_to_end.rs`
- Test: `crates/distributed-query/tests/fencing.rs`

**Interfaces:**
- Consumes `LogicalOperator::ChangeScan { axis, start, end, system_snapshot }`.
- Produces a change-specific resolved scope, worker request path, columnar batch exchange, and snapshot token validation.

- [ ] Write failing engine tests proving `CHANGES` does not resolve to `ValidTimeSpec::Current` and preserves parameterized valid/system windows.
- [ ] Write failing distributed tests for stale snapshot rejection and ordered complete event streams across shards.
- [ ] Run the two focused suites; expect ChangeScan scope fallback or unsupported execution.
- [ ] Resolve the explicit ChangeScan expressions into a dedicated event scope. Carry the statement snapshot ceiling in every worker request and reject a worker result with a mismatched fingerprint or sequence.
- [ ] Run `cargo test -p cypher-engine --test end_to_end --no-default-features` and `cargo test -p distributed-query --test fencing --no-default-features`; expect the new cases to pass.
- [ ] Commit `feat(cluster): fence distributed change event scans`.

### Task 6: Backend Capability and Clean-Break Gate

**Files:**
- Modify: `crates/storage-api/src/lib.rs`
- Modify: `crates/adapter-memory/src/lib.rs`
- Modify: `crates/adapter-rocksdb/src/lib.rs`
- Modify: `crates/adapter-postgres/src/lib.rs`
- Modify: `crates/adapter-neo4j/src/lib.rs`
- Create: `scripts/check-tcypher-clean-break.sh`
- Test: `crates/storage-api/tests/mapping_lifecycle.rs`

**Interfaces:**
- Adds capability declarations for bounded canonical event candidate scans.
- Requires all adapters to persist canonical event records without receiving source query text.

- [ ] Write failing mapping TCK fixtures for point, valid-time change, system-time change, and DELETE parity.
- [ ] Run `cargo test -p storage-api --test mapping_lifecycle`; expect missing event capability or mapping behavior.
- [ ] Implement event persistence and bounded candidate retrieval per adapter. Mark only an adapter fragment that proves exact semantics as `Exact`; otherwise return candidates for DTGProxy residual execution.
- [ ] Add a clean-break script that rejects `Diff`, `DIFF GRAPH`, `AT VALID_TIME`, `AT TRANSACTION_TIME`, and legacy query paths except asserted negative fixtures.
- [ ] Run the mapping TCK and the clean-break script; expect pass.
- [ ] Commit `feat(storage): serve canonical change events across adapters`.
