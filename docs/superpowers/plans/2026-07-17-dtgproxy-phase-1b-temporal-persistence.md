# DTGProxy Phase 1B Temporal Persistence Implementation Plan

> **Execution rule:** implement task-by-task with test-driven development. Every production
> interface first appears in a failing test, and every task ends with focused regressions.

**Goal:** Prove DTGProxy's central single-node claim end to end: a typed bitemporal property
graph mutation is deterministically rewritten into ordinary KV records, atomically persisted in
RocksDB, and read back without type or temporal-semantic loss through Current, AS OF, adjacency,
and DIFF APIs.

**Architecture:** Add a backend-neutral `temporal-storage` crate above `StorageAdapter`. Stable
key and value codecs express graph identities, Current materializations, immutable History
anchors, and double adjacency. The rewriter computes a complete valid-time segmentation at the
transaction snapshot and emits one `CommittedMutationBatch`; the adapter remains unaware of
temporal semantics. A prefix-scan extension to the adapter SPI supports ordered history lookup
and adjacency expansion. Phase 1B uses a full anchor per committed element change as the
correctness baseline. Phase 1C will introduce bounded deltas and anchor compaction behind the
same read API, then add the Temporal Cypher subset.

**Tech Stack:** Rust 1.93, edition 2024; existing `temporal-types`, `storage-api`, memory and
RocksDB adapters; no unstable/default serializers in the durable format.

## Non-negotiable invariants

- Durable integer fields use big-endian encoding and every value has magic plus format version.
- Current and History are updated in the same committed adapter batch.
- History anchors are immutable; a newer correction never overwrites an older transaction-time
  view.
- A Current record is the latest transaction-time projection and contains disjoint valid-time
  segments.
- Identical logical input produces byte-identical keys, values, mutation order, and fingerprint.
- Reads decode only canonical payloads, never a lossy native projection.
- Current and AS OF results must match the in-memory semantic oracle.
- Vertex and edge IDs are graph-scoped; edge endpoints and edge type are immutable after
  identity creation.
- Current out/in adjacency is maintained atomically with the edge and filtered by exact valid
  time after prefix scan.
- Phase 1B does not claim distributed isolation: it proves deterministic single-shard rewriting
  and persistence. Raft and distributed bitemporal transactions remain Phase 2 and Phase 3.

---

### Task 1: Ordered Scan Contract

**Files:**
- Modify: `crates/storage-api/src/lib.rs`
- Create: `crates/storage-api/tests/key_span.rs`
- Modify: `crates/adapter-memory/src/lib.rs`
- Modify: `crates/adapter-rocksdb/src/lib.rs`
- Modify: both adapter contract test files

**Produces:** `KeySpan::prefix`, `StorageAdapter::scan`, and ordered `KeyValue` results.

- [ ] Write failing contract tests for an empty prefix, exact keyspace isolation, bytewise order,
  exclusive upper-bound construction (including an all-`0xff` prefix), and snapshot-consistent
  scan results.
- [ ] Run the storage and memory tests to capture RED.
- [ ] Implement stable prefix-bound calculation and the object-safe boxed-future SPI method.
- [ ] Implement memory scan using `BTreeMap::range` and RocksDB scan using one DB snapshot plus a
  bounded iterator in the selected Column Family.
- [ ] Run both adapters' complete contract suites and strict workspace Clippy.
- [ ] Commit as `feat: add ordered storage scans`.

### Task 2: Stable Graph Key Codec

**Files:**
- Modify: root `Cargo.toml`
- Create: `crates/temporal-storage/Cargo.toml`
- Create: `crates/temporal-storage/src/lib.rs`
- Create: `crates/temporal-storage/src/key.rs`
- Create: `crates/temporal-storage/tests/key_codec.rs`
- Modify: `crates/temporal-types/src/time.rs`

**Produces:** typed graph IDs and deterministic identity/current/history/adjacency keys.

- [ ] Write failing tests for graph, partition, vertex, and edge key golden bytes; round-trip
  decoding; keyspace selection; reverse transaction-time ordering; and malformed/trailing data.
- [ ] Add failing time-component accessor tests rather than exposing tuple fields.
- [ ] Implement `GraphId(u64)`, `PartitionId(u32)`, `ElementId(u128)`, `LabelId(u32)`,
  `EdgeTypeId(u32)`, `ElementKind`, and `ElementRef`.
- [ ] Implement explicit key tags: vertex identity `0x01`, edge identity `0x02`, Current vertex
  `0x08`, Current edge `0x09`, out adjacency `0x10`, in adjacency `0x11`, and History anchor
  `0x20`. Include graph and partition in every primary key.
- [ ] Encode transaction time as signed-micros order-preserving bytes plus logical counter;
  History uses the bitwise reverse of that ordered representation so newer anchors sort first.
- [ ] Run codec tests, format, and Clippy; commit as `feat: add temporal graph key codec`.

### Task 3: Stable Projection Record Codec

**Files:**
- Create: `crates/temporal-storage/src/record.rs`
- Create: `crates/temporal-storage/tests/record_codec.rs`
- Modify: `crates/temporal-storage/src/lib.rs`

**Produces:** `VertexIdentity`, `EdgeIdentity`, `ValidSegment`, `ProjectionRecord`, and
`HistoryAnchor` codecs.

- [ ] Write failing golden-vector and round-trip tests covering vertices, edges, finite and
  unbounded valid intervals, empty projections, multiple disjoint segments, every canonical
  graph value type, checksum mismatch, duplicate/overlapping segments, wrong magic/version,
  truncation, and trailing bytes.
- [ ] Implement versioned values with independent magics for identity, projection, and anchor.
  Embed length-delimited `CanonicalElement::encode()` bytes and a deterministic FNV-1a checksum.
- [ ] Validate strict increasing/disjoint half-open valid segments during construction and decode.
- [ ] Make `HistoryAnchor` carry `commit_ts`, the exact changed valid interval, and the complete
  projection at that transaction time. This metadata enables overlap conflict checks without
  comparing entire states.
- [ ] Run record/codec regressions and commit as `feat: encode temporal projection records`.

### Task 4: Vertex Rewriter and Current/AS OF Reads

**Files:**
- Create: `crates/temporal-storage/src/store.rs`
- Create: `crates/temporal-storage/src/rewrite.rs`
- Create: `crates/temporal-storage/tests/vertex_roundtrip_memory.rs`
- Modify: `crates/temporal-storage/src/lib.rs`

**Produces:** `TemporalStore`, `VertexMutation`, `CommitContext`, `put_vertex`, `delete_vertex`,
`vertex_current`, and `vertex_as_of`.

- [ ] Write failing memory-adapter tests for initial insert, disjoint insert, retroactive
  correction with residual splitting, partial delete, transaction-time audit, no-result reads,
  canonical type preservation, deterministic replay, invalid commit order, and stale overlapping
  writer conflict. Compare every view to `temporal-model`.
- [ ] Implement AS OF anchor selection by ordered History prefix scan. At `read_ts`, load the
  newest anchor not newer than the snapshot; inspect later anchors' changed intervals to reject
  only overlapping stale writes.
- [ ] Rewrite a valid interval by subtracting it from affected segments, inserting/replacing the
  requested present segment (or leaving a hole for delete), sorting, and coalescing adjacent
  equal payloads.
- [ ] Emit deterministic mutations in fixed order: identity-if-new, Current projection, immutable
  History anchor. Use the caller's shard/log/transaction identifiers in one committed batch.
- [ ] Implement Current fast-path reads and AS OF reconstruction from the selected full anchor.
- [ ] Run all memory round-trip tests and commit as `feat: persist bitemporal vertices`.

### Task 5: Edges and Double Adjacency

**Files:**
- Modify: `crates/temporal-storage/src/store.rs`
- Modify: `crates/temporal-storage/src/rewrite.rs`
- Create: `crates/temporal-storage/tests/edge_roundtrip_memory.rs`

**Produces:** edge mutation/read APIs plus `expand_out_current` and `expand_in_current`.

- [ ] Write failing tests for typed edge round-trip, immutable identity, both adjacency
  directions, exact valid-time filtering, partial delete, fully absent Current adjacency removal,
  AS OF edge reads, and atomic rollback on a malformed edge batch.
- [ ] Implement edge identity records containing immutable source, destination, and edge type.
- [ ] Atomically write Current edge, History anchor, and both adjacency keys. Adjacency values
  point to the canonical Current edge record; delete both keys only when the latest projection
  contains no present valid segment.
- [ ] Implement ordered prefix expansions and residual exact-time filtering.
- [ ] Run vertex/edge suites plus both adapter contracts; commit as
  `feat: persist bitemporal edges and adjacency`.

### Task 6: Temporal DIFF and RocksDB End-to-End Proof

**Files:**
- Create: `crates/temporal-storage/src/diff.rs`
- Create: `crates/temporal-storage/tests/diff.rs`
- Create: `crates/temporal-storage/tests/rocksdb_roundtrip.rs`
- Modify: `crates/temporal-storage/Cargo.toml`

**Produces:** `diff_vertex`, `diff_edge`, and durable adapter TCK coverage.

- [ ] Write failing DIFF tests that partition the valid-time domain at all segment boundaries and
  return coalesced `Added`, `Removed`, and `Changed { before, after }` ranges between two
  transaction snapshots.
- [ ] Implement DIFF from two AS OF projections without backend-specific logic.
- [ ] Port the complete vertex, correction, deletion, edge, adjacency, AS OF, DIFF, and canonical
  type round-trip sequence to a real temporary RocksDB database.
- [ ] Drop and reopen RocksDB mid-sequence, then compare every logical result and canonical graph
  hash with the memory adapter/oracle.
- [ ] Create a checkpoint, advance the source, and prove temporal reads from the checkpoint remain
  at the captured log/transaction frontier.
- [ ] Run all temporal-storage tests and commit as `feat: prove durable temporal graph round trips`.

### Task 7: Phase 1B Performance and Acceptance

**Files:**
- Create: `crates/temporal-storage/benches/roundtrip.rs`
- Modify: `crates/temporal-storage/Cargo.toml`
- Modify: `README.md`
- Modify: this plan with actual evidence

- [ ] Add reproducible benchmarks for Current point lookup, AS OF anchor lookup at several history
  depths, retroactive correction at several segment counts, and one-hop expansion at several
  degrees. Report throughput/latency and bytes written; do not encode unmeasured targets as
  claims.
- [ ] Add a deterministic randomized TCK sequence that applies the same operations to the model,
  memory-backed store, and RocksDB-backed store, checking Current/AS OF equivalence after every
  commit.
- [ ] Document the Phase 1B API, durable layout, correctness boundaries, benchmark invocation,
  and the anchor-only write-amplification tradeoff that Phase 1C will remove.
- [ ] Run `cargo fmt --all -- --check`, strict workspace Clippy, full workspace tests, release
  benchmarks, CLI version, and `git diff --check` with the verified LLVM environment.
- [ ] Record exact counts/results and commits, then commit as
  `docs: record temporal persistence verification`.

## Exit boundary

Phase 1B is accepted only when a vertex-and-edge temporal graph survives
`encode -> rewrite -> atomic persist -> restart/checkpoint -> Current/AS OF/DIFF decode` and the
decoded typed graph matches the memory semantic oracle. Phase 1B deliberately leaves bounded
Anchor+Delta compaction and Temporal Cypher to Phase 1C; it does not advance the overall goal to
complete.
