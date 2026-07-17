# DTGProxy Phase 1C History, Transaction, and Query Plan

**Goal:** Finish the single-node product slice by replacing full History scans/full-anchor write
amplification with bounded seek plus Anchor+Delta, adding atomic multi-element temporal
transactions, and exposing the result through a small typed Temporal IR and query executor.

**Why this phase comes before Raft:** Phase 1B proved semantic round-trip correctness, but its
benchmark showed 222–231 µs AS OF latency at only 32 anchors because every lookup scans the full
element prefix. Replicating that representation would multiply an avoidable local cost. Phase 1C
first stabilizes the state-machine format and query boundary that Phase 2 will replicate.

## Constraints

- Phase 1B's public Current/AS OF/DIFF results remain unchanged.
- History entries are immutable and have explicit value tags/magic/version/checksum.
- A bounded history chain always terminates at an Anchor; corrupt or over-limit chains fail closed.
- Delta replay is deterministic and uses the same interval rewriter as commits.
- One transaction produces one adapter batch and one applied log index, even when it changes
  multiple vertices/edges and all associated adjacency records.
- Single-node Temporal Snapshot Isolation checks overlapping valid intervals for every written
  identity. Endpoint existence is checked for edge writes; edge/vertex guard locking remains a
  Phase 3 distributed concern.
- Query execution reads through `TemporalStore`, never RocksDB directly.

---

### Task 1: Bounded Range/Seek SPI and AS OF Fast Path

**Files:** `storage-api`, both adapters, `temporal-storage` key/store/tests/benchmark.

- [x] Add failing tests for bounded ranges, prefix-constrained seek, exclusive ends, and scan
  limits in Memory and RocksDB.
- [x] Extend `KeySpan` without changing existing prefix behavior; adapters must stop at the bound
  or limit while retaining snapshot consistency.
- [x] Add a History seek span that starts at `reverse(requested_tx)` and is bounded by the element
  prefix, so entries newer than the requested snapshot are never decoded.
- [x] Make Current AS OF point reads request only the first eligible full anchor in the Phase 1B
  format; retain full scans only for conflict validation until Task 2.
- [x] Add a single-decode guard that corrupts an older anchor and re-run the release benchmark.
  Latest and oldest lookup decode one Phase 1B anchor, independent of history depth.
- [x] Commit as `perf: bound temporal history seeks`.

Task 1 benchmark evidence (`DTGPROXY_BENCH_ITERS=200`): AS OF latest improved from 222,166 to
11,462 ns/op (94.8%); AS OF oldest at depth 32 improved from 230,516 to 2,828 ns/op (98.8%).
Current lookup measured 7,697 ns/op, synchronous correction 424,380 ns/op, degree-32 expansion
29,440 ns/op, and the database occupied 290,151 bytes.

### Task 2: Anchor+Delta Durable Format and Reconstruction

**Files:** `temporal-storage/src/record.rs`, new `history.rs`, store/rewrite/tests.

- [x] Write failing codec tests for `HistoryDelta` Put/Delete changes, canonical payloads,
  corruption, and mixed Anchor/Delta chains.
- [x] Introduce `HistoryEntry::{Anchor, Delta}` decoding by magic. A Delta stores `commit_ts`,
  exact changed valid interval, and optional canonical replacement.
- [x] Use an explicit policy (initial anchor, then at most 15 deltas) and force an anchor when
  replay-count or encoded-byte thresholds are reached.
- [x] Reconstruct AS OF by bounded seek, scanning older entries only until the first Anchor,
  reversing collected deltas, and applying the shared deterministic interval rewrite.
- [x] Use entry metadata for bounded conflict scans over `(read_ts, commit_ts)` rather than
  decoding all element history.
- [x] Add restart/checkpoint, randomized equivalence, corrupt-chain, maximum-depth, and migration
  tests from Phase 1B full-anchor data.
- [x] Benchmark history depths 1/16/1,000 and record bytes per correction; commit as
  `feat: add bounded anchor delta history`.

Task 2 benchmark evidence (`DTGPROXY_BENCH_ITERS=200`, 1,008 element commits): replay depth 1
5,684 ns/op; maximum replay depth 16 20,457 ns/op; snapshot age 1,000 13,528 ns/op; Current
lookup 3,095 ns/op; synchronous correction 131,046 ns/op; degree-32 expansion 29,456 ns/op.
History values used 100,827 bytes versus 148,077 hypothetical all-anchor bytes, a 31.9%
reduction on the single-segment workload.

### Task 3: Atomic Multi-element Single-node Transactions

**Files:** new `transaction.rs`; refactor store/rewrite; transaction TCK.

- [x] Write failing tests for two vertices plus an edge in one commit, atomic failure, deterministic
  retry, overlapping stale-write rejection, non-overlapping stale writes, endpoint existence,
  partial endpoint deletion, and immutable identity.
- [x] Define `TemporalTransaction` and typed operation enums. Normalize and sort operations by
  graph/partition/kind/element/valid start before assigning deterministic mutation sequences.
- [x] Pin preparation to the next state-machine log index, validate the complete write set against
  that stable logical snapshot, and stage Current/History/double-adjacency mutations without
  applying them. Already-applied indices retain deterministic replay behavior.
- [x] Validate edge valid intervals are fully covered by both endpoint projections after applying
  all staged vertex changes in the same transaction.
- [x] Apply exactly one `CommittedMutationBatch`; prove any validation/adapter failure leaves all
  elements and adjacency unchanged.
- [x] Keep single-element convenience methods as wrappers around the transaction API.
- [x] Commit as `feat: add atomic temporal graph transactions`.

Task 3 evidence: seven focused semantic tests cover multi-element atomicity, deterministic replay,
log gaps, interval conflicts, endpoint coverage, and coordinated endpoint/edge deletion. A shared
adapter TCK passes against Memory and RocksDB, including rollback without applied-index advance.
Single-element edge tests now seed and validate real endpoints rather than bypassing graph
referential integrity.

### Task 4: Typed Temporal IR and Local Executor

**Files:** new `temporal-ir` and `query-executor` crates plus TCK.

- [ ] Define versioned typed IR for vertex lookup, edge lookup, expand out/in/both, Current,
  transaction AS OF, valid-time point selector, and element DIFF.
- [ ] Validate plans (graph/partition scope, required time selectors, bounded result limits) before
  execution.
- [ ] Implement executor operators over `TemporalStore` with deterministic ordering and explicit
  residual valid-time filtering.
- [ ] Add typed result records preserving canonical values and edge identities.
- [ ] Test equivalent Current/AS OF/DIFF plans against direct APIs and checkpoint databases.
- [ ] Commit as `feat: execute local temporal IR`.

### Task 5: Minimal Temporal Query Frontend

**Files:** new `temporal-query` crate, CLI integration, parser/executor tests.

- [ ] Specify a deliberately small syntax for ID lookup/expand plus `FOR VALID TIME`,
  `AS OF TRANSACTION TIME`, and `DIFF TRANSACTION TIME` clauses.
- [ ] Implement a hand-written bounded parser with structured errors and no backend-specific
  syntax leakage.
- [ ] Compile syntax to the typed IR; test whitespace, integer bounds, malformed clauses, and
  unsupported Cypher constructs.
- [ ] Add CLI `query --db <path> --text <query>` and machine-readable canonical output.
- [ ] Commit as `feat: add temporal query subset`.

### Task 6: Phase 1 Acceptance

- [ ] Run deterministic and randomized model/Memory/RocksDB TCKs including restart/checkpoint.
- [ ] Run format, strict Clippy, full workspace tests, parser/key/value fuzz smoke tests, CLI, and
  diff checks.
- [ ] Run release benchmarks and compare Phase 1B vs Phase 1C history latency/write bytes.
- [ ] Document compatibility, migration, transaction/isolation limits, IR/query grammar, measured
  results, and remaining distributed scope.
- [ ] Record exact evidence and commits. Phase 1 completion must not mark the overall DTGProxy
  objective complete; Phase 2–6 remain.
