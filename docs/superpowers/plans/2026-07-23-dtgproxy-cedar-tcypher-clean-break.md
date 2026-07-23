# DTGProxy Cedar T-Cypher Clean-Break Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace DTGProxy's former temporal query surface with Cedar T-Cypher and one distributed vectorized execution path across RocksDB, Neo4j, and PostgreSQL.

**Architecture:** Preserve DTGProxy's canonical temporal KV, Raft, 2PC, adapters, and analytics ledger. Replace the language and query layers from syntax through physical execution, port Cedar's independent interval algorithms into Rust, and extend the existing batch/distributed runtime rather than introducing Cedar's C++ runtime.

**Tech Stack:** Rust 2024 workspace, Tokio/Tonic, DTGProxy Temporal IR, RocksDB, Neo4j Bolt, PostgreSQL, Cedar Apache-2.0 semantic and algorithm references.

## Global Constraints

- `FOR VALID_TIME`, `FOR SYSTEM_TIME`, and `CHANGES` are the only temporal query syntax.
- Do not retain parser aliases, old plan decoders, V1/V2, legacy, compat, or scalar production fallback paths.
- User writes provide `VALID FROM`; DTGProxy alone assigns system time.
- All intervals are half-open and retain true fact boundaries.
- Mixed paths are bounded and apply `TRAIL` to the complete relationship pattern.
- Backends receive canonical typed fragments, never raw user T-Cypher.
- Every allocation, exchange, and spill format is bounded and validated.
- Cedar-derived code records the source commit and Apache-2.0 provenance.
- Existing analytics ledger, checkpoint takeover, and GC semantics remain intact.

---

### Task 1: Clean-Break T-Cypher AST and Parser

**Files:**
- Modify: `crates/cypher-ast/src/temporal.rs`
- Modify: `crates/cypher-ast/src/statement.rs`
- Modify: `crates/cypher-ast/src/lib.rs`
- Modify: `crates/cypher-syntax/src/parser.rs`
- Modify: `crates/cypher-syntax/tests/parser_temporal.rs`
- Modify: `crates/cypher-syntax/tests/limits.rs`

**Interfaces:**
- Produces: `TemporalAxis`, `TemporalMode`, `TemporalScope`, `TemporalContext::scopes()`, and `Statement::Query`.
- Removes: `ValidTimeScope`, `TransactionTimeScope`, `DiffStatement`, and `Statement::Diff`.

- [ ] Write parser tests for query-level `FOR`, `CHANGES`, invalid duplicate axes, `VALID FROM`, and deterministic rejection of `AT`/`DIFF GRAPH`.
- [ ] Run `cargo test -p cypher-syntax --test parser_temporal`; expect old parser assertions to fail.
- [ ] Implement the orthogonal AST and parser, including exact source spans and bounded scope counts.
- [ ] Run `cargo test -p cypher-syntax`; expect all syntax tests to pass.
- [ ] Commit with `git commit -m "feat(query): adopt Cedar T-Cypher syntax"`.

### Task 2: Binder and Temporal Semantic Rules

**Files:**
- Modify: `crates/cypher-sema/src/analyzer.rs`
- Modify: `crates/cypher-sema/src/types.rs`
- Modify: `crates/cypher-sema/tests/temporal.rs`
- Modify: `crates/cypher-sema/tests/updates.rs`
- Create: `crates/cypher-sema/tests/changes.rs`

**Interfaces:**
- Consumes: `TemporalScope { axis, mode, start, end }`.
- Produces: analyzed state/change scopes, stable error codes, metadata function types, and `LIST<RELATIONSHIP>` path bindings.

- [ ] Add failing semantic tests for scope inheritance, one change axis, historical-system-time write rejection, `VALID TO`, mixed-path changes, and path-property rejection.
- [ ] Run the focused sema tests; expect the new cases to fail.
- [ ] Implement temporal scope validation and metadata/path typing without compatibility branches.
- [ ] Run `cargo test -p cypher-sema`; expect pass.
- [ ] Commit with `git commit -m "feat(query): bind Cedar temporal semantics"`.

### Task 3: Temporal IR Clean Break

**Files:**
- Modify: `crates/temporal-ir/src/logical.rs`
- Modify: `crates/temporal-ir/src/expression.rs`
- Modify: `crates/temporal-ir/src/header.rs`
- Modify: `crates/temporal-ir/tests/validate.rs`
- Modify: `crates/cypher-compiler/src/compiler.rs`
- Modify: `crates/cypher-compiler/tests/compile.rs`

**Interfaces:**
- Produces: `TemporalPointScan`, `TemporalRangeScan`, `ChangeScan`, `IntervalDerive`, `IntervalAlign`, `TemporalCoalesce`, `BoundedVariableExpand`, `PathTrail`, `PropertyGather`, and `MetadataProject`.
- Removes: `LogicalOperator::Diff` and language-facing transaction-time names.

- [ ] Add failing IR validation/golden tests for point, range, changes, demanded facts, metadata, and mixed segment bounds.
- [ ] Run focused compiler/IR tests; expect unsupported variants.
- [ ] Lower new scopes into explicit typed operators and include every scope in fingerprints.
- [ ] Run `cargo test -p temporal-ir -p cypher-compiler`; expect pass.
- [ ] Commit with `git commit -m "feat(query): lower Cedar temporal IR"`.

### Task 4: Cedar Interval Correctness Kernel

**Files:**
- Create: `crates/query-executor/src/interval_derive.rs`
- Create: `crates/query-executor/src/interval_align.rs`
- Create: `crates/query-executor/src/temporal_coalesce.rs`
- Modify: `crates/query-executor/src/lib.rs`
- Create: `crates/query-executor/tests/cedar_interval_oracle.rs`
- Modify: `third_party/manifest.toml`
- Add: `third_party/licenses/Cedar-Apache-2.0.txt`

**Interfaces:**
- Produces: `derive_visible_intervals`, `align_temporal_intervals`, and `coalesce_temporal_intervals`, all returning bounded `Result` values over DTGProxy types.

- [ ] Add a scalar bitemporal oracle covering same-valid-time corrections, predecessor/successor, deletes, infinity, alignment gaps, and provenance-sensitive coalescing.
- [ ] Run the oracle tests; expect missing functions.
- [ ] Port the three independent algorithms from Cedar `b37d3a2`, add size bounds and attribution.
- [ ] Run the focused tests and existing temporal row tests; expect pass.
- [ ] Commit with `git commit -m "feat(runtime): port Cedar temporal interval kernel"`.

### Task 5: Columnar Batch Runtime

**Files:**
- Create: `crates/query-executor/src/column_batch.rs`
- Create: `crates/query-executor/src/column.rs`
- Modify: `crates/query-executor/src/batch.rs`
- Modify: `crates/query-executor/src/executor.rs`
- Modify: `crates/query-executor/src/temporal.rs`
- Create: `crates/query-executor/tests/column_batch.rs`

**Interfaces:**
- Produces: `ColumnBatch`, typed columns, validity bitmaps, offset arenas, `row(index)` only for protocol/test boundaries, and canonical batch encoding.

- [ ] Add failing type/null/offset/list/path/encoding round-trip and corruption tests.
- [ ] Implement typed columns with checked lengths and bounded arenas.
- [ ] Convert production operator exchange from row vectors to `ColumnBatch`; retain row materialization only at declared boundaries.
- [ ] Run `cargo test -p query-executor`; expect pass.
- [ ] Commit with `git commit -m "feat(runtime): execute typed column batches"`.

### Task 6: Demand-Driven Temporal Pipelines

**Files:**
- Modify: `crates/cypher-compiler/src/compiler.rs`
- Modify: `crates/physical-plan/src/lib.rs`
- Modify: `crates/query-executor/src/temporal.rs`
- Modify: `crates/query-executor/src/temporal_row.rs`
- Create: `crates/query-executor/tests/demanded_facts.rs`

**Interfaces:**
- Produces: `FactDemandSet` and vectorized point/range/change pipelines with late `PropertyGather`.

- [ ] Add tests proving unused property changes do not split output while demanded property/provenance changes do.
- [ ] Implement demand collection, candidate pruning, interval alignment, metadata projection, and coalescing.
- [ ] Verify change queries return immutable PUT/DELETE events without state reconstruction.
- [ ] Run compiler, executor, and engine temporal tests; expect pass.
- [ ] Commit with `git commit -m "feat(runtime): execute demanded temporal facts"`.

### Task 7: Fixed, Variable, and Mixed TRAIL Paths

**Files:**
- Modify: `crates/cypher-ast/src/pattern.rs`
- Modify: `crates/cypher-syntax/src/pattern_parser.rs`
- Modify: `crates/cypher-compiler/src/compiler.rs`
- Modify: `crates/physical-plan/src/lib.rs`
- Create: `crates/query-executor/src/segmented_frontier.rs`
- Create: `crates/query-executor/tests/mixed_paths.rs`

**Interfaces:**
- Produces: per-segment `[min_hops,max_hops]`, ordered path-list bindings, and a segmented frontier carrying one global visited-edge set.

- [ ] Add point/range tests for fixed, variable, variable-fixed, fixed-variable, multi-variable, global TRAIL, projections, and deterministic mixed-CHANGES rejection.
- [ ] Implement the bounded segmented frontier and temporal-domain intersection per hop.
- [ ] Add shard-bucket serialization tests with strict frontier byte/row/hop limits.
- [ ] Run focused path tests and the Cypher engine suite; expect pass.
- [ ] Commit with `git commit -m "feat(runtime): execute mixed temporal trail paths"`.

### Task 8: Memory Accounts, Spill, Cancellation, and Metrics

**Files:**
- Create: `crates/query-executor/src/memory.rs`
- Create: `crates/query-executor/src/spill.rs`
- Create: `crates/query-executor/src/metrics.rs`
- Modify: `crates/query-executor/src/context.rs`
- Modify: `crates/query-executor/src/executor.rs`
- Modify: `crates/query-executor/src/segmented_frontier.rs`
- Create: `crates/query-executor/tests/resource_control.rs`

**Interfaces:**
- Produces: hierarchical reservations, checksummed query-private spill partitions, cooperative cancellation points, and stable operator metrics.

- [ ] Add low-memory tests forcing sort/join/aggregate/distinct/coalesce/frontier spill and checking cleanup.
- [ ] Add cancellation tests at morsel, frontier, spill, gather, exchange, and output boundaries.
- [ ] Implement reserve-before-allocate and bounded spill; reject unsupported overflow deterministically.
- [ ] Add `EXPLAIN ANALYZE` counters and serialization tests.
- [ ] Run executor tests including process-stop cleanup; expect pass.
- [ ] Commit with `git commit -m "feat(runtime): bound T-Cypher query resources"`.

### Task 9: Distributed Columnar Execution and Snapshot Fencing

**Files:**
- Modify: `crates/distributed-query/src/lib.rs`
- Modify: `crates/distributed-query/tests/fencing.rs`
- Modify: `crates/distributed-query/tests/local_worker.rs`
- Modify: `crates/cypher-engine/src/lib.rs`
- Modify: `crates/gateway-node/src/service.rs`

**Interfaces:**
- Produces: columnar `BatchEnvelope`, scope-aware `SnapshotToken`, segmented frontier exchange, credit backpressure, and token validation at every fragment.

- [ ] Add failing tests for schema checksum, sequence, scope ceiling, epoch mismatch, retries, and cross-shard mixed paths.
- [ ] Encode/decode canonical column batches and frontier states with strict bounds.
- [ ] Enforce safe timestamp, system-scope ceiling, topology lineage, deadline, cancellation, and retry invariants.
- [ ] Run distributed-query, engine, and gateway focused tests; expect pass.
- [ ] Commit with `git commit -m "feat(cluster): distribute Cedar T-Cypher batches"`.

### Task 10: Temporal Writes and Transactions

**Files:**
- Modify: `crates/cypher-engine/src/write.rs`
- Modify: `crates/cypher-engine/tests/write_materializer.rs`
- Modify: `crates/txn-protocol/src/lib.rs`
- Modify: `crates/gateway-node/tests/service.rs`

**Interfaces:**
- Consumes: `VALID FROM` mutations and the existing canonical Version Rewriter.
- Produces: read-your-writes overlays, single-shard 1PC, distributed 2PC, and cancellation behavior around the commit point.

- [ ] Add tests for backdated valid time, system-time assignment, overlay reads, endpoint guards, PREPARING participant freeze, and COMMITTED roll-forward.
- [ ] Update write materialization to consume the new AST/IR without old temporal scopes.
- [ ] Verify request IDs and deterministic mutation bytes remain stable across retry.
- [ ] Run transaction, engine, and gateway tests; expect pass.
- [ ] Commit with `git commit -m "feat(txn): commit Cedar temporal mutations"`.

### Task 11: Three Backend Batch Adapter Semantics

**Files:**
- Modify: `crates/storage-api/src/lib.rs`
- Modify: `crates/adapter-rocksdb/src/lib.rs`
- Modify: `crates/adapter-postgres/src/lib.rs`
- Modify: `crates/adapter-neo4j/src/lib.rs`
- Modify: `crates/gateway-node/tests/three_backend_deployment.rs`

**Interfaces:**
- Produces: bounded batch point/range/change scans, property gather, adjacency expansion, capability declarations, and residual-semantics markers.

- [ ] Extend the shared adapter TCK with identical point/range/change/path/write fixtures.
- [ ] Implement exact/lossless-candidate capabilities and typed batch primitives in all three adapters.
- [ ] Verify migration snapshots preserve canonical bytes and query results.
- [ ] Run native PostgreSQL and Neo4j mapping tests plus RocksDB tests.
- [ ] Commit with `git commit -m "feat(storage): serve Cedar temporal batches on three backends"`.

### Task 12: Analytics Projection and Public Surface Migration

**Files:**
- Modify: `crates/graph-projection/src/lib.rs`
- Modify: `crates/analytics-runtime/src/provider.rs`
- Modify: `crates/gateway-node/src/analytics_scheduler.rs`
- Modify: all Cypher query fixtures under `crates/*/tests` and user documentation.

**Interfaces:**
- Produces: Snapshot/Interval/Event/Delta graph projections driven only by new scopes while preserving ledger/checkpoint/GC manifests.

- [ ] Convert every fixture and example to new syntax; add negative old-syntax migration tests.
- [ ] Update projection derivation and fingerprints without changing durable job state semantics.
- [ ] Run analytics algorithm, takeover, checkpoint, and GC tests.
- [ ] Commit with `git commit -m "feat(analytics): project graphs from Cedar scopes"`.

### Task 13: Clean-Break, License, and Documentation Gate

**Files:**
- Modify: `README.md` and `docs/*.md` as matched by the audit.
- Remove: obsolete query plans/specs and compatibility-only code/tests.
- Create: `scripts/check-tcypher-clean-break.sh`
- Modify: `.github/workflows/*.yml`

**Interfaces:**
- Produces: an automated gate rejecting old syntax/types/paths outside explicit negative fixtures.

- [ ] Implement searches for old syntax, Diff AST/IR, V1/V2, legacy/compat/fallback query paths.
- [ ] Remove every positive match or explicitly isolate an asserted negative fixture.
- [ ] Validate Cedar license, source commit, NOTICE, and third-party manifest.
- [ ] Run the gate, format, clippy, docs, and dependency/license checks.
- [ ] Commit with `git commit -m "chore(query): enforce T-Cypher clean break"`.

### Task 14: Full Verification, CI Repair, Performance, and Push

**Files:**
- Modify only files required by observed failures.
- Add verification artifacts under `docs/verification/` and benchmark outputs under the existing benchmark artifact path.

**Interfaces:**
- Produces: authoritative green local/CI evidence and the pushed `feature/dtgproxy` branch.

- [ ] Run workspace format, clippy, unit, TCK, integration, recovery, and clean-break gates; record exact results.
- [ ] Run PostgreSQL/Neo4j native mapping and the complete three-backend × two-mode matrix.
- [ ] Diagnose each failure from exact logs, add a reproducing test, make the minimal root-cause fix, and rerun until green.
- [ ] Run parser/plan/point/range/change/1-hop/mixed-path/join/aggregate/spill/exchange benchmarks and compare the specified gates.
- [ ] Audit every specification requirement against current code and evidence; leave no missing or indirect item.
- [ ] Commit all verified changes and push `feature/dtgproxy` to `git@github.com:yangwangcs/DTGProxy.git`.
