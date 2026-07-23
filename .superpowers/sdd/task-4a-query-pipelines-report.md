# Task 4A: Cypher 25 WITH, UNWIND, and UNION pipelines report

## RED evidence

### WITH scope and UNION schema contracts

Command:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-sema --test scope
```

Initial result: RED (exit 101). Three semantic tests failed for the intended reasons:

- `WITH *` reached the expression parser and failed with `DTG-CYPHER-EXPECTED-EXPRESSION`.
- `RETURN 1 AS left UNION RETURN 2 AS right` was incorrectly accepted.
- Integer/String UNION columns with the same name were incorrectly accepted.

After the semantic fix, all scope tests pass, including sibling UNION scope isolation and alias
shadowing. One initial alias fixture used the reserved clause word `next`; it was renamed before
claiming semantic RED/GREEN evidence.

### Multiple UNION boundaries, complete WITH lowering, and subquery UNION

Command:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-compiler --test compile
```

Initial result: RED (exit 101):

- the three-boundary mixed chain failed with
  `DTG-CYPHER-NESTED-UNION-NOT-YET-LOWERED`;
- `WITH DISTINCT` failed at the expression parser;
- UNION inside `CALL { ... }` failed with `DTG-CYPHER-CLAUSE-NOT-YET-LOWERED`.

The compiler suite is now GREEN. It proves that the mixed chain is lowered as three ordered binary
boundaries `[ALL, DISTINCT, ALL]`, not flattened.

### Physical DAG execution

Command:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-engine --test end_to_end \
  mixed_union_chain_preserves_each_boundary_multiplicity -- --exact
```

Initial result: RED (exit 101) with `Optimize("... UnsupportedUnionShape")`. The compiler had
already produced the correct nested logical tree; the one-level physical UNION optimizer rejected
it. After recursive physical planning and topological coordinator execution, the result is exactly
`[1, 2, 2]`, which distinguishes the required left-associated boundary semantics from flattening.

### Runtime list expressions and interval composition

The interval `UNION + WITH [n] + UNWIND` test initially failed with
`DTG-CYPHER-NONLITERAL-LIST: list literal contains a runtime expression`. `ScalarExpr::List` now
evaluates list items per upstream row. The focused test is GREEN and UNION DISTINCT returns one
visible row with the exact source valid-time region and merged deterministic UNWIND provenance.

### Temporal DISTINCT

The exact temporal DISTINCT test initially failed to compile because no exact-distinct API existed.
The corrected implementation removes rows with identical `(values, region)` and merges their
provenance by sorted set union. Its GREEN test keeps adjacent unequal regions separate; UNION
DISTINCT no longer calls temporal coalescing.

### UNWIND null and interval memory bounds

- The ASC/DESC null-ordering test first failed with `NullInNonNullableColumn`: UNWIND list items
  were incorrectly assigned a non-nullable binding. The binding is now nullable and the test is
  GREEN for Cypher null placement in both directions.
- A one-byte interval fragment budget test first returned a row instead of an error. Shared
  TemporalRow accounting now fences every shard/coordinator operator and UNION/JOIN intermediate;
  the test is GREEN with `MemoryLimitExceeded`.

## GREEN evidence

Focused results:

```text
cypher-sema scope                         7 passed, 0 failed
cypher-compiler compile                  15 passed, 0 failed
mixed three-boundary UNION               1 passed, 0 failed
WITH DISTINCT/group/order/where/skip     1 passed, 0 failed
ASC/DESC Cypher null ordering            1 passed, 0 failed
grouped WITH + chained UNWIND             1 passed, 0 failed
empty-list/null UNWIND                    1 passed, 0 failed
UNION chain in read subquery              1 passed, 0 failed
interval UNION/WITH/UNWIND                1 passed, 0 failed
exact temporal DISTINCT                   1 passed, 0 failed
PrimaryReplica/SharedNothing equivalence  1 passed, 0 failed
Gateway explicit transaction integration 1 passed, 0 failed
interval one-byte memory fence            1 passed, 0 failed
```

The Gateway integration now executes successful and deliberately failing statements through
`UNWIND -> WITH -> UNWIND -> CREATE` in one explicit transaction. Three successful rows commit;
the failing multi-row statement publishes no rows and does not damage prior staged writes.

Required suite command:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-syntax -p cypher-sema -p cypher-compiler -p temporal-ir \
  -p query-executor -p query-optimizer -p distributed-query -p cypher-engine -p gateway-node
```

Result before the final interval-budget self-review change: GREEN (exit 0), with all selected unit,
integration, process, and doc tests passing. A fresh final run is recorded below.

## Implementation notes

- WITH now creates a lexical scope from its projected fields, including `*`, aliases, expressions,
  DISTINCT, grouped aggregation, and attached WHERE/ORDER BY/SKIP/LIMIT operators.
- Projection and Aggregate physical operators carry their own output schemas. Multi-stage pipelines
  no longer apply a fragment's final schema to intermediate rows.
- ORDER BY supports expression keys, ASC/DESC direction, deterministic multi-key comparison, and
  Cypher null placement. Hidden expression slots are projected away before the next clause.
- UNWIND evaluates once per upstream row, retains upstream slots, supports runtime/nested lists,
  emits no rows for null or empty lists, preserves null list items, and chunks expanded output.
- UNION lowering snapshots the branch input state, isolates sibling scope, and builds an ordered
  left-associated binary tree for every boundary. Column names and typed schemas are checked before
  lowering.
- The optimizer recursively preserves UNION boundaries. Source-free leaves run once on the
  coordinator. Graph-source leaves run on the primary or all routed shards and gather before global
  projection/distinct/aggregate/sort/skip/limit work.
- The distributed coordinator evaluates the physical DAG in fragment order, preserving left/right
  exchange order. Interval UNION uses exact temporal DISTINCT; regions and provenance are not
  coalesced as a side effect of duplicate removal.
- Existing logical/physical node, fragment, exchange, batch, memory, credit, cancellation, and
  deadline bounds remain authoritative. Interval row intermediates now use the same deterministic
  memory-budget failure principle as point batches.

## Self-review

- Confirmed the mixed UNION chain remains three physical two-input coordinator boundaries; no
  mixed ALL/DISTINCT flattening exists.
- Confirmed sibling UNION branches restore their initial scope and cannot see sibling bindings.
- Confirmed Project/Aggregate output schemas advance per operator in both point and interval paths.
- Confirmed temporal DISTINCT compares visible values plus the exact region, merges provenance
  deterministically, and never merges unequal regions.
- Confirmed PrimaryReplica and SharedNothing return identical typed batches for a graph-source
  WITH/aggregate pipeline.
- Confirmed source-free UNION/UNWIND branches are coordinator-only and cannot multiply by shard
  count.
- Confirmed failed explicit-transaction multi-row writes do not partially publish candidate rows.
- Confirmed no versioned query/IR namespace, compatibility executor, text fallback, backend Cypher
  pass-through, Cypher 5 path, or old nested-UNION limitation was introduced.
- `rustfmt --edition 2024` completed for all touched Rust files and `git diff --check` returned exit
  0 before final verification.

## Concerns

- Named procedure execution/CALL YIELD completion and the remaining correlated, EXISTS/COUNT,
  writing, and `IN TRANSACTIONS` subquery forms remain explicitly assigned to Tasks 4B and 4C.
  This task only extends read-subquery UNION chains through the same logical/physical operators.

## Final verification after self-review

The required nine-package command was rerun after the interval memory-budget fix and expression
ORDER BY completion. Result: GREEN (exit 0), with zero failures across all selected unit,
integration, process, and doc tests. Notable totals include 28/28 Cypher engine end-to-end tests,
7/7 semantic scope tests, 8/8 distributed coordinator tests, 14/14 TemporalRow tests, 9/9
temporal scan tests, and the Gateway remote explicit-transaction service integration test.

Final hygiene:

```text
rustfmt --edition 2024 --check <all Task 4A Rust files>
# exit 0

git diff --check
# exit 0
```

## Files changed

- `.superpowers/sdd/task-4a-query-pipelines-report.md`
- `crates/cypher-sema/src/analyzer.rs`
- `crates/cypher-sema/tests/scope.rs`
- `crates/cypher-compiler/src/compiler.rs`
- `crates/cypher-compiler/tests/compile.rs`
- `crates/temporal-ir/src/expression.rs`
- `crates/temporal-ir/src/lib.rs`
- `crates/temporal-ir/src/logical.rs`
- `crates/physical-plan/src/lib.rs`
- `crates/physical-plan/tests/validate.rs`
- `crates/query-executor/src/executor.rs`
- `crates/query-executor/src/expression.rs`
- `crates/query-executor/src/lib.rs`
- `crates/query-executor/src/temporal.rs`
- `crates/query-executor/src/temporal_row.rs`
- `crates/query-executor/tests/graph_overlay.rs`
- `crates/query-executor/tests/operators.rs`
- `crates/query-executor/tests/temporal_rows.rs`
- `crates/query-executor/tests/temporal_scan.rs`
- `crates/query-optimizer/src/lib.rs`
- `crates/query-optimizer/tests/operator_fidelity.rs`
- `crates/distributed-query/src/coordinator.rs`
- `crates/distributed-query/tests/coordinator.rs`
- `crates/cypher-engine/tests/end_to_end.rs`
- `crates/gateway-node/src/service.rs`
- `crates/gateway-node/tests/service.rs`

## Review remediation on 2026-07-21

### Additional RED evidence

```text
cargo test -p cypher-compiler --test compile
# exit 101: 15 passed, 2 failed
# - legal UNION schemas with different internal SlotIds were rejected
# - count(value) + 1 produced no Aggregate operator

cargo test -p query-executor --test operators
# exit 101: 3 passed, 1 failed
# - one point UNWIND list with MAX_BATCH_ROWS + 1 items was fully expanded

cargo test -p query-executor --test temporal_rows
# exit 101: 12 passed, 2 failed
# - temporal DISTINCT retained equal values/region with different provenance
# - one interval UNWIND list with MAX_BATCH_ROWS + 1 items was fully expanded

cargo test -p query-optimizer --test partitioning
# exit 101: 4 passed, 1 failed
# - source-free PrimaryReplica plan was placed on Shard(42)

cargo test -p cypher-engine --test end_to_end \
  graph_source_with_pipeline_is_canonical_across_deployment_modes -- --exact
# exit 101: PrimaryReplica count 1, two-nonempty-shard SharedNothing count 2

cargo test -p gateway-node --lib \
  later_candidate_failure_keeps_revision_overlay_and_writes_unchanged
# initial compile RED: the atomic publish boundary did not exist
```

### Additional GREEN evidence and implementation

- UNION compares visible name/type/nullability contracts independently of slots and inserts a
  positional Project that rewrites each later branch into the first branch's canonical slots.
- Aggregate functions are recursively extracted into hidden aggregate slots. A final Project
  evaluates composite expressions; aggregate arguments containing aggregates fail with
  `DTG-CYPHER-NESTED-AGGREGATE`.
- Temporal DISTINCT keys on visible values plus exact `TemporalRegion`; collapsed rows merge sorted,
  deduplicated provenance.
- Point and interval UNWIND reject a single upstream list above `MAX_BATCH_ROWS` before expansion,
  check cancellation/deadline per row, account bytes before retention, and stop at the fragment
  memory limit. `MAX_BATCH_ROWS` is a per-list and per-output-batch bound, not a whole-query result
  bound: expansion across multiple upstream rows is chunked and remains bounded by total fragment
  memory.
- Point exchange inputs and interval UNION rows are accounted before `push`/`extend`; point DISTINCT
  UNION output is rechunked instead of requiring all distinct rows to fit one batch.
- Source-free plans are placed on the coordinator before deployment-mode dispatch. Graph-source
  PrimaryReplica plans remain on their selected primary shard.
- Candidate publication computes the next revision, validates every candidate row, and stages on an
  overlay clone before changing revision, overlay, or writes. The later-row conflict test proves all
  three prior fields remain unchanged.
- Deployment equivalence now compares the same two-element dataset stored on one PrimaryReplica
  shard against two nonempty SharedNothing shards.

Focused final results:

```text
cypher-compiler compile                  17 passed, 0 failed
query-executor operators                 4 passed, 0 failed
query-executor temporal_rows            14 passed, 0 failed
query-optimizer partitioning             5 passed, 0 failed
distributed-query coordinator            8 passed, 0 failed
cypher-engine end_to_end                 28 passed, 0 failed
gateway candidate publication            1 passed, 0 failed
```

The required nine-package command was run again after all review fixes. Result: GREEN (exit 0),
including all unit, integration, process, and documentation tests. The latest-only scan found no
alternate query/IR directory or namespace, compatibility executor, removed language-profile path,
backend Cypher pass-through, or fallback implementation. External live protocol identifiers,
persistent key prefixes, and the Neo4j Query API endpoint remain intentionally unchanged because
they identify the current wire/storage contract rather than an older DTGProxy implementation.

## Point UNION DISTINCT memory remediation on 2026-07-21

### RED

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p query-executor --lib \
  point_union_distinct_accounts_unique_rows_before_retention
```

Result: RED (exit 101). The focused test required `union(input, false, memory_limit)`, but the point
UNION implementation accepted only the input batches and `all` flag. There was no operator-local
budget with which to reject a new unique row before retaining it.

The fixture uses point rows `[1, 1, 2]`. Each integer has an estimated payload of 9 bytes, so the
two-row DISTINCT result is exactly 18 bytes. It requires an 18-byte budget to succeed and a 17-byte
budget to fail deterministically with `MemoryLimitExceeded { limit: 17, required: 18 }`.

### GREEN and implementation

- `BatchExecutor` now passes the physical fragment memory limit into point UNION.
- UNION DISTINCT checks whether the row is new, computes that row's estimated payload, checks the
  next cumulative size, and returns before `push` when the limit would be exceeded.
- UNION ALL also validates its retained batches against the supplied memory limit.
- `batches_from_rows` now consumes the owned row vector and moves at most `MAX_BATCH_ROWS` rows into
  each `RecordBatch`. It no longer keeps the complete owned result while cloning every chunk through
  `chunk.to_vec()`.
- The owned batching test proves `MAX_BATCH_ROWS + 1` ordered rows become batches of 16,384 and 1
  without changing row order. Existing distributed UNION DISTINCT coverage continues to prove
  stable distinct values across gathered branches.

Focused verification:

```text
cargo test -p query-executor --lib executor::tests::
# 2 passed, 0 failed

cargo test -p query-executor -p distributed-query
# exit 0; query-executor and distributed-query unit, integration, and doc tests all passed
# distributed coordinator: 8 passed, 0 failed
```

Required final suite:

```text
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p cypher-syntax -p cypher-sema -p cypher-compiler -p temporal-ir \
  -p query-executor -p query-optimizer -p distributed-query -p cypher-engine -p gateway-node
```

Result: GREEN (exit 0) after the point UNION memory fix. This includes 2/2 query-executor unit
tests, 28/28 Cypher engine end-to-end tests, 17/17 compiler tests, 14/14 temporal-row tests, 8/8
distributed coordinator tests, Gateway process/service integration, and all selected doc tests.
