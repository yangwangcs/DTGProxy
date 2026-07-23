# Task 4C: Structured Cypher 25 subqueries and transactional Apply

## Context

Tasks 4A and 4B establish the current-only row pipeline and typed procedure boundary. The remaining
subquery scaffold is not an implementation: `CALL {}` retains raw source, semantic analysis leaks
all outer bindings into a reparsed body, the compiler splices clauses into one parent plan, and
EXISTS/COUNT/`IN TRANSACTIONS` have no typed AST, isolated child plan, or runtime semantics.

Replace that scaffold with one structured Cypher 25 implementation. Do not retain the raw-string
path, a second parser/executor, Gateway text inspection, backend Cypher pass-through, or a versioned
query namespace.

## Required behavior

### 1. Structured syntax and AST

1. Represent a call subquery as a structured AST containing explicit imports, a parsed child query,
   exported fields, and an optional typed `IN TRANSACTIONS` batch specification.
2. Represent `EXISTS { ... }` and `COUNT { ... }` as structured expression-subquery AST nodes whose
   bodies are parsed once. They are not ordinary function calls.
3. Parse braces with lexer/token depth so nested maps, expressions, subqueries, and UNION branches
   cannot terminate a body early. Parse the complete suffix and reject trailing garbage.
4. Support only the current Cypher 25 scoped form `CALL (x, y) { ... }` (and `CALL () { ... }` for
   no imports), plus `CALL (x, y) { ... } IN TRANSACTIONS [OF n ROWS]`. Do not support the removed
   implicit/importing-`WITH` form. Require a positive, statically bounded batch size and one
   explicitly supported error policy. Reject zero, overflow, dynamic/unbounded sizes, incompatible
   suffixes, and every raw/legacy call form.
5. Enforce existing parser depth, token, expression, query-node, and input-byte bounds recursively.

### 2. Semantic ownership and effects

1. Analyze a child against only the variables listed in its scoped `CALL (...)` import list plus
   inherited graph/temporal/security context.
   An outer variable that was not imported is an error.
2. Child exports are the only bindings added to the parent. Reject duplicate exports, parent/export
   name collisions, invalid shadowing, unknown imports, and imports not visible at the call site.
3. Compose child read/write/procedure effects with the parent in statement order. Ordinary write
   subqueries are legal only when they can stage every mutation through the existing candidate and
   revision-fenced transaction path; no prefix execution or placeholder success is allowed.
4. Type `EXISTS {}` as non-null Boolean and `COUNT {}` as non-null, non-negative Integer independent
   of child output columns. Preserve Cypher three-valued expression semantics around their result.
5. Reject `IN TRANSACTIONS` inside an explicit transaction, beneath a parent write overlay, in a
   nested context that promises global rollback, or with an unsupported error policy before TSO,
   projection, provider, or backend work.

### 3. Isolated logical and physical child plans

1. Every child query has its own plan identity, root `Argument`, input/output schema, slot namespace,
   effect, snapshot fingerprint, graph identity, temporal context, limits, and validation boundary.
2. Connect parent and child with typed `Apply` operators carrying apply kind, child plan identity,
   explicit import slot mapping, explicit export slot mapping, and exact output schema.
3. Read `CALL {}` uses inner correlated Apply: execute the child once per outer row, multiply by
   child rows, and remove the outer row when the child produces none. Optional/left Apply exists only
   for syntax that explicitly requires it.
4. Lower `EXISTS {}` to a Boolean semi-apply that may stop after the first visible child row. Lower
   `COUNT {}` to a correlated scalar aggregate that consumes all visible child rows and returns zero
   for no rows.
5. Lower `IN TRANSACTIONS` to an explicit batch-subtransaction operator. It owns deterministic input
   batching, independent transaction contexts, retry/error reporting, and batch summaries; it is not
   a flag on ordinary Apply.
6. Recursive validation proves acyclicity, unique child identities, known import/export slots, exact
   schemas, bounded recursion depth/node count/batch size, legal effect ordering, one graph identity,
   and matching snapshot/security/temporal fences.
7. Optimizer decorrelation to join/semi-join is permitted only when equivalence is proven. Otherwise
   retain bounded nested Apply. Never flatten a write child into the parent's operator sequence.

### 4. Runtime, interval, and distributed semantics

1. Add a child-plan invocation API accepting a bounded input `RecordBatch`, immutable inherited
   `ExecutionContext`, graph overlay, and child physical plan. Cancellation, deadline, memory,
   invocation, output, and recursion budgets apply before retention and across nested calls.
2. A normal child inherits the parent's fixed transaction snapshot, graph, valid-time scope,
   topology/schema/placement identities, security fingerprint, cancellation token, and current
   transaction overlay. It never allocates a fresh snapshot.
3. Interval execution intersects parent and child temporal regions. Empty intersections produce no
   inner-apply row. Preserve and combine `TemporalProvenance`, then coalesce only equivalent adjacent
   regions under the existing temporal-row rules.
4. Distributed execution routes child fragments under the same fenced snapshot and produces
   canonical-equivalent results in PrimaryReplica and Shared-Nothing. A source-free child is not
   multiplied by shard count. Missing shards never produce partial success.
5. EXISTS short-circuiting must cancel/stop remaining child work without exposing partial rows;
   COUNT must include every visible child row exactly once.

### 5. Write subqueries and `IN TRANSACTIONS`

1. Ordinary write subqueries stage mutations for every outer row into the parent statement overlay.
   Validate the complete candidate before publication. Any child failure leaves the pre-statement
   overlay and durable graph unchanged.
2. In an explicit transaction, ordinary read/write subqueries use the BEGIN snapshot and existing
   overlay; successful writes remain staged until COMMIT and disappear on ROLLBACK.
3. `IN TRANSACTIONS` deterministically partitions outer rows into fixed-size batches. Each batch
   receives a distinct auto-commit transaction id, start timestamp, commit timestamp, candidate,
   overlay, retry scope, and result summary.
4. Successful earlier batches remain committed if a later batch fails. Report the failing batch,
   committed batch count, row counts, transaction identities/timestamps, and stable error without
   claiming statement-wide rollback.
5. Batch commit order defines visibility and summary order. Replays use deterministic request/batch
   identity and must not duplicate a committed batch.

## Stable errors and resource rules

- Use the existing DTG/Cypher error envelope. Do not expose backend strings.
- Add stable errors for invalid import/export scope, recursive plan violation, invalid batch spec,
  illegal transactional context, child snapshot/security mismatch, nested budget exhaustion, and
  batch failure.
- No child plan, row batch, candidate mutation set, or result table may be fully retained before its
  owning byte/row/node budget is checked.
- No blocking parser, provider, backend, or transaction operation may run on a Tokio worker thread.

## Required TDD coverage

Record actual RED and GREEN evidence for at least these named behaviors.

Parser/AST:

- `parses_nested_call_subquery_with_imports_and_batch_spec`
- `parses_exists_and_count_subquery_expressions`
- `rejects_unbalanced_subquery_suffix_invalid_batch_size_and_legacy_call_form`

Semantic/compiler/IR:

- `subquery_requires_explicit_import_and_exports_only_declared_columns`
- `subquery_rejects_outer_leak_shadow_collision_and_invalid_write_context`
- `lowers_correlated_apply_with_separate_parent_child_schemas`
- `lowers_exists_to_semi_apply_and_count_to_scalar_aggregate`
- `rejects_recursive_apply_cycle_unknown_import_slot_and_unbounded_batch_spec`

Runtime/distributed temporal correctness:

- `correlated_read_subquery_runs_per_outer_row_and_empty_child_has_inner_apply_semantics`
- `exists_short_circuits_and_count_handles_zero_null_and_multiple_rows`
- `interval_subquery_apply_intersects_regions_without_losing_provenance`
- `correlated_subquery_is_snapshot_identical_in_primary_replica_and_shared_nothing`

Gateway/transactions:

- `call_subquery_write_stages_all_outer_rows_and_failed_child_leaves_overlay_unchanged`
- `explicit_transaction_subquery_read_and_write_use_the_begin_snapshot`
- `call_in_transactions_commits_deterministic_batches_with_distinct_timestamp_pairs`
- `call_in_transactions_failure_reports_batch_and_never_rolls_back_prior_commits`
- `call_in_transactions_is_rejected_inside_explicit_transaction`

Also cover nested UNION children, empty child results, NULL imported values, duplicate/colliding
exports, cancellation/deadline while a child runs, recursive depth/node/row/byte limits, malformed
physical child plans, replay after a committed batch, and no backend query-string dispatch.

## Implementation sequence

1. Structured AST/parser plus semantic import/export/effect scopes.
2. Isolated read Apply, EXISTS, and COUNT through IR, physical plan, executor, and both deployment
   modes, including interval regions/provenance.
3. Ordinary write Apply using the parent statement/explicit transaction candidate and overlay.
4. Independent `IN TRANSACTIONS` batching, commit/retry/reporting, and explicit-transaction rejection.

Each slice must pass its owning crate tests before the next begins. After all slices, run the full
query/transaction package set, `cargo fmt --all -- --check`, and `git diff --check`. Do not commit.
Append RED/GREEN evidence, implementation notes, self-review, test commands/results, and concerns to
`.superpowers/sdd/task-4c-structured-subqueries-report.md`.
