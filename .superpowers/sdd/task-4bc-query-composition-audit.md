# Task 4B/4C: Current Cypher Query-Composition Audit

Date: 2026-07-20

Scope: read-only design and gap audit for the two tasks following Task 4A.  This document is
current-only: it names no old parser/IR/executor/API compatibility surface, and all proposed work
extends the existing top-level `cypher-* -> temporal-ir -> physical-plan -> distributed-query ->
query-executor -> gateway-node` path.

## Binding constraints

- The approved specification requires Cypher 25 `CALL ... YIELD`, named procedures, `CALL {}`
  subqueries, `EXISTS`/`COUNT` subqueries, correlated/importing scope, and `CALL {} IN
  TRANSACTIONS` (`docs/superpowers/specs/2026-07-19-dtgproxy-temporal-cypher-analytics-design.md`,
  sections 3.2, 5.3, 6.4, 7.2).
- A procedure descriptor must declare input/output schema, permissions, determinism, side effects,
  resource limits, and allowed Cypher profile.  Procedure execution is a normal row-producing
  query operator, not a JSON side channel.
- A single query snapshot, graph/schema/topology/security identities, cancellation/deadline and
  resource limits must cross every nested plan and procedure/provider boundary.  PrimaryReplica
  and SharedNothing use the same language/transaction contracts.
- Do not send source Cypher to any backend.  Do not add a versioned namespace, text executor,
  backend-Cypher fallback, or legacy adapter.

## Current execution spine (reusable)

1. `cypher-syntax` already identifies top-level clause boundaries while honoring brace/paren
   nesting.  It puts a named call in `Clause::procedure: ProcedureCall { name, arguments: String,
   yield_items }`; it puts `CALL { ... }` body text in `Clause::subquery`.
2. `cypher-sema::SemanticAnalyzer::analyze_with_bindings` already accepts an initial scope.
   This is the correct entry point for importing/correlated subquery analysis once import rules are
   enforced explicitly.
3. `cypher-compiler::Lowerer` owns lexical slot allocation, `RowSchema`, source-free
   `Argument`, `Unwind`, projections and `LogicalPlanBuilder` validation.  The physical optimizer
   and `distributed-query` already route coordinator-only branches, scans, unions, joins and
   temporal row operators.
4. `query-executor` has bounded `RecordBatch`, `ExecutionContext` carrying parameters,
   cancellation/deadline and `GraphOverlay`; `RuntimeValue`, expression evaluation, projections,
   aggregate, `Unwind`, and row limits are directly reusable.  `gateway-node` constructs a fixed
   snapshot and overlay for explicit transactions and stages physical writes atomically through
   the existing candidate/revision-fenced path.
5. `analytics-api::{AnalyticsProvider, AlgorithmRequest, AlgorithmResult}` plus
   `analytics-runtime::BuiltInProvider` are reusable only as one provider implementation behind a
   general procedure registry.  `AlgorithmResult` already provides columns plus rows, which is
   sufficient to adapt an analytics procedure result to `RecordBatch` after type/schema
   validation.

## 4B: Named procedures and `CALL ... YIELD`

### Existing behavior

- Parser support is shallow but present:
  `parse_procedure_call` captures the qualified text name and raw argument text;
  `parse_yield_items` supports only `field` and `field AS alias`.  The separate `YIELD` clause is
  retained as a clause boundary.
- The compiler produces `LogicalOperator::ProcedureCall { procedure_id: stable_id(lowercase_name) }`
  and appends each requested YIELD field to the current schema as nullable `ValueType::Any`.
  `CompiledQuery` retains only the first procedure descriptor (`procedure_descriptor` finds the
  first `CALL`).
- The optimizer maps that logical operator to `PhysicalOperator::Procedure`, but
  `BatchExecutor::execute_operators` returns `RuntimeError::UnsupportedOperator("Procedure")`.
- Semantic analysis assigns `QueryEffect::Procedure` to every named call and does not consult a
  procedure signature, bind arguments, validate output fields/types, distinguish read/write
  effects, or validate YIELD duplicates/shadowing against an authoritative schema.
- Gateway handles a `QueryEffect::Procedure` as an analytics-only short circuit:
  `parse_algorithm_call` accepts only `dtg.*`, only parses a map-shaped raw argument string, and
  `execute_analytics_call` projects all shards to a snapshot then invokes `BuiltInProvider`.
  HTTP returns JSON.  Bolt returns one `result` string column containing that JSON, so requested
  YIELD fields and any following `WITH`/`RETURN` do not execute.  Procedure calls are rejected in
  an explicit Bolt transaction.

### Blockers and required ownership

| Owner | Required current-only change | Reuse / constraint |
|---|---|---|
| `cypher-ast`, `cypher-syntax` | Replace raw procedure arguments with parsed expression arguments; represent `YIELD *`, aliases and the clause span structurally. | Keep one AST, source spans and Cypher 25 scanner/profile.  Do not parse the call body again in Gateway. |
| Procedure catalog (new current module/crate at the planned `procedure-runtime` boundary) | Define `ProcedureSignature`/descriptor: qualified name, input schema/defaults, output `RowSchema`, effect, determinism, permission/capability, resource budget, permitted profile; define invocation trait that receives execution context and input rows and yields bounded batches. | Register a built-in adapter for analytics providers, but do not hard-code `dtg.*` or `BuiltInProvider` in Gateway. |
| `cypher-sema` | Resolve name against catalog; type-check positional/named arguments, validate requested YIELD fields and aliases, establish the output lexical scope, enforce side-effect ordering and explicit-transaction eligibility. | `Scope` and `analyze_with_bindings` are reusable.  Unknown/missing/duplicate fields must fail before lowering. |
| `temporal-ir`, `physical-plan` | Enrich `ProcedureCall` with a stable catalog identity, argument scalar expressions, output binding map/schema, and declared execution placement/effect; plan validation must validate input slots and output schema. | Existing operator and header identity validation are reusable.  Stable hash alone is not an authority boundary. |
| `query-optimizer`, `distributed-query`, `query-executor` | Select coordinator/shard placement from descriptor.  Execute a procedure for each upstream row (or once for a source-free `Argument`), preserve upstream bindings, append validated yielded values, enforce row/byte/deadline/cancel limits. | `ExecutionContext`, `RecordBatch`, `BatchExecutor`, coordinator placement and resource fences are reusable. |
| `gateway-node` | Construct an invocation context from routing + fixed snapshot + overlay; expose normal `CypherQueryResponse`/Bolt fields and rows.  Permit only descriptor-approved read-only calls in an explicit transaction; mutation procedures must stage through the same statement candidate path or be rejected. | Replace only the current `compiled.is_procedure()` analytics short circuit.  Retain snapshot/overlay lifetime. |

### Design decisions required before code

1. The procedure catalog is the authority for both semantic schema and runtime behavior.  A
   `ProcedureCall` must reference the resolved descriptor identity/version rather than an
   unverified `u32` hash.  Cache keys must include catalog/procedure revision.
2. Invoke procedures in the coordinator initially unless a descriptor explicitly marks a
   deterministic shard-local implementation and its partitioning contract is satisfiable.  Global
   analytics remains coordinator/provider-owned; it must return typed rows, not serialized JSON.
3. A named procedure with an empty YIELD list still executes only where Cypher permits it; its
   result rows must not accidentally multiply a following graph pipeline.  Encode this in the
   descriptor/call semantics rather than special-casing Gateway.
4. Procedure effects must be a richer property than `QueryEffect::Procedure`.  A query containing
   `MATCH ... CALL readProc ... RETURN` is a read query; one containing a registered write
   procedure is a write query and must participate in transaction staging.  Unregistered or
   disallowed procedures fail before timestamp allocation.

### Required named tests

Add focused tests in the owning crates and one end-to-end Bolt test:

- `cypher_syntax::parses_qualified_call_typed_arguments_yield_star_and_aliases`
- `cypher_syntax::rejects_call_yield_after_nested_argument_and_duplicate_alias`
- `cypher_sema::resolves_procedure_signature_and_types_yield_scope`
- `cypher_sema::rejects_unknown_procedure_argument_output_permission_and_illegal_effect_order`
- `cypher_compiler::lowers_correlated_procedure_call_with_input_slots_and_output_schema`
- `temporal_ir::rejects_procedure_plan_with_unknown_argument_slot_or_schema_mismatch`
- `query_executor::procedure_call_executes_once_per_input_row_preserves_bindings_and_limits_rows`
- `query_executor::procedure_error_cancel_and_deadline_abort_without_partial_rows`
- `distributed_query::shard_local_procedure_is_canonical_in_primary_replica_and_shared_nothing`
- `gateway_node::bolt_call_yield_returns_descriptor_columns_and_rows_for_following_with_return`
- `gateway_node::explicit_transaction_read_procedure_uses_begin_snapshot_and_overlay`
- `gateway_node::procedure_is_not_dispatched_to_backend_or_json_result_side_channel`

## 4C: `CALL {}` read/write subqueries, EXISTS/COUNT subqueries, importing scope, and `IN TRANSACTIONS`

### Existing behavior

- `CALL { ... }` recognition is top-level only.  `parse_subquery_call` stores a raw body string,
  takes the last `}`, and rejects every suffix; therefore `IN TRANSACTIONS` is rejected.  It cannot
  represent import syntax, batch configuration, nested braces robustly as a structured node, or
  an expression subquery.
- `cypher-sema` reparses the raw subquery and passes **all** outer bindings into
  `analyze_with_bindings`.  It only permits a `ReadOnly` subquery and then copies every output field
  into the outer scope.  This means imports are implicit rather than Cypher's explicit importing
  scope, write/procedure subqueries are hard rejected, shadowing/collision rules are not modeled,
  and no cardinality/apply semantics are checked.
- The compiler repeats the read-only rejection and recursively calls `self.clause` for subquery
  clauses in the *same* `Lowerer`.  This is an inline splice, not an isolated child plan.  It
  accidentally supports simple correlated reads but cannot provide independent child schemas,
  row-per-outer-row apply semantics, null/empty-row behavior, subquery writes, or separate plan
  identities.  It also rejects subquery graph/temporal selectors, which is correct inheritance
  direction but must become explicit child context inheritance.
- The AST `Expression` only has literals, collections, unary/binary/property/index and ordinary
  `FunctionCall`.  It has no `ExistsSubquery` or `CountSubquery` node; the expression parser treats
  `EXISTS`/`COUNT` as regular function syntax and cannot consume a query body.  `ScalarExpr` also
  has no subquery expression/evaluation hook.
- `LogicalOperator` has joins but no `Apply`, `SemiJoin`, `AntiJoin`, child-plan container, or
  subquery transaction operator.  The physical plan and batch executor consequently have no
  nested plan invocation.  Current query execution accepts one text request and one physical plan
  through `CypherQueryEngine`; it has no recursive input-row API.
- Gateway has the necessary explicit transaction fixed timestamp, overlay, atomic candidate and
  commit machinery, but only top-level writes call it.  A read-only subquery eventually reaches
  standard response execution; a write subquery cannot enter it.  `CALL {} IN TRANSACTIONS` has no
  independent batch commit/retry/error policy and must never reuse an outer explicit transaction.

### Blockers and required ownership

| Owner | Required current-only change | Reuse / constraint |
|---|---|---|
| `cypher-ast`, `cypher-syntax` | Add structured `CallSubquery { imports, body: QueryStatement, in_transactions: Option<BatchSpec> }`; parse nested braces with token depth, import `WITH`/scope syntax, batch size and supported error policy.  Add `Expression::{ExistsSubquery, CountSubquery}` with structured query body. | Retain current AST/profile.  Do not retain raw string body or rerun top-level parser from Gateway/compiler. |
| `cypher-sema` | Analyze child query with only explicit imported symbols (with Cypher 25 scope rules); reject accidental outer references, name collisions and invalid exported names; derive composed effects.  Validate `EXISTS` returns Boolean and `COUNT {}` returns non-negative Integer independent of inner columns. | Reuse `Scope`, `CypherType`, and current semantic error envelope. |
| `cypher-compiler`, `temporal-ir` | Lower every child to an isolated child plan, then connect it to the outer input with `Apply` carrying import slots/export slots and apply kind.  Lower EXISTS to semi/anti-style scalar result and COUNT to correlated aggregate scalar.  Lower `IN TRANSACTIONS` to a batch subtransaction operator that owns batching and error policy. | Existing `LogicalPlanBuilder`, schemas and `Argument` provide child root input.  Add plan ids/recursive validation with bounded depth/node/batch limits. |
| `physical-plan`, optimizer, distributed runtime | Make child fragments explicit and identity-fenced; choose decorrelation to join/semi-join only when valid, otherwise execute bounded nested Apply.  Route each child under the inherited read snapshot and temporal context. | Reuse exchanges, coordinator rows and interval operators.  Never flatten a write child into the parent operator list. |
| `query-executor` | Add an invocation interface accepting an input `RecordBatch` and child physical plan/context; define apply row multiplicity, empty child result and scalar subquery behavior; preserve `TemporalRow` intervals/provenance for read children. | Reuse `ExecutionContext`, but child contexts must inherit immutable snapshot, graph overlay and fences. |
| `gateway-node` / transaction coordinator | For ordinary write subqueries, construct one statement candidate over all outer rows and publish only after all child rows validate; failures leave overlay and staged writes unchanged.  For `IN TRANSACTIONS`, isolate fixed-size input batches into independently committed transactions with unique TSO timestamps and deterministic reporting; reject it within an explicit Bolt transaction. | Reuse `prepare_cypher_writes`, candidate validation, revision fence and coordinator commit.  Do not share the parent's fixed snapshot/commit timestamp across batch transactions. |

### Required semantics to lock before implementation

1. Normal `CALL {}` inherits graph, valid-time, transaction snapshot, security fingerprint,
   cancellation and overlay from its parent.  It does **not** implicitly inherit every variable:
   imports are explicit and child exports are the only new outer bindings.  This is the correction
   to the current broad `analyze_with_bindings` call.
2. Read `CALL {}` is a correlated Apply: execute once per outer row and multiply it by child rows;
   no child row removes the outer row for an inner apply.  Use a distinct optional/left-apply only
   when Cypher syntax/semantics require it.  Do not use the current inline lowering because it
   loses that boundary.
3. `EXISTS {}` is a Boolean semi-apply and may stop after the first visible child row; `COUNT {}`
   is a correlated scalar aggregate over all visible child rows.  Both must honor NULL/empty input,
   point/interval temporal regions and snapshot identity.
4. Ordinary `CALL {}` writes are part of the parent statement/explicit transaction: all outer-row
   child mutations stage in the same overlay and either publish together or not at all.
5. `CALL {} IN TRANSACTIONS [OF n ROWS]` is the sole exception: split the outer rows deterministically
   into bounded batches; each receives an independent auto-commit transaction context, snapshot and
   retry/error result.  It is rejected in an explicit transaction, with a parent write overlay, or
   in any form that would promise global rollback.  Its visibility and summary follow batch commit
   order, never a fictitious single atomic statement.

### Required named tests

Parser/AST:

- `cypher_syntax::parses_nested_call_subquery_with_imports_and_batch_spec`
- `cypher_syntax::parses_exists_and_count_subquery_expressions`
- `cypher_syntax::rejects_unbalanced_subquery_suffix_invalid_batch_size_and_legacy_call_form`

Semantic/compiler/IR:

- `cypher_sema::subquery_requires_explicit_import_and_exports_only_declared_columns`
- `cypher_sema::subquery_rejects_outer_leak_shadow_collision_and_invalid_write_context`
- `cypher_compiler::lowers_correlated_apply_with_separate_parent_child_schemas`
- `cypher_compiler::lowers_exists_to_semi_apply_and_count_to_scalar_aggregate`
- `temporal_ir::rejects_recursive_apply_cycle_unknown_import_slot_and_unbounded_batch_spec`

Runtime/distributed temporal correctness:

- `query_executor::correlated_read_subquery_runs_per_outer_row_and_empty_child_has_inner_apply_semantics`
- `query_executor::exists_short_circuits_and_count_handles_zero_null_and_multiple_rows`
- `query_executor::interval_subquery_apply_intersects_regions_without_losing_provenance`
- `distributed_query::correlated_subquery_is_snapshot_identical_in_primary_replica_and_shared_nothing`

Gateway/transactions:

- `gateway_node::call_subquery_write_stages_all_outer_rows_and_failed_child_leaves_overlay_unchanged`
- `gateway_node::explicit_transaction_subquery_read_and_write_use_the_begin_snapshot`
- `gateway_node::call_in_transactions_commits_deterministic_batches_with_distinct_timestamp_pairs`
- `gateway_node::call_in_transactions_failure_reports_batch_and_never_rolls_back_prior_commits`
- `gateway_node::call_in_transactions_is_rejected_inside_explicit_transaction`

## Recommended sequence and acceptance gates

1. Complete Task 4A first, retaining its shared UNION child-plan behavior.
2. Implement 4B catalog/signature/typed-row procedure path before 4C.  This removes Gateway's
   current whole-query analytics bypass and gives subqueries a valid effect/capability model.
3. Implement 4C in three internally testable slices: structured AST + semantic scopes; recursive
   read Apply/EXISTS/COUNT; then write Apply and independent `IN TRANSACTIONS` batching.
4. Each slice must retain point and interval behavior, fixed snapshots, explicit transaction
   overlay atomicity, current resource limits, and both deployment modes.  Run focused crate tests
   first; only after 4B and 4C are complete proceed to the planned unified boundary audit.

## Audit conclusion

The current implementation deliberately contains scaffolding for these surfaces but not their
runtime semantics.  The main reusable assets are the current row schema/slot system, bounded
batches, execution context, physical coordinator, provider result tables, and fixed-snapshot write
candidate path.  The critical correction is architectural: procedures and subqueries must become
typed, independently validated row-plan boundaries.  They cannot be completed safely by adding
more raw-string parsing or Gateway-only special cases.
