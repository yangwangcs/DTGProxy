# Task 4C structured subqueries report

## Slice 1: structured AST, parser, and semantic scopes

Status: complete in the uncommitted worktree.

### RED evidence

- `cargo test -p cypher-syntax --test structured_subqueries`
  failed because `SubqueryErrorPolicy`, `Clause::call_subquery`, and
  `Expression::{ExistsSubquery,CountSubquery}` did not exist. This proved the parser test was
  exercising the missing structured surface rather than the raw-string scaffold.
- `cargo test -p cypher-sema --test subqueries`
  failed because `CALL (...) {}` was misclassified as a procedure and because the old analyzer
  inherited every outer binding.
- `exists_and_count_subqueries_validate_scope_and_have_stable_types` initially failed because an
  unbound variable in an expression child was silently accepted.
- `rejects_in_transactions_until_the_batch_subtransaction_operator_is_lowered` initially failed
  because the compiler silently lowered the batch form as an ordinary inline CALL.

### Implementation

- Replaced `Clause::subquery() -> Option<&str>` with typed `CallSubquery`, explicit imports,
  structured child `QueryStatement`, exports, and a bounded typed `InTransactions` specification.
- Added structured `Expression::ExistsSubquery` and `Expression::CountSubquery` bodies.
- CALL and expression bodies use lexer token depth, recursively use current parser limits, and
  contribute recursively to AST-node limits.
- Only Cypher 25 scoped `CALL (...) {}` is accepted. Raw `CALL {}`, unbalanced bodies, invalid
  suffixes, zero/dynamic/overflow/over-limit batches, and trailing tokens fail closed.
- Semantic analysis constructs the child scope from only the explicit CALL imports. It rejects
  duplicate/unknown imports, duplicate exports, export-schema mismatches, and parent/export name
  collisions. Only child RETURN fields enter the parent scope.
- EXISTS/COUNT children inherit the visible expression scope, are recursively analyzed under the
  same profile/catalog/access policy, have Boolean/Integer types, and reject write effects.
- The compiler consumes the structured child AST directly. It no longer reparses a raw body.
  Until the dedicated operator exists, `IN TRANSACTIONS` and expression subqueries fail closed.
- All prior tests were migrated to `CALL () {}` or `CALL (name) {}`. No compatibility alias for
  the removed importing-WITH/raw CALL form was retained.

### GREEN evidence

- Parser/AST named tests: 3 passed, 0 failed.
- Semantic named tests: 3 passed, 0 failed.
- Compiler fail-closed batch test: 1 passed, 0 failed.
- `cargo test -p cypher-ast -p cypher-syntax -p cypher-sema -p cypher-compiler`:
  77 passed, 0 failed; all doc tests passed.
- `cargo check --workspace --all-targets`: exit 0.
- `cargo fmt --all -- --check`: exit 0.
- `git diff --check`: exit 0.

## Slice 3 completion and Slice 4 functional implementation

### Completed ordinary child-write semantics

- Child-local MATCH/UNWIND prefixes now execute through the Gateway at the inherited fixed snapshot
  and current transaction overlay. Their bounded rows feed the isolated structured mutation
  program; nested child prefixes recurse through the same path.
- Write-subquery exports now preserve Cypher row multiplication. A child that returns two rows no
  longer fails with `DTG-CYPHER-WRITE-SUBQUERY-MULTIROW-EXPORT`; each exported row feeds the
  following parent mutations, and the resulting temporal transactions are merged deterministically.
- Top-level and nested CREATE identity seeds are request-scoped. Distinct statements in one
  explicit transaction cannot alias the same anonymous element merely because they use the same
  variable position.
- Shared-Nothing point traversal now routes exact destination identity/history reads to the owning
  Shard while keeping graph-wide scans local. Cross-Shard relationships therefore retain their
  remote destination instead of being dropped during Expand.

### `IN TRANSACTIONS`

- The compiler lowers the current Cypher 25 suffix to explicit
  `LogicalOperator::BatchSubtransaction` and `PhysicalOperator::BatchSubtransaction` nodes. Batch
  size is typed and validated independently from ordinary Apply.
- Auto-commit execution partitions the resolved outer rows into fixed-size batches. Every batch
  receives a deterministic derived request identity, a distinct start/commit timestamp pair, its
  own candidate and commit, and an ordered summary containing batch index, row range, row count,
  transaction identity, timestamps, and participants.
- Explicit Bolt transactions reject the batch operator with
  `DTG-CYPHER-IN-TRANSACTIONS-EXPLICIT` before timestamp allocation or write preparation.
- Successful request replay returns the retained ordered batch summary and does not issue duplicate
  batch commits in the live Gateway process.
- A later preparation or commit failure is wrapped as
  `DTG-CYPHER-IN-TRANSACTIONS-BATCH-FAILED` with the failing batch, committed batch/row counts,
  committed summaries, and `statement_rolled_back: false`.

### RED/GREEN evidence

- Multi-row write export first failed with
  `DTG-CYPHER-WRITE-SUBQUERY-MULTIROW-EXPORT`; the focused regression now passes and the full write
  materializer target passes 14/14.
- Cross-Shard traversal first returned zero rows after request-scoped IDs distributed endpoints
  across physical Shards. The Gateway integration now returns both relationships and completes its
  later explicit-transaction snapshot/overlay assertions.
- The batch compiler regression first failed because no `BatchSubtransaction` logical variant or
  compiled batch accessor existed; it now passes.
- The Gateway batch regression first returned one ordinary `cypher_write`. It now commits three
  rows as two ordered batches (2 + 1), returns distinct transaction identities, and an identical
  replay leaves exactly three durable vertices.
- Six-package regression command:
  `cargo test -p temporal-ir -p physical-plan -p query-optimizer -p cypher-compiler -p cypher-engine -p distributed-query`
  completed with exit 0. This includes compiler 23/23, engine end-to-end 29/29, write materializer
  14/14, distributed coordinator 10/10, distributed subqueries 9/9, physical validation 13/13,
  optimizer 7/7 at the initial suite checkpoint, and Temporal IR validation 7/7. A subsequent
  dedicated physical batch-boundary regression exposed and fixed missing Write output Schema;
  physical-plan 13/13 and query-optimizer 8/8 then passed.

### Slice 4 completion after recovery and interval rework

- Batch replay no longer depends on Gateway-process memory. Each batch claims a deterministic
  distributed constraint-ledger key derived from cluster, graph, request, query fingerprint, and
  batch index. The claim and business mutations commit atomically in the same distributed
  transaction; replay reconstructs the original transaction summary from the committed owner.
- The real later-batch failure regression commits the first two rows, rejects the malformed next
  batch with `DTG-CYPHER-IN-TRANSACTIONS-BATCH-FAILED`, reports committed rows/batches and
  `statement_rolled_back: false`, and proves replay leaves exactly the already committed rows.
- Shared-Nothing interval Expand now extracts remote destination identities from matched edge
  segments, routes their historical segment reads to the owning Shards, and performs the normal
  source/edge/destination temporal intersection without dropping cross-Shard endpoints.

Task 4C is complete. Remaining work belongs to the analytics feature track and the later unified
boundary/engineering audit, not structured subquery semantics.

## Slice 2: distributed read Apply, interval Apply, and EXISTS short-circuit

Status: complete in the uncommitted shared worktree.

### RED evidence

- Initial command: `cargo test -p distributed-query --test subqueries -- --nocapture`.
  The test itself did not compile because it referenced undeclared `futures_lite`, passed `u64`
  where the storage key API now requires `u128`, and called crate-private
  `RecordBatch::into_rows`. The fixture was changed only to native async/await, explicit `u128`
  conversion, and the public borrowed `rows()` accessor.
- The next runs exposed two more fixture-only failures before feature RED:
  `NonContiguousLogIndex { expected: 1, actual: 2 }` because shard-local Raft indexes reused
  element ids, then `DuplicateWorker(0)` because the helper hard-coded shard zero. The fixture now
  uses local contiguous indexes and the requested shard identity.
- Effective optimizer RED, same command:
  `Physical("physical plan validation failed: FragmentOutputMismatch(FragmentId(0))")`.
  Root cause: the correlated child `Argument` carried imported slots logically, but
  `PhysicalOperator::Argument` erased its schema to empty during validation.
- After preserving the physical Argument schema, the same test advanced to the required runtime
  RED:
  `Execution("physical query execution failed: UnsupportedOperator(\"distributed Apply child plan\")")`.
  This proved the child was isolated and valid but had no distributed invocation path.
- After adding the invocation path, RED advanced once more to
  `UnsupportedOperator("HashJoin")`: the coordinator did not yet connect the child Argument branch
  with the shard scan branch. A typed bounded coordinator join was then added.
- The pre-fix executor materialized all child rows before deciding EXISTS. The regression
  `local_exists_stops_before_retaining_the_second_visible_child_row` uses a 9-byte child budget and
  two visible 9-byte rows: full materialization necessarily fails, while true first-row execution
  succeeds. `distributed_exists_stops_after_first_visible_shard_row` uses counting workers and
  requires the second shard invocation count to stay zero.

### Implementation

- `PhysicalOperator::Argument` now carries its exact output schema. This makes imported child slots
  explicit in physical validation and execution rather than relying on an out-of-band convention.
- Added the transport-neutral borrowed `ChildPlanInvoker` boundary with typed point and interval
  entry points and `ChildOutputDemand::{AllRows, FirstVisibleRow}`. It accepts only a bounded typed
  batch/temporal rows, immutable inherited `ExecutionContext`, and a child `PhysicalPlan`; no query
  text, backend query string, or logical AST crosses the boundary.
- `DistributedCoordinator` implements child invocation by recursively executing the isolated child
  physical DAG with the exact same cloned `SnapshotToken`, valid-time point/window, absolute
  deadline, batch bound, cancellation state, graph overlay, procedure/security context, and worker
  snapshot fences.
- Correlated input is injected only into an incoming-free coordinator fragment whose first operator
  is the child `Argument` with an exact matching schema. Shard scans and other source fragments
  never receive the correlated batch. A source-free child stays coordinator-only and executes once
  per Apply invocation, never once per shard.
- Added the bounded typed coordinator row join needed for the isolated
  `Argument x distributed scan` child DAG. Missing/failed shards still fail the query; ordinary
  Apply and COUNT never expose partial results.
- Interval child invocation uses the same distributed DAG and snapshot, intersects the child rows
  with the parent temporal region, combines parent/child `TemporalProvenance`, and leaves existing
  equivalent-region coalescing rules in control.
- EXISTS passes `FirstVisibleRow` explicitly. Distributed short-circuit is enabled only for a
  conservatively proven shape; it stops before starting remaining workers once a shard result is
  sufficient and then completes the required row-preserving coordinator suffix. Unsafe shapes
  containing Filter, Expand, Unwind, Aggregate, Sort, Skip, Limit, Union, Procedure, nested Apply,
  or write work fully execute before the Boolean result is decided.
- Coordinator-only local EXISTS also has true first-row execution for the conservative
  Argument/Unwind/Project/TemporalSlice/Finish shape. UNWIND stops generation after the first row
  only when every remaining operator is proven nonempty-preserving; Filter, another Unwind,
  Expand, and every Limit are deliberately excluded.
- Existing synchronous interval coordinator tests were migrated to async calls because distributed
  interval child invocation must await shard work without blocking a Tokio worker.

### Files changed for Slice 2

- `crates/physical-plan/src/lib.rs`
- `crates/physical-plan/tests/validate.rs`
- `crates/query-optimizer/src/lib.rs`
- `crates/query-optimizer/tests/operator_fidelity.rs`
- `crates/query-executor/src/{child_invocation,error,executor,lib,temporal}.rs`
- `crates/query-executor/tests/{operators,procedures,subqueries,temporal_rows}.rs`
- `crates/distributed-query/src/coordinator.rs`
- `crates/distributed-query/tests/{coordinator,subqueries}.rs`

### GREEN evidence

- `CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p query-executor --no-default-features`
  passed: 52 tests, 0 failed; doc tests passed.
- `CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p distributed-query`
  passed: 20 tests, 0 failed; doc tests passed. This includes all three distributed subquery tests:
  PrimaryReplica/Shared-Nothing snapshot equivalence, distributed interval Apply with provenance,
  and counting-worker EXISTS short-circuit.
- `CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p physical-plan -p query-optimizer`
  passed: 18 tests, 0 failed; doc tests passed.
- `cargo fmt --all -- --check`: exit 0.
- `git diff --check`: exit 0.

### Self-review and concerns

- Reviewed the invocation data flow from parent Apply through child Argument, shard fragments,
  coordinator exchanges, and result remapping. The child receives no fresh snapshot and no backend
  text. Parent input is absent from scan/source-free shard branches by construction.
- Reviewed early-exit safety separately from result truncation. `FirstVisibleRow` is an explicit API
  demand, later workers are not started after a sufficient visible shard row, and the local UNWIND
  regression would fail if the second row were retained.
- The current worker/storage scan API returns an owned batch vector, so the selected first worker
  can still materialize its local scan before the coordinator observes the first row. Remaining
  workers and safe local row generation are genuinely stopped; making the first shard scan itself
  cursor-streaming would require a broader storage/worker streaming change outside this slice.
- One diagnostic `cargo check -p query-executor` was accidentally run with default features before
  the required RocksDB compiler variables and failed in `librocksdb-sys` on missing C++ headers.
  All recorded verification commands above used the required `CXX`/`LIBCLANG_PATH` environment (or
  disabled the optional default runtime feature for executor-only tests).

## Slice 2 rework: independent audit fixes

### Additional RED/GREEN evidence

- `primary_replica_graph_parent_apply_gathers_before_child_invocation` first failed with
  `UnsupportedOperator("distributed Apply child plan")`: PrimaryReplica had kept the graph parent
  and Apply in a shard fragment. The optimizer now creates a partition-local parent prefix,
  gathers it, and executes Apply at the coordinator; the test passes with four parent/child rows.
- `shared_nothing_missing_expected_shard_never_returns_partial_apply_success` initially could not
  compile because no stable missing-shard error existed. After adding the test contract and
  implementation, a registered-only shard 0 with a plan expecting `[0, 1]` returns exactly
  `DistributedQueryError::MissingShards(vec![1])`, even when EXISTS could otherwise see shard 0.
- `keyed_inner_join_is_not_first_row_short_circuit_safe` first failed because the classifier treated
  all inner joins as safe. It now requires `keys.is_empty()`; the keyed plan test passes.
- `interval_exists_and_count_split_parent_region_at_child_visibility_cells` was added against the
  missing cell helper, then passes after valid/transaction boundary decomposition. For parent
  `[0,10)`, children `[2,8)` and `[5,9)`, EXISTS is F/T/T/T/F and COUNT is 0/1/2/1/0, with
  active child provenance merged per cell. Interval EXISTS now always requests all child rows;
  first-row short-circuit cannot decide temporal cells.
- `rejects_apply_child_without_one_exact_argument_source` and
  `rejects_apply_tree_whose_cumulative_nodes_exceed_the_global_bound` first failed to compile due
  missing validation categories. They now pass with exact malformed-plan rejection, including a
  global 4,096-node recursive tree bound rather than a per-level reset.
- `worker_never_widens_an_earlier_parent_deadline` first returned an empty batch despite an expired
  parent deadline because the request deadline replaced it. Worker and coordinator deadline
  installation now take the earlier deadline; the test passes with `DeadlineExceeded`.
- `child_errors_preserve_stable_typed_categories` passes after structural mappings for snapshot,
  security, cancellation, memory, invocation, output-row, missing-shard, and recursive-plan
  categories. Worker temporal errors no longer stringify cancellation/deadline/budget categories.
- `apply_rejects_the_first_exceeding_row_before_retention` was deliberately run once with the old
  push-before-check behavior and failed (`ApplyOutputRowLimit` on the first call), then passed after
  restoring pre-retention row/byte checks. It verifies the retained vector and byte counter remain
  unchanged when the next row exceeds either bound.

### Rework implementation notes

- Physical plan headers and fragment requests now carry explicit expected shard identities. Optimizer
  context defaults to deterministic IDs for tests, while `cypher-engine` injects its configured real
  shard IDs. AllShards checks the full expected set before invoking any worker, so EXISTS cannot
  convert a missing shard into success.
- `PhysicalOperator::Argument` source validation now requires exactly one incoming-free coordinator
  Argument with exact schema in every child plan. Recursive physical validation accumulates all
  fragment/operator nodes across nested child trees under one bound.
- `ExecutionContext` deadline is publicly readable for execution-boundary inheritance; worker and
  coordinator retain a stricter parent deadline when request/deadline metadata is later.
- Point and interval Apply retention checks happen per output row before ownership enters the output
  vector. Interval scalar Apply uses temporal cells and no longer applies a single Boolean/count to
  the whole parent region.
- Coordinator point joins use the same pre-retention discipline: the first row whose incremental
  byte estimate exceeds the fragment budget returns the typed memory error while the owned row
  vector and byte counter remain unchanged. A direct helper regression verifies both invariants.
- Nested point and interval child invocations share one concurrency-safe Apply budget ledger. The
  ledger latches the strictest invocation/output/depth limit observed in the recursive chain, and
  depth is released by an RAII guard. The integration regression exercises a child Apply nested
  below another child Apply in both point and interval execution and receives the stable recursive
  plan violation instead of escaping under the inner plan's more permissive limits.
- Coordinator worker awaits are fenced with cancellation and deadline futures under Tokio. The
  synchronous compatibility path avoids constructing Tokio timer futures when driven by
  `futures::executor::block_on`, while still checking cancellation/deadline before and after the
  immediate worker future; this preserves the existing coordinator API tests.

### Rework verification

- `CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p distributed-query`:
  28 tests passed, 0 failed; doc tests passed. This includes nested point/interval Apply ledger,
  hanging-worker cancellation, typed child errors, keyed-join short-circuit classification,
  explicit missing-shard rejection, and inherited-deadline tests.
- `CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p query-executor --no-default-features`:
  55 tests passed, 0 failed; doc tests passed, including interval cells, pre-retention checks, and
  the shared ledger unit regression.
- `CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p physical-plan -p query-optimizer`:
  20 tests passed, 0 failed; doc tests passed.
- `CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test -p cypher-engine --no-default-features --lib`:
  compiled and passed its library test target (0 tests, 0 failures).
- `cargo fmt --all -- --check`: exit 0.
- `git diff --check`: exit 0.

### Remaining concern after rework

The first worker/storage scan still returns an owned local batch before the coordinator observes its
first row; the missing-shard contract and later-worker cancellation are strict, and local UNWIND
generation is strict, but cursor-level first-row storage streaming remains outside this slice.

## Slice 3: ordinary write subqueries through the statement candidate

Status: complete for ordinary child writes whose correlated outer-row inputs are produced by the
parent prefix. `IN TRANSACTIONS` remains deliberately fail-closed for Slice 4.

### RED evidence

- `cargo test -p cypher-compiler --test write_plan ordinary_write_subquery_keeps_nested_mutations_and_parent_read_prefix -- --exact --nocapture`
  failed to compile with `no variant or associated item named Subquery found for enum
  CompiledMutation`. This proved the regression required a structured nested mutation program
  rather than accepting the previous top-level-only mutation list.
- `cargo test -p cypher-engine --test write_materializer ordinary_write_subquery_materializes_imported_scalar_for_each_outer_row -- --exact --nocapture`
  then failed to compile at the production exhaustive match because
  `CompiledMutation::Subquery(_)` was not handled. No child mutation could be silently ignored.
- The first Gateway test attempt did not reach Rust because the default RocksDB C++ build could not
  find `cstdint`. It was rerun with the repository's required LLVM `CXX`/`LIBCLANG_PATH` settings;
  this environment failure is not counted as feature RED.

### Implementation

- `MutationPlan` now retains `CompiledMutation::Subquery` nodes in statement order. Each node owns
  its clause identity, explicit imports, explicit exports, and recursively nested mutation plan;
  child writes are not flattened into the parent mutation list.
- The compiler permits semantically validated ordinary write children while retaining the complete
  isolated child logical plan under `LogicalOperator::Apply`. Named procedures in a write child
  remain fail-closed, and `IN TRANSACTIONS` remains fail-closed.
- `read_prefix_plan` recursively detects a Write in an Apply child and returns the Apply parent's
  input. The gateway therefore executes only the parent row-producing prefix; query-executor still
  returns `UnsupportedOperator("Write")` for every `PhysicalOperator::Write`.
- Write materialization accepts scalar correlated bindings, resolves identifier property values
  from the explicit import scope, derives a deterministic child seed from the outer-row seed and
  child clause identity, and recursively materializes the child mutation program. Only declared
  exports re-enter the parent binding scope; child-local variables do not leak. Child overlay
  elements, scoped temporal transactions, and MERGE constraints join the owning row candidate.
- Structured child exports retain their source expressions as well as their parent-visible names.
  A graph binding exported through `RETURN n AS created` is injected under `created`, so a later
  parent mutation can consume it without flattening or guessing the child projection.
- Gateway write-binding and MERGE discovery now recurse through structured child mutation plans.
  Existing preparation still allocates one transaction context for the complete statement, builds
  every outer-row write in a local candidate vector, validates the full candidate, then publishes
  it through the revision-fenced overlay path. Any row error returns before auto-commit or explicit
  transaction publication.
- The existing cross-shard Gateway integration now covers an autocommit statement whose second
  child row fails: the first row remains absent durably. A successful two-row retry becomes visible
  as exactly two elements. It also covers explicit-transaction write subqueries, read-your-writes
  through the existing overlay at the BEGIN snapshot, exclusion of a later external commit, and
  disappearance after ROLLBACK.
- Two stale compiler tests that searched the parent node vector for read-child UNWIND/UNION were
  migrated to inspect `Apply.child_plan()`, matching the Slice 2 isolated-plan contract.

### GREEN evidence

- Compiler write-subquery regression: 1 passed, 0 failed.
- Write materializer regressions: 2 passed, 0 failed; full write materializer target: 13 passed,
  0 failed.
- Required Gateway named tests:
  `call_subquery_write_stages_all_outer_rows_and_failed_child_leaves_overlay_unchanged` and
  `explicit_transaction_subquery_read_and_write_use_the_begin_snapshot`: 2 passed, 0 failed.
- Cross-shard Gateway service integration, including durable failure/success and explicit
  transaction overlay/rollback checks: 1 passed, 0 failed.
- `cargo test -p cypher-compiler`: 26 passed, 0 failed; doc tests passed.
- `cargo test -p query-executor --no-default-features`: 57 tests passed, 0 failed; doc tests passed.
- `cargo fmt --all -- --check`: exit 0.
- `git diff --check`: exit 0.

### Self-review and concerns

- Reviewed the path from recursive compiler mutation ownership through outer-prefix row extraction,
  per-row materialization, candidate validation, revision-fenced explicit-transaction publication,
  COMMIT, and ROLLBACK. No query-executor or optimizer persistence path was added.
- Reviewed failure timing with a real Gateway request: row one materializes locally, row two fails
  on an unsupported Map property, and a subsequent fenced read sees zero durable elements.
- The existing `cypher-engine` full suite has one unrelated read-only Slice 2 failure:
  `union_chain_inside_read_subquery_executes_with_boundary_semantics` reaches optimizer physical
  validation as `InvalidApplyArgumentSource`. The other 28 end-to-end engine tests pass. This slice
  did not change optimizer/physical Apply partitioning and did not broaden into that audit.
- This slice executes the parent read prefix and supports direct/nested child mutation programs and
  exported graph bindings. A child-local row-producing read pipeline before its Write (for example,
  child-local MATCH/UNWIND feeding CREATE) still needs a gateway-owned child-prefix invocation that
  supplies its rows to the mutation program. It is not silently executed or sent to query-executor;
  such shapes currently fail during materialization rather than risking partial publication.

## Slice 2 final audit closure

### Final RED/GREEN evidence

- `bounded_temporal_hash_join_rejects_the_first_row_beyond_memory_before_retention` first failed to
  compile because no bounded temporal join API existed. Inner and left temporal joins now estimate
  and check every candidate row before pushing it; the focused regression passes.
- `local_nested_apply_consumes_the_outer_recursive_budget` first returned `Boolean(true)` instead
  of `RecursivePlanViolation`. Invocation, depth, and output charging now happen at the common point
  and interval Apply boundary, so local and distributed children share the same ledger without
  invoker-only bypass or double charging; the focused regression passes.
- `child_invocation_cancellation_stops_a_hanging_worker_without_tokio_runtime` remained pending
  until manually terminated. The non-Tokio await path now polls worker and cancellation together;
  `child_invocation_deadline_stops_a_hanging_worker_without_tokio_runtime` also proves a deadline
  wake interrupts a permanently pending worker. Both pass.
- Transport payload and credit failures initially lacked stable runtime variants. They now map to
  `ChildTransportBudgetExceeded`, while sequence/exchange violations use
  `ChildTransportProtocolViolation`; the typed mapping regression passes.

### Final implementation notes

- Temporal hash/left joins use bounded candidate retention before coalescing. Coordinator interval
  joins pass the fragment memory budget into these APIs instead of validating only after full
  materialization.
- Apply ledger charging moved out of `DistributedChildInvoker` and into common point/interval Apply.
  Every output row is charged before owned retention, with scalar COUNT/EXISTS charging the actual
  Apply result row rather than every consumed child row.
- The non-Tokio compatibility path uses `std::future::poll_fn` for worker/cancellation racing and a
  deadline waker thread created only after the worker first returns Pending. The Tokio path retains
  `tokio::select!`.

### Final verification

- `cargo test -p distributed-query`: 30 tests passed, 0 failed; doc tests passed.
- `cargo test -p query-executor --no-default-features`: 57 tests passed, 0 failed; doc tests passed.
- `cargo test -p physical-plan -p query-optimizer`: 20 tests passed, 0 failed; doc tests passed.
- `cargo test -p cypher-engine --no-default-features --lib`: compiled and passed its library target.
- `cargo fmt --all -- --check`: exit 0.
- `git diff --check`: exit 0.
