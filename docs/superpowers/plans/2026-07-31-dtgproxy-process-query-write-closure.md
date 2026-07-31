# DTGProxy Process Query and Write Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the real four-process Bolt path execute the fixed point lookup, global count, and durable create workloads correctly before measuring Fjall, PostgreSQL, and Neo4j.

**Architecture:** Data remains a fenced single-storage-access worker. The protocol transport preserves raw batches by fragment ID, while Gateway binds parameters and uses a new materialized-source entry point in the shared `dtg-query` runtime to execute the physical operator DAG. Writes use the current transaction and Shard RPC contracts. The existing diagnostic plan resumes only after these production semantics pass live Fjall certification.

**Tech Stack:** Rust 1.93, Tokio 1.53, Tonic 0.14, Bolt 5.4, DTG physical plans, `dtg-query`, DTG transaction/Shard RPCs, Fjall, PostgreSQL 17, Neo4j 5.26 Community.

## Global Constraints

- The approved design is `docs/superpowers/specs/2026-07-31-dtgproxy-process-query-write-closure-design.md`.
- The fixed workloads and parameters are unchanged.
- The measured path is persistent Bolt client -> Gateway -> Data Node -> official provider.
- Point lookup uses a truthful logical scan bound of exactly 4,096 and returns exactly scalar integer 2,048.
- Global `COUNT(*)` returns exactly one scalar integer, including zero on empty input and 4,096 after diagnostic preload.
- Data executes one fully validated storage access; Gateway owns parameter binding and the global operator DAG.
- CREATE acknowledgement is returned only after a durable production transaction outcome.
- No benchmark-only branch may enter production language, query, transaction, storage, or process behavior.
- Unsupported or incompletely identified fragment shapes fail closed.
- Every implementation task follows red-green-refactor, has an independent task review, and commits only its scoped files.

---

### Task 1: Parse and execute global `COUNT(*)`

**Files:**
- Modify: `crates/language/dtg-language/src/ast.rs`
- Modify: `crates/language/dtg-language/src/parser.rs`
- Modify: `crates/language/dtg-language/src/normalize.rs`
- Test: `crates/language/dtg-language/tests/compile.rs`
- Modify: `crates/execution/dtg-query/src/operator.rs`
- Modify: `crates/execution/dtg-query/src/runtime.rs`
- Test: `crates/execution/dtg-query/tests/runtime.rs`

**Interfaces:**
- Produces AST expression `Expr::CountStar`.
- Produces a `LogicalNodeKind::Aggregate` with zero groups and one non-distinct `AggregateKind::Count` whose argument is `None`.
- Produces `AggregateOperator::count_all(input, count_name)`.

- [ ] **Step 1: Write the failing language test**

Compile the exact statement and assert its root is a zero-group aggregate:

```rust
#[test]
fn compiles_global_count_star() {
    let program = Language::default()
        .compile("MATCH (n) RETURN COUNT(*)")
        .unwrap();
    let LogicalStatement::Query(plan) = &program.statement else { panic!() };
    let LogicalNodeKind::Aggregate(aggregate) = &plan.node(plan.root).unwrap().kind else { panic!() };
    assert!(aggregate.groups.is_empty());
    assert_eq!(aggregate.aggregates.len(), 1);
    assert_eq!(aggregate.aggregates[0].function, AggregateKind::Count);
    assert_eq!(aggregate.aggregates[0].argument, None);
    assert!(!aggregate.aggregates[0].distinct);
    assert_eq!(program.result_schema.fields[0].name, "COUNT(*)");
}
```

- [ ] **Step 2: Verify the language test fails for the parse error**

Run: `cargo test --locked -p dtg-language compiles_global_count_star -- --exact --nocapture`

Expected: FAIL because `COUNT(*)` leaves trailing input.

- [ ] **Step 3: Add the minimal AST, parser, and normalization support**

Recognize only the exact case-insensitive token sequence `COUNT ( * )` as `Expr::CountStar`. Reject
`COUNT(expr)`, `DISTINCT`, and other functions through the existing parse/semantic error path. When
the RETURN list contains `CountStar`, require it to be the only RETURN item and build:

```rust
LogicalNodeKind::Aggregate(Aggregate {
    input: plan.root,
    groups: Vec::new(),
    aggregates: vec![AggregateFunction {
        function: AggregateKind::Count,
        argument: None,
        alias: "COUNT(*)".into(),
        distinct: false,
    }],
})
```

- [ ] **Step 4: Write failing grouped/global aggregate runtime tests**

Add one test that feeds three rows to a zero-group count and expects `[[Integer(3)]]`, and one test
that feeds an empty batch and expects `[[Integer(0)]]`.

- [ ] **Step 5: Verify the runtime tests fail because zero groups are unsupported**

Run: `cargo test --locked -p dtg-query global_count -- --nocapture`

- [ ] **Step 6: Implement `AggregateOperator::count_all` and zero-group runtime lowering**

The operator drains all input, checked-adds the row count, and emits exactly once with schema:

```rust
RowSchema { fields: vec![Field {
    name: count_name.into(),
    data_type: LogicalType::Integer,
    nullable: false,
}] }
```

Keep the existing one-group `count_by` path unchanged. In `QueryRuntime::build_operator`, route zero
groups plus one supported count-star aggregate to `count_all`; route one group to `count_by`; reject
all other shapes.

- [ ] **Step 7: Verify green and commit**

```bash
cargo test --locked -p dtg-language compiles_global_count_star -- --exact --nocapture
cargo test --locked -p dtg-query global_count -- --nocapture
cargo test --locked -p dtg-language
cargo test --locked -p dtg-query
git diff --check
git add crates/language/dtg-language crates/execution/dtg-query
git commit -m "feat(query): support global count star"
```

---

### Task 2: Add parameter binding and materialized fragment sources

**Files:**
- Modify: `crates/execution/dtg-query/src/runtime.rs`
- Modify: `crates/execution/dtg-query/src/expression.rs`
- Modify: `crates/execution/dtg-query/src/lib.rs`
- Test: `crates/execution/dtg-query/tests/runtime.rs`
- Modify: `crates/execution/dtg-execution/src/gateway.rs`
- Test: `crates/execution/dtg-execution/tests/gateway_process.rs`

**Interfaces:**
- Produces `QueryRuntime::execute_materialized(plan, fragment_batches, budget, cancellation)`.
- Produces recursive `bind_logical_expr(expression, parameters)` in `dtg-execution` before `Expression` construction.
- Materialized inputs use `BTreeMap<u32, Vec<ColumnBatch>>`, keyed by executable fragment ID.

- [ ] **Step 1: Write a failing materialized point-filter test**

Construct an executable `Source -> Filter(n.id = 2048) -> Project(n.id)` plan and supply two
materialized batches containing map-valued vertices with IDs 1 through 4,096. Assert final schema
`n.id` and the sole row `Integer(2048)`.

- [ ] **Step 2: Verify red**

Run: `cargo test --locked -p dtg-query materialized_source_filters_and_projects -- --exact --nocapture`

Expected: compile failure because `execute_materialized` does not exist.

- [ ] **Step 3: Generalize the operator DAG builder**

Extract the current recursive builder so its source branch accepts either local storage or
materialized fragment batches. For each materialized fragment, validate presence, nonempty source
name, identical one-column input schemas, and project the storage field to the physical source
output name. Merge multiple fragments using the same deterministic spill merge used by local
sources. Reject missing and extra fragment IDs.

The public method is:

```rust
pub async fn execute_materialized(
    &self,
    plan: &ExecutablePlan,
    fragment_batches: BTreeMap<u32, Vec<ColumnBatch>>,
    budget: QueryBudget,
    cancellation: CancellationToken,
) -> Result<QueryStream, QueryError>
```

- [ ] **Step 4: Write failing parameter-binding tests**

Test recursive binding in binary, list, map, unary, projection, aggregate argument, sort, and unwind
expressions. The fixed point predicate must become `LogicalExpr::Literal(Value::Integer(2048))`.
Missing names and unsupported Gateway values must return typed `GatewayExecutionError`s before
remote success can be returned.

- [ ] **Step 5: Verify red and implement minimal recursive binding**

Run: `cargo test --locked -p dtg-execution --test gateway_process binds_ -- --nocapture`

Convert `GatewayValue::{Null,Boolean,Integer,FloatBits,Bytes,String,List,Map}` recursively to storage
`Value`; reject graph values if present. Apply binding inside `lower_expression` through a new
parameter-aware lowering path. Preserve the existing parameter-free `lower_plan` API for composed
callers.

- [ ] **Step 6: Verify green and commit**

```bash
cargo test --locked -p dtg-query materialized_ -- --nocapture
cargo test --locked -p dtg-query
cargo test --locked -p dtg-execution --test gateway_process binds_ -- --nocapture
cargo test --locked -p dtg-execution --test gateway_process
git diff --check
git add crates/execution/dtg-query crates/execution/dtg-execution/src/gateway.rs \
  crates/execution/dtg-execution/tests/gateway_process.rs
git commit -m "feat(execution): run plans over remote fragment batches"
```

---

### Task 3: Execute remote query operators at Gateway and harden Data fragments

**Files:**
- Modify: `crates/execution/dtg-execution/src/gateway.rs`
- Modify: `crates/execution/dtg-execution/src/data.rs`
- Test: `crates/execution/dtg-execution/tests/gateway_process.rs`
- Modify: `crates/processes/dtg-data/src/service.rs`
- Test: `crates/processes/dtg-data/tests/process.rs`

**Interfaces:**
- Protocol decoding preserves `fragment_id`, ordered batches, raw fields, and rows until query execution completes.
- `GatewayProtocolV2Transport` runs materialized execution for planned queries and keeps control/write decoding unchanged.
- Data accepts exactly one storage access per fragment and validates the complete envelope.

- [ ] **Step 1: Write failing protocol tests for the exact point and count queries**

Make the recording protocol client return raw Data batches with field `value` and fragment IDs.
Execute the exact statements through process `GatewayExecution` and assert:

```rust
assert_eq!(point.fields(), &["n.id"]);
assert_eq!(point.rows(), &[vec![GatewayValue::Integer(2048)]]);
assert_eq!(count.fields(), &["COUNT(*)"]);
assert_eq!(count.rows(), &[vec![GatewayValue::Integer(4096)]]);
```

The count fixture must split rows across two fragments to prove the aggregate is global.

- [ ] **Step 2: Verify red**

Run: `cargo test --locked -p dtg-execution --test gateway_process process_executes_remote_ -- --nocapture`

Expected: point leaks `value` rows and count is not aggregated.

- [ ] **Step 3: Implement raw response preservation and Gateway execution**

Decode each `ColumnBatch` with its 16-byte fragment ID into a checked `u32`, convert Gateway values
to Query values, and group batches by fragment. For query requests, bind and lower the retained
physical plan, call `QueryRuntime::execute_materialized`, drain the stream under a bounded query
budget/cancellation token, convert final rows to Gateway values, and require the final schema names
to equal `request.result_fields`. Reject unknown, duplicate, missing, or malformed fragments.

- [ ] **Step 4: Write failing Data fail-closed tests**

Mutate a valid fragment to contain two storage accesses, a truncated operator section, and trailing
bytes. Each request must fail with `failed_precondition`; none may return plausible raw rows.

- [ ] **Step 5: Verify red and harden the fragment decoder**

Run: `cargo test --locked -p dtg-data --test process fragment_rejects_ -- --nocapture`

Decode the complete current envelope, require one storage access, validate operator count and every
encoded operator structurally, and require cursor exhaustion. Operators remain Gateway-owned; Data
must not evaluate them.

- [ ] **Step 6: Verify green and commit**

```bash
cargo test --locked -p dtg-execution --test gateway_process
cargo test --locked -p dtg-data --test process -- --test-threads=1
cargo test --locked -p dtg-execution
cargo test --locked -p dtg-data
git diff --check
git add crates/execution/dtg-execution crates/processes/dtg-data
git commit -m "feat(gateway): execute remote physical query plans"
```

---

### Task 4: Implement real process-mode auto-commit CREATE

**Files:**
- Modify: `crates/execution/dtg-execution/src/gateway.rs`
- Test: `crates/execution/dtg-execution/tests/gateway_process.rs`
- Test: `crates/processes/dtg-data/tests/process.rs`
- Test: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`

**Interfaces:**
- Extends `GatewayProtocolV2Client` with `apply_transaction(proto::TransactionRequest)` and implements it with the existing `DataServiceClient::apply_transaction` RPC on the same Data endpoint.
- Consumes normalized `LogicalWrite`, the first active `CatalogShard`, `ShardCommand::CommitSingleShard`, and the existing Data transaction RPC.
- Produces durable empty-row acknowledgement for the exact fixed CREATE statement.

- [ ] **Step 1: Record the existing write-path evidence in the task brief**

The implementer must read `.superpowers/sdd/write-path-investigation.md` first. The existing
production surface is `DataService::ApplyTransaction(TransactionRequest)`, which decodes a current
`ShardCommand`, checks placement/backend fences, proposes it through Shard Raft, and returns status
only after `apply_transaction_command` succeeds. Use that surface; do not add a provider call.

- [ ] **Step 2: Write a failing process CREATE test**

Execute `CREATE (n:Bench {value: 1}) VALID FROM 1` through process `GatewayExecution` with a
protocol-faithful client. Assert the current trait has no transaction submission and the statement
cannot receive an acknowledgement. Retain that result as the required RED evidence.

- [ ] **Step 3: Implement the minimal production transaction path**

Support the general input-free single-vertex CREATE subset and fail closed for other normalized
writes. Use the nonzero request ID as the transaction ID, command ID, idempotency key, and new
`VertexId`; this keeps retry identity stable for one request and avoids a process-local allocator.
Resolve the first active catalog Shard and its placement/backend fences, convert literal properties
recursively to storage values, require literal valid time, create a version-1 `VertexVersion` with
the planning snapshot transaction time and open-ended valid interval, encode a current
`ShardCommand::CommitSingleShard`, and submit:

```rust
proto::TransactionRequest {
    context: Some(shard_context_from_catalog(request.context(), shard)),
    transaction_id: request.context().request_id().to_be_bytes().to_vec(),
    operation: proto::TransactionOperation::Commit.into(),
    idempotency_key: request.context().request_id().to_be_bytes().to_vec(),
    payload: Some(bounded_current_shard_command(command)?),
}
```

Validate the typed success status and acknowledge only after the Data RPC returns. Treat every
other status or transport ambiguity as an error. Labels remain accepted language metadata because
the current vertex storage contract has no label field; no hidden benchmark property is added.

- [ ] **Step 4: Add live Bolt write verification**

Strengthen the ignored Fjall live test to send one CREATE over Bolt, require zero returned rows, then
query the new total through the corrected count path and observe exactly one additional vertex.

- [ ] **Step 5: Verify green and commit**

```bash
cargo test --locked -p dtg-execution --test gateway_process process_create_ -- --nocapture
cargo test --locked -p dtg-data --test process -- --test-threads=1
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
git diff --check
git add crates/execution/dtg-execution/src/gateway.rs \
  crates/execution/dtg-execution/tests/gateway_process.rs \
  crates/processes/dtg-data/tests/process.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs
git commit -m "feat(gateway): dispatch process writes transactionally"
```

---

### Task 5: Restore truthful diagnostic lifecycle and run the matrix

**Files:**
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/bolt.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/cluster.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`
- Then execute Tasks 4 and 5 from `docs/superpowers/plans/2026-07-31-dtgproxy-three-backend-e2e-diagnostic.md`.

**Interfaces:**
- Produces exact per-operation correctness gates and collision-safe four-process startup.
- Resumes immutable artifact publication and isolated container measurement only after Fjall live certification.

- [ ] **Step 1: Write failing diagnostic correctness tests**

Point lookup must reject any response other than field `n.id` and scalar integer 2,048. Count must
reject any response other than field `COUNT(*)` and scalar integer 4,096. CREATE must require a
successful empty-row summary. Stable but incorrect digests must fail.

- [ ] **Step 2: Restore the truthful scan bound and verify the old workaround fails**

Set `DTG_GATEWAY_LOGICAL_SCAN_BOUND=4096` for point and count. Run the ignored Fjall test before the
production fixes are applied and retain the RED evidence showing incorrect semantics.

- [ ] **Step 3: Reserve unique ports through startup**

Replace dropped `free_address()` probes with an owned reservation set. Keep every listener bound
until immediately before its corresponding child spawn, assert all addresses differ, and add a test
that requests all cell addresses repeatedly without duplicates.

- [ ] **Step 4: Run live Fjall certification**

```bash
cargo build --release --locked -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway
DTG_BACKEND_E2E_BIN_DIR="$PWD/target/release" \
  cargo test --release --locked -p dtg-gateway --test backend_e2e_diagnostic \
  fjall_cell_uses_real_four_process_bolt_path -- --ignored --exact --nocapture
```

Require exact point/count values, durable CREATE, and retirement of all four recorded PIDs.

- [ ] **Step 5: Independently re-review Task 3 and mark it complete only if clean**

Review the complete range `89ec2fb..HEAD` against the original Task 3 brief plus the closure design.
Resolve every Critical and Important finding before appending the Task 3 clean line to the progress
ledger.

- [ ] **Step 6: Complete artifact/container tasks and the full measurement**

Execute Tasks 4 and 5 from the original diagnostic plan. Run one backend at a time, remove its exact
external service before starting the next, produce 54 raw observations and 18 summary groups, verify
SHA-256 files from raw observations, and confirm all owned children/containers are absent.

- [ ] **Step 7: Commit lifecycle fixes and measured report**

Use the original plan's scoped commits:

```text
fix(perf): enforce diagnostic query identities
test(perf): publish immutable backend diagnostics
perf: measure three backend end-to-end paths
```
