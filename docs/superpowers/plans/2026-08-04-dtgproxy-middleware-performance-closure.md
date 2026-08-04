# DTGProxy Middleware Performance Closure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make read/write middleware performance measurable and safe, prove static multi-Data routing for every official backend, and remove fail-open provider assignment startup.

**Architecture:** Preserve the existing bounded Bolt and Gateway–Data pipelines. Extend diagnostics with stage evidence, construct one independent three-shard topology for each provider, and choose read optimizations only after comparing measured stages. Preserve single-statement `COMMITTED` semantics; bulk snapshot ingestion is measured only after receipt completion.

**Tech Stack:** Rust, Tokio, tonic/prost, RequestStageMetrics, Bolt 5.4 harness, Raft apply batcher, Fjall/PostgreSQL/Kuzu adapters.

## Global Constraints

- One cluster and all active Data replicas in it use one provider kind only.
- Never bypass Gateway/Data, immutable read fences, Raft apply, or provider atomic apply.
- All queues, credits, result buffers, batching and cancellation registries remain bounded.
- `PENDING` ingestion admission is never reported as a durable write.
- Formal release artifacts use exactly three repetitions and are generated under `target/`, never committed.
- Migration, rebalance, split/merge, follower read and cross-host HA remain out of scope.

---

### Task 1: Fail closed on programmatic provider mismatch

**Files:**

- Modify: `crates/processes/dtg-data/src/service.rs:330-382`
- Modify: `crates/processes/dtg-data/tests/process.rs`

**Interfaces:** `DataNodeBuilder::start()` returns `Err(DataNodeError::Build(..))` before creating business/consensus roots when any configured assignment has another `ProviderKind`.

- [ ] **Step 1: Write the failing test**

Replace the mismatch test with:

```rust
let result = DataNodeBuilder::from_config(config).start().await;
assert!(matches!(result, Err(DataNodeError::Build(message))
    if message.contains("does not match configured backend")));
assert!(!business_root.exists());
assert!(!consensus_root.exists());
```

- [ ] **Step 2: Run red**

Run: `cargo test --locked -p dtg-data --test process production_config_rejects_mismatched_assignment_before_opening_namespace`

Expected: FAIL because the builder currently records `ReplicaFailure` and returns Ready.

- [ ] **Step 3: Preflight all assignments**

At the beginning of `DataNodeBuilder::start`, before `create_dir_all`, return the existing build error when an assignment differs from `configured_provider_kind`:

```rust
if let Some(configured) = self.configured_provider_kind.as_ref() {
    if let Some(binding) = self.assignments.iter().find(|binding| binding.provider_kind() != configured) {
        return Err(DataNodeError::Build(format!(
            "assignment provider {:?} does not match configured backend {:?}",
            binding.provider_kind(), configured
        )));
    }
}
```

Then retain the existing matching-provider assignment loop.

- [ ] **Step 4: Verify and commit**

Run `cargo test --locked -p dtg-data --test process`, then commit:

```bash
git add crates/processes/dtg-data/src/service.rs crates/processes/dtg-data/tests/process.rs
git commit -m "fix(data): fail closed on provider mismatch"
```

### Task 2: Certify three Data shards for every provider

**Files:**

- Modify: `crates/processes/dtg-gateway/tests/four_process_cluster.rs:192-620`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/cluster.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`

**Interfaces:** `static_shard_case(backend, runtime)` constructs three same-provider endpoints with independent namespaces and returns endpoint call deltas and request-fence evidence.

- [ ] **Step 1: Write failing Kuzu and PostgreSQL topology tests**

Each provider-specific test must assert:

```rust
assert_eq!(point_call_delta, [0, 0, 1]);
assert_eq!(adjacency_call_delta, [0, 0, 1]);
assert_eq!(count_call_delta, [1, 1, 1]);
assert!(fences.iter().all(|fence| fence.snapshot_immutable));
```

PostgreSQL is ignored unless endpoint/credential test environment is present.

- [ ] **Step 2: Run Kuzu red**

Run: `cargo test --locked -p dtg-gateway --test four_process_cluster static_shard_snapshot_routes_kuzu`

Expected: FAIL because the current helper hard-codes `ProviderKind::Fjall`.

- [ ] **Step 3: Implement provider-specific topology construction**

Replace the Fjall-only binding helper with a `Backend` argument. Use `DataProcessConfig` construction so only PostgreSQL receives endpoint/credential profile; Fjall/Kuzu use their own temporary storage roots. Do not share storage or consensus namespaces.

- [ ] **Step 4: Verify and commit**

Run Fjall and Kuzu focused tests. Start temporary loopback PostgreSQL with the established `initdb`/`pg_ctl` trap, run the ignored PostgreSQL case, then commit:

```bash
git add crates/processes/dtg-gateway/tests/four_process_cluster.rs crates/processes/dtg-gateway/tests/backend_e2e_support/cluster.rs crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs
git commit -m "test(gateway): certify static routing for each backend"
```

### Task 3: Make stage evidence complete and artifact-enforced

**Files:**

- Modify: `crates/execution/dtg-execution/src/request_metrics.rs`
- Modify: `crates/execution/dtg-execution/src/gateway.rs`
- Modify: `crates/processes/dtg-data/src/service.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs`
- Modify: `crates/execution/dtg-execution/tests/gateway_process.rs`
- Modify: `crates/processes/dtg-data/tests/process.rs`

**Interfaces:** The artifact contains a workload-specific required subset of stable stages: Gateway planning/routing, transport/pipeline wait, Data validation/execution, Raft queue/apply, and provider apply.

- [ ] **Step 1: Write failing artifact and metric tests**

```rust
assert!(QuickDiagnosticArtifact::new(revision, observations_without_provider_apply)
    .unwrap_err()
    .to_string()
    .contains("provider_apply"));
```

A deterministic point read and committed write must each observe all applicable stages exactly once.

- [ ] **Step 2: Run red**

Run:

```bash
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic quick_artifact_requires_required_stage_set
cargo test --locked -p dtg-execution --test gateway_process middleware_stage_
cargo test --locked -p dtg-data --test process middleware_stage_
```

- [ ] **Step 3: Add boundary timers only**

Add stable `RequestDetail` names at existing call sites. Timers finish exactly once on success, typed failure or cancellation; never record queries, values or credentials. Make `QuickDiagnosticArtifact::new` reject required-stage omissions and serialize `transport_mode` plus stage means.

- [ ] **Step 4: Verify and commit**

Run the three tests above without filters, then:

```bash
git add crates/execution/dtg-execution/src/request_metrics.rs crates/execution/dtg-execution/src/gateway.rs crates/processes/dtg-data/src/service.rs crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs crates/execution/dtg-execution/tests/gateway_process.rs crates/processes/dtg-data/tests/process.rs
git commit -m "feat(metrics): expose middleware stage evidence"
```

### Task 4: Optimize the measured read stage without another protocol

**Files:**

- Modify: `crates/execution/dtg-execution/src/gateway.rs:852-1435`
- Modify: `crates/processes/dtg-data/src/service.rs:2161-2564`
- Modify: `crates/execution/dtg-execution/tests/gateway_process.rs`
- Modify: `crates/processes/dtg-data/tests/process.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`

**Interfaces:** Existing `TonicGatewayPipelinePool`, request IDs, credits, cancellation tombstones, response-batch frames and session/unary fallback remain authoritative.

- [ ] **Step 1: Write a failing contention/fence test**

```rust
assert_eq!(fast_result.await?, expected_rows);
assert!(slow_request.is_cancelled());
assert_eq!(transport.sent_fragments(), 2, "new fence must not reuse old encoding");
```

- [ ] **Step 2: Run red**

Run: `cargo test --locked -p dtg-execution --test gateway_process pipeline_contention_and_fence_cache`

- [ ] **Step 3: Apply exactly one measured optimization**

Use Task 3 evidence to choose one: a bounded fence-keyed encoded-fragment cache if Gateway/codec dominates; least-loaded existing stream selection if pipeline wait dominates; compatible response-batch frames if Data encoding dominates. Never wait to fill a batch, exceed existing `MAX_GATEWAY_PIPELINE_*` bounds, or remove cancellation/session fallback.

- [ ] **Step 4: Verify and commit**

Run pipeline tests and one Fjall selected diagnostic. Verify unchanged result digest and a reduced chosen stage; commit with `perf(gateway): reduce measured read pipeline overhead`.

### Task 5: Measure batch writes only at COMMITTED

**Files:**

- Modify: `crates/processes/dtg-data/tests/process.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs`
- Modify: `README.md`

**Interfaces:** Existing `AcceptSnapshotIngest`, `GetSnapshotIngestReceipt` and per-shard apply batcher are reused; no public RPC semantics change.

- [ ] **Step 1: Write a failing completion test**

```rust
assert!(receipts.iter().all(|receipt| receipt.state() == SnapshotIngestState::Committed));
assert_eq!(current_vertex_count(&node).await?, batch_len as u64);
```

- [ ] **Step 2: Run red**

Run: `cargo test --locked -p dtg-data --test process snapshot_ingest_committed_batch_is_durable`

- [ ] **Step 3: Add a diagnostic-only completion driver**

Submit bounded existing ingest batches, poll/retry with the same receipt IDs to terminal state, count only committed items and audit actual final vertex count. Preserve queue-full, cancellation, duplicate receipt and provider error behavior.

- [ ] **Step 4: Verify and commit**

Run the deterministic test and three provider live cells with three-repeat generated artifacts. Require zero errors, stable digest and `persisted_operations == committed_operations`. Update README with separate admission, single-commit and batch-to-commit tables, then commit `test(perf): measure committed batch ingestion`.

### Task 6: Final certification

**Files:**

- Create: `docs/audit/performance/2026-08-04-middleware-performance-closure.md`

- [ ] **Step 1: Run quality gates**

```bash
cargo fmt --all -- --check
git diff --check
bash scripts/check-layered-architecture.sh
bash scripts/tests/check-layered-architecture-contract.sh
bash scripts/tests/local-cluster-contract.sh
```

- [ ] **Step 2: Run focused suites**

```bash
cargo test --locked -p dtg-plan --test planning
cargo test --locked -p dtg-execution --test gateway_process
cargo test --locked -p dtg-data --test process
cargo test --locked -p dtg-gateway --test four_process_cluster
```

- [ ] **Step 3: Generate and publish evidence**

Build release binaries and run three serial, three-repeat backend artifacts (PostgreSQL under disposable loopback). The audit records revision, commands, stage deltas, QPS, p50/p95/p99, errors, result digests, host variance and unmet targets. Do not commit generated artifacts.

- [ ] **Step 4: Commit**

```bash
git add docs/audit/performance/2026-08-04-middleware-performance-closure.md README.md
git commit -m "docs: record middleware performance closure"
```
