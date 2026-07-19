# DTGProxy Temporal IR and Distributed Query Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** Implement validated Temporal IR v2, deterministic RBO/CBO optimization, backend capability lowering, and bounded distributed physical execution for both deployment modes.

**Architecture:** Logical algebra remains backend/distribution independent. The optimizer converts it into a versioned fragment DAG with explicit exchanges and budgets. Workers execute native operators or validated backend fragments at one fenced snapshot; the coordinator streams and validates batches.

**Tech Stack:** Rust 1.93, existing temporal storage/shard client/Raft runtime, blake3, Tokio streams/channels, proptest and deterministic fault injection.

## Global Constraints

- Do not change v1 plan wire semantics.
- A plan is executable only after structural, schema, temporal, capability, resource, and topology validation.
- Every distributed request is deadline/cancellation/snapshot fenced.
- Never silently gather an unbounded distributed dataset.

---

### Task 1: Temporal IR v2 arena and validator

**Files:**
- Create: `crates/temporal-ir/src/v2/{mod,header,schema,expr,logical,validate,codec}.rs`
- Modify: `crates/temporal-ir/src/lib.rs`
- Test: `crates/temporal-ir/tests/{v2_validate,v2_temporal,v2_codec,v1_compat}.rs`

1. Write failing tests for each operator, invalid node IDs, cycles, slot mismatches, illegal temporal scopes, mixed graphs, update ordering, excessive nodes/depth, and codec round trips/version rejection.
2. Implement immutable IDs, row schemas, scalar expressions, logical operators, plan builder, validator, and deterministic binary codec.
3. Run `cargo test -p temporal-ir`; expect pass with all v1 tests unchanged.
4. Commit: `feat(ir): add validated temporal logical IR v2`.

### Task 2: Physical plan and capability contracts

**Files:**
- Create: `crates/physical-plan/Cargo.toml`
- Create: `crates/physical-plan/src/{lib,header,operator,fragment,exchange,backend,validate,codec,error}.rs`
- Modify: `crates/storage-api/src/lib.rs`
- Test: `crates/physical-plan/tests/{validate,codec,capabilities,budgets}.rs`
- Test: `crates/storage-api/tests/query_capabilities.rs`

1. Write failing tests for fragment DAGs, exchange schemas, budget zero/overflow, unknown capabilities, and raw query rejection.
2. Add versioned `QueryCapabilitiesV1`, typed predicate/adjacency/temporal-index fragment descriptors, and optional `execute_fragment` defaulting to unsupported.
3. Implement physical operators, exchanges, codecs, and validators.
4. Run `cargo test -p storage-api -p physical-plan`; expect pass.
5. Commit: `feat(plan): define physical DAG and validated pushdown SPI`.

### Task 3: Rule and cost optimizer

**Files:**
- Create: `crates/query-optimizer/Cargo.toml`
- Create: `crates/query-optimizer/src/{lib,context,normalize,rules,memo,stats,cost,enumerate,partition,pushdown,trace,error}.rs`
- Test: `crates/query-optimizer/tests/{temporal_rules,predicate_pushdown,join_order,partitioning,backend_fallback,trace}.rs`

1. Write golden-plan tests for temporal normalization, unsafe time-predicate rejection, filter/projection/limit pushdown, expand reversal, join alternatives, exchange insertion, and capability fallback.
2. Implement a bounded memo, statistics snapshots, deterministic cost tie-breaking, rule traces, and physical-plan validation at exit.
3. Add catalog statistics interfaces without coupling the optimizer to control-plane persistence.
4. Run `cargo test -p query-optimizer`; expect pass.
5. Commit: `feat(optimizer): add temporal RBO and distributed CBO`.

### Task 4: Vectorized operator runtime

**Files:**
- Create: `crates/query-executor/src/v2/{mod,batch,context,scan,expand,filter,project,join,aggregate,sort,write,procedure,spill,error}.rs`
- Modify: `crates/query-executor/src/lib.rs`
- Test: `crates/query-executor/tests/{v2_operators,v2_memory,v2_spill,v2_cancel,v1_equivalence}.rs`

1. Write failing tests for operator semantics, NULL behavior, bounded batches, memory exhaustion, spill checksum/recovery, deadline, and cancellation.
2. Implement batch iterators/streams, accounting allocator, external sort/hash spill, and each physical operator.
3. Compare v1 and v2 results for all representable legacy plans.
4. Run `cargo test -p query-executor --exclude adapter-rocksdb`; expect pass in pure-Rust environment.
5. Commit: `feat(executor): execute bounded physical operators`.

### Task 5: Distributed coordinator, workers, and exchanges

**Files:**
- Create: `crates/distributed-query/Cargo.toml`
- Create: `crates/distributed-query/src/{lib,protocol,coordinator,worker,exchange,credits,cancel,retry,merge,metrics,error}.rs`
- Test: `crates/distributed-query/tests/{snapshot_fence,exchange,backpressure,cancel,retry,stale_epoch,partial_failure}.rs`

1. Write fault-injected tests for mismatched graph/schema/topology/security/snapshot, duplicate/missing batches, credit exhaustion, cancellation, retryable leader movement, and non-retryable partial writes.
2. Implement versioned fragment requests/responses, worker trait, local worker, coordinator, credit-based exchanges, deterministic merge, cancellation tree, and retry classification.
3. Ensure no response data is exposed before identity validation.
4. Run `cargo test -p distributed-query`; expect pass.
5. Commit: `feat(query): add fenced distributed fragment runtime`.

### Task 6: Deployment routing and adapter integration

**Files:**
- Modify: `crates/dtgproxy/src/routing.rs`
- Modify: `crates/dtgproxy/src/runtime.rs`
- Modify: `crates/gateway-node/src/service.rs`
- Modify: `crates/shard-client/src/lib.rs`
- Test: `tests/distributed_query_modes.rs`
- Test: `tests/backend_query_equivalence.rs`

1. Write the same multi-shard query tests for PrimaryReplica and Shared-Nothing.
2. Route point/partition-local plans without exchange and global plans through the physical DAG.
3. Add backend fragment execution for capabilities actually implemented; prove native fallback gives identical results.
4. Run deployment/backend equivalence suites and stale-topology retry tests.
5. Commit: `feat(query): route physical plans across both deployment modes`.
