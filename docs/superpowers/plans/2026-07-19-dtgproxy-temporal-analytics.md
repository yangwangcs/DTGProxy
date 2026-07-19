# DTGProxy Temporal Graph Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** Implement canonical snapshot/window projections, a hot-pluggable analytics SPI, the stable ordinary/temporal algorithm catalog, native distributed execution, incremental maintenance, procedures, caching, and resource isolation.

**Architecture:** Temporal storage produces an immutable backend-neutral CSR projection at one fenced snapshot. Typed providers execute algorithms against that projection. The native provider supports deterministic local and vertex-centric distributed jobs; incremental jobs consume committed logical changes and checkpoint replay positions.

**Tech Stack:** Rust 1.93, petgraph, existing temporal storage/distributed query runtime, Tokio, blake3, deterministic reference oracles.

## Global Constraints

- Algorithms never inspect adapter-native encodings.
- Projection/result cache keys include snapshot, schema, topology, security, spec, provider, and algorithm versions.
- Provider results are schema-checked and size-limited before exposure.
- Distributed algorithms fence topology epoch and checkpoint applied-log positions.
- Optional native/GPU providers are not trusted core and enter only after final boundary audit.

---

### Task 1: Analytics API and provider registry

**Files:**
- Create: `crates/analytics-api/Cargo.toml`
- Create: `crates/analytics-api/src/{lib,descriptor,algorithm,parameter,result,provider,registry,limits,error}.rs`
- Test: `crates/analytics-api/tests/{descriptor,registry,schema,limits,compatibility}.rs`

1. Write failing tests for descriptors, duplicate/versioned registration, typed parameters/results, directed/weighted/time requirements, cancellation, and limits.
2. Implement `ProviderDescriptorV1`, `AlgorithmDescriptorV1`, request/result types, provider trait, immutable registry snapshot, and compatibility validation.
3. Run `cargo test -p analytics-api`; expect pass.
4. Commit: `feat(analytics): define stable provider and algorithm SPI`.

### Task 2: Canonical temporal projections

**Files:**
- Create: `crates/graph-projection/Cargo.toml`
- Create: `crates/graph-projection/src/{lib,spec,builder,csr,column,temporal,distributed,cache,identity,error}.rs`
- Test: `crates/graph-projection/tests/{snapshot,window,direction,properties,distributed_merge,cache_security}.rs`

1. Write failing tests for AS OF/window semantics, transaction-time fencing, direction, self/multi-edges, property columns, deterministic ID mapping, shard merge, and security-isolated cache keys.
2. Implement bounded projection scans, CSR construction, temporal edge intervals/events, property columns, identity hashing, and immutable cache entries.
3. Compare projections across memory/RocksDB/Neo4j/PostgreSQL fixtures.
4. Run `cargo test -p graph-projection`; expect pass.
5. Commit: `feat(analytics): build canonical bitemporal projections`.

### Task 3: Native ordinary graph algorithms

**Files:**
- Create: `crates/analytics-native/Cargo.toml`
- Create: `crates/analytics-native/src/{lib,traversal,components,centrality,triangles,shortest_path,community,math,error}.rs`
- Test: `crates/analytics-native/tests/{traversal,components,pagerank,triangles,shortest_paths,centrality,community}.rs`

1. Write small-graph oracle tests and randomized invariant tests for BFS, DFS, WCC, SCC, degree, PageRank, triangle count, clustering coefficients, SSSP, bounded APSP, betweenness, closeness, label propagation, and Louvain.
2. Implement deterministic algorithms with explicit directed/weighted behavior, convergence limits, cancellation checks, and overflow-safe arithmetic.
3. Run `cargo test -p analytics-native`; expect pass.
4. Commit: `feat(analytics): implement stable native graph algorithms`.

### Task 4: Native temporal algorithms

**Files:**
- Create: `crates/analytics-native/src/temporal/{mod,path,reachability,centrality,pagerank,motif,burst,window}.rs`
- Test: `crates/analytics-native/tests/{temporal_paths,temporal_reachability,temporal_centrality,temporal_pagerank,temporal_motifs,burst,windowed}.rs`

1. Write timestamped oracle graphs that distinguish earliest-arrival, latest-departure, fastest, and minimum-hop temporal paths.
2. Implement temporal reachability/closeness/betweenness/PageRank, motif counting, burst score, and windowed components/degree/triangles with strict nondecreasing-time traversal semantics.
3. Add boundary tests for equal timestamps, open/closed windows, zero-duration edges, unreachable pairs, and deterministic ties.
4. Run temporal algorithm tests; expect pass.
5. Commit: `feat(analytics): implement stable temporal algorithm catalog`.

### Task 5: Petgraph provider and conformance

**Files:**
- Create: `crates/provider-petgraph/Cargo.toml`
- Create: `crates/provider-petgraph/src/{lib,convert,provider,error}.rs`
- Test: `crates/provider-petgraph/tests/{conformance,equivalence,cancel,limits}.rs`

1. Pin petgraph and record its source/license metadata in the crate.
2. Implement only algorithms with matching declared semantics; convert stable IDs deterministically.
3. Run the common provider conformance suite against native and petgraph providers and compare supported algorithm outputs/tolerances.
4. Commit: `feat(analytics): add petgraph provider`.

### Task 6: Analytics runtime, procedures, and cache

**Files:**
- Create: `crates/analytics-runtime/Cargo.toml`
- Create: `crates/analytics-runtime/src/{lib,runtime,catalog,projection_cache,result_cache,scheduler,budget,cancel,metadata,error}.rs`
- Modify: `crates/procedure-runtime/src/catalog.rs`
- Test: `crates/analytics-runtime/tests/{dispatch,cache,budgets,cancel,metadata,procedures}.rs`

1. Write failing tests for provider selection, explicit provider requests, cache hit/miss/isolation, CPU/memory/deadline quotas, cancellation, result validation, and metadata completeness.
2. Implement runtime dispatch, tenant scheduler, projection/result caches, stable run metadata, and `dtg.graph.project`, `dtg.graph.drop`, `dtg.algo.list`, `dtg.algo.run`, `dtg.algo.status`, `dtg.algo.cancel` procedures.
3. Compile `CALL ... YIELD` and `ANALYZE GRAPH ... USING ...` into the same procedure/runtime request.
4. Run `cargo test -p analytics-runtime -p procedure-runtime`; expect pass.
5. Commit: `feat(analytics): expose resource-bounded runtime and procedures`.

### Task 7: Distributed native analytics

**Files:**
- Create: `crates/analytics-native/src/distributed/{mod,job,partition,superstep,message,aggregate,checkpoint,recover}.rs`
- Modify: `crates/distributed-query/src/protocol.rs`
- Test: `crates/analytics-native/tests/{distributed_bfs,distributed_components,distributed_pagerank,distributed_failure,rebalance}.rs`

1. Write equivalence tests between local and 2/3/8-partition execution.
2. Implement partition-local state, bounded messages, global aggregators, convergence, checkpoints, retry, cancellation, and topology fencing.
3. Reject unsupported distributed algorithms unless the projection is within the configured gather limit.
4. Inject worker loss and rebalance; verify recovery from the last complete superstep.
5. Commit: `feat(analytics): add topology-fenced distributed execution`.

### Task 8: Incremental analytics

**Files:**
- Create: `crates/incremental-analytics/Cargo.toml`
- Create: `crates/incremental-analytics/src/{lib,change,subscription,state,checkpoint,replay,degree,components,pagerank,window,error}.rs`
- Modify: `crates/storage-api/src/lib.rs`
- Test: `crates/incremental-analytics/tests/{replay,gap,degree,components,pagerank,window,upgrade,topology}.rs`

1. Add a versioned logical committed-change cursor to storage SPI with unsupported default.
2. Write replay/gap/idempotency tests before implementations.
3. Implement incremental degree, windowed degree, supported component maintenance, residual/delta PageRank, and window expiration; checkpoint input indexes and state digests.
4. On gaps/topology/provider incompatibility, deterministically replay or full-recompute and expose the reason in metadata.
5. Commit: `feat(analytics): maintain checkpointed incremental jobs`.

### Task 9: Stable analytics matrix

**Files:**
- Create: `tests/analytics_backend_deployment_matrix.rs`
- Create: `tests/analytics_procedure_e2e.rs`
- Create: `benches/analytics.rs`

1. Run every stable algorithm on fixed snapshot/window fixtures across three backends and two deployment modes.
2. Compare canonical output and floating-point tolerances to native reference results.
3. Exercise provider hot registration/removal, cancellation, cache invalidation, transaction visibility, rebalance, and incremental replay.
4. Record projection, execution, exchange, and memory metrics.
5. Commit: `test(analytics): certify stable temporal algorithm matrix`.
