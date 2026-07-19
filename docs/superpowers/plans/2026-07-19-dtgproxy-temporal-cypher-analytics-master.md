# DTGProxy Temporal Cypher and Analytics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** Deliver the DTGProxy 1.0 prototype as a backend-neutral Cypher/Bolt, distributed bitemporal query and transaction, and temporal graph analytics middleware over RocksDB, Neo4j, and PostgreSQL.

**Architecture:** Preserve the proven storage, Raft, routing, and transaction layers. Add a versioned language front end and Temporal IR v2 beside the existing debug DSL/IR v1, lower v2 to an explicit distributed physical DAG, and expose one session/transaction service to Bolt and the existing gateway API. Analytics consumes immutable canonical projections, never backend-native graph objects. Every public wire, plan, projection, and provider structure is versioned and bounded.

**Tech Stack:** Rust 1.93, Tokio, Tonic, existing Raft and adapter SPI, handwritten safe-Rust Cypher/Bolt core, serde for control metadata, blake3 for fingerprints, petgraph as the first in-process provider, optional C/WASM/remote provider boundaries.

## Global Constraints

- Keep `#![forbid(unsafe_code)]` in every core crate. Any future native FFI is isolated in a `*-sys` crate and is not part of the 1.0 trusted core.
- Keep `temporal-query` and `TemporalPlan` v1 as a supported debug surface until v2 migration tests prove equivalence.
- Never send user Cypher to a backend. Backend pushdown receives only validated physical fragments.
- TSO exclusively owns new transaction timestamps. Client-supplied transaction time is read-only.
- Enforce graph, schema, topology, security, compatibility-profile, and snapshot identities at every distributed boundary.
- All collections and frames have explicit byte/item/depth limits before allocation.
- Results are deterministically ordered whenever Cypher semantics require ordering; unordered results still have deterministic canonical encoding for tests.
- PrimaryReplica and Shared-Nothing share language, transaction, and analytics APIs; only routing and exchange topology differ.
- RocksDB, Neo4j, and PostgreSQL must pass the same semantic suite. Backend-native capabilities are optimizations only.
- Complete implementation first; perform the unified license, security, compatibility, resource-boundary, and performance audit only after the main feature matrix passes.

---

## 1. Existing Foundation and Compatibility Strategy

The implementation builds on these existing stable contracts:

| Existing crate | Contract retained | Extension point |
|---|---|---|
| `storage-api` | versioned adapter descriptor, idempotent Raft apply, multi-get, ordered scan, snapshots | query pushdown descriptors and change cursor |
| `adapter-*` | RocksDB/Neo4j/PostgreSQL logical keyspaces and hot swap | optional physical fragment execution |
| `temporal-storage` | canonical bitemporal materialization and history | projection scans and write lowering |
| `txn-protocol` | home-shard 2PC, participant intents, recovery | read/write dependency metadata and constraint locks |
| `control-plane` | graph/schema/topology/backend profiles | statistics, procedures, algorithms, policies |
| `temporal-ir` | v1 debug plans | isolated `v2` module with logical row algebra |
| `query-executor` | v1 local and distributed scan execution | v2 operator runtime and physical fragments |
| `gateway-node` | admission, routing, remote shard access | session service and Bolt listener |
| `dtgproxy` | embedded runtime, transaction coordinator, migrations | compiled-query and analytics orchestration |

Compatibility is additive: v1 types are not renamed or reinterpreted. A `LegacyPlanAdapter` may convert only semantically representable v1 plans into v2 plans.

## 2. Workspace Dependency Graph

```text
temporal-types
  ├─ cypher-ast ── cypher-syntax
  ├─ temporal-ir(v2)
  └─ analytics-api

cypher-syntax ──> cypher-ast
cypher-sema ─────> cypher-ast + control-plane
cypher-compiler ─> cypher-syntax + cypher-sema + temporal-ir

temporal-ir ─────> physical-plan ──> distributed-query
query-optimizer ─> temporal-ir + physical-plan + storage-api
distributed-query -> query-executor + shard-client + txn-protocol

procedure-runtime -> cypher-ast + analytics-api
graph-projection ─> analytics-api + temporal-storage + distributed-query
analytics-runtime -> analytics-api + graph-projection + procedure-runtime
analytics-native ─> analytics-api
provider-petgraph -> analytics-api + petgraph
incremental-analytics -> analytics-api + graph-projection

bolt-protocol ────> cypher-ast
bolt-server ──────> bolt-protocol + gateway-node session API
gateway-node ─────> cypher-compiler + query-optimizer + distributed-query
dtgproxy ─────────> all orchestration crates
```

No dependency may point from a semantic/core crate to a concrete adapter.

## 3. Shared Public Types

### 3.1 Cypher value and row

`crates/cypher-ast/src/value.rs` owns the public language value:

```rust
pub enum CypherValue {
    Null,
    Boolean(bool),
    Integer(i64),
    Float(CanonicalFloat),
    String(Arc<str>),
    Bytes(Arc<[u8]>),
    List(Arc<[CypherValue]>),
    Map(Arc<BTreeMap<Arc<str>, CypherValue>>),
    Node(NodeValue),
    Relationship(RelationshipValue),
    Path(PathValue),
    Date(DateValue),
    LocalTime(LocalTimeValue),
    Time(TimeValue),
    LocalDateTime(LocalDateTimeValue),
    DateTime(DateTimeValue),
    Duration(DurationValue),
    Point(PointValue),
    Vector(VectorValue),
}

pub struct RecordBatch {
    pub schema: Arc<RowSchema>,
    pub rows: Vec<Box<[CypherValue]>>,
}
```

Floats use bit-preserving equality only for transport; Cypher comparison, NaN, NULL, and ordering rules live in `cypher-sema::value_semantics`.

### 3.2 Diagnostics

All language stages return stable diagnostics:

```rust
pub struct Diagnostic {
    pub code: DiagnosticCode,
    pub severity: Severity,
    pub message: String,
    pub primary: SourceSpan,
    pub related: Vec<RelatedSpan>,
}

pub struct Compilation<T> {
    pub value: Option<T>,
    pub diagnostics: Vec<Diagnostic>,
}
```

Diagnostics never embed backend error strings directly; errors map through a versioned DTG error catalog.

### 3.3 Temporal and snapshot context

`temporal-ir::v2` defines:

```rust
pub enum ValidTimeScope {
    AsOf(ValidTimeExpr),
    Between { start: ValidTimeExpr, end: ValidTimeExpr },
}

pub enum TransactionTimeScope {
    Current,
    AsOf(TransactionTimeExpr),
}

pub struct ReadSnapshot {
    pub graph_id: u64,
    pub schema_version: u64,
    pub topology_epoch: u64,
    pub transaction_time: TransactionTime,
    pub security_fingerprint: [u8; 32],
}
```

One query obtains exactly one `ReadSnapshot`; every shard request and response repeats its fingerprint.

## 4. Cypher Front End Code Design

### 4.1 Crates and modules

```text
crates/cypher-syntax/src/
  lib.rs limits.rs scanner.rs lexer.rs token.rs parser.rs cst.rs diagnostic.rs
crates/cypher-ast/src/
  lib.rs version.rs statement.rs clause.rs pattern.rs expression.rs value.rs temporal.rs visit.rs
crates/cypher-sema/src/
  lib.rs catalog.rs scope.rs binder.rs types.rs aggregate.rs update.rs temporal.rs functions.rs errors.rs
crates/cypher-compiler/src/
  lib.rs options.rs normalize.rs lower.rs fingerprint.rs cache.rs legacy.rs
```

The parser is lossless and recovery-aware. Version scanning occurs before lexing so reserved-word and feature gates are profile-specific. DTG `AT` and `DIFF` nodes remain explicit through semantic analysis, then lower to ordinary temporal logical operators.

### 4.2 Compiler API

```rust
pub trait CompilationCatalog: Send + Sync {
    fn graph(&self, name: &str) -> Result<GraphSchema, CatalogLookupError>;
    fn procedure(&self, name: &QualifiedName) -> Option<ProcedureSignature>;
    fn function(&self, name: &QualifiedName) -> Option<FunctionSignature>;
}

pub struct CypherCompiler<C> { /* catalog, cache, limits */ }

impl<C: CompilationCatalog> CypherCompiler<C> {
    pub fn compile(
        &self,
        text: &str,
        parameters: &ParameterTypes,
        session: &CompileSession,
    ) -> Compilation<CompiledQuery>;
}

pub struct CompiledQuery {
    pub profile: CypherProfile,
    pub fingerprint: QueryFingerprint,
    pub parameter_schema: ParameterSchema,
    pub result_schema: RowSchema,
    pub effect: QueryEffect,
    pub logical_plan: temporal_ir::v2::LogicalPlan,
}
```

Plan-cache keys include normalized query, language baseline, graph/schema version, parameter type shape, security fingerprint, and feature flags.

## 5. Temporal IR v2 and Optimizer Code Design

### 5.1 Logical plan

`temporal-ir::v2::LogicalPlan` is an arena of immutable nodes with validated input IDs:

```rust
pub struct LogicalPlan {
    pub header: PlanHeaderV2,
    pub nodes: Vec<LogicalNode>,
    pub root: LogicalNodeId,
    pub output: RowSchema,
}

pub enum LogicalOperator {
    Argument, NodeScan(NodeScan), RelationshipScan(RelationshipScan),
    Expand(Expand), VarExpand(VarExpand), ShortestPath(ShortestPath),
    Filter(ScalarExpr), Project(Vec<NamedExpr>), Unwind(Unwind),
    Aggregate(Aggregate), Distinct(Vec<SlotId>), Sort(Sort),
    Skip(ScalarExpr), Limit(ScalarExpr),
    InnerJoin(Join), LeftJoin(Join), SemiJoin(Join), AntiJoin(Join),
    Union { all: bool }, Apply(Apply),
    TemporalSlice(TemporalContext), Diff(Diff),
    Create(Create), Merge(Merge), Set(Set), Remove(Remove), Delete(Delete),
    ProcedureCall(ProcedureCall), Finish,
}
```

`LogicalPlan::validate()` proves acyclicity, valid slots, consistent schemas, bounded node count, one graph identity, legal temporal scopes, and write/read effect ordering.

### 5.2 Physical plan

`physical-plan` owns serializable execution contracts:

```rust
pub struct PhysicalPlan {
    pub header: PhysicalPlanHeaderV1,
    pub fragments: Vec<PlanFragment>,
    pub exchanges: Vec<Exchange>,
    pub root: FragmentId,
}

pub enum PhysicalOperator {
    NativeScan(NativeScan), NativeExpand(NativeExpand),
    BackendFragment(ValidatedBackendFragment),
    Filter(PhysicalExpr), Project(Vec<PhysicalExpr>),
    HashJoin(HashJoin), MergeJoin(MergeJoin), Aggregate(AggregateExec),
    Sort(SortExec), TopN(TopNExec), Write(WriteExec), Procedure(ProcedureExec),
}

pub enum ExchangeKind { Gather, Broadcast, HashPartition(Vec<SlotId>), RangePartition(Vec<SlotId>) }
```

Every fragment carries input/output schemas, memory budget, cancellation ID, snapshot fingerprint, and required adapter capabilities.

### 5.3 Optimizer

`query-optimizer` runs deterministic passes:

1. temporal scope normalization and predicate safety tagging;
2. constant folding and three-valued predicate simplification;
3. projection/limit pruning;
4. predicate and temporal-index pushdown;
5. expand direction and join reordering;
6. statistics-based scan/expand/join alternatives;
7. shard routing and exchange insertion;
8. backend capability selection with native fallback;
9. resource-budget and physical-plan validation.

Rules expose trace records so `EXPLAIN` can state why a rewrite or pushdown was accepted/rejected.

## 6. Distributed Query and Transaction Code Design

### 6.1 Runtime service contracts

`distributed-query` separates coordinator, worker, exchange, and cancellation:

```rust
pub trait QueryWorker: Send + Sync {
    fn execute<'a>(&'a self, request: FragmentRequest)
        -> QueryFuture<'a, Box<dyn BatchStream + Send + 'a>>;
    fn cancel<'a>(&'a self, cancellation: CancellationId) -> QueryFuture<'a, ()>;
}

pub struct FragmentRequest {
    pub protocol_version: u16,
    pub plan: PlanFragment,
    pub snapshot: ReadSnapshot,
    pub deadline_unix_ms: u64,
    pub memory_bytes: u64,
    pub batch_rows: u32,
}
```

The coordinator validates all response identities before accepting a batch. Exchange senders reserve credits before serialization; receivers release credits after consumption. Spill files are checksummed, query-scoped, and removed on success, failure, or startup recovery.

### 6.2 Session and transaction overlay

`gateway-node::session` is transport-independent:

```rust
pub trait QueryService: Send + Sync {
    fn run<'a>(&'a self, session: SessionId, request: RunRequest)
        -> GatewayFuture<'a, QueryCursor>;
    fn pull<'a>(&'a self, session: SessionId, cursor: CursorId, n: u64)
        -> GatewayFuture<'a, PullResult>;
    fn begin<'a>(&'a self, session: SessionId, options: TxOptions)
        -> GatewayFuture<'a, TxHandle>;
    fn commit<'a>(&'a self, session: SessionId, tx: TxHandle)
        -> GatewayFuture<'a, Bookmark>;
    fn rollback<'a>(&'a self, session: SessionId, tx: TxHandle)
        -> GatewayFuture<'a, ()>;
}
```

`TransactionOverlay` stores canonical element deltas keyed by stable identity. Reads compose base snapshot + prior statement deltas + current statement delta. Commit lowers the final overlay into `PreparedMutationBatch` values and delegates to the existing `TransactionCoordinator`/`txn-protocol` home-shard 2PC.

MERGE and uniqueness constraints acquire deterministic logical constraint keys. Serializable mode adds read/range dependencies to prewrite validation; snapshot mode retains current write-write conflict behavior.

## 7. Bolt Code Design

`bolt-protocol` contains no I/O and fuzzes independently:

```text
src/handshake.rs manifest.rs packstream.rs value.rs message.rs version.rs limits.rs error.rs
```

`bolt-server` owns TCP/TLS/WebSocket transports and one state machine:

```rust
pub enum ConnectionState { Connected, Ready, Streaming, TxReady, TxStreaming, Failed, Interrupted, Defunct }

pub struct BoltMachine<S> { service: Arc<S>, state: ConnectionState, session: SessionId, /* cursors */ }

impl<S: QueryService> BoltMachine<S> {
    pub async fn handle(&mut self, message: ClientMessage) -> Result<Vec<ServerMessage>, BoltError>;
}
```

Transport code only frames bytes and drives the state machine. PULL/DISCARD use cursor paging, demand credits, deadlines, and cancellation; ROUTE returns gateway addresses only.

## 8. Analytics Code Design

### 8.1 Canonical projection

```rust
pub struct ProjectionSpec {
    pub graph_id: u64,
    pub node_predicate: Option<CompiledPredicate>,
    pub relationship_predicate: Option<CompiledPredicate>,
    pub valid_time: ValidTimeScope,
    pub transaction_time: TransactionTimeScope,
    pub direction: ProjectionDirection,
    pub properties: PropertySelection,
}

pub struct CanonicalProjection {
    pub metadata: ProjectionMetadata,
    pub vertex_ids: Arc<[ElementId]>,
    pub offsets: Arc<[u64]>,
    pub neighbors: Arc<[u64]>,
    pub edge_ids: Arc<[ElementId]>,
    pub edge_times: Arc<[TemporalEdgeInterval]>,
    pub columns: Arc<[PropertyColumn]>,
}
```

IDs are sorted and remapped densely. Projection identity hashes graph/schema/topology/snapshot/spec/security. Cache entries are immutable and tenant-budgeted.

### 8.2 Provider SPI

```rust
pub trait AnalyticsProvider: Send + Sync {
    fn descriptor(&self) -> &ProviderDescriptorV1;
    fn algorithms(&self) -> &[AlgorithmDescriptorV1];
    fn run<'a>(&'a self, request: AlgorithmRequest<'a>) -> AnalyticsFuture<'a, AlgorithmResult>;
    fn cancel<'a>(&'a self, run: RunId) -> AnalyticsFuture<'a, ()>;
}
```

The provider receives only validated projections and typed parameters. The first stable providers are `analytics-native` and `provider-petgraph`. TGLib, LAGraph, GraphScope, and cuGraph enter through separate optional crates after the final license/native-boundary audit.

### 8.3 Stable algorithm catalog

Ordinary graph: BFS, DFS, weak/strong connected components, degree, PageRank, triangle count, local/global clustering coefficient, unweighted/weighted single-source shortest path, all-pairs shortest path for bounded projections, betweenness, closeness, label propagation, Louvain.

Temporal graph: earliest-arrival, latest-departure, fastest, shortest temporal path, temporal reachability, temporal closeness, temporal betweenness, temporal PageRank, temporal motif counting, burst/change-point score, windowed components, windowed degree, windowed triangle count.

Each descriptor fixes directedness, weight/time requirements, determinism, incremental support, partitioning support, parameter schema, result schema, and complexity/resource hints.

### 8.4 Distributed and incremental execution

Vertex-centric native jobs use partition-local state, superstep messages, global aggregators, checkpointed iteration, topology epoch fencing, and deterministic convergence. Algorithms not safely distributable run on a gathered bounded projection or reject with an actionable capacity error.

Incremental jobs consume committed logical change batches after transaction apply. Checkpoints record input applied-log indexes, projection identity, provider/algorithm version, and state digest. Any gap, topology change, or incompatible algorithm upgrade triggers replay or full recomputation.

## 9. Execution Order and Checkpoints

1. Implement the Cypher/Bolt plan and commit after parser/sema/protocol state-machine suites pass.
2. Implement IR/optimizer/distributed-query plan and commit after local/distributed equivalence and fault tests pass.
3. Implement Cypher temporal writes and transaction plan and commit after single-/multi-shard recovery tests pass.
4. Implement projection/analytics plan and commit after algorithm oracle and provider conformance suites pass.
5. Integrate gateway end-to-end and run backend/deployment matrices.
6. Only then execute the unified boundary certification plan and fix every finding.

Detailed plans:

- `docs/superpowers/plans/2026-07-19-dtgproxy-cypher-bolt.md`
- `docs/superpowers/plans/2026-07-19-dtgproxy-temporal-ir-distributed-query.md`
- `docs/superpowers/plans/2026-07-19-dtgproxy-temporal-transactions.md`
- `docs/superpowers/plans/2026-07-19-dtgproxy-temporal-analytics.md`
- `docs/superpowers/plans/2026-07-19-dtgproxy-boundary-certification.md`

## 10. Definition of Done

- Every stable syntax, semantic, transaction, algorithm, backend, and deployment row in the approved specification has a named automated test.
- No `todo!`, `unimplemented!`, placeholder success, silent fallback, or ignored conformance test exists in stable code.
- Cypher 5 and Cypher 25 profiles are versioned and tested independently; DTG extensions never change standard-query semantics.
- Explicit and auto-commit Bolt transactions share the same coordinator used by the existing API.
- Three backends and both deployment modes return canonical-equivalent results for the stable matrix.
- Crash, retry, replay, cancellation, rebalance, and stale-epoch tests demonstrate safe failure.
- The final boundary report records dependency licenses, source provenance, protocol/TCK compatibility, fuzz results, resource limits, security findings, and benchmark deltas.
- `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --all-features -- -D warnings`, and the complete test matrix pass in a repaired C/C++ toolchain environment.
