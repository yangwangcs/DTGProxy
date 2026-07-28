# DTGProxy Snapshot CSR Integration and Clean-Break Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Integrate the Snapshot CSR core with bounded local and remote backends, one exact fragment snapshot, the sole GraphAccessPathSelector, durable Overlay updates, bounded observability, reproducible profiles, clean-break deletion, and final three-backend correctness/resource/performance gates.

**Architecture:** Every fragment opens one caller-owned ReadSnapshot and derives one complete SnapshotCsrKey. Physical Expand carries only shape/cost estimates; query-executor's GraphAccessPathSelector chooses bounded backend adjacency or Snapshot CSR using exact snapshot identity, typed capability generation, cache/admission state, memory/deadline/cancellation, and a versioned measured profile. Local, Sidecar, and Remote read views expose the same bounded pages and exact applied index; no production path wraps full namespace materialization or full graph export.

**Tech Stack:** Rust 1.93, edition 2024, snapshot-csr core plan APIs, storage-api typed pages, RocksDB snapshot iterators, PostgreSQL REPEATABLE READ READ ONLY transactions, Neo4j explicit Query API transactions, Sidecar protobuf, tonic shard transport, query-executor, distributed-query, paper-benchmark, fixed-seed TCK.

## Global Constraints

- This plan starts only after docs/superpowers/plans/2026-07-28-dtgproxy-snapshot-csr-core.md passes its completion gate.
- Current + History Anchor/Delta + Adjacency + applied_log_index remains the only persistent authority. Cache miss, eviction, and process restart must reconstruct from backends without state loss.
- Do not add feature flags, legacy aliases, compatibility modules, dual reads, dual writes, fallback-to-old-path behavior, or backend-specific query semantics.
- PointHistoryReader, bounded adjacency, property hydration, CSR build, and mixed CSR/backend reads in one fragment must use the same ReadSnapshot and the same exact applied_log_index.
- CSR miss, admission rejection, and InsufficientMemory are optional-path outcomes that may choose bounded backend adjacency. Corruption, snapshot mismatch, applied-index mismatch, placement epoch mismatch, mapping/schema mismatch, and non-contiguous Overlay are fail-closed errors.
- The sole GraphAccessPathSelector must consider query shape, depth, direction, estimated cardinality/fanout, demanded properties, exact snapshot identity, cache hit/build cost, memory, deadline, cancellation, typed capability generation, and a versioned measured profile.
- Without a trusted profile, select the conservative bounded backend path. Never blindly build a large CSR for a cold low-reuse query.
- One-hop or low estimated expansion uses bounded backend adjacency; depth >= 2, large expansion, or repeated snapshot may use Snapshot CSR only after admission.
- Point CSR must not serve interval semantics. Until a separate versioned-edge table exists, valid-time interval Expand uses the bounded backend path.
- RocksDB pages use native snapshot iterators; PostgreSQL pages use bounded ordered resumable SQL in REPEATABLE READ READ ONLY; Neo4j pages use bounded parameterized queries in one explicit Query API transaction.
- Do not use PostgreSQL load_all_entries, a full namespace materialization, Neo4j logical export, or a full graph export for query execution or CSR construction.
- Sidecar and Remote continuations must carry a stable non-zero ReadIndex and capability generation; any change within a chain fails closed.
- Overlay consumes only post-durable-apply canonical committed mutations. Overlay failure never rolls back persistence and immediately removes the rolling cache entry from query eligibility.
- Metrics are bounded counters or histograms. Labels must not include raw query text, element IDs, graph IDs, or arbitrary high-cardinality strings.
- The 2% edge ratio, 64 MiB retained bytes, and 10% slowdown values are benchmark seeds, not production defaults. Checked-in defaults must come from reproducible RocksDB, PostgreSQL, and Neo4j results.
- Correctness uses the same fixed seed and dataset for Memory oracle, RocksDB, PostgreSQL, and Neo4j.
- Performance acceptance is: hot CSR depth 2+ throughput at least 2x bounded backend, hot CSR p95 at least 30% lower, retained memory no more than 115% of reservation, cold low-reuse p95 regression no more than 10%, and pre-rebuild Overlay throughput loss no more than 10%.
- Final combined completion also requires the existing PointHistory performance report to contain real post-change PostgreSQL/Neo4j continuation evidence, allocation/copy measurements, retained-memory measurements, and acceptance calculations.
- Keep #![forbid(unsafe_code)], checked length arithmetic, bounded continuations, cancellation polling, and exact generation/index fences.
- Preserve unrelated dirty-worktree changes and stage only files named by the current task.

---

## File Structure

- Modify crates/adapter-rocksdb/src/lib.rs and tests: snapshot adjacency and edge-identity pages over native iterators.
- Modify crates/adapter-postgres/src/lib.rs and live tests: expose typed pages on PostgresReadSnapshot and replace query/CSR materialization with resumable SQL.
- Modify crates/adapter-neo4j/src/lib.rs and live tests: expose typed pages in one Query API transaction with parameterized continuation.
- Create crates/temporal-storage/src/snapshot_graph_page_tck.rs: shared fixed-seed page contract.
- Modify crates/temporal-storage/src/lib.rs: export the page TCK under test support.
- Modify crates/adapter-sidecar/src/lib.rs, service.rs, and protocol/client tests: read-view adjacency and edge-identity messages.
- Modify crates/shard-client/src/lib.rs, storage_adapter.rs, embedded.rs, remote.rs, and TCK tests: typed adjacency, edge identity, and canonical batch commands.
- Modify crates/data-node/src/query_codec.rs, service.rs, and tests: bounded graph-page wire plan/stream encoding.
- Modify crates/physical-plan/src/lib.rs and tests: ExpandEstimate and GraphExpand access metadata.
- Modify crates/query-optimizer/src/lib.rs and tests: emit shape estimates, not a selected execution path.
- Create crates/query-executor/src/graph_access.rs: GraphAccessPathSelector and access request/decision contract.
- Create crates/query-executor/src/graph_profile.rs: in-memory measured profile contract; Task 10 adds strict JSON loading.
- Create crates/query-executor/src/bounded_adjacency.rs: snapshot-bound backend adjacency and batch hydration.
- Modify crates/query-executor/src/temporal.rs, context.rs, error.rs, lib.rs, and tests: exact snapshot execution, CSR traversal, selector lowering, metrics, and failure mapping.
- Modify crates/distributed-query/src/lib.rs, worker.rs, coordinator.rs, and tests: placement/mapping identity and exact applied-index enforcement.
- Modify crates/shard-runtime/src/state_machine.rs and lib.rs: post-durable-apply broadcast.
- Create crates/temporal-storage/src/committed_graph_delta.rs: project committed batches into visible normalized deltas for active rolling keys.
- Modify crates/paper-benchmark sources/tests and add snapshot traversal experiment artifacts.
- Create crates/paper-benchmark/tests/snapshot_csr.rs: benchmark schema, matrix, artifact, and profile-generation contract tests.
- Create docs/audit/performance/2026-07-28-snapshot-csr-benchmark-spec.json: fixed dataset, seed, lifecycle, matrix, warmup, repetition, and statistics contract.
- Create scripts/run-snapshot-csr-performance.sh and scripts/tests/run-snapshot-csr-performance-contract.sh: reproducible three-backend run orchestration.
- Create docs/audit/performance/2026-07-28-snapshot-csr-profile-v1.json: versioned selector/rebuild profile.
- Create docs/audit/performance/2026-07-28-snapshot-csr-clean-break.md: final methods, evidence, results, bottlenecks, and conclusion.
- Create scripts/check-snapshot-csr-clean-break.sh and scripts/tests/check-snapshot-csr-clean-break.sh: compile/source/evidence gates.
- Delete obsolete Expand selection/execution, PostgreSQL materialization query paths, and old traversal benchmark entry points after replacement gates pass.

### Task 7: Implement Real Bounded Local Backend Pages

**Files:**
- Create: crates/temporal-storage/src/snapshot_graph_page_tck.rs
- Modify: crates/temporal-storage/src/lib.rs
- Modify: crates/adapter-memory/tests/adapter_contract.rs
- Modify: crates/adapter-rocksdb/src/lib.rs
- Modify: crates/adapter-rocksdb/tests/adapter_contract.rs
- Modify: crates/adapter-postgres/src/lib.rs
- Modify: crates/adapter-postgres/tests/live_postgres.rs
- Modify: crates/adapter-neo4j/src/lib.rs
- Modify: crates/adapter-neo4j/tests/live_neo4j.rs

**Interfaces:**
- Consumes: ReadSnapshot::expand_adjacency, ReadSnapshot::scan_edge_identities, AdjacencyExpandRequest/Page, EdgeIdentityScanRequest/Page, snapshot-csr applied-index wrappers.
- Produces:

~~~rust
pub fn run_snapshot_graph_page_tck(
    adapter: &dyn StorageAdapter,
) -> Result<SnapshotGraphPageTckOutcome, TemporalStoreError>;

pub struct SnapshotGraphPageTckOutcome {
    pub applied_log_index: u64,
    pub outgoing_digest: [u8; 32],
    pub incoming_digest: [u8; 32],
    pub edge_identity_digest: [u8; 32],
    pub page_count: u64,
}
~~~

- [ ] **Step 1: Add the fixed-seed TCK and backend RED calls**

The dataset contains local, cross-shard, parallel, self-loop, currently deleted/historically visible, and future-created edges. Request page bounds of 2 items and 512 bytes to force continuation. Run the same function from Memory, RocksDB, PostgreSQL live, and Neo4j live tests.

- [ ] **Step 2: Run RED backend tests**

Run:

~~~bash
cargo test --locked -p adapter-rocksdb --test adapter_contract snapshot_graph_pages -- --test-threads=1
cargo test --locked -p adapter-postgres --test live_postgres snapshot_graph_pages -- --test-threads=1
cargo test --locked -p adapter-neo4j --test live_neo4j snapshot_graph_pages -- --test-threads=1
~~~

Expected: each selected test fails with UnsupportedOperation for snapshot adjacency or snapshot edge identity. A live backend connection failure is environment evidence, not GREEN.

- [ ] **Step 3: Implement RocksDB snapshot pages**

In RocksReadSnapshot, expose the existing bounded adjacency iterator through the trait and add scan_edge_identities using snapshot.iterator_cf from request.span().start(). Both methods stop before max_items/max_bytes, return the first unconsumed LogicalKey as continuation, preserve strict key order, and report self.applied_log_index.

Advertise Candidate adjacency and Exact edge_identity_scan only when begin_read_snapshot is supported. AdjacencyExpandRequest carries no temporal predicate, so no backend may advertise Exact graph visibility for this page.

- [ ] **Step 4: Implement PostgreSQL snapshot pages**

Expose PostgresReadSnapshot::expand_adjacency_page directly from impl ReadSnapshot. Add POSTGRES_TYPED_EDGE_IDENTITY_SCAN_SQL over dtgproxy.edge_identity with:

~~~sql
WHERE instance_id = $1
  AND graph_id = $2
  AND partition_id = $3
  AND edge_id >= $4
ORDER BY edge_id
LIMIT $5
~~~

Fetch max_items + 1 rows, encode canonical EdgeIdentity keys/values, enforce the response byte limit before retention, and return the first unconsumed canonical key. Keep the entire continuation chain in the existing REPEATABLE READ READ ONLY transaction.

Change the snapshot adjacency page guarantee to Candidate; BoundedAdjacencyReader performs the mandatory temporal visibility residual.

Raw StorageAdapter::multi_get/scan and logical export may retain management-specific code temporarily, but query/CSR methods in this task must not call load_all_entries.

- [ ] **Step 5: Implement Neo4j snapshot pages**

Expose Neo4jReadSnapshot::expand_adjacency_page through impl ReadSnapshot. Add one parameterized edge-identity query over DTGCanonicalRecord with instance_id, keyspace, lower key, upper key, ORDER BY logical_key_hex, and LIMIT max_items + 1. Execute it through the same locked Query API transaction created by begin_query_snapshot; do not call logical export endpoints.

Keep the snapshot adjacency page guarantee Candidate and the edge-identity page Exact.

- [ ] **Step 6: Add corruption and boundary coverage**

For each backend assert below/equal/above item and byte limits, strict ordering, exact continuation resumption with no duplicate/omitted row, exact applied index, historical identity coverage, and rejection of a malformed continuation. PostgreSQL and Neo4j tests must verify query logging shows multiple bounded pages rather than one full materialization.

- [ ] **Step 7: Run GREEN local backend verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p adapter-memory --test adapter_contract snapshot_graph_pages -- --test-threads=1
cargo test --locked -p adapter-rocksdb --test adapter_contract snapshot_graph_pages -- --test-threads=1
bash scripts/test-postgres-live.sh
bash scripts/test-neo4j-live.sh
~~~

Expected: all available commands exit 0 and all four TCK outcomes have identical digests. Exit 78 from either live script records unavailable evidence and blocks final three-backend completion.

- [ ] **Step 8: Commit**

~~~bash
git add crates/temporal-storage/src/snapshot_graph_page_tck.rs crates/temporal-storage/src/lib.rs crates/adapter-memory/tests/adapter_contract.rs crates/adapter-rocksdb crates/adapter-postgres crates/adapter-neo4j
git commit -m "feat: add bounded backend csr pages"
~~~

### Task 8: Add Sidecar and Remote Typed Page Transport

**Files:**
- Modify: crates/adapter-sidecar/src/lib.rs
- Modify: crates/adapter-sidecar/src/service.rs
- Modify: crates/adapter-sidecar/tests/client.rs
- Modify: crates/adapter-sidecar/tests/protocol.rs
- Modify: crates/adapter-sidecar/tests/stateful_snapshot.rs
- Modify: crates/shard-client/src/lib.rs
- Modify: crates/shard-client/src/storage_adapter.rs
- Modify: crates/shard-client/src/embedded.rs
- Modify: crates/shard-client/src/remote.rs
- Modify: crates/shard-client/tests/embedded_tck.rs
- Modify: crates/shard-client/tests/remote_tck.rs
- Modify: crates/data-node/src/query_codec.rs
- Modify: crates/data-node/src/service.rs

**Interfaces:**
- Consumes: the three snapshot methods expand_adjacency, scan_edge_identities, scan_canonical_batch.
- Produces:

~~~rust
pub struct SnapshotPageContext {
    pub request: ShardRequestContext,
    pub read_index: u64,
    pub capability_generation: u64,
}
pub struct AdjacencyExpandCommand {
    pub context: SnapshotPageContext,
    pub request: AdjacencyExpandRequest,
}
pub struct EdgeIdentityScanCommand {
    pub context: SnapshotPageContext,
    pub request: EdgeIdentityScanRequest,
}
pub struct CanonicalBatchScanCommand {
    pub context: SnapshotPageContext,
    pub request: CanonicalBatchScanRequest,
}
pub trait ShardClient {
    fn expand_adjacency<'a>(
        &'a self,
        request: AdjacencyExpandCommand,
    ) -> ShardClientFuture<'a, AdjacencyExpandPage>;
    fn scan_edge_identities<'a>(
        &'a self,
        request: EdgeIdentityScanCommand,
    ) -> ShardClientFuture<'a, EdgeIdentityScanPage>;
    fn scan_canonical_batch<'a>(
        &'a self,
        request: CanonicalBatchScanCommand,
    ) -> ShardClientFuture<'a, CanonicalBatchScanPage>;
}
~~~

- [ ] **Step 1: Add protocol round-trip RED tests**

Round-trip local/cross adjacency entries, edge identities, canonical batch subpages, continuation, applied index, guarantee, item/byte bounds, read_index, and capability_generation. Add malformed enum, missing request, oversized frame, zero ReadIndex, zero generation, and unknown continuation cases.

- [ ] **Step 2: Run RED protocol tests**

Run:

~~~bash
cargo test --locked -p adapter-sidecar --test protocol snapshot_graph_page -- --test-threads=1
cargo test --locked -p shard-client --test remote_tck snapshot_graph_page -- --test-threads=1
~~~

Expected: FAIL because the new request/response variants and commands do not exist.

- [ ] **Step 3: Add Sidecar read-view messages**

Add feature bits ADJACENCY_EXPAND_READ_VIEW_V1 and EDGE_IDENTITY_SCAN_READ_VIEW_V1. Add Request/Response protobuf variants and ReadViewCommand variants. ReadViewWorker invokes the already-open snapshot; every response carries its exact applied index. SidecarReadSnapshot rejects a response index different from its initial read-view index.

Keep canonical batch as the existing explicit typed message; do not tunnel any of the three operations through Request::Scan or logical snapshot export.

- [ ] **Step 4: Add shard command codecs and data-node execution**

Use distinct bounded plan magic/version tags for adjacency, edge identity, and canonical batch. The decoder validates maximum plan size before allocation, page bounds before dispatch, and continuation keyspace/direction. data-node opens one read view at requested read_index, rejects capability generation mismatch, executes the typed method, and streams one bounded page result.

- [ ] **Step 5: Implement Embedded and Remote parity**

EmbeddedShardClient calls the local ReadSnapshot methods. RemoteShardClient sends the typed plans and reconstructs validated storage-api page types.

RemoteReadSnapshot stores read_index and capability_generation captured at begin_read_snapshot. Before and after every RPC it requires non-zero values and exact equality. Implement multi-page scan_canonical_batch so PointHistoryReader never degrades to one RPC per element.

- [ ] **Step 6: Add moved-fence and malformed-stream tests**

Cover moved ReadIndex on page 2, capability generation change on page 2, terminal frame omitted, duplicate sequence, byte overrun, malformed page, unavailable capability, and timeout/cancellation. Each must fail closed and must not retry through raw scan.

- [ ] **Step 7: Run GREEN transport verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p adapter-sidecar -- --test-threads=1
cargo test --locked -p shard-client --test embedded_tck -- --test-threads=1
cargo test --locked -p shard-client --test remote_tck -- --test-threads=1
cargo test --locked -p data-node snapshot_graph_page -- --test-threads=1
~~~

Expected: PASS; Local, Embedded, Sidecar, and Remote pages are byte/order equivalent and all fence/generation faults fail closed.

- [ ] **Step 8: Commit**

~~~bash
git add crates/adapter-sidecar crates/shard-client crates/data-node
git commit -m "feat: transport bounded csr pages"
~~~

### Task 9: Bind Fragments to One Snapshot and Integrate the Sole Selector

**Files:**
- Modify: crates/physical-plan/src/lib.rs
- Modify: crates/physical-plan/tests/validate.rs
- Modify: crates/query-optimizer/src/lib.rs
- Modify: crates/query-optimizer/tests/operator_fidelity.rs
- Create: crates/query-executor/src/graph_access.rs
- Create: crates/query-executor/src/graph_profile.rs
- Create: crates/query-executor/src/bounded_adjacency.rs
- Modify: crates/query-executor/src/temporal.rs
- Modify: crates/query-executor/src/context.rs
- Modify: crates/query-executor/src/error.rs
- Modify: crates/query-executor/src/lib.rs
- Modify: crates/query-executor/Cargo.toml
- Create: crates/query-executor/tests/graph_access.rs
- Modify: crates/query-executor/tests/temporal_scan.rs
- Modify: crates/distributed-query/src/lib.rs
- Modify: crates/distributed-query/src/worker.rs
- Modify: crates/distributed-query/src/coordinator.rs
- Modify: crates/distributed-query/tests/fencing.rs
- Modify: crates/distributed-query/tests/local_worker.rs
- Modify: crates/shard-runtime/src/state_machine.rs
- Modify: crates/shard-runtime/src/lib.rs
- Modify: crates/shard-runtime/Cargo.toml
- Create: crates/temporal-storage/src/committed_graph_delta.rs
- Modify: crates/temporal-storage/src/lib.rs

**Interfaces:**
- Consumes: SnapshotCsrCache, SnapshotCsrBuilder, Bounded backend pages, ExecutionContext::check_fences, FragmentRequest required indexes/generations.
- Produces:

~~~rust
pub struct ExpandEstimate {
    pub depth: u16,
    pub estimated_input_rows: u64,
    pub estimated_fanout: u64,
    pub repeated_snapshot_uses: u32,
    pub demanded_properties: Vec<u32>,
}
pub enum GraphQueryShape {
    Point,
    SmallBatchPoint,
    Interval,
    Expand,
}
pub enum PhysicalAccess {
    Generic,
    Primitive {
        primitive: PrimitiveKind,
        guarantee: AccessGuarantee,
        residual: ResidualPolicy,
        constraints: Vec<PhysicalPropertyConstraint>,
    },
    GraphExpand { estimate: ExpandEstimate },
}
pub enum GraphAccessPath {
    PointHistory,
    IntervalHistory,
    BoundedBackend,
    SnapshotCsrHit,
    SnapshotCsrBuild,
}
pub struct GraphCostProfileV1 {
    pub format_version: u16,
    pub revision: String,
    pub backend_family: String,
    pub minimum_csr_depth: u16,
    pub maximum_backend_expansion: u64,
    pub minimum_reuse_count: u32,
    pub maximum_cold_build_ns_per_edge: u64,
    pub overlay_rebuild_edge_ratio_basis_points: u32,
    pub overlay_rebuild_bytes: u64,
    pub overlay_rebuild_slowdown_basis_points: u32,
}
pub struct GraphAccessPathRequest<'a> {
    pub shape: GraphQueryShape,
    pub estimate: Option<&'a ExpandEstimate>,
    pub snapshot: &'a FragmentSnapshotIdentity,
    pub csr_key: Option<&'a SnapshotCsrKey>,
    pub capabilities: QueryPrimitiveCapabilities,
    pub cache_probe: Option<CacheProbe>,
    pub build_estimate_bytes: u64,
    pub query_memory_available: u64,
    pub deadline_remaining_millis: u64,
    pub cancelled: bool,
}
pub struct FragmentSnapshotIdentity {
    pub graph_id: GraphId,
    pub shard_id: u32,
    pub placement_epoch: u64,
    pub applied_log_index: u64,
    pub transaction_time: TransactionTimePredicate,
    pub visibility_transaction_time: TransactionTime,
    pub valid_time: ValidTimePredicate,
    pub schema_version: u64,
    pub mapping_generation: u64,
    pub capability_generation: u64,
}
pub struct GraphAccessPathSelector;
pub enum GraphAccessError {
    Cancelled,
    SnapshotMismatch,
    CapabilityGenerationMismatch,
    CorruptCache(String),
    InvalidOverlay(String),
    UnsupportedBoundedPath,
    InvalidRequest(&'static str),
}
impl GraphAccessPathSelector {
    pub fn new(profile: Option<Arc<GraphCostProfileV1>>) -> Self;
    pub fn select(
        &self,
        request: GraphAccessPathRequest<'_>,
    ) -> Result<GraphAccessPath, GraphAccessError>;
}
pub struct BoundedAdjacencyRequest {
    pub graph_id: GraphId,
    pub shard_id: u32,
    pub source: ElementRef,
    pub direction: CsrDirection,
    pub valid_time: ValidTime,
    pub visibility_transaction_time: TransactionTime,
    pub edge_types: Vec<u32>,
    pub demanded_properties: Vec<u32>,
    pub page_bounds: QueryPageBounds,
}
pub struct ExpandedEdge {
    pub relationship: EdgeRecord,
    pub destination: VertexRecord,
}
pub struct BoundedAdjacencyReader;
impl BoundedAdjacencyReader {
    pub async fn expand(
        &self,
        read: &dyn ReadSnapshot,
        request: &BoundedAdjacencyRequest,
        context: &ExecutionContext,
    ) -> Result<Vec<ExpandedEdge>, TemporalExecutionError>;
}
#[derive(Clone)]
pub struct CommittedApplyBroadcaster;
pub enum CommittedApplyBroadcastError {
    ZeroCapacity,
    Closed,
}
impl CommittedApplyBroadcaster {
    pub fn new(capacity: usize) -> Result<Self, CommittedApplyBroadcastError>;
    pub fn subscribe(
        &self,
    ) -> tokio::sync::broadcast::Receiver<CommittedMutationBatch>;
    pub(crate) fn publish_after_apply(
        &self,
        batch: CommittedMutationBatch,
    ) -> Result<usize, CommittedApplyBroadcastError>;
}
pub struct CommittedGraphDeltaProjector;
impl CommittedGraphDeltaProjector {
    pub async fn project(
        &self,
        read: &dyn ReadSnapshot,
        batch: &CommittedMutationBatch,
        active_identities: &[SnapshotCsrIdentity],
        visibility_transaction_time: TransactionTime,
    ) -> Result<Vec<CommittedGraphDelta>, TemporalStoreError>;
}
pub async fn execute_fragment_in_snapshot(
    &self,
    read: &dyn ReadSnapshot,
    fragment: &PlanFragment,
    context: &ExecutionContext,
    temporal_read: TemporalRead,
    snapshot_identity: FragmentSnapshotIdentity,
) -> Result<Vec<RecordBatch>, TemporalExecutionError>;
~~~

- [ ] **Step 1: Write selector and exact-snapshot RED tests**

Cover point and small-batch PointHistory, non-Expand interval materialization, one-hop bounded backend, interval Expand bounded backend, depth-2 admitted CSR, repeated snapshot CSR, no-profile backend, CSR hit despite cold-build cost, InsufficientMemory backend, deadline backend, cancellation error, unsupported typed primitive error, corruption error, exact mixed CSR/property hydration identity, and stale/ahead index rejection.

- [ ] **Step 2: Run RED tests**

Run:

~~~bash
cargo test --locked -p query-executor --test graph_access -- --test-threads=1
cargo test --locked -p distributed-query --test fencing graph_snapshot -- --test-threads=1
~~~

Expected: FAIL because GraphAccessPathSelector, ExpandEstimate, and exact snapshot execution are missing.

- [ ] **Step 3: Add physical shape metadata**

PhysicalAccess::GraphExpand validates depth >= 1, non-zero estimates, and demanded property IDs sorted/deduplicated. query-optimizer calculates consecutive Expand depth and propagates cardinality/fanout estimates; it does not choose backend versus CSR. CandidateScan and ChangeScan remain Primitive metadata until Task 11.

- [ ] **Step 4: Implement conservative selector behavior**

GraphAccessPathSelector owns Option<Arc<GraphCostProfileV1>>. Point and small-batch point shapes return PointHistory; a non-Expand interval shape returns IntervalHistory. If no valid profile exists, Expand chooses BoundedBackend when snapshot adjacency is supported. SnapshotCsrHit is allowed for an exact healthy hit. SnapshotCsrBuild requires point temporal predicates, profile benefit at the requested depth/fanout/reuse, successful admission estimate, sufficient deadline, and an exact typed page capability set.

Point, SmallBatchPoint, and Interval requests pass estimate == None, csr_key == None, and cache_probe == None. Expand requests pass all three as Some; any other combination returns GraphAccessError::InvalidRequest.

Map CacheProbe::AdmissionRejected and InsufficientMemory to BoundedBackend. Return fail-closed GraphAccessError for key mismatch, applied-index mismatch, invalid Overlay, corruption, generation mismatch, and unsupported bounded backend when CSR is unavailable.

- [ ] **Step 5: Add one caller-owned fragment snapshot**

Extend FragmentRequest with:

~~~rust
required_placement_epochs: BTreeMap<u32, u64>,
required_mapping_generations: BTreeMap<u32, u64>,
~~~

Worker execution follows the existing change-fragment binding pattern but requires read.applied_log_index() == required_applied_index, not merely greater than or equal. Build FragmentSnapshotIdentity from SnapshotToken graph/schema/transaction, shard, exact placement epoch, exact applied index, valid predicate, direction, and mapping generation. Current uses TransactionTimePredicate::Current plus SnapshotToken::transaction_time as visibility_transaction_time; AS OF uses the same transaction time in both fields. Apply the same exact-index rule to interval and change fragments.

- [ ] **Step 6: Implement snapshot-bound backend and CSR Expand**

BoundedAdjacencyReader pages local/cross adjacency inside the supplied snapshot, batch-filters edges with PointHistoryReader, batch-hydrates destination properties inside the same snapshot, and polls context.check_fences between pages/batches.

CSR Expand looks up canonical source ID, merges a valid committed Overlay when serving its exact covered index, routes remote frontier using vertex_owner_shard, and hydrates demanded properties in the same read snapshot. It never calls TemporalStore::expand_out_current, expand_in_current, expand_out_as_of, or expand_in_as_of.

- [ ] **Step 7: Add post-durable-apply Overlay projection**

shard-runtime broadcasts CommittedMutationBatch only after adapter.apply_committed returns the matching ApplyReceipt. committed_graph_delta projects affected edge identities against each active latest SnapshotCsrIdentity using the post-apply ReadSnapshot and batched PointHistoryReader; it emits normalized Add/Remove operations at the receipt index.

Overlay projection/update runs outside the persistence acknowledgement path. A projection, reservation, gap, or update failure calls cache.invalidate_latest immediately and records the error; it never changes the durable receipt.

- [ ] **Step 8: Integrate execution through the selector only**

Every PhysicalOperator::Expand obtains its GraphExpand estimate, derives the exact key, calls GraphAccessPathSelector once, and dispatches to BoundedAdjacencyReader, cache hit, or admitted single-flight build. Remove any new backend-specific branching introduced during integration; Task 11 deletes the pre-existing branches after all replacement tests pass.

- [ ] **Step 9: Run GREEN integration verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p query-optimizer operator_fidelity -- --test-threads=1
cargo test --locked -p query-executor --test graph_access -- --test-threads=1
cargo test --locked -p query-executor --test temporal_scan -- --test-threads=1
cargo test --locked -p distributed-query --test fencing -- --test-threads=1
cargo test --locked -p distributed-query --test local_worker -- --test-threads=1
cargo test --locked -p shard-runtime committed_apply -- --test-threads=1
~~~

Expected: PASS; all Expand execution enters through the selector, mixed reads share exact identity, and stale/ahead/generation/Overlay faults fail closed.

- [ ] **Step 10: Commit**

~~~bash
git add Cargo.lock crates/physical-plan crates/query-optimizer crates/query-executor crates/distributed-query crates/shard-runtime crates/temporal-storage/src/committed_graph_delta.rs crates/temporal-storage/src/lib.rs
git commit -m "feat: integrate snapshot csr access selection"
~~~

### Task 10: Add Bounded Observability and Versioned Benchmark Profiles

**Files:**
- Modify: crates/query-executor/src/context.rs
- Modify: crates/query-executor/src/graph_access.rs
- Modify: crates/query-executor/src/graph_profile.rs
- Modify: crates/query-executor/src/lib.rs
- Modify: crates/query-executor/Cargo.toml
- Modify: crates/query-executor/tests/graph_access.rs
- Modify: crates/paper-benchmark/src/path.rs
- Modify: crates/paper-benchmark/src/artifact.rs
- Modify: crates/paper-benchmark/src/bin/dtgproxy-paper-benchmark.rs
- Modify: crates/paper-benchmark/src/bin/dtgproxy-paper-cell-executor.rs
- Modify: crates/paper-benchmark/tests/artifact_contract.rs
- Create: crates/paper-benchmark/tests/snapshot_csr.rs
- Create: docs/audit/performance/2026-07-28-snapshot-csr-benchmark-spec.json
- Create: docs/audit/performance/2026-07-28-snapshot-csr-profile-v1.json
- Create: scripts/run-snapshot-csr-performance.sh
- Create: scripts/tests/run-snapshot-csr-performance-contract.sh

**Interfaces:**
- Produces:

~~~rust
pub enum SelectedGraphAccessPath {
    PointHistory,
    IntervalHistory,
    BoundedBackend,
    SnapshotCsrHit,
    SnapshotCsrBuild,
}
impl GraphCostProfileV1 {
    pub fn from_json(bytes: &[u8]) -> Result<Self, GraphProfileError>;
    pub fn validate(&self) -> Result<(), GraphProfileError>;
}
pub enum GraphProfileError {
    InvalidJson(String),
    UnsupportedVersion { expected: u16, actual: u16 },
    InvalidRevision,
    InvalidBackendFamily,
    InvalidThreshold(&'static str),
    UnverifiedSeedThreshold,
    ArtifactDigestMismatch,
}
~~~

- [ ] **Step 1: Write metrics/profile RED tests**

Assert profile version != 1, empty revision, unknown backend, zero thresholds, 2%/64 MiB/10% seed values marked unverified, and corrupt JSON fail conservatively. Assert selected path counters, cache hit/miss/reject, build rows/bytes/duration, retained bytes, overlay entries/bytes, backend pages/RPC/wait, snapshot index, and peak query retained bytes appear in QueryExecutionMetricsSnapshot.

- [ ] **Step 2: Run RED tests**

Run:

~~~bash
cargo test --locked -p query-executor --test graph_access metrics_and_profile -- --test-threads=1
cargo test --locked -p paper-benchmark snapshot_csr -- --test-threads=1
~~~

Expected: FAIL because GraphCostProfileV1::from_json, strict validation, and the Snapshot CSR metrics are missing.

- [ ] **Step 3: Extend bounded metrics**

Add AtomicU64 counters/histogram buckets without string labels for:

~~~text
selected_access_path
snapshot_applied_log_index
history_records_scanned
history_bytes_scanned
history_payloads_decoded
history_payload_bytes_copied
csr_cache_hit
csr_cache_miss
csr_admission_reject
csr_build_rows
csr_build_bytes
csr_build_duration_micros
csr_retained_bytes
overlay_entries
overlay_bytes
backend_pages
backend_rpc
backend_wait_micros
peak_query_retained_bytes
~~~

Use five fixed path counters rather than a free-form label. Snapshot index is a last-observed numeric gauge, not a label.

- [ ] **Step 4: Implement strict profile loading**

Add serde and serde_json dependencies. deny_unknown_fields, require format_version == 1, a non-empty immutable revision, one of rocksdb/postgresql/neo4j backend families, non-zero thresholds, and overlay values derived from evidence. Loader failure leaves selector.profile == None and therefore selects bounded backend.

- [ ] **Step 5: Extend the reproducible benchmark harness**

Add traversal workload parameters for graph size, degree distribution, depth 1/2/3/5, cold/hot CSR, Overlay 0/1/2/5%, outgoing/incoming/both, current/repeated historical, single/concurrent, fixed seed, fixed lifecycle, warmup, repetitions, and statistics. Record path identity, p50/p95, throughput, build cost, reservation, actual retained bytes, and Overlay slowdown. Add paper-benchmark subcommands generate-snapshot-csr-profile and verify-snapshot-csr-profile; generation requires three verified backend artifact directories and verification requires their digests to match the profile.

The three design seed values are emitted as candidate rows; the profile generator selects checked-in thresholds only from verified three-backend artifacts.

run-snapshot-csr-performance.sh accepts the benchmark spec and one output root, verifies a clean revision identity, runs RocksDB/PostgreSQL/Neo4j with the same spec, and writes the three artifact directories snapshot-csr-rocksdb, snapshot-csr-postgresql, and snapshot-csr-neo4j. Its contract test uses fixture executors only to verify command construction and refuses to mark fixture output as measured evidence.

- [ ] **Step 6: Produce and validate profile-v1**

Run the benchmark's validation/generation commands documented in the JSON metadata. The checked-in file contains revision, dirty-worktree digest, dataset digest, backend artifact digests, warmup, repetitions, statistics method, and selected thresholds. Simulated runs may test the schema but cannot populate production defaults.

- [ ] **Step 7: Run GREEN observability/profile verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p query-executor --test graph_access -- --test-threads=1
cargo test --locked -p paper-benchmark snapshot_csr -- --test-threads=1
bash scripts/tests/run-snapshot-csr-performance-contract.sh
cargo run --locked -p paper-benchmark --bin dtgproxy-paper-benchmark -- verify-snapshot-csr-profile --profile docs/audit/performance/2026-07-28-snapshot-csr-profile-v1.json --rocksdb .superpowers/sdd/snapshot-csr-rocksdb --postgresql .superpowers/sdd/snapshot-csr-postgresql --neo4j .superpowers/sdd/snapshot-csr-neo4j
~~~

Expected: all commands exit 0; corrupt/missing profiles choose bounded backend and verified metrics contain no high-cardinality labels.

- [ ] **Step 8: Commit**

~~~bash
git add Cargo.lock crates/query-executor crates/paper-benchmark docs/audit/performance/2026-07-28-snapshot-csr-benchmark-spec.json docs/audit/performance/2026-07-28-snapshot-csr-profile-v1.json scripts/run-snapshot-csr-performance.sh scripts/tests/run-snapshot-csr-performance-contract.sh
git commit -m "perf: add snapshot csr selector profile"
~~~

### Task 11: Delete Old Expand and Unbounded Query Paths

**Files:**
- Modify: crates/physical-plan/src/lib.rs
- Modify: crates/query-optimizer/src/lib.rs
- Modify: crates/query-optimizer/tests/operator_fidelity.rs
- Modify: crates/query-executor/src/temporal.rs
- Modify: crates/temporal-storage/src/store.rs
- Modify: crates/temporal-storage/tests/transaction.rs
- Modify: crates/temporal-storage/tests/historical_expansion.rs
- Modify: crates/temporal-storage/tests/edge_roundtrip_memory.rs
- Modify: crates/temporal-storage/tests/rocksdb_roundtrip.rs
- Modify: crates/replica-snapshot/tests/temporal_equivalence.rs
- Modify: crates/dtgproxy/tests/distributed_transactions.rs
- Modify: crates/adapter-postgres/src/lib.rs
- Modify: crates/temporal-storage/benches/roundtrip.rs
- Create: scripts/check-snapshot-csr-clean-break.sh
- Create: scripts/tests/check-snapshot-csr-clean-break.sh

**Interfaces:**
- Consumes: GraphExpand + GraphAccessPathSelector, BoundedAdjacencyReader, Snapshot CSR paths.
- Produces: no compatibility API. The old four TemporalStore Expand entry points and PrimitiveKind::AdjacencyExpand are absent from production code.

- [ ] **Step 1: Add a failing clean-break source gate**

The script exits non-zero when production source contains:

~~~text
PrimitiveKind::AdjacencyExpand
fn expand_out_current
fn expand_in_current
fn expand_out_as_of
fn expand_in_as_of
.expand_out_current
.expand_in_current
.expand_out_as_of
.expand_in_as_of
load_all_entries
legacy snapshot csr
snapshot csr compat
snapshot csr feature flag
dual read
fallback old expand
~~~

Allow only explicit test-oracle module names and historical report files; do not allow production call sites through path exclusions.

- [ ] **Step 2: Run the RED gate**

Run:

~~~bash
bash scripts/check-snapshot-csr-clean-break.sh
~~~

Expected: FAIL and list current PrimitiveKind::AdjacencyExpand, TemporalStore Expand methods/callers, PostgreSQL load_all_entries, and the old roundtrip Expand benchmark cells.

- [ ] **Step 3: Remove old physical selection metadata**

Delete PrimitiveKind::AdjacencyExpand and its validation/optimizer mapping. Physical Expand must carry GraphExpand estimates and execute only through GraphAccessPathSelector.

- [ ] **Step 4: Remove old executor/store Expand production methods**

Delete combined_expand_edges and direct calls to the four TemporalStore Expand methods. Delete the four public store methods after migrating tests to BoundedAdjacencyReader or a cfg(test) independent oracle that does not compile into production.

Migrate replica-snapshot and distributed transaction equivalence tests to the new snapshot-bound reader so they continue testing canonical persistence equivalence.

- [ ] **Step 5: Remove PostgreSQL full-materialization query behavior**

Replace raw multi_get/scan implementations used by production reads with bounded native point/range SQL. Rename any logical-export-only whole-instance helper to export_snapshot_entries and make it callable only from logical snapshot export. Delete load_all_entries entirely.

- [ ] **Step 6: Remove old benchmark and compatibility entry points**

Delete expand_out_current_partition_32_edges, expand_out_as_of_partition_32_edges, and any old feature/compat selection code. Retain immutable historical result artifacts, canonical encodings, backend indexes, and independent correctness oracles.

- [ ] **Step 7: Run GREEN clean-break and regression gates**

Run:

~~~bash
bash scripts/tests/check-snapshot-csr-clean-break.sh
bash scripts/check-snapshot-csr-clean-break.sh
cargo fmt --all -- --check
cargo clippy --locked -p physical-plan -p query-optimizer -p query-executor -p temporal-storage -p adapter-postgres --all-targets -- -D warnings
cargo test --locked -p query-optimizer -- --test-threads=1
cargo test --locked -p query-executor -- --test-threads=1
cargo test --locked -p temporal-storage --all-features -- --test-threads=1
~~~

Expected: all commands exit 0 and source scan finds no old symbol, production call, compatibility branch, dual read, or old-path fallback.

- [ ] **Step 8: Commit**

~~~bash
git add crates/physical-plan crates/query-optimizer crates/query-executor crates/temporal-storage crates/replica-snapshot/tests/temporal_equivalence.rs crates/dtgproxy/tests/distributed_transactions.rs crates/adapter-postgres/src/lib.rs scripts/check-snapshot-csr-clean-break.sh scripts/tests/check-snapshot-csr-clean-break.sh
git commit -m "refactor: remove legacy expand paths"
~~~

### Task 12: Run Final Correctness, Resource, Backend, and Performance Gates

**Files:**
- Create: docs/audit/performance/2026-07-28-snapshot-csr-clean-break.md
- Modify: docs/audit/performance/2026-07-28-snapshot-csr-profile-v1.json
- Modify: scripts/check-snapshot-csr-clean-break.sh
- Modify: crates/snapshot-csr/tests/builder.rs
- Modify: crates/snapshot-csr/tests/committed_overlay.rs
- Modify: crates/query-executor/tests/graph_access.rs
- Modify: crates/distributed-query/tests/performance_regressions.rs
- Modify: crates/adapter-rocksdb/tests/adapter_contract.rs
- Modify: crates/adapter-postgres/tests/live_postgres.rs
- Modify: crates/adapter-neo4j/tests/live_neo4j.rs
- Modify: crates/paper-benchmark/tests/snapshot_csr.rs
- Modify: docs/audit/performance/2026-07-28-snapshot-csr-benchmark-spec.json
- Modify: scripts/run-snapshot-csr-performance.sh

**Interfaces:**
- Consumes: Tasks 0-11 evidence, fixed-seed TCK, verified profile, clean-break script.
- Produces: a revision-bound report with exact method, raw artifacts, acceptance calculations, failures, bottlenecks, and conclusion.

- [ ] **Step 1: Create the report structure before final runs**

Use these exact sections:

~~~markdown
# Snapshot CSR Clean-Break Performance and Correctness Report

## Revision and dirty-worktree digest
## Environment and backend versions
## Dataset, fixed seed, and graph distributions
## Commands and lifecycle
## Correctness matrix
## Resource and corruption matrix
## Backend page and transport equivalence
## Selector and profile evidence
## Traversal raw runs
## Throughput and latency comparison
## Reservation versus actual retained memory
## Overlay rebuild thresholds
## Clean-break deletion evidence
## PointHistory prerequisite evidence
## Remaining bottlenecks
## Conclusion
~~~

Do not claim a gate before its raw artifact is linked and its calculation is shown.

- [ ] **Step 2: Run the fixed-seed correctness matrix**

Cover depth 1/2/3/5, outgoing/incoming/both, cycles, self-loops, parallel edges, cross-shard edges, current, historical, currently deleted/historically visible, future-created exclusion, and Base+Overlay at every covered index. Compare bounded backend and CSR results with the Memory oracle and run the identical dataset on RocksDB, PostgreSQL, and Neo4j.

Expected: identical canonical row digests and exact applied indexes for every cell.

- [ ] **Step 3: Run the resource/failure matrix**

Run below/equal/above boundaries for page items/bytes, build candidates/vertices/bytes, published cache, Overlay entries/bytes, cancellation, deadline, continuation corruption, offset corruption, local-ID corruption, snapshot mismatch, generation change, rebuild failure, and restart-empty-cache.

Expected: all faults are classified, correctness faults fail closed, optional admission faults use bounded backend, and usage counters return to their expected retained baseline.

- [ ] **Step 4: Run live backend and transport gates**

Run:

~~~bash
cargo test --locked -p adapter-rocksdb -- --test-threads=1
bash scripts/test-postgres-live.sh
bash scripts/test-neo4j-live.sh
cargo test --locked -p adapter-sidecar -- --test-threads=1
cargo test --locked -p shard-client --test embedded_tck -- --test-threads=1
cargo test --locked -p shard-client --test remote_tck -- --test-threads=1
~~~

Expected: all commands exit 0. Exit 78 is recorded as unavailable and blocks the completion conclusion.

- [ ] **Step 5: Run the traversal performance matrix**

Run Memory oracle, RocksDB, PostgreSQL, and Neo4j under identical graph scale, degree distribution, depth 1/2/3/5, cold/hot CSR, Overlay 0/1/2/5%, direction, current/repeated historical, single/concurrent, backend lifecycle, CPU concurrency, seed, warmup, repetitions, and statistics.

Run:

~~~bash
bash scripts/run-snapshot-csr-performance.sh docs/audit/performance/2026-07-28-snapshot-csr-benchmark-spec.json .superpowers/sdd
cargo run --locked -p paper-benchmark --bin dtgproxy-paper-benchmark -- generate-snapshot-csr-profile --revision "$(git rev-parse HEAD)" --rocksdb .superpowers/sdd/snapshot-csr-rocksdb --postgresql .superpowers/sdd/snapshot-csr-postgresql --neo4j .superpowers/sdd/snapshot-csr-neo4j --output docs/audit/performance/2026-07-28-snapshot-csr-profile-v1.json
~~~

Expected: both commands exit 0, all three artifacts verify, and the generator refuses unavailable, simulated, revision-mismatched, or non-comparable inputs.

Calculate:

~~~text
throughput_ratio = hot_csr_ops_per_second / bounded_backend_ops_per_second
p95_reduction = (backend_p95_ns - hot_csr_p95_ns) / backend_p95_ns
reservation_ratio = actual_retained_bytes / reserved_bytes
cold_regression = (cold_selected_p95_ns - bounded_backend_p95_ns) / bounded_backend_p95_ns
overlay_loss = (base_csr_ops_per_second - overlay_ops_per_second) / base_csr_ops_per_second
~~~

Require throughput_ratio >= 2.0 and p95_reduction >= 0.30 for representative depth 2+ cells, reservation_ratio <= 1.15, cold_regression <= 0.10 for low-reuse cells, and overlay_loss <= 0.10 before rebuild.

- [ ] **Step 6: Verify the PointHistory prerequisite**

Check docs/audit/performance/2026-07-28-point-history-clean-break.md contains real post-change runs, live PostgreSQL and Neo4j continuation evidence, allocation/copy bytes, peak retained bytes, replay-depth acceptance calculations, and no simulated data presented as backend evidence.

Expected: the check passes. If it does not, Snapshot CSR code may remain complete but the combined clean-break conclusion must remain not achieved.

- [ ] **Step 7: Run the full clean-break verification**

Run:

~~~bash
bash scripts/check-point-history-clean-break.sh
bash scripts/check-snapshot-csr-clean-break.sh
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features -- --test-threads=1
~~~

Expected: all commands exit 0. The source gate confirms no old history reconstruction, old Expand selection, compat/feature, dual read, old-path fallback, or PostgreSQL full-materialization query call remains.

- [ ] **Step 8: Finalize evidence and profile**

Populate every report table from raw revision-bound artifacts. If a performance gate misses, optimize the new builder/layout/cache/Overlay or adjust only a threshold supported by the three-backend evidence; never restore a deleted path. Record unresolved bottlenecks explicitly.

- [ ] **Step 9: Commit final verification artifacts**

~~~bash
git add docs/audit/performance/2026-07-28-snapshot-csr-clean-break.md docs/audit/performance/2026-07-28-snapshot-csr-benchmark-spec.json docs/audit/performance/2026-07-28-snapshot-csr-profile-v1.json scripts/check-snapshot-csr-clean-break.sh scripts/run-snapshot-csr-performance.sh crates/snapshot-csr/tests crates/query-executor/tests crates/distributed-query/tests/performance_regressions.rs crates/adapter-rocksdb/tests/adapter_contract.rs crates/adapter-postgres/tests/live_postgres.rs crates/adapter-neo4j/tests/live_neo4j.rs crates/paper-benchmark
git commit -m "perf: verify snapshot csr clean break"
~~~

## Final Definition of Done

- All production point and interval history reads satisfy the approved PointHistory clean-break plan.
- Every production Expand is selected by GraphAccessPathSelector as bounded backend adjacency or exact Snapshot CSR.
- Every fragment and mixed path uses one exact ReadSnapshot, applied_log_index, placement epoch, schema version, mapping generation, and capability generation.
- Local, Embedded, Sidecar, and Remote bounded pages are equivalent; RocksDB, PostgreSQL, and Neo4j fixed-seed TCK passes.
- Overlay is contiguous, durable-post-apply, bounded, and invalidates immediately on any correctness fault.
- Old history reconstruction, old Expand selection/execution, PostgreSQL full-materialization query behavior, compatibility code, old feature paths, and old benchmark entry points are deleted.
- Resource, corruption, cancellation, deadline, rebuild, and restart-empty-cache tests pass without reservation leaks.
- Real revision-bound data satisfies every traversal acceptance threshold and the report distinguishes unavailable evidence from passing evidence.
