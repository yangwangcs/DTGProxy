# DTGProxy Snapshot CSR Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the independent Snapshot CSR core: complete snapshot identity, deterministic directional CSR arrays, leak-free memory reservation, snapshot-bound graph pages, immutable current/historical builders, a durable committed Delta Overlay, and an atomic bounded cache.

**Architecture:** A new snapshot-csr crate owns only graph execution-cache concerns and depends on stable storage/time/graph contracts, never parser, T-Cypher AST, Bolt, query-executor, or a concrete adapter. storage-api supplies bounded snapshot pages; temporal-storage supplies canonical graph-key decoding and PointHistoryReader; the builder returns an unpublished immutable image that the cache publishes only after a second published-memory reservation succeeds.

**Tech Stack:** Rust 1.93, edition 2024, blake3 1.8.5, tokio 1.53.0 synchronization, storage-api::ReadSnapshot, temporal-storage canonical graph keys and PointHistoryReader, adapter-memory oracle tests.

## Global Constraints

- Current + History Anchor/Delta + Adjacency + applied_log_index remains the only persistent authority. Snapshot CSR is a discardable execution cache and must be reconstructible after restart or eviction.
- The migration is a clean break. Do not add feature flags, legacy aliases, dual reads, dual writes, compatibility modules, or runtime fallback to an old CSR/Expand implementation.
- SnapshotCsrKey equality must include graph_id, shard_id, placement_epoch, applied_log_index, transaction-time predicate, valid-time predicate, direction, schema_version, and mapping_generation.
- A point snapshot CSR contains only edges visible at its exact temporal predicates. An interval predicate requires a separate versioned-edge columnar table; it must never be represented by point CSR arrays.
- Store each canonical u128 vertex ID once in the dictionary, use u32 local vertex IDs in adjacency arrays, retain vertex owner shard, edge owner shard, and placement epoch, and keep outgoing and incoming arrays independent.
- Select u32 offsets only when the pre-build adjacency bound is less than u32::MAX; otherwise select u64 before allocation. Never switch width during a build.
- Compute an exact or conservative reservation before every dense allocation. InsufficientMemory is a classified optional-path outcome; partially built arrays and process OOM are not acceptable outcomes.
- Build from one caller-supplied ReadSnapshot and require every page applied_log_index to equal ReadSnapshot::applied_log_index().
- Historical CSR candidates must come from the canonical edge-identity keyspace and batched PointHistoryReader filtering. Never infer the historical edge universe from current adjacency.
- A failed, cancelled, expired, epoch-mismatched, mapping-mismatched, or corrupt build must release workspace reservations and must never become cache-visible.
- Overlay changes must be durable, canonical, committed, exactly contiguous from Base index I through I + 1..=J, and identity-matched. Gap, reorder, decode failure, epoch change, schema change, mapping-generation change, or capacity rejection invalidates the rolling entry immediately.
- The 2% edge ratio, 64 MiB retained bytes, and 10% traversal slowdown values are benchmark seed values, not production defaults. Production callers must provide a validated profile.
- Initially retain at most one preferred latest entry and one hot historical entry per shard. Historical entries use LRU; active Arc references remain valid after eviction.
- Keep build-workspace, published-CSR, Overlay, node-total, and query-admission accounting separate so a rebuild cannot hide simultaneous old image, new workspace, new image, Overlay, and active-query memory.
- Keep #![forbid(unsafe_code)] and use checked arithmetic for lengths, offsets, byte estimates, and continuation advancement.
- Preserve unrelated dirty-worktree changes and stage only files named by the current task.

---

## File Structure

- Modify Cargo.toml: add crates/snapshot-csr to workspace members.
- Create crates/snapshot-csr/Cargo.toml: narrow dependencies on blake3, storage-api, temporal-storage, temporal-types, and tokio.
- Create crates/snapshot-csr/src/lib.rs: public exports only.
- Create crates/snapshot-csr/src/error.rs: one classified error contract shared by layout, budgets, pages, builder, Overlay, and cache.
- Create crates/snapshot-csr/src/identity.rs: temporal predicates, direction, SnapshotCsrKey, and cache identity.
- Create crates/snapshot-csr/src/csr.rs: dense dictionary, edge columns, offset variants, immutable traversal, validation, and fingerprint.
- Create crates/snapshot-csr/src/budget.rs: multi-ledger byte accounting, RAII reservations, and size estimators.
- Create crates/snapshot-csr/src/page_source.rs: exact applied-index checks around snapshot graph pages.
- Create crates/snapshot-csr/src/builder.rs: current/historical immutable build pipeline and cancellation fence.
- Create crates/snapshot-csr/src/committed_overlay.rs: durable committed adjacency delta type, continuity checks, merge traversal, invalidation, and rebuild signals.
- Create crates/snapshot-csr/src/cache.rs: latest/historical policy, LRU, admission, single flight, atomic publish, and active-reference-safe eviction.
- Create crates/snapshot-csr/tests/core_layout.rs: identity/layout/round-trip/fingerprint tests.
- Create crates/snapshot-csr/tests/budget.rs: below/equal/above, injected failure, and retained-byte estimator tests.
- Create crates/snapshot-csr/tests/builder.rs: current/historical oracle, historical delete, cancellation, corruption, and snapshot-index tests.
- Create crates/snapshot-csr/tests/committed_overlay.rs: Base+Overlay equivalence, continuity, identity, capacity, and rebuild-signal tests.
- Create crates/snapshot-csr/tests/cache.rs: miss, single flight, cancellation, LRU, active reference, rebuild peak, failed publish, and restart-empty tests.
- Modify crates/storage-api/src/query.rs: typed edge-identity page and capability contract.
- Modify crates/storage-api/src/lib.rs: ReadSnapshot graph-page methods and forwarding implementations.
- Modify crates/storage-api/tests/query_primitives.rs: page validation and unsupported-capability tests.
- Modify crates/temporal-storage/src/key.rs: whole-partition local/cross adjacency prefixes.
- Modify crates/temporal-storage/src/lib.rs: export the new prefix helpers.
- Modify crates/temporal-storage/src/observed_adapter.rs: forward and observe the new snapshot methods.
- Modify crates/adapter-memory/src/lib.rs: Memory ReadSnapshot oracle implementation.
- Modify crates/adapter-memory/tests/adapter_contract.rs: bounded page and applied-index contract tests.

### Task 1: Add the Pure Snapshot CSR Identity and Immutable Layout

**Files:**
- Modify: Cargo.toml
- Create: crates/snapshot-csr/Cargo.toml
- Create: crates/snapshot-csr/src/lib.rs
- Create: crates/snapshot-csr/src/error.rs
- Create: crates/snapshot-csr/src/identity.rs
- Create: crates/snapshot-csr/src/csr.rs
- Create: crates/snapshot-csr/tests/core_layout.rs

**Interfaces:**
- Consumes: temporal_storage::{ElementId, GraphId, PartitionId}; temporal_types::{Interval, TransactionTime, ValidTime}.
- Produces:

~~~rust
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CsrDirection { Outgoing, Incoming }
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TransactionTimePredicate {
    Current,
    AsOf(TransactionTime),
    Interval(Interval<TransactionTime>),
}
impl Ord for TransactionTimePredicate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering;
}
impl PartialOrd for TransactionTimePredicate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering>;
}
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ValidTimePredicate {
    Point(ValidTime),
    Interval(Interval<ValidTime>),
}
impl Ord for ValidTimePredicate {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering;
}
impl PartialOrd for ValidTimePredicate {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering>;
}
pub struct SnapshotCsrKey;
impl SnapshotCsrKey {
    pub fn new(
        graph_id: GraphId,
        shard_id: u32,
        placement_epoch: u64,
        applied_log_index: u64,
        transaction_time: TransactionTimePredicate,
        valid_time: ValidTimePredicate,
        direction: CsrDirection,
        schema_version: u64,
        mapping_generation: u64,
    ) -> Result<Self, SnapshotCsrError>;
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OffsetWidth { U32, U64 }
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryCategory {
    NodeTotal,
    QueryAdmission,
    BuildWorkspace,
    PublishedCsr,
    Overlay,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CacheInvalidationReason {
    OverlayGap,
    OverlayIdentityMismatch,
    OverlayCapacity,
    Corruption,
    SnapshotFenceChanged,
}
#[derive(Debug)]
pub enum SnapshotCsrError {
    InvalidSnapshotKey(&'static str),
    VersionedEdgesRequired,
    DuplicateVertexOwner { vertex_id: u128, first_owner: u32, second_owner: u32 },
    DuplicateEdgeId { edge_id: u128 },
    NonLocalAnchor { edge_id: u128, expected_shard: u32, actual_shard: u32 },
    InvalidOffsets,
    InvalidLocalVertexId { local_id: u64, vertex_count: u64 },
    InvalidEdgeRef { edge_ref: u64, edge_count: u64 },
    LocalIdSpaceExceeded { vertices: u64 },
    EdgeRefSpaceExceeded { edges: u64 },
    SizeOverflow,
    InvalidMemoryReservation,
    InsufficientMemory { category: MemoryCategory, requested: u64, available: u64 },
    AppliedIndexMismatch { expected: u64, actual: u64 },
    UnsupportedSnapshotPrimitive(&'static str),
    MalformedContinuation,
    CandidateLimitExceeded { limit: u64, actual: u64 },
    CorruptGraphEntry(String),
    FenceChanged(&'static str),
    Cancelled,
    DeadlineExceeded,
    InvalidOverlay(&'static str),
    NonContiguousOverlay { expected: u64, actual: u64 },
    CacheBuildFailed(String),
}
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SnapshotCsrIdentity {
    graph_id: GraphId,
    shard_id: u32,
    placement_epoch: u64,
    transaction_time: TransactionTimePredicate,
    valid_time: ValidTimePredicate,
    direction: CsrDirection,
    schema_version: u64,
    mapping_generation: u64,
}
impl SnapshotCsrIdentity {
    pub const fn graph_id(&self) -> GraphId;
    pub const fn shard_id(&self) -> u32;
    pub const fn placement_epoch(&self) -> u64;
    pub const fn transaction_time(&self) -> TransactionTimePredicate;
    pub const fn valid_time(&self) -> ValidTimePredicate;
    pub const fn direction(&self) -> CsrDirection;
    pub const fn schema_version(&self) -> u64;
    pub const fn mapping_generation(&self) -> u64;
}
pub struct CsrVertex { pub id: ElementId, pub owner_shard: u32 }
pub struct CsrEdge {
    pub id: ElementId,
    pub owner_shard: u32,
    pub edge_type: u32,
    pub source: CsrVertex,
    pub destination: CsrVertex,
}
pub struct SnapshotCsr;
pub struct CsrNeighbors<'a> {
    csr: &'a SnapshotCsr,
    next: usize,
    end: usize,
}
impl Iterator for CsrNeighbors<'_> {
    type Item = CsrNeighbor;
    fn next(&mut self) -> Option<Self::Item>;
}
impl SnapshotCsr {
    pub fn from_edges(
        key: SnapshotCsrKey,
        edges: impl IntoIterator<Item = CsrEdge>,
    ) -> Result<Self, SnapshotCsrError>;
    pub fn key(&self) -> &SnapshotCsrKey;
    pub fn fingerprint(&self) -> [u8; 32];
    pub fn offset_width(&self) -> OffsetWidth;
    pub fn retained_bytes(&self) -> u64;
    pub fn neighbors(&self, canonical_vertex: ElementId)
        -> Result<CsrNeighbors<'_>, SnapshotCsrError>;
}
~~~

- Invariant: SnapshotCsr::from_edges accepts only ValidTimePredicate::Point and a point/current transaction predicate; interval predicates return SnapshotCsrError::VersionedEdgesRequired.
- Invariant: the edge column is sorted by canonical edge ID, edge_refs point into that column, and adjacency rows are sorted by local anchor, remote local ID, edge type, and edge ID.

- [ ] **Step 1: Add the crate scaffold and failing public-contract tests**

Add the workspace member, a manifest with the exact package metadata below, module declarations in lib.rs, and tests that import the not-yet-defined public types:

~~~toml
[package]
name = "snapshot-csr"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
blake3 = "=1.8.5"
storage-api = { path = "../storage-api" }
temporal-storage = { path = "../temporal-storage" }
temporal-types = { path = "../temporal-types" }
tokio = { version = "=1.53.0", features = ["sync"] }

[dev-dependencies]
adapter-memory = { path = "../adapter-memory" }

[lints]
workspace = true
~~~

The first test must construct two keys differing only in mapping_generation and assert inequality; a second must construct an interval key and assert from_edges returns VersionedEdgesRequired.

- [ ] **Step 2: Run the RED test**

Run:

~~~bash
cargo test --locked -p snapshot-csr --test core_layout -- --test-threads=1
~~~

Expected: FAIL with unresolved imports for SnapshotCsrKey, SnapshotCsr, or CsrEdge. It must not fail because another workspace package is broken.

- [ ] **Step 3: Implement complete key validation**

Implement the public predicates and SnapshotCsrKey with Clone, Debug, Eq, Hash, Ord, PartialEq, and PartialOrd. Because temporal_types::Interval is not Ord, implement predicate ordering explicitly with fixed variant tags followed by start/end comparison; do not order by debug strings. SnapshotCsrKey::new rejects placement_epoch == 0, schema_version == 0, and mapping_generation == 0. Preserve every u32 shard_id and applied_log_index == 0 so an empty Memory snapshot can be represented.

Add exact getters for every field:

~~~rust
pub const fn graph_id(&self) -> GraphId;
pub const fn shard_id(&self) -> u32;
pub const fn placement_epoch(&self) -> u64;
pub const fn applied_log_index(&self) -> u64;
pub const fn transaction_time(&self) -> TransactionTimePredicate;
pub const fn valid_time(&self) -> ValidTimePredicate;
pub const fn direction(&self) -> CsrDirection;
pub const fn schema_version(&self) -> u64;
pub const fn mapping_generation(&self) -> u64;
pub const fn is_latest(&self) -> bool;
pub fn cache_identity(&self) -> SnapshotCsrIdentity;
~~~

SnapshotCsrIdentity contains every SnapshotCsrKey field except applied_log_index and is used only to validate Overlay continuity across committed indexes.

Implement Display and std::error::Error for SnapshotCsrError. Later tasks reuse and extend behavior through the variants already declared here; they must not introduce stringly typed fallback errors for classified conditions.

- [ ] **Step 4: Implement deterministic dense arrays**

Use these private representations:

~~~rust
enum Offsets {
    U32(Vec<u32>),
    U64(Vec<u64>),
}

pub struct SnapshotCsr {
    key: SnapshotCsrKey,
    vertex_ids: Vec<u128>,
    vertex_owners: Vec<u32>,
    offsets: Offsets,
    neighbors: Vec<u32>,
    edge_refs: Vec<u32>,
    edge_ids: Vec<u128>,
    edge_owners: Vec<u32>,
    edge_types: Vec<u32>,
    fingerprint: [u8; 32],
}
~~~

Build the vertex dictionary from both endpoints, sort and deduplicate by canonical u128 ID, and reject the same canonical vertex ID paired with two owners. Reject more than u32::MAX dictionary vertices or distinct edge-column rows because local IDs and edge_refs are u32. Reject duplicate edge IDs, reject an edge whose anchor endpoint owner differs from key.shard_id, allow self-loops and parallel edges, and retain the remote endpoint owner. For outgoing arrays the anchor is source; for incoming arrays the anchor is destination.

- [ ] **Step 5: Implement validation, offset-width selection, traversal, and fingerprint**

Add:

~~~rust
impl OffsetWidth {
    pub const fn for_adjacency_bound(bound: u64) -> Self {
        if bound < u64::from(u32::MAX) { Self::U32 } else { Self::U64 }
    }
}

impl SnapshotCsr {
    pub fn validate(&self) -> Result<(), SnapshotCsrError>;
}
~~~

validate must enforce offsets length == vertex_count + 1, first offset == 0, monotonic offsets, final offset == neighbors.len(), neighbors.len() == edge_refs.len(), every local vertex ID is in range, every edge ref is in range, and all edge columns have equal lengths. Fingerprint with domain DTGProxy/SnapshotCsr/V1 and every key/array byte in canonical big-endian order.

CsrNeighbors yields:

~~~rust
pub struct CsrNeighbor {
    pub vertex_id: ElementId,
    pub vertex_owner_shard: u32,
    pub edge_id: ElementId,
    pub edge_owner_shard: u32,
    pub edge_type: u32,
}
~~~

- [ ] **Step 6: Add full layout tests**

Cover deterministic input permutations, empty graph, duplicate vertex owner, duplicate edge ID, corrupt/non-monotone/final offsets through a test-only constructor, invalid local ID, u32/u64 choice at u32::MAX - 1 and u32::MAX, outgoing-only versus incoming-only allocation, self-loop, parallel edges, cross-shard edge, canonical ID/owner round trip, and stable fingerprint.

- [ ] **Step 7: Run GREEN verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p snapshot-csr --test core_layout -- --test-threads=1
cargo clippy --locked -p snapshot-csr --all-targets -- -D warnings
~~~

Expected: all commands exit 0. The tests assert that an outgoing build does not allocate an incoming array and vice versa.

- [ ] **Step 8: Commit**

~~~bash
git add Cargo.toml Cargo.lock crates/snapshot-csr
git commit -m "feat: add snapshot csr core layout"
~~~

### Task 2: Add RAII Reservation and Retained-Byte Accounting

**Files:**
- Create: crates/snapshot-csr/src/budget.rs
- Modify: crates/snapshot-csr/src/lib.rs
- Modify: crates/snapshot-csr/src/csr.rs
- Create: crates/snapshot-csr/tests/budget.rs

**Interfaces:**
- Consumes: SnapshotCsr::retained_bytes and OffsetWidth.
- Produces:

~~~rust
pub struct CsrMemoryLimits {
    pub node_bytes: u64,
    pub query_bytes: u64,
    pub build_workspace_bytes: u64,
    pub published_bytes: u64,
    pub overlay_bytes: u64,
}
#[derive(Clone)]
pub struct CsrMemoryAccount;
pub struct MemoryReservation;
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CsrMemoryUsage {
    pub node_bytes: u64,
    pub query_bytes: u64,
    pub build_workspace_bytes: u64,
    pub published_bytes: u64,
    pub overlay_bytes: u64,
}
pub struct CsrSizeEstimate;
impl CsrMemoryAccount {
    pub fn new(limits: CsrMemoryLimits) -> Result<Self, SnapshotCsrError>;
    pub fn reserve_query(&self, bytes: u64) -> Result<MemoryReservation, SnapshotCsrError>;
    pub fn reserve_build(&self, bytes: u64) -> Result<MemoryReservation, SnapshotCsrError>;
    pub fn reserve_published(&self, bytes: u64) -> Result<MemoryReservation, SnapshotCsrError>;
    pub fn reserve_overlay(&self, bytes: u64) -> Result<MemoryReservation, SnapshotCsrError>;
    pub fn usage(&self) -> CsrMemoryUsage;
}
impl CsrSizeEstimate {
    pub fn for_counts(
        vertex_count: u64,
        adjacency_count: u64,
        edge_count: u64,
        width: OffsetWidth,
    ) -> Result<Self, SnapshotCsrError>;
    pub const fn bytes(self) -> u64;
}
~~~

- [ ] **Step 1: Write failing boundary and Drop tests**

Create an account with node/query/build/published/overlay limits of 100 bytes. Assert a 100-byte build reservation succeeds, a second byte fails with InsufficientMemory { category: BuildWorkspace, requested: 1, available: 0 }, and dropping the first reservation returns all usage counters to zero. Add a panic/unwind test using catch_unwind and an injected validation error test.

- [ ] **Step 2: Run the RED test**

Run:

~~~bash
cargo test --locked -p snapshot-csr --test budget -- --test-threads=1
~~~

Expected: FAIL because CsrMemoryAccount and MemoryReservation do not exist.

- [ ] **Step 3: Implement one atomic multi-ledger reservation**

Use one std::sync::Mutex around CsrMemoryUsage so each reservation checks and increments all affected ledgers atomically:

~~~text
reserve_query(bytes): query += bytes
reserve_build(bytes): node += bytes, query += bytes, build_workspace += bytes
reserve_published(bytes): node += bytes, published += bytes
reserve_overlay(bytes): node += bytes, overlay += bytes
~~~

MemoryReservation owns Arc<MemoryAccountInner> and its exact charge. Drop subtracts the charge once. Add MemoryReservation::bytes(), MemoryReservation::category(), and MemoryReservation::release(self). Zero-byte reservations return SnapshotCsrError::InvalidMemoryReservation.

- [ ] **Step 4: Implement checked dense-size estimation**

Count byte widths exactly:

~~~text
vertex_ids = vertex_count * 16
vertex_owners = vertex_count * 4
offsets = (vertex_count + 1) * (4 or 8)
neighbors = adjacency_count * 4
edge_refs = adjacency_count * 4
edge_ids = edge_count * 16
edge_owners = edge_count * 4
edge_types = edge_count * 4
~~~

Use checked_add and checked_mul; overflow returns SnapshotCsrError::SizeOverflow. Add SnapshotCsr::retained_bytes_from_capacities() and require it to include Vec capacities rather than lengths.

- [ ] **Step 5: Test actual retained bytes against the estimator**

Build graphs at 0, 1, 127, and 4096 edges with self, parallel, and remote endpoints. Assert actual retained bytes <= CsrSizeEstimate::bytes(). If the allocator returns capacity above the requested capacity, grow the estimate by the observed capacity rule rather than weakening the assertion.

- [ ] **Step 6: Test all failure release paths**

Inject failures immediately after dictionary allocation, degree allocation, prefix construction, adjacency fill, validation, and fingerprinting through a test-only BuildFailurePoint enum. Each case must return an error and usage() must equal CsrMemoryUsage::default().

- [ ] **Step 7: Run GREEN verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p snapshot-csr --test budget -- --test-threads=1
cargo test --locked -p snapshot-csr --test core_layout -- --test-threads=1
~~~

Expected: PASS; below/equal/above boundaries and all injected failures leave no retained reservation.

- [ ] **Step 8: Commit**

~~~bash
git add crates/snapshot-csr/src/budget.rs crates/snapshot-csr/src/csr.rs crates/snapshot-csr/src/lib.rs crates/snapshot-csr/tests/budget.rs
git commit -m "feat: add snapshot csr memory accounting"
~~~

### Task 3: Define Caller-Snapshot Bounded Graph Pages and the Memory Oracle

**Files:**
- Modify: crates/storage-api/src/query.rs
- Modify: crates/storage-api/src/lib.rs
- Modify: crates/storage-api/tests/query_primitives.rs
- Modify: crates/temporal-storage/src/key.rs
- Modify: crates/temporal-storage/src/lib.rs
- Modify: crates/temporal-storage/src/observed_adapter.rs
- Modify: crates/adapter-memory/src/lib.rs
- Modify: crates/adapter-memory/tests/adapter_contract.rs
- Create: crates/snapshot-csr/src/page_source.rs
- Modify: crates/snapshot-csr/src/lib.rs
- Create: crates/snapshot-csr/tests/page_source.rs

**Interfaces:**
- Consumes: existing AdjacencyExpandRequest/Page, QueryPageBounds, KeySpan, ReadSnapshot, edge_identity_prefix, and canonical page validation.
- Produces:

~~~rust
pub struct EdgeIdentityScanRequest;
impl EdgeIdentityScanRequest {
    pub fn new(span: KeySpan, bounds: QueryPageBounds)
        -> Result<Self, QueryPrimitiveError>;
    pub fn span(&self) -> &KeySpan;
    pub const fn bounds(&self) -> QueryPageBounds;
}
pub struct EdgeIdentityScanPage;
impl EdgeIdentityScanPage {
    pub fn new(
        request: &EdgeIdentityScanRequest,
        applied_log_index: u64,
        entries: Vec<KeyValue>,
        next_start: Option<LogicalKey>,
    ) -> Result<Self, QueryPrimitiveError>;
}
pub trait ReadSnapshot: Send + Sync {
    fn expand_adjacency<'a>(
        &'a self,
        request: &'a AdjacencyExpandRequest,
    ) -> AdapterFuture<'a, AdjacencyExpandPage>;
    fn scan_edge_identities<'a>(
        &'a self,
        request: &'a EdgeIdentityScanRequest,
    ) -> AdapterFuture<'a, EdgeIdentityScanPage>;
}
pub async fn read_adjacency_page(
    read: &dyn ReadSnapshot,
    request: &AdjacencyExpandRequest,
) -> Result<AdjacencyExpandPage, SnapshotCsrError>;
pub async fn read_edge_identity_page(
    read: &dyn ReadSnapshot,
    request: &EdgeIdentityScanRequest,
) -> Result<EdgeIdentityScanPage, SnapshotCsrError>;
~~~

QueryPrimitiveCapabilities gains edge_identity_scan: PushdownGuarantee as the fifth constructor argument, ordered before change_scan. The existing adjacency_expand field describes both adapter-level and snapshot-bound typed adjacency; an adapter may advertise it only when begin_read_snapshot exposes the same bounded operation.

- [ ] **Step 1: Write failing page validation tests**

Add tests for EdgeIdentityScanRequest rejecting non-Identity keyspace, zero bounds, and unbounded maximums. Add page tests rejecting an entry outside the request span, non-strict order, malformed continuation, too many items, too many bytes, and an unknown tag decoded by temporal-storage.

- [ ] **Step 2: Run the storage API RED test**

Run:

~~~bash
cargo test --locked -p storage-api --test query_primitives edge_identity -- --test-threads=1
~~~

Expected: FAIL because EdgeIdentityScanRequest and EdgeIdentityScanPage are missing.

- [ ] **Step 3: Add the typed page and capability**

Reuse validate_scan_page with QueryPrimitiveKind::EdgeIdentityScan, exact Identity keyspace validation, and PushdownGuarantee::Exact fixed by the page constructor. Add accessors applied_log_index(), entries(), next_start(), and into_entries().

Update every QueryPrimitiveCapabilities::new call in the workspace to pass PushdownGuarantee::Unsupported for edge_identity_scan until its backend task implements support. Do not infer support from generic scan.

- [ ] **Step 4: Extend ReadSnapshot and forwarding wrappers**

Add default UnsupportedOperation errors named snapshot adjacency expand and snapshot edge identity scan. Forward both methods in Arc<T>, &T, registry-bound snapshots, and ObservedReadSnapshot. ObservedReadSnapshot records exactly one adapter RPC for each page call.

- [ ] **Step 5: Add whole-partition adjacency prefixes**

Export:

~~~rust
pub fn out_adjacency_partition_prefix(graph: GraphId, partition: PartitionId) -> Vec<u8>;
pub fn cross_out_adjacency_partition_prefix(graph: GraphId, partition: PartitionId) -> Vec<u8>;
pub fn in_adjacency_partition_prefix(graph: GraphId, partition: PartitionId) -> Vec<u8>;
pub fn cross_in_adjacency_partition_prefix(graph: GraphId, partition: PartitionId) -> Vec<u8>;
~~~

Each prefix is tag + graph u64 + partition u32. Builder requests contain the local and cross span for one direction; AdjacencyExpandRequest continues to reject mixed AdjOut/AdjIn keyspaces.

- [ ] **Step 6: Implement MemoryReadSnapshot pages**

Use the existing bounded_page helper for edge identities. For adjacency, iterate the request spans in input ordinal order, stop before exceeding max_items or max_bytes, and return AdjacencyCursor { input_ordinal, start } for the first unconsumed entry. Return PushdownGuarantee::Candidate because temporal visibility remains above the adapter.

MemoryAdapter advertises Candidate adjacency, Exact edge_identity_scan, and exposes both only through its cloned read snapshot.

- [ ] **Step 7: Add exact-index wrapper tests**

Construct a fake ReadSnapshot whose applied_log_index is 7 but whose page reports 8. read_adjacency_page and read_edge_identity_page must return SnapshotCsrError::AppliedIndexMismatch { expected: 7, actual: 8 }. Add unsupported tests proving no raw scan is attempted when either typed method returns UnsupportedOperation.

- [ ] **Step 8: Run GREEN verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p storage-api --test query_primitives -- --test-threads=1
cargo test --locked -p adapter-memory --test adapter_contract -- --test-threads=1
cargo test --locked -p snapshot-csr --test page_source -- --test-threads=1
~~~

Expected: PASS, including mixed direction, malformed continuation, item/byte overrun, non-strict ordering, wrong applied index, and unsupported-without-raw-fallback cases.

- [ ] **Step 9: Commit**

~~~bash
git add Cargo.lock crates/storage-api crates/temporal-storage/src/key.rs crates/temporal-storage/src/lib.rs crates/temporal-storage/src/observed_adapter.rs crates/adapter-memory crates/snapshot-csr/src/page_source.rs crates/snapshot-csr/src/lib.rs crates/snapshot-csr/tests/page_source.rs
git commit -m "feat: add snapshot bound graph pages"
~~~

### Task 4: Build Immutable Current and Historical CSR in One Snapshot

**Files:**
- Create: crates/snapshot-csr/src/builder.rs
- Modify: crates/snapshot-csr/src/lib.rs
- Modify: crates/snapshot-csr/src/csr.rs
- Create: crates/snapshot-csr/tests/builder.rs

**Interfaces:**
- Consumes: read_adjacency_page, read_edge_identity_page, PointHistoryReader::read_batch, EdgeIdentity::decode, decode_graph_key, CsrMemoryAccount::reserve_build, SnapshotCsr::from_edges.
- Produces:

~~~rust
pub struct SnapshotBuildLimits {
    pub max_candidate_edges: u64,
    pub max_vertices: u64,
    pub page_bounds: QueryPageBounds,
    pub history_budget: HistoryReadBudget,
}
pub trait BuildFence: Send + Sync {
    fn check(&self) -> Result<(), SnapshotCsrError>;
}
pub struct SnapshotCsrBuilder {
    memory: CsrMemoryAccount,
}
pub struct UnpublishedSnapshotCsr;
impl SnapshotCsrBuilder {
    pub fn new(memory: CsrMemoryAccount) -> Self;
    pub async fn build(
        &self,
        read: &dyn ReadSnapshot,
        key: SnapshotCsrKey,
        visibility_transaction_time: TransactionTime,
        limits: SnapshotBuildLimits,
        fence: &dyn BuildFence,
    ) -> Result<UnpublishedSnapshotCsr, SnapshotCsrError>;
}
impl UnpublishedSnapshotCsr {
    pub fn csr(&self) -> &SnapshotCsr;
    pub fn retained_bytes(&self) -> u64;
}
~~~

- [ ] **Step 1: Write current and historical oracle tests**

Seed one local edge, one parallel edge, one self-loop, and one cross-shard edge. Assert outgoing and incoming CSR results equal a BTreeMap oracle. Seed an edge deleted at transaction 300 but visible at transaction 200 and an edge created at 250; the transaction-200 historical build must include the deleted-current edge and exclude the not-yet-created edge.

- [ ] **Step 2: Run the builder RED test**

Run:

~~~bash
cargo test --locked -p snapshot-csr --test builder -- --test-threads=1
~~~

Expected: FAIL because SnapshotCsrBuilder is missing.

- [ ] **Step 3: Validate identity and reserve before arrays**

At entry, require key.applied_log_index() == read.applied_log_index(), a point valid predicate, and a Current/AsOf point transaction predicate. Convert limits.max_candidate_edges and max_vertices into a conservative CsrSizeEstimate and reserve build workspace before creating dictionary, degree, adjacency, or edge vectors.

Call fence.check() before the first page, after every page, after each PointHistoryReader batch, before prefix sum, before validation, and before fingerprint completion.

- [ ] **Step 4: Implement the current candidate loop**

For key.transaction_time == Current, create two partition spans for the selected direction: local adjacency and cross adjacency. Page with continuation until exhausted. Decode every GraphKey into a CsrEdge identity, reject the wrong graph/partition/direction, reject duplicate edge IDs, and stop with CandidateLimitExceeded before retaining candidate max + 1.

Batch PointHistoryRequest values in chunks no larger than MAX_CANONICAL_BATCH_RANGES, using visibility_transaction_time and the exact ValidTimePredicate::Point. Retain only outcomes with value.is_some(); demanded properties are Selected(&[]) because visibility, endpoints, owners, and edge type come from identity/adjacency bytes.

- [ ] **Step 5: Implement the historical candidate loop**

For key.transaction_time == AsOf(transaction_time), require visibility_transaction_time == transaction_time, then scan edge_identity_prefix(graph, partition) using EdgeIdentityScanRequest pages. Decode each key as GraphKey::EdgeIdentity and each value as EdgeIdentity. Reject a current-adjacency-derived candidate source.

Batch PointHistoryReader requests at transaction_time and the key valid point, retain visible edges, then keep only edges whose source owner is this shard for outgoing or destination owner is this shard for incoming.

- [ ] **Step 6: Finish arrays and keep the object unpublished**

Pass visible CsrEdge values to SnapshotCsr::from_edges, call validate, and verify retained_bytes <= the workspace estimate. UnpublishedSnapshotCsr owns both SnapshotCsr and the build MemoryReservation; no Arc or cache handle is created in this task.

If any page reports the wrong index, continuation repeats or moves backwards, a key/value is corrupt, a limit is exceeded, a fence fails, or validation fails, return the classified error and let the owned reservation drop.

- [ ] **Step 7: Add failure matrix tests**

Cover cancellation before page 1, cancellation between pages, deadline during PointHistory batch, wrong applied index, placement/schema/mapping key mismatch injected by fence, malformed adjacency key, malformed EdgeIdentity value, repeated continuation, candidate limit below/equal/above, history byte failure, and fingerprint failure injection. Assert memory usage is zero and no cache-visible object exists after each error.

- [ ] **Step 8: Run GREEN verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p snapshot-csr --test builder -- --test-threads=1
cargo test --locked -p snapshot-csr --test budget -- --test-threads=1
~~~

Expected: PASS; current and historical results equal the independent oracle, including currently deleted/historically visible and not-yet-created cases.

- [ ] **Step 9: Commit**

~~~bash
git add crates/snapshot-csr/src/builder.rs crates/snapshot-csr/src/csr.rs crates/snapshot-csr/src/lib.rs crates/snapshot-csr/tests/builder.rs
git commit -m "feat: build immutable snapshot csr"
~~~

### Task 5: Add the Durable Committed CSR Overlay

**Files:**
- Create: crates/snapshot-csr/src/committed_overlay.rs
- Modify: crates/snapshot-csr/src/lib.rs
- Create: crates/snapshot-csr/tests/committed_overlay.rs

**Interfaces:**
- Consumes: SnapshotCsrIdentity, SnapshotCsr traversal, CsrMemoryAccount::reserve_overlay.
- Produces a type deliberately distinct from query_executor::GraphOverlay:

~~~rust
pub enum CommittedAdjacencyOperation { Add(CsrEdge), Remove { edge_id: ElementId } }
pub struct CommittedGraphDelta {
    pub identity: SnapshotCsrIdentity,
    pub applied_log_index: u64,
    pub operations: Vec<CommittedAdjacencyOperation>,
}
pub struct OverlayRebuildPolicy {
    pub edge_ratio_basis_points: u32,
    pub retained_bytes: u64,
    pub slowdown_basis_points: u32,
}
impl OverlayRebuildPolicy {
    pub fn new(
        edge_ratio_basis_points: u32,
        retained_bytes: u64,
        slowdown_basis_points: u32,
    ) -> Result<Self, SnapshotCsrError>;
}
impl CommittedGraphDelta {
    pub fn new(
        identity: SnapshotCsrIdentity,
        applied_log_index: u64,
        operations: Vec<CommittedAdjacencyOperation>,
    ) -> Result<Self, SnapshotCsrError>;
}
pub struct CommittedCsrOverlay;
pub enum OverlayApplyOutcome { Applied, RebuildRequired }
impl CommittedCsrOverlay {
    pub fn new(
        base: Arc<SnapshotCsr>,
        memory: CsrMemoryAccount,
        max_entries: u64,
        max_bytes: u64,
        policy: OverlayRebuildPolicy,
    ) -> Result<Self, SnapshotCsrError>;
    pub fn apply(
        &mut self,
        delta: CommittedGraphDelta,
    ) -> Result<OverlayApplyOutcome, SnapshotCsrError>;
    pub fn neighbors(
        &self,
        vertex: ElementId,
    ) -> Result<Vec<CsrNeighbor>, SnapshotCsrError>;
    pub const fn covered_through(&self) -> u64;
    pub const fn retained_entries(&self) -> u64;
    pub const fn retained_bytes(&self) -> u64;
    pub const fn is_valid(&self) -> bool;
    pub fn observe_traversal_slowdown_basis_points(&mut self, value: u32)
        -> OverlayApplyOutcome;
}
~~~

- [ ] **Step 1: Write Base+Overlay and fail-closed tests**

Start Base at index 10. Apply addition at 11, removal at 12, and another addition at 13. Compare neighbors at each index with an independently updated BTreeMap oracle. Add gap 12 after 10, reorder 10 after 11, identity mismatch, mapping change, epoch change, duplicate edge, max entry, and max byte cases; every error must set is_valid() to false.

- [ ] **Step 2: Run the Overlay RED test**

Run:

~~~bash
cargo test --locked -p snapshot-csr --test committed_overlay -- --test-threads=1
~~~

Expected: FAIL because CommittedCsrOverlay is missing.

- [ ] **Step 3: Implement canonical committed delta validation**

CommittedGraphDelta::new validates non-empty operations, unique edge IDs per delta, exact SnapshotCsrIdentity equality, and applied_log_index == covered_through + 1. Add and Remove are normalized only after durable apply; this core type does not accept PreparedMutationBatch, transaction-local changes, timestamps in place of log indexes, or best-effort feeds.

- [ ] **Step 4: Implement bounded additions and tombstones**

Store additions by anchor local ID in BTreeMap<u32, Vec<CsrEdge>> and tombstones as BTreeSet<(u32, u128)>. Before mutating, compute the new retained byte charge, acquire the incremental overlay reservation, and validate limits. On any failure set invalidation reason, clear query eligibility, and release all overlay reservations. Persistence is not called from this type.

- [ ] **Step 5: Implement deterministic merged traversal**

neighbors reads Base, removes tombstoned edge IDs, overlays additions by edge ID, then sorts by vertex ID, edge type, and edge ID. A removed edge followed by an add at a later contiguous index becomes the new edge; an add followed by remove disappears. Reject traversal when invalidated or when the requested served index is not exactly covered_through.

- [ ] **Step 6: Implement rebuild signals without production defaults**

OverlayRebuildPolicy::new requires non-zero values and is always caller-supplied. Expose these benchmark-only constants:

~~~rust
pub const DESIGN_SEED_EDGE_RATIO_BPS: u32 = 200;
pub const DESIGN_SEED_RETAINED_BYTES: u64 = 64 * 1024 * 1024;
pub const DESIGN_SEED_SLOWDOWN_BPS: u32 = 1_000;
~~~

RebuildRequired is returned at the earliest configured edge ratio, byte, or measured slowdown threshold. No Default implementation may use these constants.

- [ ] **Step 7: Verify memory release and persistence isolation**

Assert invalidation and Drop release overlay usage to zero. Use a fake persistence counter and prove Overlay apply never calls it; a rejected overlay update cannot change the already-committed backend state.

- [ ] **Step 8: Run GREEN verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p snapshot-csr --test committed_overlay -- --test-threads=1
cargo test --locked -p snapshot-csr --test budget -- --test-threads=1
~~~

Expected: PASS; Base+Overlay equals the oracle at every covered index and every gap/reorder/identity/capacity fault fails closed.

- [ ] **Step 9: Commit**

~~~bash
git add crates/snapshot-csr/src/committed_overlay.rs crates/snapshot-csr/src/lib.rs crates/snapshot-csr/tests/committed_overlay.rs
git commit -m "feat: add committed csr overlay"
~~~

### Task 6: Add Cache Admission, Single Flight, Atomic Publish, and Rebuild Lifecycle

**Files:**
- Create: crates/snapshot-csr/src/cache.rs
- Modify: crates/snapshot-csr/src/lib.rs
- Create: crates/snapshot-csr/tests/cache.rs

**Interfaces:**
- Consumes: SnapshotCsrBuilder, UnpublishedSnapshotCsr, CsrMemoryAccount::reserve_published, CommittedCsrOverlay.
- Produces:

~~~rust
pub enum CacheProbe { Hit, Miss, AdmissionRejected }
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotCsrCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub admission_rejects: u64,
    pub builds_started: u64,
    pub builds_published: u64,
    pub evictions: u64,
    pub invalidations: u64,
}
pub struct SnapshotCsrHandle;
impl SnapshotCsrHandle {
    pub fn csr(&self) -> &SnapshotCsr;
    pub fn served_applied_log_index(&self) -> u64;
    pub fn neighbors(
        &self,
        vertex: ElementId,
    ) -> Result<Vec<CsrNeighbor>, SnapshotCsrError>;
}
pub struct SnapshotCsrCache;
impl SnapshotCsrCache {
    pub fn new(memory: CsrMemoryAccount) -> Self;
    pub fn probe(&self, key: &SnapshotCsrKey) -> CacheProbe;
    pub async fn get_or_build<F, Fut>(
        &self,
        key: SnapshotCsrKey,
        build: F,
    ) -> Result<SnapshotCsrHandle, SnapshotCsrError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<UnpublishedSnapshotCsr, SnapshotCsrError>>;
    pub fn invalidate_latest(
        &self,
        identity: &SnapshotCsrIdentity,
        reason: CacheInvalidationReason,
    );
    pub fn evict_historical_lru(&self, shard_id: u32) -> bool;
    pub fn stats(&self) -> SnapshotCsrCacheStats;
}
~~~

- [ ] **Step 1: Write concurrent miss and publication tests**

Use a barrier and AtomicU64 build counter. Launch eight get_or_build calls for the same key; exactly one builder runs and all eight handles expose the same fingerprint. While the builder is paused after array completion, probe must remain Miss and no handle may observe the unpublished object.

- [ ] **Step 2: Run the cache RED test**

Run:

~~~bash
cargo test --locked -p snapshot-csr --test cache -- --test-threads=1
~~~

Expected: FAIL because SnapshotCsrCache is missing.

- [ ] **Step 3: Implement exact-key entries and single flight**

Use a tokio::sync::Mutex-protected state with entries keyed by complete SnapshotCsrKey and in_flight keyed by the same key. Waiters use Notify and re-check the result after wake. A cancelled waiter drops only its wait; it must not publish or remove another caller's completed result.

CacheProbe::AdmissionRejected is returned only for classified memory/admission refusal. Corruption, applied-index mismatch, epoch mismatch, mapping mismatch, and invalid Overlay are returned as errors, not converted to misses.

- [ ] **Step 4: Implement atomic memory handoff and publication**

After build succeeds, acquire a published reservation for unpublished.retained_bytes while the workspace reservation is still held. Insert Arc<PublishedEntry> under the mutex only after validate and fingerprint succeed. Then consume UnpublishedSnapshotCsr and release the workspace reservation. If published reservation fails, return InsufficientMemory and leave the cache unchanged.

- [ ] **Step 5: Implement latest/historical policy and LRU**

Classify Current predicates as latest and AsOf predicates as historical. Per shard/direction, retain one preferred latest and one historical entry initially. A new historical publish evicts the least-recently-hit historical entry whose cache Arc is removable; active external Arc handles remain valid but no new lookup returns an evicted entry. A latest publish at a higher exact applied index replaces only the matching SnapshotCsrIdentity lineage. Current has no wall-clock payload in the key; exact applied_log_index identifies the read view while SnapshotCsrIdentity stays stable across contiguous Overlay indexes.

- [ ] **Step 6: Implement bounded rebuild**

Keep the old published latest entry serviceable while a new single-flight build owns workspace memory. Do not remove the old entry until the new published reservation succeeds and the atomic swap occurs. If Overlay becomes invalid, invalidate lookup eligibility immediately; active handles may finish only at their already-covered exact index.

- [ ] **Step 7: Add deterministic lifecycle tests**

Cover concurrent miss, builder failure, published-reservation failure, waiter cancellation, cache eviction, active reference after eviction, same key/different generation non-hit, rebuild old-image availability, rebuild peak usage, invalid Overlay, and a newly constructed empty cache after simulated restart rebuilding from the Memory backend.

- [ ] **Step 8: Run GREEN verification**

Run:

~~~bash
cargo fmt --all -- --check
cargo test --locked -p snapshot-csr -- --test-threads=1
cargo clippy --locked -p snapshot-csr --all-targets -- -D warnings
~~~

Expected: all commands exit 0; failed/cancelled builds leave no entry or reservation, active references survive eviction, and restart-empty-cache produces the same fingerprint after rebuild.

- [ ] **Step 9: Commit**

~~~bash
git add crates/snapshot-csr/src/cache.rs crates/snapshot-csr/src/lib.rs crates/snapshot-csr/tests/cache.rs
git commit -m "feat: add snapshot csr cache lifecycle"
~~~

## Core Plan Completion Gate

Run:

~~~bash
cargo fmt --all -- --check
cargo clippy --locked -p storage-api -p temporal-storage -p adapter-memory -p snapshot-csr --all-targets -- -D warnings
cargo test --locked -p storage-api -- --test-threads=1
cargo test --locked -p temporal-storage -- --test-threads=1
cargo test --locked -p adapter-memory -- --test-threads=1
cargo test --locked -p snapshot-csr -- --test-threads=1
~~~

Expected: all commands exit 0. Source inspection must show no snapshot-csr dependency on cypher-ast, cypher-syntax, cypher-sema, cypher-compiler, bolt-protocol, query-executor, distributed-query, adapter-rocksdb, adapter-postgres, or adapter-neo4j. Integration Tasks 7-12 begin only after this gate is review-clean.
