# Sidecar Resumable Snapshot Sessions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every process-isolated DTGProxy Adapter a first-class source or target of bounded, reconnect-safe canonical logical snapshot migration, including PostgreSQL serve and restore-target modes.

**Architecture:** `SidecarService` owns the active Adapter or one preconfigured restore target plus a process-global bounded session registry. Each export/restore session runs in a dedicated bounded worker actor so the existing borrow-based Reader/Restore Session remains on that thread's stack; TCP connections only carry canonical, idempotent commands identified by request ID, session ID, ordinal, and digest. `SidecarAdapter` and `SidecarAdapterFactory` expose those remote sessions through the existing Storage Adapter SPI, leaving `adapter-registry` responsible for capability gates and end-to-end migration.

**Tech Stack:** Rust 1.93, edition 2024, standard-library threads/channels/TCP, Prost 0.11.9, CRC32 1.5.0, BLAKE3 1.8.5, getrandom 0.4.3, PostgreSQL 0.19.14, RocksDB 0.24.0.

## Global Constraints

- The canonical snapshot format remains Storage API version 1; this plan does not fork or duplicate it.
- The fixed DTAS frame `WIRE_VERSION` remains 1; new clients negotiate exact features with `Hello`.
- Encoded frame payloads are capped at exactly `20 * 1024 * 1024` bytes and checked before allocation.
- Logical chunks remain capped at exactly 16 MiB and 65,536 entries.
- Session IDs are 128 random bits from the operating-system CSPRNG and are not authorization credentials.
- Default active export sessions are 32 with a hard maximum of 64; a restore-target permits exactly one active restore; total active sessions are at most 65.
- Each session command channel has capacity 1; no response cache may retain more than the latest export chunk per export session.
- Idle TTL and completed-result TTL default to 300 seconds; the Reaper interval defaults to 5 seconds; worker shutdown acknowledgement defaults to 30 seconds.
- Begin replay and expired-session tombstone caches each hold at most 4,096 entries.
- Sessions survive connection loss and connection-pool switching but not Sidecar process loss.
- A target is unavailable through Sidecar data RPC until `finish` succeeds, final Descriptor/Applied Index validation succeeds, and the service atomically installs the Adapter.
- A crash after backend publication but before `RestoreComplete` leaves the target quarantined; the source may not enter Dual Apply or Cutover.
- Unauthenticated TCP listeners remain loopback-only; no secret field is serialized, logged, or exposed through Debug.
- Backend work never runs on a Raft Apply worker or while the session registry lock is held.
- `#![forbid(unsafe_code)]` remains effective in every DTGProxy crate.
- Existing Describe/Apply/MultiGet/Scan/AppliedLogIndex/Health behavior and tests remain green.
- The approved design is `docs/superpowers/specs/2026-07-18-sidecar-resumable-snapshot-sessions-design.md` and is authoritative when a detail is not repeated here.

## File Structure

- `crates/adapter-registry/src/lib.rs`: dynamic provider names, public request inspection, and final restore Descriptor/index fencing.
- `crates/adapter-registry/tests/registry.rs`: Registry pre-publication and post-publication conformance tests.
- `crates/adapter-sidecar/src/lib.rs`: public model, DTAS framing/Protobuf codec, base client, TCP transport/server, and module re-exports.
- `crates/adapter-sidecar/src/service.rs`: service state machine, base/session dispatch, capability intersection, publication, health, and metrics snapshot.
- `crates/adapter-sidecar/src/session.rs`: bounded registry, Begin replay cache, tombstones, Reaper, session actor commands, and lifecycle guards.
- `crates/adapter-sidecar/src/snapshot_client.rs`: remote logical Reader, dynamic remote Factory, remote Restore Session, and abort-on-drop.
- `crates/adapter-sidecar/proto/dtg_adapter_v1.proto`: canonical documentation for the hand-derived Prost wire messages.
- `crates/adapter-sidecar/tests/snapshot_protocol.rs`: all new request/response codec and fail-closed tests.
- `crates/adapter-sidecar/tests/support/mod.rs`: reusable snapshot-capable test Adapter/Factory, deterministic executor, and fault controls; production code never depends on it.
- `crates/adapter-sidecar/tests/service_state.rs`: Active/Waiting/Restoring/Faulted behavior and feature intersection.
- `crates/adapter-sidecar/tests/export_session.rs`: export ordering, replay, capacity, TTL, and cleanup.
- `crates/adapter-sidecar/tests/restore_session.rs`: restore idempotency, divergence, publication, and cleanup.
- `crates/adapter-sidecar/tests/snapshot_client.rs`: SPI-level loopback export/restore and capability gates.
- `crates/adapter-sidecar/tests/tcp_snapshot.rs`: reconnect, response loss, pool switching, shutdown, and bounded-resource tests.
- `crates/adapter-postgres/src/sidecar.rs`: validated serve/restore-target configuration and service construction.
- `crates/adapter-postgres/src/bin/dtgproxy-postgres-sidecar.rs`: minimal environment/bootstrap wrapper.
- `crates/adapter-postgres/tests/sidecar_config.rs`: secret-safe mode/config validation without a database.
- `crates/adapter-postgres/tests/live_sidecar.rs`: ignored-by-default real TCP PostgreSQL migration tests.
- `docs/adapter-spi.md` and `docs/postgresql-adapter.md`: implemented wire and certification evidence.

---

### Task 1: Harden the Registry Restore Boundary

**Files:**
- Modify: `crates/adapter-registry/src/lib.rs`
- Modify: `crates/adapter-registry/tests/registry.rs`
- Modify: `crates/adapter-rocksdb/src/lib.rs`
- Modify: `crates/adapter-postgres/src/lib.rs`

**Interfaces:**
- Produces: `AdapterFactory::provider_name(&self) -> &str`.
- Produces: `AdapterOpenRequest::public_parameters(&self) -> &BTreeMap<String, String>`.
- Produces: required `AdapterRestoreSession::abort(self: Box<Self>) -> AdapterRestoreFuture<'_, ()>` for observable cleanup.
- Produces: `RegistryError::FinalDescriptorMismatch`, `RegistryError::RestoredIndexMismatch { expected: u64, actual: u64 }`, and `RegistryError::Target(AdapterError)`.
- Guarantees: `AdapterRegistry::restore` revalidates the returned Adapter after `finish` and before creating `OpenedAdapter`.

- [ ] **Step 1: Write failing request/provider/final-fence tests**

Add these cases to `crates/adapter-registry/tests/registry.rs`; use the file's existing test executor and mock Factory conventions:

```rust
#[test]
fn public_request_view_excludes_secrets() {
    let request = AdapterOpenRequest::new("target")
        .with_parameter("pool_size", "8")
        .with_secret("connection_string", SecretString::new("postgres://secret"));
    assert_eq!(request.public_parameters().get("pool_size").map(String::as_str), Some("8"));
    assert!(!format!("{request:?}").contains("postgres://secret"));
}

#[test]
fn registry_rejects_descriptor_or_index_drift_after_finish() {
    let descriptor_error = match block_on(restore_with_final_adapter(
        prospective_descriptor("target-v1"),
        final_adapter("target-v2", 7),
        manifest_at(7),
    )) { Err(error) => error, Ok(_) => panic!("descriptor drift was accepted") };
    assert!(matches!(descriptor_error, RegistryError::FinalDescriptorMismatch));

    let index_error = match block_on(restore_with_final_adapter(
        prospective_descriptor("target-v1"),
        final_adapter("target-v1", 6),
        manifest_at(7),
    )) { Err(error) => error, Ok(_) => panic!("index drift was accepted") };
    assert!(matches!(index_error, RegistryError::RestoredIndexMismatch { expected: 7, actual: 6 }));
}
```

Implement the test setup with a `FixedAdapter { descriptor, applied_index }` whose read/apply methods return empty successful values, and a `FinalRestoreFactory { prospective, final_adapter }` whose Restore Session returns the prospective Descriptor and the supplied final Adapter. `restore_with_final_adapter` registers that Factory, creates an empty `LogicalSnapshotReader`/Manifest at the requested index, and calls `AdapterRegistry::restore`; no production test hook is added.

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-registry --test registry`

Expected: compile failures for `public_parameters`, dynamic provider signature, and the two final-fence errors.

- [ ] **Step 3: Implement public inspection and the final restore fence**

Change the trait and request API exactly as follows:

```rust
pub trait AdapterFactory: Send + Sync {
    fn provider_name(&self) -> &str;
    // existing open and begin_restore methods remain unchanged
}

impl AdapterOpenRequest {
    #[must_use]
    pub const fn public_parameters(&self) -> &BTreeMap<String, String> {
        &self.parameters
    }
}
```

Extend the Restore Session without changing its borrow-based ownership:

```rust
pub trait AdapterRestoreSession: Send {
    fn descriptor(&self) -> AdapterDescriptorV1;
    fn write_chunk<'a>(&'a mut self, chunk: LogicalSnapshotChunkV1) -> AdapterRestoreFuture<'a, ()>;
    fn finish<'a>(self: Box<Self>, manifest: LogicalSnapshotManifestV1)
        -> AdapterRestoreFuture<'a, Arc<dyn StorageAdapter>> where Self: 'a;
    fn abort<'a>(self: Box<Self>) -> AdapterRestoreFuture<'a, ()> where Self: 'a;
}
```

In `register`, own the provider string before moving the Factory. Update every in-tree Factory implementation to the dynamic `&str` signature. RocksDB `abort` drops its open staging Adapter and returns any `remove_dir_all` failure; PostgreSQL `abort` calls `delete_namespace` and returns any database failure. Keep Drop as best-effort crash fallback, but Sidecar uses the explicit result to choose Waiting versus Faulted. In `restore`, retain the Manifest applied index, call `finish`, compare `adapter.descriptor()` to the prospective Descriptor, validate it again against the requested Requirement, and require `adapter.applied_log_index() == manifest.header().applied_log_index()`. Map this final Adapter read through `RegistryError::Target`, not the source-reader error variant. Never put a mismatched Adapter into `OpenedAdapter`.

- [ ] **Step 4: Run GREEN and all Registry tests**

Run: `cargo test -p adapter-registry`

Expected: all Registry unit/integration tests pass with no ignored failure.

- [ ] **Step 5: Commit**

```bash
git add crates/adapter-registry crates/adapter-rocksdb/src/lib.rs crates/adapter-postgres/src/lib.rs
git commit -m "fix: fence restored adapter publication"
```

### Task 2: Define and Canonically Encode the Snapshot Wire

**Files:**
- Modify: `crates/adapter-sidecar/Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/adapter-sidecar/proto/dtg_adapter_v1.proto`
- Modify: `crates/adapter-sidecar/src/lib.rs`
- Create: `crates/adapter-sidecar/tests/snapshot_protocol.rs`
- Modify: `crates/adapter-sidecar/tests/protocol.rs`

**Interfaces:**
- Produces: `FeatureSet`, `HelloRequest`, `HelloResponse`, `PublicAdapterOpenRequest`.
- Produces: all snapshot `Request` and `Response` variants listed below.
- Produces: stable `RemoteErrorCode` values 100 through 114.
- Changes: `MAX_FRAME_PAYLOAD_BYTES` to exactly 20 MiB.

- [ ] **Step 1: Write failing round-trip and limit tests**

Create `snapshot_protocol.rs` with one vector containing every new variant:

```rust
let requests = vec![
    Request::Hello(HelloRequest::adapter_client()),
    Request::BeginExport(BeginExportRequest {
        limits: LogicalSnapshotExportRequest::new(4096, 4 * 1024 * 1024).unwrap(),
        expected_applied_log_index: Some(9),
    }),
    Request::ExportNext { session_id: 11, expected_ordinal: 0 },
    Request::BeginRestore(BeginRestoreRequest {
        header: LogicalSnapshotHeaderV1::new(22, 9),
        target: PublicAdapterOpenRequest::new("target").with_parameter("pool_size", "8"),
    }),
    Request::RestoreChunk { session_id: 12, chunk: sample_chunk(22, 0) },
    Request::FinishRestore { session_id: 12, manifest: sample_manifest(22, 9) },
    Request::AbortSession { session_id: 12 },
];
for (ordinal, request) in requests.into_iter().enumerate() {
    let encoded = encode_frame(100 + ordinal as u128, &request).unwrap();
    assert_eq!(decode_frame::<Request>(&encoded).unwrap().into_message(), request);
}
```

Add the corresponding `Hello`, `ExportStarted`, `ExportChunk`, `ExportComplete`, `RestoreStarted`, `RestoreChunkAccepted`, `RestoreComplete`, and `SessionAborted` responses. Add negative tests for 15/17-byte IDs, 31/33-byte digests, invalid format version, unordered/duplicate public parameters, invalid chunk limits, unknown Feature bits, and a declared payload of `20 MiB + 1` rejected before allocation. Update the existing limit assertion from 16 to 20 MiB.

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-sidecar --test snapshot_protocol --test protocol`

Expected: compile failures because the types/variants do not exist and the payload constant is still 16 MiB.

- [ ] **Step 3: Add dependencies, public model, Prost messages, and codecs**

Pin:

```toml
adapter-registry = { path = "../adapter-registry" }
blake3 = "=1.8.5"
getrandom = "=0.4.3"
```

Define the public enum surface exactly:

```rust
pub enum Request {
    Hello(HelloRequest),
    Describe,
    Apply(CommittedMutationBatch),
    MultiGet(Vec<LogicalKey>),
    Scan(KeySpan),
    AppliedLogIndex,
    Health,
    BeginExport(BeginExportRequest),
    ExportNext { session_id: u128, expected_ordinal: u64 },
    BeginRestore(BeginRestoreRequest),
    RestoreChunk { session_id: u128, chunk: LogicalSnapshotChunkV1 },
    FinishRestore { session_id: u128, manifest: LogicalSnapshotManifestV1 },
    AbortSession { session_id: u128 },
}

pub enum Response {
    Hello(HelloResponse),
    Descriptor(AdapterDescriptorV1),
    Apply(ApplyReceipt),
    MultiGet(Vec<Option<Vec<u8>>>),
    Scan(Vec<KeyValue>),
    AppliedLogIndex(u64),
    Health(HealthStatus),
    ExportStarted(ExportStarted),
    ExportChunk { session_id: u128, chunk: LogicalSnapshotChunkV1 },
    ExportComplete { session_id: u128, manifest: LogicalSnapshotManifestV1 },
    RestoreStarted(RestoreStarted),
    RestoreChunkAccepted { session_id: u128, ordinal: u64, digest: [u8; 32] },
    RestoreComplete(RestoreComplete),
    SessionAborted { session_id: u128 },
    Error(RemoteError),
}
```

`FeatureSet` is a checked `u64` newtype with bits 0..=3 for `BASE_ADAPTER_V1`, `LOGICAL_EXPORT_SESSION_V1`, `LOGICAL_RESTORE_SESSION_V1`, and `RESUMABLE_ORDINAL_REPLAY_V1`. Encode u128 values as exactly 16 big-endian bytes and digests as exactly 32 bytes. Decode snapshot objects only through their validating constructors and `LogicalSnapshotAccumulator` rules; reject rather than normalize noncanonical fields.

`FeatureSet::from_bits(u64) -> Result<Self, ProtocolError>` rejects unknown bits; `bits(self) -> u64`, `contains(self, other) -> bool`, `intersection(self, other) -> Self`, and `union(self, other) -> Self` are the only bit operations used by protocol/service code.

Define the message structs and helpers with these exact fields:

```rust
pub struct HelloRequest {
    pub required_features: FeatureSet,
    pub optional_features: FeatureSet,
    pub max_payload_bytes: u32,
}
impl HelloRequest {
    pub fn adapter_client() -> Self;
    pub fn restore_client() -> Self;
}

pub struct HelloResponse {
    pub wire_version: u16,
    pub negotiated_features: FeatureSet,
    pub max_payload_bytes: u32,
    pub max_chunk_bytes: u32,
    pub max_chunk_entries: u32,
    pub snapshot_format_version: u16,
}

pub struct PublicAdapterOpenRequest {
    instance_id: String,
    parameters: BTreeMap<String, String>,
}
impl PublicAdapterOpenRequest {
    pub fn new(instance_id: impl Into<String>) -> Self;
    pub fn with_parameter(self, name: impl Into<String>, value: impl Into<String>) -> Self;
    pub fn instance_id(&self) -> &str;
    pub fn parameters(&self) -> &BTreeMap<String, String>;
}

pub struct BeginExportRequest {
    pub limits: LogicalSnapshotExportRequest,
    pub expected_applied_log_index: Option<u64>,
}
pub struct BeginRestoreRequest {
    pub header: LogicalSnapshotHeaderV1,
    pub target: PublicAdapterOpenRequest,
}
pub struct ExportStarted {
    pub session_id: u128,
    pub header: LogicalSnapshotHeaderV1,
    pub limits: LogicalSnapshotExportRequest,
}
pub struct RestoreStarted {
    pub session_id: u128,
    pub prospective_descriptor: AdapterDescriptorV1,
    pub max_chunk_bytes: u32,
    pub max_chunk_entries: u32,
}
pub struct RestoreComplete {
    pub session_id: u128,
    pub final_descriptor: AdapterDescriptorV1,
    pub applied_log_index: u64,
}
```

Assign stable codes exactly: FeatureUnsupported=100, NotActive=101, ResourceExhausted=102, SessionUnknown=103, SessionExpired=104, SessionKindMismatch=105, SessionBusy=106, OrdinalGap=107, OrdinalRegression=108, ChunkDigestMismatch=109, RequestReplayMismatch=110, TerminalReplayMismatch=111, RestoreAlreadyInProgress=112, ServiceFaulted=113, and TargetRequestMismatch=114.

- [ ] **Step 4: Run GREEN plus fuzz-like corruption regression**

Run: `cargo test -p adapter-sidecar --test snapshot_protocol --test protocol`

Expected: every old and new variant round-trips, and every corrupt/oversized/noncanonical case returns a typed `ProtocolError`.

- [ ] **Step 5: Commit**

```bash
git add Cargo.lock crates/adapter-sidecar
git commit -m "feat: add Sidecar snapshot wire protocol"
```

### Task 3: Introduce the Sidecar Service State Machine

**Files:**
- Create: `crates/adapter-sidecar/src/service.rs`
- Modify: `crates/adapter-sidecar/src/lib.rs`
- Create: `crates/adapter-sidecar/tests/support/mod.rs`
- Create: `crates/adapter-sidecar/tests/service_state.rs`
- Modify: `crates/adapter-sidecar/tests/client.rs`

**Interfaces:**
- Produces: `SidecarService::active`, `SidecarService::restore_target`, and `SidecarService::dispatch`.
- Produces: `SidecarSessionConfig`, `SidecarServiceState`, and `SidecarMetricsSnapshot`.
- Produces: `LoopbackTransport::from_service`.

- [ ] **Step 1: Write failing Active/Waiting/feature tests**

```rust
#[test]
fn restore_target_is_not_active_but_negotiates_restore() {
    let service = SidecarService::restore_target(
        Arc::new(SnapshotTestFactory::new()),
        Arc::new(AdapterOpenRequest::new("target")),
        SidecarSessionConfig::default(),
    ).unwrap();
    assert_eq!(service.state(), SidecarServiceState::WaitingForRestore);
    assert!(matches!(block_on(service.dispatch(1, Request::Health)), Response::Health(HealthStatus { ready: false, .. })));
    assert!(matches!(block_on(service.dispatch(2, Request::MultiGet(vec![]))), Response::Error(RemoteError { code, .. }) if code == RemoteErrorCode::NotActive as u32));
    let hello = block_on(service.dispatch(3, Request::Hello(HelloRequest::restore_client())));
    assert!(matches!(hello, Response::Hello(response) if response.negotiated_features.contains(FeatureSet::LOGICAL_RESTORE_SESSION_V1)));
}

#[test]
fn descriptor_intersects_backend_and_wire_capabilities() {
    let service = active_snapshot_service_without_restore_wire();
    let Response::Descriptor(descriptor) = block_on(service.dispatch(4, Request::Describe)) else { panic!() };
    assert!(descriptor.capabilities().logical_export);
    assert!(!descriptor.capabilities().logical_restore);
}
```

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-sidecar --test service_state --test client`

Expected: missing `SidecarService`, config/state, and service-backed Loopback transport.

- [ ] **Step 3: Implement service construction and base dispatch**

Use this state model:

```rust
enum ServiceMode {
    Active(Arc<dyn StorageAdapter>),
    WaitingForRestore {
        factory: Arc<dyn AdapterFactory>,
        request: Arc<AdapterOpenRequest>,
    },
    Restoring {
        session_id: u128,
        factory: Arc<dyn AdapterFactory>,
        request: Arc<AdapterOpenRequest>,
    },
    Faulted { detail: String },
}

pub enum SidecarServiceState { WaitingForRestore, Restoring, Active, Faulted }
```

Constructors return `Result<Arc<SidecarService>, SidecarServerError>` and establish the shared shutdown/lifecycle state; Task 4 attaches the single owned Reaper when the registry is introduced. `dispatch(request_id, request)` handles Hello/Health/base operations and delegates session operations to later tasks. Waiting/Restoring return `NOT_ACTIVE` for data RPC; Faulted returns `SERVICE_FAULTED`. `Describe` on Active returns backend capabilities intersected with configured Wire features.

Update `LoopbackTransport::new(adapter)` to wrap an Active service and add `from_service(Arc<SidecarService>)`. It assigns monotonically unique frame Request IDs and calls `service.dispatch(request_id, request)`.

`tests/support/mod.rs` defines `SnapshotTestAdapter` over an `Arc<Mutex<BTreeMap<LogicalKey, Vec<u8>>>>`, a canonical `LogicalSnapshotReader`, and `SnapshotTestFactory`/Restore Session with counters for open, Chunk write, finish, drop, and injected failure. Export/restore tests import it with `mod support;`; do not add test-only switches to production APIs.

Define and validate these config/metrics fields now so later tasks do not rename them:

```rust
pub struct SidecarSessionConfig {
    max_export_sessions: usize,
    max_restore_sessions: usize,
    idle_ttl: Duration,
    completed_ttl: Duration,
    reaper_interval: Duration,
    shutdown_timeout: Duration,
    begin_replay_capacity: usize,
    tombstone_capacity: usize,
}
impl SidecarSessionConfig {
    pub fn with_max_export_sessions(self, value: usize) -> Result<Self, SidecarServerError>;
    pub fn with_max_restore_sessions(self, value: usize) -> Result<Self, SidecarServerError>;
    pub fn with_idle_ttl(self, value: Duration) -> Result<Self, SidecarServerError>;
    pub fn with_completed_ttl(self, value: Duration) -> Result<Self, SidecarServerError>;
    pub fn with_reaper_interval(self, value: Duration) -> Result<Self, SidecarServerError>;
    pub fn with_shutdown_timeout(self, value: Duration) -> Result<Self, SidecarServerError>;
}

pub struct SidecarMetricsSnapshot {
    pub active_exports: u64,
    pub active_restores: u64,
    pub expired_sessions: u64,
    pub aborted_sessions: u64,
    pub rejected_sessions: u64,
    pub chunk_bytes: u64,
    pub chunk_operations: u64,
    pub chunk_latency_micros: u64,
    pub export_ordinal_replays: u64,
    pub digest_failures: u64,
    pub begin_replay_hits: u64,
    pub session_busy: u64,
    pub queue_wait_micros: u64,
    pub publish_successes: u64,
    pub publish_failures: u64,
    pub reaper_cleanups: u64,
    pub reaper_cleanup_micros: u64,
    pub worker_join_timeouts: u64,
}
```

- [ ] **Step 4: Run GREEN and base regressions**

Run: `cargo test -p adapter-sidecar --test service_state --test client`

Expected: service state tests and the original Storage Adapter client contract pass.

- [ ] **Step 5: Commit**

```bash
git add crates/adapter-sidecar
git commit -m "feat: add Sidecar service state machine"
```

### Task 4: Build the Bounded Session Registry and Reaper

**Files:**
- Create: `crates/adapter-sidecar/src/session.rs`
- Modify: `crates/adapter-sidecar/src/service.rs`
- Create: `crates/adapter-sidecar/tests/session_lifecycle.rs`

**Interfaces:**
- Produces: internal `SessionRegistry`, `SessionHandle`, `SessionKind`, and `SessionActivity`.
- Produces: Begin replay and tombstone behavior used by export/restore tasks.
- Produces: `SidecarService::metrics() -> SidecarMetricsSnapshot`.

- [ ] **Step 1: Write failing configuration and direct registry lifecycle tests**

```rust
#[test]
fn config_enforces_the_approved_hard_bounds() {
    assert!(SidecarSessionConfig::default().with_max_export_sessions(65).is_err());
    assert!(SidecarSessionConfig::default().with_max_restore_sessions(2).is_err());
    assert!(SidecarSessionConfig::default().with_idle_ttl(Duration::ZERO).is_err());
}

// In session.rs, where private registry internals are visible:
#[test]
fn begin_replay_and_tombstone_caches_are_bounded_and_content_checked() {
    let config = test_config_with_cache_capacity(2);
    let mut registry = SessionRegistry::new(config);
    let response = Response::SessionAborted { session_id: 7 };
    registry.remember_begin(77, [1; 32], 7, response.clone()).unwrap();
    assert_eq!(registry.replay_begin(77, [1; 32]).unwrap(), Some(response));
    assert!(matches!(registry.replay_begin(77, [2; 32]), Err(SessionError::RequestReplayMismatch)));
    registry.expire_for_test(7, SessionKind::Export);
    assert!(matches!(registry.lookup(7), Err(SessionError::Expired)));
    assert!(registry.begin_replay_len() <= 2);
    assert!(registry.tombstone_len() <= 2);
}
```

- [ ] **Step 2: Run RED**

Run:

```bash
cargo test -p adapter-sidecar --test session_lifecycle
cargo test -p adapter-sidecar --lib session::tests::
```

Expected: missing validated config builders, metrics, replay cache, and Reaper.

- [ ] **Step 3: Implement bounded lifecycle primitives**

`SessionActivity` owns a mutex-protected `{ in_flight, last_activity, terminal_at }`. `try_begin_command` atomically rejects concurrent commands with `SESSION_BUSY`; every actor command carries an activity completion guard so the actor—not the timed-out TCP Dispatcher—clears `in_flight` and refreshes activity.

`SessionRegistry` contains exactly:

```rust
struct SessionRegistry {
    sessions: BTreeMap<u128, SessionHandle>,
    begin_replays: BTreeMap<u128, BeginReplay>,
    tombstones: BTreeMap<[u8; 32], Tombstone>,
    export_count: usize,
    restore_count: usize,
}
```

The `#[cfg(test)]` module in `session.rs` may construct a two-entry config directly through `test_config_with_cache_capacity(2)` and expose `expire_for_test`, `begin_replay_len`, and `tombstone_len` only inside that private test module. None of these helpers is exported from the crate.

Generate Session IDs using `getrandom::fill(&mut [u8; 16])`, reject zero/collision, and never log them. Hash tombstone IDs with domain-separated BLAKE3. Begin replay hashes the canonical encoded request and stores only the small Started response. The Reaper wakes every configured interval even without traffic, removes idle/terminal sessions, sends Shutdown, waits up to 30 seconds for an exit acknowledgement, then joins. A timeout transitions the service to Faulted and increments `worker_join_timeouts`.

- [ ] **Step 4: Run GREEN and thread-leak repetition**

Run:

```bash
cargo test -p adapter-sidecar --test session_lifecycle -- --test-threads=1
cargo test -p adapter-sidecar --lib session::tests:: -- --test-threads=1
```

Expected: capacities reject deterministically; direct registry tests prove bounded caches, content-checked replay, tombstone lookup, and actor exit acknowledgement. Export/restore integration tasks add real expiry and 100-cycle thread-leak coverage once their actors exist.

- [ ] **Step 5: Commit**

```bash
git add crates/adapter-sidecar/src/session.rs crates/adapter-sidecar/src/service.rs crates/adapter-sidecar/tests/session_lifecycle.rs
git commit -m "feat: add bounded Sidecar session registry"
```

### Task 5: Implement the Export Worker Actor

**Files:**
- Modify: `crates/adapter-sidecar/src/session.rs`
- Modify: `crates/adapter-sidecar/src/service.rs`
- Create: `crates/adapter-sidecar/tests/export_session.rs`

**Interfaces:**
- Consumes: `StorageAdapter::begin_logical_export` and `LogicalSnapshotReader`.
- Implements: BeginExport, ExportNext, ExportChunk, ExportComplete, and AbortSession for export sessions.

- [ ] **Step 1: Write failing export ordering and replay tests**

```rust
#[test]
fn export_replays_only_the_latest_chunk_and_never_skips() {
    let service = populated_snapshot_service();
    let started = begin_export(&service, 100);
    let first = export_next(&service, started.session_id, 0);
    assert_eq!(export_next(&service, started.session_id, 0), first);
    assert_error_code(
        block_on(service.dispatch(103, Request::ExportNext {
            session_id: started.session_id,
            expected_ordinal: 2,
        })),
        RemoteErrorCode::OrdinalGap,
    );
    let complete = consume_to_complete(&service, started.session_id, first);
    assert_eq!(export_next(&service, started.session_id, complete.total_chunks), complete.response);
}

#[test]
fn export_capacity_and_abort_release_the_backend_reader() {
    let service = active_export_service(SidecarSessionConfig::default().with_max_export_sessions(1).unwrap());
    let first = begin_export(&service, 200);
    assert_error_code(block_on(service.dispatch(201, begin_export_request())), RemoteErrorCode::ResourceExhausted);
    assert!(matches!(block_on(service.dispatch(202, Request::AbortSession { session_id: first.session_id })), Response::SessionAborted { .. }));
    assert!(matches!(block_on(service.dispatch(203, begin_export_request())), Response::ExportStarted(_)));
}
```

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-sidecar --test export_session`

Expected: BeginExport currently returns unsupported-session errors.

- [ ] **Step 3: Implement the stack-owned export actor**

The actor closure must declare variables in this lifetime order:

```rust
let adapter = Arc::clone(&adapter);
let reader = block_on_dispatch(adapter.begin_logical_export(limits))?;
let header = reader.header().clone();
let mut reader = Some(reader);
let mut next_ordinal = 0_u64;
let mut last_chunk: Option<LogicalSnapshotChunkV1> = None;
let mut terminal: Option<LogicalSnapshotManifestV1> = None;
export_command_loop(&mut reader, &mut next_ordinal, &mut last_chunk, &mut terminal, receiver);
```

For `expected == next`, call `reader.as_mut().expect("nonterminal reader").next_chunk()`; for exactly the cached previous ordinal, return the cached clone; gaps and older regressions fail with distinct codes. EOF takes the Reader from the Option, consumes `finish()`, caches the Manifest, marks terminal activity, and returns `ExportComplete`. Validate optional expected Applied Index before registering the session. Register only after the actor has successfully opened its Reader and returned the Header through a one-shot startup channel.

- [ ] **Step 4: Run GREEN plus Memory Adapter regressions**

Run: `cargo test -p adapter-sidecar --test export_session && cargo test -p adapter-memory`

Expected: replay/gap/capacity/abort/terminal cases pass and Memory Adapter tests remain green.

- [ ] **Step 5: Commit**

```bash
git add crates/adapter-sidecar
git commit -m "feat: serve resumable snapshot exports"
```

### Task 6: Implement the Restore Worker and Atomic Service Publication

**Files:**
- Modify: `crates/adapter-sidecar/src/session.rs`
- Modify: `crates/adapter-sidecar/src/service.rs`
- Create: `crates/adapter-sidecar/tests/restore_session.rs`

**Interfaces:**
- Consumes: `AdapterFactory::begin_restore` and `AdapterRestoreSession`.
- Implements: BeginRestore, RestoreChunk, FinishRestore, RestoreComplete, AbortSession, and Waiting→Restoring→Active/Faulted transitions.

- [ ] **Step 1: Write failing restore idempotency/publication tests**

```rust
#[test]
fn restore_is_hidden_until_manifest_and_replays_ack_and_finish() {
    let service = snapshot_restore_target();
    let started = begin_restore(&service, 300, header_at(9));
    assert_eq!(service.state(), SidecarServiceState::Restoring);
    let chunk = source_chunk(0);
    let ack = restore_chunk(&service, 301, started.session_id, chunk.clone());
    assert_eq!(restore_chunk(&service, 302, started.session_id, chunk), ack);
    assert_error_code(block_on(service.dispatch(303, Request::MultiGet(vec![]))), RemoteErrorCode::NotActive);
    let complete = finish_restore(&service, 304, started.session_id, source_manifest());
    assert_eq!(service.state(), SidecarServiceState::Active);
    assert_eq!(finish_restore(&service, 305, started.session_id, source_manifest()), complete);
}

#[test]
fn divergent_duplicate_or_bad_manifest_never_activates_target() {
    let service = snapshot_restore_target();
    let started = begin_restore(&service, 400, header_at(9));
    restore_chunk(&service, 401, started.session_id, source_chunk(0));
    let divergent = divergent_chunk_with_ordinal(0);
    assert_error_code(block_on(service.dispatch(402, Request::RestoreChunk { session_id: started.session_id, chunk: divergent })), RemoteErrorCode::ChunkDigestMismatch);
    assert_ne!(service.state(), SidecarServiceState::Active);
}
```

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-sidecar --test restore_session`

Expected: restore messages are not dispatched and target state never transitions.

- [ ] **Step 3: Implement stack-owned restore and publication fencing**

The actor stack owns `Arc<dyn AdapterFactory>`, `Arc<AdapterOpenRequest>`, and `Option<Box<dyn AdapterRestoreSession + '_>>` in that order so Finish can take and consume the session. It uses `LogicalSnapshotAccumulator` before forwarding each chunk. The latest identical `(ordinal, digest)` returns the same Ack without a backend call; same ordinal/different digest, gaps, and older regressions close the restore, drop the session, and clean the hidden target. Actor exit sends `SessionExit { session_id, cleanup_confirmed, terminal }` over the service lifecycle channel so Waiting/Faulted transitions never depend on the TCP request still being connected.

`FinishRestore` verifies the entire Manifest, calls `session.finish`, then returns the completed Adapter to `SidecarService`. Under the publication lock, the service checks the session ID/state, final Descriptor equality and capability requirement, exact Applied Index, and health; it then atomically installs Active. Any pre-finish cleanup uncertainty or post-finish contract/install failure calls:

```rust
fn fault(&self, detail: impl Into<String>) {
    *self.mode.lock().unwrap_or_else(PoisonError::into_inner) =
        ServiceMode::Faulted { detail: redact(detail.into()) };
}
```

Before creating the Worker, compare `BeginRestore.target.instance_id/parameters` byte-for-byte with the configured `AdapterOpenRequest.instance_id/public_parameters`; mismatch returns `TARGET_REQUEST_MISMATCH` and no namespace is created. Only after installation may the service cache/return `RestoreComplete`. Session commands are looked up before ordinary Active-mode dispatch so a lost Finish response can still replay its terminal result after the service is Active. A second distinct Begin while Restoring returns `RESTORE_ALREADY_IN_PROGRESS`; the same frame Request ID is served from Begin replay cache.

- [ ] **Step 4: Run GREEN and cleanup stress**

Run: `cargo test -p adapter-sidecar --test restore_session -- --test-threads=1`

Expected: Ack/Finish replay, divergence, bad Manifest, abort, expiry, second Begin, and cleanup/fault cases pass with no Active target on failure.

- [ ] **Step 5: Commit**

```bash
git add crates/adapter-sidecar
git commit -m "feat: serve atomic snapshot restores"
```

### Task 7: Add the Remote Logical Snapshot Reader

**Files:**
- Create: `crates/adapter-sidecar/src/snapshot_client.rs`
- Modify: `crates/adapter-sidecar/src/lib.rs`
- Create: `crates/adapter-sidecar/tests/snapshot_client.rs`
- Modify: `crates/adapter-sidecar/tests/client.rs`

**Interfaces:**
- Changes: `SidecarAdapter<T>` stores `Arc<T>` and negotiated `HelloResponse`.
- Produces: `impl<T: SidecarTransport + ?Sized> SidecarTransport for Arc<T>` and `SidecarAdapter::connect_shared(Arc<T>)`.
- Implements: `SidecarAdapter<T>::begin_logical_export`.
- Produces: internal `RemoteLogicalSnapshotReader<T>`.

- [ ] **Step 1: Write the failing SPI-level export test**

```rust
#[test]
fn sidecar_adapter_exports_a_canonical_snapshot_through_the_spi() {
    let backend = populated_snapshot_adapter_at_index(2);
    let service = SidecarService::active(backend, SidecarSessionConfig::default()).unwrap();
    let adapter = block_on(SidecarAdapter::connect(LoopbackTransport::from_service(service))).unwrap();
    let mut reader = block_on(adapter.begin_logical_export(LogicalSnapshotExportRequest::default())).unwrap();
    let header = reader.header().clone();
    let mut accumulator = LogicalSnapshotAccumulator::new(header.clone());
    while let Some(chunk) = block_on(reader.next_chunk()).unwrap() {
        accumulator.observe(&chunk).unwrap();
    }
    let manifest = block_on(reader.finish()).unwrap();
    accumulator.verify(&manifest).unwrap();
    assert_eq!(header.applied_log_index(), 2);
}
```

Also test a scripted server returning a wrong Session ID, wrong Ordinal, wrong Digest, or mismatched terminal Manifest; each must become `AdapterError` and trigger best-effort Abort on Drop.

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-sidecar --test snapshot_client --test client`

Expected: Sidecar Adapter reports unsupported logical export.

- [ ] **Step 3: Implement negotiation, capability intersection, and Reader**

`SidecarAdapter::connect` wraps its transport in Arc and delegates to `connect_shared`. Both send Hello requiring `BASE_ADAPTER_V1` and optionally requesting export/ordinal replay, then Describe/Health/AppliedIndex. Store `transport: Arc<T>` so the returned Reader can own a transport clone. Implement `SidecarTransport` for `Arc<T>` without changing Request IDs. `begin_logical_export` requires both export and replay features, sends BeginExport, and creates:

```rust
struct RemoteLogicalSnapshotReader<T: SidecarTransport> {
    transport: Arc<T>,
    session_id: u128,
    header: LogicalSnapshotHeaderV1,
    next_ordinal: u64,
    accumulator: LogicalSnapshotAccumulator,
    terminal: Option<LogicalSnapshotManifestV1>,
    finished: bool,
}
```

Increment `next_ordinal` only after Session ID, Ordinal, Chunk Digest, limits, and accumulator validation pass. `finish` requires an observed terminal Manifest and verifies it. Drop of an unfinished Reader performs one bounded best-effort Abort using the transport's configured timeout; it never spawns an unbounded thread.

- [ ] **Step 4: Run GREEN and all client tests**

Run: `cargo test -p adapter-sidecar --test snapshot_client --test client`

Expected: canonical export passes; every malformed remote response fails closed; old apply/read/index checks still pass.

- [ ] **Step 5: Commit**

```bash
git add crates/adapter-sidecar
git commit -m "feat: export snapshots through Sidecar clients"
```

### Task 8: Add the Dynamic Remote Adapter Factory and Restore Client

**Files:**
- Modify: `crates/adapter-sidecar/src/snapshot_client.rs`
- Modify: `crates/adapter-sidecar/src/lib.rs`
- Modify: `crates/adapter-sidecar/tests/snapshot_client.rs`
- Modify: `crates/adapter-registry/tests/registry.rs`

**Interfaces:**
- Produces: `SidecarAdapterFactory<T>::connect(provider_name: impl Into<String>, transport: T)` and `connect_shared(provider_name, Arc<T>)`.
- Implements: `AdapterFactory` and internal `RemoteAdapterRestoreSession<T>`.
- Enables: `AdapterRegistry::restore` from any logical Reader into a restore-target Sidecar.

- [ ] **Step 1: Write failing Registry-level loopback migration tests**

```rust
#[test]
fn registry_migrates_snapshot_source_into_remote_restore_target() {
    let source = populated_snapshot_adapter_at_index(3);
    let reader = block_on(source.begin_logical_export(LogicalSnapshotExportRequest::default())).unwrap();
    let service = snapshot_restore_target();
    let remote = block_on(SidecarAdapterFactory::connect(
        "memory-sidecar",
        LoopbackTransport::from_service(Arc::clone(&service)),
    )).unwrap();
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(remote)).unwrap();
    let opened = block_on(registry.restore(
        "memory-sidecar",
        &AdapterOpenRequest::new("target"),
        AdapterRequirement::Development,
        reader,
    )).unwrap();
    assert_eq!(opened.adapter().applied_log_index().unwrap(), 3);
    assert_eq!(service.state(), SidecarServiceState::Active);
}
```

Add failures for prospective Descriptor rejection before the first Chunk, final Descriptor drift, Ack mismatch, `RestoreComplete` index mismatch, a secret present in the client request never appearing in captured wire bytes, and Drop before Finish aborting the target.

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-sidecar --test snapshot_client && cargo test -p adapter-registry`

Expected: missing Sidecar Factory/Restore Session implementation.

- [ ] **Step 3: Implement the Factory and remote Restore Session**

The Factory owns a dynamic validated provider name, `Arc<T>`, and Hello result. `begin_restore` sends only `request.instance_id()` plus `request.public_parameters()`; it does not inspect or serialize `request.secret(...)`. `RestoreStarted.prospective_descriptor` is returned by `descriptor()` for Registry validation before any Chunk.

```rust
struct RemoteAdapterRestoreSession<T: SidecarTransport> {
    transport: Arc<T>,
    session_id: u128,
    descriptor: AdapterDescriptorV1,
    next_ordinal: u64,
    accumulator: LogicalSnapshotAccumulator,
    finished: bool,
}
```

`write_chunk` validates locally, sends RestoreChunk, and requires exact Session ID/Ordinal/Digest in the Ack before advancing. `finish` verifies the Manifest locally, requires exact final Descriptor and Applied Index, then calls `SidecarAdapter::connect_shared` over the shared transport and returns it as `Arc<dyn StorageAdapter>`. Implement the Factory for `T: SidecarTransport + 'static` because the returned Adapter is owned behind `Arc<dyn StorageAdapter>`. Drop before Finish sends one bounded Abort.

- [ ] **Step 4: Run GREEN and Registry conformance**

Run: `cargo test -p adapter-sidecar --test snapshot_client && cargo test -p adapter-registry`

Expected: complete loopback Registry migration succeeds and every mismatch is rejected before `OpenedAdapter` publication.

- [ ] **Step 5: Commit**

```bash
git add crates/adapter-sidecar crates/adapter-registry
git commit -m "feat: restore adapters through Sidecar factories"
```

### Task 9: Make TCP Sessions Reconnect-Safe and Shutdown-Bounded

**Files:**
- Modify: `crates/adapter-sidecar/src/lib.rs`
- Modify: `crates/adapter-sidecar/src/service.rs`
- Modify: `crates/adapter-sidecar/src/session.rs`
- Modify: `crates/adapter-sidecar/tests/tcp.rs`
- Create: `crates/adapter-sidecar/tests/tcp_snapshot.rs`

**Interfaces:**
- Produces: `serve_service_connection` and `spawn_tcp_sidecar_service`.
- Preserves: `serve_connection` and `spawn_tcp_sidecar_server` as base-compatible wrappers.
- Guarantees: the same service/session registry is shared across every TCP worker and connection.

- [ ] **Step 1: Write deterministic TCP response-loss tests**

Build a fault-injecting test server that reads a valid frame, calls `service.dispatch(frame.request_id(), frame.into_message())`, then closes before writing the selected response. Assert:

```rust
#[test]
fn lost_export_response_replays_the_same_chunk_on_another_connection() {
    let (transport, faults, service) = fault_injected_server(Fault::DropFirstExportChunkResponse);
    let adapter = block_on(SidecarAdapter::connect(transport)).unwrap();
    let mut reader = block_on(adapter.begin_logical_export(LogicalSnapshotExportRequest::default())).unwrap();
    let chunk = block_on(reader.next_chunk()).unwrap().unwrap();
    assert_eq!(chunk.ordinal(), 0);
    assert_eq!(faults.accepted_connections(), 2);
    assert_eq!(service.metrics().export_ordinal_replays, 1);
}
```

Add equivalent tests for lost BeginExport, lost Restore Ack, lost Finish response, pool size 2 switching connections, server shutdown with active export/restore actors, full session capacity, slow actor queue saturation, and no active worker after shutdown.

- [ ] **Step 2: Run RED**

Run: `cargo test -p adapter-sidecar --test tcp_snapshot -- --test-threads=1`

Expected: current server dispatch is connection/Adapter-bound and cannot preserve snapshot sessions.

- [ ] **Step 3: Route TCP through the shared service**

`serve_service_connection(stream, Arc<SidecarService>)` passes the frame Request ID into `service.dispatch`. `spawn_tcp_sidecar_service` clones the same `Arc<SidecarService>` into all fixed TCP workers. The legacy server wrapper constructs one Active service once per server, not once per connection.

Shutdown order is fixed: reject new Begin, stop accept, close active sockets, signal all actors, wait for exit acknowledgements up to the configured deadline, join actors/Reaper/TCP workers, then release Adapter/Factory. Never call `JoinHandle::join` before an exit acknowledgement because Rust thread joins have no timeout.

Expose counters through `SidecarMetricsSnapshot`: active/expired/aborted/rejected sessions, Chunk bytes/operations/cumulative latency, ordinal replay, digest failures, Begin replay hits, queue busy/cumulative wait, publish success/failure, Reaper cleanup count/cumulative latency, and join timeouts. Counters contain no IDs or user keys.

- [ ] **Step 4: Run GREEN repeatedly and keep base TCP tests green**

Run: `for run in 1 2 3 4 5; do cargo test -p adapter-sidecar --test tcp_snapshot -- --test-threads=1 || exit 1; done && cargo test -p adapter-sidecar --test tcp`

Expected: five deterministic fault runs and all legacy TCP tests pass; no hang exceeds the test's explicit deadline.

- [ ] **Step 5: Commit**

```bash
git add crates/adapter-sidecar
git commit -m "feat: make Sidecar snapshot sessions reconnect-safe"
```

### Task 10: Add PostgreSQL Serve and Restore-Target Modes

**Files:**
- Modify: `crates/adapter-postgres/src/lib.rs`
- Create: `crates/adapter-postgres/src/sidecar.rs`
- Modify: `crates/adapter-postgres/src/bin/dtgproxy-postgres-sidecar.rs`
- Create: `crates/adapter-postgres/tests/sidecar_config.rs`
- Create: `crates/adapter-postgres/tests/live_sidecar.rs`

**Interfaces:**
- Produces: `PostgresSidecarMode::{Serve, RestoreTarget}`.
- Produces: `PostgresSidecarConfig::from_values(&BTreeMap<String, String>)` and `build_service(&self, SidecarSessionConfig)`.
- Environment: `DTGPROXY_MODE=serve|restore-target` (absent means `serve` for backward compatibility); existing URL, instance, listen, and pool-size variables remain.

- [ ] **Step 1: Write failing secret-safe config/mode tests**

```rust
#[test]
fn serve_and_restore_target_build_distinct_service_modes() {
    let serve = PostgresSidecarConfig::from_values(values("serve")).unwrap();
    let restore = PostgresSidecarConfig::from_values(values("restore-target")).unwrap();
    assert_eq!(serve.mode(), PostgresSidecarMode::Serve);
    assert_eq!(restore.mode(), PostgresSidecarMode::RestoreTarget);
    assert!(!format!("{restore:?}").contains("postgres://secret"));
}

#[test]
fn unauthenticated_listener_and_unknown_mode_fail_closed() {
    assert!(PostgresSidecarConfig::from_values(values_with_listen("serve", "0.0.0.0:9711")).is_err());
    assert!(PostgresSidecarConfig::from_values(values("unknown")).is_err());
}
```

The live test starts a restore-target service on `127.0.0.1:0`, migrates a real RocksDB source through `SidecarAdapterFactory`, then applies the next log entry to the returned remote Adapter and verifies it directly in PostgreSQL.

- [ ] **Step 2: Run RED and compile the ignored live test**

Run: `cargo test -p adapter-postgres --test sidecar_config && cargo test -p adapter-postgres --test live_sidecar --no-run`

Expected: config module/types are missing; the live test may also reveal missing service constructors.

- [ ] **Step 3: Implement mode parsing and service construction**

`serve` opens `PostgresAdapter` from the locally held URL/instance/pool values, validates `HotPluggableReplica`, and builds `SidecarService::active`. `restore-target` creates an owned `AdapterOpenRequest` with the local `connection_string` Secret and `pool_size` public parameter but does not call open; it builds `SidecarService::restore_target(Arc::new(PostgresAdapterFactory), Arc::new(request), config)`.

The binary reduces to parse environment → build service → `spawn_tcp_sidecar_service` → park. It prints only mode and bound address. It never prints the URL, request Debug value, Session ID, or Secret.

- [ ] **Step 4: Run GREEN, real PostgreSQL migration, and cross-backend checks**

Run static tests:

```bash
cargo test -p adapter-postgres --test sidecar_config
cargo test -p adapter-postgres --test live_sidecar --no-run
```

Run against a disposable PostgreSQL database:

```bash
DTGPROXY_POSTGRES_URL='host=127.0.0.1 user=dtgproxy password=dtgproxy dbname=dtgproxy' \
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p adapter-postgres --test live_sidecar -- --ignored --test-threads=1
```

Expected: RocksDB→PostgreSQL and PostgreSQL→RocksDB over real TCP preserve every Keyspace/Key/Value and exact Applied Index; bad Manifest leaves `published=false`/cleaned; next Raft log Apply succeeds only on the published Active target.

- [ ] **Step 5: Commit**

```bash
git add crates/adapter-postgres
git commit -m "feat: add PostgreSQL Sidecar restore targets"
```

### Task 11: Documentation, Full Verification, and Certification Gate

**Files:**
- Modify: `docs/adapter-spi.md`
- Modify: `docs/postgresql-adapter.md`
- Modify: `docs/superpowers/plans/2026-07-18-sidecar-resumable-snapshot-sessions.md`

**Interfaces:**
- Produces: exact operator modes, protocol limits, error/retry contract, metrics, crash-window procedure, and test evidence.

- [ ] **Step 1: Update operator and SPI documentation**

Document all exact defaults and hard caps, feature bits, error codes, `serve`/`restore-target` environment, loopback restriction, source-not-cutover rule, post-finish quarantine procedure, and the commands used for live certification. Replace the current sentence saying remote snapshot sessions are missing with the verified status only after its corresponding test passes.

- [ ] **Step 2: Run formatting and focused suites**

```bash
cargo fmt --all -- --check
cargo test -p adapter-registry
cargo test -p adapter-sidecar -- --test-threads=1
cargo test -p adapter-postgres --test sql_contract --test sidecar_config
```

Expected: zero failures and zero formatting diff.

- [ ] **Step 3: Run complete workspace verification**

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test --workspace --all-targets

CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo clippy --workspace --all-targets -- -D warnings
```

Expected: all non-ignored tests pass and strict Clippy exits 0. Read the complete output; do not infer workspace success from a focused package run.

- [ ] **Step 4: Run release-gate audit**

```bash
rg -n 'T[B]D|T[O]DO|F[I]XME|unimplemented!|todo!|panic!\("not implemented' crates/adapter-sidecar crates/adapter-postgres docs/adapter-spi.md docs/postgresql-adapter.md
git diff --check
git status --short
```

Expected: no implementation/documentation placeholders, no whitespace errors, and only the intended documentation/plan evidence changes before the final commit. PostgreSQL remote migration may be called certified only when the ignored live command in Task 10 has passed on a disposable server.

- [ ] **Step 5: Commit verified evidence**

```bash
git add docs/adapter-spi.md docs/postgresql-adapter.md docs/superpowers/plans/2026-07-18-sidecar-resumable-snapshot-sessions.md
git commit -m "docs: certify Sidecar snapshot migration"
```

## Completion Evidence Matrix

| Approved requirement | Authoritative evidence |
|---|---|
| Hello and feature/capability intersection | `snapshot_protocol.rs`, `service_state.rs` |
| 20 MiB pre-allocation frame bound | `protocol.rs`, `snapshot_protocol.rs` |
| Global reconnect-safe sessions | `tcp_snapshot.rs` response-loss/pool-switch tests |
| Borrow-safe Worker ownership | `session.rs` actor implementation plus `#![forbid(unsafe_code)]` and Clippy |
| Begin/Chunk/Finish idempotency | `export_session.rs`, `restore_session.rs`, `tcp_snapshot.rs` |
| Hidden restore and atomic service publication | `restore_session.rs`, PostgreSQL `live_sidecar.rs` |
| Bounded sessions/queues/caches/TTL/Reaper | `session_lifecycle.rs`, metrics assertions |
| Secret-safe public target request | protocol capture test and `sidecar_config.rs` Debug assertion |
| Remote source SPI | `snapshot_client.rs` export accumulator test |
| Remote target Factory SPI | Registry-level loopback and live TCP migration tests |
| Shutdown and crash-window behavior | `tcp_snapshot.rs`, PostgreSQL residue/quarantine cases |
| No base Sidecar regression | original `client.rs`, `protocol.rs`, `tcp.rs` |
| PostgreSQL remote certification | real ignored `live_sidecar` command output |
| Workspace stability | full workspace tests, fmt, strict Clippy, clean diff audit |
