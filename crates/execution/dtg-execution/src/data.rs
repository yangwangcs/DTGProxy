use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use dtg_shard::{
    RaftProgress, ReplicaKey, ReplicaObservation, ShardCommand, ShardError, ShardHost,
};
use dtg_snapshot_csr::{
    CommittedAdjacencyOperation, CommittedCsrOverlay, CommittedGraphDelta, CsrDirection,
    SnapshotCsr, SnapshotCsrBuildBudget, SnapshotCsrError, SnapshotCsrKey,
};
use dtg_storage::{
    AdjacencyDirection, AdjacencyRead, ChangeCursor, ChangesRead, ConsensusStore, EdgeScan,
    LogicalMutation, LogicalReplicaActivation, LogicalSnapshotSink, NamespaceId, ProviderKind,
    PushdownExecutor, ReadFence, ReplicaBinding, ReplicaStateStore, StorageError, StoreFuture,
    TemporalReadView, TransactionTime, VertexId, VertexRead, VertexScan,
};
use prost_011::Message as _;
use raft::eraftpb::Message;
use tokio::sync::watch;

use crate::{
    GatewayExecutionError, GatewayFuture, GatewayRetry, GatewayRows, GatewayValue, RequestDetail,
    RequestStageMetrics, StageOutcome,
};

const PARTIAL_VERTEX_COUNT_FIELD: &str = "__dtg_partial_vertex_count";
const MAX_CACHED_ADJACENCY_BYTES_PER_REPLICA: usize = 1024 * 1024;
const MAX_CACHED_ADJACENCY_SCOPES_PER_REPLICA: usize = 64;
const MAX_SNAPSHOT_CSR_BYTES_PER_REPLICA: usize = 1024 * 1024;
const MAX_CACHED_SNAPSHOT_CSR_BYTES_PER_REPLICA: usize = 2 * MAX_SNAPSHOT_CSR_BYTES_PER_REPLICA;
const MAX_CACHED_SNAPSHOT_CSR_IMAGES_PER_REPLICA: usize = 2;
const SNAPSHOT_CSR_EDGE_PAGE_SIZE: u32 = 1024;
const MAX_SNAPSHOT_CSR_EDGES: usize = 8 * 1024;
const SNAPSHOT_CSR_OVERLAY_CHANGE_PAGE_SIZE: u32 = 1024;
const MAX_SNAPSHOT_CSR_OVERLAY_CHANGES: usize = 8 * 1024;
const MAX_SNAPSHOT_CSR_OVERLAY_BYTES: usize = 256 * 1024;
const MIN_CSR_TRAVERSAL_ROW_BOUND: u32 = 64;

#[derive(Clone)]
pub struct ResolvedReplicaStore {
    state: Arc<dyn ReplicaStateStore>,
    snapshot_sink: Option<Arc<dyn LogicalSnapshotSink>>,
    activation: Option<Arc<dyn LogicalReplicaActivation>>,
    pushdown: Option<Arc<dyn PushdownExecutor>>,
}

impl ResolvedReplicaStore {
    pub fn state_only(state: Arc<dyn ReplicaStateStore>) -> Self {
        Self {
            state,
            snapshot_sink: None,
            activation: None,
            pushdown: None,
        }
    }

    pub fn with_snapshot_runtime(
        mut self,
        sink: Arc<dyn LogicalSnapshotSink>,
        activation: Arc<dyn LogicalReplicaActivation>,
    ) -> Self {
        self.snapshot_sink = Some(sink);
        self.activation = Some(activation);
        self
    }

    pub fn with_pushdown(mut self, pushdown: Arc<dyn PushdownExecutor>) -> Self {
        self.pushdown = Some(pushdown);
        self
    }

    pub const fn state(&self) -> &Arc<dyn ReplicaStateStore> {
        &self.state
    }

    pub const fn snapshot_sink(&self) -> Option<&Arc<dyn LogicalSnapshotSink>> {
        self.snapshot_sink.as_ref()
    }

    pub const fn activation(&self) -> Option<&Arc<dyn LogicalReplicaActivation>> {
        self.activation.as_ref()
    }

    pub const fn pushdown(&self) -> Option<&Arc<dyn PushdownExecutor>> {
        self.pushdown.as_ref()
    }
}

pub trait ProviderResolver: Send + Sync {
    fn provider_kind(&self) -> ProviderKind;

    fn open<'a>(&'a self, binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>>;

    fn open_runtime<'a>(
        &'a self,
        binding: ReplicaBinding,
    ) -> StoreFuture<'a, ResolvedReplicaStore> {
        Box::pin(async move {
            self.open(binding)
                .await
                .map(ResolvedReplicaStore::state_only)
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionBuildError {
    MissingComponent(&'static str),
    DuplicateProvider(ProviderKind),
    ProviderKindDrift {
        registered: ProviderKind,
        resolver: ProviderKind,
    },
}

impl fmt::Display for ExecutionBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingComponent(component) => {
                write!(formatter, "missing execution component: {component}")
            }
            Self::DuplicateProvider(kind) => {
                write!(formatter, "duplicate provider resolver: {kind:?}")
            }
            Self::ProviderKindDrift {
                registered,
                resolver,
            } => write!(
                formatter,
                "provider resolver kind drift: registered {registered:?}, resolver {resolver:?}"
            ),
        }
    }
}

impl std::error::Error for ExecutionBuildError {}

#[derive(Default)]
pub struct ProviderResolverSet {
    resolvers: BTreeMap<ProviderKind, Arc<dyn ProviderResolver>>,
    namespace_owners: Mutex<BTreeMap<NamespaceId, ReplicaBinding>>,
}

impl ProviderResolverSet {
    pub fn new() -> Self {
        Self::default()
    }

    fn insert(
        &mut self,
        kind: ProviderKind,
        resolver: Arc<dyn ProviderResolver>,
    ) -> Result<(), ExecutionBuildError> {
        let resolver_kind = resolver.provider_kind();
        if resolver_kind != kind {
            return Err(ExecutionBuildError::ProviderKindDrift {
                registered: kind,
                resolver: resolver_kind,
            });
        }
        if self.resolvers.contains_key(&kind) {
            return Err(ExecutionBuildError::DuplicateProvider(kind));
        }
        self.resolvers.insert(kind, resolver);
        Ok(())
    }

    pub fn provider_kinds(&self) -> Vec<ProviderKind> {
        self.resolvers.keys().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.resolvers.is_empty()
    }

    pub fn open(&self, binding: ReplicaBinding) -> StoreFuture<'_, Arc<dyn ReplicaStateStore>> {
        Box::pin(async move { Ok(self.open_runtime(binding).await?.state().clone()) })
    }

    pub fn open_runtime(&self, binding: ReplicaBinding) -> StoreFuture<'_, ResolvedReplicaStore> {
        Box::pin(async move {
            let provider_kind = binding.provider_kind().clone();
            let resolver = self
                .resolvers
                .get(&provider_kind)
                .ok_or(StorageError::Unsupported)?;
            if resolver.provider_kind() != provider_kind {
                return Err(StorageError::InvalidBinding(
                    "provider resolver kind changed after registration".into(),
                ));
            }

            let store = resolver.open_runtime(binding.clone()).await?;
            if store.state().binding() != &binding {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(binding),
                    actual: Box::new(store.state().binding().clone()),
                });
            }

            let mut owners = self.namespace_owners.lock().map_err(|_| {
                StorageError::Internal("namespace ownership registry is poisoned".into())
            })?;
            match owners.get(binding.namespace_id()) {
                Some(owner) if owner != &binding => {
                    return Err(StorageError::NamespaceOwnerMismatch {
                        expected: Box::new(owner.clone()),
                        actual: Box::new(binding),
                    });
                }
                Some(_) => {}
                None => {
                    owners.insert(binding.namespace_id().clone(), binding);
                }
            }
            Ok(store)
        })
    }
}

pub struct DataExecution {
    shards: Mutex<ShardHost>,
    replica_routes: RwLock<BTreeMap<ReplicaRoute, ReplicaKey>>,
    route_lifecycle: RwLock<()>,
    providers: ProviderResolverSet,
    stores: Mutex<BTreeMap<ReplicaKey, ResolvedReplicaStore>>,
    read_views: Mutex<BTreeMap<ReplicaKey, CachedReadView>>,
    vertex_counts: Mutex<BTreeMap<ReplicaKey, CachedVertexCount>>,
    vertex_count_flights: Mutex<BTreeMap<ReplicaKey, VertexCountFlight>>,
    adjacencies: Mutex<BTreeMap<ReplicaKey, CachedAdjacencies>>,
    adjacency_flights: Mutex<BTreeMap<ReplicaKey, BTreeMap<AdjacencyScope, AdjacencyFlight>>>,
    snapshot_csrs: Mutex<BTreeMap<ReplicaKey, CachedSnapshotCsrs>>,
    snapshot_csr_flights: Mutex<BTreeMap<ReplicaKey, BTreeMap<SnapshotCsrKey, SnapshotCsrFlight>>>,
    request_metrics: Arc<RequestStageMetrics>,
}

pub struct ReplicaLookup {
    key: ReplicaKey,
    lock_wait_nanoseconds: u64,
    lookup_nanoseconds: u64,
    cache_hit: bool,
}

impl ReplicaLookup {
    pub const fn key(&self) -> ReplicaKey {
        self.key
    }

    pub const fn lock_wait_nanoseconds(&self) -> u64 {
        self.lock_wait_nanoseconds
    }

    pub const fn lookup_nanoseconds(&self) -> u64 {
        self.lookup_nanoseconds
    }

    pub const fn cache_hit(&self) -> bool {
        self.cache_hit
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ReplicaRoute {
    cluster_id: dtg_storage::ClusterId,
    graph_id: dtg_storage::GraphId,
    shard_id: dtg_storage::ShardId,
    placement_epoch: dtg_storage::PlacementEpoch,
    backend_generation: dtg_storage::BackendGeneration,
    replica_id: Option<dtg_storage::ReplicaId>,
}

impl ReplicaRoute {
    const fn new(
        cluster_id: dtg_storage::ClusterId,
        graph_id: dtg_storage::GraphId,
        shard_id: dtg_storage::ShardId,
        placement_epoch: dtg_storage::PlacementEpoch,
        backend_generation: dtg_storage::BackendGeneration,
        replica_id: Option<dtg_storage::ReplicaId>,
    ) -> Self {
        Self {
            cluster_id,
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
        }
    }

    const fn from_binding(
        binding: &ReplicaBinding,
        replica_id: Option<dtg_storage::ReplicaId>,
    ) -> Self {
        Self::new(
            binding.cluster_id(),
            binding.graph_id(),
            binding.shard_id(),
            binding.placement_epoch(),
            binding.backend_generation(),
            replica_id,
        )
    }

    const fn without_replica(self) -> Self {
        Self {
            replica_id: None,
            ..self
        }
    }
}

pub struct RaftApplyTiming {
    progress: RaftProgress,
    lock_wait_nanoseconds: u64,
    propose_nanoseconds: u64,
    drive_ready_nanoseconds: u64,
}

impl RaftApplyTiming {
    pub const fn progress(&self) -> &RaftProgress {
        &self.progress
    }

    pub const fn lock_wait_nanoseconds(&self) -> u64 {
        self.lock_wait_nanoseconds
    }

    pub const fn propose_nanoseconds(&self) -> u64 {
        self.propose_nanoseconds
    }

    pub const fn drive_ready_nanoseconds(&self) -> u64 {
        self.drive_ready_nanoseconds
    }

    pub fn into_progress(self) -> RaftProgress {
        self.progress
    }
}

struct CachedReadView {
    store: Arc<dyn ReplicaStateStore>,
    fence: ReadFence,
    view: Arc<dyn TemporalReadView>,
}

#[derive(Clone, Eq, PartialEq)]
struct VertexCountScope {
    transaction_time: TransactionTime,
    valid_at: i64,
    after: Option<VertexId>,
    limit: u32,
}

struct CachedVertexCount {
    store: Arc<dyn ReplicaStateStore>,
    fence: ReadFence,
    scope: VertexCountScope,
    count: i64,
}

struct VertexCountFlight {
    store: Arc<dyn ReplicaStateStore>,
    fence: ReadFence,
    scope: VertexCountScope,
    completed: watch::Sender<()>,
}

enum VertexCountFlightLease {
    Owner,
    Waiter(watch::Receiver<()>),
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct AdjacencyScope {
    vertex_id: VertexId,
    direction: u8,
    transaction_time: TransactionTime,
    valid_at: i64,
    limit: u32,
}

struct TraversalScope<'a> {
    vertex_id: VertexId,
    directions: &'a [AdjacencyDirection],
    transaction_time: TransactionTime,
    valid_at: i64,
    limit: u32,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum GraphAccessPath {
    SnapshotCsr,
    BoundedAdjacency,
}

fn select_graph_access_path(scope: &TraversalScope<'_>) -> GraphAccessPath {
    if !scope.directions.is_empty()
        && scope.limit >= MIN_CSR_TRAVERSAL_ROW_BOUND
        && scope
            .directions
            .iter()
            .all(|direction| *direction != AdjacencyDirection::Both)
    {
        GraphAccessPath::SnapshotCsr
    } else {
        GraphAccessPath::BoundedAdjacency
    }
}

struct CachedAdjacencies {
    store: Arc<dyn ReplicaStateStore>,
    fence: ReadFence,
    bytes: usize,
    entries: BTreeMap<AdjacencyScope, Vec<dtg_storage::EdgeVersion>>,
}

struct CachedSnapshotCsrs {
    store: Arc<dyn ReplicaStateStore>,
    bytes: usize,
    entries: BTreeMap<SnapshotCsrKey, Arc<SnapshotCsr>>,
}

struct SnapshotCsrFlight {
    store: Arc<dyn ReplicaStateStore>,
    fence: ReadFence,
    completed: watch::Sender<()>,
}

enum SnapshotCsrFlightLease {
    Owner,
    Waiter(watch::Receiver<()>),
}

struct AdjacencyFlight {
    store: Arc<dyn ReplicaStateStore>,
    fence: ReadFence,
    completed: watch::Sender<()>,
}

enum AdjacencyFlightLease {
    Owner,
    Waiter(watch::Receiver<()>),
}

impl DataExecution {
    pub fn builder() -> DataExecutionBuilder {
        DataExecutionBuilder::default()
    }

    pub fn request_metrics(&self) -> Arc<RequestStageMetrics> {
        Arc::clone(&self.request_metrics)
    }

    pub fn provider_kinds(&self) -> Vec<ProviderKind> {
        self.providers.provider_kinds()
    }

    pub fn open_store(
        &self,
        binding: ReplicaBinding,
    ) -> StoreFuture<'_, Arc<dyn ReplicaStateStore>> {
        self.providers.open(binding)
    }

    pub fn open_runtime_store(
        &self,
        binding: ReplicaBinding,
    ) -> StoreFuture<'_, ResolvedReplicaStore> {
        self.providers.open_runtime(binding)
    }

    pub fn add_replica(
        &self,
        consensus_store: Arc<dyn ConsensusStore>,
        state_store: Arc<dyn ReplicaStateStore>,
    ) -> Result<ReplicaKey, ShardError> {
        let _lifecycle = self.route_lifecycle.write().map_err(|_| {
            ShardError::InvalidLifecycle("replica route lifecycle is poisoned".into())
        })?;
        let binding = state_store.binding().clone();
        let key = self.lock_shards()?.add(consensus_store, state_store)?;
        self.register_replica_route(key, &binding)?;
        self.clear_read_view(key)?;
        Ok(key)
    }

    pub fn add_replica_runtime(
        &self,
        consensus_store: Arc<dyn ConsensusStore>,
        runtime_store: ResolvedReplicaStore,
    ) -> Result<ReplicaKey, ShardError> {
        let _lifecycle = self.route_lifecycle.write().map_err(|_| {
            ShardError::InvalidLifecycle("replica route lifecycle is poisoned".into())
        })?;
        let binding = runtime_store.state().binding().clone();
        let key = self
            .lock_shards()?
            .add(consensus_store, runtime_store.state().clone())?;
        self.stores
            .lock()
            .map_err(|_| ShardError::InvalidLifecycle("replica store mutex is poisoned".into()))?
            .insert(key, runtime_store);
        self.register_replica_route(key, &binding)?;
        self.clear_read_view(key)?;
        Ok(key)
    }

    pub fn start_replica(&self, key: ReplicaKey) -> Result<(), ShardError> {
        self.lock_shards()?.start(key)
    }

    pub fn campaign_replica(&self, key: ReplicaKey) -> Result<(), ShardError> {
        self.lock_shards()?.campaign(key)
    }

    pub fn tick_replica(&self, key: ReplicaKey) -> Result<bool, ShardError> {
        self.lock_shards()?.tick(key)
    }

    pub fn step_replica(&self, key: ReplicaKey, message: Message) -> Result<(), ShardError> {
        self.lock_shards()?.step(key, message)
    }

    pub fn drive_replica(&self, key: ReplicaKey) -> Result<RaftProgress, ShardError> {
        self.lock_shards()?.drive_ready(key)
    }

    pub fn remove_replica(&self, key: ReplicaKey) -> Result<RaftProgress, ShardError> {
        let _lifecycle = self.route_lifecycle.write().map_err(|_| {
            ShardError::InvalidLifecycle("replica route lifecycle is poisoned".into())
        })?;
        let progress = {
            let mut shards = self.lock_shards()?;
            let progress = shards.drive_ready(key)?;
            if !progress.messages().is_empty() {
                return Err(ShardError::InvalidLifecycle(
                    "replica removal requires outbound Raft messages to be delivered first".into(),
                ));
            }
            shards.stop(key)?;
            shards.seal(key)?;
            shards.sealed_remove(key)?;
            progress
        };
        self.unregister_replica_route(key)?;
        self.stores
            .lock()
            .map_err(|_| ShardError::InvalidLifecycle("replica store mutex is poisoned".into()))?
            .remove(&key);
        self.clear_read_view(key)?;
        Ok(progress)
    }

    pub fn replica_keys(&self) -> Vec<ReplicaKey> {
        self.lock_shards()
            .map_or_else(|_| Vec::new(), |shards| shards.keys())
    }

    pub fn replica_observations(&self) -> Vec<ReplicaObservation> {
        self.lock_shards()
            .map_or_else(|_| Vec::new(), |shards| shards.observations())
    }

    pub fn replica_observation(&self, key: ReplicaKey) -> Result<ReplicaObservation, ShardError> {
        self.lock_shards()?.observe(key)
    }

    pub fn locate_replica(
        &self,
        cluster_id: dtg_storage::ClusterId,
        graph_id: dtg_storage::GraphId,
        shard_id: dtg_storage::ShardId,
        placement_epoch: dtg_storage::PlacementEpoch,
        backend_generation: dtg_storage::BackendGeneration,
        replica_id: Option<dtg_storage::ReplicaId>,
    ) -> Result<ReplicaKey, ShardError> {
        self.locate_replica_timed(
            cluster_id,
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
        )
        .map(|lookup| lookup.key)
    }

    pub fn locate_replica_timed(
        &self,
        cluster_id: dtg_storage::ClusterId,
        graph_id: dtg_storage::GraphId,
        shard_id: dtg_storage::ShardId,
        placement_epoch: dtg_storage::PlacementEpoch,
        backend_generation: dtg_storage::BackendGeneration,
        replica_id: Option<dtg_storage::ReplicaId>,
    ) -> Result<ReplicaLookup, ShardError> {
        let _lifecycle = self.route_lifecycle.read().map_err(|_| {
            ShardError::InvalidLifecycle("replica route lifecycle is poisoned".into())
        })?;
        let route = ReplicaRoute::new(
            cluster_id,
            graph_id,
            shard_id,
            placement_epoch,
            backend_generation,
            replica_id,
        );
        let lookup_started = Instant::now();
        if let Some(key) = self
            .replica_routes
            .read()
            .map_err(|_| ShardError::InvalidLifecycle("replica route cache is poisoned".into()))?
            .get(&route)
            .copied()
        {
            return Ok(ReplicaLookup {
                key,
                lock_wait_nanoseconds: 0,
                lookup_nanoseconds: elapsed_nanoseconds(lookup_started),
                cache_hit: true,
            });
        }
        let lock_started = Instant::now();
        let shards = self.lock_shards()?;
        let lock_wait_nanoseconds = elapsed_nanoseconds(lock_started);
        let mut matches = shards
            .observations()
            .into_iter()
            .filter(|observation| {
                let binding = observation.binding();
                binding.cluster_id() == cluster_id
                    && binding.graph_id() == graph_id
                    && binding.shard_id() == shard_id
                    && binding.placement_epoch() == placement_epoch
                    && binding.backend_generation() == backend_generation
                    && replica_id.is_none_or(|replica_id| binding.replica_id() == replica_id)
            })
            .map(|observation| {
                let binding = observation.binding();
                ReplicaKey::new(
                    binding.cluster_id(),
                    binding.graph_id(),
                    binding.shard_id(),
                    binding.replica_id(),
                )
            });
        let key = matches.next().ok_or(ShardError::ReplicaNotFound)?;
        if matches.next().is_some() {
            return Err(ShardError::InvalidCommand(
                "request fence resolves to multiple local replicas".into(),
            ));
        }
        Ok(ReplicaLookup {
            key,
            lock_wait_nanoseconds,
            lookup_nanoseconds: elapsed_nanoseconds(lookup_started),
            cache_hit: false,
        })
    }

    pub fn apply_transaction_command(
        &self,
        key: ReplicaKey,
        command: ShardCommand,
    ) -> Result<RaftProgress, ShardError> {
        self.apply_transaction_command_timed(key, command)
            .map(RaftApplyTiming::into_progress)
    }

    pub fn apply_transaction_command_timed(
        &self,
        key: ReplicaKey,
        command: ShardCommand,
    ) -> Result<RaftApplyTiming, ShardError> {
        self.apply_transaction_commands_timed(key, vec![command])
    }

    pub fn apply_transaction_commands_timed(
        &self,
        key: ReplicaKey,
        commands: Vec<ShardCommand>,
    ) -> Result<RaftApplyTiming, ShardError> {
        if commands.is_empty() {
            return Err(ShardError::InvalidCommand(
                "Raft apply batch must contain at least one command".into(),
            ));
        }
        let lock_started = Instant::now();
        let mut shards = self.lock_shards()?;
        let lock_wait_nanoseconds = elapsed_nanoseconds(lock_started);
        let propose_started = Instant::now();
        for command in commands {
            shards.propose(key, command)?;
        }
        let propose_nanoseconds = elapsed_nanoseconds(propose_started);
        let drive_ready_started = Instant::now();
        let progress = shards.drive_ready(key)?;
        Ok(RaftApplyTiming {
            progress,
            lock_wait_nanoseconds,
            propose_nanoseconds,
            drive_ready_nanoseconds: elapsed_nanoseconds(drive_ready_started),
        })
    }

    pub fn receive_raft_message(
        &self,
        key: ReplicaKey,
        encoded: &[u8],
        expected_from: dtg_storage::ReplicaId,
        expected_to: dtg_storage::ReplicaId,
        expected_term: u64,
    ) -> Result<RaftProgress, ShardError> {
        let message = Message::decode(encoded).map_err(|error| {
            ShardError::InvalidRaftState(format!("invalid Raft message encoding: {error}"))
        })?;
        if message.from != expected_from.get()
            || message.to != expected_to.get()
            || message.term != expected_term
        {
            return Err(ShardError::InvalidRaftState(
                "Raft envelope and encoded message fences differ".into(),
            ));
        }
        let mut shards = self.lock_shards()?;
        shards.step(key, message)?;
        shards.drive_ready(key)
    }

    pub fn execute_fragment<'a>(
        &'a self,
        key: ReplicaKey,
        applied_index: u64,
        transaction_time: TransactionTime,
        valid_at: i64,
        encoded: &'a [u8],
    ) -> GatewayFuture<'a, Result<GatewayRows, GatewayExecutionError>> {
        Box::pin(async move {
            let store = self.runtime_store(key)?;
            let binding = store.state().binding().clone();
            let read = decode_fragment_read(encoded, transaction_time, valid_at)?;
            let fence = ReadFence::new(binding, applied_index);
            let view = self
                .read_view(key, store.state().clone(), fence.clone())
                .await?;
            let diagnostics_before = view.diagnostics();
            let scan = matches!(
                &read,
                FragmentRead::Scan { .. }
                    | FragmentRead::Count { .. }
                    | FragmentRead::Adjacency { .. }
                    | FragmentRead::Traversal { .. }
            );
            let (field, rows) = match read {
                FragmentRead::Point(id) => (
                    "value",
                    view.get_vertex(VertexRead::new(id, valid_at, transaction_time))
                        .await
                        .map_err(data_storage_error)?
                        .into_iter()
                        .map(|vertex| vertex_row(&vertex))
                        .collect::<Vec<_>>(),
                ),
                FragmentRead::Scan { after, limit } => (
                    "value",
                    view.scan_vertices(
                        VertexScan::new(valid_at, transaction_time, after, limit)
                            .map_err(data_storage_error)?,
                    )
                    .await
                    .map_err(data_storage_error)?
                    .rows()
                    .iter()
                    .map(vertex_row)
                    .collect::<Vec<_>>(),
                ),
                FragmentRead::Count { after, limit } => {
                    let scope = VertexCountScope {
                        transaction_time,
                        valid_at,
                        after,
                        limit,
                    };
                    let count = self
                        .vertex_count(key, store.state(), &fence, &view, scope)
                        .await?;
                    (
                        PARTIAL_VERTEX_COUNT_FIELD,
                        vec![vec![GatewayValue::Integer(count)]],
                    )
                }
                FragmentRead::Adjacency {
                    vertex_id,
                    direction,
                    limit,
                } => (
                    "value",
                    self.execute_traversal(
                        key,
                        store.state(),
                        &fence,
                        &view,
                        TraversalScope {
                            vertex_id,
                            directions: std::slice::from_ref(&direction),
                            transaction_time,
                            valid_at,
                            limit,
                        },
                    )
                    .await?
                    .iter()
                    .map(edge_row)
                    .collect::<Vec<_>>(),
                ),
                FragmentRead::Traversal {
                    vertex_id,
                    directions,
                    limit,
                } => (
                    "value",
                    self.execute_traversal(
                        key,
                        store.state(),
                        &fence,
                        &view,
                        TraversalScope {
                            vertex_id,
                            directions: &directions,
                            transaction_time,
                            valid_at,
                            limit,
                        },
                    )
                    .await?
                    .iter()
                    .map(edge_row)
                    .collect::<Vec<_>>(),
                ),
            };
            self.record_temporal_read_view_diagnostics(
                diagnostics_before,
                view.diagnostics(),
                scan,
            );
            GatewayRows::new(vec![field.into()], rows)
        })
    }

    fn runtime_store(
        &self,
        key: ReplicaKey,
    ) -> Result<ResolvedReplicaStore, GatewayExecutionError> {
        self.stores
            .lock()
            .map_err(|_| data_error("replica store mutex is poisoned"))?
            .get(&key)
            .cloned()
            .ok_or_else(|| data_error("replica runtime store is absent"))
    }

    fn read_view<'a>(
        &'a self,
        key: ReplicaKey,
        store: Arc<dyn ReplicaStateStore>,
        fence: ReadFence,
    ) -> GatewayFuture<'a, Result<Arc<dyn TemporalReadView>, GatewayExecutionError>> {
        Box::pin(async move {
            let cache_started = Instant::now();
            if let Some(view) = self.cached_read_view(key, &store, &fence)?
                && store.applied_index().await.map_err(data_storage_error)? == fence.applied_index()
            {
                self.request_metrics.record_detail(
                    RequestDetail::DataReadViewCacheHit,
                    StageOutcome::Success,
                    elapsed_nanoseconds(cache_started),
                );
                return Ok(view);
            }

            self.request_metrics.record_detail(
                RequestDetail::DataReadViewCacheMiss,
                StageOutcome::Success,
                elapsed_nanoseconds(cache_started),
            );

            let open_started = Instant::now();
            let opened = store
                .begin_read_view(fence.clone())
                .await
                .map_err(data_storage_error)?;
            self.request_metrics.record_detail(
                RequestDetail::DataReadViewOpen,
                StageOutcome::Success,
                elapsed_nanoseconds(open_started),
            );
            if opened.fence() != &fence {
                return Err(data_error(
                    "provider returned a read view for a different read fence",
                ));
            }
            let opened: Arc<dyn TemporalReadView> = Arc::from(opened);

            if store.applied_index().await.map_err(data_storage_error)? != fence.applied_index() {
                return Ok(opened);
            }

            // Holding these in lifecycle order prevents a removed store from repopulating the
            // cache after a replacement has cleared its entry.
            let stores = self
                .stores
                .lock()
                .map_err(|_| data_error("replica store mutex is poisoned"))?;
            if !stores
                .get(&key)
                .is_some_and(|runtime| Arc::ptr_eq(runtime.state(), &store))
            {
                return Err(data_error(
                    "replica runtime store changed while opening read view",
                ));
            }
            let mut read_views = self
                .read_views
                .lock()
                .map_err(|_| data_error("read view cache mutex is poisoned"))?;
            if let Some(cached) = read_views.get(&key)
                && Arc::ptr_eq(&cached.store, &store)
                && cached.fence == fence
            {
                return Ok(Arc::clone(&cached.view));
            }
            if read_views.get(&key).is_some_and(|cached| {
                Arc::ptr_eq(&cached.store, &store)
                    && cached.fence.binding() == fence.binding()
                    && cached.fence.applied_index() > fence.applied_index()
            }) {
                return Ok(opened);
            }
            read_views.insert(
                key,
                CachedReadView {
                    store,
                    fence,
                    view: Arc::clone(&opened),
                },
            );
            Ok(opened)
        })
    }

    fn cached_read_view(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
    ) -> Result<Option<Arc<dyn TemporalReadView>>, GatewayExecutionError> {
        let read_views = self
            .read_views
            .lock()
            .map_err(|_| data_error("read view cache mutex is poisoned"))?;
        Ok(read_views.get(&key).and_then(|cached| {
            (Arc::ptr_eq(&cached.store, store) && cached.fence == *fence)
                .then(|| Arc::clone(&cached.view))
        }))
    }

    fn cached_vertex_count(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        scope: &VertexCountScope,
    ) -> Result<Option<i64>, GatewayExecutionError> {
        let vertex_counts = self
            .vertex_counts
            .lock()
            .map_err(|_| data_error("vertex count cache mutex is poisoned"))?;
        Ok(vertex_counts.get(&key).and_then(|cached| {
            (Arc::ptr_eq(&cached.store, store) && cached.fence == *fence && cached.scope == *scope)
                .then_some(cached.count)
        }))
    }

    fn adjacency<'a>(
        &'a self,
        key: ReplicaKey,
        store: &'a Arc<dyn ReplicaStateStore>,
        fence: &'a ReadFence,
        view: &'a Arc<dyn TemporalReadView>,
        scope: AdjacencyScope,
    ) -> GatewayFuture<'a, Result<Vec<dtg_storage::EdgeVersion>, GatewayExecutionError>> {
        Box::pin(async move {
            loop {
                let cache_started = Instant::now();
                if let Some(edges) = self.cached_adjacency(key, store, fence, &scope)? {
                    self.request_metrics.record_detail(
                        RequestDetail::DataAdjacencyCacheHit,
                        StageOutcome::Success,
                        elapsed_nanoseconds(cache_started),
                    );
                    return Ok(edges);
                }
                self.request_metrics.record_detail(
                    RequestDetail::DataAdjacencyCacheMiss,
                    StageOutcome::Success,
                    elapsed_nanoseconds(cache_started),
                );
                match self.acquire_adjacency_flight(key, store, fence, &scope)? {
                    AdjacencyFlightLease::Owner => {
                        let result = async {
                            let direction = adjacency_direction_from_key(scope.direction)?;
                            let timer = self
                                .request_metrics
                                .start_detail(RequestDetail::DataAdjacencyBackendExpand);
                            let edges = timer.finish_result(
                                view.expand(
                                    AdjacencyRead::new(
                                        scope.vertex_id,
                                        direction,
                                        scope.valid_at,
                                        scope.transaction_time,
                                        scope.limit,
                                    )
                                    .map_err(data_storage_error)?,
                                )
                                .await
                                .map_err(data_storage_error),
                            )?;
                            if edges.len() > scope.limit as usize {
                                return Err(data_error(
                                    "adjacency read exceeded the requested bound",
                                ));
                            }
                            self.cache_adjacency(key, store, fence, view, scope.clone(), &edges)?;
                            Ok(edges)
                        }
                        .await;
                        self.finish_adjacency_flight(key, store, fence, &scope)?;
                        return result;
                    }
                    AdjacencyFlightLease::Waiter(mut completed) => {
                        let _ = completed.changed().await;
                    }
                }
            }
        })
    }

    fn cached_adjacency(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        scope: &AdjacencyScope,
    ) -> Result<Option<Vec<dtg_storage::EdgeVersion>>, GatewayExecutionError> {
        let adjacencies = self
            .adjacencies
            .lock()
            .map_err(|_| data_error("adjacency cache mutex is poisoned"))?;
        Ok(adjacencies.get(&key).and_then(|cached| {
            (Arc::ptr_eq(&cached.store, store) && cached.fence == *fence)
                .then(|| cached.entries.get(scope).cloned())
                .flatten()
        }))
    }

    fn acquire_adjacency_flight(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        scope: &AdjacencyScope,
    ) -> Result<AdjacencyFlightLease, GatewayExecutionError> {
        let mut flights = self
            .adjacency_flights
            .lock()
            .map_err(|_| data_error("adjacency flight mutex is poisoned"))?;
        let replica_flights = flights.entry(key).or_default();
        if let Some(flight) = replica_flights.get(scope)
            && Arc::ptr_eq(&flight.store, store)
            && flight.fence == *fence
        {
            return Ok(AdjacencyFlightLease::Waiter(flight.completed.subscribe()));
        }
        let (completed, _) = watch::channel(());
        replica_flights.insert(
            scope.clone(),
            AdjacencyFlight {
                store: Arc::clone(store),
                fence: fence.clone(),
                completed,
            },
        );
        Ok(AdjacencyFlightLease::Owner)
    }

    fn finish_adjacency_flight(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        scope: &AdjacencyScope,
    ) -> Result<(), GatewayExecutionError> {
        let mut flights = self
            .adjacency_flights
            .lock()
            .map_err(|_| data_error("adjacency flight mutex is poisoned"))?;
        let Some(replica_flights) = flights.get_mut(&key) else {
            return Ok(());
        };
        if replica_flights
            .get(scope)
            .is_some_and(|flight| Arc::ptr_eq(&flight.store, store) && flight.fence == *fence)
        {
            replica_flights.remove(scope);
        }
        if replica_flights.is_empty() {
            flights.remove(&key);
        }
        Ok(())
    }

    fn execute_traversal<'a>(
        &'a self,
        key: ReplicaKey,
        store: &'a Arc<dyn ReplicaStateStore>,
        fence: &'a ReadFence,
        view: &'a Arc<dyn TemporalReadView>,
        scope: TraversalScope<'a>,
    ) -> GatewayFuture<'a, Result<Vec<dtg_storage::EdgeVersion>, GatewayExecutionError>> {
        Box::pin(async move {
            if select_graph_access_path(&scope) == GraphAccessPath::SnapshotCsr
                && let Some(edges) = self
                    .execute_traversal_with_snapshot_csr(key, store, fence, view, &scope)
                    .await?
            {
                return Ok(edges);
            }
            let mut frontier = vec![scope.vertex_id];
            let mut output = Vec::new();
            for (hop, direction) in scope.directions.iter().copied().enumerate() {
                let final_hop = hop + 1 == scope.directions.len();
                let mut remaining = scope.limit;
                let mut next = Vec::new();
                for vertex in std::mem::take(&mut frontier) {
                    if remaining == 0 {
                        break;
                    }
                    let edges = self
                        .adjacency(
                            key,
                            store,
                            fence,
                            view,
                            AdjacencyScope {
                                vertex_id: vertex,
                                direction: adjacency_direction_key(direction),
                                transaction_time: scope.transaction_time,
                                valid_at: scope.valid_at,
                                limit: remaining,
                            },
                        )
                        .await?;
                    remaining =
                        remaining.saturating_sub(u32::try_from(edges.len()).unwrap_or(u32::MAX));
                    if final_hop {
                        output.extend(edges);
                    } else {
                        for edge in edges {
                            next.push(traversal_next_vertex(vertex, &edge, direction)?);
                        }
                    }
                }
                if !final_hop {
                    frontier = next;
                }
            }
            Ok(output)
        })
    }

    fn execute_traversal_with_snapshot_csr<'a>(
        &'a self,
        key: ReplicaKey,
        store: &'a Arc<dyn ReplicaStateStore>,
        fence: &'a ReadFence,
        view: &'a Arc<dyn TemporalReadView>,
        scope: &'a TraversalScope<'a>,
    ) -> GatewayFuture<'a, Result<Option<Vec<dtg_storage::EdgeVersion>>, GatewayExecutionError>>
    {
        Box::pin(async move {
            if scope.directions.contains(&AdjacencyDirection::Both) {
                return Ok(None);
            }
            let mut csrs = BTreeMap::new();
            for direction in scope.directions.iter().copied() {
                let direction = match direction {
                    AdjacencyDirection::Outgoing => CsrDirection::Outgoing,
                    AdjacencyDirection::Incoming => CsrDirection::Incoming,
                    AdjacencyDirection::Both => return Ok(None),
                };
                if csrs.contains_key(&direction) {
                    continue;
                }
                let Some(csr) = self
                    .snapshot_csr(
                        key,
                        store,
                        fence,
                        view,
                        scope.transaction_time,
                        scope.valid_at,
                        direction,
                    )
                    .await?
                else {
                    return Ok(None);
                };
                csrs.insert(direction, csr);
            }

            let mut frontier = vec![scope.vertex_id];
            let mut output = Vec::new();
            for (hop, direction) in scope.directions.iter().copied().enumerate() {
                let final_hop = hop + 1 == scope.directions.len();
                let direction = match direction {
                    AdjacencyDirection::Outgoing => CsrDirection::Outgoing,
                    AdjacencyDirection::Incoming => CsrDirection::Incoming,
                    AdjacencyDirection::Both => return Ok(None),
                };
                let csr = csrs
                    .get(&direction)
                    .ok_or_else(|| data_error("snapshot CSR direction is missing"))?;
                let mut remaining = scope.limit;
                let mut next = Vec::new();
                for vertex in std::mem::take(&mut frontier) {
                    if remaining == 0 {
                        break;
                    }
                    let neighbors = match csr.neighbors(vertex) {
                        Ok(neighbors) => neighbors,
                        Err(SnapshotCsrError::MissingVertex(_)) => continue,
                        Err(error) => return Err(data_error(error.to_string())),
                    };
                    for neighbor in neighbors.take(remaining as usize) {
                        remaining = remaining.saturating_sub(1);
                        if final_hop {
                            output.push(neighbor.edge().clone());
                        } else {
                            next.push(neighbor.vertex());
                        }
                    }
                }
                if !final_hop {
                    frontier = next;
                }
            }
            Ok(Some(output))
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn snapshot_csr<'a>(
        &'a self,
        key: ReplicaKey,
        store: &'a Arc<dyn ReplicaStateStore>,
        fence: &'a ReadFence,
        view: &'a Arc<dyn TemporalReadView>,
        transaction_time: TransactionTime,
        valid_at: i64,
        direction: CsrDirection,
    ) -> GatewayFuture<'a, Result<Option<Arc<SnapshotCsr>>, GatewayExecutionError>> {
        Box::pin(async move {
            let csr_key = SnapshotCsrKey::new(
                store.binding().clone(),
                fence.applied_index(),
                transaction_time,
                valid_at,
                direction,
            );
            loop {
                let cache_started = Instant::now();
                if let Some(csr) = self.cached_snapshot_csr(key, store, &csr_key)? {
                    self.request_metrics.record_detail(
                        RequestDetail::DataSnapshotCsrCacheHit,
                        StageOutcome::Success,
                        elapsed_nanoseconds(cache_started),
                    );
                    return Ok(Some(csr));
                }
                self.request_metrics.record_detail(
                    RequestDetail::DataSnapshotCsrCacheMiss,
                    StageOutcome::Success,
                    elapsed_nanoseconds(cache_started),
                );
                match self.acquire_snapshot_csr_flight(key, store, fence, &csr_key)? {
                    SnapshotCsrFlightLease::Owner => {
                        let result = match self
                            .extend_snapshot_csr(
                                key,
                                store,
                                view,
                                csr_key.clone(),
                                transaction_time,
                                valid_at,
                            )
                            .await
                        {
                            Ok(Some(csr)) => Ok(Some(csr)),
                            Ok(None) => {
                                self.build_snapshot_csr(
                                    view,
                                    csr_key.clone(),
                                    transaction_time,
                                    valid_at,
                                )
                                .await
                            }
                            Err(error) => Err(error),
                        };
                        self.finish_snapshot_csr_flight(key, store, fence, &csr_key)?;
                        let result = result?;
                        let Some(csr) = result else {
                            return Ok(None);
                        };
                        self.cache_snapshot_csr(
                            key,
                            store,
                            fence,
                            view,
                            csr_key.clone(),
                            Arc::clone(&csr),
                        )?;
                        return Ok(Some(csr));
                    }
                    SnapshotCsrFlightLease::Waiter(mut completed) => {
                        let _ = completed.changed().await;
                    }
                }
            }
        })
    }

    fn build_snapshot_csr<'a>(
        &'a self,
        view: &'a Arc<dyn TemporalReadView>,
        csr_key: SnapshotCsrKey,
        transaction_time: TransactionTime,
        valid_at: i64,
    ) -> GatewayFuture<'a, Result<Option<Arc<SnapshotCsr>>, GatewayExecutionError>> {
        Box::pin(async move {
            let build_started = Instant::now();
            let mut after = None;
            let mut edges = Vec::new();
            loop {
                let page = view
                    .scan_edges(
                        EdgeScan::new(
                            valid_at,
                            transaction_time,
                            after,
                            SNAPSHOT_CSR_EDGE_PAGE_SIZE,
                        )
                        .map_err(data_storage_error)?,
                    )
                    .await
                    .map_err(data_storage_error)?;
                if edges.len().saturating_add(page.rows().len()) > MAX_SNAPSHOT_CSR_EDGES {
                    return Ok(None);
                }
                edges.extend(page.rows().iter().cloned());
                match page.next_after() {
                    Some(next_after) => after = Some(next_after),
                    None => break,
                }
            }
            let csr = match SnapshotCsr::build(
                csr_key,
                edges,
                SnapshotCsrBuildBudget::new(MAX_SNAPSHOT_CSR_BYTES_PER_REPLICA),
            ) {
                Ok(csr) => Arc::new(csr),
                Err(error) if error.is_insufficient_memory() => return Ok(None),
                Err(error) => return Err(data_error(error.to_string())),
            };
            self.request_metrics.record_detail(
                RequestDetail::DataSnapshotCsrBuild,
                StageOutcome::Success,
                elapsed_nanoseconds(build_started),
            );
            Ok(Some(csr))
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn extend_snapshot_csr<'a>(
        &'a self,
        key: ReplicaKey,
        store: &'a Arc<dyn ReplicaStateStore>,
        view: &'a Arc<dyn TemporalReadView>,
        csr_key: SnapshotCsrKey,
        transaction_time: TransactionTime,
        valid_at: i64,
    ) -> GatewayFuture<'a, Result<Option<Arc<SnapshotCsr>>, GatewayExecutionError>> {
        Box::pin(async move {
            let Some(base) = self.latest_snapshot_csr_base(key, store, &csr_key)? else {
                return Ok(None);
            };
            let mut overlay = match CommittedCsrOverlay::new(base, MAX_SNAPSHOT_CSR_OVERLAY_BYTES) {
                Ok(overlay) => overlay,
                Err(_) => return Ok(None),
            };
            let mut after = Some(ChangeCursor::new(overlay.covered_through(), u64::MAX));
            let mut expected_index = overlay.covered_through().saturating_add(1);
            let mut operations = Vec::new();
            let mut current_index = None;
            let mut change_count = 0_usize;
            loop {
                let page = view
                    .changes(
                        ChangesRead::new(
                            after,
                            csr_key.applied_index(),
                            SNAPSHOT_CSR_OVERLAY_CHANGE_PAGE_SIZE,
                        )
                        .map_err(data_storage_error)?,
                    )
                    .await
                    .map_err(data_storage_error)?;
                for change in page.rows() {
                    change_count = change_count.saturating_add(1);
                    if change_count > MAX_SNAPSHOT_CSR_OVERLAY_CHANGES {
                        return Ok(None);
                    }
                    let index = change.raft_index();
                    if index < expected_index || index > csr_key.applied_index() {
                        return Ok(None);
                    }
                    match current_index {
                        Some(current) if current == index => {}
                        Some(current) => {
                            if current != expected_index {
                                return Ok(None);
                            }
                            let Ok(delta) = CommittedGraphDelta::new(
                                csr_key.with_applied_index(current),
                                std::mem::take(&mut operations),
                            ) else {
                                return Ok(None);
                            };
                            if overlay.apply(delta).is_err() {
                                return Ok(None);
                            }
                            expected_index = expected_index.saturating_add(1);
                            if index != expected_index {
                                return Ok(None);
                            }
                            current_index = Some(index);
                        }
                        None => {
                            if index != expected_index {
                                return Ok(None);
                            }
                            current_index = Some(index);
                        }
                    }
                    match change.mutation() {
                        LogicalMutation::PutEdge(edge)
                            if edge.transaction_time() <= transaction_time
                                && edge.valid_time().start() <= valid_at
                                && valid_at < edge.valid_time().end() =>
                        {
                            operations.push(CommittedAdjacencyOperation::Add(edge.clone()));
                        }
                        LogicalMutation::DeleteEdge(tombstone)
                            if tombstone.transaction_time() <= transaction_time =>
                        {
                            operations.push(CommittedAdjacencyOperation::Remove(tombstone.clone()));
                        }
                        _ => {}
                    }
                }
                match page.next_after() {
                    Some(next_after) => after = Some(next_after),
                    None => break,
                }
            }
            let Some(current) = current_index else {
                return Ok(None);
            };
            if current != expected_index || current != csr_key.applied_index() {
                return Ok(None);
            }
            let Ok(delta) = CommittedGraphDelta::new(csr_key.clone(), operations) else {
                return Ok(None);
            };
            if overlay.apply(delta).is_err() {
                return Ok(None);
            }
            match overlay.materialize(
                csr_key,
                SnapshotCsrBuildBudget::new(MAX_SNAPSHOT_CSR_BYTES_PER_REPLICA),
            ) {
                Ok(csr) => Ok(Some(Arc::new(csr))),
                Err(error) if error.is_insufficient_memory() => Ok(None),
                Err(_) => Ok(None),
            }
        })
    }

    fn acquire_snapshot_csr_flight(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        csr_key: &SnapshotCsrKey,
    ) -> Result<SnapshotCsrFlightLease, GatewayExecutionError> {
        let mut flights = self
            .snapshot_csr_flights
            .lock()
            .map_err(|_| data_error("snapshot CSR flight mutex is poisoned"))?;
        let replica_flights = flights.entry(key).or_default();
        if let Some(flight) = replica_flights.get(csr_key)
            && Arc::ptr_eq(&flight.store, store)
            && flight.fence == *fence
        {
            return Ok(SnapshotCsrFlightLease::Waiter(flight.completed.subscribe()));
        }
        let (completed, _) = watch::channel(());
        replica_flights.insert(
            csr_key.clone(),
            SnapshotCsrFlight {
                store: Arc::clone(store),
                fence: fence.clone(),
                completed,
            },
        );
        Ok(SnapshotCsrFlightLease::Owner)
    }

    fn finish_snapshot_csr_flight(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        csr_key: &SnapshotCsrKey,
    ) -> Result<(), GatewayExecutionError> {
        let mut flights = self
            .snapshot_csr_flights
            .lock()
            .map_err(|_| data_error("snapshot CSR flight mutex is poisoned"))?;
        let Some(replica_flights) = flights.get_mut(&key) else {
            return Ok(());
        };
        if replica_flights
            .get(csr_key)
            .is_some_and(|flight| Arc::ptr_eq(&flight.store, store) && flight.fence == *fence)
        {
            replica_flights.remove(csr_key);
        }
        if replica_flights.is_empty() {
            flights.remove(&key);
        }
        Ok(())
    }

    fn cached_snapshot_csr(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        csr_key: &SnapshotCsrKey,
    ) -> Result<Option<Arc<SnapshotCsr>>, GatewayExecutionError> {
        let csrs = self
            .snapshot_csrs
            .lock()
            .map_err(|_| data_error("snapshot CSR cache mutex is poisoned"))?;
        Ok(csrs.get(&key).and_then(|cached| {
            (Arc::ptr_eq(&cached.store, store))
                .then(|| cached.entries.get(csr_key).cloned())
                .flatten()
        }))
    }

    fn latest_snapshot_csr_base(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        csr_key: &SnapshotCsrKey,
    ) -> Result<Option<Arc<SnapshotCsr>>, GatewayExecutionError> {
        let csrs = self
            .snapshot_csrs
            .lock()
            .map_err(|_| data_error("snapshot CSR cache mutex is poisoned"))?;
        Ok(csrs.get(&key).and_then(|cached| {
            Arc::ptr_eq(&cached.store, store).then_some(())?;
            cached
                .entries
                .iter()
                .filter(|(candidate, _)| {
                    candidate.same_lineage(csr_key)
                        && candidate.applied_index() < csr_key.applied_index()
                })
                .max_by_key(|(candidate, _)| candidate.applied_index())
                .map(|(_, csr)| Arc::clone(csr))
        }))
    }

    #[allow(clippy::too_many_arguments)]
    fn cache_snapshot_csr(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        view: &Arc<dyn TemporalReadView>,
        csr_key: SnapshotCsrKey,
        csr: Arc<SnapshotCsr>,
    ) -> Result<(), GatewayExecutionError> {
        let stores = self
            .stores
            .lock()
            .map_err(|_| data_error("replica store mutex is poisoned"))?;
        if !stores
            .get(&key)
            .is_some_and(|runtime| Arc::ptr_eq(runtime.state(), store))
        {
            return Ok(());
        }
        let read_views = self
            .read_views
            .lock()
            .map_err(|_| data_error("read view cache mutex is poisoned"))?;
        if !read_views.get(&key).is_some_and(|cached| {
            Arc::ptr_eq(&cached.store, store)
                && cached.fence == *fence
                && Arc::ptr_eq(&cached.view, view)
        }) {
            return Ok(());
        }
        let mut csrs = self
            .snapshot_csrs
            .lock()
            .map_err(|_| data_error("snapshot CSR cache mutex is poisoned"))?;
        let cached = csrs.entry(key).or_insert_with(|| CachedSnapshotCsrs {
            store: Arc::clone(store),
            bytes: 0,
            entries: BTreeMap::new(),
        });
        if !Arc::ptr_eq(&cached.store, store) {
            *cached = CachedSnapshotCsrs {
                store: Arc::clone(store),
                bytes: 0,
                entries: BTreeMap::new(),
            };
        }
        if cached.entries.contains_key(&csr_key)
            || cached.entries.len() == MAX_CACHED_SNAPSHOT_CSR_IMAGES_PER_REPLICA
            || cached.bytes.saturating_add(csr.retained_bytes())
                > MAX_CACHED_SNAPSHOT_CSR_BYTES_PER_REPLICA
        {
            return Ok(());
        }
        cached.bytes = cached.bytes.saturating_add(csr.retained_bytes());
        cached.entries.insert(csr_key, csr);
        Ok(())
    }

    fn cache_adjacency(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        view: &Arc<dyn TemporalReadView>,
        scope: AdjacencyScope,
        edges: &[dtg_storage::EdgeVersion],
    ) -> Result<(), GatewayExecutionError> {
        let Some(bytes) = cached_adjacency_bytes(edges) else {
            return Ok(());
        };
        if bytes > MAX_CACHED_ADJACENCY_BYTES_PER_REPLICA {
            return Ok(());
        }
        let stores = self
            .stores
            .lock()
            .map_err(|_| data_error("replica store mutex is poisoned"))?;
        if !stores
            .get(&key)
            .is_some_and(|runtime| Arc::ptr_eq(runtime.state(), store))
        {
            return Ok(());
        }
        let read_views = self
            .read_views
            .lock()
            .map_err(|_| data_error("read view cache mutex is poisoned"))?;
        if !read_views.get(&key).is_some_and(|cached| {
            Arc::ptr_eq(&cached.store, store)
                && cached.fence == *fence
                && Arc::ptr_eq(&cached.view, view)
        }) {
            return Ok(());
        }
        let mut adjacencies = self
            .adjacencies
            .lock()
            .map_err(|_| data_error("adjacency cache mutex is poisoned"))?;
        let cached = adjacencies.entry(key).or_insert_with(|| CachedAdjacencies {
            store: Arc::clone(store),
            fence: fence.clone(),
            bytes: 0,
            entries: BTreeMap::new(),
        });
        if !Arc::ptr_eq(&cached.store, store) || cached.fence != *fence {
            *cached = CachedAdjacencies {
                store: Arc::clone(store),
                fence: fence.clone(),
                bytes: 0,
                entries: BTreeMap::new(),
            };
        }
        if cached.entries.contains_key(&scope)
            || cached.entries.len() == MAX_CACHED_ADJACENCY_SCOPES_PER_REPLICA
            || cached.bytes.saturating_add(bytes) > MAX_CACHED_ADJACENCY_BYTES_PER_REPLICA
        {
            return Ok(());
        }
        cached.bytes = cached.bytes.saturating_add(bytes);
        cached.entries.insert(scope, edges.to_vec());
        Ok(())
    }

    fn vertex_count<'a>(
        &'a self,
        key: ReplicaKey,
        store: &'a Arc<dyn ReplicaStateStore>,
        fence: &'a ReadFence,
        view: &'a Arc<dyn TemporalReadView>,
        scope: VertexCountScope,
    ) -> GatewayFuture<'a, Result<i64, GatewayExecutionError>> {
        Box::pin(async move {
            loop {
                if let Some(count) = self.cached_vertex_count(key, store, fence, &scope)? {
                    return Ok(count);
                }
                match self.acquire_vertex_count_flight(key, store, fence, &scope)? {
                    VertexCountFlightLease::Owner => {
                        let result = async {
                            let count = view
                                .scan_vertices(
                                    VertexScan::new(
                                        scope.valid_at,
                                        scope.transaction_time,
                                        scope.after,
                                        scope.limit,
                                    )
                                    .map_err(data_storage_error)?,
                                )
                                .await
                                .map_err(data_storage_error)?
                                .rows()
                                .len();
                            let count = i64::try_from(count)
                                .map_err(|_| data_error("vertex count exceeds i64"))?;
                            self.cache_vertex_count(key, store, fence, view, scope.clone(), count)?;
                            Ok(count)
                        }
                        .await;
                        self.finish_vertex_count_flight(key, store, fence, &scope)?;
                        return result;
                    }
                    VertexCountFlightLease::Waiter(mut completed) => {
                        let _ = completed.changed().await;
                    }
                }
            }
        })
    }

    fn acquire_vertex_count_flight(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        scope: &VertexCountScope,
    ) -> Result<VertexCountFlightLease, GatewayExecutionError> {
        let mut flights = self
            .vertex_count_flights
            .lock()
            .map_err(|_| data_error("vertex count flight mutex is poisoned"))?;
        if let Some(flight) = flights.get(&key)
            && Arc::ptr_eq(&flight.store, store)
            && flight.fence == *fence
            && flight.scope == *scope
        {
            return Ok(VertexCountFlightLease::Waiter(flight.completed.subscribe()));
        }
        let (completed, _) = watch::channel(());
        flights.insert(
            key,
            VertexCountFlight {
                store: Arc::clone(store),
                fence: fence.clone(),
                scope: scope.clone(),
                completed,
            },
        );
        Ok(VertexCountFlightLease::Owner)
    }

    fn finish_vertex_count_flight(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        scope: &VertexCountScope,
    ) -> Result<(), GatewayExecutionError> {
        let mut flights = self
            .vertex_count_flights
            .lock()
            .map_err(|_| data_error("vertex count flight mutex is poisoned"))?;
        if flights.get(&key).is_some_and(|flight| {
            Arc::ptr_eq(&flight.store, store) && flight.fence == *fence && flight.scope == *scope
        }) {
            flights.remove(&key);
        }
        Ok(())
    }

    fn cache_vertex_count(
        &self,
        key: ReplicaKey,
        store: &Arc<dyn ReplicaStateStore>,
        fence: &ReadFence,
        view: &Arc<dyn TemporalReadView>,
        scope: VertexCountScope,
        count: i64,
    ) -> Result<(), GatewayExecutionError> {
        // Preserve the same lifecycle ordering as read-view publication, then bind the scalar
        // to that exact cached view so a removed replica cannot repopulate the count cache.
        let stores = self
            .stores
            .lock()
            .map_err(|_| data_error("replica store mutex is poisoned"))?;
        if !stores
            .get(&key)
            .is_some_and(|runtime| Arc::ptr_eq(runtime.state(), store))
        {
            return Ok(());
        }
        let read_views = self
            .read_views
            .lock()
            .map_err(|_| data_error("read view cache mutex is poisoned"))?;
        if !read_views.get(&key).is_some_and(|cached| {
            Arc::ptr_eq(&cached.store, store)
                && cached.fence == *fence
                && Arc::ptr_eq(&cached.view, view)
        }) {
            return Ok(());
        }
        let mut vertex_counts = self
            .vertex_counts
            .lock()
            .map_err(|_| data_error("vertex count cache mutex is poisoned"))?;
        vertex_counts.insert(
            key,
            CachedVertexCount {
                store: Arc::clone(store),
                fence: fence.clone(),
                scope,
                count,
            },
        );
        Ok(())
    }

    fn clear_read_view(&self, key: ReplicaKey) -> Result<(), ShardError> {
        self.read_views
            .lock()
            .map_err(|_| ShardError::InvalidLifecycle("read view cache mutex is poisoned".into()))?
            .remove(&key);
        self.vertex_counts
            .lock()
            .map_err(|_| {
                ShardError::InvalidLifecycle("vertex count cache mutex is poisoned".into())
            })?
            .remove(&key);
        self.vertex_count_flights
            .lock()
            .map_err(|_| {
                ShardError::InvalidLifecycle("vertex count flight mutex is poisoned".into())
            })?
            .remove(&key);
        self.adjacencies
            .lock()
            .map_err(|_| ShardError::InvalidLifecycle("adjacency cache mutex is poisoned".into()))?
            .remove(&key);
        self.adjacency_flights
            .lock()
            .map_err(|_| ShardError::InvalidLifecycle("adjacency flight mutex is poisoned".into()))?
            .remove(&key);
        self.snapshot_csrs
            .lock()
            .map_err(|_| {
                ShardError::InvalidLifecycle("snapshot CSR cache mutex is poisoned".into())
            })?
            .remove(&key);
        self.snapshot_csr_flights
            .lock()
            .map_err(|_| {
                ShardError::InvalidLifecycle("snapshot CSR flight mutex is poisoned".into())
            })?
            .remove(&key);
        Ok(())
    }

    fn record_temporal_read_view_diagnostics(
        &self,
        before: Option<dtg_storage::TemporalReadViewDiagnostics>,
        after: Option<dtg_storage::TemporalReadViewDiagnostics>,
        scan: bool,
    ) {
        let (Some(before), Some(after)) = (before, after) else {
            return;
        };
        if scan {
            self.request_metrics.record_detail(
                RequestDetail::DataTemporalScanIdCollection,
                StageOutcome::Success,
                after
                    .scan_id_collection_nanoseconds
                    .saturating_sub(before.scan_id_collection_nanoseconds),
            );
            self.request_metrics.record_detail(
                RequestDetail::DataTemporalScanVisibility,
                StageOutcome::Success,
                after
                    .scan_visibility_nanoseconds
                    .saturating_sub(before.scan_visibility_nanoseconds),
            );
        } else {
            self.request_metrics.record_detail(
                RequestDetail::DataTemporalPointEvaluation,
                StageOutcome::Success,
                after
                    .point_evaluation_nanoseconds
                    .saturating_sub(before.point_evaluation_nanoseconds),
            );
        }
    }

    fn lock_shards(&self) -> Result<std::sync::MutexGuard<'_, ShardHost>, ShardError> {
        self.shards
            .lock()
            .map_err(|_| ShardError::InvalidLifecycle("ShardHost mutex is poisoned".into()))
    }

    fn register_replica_route(
        &self,
        key: ReplicaKey,
        binding: &ReplicaBinding,
    ) -> Result<(), ShardError> {
        let mut routes = self
            .replica_routes
            .write()
            .map_err(|_| ShardError::InvalidLifecycle("replica route cache is poisoned".into()))?;
        insert_replica_route(&mut routes, key, binding);
        Ok(())
    }

    fn unregister_replica_route(&self, key: ReplicaKey) -> Result<(), ShardError> {
        let mut routes = self
            .replica_routes
            .write()
            .map_err(|_| ShardError::InvalidLifecycle("replica route cache is poisoned".into()))?;
        let affected = routes
            .iter()
            .filter_map(|(route, mapped)| (*mapped == key).then_some(route.without_replica()))
            .collect::<Vec<_>>();
        routes.retain(|_, mapped| *mapped != key);
        for wildcard in affected {
            refresh_wildcard_route(&mut routes, wildcard);
        }
        Ok(())
    }
}

fn refresh_wildcard_route(routes: &mut BTreeMap<ReplicaRoute, ReplicaKey>, wildcard: ReplicaRoute) {
    routes.remove(&wildcard);
    let mut matches = routes.iter().filter_map(|(route, key)| {
        (route.replica_id.is_some() && route.without_replica() == wildcard).then_some(*key)
    });
    let Some(key) = matches.next() else {
        return;
    };
    if matches.next().is_none() {
        routes.insert(wildcard, key);
    }
}

fn insert_replica_route(
    routes: &mut BTreeMap<ReplicaRoute, ReplicaKey>,
    key: ReplicaKey,
    binding: &ReplicaBinding,
) {
    let exact = ReplicaRoute::from_binding(binding, Some(binding.replica_id()));
    let wildcard = exact.without_replica();
    routes.insert(exact, key);
    refresh_wildcard_route(routes, wildcard);
}

fn elapsed_nanoseconds(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

enum FragmentRead {
    Point(VertexId),
    Scan {
        after: Option<VertexId>,
        limit: u32,
    },
    Count {
        after: Option<VertexId>,
        limit: u32,
    },
    Adjacency {
        vertex_id: VertexId,
        direction: AdjacencyDirection,
        limit: u32,
    },
    Traversal {
        vertex_id: VertexId,
        directions: Vec<AdjacencyDirection>,
        limit: u32,
    },
}

fn decode_fragment_read(
    encoded: &[u8],
    transaction_time: TransactionTime,
    valid_at: i64,
) -> Result<FragmentRead, GatewayExecutionError> {
    let mut cursor = FragmentCursor::new(encoded);
    if cursor.u64()? == 0 || cursor.u32()? == 0 {
        return Err(data_error("physical fragment version or root is zero"));
    }
    cursor.skip_row_schema()?;
    if cursor.u32()? != 1 {
        return Err(data_error(
            "physical fragment must contain exactly one storage access",
        ));
    }
    cursor.u32()?;
    let read = match cursor.u8()? {
        0 => match cursor.u8()? {
            0 => {
                let id =
                    VertexId::new(cursor.u128()?).map_err(|error| data_error(error.to_string()))?;
                decode_logical_read_scope(&mut cursor, transaction_time, valid_at)?;
                if cursor.u32()? != 1 {
                    return Err(data_error("logical point row bound must equal one"));
                }
                FragmentRead::Point(id)
            }
            1 => {
                decode_logical_read_scope(&mut cursor, transaction_time, valid_at)?;
                let limit = cursor.u32()?;
                if limit == 0 {
                    return Err(data_error("logical scan limit is zero"));
                }
                FragmentRead::Scan { after: None, limit }
            }
            2 => {
                decode_logical_read_scope(&mut cursor, transaction_time, valid_at)?;
                let limit = cursor.u32()?;
                if limit == 0 {
                    return Err(data_error("logical count row bound is zero"));
                }
                FragmentRead::Count { after: None, limit }
            }
            4 => {
                let vertex_id =
                    VertexId::new(cursor.u128()?).map_err(|error| data_error(error.to_string()))?;
                let direction = match cursor.u8()? {
                    0 => AdjacencyDirection::Outgoing,
                    1 => AdjacencyDirection::Incoming,
                    2 => AdjacencyDirection::Both,
                    _ => return Err(data_error("logical adjacency direction is invalid")),
                };
                decode_logical_read_scope(&mut cursor, transaction_time, valid_at)?;
                let limit = cursor.u32()?;
                if limit == 0 {
                    return Err(data_error("logical adjacency row bound is zero"));
                }
                FragmentRead::Adjacency {
                    vertex_id,
                    direction,
                    limit,
                }
            }
            5 => {
                let vertex_id =
                    VertexId::new(cursor.u128()?).map_err(|error| data_error(error.to_string()))?;
                let directions = cursor.len()?;
                if !(2..=32).contains(&directions) {
                    return Err(data_error(
                        "logical traversal hop count must be within 2..=32",
                    ));
                }
                let directions = (0..directions)
                    .map(|_| decode_adjacency_direction(cursor.u8()?))
                    .collect::<Result<Vec<_>, _>>()?;
                decode_logical_read_scope(&mut cursor, transaction_time, valid_at)?;
                let limit = cursor.u32()?;
                if limit == 0 {
                    return Err(data_error("logical traversal row bound is zero"));
                }
                FragmentRead::Traversal {
                    vertex_id,
                    directions,
                    limit,
                }
            }
            _ => {
                return Err(data_error(
                    "physical logical access is not executable by the Data worker",
                ));
            }
        },
        1 => {
            if cursor.u32()? == 0 {
                return Err(data_error("pushdown contract version is zero"));
            }
            let capability_count = cursor.len()?;
            for _ in 0..capability_count {
                cursor.string()?;
            }
            let read = match cursor.u8()? {
                0 => {
                    let id = VertexId::new(cursor.u128()?)
                        .map_err(|error| data_error(error.to_string()))?;
                    cursor.i64()?;
                    cursor.i64()?;
                    FragmentRead::Point(id)
                }
                1 => {
                    cursor.i64()?;
                    cursor.i64()?;
                    let after = match cursor.u8()? {
                        0 => None,
                        1 => Some(
                            VertexId::new(cursor.u128()?)
                                .map_err(|error| data_error(error.to_string()))?,
                        ),
                        _ => return Err(data_error("pushdown scan cursor tag is invalid")),
                    };
                    let limit = cursor.u32()?;
                    if limit == 0 {
                        return Err(data_error("pushdown scan limit is zero"));
                    }
                    FragmentRead::Scan { after, limit }
                }
                2 => {
                    cursor.i64()?;
                    cursor.i64()?;
                    let after = match cursor.u8()? {
                        0 => None,
                        1 => Some(
                            VertexId::new(cursor.u128()?)
                                .map_err(|error| data_error(error.to_string()))?,
                        ),
                        _ => return Err(data_error("pushdown count cursor tag is invalid")),
                    };
                    let limit = cursor.u32()?;
                    if limit == 0 {
                        return Err(data_error("pushdown count row bound is zero"));
                    }
                    FragmentRead::Count { after, limit }
                }
                _ => return Err(data_error("unknown physical pushdown operation")),
            };
            if cursor.u8()? & !0x1f != 0 {
                return Err(data_error("pushdown semantic flags are invalid"));
            }
            if cursor.flag("pushdown residual")? {
                cursor.skip_physical_expr(0)?;
            }
            read
        }
        _ => return Err(data_error("unknown physical storage access")),
    };
    let operator_count = cursor.len()?;
    for _ in 0..operator_count {
        cursor.skip_operator()?;
    }
    cursor.finish()?;
    Ok(read)
}

fn decode_adjacency_direction(value: u8) -> Result<AdjacencyDirection, GatewayExecutionError> {
    match value {
        0 => Ok(AdjacencyDirection::Outgoing),
        1 => Ok(AdjacencyDirection::Incoming),
        2 => Ok(AdjacencyDirection::Both),
        _ => Err(data_error("logical adjacency direction is invalid")),
    }
}

const fn adjacency_direction_key(direction: AdjacencyDirection) -> u8 {
    match direction {
        AdjacencyDirection::Outgoing => 0,
        AdjacencyDirection::Incoming => 1,
        AdjacencyDirection::Both => 2,
    }
}

fn adjacency_direction_from_key(
    direction: u8,
) -> Result<AdjacencyDirection, GatewayExecutionError> {
    decode_adjacency_direction(direction)
}

fn cached_adjacency_bytes(edges: &[dtg_storage::EdgeVersion]) -> Option<usize> {
    edges.iter().try_fold(0_usize, |total, edge| {
        total
            .checked_add(72)?
            .checked_add(edge.edge_type().len())?
            .checked_add(cached_properties_bytes(edge.properties())?)
    })
}

fn cached_properties_bytes(properties: &dtg_storage::Properties) -> Option<usize> {
    properties.iter().try_fold(8_usize, |total, (name, value)| {
        total
            .checked_add(name.len())?
            .checked_add(cached_value_bytes(value)?)
    })
}

fn cached_value_bytes(value: &dtg_storage::Value) -> Option<usize> {
    match value {
        dtg_storage::Value::Null => Some(1),
        dtg_storage::Value::Boolean(_) => Some(2),
        dtg_storage::Value::Integer(_) | dtg_storage::Value::FloatBits(_) => Some(9),
        dtg_storage::Value::Bytes(value) => 9_usize.checked_add(value.len()),
        dtg_storage::Value::String(value) => 9_usize.checked_add(value.len()),
        dtg_storage::Value::List(values) => values.iter().try_fold(9_usize, |total, value| {
            total.checked_add(cached_value_bytes(value)?)
        }),
        dtg_storage::Value::Map(values) => {
            values.iter().try_fold(9_usize, |total, (name, value)| {
                total
                    .checked_add(name.len())?
                    .checked_add(cached_value_bytes(value)?)
            })
        }
    }
}

fn traversal_next_vertex(
    current: VertexId,
    edge: &dtg_storage::EdgeVersion,
    direction: AdjacencyDirection,
) -> Result<VertexId, GatewayExecutionError> {
    match direction {
        AdjacencyDirection::Outgoing if edge.source() == current => Ok(edge.target()),
        AdjacencyDirection::Incoming if edge.target() == current => Ok(edge.source()),
        AdjacencyDirection::Both if edge.source() == current => Ok(edge.target()),
        AdjacencyDirection::Both if edge.target() == current => Ok(edge.source()),
        _ => Err(data_error(
            "adjacency traversal returned an edge outside its requested direction",
        )),
    }
}

fn decode_logical_read_scope(
    cursor: &mut FragmentCursor<'_>,
    transaction_time: TransactionTime,
    valid_at: i64,
) -> Result<(), GatewayExecutionError> {
    let transaction_scope = match cursor.u8()? {
        0 => None,
        1 => match cursor.u8()? {
            0 => Some(
                TransactionTime::new(cursor.i64()?)
                    .map_err(|error| data_error(error.to_string()))?,
            ),
            _ => return Err(data_error("parameterized temporal scopes must be resolved")),
        },
        2 => {
            return Err(data_error(
                "CHANGES fragment scan requires a dedicated worker",
            ));
        }
        _ => return Err(data_error("invalid temporal scope tag")),
    };
    if transaction_scope.is_some_and(|value| value != transaction_time) {
        return Err(data_error(
            "logical transaction-time scope diverges from the execution fence",
        ));
    }
    let valid_scope = match cursor.u8()? {
        0 => None,
        1 => match cursor.u8()? {
            0 => Some(cursor.i64()?),
            _ => return Err(data_error("parameterized valid time must be resolved")),
        },
        _ => return Err(data_error("unsupported valid-time fragment predicate")),
    };
    if valid_scope.is_some_and(|value| value != valid_at) {
        return Err(data_error(
            "logical valid-time scope diverges from the execution fence",
        ));
    }
    Ok(())
}

fn vertex_row(vertex: &dtg_storage::VertexVersion) -> Vec<GatewayValue> {
    let mut value = BTreeMap::new();
    value.insert(
        "id".into(),
        i64::try_from(vertex.id().get()).map_or_else(
            |_| GatewayValue::Bytes(vertex.id().get().to_be_bytes().to_vec()),
            GatewayValue::Integer,
        ),
    );
    value.insert(
        "properties".into(),
        GatewayValue::Map(
            vertex
                .properties()
                .iter()
                .map(|(name, value)| (name.clone(), gateway_value(value)))
                .collect(),
        ),
    );
    vec![GatewayValue::Map(value)]
}

fn edge_row(edge: &dtg_storage::EdgeVersion) -> Vec<GatewayValue> {
    let mut value = BTreeMap::new();
    value.insert("id".into(), gateway_identifier(edge.id().get()));
    value.insert("source".into(), gateway_identifier(edge.source().get()));
    value.insert("target".into(), gateway_identifier(edge.target().get()));
    value.insert("type".into(), GatewayValue::String(edge.edge_type().into()));
    value.insert(
        "properties".into(),
        GatewayValue::Map(
            edge.properties()
                .iter()
                .map(|(name, value)| (name.clone(), gateway_value(value)))
                .collect(),
        ),
    );
    vec![GatewayValue::Map(value)]
}

fn gateway_identifier(identifier: u128) -> GatewayValue {
    i64::try_from(identifier).map_or_else(
        |_| GatewayValue::Bytes(identifier.to_be_bytes().to_vec()),
        GatewayValue::Integer,
    )
}

fn gateway_value(value: &dtg_storage::Value) -> GatewayValue {
    match value {
        dtg_storage::Value::Null => GatewayValue::Null,
        dtg_storage::Value::Boolean(value) => GatewayValue::Boolean(*value),
        dtg_storage::Value::Integer(value) => GatewayValue::Integer(*value),
        dtg_storage::Value::FloatBits(value) => GatewayValue::FloatBits(*value),
        dtg_storage::Value::Bytes(value) => GatewayValue::Bytes(value.clone()),
        dtg_storage::Value::String(value) => GatewayValue::String(value.clone()),
        dtg_storage::Value::List(values) => {
            GatewayValue::List(values.iter().map(gateway_value).collect())
        }
        dtg_storage::Value::Map(values) => GatewayValue::Map(
            values
                .iter()
                .map(|(name, value)| (name.clone(), gateway_value(value)))
                .collect(),
        ),
    }
}

fn data_storage_error(error: StorageError) -> GatewayExecutionError {
    GatewayExecutionError::new(error.code(), error.to_string(), GatewayRetry::Safe)
}

fn data_error(message: impl Into<String>) -> GatewayExecutionError {
    GatewayExecutionError::new("DTG-EXECUTION-DATA-FRAGMENT", message, GatewayRetry::Never)
}

struct FragmentCursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> FragmentCursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn exact(&mut self, length: usize) -> Result<&'a [u8], GatewayExecutionError> {
        let end = self
            .offset
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| data_error("physical fragment is truncated"))?;
        let bytes = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(bytes)
    }

    fn u8(&mut self) -> Result<u8, GatewayExecutionError> {
        Ok(self.exact(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, GatewayExecutionError> {
        Ok(u32::from_be_bytes(
            self.exact(4)?.try_into().expect("length checked"),
        ))
    }

    fn u64(&mut self) -> Result<u64, GatewayExecutionError> {
        Ok(u64::from_be_bytes(
            self.exact(8)?.try_into().expect("length checked"),
        ))
    }

    fn u128(&mut self) -> Result<u128, GatewayExecutionError> {
        Ok(u128::from_be_bytes(
            self.exact(16)?.try_into().expect("length checked"),
        ))
    }

    fn i64(&mut self) -> Result<i64, GatewayExecutionError> {
        Ok(i64::from_be_bytes(
            self.exact(8)?.try_into().expect("length checked"),
        ))
    }

    fn len(&mut self) -> Result<usize, GatewayExecutionError> {
        usize::try_from(self.u32()?).map_err(|_| data_error("fragment length exceeds usize"))
    }

    fn string(&mut self) -> Result<&'a str, GatewayExecutionError> {
        let length = self.len()?;
        std::str::from_utf8(self.exact(length)?)
            .map_err(|_| data_error("fragment string is not UTF-8"))
    }

    fn skip_row_schema(&mut self) -> Result<(), GatewayExecutionError> {
        let fields = self.len()?;
        for _ in 0..fields {
            self.string()?;
            self.skip_type(0)?;
            match self.u8()? {
                0 | 1 => {}
                _ => return Err(data_error("fragment nullable flag is invalid")),
            }
        }
        Ok(())
    }

    fn skip_type(&mut self, depth: usize) -> Result<(), GatewayExecutionError> {
        if depth > 32 {
            return Err(data_error("fragment type nesting exceeds 32"));
        }
        match self.u8()? {
            0..=5 | 7..=10 => Ok(()),
            6 => self.skip_type(depth + 1),
            _ => Err(data_error("fragment logical type tag is invalid")),
        }
    }

    fn finish(&self) -> Result<(), GatewayExecutionError> {
        if self.offset != self.bytes.len() {
            return Err(data_error("physical fragment contains trailing bytes"));
        }
        Ok(())
    }

    fn flag(&mut self, name: &str) -> Result<bool, GatewayExecutionError> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(data_error(format!("fragment {name} flag is invalid"))),
        }
    }

    fn skip_operator(&mut self) -> Result<(), GatewayExecutionError> {
        self.u32()?;
        match self.u8()? {
            0 => {
                self.u32()?;
                let fragments = self.len()?;
                for _ in 0..fragments {
                    self.u32()?;
                }
                self.string()?;
            }
            1 => {
                self.u32()?;
                self.skip_physical_expr(0)?;
            }
            2 => {
                self.u32()?;
                self.skip_projections(0)?;
            }
            3 => {
                self.u32()?;
                self.u32()?;
                if self.u8()? > 3 {
                    return Err(data_error("fragment join kind is invalid"));
                }
                if self.flag("join predicate")? {
                    self.skip_physical_expr(0)?;
                }
            }
            4 => {
                self.u32()?;
                self.skip_projections(0)?;
                let aggregates = self.len()?;
                for _ in 0..aggregates {
                    if self.u8()? > 5 {
                        return Err(data_error("fragment aggregate kind is invalid"));
                    }
                    if self.flag("aggregate argument")? {
                        self.skip_logical_expr(0)?;
                    }
                    self.string()?;
                    self.flag("aggregate distinct")?;
                }
            }
            5 => {
                self.u32()?;
                let keys = self.len()?;
                for _ in 0..keys {
                    self.skip_logical_expr(0)?;
                    if self.u8()? > 1 {
                        return Err(data_error("fragment sort direction is invalid"));
                    }
                }
            }
            6 => {
                self.u32()?;
                self.u64()?;
                if self.flag("limit")? {
                    self.u64()?;
                }
            }
            7 => {
                self.u32()?;
                self.skip_physical_expr(0)?;
                self.string()?;
            }
            _ => return Err(data_error("fragment physical operator tag is invalid")),
        }
        Ok(())
    }

    fn skip_projections(&mut self, depth: usize) -> Result<(), GatewayExecutionError> {
        let projections = self.len()?;
        for _ in 0..projections {
            self.skip_logical_expr(depth + 1)?;
            self.string()?;
        }
        Ok(())
    }

    fn skip_physical_expr(&mut self, depth: usize) -> Result<(), GatewayExecutionError> {
        if depth > 64 {
            return Err(data_error("fragment expression nesting exceeds 64"));
        }
        match self.u8()? {
            0 => self.skip_logical_expr(depth + 1),
            1 => {
                self.u32()?;
                if self.u8()? & !0x1f != 0 {
                    return Err(data_error("fragment semantic flags are invalid"));
                }
                Ok(())
            }
            _ => Err(data_error("fragment physical expression tag is invalid")),
        }
    }

    fn skip_logical_expr(&mut self, depth: usize) -> Result<(), GatewayExecutionError> {
        if depth > 64 {
            return Err(data_error("fragment expression nesting exceeds 64"));
        }
        match self.u8()? {
            0 => self.skip_value(depth + 1),
            1 | 2 => self.string().map(|_| ()),
            3 => {
                self.skip_logical_expr(depth + 1)?;
                self.string()?;
                Ok(())
            }
            4 => {
                if self.u8()? > 2 {
                    return Err(data_error("fragment unary operator is invalid"));
                }
                self.skip_logical_expr(depth + 1)
            }
            5 => {
                if self.u8()? > 12 {
                    return Err(data_error("fragment binary operator is invalid"));
                }
                self.skip_logical_expr(depth + 1)?;
                self.skip_logical_expr(depth + 1)
            }
            6 => {
                let values = self.len()?;
                for _ in 0..values {
                    self.skip_logical_expr(depth + 1)?;
                }
                Ok(())
            }
            7 => {
                let values = self.len()?;
                for _ in 0..values {
                    self.string()?;
                    self.skip_logical_expr(depth + 1)?;
                }
                Ok(())
            }
            _ => Err(data_error("fragment logical expression tag is invalid")),
        }
    }

    fn skip_value(&mut self, depth: usize) -> Result<(), GatewayExecutionError> {
        if depth > 64 {
            return Err(data_error("fragment value nesting exceeds 64"));
        }
        match self.u8()? {
            0 => Ok(()),
            1 => {
                self.flag("boolean value")?;
                Ok(())
            }
            2 | 3 => self.exact(8).map(|_| ()),
            4 | 5 => {
                let length = self.len()?;
                self.exact(length).map(|_| ())
            }
            6 => {
                let values = self.len()?;
                for _ in 0..values {
                    self.skip_value(depth + 1)?;
                }
                Ok(())
            }
            7 => {
                let values = self.len()?;
                for _ in 0..values {
                    self.string()?;
                    self.skip_value(depth + 1)?;
                }
                Ok(())
            }
            _ => Err(data_error("fragment value tag is invalid")),
        }
    }
}

pub struct DataExecutionBuilder {
    shards: ShardHost,
    providers: ProviderResolverSet,
    error: Option<ExecutionBuildError>,
    request_metrics: Arc<RequestStageMetrics>,
}

impl Default for DataExecutionBuilder {
    fn default() -> Self {
        Self {
            shards: ShardHost::new(),
            providers: ProviderResolverSet::new(),
            error: None,
            request_metrics: Arc::new(RequestStageMetrics::default()),
        }
    }
}

impl DataExecutionBuilder {
    pub fn with_shards(mut self, shards: ShardHost) -> Self {
        self.shards = shards;
        self
    }

    pub fn with_provider(
        mut self,
        kind: ProviderKind,
        resolver: Arc<dyn ProviderResolver>,
    ) -> Self {
        if self.error.is_none()
            && let Err(error) = self.providers.insert(kind, resolver)
        {
            self.error = Some(error);
        }
        self
    }

    pub fn with_request_metrics(mut self, request_metrics: Arc<RequestStageMetrics>) -> Self {
        self.request_metrics = request_metrics;
        self
    }

    pub fn build(self) -> Result<DataExecution, ExecutionBuildError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.providers.is_empty() {
            return Err(ExecutionBuildError::MissingComponent("provider resolver"));
        }
        let mut replica_routes = BTreeMap::new();
        for observation in self.shards.observations() {
            let binding = observation.binding();
            let key = ReplicaKey::new(
                binding.cluster_id(),
                binding.graph_id(),
                binding.shard_id(),
                binding.replica_id(),
            );
            insert_replica_route(&mut replica_routes, key, binding);
        }
        Ok(DataExecution {
            shards: Mutex::new(self.shards),
            replica_routes: RwLock::new(replica_routes),
            route_lifecycle: RwLock::new(()),
            providers: self.providers,
            stores: Mutex::new(BTreeMap::new()),
            read_views: Mutex::new(BTreeMap::new()),
            vertex_counts: Mutex::new(BTreeMap::new()),
            vertex_count_flights: Mutex::new(BTreeMap::new()),
            adjacencies: Mutex::new(BTreeMap::new()),
            adjacency_flights: Mutex::new(BTreeMap::new()),
            snapshot_csrs: Mutex::new(BTreeMap::new()),
            snapshot_csr_flights: Mutex::new(BTreeMap::new()),
            request_metrics: self.request_metrics,
        })
    }
}
