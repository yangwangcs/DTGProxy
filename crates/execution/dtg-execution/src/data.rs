use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use dtg_shard::{
    RaftProgress, ReplicaKey, ReplicaObservation, ShardCommand, ShardError, ShardHost,
};
use dtg_storage::{
    ConsensusStore, LogicalReplicaActivation, LogicalSnapshotSink, NamespaceId, ProviderKind,
    PushdownExecutor, ReadFence, ReplicaBinding, ReplicaStateStore, StorageError, StoreFuture,
    TemporalReadView, TransactionTime, VertexId, VertexRead, VertexScan,
};
use prost_011::Message as _;
use raft::eraftpb::Message;

use crate::{
    GatewayExecutionError, GatewayFuture, GatewayRetry, GatewayRows, GatewayValue, RequestDetail,
    RequestStageMetrics, StageOutcome,
};

const PARTIAL_VERTEX_COUNT_FIELD: &str = "__dtg_partial_vertex_count";

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
    providers: ProviderResolverSet,
    stores: Mutex<BTreeMap<ReplicaKey, ResolvedReplicaStore>>,
    read_views: Mutex<BTreeMap<ReplicaKey, CachedReadView>>,
    vertex_counts: Mutex<BTreeMap<ReplicaKey, CachedVertexCount>>,
    request_metrics: Arc<RequestStageMetrics>,
}

pub struct ReplicaLookup {
    key: ReplicaKey,
    lock_wait_nanoseconds: u64,
    lookup_nanoseconds: u64,
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

impl DataExecution {
    pub fn builder() -> DataExecutionBuilder {
        DataExecutionBuilder::default()
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
        let key = self.lock_shards()?.add(consensus_store, state_store)?;
        self.clear_read_view(key)?;
        Ok(key)
    }

    pub fn add_replica_runtime(
        &self,
        consensus_store: Arc<dyn ConsensusStore>,
        runtime_store: ResolvedReplicaStore,
    ) -> Result<ReplicaKey, ShardError> {
        let key = self
            .lock_shards()?
            .add(consensus_store, runtime_store.state().clone())?;
        self.stores
            .lock()
            .map_err(|_| ShardError::InvalidLifecycle("replica store mutex is poisoned".into()))?
            .insert(key, runtime_store);
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
        let lock_started = Instant::now();
        let shards = self.lock_shards()?;
        let lock_wait_nanoseconds = elapsed_nanoseconds(lock_started);
        let lookup_started = Instant::now();
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
        let lock_started = Instant::now();
        let mut shards = self.lock_shards()?;
        let lock_wait_nanoseconds = elapsed_nanoseconds(lock_started);
        let propose_started = Instant::now();
        shards.propose(key, command)?;
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
            let read = decode_fragment_read(encoded)?;
            let fence = ReadFence::new(binding, applied_index);
            let view = self
                .read_view(key, store.state().clone(), fence.clone())
                .await?;
            let diagnostics_before = view.diagnostics();
            let scan = matches!(
                &read,
                FragmentRead::Scan { .. } | FragmentRead::Count { .. }
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
                    let count =
                        match self.cached_vertex_count(key, store.state(), &fence, &scope)? {
                            Some(count) => count,
                            None => {
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
                                self.cache_vertex_count(
                                    key,
                                    store.state(),
                                    &fence,
                                    &view,
                                    scope,
                                    count,
                                )?;
                                count
                            }
                        };
                    (
                        PARTIAL_VERTEX_COUNT_FIELD,
                        vec![vec![GatewayValue::Integer(count)]],
                    )
                }
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
}

fn elapsed_nanoseconds(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX)
}

enum FragmentRead {
    Point(VertexId),
    Scan { after: Option<VertexId>, limit: u32 },
    Count { after: Option<VertexId>, limit: u32 },
}

fn decode_fragment_read(encoded: &[u8]) -> Result<FragmentRead, GatewayExecutionError> {
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
                decode_logical_read_scope(&mut cursor)?;
                if cursor.u32()? != 1 {
                    return Err(data_error("logical point row bound must equal one"));
                }
                FragmentRead::Point(id)
            }
            1 => {
                decode_logical_read_scope(&mut cursor)?;
                let limit = cursor.u32()?;
                if limit == 0 {
                    return Err(data_error("logical scan limit is zero"));
                }
                FragmentRead::Scan { after: None, limit }
            }
            2 => {
                decode_logical_read_scope(&mut cursor)?;
                let limit = cursor.u32()?;
                if limit == 0 {
                    return Err(data_error("logical count row bound is zero"));
                }
                FragmentRead::Count { after: None, limit }
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

fn decode_logical_read_scope(cursor: &mut FragmentCursor<'_>) -> Result<(), GatewayExecutionError> {
    match cursor.u8()? {
        0 => {}
        1 => match cursor.u8()? {
            0 => {
                cursor.i64()?;
            }
            _ => return Err(data_error("parameterized temporal scopes must be resolved")),
        },
        2 => {
            return Err(data_error(
                "CHANGES fragment scan requires a dedicated worker",
            ));
        }
        _ => return Err(data_error("invalid temporal scope tag")),
    }
    match cursor.u8()? {
        0 => {}
        1 => match cursor.u8()? {
            0 => {
                cursor.i64()?;
            }
            _ => return Err(data_error("parameterized valid time must be resolved")),
        },
        _ => return Err(data_error("unsupported valid-time fragment predicate")),
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
        Ok(DataExecution {
            shards: Mutex::new(self.shards),
            providers: self.providers,
            stores: Mutex::new(BTreeMap::new()),
            read_views: Mutex::new(BTreeMap::new()),
            vertex_counts: Mutex::new(BTreeMap::new()),
            request_metrics: self.request_metrics,
        })
    }
}
