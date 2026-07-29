use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use dtg_shard::{
    RaftProgress, ReplicaKey, ReplicaObservation, ShardCommand, ShardError, ShardHost,
};
use dtg_storage::{
    ConsensusStore, LogicalReplicaActivation, LogicalSnapshotSink, NamespaceId, ProviderKind,
    PushdownExecutor, ReadFence, ReplicaBinding, ReplicaStateStore, StorageError, StoreFuture,
    TransactionTime, VertexId, VertexRead, VertexScan,
};
use prost_011::Message as _;
use raft::eraftpb::Message;

use crate::{GatewayExecutionError, GatewayFuture, GatewayRetry, GatewayRows, GatewayValue};

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
        self.lock_shards()?.add(consensus_store, state_store)
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
        let mut matches = self
            .lock_shards()?
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
        Ok(key)
    }

    pub fn apply_transaction_command(
        &self,
        key: ReplicaKey,
        command: ShardCommand,
    ) -> Result<RaftProgress, ShardError> {
        let mut shards = self.lock_shards()?;
        shards.propose(key, command)?;
        shards.drive_ready(key)
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
            let view = store
                .state()
                .begin_read_view(ReadFence::new(binding, applied_index))
                .await
                .map_err(data_storage_error)?;
            let rows = match read {
                FragmentRead::VertexPoint(id) => view
                    .get_vertex(VertexRead::new(id, valid_at, transaction_time))
                    .await
                    .map_err(data_storage_error)?
                    .into_iter()
                    .map(|vertex| vertex_row(&vertex))
                    .collect(),
                FragmentRead::VertexScan { after, limit } => view
                    .scan_vertices(
                        VertexScan::new(valid_at, transaction_time, after, limit)
                            .map_err(data_storage_error)?,
                    )
                    .await
                    .map_err(data_storage_error)?
                    .rows()
                    .iter()
                    .map(vertex_row)
                    .collect(),
            };
            GatewayRows::new(vec!["value".into()], rows)
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

    fn lock_shards(&self) -> Result<std::sync::MutexGuard<'_, ShardHost>, ShardError> {
        self.shards
            .lock()
            .map_err(|_| ShardError::InvalidLifecycle("ShardHost mutex is poisoned".into()))
    }
}

enum FragmentRead {
    VertexPoint(VertexId),
    VertexScan { after: Option<VertexId>, limit: u32 },
}

fn decode_fragment_read(encoded: &[u8]) -> Result<FragmentRead, GatewayExecutionError> {
    let mut cursor = FragmentCursor::new(encoded);
    if cursor.u64()? == 0 || cursor.u32()? == 0 {
        return Err(data_error("physical fragment version or root is zero"));
    }
    cursor.skip_row_schema()?;
    if cursor.u32()? == 0 {
        return Err(data_error("physical fragment has no storage access"));
    }
    cursor.u32()?;
    match cursor.u8()? {
        0 => match cursor.u8()? {
            0 => Ok(FragmentRead::VertexPoint(
                VertexId::new(cursor.u128()?).map_err(|error| data_error(error.to_string()))?,
            )),
            1 => Ok(FragmentRead::VertexScan {
                after: None,
                limit: decode_logical_scan_limit(&mut cursor)?,
            }),
            _ => Err(data_error(
                "physical logical access is not executable by the Data worker",
            )),
        },
        1 => {
            if cursor.u32()? == 0 {
                return Err(data_error("pushdown contract version is zero"));
            }
            let capability_count = cursor.len()?;
            for _ in 0..capability_count {
                cursor.string()?;
            }
            match cursor.u8()? {
                0 => {
                    let id = VertexId::new(cursor.u128()?)
                        .map_err(|error| data_error(error.to_string()))?;
                    cursor.i64()?;
                    cursor.i64()?;
                    Ok(FragmentRead::VertexPoint(id))
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
                    Ok(FragmentRead::VertexScan { after, limit })
                }
                _ => Err(data_error("unknown physical pushdown operation")),
            }
        }
        _ => Err(data_error("unknown physical storage access")),
    }
}

fn decode_logical_scan_limit(
    cursor: &mut FragmentCursor<'_>,
) -> Result<u32, GatewayExecutionError> {
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
    let limit = cursor.u32()?;
    if limit == 0 {
        return Err(data_error("logical scan limit is zero"));
    }
    Ok(limit)
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
}

pub struct DataExecutionBuilder {
    shards: ShardHost,
    providers: ProviderResolverSet,
    error: Option<ExecutionBuildError>,
}

impl Default for DataExecutionBuilder {
    fn default() -> Self {
        Self {
            shards: ShardHost::new(),
            providers: ProviderResolverSet::new(),
            error: None,
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
        })
    }
}
