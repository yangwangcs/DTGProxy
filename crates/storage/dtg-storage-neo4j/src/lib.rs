#![forbid(unsafe_code)]

mod apply;
mod codec;
mod config;
mod model;
mod read_view;
mod schema;
mod snapshot;

use std::fmt;
use std::sync::{Arc, Mutex};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use dtg_storage::{
    ApplyReceipt, BackendClass, BindingRole, CapabilityManifest, CommittedShardBatch,
    LogicalReplicaActivation, LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt,
    LogicalSnapshotReader, LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter,
    ProviderKind, PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest, ReadFence,
    ReplicaBinding, ReplicaMetadata, ReplicaStateStore, SnapshotHeader, SnapshotRecord,
    SnapshotRequest, StorageError, StorageTckFactory, StorageTckStore, StoreFuture,
    TemporalReadView,
};
use serde_json::Value;

pub use config::{Neo4jConfig, QueryApiContract};
pub use model::NativeModel;
use read_view::Neo4jReadView;
use schema::{
    NEO4J_CONTRACT_VERSION, NEO4J_LAYOUT_VERSION, begin_fenced_transaction, fenced_parameters,
    initialize_namespace, neo4j_capabilities, read_applied_index, validate_neo4j_binding,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;

pub(crate) struct Neo4jInner {
    config: Neo4jConfig,
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
    apply_guard: tokio::sync::Mutex<()>,
}

#[derive(Clone)]
pub struct Neo4jReplicaStore {
    inner: Arc<Neo4jInner>,
}

impl fmt::Debug for Neo4jReplicaStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Neo4jReplicaStore")
            .field("binding", &self.inner.binding)
            .finish_non_exhaustive()
    }
}

impl Neo4jReplicaStore {
    pub async fn open(config: Neo4jConfig, binding: ReplicaBinding) -> Result<Self, StorageError> {
        validate_neo4j_binding(&binding)?;
        let capabilities = neo4j_capabilities()?;
        let client = config.client()?;
        initialize_namespace(&client, &binding).await?;
        Ok(Self {
            inner: Arc::new(Neo4jInner {
                config,
                binding,
                capabilities,
                apply_guard: tokio::sync::Mutex::new(()),
            }),
        })
    }

    pub fn capabilities(&self) -> &CapabilityManifest {
        &self.inner.capabilities
    }

    pub(crate) fn binding_ref(&self) -> &ReplicaBinding {
        &self.inner.binding
    }

    pub(crate) fn client(&self) -> Result<config::QueryApiClient, StorageError> {
        self.inner.config.client()
    }

    pub(crate) async fn open_read_view(
        &self,
        fence: ReadFence,
    ) -> Result<Neo4jReadView, StorageError> {
        if fence.binding() != self.binding_ref() {
            return Err(StorageError::StaleBinding {
                expected: Box::new(self.binding_ref().clone()),
                actual: Box::new(fence.binding().clone()),
            });
        }
        if fence.capability_digest() != self.binding_ref().capability_digest() {
            return Err(StorageError::CapabilityDrift);
        }
        let client = self.client()?;
        let (transaction, applied) =
            begin_fenced_transaction(&client, self.binding_ref(), false).await?;
        if applied != fence.applied_index() {
            let _ = transaction.rollback().await;
            return Err(StorageError::ReadFenceUnavailable {
                requested: fence.applied_index(),
                applied,
            });
        }
        Ok(Neo4jReadView::new(transaction, fence))
    }
}

impl ReplicaStateStore for Neo4jReplicaStore {
    fn binding(&self) -> &ReplicaBinding {
        self.binding_ref()
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async move {
            let client = self.client()?;
            read_applied_index(&client, self.binding_ref()).await
        })
    }

    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async move {
            let client = self.client()?;
            let mut parameters = fenced_parameters(self.binding_ref());
            parameters.insert("key".into(), Value::String(name.to_owned()));
            parameters.insert("limit".into(), Value::from(1));
            let rows = client
                .execute(
                    "MATCH (owner:DtgOwner {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       binding_digest: $binding_digest
                     })
                     OPTIONAL MATCH (metadata:DtgMetadata {
                       namespace_id: $namespace_id,
                       backend_generation: $backend_generation,
                       key: $key
                     })
                     RETURN metadata.value LIMIT $limit",
                    Value::Object(parameters),
                )
                .await?;
            let Some(encoded) = rows
                .first()
                .and_then(|row| row.first())
                .and_then(Value::as_str)
            else {
                return Ok(None);
            };
            let bytes = STANDARD.decode(encoded).map_err(|_| {
                StorageError::Internal("invalid Neo4j metadata value encoding".into())
            })?;
            Ok(Some(ReplicaMetadata::new(
                name,
                codec::decode_value(&bytes)?,
            )?))
        })
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move { apply::apply_batch(self, batch, None).await })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        Box::pin(async move {
            Ok(Box::new(self.open_read_view(fence).await?) as Box<dyn TemporalReadView>)
        })
    }
}

impl LogicalSnapshotSource for Neo4jReplicaStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        Box::pin(async move { snapshot::snapshot_reader(self, fence, request).await })
    }
}

impl LogicalSnapshotSink for Neo4jReplicaStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        Box::pin(async move { snapshot::snapshot_writer(self, binding, header).await })
    }
}

impl LogicalReplicaActivation for Neo4jReplicaStore {
    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt> {
        Box::pin(async move { snapshot::activate_candidate(self, candidate, active_binding).await })
    }
}

impl PushdownExecutor for Neo4jReplicaStore {
    fn binding(&self) -> &ReplicaBinding {
        self.binding_ref()
    }

    fn capabilities(&self) -> &CapabilityManifest {
        self.capabilities()
    }

    fn execute_pushdown(&self, request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome> {
        Box::pin(async move {
            request.validate()?;
            let view = self.open_read_view(request.fence().clone()).await?;
            let rows = match request.operation() {
                PushdownOperation::Vertex(read) => view
                    .get_vertex(read.clone())
                    .await?
                    .into_iter()
                    .map(SnapshotRecord::Vertex)
                    .collect(),
                PushdownOperation::VertexScan(scan) => view
                    .scan_vertices(scan.clone())
                    .await?
                    .rows()
                    .iter()
                    .cloned()
                    .map(SnapshotRecord::Vertex)
                    .collect(),
            };
            if self
                .capabilities()
                .contains_all(request.required_capabilities())
            {
                return Ok(PushdownOutcome::Exact(rows));
            }
            let guarantees = self
                .capabilities()
                .intersection(request.required_capabilities());
            let operation_supported = match request.operation() {
                PushdownOperation::Vertex(_) | PushdownOperation::VertexScan(_) => {
                    guarantees.supports("point")
                }
            };
            if operation_supported {
                Ok(PushdownOutcome::ResidualRequired { rows, guarantees })
            } else {
                Ok(PushdownOutcome::Unsupported)
            }
        })
    }
}

pub struct Neo4jStorageTckFactory {
    config: Neo4jConfig,
    capabilities: CapabilityManifest,
}

impl Neo4jStorageTckFactory {
    pub fn new(config: Neo4jConfig) -> Self {
        Self {
            config,
            capabilities: neo4j_capabilities().expect("static Neo4j capability manifest is valid"),
        }
    }
}

impl StorageTckFactory for Neo4jStorageTckFactory {
    fn capabilities(&self) -> CapabilityManifest {
        self.capabilities.clone()
    }

    fn binding(
        &self,
        namespace: &str,
        backend_generation: u64,
    ) -> Result<ReplicaBinding, StorageError> {
        let class = BackendClass::new(
            ProviderKind::Neo4j,
            NEO4J_CONTRACT_VERSION,
            NEO4J_LAYOUT_VERSION,
            self.capabilities.names().map(str::to_owned),
        )?;
        ReplicaBinding::builder()
            .cluster_id(1)
            .graph_id(7)
            .shard_id(11)
            .placement_epoch(13)
            .replica_id(17)
            .backend_generation(backend_generation)
            .backend_class_digest(class.digest())
            .provider_kind(ProviderKind::Neo4j)
            .contract_version(NEO4J_CONTRACT_VERSION)
            .layout_version(NEO4J_LAYOUT_VERSION)
            .capability_digest(self.capabilities.digest())
            .namespace_id(namespace)
            .endpoint_profile_ref("neo4j-tck")
            .credential_ref("neo4j-tck")
            .role(BindingRole::Active)
            .build()
    }

    fn open(&self, binding: ReplicaBinding) -> StoreFuture<'_, Box<dyn StorageTckStore>> {
        Box::pin(async move {
            Ok(Box::new(Neo4jStorageTckStore {
                store: Neo4jReplicaStore::open(self.config.clone(), binding).await?,
                injected_failure_after: Mutex::new(None),
            }) as Box<dyn StorageTckStore>)
        })
    }
}

struct Neo4jStorageTckStore {
    store: Neo4jReplicaStore,
    injected_failure_after: Mutex<Option<usize>>,
}

impl ReplicaStateStore for Neo4jStorageTckStore {
    fn binding(&self) -> &ReplicaBinding {
        self.store.binding_ref()
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        self.store.applied_index()
    }

    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        self.store.replica_metadata(name)
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        Box::pin(async move {
            let failure_after = self
                .injected_failure_after
                .lock()
                .map_err(|_| StorageError::Internal("Neo4j TCK fault lock is poisoned".into()))?
                .take();
            apply::apply_batch(&self.store, batch, failure_after).await
        })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        self.store.begin_read_view(fence)
    }
}

impl LogicalSnapshotSource for Neo4jStorageTckStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        self.store.begin_snapshot(fence, request)
    }
}

impl LogicalSnapshotSink for Neo4jStorageTckStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        self.store.begin_restore(binding, header)
    }
}

impl PushdownExecutor for Neo4jStorageTckStore {
    fn binding(&self) -> &ReplicaBinding {
        self.store.binding_ref()
    }

    fn capabilities(&self) -> &CapabilityManifest {
        self.store.capabilities()
    }

    fn execute_pushdown(&self, request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome> {
        self.store.execute_pushdown(request)
    }
}

impl StorageTckStore for Neo4jStorageTckStore {
    fn arm_apply_failure_after(&self, staged_mutations: usize) -> Result<(), StorageError> {
        *self
            .injected_failure_after
            .lock()
            .map_err(|_| StorageError::Internal("Neo4j TCK fault lock is poisoned".into()))? =
            Some(staged_mutations);
        Ok(())
    }
}
