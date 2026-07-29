#![forbid(unsafe_code)]

mod apply;
mod codec;
mod config;
mod read_view;
mod schema;
mod snapshot;

use std::fmt;
use std::sync::{Arc, Mutex};

use dtg_storage::{
    ApplyReceipt, BackendClass, BindingRole, CapabilityManifest, CommittedShardBatch,
    LogicalReplicaActivation, LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt,
    LogicalSnapshotReader, LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter,
    ProviderKind, PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest, ReadFence,
    ReplicaBinding, ReplicaMetadata, ReplicaStateStore, SnapshotHeader, SnapshotRecord,
    SnapshotRequest, StorageError, StorageTckFactory, StorageTckStore, StoreFuture,
    TemporalReadView,
};
use tokio_postgres::Client;

pub use config::PostgresConfig;
use read_view::PostgresReadView;
use schema::{
    POSTGRES_CONTRACT_VERSION, POSTGRES_LAYOUT_VERSION, ensure_serving_binding,
    initialize_namespace, postgres_capabilities, read_applied_index, validate_postgres_binding,
    verify_owner,
};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;

struct PostgresInner {
    config: PostgresConfig,
    schema_name: String,
    binding: ReplicaBinding,
    capabilities: CapabilityManifest,
}

#[derive(Clone)]
pub struct PostgresReplicaStore {
    inner: Arc<PostgresInner>,
}

impl fmt::Debug for PostgresReplicaStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PostgresReplicaStore")
            .field("binding", &self.inner.binding)
            .field("schema_name", &self.inner.schema_name)
            .finish_non_exhaustive()
    }
}

impl PostgresReplicaStore {
    pub async fn open(
        connection_string: impl AsRef<str>,
        binding: ReplicaBinding,
    ) -> Result<Self, StorageError> {
        Self::open_with_config(PostgresConfig::new(connection_string)?, binding).await
    }

    pub async fn open_with_config(
        config: PostgresConfig,
        binding: ReplicaBinding,
    ) -> Result<Self, StorageError> {
        validate_postgres_binding(&binding)?;
        let capabilities = postgres_capabilities()?;
        let schema_name = config::schema_name(binding.namespace_id());
        initialize_namespace(&config, &schema_name, &binding).await?;
        Ok(Self {
            inner: Arc::new(PostgresInner {
                config,
                schema_name,
                binding,
                capabilities,
            }),
        })
    }

    pub fn capabilities(&self) -> &CapabilityManifest {
        &self.inner.capabilities
    }

    pub(crate) fn binding_ref(&self) -> &ReplicaBinding {
        &self.inner.binding
    }

    pub(crate) async fn connect(&self) -> Result<Client, StorageError> {
        self.inner.config.connect(&self.inner.schema_name).await
    }

    pub(crate) async fn open_read_view(
        &self,
        fence: ReadFence,
    ) -> Result<PostgresReadView, StorageError> {
        if fence.binding() != self.binding_ref() {
            return Err(StorageError::StaleBinding {
                expected: Box::new(self.binding_ref().clone()),
                actual: Box::new(fence.binding().clone()),
            });
        }
        if fence.capability_digest() != self.binding_ref().capability_digest() {
            return Err(StorageError::CapabilityDrift);
        }
        let client = self.connect().await?;
        client
            .batch_execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            .await
            .map_err(config::postgres_error)?;
        let result = async {
            verify_owner(&client, self.binding_ref(), false).await?;
            ensure_serving_binding(self.binding_ref())?;
            let applied = read_applied_index(&client).await?;
            if applied != fence.applied_index() {
                return Err(StorageError::ReadFenceUnavailable {
                    requested: fence.applied_index(),
                    applied,
                });
            }
            Ok(())
        }
        .await;
        match result {
            Ok(()) => Ok(PostgresReadView::new(client, fence)),
            Err(error) => {
                let _ = client.batch_execute("ROLLBACK").await;
                Err(error)
            }
        }
    }
}

impl ReplicaStateStore for PostgresReplicaStore {
    fn binding(&self) -> &ReplicaBinding {
        self.binding_ref()
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        Box::pin(async move {
            let client = self.connect().await?;
            verify_owner(&client, self.binding_ref(), false).await?;
            ensure_serving_binding(self.binding_ref())?;
            read_applied_index(&client).await
        })
    }

    fn replica_metadata<'a>(&'a self, name: &'a str) -> StoreFuture<'a, Option<ReplicaMetadata>> {
        Box::pin(async move {
            let client = self.connect().await?;
            verify_owner(&client, self.binding_ref(), false).await?;
            ensure_serving_binding(self.binding_ref())?;
            let row = client
                .query_opt(
                    "SELECT value FROM replica_metadata WHERE name = $1",
                    &[&name],
                )
                .await
                .map_err(config::postgres_error)?;
            row.map(|row| {
                ReplicaMetadata::new(
                    name,
                    codec::decode_value(row.get::<_, Vec<u8>>(0).as_slice())?,
                )
            })
            .transpose()
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

impl LogicalSnapshotSource for PostgresReplicaStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        Box::pin(async move { snapshot::snapshot_reader(self, fence, request).await })
    }
}

impl LogicalSnapshotSink for PostgresReplicaStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        Box::pin(async move { snapshot::snapshot_writer(self, binding, header).await })
    }
}

impl LogicalReplicaActivation for PostgresReplicaStore {
    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt> {
        Box::pin(async move { snapshot::activate_candidate(self, candidate, active_binding).await })
    }
}

impl PushdownExecutor for PostgresReplicaStore {
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

pub struct PostgresStorageTckFactory {
    connection_string: String,
    capabilities: CapabilityManifest,
}

impl PostgresStorageTckFactory {
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            capabilities: postgres_capabilities()
                .expect("static PostgreSQL capability manifest is valid"),
        }
    }
}

impl StorageTckFactory for PostgresStorageTckFactory {
    fn capabilities(&self) -> CapabilityManifest {
        self.capabilities.clone()
    }

    fn binding(
        &self,
        namespace: &str,
        backend_generation: u64,
    ) -> Result<ReplicaBinding, StorageError> {
        let class = BackendClass::new(
            ProviderKind::PostgreSql,
            POSTGRES_CONTRACT_VERSION,
            POSTGRES_LAYOUT_VERSION,
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
            .provider_kind(ProviderKind::PostgreSql)
            .contract_version(POSTGRES_CONTRACT_VERSION)
            .layout_version(POSTGRES_LAYOUT_VERSION)
            .capability_digest(self.capabilities.digest())
            .namespace_id(namespace)
            .endpoint_profile_ref("postgres-tck")
            .credential_ref("postgres-tck")
            .role(BindingRole::Active)
            .build()
    }

    fn open(&self, binding: ReplicaBinding) -> StoreFuture<'_, Box<dyn StorageTckStore>> {
        Box::pin(async move {
            Ok(Box::new(PostgresStorageTckStore {
                store: PostgresReplicaStore::open(&self.connection_string, binding).await?,
                injected_failure_after: Mutex::new(None),
            }) as Box<dyn StorageTckStore>)
        })
    }
}

struct PostgresStorageTckStore {
    store: PostgresReplicaStore,
    injected_failure_after: Mutex<Option<usize>>,
}

impl ReplicaStateStore for PostgresStorageTckStore {
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
                .map_err(|_| {
                    StorageError::Internal("PostgreSQL TCK fault lock is poisoned".into())
                })?
                .take();
            apply::apply_batch(&self.store, batch, failure_after).await
        })
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        self.store.begin_read_view(fence)
    }
}

impl LogicalSnapshotSource for PostgresStorageTckStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        self.store.begin_snapshot(fence, request)
    }
}

impl LogicalSnapshotSink for PostgresStorageTckStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        self.store.begin_restore(binding, header)
    }
}

impl PushdownExecutor for PostgresStorageTckStore {
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

impl StorageTckStore for PostgresStorageTckStore {
    fn arm_apply_failure_after(&self, staged_mutations: usize) -> Result<(), StorageError> {
        *self.injected_failure_after.lock().map_err(|_| {
            StorageError::Internal("PostgreSQL TCK fault lock is poisoned".into())
        })? = Some(staged_mutations);
        Ok(())
    }
}
