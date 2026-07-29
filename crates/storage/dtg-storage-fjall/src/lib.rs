#![forbid(unsafe_code)]

mod artifact;
mod codec;
mod consensus;
mod graph;
mod namespace;
mod read_view;
mod snapshot;

#[cfg(feature = "tck")]
use std::path::{Path, PathBuf};

#[cfg(feature = "tck")]
use dtg_storage::{
    ApplyReceipt, BackendClass, BindingRole, CommittedShardBatch, LogicalReplicaActivation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, LogicalSnapshotReader,
    LogicalSnapshotSink, LogicalSnapshotSource, LogicalSnapshotWriter, ProviderKind, ReadFence,
    SnapshotRequest, StorageError, StorageTckFactory, StorageTckStore, TemporalReadView,
};
use dtg_storage::{
    CapabilityManifest, PushdownExecutor, PushdownOperation, PushdownOutcome, PushdownRequest,
    ReplicaBinding, ReplicaStateStore, SnapshotRecord, StoreFuture,
};

pub use artifact::FjallArtifactStore;
pub use consensus::FjallConsensusStore;
#[cfg(feature = "tck")]
pub use graph::FjallGraphPause;
pub use graph::FjallReplicaStore;

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;

impl PushdownExecutor for FjallReplicaStore {
    fn binding(&self) -> &ReplicaBinding {
        ReplicaStateStore::binding(self)
    }

    fn capabilities(&self) -> &CapabilityManifest {
        self.capabilities()
    }

    fn execute_pushdown(&self, request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome> {
        Box::pin(async move {
            request.validate()?;
            self.verify_fence(request.fence())?;
            let view = self.begin_read_view(request.fence().clone()).await?;
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
                PushdownOperation::Vertex(_) => guarantees.supports("point"),
                PushdownOperation::VertexScan(_) => guarantees.supports("point"),
            };
            if operation_supported {
                Ok(PushdownOutcome::ResidualRequired { rows, guarantees })
            } else {
                Ok(PushdownOutcome::Unsupported)
            }
        })
    }
}

#[cfg(feature = "tck")]
pub struct FjallStorageTckFactory {
    root: PathBuf,
    capabilities: CapabilityManifest,
}

#[cfg(feature = "tck")]
impl FjallStorageTckFactory {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
            capabilities: CapabilityManifest::from_names([
                "adjacency",
                "immutable-read-view",
                "logical-snapshot",
                "point",
            ])
            .expect("static Fjall capability manifest is valid"),
        }
    }
}

#[cfg(feature = "tck")]
impl StorageTckFactory for FjallStorageTckFactory {
    fn capabilities(&self) -> CapabilityManifest {
        self.capabilities.clone()
    }

    fn binding(
        &self,
        namespace: &str,
        backend_generation: u64,
    ) -> Result<ReplicaBinding, StorageError> {
        let class = BackendClass::new(
            ProviderKind::Fjall,
            1,
            1,
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
            .provider_kind(ProviderKind::Fjall)
            .contract_version(1)
            .layout_version(1)
            .capability_digest(self.capabilities.digest())
            .namespace_id(namespace)
            .endpoint_profile_ref("local-fjall")
            .credential_ref("local-fjall")
            .role(BindingRole::Active)
            .build()
    }

    fn open(&self, binding: ReplicaBinding) -> StoreFuture<'_, Box<dyn StorageTckStore>> {
        Box::pin(async move {
            let path = self.root.join(binding.namespace_id().as_str());
            Ok(Box::new(FjallStorageTckStore {
                store: FjallReplicaStore::open(path, binding)?,
            }) as Box<dyn StorageTckStore>)
        })
    }
}

#[cfg(feature = "tck")]
struct FjallStorageTckStore {
    store: FjallReplicaStore,
}

#[cfg(feature = "tck")]
impl ReplicaStateStore for FjallStorageTckStore {
    fn binding(&self) -> &ReplicaBinding {
        ReplicaStateStore::binding(&self.store)
    }

    fn applied_index(&self) -> StoreFuture<'_, u64> {
        self.store.applied_index()
    }

    fn replica_metadata<'a>(
        &'a self,
        name: &'a str,
    ) -> StoreFuture<'a, Option<dtg_storage::ReplicaMetadata>> {
        self.store.replica_metadata(name)
    }

    fn apply(&self, batch: CommittedShardBatch) -> StoreFuture<'_, ApplyReceipt> {
        self.store.apply(batch)
    }

    fn begin_read_view(&self, fence: ReadFence) -> StoreFuture<'_, Box<dyn TemporalReadView>> {
        self.store.begin_read_view(fence)
    }
}

#[cfg(feature = "tck")]
impl LogicalSnapshotSource for FjallStorageTckStore {
    fn begin_snapshot(
        &self,
        fence: ReadFence,
        request: SnapshotRequest,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotReader>> {
        self.store.begin_snapshot(fence, request)
    }
}

#[cfg(feature = "tck")]
impl LogicalSnapshotSink for FjallStorageTckStore {
    fn begin_restore(
        &self,
        binding: ReplicaBinding,
        header: dtg_storage::SnapshotHeader,
    ) -> StoreFuture<'_, Box<dyn LogicalSnapshotWriter>> {
        self.store.begin_restore(binding, header)
    }
}

#[cfg(feature = "tck")]
impl PushdownExecutor for FjallStorageTckStore {
    fn binding(&self) -> &ReplicaBinding {
        PushdownExecutor::binding(&self.store)
    }

    fn capabilities(&self) -> &CapabilityManifest {
        PushdownExecutor::capabilities(&self.store)
    }

    fn execute_pushdown(&self, request: PushdownRequest) -> StoreFuture<'_, PushdownOutcome> {
        self.store.execute_pushdown(request)
    }
}

#[cfg(feature = "tck")]
impl StorageTckStore for FjallStorageTckStore {
    fn arm_apply_failure_after(&self, staged_mutations: usize) -> Result<(), StorageError> {
        self.store.arm_apply_failure_after(staged_mutations)
    }

    fn activate_candidate(
        &self,
        candidate: LogicalSnapshotCandidateReceipt,
        active_binding: ReplicaBinding,
    ) -> StoreFuture<'_, LogicalReplicaActivationReceipt> {
        LogicalReplicaActivation::activate_candidate(&self.store, candidate, active_binding)
    }
}
