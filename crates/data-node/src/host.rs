use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{Arc, Mutex, RwLock};

use adapter_registry::MigrationStatus;
use raft::eraftpb::Message;
use raft_transport::RoutedRaftMessage;
use shard_runtime::{BackendLifecycle, ReplicaMetadata};
use storage_api::{KeySpan, KeyValue, LogicalKey};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

use crate::replica_actor::{ActorCommand, ReplicaActorHandle};
use crate::{
    BackendManager, BackendSlotState, ChunkAppendOutcome, MigrationChunk, MigrationReceipt,
    MigrationReceiptStore, MigrationStorageError, NodeConfig, NodeIdentityStore,
    ReceiptWriteOutcome, ReplicaEntry, ReplicaManifestStore, ReplicaRole, SnapshotInbox,
    StorageError,
};

const MAX_QUEUE_CAPACITY: usize = 65_536;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ReplicaKey {
    graph_id: u64,
    shard_id: u32,
}

impl ReplicaKey {
    pub fn new(graph_id: u64, shard_id: u32) -> Result<Self, HostError> {
        if graph_id == 0 || shard_id == 0 {
            return Err(HostError::InvalidReplicaKey);
        }
        Ok(Self { graph_id, shard_id })
    }

    #[must_use]
    pub const fn graph_id(self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn shard_id(self) -> u32 {
        self.shard_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaSpec {
    entry: ReplicaEntry,
}

impl ReplicaSpec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
        voters: Vec<u64>,
        role: ReplicaRole,
        schema_version: u64,
        backend_generation: u64,
        relative_directory: impl Into<String>,
    ) -> Result<Self, HostError> {
        Ok(Self {
            entry: ReplicaEntry::new(
                graph_id,
                shard_id,
                placement_epoch,
                voters,
                role,
                schema_version,
                backend_generation,
                relative_directory,
            )?,
        })
    }

    pub(crate) const fn from_entry(entry: ReplicaEntry) -> Self {
        Self { entry }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_backend(
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
        voters: Vec<u64>,
        role: ReplicaRole,
        schema_version: u64,
        backend_slot: BackendSlotState,
        relative_directory: impl Into<String>,
    ) -> Result<Self, HostError> {
        Ok(Self {
            entry: ReplicaEntry::new_with_backend(
                graph_id,
                shard_id,
                placement_epoch,
                voters,
                role,
                schema_version,
                backend_slot,
                relative_directory,
            )?,
        })
    }

    pub(crate) fn entry(&self) -> &ReplicaEntry {
        &self.entry
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.entry.graph_id()
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.entry.shard_id()
    }

    #[must_use]
    pub const fn placement_epoch(&self) -> u64 {
        self.entry.placement_epoch()
    }

    #[must_use]
    pub fn voters(&self) -> &[u64] {
        self.entry.voters()
    }

    #[must_use]
    pub const fn role(&self) -> ReplicaRole {
        self.entry.role()
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.entry.schema_version()
    }

    #[must_use]
    pub const fn backend_generation(&self) -> u64 {
        self.entry.backend_generation()
    }

    #[must_use]
    pub const fn backend_slot(&self) -> &BackendSlotState {
        self.entry.backend_slot()
    }

    #[must_use]
    pub const fn snapshot_index(&self) -> u64 {
        self.entry.snapshot_index()
    }

    #[must_use]
    pub fn relative_directory(&self) -> &str {
        self.entry.relative_directory()
    }

    fn key(&self) -> ReplicaKey {
        ReplicaKey {
            graph_id: self.graph_id(),
            shard_id: self.shard_id(),
        }
    }

    pub(crate) fn reconcile_backend(
        &self,
        metadata: ReplicaMetadata,
        local_status: MigrationStatus,
    ) -> Result<Self, HostError> {
        let backend_slot = match metadata.backend_lifecycle {
            BackendLifecycle::Active => match local_status {
                MigrationStatus::DualApplying {
                    source_generation, ..
                } if source_generation == metadata.backend_generation => {
                    self.backend_slot().clone()
                }
                MigrationStatus::Idle { generation }
                    if generation == metadata.backend_generation =>
                {
                    self.backend_slot().reconcile_active(generation)?
                }
                _ => return Err(StorageError::InvalidBackendTransition.into()),
            },
            BackendLifecycle::DualApplying {
                target_generation,
                target_profile_digest,
                fence_index,
            } => self.backend_slot().validate_replicated_dual(
                metadata.backend_generation,
                target_generation,
                target_profile_digest,
                fence_index,
            )?,
        };
        Ok(Self::from_entry(
            self.entry.clone().with_backend_slot(backend_slot)?,
        ))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaStatus {
    graph_id: u64,
    shard_id: u32,
    placement_epoch: u64,
    node_id: u64,
    leader: bool,
    leader_id: Option<u64>,
    term: u64,
    commit_index: u64,
    applied_index: u64,
    role: ReplicaRole,
    schema_version: u64,
    backend_generation: u64,
    snapshot_index: u64,
    ready: bool,
}

impl ReplicaStatus {
    #[allow(clippy::too_many_arguments)]
    pub(crate) const fn new(
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
        node_id: u64,
        leader: bool,
        leader_id: Option<u64>,
        term: u64,
        commit_index: u64,
        applied_index: u64,
        role: ReplicaRole,
        schema_version: u64,
        backend_generation: u64,
        snapshot_index: u64,
        ready: bool,
    ) -> Self {
        Self {
            graph_id,
            shard_id,
            placement_epoch,
            node_id,
            leader,
            leader_id,
            term,
            commit_index,
            applied_index,
            role,
            schema_version,
            backend_generation,
            snapshot_index,
            ready,
        }
    }

    #[must_use]
    pub const fn graph_id(self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn shard_id(self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(self) -> u64 {
        self.placement_epoch
    }

    #[must_use]
    pub const fn node_id(self) -> u64 {
        self.node_id
    }

    #[must_use]
    pub const fn is_leader(self) -> bool {
        self.leader
    }

    #[must_use]
    pub const fn leader_id(self) -> Option<u64> {
        self.leader_id
    }

    #[must_use]
    pub const fn applied_index(self) -> u64 {
        self.applied_index
    }

    #[must_use]
    pub const fn term(self) -> u64 {
        self.term
    }

    #[must_use]
    pub const fn commit_index(self) -> u64 {
        self.commit_index
    }

    #[must_use]
    pub const fn role(self) -> ReplicaRole {
        self.role
    }

    #[must_use]
    pub const fn schema_version(self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn backend_generation(self) -> u64 {
        self.backend_generation
    }

    #[must_use]
    pub const fn snapshot_index(self) -> u64 {
        self.snapshot_index
    }

    #[must_use]
    pub const fn ready(self) -> bool {
        self.ready
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnsureReplicaOutcome {
    Created,
    Existing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendRuntimeStatus {
    replica: ReplicaStatus,
    slot: BackendSlotState,
    local: MigrationStatus,
}

impl BackendRuntimeStatus {
    #[must_use]
    pub const fn replica(&self) -> ReplicaStatus {
        self.replica
    }

    #[must_use]
    pub const fn slot(&self) -> &BackendSlotState {
        &self.slot
    }

    #[must_use]
    pub const fn local(&self) -> MigrationStatus {
        self.local
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProposalOutcome {
    status: ReplicaStatus,
    duplicate: bool,
}

impl ProposalOutcome {
    pub(crate) const fn new(status: ReplicaStatus, duplicate: bool) -> Self {
        Self { status, duplicate }
    }

    #[must_use]
    pub const fn status(self) -> ReplicaStatus {
        self.status
    }

    #[must_use]
    pub const fn duplicate(self) -> bool {
        self.duplicate
    }
}

pub struct DataNodeHost {
    config: NodeConfig,
    _identity_store: NodeIdentityStore,
    manifest_store: Mutex<ReplicaManifestStore>,
    migration_receipts: Mutex<MigrationReceiptStore>,
    snapshot_inbox: SnapshotInbox,
    backend_manager: Arc<BackendManager>,
    replicas: RwLock<BTreeMap<ReplicaKey, ReplicaActorHandle>>,
    dormant_learners: RwLock<BTreeMap<ReplicaKey, ReplicaSpec>>,
    ensure_gate: AsyncMutex<()>,
    queue_capacity: usize,
}

impl DataNodeHost {
    #[must_use]
    pub const fn identity(&self) -> &crate::NodeIdentity {
        self.config.identity()
    }

    pub async fn open(config: NodeConfig, queue_capacity: usize) -> Result<Self, HostError> {
        if queue_capacity == 0 || queue_capacity > MAX_QUEUE_CAPACITY {
            return Err(HostError::InvalidQueueCapacity);
        }
        let identity_store =
            NodeIdentityStore::open_or_create(config.data_directory(), config.identity().clone())?;
        let mut manifest_store = ReplicaManifestStore::open(config.data_directory())?;
        let migration_receipts = MigrationReceiptStore::open(config.data_directory())?;
        let snapshot_inbox = SnapshotInbox::open(config.data_directory())?;
        let backend_manager = Arc::new(
            BackendManager::production().map_err(|error| HostError::Adapter(error.to_string()))?,
        );
        let entries = manifest_store
            .manifest()
            .replicas()
            .cloned()
            .collect::<Vec<_>>();
        let mut replicas = BTreeMap::new();
        let mut dormant_learners = BTreeMap::new();
        for entry in entries {
            let spec = ReplicaSpec::from_entry(entry);
            validate_local_replica(config.identity().node_id(), &spec)?;
            if spec.role() == ReplicaRole::Learner && spec.snapshot_index() == 0 {
                dormant_learners.insert(spec.key(), spec);
                continue;
            }
            let handle = ReplicaActorHandle::open(
                config.identity().node_id(),
                config.data_directory(),
                Arc::clone(&backend_manager),
                spec.clone(),
                queue_capacity,
            )
            .await?;
            if handle.spec != spec {
                let mut manifest = manifest_store.manifest().clone();
                manifest.reconcile_backend(handle.spec.entry().clone())?;
                manifest_store.persist(&manifest)?;
            }
            replicas.insert(spec.key(), handle);
        }
        Ok(Self {
            config,
            _identity_store: identity_store,
            manifest_store: Mutex::new(manifest_store),
            migration_receipts: Mutex::new(migration_receipts),
            snapshot_inbox,
            backend_manager,
            replicas: RwLock::new(replicas),
            dormant_learners: RwLock::new(dormant_learners),
            ensure_gate: AsyncMutex::new(()),
            queue_capacity,
        })
    }

    #[must_use]
    pub fn data_directory(&self) -> &std::path::Path {
        self.config.data_directory()
    }

    pub fn replica_keys(&self) -> Result<Vec<ReplicaKey>, HostError> {
        Ok(self
            .replicas
            .read()
            .map_err(|_| HostError::LockPoisoned)?
            .keys()
            .copied()
            .collect())
    }

    pub fn append_migration_chunk(
        &self,
        chunk: MigrationChunk,
    ) -> Result<ChunkAppendOutcome, HostError> {
        self.snapshot_inbox.append(chunk).map_err(HostError::from)
    }

    pub fn migration_receipt(
        &self,
        migration_id: [u8; 16],
        step: u32,
    ) -> Result<Option<MigrationReceipt>, HostError> {
        self.migration_receipts
            .lock()
            .map_err(|_| HostError::LockPoisoned)
            .map(|store| store.get(migration_id, step).cloned())
    }

    pub fn record_migration_receipt(
        &self,
        migration_id: [u8; 16],
        step: u32,
        input_digest: [u8; 32],
        outcome: Vec<u8>,
    ) -> Result<ReceiptWriteOutcome, HostError> {
        self.migration_receipts
            .lock()
            .map_err(|_| HostError::LockPoisoned)?
            .record(migration_id, step, input_digest, outcome)
            .map_err(HostError::from)
    }

    pub async fn ensure_replica(
        &self,
        spec: ReplicaSpec,
    ) -> Result<EnsureReplicaOutcome, HostError> {
        let _guard = self.ensure_gate.lock().await;
        validate_local_replica(self.config.identity().node_id(), &spec)?;
        if let Some(existing) = self
            .replicas
            .read()
            .map_err(|_| HostError::LockPoisoned)?
            .get(&spec.key())
        {
            return if existing.spec == spec {
                Ok(EnsureReplicaOutcome::Existing)
            } else {
                Err(HostError::ReplicaSpecConflict {
                    graph_id: spec.graph_id(),
                    shard_id: spec.shard_id(),
                })
            };
        }
        if let Some(existing) = self
            .dormant_learners
            .read()
            .map_err(|_| HostError::LockPoisoned)?
            .get(&spec.key())
        {
            return if existing == &spec {
                Ok(EnsureReplicaOutcome::Existing)
            } else {
                Err(HostError::ReplicaSpecConflict {
                    graph_id: spec.graph_id(),
                    shard_id: spec.shard_id(),
                })
            };
        }
        if spec.role() == ReplicaRole::Learner {
            let mut store = self
                .manifest_store
                .lock()
                .map_err(|_| HostError::LockPoisoned)?;
            let mut manifest = store.manifest().clone();
            manifest.insert(spec.entry().clone())?;
            store.persist(&manifest)?;
            self.dormant_learners
                .write()
                .map_err(|_| HostError::LockPoisoned)?
                .insert(spec.key(), spec);
            return Ok(EnsureReplicaOutcome::Created);
        }
        let handle = ReplicaActorHandle::open(
            self.config.identity().node_id(),
            self.config.data_directory(),
            Arc::clone(&self.backend_manager),
            spec.clone(),
            self.queue_capacity,
        )
        .await?;
        let persist_result = (|| {
            let mut store = self
                .manifest_store
                .lock()
                .map_err(|_| HostError::LockPoisoned)?;
            let mut manifest = store.manifest().clone();
            manifest.insert(spec.entry().clone())?;
            store.persist(&manifest)?;
            Ok(())
        })();
        if let Err(error) = persist_result {
            let _ = handle.shutdown().await;
            return Err(error);
        }
        self.replicas
            .write()
            .map_err(|_| HostError::LockPoisoned)?
            .insert(spec.key(), handle);
        Ok(EnsureReplicaOutcome::Created)
    }

    pub async fn campaign(&self, key: ReplicaKey) -> Result<ReplicaStatus, HostError> {
        let sender = self.sender(key)?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::Campaign(response))
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
    }

    pub async fn prepare_backend_target(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        target_generation: u64,
        target_profile: crate::BackendProfile,
    ) -> Result<ReplicaStatus, HostError> {
        let sender = self.sender(key)?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::PrepareBackendTarget {
                placement_epoch,
                target_generation,
                target_profile,
                response,
            })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        let (spec, status) = receiver.await.map_err(|_| HostError::ActorStopped)??;
        {
            let mut store = self
                .manifest_store
                .lock()
                .map_err(|_| HostError::LockPoisoned)?;
            let mut manifest = store.manifest().clone();
            manifest.reconcile_backend(spec.entry().clone())?;
            store.persist(&manifest)?;
        }
        let mut replicas = self.replicas.write().map_err(|_| HostError::LockPoisoned)?;
        let handle = replicas.get_mut(&key).ok_or(HostError::UnknownReplica {
            graph_id: key.graph_id,
            shard_id: key.shard_id,
        })?;
        handle.spec = spec;
        Ok(status)
    }

    pub async fn backend_runtime_status(
        &self,
        key: ReplicaKey,
    ) -> Result<BackendRuntimeStatus, HostError> {
        let sender = self.sender(key)?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::BackendState(response))
            .await
            .map_err(|_| HostError::ActorStopped)?;
        let (spec, replica, local) = receiver.await.map_err(|_| HostError::ActorStopped)??;
        Ok(BackendRuntimeStatus {
            replica,
            slot: spec.backend_slot().clone(),
            local,
        })
    }

    pub async fn propose_backend_transition(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        request_id: u128,
        command: Vec<u8>,
    ) -> Result<ProposalOutcome, HostError> {
        let outcome = self
            .propose_with_outcome(key, placement_epoch, request_id, command)
            .await?;
        let sender = self.sender(key)?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::BackendState(response))
            .await
            .map_err(|_| HostError::ActorStopped)?;
        let (spec, _, _) = receiver.await.map_err(|_| HostError::ActorStopped)??;
        {
            let mut store = self
                .manifest_store
                .lock()
                .map_err(|_| HostError::LockPoisoned)?;
            let mut manifest = store.manifest().clone();
            manifest.reconcile_backend(spec.entry().clone())?;
            store.persist(&manifest)?;
        }
        let mut replicas = self.replicas.write().map_err(|_| HostError::LockPoisoned)?;
        let handle = replicas.get_mut(&key).ok_or(HostError::UnknownReplica {
            graph_id: key.graph_id,
            shard_id: key.shard_id,
        })?;
        handle.spec = spec;
        Ok(outcome)
    }

    pub async fn propose(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        request_id: u128,
        command: Vec<u8>,
    ) -> Result<ReplicaStatus, HostError> {
        self.propose_with_outcome(key, placement_epoch, request_id, command)
            .await
            .map(ProposalOutcome::status)
    }

    pub async fn propose_with_outcome(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        request_id: u128,
        command: Vec<u8>,
    ) -> Result<ProposalOutcome, HostError> {
        let (sender, expected_epoch) = self.sender_and_epoch(key)?;
        if placement_epoch != expected_epoch {
            return Err(HostError::StaleEpoch {
                expected: expected_epoch,
                actual: placement_epoch,
            });
        }
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::Propose {
                request_id,
                command,
                response,
            })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
    }

    pub async fn proposal_status(
        &self,
        key: ReplicaKey,
        request_id: u128,
        command: Vec<u8>,
    ) -> Result<ProposalOutcome, HostError> {
        let sender = self.sender(key)?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::ProposalStatus {
                request_id,
                command,
                response,
            })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
    }

    pub async fn step(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        message: Message,
    ) -> Result<ReplicaStatus, HostError> {
        let (sender, expected_epoch) = self.sender_and_epoch(key)?;
        if placement_epoch != expected_epoch {
            return Err(HostError::StaleEpoch {
                expected: expected_epoch,
                actual: placement_epoch,
            });
        }
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::Step {
                message: Box::new(message),
                response,
            })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
    }

    pub async fn step_routed(&self, routed: RoutedRaftMessage) -> Result<ReplicaStatus, HostError> {
        if routed.route().cluster_id() != self.config.identity().cluster_id() {
            return Err(HostError::WrongCluster);
        }
        let expected_target = self.config.identity().node_id();
        if routed.message().to != expected_target {
            return Err(HostError::WrongTarget {
                expected: expected_target,
                actual: routed.message().to,
            });
        }
        let key = ReplicaKey::new(routed.route().graph_id(), routed.route().shard_id())?;
        let placement_epoch = routed.route().placement_epoch();
        self.step(key, placement_epoch, routed.into_message()).await
    }

    pub fn try_tick(&self, key: ReplicaKey) -> Result<(), HostError> {
        self.sender(key)?
            .try_send(ActorCommand::Tick)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => HostError::Overloaded {
                    graph_id: key.graph_id,
                    shard_id: key.shard_id,
                },
                mpsc::error::TrySendError::Closed(_) => HostError::ActorStopped,
            })
    }

    pub async fn status(&self, key: ReplicaKey) -> Result<ReplicaStatus, HostError> {
        if let Some(spec) = self
            .dormant_learners
            .read()
            .map_err(|_| HostError::LockPoisoned)?
            .get(&key)
        {
            return Ok(dormant_status(self.identity().node_id(), spec));
        }
        let sender = self.sender(key)?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::Status(response))
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
    }

    pub fn learner_replica_directory(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
    ) -> Result<std::path::PathBuf, HostError> {
        let learners = self
            .dormant_learners
            .read()
            .map_err(|_| HostError::LockPoisoned)?;
        let spec = learners.get(&key).ok_or(HostError::UnknownReplica {
            graph_id: key.graph_id,
            shard_id: key.shard_id,
        })?;
        if spec.placement_epoch() != placement_epoch {
            return Err(HostError::StaleEpoch {
                expected: spec.placement_epoch(),
                actual: placement_epoch,
            });
        }
        Ok(self.data_directory().join(spec.relative_directory()))
    }

    pub async fn mark_learner_snapshot(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        snapshot_index: u64,
    ) -> Result<ReplicaStatus, HostError> {
        if snapshot_index == 0 {
            return Err(HostError::InvalidSnapshotIndex);
        }
        let current = self
            .dormant_learners
            .read()
            .map_err(|_| HostError::LockPoisoned)?
            .get(&key)
            .cloned()
            .ok_or(HostError::UnknownReplica {
                graph_id: key.graph_id,
                shard_id: key.shard_id,
            })?;
        if current.placement_epoch() != placement_epoch {
            return Err(HostError::StaleEpoch {
                expected: current.placement_epoch(),
                actual: placement_epoch,
            });
        }
        let entry = current
            .entry()
            .clone()
            .with_snapshot_index(snapshot_index)?;
        let spec = ReplicaSpec::from_entry(entry.clone());
        let status = dormant_status(self.identity().node_id(), &spec);
        {
            let mut store = self
                .manifest_store
                .lock()
                .map_err(|_| HostError::LockPoisoned)?;
            let mut manifest = store.manifest().clone();
            manifest.replace(entry)?;
            store.persist(&manifest)?;
        }
        let handle = ReplicaActorHandle::open(
            self.identity().node_id(),
            self.data_directory(),
            Arc::clone(&self.backend_manager),
            spec.clone(),
            self.queue_capacity,
        )
        .await?;
        self.dormant_learners
            .write()
            .map_err(|_| HostError::LockPoisoned)?
            .remove(&key);
        self.replicas
            .write()
            .map_err(|_| HostError::LockPoisoned)?
            .insert(key, handle);
        Ok(status)
    }

    pub async fn multi_get(
        &self,
        key: ReplicaKey,
        keys: Vec<LogicalKey>,
    ) -> Result<Vec<Option<Vec<u8>>>, HostError> {
        let sender = self.sender(key)?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::MultiGet { keys, response })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
    }

    pub async fn scan(&self, key: ReplicaKey, span: KeySpan) -> Result<Vec<KeyValue>, HostError> {
        let sender = self.sender(key)?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::Scan { span, response })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
    }

    pub async fn create_snapshot(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        destination: std::path::PathBuf,
    ) -> Result<replica_snapshot::SnapshotManifestV1, HostError> {
        let (sender, actual_epoch) = self.sender_and_epoch(key)?;
        if placement_epoch != actual_epoch {
            return Err(HostError::StaleEpoch {
                expected: actual_epoch,
                actual: placement_epoch,
            });
        }
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::CreateSnapshot {
                destination,
                response,
            })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn change_membership(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        operation_id: u128,
        old_voters: Vec<u64>,
        new_voters: Vec<u64>,
        learners: Vec<u64>,
    ) -> Result<(ReplicaStatus, bool), HostError> {
        let (sender, actual_epoch) = self.sender_and_epoch(key)?;
        if placement_epoch != actual_epoch {
            return Err(HostError::StaleEpoch {
                expected: actual_epoch,
                actual: placement_epoch,
            });
        }
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::ChangeMembership {
                operation_id,
                old_voters,
                new_voters,
                learners,
                response,
            })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
    }

    pub async fn delete_replica(
        &self,
        key: ReplicaKey,
        placement_epoch: u64,
        operation_id: u128,
        minimum_safe_index: u64,
    ) -> Result<bool, HostError> {
        let status = match self.status(key).await {
            Ok(status) => status,
            Err(HostError::UnknownReplica { .. }) => return Ok(false),
            Err(error) => return Err(error),
        };
        if status.placement_epoch() != placement_epoch {
            return Err(HostError::StaleEpoch {
                expected: status.placement_epoch(),
                actual: placement_epoch,
            });
        }
        if status.applied_index() < minimum_safe_index || status.is_leader() {
            return Err(HostError::UnsafeReplicaDelete {
                applied: status.applied_index(),
                minimum: minimum_safe_index,
            });
        }
        let handle = self
            .replicas
            .write()
            .map_err(|_| HostError::LockPoisoned)?
            .remove(&key);
        let dormant = self
            .dormant_learners
            .write()
            .map_err(|_| HostError::LockPoisoned)?
            .remove(&key);
        let relative_directory = handle
            .as_ref()
            .map(|handle| handle.spec.relative_directory().to_owned())
            .or_else(|| {
                dormant
                    .as_ref()
                    .map(|spec| spec.relative_directory().to_owned())
            })
            .ok_or(HostError::UnknownReplica {
                graph_id: key.graph_id,
                shard_id: key.shard_id,
            })?;
        if let Some(handle) = handle {
            handle.shutdown().await?;
        }
        {
            let mut store = self
                .manifest_store
                .lock()
                .map_err(|_| HostError::LockPoisoned)?;
            let mut manifest = store.manifest().clone();
            manifest.remove(key.graph_id, key.shard_id);
            store.persist(&manifest)?;
        }
        let source = self.data_directory().join(relative_directory);
        if source.exists() {
            let trash = self
                .data_directory()
                .join("trash")
                .join(format!("{operation_id:032x}"));
            std::fs::create_dir_all(
                trash
                    .parent()
                    .expect("trash destination always has a parent"),
            )
            .map_err(HostError::from_io)?;
            std::fs::rename(source, trash).map_err(HostError::from_io)?;
        }
        Ok(true)
    }

    pub async fn activate_replica(
        &self,
        key: ReplicaKey,
        source_epoch: u64,
        target_epoch: u64,
        voters: Vec<u64>,
    ) -> Result<(ReplicaStatus, bool), HostError> {
        let _guard = self.ensure_gate.lock().await;
        let sender = self.sender(key)?;
        let current_epoch = self
            .manifest_store
            .lock()
            .map_err(|_| HostError::LockPoisoned)?
            .manifest()
            .get(key.graph_id, key.shard_id)
            .ok_or(HostError::UnknownReplica {
                graph_id: key.graph_id,
                shard_id: key.shard_id,
            })?
            .placement_epoch();
        if current_epoch != source_epoch && current_epoch != target_epoch {
            return Err(HostError::StaleEpoch {
                expected: current_epoch,
                actual: source_epoch,
            });
        }
        let (prepared_sender, prepared_receiver) = oneshot::channel();
        sender
            .send(ActorCommand::PrepareActivation {
                target_epoch,
                voters,
                response: prepared_sender,
            })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        let activated = prepared_receiver
            .await
            .map_err(|_| HostError::ActorStopped)??;
        let duplicate = current_epoch == target_epoch;
        {
            let mut store = self
                .manifest_store
                .lock()
                .map_err(|_| HostError::LockPoisoned)?;
            let mut manifest = store.manifest().clone();
            manifest.activate(activated.entry().clone())?;
            store.persist(&manifest)?;
        }
        let (commit_sender, commit_receiver) = oneshot::channel();
        sender
            .send(ActorCommand::CommitActivation {
                spec: Box::new(activated),
                response: commit_sender,
            })
            .await
            .map_err(|_| HostError::ActorStopped)?;
        let status = commit_receiver
            .await
            .map_err(|_| HostError::ActorStopped)??;
        Ok((status, duplicate))
    }

    pub async fn take_outbound(
        &self,
        key: ReplicaKey,
        maximum: usize,
    ) -> Result<Vec<Message>, HostError> {
        let outbound = {
            let replicas = self.replicas.read().map_err(|_| HostError::LockPoisoned)?;
            let handle = replicas.get(&key).ok_or(HostError::UnknownReplica {
                graph_id: key.graph_id,
                shard_id: key.shard_id,
            })?;
            handle.outbound()
        };
        Ok(ReplicaActorHandle::take_outbound_from(&outbound, maximum).await)
    }

    pub async fn shutdown(self) -> Result<(), HostError> {
        let handles = {
            let mut replicas = self.replicas.write().map_err(|_| HostError::LockPoisoned)?;
            std::mem::take(&mut *replicas)
        };
        let mut first_error = None;
        for handle in handles.into_values() {
            if let Err(error) = handle.shutdown().await
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn sender(&self, key: ReplicaKey) -> Result<mpsc::Sender<ActorCommand>, HostError> {
        self.sender_and_epoch(key).map(|(sender, _)| sender)
    }

    fn sender_and_epoch(
        &self,
        key: ReplicaKey,
    ) -> Result<(mpsc::Sender<ActorCommand>, u64), HostError> {
        let replicas = self.replicas.read().map_err(|_| HostError::LockPoisoned)?;
        if self
            .dormant_learners
            .read()
            .map_err(|_| HostError::LockPoisoned)?
            .contains_key(&key)
        {
            return Err(HostError::ReplicaNotReady {
                graph_id: key.graph_id,
                shard_id: key.shard_id,
            });
        }
        let handle = replicas.get(&key).ok_or(HostError::UnknownReplica {
            graph_id: key.graph_id,
            shard_id: key.shard_id,
        })?;
        let epoch = self
            .manifest_store
            .lock()
            .map_err(|_| HostError::LockPoisoned)?
            .manifest()
            .get(key.graph_id, key.shard_id)
            .ok_or(HostError::UnknownReplica {
                graph_id: key.graph_id,
                shard_id: key.shard_id,
            })?
            .placement_epoch();
        Ok((handle.sender(), epoch))
    }
}

fn validate_local_replica(node_id: u64, spec: &ReplicaSpec) -> Result<(), HostError> {
    if spec.role() == ReplicaRole::Voter && !spec.voters().contains(&node_id) {
        return Err(HostError::LocalNodeNotVoter { node_id });
    }
    Ok(())
}

fn dormant_status(node_id: u64, spec: &ReplicaSpec) -> ReplicaStatus {
    ReplicaStatus::new(
        spec.graph_id(),
        spec.shard_id(),
        spec.placement_epoch(),
        node_id,
        false,
        None,
        0,
        0,
        spec.snapshot_index(),
        spec.role(),
        spec.schema_version(),
        spec.backend_generation(),
        spec.snapshot_index(),
        spec.snapshot_index() > 0,
    )
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostError {
    Storage(StorageError),
    MigrationStorage(MigrationStorageError),
    Io(String),
    DurableReplica(String),
    Snapshot(String),
    Adapter(String),
    InvalidQueueCapacity,
    InvalidReplicaKey,
    UnknownReplica { graph_id: u64, shard_id: u32 },
    ReplicaSpecConflict { graph_id: u64, shard_id: u32 },
    ReplicaNotReady { graph_id: u64, shard_id: u32 },
    LocalNodeNotVoter { node_id: u64 },
    LearnerNotYetSupported,
    InvalidSnapshotIndex,
    NotLeader { leader_id: Option<u64> },
    MembershipConflict,
    MembershipPending,
    UnsafeReplicaDelete { applied: u64, minimum: u64 },
    ActivationFenceMismatch,
    WrongCluster,
    WrongTarget { expected: u64, actual: u64 },
    RequestEnvelopeMismatch { expected: u128, actual: u128 },
    RequestMismatch { request_id: u128 },
    ProposalPending { request_id: u128 },
    StaleEpoch { expected: u64, actual: u64 },
    Overloaded { graph_id: u64, shard_id: u32 },
    OutboundOverloaded,
    ActorStopped,
    ReadyLoopLimit,
    LockPoisoned,
    Join(String),
}

impl HostError {
    pub(crate) fn from_io(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }

    pub(crate) fn from_durable(error: shard_runtime::DurableReplicaError) -> Self {
        Self::DurableReplica(error.to_string())
    }

    pub(crate) fn from_runtime(error: shard_runtime::ShardRuntimeError) -> Self {
        match error {
            shard_runtime::ShardRuntimeError::RequestEnvelopeMismatch { expected, actual } => {
                Self::RequestEnvelopeMismatch { expected, actual }
            }
            shard_runtime::ShardRuntimeError::RequestMismatch { request_id } => {
                Self::RequestMismatch { request_id }
            }
            other => Self::DurableReplica(other.to_string()),
        }
    }
}

impl Display for HostError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "node storage error: {error}"),
            Self::MigrationStorage(error) => write!(formatter, "migration storage error: {error}"),
            Self::Io(message) => write!(formatter, "data node I/O error: {message}"),
            Self::DurableReplica(message) => write!(formatter, "durable Replica error: {message}"),
            Self::Snapshot(message) => write!(formatter, "snapshot error: {message}"),
            Self::Adapter(message) => write!(formatter, "Adapter error: {message}"),
            Self::InvalidQueueCapacity => formatter.write_str("invalid actor queue capacity"),
            Self::InvalidReplicaKey => formatter.write_str("invalid Replica key"),
            Self::UnknownReplica { graph_id, shard_id } => {
                write!(formatter, "unknown Replica ({graph_id}, {shard_id})")
            }
            Self::ReplicaSpecConflict { graph_id, shard_id } => write!(
                formatter,
                "Replica ({graph_id}, {shard_id}) already has a different specification"
            ),
            Self::ReplicaNotReady { graph_id, shard_id } => {
                write!(formatter, "Replica ({graph_id}, {shard_id}) is not ready")
            }
            Self::LocalNodeNotVoter { node_id } => {
                write!(formatter, "local node {node_id} is not a voter")
            }
            Self::LearnerNotYetSupported => {
                formatter.write_str("learner Replica hosting is not connected yet")
            }
            Self::InvalidSnapshotIndex => formatter.write_str("invalid snapshot index"),
            Self::NotLeader { leader_id } => {
                write!(
                    formatter,
                    "Replica is not leader; current leader is {leader_id:?}"
                )
            }
            Self::MembershipConflict => {
                formatter.write_str("Raft membership differs from the expected voter set")
            }
            Self::MembershipPending => {
                formatter.write_str("Raft membership change has not committed yet")
            }
            Self::UnsafeReplicaDelete { applied, minimum } => write!(
                formatter,
                "Replica applied index {applied} is below safe delete index {minimum} or it is still leader"
            ),
            Self::ActivationFenceMismatch => formatter.write_str(
                "Replica epoch or Raft membership does not satisfy the activation fence",
            ),
            Self::WrongCluster => formatter.write_str("routed Raft message has another cluster"),
            Self::WrongTarget { expected, actual } => write!(
                formatter,
                "routed Raft message targets node {actual}; expected {expected}"
            ),
            Self::RequestEnvelopeMismatch { expected, actual } => write!(
                formatter,
                "proposal request ID {expected} differs from command request ID {actual}"
            ),
            Self::RequestMismatch { request_id } => write!(
                formatter,
                "request {request_id} was retried with different command bytes"
            ),
            Self::ProposalPending { request_id } => {
                write!(formatter, "proposal {request_id} has not applied yet")
            }
            Self::StaleEpoch { expected, actual } => write!(
                formatter,
                "stale placement epoch {actual}; expected {expected}"
            ),
            Self::Overloaded { graph_id, shard_id } => {
                write!(formatter, "Replica ({graph_id}, {shard_id}) queue is full")
            }
            Self::OutboundOverloaded => formatter.write_str("Replica outbound queue is full"),
            Self::ActorStopped => formatter.write_str("Replica actor has stopped"),
            Self::ReadyLoopLimit => formatter.write_str("Replica Ready loop did not quiesce"),
            Self::LockPoisoned => formatter.write_str("data node lock is poisoned"),
            Self::Join(message) => write!(formatter, "Replica actor join error: {message}"),
        }
    }
}

impl Error for HostError {}

impl From<StorageError> for HostError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

impl From<MigrationStorageError> for HostError {
    fn from(error: MigrationStorageError) -> Self {
        Self::MigrationStorage(error)
    }
}
