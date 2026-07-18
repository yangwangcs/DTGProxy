use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::{Mutex, RwLock};

use raft::eraftpb::Message;
use raft_transport::RoutedRaftMessage;
use storage_api::LogicalKey;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

use crate::replica_actor::{ActorCommand, ReplicaActorHandle};
use crate::{
    NodeConfig, NodeIdentityStore, ReplicaEntry, ReplicaManifestStore, ReplicaRole, StorageError,
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
    pub fn relative_directory(&self) -> &str {
        self.entry.relative_directory()
    }

    fn key(&self) -> ReplicaKey {
        ReplicaKey {
            graph_id: self.graph_id(),
            shard_id: self.shard_id(),
        }
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
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnsureReplicaOutcome {
    Created,
    Existing,
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
    replicas: RwLock<BTreeMap<ReplicaKey, ReplicaActorHandle>>,
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
        let manifest_store = ReplicaManifestStore::open(config.data_directory())?;
        let entries = manifest_store
            .manifest()
            .replicas()
            .cloned()
            .collect::<Vec<_>>();
        let mut replicas = BTreeMap::new();
        for entry in entries {
            let spec = ReplicaSpec::from_entry(entry);
            validate_local_replica(config.identity().node_id(), &spec)?;
            let handle = ReplicaActorHandle::open(
                config.identity().node_id(),
                config.data_directory(),
                spec.clone(),
                queue_capacity,
            )
            .await?;
            replicas.insert(spec.key(), handle);
        }
        Ok(Self {
            config,
            _identity_store: identity_store,
            manifest_store: Mutex::new(manifest_store),
            replicas: RwLock::new(replicas),
            ensure_gate: AsyncMutex::new(()),
            queue_capacity,
        })
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
        let handle = ReplicaActorHandle::open(
            self.config.identity().node_id(),
            self.config.data_directory(),
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
        let sender = self.sender(key)?;
        let (response, receiver) = oneshot::channel();
        sender
            .send(ActorCommand::Status(response))
            .await
            .map_err(|_| HostError::ActorStopped)?;
        receiver.await.map_err(|_| HostError::ActorStopped)?
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
        let handle = replicas.get(&key).ok_or(HostError::UnknownReplica {
            graph_id: key.graph_id,
            shard_id: key.shard_id,
        })?;
        Ok((handle.sender(), handle.spec.placement_epoch()))
    }
}

fn validate_local_replica(node_id: u64, spec: &ReplicaSpec) -> Result<(), HostError> {
    if spec.role() == ReplicaRole::Learner {
        return Err(HostError::LearnerNotYetSupported);
    }
    if !spec.voters().contains(&node_id) {
        return Err(HostError::LocalNodeNotVoter { node_id });
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HostError {
    Storage(StorageError),
    Io(String),
    DurableReplica(String),
    Adapter(String),
    InvalidQueueCapacity,
    InvalidReplicaKey,
    UnknownReplica { graph_id: u64, shard_id: u32 },
    ReplicaSpecConflict { graph_id: u64, shard_id: u32 },
    LocalNodeNotVoter { node_id: u64 },
    LearnerNotYetSupported,
    WrongCluster,
    WrongTarget { expected: u64, actual: u64 },
    RequestEnvelopeMismatch { expected: u128, actual: u128 },
    RequestMismatch { request_id: u128 },
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
            Self::Io(message) => write!(formatter, "data node I/O error: {message}"),
            Self::DurableReplica(message) => write!(formatter, "durable Replica error: {message}"),
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
            Self::LocalNodeNotVoter { node_id } => {
                write!(formatter, "local node {node_id} is not a voter")
            }
            Self::LearnerNotYetSupported => {
                formatter.write_str("learner Replica hosting is not connected yet")
            }
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
