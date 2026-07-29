use std::collections::BTreeMap;
use std::sync::Arc;

use dtg_kernel::{ClusterId, GraphId, ReplicaId, ShardId};
use dtg_storage::{ConsensusStore, ReplicaStateStore};
use raft::eraftpb::Message;

use crate::{
    RaftProgress, RaftReplica, ReplicaLifecycle, ReplicaObservation, ShardCommand, ShardError,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct ReplicaKey {
    cluster_id: ClusterId,
    graph_id: GraphId,
    shard_id: ShardId,
    replica_id: ReplicaId,
}

impl ReplicaKey {
    pub const fn new(
        cluster_id: ClusterId,
        graph_id: GraphId,
        shard_id: ShardId,
        replica_id: ReplicaId,
    ) -> Self {
        Self {
            cluster_id,
            graph_id,
            shard_id,
            replica_id,
        }
    }

    pub const fn cluster_id(self) -> ClusterId {
        self.cluster_id
    }

    pub const fn graph_id(self) -> GraphId {
        self.graph_id
    }

    pub const fn shard_id(self) -> ShardId {
        self.shard_id
    }

    pub const fn replica_id(self) -> ReplicaId {
        self.replica_id
    }
}

#[derive(Default)]
pub struct ShardHost {
    replicas: BTreeMap<ReplicaKey, RaftReplica>,
}

impl ShardHost {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(
        &mut self,
        consensus_store: Arc<dyn ConsensusStore>,
        state_store: Arc<dyn ReplicaStateStore>,
    ) -> Result<ReplicaKey, ShardError> {
        let replica = RaftReplica::open(consensus_store, state_store)?;
        if self.replicas.values().any(|existing| {
            same_active_generation(existing.binding(), replica.binding())
                && existing.binding().backend_class_digest()
                    != replica.binding().backend_class_digest()
        }) {
            return Err(ShardError::HeterogeneousGeneration);
        }
        let key = ReplicaKey::new(
            replica.binding().cluster_id(),
            replica.binding().graph_id(),
            replica.binding().shard_id(),
            replica.binding().replica_id(),
        );
        if self.replicas.contains_key(&key) {
            return Err(ShardError::DuplicateReplica);
        }
        self.replicas.insert(key, replica);
        Ok(key)
    }

    pub fn start(&mut self, key: ReplicaKey) -> Result<(), ShardError> {
        self.replica_mut(key)?.start()
    }

    pub fn stop(&mut self, key: ReplicaKey) -> Result<(), ShardError> {
        self.replica_mut(key)?.stop()
    }

    pub fn campaign(&mut self, key: ReplicaKey) -> Result<(), ShardError> {
        self.replica_mut(key)?.campaign()
    }

    pub fn tick(&mut self, key: ReplicaKey) -> Result<bool, ShardError> {
        self.replica_mut(key)?.tick()
    }

    pub fn step(&mut self, key: ReplicaKey, message: Message) -> Result<(), ShardError> {
        self.replica_mut(key)?.step(message)
    }

    pub fn propose(&mut self, key: ReplicaKey, command: ShardCommand) -> Result<(), ShardError> {
        self.replica_mut(key)?.propose(command)
    }

    pub fn drive_ready(&mut self, key: ReplicaKey) -> Result<RaftProgress, ShardError> {
        self.replica_mut(key)?.drive_ready()
    }

    pub fn transfer_leader(
        &mut self,
        key: ReplicaKey,
        target: ReplicaId,
    ) -> Result<(), ShardError> {
        self.replica_mut(key)?.transfer_leader(target)
    }

    pub fn observe(&self, key: ReplicaKey) -> Result<ReplicaObservation, ShardError> {
        Ok(self.replica(key)?.observe())
    }

    pub fn seal(&mut self, key: ReplicaKey) -> Result<(), ShardError> {
        self.replica_mut(key)?.seal()
    }

    pub fn sealed_remove(&mut self, key: ReplicaKey) -> Result<(), ShardError> {
        if self.replica(key)?.lifecycle() != ReplicaLifecycle::Sealed {
            return Err(ShardError::InvalidLifecycle(
                "replica must be sealed before removal".into(),
            ));
        }
        self.replicas
            .remove(&key)
            .ok_or(ShardError::ReplicaNotFound)?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.replicas.len()
    }

    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }

    fn replica(&self, key: ReplicaKey) -> Result<&RaftReplica, ShardError> {
        self.replicas.get(&key).ok_or(ShardError::ReplicaNotFound)
    }

    fn replica_mut(&mut self, key: ReplicaKey) -> Result<&mut RaftReplica, ShardError> {
        self.replicas
            .get_mut(&key)
            .ok_or(ShardError::ReplicaNotFound)
    }
}

fn same_active_generation(
    left: &dtg_storage::ReplicaBinding,
    right: &dtg_storage::ReplicaBinding,
) -> bool {
    left.cluster_id() == right.cluster_id()
        && left.graph_id() == right.graph_id()
        && left.shard_id() == right.shard_id()
        && left.placement_epoch() == right.placement_epoch()
        && left.backend_generation() == right.backend_generation()
}
