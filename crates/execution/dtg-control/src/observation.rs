use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BackendGeneration, ControlError, Digest32, GraphId, ReplicaBinding, ReplicaId, ShardId, Version,
};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum ObservedReplicaLifecycle {
    Allocated,
    Learner,
    Voter,
    Sealed,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedReplicaState {
    node_id: String,
    catalog_version: Version,
    binding: ReplicaBinding,
    backend_class_digest: Digest32,
    lifecycle: ObservedReplicaLifecycle,
    leader: bool,
    caught_up: bool,
}

impl ObservedReplicaState {
    pub fn new(
        node_id: String,
        catalog_version: Version,
        binding: ReplicaBinding,
        backend_class_digest: Digest32,
        lifecycle: ObservedReplicaLifecycle,
    ) -> Result<Self, ControlError> {
        if node_id.trim().is_empty() {
            return Err(ControlError::InvalidObservation("node identifier is empty"));
        }
        if backend_class_digest != binding.backend_class_digest() {
            return Err(ControlError::InvalidObservation(
                "reported backend class does not match the binding",
            ));
        }
        Ok(Self {
            node_id,
            catalog_version,
            binding,
            backend_class_digest,
            lifecycle,
            leader: false,
            caught_up: false,
        })
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }

    pub const fn backend_class_digest(&self) -> Digest32 {
        self.backend_class_digest
    }

    pub const fn lifecycle(&self) -> ObservedReplicaLifecycle {
        self.lifecycle
    }

    pub const fn is_leader(&self) -> bool {
        self.leader
    }

    pub const fn is_caught_up(&self) -> bool {
        self.caught_up
    }

    #[must_use]
    pub const fn with_leader(mut self, leader: bool) -> Self {
        self.leader = leader;
        self
    }

    #[must_use]
    pub const fn with_caught_up(mut self, caught_up: bool) -> Self {
        self.caught_up = caught_up;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObservedNodeState {
    node_id: String,
    catalog_version: Version,
    replicas: Vec<ObservedReplicaState>,
}

impl ObservedNodeState {
    pub fn new(
        node_id: String,
        catalog_version: Version,
        replicas: Vec<ObservedReplicaState>,
    ) -> Result<Self, ControlError> {
        if node_id.trim().is_empty() {
            return Err(ControlError::InvalidObservation("node identifier is empty"));
        }
        Ok(Self {
            node_id,
            catalog_version,
            replicas,
        })
    }

    pub fn validate(&self) -> Result<(), ControlError> {
        let mut classes = BTreeMap::new();
        let mut replicas = BTreeSet::new();
        for replica in &self.replicas {
            if replica.node_id() != self.node_id {
                return Err(ControlError::InvalidObservation(
                    "replica observation belongs to another node",
                ));
            }
            if replica.catalog_version() != self.catalog_version {
                return Err(ControlError::InvalidObservation(
                    "replica and node catalog versions differ",
                ));
            }
            let binding = replica.binding();
            let group = (
                binding.graph_id(),
                binding.shard_id(),
                binding.backend_generation(),
            );
            match classes.insert(group, replica.backend_class_digest()) {
                Some(digest) if digest != replica.backend_class_digest() => {
                    return Err(ControlError::MixedBackendClass);
                }
                _ => {}
            }
            if !replicas.insert((
                binding.graph_id(),
                binding.shard_id(),
                binding.backend_generation(),
                binding.replica_id(),
            )) {
                return Err(ControlError::InvalidObservation(
                    "duplicate replica observation",
                ));
            }
        }
        Ok(())
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }

    pub fn replicas(&self) -> &[ObservedReplicaState] {
        &self.replicas
    }
}

pub(crate) type ObservationKey = (GraphId, ShardId, BackendGeneration, ReplicaId);

pub(crate) fn observation_key(observation: &ObservedReplicaState) -> ObservationKey {
    let binding = observation.binding();
    (
        binding.graph_id(),
        binding.shard_id(),
        binding.backend_generation(),
        binding.replica_id(),
    )
}
