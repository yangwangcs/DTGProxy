use std::collections::{BTreeMap, BTreeSet};

use crate::{
    BackendClass, BackendGeneration, BindingRole, ControlError, Digest32, GraphId, PlacementEpoch,
    ReplicaBindingRecord, ReplicaId, ShardId,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardPlacement {
    pub graph_id: GraphId,
    pub shard_id: ShardId,
    pub placement_epoch: PlacementEpoch,
    pub active_generation: BackendGeneration,
    pub backend_class: BackendClass,
    pub replicas: Vec<ReplicaBindingRecord>,
}

impl ShardPlacement {
    pub fn validate(&self) -> Result<(), ControlError> {
        if self.replicas.is_empty() {
            return Err(ControlError::InvalidPlacement(
                "at least one replica binding is required",
            ));
        }

        let mut generation_classes = BTreeMap::new();
        let mut replicas = BTreeSet::new();
        for record in &self.replicas {
            let binding = record.binding();
            if binding.graph_id() != self.graph_id
                || binding.shard_id() != self.shard_id
                || binding.placement_epoch() != self.placement_epoch
            {
                return Err(ControlError::InvalidPlacement(
                    "binding identity does not match placement identity",
                ));
            }
            if !replicas.insert((binding.backend_generation(), binding.replica_id())) {
                return Err(ControlError::InvalidPlacement("duplicate replica binding"));
            }

            match generation_classes.insert(
                binding.backend_generation(),
                record.backend_class().digest(),
            ) {
                Some(digest) if digest != record.backend_class().digest() => {
                    return Err(ControlError::MixedBackendClass);
                }
                _ => {}
            }

            if binding.backend_generation() == self.active_generation {
                if binding.role() == BindingRole::Retiring {
                    return Err(ControlError::InvalidPlacement(
                        "active-generation replicas cannot be retiring",
                    ));
                }
            } else if binding.role() != BindingRole::Retiring {
                return Err(ControlError::InvalidPlacement(
                    "non-active generations must be retiring",
                ));
            }
        }

        match generation_classes.get(&self.active_generation) {
            Some(digest) if *digest == self.backend_class.digest() => Ok(()),
            Some(_) => Err(ControlError::MixedBackendClass),
            None => Err(ControlError::InvalidPlacement(
                "active generation has no replica binding",
            )),
        }
    }

    pub(crate) fn generation_classes(&self) -> BTreeMap<BackendGeneration, Digest32> {
        let mut classes = BTreeMap::new();
        classes.insert(self.active_generation, self.backend_class.digest());
        for replica in &self.replicas {
            classes.insert(
                replica.binding().backend_generation(),
                replica.backend_class().digest(),
            );
        }
        classes
    }

    pub(crate) fn replica_key(record: &ReplicaBindingRecord) -> (BackendGeneration, ReplicaId) {
        (
            record.binding().backend_generation(),
            record.binding().replica_id(),
        )
    }
}
