use std::collections::{BTreeMap, BTreeSet};

use crate::observation::{ObservationKey, observation_key};
use crate::{
    BackendGeneration, BindingRole, CatalogState, ControlError, GraphId, NamespaceId,
    ObservedNodeState, ObservedReplicaLifecycle, ObservedReplicaState, PlacementEpoch,
    ReplicaBinding, ReplicaId, ShardId, ShardPlacement,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReconcileAction {
    Allocate {
        binding: ReplicaBinding,
    },
    StartLearner {
        graph_id: GraphId,
        shard_id: ShardId,
        placement_epoch: PlacementEpoch,
        backend_generation: BackendGeneration,
        replica_id: ReplicaId,
    },
    Promote {
        graph_id: GraphId,
        shard_id: ShardId,
        placement_epoch: PlacementEpoch,
        backend_generation: BackendGeneration,
        replica_id: ReplicaId,
    },
    TransferLeader {
        graph_id: GraphId,
        shard_id: ShardId,
        placement_epoch: PlacementEpoch,
        from: ReplicaId,
        to: ReplicaId,
    },
    Seal {
        graph_id: GraphId,
        shard_id: ShardId,
        placement_epoch: PlacementEpoch,
        backend_generation: BackendGeneration,
        replica_id: ReplicaId,
    },
    Migrate {
        graph_id: GraphId,
        shard_id: ShardId,
        placement_epoch: PlacementEpoch,
        from_generation: BackendGeneration,
        to_generation: BackendGeneration,
    },
    DeleteNamespace {
        graph_id: GraphId,
        shard_id: ShardId,
        placement_epoch: PlacementEpoch,
        backend_generation: BackendGeneration,
        replica_id: ReplicaId,
        namespace_id: NamespaceId,
    },
}

pub struct Reconciler;

impl Reconciler {
    pub fn reconcile(
        catalog: &CatalogState,
        observed_nodes: &[ObservedNodeState],
    ) -> Result<Vec<ReconcileAction>, ControlError> {
        let observations = collect_observations(catalog, observed_nodes)?;
        let mut actions = Vec::new();
        for placement in catalog.placements() {
            reconcile_placement(catalog, placement, &observations, &mut actions);
        }
        Ok(actions)
    }
}

fn collect_observations<'a>(
    catalog: &CatalogState,
    observed_nodes: &'a [ObservedNodeState],
) -> Result<BTreeMap<ObservationKey, &'a ObservedReplicaState>, ControlError> {
    let mut observations = BTreeMap::new();
    for node in observed_nodes {
        node.validate()?;
        if node.catalog_version() != catalog.version() {
            return Err(ControlError::StaleObservation {
                expected: catalog.version(),
                actual: node.catalog_version(),
            });
        }
        for replica in node.replicas() {
            if replica.catalog_version() != catalog.version() {
                return Err(ControlError::StaleObservation {
                    expected: catalog.version(),
                    actual: replica.catalog_version(),
                });
            }
            if observations
                .insert(observation_key(replica), replica)
                .is_some()
            {
                return Err(ControlError::InvalidObservation(
                    "replica was reported by multiple nodes",
                ));
            }
        }
    }
    Ok(observations)
}

fn reconcile_placement(
    catalog: &CatalogState,
    placement: &ShardPlacement,
    observations: &BTreeMap<ObservationKey, &ObservedReplicaState>,
    actions: &mut Vec<ReconcileAction>,
) {
    let mut replicas = placement.replicas.iter().collect::<Vec<_>>();
    replicas.sort_by_key(|record| ShardPlacement::replica_key(record));

    for record in &replicas {
        let binding = record.binding();
        if binding.role() == BindingRole::Retiring {
            continue;
        }
        match observations.get(&key_for_binding(binding)) {
            None => actions.push(ReconcileAction::Allocate {
                binding: binding.clone(),
            }),
            Some(observation) if observation.lifecycle() == ObservedReplicaLifecycle::Allocated => {
                actions.push(ReconcileAction::StartLearner {
                    graph_id: placement.graph_id,
                    shard_id: placement.shard_id,
                    placement_epoch: placement.placement_epoch,
                    backend_generation: binding.backend_generation(),
                    replica_id: binding.replica_id(),
                });
            }
            Some(observation)
                if observation.lifecycle() == ObservedReplicaLifecycle::Learner
                    && observation.is_caught_up() =>
            {
                actions.push(ReconcileAction::Promote {
                    graph_id: placement.graph_id,
                    shard_id: placement.shard_id,
                    placement_epoch: placement.placement_epoch,
                    backend_generation: binding.backend_generation(),
                    replica_id: binding.replica_id(),
                });
            }
            Some(_) => {}
        }
    }

    let active_voter = replicas
        .iter()
        .filter(|record| {
            let binding = record.binding();
            binding.backend_generation() == placement.active_generation
                && binding.role() != BindingRole::Retiring
        })
        .filter_map(|record| observations.get(&key_for_binding(record.binding())))
        .filter(|observation| observation.lifecycle() == ObservedReplicaLifecycle::Voter)
        .map(|observation| observation.binding().replica_id())
        .min();
    if let Some(to) = active_voter {
        for record in &replicas {
            let binding = record.binding();
            if binding.role() != BindingRole::Retiring {
                continue;
            }
            if let Some(observation) = observations.get(&key_for_binding(binding))
                && observation.is_leader()
            {
                actions.push(ReconcileAction::TransferLeader {
                    graph_id: placement.graph_id,
                    shard_id: placement.shard_id,
                    placement_epoch: placement.placement_epoch,
                    from: binding.replica_id(),
                    to,
                });
            }
        }
    }

    for record in &replicas {
        let binding = record.binding();
        if binding.role() != BindingRole::Retiring {
            continue;
        }
        if let Some(observation) = observations.get(&key_for_binding(binding))
            && observation.lifecycle() != ObservedReplicaLifecycle::Sealed
        {
            actions.push(ReconcileAction::Seal {
                graph_id: placement.graph_id,
                shard_id: placement.shard_id,
                placement_epoch: placement.placement_epoch,
                backend_generation: binding.backend_generation(),
                replica_id: binding.replica_id(),
            });
        }
    }

    let active_incomplete = replicas
        .iter()
        .filter(|record| {
            let binding = record.binding();
            binding.backend_generation() == placement.active_generation
                && binding.role() != BindingRole::Retiring
        })
        .any(|record| {
            observations
                .get(&key_for_binding(record.binding()))
                .is_none_or(|observation| {
                    observation.lifecycle() != ObservedReplicaLifecycle::Voter
                })
        });
    if active_incomplete {
        let old_generations = replicas
            .iter()
            .filter_map(|record| {
                let binding = record.binding();
                (binding.backend_generation() != placement.active_generation
                    && observations.contains_key(&key_for_binding(binding)))
                .then_some(binding.backend_generation())
            })
            .collect::<BTreeSet<_>>();
        for from_generation in old_generations {
            actions.push(ReconcileAction::Migrate {
                graph_id: placement.graph_id,
                shard_id: placement.shard_id,
                placement_epoch: placement.placement_epoch,
                from_generation,
                to_generation: placement.active_generation,
            });
        }
    }

    for record in &replicas {
        let binding = record.binding();
        if binding.role() != BindingRole::Retiring
            || catalog.is_generation_pinned(
                placement.graph_id,
                placement.shard_id,
                binding.backend_generation(),
            )
        {
            continue;
        }
        if let Some(observation) = observations.get(&key_for_binding(binding))
            && observation.lifecycle() == ObservedReplicaLifecycle::Sealed
        {
            actions.push(ReconcileAction::DeleteNamespace {
                graph_id: placement.graph_id,
                shard_id: placement.shard_id,
                placement_epoch: placement.placement_epoch,
                backend_generation: binding.backend_generation(),
                replica_id: binding.replica_id(),
                namespace_id: binding.namespace_id().clone(),
            });
        }
    }
}

fn key_for_binding(binding: &ReplicaBinding) -> ObservationKey {
    (
        binding.graph_id(),
        binding.shard_id(),
        binding.backend_generation(),
        binding.replica_id(),
    )
}
