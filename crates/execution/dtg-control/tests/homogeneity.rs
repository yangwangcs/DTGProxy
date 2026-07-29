use dtg_control::{
    BackendClass, BindingRole, CatalogCommand, CatalogState, GraphId, ObservedNodeState,
    ObservedReplicaLifecycle, ObservedReplicaState, PlacementEpoch, ProviderKind, Reconciler,
    ReplicaBinding, ReplicaBindingRecord, ShardId, ShardPlacement, Version,
};

fn class(provider: ProviderKind) -> BackendClass {
    BackendClass::new(provider, 1, 1, ["point-read"]).unwrap()
}

fn replica(
    shard: u64,
    epoch: u64,
    generation: u64,
    replica: u64,
    class: &BackendClass,
    role: BindingRole,
) -> ReplicaBindingRecord {
    let binding = ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(1)
        .shard_id(shard)
        .placement_epoch(epoch)
        .replica_id(replica)
        .backend_generation(generation)
        .backend_class_digest(class.digest())
        .provider_kind(class.provider_kind().clone())
        .contract_version(1)
        .layout_version(1)
        .capability_digest(class.required_capabilities().digest())
        .namespace_id(format!("shard-{shard}-gen-{generation}-replica-{replica}"))
        .endpoint_profile_ref(format!("endpoint://node-{replica}"))
        .credential_ref(format!("secret://control/replica-{replica}"))
        .role(role)
        .build()
        .unwrap();
    ReplicaBindingRecord::new(binding, class.clone()).unwrap()
}

fn placement_with_replicas(
    backend_class: BackendClass,
    replicas: Vec<ReplicaBindingRecord>,
) -> ShardPlacement {
    ShardPlacement {
        graph_id: GraphId::new(1).unwrap(),
        shard_id: ShardId::new(1).unwrap(),
        placement_epoch: PlacementEpoch::new(1).unwrap(),
        active_generation: dtg_control::BackendGeneration::new(1).unwrap(),
        backend_class,
        replicas,
    }
}

fn observation(
    node: &str,
    version: Version,
    record: &ReplicaBindingRecord,
) -> ObservedReplicaState {
    ObservedReplicaState::new(
        node.to_owned(),
        version,
        record.binding().clone(),
        record.backend_class().digest(),
        ObservedReplicaLifecycle::Voter,
    )
    .unwrap()
}

#[test]
fn one_generation_rejects_mixed_backend_classes() {
    let fjall = class(ProviderKind::Fjall);
    let postgres = class(ProviderKind::PostgreSql);
    let placement = placement_with_replicas(
        fjall.clone(),
        vec![
            replica(1, 1, 1, 1, &fjall, BindingRole::Active),
            replica(1, 1, 1, 2, &postgres, BindingRole::Active),
        ],
    );
    assert_eq!(
        placement.validate().unwrap_err().code(),
        "DTG-CONTROL-MIXED-BACKEND-CLASS"
    );
}

#[test]
fn different_generations_accept_different_backend_classes() {
    let fjall = class(ProviderKind::Fjall);
    let postgres = class(ProviderKind::PostgreSql);
    let placement = ShardPlacement {
        graph_id: GraphId::new(1).unwrap(),
        shard_id: ShardId::new(1).unwrap(),
        placement_epoch: PlacementEpoch::new(2).unwrap(),
        active_generation: dtg_control::BackendGeneration::new(2).unwrap(),
        backend_class: postgres.clone(),
        replicas: vec![
            replica(1, 2, 1, 1, &fjall, BindingRole::Retiring),
            replica(1, 2, 2, 2, &postgres, BindingRole::Active),
        ],
    };
    assert!(placement.validate().is_ok());
}

#[test]
fn one_node_accepts_independent_heterogeneous_shards() {
    let fjall = class(ProviderKind::Fjall);
    let neo4j = class(ProviderKind::Neo4j);
    let first = replica(1, 1, 1, 1, &fjall, BindingRole::Active);
    let second = replica(2, 1, 1, 2, &neo4j, BindingRole::Active);
    let node = ObservedNodeState::new(
        "node-a".to_owned(),
        Version::new(1),
        vec![
            observation("node-a", Version::new(1), &first),
            observation("node-a", Version::new(1), &second),
        ],
    )
    .unwrap();
    assert!(node.validate().is_ok());
}

#[test]
fn one_node_rejects_mixed_classes_inside_one_shard_generation() {
    let fjall = class(ProviderKind::Fjall);
    let neo4j = class(ProviderKind::Neo4j);
    let first = replica(1, 1, 1, 1, &fjall, BindingRole::Active);
    let second = replica(1, 1, 1, 2, &neo4j, BindingRole::Active);
    let node = ObservedNodeState::new(
        "node-a".to_owned(),
        Version::new(1),
        vec![
            observation("node-a", Version::new(1), &first),
            observation("node-a", Version::new(1), &second),
        ],
    )
    .unwrap();
    assert_eq!(
        node.validate().unwrap_err().code(),
        "DTG-CONTROL-MIXED-BACKEND-CLASS"
    );
}

#[test]
fn reconciler_rejects_stale_observations() {
    let fjall = class(ProviderKind::Fjall);
    let record = replica(1, 1, 1, 1, &fjall, BindingRole::Active);
    let placement = placement_with_replicas(fjall, vec![record.clone()]);
    let catalog = CatalogState::new()
        .apply(CatalogCommand::put_placement(Version::new(0), placement))
        .unwrap();
    let stale = ObservedNodeState::new(
        "node-a".to_owned(),
        Version::new(0),
        vec![observation("node-a", Version::new(0), &record)],
    )
    .unwrap();

    assert_eq!(
        Reconciler::reconcile(&catalog, &[stale])
            .unwrap_err()
            .code(),
        "DTG-CONTROL-STALE-OBSERVATION"
    );
}
