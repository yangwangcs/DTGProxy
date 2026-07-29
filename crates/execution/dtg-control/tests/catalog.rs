use dtg_control::{
    BackendClass, BindingRole, CatalogCommand, CatalogState, GraphId, ObservedNodeState,
    ObservedReplicaLifecycle, ObservedReplicaState, PlacementEpoch, ProviderKind, ReconcileAction,
    Reconciler, ReplicaBinding, ReplicaBindingRecord, ReplicaId, RetentionPin, ShardId,
    ShardPlacement, Version,
};

fn backend_class(provider: ProviderKind) -> BackendClass {
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
    replica_with_credential(
        shard,
        epoch,
        generation,
        replica,
        class,
        role,
        format!("secret://control/replica-{replica}"),
    )
}

fn replica_with_credential(
    shard: u64,
    epoch: u64,
    generation: u64,
    replica: u64,
    class: &BackendClass,
    role: BindingRole,
    credential_ref: String,
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
        .contract_version(class.contract_version())
        .layout_version(class.layout_version())
        .capability_digest(class.required_capabilities().digest())
        .namespace_id(format!(
            "graph-1-shard-{shard}-generation-{generation}-replica-{replica}"
        ))
        .endpoint_profile_ref(format!("endpoint://node-{replica}"))
        .credential_ref(credential_ref)
        .role(role)
        .build()
        .unwrap();
    ReplicaBindingRecord::new(binding, class.clone()).unwrap()
}

fn placement(
    epoch: u64,
    active_generation: u64,
    backend_class: BackendClass,
    replicas: Vec<ReplicaBindingRecord>,
) -> ShardPlacement {
    ShardPlacement {
        graph_id: GraphId::new(1).unwrap(),
        shard_id: ShardId::new(1).unwrap(),
        placement_epoch: PlacementEpoch::new(epoch).unwrap(),
        active_generation: dtg_control::BackendGeneration::new(active_generation).unwrap(),
        backend_class,
        replicas,
    }
}

fn observed(
    node: &str,
    catalog_version: Version,
    record: &ReplicaBindingRecord,
    lifecycle: ObservedReplicaLifecycle,
) -> ObservedReplicaState {
    ObservedReplicaState::new(
        node.to_owned(),
        catalog_version,
        record.binding().clone(),
        record.backend_class().digest(),
        lifecycle,
    )
    .unwrap()
}

#[test]
fn catalog_commands_use_version_cas_and_preserve_prior_state() {
    let class = backend_class(ProviderKind::Fjall);
    let desired = placement(
        1,
        1,
        class.clone(),
        vec![replica(1, 1, 1, 1, &class, BindingRole::Active)],
    );
    let initial = CatalogState::new();
    let command = CatalogCommand::put_placement(initial.version(), desired.clone());
    let next = initial.apply(command.clone()).unwrap();

    assert_eq!(initial.version(), Version::new(0));
    assert!(
        initial
            .placement(GraphId::new(1).unwrap(), ShardId::new(1).unwrap())
            .is_none()
    );
    assert_eq!(next.version(), Version::new(1));
    assert_eq!(
        next.placement(GraphId::new(1).unwrap(), ShardId::new(1).unwrap()),
        Some(&desired)
    );
    assert_eq!(
        next.apply(command).unwrap_err().code(),
        "DTG-CONTROL-STALE-CATALOG"
    );
}

#[test]
fn lineage_is_append_only_and_generation_class_cannot_be_reused() {
    let fjall = backend_class(ProviderKind::Fjall);
    let postgres = backend_class(ProviderKind::PostgreSql);
    let neo4j = backend_class(ProviderKind::Neo4j);
    let first = placement(
        1,
        1,
        fjall.clone(),
        vec![replica(1, 1, 1, 1, &fjall, BindingRole::Active)],
    );
    let state = CatalogState::new()
        .apply(CatalogCommand::put_placement(Version::new(0), first))
        .unwrap();
    let migrated = placement(
        2,
        2,
        postgres.clone(),
        vec![
            replica(1, 2, 1, 1, &fjall, BindingRole::Retiring),
            replica(1, 2, 2, 2, &postgres, BindingRole::Active),
        ],
    );
    let state = state
        .apply(CatalogCommand::put_placement(state.version(), migrated))
        .unwrap();
    let lineage = state.lineage(GraphId::new(1).unwrap(), ShardId::new(1).unwrap());
    assert_eq!(lineage.len(), 2);
    assert_eq!(lineage[0].generation().get(), 1);
    assert_eq!(lineage[1].generation().get(), 2);
    assert_eq!(lineage[0].backend_class_digest(), fjall.digest());
    assert_eq!(lineage[1].backend_class_digest(), postgres.digest());

    let reused = placement(
        3,
        2,
        neo4j.clone(),
        vec![replica(1, 3, 2, 3, &neo4j, BindingRole::Active)],
    );
    assert_eq!(
        state
            .apply(CatalogCommand::put_placement(state.version(), reused))
            .unwrap_err()
            .code(),
        "DTG-CONTROL-LINEAGE"
    );
}

#[test]
fn retention_pin_blocks_generation_removal_until_released() {
    let fjall = backend_class(ProviderKind::Fjall);
    let postgres = backend_class(ProviderKind::PostgreSql);
    let initial = placement(
        1,
        1,
        fjall.clone(),
        vec![replica(1, 1, 1, 1, &fjall, BindingRole::Active)],
    );
    let state = CatalogState::new()
        .apply(CatalogCommand::put_placement(Version::new(0), initial))
        .unwrap();
    let pin = RetentionPin::new("analytics/job-7".to_owned()).unwrap();
    let state = state
        .apply(CatalogCommand::pin_retention(
            state.version(),
            GraphId::new(1).unwrap(),
            ShardId::new(1).unwrap(),
            dtg_control::BackendGeneration::new(1).unwrap(),
            pin.clone(),
        ))
        .unwrap();
    let migrated = placement(
        2,
        2,
        postgres.clone(),
        vec![replica(1, 2, 2, 2, &postgres, BindingRole::Active)],
    );
    assert_eq!(
        state
            .apply(CatalogCommand::put_placement(
                state.version(),
                migrated.clone(),
            ))
            .unwrap_err()
            .code(),
        "DTG-CONTROL-RETENTION-PINNED"
    );

    let state = state
        .apply(CatalogCommand::unpin_retention(
            state.version(),
            GraphId::new(1).unwrap(),
            ShardId::new(1).unwrap(),
            dtg_control::BackendGeneration::new(1).unwrap(),
            pin,
        ))
        .unwrap();
    let state = state
        .apply(CatalogCommand::put_placement(state.version(), migrated))
        .unwrap();
    assert_eq!(state.version(), Version::new(4));
}

#[test]
fn catalog_rejects_plaintext_credentials() {
    let class = backend_class(ProviderKind::Fjall);
    let binding = ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(1)
        .shard_id(1)
        .placement_epoch(1)
        .replica_id(1)
        .backend_generation(1)
        .backend_class_digest(class.digest())
        .provider_kind(class.provider_kind().clone())
        .contract_version(1)
        .layout_version(1)
        .capability_digest(class.required_capabilities().digest())
        .namespace_id("unsafe")
        .endpoint_profile_ref("endpoint://node-1")
        .credential_ref("password=hunter2")
        .role(BindingRole::Active)
        .build()
        .unwrap();

    assert_eq!(
        ReplicaBindingRecord::new(binding, class)
            .unwrap_err()
            .code(),
        "DTG-CONTROL-CREDENTIAL-PLAINTEXT"
    );
}

#[test]
fn reconciler_is_deterministic_idempotent_and_does_not_mutate_catalog() {
    let fjall = backend_class(ProviderKind::Fjall);
    let postgres = backend_class(ProviderKind::PostgreSql);
    let active = replica(1, 2, 2, 1, &postgres, BindingRole::Active);
    let learner = replica(1, 2, 2, 2, &postgres, BindingRole::Candidate);
    let retiring = replica(1, 2, 1, 3, &fjall, BindingRole::Retiring);
    let desired = placement(
        2,
        2,
        postgres,
        vec![active.clone(), learner.clone(), retiring.clone()],
    );
    let catalog = CatalogState::new()
        .apply(CatalogCommand::put_placement(Version::new(0), desired))
        .unwrap();
    let before = catalog.clone();

    let allocated_learner = observed(
        "node-b",
        catalog.version(),
        &learner,
        ObservedReplicaLifecycle::Allocated,
    );
    let old_leader = observed(
        "node-a",
        catalog.version(),
        &retiring,
        ObservedReplicaLifecycle::Voter,
    )
    .with_leader(true);
    let node_a =
        ObservedNodeState::new("node-a".to_owned(), catalog.version(), vec![old_leader]).unwrap();
    let node_b = ObservedNodeState::new(
        "node-b".to_owned(),
        catalog.version(),
        vec![allocated_learner],
    )
    .unwrap();

    let first = Reconciler::reconcile(&catalog, &[node_b.clone(), node_a.clone()]).unwrap();
    let second = Reconciler::reconcile(&catalog, &[node_a, node_b]).unwrap();
    assert_eq!(first, second);
    assert_eq!(catalog, before);
    assert!(first.iter().any(|action| matches!(
        action,
        ReconcileAction::Allocate { binding }
            if binding.replica_id() == ReplicaId::new(1).unwrap()
    )));
    assert!(first.iter().any(|action| matches!(
        action,
        ReconcileAction::StartLearner { replica_id, .. }
            if *replica_id == ReplicaId::new(2).unwrap()
    )));
    assert!(first.iter().any(|action| matches!(
        action,
        ReconcileAction::Seal { replica_id, .. }
            if *replica_id == ReplicaId::new(3).unwrap()
    )));
    assert!(first.iter().any(|action| matches!(
        action,
        ReconcileAction::Migrate {
            from_generation,
            to_generation,
            ..
        } if from_generation.get() == 1 && to_generation.get() == 2
    )));
}

#[test]
fn reconciler_promotes_transfers_leadership_and_deletes_unpinned_namespace() {
    let fjall = backend_class(ProviderKind::Fjall);
    let postgres = backend_class(ProviderKind::PostgreSql);
    let active = replica(1, 2, 2, 1, &postgres, BindingRole::Active);
    let learner = replica(1, 2, 2, 2, &postgres, BindingRole::Candidate);
    let retiring = replica(1, 2, 1, 3, &fjall, BindingRole::Retiring);
    let desired = placement(
        2,
        2,
        postgres,
        vec![active.clone(), learner.clone(), retiring.clone()],
    );
    let catalog = CatalogState::new()
        .apply(CatalogCommand::put_placement(Version::new(0), desired))
        .unwrap();
    let active_observation = observed(
        "node-a",
        catalog.version(),
        &active,
        ObservedReplicaLifecycle::Voter,
    );
    let learner_observation = observed(
        "node-b",
        catalog.version(),
        &learner,
        ObservedReplicaLifecycle::Learner,
    )
    .with_caught_up(true);
    let retired_leader = observed(
        "node-c",
        catalog.version(),
        &retiring,
        ObservedReplicaLifecycle::Sealed,
    )
    .with_leader(true);
    let observations = vec![
        ObservedNodeState::new("node-c".to_owned(), catalog.version(), vec![retired_leader])
            .unwrap(),
        ObservedNodeState::new(
            "node-a".to_owned(),
            catalog.version(),
            vec![active_observation],
        )
        .unwrap(),
        ObservedNodeState::new(
            "node-b".to_owned(),
            catalog.version(),
            vec![learner_observation],
        )
        .unwrap(),
    ];
    let actions = Reconciler::reconcile(&catalog, &observations).unwrap();

    assert!(actions.iter().any(|action| matches!(
        action,
        ReconcileAction::Promote { replica_id, .. }
            if *replica_id == ReplicaId::new(2).unwrap()
    )));
    assert!(actions.iter().any(|action| matches!(
        action,
        ReconcileAction::TransferLeader { from, to, .. }
            if *from == ReplicaId::new(3).unwrap() && *to == ReplicaId::new(1).unwrap()
    )));
    assert!(actions.iter().any(|action| matches!(
        action,
        ReconcileAction::DeleteNamespace { replica_id, .. }
            if *replica_id == ReplicaId::new(3).unwrap()
    )));
}

#[test]
fn reconciler_suppresses_namespace_deletion_while_generation_is_pinned() {
    let fjall = backend_class(ProviderKind::Fjall);
    let postgres = backend_class(ProviderKind::PostgreSql);
    let retiring = replica(1, 2, 1, 1, &fjall, BindingRole::Retiring);
    let active = replica(1, 2, 2, 2, &postgres, BindingRole::Active);
    let desired = placement(2, 2, postgres, vec![retiring.clone(), active.clone()]);
    let catalog = CatalogState::new()
        .apply(CatalogCommand::put_placement(Version::new(0), desired))
        .unwrap();
    let catalog = catalog
        .apply(CatalogCommand::pin_retention(
            catalog.version(),
            GraphId::new(1).unwrap(),
            ShardId::new(1).unwrap(),
            dtg_control::BackendGeneration::new(1).unwrap(),
            RetentionPin::new("analytics/job-8".to_owned()).unwrap(),
        ))
        .unwrap();
    let observations = [
        ObservedNodeState::new(
            "node-a".to_owned(),
            catalog.version(),
            vec![observed(
                "node-a",
                catalog.version(),
                &retiring,
                ObservedReplicaLifecycle::Sealed,
            )],
        )
        .unwrap(),
        ObservedNodeState::new(
            "node-b".to_owned(),
            catalog.version(),
            vec![observed(
                "node-b",
                catalog.version(),
                &active,
                ObservedReplicaLifecycle::Voter,
            )],
        )
        .unwrap(),
    ];

    let actions = Reconciler::reconcile(&catalog, &observations).unwrap();
    assert!(!actions.iter().any(|action| matches!(
        action,
        ReconcileAction::DeleteNamespace { replica_id, .. }
            if *replica_id == ReplicaId::new(1).unwrap()
    )));
}
