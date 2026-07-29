use dtg_control::{
    BackendClass, BackendGeneration, BindingRole, Digest32, GraphId, MigrationId, MigrationReceipt,
    MigrationRecord, MigrationState, PlacementEpoch, ProviderKind, ReplicaBinding, ReplicaId,
    ShardId, Version,
};

fn class(provider: ProviderKind) -> BackendClass {
    BackendClass::new(provider, 1, 1, ["logical-snapshot", "ordered-mirror"]).unwrap()
}

fn binding(
    backend_class: &BackendClass,
    generation: u64,
    role: BindingRole,
    namespace: &str,
) -> ReplicaBinding {
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(1)
        .shard_id(1)
        .placement_epoch(7)
        .replica_id(1)
        .backend_generation(generation)
        .backend_class_digest(backend_class.digest())
        .provider_kind(backend_class.provider_kind().clone())
        .contract_version(backend_class.contract_version())
        .layout_version(backend_class.layout_version())
        .capability_digest(backend_class.required_capabilities().digest())
        .namespace_id(namespace)
        .endpoint_profile_ref("endpoint://fixture/node-1")
        .credential_ref("secret://fixture/node-1")
        .role(role)
        .build()
        .unwrap()
}

fn exercise_direction(source: ProviderKind, target: ProviderKind, migration_id: u128) {
    let source_class = class(source.clone());
    let target_class = class(target.clone());
    let source_binding = binding(&source_class, 3, BindingRole::Active, "fixture-source");
    let target_binding = binding(&target_class, 4, BindingRole::Candidate, "fixture-target");
    let source_owner = source_binding.identity_digest();
    let target_owner = target_binding.identity_digest();
    let replica_id = ReplicaId::new(1).unwrap();
    let mut migration = MigrationRecord::new_direction(
        MigrationId::new(migration_id).unwrap(),
        GraphId::new(1).unwrap(),
        ShardId::new(1).unwrap(),
        Version::new(12),
        PlacementEpoch::new(7).unwrap(),
        BackendGeneration::new(3).unwrap(),
        source_class.clone(),
        BackendGeneration::new(4).unwrap(),
        target_class.clone(),
        vec![replica_id],
    )
    .unwrap();

    assert_eq!(
        migration.provider_direction(),
        Some((source_class.provider_kind(), target_class.provider_kind()))
    );
    let allocate = migration
        .claim_target_namespace(replica_id, source_owner, target_owner, None)
        .unwrap();
    assert_eq!(allocate.source_backend_class(), Some(&source_class));
    assert_eq!(allocate.target_backend_class(), &target_class);
    assert!(migration.accepts(
        PlacementEpoch::new(7).unwrap(),
        BackendGeneration::new(3).unwrap()
    ));

    migration.begin_backfill(41).unwrap();
    migration.begin_mirroring().unwrap();
    let digest = Digest32::new([migration_id as u8; 32]);
    migration
        .record_receipt(
            MigrationReceipt::mirrored(
                migration.id(),
                replica_id,
                migration.catalog_version(),
                migration.source_generation(),
                migration.target_generation(),
                target_class.digest(),
                44,
                digest,
                44,
                digest,
            )
            .unwrap(),
        )
        .unwrap();
    migration.prepare().unwrap();
    migration.activate(PlacementEpoch::new(8).unwrap()).unwrap();
    migration.publish_activation(1_000).unwrap();
    assert!(migration.accepts(
        PlacementEpoch::new(8).unwrap(),
        BackendGeneration::new(4).unwrap()
    ));

    migration
        .record_reverse_receipt(
            MigrationReceipt::reverse_mirrored(
                migration.id(),
                replica_id,
                migration.catalog_version(),
                migration.source_generation(),
                migration.target_generation(),
                target_class.digest(),
                48,
                digest,
                48,
                digest,
            )
            .unwrap(),
        )
        .unwrap();
    migration.cleanup(replica_id, source_owner, true).unwrap();
    assert_eq!(migration.state(), MigrationState::Completed);
}

#[test]
fn every_directed_builtin_provider_pair_uses_the_same_neutral_contract() {
    let directions = [
        (ProviderKind::Fjall, ProviderKind::PostgreSql),
        (ProviderKind::Fjall, ProviderKind::Neo4j),
        (ProviderKind::PostgreSql, ProviderKind::Fjall),
        (ProviderKind::PostgreSql, ProviderKind::Neo4j),
        (ProviderKind::Neo4j, ProviderKind::Fjall),
        (ProviderKind::Neo4j, ProviderKind::PostgreSql),
    ];

    for (offset, (source, target)) in directions.into_iter().enumerate() {
        exercise_direction(source, target, 100 + offset as u128);
    }
}
