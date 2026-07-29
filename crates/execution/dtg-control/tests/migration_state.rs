use dtg_control::{
    BackendClass, BackendGeneration, Digest32, GraphId, MigrationId, MigrationReceipt,
    MigrationRecord, PlacementEpoch, ProviderKind, ReplicaId, ShardId, Version,
};

fn epoch(value: u64) -> PlacementEpoch {
    PlacementEpoch::new(value).unwrap()
}

fn generation(value: u64) -> BackendGeneration {
    BackendGeneration::new(value).unwrap()
}

fn target_class() -> BackendClass {
    BackendClass::new(ProviderKind::PostgreSql, 1, 1, ["point-read"]).unwrap()
}

fn prepared_migration(
    source_epoch: PlacementEpoch,
    source_generation: BackendGeneration,
    target_generation: BackendGeneration,
) -> MigrationRecord {
    let target_class = target_class();
    let voters = vec![ReplicaId::new(1).unwrap(), ReplicaId::new(2).unwrap()];
    let mut migration = MigrationRecord::new(
        MigrationId::new(17).unwrap(),
        GraphId::new(1).unwrap(),
        ShardId::new(1).unwrap(),
        Version::new(12),
        source_epoch,
        source_generation,
        target_generation,
        target_class.clone(),
        voters.clone(),
    )
    .unwrap();

    migration.begin_backfill(41).unwrap();
    migration.begin_mirroring().unwrap();
    let digest = Digest32::new([7; 32]);
    for replica_id in voters {
        migration
            .record_receipt(
                MigrationReceipt::mirrored(
                    migration.id(),
                    replica_id,
                    migration.catalog_version(),
                    source_generation,
                    target_generation,
                    target_class.digest(),
                    44,
                    digest,
                    44,
                    digest,
                )
                .unwrap(),
            )
            .unwrap();
    }
    migration.prepare().unwrap();
    migration
}

fn active_in_grace(active_generation: BackendGeneration) -> MigrationRecord {
    let mut migration = prepared_migration(epoch(7), generation(3), active_generation);
    migration.activate(epoch(8)).unwrap();
    migration.publish_activation(1_000).unwrap();
    migration
}

#[test]
fn activation_is_monotonic_and_fences_old_epoch() {
    let mut migration = prepared_migration(epoch(7), generation(3), generation(4));
    migration.activate(epoch(8)).unwrap();
    assert_eq!(migration.active_generation(), generation(4));
    assert!(!migration.accepts(epoch(7), generation(3)));
    assert!(migration.accepts(epoch(8), generation(4)));
}

#[test]
fn rollback_uses_a_new_generation() {
    let mut migration = active_in_grace(generation(4));
    let rollback = migration.rollback(epoch(9), generation(5)).unwrap();
    assert_eq!(rollback.generation(), generation(5));
    assert_eq!(migration.active_generation(), generation(5));
    assert!(migration.accepts(epoch(9), generation(5)));
    assert!(!migration.accepts(epoch(8), generation(4)));
}
