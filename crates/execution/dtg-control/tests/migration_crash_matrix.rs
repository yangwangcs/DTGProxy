use dtg_control::{
    BackendClass, BackendGeneration, ControlError, Digest32, GraphId, MigrationActionKind,
    MigrationId, MigrationReceipt, MigrationRecord, MigrationState, PlacementEpoch, ProviderKind,
    ReplicaId, ShardId, Version,
};

const CATALOG_VERSION: Version = Version::new(12);

fn epoch(value: u64) -> PlacementEpoch {
    PlacementEpoch::new(value).unwrap()
}

fn generation(value: u64) -> BackendGeneration {
    BackendGeneration::new(value).unwrap()
}

fn replica(value: u64) -> ReplicaId {
    ReplicaId::new(value).unwrap()
}

fn owner(value: u8) -> Digest32 {
    Digest32::new([value; 32])
}

fn logical_digest() -> Digest32 {
    Digest32::new([9; 32])
}

fn target_class() -> BackendClass {
    BackendClass::new(ProviderKind::PostgreSql, 1, 1, ["point-read"]).unwrap()
}

fn allocating_migration() -> MigrationRecord {
    MigrationRecord::new(
        MigrationId::new(17).unwrap(),
        GraphId::new(1).unwrap(),
        ShardId::new(1).unwrap(),
        CATALOG_VERSION,
        epoch(7),
        generation(3),
        generation(4),
        target_class(),
        vec![replica(1), replica(2)],
    )
    .unwrap()
}

fn mirroring_migration() -> MigrationRecord {
    let mut migration = allocating_migration();
    for replica_id in [replica(1), replica(2)] {
        migration
            .claim_target_namespace(
                replica_id,
                owner(replica_id.get() as u8),
                owner(10 + replica_id.get() as u8),
                None,
            )
            .unwrap();
    }
    migration.begin_backfill(41).unwrap();
    migration.begin_mirroring().unwrap();
    migration
}

fn prepared_migration() -> MigrationRecord {
    let mut migration = mirroring_migration();
    for replica_id in [replica(1), replica(2)] {
        migration
            .record_receipt(
                MigrationReceipt::mirrored(
                    migration.id(),
                    replica_id,
                    CATALOG_VERSION,
                    generation(3),
                    generation(4),
                    migration.target_backend_class().digest(),
                    44,
                    logical_digest(),
                    44,
                    logical_digest(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    migration.prepare().unwrap();
    migration
}

fn grace_migration() -> MigrationRecord {
    let mut migration = prepared_migration();
    migration.activate(epoch(8)).unwrap();
    migration.publish_activation(1_000).unwrap();
    migration
}

fn grace_with_reverse_mirror() -> MigrationRecord {
    let mut migration = grace_migration();
    for replica_id in [replica(1), replica(2)] {
        migration
            .record_reverse_receipt(
                MigrationReceipt::reverse_mirrored(
                    migration.id(),
                    replica_id,
                    CATALOG_VERSION,
                    generation(3),
                    generation(4),
                    migration.target_backend_class().digest(),
                    48,
                    logical_digest(),
                    48,
                    logical_digest(),
                )
                .unwrap(),
            )
            .unwrap();
    }
    migration
}

#[test]
fn crash_before_after_and_exact_retry_are_idempotent() {
    let before = allocating_migration();
    let mut recovered_before = before.clone();
    let action = recovered_before.begin_backfill(41).unwrap();

    let mut recovered_after = recovered_before.clone();
    assert_eq!(recovered_after.begin_backfill(41).unwrap(), action);
    assert_eq!(recovered_after, recovered_before);
    assert_eq!(
        recovered_after.begin_backfill(42).unwrap_err().code(),
        "DTG-CONTROL-MIGRATION-TRANSITION"
    );
}

#[test]
fn stale_controller_is_fenced_by_catalog_version() {
    let mut migration = mirroring_migration();
    let stale = MigrationReceipt::verified(
        migration.id(),
        replica(1),
        Version::new(11),
        generation(3),
        generation(4),
        migration.target_backend_class().digest(),
        44,
        logical_digest(),
    )
    .unwrap();

    assert!(matches!(
        migration.record_receipt(stale),
        Err(ControlError::StaleCatalog { .. })
    ));
}

#[test]
fn forward_mirror_ack_requires_both_generations_durable_at_one_digest() {
    assert_eq!(
        MigrationReceipt::mirrored(
            MigrationId::new(17).unwrap(),
            replica(1),
            CATALOG_VERSION,
            generation(3),
            generation(4),
            target_class().digest(),
            44,
            logical_digest(),
            43,
            logical_digest(),
        )
        .unwrap_err()
        .code(),
        "DTG-CONTROL-MIGRATION-DIGEST"
    );
}

#[test]
fn all_voters_must_report_one_verified_index_and_digest() {
    let mut migration = mirroring_migration();
    for (replica_id, digest) in [(replica(1), owner(7)), (replica(2), owner(8))] {
        migration
            .record_receipt(
                MigrationReceipt::mirrored(
                    migration.id(),
                    replica_id,
                    CATALOG_VERSION,
                    generation(3),
                    generation(4),
                    migration.target_backend_class().digest(),
                    44,
                    digest,
                    44,
                    digest,
                )
                .unwrap(),
            )
            .unwrap();
    }

    assert_eq!(
        migration.prepare().unwrap_err().code(),
        "DTG-CONTROL-MIGRATION-DIGEST"
    );
}

#[test]
fn verification_without_dual_durable_apply_cannot_prepare_cutover() {
    let mut migration = mirroring_migration();
    for replica_id in [replica(1), replica(2)] {
        migration
            .record_receipt(
                MigrationReceipt::verified(
                    migration.id(),
                    replica_id,
                    CATALOG_VERSION,
                    generation(3),
                    generation(4),
                    migration.target_backend_class().digest(),
                    44,
                    logical_digest(),
                )
                .unwrap(),
            )
            .unwrap();
    }

    assert_eq!(
        migration.prepare().unwrap_err().code(),
        "DTG-CONTROL-MIGRATION-INCOMPLETE"
    );
}

#[test]
fn namespace_collision_never_rebinds_an_existing_owner() {
    let mut migration = allocating_migration();
    assert_eq!(
        migration
            .claim_target_namespace(replica(1), owner(1), owner(11), Some(owner(99)))
            .unwrap_err()
            .code(),
        "DTG-CONTROL-MIGRATION-NAMESPACE-COLLISION"
    );
    let action = migration
        .claim_target_namespace(replica(1), owner(1), owner(11), Some(owner(11)))
        .unwrap();
    assert_eq!(action.kind(), MigrationActionKind::Allocate);
    assert_eq!(action.owner_identity(), Some(owner(11)));
}

#[test]
fn abort_before_cutover_retains_source_authority_and_receipts() {
    let mut migration = mirroring_migration();
    let receipt = MigrationReceipt::verified(
        migration.id(),
        replica(1),
        CATALOG_VERSION,
        generation(3),
        generation(4),
        migration.target_backend_class().digest(),
        44,
        logical_digest(),
    )
    .unwrap();
    migration.record_receipt(receipt).unwrap();
    let action = migration.abort(CATALOG_VERSION).unwrap();

    assert_eq!(action.kind(), MigrationActionKind::Abort);
    assert_eq!(migration.state(), MigrationState::Aborted);
    assert!(migration.accepts(epoch(7), generation(3)));
    assert_eq!(migration.receipts().count(), 1);
    assert_eq!(migration.abort(CATALOG_VERSION).unwrap(), action);
}

#[test]
fn cutover_is_committed_before_meta_publication_and_retries_exactly() {
    let mut migration = prepared_migration();
    assert_eq!(
        migration.publish_activation(1_000).unwrap_err().code(),
        "DTG-CONTROL-MIGRATION-TRANSITION"
    );

    let commit = migration.activate(epoch(8)).unwrap();
    assert_eq!(commit.kind(), MigrationActionKind::CommitCutover);
    assert_eq!(migration.activate(epoch(8)).unwrap(), commit);
    assert!(matches!(
        migration.state(),
        MigrationState::Activating { .. }
    ));

    let publish = migration.publish_activation(1_000).unwrap();
    assert_eq!(publish.kind(), MigrationActionKind::PublishActivation);
    assert_eq!(migration.publish_activation(1_000).unwrap(), publish);
    assert!(matches!(migration.state(), MigrationState::Grace { .. }));
}

#[test]
fn grace_reverse_mirror_failure_preserves_rollback_namespace() {
    let mut migration = grace_migration();
    let failed = MigrationReceipt::reverse_mirrored(
        migration.id(),
        replica(1),
        CATALOG_VERSION,
        generation(3),
        generation(4),
        migration.target_backend_class().digest(),
        48,
        logical_digest(),
        47,
        logical_digest(),
    );
    assert_eq!(failed.unwrap_err().code(), "DTG-CONTROL-MIGRATION-DIGEST");
    assert!(matches!(migration.state(), MigrationState::Grace { .. }));

    let rollback = migration.rollback(epoch(9), generation(5)).unwrap();
    assert_eq!(rollback.kind(), MigrationActionKind::Rollback);
    assert_eq!(rollback.retained_owner_identity(), Some(owner(1)));
}

#[test]
fn cleanup_requires_exact_owner_identity_and_released_pins() {
    let mut migration = grace_with_reverse_mirror();
    assert_eq!(
        migration
            .cleanup(replica(1), owner(1), false)
            .unwrap_err()
            .code(),
        "DTG-CONTROL-MIGRATION-CLEANUP-FENCED"
    );
    assert_eq!(
        migration
            .cleanup(replica(1), owner(99), true)
            .unwrap_err()
            .code(),
        "DTG-CONTROL-MIGRATION-CLEANUP-FENCED"
    );

    let first = migration.cleanup(replica(1), owner(1), true).unwrap();
    assert_eq!(first.kind(), MigrationActionKind::Cleanup);
    assert_eq!(
        migration.cleanup(replica(1), owner(1), true).unwrap(),
        first
    );
    assert!(matches!(migration.state(), MigrationState::Grace { .. }));

    migration.cleanup(replica(2), owner(2), true).unwrap();
    assert_eq!(migration.state(), MigrationState::Completed);
}

#[test]
fn every_action_retains_generational_and_catalog_fences() {
    let mut migration = allocating_migration();
    let action = migration
        .claim_target_namespace(replica(1), owner(1), owner(11), None)
        .unwrap();
    assert_eq!(action.source_generation(), generation(3));
    assert_eq!(action.target_generation(), generation(4));
    assert_eq!(
        action.target_backend_class(),
        migration.target_backend_class()
    );
    assert_eq!(action.catalog_version(), CATALOG_VERSION);
}
