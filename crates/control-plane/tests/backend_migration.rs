use std::collections::BTreeMap;

use control_plane::{
    BackendMigrationRecord, BackendMigrationState, BackendProfile, BackendReplicaReceipt, Catalog,
    CatalogCommand, CatalogError, CatalogState, DeploymentMode, GraphDefinition, Placement,
    TopologyDefinition,
};
use storage_api::AdapterRequirement;

#[test]
fn backend_migration_requires_complete_replica_receipts_before_publish() {
    let mut state = catalog_with_graph();
    let target = profile("postgresql", 2);
    let migration = BackendMigrationRecord::new(
        91,
        7,
        state.graph(7).unwrap().backend().clone(),
        target.clone(),
        4,
        1_000,
    )
    .unwrap();

    state
        .apply(CatalogCommand::create_backend_migration(2, 1, migration))
        .unwrap();
    assert_eq!(
        state.active_backend_migration(7).unwrap().state(),
        BackendMigrationState::Preparing
    );
    assert!(matches!(
        state
            .apply(CatalogCommand::publish_backend(
                100,
                2,
                7,
                1,
                target.clone(),
            ))
            .unwrap_err(),
        CatalogError::ActiveBackendMigrationConflict { graph_id: 7 }
    ));
    let changed_topology = TopologyDefinition::new(
        DeploymentMode::SharedNothing,
        99,
        128,
        2,
        vec![
            Placement::new(10, 2, vec![10]).unwrap(),
            Placement::new(20, 2, vec![20]).unwrap(),
        ],
    )
    .unwrap();
    assert!(matches!(
        state
            .apply(CatalogCommand::publish_topology(
                101,
                2,
                7,
                1,
                changed_topology,
            ))
            .unwrap_err(),
        CatalogError::BackendMigrationTopologyConflict { graph_id: 7 }
    ));

    let digest = target.digest().unwrap();
    let restored = receipts(BackendMigrationState::Restored, digest, 40);
    state
        .apply(CatalogCommand::advance_backend_migration(
            3,
            2,
            91,
            0,
            BackendMigrationState::Restored,
            4,
            1_100,
            restored,
        ))
        .unwrap();

    let incomplete = vec![receipt(
        BackendMigrationState::DualApplying,
        10,
        10,
        digest,
        50,
    )];
    assert!(matches!(
        state
            .apply(CatalogCommand::advance_backend_migration(
                4,
                3,
                91,
                1,
                BackendMigrationState::DualApplying,
                4,
                1_200,
                incomplete,
            ))
            .unwrap_err(),
        CatalogError::IncompleteBackendMigrationReceipts { .. }
    ));

    for (command_id, phase, index) in [
        (5, BackendMigrationState::DualApplying, 50),
        (6, BackendMigrationState::Verified, 60),
        (7, BackendMigrationState::CutOver, 70),
    ] {
        let revision = state.revision();
        let state_revision = state.backend_migration(91).unwrap().state_revision();
        state
            .apply(CatalogCommand::advance_backend_migration(
                command_id,
                revision,
                91,
                state_revision,
                phase,
                4,
                1_000 + index * 10,
                receipts(phase, digest, index),
            ))
            .unwrap();
    }

    let revision = state.revision();
    let state_revision = state.backend_migration(91).unwrap().state_revision();
    state
        .apply(CatalogCommand::publish_backend_migration(
            8,
            revision,
            91,
            state_revision,
            4,
            2_000,
        ))
        .unwrap();

    assert_eq!(state.graph(7).unwrap().backend(), &target);
    assert_eq!(
        state.backend_migration(91).unwrap().state(),
        BackendMigrationState::Published
    );
    assert!(state.active_backend_migration(7).is_some());

    let recovered = CatalogState::decode_snapshot(&state.encode_snapshot().unwrap()).unwrap();
    assert_eq!(recovered, state);
    assert_eq!(recovered.backend_migration(91).unwrap().receipts().len(), 8);
}

#[test]
fn workflow_is_exclusive_generation_safe_and_crash_recoverable() {
    let directory = tempfile::tempdir().unwrap();
    let mut catalog = Catalog::open(directory.path()).unwrap();
    catalog
        .execute(CatalogCommand::create_graph(1, 0, graph()))
        .unwrap();

    let target = profile("neo4j", 2);
    let migration = BackendMigrationRecord::new(
        101,
        7,
        catalog.state().graph(7).unwrap().backend().clone(),
        target.clone(),
        9,
        3_000,
    )
    .unwrap();
    let create = CatalogCommand::create_backend_migration(2, 1, migration);
    assert_eq!(
        CatalogCommand::decode(&create.encode().unwrap()).unwrap(),
        create
    );
    catalog.execute(create).unwrap();

    let competing = BackendMigrationRecord::new(
        102,
        7,
        catalog.state().graph(7).unwrap().backend().clone(),
        target.clone(),
        9,
        3_001,
    )
    .unwrap();
    assert!(matches!(
        catalog
            .execute(CatalogCommand::create_backend_migration(3, 2, competing))
            .unwrap_err(),
        CatalogError::ActiveBackendMigrationConflict { graph_id: 7 }
    ));

    catalog.checkpoint().unwrap();
    drop(catalog);
    let reopened = Catalog::open(directory.path()).unwrap();
    let recovered = reopened.state().backend_migration(101).unwrap();
    assert_eq!(recovered.target(), &target);
    assert_eq!(recovered.owner_term(), 9);
    assert_eq!(recovered.state(), BackendMigrationState::Preparing);

    assert!(matches!(
        BackendMigrationRecord::new(
            103,
            7,
            profile("rocksdb", 1),
            profile("postgresql", 3),
            1,
            4_000,
        )
        .unwrap_err(),
        CatalogError::NonSequentialBackendGeneration { .. }
    ));
}

fn catalog_with_graph() -> CatalogState {
    let mut state = CatalogState::new();
    state
        .apply(CatalogCommand::create_graph(1, 0, graph()))
        .unwrap();
    state
}

fn graph() -> GraphDefinition {
    GraphDefinition::new(
        7,
        "social",
        1,
        TopologyDefinition::new(
            DeploymentMode::SharedNothing,
            99,
            128,
            1,
            vec![
                Placement::new(10, 1, vec![10]).unwrap(),
                Placement::new(20, 1, vec![20]).unwrap(),
            ],
        )
        .unwrap(),
        profile("rocksdb", 1),
    )
    .unwrap()
}

fn profile(provider: &str, generation: u64) -> BackendProfile {
    BackendProfile::new(
        provider,
        BTreeMap::from([("endpoint".into(), format!("{provider}://local"))]),
        BTreeMap::new(),
        AdapterRequirement::HotPluggableReplica,
        generation,
    )
    .unwrap()
}

fn receipts(
    state: BackendMigrationState,
    profile_digest: [u8; 32],
    applied_index: u64,
) -> Vec<BackendReplicaReceipt> {
    vec![
        receipt(state, 10, 10, profile_digest, applied_index),
        receipt(state, 20, 20, profile_digest, applied_index),
    ]
}

fn receipt(
    state: BackendMigrationState,
    shard_id: u32,
    node_id: u64,
    profile_digest: [u8; 32],
    applied_index: u64,
) -> BackendReplicaReceipt {
    BackendReplicaReceipt::new(state, shard_id, node_id, applied_index, profile_digest).unwrap()
}
