use std::collections::BTreeMap;

use control_plane::{
    BackendProfile, CatalogCommand, CatalogState, DeploymentMode, GraphDefinition,
    MigrationProgress, MigrationRecord, MigrationState, Placement, TopologyDefinition,
};
use storage_api::AdapterRequirement;

fn graph() -> GraphDefinition {
    GraphDefinition::new(
        7,
        "social",
        1,
        TopologyDefinition::new(
            DeploymentMode::PrimaryReplica,
            99,
            1,
            1,
            vec![Placement::new(10, 1, vec![10, 11, 12]).unwrap()],
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::new(),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn topology_cutover_records_lineage_and_committed_fence_in_one_command() {
    let mut state = CatalogState::new();
    state
        .apply(CatalogCommand::create_graph(1, 0, graph()))
        .unwrap();
    let migration = MigrationRecord::new_shard(
        100,
        7,
        10,
        1,
        2,
        vec![10, 11, 12],
        vec![11, 12, 13],
        9,
        1_000,
    )
    .unwrap();
    state
        .apply(CatalogCommand::create_migration(2, 1, migration))
        .unwrap();
    let transitions = [
        (
            MigrationState::Copying,
            MigrationProgress::new(9, 1_100).unwrap(),
        ),
        (
            MigrationState::CatchingUp,
            MigrationProgress::new(9, 1_200)
                .unwrap()
                .with_snapshot(50, [0x55; 32])
                .unwrap(),
        ),
        (
            MigrationState::Ready,
            MigrationProgress::new(9, 1_300)
                .unwrap()
                .with_snapshot(50, [0x55; 32])
                .unwrap()
                .with_catchup_index(75)
                .unwrap(),
        ),
        (
            MigrationState::Committing,
            MigrationProgress::new(9, 1_400)
                .unwrap()
                .with_snapshot(50, [0x55; 32])
                .unwrap()
                .with_catchup_index(75)
                .unwrap(),
        ),
    ];
    for (offset, (next, progress)) in transitions.into_iter().enumerate() {
        state
            .apply(CatalogCommand::advance_migration(
                3 + offset as u128,
                state.revision(),
                100,
                state.migration(100).unwrap().state_revision(),
                next,
                progress,
            ))
            .unwrap();
    }
    let topology = TopologyDefinition::new(
        DeploymentMode::PrimaryReplica,
        99,
        1,
        2,
        vec![Placement::new(10, 2, vec![11, 12, 13]).unwrap()],
    )
    .unwrap();
    let progress = MigrationProgress::new(9, 1_500)
        .unwrap()
        .with_snapshot(50, [0x55; 32])
        .unwrap()
        .with_catchup_index(75)
        .unwrap()
        .with_cutover_index(80)
        .unwrap();
    state
        .apply(CatalogCommand::commit_migration(
            9,
            state.revision(),
            100,
            state.migration(100).unwrap().state_revision(),
            topology,
            progress,
        ))
        .unwrap();

    assert_eq!(state.graph(7).unwrap().topology().epoch(), 2);
    assert_eq!(
        state.migration(100).unwrap().state(),
        MigrationState::Committed
    );
    let lineage = state.lineage(7, 10, 1).unwrap();
    assert_eq!(lineage.target_epoch(), 2);
    assert_eq!(lineage.cutover_index(), 80);
    assert_eq!(lineage.migration_id(), 100);

    let recovered = CatalogState::decode_snapshot(&state.encode_snapshot().unwrap()).unwrap();
    assert_eq!(recovered.lineage(7, 10, 1), Some(lineage));
}
