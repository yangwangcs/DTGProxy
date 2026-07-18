use std::collections::BTreeMap;

use control_plane::{
    BackendProfile, CatalogCommand, CatalogError, CatalogState, DeploymentMode, GraphDefinition,
    MigrationError, MigrationProgress, MigrationRecord, MigrationState, Placement,
    TopologyDefinition,
};
use storage_api::AdapterRequirement;

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
                Placement::new(10, 1, vec![10, 11, 12]).unwrap(),
                Placement::new(20, 1, vec![20, 21, 22]).unwrap(),
            ],
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

fn migration(id: u128) -> MigrationRecord {
    MigrationRecord::new_shard(
        id,
        7,
        10,
        1,
        2,
        vec![10, 11, 12],
        vec![11, 12, 13],
        9,
        1_000,
    )
    .unwrap()
}

fn initialized() -> CatalogState {
    let mut state = CatalogState::new();
    state
        .apply(CatalogCommand::create_graph(1, 0, graph()))
        .unwrap();
    state
}

#[test]
fn legal_workflow_survives_a_snapshot_after_every_transition() {
    let mut state = initialized();
    state
        .apply(CatalogCommand::create_migration(2, 1, migration(100)))
        .unwrap();
    assert_eq!(state.active_migration(7, 10).unwrap().migration_id(), 100);

    let steps = [
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
        (
            MigrationState::Committed,
            MigrationProgress::new(9, 1_500)
                .unwrap()
                .with_snapshot(50, [0x55; 32])
                .unwrap()
                .with_catchup_index(75)
                .unwrap()
                .with_cutover_index(80)
                .unwrap(),
        ),
        (
            MigrationState::Cleaning,
            MigrationProgress::new(9, 1_600)
                .unwrap()
                .with_snapshot(50, [0x55; 32])
                .unwrap()
                .with_catchup_index(75)
                .unwrap()
                .with_cutover_index(80)
                .unwrap(),
        ),
        (
            MigrationState::Cleaned,
            MigrationProgress::new(9, 1_700)
                .unwrap()
                .with_snapshot(50, [0x55; 32])
                .unwrap()
                .with_catchup_index(75)
                .unwrap()
                .with_cutover_index(80)
                .unwrap(),
        ),
    ];
    for (offset, (next, progress)) in steps.into_iter().enumerate() {
        let current = state.migration(100).unwrap().state_revision();
        state
            .apply(CatalogCommand::advance_migration(
                10 + offset as u128,
                state.revision(),
                100,
                current,
                next,
                progress,
            ))
            .unwrap();
        let encoded = state.encode_snapshot().unwrap();
        state = CatalogState::decode_snapshot(&encoded).unwrap();
        assert_eq!(state.migration(100).unwrap().state(), next);
    }
    assert!(state.active_migration(7, 10).is_none());
}

#[test]
fn rejects_illegal_edges_stale_state_revision_and_two_active_owners() {
    let mut state = initialized();
    state
        .apply(CatalogCommand::create_migration(2, 1, migration(100)))
        .unwrap();
    let conflict = state
        .apply(CatalogCommand::create_migration(3, 2, migration(101)))
        .unwrap_err();
    assert!(matches!(
        conflict,
        CatalogError::Migration(MigrationError::ActiveWorkflowConflict { .. })
    ));

    let illegal = state
        .apply(CatalogCommand::advance_migration(
            4,
            2,
            100,
            1,
            MigrationState::Ready,
            MigrationProgress::new(9, 1_100).unwrap(),
        ))
        .unwrap_err();
    assert!(matches!(
        illegal,
        CatalogError::Migration(MigrationError::IllegalTransition { .. })
    ));

    state
        .apply(CatalogCommand::advance_migration(
            5,
            2,
            100,
            1,
            MigrationState::Copying,
            MigrationProgress::new(9, 1_100).unwrap(),
        ))
        .unwrap();
    let missing_fence = state
        .apply(CatalogCommand::advance_migration(
            55,
            3,
            100,
            2,
            MigrationState::CatchingUp,
            MigrationProgress::new(9, 1_150).unwrap(),
        ))
        .unwrap_err();
    assert!(matches!(
        missing_fence,
        CatalogError::Migration(MigrationError::MissingSnapshotFence)
    ));
    let stale = state
        .apply(CatalogCommand::advance_migration(
            6,
            3,
            100,
            1,
            MigrationState::CatchingUp,
            MigrationProgress::new(9, 1_200)
                .unwrap()
                .with_snapshot(50, [0x55; 32])
                .unwrap(),
        ))
        .unwrap_err();
    assert!(matches!(
        stale,
        CatalogError::Migration(MigrationError::StaleStateRevision {
            expected: 2,
            actual: 1
        })
    ));
}

#[test]
fn abort_is_only_available_before_commit_and_failure_is_durable() {
    let mut state = initialized();
    state
        .apply(CatalogCommand::create_migration(2, 1, migration(100)))
        .unwrap();
    state
        .apply(CatalogCommand::fail_migration(
            3,
            2,
            100,
            1,
            9,
            1_050,
            "target node unavailable",
        ))
        .unwrap();
    let failed = state.migration(100).unwrap();
    assert_eq!(failed.retry_count(), 1);
    assert_eq!(failed.last_error(), Some("target node unavailable"));
    assert_eq!(failed.state_revision(), 2);

    state
        .apply(CatalogCommand::advance_migration(
            4,
            3,
            100,
            2,
            MigrationState::Aborting,
            MigrationProgress::new(9, 1_100).unwrap(),
        ))
        .unwrap();
    state
        .apply(CatalogCommand::advance_migration(
            5,
            4,
            100,
            3,
            MigrationState::Aborted,
            MigrationProgress::new(9, 1_200).unwrap(),
        ))
        .unwrap();
    assert!(state.active_migration(7, 10).is_none());

    let replay = CatalogCommand::advance_migration(
        5,
        4,
        100,
        3,
        MigrationState::Aborted,
        MigrationProgress::new(9, 1_200).unwrap(),
    );
    assert!(state.apply(replay).unwrap().duplicate());
}

#[test]
fn migration_commands_round_trip_canonically() {
    let commands = [
        CatalogCommand::create_migration(2, 1, migration(100)),
        CatalogCommand::advance_migration(
            3,
            2,
            100,
            1,
            MigrationState::Copying,
            MigrationProgress::new(9, 1_100).unwrap(),
        ),
        CatalogCommand::fail_migration(4, 3, 100, 2, 9, 1_200, "retry"),
    ];
    for command in commands {
        assert_eq!(
            CatalogCommand::decode(&command.encode().unwrap()).unwrap(),
            command
        );
    }
}

#[test]
fn transition_matrix_has_no_implicit_skip_or_post_commit_abort_edge() {
    use MigrationState::{
        Aborted, Aborting, CatchingUp, Cleaned, Cleaning, Committed, Committing, Copying,
        Preparing, Ready,
    };
    let states = [
        Preparing, Copying, CatchingUp, Ready, Committing, Committed, Cleaning, Cleaned, Aborting,
        Aborted,
    ];
    let legal = [
        (Preparing, Copying),
        (Copying, CatchingUp),
        (CatchingUp, Ready),
        (Ready, Committing),
        (Committing, Committed),
        (Committed, Cleaning),
        (Cleaning, Cleaned),
        (Preparing, Aborting),
        (Copying, Aborting),
        (CatchingUp, Aborting),
        (Ready, Aborting),
        (Aborting, Aborted),
    ];
    for from in states {
        for to in states {
            assert_eq!(
                from.can_transition_to(to),
                legal.contains(&(from, to)),
                "unexpected edge {from:?} -> {to:?}"
            );
        }
    }
}
