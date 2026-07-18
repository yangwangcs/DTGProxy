use std::collections::BTreeMap;

use control_plane::{
    BackendProfile, CatalogCommand, DeploymentMode, GraphDefinition, Placement, TopologyDefinition,
};
use meta_node::{MetaStateError, MetaStateMachine};
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
                Placement::new(10, 1, vec![1, 2, 3]).unwrap(),
                Placement::new(20, 1, vec![1, 2, 3]).unwrap(),
            ],
        )
        .unwrap(),
        BackendProfile::new(
            "rocksdb",
            BTreeMap::from([("path".into(), "data/graph-7".into())]),
            BTreeMap::new(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .unwrap(),
    )
    .unwrap()
}

#[test]
fn only_contiguous_committed_entries_advance_catalog_and_emit_events() {
    let mut state = MetaStateMachine::new(8).unwrap();
    let command = CatalogCommand::create_graph(101, 0, graph())
        .encode()
        .unwrap();

    assert!(matches!(
        state.apply_committed(1, 2, &command),
        Err(MetaStateError::NonContiguousIndex {
            expected: 1,
            actual: 2
        })
    ));
    assert_eq!(state.catalog().revision(), 0);
    let receipt = state.apply_committed(1, 1, &command).unwrap();
    assert_eq!(receipt.applied_index(), 1);
    assert_eq!(receipt.catalog_revision(), 1);
    assert!(!receipt.duplicate());
    let watch = state.watch_after(0).unwrap();
    assert_eq!(watch.events().len(), 1);
    assert_eq!(watch.events()[0].revision(), 1);
    assert_eq!(watch.events()[0].command(), command);
}

#[test]
fn command_replay_across_new_raft_indices_is_idempotent_and_payload_bound() {
    let mut state = MetaStateMachine::new(8).unwrap();
    let command = CatalogCommand::create_graph(101, 0, graph())
        .encode()
        .unwrap();
    state.apply_committed(1, 1, &command).unwrap();
    let replay = state.apply_committed(2, 2, &command).unwrap();
    assert!(replay.duplicate());
    assert_eq!(replay.catalog_revision(), 1);
    assert_eq!(state.applied_index(), 2);

    let different = CatalogCommand::publish_schema(101, 1, 7, 1, 2)
        .encode()
        .unwrap();
    assert!(matches!(
        state.apply_committed(2, 3, &different),
        Err(MetaStateError::Catalog(_))
    ));
    assert_eq!(state.applied_index(), 2);
}

#[test]
fn snapshot_restores_exact_state_and_bounded_watch_compaction() {
    let mut state = MetaStateMachine::new(2).unwrap();
    state
        .apply_committed(
            1,
            1,
            &CatalogCommand::create_graph(101, 0, graph())
                .encode()
                .unwrap(),
        )
        .unwrap();
    state
        .apply_committed(
            1,
            2,
            &CatalogCommand::publish_schema(102, 1, 7, 1, 2)
                .encode()
                .unwrap(),
        )
        .unwrap();
    state
        .apply_committed(
            1,
            3,
            &CatalogCommand::publish_schema(103, 2, 7, 2, 3)
                .encode()
                .unwrap(),
        )
        .unwrap();
    assert!(matches!(
        state.watch_after(0),
        Err(MetaStateError::RevisionCompacted {
            compacted_through: 1
        })
    ));

    let snapshot = state.encode_snapshot().unwrap();
    let restored = MetaStateMachine::decode_snapshot(&snapshot, 2).unwrap();
    assert_eq!(restored.applied_index(), 3);
    assert_eq!(restored.catalog(), state.catalog());
    assert!(restored.watch_after(3).unwrap().events().is_empty());

    let mut corrupted = snapshot;
    let middle = corrupted.len() / 2;
    corrupted[middle] ^= 1;
    assert_eq!(
        MetaStateMachine::decode_snapshot(&corrupted, 2).unwrap_err(),
        MetaStateError::SnapshotChecksumMismatch
    );
}
