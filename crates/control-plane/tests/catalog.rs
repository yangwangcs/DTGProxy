use std::collections::BTreeMap;
use std::fs;

use control_plane::{
    BackendProfile, Catalog, CatalogCommand, CatalogError, CatalogState, DeploymentMode,
    GraphDefinition, Placement, TopologyDefinition,
};
use storage_api::AdapterRequirement;

#[test]
fn commands_increment_only_the_owned_epoch_and_reject_stale_publishers() {
    let mut state = CatalogState::new();
    let created = state
        .apply(CatalogCommand::create_graph(1, 0, graph()))
        .unwrap();
    assert_eq!(created.revision(), 1);
    assert!(!created.duplicate());

    let next_profile = BackendProfile::new(
        "postgres",
        BTreeMap::from([("endpoint".into(), "127.0.0.1:7001".into())]),
        BTreeMap::from([("password".into(), "secret://postgres/main".into())]),
        AdapterRequirement::HotPluggableReplica,
        2,
    )
    .unwrap();
    state
        .apply(CatalogCommand::publish_backend(2, 1, 7, 1, next_profile))
        .unwrap();
    let graph = state.graph(7).unwrap();
    assert_eq!(graph.backend().generation(), 2);
    assert_eq!(graph.topology().epoch(), 1);
    assert_eq!(graph.schema_version(), 1);

    let topology = TopologyDefinition::new(
        DeploymentMode::SharedNothing,
        100,
        256,
        2,
        vec![placement(10, 2), placement(20, 2), placement(30, 1)],
    )
    .unwrap();
    state
        .apply(CatalogCommand::publish_topology(3, 2, 7, 1, topology))
        .unwrap();
    state
        .apply(CatalogCommand::publish_schema(4, 3, 7, 1, 2))
        .unwrap();
    let graph = state.graph(7).unwrap();
    assert_eq!(graph.topology().epoch(), 2);
    assert_eq!(graph.schema_version(), 2);
    assert_eq!(graph.backend().generation(), 2);

    let error = state
        .apply(CatalogCommand::publish_schema(5, 4, 7, 1, 2))
        .unwrap_err();
    assert!(matches!(
        error,
        CatalogError::StaleSchemaVersion {
            expected: 2,
            actual: 1,
            ..
        }
    ));
}

#[test]
fn command_ids_are_idempotent_but_cannot_be_reused_for_different_content() {
    let mut state = CatalogState::new();
    let command = CatalogCommand::create_graph(9, 0, graph());
    let first = state.apply(command.clone()).unwrap();
    let replay = state.apply(command).unwrap();
    assert_eq!(replay.revision(), first.revision());
    assert!(replay.duplicate());

    let mismatch = CatalogCommand::create_graph(9, 0, graph_with_name("different"));
    assert_eq!(
        state.apply(mismatch).unwrap_err(),
        CatalogError::CommandReplayMismatch { command_id: 9 }
    );
}

#[test]
fn decision_log_and_checkpoint_recover_the_same_catalog() {
    let directory = tempfile::tempdir().unwrap();
    let mut catalog = Catalog::open(directory.path()).unwrap();
    catalog
        .execute(CatalogCommand::create_graph(1, 0, graph()))
        .unwrap();
    catalog
        .execute(CatalogCommand::publish_schema(2, 1, 7, 1, 2))
        .unwrap();
    drop(catalog);

    let mut reopened = Catalog::open(directory.path()).unwrap();
    assert_eq!(reopened.state().revision(), 2);
    assert_eq!(reopened.state().graph(7).unwrap().schema_version(), 2);
    reopened.checkpoint().unwrap();
    assert_eq!(fs::metadata(reopened.log_path()).unwrap().len(), 0);
    drop(reopened);

    let recovered = Catalog::open(directory.path()).unwrap();
    assert_eq!(recovered.state().revision(), 2);
    let graph = recovered.state().graph(7).unwrap();
    assert_eq!(graph.name(), "social");
    assert_eq!(graph.schema_version(), 2);
    assert_eq!(graph.topology().epoch(), 1);
    assert_eq!(graph.backend().provider(), "rocksdb");
}

#[test]
fn corrupted_snapshot_or_log_is_rejected_instead_of_reset() {
    let snapshot_directory = tempfile::tempdir().unwrap();
    let mut catalog = Catalog::open(snapshot_directory.path()).unwrap();
    catalog
        .execute(CatalogCommand::create_graph(1, 0, graph()))
        .unwrap();
    catalog.checkpoint().unwrap();
    let snapshot = catalog.snapshot_path().to_path_buf();
    drop(catalog);
    let mut bytes = fs::read(&snapshot).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x80;
    fs::write(&snapshot, bytes).unwrap();
    assert!(matches!(
        Catalog::open(snapshot_directory.path()).unwrap_err(),
        CatalogError::ChecksumMismatch
    ));

    let log_directory = tempfile::tempdir().unwrap();
    let mut catalog = Catalog::open(log_directory.path()).unwrap();
    catalog
        .execute(CatalogCommand::create_graph(1, 0, graph()))
        .unwrap();
    let log = catalog.log_path().to_path_buf();
    drop(catalog);
    let mut bytes = fs::read(&log).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0x01;
    fs::write(&log, bytes).unwrap();
    assert!(matches!(
        Catalog::open(log_directory.path()).unwrap_err(),
        CatalogError::ChecksumMismatch
    ));
}

#[test]
fn every_consensus_command_round_trips_canonically() {
    let commands = [
        CatalogCommand::create_graph(1, 0, graph()),
        CatalogCommand::publish_topology(
            2,
            1,
            7,
            1,
            TopologyDefinition::new(
                DeploymentMode::SharedNothing,
                100,
                256,
                2,
                vec![placement(10, 2), placement(20, 2)],
            )
            .unwrap(),
        ),
        CatalogCommand::publish_schema(3, 2, 7, 1, 2),
        CatalogCommand::publish_backend(
            4,
            3,
            7,
            1,
            BackendProfile::new(
                "neo4j",
                BTreeMap::from([("endpoint".into(), "127.0.0.1:7687".into())]),
                BTreeMap::from([("password".into(), "secret://neo4j/main".into())]),
                AdapterRequirement::HotPluggableReplica,
                2,
            )
            .unwrap(),
        ),
    ];

    for command in commands {
        let bytes = command.encode().unwrap();
        assert_eq!(CatalogCommand::decode(&bytes).unwrap(), command);
    }
}

fn graph() -> GraphDefinition {
    graph_with_name("social")
}

fn graph_with_name(name: &str) -> GraphDefinition {
    GraphDefinition::new(
        7,
        name,
        1,
        TopologyDefinition::new(
            DeploymentMode::SharedNothing,
            99,
            128,
            1,
            vec![placement(10, 1), placement(20, 1)],
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

fn placement(shard_id: u32, epoch: u64) -> Placement {
    Placement::new(shard_id, epoch, vec![u64::from(shard_id)]).unwrap()
}
