use std::collections::BTreeMap;

use control_plane::{
    BackendProfile, CatalogCommand, CatalogState, DeploymentMode, GraphDefinition, Placement,
    RetentionPin, RetentionPinKind, TopologyDefinition,
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
            vec![Placement::new(10, 1, vec![1]).unwrap()],
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
fn transaction_backup_and_cdc_pins_are_durable_expiring_cleanup_fences() {
    let mut state = CatalogState::new();
    state
        .apply(CatalogCommand::create_graph(1, 0, graph()))
        .unwrap();
    for (offset, kind) in [
        RetentionPinKind::Transaction,
        RetentionPinKind::Backup,
        RetentionPinKind::ChangeDataCapture,
    ]
    .into_iter()
    .enumerate()
    {
        let pin_id = 100 + offset as u128;
        let pin = RetentionPin::new(pin_id, 7, 10, 1, kind, 5_000).unwrap();
        state
            .apply(CatalogCommand::acquire_retention_pin(
                10 + offset as u128,
                state.revision(),
                pin,
            ))
            .unwrap();
    }
    assert!(state.cleanup_is_pinned(7, 10, 1, 4_999));
    assert!(!state.cleanup_is_pinned(7, 10, 1, 5_000));

    let encoded = state.encode_snapshot().unwrap();
    let mut restored = CatalogState::decode_snapshot(&encoded).unwrap();
    assert_eq!(restored, state);
    restored
        .apply(CatalogCommand::release_retention_pin(
            20,
            restored.revision(),
            100,
        ))
        .unwrap();
    assert_eq!(restored.retention_pins().len(), 2);
}
