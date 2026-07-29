use dtg_control::{
    ActionCommand, ActionFailure, ActionState, ControlActionLedger, GraphId, PlacementEpoch,
    ReconcileAction, ReplicaId, ShardId, Version,
};

#[test]
fn duplicate_reconciliation_delivery_has_one_authoritative_action() {
    let action = ReconcileAction::Promote {
        graph_id: GraphId::new(1).unwrap(),
        shard_id: ShardId::new(2).unwrap(),
        placement_epoch: PlacementEpoch::new(3).unwrap(),
        backend_generation: dtg_control::BackendGeneration::new(4).unwrap(),
        replica_id: ReplicaId::new(5).unwrap(),
    };
    let mut ledger = ControlActionLedger::new();

    let first = ledger
        .apply(ActionCommand::enqueue(Version::new(9), action.clone()))
        .unwrap();
    let duplicate = ledger
        .apply(ActionCommand::enqueue(Version::new(9), action))
        .unwrap();

    assert_eq!(first.action_id(), duplicate.action_id());
    assert_eq!(ledger.records().count(), 1);
    assert_eq!(first.state(), &ActionState::Pending { attempt: 0 });
}

#[test]
fn failed_action_retries_with_a_new_lease_and_stale_completion_is_fenced() {
    let action = ReconcileAction::TransferLeader {
        graph_id: GraphId::new(1).unwrap(),
        shard_id: ShardId::new(2).unwrap(),
        placement_epoch: PlacementEpoch::new(3).unwrap(),
        from: ReplicaId::new(4).unwrap(),
        to: ReplicaId::new(5).unwrap(),
    };
    let mut ledger = ControlActionLedger::new();
    let action_id = ledger
        .apply(ActionCommand::enqueue(Version::new(9), action))
        .unwrap()
        .action_id();
    let first = ledger
        .apply(ActionCommand::claim(action_id, "controller-1", 100, 10))
        .unwrap()
        .lease()
        .unwrap();
    ledger
        .apply(ActionCommand::fail(
            action_id,
            first,
            ActionFailure::retryable("transport unavailable"),
        ))
        .unwrap();
    let second = ledger
        .apply(ActionCommand::claim(action_id, "controller-2", 200, 10))
        .unwrap()
        .lease()
        .unwrap();

    assert_ne!(first, second);
    assert!(
        ledger
            .apply(ActionCommand::complete(action_id, first))
            .is_err()
    );
    let completed = ledger
        .apply(ActionCommand::complete(action_id, second))
        .unwrap();
    assert!(matches!(completed.state(), ActionState::Completed { .. }));
    assert!(
        ledger
            .apply(ActionCommand::complete(action_id, first))
            .is_err()
    );
}

#[test]
fn action_commands_round_trip_through_the_current_authority_format() {
    let action = ReconcileAction::TransferLeader {
        graph_id: GraphId::new(1).unwrap(),
        shard_id: ShardId::new(2).unwrap(),
        placement_epoch: PlacementEpoch::new(3).unwrap(),
        from: ReplicaId::new(4).unwrap(),
        to: ReplicaId::new(5).unwrap(),
    };
    let enqueue = ActionCommand::enqueue(Version::new(9), action);
    let decoded = ActionCommand::decode(&enqueue.encode_current().unwrap()).unwrap();
    assert_eq!(decoded, enqueue);
}
