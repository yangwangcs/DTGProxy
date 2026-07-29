use std::collections::BTreeMap;

use dtg_execution::{
    PhysicalWritePlanner, RoutedWriteMutation, WritePlanningContext, WriteShardTarget,
};
use dtg_language_ir::{LogicalMutation as IrMutation, LogicalWrite, ValidTimeExpr};
use dtg_storage::{
    BackendGeneration, CommandId, GraphId, LogicalMutation, PlacementEpoch, ShardId, TransactionId,
    TransactionTime, ValidInterval, Version, VertexId, VertexVersion,
};
use dtg_transaction::{ShardSnapshotFence, SnapshotToken};

fn vertex(id: u128, transaction_time: TransactionTime) -> LogicalMutation {
    LogicalMutation::PutVertex(
        VertexVersion::new(
            VertexId::new(id).unwrap(),
            Version::new(1),
            ValidInterval::new(0, 100).unwrap(),
            transaction_time,
            BTreeMap::new(),
        )
        .unwrap(),
    )
}

#[test]
fn normalized_writes_lower_to_sorted_fenced_typed_participants() {
    let transaction_id = TransactionId::new(41).unwrap();
    let start_time = TransactionTime::new(73).unwrap();
    let shard_3 = ShardId::new(3).unwrap();
    let shard_9 = ShardId::new(9).unwrap();
    let epoch_3 = PlacementEpoch::new(5).unwrap();
    let epoch_9 = PlacementEpoch::new(7).unwrap();
    let generation_3 = BackendGeneration::new(11).unwrap();
    let generation_9 = BackendGeneration::new(13).unwrap();
    let snapshot = SnapshotToken::new(
        transaction_id,
        start_time,
        Version::new(17),
        vec![
            (
                shard_9,
                ShardSnapshotFence {
                    placement_epoch: epoch_9,
                    backend_generation: generation_9,
                    applied_index: 101,
                    closed_time: start_time,
                },
            ),
            (
                shard_3,
                ShardSnapshotFence {
                    placement_epoch: epoch_3,
                    backend_generation: generation_3,
                    applied_index: 99,
                    closed_time: start_time,
                },
            ),
        ],
    )
    .unwrap();
    let context = WritePlanningContext::new(
        GraphId::new(2).unwrap(),
        Version::new(19),
        Version::new(23),
        snapshot,
    )
    .unwrap();
    let logical = LogicalWrite {
        input: None,
        mutations: vec![
            IrMutation::Delete {
                variable: "right".into(),
                valid_from: ValidTimeExpr::Literal(0),
            },
            IrMutation::Delete {
                variable: "left".into(),
                valid_from: ValidTimeExpr::Literal(0),
            },
        ],
    };

    let plan = PhysicalWritePlanner::lower(&logical, context, |index, _| {
        let routed = match index {
            0 => RoutedWriteMutation::new(
                WriteShardTarget::new(
                    shard_9,
                    epoch_9,
                    generation_9,
                    101,
                    CommandId::new(9001).unwrap(),
                )
                .unwrap(),
                vertex(9, start_time),
            )
            .unwrap(),
            1 => RoutedWriteMutation::new(
                WriteShardTarget::new(
                    shard_3,
                    epoch_3,
                    generation_3,
                    99,
                    CommandId::new(3001).unwrap(),
                )
                .unwrap(),
                vertex(3, start_time),
            )
            .unwrap(),
            _ => unreachable!(),
        };
        Ok(vec![routed])
    })
    .unwrap();

    assert_eq!(plan.version(), Version::new(1));
    assert_eq!(plan.graph_id(), GraphId::new(2).unwrap());
    assert_eq!(plan.schema_version(), Version::new(19));
    assert_eq!(plan.topology_version(), Version::new(23));
    assert_eq!(plan.transaction_id(), transaction_id);
    assert_eq!(plan.transaction_snapshot(), start_time);
    assert_eq!(
        plan.fragments()
            .iter()
            .map(|fragment| fragment.target().shard_id())
            .collect::<Vec<_>>(),
        vec![shard_3, shard_9]
    );
    assert_eq!(plan.fragments()[0].target().placement_epoch(), epoch_3);
    assert_eq!(
        plan.fragments()[0].target().backend_generation(),
        generation_3
    );
    assert_eq!(plan.fragments()[0].target().applied_index(), 99);
    assert_eq!(
        plan.fragments()[0].target().command_id(),
        CommandId::new(3001).unwrap()
    );

    let participants = plan.participant_writes().unwrap();
    assert_eq!(
        participants
            .iter()
            .map(|participant| participant.shard_id())
            .collect::<Vec<_>>(),
        vec![shard_3, shard_9]
    );
    assert_eq!(participants[0].mutations(), &[vertex(3, start_time)]);
    assert_eq!(participants[1].mutations(), &[vertex(9, start_time)]);

    let (transaction, submitted) = plan.transaction_submission().unwrap();
    assert_eq!(transaction.snapshot(), plan.snapshot());
    assert_eq!(transaction.overlay().mutations().len(), 2);
    assert_eq!(submitted, participants);
}
