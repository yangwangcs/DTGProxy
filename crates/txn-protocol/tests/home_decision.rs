use storage_api::{Keyspace, MutationOperation};
use temporal_types::TransactionTime;
use txn_protocol::{
    HomeDecisionEngine, HomeTransactionRecord, ParticipantProof, ShardEpoch, TransactionId,
    TransactionState, TxnProtocolError,
};

fn shard(shard_id: u32) -> ShardEpoch {
    ShardEpoch::new(shard_id, 9).unwrap()
}

fn committed(commit_ts: TransactionTime) -> HomeTransactionRecord {
    HomeTransactionRecord::new(
        TransactionId::new(77),
        TransactionTime::new(100, 0),
        TransactionState::Committed,
        Some(commit_ts),
        vec![shard(10), shard(20)],
        vec![
            ParticipantProof::new(shard(10), TransactionTime::new(100, 1), [10; 32]),
            ParticipantProof::new(shard(20), TransactionTime::new(100, 1), [20; 32]),
        ],
    )
    .unwrap()
}

#[test]
fn home_decision_is_one_durable_idempotent_mutation() {
    let decision = committed(TransactionTime::new(101, 0));
    let key = HomeDecisionEngine::inspection_key(shard(10), decision.transaction_id()).unwrap();
    assert_eq!(key.keyspace(), Keyspace::Txn);

    let first = HomeDecisionEngine::record(shard(10), &decision, None).unwrap();
    assert!(!first.duplicate());
    assert_eq!(first.mutations().len(), 1);
    let MutationOperation::Put {
        key: stored_key,
        value,
    } = &first.mutations()[0].operation
    else {
        panic!("Home decision must be stored as one Put");
    };
    assert_eq!(stored_key, &key);
    assert_eq!(
        HomeTransactionRecord::decode(value.as_slice()).unwrap(),
        decision
    );

    let replay = HomeDecisionEngine::record(shard(10), &decision, Some(value)).unwrap();
    assert!(replay.duplicate());
    assert!(replay.mutations().is_empty());
}

#[test]
fn a_home_decision_is_irreversible_and_must_live_on_the_home_participant() {
    let decision = committed(TransactionTime::new(101, 0));
    let first = HomeDecisionEngine::record(shard(10), &decision, None).unwrap();
    let MutationOperation::Put { value, .. } = &first.mutations()[0].operation else {
        unreachable!();
    };
    let conflicting = committed(TransactionTime::new(102, 0));
    assert!(matches!(
        HomeDecisionEngine::record(shard(10), &conflicting, Some(value)),
        Err(TxnProtocolError::HomeDecisionConflict)
    ));
    assert!(matches!(
        HomeDecisionEngine::record(shard(30), &decision, None),
        Err(TxnProtocolError::HomeParticipantMissing { .. })
    ));
}
