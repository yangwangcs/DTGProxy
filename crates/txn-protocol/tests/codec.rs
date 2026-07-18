use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use temporal_types::TransactionTime;
use txn_protocol::{
    HomeTransactionRecord, IsolationLevel, ParticipantProof, PrewriteRequest, ShardEpoch,
    TransactionId, TransactionState, TxnProtocolError,
};

fn participant(shard_id: u32) -> ShardEpoch {
    ShardEpoch::new(shard_id, 7).unwrap()
}

fn prewrite() -> PrewriteRequest {
    PrewriteRequest::new(
        TransactionId::new(99),
        TransactionTime::new(100, 0),
        3,
        participant(20),
        participant(10),
        vec![participant(20), participant(10)],
        IsolationLevel::TemporalSnapshot,
        TransactionTime::new(200, 0),
        PreparedMutationBatch {
            shard_id: 20,
            txn_id: 99,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/7".to_vec()),
                b"canonical-value".to_vec(),
            )],
        },
    )
    .unwrap()
}

#[test]
fn prewrite_round_trips_with_a_canonical_sorted_participant_set() {
    let request = prewrite();
    assert_eq!(request.participants(), &[participant(10), participant(20)]);
    assert_eq!(
        PrewriteRequest::decode(&request.encode().unwrap()).unwrap(),
        request
    );
    assert_eq!(request.intent_digest(), request.intent_digest());
}

#[test]
fn committed_home_record_round_trips_with_all_participant_proofs() {
    let request = prewrite();
    let proofs = request
        .participants()
        .iter()
        .copied()
        .map(|participant| {
            ParticipantProof::new(
                participant,
                TransactionTime::new(100, 1),
                [participant.shard_id() as u8; 32],
            )
        })
        .collect();
    let record = HomeTransactionRecord::new(
        request.transaction_id(),
        request.start_ts(),
        TransactionState::Committed,
        Some(TransactionTime::new(101, 0)),
        request.participants().to_vec(),
        proofs,
    )
    .unwrap();

    assert_eq!(
        HomeTransactionRecord::decode(&record.encode().unwrap()).unwrap(),
        record
    );
}

#[test]
fn home_commit_must_be_after_every_participant_minimum() {
    let request = prewrite();
    let proofs = request
        .participants()
        .iter()
        .copied()
        .map(|participant| {
            ParticipantProof::new(
                participant,
                TransactionTime::new(101, 0),
                [participant.shard_id() as u8; 32],
            )
        })
        .collect();
    assert!(matches!(
        HomeTransactionRecord::new(
            request.transaction_id(),
            request.start_ts(),
            TransactionState::Committed,
            Some(TransactionTime::new(101, 0)),
            request.participants().to_vec(),
            proofs,
        ),
        Err(TxnProtocolError::CommitBeforeParticipantMinimum)
    ));
}

#[test]
fn malformed_participant_sets_and_batches_are_rejected_at_construction() {
    let duplicate = PrewriteRequest::new(
        TransactionId::new(1),
        TransactionTime::new(1, 0),
        1,
        participant(10),
        participant(10),
        vec![participant(10), participant(10)],
        IsolationLevel::TemporalSnapshot,
        TransactionTime::new(2, 0),
        PreparedMutationBatch {
            shard_id: 10,
            txn_id: 1,
            mutations: vec![Mutation::delete(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, b"k".to_vec()),
            )],
        },
    );
    assert!(duplicate.is_err());

    let mut wrong_batch = prewrite().batch().clone();
    wrong_batch.shard_id = 999;
    assert!(
        PrewriteRequest::new(
            TransactionId::new(99),
            TransactionTime::new(100, 0),
            3,
            participant(20),
            participant(10),
            vec![participant(10), participant(20)],
            IsolationLevel::TemporalSnapshot,
            TransactionTime::new(200, 0),
            wrong_batch,
        )
        .is_err()
    );
}
