use std::collections::BTreeMap;

use storage_api::{Keyspace, LogicalKey, Mutation, MutationOperation, PreparedMutationBatch};
use temporal_types::TransactionTime;
use txn_protocol::{
    ConstraintClaim, HomeTransactionRecord, IsolationLevel, ParticipantEngine, ParticipantProof,
    PointReadVersion, PrewriteMetadata, PrewriteRequest, RangeReadFingerprint, RecoveryAction,
    ShardEpoch, TransactionId, TransactionState, TxnProtocolError, recovery_action,
};

fn request() -> PrewriteRequest {
    request_with_id(77)
}

fn metadata(schema_version: u64, placement_epoch: u64) -> PrewriteMetadata {
    PrewriteMetadata::new(schema_version, placement_epoch, Vec::new(), Vec::new())
        .expect("current prewrite metadata")
}

fn request_with_id(transaction_id: u128) -> PrewriteRequest {
    PrewriteRequest::new(
        TransactionId::new(transaction_id),
        TransactionTime::new(100, 0),
        5,
        ShardEpoch::new(20, 9).unwrap(),
        ShardEpoch::new(10, 9).unwrap(),
        vec![
            ShardEpoch::new(10, 9).unwrap(),
            ShardEpoch::new(20, 9).unwrap(),
        ],
        IsolationLevel::TemporalSnapshot,
        TransactionTime::new(200, 0),
        PreparedMutationBatch {
            shard_id: 20,
            txn_id: transaction_id,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/77".to_vec()),
                b"value".to_vec(),
            )],
        },
        Vec::new(),
        metadata(5, 9),
    )
    .unwrap()
}

fn apply_to_map(map: &mut BTreeMap<LogicalKey, Vec<u8>>, mutations: &[Mutation]) {
    for mutation in mutations {
        match &mutation.operation {
            MutationOperation::Put { key, value } => {
                map.insert(key.clone(), value.clone());
            }
            MutationOperation::Delete { key } => {
                map.remove(key);
            }
        }
    }
}

fn values_for(map: &BTreeMap<LogicalKey, Vec<u8>>, keys: &[LogicalKey]) -> Vec<Option<Vec<u8>>> {
    keys.iter().map(|key| map.get(key).cloned()).collect()
}

fn request_with_constraint(transaction_id: u128, owner: &[u8]) -> PrewriteRequest {
    let start = if transaction_id == 91 { 100 } else { 200 };
    PrewriteRequest::new(
        TransactionId::new(transaction_id),
        TransactionTime::new(start, 0),
        5,
        ShardEpoch::new(20, 9).unwrap(),
        ShardEpoch::new(10, 9).unwrap(),
        vec![
            ShardEpoch::new(10, 9).unwrap(),
            ShardEpoch::new(20, 9).unwrap(),
        ],
        IsolationLevel::TemporalSnapshot,
        TransactionTime::new(start + 100, 0),
        PreparedMutationBatch {
            shard_id: 20,
            txn_id: transaction_id,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(
                    Keyspace::Current,
                    format!("vertex/constraint-owner/{transaction_id}").into_bytes(),
                ),
                owner.to_vec(),
            )],
        },
        vec![
            ConstraintClaim::new(
                LogicalKey::in_keyspace(
                    Keyspace::Txn,
                    b"dtg/constraint/v1/graph/7/name/ada".to_vec(),
                ),
                owner.to_vec(),
            )
            .unwrap(),
        ],
        metadata(5, 9),
    )
    .unwrap()
}

#[test]
fn prewrite_persists_an_intent_and_locks_then_replays_idempotently() {
    let request = request();
    let keys = ParticipantEngine::prewrite_inspection_keys(&request).unwrap();
    let first = ParticipantEngine::prewrite(&request, &vec![None; keys.len()]).unwrap();
    assert!(!first.duplicate());
    assert_eq!(first.proof().participant(), request.participant());
    assert_eq!(first.proof().intent_digest(), request.intent_digest());

    let mut state = BTreeMap::new();
    apply_to_map(&mut state, first.mutations());
    let replay = ParticipantEngine::prewrite(&request, &values_for(&state, &keys)).unwrap();
    assert!(replay.duplicate());
    assert!(replay.mutations().is_empty());
}

#[test]
fn write_after_start_and_another_transaction_intent_are_conflicts() {
    let first = request();
    let keys = ParticipantEngine::prewrite_inspection_keys(&first).unwrap();
    let outcome = ParticipantEngine::prewrite(&first, &vec![None; keys.len()]).unwrap();
    let mut state = BTreeMap::new();
    apply_to_map(&mut state, outcome.mutations());

    let second = request_with_id(88);
    let second_keys = ParticipantEngine::prewrite_inspection_keys(&second).unwrap();
    assert!(matches!(
        ParticipantEngine::prewrite(&second, &values_for(&state, &second_keys)),
        Err(TxnProtocolError::IntentConflict { .. })
    ));

    let finalize_keys = ParticipantEngine::finalize_inspection_keys(&first).unwrap();
    let finalized = ParticipantEngine::finalize(
        &first,
        TransactionTime::new(150, 0),
        &values_for(&state, &finalize_keys),
    )
    .unwrap();
    apply_to_map(&mut state, finalized.mutations());
    let values = values_for(&state, &second_keys);
    assert!(matches!(
        ParticipantEngine::prewrite(&second, &values),
        Err(TxnProtocolError::WriteConflict { .. })
    ));
}

#[test]
fn finalize_materializes_business_mutations_and_releases_locks() {
    let request = request();
    let prewrite_keys = ParticipantEngine::prewrite_inspection_keys(&request).unwrap();
    let prewrite = ParticipantEngine::prewrite(&request, &vec![None; prewrite_keys.len()]).unwrap();
    let mut state = BTreeMap::new();
    apply_to_map(&mut state, prewrite.mutations());

    let finalize_keys = ParticipantEngine::finalize_inspection_keys(&request).unwrap();
    let finalized = ParticipantEngine::finalize(
        &request,
        TransactionTime::new(101, 0),
        &values_for(&state, &finalize_keys),
    )
    .unwrap();
    assert!(finalized.mutations().iter().any(|mutation| matches!(
        &mutation.operation,
        MutationOperation::Put { key, value }
            if key.keyspace() == Keyspace::Current && value == b"value"
    )));
    apply_to_map(&mut state, finalized.mutations());
    assert!(
        finalize_keys[1..]
            .iter()
            .all(|key| !state.contains_key(key))
    );
}

#[test]
fn committed_constraint_claim_rejects_a_different_element_owner() {
    let first = request_with_constraint(91, b"element-a");
    let prewrite_keys = ParticipantEngine::prewrite_inspection_keys(&first).unwrap();
    let prewrite = ParticipantEngine::prewrite(&first, &vec![None; prewrite_keys.len()]).unwrap();
    let mut state = BTreeMap::new();
    apply_to_map(&mut state, prewrite.mutations());

    let finalize_keys = ParticipantEngine::finalize_inspection_keys(&first).unwrap();
    let finalized = ParticipantEngine::finalize(
        &first,
        TransactionTime::new(150, 0),
        &values_for(&state, &finalize_keys),
    )
    .unwrap();
    apply_to_map(&mut state, finalized.mutations());

    let second = request_with_constraint(92, b"element-b");
    let second_keys = ParticipantEngine::prewrite_inspection_keys(&second).unwrap();
    assert!(matches!(
        ParticipantEngine::prewrite(&second, &values_for(&state, &second_keys)),
        Err(TxnProtocolError::ConstraintConflict { .. })
    ));
}

#[test]
fn serializable_point_read_rejects_a_changed_committed_version() {
    let first = request();
    let first_keys = ParticipantEngine::prewrite_inspection_keys(&first).unwrap();
    let prewrite = ParticipantEngine::prewrite(&first, &vec![None; first_keys.len()]).unwrap();
    let mut state = BTreeMap::new();
    apply_to_map(&mut state, prewrite.mutations());
    let finalize_keys = ParticipantEngine::finalize_inspection_keys(&first).unwrap();
    let finalized = ParticipantEngine::finalize(
        &first,
        TransactionTime::new(150, 0),
        &values_for(&state, &finalize_keys),
    )
    .unwrap();
    apply_to_map(&mut state, finalized.mutations());

    let metadata = PrewriteMetadata::new(
        5,
        9,
        vec![
            PointReadVersion::new(
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/77".to_vec()),
                Some(TransactionTime::new(100, 0)),
            )
            .unwrap(),
        ],
        vec![RangeReadFingerprint::new(Keyspace::Current, b"vertex/".to_vec(), [7; 32]).unwrap()],
    )
    .unwrap();
    let second = PrewriteRequest::new(
        TransactionId::new(98),
        TransactionTime::new(200, 0),
        5,
        ShardEpoch::new(20, 9).unwrap(),
        ShardEpoch::new(10, 9).unwrap(),
        vec![
            ShardEpoch::new(10, 9).unwrap(),
            ShardEpoch::new(20, 9).unwrap(),
        ],
        IsolationLevel::TemporalSerializable,
        TransactionTime::new(300, 0),
        PreparedMutationBatch {
            shard_id: 20,
            txn_id: 98,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/98".to_vec()),
                b"value".to_vec(),
            )],
        },
        Vec::new(),
        metadata,
    )
    .unwrap();
    let keys = ParticipantEngine::prewrite_inspection_keys(&second).unwrap();
    assert!(matches!(
        ParticipantEngine::prewrite(&second, &values_for(&state, &keys)),
        Err(TxnProtocolError::ReadDependencyConflict { .. })
    ));
}

#[test]
fn constraint_only_participant_commits_its_durable_owner() {
    let request = PrewriteRequest::new(
        TransactionId::new(93),
        TransactionTime::new(300, 0),
        5,
        ShardEpoch::new(20, 9).unwrap(),
        ShardEpoch::new(20, 9).unwrap(),
        vec![ShardEpoch::new(20, 9).unwrap()],
        IsolationLevel::TemporalSnapshot,
        TransactionTime::new(400, 0),
        PreparedMutationBatch {
            shard_id: 20,
            txn_id: 93,
            mutations: Vec::new(),
        },
        vec![
            ConstraintClaim::new(
                LogicalKey::in_keyspace(
                    Keyspace::Txn,
                    b"dtg/constraint/v1/graph/7/name/grace".to_vec(),
                ),
                b"element-grace".to_vec(),
            )
            .unwrap(),
        ],
        metadata(5, 9),
    )
    .unwrap();
    let prewrite_keys = ParticipantEngine::prewrite_inspection_keys(&request).unwrap();
    let prewrite = ParticipantEngine::prewrite(&request, &vec![None; prewrite_keys.len()]).unwrap();
    let mut state = BTreeMap::new();
    apply_to_map(&mut state, prewrite.mutations());

    let finalize_keys = ParticipantEngine::finalize_inspection_keys(&request).unwrap();
    let finalized = ParticipantEngine::finalize(
        &request,
        TransactionTime::new(350, 0),
        &values_for(&state, &finalize_keys),
    )
    .unwrap();
    apply_to_map(&mut state, finalized.mutations());
    assert_eq!(
        state.get(&LogicalKey::in_keyspace(
            Keyspace::Txn,
            b"dtg/constraint/v1/graph/7/name/grace".to_vec(),
        )),
        Some(&b"element-grace".to_vec())
    );
}

#[test]
fn committed_constraint_owner_is_visible_only_at_or_after_its_commit_timestamp() {
    let request = request_with_constraint(91, b"element-a");
    let prewrite_keys = ParticipantEngine::prewrite_inspection_keys(&request).unwrap();
    let prewrite = ParticipantEngine::prewrite(&request, &vec![None; prewrite_keys.len()]).unwrap();
    let mut state = BTreeMap::new();
    apply_to_map(&mut state, prewrite.mutations());
    let finalize_keys = ParticipantEngine::finalize_inspection_keys(&request).unwrap();
    let finalized = ParticipantEngine::finalize(
        &request,
        TransactionTime::new(150, 0),
        &values_for(&state, &finalize_keys),
    )
    .unwrap();
    apply_to_map(&mut state, finalized.mutations());

    let constraint = request.constraint_claims().first().unwrap().key();
    let owner_keys =
        ParticipantEngine::constraint_owner_read_keys(request.participant(), constraint).unwrap();
    let owner_values = values_for(&state, &owner_keys);

    assert_eq!(
        ParticipantEngine::constraint_owner_at_snapshot(
            owner_values[0].as_deref(),
            owner_values[1].as_deref(),
            TransactionTime::new(149, 0),
        )
        .unwrap(),
        None
    );
    assert_eq!(
        ParticipantEngine::constraint_owner_at_snapshot(
            owner_values[0].as_deref(),
            owner_values[1].as_deref(),
            TransactionTime::new(150, 0),
        )
        .unwrap(),
        Some(b"element-a".to_vec())
    );
}

#[test]
fn abort_releases_locks_and_is_idempotent_but_cannot_reverse_commit() {
    let request = request();
    let prewrite_keys = ParticipantEngine::prewrite_inspection_keys(&request).unwrap();
    let prewrite = ParticipantEngine::prewrite(&request, &vec![None; prewrite_keys.len()]).unwrap();
    let mut state = BTreeMap::new();
    apply_to_map(&mut state, prewrite.mutations());

    let abort_keys = ParticipantEngine::abort_inspection_keys(&request).unwrap();
    let aborted = ParticipantEngine::abort(&request, &values_for(&state, &abort_keys)).unwrap();
    assert!(!aborted.duplicate());
    apply_to_map(&mut state, aborted.mutations());
    assert!(abort_keys[1..].iter().all(|key| !state.contains_key(key)));

    let replay = ParticipantEngine::abort(&request, &values_for(&state, &abort_keys)).unwrap();
    assert!(replay.duplicate());
    assert!(replay.mutations().is_empty());
    assert!(matches!(
        ParticipantEngine::finalize(
            &request,
            TransactionTime::new(150, 0),
            &values_for(&state, &abort_keys),
        ),
        Err(TxnProtocolError::TransactionAlreadyAborted)
    ));
}

#[test]
fn one_phase_commit_materializes_without_exposing_an_intent_and_replays_durably() {
    let request = request();
    let keys = ParticipantEngine::prewrite_inspection_keys(&request).unwrap();
    let outcome = ParticipantEngine::one_phase_commit(
        &request,
        TransactionTime::new(101, 0),
        &vec![None; keys.len()],
    )
    .unwrap();
    assert!(!outcome.duplicate());
    let mut state = BTreeMap::new();
    apply_to_map(&mut state, outcome.mutations());
    assert_eq!(
        state.get(&LogicalKey::in_keyspace(
            Keyspace::Current,
            b"vertex/77".to_vec()
        )),
        Some(&b"value".to_vec())
    );
    assert!(
        keys.iter()
            .skip(1)
            .step_by(2)
            .all(|key| !state.contains_key(key))
    );

    let replay = ParticipantEngine::one_phase_commit(
        &request,
        TransactionTime::new(101, 0),
        &values_for(&state, &keys),
    )
    .unwrap();
    assert!(replay.duplicate());
    assert!(replay.mutations().is_empty());
}

#[test]
fn recovery_uses_the_home_decision_and_only_times_out_undecided_intents() {
    let request = request();
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
    let committed = HomeTransactionRecord::new(
        request.transaction_id(),
        request.start_ts(),
        TransactionState::Committed,
        Some(TransactionTime::new(101, 0)),
        request.participants().to_vec(),
        proofs,
    )
    .unwrap();
    assert_eq!(
        recovery_action(
            Some(&committed),
            request.expires_at(),
            TransactionTime::new(150, 0)
        ),
        RecoveryAction::RollForward {
            commit_ts: TransactionTime::new(101, 0)
        }
    );
    assert_eq!(
        recovery_action(None, request.expires_at(), TransactionTime::new(150, 0)),
        RecoveryAction::Wait
    );
    assert_eq!(
        recovery_action(None, request.expires_at(), TransactionTime::new(201, 0)),
        RecoveryAction::Rollback
    );
}
