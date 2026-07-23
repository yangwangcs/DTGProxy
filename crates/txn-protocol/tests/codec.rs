use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use temporal_types::TransactionTime;
use txn_protocol::{
    HomeTransactionRecord, IsolationLevel, ParticipantProof, PointReadVersion, PrewriteMetadata,
    PrewriteRequest, RangeReadFingerprint, ShardEpoch, TransactionId, TransactionState,
    TxnProtocolError,
};

fn participant(shard_id: u32) -> ShardEpoch {
    ShardEpoch::new(shard_id, 7).unwrap()
}

fn metadata(schema_version: u64, placement_epoch: u64) -> PrewriteMetadata {
    PrewriteMetadata::new(schema_version, placement_epoch, Vec::new(), Vec::new())
        .expect("current prewrite metadata")
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
        Vec::new(),
        metadata(3, 7),
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
fn prewrite_without_required_metadata_is_rejected() {
    let mut malformed = prewrite().encode().expect("encode current prewrite");
    let payload_end = malformed.len() - 4;
    let metadata_offset = malformed[..payload_end]
        .iter()
        .rposition(|byte| *byte == 0x5a)
        .expect("metadata field tag");
    malformed.drain(metadata_offset..payload_end);
    let payload_length = u32::try_from(malformed.len() - 14).expect("payload length");
    malformed[6..10].copy_from_slice(&payload_length.to_be_bytes());
    let checksum_offset = malformed.len() - 4;
    let checksum = crc32fast::hash(&malformed[..checksum_offset]).to_be_bytes();
    malformed[checksum_offset..].copy_from_slice(&checksum);

    assert!(matches!(
        PrewriteRequest::decode(&malformed),
        Err(TxnProtocolError::MissingField("prewrite metadata"))
    ));
}

#[test]
fn prewrite_metadata_round_trips_with_read_dependencies_and_fences() {
    let metadata = PrewriteMetadata::new(
        3,
        7,
        vec![
            PointReadVersion::new(
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/7".to_vec()),
                Some(TransactionTime::new(99, 0)),
            )
            .expect("point read"),
        ],
        vec![
            RangeReadFingerprint::new(Keyspace::Current, b"vertex/".to_vec(), [7; 32])
                .expect("range read"),
        ],
    )
    .expect("metadata");
    let request = PrewriteRequest::new(
        TransactionId::new(100),
        TransactionTime::new(100, 0),
        3,
        participant(20),
        participant(10),
        vec![participant(20), participant(10)],
        IsolationLevel::TemporalSerializable,
        TransactionTime::new(200, 0),
        PreparedMutationBatch {
            shard_id: 20,
            txn_id: 100,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/8".to_vec()),
                b"canonical-value".to_vec(),
            )],
        },
        Vec::new(),
        metadata,
    )
    .expect("prewrite request");

    let decoded = PrewriteRequest::decode(&request.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, request);
    let metadata = decoded.metadata();
    assert_eq!(metadata.schema_version(), 3);
    assert_eq!(metadata.topology_epoch(), 7);
    assert_eq!(metadata.point_reads().len(), 1);
    assert_eq!(metadata.range_reads().len(), 1);
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
        Vec::new(),
        metadata(1, 7),
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
            Vec::new(),
            metadata(3, 7),
        )
        .is_err()
    );
}
