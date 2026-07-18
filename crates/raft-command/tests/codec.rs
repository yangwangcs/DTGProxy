use raft_command::{
    AbortIntentV1, ApplyPreparedV1, CommandBodyV1, CommandCodecError, CommandEnvelopeV1,
    FinalizeV1, MAX_COMMAND_BYTES, OnePhaseCommitV1, PrewriteV1, RecordDecisionV1,
};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use temporal_types::TransactionTime;
use txn_protocol::{
    HomeTransactionRecord, IsolationLevel, ParticipantProof, PrewriteRequest, ShardEpoch,
    TransactionId, TransactionState,
};

fn apply_command() -> CommandEnvelopeV1 {
    CommandEnvelopeV1::new(
        7,
        11,
        99,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(1_234_567, 3),
            batch: PreparedMutationBatch {
                shard_id: 7,
                txn_id: 42,
                mutations: vec![
                    Mutation::put(
                        0,
                        LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
                        b"current-value".to_vec(),
                    ),
                    Mutation::delete(
                        1,
                        LogicalKey::in_keyspace(Keyspace::AdjOut, b"edge/9".to_vec()),
                    ),
                ],
            },
        }),
    )
}

fn participant(shard_id: u32) -> ShardEpoch {
    ShardEpoch::new(shard_id, 11).unwrap()
}

fn prewrite_request() -> PrewriteRequest {
    PrewriteRequest::new(
        TransactionId::new(42),
        TransactionTime::new(100, 0),
        5,
        participant(7),
        participant(7),
        vec![participant(7)],
        IsolationLevel::TemporalSnapshot,
        TransactionTime::new(200, 0),
        PreparedMutationBatch {
            shard_id: 7,
            txn_id: 42,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/42".to_vec()),
                b"value".to_vec(),
            )],
        },
    )
    .unwrap()
}

#[test]
fn every_distributed_transaction_phase_round_trips_canonically() {
    let request = prewrite_request();
    let proof = ParticipantProof::new(
        request.participant(),
        TransactionTime::new(100, 1),
        request.intent_digest(),
    );
    let decision = HomeTransactionRecord::new(
        request.transaction_id(),
        request.start_ts(),
        TransactionState::Committed,
        Some(TransactionTime::new(101, 0)),
        request.participants().to_vec(),
        vec![proof.clone()],
    )
    .unwrap();
    let commands = [
        CommandEnvelopeV1::new(
            7,
            11,
            1001,
            CommandBodyV1::Prewrite(PrewriteV1 {
                request: request.clone(),
                expected_proof: proof,
            }),
        ),
        CommandEnvelopeV1::new(
            7,
            11,
            1002,
            CommandBodyV1::RecordDecision(RecordDecisionV1 {
                home: participant(7),
                decision,
            }),
        ),
        CommandEnvelopeV1::new(
            7,
            11,
            1003,
            CommandBodyV1::Finalize(FinalizeV1 {
                participant: participant(7),
                transaction_id: request.transaction_id(),
                intent_digest: request.intent_digest(),
                commit_ts: TransactionTime::new(101, 0),
            }),
        ),
        CommandEnvelopeV1::new(
            7,
            11,
            1004,
            CommandBodyV1::OnePhaseCommit(OnePhaseCommitV1 {
                request: request.clone(),
                expected_proof: ParticipantProof::new(
                    request.participant(),
                    TransactionTime::new(100, 1),
                    request.intent_digest(),
                ),
                commit_ts: TransactionTime::new(101, 0),
            }),
        ),
        CommandEnvelopeV1::new(
            7,
            11,
            1005,
            CommandBodyV1::AbortIntent(AbortIntentV1 {
                participant: participant(7),
                transaction_id: request.transaction_id(),
                intent_digest: request.intent_digest(),
            }),
        ),
    ];
    for command in commands {
        let bytes = command.encode().unwrap();
        assert_eq!(CommandEnvelopeV1::decode(&bytes).unwrap(), command);
        assert_eq!(
            CommandEnvelopeV1::decode(&bytes).unwrap().encode().unwrap(),
            bytes
        );
    }
}

#[test]
fn apply_and_closed_timestamp_commands_round_trip_exactly() {
    let apply = apply_command();
    let apply_bytes = apply.encode().unwrap();
    assert_eq!(CommandEnvelopeV1::decode(&apply_bytes).unwrap(), apply);

    let tick = CommandEnvelopeV1::new(
        7,
        11,
        100,
        CommandBodyV1::ClosedTimestampTick(TransactionTime::new(-5, 8)),
    );
    let tick_bytes = tick.encode().unwrap();
    assert_eq!(CommandEnvelopeV1::decode(&tick_bytes).unwrap(), tick);
}

#[test]
fn closed_timestamp_wire_format_has_stable_golden_bytes() {
    let tick = CommandEnvelopeV1::new(
        1,
        2,
        3,
        CommandBodyV1::ClosedTimestampTick(TransactionTime::new(4, 5)),
    );

    assert_eq!(
        hex(&tick.encode().unwrap()),
        "4454524300010200000000010000000000000002000000000000000000000000000000030000000c0000000000000004000000059a153cb5"
    );
}

#[test]
fn decoder_rejects_duplicate_sequences_unknown_tags_and_trailing_body_bytes() {
    let mut duplicate = apply_command().encode().unwrap();
    let second_mutation_sequence_offset = 111;
    duplicate[second_mutation_sequence_offset..second_mutation_sequence_offset + 4]
        .copy_from_slice(&0_u32.to_be_bytes());
    refresh_checksum(&mut duplicate);
    assert_eq!(
        CommandEnvelopeV1::decode(&duplicate),
        Err(CommandCodecError::NonCanonicalMutationSequence {
            expected: 1,
            actual: 0,
        })
    );

    let mut unknown_tag = apply_command().encode().unwrap();
    unknown_tag[6] = 99;
    refresh_checksum(&mut unknown_tag);
    assert_eq!(
        CommandEnvelopeV1::decode(&unknown_tag),
        Err(CommandCodecError::UnknownBodyTag { tag: 99 })
    );

    let mut trailing_body = apply_command().encode().unwrap();
    let checksum_offset = trailing_body.len() - 4;
    trailing_body.insert(checksum_offset, 0);
    let body_length = u32::from_be_bytes(trailing_body[36..40].try_into().unwrap()) + 1;
    trailing_body[36..40].copy_from_slice(&body_length.to_be_bytes());
    refresh_checksum(&mut trailing_body);
    assert_eq!(
        CommandEnvelopeV1::decode(&trailing_body),
        Err(CommandCodecError::TrailingBodyBytes { remaining: 1 })
    );
}

#[test]
fn corruption_trailing_bytes_and_noncanonical_mutations_fail_closed() {
    let valid = apply_command().encode().unwrap();

    let mut corrupted = valid.clone();
    corrupted[45] ^= 0x80;
    assert_eq!(
        CommandEnvelopeV1::decode(&corrupted),
        Err(CommandCodecError::ChecksumMismatch)
    );

    let mut trailing = valid;
    trailing.push(0);
    assert!(matches!(
        CommandEnvelopeV1::decode(&trailing),
        Err(CommandCodecError::LengthMismatch { .. })
    ));

    let mut command = apply_command();
    let CommandBodyV1::ApplyPrepared(apply) = &mut command.body else {
        unreachable!();
    };
    apply.batch.mutations[1].sequence = 0;
    assert_eq!(
        command.encode(),
        Err(CommandCodecError::NonCanonicalMutationSequence {
            expected: 1,
            actual: 0,
        })
    );
}

#[test]
fn every_phase_one_keyspace_and_operation_survives_the_command_codec() {
    let mutations = Keyspace::ALL
        .into_iter()
        .enumerate()
        .map(|(sequence, keyspace)| {
            let key = LogicalKey::in_keyspace(keyspace, vec![keyspace.tag(), 0, 255]);
            if sequence % 2 == 0 {
                Mutation::put(u32::try_from(sequence).unwrap(), key, vec![0, 1, 255])
            } else {
                Mutation::delete(u32::try_from(sequence).unwrap(), key)
            }
        })
        .collect();
    let command = CommandEnvelopeV1::new(
        9,
        10,
        u128::MAX,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(i64::MIN, u32::MAX),
            batch: PreparedMutationBatch {
                shard_id: 9,
                txn_id: u128::MAX - 1,
                mutations,
            },
        }),
    );

    let decoded = CommandEnvelopeV1::decode(&command.encode().unwrap()).unwrap();
    assert_eq!(decoded, command);
}

#[test]
fn bounded_arbitrary_bytes_never_panic_or_allocate_past_the_command_limit() {
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    for length in 0..10_000_usize {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bounded_length = length % 513;
        let mut bytes = vec![0_u8; bounded_length];
        for byte in &mut bytes {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = state.to_le_bytes()[0];
        }
        let _ = CommandEnvelopeV1::decode(&bytes);
    }

    let oversized = vec![0_u8; MAX_COMMAND_BYTES + 1];
    assert!(matches!(
        CommandEnvelopeV1::decode(&oversized),
        Err(CommandCodecError::CommandTooLarge { .. })
    ));
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(DIGITS[usize::from(byte >> 4)]));
        encoded.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn refresh_checksum(bytes: &mut [u8]) {
    let checksum_offset = bytes.len() - 4;
    let checksum = crc32fast::hash(&bytes[..checksum_offset]);
    bytes[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
}
