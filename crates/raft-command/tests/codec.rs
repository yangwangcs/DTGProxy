use raft_command::{
    AbortBackendMigrationV1, AbortIntentV1, AdvanceAnalyticsArtifactFenceV1,
    AnalyticsArtifactKindV1, ApplyPreparedV1, BeginBackendDualApplyV1, CommandBodyV1,
    CommandCodecError, CommandEnvelopeV1, CutoverBackendV1, DeleteAnalyticsArtifactGenerationV1,
    FinalizeV1, MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES, MAX_COMMAND_BYTES, OnePhaseCommitV1,
    PinAnalyticsArtifactGenerationV1, PrewriteV1, PutAnalyticsArtifactChunkV1, RecordDecisionV1,
};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch};
use temporal_types::TransactionTime;
use txn_protocol::{
    HomeTransactionRecord, IsolationLevel, ParticipantProof, PrewriteMetadata, PrewriteRequest,
    ShardEpoch, TransactionId, TransactionState,
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

fn metadata(schema_version: u64, placement_epoch: u64) -> PrewriteMetadata {
    PrewriteMetadata::new(schema_version, placement_epoch, Vec::new(), Vec::new())
        .expect("current prewrite metadata")
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
        Vec::new(),
        metadata(5, 11),
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
            11,
            3,
            924,
            CommandBodyV1::AdvanceAnalyticsArtifactFence(
                AdvanceAnalyticsArtifactFenceV1::new(
                    501,
                    AnalyticsArtifactKindV1::Checkpoint,
                    8,
                    19,
                )
                .unwrap(),
            ),
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
fn placement_epoch_activation_is_canonical_and_strictly_sequential() {
    let command = CommandEnvelopeV1::new(11, 3, 900, CommandBodyV1::ActivatePlacementEpoch(4));
    let encoded = command.encode().unwrap();
    assert_eq!(CommandEnvelopeV1::decode(&encoded).unwrap(), command);
    assert!(matches!(
        CommandEnvelopeV1::new(11, 3, 901, CommandBodyV1::ActivatePlacementEpoch(5),)
            .encode()
            .unwrap_err(),
        raft_command::CommandCodecError::InvalidTargetEpoch { .. }
    ));
}

#[test]
fn backend_lifecycle_commands_round_trip_with_generation_and_digest_fences() {
    let digest = [0x5a; 32];
    let commands = [
        CommandEnvelopeV1::new(
            11,
            3,
            910,
            CommandBodyV1::BeginBackendDualApply(BeginBackendDualApplyV1 {
                source_generation: 7,
                target_generation: 8,
                target_profile_digest: digest,
                fence_index: 101,
            }),
        ),
        CommandEnvelopeV1::new(
            11,
            3,
            911,
            CommandBodyV1::CutoverBackend(CutoverBackendV1 {
                source_generation: 7,
                target_generation: 8,
                target_profile_digest: digest,
            }),
        ),
        CommandEnvelopeV1::new(
            11,
            3,
            912,
            CommandBodyV1::AbortBackendMigration(AbortBackendMigrationV1 {
                source_generation: 7,
                target_generation: 8,
                target_profile_digest: digest,
            }),
        ),
    ];

    for command in commands {
        let encoded = command.encode().unwrap();
        assert_eq!(CommandEnvelopeV1::decode(&encoded).unwrap(), command);
        assert_eq!(
            CommandEnvelopeV1::decode(&encoded)
                .unwrap()
                .encode()
                .unwrap(),
            encoded
        );
    }
}

#[test]
fn analytics_artifact_put_and_delete_commands_are_canonical_and_bounded() {
    let put = PutAnalyticsArtifactChunkV1::new(
        501,
        AnalyticsArtifactKindV1::Checkpoint,
        7,
        1_725_000_000_123,
        0,
        [0; 32],
        b"checkpoint-chunk".to_vec(),
    )
    .unwrap();
    assert_eq!(put.created_at_unix_ms, 1_725_000_000_123);
    let commands = [
        CommandEnvelopeV1::new(11, 3, 920, CommandBodyV1::PutAnalyticsArtifactChunk(put)),
        CommandEnvelopeV1::new(
            11,
            3,
            921,
            CommandBodyV1::DeleteAnalyticsArtifactGeneration(
                DeleteAnalyticsArtifactGenerationV1::new_with_gc_epoch(
                    501,
                    AnalyticsArtifactKindV1::Checkpoint,
                    7,
                    19,
                )
                .unwrap(),
            ),
        ),
    ];

    for command in commands {
        let encoded = command.encode().unwrap();
        assert_eq!(CommandEnvelopeV1::decode(&encoded).unwrap(), command);
    }
    assert_eq!(
        DeleteAnalyticsArtifactGenerationV1::new_with_gc_epoch(
            501,
            AnalyticsArtifactKindV1::Result,
            7,
            0,
        )
        .unwrap_err(),
        CommandCodecError::InvalidAnalyticsArtifact
    );
    assert_eq!(
        AdvanceAnalyticsArtifactFenceV1::new(501, AnalyticsArtifactKindV1::Result, 8, 0,)
            .unwrap_err(),
        CommandCodecError::InvalidAnalyticsArtifact
    );
    assert_eq!(
        PutAnalyticsArtifactChunkV1::new(
            501,
            AnalyticsArtifactKindV1::Result,
            7,
            1_725_000_000_123,
            1,
            [0; 32],
            b"invalid-chain".to_vec(),
        )
        .unwrap_err(),
        CommandCodecError::InvalidAnalyticsArtifact
    );
    assert_eq!(
        PutAnalyticsArtifactChunkV1::new(
            501,
            AnalyticsArtifactKindV1::Result,
            7,
            0,
            0,
            [0; 32],
            b"missing-created-at".to_vec(),
        )
        .unwrap_err(),
        CommandCodecError::InvalidAnalyticsArtifact
    );
}

#[test]
fn legacy_artifact_put_without_created_at_fails_closed() {
    let command = CommandEnvelopeV1::new(
        11,
        3,
        923,
        CommandBodyV1::PutAnalyticsArtifactChunk(
            PutAnalyticsArtifactChunkV1::new(
                501,
                AnalyticsArtifactKindV1::Result,
                7,
                1_725_000_000_123,
                0,
                [0; 32],
                b"legacy".to_vec(),
            )
            .unwrap(),
        ),
    );
    let mut legacy = command.encode().unwrap();
    legacy.drain(65..73);
    let body_length = u32::from_be_bytes(legacy[36..40].try_into().unwrap()) - 8;
    legacy[36..40].copy_from_slice(&body_length.to_be_bytes());
    refresh_checksum(&mut legacy);
    assert!(CommandEnvelopeV1::decode(&legacy).is_err());
}

#[test]
fn analytics_artifact_pin_command_is_canonical_and_manifest_bounded() {
    let digest = *blake3::hash(b"ab").as_bytes();
    let pin = PinAnalyticsArtifactGenerationV1::new(
        501,
        AnalyticsArtifactKindV1::Checkpoint,
        7,
        2,
        2,
        digest,
    )
    .unwrap();
    let command = CommandEnvelopeV1::new(
        11,
        3,
        922,
        CommandBodyV1::PinAnalyticsArtifactGeneration(pin),
    );
    let encoded = command.encode().unwrap();
    assert_eq!(CommandEnvelopeV1::decode(&encoded).unwrap(), command);
    assert_eq!(
        CommandEnvelopeV1::decode(&encoded)
            .unwrap()
            .encode()
            .unwrap(),
        encoded
    );

    for (count, total_bytes, content_digest) in [
        (0, 1, digest),
        (1, 0, digest),
        (2, 1, digest),
        (4097, 4097, digest),
        (1, MAX_ANALYTICS_ARTIFACT_CHUNK_BYTES as u64 + 1, digest),
        (1, 1, [0; 32]),
    ] {
        assert_eq!(
            PinAnalyticsArtifactGenerationV1::new(
                501,
                AnalyticsArtifactKindV1::Checkpoint,
                7,
                count,
                total_bytes,
                content_digest,
            ),
            Err(CommandCodecError::InvalidAnalyticsArtifact)
        );
    }
}

#[test]
fn backend_lifecycle_codec_rejects_zero_nonconsecutive_generations_and_zero_digest() {
    let invalid = [
        CommandBodyV1::BeginBackendDualApply(BeginBackendDualApplyV1 {
            source_generation: 0,
            target_generation: 1,
            target_profile_digest: [1; 32],
            fence_index: 0,
        }),
        CommandBodyV1::CutoverBackend(CutoverBackendV1 {
            source_generation: 7,
            target_generation: 9,
            target_profile_digest: [1; 32],
        }),
        CommandBodyV1::AbortBackendMigration(AbortBackendMigrationV1 {
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: [0; 32],
        }),
    ];

    for body in invalid {
        assert!(matches!(
            CommandEnvelopeV1::new(11, 3, 920, body).encode(),
            Err(CommandCodecError::InvalidBackendGeneration)
                | Err(CommandCodecError::InvalidBackendProfileDigest)
        ));
    }
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
