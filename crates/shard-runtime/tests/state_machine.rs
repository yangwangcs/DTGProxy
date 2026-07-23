use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use raft_command::{
    AbortBackendMigrationV1, AnalyticsArtifactKindV1, ApplyPreparedV1, BeginBackendDualApplyV1,
    CommandBodyV1, CommandEnvelopeV1, CutoverBackendV1, DeleteAnalyticsArtifactGenerationV1,
    PinAnalyticsArtifactGenerationV1, PutAnalyticsArtifactChunkV1,
};
use shard_runtime::{
    BackendLifecycle, CommittedEntryOutcome, MIN_REPLICA_TIME, ShardRuntimeError,
    ShardStateMachine, analytics_artifact_chunk_key, analytics_artifact_generation_pin_key,
    decode_analytics_artifact_chunk, decode_analytics_artifact_generation_pin,
};
use storage_api::{
    AdapterCapabilities, AdapterError, AdapterFuture, KeySpan, KeyValue, Keyspace, LogicalKey,
    Mutation, PreparedMutationBatch, StorageAdapter,
};
use temporal_types::TransactionTime;

fn ts(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn apply_command(epoch: u64, request_id: u128, commit: i64, value: &[u8]) -> Vec<u8> {
    CommandEnvelopeV1::new(
        7,
        epoch,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: ts(commit),
            batch: PreparedMutationBatch {
                shard_id: 7,
                txn_id: 500,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
                    value.to_vec(),
                )],
            },
        }),
    )
    .encode()
    .unwrap()
}

fn tick_command(epoch: u64, request_id: u128, closed: i64) -> Vec<u8> {
    CommandEnvelopeV1::new(
        7,
        epoch,
        request_id,
        CommandBodyV1::ClosedTimestampTick(ts(closed)),
    )
    .encode()
    .unwrap()
}

fn backend_command(request_id: u128, body: CommandBodyV1) -> Vec<u8> {
    CommandEnvelopeV1::new(7, 9, request_id, body)
        .encode()
        .unwrap()
}

#[test]
fn analytics_artifact_chunks_are_chained_generation_fenced_and_recovered() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let first = PutAnalyticsArtifactChunkV1::new(
        501,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1_725_000_000_123,
        0,
        [0; 32],
        b"first".to_vec(),
    )
    .unwrap();
    let first_digest = first.payload_digest;
    let first_command =
        backend_command(901, CommandBodyV1::PutAnalyticsArtifactChunk(first.clone()));
    block_on(machine.apply_entry(1, 1, &first_command)).unwrap();
    let second = PutAnalyticsArtifactChunkV1::new(
        501,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1_725_000_000_123,
        1,
        first_digest,
        b"second".to_vec(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        2,
        &backend_command(902, CommandBodyV1::PutAnalyticsArtifactChunk(second)),
    ))
    .unwrap();

    let stored = block_on(machine.adapter().multi_get(&[analytics_artifact_chunk_key(
        501,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1,
    )]))
    .unwrap()
    .pop()
    .flatten()
    .unwrap();
    assert_eq!(
        decode_analytics_artifact_chunk(&stored).unwrap().payload(),
        b"second"
    );

    let fork = PutAnalyticsArtifactChunkV1::new(
        501,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1_725_000_000_123,
        2,
        [7; 32],
        b"fork".to_vec(),
    )
    .unwrap();
    assert!(matches!(
        block_on(machine.apply_entry(
            1,
            3,
            &backend_command(903, CommandBodyV1::PutAnalyticsArtifactChunk(fork)),
        )),
        Err(ShardRuntimeError::AnalyticsArtifactFence)
    ));

    block_on(
        machine.apply_entry(
            1,
            3,
            &backend_command(
                904,
                CommandBodyV1::DeleteAnalyticsArtifactGeneration(
                    DeleteAnalyticsArtifactGenerationV1::new(
                        501,
                        AnalyticsArtifactKindV1::Checkpoint,
                        1,
                    )
                    .unwrap(),
                ),
            ),
        ),
    )
    .unwrap();
    let adapter = machine.into_adapter();
    assert!(
        block_on(adapter.multi_get(&[analytics_artifact_chunk_key(
            501,
            AnalyticsArtifactKindV1::Checkpoint,
            1,
            0,
        )]))
        .unwrap()[0]
            .is_none()
    );
    let mut reopened = block_on(ShardStateMachine::open(adapter, 7, 9)).unwrap();
    let stale = PutAnalyticsArtifactChunkV1::new(
        501,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1_725_000_000_123,
        0,
        [0; 32],
        b"stale".to_vec(),
    )
    .unwrap();
    assert!(matches!(
        block_on(reopened.apply_entry(
            2,
            4,
            &backend_command(905, CommandBodyV1::PutAnalyticsArtifactChunk(stale)),
        )),
        Err(ShardRuntimeError::AnalyticsArtifactFence)
    ));
}

#[test]
fn analytics_artifact_generation_is_pinned_only_after_full_validation() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let first = PutAnalyticsArtifactChunkV1::new(
        520,
        AnalyticsArtifactKindV1::Result,
        3,
        1_725_000_000_123,
        0,
        [0; 32],
        b"first".to_vec(),
    )
    .unwrap();
    let second = PutAnalyticsArtifactChunkV1::new(
        520,
        AnalyticsArtifactKindV1::Result,
        3,
        1_725_000_000_123,
        1,
        first.payload_digest,
        b"second".to_vec(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &backend_command(930, CommandBodyV1::PutAnalyticsArtifactChunk(first)),
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        2,
        &backend_command(931, CommandBodyV1::PutAnalyticsArtifactChunk(second)),
    ))
    .unwrap();
    let content_digest = *blake3::hash(b"firstsecond").as_bytes();
    let pin = PinAnalyticsArtifactGenerationV1::new(
        520,
        AnalyticsArtifactKindV1::Result,
        3,
        2,
        11,
        content_digest,
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        3,
        &backend_command(932, CommandBodyV1::PinAnalyticsArtifactGeneration(pin)),
    ))
    .unwrap();

    let stored = block_on(
        machine
            .adapter()
            .multi_get(&[analytics_artifact_generation_pin_key(
                520,
                AnalyticsArtifactKindV1::Result,
                3,
            )]),
    )
    .unwrap()
    .pop()
    .flatten()
    .unwrap();
    let stored = decode_analytics_artifact_generation_pin(&stored).unwrap();
    assert_eq!(stored.expected_chunk_count(), 2);
    assert_eq!(stored.expected_total_bytes(), 11);
    assert_eq!(stored.expected_content_digest(), content_digest);

    let adapter = machine.into_adapter();
    let recovered = block_on(ShardStateMachine::open(adapter, 7, 9)).unwrap();
    let stored = block_on(
        recovered
            .adapter()
            .multi_get(&[analytics_artifact_generation_pin_key(
                520,
                AnalyticsArtifactKindV1::Result,
                3,
            )]),
    )
    .unwrap()
    .pop()
    .flatten()
    .unwrap();
    assert_eq!(
        decode_analytics_artifact_generation_pin(&stored)
            .unwrap()
            .expected_content_digest(),
        content_digest
    );
}

#[test]
fn pinned_generation_is_idempotent_immutable_and_survives_newer_generation() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let payload = b"pinned";
    let first = PutAnalyticsArtifactChunkV1::new(
        521,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1_725_000_000_123,
        0,
        [0; 32],
        payload.to_vec(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &backend_command(933, CommandBodyV1::PutAnalyticsArtifactChunk(first.clone())),
    ))
    .unwrap();
    let pin = PinAnalyticsArtifactGenerationV1::new(
        521,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1,
        u64::try_from(payload.len()).unwrap(),
        *blake3::hash(payload).as_bytes(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        2,
        &backend_command(934, CommandBodyV1::PinAnalyticsArtifactGeneration(pin)),
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        3,
        &backend_command(935, CommandBodyV1::PinAnalyticsArtifactGeneration(pin)),
    ))
    .unwrap();

    let conflicting_pin = PinAnalyticsArtifactGenerationV1::new(
        521,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1,
        u64::try_from(payload.len()).unwrap(),
        [0x55; 32],
    )
    .unwrap();
    assert!(matches!(
        block_on(machine.apply_entry(
            1,
            4,
            &backend_command(
                936,
                CommandBodyV1::PinAnalyticsArtifactGeneration(conflicting_pin),
            ),
        )),
        Err(ShardRuntimeError::AnalyticsArtifactConflict)
    ));
    let overwrite = PutAnalyticsArtifactChunkV1::new(
        521,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1_725_000_000_123,
        0,
        [0; 32],
        payload.to_vec(),
    )
    .unwrap();
    assert!(matches!(
        block_on(machine.apply_entry(
            1,
            4,
            &backend_command(937, CommandBodyV1::PutAnalyticsArtifactChunk(overwrite),),
        )),
        Err(ShardRuntimeError::AnalyticsArtifactFence)
    ));
    let delete =
        DeleteAnalyticsArtifactGenerationV1::new(521, AnalyticsArtifactKindV1::Checkpoint, 1)
            .unwrap();
    assert!(matches!(
        block_on(machine.apply_entry(
            1,
            4,
            &backend_command(
                938,
                CommandBodyV1::DeleteAnalyticsArtifactGeneration(delete),
            ),
        )),
        Err(ShardRuntimeError::AnalyticsArtifactFence)
    ));

    let next = PutAnalyticsArtifactChunkV1::new(
        521,
        AnalyticsArtifactKindV1::Checkpoint,
        2,
        1_725_000_000_124,
        0,
        [0; 32],
        b"newer".to_vec(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        4,
        &backend_command(939, CommandBodyV1::PutAnalyticsArtifactChunk(next)),
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        5,
        &backend_command(
            940,
            CommandBodyV1::DeleteAnalyticsArtifactGeneration(delete),
        ),
    ))
    .unwrap();
    assert!(
        block_on(
            machine
                .adapter()
                .multi_get(&[analytics_artifact_generation_pin_key(
                    521,
                    AnalyticsArtifactKindV1::Checkpoint,
                    1,
                ),])
        )
        .unwrap()[0]
            .is_none()
    );
}

#[test]
fn analytics_artifact_rejects_exact_old_generation_replay_after_generation_advances() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let first = PutAnalyticsArtifactChunkV1::new(
        502,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1_725_000_000_123,
        0,
        [0; 32],
        b"first".to_vec(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &backend_command(906, CommandBodyV1::PutAnalyticsArtifactChunk(first.clone())),
    ))
    .unwrap();
    let next_generation = PutAnalyticsArtifactChunkV1::new(
        502,
        AnalyticsArtifactKindV1::Checkpoint,
        2,
        1_725_000_000_124,
        0,
        [0; 32],
        b"replacement".to_vec(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        2,
        &backend_command(
            907,
            CommandBodyV1::PutAnalyticsArtifactChunk(next_generation),
        ),
    ))
    .unwrap();

    assert!(matches!(
        block_on(machine.apply_entry(
            1,
            3,
            &backend_command(908, CommandBodyV1::PutAnalyticsArtifactChunk(first)),
        )),
        Err(ShardRuntimeError::AnalyticsArtifactFence)
    ));
}

#[test]
fn analytics_artifact_deletes_old_generation_without_sealing_current_generation() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let old = PutAnalyticsArtifactChunkV1::new(
        503,
        AnalyticsArtifactKindV1::Result,
        1,
        1_725_000_000_123,
        0,
        [0; 32],
        b"old".to_vec(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &backend_command(909, CommandBodyV1::PutAnalyticsArtifactChunk(old)),
    ))
    .unwrap();
    let current = PutAnalyticsArtifactChunkV1::new(
        503,
        AnalyticsArtifactKindV1::Result,
        2,
        1_725_000_000_124,
        0,
        [0; 32],
        b"current".to_vec(),
    )
    .unwrap();
    let current_digest = current.payload_digest;
    block_on(machine.apply_entry(
        1,
        2,
        &backend_command(910, CommandBodyV1::PutAnalyticsArtifactChunk(current)),
    ))
    .unwrap();

    block_on(
        machine.apply_entry(
            1,
            3,
            &backend_command(
                911,
                CommandBodyV1::DeleteAnalyticsArtifactGeneration(
                    DeleteAnalyticsArtifactGenerationV1::new(
                        503,
                        AnalyticsArtifactKindV1::Result,
                        1,
                    )
                    .unwrap(),
                ),
            ),
        ),
    )
    .unwrap();
    assert!(
        block_on(machine.adapter().multi_get(&[analytics_artifact_chunk_key(
            503,
            AnalyticsArtifactKindV1::Result,
            1,
            0,
        )]))
        .unwrap()[0]
            .is_none()
    );

    let append_current = PutAnalyticsArtifactChunkV1::new(
        503,
        AnalyticsArtifactKindV1::Result,
        2,
        1_725_000_000_124,
        1,
        current_digest,
        b"current-next".to_vec(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        4,
        &backend_command(
            912,
            CommandBodyV1::PutAnalyticsArtifactChunk(append_current),
        ),
    ))
    .unwrap();
}

#[test]
fn analytics_artifact_retries_preserve_temporal_watermarks_and_generation_rules() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    block_on(machine.apply_entry(1, 1, &apply_command(9, 913, 100, b"business"))).unwrap();
    let watermark = machine.metadata().adapter_applied_ts;
    let first = PutAnalyticsArtifactChunkV1::new(
        504,
        AnalyticsArtifactKindV1::Checkpoint,
        1,
        1_725_000_000_123,
        0,
        [0; 32],
        b"first".to_vec(),
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        2,
        &backend_command(914, CommandBodyV1::PutAnalyticsArtifactChunk(first.clone())),
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        3,
        &backend_command(915, CommandBodyV1::PutAnalyticsArtifactChunk(first.clone())),
    ))
    .unwrap();
    assert_eq!(machine.metadata().adapter_applied_ts, watermark);
    assert_eq!(machine.metadata().closed_ts, MIN_REPLICA_TIME);
    assert_eq!(machine.metadata().resolved_ts, MIN_REPLICA_TIME);

    block_on(
        machine.apply_entry(
            1,
            4,
            &backend_command(
                916,
                CommandBodyV1::DeleteAnalyticsArtifactGeneration(
                    DeleteAnalyticsArtifactGenerationV1::new(
                        504,
                        AnalyticsArtifactKindV1::Checkpoint,
                        1,
                    )
                    .unwrap(),
                ),
            ),
        ),
    )
    .unwrap();
    assert!(matches!(
        block_on(machine.apply_entry(
            1,
            5,
            &backend_command(917, CommandBodyV1::PutAnalyticsArtifactChunk(first)),
        )),
        Err(ShardRuntimeError::AnalyticsArtifactFence)
    ));
    let non_initial_generation = PutAnalyticsArtifactChunkV1::new(
        504,
        AnalyticsArtifactKindV1::Checkpoint,
        2,
        1_725_000_000_124,
        1,
        [8; 32],
        b"invalid-new-generation".to_vec(),
    )
    .unwrap();
    assert!(matches!(
        block_on(machine.apply_entry(
            1,
            5,
            &backend_command(
                918,
                CommandBodyV1::PutAnalyticsArtifactChunk(non_initial_generation),
            ),
        )),
        Err(ShardRuntimeError::AnalyticsArtifactFence)
    ));
    assert_eq!(machine.metadata().applied_index, 4);
    assert_eq!(machine.metadata().adapter_applied_ts, watermark);
}

#[test]
fn business_commands_cannot_write_the_reserved_analytics_namespace() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let command = backend_command(
        919,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: ts(100),
            batch: PreparedMutationBatch {
                shard_id: 7,
                txn_id: 501,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(
                        Keyspace::Meta,
                        b"\x01dtg/analytics/v1/chunk/forbidden".to_vec(),
                    ),
                    b"forbidden".to_vec(),
                )],
            },
        }),
    );
    assert!(matches!(
        block_on(machine.apply_entry(1, 1, &command)),
        Err(ShardRuntimeError::ReservedMetadataKey)
    ));
    assert_eq!(machine.metadata().applied_index, 0);
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 0);
}

#[test]
fn backend_lifecycle_is_replicated_validated_and_recovered_with_metadata() {
    let digest = [0x7b; 32];
    let mut machine = block_on(ShardStateMachine::open_with_backend_generation(
        MemoryAdapter::new(),
        7,
        9,
        7,
    ))
    .unwrap();
    let begin = backend_command(
        801,
        CommandBodyV1::BeginBackendDualApply(BeginBackendDualApplyV1 {
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: digest,
            fence_index: 0,
        }),
    );
    block_on(machine.apply_entry(1, 1, &begin)).unwrap();
    assert_eq!(
        machine.metadata().backend_lifecycle,
        BackendLifecycle::DualApplying {
            target_generation: 8,
            target_profile_digest: digest,
            fence_index: 0,
        }
    );

    block_on(machine.apply_entry(1, 2, &apply_command(9, 802, 100, b"during-dual"))).unwrap();
    let cutover = backend_command(
        803,
        CommandBodyV1::CutoverBackend(CutoverBackendV1 {
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: digest,
        }),
    );
    block_on(machine.apply_entry(1, 3, &cutover)).unwrap();
    assert_eq!(machine.metadata().backend_generation, 8);
    assert_eq!(
        machine.metadata().backend_lifecycle,
        BackendLifecycle::Active
    );

    let stale_abort = backend_command(
        804,
        CommandBodyV1::AbortBackendMigration(AbortBackendMigrationV1 {
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: digest,
        }),
    );
    assert!(matches!(
        block_on(machine.apply_entry(1, 4, &stale_abort)),
        Err(ShardRuntimeError::BackendLifecycleConflict)
    ));
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 3);

    let adapter = machine.into_adapter();
    let recovered = block_on(ShardStateMachine::open_with_backend_generation(
        adapter, 7, 9, 8,
    ))
    .unwrap();
    assert_eq!(recovered.metadata().backend_generation, 8);
    assert_eq!(
        recovered.metadata().backend_lifecycle,
        BackendLifecycle::Active
    );
}

#[test]
fn backend_abort_returns_to_the_source_generation() {
    let digest = [0x21; 32];
    let mut machine = block_on(ShardStateMachine::open_with_backend_generation(
        MemoryAdapter::new(),
        7,
        9,
        4,
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &backend_command(
            811,
            CommandBodyV1::BeginBackendDualApply(BeginBackendDualApplyV1 {
                source_generation: 4,
                target_generation: 5,
                target_profile_digest: digest,
                fence_index: 0,
            }),
        ),
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        2,
        &backend_command(
            812,
            CommandBodyV1::AbortBackendMigration(AbortBackendMigrationV1 {
                source_generation: 4,
                target_generation: 5,
                target_profile_digest: digest,
            }),
        ),
    ))
    .unwrap();
    assert_eq!(machine.metadata().backend_generation, 4);
    assert_eq!(
        machine.metadata().backend_lifecycle,
        BackendLifecycle::Active
    );
}

#[test]
fn backend_abort_can_cancel_local_preparation_before_dual_apply_begins() {
    let mut machine = block_on(ShardStateMachine::open_with_backend_generation(
        MemoryAdapter::new(),
        7,
        9,
        4,
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &backend_command(
            813,
            CommandBodyV1::AbortBackendMigration(AbortBackendMigrationV1 {
                source_generation: 4,
                target_generation: 5,
                target_profile_digest: [0x22; 32],
            }),
        ),
    ))
    .unwrap();
    assert_eq!(machine.metadata().backend_generation, 4);
    assert_eq!(
        machine.metadata().backend_lifecycle,
        BackendLifecycle::Active
    );
    assert_eq!(machine.metadata().applied_index, 1);
}

#[test]
fn committed_business_rejection_advances_and_replays_before_the_next_entry() {
    let mut machine = block_on(ShardStateMachine::open_with_backend_generation(
        MemoryAdapter::new(),
        7,
        9,
        4,
    ))
    .unwrap();
    let rejected = backend_command(
        814,
        CommandBodyV1::CutoverBackend(CutoverBackendV1 {
            source_generation: 4,
            target_generation: 5,
            target_profile_digest: [0x23; 32],
        }),
    );

    let outcome = block_on(machine.apply_committed_entry(1, 1, &rejected)).unwrap();
    assert!(matches!(
        outcome,
        CommittedEntryOutcome::Rejected { ref message, .. }
            if message.contains("backend lifecycle")
    ));
    assert_eq!(machine.metadata().applied_index, 1);
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 1);
    assert!(matches!(
        block_on(machine.request_replay(814, &rejected)),
        Err(ShardRuntimeError::CommittedRejection { .. })
    ));

    let applied = apply_command(9, 815, 100, b"after-rejection");
    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 2, &applied)).unwrap(),
        CommittedEntryOutcome::Applied(_)
    ));
    assert_eq!(machine.metadata().applied_index, 2);
    assert_eq!(
        read_current(machine.adapter()),
        Some(b"after-rejection".to_vec())
    );
}

#[test]
fn committed_rejection_and_following_entry_survive_reopen() {
    let mut machine = block_on(ShardStateMachine::open_with_backend_generation(
        MemoryAdapter::new(),
        7,
        9,
        4,
    ))
    .unwrap();
    let rejected = backend_command(
        816,
        CommandBodyV1::CutoverBackend(CutoverBackendV1 {
            source_generation: 4,
            target_generation: 5,
            target_profile_digest: [0x24; 32],
        }),
    );

    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 1, &rejected)).unwrap(),
        CommittedEntryOutcome::Rejected { .. }
    ));
    let accepted = apply_command(9, 817, 100, b"after-reopen");
    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 2, &accepted)).unwrap(),
        CommittedEntryOutcome::Applied(_)
    ));

    let adapter = machine.into_adapter();
    let reopened = block_on(ShardStateMachine::open_with_backend_generation(
        adapter, 7, 9, 4,
    ))
    .unwrap();
    assert_eq!(reopened.metadata().applied_index, 2);
    assert_eq!(
        read_current(reopened.adapter()),
        Some(b"after-reopen".to_vec())
    );
    assert!(matches!(
        block_on(reopened.request_replay(816, &rejected)),
        Err(ShardRuntimeError::CommittedRejection { message, .. })
            if message.contains("backend lifecycle")
    ));
    assert!(block_on(reopened.request_replay(817, &accepted)).unwrap());
}

#[test]
fn committed_applied_duplicate_advances_after_epoch_activation() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let applied = apply_command(9, 818, 100, b"before-epoch-change");
    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 1, &applied)).unwrap(),
        CommittedEntryOutcome::Applied(_)
    ));
    let activate = CommandEnvelopeV1::new(7, 9, 819, CommandBodyV1::ActivatePlacementEpoch(10))
        .encode()
        .unwrap();
    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 2, &activate)).unwrap(),
        CommittedEntryOutcome::Applied(_)
    ));

    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 3, &applied)).unwrap(),
        CommittedEntryOutcome::Applied(receipt) if receipt.duplicate
    ));
    assert_eq!(machine.metadata().placement_epoch, 10);
    assert_eq!(machine.metadata().applied_index, 3);
    assert!(block_on(machine.request_replay(818, &applied)).unwrap());
}

#[test]
fn committed_rejected_duplicate_advances_after_epoch_activation() {
    let mut machine = block_on(ShardStateMachine::open_with_backend_generation(
        MemoryAdapter::new(),
        7,
        9,
        4,
    ))
    .unwrap();
    let rejected = backend_command(
        820,
        CommandBodyV1::CutoverBackend(CutoverBackendV1 {
            source_generation: 4,
            target_generation: 5,
            target_profile_digest: [0x25; 32],
        }),
    );
    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 1, &rejected)).unwrap(),
        CommittedEntryOutcome::Rejected { .. }
    ));
    let activate = CommandEnvelopeV1::new(7, 9, 821, CommandBodyV1::ActivatePlacementEpoch(10))
        .encode()
        .unwrap();
    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 2, &activate)).unwrap(),
        CommittedEntryOutcome::Applied(_)
    ));

    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 3, &rejected)).unwrap(),
        CommittedEntryOutcome::Rejected { receipt, .. } if receipt.duplicate
    ));
    assert_eq!(machine.metadata().placement_epoch, 10);
    assert_eq!(machine.metadata().applied_index, 3);
    assert!(matches!(
        block_on(machine.request_replay(820, &rejected)),
        Err(ShardRuntimeError::CommittedRejection { .. })
    ));
}

#[test]
fn committed_new_request_with_stale_epoch_is_rejected_and_advances() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let stale = apply_command(8, 822, 100, b"stale-authority");

    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 1, &stale)).unwrap(),
        CommittedEntryOutcome::Rejected { ref message, .. }
            if message.contains("placement epoch")
    ));
    assert_eq!(machine.metadata().applied_index, 1);
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 1);
    assert!(matches!(
        block_on(machine.request_replay(822, &stale)),
        Err(ShardRuntimeError::CommittedRejection { .. })
    ));
}

#[test]
fn an_exact_epoch_activation_retry_is_recognized_after_the_epoch_changes() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let command = CommandEnvelopeV1::new(7, 9, 150, CommandBodyV1::ActivatePlacementEpoch(10))
        .encode()
        .unwrap();
    block_on(machine.apply_entry(1, 1, &command)).unwrap();
    assert_eq!(machine.metadata().placement_epoch, 10);
    assert!(block_on(machine.request_replay(150, &command)).unwrap());
}

#[test]
fn committed_commands_atomically_advance_business_state_metadata_and_safe_time() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    assert_eq!(machine.metadata().safe_ts(), MIN_REPLICA_TIME);

    let apply = apply_command(9, 101, 100, b"v1");
    let receipt = block_on(machine.apply_entry(1, 1, &apply)).unwrap();
    assert!(!receipt.duplicate);
    assert_eq!(machine.metadata().applied_index, 1);
    assert_eq!(machine.metadata().adapter_applied_ts, ts(100));
    assert_eq!(machine.metadata().safe_ts(), MIN_REPLICA_TIME);
    assert_eq!(read_current(machine.adapter()), Some(b"v1".to_vec()));

    let tick = tick_command(9, 102, 100);
    block_on(machine.apply_entry(1, 2, &tick)).unwrap();
    assert_eq!(machine.metadata().closed_ts, ts(100));
    assert_eq!(machine.metadata().resolved_ts, ts(100));
    assert_eq!(machine.servable_safe_ts().unwrap(), ts(100));
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 2);
}

#[test]
fn closed_timestamp_may_trail_applied_commits_without_regressing_adapter_frontier() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    block_on(machine.apply_entry(1, 1, &apply_command(9, 101, 100, b"v1"))).unwrap();

    block_on(machine.apply_entry(1, 2, &tick_command(9, 102, 50))).unwrap();
    assert_eq!(machine.metadata().closed_ts, ts(50));
    assert_eq!(machine.metadata().adapter_applied_ts, ts(100));
    assert_eq!(machine.servable_safe_ts().unwrap(), ts(50));

    block_on(machine.apply_entry(1, 3, &tick_command(9, 103, 120))).unwrap();
    assert_eq!(machine.metadata().adapter_applied_ts, ts(120));
    assert_eq!(machine.servable_safe_ts().unwrap(), ts(120));
}

#[test]
fn gaps_epoch_mismatch_and_time_regressions_fail_before_adapter_mutation() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();

    assert!(matches!(
        block_on(machine.apply_entry(1, 1, &apply_command(8, 101, 100, b"stale"))),
        Err(ShardRuntimeError::StaleEpoch { .. })
    ));
    assert!(matches!(
        block_on(machine.apply_entry(1, 2, &apply_command(9, 102, 100, b"gap"))),
        Err(ShardRuntimeError::NonContiguousIndex {
            expected: 1,
            actual: 2,
        })
    ));
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 0);
    assert_eq!(read_current(machine.adapter()), None);

    block_on(machine.apply_entry(2, 1, &apply_command(9, 103, 100, b"first"))).unwrap();
    assert!(matches!(
        block_on(machine.apply_entry(2, 2, &apply_command(9, 104, 99, b"older"))),
        Err(ShardRuntimeError::NonMonotonicCommit { .. })
    ));
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 1);

    block_on(machine.apply_entry(2, 2, &tick_command(9, 105, 100))).unwrap();
    assert!(matches!(
        block_on(machine.apply_entry(2, 3, &apply_command(9, 106, 100, b"late"))),
        Err(ShardRuntimeError::CommitAtOrBeforeClosed { .. })
    ));
    assert!(matches!(
        block_on(machine.apply_entry(1, 3, &apply_command(9, 107, 101, b"term"))),
        Err(ShardRuntimeError::NonMonotonicTerm { .. })
    ));
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 2);
}

#[test]
fn old_entry_replay_is_idempotent_but_divergent_replay_fails_closed() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let apply = apply_command(9, 101, 100, b"v1");
    block_on(machine.apply_entry(1, 1, &apply)).unwrap();
    block_on(machine.apply_entry(1, 2, &tick_command(9, 102, 100))).unwrap();

    let replay = block_on(machine.apply_entry(1, 1, &apply)).unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.applied_log_index, 2);
    assert!(matches!(
        block_on(machine.apply_entry(1, 1, &apply_command(9, 101, 100, b"different"))),
        Err(ShardRuntimeError::DivergentReplay { index: 1 })
    ));
    assert_eq!(read_current(machine.adapter()), Some(b"v1".to_vec()));
}

#[test]
fn request_id_replay_at_a_new_log_index_is_durable_and_payload_bound() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    let original = apply_command(9, 101, 100, b"v1");
    assert!(
        !block_on(machine.apply_entry(1, 1, &original))
            .unwrap()
            .duplicate
    );

    let replay = block_on(machine.apply_entry(1, 2, &original)).unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.applied_log_index, 2);
    assert_eq!(read_current(machine.adapter()), Some(b"v1".to_vec()));

    assert!(matches!(
        block_on(machine.apply_entry(1, 3, &apply_command(9, 101, 200, b"different"))),
        Err(ShardRuntimeError::RequestMismatch { request_id: 101 })
    ));
    assert_eq!(machine.metadata().applied_index, 2);
    assert_eq!(read_current(machine.adapter()), Some(b"v1".to_vec()));
}

#[test]
fn raft_leader_noop_entries_advance_the_same_durable_apply_index() {
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    block_on(machine.apply_noop_entry(1, 1)).unwrap();
    block_on(machine.apply_entry(1, 2, &apply_command(9, 101, 100, b"after-noop"))).unwrap();

    let replay = block_on(machine.apply_noop_entry(1, 1)).unwrap();
    assert!(replay.duplicate);
    assert_eq!(replay.applied_log_index, 2);
    assert_eq!(
        read_current(machine.adapter()),
        Some(b"after-noop".to_vec())
    );
}

#[test]
fn adapter_failure_fences_serving_until_the_same_entry_replays_successfully() {
    let fail_next = Arc::new(AtomicBool::new(false));
    let adapter = FaultAdapter::new(Arc::clone(&fail_next));
    let mut machine = block_on(ShardStateMachine::open(adapter, 7, 9)).unwrap();
    let tick = tick_command(9, 101, 50);
    fail_next.store(true, Ordering::SeqCst);

    assert!(matches!(
        block_on(machine.apply_entry(1, 1, &tick)),
        Err(ShardRuntimeError::Adapter(_))
    ));
    assert!(!machine.is_healthy());
    assert!(matches!(
        machine.servable_safe_ts(),
        Err(ShardRuntimeError::ReplicaFaulted { failed_index: 1 })
    ));
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 0);

    block_on(machine.apply_entry(1, 1, &tick)).unwrap();
    assert!(machine.is_healthy());
    assert_eq!(machine.servable_safe_ts().unwrap(), ts(50));
}

fn read_current<A: StorageAdapter>(adapter: &A) -> Option<Vec<u8>> {
    let key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    block_on(adapter.multi_get(&[key])).unwrap().pop().flatten()
}

struct FaultAdapter {
    inner: MemoryAdapter,
    fail_next: Arc<AtomicBool>,
}

impl FaultAdapter {
    fn new(fail_next: Arc<AtomicBool>) -> Self {
        Self {
            inner: MemoryAdapter::new(),
            fail_next,
        }
    }
}

impl StorageAdapter for FaultAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        self.inner.capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: storage_api::CommittedMutationBatch,
    ) -> AdapterFuture<'a, storage_api::ApplyReceipt> {
        if self.fail_next.swap(false, Ordering::SeqCst) {
            Box::pin(async { Err(AdapterError::Backend("injected apply failure".to_owned())) })
        } else {
            self.inner.apply_committed(batch)
        }
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        self.inner.multi_get(keys)
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.inner.scan(span)
    }

    fn applied_log_index(&self) -> Result<u64, AdapterError> {
        self.inner.applied_log_index()
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct ThreadWaker(std::thread::Thread);

    impl Wake for ThreadWaker {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.0.unpark();
        }
    }

    let waker = Waker::from(Arc::new(ThreadWaker(std::thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::park(),
        }
    }
}
