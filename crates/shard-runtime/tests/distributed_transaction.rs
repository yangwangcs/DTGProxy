use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_rocksdb::RocksAdapter;
use raft_command::{
    AbortIntentV1, CommandBodyV1, CommandEnvelopeV1, FinalizeV1, OnePhaseCommitV1, PrewriteV1,
    RecordDecisionV1,
};
use shard_runtime::ShardStateMachine;
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter};
use temporal_types::TransactionTime;
use txn_protocol::{
    HomeDecisionEngine, HomeTransactionRecord, IsolationLevel, ParticipantEngine, ParticipantProof,
    PrewriteRequest, ShardEpoch, TransactionId, TransactionState,
};

fn participant() -> ShardEpoch {
    ShardEpoch::new(7, 9).unwrap()
}

fn request(transaction_id: u128, start: i64) -> PrewriteRequest {
    PrewriteRequest::new(
        TransactionId::new(transaction_id),
        TransactionTime::new(start, 0),
        3,
        participant(),
        participant(),
        vec![participant()],
        IsolationLevel::TemporalSnapshot,
        TransactionTime::new(start + 100, 0),
        PreparedMutationBatch {
            shard_id: 7,
            txn_id: transaction_id,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, b"vertex/77".to_vec()),
                b"committed".to_vec(),
            )],
        },
    )
    .unwrap()
}

fn command(request_id: u128, body: CommandBodyV1) -> Vec<u8> {
    CommandEnvelopeV1::new(7, 9, request_id, body)
        .encode()
        .unwrap()
}

#[test]
fn prewrite_survives_restart_home_decision_and_finalize_release_the_safe_frontier() {
    let request = request(77, 100);
    let proof = ParticipantProof::new(
        participant(),
        TransactionTime::new(100, 1),
        request.intent_digest(),
    );
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &command(
            1001,
            CommandBodyV1::ClosedTimestampTick(TransactionTime::new(90, 0)),
        ),
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        2,
        &command(
            1002,
            CommandBodyV1::Prewrite(PrewriteV1 {
                request: request.clone(),
                expected_proof: proof.clone(),
            }),
        ),
    ))
    .unwrap();
    assert_eq!(read_current(machine.adapter()), None);
    let lock_keys = ParticipantEngine::prewrite_inspection_keys(&request).unwrap();
    assert!(read(machine.adapter(), &lock_keys[1]).is_some());

    block_on(machine.apply_entry(
        1,
        3,
        &command(
            1003,
            CommandBodyV1::ClosedTimestampTick(TransactionTime::new(120, 0)),
        ),
    ))
    .unwrap();
    assert!(machine.metadata().resolved_ts < request.start_ts());

    let adapter = machine.into_adapter();
    let mut machine = block_on(ShardStateMachine::open(adapter, 7, 9)).unwrap();
    assert!(machine.metadata().resolved_ts < request.start_ts());

    let decision = HomeTransactionRecord::new(
        request.transaction_id(),
        request.start_ts(),
        TransactionState::Committed,
        Some(TransactionTime::new(101, 0)),
        request.participants().to_vec(),
        vec![proof],
    )
    .unwrap();
    block_on(machine.apply_entry(
        1,
        4,
        &command(
            1004,
            CommandBodyV1::RecordDecision(RecordDecisionV1 {
                home: participant(),
                decision: decision.clone(),
            }),
        ),
    ))
    .unwrap();
    let home_key =
        HomeDecisionEngine::inspection_key(participant(), request.transaction_id()).unwrap();
    assert_eq!(
        HomeTransactionRecord::decode(&read(machine.adapter(), &home_key).unwrap()).unwrap(),
        decision
    );

    let finalize = command(
        1005,
        CommandBodyV1::Finalize(FinalizeV1 {
            participant: participant(),
            transaction_id: request.transaction_id(),
            intent_digest: request.intent_digest(),
            commit_ts: TransactionTime::new(101, 0),
        }),
    );
    block_on(machine.apply_entry(1, 5, &finalize)).unwrap();
    assert_eq!(read_current(machine.adapter()), Some(b"committed".to_vec()));
    assert!(read(machine.adapter(), &lock_keys[1]).is_none());
    assert_eq!(machine.metadata().resolved_ts, TransactionTime::new(120, 0));
    assert!(
        block_on(machine.apply_entry(1, 5, &finalize))
            .unwrap()
            .duplicate
    );
}

#[test]
fn replicated_abort_releases_intent_without_materializing_business_data() {
    let request = request(88, 100);
    let proof = ParticipantProof::new(
        participant(),
        TransactionTime::new(100, 1),
        request.intent_digest(),
    );
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &command(
            2001,
            CommandBodyV1::Prewrite(PrewriteV1 {
                request: request.clone(),
                expected_proof: proof,
            }),
        ),
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        2,
        &command(
            2002,
            CommandBodyV1::AbortIntent(AbortIntentV1 {
                participant: participant(),
                transaction_id: request.transaction_id(),
                intent_digest: request.intent_digest(),
            }),
        ),
    ))
    .unwrap();
    assert_eq!(read_current(machine.adapter()), None);
    let keys = ParticipantEngine::prewrite_inspection_keys(&request).unwrap();
    assert!(read(machine.adapter(), &keys[1]).is_none());
}

#[test]
fn a_client_retry_in_a_new_log_entry_does_not_collide_with_adapter_fingerprints() {
    let request = request(99, 100);
    let proof = ParticipantProof::new(
        participant(),
        TransactionTime::new(100, 1),
        request.intent_digest(),
    );
    let prewrite = command(
        3001,
        CommandBodyV1::Prewrite(PrewriteV1 {
            request,
            expected_proof: proof,
        }),
    );
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    assert!(
        !block_on(machine.apply_entry(1, 1, &prewrite))
            .unwrap()
            .duplicate
    );
    assert!(
        !block_on(machine.apply_entry(1, 2, &prewrite))
            .unwrap()
            .duplicate
    );
    assert_eq!(machine.metadata().applied_index, 2);
}

#[test]
fn rocksdb_restart_restores_the_unresolved_intent_frontier() {
    let directory = tempfile::tempdir().unwrap();
    let request = request(111, 100);
    let proof = ParticipantProof::new(
        participant(),
        TransactionTime::new(100, 1),
        request.intent_digest(),
    );
    let mut machine = block_on(ShardStateMachine::open(
        RocksAdapter::open(directory.path()).unwrap(),
        7,
        9,
    ))
    .unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &command(
            4001,
            CommandBodyV1::Prewrite(PrewriteV1 {
                request: request.clone(),
                expected_proof: proof,
            }),
        ),
    ))
    .unwrap();
    drop(machine);

    let mut recovered = block_on(ShardStateMachine::open(
        RocksAdapter::open(directory.path()).unwrap(),
        7,
        9,
    ))
    .unwrap();
    block_on(recovered.apply_entry(
        1,
        2,
        &command(
            4002,
            CommandBodyV1::ClosedTimestampTick(TransactionTime::new(150, 0)),
        ),
    ))
    .unwrap();
    assert!(recovered.metadata().resolved_ts < request.start_ts());
    let keys = ParticipantEngine::prewrite_inspection_keys(&request).unwrap();
    assert!(read(recovered.adapter(), &keys[1]).is_some());
}

#[test]
fn one_phase_commit_retry_after_state_machine_reopen_is_durably_idempotent() {
    let participant = ShardEpoch::new(7, 9).unwrap();
    let request = PrewriteRequest::new(
        TransactionId::new(500),
        TransactionTime::new(100, 0),
        3,
        participant,
        participant,
        vec![participant],
        IsolationLevel::TemporalSnapshot,
        TransactionTime::new(200, 0),
        PreparedMutationBatch {
            shard_id: 7,
            txn_id: 500,
            mutations: vec![Mutation::put(
                0,
                LogicalKey::in_keyspace(Keyspace::Current, b"one-phase".to_vec()),
                b"durable".to_vec(),
            )],
        },
    )
    .unwrap();
    let command = command(
        5001,
        CommandBodyV1::OnePhaseCommit(OnePhaseCommitV1 {
            expected_proof: ParticipantProof::new(
                participant,
                TransactionTime::new(100, 1),
                request.intent_digest(),
            ),
            request,
            commit_ts: TransactionTime::new(101, 0),
        }),
    );
    let mut machine = block_on(ShardStateMachine::open(MemoryAdapter::new(), 7, 9)).unwrap();
    block_on(machine.apply_entry(1, 1, &command)).unwrap();
    let mut recovered = block_on(ShardStateMachine::open(machine.into_adapter(), 7, 9)).unwrap();
    block_on(recovered.apply_entry(1, 2, &command)).unwrap();

    assert_eq!(recovered.metadata().applied_index, 2);
    assert_eq!(
        read(
            recovered.adapter(),
            &LogicalKey::in_keyspace(Keyspace::Current, b"one-phase".to_vec())
        ),
        Some(b"durable".to_vec())
    );
}

fn read_current<A: StorageAdapter>(adapter: &A) -> Option<Vec<u8>> {
    read(
        adapter,
        &LogicalKey::in_keyspace(Keyspace::Current, b"vertex/77".to_vec()),
    )
}

fn read<A: StorageAdapter>(adapter: &A, key: &LogicalKey) -> Option<Vec<u8>> {
    block_on(adapter.multi_get(std::slice::from_ref(key)))
        .unwrap()
        .pop()
        .flatten()
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
