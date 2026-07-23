use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_rocksdb::RocksAdapter;
use raft_command::{
    AbortIntentV1, CommandBodyV1, CommandEnvelopeV1, FinalizeV1, OnePhaseCommitV1, PrewriteV1,
    RecordDecisionV1,
};
use shard_runtime::{ShardRuntimeError, ShardStateMachine};
use storage_api::{
    AdapterCapabilities, AdapterFuture, KeySpan, KeyValue, Keyspace, LogicalKey, Mutation,
    PreparedMutationBatch, StorageAdapter,
};
use temporal_types::TransactionTime;
use txn_protocol::{
    HomeDecisionEngine, HomeTransactionRecord, IsolationLevel, ParticipantEngine, ParticipantProof,
    PrewriteMetadata, PrewriteRequest, ShardEpoch, TransactionId, TransactionState,
    TxnProtocolError,
};

fn participant() -> ShardEpoch {
    ShardEpoch::new(7, 9).unwrap()
}

fn metadata(schema_version: u64, placement_epoch: u64) -> PrewriteMetadata {
    PrewriteMetadata::new(schema_version, placement_epoch, Vec::new(), Vec::new())
        .expect("current prewrite metadata")
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
        Vec::new(),
        metadata(3, 9),
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
fn a_client_retry_in_a_new_log_entry_advances_raft_without_reapplying_business_state() {
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
        block_on(machine.apply_entry(1, 2, &prewrite))
            .unwrap()
            .duplicate
    );
    assert_eq!(machine.metadata().applied_index, 2);
}

#[test]
fn committed_request_replay_mismatch_is_fatal_and_does_not_advance() {
    let request = request(101, 100);
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
            3101,
            CommandBodyV1::Prewrite(PrewriteV1 {
                request: request.clone(),
                expected_proof: proof,
            }),
        ),
    ))
    .unwrap();
    let mismatched = command(
        3102,
        CommandBodyV1::Finalize(FinalizeV1 {
            participant: participant(),
            transaction_id: request.transaction_id(),
            intent_digest: [0x7f; 32],
            commit_ts: TransactionTime::new(101, 0),
        }),
    );

    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 2, &mismatched)),
        Err(ShardRuntimeError::Transaction(
            TxnProtocolError::RequestReplayMismatch
        ))
    ));
    assert_eq!(machine.metadata().applied_index, 1);
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 1);
    assert!(!block_on(machine.request_replay(3102, &mismatched)).unwrap());
}

#[test]
fn committed_missing_intent_lock_is_fatal_and_does_not_advance() {
    let request = request(102, 100);
    let lock_key = ParticipantEngine::prewrite_inspection_keys(&request).unwrap()[1].clone();
    let hide_lock = Arc::new(AtomicBool::new(false));
    let adapter = MissingLockAdapter {
        inner: MemoryAdapter::new(),
        lock_key,
        hide_lock: Arc::clone(&hide_lock),
    };
    let proof = ParticipantProof::new(
        participant(),
        TransactionTime::new(100, 1),
        request.intent_digest(),
    );
    let mut machine = block_on(ShardStateMachine::open(adapter, 7, 9)).unwrap();
    block_on(machine.apply_entry(
        1,
        1,
        &command(
            3201,
            CommandBodyV1::Prewrite(PrewriteV1 {
                request: request.clone(),
                expected_proof: proof,
            }),
        ),
    ))
    .unwrap();
    hide_lock.store(true, Ordering::SeqCst);
    let finalize = command(
        3202,
        CommandBodyV1::Finalize(FinalizeV1 {
            participant: participant(),
            transaction_id: request.transaction_id(),
            intent_digest: request.intent_digest(),
            commit_ts: TransactionTime::new(101, 0),
        }),
    );

    assert!(matches!(
        block_on(machine.apply_committed_entry(1, 2, &finalize)),
        Err(ShardRuntimeError::Transaction(
            TxnProtocolError::MissingIntentLock { .. }
        ))
    ));
    assert_eq!(machine.metadata().applied_index, 1);
    assert_eq!(machine.adapter().applied_log_index().unwrap(), 1);
    assert!(!block_on(machine.request_replay(3202, &finalize)).unwrap());
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
        Vec::new(),
        metadata(3, 9),
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

struct MissingLockAdapter {
    inner: MemoryAdapter,
    lock_key: LogicalKey,
    hide_lock: Arc<AtomicBool>,
}

impl StorageAdapter for MissingLockAdapter {
    fn capabilities(&self) -> AdapterCapabilities {
        self.inner.capabilities()
    }

    fn apply_committed<'a>(
        &'a self,
        batch: storage_api::CommittedMutationBatch,
    ) -> AdapterFuture<'a, storage_api::ApplyReceipt> {
        self.inner.apply_committed(batch)
    }

    fn multi_get<'a>(&'a self, keys: &'a [LogicalKey]) -> AdapterFuture<'a, Vec<Option<Vec<u8>>>> {
        Box::pin(async move {
            let mut values = self.inner.multi_get(keys).await?;
            if self.hide_lock.load(Ordering::SeqCst) {
                for (key, value) in keys.iter().zip(&mut values) {
                    if key == &self.lock_key {
                        *value = None;
                    }
                }
            }
            Ok(values)
        })
    }

    fn scan<'a>(&'a self, span: &'a KeySpan) -> AdapterFuture<'a, Vec<KeyValue>> {
        self.inner.scan(span)
    }

    fn applied_log_index(&self) -> Result<u64, storage_api::AdapterError> {
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
