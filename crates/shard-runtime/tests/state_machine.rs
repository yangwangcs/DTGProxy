use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use shard_runtime::{MIN_REPLICA_TIME, ShardRuntimeError, ShardStateMachine};
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
