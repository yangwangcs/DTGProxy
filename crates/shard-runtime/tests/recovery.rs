use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_rocksdb::RocksAdapter;
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use shard_runtime::ShardStateMachine;
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter};
use temporal_types::TransactionTime;

#[test]
fn memory_adapter_recovers_durable_replica_metadata() {
    let adapter = MemoryAdapter::new();
    let adapter = exercise_and_release(adapter);
    let recovered = block_on(ShardStateMachine::open(adapter, 7, 9)).unwrap();
    assert_recovered(&recovered);
}

#[test]
fn rocksdb_adapter_recovers_replica_metadata_and_business_state_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let adapter = RocksAdapter::open(directory.path()).unwrap();
    drop(exercise_and_release(adapter));

    let recovered = block_on(ShardStateMachine::open(
        RocksAdapter::open(directory.path()).unwrap(),
        7,
        9,
    ))
    .unwrap();
    assert_recovered(&recovered);
}

fn exercise_and_release<A: StorageAdapter>(adapter: A) -> A {
    let mut machine = block_on(ShardStateMachine::open(adapter, 7, 9)).unwrap();
    let apply = CommandEnvelopeV1::new(
        7,
        9,
        101,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(100, 0),
            batch: PreparedMutationBatch {
                shard_id: 7,
                txn_id: 500,
                mutations: vec![Mutation::put(
                    0,
                    LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec()),
                    b"durable".to_vec(),
                )],
            },
        }),
    )
    .encode()
    .unwrap();
    let tick = CommandEnvelopeV1::new(
        7,
        9,
        102,
        CommandBodyV1::ClosedTimestampTick(TransactionTime::new(100, 0)),
    )
    .encode()
    .unwrap();
    block_on(machine.apply_entry(3, 1, &apply)).unwrap();
    block_on(machine.apply_entry(3, 2, &tick)).unwrap();
    machine.into_adapter()
}

fn assert_recovered<A: StorageAdapter>(machine: &ShardStateMachine<A>) {
    assert_eq!(machine.metadata().shard_id, 7);
    assert_eq!(machine.metadata().placement_epoch, 9);
    assert_eq!(machine.metadata().last_term, 3);
    assert_eq!(machine.metadata().applied_index, 2);
    assert_eq!(
        machine.servable_safe_ts().unwrap(),
        TransactionTime::new(100, 0)
    );
    let key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    assert_eq!(
        block_on(machine.adapter().multi_get(&[key]))
            .unwrap()
            .pop()
            .flatten(),
        Some(b"durable".to_vec())
    );
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
