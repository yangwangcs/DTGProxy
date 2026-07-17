use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_rocksdb::RocksAdapter;
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use replica_snapshot::{
    SnapshotError, SnapshotManifestV1, create_rocks_checkpoint, open_verified_checkpoint,
};
use shard_runtime::ShardStateMachine;
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter};
use temporal_types::TransactionTime;

#[test]
fn checkpoint_manifest_round_trips_and_rejects_corruption() {
    let manifest = SnapshotManifestV1 {
        shard_id: 7,
        placement_epoch: 9,
        term: 11,
        applied_index: 13,
        closed_ts: TransactionTime::new(17, 1),
        resolved_ts: TransactionTime::new(19, 2),
        adapter_applied_ts: TransactionTime::new(23, 3),
        voters: vec![1, 2, 3],
        checkpoint_digest: [29; 32],
    };
    let encoded = manifest.encode().unwrap();
    assert_eq!(SnapshotManifestV1::decode(&encoded).unwrap(), manifest);

    let mut corrupted = encoded;
    corrupted[20] ^= 1;
    assert_eq!(
        SnapshotManifestV1::decode(&corrupted),
        Err(SnapshotError::ManifestChecksumMismatch)
    );
}

#[test]
fn verified_rocks_checkpoint_restores_matching_state_then_replays_log_suffix() {
    let source_dir = tempfile::tempdir().unwrap();
    let checkpoint_parent = tempfile::tempdir().unwrap();
    let checkpoint_dir = checkpoint_parent.path().join("checkpoint-3");
    let mut source = block_on(ShardStateMachine::open(
        RocksAdapter::open(source_dir.path()).unwrap(),
        7,
        9,
    ))
    .unwrap();
    block_on(source.apply_noop_entry(1, 1)).unwrap();
    let first = apply_command(101, 100, b"at-checkpoint");
    block_on(source.apply_entry(1, 2, &first)).unwrap();
    let tick = tick_command(102, 100);
    block_on(source.apply_entry(1, 3, &tick)).unwrap();

    let manifest = create_rocks_checkpoint(&source, &[1, 2, 3], &checkpoint_dir).unwrap();
    assert_eq!(manifest.applied_index, 3);
    assert_eq!(manifest.term, 1);

    let suffix = apply_command(103, 200, b"after-checkpoint");
    block_on(source.apply_entry(2, 4, &suffix)).unwrap();
    let tick_2 = tick_command(104, 200);
    block_on(source.apply_entry(2, 5, &tick_2)).unwrap();

    let restored_adapter = open_verified_checkpoint(&checkpoint_dir, &manifest).unwrap();
    let mut restored = block_on(ShardStateMachine::open(restored_adapter, 7, 9)).unwrap();
    assert_eq!(restored.metadata().applied_index, 3);
    block_on(restored.apply_entry(2, 4, &suffix)).unwrap();
    block_on(restored.apply_entry(2, 5, &tick_2)).unwrap();

    assert_eq!(restored.metadata(), source.metadata());
    assert_eq!(read_current(&restored), read_current(&source));
}

#[test]
fn modified_checkpoint_is_rejected_before_rocksdb_is_opened() {
    let source_dir = tempfile::tempdir().unwrap();
    let checkpoint_parent = tempfile::tempdir().unwrap();
    let checkpoint_dir = checkpoint_parent.path().join("checkpoint");
    let mut source = block_on(ShardStateMachine::open(
        RocksAdapter::open(source_dir.path()).unwrap(),
        7,
        9,
    ))
    .unwrap();
    block_on(source.apply_noop_entry(1, 1)).unwrap();
    let manifest = create_rocks_checkpoint(&source, &[1, 2, 3], &checkpoint_dir).unwrap();

    let current_file = std::fs::read_dir(&checkpoint_dir)
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_type().unwrap().is_file())
        .unwrap()
        .path();
    let mut contents = std::fs::read(&current_file).unwrap();
    contents.push(0);
    std::fs::write(current_file, contents).unwrap();

    assert!(matches!(
        open_verified_checkpoint(&checkpoint_dir, &manifest),
        Err(SnapshotError::CheckpointDigestMismatch)
    ));
}

fn apply_command(request_id: u128, commit: i64, value: &[u8]) -> Vec<u8> {
    CommandEnvelopeV1::new(
        7,
        9,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(commit, 0),
            batch: PreparedMutationBatch {
                shard_id: 7,
                txn_id: request_id + 1_000,
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

fn tick_command(request_id: u128, closed: i64) -> Vec<u8> {
    CommandEnvelopeV1::new(
        7,
        9,
        request_id,
        CommandBodyV1::ClosedTimestampTick(TransactionTime::new(closed, 0)),
    )
    .encode()
    .unwrap()
}

fn read_current(machine: &ShardStateMachine<RocksAdapter>) -> Option<Vec<u8>> {
    let key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    block_on(machine.adapter().multi_get(&[key]))
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
