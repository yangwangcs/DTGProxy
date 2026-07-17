use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_rocksdb::RocksAdapter;
use raft::Storage;
use raft::eraftpb::{Entry, EntryType, HardState};
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use raft_logstore::RocksRaftStorage;
use replica_snapshot::{
    SnapshotError, SnapshotFailpoint, activate_published_local_snapshot,
    create_and_activate_local_snapshot_with_failpoint, create_snapshot_bundle,
    create_snapshot_bundle_with_failpoint, install_received_snapshot_bundle,
    install_snapshot_bundle, install_snapshot_bundle_with_failpoint, open_snapshot_bundle,
    raft_snapshot,
};
use shard_runtime::{DurableRaftReplica, DurableReplicaError, ShardStateMachine};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter};
use temporal_types::TransactionTime;

#[test]
fn published_bundle_installs_raft_and_adapter_then_replays_committed_suffix() {
    let root = tempfile::tempdir().unwrap();
    let source_path = root.path().join("source");
    let bundle_path = root.path().join("snapshot-3");
    let installed_path = root.path().join("replica-2-generation-1");
    let mut source = block_on(ShardStateMachine::open(
        RocksAdapter::open(&source_path).unwrap(),
        7,
        9,
    ))
    .unwrap();
    block_on(source.apply_noop_entry(1, 1)).unwrap();
    block_on(source.apply_entry(1, 2, &command(101, 100, b"at-snapshot"))).unwrap();
    block_on(source.apply_entry(1, 3, &tick(102, 100))).unwrap();

    let manifest = create_snapshot_bundle(&source, &[1, 2, 3], &bundle_path).unwrap();
    assert_eq!(open_snapshot_bundle(&bundle_path).unwrap(), manifest);

    let suffix = command(103, 200, b"after-snapshot");
    let tick = tick(104, 200);
    block_on(source.apply_entry(2, 4, &suffix)).unwrap();
    block_on(source.apply_entry(2, 5, &tick)).unwrap();

    let incoming_snapshot = raft_snapshot(&manifest).unwrap();
    let installed = block_on(install_received_snapshot_bundle(
        &incoming_snapshot,
        &bundle_path,
        &installed_path,
    ))
    .unwrap();
    let storage = RocksRaftStorage::open(&installed.raft_wal_path, &[1, 2, 3]).unwrap();
    storage
        .persist_ready(
            None,
            &[entry(2, 4, suffix), entry(2, 5, tick)],
            Some(&HardState {
                term: 2,
                commit: 5,
                ..Default::default()
            }),
        )
        .unwrap();
    drop(storage);

    let mut restored = block_on(DurableRaftReplica::open(
        2,
        &[1, 2, 3],
        7,
        9,
        &installed.raft_wal_path,
        &installed.adapter_path,
    ))
    .unwrap();
    block_on(drain(&mut restored)).unwrap();

    assert_eq!(restored.metadata(), source.metadata());
    assert_eq!(
        read_current(restored.adapter()),
        read_current(source.adapter())
    );
}

#[test]
fn incoming_raft_snapshot_must_exactly_match_the_checkpoint_manifest() {
    let root = tempfile::tempdir().unwrap();
    let source_path = root.path().join("source");
    let bundle_path = root.path().join("bundle");
    let destination = root.path().join("installed");
    let mut source = block_on(ShardStateMachine::open(
        RocksAdapter::open(&source_path).unwrap(),
        7,
        9,
    ))
    .unwrap();
    block_on(source.apply_noop_entry(1, 1)).unwrap();
    let manifest = create_snapshot_bundle(&source, &[1, 2, 3], &bundle_path).unwrap();
    let mut incoming = raft_snapshot(&manifest).unwrap();
    incoming.metadata.as_mut().unwrap().term += 1;

    assert!(matches!(
        block_on(install_received_snapshot_bundle(
            &incoming,
            &bundle_path,
            &destination,
        )),
        Err(SnapshotError::IncomingRaftSnapshotMismatch)
    ));
    assert!(!destination.exists());
}

#[test]
fn unpublished_checkpoint_and_install_generations_are_never_visible() {
    let root = tempfile::tempdir().unwrap();
    let source_path = root.path().join("source");
    let mut source = block_on(ShardStateMachine::open(
        RocksAdapter::open(&source_path).unwrap(),
        7,
        9,
    ))
    .unwrap();
    block_on(source.apply_noop_entry(1, 1)).unwrap();

    let bundle_path = root.path().join("bundle");
    assert!(matches!(
        create_snapshot_bundle_with_failpoint(
            &source,
            &[1, 2, 3],
            &bundle_path,
            SnapshotFailpoint::AfterCheckpointBeforeManifest,
        ),
        Err(SnapshotError::InjectedFailure(
            SnapshotFailpoint::AfterCheckpointBeforeManifest
        ))
    ));
    assert!(!bundle_path.exists());
    create_snapshot_bundle(&source, &[1, 2, 3], &bundle_path).unwrap();

    let installed_path = root.path().join("installed");
    assert!(matches!(
        block_on(install_snapshot_bundle_with_failpoint(
            &bundle_path,
            &installed_path,
            SnapshotFailpoint::AfterSnapshotPersistBeforePublish,
        )),
        Err(SnapshotError::InjectedFailure(
            SnapshotFailpoint::AfterSnapshotPersistBeforePublish
        ))
    ));
    assert!(!installed_path.exists());
    let installed = block_on(install_snapshot_bundle(&bundle_path, &installed_path)).unwrap();
    assert!(installed.adapter_path.is_dir());
    assert!(installed.raft_wal_path.is_dir());
}

#[test]
fn every_publication_crash_boundary_is_fail_closed_or_fully_visible() {
    let root = tempfile::tempdir().unwrap();
    let source_path = root.path().join("source");
    let mut source = block_on(ShardStateMachine::open(
        RocksAdapter::open(&source_path).unwrap(),
        7,
        9,
    ))
    .unwrap();
    block_on(source.apply_noop_entry(1, 1)).unwrap();

    for (ordinal, failpoint) in [
        SnapshotFailpoint::BeforeCheckpoint,
        SnapshotFailpoint::AfterCheckpointBeforeManifest,
        SnapshotFailpoint::AfterManifestSyncBeforePublish,
    ]
    .into_iter()
    .enumerate()
    {
        let destination = root.path().join(format!("unpublished-bundle-{ordinal}"));
        assert!(matches!(
            create_snapshot_bundle_with_failpoint(
                &source,
                &[1, 2, 3],
                &destination,
                failpoint,
            ),
            Err(SnapshotError::InjectedFailure(actual)) if actual == failpoint
        ));
        assert!(!destination.exists());
    }

    let published_after_error = root.path().join("published-bundle");
    assert!(matches!(
        create_snapshot_bundle_with_failpoint(
            &source,
            &[1, 2, 3],
            &published_after_error,
            SnapshotFailpoint::AfterPublish,
        ),
        Err(SnapshotError::InjectedFailure(
            SnapshotFailpoint::AfterPublish
        ))
    ));
    open_snapshot_bundle(&published_after_error).unwrap();

    for (ordinal, failpoint) in [
        SnapshotFailpoint::AfterAdapterCopyBeforeSnapshotPersist,
        SnapshotFailpoint::AfterSnapshotPersistBeforePublish,
    ]
    .into_iter()
    .enumerate()
    {
        let destination = root.path().join(format!("unpublished-install-{ordinal}"));
        assert!(matches!(
            block_on(install_snapshot_bundle_with_failpoint(
                &published_after_error,
                &destination,
                failpoint,
            )),
            Err(SnapshotError::InjectedFailure(actual)) if actual == failpoint
        ));
        assert!(!destination.exists());
    }

    let installed_after_error = root.path().join("published-install");
    assert!(matches!(
        block_on(install_snapshot_bundle_with_failpoint(
            &published_after_error,
            &installed_after_error,
            SnapshotFailpoint::AfterPublish,
        )),
        Err(SnapshotError::InjectedFailure(
            SnapshotFailpoint::AfterPublish
        ))
    ));
    assert!(installed_after_error.join("adapter").is_dir());
    assert!(installed_after_error.join("raft").is_dir());
}

#[test]
fn source_wal_compacts_only_after_published_bundle_and_restart_continues() {
    let root = tempfile::tempdir().unwrap();
    let wal = root.path().join("raft");
    let adapter = root.path().join("adapter");
    let bundle = root.path().join("bundle");
    let mut replica = block_on(DurableRaftReplica::open(1, &[1], 7, 9, &wal, &adapter)).unwrap();
    block_on(elect_and_drain(&mut replica));
    replica
        .propose(101, command(101, 100, b"before-snapshot"))
        .unwrap();
    block_on(drain(&mut replica)).unwrap();
    let snapshot_index = replica.metadata().applied_index;

    assert!(matches!(
        create_and_activate_local_snapshot_with_failpoint(
            replica.state_machine(),
            replica.raft_storage(),
            &[1],
            &bundle,
            SnapshotFailpoint::AfterBundlePublishBeforeWalSnapshot,
        ),
        Err(SnapshotError::InjectedFailure(
            SnapshotFailpoint::AfterBundlePublishBeforeWalSnapshot
        ))
    ));
    assert!(bundle.is_dir());
    assert_eq!(replica.raft_storage().first_index().unwrap(), 1);

    activate_published_local_snapshot(replica.raft_storage(), &bundle).unwrap();
    activate_published_local_snapshot(replica.raft_storage(), &bundle).unwrap();
    assert_eq!(
        replica.raft_storage().first_index().unwrap(),
        snapshot_index + 1
    );
    drop(replica);

    let mut reopened = block_on(DurableRaftReplica::open(1, &[1], 7, 9, &wal, &adapter)).unwrap();
    block_on(elect_and_drain(&mut reopened));
    reopened
        .propose(102, command(102, 200, b"after-snapshot"))
        .unwrap();
    block_on(drain(&mut reopened)).unwrap();
    assert_eq!(
        read_current(reopened.adapter()),
        Some(b"after-snapshot".to_vec())
    );
}

#[test]
fn crash_after_source_wal_snapshot_is_idempotently_recoverable() {
    let root = tempfile::tempdir().unwrap();
    let wal = root.path().join("raft");
    let adapter = root.path().join("adapter");
    let bundle = root.path().join("bundle");
    let mut replica = block_on(DurableRaftReplica::open(1, &[1], 7, 9, &wal, &adapter)).unwrap();
    block_on(elect_and_drain(&mut replica));
    replica
        .propose(101, command(101, 100, b"snapshotted"))
        .unwrap();
    block_on(drain(&mut replica)).unwrap();
    let snapshot_index = replica.metadata().applied_index;

    assert!(matches!(
        replica_snapshot::create_and_activate_local_snapshot_with_failpoint(
            replica.state_machine(),
            replica.raft_storage(),
            &[1],
            &bundle,
            SnapshotFailpoint::AfterWalSnapshotPersist,
        ),
        Err(SnapshotError::InjectedFailure(
            SnapshotFailpoint::AfterWalSnapshotPersist
        ))
    ));
    assert_eq!(
        replica.raft_storage().first_index().unwrap(),
        snapshot_index + 1
    );
    activate_published_local_snapshot(replica.raft_storage(), &bundle).unwrap();
    drop(replica);

    let reopened = block_on(DurableRaftReplica::open(1, &[1], 7, 9, &wal, &adapter)).unwrap();
    assert_eq!(reopened.metadata().applied_index, snapshot_index);
    assert_eq!(
        read_current(reopened.adapter()),
        Some(b"snapshotted".to_vec())
    );
}

fn entry(term: u64, index: u64, data: Vec<u8>) -> Entry {
    Entry {
        entry_type: EntryType::EntryNormal.into(),
        term,
        index,
        data,
        ..Default::default()
    }
}

async fn drain(replica: &mut DurableRaftReplica) -> Result<(), DurableReplicaError> {
    for _ in 0..100 {
        if !replica.has_ready() {
            return Ok(());
        }
        let _messages = replica.process_ready().await?;
    }
    panic!("Ready loop did not quiesce");
}

async fn elect_and_drain(replica: &mut DurableRaftReplica) {
    replica.campaign().unwrap();
    for _ in 0..20 {
        drain(replica).await.unwrap();
        if replica.is_leader() {
            return;
        }
        replica.tick();
    }
    panic!("single-node Replica did not elect itself");
}

fn command(request_id: u128, commit: i64, value: &[u8]) -> Vec<u8> {
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

fn tick(request_id: u128, closed: i64) -> Vec<u8> {
    CommandEnvelopeV1::new(
        7,
        9,
        request_id,
        CommandBodyV1::ClosedTimestampTick(TransactionTime::new(closed, 0)),
    )
    .encode()
    .unwrap()
}

fn read_current(adapter: &RocksAdapter) -> Option<Vec<u8>> {
    let key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    block_on(adapter.multi_get(&[key])).unwrap().pop().flatten()
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
