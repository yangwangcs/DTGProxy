use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use adapter_registry::{
    AdapterFactory, AdapterFactoryFuture, AdapterOpenRequest, AdapterRegistry, HotSwapAdapter,
};
use raft_command::{
    AbortBackendMigrationV1, ApplyPreparedV1, BeginBackendDualApplyV1, CommandBodyV1,
    CommandEnvelopeV1, CutoverBackendV1,
};
use shard_runtime::{DurableRaftReplica, DurableReplicaError};
use storage_api::{
    AdapterRequirement, Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter,
};
use temporal_types::TransactionTime;

#[test]
fn single_replica_reopens_raft_wal_and_adapter_then_continues_proposing() {
    let root = tempfile::tempdir().unwrap();
    let wal = root.path().join("raft");
    let adapter = root.path().join("adapter");
    let mut replica = block_on(DurableRaftReplica::open(1, &[1], 7, 9, &wal, &adapter)).unwrap();
    block_on(elect_and_drain(&mut replica));
    let first = command(101, 100, b"before-restart");
    replica.propose(101, first).unwrap();
    block_on(drain(&mut replica)).unwrap();
    let first_index = replica.metadata().applied_index;
    assert_eq!(read_current(&replica), Some(b"before-restart".to_vec()));
    drop(replica);

    let mut reopened = block_on(DurableRaftReplica::open(1, &[1], 7, 9, &wal, &adapter)).unwrap();
    assert_eq!(reopened.metadata().applied_index, first_index);
    block_on(elect_and_drain(&mut reopened));
    let second = command(102, 200, b"after-restart");
    reopened.propose(102, second).unwrap();
    block_on(drain(&mut reopened)).unwrap();
    assert_eq!(read_current(&reopened), Some(b"after-restart".to_vec()));
    assert!(reopened.metadata().applied_index > first_index);
}

#[test]
fn committed_wal_entry_left_before_adapter_apply_is_replayed_after_crash() {
    let root = tempfile::tempdir().unwrap();
    let wal = root.path().join("raft");
    let adapter = root.path().join("adapter");
    let mut replica = block_on(DurableRaftReplica::open(1, &[1], 7, 9, &wal, &adapter)).unwrap();
    block_on(elect_and_drain(&mut replica));
    let applied_before = replica.metadata().applied_index;
    replica
        .propose(101, command(101, 100, b"recover-me"))
        .unwrap();
    replica.inject_crash_after_wal_before_apply_once();
    assert!(matches!(
        block_on(drain(&mut replica)),
        Err(DurableReplicaError::InjectedCrashAfterWalBeforeApply)
    ));
    assert_eq!(replica.metadata().applied_index, applied_before);
    drop(replica);

    let mut reopened = block_on(DurableRaftReplica::open(1, &[1], 7, 9, &wal, &adapter)).unwrap();
    block_on(drain(&mut reopened)).unwrap();
    assert_eq!(read_current(&reopened), Some(b"recover-me".to_vec()));
    assert!(reopened.metadata().applied_index > applied_before);
}

#[test]
fn durable_replica_accepts_a_recovered_hot_swap_slot() {
    let root = tempfile::tempdir().unwrap();
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(MemoryFactory)).unwrap();
    let opened = block_on(registry.open(
        "memory",
        &AdapterOpenRequest::new("injected-shard-7"),
        AdapterRequirement::Development,
    ))
    .unwrap();
    let slot = Arc::new(HotSwapAdapter::recover_active(opened, 7).unwrap());

    let mut replica = block_on(DurableRaftReplica::open_with_adapter_slot(
        1,
        &[1],
        7,
        9,
        root.path().join("raft"),
        Arc::clone(&slot),
    ))
    .unwrap();
    block_on(elect_and_drain(&mut replica));
    replica
        .propose(701, command(701, 700, b"injected-adapter"))
        .unwrap();
    block_on(drain(&mut replica)).unwrap();

    assert_eq!(replica.backend_slot().generation(), 7);
    assert_eq!(read_current(&replica), Some(b"injected-adapter".to_vec()));
    assert_eq!(
        slot.applied_log_index().unwrap(),
        replica.metadata().applied_index
    );
}

#[test]
fn durable_replica_dual_applies_and_cuts_over_at_replicated_log_entries() {
    let root = tempfile::tempdir().unwrap();
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(MemoryFactory)).unwrap();
    let source = block_on(registry.open(
        "memory",
        &AdapterOpenRequest::new("source-generation-7"),
        AdapterRequirement::Development,
    ))
    .unwrap();
    let target = block_on(registry.open(
        "memory",
        &AdapterOpenRequest::new("target-generation-8"),
        AdapterRequirement::Development,
    ))
    .unwrap();
    let target_probe = target.clone();
    let slot = Arc::new(HotSwapAdapter::recover_active(source, 7).unwrap());
    slot.start_migration(target, AdapterRequirement::Development)
        .unwrap();
    let digest = [0x33; 32];
    let mut replica = block_on(DurableRaftReplica::open_with_adapter_slot(
        1,
        &[1],
        7,
        9,
        root.path().join("raft"),
        Arc::clone(&slot),
    ))
    .unwrap();
    block_on(elect_and_drain(&mut replica));
    let fence = replica.metadata().applied_index;
    let begin = CommandEnvelopeV1::new(
        7,
        9,
        710,
        CommandBodyV1::BeginBackendDualApply(BeginBackendDualApplyV1 {
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: digest,
            fence_index: fence,
        }),
    )
    .encode()
    .unwrap();
    replica.propose(710, begin).unwrap();
    block_on(drain(&mut replica)).unwrap();
    replica
        .propose(711, command(711, 710, b"dual-applied"))
        .unwrap();
    block_on(drain(&mut replica)).unwrap();
    assert_eq!(
        target_probe.adapter().applied_log_index().unwrap(),
        replica.metadata().applied_index
    );

    let cutover = CommandEnvelopeV1::new(
        7,
        9,
        712,
        CommandBodyV1::CutoverBackend(CutoverBackendV1 {
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: digest,
        }),
    )
    .encode()
    .unwrap();
    replica.propose(712, cutover).unwrap();
    block_on(drain(&mut replica)).unwrap();

    assert_eq!(slot.generation(), 8);
    assert_eq!(replica.metadata().backend_generation, 8);
    assert_eq!(read_current(&replica), Some(b"dual-applied".to_vec()));
}

#[test]
fn durable_replica_aborts_a_prepared_target_before_begin_is_replicated() {
    let root = tempfile::tempdir().unwrap();
    let mut registry = AdapterRegistry::new();
    registry.register(Arc::new(MemoryFactory)).unwrap();
    let source = block_on(registry.open(
        "memory",
        &AdapterOpenRequest::new("source-generation-7"),
        AdapterRequirement::Development,
    ))
    .unwrap();
    let target = block_on(registry.open(
        "memory",
        &AdapterOpenRequest::new("prepared-generation-8"),
        AdapterRequirement::Development,
    ))
    .unwrap();
    let slot = Arc::new(HotSwapAdapter::recover_active(source, 7).unwrap());
    slot.start_migration(target, AdapterRequirement::Development)
        .unwrap();
    let mut replica = block_on(DurableRaftReplica::open_with_adapter_slot(
        1,
        &[1],
        7,
        9,
        root.path().join("raft"),
        Arc::clone(&slot),
    ))
    .unwrap();
    block_on(elect_and_drain(&mut replica));
    let abort = CommandEnvelopeV1::new(
        7,
        9,
        720,
        CommandBodyV1::AbortBackendMigration(AbortBackendMigrationV1 {
            source_generation: 7,
            target_generation: 8,
            target_profile_digest: [0x44; 32],
        }),
    )
    .encode()
    .unwrap();
    replica.propose(720, abort).unwrap();
    block_on(drain(&mut replica)).unwrap();

    assert_eq!(slot.generation(), 7);
    assert!(matches!(
        slot.migration_status(),
        adapter_registry::MigrationStatus::Idle { generation: 7 }
    ));
    assert_eq!(replica.metadata().backend_generation, 7);
}

#[test]
fn rejected_committed_entry_does_not_block_the_following_entry_in_a_ready_batch() {
    let root = tempfile::tempdir().unwrap();
    let mut replica = block_on(DurableRaftReplica::open(
        1,
        &[1],
        7,
        9,
        root.path().join("raft"),
        root.path().join("adapter"),
    ))
    .unwrap();
    block_on(elect_and_drain(&mut replica));
    let before = replica.metadata().applied_index;
    let rejected = CommandEnvelopeV1::new(
        7,
        9,
        730,
        CommandBodyV1::CutoverBackend(CutoverBackendV1 {
            source_generation: 2,
            target_generation: 3,
            target_profile_digest: [0x45; 32],
        }),
    )
    .encode()
    .unwrap();

    replica.propose(730, rejected).unwrap();
    replica
        .propose(731, command(731, 730, b"after-rejection"))
        .unwrap();
    block_on(drain(&mut replica)).unwrap();

    assert_eq!(replica.metadata().applied_index, before + 2);
    assert_eq!(read_current(&replica), Some(b"after-rejection".to_vec()));
    assert_eq!(replica.backend_slot().generation(), 1);
}

#[test]
fn propose_rejects_request_id_that_differs_from_the_command_envelope() {
    let root = tempfile::tempdir().unwrap();
    let mut replica = block_on(DurableRaftReplica::open(
        1,
        &[1],
        7,
        9,
        root.path().join("raft"),
        root.path().join("adapter"),
    ))
    .unwrap();

    assert!(matches!(
        replica.propose(999, command(731, 100, b"not-proposed")),
        Err(DurableReplicaError::RequestEnvelopeMismatch {
            expected: 999,
            actual: 731,
        })
    ));
    assert_eq!(replica.commit_index(), 0);
    assert_eq!(replica.metadata().applied_index, 0);
}

#[test]
fn single_replica_read_index_barrier_returns_a_nonzero_applied_index() {
    let root = tempfile::tempdir().unwrap();
    let mut replica = block_on(DurableRaftReplica::open(
        1,
        &[1],
        7,
        9,
        root.path().join("raft"),
        root.path().join("adapter"),
    ))
    .unwrap();
    block_on(elect_and_drain(&mut replica));
    let context = b"durable-read-index-1".to_vec();

    replica.request_read_index(context.clone()).unwrap();
    block_on(drain(&mut replica)).unwrap();

    let (completed_context, read_index, leader_id, term) =
        replica.take_completed_read_state().unwrap();
    assert_eq!(completed_context, context);
    assert_ne!(read_index, 0);
    assert_eq!(leader_id, 1);
    assert_eq!(term, replica.current_term());
    assert!(replica.metadata().applied_index >= read_index);
    assert!(replica.take_completed_read_state().is_none());
}

#[test]
fn canceled_read_index_retains_its_context_until_the_late_ready_state_is_consumed() {
    let root = tempfile::tempdir().unwrap();
    let mut replica = block_on(DurableRaftReplica::open(
        1,
        &[1],
        7,
        9,
        root.path().join("raft"),
        root.path().join("adapter"),
    ))
    .unwrap();
    block_on(elect_and_drain(&mut replica));
    let canceled = b"canceled-read-index".to_vec();

    replica.request_read_index(canceled.clone()).unwrap();
    replica.cancel_read_index(&canceled);
    assert!(matches!(
        replica.request_read_index(canceled.clone()),
        Err(DurableReplicaError::DuplicateReadIndexContext)
    ));
    let replacement = b"replacement-read-index".to_vec();
    replica.request_read_index(replacement.clone()).unwrap();
    block_on(drain(&mut replica)).unwrap();
    let (context, read_index, _, _) = replica.take_completed_read_state().unwrap();
    assert_eq!(context, replacement);
    assert_ne!(read_index, 0);
    assert!(replica.take_completed_read_state().is_none());

    replica.request_read_index(canceled.clone()).unwrap();
    block_on(drain(&mut replica)).unwrap();
    let (context, read_index, _, _) = replica.take_completed_read_state().unwrap();
    assert_eq!(context, canceled);
    assert_ne!(read_index, 0);
}

struct MemoryFactory;

impl AdapterFactory for MemoryFactory {
    fn provider_name(&self) -> &str {
        "memory"
    }

    fn open<'a>(&'a self, _request: &'a AdapterOpenRequest) -> AdapterFactoryFuture<'a> {
        Box::pin(async { Ok(Arc::new(MemoryAdapter::new()) as Arc<dyn StorageAdapter>) })
    }
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

async fn drain(replica: &mut DurableRaftReplica) -> Result<(), DurableReplicaError> {
    for _ in 0..100 {
        if !replica.has_ready() {
            return Ok(());
        }
        let messages = replica.process_ready().await?;
        assert!(messages.is_empty());
    }
    panic!("Ready loop did not quiesce");
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

fn read_current(replica: &DurableRaftReplica) -> Option<Vec<u8>> {
    let key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    block_on(replica.adapter().multi_get(&[key]))
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
