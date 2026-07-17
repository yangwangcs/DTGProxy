use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use raft::eraftpb::Message;
use raft_command::{ApplyPreparedV1, CommandBodyV1, CommandEnvelopeV1};
use shard_runtime::{
    DeterministicTransport, InProcessShardGroup, MultiRaftRuntime, ReplicationError,
};
use storage_api::{Keyspace, LogicalKey, Mutation, PreparedMutationBatch, StorageAdapter};
use temporal_types::TransactionTime;

const VOTERS: &[u64] = &[1, 2, 3];

fn apply_command(
    shard_id: u32,
    epoch: u64,
    request_id: u128,
    commit: i64,
    value: &[u8],
) -> Vec<u8> {
    CommandEnvelopeV1::new(
        shard_id,
        epoch,
        request_id,
        CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
            commit_ts: TransactionTime::new(commit, 0),
            batch: PreparedMutationBatch {
                shard_id,
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

#[test]
fn three_replicas_elect_replicate_and_ack_only_after_leader_apply() {
    let mut group = block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap();
    block_on(group.elect(1)).unwrap();
    assert_eq!(group.leader_id(), Some(1));

    let command = apply_command(7, 9, 101, 100, b"v1");
    let receipt = block_on(group.propose_and_wait(command.clone(), 20)).unwrap();
    assert_eq!(receipt.leader_id, 1);
    assert!(
        receipt.index >= 2,
        "leader election must durably apply its no-op"
    );
    for node_id in VOTERS {
        assert_eq!(read_current(&group, *node_id), Some(b"v1".to_vec()));
        assert_eq!(
            group.replica_metadata(*node_id).unwrap().applied_index,
            receipt.index
        );
    }

    assert_eq!(
        block_on(group.propose_and_wait(command, 1)).unwrap(),
        receipt
    );
    assert!(matches!(
        block_on(group.propose_and_wait(apply_command(7, 9, 101, 100, b"different-retry"), 1,)),
        Err(ReplicationError::RequestMismatch { request_id: 101 })
    ));
}

#[test]
fn one_follower_can_stop_restart_and_catch_up_without_shared_replica_state() {
    let mut group = block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap();
    block_on(group.elect(1)).unwrap();
    group.stop_node(3).unwrap();

    let receipt =
        block_on(group.propose_and_wait(apply_command(7, 9, 101, 100, b"while-three-down"), 20))
            .unwrap();
    assert_eq!(read_current(&group, 1), Some(b"while-three-down".to_vec()));
    assert_eq!(read_current(&group, 2), Some(b"while-three-down".to_vec()));
    assert_eq!(read_current(&group, 3), None);

    group.restart_node(3).unwrap();
    block_on(group.drive_ticks(20)).unwrap();
    assert_eq!(read_current(&group, 3), Some(b"while-three-down".to_vec()));
    assert_eq!(
        group.replica_metadata(3).unwrap().applied_index,
        receipt.index
    );
}

#[test]
fn leader_loss_re_election_minority_refusal_and_partition_heal_converge() {
    let mut group = block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap();
    group.transport_mut().set_reverse_ready_order(true);
    block_on(group.elect(1)).unwrap();
    block_on(group.propose_and_wait(apply_command(7, 9, 101, 100, b"before-failover"), 20))
        .unwrap();

    group.stop_node(1).unwrap();
    block_on(group.elect(2)).unwrap();
    let after_failover = apply_command(7, 9, 102, 200, b"after-failover");
    block_on(group.propose_and_wait(after_failover, 20)).unwrap();
    assert_eq!(read_current(&group, 2), Some(b"after-failover".to_vec()));
    assert_eq!(read_current(&group, 3), Some(b"after-failover".to_vec()));

    group.transport_mut().isolate(2);
    let minority = apply_command(7, 9, 103, 300, b"minority");
    assert!(matches!(
        block_on(group.propose_and_wait(minority.clone(), 5)),
        Err(ReplicationError::QuorumUnavailable {
            request_id: 103,
            ..
        })
    ));
    assert_eq!(read_current(&group, 2), Some(b"after-failover".to_vec()));

    group.transport_mut().heal(2);
    block_on(group.propose_and_wait(minority, 30)).unwrap();
    group.restart_node(1).unwrap();
    block_on(group.drive_ticks(30)).unwrap();
    for node_id in VOTERS {
        assert_eq!(read_current(&group, *node_id), Some(b"minority".to_vec()));
    }
}

#[test]
fn two_multi_raft_groups_on_the_same_nodes_progress_independently() {
    let mut runtime = MultiRaftRuntime::new();
    runtime
        .insert_group(block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap())
        .unwrap();
    runtime
        .insert_group(block_on(InProcessShardGroup::new(8, 4, VOTERS)).unwrap())
        .unwrap();
    assert!(runtime.owns(1, 7));
    assert!(runtime.owns(1, 8));

    block_on(runtime.group_mut(7).unwrap().elect(1)).unwrap();
    block_on(runtime.group_mut(8).unwrap().elect(2)).unwrap();
    block_on(
        runtime
            .group_mut(7)
            .unwrap()
            .propose_and_wait(apply_command(7, 9, 701, 100, b"shard-seven"), 20),
    )
    .unwrap();
    block_on(
        runtime
            .group_mut(8)
            .unwrap()
            .propose_and_wait(apply_command(8, 4, 801, 100, b"shard-eight"), 20),
    )
    .unwrap();

    assert_eq!(
        read_current(runtime.group(7).unwrap(), 1),
        Some(b"shard-seven".to_vec())
    );
    assert_eq!(
        read_current(runtime.group(8).unwrap(), 2),
        Some(b"shard-eight".to_vec())
    );
}

#[test]
fn deterministic_transport_controls_drop_delay_isolation_and_reordering() {
    let mut transport = DeterministicTransport::new();
    transport.delay_link(1, 2, 2);
    transport.drop_next(2, 1, 1);
    transport.set_reverse_ready_order(true);

    transport.send(message(1, 2, 1));
    transport.send(message(2, 1, 2));
    transport.send(message(3, 1, 3));
    assert_eq!(
        transport
            .take_ready()
            .into_iter()
            .map(|message| message.index)
            .collect::<Vec<_>>(),
        vec![3]
    );
    transport.advance();
    assert!(transport.take_ready().is_empty());
    transport.advance();
    assert_eq!(transport.take_ready()[0].index, 1);

    transport.isolate(3);
    transport.send(message(3, 1, 4));
    transport.send(message(1, 3, 5));
    assert!(transport.take_ready().is_empty());
    transport.heal(3);
    transport.duplicate_next(3, 1, 1);
    transport.send(message(3, 1, 6));
    assert_eq!(
        transport
            .take_ready()
            .into_iter()
            .map(|message| message.index)
            .collect::<Vec<_>>(),
        vec![6, 6]
    );
}

fn message(from: u64, to: u64, index: u64) -> Message {
    Message {
        from,
        to,
        index,
        ..Default::default()
    }
}

fn read_current(group: &InProcessShardGroup, node_id: u64) -> Option<Vec<u8>> {
    let key = LogicalKey::in_keyspace(Keyspace::Current, b"vertex/1".to_vec());
    block_on(group.replica_adapter(node_id).unwrap().multi_get(&[key]))
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
