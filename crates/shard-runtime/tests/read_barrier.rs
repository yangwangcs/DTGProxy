use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use shard_runtime::{InProcessShardGroup, ReadBarrierError, ReadPermitMode};
use temporal_types::TransactionTime;

const VOTERS: &[u64] = &[1, 2, 3];

#[test]
fn leader_linearizable_permit_requires_current_epoch_leadership_and_quorum_read_index() {
    let mut group = block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap();
    block_on(group.elect(1)).unwrap();

    assert!(matches!(
        block_on(group.leader_read_permit(2, 9, 5)),
        Err(ReadBarrierError::NotLeader {
            leader_hint: Some(1),
            ..
        })
    ));
    assert!(matches!(
        block_on(group.leader_read_permit(1, 8, 5)),
        Err(ReadBarrierError::StaleEpoch {
            expected: 9,
            actual: 8
        })
    ));

    let permit = block_on(group.leader_read_permit(1, 9, 5)).unwrap();
    assert_eq!(permit.node_id(), 1);
    assert_eq!(permit.mode(), ReadPermitMode::LeaderLinearizable);
    assert!(
        group.replica_metadata(1).unwrap().applied_index >= permit.read_index(),
        "ReadIndex is usable only after the leader Adapter applies through it"
    );

    group.transport_mut().isolate(1);
    assert!(matches!(
        block_on(group.leader_read_permit(1, 9, 5)),
        Err(ReadBarrierError::NotReady { .. }) | Err(ReadBarrierError::NotLeader { .. })
    ));
}

#[test]
fn follower_snapshot_permit_requires_safe_time_and_current_read_index_proof() {
    let mut group = block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap();
    block_on(group.elect(1)).unwrap();
    let initial_proof = block_on(group.issue_follower_read_proof(9, 5)).unwrap();
    assert!(matches!(
        group.follower_read_permit(2, 9, ts(100), &initial_proof),
        Err(ReadBarrierError::NotReady { .. })
    ));

    block_on(group.advance_closed_timestamp(ts(100), 20)).unwrap();
    let proof = block_on(group.issue_follower_read_proof(9, 5)).unwrap();
    let permit = group.follower_read_permit(2, 9, ts(100), &proof).unwrap();
    assert_eq!(permit.node_id(), 2);
    assert_eq!(
        permit.mode(),
        ReadPermitMode::FollowerSnapshot { read_ts: ts(100) }
    );
    assert!(matches!(
        group.follower_read_permit(2, 9, ts(101), &proof),
        Err(ReadBarrierError::NotReady { .. })
    ));
    assert!(matches!(
        group.follower_read_permit(2, 8, ts(100), &proof),
        Err(ReadBarrierError::StaleEpoch { .. })
    ));
}

#[test]
fn lagging_follower_is_rejected_until_adapter_apply_catches_the_read_proof() {
    let mut group = block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap();
    block_on(group.elect(1)).unwrap();
    group.transport_mut().isolate(3);
    block_on(group.advance_closed_timestamp(ts(100), 20)).unwrap();
    let proof = block_on(group.issue_follower_read_proof(9, 5)).unwrap();

    assert!(matches!(
        group.follower_read_permit(3, 9, ts(100), &proof),
        Err(ReadBarrierError::AdapterLagging { .. })
    ));
    group.transport_mut().heal(3);
    block_on(group.drive_ticks(20)).unwrap();
    let refreshed = block_on(group.issue_follower_read_proof(9, 5)).unwrap();
    group
        .follower_read_permit(3, 9, ts(100), &refreshed)
        .unwrap();
}

#[test]
fn follower_proof_is_invalidated_by_leadership_term_change() {
    let mut group = block_on(InProcessShardGroup::new(7, 9, VOTERS)).unwrap();
    block_on(group.elect(1)).unwrap();
    block_on(group.advance_closed_timestamp(ts(100), 20)).unwrap();
    let old_proof = block_on(group.issue_follower_read_proof(9, 5)).unwrap();

    group.stop_node(1).unwrap();
    block_on(group.elect(2)).unwrap();
    assert!(matches!(
        group.follower_read_permit(3, 9, ts(100), &old_proof),
        Err(ReadBarrierError::NotReady { .. })
    ));
}

fn ts(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
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
