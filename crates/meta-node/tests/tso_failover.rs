use std::collections::BTreeSet;
use std::sync::Arc;

use meta_node::{MetaStateMachine, ReplicatedTso, TsoError};
use temporal_types::TransactionTime;
use timestamp_oracle::ManualClock;

const MIN_TIME: TransactionTime = TransactionTime::new(i64::MIN, 0);

#[test]
fn leader_crash_after_commit_before_return_abandons_but_never_reuses_lease() {
    let clock = Arc::new(ManualClock::new(1_000));
    let first_leader = ReplicatedTso::new(clock.clone(), 8, 1_000_000).unwrap();
    let mut meta = MetaStateMachine::new(16).unwrap();
    let abandoned = first_leader
        .plan_reservation(meta.timestamp_high_water(), 101, 1)
        .unwrap();
    meta.apply_committed(1, 1, &abandoned.encode()).unwrap();
    assert_eq!(meta.timestamp_high_water(), abandoned.new_high_water());
    drop(first_leader);

    clock.set(900);
    let restored = MetaStateMachine::decode_snapshot(&meta.encode_snapshot().unwrap(), 16).unwrap();
    let replacement = ReplicatedTso::new(clock, 8, 1_000_000).unwrap();
    let next = replacement
        .plan_reservation(restored.timestamp_high_water(), 102, 3)
        .unwrap();
    assert!(next.first() > abandoned.new_high_water());
    let mut restored = restored;
    restored.apply_committed(2, 2, &next.encode()).unwrap();
    replacement
        .activate_committed(&next, restored.timestamp_high_water())
        .unwrap();
    let issued = replacement.allocate(3).unwrap();
    assert_eq!(issued.first(), next.first());
    assert!(issued.last().unwrap() <= next.new_high_water());
}

#[test]
fn committed_reservation_replay_is_idempotent_and_conflicts_fail_closed() {
    let clock = Arc::new(ManualClock::new(10));
    let tso = ReplicatedTso::new(clock, 4, 1_000).unwrap();
    let mut meta = MetaStateMachine::new(16).unwrap();
    let command = tso.plan_reservation(MIN_TIME, 201, 1).unwrap();
    let first = meta.apply_committed(1, 1, &command.encode()).unwrap();
    assert!(!first.duplicate());
    let replay = meta.apply_committed(2, 2, &command.encode()).unwrap();
    assert!(replay.duplicate());

    let conflicting = meta_node::ReserveTimestampCommand::new(
        201,
        MIN_TIME,
        TransactionTime::new(20, 0),
        TransactionTime::new(20, 3),
    )
    .unwrap();
    assert!(meta.apply_committed(2, 3, &conflicting.encode()).is_err());
    assert_eq!(meta.applied_index(), 2);
}

#[test]
fn concurrent_batches_are_disjoint_and_lease_exhaustion_is_explicit() {
    let clock = Arc::new(ManualClock::new(500));
    let tso = Arc::new(ReplicatedTso::new(clock, 64, 1_000).unwrap());
    let command = tso.plan_reservation(MIN_TIME, 301, 64).unwrap();
    tso.activate_committed(&command, command.new_high_water())
        .unwrap();
    let workers = (0..8)
        .map(|_| {
            let tso = Arc::clone(&tso);
            std::thread::spawn(move || {
                (0..8)
                    .map(|_| tso.allocate(1).unwrap().first())
                    .collect::<Vec<_>>()
            })
        })
        .collect::<Vec<_>>();
    let issued = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(issued.len(), 64);
    assert_eq!(issued.iter().copied().collect::<BTreeSet<_>>().len(), 64);
    assert_eq!(tso.allocate(1).unwrap_err(), TsoError::LeaseExhausted);
}
