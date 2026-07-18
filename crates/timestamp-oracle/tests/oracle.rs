use std::collections::BTreeSet;
use std::fs;
use std::sync::Arc;
use std::thread;

use temporal_types::TransactionTime;
use timestamp_oracle::{
    FileTimestampStore, ManualClock, MemoryTimestampStore, TimestampOracle, TimestampOracleError,
};

#[test]
fn logical_component_preserves_monotonicity_when_the_physical_clock_regresses() {
    let clock = Arc::new(ManualClock::new(1_000));
    let store = Arc::new(MemoryTimestampStore::new());
    let oracle = TimestampOracle::open(store, clock.clone(), 8).unwrap();

    assert_eq!(oracle.next().unwrap(), TransactionTime::new(1_000, 0));
    clock.set(900);
    assert_eq!(oracle.next().unwrap(), TransactionTime::new(1_000, 1));
    clock.set(1_001);
    assert_eq!(oracle.next().unwrap(), TransactionTime::new(1_001, 0));
    assert_eq!(
        oracle.closed_timestamp().unwrap(),
        TransactionTime::new(1_001, 0)
    );
}

#[test]
fn next_after_and_snapshot_tokens_are_strictly_fenced() {
    let clock = Arc::new(ManualClock::new(10));
    let oracle = TimestampOracle::open(Arc::new(MemoryTimestampStore::new()), clock, 4).unwrap();

    let fenced = oracle.next_after(TransactionTime::new(99, 7)).unwrap();
    assert_eq!(fenced, TransactionTime::new(99, 8));
    let snapshot = oracle.snapshot().unwrap();
    assert_eq!(snapshot.transaction_time(), TransactionTime::new(99, 9));
    assert!(snapshot.transaction_time() > fenced);
}

#[test]
fn concurrent_callers_receive_unique_globally_ordered_timestamps() {
    let oracle = Arc::new(
        TimestampOracle::open(
            Arc::new(MemoryTimestampStore::new()),
            Arc::new(ManualClock::new(1_234)),
            64,
        )
        .unwrap(),
    );
    let workers = (0..8)
        .map(|_| {
            let oracle = Arc::clone(&oracle);
            thread::spawn(move || (0..500).map(|_| oracle.next().unwrap()).collect::<Vec<_>>())
        })
        .collect::<Vec<_>>();
    let timestamps = workers
        .into_iter()
        .flat_map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    let unique = timestamps.iter().copied().collect::<BTreeSet<_>>();

    assert_eq!(timestamps.len(), 4_000);
    assert_eq!(unique.len(), timestamps.len());
}

#[test]
fn restart_skips_every_timestamp_in_the_previously_reserved_block() {
    let directory = tempfile::tempdir().unwrap();
    let state_path = directory.path().join("tso.state");
    let clock = Arc::new(ManualClock::new(500));
    let first = TimestampOracle::open(
        Arc::new(FileTimestampStore::new(&state_path)),
        clock.clone(),
        4,
    )
    .unwrap();
    assert_eq!(first.next().unwrap(), TransactionTime::new(500, 0));
    drop(first);

    let reopened =
        TimestampOracle::open(Arc::new(FileTimestampStore::new(&state_path)), clock, 4).unwrap();
    assert_eq!(reopened.next().unwrap(), TransactionTime::new(500, 4));
}

#[test]
fn corrupt_persistent_state_is_rejected_instead_of_reset() {
    let directory = tempfile::tempdir().unwrap();
    let state_path = directory.path().join("tso.state");
    fs::write(&state_path, b"not-a-timestamp-oracle-record").unwrap();

    let error = match TimestampOracle::open(
        Arc::new(FileTimestampStore::new(&state_path)),
        Arc::new(ManualClock::new(1)),
        4,
    ) {
        Ok(_) => panic!("corrupt TSO state was accepted"),
        Err(error) => error,
    };
    assert!(matches!(error, TimestampOracleError::CorruptState));
}
