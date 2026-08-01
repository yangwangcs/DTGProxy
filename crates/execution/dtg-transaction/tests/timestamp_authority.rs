use std::sync::{Arc, Mutex};

use dtg_transaction::{
    CommitResolution, DurableTimestampAuthority, TimestampAuthority, TimestampCommandLog,
    TimestampLogFuture, TransactionId, TransactionTime, TxnError,
};

#[derive(Default)]
struct MemoryTimestampLog {
    entries: Mutex<Vec<Vec<u8>>>,
    batches: Mutex<usize>,
    fail_append: Mutex<bool>,
}

impl TimestampCommandLog for MemoryTimestampLog {
    fn replay(&self) -> TimestampLogFuture<'_, Vec<Vec<u8>>> {
        Box::pin(async move { Ok(self.entries.lock().unwrap().clone()) })
    }

    fn append_batch(&self, commands: Vec<Vec<u8>>) -> TimestampLogFuture<'_, ()> {
        Box::pin(async move {
            *self.batches.lock().unwrap() += 1;
            if *self.fail_append.lock().unwrap() {
                return Err(TxnError::Storage(
                    "injected timestamp append failure".into(),
                ));
            }
            self.entries.lock().unwrap().extend(commands);
            Ok(())
        })
    }
}

#[tokio::test]
async fn timestamp_authority_persists_a_batch_atomically_and_replays_it() {
    let log = Arc::new(MemoryTimestampLog::default());
    let authority = DurableTimestampAuthority::open(log.clone()).await.unwrap();
    let first = TransactionId::new(1).unwrap();
    let second = TransactionId::new(2).unwrap();

    assert_eq!(
        authority
            .apply_batch(vec![
                dtg_transaction::TimestampOperation::AllocateStart {
                    transaction_id: first,
                },
                dtg_transaction::TimestampOperation::ReserveCommit {
                    transaction_id: first,
                },
                dtg_transaction::TimestampOperation::ResolveCommit {
                    transaction_id: first,
                    commit_time: TransactionTime::new(2).unwrap(),
                    resolution: CommitResolution::Committed,
                },
                dtg_transaction::TimestampOperation::AllocateStart {
                    transaction_id: second,
                },
            ])
            .await,
        vec![
            Ok(TransactionTime::new(1).unwrap()),
            Ok(TransactionTime::new(2).unwrap()),
            Ok(TransactionTime::new(2).unwrap()),
            Ok(TransactionTime::new(3).unwrap()),
        ]
    );
    assert_eq!(*log.batches.lock().unwrap(), 1);

    let restarted = DurableTimestampAuthority::open(log).await.unwrap();
    assert_eq!(
        restarted
            .allocate_start_time(TransactionId::new(3).unwrap())
            .await,
        Ok(TransactionTime::new(4).unwrap())
    );
}

#[tokio::test]
async fn failed_timestamp_batch_does_not_publish_staged_state() {
    let log = Arc::new(MemoryTimestampLog {
        fail_append: Mutex::new(true),
        ..MemoryTimestampLog::default()
    });
    let authority = DurableTimestampAuthority::open(log.clone()).await.unwrap();

    assert!(matches!(
        authority
            .apply_batch(vec![dtg_transaction::TimestampOperation::AllocateStart {
                transaction_id: TransactionId::new(1).unwrap(),
            }])
            .await
            .as_slice(),
        [Err(TxnError::Storage(_))]
    ));
    *log.fail_append.lock().unwrap() = false;

    assert_eq!(
        authority
            .allocate_start_time(TransactionId::new(2).unwrap())
            .await,
        Ok(TransactionTime::new(1).unwrap())
    );
}

#[tokio::test]
async fn timestamp_authority_replays_idempotent_allocations_and_resolutions() {
    let log = Arc::new(MemoryTimestampLog::default());
    let authority = DurableTimestampAuthority::open(log.clone()).await.unwrap();
    let first = TransactionId::new(11).unwrap();

    assert_eq!(
        authority.allocate_start_time(first).await.unwrap(),
        TransactionTime::new(1).unwrap()
    );
    assert_eq!(
        authority.allocate_start_time(first).await.unwrap(),
        TransactionTime::new(1).unwrap()
    );
    assert_eq!(
        authority.reserve_commit_time(first).await.unwrap(),
        TransactionTime::new(2).unwrap()
    );
    authority
        .resolve_commit_time(
            first,
            TransactionTime::new(2).unwrap(),
            CommitResolution::Committed,
        )
        .await
        .unwrap();
    authority
        .resolve_commit_time(
            first,
            TransactionTime::new(2).unwrap(),
            CommitResolution::Committed,
        )
        .await
        .unwrap();

    let restarted = DurableTimestampAuthority::open(log).await.unwrap();
    assert_eq!(
        restarted.commit_time_reservation(first).await.unwrap(),
        Some(dtg_transaction::CommitTimeReservation::new(
            TransactionTime::new(2).unwrap(),
            Some(CommitResolution::Committed),
        ))
    );
    assert_eq!(
        restarted
            .allocate_start_time(TransactionId::new(12).unwrap())
            .await
            .unwrap(),
        TransactionTime::new(3).unwrap()
    );
    assert_eq!(
        restarted
            .reserve_commit_time(TransactionId::new(12).unwrap())
            .await
            .unwrap(),
        TransactionTime::new(4).unwrap()
    );
}

#[tokio::test]
async fn timestamp_authority_rejects_conflicting_terminal_resolution() {
    let authority = DurableTimestampAuthority::open(Arc::new(MemoryTimestampLog::default()))
        .await
        .unwrap();
    let transaction_id = TransactionId::new(21).unwrap();
    let commit_time = authority.reserve_commit_time(transaction_id).await.unwrap();

    authority
        .resolve_commit_time(transaction_id, commit_time, CommitResolution::Aborted)
        .await
        .unwrap();
    assert_eq!(
        authority
            .resolve_commit_time(transaction_id, commit_time, CommitResolution::Committed)
            .await,
        Err(TxnError::CorruptRecovery)
    );
}

#[tokio::test]
async fn concurrent_start_and_commit_reservation_keeps_the_commit_after_the_start() {
    let authority = Arc::new(
        DurableTimestampAuthority::open(Arc::new(MemoryTimestampLog::default()))
            .await
            .unwrap(),
    );
    let transaction_id = TransactionId::new(26).unwrap();

    let (start, commit) = tokio::join!(
        authority.allocate_start_time(transaction_id),
        authority.reserve_commit_time(transaction_id),
    );

    assert!(commit.unwrap() > start.unwrap());
}

#[tokio::test]
async fn pending_lower_commit_time_fences_the_published_start_frontier() {
    let authority = DurableTimestampAuthority::open(Arc::new(MemoryTimestampLog::default()))
        .await
        .unwrap();
    let first = TransactionId::new(31).unwrap();
    let second = TransactionId::new(32).unwrap();
    let later = TransactionId::new(33).unwrap();

    assert_eq!(
        authority.allocate_start_time(first).await.unwrap(),
        TransactionTime::new(1).unwrap()
    );
    let lower = authority.reserve_commit_time(first).await.unwrap();
    let higher = authority.reserve_commit_time(second).await.unwrap();
    authority
        .resolve_commit_time(second, higher, CommitResolution::Committed)
        .await
        .unwrap();

    assert_eq!(
        authority.allocate_start_time(later).await.unwrap(),
        TransactionTime::new(1).unwrap()
    );

    authority
        .resolve_commit_time(first, lower, CommitResolution::Aborted)
        .await
        .unwrap();
    assert_eq!(
        authority
            .allocate_start_time(TransactionId::new(34).unwrap())
            .await
            .unwrap(),
        TransactionTime::new(4).unwrap()
    );
}

#[tokio::test]
async fn timestamp_authority_fails_closed_on_malformed_replay() {
    let log = Arc::new(MemoryTimestampLog::default());
    log.entries.lock().unwrap().push(b"not-a-command".to_vec());

    assert!(matches!(
        DurableTimestampAuthority::open(log).await,
        Err(TxnError::CorruptRecovery)
    ));
}
