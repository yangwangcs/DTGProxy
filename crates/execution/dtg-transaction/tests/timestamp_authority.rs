use std::sync::{Arc, Mutex};

use dtg_transaction::{
    CommitResolution, DurableTimestampAuthority, TimestampAuthority, TimestampCommandLog,
    TimestampLogFuture, TransactionId, TransactionTime, TxnError,
};

#[derive(Default)]
struct MemoryTimestampLog {
    entries: Mutex<Vec<Vec<u8>>>,
}

impl TimestampCommandLog for MemoryTimestampLog {
    fn replay(&self) -> TimestampLogFuture<'_, Vec<Vec<u8>>> {
        Box::pin(async move { Ok(self.entries.lock().unwrap().clone()) })
    }

    fn append(&self, command: Vec<u8>) -> TimestampLogFuture<'_, ()> {
        Box::pin(async move {
            self.entries.lock().unwrap().push(command);
            Ok(())
        })
    }
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
async fn timestamp_authority_fails_closed_on_malformed_replay() {
    let log = Arc::new(MemoryTimestampLog::default());
    log.entries.lock().unwrap().push(b"not-a-command".to_vec());

    assert!(matches!(
        DurableTimestampAuthority::open(log).await,
        Err(TxnError::CorruptRecovery)
    ));
}
