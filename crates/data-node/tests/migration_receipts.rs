use std::fs::OpenOptions;
use std::io::Write;

use data_node::{MigrationReceiptStore, MigrationStorageError, ReceiptWriteOutcome};

#[test]
fn receipt_replay_is_idempotent_and_conflicting_input_fails_closed() {
    let temporary = tempfile::tempdir().unwrap();
    let mut store = MigrationReceiptStore::open(temporary.path()).unwrap();
    assert_eq!(
        store
            .record([0x11; 16], 3, [0x22; 32], b"installed:50".to_vec())
            .unwrap(),
        ReceiptWriteOutcome::Stored
    );
    assert_eq!(
        store
            .record([0x11; 16], 3, [0x22; 32], b"installed:50".to_vec())
            .unwrap(),
        ReceiptWriteOutcome::Duplicate
    );
    assert!(matches!(
        store.record([0x11; 16], 3, [0x23; 32], b"installed:50".to_vec()),
        Err(MigrationStorageError::ReceiptReplayConflict { step: 3 })
    ));
    drop(store);

    let reopened = MigrationReceiptStore::open(temporary.path()).unwrap();
    let receipt = reopened.get([0x11; 16], 3).unwrap();
    assert_eq!(receipt.input_digest(), &[0x22; 32]);
    assert_eq!(receipt.outcome(), b"installed:50");
}

#[test]
fn torn_trailing_receipt_is_repaired_but_committed_corruption_is_rejected() {
    let temporary = tempfile::tempdir().unwrap();
    let mut store = MigrationReceiptStore::open(temporary.path()).unwrap();
    store
        .record([0x31; 16], 1, [0x41; 32], b"ok".to_vec())
        .unwrap();
    let path = store.path().to_path_buf();
    drop(store);
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(b"torn")
        .unwrap();
    let repaired = MigrationReceiptStore::open(temporary.path()).unwrap();
    assert!(repaired.get([0x31; 16], 1).is_some());
    drop(repaired);

    let mut bytes = std::fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x80;
    std::fs::write(path, bytes).unwrap();
    assert!(matches!(
        MigrationReceiptStore::open(temporary.path()),
        Err(MigrationStorageError::ReceiptChecksumMismatch)
    ));
}
