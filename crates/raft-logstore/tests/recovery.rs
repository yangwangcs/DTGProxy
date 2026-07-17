use raft::eraftpb::{ConfState, Entry, HardState, Snapshot};
use raft::{GetEntriesContext, Storage};
use raft_logstore::{PersistFailpoint, RaftLogStoreError, RocksRaftStorage};

#[test]
fn hard_state_and_entries_survive_process_style_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let store = RocksRaftStorage::open(directory.path(), &[1, 2, 3]).unwrap();
    let entries = vec![entry(1, 1, b"one"), entry(2, 1, b"two")];
    let hard_state = HardState {
        term: 1,
        vote: 1,
        commit: 2,
    };
    store
        .persist_ready(None, &entries, Some(&hard_state))
        .unwrap();
    drop(store);

    let reopened = RocksRaftStorage::open(directory.path(), &[1, 2, 3]).unwrap();
    assert_eq!(reopened.initial_state().unwrap().hard_state, hard_state);
    assert_eq!(reopened.first_index().unwrap(), 1);
    assert_eq!(reopened.last_index().unwrap(), 2);
    assert_eq!(
        reopened
            .entries(1, 3, None, GetEntriesContext::empty(false))
            .unwrap(),
        entries
    );
}

#[test]
fn conflicting_suffix_is_atomically_replaced_and_recovered() {
    let directory = tempfile::tempdir().unwrap();
    let store = RocksRaftStorage::open(directory.path(), &[1, 2, 3]).unwrap();
    store
        .persist_ready(
            None,
            &[
                entry(1, 1, b"one"),
                entry(2, 1, b"old"),
                entry(3, 1, b"gone"),
            ],
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: 1,
            }),
        )
        .unwrap();
    store
        .persist_ready(
            None,
            &[entry(2, 2, b"new"), entry(3, 2, b"three")],
            Some(&HardState {
                term: 2,
                vote: 2,
                commit: 3,
            }),
        )
        .unwrap();
    drop(store);

    let reopened = RocksRaftStorage::open(directory.path(), &[1, 2, 3]).unwrap();
    let entries = reopened
        .entries(1, 4, None, GetEntriesContext::empty(false))
        .unwrap();
    assert_eq!(
        entries,
        vec![
            entry(1, 1, b"one"),
            entry(2, 2, b"new"),
            entry(3, 2, b"three"),
        ]
    );
}

#[test]
fn durable_snapshot_restores_membership_position_and_payload_before_compaction() {
    let directory = tempfile::tempdir().unwrap();
    let store = RocksRaftStorage::open(directory.path(), &[1, 2, 3]).unwrap();
    store
        .persist_ready(
            None,
            &[entry(1, 1, b"one"), entry(2, 1, b"two")],
            Some(&HardState {
                term: 1,
                vote: 1,
                commit: 2,
            }),
        )
        .unwrap();
    let snapshot = snapshot(2, 1, &[1, 2, 3], b"manifest-v1");
    store.persist_snapshot(&snapshot).unwrap();
    drop(store);

    let reopened = RocksRaftStorage::open(directory.path(), &[1, 2, 3]).unwrap();
    assert_eq!(reopened.first_index().unwrap(), 3);
    assert_eq!(reopened.last_index().unwrap(), 2);
    assert_eq!(reopened.term(2).unwrap(), 1);
    assert_eq!(reopened.snapshot(2, 2).unwrap(), snapshot);
    assert_eq!(
        reopened.initial_state().unwrap().conf_state.voters,
        vec![1, 2, 3]
    );
}

#[test]
fn crash_failpoints_prove_before_write_rollback_and_after_write_recovery() {
    let directory = tempfile::tempdir().unwrap();
    let store = RocksRaftStorage::open(directory.path(), &[1, 2, 3]).unwrap();
    let hard_state = HardState {
        term: 1,
        vote: 1,
        commit: 1,
    };

    store.inject_failure_once(PersistFailpoint::BeforeWrite);
    assert!(matches!(
        store.persist_ready(None, &[entry(1, 1, b"one")], Some(&hard_state)),
        Err(RaftLogStoreError::InjectedFailure(
            PersistFailpoint::BeforeWrite
        ))
    ));
    drop(store);
    let store = RocksRaftStorage::open(directory.path(), &[1, 2, 3]).unwrap();
    assert_eq!(store.last_index().unwrap(), 0);

    store.inject_failure_once(PersistFailpoint::AfterWriteBeforeCache);
    assert!(matches!(
        store.persist_ready(None, &[entry(1, 1, b"one")], Some(&hard_state)),
        Err(RaftLogStoreError::InjectedFailure(
            PersistFailpoint::AfterWriteBeforeCache
        ))
    ));
    drop(store);
    let reopened = RocksRaftStorage::open(directory.path(), &[1, 2, 3]).unwrap();
    assert_eq!(reopened.last_index().unwrap(), 1);
    assert_eq!(reopened.initial_state().unwrap().hard_state, hard_state);
}

fn entry(index: u64, term: u64, data: &[u8]) -> Entry {
    Entry {
        index,
        term,
        data: data.to_vec(),
        ..Default::default()
    }
}

fn snapshot(index: u64, term: u64, voters: &[u64], data: &[u8]) -> Snapshot {
    Snapshot {
        data: data.to_vec(),
        metadata: Some(raft::eraftpb::SnapshotMetadata {
            conf_state: Some(ConfState {
                voters: voters.to_vec(),
                ..Default::default()
            }),
            index,
            term,
        }),
    }
}
