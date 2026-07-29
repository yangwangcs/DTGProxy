use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, ConsensusCommandEnvelope,
    ConsensusEntry, ConsensusSnapshotInstall, ConsensusSnapshotMetadata, ConsensusStore, Digest32,
    LogicalSnapshotCandidateReceipt, ProviderKind, RaftHardState, RaftMembership, ReplicaBinding,
    ReplicaId, SnapshotHeader, SnapshotId, SnapshotManifest, StorageError,
};
use dtg_storage_fjall::FjallConsensusStore;
use fjall::{Database, KeyspaceCreateOptions, PersistMode};

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("Fjall storage future unexpectedly yielded"),
    }
}

fn fixture_replica() -> ReplicaBinding {
    let capabilities = CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
    .unwrap();
    let class = BackendClass::new(
        ProviderKind::Fjall,
        1,
        1,
        capabilities.names().map(str::to_owned),
    )
    .unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(2)
        .shard_id(3)
        .placement_epoch(4)
        .replica_id(5)
        .backend_generation(6)
        .backend_class_digest(class.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(capabilities.digest())
        .namespace_id("consensus-recovery")
        .endpoint_profile_ref("local")
        .credential_ref("local")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn entry(index: u64) -> ConsensusEntry {
    ConsensusEntry::new(
        1,
        2,
        index,
        CommandId::new(u128::from(index)).unwrap(),
        ConsensusCommandEnvelope::new(1, vec![index as u8]).unwrap(),
    )
    .unwrap()
}

fn divergent_entry(index: u64) -> ConsensusEntry {
    ConsensusEntry::new(
        1,
        9,
        index,
        CommandId::new(u128::from(index) + 1000).unwrap(),
        ConsensusCommandEnvelope::new(1, vec![99, index as u8]).unwrap(),
    )
    .unwrap()
}

fn snapshot_install(active: &ReplicaBinding) -> ConsensusSnapshotInstall {
    let candidate = active
        .to_builder()
        .role(BindingRole::Candidate)
        .build()
        .unwrap();
    let header = SnapshotHeader::new(SnapshotId::new(91).unwrap(), active.clone(), 10, 1).unwrap();
    let manifest = SnapshotManifest {
        snapshot_id: header.snapshot_id(),
        chunk_count: 2,
        record_count: 7,
        content_digest: Digest32::new([0x91; 32]),
    };
    ConsensusSnapshotInstall::new(
        LogicalSnapshotCandidateReceipt::new(candidate, header, manifest).unwrap(),
        active.clone(),
        RaftHardState {
            current_term: 3,
            voted_for: None,
            committed_index: 10,
        },
        RaftMembership {
            voters: vec![active.replica_id()],
            learners: vec![],
            configuration_index: 10,
        },
        ConsensusSnapshotMetadata {
            snapshot_id: 91,
            last_included_term: 3,
            last_included_index: 10,
            content_digest: Digest32::new([0x91; 32]),
        },
    )
    .unwrap()
}

#[test]
fn consensus_entries_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let store = FjallConsensusStore::open(dir.path(), fixture_replica()).unwrap();
    block_on(store.append(vec![entry(4), entry(5)])).unwrap();
    let hard_state = RaftHardState {
        current_term: 7,
        voted_for: Some(ReplicaId::new(5).unwrap()),
        committed_index: 5,
    };
    let membership = RaftMembership {
        voters: vec![ReplicaId::new(5).unwrap()],
        learners: vec![ReplicaId::new(6).unwrap()],
        configuration_index: 5,
    };
    let snapshot = ConsensusSnapshotMetadata {
        snapshot_id: 9,
        last_included_term: 2,
        last_included_index: 3,
        content_digest: Digest32::new([8; 32]),
    };
    block_on(store.set_hard_state(hard_state)).unwrap();
    block_on(store.set_membership(membership.clone())).unwrap();
    block_on(store.set_snapshot_metadata(snapshot.clone())).unwrap();
    drop(store);
    let reopened = FjallConsensusStore::open(dir.path(), fixture_replica()).unwrap();
    assert_eq!(block_on(reopened.entries(4, 6, 1024)).unwrap().len(), 2);
    assert_eq!(block_on(reopened.hard_state()).unwrap(), hard_state);
    assert_eq!(block_on(reopened.membership()).unwrap(), membership);
    assert_eq!(
        block_on(reopened.snapshot_metadata()).unwrap(),
        Some(snapshot)
    );
}

#[test]
fn snapshot_install_journal_survives_restart_and_commits_consensus_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_replica();
    let install = snapshot_install(&binding);
    let store = FjallConsensusStore::open(dir.path(), binding.clone()).unwrap();
    block_on(store.stage_snapshot_install(install.clone())).unwrap();
    assert_eq!(
        block_on(store.snapshot_install()).unwrap(),
        Some(install.clone())
    );
    assert_eq!(
        block_on(store.hard_state()).unwrap(),
        RaftHardState::default()
    );
    assert!(block_on(store.snapshot_metadata()).unwrap().is_none());
    drop(store);

    let reopened = FjallConsensusStore::open(dir.path(), binding).unwrap();
    assert_eq!(
        block_on(reopened.snapshot_install()).unwrap(),
        Some(install.clone())
    );
    block_on(reopened.commit_snapshot_install(install.clone())).unwrap();
    assert!(block_on(reopened.snapshot_install()).unwrap().is_none());
    assert_eq!(
        block_on(reopened.hard_state()).unwrap(),
        install.hard_state()
    );
    assert_eq!(
        block_on(reopened.membership()).unwrap(),
        *install.membership()
    );
    assert_eq!(
        block_on(reopened.snapshot_metadata()).unwrap(),
        Some(install.metadata().clone())
    );
}

#[test]
fn divergent_append_atomically_replaces_and_truncates_the_suffix() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_replica();
    let store = FjallConsensusStore::open(dir.path(), binding).unwrap();
    block_on(store.append(vec![entry(4), entry(5), entry(6)])).unwrap();

    block_on(store.append(vec![entry(4), entry(5)])).unwrap();
    assert_eq!(
        block_on(store.entries(4, 7, 4096)).unwrap(),
        vec![entry(4), entry(5), entry(6)]
    );

    let replacement = divergent_entry(5);
    block_on(store.append(vec![replacement.clone()])).unwrap();
    assert_eq!(
        block_on(store.entries(4, 7, 4096)).unwrap(),
        vec![entry(4), replacement]
    );

    let before_gap = block_on(store.entries(4, 8, 4096)).unwrap();
    assert!(matches!(
        block_on(store.append(vec![entry(7)])),
        Err(StorageError::InvalidConsensus(_))
    ));
    assert_eq!(block_on(store.entries(4, 8, 4096)).unwrap(), before_gap);
}

#[test]
fn consensus_ranges_truncation_and_reopen_preserve_one_contiguous_log() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_replica();
    let store = FjallConsensusStore::open(dir.path(), binding.clone()).unwrap();
    block_on(store.append(vec![entry(4), entry(5), entry(6)])).unwrap();
    assert!(matches!(
        block_on(store.entries(7, 6, 1024)),
        Err(StorageError::InvalidConsensus(_))
    ));
    assert!(matches!(
        block_on(store.entries(4, 7, 0)),
        Err(StorageError::InvalidConsensus(_))
    ));
    assert!(block_on(store.entries(5, 5, 1024)).unwrap().is_empty());
    assert!(matches!(
        block_on(store.truncate_suffix(0)),
        Err(StorageError::InvalidConsensus(_))
    ));
    block_on(store.truncate_suffix(6)).unwrap();
    drop(store);

    let reopened = FjallConsensusStore::open(dir.path(), binding).unwrap();
    assert_eq!(
        block_on(reopened.entries(4, 7, 4096)).unwrap(),
        vec![entry(4), entry(5)]
    );
    block_on(reopened.append(vec![entry(6)])).unwrap();
    assert_eq!(block_on(reopened.entries(4, 7, 4096)).unwrap().len(), 3);
}

#[test]
fn consensus_records_fail_closed_on_unknown_codec_versions() {
    let dir = tempfile::tempdir().unwrap();
    let binding = fixture_replica();
    let store = FjallConsensusStore::open(dir.path(), binding.clone()).unwrap();
    block_on(store.append(vec![entry(4)])).unwrap();
    block_on(store.set_hard_state(RaftHardState {
        current_term: 4,
        voted_for: Some(ReplicaId::new(5).unwrap()),
        committed_index: 4,
    }))
    .unwrap();
    block_on(store.set_membership(RaftMembership {
        voters: vec![ReplicaId::new(5).unwrap()],
        learners: vec![],
        configuration_index: 4,
    }))
    .unwrap();
    block_on(store.set_snapshot_metadata(ConsensusSnapshotMetadata {
        snapshot_id: 5,
        last_included_term: 3,
        last_included_index: 3,
        content_digest: Digest32::new([6; 32]),
    }))
    .unwrap();
    drop(store);

    let db = Database::builder(dir.path()).open().unwrap();
    for (partition, key) in [
        ("raft_log", 4_u64.to_be_bytes().to_vec()),
        ("raft_state", b"hard_state".to_vec()),
        ("raft_state", b"membership".to_vec()),
        ("raft_snapshot", b"snapshot".to_vec()),
    ] {
        let keyspace = db
            .keyspace(partition, KeyspaceCreateOptions::default)
            .unwrap();
        let mut bytes = keyspace.get(&key).unwrap().unwrap().to_vec();
        bytes[0] = 2;
        keyspace.insert(key, bytes).unwrap();
    }
    db.persist(PersistMode::SyncAll).unwrap();
    drop(db);

    let reopened = FjallConsensusStore::open(dir.path(), binding).unwrap();
    assert!(block_on(reopened.entries(4, 5, 4096)).is_err());
    assert!(block_on(reopened.hard_state()).is_err());
    assert!(block_on(reopened.membership()).is_err());
    assert!(block_on(reopened.snapshot_metadata()).is_err());
}
