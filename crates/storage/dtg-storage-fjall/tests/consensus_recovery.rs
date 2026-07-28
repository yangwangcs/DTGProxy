use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use dtg_storage::{
    BackendClass, BindingRole, CapabilityManifest, CommandId, ConsensusCommandEnvelope,
    ConsensusEntry, ConsensusSnapshotMetadata, ConsensusStore, Digest32, ProviderKind,
    RaftHardState, RaftMembership, ReplicaBinding, ReplicaId,
};
use dtg_storage_fjall::FjallConsensusStore;

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
    let capabilities = CapabilityManifest::from_names(["point"]).unwrap();
    let class = BackendClass::new(ProviderKind::Fjall, 1, 1, ["point"]).unwrap();
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
