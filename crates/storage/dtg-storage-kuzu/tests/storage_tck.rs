use std::{
    future::Future,
    pin::pin,
    task::{Context, Poll, Waker},
};

use dtg_storage::{
    BindingRole, LogicalReplicaActivation, LogicalSnapshotCandidateReceipt, LogicalSnapshotSink,
    LogicalSnapshotSource, ReplicaStateStore, SnapshotRequest, StorageTckFactory, run_storage_tck,
};
use dtg_storage_kuzu::{KuzuReplicaStore, KuzuStorageTckFactory};

fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    match future.as_mut().poll(&mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("Kuzu storage future unexpectedly yielded"),
    }
}

#[test]
fn kuzu_passes_the_shared_storage_tck() {
    let directory = tempfile::tempdir().unwrap();
    let factory = KuzuStorageTckFactory::new(directory.path());
    block_on(run_storage_tck(&factory)).unwrap();
}

#[test]
fn kuzu_activation_rebinds_a_restored_candidate_namespace() {
    let directory = tempfile::tempdir().unwrap();
    let factory = KuzuStorageTckFactory::new(directory.path());
    let candidate_binding = factory
        .binding("activation-candidate", 1)
        .unwrap()
        .to_builder()
        .role(BindingRole::Candidate)
        .build()
        .unwrap();
    let active_binding = candidate_binding
        .to_builder()
        .role(BindingRole::Active)
        .build()
        .unwrap();
    let candidate = KuzuReplicaStore::open(
        directory
            .path()
            .join(candidate_binding.namespace_id().as_str()),
        candidate_binding.clone(),
    )
    .unwrap();

    let mut reader = block_on(candidate.begin_snapshot(
        dtg_storage::ReadFence::new(candidate_binding.clone(), 0),
        SnapshotRequest::new(1, 1).unwrap(),
    ))
    .unwrap();
    let header = reader.header().clone();
    let mut chunks = Vec::new();
    while let Some(chunk) = block_on(reader.next_chunk()).unwrap() {
        chunks.push(chunk);
    }
    let manifest = block_on(reader.finish()).unwrap();

    let mut writer =
        block_on(candidate.begin_restore(candidate_binding.clone(), header.clone())).unwrap();
    for chunk in chunks {
        block_on(writer.write_chunk(chunk)).unwrap();
    }
    block_on(writer.commit(manifest.clone())).unwrap();
    let receipt =
        LogicalSnapshotCandidateReceipt::new(candidate_binding, header, manifest).unwrap();
    block_on(candidate.activate_candidate(receipt, active_binding.clone())).unwrap();

    let reopened = KuzuReplicaStore::open(
        directory
            .path()
            .join(active_binding.namespace_id().as_str()),
        active_binding,
    )
    .unwrap();
    assert_eq!(block_on(reopened.applied_index()).unwrap(), 0);
}
