use std::sync::Arc;

use dtg_snapshot_csr::{
    CommittedAdjacencyOperation, CommittedCsrOverlay, CommittedGraphDelta, CsrDirection,
    SnapshotCsr, SnapshotCsrBuildBudget, SnapshotCsrKey,
};
use dtg_storage::{
    BackendClass, BindingRole, EdgeId, EdgeTombstone, EdgeVersion, Properties, ProviderKind,
    ReplicaBinding, TransactionTime, ValidInterval, Version, VertexId,
};

fn binding() -> ReplicaBinding {
    let backend = BackendClass::new(ProviderKind::Fjall, 1, 1, [] as [&str; 0]).unwrap();
    ReplicaBinding::builder()
        .cluster_id(1)
        .graph_id(2)
        .shard_id(3)
        .placement_epoch(4)
        .replica_id(5)
        .backend_generation(6)
        .backend_class_digest(backend.digest())
        .provider_kind(ProviderKind::Fjall)
        .contract_version(1)
        .layout_version(1)
        .capability_digest(backend.required_capabilities().digest())
        .namespace_id("snapshot-csr-overlay")
        .endpoint_profile_ref("fixture")
        .credential_ref("fixture")
        .role(BindingRole::Active)
        .build()
        .unwrap()
}

fn edge(id: u128, source: u128, target: u128) -> EdgeVersion {
    EdgeVersion::new(
        EdgeId::new(id).unwrap(),
        VertexId::new(source).unwrap(),
        VertexId::new(target).unwrap(),
        "LINK",
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(10).unwrap(),
        Properties::new(),
    )
    .unwrap()
}

fn tombstone(id: u128) -> EdgeTombstone {
    EdgeTombstone::new(
        EdgeId::new(id).unwrap(),
        Version::new(1),
        TransactionTime::new(10).unwrap(),
    )
}

fn edge_at(
    id: u128,
    source: u128,
    target: u128,
    version: u64,
    transaction_time: i64,
) -> EdgeVersion {
    EdgeVersion::new(
        EdgeId::new(id).unwrap(),
        VertexId::new(source).unwrap(),
        VertexId::new(target).unwrap(),
        "LINK",
        Version::new(version),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(transaction_time).unwrap(),
        Properties::new(),
    )
    .unwrap()
}

#[test]
fn committed_overlay_merges_contiguous_adds_and_removes_at_the_exact_index() {
    let key = SnapshotCsrKey::new(
        binding(),
        10,
        TransactionTime::new(10).unwrap(),
        7,
        CsrDirection::Outgoing,
    );
    let base = Arc::new(
        SnapshotCsr::build(
            key.clone(),
            vec![edge(7, 10, 20)],
            SnapshotCsrBuildBudget::new(16 * 1024),
        )
        .unwrap(),
    );
    let mut overlay = CommittedCsrOverlay::new(base, 16 * 1024).unwrap();

    overlay
        .apply(
            CommittedGraphDelta::new(
                key.with_applied_index(11),
                vec![CommittedAdjacencyOperation::Add(edge(8, 10, 30))],
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        overlay
            .neighbors(VertexId::new(10).unwrap(), 11)
            .unwrap()
            .iter()
            .map(|edge| edge.id().get())
            .collect::<Vec<_>>(),
        vec![7, 8]
    );

    overlay
        .apply(
            CommittedGraphDelta::new(
                key.with_applied_index(12),
                vec![CommittedAdjacencyOperation::Remove(tombstone(7))],
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(
        overlay
            .neighbors(VertexId::new(10).unwrap(), 12)
            .unwrap()
            .iter()
            .map(|edge| edge.id().get())
            .collect::<Vec<_>>(),
        vec![8]
    );
}

#[test]
fn a_gap_invalidates_the_overlay_instead_of_serving_a_guessed_snapshot() {
    let key = SnapshotCsrKey::new(
        binding(),
        10,
        TransactionTime::new(10).unwrap(),
        7,
        CsrDirection::Outgoing,
    );
    let base = Arc::new(
        SnapshotCsr::build(
            key.clone(),
            vec![edge(7, 10, 20)],
            SnapshotCsrBuildBudget::new(16 * 1024),
        )
        .unwrap(),
    );
    let mut overlay = CommittedCsrOverlay::new(base, 16 * 1024).unwrap();

    assert!(
        overlay
            .apply(
                CommittedGraphDelta::new(
                    key.with_applied_index(12),
                    vec![CommittedAdjacencyOperation::Add(edge(8, 10, 30))],
                )
                .unwrap(),
            )
            .is_err()
    );
    assert!(!overlay.is_valid());
    assert!(overlay.neighbors(VertexId::new(10).unwrap(), 10).is_err());
}

#[test]
fn removing_an_overlay_addition_releases_its_budget_before_recording_the_tombstone() {
    let key = SnapshotCsrKey::new(
        binding(),
        10,
        TransactionTime::new(10).unwrap(),
        7,
        CsrDirection::Outgoing,
    );
    let base = Arc::new(
        SnapshotCsr::build(
            key.clone(),
            vec![edge(7, 10, 20)],
            SnapshotCsrBuildBudget::new(16 * 1024),
        )
        .unwrap(),
    );
    let mut overlay = CommittedCsrOverlay::new(base, 100).unwrap();

    overlay
        .apply(
            CommittedGraphDelta::new(
                key.with_applied_index(11),
                vec![CommittedAdjacencyOperation::Add(edge(8, 10, 30))],
            )
            .unwrap(),
        )
        .unwrap();
    overlay
        .apply(
            CommittedGraphDelta::new(
                key.with_applied_index(12),
                vec![CommittedAdjacencyOperation::Remove(tombstone(8))],
            )
            .unwrap(),
        )
        .unwrap();

    assert_eq!(overlay.retained_bytes(), 0);
    assert_eq!(
        overlay
            .neighbors(VertexId::new(10).unwrap(), 12)
            .unwrap()
            .iter()
            .map(|edge| edge.id().get())
            .collect::<Vec<_>>(),
        vec![7]
    );
}

#[test]
fn exceeding_the_overlay_entry_budget_invalidates_it_before_an_unbounded_log_accumulates() {
    let key = SnapshotCsrKey::new(
        binding(),
        10,
        TransactionTime::new(10).unwrap(),
        7,
        CsrDirection::Outgoing,
    );
    let base = Arc::new(
        SnapshotCsr::build(
            key.clone(),
            vec![edge(7, 10, 20)],
            SnapshotCsrBuildBudget::new(16 * 1024),
        )
        .unwrap(),
    );
    let mut overlay = CommittedCsrOverlay::with_limits(base, 16 * 1024, 1).unwrap();

    overlay
        .apply(
            CommittedGraphDelta::new(
                key.with_applied_index(11),
                vec![CommittedAdjacencyOperation::Remove(tombstone(7))],
            )
            .unwrap(),
        )
        .unwrap();
    assert!(
        overlay
            .apply(
                CommittedGraphDelta::new(
                    key.with_applied_index(12),
                    vec![CommittedAdjacencyOperation::Add(edge(8, 10, 30))],
                )
                .unwrap(),
            )
            .is_err()
    );
    assert!(!overlay.is_valid());
}

#[test]
fn an_older_transaction_version_cannot_replace_a_newer_base_edge() {
    let key = SnapshotCsrKey::new(
        binding(),
        10,
        TransactionTime::new(20).unwrap(),
        7,
        CsrDirection::Outgoing,
    );
    let base = Arc::new(
        SnapshotCsr::build(
            key.clone(),
            vec![edge_at(7, 10, 20, 2, 20)],
            SnapshotCsrBuildBudget::new(16 * 1024),
        )
        .unwrap(),
    );
    let mut overlay = CommittedCsrOverlay::new(base, 16 * 1024).unwrap();

    overlay
        .apply(
            CommittedGraphDelta::new(
                key.with_applied_index(11),
                vec![CommittedAdjacencyOperation::Add(edge_at(7, 10, 30, 9, 10))],
            )
            .unwrap(),
        )
        .unwrap();

    assert_eq!(
        overlay
            .neighbors(VertexId::new(10).unwrap(), 11)
            .unwrap()
            .into_iter()
            .map(|edge| edge.target().get())
            .collect::<Vec<_>>(),
        vec![20]
    );
}
