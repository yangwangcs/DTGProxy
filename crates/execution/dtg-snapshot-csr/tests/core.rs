use dtg_snapshot_csr::{CsrDirection, SnapshotCsr, SnapshotCsrBuildBudget, SnapshotCsrKey};
use dtg_storage::{
    BackendClass, BindingRole, EdgeId, EdgeVersion, Properties, ProviderKind, ReplicaBinding,
    TransactionTime, ValidInterval, Version, VertexId,
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
        .namespace_id("snapshot-csr-core")
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

#[test]
fn outgoing_csr_keeps_dense_neighbors_and_the_exact_snapshot_identity() {
    let key = SnapshotCsrKey::new(
        binding(),
        17,
        TransactionTime::new(10).unwrap(),
        7,
        CsrDirection::Outgoing,
    );
    let csr = SnapshotCsr::build(
        key.clone(),
        vec![edge(8, 20, 30), edge(7, 10, 20), edge(9, 20, 40)],
        SnapshotCsrBuildBudget::new(16 * 1024),
    )
    .unwrap();

    assert_eq!(csr.key(), &key);
    assert_eq!(
        csr.neighbors(VertexId::new(20).unwrap())
            .unwrap()
            .map(|neighbor| neighbor.edge().id().get())
            .collect::<Vec<_>>(),
        vec![8, 9]
    );
    assert!(csr.retained_bytes() <= 16 * 1024);
}

#[test]
fn build_rejects_an_image_larger_than_its_reservation() {
    let error = SnapshotCsr::build(
        SnapshotCsrKey::new(
            binding(),
            17,
            TransactionTime::new(10).unwrap(),
            7,
            CsrDirection::Outgoing,
        ),
        vec![edge(7, 10, 20)],
        SnapshotCsrBuildBudget::new(1),
    )
    .unwrap_err();

    assert!(error.is_insufficient_memory());
}

#[test]
fn build_charges_the_full_property_payload_against_its_reservation() {
    let key = SnapshotCsrKey::new(
        binding(),
        9,
        TransactionTime::new(23).unwrap(),
        17,
        CsrDirection::Outgoing,
    );
    let mut properties = Properties::new();
    properties.insert(
        "payload".into(),
        dtg_storage::Value::String("x".repeat(512)),
    );
    let edge = EdgeVersion::new(
        EdgeId::new(71).unwrap(),
        VertexId::new(10).unwrap(),
        VertexId::new(20).unwrap(),
        "LINK",
        Version::new(1),
        ValidInterval::new(1, 100).unwrap(),
        TransactionTime::new(23).unwrap(),
        properties,
    )
    .unwrap();

    let error = SnapshotCsr::build(key, vec![edge], SnapshotCsrBuildBudget::new(200)).unwrap_err();
    assert!(error.is_insufficient_memory());
}

#[test]
fn direction_is_part_of_the_snapshot_identity() {
    let binding = binding();
    let outgoing = SnapshotCsrKey::new(
        binding.clone(),
        17,
        TransactionTime::new(10).unwrap(),
        7,
        CsrDirection::Outgoing,
    );
    let incoming = SnapshotCsrKey::new(
        binding,
        17,
        TransactionTime::new(10).unwrap(),
        7,
        CsrDirection::Incoming,
    );

    assert_ne!(outgoing, incoming);
}
