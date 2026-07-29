#![forbid(unsafe_code)]

use dtg_storage_neo4j::NativeModel;

#[test]
fn native_model_uses_nodes_relationships_and_owner_fence() {
    let model = NativeModel::v1();
    for label in [
        "DtgOwner",
        "DtgVertex",
        "DtgVersion",
        "DtgTransaction",
        "DtgMetadata",
        "DtgReplay",
        "DtgChange",
        "DtgSnapshotStage",
    ] {
        assert!(
            model.labels().contains(&label),
            "missing native label {label}"
        );
    }
    assert!(model.relationships().contains(&"DTG_EDGE"));
    assert!(model.relationships().contains(&"HAS_VERSION"));
    assert!(
        model
            .constraints()
            .iter()
            .all(|value| value.contains("namespace_id")),
        "every uniqueness constraint must include namespace identity"
    );
    assert!(
        model
            .indexes()
            .iter()
            .all(|value| value.contains("namespace_id")),
        "every native index must be namespace-scoped"
    );
    assert!(!model.labels().contains(&"CanonicalKv"));
    assert!(!model.relationships().contains(&"CANONICAL_ENTRY"));
}

#[test]
fn model_declares_generation_fenced_bounded_query_shapes() {
    let model = NativeModel::v1();
    for query in model.query_contracts() {
        let normalized = query.to_ascii_lowercase();
        assert!(normalized.contains("namespace_id: $namespace_id"));
        assert!(normalized.contains("backend_generation: $backend_generation"));
        assert!(normalized.contains("limit $limit") || normalized.contains("owner"));
    }
}
