use analytics_api::{AlgorithmType, AlgorithmValue, GraphModel, builtin_algorithm_descriptors};

#[test]
fn built_in_degree_descriptor_has_exact_typed_signature() {
    let descriptors = builtin_algorithm_descriptors();
    let degree = descriptors
        .iter()
        .find(|descriptor| descriptor.name() == "dtg.graph.degree")
        .expect("degree descriptor");

    assert!(degree.inputs().is_empty());
    assert_eq!(
        degree
            .outputs()
            .iter()
            .map(|field| (field.name(), field.value_type(), field.nullable()))
            .collect::<Vec<_>>(),
        vec![
            ("vertexId", AlgorithmType::Vertex, false),
            ("inDegree", AlgorithmType::Integer, false),
            ("outDegree", AlgorithmType::Integer, false),
            ("degree", AlgorithmType::Integer, false),
        ]
    );
}

#[test]
fn built_in_algorithm_names_are_unique_and_all_have_outputs() {
    let descriptors = builtin_algorithm_descriptors();
    let mut names = descriptors
        .iter()
        .map(|descriptor| descriptor.name())
        .collect::<Vec<_>>();
    names.sort_unstable();
    names.dedup();

    assert_eq!(names.len(), descriptors.len());
    assert!(
        descriptors
            .iter()
            .all(|descriptor| !descriptor.outputs().is_empty())
    );
}

#[test]
fn native_snapshot_algorithms_declare_distributed_support() {
    let descriptors = builtin_algorithm_descriptors();

    for name in ["dtg.graph.degree", "dtg.graph.wcc", "dtg.graph.pageRank"] {
        assert!(
            descriptors
                .iter()
                .find(|descriptor| descriptor.name() == name)
                .expect("built-in descriptor")
                .distributed(),
            "{name} must advertise native distributed execution"
        );
    }
}

#[test]
fn stable_temporal_catalog_includes_event_interval_and_delta_consumers() {
    let descriptors = builtin_algorithm_descriptors();
    let descriptor = |name: &str| {
        descriptors
            .iter()
            .find(|descriptor| descriptor.name() == name)
            .unwrap_or_else(|| panic!("missing descriptor {name}"))
    };
    let signature = |name: &str| {
        descriptor(name)
            .inputs()
            .iter()
            .map(|field| (field.name(), field.value_type(), field.default()))
            .collect::<Vec<_>>()
    };
    let outputs = |name: &str| {
        descriptor(name)
            .outputs()
            .iter()
            .map(|field| (field.name(), field.value_type(), field.nullable()))
            .collect::<Vec<_>>()
    };

    for name in [
        "dtg.temporal.windowedComponents",
        "dtg.temporal.windowedTriangleCount",
        "dtg.temporal.changePoint",
        "dtg.temporal.motifCount",
    ] {
        assert_eq!(descriptor(name).graph_models(), &[GraphModel::Event]);
    }
    assert_eq!(
        descriptor("dtg.temporal.intervalComponents").graph_models(),
        &[GraphModel::Interval]
    );
    assert_eq!(
        descriptor("dtg.temporal.deltaSummary").graph_models(),
        &[GraphModel::Delta]
    );
    assert_eq!(
        signature("dtg.temporal.windowedComponents"),
        vec![
            ("validFrom", AlgorithmType::Time, None),
            ("validTo", AlgorithmType::Time, None),
        ]
    );
    assert_eq!(
        signature("dtg.temporal.motifCount"),
        vec![
            ("validFrom", AlgorithmType::Time, None),
            ("validTo", AlgorithmType::Time, None),
            (
                "deltaMicros",
                AlgorithmType::Integer,
                Some(&AlgorithmValue::Integer(1_000_000)),
            ),
        ]
    );
    assert_eq!(
        outputs("dtg.temporal.windowedComponents"),
        vec![
            ("vertexId", AlgorithmType::Vertex, false),
            ("componentId", AlgorithmType::Vertex, false),
        ]
    );
    assert_eq!(
        outputs("dtg.temporal.windowedTriangleCount"),
        vec![("triangleCount", AlgorithmType::Integer, false)]
    );
    assert_eq!(
        outputs("dtg.temporal.changePoint"),
        vec![
            ("vertexId", AlgorithmType::Vertex, false),
            ("score", AlgorithmType::Float, false),
        ]
    );
    assert_eq!(
        outputs("dtg.temporal.motifCount"),
        vec![
            ("motif", AlgorithmType::String, false),
            ("count", AlgorithmType::Integer, false),
        ]
    );
    assert_eq!(
        outputs("dtg.temporal.intervalComponents"),
        vec![
            ("vertexId", AlgorithmType::Vertex, false),
            ("componentId", AlgorithmType::Vertex, false),
        ]
    );
    assert_eq!(
        outputs("dtg.temporal.deltaSummary"),
        vec![
            ("entityType", AlgorithmType::String, false),
            ("change", AlgorithmType::String, false),
            ("count", AlgorithmType::Integer, false),
        ]
    );
}

#[test]
fn stable_ordinary_catalog_includes_traversal_centrality_apsp_and_louvain() {
    let descriptors = builtin_algorithm_descriptors();
    let descriptor = |name: &str| {
        descriptors
            .iter()
            .find(|descriptor| descriptor.name() == name)
            .unwrap_or_else(|| panic!("missing descriptor {name}"))
    };

    for name in [
        "dtg.graph.dfs",
        "dtg.graph.allPairsShortestPath",
        "dtg.graph.betweenness",
        "dtg.graph.closeness",
        "dtg.graph.louvain",
    ] {
        assert_eq!(descriptor(name).graph_models(), &[GraphModel::Snapshot]);
        assert!(descriptor(name).deterministic());
        assert!(!descriptor(name).distributed());
        assert!(!descriptor(name).outputs().is_empty());
    }
    assert!(descriptor("dtg.graph.allPairsShortestPath").exact());
    assert!(descriptor("dtg.graph.betweenness").exact());
    assert!(descriptor("dtg.graph.closeness").exact());
    assert!(!descriptor("dtg.graph.louvain").exact());
    assert_eq!(
        descriptor("dtg.graph.dfs")
            .inputs()
            .iter()
            .map(|field| (field.name(), field.value_type()))
            .collect::<Vec<_>>(),
        vec![("source", AlgorithmType::Vertex)]
    );
    assert_eq!(
        descriptor("dtg.graph.allPairsShortestPath")
            .outputs()
            .iter()
            .map(|field| (field.name(), field.value_type()))
            .collect::<Vec<_>>(),
        vec![
            ("source", AlgorithmType::Vertex),
            ("target", AlgorithmType::Vertex),
            ("distance", AlgorithmType::Float),
        ]
    );
    assert_eq!(
        descriptor("dtg.graph.louvain")
            .outputs()
            .iter()
            .map(|field| (field.name(), field.value_type()))
            .collect::<Vec<_>>(),
        vec![
            ("vertexId", AlgorithmType::Vertex),
            ("communityId", AlgorithmType::Vertex),
        ]
    );
}
