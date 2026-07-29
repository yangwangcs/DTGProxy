use std::collections::BTreeMap;

use dtg_analytics::{
    AlgorithmBudget, AlgorithmCatalog, AlgorithmRequest, AlgorithmResult, AnalyticsError,
    BackendGeneration, BuiltInAlgorithmId, CancellationToken, EdgeId, PartitionProvenance,
    ProjectedEdge, ProjectedVertex, ProjectionBudget, ProjectionSpec, PropertyColumnSpec,
    PropertyType, ShardId, ShardProjectionPart, ShardSnapshotProvenance, SnapshotCsr,
    SnapshotProvenance, TransactionId, TransactionTime, Value, Version, VertexId, run_builtin,
};

const ALL_ALGORITHMS: [BuiltInAlgorithmId; 20] = [
    BuiltInAlgorithmId::BreadthFirstSearch,
    BuiltInAlgorithmId::DepthFirstSearch,
    BuiltInAlgorithmId::BoundedSingleSourceShortestPath,
    BuiltInAlgorithmId::BoundedAllPairsShortestPaths,
    BuiltInAlgorithmId::StronglyConnectedComponents,
    BuiltInAlgorithmId::WeaklyConnectedComponents,
    BuiltInAlgorithmId::PageRank,
    BuiltInAlgorithmId::DegreeCentrality,
    BuiltInAlgorithmId::ClosenessCentrality,
    BuiltInAlgorithmId::BetweennessCentrality,
    BuiltInAlgorithmId::TriangleCount,
    BuiltInAlgorithmId::ClusteringCoefficient,
    BuiltInAlgorithmId::KCore,
    BuiltInAlgorithmId::LabelPropagation,
    BuiltInAlgorithmId::Louvain,
    BuiltInAlgorithmId::EarliestArrival,
    BuiltInAlgorithmId::LatestDeparture,
    BuiltInAlgorithmId::TemporalReachability,
    BuiltInAlgorithmId::TemporalMotif,
    BuiltInAlgorithmId::ChangePoint,
];

fn vertex(id: u128) -> VertexId {
    VertexId::new(id).expect("nonzero vertex")
}

fn edge(id: u128) -> EdgeId {
    EdgeId::new(id).expect("nonzero edge")
}

fn properties(entries: &[(&str, Value)]) -> BTreeMap<String, Value> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect()
}

fn snapshot() -> SnapshotProvenance {
    let shard_id = ShardId::new(1).expect("shard");
    SnapshotProvenance::new(
        TransactionId::new(1).expect("transaction"),
        TransactionTime::new(100).expect("start time"),
        Version::new(3),
        vec![(
            shard_id,
            ShardSnapshotProvenance {
                placement_epoch: dtg_analytics::PlacementEpoch::new(4).expect("epoch"),
                backend_generation: BackendGeneration::new(5).expect("generation"),
                applied_index: 6,
                closed_time: TransactionTime::new(110).expect("closed time"),
            },
        )],
    )
    .expect("snapshot")
}

fn projected_edge(
    id: u128,
    source: u128,
    target: u128,
    weight: i64,
    departure: i64,
    arrival: i64,
    signal: i64,
) -> ProjectedEdge {
    ProjectedEdge {
        id: edge(id),
        source: vertex(source),
        target: vertex(target),
        properties: properties(&[
            ("weight", Value::Integer(weight)),
            ("departure", Value::Integer(departure)),
            ("arrival", Value::Integer(arrival)),
            ("event_time", Value::Integer(departure)),
            ("signal", Value::Integer(signal)),
        ]),
    }
}

fn graph() -> SnapshotCsr {
    let snapshot = snapshot();
    let shard_id = ShardId::new(1).expect("shard");
    let fence = *snapshot.shards().get(&shard_id).expect("fence");
    let part = ShardProjectionPart {
        snapshot,
        provenance: PartitionProvenance {
            shard_id,
            placement_epoch: fence.placement_epoch,
            backend_generation: fence.backend_generation,
            applied_index: fence.applied_index,
            partition_index: 0,
            partition_count: 1,
        },
        vertices: (1..=5)
            .rev()
            .map(|id| ProjectedVertex {
                id: vertex(id),
                properties: BTreeMap::new(),
            })
            .collect(),
        edges: vec![
            projected_edge(7, 5, 4, 1, 14, 15, 13),
            projected_edge(3, 1, 3, 5, 2, 7, 10),
            projected_edge(1, 1, 2, 1, 1, 2, 1),
            projected_edge(6, 4, 5, 1, 12, 13, 13),
            projected_edge(2, 2, 3, 2, 3, 5, 10),
            projected_edge(5, 3, 4, 1, 10, 11, 12),
            projected_edge(4, 3, 1, 1, 8, 9, 11),
        ],
    };
    let edge_columns = ["weight", "departure", "arrival", "event_time", "signal"]
        .into_iter()
        .map(|name| PropertyColumnSpec::required(name, PropertyType::Integer))
        .collect();
    SnapshotCsr::assemble(
        vec![part],
        ProjectionSpec::new(true, Vec::new(), edge_columns).expect("spec"),
        ProjectionBudget::new(128 * 1024, 0, CancellationToken::new()),
    )
    .expect("graph")
}

fn budget() -> AlgorithmBudget {
    AlgorithmBudget::new(128 * 1024, 64, 100_000, CancellationToken::new())
}

fn request(id: BuiltInAlgorithmId) -> AlgorithmRequest {
    AlgorithmRequest::new(id)
}

fn assert_close(left: f64, right: f64) {
    assert!((left - right).abs() < 1.0e-9, "{left} != {right}");
}

#[test]
fn catalog_is_closed_complete_versioned_and_has_no_registry_fallback() {
    assert_eq!(AlgorithmCatalog::ids(), &ALL_ALGORITHMS);
    for id in ALL_ALGORITHMS {
        let descriptor = AlgorithmCatalog::descriptor(id);
        assert_eq!(descriptor.id, id);
        assert!(descriptor.deterministic);
        assert_eq!(
            descriptor.tie_break,
            dtg_analytics::DeterministicTieBreak::StableVertexThenEdgeId
        );
        assert!(descriptor.max_memory_bytes > 0);
        assert!(descriptor.max_iterations > 0);
        assert_eq!(descriptor.cancellation_check_interval, 1);
        assert!(descriptor.result_schema_version > 0);
        assert!(descriptor.checkpoint_schema_version > 0);
    }
    assert_eq!(
        AlgorithmCatalog::descriptor(BuiltInAlgorithmId::BoundedSingleSourceShortestPath)
            .required_edge_properties,
        &["weight"]
    );
    assert_eq!(
        AlgorithmCatalog::descriptor(BuiltInAlgorithmId::EarliestArrival).required_edge_properties,
        &["departure", "arrival"]
    );
    assert_eq!(
        AlgorithmCatalog::descriptor(BuiltInAlgorithmId::ChangePoint).required_edge_properties,
        &["event_time", "signal"]
    );
    assert!(BuiltInAlgorithmId::try_from("user.uploaded_code").is_err());
    assert!(BuiltInAlgorithmId::try_from("registry.fallback").is_err());
}

#[test]
fn bfs_and_dfs_use_stable_vertex_id_ties() {
    let graph = graph();
    let mut bfs = request(BuiltInAlgorithmId::BreadthFirstSearch);
    bfs.source = Some(vertex(1));
    let mut dfs = request(BuiltInAlgorithmId::DepthFirstSearch);
    dfs.source = Some(vertex(1));

    assert_eq!(
        run_builtin(&graph, &bfs, budget()).expect("BFS"),
        AlgorithmResult::Traversal(vec![vertex(1), vertex(2), vertex(3), vertex(4), vertex(5)])
    );
    assert_eq!(
        run_builtin(&graph, &dfs, budget()).expect("DFS"),
        AlgorithmResult::Traversal(vec![vertex(1), vertex(2), vertex(3), vertex(4), vertex(5)])
    );
}

#[test]
fn bounded_sssp_and_apsp_match_weighted_oracles() {
    let graph = graph();
    let mut sssp = request(BuiltInAlgorithmId::BoundedSingleSourceShortestPath);
    sssp.source = Some(vertex(1));
    sssp.weight_property = Some("weight".into());
    let AlgorithmResult::Distances(distances) = run_builtin(&graph, &sssp, budget()).expect("SSSP")
    else {
        panic!("distance result expected")
    };
    assert_close(distances[&vertex(1)], 0.0);
    assert_close(distances[&vertex(2)], 1.0);
    assert_close(distances[&vertex(3)], 3.0);
    assert_close(distances[&vertex(4)], 4.0);
    assert_close(distances[&vertex(5)], 5.0);

    let mut apsp = request(BuiltInAlgorithmId::BoundedAllPairsShortestPaths);
    apsp.weight_property = Some("weight".into());
    apsp.max_pairs = 25;
    let AlgorithmResult::AllPairs(distances) = run_builtin(&graph, &apsp, budget()).expect("APSP")
    else {
        panic!("all-pairs result expected")
    };
    assert_close(distances[&(vertex(1), vertex(5))], 5.0);
    assert_close(distances[&(vertex(5), vertex(4))], 1.0);
    assert!(!distances.contains_key(&(vertex(5), vertex(1))));
}

#[test]
fn strongly_and_weakly_connected_components_match_oracles() {
    let graph = graph();
    assert_eq!(
        run_builtin(
            &graph,
            &request(BuiltInAlgorithmId::StronglyConnectedComponents),
            budget(),
        )
        .expect("SCC"),
        AlgorithmResult::Components(vec![
            vec![vertex(1), vertex(2), vertex(3)],
            vec![vertex(4), vertex(5)],
        ])
    );
    assert_eq!(
        run_builtin(
            &graph,
            &request(BuiltInAlgorithmId::WeaklyConnectedComponents),
            budget(),
        )
        .expect("WCC"),
        AlgorithmResult::Components(vec![vec![
            vertex(1),
            vertex(2),
            vertex(3),
            vertex(4),
            vertex(5),
        ]])
    );
}

#[test]
fn centrality_algorithms_are_deterministic_and_match_small_graph_oracles() {
    let graph = graph();
    let mut page_rank = request(BuiltInAlgorithmId::PageRank);
    page_rank.iterations = 20;
    let first = run_builtin(&graph, &page_rank, budget()).expect("PageRank");
    let second = run_builtin(&graph, &page_rank, budget()).expect("PageRank repeat");
    assert_eq!(first, second);
    let AlgorithmResult::Scores(scores) = first else {
        panic!("score result expected")
    };
    assert_close(scores.values().sum(), 1.0);

    let AlgorithmResult::Scores(degree) = run_builtin(
        &graph,
        &request(BuiltInAlgorithmId::DegreeCentrality),
        budget(),
    )
    .expect("degree") else {
        panic!("degree scores expected")
    };
    assert!(degree[&vertex(3)] > degree[&vertex(2)]);

    let AlgorithmResult::Scores(closeness) = run_builtin(
        &graph,
        &request(BuiltInAlgorithmId::ClosenessCentrality),
        budget(),
    )
    .expect("closeness") else {
        panic!("closeness scores expected")
    };
    assert!(closeness[&vertex(3)] > closeness[&vertex(5)]);

    let AlgorithmResult::Scores(betweenness) = run_builtin(
        &graph,
        &request(BuiltInAlgorithmId::BetweennessCentrality),
        budget(),
    )
    .expect("betweenness") else {
        panic!("betweenness scores expected")
    };
    assert!(betweenness[&vertex(3)] > betweenness[&vertex(2)]);
}

#[test]
fn triangle_clustering_and_k_core_match_oracles() {
    let graph = graph();
    assert_eq!(
        run_builtin(
            &graph,
            &request(BuiltInAlgorithmId::TriangleCount),
            budget(),
        )
        .expect("triangles"),
        AlgorithmResult::Count(1)
    );

    let AlgorithmResult::Scores(coefficients) = run_builtin(
        &graph,
        &request(BuiltInAlgorithmId::ClusteringCoefficient),
        budget(),
    )
    .expect("clustering") else {
        panic!("clustering scores expected")
    };
    assert_close(coefficients[&vertex(2)], 1.0);

    let AlgorithmResult::CoreNumbers(cores) =
        run_builtin(&graph, &request(BuiltInAlgorithmId::KCore), budget()).expect("k-core")
    else {
        panic!("core numbers expected")
    };
    assert_eq!(cores[&vertex(1)], 2);
    assert_eq!(cores[&vertex(2)], 2);
    assert_eq!(cores[&vertex(3)], 2);
}

#[test]
fn label_propagation_and_louvain_are_stable_closed_community_algorithms() {
    let graph = graph();
    let mut labels = request(BuiltInAlgorithmId::LabelPropagation);
    labels.iterations = 12;
    let first = run_builtin(&graph, &labels, budget()).expect("label propagation");
    let second = run_builtin(&graph, &labels, budget()).expect("repeat label propagation");
    assert_eq!(first, second);
    let AlgorithmResult::Communities(labels) = first else {
        panic!("communities expected")
    };
    assert_eq!(labels[&vertex(1)], labels[&vertex(2)]);
    assert_eq!(labels[&vertex(4)], labels[&vertex(5)]);

    let mut louvain = request(BuiltInAlgorithmId::Louvain);
    louvain.iterations = 12;
    let first = run_builtin(&graph, &louvain, budget()).expect("Louvain");
    let second = run_builtin(&graph, &louvain, budget()).expect("repeat Louvain");
    assert_eq!(first, second);
}

#[test]
fn temporal_path_reachability_motif_and_change_point_match_oracles() {
    let graph = graph();
    let mut earliest = request(BuiltInAlgorithmId::EarliestArrival);
    earliest.source = Some(vertex(1));
    earliest.target = Some(vertex(5));
    earliest.departure_property = Some("departure".into());
    earliest.arrival_property = Some("arrival".into());
    earliest.time_start = Some(0);
    earliest.time_end = Some(13);
    let AlgorithmResult::TemporalPath(Some(path)) =
        run_builtin(&graph, &earliest, budget()).expect("earliest arrival")
    else {
        panic!("temporal path expected")
    };
    assert_eq!(
        path.vertices,
        vec![vertex(1), vertex(2), vertex(3), vertex(4), vertex(5)]
    );
    assert_eq!(path.departure_time, 1);
    assert_eq!(path.arrival_time, 13);

    let mut latest = request(BuiltInAlgorithmId::LatestDeparture);
    latest.source = Some(vertex(1));
    latest.target = Some(vertex(5));
    latest.departure_property = Some("departure".into());
    latest.arrival_property = Some("arrival".into());
    latest.time_start = Some(0);
    latest.time_end = Some(13);
    let AlgorithmResult::TemporalPath(Some(path)) =
        run_builtin(&graph, &latest, budget()).expect("latest departure")
    else {
        panic!("temporal path expected")
    };
    assert_eq!(
        path.vertices,
        vec![vertex(1), vertex(3), vertex(4), vertex(5)]
    );
    assert_eq!(path.departure_time, 2);
    assert_eq!(path.arrival_time, 13);

    let mut reachability = request(BuiltInAlgorithmId::TemporalReachability);
    reachability.source = Some(vertex(1));
    reachability.departure_property = Some("departure".into());
    reachability.arrival_property = Some("arrival".into());
    reachability.time_start = Some(0);
    reachability.time_end = Some(13);
    assert_eq!(
        run_builtin(&graph, &reachability, budget()).expect("temporal reachability"),
        AlgorithmResult::Reachable(vec![vertex(1), vertex(2), vertex(3), vertex(4), vertex(5),])
    );

    let mut motif = request(BuiltInAlgorithmId::TemporalMotif);
    motif.departure_property = Some("departure".into());
    motif.arrival_property = Some("arrival".into());
    motif.time_start = Some(0);
    motif.time_end = Some(13);
    let AlgorithmResult::Count(count) = run_builtin(&graph, &motif, budget()).expect("motif")
    else {
        panic!("motif count expected")
    };
    assert!(count > 0);

    let mut change = request(BuiltInAlgorithmId::ChangePoint);
    change.event_time_property = Some("event_time".into());
    change.signal_property = Some("signal".into());
    change.change_threshold = 5.0;
    assert_eq!(
        run_builtin(&graph, &change, budget()).expect("change point"),
        AlgorithmResult::ChangePoints(vec![edge(3)])
    );
}

#[test]
fn every_catalog_entry_observes_cancellation_before_dispatch_work() {
    let graph = graph();
    for id in ALL_ALGORITHMS {
        let cancellation = CancellationToken::new();
        cancellation.cancel();
        let cancelled = AlgorithmBudget::new(128 * 1024, 64, 100_000, cancellation);
        assert_eq!(
            run_builtin(&graph, &request(id), cancelled).expect_err("cancelled built-in"),
            AnalyticsError::Cancelled,
            "{} did not observe cancellation",
            id.as_str()
        );
    }
}

#[test]
fn algorithms_fail_closed_on_iteration_work_memory_and_shape_limits() {
    let graph = graph();
    let mut page_rank = request(BuiltInAlgorithmId::PageRank);
    page_rank.iterations = 5;
    assert_eq!(
        run_builtin(
            &graph,
            &page_rank,
            AlgorithmBudget::new(128 * 1024, 4, 100_000, CancellationToken::new()),
        )
        .expect_err("iteration cap"),
        AnalyticsError::IterationLimit
    );

    let mut bfs = request(BuiltInAlgorithmId::BreadthFirstSearch);
    bfs.source = Some(vertex(1));
    assert_eq!(
        run_builtin(
            &graph,
            &bfs,
            AlgorithmBudget::new(128 * 1024, 64, 1, CancellationToken::new()),
        )
        .expect_err("work cap"),
        AnalyticsError::ResourceLimit
    );
    assert_eq!(
        run_builtin(
            &graph,
            &bfs,
            AlgorithmBudget::new(1, 64, 100_000, CancellationToken::new()),
        )
        .expect_err("memory cap"),
        AnalyticsError::ResourceLimit
    );

    let forward_only = {
        let snapshot = snapshot();
        let shard_id = ShardId::new(1).expect("shard");
        let fence = *snapshot.shards().get(&shard_id).expect("fence");
        SnapshotCsr::assemble(
            vec![ShardProjectionPart {
                snapshot,
                provenance: PartitionProvenance {
                    shard_id,
                    placement_epoch: fence.placement_epoch,
                    backend_generation: fence.backend_generation,
                    applied_index: fence.applied_index,
                    partition_index: 0,
                    partition_count: 1,
                },
                vertices: vec![
                    ProjectedVertex {
                        id: vertex(1),
                        properties: BTreeMap::new(),
                    },
                    ProjectedVertex {
                        id: vertex(2),
                        properties: BTreeMap::new(),
                    },
                ],
                edges: vec![projected_edge(1, 1, 2, 1, 1, 2, 1)],
            }],
            ProjectionSpec::new(
                false,
                Vec::new(),
                ["weight", "departure", "arrival", "event_time", "signal"]
                    .into_iter()
                    .map(|name| PropertyColumnSpec::required(name, PropertyType::Integer))
                    .collect(),
            )
            .expect("spec"),
            ProjectionBudget::new(64 * 1024, 0, CancellationToken::new()),
        )
        .expect("forward-only graph")
    };
    assert_eq!(
        run_builtin(
            &forward_only,
            &request(BuiltInAlgorithmId::StronglyConnectedComponents),
            budget(),
        )
        .expect_err("SCC declares reverse requirement"),
        AnalyticsError::MissingReverseCsr
    );
}
