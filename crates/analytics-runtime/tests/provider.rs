use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use analytics_api::{
    AlgorithmRequest, AlgorithmValue, AnalyticsOutput, AnalyticsProvider, DeltaEdge, DeltaGraph,
    DeltaKind, DeltaVertex, EdgeId, EventEdge, EventGraph, IntervalEdge, IntervalGraph,
    IntervalVertex, ProjectedGraph, ProviderError, SnapshotEdge, SnapshotGraph, VertexId,
};
use analytics_runtime::{
    AlgorithmError, BuiltInProvider, MAX_TEMPORAL_MOTIF_EVENTS,
    all_pairs_shortest_paths_cancellable, betweenness_centrality_cancellable,
    closeness_centrality_cancellable, dfs_cancellable, louvain_communities_cancellable,
    page_rank_cancellable,
};
use analytics_runtime::{bfs_cancellable, scc_cancellable, sssp_cancellable, wcc_cancellable};
use temporal_types::{CanonicalElement, Interval, ValidTime};

#[test]
fn builtin_provider_catalog_and_typed_execution_share_one_spi() {
    let provider = BuiltInProvider::new();
    let names = provider
        .algorithms()
        .into_iter()
        .map(|descriptor| descriptor.name().to_owned())
        .collect::<Vec<_>>();
    assert!(names.contains(&"dtg.graph.bfs".to_owned()));
    assert!(names.contains(&"dtg.temporal.earliestArrival".to_owned()));
    assert!(provider.descriptor().distributed());

    let graph = SnapshotGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        vec![SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).expect("edge")],
        true,
    )
    .expect("graph");
    let result = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.graph.bfs",
                ProjectedGraph::Snapshot(graph),
                BTreeMap::from([("source".into(), AlgorithmValue::Vertex(VertexId::new(1)))]),
            )
            .expect("request"),
        )
        .expect("execute");
    assert_eq!(result.columns(), &["vertexId", "distance", "predecessor"]);
    assert_eq!(result.rows().len(), 2);
}

#[test]
fn builtin_provider_exposes_complete_stable_ordinary_algorithm_catalog() {
    let provider = BuiltInProvider::new();
    let graph = SnapshotGraph::new(
        (1..=6).map(VertexId::new).collect(),
        vec![(1, 2), (1, 3), (2, 3), (4, 5), (4, 6), (5, 6)]
            .into_iter()
            .map(|(source, destination)| {
                SnapshotEdge::new(VertexId::new(source), VertexId::new(destination), 1.0).unwrap()
            })
            .collect(),
        false,
    )
    .unwrap();
    let execute = |algorithm: &str, parameters| {
        provider
            .execute(
                AlgorithmRequest::new(
                    algorithm,
                    ProjectedGraph::Snapshot(graph.clone()),
                    parameters,
                )
                .unwrap(),
            )
            .unwrap()
    };

    let dfs = execute(
        "dtg.graph.dfs",
        BTreeMap::from([("source".into(), AlgorithmValue::Vertex(VertexId::new(1)))]),
    );
    assert_eq!(dfs.columns(), &["vertexId", "depth", "predecessor"]);
    assert_eq!(dfs.rows().len(), 3);

    let all_pairs = execute("dtg.graph.allPairsShortestPath", BTreeMap::new());
    assert_eq!(all_pairs.columns(), &["source", "target", "distance"]);
    assert_eq!(all_pairs.rows().len(), 18);

    for algorithm in ["dtg.graph.betweenness", "dtg.graph.closeness"] {
        let result = execute(algorithm, BTreeMap::new());
        assert_eq!(result.columns(), &["vertexId", "score"]);
        assert_eq!(result.rows().len(), 6);
    }

    let louvain = execute("dtg.graph.louvain", BTreeMap::new());
    assert_eq!(louvain.columns(), &["vertexId", "communityId"]);
    assert_eq!(louvain.rows().len(), 6);
}

#[test]
fn builtin_provider_reports_invalid_betweenness_weights_as_parameters() {
    let provider = BuiltInProvider::new();
    let graph = SnapshotGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        vec![SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 0.0).unwrap()],
        true,
    )
    .unwrap();

    let error = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.graph.betweenness",
                ProjectedGraph::Snapshot(graph),
                BTreeMap::new(),
            )
            .unwrap(),
        )
        .unwrap_err();

    assert_eq!(error.code(), "DTG-ANALYTICS-PARAMETER");
}

#[derive(Default)]
struct StopAfterOneRow {
    attempts: usize,
}

impl AnalyticsOutput for StopAfterOneRow {
    fn declare_columns(&mut self, _columns: Vec<String>) -> Result<(), ProviderError> {
        Ok(())
    }

    fn push_row(&mut self, _row: Vec<AlgorithmValue>) -> Result<(), ProviderError> {
        self.attempts += 1;
        if self.attempts > 1 {
            return Err(ProviderError::new("DTG-TEST-STOP", "stop output"));
        }
        Ok(())
    }
}

#[test]
fn builtin_provider_stops_generating_rows_when_the_output_sink_rejects_one() {
    let provider = BuiltInProvider::new();
    let graph = SnapshotGraph::new(
        vec![VertexId::new(1), VertexId::new(2), VertexId::new(3)],
        Vec::new(),
        true,
    )
    .unwrap();
    let request = AlgorithmRequest::new(
        "dtg.graph.degree",
        ProjectedGraph::Snapshot(graph),
        BTreeMap::new(),
    )
    .unwrap();
    let mut output = StopAfterOneRow::default();

    let error = provider.execute_into(request, &mut output).unwrap_err();

    assert_eq!(error.code(), "DTG-TEST-STOP");
    assert_eq!(output.attempts, 2);
}

#[test]
fn page_rank_cancellation_is_checked_between_iterations() {
    let graph = SnapshotGraph::new(
        (1..=4).map(VertexId::new).collect(),
        vec![
            SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(2), VertexId::new(3), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(3), VertexId::new(4), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(4), VertexId::new(4), 1.0).unwrap(),
        ],
        true,
    )
    .unwrap();
    let checks = AtomicUsize::new(0);
    let error = page_rank_cancellable(&graph, 0.85, 100, 1e-30, || {
        checks.fetch_add(1, Ordering::Relaxed) >= 2
    })
    .expect_err("cancellation must stop a long iteration before its configured limit");
    assert_eq!(error, AlgorithmError::Canceled);
    assert!(checks.load(Ordering::Relaxed) < 100);
}

#[test]
fn bfs_and_sssp_check_cancellation_inside_their_frontier_loops() {
    let graph = cancellation_graph();
    let bfs_checks = AtomicUsize::new(0);
    assert_eq!(
        bfs_cancellable(&graph, VertexId::new(1), || {
            bfs_checks.fetch_add(1, Ordering::Relaxed) >= 1
        })
        .map(|_| ()),
        Err(AlgorithmError::Canceled)
    );
    assert!(bfs_checks.load(Ordering::Relaxed) >= 2);

    let sssp_checks = AtomicUsize::new(0);
    assert_eq!(
        sssp_cancellable(&graph, VertexId::new(1), || {
            sssp_checks.fetch_add(1, Ordering::Relaxed) >= 1
        })
        .map(|_| ()),
        Err(AlgorithmError::Canceled)
    );
    assert!(sssp_checks.load(Ordering::Relaxed) >= 2);
}

#[test]
fn local_wcc_and_scc_check_cancellation_inside_component_loops() {
    let graph = cancellation_graph();
    let wcc_checks = AtomicUsize::new(0);
    assert_eq!(
        wcc_cancellable(&graph, || {
            wcc_checks.fetch_add(1, Ordering::Relaxed) >= 1
        }),
        Err(AlgorithmError::Canceled)
    );
    assert!(wcc_checks.load(Ordering::Relaxed) >= 2);

    let scc_checks = AtomicUsize::new(0);
    assert_eq!(
        scc_cancellable(&graph, || {
            scc_checks.fetch_add(1, Ordering::Relaxed) >= 1
        }),
        Err(AlgorithmError::Canceled)
    );
    assert!(scc_checks.load(Ordering::Relaxed) >= 2);
}

#[test]
fn local_algorithms_poll_cancellation_inside_dense_inner_loops() {
    let graph = SnapshotGraph::new(
        vec![VertexId::new(1)],
        (0..32)
            .map(|_| SnapshotEdge::new(VertexId::new(1), VertexId::new(1), 1.0).unwrap())
            .collect(),
        true,
    )
    .unwrap();
    let cancel_after_six_checks = || {
        let checks = AtomicUsize::new(0);
        move || checks.fetch_add(1, Ordering::Relaxed) >= 6
    };

    assert_eq!(
        bfs_cancellable(&graph, VertexId::new(1), cancel_after_six_checks()).map(|_| ()),
        Err(AlgorithmError::Canceled)
    );
    assert_eq!(
        sssp_cancellable(&graph, VertexId::new(1), cancel_after_six_checks()).map(|_| ()),
        Err(AlgorithmError::Canceled)
    );
    assert_eq!(
        wcc_cancellable(&graph, cancel_after_six_checks()),
        Err(AlgorithmError::Canceled)
    );
    assert_eq!(
        scc_cancellable(&graph, cancel_after_six_checks()),
        Err(AlgorithmError::Canceled)
    );
    assert_eq!(
        page_rank_cancellable(&graph, 0.85, 100, 1e-9, cancel_after_six_checks()),
        Err(AlgorithmError::Canceled)
    );
}

#[test]
fn added_stable_algorithms_poll_cancellation_inside_dense_inner_loops() {
    let graph = SnapshotGraph::new(
        vec![VertexId::new(1)],
        (0..32)
            .map(|_| SnapshotEdge::new(VertexId::new(1), VertexId::new(1), 1.0).unwrap())
            .collect(),
        true,
    )
    .unwrap();
    let cancel_after_six_checks = || {
        let checks = AtomicUsize::new(0);
        move || checks.fetch_add(1, Ordering::Relaxed) >= 6
    };

    assert_eq!(
        dfs_cancellable(&graph, VertexId::new(1), cancel_after_six_checks()).map(|_| ()),
        Err(AlgorithmError::Canceled)
    );
    assert_eq!(
        all_pairs_shortest_paths_cancellable(&graph, 1_000, cancel_after_six_checks()).map(|_| ()),
        Err(AlgorithmError::Canceled)
    );
    assert_eq!(
        betweenness_centrality_cancellable(&graph, cancel_after_six_checks()).map(|_| ()),
        Err(AlgorithmError::Canceled)
    );
    assert_eq!(
        closeness_centrality_cancellable(&graph, 1_000, cancel_after_six_checks()).map(|_| ()),
        Err(AlgorithmError::Canceled)
    );
    assert_eq!(
        louvain_communities_cancellable(&graph, 10, 20, 1.0, cancel_after_six_checks()).map(|_| ()),
        Err(AlgorithmError::Canceled)
    );
}

fn cancellation_graph() -> SnapshotGraph {
    SnapshotGraph::new(
        (1..=4).map(VertexId::new).collect(),
        vec![
            SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(2), VertexId::new(3), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(3), VertexId::new(4), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(4), VertexId::new(1), 1.0).unwrap(),
        ],
        true,
    )
    .unwrap()
}

#[test]
fn builtin_provider_exposes_fastest_path_and_temporal_degree() {
    let provider = BuiltInProvider::new();
    let graph = EventGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        vec![
            EventEdge::new(
                VertexId::new(1),
                VertexId::new(2),
                ValidTime::from_micros(10),
                2,
                1.0,
            )
            .expect("event"),
        ],
    )
    .expect("graph");
    let request = |algorithm| {
        AlgorithmRequest::new(
            algorithm,
            ProjectedGraph::Event(graph.clone()),
            BTreeMap::from([
                ("source".into(), AlgorithmValue::Vertex(VertexId::new(1))),
                (
                    "validFrom".into(),
                    AlgorithmValue::Time(ValidTime::from_micros(0)),
                ),
                (
                    "validTo".into(),
                    AlgorithmValue::Time(ValidTime::from_micros(20)),
                ),
            ]),
        )
        .expect("request")
    };
    let fastest = provider
        .execute(request("dtg.temporal.fastestPath"))
        .expect("fastest");
    assert_eq!(
        fastest.columns(),
        &["vertexId", "travelTime", "arrivalTime"]
    );
    assert!(
        provider
            .execute(request("dtg.temporal.degree"))
            .expect("degree")
            .rows()
            .len()
            >= 2
    );
}

#[test]
fn builtin_provider_runs_temporal_page_rank_on_the_requested_event_window() {
    let provider = BuiltInProvider::new();
    let graph = EventGraph::new(
        vec![VertexId::new(1), VertexId::new(2), VertexId::new(3)],
        vec![
            EventEdge::new(
                VertexId::new(1),
                VertexId::new(2),
                ValidTime::from_micros(5),
                0,
                1.0,
            )
            .unwrap(),
            EventEdge::new(
                VertexId::new(2),
                VertexId::new(3),
                ValidTime::from_micros(10),
                0,
                1.0,
            )
            .unwrap(),
            EventEdge::new(
                VertexId::new(3),
                VertexId::new(1),
                ValidTime::from_micros(50),
                0,
                1.0,
            )
            .unwrap(),
        ],
    )
    .unwrap();
    let result = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.temporal.pageRank",
                ProjectedGraph::Event(graph),
                BTreeMap::from([
                    (
                        "validFrom".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(0)),
                    ),
                    (
                        "validTo".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(20)),
                    ),
                ]),
            )
            .unwrap(),
        )
        .expect("temporal PageRank");

    assert_eq!(result.columns(), &["vertexId", "score"]);
    assert_eq!(result.rows().len(), 3);
    let total = result
        .rows()
        .iter()
        .map(|row| match row.as_slice() {
            [AlgorithmValue::Vertex(_), AlgorithmValue::FloatBits(score)] => f64::from_bits(*score),
            other => panic!("unexpected temporal PageRank row: {other:?}"),
        })
        .sum::<f64>();
    assert!((total - 1.0).abs() < 1e-9, "rank mass must be normalized");
}

#[test]
fn builtin_provider_computes_temporal_burstiness_from_inter_event_times() {
    let provider = BuiltInProvider::new();
    let graph = EventGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        [0, 10, 20]
            .into_iter()
            .map(|time| {
                EventEdge::new(
                    VertexId::new(1),
                    VertexId::new(2),
                    ValidTime::from_micros(time),
                    0,
                    1.0,
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let result = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.temporal.burstiness",
                ProjectedGraph::Event(graph),
                BTreeMap::from([
                    (
                        "validFrom".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(0)),
                    ),
                    (
                        "validTo".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(20)),
                    ),
                ]),
            )
            .unwrap(),
        )
        .expect("temporal burstiness");

    assert_eq!(result.columns(), &["vertexId", "score"]);
    assert!(result.rows().iter().any(|row| matches!(
        row.as_slice(),
        [AlgorithmValue::Vertex(vertex), AlgorithmValue::FloatBits(score)]
            if *vertex == VertexId::new(1) && f64::from_bits(*score) == -1.0
    )));
}

#[test]
fn builtin_provider_computes_temporal_clustering_in_the_requested_window() {
    let provider = BuiltInProvider::new();
    let graph = EventGraph::new(
        vec![VertexId::new(1), VertexId::new(2), VertexId::new(3)],
        vec![(1, 2, 5), (2, 3, 10), (1, 3, 15), (3, 1, 50)]
            .into_iter()
            .map(|(source, destination, time)| {
                EventEdge::new(
                    VertexId::new(source),
                    VertexId::new(destination),
                    ValidTime::from_micros(time),
                    0,
                    1.0,
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let result = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.temporal.clusteringCoefficient",
                ProjectedGraph::Event(graph),
                BTreeMap::from([
                    (
                        "validFrom".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(0)),
                    ),
                    (
                        "validTo".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(20)),
                    ),
                ]),
            )
            .unwrap(),
        )
        .expect("temporal clustering");

    assert_eq!(result.columns(), &["vertexId", "coefficient"]);
    assert!(result.rows().iter().all(|row| matches!(
        row.as_slice(),
        [AlgorithmValue::Vertex(_), AlgorithmValue::FloatBits(value)]
            if f64::from_bits(*value) == 1.0
    )));
}

#[test]
fn builtin_provider_computes_topological_overlap_across_explicit_windows() {
    let provider = BuiltInProvider::new();
    let graph = EventGraph::new(
        (1..=4).map(VertexId::new).collect(),
        vec![(1, 2, 5), (1, 3, 10), (1, 2, 25), (1, 4, 30)]
            .into_iter()
            .map(|(source, destination, time)| {
                EventEdge::new(
                    VertexId::new(source),
                    VertexId::new(destination),
                    ValidTime::from_micros(time),
                    0,
                    1.0,
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let result = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.temporal.topologicalOverlap",
                ProjectedGraph::Event(graph),
                BTreeMap::from([
                    (
                        "firstFrom".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(0)),
                    ),
                    (
                        "firstTo".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(20)),
                    ),
                    (
                        "secondFrom".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(21)),
                    ),
                    (
                        "secondTo".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(40)),
                    ),
                ]),
            )
            .unwrap(),
        )
        .expect("topological overlap");

    assert_eq!(result.columns(), &["vertexId", "score"]);
    assert!(result.rows().iter().any(|row| matches!(
        row.as_slice(),
        [AlgorithmValue::Vertex(vertex), AlgorithmValue::FloatBits(score)]
            if *vertex == VertexId::new(1) && f64::from_bits(*score) == 0.5
    )));
}

#[test]
fn builtin_provider_returns_typed_results_for_new_event_algorithms() {
    let provider = BuiltInProvider::new();
    let graph = EventGraph::new(
        (1..=4).map(VertexId::new).collect(),
        vec![(1, 2, 1), (2, 3, 2), (3, 1, 3), (1, 2, 21)]
            .into_iter()
            .map(|(source, destination, time)| {
                EventEdge::new(
                    VertexId::new(source),
                    VertexId::new(destination),
                    ValidTime::from_micros(time),
                    0,
                    1.0,
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let execute = |algorithm, parameters| {
        provider
            .execute(
                AlgorithmRequest::new(algorithm, ProjectedGraph::Event(graph.clone()), parameters)
                    .unwrap(),
            )
            .unwrap()
    };
    let window = BTreeMap::from([
        (
            "validFrom".into(),
            AlgorithmValue::Time(ValidTime::from_micros(0)),
        ),
        (
            "validTo".into(),
            AlgorithmValue::Time(ValidTime::from_micros(10)),
        ),
    ]);

    let components = execute("dtg.temporal.windowedComponents", window.clone());
    assert_eq!(components.columns(), &["vertexId", "componentId"]);
    assert_eq!(
        components.rows(),
        &[
            vec![
                AlgorithmValue::Vertex(VertexId::new(1)),
                AlgorithmValue::Vertex(VertexId::new(1)),
            ],
            vec![
                AlgorithmValue::Vertex(VertexId::new(2)),
                AlgorithmValue::Vertex(VertexId::new(1)),
            ],
            vec![
                AlgorithmValue::Vertex(VertexId::new(3)),
                AlgorithmValue::Vertex(VertexId::new(1)),
            ],
            vec![
                AlgorithmValue::Vertex(VertexId::new(4)),
                AlgorithmValue::Vertex(VertexId::new(4)),
            ],
        ]
    );

    let triangles = execute("dtg.temporal.windowedTriangleCount", window.clone());
    assert_eq!(triangles.columns(), &["triangleCount"]);
    assert_eq!(triangles.rows(), &[vec![AlgorithmValue::Integer(1)]]);

    let change_points = execute(
        "dtg.temporal.changePoint",
        BTreeMap::from([
            (
                "firstFrom".into(),
                AlgorithmValue::Time(ValidTime::from_micros(0)),
            ),
            (
                "firstTo".into(),
                AlgorithmValue::Time(ValidTime::from_micros(10)),
            ),
            (
                "secondFrom".into(),
                AlgorithmValue::Time(ValidTime::from_micros(20)),
            ),
            (
                "secondTo".into(),
                AlgorithmValue::Time(ValidTime::from_micros(30)),
            ),
        ]),
    );
    assert_eq!(change_points.columns(), &["vertexId", "score"]);
    let score = change_points
        .rows()
        .iter()
        .find_map(|row| match row.as_slice() {
            [
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::FloatBits(score),
            ] if *vertex == VertexId::new(1) => Some(f64::from_bits(*score)),
            _ => None,
        })
        .expect("vertex 1 change-point score");
    assert!((score - (1.0 / 3.0)).abs() < 1e-12);

    let motifs = execute("dtg.temporal.motifCount", window);
    assert_eq!(motifs.columns(), &["motif", "count"]);
    assert_eq!(
        motifs.rows(),
        &[vec![
            AlgorithmValue::String("A>B|B>C|C>A".into()),
            AlgorithmValue::Integer(1),
        ]]
    );
}

#[test]
fn builtin_provider_executes_interval_components_and_delta_summary() {
    let provider = BuiltInProvider::new();
    let valid = Interval::forever_from(ValidTime::from_micros(0));
    let payload = CanonicalElement::new(1, BTreeMap::new());
    let interval = IntervalGraph::new(
        vec![
            IntervalVertex::new(VertexId::new(1), valid, payload.clone()),
            IntervalVertex::new(VertexId::new(2), valid, payload.clone()),
            IntervalVertex::new(VertexId::new(3), valid, payload.clone()),
        ],
        vec![
            IntervalEdge::new(
                EdgeId::new(9),
                VertexId::new(2),
                VertexId::new(1),
                valid,
                payload,
                1.0,
            )
            .unwrap(),
        ],
        true,
    )
    .unwrap();
    let components = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.temporal.intervalComponents",
                ProjectedGraph::Interval(interval),
                BTreeMap::new(),
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(components.columns(), &["vertexId", "componentId"]);
    assert_eq!(
        components.rows(),
        &[
            vec![
                AlgorithmValue::Vertex(VertexId::new(1)),
                AlgorithmValue::Vertex(VertexId::new(1)),
            ],
            vec![
                AlgorithmValue::Vertex(VertexId::new(2)),
                AlgorithmValue::Vertex(VertexId::new(1)),
            ],
            vec![
                AlgorithmValue::Vertex(VertexId::new(3)),
                AlgorithmValue::Vertex(VertexId::new(3)),
            ],
        ]
    );

    let delta = DeltaGraph::new(
        vec![
            DeltaVertex::new(VertexId::new(1), DeltaKind::Added, None, None),
            DeltaVertex::new(VertexId::new(2), DeltaKind::Updated, None, None),
        ],
        vec![
            DeltaEdge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                DeltaKind::Removed,
                None,
                None,
                Some(1.0),
                None,
            )
            .unwrap(),
        ],
    );
    let summary = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.temporal.deltaSummary",
                ProjectedGraph::Delta(delta),
                BTreeMap::new(),
            )
            .unwrap(),
        )
        .unwrap();
    assert_eq!(summary.columns(), &["entityType", "change", "count"]);
    assert_eq!(
        summary.rows(),
        &[
            vec![
                AlgorithmValue::String("VERTEX".into()),
                AlgorithmValue::String("ADDED".into()),
                AlgorithmValue::Integer(1),
            ],
            vec![
                AlgorithmValue::String("VERTEX".into()),
                AlgorithmValue::String("UPDATED".into()),
                AlgorithmValue::Integer(1),
            ],
            vec![
                AlgorithmValue::String("EDGE".into()),
                AlgorithmValue::String("REMOVED".into()),
                AlgorithmValue::Integer(1),
            ],
        ]
    );
}

#[test]
fn builtin_provider_rejects_invalid_change_point_windows_and_motif_delta() {
    let provider = BuiltInProvider::new();
    let graph = EventGraph::new(vec![VertexId::new(1)], Vec::new()).unwrap();
    let change_point_error = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.temporal.changePoint",
                ProjectedGraph::Event(graph.clone()),
                BTreeMap::from([
                    (
                        "firstFrom".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(0)),
                    ),
                    (
                        "firstTo".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(10)),
                    ),
                    (
                        "secondFrom".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(10)),
                    ),
                    (
                        "secondTo".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(20)),
                    ),
                ]),
            )
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(change_point_error.code(), "DTG-ANALYTICS-TEMPORAL-WINDOW");

    let delta_error = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.temporal.motifCount",
                ProjectedGraph::Event(graph),
                BTreeMap::from([
                    (
                        "validFrom".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(0)),
                    ),
                    (
                        "validTo".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(10)),
                    ),
                    ("deltaMicros".into(), AlgorithmValue::Integer(0)),
                ]),
            )
            .unwrap(),
        )
        .unwrap_err();
    assert_eq!(delta_error.code(), "DTG-ANALYTICS-PARAMETER");
}

#[test]
fn builtin_provider_maps_temporal_motif_capacity_to_stable_error_code() {
    let provider = BuiltInProvider::new();
    let graph = EventGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        (0..=MAX_TEMPORAL_MOTIF_EVENTS)
            .map(|time| {
                EventEdge::new(
                    VertexId::new(1),
                    VertexId::new(2),
                    ValidTime::from_micros(i64::try_from(time).unwrap()),
                    0,
                    1.0,
                )
                .unwrap()
            })
            .collect(),
    )
    .unwrap();
    let error = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.temporal.motifCount",
                ProjectedGraph::Event(graph),
                BTreeMap::from([
                    (
                        "validFrom".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(0)),
                    ),
                    (
                        "validTo".into(),
                        AlgorithmValue::Time(ValidTime::from_micros(i64::MAX)),
                    ),
                    ("deltaMicros".into(), AlgorithmValue::Integer(10)),
                ]),
            )
            .unwrap(),
        )
        .unwrap_err();

    assert_eq!(error.code(), "DTG-ANALYTICS-CAPACITY");
}
