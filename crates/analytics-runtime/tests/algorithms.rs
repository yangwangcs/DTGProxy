use std::collections::BTreeMap;

use analytics_api::{
    DeltaEdge, DeltaGraph, DeltaKind, DeltaVertex, EdgeId, EventEdge, EventGraph, IntervalEdge,
    IntervalGraph, IntervalVertex, SnapshotEdge, SnapshotGraph, VertexId,
};
use analytics_runtime::{
    AlgorithmError, DeltaEntityType, MAX_TEMPORAL_MOTIF_EVENTS, MAX_TEMPORAL_MOTIF_TRIPLES,
    TemporalPathRequest, TimeOrder, WaitingPolicy, all_pairs_shortest_paths_cancellable,
    betweenness_centrality_cancellable, bfs, change_point_scores_cancellable,
    closeness_centrality_cancellable, clustering_coefficient, degree_centrality,
    delta_summary_cancellable, dfs_cancellable, earliest_arrival, interval_components_cancellable,
    k_core, label_propagation, latest_departure, louvain_communities_cancellable,
    min_hop_temporal_path, page_rank, scc, sssp, temporal_motif_count_cancellable,
    temporal_reachability, triangle_count, wcc, windowed_components_cancellable,
    windowed_triangle_count_cancellable,
};
use temporal_types::{CanonicalElement, Interval, ValidTime};

#[test]
fn runs_deterministic_snapshot_algorithms() {
    let graph =
        SnapshotGraph::new(ids(&[1, 2, 3, 4]), vec![edge(1, 2), edge(2, 3)], true).expect("graph");
    let traversal = bfs(&graph, VertexId::new(1)).expect("bfs");
    assert_eq!(traversal.distance(VertexId::new(3)), Some(2));
    assert_eq!(traversal.distance(VertexId::new(4)), None);

    let components = wcc(&graph);
    assert_eq!(components.get(&VertexId::new(3)), Some(&VertexId::new(1)));
    assert_eq!(components.get(&VertexId::new(4)), Some(&VertexId::new(4)));

    let ranks = page_rank(&graph, 0.85, 100, 1e-12).expect("PageRank");
    let total = ranks.values().sum::<f64>();
    assert!((total - 1.0).abs() < 1e-9);
    assert!(ranks[&VertexId::new(3)] > ranks[&VertexId::new(1)]);
}

#[test]
fn runs_weighted_shortest_paths_scc_and_degree() {
    let graph = SnapshotGraph::new(
        ids(&[1, 2, 3, 4]),
        vec![
            SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 5.0).expect("edge"),
            SnapshotEdge::new(VertexId::new(1), VertexId::new(3), 1.0).expect("edge"),
            SnapshotEdge::new(VertexId::new(3), VertexId::new(2), 1.0).expect("edge"),
            SnapshotEdge::new(VertexId::new(2), VertexId::new(1), 1.0).expect("edge"),
        ],
        true,
    )
    .expect("graph");
    let shortest = sssp(&graph, VertexId::new(1)).expect("SSSP");
    assert_eq!(shortest.distance(VertexId::new(2)), Some(2.0));
    assert_eq!(
        shortest.predecessor(VertexId::new(2)),
        Some(VertexId::new(3))
    );

    let components = scc(&graph);
    assert_eq!(components[&VertexId::new(1)], components[&VertexId::new(3)]);
    assert_ne!(components[&VertexId::new(1)], components[&VertexId::new(4)]);

    let degree = degree_centrality(&graph);
    assert_eq!(degree[&VertexId::new(1)].outgoing(), 2);
    assert_eq!(degree[&VertexId::new(1)].incoming(), 1);
}

#[test]
fn dfs_apsp_and_ordinary_centralities_match_small_graph_oracles() {
    let graph = SnapshotGraph::new(
        ids(&[1, 2, 3, 4]),
        vec![
            SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(2), VertexId::new(3), 2.0).unwrap(),
            SnapshotEdge::new(VertexId::new(1), VertexId::new(4), 10.0).unwrap(),
        ],
        false,
    )
    .unwrap();

    let dfs = dfs_cancellable(&graph, VertexId::new(1), || false).unwrap();
    assert_eq!(dfs.depth(VertexId::new(1)), Some(0));
    assert_eq!(dfs.predecessor(VertexId::new(2)), Some(VertexId::new(1)));
    assert_eq!(dfs.predecessor(VertexId::new(3)), Some(VertexId::new(2)));

    let distances = all_pairs_shortest_paths_cancellable(&graph, 1_000, || false).unwrap();
    assert_eq!(
        distances.get(&(VertexId::new(1), VertexId::new(3))),
        Some(&3.0)
    );
    assert_eq!(
        all_pairs_shortest_paths_cancellable(&graph, 1, || false),
        Err(AlgorithmError::AllPairsCapacity)
    );

    let path = SnapshotGraph::new(ids(&[1, 2, 3]), vec![edge(1, 2), edge(2, 3)], false).unwrap();
    let betweenness = betweenness_centrality_cancellable(&path, || false).unwrap();
    assert_eq!(betweenness[&VertexId::new(1)], 0.0);
    assert_eq!(betweenness[&VertexId::new(2)], 1.0);
    assert_eq!(betweenness[&VertexId::new(3)], 0.0);
    let closeness = closeness_centrality_cancellable(&path, 1_000, || false).unwrap();
    assert!((closeness[&VertexId::new(1)] - (2.0 / 3.0)).abs() < 1e-12);
    assert_eq!(closeness[&VertexId::new(2)], 1.0);
    assert!((closeness[&VertexId::new(3)] - (2.0 / 3.0)).abs() < 1e-12);
}

#[test]
fn deterministic_louvain_separates_disconnected_dense_groups() {
    let graph = SnapshotGraph::new(
        ids(&[1, 2, 3, 4, 5, 6]),
        vec![
            edge(1, 2),
            edge(1, 3),
            edge(2, 3),
            edge(4, 5),
            edge(4, 6),
            edge(5, 6),
        ],
        false,
    )
    .unwrap();

    let communities = louvain_communities_cancellable(&graph, 10, 20, 1.0, || false).unwrap();

    assert_eq!(communities[&VertexId::new(1)], VertexId::new(1));
    assert_eq!(communities[&VertexId::new(2)], VertexId::new(1));
    assert_eq!(communities[&VertexId::new(3)], VertexId::new(1));
    assert_eq!(communities[&VertexId::new(4)], VertexId::new(4));
    assert_eq!(communities[&VertexId::new(5)], VertexId::new(4));
    assert_eq!(communities[&VertexId::new(6)], VertexId::new(4));
}

#[test]
fn weighted_betweenness_uses_shortest_weight_not_hop_count() {
    let graph = SnapshotGraph::new(
        ids(&[1, 2, 3]),
        vec![
            SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 10.0).unwrap(),
            SnapshotEdge::new(VertexId::new(1), VertexId::new(3), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(3), VertexId::new(2), 1.0).unwrap(),
        ],
        false,
    )
    .unwrap();

    let scores = betweenness_centrality_cancellable(&graph, || false).unwrap();

    assert_eq!(scores[&VertexId::new(1)], 0.0);
    assert_eq!(scores[&VertexId::new(2)], 0.0);
    assert_eq!(scores[&VertexId::new(3)], 1.0);
}

#[test]
fn weighted_betweenness_rejects_non_positive_weights() {
    for weight in [-1.0, 0.0] {
        let graph = SnapshotGraph::new(
            ids(&[1, 2]),
            vec![SnapshotEdge::new(VertexId::new(1), VertexId::new(2), weight).unwrap()],
            true,
        )
        .unwrap();

        assert_eq!(
            betweenness_centrality_cancellable(&graph, || false),
            Err(AlgorithmError::InvalidBetweennessWeight)
        );
    }
}

#[test]
fn weighted_betweenness_counts_parallel_shortest_paths_by_edge() {
    let graph = SnapshotGraph::new(
        ids(&[1, 2, 3]),
        vec![
            SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(2), VertexId::new(3), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(1), VertexId::new(3), 2.0).unwrap(),
        ],
        true,
    )
    .unwrap();

    let scores = betweenness_centrality_cancellable(&graph, || false).unwrap();

    assert!((scores[&VertexId::new(2)] - 2.0 / 3.0).abs() < 1e-12);
}

#[test]
fn louvain_aggregation_does_not_treat_supernode_self_loops_as_stay_bias() {
    let graph = SnapshotGraph::new(
        ids(&[1, 2, 3, 4, 5]),
        vec![
            edge(1, 2),
            edge(1, 3),
            edge(1, 4),
            edge(1, 5),
            edge(2, 4),
            edge(2, 5),
        ],
        false,
    )
    .unwrap();

    let communities = louvain_communities_cancellable(&graph, 10, 20, 1.0, || false).unwrap();

    assert!(
        communities
            .values()
            .all(|community| *community == VertexId::new(1)),
        "the second Louvain level must merge the negative-modularity first-level split: {communities:?}"
    );
}

#[test]
fn earliest_arrival_obeys_time_order_waiting_and_window() {
    let graph = EventGraph::new(
        ids(&[1, 2, 3]),
        vec![event(1, 2, 10, 5), event(2, 3, 15, 2), event(2, 3, 16, 1)],
    )
    .expect("event graph");
    let non_decreasing = earliest_arrival(
        &graph,
        TemporalPathRequest::new(
            VertexId::new(1),
            ValidTime::from_micros(0),
            ValidTime::from_micros(30),
            TimeOrder::NonDecreasing,
            WaitingPolicy::Allowed,
        )
        .expect("request"),
    )
    .expect("journey");
    assert_eq!(
        non_decreasing.arrival(VertexId::new(3)),
        Some(ValidTime::from_micros(17))
    );
    assert_eq!(
        non_decreasing.predecessor(VertexId::new(3)),
        Some(VertexId::new(2))
    );

    let strict = earliest_arrival(
        &graph,
        TemporalPathRequest::new(
            VertexId::new(1),
            ValidTime::from_micros(0),
            ValidTime::from_micros(30),
            TimeOrder::Strict,
            WaitingPolicy::Allowed,
        )
        .expect("request"),
    )
    .expect("strict journey");
    assert_eq!(
        strict.arrival(VertexId::new(3)),
        Some(ValidTime::from_micros(17))
    );
}

#[test]
fn snapshot_structural_algorithms_are_available() {
    let graph = SnapshotGraph::new(
        ids(&[1, 2, 3]),
        vec![edge(1, 2), edge(2, 3), edge(1, 3)],
        false,
    )
    .expect("graph");
    assert_eq!(triangle_count(&graph), 1);
    assert_eq!(clustering_coefficient(&graph)[&VertexId::new(1)], 1.0);
    assert_eq!(k_core(&graph, 2).len(), 3);
    assert_eq!(label_propagation(&graph, 20).unwrap().len(), 3);
}

#[test]
fn temporal_reachability_and_path_objectives_are_available() {
    let graph = EventGraph::new(ids(&[1, 2, 3]), vec![event(1, 2, 1, 2), event(2, 3, 4, 1)])
        .expect("event graph");
    let request = TemporalPathRequest::new(
        VertexId::new(1),
        ValidTime::from_micros(0),
        ValidTime::from_micros(10),
        TimeOrder::NonDecreasing,
        WaitingPolicy::Allowed,
    )
    .expect("request");
    assert!(temporal_reachability(&graph, request).unwrap()[&VertexId::new(3)]);
    assert_eq!(
        min_hop_temporal_path(&graph, request).unwrap()[&VertexId::new(3)],
        2
    );
    assert_eq!(
        latest_departure(&graph, VertexId::new(3), ValidTime::from_micros(10)).unwrap()
            [&VertexId::new(1)],
        ValidTime::from_micros(1)
    );
}

#[test]
fn windowed_components_triangles_and_change_points_match_small_graph_oracles() {
    let graph = EventGraph::new(
        ids(&[1, 2, 3, 4]),
        vec![
            event(1, 2, 1, 0),
            event(2, 3, 2, 0),
            event(3, 1, 3, 0),
            event(1, 2, 21, 0),
        ],
    )
    .unwrap();
    let components = windowed_components_cancellable(
        &graph,
        ValidTime::from_micros(0),
        ValidTime::from_micros(10),
        || false,
    )
    .unwrap();
    assert_eq!(components[&VertexId::new(3)], VertexId::new(1));
    assert_eq!(components[&VertexId::new(4)], VertexId::new(4));
    assert_eq!(
        windowed_triangle_count_cancellable(
            &graph,
            ValidTime::from_micros(0),
            ValidTime::from_micros(10),
            || false,
        )
        .unwrap(),
        1
    );

    let scores = change_point_scores_cancellable(
        &graph,
        ValidTime::from_micros(0),
        ValidTime::from_micros(10),
        ValidTime::from_micros(20),
        ValidTime::from_micros(30),
        || false,
    )
    .unwrap();
    assert_eq!(scores[&VertexId::new(4)], 0.0);
    assert!((scores[&VertexId::new(1)] - (1.0 / 3.0)).abs() < 1e-12);
    assert_eq!(scores[&VertexId::new(3)], 1.0);
}

#[test]
fn change_points_return_time_overflow_for_unrepresentable_window_durations() {
    let graph = EventGraph::new(ids(&[1]), Vec::new()).unwrap();

    assert_eq!(
        change_point_scores_cancellable(
            &graph,
            ValidTime::from_micros(i64::MIN),
            ValidTime::from_micros(0),
            ValidTime::from_micros(1),
            ValidTime::from_micros(2),
            || false,
        ),
        Err(AlgorithmError::TimeOverflow)
    );
    assert_eq!(
        change_point_scores_cancellable(
            &graph,
            ValidTime::from_micros(i64::MIN),
            ValidTime::from_micros(i64::MIN + 1),
            ValidTime::from_micros(-1),
            ValidTime::from_micros(i64::MAX),
            || false,
        ),
        Err(AlgorithmError::TimeOverflow)
    );
}

#[test]
fn temporal_motifs_are_canonical_and_capacity_bounded() {
    let graph = EventGraph::new(
        ids(&[1, 2, 3]),
        vec![event(1, 2, 1, 0), event(2, 3, 2, 0), event(3, 1, 3, 0)],
    )
    .unwrap();
    assert_eq!(
        temporal_motif_count_cancellable(
            &graph,
            ValidTime::from_micros(0),
            ValidTime::from_micros(10),
            10,
            || false,
        )
        .unwrap(),
        BTreeMap::from([("A>B|B>C|C>A".to_owned(), 1)])
    );

    let oversized = EventGraph::new(
        ids(&[1, 2]),
        (0..=MAX_TEMPORAL_MOTIF_EVENTS)
            .map(|time| event(1, 2, i64::try_from(time).unwrap(), 0))
            .collect(),
    )
    .unwrap();
    assert_eq!(
        temporal_motif_count_cancellable(
            &oversized,
            ValidTime::from_micros(0),
            ValidTime::from_micros(i64::MAX),
            10,
            || false,
        ),
        Err(AlgorithmError::TemporalMotifCapacity)
    );
}

#[test]
fn temporal_motif_triple_budget_bounds_parallel_equal_time_candidates() {
    assert_eq!(MAX_TEMPORAL_MOTIF_TRIPLES, 1_000_000);
    let graph = EventGraph::new(
        ids(&[1, 2]),
        (0..MAX_TEMPORAL_MOTIF_EVENTS)
            .map(|_| event(1, 2, 5, 0))
            .collect(),
    )
    .unwrap();

    assert_eq!(
        temporal_motif_count_cancellable(
            &graph,
            ValidTime::from_micros(5),
            ValidTime::from_micros(5),
            1,
            || false,
        ),
        Err(AlgorithmError::TemporalMotifCapacity)
    );
}

#[test]
fn temporal_motifs_accept_triples_whose_endpoint_union_is_connected() {
    let graph = EventGraph::new(
        ids(&[1, 2, 3]),
        vec![event(1, 2, 1, 0), event(2, 3, 2, 0), event(1, 1, 3, 0)],
    )
    .unwrap();

    assert_eq!(
        temporal_motif_count_cancellable(
            &graph,
            ValidTime::from_micros(0),
            ValidTime::from_micros(10),
            10,
            || false,
        )
        .unwrap(),
        BTreeMap::from([("A>B|B>C|A>A".to_owned(), 1)])
    );
}

#[test]
fn temporal_motifs_compare_extreme_event_spans_without_saturation() {
    let graph = EventGraph::new(
        ids(&[1, 2, 3]),
        vec![
            event(1, 2, i64::MIN, 0),
            event(2, 3, 0, 0),
            event(3, 1, i64::MAX, 0),
        ],
    )
    .unwrap();

    assert!(
        temporal_motif_count_cancellable(
            &graph,
            ValidTime::from_micros(i64::MIN),
            ValidTime::from_micros(i64::MAX),
            i64::MAX,
            || false,
        )
        .unwrap()
        .is_empty()
    );
}

#[test]
fn equal_time_motifs_are_invariant_under_vertex_renaming() {
    let original = EventGraph::new(
        ids(&[1, 2, 3]),
        vec![event(1, 2, 5, 0), event(2, 3, 5, 0), event(3, 1, 5, 0)],
    )
    .unwrap();
    let renamed = EventGraph::new(
        ids(&[10, 20, 30]),
        vec![
            event(20, 10, 5, 0),
            event(10, 30, 5, 0),
            event(30, 20, 5, 0),
        ],
    )
    .unwrap();

    let count = |graph| {
        temporal_motif_count_cancellable(
            graph,
            ValidTime::from_micros(0),
            ValidTime::from_micros(10),
            10,
            || false,
        )
        .unwrap()
    };
    assert_eq!(
        count(&original),
        BTreeMap::from([("A>B|B>C|C>A".to_owned(), 1)])
    );
    assert_eq!(count(&renamed), count(&original));
}

#[test]
fn equal_time_parallel_motifs_are_invariant_under_vertex_renaming() {
    let original = EventGraph::new(
        ids(&[1, 2]),
        vec![event(1, 2, 5, 0), event(1, 2, 5, 0), event(2, 1, 5, 0)],
    )
    .unwrap();
    let renamed = EventGraph::new(
        ids(&[10, 20]),
        vec![
            event(20, 10, 5, 0),
            event(20, 10, 5, 0),
            event(10, 20, 5, 0),
        ],
    )
    .unwrap();

    let count = |graph| {
        temporal_motif_count_cancellable(
            graph,
            ValidTime::from_micros(0),
            ValidTime::from_micros(10),
            10,
            || false,
        )
        .unwrap()
    };
    assert_eq!(
        count(&original),
        BTreeMap::from([("A>B|A>B|B>A".to_owned(), 1)])
    );
    assert_eq!(count(&renamed), count(&original));
}

#[test]
fn interval_components_and_delta_summary_consume_native_graph_models() {
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
                VertexId::new(1),
                VertexId::new(2),
                valid,
                payload,
                1.0,
            )
            .unwrap(),
        ],
        true,
    )
    .unwrap();
    let components = interval_components_cancellable(&interval, || false).unwrap();
    assert_eq!(components[&VertexId::new(2)], VertexId::new(1));
    assert_eq!(components[&VertexId::new(3)], VertexId::new(3));

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
    let summary = delta_summary_cancellable(&delta, || false).unwrap();
    assert_eq!(summary[&(DeltaEntityType::Vertex, DeltaKind::Added)], 1);
    assert_eq!(summary[&(DeltaEntityType::Vertex, DeltaKind::Updated)], 1);
    assert_eq!(summary[&(DeltaEntityType::Edge, DeltaKind::Removed)], 1);
}

#[test]
fn interval_components_emit_one_component_per_vertex_across_multiple_segments() {
    let valid = |start, end| {
        Interval::new(
            ValidTime::from_micros(start),
            Some(ValidTime::from_micros(end)),
        )
        .unwrap()
    };
    let payload = CanonicalElement::new(1, BTreeMap::new());
    let graph = IntervalGraph::new(
        vec![
            IntervalVertex::new(VertexId::new(1), valid(0, 5), payload.clone()),
            IntervalVertex::new(VertexId::new(1), valid(5, 10), payload.clone()),
            IntervalVertex::new(VertexId::new(2), valid(0, 10), payload.clone()),
            IntervalVertex::new(VertexId::new(3), valid(0, 4), payload.clone()),
            IntervalVertex::new(VertexId::new(3), valid(6, 10), payload.clone()),
        ],
        vec![
            IntervalEdge::new(
                EdgeId::new(9),
                VertexId::new(1),
                VertexId::new(2),
                valid(0, 10),
                payload,
                1.0,
            )
            .unwrap(),
        ],
        true,
    )
    .unwrap();

    assert_eq!(
        interval_components_cancellable(&graph, || false).unwrap(),
        BTreeMap::from([
            (VertexId::new(1), VertexId::new(1)),
            (VertexId::new(2), VertexId::new(1)),
            (VertexId::new(3), VertexId::new(3)),
        ])
    );
}

#[test]
fn new_temporal_algorithms_check_cancellation_inside_scan_loops() {
    let graph = EventGraph::new(
        ids(&[1, 2, 3]),
        vec![event(1, 2, 1, 0), event(2, 3, 2, 0), event(3, 1, 3, 0)],
    )
    .unwrap();
    assert_eq!(
        windowed_triangle_count_cancellable(
            &graph,
            ValidTime::from_micros(0),
            ValidTime::from_micros(10),
            || true,
        ),
        Err(AlgorithmError::Canceled)
    );
    assert_eq!(
        temporal_motif_count_cancellable(
            &graph,
            ValidTime::from_micros(0),
            ValidTime::from_micros(10),
            10,
            || true,
        ),
        Err(AlgorithmError::Canceled)
    );
}

fn ids(values: &[u128]) -> Vec<VertexId> {
    values.iter().copied().map(VertexId::new).collect()
}

fn edge(source: u128, destination: u128) -> SnapshotEdge {
    SnapshotEdge::new(VertexId::new(source), VertexId::new(destination), 1.0).expect("edge")
}

fn event(source: u128, destination: u128, time: i64, duration: i64) -> EventEdge {
    EventEdge::new(
        VertexId::new(source),
        VertexId::new(destination),
        ValidTime::from_micros(time),
        duration,
        1.0,
    )
    .expect("event")
}
