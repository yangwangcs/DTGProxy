use analytics_api::{EventEdge, EventGraph, SnapshotEdge, SnapshotGraph, VertexId};
use analytics_runtime::{
    TemporalPathRequest, TimeOrder, WaitingPolicy, bfs, clustering_coefficient, degree_centrality,
    earliest_arrival, k_core, label_propagation, latest_departure, min_hop_temporal_path,
    page_rank, scc, sssp, temporal_reachability, triangle_count, wcc,
};
use temporal_types::ValidTime;

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
