use analytics_api::{EventEdge, EventGraph, SnapshotEdge, SnapshotGraph, VertexId};
use analytics_runtime::{
    TemporalPathRequest, TimeOrder, WaitingPolicy, bfs, earliest_arrival, page_rank, wcc,
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
