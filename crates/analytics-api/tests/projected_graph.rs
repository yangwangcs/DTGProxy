use analytics_api::{
    EventEdge, EventGraph, GraphProjectionError, SnapshotEdge, SnapshotGraph, VertexId,
};
use temporal_types::ValidTime;

#[test]
fn builds_deterministic_snapshot_and_event_adjacency() {
    let snapshot = SnapshotGraph::new(
        vec![VertexId::new(3), VertexId::new(1), VertexId::new(2)],
        vec![
            SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).expect("edge"),
            SnapshotEdge::new(VertexId::new(1), VertexId::new(3), 2.0).expect("edge"),
        ],
        true,
    )
    .expect("snapshot graph");
    assert_eq!(
        snapshot.vertices(),
        &[VertexId::new(1), VertexId::new(2), VertexId::new(3)]
    );
    assert_eq!(snapshot.outgoing(VertexId::new(1)).len(), 2);

    let events = EventGraph::new(
        vec![VertexId::new(1), VertexId::new(2), VertexId::new(3)],
        vec![
            EventEdge::new(
                VertexId::new(1),
                VertexId::new(3),
                ValidTime::from_micros(20),
                2,
                1.0,
            )
            .expect("event"),
            EventEdge::new(
                VertexId::new(1),
                VertexId::new(2),
                ValidTime::from_micros(10),
                1,
                1.0,
            )
            .expect("event"),
        ],
    )
    .expect("event graph");
    assert_eq!(
        events.outgoing(VertexId::new(1))[0].event_time(),
        ValidTime::from_micros(10)
    );
}

#[test]
fn rejects_unknown_endpoints_duplicate_vertices_and_invalid_weights() {
    assert_eq!(
        SnapshotGraph::new(vec![VertexId::new(1), VertexId::new(1)], Vec::new(), true,)
            .expect_err("duplicates"),
        GraphProjectionError::DuplicateVertex(VertexId::new(1))
    );
    assert_eq!(
        SnapshotEdge::new(VertexId::new(1), VertexId::new(2), f64::NAN).expect_err("NaN weight"),
        GraphProjectionError::InvalidWeight
    );
    assert_eq!(
        EventGraph::new(
            vec![VertexId::new(1)],
            vec![
                EventEdge::new(
                    VertexId::new(1),
                    VertexId::new(2),
                    ValidTime::from_micros(1),
                    0,
                    1.0,
                )
                .expect("event")
            ],
        )
        .expect_err("unknown endpoint"),
        GraphProjectionError::UnknownVertex(VertexId::new(2))
    );
}
