use std::collections::BTreeMap;

use analytics_api::{
    DeltaEdge, DeltaKind, EdgeId, EventEdge, EventGraph, GraphProjectionError, IntervalEdge,
    IntervalGraph, IntervalVertex, PartitionedSnapshotGraph, ProviderDescriptor, SnapshotEdge,
    SnapshotGraph, SnapshotPartition, VertexId,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, ValidTime};

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

#[test]
fn interval_graph_accepts_an_edge_covered_by_adjacent_vertex_segments() {
    let valid = |start, end| {
        Interval::new(
            ValidTime::from_micros(start),
            Some(ValidTime::from_micros(end)),
        )
        .expect("valid interval")
    };

    let graph = IntervalGraph::new(
        vec![
            IntervalVertex::new(VertexId::new(1), valid(1, 5), payload("one")),
            IntervalVertex::new(VertexId::new(1), valid(5, 10), payload("two")),
            IntervalVertex::new(VertexId::new(2), valid(1, 10), payload("three")),
        ],
        vec![
            IntervalEdge::new(
                EdgeId::new(1),
                VertexId::new(1),
                VertexId::new(2),
                valid(1, 10),
                payload("edge"),
                1.0,
            )
            .expect("edge"),
        ],
        true,
    )
    .expect("adjacent endpoint intervals cover the edge");

    assert_eq!(graph.edges().len(), 1);
}

#[test]
fn delta_edge_exposes_before_and_after_weights() {
    let edge = DeltaEdge::new(
        EdgeId::new(1),
        VertexId::new(1),
        VertexId::new(2),
        DeltaKind::Updated,
        Some(payload("before")),
        Some(payload("after")),
        Some(2.5),
        Some(3.5),
    )
    .expect("delta edge");

    assert_eq!(edge.before_weight(), Some(2.5));
    assert_eq!(edge.after_weight(), Some(3.5));
}

#[test]
fn interval_graph_retains_edge_identity_and_payloads_for_open_ended_coverage() {
    let valid = |start, end| {
        Interval::new(
            ValidTime::from_micros(start),
            Some(ValidTime::from_micros(end)),
        )
        .expect("valid interval")
    };
    let before = payload("before");
    let after = payload("after");
    let edge_payload = payload("edge");
    let graph = IntervalGraph::new(
        vec![
            IntervalVertex::new(VertexId::new(1), valid(1, 5), before.clone()),
            IntervalVertex::new(
                VertexId::new(1),
                Interval::forever_from(ValidTime::from_micros(5)),
                after.clone(),
            ),
            IntervalVertex::new(
                VertexId::new(2),
                Interval::forever_from(ValidTime::from_micros(1)),
                before.clone(),
            ),
        ],
        vec![
            IntervalEdge::new(
                EdgeId::new(9),
                VertexId::new(1),
                VertexId::new(2),
                Interval::forever_from(ValidTime::from_micros(1)),
                edge_payload.clone(),
                1.0,
            )
            .expect("edge"),
        ],
        true,
    )
    .expect("open-ended adjacent intervals cover the edge");

    assert_eq!(graph.vertices()[0].payload(), &before);
    assert_eq!(graph.edges()[0].edge_id(), EdgeId::new(9));
    assert_eq!(graph.edges()[0].payload(), &edge_payload);
}

#[test]
fn delta_edge_retains_identity_and_payload_change() {
    let before = payload("before");
    let after = payload("after");
    let edge = DeltaEdge::new(
        EdgeId::new(3),
        VertexId::new(1),
        VertexId::new(2),
        DeltaKind::Updated,
        Some(before.clone()),
        Some(after.clone()),
        Some(2.5),
        Some(2.5),
    )
    .expect("delta edge");

    assert_eq!(edge.edge_id(), EdgeId::new(3));
    assert_eq!(edge.before_payload(), Some(&before));
    assert_eq!(edge.after_payload(), Some(&after));
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(value.to_owned()))]),
    )
}

#[test]
fn partitioned_snapshot_canonicalizes_shards_vertices_and_edges() {
    let graph = PartitionedSnapshotGraph::new(
        vec![
            SnapshotPartition::new(
                9,
                vec![VertexId::new(4), VertexId::new(3)],
                vec![snapshot_edge(4, 1), snapshot_edge(3, 4)],
            ),
            SnapshotPartition::new(
                2,
                vec![VertexId::new(2), VertexId::new(1)],
                vec![snapshot_edge(2, 3), snapshot_edge(1, 2)],
            ),
        ],
        true,
    )
    .expect("partitioned snapshot");

    assert_eq!(
        graph
            .partitions()
            .iter()
            .map(SnapshotPartition::shard_id)
            .collect::<Vec<_>>(),
        vec![2, 9]
    );
    assert_eq!(
        graph.partitions()[0].vertices(),
        &[VertexId::new(1), VertexId::new(2)]
    );
    assert_eq!(
        graph.vertices(),
        &[
            VertexId::new(1),
            VertexId::new(2),
            VertexId::new(3),
            VertexId::new(4),
        ]
    );
    assert_eq!(graph.owner(VertexId::new(4)), Some(9));

    let canonical = graph
        .canonical_snapshot_bounded(4, 4)
        .expect("bounded canonical fallback");
    assert_eq!(canonical.vertices(), graph.vertices());
    assert_eq!(canonical.edges().len(), 4);
}

#[test]
fn partitioned_snapshot_rejects_ambiguous_ownership_and_unknown_endpoints() {
    assert_eq!(
        PartitionedSnapshotGraph::new(
            vec![
                SnapshotPartition::new(2, vec![VertexId::new(1)], Vec::new()),
                SnapshotPartition::new(2, vec![VertexId::new(2)], Vec::new()),
            ],
            true,
        )
        .expect_err("duplicate shard"),
        GraphProjectionError::DuplicateShard(2)
    );
    assert_eq!(
        PartitionedSnapshotGraph::new(
            vec![
                SnapshotPartition::new(2, vec![VertexId::new(1)], Vec::new()),
                SnapshotPartition::new(9, vec![VertexId::new(1)], Vec::new()),
            ],
            true,
        )
        .expect_err("duplicate owner"),
        GraphProjectionError::DuplicateVertexOwner(VertexId::new(1))
    );
    assert_eq!(
        PartitionedSnapshotGraph::new(
            vec![SnapshotPartition::new(
                2,
                vec![VertexId::new(1)],
                vec![snapshot_edge(1, 99)],
            )],
            true,
        )
        .expect_err("unknown global endpoint"),
        GraphProjectionError::UnknownVertex(VertexId::new(99))
    );
}

#[test]
fn partitioned_snapshot_bounds_canonical_fallback() {
    let graph = PartitionedSnapshotGraph::new(
        vec![SnapshotPartition::new(
            2,
            vec![VertexId::new(1), VertexId::new(2)],
            vec![snapshot_edge(1, 2)],
        )],
        true,
    )
    .expect("partitioned snapshot");

    assert_eq!(
        graph
            .canonical_snapshot_bounded(1, 1)
            .expect_err("vertex bound"),
        GraphProjectionError::SnapshotVertexLimit {
            limit: 1,
            actual: 2,
        }
    );
    assert_eq!(
        graph
            .canonical_snapshot_bounded(2, 0)
            .expect_err("edge bound"),
        GraphProjectionError::SnapshotEdgeLimit {
            limit: 0,
            actual: 1,
        }
    );
}

fn snapshot_edge(source: u128, destination: u128) -> SnapshotEdge {
    SnapshotEdge::new(VertexId::new(source), VertexId::new(destination), 1.0).expect("edge")
}

#[test]
fn provider_descriptor_exposes_its_persisted_version() {
    let descriptor = ProviderDescriptor::new("analytics-native", "3.2.1", true, false);

    assert_eq!(descriptor.version(), "3.2.1");
}
