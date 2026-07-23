use std::collections::BTreeMap;

use analytics_api::{
    AlgorithmRequest, AlgorithmValue, AnalyticsProvider, PartitionedSnapshotGraph, ProjectedGraph,
    SnapshotEdge, SnapshotPartition, VertexId,
};
use analytics_runtime::BuiltInProvider;

#[test]
fn distributed_provider_is_equivalent_across_deployment_partitionings() {
    let provider = BuiltInProvider::new();
    assert!(provider.descriptor().distributed());

    let primary_replica = PartitionedSnapshotGraph::new(
        vec![SnapshotPartition::new(
            0,
            vertices(&[1, 2, 3, 4]),
            edges(&[(1, 2), (2, 3), (3, 1), (3, 4)]),
        )],
        true,
    )
    .expect("primary-replica projection");
    let shared_nothing = PartitionedSnapshotGraph::new(
        vec![
            SnapshotPartition::new(7, vertices(&[3, 4]), edges(&[(3, 4), (3, 1)])),
            SnapshotPartition::new(2, vertices(&[1, 2]), edges(&[(2, 3), (1, 2)])),
        ],
        true,
    )
    .expect("shared-nothing projection");

    for algorithm in ["dtg.graph.degree", "dtg.graph.wcc", "dtg.graph.pageRank"] {
        let primary = provider
            .execute(request(algorithm, primary_replica.clone()))
            .expect("primary-replica execution");
        let distributed = provider
            .execute(request(algorithm, shared_nothing.clone()))
            .expect("shared-nothing execution");
        assert_eq!(distributed, primary, "{algorithm} must reduce canonically");
    }
}

#[test]
fn distributed_provider_is_invariant_to_partition_input_order() {
    let provider = BuiltInProvider::new();
    let left = PartitionedSnapshotGraph::new(
        vec![
            SnapshotPartition::new(2, vertices(&[1, 2]), edges(&[(2, 3), (1, 2)])),
            SnapshotPartition::new(7, vertices(&[3, 4]), edges(&[(3, 4), (3, 1)])),
        ],
        true,
    )
    .expect("forward partition order");
    let right = PartitionedSnapshotGraph::new(
        vec![
            SnapshotPartition::new(7, vertices(&[4, 3]), edges(&[(3, 1), (3, 4)])),
            SnapshotPartition::new(2, vertices(&[2, 1]), edges(&[(1, 2), (2, 3)])),
        ],
        true,
    )
    .expect("reverse partition order");

    for algorithm in ["dtg.graph.degree", "dtg.graph.wcc", "dtg.graph.pageRank"] {
        assert_eq!(
            provider
                .execute(request(algorithm, left.clone()))
                .expect("left execution"),
            provider
                .execute(request(algorithm, right.clone()))
                .expect("right execution"),
            "{algorithm} must not depend on partition input order"
        );
    }
}

#[test]
fn undirected_parallel_edges_are_bitwise_equivalent_across_partitionings_and_order() {
    let provider = BuiltInProvider::new();
    let primary_replica = PartitionedSnapshotGraph::new(
        vec![SnapshotPartition::new(
            0,
            vertices(&[1, 2, 3]),
            edges(&[(1, 2), (1, 2), (2, 3)]),
        )],
        false,
    )
    .expect("primary-replica undirected projection");
    let shared_nothing = PartitionedSnapshotGraph::new(
        vec![
            SnapshotPartition::new(2, vertices(&[1, 2]), edges(&[(1, 2), (1, 2)])),
            SnapshotPartition::new(7, vertices(&[3]), edges(&[(2, 3)])),
        ],
        false,
    )
    .expect("shared-nothing undirected projection");
    let reversed = PartitionedSnapshotGraph::new(
        vec![
            SnapshotPartition::new(7, vertices(&[3]), edges(&[(2, 3)])),
            SnapshotPartition::new(2, vertices(&[2, 1]), edges(&[(1, 2), (1, 2)])),
        ],
        false,
    )
    .expect("reordered undirected projection");

    for algorithm in ["dtg.graph.degree", "dtg.graph.wcc"] {
        let expected = provider
            .execute(request(algorithm, primary_replica.clone()))
            .expect("primary execution");
        assert_eq!(
            provider
                .execute(request(algorithm, shared_nothing.clone()))
                .expect("shared execution"),
            expected,
            "{algorithm} must preserve undirected parallel-edge semantics"
        );
        assert_eq!(
            provider
                .execute(request(algorithm, reversed.clone()))
                .expect("reordered execution"),
            expected,
            "{algorithm} must ignore partition input order"
        );
    }

    let expected = page_rank_bits(
        provider
            .execute(request("dtg.graph.pageRank", primary_replica))
            .expect("primary PageRank"),
    );
    assert_eq!(
        page_rank_bits(
            provider
                .execute(request("dtg.graph.pageRank", shared_nothing))
                .expect("shared PageRank"),
        ),
        expected,
        "PageRank must be bitwise equivalent across partitionings"
    );
    assert_eq!(
        page_rank_bits(
            provider
                .execute(request("dtg.graph.pageRank", reversed))
                .expect("reordered PageRank"),
        ),
        expected,
        "PageRank must be bitwise invariant to partition input order"
    );
}

#[test]
fn non_native_snapshot_algorithm_uses_bounded_canonical_fallback() {
    let provider = BuiltInProvider::new();
    let graph = PartitionedSnapshotGraph::new(
        vec![
            SnapshotPartition::new(7, vertices(&[3, 4]), edges(&[(3, 4), (3, 1)])),
            SnapshotPartition::new(2, vertices(&[1, 2]), edges(&[(2, 3), (1, 2)])),
        ],
        true,
    )
    .expect("partitioned projection");

    let distributed = provider
        .execute(request("dtg.graph.triangleCount", graph.clone()))
        .expect("bounded gathered fallback");
    let canonical = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.graph.triangleCount",
                ProjectedGraph::Snapshot(
                    graph
                        .canonical_snapshot_bounded(4, 4)
                        .expect("canonical graph"),
                ),
                BTreeMap::new(),
            )
            .expect("request"),
        )
        .expect("canonical execution");

    assert_eq!(distributed, canonical);
}

fn request(algorithm: &str, graph: PartitionedSnapshotGraph) -> AlgorithmRequest {
    AlgorithmRequest::new(
        algorithm,
        ProjectedGraph::PartitionedSnapshot(graph),
        BTreeMap::new(),
    )
    .expect("request")
}

fn page_rank_bits(result: analytics_api::AlgorithmResult) -> Vec<(VertexId, u64)> {
    result
        .rows()
        .iter()
        .map(|row| match row.as_slice() {
            [
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::FloatBits(bits),
            ] => (*vertex, *bits),
            other => panic!("unexpected PageRank row: {other:?}"),
        })
        .collect()
}

fn vertices(values: &[u128]) -> Vec<VertexId> {
    values.iter().copied().map(VertexId::new).collect()
}

fn edges(values: &[(u128, u128)]) -> Vec<SnapshotEdge> {
    values
        .iter()
        .map(|(source, destination)| {
            SnapshotEdge::new(VertexId::new(*source), VertexId::new(*destination), 1.0)
                .expect("edge")
        })
        .collect()
}
