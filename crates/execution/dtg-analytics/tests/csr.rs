use std::collections::BTreeMap;

use dtg_analytics::{
    BackendGeneration, CancellationToken, EdgeId, PartitionProvenance, ProjectedEdge,
    ProjectedVertex, ProjectionBudget, ProjectionError, ProjectionSpec, PropertyColumnSpec,
    PropertyType, ShardId, ShardProjectionPart, ShardSnapshotProvenance, SnapshotCsr,
    SnapshotProvenance, TransactionId, TransactionTime, Value, Version, VertexId,
};

fn vertex(id: u128) -> VertexId {
    VertexId::new(id).expect("nonzero vertex")
}

fn edge(id: u128) -> EdgeId {
    EdgeId::new(id).expect("nonzero edge")
}

fn shard(id: u64) -> ShardId {
    ShardId::new(id).expect("nonzero shard")
}

fn snapshot() -> SnapshotProvenance {
    SnapshotProvenance::new(
        TransactionId::new(91).expect("transaction"),
        TransactionTime::new(100).expect("start time"),
        Version::new(7),
        vec![
            (
                shard(1),
                ShardSnapshotProvenance {
                    placement_epoch: dtg_analytics::PlacementEpoch::new(11).expect("epoch"),
                    backend_generation: BackendGeneration::new(21).expect("generation"),
                    applied_index: 31,
                    closed_time: TransactionTime::new(110).expect("closed time"),
                },
            ),
            (
                shard(2),
                ShardSnapshotProvenance {
                    placement_epoch: dtg_analytics::PlacementEpoch::new(12).expect("epoch"),
                    backend_generation: BackendGeneration::new(22).expect("generation"),
                    applied_index: 32,
                    closed_time: TransactionTime::new(111).expect("closed time"),
                },
            ),
        ],
    )
    .expect("valid snapshot provenance")
}

fn properties(entries: &[(&str, Value)]) -> BTreeMap<String, Value> {
    entries
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect()
}

fn projected_vertex(id: u128, rank: i64) -> ProjectedVertex {
    ProjectedVertex {
        id: vertex(id),
        properties: properties(&[("rank", Value::Integer(rank))]),
    }
}

fn projected_edge(id: u128, source: u128, target: u128, weight: i64) -> ProjectedEdge {
    ProjectedEdge {
        id: edge(id),
        source: vertex(source),
        target: vertex(target),
        properties: properties(&[("weight", Value::Integer(weight))]),
    }
}

fn provenance(shard_id: u64, partition_index: u32, partition_count: u32) -> PartitionProvenance {
    let snapshot = snapshot();
    let shard_id = shard(shard_id);
    let fence = snapshot.shards().get(&shard_id).expect("shard fence");
    PartitionProvenance {
        shard_id,
        placement_epoch: fence.placement_epoch,
        backend_generation: fence.backend_generation,
        applied_index: fence.applied_index,
        partition_index,
        partition_count,
    }
}

fn part_a() -> ShardProjectionPart {
    ShardProjectionPart {
        snapshot: snapshot(),
        provenance: provenance(1, 0, 1),
        vertices: vec![projected_vertex(2, 20), projected_vertex(1, 10)],
        edges: vec![projected_edge(101, 1, 2, 1)],
    }
}

fn part_b() -> ShardProjectionPart {
    ShardProjectionPart {
        snapshot: snapshot(),
        provenance: provenance(2, 0, 1),
        vertices: vec![projected_vertex(4, 40), projected_vertex(3, 30)],
        edges: vec![projected_edge(103, 3, 4, 3), projected_edge(102, 2, 3, 2)],
    }
}

fn spec(reverse: bool) -> ProjectionSpec {
    ProjectionSpec::new(
        reverse,
        vec![PropertyColumnSpec::required("rank", PropertyType::Integer)],
        vec![PropertyColumnSpec::required(
            "weight",
            PropertyType::Integer,
        )],
    )
    .expect("projection spec")
}

fn budget() -> ProjectionBudget {
    ProjectionBudget::new(64 * 1024, 64 * 1024, CancellationToken::new())
}

#[test]
fn csr_vertex_order_and_digest_are_stable_across_part_arrival_order() {
    let left = SnapshotCsr::assemble(vec![part_b(), part_a()], spec(true), budget())
        .expect("left projection");
    let right = SnapshotCsr::assemble(vec![part_a(), part_b()], spec(true), budget())
        .expect("right projection");

    assert_eq!(left.digest(), right.digest());
    assert_eq!(left.vertex_ids(), right.vertex_ids());
    assert_eq!(left.partition_provenance(), right.partition_provenance());
    assert_eq!(
        left.vertex_ids(),
        &[vertex(1), vertex(2), vertex(3), vertex(4)]
    );
}

#[test]
fn csr_has_canonical_forward_and_optional_reverse_arrays() {
    let csr =
        SnapshotCsr::assemble(vec![part_b(), part_a()], spec(true), budget()).expect("projection");

    assert_eq!(csr.offsets(), &[0, 1, 2, 3, 3]);
    assert_eq!(csr.neighbors(), &[1, 2, 3]);
    assert_eq!(csr.edge_ids(), &[edge(101), edge(102), edge(103)]);

    let reverse = csr.reverse().expect("reverse CSR requested");
    assert_eq!(reverse.offsets(), &[0, 0, 1, 2, 3]);
    assert_eq!(reverse.neighbors(), &[0, 1, 2]);
    assert_eq!(reverse.forward_edge_indices(), &[0, 1, 2]);

    let forward_only = SnapshotCsr::assemble(vec![part_a(), part_b()], spec(false), budget())
        .expect("forward projection");
    assert!(forward_only.reverse().is_none());
}

#[test]
fn typed_property_columns_follow_canonical_vertex_and_edge_order() {
    let csr =
        SnapshotCsr::assemble(vec![part_b(), part_a()], spec(false), budget()).expect("projection");

    let ranks = csr.vertex_property("rank").expect("rank column");
    assert_eq!(ranks.property_type(), PropertyType::Integer);
    assert_eq!(
        ranks.values(),
        &[
            Some(Value::Integer(10)),
            Some(Value::Integer(20)),
            Some(Value::Integer(30)),
            Some(Value::Integer(40)),
        ]
    );

    let weights = csr.edge_property("weight").expect("weight column");
    assert_eq!(weights.property_type(), PropertyType::Integer);
    assert_eq!(
        weights.values(),
        &[
            Some(Value::Integer(1)),
            Some(Value::Integer(2)),
            Some(Value::Integer(3)),
        ]
    );
}

#[test]
fn snapshot_and_partition_provenance_fail_closed() {
    let mut mismatched_snapshot = part_b();
    mismatched_snapshot.snapshot = SnapshotProvenance::new(
        TransactionId::new(92).expect("transaction"),
        TransactionTime::new(100).expect("start time"),
        Version::new(7),
        snapshot()
            .shards()
            .iter()
            .map(|(id, fence)| (*id, *fence))
            .collect(),
    )
    .expect("other valid snapshot");
    assert_eq!(
        SnapshotCsr::assemble(vec![part_a(), mismatched_snapshot], spec(false), budget())
            .expect_err("mixed snapshots must fail"),
        ProjectionError::SnapshotMismatch
    );

    let mut stale_fence = part_b();
    stale_fence.provenance.applied_index -= 1;
    assert_eq!(
        SnapshotCsr::assemble(vec![part_a(), stale_fence], spec(false), budget())
            .expect_err("partition fence must match snapshot"),
        ProjectionError::PartitionFenceMismatch
    );
}

#[test]
fn duplicate_partitions_and_partition_gaps_are_rejected() {
    assert_eq!(
        SnapshotCsr::assemble(vec![part_a(), part_a(), part_b()], spec(false), budget())
            .expect_err("duplicate partition"),
        ProjectionError::DuplicatePartition
    );

    let mut first = part_a();
    first.provenance.partition_count = 3;
    let mut third = part_a();
    third.provenance.partition_count = 3;
    third.provenance.partition_index = 2;
    third.vertices = Vec::new();
    third.edges = Vec::new();
    assert_eq!(
        SnapshotCsr::assemble(vec![first, third, part_b()], spec(false), budget())
            .expect_err("missing partition one"),
        ProjectionError::PartitionGap
    );
}

#[test]
fn duplicate_graph_ids_and_dangling_edges_are_rejected() {
    let mut duplicate_vertex = part_b();
    duplicate_vertex.vertices.push(projected_vertex(1, 99));
    assert_eq!(
        SnapshotCsr::assemble(vec![part_a(), duplicate_vertex], spec(false), budget())
            .expect_err("duplicate stable vertex id"),
        ProjectionError::DuplicateVertex
    );

    let mut duplicate_edge = part_b();
    duplicate_edge.edges.push(projected_edge(101, 3, 4, 1));
    assert_eq!(
        SnapshotCsr::assemble(vec![part_a(), duplicate_edge], spec(false), budget())
            .expect_err("duplicate stable edge id"),
        ProjectionError::DuplicateEdge
    );

    let mut dangling = part_b();
    dangling.edges.push(projected_edge(999, 4, 99, 1));
    assert_eq!(
        SnapshotCsr::assemble(vec![part_a(), dangling], spec(false), budget())
            .expect_err("edge endpoint absent from projection"),
        ProjectionError::DanglingEdge
    );
}

#[test]
fn property_types_and_required_columns_are_validated() {
    let mut wrong_type = part_a();
    wrong_type.vertices[0]
        .properties
        .insert("rank".into(), Value::String("twenty".into()));
    assert_eq!(
        SnapshotCsr::assemble(vec![wrong_type, part_b()], spec(false), budget())
            .expect_err("typed property mismatch"),
        ProjectionError::PropertyTypeMismatch
    );

    let mut missing = part_a();
    missing.vertices[0].properties.remove("rank");
    assert_eq!(
        SnapshotCsr::assemble(vec![missing, part_b()], spec(false), budget())
            .expect_err("required column is absent"),
        ProjectionError::MissingRequiredProperty
    );
}

#[test]
fn projection_charges_memory_then_spill_and_rejects_total_budget_overflow() {
    let spill_budget = ProjectionBudget::new(1, 64 * 1024, CancellationToken::new());
    let csr = SnapshotCsr::assemble(vec![part_a(), part_b()], spec(true), spill_budget)
        .expect("spill reservation covers projection");
    assert_eq!(csr.budget_usage().memory_bytes, 1);
    assert!(csr.budget_usage().spill_bytes > 0);

    let exhausted = ProjectionBudget::new(1, 1, CancellationToken::new());
    assert_eq!(
        SnapshotCsr::assemble(vec![part_a(), part_b()], spec(true), exhausted)
            .expect_err("total projection budget is too small"),
        ProjectionError::ResourceLimit
    );
}

#[test]
fn projection_observes_cancellation_and_digest_covers_typed_content() {
    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let cancelled_budget = ProjectionBudget::new(64 * 1024, 0, cancellation);
    assert_eq!(
        SnapshotCsr::assemble(vec![part_a(), part_b()], spec(true), cancelled_budget)
            .expect_err("cancelled assembly"),
        ProjectionError::Cancelled
    );

    let base = SnapshotCsr::assemble(vec![part_a(), part_b()], spec(false), budget())
        .expect("base projection");
    let mut changed = part_b();
    changed.vertices[0]
        .properties
        .insert("rank".into(), Value::Integer(41));
    let changed = SnapshotCsr::assemble(vec![part_a(), changed], spec(false), budget())
        .expect("changed projection");
    assert_ne!(base.digest(), changed.digest());
}
