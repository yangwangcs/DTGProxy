use query_executor::{
    DistributedQueryError, QueryResult, ShardQueryBatch, SnapshotToken, merge_distributed_results,
};
use temporal_ir::{ScanOperator, TemporalPlan, TemporalSelector};
use temporal_storage::GraphId;
use temporal_types::{TransactionTime, ValidTime};

#[test]
fn merge_rejects_mixed_topology_and_snapshot_streams() {
    let snapshot = TransactionTime::new(100, 1);
    let plan = TemporalPlan::global_scan(
        GraphId::new(7),
        ScanOperator::Vertices,
        ValidTime::from_micros(1),
        TemporalSelector::AsOf(snapshot),
        10,
    );
    assert_eq!(
        merge_distributed_results(
            &plan,
            9,
            &[1],
            vec![ShardQueryBatch::new(
                1,
                8,
                SnapshotToken::AsOf(snapshot),
                QueryResult::default(),
            )],
        ),
        Err(DistributedQueryError::TopologyEpochMismatch {
            expected: 9,
            actual: 8,
        })
    );
    assert_eq!(
        merge_distributed_results(
            &plan,
            9,
            &[1],
            vec![ShardQueryBatch::new(
                1,
                9,
                SnapshotToken::AsOf(TransactionTime::new(100, 2)),
                QueryResult::default(),
            )],
        ),
        Err(DistributedQueryError::SnapshotMismatch)
    );
}
