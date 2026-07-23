use distributed_query::SnapshotToken;
use query_executor::{ExecutionContext, TemporalBatchExecutor};
use temporal_ir::{LanguageProfile, PlanHeader, RowSchema};
use temporal_types::TransactionTime;

#[test]
fn latest_query_surface_has_no_versioned_namespace() {
    let header = PlanHeader::new(1, 1, 1, LanguageProfile::Cypher25, "2026.07", [1; 32])
        .expect("valid latest plan header");
    let snapshot = SnapshotToken::new(1, 1, 1, TransactionTime::new(1, 0), [2; 32])
        .expect("valid latest snapshot");

    assert_eq!(header.graph_id(), snapshot.graph_id());
    let _ = RowSchema::empty();
    let _ = std::any::type_name::<ExecutionContext>();
    let _ = std::any::type_name::<TemporalBatchExecutor<()>>();
}
