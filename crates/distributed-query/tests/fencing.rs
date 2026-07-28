use std::collections::BTreeMap;

use distributed_query::{
    BatchEnvelope, BatchMerger, DistributedQueryError, FragmentRequest, SnapshotToken,
};
use physical_plan::FragmentId;
use temporal_types::TransactionTime;

fn snapshot(epoch: u64) -> SnapshotToken {
    SnapshotToken::new(7, 3, epoch, TransactionTime::new(100, 2), [5; 32]).expect("snapshot")
}

#[test]
fn merges_exactly_one_complete_sequence_from_each_expected_shard() {
    let token = snapshot(11);
    let mut merger = BatchMerger::new(vec![1, 2], token.clone(), 1024).expect("merger");
    merger
        .push(BatchEnvelope::new(1, 0, false, &token, b"one".to_vec()))
        .expect("batch");
    merger
        .push(BatchEnvelope::new(2, 0, false, &token, b"two".to_vec()))
        .expect("batch");

    assert_eq!(
        merger.finish().expect("complete"),
        vec![b"one".to_vec(), b"two".to_vec()]
    );
}

#[test]
fn rejects_a_batch_from_a_stale_topology_epoch() {
    let expected = snapshot(11);
    let stale = snapshot(10);
    let mut merger = BatchMerger::new(vec![1], expected, 1024).expect("merger");

    let error = merger
        .push(BatchEnvelope::new(1, 0, false, &stale, vec![]))
        .expect_err("stale epoch must fail");

    assert_eq!(error, DistributedQueryError::SnapshotMismatch);
}

#[test]
fn rejects_duplicate_or_gapped_batch_sequences() {
    let token = snapshot(11);
    let mut merger = BatchMerger::new(vec![1], token.clone(), 1024).expect("merger");

    let error = merger
        .push(BatchEnvelope::new(1, 1, true, &token, vec![]))
        .expect_err("gap must fail");

    assert_eq!(
        error,
        DistributedQueryError::UnexpectedSequence {
            shard_id: 1,
            expected: 0,
            actual: 1,
        }
    );
}

#[test]
fn refuses_to_finish_with_an_incomplete_shard_stream() {
    let token = snapshot(11);
    let merger = BatchMerger::new(vec![1, 2], token, 1024).expect("merger");

    assert_eq!(
        merger.finish(),
        Err(DistributedQueryError::IncompleteShards(vec![1, 2]))
    );
}

#[test]
fn fragment_request_requires_a_complete_nonzero_read_index_map_for_expected_shards() {
    let request = FragmentRequest::new(FragmentId::new(1), snapshot(11), 1, 1024, 1)
        .expect("request")
        .with_expected_shards(vec![1, 2])
        .expect("expected shards");

    assert_eq!(
        request
            .clone()
            .with_required_applied_indexes(BTreeMap::from([(1, 7)])),
        Err(DistributedQueryError::InvalidRequest)
    );
    assert_eq!(
        request.with_required_applied_indexes(BTreeMap::from([(1, 7), (2, 0)])),
        Err(DistributedQueryError::InvalidRequest)
    );
}

#[test]
fn fragment_request_requires_complete_nonzero_capability_generations() {
    let request = FragmentRequest::new(FragmentId::new(1), snapshot(11), 1, 1024, 1)
        .expect("request")
        .with_expected_shards(vec![1, 2])
        .expect("expected shards");

    assert_eq!(
        request
            .clone()
            .with_expected_capability_generations(BTreeMap::from([(1, 7)])),
        Err(DistributedQueryError::InvalidRequest)
    );
    assert_eq!(
        request.with_expected_capability_generations(BTreeMap::from([(1, 7), (2, 0)])),
        Err(DistributedQueryError::InvalidRequest)
    );
}
