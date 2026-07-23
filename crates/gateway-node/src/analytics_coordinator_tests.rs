use analytics_ledger::AnalyticsJobId;
use temporal_types::TransactionTime;

use crate::analytics_coordinator::{decode_canonical_job_id, timestamp_job_candidate};

#[test]
fn timestamp_job_candidates_preserve_tso_monotonic_order() {
    let earlier = timestamp_job_candidate(TransactionTime::new(1_000, 7)).unwrap();
    let later_logical = timestamp_job_candidate(TransactionTime::new(1_000, 8)).unwrap();
    let later_physical = timestamp_job_candidate(TransactionTime::new(1_001, 0)).unwrap();

    assert!(earlier < later_logical);
    assert!(later_logical < later_physical);
}

#[test]
fn canonical_job_ids_are_exactly_one_nonzero_u128() {
    let expected = AnalyticsJobId::new(42).unwrap();
    assert_eq!(
        decode_canonical_job_id(&42_u128.to_be_bytes()).unwrap(),
        expected,
    );
    assert!(decode_canonical_job_id(&[1; 15]).is_err());
    assert!(decode_canonical_job_id(&[0; 16]).is_err());
}
