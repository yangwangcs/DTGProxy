use dtg_kernel::{
    BackendGeneration, GraphId, PlacementEpoch, ShardId, TransactionTime, ValidInterval,
};

#[test]
fn zero_and_reversed_values_are_rejected() {
    assert!(GraphId::new(0).is_err());
    assert!(ShardId::new(0).is_err());
    assert!(PlacementEpoch::new(0).is_err());
    assert!(BackendGeneration::new(0).is_err());
    assert!(ValidInterval::new(20, 10).is_err());
}

#[test]
fn adjacent_intervals_do_not_overlap() {
    let left = ValidInterval::new(10, 20).unwrap();
    let right = ValidInterval::new(20, 30).unwrap();
    assert!(!left.overlaps(right));
    assert_eq!(TransactionTime::new(9).unwrap().get(), 9);
}
