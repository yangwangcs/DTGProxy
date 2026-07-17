use temporal_types::{TransactionTime, ValidTime};

#[test]
fn time_components_are_available_for_stable_storage_codecs() {
    let valid = ValidTime::from_micros(-7);
    let transaction = TransactionTime::new(42, 3);

    assert_eq!(valid.as_micros(), -7);
    assert_eq!(transaction.physical_micros(), 42);
    assert_eq!(transaction.logical(), 3);
}
