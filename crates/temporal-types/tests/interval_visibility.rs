use temporal_types::{BitemporalVersion, Interval, TransactionTime, ValidTime};

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

#[test]
fn interval_is_half_open_and_rejects_empty_ranges() {
    let interval = Interval::new(valid(10), Some(valid(20))).unwrap();

    assert!(interval.contains(valid(10)));
    assert!(interval.contains(valid(19)));
    assert!(!interval.contains(valid(20)));
    assert!(Interval::new(valid(10), Some(valid(10))).is_err());
}

#[test]
fn bitemporal_visibility_requires_both_dimensions() {
    let version = BitemporalVersion::new(
        "risk=high",
        Interval::new(valid(10), Some(valid(20))).unwrap(),
        Interval::new(tx(100), Some(tx(200))).unwrap(),
    );

    assert!(version.is_visible_at(valid(15), tx(150)));
    assert!(!version.is_visible_at(valid(20), tx(150)));
    assert!(!version.is_visible_at(valid(15), tx(200)));
}

#[test]
fn subtracting_an_inner_interval_returns_two_residuals() {
    let source = Interval::new(valid(1), Some(valid(10))).unwrap();
    let cut = Interval::new(valid(4), Some(valid(7))).unwrap();

    assert_eq!(
        source.subtract(&cut),
        vec![
            Interval::new(valid(1), Some(valid(4))).unwrap(),
            Interval::new(valid(7), Some(valid(10))).unwrap(),
        ]
    );
}

#[test]
fn unbounded_interval_contains_all_later_values() {
    let interval = Interval::forever_from(valid(10));

    assert!(!interval.contains(valid(9)));
    assert!(interval.contains(valid(10)));
    assert!(interval.contains(valid(i64::MAX)));
}
