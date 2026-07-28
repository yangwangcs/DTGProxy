use query_executor::{
    ChangeEvent, IntervalCell, TemporalOperation, coalesce_interval_cells,
    derive_visible_intervals, intersect_interval_sets,
};
use temporal_types::{Interval, TransactionTime, ValidTime};

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn transaction(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

#[test]
fn derive_uses_latest_visible_correction_and_the_next_valid_start() {
    let intervals = derive_visible_intervals(
        vec![
            ChangeEvent::put(valid(1), transaction(10), "old"),
            ChangeEvent::put(valid(1), transaction(20), "corrected"),
            ChangeEvent::delete(valid(5), transaction(30)),
            ChangeEvent::put(valid(9), transaction(40), "new"),
        ],
        transaction(35),
        16,
    )
    .expect("bounded interval derivation");

    assert_eq!(intervals.len(), 2);
    assert_eq!(
        intervals[0].valid(),
        Interval::new(valid(1), Some(valid(5))).unwrap()
    );
    assert_eq!(intervals[0].value(), Some(&"corrected"));
    assert_eq!(intervals[0].operation(), TemporalOperation::Put);
    assert_eq!(intervals[1].valid(), Interval::forever_from(valid(5)));
    assert_eq!(intervals[1].value(), None);
    assert_eq!(intervals[1].operation(), TemporalOperation::Delete);
}

#[test]
fn intersection_preserves_only_cells_visible_in_every_demanded_fact() {
    let cells = intersect_interval_sets(
        &[
            vec![Interval::new(valid(1), Some(valid(5))).unwrap()],
            vec![
                Interval::new(valid(2), Some(valid(3))).unwrap(),
                Interval::new(valid(4), Some(valid(6))).unwrap(),
            ],
        ],
        16,
    )
    .expect("bounded alignment");

    assert_eq!(
        cells,
        vec![
            Interval::new(valid(2), Some(valid(3))).unwrap(),
            Interval::new(valid(4), Some(valid(5))).unwrap(),
        ]
    );
}

#[test]
fn coalesce_requires_matching_values_and_requested_provenance() {
    let valid_one = Interval::new(valid(1), Some(valid(2))).unwrap();
    let valid_two = Interval::new(valid(2), Some(valid(3))).unwrap();
    let merged = coalesce_interval_cells(
        vec![
            IntervalCell::new(valid_one, "value", transaction(10), TemporalOperation::Put),
            IntervalCell::new(valid_two, "value", transaction(10), TemporalOperation::Put),
        ],
        true,
        16,
    )
    .expect("coalescing");
    assert_eq!(merged.len(), 1);
    assert_eq!(
        merged[0].valid(),
        Interval::new(valid(1), Some(valid(3))).unwrap()
    );

    let unmerged = coalesce_interval_cells(
        vec![
            IntervalCell::new(valid_one, "value", transaction(10), TemporalOperation::Put),
            IntervalCell::new(valid_two, "value", transaction(11), TemporalOperation::Put),
        ],
        true,
        16,
    )
    .expect("coalescing");
    assert_eq!(unmerged.len(), 2);
}
