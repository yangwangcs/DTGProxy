use temporal_model::Timeline;
use temporal_types::{Interval, TransactionTime, ValidTime};

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn interval(from: i64, to: i64) -> Interval<ValidTime> {
    Interval::new(valid(from), Some(valid(to))).unwrap()
}

#[test]
fn retroactive_correction_preserves_old_knowledge_and_splits_new_knowledge() {
    let mut timeline = Timeline::new();
    timeline
        .put_initial(interval(1, 10), "A".to_owned(), tx(100))
        .unwrap();

    let summary = timeline
        .correct(interval(4, 7), "B".to_owned(), tx(150), tx(200))
        .unwrap();

    assert_eq!(summary.closed_versions, 1);
    assert_eq!(summary.opened_versions, 3);
    assert_eq!(
        timeline
            .value_at(valid(5), tx(150))
            .unwrap()
            .map(String::as_str),
        Some("A")
    );
    assert_eq!(
        timeline
            .value_at(valid(2), tx(250))
            .unwrap()
            .map(String::as_str),
        Some("A")
    );
    assert_eq!(
        timeline
            .value_at(valid(5), tx(250))
            .unwrap()
            .map(String::as_str),
        Some("B")
    );
    assert_eq!(
        timeline
            .value_at(valid(8), tx(250))
            .unwrap()
            .map(String::as_str),
        Some("A")
    );
}

#[test]
fn stale_writer_conflicts_with_a_later_overlapping_commit() {
    let mut timeline = Timeline::new();
    timeline
        .put_initial(interval(1, 10), "A".to_owned(), tx(100))
        .unwrap();
    timeline
        .correct(interval(4, 7), "B".to_owned(), tx(150), tx(180))
        .unwrap();

    let error = timeline
        .correct(interval(5, 6), "C".to_owned(), tx(150), tx(200))
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "write conflict after transaction snapshot"
    );
}

#[test]
fn commit_timestamp_must_follow_the_read_snapshot() {
    let mut timeline = Timeline::new();

    let error = timeline
        .correct(interval(1, 2), "A".to_owned(), tx(200), tx(200))
        .unwrap_err();

    assert_eq!(
        error.to_string(),
        "commit timestamp must follow transaction snapshot"
    );
}

