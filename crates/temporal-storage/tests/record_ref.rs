use std::collections::BTreeMap;

use temporal_storage::{
    HistoryAnchor, HistoryDelta, HistoryEntry, HistoryEntryRef, HistoryOperationRef,
    ProjectionRecord, ProjectionRecordRef, ValidSegment,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn tx(physical_micros: i64) -> TransactionTime {
    TransactionTime::new(physical_micros, 0)
}

fn valid(micros: i64) -> ValidTime {
    ValidTime::from_micros(micros)
}

fn interval(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(valid(start), Some(valid(end))).unwrap()
}

fn payload(name: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String(name.to_owned()))]),
    )
}

fn projection_with_two_segments() -> ProjectionRecord {
    ProjectionRecord::new(
        tx(1),
        vec![
            ValidSegment::new(interval(1, 4), payload("old")),
            ValidSegment::new(interval(7, 10), payload("new")),
        ],
    )
    .unwrap()
}

#[test]
fn projection_ref_selects_only_the_segment_containing_valid_time() {
    let projection = projection_with_two_segments();
    let encoded = projection.encode().unwrap();
    let view = ProjectionRecordRef::parse(&encoded).unwrap();
    assert_eq!(
        view.visible_at(valid(2))
            .unwrap()
            .unwrap()
            .property(1)
            .unwrap()
            .unwrap()
            .decode()
            .unwrap(),
        GraphValue::String("old".to_owned())
    );
    assert_eq!(view.visible_at(valid(5)).unwrap(), None);
    assert_eq!(
        view.visible_at(valid(8))
            .unwrap()
            .unwrap()
            .property(1)
            .unwrap()
            .unwrap()
            .decode()
            .unwrap(),
        GraphValue::String("new".to_owned())
    );
}

#[test]
fn delta_ref_skips_non_matching_payload_but_still_checks_checksum() {
    let encoded = HistoryDelta::put(tx(3), interval(10, 20), payload("large"))
        .encode()
        .unwrap();
    let view = HistoryEntryRef::parse(&encoded).unwrap();
    assert!(!view.changed_valid().contains(valid(5)));

    let mut corrupted = encoded;
    *corrupted.last_mut().unwrap() ^= 1;
    assert_matching_history_error(&corrupted);
}

#[test]
fn borrowed_entries_match_owned_empty_and_multi_segment_anchors() {
    let empty_anchor = HistoryAnchor::new(
        tx(4),
        interval(1, 3),
        ProjectionRecord::new(tx(4), Vec::new()).unwrap(),
    )
    .unwrap();
    let empty_encoded = empty_anchor.encode().unwrap();
    let empty_owned = HistoryEntry::decode(&empty_encoded).unwrap();
    let empty_view = HistoryEntryRef::parse(&empty_encoded).unwrap();

    assert_eq!(empty_view.commit_ts(), empty_owned.commit_ts());
    assert_eq!(empty_view.changed_valid(), empty_owned.changed_valid());
    assert_eq!(empty_view.replacement().unwrap(), None);
    match (empty_owned, empty_view) {
        (HistoryEntry::Anchor(anchor), HistoryEntryRef::Anchor(view)) => {
            assert_eq!(
                view.projection().commit_ts(),
                anchor.projection().commit_ts()
            );
            assert_eq!(view.projection().segment_count(), 0);
        }
        _ => panic!("expected anchor records"),
    }

    let multi_anchor =
        HistoryAnchor::new(tx(1), interval(1, 10), projection_with_two_segments()).unwrap();
    let multi_encoded = multi_anchor.encode().unwrap();
    let multi_owned = HistoryEntry::decode(&multi_encoded).unwrap();
    let multi_view = HistoryEntryRef::parse(&multi_encoded).unwrap();

    assert_eq!(multi_view.commit_ts(), multi_owned.commit_ts());
    assert_eq!(multi_view.changed_valid(), multi_owned.changed_valid());
    match (multi_owned, multi_view) {
        (HistoryEntry::Anchor(anchor), HistoryEntryRef::Anchor(view)) => {
            assert_eq!(
                view.projection().commit_ts(),
                anchor.projection().commit_ts()
            );
            assert_eq!(
                view.projection().segment_count(),
                anchor.projection().segments().len()
            );
            for time in [valid(2), valid(5), valid(8)] {
                let borrowed = view
                    .projection()
                    .visible_at(time)
                    .unwrap()
                    .map(|payload| payload.encoded().to_vec());
                let owned = anchor
                    .projection()
                    .visible_at(time)
                    .map(|payload| payload.encode().unwrap());
                assert_eq!(borrowed, owned);
            }
        }
        _ => panic!("expected anchor records"),
    }
}

#[test]
fn borrowed_entries_match_owned_put_and_delete_deltas() {
    let put = HistoryDelta::put(tx(6), interval(10, 20), payload("put"));
    let put_encoded = put.encode().unwrap();
    let put_owned = HistoryEntry::decode(&put_encoded).unwrap();
    let put_view = HistoryEntryRef::parse(&put_encoded).unwrap();

    assert_eq!(put_view.commit_ts(), put_owned.commit_ts());
    assert_eq!(put_view.changed_valid(), put_owned.changed_valid());
    match (put_owned, put_view) {
        (HistoryEntry::Delta(delta), HistoryEntryRef::Delta(view)) => {
            match (delta.replacement(), view.operation()) {
                (Some(owned), HistoryOperationRef::Put(borrowed)) => {
                    assert_eq!(borrowed.encoded(), owned.encode().unwrap());
                }
                _ => panic!("expected put operations"),
            }
        }
        _ => panic!("expected delta records"),
    }

    let delete = HistoryDelta::delete(tx(7), interval(20, 30));
    let delete_encoded = delete.encode().unwrap();
    let delete_owned = HistoryEntry::decode(&delete_encoded).unwrap();
    let delete_view = HistoryEntryRef::parse(&delete_encoded).unwrap();

    assert_eq!(delete_view.commit_ts(), delete_owned.commit_ts());
    assert_eq!(delete_view.changed_valid(), delete_owned.changed_valid());
    assert_eq!(delete_view.replacement().unwrap(), None);
    match (delete_owned, delete_view) {
        (HistoryEntry::Delta(delta), HistoryEntryRef::Delta(view)) => {
            assert_eq!(delta.replacement(), None);
            assert_eq!(view.operation(), HistoryOperationRef::Delete);
        }
        _ => panic!("expected delta records"),
    }
}

#[test]
fn borrowed_history_entry_matches_owned_corruption_classes() {
    let put = HistoryDelta::put(tx(8), interval(10, 20), payload("large"))
        .encode()
        .unwrap();

    assert_history_version_and_checksum_errors_match(&put);

    let mut put_length = put;
    put_length[36..40].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_matching_history_error(&put_length);

    let delete = HistoryDelta::delete(tx(9), interval(20, 30))
        .encode()
        .unwrap();
    assert_history_version_and_checksum_errors_match(&delete);

    let anchor = HistoryAnchor::new(tx(1), interval(1, 10), projection_with_two_segments())
        .unwrap()
        .encode()
        .unwrap();
    assert_history_version_and_checksum_errors_match(&anchor);

    let mut anchor_length = anchor;
    anchor_length[35..39].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_matching_history_error(&anchor_length);
}

#[test]
fn borrowed_projection_matches_owned_corruption_classes() {
    let encoded = projection_with_two_segments().encode().unwrap();

    let mut checksum = encoded.clone();
    *checksum.last_mut().unwrap() ^= 1;
    assert_matching_projection_error(&checksum);

    let mut version = encoded.clone();
    version[5] = 2;
    assert_matching_projection_error(&version);

    let mut length = encoded;
    length[39..43].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_matching_projection_error(&length);
}

fn assert_history_version_and_checksum_errors_match(encoded: &[u8]) {
    let mut checksum = encoded.to_vec();
    *checksum.last_mut().unwrap() ^= 1;
    assert_matching_history_error(&checksum);

    let mut version = encoded.to_vec();
    version[5] = 2;
    assert_matching_history_error(&version);
}

fn assert_matching_history_error(encoded: &[u8]) {
    assert_eq!(
        HistoryEntryRef::parse(encoded).unwrap_err(),
        HistoryEntry::decode(encoded).unwrap_err()
    );
}

fn assert_matching_projection_error(encoded: &[u8]) {
    assert_eq!(
        ProjectionRecordRef::parse(encoded).unwrap_err(),
        ProjectionRecord::decode(encoded).unwrap_err()
    );
}
