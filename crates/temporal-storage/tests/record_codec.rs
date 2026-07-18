use std::collections::BTreeMap;

use temporal_storage::{
    EdgeIdentity, EdgeTypeId, ElementId, ElementRef, GraphId, HistoryAnchor, HistoryDelta,
    HistoryEntry, LabelId, PartitionId, ProjectionRecord, RecordCodecError, ValidSegment,
    VertexIdentity,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn vertex_ref() -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(2), ElementId::new(3))
}

fn edge_ref() -> ElementRef {
    ElementRef::edge(GraphId::new(1), PartitionId::new(2), ElementId::new(30))
}

fn payload(name: &str) -> CanonicalElement {
    CanonicalElement::new(
        9,
        BTreeMap::from([
            (1, GraphValue::String(name.to_owned())),
            (2, GraphValue::Null),
            (3, GraphValue::Boolean(true)),
            (4, GraphValue::Integer(-42)),
            (5, GraphValue::FloatBits(f64::NAN.to_bits())),
            (6, GraphValue::Bytes(vec![0, 1, 2])),
            (7, GraphValue::TimestampMicros(123)),
            (
                8,
                GraphValue::List(vec![GraphValue::Integer(1), GraphValue::String("x".into())]),
            ),
        ]),
    )
}

fn interval(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(
        ValidTime::from_micros(start),
        end.map(ValidTime::from_micros),
    )
    .unwrap()
}

#[test]
fn vertex_and_edge_identity_records_round_trip() {
    let vertex = VertexIdentity::new(vertex_ref(), LabelId::new(7)).unwrap();
    let edge = EdgeIdentity::new(
        edge_ref(),
        EdgeTypeId::new(8),
        ElementId::new(3),
        ElementId::new(4),
    )
    .unwrap();

    let vertex_bytes = vertex.encode();
    let edge_bytes = edge.encode();

    assert_eq!(
        vertex_bytes,
        vec![
            0x44, 0x54, 0x47, 0x49, 0x00, 0x01, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x07, 0x2c, 0xf9, 0x97,
            0xb4, 0xcc, 0xa7, 0xac, 0xcc,
        ]
    );
    assert_eq!(VertexIdentity::decode(&vertex_bytes).unwrap(), vertex);
    assert_eq!(EdgeIdentity::decode(&edge_bytes).unwrap(), edge);
}

#[test]
fn cross_partition_edge_identity_round_trips_endpoint_partitions() {
    let source = vertex_ref();
    let destination = ElementRef::vertex(source.graph(), PartitionId::new(9), ElementId::new(4));
    let edge =
        EdgeIdentity::new_between(edge_ref(), EdgeTypeId::new(8), source, destination).unwrap();

    let bytes = edge.encode();
    assert_eq!(&bytes[..4], b"DTGI");
    assert_eq!(&bytes[4..6], 2_u16.to_be_bytes());
    assert_eq!(EdgeIdentity::decode(&bytes).unwrap(), edge);
    assert_eq!(edge.source_ref(), source);
    assert_eq!(edge.destination_ref(), destination);
}

#[test]
fn projection_round_trips_disjoint_finite_and_unbounded_segments_without_type_loss() {
    let projection = ProjectionRecord::new(
        TransactionTime::new(100, 2),
        vec![
            ValidSegment::new(interval(1, Some(4)), payload("old")),
            ValidSegment::new(interval(7, None), payload("new")),
        ],
    )
    .unwrap();

    let bytes = projection.encode().unwrap();
    let decoded = ProjectionRecord::decode(&bytes).unwrap();

    assert_eq!(&bytes[..4], b"DTGP");
    assert_eq!(&bytes[4..6], 1_u16.to_be_bytes());
    assert_eq!(decoded, projection);
    assert_eq!(
        decoded.visible_at(ValidTime::from_micros(2)),
        Some(&payload("old"))
    );
    assert_eq!(decoded.visible_at(ValidTime::from_micros(5)), None);
    assert_eq!(
        decoded.visible_at(ValidTime::from_micros(99)),
        Some(&payload("new"))
    );
}

#[test]
fn empty_projection_and_history_anchor_round_trip() {
    let commit = TransactionTime::new(200, 0);
    let projection = ProjectionRecord::new(commit, Vec::new()).unwrap();
    let anchor = HistoryAnchor::new(commit, interval(4, Some(7)), projection).unwrap();

    let bytes = anchor.encode().unwrap();

    assert_eq!(&bytes[..4], b"DTGA");
    assert_eq!(HistoryAnchor::decode(&bytes).unwrap(), anchor);
}

#[test]
fn constructor_rejects_overlapping_or_unsorted_segments_and_mismatched_anchor_time() {
    let first = ValidSegment::new(interval(1, Some(5)), payload("a"));
    let overlapping = ValidSegment::new(interval(4, Some(7)), payload("b"));
    let unsorted = ValidSegment::new(interval(-1, Some(0)), payload("c"));

    assert_eq!(
        ProjectionRecord::new(TransactionTime::new(1, 0), vec![first.clone(), overlapping]),
        Err(RecordCodecError::OverlappingOrUnsortedSegments)
    );
    assert_eq!(
        ProjectionRecord::new(TransactionTime::new(1, 0), vec![first, unsorted]),
        Err(RecordCodecError::OverlappingOrUnsortedSegments)
    );

    let projection = ProjectionRecord::new(TransactionTime::new(1, 0), Vec::new()).unwrap();
    assert_eq!(
        HistoryAnchor::new(TransactionTime::new(2, 0), interval(1, None), projection),
        Err(RecordCodecError::CommitTimestampMismatch)
    );
}

#[test]
fn decoders_reject_corruption_wrong_versions_truncation_and_trailing_bytes() {
    let projection = ProjectionRecord::new(
        TransactionTime::new(100, 0),
        vec![ValidSegment::new(interval(1, None), payload("value"))],
    )
    .unwrap();
    let bytes = projection.encode().unwrap();

    let mut wrong_magic = bytes.clone();
    wrong_magic[0] = b'X';
    assert_eq!(
        ProjectionRecord::decode(&wrong_magic),
        Err(RecordCodecError::InvalidMagic)
    );

    let mut wrong_version = bytes.clone();
    wrong_version[5] = 2;
    assert_eq!(
        ProjectionRecord::decode(&wrong_version),
        Err(RecordCodecError::UnsupportedVersion(2))
    );

    assert_eq!(
        ProjectionRecord::decode(&bytes[..bytes.len() - 1]),
        Err(RecordCodecError::UnexpectedEnd)
    );

    let mut corrupt = bytes.clone();
    let payload_byte = corrupt.len() - 9;
    corrupt[payload_byte] ^= 1;
    assert_eq!(
        ProjectionRecord::decode(&corrupt),
        Err(RecordCodecError::ChecksumMismatch)
    );

    let mut trailing = bytes;
    trailing.push(0);
    assert_eq!(
        ProjectionRecord::decode(&trailing),
        Err(RecordCodecError::TrailingBytes)
    );
}

#[test]
fn identity_constructor_rejects_the_wrong_element_kind() {
    assert_eq!(
        VertexIdentity::new(edge_ref(), LabelId::new(1)),
        Err(RecordCodecError::WrongElementKind)
    );
    assert_eq!(
        EdgeIdentity::new(
            vertex_ref(),
            EdgeTypeId::new(1),
            ElementId::new(1),
            ElementId::new(2),
        ),
        Err(RecordCodecError::WrongElementKind)
    );
}

#[test]
fn history_put_and_delete_deltas_round_trip_with_canonical_types() {
    let put = HistoryDelta::put(
        TransactionTime::new(300, 1),
        interval(4, Some(7)),
        payload("delta"),
    );
    let delete = HistoryDelta::delete(TransactionTime::new(400, 0), interval(8, None));

    let put_bytes = put.encode().unwrap();
    let delete_bytes = delete.encode().unwrap();

    assert_eq!(&put_bytes[..4], b"DTGD");
    assert_eq!(HistoryDelta::decode(&put_bytes).unwrap(), put);
    assert_eq!(HistoryDelta::decode(&delete_bytes).unwrap(), delete);
    assert_eq!(
        HistoryEntry::decode(&put_bytes).unwrap(),
        HistoryEntry::Delta(put)
    );
    assert_eq!(
        HistoryEntry::decode(&delete_bytes).unwrap(),
        HistoryEntry::Delta(delete)
    );
}

#[test]
fn history_entry_dispatches_anchors_and_rejects_delta_corruption() {
    let commit = TransactionTime::new(100, 0);
    let projection = ProjectionRecord::new(commit, Vec::new()).unwrap();
    let anchor = HistoryAnchor::new(commit, interval(1, None), projection).unwrap();
    assert_eq!(
        HistoryEntry::decode(&anchor.encode().unwrap()).unwrap(),
        HistoryEntry::Anchor(anchor)
    );

    let delta = HistoryDelta::put(commit, interval(1, None), payload("value"));
    let mut corrupt = delta.encode().unwrap();
    let payload_byte = corrupt.len() - 9;
    corrupt[payload_byte] ^= 1;
    assert_eq!(
        HistoryDelta::decode(&corrupt),
        Err(RecordCodecError::ChecksumMismatch)
    );
}
