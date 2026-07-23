use std::collections::BTreeMap;

use storage_api::{Keyspace, LogicalKey};
use temporal_storage::{
    EdgeTypeId, ElementId, ElementRef, GraphId, HistoryAnchor, HistoryEntry, LabelId, PartitionId,
    ProjectionRecord, ValidSegment, VertexIdentity, current_edge_key, current_vertex_key,
    decode_canonical_graph_entry, encode_canonical_graph_entry, history_anchor_key,
    out_adjacency_key,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn vertex() -> ElementRef {
    ElementRef::vertex(GraphId::new(7), PartitionId::new(2), ElementId::new(11))
}

fn edge() -> ElementRef {
    ElementRef::edge(GraphId::new(7), PartitionId::new(2), ElementId::new(12))
}

fn projection_at(commit_ts: TransactionTime) -> ProjectionRecord {
    let payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String("value".to_owned()))]),
    );
    ProjectionRecord::new(
        commit_ts,
        vec![ValidSegment::new(
            Interval::new(ValidTime::from_micros(1), None).unwrap(),
            payload,
        )],
    )
    .unwrap()
}

fn projection() -> ProjectionRecord {
    projection_at(TransactionTime::new(20, 1))
}

#[test]
fn graph_entries_decode_and_reencode_byte_identically() {
    let vertex_identity = VertexIdentity::new(vertex(), LabelId::new(3)).unwrap();
    let cases = [
        (
            temporal_storage::vertex_identity_key(vertex()),
            vertex_identity.encode(),
        ),
        (current_vertex_key(vertex()), projection().encode().unwrap()),
        (current_edge_key(edge()), projection().encode().unwrap()),
        (
            out_adjacency_key(
                vertex().graph(),
                vertex().partition(),
                vertex().id(),
                EdgeTypeId::new(5),
                0,
                ElementId::new(99),
                edge().id(),
            ),
            projection().encode().unwrap(),
        ),
    ];

    for (key, value) in cases {
        let entry = decode_canonical_graph_entry(&key, &value).unwrap();
        assert_eq!(encode_canonical_graph_entry(&entry).unwrap(), (key, value));
    }
}

#[test]
fn edge_history_and_opaque_entries_round_trip_without_loss() {
    let history_key = history_anchor_key(edge(), TransactionTime::new(30, 0), 0);
    let history = HistoryEntry::Anchor(
        HistoryAnchor::new(
            TransactionTime::new(30, 0),
            Interval::new(ValidTime::from_micros(2), None).unwrap(),
            projection_at(TransactionTime::new(30, 0)),
        )
        .unwrap(),
    );
    let history_value = history.encode().unwrap();
    let history_entry = decode_canonical_graph_entry(&history_key, &history_value).unwrap();
    assert_eq!(
        encode_canonical_graph_entry(&history_entry).unwrap(),
        (history_key, history_value)
    );

    let opaque_key = LogicalKey::in_keyspace(Keyspace::Txn, b"opaque".to_vec());
    let opaque_value = vec![9, 8, 7];
    let opaque_entry = decode_canonical_graph_entry(&opaque_key, &opaque_value).unwrap();
    assert_eq!(
        encode_canonical_graph_entry(&opaque_entry).unwrap(),
        (opaque_key, opaque_value)
    );
}

#[test]
fn mismatched_identity_and_value_are_rejected() {
    let key = current_vertex_key(vertex());
    let value = VertexIdentity::new(vertex(), LabelId::new(3))
        .unwrap()
        .encode();
    assert!(decode_canonical_graph_entry(&key, &value).is_err());
}

#[test]
fn history_key_timestamp_must_match_encoded_entry_timestamp() {
    let key = history_anchor_key(edge(), TransactionTime::new(30, 0), 0);
    let value = HistoryEntry::Anchor(
        HistoryAnchor::new(
            TransactionTime::new(31, 0),
            Interval::new(ValidTime::from_micros(2), None).unwrap(),
            projection_at(TransactionTime::new(31, 0)),
        )
        .unwrap(),
    )
    .encode()
    .unwrap();
    assert!(decode_canonical_graph_entry(&key, &value).is_err());
}
