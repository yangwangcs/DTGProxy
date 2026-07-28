use std::collections::BTreeMap;

use temporal_storage::{
    CanonicalTemporalEvent, EdgeTypeId, ElementId, ElementRef, GraphId, GraphKey, LabelId,
    PartitionId, TemporalEventMetadata, TemporalEventOperation, decode_graph_key, graph_key_scope,
    temporal_event_graph_prefix, temporal_event_key, temporal_event_valid_key,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

fn vertex(id: u128) -> ElementRef {
    ElementRef::vertex(GraphId::new(7), PartitionId::new(2), ElementId::new(id))
}

#[test]
fn event_codec_round_trips_and_commit_order_is_encoded_in_the_key() {
    let event = CanonicalTemporalEvent::put(
        vertex(4),
        Interval::new(ValidTime::from_micros(10), None).unwrap(),
        TransactionTime::new(20, 3),
        1,
        CanonicalElement::new(1, BTreeMap::from([(1, GraphValue::String("Ada".into()))])),
    )
    .expect("event");
    let decoded = CanonicalTemporalEvent::decode(&event.encode().expect("encode")).expect("decode");
    assert_eq!(decoded, event);
    assert!(
        temporal_event_key(&event)
            .as_bytes()
            .starts_with(&temporal_event_graph_prefix(GraphId::new(7)))
    );
    assert_eq!(
        decode_graph_key(&temporal_event_key(&event)).unwrap(),
        GraphKey::TemporalEvent {
            element: event.element(),
            commit_ts: event.commit_ts(),
            ordinal: event.ordinal(),
        }
    );
    let valid_key = decode_graph_key(&temporal_event_valid_key(&event)).unwrap();
    assert_eq!(
        valid_key,
        GraphKey::TemporalEventValid {
            element: event.element(),
            valid_from: event.valid().start(),
            commit_ts: event.commit_ts(),
            ordinal: event.ordinal(),
        }
    );
    assert_eq!(
        graph_key_scope(valid_key),
        (GraphId::new(7), PartitionId::new(2))
    );

    let later = CanonicalTemporalEvent::delete(
        vertex(5),
        Interval::new(ValidTime::from_micros(11), None).unwrap(),
        TransactionTime::new(21, 0),
        0,
    )
    .expect("delete event");
    assert!(temporal_event_key(&event).as_bytes() < temporal_event_key(&later).as_bytes());
    assert!(
        temporal_event_valid_key(&event).as_bytes() < temporal_event_valid_key(&later).as_bytes()
    );

    let negative_valid = CanonicalTemporalEvent::delete(
        vertex(6),
        Interval::new(ValidTime::from_micros(-1), None).unwrap(),
        TransactionTime::new(22, 0),
        0,
    )
    .expect("negative valid-time event");
    assert!(
        temporal_event_valid_key(&negative_valid).as_bytes()
            < temporal_event_valid_key(&event).as_bytes()
    );
    assert_eq!(later.operation(), TemporalEventOperation::Delete);
    assert!(later.payload().is_none());
}

#[test]
fn event_codec_preserves_edge_puts_and_rejects_checksum_corruption() {
    let edge = ElementRef::edge(GraphId::new(7), PartitionId::new(2), ElementId::new(9));
    let event = CanonicalTemporalEvent::put_with_metadata(
        edge,
        Interval::new(ValidTime::from_micros(10), Some(ValidTime::from_micros(20))).unwrap(),
        TransactionTime::new(20, 3),
        2,
        CanonicalElement::new(2, BTreeMap::new()),
        TemporalEventMetadata::edge(EdgeTypeId::new(4), vertex(1), vertex(2)),
    )
    .expect("event");

    let mut encoded = event.encode().expect("encode");
    encoded[0] ^= 1;
    assert!(CanonicalTemporalEvent::decode(&encoded).is_err());

    assert_eq!(event.element(), edge);
    assert_eq!(event.operation(), TemporalEventOperation::Put);
    assert_eq!(
        event.metadata(),
        Some(&TemporalEventMetadata::edge(
            EdgeTypeId::new(4),
            vertex(1),
            vertex(2)
        ))
    );
}

#[test]
fn event_codec_preserves_vertex_identity_metadata() {
    let event = CanonicalTemporalEvent::delete_with_metadata(
        vertex(4),
        Interval::new(ValidTime::from_micros(10), None).unwrap(),
        TransactionTime::new(20, 3),
        1,
        TemporalEventMetadata::vertex(LabelId::new(7)),
    )
    .expect("event");
    assert_eq!(
        CanonicalTemporalEvent::decode(&event.encode().expect("encode"))
            .expect("decode")
            .metadata(),
        Some(&TemporalEventMetadata::vertex(LabelId::new(7)))
    );
}

#[test]
fn event_codec_rejects_payload_for_delete() {
    let result = CanonicalTemporalEvent::new(
        vertex(4),
        TemporalEventOperation::Delete,
        Interval::new(ValidTime::from_micros(10), None).unwrap(),
        TransactionTime::new(20, 3),
        1,
        Some(CanonicalElement::new(1, BTreeMap::new())),
    );

    assert!(result.is_err());
}
