use std::collections::BTreeMap;

use temporal_types::{CanonicalElement, GraphValue};

#[test]
fn canonical_payload_round_trips_without_type_loss() {
    let properties = BTreeMap::from([
        (1, GraphValue::Integer(-7)),
        (2, GraphValue::String("risk".to_owned())),
        (3, GraphValue::Bytes(vec![0, 1, 255])),
        (
            4,
            GraphValue::List(vec![GraphValue::Boolean(true), GraphValue::Null]),
        ),
        (5, GraphValue::FloatBits(1.25_f64.to_bits())),
        (6, GraphValue::TimestampMicros(1_700_000_000_000_000)),
    ]);
    let element = CanonicalElement::new(9, properties);

    let bytes = element.encode().unwrap();

    assert_eq!(CanonicalElement::decode(&bytes).unwrap(), element);
}

#[test]
fn canonical_encoding_is_independent_of_insertion_order() {
    let left = CanonicalElement::new(
        1,
        BTreeMap::from([
            (7, GraphValue::Integer(8)),
            (2, GraphValue::String("x".to_owned())),
        ]),
    );
    let right = CanonicalElement::new(
        1,
        BTreeMap::from([
            (2, GraphValue::String("x".to_owned())),
            (7, GraphValue::Integer(8)),
        ]),
    );

    assert_eq!(left.encode().unwrap(), right.encode().unwrap());
}

#[test]
fn decoder_rejects_trailing_or_truncated_data() {
    let value = CanonicalElement::new(1, BTreeMap::new());
    let encoded = value.encode().unwrap();
    let mut trailing = encoded.clone();
    trailing.push(0);

    assert!(CanonicalElement::decode(&trailing).is_err());
    assert!(CanonicalElement::decode(&encoded[..5]).is_err());
}

#[test]
fn decoder_rejects_values_nested_beyond_the_limit() {
    let mut nested = GraphValue::Null;
    for _ in 0..65 {
        nested = GraphValue::List(vec![nested]);
    }
    let element = CanonicalElement::new(1, BTreeMap::from([(1, nested)]));

    assert!(element.encode().is_err());
}
