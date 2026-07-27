use std::collections::BTreeMap;

use temporal_types::{CanonicalElement, CanonicalElementRef, CodecError, GraphValue};

#[test]
fn borrowed_view_finds_one_property_without_materializing_the_map() {
    let encoded = CanonicalElement::new(
        7,
        BTreeMap::from([
            (1, GraphValue::Integer(11)),
            (9, GraphValue::String("kept".into())),
        ]),
    )
    .encode()
    .unwrap();
    let view = CanonicalElementRef::parse(&encoded).unwrap();
    assert_eq!(view.schema_version(), 7);
    assert_eq!(
        view.property(1).unwrap().unwrap().decode().unwrap(),
        GraphValue::Integer(11)
    );
    assert_eq!(view.property(2).unwrap(), None);
    assert_eq!(view.encoded(), encoded.as_slice());
}

#[test]
fn borrowed_view_rejects_truncated_values() {
    let encoded = CanonicalElement::new(1, BTreeMap::new()).encode().unwrap();
    assert_eq!(
        CanonicalElementRef::parse(&encoded[..encoded.len() - 1]),
        Err(CodecError::UnexpectedEnd)
    );
}

#[test]
fn projection_deduplicates_demanded_properties_in_canonical_id_order() {
    let encoded = CanonicalElement::new(
        7,
        BTreeMap::from([
            (1, GraphValue::Integer(11)),
            (4, GraphValue::Boolean(true)),
            (9, GraphValue::String("kept".into())),
        ]),
    )
    .encode()
    .unwrap();

    let projected = CanonicalElementRef::parse(&encoded)
        .unwrap()
        .project(&[9, 1, 9])
        .unwrap();

    assert_eq!(projected.schema_version(), 7);
    assert_eq!(
        projected.properties(),
        &BTreeMap::from([
            (1, GraphValue::Integer(11)),
            (9, GraphValue::String("kept".into())),
        ])
    );
    assert_eq!(
        projected.encode().unwrap(),
        CanonicalElement::new(
            7,
            BTreeMap::from([
                (1, GraphValue::Integer(11)),
                (9, GraphValue::String("kept".into())),
            ])
        )
        .encode()
        .unwrap()
    );
}

#[test]
fn borrowed_view_rejects_nested_list_bytes_and_string_corruption() {
    let nested_list = CanonicalElement::new(
        1,
        BTreeMap::from([(
            1,
            GraphValue::List(vec![GraphValue::List(vec![GraphValue::Null])]),
        )]),
    )
    .encode()
    .unwrap();
    assert_eq!(
        CanonicalElementRef::parse(&nested_list[..nested_list.len() - 1]),
        Err(CodecError::InvalidCollectionLength)
    );

    let mut nested_string = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::List(vec![GraphValue::String("x".into())]))]),
    )
    .encode()
    .unwrap();
    *nested_string.last_mut().unwrap() = 0xff;
    assert_eq!(
        CanonicalElementRef::parse(&nested_string),
        Err(CodecError::InvalidUtf8)
    );

    let nested_bytes = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::List(vec![GraphValue::Bytes(vec![1])]))]),
    )
    .encode()
    .unwrap();
    assert_eq!(
        CanonicalElementRef::parse(&nested_bytes[..nested_bytes.len() - 1]),
        Err(CodecError::UnexpectedEnd)
    );
}
