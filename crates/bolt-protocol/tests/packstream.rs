use std::collections::BTreeMap;

use bolt_protocol::{PackStreamLimits, Value, decode, decode_with_limits, encode};

#[test]
fn round_trips_nested_packstream_values() {
    let value = Value::Map(BTreeMap::from([
        ("name".into(), Value::String("DTGProxy".into())),
        (
            "values".into(),
            Value::List(vec![
                Value::Null,
                Value::Boolean(true),
                Value::Integer(-17),
                Value::FloatBits(42.5_f64.to_bits()),
                Value::Bytes(vec![1, 2, 3]),
            ]),
        ),
    ]));

    let encoded = encode(&value).expect("value should encode");
    let decoded = decode(&encoded).expect("value should decode");

    assert_eq!(decoded, value);
}

#[test]
fn uses_tiny_markers_for_small_strings_lists_and_maps() {
    assert_eq!(
        encode(&Value::String("abc".into())).expect("encode")[0],
        0x83
    );
    assert_eq!(encode(&Value::List(vec![])).expect("encode")[0], 0x90);
    assert_eq!(
        encode(&Value::Map(BTreeMap::new())).expect("encode")[0],
        0xA0
    );
}

#[test]
fn rejects_values_deeper_than_the_configured_limit() {
    let bytes = [0x91, 0x91, 0x91, 0xC0];
    let limits = PackStreamLimits::new(1024, 1024, 2).expect("limits should build");
    let error = decode_with_limits(&bytes, limits).expect_err("depth must fail");

    assert_eq!(error.code(), "DTG-BOLT-NESTING-LIMIT");
}

#[test]
fn rejects_trailing_bytes() {
    let error = decode(&[0xC0, 0xC0]).expect_err("trailing byte must fail");

    assert_eq!(error.code(), "DTG-BOLT-TRAILING-BYTES");
}
