use std::collections::BTreeMap;

use analytics_api::{AlgorithmValue, VertexId};
use analytics_ledger::{decode_algorithm_parameters, encode_algorithm_parameters};
use temporal_types::ValidTime;

fn every_value() -> BTreeMap<String, AlgorithmValue> {
    BTreeMap::from([
        ("boolean".into(), AlgorithmValue::Boolean(true)),
        (
            "float".into(),
            AlgorithmValue::FloatBits((-3.5_f64).to_bits()),
        ),
        ("integer".into(), AlgorithmValue::Integer(-17)),
        ("null".into(), AlgorithmValue::Null),
        ("string".into(), AlgorithmValue::String("时态".into())),
        (
            "time".into(),
            AlgorithmValue::Time(ValidTime::from_micros(-23)),
        ),
        (
            "vertex".into(),
            AlgorithmValue::Vertex(VertexId::new(u128::MAX)),
        ),
    ])
}

#[test]
fn typed_parameter_codec_round_trips_every_current_value_canonically() {
    let parameters = every_value();
    let encoded = encode_algorithm_parameters(&parameters).expect("canonical parameters");

    assert_eq!(
        decode_algorithm_parameters(&encoded).expect("decode canonical parameters"),
        parameters
    );
    assert_eq!(
        encode_algorithm_parameters(&decode_algorithm_parameters(&encoded).unwrap()).unwrap(),
        encoded
    );
}

#[test]
fn typed_parameter_codec_is_independent_of_insertion_order() {
    let mut ascending = BTreeMap::new();
    ascending.insert("alpha".into(), AlgorithmValue::Integer(1));
    ascending.insert("omega".into(), AlgorithmValue::String("last".into()));
    let mut descending = BTreeMap::new();
    descending.insert("omega".into(), AlgorithmValue::String("last".into()));
    descending.insert("alpha".into(), AlgorithmValue::Integer(1));

    assert_eq!(
        encode_algorithm_parameters(&ascending).unwrap(),
        encode_algorithm_parameters(&descending).unwrap()
    );
}

#[test]
fn typed_parameter_codec_rejects_oversize_and_corruption() {
    let oversized = BTreeMap::from([(
        "value".into(),
        AlgorithmValue::String("x".repeat(1024 * 1024)),
    )]);
    assert!(encode_algorithm_parameters(&oversized).is_err());

    let encoded = encode_algorithm_parameters(&every_value()).unwrap();
    for corrupt in [
        encoded[..encoded.len() - 1].to_vec(),
        {
            let mut bytes = encoded.clone();
            let offset = bytes.len() / 2;
            bytes[offset] ^= 0x80;
            bytes
        },
        {
            let mut bytes = encoded.clone();
            bytes.extend_from_slice(&[0]);
            bytes
        },
    ] {
        assert!(decode_algorithm_parameters(&corrupt).is_err());
    }
}
