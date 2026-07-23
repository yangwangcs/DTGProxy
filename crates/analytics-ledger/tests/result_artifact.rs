use std::collections::BTreeMap;

use analytics_api::{AlgorithmResult, AlgorithmValue, VertexId};
use analytics_ledger::{
    ProviderCheckpointArtifactV1, decode_provider_checkpoint_artifact,
    encode_provider_checkpoint_artifact,
};
use analytics_ledger::{decode_algorithm_result_artifact, encode_algorithm_result_artifact};
use temporal_types::ValidTime;

fn result() -> AlgorithmResult {
    AlgorithmResult::new(
        vec![
            "null".into(),
            "bool".into(),
            "integer".into(),
            "float".into(),
            "string".into(),
            "vertex".into(),
            "time".into(),
        ],
        vec![vec![
            AlgorithmValue::Null,
            AlgorithmValue::Boolean(true),
            AlgorithmValue::Integer(-7),
            AlgorithmValue::FloatBits(f64::NAN.to_bits()),
            AlgorithmValue::String("canonical".into()),
            AlgorithmValue::Vertex(VertexId::new(42)),
            AlgorithmValue::Time(ValidTime::from_micros(99)),
        ]],
        BTreeMap::from([
            ("algorithm".into(), AlgorithmValue::String("degree".into())),
            ("completed".into(), AlgorithmValue::Integer(1)),
        ]),
    )
    .unwrap()
}

#[test]
fn result_artifact_round_trips_every_value_kind_byte_identically() {
    let expected = result();
    let encoded = encode_algorithm_result_artifact(&expected).unwrap();
    let decoded = decode_algorithm_result_artifact(&encoded).unwrap();

    assert_eq!(decoded, expected);
    assert_eq!(encode_algorithm_result_artifact(&decoded).unwrap(), encoded);
}

#[test]
fn result_artifact_rejects_corruption_truncation_and_trailing_bytes() {
    let encoded = encode_algorithm_result_artifact(&result()).unwrap();

    for length in 0..encoded.len() {
        assert!(decode_algorithm_result_artifact(&encoded[..length]).is_err());
    }

    let mut corrupt = encoded.clone();
    corrupt[12] ^= 0x80;
    assert!(decode_algorithm_result_artifact(&corrupt).is_err());

    let mut trailing = encoded;
    trailing.push(0);
    assert!(decode_algorithm_result_artifact(&trailing).is_err());
}

#[test]
fn provider_checkpoint_round_trips_native_state_and_rejects_corruption() {
    let prefix = AlgorithmResult::new(
        vec!["degree".into()],
        vec![
            vec![AlgorithmValue::Integer(1)],
            vec![AlgorithmValue::Integer(2)],
            vec![AlgorithmValue::Integer(3)],
        ],
        BTreeMap::new(),
    )
    .unwrap();
    let prefix = encode_algorithm_result_artifact(&prefix).unwrap();
    let checkpoint = ProviderCheckpointArtifactV1::new(
        [7; 32],
        [9; 32],
        "dtg.graph.pageRank",
        3,
        b"native-rank-vector".to_vec(),
        prefix,
    )
    .unwrap();
    let bytes = encode_provider_checkpoint_artifact(checkpoint.clone());
    assert_eq!(
        decode_provider_checkpoint_artifact(&bytes).unwrap(),
        checkpoint
    );
    let mut corrupt = bytes.clone();
    corrupt[20] ^= 1;
    assert!(decode_provider_checkpoint_artifact(&corrupt).is_err());
    assert!(decode_provider_checkpoint_artifact(&bytes[..bytes.len() - 1]).is_err());
}
