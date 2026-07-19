use std::collections::BTreeMap;

use bolt_protocol::{ClientMessage, Value, decode_client_message, encode_client_message};

#[test]
fn round_trips_run_and_pull_messages() {
    let run = ClientMessage::Run {
        query: "MATCH (n) RETURN n".into(),
        parameters: BTreeMap::from([("limit".into(), Value::Integer(10))]),
        extra: BTreeMap::new(),
    };
    let pull = ClientMessage::Pull {
        n: 100,
        query_id: Some(7),
    };

    for message in [run, pull] {
        let encoded = encode_client_message(&message).expect("message should encode");
        let decoded = decode_client_message(&encoded).expect("message should decode");
        assert_eq!(decoded, message);
    }
}

#[test]
fn rejects_unknown_message_signatures() {
    let error = decode_client_message(&[0xB0, 0x7E]).expect_err("signature must fail");

    assert_eq!(error.code(), "DTG-BOLT-UNKNOWN-MESSAGE");
}
