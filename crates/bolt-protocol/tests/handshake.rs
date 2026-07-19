use bolt_protocol::{BOLT_MAGIC, BoltVersion, Handshake, negotiate};

#[test]
fn decodes_the_fixed_handshake_preamble() {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&BOLT_MAGIC.to_be_bytes());
    bytes.extend_from_slice(&[0, 0, 8, 5]);
    bytes.extend_from_slice(&[0, 0, 7, 5]);
    bytes.extend_from_slice(&[0, 0, 4, 4]);
    bytes.extend_from_slice(&[0, 0, 0, 0]);

    let handshake = Handshake::decode(&bytes).expect("handshake should decode");

    assert_eq!(handshake.proposals()[0], BoltVersion::new(5, 8, 0));
    assert_eq!(handshake.proposals()[2], BoltVersion::new(4, 4, 0));
}

#[test]
fn selects_the_highest_mutually_supported_version() {
    let proposals = [
        BoltVersion::new(5, 8, 3),
        BoltVersion::new(4, 4, 0),
        BoltVersion::new(0, 0, 0),
        BoltVersion::new(0, 0, 0),
    ];
    let supported = [BoltVersion::new(5, 6, 0), BoltVersion::new(5, 7, 0)];

    assert_eq!(
        negotiate(&proposals, &supported),
        Some(BoltVersion::new(5, 7, 0))
    );
}

#[test]
fn rejects_an_invalid_magic_number() {
    let error = Handshake::decode(&[0; 20]).expect_err("magic must fail");

    assert_eq!(error.code(), "DTG-BOLT-INVALID-MAGIC");
}
