use bolt_protocol::{ChunkDecoder, encode_chunks};

#[test]
fn encodes_and_incrementally_decodes_chunked_messages() {
    let payload = b"abcdefghij";
    let framed = encode_chunks(payload, 4).expect("message should frame");
    let mut decoder = ChunkDecoder::new(1024, 4).expect("decoder should build");
    let mut messages = Vec::new();

    for byte in framed {
        messages.extend(decoder.push(&[byte]).expect("fragment should decode"));
    }

    assert_eq!(messages, vec![payload.to_vec()]);
}

#[test]
fn decodes_multiple_messages_from_one_network_read() {
    let mut framed = encode_chunks(b"one", 16).expect("frame one");
    framed.extend(encode_chunks(b"two", 16).expect("frame two"));
    let mut decoder = ChunkDecoder::new(1024, 16).expect("decoder should build");

    let messages = decoder.push(&framed).expect("messages should decode");

    assert_eq!(messages, vec![b"one".to_vec(), b"two".to_vec()]);
}

#[test]
fn rejects_a_message_larger_than_its_limit_before_appending_payload() {
    let framed = encode_chunks(b"0123456789", 10).expect("message should frame");
    let mut decoder = ChunkDecoder::new(8, 10).expect("decoder should build");
    let error = decoder.push(&framed).expect_err("message limit must fail");

    assert_eq!(error.code(), "DTG-BOLT-MESSAGE-LIMIT");
}
