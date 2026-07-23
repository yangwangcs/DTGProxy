use storage_api::{AdapterError, KeySpan, KeySpanError, Keyspace};

#[test]
fn prefix_span_has_an_exclusive_lexicographic_upper_bound() {
    let span = KeySpan::prefix(Keyspace::History, vec![0x20, 0x12, 0xff]);

    assert_eq!(span.keyspace(), Keyspace::History);
    assert_eq!(span.start(), &[0x20, 0x12, 0xff]);
    assert_eq!(span.end(), Some(&[0x20, 0x13][..]));
    assert!(span.contains(&[0x20, 0x12, 0xff]));
    assert!(span.contains(&[0x20, 0x12, 0xff, 0x00]));
    assert!(!span.contains(&[0x20, 0x13]));
}

#[test]
fn empty_and_all_ff_prefixes_have_an_unbounded_upper_end() {
    let all = KeySpan::prefix(Keyspace::Current, Vec::new());
    let terminal = KeySpan::prefix(Keyspace::Current, vec![0xff, 0xff]);

    assert_eq!(all.end(), None);
    assert!(all.contains(b"anything"));
    assert_eq!(terminal.end(), None);
    assert!(terminal.contains(&[0xff, 0xff, 0x01]));
    assert!(!terminal.contains(&[0xff, 0xfe, 0xff]));
}

#[test]
fn bounded_range_and_prefix_seek_have_stable_inclusive_exclusive_semantics() {
    let range = KeySpan::range(Keyspace::History, b"b".to_vec(), Some(b"d".to_vec())).unwrap();
    assert!(range.contains(b"b"));
    assert!(range.contains(b"c"));
    assert!(!range.contains(b"a"));
    assert!(!range.contains(b"d"));

    let seek = KeySpan::prefix_from(Keyspace::History, b"edge:".to_vec(), b"edge:2".to_vec())
        .unwrap()
        .with_limit(2)
        .unwrap()
        .with_max_bytes(64)
        .unwrap();
    assert_eq!(seek.start(), b"edge:2");
    assert_eq!(seek.limit(), Some(2));
    assert_eq!(seek.max_bytes(), Some(64));
    assert!(seek.contains(b"edge:2"));
    assert!(seek.contains(b"edge:9"));
    assert!(!seek.contains(b"other"));
}

#[test]
fn invalid_ranges_seeks_and_limits_are_rejected() {
    assert_eq!(
        KeySpan::range(Keyspace::Current, b"z".to_vec(), Some(b"a".to_vec())),
        Err(KeySpanError::EmptyOrReversed)
    );
    assert_eq!(
        KeySpan::prefix_from(Keyspace::Current, b"edge:".to_vec(), b"vertex:".to_vec()),
        Err(KeySpanError::StartOutsidePrefix)
    );
    assert_eq!(
        KeySpan::prefix(Keyspace::Current, Vec::new()).with_limit(0),
        Err(KeySpanError::ZeroLimit)
    );
    assert_eq!(
        KeySpan::prefix(Keyspace::Current, Vec::new()).with_max_bytes(0),
        Err(KeySpanError::ZeroByteLimit)
    );
}

#[test]
fn scan_response_body_limit_has_a_distinct_wire_byte_contract() {
    let error = AdapterError::ScanResponseByteLimit {
        limit: 64,
        required: 65,
    };

    assert_eq!(
        error.to_string(),
        "scan response body requires 65 wire bytes above limit 64"
    );
}
