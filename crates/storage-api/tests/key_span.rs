use storage_api::{KeySpan, Keyspace};

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
