use cypher_syntax::{CypherVersion, TokenKind, lex};

#[test]
fn lexes_identifiers_parameters_and_punctuation_with_source_spans() {
    let source = "MATCH (n:Account {id: $account_id}) RETURN n";
    let lexed = lex(source).expect("query should lex");

    assert_eq!(lexed.profile().version(), CypherVersion::V25);
    assert_eq!(lexed.tokens()[0].kind(), &TokenKind::Word("MATCH".into()));
    assert_eq!(lexed.tokens()[0].span().start(), 0);
    assert_eq!(lexed.tokens()[0].span().end(), 5);
    assert!(
        lexed
            .tokens()
            .iter()
            .any(|token| token.kind() == &TokenKind::Parameter("account_id".into()))
    );
    assert_eq!(lexed.source(&lexed.tokens()[1]), "(");
}

#[test]
fn skips_line_and_block_comments_without_losing_original_offsets() {
    let source = "// heading\nMATCH /* entity */ (n) RETURN n";
    let lexed = lex(source).expect("query should lex");

    let first = &lexed.tokens()[0];
    assert_eq!(first.kind(), &TokenKind::Word("MATCH".into()));
    assert_eq!(first.span().start(), 11);
}

#[test]
fn decodes_escaped_identifiers_and_string_literals() {
    let source = "MATCH (`odd``name` {text: 'it\\'s'}) RETURN `odd``name`";
    let lexed = lex(source).expect("query should lex");

    assert!(
        lexed
            .tokens()
            .iter()
            .any(|token| { token.kind() == &TokenKind::EscapedIdentifier("odd`name".into()) })
    );
    assert!(
        lexed
            .tokens()
            .iter()
            .any(|token| token.kind() == &TokenKind::String("it's".into()))
    );
}

#[test]
fn reports_unterminated_literals_at_their_start() {
    let error = lex("RETURN 'missing").expect_err("unterminated string must fail");

    assert_eq!(error.code(), "DTG-CYPHER-UNTERMINATED-STRING");
    assert_eq!(error.span().start(), 7);
}
