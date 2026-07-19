use cypher_syntax::{SyntaxLimits, lex_with_limits};

#[test]
fn rejects_query_larger_than_the_configured_byte_limit() {
    let limits = SyntaxLimits::new(8, 32, 8, 32).expect("limits should be valid");
    let error = lex_with_limits("MATCH (n)", limits).expect_err("query is too large");

    assert_eq!(error.code(), "DTG-CYPHER-QUERY-TOO-LARGE");
}

#[test]
fn rejects_more_tokens_than_the_configured_limit() {
    let limits = SyntaxLimits::new(1_024, 3, 8, 32).expect("limits should be valid");
    let error = lex_with_limits("RETURN 1 + 2", limits).expect_err("too many tokens");

    assert_eq!(error.code(), "DTG-CYPHER-TOO-MANY-TOKENS");
}
