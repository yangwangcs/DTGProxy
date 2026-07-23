use cypher_sema::{CypherType, QueryEffect, SemanticAnalyzer};
use cypher_syntax::parse;

#[test]
fn subquery_requires_explicit_import_and_exports_only_declared_columns() {
    let analyzed = SemanticAnalyzer::new()
        .analyze(
            &parse(
                "UNWIND [1] AS outer_value \
                 CALL (outer_value) { UNWIND [outer_value] AS hidden RETURN hidden AS exported } \
                 RETURN exported",
            )
            .unwrap(),
        )
        .expect("explicit import and exported RETURN field should bind");

    assert_eq!(analyzed.output()[0].name(), "exported");
    assert_eq!(analyzed.output()[0].cypher_type(), &CypherType::Integer);

    for query in [
        "UNWIND [1] AS outer_value CALL () { RETURN outer_value AS leaked } RETURN leaked",
        "UNWIND [1] AS outer_value CALL (outer_value) { RETURN outer_value AS exported } RETURN outer_value, hidden",
    ] {
        let error = SemanticAnalyzer::new()
            .analyze(&parse(query).unwrap())
            .expect_err("unimported or unexported child symbols must not leak");
        assert_eq!(error.code(), "DTG-CYPHER-UNBOUND-VARIABLE", "{query}");
    }
}

#[test]
fn subquery_rejects_outer_leak_shadow_collision_and_invalid_write_context() {
    for (query, expected_code) in [
        (
            "UNWIND [1] AS existing CALL () { RETURN 2 AS existing } RETURN existing",
            "DTG-CYPHER-SUBQUERY-EXPORT-COLLISION",
        ),
        (
            "CALL (missing) { RETURN missing AS value } RETURN value",
            "DTG-CYPHER-UNKNOWN-SUBQUERY-IMPORT",
        ),
        (
            "UNWIND [1] AS value CALL (value, value) { RETURN value AS copy } RETURN copy",
            "DTG-CYPHER-DUPLICATE-SUBQUERY-IMPORT",
        ),
        (
            "CALL () { RETURN 1 AS value, 2 AS value } RETURN value",
            "DTG-CYPHER-DUPLICATE-SUBQUERY-EXPORT",
        ),
    ] {
        let error = SemanticAnalyzer::new()
            .analyze(&parse(query).unwrap())
            .expect_err("invalid subquery scope must be rejected");
        assert_eq!(error.code(), expected_code, "{query}");
    }

    let write = SemanticAnalyzer::new()
        .analyze(&parse("CALL () { CREATE (n:Seen) RETURN n }").unwrap())
        .expect("ordinary write subquery effect should compose with its parent");
    assert_eq!(write.effect(), QueryEffect::Write);
}

#[test]
fn exists_and_count_subqueries_validate_scope_and_have_stable_types() {
    let analyzed = SemanticAnalyzer::new()
        .analyze(
            &parse(
                "UNWIND [1] AS outer_value \
                 WHERE EXISTS { RETURN outer_value AS visible } \
                 RETURN COUNT { UNWIND [outer_value] AS value RETURN value } AS total",
            )
            .unwrap(),
        )
        .expect("correlated expression subqueries should inherit the visible scope");
    assert_eq!(analyzed.output()[0].name(), "total");
    assert_eq!(analyzed.output()[0].cypher_type(), &CypherType::Integer);

    let unbound = SemanticAnalyzer::new()
        .analyze(
            &parse("UNWIND [1] AS outer_value WHERE EXISTS { RETURN missing AS value } RETURN outer_value")
                .unwrap(),
        )
        .expect_err("expression subquery body must be semantically analyzed");
    assert_eq!(unbound.code(), "DTG-CYPHER-UNBOUND-VARIABLE");

    let write = SemanticAnalyzer::new()
        .analyze(&parse("RETURN EXISTS { CREATE (n:Forbidden) RETURN n } AS value").unwrap())
        .expect_err("expression subqueries cannot mutate the graph");
    assert_eq!(write.code(), "DTG-CYPHER-SUBQUERY-EXPRESSION-WRITE");
}
