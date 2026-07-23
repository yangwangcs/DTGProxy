use cypher_sema::{CypherType, QueryEffect, SemanticAnalyzer};
use cypher_syntax::parse;
use procedure_runtime::{
    ProcedureAccess, ProcedureCatalog, ProcedureDefinition, ProcedureEffect, ProcedureField,
    ProcedurePermission, ProcedurePlacement,
};
use temporal_ir::ValueType;

#[test]
fn resolves_procedure_signature_and_types_yield_scope() {
    let parsed = parse(
        "CALL dtg.graph.degree({}) YIELD vertexId, degree AS score \
         WITH vertexId, score WHERE score >= 0 RETURN vertexId, score",
    )
    .expect("query parses");
    let catalog = ProcedureCatalog::builtin_analytics().unwrap();
    let analyzed = SemanticAnalyzer::new()
        .analyze_with_procedures(&parsed, &catalog, &ProcedureAccess::analytics_read())
        .expect("procedure should resolve");

    assert_eq!(analyzed.effect(), QueryEffect::ReadOnly);
    assert_eq!(analyzed.procedures().len(), 1);
    assert_eq!(
        analyzed
            .output()
            .iter()
            .map(|field| (field.name(), field.cypher_type()))
            .collect::<Vec<_>>(),
        vec![
            ("vertexId", &CypherType::String),
            ("score", &CypherType::Integer),
        ]
    );
    let procedure = &analyzed.procedures()[0];
    assert_eq!(
        procedure.descriptor().identity().catalog_revision(),
        catalog.revision()
    );
    assert_eq!(procedure.arguments().len(), 0);
    assert_eq!(
        procedure
            .yields()
            .iter()
            .map(|item| (item.source_name(), item.output_name()))
            .collect::<Vec<_>>(),
        vec![("vertexId", "vertexId"), ("degree", "score")]
    );

    let parsed = parse("CALL dtg.graph.degree({}) YIELD * RETURN degree").unwrap();
    SemanticAnalyzer::new()
        .analyze_with_procedures(&parsed, &catalog, &ProcedureAccess::analytics_read())
        .expect("YIELD * binds the exact output schema");
}

#[test]
fn rejects_unknown_procedure_argument_output_permission_and_unstaged_write_effect() {
    let catalog = ProcedureCatalog::builtin_analytics().unwrap();
    let analyzer = SemanticAnalyzer::new();
    let access = ProcedureAccess::analytics_read();
    let cases = [
        (
            "CALL dtg.graph.missing() YIELD value RETURN value",
            "DTG-CYPHER-UNKNOWN-PROCEDURE",
        ),
        (
            "CALL dtg.graph.degree({unknown: 1}) YIELD degree RETURN degree",
            "DTG-CYPHER-UNKNOWN-PROCEDURE-ARGUMENT",
        ),
        (
            "CALL dtg.graph.bfs({}) YIELD vertexId RETURN vertexId",
            "DTG-CYPHER-MISSING-PROCEDURE-ARGUMENT",
        ),
        (
            "CALL dtg.graph.bfs({source: true}) YIELD vertexId RETURN vertexId",
            "DTG-CYPHER-PROCEDURE-ARGUMENT-TYPE",
        ),
        (
            "CALL dtg.graph.degree({}) YIELD missing RETURN missing",
            "DTG-CYPHER-UNKNOWN-YIELD",
        ),
    ];
    for (query, code) in cases {
        let error = analyzer
            .analyze_with_procedures(&parse(query).unwrap(), &catalog, &access)
            .expect_err(query);
        assert_eq!(error.code(), code, "{query}");
    }

    let denied = analyzer
        .analyze_with_procedures(
            &parse("CALL dtg.graph.degree({}) YIELD degree RETURN degree").unwrap(),
            &catalog,
            &ProcedureAccess::denied(),
        )
        .expect_err("permission denial");
    assert_eq!(denied.code(), "DTG-CYPHER-PROCEDURE-PERMISSION");

    let write_catalog = ProcedureCatalog::from_definitions(vec![ProcedureDefinition::new(
        "dtg.test.unstagedWrite",
        Vec::new(),
        vec![ProcedureField::new("changed", ValueType::Boolean, false)],
        ProcedureEffect::Write,
        ProcedurePermission::GraphWrite,
        ProcedurePlacement::Coordinator,
    )])
    .unwrap();
    let error = analyzer
        .analyze_with_procedures(
            &parse("CALL dtg.test.unstagedWrite() YIELD changed RETURN changed").unwrap(),
            &write_catalog,
            &ProcedureAccess::allow_all(),
        )
        .expect_err("write procedures must fail until a typed staging path exists");
    assert_eq!(error.code(), "DTG-CYPHER-PROCEDURE-WRITE-UNSUPPORTED");
}

#[test]
fn rejects_read_procedure_mixed_with_writes_in_either_statement_order() {
    let catalog = ProcedureCatalog::builtin_analytics().unwrap();
    for query in [
        "CREATE (n:SeenBefore) CALL dtg.graph.degree({}) YIELD degree RETURN degree",
        "CALL dtg.graph.degree({}) YIELD degree CREATE (n:SeenAfter)",
    ] {
        let error = SemanticAnalyzer::new()
            .analyze_with_procedures(
                &parse(query).unwrap(),
                &catalog,
                &ProcedureAccess::allow_all(),
            )
            .expect_err("mixed write/procedure statements have no 1.0 staging pipeline");
        assert_eq!(
            error.code(),
            "DTG-CYPHER-WRITE-PROCEDURE-UNSUPPORTED",
            "{query}"
        );
    }

    for query in [
        "CREATE (n:NestedBefore) CALL () { CALL dtg.graph.degree({}) YIELD degree RETURN degree } RETURN degree",
        "CALL () { CALL dtg.graph.degree({}) YIELD degree RETURN degree } CREATE (n:NestedAfter) RETURN degree",
    ] {
        let error = SemanticAnalyzer::new()
            .analyze_with_procedures(
                &parse(query).unwrap(),
                &catalog,
                &ProcedureAccess::allow_all(),
            )
            .expect_err("nested procedures must participate in outer effect validation");
        assert_eq!(
            error.code(),
            "DTG-CYPHER-WRITE-PROCEDURE-UNSUPPORTED",
            "{query}"
        );
    }

    let nested = SemanticAnalyzer::new()
        .analyze_with_procedures(
            &parse(
                "CALL () { CALL dtg.graph.degree({}) YIELD degree RETURN degree } RETURN degree",
            )
            .unwrap(),
            &catalog,
            &ProcedureAccess::allow_all(),
        )
        .expect_err("nested named procedures are not executable before Task 4C");
    assert_eq!(nested.code(), "DTG-CYPHER-SUBQUERY-PROCEDURE-UNSUPPORTED");
}
