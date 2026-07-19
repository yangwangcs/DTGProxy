use cypher_ast::{Expression, RelationshipDirection};
use cypher_compiler::{CompileSession, CompiledMutation, CypherCompiler, PropertyTarget};

#[test]
fn compiler_preserves_structured_write_clauses() {
    let session = CompileSession::new("accounts", 7, 3, 11).unwrap();
    let compiled = CypherCompiler::new()
        .compile(
            "USE accounts AT VALID_TIME AS OF $valid CREATE (a:Person {name: $name})-[r:KNOWS {since: 2024}]->(b:Person) SET a.status = 'active' REMOVE b.legacy DETACH DELETE r RETURN a",
            &session,
        )
        .unwrap();

    assert!(!compiled.is_read_only());
    let writes = compiled.mutation_plan().mutations();
    assert_eq!(writes.len(), 4);

    let CompiledMutation::Create(pattern) = &writes[0] else {
        panic!("first write must retain the CREATE pattern");
    };
    let path = &pattern.paths()[0];
    assert_eq!(path.start().variable().unwrap().value(), "a");
    assert_eq!(path.start().labels()[0].value(), "Person");
    assert!(matches!(
        path.start().properties(),
        Some(Expression::Map(_))
    ));
    assert_eq!(path.chains()[0].relationship().types()[0].value(), "KNOWS");
    assert_eq!(
        path.chains()[0].relationship().direction(),
        RelationshipDirection::Outgoing
    );

    let CompiledMutation::SetProperty { target, value } = &writes[1] else {
        panic!("SET must be structured");
    };
    assert_eq!(target, &PropertyTarget::new("a", "status"));
    assert_eq!(value, &Expression::String("active".into()));

    assert_eq!(
        writes[2],
        CompiledMutation::RemoveProperty(PropertyTarget::new("b", "legacy"))
    );
    assert_eq!(
        writes[3],
        CompiledMutation::Delete {
            variables: vec!["r".into()],
            detach: true,
        }
    );
}

#[test]
fn malformed_write_clause_is_rejected_during_compilation() {
    let session = CompileSession::new("accounts", 7, 3, 11).unwrap();
    let error = CypherCompiler::new()
        .compile("CREATE (a) SET a = 1", &session)
        .unwrap_err();

    assert_eq!(error.code(), "DTG-CYPHER-INVALID-SET-TARGET");
}
