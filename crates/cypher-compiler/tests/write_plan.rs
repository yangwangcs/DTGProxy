use cypher_ast::{Expression, RelationshipDirection};
use cypher_compiler::{CompileSession, CompiledMutation, CypherCompiler, PropertyTarget};
use temporal_ir::LogicalOperator;

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

#[test]
fn ordinary_write_subquery_keeps_nested_mutations_and_parent_read_prefix() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS value CALL (value) { CREATE (n:Item {id: value}) }",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .expect("ordinary write subquery must compile");

    let [CompiledMutation::Subquery(subquery)] = compiled.mutation_plan().mutations() else {
        panic!("write child must remain an ordered nested mutation program");
    };
    assert_eq!(subquery.imports(), &["value"]);
    assert!(subquery.exports().is_empty());
    assert!(matches!(
        subquery.mutation_plan().mutations(),
        [CompiledMutation::Create(_)]
    ));

    let apply = compiled
        .logical_plan()
        .nodes()
        .iter()
        .find_map(|node| match node.operator() {
            LogicalOperator::Apply { apply } => Some(apply),
            _ => None,
        })
        .expect("structured Apply");
    assert!(
        apply
            .child_plan()
            .nodes()
            .iter()
            .any(|node| { matches!(node.operator(), LogicalOperator::Create) })
    );

    let prefix = compiled
        .read_prefix_plan()
        .expect("parent input is the write subquery read prefix");
    assert!(
        prefix
            .nodes()
            .iter()
            .any(|node| matches!(node.operator(), LogicalOperator::Unwind { .. }))
    );
    assert!(prefix.nodes().iter().all(|node| {
        !matches!(
            node.operator(),
            LogicalOperator::Apply { .. }
                | LogicalOperator::Create
                | LogicalOperator::Merge
                | LogicalOperator::Set
                | LogicalOperator::Remove
                | LogicalOperator::Delete { .. }
        )
    }));
}

#[test]
fn child_local_write_prefix_is_isolated_from_its_structured_mutation_program() {
    let compiled = CypherCompiler::new()
        .compile(
            "CALL () { UNWIND [1, 2] AS value WITH value CREATE (n:Item {id: value}) }",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .expect("write subquery with a local row-producing prefix");

    let [CompiledMutation::Subquery(subquery)] = compiled.mutation_plan().mutations() else {
        panic!("write child must remain an ordered nested mutation program");
    };
    let prefix = subquery
        .read_prefix_plan()
        .expect("the child must retain its own isolated read prefix");
    assert!(prefix.nodes().iter().any(|node| {
        matches!(
            node.operator(),
            LogicalOperator::Unwind { .. } | LogicalOperator::Project { .. }
        )
    }));
    assert!(prefix.nodes().iter().all(|node| {
        !matches!(
            node.operator(),
            LogicalOperator::Create
                | LogicalOperator::Merge
                | LogicalOperator::Set
                | LogicalOperator::Remove
                | LogicalOperator::Delete { .. }
                | LogicalOperator::Apply { .. }
        )
    }));
    assert!(matches!(
        subquery.mutation_plan().mutations(),
        [CompiledMutation::Create(_)]
    ));
}
