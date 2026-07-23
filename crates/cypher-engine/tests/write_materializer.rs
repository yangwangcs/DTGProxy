use std::collections::{BTreeMap, BTreeSet};

use cypher_compiler::{CompileSession, CompiledMutation, CypherCompiler};
use cypher_engine::{
    MaterializedElementKind, WriteContext, WriteSubqueryInput, WriteSubqueryRow, materialize_write,
    materialize_write_with_subquery_inputs, probe_merge_constraints, schema_id,
};
use query_executor::RuntimeValue;
use query_executor::VertexRecord;
use temporal_storage::{ElementId, ElementRef, GraphId, LabelId};
use temporal_types::{CanonicalElement, GraphValue, Interval, ValidTime};

#[test]
fn create_and_set_are_folded_into_one_deterministic_overlay_write() {
    let compiled = CypherCompiler::new()
        .compile(
            "USE accounts FOR VALID_TIME AS OF $valid CREATE (a:Person {name: $name}) SET a.status = 'active' RETURN a",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let parameters = BTreeMap::from([
        ("valid".into(), RuntimeValue::TimestampMicros(1_000)),
        ("name".into(), RuntimeValue::String("Ada".into())),
    ]);
    let context = WriteContext::new(
        7,
        3,
        128,
        [9; 32],
        Interval::forever_from(ValidTime::from_micros(1_000)),
        parameters,
    )
    .unwrap();

    let first = materialize_write(&compiled, &context).unwrap();
    let second = materialize_write(&compiled, &context).unwrap();
    assert_eq!(
        first, second,
        "retry must reproduce exactly the same write set"
    );
    assert_eq!(first.scoped_transactions().len(), 1);

    let a = first.binding("a").unwrap();
    assert_eq!(a.kind(), MaterializedElementKind::Vertex);
    assert_eq!(a.type_id(), schema_id("Person"));
    assert_eq!(
        a.payload().property(schema_id("name")),
        Some(&GraphValue::String("Ada".into()))
    );
    assert_eq!(
        a.payload().property(schema_id("status")),
        Some(&GraphValue::String("active".into()))
    );
}

#[test]
fn ordinary_write_subquery_materializes_imported_scalar_for_each_outer_row() {
    let compiled = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS value CALL (value) { CREATE (n:Item {id: value}) }",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .expect("ordinary write subquery");

    let materialize_row = |row_index, value| {
        materialize_write(
            &compiled,
            &WriteContext::new(
                7,
                3,
                128,
                [row_index; 32],
                Interval::forever_from(ValidTime::from_micros(1_000)),
                BTreeMap::new(),
            )
            .unwrap()
            .with_existing_bindings(BTreeMap::from([(
                "value".to_owned(),
                RuntimeValue::Integer(value),
            )]))
            .unwrap(),
        )
        .expect("child row materialization")
    };

    let first = materialize_row(1, 1);
    let second = materialize_row(2, 2);
    let property = schema_id("id");
    assert_eq!(
        first.overlay_elements()[0].payload().property(property),
        Some(&GraphValue::Integer(1))
    );
    assert_eq!(
        second.overlay_elements()[0].payload().property(property),
        Some(&GraphValue::Integer(2))
    );
    assert_ne!(
        first.overlay_elements()[0].element(),
        second.overlay_elements()[0].element()
    );
    assert!(first.binding("n").is_none(), "child locals must not leak");
}

#[test]
fn write_subquery_alias_export_feeds_the_following_parent_mutation() {
    let compiled = CypherCompiler::new()
        .compile(
            "CALL () { CREATE (n:Item {id: 1}) RETURN n AS created } \
             SET created.status = 'ready'",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .expect("write subquery alias export");
    let context = WriteContext::new(
        7,
        3,
        128,
        [13; 32],
        Interval::forever_from(ValidTime::from_micros(1_000)),
        BTreeMap::new(),
    )
    .unwrap();

    let writes = materialize_write(&compiled, &context)
        .expect("the parent mutation must consume the child alias export");
    assert_eq!(
        writes
            .binding("created")
            .unwrap()
            .payload()
            .property(schema_id("status")),
        Some(&GraphValue::String("ready".into()))
    );
}

#[test]
fn multi_row_write_subquery_exports_multiply_the_following_parent_mutation() {
    let compiled = CypherCompiler::new()
        .compile(
            "CALL () { \
               UNWIND [1, 2] AS value \
               CREATE (n:Item {id: value}) \
               RETURN n AS created \
             } \
             SET created.status = 'ready'",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .expect("multi-row write subquery export");
    let clause_start = match &compiled.mutation_plan().mutations()[0] {
        CompiledMutation::Subquery(subquery) => subquery.clause_start(),
        mutation => panic!("expected write subquery, got {mutation:?}"),
    };
    let inputs = [WriteSubqueryInput::new(
        clause_start,
        vec![
            WriteSubqueryRow::new(
                BTreeMap::from([("value".to_owned(), RuntimeValue::Integer(1))]),
                Vec::new(),
            ),
            WriteSubqueryRow::new(
                BTreeMap::from([("value".to_owned(), RuntimeValue::Integer(2))]),
                Vec::new(),
            ),
        ],
    )];
    let context = WriteContext::new(
        7,
        3,
        128,
        [14; 32],
        Interval::forever_from(ValidTime::from_micros(1_000)),
        BTreeMap::new(),
    )
    .unwrap();

    let writes = materialize_write_with_subquery_inputs(&compiled, &context, &inputs)
        .expect("each child output row must feed the following SET");
    assert_eq!(writes.overlay_elements().len(), 2);
    assert_eq!(writes.scoped_transactions().len(), 2);
    let mut identifiers = writes
        .overlay_elements()
        .iter()
        .map(|element| {
            assert_eq!(
                element.payload().property(schema_id("status")),
                Some(&GraphValue::String("ready".into()))
            );
            match element.payload().property(schema_id("id")) {
                Some(GraphValue::Integer(value)) => *value,
                value => panic!("expected integer child id, got {value:?}"),
            }
        })
        .collect::<Vec<_>>();
    identifiers.sort();
    assert_eq!(identifiers, vec![1, 2]);
}

#[test]
fn created_cross_partition_edge_keeps_endpoint_identity_in_overlay() {
    let compiled = CypherCompiler::new()
        .compile(
            "CREATE (a:Person)-[r:KNOWS {weight: 2}]->(b:Person)",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let context = WriteContext::new(
        7,
        3,
        65_536,
        [3; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::new(),
    )
    .unwrap();

    let writes = materialize_write(&compiled, &context).unwrap();
    let a = writes.binding("a").unwrap();
    let b = writes.binding("b").unwrap();
    let r = writes.binding("r").unwrap();
    assert_eq!(r.kind(), MaterializedElementKind::Relationship);
    assert_eq!(r.element().partition(), a.element().partition());
    assert_eq!(r.source(), Some(a.element()));
    assert_eq!(r.destination(), Some(b.element()));
    let RuntimeValue::Relationship(runtime) = r.runtime_value() else {
        panic!("relationship materialization must produce a relationship value");
    };
    assert_eq!(runtime.source_ref(), a.element());
    assert_eq!(runtime.destination_ref(), b.element());
    assert!(writes.scoped_transactions().len() >= 2);
}

#[test]
fn merge_creates_a_deterministic_element_when_constraint_lookup_is_empty() {
    let compiled = CypherCompiler::new()
        .compile(
            "MERGE (a:Person {email: $email})",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let context = WriteContext::new(
        7,
        3,
        16,
        [1; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::from([("email".into(), RuntimeValue::String("a@b.test".into()))]),
    )
    .unwrap();

    let writes = materialize_write(&compiled, &context).expect("merge create");
    let retry_context = WriteContext::new(
        7,
        3,
        16,
        [8; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::from([("email".into(), RuntimeValue::String("a@b.test".into()))]),
    )
    .unwrap();
    let retry = materialize_write(&compiled, &retry_context).expect("merge retry");
    assert!(writes.binding("a").is_some());
    assert_eq!(
        writes.binding("a").unwrap().element(),
        retry.binding("a").unwrap().element()
    );
    assert_eq!(writes.merge_constraints().len(), 1);
    assert_eq!(retry.merge_constraints(), writes.merge_constraints());
    assert_eq!(
        writes.merge_constraints()[0].owner(),
        writes.binding("a").unwrap().element()
    );
}

#[test]
fn merge_reuses_an_existing_binding_without_emitting_a_duplicate_mutation() {
    let compiled = CypherCompiler::new()
        .compile(
            "MERGE (a:Person {email: $email})",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let element = ElementRef::vertex(
        GraphId::new(7),
        temporal_storage::PartitionId::new(2),
        ElementId::new(42),
    );
    let context = WriteContext::new(
        7,
        3,
        16,
        [1; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::from([("email".into(), RuntimeValue::String("a@b.test".into()))]),
    )
    .unwrap()
    .with_existing_bindings(BTreeMap::from([(
        "a".into(),
        RuntimeValue::Node(VertexRecord::new(
            element,
            Some(LabelId::new(schema_id("Person"))),
            CanonicalElement::new(3, BTreeMap::new()),
        )),
    )]))
    .unwrap();
    let writes = materialize_write(&compiled, &context).expect("merge reuse");
    assert!(writes.scoped_transactions().is_empty());
    assert_eq!(writes.binding("a").unwrap().element(), element);
    assert!(writes.merge_constraints().is_empty());
}

#[test]
fn full_path_merge_probe_restores_exact_claim_bindings_as_a_resolved_no_op() {
    let compiled = CypherCompiler::new()
        .compile(
            "MERGE (a:Person {name: 'Ada'})-[r:KNOWS {since: 2024}]->(b:Person {name: 'Bob'})",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let probe_context = WriteContext::new(
        7,
        3,
        128,
        [5; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::new(),
    )
    .unwrap();

    let probe = probe_merge_constraints(&compiled, &probe_context).expect("merge probe");
    assert_eq!(probe.merge_constraints().len(), 1);
    let constraint = &probe.merge_constraints()[0];
    assert_eq!(
        constraint.binding_names(),
        &BTreeSet::from(["a".to_owned(), "b".to_owned(), "r".to_owned()])
    );
    let restored = constraint
        .binding_names()
        .iter()
        .map(|name| {
            (
                name.clone(),
                probe
                    .binding(name)
                    .expect("claim binding exists")
                    .runtime_value(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let retry_context = WriteContext::new(
        7,
        3,
        128,
        [9; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::new(),
    )
    .unwrap()
    .with_existing_bindings(restored.clone())
    .unwrap()
    .with_resolved_merge_keys(BTreeSet::from([constraint.key()]));

    let retry = materialize_write(&compiled, &retry_context).expect("resolved merge retry");
    assert_eq!(
        retry.bindings().keys().cloned().collect::<BTreeSet<_>>(),
        constraint.binding_names().clone()
    );
    for (name, value) in restored {
        assert_eq!(retry.binding(&name).unwrap().runtime_value(), value);
    }
    assert!(retry.scoped_transactions().is_empty());
    assert!(retry.merge_constraints().is_empty());
}

#[test]
fn relationship_merge_with_bound_endpoints_claims_only_the_edge_and_hashes_endpoint_identity() {
    let compiled = CypherCompiler::new()
        .compile(
            "MERGE (a)-[r:KNOWS {since: 2024}]->(b)",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let endpoint = |partition, id| {
        RuntimeValue::Node(VertexRecord::new(
            ElementRef::vertex(
                GraphId::new(7),
                temporal_storage::PartitionId::new(partition),
                ElementId::new(id),
            ),
            Some(LabelId::new(schema_id("Person"))),
            CanonicalElement::new(3, BTreeMap::new()),
        ))
    };
    let context = |destination_id| {
        WriteContext::new(
            7,
            3,
            128,
            [7; 32],
            Interval::forever_from(ValidTime::from_micros(5)),
            BTreeMap::new(),
        )
        .unwrap()
        .with_existing_bindings(BTreeMap::from([
            ("a".to_owned(), endpoint(2, 41)),
            ("b".to_owned(), endpoint(3, destination_id)),
        ]))
        .unwrap()
    };

    let first = probe_merge_constraints(&compiled, &context(42)).expect("first edge probe");
    let changed = probe_merge_constraints(&compiled, &context(43)).expect("changed edge probe");

    assert_eq!(first.merge_constraints().len(), 1);
    assert_eq!(
        first.merge_constraints()[0].binding_names(),
        &BTreeSet::from(["r".to_owned()])
    );
    assert!(first.binding("r").is_some());
    assert!(!first.scoped_transactions().is_empty());
    assert_ne!(
        first.merge_constraints()[0].key(),
        changed.merge_constraints()[0].key(),
        "the canonical endpoint identity must participate in relationship MERGE hashing"
    );
}

#[test]
fn merge_with_an_empty_upstream_match_produces_no_bindings_or_claims() {
    let compiled = CypherCompiler::new()
        .compile(
            "MATCH (a:Missing) MERGE (a)-[r:NEVER]->(b:NeverCreated)",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let context = WriteContext::new(
        7,
        3,
        128,
        [6; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::new(),
    )
    .unwrap()
    .without_input_row();

    let writes = materialize_write(&compiled, &context).expect("empty upstream write");
    assert!(writes.bindings().is_empty());
    assert!(writes.scoped_transactions().is_empty());
    assert!(writes.merge_constraints().is_empty());
}

#[test]
fn anonymous_merge_pattern_recovers_through_deterministic_probe_names() {
    let compiled = CypherCompiler::new()
        .compile(
            "MERGE (:AnonymousLeft)-[:ANONYMOUS_LINK]->(:AnonymousRight)",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let context = WriteContext::new(
        7,
        3,
        128,
        [10; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::new(),
    )
    .unwrap();
    let probe = probe_merge_constraints(&compiled, &context).expect("anonymous merge probe");
    let constraint = &probe.merge_constraints()[0];
    assert_eq!(
        constraint.binding_names(),
        &BTreeSet::from([
            "__relationship_2".to_owned(),
            "__vertex_0".to_owned(),
            "__vertex_1".to_owned(),
        ])
    );
    let restored = constraint
        .binding_names()
        .iter()
        .map(|name| (name.clone(), probe.binding(name).unwrap().runtime_value()))
        .collect();
    let retry_context = context
        .with_existing_bindings(restored)
        .unwrap()
        .with_resolved_merge_keys(BTreeSet::from([constraint.key()]));

    let retry = materialize_write(&compiled, &retry_context).expect("anonymous merge recovery");
    assert_eq!(
        retry.bindings().keys().collect::<BTreeSet<_>>(),
        constraint.binding_names().iter().collect()
    );
    assert!(retry.scoped_transactions().is_empty());
    assert!(retry.merge_constraints().is_empty());
}

#[test]
fn identical_merge_clauses_keep_claim_local_aliases_and_recover_independently() {
    let compiled = CypherCompiler::new()
        .compile(
            "MERGE (a:Person {id: 1}) MERGE (b:Person {id: 1})",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let context = WriteContext::new(
        7,
        3,
        128,
        [11; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::new(),
    )
    .unwrap();

    let probe = probe_merge_constraints(&compiled, &context).expect("multi-clause probe");
    assert_eq!(probe.merge_constraints().len(), 2);
    let constraints = probe.merge_constraints();
    assert_eq!(
        constraints
            .iter()
            .map(|constraint| constraint.binding_names().clone())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            BTreeSet::from(["a".to_owned()]),
            BTreeSet::from(["b".to_owned()]),
        ])
    );
    assert_eq!(
        probe.binding("a").unwrap().element(),
        probe.binding("b").unwrap().element()
    );
    assert!(
        constraints
            .iter()
            .all(|constraint| constraint.owner() == probe.binding("a").unwrap().element())
    );
    let restored = constraints
        .iter()
        .flat_map(|constraint| constraint.binding_names())
        .map(|name| (name.clone(), probe.binding(name).unwrap().runtime_value()))
        .collect();
    let retry_context = context
        .with_existing_bindings(restored)
        .unwrap()
        .with_resolved_merge_keys(BTreeSet::from([constraints[0].key()]));

    let retry = materialize_write(&compiled, &retry_context).expect("multi-clause recovery");
    assert_eq!(
        retry.binding("a").unwrap().element(),
        retry.binding("b").unwrap().element()
    );
    assert!(retry.scoped_transactions().is_empty());
    assert!(retry.merge_constraints().is_empty());
}

#[test]
fn anonymous_first_merge_does_not_suppress_a_later_named_merge() {
    let compiled = CypherCompiler::new()
        .compile(
            "MERGE (:A {id: 1}) MERGE (b:B {id: 2})",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let context = WriteContext::new(
        7,
        3,
        128,
        [12; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::new(),
    )
    .unwrap();

    let probe = probe_merge_constraints(&compiled, &context).expect("multi-merge probe");
    assert_eq!(probe.merge_constraints().len(), 2);
    assert!(probe.binding("__vertex_0").is_some());
    assert!(probe.binding("b").is_some());
    assert!(
        probe
            .merge_constraints()
            .iter()
            .any(|constraint| constraint.binding_names() == &BTreeSet::from(["b".to_owned()]))
    );
}

#[test]
fn existing_bindings_are_updated_and_deleted_as_temporal_mutations() {
    let compiled = CypherCompiler::new()
        .compile(
            "MATCH (a:Person) SET a.status = 'active' DELETE a",
            &CompileSession::new("accounts", 7, 3, 11).unwrap(),
        )
        .unwrap();
    let element = ElementRef::vertex(
        GraphId::new(7),
        temporal_storage::PartitionId::new(2),
        ElementId::new(42),
    );
    let existing = RuntimeValue::Node(VertexRecord::new(
        element,
        Some(LabelId::new(schema_id("Person"))),
        CanonicalElement::new(
            3,
            BTreeMap::from([(schema_id("name"), GraphValue::String("Ada".into()))]),
        ),
    ));
    let context = WriteContext::new(
        7,
        3,
        16,
        [4; 32],
        Interval::forever_from(ValidTime::from_micros(5)),
        BTreeMap::new(),
    )
    .unwrap()
    .with_existing_bindings(BTreeMap::from([(String::from("a"), existing)]))
    .unwrap();

    let writes = materialize_write(&compiled, &context).unwrap();
    let a = writes.binding("a").unwrap();
    assert!(a.deleted());
    assert_eq!(writes.scoped_transactions().len(), 1);
}
