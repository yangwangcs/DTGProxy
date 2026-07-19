use std::collections::BTreeMap;

use cypher_compiler::{CompileSession, CypherCompiler};
use cypher_engine::{MaterializedElementKind, WriteContext, materialize_write, schema_id};
use query_executor::v2::RuntimeValue;
use temporal_types::{GraphValue, Interval, ValidTime};

#[test]
fn create_and_set_are_folded_into_one_deterministic_overlay_write() {
    let compiled = CypherCompiler::new()
        .compile(
            "USE accounts AT VALID_TIME AS OF $valid CREATE (a:Person {name: $name}) SET a.status = 'active' RETURN a",
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
    assert!(writes.scoped_transactions().len() >= 2);
}

#[test]
fn unsupported_merge_is_rejected_instead_of_degrading_to_racy_create() {
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

    let error = materialize_write(&compiled, &context).unwrap_err();
    assert_eq!(error.code(), "DTG-CYPHER-MERGE-CONSTRAINT-REQUIRED");
}
