use analytics_api::AlgorithmValue;
use cypher_ast::CypherProfile;
use procedure_runtime::{
    ProcedureArgumentStyle, ProcedureCatalog, ProcedureDefinition, ProcedureEffect, ProcedureError,
    ProcedureField, ProcedureLimits, ProcedurePermission, ProcedurePlacement,
};
use temporal_ir::ValueType;

#[test]
fn built_in_catalog_owns_revision_identity_schema_and_policy() {
    let catalog = ProcedureCatalog::builtin_analytics().expect("valid built-in catalog");
    let degree = catalog
        .resolve("DTG.GRAPH.DEGREE")
        .expect("case-insensitive current procedure lookup");

    assert_ne!(catalog.revision(), 0);
    assert_eq!(degree.identity().catalog_revision(), catalog.revision());
    assert_ne!(degree.identity().procedure_revision(), 0);
    assert_ne!(degree.identity().authority_key(), [0; 32]);
    assert_eq!(
        degree.argument_style(),
        ProcedureArgumentStyle::MapOrPositional
    );
    assert!(degree.inputs().is_empty());
    assert_eq!(
        degree
            .output()
            .columns()
            .iter()
            .map(|column| (column.name(), column.value_type(), column.nullable()))
            .collect::<Vec<_>>(),
        vec![
            ("vertexId", &ValueType::String, false),
            ("inDegree", &ValueType::Integer, false),
            ("outDegree", &ValueType::Integer, false),
            ("degree", &ValueType::Integer, false),
        ]
    );
    assert_eq!(degree.effect(), ProcedureEffect::ReadOnly);
    assert_eq!(degree.permission(), ProcedurePermission::AnalyticsRead);
    assert_eq!(degree.placement(), ProcedurePlacement::Coordinator);
    assert!(degree.deterministic());
    assert!(degree.supports_profile(CypherProfile::cypher_25()));
    assert!(degree.supports_overlay());
    assert!(degree.limits().max_invocations() > 0);
    assert!(degree.limits().max_output_rows() > 0);
    assert!(degree.limits().max_result_bytes() > 0);
    assert!(degree.limits().max_graph_vertices() > 0);
    assert!(degree.limits().max_graph_edges() > 0);
    assert!(degree.limits().max_graph_bytes() > 0);
}

#[test]
fn catalog_rejects_unknown_name_and_revision_identity_mismatch() {
    let catalog = ProcedureCatalog::builtin_analytics().expect("valid built-in catalog");
    assert!(catalog.resolve("dtg.graph.missing").is_none());

    let degree = catalog.resolve("dtg.graph.degree").unwrap();
    assert!(catalog.validate_identity(degree.identity()).is_ok());
    assert!(
        catalog
            .validate_identity(
                &degree
                    .identity()
                    .with_catalog_revision(catalog.revision() + 1)
            )
            .is_err()
    );
}

fn catalog_with_default(value: i64) -> ProcedureCatalog {
    ProcedureCatalog::from_definitions(vec![ProcedureDefinition::new(
        "dtg.test.defaulted",
        vec![
            ProcedureField::new("value", ValueType::Integer, false)
                .with_default(AlgorithmValue::Integer(value)),
        ],
        vec![ProcedureField::new("result", ValueType::Integer, false)],
        ProcedureEffect::ReadOnly,
        ProcedurePermission::AnalyticsRead,
        ProcedurePlacement::Coordinator,
    )])
    .unwrap()
}

#[test]
fn default_value_changes_catalog_and_procedure_revisions() {
    let first = catalog_with_default(1);
    let second = catalog_with_default(2);

    assert_ne!(first.revision(), second.revision());
    assert_ne!(
        first
            .resolve("dtg.test.defaulted")
            .unwrap()
            .identity()
            .procedure_revision(),
        second
            .resolve("dtg.test.defaulted")
            .unwrap()
            .identity()
            .procedure_revision()
    );
}

#[test]
fn graph_projection_limits_are_catalog_authority() {
    let definition = |vertices| {
        ProcedureDefinition::new(
            "dtg.test.boundedGraph",
            Vec::new(),
            vec![ProcedureField::new("result", ValueType::Integer, false)],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        )
        .with_limits(
            ProcedureLimits::new(10, 10, 10, 1024, 4096)
                .unwrap()
                .with_graph_limits(vertices, 20, 8192)
                .unwrap(),
        )
    };
    let first = ProcedureCatalog::from_definitions(vec![definition(10)]).unwrap();
    let second = ProcedureCatalog::from_definitions(vec![definition(11)]).unwrap();

    assert_ne!(first.revision(), second.revision());
    assert_eq!(
        first
            .resolve("dtg.test.boundedGraph")
            .unwrap()
            .limits()
            .max_graph_vertices(),
        10
    );
}

fn definition_with_fields(
    inputs: Vec<ProcedureField>,
    outputs: Vec<ProcedureField>,
) -> ProcedureDefinition {
    ProcedureDefinition::new(
        "dtg.test.definition",
        inputs,
        outputs,
        ProcedureEffect::ReadOnly,
        ProcedurePermission::AnalyticsRead,
        ProcedurePlacement::Coordinator,
    )
}

fn integer_field(name: &str) -> ProcedureField {
    ProcedureField::new(name, ValueType::Integer, false)
}

#[test]
fn catalog_rejects_empty_input_field_name() {
    assert_eq!(
        ProcedureCatalog::from_definitions(vec![definition_with_fields(
            vec![integer_field("")],
            vec![integer_field("result")],
        )]),
        Err(ProcedureError::InvalidCatalog)
    );
}

#[test]
fn catalog_rejects_duplicate_input_field_names() {
    assert_eq!(
        ProcedureCatalog::from_definitions(vec![definition_with_fields(
            vec![integer_field("value"), integer_field("value")],
            vec![integer_field("result")],
        )]),
        Err(ProcedureError::InvalidCatalog)
    );
}

#[test]
fn catalog_rejects_empty_output_field_name() {
    assert_eq!(
        ProcedureCatalog::from_definitions(vec![definition_with_fields(
            Vec::new(),
            vec![integer_field("")],
        )]),
        Err(ProcedureError::InvalidCatalog)
    );
}

#[test]
fn catalog_rejects_duplicate_output_field_names() {
    assert_eq!(
        ProcedureCatalog::from_definitions(vec![definition_with_fields(
            Vec::new(),
            vec![integer_field("result"), integer_field("result")],
        )]),
        Err(ProcedureError::InvalidCatalog)
    );
}

#[test]
fn catalog_rejects_default_incompatible_with_field_type() {
    assert_eq!(
        ProcedureCatalog::from_definitions(vec![definition_with_fields(
            vec![integer_field("value").with_default(AlgorithmValue::String("bad".into()))],
            vec![integer_field("result")],
        )]),
        Err(ProcedureError::InvalidCatalog)
    );
}

#[test]
fn catalog_rejects_null_default_for_non_nullable_field() {
    assert_eq!(
        ProcedureCatalog::from_definitions(vec![definition_with_fields(
            vec![integer_field("value").with_default(AlgorithmValue::Null)],
            vec![integer_field("result")],
        )]),
        Err(ProcedureError::InvalidCatalog)
    );
}
