use cypher_compiler::{CompileSession, CypherCompiler};
use physical_plan::{
    AccessGuarantee, PhysicalAccess, PhysicalComparisonOperator, PhysicalOperator,
    PhysicalPropertyConstraint, PrimitiveKind, ResidualPolicy,
};
use query_optimizer::{
    CapabilitySnapshot, DeploymentMode, Optimizer, OptimizerContext, OptimizerError,
};
use temporal_ir::{
    ChangeAxis, Column, LanguageProfile, LogicalOperator, LogicalPlan, LogicalPlanBuilder,
    PlanHeader, RowSchema, ScalarExpr, SlotId, TransactionTimeSpec, ValueType,
};
use temporal_types::GraphValue;

#[test]
fn physical_plan_preserves_slots_and_expressions_from_logical_plan() {
    let logical = CypherCompiler::new()
        .compile(
            "MATCH (a:Account)-[r:TRANSFERRED]->(b:Account) \
             WHERE a.active = true RETURN a, b",
            &CompileSession::new("accounts", 7, 3, 11).expect("session"),
        )
        .expect("compile")
        .logical_plan()
        .clone();
    let optimized = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 64 << 20, 256 << 20)
                .expect("context"),
        )
        .expect("optimize");
    let physical = optimized.plan().fragments()[0].operators();

    assert_eq!(logical.nodes().len(), physical.len());
    for (logical, physical) in logical.nodes().iter().zip(physical) {
        match (logical.operator(), physical) {
            (
                LogicalOperator::NodeScan { binding, labels },
                PhysicalOperator::NodeScan {
                    binding: actual_binding,
                    labels: actual_labels,
                    output,
                },
            ) => {
                assert_eq!(actual_binding, binding);
                assert_eq!(actual_labels, labels);
                assert_eq!(output, logical.output());
            }
            (
                LogicalOperator::Expand {
                    source,
                    relationship,
                    destination,
                    outgoing,
                    types,
                },
                PhysicalOperator::Expand {
                    source: actual_source,
                    relationship: actual_relationship,
                    destination: actual_destination,
                    outgoing: actual_outgoing,
                    types: actual_types,
                    output,
                },
            ) => {
                assert_eq!(actual_source, source);
                assert_eq!(actual_relationship, relationship);
                assert_eq!(actual_destination, destination);
                assert_eq!(actual_outgoing, outgoing);
                assert_eq!(actual_types, types);
                assert_eq!(output, logical.output());
            }
            (
                LogicalOperator::Project { expressions },
                PhysicalOperator::Project {
                    expressions: actual_expressions,
                    output,
                },
            ) => {
                assert_eq!(actual_expressions, expressions);
                assert_eq!(output, logical.output());
            }
            (LogicalOperator::Filter { predicate }, PhysicalOperator::Filter(actual)) => {
                assert_eq!(actual, predicate);
            }
            (LogicalOperator::Argument, PhysicalOperator::Argument { .. }) => {}
            (
                LogicalOperator::TemporalSlice {
                    valid_time,
                    transaction_time,
                },
                PhysicalOperator::TemporalSlice {
                    valid_time: actual_valid,
                    transaction_time: actual_transaction,
                },
            ) => {
                assert_eq!(actual_valid, valid_time);
                assert_eq!(actual_transaction, transaction_time);
            }
            (logical, physical) => panic!("operator lost information: {logical:?} -> {physical:?}"),
        }
    }
}

#[test]
fn batch_subtransaction_remains_a_distinct_physical_boundary() {
    let logical = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2, 3] AS value \
             CALL (value) { CREATE (:Item {id: value}) } \
             IN TRANSACTIONS OF 2 ROWS",
            &CompileSession::new("accounts", 7, 3, 11).expect("session"),
        )
        .expect("compile batch subtransaction")
        .logical_plan()
        .clone();
    let optimized = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 2, 64 << 20, 256 << 20)
                .expect("context"),
        )
        .expect("optimize batch subtransaction");

    optimized.plan().validate().expect("physical batch plan");
    let batches = optimized
        .plan()
        .fragments()
        .iter()
        .flat_map(|fragment| fragment.operators())
        .filter_map(|operator| match operator {
            PhysicalOperator::BatchSubtransaction { batch_rows, .. } => Some(*batch_rows),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(batches, vec![2]);
}

#[test]
fn physical_plan_preserves_change_scan_scope() {
    let logical = CypherCompiler::new()
        .compile(
            "USE accounts CHANGES FOR VALID_TIME BETWEEN $from AND $to \
             FOR SYSTEM_TIME AS OF $snapshot MATCH (n) RETURN n",
            &CompileSession::new("accounts", 7, 3, 11).expect("session"),
        )
        .expect("compile changes")
        .logical_plan()
        .clone();
    let optimized = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 64 << 20, 256 << 20)
                .expect("context"),
        )
        .expect("optimize changes");

    let change = optimized
        .plan()
        .fragments()
        .iter()
        .flat_map(|fragment| fragment.operators())
        .find_map(|operator| match operator {
            PhysicalOperator::ChangeScan {
                axis,
                start,
                end,
                system_snapshot,
            } => Some((*axis, start.clone(), end.clone(), system_snapshot.clone())),
            _ => None,
        })
        .expect("physical plan must retain the event scan");
    assert_eq!(change.0, ChangeAxis::ValidTime);
    assert_eq!(change.1, ScalarExpr::Parameter("from".into()));
    assert_eq!(change.2, ScalarExpr::Parameter("to".into()));
    assert_eq!(
        change.3,
        TransactionTimeSpec::AsOf(ScalarExpr::Parameter("snapshot".into()))
    );
}

fn optimize_match(query: &str, capabilities: CapabilitySnapshot) -> query_optimizer::OptimizedPlan {
    let logical = CypherCompiler::new()
        .compile(
            query,
            &CompileSession::new("accounts", 7, 3, 11).expect("session"),
        )
        .expect("compile")
        .logical_plan()
        .clone();
    Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 64 << 20, 256 << 20)
                .expect("context")
                .with_current_projection_candidate_scan(true)
                .with_capability_snapshot(capabilities),
        )
        .expect("optimize")
}

fn primitive(primitive: PrimitiveKind, guarantee: AccessGuarantee) -> PhysicalAccess {
    PhysicalAccess::Primitive {
        primitive,
        guarantee,
        residual: ResidualPolicy::Evaluate,
        constraints: Vec::new(),
    }
}

fn optimize_predicate(predicate: ScalarExpr) -> query_optimizer::OptimizedPlan {
    let header = PlanHeader::new(
        7,
        3,
        11,
        LanguageProfile::Cypher25,
        "Cypher 25 / 2026.07",
        [23; 32],
    )
    .expect("header");
    let schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "n", ValueType::Node, false),
        Column::new(SlotId::new(1), "other", ValueType::Node, false),
    ])
    .expect("schema");
    let mut builder = LogicalPlanBuilder::new(header);
    let scan = builder
        .add(
            LogicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
            },
            Vec::new(),
            schema.clone(),
        )
        .expect("scan");
    let filter = builder
        .add(
            LogicalOperator::Filter {
                predicate: predicate.clone(),
            },
            vec![scan],
            schema,
        )
        .expect("filter");
    let logical: LogicalPlan = builder.finish(filter).expect("plan");
    Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 64 << 20, 256 << 20)
                .expect("context")
                .with_current_projection_candidate_scan(true)
                .with_capability_snapshot(
                    CapabilitySnapshot::new(
                        9,
                        AccessGuarantee::Candidate,
                        AccessGuarantee::Unsupported,
                        AccessGuarantee::Unsupported,
                    )
                    .expect("capabilities"),
                ),
        )
        .expect("optimize")
}

fn property(slot: u32, property_id: u32) -> ScalarExpr {
    ScalarExpr::Property {
        value: Box::new(ScalarExpr::Slot(SlotId::new(slot))),
        property_id,
    }
}

fn candidate_constraints(plan: &query_optimizer::OptimizedPlan) -> &[PhysicalPropertyConstraint] {
    plan.plan()
        .fragments()
        .iter()
        .flat_map(|fragment| fragment.access())
        .find_map(|access| match access {
            PhysicalAccess::Primitive {
                primitive: PrimitiveKind::CandidateScan,
                constraints,
                ..
            } => Some(constraints.as_slice()),
            _ => None,
        })
        .expect("candidate scan access")
}

#[test]
fn candidate_scan_extracts_literal_property_comparisons_and_keeps_filter() {
    let predicate = ScalarExpr::And(
        Box::new(ScalarExpr::Equal(
            Box::new(property(0, 41)),
            Box::new(ScalarExpr::Literal(GraphValue::Boolean(true))),
        )),
        Box::new(ScalarExpr::Less(
            Box::new(ScalarExpr::Literal(GraphValue::Integer(18))),
            Box::new(property(0, 42)),
        )),
    );
    let optimized = optimize_predicate(predicate.clone());

    assert_eq!(
        candidate_constraints(&optimized),
        &[
            PhysicalPropertyConstraint::new(
                41,
                PhysicalComparisonOperator::Equal,
                GraphValue::Boolean(true),
            ),
            PhysicalPropertyConstraint::new(
                42,
                PhysicalComparisonOperator::GreaterThan,
                GraphValue::Integer(18),
            ),
        ]
    );
    assert!(
        optimized
            .plan()
            .fragments()
            .iter()
            .flat_map(|fragment| fragment.operators())
            .any(|operator| operator == &PhysicalOperator::Filter(predicate.clone()))
    );
}

#[test]
fn candidate_scan_does_not_extract_or_parameter_or_cross_slot_predicates() {
    let rejected = [
        ScalarExpr::Or(
            Box::new(ScalarExpr::Equal(
                Box::new(property(0, 41)),
                Box::new(ScalarExpr::Literal(GraphValue::Boolean(true))),
            )),
            Box::new(ScalarExpr::Equal(
                Box::new(property(0, 42)),
                Box::new(ScalarExpr::Literal(GraphValue::Integer(7))),
            )),
        ),
        ScalarExpr::Greater(
            Box::new(property(0, 41)),
            Box::new(ScalarExpr::Parameter("minimum".into())),
        ),
        ScalarExpr::Equal(
            Box::new(property(1, 41)),
            Box::new(ScalarExpr::Literal(GraphValue::Integer(7))),
        ),
    ];

    for predicate in rejected {
        let optimized = optimize_predicate(predicate.clone());
        assert!(candidate_constraints(&optimized).is_empty());
        assert!(
            optimized
                .plan()
                .fragments()
                .iter()
                .flat_map(|fragment| fragment.operators())
                .any(|operator| operator == &PhysicalOperator::Filter(predicate.clone()))
        );
    }
}

#[test]
fn default_context_keeps_graph_access_generic() {
    let logical = CypherCompiler::new()
        .compile(
            "MATCH (n) RETURN n",
            &CompileSession::new("accounts", 7, 3, 11).expect("session"),
        )
        .expect("compile")
        .logical_plan()
        .clone();
    let optimized = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 64 << 20, 256 << 20)
                .expect("context"),
        )
        .expect("optimize");

    assert_eq!(optimized.plan().header().capability_generation(), 1);
    assert!(
        optimized
            .plan()
            .fragments()
            .iter()
            .flat_map(|fragment| fragment.access())
            .all(|access| *access == PhysicalAccess::Generic)
    );
}

#[test]
fn capability_generation_must_be_non_zero() {
    assert_eq!(
        CapabilitySnapshot::new(
            0,
            AccessGuarantee::Unsupported,
            AccessGuarantee::Unsupported,
            AccessGuarantee::Unsupported,
        ),
        Err(OptimizerError::InvalidCapabilityGeneration)
    );
}

#[test]
fn candidate_and_exact_capabilities_select_primitives_with_residuals() {
    let capabilities = CapabilitySnapshot::new(
        17,
        AccessGuarantee::Candidate,
        AccessGuarantee::Exact,
        AccessGuarantee::Candidate,
    )
    .expect("capabilities");
    let graph = optimize_match("MATCH (a)-[r]->(b) RETURN b", capabilities);
    let changes = optimize_match(
        "USE accounts CHANGES FOR VALID_TIME BETWEEN $from AND $to MATCH (n) RETURN n",
        capabilities,
    );
    let graph_accesses = graph
        .plan()
        .fragments()
        .iter()
        .flat_map(|fragment| fragment.access());
    let change_accesses = changes
        .plan()
        .fragments()
        .iter()
        .flat_map(|fragment| fragment.access());
    let accesses = graph_accesses
        .chain(change_accesses)
        .cloned()
        .collect::<Vec<_>>();

    assert_eq!(graph.plan().header().capability_generation(), 17);
    assert_eq!(changes.plan().header().capability_generation(), 17);
    assert!(accesses.contains(&primitive(
        PrimitiveKind::CandidateScan,
        AccessGuarantee::Candidate
    )));
    assert!(accesses.contains(&primitive(
        PrimitiveKind::AdjacencyExpand,
        AccessGuarantee::Exact
    )));
    assert!(accesses.contains(&primitive(
        PrimitiveKind::ChangeScan,
        AccessGuarantee::Candidate
    )));
}

#[test]
fn candidate_scan_is_attached_only_to_node_scans_in_phase_one() {
    let capabilities = CapabilitySnapshot::new(
        5,
        AccessGuarantee::Exact,
        AccessGuarantee::Unsupported,
        AccessGuarantee::Unsupported,
    )
    .expect("capabilities");
    let optimized = optimize_match("MATCH ()-[r]->() RETURN r", capabilities);
    let non_node_accesses = optimized
        .plan()
        .fragments()
        .iter()
        .flat_map(|fragment| fragment.operators().iter().zip(fragment.access()))
        .filter_map(|(operator, access)| {
            (!matches!(operator, PhysicalOperator::NodeScan { .. })).then_some(access.clone())
        })
        .collect::<Vec<_>>();

    assert!(!non_node_accesses.is_empty());
    assert!(non_node_accesses.iter().all(|access| !matches!(
        access,
        PhysicalAccess::Primitive {
            primitive: PrimitiveKind::CandidateScan,
            ..
        }
    )));
}

#[test]
fn system_time_as_of_node_scan_stays_generic() {
    let capabilities = CapabilitySnapshot::new(
        5,
        AccessGuarantee::Exact,
        AccessGuarantee::Unsupported,
        AccessGuarantee::Unsupported,
    )
    .expect("capabilities");
    let optimized = optimize_match(
        "FOR SYSTEM_TIME AS OF $snapshot MATCH (n) RETURN n",
        capabilities,
    );
    let accesses = optimized
        .plan()
        .fragments()
        .iter()
        .flat_map(|fragment| fragment.access())
        .cloned()
        .collect::<Vec<_>>();

    assert!(
        accesses
            .iter()
            .all(|access| *access == PhysicalAccess::Generic)
    );
}
