use cypher_compiler::{CompileSession, CypherCompiler};
use physical_plan::PhysicalOperator;
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};
use temporal_ir::LogicalOperator;

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
