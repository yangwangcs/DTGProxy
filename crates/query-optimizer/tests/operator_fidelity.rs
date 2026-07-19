use cypher_compiler::{CompileSession, CypherCompiler};
use physical_plan::PhysicalOperator;
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};
use temporal_ir::v2::LogicalOperator;

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
                },
            ) => {
                assert_eq!(actual_binding, binding);
                assert_eq!(actual_labels, labels);
            }
            (
                LogicalOperator::Expand {
                    source,
                    relationship,
                    destination,
                    outgoing,
                },
                PhysicalOperator::Expand {
                    source: actual_source,
                    relationship: actual_relationship,
                    destination: actual_destination,
                    outgoing: actual_outgoing,
                },
            ) => {
                assert_eq!(actual_source, source);
                assert_eq!(actual_relationship, relationship);
                assert_eq!(actual_destination, destination);
                assert_eq!(actual_outgoing, outgoing);
            }
            (
                LogicalOperator::Project { expressions },
                PhysicalOperator::Project {
                    expressions: actual_expressions,
                },
            ) => assert_eq!(actual_expressions, expressions),
            (LogicalOperator::Filter { predicate }, PhysicalOperator::Filter(actual)) => {
                assert_eq!(actual, predicate);
            }
            (LogicalOperator::Argument, PhysicalOperator::Argument)
            | (LogicalOperator::TemporalSlice, PhysicalOperator::TemporalSlice) => {}
            (logical, physical) => panic!("operator lost information: {logical:?} -> {physical:?}"),
        }
    }
}
