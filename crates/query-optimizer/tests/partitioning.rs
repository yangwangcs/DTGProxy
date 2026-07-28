use cypher_compiler::{CompileSession, CypherCompiler};
use physical_plan::{COUNT_AGGREGATE_FUNCTION_ID, ExchangeKind, PhysicalOperator, Placement};
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};
use temporal_ir::{
    Column, LanguageProfile, LogicalOperator, LogicalPlanBuilder, PlanHeader, RowSchema,
    ScalarExpr, SlotId, ValueType,
};

fn logical() -> temporal_ir::LogicalPlan {
    CypherCompiler::new()
        .compile(
            "FOR VALID_TIME AS OF $valid MATCH (a)-[r]->(b) WHERE a.active = true RETURN a, b",
            &CompileSession::new("accounts", 7, 3, 11).expect("session"),
        )
        .expect("compile")
        .logical_plan()
        .clone()
}

#[test]
fn shared_nothing_keeps_graph_work_on_shards_and_gathers_results() {
    let optimized = Optimizer::new()
        .optimize(
            &logical(),
            OptimizerContext::new(DeploymentMode::SharedNothing, 8, 64 << 20, 256 << 20)
                .expect("context"),
        )
        .expect("optimize");

    optimized.plan().validate().expect("physical plan");
    assert_eq!(
        optimized.plan().fragments()[0].placement(),
        Placement::AllShards
    );
    assert_eq!(
        optimized
            .plan()
            .fragments()
            .last()
            .expect("root")
            .placement(),
        Placement::Coordinator
    );
    assert!(
        optimized
            .plan()
            .exchanges()
            .iter()
            .any(|exchange| matches!(exchange.kind(), ExchangeKind::Gather))
    );
    assert!(
        optimized
            .trace()
            .iter()
            .any(|event| event.rule() == "partition-local-graph-operators")
    );
}

#[test]
fn shared_nothing_pushes_exact_bounded_top_k_to_each_shard() {
    let logical = CypherCompiler::new()
        .compile(
            "MATCH (n) WHERE n.active = true WITH n.id AS id ORDER BY id LIMIT 4096 RETURN id",
            &CompileSession::new("scale_graph", 7, 3, 11).expect("session"),
        )
        .expect("compile bounded top-k")
        .logical_plan()
        .clone();
    let optimized = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 8, 64 << 20, 256 << 20)
                .expect("context"),
        )
        .expect("optimize bounded top-k");

    optimized.plan().validate().expect("physical top-k plan");
    let shard = optimized
        .plan()
        .fragments()
        .iter()
        .find(|fragment| fragment.placement() == Placement::AllShards)
        .expect("shard fragment");
    assert!(matches!(
        shard.operators(),
        [
            ..,
            PhysicalOperator::Project { .. },
            PhysicalOperator::Sort { .. },
            PhysicalOperator::Limit {
                count: ScalarExpr::Literal(temporal_types::GraphValue::Integer(4096))
            }
        ]
    ));
    let bounded_schema = logical
        .nodes()
        .iter()
        .find(|node| matches!(node.operator(), LogicalOperator::Limit { .. }))
        .expect("logical limit")
        .output();
    assert_eq!(shard.output(), bounded_schema);

    let coordinator = optimized
        .plan()
        .fragments()
        .iter()
        .find(|fragment| fragment.placement() == Placement::Coordinator)
        .expect("coordinator fragment");
    assert!(matches!(
        coordinator.operators(),
        [
            PhysicalOperator::Sort { .. },
            PhysicalOperator::Limit {
                count: ScalarExpr::Literal(temporal_types::GraphValue::Integer(4096))
            },
            PhysicalOperator::Project { .. }
        ]
    ));
    let exchange = optimized
        .plan()
        .exchanges()
        .iter()
        .find(|exchange| exchange.from() == shard.id())
        .expect("bounded shard exchange");
    assert_eq!(exchange.schema(), bounded_schema);
    assert!(
        optimized
            .trace()
            .iter()
            .any(|event| event.rule() == "partition-local-exact-top-k")
    );
}

#[test]
fn shared_nothing_only_pushes_statically_bounded_top_k_without_skip() {
    for query in [
        "MATCH (n) WITH n.id AS id ORDER BY id SKIP 1 LIMIT 4096 RETURN id",
        "MATCH (n) WITH n.id AS id ORDER BY id LIMIT $limit RETURN id",
        "MATCH (n) RETURN n.id LIMIT 4096",
    ] {
        let logical = CypherCompiler::new()
            .compile(
                query,
                &CompileSession::new("scale_graph", 7, 3, 11).expect("session"),
            )
            .expect("compile conservative limit")
            .logical_plan()
            .clone();
        let optimized = Optimizer::new()
            .optimize(
                &logical,
                OptimizerContext::new(DeploymentMode::SharedNothing, 8, 64 << 20, 256 << 20)
                    .expect("context"),
            )
            .expect("optimize conservative limit");
        let shard = optimized
            .plan()
            .fragments()
            .iter()
            .find(|fragment| fragment.placement() == Placement::AllShards)
            .expect("shard fragment");

        assert!(
            shard.operators().iter().all(|operator| !matches!(
                operator,
                PhysicalOperator::Sort { .. } | PhysicalOperator::Limit { .. }
            )),
            "unsafe limit shape must retain the coordinator gather path: {query}"
        );
        assert!(
            optimized
                .trace()
                .iter()
                .all(|event| event.rule() != "partition-local-exact-top-k")
        );
    }
}

#[test]
fn shared_nothing_exchanges_one_partial_row_per_shard_for_global_count() {
    for query in [
        "USE scale_graph FOR VALID_TIME AS OF 1000 MATCH (n) RETURN count(n) AS count",
        "USE scale_graph FOR VALID_TIME AS OF 1000 MATCH (n) RETURN count(*) AS count",
    ] {
        let logical = CypherCompiler::new()
            .compile(
                query,
                &CompileSession::new("scale_graph", 7, 3, 11).expect("session"),
            )
            .expect("compile count")
            .logical_plan()
            .clone();
        let optimized = Optimizer::new()
            .optimize(
                &logical,
                OptimizerContext::new(DeploymentMode::SharedNothing, 8, 64 << 20, 256 << 20)
                    .expect("context"),
            )
            .expect("optimize count");

        optimized.plan().validate().expect("physical count plan");
        let shard = optimized
            .plan()
            .fragments()
            .iter()
            .find(|fragment| fragment.placement() == Placement::AllShards)
            .expect("shard fragment");
        assert!(
            shard.operators().iter().any(|operator| matches!(
                operator,
                PhysicalOperator::Aggregate {
                    phase: physical_plan::AggregatePhase::PartialCount,
                    ..
                }
            )),
            "count must be partially aggregated on each shard: {query}"
        );
        let exchange = optimized
            .plan()
            .exchanges()
            .iter()
            .find(|exchange| exchange.from() == shard.id())
            .expect("shard exchange");
        assert_eq!(exchange.schema().columns().len(), 1, "{query}");
        let coordinator = optimized
            .plan()
            .fragments()
            .iter()
            .find(|fragment| fragment.placement() == Placement::Coordinator)
            .expect("coordinator fragment");
        assert!(
            coordinator.operators().iter().any(|operator| matches!(
                operator,
                PhysicalOperator::Aggregate {
                    phase: physical_plan::AggregatePhase::FinalCount,
                    ..
                }
            )),
            "partial counts must be finalized on the coordinator: {query}"
        );
    }
}

#[test]
fn shared_nothing_keeps_unsafe_or_non_count_aggregates_on_the_coordinator() {
    for query in [
        "MATCH (n) RETURN count(n.amount) AS count",
        "MATCH (n) RETURN n, count(n) AS count",
        "MATCH (n) RETURN sum(n.amount) AS total",
    ] {
        let logical = CypherCompiler::new()
            .compile(
                query,
                &CompileSession::new("accounts", 7, 3, 11).expect("session"),
            )
            .expect("compile aggregate")
            .logical_plan()
            .clone();
        let optimized = Optimizer::new()
            .optimize(
                &logical,
                OptimizerContext::new(DeploymentMode::SharedNothing, 8, 64 << 20, 256 << 20)
                    .expect("context"),
            )
            .expect("optimize aggregate");
        let shard = optimized
            .plan()
            .fragments()
            .iter()
            .find(|fragment| fragment.placement() == Placement::AllShards)
            .expect("shard fragment");

        assert!(
            shard
                .operators()
                .iter()
                .all(|operator| !matches!(operator, PhysicalOperator::Aggregate { .. })),
            "unsafe aggregate must retain the global gather path: {query}"
        );
    }
}

#[test]
fn shared_nothing_does_not_push_count_of_a_nullable_variable() {
    let input = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "maybe_n",
        ValueType::Node,
        true,
    )])
    .expect("input schema");
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(1),
        "count",
        ValueType::Any,
        true,
    )])
    .expect("output schema");
    let header = PlanHeader::new(
        7,
        3,
        11,
        LanguageProfile::Cypher25,
        "Cypher 25 / nullable count test",
        [41; 32],
    )
    .expect("header");
    let mut builder = LogicalPlanBuilder::new(header);
    let scan = builder
        .add(
            LogicalOperator::NodeScan {
                binding: SlotId::new(0),
                labels: Vec::new(),
            },
            Vec::new(),
            input,
        )
        .expect("scan");
    let aggregate = builder
        .add(
            LogicalOperator::Aggregate {
                grouping: Vec::new(),
                aggregates: vec![(
                    SlotId::new(1),
                    ScalarExpr::Function {
                        function_id: COUNT_AGGREGATE_FUNCTION_ID,
                        arguments: vec![ScalarExpr::Slot(SlotId::new(0))],
                    },
                )],
            },
            vec![scan],
            output,
        )
        .expect("aggregate");
    let logical = builder.finish(aggregate).expect("logical plan");
    let optimized = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 8, 64 << 20, 256 << 20)
                .expect("context"),
        )
        .expect("optimize nullable count");
    let shard = optimized
        .plan()
        .fragments()
        .iter()
        .find(|fragment| fragment.placement() == Placement::AllShards)
        .expect("shard fragment");

    assert!(
        shard
            .operators()
            .iter()
            .all(|operator| !matches!(operator, PhysicalOperator::Aggregate { .. }))
    );
}

#[test]
fn primary_replica_uses_one_local_fragment_without_exchange() {
    let optimized = Optimizer::new()
        .optimize(
            &logical(),
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 64 << 20, 256 << 20)
                .expect("context"),
        )
        .expect("optimize");

    assert_eq!(optimized.plan().fragments().len(), 1);
    assert_eq!(
        optimized.plan().fragments()[0].placement(),
        Placement::Shard(0)
    );
    assert!(optimized.plan().exchanges().is_empty());
}

#[test]
fn primary_replica_preserves_catalog_shard_identity() {
    let context = OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 64 << 20, 256 << 20)
        .expect("context")
        .with_primary_shard(42);
    let optimized = Optimizer::new()
        .optimize(&logical(), context)
        .expect("optimize");

    assert_eq!(
        optimized.plan().fragments()[0].placement(),
        Placement::Shard(42)
    );
}

#[test]
fn shared_nothing_runs_source_free_unwind_once_on_the_coordinator() {
    let logical = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2, 3] AS item RETURN item",
            &CompileSession::new("accounts", 7, 3, 11).expect("session"),
        )
        .expect("compile")
        .logical_plan()
        .clone();
    let optimized = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::SharedNothing, 2, 64 << 20, 256 << 20)
                .expect("context"),
        )
        .expect("optimize");

    assert_eq!(optimized.plan().fragments().len(), 1);
    assert_eq!(
        optimized.plan().fragments()[0].placement(),
        Placement::Coordinator
    );
    assert!(optimized.plan().exchanges().is_empty());
}

#[test]
fn primary_replica_also_runs_source_free_pipelines_on_the_coordinator() {
    let logical = CypherCompiler::new()
        .compile(
            "UNWIND [1, 2] AS item RETURN item",
            &CompileSession::new("accounts", 7, 3, 11).expect("session"),
        )
        .expect("compile")
        .logical_plan()
        .clone();
    let optimized = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 64 << 20, 256 << 20)
                .expect("context")
                .with_primary_shard(42),
        )
        .expect("optimize");

    assert_eq!(optimized.plan().fragments().len(), 1);
    assert_eq!(
        optimized.plan().fragments()[0].placement(),
        Placement::Coordinator
    );
    assert!(optimized.plan().exchanges().is_empty());
}

#[test]
fn global_procedures_are_coordinator_only_in_both_deployment_modes() {
    for (mode, shards) in [
        (DeploymentMode::PrimaryReplica, 1),
        (DeploymentMode::SharedNothing, 2),
    ] {
        let source_free = CypherCompiler::new()
            .compile(
                "CALL dtg.graph.degree({}) YIELD vertexId, degree RETURN vertexId, degree",
                &CompileSession::new("accounts", 7, 3, 11).unwrap(),
            )
            .unwrap();
        let optimized = Optimizer::new()
            .optimize(
                source_free.logical_plan(),
                OptimizerContext::new(mode, shards, 64 << 20, 256 << 20).unwrap(),
            )
            .unwrap();
        assert_eq!(optimized.plan().fragments().len(), 1);
        assert_eq!(
            optimized.plan().fragments()[0].placement(),
            Placement::Coordinator
        );

        let correlated = CypherCompiler::new()
            .compile(
                "MATCH (n) CALL dtg.graph.degree({}) YIELD degree RETURN n, degree",
                &CompileSession::new("accounts", 7, 3, 11).unwrap(),
            )
            .unwrap();
        let optimized = Optimizer::new()
            .optimize(
                correlated.logical_plan(),
                OptimizerContext::new(mode, shards, 64 << 20, 256 << 20).unwrap(),
            )
            .unwrap();
        assert!(optimized.plan().fragments().iter().any(|fragment| {
            fragment.placement() == Placement::Coordinator
                && fragment.operators().iter().any(|operator| {
                    matches!(operator, physical_plan::PhysicalOperator::Procedure { .. })
                })
        }));
        assert!(
            optimized
                .plan()
                .exchanges()
                .iter()
                .any(|exchange| { matches!(exchange.kind(), ExchangeKind::Gather) })
        );
    }
}
