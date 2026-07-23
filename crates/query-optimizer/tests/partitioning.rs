use cypher_compiler::{CompileSession, CypherCompiler};
use physical_plan::{ExchangeKind, Placement};
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};

fn logical() -> temporal_ir::LogicalPlan {
    CypherCompiler::new()
        .compile(
            "AT VALID_TIME AS OF $valid MATCH (a)-[r]->(b) WHERE a.active = true RETURN a, b",
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
