use cypher_compiler::{CompileSession, CypherCompiler};
use physical_plan::{ExchangeKind, Placement};
use query_optimizer::{DeploymentMode, Optimizer, OptimizerContext};

fn logical() -> temporal_ir::v2::LogicalPlan {
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
