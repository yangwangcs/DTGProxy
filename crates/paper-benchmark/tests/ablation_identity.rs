use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use cypher_engine::{
    CypherQueryEngine, CypherQueryRequest, DeploymentMode, EngineConfig, ResourceLimits, schema_id,
};
use distributed_query::{DistributedCoordinator, LocalFragmentWorker};
use paper_benchmark::{
    AblationAxis, BenchmarkAblationConfig, BenchmarkAblationCounters,
    BenchmarkAblationCountersSnapshot, ResultIdentity, validate_ablation_exercised,
};
use physical_plan::{
    AccessGuarantee, MemoryBudget, PhysicalAccess, PhysicalComparisonOperator, PhysicalOperator,
    PhysicalPlan, PhysicalPlanBuilder, PhysicalPlanHeader, PhysicalPropertyConstraint, Placement,
    PrimitiveKind, ResidualPolicy,
};
use query_executor::{ExecutionContext, TemporalBatchExecutor, TemporalRead};
use temporal_ir::{Column, RowSchema, ScalarExpr, SlotId, ValueType};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

const GRAPH_ID: u64 = 7;
const SCHEMA_VERSION: u64 = 3;
const TOPOLOGY_EPOCH: u64 = 11;
const SHARDS: [u32; 2] = [1, 2];
const SECURITY: [u8; 32] = [0x5a; 32];

#[derive(Debug)]
struct FixtureObservation {
    schemas: Vec<RowSchema>,
    identity: ResultIdentity,
    counters: BenchmarkAblationCountersSnapshot,
}

struct Fixture {
    engine: CypherQueryEngine,
    coordinator: DistributedCoordinator,
}

impl Fixture {
    async fn new() -> Self {
        let mut coordinator = DistributedCoordinator::new(8 << 20, 8).expect("coordinator");
        for shard_id in SHARDS {
            let store = TemporalStore::new(MemoryAdapter::new());
            seed_shard(&store, shard_id).await;
            let worker = LocalFragmentWorker::new(
                shard_id,
                GRAPH_ID,
                SCHEMA_VERSION,
                TOPOLOGY_EPOCH,
                SECURITY,
                TemporalBatchExecutor::new(store),
            );
            coordinator.register(Arc::new(worker)).expect("worker");
        }
        let engine = CypherQueryEngine::new(
            EngineConfig::new(
                "accounts",
                GRAPH_ID,
                SCHEMA_VERSION,
                TOPOLOGY_EPOCH,
                DeploymentMode::SharedNothing,
                SHARDS.to_vec(),
                ResourceLimits::new(8 << 20, 32 << 20, 1).expect("limits"),
            )
            .expect("engine config"),
        );
        Self {
            engine,
            coordinator,
        }
    }

    async fn run(&self, config: BenchmarkAblationConfig) -> FixtureObservation {
        let counters = Arc::new(BenchmarkAblationCounters::default());
        let mut schemas = Vec::new();
        let mut rows = Vec::new();
        let candidate_store = TemporalStore::new(MemoryAdapter::new());
        seed_shard(&candidate_store, 0).await;
        let candidate_executor = TemporalBatchExecutor::new(candidate_store);
        let candidate_plan = candidate_plan();
        let candidate_fragment = &candidate_plan.fragments()[0];
        let candidate_context =
            ExecutionContext::default().with_benchmark_ablations(config, Arc::clone(&counters));
        let mut candidate_source = candidate_executor.open_fragment_morsels(
            candidate_fragment,
            &candidate_context,
            TemporalRead::current(GraphId::new(GRAPH_ID), ValidTime::from_micros(5)),
            None,
            1,
        );
        schemas.push(candidate_fragment.output().clone());
        while let Some(morsel) = candidate_source.next().await.expect("candidate morsel") {
            rows.extend(
                morsel
                    .batch()
                    .rows()
                    .iter()
                    .map(|row| format!("candidate:{row:?}")),
            );
        }
        drop(candidate_source);

        let expand_plan = current_expand_plan();
        let expand_fragment = &expand_plan.fragments()[0];
        let expand_batches = candidate_executor
            .execute_fragment(
                expand_fragment,
                &candidate_context,
                TemporalRead::current(GraphId::new(GRAPH_ID), ValidTime::from_micros(5)),
            )
            .await
            .expect("current expand fixture");
        schemas.push(expand_fragment.output().clone());
        rows.extend(
            expand_batches
                .iter()
                .flat_map(|batch| batch.rows())
                .map(|row| format!("current-expand:{row:?}")),
        );

        let response = self
            .engine
            .execute(
                &self.coordinator,
                CypherQueryRequest::new(
                    "USE accounts MATCH (n) RETURN n",
                    BTreeMap::new(),
                    ValidTime::from_micros(5),
                    TransactionTime::new(1_000, 0),
                    SECURITY,
                    deadline_ms(),
                )
                .with_benchmark_ablations(config, Arc::clone(&counters)),
            )
            .await
            .expect("distributed scan fixture");
        schemas.push(response.schema().clone());
        rows.extend(
            response
                .batches()
                .iter()
                .flat_map(|batch| batch.rows())
                .map(|row| format!("distributed:{row:?}")),
        );
        rows.sort();
        let row_count = u64::try_from(rows.len()).expect("fixture row count");
        let digest = blake3::hash(rows.join("\n").as_bytes())
            .to_hex()
            .to_string();
        FixtureObservation {
            schemas,
            identity: ResultIdentity { digest, row_count },
            counters: counters.snapshot(),
        }
    }
}

fn candidate_plan() -> PhysicalPlan {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("candidate schema");
    let active = schema_id("active");
    let predicate = ScalarExpr::Equal(
        Box::new(ScalarExpr::Property {
            value: Box::new(ScalarExpr::Slot(SlotId::new(0))),
            property_id: active,
        }),
        Box::new(ScalarExpr::Literal(GraphValue::Boolean(true))),
    );
    let constraint = PhysicalPropertyConstraint::new(
        active,
        PhysicalComparisonOperator::Equal,
        GraphValue::Boolean(true),
    );
    let header = PhysicalPlanHeader::new(GRAPH_ID, SCHEMA_VERSION, TOPOLOGY_EPOCH, [0xa5; 32])
        .expect("physical header");
    let mut builder = PhysicalPlanBuilder::new(header);
    let root = builder
        .add_fragment_with_access(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: schema.clone(),
                },
                PhysicalOperator::Filter(predicate),
            ],
            vec![
                PhysicalAccess::Primitive {
                    primitive: PrimitiveKind::CandidateScan,
                    guarantee: AccessGuarantee::Candidate,
                    residual: ResidualPolicy::Evaluate,
                    constraints: vec![constraint],
                },
                PhysicalAccess::Generic,
            ],
            schema,
            MemoryBudget::new(8 << 20, 32 << 20).expect("physical budget"),
        )
        .expect("candidate fragment");
    builder.finish(root).expect("candidate plan")
}

fn current_expand_plan() -> PhysicalPlan {
    let scan_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "n",
        ValueType::Node,
        false,
    )])
    .expect("scan schema");
    let expand_schema = RowSchema::new(vec![
        scan_schema.columns()[0].clone(),
        Column::new(SlotId::new(1), "r", ValueType::Relationship, false),
        Column::new(SlotId::new(2), "m", ValueType::Node, false),
    ])
    .expect("expand schema");
    let mut builder = PhysicalPlanBuilder::new(
        PhysicalPlanHeader::new(GRAPH_ID, SCHEMA_VERSION, TOPOLOGY_EPOCH, [0xb6; 32])
            .expect("expand header"),
    );
    let root = builder
        .add_fragment(
            Placement::Shard(0),
            vec![
                PhysicalOperator::NodeScan {
                    binding: SlotId::new(0),
                    labels: Vec::new(),
                    output: scan_schema,
                },
                PhysicalOperator::Expand {
                    source: SlotId::new(0),
                    relationship: SlotId::new(1),
                    destination: SlotId::new(2),
                    outgoing: true,
                    types: Vec::new(),
                    output: expand_schema.clone(),
                },
            ],
            expand_schema,
            MemoryBudget::new(8 << 20, 32 << 20).expect("expand budget"),
        )
        .expect("expand fragment");
    builder.finish(root).expect("expand plan")
}

#[tokio::test(flavor = "current_thread")]
async fn every_single_disabled_ablation_preserves_identity_and_executes_its_alternate() {
    let fixture = Fixture::new().await;
    let production = fixture.run(BenchmarkAblationConfig::default()).await;
    assert_eq!(
        production.counters,
        BenchmarkAblationCountersSnapshot::default()
    );

    let cases = [
        (
            AblationAxis::NativePushdown,
            BenchmarkAblationConfig {
                native_pushdown: false,
                ..BenchmarkAblationConfig::default()
            },
        ),
        (
            AblationAxis::ColumnBatches,
            BenchmarkAblationConfig {
                column_batches: false,
                ..BenchmarkAblationConfig::default()
            },
        ),
        (
            AblationAxis::BoundedLazyPages,
            BenchmarkAblationConfig {
                bounded_lazy_pages: false,
                ..BenchmarkAblationConfig::default()
            },
        ),
        (
            AblationAxis::ParallelShardFanout,
            BenchmarkAblationConfig {
                parallel_shard_fanout: false,
                ..BenchmarkAblationConfig::default()
            },
        ),
        (
            AblationAxis::BatchedPropertyGather,
            BenchmarkAblationConfig {
                batched_property_gather: false,
                ..BenchmarkAblationConfig::default()
            },
        ),
    ];

    for (axis, config) in cases {
        let ablated = fixture.run(config).await;
        assert_eq!(
            ablated.schemas, production.schemas,
            "schema drift for {axis:?}"
        );
        assert_eq!(
            ablated.identity, production.identity,
            "identity drift for {axis:?}"
        );
        validate_ablation_exercised(config, ablated.counters).unwrap_or_else(|error| {
            panic!("alternate path was not exercised for {axis:?}: {error}")
        });
        assert_eq!(ablated.counters.exercised_axes(), vec![axis]);
    }
}

#[test]
fn production_defaults_enable_every_optimization_and_zero_counters_are_unsupported_when_disabled() {
    assert_eq!(
        BenchmarkAblationConfig::default(),
        BenchmarkAblationConfig {
            native_pushdown: true,
            column_batches: true,
            bounded_lazy_pages: true,
            parallel_shard_fanout: true,
            batched_property_gather: true,
        }
    );
    let unsupported = validate_ablation_exercised(
        BenchmarkAblationConfig {
            native_pushdown: false,
            ..BenchmarkAblationConfig::default()
        },
        BenchmarkAblationCountersSnapshot::default(),
    )
    .expect_err("a label-only ablation must not be reported as available");
    assert_eq!(unsupported.axis(), AblationAxis::NativePushdown);
}

async fn seed_shard(store: &TemporalStore<MemoryAdapter>, shard_id: u32) {
    let partition = PartitionId::new(shard_id);
    let base = u128::from(shard_id) * 100;
    let active = schema_id("active");
    let lifetime = Interval::new(ValidTime::from_micros(0), None).expect("lifetime");
    let mut vertices = TemporalTransaction::new();
    for (offset, is_active) in [
        (1_u128, true),
        (2, true),
        (11, false),
        (12, false),
        (21, false),
        (22, false),
    ] {
        vertices = vertices.with_vertex(
            VertexMutation::put(
                ElementRef::vertex(
                    GraphId::new(GRAPH_ID),
                    partition,
                    ElementId::new(base + offset),
                ),
                LabelId::new(1),
                lifetime,
                CanonicalElement::new(
                    1,
                    BTreeMap::from([(active, GraphValue::Boolean(is_active))]),
                ),
            )
            .expect("vertex"),
        );
    }
    store
        .commit_transaction(
            CommitContext::new(
                shard_id,
                1,
                base + 1,
                TransactionTime::new(0, 0),
                TransactionTime::new(100, 0),
            ),
            vertices,
        )
        .await
        .expect("vertices");

    let mut edges = TemporalTransaction::new();
    for (edge_offset, source_offset, destination_offset) in [
        (31_u128, 1_u128, 11_u128),
        (32, 1, 12),
        (33, 2, 21),
        (34, 2, 22),
    ] {
        edges = edges.with_edge(
            EdgeMutation::put(
                ElementRef::edge(
                    GraphId::new(GRAPH_ID),
                    partition,
                    ElementId::new(base + edge_offset),
                ),
                EdgeTypeId::new(1),
                ElementId::new(base + source_offset),
                ElementId::new(base + destination_offset),
                lifetime,
                CanonicalElement::new(1, BTreeMap::new()),
            )
            .expect("edge"),
        );
    }
    store
        .commit_transaction(
            CommitContext::new(
                shard_id,
                2,
                base + 2,
                TransactionTime::new(100, 0),
                TransactionTime::new(200, 0),
            ),
            edges,
        )
        .await
        .expect("edges");
}

fn deadline_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_millis(),
    )
    .expect("deadline")
        + 30_000
}
