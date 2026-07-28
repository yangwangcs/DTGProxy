use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use cypher_compiler::{CompileSession, CypherCompiler};
use cypher_engine::{
    CypherQueryEngine, CypherQueryRequest, DeploymentMode, EngineConfig, ExternalTtfr,
    MaterializedPathObservation, MaterializedRunError, MaterializedRunObservation,
    PairedMaterializedReport, QueryMetricAvailability, QueryMetricUnavailableReason,
    QueryOverheadGateError, QueryScopedMetric, QueryScopedOverhead, QueryScopedOverheadLimits,
    ResourceLimits, run_materialized_pair, run_observed_materialized_pair,
};
use distributed_query::{DistributedCoordinator, LocalFragmentWorker};
use query_executor::{
    ExecutionContext, RecordBatch, RuntimeValue, TemporalBatchExecutor, TemporalRead,
};
use query_optimizer::{Optimizer, OptimizerContext};
use temporal_ir::{Column, RowSchema, SlotId, ValueType};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

const QUERY: &str = "MATCH (n) RETURN n";
const SNAPSHOT: TransactionTime = TransactionTime::new(150, 0);
const SECURITY: [u8; 32] = [41; 32];

#[tokio::test]
async fn direct_and_proxy_paths_match_on_a_fixed_fixture_and_snapshot() {
    let direct_store = fixture_store().await;
    let direct_executor = TemporalBatchExecutor::new(direct_store);
    let logical = CypherCompiler::new()
        .compile(
            QUERY,
            &CompileSession::new("accounts", 7, 3, 11).expect("compile session"),
        )
        .expect("compile")
        .logical_plan()
        .clone();
    let direct_plan = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 1 << 20, 4 << 20)
                .expect("optimizer context")
                .with_shard_ids(vec![42])
                .expect("direct shard")
                .with_primary_shard(42),
        )
        .expect("direct plan")
        .plan()
        .clone();
    assert_eq!(direct_plan.fragments().len(), 1);

    let proxy_store = fixture_store().await;
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        SECURITY,
        TemporalBatchExecutor::new(proxy_store),
    );
    let mut coordinator = DistributedCoordinator::new(4 << 20, 16).expect("coordinator");
    coordinator.register(Arc::new(worker)).expect("worker");
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::PrimaryReplica,
            vec![42],
            ResourceLimits::new(1 << 20, 4 << 20, 256).expect("limits"),
        )
        .expect("engine config"),
    );

    let report = run_materialized_pair(
        "memory/accounts-v1",
        SNAPSHOT,
        QUERY,
        3,
        || async {
            direct_executor
                .execute_fragment(
                    &direct_plan.fragments()[0],
                    &ExecutionContext::default(),
                    TemporalRead::as_of(GraphId::new(7), ValidTime::from_micros(5), SNAPSHOT),
                )
                .await
                .map_err(|error| Box::new(error) as MaterializedRunError)
        },
        || async {
            engine
                .execute(
                    &coordinator,
                    CypherQueryRequest::new(
                        QUERY,
                        BTreeMap::new(),
                        ValidTime::from_micros(5),
                        SNAPSHOT,
                        SECURITY,
                        now_ms() + 10_000,
                    )
                    .with_fixed_transaction_snapshot(SNAPSHOT),
                )
                .await
                .map(|response| response.batches().to_vec())
                .map_err(|error| Box::new(error) as MaterializedRunError)
        },
    )
    .await
    .expect("paired gate");

    assert_eq!(report.fixture_id(), "memory/accounts-v1");
    assert_eq!(report.snapshot(), SNAPSHOT);
    assert_eq!(report.semantic_id(), QUERY);
    assert_eq!(
        report.direct().result_digest(),
        report.proxy().result_digest()
    );
    assert_eq!(report.direct().row_count(), 3);
    assert_eq!(report.proxy().row_count(), 3);
    assert_eq!(report.direct().latency_micros().sample_count(), 3);
    assert_eq!(report.proxy().latency_micros().sample_count(), 3);
    assert_eq!(
        report.external_ttfr(),
        ExternalTtfr::UnavailableMaterializedApi
    );
}

#[tokio::test]
async fn observed_pair_uses_query_lifecycle_metrics_from_the_engine_response() {
    let direct_store = fixture_store().await;
    let direct_executor = TemporalBatchExecutor::new(direct_store);
    let logical = CypherCompiler::new()
        .compile(
            QUERY,
            &CompileSession::new("accounts", 7, 3, 11).expect("compile session"),
        )
        .expect("compile")
        .logical_plan()
        .clone();
    let direct_plan = Optimizer::new()
        .optimize(
            &logical,
            OptimizerContext::new(DeploymentMode::PrimaryReplica, 1, 1 << 20, 4 << 20)
                .expect("optimizer context")
                .with_shard_ids(vec![42])
                .expect("direct shard")
                .with_primary_shard(42),
        )
        .expect("direct plan")
        .plan()
        .clone();

    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        SECURITY,
        TemporalBatchExecutor::new(fixture_store().await),
    );
    let mut coordinator = DistributedCoordinator::new(4 << 20, 16).expect("coordinator");
    coordinator.register(Arc::new(worker)).expect("worker");
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::PrimaryReplica,
            vec![42],
            ResourceLimits::new(1 << 20, 4 << 20, 256).expect("limits"),
        )
        .expect("engine config"),
    );

    let report = run_observed_materialized_pair(
        "memory/accounts-v1-observed",
        SNAPSHOT,
        QUERY,
        1,
        || async {
            direct_executor
                .execute_fragment(
                    &direct_plan.fragments()[0],
                    &ExecutionContext::default(),
                    TemporalRead::as_of(GraphId::new(7), ValidTime::from_micros(5), SNAPSHOT),
                )
                .await
                .map(|batches| {
                    MaterializedRunObservation::unavailable(
                        batches,
                        QueryMetricUnavailableReason::ExternalBackendNotObserved,
                    )
                })
                .map_err(|error| Box::new(error) as MaterializedRunError)
        },
        || async {
            engine
                .execute(
                    &coordinator,
                    CypherQueryRequest::new(
                        QUERY,
                        BTreeMap::new(),
                        ValidTime::from_micros(5),
                        SNAPSHOT,
                        SECURITY,
                        now_ms() + 10_000,
                    )
                    .with_fixed_transaction_snapshot(SNAPSHOT),
                )
                .await
                .map(|response| {
                    MaterializedRunObservation::observed(
                        response.batches().to_vec(),
                        response.query_scoped_overhead().clone(),
                    )
                })
                .map_err(|error| Box::new(error) as MaterializedRunError)
        },
    )
    .await
    .expect("observed paired gate");

    assert_eq!(
        report.proxy().query_scoped_overhead().compile_count(),
        &QueryMetricAvailability::Observed(1),
    );
    assert_eq!(
        report.proxy().query_scoped_overhead().optimize_count(),
        &QueryMetricAvailability::Observed(1),
    );
    assert!(matches!(
        report.proxy().query_scoped_overhead().adapter_rpc_count(),
        QueryMetricAvailability::Observed(count) if *count > 0
    ));
    assert!(matches!(
        report.proxy().query_scoped_overhead().sent_frame_count(),
        QueryMetricAvailability::Observed(count) if *count > 0
    ));
    assert!(matches!(
        report.proxy().query_scoped_overhead().wire_encoded_bytes(),
        QueryMetricAvailability::Observed(bytes) if *bytes > 0
    ));
    assert!(matches!(
        report.proxy().query_scoped_overhead().wire_decoded_bytes(),
        QueryMetricAvailability::Observed(bytes) if *bytes > 0
    ));
    assert_eq!(
        report.proxy().query_scoped_overhead().value_copy_bytes(),
        &QueryMetricAvailability::Observed(0),
    );
    assert!(matches!(
        report
            .proxy()
            .query_scoped_overhead()
            .peak_retained_memory_bytes(),
        QueryMetricAvailability::Observed(bytes) if *bytes > 0
    ));
    report
        .evaluate_query_scoped_overhead(&QueryScopedOverheadLimits::new(
            1,
            1,
            4,
            4,
            1 << 20,
            1 << 20,
            0,
            4 << 20,
        ))
        .expect("real query metrics satisfy the release fixture budget");
}

#[test]
fn paired_report_requires_equal_fixture_snapshot_and_result_digest() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let result = vec![
        RecordBatch::try_new(
            schema,
            vec![
                vec![RuntimeValue::Integer(1)],
                vec![RuntimeValue::Integer(2)],
            ],
        )
        .expect("batch"),
    ];
    let direct = MaterializedPathObservation::from_samples(&result, vec![30, 10, 20]);
    let proxy = MaterializedPathObservation::from_samples(&result, vec![40, 20, 30]);

    let report = PairedMaterializedReport::new(
        "memory/accounts-v1",
        TransactionTime::new(150, 0),
        "MATCH (n) RETURN n",
        direct,
        proxy,
    )
    .expect("paired report");

    assert_eq!(
        report.direct().result_digest(),
        report.proxy().result_digest()
    );
    assert_eq!(report.direct().row_count(), 2);
    assert_eq!(report.direct().batch_count(), 1);
    assert_eq!(report.direct().latency_micros().sample_count(), 3);
    assert_eq!(report.direct().latency_micros().p50(), 20);
    assert_eq!(report.direct().latency_micros().p95(), 30);
    assert_eq!(report.direct().latency_micros().p99(), 30);
    assert_eq!(
        report.external_ttfr(),
        ExternalTtfr::UnavailableMaterializedApi
    );
}

#[test]
fn paired_report_rejects_a_semantic_mismatch() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let direct = vec![
        RecordBatch::try_new(schema.clone(), vec![vec![RuntimeValue::Integer(1)]])
            .expect("direct batch"),
    ];
    let proxy = vec![
        RecordBatch::try_new(schema, vec![vec![RuntimeValue::Integer(2)]]).expect("proxy batch"),
    ];

    assert!(
        PairedMaterializedReport::new(
            "memory/accounts-v1",
            TransactionTime::new(150, 0),
            "MATCH (n) RETURN n",
            MaterializedPathObservation::from_samples(&direct, vec![1]),
            MaterializedPathObservation::from_samples(&proxy, vec![1]),
        )
        .is_err()
    );
}

#[tokio::test]
async fn paired_runner_rejects_semantic_drift_in_any_sample() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let direct_runs = Arc::new(AtomicUsize::new(0));

    let result = run_materialized_pair(
        "memory/drift",
        SNAPSHOT,
        QUERY,
        2,
        {
            let schema = schema.clone();
            let direct_runs = Arc::clone(&direct_runs);
            move || {
                let schema = schema.clone();
                let value = direct_runs.fetch_add(1, Ordering::Relaxed) + 1;
                async move {
                    Ok(vec![
                        RecordBatch::try_new(
                            schema,
                            vec![vec![RuntimeValue::Integer(value as i64)]],
                        )
                        .expect("direct batch"),
                    ])
                }
            }
        },
        move || {
            let schema = schema.clone();
            async move {
                Ok(vec![
                    RecordBatch::try_new(schema, vec![vec![RuntimeValue::Integer(1)]])
                        .expect("proxy batch"),
                ])
            }
        },
    )
    .await;

    assert!(matches!(
        result,
        Err(cypher_engine::PairedReportError::ResultMismatch)
    ));
}

#[test]
fn result_digest_includes_schema_and_values_but_not_batch_boundaries() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let one_batch = vec![
        RecordBatch::try_new(
            schema.clone(),
            vec![
                vec![RuntimeValue::Integer(1)],
                vec![RuntimeValue::Integer(2)],
            ],
        )
        .expect("one batch"),
    ];
    let two_batches = vec![
        RecordBatch::try_new(schema.clone(), vec![vec![RuntimeValue::Integer(1)]])
            .expect("first batch"),
        RecordBatch::try_new(schema, vec![vec![RuntimeValue::Integer(2)]]).expect("second batch"),
    ];

    let one = MaterializedPathObservation::from_samples(&one_batch, vec![1]);
    let two = MaterializedPathObservation::from_samples(&two_batches, vec![1]);
    assert_eq!(one.result_digest(), two.result_digest());
    assert_ne!(one.batch_count(), two.batch_count());
}

#[test]
fn query_scoped_overhead_gate_rejects_metrics_unavailable_from_materialized_runs() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let result = vec![
        RecordBatch::try_new(schema, vec![vec![RuntimeValue::Integer(1)]]).expect("result batch"),
    ];
    let direct = MaterializedPathObservation::from_samples(&result, vec![1]);
    let proxy = MaterializedPathObservation::from_samples(&result, vec![1]);
    let report = PairedMaterializedReport::new(
        "memory/accounts-v1",
        TransactionTime::new(150, 0),
        "MATCH (n) RETURN n",
        direct,
        proxy,
    )
    .expect("paired report");

    assert_eq!(
        report.proxy().query_scoped_overhead().adapter_rpc_count(),
        &QueryMetricAvailability::Unavailable(
            QueryMetricUnavailableReason::MaterializedExecutionApi,
        ),
    );
    assert!(matches!(
        report.evaluate_query_scoped_overhead(&QueryScopedOverheadLimits::new(
            1, 1, 1, 1, 1, 1, 1, 1,
        )),
        Err(QueryOverheadGateError::MetricUnavailable {
            metric: QueryScopedMetric::CompileCount,
            reason: QueryMetricUnavailableReason::MaterializedExecutionApi,
        })
    ));
}

#[test]
fn query_scoped_overhead_gate_accepts_observed_metrics_within_limits() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let result = vec![
        RecordBatch::try_new(schema, vec![vec![RuntimeValue::Integer(1)]]).expect("result batch"),
    ];
    let overhead = QueryScopedOverhead::observed(1, 1, 1, 1, 64, 64, 0, 1024);
    let direct = MaterializedPathObservation::from_samples_with_query_scoped_overhead(
        &result,
        vec![1],
        overhead.clone(),
    );
    let proxy = MaterializedPathObservation::from_samples_with_query_scoped_overhead(
        &result,
        vec![1],
        overhead,
    );
    let report = PairedMaterializedReport::new(
        "memory/accounts-v1",
        TransactionTime::new(150, 0),
        "MATCH (n) RETURN n",
        direct,
        proxy,
    )
    .expect("paired report");

    report
        .evaluate_query_scoped_overhead(&QueryScopedOverheadLimits::new(
            1, 1, 1, 1, 64, 64, 0, 1024,
        ))
        .expect("observed metrics satisfy limits");
}

#[test]
fn query_scoped_overhead_gate_rejects_an_observed_limit_breach() {
    let schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .expect("schema");
    let result = vec![
        RecordBatch::try_new(schema, vec![vec![RuntimeValue::Integer(1)]]).expect("result batch"),
    ];
    let direct = MaterializedPathObservation::from_samples_with_query_scoped_overhead(
        &result,
        vec![1],
        QueryScopedOverhead::observed(1, 1, 1, 1, 64, 64, 0, 1024),
    );
    let proxy = MaterializedPathObservation::from_samples_with_query_scoped_overhead(
        &result,
        vec![1],
        QueryScopedOverhead::observed(2, 1, 1, 1, 64, 64, 0, 1024),
    );
    let report = PairedMaterializedReport::new(
        "memory/accounts-v1",
        TransactionTime::new(150, 0),
        "MATCH (n) RETURN n",
        direct,
        proxy,
    )
    .expect("paired report");

    assert!(matches!(
        report.evaluate_query_scoped_overhead(&QueryScopedOverheadLimits::new(
            1, 1, 1, 1, 64, 64, 0, 1024,
        )),
        Err(QueryOverheadGateError::LimitExceeded {
            metric: QueryScopedMetric::CompileCount,
            observed: 2,
            limit: 1,
        })
    ));
}

async fn fixture_store() -> TemporalStore<MemoryAdapter> {
    let store = TemporalStore::new(MemoryAdapter::new());
    for (index, element_id) in [3_u128, 1, 2].into_iter().enumerate() {
        let element = ElementRef::vertex(
            GraphId::new(7),
            PartitionId::new(42),
            ElementId::new(element_id),
        );
        store
            .commit_vertex(
                CommitContext::new(
                    42,
                    u64::try_from(index + 1).expect("log index"),
                    element_id,
                    TransactionTime::new(0, 0),
                    TransactionTime::new(100, 0),
                ),
                VertexMutation::put(
                    element,
                    LabelId::new(1),
                    Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                    CanonicalElement::new(
                        1,
                        BTreeMap::from([(1, GraphValue::Integer(element_id as i64))]),
                    ),
                )
                .expect("mutation"),
            )
            .await
            .expect("commit");
    }
    store
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("milliseconds")
}
