use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_memory::MemoryAdapter;
use cypher_engine::{
    CypherQueryEngine, CypherQueryRequest, DeploymentMode, EngineConfig, EngineError,
    ResourceLimits,
};
use distributed_query::{DistributedCoordinator, LocalFragmentWorker};
use query_executor::{
    GraphOverlay, GraphOverlayEntry, RuntimeValue, TemporalBatchExecutor, VertexRecord,
};
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn expired_query_deadline_is_installed_on_the_execution_context() {
    let security = [37; 32];
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::PrimaryReplica,
            vec![42],
            ResourceLimits::new(1 << 20, 4 << 20, 256).unwrap(),
        )
        .unwrap(),
    );
    let coordinator = DistributedCoordinator::new(4 << 20, 16).unwrap();
    let request = CypherQueryRequest::new(
        "RETURN 1",
        BTreeMap::new(),
        ValidTime::from_micros(5),
        TransactionTime::new(150, 0),
        security,
        now_ms().saturating_sub(1),
    );

    assert_eq!(
        block_on(engine.execute(&coordinator, request)),
        Err(EngineError::DeadlineExceeded)
    );
}

#[test]
fn executes_parameterized_temporal_cypher_on_nonzero_primary_shard() {
    let security = [9; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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

    let visible = request(150, security);
    let response = block_on(engine.execute(&coordinator, visible)).expect("visible query");
    assert_eq!(response.row_count(), 1);
    let RuntimeValue::Node(node) = &response.batches()[0].rows()[0][0] else {
        panic!("expected node");
    };
    assert_eq!(node.element().id(), ElementId::new(1));

    let historical = request(50, security);
    let response = block_on(engine.execute(&coordinator, historical)).expect("historical query");
    assert_eq!(response.row_count(), 0);
}

#[test]
fn executes_count_aggregate_after_temporal_scan() {
    let security = [8; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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
    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "USE accounts FOR VALID_TIME AS OF 5 MATCH (n) RETURN count(n)",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("count query");
    assert_eq!(response.row_count(), 1);
    assert_eq!(response.batches()[0].rows()[0][0], RuntimeValue::Integer(1));
}

#[test]
fn executes_valid_time_interval_by_returning_elements_intersecting_the_window() {
    let security = [5; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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
    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "USE accounts FOR VALID_TIME BETWEEN $from AND $to MATCH (n) RETURN n",
            BTreeMap::from([
                ("from".into(), RuntimeValue::TimestampMicros(0)),
                ("to".into(), RuntimeValue::TimestampMicros(5)),
            ]),
            ValidTime::from_micros(999),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("interval query");
    assert_eq!(response.row_count(), 1);
}

#[test]
fn interval_query_rejects_a_nonempty_transaction_overlay_instead_of_omitting_it() {
    let security = [4; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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
    let element = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(9));
    let mut overlay = GraphOverlay::new(4).expect("overlay");
    overlay
        .stage([GraphOverlayEntry::put(
            42,
            Interval::new(ValidTime::from_micros(2), Some(ValidTime::from_micros(4)))
                .expect("staged interval"),
            RuntimeValue::Node(VertexRecord::new(
                element,
                Some(LabelId::new(1)),
                CanonicalElement::new(3, BTreeMap::new()),
            )),
        )
        .expect("entry")])
        .expect("stage");

    let error = block_on(
        engine.execute(
            &coordinator,
            CypherQueryRequest::new(
                "USE accounts FOR VALID_TIME BETWEEN 1 AND 5 MATCH (n) RETURN n",
                BTreeMap::new(),
                ValidTime::from_micros(999),
                TransactionTime::new(150, 0),
                security,
                now_ms() + 10_000,
            )
            .with_graph_overlay(overlay),
        ),
    )
    .expect_err("interval overlay must not be silently omitted");

    assert_eq!(error, EngineError::IntervalOverlayUnsupported);
}

#[test]
fn fixed_transaction_rejects_an_explicit_transaction_time_override() {
    let security = [3; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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

    let error = block_on(
        engine.execute(
            &coordinator,
            CypherQueryRequest::new(
                "USE accounts FOR SYSTEM_TIME AS OF 200 MATCH (n) RETURN n",
                BTreeMap::new(),
                ValidTime::from_micros(5),
                TransactionTime::new(150, 0),
                security,
                now_ms() + 10_000,
            )
            .with_fixed_transaction_snapshot(TransactionTime::new(150, 0)),
        ),
    )
    .expect_err("fixed transaction snapshot must not be overridden");

    assert_eq!(error, EngineError::FixedSnapshotOverride);
}

#[test]
fn interval_query_returns_a_middle_segment_with_its_temporal_region() {
    let security = [13; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed_interval_only(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "USE accounts FOR VALID_TIME BETWEEN 0 AND 5 MATCH (n) RETURN n",
            BTreeMap::new(),
            ValidTime::from_micros(999),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("interval query");

    assert_eq!(response.row_count(), 1);
    let rows = response.temporal_rows().expect("temporal rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].region().valid().start(), ValidTime::from_micros(2));
    assert_eq!(
        rows[0].region().valid().end(),
        Some(ValidTime::from_micros(3))
    );
}

#[test]
fn interval_cypher_aggregate_returns_piecewise_temporal_counts() {
    let security = [27; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    seed_interval_only_at(&store, 2);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "USE accounts FOR VALID_TIME BETWEEN 0 AND 5 MATCH (n) RETURN count(n)",
            BTreeMap::new(),
            ValidTime::from_micros(999),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("interval aggregate query");

    let rows = response.temporal_rows().expect("temporal rows");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].values(), &[RuntimeValue::Integer(1)]);
    assert_eq!(
        rows[0].region().valid(),
        Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(2)))
            .expect("first interval")
    );
    assert_eq!(rows[1].values(), &[RuntimeValue::Integer(2)]);
    assert_eq!(
        rows[1].region().valid(),
        Interval::new(ValidTime::from_micros(2), Some(ValidTime::from_micros(3)))
            .expect("second interval")
    );
    assert_eq!(rows[2].values(), &[RuntimeValue::Integer(1)]);
    assert_eq!(
        rows[2].region().valid(),
        Interval::new(ValidTime::from_micros(3), Some(ValidTime::from_micros(5)))
            .expect("third interval")
    );
}

#[test]
fn executes_unwind_rows_through_the_distributed_runtime() {
    let security = [4; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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
    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "UNWIND [1, 2, 3] AS value RETURN value",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("UNWIND query");
    assert_eq!(response.row_count(), 3);
}

#[test]
fn executes_a_read_subquery_through_the_distributed_runtime() {
    let security = [21; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "CALL () { UNWIND [1, 2] AS value RETURN value } RETURN value",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("subquery query");
    let values = response
        .batches()
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![RuntimeValue::Integer(1), RuntimeValue::Integer(2)]
    );
}

#[test]
fn executes_a_correlated_read_subquery_through_the_distributed_runtime() {
    let security = [22; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "UNWIND [1, 2] AS value CALL (value) { RETURN value AS copy } RETURN copy",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("correlated subquery query");
    let values = response
        .batches()
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![RuntimeValue::Integer(1), RuntimeValue::Integer(2)]
    );
}

#[test]
fn executes_union_all_query_parts_through_the_physical_dag() {
    let security = [12; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "UNWIND [1] AS value RETURN value UNION ALL UNWIND [2] AS value RETURN value",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("UNION ALL query");
    let values = response
        .batches()
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    assert_eq!(
        values,
        vec![RuntimeValue::Integer(1), RuntimeValue::Integer(2)]
    );
}

#[test]
fn executes_union_query_parts_with_global_distinct() {
    let security = [13; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "UNWIND [1] AS value RETURN value UNION UNWIND [1] AS value RETURN value",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("UNION query");
    assert_eq!(response.row_count(), 1);
    assert_eq!(response.batches()[0].rows()[0][0], RuntimeValue::Integer(1));
}

#[test]
fn mixed_union_chain_preserves_each_boundary_multiplicity() {
    let security = [31; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "UNWIND [1, 1] AS value RETURN value \
             UNION ALL UNWIND [1] AS value RETURN value \
             UNION UNWIND [1, 2] AS value RETURN value \
             UNION ALL UNWIND [2] AS value RETURN value",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("mixed UNION chain");
    let values = response
        .batches()
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();

    assert_eq!(
        values,
        vec![
            RuntimeValue::Integer(1),
            RuntimeValue::Integer(2),
            RuntimeValue::Integer(2),
        ]
    );
}

#[test]
fn union_normalizes_internal_slots_and_matches_point_distinct_semantics() {
    let security = [39; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "RETURN 1 AS value UNION UNWIND [2] AS x RETURN x AS value",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("normalized UNION");

    assert_eq!(
        response
            .batches()
            .iter()
            .flat_map(|batch| batch.rows())
            .map(|row| row[0].clone())
            .collect::<Vec<_>>(),
        vec![RuntimeValue::Integer(1), RuntimeValue::Integer(2)]
    );
}

#[test]
fn composite_aggregate_expressions_execute_for_global_and_grouped_inputs() {
    let security = [40; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let global = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "UNWIND [1, 2] AS value WITH count(value) + 1 AS total RETURN total",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("global composite aggregate");
    assert_eq!(
        global.batches()[0].rows(),
        &[vec![RuntimeValue::Integer(3)]]
    );

    let grouped = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "UNWIND [1, 1, 2] AS value \
             WITH value, count(value) + 1 AS total RETURN value, total ORDER BY value",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("grouped composite aggregate");
    assert_eq!(
        grouped
            .batches()
            .iter()
            .flat_map(|batch| batch.rows())
            .cloned()
            .collect::<Vec<_>>(),
        vec![
            vec![RuntimeValue::Integer(1), RuntimeValue::Integer(3)],
            vec![RuntimeValue::Integer(2), RuntimeValue::Integer(2)],
        ]
    );
}

#[test]
fn with_distinct_grouping_filter_sort_skip_and_limit_execute_in_order() {
    let security = [32; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "UNWIND [4, 1, 1, 3, 2] AS value \
             WITH DISTINCT value AS grouped WHERE grouped > 0 \
             ORDER BY -grouped ASC SKIP 1 LIMIT 1 RETURN grouped",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("WITH pipeline");

    assert_eq!(response.row_count(), 1);
    assert_eq!(response.batches()[0].rows()[0][0], RuntimeValue::Integer(3));
}

#[test]
fn order_by_direction_uses_cypher_null_placement() {
    let security = [38; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    for (direction, expected) in [
        (
            "ASC",
            vec![
                RuntimeValue::Integer(1),
                RuntimeValue::Integer(2),
                RuntimeValue::Null,
            ],
        ),
        (
            "DESC",
            vec![
                RuntimeValue::Null,
                RuntimeValue::Integer(2),
                RuntimeValue::Integer(1),
            ],
        ),
    ] {
        let response = block_on(engine.execute(
            &coordinator,
            CypherQueryRequest::new(
                format!("UNWIND [2, null, 1] AS value RETURN value ORDER BY value {direction}"),
                BTreeMap::new(),
                ValidTime::from_micros(5),
                TransactionTime::new(150, 0),
                security,
                now_ms() + 10_000,
            ),
        ))
        .expect("ordered null pipeline");
        let values = response
            .batches()
            .iter()
            .flat_map(|batch| batch.rows())
            .map(|row| row[0].clone())
            .collect::<Vec<_>>();
        assert_eq!(values, expected, "{direction}");
    }
}

#[test]
fn grouped_with_aggregation_and_chained_unwind_execute_per_upstream_row() {
    let security = [33; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "UNWIND [[1, 1], [2]] AS values UNWIND values AS value \
             WITH value, count(value) AS occurrences RETURN value, occurrences ORDER BY value",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("grouped WITH and chained UNWIND");
    let rows = response
        .batches()
        .iter()
        .flat_map(|batch| batch.rows())
        .cloned()
        .collect::<Vec<_>>();

    assert_eq!(
        rows,
        vec![
            vec![RuntimeValue::Integer(1), RuntimeValue::Integer(2)],
            vec![RuntimeValue::Integer(2), RuntimeValue::Integer(1)],
        ]
    );
}

#[test]
fn unwind_empty_list_and_null_emit_no_rows() {
    let security = [34; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    for query in [
        "UNWIND [] AS value RETURN value",
        "UNWIND null AS value RETURN value",
    ] {
        let response = block_on(engine.execute(
            &coordinator,
            CypherQueryRequest::new(
                query,
                BTreeMap::new(),
                ValidTime::from_micros(5),
                TransactionTime::new(150, 0),
                security,
                now_ms() + 10_000,
            ),
        ))
        .expect("empty UNWIND input");
        assert_eq!(response.row_count(), 0, "{query}");
    }
}

#[test]
fn union_chain_inside_read_subquery_executes_with_boundary_semantics() {
    let security = [35; 32];
    let worker = LocalFragmentWorker::new(
        42,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(TemporalStore::new(MemoryAdapter::new())),
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "CALL () { RETURN 1 AS value UNION ALL RETURN 2 AS value UNION RETURN 2 AS value } \
             RETURN value",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("UNION subquery");
    let values = response
        .batches()
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();

    assert_eq!(
        values,
        vec![RuntimeValue::Integer(1), RuntimeValue::Integer(2)]
    );
}

#[test]
fn interval_union_deduplicates_identical_regions_from_two_branches() {
    let security = [15; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed_interval_only(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "USE accounts FOR VALID_TIME BETWEEN 0 AND 5 \
             MATCH (n) RETURN n UNION MATCH (m) RETURN m AS n",
            BTreeMap::new(),
            ValidTime::from_micros(999),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("interval union query");

    assert_eq!(response.row_count(), 1);
    assert_eq!(response.temporal_rows().expect("temporal rows").len(), 1);
}

#[test]
fn interval_union_all_preserves_branch_multiplicity() {
    let security = [16; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed_interval_only(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "USE accounts FOR VALID_TIME BETWEEN 0 AND 5 \
             MATCH (n) RETURN n UNION ALL MATCH (m) RETURN m AS n",
            BTreeMap::new(),
            ValidTime::from_micros(999),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("interval union all query");

    assert_eq!(response.row_count(), 2);
    assert_eq!(response.temporal_rows().expect("temporal rows").len(), 2);
}

#[test]
fn interval_union_with_and_unwind_preserve_regions_and_exact_multiplicity() {
    let security = [37; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed_interval_only(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "USE accounts FOR VALID_TIME BETWEEN 0 AND 5 \
             MATCH (n) WITH [n, n] AS nodes UNWIND nodes AS value RETURN value \
             UNION MATCH (m) WITH [m] AS nodes UNWIND nodes AS value RETURN value",
            BTreeMap::new(),
            ValidTime::from_micros(999),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("interval UNION/WITH/UNWIND pipeline");

    let rows = response.temporal_rows().expect("temporal rows");
    assert_eq!(rows.len(), 1);
    assert!(rows.iter().all(|row| {
        row.region().valid()
            == Interval::new(ValidTime::from_micros(2), Some(ValidTime::from_micros(3)))
                .expect("region")
    }));
}

#[test]
fn executes_with_alias_projection_before_return() {
    let security = [3; 32];
    let store = TemporalStore::new(MemoryAdapter::new());
    seed(&store);
    let worker =
        LocalFragmentWorker::new(42, 7, 3, 11, security, TemporalBatchExecutor::new(store));
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
    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "MATCH (n) WITH n AS m RETURN m",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("WITH query");
    assert_eq!(response.row_count(), 1);
}

#[test]
fn gathers_shared_nothing_shards_under_one_temporal_snapshot() {
    let security = [7; 32];
    let mut coordinator = DistributedCoordinator::new(4 << 20, 16).expect("coordinator");
    coordinator
        .register(Arc::new(worker(8, 2, security)))
        .expect("worker 8");
    coordinator
        .register(Arc::new(worker(3, 1, security)))
        .expect("worker 3");
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::SharedNothing,
            vec![8, 3],
            ResourceLimits::new(1 << 20, 4 << 20, 1).expect("limits"),
        )
        .expect("engine config"),
    );

    let response =
        block_on(engine.execute(&coordinator, request(150, security))).expect("distributed query");
    let ids = response
        .batches()
        .iter()
        .flat_map(|batch| batch.rows())
        .map(|row| match &row[0] {
            RuntimeValue::Node(node) => node.element().id(),
            value => panic!("expected node, got {value:?}"),
        })
        .collect::<Vec<_>>();
    assert_eq!(ids, vec![ElementId::new(1), ElementId::new(2)]);
}

#[test]
fn gathers_interval_rows_from_all_shards_without_losing_regions() {
    let security = [14; 32];
    let mut coordinator = DistributedCoordinator::new(4 << 20, 16).expect("coordinator");
    coordinator
        .register(Arc::new(interval_worker(8, 2, security)))
        .expect("worker 8");
    coordinator
        .register(Arc::new(interval_worker(3, 1, security)))
        .expect("worker 3");
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::SharedNothing,
            vec![8, 3],
            ResourceLimits::new(1 << 20, 4 << 20, 256).expect("limits"),
        )
        .expect("engine config"),
    );

    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "USE accounts FOR VALID_TIME BETWEEN 0 AND 5 MATCH (n) RETURN n",
            BTreeMap::new(),
            ValidTime::from_micros(999),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("shared interval query");

    assert_eq!(response.row_count(), 2);
    let rows = response.temporal_rows().expect("temporal rows");
    assert_eq!(rows.len(), 2);
    assert!(rows.iter().all(|row| {
        row.region().valid()
            == Interval::new(ValidTime::from_micros(2), Some(ValidTime::from_micros(3)))
                .expect("region")
    }));
}

#[test]
fn shared_nothing_aggregates_after_global_gather() {
    let security = [6; 32];
    let mut coordinator = DistributedCoordinator::new(4 << 20, 16).expect("coordinator");
    coordinator
        .register(Arc::new(worker(8, 2, security)))
        .expect("worker 8");
    coordinator
        .register(Arc::new(worker(3, 1, security)))
        .expect("worker 3");
    let engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::SharedNothing,
            vec![8, 3],
            ResourceLimits::new(1 << 20, 4 << 20, 256).expect("limits"),
        )
        .expect("engine config"),
    );
    let response = block_on(engine.execute(
        &coordinator,
        CypherQueryRequest::new(
            "MATCH (n) RETURN count(n)",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        ),
    ))
    .expect("distributed count query");
    assert_eq!(response.row_count(), 1);
    assert_eq!(response.batches()[0].rows()[0][0], RuntimeValue::Integer(2));
}

#[test]
fn graph_source_with_pipeline_is_canonical_across_deployment_modes() {
    let security = [36; 32];
    let mut primary_coordinator =
        DistributedCoordinator::new(4 << 20, 16).expect("primary coordinator");
    primary_coordinator
        .register(Arc::new(worker_with_elements(42, &[1, 2], security)))
        .expect("primary worker");
    let primary_engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::PrimaryReplica,
            vec![42],
            ResourceLimits::new(1 << 20, 4 << 20, 256).expect("limits"),
        )
        .expect("primary engine"),
    );

    let mut shared_coordinator =
        DistributedCoordinator::new(4 << 20, 16).expect("shared coordinator");
    shared_coordinator
        .register(Arc::new(worker(8, 1, security)))
        .expect("worker 8");
    shared_coordinator
        .register(Arc::new(worker(3, 2, security)))
        .expect("worker 3");
    let shared_engine = CypherQueryEngine::new(
        EngineConfig::new(
            "accounts",
            7,
            3,
            11,
            DeploymentMode::SharedNothing,
            vec![8, 3],
            ResourceLimits::new(1 << 20, 4 << 20, 256).expect("limits"),
        )
        .expect("shared engine"),
    );
    let request = || {
        CypherQueryRequest::new(
            "MATCH (n) WITH n AS item RETURN count(item)",
            BTreeMap::new(),
            ValidTime::from_micros(5),
            TransactionTime::new(150, 0),
            security,
            now_ms() + 10_000,
        )
    };

    let primary = block_on(primary_engine.execute(&primary_coordinator, request()))
        .expect("primary pipeline");
    let shared =
        block_on(shared_engine.execute(&shared_coordinator, request())).expect("shared pipeline");

    assert_eq!(primary.schema(), shared.schema());
    assert_eq!(primary.batches(), shared.batches());
}

fn request(transaction_micros: i64, security: [u8; 32]) -> CypherQueryRequest {
    CypherQueryRequest::new(
        "USE accounts FOR VALID_TIME AS OF $valid \
         FOR SYSTEM_TIME AS OF $tx MATCH (n) RETURN n",
        BTreeMap::from([
            ("valid".into(), RuntimeValue::TimestampMicros(5)),
            (
                "tx".into(),
                RuntimeValue::TimestampMicros(transaction_micros),
            ),
        ]),
        ValidTime::from_micros(999),
        TransactionTime::new(999, 0),
        security,
        now_ms() + 10_000,
    )
}

fn seed(store: &TemporalStore<MemoryAdapter>) {
    let element = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(1));
    block_on(
        store.commit_vertex(
            CommitContext::new(
                42,
                1,
                1,
                TransactionTime::new(0, 0),
                TransactionTime::new(100, 0),
            ),
            VertexMutation::put(
                element,
                LabelId::new(1),
                Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                CanonicalElement::new(3, BTreeMap::from([(1, GraphValue::String("alice".into()))])),
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
}

fn seed_interval_only(store: &TemporalStore<MemoryAdapter>) {
    seed_interval_only_at(store, 1);
}

fn seed_interval_only_at(store: &TemporalStore<MemoryAdapter>, log_index: u64) {
    let element = ElementRef::vertex(GraphId::new(7), PartitionId::new(0), ElementId::new(2));
    block_on(
        store.commit_vertex(
            CommitContext::new(
                42,
                log_index,
                2,
                TransactionTime::new(0, 0),
                TransactionTime::new(100, 0),
            ),
            VertexMutation::put(
                element,
                LabelId::new(1),
                Interval::new(ValidTime::from_micros(2), Some(ValidTime::from_micros(3)))
                    .expect("interval"),
                CanonicalElement::new(3, BTreeMap::new()),
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
}

fn worker(
    shard_id: u32,
    element_id: u128,
    security: [u8; 32],
) -> LocalFragmentWorker<MemoryAdapter> {
    worker_with_elements(shard_id, &[element_id], security)
}

fn worker_with_elements(
    shard_id: u32,
    element_ids: &[u128],
    security: [u8; 32],
) -> LocalFragmentWorker<MemoryAdapter> {
    let store = TemporalStore::new(MemoryAdapter::new());
    for (index, element_id) in element_ids.iter().copied().enumerate() {
        let element = ElementRef::vertex(
            GraphId::new(7),
            PartitionId::new(shard_id),
            ElementId::new(element_id),
        );
        block_on(
            store.commit_vertex(
                CommitContext::new(
                    shard_id,
                    u64::try_from(index).expect("log index") + 1,
                    element_id,
                    TransactionTime::new(0, 0),
                    TransactionTime::new(100, 0),
                ),
                VertexMutation::put(
                    element,
                    LabelId::new(1),
                    Interval::new(ValidTime::from_micros(1), None).expect("interval"),
                    CanonicalElement::new(
                        3,
                        BTreeMap::from([(1, GraphValue::Integer(element_id as i64))]),
                    ),
                )
                .expect("mutation"),
            ),
        )
        .expect("commit");
    }
    LocalFragmentWorker::new(
        shard_id,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(store),
    )
}

fn interval_worker(
    shard_id: u32,
    element_id: u128,
    security: [u8; 32],
) -> LocalFragmentWorker<MemoryAdapter> {
    let store = TemporalStore::new(MemoryAdapter::new());
    let element = ElementRef::vertex(
        GraphId::new(7),
        PartitionId::new(shard_id),
        ElementId::new(element_id),
    );
    block_on(
        store.commit_vertex(
            CommitContext::new(
                shard_id,
                1,
                element_id,
                TransactionTime::new(0, 0),
                TransactionTime::new(100, 0),
            ),
            VertexMutation::put(
                element,
                LabelId::new(1),
                Interval::new(ValidTime::from_micros(2), Some(ValidTime::from_micros(3)))
                    .expect("interval"),
                CanonicalElement::new(3, BTreeMap::new()),
            )
            .expect("mutation"),
        ),
    )
    .expect("commit");
    LocalFragmentWorker::new(
        shard_id,
        7,
        3,
        11,
        security,
        TemporalBatchExecutor::new(store),
    )
}

fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis(),
    )
    .expect("time")
}

fn block_on<F: Future>(future: F) -> F::Output {
    struct Noop;
    impl Wake for Noop {
        fn wake(self: Arc<Self>) {}
    }
    let waker = Waker::from(Arc::new(Noop));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
