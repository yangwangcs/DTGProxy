use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use analytics_api::{ProjectedGraph, SnapshotGraph};
use analytics_ledger::{GraphProjectionScope, ProjectionLimits};
use physical_plan::{
    MemoryBudget, PhysicalOperator, PhysicalPlanBuilder, PhysicalPlanHeader, Placement,
};
use procedure_runtime::{
    JobInvocationContext, ProcedureAccess, ProcedureCatalog, ProcedureDefinition, ProcedureEffect,
    ProcedureError, ProcedureField, ProcedureInvocation, ProcedureLimits, ProcedureOutput,
    ProcedurePermission, ProcedurePlacement, ProcedureProvider, ProcedureRegistry, ProcedureValue,
};
use query_executor::{
    BatchExecutor, CancellationToken, ExecutionContext, RecordBatch, RuntimeError, RuntimeValue,
    TemporalRegion, TemporalRow, execute_interval_coordinator_operators,
    preflight_procedure_parameters,
};
use temporal_ir::{
    Column, ProcedureArgument, ProcedureYieldBinding, ResolvedProcedure, RowSchema, ScalarExpr,
    SlotId, ValueType,
};
use temporal_types::{Interval, TransactionTime, ValidTime};

struct EchoProvider {
    calls: Arc<AtomicUsize>,
    source_free_rows: Vec<Vec<ProcedureValue>>,
}

impl ProcedureProvider for EchoProvider {
    fn invoke(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let rows = invocation.arguments().get("input").map_or_else(
            || self.source_free_rows.clone(),
            |value| vec![vec![value.clone()]],
        );
        output.declare_columns(vec!["value".into()])?;
        for row in rows {
            output.push_row(row)?;
        }
        Ok(())
    }
}

fn runtime(
    inputs: Vec<ProcedureField>,
    limits: ProcedureLimits,
    rows: Vec<Vec<ProcedureValue>>,
) -> (Arc<ProcedureRegistry>, ResolvedProcedure, Arc<AtomicUsize>) {
    let catalog = ProcedureCatalog::from_definitions(vec![
        ProcedureDefinition::new(
            "dtg.test.echo",
            inputs,
            vec![ProcedureField::new("value", ValueType::Integer, false)],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        )
        .with_limits(limits),
    ])
    .unwrap();
    let descriptor = catalog.resolve("dtg.test.echo").unwrap().clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(
            *descriptor.identity(),
            Arc::new(EchoProvider {
                calls: Arc::clone(&calls),
                source_free_rows: rows,
            }),
        )
        .unwrap();
    let limits = descriptor.limits();
    let resolved = ResolvedProcedure::new(
        *descriptor.identity(),
        descriptor.name(),
        if descriptor.inputs().is_empty() {
            Vec::new()
        } else {
            vec![ProcedureArgument::new(
                "input",
                ScalarExpr::Slot(SlotId::new(0)),
            )]
        },
        vec![ProcedureYieldBinding::new(
            0,
            if descriptor.inputs().is_empty() {
                SlotId::new(0)
            } else {
                SlotId::new(1)
            },
        )],
        descriptor.output().clone(),
        descriptor.effect(),
        descriptor.placement(),
        limits.max_invocations(),
        limits.max_input_rows(),
        limits.max_output_rows(),
        limits.max_value_bytes(),
        limits.max_result_bytes(),
        descriptor.supports_overlay(),
    );
    (Arc::new(registry), resolved, calls)
}

fn context(registry: Arc<ProcedureRegistry>) -> ExecutionContext {
    ExecutionContext::default().with_procedure_runtime(
        registry,
        Some(Arc::new(ProjectedGraph::Snapshot(
            SnapshotGraph::new(Vec::new(), Vec::new(), true).unwrap(),
        ))),
        [7; 32],
        ProcedureAccess::analytics_read(),
    )
}

struct JobContextProvider {
    captured: Arc<Mutex<Option<JobInvocationContext>>>,
}

impl ProcedureProvider for JobContextProvider {
    fn invoke(
        &self,
        invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        *self.captured.lock().unwrap() = invocation.job_context().cloned();
        output.declare_columns(vec!["value".into()])?;
        output.push_row(vec![ProcedureValue::Integer(1)])
    }
}

#[tokio::test]
async fn execution_context_forwards_immutable_job_fences_to_procedure_invocation() {
    let catalog = ProcedureCatalog::from_definitions(vec![ProcedureDefinition::new(
        "dtg.test.jobContext",
        Vec::new(),
        vec![ProcedureField::new("value", ValueType::Integer, false)],
        ProcedureEffect::ReadOnly,
        ProcedurePermission::AnalyticsRead,
        ProcedurePlacement::Coordinator,
    )])
    .unwrap();
    let descriptor = catalog.resolve("dtg.test.jobContext").unwrap().clone();
    let captured = Arc::new(Mutex::new(None));
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(
            *descriptor.identity(),
            Arc::new(JobContextProvider {
                captured: Arc::clone(&captured),
            }),
        )
        .unwrap();
    let limits = descriptor.limits();
    let procedure = ResolvedProcedure::new(
        *descriptor.identity(),
        descriptor.name(),
        Vec::new(),
        vec![ProcedureYieldBinding::new(0, SlotId::new(0))],
        descriptor.output().clone(),
        descriptor.effect(),
        descriptor.placement(),
        limits.max_invocations(),
        limits.max_input_rows(),
        limits.max_output_rows(),
        limits.max_value_bytes(),
        limits.max_result_bytes(),
        descriptor.supports_overlay(),
    );
    let output = descriptor.output().clone();
    let plan = fragment(procedure, output, 1 << 20);
    let job_context = JobInvocationContext::new(
        301,
        u64::MAX,
        7,
        11,
        13,
        17,
        19,
        TransactionTime::new(23, 29),
        GraphProjectionScope::Snapshot {
            valid_time: ValidTime::from_micros(31),
        },
        ProjectionLimits::new(37, 41, 43).unwrap(),
    )
    .unwrap();
    let execution = context(Arc::new(registry)).with_job_invocation_context(job_context.clone());

    BatchExecutor::new()
        .execute_fragment(&plan.fragments()[0], &execution, Vec::new())
        .await
        .unwrap();

    assert_eq!(*captured.lock().unwrap(), Some(job_context));
}

fn fragment(
    procedure: ResolvedProcedure,
    output: RowSchema,
    memory: u64,
) -> physical_plan::PhysicalPlan {
    let mut builder = PhysicalPlanBuilder::new(PhysicalPlanHeader::new(7, 3, 11, [9; 32]).unwrap());
    let operators = if procedure.arguments().is_empty() {
        vec![
            PhysicalOperator::Argument {
                output: RowSchema::empty(),
            },
            PhysicalOperator::Procedure {
                procedure,
                output: output.clone(),
            },
        ]
    } else {
        let input_width = output
            .columns()
            .len()
            .checked_sub(procedure.yields().len())
            .expect("procedure output includes the preserved input schema");
        let input = RowSchema::new(output.columns()[..input_width].to_vec()).unwrap();
        vec![
            PhysicalOperator::Project {
                expressions: input
                    .columns()
                    .iter()
                    .map(|column| (column.slot(), ScalarExpr::Slot(column.slot())))
                    .collect(),
                output: input,
            },
            PhysicalOperator::Procedure {
                procedure,
                output: output.clone(),
            },
        ]
    };
    let root = builder
        .add_fragment(
            Placement::Coordinator,
            operators,
            output,
            MemoryBudget::new(memory, memory).unwrap(),
        )
        .unwrap();
    builder.finish(root).unwrap()
}

#[tokio::test]
async fn procedure_call_executes_once_per_input_row_preserves_bindings_and_limits_rows() {
    let limits = ProcedureLimits::new(10, 10, 10, 1024, 4096).unwrap();
    let (registry, procedure, calls) = runtime(
        Vec::new(),
        limits,
        vec![
            vec![ProcedureValue::Integer(1)],
            vec![ProcedureValue::Integer(2)],
        ],
    );
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let plan = fragment(procedure, output.clone(), 1 << 20);
    let batches = BatchExecutor::new()
        .execute_fragment(&plan.fragments()[0], &context(registry), Vec::new())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(batches[0].rows().len(), 2);

    let (registry, procedure, calls) = runtime(
        vec![ProcedureField::new("input", ValueType::Integer, false)],
        limits,
        Vec::new(),
    );
    let input_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "input",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "input", ValueType::Integer, false),
        Column::new(SlotId::new(1), "value", ValueType::Integer, false),
    ])
    .unwrap();
    let plan = fragment(procedure, output, 1 << 20);
    let input = RecordBatch::try_new(
        input_schema,
        vec![
            vec![RuntimeValue::Integer(4)],
            vec![RuntimeValue::Integer(7)],
        ],
    )
    .unwrap();
    let batches = BatchExecutor::new()
        .execute_fragment(&plan.fragments()[0], &context(registry), vec![input])
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        batches[0].rows(),
        &[
            vec![RuntimeValue::Integer(4), RuntimeValue::Integer(4)],
            vec![RuntimeValue::Integer(7), RuntimeValue::Integer(7)],
        ]
    );
}

#[tokio::test]
async fn interval_procedure_call_preserves_temporal_region_for_every_provider_row() {
    let limits = ProcedureLimits::new(10, 10, 10, 1024, 4096).unwrap();
    let (registry, procedure, calls) = runtime(
        Vec::new(),
        limits,
        vec![
            vec![ProcedureValue::Integer(1)],
            vec![ProcedureValue::Integer(2)],
        ],
    );
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let plan = fragment(procedure, output, 1 << 20);
    let region = TemporalRegion::new(
        Interval::new(
            ValidTime::from_micros(1_000),
            Some(ValidTime::from_micros(2_000)),
        )
        .unwrap(),
        Interval::new(
            TransactionTime::new(10, 0),
            Some(TransactionTime::new(10, 1)),
        )
        .unwrap(),
    );

    let rows = execute_interval_coordinator_operators(
        &plan.fragments()[0],
        RowSchema::empty(),
        vec![TemporalRow::new(Vec::new(), region)],
        &context(registry),
    )
    .await
    .expect("interval procedure");

    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        rows.iter().map(TemporalRow::values).collect::<Vec<_>>(),
        vec![
            &[RuntimeValue::Integer(1)][..],
            &[RuntimeValue::Integer(2)][..]
        ]
    );
    assert!(rows.iter().all(|row| row.region() == region));
}

#[tokio::test]
async fn procedure_error_cancel_deadline_invocation_and_memory_abort_without_partial_rows() {
    let limits = ProcedureLimits::new(1, 10, 10, 1024, 4096).unwrap();
    let (registry, procedure, calls) = runtime(
        vec![ProcedureField::new("input", ValueType::Integer, false)],
        limits,
        Vec::new(),
    );
    let input_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "input",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "input", ValueType::Integer, false),
        Column::new(SlotId::new(1), "value", ValueType::Integer, false),
    ])
    .unwrap();
    let plan = fragment(procedure, output, 1 << 20);
    let input = RecordBatch::try_new(
        input_schema,
        vec![
            vec![RuntimeValue::Integer(1)],
            vec![RuntimeValue::Integer(2)],
        ],
    )
    .unwrap();
    assert_eq!(
        BatchExecutor::new()
            .execute_fragment(
                &plan.fragments()[0],
                &context(Arc::clone(&registry)),
                vec![input.clone()],
            )
            .await,
        Err(RuntimeError::ProcedureInvocationLimit { max: 1 })
    );

    let cancellation = CancellationToken::new();
    cancellation.cancel();
    let before = calls.load(Ordering::SeqCst);
    assert_eq!(
        BatchExecutor::new()
            .execute_fragment(
                &plan.fragments()[0],
                &context(Arc::clone(&registry)).with_cancellation(cancellation),
                vec![input.clone()],
            )
            .await,
        Err(RuntimeError::Cancelled)
    );
    assert_eq!(calls.load(Ordering::SeqCst), before);
    assert_eq!(
        BatchExecutor::new()
            .execute_fragment(
                &plan.fragments()[0],
                &context(registry).with_deadline(Instant::now() - Duration::from_millis(1)),
                vec![input],
            )
            .await,
        Err(RuntimeError::DeadlineExceeded)
    );

    let (registry, procedure, _) = runtime(
        Vec::new(),
        ProcedureLimits::new(10, 10, 10, 1024, 4096).unwrap(),
        vec![vec![ProcedureValue::Integer(1)]],
    );
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let plan = fragment(procedure, output, 1);
    assert!(matches!(
        BatchExecutor::new()
            .execute_fragment(&plan.fragments()[0], &context(registry), Vec::new(),)
            .await,
        Err(RuntimeError::MemoryLimitExceeded { .. })
    ));
}

#[tokio::test]
async fn procedure_input_row_limit_is_independent_and_fails_before_provider_invocation() {
    let limits = ProcedureLimits::new(10, 1, 10, 1024, 4096).unwrap();
    let (registry, procedure, calls) = runtime(
        vec![ProcedureField::new("input", ValueType::Integer, false)],
        limits,
        Vec::new(),
    );
    let input_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "input",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let output = RowSchema::new(vec![
        Column::new(SlotId::new(0), "input", ValueType::Integer, false),
        Column::new(SlotId::new(1), "value", ValueType::Integer, false),
    ])
    .unwrap();
    let plan = fragment(procedure, output, 1 << 20);
    let input = RecordBatch::try_new(
        input_schema,
        vec![
            vec![RuntimeValue::Integer(1)],
            vec![RuntimeValue::Integer(2)],
        ],
    )
    .unwrap();

    assert_eq!(
        BatchExecutor::new()
            .execute_fragment(&plan.fragments()[0], &context(registry), vec![input],)
            .await,
        Err(RuntimeError::ProcedureInputRowLimit { max: 1 })
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn procedure_without_yield_preserves_provider_row_cardinality() {
    let limits = ProcedureLimits::new(10, 10, 10, 1024, 4096).unwrap();
    for (provider_rows, expected_rows) in [
        (
            vec![
                vec![ProcedureValue::Integer(1)],
                vec![ProcedureValue::Integer(2)],
            ],
            2,
        ),
        (Vec::new(), 0),
    ] {
        let (registry, procedure, calls) = runtime(Vec::new(), limits, provider_rows);
        let procedure = ResolvedProcedure::new(
            *procedure.identity(),
            procedure.name(),
            Vec::new(),
            Vec::new(),
            procedure.provider_output().clone(),
            procedure.effect(),
            procedure.placement(),
            procedure.max_invocations(),
            procedure.max_input_rows(),
            procedure.max_output_rows(),
            procedure.max_value_bytes(),
            procedure.max_result_bytes(),
            procedure.supports_overlay(),
        );
        let plan = fragment(procedure, RowSchema::empty(), 1 << 20);
        let batches = BatchExecutor::new()
            .execute_fragment(&plan.fragments()[0], &context(registry), Vec::new())
            .await
            .unwrap();
        let actual_rows = batches
            .iter()
            .map(|batch| batch.rows().len())
            .sum::<usize>();

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(actual_rows, expected_rows);
    }
}

struct BlockingProvider {
    started: tokio::sync::mpsc::UnboundedSender<()>,
    released: Arc<(Mutex<bool>, Condvar)>,
}

struct FanoutProvider {
    calls: Arc<AtomicUsize>,
}

impl ProcedureProvider for FanoutProvider {
    fn invoke(
        &self,
        _invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        output.declare_columns(vec!["value".into()])?;
        for value in 0..10 {
            output.push_row(vec![ProcedureValue::Integer(value)])?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn composed_procedure_rows_charge_memory_before_retention_and_next_invocation() {
    let catalog = ProcedureCatalog::from_definitions(vec![ProcedureDefinition::new(
        "dtg.test.fanout",
        vec![ProcedureField::new("input", ValueType::Integer, false)],
        vec![ProcedureField::new("value", ValueType::Integer, false)],
        ProcedureEffect::ReadOnly,
        ProcedurePermission::AnalyticsRead,
        ProcedurePlacement::Coordinator,
    )])
    .unwrap();
    let descriptor = catalog.resolve("dtg.test.fanout").unwrap().clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(
            *descriptor.identity(),
            Arc::new(FanoutProvider {
                calls: Arc::clone(&calls),
            }),
        )
        .unwrap();
    let limits = descriptor.limits();
    let procedure = ResolvedProcedure::new(
        *descriptor.identity(),
        descriptor.name(),
        vec![ProcedureArgument::new(
            "input",
            ScalarExpr::Slot(SlotId::new(0)),
        )],
        vec![ProcedureYieldBinding::new(0, SlotId::new(1))],
        descriptor.output().clone(),
        descriptor.effect(),
        descriptor.placement(),
        limits.max_invocations(),
        limits.max_input_rows(),
        limits.max_output_rows(),
        limits.max_value_bytes(),
        limits.max_result_bytes(),
        descriptor.supports_overlay(),
    );
    let input_schema = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "input",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let output_schema = RowSchema::new(vec![
        Column::new(SlotId::new(0), "input", ValueType::Integer, false),
        Column::new(SlotId::new(1), "value", ValueType::Integer, false),
    ])
    .unwrap();
    let plan = fragment(procedure, output_schema, 100);
    let input = RecordBatch::try_new(
        input_schema,
        vec![
            vec![RuntimeValue::Integer(1)],
            vec![RuntimeValue::Integer(2)],
        ],
    )
    .unwrap();

    assert!(matches!(
        BatchExecutor::new()
            .execute_fragment(
                &plan.fragments()[0],
                &context(Arc::new(registry)),
                vec![input]
            )
            .await,
        Err(RuntimeError::MemoryLimitExceeded { limit: 100, .. })
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

struct OversizedBytesProvider {
    calls: Arc<AtomicUsize>,
}

impl ProcedureProvider for OversizedBytesProvider {
    fn invoke(
        &self,
        _invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        output.declare_columns(vec!["value".into()])?;
        output.push_row(vec![ProcedureValue::Bytes(vec![0; 1024])])
    }
}

#[tokio::test]
async fn single_oversized_composed_row_is_rejected_before_owned_retention() {
    let catalog = ProcedureCatalog::from_definitions(vec![
        ProcedureDefinition::new(
            "dtg.test.oversizedBytes",
            Vec::new(),
            vec![ProcedureField::new("value", ValueType::Bytes, false)],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        )
        .with_limits(ProcedureLimits::new(1, 1, 1, 2048, 4096).unwrap()),
    ])
    .unwrap();
    let descriptor = catalog.resolve("dtg.test.oversizedBytes").unwrap().clone();
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(
            *descriptor.identity(),
            Arc::new(OversizedBytesProvider {
                calls: Arc::clone(&calls),
            }),
        )
        .unwrap();
    let limits = descriptor.limits();
    let procedure = ResolvedProcedure::new(
        *descriptor.identity(),
        descriptor.name(),
        Vec::new(),
        vec![ProcedureYieldBinding::new(0, SlotId::new(0))],
        descriptor.output().clone(),
        descriptor.effect(),
        descriptor.placement(),
        limits.max_invocations(),
        limits.max_input_rows(),
        limits.max_output_rows(),
        limits.max_value_bytes(),
        limits.max_result_bytes(),
        descriptor.supports_overlay(),
    );

    assert_eq!(
        BatchExecutor::new()
            .execute_fragment(
                &fragment(procedure, descriptor.output().clone(), 32).fragments()[0],
                &context(Arc::new(registry)),
                Vec::new(),
            )
            .await,
        Err(RuntimeError::MemoryLimitExceeded {
            limit: 32,
            required: 1029,
        })
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

impl ProcedureProvider for BlockingProvider {
    fn invoke(
        &self,
        _invocation: &ProcedureInvocation,
        output: &mut ProcedureOutput,
    ) -> Result<(), ProcedureError> {
        self.started.send(()).unwrap();
        let (released, changed) = &*self.released;
        let mut released = released.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
        output.declare_columns(vec!["value".into()])?;
        output.push_row(vec![ProcedureValue::Integer(1)])
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_provider_cancellation_returns_without_waiting_for_blocking_job_exit() {
    let catalog = ProcedureCatalog::from_definitions(vec![ProcedureDefinition::new(
        "dtg.test.blocking",
        Vec::new(),
        vec![ProcedureField::new("value", ValueType::Integer, false)],
        ProcedureEffect::ReadOnly,
        ProcedurePermission::AnalyticsRead,
        ProcedurePlacement::Coordinator,
    )])
    .unwrap();
    let descriptor = catalog.resolve("dtg.test.blocking").unwrap().clone();
    let (started_tx, mut started_rx) = tokio::sync::mpsc::unbounded_channel();
    let released = Arc::new((Mutex::new(false), Condvar::new()));
    let mut registry = ProcedureRegistry::new(catalog);
    registry
        .register(
            *descriptor.identity(),
            Arc::new(BlockingProvider {
                started: started_tx,
                released: Arc::clone(&released),
            }),
        )
        .unwrap();
    let limits = descriptor.limits();
    let procedure = ResolvedProcedure::new(
        *descriptor.identity(),
        descriptor.name(),
        Vec::new(),
        vec![ProcedureYieldBinding::new(0, SlotId::new(0))],
        descriptor.output().clone(),
        descriptor.effect(),
        descriptor.placement(),
        limits.max_invocations(),
        limits.max_input_rows(),
        limits.max_output_rows(),
        limits.max_value_bytes(),
        limits.max_result_bytes(),
        descriptor.supports_overlay(),
    );
    let output = RowSchema::new(vec![Column::new(
        SlotId::new(0),
        "value",
        ValueType::Integer,
        false,
    )])
    .unwrap();
    let plan = fragment(procedure, output, 1 << 20);
    let cancellation = CancellationToken::new();
    let execution = tokio::spawn({
        let cancellation = cancellation.clone();
        let registry = Arc::new(registry);
        async move {
            BatchExecutor::new()
                .execute_fragment(
                    &plan.fragments()[0],
                    &context(registry).with_cancellation(cancellation),
                    Vec::new(),
                )
                .await
        }
    });
    started_rx.recv().await.unwrap();
    cancellation.cancel();

    let result = tokio::time::timeout(Duration::from_millis(250), execution)
        .await
        .expect("cancellation must not wait for the blocking provider")
        .unwrap();
    assert_eq!(result, Err(RuntimeError::Cancelled));

    let (flag, changed) = &*released;
    *flag.lock().unwrap() = true;
    changed.notify_all();
}

#[test]
fn static_procedure_parameters_preflight_before_graph_projection() {
    let limits = ProcedureLimits::new(10, 10, 10, 8, 64).unwrap();
    let (registry, procedure, _) = runtime(
        vec![ProcedureField::new("input", ValueType::String, false)],
        limits,
        Vec::new(),
    );
    let procedure = ResolvedProcedure::new(
        *procedure.identity(),
        procedure.name(),
        vec![ProcedureArgument::new(
            "input",
            ScalarExpr::Parameter("input".into()),
        )],
        procedure.yields().to_vec(),
        procedure.provider_output().clone(),
        procedure.effect(),
        procedure.placement(),
        procedure.max_invocations(),
        procedure.max_input_rows(),
        procedure.max_output_rows(),
        procedure.max_value_bytes(),
        procedure.max_result_bytes(),
        procedure.supports_overlay(),
    );

    assert_eq!(
        preflight_procedure_parameters(
            [&procedure],
            &registry,
            &BTreeMap::new(),
            [7; 32],
            &ProcedureAccess::analytics_read(),
        ),
        Err(RuntimeError::MissingParameter("input".into()))
    );
    assert_eq!(
        preflight_procedure_parameters(
            [&procedure],
            &registry,
            &BTreeMap::from([("input".into(), RuntimeValue::String("too-large".into()))]),
            [7; 32],
            &ProcedureAccess::analytics_read(),
        ),
        Err(RuntimeError::ProcedureFailed(
            "DTG-PROCEDURE-VALUE-BYTES".into()
        ))
    );

    let catalog = ProcedureCatalog::from_definitions(vec![
        ProcedureDefinition::new(
            "dtg.test.mixedPreflight",
            vec![
                ProcedureField::new("row", ValueType::Integer, false),
                ProcedureField::new("static", ValueType::String, false),
            ],
            vec![ProcedureField::new("value", ValueType::Integer, false)],
            ProcedureEffect::ReadOnly,
            ProcedurePermission::AnalyticsRead,
            ProcedurePlacement::Coordinator,
        )
        .with_limits(ProcedureLimits::new(10, 10, 10, 8, 64).unwrap()),
    ])
    .unwrap();
    let descriptor = catalog.resolve("dtg.test.mixedPreflight").unwrap().clone();
    let mixed = ResolvedProcedure::new(
        *descriptor.identity(),
        descriptor.name(),
        vec![
            ProcedureArgument::new("row", ScalarExpr::Slot(SlotId::new(0))),
            ProcedureArgument::new("static", ScalarExpr::Parameter("static".into())),
        ],
        Vec::new(),
        descriptor.output().clone(),
        descriptor.effect(),
        descriptor.placement(),
        descriptor.limits().max_invocations(),
        descriptor.limits().max_input_rows(),
        descriptor.limits().max_output_rows(),
        descriptor.limits().max_value_bytes(),
        descriptor.limits().max_result_bytes(),
        descriptor.supports_overlay(),
    );
    let registry = ProcedureRegistry::new(catalog);

    assert_eq!(
        preflight_procedure_parameters(
            [&mixed],
            &registry,
            &BTreeMap::from([("static".into(), RuntimeValue::String("too-large".into()))]),
            [7; 32],
            &ProcedureAccess::analytics_read(),
        ),
        Err(RuntimeError::ProcedureFailed(
            "DTG-PROCEDURE-VALUE-BYTES".into()
        ))
    );
    assert_eq!(
        preflight_procedure_parameters(
            [&mixed],
            &registry,
            &BTreeMap::new(),
            [7; 32],
            &ProcedureAccess::analytics_read(),
        ),
        Err(RuntimeError::MissingParameter("static".into()))
    );

    let mixed_expression = ResolvedProcedure::new(
        *descriptor.identity(),
        descriptor.name(),
        vec![ProcedureArgument::new(
            "row",
            ScalarExpr::Add(
                Box::new(ScalarExpr::Slot(SlotId::new(0))),
                Box::new(ScalarExpr::Parameter("static".into())),
            ),
        )],
        Vec::new(),
        descriptor.output().clone(),
        descriptor.effect(),
        descriptor.placement(),
        descriptor.limits().max_invocations(),
        descriptor.limits().max_input_rows(),
        descriptor.limits().max_output_rows(),
        descriptor.limits().max_value_bytes(),
        descriptor.limits().max_result_bytes(),
        descriptor.supports_overlay(),
    );
    assert_eq!(
        preflight_procedure_parameters(
            [&mixed_expression],
            &registry,
            &BTreeMap::new(),
            [7; 32],
            &ProcedureAccess::analytics_read(),
        ),
        Err(RuntimeError::MissingParameter("static".into()))
    );
    assert_eq!(
        preflight_procedure_parameters(
            [&mixed_expression],
            &registry,
            &BTreeMap::from([("static".into(), RuntimeValue::String("too-large".into()))]),
            [7; 32],
            &ProcedureAccess::analytics_read(),
        ),
        Err(RuntimeError::ProcedureFailed(
            "DTG-PROCEDURE-VALUE-BYTES".into()
        ))
    );
}
