use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use physical_plan::{
    AggregatePhase, PhysicalApply, PhysicalOperator, PhysicalPlan, Placement, PlanFragment,
};
use procedure_runtime::{
    ProcedureAccess, ProcedureInvocation, ProcedureRegistry, ProcedureResult, ProcedureValue,
};
use temporal_ir::{
    ApplyKind, LogicalOperator, ProcedureYieldBinding, ResolvedProcedure, RowSchema, ScalarExpr,
    SortKey,
};

use super::expression;
use super::temporal::batches_from_rows;
use super::{
    ApplyBudgetLedger, ChildInvocationLimits, ChildOutputDemand, ChildPlanInvoker,
    ExecutionContext, RecordBatch, RuntimeError, RuntimeValue,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct BatchExecutor;

pub fn preflight_procedure_parameters<'a>(
    procedures: impl IntoIterator<Item = &'a ResolvedProcedure>,
    registry: &ProcedureRegistry,
    parameters: &BTreeMap<String, RuntimeValue>,
    security_fingerprint: [u8; 32],
    access: &ProcedureAccess,
) -> Result<(), RuntimeError> {
    let procedures = procedures.into_iter().collect::<Vec<_>>();
    for procedure in &procedures {
        let mut referenced = BTreeSet::new();
        for argument in procedure.arguments() {
            argument.expression().visit_parameters(&mut |name| {
                referenced.insert(name);
            });
        }
        for name in referenced {
            let value = parameters
                .get(name)
                .ok_or_else(|| RuntimeError::MissingParameter(name.to_owned()))?;
            if value.estimated_bytes()? > procedure.max_value_bytes() {
                return Err(RuntimeError::ProcedureFailed(
                    "DTG-PROCEDURE-VALUE-BYTES".into(),
                ));
            }
        }
    }
    let context = ExecutionContext::new(parameters.clone());
    for procedure in procedures {
        let mut deferred = BTreeSet::new();
        let mut arguments = BTreeMap::new();
        for argument in procedure.arguments() {
            let mut has_slot = false;
            argument.expression().visit_slots(&mut |_| has_slot = true);
            if has_slot {
                deferred.insert(argument.name().to_owned());
            } else {
                arguments.insert(
                    argument.name().to_owned(),
                    procedure_value(expression::evaluate(
                        argument.expression(),
                        &RowSchema::empty(),
                        &[],
                        &context,
                    )?)?,
                );
            }
        }
        registry
            .preflight_partial(
                *procedure.identity(),
                arguments,
                &deferred,
                security_fingerprint,
                access,
            )
            .map_err(|error| RuntimeError::ProcedureFailed(error.code().to_owned()))?;
    }
    Ok(())
}

impl BatchExecutor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }

    pub fn evaluate(
        &self,
        expression: &ScalarExpr,
        schema: &RowSchema,
        row: &[RuntimeValue],
        context: &ExecutionContext,
    ) -> Result<RuntimeValue, RuntimeError> {
        context.check_fences()?;
        expression::evaluate(expression, schema, row, context)
    }

    pub async fn execute_fragment(
        &self,
        fragment: &PlanFragment,
        context: &ExecutionContext,
        batches: Vec<RecordBatch>,
    ) -> Result<Vec<RecordBatch>, RuntimeError> {
        self.execute_fragment_with_invoker(fragment, context, batches, None)
            .await
    }

    pub async fn execute_fragment_with_invoker(
        &self,
        fragment: &PlanFragment,
        context: &ExecutionContext,
        batches: Vec<RecordBatch>,
        child_invoker: Option<&dyn ChildPlanInvoker>,
    ) -> Result<Vec<RecordBatch>, RuntimeError> {
        self.execute_fragment_with_invoker_and_demand(
            fragment,
            context,
            batches,
            child_invoker,
            ChildOutputDemand::AllRows,
        )
        .await
    }

    pub async fn execute_fragment_with_invoker_and_demand(
        &self,
        fragment: &PlanFragment,
        context: &ExecutionContext,
        batches: Vec<RecordBatch>,
        child_invoker: Option<&dyn ChildPlanInvoker>,
        demand: ChildOutputDemand,
    ) -> Result<Vec<RecordBatch>, RuntimeError> {
        self.execute_fragment_with_invoker_and_demand_and_ledger(
            fragment,
            context,
            batches,
            child_invoker,
            demand,
            ApplyBudgetLedger::default(),
        )
        .await
    }

    pub async fn execute_fragment_with_invoker_and_demand_and_ledger(
        &self,
        fragment: &PlanFragment,
        context: &ExecutionContext,
        batches: Vec<RecordBatch>,
        child_invoker: Option<&dyn ChildPlanInvoker>,
        demand: ChildOutputDemand,
        ledger: ApplyBudgetLedger,
    ) -> Result<Vec<RecordBatch>, RuntimeError> {
        self.execute_operators(
            fragment.operators(),
            fragment.output(),
            fragment.budget().memory_bytes(),
            context,
            batches,
            child_invoker,
            demand,
            &ledger,
        )
        .await
    }

    pub async fn execute_logical_row_operator(
        &self,
        operator: &LogicalOperator,
        input: RecordBatch,
        output: &RowSchema,
        memory_limit: u64,
        context: &ExecutionContext,
    ) -> Result<Vec<RecordBatch>, RuntimeError> {
        let physical = match operator {
            LogicalOperator::Filter { predicate } => PhysicalOperator::Filter(predicate.clone()),
            LogicalOperator::Project { expressions } => PhysicalOperator::Project {
                expressions: expressions.clone(),
                output: output.clone(),
            },
            LogicalOperator::Unwind {
                expression,
                binding,
            } => PhysicalOperator::Unwind {
                expression: expression.clone(),
                binding: *binding,
                output: output.clone(),
            },
            LogicalOperator::Aggregate {
                grouping,
                aggregates,
            } => PhysicalOperator::Aggregate {
                phase: AggregatePhase::Single,
                grouping: grouping.clone(),
                aggregates: aggregates.clone(),
                output: output.clone(),
            },
            LogicalOperator::Sort { keys } => PhysicalOperator::Sort { keys: keys.clone() },
            LogicalOperator::Skip { count } => PhysicalOperator::Skip {
                count: count.clone(),
            },
            LogicalOperator::Limit { count } => PhysicalOperator::Limit {
                count: count.clone(),
            },
            _ => return Err(RuntimeError::UnsupportedOperator("logical row operator")),
        };
        self.execute_operators(
            std::slice::from_ref(&physical),
            output,
            memory_limit,
            context,
            vec![input],
            None,
            ChildOutputDemand::AllRows,
            &ApplyBudgetLedger::default(),
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn execute_operators(
        &self,
        operators: &[PhysicalOperator],
        output: &RowSchema,
        memory_limit: u64,
        context: &ExecutionContext,
        mut batches: Vec<RecordBatch>,
        child_invoker: Option<&dyn ChildPlanInvoker>,
        demand: ChildOutputDemand,
        ledger: &ApplyBudgetLedger,
    ) -> Result<Vec<RecordBatch>, RuntimeError> {
        context.check_fences()?;
        ensure_memory(&batches, memory_limit)?;
        for (operator_index, operator) in operators.iter().enumerate() {
            context.check_fences()?;
            batches = match operator {
                PhysicalOperator::Argument { .. } => argument(batches)?,
                PhysicalOperator::Filter(predicate) => filter(batches, predicate, context)?,
                PhysicalOperator::Project {
                    expressions,
                    output: project_output,
                } => project(batches, expressions, project_output, context)?,
                PhysicalOperator::Unwind {
                    expression,
                    binding,
                    output: unwind_output,
                } => unwind(
                    batches,
                    expression,
                    *binding,
                    unwind_output,
                    memory_limit,
                    context,
                    (demand == ChildOutputDemand::FirstVisibleRow
                        && suffix_preserves_nonempty(&operators[operator_index + 1..]))
                    .then_some(1),
                )?,
                PhysicalOperator::Skip { count } => skip(batches, row_count(count, context)?)?,
                PhysicalOperator::Limit { count } => limit(batches, row_count(count, context)?)?,
                PhysicalOperator::Sort { keys } => sort(batches, keys)?,
                PhysicalOperator::Aggregate {
                    phase,
                    grouping,
                    aggregates,
                    output: aggregate_output,
                } => aggregate(
                    batches,
                    *phase,
                    grouping,
                    aggregates,
                    aggregate_output,
                    context,
                )?,
                PhysicalOperator::TemporalSlice { .. } | PhysicalOperator::Finish => batches,
                PhysicalOperator::NodeScan { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("NodeScan"));
                }
                PhysicalOperator::RelationshipScan { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("RelationshipScan"));
                }
                PhysicalOperator::Expand { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("Expand"));
                }
                PhysicalOperator::HashJoin { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("HashJoin"));
                }
                PhysicalOperator::Union { all } => union(batches, *all, memory_limit)?,
                PhysicalOperator::ChangeScan { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("ChangeScan"));
                }
                PhysicalOperator::Write { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("Write"));
                }
                PhysicalOperator::Procedure {
                    procedure,
                    output: procedure_output,
                } => {
                    execute_procedure(batches, procedure, procedure_output, memory_limit, context)
                        .await?
                }
                PhysicalOperator::Apply {
                    apply,
                    output: apply_output,
                } => {
                    execute_apply(
                        *self,
                        batches,
                        apply,
                        apply_output,
                        memory_limit,
                        context,
                        child_invoker,
                        ledger,
                    )
                    .await?
                }
                PhysicalOperator::BatchSubtransaction { .. } => {
                    return Err(RuntimeError::UnsupportedOperator("BatchSubtransaction"));
                }
            };
            if demand == ChildOutputDemand::FirstVisibleRow
                && suffix_preserves_nonempty(&operators[operator_index + 1..])
            {
                retain_first_row(&mut batches)?;
            }
            ensure_memory(&batches, memory_limit)?;
        }
        if batches.iter().any(|batch| batch.schema() != output) {
            return Err(RuntimeError::OutputSchemaMismatch);
        }
        Ok(batches)
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_apply(
    executor: BatchExecutor,
    batches: Vec<RecordBatch>,
    apply: &PhysicalApply,
    output: &RowSchema,
    memory_limit: u64,
    context: &ExecutionContext,
    child_invoker: Option<&dyn ChildPlanInvoker>,
    ledger: &ApplyBudgetLedger,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    let mut output_rows = Vec::new();
    let mut retained_bytes = 0_u64;
    let limits = ChildInvocationLimits {
        max_invocations: apply.max_invocations(),
        max_output_rows: apply.max_output_rows(),
        max_depth: apply.max_depth(),
    };
    for batch in batches {
        let parent_schema = batch.schema().clone();
        for parent_row in batch.into_rows() {
            context.check_fences()?;
            ledger.charge_invocation(limits)?;
            let _depth = ledger.enter(limits)?;
            let child_row = apply
                .child_input()
                .columns()
                .iter()
                .map(|child_column| {
                    let mapping = apply
                        .imports()
                        .iter()
                        .find(|mapping| mapping.child_slot() == child_column.slot())
                        .ok_or(RuntimeError::MissingSlot(child_column.slot()))?;
                    let index = parent_schema
                        .columns()
                        .iter()
                        .position(|column| column.slot() == mapping.parent_slot())
                        .ok_or(RuntimeError::MissingSlot(mapping.parent_slot()))?;
                    parent_row
                        .get(index)
                        .cloned()
                        .ok_or(RuntimeError::MissingSlot(mapping.parent_slot()))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let child_input = RecordBatch::try_new(apply.child_input().clone(), vec![child_row])?;
            let demand = if matches!(apply.kind(), ApplyKind::Exists { .. }) {
                ChildOutputDemand::FirstVisibleRow
            } else {
                ChildOutputDemand::AllRows
            };
            let child_batches = if let Some(invoker) = child_invoker {
                invoker
                    .invoke(
                        apply.child_plan(),
                        child_input,
                        context,
                        demand,
                        limits,
                        ledger.clone(),
                    )
                    .await?
            } else {
                Box::pin(execute_local_child_plan(
                    executor,
                    apply.child_plan(),
                    context,
                    child_input,
                    child_invoker,
                    demand,
                    ledger,
                ))
                .await?
            };
            let child_row_count = child_batches.iter().try_fold(0_u64, |total, batch| {
                total
                    .checked_add(
                        u64::try_from(batch.rows().len())
                            .map_err(|_| RuntimeError::SizeOverflow)?,
                    )
                    .ok_or(RuntimeError::SizeOverflow)
            })?;
            match apply.kind() {
                ApplyKind::Inner => {
                    for child_batch in child_batches {
                        let child_schema = child_batch.schema().clone();
                        for child_row in child_batch.into_rows() {
                            let mut row = parent_row.clone();
                            for column in &output.columns()[parent_schema.columns().len()..] {
                                let mapping = apply
                                    .exports()
                                    .iter()
                                    .find(|mapping| mapping.parent_slot() == column.slot())
                                    .ok_or(RuntimeError::MissingSlot(column.slot()))?;
                                let index = child_schema
                                    .columns()
                                    .iter()
                                    .position(|column| column.slot() == mapping.child_slot())
                                    .ok_or(RuntimeError::MissingSlot(mapping.child_slot()))?;
                                row.push(
                                    child_row
                                        .get(index)
                                        .cloned()
                                        .ok_or(RuntimeError::MissingSlot(mapping.child_slot()))?,
                                );
                            }
                            retain_apply_row(
                                &mut output_rows,
                                &mut retained_bytes,
                                row,
                                output,
                                apply.max_output_rows(),
                                memory_limit,
                                ledger,
                                limits,
                            )?;
                        }
                    }
                }
                ApplyKind::Exists { .. } => {
                    let mut row = parent_row;
                    row.push(RuntimeValue::Boolean(child_row_count != 0));
                    retain_apply_row(
                        &mut output_rows,
                        &mut retained_bytes,
                        row,
                        output,
                        apply.max_output_rows(),
                        memory_limit,
                        ledger,
                        limits,
                    )?;
                }
                ApplyKind::Count { .. } => {
                    let mut row = parent_row;
                    row.push(RuntimeValue::Integer(
                        i64::try_from(child_row_count).map_err(|_| RuntimeError::SizeOverflow)?,
                    ));
                    retain_apply_row(
                        &mut output_rows,
                        &mut retained_bytes,
                        row,
                        output,
                        apply.max_output_rows(),
                        memory_limit,
                        ledger,
                        limits,
                    )?;
                }
            }
        }
    }
    let batches = batches_from_rows(output, output_rows)?;
    ensure_memory(&batches, memory_limit)?;
    Ok(batches)
}

#[allow(clippy::too_many_arguments)]
fn retain_apply_row(
    output_rows: &mut Vec<Vec<RuntimeValue>>,
    retained_bytes: &mut u64,
    row: Vec<RuntimeValue>,
    output: &RowSchema,
    max_output_rows: u64,
    memory_limit: u64,
    ledger: &ApplyBudgetLedger,
    limits: ChildInvocationLimits,
) -> Result<(), RuntimeError> {
    let next_rows = u64::try_from(output_rows.len())
        .map_err(|_| RuntimeError::SizeOverflow)?
        .checked_add(1)
        .ok_or(RuntimeError::SizeOverflow)?;
    if next_rows > max_output_rows {
        return Err(RuntimeError::ApplyOutputRowLimit {
            max: max_output_rows,
        });
    }
    ledger.charge_output(1, limits)?;
    let bytes = RecordBatch::try_new(output.clone(), vec![row.clone()])?.estimated_bytes();
    let next_bytes = retained_bytes
        .checked_add(bytes)
        .ok_or(RuntimeError::SizeOverflow)?;
    if next_bytes > memory_limit {
        return Err(RuntimeError::MemoryLimitExceeded {
            limit: memory_limit,
            required: next_bytes,
        });
    }
    *retained_bytes = next_bytes;
    output_rows.push(row);
    Ok(())
}

async fn execute_local_child_plan(
    executor: BatchExecutor,
    plan: &PhysicalPlan,
    context: &ExecutionContext,
    input: RecordBatch,
    child_invoker: Option<&dyn ChildPlanInvoker>,
    demand: ChildOutputDemand,
    ledger: &ApplyBudgetLedger,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    plan.validate()
        .map_err(|_| RuntimeError::InvalidPhysicalPlan)?;
    let mut results: BTreeMap<physical_plan::FragmentId, Vec<RecordBatch>> = BTreeMap::new();
    for fragment in plan.fragments() {
        if fragment.placement() != Placement::Coordinator {
            return Err(RuntimeError::UnsupportedOperator(
                "distributed Apply child plan",
            ));
        }
        let incoming = plan
            .exchanges()
            .iter()
            .filter(|exchange| exchange.to() == fragment.id())
            .collect::<Vec<_>>();
        let mut batches = if incoming.is_empty() {
            vec![input.clone()]
        } else {
            let mut batches = Vec::new();
            for exchange in incoming {
                batches.extend(
                    results
                        .get(&exchange.from())
                        .ok_or(RuntimeError::InvalidPhysicalPlan)?
                        .iter()
                        .cloned(),
                );
            }
            batches
        };
        batches = Box::pin(
            executor.execute_fragment_with_invoker_and_demand_and_ledger(
                fragment,
                context,
                batches,
                child_invoker,
                if demand == ChildOutputDemand::FirstVisibleRow
                    && local_first_row_short_circuit_safe(plan)
                {
                    demand
                } else {
                    ChildOutputDemand::AllRows
                },
                ledger.clone(),
            ),
        )
        .await?;
        results.insert(fragment.id(), batches);
    }
    let mut batches = results
        .remove(&plan.root())
        .ok_or(RuntimeError::InvalidPhysicalPlan)?;
    if demand == ChildOutputDemand::FirstVisibleRow {
        retain_first_row(&mut batches)?;
    }
    Ok(batches)
}

fn retain_first_row(batches: &mut Vec<RecordBatch>) -> Result<(), RuntimeError> {
    let Some((schema, row)) = batches.iter().find_map(|batch| {
        batch
            .rows()
            .first()
            .map(|row| (batch.schema().clone(), row.clone()))
    }) else {
        batches.clear();
        return Ok(());
    };
    *batches = vec![RecordBatch::try_new(schema, vec![row])?];
    Ok(())
}

fn local_first_row_short_circuit_safe(plan: &PhysicalPlan) -> bool {
    plan.fragments().iter().all(|fragment| {
        fragment.operators().iter().all(|operator| {
            matches!(
                operator,
                PhysicalOperator::Argument { .. }
                    | PhysicalOperator::Project { .. }
                    | PhysicalOperator::Unwind { .. }
                    | PhysicalOperator::TemporalSlice { .. }
                    | PhysicalOperator::Finish
            )
        })
    })
}

fn suffix_preserves_nonempty(operators: &[PhysicalOperator]) -> bool {
    operators.iter().all(|operator| {
        matches!(
            operator,
            PhysicalOperator::Project { .. }
                | PhysicalOperator::TemporalSlice { .. }
                | PhysicalOperator::Finish
        )
    })
}

async fn execute_procedure(
    batches: Vec<RecordBatch>,
    procedure: &temporal_ir::ResolvedProcedure,
    output: &RowSchema,
    memory_limit: u64,
    context: &ExecutionContext,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    let input_rows = batches.iter().try_fold(0_usize, |total, batch| {
        total
            .checked_add(batch.rows().len())
            .ok_or(RuntimeError::SizeOverflow)
    })?;
    if u64::try_from(input_rows).map_err(|_| RuntimeError::SizeOverflow)?
        > procedure.max_input_rows()
    {
        return Err(RuntimeError::ProcedureInputRowLimit {
            max: procedure.max_input_rows(),
        });
    }
    if input_rows > usize::try_from(procedure.max_invocations()).unwrap_or(usize::MAX) {
        return Err(RuntimeError::ProcedureInvocationLimit {
            max: procedure.max_invocations(),
        });
    }
    let registry = context.procedure_registry()?;
    let mut output_rows = Vec::new();
    let mut retained_bytes = 0_u64;
    for batch in batches {
        let input_schema = batch.schema().clone();
        for input_row in batch.into_rows() {
            let result =
                invoke_procedure_row(registry, procedure, &input_schema, &input_row, context)
                    .await?;
            for provider_row in result.rows() {
                let next_count = output_rows
                    .len()
                    .checked_add(1)
                    .ok_or(RuntimeError::SizeOverflow)?;
                if u64::try_from(next_count).map_err(|_| RuntimeError::SizeOverflow)?
                    > procedure.max_output_rows()
                {
                    return Err(RuntimeError::ProcedureOutputRowLimit {
                        max: procedure.max_output_rows(),
                    });
                }
                let row_bytes =
                    estimate_composed_procedure_row(&input_row, provider_row, procedure.yields())?;
                let required = retained_bytes
                    .checked_add(row_bytes)
                    .ok_or(RuntimeError::SizeOverflow)?;
                if required > memory_limit {
                    return Err(RuntimeError::MemoryLimitExceeded {
                        limit: memory_limit,
                        required,
                    });
                }
                let mut row = input_row.clone();
                for binding in procedure.yields() {
                    let value = provider_row
                        .get(
                            usize::try_from(binding.source_index())
                                .map_err(|_| RuntimeError::SizeOverflow)?,
                        )
                        .ok_or_else(provider_schema_error)?;
                    row.push(runtime_value(value.clone()));
                }
                retained_bytes = required;
                output_rows.push(row);
            }
        }
    }
    let batches = batches_from_rows(output, output_rows)?;
    ensure_memory(&batches, memory_limit)?;
    Ok(batches)
}

pub(super) async fn invoke_procedure_row(
    registry: &ProcedureRegistry,
    procedure: &ResolvedProcedure,
    input_schema: &RowSchema,
    input_row: &[RuntimeValue],
    context: &ExecutionContext,
) -> Result<ProcedureResult, RuntimeError> {
    context.check_fences()?;
    let arguments = procedure
        .arguments()
        .iter()
        .map(|argument| {
            Ok((
                argument.name().to_owned(),
                procedure_value(expression::evaluate(
                    argument.expression(),
                    input_schema,
                    input_row,
                    context,
                )?)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, RuntimeError>>()?;
    let mut invocation = ProcedureInvocation::new(
        *procedure.identity(),
        arguments,
        context.procedure_graph(),
        context.security_fingerprint(),
        context.procedure_access(),
    );
    if let Some(job_context) = context.job_invocation_context() {
        invocation = invocation.with_job_context(job_context.clone());
    }
    let invocation = registry.invoke(invocation);
    tokio::pin!(invocation);
    let result = if let Some(deadline) = context.deadline() {
        tokio::select! {
            _ = context.cancelled() => return Err(RuntimeError::Cancelled),
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                return Err(RuntimeError::DeadlineExceeded);
            }
            result = &mut invocation => result,
        }
    } else {
        tokio::select! {
            _ = context.cancelled() => return Err(RuntimeError::Cancelled),
            result = &mut invocation => result,
        }
    }
    .map_err(|error| RuntimeError::ProcedureFailed(error.code().to_owned()))?;
    context.check_fences()?;
    Ok(result)
}

pub(super) fn estimate_composed_procedure_row(
    input: &[RuntimeValue],
    provider: &[ProcedureValue],
    yields: &[ProcedureYieldBinding],
) -> Result<u64, RuntimeError> {
    let mut bytes = input.iter().try_fold(0_u64, |total, value| {
        total
            .checked_add(value.estimated_bytes()?)
            .ok_or(RuntimeError::SizeOverflow)
    })?;
    for binding in yields {
        let value = provider
            .get(usize::try_from(binding.source_index()).map_err(|_| RuntimeError::SizeOverflow)?)
            .ok_or_else(provider_schema_error)?;
        bytes = bytes
            .checked_add(
                value
                    .estimated_bytes()
                    .map_err(|_| RuntimeError::SizeOverflow)?,
            )
            .ok_or(RuntimeError::SizeOverflow)?;
    }
    Ok(bytes)
}

pub(super) fn provider_schema_error() -> RuntimeError {
    RuntimeError::ProcedureFailed("DTG-PROCEDURE-PROVIDER-SCHEMA".into())
}

fn procedure_value(value: RuntimeValue) -> Result<ProcedureValue, RuntimeError> {
    Ok(match value {
        RuntimeValue::Null => ProcedureValue::Null,
        RuntimeValue::Boolean(value) => ProcedureValue::Boolean(value),
        RuntimeValue::Integer(value) => ProcedureValue::Integer(value),
        RuntimeValue::FloatBits(value) => ProcedureValue::FloatBits(value),
        RuntimeValue::String(value) => ProcedureValue::String(value),
        RuntimeValue::Bytes(value) => ProcedureValue::Bytes(value),
        RuntimeValue::TimestampMicros(value) => ProcedureValue::TimestampMicros(value),
        RuntimeValue::List(values) => ProcedureValue::List(
            values
                .into_iter()
                .map(procedure_value)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        RuntimeValue::Map(values) => ProcedureValue::Map(
            values
                .into_iter()
                .map(|(key, value)| Ok((key, procedure_value(value)?)))
                .collect::<Result<_, RuntimeError>>()?,
        ),
        RuntimeValue::Node(_) => return Err(RuntimeError::ProcedureValueUnsupported("NODE")),
        RuntimeValue::Relationship(_) => {
            return Err(RuntimeError::ProcedureValueUnsupported("RELATIONSHIP"));
        }
    })
}

pub(super) fn runtime_value(value: ProcedureValue) -> RuntimeValue {
    match value {
        ProcedureValue::Null => RuntimeValue::Null,
        ProcedureValue::Boolean(value) => RuntimeValue::Boolean(value),
        ProcedureValue::Integer(value) => RuntimeValue::Integer(value),
        ProcedureValue::FloatBits(value) => RuntimeValue::FloatBits(value),
        ProcedureValue::String(value) => RuntimeValue::String(value),
        ProcedureValue::Bytes(value) => RuntimeValue::Bytes(value),
        ProcedureValue::TimestampMicros(value) => RuntimeValue::TimestampMicros(value),
        ProcedureValue::List(values) => {
            RuntimeValue::List(values.into_iter().map(runtime_value).collect())
        }
        ProcedureValue::Map(values) => RuntimeValue::Map(
            values
                .into_iter()
                .map(|(key, value)| (key, runtime_value(value)))
                .collect(),
        ),
    }
}

fn union(
    batches: Vec<RecordBatch>,
    all: bool,
    memory_limit: u64,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    if all || batches.is_empty() {
        ensure_memory(&batches, memory_limit)?;
        return Ok(batches);
    }
    let schema = batches[0].schema().clone();
    let mut rows = Vec::new();
    let mut required = 0_u64;
    for row in batches.into_iter().flat_map(RecordBatch::into_rows) {
        if !rows.contains(&row) {
            let row_bytes = row.iter().try_fold(0_u64, |total, value| {
                total
                    .checked_add(value.estimated_bytes()?)
                    .ok_or(RuntimeError::SizeOverflow)
            })?;
            let next_required = required
                .checked_add(row_bytes)
                .ok_or(RuntimeError::SizeOverflow)?;
            if next_required > memory_limit {
                return Err(RuntimeError::MemoryLimitExceeded {
                    limit: memory_limit,
                    required: next_required,
                });
            }
            required = next_required;
            rows.push(row);
        }
    }
    batches_from_rows(&schema, rows)
}

fn sort(batches: Vec<RecordBatch>, keys: &[SortKey]) -> Result<Vec<RecordBatch>, RuntimeError> {
    if batches.is_empty() {
        return Ok(batches);
    }
    let schema = batches[0].schema().clone();
    let key_indices = keys
        .iter()
        .map(|key| {
            schema
                .columns()
                .iter()
                .position(|column| column.slot() == key.slot())
                .map(|index| (index, key.ascending()))
                .ok_or(RuntimeError::MissingSlot(key.slot()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows = batches
        .into_iter()
        .flat_map(RecordBatch::into_rows)
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        key_indices
            .iter()
            .map(|(index, ascending)| sort_value_order(&left[*index], &right[*index], *ascending))
            .find(|ordering| *ordering != Ordering::Equal)
            .unwrap_or(Ordering::Equal)
    });
    Ok(vec![RecordBatch::try_new(schema, rows)?])
}

fn sort_value_order(left: &RuntimeValue, right: &RuntimeValue, ascending: bool) -> Ordering {
    let ordering = match (left, right) {
        (RuntimeValue::Null, RuntimeValue::Null) => Ordering::Equal,
        (RuntimeValue::Null, _) => Ordering::Greater,
        (_, RuntimeValue::Null) => Ordering::Less,
        _ => compare_values(left, right),
    };
    if ascending {
        ordering
    } else {
        ordering.reverse()
    }
}

fn aggregate(
    batches: Vec<RecordBatch>,
    phase: AggregatePhase,
    grouping: &[temporal_ir::SlotId],
    aggregates: &[(temporal_ir::SlotId, ScalarExpr)],
    output: &RowSchema,
    context: &ExecutionContext,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    let schema = batches
        .first()
        .map_or_else(RowSchema::empty, |batch| batch.schema().clone());
    let rows = batches
        .into_iter()
        .flat_map(RecordBatch::into_rows)
        .collect::<Vec<_>>();
    if phase == AggregatePhase::FinalCount {
        return finalize_partial_counts(&schema, &rows, aggregates, output);
    }
    let grouping_indices = grouping
        .iter()
        .map(|slot| {
            schema
                .columns()
                .iter()
                .position(|column| column.slot() == *slot)
                .ok_or(RuntimeError::MissingSlot(*slot))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut groups: Vec<(Vec<RuntimeValue>, Vec<Vec<RuntimeValue>>)> = Vec::new();
    if grouping.is_empty() {
        groups.push((Vec::new(), rows));
    } else {
        for row in rows {
            let key = grouping_indices
                .iter()
                .map(|index| row[*index].clone())
                .collect::<Vec<_>>();
            if let Some((_, members)) = groups.iter_mut().find(|(candidate, _)| *candidate == key) {
                members.push(row);
            } else {
                groups.push((key, vec![row]));
            }
        }
    }
    let mut output_rows = Vec::with_capacity(groups.len());
    for (key, members) in groups {
        let mut values = grouping
            .iter()
            .copied()
            .zip(key)
            .collect::<std::collections::BTreeMap<_, _>>();
        for (slot, expression) in aggregates {
            values.insert(
                *slot,
                aggregate_expression(expression, &schema, &members, context)?,
            );
        }
        output_rows.push(
            output
                .columns()
                .iter()
                .map(|column| {
                    values
                        .get(&column.slot())
                        .cloned()
                        .ok_or(RuntimeError::MissingSlot(column.slot()))
                })
                .collect::<Result<Vec<_>, _>>()?,
        );
    }
    batches_from_rows(output, output_rows)
}

fn finalize_partial_counts(
    schema: &RowSchema,
    rows: &[Vec<RuntimeValue>],
    aggregates: &[(temporal_ir::SlotId, ScalarExpr)],
    output: &RowSchema,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    let mut values = BTreeMap::new();
    for (slot, _) in aggregates {
        let index = schema
            .columns()
            .iter()
            .position(|column| column.slot() == *slot)
            .ok_or(RuntimeError::MissingSlot(*slot))?;
        let count = rows.iter().try_fold(0_i64, |total, row| {
            let value = row.get(index).ok_or(RuntimeError::MissingSlot(*slot))?;
            let RuntimeValue::Integer(partial) = value else {
                return Err(RuntimeError::TypeMismatch {
                    expected: temporal_ir::ValueType::Integer,
                    actual: value.kind(),
                });
            };
            total
                .checked_add(*partial)
                .ok_or(RuntimeError::ArithmeticOverflow)
        })?;
        values.insert(*slot, RuntimeValue::Integer(count));
    }
    let row = output
        .columns()
        .iter()
        .map(|column| {
            values
                .get(&column.slot())
                .cloned()
                .ok_or(RuntimeError::MissingSlot(column.slot()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    batches_from_rows(output, vec![row])
}

pub(crate) fn aggregate_expression(
    expression: &ScalarExpr,
    schema: &RowSchema,
    rows: &[Vec<RuntimeValue>],
    context: &ExecutionContext,
) -> Result<RuntimeValue, RuntimeError> {
    let ScalarExpr::Function {
        function_id,
        arguments,
    } = expression
    else {
        return Err(RuntimeError::UnsupportedOperator("non-function Aggregate"));
    };
    if *function_id == function_id_for("count") {
        let mut count = 0_i64;
        for row in rows {
            if arguments.is_empty()
                || !matches!(
                    expression::evaluate(&arguments[0], schema, row, context)?,
                    RuntimeValue::Null
                )
            {
                count = count
                    .checked_add(1)
                    .ok_or(RuntimeError::ArithmeticOverflow)?;
            }
        }
        return Ok(RuntimeValue::Integer(count));
    }
    let mut values = rows
        .iter()
        .map(|row| {
            arguments
                .first()
                .ok_or(RuntimeError::FunctionUnsupported(*function_id))
                .and_then(|argument| expression::evaluate(argument, schema, row, context))
        })
        .filter_map(|value| match value {
            Ok(RuntimeValue::Null) => None,
            Ok(value) => Some(Ok(value)),
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    if values.is_empty() {
        return Ok(RuntimeValue::Null);
    }
    if *function_id == function_id_for("sum") || *function_id == function_id_for("avg") {
        let mut total = 0.0_f64;
        for value in &values {
            total += numeric_value(value)?;
        }
        if *function_id == function_id_for("avg") {
            total /= values.len() as f64;
        }
        return Ok(RuntimeValue::FloatBits(total.to_bits()));
    }
    if *function_id == function_id_for("min") || *function_id == function_id_for("max") {
        values.sort_by(compare_values);
        return Ok(if *function_id == function_id_for("min") {
            values.remove(0)
        } else {
            values.pop().expect("non-empty aggregate values")
        });
    }
    Err(RuntimeError::FunctionUnsupported(*function_id))
}

fn numeric_value(value: &RuntimeValue) -> Result<f64, RuntimeError> {
    match value {
        RuntimeValue::Integer(value) => Ok(*value as f64),
        RuntimeValue::FloatBits(value) => Ok(f64::from_bits(*value)),
        value => Err(RuntimeError::TypeMismatch {
            expected: temporal_ir::ValueType::Float,
            actual: value.kind(),
        }),
    }
}

fn function_id_for(name: &str) -> u32 {
    let digest = blake3::hash(name.as_bytes());
    u32::from_be_bytes(
        digest.as_bytes()[..4]
            .try_into()
            .expect("digest has four bytes"),
    )
}

pub(crate) fn compare_values(left: &RuntimeValue, right: &RuntimeValue) -> Ordering {
    use RuntimeValue as Value;
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Boolean(left), Value::Boolean(right)) => left.cmp(right),
        (Value::Integer(left), Value::Integer(right)) => left.cmp(right),
        (Value::FloatBits(left), Value::FloatBits(right)) => {
            f64::from_bits(*left).total_cmp(&f64::from_bits(*right))
        }
        (Value::String(left), Value::String(right)) => left.cmp(right),
        (Value::TimestampMicros(left), Value::TimestampMicros(right)) => left.cmp(right),
        (Value::Node(left), Value::Node(right)) => left.element().cmp(&right.element()),
        (Value::Relationship(left), Value::Relationship(right)) => {
            left.element().cmp(&right.element())
        }
        (left, right) => left.kind().cmp(right.kind()),
    }
}

fn argument(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>, RuntimeError> {
    if batches.is_empty() {
        return Ok(vec![RecordBatch::try_new(
            RowSchema::empty(),
            vec![Vec::new()],
        )?]);
    }
    Ok(batches)
}

fn filter(
    batches: Vec<RecordBatch>,
    predicate: &ScalarExpr,
    context: &ExecutionContext,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    batches
        .into_iter()
        .map(|batch| {
            let schema = batch.schema().clone();
            let mut rows = Vec::new();
            for row in batch.into_rows() {
                context.check_fences()?;
                match expression::evaluate(predicate, &schema, &row, context)? {
                    RuntimeValue::Boolean(true) => rows.push(row),
                    RuntimeValue::Boolean(false) | RuntimeValue::Null => {}
                    value => return Err(RuntimeError::InvalidPredicate(value.kind())),
                }
            }
            RecordBatch::try_new(schema, rows)
        })
        .collect()
}

fn project(
    batches: Vec<RecordBatch>,
    expressions: &[(temporal_ir::SlotId, ScalarExpr)],
    output: &RowSchema,
    context: &ExecutionContext,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    let ordered = output
        .columns()
        .iter()
        .map(|column| {
            expressions
                .iter()
                .find(|(slot, _)| *slot == column.slot())
                .map(|(_, expression)| expression)
                .ok_or(RuntimeError::MissingSlot(column.slot()))
        })
        .collect::<Result<Vec<_>, _>>()?;
    batches
        .into_iter()
        .map(|batch| {
            let schema = batch.schema().clone();
            let rows = batch
                .into_rows()
                .into_iter()
                .map(|row| {
                    context.check_fences()?;
                    ordered
                        .iter()
                        .map(|expression| expression::evaluate(expression, &schema, &row, context))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?;
            RecordBatch::try_new(output.clone(), rows)
        })
        .collect()
}

fn unwind(
    batches: Vec<RecordBatch>,
    expression: &ScalarExpr,
    binding: temporal_ir::SlotId,
    output: &RowSchema,
    memory_limit: u64,
    context: &ExecutionContext,
    row_limit: Option<usize>,
) -> Result<Vec<RecordBatch>, RuntimeError> {
    let mut output_batches = Vec::new();
    let mut rows = Vec::with_capacity(super::MAX_BATCH_ROWS);
    let mut required = 0_u64;
    'input: for batch in batches {
        let schema = batch.schema().clone();
        for row in batch.into_rows() {
            context.check_fences()?;
            let value = expression::evaluate(expression, &schema, &row, context)?;
            let values = match value {
                RuntimeValue::List(values) => values,
                RuntimeValue::Null => Vec::new(),
                value => {
                    return Err(RuntimeError::TypeMismatch {
                        expected: temporal_ir::ValueType::List(Box::new(
                            temporal_ir::ValueType::Any,
                        )),
                        actual: value.kind(),
                    });
                }
            };
            if values.len() > super::MAX_BATCH_ROWS {
                return Err(RuntimeError::BatchTooLarge {
                    max: super::MAX_BATCH_ROWS,
                    actual: values.len(),
                });
            }
            let existing = schema
                .columns()
                .iter()
                .zip(&row)
                .map(|(column, value)| (column.slot(), value.clone()))
                .collect::<std::collections::BTreeMap<_, _>>();
            for value in values {
                context.check_fences()?;
                let output_row = output
                    .columns()
                    .iter()
                    .map(|column| {
                        if column.slot() == binding {
                            Ok(value.clone())
                        } else {
                            existing
                                .get(&column.slot())
                                .cloned()
                                .ok_or(RuntimeError::MissingSlot(column.slot()))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let row_bytes = output_row.iter().try_fold(0_u64, |total, value| {
                    total
                        .checked_add(value.estimated_bytes()?)
                        .ok_or(RuntimeError::SizeOverflow)
                })?;
                required = required
                    .checked_add(row_bytes)
                    .ok_or(RuntimeError::SizeOverflow)?;
                if required > memory_limit {
                    return Err(RuntimeError::MemoryLimitExceeded {
                        limit: memory_limit,
                        required,
                    });
                }
                rows.push(output_row);
                if row_limit.is_some_and(|limit| rows.len() >= limit) {
                    break 'input;
                }
                if rows.len() == super::MAX_BATCH_ROWS {
                    output_batches.push(RecordBatch::try_new(output.clone(), rows)?);
                    rows = Vec::with_capacity(super::MAX_BATCH_ROWS);
                }
            }
        }
    }
    if !rows.is_empty() || output_batches.is_empty() {
        output_batches.push(RecordBatch::try_new(output.clone(), rows)?);
    }
    Ok(output_batches)
}

pub(crate) fn row_count(
    expression: &ScalarExpr,
    context: &ExecutionContext,
) -> Result<usize, RuntimeError> {
    match expression::evaluate(expression, &RowSchema::empty(), &[], context)? {
        RuntimeValue::Integer(value) if value >= 0 => {
            usize::try_from(value).map_err(|_| RuntimeError::InvalidRowCount)
        }
        _ => Err(RuntimeError::InvalidRowCount),
    }
}

fn skip(batches: Vec<RecordBatch>, mut count: usize) -> Result<Vec<RecordBatch>, RuntimeError> {
    let mut output = Vec::with_capacity(batches.len());
    for batch in batches {
        let schema = batch.schema().clone();
        let rows = batch.into_rows();
        let skipped = count.min(rows.len());
        count -= skipped;
        let rows = rows.into_iter().skip(skipped).collect();
        output.push(RecordBatch::try_new(schema, rows)?);
    }
    Ok(output)
}

fn limit(batches: Vec<RecordBatch>, mut count: usize) -> Result<Vec<RecordBatch>, RuntimeError> {
    let mut output = Vec::new();
    for batch in batches {
        if count == 0 {
            break;
        }
        let schema = batch.schema().clone();
        let rows = batch.into_rows();
        let taken = count.min(rows.len());
        count -= taken;
        output.push(RecordBatch::try_new(
            schema,
            rows.into_iter().take(taken).collect(),
        )?);
    }
    Ok(output)
}

fn ensure_memory(batches: &[RecordBatch], limit: u64) -> Result<(), RuntimeError> {
    let required = batches.iter().try_fold(0_u64, |total, batch| {
        total
            .checked_add(batch.estimated_bytes())
            .ok_or(RuntimeError::SizeOverflow)
    })?;
    if required > limit {
        return Err(RuntimeError::MemoryLimitExceeded { limit, required });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use temporal_ir::{Column, SlotId, ValueType};

    #[test]
    fn point_union_distinct_accounts_unique_rows_before_retention() {
        let schema = integer_schema();
        let input = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    vec![RuntimeValue::Integer(1)],
                    vec![RuntimeValue::Integer(1)],
                    vec![RuntimeValue::Integer(2)],
                ],
            )
            .expect("input"),
        ];

        let exact = union(input.clone(), false, 18).expect("exact budget");
        assert_eq!(exact.len(), 1);
        assert_eq!(
            exact[0].rows(),
            &[
                vec![RuntimeValue::Integer(1)],
                vec![RuntimeValue::Integer(2)],
            ]
        );

        let error = union(input, false, 17).expect_err("second unique row exceeds budget");
        assert_eq!(
            error,
            RuntimeError::MemoryLimitExceeded {
                limit: 17,
                required: 18,
            }
        );
    }

    #[test]
    fn owned_row_batching_rechunks_without_changing_order() {
        let schema = integer_schema();
        let rows = (0..=super::super::MAX_BATCH_ROWS)
            .map(|value| vec![RuntimeValue::Integer(value as i64)])
            .collect::<Vec<_>>();

        let batches = batches_from_rows(&schema, rows).expect("rechunk");

        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].rows().len(), super::super::MAX_BATCH_ROWS);
        assert_eq!(batches[1].rows(), &[vec![RuntimeValue::Integer(16_384)]]);
    }

    #[test]
    fn composed_procedure_size_is_estimated_from_borrowed_values() {
        let input = vec![RuntimeValue::String("abc".into())];
        let provider = vec![ProcedureValue::Bytes(vec![0; 4])];
        let yields = vec![ProcedureYieldBinding::new(0, SlotId::new(1))];

        assert_eq!(
            estimate_composed_procedure_row(&input, &provider, &yields),
            Ok(17)
        );
        assert_eq!(input[0], RuntimeValue::String("abc".into()));
        assert_eq!(provider[0], ProcedureValue::Bytes(vec![0; 4]));
    }

    #[test]
    fn apply_rejects_the_first_exceeding_row_before_retention() {
        let schema = integer_schema();
        let mut rows = Vec::new();
        let mut retained_bytes = 0;
        let ledger = ApplyBudgetLedger::default();
        let limits = ChildInvocationLimits {
            max_invocations: 1,
            max_output_rows: 1,
            max_depth: 1,
        };
        retain_apply_row(
            &mut rows,
            &mut retained_bytes,
            vec![RuntimeValue::Integer(1)],
            &schema,
            1,
            9,
            &ledger,
            limits,
        )
        .unwrap();

        let error = retain_apply_row(
            &mut rows,
            &mut retained_bytes,
            vec![RuntimeValue::Integer(2)],
            &schema,
            1,
            9,
            &ledger,
            limits,
        )
        .expect_err("second row exceeds max_output_rows before push");
        assert_eq!(error, RuntimeError::ApplyOutputRowLimit { max: 1 });
        assert_eq!(rows, vec![vec![RuntimeValue::Integer(1)]]);
        assert_eq!(retained_bytes, 9);

        let string_schema = RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::String,
            false,
        )])
        .unwrap();
        let mut oversized = Vec::new();
        let mut oversized_bytes = 0;
        let oversized_ledger = ApplyBudgetLedger::default();
        let error = retain_apply_row(
            &mut oversized,
            &mut oversized_bytes,
            vec![RuntimeValue::String("too large".into())],
            &string_schema,
            1,
            5,
            &oversized_ledger,
            limits,
        )
        .expect_err("oversized first row must fail before push");
        assert!(matches!(error, RuntimeError::MemoryLimitExceeded { .. }));
        assert!(oversized.is_empty());
        assert_eq!(oversized_bytes, 0);
    }

    fn integer_schema() -> RowSchema {
        RowSchema::new(vec![Column::new(
            SlotId::new(0),
            "value",
            ValueType::Integer,
            false,
        )])
        .expect("schema")
    }
}
