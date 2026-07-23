use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use physical_plan::PhysicalPlan;
use temporal_ir::RowSchema;

use super::{ExecutionContext, RecordBatch, RuntimeError, TemporalRow};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChildOutputDemand {
    AllRows,
    FirstVisibleRow,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChildInvocationLimits {
    pub max_invocations: u64,
    pub max_output_rows: u64,
    pub max_depth: u16,
}

#[derive(Clone, Debug, Default)]
pub struct ApplyBudgetLedger {
    state: Arc<Mutex<LedgerState>>,
}

#[derive(Debug, Default)]
struct LedgerState {
    invocations: u64,
    output_rows: u64,
    depth: u16,
    invocation_limit: Option<u64>,
    output_limit: Option<u64>,
    depth_limit: Option<u16>,
}

impl ApplyBudgetLedger {
    pub fn charge_invocation(&self, limits: ChildInvocationLimits) -> Result<(), RuntimeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| RuntimeError::RecursivePlanViolation)?;
        state.invocation_limit = Some(
            state
                .invocation_limit
                .map_or(limits.max_invocations, |current| {
                    current.min(limits.max_invocations)
                }),
        );
        state.invocations = state
            .invocations
            .checked_add(1)
            .ok_or(RuntimeError::SizeOverflow)?;
        let max = state.invocation_limit.unwrap_or(limits.max_invocations);
        if state.invocations > max {
            return Err(RuntimeError::ApplyInvocationLimit { max });
        }
        Ok(())
    }

    pub fn charge_output(
        &self,
        rows: u64,
        limits: ChildInvocationLimits,
    ) -> Result<(), RuntimeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| RuntimeError::RecursivePlanViolation)?;
        state.output_limit = Some(
            state
                .output_limit
                .map_or(limits.max_output_rows, |current| {
                    current.min(limits.max_output_rows)
                }),
        );
        state.output_rows = state
            .output_rows
            .checked_add(rows)
            .ok_or(RuntimeError::SizeOverflow)?;
        let max = state.output_limit.unwrap_or(limits.max_output_rows);
        if state.output_rows > max {
            return Err(RuntimeError::ApplyOutputRowLimit { max });
        }
        Ok(())
    }

    pub fn enter(&self, limits: ChildInvocationLimits) -> Result<ApplyDepthGuard, RuntimeError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| RuntimeError::RecursivePlanViolation)?;
        state.depth_limit = Some(
            state
                .depth_limit
                .map_or(limits.max_depth, |current| current.min(limits.max_depth)),
        );
        state.depth = state
            .depth
            .checked_add(1)
            .ok_or(RuntimeError::SizeOverflow)?;
        if state.depth > state.depth_limit.unwrap_or(limits.max_depth) {
            state.depth -= 1;
            return Err(RuntimeError::RecursivePlanViolation);
        }
        Ok(ApplyDepthGuard {
            ledger: self.clone(),
        })
    }
}

pub struct ApplyDepthGuard {
    ledger: ApplyBudgetLedger,
}

impl Drop for ApplyDepthGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.ledger.state.lock() {
            state.depth = state.depth.saturating_sub(1);
        }
    }
}

pub type ChildInvocationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<RecordBatch>, RuntimeError>> + Send + 'a>>;
pub type IntervalChildInvocationFuture<'a> =
    Pin<Box<dyn Future<Output = Result<Vec<TemporalRow>, RuntimeError>> + Send + 'a>>;

pub trait ChildPlanInvoker: Send + Sync {
    fn invoke<'a>(
        &'a self,
        plan: &'a PhysicalPlan,
        input: RecordBatch,
        context: &'a ExecutionContext,
        demand: ChildOutputDemand,
        limits: ChildInvocationLimits,
        ledger: ApplyBudgetLedger,
    ) -> ChildInvocationFuture<'a>;

    #[allow(clippy::too_many_arguments)]
    fn invoke_interval<'a>(
        &'a self,
        plan: &'a PhysicalPlan,
        input_schema: RowSchema,
        input_rows: Vec<TemporalRow>,
        context: &'a ExecutionContext,
        demand: ChildOutputDemand,
        limits: ChildInvocationLimits,
        ledger: ApplyBudgetLedger,
    ) -> IntervalChildInvocationFuture<'a>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_apply_budget_ledger_is_shared_across_clones() {
        let ledger = ApplyBudgetLedger::default();
        let nested = ledger.clone();
        let limits = ChildInvocationLimits {
            max_invocations: 1,
            max_output_rows: 1,
            max_depth: 1,
        };
        let permissive_nested = ChildInvocationLimits {
            max_invocations: 10,
            max_output_rows: 10,
            max_depth: 10,
        };
        ledger.charge_invocation(limits).unwrap();
        assert_eq!(
            nested.charge_invocation(permissive_nested),
            Err(RuntimeError::ApplyInvocationLimit { max: 1 })
        );
        ledger.charge_output(1, limits).unwrap();
        assert_eq!(
            nested.charge_output(1, permissive_nested),
            Err(RuntimeError::ApplyOutputRowLimit { max: 1 })
        );
        let _outer = ledger.enter(limits).unwrap();
        assert!(matches!(
            nested.enter(permissive_nested),
            Err(RuntimeError::RecursivePlanViolation)
        ));
    }
}
