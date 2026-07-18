use std::error::Error;
use std::fmt::{self, Display, Formatter};

use shard_runtime::{FollowerReadProof, InProcessShardGroup, ReadBarrierError, ReadPermit};
use temporal_ir::{PlanBody, PlanError, TemporalPlan, TemporalSelector};
use temporal_storage::TemporalStore;
use temporal_types::TransactionTime;

use crate::{ExecutorError, LocalExecutor, QueryResult};

pub struct ShardQueryExecutor;

impl ShardQueryExecutor {
    pub async fn execute_leader(
        group: &mut InProcessShardGroup,
        node_id: u64,
        placement_epoch: u64,
        plan: &TemporalPlan,
        max_ticks: usize,
    ) -> Result<QueryResult, ShardQueryError> {
        validate_plan_scope(group, plan)?;
        let permit = group
            .leader_read_permit(node_id, placement_epoch, max_ticks)
            .await?;
        execute_permitted(group, permit, plan).await
    }

    pub async fn execute_follower(
        group: &InProcessShardGroup,
        node_id: u64,
        placement_epoch: u64,
        proof: &FollowerReadProof,
        plan: &TemporalPlan,
    ) -> Result<QueryResult, ShardQueryError> {
        validate_plan_scope(group, plan)?;
        let read_ts = follower_read_timestamp(plan)?;
        let permit = group.follower_read_permit(node_id, placement_epoch, read_ts, proof)?;
        execute_permitted(group, permit, plan).await
    }
}

fn validate_plan_scope(
    group: &InProcessShardGroup,
    plan: &TemporalPlan,
) -> Result<(), ShardQueryError> {
    plan.validate().map_err(ExecutorError::Plan)?;
    if plan.is_global() {
        return Ok(());
    }
    let actual = plan.scope().partition().value();
    if actual != group.shard_id() {
        return Err(ShardQueryError::ShardMismatch {
            expected: group.shard_id(),
            actual,
        });
    }
    Ok(())
}

fn follower_read_timestamp(plan: &TemporalPlan) -> Result<TransactionTime, ShardQueryError> {
    match plan.body() {
        PlanBody::Point {
            transaction: TemporalSelector::AsOf(read_ts),
            ..
        } => Ok(*read_ts),
        PlanBody::Diff { to_transaction, .. } => Ok(*to_transaction),
        PlanBody::Point {
            transaction: TemporalSelector::Current,
            ..
        }
        | PlanBody::Scan {
            transaction: TemporalSelector::Current,
            ..
        } => Err(ShardQueryError::FollowerCurrentUnsupported),
        PlanBody::Scan {
            transaction: TemporalSelector::AsOf(read_ts),
            ..
        } => Ok(*read_ts),
    }
}

async fn execute_permitted(
    group: &InProcessShardGroup,
    permit: ReadPermit,
    plan: &TemporalPlan,
) -> Result<QueryResult, ShardQueryError> {
    let adapter =
        group
            .replica_adapter(permit.node_id())
            .ok_or(ReadBarrierError::NodeNotFound {
                node_id: permit.node_id(),
            })?;
    let executor = LocalExecutor::new(TemporalStore::new(adapter));
    executor.execute(plan).await.map_err(Into::into)
}

#[derive(Debug, Eq, PartialEq)]
pub enum ShardQueryError {
    Barrier(ReadBarrierError),
    Executor(ExecutorError),
    ShardMismatch { expected: u32, actual: u32 },
    FollowerCurrentUnsupported,
}

impl Display for ShardQueryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Barrier(error) => Display::fmt(error, formatter),
            Self::Executor(error) => Display::fmt(error, formatter),
            Self::ShardMismatch { expected, actual } => {
                write!(
                    formatter,
                    "expected shard {expected}, got partition {actual}"
                )
            }
            Self::FollowerCurrentUnsupported => {
                formatter.write_str("follower reads require AS OF or DIFF transaction time")
            }
        }
    }
}

impl Error for ShardQueryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Barrier(error) => Some(error),
            Self::Executor(error) => Some(error),
            Self::ShardMismatch { .. } | Self::FollowerCurrentUnsupported => None,
        }
    }
}

impl From<ReadBarrierError> for ShardQueryError {
    fn from(error: ReadBarrierError) -> Self {
        Self::Barrier(error)
    }
}

impl From<ExecutorError> for ShardQueryError {
    fn from(error: ExecutorError) -> Self {
        Self::Executor(error)
    }
}

impl From<PlanError> for ShardQueryError {
    fn from(error: PlanError) -> Self {
        Self::Executor(ExecutorError::Plan(error))
    }
}
