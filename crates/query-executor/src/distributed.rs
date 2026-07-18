use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_ir::{PlanBody, TemporalPlan, TemporalSelector};
use temporal_types::TransactionTime;

use crate::{QueryRecord, QueryResult};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotToken {
    Current,
    AsOf(TransactionTime),
}

impl From<TemporalSelector> for SnapshotToken {
    fn from(value: TemporalSelector) -> Self {
        match value {
            TemporalSelector::Current => Self::Current,
            TemporalSelector::AsOf(timestamp) => Self::AsOf(timestamp),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardQueryBatch {
    shard_id: u32,
    topology_epoch: u64,
    snapshot: SnapshotToken,
    result: QueryResult,
}

impl ShardQueryBatch {
    #[must_use]
    pub const fn new(
        shard_id: u32,
        topology_epoch: u64,
        snapshot: SnapshotToken,
        result: QueryResult,
    ) -> Self {
        Self {
            shard_id,
            topology_epoch,
            snapshot,
            result,
        }
    }
}

pub fn merge_distributed_results(
    plan: &TemporalPlan,
    expected_topology_epoch: u64,
    expected_shards: &[u32],
    batches: Vec<ShardQueryBatch>,
) -> Result<QueryResult, DistributedQueryError> {
    let PlanBody::Scan {
        transaction, limit, ..
    } = plan.body()
    else {
        return Err(DistributedQueryError::NotGlobalScan);
    };
    let expected_snapshot = SnapshotToken::from(*transaction);
    let expected = expected_shards.iter().copied().collect::<BTreeSet<_>>();
    let mut observed = BTreeSet::new();
    let mut records = BTreeMap::new();
    for batch in batches {
        if batch.topology_epoch != expected_topology_epoch {
            return Err(DistributedQueryError::TopologyEpochMismatch {
                expected: expected_topology_epoch,
                actual: batch.topology_epoch,
            });
        }
        if batch.snapshot != expected_snapshot {
            return Err(DistributedQueryError::SnapshotMismatch);
        }
        if !expected.contains(&batch.shard_id) {
            return Err(DistributedQueryError::UnexpectedShard {
                shard_id: batch.shard_id,
            });
        }
        if !observed.insert(batch.shard_id) {
            return Err(DistributedQueryError::DuplicateShard {
                shard_id: batch.shard_id,
            });
        }
        for record in batch.result.into_records() {
            records.entry(record_key(&record)).or_insert(record);
        }
    }
    if observed != expected {
        return Err(DistributedQueryError::MissingShard);
    }
    Ok(QueryResult::from_records(
        records
            .into_values()
            .take(usize::try_from(*limit).expect("u32 limit fits usize"))
            .collect(),
    ))
}

fn record_key(record: &QueryRecord) -> (u8, u64, u32, u128) {
    match record {
        QueryRecord::Vertex(record) => {
            let element = record.element();
            (
                1,
                element.graph().value(),
                element.partition().value(),
                element.id().value(),
            )
        }
        QueryRecord::Edge(record) => {
            let element = record.element();
            (
                2,
                element.graph().value(),
                element.partition().value(),
                element.id().value(),
            )
        }
        QueryRecord::Change(record) => {
            let element = record.element();
            (
                3,
                element.graph().value(),
                element.partition().value(),
                element.id().value(),
            )
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DistributedQueryError {
    NotGlobalScan,
    TopologyEpochMismatch { expected: u64, actual: u64 },
    SnapshotMismatch,
    UnexpectedShard { shard_id: u32 },
    DuplicateShard { shard_id: u32 },
    MissingShard,
}

impl Display for DistributedQueryError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotGlobalScan => formatter.write_str("distributed merge requires a global scan"),
            Self::TopologyEpochMismatch { expected, actual } => write!(
                formatter,
                "distributed query topology epoch {actual} differs from {expected}"
            ),
            Self::SnapshotMismatch => {
                formatter.write_str("distributed query Shards used different snapshots")
            }
            Self::UnexpectedShard { shard_id } => {
                write!(
                    formatter,
                    "distributed query returned unexpected Shard {shard_id}"
                )
            }
            Self::DuplicateShard { shard_id } => {
                write!(
                    formatter,
                    "distributed query returned Shard {shard_id} twice"
                )
            }
            Self::MissingShard => {
                formatter.write_str("distributed query did not return every required Shard")
            }
        }
    }
}

impl Error for DistributedQueryError {}
