#![forbid(unsafe_code)]

//! DTGProxy deployment modes and deterministic graph-partition routing.
//!
//! `PrimaryReplica` is the compatibility deployment for one logical Shard.
//! `SharedNothing` uses several independently owned Shards. Each Shard can still
//! be a replicated Raft group; the two modes therefore share the same consensus,
//! temporal state-machine, and read-barrier implementation.

mod transaction;

pub use transaction::{
    PreparedShardTransaction, ScopedTemporalTransaction, TransactionContext,
    TransactionCoordinator, TransactionCoordinatorError, TransactionReceipt, TransactionStatus,
};

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use query_executor::{ExecutorError, LocalExecutor, QueryResult};
use shard_runtime::{
    InProcessShardGroup, MultiRaftRuntime, ProposalReceipt, ReadBarrierError, ReplicationError,
};
use temporal_ir::{GraphScope, PlanBody, PlanError, TemporalPlan, TemporalSelector};
use temporal_storage::TemporalStore;
use temporal_types::TransactionTime;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeploymentMode {
    PrimaryReplica,
    SharedNothing,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardPlacement {
    shard_id: u32,
    placement_epoch: u64,
    voters: Vec<u64>,
}

impl ShardPlacement {
    pub fn new(
        shard_id: u32,
        placement_epoch: u64,
        mut voters: Vec<u64>,
    ) -> Result<Self, DeploymentError> {
        if placement_epoch == 0 {
            return Err(DeploymentError::ZeroPlacementEpoch { shard_id });
        }
        if voters.is_empty() {
            return Err(DeploymentError::NoVoters { shard_id });
        }
        voters.sort_unstable();
        for duplicate in voters.windows(2) {
            if duplicate[0] == duplicate[1] {
                return Err(DeploymentError::DuplicateVoter {
                    shard_id,
                    node_id: duplicate[0],
                });
            }
        }
        Ok(Self {
            shard_id,
            placement_epoch,
            voters,
        })
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(&self) -> u64 {
        self.placement_epoch
    }

    #[must_use]
    pub fn voters(&self) -> &[u64] {
        &self.voters
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeploymentConfig {
    mode: DeploymentMode,
    route_seed: u64,
    shards: Vec<ShardPlacement>,
}

impl DeploymentConfig {
    #[must_use]
    pub fn primary_replica(shard: ShardPlacement) -> Self {
        Self {
            mode: DeploymentMode::PrimaryReplica,
            route_seed: 0,
            shards: vec![shard],
        }
    }

    pub fn shared_nothing(
        route_seed: u64,
        mut shards: Vec<ShardPlacement>,
    ) -> Result<Self, DeploymentError> {
        if shards.len() < 2 {
            return Err(DeploymentError::SharedNothingNeedsMultipleShards {
                actual: shards.len(),
            });
        }
        shards.sort_by_key(ShardPlacement::shard_id);
        for duplicate in shards.windows(2) {
            if duplicate[0].shard_id == duplicate[1].shard_id {
                return Err(DeploymentError::DuplicateShard {
                    shard_id: duplicate[0].shard_id,
                });
            }
        }
        Ok(Self {
            mode: DeploymentMode::SharedNothing,
            route_seed,
            shards,
        })
    }

    #[must_use]
    pub const fn mode(&self) -> DeploymentMode {
        self.mode
    }

    #[must_use]
    pub const fn route_seed(&self) -> u64 {
        self.route_seed
    }

    #[must_use]
    pub fn all_shards(&self) -> &[ShardPlacement] {
        &self.shards
    }

    /// Route one graph partition to exactly one logical Shard.
    ///
    /// Shared-Nothing mode uses highest-random-weight (rendezvous) hashing. Adding
    /// one Shard can therefore only keep a partition on its previous Shard or move
    /// it to the new Shard; it cannot reshuffle partitions between existing Shards.
    #[must_use]
    pub fn route_scope(&self, scope: GraphScope) -> &ShardPlacement {
        if self.mode == DeploymentMode::PrimaryReplica {
            return &self.shards[0];
        }
        self.shards
            .iter()
            .max_by_key(|shard| route_score(self.route_seed, scope, shard.shard_id))
            .expect("validated DeploymentConfig always contains Shards")
    }

    pub fn route_plan<'config>(
        &'config self,
        plan: &TemporalPlan,
    ) -> Result<QueryRoute<'config>, DeploymentError> {
        plan.validate().map_err(DeploymentError::InvalidPlan)?;
        let policy = match plan.body() {
            PlanBody::Point { transaction, .. } => match transaction {
                TemporalSelector::Current => QueryRoutePolicy::LeaderRequired,
                TemporalSelector::AsOf(read_ts) => {
                    QueryRoutePolicy::FollowerEligible { read_ts: *read_ts }
                }
            },
            PlanBody::Diff { to_transaction, .. } => QueryRoutePolicy::FollowerEligible {
                read_ts: *to_transaction,
            },
        };
        Ok(QueryRoute {
            shard: self.route_scope(plan.scope()),
            policy,
        })
    }

    /// Materialize the configured topology in the deterministic in-process runtime.
    /// Production processes use the same placements with durable replicas and a
    /// replaceable network transport.
    pub async fn build_in_process_runtime(&self) -> Result<MultiRaftRuntime, ReplicationError> {
        let mut runtime = MultiRaftRuntime::new();
        for shard in &self.shards {
            runtime.insert_group(
                InProcessShardGroup::new(shard.shard_id, shard.placement_epoch, shard.voters())
                    .await?,
            )?;
        }
        Ok(runtime)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryRoutePolicy {
    LeaderRequired,
    FollowerEligible { read_ts: TransactionTime },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryRoute<'config> {
    shard: &'config ShardPlacement,
    policy: QueryRoutePolicy,
}

impl<'config> QueryRoute<'config> {
    #[must_use]
    pub const fn shard(self) -> &'config ShardPlacement {
        self.shard
    }

    #[must_use]
    pub const fn policy(self) -> QueryRoutePolicy {
        self.policy
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeploymentError {
    ZeroPlacementEpoch { shard_id: u32 },
    NoVoters { shard_id: u32 },
    DuplicateVoter { shard_id: u32, node_id: u64 },
    DuplicateShard { shard_id: u32 },
    SharedNothingNeedsMultipleShards { actual: usize },
    InvalidPlan(PlanError),
}

impl Display for DeploymentError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroPlacementEpoch { shard_id } => {
                write!(formatter, "Shard {shard_id} has placement epoch zero")
            }
            Self::NoVoters { shard_id } => write!(formatter, "Shard {shard_id} has no voters"),
            Self::DuplicateVoter { shard_id, node_id } => {
                write!(formatter, "Shard {shard_id} contains voter {node_id} twice")
            }
            Self::DuplicateShard { shard_id } => {
                write!(formatter, "Shard {shard_id} is configured twice")
            }
            Self::SharedNothingNeedsMultipleShards { actual } => write!(
                formatter,
                "Shared-Nothing mode needs at least two Shards; got {actual}"
            ),
            Self::InvalidPlan(error) => write!(formatter, "invalid Temporal IR: {error}"),
        }
    }
}

impl Error for DeploymentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidPlan(error) => Some(error),
            _ => None,
        }
    }
}

/// Deterministic acceptance runtime for both deployment modes.
///
/// It proves the routing and temporal authorization contract against the same
/// `MultiRaftRuntime` used by lower-level fault tests. The process runtime swaps
/// in durable RocksDB replicas and TCP (later pooled async) transport without
/// changing this routing contract.
pub struct InProcessDeploymentRuntime {
    config: DeploymentConfig,
    raft: MultiRaftRuntime,
}

impl InProcessDeploymentRuntime {
    pub async fn new(config: DeploymentConfig) -> Result<Self, ReplicationError> {
        let raft = config.build_in_process_runtime().await?;
        Ok(Self { config, raft })
    }

    #[must_use]
    pub const fn config(&self) -> &DeploymentConfig {
        &self.config
    }

    #[must_use]
    pub const fn raft(&self) -> &MultiRaftRuntime {
        &self.raft
    }

    pub const fn raft_mut(&mut self) -> &mut MultiRaftRuntime {
        &mut self.raft
    }

    pub async fn elect(&mut self, shard_id: u32, node_id: u64) -> Result<(), ReplicationError> {
        self.raft.group_mut(shard_id)?.elect(node_id).await
    }

    pub async fn propose_scoped(
        &mut self,
        scope: GraphScope,
        command: Vec<u8>,
        max_ticks: usize,
    ) -> Result<ProposalReceipt, ReplicationError> {
        let shard_id = self.config.route_scope(scope).shard_id;
        self.raft
            .group_mut(shard_id)?
            .propose_and_wait(command, max_ticks)
            .await
    }

    pub async fn propose_shard(
        &mut self,
        shard_id: u32,
        command: Vec<u8>,
        max_ticks: usize,
    ) -> Result<ProposalReceipt, ReplicationError> {
        self.raft
            .group_mut(shard_id)?
            .propose_and_wait(command, max_ticks)
            .await
    }

    pub async fn propose_shards(
        &mut self,
        commands: Vec<(u32, Vec<u8>)>,
        max_ticks: usize,
    ) -> Result<Vec<(u32, Result<ProposalReceipt, ReplicationError>)>, ReplicationError> {
        self.raft.propose_many(commands, max_ticks).await
    }

    pub async fn advance_closed_timestamp(
        &mut self,
        scope: GraphScope,
        closed_ts: TransactionTime,
        max_ticks: usize,
    ) -> Result<ProposalReceipt, ReplicationError> {
        let shard_id = self.config.route_scope(scope).shard_id;
        self.raft
            .group_mut(shard_id)?
            .advance_closed_timestamp(closed_ts, max_ticks)
            .await
    }

    /// Execute after a quorum-backed leader ReadIndex barrier.
    pub async fn execute_leader(
        &mut self,
        plan: &TemporalPlan,
        max_ticks: usize,
    ) -> Result<QueryResult, RoutedRuntimeError> {
        let (shard_id, placement_epoch) = {
            let route = self.config.route_plan(plan)?;
            (route.shard.shard_id, route.shard.placement_epoch)
        };
        let group = self.raft.group_mut(shard_id)?;
        let leader_id = group
            .leader_id()
            .ok_or(RoutedRuntimeError::NoLeader { shard_id })?;
        let permit = group
            .leader_read_permit(leader_id, placement_epoch, max_ticks)
            .await?;
        let adapter =
            group
                .replica_adapter(permit.node_id())
                .ok_or(ReadBarrierError::NodeNotFound {
                    node_id: permit.node_id(),
                })?;
        LocalExecutor::new(TemporalStore::new(adapter))
            .execute(plan)
            .await
            .map_err(Into::into)
    }

    /// Execute an `AS OF` or `DIFF` plan on a follower after ReadIndex, term,
    /// applied-index, placement-epoch, and closed-timestamp validation.
    pub async fn execute_follower(
        &mut self,
        plan: &TemporalPlan,
        follower_id: u64,
        max_ticks: usize,
    ) -> Result<QueryResult, RoutedRuntimeError> {
        let (shard_id, placement_epoch, read_ts) = {
            let route = self.config.route_plan(plan)?;
            let QueryRoutePolicy::FollowerEligible { read_ts } = route.policy else {
                return Err(RoutedRuntimeError::CurrentRequiresLeader);
            };
            (route.shard.shard_id, route.shard.placement_epoch, read_ts)
        };
        let group = self.raft.group_mut(shard_id)?;
        let proof = group
            .issue_follower_read_proof(placement_epoch, max_ticks)
            .await?;
        let permit = group.follower_read_permit(follower_id, placement_epoch, read_ts, &proof)?;
        let adapter =
            group
                .replica_adapter(permit.node_id())
                .ok_or(ReadBarrierError::NodeNotFound {
                    node_id: permit.node_id(),
                })?;
        LocalExecutor::new(TemporalStore::new(adapter))
            .execute(plan)
            .await
            .map_err(Into::into)
    }
}

#[derive(Debug)]
pub enum RoutedRuntimeError {
    Deployment(DeploymentError),
    Replication(ReplicationError),
    Barrier(ReadBarrierError),
    Executor(ExecutorError),
    NoLeader { shard_id: u32 },
    CurrentRequiresLeader,
}

impl Display for RoutedRuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Deployment(error) => Display::fmt(error, formatter),
            Self::Replication(error) => Display::fmt(error, formatter),
            Self::Barrier(error) => Display::fmt(error, formatter),
            Self::Executor(error) => Display::fmt(error, formatter),
            Self::NoLeader { shard_id } => write!(formatter, "Shard {shard_id} has no leader"),
            Self::CurrentRequiresLeader => {
                formatter.write_str("CURRENT plans require leader execution")
            }
        }
    }
}

impl Error for RoutedRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Deployment(error) => Some(error),
            Self::Replication(error) => Some(error),
            Self::Barrier(error) => Some(error),
            Self::Executor(error) => Some(error),
            Self::NoLeader { .. } | Self::CurrentRequiresLeader => None,
        }
    }
}

impl From<DeploymentError> for RoutedRuntimeError {
    fn from(error: DeploymentError) -> Self {
        Self::Deployment(error)
    }
}

impl From<ReplicationError> for RoutedRuntimeError {
    fn from(error: ReplicationError) -> Self {
        Self::Replication(error)
    }
}

impl From<ReadBarrierError> for RoutedRuntimeError {
    fn from(error: ReadBarrierError) -> Self {
        Self::Barrier(error)
    }
}

impl From<ExecutorError> for RoutedRuntimeError {
    fn from(error: ExecutorError) -> Self {
        Self::Executor(error)
    }
}

fn route_score(seed: u64, scope: GraphScope, shard_id: u32) -> u64 {
    let graph = scope.graph().value();
    let partition = u64::from(scope.partition().value());
    let shard = u64::from(shard_id);
    splitmix64(
        seed ^ splitmix64(graph)
            ^ splitmix64(partition.wrapping_add(0x9e37_79b9_7f4a_7c15))
            ^ splitmix64(shard.wrapping_add(0xd1b5_4a32_d192_ed03)),
    )
}

const fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
