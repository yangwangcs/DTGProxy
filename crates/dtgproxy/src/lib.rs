#![forbid(unsafe_code)]

//! DTGProxy deployment modes and deterministic graph-partition routing.
//!
//! `PrimaryReplica` is the compatibility deployment for one logical Shard.
//! `SharedNothing` uses several independently owned Shards. Each Shard can still
//! be a replicated Raft group; the two modes therefore share the same consensus,
//! temporal state-machine, and read-barrier implementation.

pub mod config;
pub mod gateway;
mod transaction;

pub use control_plane;
pub use transaction::{
    PreparedShardTransaction, RoutedConstraintClaim, ScopedTemporalTransaction, TransactionContext,
    TransactionCoordinator, TransactionCoordinatorError, TransactionReceipt,
    TransactionRecoveryReceipt, TransactionStatus,
};

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

use shard_runtime::{InProcessShardGroup, MultiRaftRuntime, ProposalReceipt, ReplicationError};
use storage_api::StorageAdapter;
use temporal_ir::GraphScope;
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
    virtual_partitions: u32,
    shards: Vec<ShardPlacement>,
}

impl DeploymentConfig {
    #[must_use]
    pub fn primary_replica(shard: ShardPlacement) -> Self {
        Self {
            mode: DeploymentMode::PrimaryReplica,
            route_seed: 0,
            virtual_partitions: 1,
            shards: vec![shard],
        }
    }

    pub fn shared_nothing(
        route_seed: u64,
        shards: Vec<ShardPlacement>,
    ) -> Result<Self, DeploymentError> {
        Self::shared_nothing_with_virtual_partitions(route_seed, 65_536, shards)
    }

    pub fn shared_nothing_with_virtual_partitions(
        route_seed: u64,
        virtual_partitions: u32,
        mut shards: Vec<ShardPlacement>,
    ) -> Result<Self, DeploymentError> {
        if virtual_partitions == 0 {
            return Err(DeploymentError::ZeroVirtualPartitions);
        }
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
            virtual_partitions,
            shards,
        })
    }

    pub fn from_catalog(graph: &control_plane::GraphDefinition) -> Result<Self, DeploymentError> {
        let topology = graph.topology();
        let shards = topology
            .placements()
            .iter()
            .map(|placement| {
                ShardPlacement::new(
                    placement.shard_id(),
                    placement.epoch(),
                    placement.voters().to_vec(),
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        match topology.mode() {
            control_plane::DeploymentMode::PrimaryReplica => {
                let shard = shards
                    .into_iter()
                    .next()
                    .expect("validated catalog PrimaryReplica topology has one Shard");
                let mut config = Self::primary_replica(shard);
                config.virtual_partitions = topology.virtual_partitions();
                Ok(config)
            }
            control_plane::DeploymentMode::SharedNothing => {
                Self::shared_nothing_with_virtual_partitions(
                    topology.route_seed(),
                    topology.virtual_partitions(),
                    shards,
                )
            }
        }
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
    pub const fn virtual_partitions(&self) -> u32 {
        self.virtual_partitions
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

    pub async fn build_runtime_with_adapters(
        &self,
        mut adapters: BTreeMap<(u32, u64), Arc<dyn StorageAdapter>>,
    ) -> Result<MultiRaftRuntime, ReplicationError> {
        let mut runtime = MultiRaftRuntime::new();
        for shard in &self.shards {
            let mut shard_adapters = BTreeMap::new();
            for node_id in shard.voters() {
                let adapter = adapters
                    .remove(&(shard.shard_id(), *node_id))
                    .ok_or(ReplicationError::MissingAdapter { node_id: *node_id })?;
                shard_adapters.insert(*node_id, adapter);
            }
            runtime.insert_group(
                InProcessShardGroup::new_with_adapters(
                    shard.shard_id(),
                    shard.placement_epoch(),
                    shard.voters(),
                    shard_adapters,
                )
                .await?,
            )?;
        }
        if let Some(((_, node_id), _)) = adapters.into_iter().next() {
            return Err(ReplicationError::UnexpectedAdapter { node_id });
        }
        Ok(runtime)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeploymentError {
    ZeroPlacementEpoch { shard_id: u32 },
    NoVoters { shard_id: u32 },
    DuplicateVoter { shard_id: u32, node_id: u64 },
    DuplicateShard { shard_id: u32 },
    SharedNothingNeedsMultipleShards { actual: usize },
    ZeroVirtualPartitions,
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
            Self::ZeroVirtualPartitions => {
                formatter.write_str("virtual partition count must be nonzero")
            }
        }
    }
}

impl Error for DeploymentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
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

    pub async fn new_with_adapters(
        config: DeploymentConfig,
        adapters: BTreeMap<(u32, u64), Arc<dyn StorageAdapter>>,
    ) -> Result<Self, ReplicationError> {
        let raft = config.build_runtime_with_adapters(adapters).await?;
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
