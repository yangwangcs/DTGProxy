use std::collections::BTreeMap;

use dtg_language_ir::{LogicalMutation as IrMutation, LogicalWrite};
use dtg_storage::{
    BackendGeneration, CommandId, GraphId, LogicalMutation, PlacementEpoch, ShardId, TransactionId,
    TransactionTime, Version,
};
use dtg_transaction::{ParticipantWrite, SnapshotToken, TransactionContext};

pub const PHYSICAL_WRITE_VERSION: Version = Version::new(1);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PhysicalWriteError {
    InvalidPlan(String),
    InvalidFence(String),
    InvalidMutation(String),
    Transaction(String),
}

impl PhysicalWriteError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidPlan(_) => "DTG-EXEC-WRITE-PLAN",
            Self::InvalidFence(_) => "DTG-EXEC-WRITE-FENCE",
            Self::InvalidMutation(_) => "DTG-EXEC-WRITE-MUTATION",
            Self::Transaction(_) => "DTG-EXEC-WRITE-TRANSACTION",
        }
    }
}

impl std::fmt::Display for PhysicalWriteError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::InvalidPlan(message)
            | Self::InvalidFence(message)
            | Self::InvalidMutation(message)
            | Self::Transaction(message) => message,
        };
        write!(formatter, "{}: {message}", self.code())
    }
}

impl std::error::Error for PhysicalWriteError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WritePlanningContext {
    graph_id: GraphId,
    schema_version: Version,
    topology_version: Version,
    snapshot: SnapshotToken,
}

impl WritePlanningContext {
    pub fn new(
        graph_id: GraphId,
        schema_version: Version,
        topology_version: Version,
        snapshot: SnapshotToken,
    ) -> Result<Self, PhysicalWriteError> {
        if schema_version.get() == 0 || topology_version.get() == 0 {
            return Err(PhysicalWriteError::InvalidFence(
                "schema and topology versions must be nonzero".into(),
            ));
        }
        Ok(Self {
            graph_id,
            schema_version,
            topology_version,
            snapshot,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct WriteShardTarget {
    shard_id: ShardId,
    placement_epoch: PlacementEpoch,
    backend_generation: BackendGeneration,
    applied_index: u64,
    command_id: CommandId,
}

impl WriteShardTarget {
    pub fn new(
        shard_id: ShardId,
        placement_epoch: PlacementEpoch,
        backend_generation: BackendGeneration,
        applied_index: u64,
        command_id: CommandId,
    ) -> Result<Self, PhysicalWriteError> {
        Ok(Self {
            shard_id,
            placement_epoch,
            backend_generation,
            applied_index,
            command_id,
        })
    }

    pub const fn shard_id(self) -> ShardId {
        self.shard_id
    }

    pub const fn placement_epoch(self) -> PlacementEpoch {
        self.placement_epoch
    }

    pub const fn backend_generation(self) -> BackendGeneration {
        self.backend_generation
    }

    pub const fn applied_index(self) -> u64 {
        self.applied_index
    }

    pub const fn command_id(self) -> CommandId {
        self.command_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoutedWriteMutation {
    target: WriteShardTarget,
    mutation: LogicalMutation,
}

impl RoutedWriteMutation {
    pub fn new(
        target: WriteShardTarget,
        mutation: LogicalMutation,
    ) -> Result<Self, PhysicalWriteError> {
        if matches!(
            mutation,
            LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_)
        ) {
            return Err(PhysicalWriteError::InvalidMutation(
                "physical graph writes cannot contain transaction or replica metadata".into(),
            ));
        }
        Ok(Self { target, mutation })
    }

    pub const fn target(&self) -> WriteShardTarget {
        self.target
    }

    pub const fn mutation(&self) -> &LogicalMutation {
        &self.mutation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalWriteFragment {
    target: WriteShardTarget,
    mutations: Vec<LogicalMutation>,
}

impl PhysicalWriteFragment {
    pub const fn target(&self) -> WriteShardTarget {
        self.target
    }

    pub fn mutations(&self) -> &[LogicalMutation] {
        &self.mutations
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PhysicalWritePlan {
    version: Version,
    graph_id: GraphId,
    schema_version: Version,
    topology_version: Version,
    snapshot: SnapshotToken,
    fragments: Vec<PhysicalWriteFragment>,
}

impl PhysicalWritePlan {
    pub const fn version(&self) -> Version {
        self.version
    }

    pub const fn graph_id(&self) -> GraphId {
        self.graph_id
    }

    pub const fn schema_version(&self) -> Version {
        self.schema_version
    }

    pub const fn topology_version(&self) -> Version {
        self.topology_version
    }

    pub const fn transaction_id(&self) -> TransactionId {
        self.snapshot.transaction_id
    }

    pub const fn transaction_snapshot(&self) -> TransactionTime {
        self.snapshot.start_time
    }

    pub const fn catalog_version(&self) -> Version {
        self.snapshot.catalog_version
    }

    pub const fn snapshot(&self) -> &SnapshotToken {
        &self.snapshot
    }

    pub fn fragments(&self) -> &[PhysicalWriteFragment] {
        &self.fragments
    }

    pub fn participant_writes(&self) -> Result<Vec<ParticipantWrite>, PhysicalWriteError> {
        self.fragments
            .iter()
            .map(|fragment| {
                ParticipantWrite::new(fragment.target.shard_id, fragment.mutations.clone())
                    .map_err(|error| PhysicalWriteError::Transaction(error.to_string()))
            })
            .collect()
    }

    pub fn transaction_submission(
        &self,
    ) -> Result<(TransactionContext, Vec<ParticipantWrite>), PhysicalWriteError> {
        let participants = self.participant_writes()?;
        let mut context = TransactionContext::new(self.snapshot.clone());
        for mutation in self
            .fragments
            .iter()
            .flat_map(PhysicalWriteFragment::mutations)
        {
            context
                .stage(mutation.clone())
                .map_err(|error| PhysicalWriteError::Transaction(error.to_string()))?;
        }
        Ok((context, participants))
    }
}

pub struct PhysicalWritePlanner;

impl PhysicalWritePlanner {
    pub fn lower<F>(
        logical: &LogicalWrite,
        context: WritePlanningContext,
        mut lower_mutation: F,
    ) -> Result<PhysicalWritePlan, PhysicalWriteError>
    where
        F: FnMut(usize, &IrMutation) -> Result<Vec<RoutedWriteMutation>, PhysicalWriteError>,
    {
        if logical.mutations.is_empty() {
            return Err(PhysicalWriteError::InvalidPlan(
                "normalized logical write contains no mutations".into(),
            ));
        }
        let mut grouped = BTreeMap::<ShardId, PhysicalWriteFragment>::new();
        let mut command_shards = BTreeMap::<CommandId, ShardId>::new();
        for (index, mutation) in logical.mutations.iter().enumerate() {
            let routed = lower_mutation(index, mutation)?;
            if routed.is_empty() {
                return Err(PhysicalWriteError::InvalidMutation(format!(
                    "logical mutation {index} lowered to no typed mutations"
                )));
            }
            for routed in routed {
                validate_target(&context.snapshot, routed.target)?;
                match command_shards.insert(routed.target.command_id, routed.target.shard_id) {
                    Some(shard_id) if shard_id != routed.target.shard_id => {
                        return Err(PhysicalWriteError::InvalidFence(
                            "command identity is reused across Shards".into(),
                        ));
                    }
                    _ => {}
                }
                match grouped.get_mut(&routed.target.shard_id) {
                    Some(fragment) => {
                        if fragment.target != routed.target {
                            return Err(PhysicalWriteError::InvalidFence(
                                "one Shard has inconsistent placement, backend, snapshot, or command fences"
                                    .into(),
                            ));
                        }
                        fragment.mutations.push(routed.mutation);
                    }
                    None => {
                        grouped.insert(
                            routed.target.shard_id,
                            PhysicalWriteFragment {
                                target: routed.target,
                                mutations: vec![routed.mutation],
                            },
                        );
                    }
                }
            }
        }
        Ok(PhysicalWritePlan {
            version: PHYSICAL_WRITE_VERSION,
            graph_id: context.graph_id,
            schema_version: context.schema_version,
            topology_version: context.topology_version,
            snapshot: context.snapshot,
            fragments: grouped.into_values().collect(),
        })
    }
}

fn validate_target(
    snapshot: &SnapshotToken,
    target: WriteShardTarget,
) -> Result<(), PhysicalWriteError> {
    let fence = snapshot.shards.get(&target.shard_id).ok_or_else(|| {
        PhysicalWriteError::InvalidFence(format!(
            "Shard {} is absent from the transaction snapshot",
            target.shard_id.get()
        ))
    })?;
    if fence.placement_epoch != target.placement_epoch
        || fence.backend_generation != target.backend_generation
        || fence.applied_index != target.applied_index
    {
        return Err(PhysicalWriteError::InvalidFence(format!(
            "Shard {} target does not match its transaction snapshot fence",
            target.shard_id.get()
        )));
    }
    Ok(())
}
