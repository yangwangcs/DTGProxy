use std::error::Error;
use std::fmt::{self, Display, Formatter};

use control_plane::{
    CatalogCommand, CatalogState, GraphDefinition, MigrationProgress, MigrationRecord,
    MigrationState, Placement, TopologyDefinition,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotFence {
    pub index: u64,
    pub checksum: [u8; 32],
}

pub trait CatalogApi {
    async fn load(&self) -> Result<CatalogState, ControllerError>;

    async fn propose(&self, command: CatalogCommand) -> Result<(), ControllerError>;
}

pub trait DataPlaneApi {
    async fn ensure_target_learners(
        &self,
        migration: &MigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<(), ControllerError>;

    async fn copy_snapshot(
        &self,
        migration: &MigrationRecord,
    ) -> Result<SnapshotFence, ControllerError>;

    async fn catch_up(&self, migration: &MigrationRecord) -> Result<u64, ControllerError>;

    async fn commit_membership(&self, migration: &MigrationRecord) -> Result<u64, ControllerError>;

    async fn activate_target_replicas(
        &self,
        migration: &MigrationRecord,
    ) -> Result<(), ControllerError>;

    async fn cleanup_safe(&self, migration: &MigrationRecord) -> Result<bool, ControllerError>;

    async fn delete_source_replicas(
        &self,
        migration: &MigrationRecord,
    ) -> Result<(), ControllerError>;

    async fn delete_target_learners(
        &self,
        migration: &MigrationRecord,
    ) -> Result<(), ControllerError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconcileOutcome {
    Advanced {
        migration_id: u128,
        from: MigrationState,
        to: MigrationState,
    },
    Waiting {
        migration_id: u128,
        state: MigrationState,
    },
    Terminal {
        migration_id: u128,
        state: MigrationState,
    },
    Missing {
        migration_id: u128,
    },
}

pub struct Reconciler<C, D> {
    catalog: C,
    data: D,
    owner_term: u64,
}

impl<C, D> Reconciler<C, D>
where
    C: CatalogApi,
    D: DataPlaneApi,
{
    pub fn new(catalog: C, data: D, owner_term: u64) -> Result<Self, ControllerError> {
        if owner_term == 0 {
            return Err(ControllerError::InvalidOwnerTerm);
        }
        Ok(Self {
            catalog,
            data,
            owner_term,
        })
    }

    pub async fn reconcile(
        &self,
        migration_id: u128,
        now_unix_ms: u64,
    ) -> Result<ReconcileOutcome, ControllerError> {
        if now_unix_ms == 0 {
            return Err(ControllerError::InvalidTime);
        }
        let state = self.catalog.load().await?;
        let Some(migration) = state.migration(migration_id).cloned() else {
            return Ok(ReconcileOutcome::Missing { migration_id });
        };
        if self.owner_term < migration.owner_term() {
            return Err(ControllerError::ExpiredOwner {
                active: migration.owner_term(),
                actual: self.owner_term,
            });
        }
        match migration.state() {
            MigrationState::Preparing => {
                let graph = state
                    .graph(migration.graph_id())
                    .ok_or(ControllerError::MissingGraph(migration.graph_id()))?;
                self.data.ensure_target_learners(&migration, graph).await?;
                self.advance(
                    &state,
                    &migration,
                    MigrationState::Copying,
                    progress(&migration, self.owner_term, now_unix_ms)?,
                )
                .await
            }
            MigrationState::Copying => {
                let snapshot = self.data.copy_snapshot(&migration).await?;
                let progress = progress(&migration, self.owner_term, now_unix_ms)?
                    .with_snapshot(snapshot.index, snapshot.checksum)
                    .map_err(|error| ControllerError::Catalog(error.to_string()))?;
                self.advance(&state, &migration, MigrationState::CatchingUp, progress)
                    .await
            }
            MigrationState::CatchingUp => {
                let applied = self.data.catch_up(&migration).await?;
                let snapshot_index = migration
                    .snapshot_index()
                    .ok_or(ControllerError::MissingSnapshotFence)?;
                if applied < snapshot_index {
                    return Ok(ReconcileOutcome::Waiting {
                        migration_id,
                        state: migration.state(),
                    });
                }
                let progress = progress(&migration, self.owner_term, now_unix_ms)?
                    .with_catchup_index(applied)
                    .map_err(|error| ControllerError::Catalog(error.to_string()))?;
                self.advance(&state, &migration, MigrationState::Ready, progress)
                    .await
            }
            MigrationState::Ready => {
                self.advance(
                    &state,
                    &migration,
                    MigrationState::Committing,
                    progress(&migration, self.owner_term, now_unix_ms)?,
                )
                .await
            }
            MigrationState::Committing => {
                let cutover_index = self.data.commit_membership(&migration).await?;
                if cutover_index < migration.catchup_index() {
                    return Err(ControllerError::CutoverBehindCatchup {
                        catchup: migration.catchup_index(),
                        cutover: cutover_index,
                    });
                }
                let progress = progress(&migration, self.owner_term, now_unix_ms)?
                    .with_cutover_index(cutover_index)
                    .map_err(|error| ControllerError::Catalog(error.to_string()))?;
                let topology = target_topology(&state, &migration)?;
                let command = CatalogCommand::commit_migration(
                    command_id(&migration, MigrationState::Committed),
                    state.revision(),
                    migration_id,
                    migration.state_revision(),
                    topology,
                    progress,
                );
                self.catalog.propose(command).await?;
                Ok(advanced(&migration, MigrationState::Committed))
            }
            MigrationState::Committed => {
                self.data.activate_target_replicas(&migration).await?;
                if state.cleanup_is_pinned(
                    migration.graph_id(),
                    migration.shard_id(),
                    migration.source_epoch(),
                    now_unix_ms,
                ) {
                    return Ok(ReconcileOutcome::Waiting {
                        migration_id,
                        state: migration.state(),
                    });
                }
                if !self.data.cleanup_safe(&migration).await? {
                    return Ok(ReconcileOutcome::Waiting {
                        migration_id,
                        state: migration.state(),
                    });
                }
                self.advance(
                    &state,
                    &migration,
                    MigrationState::Cleaning,
                    progress(&migration, self.owner_term, now_unix_ms)?,
                )
                .await
            }
            MigrationState::Cleaning => {
                self.data.delete_source_replicas(&migration).await?;
                self.advance(
                    &state,
                    &migration,
                    MigrationState::Cleaned,
                    progress(&migration, self.owner_term, now_unix_ms)?,
                )
                .await
            }
            MigrationState::Aborting => {
                self.data.delete_target_learners(&migration).await?;
                self.advance(
                    &state,
                    &migration,
                    MigrationState::Aborted,
                    progress(&migration, self.owner_term, now_unix_ms)?,
                )
                .await
            }
            state @ (MigrationState::Cleaned | MigrationState::Aborted) => {
                Ok(ReconcileOutcome::Terminal {
                    migration_id,
                    state,
                })
            }
        }
    }

    async fn advance(
        &self,
        catalog: &CatalogState,
        migration: &MigrationRecord,
        next: MigrationState,
        progress: MigrationProgress,
    ) -> Result<ReconcileOutcome, ControllerError> {
        let command = CatalogCommand::advance_migration(
            command_id(migration, next),
            catalog.revision(),
            migration.migration_id(),
            migration.state_revision(),
            next,
            progress,
        );
        self.catalog.propose(command).await?;
        Ok(advanced(migration, next))
    }
}

fn advanced(migration: &MigrationRecord, to: MigrationState) -> ReconcileOutcome {
    ReconcileOutcome::Advanced {
        migration_id: migration.migration_id(),
        from: migration.state(),
        to,
    }
}

fn progress(
    migration: &MigrationRecord,
    owner_term: u64,
    now_unix_ms: u64,
) -> Result<MigrationProgress, ControllerError> {
    let mut progress = MigrationProgress::new(owner_term, now_unix_ms)
        .map_err(|error| ControllerError::Catalog(error.to_string()))?;
    if let (Some(index), Some(checksum)) =
        (migration.snapshot_index(), migration.snapshot_checksum())
    {
        progress = progress
            .with_snapshot(index, checksum)
            .map_err(|error| ControllerError::Catalog(error.to_string()))?;
    }
    if migration.catchup_index() > 0 {
        progress = progress
            .with_catchup_index(migration.catchup_index())
            .map_err(|error| ControllerError::Catalog(error.to_string()))?;
    }
    if migration.cutover_index() > 0 {
        progress = progress
            .with_cutover_index(migration.cutover_index())
            .map_err(|error| ControllerError::Catalog(error.to_string()))?;
    }
    Ok(progress)
}

fn target_topology(
    state: &CatalogState,
    migration: &MigrationRecord,
) -> Result<TopologyDefinition, ControllerError> {
    let graph = state
        .graph(migration.graph_id())
        .ok_or(ControllerError::MissingGraph(migration.graph_id()))?;
    if graph.topology().epoch() != migration.source_epoch() {
        return Err(ControllerError::TopologyChanged);
    }
    let placements = graph
        .topology()
        .placements()
        .iter()
        .map(|placement| {
            if placement.shard_id() == migration.shard_id() {
                Placement::new(
                    placement.shard_id(),
                    migration.target_epoch(),
                    migration.target_voters().to_vec(),
                )
            } else {
                Ok(placement.clone())
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| ControllerError::Catalog(error.to_string()))?;
    TopologyDefinition::new(
        graph.topology().mode(),
        graph.topology().route_seed(),
        graph.topology().virtual_partitions(),
        migration.target_epoch(),
        placements,
    )
    .map_err(|error| ControllerError::Catalog(error.to_string()))
}

fn command_id(migration: &MigrationRecord, state: MigrationState) -> u128 {
    let mut input = Vec::with_capacity(25);
    input.extend_from_slice(&migration.migration_id().to_be_bytes());
    input.extend_from_slice(&migration.state_revision().to_be_bytes());
    input.push(state_tag(state));
    let digest = blake3::hash(&input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    let value = u128::from_be_bytes(bytes);
    if value == 0 { 1 } else { value }
}

const fn state_tag(state: MigrationState) -> u8 {
    match state {
        MigrationState::Preparing => 1,
        MigrationState::Copying => 2,
        MigrationState::CatchingUp => 3,
        MigrationState::Ready => 4,
        MigrationState::Committing => 5,
        MigrationState::Committed => 6,
        MigrationState::Cleaning => 7,
        MigrationState::Cleaned => 8,
        MigrationState::Aborting => 9,
        MigrationState::Aborted => 10,
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControllerError {
    InvalidMigrationId,
    InvalidOwnerTerm,
    InvalidTime,
    ExpiredOwner { active: u64, actual: u64 },
    MissingGraph(u64),
    MissingBackendMigration(u128),
    ActiveBackendMigration(u64),
    BackendMigrationIdConflict(u128),
    BackendMigrationPastAbortFence(u128),
    MissingSnapshotFence,
    TopologyChanged,
    CutoverBehindCatchup { catchup: u64, cutover: u64 },
    Catalog(String),
    Data(String),
}

impl Display for ControllerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMigrationId => formatter.write_str("migration ID must be nonzero"),
            Self::InvalidOwnerTerm => formatter.write_str("Controller owner term must be nonzero"),
            Self::InvalidTime => formatter.write_str("reconciliation time must be nonzero"),
            Self::ExpiredOwner { active, actual } => write!(
                formatter,
                "Controller owner term {actual} is expired; active term is {active}"
            ),
            Self::MissingGraph(graph_id) => write!(formatter, "graph {graph_id} does not exist"),
            Self::MissingBackendMigration(migration_id) => {
                write!(
                    formatter,
                    "backend migration {migration_id:032x} does not exist"
                )
            }
            Self::ActiveBackendMigration(graph_id) => {
                write!(
                    formatter,
                    "graph {graph_id} already has an active backend migration"
                )
            }
            Self::BackendMigrationIdConflict(migration_id) => write!(
                formatter,
                "migration ID {migration_id:032x} belongs to a different backend migration"
            ),
            Self::BackendMigrationPastAbortFence(migration_id) => write!(
                formatter,
                "backend migration {migration_id:032x} has passed the abort fence"
            ),
            Self::MissingSnapshotFence => formatter.write_str("snapshot fence is missing"),
            Self::TopologyChanged => {
                formatter.write_str("source topology changed during migration")
            }
            Self::CutoverBehindCatchup { catchup, cutover } => write!(
                formatter,
                "cutover index {cutover} is behind catch-up index {catchup}"
            ),
            Self::Catalog(message) => write!(formatter, "Catalog error: {message}"),
            Self::Data(message) => write!(formatter, "Data error: {message}"),
        }
    }
}

impl Error for ControllerError {}
