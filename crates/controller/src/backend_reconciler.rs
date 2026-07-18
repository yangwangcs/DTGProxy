use control_plane::{
    BackendMigrationRecord, BackendMigrationState, BackendReplicaReceipt, CatalogCommand,
    CatalogState, GraphDefinition,
};

use crate::{CatalogApi, ControllerError};

pub trait BackendDataPlaneApi {
    async fn prepare_target(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Vec<BackendReplicaReceipt>, ControllerError>;

    async fn begin_dual_apply(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Vec<BackendReplicaReceipt>, ControllerError>;

    async fn verify_dual_apply(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Option<Vec<BackendReplicaReceipt>>, ControllerError>;

    async fn cutover(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Vec<BackendReplicaReceipt>, ControllerError>;

    async fn retire_source(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<Vec<BackendReplicaReceipt>, ControllerError>;

    async fn abort(
        &self,
        migration: &BackendMigrationRecord,
        graph: &GraphDefinition,
    ) -> Result<(), ControllerError>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendReconcileOutcome {
    Advanced {
        migration_id: u128,
        from: BackendMigrationState,
        to: BackendMigrationState,
    },
    Waiting {
        migration_id: u128,
        state: BackendMigrationState,
    },
    Terminal {
        migration_id: u128,
        state: BackendMigrationState,
    },
    Missing {
        migration_id: u128,
    },
}

pub struct BackendReconciler<C, D> {
    catalog: C,
    data: D,
    owner_term: u64,
}

impl<C, D> BackendReconciler<C, D>
where
    C: CatalogApi,
    D: BackendDataPlaneApi,
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
    ) -> Result<BackendReconcileOutcome, ControllerError> {
        if now_unix_ms == 0 {
            return Err(ControllerError::InvalidTime);
        }
        let state = self.catalog.load().await?;
        let Some(migration) = state.backend_migration(migration_id).cloned() else {
            return Ok(BackendReconcileOutcome::Missing { migration_id });
        };
        if self.owner_term < migration.owner_term() {
            return Err(ControllerError::ExpiredOwner {
                active: migration.owner_term(),
                actual: self.owner_term,
            });
        }
        let graph = state
            .graph(migration.graph_id())
            .ok_or(ControllerError::MissingGraph(migration.graph_id()))?;
        match migration.state() {
            BackendMigrationState::Preparing => {
                let receipts = self.data.prepare_target(&migration, graph).await?;
                self.advance(
                    &state,
                    &migration,
                    BackendMigrationState::Restored,
                    now_unix_ms,
                    receipts,
                )
                .await
            }
            BackendMigrationState::Restored => {
                let receipts = self.data.begin_dual_apply(&migration, graph).await?;
                self.advance(
                    &state,
                    &migration,
                    BackendMigrationState::DualApplying,
                    now_unix_ms,
                    receipts,
                )
                .await
            }
            BackendMigrationState::DualApplying => {
                let Some(receipts) = self.data.verify_dual_apply(&migration, graph).await? else {
                    return Ok(BackendReconcileOutcome::Waiting {
                        migration_id,
                        state: migration.state(),
                    });
                };
                self.advance(
                    &state,
                    &migration,
                    BackendMigrationState::Verified,
                    now_unix_ms,
                    receipts,
                )
                .await
            }
            BackendMigrationState::Verified => {
                let receipts = self.data.cutover(&migration, graph).await?;
                self.advance(
                    &state,
                    &migration,
                    BackendMigrationState::CutOver,
                    now_unix_ms,
                    receipts,
                )
                .await
            }
            BackendMigrationState::CutOver => {
                let command = CatalogCommand::publish_backend_migration(
                    command_id(&migration, BackendMigrationState::Published),
                    state.revision(),
                    migration_id,
                    migration.state_revision(),
                    self.owner_term,
                    now_unix_ms,
                );
                self.catalog.propose(command).await?;
                Ok(advanced(&migration, BackendMigrationState::Published))
            }
            BackendMigrationState::Published => {
                let receipts = self.data.retire_source(&migration, graph).await?;
                self.advance(
                    &state,
                    &migration,
                    BackendMigrationState::SourceRetired,
                    now_unix_ms,
                    receipts,
                )
                .await
            }
            BackendMigrationState::Aborting => {
                self.data.abort(&migration, graph).await?;
                self.advance(
                    &state,
                    &migration,
                    BackendMigrationState::Aborted,
                    now_unix_ms,
                    Vec::new(),
                )
                .await
            }
            terminal @ (BackendMigrationState::SourceRetired | BackendMigrationState::Aborted) => {
                Ok(BackendReconcileOutcome::Terminal {
                    migration_id,
                    state: terminal,
                })
            }
        }
    }

    async fn advance(
        &self,
        catalog: &CatalogState,
        migration: &BackendMigrationRecord,
        next: BackendMigrationState,
        now_unix_ms: u64,
        receipts: Vec<BackendReplicaReceipt>,
    ) -> Result<BackendReconcileOutcome, ControllerError> {
        let command = CatalogCommand::advance_backend_migration(
            command_id(migration, next),
            catalog.revision(),
            migration.migration_id(),
            migration.state_revision(),
            next,
            self.owner_term,
            now_unix_ms,
            receipts,
        );
        self.catalog.propose(command).await?;
        Ok(advanced(migration, next))
    }
}

fn advanced(
    migration: &BackendMigrationRecord,
    to: BackendMigrationState,
) -> BackendReconcileOutcome {
    BackendReconcileOutcome::Advanced {
        migration_id: migration.migration_id(),
        from: migration.state(),
        to,
    }
}

fn command_id(migration: &BackendMigrationRecord, state: BackendMigrationState) -> u128 {
    let mut input = Vec::with_capacity(32);
    input.extend_from_slice(b"backend-migration");
    input.extend_from_slice(&migration.migration_id().to_be_bytes());
    input.extend_from_slice(&migration.state_revision().to_be_bytes());
    input.push(state_tag(state));
    let digest = blake3::hash(&input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    let value = u128::from_be_bytes(bytes);
    if value == 0 { 1 } else { value }
}

const fn state_tag(state: BackendMigrationState) -> u8 {
    match state {
        BackendMigrationState::Preparing => 1,
        BackendMigrationState::Restored => 2,
        BackendMigrationState::DualApplying => 3,
        BackendMigrationState::Verified => 4,
        BackendMigrationState::CutOver => 5,
        BackendMigrationState::Published => 6,
        BackendMigrationState::SourceRetired => 7,
        BackendMigrationState::Aborting => 8,
        BackendMigrationState::Aborted => 9,
    }
}
