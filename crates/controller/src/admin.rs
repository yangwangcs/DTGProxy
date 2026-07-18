use std::collections::BTreeMap;

use control_plane::{
    BackendMigrationRecord, BackendMigrationState, BackendProfile, CatalogCommand,
};
use storage_api::AdapterRequirement;

use crate::{CatalogApi, ControllerError};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendTargetSpec {
    provider: String,
    public_parameters: BTreeMap<String, String>,
    secret_references: BTreeMap<String, String>,
}

impl BackendTargetSpec {
    pub fn new(
        provider: impl Into<String>,
        public_parameters: BTreeMap<String, String>,
        secret_references: BTreeMap<String, String>,
    ) -> Result<Self, ControllerError> {
        let provider = provider.into();
        BackendProfile::new(
            provider.clone(),
            public_parameters.clone(),
            secret_references.clone(),
            AdapterRequirement::HotPluggableReplica,
            1,
        )
        .map_err(|error| ControllerError::Catalog(error.to_string()))?;
        Ok(Self {
            provider,
            public_parameters,
            secret_references,
        })
    }

    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    #[must_use]
    pub const fn public_parameters(&self) -> &BTreeMap<String, String> {
        &self.public_parameters
    }

    #[must_use]
    pub const fn secret_references(&self) -> &BTreeMap<String, String> {
        &self.secret_references
    }
}

pub async fn start_backend_migration<C>(
    catalog: &C,
    migration_id: u128,
    graph_id: u64,
    target: BackendTargetSpec,
    owner_term: u64,
    now_unix_ms: u64,
) -> Result<BackendMigrationRecord, ControllerError>
where
    C: CatalogApi,
{
    if migration_id == 0 {
        return Err(ControllerError::InvalidMigrationId);
    }
    if owner_term == 0 {
        return Err(ControllerError::InvalidOwnerTerm);
    }
    if now_unix_ms == 0 {
        return Err(ControllerError::InvalidTime);
    }

    let state = catalog.load().await?;
    if let Some(existing) = state.backend_migration(migration_id) {
        if existing.graph_id() == graph_id && target_matches(existing.target(), &target) {
            return Ok(existing.clone());
        }
        return Err(ControllerError::BackendMigrationIdConflict(migration_id));
    }
    if state.active_backend_migration(graph_id).is_some() {
        return Err(ControllerError::ActiveBackendMigration(graph_id));
    }
    let graph = state
        .graph(graph_id)
        .ok_or(ControllerError::MissingGraph(graph_id))?;
    validate_target_for_graph(&target, graph)?;
    let generation = graph
        .backend()
        .generation()
        .checked_add(1)
        .ok_or_else(|| ControllerError::Catalog("backend generation exhausted".into()))?;
    let target_profile = BackendProfile::new(
        target.provider,
        target.public_parameters,
        target.secret_references,
        AdapterRequirement::HotPluggableReplica,
        generation,
    )
    .map_err(|error| ControllerError::Catalog(error.to_string()))?;
    let migration = BackendMigrationRecord::new(
        migration_id,
        graph_id,
        graph.backend().clone(),
        target_profile,
        owner_term,
        now_unix_ms,
    )
    .map_err(|error| ControllerError::Catalog(error.to_string()))?;
    catalog
        .propose(CatalogCommand::create_backend_migration(
            start_command_id(migration_id),
            state.revision(),
            migration.clone(),
        ))
        .await?;
    Ok(migration)
}

pub async fn abort_backend_migration<C>(
    catalog: &C,
    migration_id: u128,
    owner_term: u64,
    now_unix_ms: u64,
) -> Result<BackendMigrationRecord, ControllerError>
where
    C: CatalogApi,
{
    if migration_id == 0 {
        return Err(ControllerError::InvalidMigrationId);
    }
    if owner_term == 0 {
        return Err(ControllerError::InvalidOwnerTerm);
    }
    if now_unix_ms == 0 {
        return Err(ControllerError::InvalidTime);
    }
    let state = catalog.load().await?;
    let migration = state
        .backend_migration(migration_id)
        .cloned()
        .ok_or(ControllerError::MissingBackendMigration(migration_id))?;
    match migration.state() {
        BackendMigrationState::Aborting | BackendMigrationState::Aborted => {
            return Ok(migration);
        }
        BackendMigrationState::Committing
        | BackendMigrationState::CutOver
        | BackendMigrationState::Published
        | BackendMigrationState::SourceRetired => {
            return Err(ControllerError::BackendMigrationPastAbortFence(
                migration_id,
            ));
        }
        BackendMigrationState::Preparing
        | BackendMigrationState::Restored
        | BackendMigrationState::DualApplying
        | BackendMigrationState::Verified => {}
    }
    let command = CatalogCommand::advance_backend_migration(
        abort_command_id(migration_id),
        state.revision(),
        migration_id,
        migration.state_revision(),
        BackendMigrationState::Aborting,
        owner_term,
        now_unix_ms,
        Vec::new(),
    );
    catalog.propose(command).await?;
    let mut updated = state;
    updated
        .apply(CatalogCommand::advance_backend_migration(
            abort_command_id(migration_id),
            updated.revision(),
            migration_id,
            migration.state_revision(),
            BackendMigrationState::Aborting,
            owner_term,
            now_unix_ms,
            Vec::new(),
        ))
        .map_err(|error| ControllerError::Catalog(error.to_string()))?;
    updated
        .backend_migration(migration_id)
        .cloned()
        .ok_or(ControllerError::MissingBackendMigration(migration_id))
}

fn target_matches(profile: &BackendProfile, target: &BackendTargetSpec) -> bool {
    profile.provider() == target.provider
        && profile.public_parameters() == &target.public_parameters
        && profile.secret_references() == &target.secret_references
        && profile.requirement() == AdapterRequirement::HotPluggableReplica
}

fn validate_target_for_graph(
    target: &BackendTargetSpec,
    graph: &control_plane::GraphDefinition,
) -> Result<(), ControllerError> {
    match target.provider() {
        "rocksdb" => return Ok(()),
        "postgresql" | "neo4j" => {}
        "sidecar" => {
            if !target.public_parameters().contains_key("target_provider") {
                return Err(ControllerError::Catalog(
                    "sidecar backend requires public parameter target_provider".into(),
                ));
            }
        }
        provider => {
            return Err(ControllerError::Catalog(format!(
                "unsupported backend provider {provider}"
            )));
        }
    }
    for placement in graph.topology().placements() {
        let shard_key = format!("sidecar_endpoint.shard.{}", placement.shard_id());
        if !target.public_parameters().contains_key(&shard_key)
            && !target.public_parameters().contains_key("sidecar_endpoint")
        {
            return Err(ControllerError::Catalog(format!(
                "backend provider {} requires public parameter {shard_key} or sidecar_endpoint",
                target.provider()
            )));
        }
    }
    Ok(())
}

fn start_command_id(migration_id: u128) -> u128 {
    admin_command_id(b"backend-admin-start", migration_id)
}

fn abort_command_id(migration_id: u128) -> u128 {
    admin_command_id(b"backend-admin-abort", migration_id)
}

fn admin_command_id(domain: &[u8], migration_id: u128) -> u128 {
    let mut input = Vec::with_capacity(35);
    input.extend_from_slice(domain);
    input.extend_from_slice(&migration_id.to_be_bytes());
    let digest = blake3::hash(&input);
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    let value = u128::from_be_bytes(bytes);
    if value == 0 { 1 } else { value }
}
