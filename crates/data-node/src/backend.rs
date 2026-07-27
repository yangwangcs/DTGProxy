use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::path::{Component, Path};
use std::sync::Arc;

use adapter_registry::{AdapterOpenRequest, AdapterRegistry, HotSwapAdapter};
use adapter_rocksdb::RocksAdapterFactory;
use adapter_sidecar::TcpSidecarAdapterFactory;
use storage_api::{AdapterRequirement, LogicalSnapshotExportRequest, StorageAdapter};

use crate::{BackendProfile, BackendSlotState, StartupBackend};

pub struct BackendManager {
    startup_backend: StartupBackend,
}

impl BackendManager {
    pub fn production(startup_backend: StartupBackend) -> Result<Self, BackendError> {
        Ok(Self { startup_backend })
    }

    pub fn validate_active_profile(&self, profile: &BackendProfile) -> Result<(), BackendError> {
        let actual = logical_backend(profile)?;
        if actual != self.startup_backend {
            return Err(BackendError::ActiveBackendMismatch {
                configured: self.startup_backend,
                profile: actual,
            });
        }
        Ok(())
    }

    pub fn validate_migration_target(&self, profile: &BackendProfile) -> Result<(), BackendError> {
        logical_backend(profile).map(|_| ())
    }

    pub async fn open_slot(
        &self,
        replica_directory: &Path,
        state: &BackendSlotState,
    ) -> Result<Arc<HotSwapAdapter>, BackendError> {
        match state {
            BackendSlotState::Active {
                generation,
                profile,
            } => {
                self.validate_active_profile(profile)?;
                let opened = self.open_profile(replica_directory, profile).await?;
                Ok(Arc::new(HotSwapAdapter::recover_active(
                    opened,
                    *generation,
                )?))
            }
            BackendSlotState::DualApplying {
                source_generation,
                source,
                target_generation,
                target,
                synchronized_index,
                ..
            } => {
                self.validate_active_profile(source)?;
                self.validate_migration_target(target)?;
                let active = self.open_profile(replica_directory, source).await?;
                let shadow = self.open_profile(replica_directory, target).await?;
                Ok(Arc::new(HotSwapAdapter::recover_dual_applying(
                    active,
                    *source_generation,
                    shadow,
                    *target_generation,
                    *synchronized_index,
                    AdapterRequirement::HotPluggableReplica,
                )?))
            }
        }
    }

    pub(crate) async fn restore_target(
        &self,
        replica_directory: &Path,
        source: Arc<dyn StorageAdapter>,
        target: &BackendProfile,
    ) -> Result<(adapter_registry::OpenedAdapter, u64), BackendError> {
        self.validate_migration_target(target)?;
        let source_index = source.applied_log_index()?;
        let reader = source
            .begin_logical_export(LogicalSnapshotExportRequest::default())
            .await?;
        if reader.header().applied_log_index() != source_index {
            return Err(BackendError::SnapshotFenceMismatch {
                source: source_index,
                snapshot: reader.header().applied_log_index(),
            });
        }
        let request = self.open_request(replica_directory, target)?;
        let registry = registry_for(target)?;
        match registry
            .restore(
                target.provider(),
                &request,
                AdapterRequirement::HotPluggableReplica,
                reader,
            )
            .await
        {
            Ok(opened) => Ok((opened, source_index)),
            Err(restore_error) => {
                let opened = registry
                    .open(
                        target.provider(),
                        &request,
                        AdapterRequirement::HotPluggableReplica,
                    )
                    .await
                    .map_err(|open_error| BackendError::RestoreAndOpen {
                        restore: restore_error.to_string(),
                        open: open_error.to_string(),
                    })?;
                let target_index = opened.adapter().applied_log_index()?;
                if target_index != source_index {
                    return Err(BackendError::SnapshotFenceMismatch {
                        source: source_index,
                        snapshot: target_index,
                    });
                }
                Ok((opened, source_index))
            }
        }
    }

    async fn open_profile(
        &self,
        replica_directory: &Path,
        profile: &BackendProfile,
    ) -> Result<adapter_registry::OpenedAdapter, BackendError> {
        let request = self.open_request(replica_directory, profile)?;
        registry_for(profile)?
            .open(
                profile.provider(),
                &request,
                AdapterRequirement::HotPluggableReplica,
            )
            .await
            .map_err(BackendError::from)
    }

    fn open_request(
        &self,
        replica_directory: &Path,
        profile: &BackendProfile,
    ) -> Result<AdapterOpenRequest, BackendError> {
        let mut request = AdapterOpenRequest::new(profile.instance_id());
        for (name, value) in profile.public_parameters() {
            let value = if profile.provider() == "rocksdb" && name == "path" {
                resolve_rocks_path(replica_directory, value)?
            } else {
                value.clone()
            };
            request = request.with_parameter(name, value);
        }
        if profile.provider() == "sidecar" {
            validate_sidecar_endpoint(profile)?;
        }
        Ok(request)
    }
}

fn logical_backend(profile: &BackendProfile) -> Result<StartupBackend, BackendError> {
    match profile.provider() {
        "rocksdb" => Ok(StartupBackend::Rocksdb),
        "sidecar" => match profile
            .public_parameters()
            .get("target_provider")
            .map(String::as_str)
        {
            Some("postgresql") => Ok(StartupBackend::Postgresql),
            Some("neo4j") => Ok(StartupBackend::Neo4j),
            _ => Err(BackendError::InvalidSidecarTargetProvider),
        },
        provider => Err(BackendError::UnsupportedProvider(provider.to_owned())),
    }
}

fn registry_for(profile: &BackendProfile) -> Result<AdapterRegistry, BackendError> {
    let mut registry = AdapterRegistry::new();
    match logical_backend(profile)? {
        StartupBackend::Rocksdb => registry.register(Arc::new(RocksAdapterFactory))?,
        StartupBackend::Postgresql | StartupBackend::Neo4j => {
            registry.register(Arc::new(TcpSidecarAdapterFactory))?;
        }
    }
    Ok(registry)
}

fn resolve_rocks_path(replica_directory: &Path, configured: &str) -> Result<String, BackendError> {
    let configured = Path::new(configured);
    if configured.is_absolute()
        || configured.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
                    | Component::CurDir
            )
        })
    {
        return Err(BackendError::UnsafeRocksPath);
    }
    Ok(replica_directory
        .join(configured)
        .to_string_lossy()
        .into_owned())
}

fn validate_sidecar_endpoint(profile: &BackendProfile) -> Result<(), BackendError> {
    let endpoint = profile
        .public_parameters()
        .get("endpoint")
        .ok_or(BackendError::MissingSidecarEndpoint)?
        .parse::<SocketAddr>()
        .map_err(|_| BackendError::InvalidSidecarEndpoint)?;
    if !endpoint.ip().is_loopback() {
        return Err(BackendError::NonLoopbackSidecarEndpoint { endpoint });
    }
    Ok(())
}

#[derive(Debug)]
pub enum BackendError {
    Registry(adapter_registry::RegistryError),
    Migration(adapter_registry::MigrationError),
    UnsafeRocksPath,
    MissingSidecarEndpoint,
    InvalidSidecarEndpoint,
    NonLoopbackSidecarEndpoint {
        endpoint: SocketAddr,
    },
    Adapter(storage_api::AdapterError),
    SnapshotFenceMismatch {
        source: u64,
        snapshot: u64,
    },
    RestoreAndOpen {
        restore: String,
        open: String,
    },
    UnsupportedProvider(String),
    InvalidSidecarTargetProvider,
    ActiveBackendMismatch {
        configured: StartupBackend,
        profile: StartupBackend,
    },
}

impl Display for BackendError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => Display::fmt(error, formatter),
            Self::Migration(error) => Display::fmt(error, formatter),
            Self::UnsafeRocksPath => {
                formatter.write_str("RocksDB backend path must stay inside the Replica directory")
            }
            Self::MissingSidecarEndpoint => formatter.write_str("Sidecar endpoint is missing"),
            Self::InvalidSidecarEndpoint => formatter.write_str("Sidecar endpoint is invalid"),
            Self::NonLoopbackSidecarEndpoint { endpoint } => write!(
                formatter,
                "plaintext Sidecar endpoint {endpoint} is not loopback"
            ),
            Self::Adapter(error) => Display::fmt(error, formatter),
            Self::SnapshotFenceMismatch { source, snapshot } => write!(
                formatter,
                "source applied index {source} differs from target snapshot index {snapshot}"
            ),
            Self::RestoreAndOpen { restore, open } => write!(
                formatter,
                "target restore failed ({restore}) and idempotent open failed ({open})"
            ),
            Self::UnsupportedProvider(provider) => {
                write!(formatter, "unsupported backend provider {provider}")
            }
            Self::InvalidSidecarTargetProvider => formatter
                .write_str("Sidecar backend must declare target_provider as postgresql or neo4j"),
            Self::ActiveBackendMismatch {
                configured,
                profile,
            } => write!(
                formatter,
                "configured startup backend {} does not match active profile backend {}",
                configured.name(),
                profile.name()
            ),
        }
    }
}

impl Error for BackendError {}

impl From<adapter_registry::RegistryError> for BackendError {
    fn from(error: adapter_registry::RegistryError) -> Self {
        Self::Registry(error)
    }
}

impl From<adapter_registry::MigrationError> for BackendError {
    fn from(error: adapter_registry::MigrationError) -> Self {
        Self::Migration(error)
    }
}

impl From<storage_api::AdapterError> for BackendError {
    fn from(error: storage_api::AdapterError) -> Self {
        Self::Adapter(error)
    }
}
