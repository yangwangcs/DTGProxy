use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::path::{Component, Path};
use std::sync::Arc;

use adapter_registry::{AdapterOpenRequest, AdapterRegistry, HotSwapAdapter};
use adapter_rocksdb::RocksAdapterFactory;
use adapter_sidecar::TcpSidecarAdapterFactory;
use storage_api::AdapterRequirement;

use crate::{BackendProfile, BackendSlotState};

pub struct BackendManager {
    registry: AdapterRegistry,
}

impl BackendManager {
    pub fn production() -> Result<Self, BackendError> {
        let mut registry = AdapterRegistry::new();
        registry.register(Arc::new(RocksAdapterFactory))?;
        registry.register(Arc::new(TcpSidecarAdapterFactory))?;
        Ok(Self { registry })
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

    async fn open_profile(
        &self,
        replica_directory: &Path,
        profile: &BackendProfile,
    ) -> Result<adapter_registry::OpenedAdapter, BackendError> {
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
        self.registry
            .open(
                profile.provider(),
                &request,
                AdapterRequirement::HotPluggableReplica,
            )
            .await
            .map_err(BackendError::from)
    }
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
    NonLoopbackSidecarEndpoint { endpoint: SocketAddr },
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
