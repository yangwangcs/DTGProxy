use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use dtg_execution::{
    ProviderKind, ProviderResolver, ReplicaBinding, ReplicaStateStore, ResolvedReplicaStore,
    StorageError, StoreFuture,
};
use dtg_storage_fjall::FjallReplicaStore;
use dtg_storage_kuzu::KuzuReplicaStore;
use dtg_storage_postgres::PostgresReplicaStore;
use dtg_storage_remote::{RemoteAuthToken, RemoteError, StorageRemoteClient};

use crate::{CredentialProfile, DataProcessConfig, EndpointProfile};

#[derive(Clone)]
struct Profiles {
    endpoints: Arc<BTreeMap<String, EndpointProfile>>,
    credentials: Arc<BTreeMap<String, CredentialProfile>>,
}

impl Profiles {
    fn from_config(config: &DataProcessConfig) -> Self {
        Self {
            endpoints: Arc::new(config.endpoint_profiles().clone()),
            credentials: Arc::new(config.credential_profiles().clone()),
        }
    }

    fn endpoint(&self, binding: &ReplicaBinding) -> Result<&EndpointProfile, StorageError> {
        self.endpoints
            .get(binding.endpoint_profile_ref())
            .ok_or_else(|| {
                StorageError::InvalidBinding(format!(
                    "unknown endpoint profile: {}",
                    binding.endpoint_profile_ref()
                ))
            })
    }

    fn credential(&self, binding: &ReplicaBinding) -> Result<&CredentialProfile, StorageError> {
        self.credentials
            .get(binding.credential_ref())
            .ok_or_else(|| {
                StorageError::InvalidBinding(format!(
                    "unknown credential profile: {}",
                    binding.credential_ref()
                ))
            })
    }
}

pub struct FjallResolver {
    root: PathBuf,
}

impl FjallResolver {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }
}

impl ProviderResolver for FjallResolver {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Fjall
    }

    fn open<'a>(&'a self, binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>> {
        Box::pin(async move {
            std::fs::create_dir_all(&self.root).map_err(|error| {
                StorageError::Internal(format!("cannot create Fjall data root: {error}"))
            })?;
            let path = self.root.join(binding.namespace_id().as_str());
            let store = FjallReplicaStore::open(path, binding)?;
            Ok(Arc::new(store) as Arc<dyn ReplicaStateStore>)
        })
    }

    fn open_runtime<'a>(
        &'a self,
        binding: ReplicaBinding,
    ) -> StoreFuture<'a, ResolvedReplicaStore> {
        Box::pin(async move {
            std::fs::create_dir_all(&self.root).map_err(|error| {
                StorageError::Internal(format!("cannot create Fjall data root: {error}"))
            })?;
            let path = self.root.join(binding.namespace_id().as_str());
            let store = Arc::new(FjallReplicaStore::open(path, binding)?);
            Ok(ResolvedReplicaStore::state_only(store.clone())
                .with_snapshot_runtime(store.clone(), store.clone())
                .with_pushdown(store))
        })
    }
}

pub struct PostgresResolver {
    profiles: Profiles,
}

impl PostgresResolver {
    pub fn from_config(config: &DataProcessConfig) -> Self {
        Self {
            profiles: Profiles::from_config(config),
        }
    }
}

impl ProviderResolver for PostgresResolver {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::PostgreSql
    }

    fn open<'a>(&'a self, binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>> {
        Box::pin(async move {
            let EndpointProfile::PostgreSql(endpoint) = self.profiles.endpoint(&binding)? else {
                return Err(StorageError::InvalidBinding(
                    "PostgreSQL binding resolved a non-PostgreSQL endpoint profile".into(),
                ));
            };
            let CredentialProfile::PostgreSql(credential) = self.profiles.credential(&binding)?
            else {
                return Err(StorageError::InvalidBinding(
                    "PostgreSQL binding resolved incompatible credentials".into(),
                ));
            };
            let connection_string = format!("{endpoint} {credential}");
            let store = PostgresReplicaStore::open(connection_string, binding).await?;
            Ok(Arc::new(store) as Arc<dyn ReplicaStateStore>)
        })
    }

    fn open_runtime<'a>(
        &'a self,
        binding: ReplicaBinding,
    ) -> StoreFuture<'a, ResolvedReplicaStore> {
        Box::pin(async move {
            let EndpointProfile::PostgreSql(endpoint) = self.profiles.endpoint(&binding)? else {
                return Err(StorageError::InvalidBinding(
                    "PostgreSQL binding resolved a non-PostgreSQL endpoint profile".into(),
                ));
            };
            let CredentialProfile::PostgreSql(credential) = self.profiles.credential(&binding)?
            else {
                return Err(StorageError::InvalidBinding(
                    "PostgreSQL binding resolved incompatible credentials".into(),
                ));
            };
            let store = Arc::new(
                PostgresReplicaStore::open(format!("{endpoint} {credential}"), binding).await?,
            );
            Ok(ResolvedReplicaStore::state_only(store.clone())
                .with_snapshot_runtime(store.clone(), store.clone())
                .with_pushdown(store))
        })
    }
}

pub struct KuzuResolver {
    root: PathBuf,
}

impl KuzuResolver {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            root: root.as_ref().to_path_buf(),
        }
    }
}

impl ProviderResolver for KuzuResolver {
    fn provider_kind(&self) -> ProviderKind {
        ProviderKind::Kuzu
    }

    fn open<'a>(&'a self, binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>> {
        Box::pin(async move {
            if binding.provider_kind() != &ProviderKind::Kuzu {
                return Err(StorageError::InvalidBinding(
                    "Kuzu resolver received a non-Kuzu binding".into(),
                ));
            }
            std::fs::create_dir_all(&self.root).map_err(|error| {
                StorageError::Internal(format!("cannot create Kuzu data root: {error}"))
            })?;
            let store =
                KuzuReplicaStore::open(self.root.join(binding.namespace_id().as_str()), binding)?;
            Ok(Arc::new(store) as Arc<dyn ReplicaStateStore>)
        })
    }

    fn open_runtime<'a>(
        &'a self,
        binding: ReplicaBinding,
    ) -> StoreFuture<'a, ResolvedReplicaStore> {
        Box::pin(async move {
            if binding.provider_kind() != &ProviderKind::Kuzu {
                return Err(StorageError::InvalidBinding(
                    "Kuzu resolver received a non-Kuzu binding".into(),
                ));
            }
            std::fs::create_dir_all(&self.root).map_err(|error| {
                StorageError::Internal(format!("cannot create Kuzu data root: {error}"))
            })?;
            let store = Arc::new(KuzuReplicaStore::open(
                self.root.join(binding.namespace_id().as_str()),
                binding,
            )?);
            Ok(ResolvedReplicaStore::state_only(store.clone())
                .with_snapshot_runtime(store.clone(), store.clone())
                .with_pushdown(store))
        })
    }
}

pub struct RemoteResolver {
    kind: ProviderKind,
    profiles: Profiles,
}

impl RemoteResolver {
    pub fn from_config(name: impl Into<String>, config: &DataProcessConfig) -> Self {
        Self {
            kind: ProviderKind::Remote(name.into()),
            profiles: Profiles::from_config(config),
        }
    }
}

impl ProviderResolver for RemoteResolver {
    fn provider_kind(&self) -> ProviderKind {
        self.kind.clone()
    }

    fn open<'a>(&'a self, binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>> {
        Box::pin(async move {
            let EndpointProfile::Remote(endpoint) = self.profiles.endpoint(&binding)? else {
                return Err(StorageError::InvalidBinding(
                    "remote binding resolved a non-remote endpoint profile".into(),
                ));
            };
            let CredentialProfile::RemoteSignedToken(secret) =
                self.profiles.credential(&binding)?
            else {
                return Err(StorageError::InvalidBinding(
                    "remote storage requires a binding-scoped signed-token credential".into(),
                ));
            };
            let store =
                StorageRemoteClient::connect(endpoint, binding, RemoteAuthToken::new(*secret))
                    .await
                    .map_err(remote_storage_error)?;
            Ok(Arc::new(store) as Arc<dyn ReplicaStateStore>)
        })
    }

    fn open_runtime<'a>(
        &'a self,
        binding: ReplicaBinding,
    ) -> StoreFuture<'a, ResolvedReplicaStore> {
        Box::pin(async move {
            let EndpointProfile::Remote(endpoint) = self.profiles.endpoint(&binding)? else {
                return Err(StorageError::InvalidBinding(
                    "remote binding resolved a non-remote endpoint profile".into(),
                ));
            };
            let CredentialProfile::RemoteSignedToken(secret) =
                self.profiles.credential(&binding)?
            else {
                return Err(StorageError::InvalidBinding(
                    "remote storage requires a binding-scoped signed-token credential".into(),
                ));
            };
            let store = Arc::new(
                StorageRemoteClient::connect(endpoint, binding, RemoteAuthToken::new(*secret))
                    .await
                    .map_err(remote_storage_error)?,
            );
            Ok(ResolvedReplicaStore::state_only(store.clone())
                .with_snapshot_runtime(store.clone(), store.clone())
                .with_pushdown(store))
        })
    }
}

fn remote_storage_error(error: RemoteError) -> StorageError {
    match error {
        RemoteError::Storage(error) => error,
        error => StorageError::Internal(error.to_string()),
    }
}
