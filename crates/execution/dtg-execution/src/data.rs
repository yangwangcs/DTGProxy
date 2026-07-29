use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use dtg_shard::{ReplicaKey, ShardError, ShardHost};
use dtg_storage::{
    ConsensusStore, NamespaceId, ProviderKind, ReplicaBinding, ReplicaStateStore, StorageError,
    StoreFuture,
};

pub trait ProviderResolver: Send + Sync {
    fn provider_kind(&self) -> ProviderKind;

    fn open<'a>(&'a self, binding: ReplicaBinding) -> StoreFuture<'a, Arc<dyn ReplicaStateStore>>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutionBuildError {
    MissingComponent(&'static str),
    DuplicateProvider(ProviderKind),
    ProviderKindDrift {
        registered: ProviderKind,
        resolver: ProviderKind,
    },
}

impl fmt::Display for ExecutionBuildError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingComponent(component) => {
                write!(formatter, "missing execution component: {component}")
            }
            Self::DuplicateProvider(kind) => {
                write!(formatter, "duplicate provider resolver: {kind:?}")
            }
            Self::ProviderKindDrift {
                registered,
                resolver,
            } => write!(
                formatter,
                "provider resolver kind drift: registered {registered:?}, resolver {resolver:?}"
            ),
        }
    }
}

impl std::error::Error for ExecutionBuildError {}

#[derive(Default)]
pub struct ProviderResolverSet {
    resolvers: BTreeMap<ProviderKind, Arc<dyn ProviderResolver>>,
    namespace_owners: Mutex<BTreeMap<NamespaceId, ReplicaBinding>>,
}

impl ProviderResolverSet {
    pub fn new() -> Self {
        Self::default()
    }

    fn insert(
        &mut self,
        kind: ProviderKind,
        resolver: Arc<dyn ProviderResolver>,
    ) -> Result<(), ExecutionBuildError> {
        let resolver_kind = resolver.provider_kind();
        if resolver_kind != kind {
            return Err(ExecutionBuildError::ProviderKindDrift {
                registered: kind,
                resolver: resolver_kind,
            });
        }
        if self.resolvers.contains_key(&kind) {
            return Err(ExecutionBuildError::DuplicateProvider(kind));
        }
        self.resolvers.insert(kind, resolver);
        Ok(())
    }

    pub fn provider_kinds(&self) -> Vec<ProviderKind> {
        self.resolvers.keys().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.resolvers.is_empty()
    }

    pub fn open(&self, binding: ReplicaBinding) -> StoreFuture<'_, Arc<dyn ReplicaStateStore>> {
        Box::pin(async move {
            let provider_kind = binding.provider_kind().clone();
            let resolver = self
                .resolvers
                .get(&provider_kind)
                .ok_or(StorageError::Unsupported)?;
            if resolver.provider_kind() != provider_kind {
                return Err(StorageError::InvalidBinding(
                    "provider resolver kind changed after registration".into(),
                ));
            }

            let store = resolver.open(binding.clone()).await?;
            if store.binding() != &binding {
                return Err(StorageError::StaleBinding {
                    expected: Box::new(binding),
                    actual: Box::new(store.binding().clone()),
                });
            }

            let mut owners = self.namespace_owners.lock().map_err(|_| {
                StorageError::Internal("namespace ownership registry is poisoned".into())
            })?;
            match owners.get(binding.namespace_id()) {
                Some(owner) if owner != &binding => {
                    return Err(StorageError::NamespaceOwnerMismatch {
                        expected: Box::new(owner.clone()),
                        actual: Box::new(binding),
                    });
                }
                Some(_) => {}
                None => {
                    owners.insert(binding.namespace_id().clone(), binding);
                }
            }
            Ok(store)
        })
    }
}

pub struct DataExecution {
    shards: ShardHost,
    providers: ProviderResolverSet,
}

impl DataExecution {
    pub fn builder() -> DataExecutionBuilder {
        DataExecutionBuilder::default()
    }

    pub fn provider_kinds(&self) -> Vec<ProviderKind> {
        self.providers.provider_kinds()
    }

    pub fn open_store(
        &self,
        binding: ReplicaBinding,
    ) -> StoreFuture<'_, Arc<dyn ReplicaStateStore>> {
        self.providers.open(binding)
    }

    pub fn add_replica(
        &mut self,
        consensus_store: Arc<dyn ConsensusStore>,
        state_store: Arc<dyn ReplicaStateStore>,
    ) -> Result<ReplicaKey, ShardError> {
        self.shards.add(consensus_store, state_store)
    }
}

pub struct DataExecutionBuilder {
    shards: ShardHost,
    providers: ProviderResolverSet,
    error: Option<ExecutionBuildError>,
}

impl Default for DataExecutionBuilder {
    fn default() -> Self {
        Self {
            shards: ShardHost::new(),
            providers: ProviderResolverSet::new(),
            error: None,
        }
    }
}

impl DataExecutionBuilder {
    pub fn with_shards(mut self, shards: ShardHost) -> Self {
        self.shards = shards;
        self
    }

    pub fn with_provider(
        mut self,
        kind: ProviderKind,
        resolver: Arc<dyn ProviderResolver>,
    ) -> Self {
        if self.error.is_none()
            && let Err(error) = self.providers.insert(kind, resolver)
        {
            self.error = Some(error);
        }
        self
    }

    pub fn build(self) -> Result<DataExecution, ExecutionBuildError> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.providers.is_empty() {
            return Err(ExecutionBuildError::MissingComponent("provider resolver"));
        }
        Ok(DataExecution {
            shards: self.shards,
            providers: self.providers,
        })
    }
}
