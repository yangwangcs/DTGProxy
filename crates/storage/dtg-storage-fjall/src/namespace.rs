use std::{
    collections::BTreeMap,
    fs,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
};

#[cfg(feature = "tck")]
use std::sync::Condvar;

use dtg_storage::{BackendClass, CapabilityManifest, ProviderKind, ReplicaBinding, StorageError};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use crate::codec::{decode_binding, encode_binding};

pub(crate) const OWNER_KEY: &[u8] = b"binding";
pub(crate) const SNAPSHOT_ACTIVATION_KEY: &[u8] = b"snapshot_activation";
pub(crate) const SNAPSHOT_INSTALL_KEY: &[u8] = b"snapshot_install";
pub(crate) const SNAPSHOT_RESTORE_IN_PROGRESS_KEY: &[u8] = b"snapshot_restore_in_progress";
pub(crate) const FJALL_CONTRACT_VERSION: u32 = 1;
pub(crate) const FJALL_LAYOUT_VERSION: u32 = 1;

static OPEN_NAMESPACES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<NamespaceInner>>>> = OnceLock::new();

pub(crate) struct NamespaceInner {
    binding: Mutex<ReplicaBinding>,
    pub(crate) db: Database,
    pub(crate) owner: Keyspace,
    pub(crate) identity: Keyspace,
    pub(crate) current_vertex: Keyspace,
    pub(crate) current_edge: Keyspace,
    pub(crate) history: Keyspace,
    pub(crate) adjacency_out: Keyspace,
    pub(crate) adjacency_in: Keyspace,
    pub(crate) temporal_index: Keyspace,
    pub(crate) transaction: Keyspace,
    pub(crate) replica_meta: Keyspace,
    pub(crate) snapshot_stage: Keyspace,
    pub(crate) artifact: Keyspace,
    pub(crate) raft_log: Keyspace,
    pub(crate) raft_state: Keyspace,
    pub(crate) raft_snapshot: Keyspace,
    pub(crate) graph_guard: GraphGuard,
    pub(crate) consensus_guard: Mutex<()>,
    pub(crate) artifact_guard: Mutex<()>,
}

#[derive(Clone)]
pub(crate) struct NamespaceDb(Arc<NamespaceInner>);

impl Deref for NamespaceDb {
    type Target = NamespaceInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl NamespaceDb {
    pub(crate) fn open(path: &Path, binding: &ReplicaBinding) -> Result<Self, StorageError> {
        validate_fjall_binding(binding)?;
        let path = canonical_namespace_path(path)?;
        let registry = OPEN_NAMESPACES.get_or_init(|| Mutex::new(BTreeMap::new()));
        let mut registry = registry
            .lock()
            .map_err(|_| StorageError::Internal("Fjall namespace registry is poisoned".into()))?;
        if let Some(shared) = registry.get(&path).and_then(Weak::upgrade) {
            let shared_binding = shared
                .binding
                .lock()
                .map_err(|_| StorageError::Internal("Fjall namespace binding is poisoned".into()))?
                .clone();
            if shared_binding == *binding {
                return Ok(Self(shared));
            }
            return Err(StorageError::NamespaceOwnerMismatch {
                expected: Box::new(shared_binding),
                actual: Box::new(binding.clone()),
            });
        }

        let db = Database::builder(&path).open().map_err(fjall_error)?;
        let owner = db
            .keyspace("owner", KeyspaceCreateOptions::default)
            .map_err(fjall_error)?;

        match owner.get(OWNER_KEY).map_err(fjall_error)? {
            Some(bytes) => {
                let actual = decode_binding(&bytes)?;
                if actual != *binding {
                    return Err(StorageError::NamespaceOwnerMismatch {
                        expected: Box::new(actual),
                        actual: Box::new(binding.clone()),
                    });
                }
            }
            None => {
                owner
                    .insert(OWNER_KEY, encode_binding(binding)?)
                    .map_err(fjall_error)?;
                db.persist(PersistMode::SyncAll).map_err(fjall_error)?;
            }
        }

        let open = |name| {
            db.keyspace(name, KeyspaceCreateOptions::default)
                .map_err(fjall_error)
        };
        let identity = open("identity")?;
        let current_vertex = open("current_vertex")?;
        let current_edge = open("current_edge")?;
        let history = open("history")?;
        let adjacency_out = open("adjacency_out")?;
        let adjacency_in = open("adjacency_in")?;
        let temporal_index = open("temporal_index")?;
        let transaction = open("transaction")?;
        let replica_meta = open("replica_meta")?;
        let snapshot_stage = open("snapshot_stage")?;
        let artifact = open("artifact")?;
        let raft_log = open("raft_log")?;
        let raft_state = open("raft_state")?;
        let raft_snapshot = open("raft_snapshot")?;
        let shared = Arc::new(NamespaceInner {
            binding: Mutex::new(binding.clone()),
            db,
            owner,
            identity,
            current_vertex,
            current_edge,
            history,
            adjacency_out,
            adjacency_in,
            temporal_index,
            transaction,
            replica_meta,
            snapshot_stage,
            artifact,
            raft_log,
            raft_state,
            raft_snapshot,
            graph_guard: GraphGuard::new(),
            consensus_guard: Mutex::new(()),
            artifact_guard: Mutex::new(()),
        });
        registry.insert(path, Arc::downgrade(&shared));
        Ok(Self(shared))
    }

    pub(crate) fn ensure_binding(&self, expected: &ReplicaBinding) -> Result<(), StorageError> {
        let actual = self
            .owner
            .get(OWNER_KEY)
            .map_err(fjall_error)?
            .ok_or_else(|| StorageError::Internal("missing namespace owner".into()))
            .and_then(|bytes| decode_binding(&bytes))?;
        if &actual == expected {
            Ok(())
        } else {
            Err(StorageError::NamespaceOwnerMismatch {
                expected: Box::new(actual),
                actual: Box::new(expected.clone()),
            })
        }
    }

    pub(crate) fn rebind(
        &self,
        expected: &ReplicaBinding,
        active: &ReplicaBinding,
    ) -> Result<(), StorageError> {
        let mut binding = self
            .binding
            .lock()
            .map_err(|_| StorageError::Internal("Fjall namespace binding is poisoned".into()))?;
        if *binding != *expected {
            return Err(StorageError::NamespaceOwnerMismatch {
                expected: Box::new(binding.clone()),
                actual: Box::new(expected.clone()),
            });
        }
        *binding = active.clone();
        Ok(())
    }
}

pub(crate) struct GraphGuard {
    mutex: Mutex<()>,
    #[cfg(feature = "tck")]
    waiters: Mutex<usize>,
    #[cfg(feature = "tck")]
    waiters_changed: Condvar,
}

impl GraphGuard {
    fn new() -> Self {
        Self {
            mutex: Mutex::new(()),
            #[cfg(feature = "tck")]
            waiters: Mutex::new(0),
            #[cfg(feature = "tck")]
            waiters_changed: Condvar::new(),
        }
    }

    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, ()>, StorageError> {
        #[cfg(feature = "tck")]
        {
            let mut waiters = self.waiters.lock().map_err(|_| {
                StorageError::Internal("Fjall graph waiter state is poisoned".into())
            })?;
            *waiters += 1;
            self.waiters_changed.notify_all();
        }

        let guard = self.mutex.lock();

        #[cfg(feature = "tck")]
        {
            let mut waiters = self.waiters.lock().map_err(|_| {
                StorageError::Internal("Fjall graph waiter state is poisoned".into())
            })?;
            *waiters -= 1;
            self.waiters_changed.notify_all();
        }

        guard.map_err(|_| StorageError::Internal("Fjall graph lock is poisoned".into()))
    }

    #[cfg(feature = "tck")]
    pub(crate) fn wait_for_waiter(&self) -> Result<(), StorageError> {
        let mut waiters = self
            .waiters
            .lock()
            .map_err(|_| StorageError::Internal("Fjall graph waiter state is poisoned".into()))?;
        while *waiters == 0 {
            waiters = self.waiters_changed.wait(waiters).map_err(|_| {
                StorageError::Internal("Fjall graph waiter state is poisoned".into())
            })?;
        }
        Ok(())
    }
}

pub(crate) fn fjall_capabilities() -> Result<CapabilityManifest, StorageError> {
    CapabilityManifest::from_names([
        "adjacency",
        "immutable-read-view",
        "logical-snapshot",
        "point",
    ])
}

fn validate_fjall_binding(binding: &ReplicaBinding) -> Result<(), StorageError> {
    let capabilities = fjall_capabilities()?;
    let class = BackendClass::new(
        ProviderKind::Fjall,
        FJALL_CONTRACT_VERSION,
        FJALL_LAYOUT_VERSION,
        capabilities.names().map(str::to_owned),
    )?;
    if binding.provider_kind() != &ProviderKind::Fjall
        || binding.contract_version() != FJALL_CONTRACT_VERSION
        || binding.layout_version() != FJALL_LAYOUT_VERSION
        || binding.capability_digest() != capabilities.digest()
        || binding.backend_class_digest() != class.digest()
    {
        return Err(StorageError::InvalidBinding(
            "binding does not match the supported Fjall backend class and capability floor".into(),
        ));
    }
    Ok(())
}

fn canonical_namespace_path(path: &Path) -> Result<PathBuf, StorageError> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| StorageError::Internal(format!("cannot resolve Fjall path: {error}")))?
            .join(path)
    };
    let canonical = if absolute.exists() {
        fs::canonicalize(&absolute).map_err(|error| {
            StorageError::Internal(format!("cannot canonicalize Fjall path: {error}"))
        })?
    } else {
        let final_component = absolute.file_name().ok_or_else(|| {
            StorageError::InvalidBinding("Fjall namespace path has no final component".into())
        })?;
        let parent = absolute.parent().ok_or_else(|| {
            StorageError::InvalidBinding("Fjall namespace path has no parent".into())
        })?;
        let canonical_parent = fs::canonicalize(parent).map_err(|error| {
            StorageError::InvalidBinding(format!(
                "Fjall namespace parent must already exist and resolve safely: {error}"
            ))
        })?;
        canonical_parent.join(final_component)
    };
    if canonical.parent() == Some(canonical.as_path()) {
        return Err(StorageError::InvalidBinding(
            "Fjall namespace path cannot resolve to a filesystem root".into(),
        ));
    }
    Ok(canonical)
}

pub(crate) fn fjall_error(error: fjall::Error) -> StorageError {
    StorageError::Internal(format!("Fjall storage error: {error}"))
}
