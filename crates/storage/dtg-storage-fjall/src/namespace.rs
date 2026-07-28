use std::{
    collections::BTreeMap,
    ops::Deref,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock, Weak},
};

use dtg_storage::{ReplicaBinding, StorageError};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use crate::codec::{decode_binding, encode_binding};

const OWNER_KEY: &[u8] = b"binding";

static OPEN_NAMESPACES: OnceLock<Mutex<BTreeMap<PathBuf, Weak<NamespaceInner>>>> = OnceLock::new();

pub(crate) struct NamespaceInner {
    binding: ReplicaBinding,
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
        let path = absolute_path(path)?;
        let registry = OPEN_NAMESPACES.get_or_init(|| Mutex::new(BTreeMap::new()));
        let mut registry = registry
            .lock()
            .map_err(|_| StorageError::Internal("Fjall namespace registry is poisoned".into()))?;
        if let Some(shared) = registry.get(&path).and_then(Weak::upgrade) {
            if shared.binding == *binding {
                return Ok(Self(shared));
            }
            return Err(StorageError::NamespaceOwnerMismatch {
                expected: Box::new(shared.binding.clone()),
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
            binding: binding.clone(),
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
        });
        registry.insert(path, Arc::downgrade(&shared));
        Ok(Self(shared))
    }
}

fn absolute_path(path: &Path) -> Result<PathBuf, StorageError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        std::env::current_dir()
            .map(|current| current.join(path))
            .map_err(|error| StorageError::Internal(format!("cannot resolve Fjall path: {error}")))
    }
}

pub(crate) fn fjall_error(error: fjall::Error) -> StorageError {
    StorageError::Internal(format!("Fjall storage error: {error}"))
}
