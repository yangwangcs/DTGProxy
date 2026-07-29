use std::{collections::BTreeMap, fmt, path::Path, sync::MutexGuard};

use dtg_storage::{
    ConsensusEntry, ConsensusSnapshotInstall, ConsensusSnapshotMetadata, ConsensusStore,
    RaftHardState, RaftMembership, ReplicaBinding, StorageError, StoreFuture,
};
use fjall::PersistMode;

use crate::{
    codec::{
        decode_consensus_entry, decode_consensus_snapshot, decode_consensus_snapshot_install,
        decode_hard_state, decode_membership, encode_consensus_entry, encode_consensus_snapshot,
        encode_consensus_snapshot_install, encode_hard_state, encode_membership,
    },
    namespace::{NamespaceDb, fjall_error},
};

const HARD_STATE_KEY: &[u8] = b"hard_state";
const MEMBERSHIP_KEY: &[u8] = b"membership";
const SNAPSHOT_KEY: &[u8] = b"snapshot";
const SNAPSHOT_INSTALL_KEY: &[u8] = b"snapshot_install";

pub struct FjallConsensusStore {
    namespace: NamespaceDb,
    binding: ReplicaBinding,
}

impl fmt::Debug for FjallConsensusStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FjallConsensusStore")
            .field("binding", &self.binding)
            .finish_non_exhaustive()
    }
}

impl FjallConsensusStore {
    pub fn open(path: impl AsRef<Path>, binding: ReplicaBinding) -> Result<Self, StorageError> {
        Ok(Self {
            namespace: NamespaceDb::open(path.as_ref(), &binding)?,
            binding,
        })
    }

    fn ensure_binding(&self) -> Result<(), StorageError> {
        let persisted = self
            .namespace
            .owner
            .get(b"binding")
            .map_err(fjall_error)?
            .ok_or_else(|| StorageError::Internal("missing namespace owner".into()))?;
        let actual = crate::codec::decode_binding(&persisted)?;
        if actual == self.binding {
            Ok(())
        } else {
            Err(StorageError::NamespaceOwnerMismatch {
                expected: Box::new(actual),
                actual: Box::new(self.binding.clone()),
            })
        }
    }
}

impl ConsensusStore for FjallConsensusStore {
    fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    fn append(&self, entries: Vec<ConsensusEntry>) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            if entries
                .windows(2)
                .any(|pair| pair[1].index() != pair[0].index().saturating_add(1))
            {
                return Err(StorageError::InvalidConsensus(
                    "consensus append entries must be contiguous".into(),
                ));
            }
            if entries.is_empty() {
                return Ok(());
            }
            let stored = self.load_log()?;
            if let Some((first, last)) = stored.keys().next().zip(stored.keys().next_back())
                && (entries[0].index() < *first || entries[0].index() > last.saturating_add(1))
            {
                return Err(StorageError::InvalidConsensus(
                    "consensus append would create a discontiguous log".into(),
                ));
            }
            let conflict = entries
                .iter()
                .find_map(|entry| match stored.get(&entry.index()) {
                    Some(existing) if existing == entry => None,
                    Some(_) | None => Some(entry.index()),
                });
            let Some(conflict) = conflict else {
                return Ok(());
            };
            if let Some(last) = stored.keys().next_back()
                && conflict > last.saturating_add(1)
            {
                return Err(StorageError::InvalidConsensus(
                    "consensus append would create a log gap".into(),
                ));
            }
            let mut batch = self
                .namespace
                .db
                .batch()
                .durability(Some(PersistMode::SyncAll));
            for index in stored.range(conflict..).map(|(index, _)| *index) {
                batch.remove(&self.namespace.raft_log, index.to_be_bytes());
            }
            for entry in entries
                .into_iter()
                .filter(|entry| entry.index() >= conflict)
            {
                batch.insert(
                    &self.namespace.raft_log,
                    entry.index().to_be_bytes(),
                    encode_consensus_entry(&entry)?,
                );
            }
            batch.commit().map_err(fjall_error)
        })
    }

    fn entries(&self, low: u64, high: u64, max_bytes: u64) -> StoreFuture<'_, Vec<ConsensusEntry>> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            if low > high || max_bytes == 0 {
                return Err(StorageError::InvalidConsensus(
                    "invalid consensus entry range".into(),
                ));
            }
            let mut used = 0_u64;
            let mut rows = Vec::new();
            let mut previous = None;
            for item in self
                .namespace
                .raft_log
                .range(low.to_be_bytes()..high.to_be_bytes())
            {
                let (key, value) = item.into_inner().map_err(fjall_error)?;
                let size = value.len() as u64;
                if used.saturating_add(size) > max_bytes {
                    break;
                }
                let index = decode_log_index(&key)?;
                let entry = decode_consensus_entry(&value)?;
                if entry.index() != index || previous.is_some_and(|previous| index != previous + 1)
                {
                    return Err(StorageError::InvalidConsensus(
                        "stored consensus log is malformed or discontiguous".into(),
                    ));
                }
                previous = Some(index);
                rows.push(entry);
                used += size;
            }
            Ok(rows)
        })
    }

    fn truncate_suffix(&self, from_index: u64) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            if from_index == 0 {
                return Err(StorageError::InvalidConsensus(
                    "consensus truncation index must be nonzero".into(),
                ));
            }
            let keys = self
                .namespace
                .raft_log
                .range(from_index.to_be_bytes()..)
                .map(|item| item.key().map(|key| key.to_vec()).map_err(fjall_error))
                .collect::<Result<Vec<_>, _>>()?;
            let mut batch = self
                .namespace
                .db
                .batch()
                .durability(Some(PersistMode::SyncAll));
            for key in keys {
                batch.remove(&self.namespace.raft_log, key);
            }
            batch.commit().map_err(fjall_error)
        })
    }

    fn hard_state(&self) -> StoreFuture<'_, RaftHardState> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            self.namespace
                .raft_state
                .get(HARD_STATE_KEY)
                .map_err(fjall_error)?
                .map(|bytes| decode_hard_state(&bytes))
                .transpose()
                .map(Option::unwrap_or_default)
        })
    }

    fn set_hard_state(&self, state: RaftHardState) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            self.namespace
                .raft_state
                .insert(HARD_STATE_KEY, encode_hard_state(state)?)
                .map_err(fjall_error)?;
            self.namespace
                .db
                .persist(PersistMode::SyncAll)
                .map_err(fjall_error)
        })
    }

    fn membership(&self) -> StoreFuture<'_, RaftMembership> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            self.namespace
                .raft_state
                .get(MEMBERSHIP_KEY)
                .map_err(fjall_error)?
                .map(|bytes| decode_membership(&bytes))
                .transpose()?
                .ok_or(StorageError::NotFound)
        })
    }

    fn set_membership(&self, membership: RaftMembership) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            self.namespace
                .raft_state
                .insert(MEMBERSHIP_KEY, encode_membership(&membership)?)
                .map_err(fjall_error)?;
            self.namespace
                .db
                .persist(PersistMode::SyncAll)
                .map_err(fjall_error)
        })
    }

    fn snapshot_metadata(&self) -> StoreFuture<'_, Option<ConsensusSnapshotMetadata>> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            self.namespace
                .raft_snapshot
                .get(SNAPSHOT_KEY)
                .map_err(fjall_error)?
                .map(|bytes| decode_consensus_snapshot(&bytes))
                .transpose()
        })
    }

    fn set_snapshot_metadata(&self, metadata: ConsensusSnapshotMetadata) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            self.namespace
                .raft_snapshot
                .insert(SNAPSHOT_KEY, encode_consensus_snapshot(&metadata)?)
                .map_err(fjall_error)?;
            self.namespace
                .db
                .persist(PersistMode::SyncAll)
                .map_err(fjall_error)
        })
    }

    fn snapshot_install(&self) -> StoreFuture<'_, Option<ConsensusSnapshotInstall>> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            self.namespace
                .raft_snapshot
                .get(SNAPSHOT_INSTALL_KEY)
                .map_err(fjall_error)?
                .map(|bytes| decode_consensus_snapshot_install(&bytes))
                .transpose()
        })
    }

    fn stage_snapshot_install(&self, install: ConsensusSnapshotInstall) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            if !same_replica_generation(install.active_binding(), &self.binding) {
                return Err(StorageError::InvalidConsensus(
                    "snapshot install journal binding differs from consensus owner".into(),
                ));
            }
            let encoded = encode_consensus_snapshot_install(&install)?;
            if let Some(existing) = self
                .namespace
                .raft_snapshot
                .get(SNAPSHOT_INSTALL_KEY)
                .map_err(fjall_error)?
            {
                if existing.as_ref() == encoded.as_slice() {
                    return Ok(());
                }
                return Err(StorageError::InvalidConsensus(
                    "a different snapshot install is already staged".into(),
                ));
            }
            self.namespace
                .raft_snapshot
                .insert(SNAPSHOT_INSTALL_KEY, encoded)
                .map_err(fjall_error)?;
            self.namespace
                .db
                .persist(PersistMode::SyncAll)
                .map_err(fjall_error)
        })
    }

    fn commit_snapshot_install(&self, install: ConsensusSnapshotInstall) -> StoreFuture<'_, ()> {
        Box::pin(async move {
            let _guard = self.lock()?;
            self.ensure_binding()?;
            if !same_replica_generation(install.active_binding(), &self.binding) {
                return Err(StorageError::InvalidConsensus(
                    "snapshot install commit binding differs from consensus owner".into(),
                ));
            }
            let expected = encode_consensus_snapshot_install(&install)?;
            let staged = self
                .namespace
                .raft_snapshot
                .get(SNAPSHOT_INSTALL_KEY)
                .map_err(fjall_error)?
                .ok_or_else(|| {
                    StorageError::InvalidConsensus(
                        "snapshot install commit has no staged journal".into(),
                    )
                })?;
            if staged.as_ref() != expected.as_slice() {
                return Err(StorageError::InvalidConsensus(
                    "snapshot install commit differs from staged journal".into(),
                ));
            }
            let mut batch = self
                .namespace
                .db
                .batch()
                .durability(Some(PersistMode::SyncAll));
            batch.insert(
                &self.namespace.raft_state,
                MEMBERSHIP_KEY,
                encode_membership(install.membership())?,
            );
            batch.insert(
                &self.namespace.raft_snapshot,
                SNAPSHOT_KEY,
                encode_consensus_snapshot(install.metadata())?,
            );
            batch.insert(
                &self.namespace.raft_state,
                HARD_STATE_KEY,
                encode_hard_state(install.hard_state())?,
            );
            batch.remove(&self.namespace.raft_snapshot, SNAPSHOT_INSTALL_KEY);
            batch.commit().map_err(fjall_error)
        })
    }
}

impl FjallConsensusStore {
    fn lock(&self) -> Result<MutexGuard<'_, ()>, StorageError> {
        self.namespace
            .consensus_guard
            .lock()
            .map_err(|_| StorageError::Internal("Fjall consensus lock is poisoned".into()))
    }

    fn load_log(&self) -> Result<BTreeMap<u64, ConsensusEntry>, StorageError> {
        let mut entries = BTreeMap::new();
        for item in self.namespace.raft_log.iter() {
            let (key, value) = item.into_inner().map_err(fjall_error)?;
            let index = decode_log_index(&key)?;
            let entry = decode_consensus_entry(&value)?;
            if entry.index() != index || entries.insert(index, entry).is_some() {
                return Err(StorageError::InvalidConsensus(
                    "stored consensus log identity is malformed".into(),
                ));
            }
        }
        if entries
            .keys()
            .zip(entries.keys().skip(1))
            .any(|(left, right)| *right != left.saturating_add(1))
        {
            return Err(StorageError::InvalidConsensus(
                "stored consensus log is discontiguous".into(),
            ));
        }
        Ok(entries)
    }
}

fn same_replica_generation(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.cluster_id() == right.cluster_id()
        && left.graph_id() == right.graph_id()
        && left.shard_id() == right.shard_id()
        && left.replica_id() == right.replica_id()
        && left.placement_epoch() == right.placement_epoch()
        && left.backend_generation() == right.backend_generation()
}

fn decode_log_index(bytes: &[u8]) -> Result<u64, StorageError> {
    let bytes: [u8; 8] = bytes
        .try_into()
        .map_err(|_| StorageError::InvalidConsensus("invalid consensus log key".into()))?;
    Ok(u64::from_be_bytes(bytes))
}
