use std::{fmt, path::Path};

use dtg_storage::{
    ConsensusEntry, ConsensusSnapshotMetadata, ConsensusStore, RaftHardState, RaftMembership,
    ReplicaBinding, StorageError, StoreFuture,
};
use fjall::PersistMode;

use crate::{
    codec::{
        decode_consensus_entry, decode_consensus_snapshot, decode_hard_state, decode_membership,
        encode_consensus_entry, encode_consensus_snapshot, encode_hard_state, encode_membership,
    },
    namespace::{NamespaceDb, fjall_error},
};

const HARD_STATE_KEY: &[u8] = b"hard_state";
const MEMBERSHIP_KEY: &[u8] = b"membership";
const SNAPSHOT_KEY: &[u8] = b"snapshot";

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
            self.ensure_binding()?;
            if entries
                .windows(2)
                .any(|pair| pair[1].index() != pair[0].index().saturating_add(1))
            {
                return Err(StorageError::InvalidConsensus(
                    "consensus append entries must be contiguous".into(),
                ));
            }
            let mut batch = self
                .namespace
                .db
                .batch()
                .durability(Some(PersistMode::SyncAll));
            for entry in entries {
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
            if low > high || max_bytes == 0 {
                return Err(StorageError::InvalidConsensus(
                    "invalid consensus entry range".into(),
                ));
            }
            let mut used = 0_u64;
            let mut rows = Vec::new();
            for item in self
                .namespace
                .raft_log
                .range(low.to_be_bytes()..high.to_be_bytes())
            {
                let (_, value) = item.into_inner().map_err(fjall_error)?;
                let size = value.len() as u64;
                if used.saturating_add(size) > max_bytes {
                    break;
                }
                rows.push(decode_consensus_entry(&value)?);
                used += size;
            }
            Ok(rows)
        })
    }

    fn truncate_suffix(&self, from_index: u64) -> StoreFuture<'_, ()> {
        Box::pin(async move {
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
}
