use std::sync::Arc;

use dtg_storage::{
    CommandId, ConsensusCommandEnvelope, ConsensusEntry, ConsensusSnapshotMetadata, ConsensusStore,
    RaftHardState, RaftMembership, ReplicaBinding, ReplicaId,
    SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
    StorageError,
};
use raft::eraftpb::{ConfState, Entry, EntryType, HardState, Snapshot};
use raft::storage::{GetEntriesContext, RaftState, Storage};

use crate::{ShardCommand, ShardError, state_machine::block_on};

const MAX_RAFT_WAL_ENTRY_BYTES: usize = 16 * 1024 * 1024 + 1024;

#[derive(Clone)]
pub struct RaftStore {
    store: Arc<dyn ConsensusStore>,
}

impl RaftStore {
    pub fn new(store: Arc<dyn ConsensusStore>) -> Result<Self, ShardError> {
        let binding = store.binding();
        if binding.namespace_id().as_str().is_empty() {
            return Err(ShardError::BindingMismatch);
        }
        Ok(Self { store })
    }

    pub fn binding(&self) -> &ReplicaBinding {
        self.store.binding()
    }

    pub fn recovery_entries(&self, applied_index: u64) -> Result<Vec<Entry>, ShardError> {
        let hard_state = block_on(self.store.hard_state())?;
        let snapshot_index = block_on(self.store.snapshot_metadata())?
            .map_or(0, |snapshot| snapshot.last_included_index);
        if snapshot_index > applied_index {
            return Err(ShardError::SnapshotStateMissing {
                snapshot_index,
                applied_index,
            });
        }
        if hard_state.committed_index <= snapshot_index {
            return Ok(Vec::new());
        }
        let low = snapshot_index.saturating_add(1);
        let high = hard_state.committed_index.checked_add(1).ok_or_else(|| {
            ShardError::InvalidRaftState("committed index cannot be represented as a range".into())
        })?;
        let entries = block_on(self.store.entries(low, high, u64::MAX))?;
        let expected = hard_state.committed_index - snapshot_index;
        if entries.len() as u64 != expected
            || entries
                .iter()
                .enumerate()
                .any(|(offset, entry)| entry.index() != low + offset as u64)
        {
            return Err(ShardError::InvalidRaftState(
                "committed WAL suffix is missing or discontiguous".into(),
            ));
        }
        entries
            .into_iter()
            .map(decode_raft_entry)
            .collect::<Result<Vec<_>, _>>()
    }

    pub(crate) fn persist_entries(&self, entries: &[Entry]) -> Result<(), ShardError> {
        let encoded = entries
            .iter()
            .map(encode_consensus_entry)
            .collect::<Result<Vec<_>, _>>()?;
        block_on(self.store.append(encoded))?;
        Ok(())
    }

    pub(crate) fn persist_hard_state(&self, state: &HardState) -> Result<(), ShardError> {
        let voted_for = if state.vote == 0 {
            None
        } else {
            Some(ReplicaId::new(state.vote).map_err(|error| {
                ShardError::InvalidRaftState(format!("invalid Raft vote: {error}"))
            })?)
        };
        block_on(self.store.set_hard_state(RaftHardState {
            current_term: state.term,
            voted_for,
            committed_index: state.commit,
        }))?;
        Ok(())
    }

    pub fn snapshot_metadata(&self) -> Result<Option<ConsensusSnapshotMetadata>, ShardError> {
        Ok(block_on(self.store.snapshot_metadata())?)
    }

    pub(crate) fn committed_index(&self) -> Result<u64, ShardError> {
        Ok(block_on(self.store.hard_state())?.committed_index)
    }

    fn membership(&self) -> Result<RaftMembership, StorageError> {
        block_on(self.store.membership())
    }

    fn all_entries(&self, low: u64, high: u64) -> Result<Vec<ConsensusEntry>, StorageError> {
        block_on(self.store.entries(low, high, u64::MAX))
    }

    fn bounds(&self) -> Result<(u64, u64), StorageError> {
        let snapshot = block_on(self.store.snapshot_metadata())?;
        let snapshot_index = snapshot
            .as_ref()
            .map_or(0, |snapshot| snapshot.last_included_index);
        let low = snapshot_index.saturating_add(1);
        let entries = self.all_entries(low, u64::MAX)?;
        if entries.first().is_some_and(|entry| entry.index() != low)
            || entries
                .windows(2)
                .any(|window| window[0].index().checked_add(1) != Some(window[1].index()))
        {
            return Err(StorageError::InvalidConsensus(
                "retained consensus log is missing or discontiguous after snapshot".into(),
            ));
        }
        let first = entries.first().map_or(low, ConsensusEntry::index);
        let last = entries.last().map_or(snapshot_index, ConsensusEntry::index);
        Ok((first, last))
    }
}

fn raft_error(error: impl std::error::Error + Send + Sync + 'static) -> raft::Error {
    raft::Error::Store(raft::StorageError::Other(Box::new(error)))
}

fn conf_state(membership: RaftMembership) -> ConfState {
    ConfState {
        voters: membership
            .voters
            .into_iter()
            .map(|replica| replica.get())
            .collect(),
        learners: membership
            .learners
            .into_iter()
            .map(|replica| replica.get())
            .collect(),
        ..ConfState::default()
    }
}

fn encode_consensus_entry(entry: &Entry) -> Result<ConsensusEntry, ShardError> {
    if entry.term == 0 || entry.index == 0 {
        return Err(ShardError::InvalidRaftState(
            "Raft WAL entry identity must be nonzero".into(),
        ));
    }
    let payload_len = 13_usize
        .checked_add(entry.context.len())
        .and_then(|length| length.checked_add(entry.data.len()))
        .filter(|length| *length <= MAX_RAFT_WAL_ENTRY_BYTES)
        .ok_or_else(|| ShardError::InvalidRaftState("Raft WAL entry is oversized".into()))?;
    let command_id = entry_command_id(entry)?;
    let mut payload = Vec::with_capacity(payload_len);
    payload.extend_from_slice(&1_u32.to_be_bytes());
    payload.push(match entry.get_entry_type() {
        EntryType::EntryNormal => 0,
        EntryType::EntryConfChange => 1,
        EntryType::EntryConfChangeV2 => 2,
    });
    put_bytes(&mut payload, &entry.context)?;
    put_bytes(&mut payload, &entry.data)?;
    let envelope =
        ConsensusCommandEnvelope::new(SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, payload)?;
    Ok(ConsensusEntry::new(
        SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
        entry.term,
        entry.index,
        command_id,
        envelope,
    )?)
}

fn decode_raft_entry(entry: ConsensusEntry) -> Result<Entry, ShardError> {
    let payload = entry.command().payload();
    if payload.len() > MAX_RAFT_WAL_ENTRY_BYTES {
        return Err(ShardError::InvalidRaftState(
            "Raft WAL entry is oversized".into(),
        ));
    }
    let mut offset = 0;
    let version = take_u32(payload, &mut offset)?;
    if version != 1 {
        return Err(ShardError::InvalidRaftState(format!(
            "unsupported internal Raft entry version {version}"
        )));
    }
    let tag = *payload
        .get(offset)
        .ok_or_else(|| ShardError::InvalidRaftState("truncated Raft WAL entry".into()))?;
    offset += 1;
    let context = take_bytes(payload, &mut offset)?.to_vec();
    let data = take_bytes(payload, &mut offset)?.to_vec();
    if offset != payload.len() {
        return Err(ShardError::InvalidRaftState(
            "Raft WAL entry contains trailing bytes".into(),
        ));
    }
    let entry_type = match tag {
        0 => EntryType::EntryNormal,
        1 => EntryType::EntryConfChange,
        2 => EntryType::EntryConfChangeV2,
        _ => {
            return Err(ShardError::InvalidRaftState(
                "unknown Raft WAL entry type".into(),
            ));
        }
    };
    let mut raft_entry = Entry {
        term: entry.term(),
        index: entry.index(),
        context,
        data,
        ..Entry::default()
    };
    raft_entry.set_entry_type(entry_type);
    if entry_command_id(&raft_entry)? != entry.command_id() {
        return Err(ShardError::InvalidRaftState(
            "Raft WAL command identifier mismatch".into(),
        ));
    }
    Ok(raft_entry)
}

fn entry_command_id(entry: &Entry) -> Result<CommandId, ShardError> {
    if entry.data.is_empty() {
        let synthetic = (u128::from(entry.term) << 64) | u128::from(entry.index);
        return Ok(CommandId::new(synthetic)?);
    }
    let command = ShardCommand::decode(&entry.data)?;
    let command_id = command.header().command_id();
    if entry.context.as_slice() != command_id.get().to_be_bytes() {
        return Err(ShardError::InvalidRaftState(
            "Raft proposal context does not match command identifier".into(),
        ));
    }
    Ok(command_id)
}

fn put_bytes(output: &mut Vec<u8>, value: &[u8]) -> Result<(), ShardError> {
    let length: u32 = value
        .len()
        .try_into()
        .map_err(|_| ShardError::InvalidRaftState("Raft entry field is too large".into()))?;
    output.extend_from_slice(&length.to_be_bytes());
    output.extend_from_slice(value);
    Ok(())
}

fn take_u32(bytes: &[u8], offset: &mut usize) -> Result<u32, ShardError> {
    let end = offset
        .checked_add(4)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| ShardError::InvalidRaftState("truncated Raft WAL entry".into()))?;
    let value = u32::from_be_bytes(bytes[*offset..end].try_into().unwrap());
    *offset = end;
    Ok(value)
}

fn take_bytes<'a>(bytes: &'a [u8], offset: &mut usize) -> Result<&'a [u8], ShardError> {
    let length = take_u32(bytes, offset)? as usize;
    let end = offset
        .checked_add(length)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(|| ShardError::InvalidRaftState("truncated Raft WAL entry".into()))?;
    let value = &bytes[*offset..end];
    *offset = end;
    Ok(value)
}

impl Storage for RaftStore {
    fn initial_state(&self) -> raft::Result<RaftState> {
        let stored = block_on(self.store.hard_state()).map_err(raft_error)?;
        let hard_state = HardState {
            term: stored.current_term,
            vote: stored.voted_for.map_or(0, |replica| replica.get()),
            commit: stored.committed_index,
        };
        let membership = match self.membership() {
            Ok(membership) => conf_state(membership),
            Err(StorageError::NotFound) => ConfState::default(),
            Err(error) => return Err(raft_error(error)),
        };
        Ok(RaftState::new(hard_state, membership))
    }

    fn entries(
        &self,
        low: u64,
        high: u64,
        max_size: impl Into<Option<u64>>,
        _context: GetEntriesContext,
    ) -> raft::Result<Vec<Entry>> {
        let (first, last) = self.bounds().map_err(raft_error)?;
        if low < first {
            return Err(raft::Error::Store(raft::StorageError::Compacted));
        }
        if high > last.saturating_add(1) || low > high {
            return Err(raft::Error::Store(raft::StorageError::Unavailable));
        }
        let stored = self.all_entries(low, high).map_err(raft_error)?;
        let mut entries = stored
            .into_iter()
            .map(decode_raft_entry)
            .collect::<Result<Vec<_>, _>>()
            .map_err(raft_error)?;
        raft::util::limit_size(&mut entries, max_size.into());
        Ok(entries)
    }

    fn term(&self, index: u64) -> raft::Result<u64> {
        if index == 0 {
            return Ok(0);
        }
        if let Some(snapshot) = block_on(self.store.snapshot_metadata()).map_err(raft_error)? {
            if index == snapshot.last_included_index {
                return Ok(snapshot.last_included_term);
            }
            if index < snapshot.last_included_index {
                return Err(raft::Error::Store(raft::StorageError::Compacted));
            }
        }
        let entries = self
            .all_entries(index, index.saturating_add(1))
            .map_err(raft_error)?;
        entries
            .first()
            .filter(|entry| entry.index() == index)
            .map(ConsensusEntry::term)
            .ok_or(raft::Error::Store(raft::StorageError::Unavailable))
    }

    fn first_index(&self) -> raft::Result<u64> {
        self.bounds().map(|(first, _)| first).map_err(raft_error)
    }

    fn last_index(&self) -> raft::Result<u64> {
        self.bounds().map(|(_, last)| last).map_err(raft_error)
    }

    fn snapshot(&self, request_index: u64, _to: u64) -> raft::Result<Snapshot> {
        let metadata = block_on(self.store.snapshot_metadata()).map_err(raft_error)?;
        let Some(metadata) = metadata else {
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        };
        if metadata.last_included_index < request_index {
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        }
        let membership = self.membership().map_err(raft_error)?;
        let mut snapshot = Snapshot {
            data: metadata.content_digest.get().to_vec(),
            ..Snapshot::default()
        };
        let snapshot_metadata = snapshot.mut_metadata();
        snapshot_metadata.index = metadata.last_included_index;
        snapshot_metadata.term = metadata.last_included_term;
        snapshot_metadata.set_conf_state(conf_state(membership));
        Ok(snapshot)
    }
}
