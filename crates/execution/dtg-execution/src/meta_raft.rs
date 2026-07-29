use std::fmt;
use std::sync::Arc;

use dtg_storage::{
    CommandId, ConsensusCommandEnvelope, ConsensusEntry, ConsensusStore, RaftHardState, ReplicaId,
    SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
    StorageError,
};
use raft::eraftpb::{Entry, EntryType, HardState, Message};
use raft::storage::MemStorage;
use raft::{Config, RawNode, StateRole};
use slog::{Logger, o};

const META_RAFT_ENTRY_VERSION: u32 = 1;
const MAX_META_COMMAND_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetaRaftRole {
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaCommittedEntry {
    term: u64,
    index: u64,
    proposal_id: u128,
    payload: Vec<u8>,
}

impl MetaCommittedEntry {
    pub const fn term(&self) -> u64 {
        self.term
    }

    pub const fn index(&self) -> u64 {
        self.index
    }

    pub const fn proposal_id(&self) -> u128 {
        self.proposal_id
    }

    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

#[derive(Default)]
pub struct MetaRaftProgress {
    pub messages: Vec<Message>,
    pub committed: Vec<MetaCommittedEntry>,
}

pub struct MetaRaftHost {
    node: RawNode<MemStorage>,
    store: Arc<dyn ConsensusStore>,
    recovery: Vec<MetaCommittedEntry>,
}

impl MetaRaftHost {
    pub async fn open(
        store: Arc<dyn ConsensusStore>,
        node_id: ReplicaId,
        applied_index: u64,
    ) -> Result<Self, MetaRaftError> {
        if store.binding().replica_id() != node_id {
            return Err(MetaRaftError::InvalidState(
                "Meta Raft store belongs to another replica".into(),
            ));
        }
        let membership = store.membership().await?;
        if membership.voters.is_empty()
            || !membership.voters.contains(&node_id)
            || membership.learners.contains(&node_id)
        {
            return Err(MetaRaftError::InvalidState(
                "configured Meta Raft membership does not contain this voter".into(),
            ));
        }
        let stored_hard_state = store.hard_state().await?;
        if applied_index > stored_hard_state.committed_index {
            return Err(MetaRaftError::InvalidState(
                "Meta applied index exceeds committed consensus index".into(),
            ));
        }
        let stored_entries = store.entries(1, u64::MAX, u64::MAX).await?;
        let entries = stored_entries
            .into_iter()
            .map(decode_entry)
            .collect::<Result<Vec<_>, _>>()?;
        let memory = MemStorage::new_with_conf_state((
            membership
                .voters
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
            membership
                .learners
                .iter()
                .map(|id| id.get())
                .collect::<Vec<_>>(),
        ));
        {
            let mut memory = memory.wl();
            memory.append(&entries).map_err(MetaRaftError::Raft)?;
            memory.set_hardstate(encode_hard_state(stored_hard_state));
        }
        let mut recovery = Vec::new();
        for entry in entries
            .iter()
            .filter(|entry| entry.index <= stored_hard_state.committed_index)
        {
            if let Some(entry) = committed_entry(entry)? {
                recovery.push(entry);
            }
        }
        let mut config = Config::new(node_id.get());
        config.applied = applied_index;
        config.election_tick = 10;
        config.heartbeat_tick = 2;
        config.validate().map_err(MetaRaftError::Raft)?;
        let logger = Logger::root(slog::Discard, o!());
        let node = RawNode::new(&config, memory, &logger).map_err(MetaRaftError::Raft)?;
        Ok(Self {
            node,
            store,
            recovery,
        })
    }

    pub fn recovery_entries(&self) -> &[MetaCommittedEntry] {
        &self.recovery
    }

    pub fn role(&self) -> MetaRaftRole {
        match self.node.status().ss.raft_state {
            StateRole::Follower => MetaRaftRole::Follower,
            StateRole::Candidate => MetaRaftRole::Candidate,
            StateRole::Leader => MetaRaftRole::Leader,
            StateRole::PreCandidate => MetaRaftRole::PreCandidate,
        }
    }

    pub fn leader_id(&self) -> Option<ReplicaId> {
        ReplicaId::new(self.node.status().ss.leader_id).ok()
    }

    pub fn campaign(&mut self) -> Result<(), MetaRaftError> {
        self.node.campaign().map_err(MetaRaftError::Raft)
    }

    pub fn tick(&mut self) -> bool {
        self.node.tick()
    }

    pub fn step(&mut self, message: Message) -> Result<(), MetaRaftError> {
        self.node.step(message).map_err(MetaRaftError::Raft)
    }

    pub fn propose(&mut self, proposal_id: u128, payload: Vec<u8>) -> Result<(), MetaRaftError> {
        if proposal_id == 0 || payload.is_empty() || payload.len() > MAX_META_COMMAND_BYTES {
            return Err(MetaRaftError::InvalidState(
                "Meta proposal identity or payload is invalid".into(),
            ));
        }
        if self.role() != MetaRaftRole::Leader {
            return Err(MetaRaftError::NotLeader(self.leader_id()));
        }
        self.node
            .propose(proposal_id.to_be_bytes().to_vec(), payload)
            .map_err(MetaRaftError::Raft)
    }

    pub fn leader_read_index(&self) -> Result<u64, MetaRaftError> {
        if self.role() != MetaRaftRole::Leader {
            return Err(MetaRaftError::NotLeader(self.leader_id()));
        }
        if !self.node.raft.commit_to_current_term() {
            return Err(MetaRaftError::NotReady);
        }
        Ok(self.node.status().hs.commit)
    }

    pub async fn drive_ready(&mut self) -> Result<MetaRaftProgress, MetaRaftError> {
        let mut progress = MetaRaftProgress::default();
        while self.node.has_ready() {
            let mut ready = self.node.ready();
            if !ready.snapshot().is_empty() {
                return Err(MetaRaftError::InvalidState(
                    "Meta snapshot installation is not available through Ready".into(),
                ));
            }
            let entries = ready.entries().to_vec();
            if !entries.is_empty() {
                self.store
                    .append(
                        entries
                            .iter()
                            .map(encode_entry)
                            .collect::<Result<Vec<_>, _>>()?,
                    )
                    .await?;
                self.node
                    .mut_store()
                    .wl()
                    .append(&entries)
                    .map_err(MetaRaftError::Raft)?;
            }
            if let Some(hard_state) = ready.hs() {
                self.persist_hard_state(hard_state).await?;
            }
            progress.messages.extend(ready.take_messages());
            progress.messages.extend(ready.take_persisted_messages());
            let mut committed = ready.take_committed_entries();
            let mut light = self.node.advance_append(ready);
            if light.commit_index().is_some() {
                let hard_state = self.node.status().hs;
                self.persist_hard_state(&hard_state).await?;
            }
            committed.extend(light.take_committed_entries());
            progress.messages.extend(light.take_messages());
            if let Some(last) = committed.last() {
                self.node.advance_apply_to(last.index);
            }
            for entry in committed {
                if let Some(entry) = committed_entry(&entry)? {
                    progress.committed.push(entry);
                }
            }
        }
        Ok(progress)
    }

    async fn persist_hard_state(&mut self, hard_state: &HardState) -> Result<(), MetaRaftError> {
        let stored = decode_hard_state(hard_state)?;
        self.store.set_hard_state(stored).await?;
        self.node.mut_store().wl().set_hardstate(hard_state.clone());
        Ok(())
    }
}

fn committed_entry(entry: &Entry) -> Result<Option<MetaCommittedEntry>, MetaRaftError> {
    if entry.get_entry_type() != EntryType::EntryNormal {
        return Err(MetaRaftError::InvalidState(
            "Meta membership changes require the configured membership workflow".into(),
        ));
    }
    if entry.data.is_empty() {
        return Ok(None);
    }
    let proposal_id = exact_proposal_id(&entry.context)?;
    Ok(Some(MetaCommittedEntry {
        term: entry.term,
        index: entry.index,
        proposal_id,
        payload: entry.data.clone(),
    }))
}

fn encode_entry(entry: &Entry) -> Result<ConsensusEntry, MetaRaftError> {
    if entry.get_entry_type() != EntryType::EntryNormal || entry.term == 0 || entry.index == 0 {
        return Err(MetaRaftError::InvalidState(
            "Meta Raft WAL entry identity is invalid".into(),
        ));
    }
    let mut payload = Vec::with_capacity(8 + entry.context.len() + entry.data.len());
    payload.extend_from_slice(&META_RAFT_ENTRY_VERSION.to_be_bytes());
    let context_len: u32 =
        entry.context.len().try_into().map_err(|_| {
            MetaRaftError::InvalidState("Meta proposal context is oversized".into())
        })?;
    payload.extend_from_slice(&context_len.to_be_bytes());
    payload.extend_from_slice(&entry.context);
    payload.extend_from_slice(&entry.data);
    let command_id = if entry.data.is_empty() {
        CommandId::new((u128::from(entry.term) << 64) | u128::from(entry.index))?
    } else {
        CommandId::new(exact_proposal_id(&entry.context)?)?
    };
    Ok(ConsensusEntry::new(
        SUPPORTED_CONSENSUS_WAL_FORMAT_VERSION,
        entry.term,
        entry.index,
        command_id,
        ConsensusCommandEnvelope::new(SUPPORTED_CONSENSUS_COMMAND_FORMAT_VERSION, payload)?,
    )?)
}

fn decode_entry(entry: ConsensusEntry) -> Result<Entry, MetaRaftError> {
    let payload = entry.command().payload();
    if payload.len() < 8 {
        return Err(MetaRaftError::InvalidState(
            "Meta Raft WAL entry is truncated".into(),
        ));
    }
    let version = u32::from_be_bytes(payload[0..4].try_into().unwrap());
    let context_len = u32::from_be_bytes(payload[4..8].try_into().unwrap()) as usize;
    let context_end = 8_usize
        .checked_add(context_len)
        .filter(|end| *end <= payload.len())
        .ok_or_else(|| MetaRaftError::InvalidState("Meta Raft context is truncated".into()))?;
    if version != META_RAFT_ENTRY_VERSION {
        return Err(MetaRaftError::InvalidState(
            "Meta Raft WAL version is unsupported".into(),
        ));
    }
    let context = payload[8..context_end].to_vec();
    let data = payload[context_end..].to_vec();
    let expected = if data.is_empty() {
        CommandId::new((u128::from(entry.term()) << 64) | u128::from(entry.index()))?
    } else {
        CommandId::new(exact_proposal_id(&context)?)?
    };
    if expected != entry.command_id() {
        return Err(MetaRaftError::InvalidState(
            "Meta Raft WAL command identity is inconsistent".into(),
        ));
    }
    Ok(Entry {
        term: entry.term(),
        index: entry.index(),
        context,
        data,
        ..Entry::default()
    })
}

fn exact_proposal_id(context: &[u8]) -> Result<u128, MetaRaftError> {
    let bytes: [u8; 16] = context
        .try_into()
        .map_err(|_| MetaRaftError::InvalidState("Meta proposal ID is malformed".into()))?;
    let value = u128::from_be_bytes(bytes);
    if value == 0 {
        return Err(MetaRaftError::InvalidState(
            "Meta proposal ID is zero".into(),
        ));
    }
    Ok(value)
}

fn encode_hard_state(state: RaftHardState) -> HardState {
    HardState {
        term: state.current_term,
        vote: state.voted_for.map_or(0, |replica| replica.get()),
        commit: state.committed_index,
    }
}

fn decode_hard_state(state: &HardState) -> Result<RaftHardState, MetaRaftError> {
    Ok(RaftHardState {
        current_term: state.term,
        voted_for: if state.vote == 0 {
            None
        } else {
            Some(ReplicaId::new(state.vote).map_err(|error| {
                MetaRaftError::InvalidState(format!("invalid Meta Raft vote: {error}"))
            })?)
        },
        committed_index: state.commit,
    })
}

#[derive(Debug)]
pub enum MetaRaftError {
    Storage(StorageError),
    Raft(raft::Error),
    InvalidState(String),
    NotLeader(Option<ReplicaId>),
    NotReady,
}

impl fmt::Display for MetaRaftError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(formatter, "Meta Raft storage error: {error}"),
            Self::Raft(error) => write!(formatter, "Meta Raft error: {error}"),
            Self::InvalidState(message) => formatter.write_str(message),
            Self::NotLeader(leader) => write!(formatter, "Meta replica is not leader: {leader:?}"),
            Self::NotReady => formatter.write_str("Meta leader is not ready for linearizable read"),
        }
    }
}

impl std::error::Error for MetaRaftError {}

impl From<StorageError> for MetaRaftError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}
