use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_types::TransactionTime;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadPermitMode {
    LeaderLinearizable,
    FollowerSnapshot { read_ts: TransactionTime },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FollowerReadProof {
    pub(crate) shard_id: u32,
    pub(crate) placement_epoch: u64,
    pub(crate) leader_id: u64,
    pub(crate) leader_term: u64,
    pub(crate) read_index: u64,
}

impl FollowerReadProof {
    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(&self) -> u64 {
        self.placement_epoch
    }

    #[must_use]
    pub const fn leader_id(&self) -> u64 {
        self.leader_id
    }

    #[must_use]
    pub const fn leader_term(&self) -> u64 {
        self.leader_term
    }

    #[must_use]
    pub const fn read_index(&self) -> u64 {
        self.read_index
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadPermit {
    shard_id: u32,
    placement_epoch: u64,
    node_id: u64,
    read_index: u64,
    mode: ReadPermitMode,
}

impl ReadPermit {
    pub(crate) const fn new(
        shard_id: u32,
        placement_epoch: u64,
        node_id: u64,
        read_index: u64,
        mode: ReadPermitMode,
    ) -> Self {
        Self {
            shard_id,
            placement_epoch,
            node_id,
            read_index,
            mode,
        }
    }

    #[must_use]
    pub const fn shard_id(self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(self) -> u64 {
        self.placement_epoch
    }

    #[must_use]
    pub const fn node_id(self) -> u64 {
        self.node_id
    }

    #[must_use]
    pub const fn read_index(self) -> u64 {
        self.read_index
    }

    #[must_use]
    pub const fn mode(self) -> ReadPermitMode {
        self.mode
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadBarrierError {
    NotLeader {
        node_id: u64,
        leader_hint: Option<u64>,
    },
    StaleEpoch {
        expected: u64,
        actual: u64,
    },
    NotReady {
        node_id: Option<u64>,
        reason: &'static str,
    },
    AdapterLagging {
        node_id: u64,
        required_index: u64,
        applied_index: u64,
    },
    NodeNotFound {
        node_id: u64,
    },
}

impl Display for ReadBarrierError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLeader {
                node_id,
                leader_hint,
            } => write!(
                formatter,
                "node {node_id} is not leader; current leader is {leader_hint:?}"
            ),
            Self::StaleEpoch { expected, actual } => {
                write!(
                    formatter,
                    "expected placement epoch {expected}, got {actual}"
                )
            }
            Self::NotReady { node_id, reason } => {
                write!(formatter, "Replica {node_id:?} is not ready: {reason}")
            }
            Self::AdapterLagging {
                node_id,
                required_index,
                applied_index,
            } => write!(
                formatter,
                "Replica {node_id} Adapter is at index {applied_index}, below required {required_index}"
            ),
            Self::NodeNotFound { node_id } => write!(formatter, "Raft node {node_id} not found"),
        }
    }
}

impl Error for ReadBarrierError {}
