use dtg_kernel::{ReplicaId, TransactionTime};
use dtg_storage::{ReadFence, ReplicaBinding};

pub const SUPPORTED_FOLLOWER_READ_PROOF_VERSION: u32 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadMode {
    Linearizable,
    Snapshot,
    Follower { requested_time: TransactionTime },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadPermit {
    request_id: Option<u128>,
    mode: ReadMode,
    fence: ReadFence,
    leader_replica: Option<ReplicaId>,
    leader_term: u64,
    closed_timestamp: Option<TransactionTime>,
}

impl ReadPermit {
    pub(crate) const fn new(
        request_id: Option<u128>,
        mode: ReadMode,
        fence: ReadFence,
        leader_replica: Option<ReplicaId>,
        leader_term: u64,
        closed_timestamp: Option<TransactionTime>,
    ) -> Self {
        Self {
            request_id,
            mode,
            fence,
            leader_replica,
            leader_term,
            closed_timestamp,
        }
    }

    pub const fn request_id(&self) -> Option<u128> {
        self.request_id
    }

    pub const fn mode(&self) -> ReadMode {
        self.mode
    }

    pub const fn fence(&self) -> &ReadFence {
        &self.fence
    }

    pub const fn leader_replica(&self) -> Option<ReplicaId> {
        self.leader_replica
    }

    pub const fn leader_term(&self) -> u64 {
        self.leader_term
    }

    pub const fn closed_timestamp(&self) -> Option<TransactionTime> {
        self.closed_timestamp
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReadFailure {
    request_id: u128,
    error: ReadError,
}

impl ReadFailure {
    pub(crate) const fn new(request_id: u128, error: ReadError) -> Self {
        Self { request_id, error }
    }

    pub const fn request_id(&self) -> u128 {
        self.request_id
    }

    pub const fn error(&self) -> &ReadError {
        &self.error
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FollowerReadProof {
    version: u32,
    leader_binding: ReplicaBinding,
    leader_replica: ReplicaId,
    leader_term: u64,
    placement_epoch: u64,
    backend_generation: u64,
    applied_index: u64,
    closed_timestamp: TransactionTime,
    signature: [u8; 32],
}

impl FollowerReadProof {
    pub const fn version(&self) -> u32 {
        self.version
    }

    pub const fn leader_binding(&self) -> &ReplicaBinding {
        &self.leader_binding
    }

    pub const fn leader_replica(&self) -> ReplicaId {
        self.leader_replica
    }

    pub const fn leader_term(&self) -> u64 {
        self.leader_term
    }

    pub const fn placement_epoch(&self) -> u64 {
        self.placement_epoch
    }

    pub const fn backend_generation(&self) -> u64 {
        self.backend_generation
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn closed_timestamp(&self) -> TransactionTime {
        self.closed_timestamp
    }
}

#[derive(Clone)]
pub struct FollowerReadProofAuthority {
    key: [u8; 32],
}

impl FollowerReadProofAuthority {
    pub fn new(key: [u8; 32]) -> Result<Self, ReadError> {
        if key == [0; 32] {
            return Err(ReadError::InvalidProofKey);
        }
        Ok(Self { key })
    }

    pub fn issue(
        &self,
        leader_binding: ReplicaBinding,
        leader_replica: ReplicaId,
        leader_term: u64,
        applied_index: u64,
        closed_timestamp: TransactionTime,
    ) -> Result<FollowerReadProof, ReadError> {
        if leader_binding.replica_id() != leader_replica || leader_term == 0 || applied_index == 0 {
            return Err(ReadError::NotReady);
        }
        let mut proof = FollowerReadProof {
            version: SUPPORTED_FOLLOWER_READ_PROOF_VERSION,
            placement_epoch: leader_binding.placement_epoch().get(),
            backend_generation: leader_binding.backend_generation().get(),
            leader_binding,
            leader_replica,
            leader_term,
            applied_index,
            closed_timestamp,
            signature: [0; 32],
        };
        proof.signature = self.signature(&proof);
        Ok(proof)
    }

    pub fn verify(&self, proof: &FollowerReadProof) -> Result<(), ReadError> {
        if proof.version != SUPPORTED_FOLLOWER_READ_PROOF_VERSION {
            return Err(ReadError::InvalidProofVersion);
        }
        if proof.leader_binding.replica_id() != proof.leader_replica
            || proof.leader_binding.placement_epoch().get() != proof.placement_epoch
            || proof.leader_binding.backend_generation().get() != proof.backend_generation
            || proof.leader_term == 0
            || proof.applied_index == 0
        {
            return Err(ReadError::InvalidProofSignature);
        }
        if proof.signature != self.signature(proof) {
            return Err(ReadError::InvalidProofSignature);
        }
        Ok(())
    }

    fn signature(&self, proof: &FollowerReadProof) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new_keyed(&self.key);
        hasher.update(b"dtg-follower-read-proof-v1");
        hasher.update(&proof.version.to_be_bytes());
        hasher.update(&proof.leader_binding.identity_digest().get());
        hasher.update(&proof.leader_replica.get().to_be_bytes());
        hasher.update(&proof.leader_term.to_be_bytes());
        hasher.update(&proof.placement_epoch.to_be_bytes());
        hasher.update(&proof.backend_generation.to_be_bytes());
        hasher.update(&proof.applied_index.to_be_bytes());
        hasher.update(&proof.closed_timestamp.get().to_be_bytes());
        *hasher.finalize().as_bytes()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadError {
    InvalidRequest,
    InvalidProofKey,
    NotLeader,
    StalePlacementEpoch,
    StaleBackendGeneration,
    InvalidProofVersion,
    InvalidProofSignature,
    NotReady,
    AdapterLagging,
    UnsafeFollowerRead,
    SnapshotTooOld,
    Overloaded,
    DuplicateRequest,
}

impl ReadError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest => "DTG-SHARD-READ-REQUEST",
            Self::InvalidProofKey => "DTG-SHARD-READ-PROOF-KEY",
            Self::NotLeader => "DTG-SHARD-READ-NOT-LEADER",
            Self::StalePlacementEpoch => "DTG-SHARD-READ-STALE-EPOCH",
            Self::StaleBackendGeneration => "DTG-SHARD-READ-STALE-GENERATION",
            Self::InvalidProofVersion => "DTG-SHARD-READ-PROOF-VERSION",
            Self::InvalidProofSignature => "DTG-SHARD-READ-PROOF-SIGNATURE",
            Self::NotReady => "DTG-SHARD-READ-NOT-READY",
            Self::AdapterLagging => "DTG-SHARD-READ-ADAPTER-LAG",
            Self::UnsafeFollowerRead => "DTG-SHARD-READ-UNSAFE-FOLLOWER",
            Self::SnapshotTooOld => "DTG-SHARD-READ-SNAPSHOT-OLD",
            Self::Overloaded => "DTG-SHARD-READ-OVERLOAD",
            Self::DuplicateRequest => "DTG-SHARD-READ-DUPLICATE",
        }
    }
}

impl core::fmt::Display for ReadError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ReadError {}
