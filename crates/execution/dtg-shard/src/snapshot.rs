use dtg_kernel::{Digest32, Version};
use dtg_storage::{
    BindingRole, ConsensusSnapshotMetadata, ConsensusStore, LogicalReplicaActivation,
    LogicalReplicaActivationReceipt, LogicalSnapshotCandidateReceipt, LogicalSnapshotSink,
    LogicalSnapshotSource, RaftHardState, RaftMembership, ReplicaBinding, SnapshotChunk,
    SnapshotHeader, SnapshotManifest, SnapshotRequest, StorageError,
};

use crate::{ReadMode, ReadPermit, state_machine::block_on};

pub const SUPPORTED_REPLICA_SNAPSHOT_FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaSnapshotManifest {
    format: Version,
    binding: ReplicaBinding,
    last_included_term: u64,
    last_included_index: u64,
    logical_digest: Digest32,
    chunks: u32,
    records: u64,
    bytes: u64,
}

impl ReplicaSnapshotManifest {
    pub const fn format(&self) -> Version {
        self.format
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn last_included_term(&self) -> u64 {
        self.last_included_term
    }

    pub const fn last_included_index(&self) -> u64 {
        self.last_included_index
    }

    pub const fn logical_digest(&self) -> Digest32 {
        self.logical_digest
    }

    pub const fn chunks(&self) -> u32 {
        self.chunks
    }

    pub const fn records(&self) -> u64 {
        self.records
    }

    pub const fn bytes(&self) -> u64 {
        self.bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaSnapshot {
    manifest: ReplicaSnapshotManifest,
    header: SnapshotHeader,
    chunks: Vec<SnapshotChunk>,
    logical_manifest: SnapshotManifest,
    hard_state: RaftHardState,
    membership: RaftMembership,
}

impl ReplicaSnapshot {
    pub const fn manifest(&self) -> &ReplicaSnapshotManifest {
        &self.manifest
    }

    pub const fn header(&self) -> &SnapshotHeader {
        &self.header
    }

    pub fn chunks(&self) -> &[SnapshotChunk] {
        &self.chunks
    }

    pub fn chunks_mut(&mut self) -> &mut [SnapshotChunk] {
        &mut self.chunks
    }

    pub const fn logical_manifest(&self) -> &SnapshotManifest {
        &self.logical_manifest
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaSnapshotInstallState {
    Candidate,
    Active,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaSnapshotInstallReceipt {
    state: ReplicaSnapshotInstallState,
    active_binding: ReplicaBinding,
    candidate_receipt: LogicalSnapshotCandidateReceipt,
    activation_receipt: LogicalReplicaActivationReceipt,
}

impl ReplicaSnapshotInstallReceipt {
    pub const fn state(&self) -> ReplicaSnapshotInstallState {
        self.state
    }

    pub const fn active_binding(&self) -> &ReplicaBinding {
        &self.active_binding
    }

    pub const fn candidate_receipt(&self) -> &LogicalSnapshotCandidateReceipt {
        &self.candidate_receipt
    }

    pub const fn activation_receipt(&self) -> &LogicalReplicaActivationReceipt {
        &self.activation_receipt
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplicaSnapshotError {
    InvalidRequest(&'static str),
    Storage(StorageError),
}

impl ReplicaSnapshotError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidRequest(_) => "DTG-SHARD-SNAPSHOT-INVALID",
            Self::Storage(_) => "DTG-SHARD-SNAPSHOT-STORAGE",
        }
    }
}

impl core::fmt::Display for ReplicaSnapshotError {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ReplicaSnapshotError {}

impl From<StorageError> for ReplicaSnapshotError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

pub fn create_replica_snapshot(
    source: &dyn LogicalSnapshotSource,
    consensus: &dyn ConsensusStore,
    permit: &ReadPermit,
    snapshot_id: u128,
    max_records_per_chunk: u32,
) -> Result<ReplicaSnapshot, ReplicaSnapshotError> {
    if permit.mode() != ReadMode::Snapshot || snapshot_id == 0 || max_records_per_chunk == 0 {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "snapshot creation requires an exact snapshot permit and nonzero bounds",
        ));
    }
    if consensus.binding() != permit.fence().binding() {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "snapshot consensus and logical bindings differ",
        ));
    }
    let hard_state = block_on(consensus.hard_state())?;
    let membership = block_on(consensus.membership())?;
    let last_included_index = permit.fence().applied_index();
    if last_included_index == 0 || hard_state.committed_index < last_included_index {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "snapshot prefix is not committed",
        ));
    }
    let last_included_term = consensus_term(consensus, last_included_index)?;
    let request = SnapshotRequest::new(snapshot_id, max_records_per_chunk)?;
    let mut reader = block_on(source.begin_snapshot(permit.fence().clone(), request))?;
    let header = reader.header().clone();
    let mut chunks = Vec::new();
    while let Some(chunk) = block_on(reader.next_chunk())? {
        chunk.validate()?;
        chunks.push(chunk);
    }
    let logical_manifest = block_on(reader.finish())?;
    logical_manifest.validate(&header, &chunks)?;
    let chunk_count: u32 = chunks
        .len()
        .try_into()
        .map_err(|_| ReplicaSnapshotError::InvalidRequest("too many snapshot chunks"))?;
    let bytes = snapshot_bytes(&chunks)?;
    let manifest = ReplicaSnapshotManifest {
        format: Version::new(u64::from(SUPPORTED_REPLICA_SNAPSHOT_FORMAT_VERSION)),
        binding: permit.fence().binding().clone(),
        last_included_term,
        last_included_index,
        logical_digest: logical_manifest.content_digest(),
        chunks: chunk_count,
        records: logical_manifest.record_count(),
        bytes,
    };
    Ok(ReplicaSnapshot {
        manifest,
        header,
        chunks,
        logical_manifest,
        hard_state,
        membership,
    })
}

pub fn install_replica_snapshot(
    snapshot: &ReplicaSnapshot,
    sink: &dyn LogicalSnapshotSink,
    activation: &dyn LogicalReplicaActivation,
    consensus: &dyn ConsensusStore,
    candidate_binding: ReplicaBinding,
    active_binding: ReplicaBinding,
) -> Result<ReplicaSnapshotInstallReceipt, ReplicaSnapshotError> {
    validate_snapshot(snapshot)?;
    if candidate_binding.role() != BindingRole::Candidate
        || active_binding.role() != BindingRole::Active
        || !same_binding_except_role(&candidate_binding, &active_binding)
        || consensus.binding() != &active_binding
        || !compatible_snapshot_target(snapshot.manifest.binding(), &active_binding)
    {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "snapshot target binding is invalid",
        ));
    }
    validate_retained_suffix(
        consensus,
        snapshot.manifest.last_included_index,
        block_on(consensus.hard_state())?.committed_index,
    )?;

    let mut writer =
        block_on(sink.begin_restore(candidate_binding.clone(), snapshot.header.clone()))?;
    for chunk in snapshot.chunks.iter().cloned() {
        if let Err(error) = block_on(writer.write_chunk(chunk)) {
            let _ = block_on(writer.abort());
            return Err(error.into());
        }
    }
    let restore_receipt = block_on(writer.commit(snapshot.logical_manifest.clone()))?;
    if restore_receipt.binding() != &candidate_binding
        || restore_receipt.manifest() != &snapshot.logical_manifest
    {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "snapshot sink returned a mismatched restore receipt",
        ));
    }
    let candidate_receipt = LogicalSnapshotCandidateReceipt::new(
        candidate_binding,
        snapshot.header.clone(),
        snapshot.logical_manifest.clone(),
    )?;

    let existing = block_on(consensus.hard_state())?;
    let installed_hard_state = RaftHardState {
        current_term: existing
            .current_term
            .max(snapshot.hard_state.current_term)
            .max(snapshot.manifest.last_included_term),
        voted_for: existing.voted_for,
        committed_index: existing
            .committed_index
            .max(snapshot.manifest.last_included_index),
    };
    block_on(consensus.set_membership(snapshot.membership.clone()))?;
    block_on(consensus.set_snapshot_metadata(ConsensusSnapshotMetadata {
        snapshot_id: snapshot.header.snapshot_id().get(),
        last_included_term: snapshot.manifest.last_included_term,
        last_included_index: snapshot.manifest.last_included_index,
        content_digest: snapshot.manifest.logical_digest,
    }))?;
    block_on(consensus.set_hard_state(installed_hard_state))?;
    verify_consensus_install(consensus, snapshot, installed_hard_state)?;

    let activation_receipt =
        block_on(activation.activate_candidate(candidate_receipt.clone(), active_binding.clone()))?;
    if activation_receipt.active_binding() != &active_binding
        || activation_receipt.snapshot_id() != snapshot.header.snapshot_id()
        || activation_receipt.applied_index() != snapshot.manifest.last_included_index
        || activation_receipt.content_digest() != snapshot.manifest.logical_digest
        || activation_receipt.format_version()
            != u32::try_from(snapshot.manifest.format.get()).unwrap_or(u32::MAX)
    {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "snapshot activation receipt is invalid",
        ));
    }
    Ok(ReplicaSnapshotInstallReceipt {
        state: ReplicaSnapshotInstallState::Active,
        active_binding,
        candidate_receipt,
        activation_receipt,
    })
}

fn validate_snapshot(snapshot: &ReplicaSnapshot) -> Result<(), ReplicaSnapshotError> {
    if snapshot.manifest.format
        != Version::new(u64::from(SUPPORTED_REPLICA_SNAPSHOT_FORMAT_VERSION))
        || snapshot.header.source_binding() != &snapshot.manifest.binding
        || snapshot.header.applied_index() != snapshot.manifest.last_included_index
        || snapshot.header.format_version() != SUPPORTED_REPLICA_SNAPSHOT_FORMAT_VERSION
        || snapshot.manifest.last_included_term == 0
        || snapshot.manifest.last_included_index == 0
        || snapshot.hard_state.committed_index < snapshot.manifest.last_included_index
    {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "replica snapshot manifest is inconsistent",
        ));
    }
    for chunk in &snapshot.chunks {
        chunk.validate()?;
    }
    snapshot
        .logical_manifest
        .validate(&snapshot.header, &snapshot.chunks)?;
    let chunks: u32 = snapshot
        .chunks
        .len()
        .try_into()
        .map_err(|_| ReplicaSnapshotError::InvalidRequest("too many snapshot chunks"))?;
    if snapshot.manifest.logical_digest != snapshot.logical_manifest.content_digest()
        || snapshot.manifest.chunks != chunks
        || snapshot.manifest.records != snapshot.logical_manifest.record_count()
        || snapshot.manifest.bytes != snapshot_bytes(&snapshot.chunks)?
    {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "replica snapshot totals or digest mismatch",
        ));
    }
    Ok(())
}

fn verify_consensus_install(
    consensus: &dyn ConsensusStore,
    snapshot: &ReplicaSnapshot,
    expected_hard_state: RaftHardState,
) -> Result<(), ReplicaSnapshotError> {
    let metadata = block_on(consensus.snapshot_metadata())?.ok_or(
        ReplicaSnapshotError::InvalidRequest("consensus snapshot metadata is missing"),
    )?;
    if metadata.snapshot_id != snapshot.header.snapshot_id().get()
        || metadata.last_included_term != snapshot.manifest.last_included_term
        || metadata.last_included_index != snapshot.manifest.last_included_index
        || metadata.content_digest != snapshot.manifest.logical_digest
        || block_on(consensus.hard_state())? != expected_hard_state
        || block_on(consensus.membership())? != snapshot.membership
    {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "consensus snapshot installation did not persist exactly",
        ));
    }
    validate_retained_suffix(
        consensus,
        snapshot.manifest.last_included_index,
        expected_hard_state.committed_index,
    )
}

fn validate_retained_suffix(
    consensus: &dyn ConsensusStore,
    snapshot_index: u64,
    committed_index: u64,
) -> Result<(), ReplicaSnapshotError> {
    if committed_index <= snapshot_index {
        return Ok(());
    }
    let low = snapshot_index
        .checked_add(1)
        .ok_or(ReplicaSnapshotError::InvalidRequest(
            "snapshot index overflow",
        ))?;
    let high = committed_index
        .checked_add(1)
        .ok_or(ReplicaSnapshotError::InvalidRequest(
            "commit index overflow",
        ))?;
    let entries = block_on(consensus.entries(low, high, u64::MAX))?;
    let expected = committed_index - snapshot_index;
    if entries.len() as u64 != expected
        || entries
            .iter()
            .enumerate()
            .any(|(offset, entry)| entry.index() != low + offset as u64)
    {
        return Err(ReplicaSnapshotError::InvalidRequest(
            "retained WAL suffix is missing or discontiguous",
        ));
    }
    Ok(())
}

fn same_binding_except_role(left: &ReplicaBinding, right: &ReplicaBinding) -> bool {
    left.to_builder()
        .role(right.role())
        .build()
        .is_ok_and(|normalized| normalized == *right)
}

fn compatible_snapshot_target(source: &ReplicaBinding, target: &ReplicaBinding) -> bool {
    source.cluster_id() == target.cluster_id()
        && source.graph_id() == target.graph_id()
        && source.shard_id() == target.shard_id()
        && source.placement_epoch() == target.placement_epoch()
        && source.backend_generation() == target.backend_generation()
        && source.backend_class_digest() == target.backend_class_digest()
        && source.provider_kind() == target.provider_kind()
        && source.contract_version() == target.contract_version()
        && source.layout_version() == target.layout_version()
        && source.capability_digest() == target.capability_digest()
}

fn consensus_term(consensus: &dyn ConsensusStore, index: u64) -> Result<u64, ReplicaSnapshotError> {
    if let Some(snapshot) = block_on(consensus.snapshot_metadata())?
        && snapshot.last_included_index == index
    {
        return Ok(snapshot.last_included_term);
    }
    let high = index
        .checked_add(1)
        .ok_or(ReplicaSnapshotError::InvalidRequest(
            "snapshot index overflow",
        ))?;
    let entries = block_on(consensus.entries(index, high, u64::MAX))?;
    entries
        .first()
        .filter(|entry| entry.index() == index && entry.term() != 0)
        .map(dtg_storage::ConsensusEntry::term)
        .ok_or(ReplicaSnapshotError::InvalidRequest(
            "snapshot term is unavailable",
        ))
}

fn snapshot_bytes(chunks: &[SnapshotChunk]) -> Result<u64, ReplicaSnapshotError> {
    chunks.iter().try_fold(0_u64, |bytes, chunk| {
        let chunk_bytes = chunk.encoded_len()?;
        bytes
            .checked_add(chunk_bytes)
            .ok_or(ReplicaSnapshotError::InvalidRequest(
                "snapshot byte count overflow",
            ))
    })
}
