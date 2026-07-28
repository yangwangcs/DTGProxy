use std::collections::BTreeMap;

use bincode::{Decode, Encode};
use dtg_storage::{
    ArtifactChunk, ArtifactKey, ArtifactKind, BindingRole, CommandId, ConsensusCommandEnvelope,
    ConsensusEntry, ConsensusSnapshotMetadata, Digest32, EdgeId, EdgeTombstone, EdgeVersion,
    LogicalMutation, ProviderKind, RaftHardState, RaftMembership, ReplicaBinding, ReplicaMetadata,
    StorageError, TransactionId, TransactionRecord, TransactionState, TransactionTime,
    ValidInterval, Value, Version, VertexId, VertexTombstone, VertexVersion,
};

const CODEC_VERSION: u32 = 1;

fn encode<T: Encode>(value: &T) -> Result<Vec<u8>, StorageError> {
    bincode::encode_to_vec(value, bincode::config::standard())
        .map_err(|error| StorageError::Internal(format!("Fjall encoding failed: {error}")))
}

fn decode<T: Decode<()>>(bytes: &[u8]) -> Result<T, StorageError> {
    let (value, consumed) = bincode::decode_from_slice(bytes, bincode::config::standard())
        .map_err(|error| StorageError::Internal(format!("Fjall decoding failed: {error}")))?;
    if consumed != bytes.len() {
        return Err(StorageError::Internal(
            "Fjall value contains trailing bytes".into(),
        ));
    }
    Ok(value)
}

fn require_version(version: u32, name: &str) -> Result<(), StorageError> {
    if version == CODEC_VERSION {
        Ok(())
    } else {
        Err(StorageError::Internal(format!(
            "unsupported Fjall {name} codec version {version}"
        )))
    }
}

#[derive(Encode, Decode)]
enum WireProviderKind {
    Fjall,
    PostgreSql,
    Neo4j,
    Remote(String),
}

#[derive(Encode, Decode)]
struct WireBinding {
    version: u32,
    cluster_id: u64,
    graph_id: u64,
    shard_id: u64,
    placement_epoch: u64,
    replica_id: u64,
    backend_generation: u64,
    backend_class_digest: [u8; 32],
    provider_kind: WireProviderKind,
    contract_version: u32,
    layout_version: u32,
    capability_digest: [u8; 32],
    namespace_id: String,
    endpoint_profile_ref: String,
    credential_ref: String,
    role: u8,
}

pub(crate) fn encode_binding(binding: &ReplicaBinding) -> Result<Vec<u8>, StorageError> {
    let provider_kind = match binding.provider_kind() {
        ProviderKind::Fjall => WireProviderKind::Fjall,
        ProviderKind::PostgreSql => WireProviderKind::PostgreSql,
        ProviderKind::Neo4j => WireProviderKind::Neo4j,
        ProviderKind::Remote(name) => WireProviderKind::Remote(name.clone()),
    };
    encode(&WireBinding {
        version: CODEC_VERSION,
        cluster_id: binding.cluster_id().get(),
        graph_id: binding.graph_id().get(),
        shard_id: binding.shard_id().get(),
        placement_epoch: binding.placement_epoch().get(),
        replica_id: binding.replica_id().get(),
        backend_generation: binding.backend_generation().get(),
        backend_class_digest: binding.backend_class_digest().get(),
        provider_kind,
        contract_version: binding.contract_version(),
        layout_version: binding.layout_version(),
        capability_digest: binding.capability_digest().get(),
        namespace_id: binding.namespace_id().as_str().to_owned(),
        endpoint_profile_ref: binding.endpoint_profile_ref().to_owned(),
        credential_ref: binding.credential_ref().to_owned(),
        role: match binding.role() {
            BindingRole::Candidate => 1,
            BindingRole::Active => 2,
            BindingRole::Retiring => 3,
        },
    })
}

pub(crate) fn decode_binding(bytes: &[u8]) -> Result<ReplicaBinding, StorageError> {
    let wire: WireBinding = decode(bytes)?;
    require_version(wire.version, "owner")?;
    let provider_kind = match wire.provider_kind {
        WireProviderKind::Fjall => ProviderKind::Fjall,
        WireProviderKind::PostgreSql => ProviderKind::PostgreSql,
        WireProviderKind::Neo4j => ProviderKind::Neo4j,
        WireProviderKind::Remote(name) => ProviderKind::Remote(name),
    };
    let role = match wire.role {
        1 => BindingRole::Candidate,
        2 => BindingRole::Active,
        3 => BindingRole::Retiring,
        tag => {
            return Err(StorageError::Internal(format!(
                "invalid Fjall owner role tag {tag}"
            )));
        }
    };
    ReplicaBinding::builder()
        .cluster_id(wire.cluster_id)
        .graph_id(wire.graph_id)
        .shard_id(wire.shard_id)
        .placement_epoch(wire.placement_epoch)
        .replica_id(wire.replica_id)
        .backend_generation(wire.backend_generation)
        .backend_class_digest(Digest32::new(wire.backend_class_digest))
        .provider_kind(provider_kind)
        .contract_version(wire.contract_version)
        .layout_version(wire.layout_version)
        .capability_digest(Digest32::new(wire.capability_digest))
        .namespace_id(wire.namespace_id)
        .endpoint_profile_ref(wire.endpoint_profile_ref)
        .credential_ref(wire.credential_ref)
        .role(role)
        .build()
}

#[derive(Encode, Decode)]
enum WireValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    Bytes(Vec<u8>),
    String(String),
    List(Vec<WireValue>),
    Map(BTreeMap<String, WireValue>),
}

impl From<&Value> for WireValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Boolean(value) => Self::Boolean(*value),
            Value::Integer(value) => Self::Integer(*value),
            Value::FloatBits(value) => Self::FloatBits(*value),
            Value::Bytes(value) => Self::Bytes(value.clone()),
            Value::String(value) => Self::String(value.clone()),
            Value::List(values) => Self::List(values.iter().map(Self::from).collect()),
            Value::Map(values) => Self::Map(
                values
                    .iter()
                    .map(|(key, value)| (key.clone(), Self::from(value)))
                    .collect(),
            ),
        }
    }
}

impl From<WireValue> for Value {
    fn from(value: WireValue) -> Self {
        match value {
            WireValue::Null => Self::Null,
            WireValue::Boolean(value) => Self::Boolean(value),
            WireValue::Integer(value) => Self::Integer(value),
            WireValue::FloatBits(value) => Self::FloatBits(value),
            WireValue::Bytes(value) => Self::Bytes(value),
            WireValue::String(value) => Self::String(value),
            WireValue::List(values) => Self::List(values.into_iter().map(Self::from).collect()),
            WireValue::Map(values) => Self::Map(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Self::from(value)))
                    .collect(),
            ),
        }
    }
}

#[derive(Encode, Decode)]
struct WireVertex {
    id: u128,
    version: u64,
    valid_start: i64,
    valid_end: i64,
    transaction_time: i64,
    properties: BTreeMap<String, WireValue>,
}

#[derive(Encode, Decode)]
struct WireEdge {
    id: u128,
    source: u128,
    target: u128,
    edge_type: String,
    version: u64,
    valid_start: i64,
    valid_end: i64,
    transaction_time: i64,
    properties: BTreeMap<String, WireValue>,
}

#[derive(Encode, Decode)]
struct WireTombstone {
    id: u128,
    version: u64,
    transaction_time: i64,
}

#[derive(Encode, Decode)]
struct WireTransaction {
    id: u128,
    state: u8,
    transaction_time: i64,
    digest: [u8; 32],
}

#[derive(Encode, Decode)]
struct WireMetadata {
    name: String,
    value: WireValue,
}

#[derive(Encode, Decode)]
enum WireMutation {
    PutVertex(WireVertex),
    DeleteVertex(WireTombstone),
    PutEdge(WireEdge),
    DeleteEdge(WireTombstone),
    PutTransaction(WireTransaction),
    PutReplicaMetadata(WireMetadata),
}

#[derive(Encode, Decode)]
struct VersionedMutation {
    version: u32,
    mutation: WireMutation,
}

fn wire_vertex(vertex: &VertexVersion) -> WireVertex {
    WireVertex {
        id: vertex.id().get(),
        version: vertex.version().get(),
        valid_start: vertex.valid_time().start(),
        valid_end: vertex.valid_time().end(),
        transaction_time: vertex.transaction_time().get(),
        properties: vertex
            .properties()
            .iter()
            .map(|(key, value)| (key.clone(), WireValue::from(value)))
            .collect(),
    }
}

fn wire_edge(edge: &EdgeVersion) -> WireEdge {
    WireEdge {
        id: edge.id().get(),
        source: edge.source().get(),
        target: edge.target().get(),
        edge_type: edge.edge_type().to_owned(),
        version: edge.version().get(),
        valid_start: edge.valid_time().start(),
        valid_end: edge.valid_time().end(),
        transaction_time: edge.transaction_time().get(),
        properties: edge
            .properties()
            .iter()
            .map(|(key, value)| (key.clone(), WireValue::from(value)))
            .collect(),
    }
}

fn wire_transaction(transaction: &TransactionRecord) -> WireTransaction {
    WireTransaction {
        id: transaction.id().get(),
        state: match transaction.state() {
            TransactionState::Prepared => 1,
            TransactionState::Committed => 2,
            TransactionState::Aborted => 3,
        },
        transaction_time: transaction.transaction_time().get(),
        digest: transaction.record_digest().get(),
    }
}

fn wire_metadata(metadata: &ReplicaMetadata) -> WireMetadata {
    WireMetadata {
        name: metadata.name().to_owned(),
        value: WireValue::from(metadata.value()),
    }
}

pub(crate) fn encode_mutation(mutation: &LogicalMutation) -> Result<Vec<u8>, StorageError> {
    let mutation = match mutation {
        LogicalMutation::PutVertex(vertex) => WireMutation::PutVertex(wire_vertex(vertex)),
        LogicalMutation::DeleteVertex(tombstone) => WireMutation::DeleteVertex(WireTombstone {
            id: tombstone.id().get(),
            version: tombstone.version().get(),
            transaction_time: tombstone.transaction_time().get(),
        }),
        LogicalMutation::PutEdge(edge) => WireMutation::PutEdge(wire_edge(edge)),
        LogicalMutation::DeleteEdge(tombstone) => WireMutation::DeleteEdge(WireTombstone {
            id: tombstone.id().get(),
            version: tombstone.version().get(),
            transaction_time: tombstone.transaction_time().get(),
        }),
        LogicalMutation::PutTransaction(transaction) => {
            WireMutation::PutTransaction(wire_transaction(transaction))
        }
        LogicalMutation::PutReplicaMetadata(metadata) => {
            WireMutation::PutReplicaMetadata(wire_metadata(metadata))
        }
    };
    encode(&VersionedMutation {
        version: CODEC_VERSION,
        mutation,
    })
}

pub(crate) fn decode_mutation(bytes: &[u8]) -> Result<LogicalMutation, StorageError> {
    let wire: VersionedMutation = decode(bytes)?;
    require_version(wire.version, "mutation")?;
    match wire.mutation {
        WireMutation::PutVertex(vertex) => {
            Ok(LogicalMutation::PutVertex(vertex_from_wire(vertex)?))
        }
        WireMutation::DeleteVertex(tombstone) => {
            Ok(LogicalMutation::DeleteVertex(VertexTombstone::new(
                VertexId::new(tombstone.id)?,
                Version::new(tombstone.version),
                transaction_time(tombstone.transaction_time)?,
            )))
        }
        WireMutation::PutEdge(edge) => Ok(LogicalMutation::PutEdge(edge_from_wire(edge)?)),
        WireMutation::DeleteEdge(tombstone) => Ok(LogicalMutation::DeleteEdge(EdgeTombstone::new(
            EdgeId::new(tombstone.id)?,
            Version::new(tombstone.version),
            transaction_time(tombstone.transaction_time)?,
        ))),
        WireMutation::PutTransaction(transaction) => Ok(LogicalMutation::PutTransaction(
            transaction_from_wire(transaction)?,
        )),
        WireMutation::PutReplicaMetadata(metadata) => Ok(LogicalMutation::PutReplicaMetadata(
            ReplicaMetadata::new(metadata.name, Value::from(metadata.value))?,
        )),
    }
}

fn transaction_time(value: i64) -> Result<TransactionTime, StorageError> {
    TransactionTime::new(value).map_err(|error| {
        StorageError::Internal(format!("invalid stored transaction time: {error}"))
    })
}

fn valid_interval(start: i64, end: i64) -> Result<ValidInterval, StorageError> {
    ValidInterval::new(start, end)
        .map_err(|error| StorageError::Internal(format!("invalid stored valid interval: {error}")))
}

fn vertex_from_wire(vertex: WireVertex) -> Result<VertexVersion, StorageError> {
    VertexVersion::new(
        VertexId::new(vertex.id)?,
        Version::new(vertex.version),
        valid_interval(vertex.valid_start, vertex.valid_end)?,
        transaction_time(vertex.transaction_time)?,
        vertex
            .properties
            .into_iter()
            .map(|(key, value)| (key, Value::from(value)))
            .collect(),
    )
}

fn edge_from_wire(edge: WireEdge) -> Result<EdgeVersion, StorageError> {
    EdgeVersion::new(
        EdgeId::new(edge.id)?,
        VertexId::new(edge.source)?,
        VertexId::new(edge.target)?,
        edge.edge_type,
        Version::new(edge.version),
        valid_interval(edge.valid_start, edge.valid_end)?,
        transaction_time(edge.transaction_time)?,
        edge.properties
            .into_iter()
            .map(|(key, value)| (key, Value::from(value)))
            .collect(),
    )
}

fn transaction_from_wire(transaction: WireTransaction) -> Result<TransactionRecord, StorageError> {
    let state = match transaction.state {
        1 => TransactionState::Prepared,
        2 => TransactionState::Committed,
        3 => TransactionState::Aborted,
        tag => {
            return Err(StorageError::Internal(format!(
                "invalid stored transaction state {tag}"
            )));
        }
    };
    TransactionRecord::new(
        TransactionId::new(transaction.id)
            .map_err(|error| StorageError::Internal(error.to_string()))?,
        state,
        transaction_time(transaction.transaction_time)?,
        Digest32::new(transaction.digest),
    )
}

#[derive(Encode, Decode)]
struct WireReplayIdentity {
    version: u32,
    term: u64,
    command_id: u128,
    mutation_digest: [u8; 32],
}

pub(crate) struct ReplayIdentity {
    pub(crate) term: u64,
    pub(crate) command_id: CommandId,
    pub(crate) mutation_digest: Digest32,
}

pub(crate) fn encode_replay_identity(
    term: u64,
    command_id: CommandId,
    mutation_digest: Digest32,
) -> Result<Vec<u8>, StorageError> {
    encode(&WireReplayIdentity {
        version: CODEC_VERSION,
        term,
        command_id: command_id.get(),
        mutation_digest: mutation_digest.get(),
    })
}

pub(crate) fn decode_replay_identity(bytes: &[u8]) -> Result<ReplayIdentity, StorageError> {
    let wire: WireReplayIdentity = decode(bytes)?;
    require_version(wire.version, "replay identity")?;
    Ok(ReplayIdentity {
        term: wire.term,
        command_id: CommandId::new(wire.command_id)?,
        mutation_digest: Digest32::new(wire.mutation_digest),
    })
}

#[derive(Encode, Decode)]
struct WireConsensusEntry {
    version: u32,
    wal_format_version: u32,
    term: u64,
    index: u64,
    command_id: u128,
    command_format_version: u32,
    payload: Vec<u8>,
}

pub(crate) fn encode_consensus_entry(entry: &ConsensusEntry) -> Result<Vec<u8>, StorageError> {
    encode(&WireConsensusEntry {
        version: CODEC_VERSION,
        wal_format_version: entry.wal_format_version(),
        term: entry.term(),
        index: entry.index(),
        command_id: entry.command_id().get(),
        command_format_version: entry.command().format_version(),
        payload: entry.command().payload().to_vec(),
    })
}

pub(crate) fn decode_consensus_entry(bytes: &[u8]) -> Result<ConsensusEntry, StorageError> {
    let wire: WireConsensusEntry = decode(bytes)?;
    require_version(wire.version, "consensus entry")?;
    ConsensusEntry::new(
        wire.wal_format_version,
        wire.term,
        wire.index,
        CommandId::new(wire.command_id)?,
        ConsensusCommandEnvelope::new(wire.command_format_version, wire.payload)?,
    )
}

#[derive(Encode, Decode)]
struct WireHardState {
    version: u32,
    current_term: u64,
    voted_for: Option<u64>,
    committed_index: u64,
}

pub(crate) fn encode_hard_state(state: RaftHardState) -> Result<Vec<u8>, StorageError> {
    encode(&WireHardState {
        version: CODEC_VERSION,
        current_term: state.current_term,
        voted_for: state.voted_for.map(|id| id.get()),
        committed_index: state.committed_index,
    })
}

pub(crate) fn decode_hard_state(bytes: &[u8]) -> Result<RaftHardState, StorageError> {
    let wire: WireHardState = decode(bytes)?;
    require_version(wire.version, "hard state")?;
    Ok(RaftHardState {
        current_term: wire.current_term,
        voted_for: wire
            .voted_for
            .map(|id| {
                dtg_storage::ReplicaId::new(id)
                    .map_err(|error| StorageError::InvalidConsensus(error.to_string()))
            })
            .transpose()?,
        committed_index: wire.committed_index,
    })
}

#[derive(Encode, Decode)]
struct WireMembership {
    version: u32,
    voters: Vec<u64>,
    learners: Vec<u64>,
    configuration_index: u64,
}

pub(crate) fn encode_membership(membership: &RaftMembership) -> Result<Vec<u8>, StorageError> {
    encode(&WireMembership {
        version: CODEC_VERSION,
        voters: membership.voters.iter().map(|id| id.get()).collect(),
        learners: membership.learners.iter().map(|id| id.get()).collect(),
        configuration_index: membership.configuration_index,
    })
}

pub(crate) fn decode_membership(bytes: &[u8]) -> Result<RaftMembership, StorageError> {
    let wire: WireMembership = decode(bytes)?;
    require_version(wire.version, "membership")?;
    let decode_id = |id| {
        dtg_storage::ReplicaId::new(id)
            .map_err(|error| StorageError::InvalidConsensus(error.to_string()))
    };
    Ok(RaftMembership {
        voters: wire
            .voters
            .into_iter()
            .map(decode_id)
            .collect::<Result<_, _>>()?,
        learners: wire
            .learners
            .into_iter()
            .map(decode_id)
            .collect::<Result<_, _>>()?,
        configuration_index: wire.configuration_index,
    })
}

#[derive(Encode, Decode)]
struct WireConsensusSnapshot {
    version: u32,
    snapshot_id: u128,
    last_included_term: u64,
    last_included_index: u64,
    content_digest: [u8; 32],
}

pub(crate) fn encode_consensus_snapshot(
    metadata: &ConsensusSnapshotMetadata,
) -> Result<Vec<u8>, StorageError> {
    encode(&WireConsensusSnapshot {
        version: CODEC_VERSION,
        snapshot_id: metadata.snapshot_id,
        last_included_term: metadata.last_included_term,
        last_included_index: metadata.last_included_index,
        content_digest: metadata.content_digest.get(),
    })
}

pub(crate) fn decode_consensus_snapshot(
    bytes: &[u8],
) -> Result<ConsensusSnapshotMetadata, StorageError> {
    let wire: WireConsensusSnapshot = decode(bytes)?;
    require_version(wire.version, "consensus snapshot")?;
    Ok(ConsensusSnapshotMetadata {
        snapshot_id: wire.snapshot_id,
        last_included_term: wire.last_included_term,
        last_included_index: wire.last_included_index,
        content_digest: Digest32::new(wire.content_digest),
    })
}

#[derive(Encode, Decode)]
struct WireArtifactChunk {
    version: u32,
    job_id: u128,
    generation: u64,
    kind: u8,
    ordinal: u64,
    payload: Vec<u8>,
}

fn artifact_kind(tag: u8) -> Result<ArtifactKind, StorageError> {
    match tag {
        1 => Ok(ArtifactKind::Checkpoint),
        2 => Ok(ArtifactKind::Result),
        _ => Err(StorageError::InvalidArtifact(
            "invalid stored artifact kind".into(),
        )),
    }
}

pub(crate) fn encode_artifact_chunk(chunk: &ArtifactChunk) -> Result<Vec<u8>, StorageError> {
    encode(&WireArtifactChunk {
        version: CODEC_VERSION,
        job_id: chunk.key().job_id(),
        generation: chunk.key().generation(),
        kind: match chunk.key().kind() {
            ArtifactKind::Checkpoint => 1,
            ArtifactKind::Result => 2,
        },
        ordinal: chunk.ordinal(),
        payload: chunk.payload().to_vec(),
    })
}

pub(crate) fn decode_artifact_chunk(bytes: &[u8]) -> Result<ArtifactChunk, StorageError> {
    let wire: WireArtifactChunk = decode(bytes)?;
    require_version(wire.version, "artifact chunk")?;
    ArtifactChunk::new(
        ArtifactKey::new(wire.job_id, wire.generation, artifact_kind(wire.kind)?)?,
        wire.ordinal,
        wire.payload,
    )
}

#[derive(Encode, Decode)]
struct WireArtifactManifest {
    version: u32,
    job_id: u128,
    generation: u64,
    kind: u8,
    chunk_count: u64,
    total_bytes: u64,
    content_digest: [u8; 32],
}

pub(crate) fn encode_artifact_manifest(
    manifest: &dtg_storage::ArtifactManifest,
) -> Result<Vec<u8>, StorageError> {
    encode(&WireArtifactManifest {
        version: CODEC_VERSION,
        job_id: manifest.key().job_id(),
        generation: manifest.key().generation(),
        kind: match manifest.key().kind() {
            ArtifactKind::Checkpoint => 1,
            ArtifactKind::Result => 2,
        },
        chunk_count: manifest.chunk_count(),
        total_bytes: manifest.total_bytes(),
        content_digest: manifest.content_digest().get(),
    })
}

pub(crate) struct StoredArtifactManifest {
    pub(crate) key: ArtifactKey,
    pub(crate) chunk_count: u64,
    pub(crate) total_bytes: u64,
    pub(crate) content_digest: Digest32,
}

pub(crate) fn decode_artifact_manifest(
    bytes: &[u8],
) -> Result<StoredArtifactManifest, StorageError> {
    let wire: WireArtifactManifest = decode(bytes)?;
    require_version(wire.version, "artifact manifest")?;
    Ok(StoredArtifactManifest {
        key: ArtifactKey::new(wire.job_id, wire.generation, artifact_kind(wire.kind)?)?,
        chunk_count: wire.chunk_count,
        total_bytes: wire.total_bytes,
        content_digest: Digest32::new(wire.content_digest),
    })
}
