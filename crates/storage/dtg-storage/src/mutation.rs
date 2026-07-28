use std::collections::BTreeMap;

use dtg_kernel::{Digest32, TransactionId, TransactionTime, ValidInterval, Value, Version};

use crate::{ReplicaBinding, StorageError};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct VertexId(u128);

impl VertexId {
    pub fn new(value: u128) -> Result<Self, StorageError> {
        (value != 0).then_some(Self(value)).ok_or_else(|| {
            StorageError::InvalidMutation("vertex identifier must be nonzero".into())
        })
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct EdgeId(u128);

impl EdgeId {
    pub fn new(value: u128) -> Result<Self, StorageError> {
        (value != 0)
            .then_some(Self(value))
            .ok_or_else(|| StorageError::InvalidMutation("edge identifier must be nonzero".into()))
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

pub type Properties = BTreeMap<String, Value>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexVersion {
    id: VertexId,
    version: Version,
    valid_time: ValidInterval,
    transaction_time: TransactionTime,
    properties: Properties,
}

impl VertexVersion {
    pub fn new(
        id: VertexId,
        version: Version,
        valid_time: ValidInterval,
        transaction_time: TransactionTime,
        properties: Properties,
    ) -> Result<Self, StorageError> {
        validate_properties(&properties)?;
        Ok(Self {
            id,
            version,
            valid_time,
            transaction_time,
            properties,
        })
    }

    pub const fn id(&self) -> VertexId {
        self.id
    }

    pub const fn version(&self) -> Version {
        self.version
    }

    pub const fn valid_time(&self) -> ValidInterval {
        self.valid_time
    }

    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    pub const fn properties(&self) -> &Properties {
        &self.properties
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeVersion {
    id: EdgeId,
    source: VertexId,
    target: VertexId,
    edge_type: String,
    version: Version,
    valid_time: ValidInterval,
    transaction_time: TransactionTime,
    properties: Properties,
}

impl EdgeVersion {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: EdgeId,
        source: VertexId,
        target: VertexId,
        edge_type: impl Into<String>,
        version: Version,
        valid_time: ValidInterval,
        transaction_time: TransactionTime,
        properties: Properties,
    ) -> Result<Self, StorageError> {
        let edge_type = edge_type.into();
        if edge_type.is_empty() {
            return Err(StorageError::InvalidMutation(
                "edge type must be nonempty".into(),
            ));
        }
        validate_properties(&properties)?;
        Ok(Self {
            id,
            source,
            target,
            edge_type,
            version,
            valid_time,
            transaction_time,
            properties,
        })
    }

    pub const fn id(&self) -> EdgeId {
        self.id
    }

    pub const fn source(&self) -> VertexId {
        self.source
    }

    pub const fn target(&self) -> VertexId {
        self.target
    }

    pub fn edge_type(&self) -> &str {
        &self.edge_type
    }

    pub const fn version(&self) -> Version {
        self.version
    }

    pub const fn valid_time(&self) -> ValidInterval {
        self.valid_time
    }

    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    pub const fn properties(&self) -> &Properties {
        &self.properties
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexTombstone {
    id: VertexId,
    version: Version,
    transaction_time: TransactionTime,
}

impl VertexTombstone {
    pub const fn new(id: VertexId, version: Version, transaction_time: TransactionTime) -> Self {
        Self {
            id,
            version,
            transaction_time,
        }
    }

    pub const fn id(&self) -> VertexId {
        self.id
    }

    pub const fn version(&self) -> Version {
        self.version
    }

    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeTombstone {
    id: EdgeId,
    version: Version,
    transaction_time: TransactionTime,
}

impl EdgeTombstone {
    pub const fn new(id: EdgeId, version: Version, transaction_time: TransactionTime) -> Self {
        Self {
            id,
            version,
            transaction_time,
        }
    }

    pub const fn id(&self) -> EdgeId {
        self.id
    }

    pub const fn version(&self) -> Version {
        self.version
    }

    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum TransactionState {
    Prepared,
    Committed,
    Aborted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionRecord {
    id: TransactionId,
    state: TransactionState,
    transaction_time: TransactionTime,
    record_digest: Digest32,
}

impl TransactionRecord {
    pub fn new(
        id: TransactionId,
        state: TransactionState,
        transaction_time: TransactionTime,
        record_digest: Digest32,
    ) -> Result<Self, StorageError> {
        if record_digest.get() == [0; 32] {
            return Err(StorageError::InvalidMutation(
                "transaction record digest must be nonzero".into(),
            ));
        }
        Ok(Self {
            id,
            state,
            transaction_time,
            record_digest,
        })
    }

    pub const fn id(&self) -> TransactionId {
        self.id
    }

    pub const fn state(&self) -> TransactionState {
        self.state
    }

    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    pub const fn record_digest(&self) -> Digest32 {
        self.record_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaMetadata {
    name: String,
    value: Value,
}

impl ReplicaMetadata {
    pub fn new(name: impl Into<String>, value: Value) -> Result<Self, StorageError> {
        let name = name.into();
        if name.is_empty() {
            return Err(StorageError::InvalidMutation(
                "replica metadata name must be nonempty".into(),
            ));
        }
        Ok(Self { name, value })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn value(&self) -> &Value {
        &self.value
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LogicalMutation {
    PutVertex(VertexVersion),
    DeleteVertex(VertexTombstone),
    /// Applies a committed edge version without resolving its endpoints locally.
    /// Referential and identity constraints belong to execution and may span Shards.
    PutEdge(EdgeVersion),
    DeleteEdge(EdgeTombstone),
    PutTransaction(TransactionRecord),
    PutReplicaMetadata(ReplicaMetadata),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct CommandId(u128);

impl CommandId {
    pub fn new(value: u128) -> Result<Self, StorageError> {
        (value != 0)
            .then_some(Self(value))
            .ok_or_else(|| StorageError::InvalidBatch("command identifier must be nonzero".into()))
    }

    pub const fn get(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommittedShardBatch {
    binding: ReplicaBinding,
    raft_term: u64,
    raft_index: u64,
    command_id: CommandId,
    mutation_digest: Digest32,
    mutations: Vec<LogicalMutation>,
}

impl CommittedShardBatch {
    pub fn new(
        binding: ReplicaBinding,
        raft_term: u64,
        raft_index: u64,
        command_id: CommandId,
        mutations: Vec<LogicalMutation>,
    ) -> Result<Self, StorageError> {
        if raft_term == 0 || raft_index == 0 {
            return Err(StorageError::InvalidBatch(
                "Raft term and index must be nonzero".into(),
            ));
        }
        if mutations.is_empty() {
            return Err(StorageError::InvalidBatch(
                "committed batch must contain at least one mutation".into(),
            ));
        }
        let mutation_digest = digest_mutations(&mutations);
        Ok(Self {
            binding,
            raft_term,
            raft_index,
            command_id,
            mutation_digest,
            mutations,
        })
    }

    pub fn validate(&self) -> Result<(), StorageError> {
        if self.raft_term == 0 || self.raft_index == 0 || self.mutations.is_empty() {
            return Err(StorageError::InvalidBatch(
                "committed batch identity is incomplete".into(),
            ));
        }
        if self.mutation_digest != digest_mutations(&self.mutations) {
            return Err(StorageError::InvalidBatch(
                "mutation digest does not match typed mutations".into(),
            ));
        }
        Ok(())
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn raft_term(&self) -> u64 {
        self.raft_term
    }

    pub const fn raft_index(&self) -> u64 {
        self.raft_index
    }

    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    pub const fn mutation_digest(&self) -> Digest32 {
        self.mutation_digest
    }

    pub fn mutations(&self) -> &[LogicalMutation] {
        &self.mutations
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyReceipt {
    binding: ReplicaBinding,
    raft_term: u64,
    raft_index: u64,
    command_id: CommandId,
    mutation_digest: Digest32,
    replayed: bool,
}

impl ApplyReceipt {
    pub fn new(batch: &CommittedShardBatch, replayed: bool) -> Self {
        Self {
            binding: batch.binding.clone(),
            raft_term: batch.raft_term,
            raft_index: batch.raft_index,
            command_id: batch.command_id,
            mutation_digest: batch.mutation_digest,
            replayed,
        }
    }

    pub const fn binding(&self) -> &ReplicaBinding {
        &self.binding
    }

    pub const fn raft_term(&self) -> u64 {
        self.raft_term
    }

    pub const fn raft_index(&self) -> u64 {
        self.raft_index
    }

    pub const fn command_id(&self) -> CommandId {
        self.command_id
    }

    pub const fn mutation_digest(&self) -> Digest32 {
        self.mutation_digest
    }

    pub const fn replayed(&self) -> bool {
        self.replayed
    }
}

fn validate_properties(properties: &Properties) -> Result<(), StorageError> {
    if properties.keys().any(String::is_empty) {
        Err(StorageError::InvalidMutation(
            "property names must be nonempty".into(),
        ))
    } else {
        Ok(())
    }
}

pub(crate) fn digest_mutations(mutations: &[LogicalMutation]) -> Digest32 {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"dtg-typed-mutations-v1");
    hasher.update(&(mutations.len() as u64).to_be_bytes());
    for mutation in mutations {
        encode_mutation(&mut hasher, mutation);
    }
    Digest32::new(*hasher.finalize().as_bytes())
}

pub(crate) fn encode_mutation(hasher: &mut blake3::Hasher, mutation: &LogicalMutation) {
    match mutation {
        LogicalMutation::PutVertex(vertex) => {
            hasher.update(&[1]);
            encode_vertex(hasher, vertex);
        }
        LogicalMutation::DeleteVertex(tombstone) => {
            hasher.update(&[2]);
            hasher.update(&tombstone.id.get().to_be_bytes());
            hasher.update(&tombstone.version.get().to_be_bytes());
            hasher.update(&tombstone.transaction_time.get().to_be_bytes());
        }
        LogicalMutation::PutEdge(edge) => {
            hasher.update(&[3]);
            encode_edge(hasher, edge);
        }
        LogicalMutation::DeleteEdge(tombstone) => {
            hasher.update(&[4]);
            hasher.update(&tombstone.id.get().to_be_bytes());
            hasher.update(&tombstone.version.get().to_be_bytes());
            hasher.update(&tombstone.transaction_time.get().to_be_bytes());
        }
        LogicalMutation::PutTransaction(transaction) => {
            hasher.update(&[5]);
            hasher.update(&transaction.id.get().to_be_bytes());
            hasher.update(&[match transaction.state {
                TransactionState::Prepared => 1,
                TransactionState::Committed => 2,
                TransactionState::Aborted => 3,
            }]);
            hasher.update(&transaction.transaction_time.get().to_be_bytes());
            hasher.update(&transaction.record_digest.get());
        }
        LogicalMutation::PutReplicaMetadata(metadata) => {
            hasher.update(&[6]);
            encode_string(hasher, &metadata.name);
            encode_value(hasher, &metadata.value);
        }
    }
}

pub(crate) fn encode_vertex(hasher: &mut blake3::Hasher, vertex: &VertexVersion) {
    hasher.update(&vertex.id.get().to_be_bytes());
    hasher.update(&vertex.version.get().to_be_bytes());
    encode_interval(hasher, vertex.valid_time);
    hasher.update(&vertex.transaction_time.get().to_be_bytes());
    encode_properties(hasher, &vertex.properties);
}

pub(crate) fn encode_edge(hasher: &mut blake3::Hasher, edge: &EdgeVersion) {
    hasher.update(&edge.id.get().to_be_bytes());
    hasher.update(&edge.source.get().to_be_bytes());
    hasher.update(&edge.target.get().to_be_bytes());
    encode_string(hasher, &edge.edge_type);
    hasher.update(&edge.version.get().to_be_bytes());
    encode_interval(hasher, edge.valid_time);
    hasher.update(&edge.transaction_time.get().to_be_bytes());
    encode_properties(hasher, &edge.properties);
}

pub(crate) fn encode_transaction(hasher: &mut blake3::Hasher, transaction: &TransactionRecord) {
    hasher.update(&transaction.id.get().to_be_bytes());
    hasher.update(&[match transaction.state {
        TransactionState::Prepared => 1,
        TransactionState::Committed => 2,
        TransactionState::Aborted => 3,
    }]);
    hasher.update(&transaction.transaction_time.get().to_be_bytes());
    hasher.update(&transaction.record_digest.get());
}

pub(crate) fn encode_metadata(hasher: &mut blake3::Hasher, metadata: &ReplicaMetadata) {
    encode_string(hasher, &metadata.name);
    encode_value(hasher, &metadata.value);
}

fn encode_interval(hasher: &mut blake3::Hasher, interval: ValidInterval) {
    hasher.update(&interval.start().to_be_bytes());
    hasher.update(&interval.end().to_be_bytes());
}

fn encode_properties(hasher: &mut blake3::Hasher, properties: &Properties) {
    hasher.update(&(properties.len() as u64).to_be_bytes());
    for (name, value) in properties {
        encode_string(hasher, name);
        encode_value(hasher, value);
    }
}

fn encode_string(hasher: &mut blake3::Hasher, value: &str) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value.as_bytes());
}

pub(crate) fn encode_value(hasher: &mut blake3::Hasher, value: &Value) {
    match value {
        Value::Null => {
            hasher.update(&[0]);
        }
        Value::Boolean(value) => {
            hasher.update(&[1, u8::from(*value)]);
        }
        Value::Integer(value) => {
            hasher.update(&[2]);
            hasher.update(&value.to_be_bytes());
        }
        Value::FloatBits(value) => {
            hasher.update(&[3]);
            hasher.update(&value.to_be_bytes());
        }
        Value::Bytes(value) => {
            hasher.update(&[4]);
            hasher.update(&(value.len() as u64).to_be_bytes());
            hasher.update(value);
        }
        Value::String(value) => {
            hasher.update(&[5]);
            encode_string(hasher, value);
        }
        Value::List(values) => {
            hasher.update(&[6]);
            hasher.update(&(values.len() as u64).to_be_bytes());
            for value in values {
                encode_value(hasher, value);
            }
        }
        Value::Map(values) => {
            hasher.update(&[7]);
            hasher.update(&(values.len() as u64).to_be_bytes());
            for (name, value) in values {
                encode_string(hasher, name);
                encode_value(hasher, value);
            }
        }
    }
}
