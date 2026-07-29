use std::collections::BTreeMap;

use dtg_kernel::{
    BackendGeneration, Digest32, PlacementEpoch, ShardId, TransactionId, TransactionTime,
    ValidInterval, Value, Version,
};
use dtg_storage::{
    CommandId, EdgeId, EdgeTombstone, EdgeVersion, LogicalMutation, Properties, ReplicaMetadata,
    TransactionRecord, TransactionState, VertexId, VertexTombstone, VertexVersion,
};

use crate::{MigrationCommand, MigrationPhase, ShardError};

pub const SUPPORTED_SHARD_COMMAND_FORMAT_VERSION: u32 = 2;
pub const SUPPORTED_TRANSACTION_INTENT_VERSION: u32 = 3;

pub const TRANSACTION_INTENT_METADATA_NAME: &str = "dtg.transaction_intent.v3";
const MAX_TRANSACTION_INTENT_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_TRANSACTION_INTENT_ITEMS: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommandHeader {
    command_id: CommandId,
    placement_epoch: PlacementEpoch,
    backend_generation: BackendGeneration,
}

impl CommandHeader {
    pub fn new(
        command_id: CommandId,
        placement_epoch: u64,
        backend_generation: u64,
    ) -> Result<Self, ShardError> {
        Ok(Self {
            command_id,
            placement_epoch: PlacementEpoch::new(placement_epoch).map_err(|_| {
                ShardError::InvalidCommand("placement epoch must be nonzero".into())
            })?,
            backend_generation: BackendGeneration::new(backend_generation).map_err(|_| {
                ShardError::InvalidCommand("backend generation must be nonzero".into())
            })?,
        })
    }

    pub const fn command_id(self) -> CommandId {
        self.command_id
    }

    pub const fn placement_epoch(self) -> PlacementEpoch {
        self.placement_epoch
    }

    pub const fn backend_generation(self) -> BackendGeneration {
        self.backend_generation
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitSingleShard {
    header: CommandHeader,
    mutations: Vec<LogicalMutation>,
}

impl CommitSingleShard {
    pub fn new(
        command_id: CommandId,
        placement_epoch: u64,
        backend_generation: u64,
        mutations: Vec<LogicalMutation>,
    ) -> Result<Self, ShardError> {
        if mutations.is_empty() {
            return Err(ShardError::InvalidCommand(
                "single-Shard commit must contain mutations".into(),
            ));
        }
        validate_single_shard_mutations(&mutations)?;
        Ok(Self {
            header: CommandHeader::new(command_id, placement_epoch, backend_generation)?,
            mutations,
        })
    }

    pub const fn header(&self) -> CommandHeader {
        self.header
    }

    pub fn mutations(&self) -> &[LogicalMutation] {
        &self.mutations
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommitSingleShardTransaction {
    header: CommandHeader,
    transaction_id: TransactionId,
    start_time: TransactionTime,
    snapshot_applied_index: u64,
    request_digest: Digest32,
    mutations: Vec<LogicalMutation>,
}

impl CommitSingleShardTransaction {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        command_id: CommandId,
        placement_epoch: u64,
        backend_generation: u64,
        transaction_id: TransactionId,
        start_time: TransactionTime,
        snapshot_applied_index: u64,
        request_digest: Digest32,
        mutations: Vec<LogicalMutation>,
    ) -> Result<Self, ShardError> {
        let command = Self {
            header: CommandHeader::new(command_id, placement_epoch, backend_generation)?,
            transaction_id,
            start_time,
            snapshot_applied_index,
            request_digest,
            mutations,
        };
        command.validate()?;
        Ok(command)
    }

    pub const fn header(&self) -> CommandHeader {
        self.header
    }

    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    pub const fn start_time(&self) -> TransactionTime {
        self.start_time
    }

    pub const fn snapshot_applied_index(&self) -> u64 {
        self.snapshot_applied_index
    }

    pub const fn request_digest(&self) -> Digest32 {
        self.request_digest
    }

    pub fn mutations(&self) -> &[LogicalMutation] {
        &self.mutations
    }

    fn validate(&self) -> Result<(), ShardError> {
        validate_intent_mutations(&self.mutations)?;
        validate_commit_follows_start(
            self.start_time,
            &self.mutations,
            "single-Shard transaction commit time must follow its start time",
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParticipantIntent {
    transaction_id: TransactionId,
    shard_id: ShardId,
    start_time: TransactionTime,
    snapshot_applied_index: u64,
    mutations: Vec<LogicalMutation>,
}

impl ParticipantIntent {
    pub fn new(
        transaction_id: TransactionId,
        shard_id: ShardId,
        start_time: TransactionTime,
        snapshot_applied_index: u64,
        mutations: Vec<LogicalMutation>,
    ) -> Result<Self, ShardError> {
        validate_intent_mutations(&mutations)?;
        let intent = Self {
            transaction_id,
            shard_id,
            start_time,
            snapshot_applied_index,
            mutations,
        };
        intent.encode_current()?;
        Ok(intent)
    }

    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    pub const fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub const fn start_time(&self) -> TransactionTime {
        self.start_time
    }

    pub const fn snapshot_applied_index(&self) -> u64 {
        self.snapshot_applied_index
    }

    pub fn mutations(&self) -> &[LogicalMutation] {
        &self.mutations
    }

    pub fn digest(&self) -> Digest32 {
        let encoded = self
            .encode_current()
            .expect("validated participant intent remains encodable");
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"dtg-transaction-participant-intent-v3");
        hasher.update(&encoded);
        Digest32::new(*hasher.finalize().as_bytes())
    }

    pub fn encode_current(&self) -> Result<Vec<u8>, ShardError> {
        validate_intent_mutations(&self.mutations)?;
        let mut encoder = Encoder::default();
        encoder.u32(SUPPORTED_TRANSACTION_INTENT_VERSION);
        encoder.u128(self.transaction_id.get());
        encoder.u64(self.shard_id.get());
        encoder.i64(self.start_time.get());
        encoder.u64(self.snapshot_applied_index);
        encoder.mutations(&self.mutations)?;
        let encoded = encoder.finish()?;
        if encoded.len() > MAX_TRANSACTION_INTENT_BYTES {
            return Err(invalid("transaction intent exceeds maximum encoded size"));
        }
        Ok(encoded)
    }

    pub fn decode_current(bytes: &[u8]) -> Result<Self, ShardError> {
        if bytes.len() > MAX_TRANSACTION_INTENT_BYTES {
            return Err(invalid("transaction intent bytes are oversized"));
        }
        let mut decoder = Decoder::new(bytes)?;
        let version = decoder.u32()?;
        if version != SUPPORTED_TRANSACTION_INTENT_VERSION {
            return Err(invalid("unsupported transaction intent version"));
        }
        let transaction_id = TransactionId::new(decoder.u128()?)?;
        let shard_id = ShardId::new(decoder.u64()?)?;
        let start_time = TransactionTime::new(decoder.i64()?)?;
        let snapshot_applied_index = decoder.u64()?;
        let mutations = decoder.mutations_with_limit(
            MAX_TRANSACTION_INTENT_ITEMS,
            "transaction intent mutation count is outside the supported bounds",
        )?;
        decoder.finish()?;
        Self::new(
            transaction_id,
            shard_id,
            start_time,
            snapshot_applied_index,
            mutations,
        )
    }

    fn metadata(&self) -> Result<ReplicaMetadata, ShardError> {
        Ok(ReplicaMetadata::new(
            TRANSACTION_INTENT_METADATA_NAME,
            Value::Bytes(self.encode_current()?),
        )?)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrewriteIntent {
    header: CommandHeader,
    prepared: TransactionRecord,
    intent: ParticipantIntent,
}

impl PrewriteIntent {
    pub fn new(
        command_id: CommandId,
        placement_epoch: u64,
        backend_generation: u64,
        prepared: TransactionRecord,
        intent: ParticipantIntent,
    ) -> Result<Self, ShardError> {
        let command = Self {
            header: CommandHeader::new(command_id, placement_epoch, backend_generation)?,
            prepared,
            intent,
        };
        command.validate()?;
        Ok(command)
    }

    pub const fn header(&self) -> CommandHeader {
        self.header
    }

    pub const fn prepared(&self) -> &TransactionRecord {
        &self.prepared
    }

    pub const fn intent(&self) -> &ParticipantIntent {
        &self.intent
    }

    fn validate(&self) -> Result<(), ShardError> {
        if self.prepared.state() != TransactionState::Prepared
            || self.prepared.id() != self.intent.transaction_id()
            || self.prepared.transaction_time() != self.intent.start_time()
            || self.prepared.record_digest() != self.intent.digest()
        {
            return Err(invalid_command_shape(
                "prewrite intent must bind one prepared record to its participant payload",
            ));
        }
        self.intent.encode_current()?;
        Ok(())
    }

    fn mutations(&self) -> Result<Vec<LogicalMutation>, ShardError> {
        self.validate()?;
        Ok(vec![
            LogicalMutation::PutTransaction(self.prepared.clone()),
            LogicalMutation::PutReplicaMetadata(self.intent.metadata()?),
        ])
    }
}

macro_rules! mutation_command {
    ($name:ident, $validator:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name {
            header: CommandHeader,
            mutations: Vec<LogicalMutation>,
        }

        impl $name {
            pub fn new(
                command_id: CommandId,
                placement_epoch: u64,
                backend_generation: u64,
                mutations: Vec<LogicalMutation>,
            ) -> Result<Self, ShardError> {
                if mutations.is_empty() {
                    return Err(ShardError::InvalidCommand(
                        concat!(stringify!($name), " must contain mutations").into(),
                    ));
                }
                $validator(&mutations)?;
                Ok(Self {
                    header: CommandHeader::new(command_id, placement_epoch, backend_generation)?,
                    mutations,
                })
            }

            pub const fn header(&self) -> CommandHeader {
                self.header
            }

            pub fn mutations(&self) -> &[LogicalMutation] {
                &self.mutations
            }
        }
    };
}

mutation_command!(RecordHomeDecision, validate_home_decision);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizeParticipant {
    header: CommandHeader,
    terminal: TransactionRecord,
    intent: Option<ParticipantIntent>,
}

impl FinalizeParticipant {
    pub fn new(
        command_id: CommandId,
        placement_epoch: u64,
        backend_generation: u64,
        terminal: TransactionRecord,
        intent: Option<ParticipantIntent>,
    ) -> Result<Self, ShardError> {
        let command = Self {
            header: CommandHeader::new(command_id, placement_epoch, backend_generation)?,
            terminal,
            intent,
        };
        command.validate()?;
        Ok(command)
    }

    pub const fn header(&self) -> CommandHeader {
        self.header
    }

    pub const fn terminal(&self) -> &TransactionRecord {
        &self.terminal
    }

    pub const fn intent(&self) -> Option<&ParticipantIntent> {
        self.intent.as_ref()
    }

    fn validate(&self) -> Result<(), ShardError> {
        match self.terminal.state() {
            TransactionState::Committed => {
                let intent = self.intent.as_ref().ok_or_else(|| {
                    invalid_command_shape("commit finalization requires participant intent")
                })?;
                if self.terminal.id() != intent.transaction_id()
                    || self.terminal.record_digest() != intent.digest()
                    || self.terminal.transaction_time() <= intent.start_time()
                    || intent.mutations().iter().any(|mutation| {
                        mutation_transaction_time(mutation) != self.terminal.transaction_time()
                    })
                {
                    return Err(invalid_command_shape(
                        "commit finalization does not match participant intent",
                    ));
                }
                intent.encode_current()?;
                Ok(())
            }
            TransactionState::Aborted if self.intent.is_none() => Ok(()),
            TransactionState::Aborted => Err(invalid_command_shape(
                "abort finalization must not carry graph mutations",
            )),
            TransactionState::Prepared => Err(invalid_command_shape(
                "participant finalization requires a terminal transaction record",
            )),
        }
    }

    fn mutations(&self) -> Result<Vec<LogicalMutation>, ShardError> {
        self.validate()?;
        let mut mutations = match &self.intent {
            Some(intent) => intent.mutations.clone(),
            None => Vec::new(),
        };
        mutations.push(LogicalMutation::PutTransaction(self.terminal.clone()));
        Ok(mutations)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdvanceClosedTimestamp {
    header: CommandHeader,
    closed_timestamp: TransactionTime,
}

impl AdvanceClosedTimestamp {
    pub fn new(
        command_id: CommandId,
        placement_epoch: u64,
        backend_generation: u64,
        closed_timestamp: TransactionTime,
    ) -> Result<Self, ShardError> {
        Ok(Self {
            header: CommandHeader::new(command_id, placement_epoch, backend_generation)?,
            closed_timestamp,
        })
    }

    pub const fn header(&self) -> CommandHeader {
        self.header
    }

    pub const fn closed_timestamp(&self) -> TransactionTime {
        self.closed_timestamp
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallSnapshot {
    header: CommandHeader,
    snapshot_id: u128,
    content_digest: Digest32,
}

impl InstallSnapshot {
    pub fn new(
        command_id: CommandId,
        placement_epoch: u64,
        backend_generation: u64,
        snapshot_id: u128,
        content_digest: Digest32,
    ) -> Result<Self, ShardError> {
        if snapshot_id == 0 || content_digest.get() == [0; 32] {
            return Err(ShardError::InvalidCommand(
                "snapshot identity must be complete".into(),
            ));
        }
        Ok(Self {
            header: CommandHeader::new(command_id, placement_epoch, backend_generation)?,
            snapshot_id,
            content_digest,
        })
    }

    pub const fn header(&self) -> CommandHeader {
        self.header
    }

    pub const fn snapshot_id(&self) -> u128 {
        self.snapshot_id
    }

    pub const fn content_digest(&self) -> Digest32 {
        self.content_digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ShardCommand {
    CommitSingleShard(CommitSingleShard),
    CommitSingleShardTransaction(CommitSingleShardTransaction),
    PrewriteIntent(PrewriteIntent),
    RecordHomeDecision(RecordHomeDecision),
    FinalizeParticipant(FinalizeParticipant),
    AdvanceClosedTimestamp(AdvanceClosedTimestamp),
    InstallSnapshot(InstallSnapshot),
    Migration(MigrationCommand),
}

impl ShardCommand {
    pub const fn header(&self) -> CommandHeader {
        match self {
            Self::CommitSingleShard(command) => command.header(),
            Self::CommitSingleShardTransaction(command) => command.header(),
            Self::PrewriteIntent(command) => command.header(),
            Self::RecordHomeDecision(command) => command.header(),
            Self::FinalizeParticipant(command) => command.header(),
            Self::AdvanceClosedTimestamp(command) => command.header(),
            Self::InstallSnapshot(command) => command.header(),
            Self::Migration(command) => command.header(),
        }
    }

    pub(crate) fn mutations(&self) -> Result<Vec<LogicalMutation>, ShardError> {
        match self {
            Self::CommitSingleShard(command) => {
                validate_single_shard_mutations(&command.mutations)?;
                Ok(command.mutations.clone())
            }
            Self::CommitSingleShardTransaction(command) => {
                command.validate()?;
                Ok(command.mutations.clone())
            }
            Self::PrewriteIntent(command) => command.mutations(),
            Self::RecordHomeDecision(command) => {
                validate_home_decision(&command.mutations)?;
                Ok(command.mutations.clone())
            }
            Self::FinalizeParticipant(command) => command.mutations(),
            Self::AdvanceClosedTimestamp(command) => Ok(vec![LogicalMutation::PutReplicaMetadata(
                ReplicaMetadata::new(
                    "dtg.closed_timestamp",
                    Value::Integer(command.closed_timestamp().get()),
                )?,
            )]),
            Self::InstallSnapshot(command) => {
                let mut metadata = BTreeMap::new();
                metadata.insert(
                    "snapshot_id".into(),
                    Value::Bytes(command.snapshot_id().to_be_bytes().to_vec()),
                );
                metadata.insert(
                    "content_digest".into(),
                    Value::Bytes(command.content_digest().get().to_vec()),
                );
                Ok(vec![LogicalMutation::PutReplicaMetadata(
                    ReplicaMetadata::new("dtg.installed_snapshot", Value::Map(metadata))?,
                )])
            }
            Self::Migration(command) => Ok(vec![LogicalMutation::PutReplicaMetadata(
                migration_metadata(command)?,
            )]),
        }
    }

    pub fn encode_current(&self) -> Result<Vec<u8>, ShardError> {
        match self {
            Self::CommitSingleShard(command) => {
                validate_single_shard_mutations(command.mutations())?
            }
            Self::CommitSingleShardTransaction(command) => command.validate()?,
            Self::PrewriteIntent(command) => command.validate()?,
            Self::RecordHomeDecision(command) => validate_home_decision(command.mutations())?,
            Self::FinalizeParticipant(command) => command.validate()?,
            Self::AdvanceClosedTimestamp(_) | Self::InstallSnapshot(_) | Self::Migration(_) => {}
        }
        let mut encoder = Encoder::default();
        encoder.u32(SUPPORTED_SHARD_COMMAND_FORMAT_VERSION);
        match self {
            Self::CommitSingleShard(command) => {
                encoder.u8(1);
                encoder.header(command.header());
                encoder.mutations(command.mutations())?;
            }
            Self::CommitSingleShardTransaction(command) => {
                encoder.u8(8);
                encoder.header(command.header());
                encoder.u128(command.transaction_id().get());
                encoder.i64(command.start_time().get());
                encoder.u64(command.snapshot_applied_index());
                encoder.bytes(&command.request_digest().get())?;
                encoder.mutations(command.mutations())?;
            }
            Self::PrewriteIntent(command) => {
                encoder.u8(2);
                encoder.header(command.header());
                encoder.transaction(command.prepared());
                encoder.bytes(&command.intent().encode_current()?)?;
            }
            Self::RecordHomeDecision(command) => {
                encoder.u8(3);
                encoder.header(command.header());
                encoder.mutations(command.mutations())?;
            }
            Self::FinalizeParticipant(command) => {
                encoder.u8(4);
                encoder.header(command.header());
                encoder.transaction(command.terminal());
                match command.intent() {
                    Some(intent) => {
                        encoder.u8(1);
                        encoder.bytes(&intent.encode_current()?)?;
                    }
                    None => encoder.u8(0),
                }
            }
            Self::AdvanceClosedTimestamp(command) => {
                encoder.u8(5);
                encoder.header(command.header());
                encoder.i64(command.closed_timestamp().get());
            }
            Self::InstallSnapshot(command) => {
                encoder.u8(6);
                encoder.header(command.header());
                encoder.u128(command.snapshot_id());
                encoder.bytes(&command.content_digest().get())?;
            }
            Self::Migration(command) => {
                encoder.u8(7);
                encoder.header(command.header());
                encoder.u8(migration_phase_tag(command.phase()));
                if let Some(migration_id) = command.migration_id() {
                    encoder.u8(1);
                    encoder.u128(migration_id);
                    encoder.u64(command.source_epoch().unwrap().get());
                    encoder.u64(command.source_generation().unwrap().get());
                    encoder.u64(command.retained_generation().unwrap().get());
                    encoder.u64(command.target_generation().unwrap().get());
                    encoder.bytes(&command.target_backend_class_digest().unwrap().get())?;
                    encoder.u64(command.catalog_version().unwrap().get());
                    encoder.u64(command.verified_index().unwrap());
                    encoder.bytes(&command.logical_digest().unwrap().get())?;
                }
            }
        }
        encoder.finish()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ShardError> {
        let mut decoder = Decoder::new(bytes)?;
        let version = decoder.u32()?;
        if version != SUPPORTED_SHARD_COMMAND_FORMAT_VERSION {
            return Err(ShardError::UnsupportedCommandVersion(version));
        }
        let tag = decoder.u8()?;
        let header = decoder.header()?;
        let command = match tag {
            1 => Self::CommitSingleShard(CommitSingleShard {
                header,
                mutations: decoder.mutations()?,
            }),
            8 => Self::CommitSingleShardTransaction(CommitSingleShardTransaction {
                header,
                transaction_id: TransactionId::new(decoder.u128()?)?,
                start_time: TransactionTime::new(decoder.i64()?)?,
                snapshot_applied_index: decoder.u64()?,
                request_digest: Digest32::new(decoder.fixed_32()?),
                mutations: decoder.mutations()?,
            }),
            2 => Self::PrewriteIntent(PrewriteIntent {
                header,
                prepared: decoder.transaction()?,
                intent: ParticipantIntent::decode_current(decoder.bytes()?)?,
            }),
            3 => Self::RecordHomeDecision(RecordHomeDecision {
                header,
                mutations: decoder.mutations()?,
            }),
            4 => Self::FinalizeParticipant(FinalizeParticipant {
                header,
                terminal: decoder.transaction()?,
                intent: match decoder.u8()? {
                    0 => None,
                    1 => Some(ParticipantIntent::decode_current(decoder.bytes()?)?),
                    _ => return Err(invalid("unknown participant intent presence tag")),
                },
            }),
            5 => Self::AdvanceClosedTimestamp(AdvanceClosedTimestamp {
                header,
                closed_timestamp: TransactionTime::new(decoder.i64()?)
                    .map_err(|error| ShardError::InvalidCommand(error.to_string()))?,
            }),
            6 => {
                let snapshot_id = decoder.u128()?;
                let digest = decoder.fixed_32()?;
                Self::InstallSnapshot(InstallSnapshot::new(
                    header.command_id(),
                    header.placement_epoch().get(),
                    header.backend_generation().get(),
                    snapshot_id,
                    Digest32::new(digest),
                )?)
            }
            7 => {
                let phase = decode_migration_phase(decoder.u8()?)?;
                let migration = if decoder.is_finished() {
                    MigrationCommand::new(
                        header.command_id(),
                        header.placement_epoch().get(),
                        header.backend_generation().get(),
                        phase,
                    )?
                } else {
                    match decoder.u8()? {
                        1 => MigrationCommand::decode_online(
                            header,
                            decoder.u128()?,
                            decoder.u64()?,
                            decoder.u64()?,
                            decoder.u64()?,
                            decoder.u64()?,
                            Digest32::new(decoder.fixed_32()?),
                            Version::new(decoder.u64()?),
                            decoder.u64()?,
                            Digest32::new(decoder.fixed_32()?),
                            phase,
                        )?,
                        _ => return Err(invalid("unknown migration context presence tag")),
                    }
                };
                Self::Migration(migration)
            }
            _ => return Err(invalid("unknown command tag")),
        };
        decoder.finish()?;
        if matches!(
            &command,
            Self::CommitSingleShard(CommitSingleShard { mutations, .. })
                | Self::CommitSingleShardTransaction(CommitSingleShardTransaction {
                    mutations,
                    ..
                })
                | Self::RecordHomeDecision(RecordHomeDecision { mutations, .. })
                if mutations.is_empty()
        ) {
            return Err(invalid("mutation command must not be empty"));
        }
        command.mutations()?;
        Ok(command)
    }
}

fn migration_phase_tag(phase: MigrationPhase) -> u8 {
    match phase {
        MigrationPhase::Prepare => 1,
        MigrationPhase::Activate => 2,
        MigrationPhase::Retire => 3,
        MigrationPhase::Grace => 4,
        MigrationPhase::Complete => 5,
        MigrationPhase::Abort => 6,
        MigrationPhase::Rollback => 7,
    }
}

fn decode_migration_phase(tag: u8) -> Result<MigrationPhase, ShardError> {
    match tag {
        1 => Ok(MigrationPhase::Prepare),
        2 => Ok(MigrationPhase::Activate),
        3 => Ok(MigrationPhase::Retire),
        4 => Ok(MigrationPhase::Grace),
        5 => Ok(MigrationPhase::Complete),
        6 => Ok(MigrationPhase::Abort),
        7 => Ok(MigrationPhase::Rollback),
        _ => Err(invalid("unknown migration phase")),
    }
}

fn migration_metadata(command: &MigrationCommand) -> Result<ReplicaMetadata, ShardError> {
    let phase = match command.phase() {
        MigrationPhase::Prepare => "prepare",
        MigrationPhase::Activate => "activate",
        MigrationPhase::Grace => "grace",
        MigrationPhase::Retire => "retire",
        MigrationPhase::Complete => "complete",
        MigrationPhase::Abort => "abort",
        MigrationPhase::Rollback => "rollback",
    };
    let Some(migration_id) = command.migration_id() else {
        return Ok(ReplicaMetadata::new(
            "dtg.migration_phase",
            Value::String(phase.into()),
        )?);
    };
    let mut metadata = BTreeMap::new();
    metadata.insert("phase".into(), Value::String(phase.into()));
    metadata.insert(
        "migration_id".into(),
        Value::Bytes(migration_id.to_be_bytes().to_vec()),
    );
    metadata.insert(
        "source_epoch".into(),
        Value::Bytes(command.source_epoch().unwrap().get().to_be_bytes().to_vec()),
    );
    metadata.insert(
        "source_generation".into(),
        Value::Bytes(
            command
                .source_generation()
                .unwrap()
                .get()
                .to_be_bytes()
                .to_vec(),
        ),
    );
    metadata.insert(
        "retained_generation".into(),
        Value::Bytes(
            command
                .retained_generation()
                .unwrap()
                .get()
                .to_be_bytes()
                .to_vec(),
        ),
    );
    metadata.insert(
        "target_generation".into(),
        Value::Bytes(
            command
                .target_generation()
                .unwrap()
                .get()
                .to_be_bytes()
                .to_vec(),
        ),
    );
    metadata.insert(
        "target_backend_class_digest".into(),
        Value::Bytes(
            command
                .target_backend_class_digest()
                .unwrap()
                .get()
                .to_vec(),
        ),
    );
    metadata.insert(
        "catalog_version".into(),
        Value::Bytes(
            command
                .catalog_version()
                .unwrap()
                .get()
                .to_be_bytes()
                .to_vec(),
        ),
    );
    metadata.insert(
        "verified_index".into(),
        Value::Bytes(command.verified_index().unwrap().to_be_bytes().to_vec()),
    );
    metadata.insert(
        "logical_digest".into(),
        Value::Bytes(command.logical_digest().unwrap().get().to_vec()),
    );
    Ok(ReplicaMetadata::new(
        "dtg.migration_phase",
        Value::Map(metadata),
    )?)
}

fn validate_single_shard_mutations(mutations: &[LogicalMutation]) -> Result<(), ShardError> {
    if mutations.iter().any(|mutation| {
        matches!(mutation, LogicalMutation::PutTransaction(_))
            || matches!(
                mutation,
                LogicalMutation::PutReplicaMetadata(metadata)
                    if metadata.name().starts_with("dtg.")
            )
    }) {
        Err(invalid_command_shape(
            "generic single-Shard commits cannot write transactions or reserved dtg metadata",
        ))
    } else {
        Ok(())
    }
}

fn validate_intent_mutations(mutations: &[LogicalMutation]) -> Result<(), ShardError> {
    if mutations.is_empty() || mutations.len() > MAX_TRANSACTION_INTENT_ITEMS {
        return Err(invalid_command_shape(
            "transaction intent mutation count is outside the supported bounds",
        ));
    }
    if mutations.iter().any(|mutation| {
        matches!(
            mutation,
            LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_)
        )
    }) {
        return Err(invalid_command_shape(
            "transaction intent may contain only graph mutations",
        ));
    }
    let transaction_time = mutation_transaction_time(&mutations[0]);
    if mutations
        .iter()
        .skip(1)
        .any(|mutation| mutation_transaction_time(mutation) != transaction_time)
    {
        return Err(invalid_command_shape(
            "transaction intent mutations must share one transaction time",
        ));
    }
    Ok(())
}

fn validate_commit_follows_start(
    start_time: TransactionTime,
    mutations: &[LogicalMutation],
    message: &'static str,
) -> Result<(), ShardError> {
    if mutation_transaction_time(&mutations[0]) <= start_time {
        Err(invalid_command_shape(message))
    } else {
        Ok(())
    }
}

fn mutation_transaction_time(mutation: &LogicalMutation) -> TransactionTime {
    match mutation {
        LogicalMutation::PutVertex(vertex) => vertex.transaction_time(),
        LogicalMutation::DeleteVertex(vertex) => vertex.transaction_time(),
        LogicalMutation::PutEdge(edge) => edge.transaction_time(),
        LogicalMutation::DeleteEdge(edge) => edge.transaction_time(),
        LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_) => {
            unreachable!("transaction intents reject non-graph mutations before timestamp checks")
        }
    }
}

fn validate_home_decision(mutations: &[LogicalMutation]) -> Result<(), ShardError> {
    if mutations.len() == 1
        && matches!(
            &mutations[0],
            LogicalMutation::PutTransaction(transaction)
                if matches!(
                    transaction.state(),
                    TransactionState::Committed | TransactionState::Aborted
                )
        )
    {
        Ok(())
    } else {
        Err(invalid_command_shape(
            "home decision requires one terminal transaction record",
        ))
    }
}

fn invalid_command_shape(message: &str) -> ShardError {
    ShardError::InvalidCommand(message.into())
}

const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;
const MAX_COLLECTION_ITEMS: usize = 1_000_000;
const MAX_ALLOCATION_ITEMS: usize = 65_536;
const MAX_VALUE_DEPTH: usize = 64;

fn invalid(message: impl Into<String>) -> ShardError {
    ShardError::InvalidCommand(message.into())
}

#[derive(Default)]
struct Encoder {
    bytes: Vec<u8>,
}

struct AllocationBudget {
    remaining: usize,
}

impl AllocationBudget {
    fn new() -> Self {
        Self {
            remaining: MAX_ALLOCATION_ITEMS,
        }
    }

    fn consume(&mut self, count: usize) -> Result<(), ShardError> {
        self.remaining = self
            .remaining
            .checked_sub(count)
            .ok_or_else(|| invalid("command collection exceeds allocation budget"))?;
        Ok(())
    }
}

impl Encoder {
    fn finish(self) -> Result<Vec<u8>, ShardError> {
        if self.bytes.len() > MAX_COMMAND_BYTES {
            Err(invalid("command exceeds maximum encoded size"))
        } else {
            Ok(self.bytes)
        }
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn u128(&mut self, value: u128) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), ShardError> {
        self.u32(
            value
                .len()
                .try_into()
                .map_err(|_| invalid("command field exceeds u32 length"))?,
        );
        self.bytes.extend_from_slice(value);
        if self.bytes.len() > MAX_COMMAND_BYTES {
            return Err(invalid("command exceeds maximum encoded size"));
        }
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), ShardError> {
        self.bytes(value.as_bytes())
    }

    fn header(&mut self, header: CommandHeader) {
        self.u128(header.command_id().get());
        self.u64(header.placement_epoch().get());
        self.u64(header.backend_generation().get());
    }

    fn transaction(&mut self, transaction: &TransactionRecord) {
        self.u128(transaction.id().get());
        self.u8(match transaction.state() {
            TransactionState::Prepared => 1,
            TransactionState::Committed => 2,
            TransactionState::Aborted => 3,
        });
        self.i64(transaction.transaction_time().get());
        self.u32(32);
        self.bytes
            .extend_from_slice(&transaction.record_digest().get());
    }

    fn mutations(&mut self, mutations: &[LogicalMutation]) -> Result<(), ShardError> {
        if mutations.len() > MAX_COLLECTION_ITEMS {
            return Err(invalid("too many logical mutations"));
        }
        self.u32(
            mutations
                .len()
                .try_into()
                .map_err(|_| invalid("too many logical mutations"))?,
        );
        let mut budget = AllocationBudget::new();
        budget.consume(mutations.len())?;
        for mutation in mutations {
            self.mutation(mutation, &mut budget)?;
        }
        Ok(())
    }

    fn mutation(
        &mut self,
        mutation: &LogicalMutation,
        budget: &mut AllocationBudget,
    ) -> Result<(), ShardError> {
        match mutation {
            LogicalMutation::PutVertex(vertex) => {
                self.u8(1);
                self.u128(vertex.id().get());
                self.u64(vertex.version().get());
                self.interval(vertex.valid_time());
                self.i64(vertex.transaction_time().get());
                self.properties(vertex.properties(), budget)?;
            }
            LogicalMutation::DeleteVertex(vertex) => {
                self.u8(2);
                self.u128(vertex.id().get());
                self.u64(vertex.version().get());
                self.i64(vertex.transaction_time().get());
            }
            LogicalMutation::PutEdge(edge) => {
                self.u8(3);
                self.u128(edge.id().get());
                self.u128(edge.source().get());
                self.u128(edge.target().get());
                self.string(edge.edge_type())?;
                self.u64(edge.version().get());
                self.interval(edge.valid_time());
                self.i64(edge.transaction_time().get());
                self.properties(edge.properties(), budget)?;
            }
            LogicalMutation::DeleteEdge(edge) => {
                self.u8(4);
                self.u128(edge.id().get());
                self.u64(edge.version().get());
                self.i64(edge.transaction_time().get());
            }
            LogicalMutation::PutTransaction(transaction) => {
                self.u8(5);
                self.u128(transaction.id().get());
                self.u8(match transaction.state() {
                    TransactionState::Prepared => 1,
                    TransactionState::Committed => 2,
                    TransactionState::Aborted => 3,
                });
                self.i64(transaction.transaction_time().get());
                self.bytes(&transaction.record_digest().get())?;
            }
            LogicalMutation::PutReplicaMetadata(metadata) => {
                self.u8(6);
                self.string(metadata.name())?;
                self.value(metadata.value(), 0, budget)?;
            }
        }
        Ok(())
    }

    fn interval(&mut self, interval: ValidInterval) {
        self.i64(interval.start());
        self.i64(interval.end());
    }

    fn properties(
        &mut self,
        properties: &Properties,
        budget: &mut AllocationBudget,
    ) -> Result<(), ShardError> {
        if properties.len() > MAX_COLLECTION_ITEMS {
            return Err(invalid("too many properties"));
        }
        self.u32(
            properties
                .len()
                .try_into()
                .map_err(|_| invalid("too many properties"))?,
        );
        budget.consume(properties.len())?;
        for (name, value) in properties {
            self.string(name)?;
            self.value(value, 0, budget)?;
        }
        Ok(())
    }

    fn value(
        &mut self,
        value: &Value,
        depth: usize,
        budget: &mut AllocationBudget,
    ) -> Result<(), ShardError> {
        if depth > MAX_VALUE_DEPTH {
            return Err(invalid("command value exceeds nesting depth budget"));
        }
        match value {
            Value::Null => self.u8(0),
            Value::Boolean(value) => {
                self.u8(1);
                self.u8(u8::from(*value));
            }
            Value::Integer(value) => {
                self.u8(2);
                self.i64(*value);
            }
            Value::FloatBits(value) => {
                self.u8(3);
                self.u64(*value);
            }
            Value::Bytes(value) => {
                self.u8(4);
                self.bytes(value)?;
            }
            Value::String(value) => {
                self.u8(5);
                self.string(value)?;
            }
            Value::List(values) => {
                if values.len() > MAX_COLLECTION_ITEMS {
                    return Err(invalid("value list is too large"));
                }
                self.u8(6);
                self.u32(
                    values
                        .len()
                        .try_into()
                        .map_err(|_| invalid("value list is too large"))?,
                );
                budget.consume(values.len())?;
                for value in values {
                    self.value(value, depth + 1, budget)?;
                }
            }
            Value::Map(values) => {
                if values.len() > MAX_COLLECTION_ITEMS {
                    return Err(invalid("value map is too large"));
                }
                self.u8(7);
                self.u32(
                    values
                        .len()
                        .try_into()
                        .map_err(|_| invalid("value map is too large"))?,
                );
                budget.consume(values.len())?;
                for (key, value) in values {
                    self.string(key)?;
                    self.value(value, depth + 1, budget)?;
                }
            }
        }
        Ok(())
    }
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
    allocation_budget: AllocationBudget,
}

impl<'a> Decoder<'a> {
    fn new(bytes: &'a [u8]) -> Result<Self, ShardError> {
        if bytes.is_empty() || bytes.len() > MAX_COMMAND_BYTES {
            return Err(invalid("command bytes are empty or oversized"));
        }
        Ok(Self {
            bytes,
            offset: 0,
            allocation_budget: AllocationBudget::new(),
        })
    }

    fn finish(self) -> Result<(), ShardError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid("command contains trailing bytes"))
        }
    }

    fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], ShardError> {
        let end = self
            .offset
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(|| invalid("truncated command"))?;
        let value = &self.bytes[self.offset..end];
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, ShardError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, ShardError> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64, ShardError> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn u128(&mut self) -> Result<u128, ShardError> {
        Ok(u128::from_be_bytes(self.take(16)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, ShardError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn bytes(&mut self) -> Result<&'a [u8], ShardError> {
        let length = self.u32()? as usize;
        self.take(length)
    }

    fn string(&mut self) -> Result<String, ShardError> {
        String::from_utf8(self.bytes()?.to_vec())
            .map_err(|_| invalid("command string is not UTF-8"))
    }

    fn fixed_32(&mut self) -> Result<[u8; 32], ShardError> {
        let value = self.bytes()?;
        value
            .try_into()
            .map_err(|_| invalid("digest must contain exactly 32 bytes"))
    }

    fn header(&mut self) -> Result<CommandHeader, ShardError> {
        CommandHeader::new(
            CommandId::new(self.u128()?).map_err(ShardError::Storage)?,
            self.u64()?,
            self.u64()?,
        )
    }

    fn transaction(&mut self) -> Result<TransactionRecord, ShardError> {
        let id = TransactionId::new(self.u128()?)?;
        let state = match self.u8()? {
            1 => TransactionState::Prepared,
            2 => TransactionState::Committed,
            3 => TransactionState::Aborted,
            _ => return Err(invalid("unknown transaction state")),
        };
        let transaction_time = TransactionTime::new(self.i64()?)?;
        let digest = Digest32::new(self.fixed_32()?);
        Ok(TransactionRecord::new(id, state, transaction_time, digest)?)
    }

    fn count(&mut self) -> Result<usize, ShardError> {
        let count = self.u32()? as usize;
        if count > MAX_COLLECTION_ITEMS {
            Err(invalid("command collection exceeds item limit"))
        } else {
            Ok(count)
        }
    }

    fn collection_count(&mut self) -> Result<usize, ShardError> {
        let count = self.count()?;
        self.allocation_budget.consume(count)?;
        Ok(count)
    }

    fn mutations(&mut self) -> Result<Vec<LogicalMutation>, ShardError> {
        self.mutations_with_limit(
            MAX_COLLECTION_ITEMS,
            "command collection exceeds item limit",
        )
    }

    fn mutations_with_limit(
        &mut self,
        limit: usize,
        limit_message: &'static str,
    ) -> Result<Vec<LogicalMutation>, ShardError> {
        let count = self.count()?;
        if count > limit {
            return Err(invalid(limit_message));
        }
        self.allocation_budget.consume(count)?;
        let mut mutations = Vec::with_capacity(count);
        for _ in 0..count {
            mutations.push(self.mutation()?);
        }
        Ok(mutations)
    }

    fn mutation(&mut self) -> Result<LogicalMutation, ShardError> {
        match self.u8()? {
            1 => Ok(LogicalMutation::PutVertex(VertexVersion::new(
                VertexId::new(self.u128()?)?,
                Version::new(self.u64()?),
                self.interval()?,
                TransactionTime::new(self.i64()?)?,
                self.properties()?,
            )?)),
            2 => Ok(LogicalMutation::DeleteVertex(VertexTombstone::new(
                VertexId::new(self.u128()?)?,
                Version::new(self.u64()?),
                TransactionTime::new(self.i64()?)?,
            ))),
            3 => Ok(LogicalMutation::PutEdge(EdgeVersion::new(
                EdgeId::new(self.u128()?)?,
                VertexId::new(self.u128()?)?,
                VertexId::new(self.u128()?)?,
                self.string()?,
                Version::new(self.u64()?),
                self.interval()?,
                TransactionTime::new(self.i64()?)?,
                self.properties()?,
            )?)),
            4 => Ok(LogicalMutation::DeleteEdge(EdgeTombstone::new(
                EdgeId::new(self.u128()?)?,
                Version::new(self.u64()?),
                TransactionTime::new(self.i64()?)?,
            ))),
            5 => {
                let id = TransactionId::new(self.u128()?)?;
                let state = match self.u8()? {
                    1 => TransactionState::Prepared,
                    2 => TransactionState::Committed,
                    3 => TransactionState::Aborted,
                    _ => return Err(invalid("unknown transaction state")),
                };
                let transaction_time = TransactionTime::new(self.i64()?)?;
                let digest = Digest32::new(self.fixed_32()?);
                Ok(LogicalMutation::PutTransaction(TransactionRecord::new(
                    id,
                    state,
                    transaction_time,
                    digest,
                )?))
            }
            6 => Ok(LogicalMutation::PutReplicaMetadata(ReplicaMetadata::new(
                self.string()?,
                self.value(0)?,
            )?)),
            _ => Err(invalid("unknown logical mutation tag")),
        }
    }

    fn interval(&mut self) -> Result<ValidInterval, ShardError> {
        Ok(ValidInterval::new(self.i64()?, self.i64()?)?)
    }

    fn properties(&mut self) -> Result<Properties, ShardError> {
        let count = self.collection_count()?;
        let mut properties = BTreeMap::new();
        for _ in 0..count {
            let name = self.string()?;
            if properties.insert(name, self.value(0)?).is_some() {
                return Err(invalid("duplicate property name"));
            }
        }
        Ok(properties)
    }

    fn value(&mut self, depth: usize) -> Result<Value, ShardError> {
        if depth > MAX_VALUE_DEPTH {
            return Err(invalid("command value exceeds nesting depth budget"));
        }
        match self.u8()? {
            0 => Ok(Value::Null),
            1 => match self.u8()? {
                0 => Ok(Value::Boolean(false)),
                1 => Ok(Value::Boolean(true)),
                _ => Err(invalid("invalid Boolean value")),
            },
            2 => Ok(Value::Integer(self.i64()?)),
            3 => Ok(Value::FloatBits(self.u64()?)),
            4 => Ok(Value::Bytes(self.bytes()?.to_vec())),
            5 => Ok(Value::String(self.string()?)),
            6 => {
                let count = self.collection_count()?;
                let mut values = Vec::new();
                for _ in 0..count {
                    values.push(self.value(depth + 1)?);
                }
                Ok(Value::List(values))
            }
            7 => {
                let count = self.collection_count()?;
                let mut values = BTreeMap::new();
                for _ in 0..count {
                    let key = self.string()?;
                    if values.insert(key, self.value(depth + 1)?).is_some() {
                        return Err(invalid("duplicate value-map key"));
                    }
                }
                Ok(Value::Map(values))
            }
            _ => Err(invalid("unknown value tag")),
        }
    }
}
