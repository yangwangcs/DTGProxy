#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};

use storage_api::{Keyspace, LogicalKey, Mutation, MutationOperation, PreparedMutationBatch};
use temporal_types::TransactionTime;

const MAGIC: [u8; 4] = *b"DTRC";
const VERSION_V1: u16 = 1;
const APPLY_PREPARED_TAG: u8 = 1;
const CLOSED_TIMESTAMP_TICK_TAG: u8 = 2;
const PUT_TAG: u8 = 1;
const DELETE_TAG: u8 = 2;
const HEADER_BYTES: usize = 40;
const CHECKSUM_BYTES: usize = 4;
const MIN_COMMAND_BYTES: usize = HEADER_BYTES + CHECKSUM_BYTES;

pub const MAX_COMMAND_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_MUTATIONS: usize = 65_536;
pub const MAX_KEY_BYTES: usize = 64 * 1024;
pub const MAX_VALUE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandEnvelopeV1 {
    pub shard_id: u32,
    pub placement_epoch: u64,
    pub request_id: u128,
    pub body: CommandBodyV1,
}

impl CommandEnvelopeV1 {
    #[must_use]
    pub const fn new(
        shard_id: u32,
        placement_epoch: u64,
        request_id: u128,
        body: CommandBodyV1,
    ) -> Self {
        Self {
            shard_id,
            placement_epoch,
            request_id,
            body,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, CommandCodecError> {
        let (body_tag, body) = self.encode_body()?;
        let command_length = HEADER_BYTES
            .checked_add(body.len())
            .and_then(|length| length.checked_add(CHECKSUM_BYTES))
            .ok_or(CommandCodecError::LengthOverflow)?;
        if command_length > MAX_COMMAND_BYTES {
            return Err(CommandCodecError::CommandTooLarge {
                max: MAX_COMMAND_BYTES,
                actual: command_length,
            });
        }
        let body_length =
            u32::try_from(body.len()).map_err(|_| CommandCodecError::LengthOverflow)?;
        let mut bytes = Vec::with_capacity(command_length);
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&VERSION_V1.to_be_bytes());
        bytes.push(body_tag);
        bytes.push(0);
        bytes.extend_from_slice(&self.shard_id.to_be_bytes());
        bytes.extend_from_slice(&self.placement_epoch.to_be_bytes());
        bytes.extend_from_slice(&self.request_id.to_be_bytes());
        bytes.extend_from_slice(&body_length.to_be_bytes());
        bytes.extend_from_slice(&body);
        bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, CommandCodecError> {
        if bytes.len() > MAX_COMMAND_BYTES {
            return Err(CommandCodecError::CommandTooLarge {
                max: MAX_COMMAND_BYTES,
                actual: bytes.len(),
            });
        }
        if bytes.len() < MIN_COMMAND_BYTES {
            return Err(CommandCodecError::Truncated {
                minimum: MIN_COMMAND_BYTES,
                actual: bytes.len(),
            });
        }
        if bytes[..MAGIC.len()] != MAGIC {
            return Err(CommandCodecError::InvalidMagic);
        }
        let version = u16::from_be_bytes([bytes[4], bytes[5]]);
        if version != VERSION_V1 {
            return Err(CommandCodecError::UnsupportedVersion { version });
        }
        let body_tag = bytes[6];
        let flags = bytes[7];
        if flags != 0 {
            return Err(CommandCodecError::UnknownFlags { flags });
        }
        let shard_id = u32::from_be_bytes(bytes[8..12].try_into().expect("fixed header slice"));
        let placement_epoch =
            u64::from_be_bytes(bytes[12..20].try_into().expect("fixed header slice"));
        let request_id = u128::from_be_bytes(bytes[20..36].try_into().expect("fixed header slice"));
        let body_length = u32::from_be_bytes(bytes[36..40].try_into().expect("fixed header slice"));
        let body_length =
            usize::try_from(body_length).map_err(|_| CommandCodecError::LengthOverflow)?;
        let expected_length = HEADER_BYTES
            .checked_add(body_length)
            .and_then(|length| length.checked_add(CHECKSUM_BYTES))
            .ok_or(CommandCodecError::LengthOverflow)?;
        if bytes.len() != expected_length {
            return Err(CommandCodecError::LengthMismatch {
                expected: expected_length,
                actual: bytes.len(),
            });
        }
        let checksum_offset = expected_length - CHECKSUM_BYTES;
        let expected_checksum = u32::from_be_bytes(
            bytes[checksum_offset..]
                .try_into()
                .expect("fixed checksum slice"),
        );
        if crc32fast::hash(&bytes[..checksum_offset]) != expected_checksum {
            return Err(CommandCodecError::ChecksumMismatch);
        }
        let mut body_reader = Reader::new(&bytes[HEADER_BYTES..checksum_offset]);
        let body = match body_tag {
            APPLY_PREPARED_TAG => {
                let commit_ts = decode_transaction_time(&mut body_reader)?;
                let batch_shard_id = body_reader.u32()?;
                if batch_shard_id != shard_id {
                    return Err(CommandCodecError::ShardMismatch {
                        envelope: shard_id,
                        batch: batch_shard_id,
                    });
                }
                let txn_id = body_reader.u128()?;
                let mutation_count = usize::try_from(body_reader.u32()?)
                    .map_err(|_| CommandCodecError::LengthOverflow)?;
                if mutation_count > MAX_MUTATIONS {
                    return Err(CommandCodecError::TooManyMutations {
                        max: MAX_MUTATIONS,
                        actual: mutation_count,
                    });
                }
                let mut mutations = Vec::with_capacity(mutation_count);
                for expected in 0..mutation_count {
                    let expected =
                        u32::try_from(expected).map_err(|_| CommandCodecError::LengthOverflow)?;
                    mutations.push(decode_mutation(&mut body_reader, expected)?);
                }
                CommandBodyV1::ApplyPrepared(ApplyPreparedV1 {
                    commit_ts,
                    batch: PreparedMutationBatch {
                        shard_id: batch_shard_id,
                        txn_id,
                        mutations,
                    },
                })
            }
            CLOSED_TIMESTAMP_TICK_TAG => {
                CommandBodyV1::ClosedTimestampTick(decode_transaction_time(&mut body_reader)?)
            }
            tag => return Err(CommandCodecError::UnknownBodyTag { tag }),
        };
        body_reader.finish()?;
        Ok(Self::new(shard_id, placement_epoch, request_id, body))
    }

    fn encode_body(&self) -> Result<(u8, Vec<u8>), CommandCodecError> {
        match &self.body {
            CommandBodyV1::ApplyPrepared(apply) => {
                if apply.batch.shard_id != self.shard_id {
                    return Err(CommandCodecError::ShardMismatch {
                        envelope: self.shard_id,
                        batch: apply.batch.shard_id,
                    });
                }
                if apply.batch.mutations.len() > MAX_MUTATIONS {
                    return Err(CommandCodecError::TooManyMutations {
                        max: MAX_MUTATIONS,
                        actual: apply.batch.mutations.len(),
                    });
                }
                let mut body = Vec::new();
                encode_transaction_time(&mut body, apply.commit_ts);
                body.extend_from_slice(&apply.batch.shard_id.to_be_bytes());
                body.extend_from_slice(&apply.batch.txn_id.to_be_bytes());
                write_length(&mut body, apply.batch.mutations.len())?;
                for (expected, mutation) in apply.batch.mutations.iter().enumerate() {
                    let expected =
                        u32::try_from(expected).map_err(|_| CommandCodecError::LengthOverflow)?;
                    if mutation.sequence != expected {
                        return Err(CommandCodecError::NonCanonicalMutationSequence {
                            expected,
                            actual: mutation.sequence,
                        });
                    }
                    let mutation_length = mutation_encoded_length(mutation)?;
                    let encoded_length = HEADER_BYTES
                        .checked_add(body.len())
                        .and_then(|length| length.checked_add(mutation_length))
                        .and_then(|length| length.checked_add(CHECKSUM_BYTES))
                        .ok_or(CommandCodecError::LengthOverflow)?;
                    if encoded_length > MAX_COMMAND_BYTES {
                        return Err(CommandCodecError::CommandTooLarge {
                            max: MAX_COMMAND_BYTES,
                            actual: encoded_length,
                        });
                    }
                    encode_mutation(&mut body, mutation)?;
                }
                Ok((APPLY_PREPARED_TAG, body))
            }
            CommandBodyV1::ClosedTimestampTick(closed_ts) => {
                let mut body = Vec::with_capacity(12);
                encode_transaction_time(&mut body, *closed_ts);
                Ok((CLOSED_TIMESTAMP_TICK_TAG, body))
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandBodyV1 {
    ApplyPrepared(ApplyPreparedV1),
    ClosedTimestampTick(TransactionTime),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyPreparedV1 {
    pub commit_ts: TransactionTime,
    pub batch: PreparedMutationBatch,
}

fn encode_transaction_time(bytes: &mut Vec<u8>, timestamp: TransactionTime) {
    bytes.extend_from_slice(&timestamp.physical_micros().to_be_bytes());
    bytes.extend_from_slice(&timestamp.logical().to_be_bytes());
}

fn decode_transaction_time(reader: &mut Reader<'_>) -> Result<TransactionTime, CommandCodecError> {
    Ok(TransactionTime::new(reader.i64()?, reader.u32()?))
}

fn encode_mutation(bytes: &mut Vec<u8>, mutation: &Mutation) -> Result<(), CommandCodecError> {
    bytes.extend_from_slice(&mutation.sequence.to_be_bytes());
    match &mutation.operation {
        MutationOperation::Put { key, value } => {
            validate_key_value_lengths(key, Some(value.as_slice()))?;
            bytes.push(PUT_TAG);
            bytes.push(key.keyspace().tag());
            write_length_delimited(bytes, key.as_bytes())?;
            write_length_delimited(bytes, value)?;
        }
        MutationOperation::Delete { key } => {
            validate_key_value_lengths(key, None)?;
            bytes.push(DELETE_TAG);
            bytes.push(key.keyspace().tag());
            write_length_delimited(bytes, key.as_bytes())?;
        }
    }
    Ok(())
}

fn decode_mutation(reader: &mut Reader<'_>, expected: u32) -> Result<Mutation, CommandCodecError> {
    let sequence = reader.u32()?;
    if sequence != expected {
        return Err(CommandCodecError::NonCanonicalMutationSequence {
            expected,
            actual: sequence,
        });
    }
    let operation_tag = reader.u8()?;
    let keyspace = decode_keyspace(reader.u8()?)?;
    let key = LogicalKey::in_keyspace(keyspace, reader.length_delimited(MAX_KEY_BYTES)?.to_vec());
    match operation_tag {
        PUT_TAG => Ok(Mutation::put(
            sequence,
            key,
            reader.length_delimited(MAX_VALUE_BYTES)?.to_vec(),
        )),
        DELETE_TAG => Ok(Mutation::delete(sequence, key)),
        tag => Err(CommandCodecError::UnknownMutationTag { tag }),
    }
}

fn validate_key_value_lengths(
    key: &LogicalKey,
    value: Option<&[u8]>,
) -> Result<(), CommandCodecError> {
    if key.as_bytes().len() > MAX_KEY_BYTES {
        return Err(CommandCodecError::KeyTooLarge {
            max: MAX_KEY_BYTES,
            actual: key.as_bytes().len(),
        });
    }
    if let Some(value) = value
        && value.len() > MAX_VALUE_BYTES
    {
        return Err(CommandCodecError::ValueTooLarge {
            max: MAX_VALUE_BYTES,
            actual: value.len(),
        });
    }
    Ok(())
}

fn mutation_encoded_length(mutation: &Mutation) -> Result<usize, CommandCodecError> {
    let (fixed_bytes, value_bytes) = match &mutation.operation {
        MutationOperation::Put { key, value } => {
            validate_key_value_lengths(key, Some(value.as_slice()))?;
            (14_usize, value.len())
        }
        MutationOperation::Delete { key } => {
            validate_key_value_lengths(key, None)?;
            (10_usize, 0)
        }
    };
    fixed_bytes
        .checked_add(mutation_key(mutation).as_bytes().len())
        .and_then(|length| length.checked_add(value_bytes))
        .ok_or(CommandCodecError::LengthOverflow)
}

fn mutation_key(mutation: &Mutation) -> &LogicalKey {
    match &mutation.operation {
        MutationOperation::Put { key, .. } | MutationOperation::Delete { key } => key,
    }
}

fn write_length(bytes: &mut Vec<u8>, length: usize) -> Result<(), CommandCodecError> {
    let length = u32::try_from(length).map_err(|_| CommandCodecError::LengthOverflow)?;
    bytes.extend_from_slice(&length.to_be_bytes());
    Ok(())
}

fn write_length_delimited(bytes: &mut Vec<u8>, value: &[u8]) -> Result<(), CommandCodecError> {
    write_length(bytes, value.len())?;
    bytes.extend_from_slice(value);
    Ok(())
}

fn decode_keyspace(tag: u8) -> Result<Keyspace, CommandCodecError> {
    match tag {
        0 => Ok(Keyspace::Meta),
        1 => Ok(Keyspace::Identity),
        2 => Ok(Keyspace::Current),
        3 => Ok(Keyspace::AdjOut),
        4 => Ok(Keyspace::AdjIn),
        5 => Ok(Keyspace::History),
        6 => Ok(Keyspace::TemporalIndex),
        7 => Ok(Keyspace::Txn),
        tag => Err(CommandCodecError::UnknownKeyspace { tag }),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], CommandCodecError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(CommandCodecError::LengthOverflow)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(CommandCodecError::Truncated {
                minimum: end,
                actual: self.bytes.len(),
            })?;
        self.offset = end;
        Ok(value)
    }

    fn u8(&mut self) -> Result<u8, CommandCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CommandCodecError> {
        Ok(u32::from_be_bytes(
            self.take(4)?.try_into().expect("fixed reader slice"),
        ))
    }

    fn i64(&mut self) -> Result<i64, CommandCodecError> {
        Ok(i64::from_be_bytes(
            self.take(8)?.try_into().expect("fixed reader slice"),
        ))
    }

    fn u128(&mut self) -> Result<u128, CommandCodecError> {
        Ok(u128::from_be_bytes(
            self.take(16)?.try_into().expect("fixed reader slice"),
        ))
    }

    fn length_delimited(&mut self, max: usize) -> Result<&'a [u8], CommandCodecError> {
        let length = usize::try_from(self.u32()?).map_err(|_| CommandCodecError::LengthOverflow)?;
        if length > max {
            return if max == MAX_KEY_BYTES {
                Err(CommandCodecError::KeyTooLarge {
                    max,
                    actual: length,
                })
            } else {
                Err(CommandCodecError::ValueTooLarge {
                    max,
                    actual: length,
                })
            };
        }
        self.take(length)
    }

    fn finish(self) -> Result<(), CommandCodecError> {
        let remaining = self.bytes.len() - self.offset;
        if remaining == 0 {
            Ok(())
        } else {
            Err(CommandCodecError::TrailingBodyBytes { remaining })
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandCodecError {
    InvalidMagic,
    UnsupportedVersion { version: u16 },
    UnknownFlags { flags: u8 },
    UnknownBodyTag { tag: u8 },
    UnknownMutationTag { tag: u8 },
    UnknownKeyspace { tag: u8 },
    Truncated { minimum: usize, actual: usize },
    LengthMismatch { expected: usize, actual: usize },
    LengthOverflow,
    ChecksumMismatch,
    CommandTooLarge { max: usize, actual: usize },
    TooManyMutations { max: usize, actual: usize },
    KeyTooLarge { max: usize, actual: usize },
    ValueTooLarge { max: usize, actual: usize },
    NonCanonicalMutationSequence { expected: u32, actual: u32 },
    ShardMismatch { envelope: u32, batch: u32 },
    TrailingBodyBytes { remaining: usize },
}

impl Display for CommandCodecError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidMagic => formatter.write_str("invalid Raft command magic"),
            Self::UnsupportedVersion { version } => {
                write!(formatter, "unsupported Raft command version {version}")
            }
            Self::UnknownFlags { flags } => write!(formatter, "unknown command flags {flags:#04x}"),
            Self::UnknownBodyTag { tag } => write!(formatter, "unknown command body tag {tag}"),
            Self::UnknownMutationTag { tag } => write!(formatter, "unknown mutation tag {tag}"),
            Self::UnknownKeyspace { tag } => write!(formatter, "unknown keyspace tag {tag}"),
            Self::Truncated { minimum, actual } => write!(
                formatter,
                "truncated command: need at least {minimum} bytes, got {actual}"
            ),
            Self::LengthMismatch { expected, actual } => write!(
                formatter,
                "command length mismatch: expected {expected} bytes, got {actual}"
            ),
            Self::LengthOverflow => formatter.write_str("command length overflows its wire field"),
            Self::ChecksumMismatch => formatter.write_str("Raft command checksum mismatch"),
            Self::CommandTooLarge { max, actual } => {
                write!(formatter, "command is {actual} bytes; maximum is {max}")
            }
            Self::TooManyMutations { max, actual } => {
                write!(
                    formatter,
                    "command has {actual} mutations; maximum is {max}"
                )
            }
            Self::KeyTooLarge { max, actual } => {
                write!(
                    formatter,
                    "mutation key is {actual} bytes; maximum is {max}"
                )
            }
            Self::ValueTooLarge { max, actual } => {
                write!(
                    formatter,
                    "mutation value is {actual} bytes; maximum is {max}"
                )
            }
            Self::NonCanonicalMutationSequence { expected, actual } => write!(
                formatter,
                "non-canonical mutation sequence: expected {expected}, got {actual}"
            ),
            Self::ShardMismatch { envelope, batch } => write!(
                formatter,
                "command shard {envelope} does not match prepared batch shard {batch}"
            ),
            Self::TrailingBodyBytes { remaining } => {
                write!(formatter, "command body has {remaining} trailing bytes")
            }
        }
    }
}

impl Error for CommandCodecError {}
