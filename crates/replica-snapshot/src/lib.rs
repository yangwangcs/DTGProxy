#![forbid(unsafe_code)]

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::File;
use std::io::{self, Read};
use std::path::{Path, PathBuf};

use adapter_rocksdb::RocksAdapter;
use shard_runtime::ShardStateMachine;
use storage_api::{AdapterError, StorageAdapter};
use temporal_types::TransactionTime;

const MAGIC: [u8; 4] = *b"DTSM";
const VERSION: u16 = 1;
const FIXED_PREFIX_BYTES: usize = 72;
const DIGEST_BYTES: usize = 32;
const CHECKSUM_BYTES: usize = 4;
const MIN_MANIFEST_BYTES: usize = FIXED_PREFIX_BYTES + DIGEST_BYTES + CHECKSUM_BYTES;
pub const MAX_SNAPSHOT_VOTERS: usize = 64;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotManifestV1 {
    pub shard_id: u32,
    pub placement_epoch: u64,
    pub term: u64,
    pub applied_index: u64,
    pub closed_ts: TransactionTime,
    pub resolved_ts: TransactionTime,
    pub adapter_applied_ts: TransactionTime,
    pub voters: Vec<u64>,
    pub checkpoint_digest: [u8; 32],
}

impl SnapshotManifestV1 {
    pub fn encode(&self) -> Result<Vec<u8>, SnapshotError> {
        validate_voters(&self.voters)?;
        let mut bytes = Vec::with_capacity(MIN_MANIFEST_BYTES + self.voters.len() * 8);
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&VERSION.to_be_bytes());
        bytes.extend_from_slice(&self.shard_id.to_be_bytes());
        bytes.extend_from_slice(&self.placement_epoch.to_be_bytes());
        bytes.extend_from_slice(&self.term.to_be_bytes());
        bytes.extend_from_slice(&self.applied_index.to_be_bytes());
        encode_timestamp(&mut bytes, self.closed_ts);
        encode_timestamp(&mut bytes, self.resolved_ts);
        encode_timestamp(&mut bytes, self.adapter_applied_ts);
        let voter_count =
            u16::try_from(self.voters.len()).map_err(|_| SnapshotError::TooManyVoters {
                max: MAX_SNAPSHOT_VOTERS,
                actual: self.voters.len(),
            })?;
        bytes.extend_from_slice(&voter_count.to_be_bytes());
        for voter in &self.voters {
            bytes.extend_from_slice(&voter.to_be_bytes());
        }
        bytes.extend_from_slice(&self.checkpoint_digest);
        let checksum = crc32fast::hash(&bytes);
        bytes.extend_from_slice(&checksum.to_be_bytes());
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        if bytes.len() < MIN_MANIFEST_BYTES {
            return Err(SnapshotError::ManifestTruncated);
        }
        if bytes[..4] != MAGIC {
            return Err(SnapshotError::InvalidManifestMagic);
        }
        let version = u16::from_be_bytes(bytes[4..6].try_into().expect("fixed version slice"));
        if version != VERSION {
            return Err(SnapshotError::UnsupportedManifestVersion { version });
        }
        let voter_count = usize::from(u16::from_be_bytes(
            bytes[70..72].try_into().expect("fixed voter count slice"),
        ));
        if voter_count > MAX_SNAPSHOT_VOTERS {
            return Err(SnapshotError::TooManyVoters {
                max: MAX_SNAPSHOT_VOTERS,
                actual: voter_count,
            });
        }
        let expected_length = MIN_MANIFEST_BYTES
            .checked_add(voter_count * 8)
            .ok_or(SnapshotError::ManifestLengthMismatch)?;
        if bytes.len() != expected_length {
            return Err(SnapshotError::ManifestLengthMismatch);
        }
        let checksum_offset = bytes.len() - CHECKSUM_BYTES;
        let stored_checksum = u32::from_be_bytes(
            bytes[checksum_offset..]
                .try_into()
                .expect("fixed checksum slice"),
        );
        if crc32fast::hash(&bytes[..checksum_offset]) != stored_checksum {
            return Err(SnapshotError::ManifestChecksumMismatch);
        }
        let mut voters = Vec::with_capacity(voter_count);
        let mut offset = FIXED_PREFIX_BYTES;
        for _ in 0..voter_count {
            voters.push(u64::from_be_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .expect("bounded voter slice"),
            ));
            offset += 8;
        }
        validate_voters(&voters)?;
        let checkpoint_digest = bytes[offset..offset + DIGEST_BYTES]
            .try_into()
            .expect("bounded checkpoint digest slice");
        Ok(Self {
            shard_id: u32::from_be_bytes(bytes[6..10].try_into().expect("fixed shard slice")),
            placement_epoch: u64::from_be_bytes(
                bytes[10..18].try_into().expect("fixed epoch slice"),
            ),
            term: u64::from_be_bytes(bytes[18..26].try_into().expect("fixed term slice")),
            applied_index: u64::from_be_bytes(bytes[26..34].try_into().expect("fixed index slice")),
            closed_ts: decode_timestamp(&bytes[34..46]),
            resolved_ts: decode_timestamp(&bytes[46..58]),
            adapter_applied_ts: decode_timestamp(&bytes[58..70]),
            voters,
            checkpoint_digest,
        })
    }
}

pub fn create_rocks_checkpoint(
    machine: &ShardStateMachine<RocksAdapter>,
    voters: &[u64],
    destination: impl AsRef<Path>,
) -> Result<SnapshotManifestV1, SnapshotError> {
    validate_voters(voters)?;
    let metadata = machine.metadata();
    let adapter_index = machine.adapter().applied_log_index()?;
    if adapter_index != metadata.applied_index {
        return Err(SnapshotError::AppliedIndexMismatch {
            manifest: metadata.applied_index,
            adapter: adapter_index,
        });
    }
    machine.adapter().checkpoint(destination.as_ref())?;
    let checkpoint_digest = hash_checkpoint(destination.as_ref())?;
    Ok(SnapshotManifestV1 {
        shard_id: metadata.shard_id,
        placement_epoch: metadata.placement_epoch,
        term: metadata.last_term,
        applied_index: metadata.applied_index,
        closed_ts: metadata.closed_ts,
        resolved_ts: metadata.resolved_ts,
        adapter_applied_ts: metadata.adapter_applied_ts,
        voters: voters.to_vec(),
        checkpoint_digest,
    })
}

pub fn open_verified_checkpoint(
    checkpoint: impl AsRef<Path>,
    manifest: &SnapshotManifestV1,
) -> Result<RocksAdapter, SnapshotError> {
    validate_voters(&manifest.voters)?;
    let digest = hash_checkpoint(checkpoint.as_ref())?;
    if digest != manifest.checkpoint_digest {
        return Err(SnapshotError::CheckpointDigestMismatch);
    }
    let adapter = RocksAdapter::open(checkpoint)?;
    let adapter_index = adapter.applied_log_index()?;
    if adapter_index != manifest.applied_index {
        return Err(SnapshotError::AppliedIndexMismatch {
            manifest: manifest.applied_index,
            adapter: adapter_index,
        });
    }
    Ok(adapter)
}

fn validate_voters(voters: &[u64]) -> Result<(), SnapshotError> {
    if voters.len() > MAX_SNAPSHOT_VOTERS {
        return Err(SnapshotError::TooManyVoters {
            max: MAX_SNAPSHOT_VOTERS,
            actual: voters.len(),
        });
    }
    if voters.is_empty() || voters[0] == 0 || voters.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(SnapshotError::NonCanonicalVoters);
    }
    Ok(())
}

fn encode_timestamp(bytes: &mut Vec<u8>, timestamp: TransactionTime) {
    bytes.extend_from_slice(&timestamp.physical_micros().to_be_bytes());
    bytes.extend_from_slice(&timestamp.logical().to_be_bytes());
}

fn decode_timestamp(bytes: &[u8]) -> TransactionTime {
    TransactionTime::new(
        i64::from_be_bytes(bytes[..8].try_into().expect("fixed physical time slice")),
        u32::from_be_bytes(bytes[8..].try_into().expect("fixed logical time slice")),
    )
}

fn hash_checkpoint(root: &Path) -> Result<[u8; 32], SnapshotError> {
    let mut files = Vec::new();
    collect_files(root, root, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/RocksCheckpoint/V1");
    hasher.update(
        &u64::try_from(files.len())
            .map_err(|_| SnapshotError::CheckpointTooLarge)?
            .to_be_bytes(),
    );
    let mut buffer = vec![0_u8; 64 * 1024];
    for (relative, path) in files {
        let relative = relative.as_bytes();
        hasher.update(
            &u64::try_from(relative.len())
                .map_err(|_| SnapshotError::CheckpointTooLarge)?
                .to_be_bytes(),
        );
        hasher.update(relative);
        let mut file = File::open(path)?;
        let length = file.metadata()?.len();
        hasher.update(&length.to_be_bytes());
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
    }
    Ok(*hasher.finalize().as_bytes())
}

fn collect_files(
    root: &Path,
    directory: &Path,
    files: &mut Vec<(String, PathBuf)>,
) -> Result<(), SnapshotError> {
    for entry in std::fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_dir() {
            collect_files(root, &path, files)?;
        } else if file_type.is_file() {
            let relative = path
                .strip_prefix(root)
                .map_err(|_| SnapshotError::InvalidCheckpointPath)?
                .to_string_lossy()
                .replace('\\', "/");
            files.push((relative, path));
        } else {
            return Err(SnapshotError::UnsupportedCheckpointEntry);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    Adapter(AdapterError),
    Io(String),
    InvalidManifestMagic,
    UnsupportedManifestVersion { version: u16 },
    ManifestTruncated,
    ManifestLengthMismatch,
    ManifestChecksumMismatch,
    TooManyVoters { max: usize, actual: usize },
    NonCanonicalVoters,
    CheckpointDigestMismatch,
    AppliedIndexMismatch { manifest: u64, adapter: u64 },
    InvalidCheckpointPath,
    UnsupportedCheckpointEntry,
    CheckpointTooLarge,
}

impl Display for SnapshotError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => write!(formatter, "Adapter checkpoint error: {error}"),
            Self::Io(message) => write!(formatter, "checkpoint I/O error: {message}"),
            Self::InvalidManifestMagic => formatter.write_str("invalid snapshot manifest magic"),
            Self::UnsupportedManifestVersion { version } => {
                write!(formatter, "unsupported snapshot manifest version {version}")
            }
            Self::ManifestTruncated => formatter.write_str("snapshot manifest is truncated"),
            Self::ManifestLengthMismatch => {
                formatter.write_str("snapshot manifest length mismatch")
            }
            Self::ManifestChecksumMismatch => {
                formatter.write_str("snapshot manifest checksum mismatch")
            }
            Self::TooManyVoters { max, actual } => {
                write!(formatter, "snapshot has {actual} voters; maximum is {max}")
            }
            Self::NonCanonicalVoters => {
                formatter.write_str("snapshot voters must be nonzero, sorted, and unique")
            }
            Self::CheckpointDigestMismatch => {
                formatter.write_str("RocksDB checkpoint digest mismatch")
            }
            Self::AppliedIndexMismatch { manifest, adapter } => write!(
                formatter,
                "snapshot manifest index {manifest} differs from Adapter index {adapter}"
            ),
            Self::InvalidCheckpointPath => formatter.write_str("invalid checkpoint path"),
            Self::UnsupportedCheckpointEntry => {
                formatter.write_str("checkpoint contains a symlink or unsupported entry")
            }
            Self::CheckpointTooLarge => formatter.write_str("checkpoint size exceeds codec limits"),
        }
    }
}

impl Error for SnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Adapter(error) => Some(error),
            _ => None,
        }
    }
}

impl From<AdapterError> for SnapshotError {
    fn from(error: AdapterError) -> Self {
        Self::Adapter(error)
    }
}

impl From<io::Error> for SnapshotError {
    fn from(error: io::Error) -> Self {
        Self::Io(error.to_string())
    }
}
