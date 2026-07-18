use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

const RECEIPT_MAGIC: [u8; 4] = *b"DTMR";
const RECEIPT_VERSION: u16 = 1;
const RECEIPT_HEADER_BYTES: usize = 10;
const CHECKSUM_BYTES: usize = 4;
const MAX_RECEIPT_OUTCOME_BYTES: usize = 1024 * 1024;
const MAX_RECEIPTS: usize = 1_048_576;
const RECEIPT_FILE: &str = "receipts.log";
const INDEX_MAGIC: [u8; 4] = *b"DTCI";
const INDEX_VERSION: u16 = 1;
const INDEX_RECORD_BYTES: usize = 34;
const COMPLETE_MAGIC: [u8; 4] = *b"DTCC";
const COMPLETE_VERSION: u16 = 1;
const COMPLETE_BYTES: usize = 58;
const MAX_CHUNK_BYTES: usize = 4 * 1024 * 1024;
const MAX_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationReceipt {
    migration_id: [u8; 16],
    step: u32,
    input_digest: [u8; 32],
    outcome: Vec<u8>,
}

impl MigrationReceipt {
    #[must_use]
    pub const fn migration_id(&self) -> &[u8; 16] {
        &self.migration_id
    }

    #[must_use]
    pub const fn step(&self) -> u32 {
        self.step
    }

    #[must_use]
    pub const fn input_digest(&self) -> &[u8; 32] {
        &self.input_digest
    }

    #[must_use]
    pub fn outcome(&self) -> &[u8] {
        &self.outcome
    }
}

pub struct MigrationReceiptStore {
    path: PathBuf,
    file: File,
    receipts: BTreeMap<([u8; 16], u32), MigrationReceipt>,
}

impl MigrationReceiptStore {
    pub fn open(data_directory: impl AsRef<Path>) -> Result<Self, MigrationStorageError> {
        let directory = data_directory.as_ref().join("migration");
        std::fs::create_dir_all(&directory)?;
        let path = directory.join(RECEIPT_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&path)?;
        let receipts = replay_receipts(&mut file)?;
        Ok(Self {
            path,
            file,
            receipts,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    #[must_use]
    pub fn get(&self, migration_id: [u8; 16], step: u32) -> Option<&MigrationReceipt> {
        self.receipts.get(&(migration_id, step))
    }

    pub fn record(
        &mut self,
        migration_id: [u8; 16],
        step: u32,
        input_digest: [u8; 32],
        outcome: Vec<u8>,
    ) -> Result<ReceiptWriteOutcome, MigrationStorageError> {
        validate_receipt(migration_id, step, input_digest, &outcome)?;
        let receipt = MigrationReceipt {
            migration_id,
            step,
            input_digest,
            outcome,
        };
        if let Some(existing) = self.receipts.get(&(migration_id, step)) {
            return if existing == &receipt {
                Ok(ReceiptWriteOutcome::Duplicate)
            } else {
                Err(MigrationStorageError::ReceiptReplayConflict { step })
            };
        }
        if self.receipts.len() >= MAX_RECEIPTS {
            return Err(MigrationStorageError::TooManyReceipts);
        }
        let record = encode_receipt(&receipt)?;
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&record)?;
        self.file.sync_all()?;
        self.receipts.insert((migration_id, step), receipt);
        Ok(ReceiptWriteOutcome::Stored)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReceiptWriteOutcome {
    Stored,
    Duplicate,
}

fn validate_receipt(
    migration_id: [u8; 16],
    step: u32,
    input_digest: [u8; 32],
    outcome: &[u8],
) -> Result<(), MigrationStorageError> {
    if migration_id == [0; 16]
        || step == 0
        || input_digest == [0; 32]
        || outcome.is_empty()
        || outcome.len() > MAX_RECEIPT_OUTCOME_BYTES
    {
        return Err(MigrationStorageError::InvalidReceipt);
    }
    Ok(())
}

fn encode_receipt(receipt: &MigrationReceipt) -> Result<Vec<u8>, MigrationStorageError> {
    let outcome_length =
        u32::try_from(receipt.outcome.len()).map_err(|_| MigrationStorageError::InvalidReceipt)?;
    let payload_length = 16_usize
        .checked_add(4 + 32 + 4)
        .and_then(|length| length.checked_add(receipt.outcome.len()))
        .ok_or(MigrationStorageError::InvalidReceipt)?;
    let mut record = Vec::with_capacity(RECEIPT_HEADER_BYTES + payload_length + CHECKSUM_BYTES);
    record.extend_from_slice(&RECEIPT_MAGIC);
    record.extend_from_slice(&RECEIPT_VERSION.to_be_bytes());
    record.extend_from_slice(
        &u32::try_from(payload_length)
            .map_err(|_| MigrationStorageError::InvalidReceipt)?
            .to_be_bytes(),
    );
    record.extend_from_slice(&receipt.migration_id);
    record.extend_from_slice(&receipt.step.to_be_bytes());
    record.extend_from_slice(&receipt.input_digest);
    record.extend_from_slice(&outcome_length.to_be_bytes());
    record.extend_from_slice(&receipt.outcome);
    record.extend_from_slice(&crc32fast::hash(&record).to_be_bytes());
    Ok(record)
}

fn replay_receipts(
    file: &mut File,
) -> Result<BTreeMap<([u8; 16], u32), MigrationReceipt>, MigrationStorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let mut receipts = BTreeMap::new();
    let mut offset = 0_usize;
    while offset < bytes.len() {
        let remaining = &bytes[offset..];
        if remaining.len() < RECEIPT_HEADER_BYTES {
            break;
        }
        if remaining[..4] != RECEIPT_MAGIC {
            return Err(MigrationStorageError::InvalidReceiptMagic);
        }
        if u16::from_be_bytes(remaining[4..6].try_into().expect("fixed receipt version"))
            != RECEIPT_VERSION
        {
            return Err(MigrationStorageError::UnsupportedReceiptVersion);
        }
        let payload_length = usize::try_from(u32::from_be_bytes(
            remaining[6..10]
                .try_into()
                .expect("fixed receipt payload length"),
        ))
        .map_err(|_| MigrationStorageError::InvalidReceipt)?;
        if payload_length > MAX_RECEIPT_OUTCOME_BYTES + 56 {
            return Err(MigrationStorageError::InvalidReceipt);
        }
        let record_length = RECEIPT_HEADER_BYTES
            .checked_add(payload_length)
            .and_then(|length| length.checked_add(CHECKSUM_BYTES))
            .ok_or(MigrationStorageError::InvalidReceipt)?;
        if remaining.len() < record_length {
            break;
        }
        let record = &remaining[..record_length];
        let checksum_offset = record.len() - CHECKSUM_BYTES;
        let expected = u32::from_be_bytes(
            record[checksum_offset..]
                .try_into()
                .expect("fixed receipt checksum"),
        );
        if crc32fast::hash(&record[..checksum_offset]) != expected {
            return Err(MigrationStorageError::ReceiptChecksumMismatch);
        }
        let receipt = decode_receipt(&record[RECEIPT_HEADER_BYTES..checksum_offset])?;
        let key = (receipt.migration_id, receipt.step);
        if let Some(existing) = receipts.insert(key, receipt.clone())
            && existing != receipt
        {
            return Err(MigrationStorageError::ReceiptReplayConflict { step: receipt.step });
        }
        if receipts.len() > MAX_RECEIPTS {
            return Err(MigrationStorageError::TooManyReceipts);
        }
        offset += record_length;
    }
    if offset < bytes.len() {
        file.set_len(offset as u64)?;
        file.sync_all()?;
    }
    file.seek(SeekFrom::End(0))?;
    Ok(receipts)
}

fn decode_receipt(payload: &[u8]) -> Result<MigrationReceipt, MigrationStorageError> {
    if payload.len() < 56 {
        return Err(MigrationStorageError::InvalidReceipt);
    }
    let migration_id = payload[..16]
        .try_into()
        .expect("fixed receipt migration ID");
    let step = u32::from_be_bytes(payload[16..20].try_into().expect("fixed receipt step"));
    let input_digest = payload[20..52]
        .try_into()
        .expect("fixed receipt input digest");
    let outcome_length = usize::try_from(u32::from_be_bytes(
        payload[52..56]
            .try_into()
            .expect("fixed receipt outcome length"),
    ))
    .map_err(|_| MigrationStorageError::InvalidReceipt)?;
    if payload.len() != 56 + outcome_length {
        return Err(MigrationStorageError::InvalidReceipt);
    }
    let outcome = payload[56..].to_vec();
    validate_receipt(migration_id, step, input_digest, &outcome)?;
    Ok(MigrationReceipt {
        migration_id,
        step,
        input_digest,
        outcome,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MigrationChunk {
    migration_id: [u8; 16],
    ordinal: u64,
    payload: Vec<u8>,
    checksum: u32,
    terminal: bool,
    content_digest: Option<[u8; 32]>,
}

impl MigrationChunk {
    pub fn new(
        migration_id: [u8; 16],
        ordinal: u64,
        payload: Vec<u8>,
        terminal: bool,
        content_digest: Option<[u8; 32]>,
    ) -> Result<Self, MigrationStorageError> {
        let checksum = crc32fast::hash(&payload);
        Self::new_with_checksum(
            migration_id,
            ordinal,
            payload,
            checksum,
            terminal,
            content_digest,
        )
    }

    pub fn new_with_checksum(
        migration_id: [u8; 16],
        ordinal: u64,
        payload: Vec<u8>,
        checksum: u32,
        terminal: bool,
        content_digest: Option<[u8; 32]>,
    ) -> Result<Self, MigrationStorageError> {
        if migration_id == [0; 16]
            || payload.is_empty()
            || payload.len() > MAX_CHUNK_BYTES
            || crc32fast::hash(&payload) != checksum
            || terminal != content_digest.is_some()
            || content_digest == Some([0; 32])
        {
            return Err(MigrationStorageError::InvalidChunk);
        }
        Ok(Self {
            migration_id,
            ordinal,
            payload,
            checksum,
            terminal,
            content_digest,
        })
    }
}

pub struct SnapshotInbox {
    root: PathBuf,
    gate: Mutex<()>,
}

impl SnapshotInbox {
    pub fn open(data_directory: impl AsRef<Path>) -> Result<Self, MigrationStorageError> {
        let root = data_directory.as_ref().join("migration").join("staging");
        std::fs::create_dir_all(&root)?;
        sync_directory(root.parent().expect("staging directory has parent"))?;
        Ok(Self {
            root,
            gate: Mutex::new(()),
        })
    }

    pub fn append(
        &self,
        chunk: MigrationChunk,
    ) -> Result<ChunkAppendOutcome, MigrationStorageError> {
        let _guard = self
            .gate
            .lock()
            .map_err(|_| MigrationStorageError::LockPoisoned)?;
        let directory = self.root.join(hex_id(chunk.migration_id));
        std::fs::create_dir_all(&directory)?;
        let archive_path = directory.join("archive.dtsa.part");
        let index_path = directory.join("chunks.index");
        let complete_path = directory.join("complete.dtg");
        let mut archive = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&archive_path)?;
        let entries = replay_chunk_index(&index_path, &mut archive)?;
        if chunk.ordinal > entries.len() as u64 {
            return Err(MigrationStorageError::UnexpectedChunkOrdinal {
                expected: entries.len() as u64,
                actual: chunk.ordinal,
            });
        }
        let duplicate = if chunk.ordinal < entries.len() as u64 {
            let entry = &entries[chunk.ordinal as usize];
            if entry.length != chunk.payload.len() as u32 || entry.checksum != chunk.checksum {
                return Err(MigrationStorageError::ChunkReplayConflict {
                    ordinal: chunk.ordinal,
                });
            }
            let mut existing = vec![0_u8; chunk.payload.len()];
            archive.seek(SeekFrom::Start(entry.offset))?;
            archive.read_exact(&mut existing)?;
            if existing != chunk.payload {
                return Err(MigrationStorageError::ChunkReplayConflict {
                    ordinal: chunk.ordinal,
                });
            }
            true
        } else {
            let offset = archive.seek(SeekFrom::End(0))?;
            let new_length = offset
                .checked_add(chunk.payload.len() as u64)
                .ok_or(MigrationStorageError::ArchiveTooLarge)?;
            if new_length > MAX_ARCHIVE_BYTES {
                return Err(MigrationStorageError::ArchiveTooLarge);
            }
            archive.write_all(&chunk.payload)?;
            archive.sync_all()?;
            append_chunk_index(
                &index_path,
                ChunkIndexEntry {
                    ordinal: chunk.ordinal,
                    offset,
                    length: chunk.payload.len() as u32,
                    checksum: chunk.checksum,
                },
            )?;
            false
        };
        let next_ordinal = entries.len() as u64 + u64::from(!duplicate);
        if !chunk.terminal {
            return Ok(if duplicate {
                ChunkAppendOutcome::Duplicate { next_ordinal }
            } else {
                ChunkAppendOutcome::Stored { next_ordinal }
            });
        }
        let expected_digest = chunk
            .content_digest
            .expect("terminal migration chunk has content digest");
        let actual_digest = hash_open_file(&mut archive)?;
        if actual_digest != expected_digest {
            return Err(MigrationStorageError::ContentDigestMismatch);
        }
        let archive_length = archive.metadata()?.len();
        if complete_path.exists() {
            let complete = read_complete(&complete_path)?;
            if complete != (expected_digest, archive_length, chunk.ordinal) {
                return Err(MigrationStorageError::CompletionConflict);
            }
        } else {
            write_complete(
                &complete_path,
                expected_digest,
                archive_length,
                chunk.ordinal,
            )?;
        }
        Ok(ChunkAppendOutcome::Completed {
            archive_path,
            content_digest: expected_digest,
            duplicate,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ChunkAppendOutcome {
    Stored {
        next_ordinal: u64,
    },
    Duplicate {
        next_ordinal: u64,
    },
    Completed {
        archive_path: PathBuf,
        content_digest: [u8; 32],
        duplicate: bool,
    },
}

#[derive(Clone, Copy)]
struct ChunkIndexEntry {
    ordinal: u64,
    offset: u64,
    length: u32,
    checksum: u32,
}

fn replay_chunk_index(
    index_path: &Path,
    archive: &mut File,
) -> Result<Vec<ChunkIndexEntry>, MigrationStorageError> {
    let mut index = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(index_path)?;
    let mut bytes = Vec::new();
    index.read_to_end(&mut bytes)?;
    let complete_length = bytes.len() / INDEX_RECORD_BYTES * INDEX_RECORD_BYTES;
    if complete_length != bytes.len() {
        index.set_len(complete_length as u64)?;
        index.sync_all()?;
        bytes.truncate(complete_length);
    }
    let mut entries = Vec::new();
    let mut expected_offset = 0_u64;
    for record in bytes.chunks_exact(INDEX_RECORD_BYTES) {
        if record[..4] != INDEX_MAGIC
            || u16::from_be_bytes(record[4..6].try_into().expect("fixed chunk index version"))
                != INDEX_VERSION
        {
            return Err(MigrationStorageError::InvalidChunkIndex);
        }
        let stored_crc = u32::from_be_bytes(
            record[30..34]
                .try_into()
                .expect("fixed chunk index checksum"),
        );
        if crc32fast::hash(&record[..30]) != stored_crc {
            return Err(MigrationStorageError::ChunkIndexChecksumMismatch);
        }
        let entry = ChunkIndexEntry {
            ordinal: u64::from_be_bytes(record[6..14].try_into().expect("fixed chunk ordinal")),
            offset: u64::from_be_bytes(record[14..22].try_into().expect("fixed chunk offset")),
            length: u32::from_be_bytes(record[22..26].try_into().expect("fixed chunk length")),
            checksum: u32::from_be_bytes(
                record[26..30].try_into().expect("fixed chunk payload CRC"),
            ),
        };
        if entry.ordinal != entries.len() as u64
            || entry.offset != expected_offset
            || entry.length == 0
            || entry.length as usize > MAX_CHUNK_BYTES
        {
            return Err(MigrationStorageError::InvalidChunkIndex);
        }
        expected_offset = expected_offset
            .checked_add(u64::from(entry.length))
            .ok_or(MigrationStorageError::ArchiveTooLarge)?;
        entries.push(entry);
    }
    let archive_length = archive.metadata()?.len();
    if archive_length < expected_offset {
        return Err(MigrationStorageError::TruncatedArchive);
    }
    if archive_length > expected_offset {
        archive.set_len(expected_offset)?;
        archive.sync_all()?;
    }
    for entry in &entries {
        let mut payload = vec![0_u8; entry.length as usize];
        archive.seek(SeekFrom::Start(entry.offset))?;
        archive.read_exact(&mut payload)?;
        if crc32fast::hash(&payload) != entry.checksum {
            return Err(MigrationStorageError::ChunkPayloadChecksumMismatch);
        }
    }
    Ok(entries)
}

fn append_chunk_index(path: &Path, entry: ChunkIndexEntry) -> Result<(), MigrationStorageError> {
    let mut record = Vec::with_capacity(INDEX_RECORD_BYTES);
    record.extend_from_slice(&INDEX_MAGIC);
    record.extend_from_slice(&INDEX_VERSION.to_be_bytes());
    record.extend_from_slice(&entry.ordinal.to_be_bytes());
    record.extend_from_slice(&entry.offset.to_be_bytes());
    record.extend_from_slice(&entry.length.to_be_bytes());
    record.extend_from_slice(&entry.checksum.to_be_bytes());
    record.extend_from_slice(&crc32fast::hash(&record).to_be_bytes());
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    file.write_all(&record)?;
    file.sync_all()?;
    Ok(())
}

fn hash_open_file(file: &mut File) -> Result<[u8; 32], MigrationStorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(*hasher.finalize().as_bytes())
}

fn write_complete(
    path: &Path,
    digest: [u8; 32],
    archive_length: u64,
    terminal_ordinal: u64,
) -> Result<(), MigrationStorageError> {
    let mut bytes = Vec::with_capacity(COMPLETE_BYTES);
    bytes.extend_from_slice(&COMPLETE_MAGIC);
    bytes.extend_from_slice(&COMPLETE_VERSION.to_be_bytes());
    bytes.extend_from_slice(&digest);
    bytes.extend_from_slice(&archive_length.to_be_bytes());
    bytes.extend_from_slice(&terminal_ordinal.to_be_bytes());
    bytes.extend_from_slice(&crc32fast::hash(&bytes).to_be_bytes());
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    std::fs::rename(temporary, path)?;
    sync_directory(path.parent().expect("completion file has parent"))
}

fn read_complete(path: &Path) -> Result<([u8; 32], u64, u64), MigrationStorageError> {
    let bytes = std::fs::read(path)?;
    if bytes.len() != COMPLETE_BYTES
        || bytes[..4] != COMPLETE_MAGIC
        || u16::from_be_bytes(bytes[4..6].try_into().expect("fixed completion version"))
            != COMPLETE_VERSION
        || crc32fast::hash(&bytes[..54])
            != u32::from_be_bytes(bytes[54..].try_into().expect("fixed completion checksum"))
    {
        return Err(MigrationStorageError::InvalidCompletion);
    }
    Ok((
        bytes[6..38].try_into().expect("fixed completion digest"),
        u64::from_be_bytes(bytes[38..46].try_into().expect("fixed archive length")),
        u64::from_be_bytes(bytes[46..54].try_into().expect("fixed terminal ordinal")),
    ))
}

fn sync_directory(path: &Path) -> Result<(), MigrationStorageError> {
    File::open(path)?.sync_all()?;
    Ok(())
}

fn hex_id(id: [u8; 16]) -> String {
    id.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MigrationStorageError {
    Io(String),
    LockPoisoned,
    InvalidReceipt,
    InvalidReceiptMagic,
    UnsupportedReceiptVersion,
    ReceiptChecksumMismatch,
    ReceiptReplayConflict { step: u32 },
    TooManyReceipts,
    InvalidChunk,
    UnexpectedChunkOrdinal { expected: u64, actual: u64 },
    ChunkReplayConflict { ordinal: u64 },
    InvalidChunkIndex,
    ChunkIndexChecksumMismatch,
    ChunkPayloadChecksumMismatch,
    TruncatedArchive,
    ArchiveTooLarge,
    ContentDigestMismatch,
    InvalidCompletion,
    CompletionConflict,
}

impl Display for MigrationStorageError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "migration storage I/O error: {message}"),
            Self::LockPoisoned => formatter.write_str("migration storage lock poisoned"),
            Self::InvalidReceipt => formatter.write_str("invalid migration receipt"),
            Self::InvalidReceiptMagic => formatter.write_str("invalid migration receipt magic"),
            Self::UnsupportedReceiptVersion => {
                formatter.write_str("unsupported migration receipt version")
            }
            Self::ReceiptChecksumMismatch => {
                formatter.write_str("migration receipt checksum mismatch")
            }
            Self::ReceiptReplayConflict { step } => {
                write!(formatter, "migration receipt step {step} replay conflict")
            }
            Self::TooManyReceipts => formatter.write_str("too many migration receipts"),
            Self::InvalidChunk => formatter.write_str("invalid migration snapshot chunk"),
            Self::UnexpectedChunkOrdinal { expected, actual } => {
                write!(
                    formatter,
                    "snapshot chunk ordinal {actual}; expected {expected}"
                )
            }
            Self::ChunkReplayConflict { ordinal } => {
                write!(formatter, "snapshot chunk {ordinal} replay conflict")
            }
            Self::InvalidChunkIndex => formatter.write_str("invalid snapshot chunk index"),
            Self::ChunkIndexChecksumMismatch => {
                formatter.write_str("snapshot chunk index checksum mismatch")
            }
            Self::ChunkPayloadChecksumMismatch => {
                formatter.write_str("snapshot staged payload checksum mismatch")
            }
            Self::TruncatedArchive => formatter.write_str("snapshot staged archive is truncated"),
            Self::ArchiveTooLarge => formatter.write_str("snapshot archive exceeds its size bound"),
            Self::ContentDigestMismatch => {
                formatter.write_str("snapshot archive content digest mismatch")
            }
            Self::InvalidCompletion => formatter.write_str("invalid snapshot completion record"),
            Self::CompletionConflict => formatter.write_str("snapshot completion replay conflict"),
        }
    }
}

impl Error for MigrationStorageError {}

impl From<std::io::Error> for MigrationStorageError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}
