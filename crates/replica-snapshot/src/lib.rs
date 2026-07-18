#![forbid(unsafe_code)]

use std::collections::BTreeSet;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use adapter_rocksdb::RocksAdapter;
use raft::Storage;
use raft::eraftpb::{ConfState, Snapshot, SnapshotMetadata};
use raft_logstore::RocksRaftStorage;
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
const MANIFEST_FILE: &str = "manifest.dtg";
const CHECKPOINT_DIRECTORY: &str = "checkpoint";
const INSTALLED_ADAPTER_DIRECTORY: &str = "adapter";
const INSTALLED_RAFT_DIRECTORY: &str = "raft";
const ARCHIVE_MAGIC: [u8; 4] = *b"DTSA";
const ARCHIVE_VERSION: u16 = 1;
const MAX_ARCHIVE_FILES: usize = 1_048_576;
const MAX_ARCHIVE_PATH_BYTES: usize = 1_024;
const MAX_ARCHIVE_FILE_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_BYTES: u64 = 1024 * 1024 * 1024 * 1024;
static STAGING_SEQUENCE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotFailpoint {
    BeforeCheckpoint,
    AfterCheckpointBeforeManifest,
    AfterManifestSyncBeforePublish,
    AfterAdapterCopyBeforeSnapshotPersist,
    AfterSnapshotPersistBeforePublish,
    AfterBundlePublishBeforeWalSnapshot,
    AfterWalSnapshotPersist,
    AfterPublish,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledSnapshot {
    pub root: PathBuf,
    pub adapter_path: PathBuf,
    pub raft_wal_path: PathBuf,
    pub manifest: SnapshotManifestV1,
}

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
    create_adapter_checkpoint(machine, voters, destination)
}

fn create_adapter_checkpoint<A: StorageAdapter>(
    machine: &ShardStateMachine<A>,
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
    machine
        .adapter()
        .create_physical_checkpoint(destination.as_ref())?;
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

pub fn create_snapshot_bundle(
    machine: &ShardStateMachine<impl StorageAdapter>,
    voters: &[u64],
    destination: impl AsRef<Path>,
) -> Result<SnapshotManifestV1, SnapshotError> {
    create_snapshot_bundle_inner(machine, voters, destination.as_ref(), None)
}

pub fn create_snapshot_bundle_with_failpoint(
    machine: &ShardStateMachine<impl StorageAdapter>,
    voters: &[u64],
    destination: impl AsRef<Path>,
    failpoint: SnapshotFailpoint,
) -> Result<SnapshotManifestV1, SnapshotError> {
    create_snapshot_bundle_inner(machine, voters, destination.as_ref(), Some(failpoint))
}

fn create_snapshot_bundle_inner<A: StorageAdapter>(
    machine: &ShardStateMachine<A>,
    voters: &[u64],
    destination: &Path,
    failpoint: Option<SnapshotFailpoint>,
) -> Result<SnapshotManifestV1, SnapshotError> {
    let staging = StagedDirectory::new(destination)?;
    fail_if(failpoint, SnapshotFailpoint::BeforeCheckpoint)?;
    let checkpoint = staging.path().join(CHECKPOINT_DIRECTORY);
    let manifest = create_adapter_checkpoint(machine, voters, &checkpoint)?;
    sync_tree(&checkpoint)?;
    fail_if(failpoint, SnapshotFailpoint::AfterCheckpointBeforeManifest)?;
    write_manifest(staging.path(), &manifest)?;
    fail_if(failpoint, SnapshotFailpoint::AfterManifestSyncBeforePublish)?;
    staging.publish(destination)?;
    fail_if(failpoint, SnapshotFailpoint::AfterPublish)?;
    Ok(manifest)
}

pub fn open_snapshot_bundle(bundle: impl AsRef<Path>) -> Result<SnapshotManifestV1, SnapshotError> {
    let bundle = bundle.as_ref();
    let manifest = read_manifest(bundle)?;
    let digest = hash_checkpoint(&bundle.join(CHECKPOINT_DIRECTORY))?;
    if digest != manifest.checkpoint_digest {
        return Err(SnapshotError::CheckpointDigestMismatch);
    }
    Ok(manifest)
}

pub fn write_snapshot_archive(
    bundle: impl AsRef<Path>,
    mut output: impl Write,
) -> Result<[u8; 32], SnapshotError> {
    let bundle = bundle.as_ref();
    open_snapshot_bundle(bundle)?;
    let mut files = Vec::new();
    collect_files(bundle, bundle, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    if files.is_empty() || files.len() > MAX_ARCHIVE_FILES {
        return Err(SnapshotError::ArchiveTooLarge);
    }
    let mut archive_hasher = blake3::Hasher::new();
    write_archive_bytes(&mut output, &mut archive_hasher, &ARCHIVE_MAGIC)?;
    write_archive_bytes(
        &mut output,
        &mut archive_hasher,
        &ARCHIVE_VERSION.to_be_bytes(),
    )?;
    write_archive_bytes(
        &mut output,
        &mut archive_hasher,
        &u32::try_from(files.len())
            .map_err(|_| SnapshotError::ArchiveTooLarge)?
            .to_be_bytes(),
    )?;
    let mut total = 10_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    for (relative, path) in files {
        validate_archive_path(&relative)?;
        let relative_bytes = relative.as_bytes();
        let path_length =
            u16::try_from(relative_bytes.len()).map_err(|_| SnapshotError::InvalidArchivePath)?;
        let file_length = std::fs::metadata(&path)?.len();
        if file_length > MAX_ARCHIVE_FILE_BYTES {
            return Err(SnapshotError::ArchiveTooLarge);
        }
        let file_digest = hash_file(&path)?;
        for bytes in [
            path_length.to_be_bytes().as_slice(),
            file_length.to_be_bytes().as_slice(),
            file_digest.as_slice(),
            relative_bytes,
        ] {
            write_archive_bytes(&mut output, &mut archive_hasher, bytes)?;
        }
        let mut file = File::open(path)?;
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            write_archive_bytes(&mut output, &mut archive_hasher, &buffer[..read])?;
        }
        total = total
            .checked_add(2 + 8 + 32)
            .and_then(|value| value.checked_add(u64::from(path_length)))
            .and_then(|value| value.checked_add(file_length))
            .ok_or(SnapshotError::ArchiveTooLarge)?;
        if total > MAX_ARCHIVE_BYTES {
            return Err(SnapshotError::ArchiveTooLarge);
        }
    }
    output.flush()?;
    Ok(*archive_hasher.finalize().as_bytes())
}

pub fn extract_snapshot_archive(
    mut input: impl Read,
    destination: impl AsRef<Path>,
) -> Result<SnapshotManifestV1, SnapshotError> {
    let destination = destination.as_ref();
    let mut header = [0_u8; 10];
    input.read_exact(&mut header)?;
    if header[..4] != ARCHIVE_MAGIC {
        return Err(SnapshotError::InvalidArchiveMagic);
    }
    let version = u16::from_be_bytes(header[4..6].try_into().expect("fixed archive version"));
    if version != ARCHIVE_VERSION {
        return Err(SnapshotError::UnsupportedArchiveVersion { version });
    }
    let file_count = usize::try_from(u32::from_be_bytes(
        header[6..10].try_into().expect("fixed archive file count"),
    ))
    .map_err(|_| SnapshotError::ArchiveTooLarge)?;
    if file_count == 0 || file_count > MAX_ARCHIVE_FILES {
        return Err(SnapshotError::ArchiveTooLarge);
    }
    let staging = StagedDirectory::new(destination)?;
    let mut paths = BTreeSet::new();
    let mut total = 10_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    for _ in 0..file_count {
        let mut entry_header = [0_u8; 42];
        input.read_exact(&mut entry_header)?;
        let path_length = usize::from(u16::from_be_bytes(
            entry_header[..2]
                .try_into()
                .expect("fixed archive path length"),
        ));
        let file_length = u64::from_be_bytes(
            entry_header[2..10]
                .try_into()
                .expect("fixed archive file length"),
        );
        let expected_digest: [u8; 32] = entry_header[10..]
            .try_into()
            .expect("fixed archive file digest");
        if path_length == 0
            || path_length > MAX_ARCHIVE_PATH_BYTES
            || file_length > MAX_ARCHIVE_FILE_BYTES
        {
            return Err(SnapshotError::ArchiveTooLarge);
        }
        let mut path_bytes = vec![0_u8; path_length];
        input.read_exact(&mut path_bytes)?;
        let relative =
            String::from_utf8(path_bytes).map_err(|_| SnapshotError::InvalidArchivePath)?;
        validate_archive_path(&relative)?;
        if !paths.insert(relative.clone()) {
            return Err(SnapshotError::DuplicateArchivePath);
        }
        let path = staging.path().join(&relative);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
        let mut remaining = file_length;
        let mut file_hasher = blake3::Hasher::new();
        while remaining > 0 {
            let maximum = usize::try_from(remaining.min(buffer.len() as u64))
                .map_err(|_| SnapshotError::ArchiveTooLarge)?;
            input.read_exact(&mut buffer[..maximum])?;
            file.write_all(&buffer[..maximum])?;
            file_hasher.update(&buffer[..maximum]);
            remaining -= maximum as u64;
        }
        file.sync_all()?;
        if file_hasher.finalize().as_bytes() != &expected_digest {
            return Err(SnapshotError::ArchiveFileDigestMismatch);
        }
        total = total
            .checked_add(42)
            .and_then(|value| value.checked_add(path_length as u64))
            .and_then(|value| value.checked_add(file_length))
            .ok_or(SnapshotError::ArchiveTooLarge)?;
        if total > MAX_ARCHIVE_BYTES {
            return Err(SnapshotError::ArchiveTooLarge);
        }
    }
    let mut trailing = [0_u8; 1];
    if input.read(&mut trailing)? != 0 {
        return Err(SnapshotError::ArchiveTrailingBytes);
    }
    sync_tree(staging.path())?;
    let manifest = open_snapshot_bundle(staging.path())?;
    staging.publish(destination)?;
    Ok(manifest)
}

fn write_archive_bytes(
    output: &mut impl Write,
    hasher: &mut blake3::Hasher,
    bytes: &[u8],
) -> Result<(), SnapshotError> {
    output.write_all(bytes)?;
    hasher.update(bytes);
    Ok(())
}

fn hash_file(path: &Path) -> Result<[u8; 32], SnapshotError> {
    let mut file = File::open(path)?;
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

fn validate_archive_path(relative: &str) -> Result<(), SnapshotError> {
    let path = Path::new(relative);
    if relative.is_empty()
        || relative.len() > MAX_ARCHIVE_PATH_BYTES
        || relative.contains('\\')
        || path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(SnapshotError::InvalidArchivePath);
    }
    Ok(())
}

pub fn create_and_activate_local_snapshot<A: StorageAdapter>(
    machine: &ShardStateMachine<A>,
    raft_storage: &RocksRaftStorage,
    voters: &[u64],
    destination: impl AsRef<Path>,
) -> Result<SnapshotManifestV1, SnapshotError> {
    create_and_activate_local_snapshot_inner(
        machine,
        raft_storage,
        voters,
        destination.as_ref(),
        None,
    )
}

pub fn create_and_activate_local_snapshot_with_failpoint<A: StorageAdapter>(
    machine: &ShardStateMachine<A>,
    raft_storage: &RocksRaftStorage,
    voters: &[u64],
    destination: impl AsRef<Path>,
    failpoint: SnapshotFailpoint,
) -> Result<SnapshotManifestV1, SnapshotError> {
    create_and_activate_local_snapshot_inner(
        machine,
        raft_storage,
        voters,
        destination.as_ref(),
        Some(failpoint),
    )
}

fn create_and_activate_local_snapshot_inner<A: StorageAdapter>(
    machine: &ShardStateMachine<A>,
    raft_storage: &RocksRaftStorage,
    voters: &[u64],
    destination: &Path,
    failpoint: Option<SnapshotFailpoint>,
) -> Result<SnapshotManifestV1, SnapshotError> {
    let manifest = match failpoint {
        Some(
            point @ (SnapshotFailpoint::BeforeCheckpoint
            | SnapshotFailpoint::AfterCheckpointBeforeManifest
            | SnapshotFailpoint::AfterManifestSyncBeforePublish
            | SnapshotFailpoint::AfterPublish),
        ) => create_snapshot_bundle_with_failpoint(machine, voters, destination, point)?,
        _ => create_snapshot_bundle(machine, voters, destination)?,
    };
    fail_if(
        failpoint,
        SnapshotFailpoint::AfterBundlePublishBeforeWalSnapshot,
    )?;
    activate_published_local_snapshot(raft_storage, destination)?;
    fail_if(failpoint, SnapshotFailpoint::AfterWalSnapshotPersist)?;
    Ok(manifest)
}

pub fn activate_published_local_snapshot(
    raft_storage: &RocksRaftStorage,
    bundle: impl AsRef<Path>,
) -> Result<SnapshotManifestV1, SnapshotError> {
    let manifest = open_snapshot_bundle(bundle)?;
    let raft_state = raft_storage
        .initial_state()
        .map_err(raft_logstore::RaftLogStoreError::from)?;
    let position_matches = raft_state.conf_state.voters == manifest.voters
        && raft_state.hard_state.commit >= manifest.applied_index
        && raft_storage
            .term(manifest.applied_index)
            .map_err(raft_logstore::RaftLogStoreError::from)?
            == manifest.term;
    if !position_matches {
        return Err(SnapshotError::RaftSnapshotPositionMismatch);
    }
    raft_storage.persist_local_snapshot_preserving_suffix(&raft_snapshot(&manifest)?)?;
    Ok(manifest)
}

pub async fn install_snapshot_bundle(
    bundle: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<InstalledSnapshot, SnapshotError> {
    install_snapshot_bundle_inner(bundle.as_ref(), destination.as_ref(), None, None).await
}

pub async fn open_installed_snapshot(
    destination: impl AsRef<Path>,
) -> Result<InstalledSnapshot, SnapshotError> {
    let root = destination.as_ref().to_path_buf();
    let manifest = read_manifest(&root)?;
    let adapter_path = root.join(INSTALLED_ADAPTER_DIRECTORY);
    let adapter = open_verified_checkpoint(&adapter_path, &manifest)?;
    let machine = ShardStateMachine::open(adapter, manifest.shard_id, manifest.placement_epoch)
        .await
        .map_err(|error| SnapshotError::StateMachine(error.to_string()))?;
    validate_machine_manifest(&machine, &manifest)?;
    drop(machine);
    let raft_wal_path = root.join(INSTALLED_RAFT_DIRECTORY);
    let raft_storage = RocksRaftStorage::open(&raft_wal_path, &manifest.voters)?;
    let raft_state = raft_storage
        .initial_state()
        .map_err(raft_logstore::RaftLogStoreError::from)?;
    if raft_state.hard_state.commit < manifest.applied_index
        || raft_storage
            .term(manifest.applied_index)
            .map_err(raft_logstore::RaftLogStoreError::from)?
            != manifest.term
    {
        return Err(SnapshotError::RaftSnapshotPositionMismatch);
    }
    Ok(InstalledSnapshot {
        root,
        adapter_path,
        raft_wal_path,
        manifest,
    })
}

pub async fn install_snapshot_bundle_with_failpoint(
    bundle: impl AsRef<Path>,
    destination: impl AsRef<Path>,
    failpoint: SnapshotFailpoint,
) -> Result<InstalledSnapshot, SnapshotError> {
    install_snapshot_bundle_inner(bundle.as_ref(), destination.as_ref(), Some(failpoint), None)
        .await
}

pub async fn install_received_snapshot_bundle(
    incoming_snapshot: &Snapshot,
    bundle: impl AsRef<Path>,
    destination: impl AsRef<Path>,
) -> Result<InstalledSnapshot, SnapshotError> {
    install_snapshot_bundle_inner(
        bundle.as_ref(),
        destination.as_ref(),
        None,
        Some(incoming_snapshot),
    )
    .await
}

async fn install_snapshot_bundle_inner(
    bundle: &Path,
    destination: &Path,
    failpoint: Option<SnapshotFailpoint>,
    incoming_snapshot: Option<&Snapshot>,
) -> Result<InstalledSnapshot, SnapshotError> {
    let manifest = open_snapshot_bundle(bundle)?;
    let manifest_snapshot = raft_snapshot(&manifest)?;
    if incoming_snapshot.is_some_and(|incoming| *incoming != manifest_snapshot) {
        return Err(SnapshotError::IncomingRaftSnapshotMismatch);
    }
    let staging = StagedDirectory::new(destination)?;
    let adapter_path = staging.path().join(INSTALLED_ADAPTER_DIRECTORY);
    copy_tree(&bundle.join(CHECKPOINT_DIRECTORY), &adapter_path)?;
    let copied_digest = hash_checkpoint(&adapter_path)?;
    if copied_digest != manifest.checkpoint_digest {
        return Err(SnapshotError::CheckpointDigestMismatch);
    }
    fail_if(
        failpoint,
        SnapshotFailpoint::AfterAdapterCopyBeforeSnapshotPersist,
    )?;

    let adapter = open_verified_checkpoint(&adapter_path, &manifest)?;
    let machine = ShardStateMachine::open(adapter, manifest.shard_id, manifest.placement_epoch)
        .await
        .map_err(|error| SnapshotError::StateMachine(error.to_string()))?;
    validate_machine_manifest(&machine, &manifest)?;
    drop(machine);

    write_manifest(staging.path(), &manifest)?;
    let raft_wal_path = staging.path().join(INSTALLED_RAFT_DIRECTORY);
    let raft_storage = RocksRaftStorage::open(&raft_wal_path, &manifest.voters)?;
    raft_storage.persist_snapshot(&manifest_snapshot)?;
    drop(raft_storage);
    sync_tree(staging.path())?;
    fail_if(
        failpoint,
        SnapshotFailpoint::AfterSnapshotPersistBeforePublish,
    )?;
    staging.publish(destination)?;

    let installed = InstalledSnapshot {
        root: destination.to_path_buf(),
        adapter_path: destination.join(INSTALLED_ADAPTER_DIRECTORY),
        raft_wal_path: destination.join(INSTALLED_RAFT_DIRECTORY),
        manifest,
    };
    fail_if(failpoint, SnapshotFailpoint::AfterPublish)?;
    Ok(installed)
}

pub fn raft_snapshot(manifest: &SnapshotManifestV1) -> Result<Snapshot, SnapshotError> {
    Ok(Snapshot {
        data: manifest.encode()?,
        metadata: Some(SnapshotMetadata {
            conf_state: Some(ConfState {
                voters: manifest.voters.clone(),
                ..Default::default()
            }),
            index: manifest.applied_index,
            term: manifest.term,
        }),
    })
}

fn validate_machine_manifest(
    machine: &ShardStateMachine<RocksAdapter>,
    manifest: &SnapshotManifestV1,
) -> Result<(), SnapshotError> {
    let metadata = machine.metadata();
    if metadata.shard_id != manifest.shard_id
        || metadata.placement_epoch != manifest.placement_epoch
        || metadata.last_term != manifest.term
        || metadata.applied_index != manifest.applied_index
        || metadata.closed_ts != manifest.closed_ts
        || metadata.resolved_ts != manifest.resolved_ts
        || metadata.adapter_applied_ts != manifest.adapter_applied_ts
    {
        return Err(SnapshotError::ReplicaMetadataMismatch);
    }
    Ok(())
}

fn write_manifest(root: &Path, manifest: &SnapshotManifestV1) -> Result<(), SnapshotError> {
    let path = root.join(MANIFEST_FILE);
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    file.write_all(&manifest.encode()?)?;
    file.sync_all()?;
    sync_directory(root)
}

fn read_manifest(root: &Path) -> Result<SnapshotManifestV1, SnapshotError> {
    let mut file = File::open(root.join(MANIFEST_FILE))?;
    let length = usize::try_from(file.metadata()?.len())
        .map_err(|_| SnapshotError::ManifestLengthMismatch)?;
    let maximum = MIN_MANIFEST_BYTES + MAX_SNAPSHOT_VOTERS * 8;
    if length > maximum {
        return Err(SnapshotError::ManifestLengthMismatch);
    }
    let mut bytes = Vec::with_capacity(length);
    file.read_to_end(&mut bytes)?;
    SnapshotManifestV1::decode(&bytes)
}

fn copy_tree(source: &Path, destination: &Path) -> Result<(), SnapshotError> {
    std::fs::create_dir(destination)?;
    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let target = destination.join(entry.file_name());
        if file_type.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else if file_type.is_file() {
            std::fs::copy(entry.path(), &target)?;
            File::open(&target)?.sync_all()?;
        } else {
            return Err(SnapshotError::UnsupportedCheckpointEntry);
        }
    }
    sync_directory(destination)
}

fn sync_tree(root: &Path) -> Result<(), SnapshotError> {
    for entry in std::fs::read_dir(root)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            sync_tree(&entry.path())?;
        } else if file_type.is_file() {
            File::open(entry.path())?.sync_all()?;
        } else {
            return Err(SnapshotError::UnsupportedCheckpointEntry);
        }
    }
    sync_directory(root)
}

fn sync_directory(directory: &Path) -> Result<(), SnapshotError> {
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn fail_if(
    configured: Option<SnapshotFailpoint>,
    current: SnapshotFailpoint,
) -> Result<(), SnapshotError> {
    if configured == Some(current) {
        Err(SnapshotError::InjectedFailure(current))
    } else {
        Ok(())
    }
}

struct StagedDirectory {
    path: PathBuf,
    published: bool,
}

impl StagedDirectory {
    fn new(destination: &Path) -> Result<Self, SnapshotError> {
        if destination.exists() {
            return Err(SnapshotError::DestinationExists);
        }
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        std::fs::create_dir_all(parent)?;
        let name = destination
            .file_name()
            .ok_or(SnapshotError::InvalidDestination)?
            .to_string_lossy();
        for _ in 0..128 {
            let sequence = STAGING_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let candidate = parent.join(format!(
                ".{name}.dtg-stage-{}-{sequence}",
                std::process::id()
            ));
            match std::fs::create_dir(&candidate) {
                Ok(()) => {
                    return Ok(Self {
                        path: candidate,
                        published: false,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        Err(SnapshotError::StagingNameExhausted)
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn publish(mut self, destination: &Path) -> Result<(), SnapshotError> {
        if destination.exists() {
            return Err(SnapshotError::DestinationExists);
        }
        std::fs::rename(&self.path, destination)?;
        self.published = true;
        let parent = destination
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(parent)
    }
}

impl Drop for StagedDirectory {
    fn drop(&mut self) {
        if !self.published {
            let _ignored = std::fs::remove_dir_all(&self.path);
        }
    }
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
    InvalidArchiveMagic,
    UnsupportedArchiveVersion { version: u16 },
    InvalidArchivePath,
    DuplicateArchivePath,
    ArchiveTooLarge,
    ArchiveFileDigestMismatch,
    ArchiveTrailingBytes,
    RaftLogStore(raft_logstore::RaftLogStoreError),
    StateMachine(String),
    ReplicaMetadataMismatch,
    RaftSnapshotPositionMismatch,
    IncomingRaftSnapshotMismatch,
    DestinationExists,
    InvalidDestination,
    StagingNameExhausted,
    InjectedFailure(SnapshotFailpoint),
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
            Self::InvalidArchiveMagic => formatter.write_str("invalid snapshot archive magic"),
            Self::UnsupportedArchiveVersion { version } => {
                write!(formatter, "unsupported snapshot archive version {version}")
            }
            Self::InvalidArchivePath => formatter.write_str("invalid snapshot archive path"),
            Self::DuplicateArchivePath => {
                formatter.write_str("snapshot archive contains a duplicate path")
            }
            Self::ArchiveTooLarge => formatter.write_str("snapshot archive exceeds codec limits"),
            Self::ArchiveFileDigestMismatch => {
                formatter.write_str("snapshot archive file digest mismatch")
            }
            Self::ArchiveTrailingBytes => {
                formatter.write_str("snapshot archive contains trailing bytes")
            }
            Self::RaftLogStore(error) => write!(formatter, "Raft snapshot WAL error: {error}"),
            Self::StateMachine(error) => write!(formatter, "snapshot state-machine error: {error}"),
            Self::ReplicaMetadataMismatch => {
                formatter.write_str("checkpoint Replica metadata differs from its manifest")
            }
            Self::RaftSnapshotPositionMismatch => formatter.write_str(
                "Raft WAL membership, term, or commit frontier differs from the snapshot manifest",
            ),
            Self::IncomingRaftSnapshotMismatch => formatter.write_str(
                "incoming Raft snapshot position, membership, or payload differs from its bundle",
            ),
            Self::DestinationExists => {
                formatter.write_str("snapshot publication destination already exists")
            }
            Self::InvalidDestination => formatter.write_str("invalid snapshot destination path"),
            Self::StagingNameExhausted => {
                formatter.write_str("could not allocate a snapshot staging directory")
            }
            Self::InjectedFailure(failpoint) => {
                write!(formatter, "injected snapshot failure at {failpoint:?}")
            }
        }
    }
}

impl Error for SnapshotError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Adapter(error) => Some(error),
            Self::RaftLogStore(error) => Some(error),
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

impl From<raft_logstore::RaftLogStoreError> for SnapshotError {
    fn from(error: raft_logstore::RaftLogStoreError) -> Self {
        Self::RaftLogStore(error)
    }
}
