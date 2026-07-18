use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use crate::StorageError;

const RECORD_MAGIC: [u8; 4] = *b"DTRP";
const MANIFEST_VERSION: u16 = 2;
const RECORD_HEADER_BYTES: usize = 10;
const CHECKSUM_BYTES: usize = 4;
const MAX_MANIFEST_BYTES: usize = 32 * 1024 * 1024;
const MAX_REPLICAS: usize = 65_536;
const MAX_VOTERS: usize = 1_024;
const MAX_DIRECTORY_BYTES: usize = 240;
const MANIFEST_FILE: &str = "replicas.manifest.log";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicaRole {
    Learner,
    Voter,
}

impl ReplicaRole {
    const fn tag(self) -> u8 {
        match self {
            Self::Learner => 1,
            Self::Voter => 2,
        }
    }

    fn from_tag(tag: u8) -> Result<Self, StorageError> {
        match tag {
            1 => Ok(Self::Learner),
            2 => Ok(Self::Voter),
            actual => Err(StorageError::InvalidReplicaRole { actual }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicaEntry {
    graph_id: u64,
    shard_id: u32,
    placement_epoch: u64,
    voters: Vec<u64>,
    role: ReplicaRole,
    schema_version: u64,
    backend_generation: u64,
    relative_directory: String,
}

impl ReplicaEntry {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
        mut voters: Vec<u64>,
        role: ReplicaRole,
        schema_version: u64,
        backend_generation: u64,
        relative_directory: impl Into<String>,
    ) -> Result<Self, StorageError> {
        if graph_id == 0 || shard_id == 0 {
            return Err(StorageError::InvalidReplicaIdentity);
        }
        if placement_epoch == 0 {
            return Err(StorageError::InvalidReplicaEpoch);
        }
        if voters.is_empty() || voters.len() > MAX_VOTERS || voters.contains(&0) {
            return Err(StorageError::InvalidReplicaVoters);
        }
        voters.sort_unstable();
        if voters.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(StorageError::InvalidReplicaVoters);
        }
        if schema_version == 0 || backend_generation == 0 {
            return Err(StorageError::InvalidReplicaGeneration);
        }
        let relative_directory = relative_directory.into();
        validate_relative_directory(&relative_directory)?;
        Ok(Self {
            graph_id,
            shard_id,
            placement_epoch,
            voters,
            role,
            schema_version,
            backend_generation,
            relative_directory,
        })
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub const fn placement_epoch(&self) -> u64 {
        self.placement_epoch
    }

    #[must_use]
    pub fn voters(&self) -> &[u64] {
        &self.voters
    }

    #[must_use]
    pub const fn role(&self) -> ReplicaRole {
        self.role
    }

    #[must_use]
    pub const fn schema_version(&self) -> u64 {
        self.schema_version
    }

    #[must_use]
    pub const fn backend_generation(&self) -> u64 {
        self.backend_generation
    }

    #[must_use]
    pub fn relative_directory(&self) -> &str {
        &self.relative_directory
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReplicaManifest {
    replicas: BTreeMap<(u64, u32), ReplicaEntry>,
}

impl ReplicaManifest {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, entry: ReplicaEntry) -> Result<(), StorageError> {
        let key = (entry.graph_id, entry.shard_id);
        if let Some(existing) = self.replicas.get(&key) {
            return if existing == &entry {
                Ok(())
            } else {
                Err(StorageError::ReplicaIdentityConflict {
                    graph_id: entry.graph_id,
                    shard_id: entry.shard_id,
                })
            };
        }
        if self.replicas.len() >= MAX_REPLICAS {
            return Err(StorageError::TooManyReplicas);
        }
        if self
            .replicas
            .values()
            .any(|existing| existing.relative_directory == entry.relative_directory)
        {
            return Err(StorageError::ReplicaDirectoryConflict {
                directory: entry.relative_directory,
            });
        }
        self.replicas.insert(key, entry);
        Ok(())
    }

    pub fn replicas(&self) -> impl Iterator<Item = &ReplicaEntry> {
        self.replicas.values()
    }
}

pub struct ReplicaManifestStore {
    path: PathBuf,
    file: File,
    manifest: ReplicaManifest,
}

impl ReplicaManifestStore {
    pub fn open(data_directory: impl AsRef<Path>) -> Result<Self, StorageError> {
        let data_directory = data_directory.as_ref();
        fs::create_dir_all(data_directory)?;
        let path = data_directory.join(MANIFEST_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)?;
        let manifest = replay_and_repair_tail(&mut file)?;
        Ok(Self {
            path,
            file,
            manifest,
        })
    }

    #[must_use]
    pub const fn manifest(&self) -> &ReplicaManifest {
        &self.manifest
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn persist(&mut self, manifest: &ReplicaManifest) -> Result<(), StorageError> {
        let payload = encode_manifest(manifest)?;
        let payload_length =
            u32::try_from(payload.len()).map_err(|_| StorageError::ManifestTooLarge)?;
        let mut record = Vec::with_capacity(RECORD_HEADER_BYTES + payload.len() + CHECKSUM_BYTES);
        record.extend_from_slice(&RECORD_MAGIC);
        record.extend_from_slice(&MANIFEST_VERSION.to_be_bytes());
        record.extend_from_slice(&payload_length.to_be_bytes());
        record.extend_from_slice(&payload);
        let checksum = crc32fast::hash(&record);
        record.extend_from_slice(&checksum.to_be_bytes());
        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&record)?;
        self.file.sync_all()?;
        self.manifest = manifest.clone();
        Ok(())
    }
}

fn replay_and_repair_tail(file: &mut File) -> Result<ReplicaManifest, StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    let mut offset = 0_usize;
    let mut manifest = ReplicaManifest::new();
    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < RECORD_HEADER_BYTES {
            break;
        }
        if bytes[offset..offset + 4] != RECORD_MAGIC {
            return Err(StorageError::InvalidManifestMagic);
        }
        let version = u16::from_be_bytes(
            bytes[offset + 4..offset + 6]
                .try_into()
                .expect("fixed version"),
        );
        if version != MANIFEST_VERSION {
            return Err(StorageError::UnsupportedManifestVersion { actual: version });
        }
        let payload_length = u32::from_be_bytes(
            bytes[offset + 6..offset + 10]
                .try_into()
                .expect("fixed length"),
        ) as usize;
        if payload_length > MAX_MANIFEST_BYTES {
            return Err(StorageError::ManifestTooLarge);
        }
        let record_length = RECORD_HEADER_BYTES
            .checked_add(payload_length)
            .and_then(|length| length.checked_add(CHECKSUM_BYTES))
            .ok_or(StorageError::ManifestTooLarge)?;
        if remaining < record_length {
            break;
        }
        let checksum_offset = offset + record_length - CHECKSUM_BYTES;
        let expected_checksum = u32::from_be_bytes(
            bytes[checksum_offset..checksum_offset + CHECKSUM_BYTES]
                .try_into()
                .expect("fixed checksum"),
        );
        if crc32fast::hash(&bytes[offset..checksum_offset]) != expected_checksum {
            return Err(StorageError::ManifestChecksumMismatch);
        }
        manifest = decode_manifest(&bytes[offset + RECORD_HEADER_BYTES..checksum_offset])?;
        offset += record_length;
    }
    if offset < bytes.len() {
        file.set_len(offset as u64)?;
        file.sync_all()?;
    }
    file.seek(SeekFrom::End(0))?;
    Ok(manifest)
}

fn encode_manifest(manifest: &ReplicaManifest) -> Result<Vec<u8>, StorageError> {
    let count =
        u32::try_from(manifest.replicas.len()).map_err(|_| StorageError::TooManyReplicas)?;
    let mut encoded = Vec::new();
    encoded.extend_from_slice(&count.to_be_bytes());
    for entry in manifest.replicas.values() {
        encoded.extend_from_slice(&entry.graph_id.to_be_bytes());
        encoded.extend_from_slice(&entry.shard_id.to_be_bytes());
        encoded.extend_from_slice(&entry.placement_epoch.to_be_bytes());
        let voter_count =
            u16::try_from(entry.voters.len()).map_err(|_| StorageError::InvalidReplicaVoters)?;
        encoded.extend_from_slice(&voter_count.to_be_bytes());
        for voter in &entry.voters {
            encoded.extend_from_slice(&voter.to_be_bytes());
        }
        encoded.push(entry.role.tag());
        encoded.extend_from_slice(&entry.schema_version.to_be_bytes());
        encoded.extend_from_slice(&entry.backend_generation.to_be_bytes());
        write_string(&mut encoded, &entry.relative_directory)?;
    }
    if encoded.len() > MAX_MANIFEST_BYTES {
        return Err(StorageError::ManifestTooLarge);
    }
    Ok(encoded)
}

fn decode_manifest(encoded: &[u8]) -> Result<ReplicaManifest, StorageError> {
    let mut decoder = Decoder::new(encoded);
    let count = decoder.read_u32()? as usize;
    if count > MAX_REPLICAS {
        return Err(StorageError::TooManyReplicas);
    }
    let mut manifest = ReplicaManifest::new();
    let mut directories = BTreeSet::new();
    for _ in 0..count {
        let graph_id = decoder.read_u64()?;
        let shard_id = decoder.read_u32()?;
        let placement_epoch = decoder.read_u64()?;
        let voter_count = usize::from(decoder.read_u16()?);
        if voter_count == 0 || voter_count > MAX_VOTERS {
            return Err(StorageError::InvalidReplicaVoters);
        }
        let mut voters = Vec::with_capacity(voter_count);
        for _ in 0..voter_count {
            voters.push(decoder.read_u64()?);
        }
        let role = ReplicaRole::from_tag(decoder.read_u8()?)?;
        let schema_version = decoder.read_u64()?;
        let backend_generation = decoder.read_u64()?;
        let directory = decoder.read_string()?;
        if !directories.insert(directory.clone()) {
            return Err(StorageError::ReplicaDirectoryConflict { directory });
        }
        manifest.insert(ReplicaEntry::new(
            graph_id,
            shard_id,
            placement_epoch,
            voters,
            role,
            schema_version,
            backend_generation,
            directory,
        )?)?;
    }
    if !decoder.is_finished() {
        return Err(StorageError::ManifestTrailingBytes);
    }
    Ok(manifest)
}

fn validate_relative_directory(directory: &str) -> Result<(), StorageError> {
    if directory.is_empty()
        || directory.len() > MAX_DIRECTORY_BYTES
        || directory.chars().any(char::is_control)
    {
        return Err(StorageError::InvalidReplicaDirectory);
    }
    let path = Path::new(directory);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
                    | Component::CurDir
            )
        })
    {
        return Err(StorageError::InvalidReplicaDirectory);
    }
    Ok(())
}

fn write_string(encoded: &mut Vec<u8>, value: &str) -> Result<(), StorageError> {
    let length = u16::try_from(value.len()).map_err(|_| StorageError::StringTooLong)?;
    encoded.extend_from_slice(&length.to_be_bytes());
    encoded.extend_from_slice(value.as_bytes());
    Ok(())
}

struct Decoder<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Decoder<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn read_exact(&mut self, length: usize) -> Result<&'a [u8], StorageError> {
        let end = self
            .offset
            .checked_add(length)
            .ok_or(StorageError::InvalidManifestLength)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(StorageError::InvalidManifestLength)?;
        self.offset = end;
        Ok(value)
    }

    fn read_u8(&mut self) -> Result<u8, StorageError> {
        Ok(self.read_exact(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32, StorageError> {
        Ok(u32::from_be_bytes(
            self.read_exact(4)?.try_into().expect("fixed u32"),
        ))
    }

    fn read_u16(&mut self) -> Result<u16, StorageError> {
        Ok(u16::from_be_bytes(
            self.read_exact(2)?.try_into().expect("fixed u16"),
        ))
    }

    fn read_u64(&mut self) -> Result<u64, StorageError> {
        Ok(u64::from_be_bytes(
            self.read_exact(8)?.try_into().expect("fixed u64"),
        ))
    }

    fn read_string(&mut self) -> Result<String, StorageError> {
        let length = usize::from(u16::from_be_bytes(
            self.read_exact(2)?.try_into().expect("fixed string length"),
        ));
        if length > MAX_DIRECTORY_BYTES {
            return Err(StorageError::StringTooLong);
        }
        String::from_utf8(self.read_exact(length)?.to_vec()).map_err(|_| StorageError::InvalidUtf8)
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}
