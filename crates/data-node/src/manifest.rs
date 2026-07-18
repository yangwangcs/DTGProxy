use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

use crate::StorageError;

const RECORD_MAGIC: [u8; 4] = *b"DTRP";
const LEGACY_MANIFEST_VERSION: u16 = 2;
const SNAPSHOT_MANIFEST_VERSION: u16 = 3;
const MANIFEST_VERSION: u16 = 4;
const RECORD_HEADER_BYTES: usize = 10;
const CHECKSUM_BYTES: usize = 4;
const MAX_MANIFEST_BYTES: usize = 32 * 1024 * 1024;
const MAX_REPLICAS: usize = 65_536;
const MAX_VOTERS: usize = 1_024;
const MAX_DIRECTORY_BYTES: usize = 240;
const MAX_BACKEND_NAME_BYTES: usize = 64;
const MAX_BACKEND_INSTANCE_BYTES: usize = 240;
const MAX_BACKEND_FIELD_BYTES: usize = 4_096;
const MAX_BACKEND_FIELDS: usize = 128;
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
pub struct BackendProfile {
    provider: String,
    instance_id: String,
    public_parameters: BTreeMap<String, String>,
    credential_refs: BTreeMap<String, String>,
    digest: [u8; 32],
}

impl BackendProfile {
    pub fn new(
        provider: impl Into<String>,
        instance_id: impl Into<String>,
        public_parameters: BTreeMap<String, String>,
        credential_refs: BTreeMap<String, String>,
    ) -> Result<Self, StorageError> {
        let provider = provider.into();
        let instance_id = instance_id.into();
        validate_backend_name(&provider)?;
        validate_backend_instance(&instance_id)?;
        validate_backend_fields(&public_parameters, true)?;
        validate_backend_fields(&credential_refs, false)?;
        let digest = backend_profile_digest(
            &provider,
            &instance_id,
            &public_parameters,
            &credential_refs,
        );
        Ok(Self {
            provider,
            instance_id,
            public_parameters,
            credential_refs,
            digest,
        })
    }

    #[must_use]
    pub fn provider(&self) -> &str {
        &self.provider
    }

    #[must_use]
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    #[must_use]
    pub const fn public_parameters(&self) -> &BTreeMap<String, String> {
        &self.public_parameters
    }

    #[must_use]
    pub const fn credential_refs(&self) -> &BTreeMap<String, String> {
        &self.credential_refs
    }

    #[must_use]
    pub const fn digest(&self) -> [u8; 32] {
        self.digest
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BackendSlotState {
    Active {
        generation: u64,
        profile: BackendProfile,
    },
    DualApplying {
        source_generation: u64,
        source: BackendProfile,
        target_generation: u64,
        target: BackendProfile,
        fence_index: u64,
        synchronized_index: u64,
    },
}

impl BackendSlotState {
    pub fn active(generation: u64, profile: BackendProfile) -> Result<Self, StorageError> {
        if generation == 0 {
            return Err(StorageError::InvalidReplicaGeneration);
        }
        Ok(Self::Active {
            generation,
            profile,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn dual_applying(
        source_generation: u64,
        source: BackendProfile,
        target_generation: u64,
        target: BackendProfile,
        fence_index: u64,
        synchronized_index: u64,
    ) -> Result<Self, StorageError> {
        if source_generation == 0
            || target_generation != source_generation.checked_add(1).unwrap_or(0)
            || synchronized_index != fence_index
        {
            return Err(StorageError::InvalidBackendTransition);
        }
        Ok(Self::DualApplying {
            source_generation,
            source,
            target_generation,
            target,
            fence_index,
            synchronized_index,
        })
    }

    #[must_use]
    pub const fn active_generation(&self) -> u64 {
        match self {
            Self::Active { generation, .. } => *generation,
            Self::DualApplying {
                source_generation, ..
            } => *source_generation,
        }
    }

    #[must_use]
    pub const fn active_profile(&self) -> &BackendProfile {
        match self {
            Self::Active { profile, .. } => profile,
            Self::DualApplying { source, .. } => source,
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
    backend_slot: BackendSlotState,
    snapshot_index: u64,
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
        let default_profile = BackendProfile::new(
            "rocksdb",
            format!("graph-{graph_id}-shard-{shard_id}-generation-{backend_generation}"),
            BTreeMap::from([("path".to_owned(), "adapter".to_owned())]),
            BTreeMap::new(),
        )?;
        Self::new_with_backend(
            graph_id,
            shard_id,
            placement_epoch,
            voters,
            role,
            schema_version,
            BackendSlotState::active(backend_generation, default_profile)?,
            relative_directory,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_backend(
        graph_id: u64,
        shard_id: u32,
        placement_epoch: u64,
        mut voters: Vec<u64>,
        role: ReplicaRole,
        schema_version: u64,
        backend_slot: BackendSlotState,
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
        let backend_generation = backend_slot.active_generation();
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
            backend_slot,
            snapshot_index: 0,
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
    pub const fn backend_slot(&self) -> &BackendSlotState {
        &self.backend_slot
    }

    #[must_use]
    pub const fn snapshot_index(&self) -> u64 {
        self.snapshot_index
    }

    #[must_use]
    pub fn relative_directory(&self) -> &str {
        &self.relative_directory
    }

    pub(crate) fn with_snapshot_index(mut self, snapshot_index: u64) -> Result<Self, StorageError> {
        if self.role != ReplicaRole::Learner || snapshot_index < self.snapshot_index {
            return Err(StorageError::InvalidReplicaSnapshotIndex);
        }
        self.snapshot_index = snapshot_index;
        Ok(self)
    }

    pub(crate) fn activated(
        mut self,
        target_epoch: u64,
        mut voters: Vec<u64>,
    ) -> Result<Self, StorageError> {
        voters.sort_unstable();
        if target_epoch != self.placement_epoch.checked_add(1).unwrap_or(0)
            || voters.is_empty()
            || voters.contains(&0)
            || voters.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(StorageError::InvalidReplicaEpoch);
        }
        self.placement_epoch = target_epoch;
        self.voters = voters;
        self.role = ReplicaRole::Voter;
        Ok(self)
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

    pub(crate) fn get(&self, graph_id: u64, shard_id: u32) -> Option<&ReplicaEntry> {
        self.replicas.get(&(graph_id, shard_id))
    }

    pub(crate) fn replace(&mut self, entry: ReplicaEntry) -> Result<(), StorageError> {
        let key = (entry.graph_id, entry.shard_id);
        let existing = self
            .replicas
            .get(&key)
            .ok_or(StorageError::InvalidReplicaIdentity)?;
        if existing.graph_id != entry.graph_id
            || existing.shard_id != entry.shard_id
            || existing.placement_epoch != entry.placement_epoch
            || existing.voters != entry.voters
            || existing.role != entry.role
            || existing.schema_version != entry.schema_version
            || existing.backend_generation != entry.backend_generation
            || existing.backend_slot != entry.backend_slot
            || existing.relative_directory != entry.relative_directory
        {
            return Err(StorageError::ReplicaIdentityConflict {
                graph_id: entry.graph_id,
                shard_id: entry.shard_id,
            });
        }
        self.replicas.insert(key, entry);
        Ok(())
    }

    pub(crate) fn remove(&mut self, graph_id: u64, shard_id: u32) -> Option<ReplicaEntry> {
        self.replicas.remove(&(graph_id, shard_id))
    }

    pub(crate) fn activate(&mut self, entry: ReplicaEntry) -> Result<(), StorageError> {
        let key = (entry.graph_id, entry.shard_id);
        let current = self
            .replicas
            .get(&key)
            .ok_or(StorageError::InvalidReplicaIdentity)?;
        if current == &entry {
            return Ok(());
        }
        if entry.placement_epoch != current.placement_epoch.checked_add(1).unwrap_or(0)
            || entry.role != ReplicaRole::Voter
            || entry.schema_version != current.schema_version
            || entry.backend_generation != current.backend_generation
            || entry.backend_slot != current.backend_slot
            || entry.relative_directory != current.relative_directory
        {
            return Err(StorageError::ReplicaIdentityConflict {
                graph_id: entry.graph_id,
                shard_id: entry.shard_id,
            });
        }
        self.replicas.insert(key, entry);
        Ok(())
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
        if !matches!(
            version,
            LEGACY_MANIFEST_VERSION | SNAPSHOT_MANIFEST_VERSION | MANIFEST_VERSION
        ) {
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
        manifest = decode_manifest(
            &bytes[offset + RECORD_HEADER_BYTES..checksum_offset],
            version,
        )?;
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
        encoded.extend_from_slice(&entry.snapshot_index.to_be_bytes());
        write_string(&mut encoded, &entry.relative_directory)?;
        encode_backend_slot(&mut encoded, &entry.backend_slot)?;
    }
    if encoded.len() > MAX_MANIFEST_BYTES {
        return Err(StorageError::ManifestTooLarge);
    }
    Ok(encoded)
}

fn decode_manifest(encoded: &[u8], version: u16) -> Result<ReplicaManifest, StorageError> {
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
        let snapshot_index = if version >= SNAPSHOT_MANIFEST_VERSION {
            decoder.read_u64()?
        } else {
            0
        };
        let directory = decoder.read_string()?;
        if !directories.insert(directory.clone()) {
            return Err(StorageError::ReplicaDirectoryConflict { directory });
        }
        let backend_slot = if version >= MANIFEST_VERSION {
            let decoded = decode_backend_slot(&mut decoder)?;
            if decoded.active_generation() != backend_generation {
                return Err(StorageError::InvalidBackendTransition);
            }
            decoded
        } else {
            let profile = BackendProfile::new(
                "rocksdb",
                format!("graph-{graph_id}-shard-{shard_id}-generation-{backend_generation}"),
                BTreeMap::from([("path".to_owned(), "adapter".to_owned())]),
                BTreeMap::new(),
            )?;
            BackendSlotState::active(backend_generation, profile)?
        };
        let entry = ReplicaEntry::new_with_backend(
            graph_id,
            shard_id,
            placement_epoch,
            voters,
            role,
            schema_version,
            backend_slot,
            directory,
        )?;
        let entry = if snapshot_index == 0 {
            entry
        } else {
            entry.with_snapshot_index(snapshot_index)?
        };
        manifest.insert(entry)?;
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

fn validate_backend_name(value: &str) -> Result<(), StorageError> {
    let valid = (1..=MAX_BACKEND_NAME_BYTES).contains(&value.len())
        && value.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && value.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"._-".contains(&byte)
        });
    if valid {
        Ok(())
    } else {
        Err(StorageError::InvalidBackendProfile)
    }
}

fn validate_backend_instance(value: &str) -> Result<(), StorageError> {
    if value.is_empty()
        || value.len() > MAX_BACKEND_INSTANCE_BYTES
        || value.chars().any(char::is_control)
    {
        Err(StorageError::InvalidBackendProfile)
    } else {
        Ok(())
    }
}

fn validate_backend_fields(
    fields: &BTreeMap<String, String>,
    public: bool,
) -> Result<(), StorageError> {
    if fields.len() > MAX_BACKEND_FIELDS {
        return Err(StorageError::InvalidBackendProfile);
    }
    for (name, value) in fields {
        validate_backend_name(name)?;
        if value.is_empty()
            || value.len() > MAX_BACKEND_FIELD_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(StorageError::InvalidBackendProfile);
        }
        if public && is_secret_parameter(name) {
            return Err(StorageError::EmbeddedBackendSecret { name: name.clone() });
        }
    }
    Ok(())
}

fn is_secret_parameter(name: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    [
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "private_key",
    ]
    .iter()
    .any(|secret| lower == *secret || lower.ends_with(&format!("_{secret}")))
}

fn backend_profile_digest(
    provider: &str,
    instance_id: &str,
    parameters: &BTreeMap<String, String>,
    credential_refs: &BTreeMap<String, String>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    for value in [provider, instance_id] {
        hasher.update(&(value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    for fields in [parameters, credential_refs] {
        hasher.update(&(fields.len() as u64).to_be_bytes());
        for (name, value) in fields {
            hasher.update(&(name.len() as u64).to_be_bytes());
            hasher.update(name.as_bytes());
            hasher.update(&(value.len() as u64).to_be_bytes());
            hasher.update(value.as_bytes());
        }
    }
    *hasher.finalize().as_bytes()
}

fn encode_backend_slot(encoded: &mut Vec<u8>, slot: &BackendSlotState) -> Result<(), StorageError> {
    match slot {
        BackendSlotState::Active {
            generation,
            profile,
        } => {
            encoded.push(1);
            encoded.extend_from_slice(&generation.to_be_bytes());
            encode_backend_profile(encoded, profile)?;
        }
        BackendSlotState::DualApplying {
            source_generation,
            source,
            target_generation,
            target,
            fence_index,
            synchronized_index,
        } => {
            encoded.push(2);
            encoded.extend_from_slice(&source_generation.to_be_bytes());
            encode_backend_profile(encoded, source)?;
            encoded.extend_from_slice(&target_generation.to_be_bytes());
            encode_backend_profile(encoded, target)?;
            encoded.extend_from_slice(&fence_index.to_be_bytes());
            encoded.extend_from_slice(&synchronized_index.to_be_bytes());
        }
    }
    Ok(())
}

fn encode_backend_profile(
    encoded: &mut Vec<u8>,
    profile: &BackendProfile,
) -> Result<(), StorageError> {
    write_bounded_string(encoded, &profile.provider, MAX_BACKEND_NAME_BYTES)?;
    write_bounded_string(encoded, &profile.instance_id, MAX_BACKEND_INSTANCE_BYTES)?;
    encode_backend_fields(encoded, &profile.public_parameters)?;
    encode_backend_fields(encoded, &profile.credential_refs)?;
    encoded.extend_from_slice(&profile.digest);
    Ok(())
}

fn encode_backend_fields(
    encoded: &mut Vec<u8>,
    fields: &BTreeMap<String, String>,
) -> Result<(), StorageError> {
    let count = u16::try_from(fields.len()).map_err(|_| StorageError::InvalidBackendProfile)?;
    encoded.extend_from_slice(&count.to_be_bytes());
    for (name, value) in fields {
        write_bounded_string(encoded, name, MAX_BACKEND_NAME_BYTES)?;
        write_bounded_string(encoded, value, MAX_BACKEND_FIELD_BYTES)?;
    }
    Ok(())
}

fn decode_backend_slot(decoder: &mut Decoder<'_>) -> Result<BackendSlotState, StorageError> {
    match decoder.read_u8()? {
        1 => BackendSlotState::active(decoder.read_u64()?, decode_backend_profile(decoder)?),
        2 => {
            let source_generation = decoder.read_u64()?;
            let source = decode_backend_profile(decoder)?;
            let target_generation = decoder.read_u64()?;
            let target = decode_backend_profile(decoder)?;
            let fence_index = decoder.read_u64()?;
            let synchronized_index = decoder.read_u64()?;
            BackendSlotState::dual_applying(
                source_generation,
                source,
                target_generation,
                target,
                fence_index,
                synchronized_index,
            )
        }
        _ => Err(StorageError::InvalidBackendTransition),
    }
}

fn decode_backend_profile(decoder: &mut Decoder<'_>) -> Result<BackendProfile, StorageError> {
    let provider = decoder.read_bounded_string(MAX_BACKEND_NAME_BYTES)?;
    let instance_id = decoder.read_bounded_string(MAX_BACKEND_INSTANCE_BYTES)?;
    let parameters = decode_backend_fields(decoder)?;
    let credential_refs = decode_backend_fields(decoder)?;
    let recorded_digest: [u8; 32] = decoder
        .read_exact(32)?
        .try_into()
        .expect("fixed backend digest");
    let profile = BackendProfile::new(provider, instance_id, parameters, credential_refs)?;
    if profile.digest != recorded_digest {
        return Err(StorageError::BackendProfileDigestMismatch);
    }
    Ok(profile)
}

fn decode_backend_fields(
    decoder: &mut Decoder<'_>,
) -> Result<BTreeMap<String, String>, StorageError> {
    let count = usize::from(decoder.read_u16()?);
    if count > MAX_BACKEND_FIELDS {
        return Err(StorageError::InvalidBackendProfile);
    }
    let mut fields = BTreeMap::new();
    for _ in 0..count {
        let name = decoder.read_bounded_string(MAX_BACKEND_NAME_BYTES)?;
        let value = decoder.read_bounded_string(MAX_BACKEND_FIELD_BYTES)?;
        if fields.insert(name, value).is_some() {
            return Err(StorageError::InvalidBackendProfile);
        }
    }
    Ok(fields)
}

fn write_string(encoded: &mut Vec<u8>, value: &str) -> Result<(), StorageError> {
    write_bounded_string(encoded, value, MAX_DIRECTORY_BYTES)
}

fn write_bounded_string(
    encoded: &mut Vec<u8>,
    value: &str,
    maximum: usize,
) -> Result<(), StorageError> {
    if value.len() > maximum {
        return Err(StorageError::StringTooLong);
    }
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
        self.read_bounded_string(MAX_DIRECTORY_BYTES)
    }

    fn read_bounded_string(&mut self, maximum: usize) -> Result<String, StorageError> {
        let length = usize::from(u16::from_be_bytes(
            self.read_exact(2)?.try_into().expect("fixed string length"),
        ));
        if length > maximum {
            return Err(StorageError::StringTooLong);
        }
        String::from_utf8(self.read_exact(length)?.to_vec()).map_err(|_| StorageError::InvalidUtf8)
    }

    const fn is_finished(&self) -> bool {
        self.offset == self.bytes.len()
    }
}
