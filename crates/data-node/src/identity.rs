use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use fs2::FileExt;

use crate::StorageError;

const IDENTITY_MAGIC: [u8; 4] = *b"DTNI";
const IDENTITY_VERSION: u16 = 1;
const IDENTITY_BODY_BYTES: usize = 24;
const IDENTITY_HEADER_BYTES: usize = 10;
const CHECKSUM_BYTES: usize = 4;
const IDENTITY_FILE: &str = "node.identity";
const LOCK_FILE: &str = ".node.lock";
const TEMP_IDENTITY_FILE: &str = ".node.identity.tmp";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeIdentity {
    cluster_id: [u8; 16],
    node_id: u64,
}

impl NodeIdentity {
    pub fn new(cluster_id: [u8; 16], node_id: u64) -> Result<Self, StorageError> {
        if cluster_id == [0; 16] {
            return Err(StorageError::ZeroClusterId);
        }
        if node_id == 0 {
            return Err(StorageError::ZeroNodeId);
        }
        Ok(Self {
            cluster_id,
            node_id,
        })
    }

    #[must_use]
    pub const fn cluster_id(&self) -> &[u8; 16] {
        &self.cluster_id
    }

    #[must_use]
    pub const fn node_id(&self) -> u64 {
        self.node_id
    }
}

pub struct NodeIdentityStore {
    identity: NodeIdentity,
    _lock: File,
    path: PathBuf,
}

impl NodeIdentityStore {
    pub fn open_or_create(
        data_directory: impl AsRef<Path>,
        expected: NodeIdentity,
    ) -> Result<Self, StorageError> {
        let data_directory = data_directory.as_ref();
        fs::create_dir_all(data_directory)?;
        let lock_path = data_directory.join(LOCK_FILE);
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        if let Err(error) = FileExt::try_lock_exclusive(&lock) {
            return if error.kind() == std::io::ErrorKind::WouldBlock {
                Err(StorageError::DirectoryLocked)
            } else {
                Err(error.into())
            };
        }

        let path = data_directory.join(IDENTITY_FILE);
        let actual = if path.exists() {
            decode_identity(&fs::read(&path)?)?
        } else {
            write_identity_atomically(data_directory, &path, &expected)?;
            expected.clone()
        };
        if actual.cluster_id != expected.cluster_id {
            return Err(StorageError::ClusterIdentityMismatch);
        }
        if actual.node_id != expected.node_id {
            return Err(StorageError::IdentityMismatch {
                expected: expected.node_id,
                actual: actual.node_id,
            });
        }
        Ok(Self {
            identity: actual,
            _lock: lock,
            path,
        })
    }

    #[must_use]
    pub const fn identity(&self) -> &NodeIdentity {
        &self.identity
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn encode_identity(identity: &NodeIdentity) -> Vec<u8> {
    let mut encoded =
        Vec::with_capacity(IDENTITY_HEADER_BYTES + IDENTITY_BODY_BYTES + CHECKSUM_BYTES);
    encoded.extend_from_slice(&IDENTITY_MAGIC);
    encoded.extend_from_slice(&IDENTITY_VERSION.to_be_bytes());
    encoded.extend_from_slice(&(IDENTITY_BODY_BYTES as u32).to_be_bytes());
    encoded.extend_from_slice(&identity.cluster_id);
    encoded.extend_from_slice(&identity.node_id.to_be_bytes());
    let checksum = crc32fast::hash(&encoded);
    encoded.extend_from_slice(&checksum.to_be_bytes());
    encoded
}

fn decode_identity(encoded: &[u8]) -> Result<NodeIdentity, StorageError> {
    let expected_length = IDENTITY_HEADER_BYTES + IDENTITY_BODY_BYTES + CHECKSUM_BYTES;
    if encoded.len() != expected_length {
        return Err(StorageError::InvalidIdentityLength);
    }
    if encoded[..4] != IDENTITY_MAGIC {
        return Err(StorageError::InvalidIdentityMagic);
    }
    let version = u16::from_be_bytes(encoded[4..6].try_into().expect("fixed version"));
    if version != IDENTITY_VERSION {
        return Err(StorageError::UnsupportedIdentityVersion { actual: version });
    }
    let body_length = u32::from_be_bytes(encoded[6..10].try_into().expect("fixed length"));
    if body_length as usize != IDENTITY_BODY_BYTES {
        return Err(StorageError::InvalidIdentityLength);
    }
    let checksum_offset = encoded.len() - CHECKSUM_BYTES;
    let expected_checksum = u32::from_be_bytes(
        encoded[checksum_offset..]
            .try_into()
            .expect("fixed checksum"),
    );
    if crc32fast::hash(&encoded[..checksum_offset]) != expected_checksum {
        return Err(StorageError::IdentityChecksumMismatch);
    }
    let cluster_id = encoded[10..26].try_into().expect("fixed cluster ID");
    let node_id = u64::from_be_bytes(encoded[26..34].try_into().expect("fixed node ID"));
    NodeIdentity::new(cluster_id, node_id)
}

fn write_identity_atomically(
    data_directory: &Path,
    destination: &Path,
    identity: &NodeIdentity,
) -> Result<(), StorageError> {
    let temporary = data_directory.join(TEMP_IDENTITY_FILE);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&encode_identity(identity))?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temporary, destination)?;
    File::open(data_directory)?.sync_all()?;
    Ok(())
}
