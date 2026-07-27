use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::{ConfigError, NodeConfig, NodeIdentity, StorageError, TlsFiles, TransportSecurity};

const FILE_CONFIG_VERSION: u32 = 1;
const MAX_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_ACTOR_QUEUE_CAPACITY: usize = 65_536;
const MIN_SHUTDOWN_GRACE_MS: u64 = 100;
const MAX_SHUTDOWN_GRACE_MS: u64 = 300_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupBackend {
    Rocksdb,
    Postgresql,
    Neo4j,
}

impl StartupBackend {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Rocksdb => "rocksdb",
            Self::Postgresql => "postgresql",
            Self::Neo4j => "neo4j",
        }
    }

    #[must_use]
    pub const fn provider(self) -> &'static str {
        match self {
            Self::Rocksdb => "rocksdb",
            Self::Postgresql | Self::Neo4j => "sidecar",
        }
    }
}

#[derive(Clone, Debug)]
pub struct DataNodeRuntimeConfig {
    node: NodeConfig,
    backend: StartupBackend,
    actor_queue_capacity: usize,
    shutdown_grace: Duration,
    raft_listen_address: Option<SocketAddr>,
    raft_peers: BTreeMap<u64, SocketAddr>,
}

impl DataNodeRuntimeConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, FileConfigError> {
        let bytes = std::fs::read(path).map_err(FileConfigError::from_io)?;
        if bytes.is_empty() || bytes.len() > MAX_CONFIG_BYTES {
            return Err(FileConfigError::InvalidFileSize {
                actual: bytes.len(),
            });
        }
        let raw: RawDataNodeConfig = serde_json::from_slice(&bytes)
            .map_err(|error| FileConfigError::Json(error.to_string()))?;
        raw.validate()
    }

    #[must_use]
    pub const fn node(&self) -> &NodeConfig {
        &self.node
    }

    #[must_use]
    pub const fn backend(&self) -> StartupBackend {
        self.backend
    }

    #[must_use]
    pub const fn actor_queue_capacity(&self) -> usize {
        self.actor_queue_capacity
    }

    #[must_use]
    pub const fn shutdown_grace(&self) -> Duration {
        self.shutdown_grace
    }

    #[must_use]
    pub const fn raft_listen_address(&self) -> Option<SocketAddr> {
        self.raft_listen_address
    }

    #[must_use]
    pub const fn raft_peers(&self) -> &BTreeMap<u64, SocketAddr> {
        &self.raft_peers
    }

    #[must_use]
    pub fn into_node(self) -> NodeConfig {
        self.node
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDataNodeConfig {
    version: u32,
    cluster_id: String,
    node_id: u64,
    backend: RawStartupBackend,
    listen_address: SocketAddr,
    advertise_address: SocketAddr,
    data_directory: PathBuf,
    meta_seeds: Vec<SocketAddr>,
    security: RawSecurity,
    actor_queue_capacity: usize,
    shutdown_grace_ms: u64,
    #[serde(default)]
    raft_listen_address: Option<SocketAddr>,
    #[serde(default)]
    raft_peers: BTreeMap<u64, SocketAddr>,
}

impl RawDataNodeConfig {
    fn validate(self) -> Result<DataNodeRuntimeConfig, FileConfigError> {
        if self.version != FILE_CONFIG_VERSION {
            return Err(FileConfigError::UnsupportedVersion {
                actual: self.version,
            });
        }
        if self.actor_queue_capacity == 0 || self.actor_queue_capacity > MAX_ACTOR_QUEUE_CAPACITY {
            return Err(FileConfigError::InvalidActorQueueCapacity {
                actual: self.actor_queue_capacity,
            });
        }
        if !(MIN_SHUTDOWN_GRACE_MS..=MAX_SHUTDOWN_GRACE_MS).contains(&self.shutdown_grace_ms) {
            return Err(FileConfigError::InvalidShutdownGrace {
                actual_ms: self.shutdown_grace_ms,
            });
        }
        if self.raft_listen_address.is_none() && !self.raft_peers.is_empty() {
            return Err(FileConfigError::MissingRaftListenAddress);
        }
        if let Some(raft_listen_address) = self.raft_listen_address
            && (raft_listen_address.port() == 0
                || !raft_listen_address.ip().is_loopback()
                || raft_listen_address == self.listen_address)
        {
            return Err(FileConfigError::InvalidRaftListenAddress);
        }
        let mut peer_addresses = std::collections::BTreeSet::new();
        for (&peer_id, &address) in &self.raft_peers {
            if peer_id == 0
                || peer_id == self.node_id
                || address.port() == 0
                || !address.ip().is_loopback()
                || !peer_addresses.insert(address)
            {
                return Err(FileConfigError::InvalidRaftPeer { node_id: peer_id });
            }
        }
        let cluster_id = decode_cluster_id(&self.cluster_id)?;
        let identity = NodeIdentity::new(cluster_id, self.node_id)?;
        let transport_security = match self.security {
            RawSecurity::LoopbackPlaintext => TransportSecurity::LoopbackPlaintext,
            RawSecurity::MutualTls {
                ca_certificate,
                node_certificate,
                private_key,
            } => TransportSecurity::MutualTls(TlsFiles::new(
                ca_certificate,
                node_certificate,
                private_key,
            )?),
        };
        let node = NodeConfig::new(
            identity,
            self.listen_address,
            self.advertise_address,
            self.data_directory,
            self.meta_seeds,
            transport_security,
            Default::default(),
        )?;
        let backend = self.backend.into_startup_backend()?;
        Ok(DataNodeRuntimeConfig {
            node,
            backend,
            actor_queue_capacity: self.actor_queue_capacity,
            shutdown_grace: Duration::from_millis(self.shutdown_grace_ms),
            raft_listen_address: self.raft_listen_address,
            raft_peers: self.raft_peers,
        })
    }
}

#[derive(Deserialize)]
#[serde(transparent)]
struct RawStartupBackend(String);

impl RawStartupBackend {
    fn into_startup_backend(self) -> Result<StartupBackend, FileConfigError> {
        match self.0.as_str() {
            "rocksdb" => Ok(StartupBackend::Rocksdb),
            "postgresql" => Ok(StartupBackend::Postgresql),
            "neo4j" => Ok(StartupBackend::Neo4j),
            _ => Err(FileConfigError::UnsupportedBackend { backend: self.0 }),
        }
    }
}

#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum RawSecurity {
    LoopbackPlaintext,
    MutualTls {
        ca_certificate: PathBuf,
        node_certificate: PathBuf,
        private_key: PathBuf,
    },
}

fn decode_cluster_id(encoded: &str) -> Result<[u8; 16], FileConfigError> {
    if encoded.len() != 32 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(FileConfigError::InvalidClusterId);
    }
    let mut cluster_id = [0_u8; 16];
    for (index, output) in cluster_id.iter_mut().enumerate() {
        let offset = index * 2;
        *output = u8::from_str_radix(&encoded[offset..offset + 2], 16)
            .map_err(|_| FileConfigError::InvalidClusterId)?;
    }
    if cluster_id == [0; 16] {
        return Err(FileConfigError::InvalidClusterId);
    }
    Ok(cluster_id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FileConfigError {
    Io(String),
    Json(String),
    InvalidFileSize { actual: usize },
    UnsupportedVersion { actual: u32 },
    UnsupportedBackend { backend: String },
    InvalidClusterId,
    InvalidActorQueueCapacity { actual: usize },
    InvalidShutdownGrace { actual_ms: u64 },
    MissingRaftListenAddress,
    InvalidRaftListenAddress,
    InvalidRaftPeer { node_id: u64 },
    NodeConfig(String),
    Identity(String),
}

impl FileConfigError {
    fn from_io(error: std::io::Error) -> Self {
        Self::Io(error.to_string())
    }
}

impl Display for FileConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "Data node config I/O error: {message}"),
            Self::Json(message) => write!(formatter, "Data node config JSON error: {message}"),
            Self::InvalidFileSize { actual } => {
                write!(formatter, "Data node config has invalid size {actual}")
            }
            Self::UnsupportedVersion { actual } => {
                write!(formatter, "unsupported Data node config version {actual}")
            }
            Self::UnsupportedBackend { backend } => {
                write!(
                    formatter,
                    "unsupported Data node logical backend {backend:?}"
                )
            }
            Self::InvalidClusterId => {
                formatter.write_str("cluster_id must be a non-zero 32-digit hexadecimal value")
            }
            Self::InvalidActorQueueCapacity { actual } => {
                write!(formatter, "invalid actor_queue_capacity {actual}")
            }
            Self::InvalidShutdownGrace { actual_ms } => {
                write!(formatter, "invalid shutdown_grace_ms {actual_ms}")
            }
            Self::MissingRaftListenAddress => formatter
                .write_str("raft_listen_address is required when raft_peers are configured"),
            Self::InvalidRaftListenAddress => formatter.write_str(
                "raft_listen_address must be a distinct loopback address with a non-zero port",
            ),
            Self::InvalidRaftPeer { node_id } => {
                write!(formatter, "invalid Raft peer address for node {node_id}")
            }
            Self::NodeConfig(message) => write!(formatter, "invalid node config: {message}"),
            Self::Identity(message) => write!(formatter, "invalid node identity: {message}"),
        }
    }
}

impl Error for FileConfigError {}

impl From<ConfigError> for FileConfigError {
    fn from(error: ConfigError) -> Self {
        Self::NodeConfig(error.to_string())
    }
}

impl From<StorageError> for FileConfigError {
    fn from(error: StorageError) -> Self {
        Self::Identity(error.to_string())
    }
}
