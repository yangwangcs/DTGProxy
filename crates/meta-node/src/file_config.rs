use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

const FILE_CONFIG_VERSION: u32 = 1;
const MAX_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_VOTERS: usize = 9;
const MAX_RESERVATION_SIZE: u32 = 1_048_576;
const MAX_FUTURE_DRIFT_MS: u64 = 300_000;
const MIN_SHUTDOWN_GRACE_MS: u64 = 100;
const MAX_SHUTDOWN_GRACE_MS: u64 = 300_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MetaTlsFiles {
    ca_certificate: PathBuf,
    node_certificate: PathBuf,
    private_key: PathBuf,
}

impl MetaTlsFiles {
    fn new(
        ca_certificate: PathBuf,
        node_certificate: PathBuf,
        private_key: PathBuf,
    ) -> Result<Self, MetaConfigError> {
        if [&ca_certificate, &node_certificate, &private_key]
            .iter()
            .any(|path| !path.is_absolute())
        {
            return Err(MetaConfigError::SecretPathNotAbsolute);
        }
        if BTreeSet::from([
            ca_certificate.clone(),
            node_certificate.clone(),
            private_key.clone(),
        ])
        .len()
            != 3
        {
            return Err(MetaConfigError::DuplicateSecretPath);
        }
        Ok(Self {
            ca_certificate,
            node_certificate,
            private_key,
        })
    }

    #[must_use]
    pub fn ca_certificate(&self) -> &Path {
        &self.ca_certificate
    }

    #[must_use]
    pub fn node_certificate(&self) -> &Path {
        &self.node_certificate
    }

    #[must_use]
    pub fn private_key(&self) -> &Path {
        &self.private_key
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaTransportSecurity {
    MutualTls(MetaTlsFiles),
    LoopbackPlaintext,
}

#[derive(Clone, Debug)]
pub struct MetaNodeRuntimeConfig {
    cluster_id: [u8; 16],
    node_id: u64,
    voters: Vec<u64>,
    listen_address: SocketAddr,
    advertise_address: SocketAddr,
    raft_listen_address: SocketAddr,
    peer_addresses: BTreeMap<u64, SocketAddr>,
    data_directory: PathBuf,
    transport_security: MetaTransportSecurity,
    timestamp_reservation_size: u32,
    maximum_future_drift_micros: i64,
    shutdown_grace: Duration,
}

impl MetaNodeRuntimeConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, MetaConfigError> {
        let bytes = std::fs::read(path).map_err(|error| MetaConfigError::Io(error.to_string()))?;
        if bytes.is_empty() || bytes.len() > MAX_CONFIG_BYTES {
            return Err(MetaConfigError::InvalidFileSize {
                actual: bytes.len(),
            });
        }
        let raw: RawMetaConfig = serde_json::from_slice(&bytes)
            .map_err(|error| MetaConfigError::Json(error.to_string()))?;
        raw.validate()
    }

    #[must_use]
    pub const fn cluster_id(&self) -> &[u8; 16] {
        &self.cluster_id
    }

    #[must_use]
    pub const fn node_id(&self) -> u64 {
        self.node_id
    }

    #[must_use]
    pub fn voters(&self) -> &[u64] {
        &self.voters
    }

    #[must_use]
    pub const fn listen_address(&self) -> SocketAddr {
        self.listen_address
    }

    #[must_use]
    pub const fn advertise_address(&self) -> SocketAddr {
        self.advertise_address
    }

    #[must_use]
    pub const fn raft_listen_address(&self) -> SocketAddr {
        self.raft_listen_address
    }

    #[must_use]
    pub const fn peer_addresses(&self) -> &BTreeMap<u64, SocketAddr> {
        &self.peer_addresses
    }

    #[must_use]
    pub fn data_directory(&self) -> &Path {
        &self.data_directory
    }

    #[must_use]
    pub const fn transport_security(&self) -> &MetaTransportSecurity {
        &self.transport_security
    }

    #[must_use]
    pub const fn timestamp_reservation_size(&self) -> u32 {
        self.timestamp_reservation_size
    }

    #[must_use]
    pub const fn maximum_future_drift_micros(&self) -> i64 {
        self.maximum_future_drift_micros
    }

    #[must_use]
    pub const fn shutdown_grace(&self) -> Duration {
        self.shutdown_grace
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawMetaConfig {
    version: u32,
    cluster_id: String,
    node_id: u64,
    voters: Vec<u64>,
    listen_address: SocketAddr,
    advertise_address: SocketAddr,
    raft_listen_address: SocketAddr,
    peer_addresses: BTreeMap<u64, SocketAddr>,
    data_directory: PathBuf,
    security: RawSecurity,
    timestamp_reservation_size: u32,
    maximum_future_drift_ms: u64,
    shutdown_grace_ms: u64,
}

impl RawMetaConfig {
    fn validate(mut self) -> Result<MetaNodeRuntimeConfig, MetaConfigError> {
        if self.version != FILE_CONFIG_VERSION {
            return Err(MetaConfigError::UnsupportedVersion {
                actual: self.version,
            });
        }
        let cluster_id = decode_cluster_id(&self.cluster_id)?;
        if self.node_id == 0 {
            return Err(MetaConfigError::InvalidNodeId);
        }
        self.voters.sort_unstable();
        if self.voters.is_empty()
            || self.voters.len() > MAX_VOTERS
            || self.voters.len().is_multiple_of(2)
            || self.voters.first() == Some(&0)
            || self.voters.windows(2).any(|pair| pair[0] == pair[1])
            || self.voters.binary_search(&self.node_id).is_err()
        {
            return Err(MetaConfigError::InvalidVoters);
        }
        let expected_peers: BTreeSet<_> = self
            .voters
            .iter()
            .copied()
            .filter(|voter| *voter != self.node_id)
            .collect();
        let actual_peers: BTreeSet<_> = self.peer_addresses.keys().copied().collect();
        if expected_peers != actual_peers
            || self
                .peer_addresses
                .values()
                .any(|address| address.port() == 0)
            || self.peer_addresses.values().collect::<BTreeSet<_>>().len()
                != self.peer_addresses.len()
        {
            return Err(MetaConfigError::InvalidPeerAddresses);
        }
        if self.listen_address.port() == 0
            || self.advertise_address.port() == 0
            || self.raft_listen_address.port() == 0
        {
            return Err(MetaConfigError::ZeroPort);
        }
        if self.advertise_address.ip().is_unspecified() {
            return Err(MetaConfigError::UnroutableAdvertiseAddress);
        }
        if !self.data_directory.is_absolute() {
            return Err(MetaConfigError::DataDirectoryNotAbsolute);
        }
        if !(1..=MAX_RESERVATION_SIZE).contains(&self.timestamp_reservation_size) {
            return Err(MetaConfigError::InvalidReservationSize);
        }
        if !(1..=MAX_FUTURE_DRIFT_MS).contains(&self.maximum_future_drift_ms) {
            return Err(MetaConfigError::InvalidFutureDrift);
        }
        if !(MIN_SHUTDOWN_GRACE_MS..=MAX_SHUTDOWN_GRACE_MS).contains(&self.shutdown_grace_ms) {
            return Err(MetaConfigError::InvalidShutdownGrace);
        }
        let transport_security = match self.security {
            RawSecurity::LoopbackPlaintext => {
                if !self.listen_address.ip().is_loopback()
                    || !self.advertise_address.ip().is_loopback()
                    || !self.raft_listen_address.ip().is_loopback()
                    || self
                        .peer_addresses
                        .values()
                        .any(|address| !address.ip().is_loopback())
                {
                    return Err(MetaConfigError::InsecureNonLoopback);
                }
                MetaTransportSecurity::LoopbackPlaintext
            }
            RawSecurity::MutualTls {
                ca_certificate,
                node_certificate,
                private_key,
            } => MetaTransportSecurity::MutualTls(MetaTlsFiles::new(
                ca_certificate,
                node_certificate,
                private_key,
            )?),
        };
        let maximum_future_drift_micros = self
            .maximum_future_drift_ms
            .checked_mul(1_000)
            .and_then(|value| i64::try_from(value).ok())
            .ok_or(MetaConfigError::InvalidFutureDrift)?;
        Ok(MetaNodeRuntimeConfig {
            cluster_id,
            node_id: self.node_id,
            voters: self.voters,
            listen_address: self.listen_address,
            advertise_address: self.advertise_address,
            raft_listen_address: self.raft_listen_address,
            peer_addresses: self.peer_addresses,
            data_directory: self.data_directory,
            transport_security,
            timestamp_reservation_size: self.timestamp_reservation_size,
            maximum_future_drift_micros,
            shutdown_grace: Duration::from_millis(self.shutdown_grace_ms),
        })
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

fn decode_cluster_id(encoded: &str) -> Result<[u8; 16], MetaConfigError> {
    if encoded.len() != 32 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(MetaConfigError::InvalidClusterId);
    }
    let mut cluster_id = [0_u8; 16];
    for (index, output) in cluster_id.iter_mut().enumerate() {
        let offset = index * 2;
        *output = u8::from_str_radix(&encoded[offset..offset + 2], 16)
            .map_err(|_| MetaConfigError::InvalidClusterId)?;
    }
    if cluster_id == [0; 16] {
        return Err(MetaConfigError::InvalidClusterId);
    }
    Ok(cluster_id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MetaConfigError {
    Io(String),
    Json(String),
    InvalidFileSize { actual: usize },
    UnsupportedVersion { actual: u32 },
    InvalidClusterId,
    InvalidNodeId,
    InvalidVoters,
    InvalidPeerAddresses,
    ZeroPort,
    UnroutableAdvertiseAddress,
    DataDirectoryNotAbsolute,
    SecretPathNotAbsolute,
    DuplicateSecretPath,
    InsecureNonLoopback,
    InvalidReservationSize,
    InvalidFutureDrift,
    InvalidShutdownGrace,
}

impl Display for MetaConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "Meta config I/O error: {message}"),
            Self::Json(message) => write!(formatter, "Meta config JSON error: {message}"),
            Self::InvalidFileSize { actual } => write!(formatter, "invalid config size {actual}"),
            Self::UnsupportedVersion { actual } => {
                write!(formatter, "unsupported Meta config version {actual}")
            }
            Self::InvalidClusterId => formatter.write_str("invalid cluster_id"),
            Self::InvalidNodeId => formatter.write_str("node_id must be non-zero"),
            Self::InvalidVoters => formatter
                .write_str("voters must be an odd, unique, bounded set containing the local node"),
            Self::InvalidPeerAddresses => formatter.write_str(
                "peer_addresses must exactly cover the other voters with unique non-zero ports",
            ),
            Self::ZeroPort => formatter.write_str("network ports cannot be zero"),
            Self::UnroutableAdvertiseAddress => {
                formatter.write_str("advertise address cannot be unspecified")
            }
            Self::DataDirectoryNotAbsolute => {
                formatter.write_str("data directory must be absolute")
            }
            Self::SecretPathNotAbsolute => formatter.write_str("TLS secret paths must be absolute"),
            Self::DuplicateSecretPath => formatter.write_str("TLS secret paths must be distinct"),
            Self::InsecureNonLoopback => {
                formatter.write_str("plaintext transport is restricted to loopback addresses")
            }
            Self::InvalidReservationSize => {
                formatter.write_str("invalid timestamp_reservation_size")
            }
            Self::InvalidFutureDrift => formatter.write_str("invalid maximum_future_drift_ms"),
            Self::InvalidShutdownGrace => formatter.write_str("invalid shutdown_grace_ms"),
        }
    }
}

impl Error for MetaConfigError {}
