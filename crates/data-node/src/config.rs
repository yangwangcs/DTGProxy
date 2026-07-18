use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use crate::NodeIdentity;

const MAX_META_SEEDS: usize = 31;
const MAX_CAPACITY_LABELS: usize = 64;
const MAX_LABEL_NAME_BYTES: usize = 64;
const MAX_LABEL_VALUE_BYTES: usize = 128;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlsFiles {
    ca_certificate: PathBuf,
    node_certificate: PathBuf,
    private_key: PathBuf,
}

impl TlsFiles {
    pub fn new(
        ca_certificate: impl Into<PathBuf>,
        node_certificate: impl Into<PathBuf>,
        private_key: impl Into<PathBuf>,
    ) -> Result<Self, ConfigError> {
        let ca_certificate = ca_certificate.into();
        let node_certificate = node_certificate.into();
        let private_key = private_key.into();
        if [&ca_certificate, &node_certificate, &private_key]
            .iter()
            .any(|path| !path.is_absolute())
        {
            return Err(ConfigError::SecretPathNotAbsolute);
        }
        let unique = BTreeSet::from([
            ca_certificate.clone(),
            node_certificate.clone(),
            private_key.clone(),
        ]);
        if unique.len() != 3 {
            return Err(ConfigError::DuplicateSecretPath);
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
pub enum TransportSecurity {
    MutualTls(TlsFiles),
    LoopbackPlaintext,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeConfig {
    identity: NodeIdentity,
    listen_address: SocketAddr,
    advertise_address: SocketAddr,
    data_directory: PathBuf,
    meta_seeds: Vec<SocketAddr>,
    transport_security: TransportSecurity,
    capacity_labels: BTreeMap<String, String>,
}

impl NodeConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        identity: NodeIdentity,
        listen_address: SocketAddr,
        advertise_address: SocketAddr,
        data_directory: impl Into<PathBuf>,
        mut meta_seeds: Vec<SocketAddr>,
        transport_security: TransportSecurity,
        capacity_labels: BTreeMap<String, String>,
    ) -> Result<Self, ConfigError> {
        let data_directory = data_directory.into();
        if !data_directory.is_absolute() {
            return Err(ConfigError::DataDirectoryNotAbsolute);
        }
        if listen_address.port() == 0
            || advertise_address.port() == 0
            || meta_seeds.iter().any(|address| address.port() == 0)
        {
            return Err(ConfigError::ZeroPort);
        }
        if advertise_address.ip().is_unspecified() {
            return Err(ConfigError::UnroutableAdvertiseAddress);
        }
        if meta_seeds.is_empty() {
            return Err(ConfigError::MissingMetaSeeds);
        }
        if meta_seeds.len() > MAX_META_SEEDS {
            return Err(ConfigError::TooManyMetaSeeds);
        }
        meta_seeds.sort_unstable();
        if let Some(duplicate) = meta_seeds
            .windows(2)
            .find(|pair| pair[0] == pair[1])
            .map(|pair| pair[0])
        {
            return Err(ConfigError::DuplicateMetaSeed { address: duplicate });
        }
        if matches!(transport_security, TransportSecurity::LoopbackPlaintext)
            && (!listen_address.ip().is_loopback()
                || !advertise_address.ip().is_loopback()
                || meta_seeds.iter().any(|address| !address.ip().is_loopback()))
        {
            return Err(ConfigError::InsecureNonLoopback);
        }
        validate_capacity_labels(&capacity_labels)?;
        Ok(Self {
            identity,
            listen_address,
            advertise_address,
            data_directory,
            meta_seeds,
            transport_security,
            capacity_labels,
        })
    }

    #[must_use]
    pub const fn identity(&self) -> &NodeIdentity {
        &self.identity
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
    pub fn data_directory(&self) -> &Path {
        &self.data_directory
    }

    #[must_use]
    pub fn meta_seeds(&self) -> &[SocketAddr] {
        &self.meta_seeds
    }

    #[must_use]
    pub const fn transport_security(&self) -> &TransportSecurity {
        &self.transport_security
    }

    #[must_use]
    pub const fn capacity_labels(&self) -> &BTreeMap<String, String> {
        &self.capacity_labels
    }
}

fn validate_capacity_labels(labels: &BTreeMap<String, String>) -> Result<(), ConfigError> {
    if labels.len() > MAX_CAPACITY_LABELS {
        return Err(ConfigError::TooManyCapacityLabels);
    }
    for (name, value) in labels {
        if name.is_empty()
            || name.len() > MAX_LABEL_NAME_BYTES
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
            || value.is_empty()
            || value.len() > MAX_LABEL_VALUE_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(ConfigError::InvalidCapacityLabel { name: name.clone() });
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    DataDirectoryNotAbsolute,
    SecretPathNotAbsolute,
    DuplicateSecretPath,
    ZeroPort,
    UnroutableAdvertiseAddress,
    MissingMetaSeeds,
    TooManyMetaSeeds,
    DuplicateMetaSeed { address: SocketAddr },
    InsecureNonLoopback,
    TooManyCapacityLabels,
    InvalidCapacityLabel { name: String },
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::DataDirectoryNotAbsolute => {
                formatter.write_str("data directory must be absolute")
            }
            Self::SecretPathNotAbsolute => formatter.write_str("TLS secret paths must be absolute"),
            Self::DuplicateSecretPath => formatter.write_str("TLS secret paths must be distinct"),
            Self::ZeroPort => formatter.write_str("network ports cannot be zero"),
            Self::UnroutableAdvertiseAddress => {
                formatter.write_str("advertise address cannot be unspecified")
            }
            Self::MissingMetaSeeds => formatter.write_str("at least one Meta seed is required"),
            Self::TooManyMetaSeeds => formatter.write_str("too many Meta seeds"),
            Self::DuplicateMetaSeed { address } => {
                write!(formatter, "duplicate Meta seed {address}")
            }
            Self::InsecureNonLoopback => {
                formatter.write_str("plaintext transport is restricted to loopback addresses")
            }
            Self::TooManyCapacityLabels => formatter.write_str("too many capacity labels"),
            Self::InvalidCapacityLabel { name } => {
                write!(formatter, "invalid capacity label {name:?}")
            }
        }
    }
}

impl Error for ConfigError {}
