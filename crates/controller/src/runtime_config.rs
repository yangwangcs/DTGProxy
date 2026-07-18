use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

const MAX_CONFIG_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct ControllerRuntimeConfig {
    cluster_id: [u8; 16],
    controller_id: u64,
    meta_seeds: Vec<SocketAddr>,
    data_nodes: BTreeMap<u64, SocketAddr>,
    reconcile_interval: Duration,
    request_timeout: Duration,
}

impl ControllerRuntimeConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ControllerConfigError> {
        let bytes =
            std::fs::read(path).map_err(|error| ControllerConfigError::Io(error.to_string()))?;
        if bytes.is_empty() || bytes.len() > MAX_CONFIG_BYTES {
            return Err(ControllerConfigError::InvalidFileSize);
        }
        let raw: RawConfig = serde_json::from_slice(&bytes)
            .map_err(|error| ControllerConfigError::Json(error.to_string()))?;
        raw.validate()
    }

    #[must_use]
    pub const fn cluster_id(&self) -> &[u8; 16] {
        &self.cluster_id
    }
    #[must_use]
    pub const fn controller_id(&self) -> u64 {
        self.controller_id
    }
    #[must_use]
    pub fn meta_seeds(&self) -> &[SocketAddr] {
        &self.meta_seeds
    }
    #[must_use]
    pub const fn data_nodes(&self) -> &BTreeMap<u64, SocketAddr> {
        &self.data_nodes
    }
    #[must_use]
    pub const fn reconcile_interval(&self) -> Duration {
        self.reconcile_interval
    }
    #[must_use]
    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    version: u32,
    cluster_id: String,
    controller_id: u64,
    meta_seeds: Vec<SocketAddr>,
    data_nodes: BTreeMap<u64, SocketAddr>,
    reconcile_interval_ms: u64,
    request_timeout_ms: u64,
    security: Security,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Security {
    LoopbackPlaintext,
}

impl RawConfig {
    fn validate(mut self) -> Result<ControllerRuntimeConfig, ControllerConfigError> {
        if self.version != 1 {
            return Err(ControllerConfigError::UnsupportedVersion(self.version));
        }
        let cluster_id = decode_cluster_id(&self.cluster_id)?;
        if self.controller_id == 0 {
            return Err(ControllerConfigError::InvalidIdentity);
        }
        self.meta_seeds.sort_unstable();
        if self.meta_seeds.is_empty()
            || self.meta_seeds.len() > 31
            || self.meta_seeds.iter().any(|address| address.port() == 0)
            || self.meta_seeds.windows(2).any(|pair| pair[0] == pair[1])
        {
            return Err(ControllerConfigError::InvalidMetaSeeds);
        }
        if self.data_nodes.is_empty()
            || self.data_nodes.len() > 4_096
            || self.data_nodes.contains_key(&0)
            || self.data_nodes.values().any(|address| address.port() == 0)
            || self.data_nodes.values().collect::<BTreeSet<_>>().len() != self.data_nodes.len()
        {
            return Err(ControllerConfigError::InvalidDataNodes);
        }
        if !(10..=60_000).contains(&self.reconcile_interval_ms)
            || !(100..=300_000).contains(&self.request_timeout_ms)
        {
            return Err(ControllerConfigError::InvalidDuration);
        }
        match self.security {
            Security::LoopbackPlaintext
                if self
                    .meta_seeds
                    .iter()
                    .chain(self.data_nodes.values())
                    .all(|address| address.ip().is_loopback()) => {}
            Security::LoopbackPlaintext => return Err(ControllerConfigError::InsecureNonLoopback),
        }
        Ok(ControllerRuntimeConfig {
            cluster_id,
            controller_id: self.controller_id,
            meta_seeds: self.meta_seeds,
            data_nodes: self.data_nodes,
            reconcile_interval: Duration::from_millis(self.reconcile_interval_ms),
            request_timeout: Duration::from_millis(self.request_timeout_ms),
        })
    }
}

fn decode_cluster_id(value: &str) -> Result<[u8; 16], ControllerConfigError> {
    if value.len() != 32 {
        return Err(ControllerConfigError::InvalidClusterId);
    }
    let mut id = [0_u8; 16];
    for (index, byte) in id.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| ControllerConfigError::InvalidClusterId)?;
    }
    if id == [0; 16] {
        return Err(ControllerConfigError::InvalidClusterId);
    }
    Ok(id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControllerConfigError {
    Io(String),
    Json(String),
    InvalidFileSize,
    UnsupportedVersion(u32),
    InvalidClusterId,
    InvalidIdentity,
    InvalidMetaSeeds,
    InvalidDataNodes,
    InvalidDuration,
    InsecureNonLoopback,
}

impl Display for ControllerConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "Controller config I/O error: {message}"),
            Self::Json(message) => write!(formatter, "Controller config JSON error: {message}"),
            Self::InvalidFileSize => formatter.write_str("invalid Controller config file size"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported Controller config version {version}")
            }
            Self::InvalidClusterId => formatter.write_str("invalid Controller cluster ID"),
            Self::InvalidIdentity => formatter.write_str("invalid Controller identity"),
            Self::InvalidMetaSeeds => formatter.write_str("invalid Controller Meta seeds"),
            Self::InvalidDataNodes => formatter.write_str("invalid Controller Data nodes"),
            Self::InvalidDuration => formatter.write_str("invalid Controller duration"),
            Self::InsecureNonLoopback => {
                formatter.write_str("plaintext Controller endpoints must be loopback")
            }
        }
    }
}

impl Error for ControllerConfigError {}
