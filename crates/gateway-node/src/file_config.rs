use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use serde::Deserialize;

const FILE_CONFIG_VERSION: u32 = 1;
const MAX_CONFIG_BYTES: usize = 1024 * 1024;
const MAX_META_SEEDS: usize = 31;
const MAX_DATA_NODES: usize = 4_096;
const MAXIMUM_INFLIGHT_LIMIT: usize = 65_536;
const MAX_RAFT_TICKS_LIMIT: usize = 1_048_576;
const MIN_WATCH_TIMEOUT_MS: u64 = 1_000;
const MAX_WATCH_TIMEOUT_MS: u64 = 300_000;
const MIN_SHUTDOWN_GRACE_MS: u64 = 100;
const MAX_SHUTDOWN_GRACE_MS: u64 = 300_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GatewayTransportSecurity {
    LoopbackPlaintext,
}

#[derive(Clone, Debug)]
pub struct GatewayNodeRuntimeConfig {
    cluster_id: [u8; 16],
    node_id: u64,
    graph_id: u64,
    listen_address: SocketAddr,
    bolt_listen_address: Option<SocketAddr>,
    advertise_address: SocketAddr,
    meta_seeds: Vec<SocketAddr>,
    data_nodes: BTreeMap<u64, SocketAddr>,
    transport_security: GatewayTransportSecurity,
    maximum_inflight: usize,
    max_raft_ticks: usize,
    catalog_watch_timeout: Duration,
    shutdown_grace: Duration,
}

impl GatewayNodeRuntimeConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, GatewayConfigError> {
        let bytes =
            std::fs::read(path).map_err(|error| GatewayConfigError::Io(error.to_string()))?;
        if bytes.is_empty() || bytes.len() > MAX_CONFIG_BYTES {
            return Err(GatewayConfigError::InvalidFileSize {
                actual: bytes.len(),
            });
        }
        let raw: RawGatewayConfig = serde_json::from_slice(&bytes)
            .map_err(|error| GatewayConfigError::Json(error.to_string()))?;
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
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn listen_address(&self) -> SocketAddr {
        self.listen_address
    }

    #[must_use]
    pub const fn bolt_listen_address(&self) -> Option<SocketAddr> {
        self.bolt_listen_address
    }

    #[must_use]
    pub const fn advertise_address(&self) -> SocketAddr {
        self.advertise_address
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
    pub const fn transport_security(&self) -> GatewayTransportSecurity {
        self.transport_security
    }

    #[must_use]
    pub const fn maximum_inflight(&self) -> usize {
        self.maximum_inflight
    }

    #[must_use]
    pub const fn max_raft_ticks(&self) -> usize {
        self.max_raft_ticks
    }

    #[must_use]
    pub const fn catalog_watch_timeout(&self) -> Duration {
        self.catalog_watch_timeout
    }

    #[must_use]
    pub const fn shutdown_grace(&self) -> Duration {
        self.shutdown_grace
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGatewayConfig {
    version: u32,
    cluster_id: String,
    node_id: u64,
    graph_id: u64,
    listen_address: SocketAddr,
    #[serde(default)]
    bolt_listen_address: Option<SocketAddr>,
    advertise_address: SocketAddr,
    meta_seeds: Vec<SocketAddr>,
    data_nodes: BTreeMap<u64, SocketAddr>,
    security: RawSecurity,
    maximum_inflight: usize,
    max_raft_ticks: usize,
    catalog_watch_timeout_ms: u64,
    shutdown_grace_ms: u64,
}

impl RawGatewayConfig {
    fn validate(mut self) -> Result<GatewayNodeRuntimeConfig, GatewayConfigError> {
        if self.version != FILE_CONFIG_VERSION {
            return Err(GatewayConfigError::UnsupportedVersion {
                actual: self.version,
            });
        }
        let cluster_id = decode_cluster_id(&self.cluster_id)?;
        if self.node_id == 0 || self.graph_id == 0 {
            return Err(GatewayConfigError::InvalidIdentity);
        }
        if self.listen_address.port() == 0
            || self
                .bolt_listen_address
                .is_some_and(|address| address.port() == 0 || address == self.listen_address)
            || self.advertise_address.port() == 0
            || self.meta_seeds.iter().any(|address| address.port() == 0)
            || self.data_nodes.values().any(|address| address.port() == 0)
        {
            return Err(GatewayConfigError::ZeroPort);
        }
        if self.advertise_address.ip().is_unspecified() {
            return Err(GatewayConfigError::UnroutableAdvertiseAddress);
        }
        if self.meta_seeds.is_empty() || self.meta_seeds.len() > MAX_META_SEEDS {
            return Err(GatewayConfigError::InvalidMetaSeeds);
        }
        self.meta_seeds.sort_unstable();
        if self.meta_seeds.windows(2).any(|pair| pair[0] == pair[1]) {
            return Err(GatewayConfigError::InvalidMetaSeeds);
        }
        if self.data_nodes.is_empty()
            || self.data_nodes.len() > MAX_DATA_NODES
            || self.data_nodes.contains_key(&0)
            || self.data_nodes.values().collect::<BTreeSet<_>>().len() != self.data_nodes.len()
        {
            return Err(GatewayConfigError::InvalidDataNodes);
        }
        if self.maximum_inflight == 0 || self.maximum_inflight > MAXIMUM_INFLIGHT_LIMIT {
            return Err(GatewayConfigError::InvalidMaximumInflight {
                actual: self.maximum_inflight,
            });
        }
        if self.max_raft_ticks == 0 || self.max_raft_ticks > MAX_RAFT_TICKS_LIMIT {
            return Err(GatewayConfigError::InvalidMaxRaftTicks {
                actual: self.max_raft_ticks,
            });
        }
        if !(MIN_WATCH_TIMEOUT_MS..=MAX_WATCH_TIMEOUT_MS).contains(&self.catalog_watch_timeout_ms) {
            return Err(GatewayConfigError::InvalidCatalogWatchTimeout {
                actual_ms: self.catalog_watch_timeout_ms,
            });
        }
        if !(MIN_SHUTDOWN_GRACE_MS..=MAX_SHUTDOWN_GRACE_MS).contains(&self.shutdown_grace_ms) {
            return Err(GatewayConfigError::InvalidShutdownGrace {
                actual_ms: self.shutdown_grace_ms,
            });
        }
        let transport_security = match self.security {
            RawSecurity::LoopbackPlaintext => {
                if !self.listen_address.ip().is_loopback()
                    || self
                        .bolt_listen_address
                        .is_some_and(|address| !address.ip().is_loopback())
                    || !self.advertise_address.ip().is_loopback()
                    || self
                        .meta_seeds
                        .iter()
                        .chain(self.data_nodes.values())
                        .any(|address| !address.ip().is_loopback())
                {
                    return Err(GatewayConfigError::InsecureNonLoopback);
                }
                GatewayTransportSecurity::LoopbackPlaintext
            }
        };
        Ok(GatewayNodeRuntimeConfig {
            cluster_id,
            node_id: self.node_id,
            graph_id: self.graph_id,
            listen_address: self.listen_address,
            bolt_listen_address: self.bolt_listen_address,
            advertise_address: self.advertise_address,
            meta_seeds: self.meta_seeds,
            data_nodes: self.data_nodes,
            transport_security,
            maximum_inflight: self.maximum_inflight,
            max_raft_ticks: self.max_raft_ticks,
            catalog_watch_timeout: Duration::from_millis(self.catalog_watch_timeout_ms),
            shutdown_grace: Duration::from_millis(self.shutdown_grace_ms),
        })
    }
}

#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum RawSecurity {
    LoopbackPlaintext,
}

fn decode_cluster_id(encoded: &str) -> Result<[u8; 16], GatewayConfigError> {
    if encoded.len() != 32 || !encoded.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(GatewayConfigError::InvalidClusterId);
    }
    let mut cluster_id = [0_u8; 16];
    for (index, output) in cluster_id.iter_mut().enumerate() {
        let offset = index * 2;
        *output = u8::from_str_radix(&encoded[offset..offset + 2], 16)
            .map_err(|_| GatewayConfigError::InvalidClusterId)?;
    }
    if cluster_id == [0; 16] {
        return Err(GatewayConfigError::InvalidClusterId);
    }
    Ok(cluster_id)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayConfigError {
    Io(String),
    Json(String),
    InvalidFileSize { actual: usize },
    UnsupportedVersion { actual: u32 },
    InvalidClusterId,
    InvalidIdentity,
    ZeroPort,
    UnroutableAdvertiseAddress,
    InvalidMetaSeeds,
    InvalidDataNodes,
    InsecureNonLoopback,
    InvalidMaximumInflight { actual: usize },
    InvalidMaxRaftTicks { actual: usize },
    InvalidCatalogWatchTimeout { actual_ms: u64 },
    InvalidShutdownGrace { actual_ms: u64 },
}

impl Display for GatewayConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "Gateway config I/O error: {message}"),
            Self::Json(message) => write!(formatter, "Gateway config JSON error: {message}"),
            Self::InvalidFileSize { actual } => write!(formatter, "invalid config size {actual}"),
            Self::UnsupportedVersion { actual } => {
                write!(formatter, "unsupported Gateway config version {actual}")
            }
            Self::InvalidClusterId => formatter.write_str("invalid cluster_id"),
            Self::InvalidIdentity => formatter.write_str("node_id and graph_id must be non-zero"),
            Self::ZeroPort => formatter.write_str("network ports cannot be zero"),
            Self::UnroutableAdvertiseAddress => {
                formatter.write_str("advertise address cannot be unspecified")
            }
            Self::InvalidMetaSeeds => formatter.write_str("invalid Meta seed set"),
            Self::InvalidDataNodes => formatter.write_str("invalid Data node address map"),
            Self::InsecureNonLoopback => {
                formatter.write_str("plaintext transport is restricted to loopback addresses")
            }
            Self::InvalidMaximumInflight { actual } => {
                write!(formatter, "invalid maximum_inflight {actual}")
            }
            Self::InvalidMaxRaftTicks { actual } => {
                write!(formatter, "invalid max_raft_ticks {actual}")
            }
            Self::InvalidCatalogWatchTimeout { actual_ms } => {
                write!(formatter, "invalid catalog_watch_timeout_ms {actual_ms}")
            }
            Self::InvalidShutdownGrace { actual_ms } => {
                write!(formatter, "invalid shutdown_grace_ms {actual_ms}")
            }
        }
    }
}

impl Error for GatewayConfigError {}
