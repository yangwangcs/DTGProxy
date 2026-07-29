use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use dtg_execution::ReplicaBinding;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EndpointProfile {
    PostgreSql(String),
    Neo4j { endpoint: String, database: String },
    Remote(String),
}

#[derive(Clone, Eq, PartialEq)]
pub enum CredentialProfile {
    None,
    PostgreSql(String),
    Neo4jBasic { username: String, password: String },
}

impl fmt::Debug for CredentialProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::PostgreSql(_) => formatter.write_str("PostgreSql([redacted])"),
            Self::Neo4jBasic { username, .. } => formatter
                .debug_struct("Neo4jBasic")
                .field("username", username)
                .field("password", &"[redacted]")
                .finish(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataConfigError {
    InvalidRpcAddress(String),
}

impl fmt::Display for DataConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRpcAddress(value) => {
                write!(formatter, "invalid Data RPC address: {value}")
            }
        }
    }
}

impl std::error::Error for DataConfigError {}

#[derive(Clone, Debug)]
pub struct DataProcessConfig {
    rpc_addr: SocketAddr,
    fjall_root: PathBuf,
    consensus_root: PathBuf,
    endpoint_profiles: BTreeMap<String, EndpointProfile>,
    credential_profiles: BTreeMap<String, CredentialProfile>,
    remote_providers: BTreeSet<String>,
    assignments: Vec<ReplicaBinding>,
}

impl DataProcessConfig {
    pub fn new(fjall_root: impl AsRef<Path>, consensus_root: impl AsRef<Path>) -> Self {
        Self {
            rpc_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 50052),
            fjall_root: fjall_root.as_ref().to_path_buf(),
            consensus_root: consensus_root.as_ref().to_path_buf(),
            endpoint_profiles: BTreeMap::new(),
            credential_profiles: BTreeMap::new(),
            remote_providers: BTreeSet::new(),
            assignments: Vec::new(),
        }
    }

    pub fn from_env() -> Result<Self, DataConfigError> {
        let fjall_root = env::var_os("DTG_DATA_FJALL_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./dtg-data/business"));
        let consensus_root = env::var_os("DTG_DATA_CONSENSUS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./dtg-data/raft"));
        let rpc_addr = env::var("DTG_DATA_RPC_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:50052".into())
            .parse()
            .map_err(|_| {
                DataConfigError::InvalidRpcAddress(
                    env::var("DTG_DATA_RPC_ADDR").unwrap_or_default(),
                )
            })?;
        Ok(Self::new(fjall_root, consensus_root).with_rpc_addr(rpc_addr))
    }

    #[must_use]
    pub const fn with_rpc_addr(mut self, rpc_addr: SocketAddr) -> Self {
        self.rpc_addr = rpc_addr;
        self
    }

    #[must_use]
    pub fn with_endpoint_profile(
        mut self,
        name: impl Into<String>,
        profile: EndpointProfile,
    ) -> Self {
        self.endpoint_profiles.insert(name.into(), profile);
        self
    }

    #[must_use]
    pub fn with_credential_profile(
        mut self,
        name: impl Into<String>,
        profile: CredentialProfile,
    ) -> Self {
        self.credential_profiles.insert(name.into(), profile);
        self
    }

    #[must_use]
    pub fn with_remote_provider(mut self, name: impl Into<String>) -> Self {
        self.remote_providers.insert(name.into());
        self
    }

    #[must_use]
    pub fn assign(mut self, binding: ReplicaBinding) -> Self {
        self.assignments.push(binding);
        self
    }

    pub const fn rpc_addr(&self) -> SocketAddr {
        self.rpc_addr
    }

    pub fn fjall_root(&self) -> &Path {
        &self.fjall_root
    }

    pub fn consensus_root(&self) -> &Path {
        &self.consensus_root
    }

    pub(crate) fn endpoint_profiles(&self) -> &BTreeMap<String, EndpointProfile> {
        &self.endpoint_profiles
    }

    pub(crate) fn credential_profiles(&self) -> &BTreeMap<String, CredentialProfile> {
        &self.credential_profiles
    }

    pub(crate) fn remote_providers(&self) -> &BTreeSet<String> {
        &self.remote_providers
    }

    pub(crate) fn assignments(&self) -> &[ReplicaBinding] {
        &self.assignments
    }
}
