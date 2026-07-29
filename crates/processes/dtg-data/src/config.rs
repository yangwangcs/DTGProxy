use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::ffi::OsString;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use dtg_execution::storage::{BackendClass, BindingRole, CapabilityManifest};
use dtg_execution::{ProviderKind, ReplicaBinding};

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
    RemoteSignedToken([u8; 32]),
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
            Self::RemoteSignedToken(_) => formatter.write_str("RemoteSignedToken([redacted])"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DataConfigError {
    InvalidRpcAddress(String),
    InvalidEnvironment(String),
    InvalidAssignment(String),
}

impl fmt::Display for DataConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRpcAddress(value) => {
                write!(formatter, "invalid Data RPC address: {value}")
            }
            Self::InvalidEnvironment(value) => {
                write!(formatter, "invalid Data environment value: {value}")
            }
            Self::InvalidAssignment(value) => {
                write!(formatter, "invalid Data assignment: {value}")
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
        Self::from_environment(|name| env::var_os(name))
    }

    #[doc(hidden)]
    pub fn from_environment(
        get: impl Fn(&str) -> Option<OsString>,
    ) -> Result<Self, DataConfigError> {
        let fjall_root = get("DTG_DATA_FJALL_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./dtg-data/business"));
        let consensus_root = get("DTG_DATA_CONSENSUS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./dtg-data/raft"));
        let rpc_addr_value = environment_string(&get, "DTG_DATA_RPC_ADDR")?
            .unwrap_or_else(|| "127.0.0.1:50052".into());
        let rpc_addr = rpc_addr_value
            .parse()
            .map_err(|_| DataConfigError::InvalidRpcAddress(rpc_addr_value))?;
        let mut config = Self::new(fjall_root, consensus_root).with_rpc_addr(rpc_addr);
        if let Some(assignments) = environment_string(&get, "DTG_DATA_ASSIGNMENTS")? {
            let capability_names =
                environment_string(&get, "DTG_DATA_CAPABILITIES")?.ok_or_else(|| {
                    DataConfigError::InvalidAssignment(
                        "DTG_DATA_CAPABILITIES is required with DTG_DATA_ASSIGNMENTS".into(),
                    )
                })?;
            let capabilities = CapabilityManifest::from_names(
                capability_names
                    .split(',')
                    .map(str::trim)
                    .filter(|name| !name.is_empty()),
            )
            .map_err(|error| DataConfigError::InvalidAssignment(error.to_string()))?;
            for assignment in assignments
                .split(';')
                .map(str::trim)
                .filter(|assignment| !assignment.is_empty())
            {
                config = config.assign(parse_assignment(assignment, &capabilities)?);
            }
        }
        Ok(config)
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

fn environment_string(
    get: &impl Fn(&str) -> Option<OsString>,
    name: &str,
) -> Result<Option<String>, DataConfigError> {
    get(name)
        .map(|value| {
            value.into_string().map_err(|_| {
                DataConfigError::InvalidEnvironment(format!("{name} is not valid UTF-8"))
            })
        })
        .transpose()
}

fn parse_assignment(
    assignment: &str,
    capabilities: &CapabilityManifest,
) -> Result<ReplicaBinding, DataConfigError> {
    let fields = assignment.split(':').collect::<Vec<_>>();
    if fields.len() != 10 {
        return Err(DataConfigError::InvalidAssignment(
            "expected cluster:graph:shard:epoch:replica:generation:provider:contract:layout:namespace"
                .into(),
        ));
    }
    let parse_u64 = |index: usize| {
        fields[index]
            .parse::<u64>()
            .map_err(|error| DataConfigError::InvalidAssignment(error.to_string()))
    };
    let provider_kind = match fields[6] {
        "fjall" => ProviderKind::Fjall,
        "postgresql" => ProviderKind::PostgreSql,
        "neo4j" => ProviderKind::Neo4j,
        provider if provider.starts_with("remote/") => {
            ProviderKind::Remote(provider["remote/".len()..].to_owned())
        }
        provider => {
            return Err(DataConfigError::InvalidAssignment(format!(
                "unsupported provider {provider}"
            )));
        }
    };
    let contract_version = u32::try_from(parse_u64(7)?)
        .map_err(|_| DataConfigError::InvalidAssignment("contract version exceeds u32".into()))?;
    let layout_version = u32::try_from(parse_u64(8)?)
        .map_err(|_| DataConfigError::InvalidAssignment("layout version exceeds u32".into()))?;
    let backend = BackendClass::new(
        provider_kind.clone(),
        contract_version,
        layout_version,
        capabilities.names().map(str::to_owned),
    )
    .map_err(|error| DataConfigError::InvalidAssignment(error.to_string()))?;
    ReplicaBinding::builder()
        .cluster_id(parse_u64(0)?)
        .graph_id(parse_u64(1)?)
        .shard_id(parse_u64(2)?)
        .placement_epoch(parse_u64(3)?)
        .replica_id(parse_u64(4)?)
        .backend_generation(parse_u64(5)?)
        .backend_class_digest(backend.digest())
        .provider_kind(provider_kind)
        .contract_version(contract_version)
        .layout_version(layout_version)
        .capability_digest(capabilities.digest())
        .namespace_id(fields[9])
        .endpoint_profile_ref("environment-bootstrap")
        .credential_ref("environment-bootstrap")
        .role(BindingRole::Active)
        .build()
        .map_err(|error| DataConfigError::InvalidAssignment(error.to_string()))
}
