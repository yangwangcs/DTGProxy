use std::collections::BTreeMap;
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
    Remote(String),
}

#[derive(Clone, Eq, PartialEq)]
pub enum CredentialProfile {
    None,
    PostgreSql(String),
    RemoteSignedToken([u8; 32]),
}

impl fmt::Debug for CredentialProfile {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::PostgreSql(_) => formatter.write_str("PostgreSql([redacted])"),
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
    backend_kind: ProviderKind,
    rpc_addr: SocketAddr,
    gateway_unix_socket: Option<PathBuf>,
    fjall_root: PathBuf,
    kuzu_root: PathBuf,
    consensus_root: PathBuf,
    endpoint_profiles: BTreeMap<String, EndpointProfile>,
    credential_profiles: BTreeMap<String, CredentialProfile>,
    assignments: Vec<ReplicaBinding>,
}

impl DataProcessConfig {
    pub fn new(fjall_root: impl AsRef<Path>, consensus_root: impl AsRef<Path>) -> Self {
        Self {
            backend_kind: ProviderKind::Fjall,
            rpc_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 50052),
            gateway_unix_socket: None,
            fjall_root: fjall_root.as_ref().to_path_buf(),
            kuzu_root: PathBuf::from("./dtg-data/kuzu"),
            consensus_root: consensus_root.as_ref().to_path_buf(),
            endpoint_profiles: BTreeMap::new(),
            credential_profiles: BTreeMap::new(),
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
        let backend_kind =
            parse_backend_kind(&required_environment_string(&get, "DTG_DATA_BACKEND_KIND")?)?;
        let fjall_root = get("DTG_DATA_FJALL_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./dtg-data/business"));
        let kuzu_root = get("DTG_DATA_KUZU_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./dtg-data/kuzu"));
        let consensus_root = get("DTG_DATA_CONSENSUS_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("./dtg-data/raft"));
        let rpc_addr_value = environment_string(&get, "DTG_DATA_RPC_ADDR")?
            .unwrap_or_else(|| "127.0.0.1:50052".into());
        let rpc_addr = rpc_addr_value
            .parse()
            .map_err(|_| DataConfigError::InvalidRpcAddress(rpc_addr_value))?;
        let gateway_unix_socket =
            environment_string(&get, "DTG_DATA_GATEWAY_UNIX_SOCKET")?.map(PathBuf::from);
        if gateway_unix_socket
            .as_ref()
            .is_some_and(|path| !path.is_absolute())
        {
            return Err(DataConfigError::InvalidEnvironment(
                "DTG_DATA_GATEWAY_UNIX_SOCKET must be an absolute path".into(),
            ));
        }
        let mut config = Self::new(fjall_root, consensus_root)
            .with_kuzu_root(kuzu_root)
            .with_rpc_addr(rpc_addr);
        config.backend_kind = backend_kind;
        config.gateway_unix_socket = gateway_unix_socket;
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
        configure_environment_bootstrap(config, &get)
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
    pub fn assign(mut self, binding: ReplicaBinding) -> Self {
        self.assignments.push(binding);
        self
    }

    #[must_use]
    pub fn with_backend_kind(mut self, backend_kind: ProviderKind) -> Self {
        self.backend_kind = backend_kind;
        self
    }

    pub fn backend_kind(&self) -> &ProviderKind {
        &self.backend_kind
    }

    pub const fn rpc_addr(&self) -> SocketAddr {
        self.rpc_addr
    }

    pub fn gateway_unix_socket(&self) -> Option<&Path> {
        self.gateway_unix_socket.as_deref()
    }

    pub fn fjall_root(&self) -> &Path {
        &self.fjall_root
    }

    pub fn kuzu_root(&self) -> &Path {
        &self.kuzu_root
    }

    #[must_use]
    pub fn with_kuzu_root(mut self, root: impl AsRef<Path>) -> Self {
        self.kuzu_root = root.as_ref().to_path_buf();
        self
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

fn required_environment_string(
    get: &impl Fn(&str) -> Option<OsString>,
    name: &str,
) -> Result<String, DataConfigError> {
    environment_string(get, name)?
        .ok_or_else(|| DataConfigError::InvalidEnvironment(format!("{name} is required")))
}

fn configure_environment_bootstrap(
    mut config: DataProcessConfig,
    get: &impl Fn(&str) -> Option<OsString>,
) -> Result<DataProcessConfig, DataConfigError> {
    if config
        .assignments
        .iter()
        .any(|binding| binding.provider_kind() != config.backend_kind())
    {
        return Err(DataConfigError::InvalidAssignment(
            "assignment provider does not match DTG_DATA_BACKEND_KIND".into(),
        ));
    }
    let has_postgresql = config
        .assignments
        .iter()
        .any(|binding| binding.provider_kind() == &ProviderKind::PostgreSql);
    if has_postgresql {
        let endpoint = required_environment_string(get, "DTG_DATA_POSTGRES_ENDPOINT")?;
        let credential = required_environment_string(get, "DTG_DATA_POSTGRES_CREDENTIAL")?;
        config = config
            .with_endpoint_profile(
                "environment-bootstrap",
                EndpointProfile::PostgreSql(endpoint),
            )
            .with_credential_profile(
                "environment-bootstrap",
                CredentialProfile::PostgreSql(credential),
            );
    }
    Ok(config)
}

fn parse_backend_kind(value: &str) -> Result<ProviderKind, DataConfigError> {
    match value {
        "fjall" => Ok(ProviderKind::Fjall),
        "postgresql" => Ok(ProviderKind::PostgreSql),
        "kuzu" => Ok(ProviderKind::Kuzu),
        _ => Err(DataConfigError::InvalidEnvironment(
            "DTG_DATA_BACKEND_KIND must be fjall, postgresql, or kuzu".into(),
        )),
    }
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
        "kuzu" => ProviderKind::Kuzu,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_accepts_only_absolute_gateway_unix_socket_paths() {
        let absolute = DataProcessConfig::from_environment(|name| match name {
            "DTG_DATA_BACKEND_KIND" => Some(OsString::from("fjall")),
            "DTG_DATA_GATEWAY_UNIX_SOCKET" => Some(OsString::from("/tmp/dtg-data.sock")),
            _ => None,
        })
        .unwrap();
        assert_eq!(
            absolute.gateway_unix_socket(),
            Some(Path::new("/tmp/dtg-data.sock"))
        );

        let error = DataProcessConfig::from_environment(|name| match name {
            "DTG_DATA_BACKEND_KIND" => Some(OsString::from("fjall")),
            "DTG_DATA_GATEWAY_UNIX_SOCKET" => Some(OsString::from("relative.sock")),
            _ => None,
        })
        .unwrap_err();
        assert!(matches!(error, DataConfigError::InvalidEnvironment(_)));
    }

    #[test]
    fn environment_bootstrap_loads_postgresql_profile() {
        let values = BTreeMap::from([
            ("DTG_DATA_BACKEND_KIND", OsString::from("postgresql")),
            (
                "DTG_DATA_CAPABILITIES",
                OsString::from("adjacency,immutable-read-view,logical-snapshot,point"),
            ),
            (
                "DTG_DATA_ASSIGNMENTS",
                OsString::from("7:11:13:17:19:23:postgresql:1:1:postgres-bench"),
            ),
            (
                "DTG_DATA_POSTGRES_ENDPOINT",
                OsString::from("host=127.0.0.1 port=55432 dbname=dtgproxy sslmode=disable"),
            ),
            (
                "DTG_DATA_POSTGRES_CREDENTIAL",
                OsString::from("user=dtgproxy password=secret"),
            ),
        ]);
        let config = DataProcessConfig::from_environment(|name| values.get(name).cloned()).unwrap();
        assert!(matches!(
            config.endpoint_profiles().get("environment-bootstrap"),
            Some(EndpointProfile::PostgreSql(value)) if value.contains("port=55432")
        ));
        assert!(matches!(
            config.credential_profiles().get("environment-bootstrap"),
            Some(CredentialProfile::PostgreSql(value)) if value == "user=dtgproxy password=secret"
        ));
    }

    #[test]
    fn environment_bootstrap_loads_local_kuzu_root_without_credentials() {
        let values = BTreeMap::from([
            ("DTG_DATA_BACKEND_KIND", OsString::from("kuzu")),
            (
                "DTG_DATA_CAPABILITIES",
                OsString::from("adjacency,immutable-read-view,logical-snapshot,point"),
            ),
            (
                "DTG_DATA_ASSIGNMENTS",
                OsString::from("7:11:13:17:19:23:kuzu:1:1:kuzu-bench"),
            ),
            ("DTG_DATA_KUZU_ROOT", OsString::from("/tmp/dtgproxy-kuzu")),
        ]);
        let config = DataProcessConfig::from_environment(|name| values.get(name).cloned()).unwrap();
        assert_eq!(config.kuzu_root(), Path::new("/tmp/dtgproxy-kuzu"));
        assert!(config.endpoint_profiles().is_empty());
        assert!(config.credential_profiles().is_empty());
    }

    #[test]
    fn external_assignment_rejects_incomplete_profile() {
        let values = BTreeMap::from([
            ("DTG_DATA_BACKEND_KIND", OsString::from("postgresql")),
            (
                "DTG_DATA_CAPABILITIES",
                OsString::from("adjacency,immutable-read-view,logical-snapshot,point"),
            ),
            (
                "DTG_DATA_ASSIGNMENTS",
                OsString::from("7:11:13:17:19:23:postgresql:1:1:postgres-bench"),
            ),
            (
                "DTG_DATA_POSTGRES_ENDPOINT",
                OsString::from("host=127.0.0.1"),
            ),
        ]);
        let error =
            DataProcessConfig::from_environment(|name| values.get(name).cloned()).unwrap_err();
        assert!(error.to_string().contains("DTG_DATA_POSTGRES_CREDENTIAL"));
    }

    #[test]
    fn environment_requires_one_declared_backend_kind() {
        let error = DataProcessConfig::from_environment(|_| None).unwrap_err();
        assert!(error.to_string().contains("DTG_DATA_BACKEND_KIND"));
    }

    #[test]
    fn environment_rejects_an_assignment_for_another_backend_kind() {
        let values = BTreeMap::from([
            ("DTG_DATA_BACKEND_KIND", OsString::from("fjall")),
            (
                "DTG_DATA_CAPABILITIES",
                OsString::from("adjacency,immutable-read-view,logical-snapshot,point"),
            ),
            (
                "DTG_DATA_ASSIGNMENTS",
                OsString::from("7:11:13:17:19:23:kuzu:1:1:kuzu-bench"),
            ),
        ]);
        let error =
            DataProcessConfig::from_environment(|name| values.get(name).cloned()).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("does not match DTG_DATA_BACKEND_KIND")
        );
    }
}
