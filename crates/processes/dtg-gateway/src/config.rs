use std::collections::BTreeMap;
use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use dtg_execution::planning::{
    CatalogShard, CatalogSnapshot, PlanningContext, SnapshotRequirements,
};
use dtg_execution::storage::{
    BackendClass, BindingRole, CapabilityManifest, ProviderKind, ReplicaBinding, TransactionTime,
    Version,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayConfig {
    bind_addr: SocketAddr,
    cluster_id: u64,
    request_timeout: Duration,
    cluster_endpoint: String,
    shard_endpoints: BTreeMap<u64, String>,
    meta_endpoint: String,
}

impl GatewayConfig {
    pub fn new(
        bind_addr: SocketAddr,
        cluster_id: u64,
        request_timeout: Duration,
    ) -> Result<Self, GatewayConfigError> {
        if cluster_id == 0 {
            return Err(GatewayConfigError::ZeroCluster);
        }
        if request_timeout.is_zero() {
            return Err(GatewayConfigError::ZeroRequestTimeout);
        }
        Ok(Self {
            bind_addr,
            cluster_id,
            request_timeout,
            cluster_endpoint: "http://127.0.0.1:7690".into(),
            shard_endpoints: BTreeMap::new(),
            meta_endpoint: "http://127.0.0.1:7689".into(),
        })
    }

    pub fn from_env() -> Result<Self, GatewayConfigError> {
        let bind_addr = std::env::var("DTG_GATEWAY_BIND")
            .unwrap_or_else(|_| "127.0.0.1:7687".into())
            .parse()
            .map_err(|error: std::net::AddrParseError| {
                GatewayConfigError::InvalidBind(error.to_string())
            })?;
        let cluster_id = std::env::var("DTG_GATEWAY_CLUSTER_ID")
            .unwrap_or_else(|_| "1".into())
            .parse()
            .map_err(|error: std::num::ParseIntError| {
                GatewayConfigError::InvalidCluster(error.to_string())
            })?;
        let timeout_ms = std::env::var("DTG_GATEWAY_REQUEST_TIMEOUT_MS")
            .unwrap_or_else(|_| "30000".into())
            .parse()
            .map_err(|error: std::num::ParseIntError| {
                GatewayConfigError::InvalidRequestTimeout(error.to_string())
            })?;
        let endpoint = std::env::var("DTG_GATEWAY_CLUSTER_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:7690".into());
        let shard_endpoints = match std::env::var("DTG_GATEWAY_SHARD_ENDPOINTS") {
            Ok(value) => parse_shard_endpoints(&value)?,
            Err(std::env::VarError::NotPresent) => BTreeMap::new(),
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(GatewayConfigError::InvalidShardEndpoints);
            }
        };
        let meta_endpoint = std::env::var("DTG_GATEWAY_META_ENDPOINT")
            .unwrap_or_else(|_| "http://127.0.0.1:7689".into());
        Self::new(bind_addr, cluster_id, Duration::from_millis(timeout_ms))?
            .with_cluster_endpoint(endpoint)?
            .with_shard_endpoints(shard_endpoints)?
            .with_meta_endpoint(meta_endpoint)
    }

    pub fn with_cluster_endpoint(
        mut self,
        endpoint: impl Into<String>,
    ) -> Result<Self, GatewayConfigError> {
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() {
            return Err(GatewayConfigError::EmptyClusterEndpoint);
        }
        self.cluster_endpoint = endpoint;
        Ok(self)
    }

    pub const fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    pub fn cluster_endpoint(&self) -> &str {
        &self.cluster_endpoint
    }

    pub fn with_shard_endpoints(
        mut self,
        shard_endpoints: BTreeMap<u64, String>,
    ) -> Result<Self, GatewayConfigError> {
        if shard_endpoints
            .values()
            .any(|endpoint| endpoint.trim().is_empty())
        {
            return Err(GatewayConfigError::InvalidShardEndpoints);
        }
        self.shard_endpoints = shard_endpoints;
        Ok(self)
    }

    pub const fn shard_endpoints(&self) -> &BTreeMap<u64, String> {
        &self.shard_endpoints
    }

    pub fn with_meta_endpoint(
        mut self,
        endpoint: impl Into<String>,
    ) -> Result<Self, GatewayConfigError> {
        let endpoint = endpoint.into();
        if endpoint.trim().is_empty() {
            return Err(GatewayConfigError::EmptyMetaEndpoint);
        }
        self.meta_endpoint = endpoint;
        Ok(self)
    }

    pub fn meta_endpoint(&self) -> &str {
        &self.meta_endpoint
    }

    pub fn planning_context_from_env(&self) -> Result<PlanningContext, GatewayConfigError> {
        let graph_id = required_u64("DTG_GATEWAY_GRAPH_ID")?;
        let catalog_version = required_u64("DTG_GATEWAY_CATALOG_VERSION")?;
        let schema_version = required_u64("DTG_GATEWAY_SCHEMA_VERSION")?;
        let transaction_time = required_i64("DTG_GATEWAY_TRANSACTION_TIME")?;
        let valid_at = required_i64("DTG_GATEWAY_VALID_AT")?;
        let logical_scan_bound = required_u32("DTG_GATEWAY_LOGICAL_SCAN_BOUND")?;
        let capability_names = required_env("DTG_GATEWAY_CAPABILITIES")?;
        let capabilities = CapabilityManifest::from_names(
            capability_names
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty()),
        )
        .map_err(|error| GatewayConfigError::InvalidPlanning(error.to_string()))?;
        let shard_specs = required_env("DTG_GATEWAY_SHARDS")?;
        let mut shards = Vec::new();
        for spec in shard_specs
            .split(',')
            .map(str::trim)
            .filter(|spec| !spec.is_empty())
        {
            let fields = spec.split(':').collect::<Vec<_>>();
            if fields.len() != 9 {
                return Err(GatewayConfigError::InvalidPlanning(
                    "DTG_GATEWAY_SHARDS entries must be shard:epoch:replica:generation:applied:provider:contract:layout:namespace"
                        .into(),
                ));
            }
            let parse = |index: usize| {
                fields[index].parse::<u64>().map_err(|error| {
                    GatewayConfigError::InvalidPlanning(format!(
                        "invalid DTG_GATEWAY_SHARDS numeric field: {error}"
                    ))
                })
            };
            let shard_id = parse(0)?;
            let placement_epoch = parse(1)?;
            let replica_id = parse(2)?;
            let backend_generation = parse(3)?;
            let applied_index = parse(4)?;
            let provider_kind = match fields[5] {
                "fjall" => ProviderKind::Fjall,
                "postgresql" => ProviderKind::PostgreSql,
                "kuzu" => ProviderKind::Kuzu,
                provider if provider.starts_with("remote/") => {
                    ProviderKind::Remote(provider["remote/".len()..].to_owned())
                }
                provider => {
                    return Err(GatewayConfigError::InvalidPlanning(format!(
                        "unsupported planning provider: {provider}"
                    )));
                }
            };
            let contract_version = u32::try_from(parse(6)?).map_err(|_| {
                GatewayConfigError::InvalidPlanning("contract version exceeds u32".into())
            })?;
            let layout_version = u32::try_from(parse(7)?).map_err(|_| {
                GatewayConfigError::InvalidPlanning("layout version exceeds u32".into())
            })?;
            let backend_class = BackendClass::new(
                provider_kind.clone(),
                contract_version,
                layout_version,
                capabilities.names().map(str::to_owned),
            )
            .map_err(|error| GatewayConfigError::InvalidPlanning(error.to_string()))?;
            let binding = ReplicaBinding::builder()
                .cluster_id(self.cluster_id)
                .graph_id(graph_id)
                .shard_id(shard_id)
                .placement_epoch(placement_epoch)
                .replica_id(replica_id)
                .backend_generation(backend_generation)
                .backend_class_digest(backend_class.digest())
                .provider_kind(provider_kind)
                .contract_version(contract_version)
                .layout_version(layout_version)
                .capability_digest(capabilities.digest())
                .namespace_id(fields[8])
                .endpoint_profile_ref("gateway-catalog")
                .credential_ref("gateway-catalog")
                .role(BindingRole::Active)
                .build()
                .map_err(|error| GatewayConfigError::InvalidPlanning(error.to_string()))?;
            shards.push(CatalogShard::new(binding, applied_index));
        }
        let transaction_time = TransactionTime::new(transaction_time)
            .map_err(|error| GatewayConfigError::InvalidPlanning(error.to_string()))?;
        let catalog = CatalogSnapshot::new(
            Version::new(catalog_version),
            Version::new(schema_version),
            shards,
        )
        .map_err(|error| GatewayConfigError::InvalidPlanning(error.to_string()))?;
        if !self.shard_endpoints.is_empty()
            && catalog.shards().iter().any(|shard| {
                !self
                    .shard_endpoints
                    .contains_key(&shard.binding().shard_id().get())
            })
        {
            return Err(GatewayConfigError::InvalidShardEndpoints);
        }
        PlanningContext::new(
            catalog,
            capabilities,
            SnapshotRequirements::fixed(transaction_time, valid_at),
            Some(logical_scan_bound),
        )
        .map_err(|error| GatewayConfigError::InvalidPlanning(error.to_string()))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayConfigError {
    ZeroCluster,
    ZeroRequestTimeout,
    InvalidBind(String),
    InvalidCluster(String),
    InvalidRequestTimeout(String),
    EmptyClusterEndpoint,
    InvalidShardEndpoints,
    EmptyMetaEndpoint,
    MissingPlanning(String),
    InvalidPlanning(String),
}

impl GatewayConfigError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::ZeroCluster => "DTG-GATEWAY-CONFIG-CLUSTER",
            Self::ZeroRequestTimeout => "DTG-GATEWAY-CONFIG-TIMEOUT",
            Self::InvalidBind(_) => "DTG-GATEWAY-CONFIG-BIND",
            Self::InvalidCluster(_) => "DTG-GATEWAY-CONFIG-CLUSTER",
            Self::InvalidRequestTimeout(_) => "DTG-GATEWAY-CONFIG-TIMEOUT",
            Self::EmptyClusterEndpoint => "DTG-GATEWAY-CONFIG-ENDPOINT",
            Self::InvalidShardEndpoints => "DTG-GATEWAY-CONFIG-SHARD-ENDPOINTS",
            Self::EmptyMetaEndpoint => "DTG-GATEWAY-CONFIG-META-ENDPOINT",
            Self::MissingPlanning(_) => "DTG-GATEWAY-CONFIG-PLANNING-MISSING",
            Self::InvalidPlanning(_) => "DTG-GATEWAY-CONFIG-PLANNING",
        }
    }
}

fn parse_shard_endpoints(value: &str) -> Result<BTreeMap<u64, String>, GatewayConfigError> {
    let mut endpoints = BTreeMap::new();
    for spec in value
        .split(',')
        .map(str::trim)
        .filter(|spec| !spec.is_empty())
    {
        let (shard, endpoint) = spec
            .split_once('=')
            .ok_or(GatewayConfigError::InvalidShardEndpoints)?;
        let shard = shard
            .trim()
            .parse()
            .map_err(|_| GatewayConfigError::InvalidShardEndpoints)?;
        let endpoint = endpoint.trim();
        if endpoint.is_empty() || endpoints.insert(shard, endpoint.to_owned()).is_some() {
            return Err(GatewayConfigError::InvalidShardEndpoints);
        }
    }
    if endpoints.is_empty() {
        return Err(GatewayConfigError::InvalidShardEndpoints);
    }
    Ok(endpoints)
}

impl fmt::Display for GatewayConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBind(message)
            | Self::InvalidCluster(message)
            | Self::InvalidRequestTimeout(message)
            | Self::MissingPlanning(message)
            | Self::InvalidPlanning(message) => {
                write!(formatter, "{}: {message}", self.code())
            }
            _ => formatter.write_str(self.code()),
        }
    }
}

fn required_env(name: &str) -> Result<String, GatewayConfigError> {
    std::env::var(name).map_err(|_| GatewayConfigError::MissingPlanning(name.into()))
}

fn required_u64(name: &str) -> Result<u64, GatewayConfigError> {
    required_env(name)?
        .parse()
        .map_err(|error| GatewayConfigError::InvalidPlanning(format!("invalid {name}: {error}")))
}

fn required_u32(name: &str) -> Result<u32, GatewayConfigError> {
    required_env(name)?
        .parse()
        .map_err(|error| GatewayConfigError::InvalidPlanning(format!("invalid {name}: {error}")))
}

fn required_i64(name: &str) -> Result<i64, GatewayConfigError> {
    required_env(name)?
        .parse()
        .map_err(|error| GatewayConfigError::InvalidPlanning(format!("invalid {name}: {error}")))
}

impl std::error::Error for GatewayConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn meta_endpoint_is_configured_separately_from_data_endpoint() {
        let config =
            GatewayConfig::new("127.0.0.1:7687".parse().unwrap(), 7, Duration::from_secs(1))
                .unwrap()
                .with_cluster_endpoint("http://data:7690")
                .unwrap()
                .with_meta_endpoint("http://meta:7689")
                .unwrap();
        assert_eq!(config.cluster_endpoint(), "http://data:7690");
        assert_eq!(config.meta_endpoint(), "http://meta:7689");
    }

    #[test]
    fn shard_endpoint_specs_require_unique_nonempty_assignments() {
        assert_eq!(
            parse_shard_endpoints("1=http://data-1,2=http://data-2").unwrap(),
            BTreeMap::from([
                (1, "http://data-1".to_owned()),
                (2, "http://data-2".to_owned()),
            ])
        );
        assert!(parse_shard_endpoints("1=http://data-1,1=http://data-2").is_err());
        assert!(parse_shard_endpoints("1=").is_err());
    }
}
