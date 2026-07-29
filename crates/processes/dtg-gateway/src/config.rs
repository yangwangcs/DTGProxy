use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayConfig {
    bind_addr: SocketAddr,
    cluster_id: u64,
    request_timeout: Duration,
    cluster_endpoint: String,
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
        Self::new(bind_addr, cluster_id, Duration::from_millis(timeout_ms))?
            .with_cluster_endpoint(endpoint)
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayConfigError {
    ZeroCluster,
    ZeroRequestTimeout,
    InvalidBind(String),
    InvalidCluster(String),
    InvalidRequestTimeout(String),
    EmptyClusterEndpoint,
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
        }
    }
}

impl fmt::Display for GatewayConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBind(message)
            | Self::InvalidCluster(message)
            | Self::InvalidRequestTimeout(message) => {
                write!(formatter, "{}: {message}", self.code())
            }
            _ => formatter.write_str(self.code()),
        }
    }
}

impl std::error::Error for GatewayConfigError {}
