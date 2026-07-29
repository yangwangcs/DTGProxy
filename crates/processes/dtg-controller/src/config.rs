use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const MAX_CONFIG_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct ControllerConfig {
    cluster_id: u64,
    node_id: u64,
    listen_addr: SocketAddr,
    data_directory: PathBuf,
    meta_endpoints: Vec<String>,
    security: TransportSecurity,
}

impl ControllerConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ProcessConfigError> {
        let bytes =
            std::fs::read(path).map_err(|error| ProcessConfigError::Io(error.to_string()))?;
        if bytes.is_empty() || bytes.len() > MAX_CONFIG_BYTES {
            return Err(ProcessConfigError::InvalidFileSize);
        }
        let raw: RawControllerConfig = serde_json::from_slice(&bytes)
            .map_err(|error| ProcessConfigError::Json(error.to_string()))?;
        raw.validate()
    }

    pub fn for_test(
        data_directory: impl AsRef<Path>,
        cluster_id: u64,
        node_id: u64,
    ) -> Result<Self, ProcessConfigError> {
        validate_config(
            cluster_id,
            node_id,
            "127.0.0.1:0"
                .parse()
                .expect("static socket address is valid"),
            data_directory.as_ref().to_path_buf(),
            Vec::new(),
            TransportSecurity::LoopbackPlaintext,
        )
    }

    pub const fn cluster_id(&self) -> u64 {
        self.cluster_id
    }

    pub const fn node_id(&self) -> u64 {
        self.node_id
    }

    pub const fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub fn data_directory(&self) -> &Path {
        &self.data_directory
    }

    pub const fn security(&self) -> &TransportSecurity {
        &self.security
    }

    pub fn meta_endpoints(&self) -> &[String] {
        &self.meta_endpoints
    }

    pub fn consensus_path(&self) -> PathBuf {
        self.data_directory.join("controller-consensus")
    }
}

#[derive(Clone, Debug)]
pub enum TransportSecurity {
    LoopbackPlaintext,
    MutualTls(TlsFiles),
}

#[derive(Clone, Debug)]
pub struct TlsFiles {
    certificate: PathBuf,
    private_key: PathBuf,
    client_ca: PathBuf,
}

impl TlsFiles {
    pub fn certificate(&self) -> &Path {
        &self.certificate
    }

    pub fn private_key(&self) -> &Path {
        &self.private_key
    }

    pub fn client_ca(&self) -> &Path {
        &self.client_ca
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawControllerConfig {
    version: u32,
    cluster_id: u64,
    node_id: u64,
    listen_addr: SocketAddr,
    data_directory: PathBuf,
    meta_endpoints: Vec<String>,
    security: RawSecurity,
}

impl RawControllerConfig {
    fn validate(self) -> Result<ControllerConfig, ProcessConfigError> {
        if self.version != 1 {
            return Err(ProcessConfigError::UnsupportedVersion(self.version));
        }
        if self.meta_endpoints.is_empty()
            || self.meta_endpoints.iter().any(|endpoint| {
                !(endpoint.starts_with("http://") || endpoint.starts_with("https://"))
            })
        {
            return Err(ProcessConfigError::InvalidRuntime);
        }
        validate_config(
            self.cluster_id,
            self.node_id,
            self.listen_addr,
            self.data_directory,
            self.meta_endpoints,
            self.security.try_into()?,
        )
    }
}

#[derive(Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
enum RawSecurity {
    LoopbackPlaintext,
    MutualTls {
        certificate: PathBuf,
        private_key: PathBuf,
        client_ca: PathBuf,
    },
}

impl TryFrom<RawSecurity> for TransportSecurity {
    type Error = ProcessConfigError;

    fn try_from(value: RawSecurity) -> Result<Self, Self::Error> {
        match value {
            RawSecurity::LoopbackPlaintext => Ok(Self::LoopbackPlaintext),
            RawSecurity::MutualTls {
                certificate,
                private_key,
                client_ca,
            } => {
                if [&certificate, &private_key, &client_ca]
                    .iter()
                    .any(|path| !path.is_absolute())
                    || certificate == private_key
                    || certificate == client_ca
                    || private_key == client_ca
                {
                    return Err(ProcessConfigError::InvalidTlsFiles);
                }
                Ok(Self::MutualTls(TlsFiles {
                    certificate,
                    private_key,
                    client_ca,
                }))
            }
        }
    }
}

fn validate_config(
    cluster_id: u64,
    node_id: u64,
    listen_addr: SocketAddr,
    data_directory: PathBuf,
    meta_endpoints: Vec<String>,
    security: TransportSecurity,
) -> Result<ControllerConfig, ProcessConfigError> {
    if cluster_id == 0 || node_id == 0 {
        return Err(ProcessConfigError::InvalidIdentity);
    }
    if data_directory.as_os_str().is_empty() {
        return Err(ProcessConfigError::InvalidRuntime);
    }
    if matches!(security, TransportSecurity::LoopbackPlaintext) && !listen_addr.ip().is_loopback() {
        return Err(ProcessConfigError::InsecureNonLoopback);
    }
    Ok(ControllerConfig {
        cluster_id,
        node_id,
        listen_addr,
        data_directory,
        meta_endpoints,
        security,
    })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProcessConfigError {
    Io(String),
    Json(String),
    InvalidFileSize,
    UnsupportedVersion(u32),
    InvalidIdentity,
    InvalidRuntime,
    InvalidTlsFiles,
    InsecureNonLoopback,
}

impl Display for ProcessConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(message) => write!(formatter, "Controller config I/O error: {message}"),
            Self::Json(message) => write!(formatter, "Controller config JSON error: {message}"),
            Self::InvalidFileSize => formatter.write_str("invalid Controller config file size"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported Controller config version {version}")
            }
            Self::InvalidIdentity => formatter.write_str("invalid Controller identity"),
            Self::InvalidRuntime => formatter.write_str("invalid Controller runtime configuration"),
            Self::InvalidTlsFiles => formatter.write_str("invalid Controller TLS files"),
            Self::InsecureNonLoopback => {
                formatter.write_str("plaintext Controller listener must be loopback")
            }
        }
    }
}

impl Error for ProcessConfigError {}
