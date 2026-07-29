use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::Deserialize;

const MAX_CONFIG_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
pub struct MetaConfig {
    cluster_id: u64,
    node_id: u64,
    listen_addr: SocketAddr,
    data_directory: PathBuf,
    analytics_lease_duration: u64,
    security: TransportSecurity,
}

impl MetaConfig {
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ProcessConfigError> {
        let bytes =
            std::fs::read(path).map_err(|error| ProcessConfigError::Io(error.to_string()))?;
        if bytes.is_empty() || bytes.len() > MAX_CONFIG_BYTES {
            return Err(ProcessConfigError::InvalidFileSize);
        }
        let raw: RawMetaConfig = serde_json::from_slice(&bytes)
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
            30,
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

    pub const fn analytics_lease_duration(&self) -> u64 {
        self.analytics_lease_duration
    }

    pub const fn security(&self) -> &TransportSecurity {
        &self.security
    }

    pub fn catalog_consensus_path(&self) -> PathBuf {
        self.data_directory.join("catalog-consensus")
    }

    pub fn timestamp_consensus_path(&self) -> PathBuf {
        self.data_directory.join("timestamp-consensus")
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
struct RawMetaConfig {
    version: u32,
    cluster_id: u64,
    node_id: u64,
    listen_addr: SocketAddr,
    data_directory: PathBuf,
    analytics_lease_duration: u64,
    security: RawSecurity,
}

impl RawMetaConfig {
    fn validate(self) -> Result<MetaConfig, ProcessConfigError> {
        if self.version != 1 {
            return Err(ProcessConfigError::UnsupportedVersion(self.version));
        }
        validate_config(
            self.cluster_id,
            self.node_id,
            self.listen_addr,
            self.data_directory,
            self.analytics_lease_duration,
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
    analytics_lease_duration: u64,
    security: TransportSecurity,
) -> Result<MetaConfig, ProcessConfigError> {
    if cluster_id == 0 || node_id == 0 {
        return Err(ProcessConfigError::InvalidIdentity);
    }
    if data_directory.as_os_str().is_empty() || analytics_lease_duration == 0 {
        return Err(ProcessConfigError::InvalidRuntime);
    }
    if matches!(security, TransportSecurity::LoopbackPlaintext) && !listen_addr.ip().is_loopback() {
        return Err(ProcessConfigError::InsecureNonLoopback);
    }
    Ok(MetaConfig {
        cluster_id,
        node_id,
        listen_addr,
        data_directory,
        analytics_lease_duration,
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
            Self::Io(message) => write!(formatter, "Meta config I/O error: {message}"),
            Self::Json(message) => write!(formatter, "Meta config JSON error: {message}"),
            Self::InvalidFileSize => formatter.write_str("invalid Meta config file size"),
            Self::UnsupportedVersion(version) => {
                write!(formatter, "unsupported Meta config version {version}")
            }
            Self::InvalidIdentity => formatter.write_str("invalid Meta identity"),
            Self::InvalidRuntime => formatter.write_str("invalid Meta runtime configuration"),
            Self::InvalidTlsFiles => formatter.write_str("invalid Meta TLS files"),
            Self::InsecureNonLoopback => {
                formatter.write_str("plaintext Meta listener must be loopback")
            }
        }
    }
}

impl Error for ProcessConfigError {}
