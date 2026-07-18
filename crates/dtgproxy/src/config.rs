use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub const NODE_CONFIG_VERSION: u16 = 1;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NodeConfig {
    version: u16,
    root: PathBuf,
    graph_id: u64,
    listen: SocketAddr,
    max_raft_ticks: usize,
    timestamp_reservation: u32,
}

impl NodeConfig {
    pub fn new(
        root: impl Into<PathBuf>,
        graph_id: u64,
        listen: SocketAddr,
        max_raft_ticks: usize,
        timestamp_reservation: u32,
    ) -> Result<Self, ConfigError> {
        let config = Self {
            version: NODE_CONFIG_VERSION,
            root: root.into(),
            graph_id,
            listen,
            max_raft_ticks,
            timestamp_reservation,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let bytes = fs::read(path).map_err(io_error)?;
        let mut config: Self = serde_json::from_slice(&bytes)
            .map_err(|error| ConfigError::InvalidJson(error.to_string()))?;
        if config.root.is_relative() {
            config.root = path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(&config.root);
        }
        config.validate()?;
        Ok(config)
    }

    pub fn store(&self, path: impl AsRef<Path>) -> Result<(), ConfigError> {
        self.validate()?;
        let path = path.as_ref();
        let parent = path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(io_error)?;
        let temporary = path.with_extension("tmp");
        let bytes = serde_json::to_vec_pretty(self)
            .map_err(|error| ConfigError::InvalidJson(error.to_string()))?;
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(io_error)?;
        file.write_all(&bytes)
            .and_then(|()| file.write_all(b"\n"))
            .and_then(|()| file.sync_all())
            .map_err(io_error)?;
        fs::rename(&temporary, path).map_err(io_error)?;
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(io_error)
    }

    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub const fn graph_id(&self) -> u64 {
        self.graph_id
    }

    #[must_use]
    pub const fn listen(&self) -> SocketAddr {
        self.listen
    }

    #[must_use]
    pub const fn max_raft_ticks(&self) -> usize {
        self.max_raft_ticks
    }

    #[must_use]
    pub const fn timestamp_reservation(&self) -> u32 {
        self.timestamp_reservation
    }

    fn validate(&self) -> Result<(), ConfigError> {
        if self.version != NODE_CONFIG_VERSION {
            return Err(ConfigError::UnsupportedVersion {
                version: self.version,
            });
        }
        if self.root.as_os_str().is_empty()
            || self.graph_id == 0
            || self.max_raft_ticks == 0
            || self.timestamp_reservation == 0
        {
            return Err(ConfigError::InvalidConfiguration);
        }
        Ok(())
    }
}

fn io_error(error: std::io::Error) -> ConfigError {
    ConfigError::Io(error.to_string())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConfigError {
    UnsupportedVersion { version: u16 },
    InvalidConfiguration,
    InvalidJson(String),
    Io(String),
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { version } => {
                write!(
                    formatter,
                    "unsupported node configuration version {version}"
                )
            }
            Self::InvalidConfiguration => formatter.write_str("invalid node configuration"),
            Self::InvalidJson(message) => {
                write!(formatter, "invalid node configuration JSON: {message}")
            }
            Self::Io(message) => write!(formatter, "node configuration I/O failed: {message}"),
        }
    }
}

impl Error for ConfigError {}
