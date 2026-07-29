#![forbid(unsafe_code)]

mod config;
mod service;

pub use config::{MetaConfig, MetaPeer, ProcessConfigError, TlsFiles, TransportSecurity};
pub use service::{MetaProcess, MetaProcessError, MetaRpcService};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
