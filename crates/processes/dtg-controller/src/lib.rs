#![forbid(unsafe_code)]

mod config;
mod service;

pub use config::{ControllerConfig, ProcessConfigError, TlsFiles, TransportSecurity};
pub use service::{ControllerProcess, ControllerProcessError, ControllerRpcService};

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
