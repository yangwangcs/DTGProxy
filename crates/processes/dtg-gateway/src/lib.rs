#![forbid(unsafe_code)]

mod bolt;
mod config;
mod runtime;
mod service;

pub use bolt::{BoltError, BoltQuery, BoltSession, BoltTransaction, serve_bolt};
pub use config::{GatewayConfig, GatewayConfigError};
pub use runtime::build_gateway_runtime;
pub use service::GatewayService;

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
