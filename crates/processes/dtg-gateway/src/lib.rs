#![forbid(unsafe_code)]

mod bolt;
mod config;
mod runtime;
mod service;

pub use bolt::{
    BoltError, BoltQuery, BoltSession, BoltStatementClass, BoltTransaction, serve_bolt,
};
pub use config::{GatewayConfig, GatewayConfigError, bolt_read_pipeline_enabled_from};
pub use runtime::build_gateway_runtime;
pub use service::GatewayService;

pub const CLEAN_BREAK_ARCHITECTURE_VERSION: u32 = 1;
