#![forbid(unsafe_code)]

mod admission;
mod catalog;
mod file_config;
mod service;

pub use admission::{AdmissionController, AdmissionError, AdmissionPermit};
pub use catalog::{
    GatewayCatalogError, GatewayCatalogRouter, GatewayCatalogSnapshot, build_remote_topology,
};
pub use file_config::{GatewayConfigError, GatewayNodeRuntimeConfig, GatewayTransportSecurity};
pub use service::{RemoteGatewayService, RemoteGatewayServiceError};
