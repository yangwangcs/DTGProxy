#![forbid(unsafe_code)]

mod admission;
mod analytics_coordinator;
#[cfg(test)]
mod analytics_coordinator_tests;
mod analytics_scheduler;
mod catalog;
mod file_config;
mod service;

pub use admission::{AdmissionController, AdmissionError, AdmissionPermit};
pub use analytics_scheduler::{
    ANALYTICS_PROCESS_STOP_FAULT_CODE, AnalyticsFaultInjector, AnalyticsFaultPoint,
    AnalyticsSchedulerMetrics, AnalyticsSchedulerMetricsSnapshot, process_stop_fault,
};
pub use catalog::{
    GatewayCatalogError, GatewayCatalogRouter, GatewayCatalogSnapshot, build_remote_topology,
};
pub use file_config::{GatewayConfigError, GatewayNodeRuntimeConfig, GatewayTransportSecurity};
pub use service::{RemoteGatewayService, RemoteGatewayServiceError};
