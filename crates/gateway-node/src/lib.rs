#![forbid(unsafe_code)]

mod admission;
mod analytics_coordinator;
#[cfg(test)]
mod analytics_coordinator_tests;
mod analytics_scheduler;
#[cfg(feature = "paper-benchmark-control")]
mod benchmark_ablation;
#[cfg(all(feature = "paper-benchmark-control", unix))]
mod benchmark_control_socket;
mod catalog;
mod file_config;
mod meta_client;
mod service;

pub use admission::{AdmissionController, AdmissionError, AdmissionPermit};
pub use analytics_scheduler::{
    ANALYTICS_PROCESS_STOP_FAULT_CODE, AnalyticsFaultInjector, AnalyticsFaultPoint,
    AnalyticsSchedulerMetrics, AnalyticsSchedulerMetricsSnapshot, process_stop_fault,
};
#[cfg(feature = "paper-benchmark-control")]
pub use benchmark_ablation::{
    BenchmarkAblationRuntime, BenchmarkCellFinished, BenchmarkCellStarted, BenchmarkControlError,
    BenchmarkQueryLease,
};
#[cfg(all(feature = "paper-benchmark-control", unix))]
pub use benchmark_control_socket::{
    BenchmarkAblationWireConfig, BenchmarkAblationWireCounters, BenchmarkControlRequest,
    BenchmarkControlResponse, serve_benchmark_ablation_control,
};
pub use catalog::{
    GatewayCatalogError, GatewayCatalogRouter, GatewayCatalogSnapshot, build_remote_topology,
};
pub use file_config::{GatewayConfigError, GatewayNodeRuntimeConfig, GatewayTransportSecurity};
pub use service::{RemoteGatewayService, RemoteGatewayServiceError};

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod test_support {
    use std::sync::Arc;

    use dtgproxy::DeploymentConfig;
    use shard_client::ShardClient;
    use storage_api::{AdapterError, StorageAdapter};

    use crate::service::RoutedShardReadAdapter;

    pub fn routed_shard_read_adapter(
        client: Arc<dyn ShardClient>,
        graph_id: u64,
        local_shard_id: u32,
        deployment: Arc<DeploymentConfig>,
        deadline_unix_ms: u64,
        request_id: u128,
    ) -> Result<impl StorageAdapter, AdapterError> {
        RoutedShardReadAdapter::new(
            client,
            graph_id,
            local_shard_id,
            deployment,
            deadline_unix_ms,
            request_id,
        )
    }
}
