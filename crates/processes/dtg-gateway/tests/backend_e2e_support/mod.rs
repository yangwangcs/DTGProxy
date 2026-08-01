mod artifact;
mod bolt;
mod cluster;

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

pub use artifact::{
    Backend, CellSpec, ProcessMetricsSnapshot, QuickDiagnosticArtifact, RawObservation,
    StageMetricsWindow, Summary, Workload, percentile_ns, stage_metrics_window_from_log, summarize,
    write_quick_artifact,
};
pub use bolt::{BoltResult, BoltSession, BoltValue, measure_cell as measure_cell_with_durations};
pub use cluster::{DiagnosticCluster, DiagnosticRuntime};

pub async fn measure_cell(address: SocketAddr, spec: CellSpec) -> io::Result<RawObservation> {
    bolt::measure_cell(
        address,
        spec,
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await
}
