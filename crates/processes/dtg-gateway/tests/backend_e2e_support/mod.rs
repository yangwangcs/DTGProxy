mod artifact;
mod bolt;
mod cluster;

use std::io;
use std::net::SocketAddr;
use std::time::Duration;

pub use artifact::{
    Backend, CellSpec, CommittedSnapshotIngestArtifact, ProcessMetricsSnapshot,
    QuickDiagnosticArtifact, RawObservation, StageMetricsWindow, Summary, Workload, percentile_ns,
    stage_metrics_window_from_log, summarize, write_committed_snapshot_ingest_artifact,
    write_quick_artifact,
};
pub use bolt::{
    BoltResult, BoltSession, BoltValue, measure_cell as measure_cell_with_durations,
    measure_pipeline_cell as measure_pipeline_cell_with_durations,
};
pub use cluster::{
    CommittedSnapshotIngestObservation, DiagnosticCluster, DiagnosticRuntime,
    SNAPSHOT_INGEST_DIAGNOSTIC_BATCH_LEN,
};

pub async fn measure_cell(address: SocketAddr, spec: CellSpec) -> io::Result<RawObservation> {
    bolt::measure_cell(
        address,
        spec,
        Duration::from_secs(1),
        Duration::from_secs(5),
    )
    .await
}
