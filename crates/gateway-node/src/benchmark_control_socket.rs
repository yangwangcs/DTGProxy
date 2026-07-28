use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use query_executor::{BenchmarkAblationConfig, BenchmarkAblationCountersSnapshot};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

use crate::BenchmarkAblationRuntime;

pub const BENCHMARK_CONTROL_SCHEMA_VERSION: u32 = 1;
const MAX_CONTROL_LINE_BYTES: usize = 16 * 1024;
const CONTROL_IO_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkAblationWireConfig {
    pub native_pushdown: bool,
    pub column_batches: bool,
    pub bounded_lazy_pages: bool,
    pub parallel_shard_fanout: bool,
    pub batched_property_gather: bool,
}

impl From<BenchmarkAblationWireConfig> for BenchmarkAblationConfig {
    fn from(value: BenchmarkAblationWireConfig) -> Self {
        Self {
            native_pushdown: value.native_pushdown,
            column_batches: value.column_batches,
            bounded_lazy_pages: value.bounded_lazy_pages,
            parallel_shard_fanout: value.parallel_shard_fanout,
            batched_property_gather: value.batched_property_gather,
        }
    }
}

impl From<BenchmarkAblationConfig> for BenchmarkAblationWireConfig {
    fn from(value: BenchmarkAblationConfig) -> Self {
        Self {
            native_pushdown: value.native_pushdown,
            column_batches: value.column_batches,
            bounded_lazy_pages: value.bounded_lazy_pages,
            parallel_shard_fanout: value.parallel_shard_fanout,
            batched_property_gather: value.batched_property_gather,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum BenchmarkControlRequest {
    BeginCell {
        schema_version: u32,
        cell_id: String,
        configuration_digest: String,
        config: BenchmarkAblationWireConfig,
    },
    FinishCell {
        schema_version: u32,
        session_token: String,
    },
    AbortCell {
        schema_version: u32,
        session_token: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum BenchmarkControlResponse {
    Begun {
        schema_version: u32,
        gateway_pid: u32,
        session_token: String,
        accepted_config: BenchmarkAblationWireConfig,
    },
    Finished {
        schema_version: u32,
        gateway_pid: u32,
        cell_id: String,
        configuration_digest: String,
        accepted_config: BenchmarkAblationWireConfig,
        queries_started: u64,
        queries_completed: u64,
        queries_failed: u64,
        queries_in_flight: u64,
        counters: BenchmarkAblationWireCounters,
    },
    Aborted {
        schema_version: u32,
        gateway_pid: u32,
    },
    Error {
        schema_version: u32,
        gateway_pid: u32,
        message: String,
    },
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BenchmarkAblationWireCounters {
    pub canonical_residual_scans: u64,
    pub row_column_conversion_boundaries: u64,
    pub eager_page_collections: u64,
    pub serial_shard_opens: u64,
    pub singleton_property_gather_reads: u64,
}

impl From<BenchmarkAblationCountersSnapshot> for BenchmarkAblationWireCounters {
    fn from(value: BenchmarkAblationCountersSnapshot) -> Self {
        Self {
            canonical_residual_scans: value.canonical_residual_scans(),
            row_column_conversion_boundaries: value.row_column_conversion_boundaries(),
            eager_page_collections: value.eager_page_collections(),
            serial_shard_opens: value.serial_shard_opens(),
            singleton_property_gather_reads: value.singleton_property_gather_reads(),
        }
    }
}

pub async fn serve_benchmark_ablation_control(
    path: &Path,
    runtime: Arc<BenchmarkAblationRuntime>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    if path.as_os_str().is_empty() || path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "benchmark control socket path is empty or already exists",
        ));
    }
    let listener = UnixListener::bind(path)?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    let result = loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break Ok(());
                }
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _)) => {
                        let runtime = Arc::clone(&runtime);
                        tokio::spawn(async move {
                            let _ = tokio::time::timeout(
                                CONTROL_IO_TIMEOUT,
                                handle_connection(stream, &runtime),
                            )
                            .await;
                        });
                    }
                    Err(error) => break Err(error),
                }
            }
        }
    };
    drop(listener);
    let cleanup = fs::remove_file(path);
    result.and(cleanup)
}

async fn handle_connection(
    stream: UnixStream,
    runtime: &BenchmarkAblationRuntime,
) -> io::Result<()> {
    let (reader, mut writer) = stream.into_split();
    let mut line = Vec::new();
    BufReader::new(reader)
        .take(u64::try_from(MAX_CONTROL_LINE_BYTES + 1).expect("constant fits u64"))
        .read_until(b'\n', &mut line)
        .await?;
    let response =
        if line.is_empty() || line.len() > MAX_CONTROL_LINE_BYTES || !line.ends_with(b"\n") {
            error_response("request must be one newline-terminated line of at most 16 KiB")
        } else {
            match serde_json::from_slice::<BenchmarkControlRequest>(&line) {
                Ok(request) => handle_request(runtime, request),
                Err(error) => error_response(&format!("invalid request: {error}")),
            }
        };
    let mut encoded = serde_json::to_vec(&response).map_err(io::Error::other)?;
    encoded.push(b'\n');
    writer.write_all(&encoded).await
}

fn handle_request(
    runtime: &BenchmarkAblationRuntime,
    request: BenchmarkControlRequest,
) -> BenchmarkControlResponse {
    match request {
        BenchmarkControlRequest::BeginCell {
            schema_version,
            cell_id,
            configuration_digest,
            config,
        } => {
            if schema_version != BENCHMARK_CONTROL_SCHEMA_VERSION {
                return error_response("unsupported schema_version");
            }
            match runtime.begin_cell(&cell_id, &configuration_digest, config.into()) {
                Ok(started) => BenchmarkControlResponse::Begun {
                    schema_version: BENCHMARK_CONTROL_SCHEMA_VERSION,
                    gateway_pid: std::process::id(),
                    session_token: started.session_token().to_owned(),
                    accepted_config: started.accepted_config().into(),
                },
                Err(error) => error_response(&error.to_string()),
            }
        }
        BenchmarkControlRequest::FinishCell {
            schema_version,
            session_token,
        } => {
            if schema_version != BENCHMARK_CONTROL_SCHEMA_VERSION {
                return error_response("unsupported schema_version");
            }
            match runtime.finish_cell(&session_token) {
                Ok(finished) => BenchmarkControlResponse::Finished {
                    schema_version: BENCHMARK_CONTROL_SCHEMA_VERSION,
                    gateway_pid: std::process::id(),
                    cell_id: finished.cell_id().to_owned(),
                    configuration_digest: finished.configuration_digest().to_owned(),
                    accepted_config: finished.config().into(),
                    queries_started: finished.queries_started(),
                    queries_completed: finished.queries_completed(),
                    queries_failed: finished.queries_failed(),
                    queries_in_flight: finished.queries_in_flight(),
                    counters: finished.counters().into(),
                },
                Err(error) => error_response(&error.to_string()),
            }
        }
        BenchmarkControlRequest::AbortCell {
            schema_version,
            session_token,
        } => {
            if schema_version != BENCHMARK_CONTROL_SCHEMA_VERSION {
                return error_response("unsupported schema_version");
            }
            match runtime.abort_cell(&session_token) {
                Ok(()) => BenchmarkControlResponse::Aborted {
                    schema_version: BENCHMARK_CONTROL_SCHEMA_VERSION,
                    gateway_pid: std::process::id(),
                },
                Err(error) => error_response(&error.to_string()),
            }
        }
    }
}

fn error_response(message: &str) -> BenchmarkControlResponse {
    BenchmarkControlResponse::Error {
        schema_version: BENCHMARK_CONTROL_SCHEMA_VERSION,
        gateway_pid: std::process::id(),
        message: message.to_owned(),
    }
}
