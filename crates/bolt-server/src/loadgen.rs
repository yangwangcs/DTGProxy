use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::{BoltProbeSession, ExternalTtfrProbeError, ExternalTtfrSample};
use bolt_protocol::Value;
use serde::Serialize;

const MAX_CONNECTIONS: usize = 4_096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalLoadConfig {
    address: SocketAddr,
    query: String,
    parameters: BTreeMap<String, Value>,
    connections: usize,
    warmup_duration: Duration,
    measurement_duration: Duration,
    operation_timeout: Duration,
    benchmark_session: Option<String>,
}

impl ExternalLoadConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        address: SocketAddr,
        query: impl Into<String>,
        parameters: BTreeMap<String, Value>,
        connections: usize,
        warmup_duration: Duration,
        measurement_duration: Duration,
        operation_timeout: Duration,
    ) -> Result<Self, ExternalTtfrProbeError> {
        let query = query.into();
        if query.trim().is_empty()
            || connections == 0
            || connections > MAX_CONNECTIONS
            || measurement_duration.is_zero()
            || operation_timeout.is_zero()
        {
            return Err(ExternalTtfrProbeError::InvalidConfiguration);
        }
        Ok(Self {
            address,
            query,
            parameters,
            connections,
            warmup_duration,
            measurement_duration,
            operation_timeout,
            benchmark_session: None,
        })
    }

    pub fn with_benchmark_session(
        mut self,
        benchmark_session: impl Into<String>,
    ) -> Result<Self, ExternalTtfrProbeError> {
        let benchmark_session = benchmark_session.into();
        if !valid_benchmark_session(&benchmark_session) {
            return Err(ExternalTtfrProbeError::InvalidConfiguration);
        }
        self.benchmark_session = Some(benchmark_session);
        Ok(self)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ExternalLoadReport {
    schema_version: u32,
    timing_boundary: &'static str,
    connections: usize,
    warmup_ns: u64,
    warmup_started_unix_ns: u64,
    measurement_started_unix_ns: u64,
    measurement_ended_unix_ns: u64,
    measured_elapsed_ns: u64,
    total_operations: usize,
    completed_operations: usize,
    throughput_ops_per_second: f64,
    connection_setup_samples_ns: Vec<u64>,
    ttfr_samples_ns: Vec<u64>,
    total_latency_samples_ns: Vec<u64>,
    result_digest: String,
    row_count: usize,
    error_count: usize,
}

impl ExternalLoadReport {
    #[must_use]
    pub fn connection_setup_samples_ns(&self) -> &[u64] {
        &self.connection_setup_samples_ns
    }

    #[must_use]
    pub fn ttfr_samples_ns(&self) -> &[u64] {
        &self.ttfr_samples_ns
    }

    #[must_use]
    pub fn total_latency_samples_ns(&self) -> &[u64] {
        &self.total_latency_samples_ns
    }

    #[must_use]
    pub const fn completed_operations(&self) -> usize {
        self.completed_operations
    }

    #[must_use]
    pub const fn total_operations(&self) -> usize {
        self.total_operations
    }

    #[must_use]
    pub const fn throughput_ops_per_second(&self) -> f64 {
        self.throughput_ops_per_second
    }

    #[must_use]
    pub const fn row_count(&self) -> usize {
        self.row_count
    }

    #[must_use]
    pub fn result_digest_hex(&self) -> &str {
        &self.result_digest
    }
}

pub async fn run_external_load(
    config: ExternalLoadConfig,
) -> Result<ExternalLoadReport, ExternalTtfrProbeError> {
    let mut connections = tokio::task::JoinSet::new();
    for _ in 0..config.connections {
        let address = config.address;
        let timeout = config.operation_timeout;
        let benchmark_session = config.benchmark_session.clone();
        connections.spawn(async move {
            let started = Instant::now();
            let session = BoltProbeSession::connect_with_benchmark_session(
                address,
                timeout,
                benchmark_session,
            )
            .await?;
            Ok::<_, ExternalTtfrProbeError>((session, started.elapsed()))
        });
    }

    let mut sessions = Vec::with_capacity(config.connections);
    let mut setup_samples = Vec::with_capacity(config.connections);
    while let Some(result) = connections.join_next().await {
        match result {
            Ok(Ok((session, setup))) => {
                sessions.push(session);
                setup_samples.push(duration_ns(setup));
            }
            Ok(Err(error)) => {
                connections.abort_all();
                return Err(error);
            }
            Err(error) => {
                connections.abort_all();
                return Err(ExternalTtfrProbeError::Protocol(format!(
                    "load connection task join failed: {error}"
                )));
            }
        }
    }

    let warmup_started = Instant::now();
    let measurement_started = warmup_started + config.warmup_duration;
    let measurement_ended = measurement_started + config.measurement_duration;
    let warmup_started_unix_ns = unix_time_ns()?;
    let measurement_started_unix_ns = warmup_started_unix_ns
        .checked_add(duration_ns(config.warmup_duration))
        .ok_or(ExternalTtfrProbeError::InvalidConfiguration)?;
    let measurement_ended_unix_ns = measurement_started_unix_ns
        .checked_add(duration_ns(config.measurement_duration))
        .ok_or(ExternalTtfrProbeError::InvalidConfiguration)?;

    let mut workers = tokio::task::JoinSet::new();
    for session in sessions {
        let config = config.clone();
        workers.spawn(async move {
            run_worker(config, session, measurement_started, measurement_ended).await
        });
    }

    let mut ttfr_samples = Vec::new();
    let mut latency_samples = Vec::new();
    let mut identity = None;
    let mut measured_elapsed = Duration::ZERO;
    let mut total_operations = 0_usize;
    while let Some(result) = workers.join_next().await {
        let worker = match result {
            Ok(Ok(worker)) => worker,
            Ok(Err(error)) => {
                workers.abort_all();
                return Err(error);
            }
            Err(error) => {
                workers.abort_all();
                return Err(ExternalTtfrProbeError::Protocol(format!(
                    "load worker join failed: {error}"
                )));
            }
        };
        measured_elapsed = measured_elapsed.max(worker.measured_elapsed);
        total_operations = total_operations
            .checked_add(worker.total_operations)
            .ok_or(ExternalTtfrProbeError::InvalidConfiguration)?;
        merge_identity(&mut identity, worker.result_digest, worker.row_count)?;
        ttfr_samples.extend(
            worker
                .samples
                .iter()
                .map(|sample| duration_ns(sample.ttfr())),
        );
        latency_samples.extend(
            worker
                .samples
                .iter()
                .map(|sample| duration_ns(sample.total_latency())),
        );
    }
    if ttfr_samples.is_empty() || measured_elapsed.is_zero() {
        return Err(ExternalTtfrProbeError::EmptyResult);
    }
    let (result_digest, row_count) = identity.ok_or(ExternalTtfrProbeError::EmptyResult)?;
    let completed_operations = ttfr_samples.len();
    Ok(ExternalLoadReport {
        schema_version: 1,
        timing_boundary: "RUN send to first decoded RECORD; connection setup excluded",
        connections: config.connections,
        warmup_ns: duration_ns(config.warmup_duration),
        warmup_started_unix_ns,
        measurement_started_unix_ns,
        measurement_ended_unix_ns,
        measured_elapsed_ns: duration_ns(measured_elapsed),
        total_operations,
        completed_operations,
        throughput_ops_per_second: completed_operations as f64 / measured_elapsed.as_secs_f64(),
        connection_setup_samples_ns: setup_samples,
        ttfr_samples_ns: ttfr_samples,
        total_latency_samples_ns: latency_samples,
        result_digest: hex_digest(result_digest),
        row_count,
        error_count: 0,
    })
}

async fn run_worker(
    config: ExternalLoadConfig,
    mut session: BoltProbeSession,
    measurement_started: Instant,
    measurement_ended: Instant,
) -> Result<WorkerReport, ExternalTtfrProbeError> {
    let mut identity = None;
    let mut total_operations = 0_usize;
    while Instant::now() < measurement_started {
        let sample = session
            .execute(&config.query, config.parameters.clone())
            .await?;
        merge_sample_identity(&mut identity, &sample)?;
        total_operations = total_operations
            .checked_add(1)
            .ok_or(ExternalTtfrProbeError::InvalidConfiguration)?;
    }

    let mut samples = Vec::new();
    while Instant::now() < measurement_ended || samples.is_empty() {
        let sample = session
            .execute(&config.query, config.parameters.clone())
            .await?;
        merge_sample_identity(&mut identity, &sample)?;
        total_operations = total_operations
            .checked_add(1)
            .ok_or(ExternalTtfrProbeError::InvalidConfiguration)?;
        samples.push(sample);
    }
    session.goodbye().await?;
    let measured_elapsed = measurement_started.elapsed();
    let (result_digest, row_count) = identity.ok_or(ExternalTtfrProbeError::EmptyResult)?;
    Ok(WorkerReport {
        measured_elapsed,
        total_operations,
        samples,
        result_digest,
        row_count,
    })
}

fn merge_sample_identity(
    identity: &mut Option<([u8; 32], usize)>,
    sample: &ExternalTtfrSample,
) -> Result<(), ExternalTtfrProbeError> {
    merge_identity(identity, sample.result_digest(), sample.row_count())
}

fn merge_identity(
    identity: &mut Option<([u8; 32], usize)>,
    result_digest: [u8; 32],
    row_count: usize,
) -> Result<(), ExternalTtfrProbeError> {
    match identity {
        Some(expected) if *expected != (result_digest, row_count) => {
            Err(ExternalTtfrProbeError::ResultMismatch)
        }
        Some(_) => Ok(()),
        None => {
            *identity = Some((result_digest, row_count));
            Ok(())
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn unix_time_ns() -> Result<u64, ExternalTtfrProbeError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| {
            ExternalTtfrProbeError::Protocol(format!("system clock precedes Unix epoch: {error}"))
        })?;
    u64::try_from(elapsed.as_nanos()).map_err(|_| ExternalTtfrProbeError::InvalidConfiguration)
}

fn hex_digest(digest: [u8; 32]) -> String {
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("write to string");
    }
    output
}

fn valid_benchmark_session(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

struct WorkerReport {
    measured_elapsed: Duration,
    total_operations: usize,
    samples: Vec<ExternalTtfrSample>,
    result_digest: [u8; 32],
    row_count: usize,
}
