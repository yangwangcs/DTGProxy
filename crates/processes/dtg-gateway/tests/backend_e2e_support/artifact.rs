use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;

use serde::Serialize;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    Fjall,
    #[serde(rename = "postgresql")]
    PostgreSql,
    Neo4j,
}

impl Backend {
    const ALL: [Self; 3] = [Self::Fjall, Self::PostgreSql, Self::Neo4j];
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Workload {
    CreateVertex,
    PointLookup,
    CountVertices,
}

impl Workload {
    const ALL: [Self; 3] = [Self::CreateVertex, Self::PointLookup, Self::CountVertices];

    pub const fn is_write(self) -> bool {
        matches!(self, Self::CreateVertex)
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CellSpec {
    pub backend: Backend,
    pub workload: Workload,
    pub concurrency: usize,
    pub repetition: u8,
}

impl CellSpec {
    pub fn matrix(seed: u64) -> Vec<Self> {
        let mut cells = Vec::with_capacity(54);
        for backend in Backend::ALL {
            for workload in Workload::ALL {
                for concurrency in [1, 8] {
                    for repetition in 0..3 {
                        cells.push(Self {
                            backend,
                            workload,
                            concurrency,
                            repetition,
                        });
                    }
                }
            }
        }
        shuffle(&mut cells, seed);
        cells
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RawObservation {
    pub backend: Backend,
    pub workload: Workload,
    pub concurrency: usize,
    pub repetition: u8,
    pub started_at_unix_ns: u64,
    pub finished_at_unix_ns: u64,
    pub warmup_finished_at_unix_ns: u64,
    pub measurement_started_at_unix_ns: u64,
    pub measurement_finished_at_unix_ns: u64,
    pub measured_duration_ns: u64,
    pub operations: u64,
    pub errors: u64,
    pub latency_samples_ns: Vec<u64>,
    pub row_count: u64,
    pub result_digest: String,
    pub query_digest: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct Summary {
    pub backend: Backend,
    pub workload: Workload,
    pub concurrency: usize,
    pub repetitions: usize,
    pub operations: u64,
    pub errors: u64,
    pub latency_samples: usize,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct QuickSummary {
    pub backend: Backend,
    pub workload: Workload,
    pub concurrency: usize,
    pub repetitions: usize,
    pub operations: u64,
    pub measured_duration_ns: u64,
    pub throughput_ops_per_second: f64,
    pub latency_samples: usize,
    pub p50_ns: u64,
    pub p95_ns: u64,
    pub p99_ns: u64,
}

#[derive(Clone, Debug, Serialize)]
pub struct QuickDiagnosticArtifact {
    pub format_version: u32,
    pub backend: Backend,
    pub revision: String,
    pub repetitions: usize,
    pub observations: Vec<RawObservation>,
    pub summaries: Vec<QuickSummary>,
}

impl QuickDiagnosticArtifact {
    pub fn new(revision: impl Into<String>, observations: Vec<RawObservation>) -> io::Result<Self> {
        let revision = revision.into();
        if revision.trim().is_empty() {
            return Err(invalid_data("quick diagnostic revision is empty"));
        }
        let backend = observations
            .first()
            .map(|observation| observation.backend)
            .ok_or_else(|| invalid_data("quick diagnostic has no observations"))?;
        let mut cells = BTreeSet::new();
        let mut result_identity = BTreeMap::<(Workload, usize), (u64, String, String)>::new();
        for observation in &observations {
            if observation.backend != backend {
                return Err(invalid_data("quick diagnostic mixes backend families"));
            }
            if !matches!(observation.concurrency, 1 | 8) {
                return Err(invalid_data("quick diagnostic concurrency must be 1 or 8"));
            }
            if observation.errors != 0
                || observation.operations == 0
                || observation.measured_duration_ns == 0
                || observation.latency_samples_ns.is_empty()
            {
                return Err(invalid_data(
                    "quick diagnostic observation is incomplete or contains errors",
                ));
            }
            if !cells.insert((
                observation.repetition,
                observation.workload,
                observation.concurrency,
            )) {
                return Err(invalid_data("quick diagnostic repeats a cell"));
            }
            if observation.workload.is_write() {
                if observation.row_count != 0 {
                    return Err(invalid_data("quick diagnostic write returned rows"));
                }
            } else {
                let identity = (
                    observation.row_count,
                    observation.result_digest.clone(),
                    observation.query_digest.clone(),
                );
                match result_identity.entry((observation.workload, observation.concurrency)) {
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(identity);
                    }
                    std::collections::btree_map::Entry::Occupied(entry)
                        if entry.get() != &identity =>
                    {
                        return Err(invalid_data("quick diagnostic read identity changed"));
                    }
                    std::collections::btree_map::Entry::Occupied(_) => {}
                }
            }
        }
        let repetitions = cells
            .iter()
            .map(|(repetition, _, _)| *repetition)
            .collect::<BTreeSet<_>>();
        if repetitions != BTreeSet::from([0, 1, 2]) || observations.len() != 18 {
            return Err(invalid_data(
                "quick diagnostic requires exactly three complete repetitions",
            ));
        }
        for repetition in 0..3 {
            for workload in Workload::ALL {
                for concurrency in [1, 8] {
                    if !cells.contains(&(repetition, workload, concurrency)) {
                        return Err(invalid_data("quick diagnostic matrix is incomplete"));
                    }
                }
            }
        }
        let summaries = summarize(&observations)
            .into_iter()
            .map(|summary| {
                let measured_duration_ns = observations
                    .iter()
                    .filter(|observation| {
                        observation.workload == summary.workload
                            && observation.concurrency == summary.concurrency
                    })
                    .map(|observation| observation.measured_duration_ns)
                    .sum::<u64>();
                let throughput_ops_per_second =
                    summary.operations as f64 * 1_000_000_000.0 / measured_duration_ns as f64;
                QuickSummary {
                    backend: summary.backend,
                    workload: summary.workload,
                    concurrency: summary.concurrency,
                    repetitions: summary.repetitions,
                    operations: summary.operations,
                    measured_duration_ns,
                    throughput_ops_per_second,
                    latency_samples: summary.latency_samples,
                    p50_ns: summary.p50_ns,
                    p95_ns: summary.p95_ns,
                    p99_ns: summary.p99_ns,
                }
            })
            .collect();
        Ok(Self {
            format_version: 1,
            backend,
            revision,
            repetitions: 3,
            observations,
            summaries,
        })
    }
}

pub fn write_quick_artifact(path: &Path, artifact: &QuickDiagnosticArtifact) -> io::Result<()> {
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "quick diagnostic output path must be absolute",
        ));
    }
    let parent = path.parent().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "quick diagnostic output has no parent directory",
        )
    })?;
    let mut output = OpenOptions::new().create_new(true).write(true).open(path)?;
    serde_json::to_writer_pretty(&mut output, artifact).map_err(io::Error::other)?;
    output.write_all(b"\n")?;
    output.sync_all()?;
    File::open(parent)?.sync_all()
}

pub fn percentile_ns(samples: &[u64], percentile: u8) -> u64 {
    assert!((1..=100).contains(&percentile));
    assert!(!samples.is_empty());
    let mut ordered = samples.to_vec();
    ordered.sort_unstable();
    let rank = (ordered.len() * usize::from(percentile)).div_ceil(100);
    ordered[rank.saturating_sub(1)]
}

pub fn summarize(observations: &[RawObservation]) -> Vec<Summary> {
    let mut groups = BTreeMap::<(Backend, Workload, usize), Vec<&RawObservation>>::new();
    for observation in observations {
        groups
            .entry((
                observation.backend,
                observation.workload,
                observation.concurrency,
            ))
            .or_default()
            .push(observation);
    }
    groups
        .into_iter()
        .map(|((backend, workload, concurrency), observations)| {
            let samples = observations
                .iter()
                .flat_map(|observation| observation.latency_samples_ns.iter().copied())
                .collect::<Vec<_>>();
            Summary {
                backend,
                workload,
                concurrency,
                repetitions: observations.len(),
                operations: observations
                    .iter()
                    .map(|observation| observation.operations)
                    .sum(),
                errors: observations
                    .iter()
                    .map(|observation| observation.errors)
                    .sum(),
                latency_samples: samples.len(),
                p50_ns: percentile_or_zero(&samples, 50),
                p95_ns: percentile_or_zero(&samples, 95),
                p99_ns: percentile_or_zero(&samples, 99),
            }
        })
        .collect()
}

fn percentile_or_zero(samples: &[u64], percentile: u8) -> u64 {
    if samples.is_empty() {
        0
    } else {
        percentile_ns(samples, percentile)
    }
}

fn shuffle<T>(values: &mut [T], seed: u64) {
    let mut state = seed;
    for index in (1..values.len()).rev() {
        let swap = (splitmix64(&mut state) as usize) % (index + 1);
        values.swap(index, swap);
    }
}

fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = *state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
