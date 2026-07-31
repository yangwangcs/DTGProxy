use std::collections::BTreeMap;

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
