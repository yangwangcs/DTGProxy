use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SampleSummary {
    pub count: usize,
    pub min: u64,
    pub max: u64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub mean: f64,
    pub median: f64,
    pub sample_standard_deviation: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RepetitionSummary {
    pub count: usize,
    pub mean: f64,
    pub median: f64,
    pub sample_standard_deviation: f64,
    pub ci95_lower: f64,
    pub ci95_upper: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatisticsError {
    EmptySamples,
    TooFewRepetitions,
    WrongFormalRepetitionCount,
    NonFiniteValue,
}

impl Display for StatisticsError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptySamples => formatter.write_str("samples are empty"),
            Self::TooFewRepetitions => {
                formatter.write_str("at least five repetitions are required")
            }
            Self::WrongFormalRepetitionCount => {
                formatter.write_str("formal statistics require exactly five repetitions")
            }
            Self::NonFiniteValue => {
                formatter.write_str("statistics input contains a non-finite value")
            }
        }
    }
}

impl std::error::Error for StatisticsError {}

pub fn summarize_samples(samples: &[u64]) -> Result<SampleSummary, StatisticsError> {
    if samples.is_empty() {
        return Err(StatisticsError::EmptySamples);
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let floating: Vec<_> = sorted.iter().map(|value| *value as f64).collect();

    Ok(SampleSummary {
        count: sorted.len(),
        min: sorted[0],
        max: sorted[sorted.len() - 1],
        p50: nearest_rank(&sorted, 50),
        p95: nearest_rank(&sorted, 95),
        p99: nearest_rank(&sorted, 99),
        mean: mean(&floating),
        median: median(&floating),
        sample_standard_deviation: sample_standard_deviation(&floating),
    })
}

pub fn summarize_repetitions(values: &[f64]) -> Result<RepetitionSummary, StatisticsError> {
    if values.len() < 5 {
        return Err(StatisticsError::TooFewRepetitions);
    }
    if values.iter().any(|value| !value.is_finite()) {
        return Err(StatisticsError::NonFiniteValue);
    }

    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let mean = mean(&sorted);
    let standard_deviation = sample_standard_deviation(&sorted);
    let critical = student_t_critical_95(sorted.len() - 1);
    let margin = critical * standard_deviation / (sorted.len() as f64).sqrt();

    Ok(RepetitionSummary {
        count: sorted.len(),
        mean,
        median: median(&sorted),
        sample_standard_deviation: standard_deviation,
        ci95_lower: mean - margin,
        ci95_upper: mean + margin,
    })
}

pub fn summarize_formal_repetitions(values: &[f64]) -> Result<RepetitionSummary, StatisticsError> {
    if values.len() != 5 {
        return Err(StatisticsError::WrongFormalRepetitionCount);
    }
    summarize_repetitions(values)
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    let rank = sorted.len().saturating_mul(percentile).saturating_add(99) / 100;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len() as f64
}

fn median(sorted: &[f64]) -> f64 {
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn sample_standard_deviation(values: &[f64]) -> f64 {
    if values.len() == 1 {
        return 0.0;
    }
    let mean = mean(values);
    let sum_of_squares = values
        .iter()
        .map(|value| {
            let difference = value - mean;
            difference * difference
        })
        .sum::<f64>();
    (sum_of_squares / (values.len() - 1) as f64).sqrt()
}

fn student_t_critical_95(degrees_of_freedom: usize) -> f64 {
    const VALUES: [f64; 30] = [
        12.706_204_736,
        4.302_652_73,
        3.182_446_305,
        2.776_445_105,
        2.570_581_836,
        2.446_911_851,
        2.364_624_252,
        2.306_004_135,
        2.262_157_163,
        2.228_138_852,
        2.200_985_16,
        2.178_812_83,
        2.160_368_656,
        2.144_786_688,
        2.131_449_546,
        2.119_905_299,
        2.109_815_578,
        2.100_922_04,
        2.093_024_054,
        2.085_963_447,
        2.079_613_845,
        2.073_873_068,
        2.068_657_61,
        2.063_898_562,
        2.059_538_553,
        2.055_529_439,
        2.051_830_516,
        2.048_407_142,
        2.045_229_642,
        2.042_272_456,
    ];
    if let Some(value) = VALUES.get(degrees_of_freedom.saturating_sub(1)) {
        return *value;
    }

    // Cornish-Fisher expansion around the two-sided 95% normal quantile.
    let degrees_of_freedom = degrees_of_freedom as f64;
    let z = 1.959_963_984_540_054;
    let z2 = z * z;
    let z3 = z2 * z;
    let z5 = z3 * z2;
    let z7 = z5 * z2;
    z + (z3 + z) / (4.0 * degrees_of_freedom)
        + (5.0 * z5 + 16.0 * z3 + 3.0 * z) / (96.0 * degrees_of_freedom.powi(2))
        + (3.0 * z7 + 19.0 * z5 + 17.0 * z3 - 15.0 * z) / (384.0 * degrees_of_freedom.powi(3))
}
