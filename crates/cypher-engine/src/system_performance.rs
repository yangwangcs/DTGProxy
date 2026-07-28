use std::fmt::{self, Display, Formatter};

use crate::{
    LatencyPercentiles, QueryMetricAvailability, QueryOverheadGateError, QueryScopedOverhead,
    QueryScopedOverheadLimits,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PerformanceScenario {
    PointLookup,
    SingleShardScan,
    MultiShardScan,
    ChangeQuery,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SystemPerformanceObservation {
    node_count: u8,
    result_digest: Option<[u8; 32]>,
    row_count: Option<usize>,
    latency_micros: LatencyPercentiles,
    ttfr_micros: LatencyPercentiles,
    throughput_rows_per_second: u64,
    peak_memory_bytes: u64,
    payload_bytes: u64,
    exchange_bytes: u64,
    query_scoped_overhead: QueryScopedOverhead,
}

impl SystemPerformanceObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_count: u8,
        latency_micros: LatencyPercentiles,
        ttfr_micros: LatencyPercentiles,
        throughput_rows_per_second: u64,
        peak_memory_bytes: u64,
        payload_bytes: u64,
        exchange_bytes: u64,
        query_scoped_overhead: QueryScopedOverhead,
    ) -> Result<Self, SystemPerformanceGateError> {
        if node_count == 0
            || latency_micros.sample_count() == 0
            || ttfr_micros.sample_count() == 0
            || throughput_rows_per_second == 0
            || peak_memory_bytes == 0
            || ttfr_micros.p50() > latency_micros.p50()
            || ttfr_micros.p95() > latency_micros.p95()
            || ttfr_micros.p99() > latency_micros.p99()
        {
            return Err(SystemPerformanceGateError::InvalidObservation);
        }
        Ok(Self {
            node_count,
            result_digest: None,
            row_count: None,
            latency_micros,
            ttfr_micros,
            throughput_rows_per_second,
            peak_memory_bytes,
            payload_bytes,
            exchange_bytes,
            query_scoped_overhead,
        })
    }

    pub fn with_result_identity(
        mut self,
        result_digest: [u8; 32],
        row_count: usize,
    ) -> Result<Self, SystemPerformanceGateError> {
        if result_digest == [0; 32] || row_count == 0 {
            return Err(SystemPerformanceGateError::InvalidResultIdentity);
        }
        self.result_digest = Some(result_digest);
        self.row_count = Some(row_count);
        Ok(self)
    }

    #[must_use]
    pub const fn node_count(&self) -> u8 {
        self.node_count
    }

    #[must_use]
    pub const fn latency_micros(&self) -> LatencyPercentiles {
        self.latency_micros
    }

    #[must_use]
    pub const fn ttfr_micros(&self) -> LatencyPercentiles {
        self.ttfr_micros
    }

    #[must_use]
    pub const fn throughput_rows_per_second(&self) -> u64 {
        self.throughput_rows_per_second
    }

    #[must_use]
    pub const fn peak_memory_bytes(&self) -> u64 {
        self.peak_memory_bytes
    }

    #[must_use]
    pub const fn payload_bytes(&self) -> u64 {
        self.payload_bytes
    }

    #[must_use]
    pub const fn exchange_bytes(&self) -> u64 {
        self.exchange_bytes
    }

    #[must_use]
    pub const fn query_scoped_overhead(&self) -> &QueryScopedOverhead {
        &self.query_scoped_overhead
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PairedSystemPerformanceReport {
    scenario: PerformanceScenario,
    direct: SystemPerformanceObservation,
    proxy: SystemPerformanceObservation,
}

impl PairedSystemPerformanceReport {
    pub fn new(
        scenario: PerformanceScenario,
        direct: SystemPerformanceObservation,
        proxy: SystemPerformanceObservation,
    ) -> Result<Self, SystemPerformanceGateError> {
        if direct.node_count != proxy.node_count {
            return Err(SystemPerformanceGateError::MismatchedNodeCount {
                direct: direct.node_count,
                proxy: proxy.node_count,
            });
        }
        match (
            direct.result_digest,
            direct.row_count,
            proxy.result_digest,
            proxy.row_count,
        ) {
            (Some(direct_digest), Some(direct_rows), Some(proxy_digest), Some(proxy_rows))
                if direct_digest == proxy_digest && direct_rows == proxy_rows => {}
            (Some(_), Some(_), Some(_), Some(_)) => {
                return Err(SystemPerformanceGateError::ResultMismatch);
            }
            _ => return Err(SystemPerformanceGateError::ResultIdentityUnavailable),
        }
        Ok(Self {
            scenario,
            direct,
            proxy,
        })
    }

    #[must_use]
    pub const fn direct(&self) -> &SystemPerformanceObservation {
        &self.direct
    }

    #[must_use]
    pub const fn proxy(&self) -> &SystemPerformanceObservation {
        &self.proxy
    }

    pub fn evaluate_release(
        &self,
        memory_budget_bytes: u64,
        query_limits: &QueryScopedOverheadLimits,
    ) -> Result<(), SystemPerformanceGateError> {
        if memory_budget_bytes == 0 {
            return Err(SystemPerformanceGateError::InvalidMemoryBudget);
        }
        self.proxy
            .query_scoped_overhead
            .evaluate(query_limits)
            .map_err(SystemPerformanceGateError::QueryOverhead)?;

        match self.scenario {
            PerformanceScenario::PointLookup => {
                evaluate_latency_overhead(
                    "point lookup p50",
                    self.direct.latency_micros.p50(),
                    self.proxy.latency_micros.p50(),
                    75,
                    15,
                )?;
                evaluate_latency_overhead(
                    "point lookup p99",
                    self.direct.latency_micros.p99(),
                    self.proxy.latency_micros.p99(),
                    300,
                    15,
                )?;
            }
            PerformanceScenario::SingleShardScan => {
                evaluate_throughput(
                    self.direct.throughput_rows_per_second,
                    self.proxy.throughput_rows_per_second,
                    90,
                )?;
            }
            PerformanceScenario::MultiShardScan | PerformanceScenario::ChangeQuery => {
                evaluate_throughput(
                    self.direct.throughput_rows_per_second,
                    self.proxy.throughput_rows_per_second,
                    85,
                )?;
            }
        }

        let (ttfr_floor, ttfr_percent) = match self.scenario {
            PerformanceScenario::PointLookup | PerformanceScenario::SingleShardScan => (250, 15),
            PerformanceScenario::MultiShardScan | PerformanceScenario::ChangeQuery => (1_000, 20),
        };
        evaluate_latency_overhead(
            "first result p99",
            self.direct.ttfr_micros.p99(),
            self.proxy.ttfr_micros.p99(),
            ttfr_floor,
            ttfr_percent,
        )?;

        let baseline_memory_limit = self
            .direct
            .peak_memory_bytes
            .saturating_add(percent_ceil(self.direct.peak_memory_bytes, 5));
        let memory_limit = memory_budget_bytes.min(baseline_memory_limit);
        if self.proxy.peak_memory_bytes > memory_limit {
            return Err(SystemPerformanceGateError::PeakMemoryExceeded {
                observed: self.proxy.peak_memory_bytes,
                limit: memory_limit,
            });
        }

        let sent_frames = match self.proxy.query_scoped_overhead.sent_frame_count() {
            QueryMetricAvailability::Observed(sent_frames) => *sent_frames,
            QueryMetricAvailability::Unavailable(reason) => {
                return Err(SystemPerformanceGateError::SentFrameCountUnavailable(
                    *reason,
                ));
            }
        };
        let exchange_limit = sent_frames
            .saturating_mul(256)
            .saturating_add(percent_ceil(self.proxy.payload_bytes, 3));
        if self.proxy.exchange_bytes > exchange_limit {
            return Err(SystemPerformanceGateError::ExchangeBytesExceeded {
                observed: self.proxy.exchange_bytes,
                limit: exchange_limit,
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScaleOutPerformance {
    one_node_rows_per_second: u64,
    four_node_rows_per_second: u64,
    eight_node_rows_per_second: u64,
}

impl ScaleOutPerformance {
    pub fn from_observations(
        one_node: &SystemPerformanceObservation,
        four_node: &SystemPerformanceObservation,
        eight_node: &SystemPerformanceObservation,
    ) -> Result<Self, SystemPerformanceGateError> {
        if one_node.node_count != 1 || four_node.node_count != 4 || eight_node.node_count != 8 {
            return Err(SystemPerformanceGateError::InvalidObservation);
        }
        match (
            one_node.result_digest,
            one_node.row_count,
            four_node.result_digest,
            four_node.row_count,
            eight_node.result_digest,
            eight_node.row_count,
        ) {
            (
                Some(one_digest),
                Some(one_rows),
                Some(four_digest),
                Some(four_rows),
                Some(eight_digest),
                Some(eight_rows),
            ) if one_digest == four_digest
                && one_digest == eight_digest
                && one_rows == four_rows
                && one_rows == eight_rows => {}
            (Some(_), Some(_), Some(_), Some(_), Some(_), Some(_)) => {
                return Err(SystemPerformanceGateError::ResultMismatch);
            }
            _ => return Err(SystemPerformanceGateError::ResultIdentityUnavailable),
        }
        Self::new(
            one_node.throughput_rows_per_second,
            four_node.throughput_rows_per_second,
            eight_node.throughput_rows_per_second,
        )
    }

    pub fn new(
        one_node_rows_per_second: u64,
        four_node_rows_per_second: u64,
        eight_node_rows_per_second: u64,
    ) -> Result<Self, SystemPerformanceGateError> {
        if one_node_rows_per_second == 0
            || four_node_rows_per_second == 0
            || eight_node_rows_per_second == 0
        {
            return Err(SystemPerformanceGateError::InvalidObservation);
        }
        Ok(Self {
            one_node_rows_per_second,
            four_node_rows_per_second,
            eight_node_rows_per_second,
        })
    }

    pub fn evaluate_release(&self) -> Result<(), SystemPerformanceGateError> {
        if u128::from(self.four_node_rows_per_second) * 10
            < u128::from(self.one_node_rows_per_second) * 28
        {
            return Err(SystemPerformanceGateError::ScaleOutBelowLimit {
                nodes: 4,
                observed_rows_per_second: self.four_node_rows_per_second,
                required_rows_per_second: ratio_ceil(self.one_node_rows_per_second, 28, 10),
            });
        }
        if u128::from(self.eight_node_rows_per_second) * 10
            < u128::from(self.one_node_rows_per_second) * 50
        {
            return Err(SystemPerformanceGateError::ScaleOutBelowLimit {
                nodes: 8,
                observed_rows_per_second: self.eight_node_rows_per_second,
                required_rows_per_second: self.one_node_rows_per_second.saturating_mul(5),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SystemPerformanceGateError {
    InvalidObservation,
    InvalidResultIdentity,
    ResultIdentityUnavailable,
    ResultMismatch,
    InvalidMemoryBudget,
    MismatchedNodeCount {
        direct: u8,
        proxy: u8,
    },
    QueryOverhead(QueryOverheadGateError),
    LatencyOverheadExceeded {
        metric: &'static str,
        observed_micros: u64,
        limit_micros: u64,
    },
    ThroughputBelowLimit {
        observed_rows_per_second: u64,
        required_rows_per_second: u64,
    },
    PeakMemoryExceeded {
        observed: u64,
        limit: u64,
    },
    SentFrameCountUnavailable(crate::QueryMetricUnavailableReason),
    ExchangeBytesExceeded {
        observed: u64,
        limit: u64,
    },
    ScaleOutBelowLimit {
        nodes: u8,
        observed_rows_per_second: u64,
        required_rows_per_second: u64,
    },
}

impl Display for SystemPerformanceGateError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidObservation => {
                formatter.write_str("invalid system performance observation")
            }
            Self::InvalidResultIdentity => formatter.write_str("invalid result identity"),
            Self::ResultIdentityUnavailable => {
                formatter.write_str("result identity is required for performance comparison")
            }
            Self::ResultMismatch => {
                formatter.write_str("direct and proxy result identities differ")
            }
            Self::InvalidMemoryBudget => formatter.write_str("memory budget must be non-zero"),
            Self::MismatchedNodeCount { direct, proxy } => {
                write!(
                    formatter,
                    "direct node count {direct} differs from proxy {proxy}"
                )
            }
            Self::QueryOverhead(error) => write!(formatter, "query overhead gate failed: {error}"),
            Self::LatencyOverheadExceeded {
                metric,
                observed_micros,
                limit_micros,
            } => write!(
                formatter,
                "{metric} exceeded: observed {observed_micros} us, limit {limit_micros} us"
            ),
            Self::ThroughputBelowLimit {
                observed_rows_per_second,
                required_rows_per_second,
            } => write!(
                formatter,
                "throughput below limit: observed {observed_rows_per_second} rows/s, required {required_rows_per_second} rows/s"
            ),
            Self::PeakMemoryExceeded { observed, limit } => {
                write!(
                    formatter,
                    "peak memory exceeded: observed {observed}, limit {limit}"
                )
            }
            Self::SentFrameCountUnavailable(reason) => {
                write!(formatter, "sent-frame count unavailable: {reason:?}")
            }
            Self::ExchangeBytesExceeded { observed, limit } => write!(
                formatter,
                "exchange bytes exceeded: observed {observed}, limit {limit}"
            ),
            Self::ScaleOutBelowLimit {
                nodes,
                observed_rows_per_second,
                required_rows_per_second,
            } => write!(
                formatter,
                "{nodes}-node throughput below limit: observed {observed_rows_per_second} rows/s, required {required_rows_per_second} rows/s"
            ),
        }
    }
}

impl std::error::Error for SystemPerformanceGateError {}

fn evaluate_latency_overhead(
    metric: &'static str,
    direct_micros: u64,
    proxy_micros: u64,
    floor_micros: u64,
    percent: u64,
) -> Result<(), SystemPerformanceGateError> {
    let limit =
        direct_micros.saturating_add(floor_micros.max(percent_ceil(direct_micros, percent)));
    if proxy_micros > limit {
        return Err(SystemPerformanceGateError::LatencyOverheadExceeded {
            metric,
            observed_micros: proxy_micros,
            limit_micros: limit,
        });
    }
    Ok(())
}

fn evaluate_throughput(
    direct_rows_per_second: u64,
    proxy_rows_per_second: u64,
    minimum_percent: u64,
) -> Result<(), SystemPerformanceGateError> {
    let required = percent_ceil(direct_rows_per_second, minimum_percent);
    if proxy_rows_per_second < required {
        return Err(SystemPerformanceGateError::ThroughputBelowLimit {
            observed_rows_per_second: proxy_rows_per_second,
            required_rows_per_second: required,
        });
    }
    Ok(())
}

fn percent_ceil(value: u64, percent: u64) -> u64 {
    ratio_ceil(value, percent, 100)
}

fn ratio_ceil(value: u64, numerator: u64, denominator: u64) -> u64 {
    let ratio =
        (u128::from(value) * u128::from(numerator)).div_ceil(u128::from(denominator.max(1)));
    u64::try_from(ratio).unwrap_or(u64::MAX)
}
