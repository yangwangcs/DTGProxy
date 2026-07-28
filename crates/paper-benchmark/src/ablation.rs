use std::error::Error;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::ContractError;

pub use query_executor::{
    AblationAxis, BenchmarkAblationConfig, BenchmarkAblationCounters,
    BenchmarkAblationCountersSnapshot,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AblationConfig {
    pub native_pushdown: bool,
    pub column_batches: bool,
    pub bounded_lazy_pages: bool,
    pub parallel_shard_fanout: bool,
    pub batched_property_gather: bool,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AblationCounters {
    pub canonical_residual_scans: u64,
    pub row_column_conversion_boundaries: u64,
    pub eager_page_collections: u64,
    pub serial_shard_opens: u64,
    pub singleton_property_gather_reads: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AblationEvidence {
    pub gateway_pid: u32,
    pub cell_id: String,
    pub configuration_digest: String,
    pub config: AblationConfig,
    pub total_operations: u64,
    pub queries_started: u64,
    pub queries_completed: u64,
    pub queries_failed: u64,
    pub queries_in_flight: u64,
    pub counters: AblationCounters,
}

pub fn ablation_config_for_label(label: &str) -> Result<AblationConfig, ContractError> {
    let mut config = AblationConfig {
        native_pushdown: true,
        column_batches: true,
        bounded_lazy_pages: true,
        parallel_shard_fanout: true,
        batched_property_gather: true,
    };
    match label {
        "production" => {}
        "no_native_pushdown" => config.native_pushdown = false,
        "no_column_batch" => config.column_batches = false,
        "no_lazy_pages" => config.bounded_lazy_pages = false,
        "no_parallel_fanout" => config.parallel_shard_fanout = false,
        "no_batched_gather" => config.batched_property_gather = false,
        _ => return Err(ContractError::InvalidField("ablation")),
    }
    Ok(config)
}

impl AblationEvidence {
    pub(crate) fn validate(
        &self,
        label: &str,
        configuration_digest: &str,
        measured_operations: u64,
    ) -> Result<(), ContractError> {
        if self.gateway_pid == 0
            || self.cell_id.trim().is_empty()
            || self.configuration_digest != configuration_digest
            || self.config != ablation_config_for_label(label)?
            || self.total_operations < measured_operations
            || self.queries_started != self.total_operations
            || self.queries_completed != self.total_operations
            || self.queries_failed != 0
            || self.queries_in_flight != 0
        {
            return Err(ContractError::InvalidField("ablation_evidence"));
        }
        let counts = [
            self.counters.canonical_residual_scans,
            self.counters.row_column_conversion_boundaries,
            self.counters.eager_page_collections,
            self.counters.serial_shard_opens,
            self.counters.singleton_property_gather_reads,
        ];
        let expected_nonzero = match label {
            "production" => None,
            "no_native_pushdown" => Some(0),
            "no_column_batch" => Some(1),
            "no_lazy_pages" => Some(2),
            "no_parallel_fanout" => Some(3),
            "no_batched_gather" => Some(4),
            _ => return Err(ContractError::InvalidField("ablation")),
        };
        if counts.iter().enumerate().any(|(index, count)| {
            Some(index) == expected_nonzero && *count == 0
                || Some(index) != expected_nonzero && *count != 0
        }) {
            return Err(ContractError::InvalidField("ablation_evidence.counters"));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct UnsupportedAblation {
    axis: AblationAxis,
}

impl UnsupportedAblation {
    #[must_use]
    pub const fn axis(self) -> AblationAxis {
        self.axis
    }
}

impl Display for UnsupportedAblation {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "benchmark fixture did not exercise the {:?} alternate path",
            self.axis
        )
    }
}

impl Error for UnsupportedAblation {}

pub fn validate_ablation_exercised(
    config: BenchmarkAblationConfig,
    counters: BenchmarkAblationCountersSnapshot,
) -> Result<(), UnsupportedAblation> {
    let checks = [
        (
            config.native_pushdown,
            counters.canonical_residual_scans(),
            AblationAxis::NativePushdown,
        ),
        (
            config.column_batches,
            counters.row_column_conversion_boundaries(),
            AblationAxis::ColumnBatches,
        ),
        (
            config.bounded_lazy_pages,
            counters.eager_page_collections(),
            AblationAxis::BoundedLazyPages,
        ),
        (
            config.parallel_shard_fanout,
            counters.serial_shard_opens(),
            AblationAxis::ParallelShardFanout,
        ),
        (
            config.batched_property_gather,
            counters.singleton_property_gather_reads(),
            AblationAxis::BatchedPropertyGather,
        ),
    ];
    for (enabled, count, axis) in checks {
        if !enabled && count == 0 {
            return Err(UnsupportedAblation { axis });
        }
    }
    Ok(())
}
