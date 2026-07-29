use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use dtg_storage::ShardId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryError {
    InvalidBatch(String),
    InvalidPlan(String),
    RowBudget,
    ScanBudget,
    MemoryBudget,
    NetworkBudget,
    SpillBudget,
    Cancelled,
    Deadline,
    SnapshotDrift,
    CapabilityDrift,
    MissingStorage(ShardId),
    MissingPushdown,
    ProviderViolation(String),
    Unsupported(String),
    Storage(String),
}

impl QueryError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::InvalidBatch(_) => "DTG-QUERY-BATCH",
            Self::InvalidPlan(_) => "DTG-QUERY-PLAN",
            Self::RowBudget => "DTG-QUERY-ROW-BUDGET",
            Self::ScanBudget => "DTG-QUERY-SCAN-BUDGET",
            Self::MemoryBudget => "DTG-QUERY-MEMORY-BUDGET",
            Self::NetworkBudget => "DTG-QUERY-NETWORK-BUDGET",
            Self::SpillBudget => "DTG-QUERY-SPILL-BUDGET",
            Self::Cancelled => "DTG-QUERY-CANCELLED",
            Self::Deadline => "DTG-QUERY-DEADLINE",
            Self::SnapshotDrift => "DTG-QUERY-SNAPSHOT-DRIFT",
            Self::CapabilityDrift => "DTG-QUERY-CAPABILITY-DRIFT",
            Self::MissingStorage(_) => "DTG-QUERY-STORAGE-MISSING",
            Self::MissingPushdown => "DTG-QUERY-PUSHDOWN-MISSING",
            Self::ProviderViolation(_) => "DTG-QUERY-PROVIDER-VIOLATION",
            Self::Unsupported(_) => "DTG-QUERY-UNSUPPORTED",
            Self::Storage(_) => "DTG-QUERY-STORAGE",
        }
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBatch(message)
            | Self::InvalidPlan(message)
            | Self::ProviderViolation(message)
            | Self::Unsupported(message)
            | Self::Storage(message) => write!(formatter, "{}: {message}", self.code()),
            Self::MissingStorage(shard_id) => {
                write!(formatter, "{}: Shard {}", self.code(), shard_id.get())
            }
            _ => formatter.write_str(self.code()),
        }
    }
}

impl std::error::Error for QueryError {}

impl From<dtg_storage::StorageError> for QueryError {
    fn from(value: dtg_storage::StorageError) -> Self {
        Self::Storage(value.to_string())
    }
}

#[derive(Clone, Debug)]
pub struct QueryBudget {
    pub max_rows: u64,
    pub max_scan_bytes: u64,
    pub max_memory_bytes: u64,
    pub max_network_bytes: u64,
    pub max_spill_bytes: u64,
    pub deadline: Instant,
}

impl QueryBudget {
    pub fn unlimited() -> Self {
        Self {
            max_rows: u64::MAX,
            max_scan_bytes: u64::MAX,
            max_memory_bytes: u64::MAX,
            max_network_bytes: u64::MAX,
            max_spill_bytes: u64::MAX,
            deadline: Instant::now() + Duration::from_secs(100 * 365 * 24 * 60 * 60),
        }
    }

    pub fn rows(max_rows: u64) -> Self {
        Self {
            max_rows,
            ..Self::unlimited()
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CancellationToken {
    cancelled: Arc<AtomicBool>,
}

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

#[derive(Debug)]
pub struct QueryContext {
    budget: QueryBudget,
    cancellation: CancellationToken,
    rows: u64,
    scan_bytes: u64,
    memory_bytes: u64,
    network_bytes: u64,
    spill_bytes: u64,
}

impl QueryContext {
    pub const fn new(budget: QueryBudget, cancellation: CancellationToken) -> Self {
        Self {
            budget,
            cancellation,
            rows: 0,
            scan_bytes: 0,
            memory_bytes: 0,
            network_bytes: 0,
            spill_bytes: 0,
        }
    }

    pub fn checkpoint(&self) -> Result<(), QueryError> {
        if self.cancellation.is_cancelled() {
            return Err(QueryError::Cancelled);
        }
        if Instant::now() > self.budget.deadline {
            return Err(QueryError::Deadline);
        }
        Ok(())
    }

    pub fn remaining_rows(&self) -> u64 {
        self.budget.max_rows.saturating_sub(self.rows)
    }

    pub fn charge_rows(&mut self, rows: u64) -> Result<(), QueryError> {
        self.rows = charge(self.rows, rows, self.budget.max_rows, QueryError::RowBudget)?;
        Ok(())
    }

    pub fn charge_scan_bytes(&mut self, bytes: u64) -> Result<(), QueryError> {
        self.scan_bytes = charge(
            self.scan_bytes,
            bytes,
            self.budget.max_scan_bytes,
            QueryError::ScanBudget,
        )?;
        Ok(())
    }

    pub fn charge_memory(&mut self, bytes: u64) -> Result<(), QueryError> {
        self.memory_bytes = charge(
            self.memory_bytes,
            bytes,
            self.budget.max_memory_bytes,
            QueryError::MemoryBudget,
        )?;
        Ok(())
    }

    pub const fn memory_bytes(&self) -> u64 {
        self.memory_bytes
    }

    pub fn release_memory(&mut self, bytes: u64) {
        self.memory_bytes = self.memory_bytes.saturating_sub(bytes);
    }

    pub fn charge_network(&mut self, bytes: u64) -> Result<(), QueryError> {
        self.network_bytes = charge(
            self.network_bytes,
            bytes,
            self.budget.max_network_bytes,
            QueryError::NetworkBudget,
        )?;
        Ok(())
    }

    pub fn charge_spill(&mut self, bytes: u64) -> Result<(), QueryError> {
        self.spill_bytes = charge(
            self.spill_bytes,
            bytes,
            self.budget.max_spill_bytes,
            QueryError::SpillBudget,
        )?;
        Ok(())
    }
}

fn charge(current: u64, amount: u64, maximum: u64, error: QueryError) -> Result<u64, QueryError> {
    let charged = current.checked_add(amount).ok_or_else(|| error.clone())?;
    if charged > maximum {
        Err(error)
    } else {
        Ok(charged)
    }
}
