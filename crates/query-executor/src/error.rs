use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_ir::{SlotId, ValueType};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RuntimeError {
    BatchTooLarge {
        max: usize,
        actual: usize,
    },
    InvalidBatchRows(usize),
    RowWidth {
        expected: usize,
        actual: usize,
    },
    NullInNonNullableColumn {
        slot: SlotId,
    },
    TypeMismatch {
        expected: ValueType,
        actual: &'static str,
    },
    MissingSlot(SlotId),
    MissingParameter(String),
    InvalidPredicate(&'static str),
    InvalidRowCount,
    ArithmeticOverflow,
    DivisionByZero,
    SizeOverflow,
    MemoryLimitExceeded {
        limit: u64,
        required: u64,
    },
    OutputSchemaMismatch,
    FunctionUnsupported(u32),
    ProcedureRuntimeMissing,
    ProcedureInvocationLimit {
        max: u32,
    },
    ProcedureInputRowLimit {
        max: u64,
    },
    ProcedureOutputRowLimit {
        max: u64,
    },
    ProcedureValueUnsupported(&'static str),
    ProcedureFailed(String),
    InvalidPhysicalPlan,
    CapabilityGenerationMismatch,
    ApplyInvocationLimit {
        max: u64,
    },
    ApplyOutputRowLimit {
        max: u64,
    },
    ChildInvocationFailed,
    MissingShards(Vec<u32>),
    ChildSnapshotMismatch,
    ChildCapabilityGenerationMismatch,
    ChildSecurityMismatch,
    RecursivePlanViolation,
    ChildIncompleteShards(Vec<u32>),
    ChildWorkerIdentityMismatch,
    ChildFragmentMismatch,
    ChildStorageFailure,
    ChildTransportBudgetExceeded(&'static str),
    ChildTransportProtocolViolation(&'static str),
    UnsupportedOperator(&'static str),
    InvalidTemporalValue(&'static str),
    InvalidTemporalInterval,
    Cancelled,
    DeadlineExceeded,
}

impl Display for RuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "physical query execution failed: {self:?}")
    }
}

impl Error for RuntimeError {}
