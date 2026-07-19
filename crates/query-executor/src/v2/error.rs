use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_ir::v2::{SlotId, ValueType};

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
    UnsupportedOperator(&'static str),
    Cancelled,
    DeadlineExceeded,
}

impl Display for RuntimeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "physical query execution failed: {self:?}")
    }
}

impl Error for RuntimeError {}
