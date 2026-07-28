use core::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum KernelError {
    ZeroIdentifier,
    NegativeTransactionTime(i64),
    InvalidInterval { start: i64, end: i64 },
}

impl fmt::Display for KernelError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroIdentifier => formatter.write_str("identifier must be nonzero"),
            Self::NegativeTransactionTime(value) => {
                write!(formatter, "transaction time must be nonnegative: {value}")
            }
            Self::InvalidInterval { start, end } => {
                write!(
                    formatter,
                    "interval start must precede end: {start} >= {end}"
                )
            }
        }
    }
}

impl std::error::Error for KernelError {}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum ErrorClass {
    InvalidInput,
    Conflict,
    Unavailable,
    Timeout,
    Internal,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum RetryClass {
    Never,
    Immediate,
    Backoff,
}
