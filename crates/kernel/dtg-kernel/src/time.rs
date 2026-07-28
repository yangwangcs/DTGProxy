use crate::KernelError;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct TransactionTime(i64);

impl TransactionTime {
    pub fn new(value: i64) -> Result<Self, KernelError> {
        (value >= 0)
            .then_some(Self(value))
            .ok_or(KernelError::NegativeTransactionTime(value))
    }

    pub const fn get(self) -> i64 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ValidInterval {
    start: i64,
    end: i64,
}

impl ValidInterval {
    pub fn new(start: i64, end: i64) -> Result<Self, KernelError> {
        (start < end)
            .then_some(Self { start, end })
            .ok_or(KernelError::InvalidInterval { start, end })
    }

    pub const fn start(self) -> i64 {
        self.start
    }

    pub const fn end(self) -> i64 {
        self.end
    }

    pub const fn overlaps(self, other: Self) -> bool {
        self.start < other.end && other.start < self.end
    }
}
