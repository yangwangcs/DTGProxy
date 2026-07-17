#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ValidTime(i64);

impl ValidTime {
    #[must_use]
    pub const fn from_micros(micros: i64) -> Self {
        Self(micros)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TransactionTime {
    physical_micros: i64,
    logical: u32,
}

impl TransactionTime {
    #[must_use]
    pub const fn new(physical_micros: i64, logical: u32) -> Self {
        Self {
            physical_micros,
            logical,
        }
    }
}
