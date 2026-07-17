#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ValidTime(i64);

impl ValidTime {
    #[must_use]
    pub const fn from_micros(micros: i64) -> Self {
        Self(micros)
    }

    #[must_use]
    pub const fn as_micros(self) -> i64 {
        self.0
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

    #[must_use]
    pub const fn physical_micros(self) -> i64 {
        self.physical_micros
    }

    #[must_use]
    pub const fn logical(self) -> u32 {
        self.logical
    }
}
