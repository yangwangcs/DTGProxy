use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// A half-open interval `[start, end)`. A missing end represents positive infinity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Interval<T> {
    start: T,
    end: Option<T>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IntervalError {
    EmptyOrReversed,
}

impl Display for IntervalError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyOrReversed => formatter.write_str("interval end must be greater than start"),
        }
    }
}

impl Error for IntervalError {}

impl<T> Interval<T>
where
    T: Copy + Ord,
{
    pub fn new(start: T, end: Option<T>) -> Result<Self, IntervalError> {
        if end.is_some_and(|end| end <= start) {
            return Err(IntervalError::EmptyOrReversed);
        }
        Ok(Self { start, end })
    }

    #[must_use]
    pub const fn forever_from(start: T) -> Self {
        Self { start, end: None }
    }

    #[must_use]
    pub const fn start(&self) -> T {
        self.start
    }

    #[must_use]
    pub const fn end(&self) -> Option<T> {
        self.end
    }

    #[must_use]
    pub fn contains(&self, value: T) -> bool {
        value >= self.start && self.end.is_none_or(|end| value < end)
    }

    #[must_use]
    pub fn overlaps(&self, other: &Self) -> bool {
        self.end.is_none_or(|end| other.start < end) && other.end.is_none_or(|end| self.start < end)
    }

    #[must_use]
    pub fn subtract(&self, cut: &Self) -> Vec<Self> {
        if !self.overlaps(cut) {
            return vec![*self];
        }

        let mut residuals = Vec::with_capacity(2);
        if self.start < cut.start {
            residuals.push(Self {
                start: self.start,
                end: Some(cut.start),
            });
        }

        if let Some(cut_end) = cut.end
            && self.end.is_none_or(|end| cut_end < end)
        {
            residuals.push(Self {
                start: cut_end,
                end: self.end,
            });
        }

        residuals
    }
}
