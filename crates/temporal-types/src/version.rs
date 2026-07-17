use crate::{Interval, TransactionTime, ValidTime};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BitemporalVersion<T> {
    value: T,
    valid: Interval<ValidTime>,
    transaction: Interval<TransactionTime>,
}

impl<T> BitemporalVersion<T> {
    #[must_use]
    pub const fn new(
        value: T,
        valid: Interval<ValidTime>,
        transaction: Interval<TransactionTime>,
    ) -> Self {
        Self {
            value,
            valid,
            transaction,
        }
    }

    #[must_use]
    pub const fn value(&self) -> &T {
        &self.value
    }

    #[must_use]
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }

    #[must_use]
    pub const fn transaction(&self) -> Interval<TransactionTime> {
        self.transaction
    }

    #[must_use]
    pub fn is_visible_at(&self, valid_time: ValidTime, transaction_time: TransactionTime) -> bool {
        self.valid.contains(valid_time) && self.transaction.contains(transaction_time)
    }

    pub fn close_transaction_at(
        &mut self,
        transaction_time: TransactionTime,
    ) -> Result<(), crate::IntervalError> {
        self.transaction = Interval::new(self.transaction.start(), Some(transaction_time))?;
        Ok(())
    }
}
