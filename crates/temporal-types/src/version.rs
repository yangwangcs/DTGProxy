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
    pub fn is_visible_at(&self, valid_time: ValidTime, transaction_time: TransactionTime) -> bool {
        self.valid.contains(valid_time) && self.transaction.contains(transaction_time)
    }
}
