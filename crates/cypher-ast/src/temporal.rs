use crate::Expression;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ValidTimeScope {
    AsOf(Expression),
    Between { start: Expression, end: Expression },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransactionTimeScope {
    Current,
    AsOf(Expression),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TemporalContext {
    valid_time: Option<ValidTimeScope>,
    transaction_time: TransactionTimeScope,
}

impl TemporalContext {
    #[must_use]
    pub const fn new(
        valid_time: Option<ValidTimeScope>,
        transaction_time: TransactionTimeScope,
    ) -> Self {
        Self {
            valid_time,
            transaction_time,
        }
    }

    #[must_use]
    pub const fn valid_time(&self) -> Option<&ValidTimeScope> {
        self.valid_time.as_ref()
    }

    #[must_use]
    pub const fn transaction_time(&self) -> &TransactionTimeScope {
        &self.transaction_time
    }
}

impl Default for TemporalContext {
    fn default() -> Self {
        Self {
            valid_time: None,
            transaction_time: TransactionTimeScope::Current,
        }
    }
}
