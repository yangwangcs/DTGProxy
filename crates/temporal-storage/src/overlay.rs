use std::error::Error;
use std::fmt::{self, Display, Formatter};

use crate::{TemporalStoreError, TemporalTransaction};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OverlaySavepoint(usize);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TransactionOverlay {
    transaction: TemporalTransaction,
    savepoints: Vec<TemporalTransaction>,
    maximum_operations: usize,
}

impl TransactionOverlay {
    pub fn new(maximum_operations: usize) -> Result<Self, TransactionOverlayError> {
        if maximum_operations == 0 {
            return Err(TransactionOverlayError::InvalidOperationLimit);
        }
        Ok(Self {
            transaction: TemporalTransaction::new(),
            savepoints: Vec::new(),
            maximum_operations,
        })
    }

    #[must_use]
    pub fn savepoint(&mut self) -> OverlaySavepoint {
        let savepoint = OverlaySavepoint(self.savepoints.len());
        self.savepoints.push(self.transaction.clone());
        savepoint
    }

    pub fn stage(
        &mut self,
        transaction: TemporalTransaction,
    ) -> Result<(), TransactionOverlayError> {
        let mut next = self.transaction.clone();
        next.merge_overlay(transaction)
            .map_err(TransactionOverlayError::Storage)?;
        let actual = next.operation_count();
        if actual > self.maximum_operations {
            return Err(TransactionOverlayError::OperationLimit {
                max: self.maximum_operations,
                actual,
            });
        }
        self.transaction = next;
        Ok(())
    }

    pub fn rollback_to(
        &mut self,
        savepoint: OverlaySavepoint,
    ) -> Result<(), TransactionOverlayError> {
        let transaction = self
            .savepoints
            .get(savepoint.0)
            .cloned()
            .ok_or(TransactionOverlayError::UnknownSavepoint)?;
        self.transaction = transaction;
        self.savepoints.truncate(savepoint.0 + 1);
        Ok(())
    }

    #[must_use]
    pub const fn operation_count(&self) -> usize {
        self.transaction.operation_count()
    }

    #[must_use]
    pub fn into_transaction(self) -> TemporalTransaction {
        self.transaction
    }
}

#[derive(Debug)]
pub enum TransactionOverlayError {
    InvalidOperationLimit,
    OperationLimit { max: usize, actual: usize },
    UnknownSavepoint,
    Storage(TemporalStoreError),
}

impl Display for TransactionOverlayError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidOperationLimit => {
                formatter.write_str("overlay operation limit must be nonzero")
            }
            Self::OperationLimit { max, actual } => {
                write!(
                    formatter,
                    "overlay has {actual} operations, exceeding its limit of {max}"
                )
            }
            Self::UnknownSavepoint => formatter.write_str("overlay savepoint does not exist"),
            Self::Storage(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for TransactionOverlayError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Storage(error) => Some(error),
            Self::InvalidOperationLimit | Self::OperationLimit { .. } | Self::UnknownSavepoint => {
                None
            }
        }
    }
}
