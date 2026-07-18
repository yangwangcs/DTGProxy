use storage_api::{Keyspace, LogicalKey, Mutation};

use crate::{HomeTransactionRecord, ShardEpoch, TransactionId, TransactionState, TxnProtocolError};

const HOME_PREFIX: &[u8] = b"\x01dtg/txn/v1/home/";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HomeDecisionOutcome {
    mutations: Vec<Mutation>,
    duplicate: bool,
}

impl HomeDecisionOutcome {
    #[must_use]
    pub fn mutations(&self) -> &[Mutation] {
        &self.mutations
    }

    #[must_use]
    pub const fn duplicate(&self) -> bool {
        self.duplicate
    }
}

pub struct HomeDecisionEngine;

impl HomeDecisionEngine {
    pub fn inspection_key(
        home: ShardEpoch,
        transaction_id: TransactionId,
    ) -> Result<LogicalKey, TxnProtocolError> {
        if transaction_id.value() == 0 {
            return Err(TxnProtocolError::InvalidTransactionId);
        }
        let mut bytes = Vec::with_capacity(HOME_PREFIX.len() + 4 + 16);
        bytes.extend_from_slice(HOME_PREFIX);
        bytes.extend_from_slice(&home.shard_id().to_be_bytes());
        bytes.extend_from_slice(&transaction_id.value().to_be_bytes());
        Ok(LogicalKey::in_keyspace(Keyspace::Txn, bytes))
    }

    pub fn record(
        home: ShardEpoch,
        decision: &HomeTransactionRecord,
        existing: Option<&[u8]>,
    ) -> Result<HomeDecisionOutcome, TxnProtocolError> {
        if !decision.participants().contains(&home) {
            return Err(TxnProtocolError::HomeParticipantMissing { home });
        }
        if !matches!(
            decision.state(),
            TransactionState::Committed | TransactionState::Aborted
        ) {
            return Err(TxnProtocolError::InvalidHomeDecisionState {
                state: decision.state(),
            });
        }
        if let Some(bytes) = existing {
            let stored = HomeTransactionRecord::decode(bytes)?;
            if stored == *decision {
                return Ok(HomeDecisionOutcome {
                    mutations: Vec::new(),
                    duplicate: true,
                });
            }
            return Err(TxnProtocolError::HomeDecisionConflict);
        }
        Ok(HomeDecisionOutcome {
            mutations: vec![Mutation::put(
                0,
                Self::inspection_key(home, decision.transaction_id())?,
                decision.encode()?,
            )],
            duplicate: false,
        })
    }
}
