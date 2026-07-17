use raft_command::{CommandBodyV1, CommandEnvelopeV1};
use storage_api::{
    ApplyReceipt, CommittedMutationBatch, LogicalKey, Mutation, MutationOperation, StorageAdapter,
};
use temporal_types::TransactionTime;

use crate::ShardRuntimeError;
use crate::metadata::{
    ReplicaMetadata, adapter_applied_ts_key, closed_ts_key, decode_entry_digest,
    encode_entry_digest, encode_position, encode_timestamp, entry_digest_key,
    is_reserved_metadata_key, load_metadata, position_key, resolved_ts_key,
};

pub struct ShardStateMachine<A> {
    adapter: A,
    metadata: ReplicaMetadata,
    faulted_at: Option<u64>,
}

impl<A> ShardStateMachine<A>
where
    A: StorageAdapter,
{
    pub async fn open(
        adapter: A,
        shard_id: u32,
        placement_epoch: u64,
    ) -> Result<Self, ShardRuntimeError> {
        let metadata = load_metadata(&adapter, shard_id, placement_epoch).await?;
        Ok(Self {
            adapter,
            metadata,
            faulted_at: None,
        })
    }

    #[must_use]
    pub const fn adapter(&self) -> &A {
        &self.adapter
    }

    #[must_use]
    pub const fn metadata(&self) -> ReplicaMetadata {
        self.metadata
    }

    #[must_use]
    pub const fn is_healthy(&self) -> bool {
        self.faulted_at.is_none()
    }

    pub fn servable_safe_ts(&self) -> Result<TransactionTime, ShardRuntimeError> {
        if let Some(failed_index) = self.faulted_at {
            return Err(ShardRuntimeError::ReplicaFaulted { failed_index });
        }
        Ok(self.metadata.safe_ts())
    }

    #[must_use]
    pub fn into_adapter(self) -> A {
        self.adapter
    }

    pub async fn apply_entry(
        &mut self,
        term: u64,
        index: u64,
        command_bytes: &[u8],
    ) -> Result<ApplyReceipt, ShardRuntimeError> {
        if term == 0 || index == 0 {
            return Err(ShardRuntimeError::InvalidLogPosition { term, index });
        }
        let command = CommandEnvelopeV1::decode(command_bytes)?;
        self.validate_authority(&command)?;

        if let Some(failed_index) = self.faulted_at {
            if failed_index != index {
                return Err(ShardRuntimeError::ReplicaFaulted { failed_index });
            }
            self.metadata = load_metadata(
                &self.adapter,
                self.metadata.shard_id,
                self.metadata.placement_epoch,
            )
            .await?;
        }

        let adapter_index = match self.adapter.applied_log_index() {
            Ok(index) => index,
            Err(error) => {
                self.faulted_at = Some(index);
                return Err(ShardRuntimeError::Adapter(error));
            }
        };
        if adapter_index != self.metadata.applied_index {
            return Err(ShardRuntimeError::MetadataIndexMismatch {
                metadata: self.metadata.applied_index,
                adapter: adapter_index,
            });
        }
        let digest = entry_digest(term, index, command_bytes);
        if index <= self.metadata.applied_index {
            if let Err(error) = self.verify_replay(term, index, digest).await {
                if matches!(error, ShardRuntimeError::Adapter(_)) {
                    self.faulted_at = Some(index);
                }
                return Err(error);
            }
            self.faulted_at = None;
            return Ok(ApplyReceipt {
                applied_log_index: self.metadata.applied_index,
                duplicate: true,
            });
        }

        let expected_index = self.metadata.applied_index.saturating_add(1);
        if index != expected_index {
            return Err(ShardRuntimeError::NonContiguousIndex {
                expected: expected_index,
                actual: index,
            });
        }
        if term < self.metadata.last_term {
            return Err(ShardRuntimeError::NonMonotonicTerm {
                current: self.metadata.last_term,
                proposed: term,
            });
        }

        let request_id = command.request_id;
        let (next_metadata, mut mutations) = self.prepare_apply(term, index, command.body)?;
        append_meta_mutation(
            &mut mutations,
            entry_digest_key(index),
            encode_entry_digest(term, digest),
        )?;
        append_meta_mutation(
            &mut mutations,
            position_key(),
            encode_position(next_metadata),
        )?;
        append_watermark_mutations(&mut mutations, self.metadata, next_metadata)?;
        let batch = CommittedMutationBatch {
            shard_id: self.metadata.shard_id,
            log_index: index,
            txn_id: request_id,
            mutations,
        };
        let receipt = match self.adapter.apply_committed(batch).await {
            Ok(receipt) => receipt,
            Err(error) => {
                self.faulted_at = Some(index);
                return Err(ShardRuntimeError::Adapter(error));
            }
        };
        if receipt.applied_log_index != index {
            self.faulted_at = Some(index);
            return Err(ShardRuntimeError::ApplyReceiptMismatch {
                expected: index,
                actual: receipt.applied_log_index,
            });
        }
        self.metadata = next_metadata;
        self.faulted_at = None;
        Ok(receipt)
    }

    fn validate_authority(&self, command: &CommandEnvelopeV1) -> Result<(), ShardRuntimeError> {
        if command.shard_id != self.metadata.shard_id {
            return Err(ShardRuntimeError::ShardMismatch {
                expected: self.metadata.shard_id,
                actual: command.shard_id,
            });
        }
        if command.placement_epoch != self.metadata.placement_epoch {
            return Err(ShardRuntimeError::StaleEpoch {
                expected: self.metadata.placement_epoch,
                actual: command.placement_epoch,
            });
        }
        Ok(())
    }

    fn prepare_apply(
        &self,
        term: u64,
        index: u64,
        body: CommandBodyV1,
    ) -> Result<(ReplicaMetadata, Vec<Mutation>), ShardRuntimeError> {
        match body {
            CommandBodyV1::ApplyPrepared(apply) => {
                validate_business_mutations(&apply.batch.mutations)?;
                if apply.commit_ts <= self.metadata.closed_ts {
                    return Err(ShardRuntimeError::CommitAtOrBeforeClosed {
                        closed: self.metadata.closed_ts,
                        proposed: apply.commit_ts,
                    });
                }
                if apply.commit_ts <= self.metadata.adapter_applied_ts {
                    return Err(ShardRuntimeError::NonMonotonicCommit {
                        current: self.metadata.adapter_applied_ts,
                        proposed: apply.commit_ts,
                    });
                }
                Ok((
                    self.metadata.after_apply(term, index, apply.commit_ts),
                    apply.batch.mutations,
                ))
            }
            CommandBodyV1::ClosedTimestampTick(closed_ts) => {
                if closed_ts < self.metadata.closed_ts {
                    return Err(ShardRuntimeError::NonMonotonicClosed {
                        current: self.metadata.closed_ts,
                        proposed: closed_ts,
                    });
                }
                Ok((self.metadata.after_tick(term, index, closed_ts), Vec::new()))
            }
        }
    }

    async fn verify_replay(
        &self,
        term: u64,
        index: u64,
        expected_digest: [u8; 32],
    ) -> Result<(), ShardRuntimeError> {
        let key = entry_digest_key(index);
        let value = self
            .adapter
            .multi_get(&[key])
            .await?
            .pop()
            .flatten()
            .ok_or(ShardRuntimeError::CorruptMetadata {
                record: "entry-digest",
            })?;
        let (stored_term, stored_digest) = decode_entry_digest(&value)?;
        if stored_term != term || stored_digest != expected_digest {
            return Err(ShardRuntimeError::DivergentReplay { index });
        }
        Ok(())
    }
}

fn validate_business_mutations(mutations: &[Mutation]) -> Result<(), ShardRuntimeError> {
    if mutations.iter().any(|mutation| {
        let key = match &mutation.operation {
            MutationOperation::Put { key, .. } | MutationOperation::Delete { key } => key,
        };
        is_reserved_metadata_key(key)
    }) {
        return Err(ShardRuntimeError::ReservedMetadataKey);
    }
    Ok(())
}

fn append_watermark_mutations(
    mutations: &mut Vec<Mutation>,
    previous: ReplicaMetadata,
    next: ReplicaMetadata,
) -> Result<(), ShardRuntimeError> {
    if next.closed_ts != previous.closed_ts {
        append_meta_mutation(mutations, closed_ts_key(), encode_timestamp(next.closed_ts))?;
    }
    if next.resolved_ts != previous.resolved_ts {
        append_meta_mutation(
            mutations,
            resolved_ts_key(),
            encode_timestamp(next.resolved_ts),
        )?;
    }
    if next.adapter_applied_ts != previous.adapter_applied_ts {
        append_meta_mutation(
            mutations,
            adapter_applied_ts_key(),
            encode_timestamp(next.adapter_applied_ts),
        )?;
    }
    Ok(())
}

fn append_meta_mutation(
    mutations: &mut Vec<Mutation>,
    key: LogicalKey,
    value: Vec<u8>,
) -> Result<(), ShardRuntimeError> {
    let sequence =
        u32::try_from(mutations.len()).map_err(|_| ShardRuntimeError::TooManyMutations)?;
    mutations.push(Mutation::put(sequence, key, value));
    Ok(())
}

fn entry_digest(term: u64, index: u64, command_bytes: &[u8]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"DTGProxy/RaftEntryDigest/V1");
    hasher.update(&term.to_be_bytes());
    hasher.update(&index.to_be_bytes());
    hasher.update(
        &u64::try_from(command_bytes.len())
            .expect("command length fits in u64")
            .to_be_bytes(),
    );
    hasher.update(command_bytes);
    *hasher.finalize().as_bytes()
}
