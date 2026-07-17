use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use storage_api::{
    AdapterError, ApplyReceipt, CommittedMutationBatch, KeySpan, Mutation, StorageAdapter,
};
use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

use crate::rewrite::rewrite_projection;
use crate::{
    ElementKind, ElementRef, HistoryAnchor, LabelId, ProjectionRecord, RecordCodecError,
    VertexIdentity, current_vertex_key, history_anchor_key, history_prefix, vertex_identity_key,
};

pub type TemporalStoreFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, TemporalStoreError>> + Send + 'a>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitContext {
    shard_id: u32,
    log_index: u64,
    txn_id: u128,
    read_ts: TransactionTime,
    commit_ts: TransactionTime,
}

impl CommitContext {
    #[must_use]
    pub const fn new(
        shard_id: u32,
        log_index: u64,
        txn_id: u128,
        read_ts: TransactionTime,
        commit_ts: TransactionTime,
    ) -> Self {
        Self {
            shard_id,
            log_index,
            txn_id,
            read_ts,
            commit_ts,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexMutation {
    element: ElementRef,
    label: LabelId,
    valid: Interval<ValidTime>,
    replacement: Option<CanonicalElement>,
}

impl VertexMutation {
    pub fn put(
        element: ElementRef,
        label: LabelId,
        valid: Interval<ValidTime>,
        payload: CanonicalElement,
    ) -> Result<Self, TemporalStoreError> {
        Self::new(element, label, valid, Some(payload))
    }

    pub fn delete(
        element: ElementRef,
        label: LabelId,
        valid: Interval<ValidTime>,
    ) -> Result<Self, TemporalStoreError> {
        Self::new(element, label, valid, None)
    }

    fn new(
        element: ElementRef,
        label: LabelId,
        valid: Interval<ValidTime>,
        replacement: Option<CanonicalElement>,
    ) -> Result<Self, TemporalStoreError> {
        if element.kind() != ElementKind::Vertex {
            return Err(TemporalStoreError::WrongElementKind);
        }
        Ok(Self {
            element,
            label,
            valid,
            replacement,
        })
    }
}

pub struct TemporalStore<A> {
    adapter: A,
}

impl<A> TemporalStore<A>
where
    A: StorageAdapter,
{
    #[must_use]
    pub const fn new(adapter: A) -> Self {
        Self { adapter }
    }

    #[must_use]
    pub const fn adapter(&self) -> &A {
        &self.adapter
    }

    pub fn commit_vertex<'a>(
        &'a self,
        context: CommitContext,
        mutation: VertexMutation,
    ) -> TemporalStoreFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            if context.commit_ts <= context.read_ts {
                return Err(TemporalStoreError::InvalidCommitOrder);
            }

            let identity = VertexIdentity::new(mutation.element, mutation.label)?;
            self.validate_vertex_identity(&identity).await?;
            let anchors = self.load_anchors(mutation.element).await?;
            let latest = anchors.first();
            if let Some(latest) = latest {
                if latest.commit_ts() > context.commit_ts {
                    return Err(TemporalStoreError::NonMonotonicCommit);
                }
                if latest.commit_ts() == context.commit_ts
                    && context.log_index > self.adapter.applied_log_index()?
                {
                    return Err(TemporalStoreError::NonMonotonicCommit);
                }
            }

            if anchors.iter().any(|anchor| {
                anchor.commit_ts() > context.read_ts
                    && anchor.commit_ts() < context.commit_ts
                    && anchor.changed_valid().overlaps(&mutation.valid)
            }) {
                return Err(TemporalStoreError::WriteConflict);
            }

            let empty = ProjectionRecord::new(context.read_ts, Vec::new())?;
            let base = latest.map_or(&empty, HistoryAnchor::projection);
            let projection = rewrite_projection(
                base,
                context.commit_ts,
                mutation.valid,
                mutation.replacement,
            )?;
            let anchor = HistoryAnchor::new(context.commit_ts, mutation.valid, projection.clone())?;

            let mutations = vec![
                Mutation::put(0, vertex_identity_key(mutation.element), identity.encode()),
                Mutation::put(
                    1,
                    current_vertex_key(mutation.element),
                    projection.encode()?,
                ),
                Mutation::put(
                    2,
                    history_anchor_key(mutation.element, context.commit_ts, 0),
                    anchor.encode()?,
                ),
            ];

            Ok(self
                .adapter
                .apply_committed(CommittedMutationBatch {
                    shard_id: context.shard_id,
                    log_index: context.log_index,
                    txn_id: context.txn_id,
                    mutations,
                })
                .await?)
        })
    }

    pub fn vertex_current<'a>(
        &'a self,
        element: ElementRef,
        valid_time: ValidTime,
    ) -> TemporalStoreFuture<'a, Option<CanonicalElement>> {
        Box::pin(async move {
            require_vertex(element)?;
            let key = current_vertex_key(element);
            let mut values = self.adapter.multi_get(&[key]).await?;
            let Some(bytes) = values.pop().flatten() else {
                return Ok(None);
            };
            let projection = ProjectionRecord::decode(&bytes)?;
            Ok(projection.visible_at(valid_time).cloned())
        })
    }

    pub fn vertex_as_of<'a>(
        &'a self,
        element: ElementRef,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Option<CanonicalElement>> {
        Box::pin(async move {
            require_vertex(element)?;
            let anchors = self.load_anchors(element).await?;
            Ok(anchors
                .iter()
                .find(|anchor| anchor.commit_ts() <= transaction_time)
                .and_then(|anchor| anchor.projection().visible_at(valid_time))
                .cloned())
        })
    }

    async fn validate_vertex_identity(
        &self,
        expected: &VertexIdentity,
    ) -> Result<(), TemporalStoreError> {
        let key = vertex_identity_key(expected.element());
        let mut values = self.adapter.multi_get(&[key]).await?;
        if let Some(bytes) = values.pop().flatten() {
            let actual = VertexIdentity::decode(&bytes)?;
            if actual != *expected {
                return Err(TemporalStoreError::IdentityMismatch);
            }
        }
        Ok(())
    }

    async fn load_anchors(
        &self,
        element: ElementRef,
    ) -> Result<Vec<HistoryAnchor>, TemporalStoreError> {
        let entries = self
            .adapter
            .scan(&KeySpan::prefix(
                storage_api::Keyspace::History,
                history_prefix(element),
            ))
            .await?;
        entries
            .into_iter()
            .map(|entry| HistoryAnchor::decode(entry.value()).map_err(TemporalStoreError::from))
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TemporalStoreError {
    InvalidCommitOrder,
    NonMonotonicCommit,
    WriteConflict,
    IdentityMismatch,
    WrongElementKind,
    Adapter(AdapterError),
    Record(RecordCodecError),
}

impl Display for TemporalStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCommitOrder => {
                formatter.write_str("commit timestamp must follow the read snapshot")
            }
            Self::NonMonotonicCommit => {
                formatter.write_str("element transaction time cannot move backwards or repeat")
            }
            Self::WriteConflict => {
                formatter.write_str("a later commit overlaps the requested valid interval")
            }
            Self::IdentityMismatch => formatter.write_str("element identity metadata changed"),
            Self::WrongElementKind => formatter.write_str("operation requires a vertex element"),
            Self::Adapter(error) => Display::fmt(error, formatter),
            Self::Record(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for TemporalStoreError {}

impl From<AdapterError> for TemporalStoreError {
    fn from(value: AdapterError) -> Self {
        Self::Adapter(value)
    }
}

impl From<RecordCodecError> for TemporalStoreError {
    fn from(value: RecordCodecError) -> Self {
        Self::Record(value)
    }
}

fn require_vertex(element: ElementRef) -> Result<(), TemporalStoreError> {
    if element.kind() == ElementKind::Vertex {
        Ok(())
    } else {
        Err(TemporalStoreError::WrongElementKind)
    }
}
