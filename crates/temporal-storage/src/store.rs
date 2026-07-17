use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use storage_api::{
    AdapterError, ApplyReceipt, CommittedMutationBatch, KeySpan, Keyspace, Mutation, StorageAdapter,
};
use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

use crate::diff::{TemporalChange, diff_projections};
use crate::rewrite::rewrite_projection;
use crate::{
    EdgeIdentity, EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, GraphKey, HistoryAnchor,
    KeyCodecError, LabelId, PartitionId, ProjectionRecord, RecordCodecError, VertexIdentity,
    current_edge_key, current_vertex_key, decode_graph_key, edge_identity_key, history_anchor_key,
    history_prefix, in_adjacency_key, in_adjacency_prefix, out_adjacency_key, out_adjacency_prefix,
    vertex_identity_key,
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeMutation {
    element: ElementRef,
    edge_type: EdgeTypeId,
    source: ElementId,
    destination: ElementId,
    valid: Interval<ValidTime>,
    replacement: Option<CanonicalElement>,
}

impl EdgeMutation {
    #[allow(clippy::too_many_arguments)]
    pub fn put(
        element: ElementRef,
        edge_type: EdgeTypeId,
        source: ElementId,
        destination: ElementId,
        valid: Interval<ValidTime>,
        payload: CanonicalElement,
    ) -> Result<Self, TemporalStoreError> {
        Self::new(
            element,
            edge_type,
            source,
            destination,
            valid,
            Some(payload),
        )
    }

    pub fn delete(
        element: ElementRef,
        edge_type: EdgeTypeId,
        source: ElementId,
        destination: ElementId,
        valid: Interval<ValidTime>,
    ) -> Result<Self, TemporalStoreError> {
        Self::new(element, edge_type, source, destination, valid, None)
    }

    fn new(
        element: ElementRef,
        edge_type: EdgeTypeId,
        source: ElementId,
        destination: ElementId,
        valid: Interval<ValidTime>,
        replacement: Option<CanonicalElement>,
    ) -> Result<Self, TemporalStoreError> {
        if element.kind() != ElementKind::Edge {
            return Err(TemporalStoreError::WrongElementKind);
        }
        Ok(Self {
            element,
            edge_type,
            source,
            destination,
            valid,
            replacement,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeView {
    element: ElementRef,
    edge_type: EdgeTypeId,
    source: ElementId,
    destination: ElementId,
    payload: CanonicalElement,
}

impl EdgeView {
    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn edge_type(&self) -> EdgeTypeId {
        self.edge_type
    }

    #[must_use]
    pub const fn source(&self) -> ElementId {
        self.source
    }

    #[must_use]
    pub const fn destination(&self) -> ElementId {
        self.destination
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
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

    pub fn commit_edge<'a>(
        &'a self,
        context: CommitContext,
        mutation: EdgeMutation,
    ) -> TemporalStoreFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            if context.commit_ts <= context.read_ts {
                return Err(TemporalStoreError::InvalidCommitOrder);
            }

            let identity = EdgeIdentity::new(
                mutation.element,
                mutation.edge_type,
                mutation.source,
                mutation.destination,
            )?;
            self.validate_edge_identity(&identity).await?;
            let anchors = self.load_anchors(mutation.element).await?;
            self.validate_commit_frontier(&anchors, context, mutation.valid)?;

            let empty = ProjectionRecord::new(context.read_ts, Vec::new())?;
            let base = anchors.first().map_or(&empty, HistoryAnchor::projection);
            let projection = rewrite_projection(
                base,
                context.commit_ts,
                mutation.valid,
                mutation.replacement,
            )?;
            let anchor = HistoryAnchor::new(context.commit_ts, mutation.valid, projection.clone())?;
            let projection_bytes = projection.encode()?;
            let out_key = out_adjacency_key(
                mutation.element.graph(),
                mutation.element.partition(),
                mutation.source,
                mutation.edge_type,
                0,
                mutation.destination,
                mutation.element.id(),
            );
            let in_key = in_adjacency_key(
                mutation.element.graph(),
                mutation.element.partition(),
                mutation.destination,
                mutation.edge_type,
                0,
                mutation.source,
                mutation.element.id(),
            );
            let mut mutations = vec![
                Mutation::put(0, edge_identity_key(mutation.element), identity.encode()),
                Mutation::put(
                    1,
                    current_edge_key(mutation.element),
                    projection_bytes.clone(),
                ),
                Mutation::put(
                    2,
                    history_anchor_key(mutation.element, context.commit_ts, 0),
                    anchor.encode()?,
                ),
            ];
            if projection.segments().is_empty() {
                mutations.push(Mutation::delete(3, out_key));
                mutations.push(Mutation::delete(4, in_key));
            } else {
                mutations.push(Mutation::put(3, out_key, projection_bytes.clone()));
                mutations.push(Mutation::put(4, in_key, projection_bytes));
            }

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

    pub fn edge_current<'a>(
        &'a self,
        element: ElementRef,
        valid_time: ValidTime,
    ) -> TemporalStoreFuture<'a, Option<CanonicalElement>> {
        Box::pin(async move {
            require_edge(element)?;
            self.current_value(current_edge_key(element), valid_time)
                .await
        })
    }

    pub fn edge_as_of<'a>(
        &'a self,
        element: ElementRef,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Option<CanonicalElement>> {
        Box::pin(async move {
            require_edge(element)?;
            let anchors = self.load_anchors(element).await?;
            Ok(anchors
                .iter()
                .find(|anchor| anchor.commit_ts() <= transaction_time)
                .and_then(|anchor| anchor.projection().visible_at(valid_time))
                .cloned())
        })
    }

    pub fn expand_out_current<'a>(
        &'a self,
        graph: GraphId,
        partition: PartitionId,
        source: ElementId,
        valid_time: ValidTime,
    ) -> TemporalStoreFuture<'a, Vec<EdgeView>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::AdjOut,
                    out_adjacency_prefix(graph, partition, source),
                ))
                .await?;
            let mut edges = Vec::new();
            for entry in entries {
                let GraphKey::OutAdjacency {
                    graph,
                    partition,
                    source,
                    edge_type,
                    destination,
                    edge,
                    ..
                } = decode_graph_key(entry.key())?
                else {
                    return Err(TemporalStoreError::UnexpectedAdjacencyKey);
                };
                let projection = ProjectionRecord::decode(entry.value())?;
                if let Some(payload) = projection.visible_at(valid_time) {
                    edges.push(EdgeView {
                        element: ElementRef::edge(graph, partition, edge),
                        edge_type,
                        source,
                        destination,
                        payload: payload.clone(),
                    });
                }
            }
            Ok(edges)
        })
    }

    pub fn expand_in_current<'a>(
        &'a self,
        graph: GraphId,
        partition: PartitionId,
        destination: ElementId,
        valid_time: ValidTime,
    ) -> TemporalStoreFuture<'a, Vec<EdgeView>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::AdjIn,
                    in_adjacency_prefix(graph, partition, destination),
                ))
                .await?;
            let mut edges = Vec::new();
            for entry in entries {
                let GraphKey::InAdjacency {
                    graph,
                    partition,
                    destination,
                    edge_type,
                    source,
                    edge,
                    ..
                } = decode_graph_key(entry.key())?
                else {
                    return Err(TemporalStoreError::UnexpectedAdjacencyKey);
                };
                let projection = ProjectionRecord::decode(entry.value())?;
                if let Some(payload) = projection.visible_at(valid_time) {
                    edges.push(EdgeView {
                        element: ElementRef::edge(graph, partition, edge),
                        edge_type,
                        source,
                        destination,
                        payload: payload.clone(),
                    });
                }
            }
            Ok(edges)
        })
    }

    pub fn diff_vertex<'a>(
        &'a self,
        element: ElementRef,
        from_transaction: TransactionTime,
        to_transaction: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<TemporalChange>> {
        Box::pin(async move {
            require_vertex(element)?;
            self.diff_element(element, from_transaction, to_transaction)
                .await
        })
    }

    pub fn diff_edge<'a>(
        &'a self,
        element: ElementRef,
        from_transaction: TransactionTime,
        to_transaction: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<TemporalChange>> {
        Box::pin(async move {
            require_edge(element)?;
            self.diff_element(element, from_transaction, to_transaction)
                .await
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

    async fn validate_edge_identity(
        &self,
        expected: &EdgeIdentity,
    ) -> Result<(), TemporalStoreError> {
        let key = edge_identity_key(expected.element());
        let mut values = self.adapter.multi_get(&[key]).await?;
        if let Some(bytes) = values.pop().flatten() {
            let actual = EdgeIdentity::decode(&bytes)?;
            if actual != *expected {
                return Err(TemporalStoreError::IdentityMismatch);
            }
        }
        Ok(())
    }

    async fn current_value(
        &self,
        key: storage_api::LogicalKey,
        valid_time: ValidTime,
    ) -> Result<Option<CanonicalElement>, TemporalStoreError> {
        let mut values = self.adapter.multi_get(&[key]).await?;
        let Some(bytes) = values.pop().flatten() else {
            return Ok(None);
        };
        let projection = ProjectionRecord::decode(&bytes)?;
        Ok(projection.visible_at(valid_time).cloned())
    }

    fn validate_commit_frontier(
        &self,
        anchors: &[HistoryAnchor],
        context: CommitContext,
        changed_valid: Interval<ValidTime>,
    ) -> Result<(), TemporalStoreError> {
        if let Some(latest) = anchors.first() {
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
                && anchor.changed_valid().overlaps(&changed_valid)
        }) {
            return Err(TemporalStoreError::WriteConflict);
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

    async fn diff_element(
        &self,
        element: ElementRef,
        from_transaction: TransactionTime,
        to_transaction: TransactionTime,
    ) -> Result<Vec<TemporalChange>, TemporalStoreError> {
        if from_transaction > to_transaction {
            return Err(TemporalStoreError::InvalidDiffOrder);
        }
        let anchors = self.load_anchors(element).await?;
        let before = anchors
            .iter()
            .find(|anchor| anchor.commit_ts() <= from_transaction)
            .map(HistoryAnchor::projection);
        let after = anchors
            .iter()
            .find(|anchor| anchor.commit_ts() <= to_transaction)
            .map(HistoryAnchor::projection);
        Ok(diff_projections(before, after))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TemporalStoreError {
    InvalidCommitOrder,
    NonMonotonicCommit,
    WriteConflict,
    IdentityMismatch,
    WrongElementKind,
    UnexpectedAdjacencyKey,
    InvalidDiffOrder,
    Adapter(AdapterError),
    Record(RecordCodecError),
    Key(KeyCodecError),
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
            Self::WrongElementKind => {
                formatter.write_str("operation received the wrong element kind")
            }
            Self::UnexpectedAdjacencyKey => {
                formatter.write_str("adjacency scan returned an unexpected key type")
            }
            Self::InvalidDiffOrder => {
                formatter.write_str("DIFF start transaction must not follow its end")
            }
            Self::Adapter(error) => Display::fmt(error, formatter),
            Self::Record(error) => Display::fmt(error, formatter),
            Self::Key(error) => Display::fmt(error, formatter),
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

impl From<KeyCodecError> for TemporalStoreError {
    fn from(value: KeyCodecError) -> Self {
        Self::Key(value)
    }
}

fn require_vertex(element: ElementRef) -> Result<(), TemporalStoreError> {
    if element.kind() == ElementKind::Vertex {
        Ok(())
    } else {
        Err(TemporalStoreError::WrongElementKind)
    }
}

fn require_edge(element: ElementRef) -> Result<(), TemporalStoreError> {
    if element.kind() == ElementKind::Edge {
        Ok(())
    } else {
        Err(TemporalStoreError::WrongElementKind)
    }
}
