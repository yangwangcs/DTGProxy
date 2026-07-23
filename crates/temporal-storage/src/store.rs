use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use storage_api::{
    AdapterError, ApplyReceipt, KeySpan, Keyspace, Mutation, MutationOperation,
    PreparedMutationBatch, StorageAdapter,
};
use temporal_types::{CanonicalElement, Interval, TransactionTime, ValidTime};

use crate::diff::{TemporalChange, diff_projections};
use crate::history::{MAX_CHAIN_ENTRIES, entry_for_commit, reconstruct};
use crate::rewrite::rewrite_projection;
use crate::transaction::TemporalOperation;
use crate::{
    EdgeIdentity, EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, GraphKey, HistoryAnchor,
    HistoryEntry, KeyCodecError, LabelId, PartitionId, ProjectionRecord, RecordCodecError,
    TemporalTransaction, VertexIdentity, cross_in_adjacency_key, cross_in_adjacency_prefix,
    cross_out_adjacency_key, cross_out_adjacency_prefix, current_edge_graph_prefix,
    current_edge_key, current_vertex_graph_prefix, current_vertex_key, decode_graph_key,
    edge_identity_graph_prefix, edge_identity_key, edge_identity_prefix, history_anchor_key,
    history_prefix, in_adjacency_key, in_adjacency_prefix, out_adjacency_key, out_adjacency_prefix,
    vertex_identity_graph_prefix, vertex_identity_key,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PrepareContext {
    shard_id: u32,
    txn_id: u128,
    read_ts: TransactionTime,
    commit_ts: TransactionTime,
}

impl PrepareContext {
    #[must_use]
    pub const fn new(
        shard_id: u32,
        txn_id: u128,
        read_ts: TransactionTime,
        commit_ts: TransactionTime,
    ) -> Self {
        Self {
            shard_id,
            txn_id,
            read_ts,
            commit_ts,
        }
    }
}

impl From<CommitContext> for PrepareContext {
    fn from(context: CommitContext) -> Self {
        Self::new(
            context.shard_id,
            context.txn_id,
            context.read_ts,
            context.commit_ts,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexMutation {
    pub(crate) element: ElementRef,
    pub(crate) label: LabelId,
    pub(crate) valid: Interval<ValidTime>,
    pub(crate) replacement: Option<CanonicalElement>,
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
    pub(crate) element: ElementRef,
    pub(crate) edge_type: EdgeTypeId,
    pub(crate) source: ElementRef,
    pub(crate) destination: ElementRef,
    pub(crate) valid: Interval<ValidTime>,
    pub(crate) replacement: Option<CanonicalElement>,
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
        Self::put_between(
            element,
            edge_type,
            ElementRef::vertex(element.graph(), element.partition(), source),
            ElementRef::vertex(element.graph(), element.partition(), destination),
            valid,
            payload,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn put_between(
        element: ElementRef,
        edge_type: EdgeTypeId,
        source: ElementRef,
        destination: ElementRef,
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
        Self::delete_between(
            element,
            edge_type,
            ElementRef::vertex(element.graph(), element.partition(), source),
            ElementRef::vertex(element.graph(), element.partition(), destination),
            valid,
        )
    }

    pub fn delete_between(
        element: ElementRef,
        edge_type: EdgeTypeId,
        source: ElementRef,
        destination: ElementRef,
        valid: Interval<ValidTime>,
    ) -> Result<Self, TemporalStoreError> {
        Self::new(element, edge_type, source, destination, valid, None)
    }

    fn new(
        element: ElementRef,
        edge_type: EdgeTypeId,
        source: ElementRef,
        destination: ElementRef,
        valid: Interval<ValidTime>,
        replacement: Option<CanonicalElement>,
    ) -> Result<Self, TemporalStoreError> {
        if element.kind() != ElementKind::Edge
            || source.kind() != ElementKind::Vertex
            || destination.kind() != ElementKind::Vertex
        {
            return Err(TemporalStoreError::WrongElementKind);
        }
        if source.graph() != element.graph()
            || destination.graph() != element.graph()
            || source.partition() != element.partition()
        {
            return Err(TemporalStoreError::InvalidEdgeEndpoints);
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
pub struct VertexView {
    element: ElementRef,
    label: LabelId,
    payload: CanonicalElement,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexTemporalSegment {
    element: ElementRef,
    label: LabelId,
    valid: Interval<ValidTime>,
    payload: CanonicalElement,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeTemporalSegment {
    element: ElementRef,
    edge_type: EdgeTypeId,
    source: ElementRef,
    destination: ElementRef,
    valid: Interval<ValidTime>,
    payload: CanonicalElement,
}

impl EdgeTemporalSegment {
    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn edge_type(&self) -> EdgeTypeId {
        self.edge_type
    }

    #[must_use]
    pub const fn source_ref(&self) -> ElementRef {
        self.source
    }

    #[must_use]
    pub const fn destination_ref(&self) -> ElementRef {
        self.destination
    }

    #[must_use]
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
    }
}

impl VertexTemporalSegment {
    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn label(&self) -> LabelId {
        self.label
    }

    #[must_use]
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
    }
}

impl VertexView {
    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn label(&self) -> LabelId {
        self.label
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeView {
    element: ElementRef,
    edge_type: EdgeTypeId,
    source: ElementRef,
    destination: ElementRef,
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
        self.source.id()
    }

    #[must_use]
    pub const fn destination(&self) -> ElementId {
        self.destination.id()
    }

    #[must_use]
    pub const fn source_ref(&self) -> ElementRef {
        self.source
    }

    #[must_use]
    pub const fn destination_ref(&self) -> ElementRef {
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

    pub fn commit_transaction<'a>(
        &'a self,
        context: CommitContext,
        transaction: TemporalTransaction,
    ) -> TemporalStoreFuture<'a, ApplyReceipt> {
        Box::pin(async move {
            if context.commit_ts <= context.read_ts {
                return Err(TemporalStoreError::InvalidCommitOrder);
            }
            let applied_log_index = self.adapter.applied_log_index()?;
            let expected_log_index = applied_log_index.saturating_add(1);
            if context.log_index > applied_log_index && context.log_index != expected_log_index {
                return Err(AdapterError::NonContiguousLogIndex {
                    expected: expected_log_index,
                    actual: context.log_index,
                }
                .into());
            }
            let prepared = self
                .prepare_transaction_inner(context.into(), transaction, Some(context.log_index))
                .await?;
            Ok(self
                .adapter
                .apply_committed(prepared.commit_at(context.log_index))
                .await?)
        })
    }

    pub fn prepare_transaction<'a>(
        &'a self,
        context: PrepareContext,
        transaction: TemporalTransaction,
    ) -> TemporalStoreFuture<'a, PreparedMutationBatch> {
        Box::pin(async move {
            self.prepare_transaction_inner(context, transaction, None)
                .await
        })
    }

    pub fn prepare_endpoint_guard<'a>(
        &'a self,
        read_ts: TransactionTime,
        vertex: ElementRef,
        required: Interval<ValidTime>,
    ) -> TemporalStoreFuture<'a, MutationOperation> {
        Box::pin(async move {
            require_vertex(vertex)?;
            let projection = self
                .load_current_projection(vertex)
                .await?
                .ok_or(TemporalStoreError::EndpointNotPresent { vertex })?;
            if projection.commit_ts() > read_ts {
                return Err(TemporalStoreError::WriteConflict);
            }
            if !projection_covers(&projection, required) {
                return Err(TemporalStoreError::EndpointNotPresent { vertex });
            }
            Ok(MutationOperation::Put {
                key: current_vertex_key(vertex),
                value: projection.encode()?,
            })
        })
    }

    async fn prepare_transaction_inner(
        &self,
        context: PrepareContext,
        transaction: TemporalTransaction,
        replay_log_index: Option<u64>,
    ) -> Result<PreparedMutationBatch, TemporalStoreError> {
        if context.commit_ts <= context.read_ts {
            return Err(TemporalStoreError::InvalidCommitOrder);
        }
        let allow_repeated_elements = transaction.allows_repeated_elements();
        let mut operations = transaction.into_operations();
        if operations.is_empty() {
            return Err(TemporalStoreError::EmptyTransaction);
        }
        operations.sort_by_key(TemporalOperation::element);
        let multi_operation_elements = operations
            .windows(2)
            .filter_map(|pair| {
                (pair[0].element() == pair[1].element()).then_some(pair[0].element())
            })
            .collect::<std::collections::BTreeSet<_>>();
        if !allow_repeated_elements && !multi_operation_elements.is_empty() {
            return Err(TemporalStoreError::DuplicateElementOperation {
                element: *multi_operation_elements
                    .first()
                    .expect("set was checked non-empty"),
            });
        }

        let mut staged_vertices = BTreeMap::<ElementRef, ProjectionRecord>::new();
        let mut guarded_vertices = BTreeMap::new();
        let mut endpoint_guards = BTreeMap::new();
        let mut staged_edges = BTreeMap::<ElementRef, ProjectionRecord>::new();
        let mut writes = Vec::new();
        for operation in operations {
            match operation {
                TemporalOperation::Vertex(mutation) => {
                    let removes_valid_time = mutation.replacement.is_none();
                    let identity = VertexIdentity::new(mutation.element, mutation.label)?;
                    self.validate_vertex_identity(&identity).await?;
                    let stored_current = self.load_current_projection(mutation.element).await?;
                    self.validate_commit_frontier(
                        mutation.element,
                        stored_current.as_ref(),
                        context,
                        mutation.valid,
                        replay_log_index,
                    )
                    .await?;
                    let current = if let Some(projection) = staged_vertices.get(&mutation.element) {
                        Some(projection.clone())
                    } else {
                        stored_current
                    };
                    let recent_entries = self
                        .load_history_chain_at(mutation.element, context.commit_ts)
                        .await?;
                    let empty = ProjectionRecord::new(context.read_ts, Vec::new())?;
                    let projection = rewrite_projection(
                        current.as_ref().unwrap_or(&empty),
                        context.commit_ts,
                        mutation.valid,
                        mutation.replacement.clone(),
                    )?;
                    let history = if multi_operation_elements.contains(&mutation.element) {
                        HistoryEntry::Anchor(HistoryAnchor::new(
                            context.commit_ts,
                            mutation.valid,
                            projection.clone(),
                        )?)
                    } else {
                        entry_for_commit(
                            &recent_entries,
                            context.commit_ts,
                            mutation.valid,
                            mutation.replacement,
                            projection.clone(),
                        )?
                    };
                    writes.push(MutationOperation::Put {
                        key: vertex_identity_key(mutation.element),
                        value: identity.encode(),
                    });
                    writes.push(MutationOperation::Put {
                        key: current_vertex_key(mutation.element),
                        value: projection.encode()?,
                    });
                    writes.push(MutationOperation::Put {
                        key: history_anchor_key(mutation.element, context.commit_ts, 0),
                        value: history.encode()?,
                    });
                    if removes_valid_time {
                        guarded_vertices.insert(mutation.element, projection.clone());
                    }
                    staged_vertices.insert(mutation.element, projection);
                }
                TemporalOperation::Edge(mutation) => {
                    let identity = EdgeIdentity::new_between(
                        mutation.element,
                        mutation.edge_type,
                        mutation.source,
                        mutation.destination,
                    )?;
                    self.validate_edge_identity(&identity).await?;
                    let stored_current = self.load_current_projection(mutation.element).await?;
                    self.validate_commit_frontier(
                        mutation.element,
                        stored_current.as_ref(),
                        context,
                        mutation.valid,
                        replay_log_index,
                    )
                    .await?;
                    let current = if let Some(projection) = staged_edges.get(&mutation.element) {
                        Some(projection.clone())
                    } else {
                        stored_current
                    };
                    if mutation.replacement.is_some() {
                        for endpoint in [mutation.source, mutation.destination]
                            .into_iter()
                            .filter(|endpoint| endpoint.partition() == mutation.element.partition())
                        {
                            let staged = staged_vertices.get(&endpoint);
                            let endpoint_projection = if let Some(projection) = staged {
                                Some(projection.clone())
                            } else {
                                self.load_current_projection(endpoint).await?
                            };
                            if endpoint_projection.as_ref().is_none_or(|projection| {
                                !projection_covers(projection, mutation.valid)
                            }) {
                                return Err(TemporalStoreError::EndpointNotPresent {
                                    vertex: endpoint,
                                });
                            }
                            if staged.is_none() {
                                let projection = endpoint_projection
                                    .expect("validated existing endpoint has a projection");
                                if projection.commit_ts() > context.read_ts {
                                    return Err(TemporalStoreError::WriteConflict);
                                }
                                endpoint_guards.entry(endpoint).or_insert(projection);
                            }
                        }
                    }
                    let recent_entries = self
                        .load_history_chain_at(mutation.element, context.commit_ts)
                        .await?;
                    let empty = ProjectionRecord::new(context.read_ts, Vec::new())?;
                    let projection = rewrite_projection(
                        current.as_ref().unwrap_or(&empty),
                        context.commit_ts,
                        mutation.valid,
                        mutation.replacement.clone(),
                    )?;
                    let history = if multi_operation_elements.contains(&mutation.element) {
                        HistoryEntry::Anchor(HistoryAnchor::new(
                            context.commit_ts,
                            mutation.valid,
                            projection.clone(),
                        )?)
                    } else {
                        entry_for_commit(
                            &recent_entries,
                            context.commit_ts,
                            mutation.valid,
                            mutation.replacement,
                            projection.clone(),
                        )?
                    };
                    let projection_bytes = projection.encode()?;
                    let cross_partition =
                        mutation.source.partition() != mutation.destination.partition();
                    let (out_key, in_key) = if cross_partition {
                        (
                            cross_out_adjacency_key(
                                mutation.element.graph(),
                                mutation.source.partition(),
                                mutation.source.id(),
                                mutation.edge_type,
                                0,
                                mutation.destination.partition(),
                                mutation.destination.id(),
                                mutation.element.partition(),
                                mutation.element.id(),
                            ),
                            cross_in_adjacency_key(
                                mutation.element.graph(),
                                mutation.destination.partition(),
                                mutation.destination.id(),
                                mutation.edge_type,
                                0,
                                mutation.source.partition(),
                                mutation.source.id(),
                                mutation.element.partition(),
                                mutation.element.id(),
                            ),
                        )
                    } else {
                        (
                            out_adjacency_key(
                                mutation.element.graph(),
                                mutation.element.partition(),
                                mutation.source.id(),
                                mutation.edge_type,
                                0,
                                mutation.destination.id(),
                                mutation.element.id(),
                            ),
                            in_adjacency_key(
                                mutation.element.graph(),
                                mutation.element.partition(),
                                mutation.destination.id(),
                                mutation.edge_type,
                                0,
                                mutation.source.id(),
                                mutation.element.id(),
                            ),
                        )
                    };
                    writes.push(MutationOperation::Put {
                        key: edge_identity_key(mutation.element),
                        value: identity.encode(),
                    });
                    writes.push(MutationOperation::Put {
                        key: current_edge_key(mutation.element),
                        value: projection_bytes.clone(),
                    });
                    writes.push(MutationOperation::Put {
                        key: history_anchor_key(mutation.element, context.commit_ts, 0),
                        value: history.encode()?,
                    });
                    if projection.segments().is_empty() {
                        writes.push(MutationOperation::Delete { key: out_key });
                        writes.push(MutationOperation::Delete { key: in_key });
                    } else {
                        writes.push(MutationOperation::Put {
                            key: out_key,
                            value: projection_bytes.clone(),
                        });
                        writes.push(MutationOperation::Put {
                            key: in_key,
                            value: projection_bytes,
                        });
                    }
                    staged_edges.insert(mutation.element, projection);
                }
            }
        }

        for (vertex, projection) in guarded_vertices {
            self.validate_incident_edge_coverage(vertex, &projection, &staged_edges)
                .await?;
        }
        for (vertex, projection) in endpoint_guards {
            writes.push(MutationOperation::Put {
                key: current_vertex_key(vertex),
                value: projection.encode()?,
            });
        }

        let writes = writes
            .into_iter()
            .map(|operation| {
                let key = match &operation {
                    MutationOperation::Put { key, .. } | MutationOperation::Delete { key } => {
                        key.clone()
                    }
                };
                (key, operation)
            })
            .collect::<BTreeMap<_, _>>();
        let mutations = writes
            .into_values()
            .enumerate()
            .map(|(sequence, operation)| {
                Ok(Mutation {
                    sequence: u32::try_from(sequence)
                        .map_err(|_| TemporalStoreError::TooManyMutations)?,
                    operation,
                })
            })
            .collect::<Result<Vec<_>, TemporalStoreError>>()?;
        Ok(PreparedMutationBatch {
            shard_id: context.shard_id,
            txn_id: context.txn_id,
            mutations,
        })
    }

    pub fn commit_vertex<'a>(
        &'a self,
        context: CommitContext,
        mutation: VertexMutation,
    ) -> TemporalStoreFuture<'a, ApplyReceipt> {
        self.commit_transaction(context, TemporalTransaction::new().with_vertex(mutation))
    }

    pub fn commit_edge<'a>(
        &'a self,
        context: CommitContext,
        mutation: EdgeMutation,
    ) -> TemporalStoreFuture<'a, ApplyReceipt> {
        self.commit_transaction(context, TemporalTransaction::new().with_edge(mutation))
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
            Ok(self
                .load_projection_at(element, transaction_time)
                .await?
                .as_ref()
                .and_then(|projection| projection.visible_at(valid_time))
                .cloned())
        })
    }

    pub fn vertex_view_current<'a>(
        &'a self,
        element: ElementRef,
        valid_time: ValidTime,
    ) -> TemporalStoreFuture<'a, Option<VertexView>> {
        Box::pin(async move {
            let Some(payload) = self.vertex_current(element, valid_time).await? else {
                return Ok(None);
            };
            let identity = self
                .load_vertex_identity(element)
                .await?
                .ok_or(TemporalStoreError::IdentityMismatch)?;
            Ok(Some(vertex_view(identity, payload)))
        })
    }

    pub fn vertex_view_as_of<'a>(
        &'a self,
        element: ElementRef,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Option<VertexView>> {
        Box::pin(async move {
            let Some(payload) = self
                .vertex_as_of(element, valid_time, transaction_time)
                .await?
            else {
                return Ok(None);
            };
            let identity = self
                .load_vertex_identity(element)
                .await?
                .ok_or(TemporalStoreError::IdentityMismatch)?;
            Ok(Some(vertex_view(identity, payload)))
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
            Ok(self
                .load_projection_at(element, transaction_time)
                .await?
                .as_ref()
                .and_then(|projection| projection.visible_at(valid_time))
                .cloned())
        })
    }

    pub fn edge_view_current<'a>(
        &'a self,
        element: ElementRef,
        valid_time: ValidTime,
    ) -> TemporalStoreFuture<'a, Option<EdgeView>> {
        Box::pin(async move {
            require_edge(element)?;
            let Some(payload) = self
                .current_value(current_edge_key(element), valid_time)
                .await?
            else {
                return Ok(None);
            };
            let identity = self
                .load_edge_identity(element)
                .await?
                .ok_or(TemporalStoreError::MissingEdgeIdentity { edge: element })?;
            Ok(Some(edge_view(identity, payload)))
        })
    }

    pub fn edge_view_as_of<'a>(
        &'a self,
        element: ElementRef,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Option<EdgeView>> {
        Box::pin(async move {
            require_edge(element)?;
            let Some(payload) = self
                .load_projection_at(element, transaction_time)
                .await?
                .as_ref()
                .and_then(|projection| projection.visible_at(valid_time))
                .cloned()
            else {
                return Ok(None);
            };
            let identity = self
                .load_edge_identity(element)
                .await?
                .ok_or(TemporalStoreError::MissingEdgeIdentity { edge: element })?;
            Ok(Some(edge_view(identity, payload)))
        })
    }

    pub fn scan_vertices_current<'a>(
        &'a self,
        graph: GraphId,
        valid_time: ValidTime,
    ) -> TemporalStoreFuture<'a, Vec<(ElementRef, CanonicalElement)>> {
        Box::pin(async move {
            Ok(self
                .scan_vertex_views_current(graph, valid_time)
                .await?
                .into_iter()
                .map(|view| (view.element, view.payload))
                .collect())
        })
    }

    pub fn scan_vertex_views_current<'a>(
        &'a self,
        graph: GraphId,
        valid_time: ValidTime,
    ) -> TemporalStoreFuture<'a, Vec<VertexView>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Current,
                    current_vertex_graph_prefix(graph),
                ))
                .await?;
            let mut vertices = Vec::new();
            for entry in entries {
                let GraphKey::CurrentVertex(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedCurrentKey);
                };
                let projection = ProjectionRecord::decode(entry.value())?;
                if let Some(payload) = projection.visible_at(valid_time) {
                    let identity = self
                        .load_vertex_identity(element)
                        .await?
                        .ok_or(TemporalStoreError::IdentityMismatch)?;
                    vertices.push(vertex_view(identity, payload.clone()));
                }
            }
            Ok(vertices)
        })
    }

    pub fn scan_vertices_as_of<'a>(
        &'a self,
        graph: GraphId,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<(ElementRef, CanonicalElement)>> {
        Box::pin(async move {
            Ok(self
                .scan_vertex_views_as_of(graph, valid_time, transaction_time)
                .await?
                .into_iter()
                .map(|view| (view.element, view.payload))
                .collect())
        })
    }

    pub fn scan_vertex_views_as_of<'a>(
        &'a self,
        graph: GraphId,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<VertexView>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Identity,
                    vertex_identity_graph_prefix(graph),
                ))
                .await?;
            let mut vertices = Vec::new();
            for entry in entries {
                let GraphKey::VertexIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedVertexIdentityKey);
                };
                let identity = VertexIdentity::decode(entry.value())?;
                if identity.element() != element {
                    return Err(TemporalStoreError::IdentityMismatch);
                }
                if let Some(payload) = self
                    .load_projection_at(element, transaction_time)
                    .await?
                    .as_ref()
                    .and_then(|projection| projection.visible_at(valid_time))
                {
                    vertices.push(vertex_view(identity, payload.clone()));
                }
            }
            Ok(vertices)
        })
    }

    pub fn scan_vertex_views_as_of_bounded<'a>(
        &'a self,
        graph: GraphId,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
        max_entries: usize,
        max_bytes: u64,
    ) -> TemporalStoreFuture<'a, (Vec<VertexView>, u64, usize)> {
        Box::pin(async move {
            let (entries, mut scanned_bytes) = self
                .scan_identity_entries(vertex_identity_graph_prefix(graph), max_entries, max_bytes)
                .await?;
            let entry_count = entries.len();
            let mut vertices = Vec::new();
            for entry in entries {
                let GraphKey::VertexIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedVertexIdentityKey);
                };
                let identity = VertexIdentity::decode(entry.value())?;
                if identity.element() != element {
                    return Err(TemporalStoreError::IdentityMismatch);
                }
                if let Some(payload) = self
                    .load_projection_at(element, transaction_time)
                    .await?
                    .as_ref()
                    .and_then(|projection| projection.visible_at(valid_time))
                {
                    let payload_bytes = payload
                        .encode()
                        .map_err(|_| TemporalStoreError::ScanByteLimit)?
                        .len();
                    scanned_bytes = charge_scan_bytes(scanned_bytes, payload_bytes, max_bytes)?;
                    vertices.push(vertex_view(identity, payload.clone()));
                }
            }
            Ok((vertices, scanned_bytes, entry_count))
        })
    }

    pub fn scan_vertex_segments_current<'a>(
        &'a self,
        graph: GraphId,
        window: Interval<ValidTime>,
    ) -> TemporalStoreFuture<'a, Vec<VertexTemporalSegment>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Current,
                    current_vertex_graph_prefix(graph),
                ))
                .await?;
            let mut segments = Vec::new();
            for entry in entries {
                let GraphKey::CurrentVertex(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedCurrentKey);
                };
                let projection = ProjectionRecord::decode(entry.value())?;
                let identity = self
                    .load_vertex_identity(element)
                    .await?
                    .ok_or(TemporalStoreError::IdentityMismatch)?;
                append_vertex_segments(&mut segments, identity, &projection, window);
            }
            sort_vertex_segments(&mut segments);
            Ok(segments)
        })
    }

    pub fn scan_vertex_segments_as_of<'a>(
        &'a self,
        graph: GraphId,
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<VertexTemporalSegment>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Identity,
                    vertex_identity_graph_prefix(graph),
                ))
                .await?;
            let mut segments = Vec::new();
            for entry in entries {
                let GraphKey::VertexIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedVertexIdentityKey);
                };
                let identity = VertexIdentity::decode(entry.value())?;
                if identity.element() != element {
                    return Err(TemporalStoreError::IdentityMismatch);
                }
                if let Some(projection) = self.load_projection_at(element, transaction_time).await?
                {
                    append_vertex_segments(&mut segments, identity, &projection, window);
                }
            }
            sort_vertex_segments(&mut segments);
            Ok(segments)
        })
    }

    pub fn scan_vertex_segments_as_of_bounded<'a>(
        &'a self,
        graph: GraphId,
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
        max_segments: usize,
        max_bytes: u64,
    ) -> TemporalStoreFuture<'a, (Vec<VertexTemporalSegment>, u64, usize)> {
        Box::pin(async move {
            let (entries, mut scanned_bytes) = self
                .scan_identity_entries(vertex_identity_graph_prefix(graph), max_segments, max_bytes)
                .await?;
            let entry_count = entries.len();
            let mut segments = Vec::new();
            for entry in entries {
                let GraphKey::VertexIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedVertexIdentityKey);
                };
                let identity = VertexIdentity::decode(entry.value())?;
                if identity.element() != element {
                    return Err(TemporalStoreError::IdentityMismatch);
                }
                let Some(projection) = self.load_projection_at(element, transaction_time).await?
                else {
                    continue;
                };
                for segment in projection.segments() {
                    let Some(valid) = intersect_valid_intervals(segment.valid(), window) else {
                        continue;
                    };
                    if segments.len() >= max_segments {
                        return Err(TemporalStoreError::ScanEntryLimit);
                    }
                    scanned_bytes = charge_scan_bytes(
                        scanned_bytes,
                        segment
                            .payload()
                            .encode()
                            .map_err(|_| TemporalStoreError::ScanByteLimit)?
                            .len(),
                        max_bytes,
                    )?;
                    segments.push(VertexTemporalSegment {
                        element: identity.element(),
                        label: identity.label(),
                        valid,
                        payload: segment.payload().clone(),
                    });
                }
            }
            sort_vertex_segments(&mut segments);
            Ok((segments, scanned_bytes, entry_count))
        })
    }

    pub fn vertex_segments_as_of<'a>(
        &'a self,
        element: ElementRef,
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<VertexTemporalSegment>> {
        Box::pin(async move {
            require_vertex(element)?;
            let Some(projection) = self.load_projection_at(element, transaction_time).await? else {
                return Ok(Vec::new());
            };
            let identity = self
                .load_vertex_identity(element)
                .await?
                .ok_or(TemporalStoreError::IdentityMismatch)?;
            let mut segments = Vec::new();
            append_vertex_segments(&mut segments, identity, &projection, window);
            sort_vertex_segments(&mut segments);
            Ok(segments)
        })
    }

    pub fn scan_edge_segments_current<'a>(
        &'a self,
        graph: GraphId,
        window: Interval<ValidTime>,
    ) -> TemporalStoreFuture<'a, Vec<EdgeTemporalSegment>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Current,
                    current_edge_graph_prefix(graph),
                ))
                .await?;
            let mut segments = Vec::new();
            for entry in entries {
                let GraphKey::CurrentEdge(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedCurrentKey);
                };
                let projection = ProjectionRecord::decode(entry.value())?;
                let identity = self
                    .load_edge_identity(element)
                    .await?
                    .ok_or(TemporalStoreError::MissingEdgeIdentity { edge: element })?;
                append_edge_segments(&mut segments, identity, &projection, window);
            }
            sort_edge_segments(&mut segments);
            Ok(segments)
        })
    }

    pub fn scan_edge_segments_as_of<'a>(
        &'a self,
        graph: GraphId,
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<EdgeTemporalSegment>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Identity,
                    edge_identity_graph_prefix(graph),
                ))
                .await?;
            let mut segments = Vec::new();
            for entry in entries {
                let GraphKey::EdgeIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedEdgeIdentityKey);
                };
                let identity = EdgeIdentity::decode(entry.value())?;
                if identity.element() != element {
                    return Err(TemporalStoreError::IdentityMismatch);
                }
                if let Some(projection) = self.load_projection_at(element, transaction_time).await?
                {
                    append_edge_segments(&mut segments, identity, &projection, window);
                }
            }
            sort_edge_segments(&mut segments);
            Ok(segments)
        })
    }

    pub fn scan_edge_segments_as_of_bounded<'a>(
        &'a self,
        graph: GraphId,
        window: Interval<ValidTime>,
        transaction_time: TransactionTime,
        max_segments: usize,
        max_bytes: u64,
    ) -> TemporalStoreFuture<'a, (Vec<EdgeTemporalSegment>, u64, usize)> {
        Box::pin(async move {
            let (entries, mut scanned_bytes) = self
                .scan_identity_entries(edge_identity_graph_prefix(graph), max_segments, max_bytes)
                .await?;
            let entry_count = entries.len();
            let mut segments = Vec::new();
            for entry in entries {
                let GraphKey::EdgeIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedEdgeIdentityKey);
                };
                let identity = EdgeIdentity::decode(entry.value())?;
                if identity.element() != element {
                    return Err(TemporalStoreError::IdentityMismatch);
                }
                let Some(projection) = self.load_projection_at(element, transaction_time).await?
                else {
                    continue;
                };
                for segment in projection.segments() {
                    let Some(valid) = intersect_valid_intervals(segment.valid(), window) else {
                        continue;
                    };
                    if segments.len() >= max_segments {
                        return Err(TemporalStoreError::ScanEntryLimit);
                    }
                    scanned_bytes = charge_scan_bytes(
                        scanned_bytes,
                        segment
                            .payload()
                            .encode()
                            .map_err(|_| TemporalStoreError::ScanByteLimit)?
                            .len(),
                        max_bytes,
                    )?;
                    segments.push(EdgeTemporalSegment {
                        element: identity.element(),
                        edge_type: identity.edge_type(),
                        source: identity.source_ref(),
                        destination: identity.destination_ref(),
                        valid,
                        payload: segment.payload().clone(),
                    });
                }
            }
            sort_edge_segments(&mut segments);
            Ok((segments, scanned_bytes, entry_count))
        })
    }

    pub fn scan_edges_current<'a>(
        &'a self,
        graph: GraphId,
        valid_time: ValidTime,
    ) -> TemporalStoreFuture<'a, Vec<EdgeView>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Current,
                    current_edge_graph_prefix(graph),
                ))
                .await?;
            let mut edges = Vec::new();
            for entry in entries {
                let GraphKey::CurrentEdge(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedCurrentKey);
                };
                let projection = ProjectionRecord::decode(entry.value())?;
                let Some(payload) = projection.visible_at(valid_time).cloned() else {
                    continue;
                };
                let identity = self
                    .load_edge_identity(element)
                    .await?
                    .ok_or(TemporalStoreError::MissingEdgeIdentity { edge: element })?;
                edges.push(edge_view(identity, payload));
            }
            Ok(edges)
        })
    }

    pub fn scan_edges_as_of<'a>(
        &'a self,
        graph: GraphId,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<EdgeView>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Identity,
                    edge_identity_graph_prefix(graph),
                ))
                .await?;
            let mut edges = Vec::new();
            for entry in entries {
                let GraphKey::EdgeIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedEdgeIdentityKey);
                };
                let identity = EdgeIdentity::decode(entry.value())?;
                if identity.element() != element {
                    return Err(TemporalStoreError::IdentityMismatch);
                }
                if let Some(payload) = self
                    .load_projection_at(element, transaction_time)
                    .await?
                    .as_ref()
                    .and_then(|projection| projection.visible_at(valid_time))
                    .cloned()
                {
                    edges.push(edge_view(identity, payload));
                }
            }
            Ok(edges)
        })
    }

    pub fn scan_edges_as_of_bounded<'a>(
        &'a self,
        graph: GraphId,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
        max_entries: usize,
        max_bytes: u64,
    ) -> TemporalStoreFuture<'a, (Vec<EdgeView>, u64, usize)> {
        Box::pin(async move {
            let (entries, mut scanned_bytes) = self
                .scan_identity_entries(edge_identity_graph_prefix(graph), max_entries, max_bytes)
                .await?;
            let entry_count = entries.len();
            let mut edges = Vec::new();
            for entry in entries {
                let GraphKey::EdgeIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedEdgeIdentityKey);
                };
                let identity = EdgeIdentity::decode(entry.value())?;
                if identity.element() != element {
                    return Err(TemporalStoreError::IdentityMismatch);
                }
                if let Some(payload) = self
                    .load_projection_at(element, transaction_time)
                    .await?
                    .as_ref()
                    .and_then(|projection| projection.visible_at(valid_time))
                    .cloned()
                {
                    let payload_bytes = payload
                        .encode()
                        .map_err(|_| TemporalStoreError::ScanByteLimit)?
                        .len();
                    scanned_bytes = charge_scan_bytes(scanned_bytes, payload_bytes, max_bytes)?;
                    edges.push(edge_view(identity, payload));
                }
            }
            Ok((edges, scanned_bytes, entry_count))
        })
    }

    pub fn scan_edge_history_as_of<'a>(
        &'a self,
        graph: GraphId,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<(EdgeIdentity, HistoryEntry)>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Identity,
                    edge_identity_graph_prefix(graph),
                ))
                .await?;
            let mut history = Vec::new();
            for entry in entries {
                let GraphKey::EdgeIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedEdgeIdentityKey);
                };
                let identity = EdgeIdentity::decode(entry.value())?;
                for version in self
                    .load_history_chain_at(element, transaction_time)
                    .await?
                {
                    history.push((identity.clone(), version));
                }
            }
            Ok(history)
        })
    }

    pub fn scan_edge_history_as_of_bounded<'a>(
        &'a self,
        graph: GraphId,
        transaction_time: TransactionTime,
        max_events: usize,
        max_bytes: u64,
    ) -> TemporalStoreFuture<'a, (Vec<(EdgeIdentity, HistoryEntry)>, u64)> {
        Box::pin(async move {
            let (entries, mut scanned_bytes) = self
                .scan_identity_entries(edge_identity_graph_prefix(graph), max_events, max_bytes)
                .await?;
            let mut history = Vec::new();
            for entry in entries {
                let GraphKey::EdgeIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedEdgeIdentityKey);
                };
                let identity = EdgeIdentity::decode(entry.value())?;
                for version in self
                    .load_history_chain_at(element, transaction_time)
                    .await?
                {
                    if history.len() == max_events {
                        return Err(TemporalStoreError::ScanEntryLimit);
                    }
                    scanned_bytes =
                        charge_scan_bytes(scanned_bytes, version.encode()?.len(), max_bytes)?;
                    history.push((identity.clone(), version));
                }
            }
            Ok((history, scanned_bytes))
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
            let spans = [
                KeySpan::prefix(
                    Keyspace::AdjOut,
                    out_adjacency_prefix(graph, partition, source),
                ),
                KeySpan::prefix(
                    Keyspace::AdjOut,
                    cross_out_adjacency_prefix(graph, partition, source),
                ),
            ];
            let mut edges = Vec::new();
            for span in spans {
                for entry in self.adapter.scan(&span).await? {
                    let (element, edge_type, source, destination) =
                        match decode_graph_key(entry.key())? {
                            GraphKey::OutAdjacency {
                                graph,
                                partition,
                                source,
                                edge_type,
                                destination,
                                edge,
                                ..
                            } => (
                                ElementRef::edge(graph, partition, edge),
                                edge_type,
                                ElementRef::vertex(graph, partition, source),
                                ElementRef::vertex(graph, partition, destination),
                            ),
                            GraphKey::CrossOutAdjacency {
                                graph,
                                partition,
                                source,
                                edge_type,
                                destination_partition,
                                destination,
                                edge_partition,
                                edge,
                                ..
                            } => (
                                ElementRef::edge(graph, edge_partition, edge),
                                edge_type,
                                ElementRef::vertex(graph, partition, source),
                                ElementRef::vertex(graph, destination_partition, destination),
                            ),
                            _ => return Err(TemporalStoreError::UnexpectedAdjacencyKey),
                        };
                    let projection = ProjectionRecord::decode(entry.value())?;
                    if let Some(payload) = projection.visible_at(valid_time) {
                        edges.push(EdgeView {
                            element,
                            edge_type,
                            source,
                            destination,
                            payload: payload.clone(),
                        });
                    }
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
            let spans = [
                KeySpan::prefix(
                    Keyspace::AdjIn,
                    in_adjacency_prefix(graph, partition, destination),
                ),
                KeySpan::prefix(
                    Keyspace::AdjIn,
                    cross_in_adjacency_prefix(graph, partition, destination),
                ),
            ];
            let mut edges = Vec::new();
            for span in spans {
                for entry in self.adapter.scan(&span).await? {
                    let (element, edge_type, source, destination) =
                        match decode_graph_key(entry.key())? {
                            GraphKey::InAdjacency {
                                graph,
                                partition,
                                destination,
                                edge_type,
                                source,
                                edge,
                                ..
                            } => (
                                ElementRef::edge(graph, partition, edge),
                                edge_type,
                                ElementRef::vertex(graph, partition, source),
                                ElementRef::vertex(graph, partition, destination),
                            ),
                            GraphKey::CrossInAdjacency {
                                graph,
                                partition,
                                destination,
                                edge_type,
                                source_partition,
                                source,
                                edge_partition,
                                edge,
                                ..
                            } => (
                                ElementRef::edge(graph, edge_partition, edge),
                                edge_type,
                                ElementRef::vertex(graph, source_partition, source),
                                ElementRef::vertex(graph, partition, destination),
                            ),
                            _ => return Err(TemporalStoreError::UnexpectedAdjacencyKey),
                        };
                    let projection = ProjectionRecord::decode(entry.value())?;
                    if let Some(payload) = projection.visible_at(valid_time) {
                        edges.push(EdgeView {
                            element,
                            edge_type,
                            source,
                            destination,
                            payload: payload.clone(),
                        });
                    }
                }
            }
            Ok(edges)
        })
    }

    pub fn expand_out_as_of<'a>(
        &'a self,
        graph: GraphId,
        partition: PartitionId,
        source: ElementId,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<EdgeView>> {
        self.expand_as_of(graph, partition, source, valid_time, transaction_time, true)
    }

    pub fn expand_in_as_of<'a>(
        &'a self,
        graph: GraphId,
        partition: PartitionId,
        destination: ElementId,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
    ) -> TemporalStoreFuture<'a, Vec<EdgeView>> {
        self.expand_as_of(
            graph,
            partition,
            destination,
            valid_time,
            transaction_time,
            false,
        )
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

    async fn load_edge_identity(
        &self,
        element: ElementRef,
    ) -> Result<Option<EdgeIdentity>, TemporalStoreError> {
        let mut values = self
            .adapter
            .multi_get(&[edge_identity_key(element)])
            .await?;
        values
            .pop()
            .flatten()
            .map(|bytes| EdgeIdentity::decode(&bytes).map_err(TemporalStoreError::from))
            .transpose()
    }

    async fn load_vertex_identity(
        &self,
        element: ElementRef,
    ) -> Result<Option<VertexIdentity>, TemporalStoreError> {
        let mut values = self
            .adapter
            .multi_get(&[vertex_identity_key(element)])
            .await?;
        values
            .pop()
            .flatten()
            .map(|bytes| VertexIdentity::decode(&bytes).map_err(Into::into))
            .transpose()
    }

    fn expand_as_of<'a>(
        &'a self,
        graph: GraphId,
        partition: PartitionId,
        endpoint: ElementId,
        valid_time: ValidTime,
        transaction_time: TransactionTime,
        outgoing: bool,
    ) -> TemporalStoreFuture<'a, Vec<EdgeView>> {
        Box::pin(async move {
            let entries = self
                .adapter
                .scan(&KeySpan::prefix(
                    Keyspace::Identity,
                    edge_identity_prefix(graph, partition),
                ))
                .await?;
            let mut edges = Vec::new();
            for entry in entries {
                let GraphKey::EdgeIdentity(element) = decode_graph_key(entry.key())? else {
                    return Err(TemporalStoreError::UnexpectedEdgeIdentityKey);
                };
                let identity = EdgeIdentity::decode(entry.value())?;
                if identity.element() != element {
                    return Err(TemporalStoreError::IdentityMismatch);
                }
                let matches_endpoint = if outgoing {
                    identity.source() == endpoint
                } else {
                    identity.destination() == endpoint
                };
                if !matches_endpoint {
                    continue;
                }
                if let Some(payload) = self
                    .load_projection_at(element, transaction_time)
                    .await?
                    .as_ref()
                    .and_then(|projection| projection.visible_at(valid_time))
                    .cloned()
                {
                    edges.push(edge_view(identity, payload));
                }
            }
            Ok(edges)
        })
    }

    async fn validate_incident_edge_coverage(
        &self,
        vertex: ElementRef,
        endpoint_projection: &ProjectionRecord,
        staged_edges: &BTreeMap<ElementRef, ProjectionRecord>,
    ) -> Result<(), TemporalStoreError> {
        let mut incident_edges = BTreeMap::new();
        let spans = [
            KeySpan::prefix(
                Keyspace::AdjOut,
                out_adjacency_prefix(vertex.graph(), vertex.partition(), vertex.id()),
            ),
            KeySpan::prefix(
                Keyspace::AdjIn,
                in_adjacency_prefix(vertex.graph(), vertex.partition(), vertex.id()),
            ),
            KeySpan::prefix(
                Keyspace::AdjOut,
                cross_out_adjacency_prefix(vertex.graph(), vertex.partition(), vertex.id()),
            ),
            KeySpan::prefix(
                Keyspace::AdjIn,
                cross_in_adjacency_prefix(vertex.graph(), vertex.partition(), vertex.id()),
            ),
        ];
        for span in spans {
            for entry in self.adapter.scan(&span).await? {
                let edge = match decode_graph_key(entry.key())? {
                    GraphKey::OutAdjacency {
                        graph,
                        partition,
                        edge,
                        ..
                    }
                    | GraphKey::InAdjacency {
                        graph,
                        partition,
                        edge,
                        ..
                    } => ElementRef::edge(graph, partition, edge),
                    GraphKey::CrossOutAdjacency {
                        graph,
                        edge_partition,
                        edge,
                        ..
                    }
                    | GraphKey::CrossInAdjacency {
                        graph,
                        edge_partition,
                        edge,
                        ..
                    } => ElementRef::edge(graph, edge_partition, edge),
                    _ => return Err(TemporalStoreError::UnexpectedAdjacencyKey),
                };
                incident_edges.insert(edge, ProjectionRecord::decode(entry.value())?);
            }
        }

        for (edge, stored_projection) in incident_edges {
            let effective_projection = staged_edges.get(&edge).unwrap_or(&stored_projection);
            if effective_projection
                .segments()
                .iter()
                .any(|segment| !projection_covers(endpoint_projection, segment.valid()))
            {
                return Err(TemporalStoreError::EndpointStillReferenced { vertex, edge });
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

    async fn validate_commit_frontier(
        &self,
        element: ElementRef,
        current: Option<&ProjectionRecord>,
        context: PrepareContext,
        changed_valid: Interval<ValidTime>,
        replay_log_index: Option<u64>,
    ) -> Result<(), TemporalStoreError> {
        if let Some(current) = current {
            if current.commit_ts() > context.commit_ts {
                return Err(TemporalStoreError::NonMonotonicCommit);
            }
            if current.commit_ts() == context.commit_ts {
                let is_unapplied_commit = match replay_log_index {
                    Some(log_index) => log_index > self.adapter.applied_log_index()?,
                    None => true,
                };
                if is_unapplied_commit {
                    return Err(TemporalStoreError::NonMonotonicCommit);
                }
            }
        }
        let intervening = self
            .load_history_between(element, context.read_ts, context.commit_ts)
            .await?;
        if intervening.iter().any(|entry| {
            entry.commit_ts() > context.read_ts
                && entry.commit_ts() < context.commit_ts
                && entry.changed_valid().overlaps(&changed_valid)
        }) {
            return Err(TemporalStoreError::WriteConflict);
        }
        Ok(())
    }

    async fn load_current_projection(
        &self,
        element: ElementRef,
    ) -> Result<Option<ProjectionRecord>, TemporalStoreError> {
        let key = match element.kind() {
            ElementKind::Vertex => current_vertex_key(element),
            ElementKind::Edge => current_edge_key(element),
        };
        let mut values = self.adapter.multi_get(&[key]).await?;
        values
            .pop()
            .flatten()
            .map(|bytes| ProjectionRecord::decode(&bytes).map_err(TemporalStoreError::from))
            .transpose()
    }

    async fn load_history_between(
        &self,
        element: ElementRef,
        read_ts: TransactionTime,
        commit_ts: TransactionTime,
    ) -> Result<Vec<HistoryEntry>, TemporalStoreError> {
        let start = history_anchor_key(element, commit_ts, 0)
            .as_bytes()
            .to_vec();
        let end = history_anchor_key(element, read_ts, 0).as_bytes().to_vec();
        let span = KeySpan::range(Keyspace::History, start, Some(end))
            .expect("commit timestamp follows read timestamp in reversed key order");
        self.adapter
            .scan(&span)
            .await?
            .into_iter()
            .map(|entry| HistoryEntry::decode(entry.value()).map_err(TemporalStoreError::from))
            .collect()
    }

    async fn load_history_chain_at(
        &self,
        element: ElementRef,
        transaction_time: TransactionTime,
    ) -> Result<Vec<HistoryEntry>, TemporalStoreError> {
        let prefix = history_prefix(element);
        let start = history_anchor_key(element, transaction_time, 0)
            .as_bytes()
            .to_vec();
        let first_span = KeySpan::prefix_from(Keyspace::History, prefix.clone(), start.clone())
            .expect("history seek key always starts with its element prefix")
            .with_limit(1)
            .expect("history seek limit is positive");
        let mut first = self.adapter.scan(&first_span).await?;
        let Some(first) = first.pop() else {
            return Ok(Vec::new());
        };
        let first = HistoryEntry::decode(first.value())?;
        if first.is_anchor() {
            return Ok(vec![first]);
        }

        let chain_span = KeySpan::prefix_from(Keyspace::History, prefix, start)
            .expect("history seek key always starts with its element prefix")
            .with_limit(MAX_CHAIN_ENTRIES)
            .expect("history chain limit is positive");
        let mut chain = Vec::new();
        for entry in self.adapter.scan(&chain_span).await? {
            let entry = HistoryEntry::decode(entry.value())?;
            let is_anchor = entry.is_anchor();
            chain.push(entry);
            if is_anchor {
                break;
            }
        }
        Ok(chain)
    }

    async fn scan_identity_entries(
        &self,
        prefix: Vec<u8>,
        max_entries: usize,
        max_bytes: u64,
    ) -> Result<(Vec<storage_api::KeyValue>, u64), TemporalStoreError> {
        if max_entries == 0 {
            let span = KeySpan::prefix(Keyspace::Identity, prefix)
                .with_limit(1)
                .expect("identity emptiness probe limit is positive");
            return if self.adapter.scan(&span).await?.is_empty() {
                Ok((Vec::new(), 0))
            } else {
                Err(TemporalStoreError::ScanEntryLimit)
            };
        }
        if max_bytes == 0 {
            let span = KeySpan::prefix(Keyspace::Identity, prefix)
                .with_limit(1)
                .expect("identity emptiness probe limit is positive")
                .with_max_bytes(1)
                .expect("identity emptiness probe byte limit is positive");
            return match self.adapter.scan(&span).await {
                Ok(entries) if entries.is_empty() => Ok((Vec::new(), 0)),
                Ok(_) | Err(AdapterError::ScanByteLimit { .. }) => {
                    Err(TemporalStoreError::ScanByteLimit)
                }
                Err(error) => Err(error.into()),
            };
        }
        let limit = max_entries
            .checked_add(1)
            .ok_or(TemporalStoreError::InvalidScanBudget)?;
        let span = KeySpan::prefix(Keyspace::Identity, prefix)
            .with_limit(limit)
            .expect("bounded identity scan limit is positive")
            .with_max_bytes(max_bytes)
            .expect("bounded identity scan byte limit is positive");
        let entries = self.adapter.scan(&span).await?;
        if entries.len() > max_entries {
            return Err(TemporalStoreError::ScanEntryLimit);
        }
        let mut scanned_bytes = 0_u64;
        for entry in &entries {
            let entry_bytes = entry
                .key()
                .as_bytes()
                .len()
                .checked_add(entry.value().len())
                .ok_or(TemporalStoreError::ScanByteLimit)?;
            scanned_bytes = scanned_bytes
                .checked_add(
                    u64::try_from(entry_bytes).map_err(|_| TemporalStoreError::ScanByteLimit)?,
                )
                .ok_or(TemporalStoreError::ScanByteLimit)?;
            if scanned_bytes > max_bytes {
                return Err(TemporalStoreError::ScanByteLimit);
            }
        }
        Ok((entries, scanned_bytes))
    }

    async fn load_projection_at(
        &self,
        element: ElementRef,
        transaction_time: TransactionTime,
    ) -> Result<Option<ProjectionRecord>, TemporalStoreError> {
        let entries = self
            .load_history_chain_at(element, transaction_time)
            .await?;
        reconstruct(&entries)
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
        let before = self.load_projection_at(element, from_transaction).await?;
        let after = self.load_projection_at(element, to_transaction).await?;
        Ok(diff_projections(before.as_ref(), after.as_ref()))
    }
}

fn charge_scan_bytes(
    current: u64,
    additional: usize,
    maximum: u64,
) -> Result<u64, TemporalStoreError> {
    let next = current
        .checked_add(u64::try_from(additional).map_err(|_| TemporalStoreError::ScanByteLimit)?)
        .ok_or(TemporalStoreError::ScanByteLimit)?;
    if next > maximum {
        return Err(TemporalStoreError::ScanByteLimit);
    }
    Ok(next)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TemporalStoreError {
    EmptyTransaction,
    TooManyMutations,
    InvalidCommitOrder,
    NonMonotonicCommit,
    WriteConflict,
    IdentityMismatch,
    WrongElementKind,
    InvalidEdgeEndpoints,
    DuplicateElementOperation {
        element: ElementRef,
    },
    OverlayIntervalConflict {
        element: ElementRef,
    },
    EndpointNotPresent {
        vertex: ElementRef,
    },
    EndpointStillReferenced {
        vertex: ElementRef,
        edge: ElementRef,
    },
    MissingEdgeIdentity {
        edge: ElementRef,
    },
    UnexpectedCurrentKey,
    UnexpectedVertexIdentityKey,
    UnexpectedEdgeIdentityKey,
    UnexpectedAdjacencyKey,
    UnexpectedHistoryAnchor,
    MissingHistoryAnchor,
    HistoryChainTooDeep,
    InvalidDiffOrder,
    InvalidScanBudget,
    ScanEntryLimit,
    ScanByteLimit,
    ScanResponseByteLimit {
        limit: u64,
        required: u64,
    },
    Adapter(AdapterError),
    Record(RecordCodecError),
    Key(KeyCodecError),
}

impl Display for TemporalStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyTransaction => formatter.write_str("temporal transaction has no operations"),
            Self::TooManyMutations => {
                formatter.write_str("temporal transaction exceeds the mutation sequence space")
            }
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
            Self::InvalidEdgeEndpoints => formatter.write_str(
                "edge endpoints must be vertices in the edge graph and the edge must be owned by its source partition",
            ),
            Self::DuplicateElementOperation { element } => {
                write!(
                    formatter,
                    "temporal transaction repeats element {element:?}"
                )
            }
            Self::OverlayIntervalConflict { element } => {
                write!(
                    formatter,
                    "transaction overlay cannot merge distinct valid intervals for {element:?}"
                )
            }
            Self::EndpointNotPresent { vertex } => {
                write!(
                    formatter,
                    "edge endpoint is not present for its full valid interval: {vertex:?}"
                )
            }
            Self::EndpointStillReferenced { vertex, edge } => {
                write!(
                    formatter,
                    "vertex deletion would leave incident edge {edge:?} without endpoint {vertex:?}"
                )
            }
            Self::MissingEdgeIdentity { edge } => {
                write!(formatter, "visible edge has no identity record: {edge:?}")
            }
            Self::UnexpectedCurrentKey => {
                formatter.write_str("Current scan returned an unexpected key type")
            }
            Self::UnexpectedVertexIdentityKey => {
                formatter.write_str("vertex identity scan returned an unexpected key type")
            }
            Self::UnexpectedEdgeIdentityKey => {
                formatter.write_str("edge identity scan returned an unexpected key type")
            }
            Self::UnexpectedAdjacencyKey => {
                formatter.write_str("adjacency scan returned an unexpected key type")
            }
            Self::UnexpectedHistoryAnchor => {
                formatter.write_str("history delta chain contains an unexpected nested anchor")
            }
            Self::MissingHistoryAnchor => {
                formatter.write_str("history delta chain does not terminate at an anchor")
            }
            Self::HistoryChainTooDeep => {
                formatter.write_str("history delta chain exceeds the configured replay limit")
            }
            Self::InvalidDiffOrder => {
                formatter.write_str("DIFF start transaction must not follow its end")
            }
            Self::InvalidScanBudget => formatter.write_str("scan budget must be positive"),
            Self::ScanEntryLimit => formatter.write_str("scan exceeds its entry budget"),
            Self::ScanByteLimit => formatter.write_str("scan exceeds its byte budget"),
            Self::ScanResponseByteLimit { limit, required } => write!(
                formatter,
                "scan response body requires {required} wire bytes above limit {limit}"
            ),
            Self::Adapter(error) => Display::fmt(error, formatter),
            Self::Record(error) => Display::fmt(error, formatter),
            Self::Key(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for TemporalStoreError {}

impl From<AdapterError> for TemporalStoreError {
    fn from(value: AdapterError) -> Self {
        match value {
            AdapterError::ScanByteLimit { .. } => Self::ScanByteLimit,
            AdapterError::ScanResponseByteLimit { limit, required } => {
                Self::ScanResponseByteLimit { limit, required }
            }
            value => Self::Adapter(value),
        }
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

fn edge_view(identity: EdgeIdentity, payload: CanonicalElement) -> EdgeView {
    EdgeView {
        element: identity.element(),
        edge_type: identity.edge_type(),
        source: identity.source_ref(),
        destination: identity.destination_ref(),
        payload,
    }
}

fn vertex_view(identity: VertexIdentity, payload: CanonicalElement) -> VertexView {
    VertexView {
        element: identity.element(),
        label: identity.label(),
        payload,
    }
}

fn append_vertex_segments(
    output: &mut Vec<VertexTemporalSegment>,
    identity: VertexIdentity,
    projection: &ProjectionRecord,
    window: Interval<ValidTime>,
) {
    for segment in projection.segments() {
        let Some(valid) = intersect_valid_intervals(segment.valid(), window) else {
            continue;
        };
        output.push(VertexTemporalSegment {
            element: identity.element(),
            label: identity.label(),
            valid,
            payload: segment.payload().clone(),
        });
    }
}

fn append_edge_segments(
    output: &mut Vec<EdgeTemporalSegment>,
    identity: EdgeIdentity,
    projection: &ProjectionRecord,
    window: Interval<ValidTime>,
) {
    for segment in projection.segments() {
        let Some(valid) = intersect_valid_intervals(segment.valid(), window) else {
            continue;
        };
        output.push(EdgeTemporalSegment {
            element: identity.element(),
            edge_type: identity.edge_type(),
            source: identity.source_ref(),
            destination: identity.destination_ref(),
            valid,
            payload: segment.payload().clone(),
        });
    }
}

fn sort_vertex_segments(segments: &mut [VertexTemporalSegment]) {
    segments.sort_by_key(|segment| (segment.element, segment.valid.start()));
}

fn sort_edge_segments(segments: &mut [EdgeTemporalSegment]) {
    segments.sort_by_key(|segment| (segment.element, segment.valid.start()));
}

fn intersect_valid_intervals(
    left: Interval<ValidTime>,
    right: Interval<ValidTime>,
) -> Option<Interval<ValidTime>> {
    let start = left.start().max(right.start());
    let end = match (left.end(), right.end()) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(end), None) | (None, Some(end)) => Some(end),
        (None, None) => None,
    };
    Interval::new(start, end).ok()
}

fn require_edge(element: ElementRef) -> Result<(), TemporalStoreError> {
    if element.kind() == ElementKind::Edge {
        Ok(())
    } else {
        Err(TemporalStoreError::WrongElementKind)
    }
}

fn projection_covers(projection: &ProjectionRecord, required: Interval<ValidTime>) -> bool {
    let mut cursor = required.start();
    for segment in projection.segments() {
        if segment.valid().end().is_some_and(|end| end <= cursor) {
            continue;
        }
        if segment.valid().start() > cursor {
            return false;
        }
        match segment.valid().end() {
            None => return true,
            Some(end) => {
                if required
                    .end()
                    .is_some_and(|required_end| required_end <= end)
                {
                    return true;
                }
                cursor = end;
            }
        }
    }
    false
}
