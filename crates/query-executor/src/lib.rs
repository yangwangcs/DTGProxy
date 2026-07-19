#![forbid(unsafe_code)]

mod distributed;
#[cfg(feature = "shard-runtime")]
mod shard_query;
pub mod v2;

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::fmt::{self, Display, Formatter};
use std::future::Future;
use std::pin::Pin;

use storage_api::StorageAdapter;
use temporal_ir::{
    ExpandDirection, PlanBody, PlanError, PointOperator, ScanOperator, TemporalPlan,
    TemporalSelector,
};
use temporal_storage::{
    EdgeTypeId, EdgeView, ElementId, ElementKind, ElementRef, LabelId, TemporalChange,
    TemporalChangeKind, TemporalStore, TemporalStoreError, VertexView,
};
use temporal_types::{CanonicalElement, CodecError};

pub use distributed::{
    DistributedQueryError, ShardQueryBatch, SnapshotToken, merge_distributed_results,
};
#[cfg(feature = "shard-runtime")]
pub use shard_query::{ShardQueryError, ShardQueryExecutor};

pub type ExecutorFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, ExecutorError>> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VertexRecord {
    element: ElementRef,
    label: Option<LabelId>,
    payload: CanonicalElement,
}

impl VertexRecord {
    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn label(&self) -> Option<LabelId> {
        self.label
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
    }
}

impl From<VertexView> for VertexRecord {
    fn from(value: VertexView) -> Self {
        Self {
            element: value.element(),
            label: Some(value.label()),
            payload: value.payload().clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EdgeRecord {
    element: ElementRef,
    edge_type: EdgeTypeId,
    source: ElementId,
    destination: ElementId,
    payload: CanonicalElement,
}

impl EdgeRecord {
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

impl From<EdgeView> for EdgeRecord {
    fn from(value: EdgeView) -> Self {
        Self {
            element: value.element(),
            edge_type: value.edge_type(),
            source: value.source(),
            destination: value.destination(),
            payload: value.payload().clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChangeRecord {
    element: ElementRef,
    change: TemporalChange,
}

impl ChangeRecord {
    #[must_use]
    pub const fn element(&self) -> ElementRef {
        self.element
    }

    #[must_use]
    pub const fn change(&self) -> &TemporalChange {
        &self.change
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryRecord {
    Vertex(VertexRecord),
    Edge(EdgeRecord),
    Change(ChangeRecord),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct QueryResult {
    records: Vec<QueryRecord>,
}

impl QueryResult {
    #[must_use]
    pub fn records(&self) -> &[QueryRecord] {
        &self.records
    }

    #[must_use]
    pub fn into_records(self) -> Vec<QueryRecord> {
        self.records
    }

    #[must_use]
    pub fn from_records(records: Vec<QueryRecord>) -> Self {
        Self { records }
    }

    pub fn to_canonical_json(&self) -> Result<String, CodecError> {
        let mut output = String::from("{\"version\":1,\"records\":[");
        for (index, record) in self.records.iter().enumerate() {
            if index != 0 {
                output.push(',');
            }
            match record {
                QueryRecord::Vertex(record) => {
                    output.push_str("{\"type\":\"vertex\",");
                    push_element_identity(&mut output, record.element);
                    write!(
                        output,
                        ",\"payload_dtp1\":\"{}\"}}",
                        payload_hex(&record.payload)?
                    )
                    .expect("writing to String cannot fail");
                }
                QueryRecord::Edge(record) => {
                    output.push_str("{\"type\":\"edge\",");
                    push_element_identity(&mut output, record.element);
                    write!(
                        output,
                        ",\"edge_type\":\"{}\",\"source_id\":\"{}\",\"destination_id\":\"{}\",\"payload_dtp1\":\"{}\"}}",
                        record.edge_type.value(),
                        record.source.value(),
                        record.destination.value(),
                        payload_hex(&record.payload)?
                    )
                    .expect("writing to String cannot fail");
                }
                QueryRecord::Change(record) => {
                    output.push_str("{\"type\":\"change\",\"element_kind\":\"");
                    output.push_str(match record.element.kind() {
                        ElementKind::Vertex => "vertex",
                        ElementKind::Edge => "edge",
                    });
                    output.push_str("\",");
                    push_element_identity(&mut output, record.element);
                    write!(
                        output,
                        ",\"valid_start_micros\":\"{}\",\"valid_end_micros\":",
                        record.change.valid().start().as_micros()
                    )
                    .expect("writing to String cannot fail");
                    if let Some(end) = record.change.valid().end() {
                        write!(output, "\"{}\"", end.as_micros())
                            .expect("writing to String cannot fail");
                    } else {
                        output.push_str("null");
                    }
                    match record.change.kind() {
                        TemporalChangeKind::Added { after } => write!(
                            output,
                            ",\"change\":\"added\",\"after_dtp1\":\"{}\"}}",
                            payload_hex(after)?
                        ),
                        TemporalChangeKind::Removed { before } => write!(
                            output,
                            ",\"change\":\"removed\",\"before_dtp1\":\"{}\"}}",
                            payload_hex(before)?
                        ),
                        TemporalChangeKind::Changed { before, after } => write!(
                            output,
                            ",\"change\":\"changed\",\"before_dtp1\":\"{}\",\"after_dtp1\":\"{}\"}}",
                            payload_hex(before)?,
                            payload_hex(after)?
                        ),
                    }
                    .expect("writing to String cannot fail");
                }
            }
        }
        output.push_str("]}");
        Ok(output)
    }
}

fn push_element_identity(output: &mut String, element: ElementRef) {
    write!(
        output,
        "\"graph\":\"{}\",\"partition\":\"{}\",\"element_id\":\"{}\"",
        element.graph().value(),
        element.partition().value(),
        element.id().value()
    )
    .expect("writing to String cannot fail");
}

fn payload_hex(payload: &CanonicalElement) -> Result<String, CodecError> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = payload.encode()?;
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(output)
}

pub struct LocalExecutor<A> {
    store: TemporalStore<A>,
}

impl<A> LocalExecutor<A>
where
    A: StorageAdapter,
{
    #[must_use]
    pub const fn new(store: TemporalStore<A>) -> Self {
        Self { store }
    }

    #[must_use]
    pub const fn store(&self) -> &TemporalStore<A> {
        &self.store
    }

    pub fn execute<'a>(&'a self, plan: &'a TemporalPlan) -> ExecutorFuture<'a, QueryResult> {
        Box::pin(async move {
            plan.validate()?;
            let records = match plan.body() {
                PlanBody::Point {
                    operator,
                    valid_time,
                    transaction,
                    limit,
                } => {
                    self.execute_point(
                        plan,
                        *operator,
                        *valid_time,
                        *transaction,
                        usize::try_from(*limit).expect("u32 result limit fits usize"),
                    )
                    .await?
                }
                PlanBody::Diff {
                    operator,
                    from_transaction,
                    to_transaction,
                    limit,
                } => {
                    let element = operator.element(plan.scope());
                    let changes = match element.kind() {
                        ElementKind::Vertex => {
                            self.store
                                .diff_vertex(element, *from_transaction, *to_transaction)
                                .await?
                        }
                        ElementKind::Edge => {
                            self.store
                                .diff_edge(element, *from_transaction, *to_transaction)
                                .await?
                        }
                    };
                    changes
                        .into_iter()
                        .take(usize::try_from(*limit).expect("u32 result limit fits usize"))
                        .map(|change| QueryRecord::Change(ChangeRecord { element, change }))
                        .collect()
                }
                PlanBody::Scan {
                    operator,
                    valid_time,
                    transaction,
                    limit,
                } => {
                    self.execute_scan(
                        plan,
                        *operator,
                        *valid_time,
                        *transaction,
                        usize::try_from(*limit).expect("u32 result limit fits usize"),
                    )
                    .await?
                }
            };
            Ok(QueryResult { records })
        })
    }

    async fn execute_scan(
        &self,
        plan: &TemporalPlan,
        operator: ScanOperator,
        valid_time: temporal_types::ValidTime,
        transaction: TemporalSelector,
        limit: usize,
    ) -> Result<Vec<QueryRecord>, ExecutorError> {
        let graph = plan.scope().graph();
        match operator {
            ScanOperator::Vertices => {
                let vertices = match transaction {
                    TemporalSelector::Current => {
                        self.store.scan_vertices_current(graph, valid_time).await?
                    }
                    TemporalSelector::AsOf(transaction_time) => {
                        self.store
                            .scan_vertices_as_of(graph, valid_time, transaction_time)
                            .await?
                    }
                };
                Ok(vertices
                    .into_iter()
                    .take(limit)
                    .map(|(element, payload)| {
                        QueryRecord::Vertex(VertexRecord {
                            element,
                            label: None,
                            payload,
                        })
                    })
                    .collect())
            }
            ScanOperator::Edges => {
                let edges = match transaction {
                    TemporalSelector::Current => {
                        self.store.scan_edges_current(graph, valid_time).await?
                    }
                    TemporalSelector::AsOf(transaction_time) => {
                        self.store
                            .scan_edges_as_of(graph, valid_time, transaction_time)
                            .await?
                    }
                };
                Ok(edges
                    .into_iter()
                    .take(limit)
                    .map(EdgeRecord::from)
                    .map(QueryRecord::Edge)
                    .collect())
            }
        }
    }

    async fn execute_point(
        &self,
        plan: &TemporalPlan,
        operator: PointOperator,
        valid_time: temporal_types::ValidTime,
        transaction: TemporalSelector,
        limit: usize,
    ) -> Result<Vec<QueryRecord>, ExecutorError> {
        let scope = plan.scope();
        match operator {
            PointOperator::VertexById(id) => {
                let element = scope.element(ElementKind::Vertex, id);
                let payload = match transaction {
                    TemporalSelector::Current => {
                        self.store.vertex_current(element, valid_time).await?
                    }
                    TemporalSelector::AsOf(transaction_time) => {
                        self.store
                            .vertex_as_of(element, valid_time, transaction_time)
                            .await?
                    }
                };
                Ok(payload
                    .into_iter()
                    .map(|payload| {
                        QueryRecord::Vertex(VertexRecord {
                            element,
                            label: None,
                            payload,
                        })
                    })
                    .collect())
            }
            PointOperator::EdgeById(id) => {
                let element = scope.element(ElementKind::Edge, id);
                let edge = match transaction {
                    TemporalSelector::Current => {
                        self.store.edge_view_current(element, valid_time).await?
                    }
                    TemporalSelector::AsOf(transaction_time) => {
                        self.store
                            .edge_view_as_of(element, valid_time, transaction_time)
                            .await?
                    }
                };
                Ok(edge
                    .into_iter()
                    .map(EdgeRecord::from)
                    .map(QueryRecord::Edge)
                    .collect())
            }
            PointOperator::Expand { origin, direction } => {
                let mut edges = BTreeMap::new();
                if matches!(direction, ExpandDirection::Out | ExpandDirection::Both) {
                    let outgoing = match transaction {
                        TemporalSelector::Current => {
                            self.store
                                .expand_out_current(
                                    scope.graph(),
                                    scope.partition(),
                                    origin,
                                    valid_time,
                                )
                                .await?
                        }
                        TemporalSelector::AsOf(transaction_time) => {
                            self.store
                                .expand_out_as_of(
                                    scope.graph(),
                                    scope.partition(),
                                    origin,
                                    valid_time,
                                    transaction_time,
                                )
                                .await?
                        }
                    };
                    edges.extend(outgoing.into_iter().map(|edge| (edge.element(), edge)));
                }
                if matches!(direction, ExpandDirection::In | ExpandDirection::Both) {
                    let incoming = match transaction {
                        TemporalSelector::Current => {
                            self.store
                                .expand_in_current(
                                    scope.graph(),
                                    scope.partition(),
                                    origin,
                                    valid_time,
                                )
                                .await?
                        }
                        TemporalSelector::AsOf(transaction_time) => {
                            self.store
                                .expand_in_as_of(
                                    scope.graph(),
                                    scope.partition(),
                                    origin,
                                    valid_time,
                                    transaction_time,
                                )
                                .await?
                        }
                    };
                    edges.extend(incoming.into_iter().map(|edge| (edge.element(), edge)));
                }
                Ok(edges
                    .into_values()
                    .take(limit)
                    .map(EdgeRecord::from)
                    .map(QueryRecord::Edge)
                    .collect())
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExecutorError {
    Plan(PlanError),
    Store(TemporalStoreError),
}

impl Display for ExecutorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Plan(error) => Display::fmt(error, formatter),
            Self::Store(error) => Display::fmt(error, formatter),
        }
    }
}

impl Error for ExecutorError {}

impl From<PlanError> for ExecutorError {
    fn from(value: PlanError) -> Self {
        Self::Plan(value)
    }
}

impl From<TemporalStoreError> for ExecutorError {
    fn from(value: TemporalStoreError) -> Self {
        Self::Store(value)
    }
}
