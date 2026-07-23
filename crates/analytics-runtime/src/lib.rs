#![forbid(unsafe_code)]

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use analytics_api::{
    DeltaEdge, DeltaGraph, DeltaKind, DeltaVertex, EdgeId, EventEdge, EventGraph,
    GraphProjectionError, IntervalEdge, IntervalGraph, IntervalVertex, PartitionedSnapshotGraph,
    SnapshotEdge, SnapshotGraph, VertexId,
};
use storage_api::{AdapterError, StorageAdapter};
use temporal_storage::{ElementRef, GraphId, TemporalStore};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

mod provider;

pub use provider::BuiltInProvider;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProjectionLimits {
    max_vertices: usize,
    max_edges: usize,
    max_bytes: u64,
}

impl ProjectionLimits {
    pub fn new(
        max_vertices: usize,
        max_edges: usize,
        max_bytes: u64,
    ) -> Result<Self, ProjectionError> {
        if max_vertices == 0 || max_edges == 0 || max_bytes == 0 {
            return Err(ProjectionError::InvalidLimits);
        }
        Ok(Self {
            max_vertices,
            max_edges,
            max_bytes,
        })
    }

    #[must_use]
    pub const fn max_vertices(self) -> usize {
        self.max_vertices
    }

    #[must_use]
    pub const fn max_edges(self) -> usize {
        self.max_edges
    }

    #[must_use]
    pub const fn max_bytes(self) -> u64 {
        self.max_bytes
    }
}

pub async fn project_snapshot<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    valid_time: ValidTime,
    transaction_time: TransactionTime,
    directed: bool,
    weight_property: Option<u32>,
) -> Result<SnapshotGraph, ProjectionError>
where
    A: StorageAdapter,
{
    let (vertices, edges) =
        project_snapshot_parts(store, graph, valid_time, transaction_time, weight_property).await?;
    SnapshotGraph::new(vertices, edges, directed).map_err(ProjectionError::Graph)
}

pub async fn project_snapshot_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    valid_time: ValidTime,
    transaction_time: TransactionTime,
    directed: bool,
    weight_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<SnapshotGraph, ProjectionError>
where
    A: StorageAdapter,
{
    let (part, _) = project_snapshot_identity_part_bounded(
        store,
        graph,
        valid_time,
        transaction_time,
        weight_property,
        limits,
    )
    .await?;
    let (vertices, edges) = part.into_parts();
    SnapshotGraph::new(
        vertices.into_values().collect(),
        edges.into_values().collect(),
        directed,
    )
    .map_err(ProjectionError::Graph)
}

pub async fn project_snapshot_parts<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    valid_time: ValidTime,
    transaction_time: TransactionTime,
    weight_property: Option<u32>,
) -> Result<(Vec<VertexId>, Vec<SnapshotEdge>), ProjectionError>
where
    A: StorageAdapter,
{
    let part =
        project_snapshot_identity_part(store, graph, valid_time, transaction_time, weight_property)
            .await?;
    Ok((
        part.vertices.into_values().collect(),
        part.edges.into_values().collect(),
    ))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotProjectionPart {
    vertices: BTreeMap<ElementRef, VertexId>,
    edges: BTreeMap<ElementRef, SnapshotEdge>,
}

impl SnapshotProjectionPart {
    #[must_use]
    pub fn into_parts(
        self,
    ) -> (
        BTreeMap<ElementRef, VertexId>,
        BTreeMap<ElementRef, SnapshotEdge>,
    ) {
        (self.vertices, self.edges)
    }
}

pub async fn project_snapshot_identity_part<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    valid_time: ValidTime,
    transaction_time: TransactionTime,
    weight_property: Option<u32>,
) -> Result<SnapshotProjectionPart, ProjectionError>
where
    A: StorageAdapter,
{
    let vertices = store
        .scan_vertex_views_as_of(graph, valid_time, transaction_time)
        .await
        .map_err(|error| projection_scan_error(error, true))?
        .into_iter()
        .map(|vertex| {
            (
                vertex.element(),
                VertexId::new(vertex.element().id().value()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let edges = store
        .scan_edges_as_of(graph, valid_time, transaction_time)
        .await
        .map_err(|error| projection_scan_error(error, false))?
        .into_iter()
        .map(|edge| {
            let weight =
                match weight_property.and_then(|property| edge.payload().property(property)) {
                    None => 1.0,
                    Some(GraphValue::Integer(value)) => *value as f64,
                    Some(GraphValue::FloatBits(value)) => f64::from_bits(*value),
                    Some(_) => return Err(ProjectionError::InvalidWeightProperty),
                };
            Ok((
                edge.element(),
                SnapshotEdge::new(
                    VertexId::new(edge.source().value()),
                    VertexId::new(edge.destination().value()),
                    weight,
                )
                .map_err(ProjectionError::Graph)?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, ProjectionError>>()?;
    Ok(SnapshotProjectionPart { vertices, edges })
}

pub async fn project_snapshot_identity_part_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    valid_time: ValidTime,
    transaction_time: TransactionTime,
    weight_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<(SnapshotProjectionPart, u64), ProjectionError>
where
    A: StorageAdapter,
{
    let (vertex_views, vertex_bytes, _) = store
        .scan_vertex_views_as_of_bounded(
            graph,
            valid_time,
            transaction_time,
            limits.max_vertices,
            limits.max_bytes,
        )
        .await
        .map_err(|error| projection_scan_error(error, true))?;
    let remaining_bytes = limits
        .max_bytes
        .checked_sub(vertex_bytes)
        .ok_or(ProjectionError::ByteLimit)?;
    if remaining_bytes == 0 {
        return Err(ProjectionError::ByteLimit);
    }
    let (edge_views, edge_bytes, _) = store
        .scan_edges_as_of_bounded(
            graph,
            valid_time,
            transaction_time,
            limits.max_edges,
            remaining_bytes,
        )
        .await
        .map_err(|error| projection_scan_error(error, false))?;
    let vertices = vertex_views
        .into_iter()
        .map(|vertex| {
            (
                vertex.element(),
                VertexId::new(vertex.element().id().value()),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let edges = edge_views
        .into_iter()
        .map(|edge| snapshot_edge(edge, weight_property))
        .collect::<Result<BTreeMap<_, _>, ProjectionError>>()?;
    Ok((
        SnapshotProjectionPart { vertices, edges },
        vertex_bytes
            .checked_add(edge_bytes)
            .ok_or(ProjectionError::ByteLimit)?,
    ))
}

fn snapshot_edge(
    edge: temporal_storage::EdgeView,
    weight_property: Option<u32>,
) -> Result<(ElementRef, SnapshotEdge), ProjectionError> {
    let weight = match weight_property.and_then(|property| edge.payload().property(property)) {
        None => 1.0,
        Some(GraphValue::Integer(value)) => *value as f64,
        Some(GraphValue::FloatBits(value)) => f64::from_bits(*value),
        Some(_) => return Err(ProjectionError::InvalidWeightProperty),
    };
    Ok((
        edge.element(),
        SnapshotEdge::new(
            VertexId::new(edge.source().value()),
            VertexId::new(edge.destination().value()),
            weight,
        )
        .map_err(ProjectionError::Graph)?,
    ))
}

fn projection_scan_error(
    error: temporal_storage::TemporalStoreError,
    vertices: bool,
) -> ProjectionError {
    match error {
        temporal_storage::TemporalStoreError::ScanEntryLimit if vertices => {
            ProjectionError::VertexLimit
        }
        temporal_storage::TemporalStoreError::ScanEntryLimit => ProjectionError::EdgeLimit,
        temporal_storage::TemporalStoreError::ScanByteLimit => ProjectionError::ByteLimit,
        temporal_storage::TemporalStoreError::ScanResponseByteLimit { limit, required } => {
            ProjectionError::ResponseByteLimit { limit, required }
        }
        temporal_storage::TemporalStoreError::Adapter(AdapterError::Unavailable(message)) => {
            ProjectionError::StorageUnavailable(message)
        }
        error => ProjectionError::Storage(error.to_string()),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn project_interval_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    window: Interval<ValidTime>,
    transaction_time: TransactionTime,
    directed: bool,
    weight_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<IntervalGraph, ProjectionError>
where
    A: StorageAdapter,
{
    let (part, _) = project_interval_part_bounded(
        store,
        graph,
        window,
        transaction_time,
        weight_property,
        limits,
    )
    .await?;
    let (vertices, edges) = part.into_parts();
    IntervalGraph::new(vertices, edges, directed).map_err(ProjectionError::Graph)
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalProjectionPart {
    vertices: Vec<IntervalVertex>,
    edges: Vec<IntervalEdge>,
}

impl IntervalProjectionPart {
    #[must_use]
    pub fn into_parts(self) -> (Vec<IntervalVertex>, Vec<IntervalEdge>) {
        (self.vertices, self.edges)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IntervalProjectionUsage {
    vertices: usize,
    edges: usize,
    bytes: u64,
}

impl IntervalProjectionUsage {
    #[must_use]
    pub const fn vertices(self) -> usize {
        self.vertices
    }

    #[must_use]
    pub const fn edges(self) -> usize {
        self.edges
    }

    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn project_interval_part_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    window: Interval<ValidTime>,
    transaction_time: TransactionTime,
    weight_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<(IntervalProjectionPart, IntervalProjectionUsage), ProjectionError>
where
    A: StorageAdapter,
{
    let (vertex_segments, vertex_bytes, vertex_entries) = store
        .scan_vertex_segments_as_of_bounded(
            graph,
            window,
            transaction_time,
            limits.max_vertices,
            limits.max_bytes,
        )
        .await
        .map_err(|error| projection_scan_error(error, true))?;
    let remaining_bytes = limits
        .max_bytes
        .checked_sub(vertex_bytes)
        .ok_or(ProjectionError::ByteLimit)?;
    let (edge_segments, edge_bytes, edge_entries) = store
        .scan_edge_segments_as_of_bounded(
            graph,
            window,
            transaction_time,
            limits.max_edges,
            remaining_bytes,
        )
        .await
        .map_err(|error| projection_scan_error(error, false))?;
    let vertices = vertex_segments
        .into_iter()
        .map(|segment| {
            IntervalVertex::new(
                VertexId::new(segment.element().id().value()),
                segment.valid(),
                segment.payload().clone(),
            )
        })
        .collect::<Vec<_>>();
    let edges = edge_segments
        .into_iter()
        .map(|segment| {
            let weight =
                match weight_property.and_then(|property| segment.payload().property(property)) {
                    None => 1.0,
                    Some(GraphValue::Integer(value)) => *value as f64,
                    Some(GraphValue::FloatBits(value)) => f64::from_bits(*value),
                    Some(_) => return Err(ProjectionError::InvalidWeightProperty),
                };
            IntervalEdge::new(
                EdgeId::new(segment.element().id().value()),
                VertexId::new(segment.source_ref().id().value()),
                VertexId::new(segment.destination_ref().id().value()),
                segment.valid(),
                segment.payload().clone(),
                weight,
            )
            .map_err(ProjectionError::Graph)
        })
        .collect::<Result<Vec<_>, ProjectionError>>()?;
    let vertex_usage = vertex_entries.max(vertices.len());
    let edge_usage = edge_entries.max(edges.len());
    Ok((
        IntervalProjectionPart { vertices, edges },
        IntervalProjectionUsage {
            vertices: vertex_usage,
            edges: edge_usage,
            bytes: vertex_bytes
                .checked_add(edge_bytes)
                .ok_or(ProjectionError::ByteLimit)?,
        },
    ))
}

#[derive(Clone, Debug)]
struct DeltaVertexState {
    vertex: VertexId,
    payload: CanonicalElement,
}

#[derive(Clone, Debug)]
struct DeltaEdgeState {
    edge_id: EdgeId,
    source: VertexId,
    destination: VertexId,
    payload: CanonicalElement,
    weight: f64,
}

#[derive(Clone, Debug)]
struct DeltaProjectionState {
    vertices: BTreeMap<ElementRef, DeltaVertexState>,
    edges: BTreeMap<ElementRef, DeltaEdgeState>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DeltaProjectionUsage {
    vertices: usize,
    edges: usize,
    bytes: u64,
}

impl DeltaProjectionUsage {
    #[must_use]
    pub const fn vertices(self) -> usize {
        self.vertices
    }

    #[must_use]
    pub const fn edges(self) -> usize {
        self.edges
    }

    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaProjectionPart {
    vertices: Vec<DeltaVertex>,
    edges: Vec<DeltaEdge>,
}

impl DeltaProjectionPart {
    #[must_use]
    pub fn into_parts(self) -> (Vec<DeltaVertex>, Vec<DeltaEdge>) {
        (self.vertices, self.edges)
    }
}

async fn project_delta_state_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    valid_time: ValidTime,
    transaction_time: TransactionTime,
    weight_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<(DeltaProjectionState, DeltaProjectionUsage), ProjectionError>
where
    A: StorageAdapter,
{
    let (vertex_views, vertex_bytes, vertex_entries) = store
        .scan_vertex_views_as_of_bounded(
            graph,
            valid_time,
            transaction_time,
            limits.max_vertices,
            limits.max_bytes,
        )
        .await
        .map_err(|error| projection_scan_error(error, true))?;
    let remaining_bytes = limits
        .max_bytes
        .checked_sub(vertex_bytes)
        .ok_or(ProjectionError::ByteLimit)?;
    let (edge_views, edge_bytes, edge_entries) = store
        .scan_edges_as_of_bounded(
            graph,
            valid_time,
            transaction_time,
            limits.max_edges,
            remaining_bytes,
        )
        .await
        .map_err(|error| projection_scan_error(error, false))?;
    let vertices = vertex_views
        .into_iter()
        .map(|vertex| {
            (
                vertex.element(),
                DeltaVertexState {
                    vertex: VertexId::new(vertex.element().id().value()),
                    payload: vertex.payload().clone(),
                },
            )
        })
        .collect();
    let edges = edge_views
        .into_iter()
        .map(|edge| {
            let weight =
                match weight_property.and_then(|property| edge.payload().property(property)) {
                    None => 1.0,
                    Some(GraphValue::Integer(value)) => *value as f64,
                    Some(GraphValue::FloatBits(value)) => f64::from_bits(*value),
                    Some(_) => return Err(ProjectionError::InvalidWeightProperty),
                };
            Ok((
                edge.element(),
                DeltaEdgeState {
                    edge_id: EdgeId::new(edge.element().id().value()),
                    source: VertexId::new(edge.source().value()),
                    destination: VertexId::new(edge.destination().value()),
                    payload: edge.payload().clone(),
                    weight,
                },
            ))
        })
        .collect::<Result<BTreeMap<_, _>, ProjectionError>>()?;
    Ok((
        DeltaProjectionState { vertices, edges },
        DeltaProjectionUsage {
            vertices: vertex_entries,
            edges: edge_entries,
            bytes: vertex_bytes
                .checked_add(edge_bytes)
                .ok_or(ProjectionError::ByteLimit)?,
        },
    ))
}

#[allow(clippy::too_many_arguments)]
pub async fn project_delta_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    valid_time: ValidTime,
    from_transaction: TransactionTime,
    to_transaction: TransactionTime,
    _directed: bool,
    weight_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<DeltaGraph, ProjectionError>
where
    A: StorageAdapter,
{
    if from_transaction > to_transaction {
        return Err(ProjectionError::InvalidDeltaOrder);
    }
    let (part, _) = project_delta_between_part_bounded(
        store,
        graph,
        valid_time,
        from_transaction,
        valid_time,
        to_transaction,
        weight_property,
        limits,
    )
    .await?;
    let (vertices, edges) = part.into_parts();
    Ok(DeltaGraph::new(vertices, edges))
}

#[allow(clippy::too_many_arguments)]
pub async fn project_valid_time_delta_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    from_valid_time: ValidTime,
    to_valid_time: ValidTime,
    transaction_time: TransactionTime,
    _directed: bool,
    weight_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<DeltaGraph, ProjectionError>
where
    A: StorageAdapter,
{
    let (part, _) = project_valid_time_delta_part_bounded(
        store,
        graph,
        from_valid_time,
        to_valid_time,
        transaction_time,
        weight_property,
        limits,
    )
    .await?;
    let (vertices, edges) = part.into_parts();
    Ok(DeltaGraph::new(vertices, edges))
}

#[allow(clippy::too_many_arguments)]
pub async fn project_valid_time_delta_part_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    from_valid_time: ValidTime,
    to_valid_time: ValidTime,
    transaction_time: TransactionTime,
    weight_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<(DeltaProjectionPart, DeltaProjectionUsage), ProjectionError>
where
    A: StorageAdapter,
{
    if from_valid_time > to_valid_time {
        return Err(ProjectionError::InvalidDeltaOrder);
    }
    project_delta_between_part_bounded(
        store,
        graph,
        from_valid_time,
        transaction_time,
        to_valid_time,
        transaction_time,
        weight_property,
        limits,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn project_delta_between_part_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    before_valid_time: ValidTime,
    before_transaction_time: TransactionTime,
    after_valid_time: ValidTime,
    after_transaction_time: TransactionTime,
    weight_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<(DeltaProjectionPart, DeltaProjectionUsage), ProjectionError>
where
    A: StorageAdapter,
{
    let (before, before_usage) = project_delta_state_bounded(
        store,
        graph,
        before_valid_time,
        before_transaction_time,
        weight_property,
        limits,
    )
    .await?;
    let remaining_vertices = limits
        .max_vertices
        .checked_sub(before_usage.vertices)
        .ok_or(ProjectionError::VertexLimit)?;
    let remaining_edges = limits
        .max_edges
        .checked_sub(before_usage.edges)
        .ok_or(ProjectionError::EdgeLimit)?;
    let remaining_bytes = limits
        .max_bytes
        .checked_sub(before_usage.bytes)
        .ok_or(ProjectionError::ByteLimit)?;
    if remaining_vertices == 0 {
        return Err(ProjectionError::VertexLimit);
    }
    if remaining_edges == 0 {
        return Err(ProjectionError::EdgeLimit);
    }
    if remaining_bytes == 0 {
        return Err(ProjectionError::ByteLimit);
    }
    let after_limits = ProjectionLimits {
        max_vertices: remaining_vertices,
        max_edges: remaining_edges,
        max_bytes: remaining_bytes,
    };
    let (after, after_usage) = project_delta_state_bounded(
        store,
        graph,
        after_valid_time,
        after_transaction_time,
        weight_property,
        after_limits,
    )
    .await?;
    let vertex_keys = before
        .vertices
        .keys()
        .chain(after.vertices.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut vertices = Vec::new();
    for key in vertex_keys {
        let before = before.vertices.get(&key);
        let after = after.vertices.get(&key);
        let change = match (before, after) {
            (None, Some(_)) => DeltaKind::Added,
            (Some(_), None) => DeltaKind::Removed,
            (Some(before), Some(after)) if before.payload != after.payload => DeltaKind::Updated,
            _ => continue,
        };
        let vertex = before.or(after).expect("union contains a state").vertex;
        vertices.push(DeltaVertex::new(
            vertex,
            change,
            before.map(|state| state.payload.clone()),
            after.map(|state| state.payload.clone()),
        ));
    }
    if vertices.len() > limits.max_vertices {
        return Err(ProjectionError::VertexLimit);
    }
    let keys = before
        .edges
        .keys()
        .chain(after.edges.keys())
        .copied()
        .collect::<BTreeSet<_>>();
    let mut edges = Vec::new();
    for key in keys {
        let before = before.edges.get(&key);
        let after = after.edges.get(&key);
        let change = match (before, after) {
            (None, Some(_)) => DeltaKind::Added,
            (Some(_), None) => DeltaKind::Removed,
            (Some(before), Some(after))
                if before.payload != after.payload
                    || before.weight.to_bits() != after.weight.to_bits() =>
            {
                DeltaKind::Updated
            }
            _ => continue,
        };
        let state = before.or(after).expect("union contains a state");
        edges.push(
            DeltaEdge::new(
                state.edge_id,
                state.source,
                state.destination,
                change,
                before.map(|state| state.payload.clone()),
                after.map(|state| state.payload.clone()),
                before.map(|state| state.weight),
                after.map(|state| state.weight),
            )
            .map_err(ProjectionError::Graph)?,
        );
        if edges.len() > limits.max_edges {
            return Err(ProjectionError::EdgeLimit);
        }
    }
    vertices.shrink_to_fit();
    Ok((
        DeltaProjectionPart { vertices, edges },
        DeltaProjectionUsage {
            vertices: before_usage
                .vertices
                .checked_add(after_usage.vertices)
                .ok_or(ProjectionError::VertexLimit)?,
            edges: before_usage
                .edges
                .checked_add(after_usage.edges)
                .ok_or(ProjectionError::EdgeLimit)?,
            bytes: before_usage
                .bytes
                .checked_add(after_usage.bytes)
                .ok_or(ProjectionError::ByteLimit)?,
        },
    ))
}

pub async fn project_event<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    transaction_time: TransactionTime,
    event_time_property: Option<u32>,
    duration_property: Option<u32>,
    max_events: usize,
) -> Result<EventGraph, ProjectionError>
where
    A: StorageAdapter,
{
    if max_events == 0 {
        return Err(ProjectionError::InvalidEventLimit);
    }
    let history = store
        .scan_edge_history_as_of(graph, transaction_time)
        .await
        .map_err(event_projection_scan_error)?;
    let mut vertices = BTreeSet::new();
    let mut events = Vec::new();
    for (identity, version) in history {
        let Some(payload) = version.replacement() else {
            continue;
        };
        let event_time = event_time_property
            .and_then(|property| payload.property(property))
            .and_then(|value| match value {
                GraphValue::TimestampMicros(value) => Some(ValidTime::from_micros(*value)),
                GraphValue::Integer(value) => Some(ValidTime::from_micros(*value)),
                _ => None,
            })
            .unwrap_or_else(|| ValidTime::from_micros(version.changed_valid().start().as_micros()));
        let duration = duration_property
            .and_then(|property| payload.property(property))
            .and_then(|value| match value {
                GraphValue::Integer(value) => u64::try_from(*value)
                    .ok()
                    .and_then(|value| i64::try_from(value).ok()),
                _ => None,
            })
            .unwrap_or(0);
        vertices.insert(VertexId::new(identity.source().value()));
        vertices.insert(VertexId::new(identity.destination().value()));
        events.push(
            EventEdge::new(
                VertexId::new(identity.source().value()),
                VertexId::new(identity.destination().value()),
                event_time,
                duration,
                1.0,
            )
            .map_err(ProjectionError::Graph)?,
        );
        if events.len() >= max_events {
            break;
        }
    }
    EventGraph::new(vertices.into_iter().collect(), events).map_err(ProjectionError::Graph)
}

pub async fn project_event_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    transaction_time: TransactionTime,
    event_time_property: Option<u32>,
    duration_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<EventGraph, ProjectionError>
where
    A: StorageAdapter,
{
    project_event_part_bounded(
        store,
        graph,
        transaction_time,
        event_time_property,
        duration_property,
        limits,
    )
    .await
    .map(|(graph, _)| graph)
}

pub async fn project_event_part_bounded<A>(
    store: &TemporalStore<A>,
    graph: GraphId,
    transaction_time: TransactionTime,
    event_time_property: Option<u32>,
    duration_property: Option<u32>,
    limits: ProjectionLimits,
) -> Result<(EventGraph, u64), ProjectionError>
where
    A: StorageAdapter,
{
    let (history, scanned_bytes) = store
        .scan_edge_history_as_of_bounded(
            graph,
            transaction_time,
            limits.max_edges,
            limits.max_bytes,
        )
        .await
        .map_err(event_projection_scan_error)?;
    let mut vertices = BTreeSet::new();
    let mut events = Vec::new();
    for (identity, version) in history {
        let Some(payload) = version.replacement() else {
            continue;
        };
        let event_time = event_time_property
            .and_then(|property| payload.property(property))
            .and_then(|value| match value {
                GraphValue::TimestampMicros(value) | GraphValue::Integer(value) => {
                    Some(ValidTime::from_micros(*value))
                }
                _ => None,
            })
            .unwrap_or_else(|| ValidTime::from_micros(version.changed_valid().start().as_micros()));
        let duration = duration_property
            .and_then(|property| payload.property(property))
            .and_then(|value| match value {
                GraphValue::Integer(value) => u64::try_from(*value)
                    .ok()
                    .and_then(|value| i64::try_from(value).ok()),
                _ => None,
            })
            .unwrap_or(0);
        for vertex in [identity.source(), identity.destination()] {
            vertices.insert(VertexId::new(vertex.value()));
            if vertices.len() > limits.max_vertices {
                return Err(ProjectionError::VertexLimit);
            }
        }
        events.push(
            EventEdge::new(
                VertexId::new(identity.source().value()),
                VertexId::new(identity.destination().value()),
                event_time,
                duration,
                1.0,
            )
            .map_err(ProjectionError::Graph)?,
        );
    }
    Ok((
        EventGraph::new(vertices.into_iter().collect(), events).map_err(ProjectionError::Graph)?,
        scanned_bytes,
    ))
}

fn event_projection_scan_error(error: temporal_storage::TemporalStoreError) -> ProjectionError {
    match error {
        temporal_storage::TemporalStoreError::ScanEntryLimit => ProjectionError::EventLimit,
        temporal_storage::TemporalStoreError::ScanByteLimit => ProjectionError::ByteLimit,
        temporal_storage::TemporalStoreError::ScanResponseByteLimit { limit, required } => {
            ProjectionError::ResponseByteLimit { limit, required }
        }
        temporal_storage::TemporalStoreError::Adapter(AdapterError::Unavailable(message)) => {
            ProjectionError::StorageUnavailable(message)
        }
        error => ProjectionError::Storage(error.to_string()),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionError {
    Storage(String),
    StorageUnavailable(String),
    Graph(GraphProjectionError),
    InvalidWeightProperty,
    InvalidEventLimit,
    InvalidLimits,
    InvalidDeltaOrder,
    VertexLimit,
    EdgeLimit,
    EventLimit,
    ByteLimit,
    ResponseByteLimit { limit: u64, required: u64 },
}

impl Display for ProjectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResponseByteLimit { limit, required } => write!(
                formatter,
                "analytics scan response requires {required} wire bytes above limit {limit}"
            ),
            _ => write!(formatter, "analytics storage projection failed: {self:?}"),
        }
    }
}

impl Error for ProjectionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BfsResult {
    distances: BTreeMap<VertexId, u64>,
    predecessors: BTreeMap<VertexId, VertexId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DfsResult {
    depths: BTreeMap<VertexId, u64>,
    predecessors: BTreeMap<VertexId, VertexId>,
}

impl DfsResult {
    #[must_use]
    pub fn depth(&self, vertex: VertexId) -> Option<u64> {
        self.depths.get(&vertex).copied()
    }

    #[must_use]
    pub fn predecessor(&self, vertex: VertexId) -> Option<VertexId> {
        self.predecessors.get(&vertex).copied()
    }
}

impl BfsResult {
    #[must_use]
    pub fn distance(&self, vertex: VertexId) -> Option<u64> {
        self.distances.get(&vertex).copied()
    }

    #[must_use]
    pub fn predecessor(&self, vertex: VertexId) -> Option<VertexId> {
        self.predecessors.get(&vertex).copied()
    }
}

pub fn bfs(graph: &SnapshotGraph, source: VertexId) -> Result<BfsResult, AlgorithmError> {
    bfs_cancellable(graph, source, || false)
}

pub fn bfs_cancellable<F>(
    graph: &SnapshotGraph,
    source: VertexId,
    canceled: F,
) -> Result<BfsResult, AlgorithmError>
where
    F: Fn() -> bool,
{
    ensure_vertex(graph.vertices(), source)?;
    let mut distances = BTreeMap::from([(source, 0_u64)]);
    let mut predecessors = BTreeMap::new();
    let mut frontier = VecDeque::from([source]);
    while let Some(vertex) = frontier.pop_front() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let next_distance = distances[&vertex]
            .checked_add(1)
            .ok_or(AlgorithmError::DistanceOverflow)?;
        for edge in graph.outgoing(vertex) {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            if let Entry::Vacant(entry) = distances.entry(edge.destination()) {
                entry.insert(next_distance);
                predecessors.insert(edge.destination(), vertex);
                frontier.push_back(edge.destination());
            }
        }
    }
    Ok(BfsResult {
        distances,
        predecessors,
    })
}

pub fn dfs(graph: &SnapshotGraph, source: VertexId) -> Result<DfsResult, AlgorithmError> {
    dfs_cancellable(graph, source, || false)
}

pub fn dfs_cancellable<F>(
    graph: &SnapshotGraph,
    source: VertexId,
    canceled: F,
) -> Result<DfsResult, AlgorithmError>
where
    F: Fn() -> bool,
{
    ensure_vertex(graph.vertices(), source)?;
    let mut depths = BTreeMap::from([(source, 0_u64)]);
    let mut predecessors = BTreeMap::new();
    let mut stack = vec![(source, 0_usize)];
    while let Some((vertex, edge_index)) = stack.last_mut() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let outgoing = graph.outgoing(*vertex);
        if *edge_index >= outgoing.len() {
            stack.pop();
            continue;
        }
        let edge = &outgoing[*edge_index];
        *edge_index = edge_index.saturating_add(1);
        let destination = edge.destination();
        if depths.contains_key(&destination) {
            continue;
        }
        let depth = depths[vertex]
            .checked_add(1)
            .ok_or(AlgorithmError::DistanceOverflow)?;
        depths.insert(destination, depth);
        predecessors.insert(destination, *vertex);
        stack.push((destination, 0));
    }
    Ok(DfsResult {
        depths,
        predecessors,
    })
}

#[derive(Clone, Debug, PartialEq)]
pub struct SsspResult {
    distances: BTreeMap<VertexId, f64>,
    predecessors: BTreeMap<VertexId, VertexId>,
}

impl SsspResult {
    #[must_use]
    pub fn distance(&self, vertex: VertexId) -> Option<f64> {
        self.distances.get(&vertex).copied()
    }

    #[must_use]
    pub fn predecessor(&self, vertex: VertexId) -> Option<VertexId> {
        self.predecessors.get(&vertex).copied()
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct WeightedFrontier {
    distance: f64,
    vertex: VertexId,
}

impl Eq for WeightedFrontier {}

impl Ord for WeightedFrontier {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .distance
            .total_cmp(&self.distance)
            .then_with(|| other.vertex.cmp(&self.vertex))
    }
}

impl PartialOrd for WeightedFrontier {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub fn sssp(graph: &SnapshotGraph, source: VertexId) -> Result<SsspResult, AlgorithmError> {
    sssp_cancellable(graph, source, || false)
}

pub fn sssp_cancellable<F>(
    graph: &SnapshotGraph,
    source: VertexId,
    canceled: F,
) -> Result<SsspResult, AlgorithmError>
where
    F: Fn() -> bool,
{
    ensure_vertex(graph.vertices(), source)?;
    for edge in graph.edges() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if edge.weight() < 0.0 {
            return Err(AlgorithmError::NegativeWeight);
        }
    }
    let mut distances = BTreeMap::from([(source, 0.0)]);
    let mut predecessors = BTreeMap::new();
    let mut frontier = BinaryHeap::from([WeightedFrontier {
        distance: 0.0,
        vertex: source,
    }]);
    while let Some(current) = frontier.pop() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if distances
            .get(&current.vertex)
            .is_none_or(|distance| *distance != current.distance)
        {
            continue;
        }
        for edge in graph.outgoing(current.vertex) {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            let next = current.distance + edge.weight();
            if !next.is_finite() {
                return Err(AlgorithmError::DistanceOverflow);
            }
            let destination = edge.destination();
            if distances
                .get(&destination)
                .is_none_or(|distance| next < *distance)
            {
                distances.insert(destination, next);
                predecessors.insert(destination, current.vertex);
                frontier.push(WeightedFrontier {
                    distance: next,
                    vertex: destination,
                });
            }
        }
    }
    Ok(SsspResult {
        distances,
        predecessors,
    })
}

pub const MAX_ALL_PAIRS_WORK: usize = 10_000_000;
pub const MAX_CENTRALITY_WORK: usize = 10_000_000;

pub fn all_pairs_shortest_paths(
    graph: &SnapshotGraph,
) -> Result<BTreeMap<(VertexId, VertexId), f64>, AlgorithmError> {
    all_pairs_shortest_paths_cancellable(graph, MAX_ALL_PAIRS_WORK, || false)
}

pub fn all_pairs_shortest_paths_cancellable<F>(
    graph: &SnapshotGraph,
    max_work: usize,
    canceled: F,
) -> Result<BTreeMap<(VertexId, VertexId), f64>, AlgorithmError>
where
    F: Fn() -> bool,
{
    ensure_repeated_shortest_path_budget(graph, max_work, AlgorithmError::AllPairsCapacity)?;
    let mut distances = BTreeMap::new();
    for source in graph.vertices() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let shortest = sssp_cancellable(graph, *source, &canceled)?;
        for target in graph.vertices() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            if let Some(distance) = shortest.distance(*target) {
                distances.insert((*source, *target), distance);
            }
        }
    }
    Ok(distances)
}

pub fn closeness_centrality(
    graph: &SnapshotGraph,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError> {
    closeness_centrality_cancellable(graph, MAX_CENTRALITY_WORK, || false)
}

pub fn closeness_centrality_cancellable<F>(
    graph: &SnapshotGraph,
    max_work: usize,
    canceled: F,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError>
where
    F: Fn() -> bool,
{
    ensure_repeated_shortest_path_budget(graph, max_work, AlgorithmError::CentralityCapacity)?;
    let total_vertices = graph.vertices().len();
    let mut scores = BTreeMap::new();
    for source in graph.vertices() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let shortest = sssp_cancellable(graph, *source, &canceled)?;
        let mut reachable = 0_usize;
        let mut distance_sum = 0.0;
        for target in graph.vertices() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            if target == source {
                continue;
            }
            if let Some(distance) = shortest.distance(*target) {
                reachable = reachable.saturating_add(1);
                distance_sum += distance;
            }
        }
        let score = if reachable == 0 || distance_sum == 0.0 || total_vertices <= 1 {
            0.0
        } else {
            let reachable = reachable as f64;
            reachable / distance_sum * (reachable / (total_vertices - 1) as f64)
        };
        scores.insert(*source, score);
    }
    Ok(scores)
}

pub fn betweenness_centrality(
    graph: &SnapshotGraph,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError> {
    betweenness_centrality_cancellable(graph, || false)
}

pub fn betweenness_centrality_cancellable<F>(
    graph: &SnapshotGraph,
    canceled: F,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError>
where
    F: Fn() -> bool,
{
    ensure_repeated_shortest_path_budget(
        graph,
        MAX_CENTRALITY_WORK,
        AlgorithmError::CentralityCapacity,
    )?;
    for edge in graph.edges() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if edge.weight() <= 0.0 {
            return Err(AlgorithmError::InvalidBetweennessWeight);
        }
    }
    let vertices = graph.vertices();
    let mut scores = vertices
        .iter()
        .copied()
        .map(|vertex| (vertex, 0.0))
        .collect::<BTreeMap<_, _>>();
    for source in vertices {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let mut stack = Vec::with_capacity(vertices.len());
        let mut predecessors = vertices
            .iter()
            .copied()
            .map(|vertex| (vertex, Vec::new()))
            .collect::<BTreeMap<_, Vec<VertexId>>>();
        let mut paths: BTreeMap<VertexId, f64> = vertices
            .iter()
            .copied()
            .map(|vertex| (vertex, 0.0))
            .collect::<BTreeMap<_, _>>();
        let mut distance = BTreeMap::from([(*source, 0.0)]);
        paths.insert(*source, 1.0);
        let mut frontier = BinaryHeap::from([WeightedFrontier {
            distance: 0.0,
            vertex: *source,
        }]);
        while let Some(current) = frontier.pop() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            if distance
                .get(&current.vertex)
                .is_none_or(|known| current.distance.total_cmp(known) != Ordering::Equal)
            {
                continue;
            }
            stack.push(current.vertex);
            for edge in graph.outgoing(current.vertex) {
                if canceled() {
                    return Err(AlgorithmError::Canceled);
                }
                let destination = edge.destination();
                let next_distance = current.distance + edge.weight();
                if !next_distance.is_finite() {
                    return Err(AlgorithmError::DistanceOverflow);
                }
                match distance
                    .get(&destination)
                    .map_or(Ordering::Less, |known| next_distance.total_cmp(known))
                {
                    Ordering::Less => {
                        distance.insert(destination, next_distance);
                        paths.insert(destination, paths[&current.vertex]);
                        let destination_predecessors = predecessors
                            .get_mut(&destination)
                            .expect("validated vertex");
                        destination_predecessors.clear();
                        destination_predecessors.push(current.vertex);
                        frontier.push(WeightedFrontier {
                            distance: next_distance,
                            vertex: destination,
                        });
                    }
                    Ordering::Equal => {
                        let path_count = paths[&destination] + paths[&current.vertex];
                        if !path_count.is_finite() {
                            return Err(AlgorithmError::DistanceOverflow);
                        }
                        paths.insert(destination, path_count);
                        predecessors
                            .get_mut(&destination)
                            .expect("validated vertex")
                            .push(current.vertex);
                    }
                    Ordering::Greater => {}
                }
            }
        }
        let mut dependency = vertices
            .iter()
            .copied()
            .map(|vertex| (vertex, 0.0))
            .collect::<BTreeMap<_, _>>();
        while let Some(vertex) = stack.pop() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            if paths[&vertex] > 0.0 {
                let factor = (1.0 + dependency[&vertex]) / paths[&vertex];
                for predecessor in &predecessors[&vertex] {
                    if canceled() {
                        return Err(AlgorithmError::Canceled);
                    }
                    let contribution = paths[predecessor] * factor;
                    *dependency
                        .get_mut(predecessor)
                        .expect("validated predecessor") += contribution;
                }
            }
            if vertex != *source {
                *scores.get_mut(&vertex).expect("validated vertex") += dependency[&vertex];
            }
        }
    }
    if !graph.directed() {
        for score in scores.values_mut() {
            *score /= 2.0;
        }
    }
    Ok(scores)
}

fn ensure_repeated_shortest_path_budget(
    graph: &SnapshotGraph,
    max_work: usize,
    error: AlgorithmError,
) -> Result<(), AlgorithmError> {
    let traversal_edges = graph.vertices().iter().try_fold(0_usize, |total, vertex| {
        total
            .checked_add(graph.outgoing(*vertex).len())
            .ok_or_else(|| error.clone())
    })?;
    let per_source = graph
        .vertices()
        .len()
        .checked_add(traversal_edges)
        .ok_or_else(|| error.clone())?;
    let work = graph
        .vertices()
        .len()
        .checked_mul(per_source)
        .ok_or_else(|| error.clone())?;
    if work > max_work { Err(error) } else { Ok(()) }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DegreeCentrality {
    incoming: u64,
    outgoing: u64,
}

impl DegreeCentrality {
    #[must_use]
    pub const fn incoming(self) -> u64 {
        self.incoming
    }

    #[must_use]
    pub const fn outgoing(self) -> u64 {
        self.outgoing
    }

    #[must_use]
    pub const fn total(self) -> u64 {
        self.incoming.saturating_add(self.outgoing)
    }
}

#[must_use]
pub fn degree_centrality(graph: &SnapshotGraph) -> BTreeMap<VertexId, DegreeCentrality> {
    let mut result = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| {
            (
                vertex,
                DegreeCentrality {
                    incoming: 0,
                    outgoing: 0,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    for edge in graph.edges() {
        let source = result.get_mut(&edge.source()).expect("validated source");
        source.outgoing = source.outgoing.saturating_add(1);
        let destination = result
            .get_mut(&edge.destination())
            .expect("validated destination");
        destination.incoming = destination.incoming.saturating_add(1);
        if !graph.directed() && edge.source() != edge.destination() {
            let destination = result
                .get_mut(&edge.destination())
                .expect("validated destination");
            destination.outgoing = destination.outgoing.saturating_add(1);
            let source = result.get_mut(&edge.source()).expect("validated source");
            source.incoming = source.incoming.saturating_add(1);
        }
    }
    result
}

#[must_use]
pub fn partitioned_degree_centrality(
    graph: &PartitionedSnapshotGraph,
) -> BTreeMap<VertexId, DegreeCentrality> {
    let mut result = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| {
            (
                vertex,
                DegreeCentrality {
                    incoming: 0,
                    outgoing: 0,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let partials = graph
        .partitions()
        .iter()
        .map(|partition| {
            let mut local = BTreeMap::<VertexId, DegreeCentrality>::new();
            for edge in partition.edges() {
                let source = local.entry(edge.source()).or_insert(DegreeCentrality {
                    incoming: 0,
                    outgoing: 0,
                });
                source.outgoing = source.outgoing.saturating_add(1);
                let destination = local.entry(edge.destination()).or_insert(DegreeCentrality {
                    incoming: 0,
                    outgoing: 0,
                });
                destination.incoming = destination.incoming.saturating_add(1);
                if !graph.directed() && edge.source() != edge.destination() {
                    let destination = local.entry(edge.destination()).or_insert(DegreeCentrality {
                        incoming: 0,
                        outgoing: 0,
                    });
                    destination.outgoing = destination.outgoing.saturating_add(1);
                    let source = local.entry(edge.source()).or_insert(DegreeCentrality {
                        incoming: 0,
                        outgoing: 0,
                    });
                    source.incoming = source.incoming.saturating_add(1);
                }
            }
            (partition.shard_id(), local)
        })
        .collect::<Vec<_>>();
    for (_, local) in partials {
        for (vertex, partial) in local {
            let total = result.get_mut(&vertex).expect("validated vertex owner");
            total.incoming = total.incoming.saturating_add(partial.incoming);
            total.outgoing = total.outgoing.saturating_add(partial.outgoing);
        }
    }
    result
}

#[must_use]
pub fn triangle_count(graph: &SnapshotGraph) -> usize {
    let neighbors = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| {
            (
                vertex,
                graph
                    .outgoing(vertex)
                    .iter()
                    .map(|edge| edge.destination())
                    .collect::<std::collections::BTreeSet<_>>(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let mut count = 0_usize;
    for (index, left) in graph.vertices().iter().enumerate() {
        for middle in graph.vertices().iter().skip(index + 1) {
            if !neighbors[left].contains(middle) {
                continue;
            }
            for right in graph
                .vertices()
                .iter()
                .skip_while(|vertex| **vertex <= *middle)
            {
                if neighbors[left].contains(right) && neighbors[middle].contains(right) {
                    count = count.saturating_add(1);
                }
            }
        }
    }
    count
}

#[must_use]
pub fn clustering_coefficient(graph: &SnapshotGraph) -> BTreeMap<VertexId, f64> {
    graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| {
            let neighbors = graph
                .outgoing(vertex)
                .iter()
                .map(|edge| edge.destination())
                .collect::<std::collections::BTreeSet<_>>();
            let degree = neighbors.len();
            let links = neighbors
                .iter()
                .enumerate()
                .flat_map(|(index, left)| {
                    neighbors
                        .iter()
                        .skip(index + 1)
                        .map(move |right| (*left, *right))
                })
                .filter(|(left, right)| {
                    graph
                        .outgoing(*left)
                        .iter()
                        .any(|edge| edge.destination() == *right)
                        || (!graph.directed()
                            && graph
                                .outgoing(*right)
                                .iter()
                                .any(|edge| edge.destination() == *left))
                })
                .count();
            let coefficient = if degree < 2 {
                0.0
            } else {
                2.0 * links as f64 / (degree * (degree - 1)) as f64
            };
            (vertex, coefficient)
        })
        .collect()
}

#[must_use]
pub fn k_core(graph: &SnapshotGraph, k: usize) -> BTreeMap<VertexId, usize> {
    let mut degrees = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, graph.outgoing(vertex).len()))
        .collect::<BTreeMap<_, _>>();
    let mut removed = std::collections::BTreeSet::new();
    let mut queue = VecDeque::new();
    for (vertex, degree) in &degrees {
        if *degree < k {
            queue.push_back(*vertex);
        }
    }
    while let Some(vertex) = queue.pop_front() {
        if !removed.insert(vertex) {
            continue;
        }
        for edge in graph.outgoing(vertex) {
            if let Some(degree) = degrees.get_mut(&edge.destination()) {
                *degree = degree.saturating_sub(1);
                if *degree < k {
                    queue.push_back(edge.destination());
                }
            }
        }
    }
    graph
        .vertices()
        .iter()
        .copied()
        .filter(|vertex| !removed.contains(vertex))
        .map(|vertex| (vertex, degrees[&vertex]))
        .collect()
}

pub fn label_propagation(
    graph: &SnapshotGraph,
    max_iterations: usize,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError> {
    label_propagation_cancellable(graph, max_iterations, || false)
}

pub fn label_propagation_cancellable<F>(
    graph: &SnapshotGraph,
    max_iterations: usize,
    canceled: F,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError>
where
    F: Fn() -> bool,
{
    if max_iterations == 0 {
        return Err(AlgorithmError::InvalidLabelPropagationConfiguration);
    }
    let mut labels = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, vertex))
        .collect::<BTreeMap<_, _>>();
    for _ in 0..max_iterations {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let mut changed = false;
        let previous = labels.clone();
        for vertex in graph.vertices() {
            let mut counts = BTreeMap::<VertexId, usize>::new();
            for edge in graph.outgoing(*vertex) {
                *counts.entry(previous[&edge.destination()]).or_default() += 1;
            }
            if !graph.directed() {
                for candidate in graph.vertices() {
                    if graph
                        .outgoing(*candidate)
                        .iter()
                        .any(|edge| edge.destination() == *vertex)
                    {
                        *counts.entry(previous[candidate]).or_default() += 1;
                    }
                }
            }
            #[allow(clippy::collapsible_if)]
            if let Some(label) = counts
                .into_iter()
                .max_by_key(|(label, count)| (*count, std::cmp::Reverse(*label)))
            {
                if labels.insert(*vertex, label.0) != Some(label.0) {
                    changed = true;
                }
            }
        }
        if !changed {
            break;
        }
    }
    Ok(labels)
}

#[derive(Clone, Debug)]
struct LouvainLevel {
    adjacency: Vec<BTreeMap<usize, f64>>,
    members: Vec<Vec<VertexId>>,
}

pub fn louvain_communities(
    graph: &SnapshotGraph,
    max_levels: usize,
    max_iterations: usize,
    resolution: f64,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError> {
    louvain_communities_cancellable(graph, max_levels, max_iterations, resolution, || false)
}

pub fn louvain_communities_cancellable<F>(
    graph: &SnapshotGraph,
    max_levels: usize,
    max_iterations: usize,
    resolution: f64,
    canceled: F,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError>
where
    F: Fn() -> bool,
{
    if max_levels == 0 || max_iterations == 0 || !resolution.is_finite() || resolution <= 0.0 {
        return Err(AlgorithmError::InvalidLouvainConfiguration);
    }
    let mut level = louvain_level_from_snapshot(graph, &canceled)?;
    let mut assignments = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, vertex))
        .collect::<BTreeMap<_, _>>();
    for _ in 0..max_levels {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let partition = louvain_local_move(&level, max_iterations, resolution, &canceled)?;
        let groups = louvain_groups(&level, &partition);
        for nodes in &groups {
            let community = nodes
                .iter()
                .flat_map(|node| level.members[*node].iter().copied())
                .min()
                .expect("a Louvain community is non-empty");
            for vertex in nodes
                .iter()
                .flat_map(|node| level.members[*node].iter().copied())
            {
                assignments.insert(vertex, community);
            }
        }
        if groups.len() == level.members.len() || groups.len() <= 1 {
            break;
        }
        level = aggregate_louvain_level(level, groups, &canceled)?;
    }
    Ok(assignments)
}

fn louvain_level_from_snapshot<F>(
    graph: &SnapshotGraph,
    canceled: &F,
) -> Result<LouvainLevel, AlgorithmError>
where
    F: Fn() -> bool,
{
    let indexes = graph
        .vertices()
        .iter()
        .enumerate()
        .map(|(index, vertex)| (*vertex, index))
        .collect::<BTreeMap<_, _>>();
    let mut adjacency = vec![BTreeMap::new(); graph.vertices().len()];
    for edge in graph.edges() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let weight = edge.weight();
        if weight < 0.0 {
            return Err(AlgorithmError::NegativeWeight);
        }
        let source = indexes[&edge.source()];
        let destination = indexes[&edge.destination()];
        if source == destination {
            *adjacency[source].entry(source).or_default() += 2.0 * weight;
        } else {
            *adjacency[source].entry(destination).or_default() += weight;
            *adjacency[destination].entry(source).or_default() += weight;
        }
    }
    Ok(LouvainLevel {
        adjacency,
        members: graph
            .vertices()
            .iter()
            .copied()
            .map(|vertex| vec![vertex])
            .collect(),
    })
}

fn louvain_local_move<F>(
    level: &LouvainLevel,
    max_iterations: usize,
    resolution: f64,
    canceled: &F,
) -> Result<Vec<usize>, AlgorithmError>
where
    F: Fn() -> bool,
{
    let count = level.adjacency.len();
    let degrees = level
        .adjacency
        .iter()
        .map(|neighbors| neighbors.values().sum::<f64>())
        .collect::<Vec<_>>();
    let total_weight = degrees.iter().sum::<f64>();
    let mut communities = (0..count).collect::<Vec<_>>();
    if total_weight == 0.0 {
        return Ok(communities);
    }
    let mut community_totals = degrees.clone();
    const GAIN_EPSILON: f64 = 1e-12;
    for _ in 0..max_iterations {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let mut moved = false;
        for node in 0..count {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            let current = communities[node];
            let degree = degrees[node];
            community_totals[current] = (community_totals[current] - degree).max(0.0);
            let mut weights_by_community = BTreeMap::<usize, f64>::new();
            for (neighbor, weight) in &level.adjacency[node] {
                if canceled() {
                    return Err(AlgorithmError::Canceled);
                }
                if *neighbor == node {
                    continue;
                }
                *weights_by_community
                    .entry(communities[*neighbor])
                    .or_default() += *weight;
            }
            weights_by_community.entry(current).or_default();
            let mut best = current;
            let mut best_gain = f64::NEG_INFINITY;
            for (community, internal_weight) in weights_by_community {
                let gain = internal_weight
                    - resolution * degree * community_totals[community] / total_weight;
                if gain > best_gain + GAIN_EPSILON
                    || ((gain - best_gain).abs() <= GAIN_EPSILON && community < best)
                {
                    best = community;
                    best_gain = gain;
                }
            }
            communities[node] = best;
            community_totals[best] += degree;
            moved |= best != current;
        }
        if !moved {
            break;
        }
    }
    Ok(communities)
}

fn louvain_groups(level: &LouvainLevel, partition: &[usize]) -> Vec<Vec<usize>> {
    let mut groups = BTreeMap::<usize, Vec<usize>>::new();
    for (node, community) in partition.iter().copied().enumerate() {
        groups.entry(community).or_default().push(node);
    }
    let mut groups = groups.into_values().collect::<Vec<_>>();
    groups.sort_by_key(|nodes| {
        nodes
            .iter()
            .flat_map(|node| level.members[*node].iter().copied())
            .min()
            .expect("a Louvain community is non-empty")
    });
    groups
}

fn aggregate_louvain_level<F>(
    level: LouvainLevel,
    groups: Vec<Vec<usize>>,
    canceled: &F,
) -> Result<LouvainLevel, AlgorithmError>
where
    F: Fn() -> bool,
{
    let mut group_by_node = vec![0_usize; level.members.len()];
    for (group, nodes) in groups.iter().enumerate() {
        for node in nodes {
            group_by_node[*node] = group;
        }
    }
    let mut members = Vec::with_capacity(groups.len());
    for nodes in &groups {
        let mut group_members = nodes
            .iter()
            .flat_map(|node| level.members[*node].iter().copied())
            .collect::<Vec<_>>();
        group_members.sort_unstable();
        members.push(group_members);
    }
    let mut adjacency = vec![BTreeMap::new(); groups.len()];
    for (source, neighbors) in level.adjacency.into_iter().enumerate() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let source_group = group_by_node[source];
        for (destination, weight) in neighbors {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            let destination_group = group_by_node[destination];
            *adjacency[source_group]
                .entry(destination_group)
                .or_default() += weight;
        }
    }
    Ok(LouvainLevel { adjacency, members })
}

#[must_use]
pub fn scc(graph: &SnapshotGraph) -> BTreeMap<VertexId, VertexId> {
    scc_cancellable(graph, || false).unwrap_or_default()
}

pub fn scc_cancellable<F>(
    graph: &SnapshotGraph,
    canceled: F,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError>
where
    F: Fn() -> bool,
{
    let mut seen = BTreeMap::<VertexId, ()>::new();
    let mut order = Vec::with_capacity(graph.vertices().len());
    for start in graph.vertices() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if seen.contains_key(start) {
            continue;
        }
        let mut stack = vec![(*start, false)];
        while let Some((vertex, expanded)) = stack.pop() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            if expanded {
                order.push(vertex);
                continue;
            }
            if seen.insert(vertex, ()).is_some() {
                continue;
            }
            stack.push((vertex, true));
            for edge in graph.outgoing(vertex).iter().rev() {
                if canceled() {
                    return Err(AlgorithmError::Canceled);
                }
                if !seen.contains_key(&edge.destination()) {
                    stack.push((edge.destination(), false));
                }
            }
        }
    }
    let mut incoming = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    for edge in graph.edges() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        incoming
            .get_mut(&edge.destination())
            .expect("validated destination")
            .push(edge.source());
        if !graph.directed() && edge.source() != edge.destination() {
            incoming
                .get_mut(&edge.source())
                .expect("validated source")
                .push(edge.destination());
        }
    }
    let mut assigned = BTreeMap::new();
    while let Some(start) = order.pop() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if assigned.contains_key(&start) {
            continue;
        }
        let mut members = Vec::new();
        let mut stack = vec![start];
        assigned.insert(start, start);
        while let Some(vertex) = stack.pop() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            members.push(vertex);
            for predecessor in &incoming[&vertex] {
                if canceled() {
                    return Err(AlgorithmError::Canceled);
                }
                if !assigned.contains_key(predecessor) {
                    assigned.insert(*predecessor, start);
                    stack.push(*predecessor);
                }
            }
        }
        let component = members
            .iter()
            .copied()
            .min()
            .expect("component is non-empty");
        for member in members {
            assigned.insert(member, component);
        }
    }
    Ok(assigned)
}

#[must_use]
pub fn wcc(graph: &SnapshotGraph) -> BTreeMap<VertexId, VertexId> {
    wcc_cancellable(graph, || false).unwrap_or_default()
}

pub fn wcc_cancellable<F>(
    graph: &SnapshotGraph,
    canceled: F,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError>
where
    F: Fn() -> bool,
{
    let mut components = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, vertex))
        .collect::<BTreeMap<_, _>>();
    loop {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let mut changed = false;
        for edge in graph.edges() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            let component = components[&edge.source()].min(components[&edge.destination()]);
            if components[&edge.source()] != component {
                components.insert(edge.source(), component);
                changed = true;
            }
            if components[&edge.destination()] != component {
                components.insert(edge.destination(), component);
                changed = true;
            }
        }
        for vertex in graph.vertices() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            let parent = components[vertex];
            let root = components[&parent];
            if parent != root {
                components.insert(*vertex, root);
                changed = true;
            }
        }
        if !changed {
            return Ok(components);
        }
    }
}

#[must_use]
pub fn partitioned_wcc(graph: &PartitionedSnapshotGraph) -> BTreeMap<VertexId, VertexId> {
    partitioned_wcc_cancellable(graph, || false).unwrap_or_default()
}

pub fn partitioned_wcc_cancellable<F>(
    graph: &PartitionedSnapshotGraph,
    canceled: F,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError>
where
    F: Fn() -> bool,
{
    let mut labels = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, vertex))
        .collect::<BTreeMap<_, _>>();
    loop {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let previous = labels.clone();
        let local_updates = graph
            .partitions()
            .iter()
            .map(|partition| {
                let mut updates = BTreeMap::<VertexId, VertexId>::new();
                for edge in partition.edges() {
                    let label = previous[&edge.source()].min(previous[&edge.destination()]);
                    updates
                        .entry(edge.source())
                        .and_modify(|current| *current = (*current).min(label))
                        .or_insert(label);
                    updates
                        .entry(edge.destination())
                        .and_modify(|current| *current = (*current).min(label))
                        .or_insert(label);
                }
                (partition.shard_id(), updates)
            })
            .collect::<Vec<_>>();
        for (_, updates) in local_updates {
            for (vertex, label) in updates {
                labels
                    .entry(vertex)
                    .and_modify(|current| *current = (*current).min(label));
            }
        }
        if labels == previous {
            return Ok(labels);
        }
    }
}

pub fn page_rank(
    graph: &SnapshotGraph,
    damping: f64,
    max_iterations: usize,
    tolerance: f64,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError> {
    page_rank_cancellable(graph, damping, max_iterations, tolerance, || false)
}

pub fn page_rank_cancellable<F>(
    graph: &SnapshotGraph,
    damping: f64,
    max_iterations: usize,
    tolerance: f64,
    canceled: F,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError>
where
    F: Fn() -> bool,
{
    if !(0.0..1.0).contains(&damping)
        || max_iterations == 0
        || !tolerance.is_finite()
        || tolerance <= 0.0
    {
        return Err(AlgorithmError::InvalidPageRankConfiguration);
    }
    let count = graph.vertices().len();
    if count == 0 {
        return Ok(BTreeMap::new());
    }
    let count_f64 = count as f64;
    let mut ranks = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, 1.0 / count_f64))
        .collect::<BTreeMap<_, _>>();
    for _ in 0..max_iterations {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let mut dangling = 0.0;
        for vertex in graph.vertices() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            if graph.outgoing(*vertex).is_empty() {
                dangling += ranks[vertex];
            }
        }
        let base = (1.0 - damping) / count_f64 + damping * dangling / count_f64;
        let mut next = BTreeMap::new();
        for vertex in graph.vertices() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            next.insert(*vertex, base);
        }
        for source in graph.vertices() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            let outgoing = graph.outgoing(*source);
            if outgoing.is_empty() {
                continue;
            }
            let contribution = damping * ranks[source] / outgoing.len() as f64;
            for edge in outgoing {
                if canceled() {
                    return Err(AlgorithmError::Canceled);
                }
                *next
                    .get_mut(&edge.destination())
                    .expect("validated endpoint") += contribution;
            }
        }
        let mut delta = 0.0;
        for vertex in graph.vertices() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            delta += (next[vertex] - ranks[vertex]).abs();
        }
        ranks = next;
        if delta <= tolerance {
            break;
        }
    }
    Ok(ranks)
}

pub fn partitioned_page_rank(
    graph: &PartitionedSnapshotGraph,
    damping: f64,
    max_iterations: usize,
    tolerance: f64,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError> {
    partitioned_page_rank_cancellable(graph, damping, max_iterations, tolerance, || false)
}

pub fn partitioned_page_rank_cancellable<F>(
    graph: &PartitionedSnapshotGraph,
    damping: f64,
    max_iterations: usize,
    tolerance: f64,
    canceled: F,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError>
where
    F: Fn() -> bool,
{
    if !(0.0..1.0).contains(&damping)
        || max_iterations == 0
        || !tolerance.is_finite()
        || tolerance <= 0.0
    {
        return Err(AlgorithmError::InvalidPageRankConfiguration);
    }
    let count = graph.vertices().len();
    if count == 0 {
        return Ok(BTreeMap::new());
    }

    let mut local_messages = graph
        .partitions()
        .iter()
        .map(|partition| {
            let mut oriented = Vec::new();
            for edge in partition.edges() {
                oriented.push((edge.source(), edge.destination(), edge.weight().to_bits()));
                if !graph.directed() && edge.source() != edge.destination() {
                    oriented.push((edge.destination(), edge.source(), edge.weight().to_bits()));
                }
            }
            oriented.sort_unstable();
            (partition.shard_id(), oriented)
        })
        .collect::<Vec<_>>();
    local_messages.sort_by_key(|(shard_id, _)| *shard_id);

    let mut outgoing = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, 0_usize))
        .collect::<BTreeMap<_, _>>();
    for (_, messages) in &local_messages {
        for (source, _, _) in messages {
            *outgoing.get_mut(source).expect("validated vertex owner") += 1;
        }
    }

    let count_f64 = count as f64;
    let mut ranks = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, 1.0 / count_f64))
        .collect::<BTreeMap<_, _>>();
    for _ in 0..max_iterations {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let dangling = graph
            .vertices()
            .iter()
            .filter(|vertex| outgoing[vertex] == 0)
            .map(|vertex| ranks[vertex])
            .sum::<f64>();
        let base = (1.0 - damping) / count_f64 + damping * dangling / count_f64;
        let mut next = graph
            .vertices()
            .iter()
            .copied()
            .map(|vertex| (vertex, base))
            .collect::<BTreeMap<_, _>>();
        let mut contributions = local_messages
            .iter()
            .flat_map(|(_, messages)| {
                messages.iter().map(|(source, destination, weight_bits)| {
                    (
                        *source,
                        *destination,
                        *weight_bits,
                        damping * ranks[source] / outgoing[source] as f64,
                    )
                })
            })
            .collect::<Vec<_>>();
        contributions.sort_unstable_by_key(|(source, destination, weight_bits, _)| {
            (*source, *destination, *weight_bits)
        });
        for (_, destination, _, contribution) in contributions {
            *next.get_mut(&destination).expect("validated vertex owner") += contribution;
        }
        let delta = graph
            .vertices()
            .iter()
            .map(|vertex| (next[vertex] - ranks[vertex]).abs())
            .sum::<f64>();
        ranks = next;
        if delta <= tolerance {
            break;
        }
    }
    Ok(ranks)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimeOrder {
    Strict,
    NonDecreasing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WaitingPolicy {
    Allowed,
    Forbidden,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TemporalPathRequest {
    source: VertexId,
    valid_from: ValidTime,
    valid_to: ValidTime,
    time_order: TimeOrder,
    waiting: WaitingPolicy,
}

impl TemporalPathRequest {
    pub fn new(
        source: VertexId,
        valid_from: ValidTime,
        valid_to: ValidTime,
        time_order: TimeOrder,
        waiting: WaitingPolicy,
    ) -> Result<Self, AlgorithmError> {
        if valid_from > valid_to
            || matches!(
                (time_order, waiting),
                (TimeOrder::Strict, WaitingPolicy::Forbidden)
            )
        {
            return Err(AlgorithmError::InvalidTemporalSemantics);
        }
        Ok(Self {
            source,
            valid_from,
            valid_to,
            time_order,
            waiting,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EarliestArrivalResult {
    arrivals: BTreeMap<VertexId, ValidTime>,
    predecessors: BTreeMap<VertexId, VertexId>,
}

impl EarliestArrivalResult {
    #[must_use]
    pub fn arrival(&self, vertex: VertexId) -> Option<ValidTime> {
        self.arrivals.get(&vertex).copied()
    }

    #[must_use]
    pub fn predecessor(&self, vertex: VertexId) -> Option<VertexId> {
        self.predecessors.get(&vertex).copied()
    }

    #[must_use]
    pub const fn arrivals(&self) -> &BTreeMap<VertexId, ValidTime> {
        &self.arrivals
    }

    #[must_use]
    pub const fn predecessors(&self) -> &BTreeMap<VertexId, VertexId> {
        &self.predecessors
    }
}

pub fn earliest_arrival(
    graph: &EventGraph,
    request: TemporalPathRequest,
) -> Result<EarliestArrivalResult, AlgorithmError> {
    ensure_vertex(graph.vertices(), request.source)?;
    let mut arrivals = BTreeMap::from([(request.source, request.valid_from)]);
    let mut predecessors = BTreeMap::new();
    let mut frontier = BinaryHeap::from([Reverse((request.valid_from, request.source))]);
    while let Some(Reverse((arrival, vertex))) = frontier.pop() {
        if arrivals.get(&vertex).copied() != Some(arrival) {
            continue;
        }
        for event in graph.outgoing(vertex) {
            if event.event_time() < request.valid_from || event.event_time() > request.valid_to {
                continue;
            }
            let first_departure = vertex == request.source
                && arrival == request.valid_from
                && !predecessors.contains_key(&vertex);
            let time_order_ok = match request.time_order {
                TimeOrder::Strict => event.event_time() > arrival,
                TimeOrder::NonDecreasing => event.event_time() >= arrival,
            };
            let waiting_ok = match request.waiting {
                WaitingPolicy::Allowed => true,
                WaitingPolicy::Forbidden => first_departure || event.event_time() == arrival,
            };
            if !time_order_ok || !waiting_ok {
                continue;
            }
            let next_arrival = event
                .arrival_time()
                .map_err(|_| AlgorithmError::TimeOverflow)?;
            if next_arrival > request.valid_to {
                continue;
            }
            let destination = event.destination();
            let improve = arrivals
                .get(&destination)
                .is_none_or(|previous| next_arrival < *previous);
            if improve {
                arrivals.insert(destination, next_arrival);
                predecessors.insert(destination, vertex);
                frontier.push(Reverse((next_arrival, destination)));
            }
        }
    }
    Ok(EarliestArrivalResult {
        arrivals,
        predecessors,
    })
}

pub fn temporal_reachability(
    graph: &EventGraph,
    request: TemporalPathRequest,
) -> Result<BTreeMap<VertexId, bool>, AlgorithmError> {
    let arrivals = earliest_arrival(graph, request)?.arrivals;
    Ok(graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, arrivals.contains_key(&vertex)))
        .collect())
}

pub fn min_hop_temporal_path(
    graph: &EventGraph,
    request: TemporalPathRequest,
) -> Result<BTreeMap<VertexId, u64>, AlgorithmError> {
    ensure_vertex(graph.vertices(), request.source)?;
    let mut hops = BTreeMap::from([(request.source, 0_u64)]);
    let mut frontier = VecDeque::from([(request.source, request.valid_from)]);
    while let Some((vertex, arrival)) = frontier.pop_front() {
        let next_hop = hops[&vertex].saturating_add(1);
        for event in graph.outgoing(vertex) {
            if event.event_time() < request.valid_from
                || event.event_time() > request.valid_to
                || event.event_time() < arrival
            {
                continue;
            }
            if let Some(previous) = hops.get(&event.destination())
                && *previous <= next_hop
            {
                continue;
            }
            let next_arrival = event
                .arrival_time()
                .map_err(|_| AlgorithmError::TimeOverflow)?;
            if next_arrival > request.valid_to {
                continue;
            }
            hops.insert(event.destination(), next_hop);
            frontier.push_back((event.destination(), next_arrival));
        }
    }
    Ok(hops)
}

pub fn latest_departure(
    graph: &EventGraph,
    destination: VertexId,
    deadline: ValidTime,
) -> Result<BTreeMap<VertexId, ValidTime>, AlgorithmError> {
    ensure_vertex(graph.vertices(), destination)?;
    let mut latest = BTreeMap::from([(destination, deadline)]);
    let mut changed = true;
    while changed {
        changed = false;
        for event in graph.events().iter().rev() {
            let Some(destination_latest) = latest.get(&event.destination()).copied() else {
                continue;
            };
            let arrival = event
                .arrival_time()
                .map_err(|_| AlgorithmError::TimeOverflow)?;
            if arrival > destination_latest {
                continue;
            }
            if latest
                .get(&event.source())
                .is_none_or(|current| event.event_time() > *current)
            {
                latest.insert(event.source(), event.event_time());
                changed = true;
            }
        }
    }
    Ok(latest)
}

pub const MAX_TEMPORAL_MOTIF_EVENTS: usize = 4_096;
pub const MAX_TEMPORAL_MOTIF_TRIPLES: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DeltaEntityType {
    Vertex,
    Edge,
}

pub fn windowed_components_cancellable<F>(
    graph: &EventGraph,
    valid_from: ValidTime,
    valid_to: ValidTime,
    canceled: F,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError>
where
    F: Fn() -> bool,
{
    validate_event_window(valid_from, valid_to)?;
    let mut edges = Vec::new();
    for event in graph.events() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if event.event_time() >= valid_from && event.event_time() <= valid_to {
            edges.push((event.source(), event.destination()));
        }
    }
    undirected_components_cancellable(graph.vertices(), edges, &canceled)
}

pub fn windowed_triangle_count_cancellable<F>(
    graph: &EventGraph,
    valid_from: ValidTime,
    valid_to: ValidTime,
    canceled: F,
) -> Result<u64, AlgorithmError>
where
    F: Fn() -> bool,
{
    validate_event_window(valid_from, valid_to)?;
    let mut neighbors = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for event in graph.events() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if event.event_time() < valid_from
            || event.event_time() > valid_to
            || event.source() == event.destination()
        {
            continue;
        }
        neighbors
            .get_mut(&event.source())
            .expect("validated source")
            .insert(event.destination());
        neighbors
            .get_mut(&event.destination())
            .expect("validated destination")
            .insert(event.source());
    }

    let mut count = 0_u64;
    for (left_index, left) in graph.vertices().iter().enumerate() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        for (middle_index, middle) in graph.vertices().iter().enumerate().skip(left_index + 1) {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            if !neighbors[left].contains(middle) {
                continue;
            }
            for right in graph.vertices().iter().skip(middle_index + 1) {
                if canceled() {
                    return Err(AlgorithmError::Canceled);
                }
                if neighbors[left].contains(right) && neighbors[middle].contains(right) {
                    count = count.saturating_add(1);
                }
            }
        }
    }
    Ok(count)
}

pub fn change_point_scores_cancellable<F>(
    graph: &EventGraph,
    first_from: ValidTime,
    first_to: ValidTime,
    second_from: ValidTime,
    second_to: ValidTime,
    canceled: F,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError>
where
    F: Fn() -> bool,
{
    if first_from >= first_to || first_to >= second_from || second_from >= second_to {
        return Err(AlgorithmError::InvalidTemporalWindow);
    }
    let first_duration = first_to
        .as_micros()
        .checked_sub(first_from.as_micros())
        .ok_or(AlgorithmError::TimeOverflow)?;
    let second_duration = second_to
        .as_micros()
        .checked_sub(second_from.as_micros())
        .ok_or(AlgorithmError::TimeOverflow)?;
    let mut first_counts = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, 0_u64))
        .collect::<BTreeMap<_, _>>();
    let mut second_counts = first_counts.clone();
    for event in graph.events() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let counts = if event.event_time() >= first_from && event.event_time() <= first_to {
            Some(&mut first_counts)
        } else if event.event_time() >= second_from && event.event_time() <= second_to {
            Some(&mut second_counts)
        } else {
            None
        };
        let Some(counts) = counts else {
            continue;
        };
        *counts.get_mut(&event.source()).expect("validated source") =
            counts[&event.source()].saturating_add(1);
        if event.destination() != event.source() {
            *counts
                .get_mut(&event.destination())
                .expect("validated destination") = counts[&event.destination()].saturating_add(1);
        }
    }

    let mut scores = BTreeMap::new();
    for vertex in graph.vertices() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let first_rate = first_counts[vertex] as f64 / first_duration as f64;
        let second_rate = second_counts[vertex] as f64 / second_duration as f64;
        let denominator = first_rate.abs() + second_rate.abs();
        let score = if denominator == 0.0 {
            0.0
        } else {
            (second_rate - first_rate).abs() / denominator
        };
        scores.insert(*vertex, score);
    }
    Ok(scores)
}

pub fn temporal_motif_count_cancellable<F>(
    graph: &EventGraph,
    valid_from: ValidTime,
    valid_to: ValidTime,
    delta_micros: i64,
    canceled: F,
) -> Result<BTreeMap<String, u64>, AlgorithmError>
where
    F: Fn() -> bool,
{
    validate_event_window(valid_from, valid_to)?;
    if delta_micros <= 0 {
        return Err(AlgorithmError::InvalidTemporalMotifDelta);
    }
    let mut candidates = Vec::new();
    for event in graph.events() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if event.event_time() < valid_from || event.event_time() > valid_to {
            continue;
        }
        candidates.push(event);
        if candidates.len() > MAX_TEMPORAL_MOTIF_EVENTS {
            return Err(AlgorithmError::TemporalMotifCapacity);
        }
    }
    ensure_temporal_motif_triple_budget(&candidates, delta_micros, &canceled)?;

    let mut motifs = BTreeMap::<String, u64>::new();
    for first_index in 0..candidates.len() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let first = candidates[first_index];
        for second_index in (first_index + 1)..candidates.len() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            let second = candidates[second_index];
            for third in candidates.iter().skip(second_index + 1) {
                if canceled() {
                    return Err(AlgorithmError::Canceled);
                }
                let span_micros = i128::from(third.event_time().as_micros())
                    - i128::from(first.event_time().as_micros());
                if span_micros > i128::from(delta_micros) {
                    break;
                }
                let events = [first, second, third];
                let distinct_vertices = [
                    first.source(),
                    first.destination(),
                    second.source(),
                    second.destination(),
                    third.source(),
                    third.destination(),
                ]
                .into_iter()
                .collect::<BTreeSet<_>>();
                if distinct_vertices.len() > 3
                    || !events_form_connected_union(events, &distinct_vertices)
                {
                    continue;
                }
                let motif = canonical_motif(events);
                let count = motifs.entry(motif).or_default();
                *count = count.saturating_add(1);
            }
        }
    }
    Ok(motifs)
}

fn ensure_temporal_motif_triple_budget<F>(
    candidates: &[&EventEdge],
    delta_micros: i64,
    canceled: &F,
) -> Result<(), AlgorithmError>
where
    F: Fn() -> bool,
{
    let mut candidate_triples = 0_u64;
    for first_index in 0..candidates.len() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let first = candidates[first_index];
        for second_index in (first_index + 1)..candidates.len() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            for third in candidates.iter().skip(second_index + 1) {
                if canceled() {
                    return Err(AlgorithmError::Canceled);
                }
                let span_micros = i128::from(third.event_time().as_micros())
                    - i128::from(first.event_time().as_micros());
                if span_micros > i128::from(delta_micros) {
                    break;
                }
                candidate_triples = candidate_triples.saturating_add(1);
                if candidate_triples > MAX_TEMPORAL_MOTIF_TRIPLES {
                    return Err(AlgorithmError::TemporalMotifCapacity);
                }
            }
        }
    }
    Ok(())
}

pub fn interval_components_cancellable<F>(
    graph: &IntervalGraph,
    canceled: F,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError>
where
    F: Fn() -> bool,
{
    let mut vertices = BTreeSet::new();
    for vertex in graph.vertices() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        vertices.insert(vertex.vertex());
    }
    let mut edges = Vec::new();
    for edge in graph.edges() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        edges.push((edge.source(), edge.destination()));
    }
    undirected_components_cancellable(&vertices.into_iter().collect::<Vec<_>>(), edges, &canceled)
}

pub fn delta_summary_cancellable<F>(
    graph: &DeltaGraph,
    canceled: F,
) -> Result<BTreeMap<(DeltaEntityType, DeltaKind), u64>, AlgorithmError>
where
    F: Fn() -> bool,
{
    if canceled() {
        return Err(AlgorithmError::Canceled);
    }
    let mut summary = BTreeMap::new();
    for vertex in graph.vertices() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let key = (DeltaEntityType::Vertex, vertex.change());
        let count = summary.entry(key).or_insert(0_u64);
        *count = count.saturating_add(1);
    }
    for edge in graph.edges() {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        let key = (DeltaEntityType::Edge, edge.change());
        let count = summary.entry(key).or_insert(0_u64);
        *count = count.saturating_add(1);
    }
    Ok(summary)
}

fn validate_event_window(valid_from: ValidTime, valid_to: ValidTime) -> Result<(), AlgorithmError> {
    if valid_from > valid_to {
        Err(AlgorithmError::InvalidTemporalWindow)
    } else {
        Ok(())
    }
}

fn undirected_components_cancellable<F>(
    vertices: &[VertexId],
    edges: impl IntoIterator<Item = (VertexId, VertexId)>,
    canceled: &F,
) -> Result<BTreeMap<VertexId, VertexId>, AlgorithmError>
where
    F: Fn() -> bool,
{
    let mut neighbors = vertices
        .iter()
        .copied()
        .map(|vertex| (vertex, BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for (source, destination) in edges {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if source == destination {
            continue;
        }
        neighbors
            .get_mut(&source)
            .expect("validated source")
            .insert(destination);
        neighbors
            .get_mut(&destination)
            .expect("validated destination")
            .insert(source);
    }

    let mut components = BTreeMap::new();
    let mut visited = BTreeSet::new();
    for seed in vertices {
        if canceled() {
            return Err(AlgorithmError::Canceled);
        }
        if !visited.insert(*seed) {
            continue;
        }
        let mut frontier = VecDeque::from([*seed]);
        while let Some(vertex) = frontier.pop_front() {
            if canceled() {
                return Err(AlgorithmError::Canceled);
            }
            components.insert(vertex, *seed);
            for neighbor in &neighbors[&vertex] {
                if canceled() {
                    return Err(AlgorithmError::Canceled);
                }
                if visited.insert(*neighbor) {
                    frontier.push_back(*neighbor);
                }
            }
        }
    }
    Ok(components)
}

fn events_form_connected_union(events: [&EventEdge; 3], vertices: &BTreeSet<VertexId>) -> bool {
    let Some(start) = vertices.first().copied() else {
        return false;
    };
    let mut reached = BTreeSet::from([start]);
    loop {
        let previous_len = reached.len();
        for event in events {
            if reached.contains(&event.source()) || reached.contains(&event.destination()) {
                reached.insert(event.source());
                reached.insert(event.destination());
            }
        }
        if reached.len() == previous_len {
            return reached.len() == vertices.len();
        }
    }
}

fn canonical_motif(events: [&EventEdge; 3]) -> String {
    const PERMUTATIONS: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];

    PERMUTATIONS
        .into_iter()
        .filter(|order| {
            events[order[0]].event_time() <= events[order[1]].event_time()
                && events[order[1]].event_time() <= events[order[2]].event_time()
        })
        .map(|order| {
            canonical_ordered_motif([events[order[0]], events[order[1]], events[order[2]]])
        })
        .min()
        .expect("the original event order is time-ordered")
}

fn canonical_ordered_motif(events: [&EventEdge; 3]) -> String {
    let mut labels = BTreeMap::<VertexId, char>::new();
    let mut next_label = b'A';
    let mut encoded = Vec::with_capacity(events.len());
    for event in events {
        let source = *labels.entry(event.source()).or_insert_with(|| {
            let label = char::from(next_label);
            next_label = next_label.saturating_add(1);
            label
        });
        let destination = *labels.entry(event.destination()).or_insert_with(|| {
            let label = char::from(next_label);
            next_label = next_label.saturating_add(1);
            label
        });
        encoded.push(format!("{source}>{destination}"));
    }
    encoded.join("|")
}

fn ensure_vertex(vertices: &[VertexId], vertex: VertexId) -> Result<(), AlgorithmError> {
    if vertices.binary_search(&vertex).is_err() {
        return Err(AlgorithmError::UnknownSource(vertex));
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlgorithmError {
    UnknownSource(VertexId),
    DistanceOverflow,
    AllPairsCapacity,
    CentralityCapacity,
    InvalidPageRankConfiguration,
    InvalidLouvainConfiguration,
    InvalidBetweennessWeight,
    NegativeWeight,
    InvalidTemporalSemantics,
    InvalidTemporalWindow,
    InvalidTemporalMotifDelta,
    TemporalMotifCapacity,
    TimeOverflow,
    InvalidLabelPropagationConfiguration,
    Canceled,
}

impl Display for AlgorithmError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "analytics algorithm failed: {self:?}")
    }
}

impl Error for AlgorithmError {}

#[cfg(test)]
mod projection_error_tests {
    use super::*;

    #[test]
    fn snapshot_and_event_projection_preserve_response_wire_byte_exhaustion() {
        let expected = ProjectionError::ResponseByteLimit {
            limit: 64,
            required: 65,
        };

        assert_eq!(
            projection_scan_error(
                temporal_storage::TemporalStoreError::ScanResponseByteLimit {
                    limit: 64,
                    required: 65,
                },
                true,
            ),
            expected
        );
        assert_eq!(
            event_projection_scan_error(
                temporal_storage::TemporalStoreError::ScanResponseByteLimit {
                    limit: 64,
                    required: 65,
                }
            ),
            expected
        );
    }

    #[test]
    fn snapshot_and_event_projection_preserve_storage_unavailability() {
        let unavailable = temporal_storage::TemporalStoreError::Adapter(AdapterError::Unavailable(
            "Shard transport closed".into(),
        ));
        assert_eq!(
            projection_scan_error(unavailable.clone(), true),
            ProjectionError::StorageUnavailable("Shard transport closed".into())
        );
        assert_eq!(
            event_projection_scan_error(unavailable),
            ProjectionError::StorageUnavailable("Shard transport closed".into())
        );
    }
}
