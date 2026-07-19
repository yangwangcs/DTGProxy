#![forbid(unsafe_code)]

use std::cmp::Ordering;
use std::cmp::Reverse;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use analytics_api::{
    EventEdge, EventGraph, GraphProjectionError, SnapshotEdge, SnapshotGraph, VertexId,
};
use storage_api::StorageAdapter;
use temporal_storage::{GraphId, TemporalStore};
use temporal_types::ValidTime;
use temporal_types::{GraphValue, TransactionTime};

mod jobs;
mod provider;

pub use jobs::{AnalyticsJobId, AnalyticsJobManager, JobCheckpoint, JobError, JobState, JobStatus};
pub use provider::BuiltInProvider;

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
    let vertices = store
        .scan_vertex_views_as_of(graph, valid_time, transaction_time)
        .await
        .map_err(|error| ProjectionError::Storage(error.to_string()))?
        .into_iter()
        .map(|vertex| VertexId::new(vertex.element().id().value()))
        .collect::<Vec<_>>();
    let edges = store
        .scan_edges_as_of(graph, valid_time, transaction_time)
        .await
        .map_err(|error| ProjectionError::Storage(error.to_string()))?
        .into_iter()
        .map(|edge| {
            let weight =
                match weight_property.and_then(|property| edge.payload().property(property)) {
                    None => 1.0,
                    Some(GraphValue::Integer(value)) => *value as f64,
                    Some(GraphValue::FloatBits(value)) => f64::from_bits(*value),
                    Some(_) => return Err(ProjectionError::InvalidWeightProperty),
                };
            SnapshotEdge::new(
                VertexId::new(edge.source().value()),
                VertexId::new(edge.destination().value()),
                weight,
            )
            .map_err(ProjectionError::Graph)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok((vertices, edges))
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
        .map_err(|error| ProjectionError::Storage(error.to_string()))?;
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectionError {
    Storage(String),
    Graph(GraphProjectionError),
    InvalidWeightProperty,
    InvalidEventLimit,
}

impl Display for ProjectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "analytics storage projection failed: {self:?}")
    }
}

impl Error for ProjectionError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BfsResult {
    distances: BTreeMap<VertexId, u64>,
    predecessors: BTreeMap<VertexId, VertexId>,
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
    ensure_vertex(graph.vertices(), source)?;
    let mut distances = BTreeMap::from([(source, 0_u64)]);
    let mut predecessors = BTreeMap::new();
    let mut frontier = VecDeque::from([source]);
    while let Some(vertex) = frontier.pop_front() {
        let next_distance = distances[&vertex]
            .checked_add(1)
            .ok_or(AlgorithmError::DistanceOverflow)?;
        for edge in graph.outgoing(vertex) {
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
    ensure_vertex(graph.vertices(), source)?;
    if graph.edges().iter().any(|edge| edge.weight() < 0.0) {
        return Err(AlgorithmError::NegativeWeight);
    }
    let mut distances = BTreeMap::from([(source, 0.0)]);
    let mut predecessors = BTreeMap::new();
    let mut frontier = BinaryHeap::from([WeightedFrontier {
        distance: 0.0,
        vertex: source,
    }]);
    while let Some(current) = frontier.pop() {
        if distances
            .get(&current.vertex)
            .is_none_or(|distance| *distance != current.distance)
        {
            continue;
        }
        for edge in graph.outgoing(current.vertex) {
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

#[must_use]
pub fn scc(graph: &SnapshotGraph) -> BTreeMap<VertexId, VertexId> {
    let mut seen = BTreeMap::<VertexId, ()>::new();
    let mut order = Vec::with_capacity(graph.vertices().len());
    for start in graph.vertices() {
        if seen.contains_key(start) {
            continue;
        }
        let mut stack = vec![(*start, false)];
        while let Some((vertex, expanded)) = stack.pop() {
            if expanded {
                order.push(vertex);
                continue;
            }
            if seen.insert(vertex, ()).is_some() {
                continue;
            }
            stack.push((vertex, true));
            for edge in graph.outgoing(vertex).iter().rev() {
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
        if assigned.contains_key(&start) {
            continue;
        }
        let mut members = Vec::new();
        let mut stack = vec![start];
        assigned.insert(start, start);
        while let Some(vertex) = stack.pop() {
            members.push(vertex);
            for predecessor in &incoming[&vertex] {
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
    assigned
}

#[must_use]
pub fn wcc(graph: &SnapshotGraph) -> BTreeMap<VertexId, VertexId> {
    let mut components = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, vertex))
        .collect::<BTreeMap<_, _>>();
    loop {
        let mut changed = false;
        for edge in graph.edges() {
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
            let parent = components[vertex];
            let root = components[&parent];
            if parent != root {
                components.insert(*vertex, root);
                changed = true;
            }
        }
        if !changed {
            return components;
        }
    }
}

pub fn page_rank(
    graph: &SnapshotGraph,
    damping: f64,
    max_iterations: usize,
    tolerance: f64,
) -> Result<BTreeMap<VertexId, f64>, AlgorithmError> {
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
        let dangling = graph
            .vertices()
            .iter()
            .filter(|vertex| graph.outgoing(**vertex).is_empty())
            .map(|vertex| ranks[vertex])
            .sum::<f64>();
        let base = (1.0 - damping) / count_f64 + damping * dangling / count_f64;
        let mut next = graph
            .vertices()
            .iter()
            .copied()
            .map(|vertex| (vertex, base))
            .collect::<BTreeMap<_, _>>();
        for source in graph.vertices() {
            let outgoing = graph.outgoing(*source);
            if outgoing.is_empty() {
                continue;
            }
            let contribution = damping * ranks[source] / outgoing.len() as f64;
            for edge in outgoing {
                *next
                    .get_mut(&edge.destination())
                    .expect("validated endpoint") += contribution;
            }
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
    InvalidPageRankConfiguration,
    NegativeWeight,
    InvalidTemporalSemantics,
    TimeOverflow,
    InvalidLabelPropagationConfiguration,
}

impl Display for AlgorithmError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "analytics algorithm failed: {self:?}")
    }
}

impl Error for AlgorithmError {}
