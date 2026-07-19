#![forbid(unsafe_code)]

use std::cmp::Reverse;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BinaryHeap, VecDeque};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use analytics_api::{EventGraph, SnapshotGraph, VertexId};
use temporal_types::ValidTime;

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
    InvalidTemporalSemantics,
    TimeOverflow,
}

impl Display for AlgorithmError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "analytics algorithm failed: {self:?}")
    }
}

impl Error for AlgorithmError {}
