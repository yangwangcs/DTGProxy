#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};

use temporal_types::ValidTime;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct VertexId(u128);

impl VertexId {
    #[must_use]
    pub const fn new(value: u128) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u128 {
        self.0
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotEdge {
    source: VertexId,
    destination: VertexId,
    weight_bits: u64,
}

impl SnapshotEdge {
    pub fn new(
        source: VertexId,
        destination: VertexId,
        weight: f64,
    ) -> Result<Self, GraphProjectionError> {
        if !weight.is_finite() {
            return Err(GraphProjectionError::InvalidWeight);
        }
        Ok(Self {
            source,
            destination,
            weight_bits: weight.to_bits(),
        })
    }

    #[must_use]
    pub const fn source(&self) -> VertexId {
        self.source
    }

    #[must_use]
    pub const fn destination(&self) -> VertexId {
        self.destination
    }

    #[must_use]
    pub fn weight(&self) -> f64 {
        f64::from_bits(self.weight_bits)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotGraph {
    vertices: Vec<VertexId>,
    edges: Vec<SnapshotEdge>,
    directed: bool,
    outgoing: BTreeMap<VertexId, Vec<SnapshotEdge>>,
}

impl SnapshotGraph {
    pub fn new(
        mut vertices: Vec<VertexId>,
        mut edges: Vec<SnapshotEdge>,
        directed: bool,
    ) -> Result<Self, GraphProjectionError> {
        vertices.sort_unstable();
        if let Some(duplicate) = vertices
            .windows(2)
            .find(|pair| pair[0] == pair[1])
            .map(|pair| pair[0])
        {
            return Err(GraphProjectionError::DuplicateVertex(duplicate));
        }
        let vertex_set = vertices.iter().copied().collect::<BTreeSet<_>>();
        validate_snapshot_endpoints(&vertex_set, &edges)?;
        edges.sort_by_key(|edge| (edge.source, edge.destination, edge.weight_bits));
        let mut outgoing = vertices
            .iter()
            .copied()
            .map(|vertex| (vertex, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        for edge in &edges {
            outgoing
                .get_mut(&edge.source)
                .expect("validated source")
                .push(edge.clone());
            if !directed && edge.source != edge.destination {
                outgoing
                    .get_mut(&edge.destination)
                    .expect("validated destination")
                    .push(SnapshotEdge {
                        source: edge.destination,
                        destination: edge.source,
                        weight_bits: edge.weight_bits,
                    });
            }
        }
        for adjacent in outgoing.values_mut() {
            adjacent.sort_by_key(|edge| (edge.destination, edge.weight_bits));
        }
        Ok(Self {
            vertices,
            edges,
            directed,
            outgoing,
        })
    }

    #[must_use]
    pub fn vertices(&self) -> &[VertexId] {
        &self.vertices
    }

    #[must_use]
    pub fn edges(&self) -> &[SnapshotEdge] {
        &self.edges
    }

    #[must_use]
    pub const fn directed(&self) -> bool {
        self.directed
    }

    #[must_use]
    pub fn outgoing(&self, vertex: VertexId) -> &[SnapshotEdge] {
        self.outgoing.get(&vertex).map_or(&[], Vec::as_slice)
    }
}

fn validate_snapshot_endpoints(
    vertices: &BTreeSet<VertexId>,
    edges: &[SnapshotEdge],
) -> Result<(), GraphProjectionError> {
    for edge in edges {
        if !vertices.contains(&edge.source) {
            return Err(GraphProjectionError::UnknownVertex(edge.source));
        }
        if !vertices.contains(&edge.destination) {
            return Err(GraphProjectionError::UnknownVertex(edge.destination));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventEdge {
    source: VertexId,
    destination: VertexId,
    event_time: ValidTime,
    duration_micros: i64,
    weight_bits: u64,
}

impl EventEdge {
    pub fn new(
        source: VertexId,
        destination: VertexId,
        event_time: ValidTime,
        duration_micros: i64,
        weight: f64,
    ) -> Result<Self, GraphProjectionError> {
        if duration_micros < 0
            || event_time
                .as_micros()
                .checked_add(duration_micros)
                .is_none()
        {
            return Err(GraphProjectionError::InvalidDuration);
        }
        if !weight.is_finite() {
            return Err(GraphProjectionError::InvalidWeight);
        }
        Ok(Self {
            source,
            destination,
            event_time,
            duration_micros,
            weight_bits: weight.to_bits(),
        })
    }

    #[must_use]
    pub const fn source(&self) -> VertexId {
        self.source
    }

    #[must_use]
    pub const fn destination(&self) -> VertexId {
        self.destination
    }

    #[must_use]
    pub const fn event_time(&self) -> ValidTime {
        self.event_time
    }

    #[must_use]
    pub const fn duration_micros(&self) -> i64 {
        self.duration_micros
    }

    #[must_use]
    pub fn weight(&self) -> f64 {
        f64::from_bits(self.weight_bits)
    }

    pub fn arrival_time(&self) -> Result<ValidTime, GraphProjectionError> {
        self.event_time
            .as_micros()
            .checked_add(self.duration_micros)
            .map(ValidTime::from_micros)
            .ok_or(GraphProjectionError::InvalidDuration)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventGraph {
    vertices: Vec<VertexId>,
    events: Vec<EventEdge>,
    outgoing: BTreeMap<VertexId, Vec<EventEdge>>,
}

impl EventGraph {
    pub fn new(
        mut vertices: Vec<VertexId>,
        mut events: Vec<EventEdge>,
    ) -> Result<Self, GraphProjectionError> {
        vertices.sort_unstable();
        if let Some(duplicate) = vertices
            .windows(2)
            .find(|pair| pair[0] == pair[1])
            .map(|pair| pair[0])
        {
            return Err(GraphProjectionError::DuplicateVertex(duplicate));
        }
        let vertex_set = vertices.iter().copied().collect::<BTreeSet<_>>();
        for event in &events {
            if !vertex_set.contains(&event.source) {
                return Err(GraphProjectionError::UnknownVertex(event.source));
            }
            if !vertex_set.contains(&event.destination) {
                return Err(GraphProjectionError::UnknownVertex(event.destination));
            }
        }
        events.sort_by_key(|event| {
            (
                event.event_time,
                event.source,
                event.destination,
                event.duration_micros,
            )
        });
        let mut outgoing = vertices
            .iter()
            .copied()
            .map(|vertex| (vertex, Vec::new()))
            .collect::<BTreeMap<_, _>>();
        for event in &events {
            outgoing
                .get_mut(&event.source)
                .expect("validated source")
                .push(event.clone());
        }
        Ok(Self {
            vertices,
            events,
            outgoing,
        })
    }

    #[must_use]
    pub fn vertices(&self) -> &[VertexId] {
        &self.vertices
    }

    #[must_use]
    pub fn events(&self) -> &[EventEdge] {
        &self.events
    }

    #[must_use]
    pub fn outgoing(&self, vertex: VertexId) -> &[EventEdge] {
        self.outgoing.get(&vertex).map_or(&[], Vec::as_slice)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphProjectionError {
    DuplicateVertex(VertexId),
    UnknownVertex(VertexId),
    InvalidWeight,
    InvalidDuration,
}

impl Display for GraphProjectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "analytics graph projection failed: {self:?}")
    }
}

impl Error for GraphProjectionError {}
