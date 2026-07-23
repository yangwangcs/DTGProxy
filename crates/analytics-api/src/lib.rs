#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use temporal_types::{CanonicalElement, Interval, ValidTime};

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

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EdgeId(u128);

impl EdgeId {
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
pub struct IntervalVertex {
    vertex: VertexId,
    valid: Interval<ValidTime>,
    payload: CanonicalElement,
}

impl IntervalVertex {
    #[must_use]
    pub fn new(vertex: VertexId, valid: Interval<ValidTime>, payload: CanonicalElement) -> Self {
        Self {
            vertex,
            valid,
            payload,
        }
    }

    #[must_use]
    pub const fn vertex(&self) -> VertexId {
        self.vertex
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalEdge {
    edge_id: EdgeId,
    source: VertexId,
    destination: VertexId,
    valid: Interval<ValidTime>,
    payload: CanonicalElement,
    weight_bits: u64,
}

impl IntervalEdge {
    pub fn new(
        edge_id: EdgeId,
        source: VertexId,
        destination: VertexId,
        valid: Interval<ValidTime>,
        payload: CanonicalElement,
        weight: f64,
    ) -> Result<Self, GraphProjectionError> {
        if !weight.is_finite() {
            return Err(GraphProjectionError::InvalidWeight);
        }
        Ok(Self {
            edge_id,
            source,
            destination,
            valid,
            payload,
            weight_bits: weight.to_bits(),
        })
    }

    #[must_use]
    pub const fn edge_id(&self) -> EdgeId {
        self.edge_id
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
    pub const fn valid(&self) -> Interval<ValidTime> {
        self.valid
    }

    #[must_use]
    pub const fn payload(&self) -> &CanonicalElement {
        &self.payload
    }

    #[must_use]
    pub fn weight(&self) -> f64 {
        f64::from_bits(self.weight_bits)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IntervalGraph {
    vertices: Vec<IntervalVertex>,
    edges: Vec<IntervalEdge>,
    directed: bool,
    outgoing: BTreeMap<VertexId, Vec<IntervalEdge>>,
}

impl IntervalGraph {
    pub fn new(
        mut vertices: Vec<IntervalVertex>,
        mut edges: Vec<IntervalEdge>,
        directed: bool,
    ) -> Result<Self, GraphProjectionError> {
        vertices.sort_by_key(|vertex| (vertex.vertex, vertex.valid.start(), vertex.valid.end()));
        if vertices
            .windows(2)
            .any(|pair| pair[0].vertex == pair[1].vertex && pair[0].valid == pair[1].valid)
        {
            return Err(GraphProjectionError::DuplicateIntervalVertex);
        }
        for edge in &edges {
            for endpoint in [edge.source, edge.destination] {
                if !endpoint_intervals_cover(&vertices, endpoint, edge.valid) {
                    return Err(GraphProjectionError::UncoveredIntervalEndpoint(endpoint));
                }
            }
        }
        edges.sort_by_key(|edge| {
            (
                edge.edge_id,
                edge.source,
                edge.destination,
                edge.valid.start(),
                edge.valid.end(),
                edge.weight_bits,
            )
        });
        let mut outgoing = BTreeMap::<VertexId, Vec<IntervalEdge>>::new();
        for vertex in &vertices {
            outgoing.entry(vertex.vertex).or_default();
        }
        for edge in &edges {
            outgoing.entry(edge.source).or_default().push(edge.clone());
            if !directed && edge.source != edge.destination {
                outgoing
                    .entry(edge.destination)
                    .or_default()
                    .push(IntervalEdge {
                        edge_id: edge.edge_id,
                        source: edge.destination,
                        destination: edge.source,
                        valid: edge.valid,
                        payload: edge.payload.clone(),
                        weight_bits: edge.weight_bits,
                    });
            }
        }
        Ok(Self {
            vertices,
            edges,
            directed,
            outgoing,
        })
    }

    #[must_use]
    pub fn vertices(&self) -> &[IntervalVertex] {
        &self.vertices
    }

    #[must_use]
    pub fn edges(&self) -> &[IntervalEdge] {
        &self.edges
    }

    #[must_use]
    pub const fn directed(&self) -> bool {
        self.directed
    }

    #[must_use]
    pub fn outgoing(&self, vertex: VertexId) -> &[IntervalEdge] {
        self.outgoing.get(&vertex).map_or(&[], Vec::as_slice)
    }
}

fn endpoint_intervals_cover(
    vertices: &[IntervalVertex],
    endpoint: VertexId,
    required: Interval<ValidTime>,
) -> bool {
    let mut covered_through = required.start();
    for vertex in vertices.iter().filter(|vertex| vertex.vertex == endpoint) {
        let valid = vertex.valid;
        if valid.end().is_some_and(|end| end <= covered_through) {
            continue;
        }
        if valid.start() > covered_through {
            return false;
        }
        match valid.end() {
            None => return true,
            Some(end)
                if required
                    .end()
                    .is_some_and(|required_end| end >= required_end) =>
            {
                return true;
            }
            Some(end) => covered_through = end,
        }
    }
    false
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum DeltaKind {
    Added,
    Removed,
    Updated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaVertex {
    vertex: VertexId,
    change: DeltaKind,
    before_payload: Option<CanonicalElement>,
    after_payload: Option<CanonicalElement>,
}

impl DeltaVertex {
    #[must_use]
    pub fn new(
        vertex: VertexId,
        change: DeltaKind,
        before_payload: Option<CanonicalElement>,
        after_payload: Option<CanonicalElement>,
    ) -> Self {
        Self {
            vertex,
            change,
            before_payload,
            after_payload,
        }
    }

    #[must_use]
    pub const fn vertex(&self) -> VertexId {
        self.vertex
    }

    #[must_use]
    pub const fn change(&self) -> DeltaKind {
        self.change
    }

    #[must_use]
    pub const fn before_payload(&self) -> Option<&CanonicalElement> {
        self.before_payload.as_ref()
    }

    #[must_use]
    pub const fn after_payload(&self) -> Option<&CanonicalElement> {
        self.after_payload.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaEdge {
    edge_id: EdgeId,
    source: VertexId,
    destination: VertexId,
    change: DeltaKind,
    before_payload: Option<CanonicalElement>,
    after_payload: Option<CanonicalElement>,
    before_weight_bits: Option<u64>,
    after_weight_bits: Option<u64>,
}

impl DeltaEdge {
    #[must_use]
    #[allow(clippy::double_must_use, clippy::too_many_arguments)]
    pub fn new(
        edge_id: EdgeId,
        source: VertexId,
        destination: VertexId,
        change: DeltaKind,
        before_payload: Option<CanonicalElement>,
        after_payload: Option<CanonicalElement>,
        before_weight: Option<f64>,
        after_weight: Option<f64>,
    ) -> Result<Self, GraphProjectionError> {
        if before_weight.is_some_and(|weight| !weight.is_finite())
            || after_weight.is_some_and(|weight| !weight.is_finite())
        {
            return Err(GraphProjectionError::InvalidWeight);
        }
        Ok(Self {
            edge_id,
            source,
            destination,
            change,
            before_payload,
            after_payload,
            before_weight_bits: before_weight.map(f64::to_bits),
            after_weight_bits: after_weight.map(f64::to_bits),
        })
    }

    #[must_use]
    pub const fn edge_id(&self) -> EdgeId {
        self.edge_id
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
    pub const fn change(&self) -> DeltaKind {
        self.change
    }

    #[must_use]
    pub const fn before_payload(&self) -> Option<&CanonicalElement> {
        self.before_payload.as_ref()
    }

    #[must_use]
    pub const fn after_payload(&self) -> Option<&CanonicalElement> {
        self.after_payload.as_ref()
    }

    #[must_use]
    pub fn before_weight(&self) -> Option<f64> {
        self.before_weight_bits.map(f64::from_bits)
    }

    #[must_use]
    pub fn after_weight(&self) -> Option<f64> {
        self.after_weight_bits.map(f64::from_bits)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DeltaGraph {
    vertices: Vec<DeltaVertex>,
    edges: Vec<DeltaEdge>,
}

impl DeltaGraph {
    #[must_use]
    pub fn new(mut vertices: Vec<DeltaVertex>, mut edges: Vec<DeltaEdge>) -> Self {
        vertices.sort_by_key(|vertex| (vertex.vertex, vertex.change));
        vertices.dedup();
        edges.sort_by_key(|edge| {
            (
                edge.edge_id,
                edge.source,
                edge.destination,
                edge.change,
                edge.before_weight_bits,
                edge.after_weight_bits,
            )
        });
        edges.dedup();
        Self { vertices, edges }
    }

    #[must_use]
    pub fn vertices(&self) -> &[DeltaVertex] {
        &self.vertices
    }

    #[must_use]
    pub fn edges(&self) -> &[DeltaEdge] {
        &self.edges
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotPartition {
    shard_id: u32,
    vertices: Vec<VertexId>,
    edges: Vec<SnapshotEdge>,
}

impl SnapshotPartition {
    #[must_use]
    pub fn new(shard_id: u32, mut vertices: Vec<VertexId>, mut edges: Vec<SnapshotEdge>) -> Self {
        vertices.sort_unstable();
        edges.sort_by_key(|edge| (edge.source(), edge.destination(), edge.weight().to_bits()));
        Self {
            shard_id,
            vertices,
            edges,
        }
    }

    #[must_use]
    pub const fn shard_id(&self) -> u32 {
        self.shard_id
    }

    #[must_use]
    pub fn vertices(&self) -> &[VertexId] {
        &self.vertices
    }

    #[must_use]
    pub fn edges(&self) -> &[SnapshotEdge] {
        &self.edges
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PartitionedSnapshotGraph {
    partitions: Vec<SnapshotPartition>,
    vertices: Vec<VertexId>,
    owners: BTreeMap<VertexId, u32>,
    edge_count: usize,
    directed: bool,
}

impl PartitionedSnapshotGraph {
    pub fn new(
        mut partitions: Vec<SnapshotPartition>,
        directed: bool,
    ) -> Result<Self, GraphProjectionError> {
        partitions.sort_by_key(SnapshotPartition::shard_id);
        if let Some(shard_id) = partitions
            .windows(2)
            .find(|pair| pair[0].shard_id == pair[1].shard_id)
            .map(|pair| pair[0].shard_id)
        {
            return Err(GraphProjectionError::DuplicateShard(shard_id));
        }

        let mut owners = BTreeMap::new();
        for partition in &partitions {
            for vertex in &partition.vertices {
                if owners.insert(*vertex, partition.shard_id).is_some() {
                    return Err(GraphProjectionError::DuplicateVertexOwner(*vertex));
                }
            }
        }
        let vertices = owners.keys().copied().collect::<Vec<_>>();
        for edge in partitions.iter().flat_map(SnapshotPartition::edges) {
            if !owners.contains_key(&edge.source()) {
                return Err(GraphProjectionError::UnknownVertex(edge.source()));
            }
            if !owners.contains_key(&edge.destination()) {
                return Err(GraphProjectionError::UnknownVertex(edge.destination()));
            }
        }
        let edge_count = partitions
            .iter()
            .map(|partition| partition.edges.len())
            .fold(0_usize, usize::saturating_add);
        Ok(Self {
            partitions,
            vertices,
            owners,
            edge_count,
            directed,
        })
    }

    #[must_use]
    pub fn partitions(&self) -> &[SnapshotPartition] {
        &self.partitions
    }

    #[must_use]
    pub fn vertices(&self) -> &[VertexId] {
        &self.vertices
    }

    #[must_use]
    pub fn owner(&self, vertex: VertexId) -> Option<u32> {
        self.owners.get(&vertex).copied()
    }

    #[must_use]
    pub const fn edge_count(&self) -> usize {
        self.edge_count
    }

    #[must_use]
    pub const fn directed(&self) -> bool {
        self.directed
    }

    pub fn canonical_snapshot_bounded(
        &self,
        max_vertices: usize,
        max_edges: usize,
    ) -> Result<SnapshotGraph, GraphProjectionError> {
        if self.vertices.len() > max_vertices {
            return Err(GraphProjectionError::SnapshotVertexLimit {
                limit: max_vertices,
                actual: self.vertices.len(),
            });
        }
        if self.edge_count > max_edges {
            return Err(GraphProjectionError::SnapshotEdgeLimit {
                limit: max_edges,
                actual: self.edge_count,
            });
        }
        SnapshotGraph::new(
            self.vertices.clone(),
            self.partitions
                .iter()
                .flat_map(|partition| partition.edges.iter().cloned())
                .collect(),
            self.directed,
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GraphProjectionError {
    DuplicateVertex(VertexId),
    DuplicateVertexOwner(VertexId),
    DuplicateShard(u32),
    UnknownVertex(VertexId),
    InvalidWeight,
    InvalidDuration,
    DuplicateIntervalVertex,
    UncoveredIntervalEndpoint(VertexId),
    SnapshotVertexLimit { limit: usize, actual: usize },
    SnapshotEdgeLimit { limit: usize, actual: usize },
}

impl Display for GraphProjectionError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "analytics graph projection failed: {self:?}")
    }
}

impl Error for GraphProjectionError {}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum GraphModel {
    Snapshot,
    Event,
    Interval,
    Delta,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum AlgorithmType {
    Boolean,
    Integer,
    Float,
    String,
    Vertex,
    Time,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlgorithmField {
    name: String,
    value_type: AlgorithmType,
    nullable: bool,
    default: Option<AlgorithmValue>,
}

impl AlgorithmField {
    #[must_use]
    pub fn required(name: impl Into<String>, value_type: AlgorithmType) -> Self {
        Self {
            name: name.into(),
            value_type,
            nullable: false,
            default: None,
        }
    }

    #[must_use]
    pub fn nullable_output(name: impl Into<String>, value_type: AlgorithmType) -> Self {
        Self {
            name: name.into(),
            value_type,
            nullable: true,
            default: None,
        }
    }

    #[must_use]
    pub fn optional(
        name: impl Into<String>,
        value_type: AlgorithmType,
        default: AlgorithmValue,
    ) -> Self {
        Self {
            name: name.into(),
            value_type,
            nullable: false,
            default: Some(default),
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn value_type(&self) -> AlgorithmType {
        self.value_type
    }

    #[must_use]
    pub const fn nullable(&self) -> bool {
        self.nullable
    }

    #[must_use]
    pub const fn default(&self) -> Option<&AlgorithmValue> {
        self.default.as_ref()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlgorithmDescriptor {
    name: String,
    version: String,
    graph_models: Vec<GraphModel>,
    exact: bool,
    deterministic: bool,
    distributed: bool,
    incremental: bool,
    inputs: Vec<AlgorithmField>,
    outputs: Vec<AlgorithmField>,
}

impl AlgorithmDescriptor {
    #[allow(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        graph_models: Vec<GraphModel>,
        exact: bool,
        deterministic: bool,
        distributed: bool,
        incremental: bool,
    ) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            graph_models,
            exact,
            deterministic,
            distributed,
            incremental,
            inputs: Vec::new(),
            outputs: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_signature(
        mut self,
        inputs: Vec<AlgorithmField>,
        outputs: Vec<AlgorithmField>,
    ) -> Self {
        self.inputs = inputs;
        self.outputs = outputs;
        self
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn graph_models(&self) -> &[GraphModel] {
        &self.graph_models
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    #[must_use]
    pub const fn exact(&self) -> bool {
        self.exact
    }

    #[must_use]
    pub const fn deterministic(&self) -> bool {
        self.deterministic
    }

    #[must_use]
    pub const fn distributed(&self) -> bool {
        self.distributed
    }

    #[must_use]
    pub const fn incremental(&self) -> bool {
        self.incremental
    }

    #[must_use]
    pub fn inputs(&self) -> &[AlgorithmField] {
        &self.inputs
    }

    #[must_use]
    pub fn outputs(&self) -> &[AlgorithmField] {
        &self.outputs
    }
}

#[must_use]
pub fn builtin_algorithm_descriptors() -> Vec<AlgorithmDescriptor> {
    use AlgorithmType::{Boolean, Float, Integer, String as Text, Time, Vertex};

    let required = |name, value_type| AlgorithmField::required(name, value_type);
    let nullable = |name, value_type| AlgorithmField::nullable_output(name, value_type);
    let optional = |name, value_type, default| AlgorithmField::optional(name, value_type, default);
    let descriptor = |name: &str, model, inputs, outputs| {
        AlgorithmDescriptor::new(
            name,
            env!("CARGO_PKG_VERSION"),
            vec![model],
            true,
            true,
            matches!(
                name,
                "dtg.graph.degree" | "dtg.graph.wcc" | "dtg.graph.pageRank"
            ),
            false,
        )
        .with_signature(inputs, outputs)
    };
    let approximate_descriptor = |name: &str, model, inputs, outputs| {
        AlgorithmDescriptor::new(
            name,
            env!("CARGO_PKG_VERSION"),
            vec![model],
            false,
            true,
            false,
            false,
        )
        .with_signature(inputs, outputs)
    };
    let vertex = || required("vertexId", Vertex);
    let source = || required("source", Vertex);
    let window = || {
        vec![
            source(),
            required("validFrom", Time),
            required("validTo", Time),
        ]
    };

    vec![
        descriptor(
            "dtg.graph.bfs",
            GraphModel::Snapshot,
            vec![source()],
            vec![
                vertex(),
                required("distance", Integer),
                nullable("predecessor", Vertex),
            ],
        ),
        descriptor(
            "dtg.graph.dfs",
            GraphModel::Snapshot,
            vec![source()],
            vec![
                vertex(),
                required("depth", Integer),
                nullable("predecessor", Vertex),
            ],
        ),
        descriptor(
            "dtg.graph.sssp",
            GraphModel::Snapshot,
            vec![source()],
            vec![
                vertex(),
                required("distance", Float),
                nullable("predecessor", Vertex),
            ],
        ),
        descriptor(
            "dtg.graph.allPairsShortestPath",
            GraphModel::Snapshot,
            Vec::new(),
            vec![
                required("source", Vertex),
                required("target", Vertex),
                required("distance", Float),
            ],
        ),
        descriptor(
            "dtg.graph.wcc",
            GraphModel::Snapshot,
            Vec::new(),
            vec![vertex(), required("componentId", Vertex)],
        ),
        descriptor(
            "dtg.graph.scc",
            GraphModel::Snapshot,
            Vec::new(),
            vec![vertex(), required("componentId", Vertex)],
        ),
        descriptor(
            "dtg.graph.pageRank",
            GraphModel::Snapshot,
            vec![
                optional(
                    "damping",
                    Float,
                    AlgorithmValue::FloatBits(0.85_f64.to_bits()),
                ),
                optional("maxIterations", Integer, AlgorithmValue::Integer(100)),
                optional(
                    "tolerance",
                    Float,
                    AlgorithmValue::FloatBits(1e-9_f64.to_bits()),
                ),
            ],
            vec![vertex(), required("score", Float)],
        ),
        descriptor(
            "dtg.graph.degree",
            GraphModel::Snapshot,
            Vec::new(),
            vec![
                vertex(),
                required("inDegree", Integer),
                required("outDegree", Integer),
                required("degree", Integer),
            ],
        ),
        descriptor(
            "dtg.graph.triangleCount",
            GraphModel::Snapshot,
            Vec::new(),
            vec![required("triangleCount", Integer)],
        ),
        descriptor(
            "dtg.graph.clusteringCoefficient",
            GraphModel::Snapshot,
            Vec::new(),
            vec![vertex(), required("coefficient", Float)],
        ),
        descriptor(
            "dtg.graph.betweenness",
            GraphModel::Snapshot,
            Vec::new(),
            vec![vertex(), required("score", Float)],
        ),
        descriptor(
            "dtg.graph.closeness",
            GraphModel::Snapshot,
            Vec::new(),
            vec![vertex(), required("score", Float)],
        ),
        descriptor(
            "dtg.graph.kCore",
            GraphModel::Snapshot,
            vec![optional("k", Integer, AlgorithmValue::Integer(2))],
            vec![vertex(), required("core", Integer)],
        ),
        descriptor(
            "dtg.graph.labelPropagation",
            GraphModel::Snapshot,
            vec![optional(
                "maxIterations",
                Integer,
                AlgorithmValue::Integer(20),
            )],
            vec![vertex(), required("label", Vertex)],
        ),
        approximate_descriptor(
            "dtg.graph.louvain",
            GraphModel::Snapshot,
            vec![
                optional("maxLevels", Integer, AlgorithmValue::Integer(10)),
                optional("maxIterations", Integer, AlgorithmValue::Integer(20)),
                optional(
                    "resolution",
                    Float,
                    AlgorithmValue::FloatBits(1.0_f64.to_bits()),
                ),
            ],
            vec![vertex(), required("communityId", Vertex)],
        ),
        descriptor(
            "dtg.temporal.earliestArrival",
            GraphModel::Event,
            {
                let mut inputs = window();
                inputs.push(optional(
                    "timeOrder",
                    Text,
                    AlgorithmValue::String("NON_DECREASING".into()),
                ));
                inputs.push(optional("waiting", Boolean, AlgorithmValue::Boolean(true)));
                inputs
            },
            vec![
                vertex(),
                required("arrivalTime", Time),
                nullable("predecessor", Vertex),
            ],
        ),
        descriptor(
            "dtg.temporal.reachability",
            GraphModel::Event,
            window(),
            vec![vertex(), required("reachable", Boolean)],
        ),
        descriptor(
            "dtg.temporal.minHop",
            GraphModel::Event,
            window(),
            vec![vertex(), required("hops", Integer)],
        ),
        descriptor(
            "dtg.temporal.latestDeparture",
            GraphModel::Event,
            vec![required("destination", Vertex), required("deadline", Time)],
            vec![vertex(), required("latestDeparture", Time)],
        ),
        descriptor(
            "dtg.temporal.fastestPath",
            GraphModel::Event,
            window(),
            vec![
                vertex(),
                required("travelTime", Integer),
                required("arrivalTime", Time),
            ],
        ),
        descriptor(
            "dtg.temporal.degree",
            GraphModel::Event,
            vec![required("validFrom", Time), required("validTo", Time)],
            vec![
                vertex(),
                required("inDegree", Integer),
                required("outDegree", Integer),
                required("degree", Integer),
            ],
        ),
        descriptor(
            "dtg.temporal.closeness",
            GraphModel::Event,
            window(),
            vec![vertex(), required("score", Float)],
        ),
        descriptor(
            "dtg.temporal.betweenness",
            GraphModel::Event,
            vec![required("validFrom", Time), required("validTo", Time)],
            vec![vertex(), required("score", Float)],
        ),
        descriptor(
            "dtg.temporal.pageRank",
            GraphModel::Event,
            vec![
                required("validFrom", Time),
                required("validTo", Time),
                optional(
                    "damping",
                    Float,
                    AlgorithmValue::FloatBits(0.85_f64.to_bits()),
                ),
                optional("maxIterations", Integer, AlgorithmValue::Integer(100)),
                optional(
                    "tolerance",
                    Float,
                    AlgorithmValue::FloatBits(1e-9_f64.to_bits()),
                ),
            ],
            vec![vertex(), required("score", Float)],
        ),
        descriptor(
            "dtg.temporal.burstiness",
            GraphModel::Event,
            vec![required("validFrom", Time), required("validTo", Time)],
            vec![vertex(), required("score", Float)],
        ),
        descriptor(
            "dtg.temporal.clusteringCoefficient",
            GraphModel::Event,
            vec![required("validFrom", Time), required("validTo", Time)],
            vec![vertex(), required("coefficient", Float)],
        ),
        descriptor(
            "dtg.temporal.topologicalOverlap",
            GraphModel::Event,
            vec![
                required("firstFrom", Time),
                required("firstTo", Time),
                required("secondFrom", Time),
                required("secondTo", Time),
            ],
            vec![vertex(), required("score", Float)],
        ),
        descriptor(
            "dtg.temporal.windowedComponents",
            GraphModel::Event,
            vec![required("validFrom", Time), required("validTo", Time)],
            vec![vertex(), required("componentId", Vertex)],
        ),
        descriptor(
            "dtg.temporal.windowedTriangleCount",
            GraphModel::Event,
            vec![required("validFrom", Time), required("validTo", Time)],
            vec![required("triangleCount", Integer)],
        ),
        descriptor(
            "dtg.temporal.changePoint",
            GraphModel::Event,
            vec![
                required("firstFrom", Time),
                required("firstTo", Time),
                required("secondFrom", Time),
                required("secondTo", Time),
            ],
            vec![vertex(), required("score", Float)],
        ),
        descriptor(
            "dtg.temporal.motifCount",
            GraphModel::Event,
            vec![
                required("validFrom", Time),
                required("validTo", Time),
                optional("deltaMicros", Integer, AlgorithmValue::Integer(1_000_000)),
            ],
            vec![required("motif", Text), required("count", Integer)],
        ),
        descriptor(
            "dtg.temporal.intervalComponents",
            GraphModel::Interval,
            Vec::new(),
            vec![vertex(), required("componentId", Vertex)],
        ),
        descriptor(
            "dtg.temporal.deltaSummary",
            GraphModel::Delta,
            Vec::new(),
            vec![
                required("entityType", Text),
                required("change", Text),
                required("count", Integer),
            ],
        ),
    ]
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderDescriptor {
    name: String,
    version: String,
    distributed: bool,
    native_isolation: bool,
}

impl ProviderDescriptor {
    #[must_use]
    pub fn new(
        name: impl Into<String>,
        version: impl Into<String>,
        distributed: bool,
        native_isolation: bool,
    ) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            distributed,
            native_isolation,
        }
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn version(&self) -> &str {
        &self.version
    }

    #[must_use]
    pub const fn distributed(&self) -> bool {
        self.distributed
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProjectedGraph {
    Snapshot(SnapshotGraph),
    PartitionedSnapshot(PartitionedSnapshotGraph),
    Event(EventGraph),
    Interval(IntervalGraph),
    Delta(DeltaGraph),
}

impl ProjectedGraph {
    #[must_use]
    pub const fn model(&self) -> GraphModel {
        match self {
            Self::Snapshot(_) | Self::PartitionedSnapshot(_) => GraphModel::Snapshot,
            Self::Event(_) => GraphModel::Event,
            Self::Interval(_) => GraphModel::Interval,
            Self::Delta(_) => GraphModel::Delta,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AlgorithmValue {
    Null,
    Boolean(bool),
    Integer(i64),
    FloatBits(u64),
    String(String),
    Vertex(VertexId),
    Time(ValidTime),
}

#[derive(Clone, Debug)]
pub struct CancellationSignal(Arc<AtomicBool>);

impl CancellationSignal {
    #[must_use]
    pub fn new() -> Self {
        Self(Arc::new(AtomicBool::new(false)))
    }

    #[must_use]
    pub fn is_canceled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn from_flag(flag: Arc<AtomicBool>) -> Self {
        Self(flag)
    }
}

impl Default for CancellationSignal {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Debug)]
pub struct AlgorithmRequest {
    algorithm: String,
    graph: Arc<ProjectedGraph>,
    parameters: BTreeMap<String, AlgorithmValue>,
    cancellation: CancellationSignal,
}

impl AlgorithmRequest {
    pub fn new(
        algorithm: impl Into<String>,
        graph: ProjectedGraph,
        parameters: BTreeMap<String, AlgorithmValue>,
    ) -> Result<Self, ProviderError> {
        let algorithm = algorithm.into();
        if algorithm.is_empty() || algorithm.len() > 255 {
            return Err(ProviderError::new(
                "DTG-ANALYTICS-INVALID-ALGORITHM",
                "algorithm name must be non-empty and at most 255 bytes",
            ));
        }
        Ok(Self {
            algorithm,
            graph: Arc::new(graph),
            parameters,
            cancellation: CancellationSignal::new(),
        })
    }

    pub fn from_shared(
        algorithm: impl Into<String>,
        graph: Arc<ProjectedGraph>,
        parameters: BTreeMap<String, AlgorithmValue>,
    ) -> Result<Self, ProviderError> {
        let algorithm = algorithm.into();
        if algorithm.is_empty() || algorithm.len() > 255 {
            return Err(ProviderError::new(
                "DTG-ANALYTICS-INVALID-ALGORITHM",
                "algorithm name must be non-empty and at most 255 bytes",
            ));
        }
        Ok(Self {
            algorithm,
            graph,
            parameters,
            cancellation: CancellationSignal::new(),
        })
    }

    #[must_use]
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    #[must_use]
    pub fn graph(&self) -> &ProjectedGraph {
        self.graph.as_ref()
    }

    #[must_use]
    pub const fn parameters(&self) -> &BTreeMap<String, AlgorithmValue> {
        &self.parameters
    }

    #[must_use]
    pub const fn cancellation(&self) -> &CancellationSignal {
        &self.cancellation
    }

    #[must_use]
    pub fn with_cancellation(mut self, cancellation: CancellationSignal) -> Self {
        self.cancellation = cancellation;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AlgorithmResult {
    columns: Vec<String>,
    rows: Vec<Vec<AlgorithmValue>>,
    metadata: BTreeMap<String, AlgorithmValue>,
}

/// Opaque, versioned provider state used to resume a deterministic analytics
/// execution.  The ledger owns persistence and fencing; providers own the
/// payload semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderCheckpoint {
    api_version: u16,
    algorithm: String,
    completed_units: u64,
    payload: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderSlice {
    next_unit: u64,
    complete: bool,
    checkpoint_payload: Vec<u8>,
}

impl ProviderSlice {
    #[must_use]
    pub const fn new(next_unit: u64, complete: bool) -> Self {
        Self {
            next_unit,
            complete,
            checkpoint_payload: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_checkpoint_payload(mut self, checkpoint_payload: Vec<u8>) -> Self {
        self.checkpoint_payload = checkpoint_payload;
        self
    }

    #[must_use]
    pub const fn next_unit(&self) -> u64 {
        self.next_unit
    }

    #[must_use]
    pub const fn complete(&self) -> bool {
        self.complete
    }

    #[must_use]
    pub fn checkpoint_payload(&self) -> &[u8] {
        &self.checkpoint_payload
    }
}

impl ProviderCheckpoint {
    pub const CURRENT_API_VERSION: u16 = 1;

    pub fn new(
        algorithm: impl Into<String>,
        completed_units: u64,
        payload: Vec<u8>,
    ) -> Result<Self, ProviderError> {
        let algorithm = algorithm.into();
        if algorithm.is_empty() || algorithm.len() > 255 {
            return Err(ProviderError::new(
                "DTG-ANALYTICS-CHECKPOINT",
                "checkpoint algorithm name must be non-empty and at most 255 bytes",
            ));
        }
        if payload.len() > 16 * 1024 * 1024 {
            return Err(ProviderError::new(
                "DTG-ANALYTICS-CHECKPOINT",
                "checkpoint payload exceeds the 16 MiB provider limit",
            ));
        }
        Ok(Self {
            api_version: Self::CURRENT_API_VERSION,
            algorithm,
            completed_units,
            payload,
        })
    }

    #[must_use]
    pub const fn api_version(&self) -> u16 {
        self.api_version
    }

    #[must_use]
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }

    #[must_use]
    pub const fn completed_units(&self) -> u64 {
        self.completed_units
    }

    #[must_use]
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
}

impl AlgorithmResult {
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn new(
        columns: Vec<String>,
        rows: Vec<Vec<AlgorithmValue>>,
        metadata: BTreeMap<String, AlgorithmValue>,
    ) -> Result<Self, ProviderError> {
        if columns.is_empty()
            || rows.iter().any(|row| row.len() != columns.len())
            || columns.iter().any(String::is_empty)
        {
            return Err(ProviderError::new(
                "DTG-ANALYTICS-INVALID-RESULT",
                "algorithm result schema and row widths must agree",
            ));
        }
        Ok(Self {
            columns,
            rows,
            metadata,
        })
    }

    #[must_use]
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    #[must_use]
    pub fn rows(&self) -> &[Vec<AlgorithmValue>] {
        &self.rows
    }

    pub fn append(&mut self, slice: AlgorithmResult) -> Result<(), ProviderError> {
        if self.columns.is_empty() {
            self.columns = slice.columns;
        } else if self.columns != slice.columns {
            return Err(ProviderError::new(
                "DTG-ANALYTICS-INVALID-RESULT",
                "execution slice schemas do not agree",
            ));
        }
        self.rows.extend(slice.rows);
        self.metadata.extend(slice.metadata);
        Ok(())
    }

    #[must_use]
    pub const fn metadata(&self) -> &BTreeMap<String, AlgorithmValue> {
        &self.metadata
    }
}

pub trait AnalyticsOutput {
    fn declare_columns(&mut self, columns: Vec<String>) -> Result<(), ProviderError>;

    fn push_row(&mut self, row: Vec<AlgorithmValue>) -> Result<(), ProviderError>;
}

impl AnalyticsOutput for AlgorithmResult {
    fn declare_columns(&mut self, columns: Vec<String>) -> Result<(), ProviderError> {
        if !self.columns.is_empty() || columns.is_empty() || columns.iter().any(String::is_empty) {
            return Err(ProviderError::new(
                "DTG-ANALYTICS-INVALID-RESULT",
                "algorithm result columns must be declared exactly once",
            ));
        }
        self.columns = columns;
        Ok(())
    }

    fn push_row(&mut self, row: Vec<AlgorithmValue>) -> Result<(), ProviderError> {
        if self.columns.is_empty() || row.len() != self.columns.len() {
            return Err(ProviderError::new(
                "DTG-ANALYTICS-INVALID-RESULT",
                "algorithm result schema and row widths must agree",
            ));
        }
        self.rows.push(row);
        Ok(())
    }
}

pub trait AnalyticsProvider: Send + Sync {
    fn descriptor(&self) -> ProviderDescriptor;

    fn algorithms(&self) -> Vec<AlgorithmDescriptor>;

    fn execute_into(
        &self,
        request: AlgorithmRequest,
        output: &mut dyn AnalyticsOutput,
    ) -> Result<(), ProviderError>;

    fn checkpoint(
        &self,
        _request: &AlgorithmRequest,
        _completed_units: u64,
    ) -> Result<ProviderCheckpoint, ProviderError> {
        Err(ProviderError::new(
            "DTG-ANALYTICS-CHECKPOINT-UNSUPPORTED",
            "provider does not support resumable execution for this algorithm",
        ))
    }

    fn restore_checkpoint(
        &self,
        _request: &AlgorithmRequest,
        _checkpoint: &ProviderCheckpoint,
    ) -> Result<u64, ProviderError> {
        Err(ProviderError::new(
            "DTG-ANALYTICS-CHECKPOINT-UNSUPPORTED",
            "provider does not support checkpoint restore for this algorithm",
        ))
    }

    fn execute_slice(
        &self,
        _request: &AlgorithmRequest,
        _start_unit: u64,
        _max_units: u64,
        _output: &mut dyn AnalyticsOutput,
    ) -> Result<ProviderSlice, ProviderError> {
        Err(ProviderError::new(
            "DTG-ANALYTICS-SLICES-UNSUPPORTED",
            "provider does not support deterministic execution slices for this algorithm",
        ))
    }

    fn execute_slice_from_checkpoint(
        &self,
        request: &AlgorithmRequest,
        checkpoint: &ProviderCheckpoint,
        max_units: u64,
        output: &mut dyn AnalyticsOutput,
    ) -> Result<ProviderSlice, ProviderError> {
        let start_unit = self.restore_checkpoint(request, checkpoint)?;
        self.execute_slice(request, start_unit, max_units, output)
    }

    fn execute(&self, request: AlgorithmRequest) -> Result<AlgorithmResult, ProviderError> {
        let mut result = AlgorithmResult {
            columns: Vec::new(),
            rows: Vec::new(),
            metadata: BTreeMap::new(),
        };
        self.execute_into(request, &mut result)?;
        if result.columns.is_empty() {
            return Err(ProviderError::new(
                "DTG-ANALYTICS-INVALID-RESULT",
                "algorithm provider did not declare result columns",
            ));
        }
        Ok(result)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProviderError {
    code: String,
    message: String,
}

impl ProviderError {
    #[must_use]
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }

    #[must_use]
    pub fn code(&self) -> &str {
        &self.code
    }

    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl Display for ProviderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl Error for ProviderError {}
