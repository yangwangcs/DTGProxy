#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use dtg_storage::{EdgeVersion, ReplicaBinding, TransactionTime, VertexId};

const DEFAULT_MAX_OVERLAY_ENTRIES: usize = 4_096;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CsrDirection {
    Outgoing,
    Incoming,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SnapshotCsrKey {
    cluster_id: u64,
    graph_id: u64,
    shard_id: u64,
    placement_epoch: u64,
    backend_generation: u64,
    applied_index: u64,
    transaction_time: TransactionTime,
    valid_at: i64,
    direction: CsrDirection,
    schema_version: u32,
    mapping_generation: u64,
}

impl SnapshotCsrKey {
    pub fn new(
        binding: ReplicaBinding,
        applied_index: u64,
        transaction_time: TransactionTime,
        valid_at: i64,
        direction: CsrDirection,
    ) -> Self {
        Self {
            cluster_id: binding.cluster_id().get(),
            graph_id: binding.graph_id().get(),
            shard_id: binding.shard_id().get(),
            placement_epoch: binding.placement_epoch().get(),
            backend_generation: binding.backend_generation().get(),
            applied_index,
            transaction_time,
            valid_at,
            direction,
            schema_version: binding.layout_version(),
            mapping_generation: binding.contract_version().into(),
        }
    }

    pub const fn applied_index(&self) -> u64 {
        self.applied_index
    }

    pub const fn transaction_time(&self) -> TransactionTime {
        self.transaction_time
    }

    pub const fn valid_at(&self) -> i64 {
        self.valid_at
    }

    pub const fn direction(&self) -> CsrDirection {
        self.direction
    }

    pub fn with_applied_index(&self, applied_index: u64) -> Self {
        Self {
            applied_index,
            ..self.clone()
        }
    }

    pub fn same_lineage(&self, other: &Self) -> bool {
        self.cluster_id == other.cluster_id
            && self.graph_id == other.graph_id
            && self.shard_id == other.shard_id
            && self.placement_epoch == other.placement_epoch
            && self.backend_generation == other.backend_generation
            && self.transaction_time == other.transaction_time
            && self.valid_at == other.valid_at
            && self.direction == other.direction
            && self.schema_version == other.schema_version
            && self.mapping_generation == other.mapping_generation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SnapshotCsrBuildBudget {
    max_retained_bytes: usize,
}

impl SnapshotCsrBuildBudget {
    pub const fn new(max_retained_bytes: usize) -> Self {
        Self { max_retained_bytes }
    }

    pub const fn max_retained_bytes(self) -> usize {
        self.max_retained_bytes
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotCsrError {
    InsufficientMemory { requested: usize, available: usize },
    DuplicateEdgeId(u128),
    LocalIdSpaceExceeded { vertices: usize },
    SizeOverflow,
    MissingVertex(VertexId),
    InvalidOverlay(&'static str),
    NonContiguousOverlay { expected: u64, actual: u64 },
    OverlayEntryLimitExceeded { entries: usize, available: usize },
}

impl SnapshotCsrError {
    pub const fn is_insufficient_memory(&self) -> bool {
        matches!(self, Self::InsufficientMemory { .. })
    }
}

impl fmt::Display for SnapshotCsrError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InsufficientMemory {
                requested,
                available,
            } => {
                write!(
                    formatter,
                    "CSR needs {requested} bytes but only {available} are admitted"
                )
            }
            Self::DuplicateEdgeId(id) => write!(formatter, "duplicate CSR edge identity {id}"),
            Self::LocalIdSpaceExceeded { vertices } => {
                write!(
                    formatter,
                    "CSR local vertex id space exceeded by {vertices} vertices"
                )
            }
            Self::SizeOverflow => formatter.write_str("CSR size estimate overflowed"),
            Self::MissingVertex(id) => {
                write!(formatter, "CSR dictionary omits vertex {}", id.get())
            }
            Self::InvalidOverlay(reason) => write!(formatter, "invalid CSR overlay: {reason}"),
            Self::NonContiguousOverlay { expected, actual } => {
                write!(
                    formatter,
                    "CSR overlay expected index {expected} but received {actual}"
                )
            }
            Self::OverlayEntryLimitExceeded { entries, available } => {
                write!(
                    formatter,
                    "CSR overlay retains {entries} entries but only {available} are admitted"
                )
            }
        }
    }
}

impl std::error::Error for SnapshotCsrError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct AdjacencyEntry {
    neighbor: u32,
    edge_ref: u32,
}

#[derive(Debug)]
pub struct SnapshotCsr {
    key: SnapshotCsrKey,
    vertex_ids: Vec<VertexId>,
    offsets: Vec<u32>,
    adjacency: Vec<AdjacencyEntry>,
    edges: Vec<EdgeVersion>,
    retained_bytes: usize,
}

impl SnapshotCsr {
    pub fn build(
        key: SnapshotCsrKey,
        mut edges: Vec<EdgeVersion>,
        budget: SnapshotCsrBuildBudget,
    ) -> Result<Self, SnapshotCsrError> {
        edges.sort_by_key(|edge| edge.id());
        for pair in edges.windows(2) {
            if pair[0].id() == pair[1].id() {
                return Err(SnapshotCsrError::DuplicateEdgeId(pair[0].id().get()));
            }
        }

        let vertex_ids = edges
            .iter()
            .flat_map(|edge| [edge.source(), edge.target()])
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if vertex_ids.len() > u32::MAX as usize {
            return Err(SnapshotCsrError::LocalIdSpaceExceeded {
                vertices: vertex_ids.len(),
            });
        }
        if edges.len() > u32::MAX as usize {
            return Err(SnapshotCsrError::SizeOverflow);
        }
        let retained_bytes = estimate_retained_bytes(&vertex_ids, &edges)?;
        if retained_bytes > budget.max_retained_bytes() {
            return Err(SnapshotCsrError::InsufficientMemory {
                requested: retained_bytes,
                available: budget.max_retained_bytes(),
            });
        }

        let dictionary = vertex_ids
            .iter()
            .copied()
            .enumerate()
            .map(|(index, id)| (id, u32::try_from(index).unwrap_or(u32::MAX)))
            .collect::<BTreeMap<_, _>>();
        let mut entries = edges
            .iter()
            .enumerate()
            .map(|(edge_ref, edge)| {
                let (anchor, neighbor) = match key.direction() {
                    CsrDirection::Outgoing => (edge.source(), edge.target()),
                    CsrDirection::Incoming => (edge.target(), edge.source()),
                };
                Ok((
                    *dictionary
                        .get(&anchor)
                        .ok_or(SnapshotCsrError::MissingVertex(anchor))?,
                    AdjacencyEntry {
                        neighbor: *dictionary
                            .get(&neighbor)
                            .ok_or(SnapshotCsrError::MissingVertex(neighbor))?,
                        edge_ref: u32::try_from(edge_ref)
                            .map_err(|_| SnapshotCsrError::SizeOverflow)?,
                    },
                ))
            })
            .collect::<Result<Vec<_>, SnapshotCsrError>>()?;
        entries.sort_by_key(|(anchor, entry)| (*anchor, entry.neighbor, entry.edge_ref));

        let mut offsets = vec![0_u32; vertex_ids.len().saturating_add(1)];
        for (anchor, _) in &entries {
            let next = offsets[*anchor as usize + 1]
                .checked_add(1)
                .ok_or(SnapshotCsrError::SizeOverflow)?;
            offsets[*anchor as usize + 1] = next;
        }
        for index in 1..offsets.len() {
            offsets[index] = offsets[index]
                .checked_add(offsets[index - 1])
                .ok_or(SnapshotCsrError::SizeOverflow)?;
        }
        let adjacency = entries.into_iter().map(|(_, entry)| entry).collect();
        Ok(Self {
            key,
            vertex_ids,
            offsets,
            adjacency,
            edges,
            retained_bytes,
        })
    }

    pub const fn key(&self) -> &SnapshotCsrKey {
        &self.key
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn neighbors(&self, vertex: VertexId) -> Result<CsrNeighbors<'_>, SnapshotCsrError> {
        let local = self
            .vertex_ids
            .binary_search(&vertex)
            .map_err(|_| SnapshotCsrError::MissingVertex(vertex))?;
        let start = self.offsets[local] as usize;
        let end = self.offsets[local + 1] as usize;
        Ok(CsrNeighbors {
            csr: self,
            next: start,
            end,
        })
    }

    fn edge_by_id(&self, edge_id: dtg_storage::EdgeId) -> Option<&EdgeVersion> {
        self.edges
            .binary_search_by_key(&edge_id, |edge| edge.id())
            .ok()
            .map(|index| &self.edges[index])
    }
}

#[derive(Debug)]
pub struct CsrNeighbor<'a> {
    vertex: VertexId,
    edge: &'a EdgeVersion,
}

impl CsrNeighbor<'_> {
    pub const fn vertex(&self) -> VertexId {
        self.vertex
    }

    pub const fn edge(&self) -> &EdgeVersion {
        self.edge
    }
}

#[derive(Debug)]
pub struct CsrNeighbors<'a> {
    csr: &'a SnapshotCsr,
    next: usize,
    end: usize,
}

impl<'a> Iterator for CsrNeighbors<'a> {
    type Item = CsrNeighbor<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next == self.end {
            return None;
        }
        let entry = self.csr.adjacency[self.next];
        self.next += 1;
        Some(CsrNeighbor {
            vertex: self.csr.vertex_ids[entry.neighbor as usize],
            edge: &self.csr.edges[entry.edge_ref as usize],
        })
    }
}

fn estimate_retained_bytes(
    vertices: &[VertexId],
    edges: &[EdgeVersion],
) -> Result<usize, SnapshotCsrError> {
    let edge_payload_bytes = edges.iter().try_fold(0_usize, |total, edge| {
        let properties = estimated_properties_bytes(edge.properties())?;
        total
            .checked_add(96)
            .and_then(|value| value.checked_add(edge.edge_type().len()))
            .and_then(|value| value.checked_add(properties))
            .ok_or(SnapshotCsrError::SizeOverflow)
    })?;
    vertices
        .len()
        .checked_mul(std::mem::size_of::<VertexId>())
        .and_then(|value| value.checked_add(vertices.len().saturating_add(1) * 4))
        .and_then(|value| value.checked_add(edges.len().saturating_mul(8)))
        .and_then(|value| value.checked_add(edge_payload_bytes))
        .ok_or(SnapshotCsrError::SizeOverflow)
}

fn estimated_properties_bytes(
    properties: &dtg_storage::Properties,
) -> Result<usize, SnapshotCsrError> {
    properties.iter().try_fold(0_usize, |total, (name, value)| {
        total
            .checked_add(name.len())
            .and_then(|bytes| bytes.checked_add(estimated_value_bytes(value).ok()?))
            .ok_or(SnapshotCsrError::SizeOverflow)
    })
}

fn estimated_value_bytes(value: &dtg_storage::Value) -> Result<usize, SnapshotCsrError> {
    match value {
        dtg_storage::Value::Null => Ok(1),
        dtg_storage::Value::Boolean(_) => Ok(2),
        dtg_storage::Value::Integer(_) | dtg_storage::Value::FloatBits(_) => Ok(9),
        dtg_storage::Value::Bytes(value) => 9_usize
            .checked_add(value.len())
            .ok_or(SnapshotCsrError::SizeOverflow),
        dtg_storage::Value::String(value) => 9_usize
            .checked_add(value.len())
            .ok_or(SnapshotCsrError::SizeOverflow),
        dtg_storage::Value::List(values) => values.iter().try_fold(9_usize, |total, value| {
            total
                .checked_add(estimated_value_bytes(value)?)
                .ok_or(SnapshotCsrError::SizeOverflow)
        }),
        dtg_storage::Value::Map(values) => {
            values.iter().try_fold(9_usize, |total, (name, value)| {
                total
                    .checked_add(name.len())
                    .and_then(|bytes| bytes.checked_add(estimated_value_bytes(value).ok()?))
                    .ok_or(SnapshotCsrError::SizeOverflow)
            })
        }
    }
}

#[derive(Clone, Debug)]
pub enum CommittedAdjacencyOperation {
    Add(EdgeVersion),
    Remove(dtg_storage::EdgeTombstone),
}

impl CommittedAdjacencyOperation {
    fn edge_id(&self) -> dtg_storage::EdgeId {
        match self {
            Self::Add(edge) => edge.id(),
            Self::Remove(tombstone) => tombstone.id(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct CommittedGraphDelta {
    key: SnapshotCsrKey,
    operations: Vec<CommittedAdjacencyOperation>,
}

impl CommittedGraphDelta {
    pub fn new(
        key: SnapshotCsrKey,
        operations: Vec<CommittedAdjacencyOperation>,
    ) -> Result<Self, SnapshotCsrError> {
        let mut edge_ids = BTreeSet::new();
        for operation in &operations {
            if !edge_ids.insert(operation.edge_id()) {
                return Err(SnapshotCsrError::InvalidOverlay(
                    "committed delta repeats an edge identity",
                ));
            }
        }
        Ok(Self { key, operations })
    }
}

pub struct CommittedCsrOverlay {
    base: Arc<SnapshotCsr>,
    covered_through: u64,
    additions: BTreeMap<VertexId, BTreeMap<dtg_storage::EdgeId, EdgeVersion>>,
    removals: BTreeMap<dtg_storage::EdgeId, dtg_storage::EdgeTombstone>,
    retained_bytes: usize,
    max_retained_bytes: usize,
    max_entries: usize,
    valid: bool,
}

impl CommittedCsrOverlay {
    pub fn new(
        base: Arc<SnapshotCsr>,
        max_retained_bytes: usize,
    ) -> Result<Self, SnapshotCsrError> {
        Self::with_limits(base, max_retained_bytes, DEFAULT_MAX_OVERLAY_ENTRIES)
    }

    pub fn with_limits(
        base: Arc<SnapshotCsr>,
        max_retained_bytes: usize,
        max_entries: usize,
    ) -> Result<Self, SnapshotCsrError> {
        if max_retained_bytes == 0 || max_entries == 0 {
            return Err(SnapshotCsrError::InvalidOverlay("overlay budget is zero"));
        }
        Ok(Self {
            covered_through: base.key().applied_index(),
            base,
            additions: BTreeMap::new(),
            removals: BTreeMap::new(),
            retained_bytes: 0,
            max_retained_bytes,
            max_entries,
            valid: true,
        })
    }

    pub fn apply(&mut self, delta: CommittedGraphDelta) -> Result<(), SnapshotCsrError> {
        let expected = self.covered_through.saturating_add(1);
        if !self.valid {
            return Err(SnapshotCsrError::InvalidOverlay("overlay is invalidated"));
        }
        if !self.base.key().same_lineage(&delta.key) {
            self.invalidate();
            return Err(SnapshotCsrError::InvalidOverlay(
                "committed delta snapshot identity differs from the base",
            ));
        }
        if delta.key.applied_index() != expected {
            self.invalidate();
            return Err(SnapshotCsrError::NonContiguousOverlay {
                expected,
                actual: delta.key.applied_index(),
            });
        }
        let mut additions = self.additions.clone();
        let mut removals = self.removals.clone();
        for operation in delta.operations {
            match operation {
                CommittedAdjacencyOperation::Add(edge) => {
                    let anchor = match self.base.key().direction() {
                        CsrDirection::Outgoing => edge.source(),
                        CsrDirection::Incoming => edge.target(),
                    };
                    if removals
                        .get(&edge.id())
                        .is_some_and(|tombstone| tombstone_is_at_least_as_new(tombstone, &edge))
                    {
                        continue;
                    }
                    removals.remove(&edge.id());
                    if additions
                        .values()
                        .filter_map(|entries| entries.get(&edge.id()))
                        .any(|existing| edge_is_at_least_as_new(existing, &edge))
                        || self
                            .base
                            .edge_by_id(edge.id())
                            .is_some_and(|existing| edge_is_at_least_as_new(existing, &edge))
                    {
                        continue;
                    }
                    for additions_at_vertex in additions.values_mut() {
                        additions_at_vertex.remove(&edge.id());
                    }
                    additions.retain(|_, additions_at_vertex| !additions_at_vertex.is_empty());
                    additions.entry(anchor).or_default().insert(edge.id(), edge);
                }
                CommittedAdjacencyOperation::Remove(tombstone) => {
                    let edge_id = tombstone.id();
                    for additions_at_vertex in additions.values_mut() {
                        if additions_at_vertex
                            .get(&edge_id)
                            .is_some_and(|edge| tombstone_is_at_least_as_new(&tombstone, edge))
                        {
                            additions_at_vertex.remove(&edge_id);
                        }
                    }
                    additions.retain(|_, additions_at_vertex| !additions_at_vertex.is_empty());
                    if self
                        .base
                        .edge_by_id(edge_id)
                        .is_some_and(|edge| tombstone_is_at_least_as_new(&tombstone, edge))
                    {
                        let should_replace = removals.get(&edge_id).is_none_or(|existing| {
                            tombstone_is_at_least_as_new_tombstone(&tombstone, existing)
                        });
                        if should_replace {
                            removals.insert(edge_id, tombstone);
                        }
                    }
                }
            }
        }
        let retained_bytes = estimate_overlay_retained_bytes(&additions, &removals)?;
        let entries = overlay_entry_count(&additions, &removals)?;
        if entries > self.max_entries {
            self.invalidate();
            return Err(SnapshotCsrError::OverlayEntryLimitExceeded {
                entries,
                available: self.max_entries,
            });
        }
        if retained_bytes > self.max_retained_bytes {
            self.invalidate();
            return Err(SnapshotCsrError::InsufficientMemory {
                requested: retained_bytes,
                available: self.max_retained_bytes,
            });
        }
        self.additions = additions;
        self.removals = removals;
        self.retained_bytes = retained_bytes;
        self.covered_through = expected;
        Ok(())
    }

    pub fn neighbors(
        &self,
        vertex: VertexId,
        applied_index: u64,
    ) -> Result<Vec<EdgeVersion>, SnapshotCsrError> {
        if !self.valid {
            return Err(SnapshotCsrError::InvalidOverlay("overlay is invalidated"));
        }
        if applied_index != self.covered_through {
            return Err(SnapshotCsrError::InvalidOverlay(
                "overlay does not exactly cover the requested index",
            ));
        }
        let mut edges = match self.base.neighbors(vertex) {
            Ok(neighbors) => neighbors
                .filter(|neighbor| {
                    !self
                        .removals
                        .get(&neighbor.edge().id())
                        .is_some_and(|tombstone| {
                            tombstone_is_at_least_as_new(tombstone, neighbor.edge())
                        })
                        && !self
                            .additions
                            .values()
                            .any(|additions| additions.contains_key(&neighbor.edge().id()))
                })
                .map(|neighbor| (neighbor.edge().id(), neighbor.edge().clone()))
                .collect::<BTreeMap<_, _>>(),
            Err(SnapshotCsrError::MissingVertex(_)) => BTreeMap::new(),
            Err(error) => return Err(error),
        };
        if let Some(additions) = self.additions.get(&vertex) {
            for (id, edge) in additions {
                edges.insert(*id, edge.clone());
            }
        }
        Ok(edges.into_values().collect())
    }

    pub fn materialize(
        &self,
        key: SnapshotCsrKey,
        budget: SnapshotCsrBuildBudget,
    ) -> Result<SnapshotCsr, SnapshotCsrError> {
        if !self.valid {
            return Err(SnapshotCsrError::InvalidOverlay("overlay is invalidated"));
        }
        if !self.base.key().same_lineage(&key) || key.applied_index() != self.covered_through {
            return Err(SnapshotCsrError::InvalidOverlay(
                "overlay does not exactly cover the materialized snapshot",
            ));
        }
        let mut edges = self
            .base
            .edges
            .iter()
            .filter(|edge| {
                !self
                    .removals
                    .get(&edge.id())
                    .is_some_and(|tombstone| tombstone_is_at_least_as_new(tombstone, edge))
            })
            .map(|edge| (edge.id(), edge.clone()))
            .collect::<BTreeMap<_, _>>();
        for edge in self.additions.values().flat_map(|entries| entries.values()) {
            edges.insert(edge.id(), edge.clone());
        }
        SnapshotCsr::build(key, edges.into_values().collect(), budget)
    }

    pub const fn covered_through(&self) -> u64 {
        self.covered_through
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub const fn is_valid(&self) -> bool {
        self.valid
    }

    fn invalidate(&mut self) {
        self.valid = false;
        self.additions.clear();
        self.removals.clear();
        self.retained_bytes = 0;
    }
}

fn estimate_overlay_retained_bytes(
    additions: &BTreeMap<VertexId, BTreeMap<dtg_storage::EdgeId, EdgeVersion>>,
    removals: &BTreeMap<dtg_storage::EdgeId, dtg_storage::EdgeTombstone>,
) -> Result<usize, SnapshotCsrError> {
    let additions_bytes = additions
        .values()
        .flat_map(|entries| entries.values())
        .try_fold(0_usize, |total, edge| {
            let properties = estimated_properties_bytes(edge.properties())?;
            total
                .checked_add(96)
                .and_then(|value| value.checked_add(edge.edge_type().len()))
                .and_then(|value| value.checked_add(properties))
                .ok_or(SnapshotCsrError::SizeOverflow)
        })?;
    removals.iter().try_fold(additions_bytes, |total, _| {
        total.checked_add(16).ok_or(SnapshotCsrError::SizeOverflow)
    })
}

fn overlay_entry_count(
    additions: &BTreeMap<VertexId, BTreeMap<dtg_storage::EdgeId, EdgeVersion>>,
    removals: &BTreeMap<dtg_storage::EdgeId, dtg_storage::EdgeTombstone>,
) -> Result<usize, SnapshotCsrError> {
    additions.values().try_fold(removals.len(), |total, edges| {
        total
            .checked_add(edges.len())
            .ok_or(SnapshotCsrError::SizeOverflow)
    })
}

fn edge_is_at_least_as_new(left: &EdgeVersion, right: &EdgeVersion) -> bool {
    (left.transaction_time(), left.version()) >= (right.transaction_time(), right.version())
}

fn tombstone_is_at_least_as_new(
    tombstone: &dtg_storage::EdgeTombstone,
    edge: &EdgeVersion,
) -> bool {
    (tombstone.transaction_time(), tombstone.version()) >= (edge.transaction_time(), edge.version())
}

fn tombstone_is_at_least_as_new_tombstone(
    left: &dtg_storage::EdgeTombstone,
    right: &dtg_storage::EdgeTombstone,
) -> bool {
    (left.transaction_time(), left.version()) >= (right.transaction_time(), right.version())
}
