use std::collections::{BTreeMap, BTreeSet};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use dtg_kernel::{
    BackendGeneration, PlacementEpoch, ShardId, TransactionId, TransactionTime, Value, Version,
};
use dtg_storage::{EdgeId, VertexId};

#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ShardSnapshotProvenance {
    pub placement_epoch: PlacementEpoch,
    pub backend_generation: BackendGeneration,
    pub applied_index: u64,
    pub closed_time: TransactionTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotProvenance {
    transaction_id: TransactionId,
    start_time: TransactionTime,
    catalog_version: Version,
    shards: BTreeMap<ShardId, ShardSnapshotProvenance>,
}

impl SnapshotProvenance {
    pub fn new(
        transaction_id: TransactionId,
        start_time: TransactionTime,
        catalog_version: Version,
        shard_fences: Vec<(ShardId, ShardSnapshotProvenance)>,
    ) -> Result<Self, ProjectionError> {
        if catalog_version.get() == 0 || shard_fences.is_empty() {
            return Err(ProjectionError::InvalidSnapshot);
        }

        let mut shards = BTreeMap::new();
        for (shard_id, fence) in shard_fences {
            if fence.closed_time < start_time {
                return Err(ProjectionError::InvalidSnapshot);
            }
            if shards.insert(shard_id, fence).is_some() {
                return Err(ProjectionError::InvalidSnapshot);
            }
        }

        Ok(Self {
            transaction_id,
            start_time,
            catalog_version,
            shards,
        })
    }

    pub const fn transaction_id(&self) -> TransactionId {
        self.transaction_id
    }

    pub const fn start_time(&self) -> TransactionTime {
        self.start_time
    }

    pub const fn catalog_version(&self) -> Version {
        self.catalog_version
    }

    pub const fn shards(&self) -> &BTreeMap<ShardId, ShardSnapshotProvenance> {
        &self.shards
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PartitionProvenance {
    pub shard_id: ShardId,
    pub placement_epoch: PlacementEpoch,
    pub backend_generation: BackendGeneration,
    pub applied_index: u64,
    pub partition_index: u32,
    pub partition_count: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedVertex {
    pub id: VertexId,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectedEdge {
    pub id: EdgeId,
    pub source: VertexId,
    pub target: VertexId,
    pub properties: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ShardProjectionPart {
    pub snapshot: SnapshotProvenance,
    pub provenance: PartitionProvenance,
    pub vertices: Vec<ProjectedVertex>,
    pub edges: Vec<ProjectedEdge>,
}

pub trait SnapshotProjectionInput {
    fn snapshot_provenance(&self) -> &SnapshotProvenance;
    fn partition_provenance(&self) -> PartitionProvenance;
    fn vertices(&self) -> &[ProjectedVertex];
    fn edges(&self) -> &[ProjectedEdge];
}

impl SnapshotProjectionInput for ShardProjectionPart {
    fn snapshot_provenance(&self) -> &SnapshotProvenance {
        &self.snapshot
    }

    fn partition_provenance(&self) -> PartitionProvenance {
        self.provenance
    }

    fn vertices(&self) -> &[ProjectedVertex] {
        &self.vertices
    }

    fn edges(&self) -> &[ProjectedEdge] {
        &self.edges
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PropertyType {
    Boolean,
    Integer,
    Float,
    Bytes,
    String,
    List,
    Map,
}

impl PropertyType {
    pub(crate) const fn matches(self, value: &Value) -> bool {
        matches!(value, Value::Null)
            || matches!(
                (self, value),
                (Self::Boolean, Value::Boolean(_))
                    | (Self::Integer, Value::Integer(_))
                    | (Self::Float, Value::FloatBits(_))
                    | (Self::Bytes, Value::Bytes(_))
                    | (Self::String, Value::String(_))
                    | (Self::List, Value::List(_))
                    | (Self::Map, Value::Map(_))
            )
    }

    pub(crate) const fn tag(self) -> u8 {
        match self {
            Self::Boolean => 1,
            Self::Integer => 2,
            Self::Float => 3,
            Self::Bytes => 4,
            Self::String => 5,
            Self::List => 6,
            Self::Map => 7,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PropertyColumnSpec {
    name: String,
    property_type: PropertyType,
    required: bool,
}

impl PropertyColumnSpec {
    pub fn required(name: impl Into<String>, property_type: PropertyType) -> Self {
        Self {
            name: name.into(),
            property_type,
            required: true,
        }
    }

    pub fn optional(name: impl Into<String>, property_type: PropertyType) -> Self {
        Self {
            name: name.into(),
            property_type,
            required: false,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub const fn property_type(&self) -> PropertyType {
        self.property_type
    }

    pub const fn is_required(&self) -> bool {
        self.required
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionSpec {
    include_reverse: bool,
    vertex_properties: Vec<PropertyColumnSpec>,
    edge_properties: Vec<PropertyColumnSpec>,
}

impl ProjectionSpec {
    pub fn new(
        include_reverse: bool,
        vertex_properties: Vec<PropertyColumnSpec>,
        edge_properties: Vec<PropertyColumnSpec>,
    ) -> Result<Self, ProjectionError> {
        validate_specs(&vertex_properties)?;
        validate_specs(&edge_properties)?;
        Ok(Self {
            include_reverse,
            vertex_properties,
            edge_properties,
        })
    }

    pub const fn include_reverse(&self) -> bool {
        self.include_reverse
    }

    pub fn vertex_properties(&self) -> &[PropertyColumnSpec] {
        &self.vertex_properties
    }

    pub fn edge_properties(&self) -> &[PropertyColumnSpec] {
        &self.edge_properties
    }
}

fn validate_specs(specs: &[PropertyColumnSpec]) -> Result<(), ProjectionError> {
    let mut names = BTreeSet::new();
    for spec in specs {
        if spec.name.is_empty() || !names.insert(spec.name.as_str()) {
            return Err(ProjectionError::InvalidSpec);
        }
    }
    Ok(())
}

#[derive(Clone, Debug)]
pub struct ProjectionBudget {
    max_memory_bytes: usize,
    max_spill_bytes: usize,
    cancellation: CancellationToken,
}

impl ProjectionBudget {
    pub const fn new(
        max_memory_bytes: usize,
        max_spill_bytes: usize,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            max_memory_bytes,
            max_spill_bytes,
            cancellation,
        }
    }

    pub(crate) const fn max_memory_bytes(&self) -> usize {
        self.max_memory_bytes
    }

    pub(crate) const fn max_spill_bytes(&self) -> usize {
        self.max_spill_bytes
    }

    pub(crate) const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetUsage {
    pub memory_bytes: usize,
    pub spill_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionError {
    InvalidSnapshot,
    SnapshotMismatch,
    PartitionFenceMismatch,
    DuplicatePartition,
    PartitionGap,
    DuplicateVertex,
    DuplicateEdge,
    DanglingEdge,
    InvalidSpec,
    MissingRequiredProperty,
    PropertyTypeMismatch,
    ResourceLimit,
    Cancelled,
}

impl ProjectionError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidSnapshot => "DTG-ANALYTICS-SNAPSHOT-INVALID",
            Self::SnapshotMismatch => "DTG-ANALYTICS-SNAPSHOT-MISMATCH",
            Self::PartitionFenceMismatch => "DTG-ANALYTICS-PARTITION-FENCE",
            Self::DuplicatePartition => "DTG-ANALYTICS-PARTITION-DUPLICATE",
            Self::PartitionGap => "DTG-ANALYTICS-PARTITION-GAP",
            Self::DuplicateVertex => "DTG-ANALYTICS-VERTEX-DUPLICATE",
            Self::DuplicateEdge => "DTG-ANALYTICS-EDGE-DUPLICATE",
            Self::DanglingEdge => "DTG-ANALYTICS-EDGE-DANGLING",
            Self::InvalidSpec => "DTG-ANALYTICS-PROJECTION-SPEC",
            Self::MissingRequiredProperty => "DTG-ANALYTICS-PROPERTY-MISSING",
            Self::PropertyTypeMismatch => "DTG-ANALYTICS-PROPERTY-TYPE",
            Self::ResourceLimit => "DTG-ANALYTICS-PROJECTION-RESOURCE",
            Self::Cancelled => "DTG-ANALYTICS-CANCELLED",
        }
    }
}

impl std::fmt::Display for ProjectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ProjectionError {}
