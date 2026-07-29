use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use blake3::Hasher;
use dtg_kernel::{Digest32, Value};
use dtg_storage::{EdgeId, VertexId};

use crate::projection::{
    BudgetUsage, PartitionProvenance, ProjectedEdge, ProjectedVertex, ProjectionBudget,
    ProjectionError, ProjectionSpec, PropertyColumnSpec, PropertyType, SnapshotProjectionInput,
    SnapshotProvenance,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TypedPropertyColumn {
    property_type: PropertyType,
    values: Vec<Option<Value>>,
}

impl TypedPropertyColumn {
    pub const fn property_type(&self) -> PropertyType {
        self.property_type
    }

    pub fn values(&self) -> &[Option<Value>] {
        &self.values
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReverseCsr {
    offsets: Vec<u64>,
    neighbors: Vec<u32>,
    forward_edge_indices: Vec<u32>,
}

impl ReverseCsr {
    pub fn offsets(&self) -> &[u64] {
        &self.offsets
    }

    pub fn neighbors(&self) -> &[u32] {
        &self.neighbors
    }

    pub fn forward_edge_indices(&self) -> &[u32] {
        &self.forward_edge_indices
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotCsr {
    snapshot: SnapshotProvenance,
    partition_provenance: Vec<PartitionProvenance>,
    vertex_ids: Vec<VertexId>,
    offsets: Vec<u64>,
    neighbors: Vec<u32>,
    edge_ids: Vec<EdgeId>,
    reverse: Option<ReverseCsr>,
    vertex_properties: BTreeMap<String, TypedPropertyColumn>,
    edge_properties: BTreeMap<String, TypedPropertyColumn>,
    digest: Digest32,
    budget_usage: BudgetUsage,
}

impl SnapshotCsr {
    pub fn assemble<P: SnapshotProjectionInput>(
        parts: Vec<P>,
        spec: ProjectionSpec,
        budget: ProjectionBudget,
    ) -> Result<Self, ProjectionError> {
        check_cancelled(&budget)?;
        let estimated_bytes = estimate_projection_bytes(&parts, &spec, &budget)?;
        let budget_usage = reserve_budget(estimated_bytes, &budget)?;
        let snapshot = validate_snapshots(&parts, &budget)?;
        let partition_provenance = validate_partitions(&parts, &snapshot, &budget)?;

        let mut vertices = BTreeMap::<VertexId, ProjectedVertex>::new();
        let mut edges = BTreeMap::<EdgeId, ProjectedEdge>::new();
        for part in &parts {
            check_cancelled(&budget)?;
            for vertex in part.vertices() {
                check_cancelled(&budget)?;
                if vertices.insert(vertex.id, vertex.clone()).is_some() {
                    return Err(ProjectionError::DuplicateVertex);
                }
            }
            for edge in part.edges() {
                check_cancelled(&budget)?;
                if edges.insert(edge.id, edge.clone()).is_some() {
                    return Err(ProjectionError::DuplicateEdge);
                }
            }
        }

        let mut vertex_ids = Vec::with_capacity(vertices.len());
        for vertex_id in vertices.keys().copied() {
            check_cancelled(&budget)?;
            vertex_ids.push(vertex_id);
        }
        if vertex_ids.len() > u32::MAX as usize {
            return Err(ProjectionError::ResourceLimit);
        }
        let mut vertex_indices = BTreeMap::new();
        for (index, vertex_id) in vertex_ids.iter().copied().enumerate() {
            check_cancelled(&budget)?;
            vertex_indices.insert(vertex_id, index as u32);
        }

        let mut ordered_edges = BTreeMap::new();
        for edge in edges.into_values() {
            check_cancelled(&budget)?;
            if !vertex_indices.contains_key(&edge.source)
                || !vertex_indices.contains_key(&edge.target)
            {
                return Err(ProjectionError::DanglingEdge);
            }
            ordered_edges.insert((edge.source, edge.target, edge.id), edge);
        }
        let mut canonical_edges = Vec::with_capacity(ordered_edges.len());
        for edge in ordered_edges.into_values() {
            check_cancelled(&budget)?;
            canonical_edges.push(edge);
        }
        if canonical_edges.len() > u32::MAX as usize {
            return Err(ProjectionError::ResourceLimit);
        }

        let mut offsets = vec![0u64; vertex_ids.len() + 1];
        for edge in &canonical_edges {
            check_cancelled(&budget)?;
            let source = vertex_indices[&edge.source] as usize;
            offsets[source + 1] = offsets[source + 1]
                .checked_add(1)
                .ok_or(ProjectionError::ResourceLimit)?;
        }
        for index in 1..offsets.len() {
            check_cancelled(&budget)?;
            offsets[index] = offsets[index]
                .checked_add(offsets[index - 1])
                .ok_or(ProjectionError::ResourceLimit)?;
        }
        let mut neighbors = Vec::with_capacity(canonical_edges.len());
        let mut edge_ids = Vec::with_capacity(canonical_edges.len());
        for edge in &canonical_edges {
            check_cancelled(&budget)?;
            neighbors.push(vertex_indices[&edge.target]);
            edge_ids.push(edge.id);
        }

        let reverse = spec
            .include_reverse()
            .then(|| build_reverse(vertex_ids.len(), &canonical_edges, &vertex_indices, &budget))
            .transpose()?;
        let mut ordered_vertices = Vec::with_capacity(vertex_ids.len());
        for vertex_id in &vertex_ids {
            check_cancelled(&budget)?;
            ordered_vertices.push(&vertices[vertex_id]);
        }
        let vertex_properties = build_columns(
            &ordered_vertices,
            spec.vertex_properties(),
            |vertex| &vertex.properties,
            &budget,
        )?;
        let mut edge_references = Vec::with_capacity(canonical_edges.len());
        for edge in &canonical_edges {
            check_cancelled(&budget)?;
            edge_references.push(edge);
        }
        let edge_properties = build_columns(
            &edge_references,
            spec.edge_properties(),
            |edge| &edge.properties,
            &budget,
        )?;

        let digest = content_digest(
            &snapshot,
            &partition_provenance,
            &vertex_ids,
            &offsets,
            &neighbors,
            &edge_ids,
            reverse.as_ref(),
            &vertex_properties,
            &edge_properties,
            &budget,
        )?;

        Ok(Self {
            snapshot,
            partition_provenance,
            vertex_ids,
            offsets,
            neighbors,
            edge_ids,
            reverse,
            vertex_properties,
            edge_properties,
            digest,
            budget_usage,
        })
    }

    pub const fn snapshot(&self) -> &SnapshotProvenance {
        &self.snapshot
    }

    pub fn partition_provenance(&self) -> &[PartitionProvenance] {
        &self.partition_provenance
    }

    pub fn vertex_ids(&self) -> &[VertexId] {
        &self.vertex_ids
    }

    pub fn offsets(&self) -> &[u64] {
        &self.offsets
    }

    pub fn neighbors(&self) -> &[u32] {
        &self.neighbors
    }

    pub fn edge_ids(&self) -> &[EdgeId] {
        &self.edge_ids
    }

    pub const fn reverse(&self) -> Option<&ReverseCsr> {
        self.reverse.as_ref()
    }

    pub fn vertex_property(&self, name: &str) -> Option<&TypedPropertyColumn> {
        self.vertex_properties.get(name)
    }

    pub fn edge_property(&self, name: &str) -> Option<&TypedPropertyColumn> {
        self.edge_properties.get(name)
    }

    pub const fn digest(&self) -> Digest32 {
        self.digest
    }

    pub const fn budget_usage(&self) -> BudgetUsage {
        self.budget_usage
    }

    pub(crate) fn vertex_index(&self, vertex_id: VertexId) -> Option<usize> {
        self.vertex_ids.binary_search(&vertex_id).ok()
    }

    pub(crate) fn outgoing_range(&self, vertex_index: usize) -> Range<usize> {
        self.offsets[vertex_index] as usize..self.offsets[vertex_index + 1] as usize
    }

    pub(crate) fn incoming_range(&self, vertex_index: usize) -> Option<Range<usize>> {
        self.reverse.as_ref().map(|reverse| {
            reverse.offsets[vertex_index] as usize..reverse.offsets[vertex_index + 1] as usize
        })
    }

    pub(crate) fn edge_number(&self, property: &str, edge_index: usize) -> Option<f64> {
        match self.edge_properties.get(property)?.values.get(edge_index)? {
            Some(Value::Integer(value)) => Some(*value as f64),
            Some(Value::FloatBits(value)) => Some(f64::from_bits(*value)),
            _ => None,
        }
    }

    pub(crate) fn edge_integer(&self, property: &str, edge_index: usize) -> Option<i64> {
        match self.edge_properties.get(property)?.values.get(edge_index)? {
            Some(Value::Integer(value)) => Some(*value),
            _ => None,
        }
    }
}

fn check_cancelled(budget: &ProjectionBudget) -> Result<(), ProjectionError> {
    if budget.cancellation().is_cancelled() {
        Err(ProjectionError::Cancelled)
    } else {
        Ok(())
    }
}

fn estimate_projection_bytes<P: SnapshotProjectionInput>(
    parts: &[P],
    spec: &ProjectionSpec,
    budget: &ProjectionBudget,
) -> Result<usize, ProjectionError> {
    let mut bytes = 256usize;
    for part in parts {
        check_cancelled(budget)?;
        bytes = bytes
            .checked_add(64)
            .and_then(|value| value.checked_add(part.vertices().len().checked_mul(128)?))
            .and_then(|value| value.checked_add(part.edges().len().checked_mul(192)?))
            .ok_or(ProjectionError::ResourceLimit)?;
        for vertex in part.vertices() {
            check_cancelled(budget)?;
            bytes = estimate_selected_properties(
                bytes,
                &vertex.properties,
                spec.vertex_properties(),
                budget,
            )?;
        }
        for edge in part.edges() {
            check_cancelled(budget)?;
            bytes = estimate_selected_properties(
                bytes,
                &edge.properties,
                spec.edge_properties(),
                budget,
            )?;
        }
    }
    if spec.include_reverse() {
        let mut edge_count = 0usize;
        for part in parts {
            check_cancelled(budget)?;
            edge_count = edge_count
                .checked_add(part.edges().len())
                .ok_or(ProjectionError::ResourceLimit)?;
        }
        bytes = bytes
            .checked_add(
                edge_count
                    .checked_mul(8)
                    .ok_or(ProjectionError::ResourceLimit)?,
            )
            .ok_or(ProjectionError::ResourceLimit)?;
    }
    Ok(bytes)
}

fn estimate_selected_properties(
    mut bytes: usize,
    properties: &BTreeMap<String, Value>,
    specs: &[PropertyColumnSpec],
    budget: &ProjectionBudget,
) -> Result<usize, ProjectionError> {
    for spec in specs {
        check_cancelled(budget)?;
        bytes = bytes
            .checked_add(spec.name().len())
            .ok_or(ProjectionError::ResourceLimit)?;
        if let Some(value) = properties.get(spec.name()) {
            bytes = bytes
                .checked_add(estimate_value_bytes(value, budget)?)
                .ok_or(ProjectionError::ResourceLimit)?;
        }
    }
    Ok(bytes)
}

fn estimate_value_bytes(
    value: &Value,
    budget: &ProjectionBudget,
) -> Result<usize, ProjectionError> {
    check_cancelled(budget)?;
    match value {
        Value::Null => Ok(1),
        Value::Boolean(_) => Ok(2),
        Value::Integer(_) | Value::FloatBits(_) => Ok(9),
        Value::Bytes(value) => 9usize
            .checked_add(value.len())
            .ok_or(ProjectionError::ResourceLimit),
        Value::String(value) => 9usize
            .checked_add(value.len())
            .ok_or(ProjectionError::ResourceLimit),
        Value::List(values) => {
            let mut bytes = 9usize;
            for value in values {
                check_cancelled(budget)?;
                bytes = bytes
                    .checked_add(estimate_value_bytes(value, budget)?)
                    .ok_or(ProjectionError::ResourceLimit)?;
            }
            Ok(bytes)
        }
        Value::Map(values) => {
            let mut bytes = 9usize;
            for (name, value) in values {
                check_cancelled(budget)?;
                let value_bytes = estimate_value_bytes(value, budget)?;
                bytes = bytes
                    .checked_add(name.len())
                    .and_then(|size| size.checked_add(value_bytes))
                    .ok_or(ProjectionError::ResourceLimit)?;
            }
            Ok(bytes)
        }
    }
}

fn reserve_budget(
    estimated_bytes: usize,
    budget: &ProjectionBudget,
) -> Result<BudgetUsage, ProjectionError> {
    let memory_bytes = estimated_bytes.min(budget.max_memory_bytes());
    let spill_bytes = estimated_bytes - memory_bytes;
    if spill_bytes > budget.max_spill_bytes() {
        return Err(ProjectionError::ResourceLimit);
    }
    Ok(BudgetUsage {
        memory_bytes,
        spill_bytes,
    })
}

fn validate_snapshots<P: SnapshotProjectionInput>(
    parts: &[P],
    budget: &ProjectionBudget,
) -> Result<SnapshotProvenance, ProjectionError> {
    let snapshot = parts
        .first()
        .ok_or(ProjectionError::PartitionGap)?
        .snapshot_provenance()
        .clone();
    for part in parts {
        check_cancelled(budget)?;
        if part.snapshot_provenance() != &snapshot {
            return Err(ProjectionError::SnapshotMismatch);
        }
    }
    Ok(snapshot)
}

fn validate_partitions<P: SnapshotProjectionInput>(
    parts: &[P],
    snapshot: &SnapshotProvenance,
    budget: &ProjectionBudget,
) -> Result<Vec<PartitionProvenance>, ProjectionError> {
    let mut partitions = BTreeMap::<dtg_kernel::ShardId, (u32, BTreeSet<u32>)>::new();
    let mut provenance = BTreeSet::new();
    for part in parts {
        check_cancelled(budget)?;
        let part_provenance = part.partition_provenance();
        let fence = snapshot
            .shards()
            .get(&part_provenance.shard_id)
            .ok_or(ProjectionError::PartitionFenceMismatch)?;
        if part_provenance.placement_epoch != fence.placement_epoch
            || part_provenance.backend_generation != fence.backend_generation
            || part_provenance.applied_index != fence.applied_index
        {
            return Err(ProjectionError::PartitionFenceMismatch);
        }
        if part_provenance.partition_count == 0
            || part_provenance.partition_index >= part_provenance.partition_count
        {
            return Err(ProjectionError::PartitionGap);
        }
        let entry = partitions
            .entry(part_provenance.shard_id)
            .or_insert_with(|| (part_provenance.partition_count, BTreeSet::new()));
        if entry.0 != part_provenance.partition_count {
            return Err(ProjectionError::PartitionGap);
        }
        if !entry.1.insert(part_provenance.partition_index) {
            return Err(ProjectionError::DuplicatePartition);
        }
        provenance.insert(part_provenance);
    }

    if partitions.len() != snapshot.shards().len() {
        return Err(ProjectionError::PartitionGap);
    }
    for shard_id in snapshot.shards().keys() {
        let (partition_count, indices) = partitions
            .get(shard_id)
            .ok_or(ProjectionError::PartitionGap)?;
        if indices.len() != *partition_count as usize {
            return Err(ProjectionError::PartitionGap);
        }
        for index in 0..*partition_count {
            check_cancelled(budget)?;
            if !indices.contains(&index) {
                return Err(ProjectionError::PartitionGap);
            }
        }
    }
    let mut ordered_provenance = Vec::with_capacity(provenance.len());
    for part in provenance {
        check_cancelled(budget)?;
        ordered_provenance.push(part);
    }
    Ok(ordered_provenance)
}

fn build_reverse(
    vertex_count: usize,
    edges: &[ProjectedEdge],
    vertex_indices: &BTreeMap<VertexId, u32>,
    budget: &ProjectionBudget,
) -> Result<ReverseCsr, ProjectionError> {
    let mut entries = BTreeMap::new();
    for (edge_index, edge) in edges.iter().enumerate() {
        check_cancelled(budget)?;
        entries.insert(
            (
                vertex_indices[&edge.target],
                vertex_indices[&edge.source],
                edge_index as u32,
            ),
            (),
        );
    }
    let mut offsets = vec![0u64; vertex_count + 1];
    for &(target, _, _) in entries.keys() {
        check_cancelled(budget)?;
        offsets[target as usize + 1] = offsets[target as usize + 1]
            .checked_add(1)
            .ok_or(ProjectionError::ResourceLimit)?;
    }
    for index in 1..offsets.len() {
        check_cancelled(budget)?;
        offsets[index] = offsets[index]
            .checked_add(offsets[index - 1])
            .ok_or(ProjectionError::ResourceLimit)?;
    }
    let mut neighbors = Vec::with_capacity(entries.len());
    let mut forward_edge_indices = Vec::with_capacity(entries.len());
    for &(_, source, edge_index) in entries.keys() {
        check_cancelled(budget)?;
        neighbors.push(source);
        forward_edge_indices.push(edge_index);
    }
    Ok(ReverseCsr {
        offsets,
        neighbors,
        forward_edge_indices,
    })
}

fn build_columns<T>(
    entities: &[&T],
    specs: &[PropertyColumnSpec],
    properties: impl Fn(&T) -> &BTreeMap<String, Value>,
    budget: &ProjectionBudget,
) -> Result<BTreeMap<String, TypedPropertyColumn>, ProjectionError> {
    let mut columns = BTreeMap::new();
    for spec in specs {
        check_cancelled(budget)?;
        let mut values = Vec::with_capacity(entities.len());
        for entity in entities {
            check_cancelled(budget)?;
            match properties(entity).get(spec.name()) {
                Some(value) if spec.property_type().matches(value) => {
                    values.push(Some(value.clone()));
                }
                Some(_) => return Err(ProjectionError::PropertyTypeMismatch),
                None if spec.is_required() => {
                    return Err(ProjectionError::MissingRequiredProperty);
                }
                None => values.push(None),
            }
        }
        columns.insert(
            spec.name().to_owned(),
            TypedPropertyColumn {
                property_type: spec.property_type(),
                values,
            },
        );
    }
    Ok(columns)
}

#[allow(clippy::too_many_arguments)]
fn content_digest(
    snapshot: &SnapshotProvenance,
    provenance: &[PartitionProvenance],
    vertex_ids: &[VertexId],
    offsets: &[u64],
    neighbors: &[u32],
    edge_ids: &[EdgeId],
    reverse: Option<&ReverseCsr>,
    vertex_properties: &BTreeMap<String, TypedPropertyColumn>,
    edge_properties: &BTreeMap<String, TypedPropertyColumn>,
    budget: &ProjectionBudget,
) -> Result<Digest32, ProjectionError> {
    check_cancelled(budget)?;
    let mut hasher = Hasher::new();
    hasher.update(b"dtgproxy.snapshot-csr.v1");
    hash_u128(&mut hasher, snapshot.transaction_id().get());
    hash_i64(&mut hasher, snapshot.start_time().get());
    hash_u64(&mut hasher, snapshot.catalog_version().get());
    for (shard_id, fence) in snapshot.shards() {
        check_cancelled(budget)?;
        hash_u64(&mut hasher, shard_id.get());
        hash_u64(&mut hasher, fence.placement_epoch.get());
        hash_u64(&mut hasher, fence.backend_generation.get());
        hash_u64(&mut hasher, fence.applied_index);
        hash_i64(&mut hasher, fence.closed_time.get());
    }
    for part in provenance {
        check_cancelled(budget)?;
        hash_u64(&mut hasher, part.shard_id.get());
        hash_u64(&mut hasher, part.placement_epoch.get());
        hash_u64(&mut hasher, part.backend_generation.get());
        hash_u64(&mut hasher, part.applied_index);
        hash_u32(&mut hasher, part.partition_index);
        hash_u32(&mut hasher, part.partition_count);
    }
    for vertex_id in vertex_ids {
        check_cancelled(budget)?;
        hash_u128(&mut hasher, vertex_id.get());
    }
    for offset in offsets {
        check_cancelled(budget)?;
        hash_u64(&mut hasher, *offset);
    }
    for neighbor in neighbors {
        check_cancelled(budget)?;
        hash_u32(&mut hasher, *neighbor);
    }
    for edge_id in edge_ids {
        check_cancelled(budget)?;
        hash_u128(&mut hasher, edge_id.get());
    }
    match reverse {
        Some(reverse) => {
            hasher.update(&[1]);
            for offset in &reverse.offsets {
                check_cancelled(budget)?;
                hash_u64(&mut hasher, *offset);
            }
            for neighbor in &reverse.neighbors {
                check_cancelled(budget)?;
                hash_u32(&mut hasher, *neighbor);
            }
            for edge_index in &reverse.forward_edge_indices {
                check_cancelled(budget)?;
                hash_u32(&mut hasher, *edge_index);
            }
        }
        None => {
            hasher.update(&[0]);
        }
    }
    hash_columns(&mut hasher, vertex_properties, budget)?;
    hash_columns(&mut hasher, edge_properties, budget)?;
    Ok(Digest32::new(*hasher.finalize().as_bytes()))
}

fn hash_columns(
    hasher: &mut Hasher,
    columns: &BTreeMap<String, TypedPropertyColumn>,
    budget: &ProjectionBudget,
) -> Result<(), ProjectionError> {
    for (name, column) in columns {
        check_cancelled(budget)?;
        hash_bytes(hasher, name.as_bytes());
        hasher.update(&[column.property_type.tag()]);
        for value in &column.values {
            check_cancelled(budget)?;
            match value {
                Some(value) => {
                    hasher.update(&[1]);
                    hash_value(hasher, value, budget)?;
                }
                None => {
                    hasher.update(&[0]);
                }
            }
        }
    }
    Ok(())
}

fn hash_value(
    hasher: &mut Hasher,
    value: &Value,
    budget: &ProjectionBudget,
) -> Result<(), ProjectionError> {
    check_cancelled(budget)?;
    match value {
        Value::Null => {
            hasher.update(&[0]);
        }
        Value::Boolean(value) => {
            hasher.update(&[1, u8::from(*value)]);
        }
        Value::Integer(value) => {
            hasher.update(&[2]);
            hash_i64(hasher, *value);
        }
        Value::FloatBits(value) => {
            hasher.update(&[3]);
            hash_u64(hasher, *value);
        }
        Value::Bytes(value) => {
            hasher.update(&[4]);
            hash_bytes(hasher, value);
        }
        Value::String(value) => {
            hasher.update(&[5]);
            hash_bytes(hasher, value.as_bytes());
        }
        Value::List(values) => {
            hasher.update(&[6]);
            hash_u64(hasher, values.len() as u64);
            for value in values {
                check_cancelled(budget)?;
                hash_value(hasher, value, budget)?;
            }
        }
        Value::Map(values) => {
            hasher.update(&[7]);
            hash_u64(hasher, values.len() as u64);
            for (name, value) in values {
                check_cancelled(budget)?;
                hash_bytes(hasher, name.as_bytes());
                hash_value(hasher, value, budget)?;
            }
        }
    }
    Ok(())
}

fn hash_bytes(hasher: &mut Hasher, value: &[u8]) {
    hash_u64(hasher, value.len() as u64);
    hasher.update(value);
}

fn hash_u32(hasher: &mut Hasher, value: u32) {
    hasher.update(&value.to_le_bytes());
}

fn hash_u64(hasher: &mut Hasher, value: u64) {
    hasher.update(&value.to_le_bytes());
}

fn hash_i64(hasher: &mut Hasher, value: i64) {
    hasher.update(&value.to_le_bytes());
}

fn hash_u128(hasher: &mut Hasher, value: u128) {
    hasher.update(&value.to_le_bytes());
}
