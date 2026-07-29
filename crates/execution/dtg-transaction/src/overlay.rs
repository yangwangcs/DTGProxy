use std::collections::{BTreeMap, BTreeSet};

use dtg_storage::{EdgeId, EdgeVersion, LogicalMutation, VertexId, VertexVersion};

use crate::TxnError;

const MAX_STAGED_MUTATIONS: usize = 4_096;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BaseGraphSnapshot {
    vertices: BTreeMap<VertexId, VertexVersion>,
    edges: BTreeMap<EdgeId, EdgeVersion>,
}

impl BaseGraphSnapshot {
    pub fn new(vertices: Vec<VertexVersion>, edges: Vec<EdgeVersion>) -> Result<Self, TxnError> {
        let mut snapshot = Self::default();
        for vertex in vertices {
            if snapshot.vertices.insert(vertex.id(), vertex).is_some() {
                return Err(TxnError::DuplicateIdentity);
            }
        }
        for edge in edges {
            if snapshot.edges.insert(edge.id(), edge).is_some() {
                return Err(TxnError::DuplicateIdentity);
            }
        }
        Ok(snapshot)
    }

    pub fn vertices(&self) -> &BTreeMap<VertexId, VertexVersion> {
        &self.vertices
    }

    pub fn edges(&self) -> &BTreeMap<EdgeId, EdgeVersion> {
        &self.edges
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TransactionOverlay {
    mutations: Vec<LogicalMutation>,
}

impl TransactionOverlay {
    pub const fn new() -> Self {
        Self {
            mutations: Vec::new(),
        }
    }

    pub fn stage(&mut self, mutation: LogicalMutation) -> Result<(), TxnError> {
        if self.mutations.len() == MAX_STAGED_MUTATIONS {
            return Err(TxnError::ResourceLimit);
        }
        if matches!(
            mutation,
            LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_)
        ) {
            return Err(TxnError::InvalidMutation);
        }
        self.mutations.push(mutation);
        Ok(())
    }

    pub fn mutations(&self) -> &[LogicalMutation] {
        &self.mutations
    }

    pub fn get_vertex(
        &self,
        base: Option<&VertexVersion>,
        id: VertexId,
        valid_at: i64,
    ) -> Option<VertexVersion> {
        if self.mutations.iter().any(
            |mutation| matches!(mutation, LogicalMutation::DeleteVertex(value) if value.id() == id),
        ) {
            return None;
        }
        self.mutations
            .iter()
            .filter_map(|mutation| match mutation {
                LogicalMutation::PutVertex(vertex)
                    if vertex.id() == id
                        && vertex.valid_time().start() <= valid_at
                        && valid_at < vertex.valid_time().end() =>
                {
                    Some(vertex)
                }
                _ => None,
            })
            .max_by_key(|vertex| vertex.version())
            .cloned()
            .or_else(|| {
                base.filter(|vertex| {
                    vertex.id() == id
                        && vertex.valid_time().start() <= valid_at
                        && valid_at < vertex.valid_time().end()
                })
                .cloned()
            })
    }

    pub fn get_edge(
        &self,
        base: Option<&EdgeVersion>,
        id: EdgeId,
        valid_at: i64,
    ) -> Option<EdgeVersion> {
        if self.mutations.iter().any(
            |mutation| matches!(mutation, LogicalMutation::DeleteEdge(value) if value.id() == id),
        ) {
            return None;
        }
        self.mutations
            .iter()
            .filter_map(|mutation| match mutation {
                LogicalMutation::PutEdge(edge)
                    if edge.id() == id
                        && edge.valid_time().start() <= valid_at
                        && valid_at < edge.valid_time().end() =>
                {
                    Some(edge)
                }
                _ => None,
            })
            .max_by_key(|edge| edge.version())
            .cloned()
            .or_else(|| {
                base.filter(|edge| {
                    edge.id() == id
                        && edge.valid_time().start() <= valid_at
                        && valid_at < edge.valid_time().end()
                })
                .cloned()
            })
    }

    pub fn validate(&self, base: &BaseGraphSnapshot) -> Result<(), TxnError> {
        self.validate_vertex_identity(base)?;
        self.validate_edge_identity(base)?;

        let mut vertices: BTreeSet<_> = base.vertices.keys().copied().collect();
        let mut edges = base.edges.clone();
        for mutation in &self.mutations {
            match mutation {
                LogicalMutation::PutVertex(vertex) => {
                    vertices.insert(vertex.id());
                }
                LogicalMutation::DeleteVertex(vertex) => {
                    vertices.remove(&vertex.id());
                }
                LogicalMutation::PutEdge(edge) => {
                    edges.insert(edge.id(), edge.clone());
                }
                LogicalMutation::DeleteEdge(edge) => {
                    edges.remove(&edge.id());
                }
                LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_) => {
                    return Err(TxnError::InvalidMutation);
                }
            }
        }
        if edges
            .values()
            .any(|edge| !vertices.contains(&edge.source()) || !vertices.contains(&edge.target()))
        {
            return Err(TxnError::ReferentialIntegrity);
        }
        Ok(())
    }

    fn validate_vertex_identity(&self, base: &BaseGraphSnapshot) -> Result<(), TxnError> {
        let mut puts: BTreeMap<VertexId, Vec<&VertexVersion>> = BTreeMap::new();
        let mut deletes = BTreeSet::new();
        for mutation in &self.mutations {
            match mutation {
                LogicalMutation::PutVertex(vertex) => {
                    puts.entry(vertex.id()).or_default().push(vertex)
                }
                LogicalMutation::DeleteVertex(vertex) => {
                    deletes.insert(vertex.id());
                }
                _ => {}
            }
        }
        for (id, versions) in puts {
            if deletes.contains(&id)
                || overlapping_vertex_versions(&versions)
                || base.vertices.get(&id).is_some_and(|existing| {
                    versions
                        .iter()
                        .any(|staged| staged.version() == existing.version())
                })
            {
                return Err(TxnError::DuplicateIdentity);
            }
        }
        Ok(())
    }

    fn validate_edge_identity(&self, base: &BaseGraphSnapshot) -> Result<(), TxnError> {
        let mut puts: BTreeMap<EdgeId, Vec<&EdgeVersion>> = BTreeMap::new();
        let mut deletes = BTreeSet::new();
        for mutation in &self.mutations {
            match mutation {
                LogicalMutation::PutEdge(edge) => puts.entry(edge.id()).or_default().push(edge),
                LogicalMutation::DeleteEdge(edge) => {
                    deletes.insert(edge.id());
                }
                _ => {}
            }
        }
        for (id, versions) in puts {
            let first = versions[0];
            if deletes.contains(&id)
                || versions.iter().skip(1).any(|edge| {
                    edge.source() != first.source()
                        || edge.target() != first.target()
                        || edge.edge_type() != first.edge_type()
                })
                || overlapping_edge_versions(&versions)
                || base.edges.get(&id).is_some_and(|existing| {
                    existing.source() != first.source()
                        || existing.target() != first.target()
                        || existing.edge_type() != first.edge_type()
                        || versions
                            .iter()
                            .any(|staged| staged.version() == existing.version())
                })
            {
                return Err(TxnError::DuplicateIdentity);
            }
        }
        Ok(())
    }
}

fn overlapping_vertex_versions(versions: &[&VertexVersion]) -> bool {
    versions.iter().enumerate().any(|(index, left)| {
        versions[index + 1..]
            .iter()
            .any(|right| left.valid_time().overlaps(right.valid_time()))
    })
}

fn overlapping_edge_versions(versions: &[&EdgeVersion]) -> bool {
    versions.iter().enumerate().any(|(index, left)| {
        versions[index + 1..]
            .iter()
            .any(|right| left.valid_time().overlaps(right.valid_time()))
    })
}
