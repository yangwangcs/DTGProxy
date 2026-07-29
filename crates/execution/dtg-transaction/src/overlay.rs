use std::collections::{BTreeMap, BTreeSet};

use dtg_storage::{EdgeId, EdgeVersion, LogicalMutation, VertexId, VertexVersion};

use crate::TxnError;

const MAX_STAGED_MUTATIONS: usize = 4_096;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BaseGraphSnapshot {
    vertices: BTreeMap<VertexId, Vec<VertexVersion>>,
    edges: BTreeMap<EdgeId, Vec<EdgeVersion>>,
}

impl BaseGraphSnapshot {
    pub fn new(vertices: Vec<VertexVersion>, edges: Vec<EdgeVersion>) -> Result<Self, TxnError> {
        let mut snapshot = Self::default();
        for vertex in vertices {
            snapshot
                .vertices
                .entry(vertex.id())
                .or_default()
                .push(vertex);
        }
        for edge in edges {
            snapshot.edges.entry(edge.id()).or_default().push(edge);
        }
        for history in snapshot.vertices.values_mut() {
            sort_vertex_history(history);
            validate_vertex_history(history.iter())?;
        }
        for history in snapshot.edges.values_mut() {
            sort_edge_history(history);
            validate_edge_history(history.iter())?;
        }
        Ok(snapshot)
    }

    pub fn vertices(&self) -> &BTreeMap<VertexId, Vec<VertexVersion>> {
        &self.vertices
    }

    pub fn edges(&self) -> &BTreeMap<EdgeId, Vec<EdgeVersion>> {
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

    pub fn get_vertex<'a>(
        &self,
        base: impl IntoIterator<Item = &'a VertexVersion>,
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
                base.into_iter()
                    .filter(|vertex| {
                        vertex.id() == id
                            && vertex.valid_time().start() <= valid_at
                            && valid_at < vertex.valid_time().end()
                    })
                    .max_by_key(|vertex| vertex.version())
                    .cloned()
            })
    }

    pub fn get_edge<'a>(
        &self,
        base: impl IntoIterator<Item = &'a EdgeVersion>,
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
                base.into_iter()
                    .filter(|edge| {
                        edge.id() == id
                            && edge.valid_time().start() <= valid_at
                            && valid_at < edge.valid_time().end()
                    })
                    .max_by_key(|edge| edge.version())
                    .cloned()
            })
    }

    pub fn validate(&self, base: &BaseGraphSnapshot) -> Result<(), TxnError> {
        self.validate_vertex_identity(base)?;
        self.validate_edge_identity(base)?;

        let mut vertices = base.vertices.clone();
        let mut edges = base.edges.clone();
        for mutation in &self.mutations {
            match mutation {
                LogicalMutation::PutVertex(vertex) => {
                    vertices
                        .entry(vertex.id())
                        .or_default()
                        .push(vertex.clone());
                }
                LogicalMutation::DeleteVertex(vertex) => {
                    vertices.remove(&vertex.id());
                }
                LogicalMutation::PutEdge(edge) => {
                    edges.entry(edge.id()).or_default().push(edge.clone());
                }
                LogicalMutation::DeleteEdge(edge) => {
                    edges.remove(&edge.id());
                }
                LogicalMutation::PutTransaction(_) | LogicalMutation::PutReplicaMetadata(_) => {
                    return Err(TxnError::InvalidMutation);
                }
            }
        }
        for history in vertices.values_mut() {
            sort_vertex_history(history);
            validate_vertex_history(history.iter())?;
        }
        for history in edges.values_mut() {
            sort_edge_history(history);
            validate_edge_history(history.iter())?;
        }
        for history in edges.values() {
            for edge in history {
                let source = vertices
                    .get(&edge.source())
                    .ok_or(TxnError::ReferentialIntegrity)?;
                let target = vertices
                    .get(&edge.target())
                    .ok_or(TxnError::ReferentialIntegrity)?;
                if !history_covers(source, edge.valid_time())
                    || !history_covers(target, edge.valid_time())
                {
                    return Err(TxnError::ReferentialIntegrity);
                }
            }
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
                    versions.iter().any(|staged| {
                        existing
                            .iter()
                            .any(|base| staged.version() == base.version())
                    })
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
                    existing.iter().any(|base| {
                        base.source() != first.source()
                            || base.target() != first.target()
                            || base.edge_type() != first.edge_type()
                            || versions
                                .iter()
                                .any(|staged| staged.version() == base.version())
                    })
                })
            {
                return Err(TxnError::DuplicateIdentity);
            }
        }
        Ok(())
    }
}

fn validate_vertex_history<'a>(
    versions: impl Iterator<Item = &'a VertexVersion>,
) -> Result<(), TxnError> {
    let versions: Vec<_> = versions.collect();
    if versions.iter().enumerate().any(|(index, left)| {
        versions[index + 1..].iter().any(|right| {
            left.version() == right.version() || left.valid_time().overlaps(right.valid_time())
        })
    }) {
        Err(TxnError::DuplicateIdentity)
    } else {
        Ok(())
    }
}

fn validate_edge_history<'a>(
    versions: impl Iterator<Item = &'a EdgeVersion>,
) -> Result<(), TxnError> {
    let versions: Vec<_> = versions.collect();
    let first = versions[0];
    if versions.iter().any(|edge| {
        edge.source() != first.source()
            || edge.target() != first.target()
            || edge.edge_type() != first.edge_type()
    }) || versions.iter().enumerate().any(|(index, left)| {
        versions[index + 1..].iter().any(|right| {
            left.version() == right.version() || left.valid_time().overlaps(right.valid_time())
        })
    }) {
        Err(TxnError::DuplicateIdentity)
    } else {
        Ok(())
    }
}

fn sort_vertex_history(history: &mut [VertexVersion]) {
    history.sort_by_key(|vertex| {
        (
            vertex.valid_time().start(),
            vertex.valid_time().end(),
            vertex.version(),
        )
    });
}

fn sort_edge_history(history: &mut [EdgeVersion]) {
    history.sort_by_key(|edge| {
        (
            edge.valid_time().start(),
            edge.valid_time().end(),
            edge.version(),
        )
    });
}

fn history_covers(versions: &[VertexVersion], required: dtg_kernel::ValidInterval) -> bool {
    let mut intervals: Vec<_> = versions.iter().map(VertexVersion::valid_time).collect();
    intervals.sort_by_key(|interval| (interval.start(), interval.end()));
    let mut covered_through = required.start();
    for interval in intervals {
        if interval.end() <= covered_through || interval.start() >= required.end() {
            continue;
        }
        if interval.start() > covered_through {
            return false;
        }
        covered_through = covered_through.max(interval.end());
        if covered_through >= required.end() {
            return true;
        }
    }
    false
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
