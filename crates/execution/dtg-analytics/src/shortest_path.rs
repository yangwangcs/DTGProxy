use std::collections::BTreeMap;

use crate::catalog::ExecutionGuard;
use crate::{AlgorithmRequest, AlgorithmResult, AnalyticsError, SnapshotCsr};

pub(crate) fn bounded_sssp(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let source = request
        .source
        .and_then(|source| csr.vertex_index(source))
        .ok_or(AnalyticsError::InvalidRequest)?;
    let distances = shortest_distances(csr, source, request.weight_property.as_deref(), guard)?;
    let mut result = BTreeMap::new();
    for (vertex_id, distance) in csr.vertex_ids().iter().copied().zip(distances) {
        guard.step()?;
        if distance.is_finite() {
            result.insert(vertex_id, distance);
        }
    }
    Ok(AlgorithmResult::Distances(result))
}

pub(crate) fn bounded_apsp(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let vertex_count = csr.vertex_ids().len();
    let pairs = vertex_count
        .checked_mul(vertex_count)
        .ok_or(AnalyticsError::ResourceLimit)?;
    if pairs > request.max_pairs {
        return Err(AnalyticsError::ResourceLimit);
    }
    guard.reserve(pairs.saturating_mul(std::mem::size_of::<f64>()))?;
    let mut result = BTreeMap::new();
    for source in 0..vertex_count {
        guard.step()?;
        let distances = shortest_distances(csr, source, request.weight_property.as_deref(), guard)?;
        for (target, distance) in distances.into_iter().enumerate() {
            guard.step()?;
            if distance.is_finite() {
                result.insert(
                    (csr.vertex_ids()[source], csr.vertex_ids()[target]),
                    distance,
                );
            }
        }
    }
    Ok(AlgorithmResult::AllPairs(result))
}

pub(crate) fn shortest_distances(
    csr: &SnapshotCsr,
    source: usize,
    weight_property: Option<&str>,
    guard: &mut ExecutionGuard,
) -> Result<Vec<f64>, AnalyticsError> {
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count.saturating_mul(9))?;
    let mut distances = vec![f64::INFINITY; vertex_count];
    let mut settled = vec![false; vertex_count];
    distances[source] = 0.0;

    for _ in 0..vertex_count {
        guard.step()?;
        let mut next = None;
        for vertex in 0..vertex_count {
            guard.step()?;
            if !settled[vertex]
                && distances[vertex].is_finite()
                && next.is_none_or(|current| distances[vertex] < distances[current])
            {
                next = Some(vertex);
            }
        }
        let Some(vertex) = next else {
            break;
        };
        settled[vertex] = true;
        for edge_index in csr.outgoing_range(vertex) {
            guard.step()?;
            let weight = match weight_property {
                Some(property) => csr
                    .edge_number(property, edge_index)
                    .ok_or(AnalyticsError::MissingProperty)?,
                None => 1.0,
            };
            if !weight.is_finite() || weight < 0.0 {
                return Err(AnalyticsError::InvalidProperty);
            }
            let neighbor = csr.neighbors()[edge_index] as usize;
            let candidate = distances[vertex] + weight;
            if candidate < distances[neighbor] {
                distances[neighbor] = candidate;
            }
        }
    }
    Ok(distances)
}
