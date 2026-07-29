use std::collections::BTreeMap;

use crate::catalog::ExecutionGuard;
use crate::centrality::undirected_adjacency;
use crate::{AlgorithmRequest, AlgorithmResult, AnalyticsError, SnapshotCsr};

pub(crate) fn k_core(
    csr: &SnapshotCsr,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let adjacency = undirected_adjacency(csr, guard)?;
    let vertex_count = adjacency.len();
    guard.reserve(vertex_count.saturating_mul(9))?;
    let mut remaining = vec![true; vertex_count];
    let mut degree = Vec::with_capacity(vertex_count);
    for neighbors in &adjacency {
        guard.step()?;
        degree.push(neighbors.len());
    }
    let mut cores = vec![0u32; vertex_count];
    let mut current_core = 0usize;
    for _ in 0..vertex_count {
        guard.step()?;
        let mut selected = None;
        for vertex in 0..vertex_count {
            guard.step()?;
            if remaining[vertex]
                && selected
                    .is_none_or(|current| (degree[vertex], vertex) < (degree[current], current))
            {
                selected = Some(vertex);
            }
        }
        let vertex = selected.ok_or(AnalyticsError::InvalidRequest)?;
        current_core = current_core.max(degree[vertex]);
        cores[vertex] = u32::try_from(current_core).map_err(|_| AnalyticsError::ResourceLimit)?;
        remaining[vertex] = false;
        for neighbor in adjacency[vertex].iter().copied() {
            guard.step()?;
            if remaining[neighbor] {
                degree[neighbor] = degree[neighbor].saturating_sub(1);
            }
        }
    }
    let mut result = BTreeMap::new();
    for (vertex_id, core) in csr.vertex_ids().iter().copied().zip(cores) {
        guard.step()?;
        result.insert(vertex_id, core);
    }
    Ok(AlgorithmResult::CoreNumbers(result))
}

pub(crate) fn label_propagation(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    guard.require_iterations(request.iterations)?;
    let adjacency = undirected_adjacency(csr, guard)?;
    let mut labels = Vec::with_capacity(adjacency.len());
    for index in 0..adjacency.len() {
        guard.step()?;
        labels.push(index as u64 + 1);
    }
    for _ in 0..request.iterations {
        guard.step()?;
        let mut next = labels.clone();
        let mut changed = false;
        for vertex in 0..adjacency.len() {
            guard.step()?;
            if adjacency[vertex].is_empty() {
                continue;
            }
            let mut counts = BTreeMap::<u64, usize>::new();
            for neighbor in adjacency[vertex].iter().copied() {
                guard.step()?;
                *counts.entry(labels[neighbor]).or_default() += 1;
            }
            let mut chosen = None;
            for (label, count) in counts {
                guard.step()?;
                if chosen.is_none_or(|(best_label, best_count)| {
                    count > best_count || (count == best_count && label < best_label)
                }) {
                    chosen = Some((label, count));
                }
            }
            let chosen = chosen
                .map(|(label, _)| label)
                .ok_or(AnalyticsError::InvalidRequest)?;
            changed |= chosen != labels[vertex];
            next[vertex] = chosen;
        }
        labels = next;
        if !changed {
            break;
        }
    }
    canonicalize_labels(&mut labels, guard)?;
    communities_result(csr, labels, guard)
}

pub(crate) fn louvain(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    guard.require_iterations(request.iterations)?;
    let adjacency = undirected_adjacency(csr, guard)?;
    let vertex_count = adjacency.len();
    let mut communities = Vec::with_capacity(vertex_count);
    let mut degrees = Vec::with_capacity(vertex_count);
    let mut total_degree = 0.0;
    for (vertex, neighbors) in adjacency.iter().enumerate() {
        guard.step()?;
        communities.push(vertex);
        let degree = neighbors.len() as f64;
        degrees.push(degree);
        total_degree += degree;
    }
    if total_degree == 0.0 {
        let mut labels = Vec::with_capacity(vertex_count);
        for index in 0..vertex_count {
            guard.step()?;
            labels.push(index as u64 + 1);
        }
        return communities_result(csr, labels, guard);
    }
    for _ in 0..request.iterations {
        guard.step()?;
        let mut moved = false;
        for vertex in 0..vertex_count {
            guard.step()?;
            let mut candidates = BTreeMap::<usize, f64>::new();
            for neighbor in adjacency[vertex].iter().copied() {
                guard.step()?;
                *candidates.entry(communities[neighbor]).or_default() += 1.0;
            }
            let mut community_totals = BTreeMap::<usize, f64>::new();
            for (member, community) in communities.iter().copied().enumerate() {
                guard.step()?;
                *community_totals.entry(community).or_default() += degrees[member];
            }
            let current = communities[vertex];
            let mut best = current;
            let mut best_gain = f64::NEG_INFINITY;
            for (candidate, internal_weight) in candidates {
                guard.step()?;
                let gain =
                    internal_weight - degrees[vertex] * community_totals[&candidate] / total_degree;
                if gain > best_gain || (gain == best_gain && candidate < best) {
                    best_gain = gain;
                    best = candidate;
                }
            }
            if best != current && best_gain > 0.0 {
                communities[vertex] = best;
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    let mut labels = Vec::with_capacity(communities.len());
    for community in communities {
        guard.step()?;
        labels.push(community as u64 + 1);
    }
    canonicalize_labels(&mut labels, guard)?;
    communities_result(csr, labels, guard)
}

fn canonicalize_labels(
    labels: &mut [u64],
    guard: &mut ExecutionGuard,
) -> Result<(), AnalyticsError> {
    let mut distinct = BTreeMap::<u64, ()>::new();
    for label in labels.iter().copied() {
        guard.step()?;
        distinct.insert(label, ());
    }
    let mut remap = BTreeMap::new();
    for (index, label) in distinct.keys().copied().enumerate() {
        guard.step()?;
        remap.insert(label, index as u64 + 1);
    }
    for label in labels {
        guard.step()?;
        *label = remap[label];
    }
    Ok(())
}

fn communities_result(
    csr: &SnapshotCsr,
    labels: Vec<u64>,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let mut result = BTreeMap::new();
    for (vertex_id, label) in csr.vertex_ids().iter().copied().zip(labels) {
        guard.step()?;
        result.insert(vertex_id, label);
    }
    Ok(AlgorithmResult::Communities(result))
}
