use std::collections::{BTreeSet, VecDeque};

use crate::catalog::ExecutionGuard;
use crate::{AlgorithmResult, AnalyticsError, SnapshotCsr};

pub(crate) fn strongly_connected_components(
    csr: &SnapshotCsr,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    if csr.reverse().is_none() {
        return Err(AnalyticsError::MissingReverseCsr);
    }
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count.saturating_mul(3))?;
    let mut assigned = vec![false; vertex_count];
    let mut components = Vec::new();
    for seed in 0..vertex_count {
        guard.step()?;
        if assigned[seed] {
            continue;
        }
        let forward = reachable(csr, seed, false, guard)?;
        let reverse = reachable(csr, seed, true, guard)?;
        let mut component = Vec::new();
        for vertex in 0..vertex_count {
            guard.step()?;
            if forward[vertex] && reverse[vertex] {
                assigned[vertex] = true;
                component.push(csr.vertex_ids()[vertex]);
            }
        }
        components.push(component);
    }
    Ok(AlgorithmResult::Components(components))
}

pub(crate) fn weakly_connected_components(
    csr: &SnapshotCsr,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    if csr.reverse().is_none() {
        return Err(AnalyticsError::MissingReverseCsr);
    }
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count)?;
    let mut visited = vec![false; vertex_count];
    let mut components = Vec::new();
    for seed in 0..vertex_count {
        guard.step()?;
        if visited[seed] {
            continue;
        }
        let mut queue = VecDeque::from([seed]);
        visited[seed] = true;
        let mut component = Vec::new();
        while let Some(vertex) = queue.pop_front() {
            guard.step()?;
            component.push(csr.vertex_ids()[vertex]);
            let neighbors = neighbors_both_directions(csr, vertex, guard)?;
            for neighbor in neighbors {
                guard.step()?;
                if !visited[neighbor] {
                    visited[neighbor] = true;
                    queue.push_back(neighbor);
                }
            }
        }
        components.push(component);
    }
    Ok(AlgorithmResult::Components(components))
}

fn reachable(
    csr: &SnapshotCsr,
    seed: usize,
    reverse_direction: bool,
    guard: &mut ExecutionGuard,
) -> Result<Vec<bool>, AnalyticsError> {
    let mut visited = vec![false; csr.vertex_ids().len()];
    let mut queue = VecDeque::from([seed]);
    visited[seed] = true;
    while let Some(vertex) = queue.pop_front() {
        guard.step()?;
        if reverse_direction {
            let reverse = csr.reverse().ok_or(AnalyticsError::MissingReverseCsr)?;
            let range = csr
                .incoming_range(vertex)
                .ok_or(AnalyticsError::MissingReverseCsr)?;
            for position in range {
                guard.step()?;
                let neighbor = reverse.neighbors()[position] as usize;
                if !visited[neighbor] {
                    visited[neighbor] = true;
                    queue.push_back(neighbor);
                }
            }
        } else {
            for edge_index in csr.outgoing_range(vertex) {
                guard.step()?;
                let neighbor = csr.neighbors()[edge_index] as usize;
                if !visited[neighbor] {
                    visited[neighbor] = true;
                    queue.push_back(neighbor);
                }
            }
        }
    }
    Ok(visited)
}

pub(crate) fn neighbors_both_directions(
    csr: &SnapshotCsr,
    vertex: usize,
    guard: &mut ExecutionGuard,
) -> Result<BTreeSet<usize>, AnalyticsError> {
    let mut neighbors = BTreeSet::new();
    for edge_index in csr.outgoing_range(vertex) {
        guard.step()?;
        neighbors.insert(csr.neighbors()[edge_index] as usize);
    }
    let reverse = csr.reverse().ok_or(AnalyticsError::MissingReverseCsr)?;
    let range = csr
        .incoming_range(vertex)
        .ok_or(AnalyticsError::MissingReverseCsr)?;
    for position in range {
        guard.step()?;
        neighbors.insert(reverse.neighbors()[position] as usize);
    }
    Ok(neighbors)
}
