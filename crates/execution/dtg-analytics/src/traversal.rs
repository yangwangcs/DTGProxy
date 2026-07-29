use std::collections::VecDeque;

use crate::{AlgorithmRequest, AlgorithmResult, AnalyticsError, SnapshotCsr};

use crate::catalog::ExecutionGuard;

pub(crate) fn breadth_first_search(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let source = source_index(csr, request)?;
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count.saturating_mul(9))?;
    let mut visited = vec![false; vertex_count];
    let mut queue = VecDeque::new();
    let mut order = Vec::new();
    visited[source] = true;
    queue.push_back((source, 0u32));
    while let Some((vertex, depth)) = queue.pop_front() {
        guard.step()?;
        order.push(csr.vertex_ids()[vertex]);
        if depth >= request.max_depth {
            continue;
        }
        for edge_index in csr.outgoing_range(vertex) {
            guard.step()?;
            let neighbor = csr.neighbors()[edge_index] as usize;
            if !visited[neighbor] {
                visited[neighbor] = true;
                queue.push_back((neighbor, depth + 1));
            }
        }
    }
    Ok(AlgorithmResult::Traversal(order))
}

pub(crate) fn depth_first_search(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let source = source_index(csr, request)?;
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count.saturating_mul(9))?;
    let mut visited = vec![false; vertex_count];
    let mut stack = vec![(source, 0u32)];
    let mut order = Vec::new();
    visited[source] = true;
    while let Some((vertex, depth)) = stack.pop() {
        guard.step()?;
        order.push(csr.vertex_ids()[vertex]);
        if depth >= request.max_depth {
            continue;
        }
        for edge_index in csr.outgoing_range(vertex).rev() {
            guard.step()?;
            let neighbor = csr.neighbors()[edge_index] as usize;
            if !visited[neighbor] {
                visited[neighbor] = true;
                stack.push((neighbor, depth + 1));
            }
        }
    }
    Ok(AlgorithmResult::Traversal(order))
}

fn source_index(csr: &SnapshotCsr, request: &AlgorithmRequest) -> Result<usize, AnalyticsError> {
    request
        .source
        .and_then(|source| csr.vertex_index(source))
        .ok_or(AnalyticsError::InvalidRequest)
}
