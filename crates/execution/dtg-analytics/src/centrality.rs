use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::catalog::ExecutionGuard;
use crate::shortest_path::shortest_distances;
use crate::{AlgorithmRequest, AlgorithmResult, AnalyticsError, SnapshotCsr};

pub(crate) fn page_rank(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    guard.require_iterations(request.iterations)?;
    if !(0.0..1.0).contains(&request.damping) {
        return Err(AnalyticsError::InvalidRequest);
    }
    let vertex_count = csr.vertex_ids().len();
    if vertex_count == 0 {
        return Ok(AlgorithmResult::Scores(BTreeMap::new()));
    }
    guard.reserve(vertex_count.saturating_mul(16))?;
    let count = vertex_count as f64;
    let mut ranks = vec![1.0 / count; vertex_count];
    for _ in 0..request.iterations {
        guard.step()?;
        let mut dangling = 0.0;
        for (vertex, rank) in ranks.iter().copied().enumerate() {
            guard.step()?;
            if csr.outgoing_range(vertex).is_empty() {
                dangling += rank;
            }
        }
        let base = (1.0 - request.damping) / count + request.damping * dangling / count;
        let mut next = vec![base; vertex_count];
        for (source, rank) in ranks.iter().copied().enumerate() {
            guard.step()?;
            let range = csr.outgoing_range(source);
            let degree = range.len();
            if degree == 0 {
                continue;
            }
            let contribution = request.damping * rank / degree as f64;
            for edge_index in range {
                guard.step()?;
                next[csr.neighbors()[edge_index] as usize] += contribution;
            }
        }
        ranks = next;
    }
    let mut total = 0.0;
    for rank in ranks.iter().copied() {
        guard.step()?;
        total += rank;
    }
    if total > 0.0 {
        for rank in &mut ranks {
            guard.step()?;
            *rank /= total;
        }
    }
    let mut scores = BTreeMap::new();
    for (vertex_id, rank) in csr.vertex_ids().iter().copied().zip(ranks) {
        guard.step()?;
        scores.insert(vertex_id, rank);
    }
    Ok(AlgorithmResult::Scores(scores))
}

pub(crate) fn degree_centrality(
    csr: &SnapshotCsr,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    csr.reverse().ok_or(AnalyticsError::MissingReverseCsr)?;
    let mut scores = BTreeMap::new();
    let denominator = csr.vertex_ids().len().saturating_sub(1).max(1) as f64;
    for vertex in 0..csr.vertex_ids().len() {
        guard.step()?;
        let out_degree = csr.outgoing_range(vertex).len();
        let in_degree = csr
            .incoming_range(vertex)
            .ok_or(AnalyticsError::MissingReverseCsr)?
            .len();
        scores.insert(
            csr.vertex_ids()[vertex],
            (out_degree + in_degree) as f64 / denominator,
        );
    }
    Ok(AlgorithmResult::Scores(scores))
}

pub(crate) fn closeness_centrality(
    csr: &SnapshotCsr,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let vertex_count = csr.vertex_ids().len();
    let mut scores = BTreeMap::new();
    for source in 0..vertex_count {
        guard.step()?;
        let distances = shortest_distances(csr, source, None, guard)?;
        let mut reachable = 0usize;
        let mut sum = 0.0;
        for (target, distance) in distances.iter().copied().enumerate() {
            guard.step()?;
            if target != source && distance.is_finite() {
                reachable += 1;
                sum += distance;
            }
        }
        let score = if sum == 0.0 || vertex_count <= 1 {
            0.0
        } else {
            (reachable * reachable) as f64 / ((vertex_count - 1) as f64 * sum)
        };
        scores.insert(csr.vertex_ids()[source], score);
    }
    Ok(AlgorithmResult::Scores(scores))
}

pub(crate) fn betweenness_centrality(
    csr: &SnapshotCsr,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count.saturating_mul(vertex_count).saturating_mul(8))?;
    let mut centrality = vec![0.0; vertex_count];
    for source in 0..vertex_count {
        guard.step()?;
        let mut predecessors = vec![Vec::<usize>::new(); vertex_count];
        let mut paths = vec![0.0; vertex_count];
        let mut distance = vec![-1i64; vertex_count];
        let mut queue = VecDeque::from([source]);
        let mut stack = Vec::new();
        paths[source] = 1.0;
        distance[source] = 0;
        while let Some(vertex) = queue.pop_front() {
            guard.step()?;
            stack.push(vertex);
            for edge_index in csr.outgoing_range(vertex) {
                guard.step()?;
                let neighbor = csr.neighbors()[edge_index] as usize;
                if distance[neighbor] < 0 {
                    distance[neighbor] = distance[vertex] + 1;
                    queue.push_back(neighbor);
                }
                if distance[neighbor] == distance[vertex] + 1 {
                    paths[neighbor] += paths[vertex];
                    predecessors[neighbor].push(vertex);
                }
            }
        }
        let mut dependency = vec![0.0; vertex_count];
        while let Some(vertex) = stack.pop() {
            guard.step()?;
            for predecessor in predecessors[vertex].iter().copied() {
                guard.step()?;
                if paths[vertex] > 0.0 {
                    dependency[predecessor] +=
                        paths[predecessor] / paths[vertex] * (1.0 + dependency[vertex]);
                }
            }
            if vertex != source {
                centrality[vertex] += dependency[vertex];
            }
        }
    }
    let mut scores = BTreeMap::new();
    for (vertex_id, score) in csr.vertex_ids().iter().copied().zip(centrality) {
        guard.step()?;
        scores.insert(vertex_id, score);
    }
    Ok(AlgorithmResult::Scores(scores))
}

pub(crate) fn triangle_count(
    csr: &SnapshotCsr,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let adjacency = undirected_adjacency(csr, guard)?;
    let mut triangles = 0u64;
    for left in 0..adjacency.len() {
        guard.step()?;
        for middle in adjacency[left].iter().copied() {
            guard.step()?;
            if middle <= left {
                continue;
            }
            for right in adjacency[middle].iter().copied() {
                guard.step()?;
                if right <= middle {
                    continue;
                }
                if adjacency[left].contains(&right) {
                    triangles = triangles
                        .checked_add(1)
                        .ok_or(AnalyticsError::ResourceLimit)?;
                }
            }
        }
    }
    Ok(AlgorithmResult::Count(triangles))
}

pub(crate) fn clustering_coefficient(
    csr: &SnapshotCsr,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let adjacency = undirected_adjacency(csr, guard)?;
    let mut scores = BTreeMap::new();
    for vertex in 0..adjacency.len() {
        guard.step()?;
        let mut neighbors = Vec::with_capacity(adjacency[vertex].len());
        for neighbor in adjacency[vertex].iter().copied() {
            guard.step()?;
            neighbors.push(neighbor);
        }
        let degree = neighbors.len();
        if degree < 2 {
            scores.insert(csr.vertex_ids()[vertex], 0.0);
            continue;
        }
        let mut connected_pairs = 0usize;
        for left in 0..degree {
            for right in left + 1..degree {
                guard.step()?;
                if adjacency[neighbors[left]].contains(&neighbors[right]) {
                    connected_pairs += 1;
                }
            }
        }
        scores.insert(
            csr.vertex_ids()[vertex],
            2.0 * connected_pairs as f64 / (degree * (degree - 1)) as f64,
        );
    }
    Ok(AlgorithmResult::Scores(scores))
}

pub(crate) fn undirected_adjacency(
    csr: &SnapshotCsr,
    guard: &mut ExecutionGuard,
) -> Result<Vec<BTreeSet<usize>>, AnalyticsError> {
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count.saturating_mul(24))?;
    let mut adjacency = vec![BTreeSet::new(); vertex_count];
    for source in 0..vertex_count {
        guard.step()?;
        for edge_index in csr.outgoing_range(source) {
            guard.step()?;
            let target = csr.neighbors()[edge_index] as usize;
            if source != target {
                adjacency[source].insert(target);
                adjacency[target].insert(source);
            }
        }
    }
    Ok(adjacency)
}
