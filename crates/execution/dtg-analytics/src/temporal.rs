use std::collections::BTreeMap;

use crate::catalog::{ExecutionGuard, TemporalPathResult};
use crate::{AlgorithmRequest, AlgorithmResult, AnalyticsError, SnapshotCsr};

pub(crate) fn earliest_arrival(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let source = request
        .source
        .and_then(|id| csr.vertex_index(id))
        .ok_or(AnalyticsError::InvalidRequest)?;
    let target = request
        .target
        .and_then(|id| csr.vertex_index(id))
        .ok_or(AnalyticsError::InvalidRequest)?;
    let (departure_property, arrival_property, start, end) = temporal_request(request)?;
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count.saturating_mul(24))?;
    let mut arrivals = vec![i64::MAX; vertex_count];
    let mut settled = vec![false; vertex_count];
    let mut predecessor = vec![None::<(usize, usize)>; vertex_count];
    arrivals[source] = start;
    for _ in 0..vertex_count {
        guard.step()?;
        let mut current = None;
        for vertex in 0..vertex_count {
            guard.step()?;
            if !settled[vertex]
                && arrivals[vertex] != i64::MAX
                && current.is_none_or(|best| (arrivals[vertex], vertex) < (arrivals[best], best))
            {
                current = Some(vertex);
            }
        }
        let Some(vertex) = current else {
            break;
        };
        settled[vertex] = true;
        for edge_index in csr.outgoing_range(vertex) {
            guard.step()?;
            let (departure, arrival) =
                edge_times(csr, edge_index, departure_property, arrival_property)?;
            if departure < arrivals[vertex]
                || departure < start
                || arrival < departure
                || arrival > end
            {
                continue;
            }
            let neighbor = csr.neighbors()[edge_index] as usize;
            let stable_predecessor =
                predecessor[neighbor].is_none_or(|existing| (vertex, edge_index) < existing);
            if arrival < arrivals[neighbor] || (arrival == arrivals[neighbor] && stable_predecessor)
            {
                arrivals[neighbor] = arrival;
                predecessor[neighbor] = Some((vertex, edge_index));
            }
        }
    }
    Ok(AlgorithmResult::TemporalPath(build_earliest_path(
        csr,
        source,
        target,
        arrivals[target],
        &predecessor,
        departure_property,
        guard,
    )?))
}

pub(crate) fn latest_departure(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let source = request
        .source
        .and_then(|id| csr.vertex_index(id))
        .ok_or(AnalyticsError::InvalidRequest)?;
    let target = request
        .target
        .and_then(|id| csr.vertex_index(id))
        .ok_or(AnalyticsError::InvalidRequest)?;
    let (departure_property, arrival_property, start, end) = temporal_request(request)?;
    let reverse = csr.reverse().ok_or(AnalyticsError::MissingReverseCsr)?;
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count.saturating_mul(24))?;
    let mut latest = vec![i64::MIN; vertex_count];
    let mut settled = vec![false; vertex_count];
    let mut successor = vec![None::<(usize, usize)>; vertex_count];
    latest[target] = end;
    for _ in 0..vertex_count {
        guard.step()?;
        let mut current = None;
        for vertex in 0..vertex_count {
            guard.step()?;
            if !settled[vertex]
                && latest[vertex] != i64::MIN
                && current.is_none_or(|best| {
                    latest[vertex] > latest[best]
                        || (latest[vertex] == latest[best] && vertex < best)
                })
            {
                current = Some(vertex);
            }
        }
        let Some(vertex) = current else {
            break;
        };
        settled[vertex] = true;
        let range = csr
            .incoming_range(vertex)
            .ok_or(AnalyticsError::MissingReverseCsr)?;
        for position in range {
            guard.step()?;
            let edge_index = reverse.forward_edge_indices()[position] as usize;
            let predecessor_vertex = reverse.neighbors()[position] as usize;
            let (departure, arrival) =
                edge_times(csr, edge_index, departure_property, arrival_property)?;
            if departure < start || arrival < departure || arrival > latest[vertex] || arrival > end
            {
                continue;
            }
            let stable_successor = successor[predecessor_vertex]
                .is_none_or(|existing| (vertex, edge_index) < existing);
            if departure > latest[predecessor_vertex]
                || (departure == latest[predecessor_vertex] && stable_successor)
            {
                latest[predecessor_vertex] = departure;
                successor[predecessor_vertex] = Some((vertex, edge_index));
            }
        }
    }
    Ok(AlgorithmResult::TemporalPath(build_latest_path(
        csr,
        source,
        target,
        latest[source],
        &successor,
        arrival_property,
        guard,
    )?))
}

pub(crate) fn temporal_reachability(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let source = request
        .source
        .and_then(|id| csr.vertex_index(id))
        .ok_or(AnalyticsError::InvalidRequest)?;
    let (departure_property, arrival_property, start, end) = temporal_request(request)?;
    let vertex_count = csr.vertex_ids().len();
    guard.reserve(vertex_count.saturating_mul(8))?;
    let mut arrivals = vec![i64::MAX; vertex_count];
    arrivals[source] = start;
    for _ in 0..vertex_count {
        guard.step()?;
        let mut changed = false;
        for vertex in 0..vertex_count {
            guard.step()?;
            if arrivals[vertex] == i64::MAX {
                continue;
            }
            for edge_index in csr.outgoing_range(vertex) {
                guard.step()?;
                let (departure, arrival) =
                    edge_times(csr, edge_index, departure_property, arrival_property)?;
                if departure >= arrivals[vertex]
                    && departure >= start
                    && arrival >= departure
                    && arrival <= end
                {
                    let neighbor = csr.neighbors()[edge_index] as usize;
                    if arrival < arrivals[neighbor] {
                        arrivals[neighbor] = arrival;
                        changed = true;
                    }
                }
            }
        }
        if !changed {
            break;
        }
    }
    let mut reachable = Vec::new();
    for (vertex, arrival) in csr.vertex_ids().iter().copied().zip(arrivals) {
        guard.step()?;
        if arrival != i64::MAX {
            reachable.push(vertex);
        }
    }
    Ok(AlgorithmResult::Reachable(reachable))
}

pub(crate) fn temporal_motif(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    let (departure_property, arrival_property, start, end) = temporal_request(request)?;
    let mut count = 0u64;
    for first_source in 0..csr.vertex_ids().len() {
        guard.step()?;
        for first_edge in csr.outgoing_range(first_source) {
            guard.step()?;
            let (first_departure, first_arrival) =
                edge_times(csr, first_edge, departure_property, arrival_property)?;
            if first_departure < start || first_arrival < first_departure || first_arrival > end {
                continue;
            }
            let middle = csr.neighbors()[first_edge] as usize;
            for second_edge in csr.outgoing_range(middle) {
                guard.step()?;
                let (second_departure, second_arrival) =
                    edge_times(csr, second_edge, departure_property, arrival_property)?;
                if second_departure >= first_arrival && second_arrival <= end {
                    count = count.checked_add(1).ok_or(AnalyticsError::ResourceLimit)?;
                }
            }
        }
    }
    Ok(AlgorithmResult::Count(count))
}

pub(crate) fn change_point(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    guard: &mut ExecutionGuard,
) -> Result<AlgorithmResult, AnalyticsError> {
    if !request.change_threshold.is_finite() || request.change_threshold < 0.0 {
        return Err(AnalyticsError::InvalidRequest);
    }
    let time_property = request
        .event_time_property
        .as_deref()
        .ok_or(AnalyticsError::InvalidRequest)?;
    let signal_property = request
        .signal_property
        .as_deref()
        .ok_or(AnalyticsError::InvalidRequest)?;
    let mut events = BTreeMap::new();
    guard.reserve(csr.edge_ids().len().saturating_mul(32))?;
    for edge_index in 0..csr.edge_ids().len() {
        guard.step()?;
        let time = csr
            .edge_integer(time_property, edge_index)
            .ok_or(AnalyticsError::MissingProperty)?;
        let signal = csr
            .edge_number(signal_property, edge_index)
            .ok_or(AnalyticsError::MissingProperty)?;
        if !signal.is_finite() {
            return Err(AnalyticsError::InvalidProperty);
        }
        events.insert((time, csr.edge_ids()[edge_index]), signal);
    }
    let mut changes = Vec::new();
    let mut previous: Option<f64> = None;
    for ((_, edge_id), signal) in events {
        guard.step()?;
        if let Some(previous_signal) = previous
            && (signal - previous_signal).abs() >= request.change_threshold
            && signal != previous_signal
        {
            changes.push(edge_id);
        }
        previous = Some(signal);
    }
    Ok(AlgorithmResult::ChangePoints(changes))
}

fn temporal_request(request: &AlgorithmRequest) -> Result<(&str, &str, i64, i64), AnalyticsError> {
    let departure = request
        .departure_property
        .as_deref()
        .ok_or(AnalyticsError::InvalidRequest)?;
    let arrival = request
        .arrival_property
        .as_deref()
        .ok_or(AnalyticsError::InvalidRequest)?;
    let start = request.time_start.ok_or(AnalyticsError::InvalidRequest)?;
    let end = request.time_end.ok_or(AnalyticsError::InvalidRequest)?;
    if start > end {
        return Err(AnalyticsError::InvalidRequest);
    }
    Ok((departure, arrival, start, end))
}

fn edge_times(
    csr: &SnapshotCsr,
    edge_index: usize,
    departure_property: &str,
    arrival_property: &str,
) -> Result<(i64, i64), AnalyticsError> {
    let departure = csr
        .edge_integer(departure_property, edge_index)
        .ok_or(AnalyticsError::MissingProperty)?;
    let arrival = csr
        .edge_integer(arrival_property, edge_index)
        .ok_or(AnalyticsError::MissingProperty)?;
    Ok((departure, arrival))
}

fn build_earliest_path(
    csr: &SnapshotCsr,
    source: usize,
    target: usize,
    arrival_time: i64,
    predecessor: &[Option<(usize, usize)>],
    departure_property: &str,
    guard: &mut ExecutionGuard,
) -> Result<Option<TemporalPathResult>, AnalyticsError> {
    if arrival_time == i64::MAX {
        return Ok(None);
    }
    let mut vertices = vec![csr.vertex_ids()[target]];
    let mut edge_ids = Vec::new();
    let mut edge_indices = Vec::new();
    let mut current = target;
    while current != source {
        guard.step()?;
        let (previous, edge_index) = predecessor[current].ok_or(AnalyticsError::InvalidRequest)?;
        vertices.push(csr.vertex_ids()[previous]);
        edge_ids.push(csr.edge_ids()[edge_index]);
        edge_indices.push(edge_index);
        current = previous;
    }
    vertices.reverse();
    edge_ids.reverse();
    edge_indices.reverse();
    let departure_time = edge_indices
        .first()
        .and_then(|edge_index| csr.edge_integer(departure_property, *edge_index))
        .unwrap_or(arrival_time);
    Ok(Some(TemporalPathResult {
        vertices,
        edge_ids,
        departure_time,
        arrival_time,
    }))
}

fn build_latest_path(
    csr: &SnapshotCsr,
    source: usize,
    target: usize,
    departure_time: i64,
    successor: &[Option<(usize, usize)>],
    arrival_property: &str,
    guard: &mut ExecutionGuard,
) -> Result<Option<TemporalPathResult>, AnalyticsError> {
    if departure_time == i64::MIN {
        return Ok(None);
    }
    let mut vertices = vec![csr.vertex_ids()[source]];
    let mut edge_ids = Vec::new();
    let mut current = source;
    let mut last_edge = None;
    while current != target {
        guard.step()?;
        let (next, edge_index) = successor[current].ok_or(AnalyticsError::InvalidRequest)?;
        vertices.push(csr.vertex_ids()[next]);
        edge_ids.push(csr.edge_ids()[edge_index]);
        last_edge = Some(edge_index);
        current = next;
    }
    let arrival_time = last_edge
        .and_then(|edge_index| csr.edge_integer(arrival_property, edge_index))
        .unwrap_or(departure_time);
    Ok(Some(TemporalPathResult {
        vertices,
        edge_ids,
        departure_time,
        arrival_time,
    }))
}
