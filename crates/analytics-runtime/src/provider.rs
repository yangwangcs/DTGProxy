use std::collections::BTreeMap;

use analytics_api::{
    AlgorithmDescriptor, AlgorithmRequest, AlgorithmResult, AlgorithmValue, AnalyticsProvider,
    EventGraph, GraphModel, ProjectedGraph, ProviderDescriptor, ProviderError, SnapshotGraph,
    VertexId,
};

use crate::{
    TemporalPathRequest, TimeOrder, WaitingPolicy, bfs, degree_centrality, earliest_arrival,
    page_rank, scc, sssp, wcc,
};

#[derive(Clone, Copy, Debug, Default)]
pub struct BuiltInProvider;

impl BuiltInProvider {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl AnalyticsProvider for BuiltInProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        ProviderDescriptor::new(
            "dtg-rust-reference",
            env!("CARGO_PKG_VERSION"),
            false,
            false,
        )
    }

    fn algorithms(&self) -> Vec<AlgorithmDescriptor> {
        [
            ("dtg.graph.bfs", GraphModel::Snapshot),
            ("dtg.graph.sssp", GraphModel::Snapshot),
            ("dtg.graph.wcc", GraphModel::Snapshot),
            ("dtg.graph.scc", GraphModel::Snapshot),
            ("dtg.graph.pageRank", GraphModel::Snapshot),
            ("dtg.graph.degree", GraphModel::Snapshot),
            ("dtg.temporal.earliestArrival", GraphModel::Event),
        ]
        .into_iter()
        .map(|(name, model)| {
            AlgorithmDescriptor::new(
                name,
                env!("CARGO_PKG_VERSION"),
                vec![model],
                true,
                true,
                false,
                false,
            )
        })
        .collect()
    }

    fn execute(&self, request: AlgorithmRequest) -> Result<AlgorithmResult, ProviderError> {
        let descriptor = self
            .algorithms()
            .into_iter()
            .find(|descriptor| descriptor.name() == request.algorithm())
            .ok_or_else(|| error("DTG-ANALYTICS-UNKNOWN", "unknown algorithm"))?;
        if !descriptor.graph_models().contains(&request.graph().model()) {
            return Err(error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "algorithm does not support this projected graph model",
            ));
        }
        match request.algorithm() {
            "dtg.graph.bfs" => run_bfs(&request),
            "dtg.graph.sssp" => run_sssp(&request),
            "dtg.graph.wcc" => run_components(&request, false),
            "dtg.graph.scc" => run_components(&request, true),
            "dtg.graph.pageRank" => run_page_rank(&request),
            "dtg.graph.degree" => run_degree(&request),
            "dtg.temporal.earliestArrival" => run_earliest(&request),
            _ => Err(error("DTG-ANALYTICS-UNKNOWN", "unknown algorithm")),
        }
    }
}

fn snapshot(request: &AlgorithmRequest) -> Result<&SnapshotGraph, ProviderError> {
    if let ProjectedGraph::Snapshot(graph) = request.graph() {
        Ok(graph)
    } else {
        Err(error(
            "DTG-ANALYTICS-GRAPH-MODEL",
            "snapshot graph is required",
        ))
    }
}

fn event(request: &AlgorithmRequest) -> Result<&EventGraph, ProviderError> {
    if let ProjectedGraph::Event(graph) = request.graph() {
        Ok(graph)
    } else {
        Err(error(
            "DTG-ANALYTICS-GRAPH-MODEL",
            "event graph is required",
        ))
    }
}

fn source(request: &AlgorithmRequest) -> Result<VertexId, ProviderError> {
    match request.parameters().get("source") {
        Some(AlgorithmValue::Vertex(vertex)) => Ok(*vertex),
        _ => Err(error("DTG-ANALYTICS-PARAMETER", "source must be a Vertex")),
    }
}

fn run_bfs(request: &AlgorithmRequest) -> Result<AlgorithmResult, ProviderError> {
    let graph = snapshot(request)?;
    let result = bfs(graph, source(request)?).map_err(execution_error)?;
    let rows = graph
        .vertices()
        .iter()
        .filter_map(|vertex| {
            result.distance(*vertex).map(|distance| {
                vec![
                    AlgorithmValue::Vertex(*vertex),
                    AlgorithmValue::Integer(i64::try_from(distance).unwrap_or(i64::MAX)),
                    result
                        .predecessor(*vertex)
                        .map_or(AlgorithmValue::Null, AlgorithmValue::Vertex),
                ]
            })
        })
        .collect();
    table(&["vertexId", "distance", "predecessor"], rows)
}

fn run_sssp(request: &AlgorithmRequest) -> Result<AlgorithmResult, ProviderError> {
    let graph = snapshot(request)?;
    let result = sssp(graph, source(request)?).map_err(execution_error)?;
    let rows = graph
        .vertices()
        .iter()
        .filter_map(|vertex| {
            result.distance(*vertex).map(|distance| {
                vec![
                    AlgorithmValue::Vertex(*vertex),
                    AlgorithmValue::FloatBits(distance.to_bits()),
                    result
                        .predecessor(*vertex)
                        .map_or(AlgorithmValue::Null, AlgorithmValue::Vertex),
                ]
            })
        })
        .collect();
    table(&["vertexId", "distance", "predecessor"], rows)
}

fn run_components(
    request: &AlgorithmRequest,
    strong: bool,
) -> Result<AlgorithmResult, ProviderError> {
    let graph = snapshot(request)?;
    let components = if strong { scc(graph) } else { wcc(graph) };
    table(
        &["vertexId", "componentId"],
        components
            .into_iter()
            .map(|(vertex, component)| {
                vec![
                    AlgorithmValue::Vertex(vertex),
                    AlgorithmValue::Vertex(component),
                ]
            })
            .collect(),
    )
}

fn run_page_rank(request: &AlgorithmRequest) -> Result<AlgorithmResult, ProviderError> {
    let damping = float_parameter(request, "damping", 0.85)?;
    let iterations = positive_integer(request, "maxIterations", 100)?;
    let tolerance = float_parameter(request, "tolerance", 1e-9)?;
    let ranks = page_rank(
        snapshot(request)?,
        damping,
        usize::try_from(iterations)
            .map_err(|_| error("DTG-ANALYTICS-PARAMETER", "maxIterations is too large"))?,
        tolerance,
    )
    .map_err(execution_error)?;
    table(
        &["vertexId", "score"],
        ranks
            .into_iter()
            .map(|(vertex, score)| {
                vec![
                    AlgorithmValue::Vertex(vertex),
                    AlgorithmValue::FloatBits(score.to_bits()),
                ]
            })
            .collect(),
    )
}

fn run_degree(request: &AlgorithmRequest) -> Result<AlgorithmResult, ProviderError> {
    table(
        &["vertexId", "inDegree", "outDegree", "degree"],
        degree_centrality(snapshot(request)?)
            .into_iter()
            .map(|(vertex, degree)| {
                vec![
                    AlgorithmValue::Vertex(vertex),
                    integer(degree.incoming()),
                    integer(degree.outgoing()),
                    integer(degree.total()),
                ]
            })
            .collect(),
    )
}

fn run_earliest(request: &AlgorithmRequest) -> Result<AlgorithmResult, ProviderError> {
    let time_order = match request.parameters().get("timeOrder") {
        None => TimeOrder::NonDecreasing,
        Some(AlgorithmValue::String(value)) if value == "NON_DECREASING" => {
            TimeOrder::NonDecreasing
        }
        Some(AlgorithmValue::String(value)) if value == "STRICT" => TimeOrder::Strict,
        _ => {
            return Err(error(
                "DTG-ANALYTICS-PARAMETER",
                "timeOrder must be STRICT or NON_DECREASING",
            ));
        }
    };
    let waiting = match request.parameters().get("waiting") {
        None | Some(AlgorithmValue::Boolean(true)) => WaitingPolicy::Allowed,
        Some(AlgorithmValue::Boolean(false)) => WaitingPolicy::Forbidden,
        _ => {
            return Err(error("DTG-ANALYTICS-PARAMETER", "waiting must be Boolean"));
        }
    };
    let path = TemporalPathRequest::new(
        source(request)?,
        time_parameter(request, "validFrom")?,
        time_parameter(request, "validTo")?,
        time_order,
        waiting,
    )
    .map_err(execution_error)?;
    let result = earliest_arrival(event(request)?, path).map_err(execution_error)?;
    table(
        &["vertexId", "arrivalTime", "predecessor"],
        result
            .arrivals()
            .iter()
            .map(|(vertex, arrival)| {
                vec![
                    AlgorithmValue::Vertex(*vertex),
                    AlgorithmValue::Time(*arrival),
                    result
                        .predecessor(*vertex)
                        .map_or(AlgorithmValue::Null, AlgorithmValue::Vertex),
                ]
            })
            .collect(),
    )
}

fn positive_integer(
    request: &AlgorithmRequest,
    name: &str,
    default: i64,
) -> Result<i64, ProviderError> {
    match request.parameters().get(name) {
        None => Ok(default),
        Some(AlgorithmValue::Integer(value)) if *value > 0 => Ok(*value),
        _ => Err(error(
            "DTG-ANALYTICS-PARAMETER",
            format!("{name} must be a positive Integer"),
        )),
    }
}

fn float_parameter(
    request: &AlgorithmRequest,
    name: &str,
    default: f64,
) -> Result<f64, ProviderError> {
    match request.parameters().get(name) {
        None => Ok(default),
        Some(AlgorithmValue::FloatBits(value)) => Ok(f64::from_bits(*value)),
        Some(AlgorithmValue::Integer(value)) => Ok(*value as f64),
        _ => Err(error(
            "DTG-ANALYTICS-PARAMETER",
            format!("{name} must be numeric"),
        )),
    }
}

fn time_parameter(
    request: &AlgorithmRequest,
    name: &str,
) -> Result<temporal_types::ValidTime, ProviderError> {
    match request.parameters().get(name) {
        Some(AlgorithmValue::Time(value)) => Ok(*value),
        _ => Err(error(
            "DTG-ANALYTICS-PARAMETER",
            format!("{name} must be a temporal instant"),
        )),
    }
}

fn integer(value: u64) -> AlgorithmValue {
    AlgorithmValue::Integer(i64::try_from(value).unwrap_or(i64::MAX))
}

fn table(
    columns: &[&str],
    rows: Vec<Vec<AlgorithmValue>>,
) -> Result<AlgorithmResult, ProviderError> {
    AlgorithmResult::new(
        columns.iter().map(|column| (*column).to_owned()).collect(),
        rows,
        BTreeMap::new(),
    )
}

fn execution_error(error: impl ToString) -> ProviderError {
    ProviderError::new("DTG-ANALYTICS-EXECUTION", error.to_string())
}

fn error(code: impl Into<String>, message: impl Into<String>) -> ProviderError {
    ProviderError::new(code, message)
}
