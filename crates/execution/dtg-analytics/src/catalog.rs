use std::collections::BTreeMap;

use dtg_language_ir::BuiltInAlgorithmId;
use dtg_storage::{EdgeId, VertexId};

use crate::SnapshotCsr;
use crate::centrality::{
    betweenness_centrality, closeness_centrality, clustering_coefficient, degree_centrality,
    page_rank, triangle_count,
};
use crate::community::{k_core, label_propagation, louvain};
use crate::components::{strongly_connected_components, weakly_connected_components};
use crate::projection::CancellationToken;
use crate::shortest_path::{bounded_apsp, bounded_sssp};
use crate::temporal::{
    change_point, earliest_arrival, latest_departure, temporal_motif, temporal_reachability,
};
use crate::traversal::{breadth_first_search, depth_first_search};

const IDS: [BuiltInAlgorithmId; 20] = [
    BuiltInAlgorithmId::BreadthFirstSearch,
    BuiltInAlgorithmId::DepthFirstSearch,
    BuiltInAlgorithmId::BoundedSingleSourceShortestPath,
    BuiltInAlgorithmId::BoundedAllPairsShortestPaths,
    BuiltInAlgorithmId::StronglyConnectedComponents,
    BuiltInAlgorithmId::WeaklyConnectedComponents,
    BuiltInAlgorithmId::PageRank,
    BuiltInAlgorithmId::DegreeCentrality,
    BuiltInAlgorithmId::ClosenessCentrality,
    BuiltInAlgorithmId::BetweennessCentrality,
    BuiltInAlgorithmId::TriangleCount,
    BuiltInAlgorithmId::ClusteringCoefficient,
    BuiltInAlgorithmId::KCore,
    BuiltInAlgorithmId::LabelPropagation,
    BuiltInAlgorithmId::Louvain,
    BuiltInAlgorithmId::EarliestArrival,
    BuiltInAlgorithmId::LatestDeparture,
    BuiltInAlgorithmId::TemporalReachability,
    BuiltInAlgorithmId::TemporalMotif,
    BuiltInAlgorithmId::ChangePoint,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CsrShape {
    Forward,
    ForwardAndReverse,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DeterministicTieBreak {
    StableVertexThenEdgeId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AlgorithmDescriptor {
    pub id: BuiltInAlgorithmId,
    pub required_shape: CsrShape,
    pub required_edge_properties: &'static [&'static str],
    pub deterministic: bool,
    pub tie_break: DeterministicTieBreak,
    pub max_memory_bytes: usize,
    pub max_iterations: u32,
    pub cancellation_check_interval: u32,
    pub supports_distributed_partial: bool,
    pub result_schema_version: u32,
    pub checkpoint_schema_version: u32,
}

pub struct AlgorithmCatalog;

impl AlgorithmCatalog {
    pub const fn ids() -> &'static [BuiltInAlgorithmId; 20] {
        &IDS
    }

    pub fn descriptor(id: BuiltInAlgorithmId) -> AlgorithmDescriptor {
        let (required_shape, required_edge_properties, supports_distributed_partial) = match id {
            BuiltInAlgorithmId::StronglyConnectedComponents
            | BuiltInAlgorithmId::WeaklyConnectedComponents
            | BuiltInAlgorithmId::DegreeCentrality
            | BuiltInAlgorithmId::ClusteringCoefficient
            | BuiltInAlgorithmId::KCore
            | BuiltInAlgorithmId::LabelPropagation
            | BuiltInAlgorithmId::Louvain => (CsrShape::ForwardAndReverse, &[][..], false),
            BuiltInAlgorithmId::LatestDeparture => (
                CsrShape::ForwardAndReverse,
                &["departure", "arrival"][..],
                false,
            ),
            BuiltInAlgorithmId::EarliestArrival
            | BuiltInAlgorithmId::TemporalReachability
            | BuiltInAlgorithmId::TemporalMotif => {
                (CsrShape::Forward, &["departure", "arrival"][..], false)
            }
            BuiltInAlgorithmId::ChangePoint => {
                (CsrShape::Forward, &["event_time", "signal"][..], true)
            }
            BuiltInAlgorithmId::BoundedSingleSourceShortestPath
            | BuiltInAlgorithmId::BoundedAllPairsShortestPaths => {
                (CsrShape::Forward, &["weight"][..], false)
            }
            BuiltInAlgorithmId::PageRank
            | BuiltInAlgorithmId::TriangleCount
            | BuiltInAlgorithmId::BetweennessCentrality
            | BuiltInAlgorithmId::ClosenessCentrality
            | BuiltInAlgorithmId::BreadthFirstSearch
            | BuiltInAlgorithmId::DepthFirstSearch => (CsrShape::Forward, &[][..], false),
        };
        AlgorithmDescriptor {
            id,
            required_shape,
            required_edge_properties,
            deterministic: true,
            tie_break: DeterministicTieBreak::StableVertexThenEdgeId,
            max_memory_bytes: 256 * 1024 * 1024,
            max_iterations: 10_000,
            cancellation_check_interval: 1,
            supports_distributed_partial,
            result_schema_version: 1,
            checkpoint_schema_version: 1,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct AlgorithmRequest {
    pub algorithm: BuiltInAlgorithmId,
    pub source: Option<VertexId>,
    pub target: Option<VertexId>,
    pub max_depth: u32,
    pub max_pairs: usize,
    pub iterations: u32,
    pub k: u32,
    pub damping: f64,
    pub weight_property: Option<String>,
    pub departure_property: Option<String>,
    pub arrival_property: Option<String>,
    pub event_time_property: Option<String>,
    pub signal_property: Option<String>,
    pub time_start: Option<i64>,
    pub time_end: Option<i64>,
    pub change_threshold: f64,
}

impl AlgorithmRequest {
    pub fn new(algorithm: BuiltInAlgorithmId) -> Self {
        Self {
            algorithm,
            source: None,
            target: None,
            max_depth: 64,
            max_pairs: 4_096,
            iterations: 20,
            k: 2,
            damping: 0.85,
            weight_property: None,
            departure_property: None,
            arrival_property: None,
            event_time_property: None,
            signal_property: None,
            time_start: None,
            time_end: None,
            change_threshold: 0.0,
        }
    }
}

#[derive(Clone, Debug)]
pub struct AlgorithmBudget {
    max_memory_bytes: usize,
    max_iterations: u32,
    max_work_items: u64,
    cancellation: CancellationToken,
}

impl AlgorithmBudget {
    pub const fn new(
        max_memory_bytes: usize,
        max_iterations: u32,
        max_work_items: u64,
        cancellation: CancellationToken,
    ) -> Self {
        Self {
            max_memory_bytes,
            max_iterations,
            max_work_items,
            cancellation,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TemporalPathResult {
    pub vertices: Vec<VertexId>,
    pub edge_ids: Vec<EdgeId>,
    pub departure_time: i64,
    pub arrival_time: i64,
}

#[derive(Clone, Debug, PartialEq)]
pub enum AlgorithmResult {
    Traversal(Vec<VertexId>),
    Distances(BTreeMap<VertexId, f64>),
    AllPairs(BTreeMap<(VertexId, VertexId), f64>),
    Components(Vec<Vec<VertexId>>),
    Scores(BTreeMap<VertexId, f64>),
    Count(u64),
    CoreNumbers(BTreeMap<VertexId, u32>),
    Communities(BTreeMap<VertexId, u64>),
    TemporalPath(Option<TemporalPathResult>),
    Reachable(Vec<VertexId>),
    ChangePoints(Vec<EdgeId>),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AnalyticsError {
    Cancelled,
    ResourceLimit,
    IterationLimit,
    InvalidRequest,
    MissingProperty,
    InvalidProperty,
    MissingReverseCsr,
}

impl AnalyticsError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::Cancelled => "DTG-ANALYTICS-CANCELLED",
            Self::ResourceLimit => "DTG-ANALYTICS-RESOURCE-LIMIT",
            Self::IterationLimit => "DTG-ANALYTICS-ITERATION-LIMIT",
            Self::InvalidRequest => "DTG-ANALYTICS-INVALID-REQUEST",
            Self::MissingProperty => "DTG-ANALYTICS-PROPERTY-MISSING",
            Self::InvalidProperty => "DTG-ANALYTICS-PROPERTY-INVALID",
            Self::MissingReverseCsr => "DTG-ANALYTICS-REVERSE-REQUIRED",
        }
    }
}

impl std::fmt::Display for AnalyticsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for AnalyticsError {}

pub fn run_builtin(
    csr: &SnapshotCsr,
    request: &AlgorithmRequest,
    budget: AlgorithmBudget,
) -> Result<AlgorithmResult, AnalyticsError> {
    let mut guard = ExecutionGuard::new(budget)?;
    let base_memory = csr
        .vertex_ids()
        .len()
        .checked_mul(128)
        .and_then(|bytes| {
            csr.edge_ids()
                .len()
                .checked_mul(64)
                .and_then(|edge_bytes| bytes.checked_add(edge_bytes))
        })
        .ok_or(AnalyticsError::ResourceLimit)?;
    guard.reserve(base_memory)?;
    let descriptor = AlgorithmCatalog::descriptor(request.algorithm);
    if descriptor.required_shape == CsrShape::ForwardAndReverse && csr.reverse().is_none() {
        return Err(AnalyticsError::MissingReverseCsr);
    }

    match request.algorithm {
        BuiltInAlgorithmId::BreadthFirstSearch => breadth_first_search(csr, request, &mut guard),
        BuiltInAlgorithmId::DepthFirstSearch => depth_first_search(csr, request, &mut guard),
        BuiltInAlgorithmId::BoundedSingleSourceShortestPath => {
            bounded_sssp(csr, request, &mut guard)
        }
        BuiltInAlgorithmId::BoundedAllPairsShortestPaths => bounded_apsp(csr, request, &mut guard),
        BuiltInAlgorithmId::StronglyConnectedComponents => {
            strongly_connected_components(csr, &mut guard)
        }
        BuiltInAlgorithmId::WeaklyConnectedComponents => {
            weakly_connected_components(csr, &mut guard)
        }
        BuiltInAlgorithmId::PageRank => page_rank(csr, request, &mut guard),
        BuiltInAlgorithmId::DegreeCentrality => degree_centrality(csr, &mut guard),
        BuiltInAlgorithmId::ClosenessCentrality => closeness_centrality(csr, &mut guard),
        BuiltInAlgorithmId::BetweennessCentrality => betweenness_centrality(csr, &mut guard),
        BuiltInAlgorithmId::TriangleCount => triangle_count(csr, &mut guard),
        BuiltInAlgorithmId::ClusteringCoefficient => clustering_coefficient(csr, &mut guard),
        BuiltInAlgorithmId::KCore => k_core(csr, &mut guard),
        BuiltInAlgorithmId::LabelPropagation => label_propagation(csr, request, &mut guard),
        BuiltInAlgorithmId::Louvain => louvain(csr, request, &mut guard),
        BuiltInAlgorithmId::EarliestArrival => earliest_arrival(csr, request, &mut guard),
        BuiltInAlgorithmId::LatestDeparture => latest_departure(csr, request, &mut guard),
        BuiltInAlgorithmId::TemporalReachability => temporal_reachability(csr, request, &mut guard),
        BuiltInAlgorithmId::TemporalMotif => temporal_motif(csr, request, &mut guard),
        BuiltInAlgorithmId::ChangePoint => change_point(csr, request, &mut guard),
    }
}

pub(crate) struct ExecutionGuard {
    budget: AlgorithmBudget,
    memory_bytes: usize,
    work_items: u64,
}

impl ExecutionGuard {
    fn new(budget: AlgorithmBudget) -> Result<Self, AnalyticsError> {
        if budget.cancellation.is_cancelled() {
            return Err(AnalyticsError::Cancelled);
        }
        Ok(Self {
            budget,
            memory_bytes: 0,
            work_items: 0,
        })
    }

    pub(crate) fn checkpoint(&self) -> Result<(), AnalyticsError> {
        if self.budget.cancellation.is_cancelled() {
            Err(AnalyticsError::Cancelled)
        } else {
            Ok(())
        }
    }

    pub(crate) fn step(&mut self) -> Result<(), AnalyticsError> {
        self.checkpoint()?;
        self.work_items = self
            .work_items
            .checked_add(1)
            .ok_or(AnalyticsError::ResourceLimit)?;
        if self.work_items > self.budget.max_work_items {
            return Err(AnalyticsError::ResourceLimit);
        }
        Ok(())
    }

    pub(crate) fn reserve(&mut self, bytes: usize) -> Result<(), AnalyticsError> {
        self.checkpoint()?;
        self.memory_bytes = self
            .memory_bytes
            .checked_add(bytes)
            .ok_or(AnalyticsError::ResourceLimit)?;
        if self.memory_bytes > self.budget.max_memory_bytes {
            return Err(AnalyticsError::ResourceLimit);
        }
        Ok(())
    }

    pub(crate) fn require_iterations(&self, iterations: u32) -> Result<(), AnalyticsError> {
        self.checkpoint()?;
        if iterations == 0 || iterations > self.budget.max_iterations {
            return Err(AnalyticsError::IterationLimit);
        }
        Ok(())
    }
}
