use std::collections::BTreeMap;

use crate::{LogicalExpr, ReadScope, RowSchema};

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum BuiltInAlgorithmId {
    BreadthFirstSearch,
    DepthFirstSearch,
    BoundedSingleSourceShortestPath,
    BoundedAllPairsShortestPaths,
    StronglyConnectedComponents,
    WeaklyConnectedComponents,
    PageRank,
    DegreeCentrality,
    ClosenessCentrality,
    BetweennessCentrality,
    TriangleCount,
    ClusteringCoefficient,
    KCore,
    LabelPropagation,
    Louvain,
    EarliestArrival,
    LatestDeparture,
    TemporalReachability,
    TemporalMotif,
    ChangePoint,
}

impl TryFrom<&str> for BuiltInAlgorithmId {
    type Error = BuiltInAlgorithmIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "bfs" => Ok(Self::BreadthFirstSearch),
            "dfs" => Ok(Self::DepthFirstSearch),
            "bounded_sssp" => Ok(Self::BoundedSingleSourceShortestPath),
            "bounded_apsp" => Ok(Self::BoundedAllPairsShortestPaths),
            "strongly_connected_components" => Ok(Self::StronglyConnectedComponents),
            "weakly_connected_components" => Ok(Self::WeaklyConnectedComponents),
            "page_rank" => Ok(Self::PageRank),
            "degree_centrality" => Ok(Self::DegreeCentrality),
            "closeness_centrality" => Ok(Self::ClosenessCentrality),
            "betweenness_centrality" => Ok(Self::BetweennessCentrality),
            "triangle_count" => Ok(Self::TriangleCount),
            "clustering_coefficient" => Ok(Self::ClusteringCoefficient),
            "k_core" => Ok(Self::KCore),
            "label_propagation" => Ok(Self::LabelPropagation),
            "louvain" => Ok(Self::Louvain),
            "earliest_arrival" => Ok(Self::EarliestArrival),
            "latest_departure" => Ok(Self::LatestDeparture),
            "temporal_reachability" => Ok(Self::TemporalReachability),
            "temporal_motif" => Ok(Self::TemporalMotif),
            "change_point" => Ok(Self::ChangePoint),
            _ => Err(BuiltInAlgorithmIdError),
        }
    }
}

impl BuiltInAlgorithmId {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BreadthFirstSearch => "bfs",
            Self::DepthFirstSearch => "dfs",
            Self::BoundedSingleSourceShortestPath => "bounded_sssp",
            Self::BoundedAllPairsShortestPaths => "bounded_apsp",
            Self::StronglyConnectedComponents => "strongly_connected_components",
            Self::WeaklyConnectedComponents => "weakly_connected_components",
            Self::PageRank => "page_rank",
            Self::DegreeCentrality => "degree_centrality",
            Self::ClosenessCentrality => "closeness_centrality",
            Self::BetweennessCentrality => "betweenness_centrality",
            Self::TriangleCount => "triangle_count",
            Self::ClusteringCoefficient => "clustering_coefficient",
            Self::KCore => "k_core",
            Self::LabelPropagation => "label_propagation",
            Self::Louvain => "louvain",
            Self::EarliestArrival => "earliest_arrival",
            Self::LatestDeparture => "latest_departure",
            Self::TemporalReachability => "temporal_reachability",
            Self::TemporalMotif => "temporal_motif",
            Self::ChangePoint => "change_point",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BuiltInAlgorithmIdError;

impl std::fmt::Display for BuiltInAlgorithmIdError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("unknown built-in algorithm")
    }
}

impl std::error::Error for BuiltInAlgorithmIdError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AnalyticsSubmission {
    pub algorithm: BuiltInAlgorithmId,
    pub execution_mode: AnalyticsExecutionMode,
    pub read_scope: ReadScope,
    pub arguments: BTreeMap<String, LogicalExpr>,
    pub result_schema: RowSchema,
    pub request_identity: Option<AnalyticsRequestIdentity>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum AnalyticsExecutionMode {
    Asynchronous,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub struct AnalyticsRequestIdentity(String);

impl AnalyticsRequestIdentity {
    pub fn new(value: String) -> Result<Self, AnalyticsRequestIdentityError> {
        (!value.is_empty())
            .then_some(Self(value))
            .ok_or(AnalyticsRequestIdentityError)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AnalyticsRequestIdentityError;

impl std::fmt::Display for AnalyticsRequestIdentityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("analytics request identity must not be empty")
    }
}

impl std::error::Error for AnalyticsRequestIdentityError {}
