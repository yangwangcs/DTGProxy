use std::collections::BTreeMap;

use dtg_kernel::Value;

use crate::plan::TemporalScope;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Hash)]
pub enum BuiltInAlgorithmId {
    Degree,
    WeaklyConnectedComponents,
    PageRank,
    TriangleCount,
    ShortestPaths,
}

impl TryFrom<&str> for BuiltInAlgorithmId {
    type Error = BuiltInAlgorithmIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "degree" => Ok(Self::Degree),
            "weakly_connected_components" => Ok(Self::WeaklyConnectedComponents),
            "page_rank" => Ok(Self::PageRank),
            "triangle_count" => Ok(Self::TriangleCount),
            "shortest_paths" => Ok(Self::ShortestPaths),
            _ => Err(BuiltInAlgorithmIdError),
        }
    }
}

impl BuiltInAlgorithmId {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Degree => "degree",
            Self::WeaklyConnectedComponents => "weakly_connected_components",
            Self::PageRank => "page_rank",
            Self::TriangleCount => "triangle_count",
            Self::ShortestPaths => "shortest_paths",
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
    pub temporal_scope: TemporalScope,
    pub arguments: BTreeMap<String, Value>,
}
