use std::collections::{BTreeMap, BTreeSet};

use analytics_api::{
    AlgorithmDescriptor, AlgorithmRequest, AlgorithmValue, AnalyticsOutput, AnalyticsProvider,
    DeltaGraph, DeltaKind, EventGraph, IntervalGraph, PartitionedSnapshotGraph, ProjectedGraph,
    ProviderCheckpoint, ProviderDescriptor, ProviderError, ProviderSlice, SnapshotGraph, VertexId,
    builtin_algorithm_descriptors,
};

use crate::{
    AlgorithmError, DeltaEntityType, MAX_ALL_PAIRS_WORK, MAX_CENTRALITY_WORK, TemporalPathRequest,
    TimeOrder, WaitingPolicy, all_pairs_shortest_paths_cancellable,
    betweenness_centrality_cancellable, bfs_cancellable, change_point_scores_cancellable,
    closeness_centrality_cancellable, clustering_coefficient, degree_centrality,
    delta_summary_cancellable, dfs_cancellable, earliest_arrival, interval_components_cancellable,
    k_core, label_propagation_cancellable, latest_departure, louvain_communities_cancellable,
    min_hop_temporal_path, page_rank_cancellable, partitioned_degree_centrality,
    partitioned_page_rank_cancellable, partitioned_wcc_cancellable, scc_cancellable,
    sssp_cancellable, temporal_motif_count_cancellable, temporal_reachability, triangle_count,
    wcc_cancellable, windowed_components_cancellable, windowed_triangle_count_cancellable,
};

const MAX_GATHERED_SNAPSHOT_VERTICES: usize = 1_000_000;
const MAX_GATHERED_SNAPSHOT_EDGES: usize = 1_000_000;
const WCC_STATE_MAGIC: [u8; 4] = *b"WCC1";
const PAGERANK_STATE_MAGIC: [u8; 4] = *b"PRK1";
const STATE_VERSION: u16 = 1;

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
        ProviderDescriptor::new("dtg-rust-reference", env!("CARGO_PKG_VERSION"), true, false)
    }

    fn algorithms(&self) -> Vec<AlgorithmDescriptor> {
        builtin_algorithm_descriptors()
    }

    fn checkpoint(
        &self,
        request: &AlgorithmRequest,
        completed_units: u64,
    ) -> Result<ProviderCheckpoint, ProviderError> {
        match request.algorithm() {
            "dtg.graph.degree" => {
                ProviderCheckpoint::new(request.algorithm(), completed_units, Vec::new())
            }
            "dtg.graph.wcc" => {
                if completed_units != 0 {
                    return Err(error(
                        "DTG-ANALYTICS-CHECKPOINT-STATE",
                        "WCC checkpoints after the initial slice must carry provider state",
                    ));
                }
                ProviderCheckpoint::new(
                    request.algorithm(),
                    0,
                    encode_wcc_state(request, 0, &initial_wcc_labels(request)?)?,
                )
            }
            "dtg.graph.pageRank" => {
                if completed_units != 0 {
                    return Err(error(
                        "DTG-ANALYTICS-CHECKPOINT-STATE",
                        "PageRank checkpoints after the initial slice must carry provider state",
                    ));
                }
                let (damping, max_iterations, tolerance) = page_rank_configuration(request)?;
                let ranks = initial_page_rank(request)?;
                ProviderCheckpoint::new(
                    request.algorithm(),
                    0,
                    encode_page_rank_state(
                        request,
                        0,
                        false,
                        damping,
                        max_iterations,
                        tolerance,
                        &ranks,
                    )?,
                )
            }
            _ => Err(error(
                "DTG-ANALYTICS-CHECKPOINT-UNSUPPORTED",
                "built-in provider only supports checkpointing Degree, WCC, and PageRank",
            )),
        }
    }

    fn restore_checkpoint(
        &self,
        request: &AlgorithmRequest,
        checkpoint: &ProviderCheckpoint,
    ) -> Result<u64, ProviderError> {
        if checkpoint.api_version() != ProviderCheckpoint::CURRENT_API_VERSION {
            return Err(error(
                "DTG-ANALYTICS-CHECKPOINT-VERSION",
                "checkpoint API version is not supported",
            ));
        }
        if checkpoint.algorithm() != request.algorithm() {
            return Err(error(
                "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
                "checkpoint does not match the requested built-in algorithm",
            ));
        }
        match request.algorithm() {
            "dtg.graph.degree" => Ok(checkpoint.completed_units()),
            "dtg.graph.wcc" => {
                let state = decode_wcc_state(request, checkpoint.payload())?;
                if state.iteration != checkpoint.completed_units() {
                    return Err(error(
                        "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
                        "WCC checkpoint cursor does not match its state",
                    ));
                }
                Ok(state.iteration)
            }
            "dtg.graph.pageRank" => {
                let state = decode_page_rank_state(request, checkpoint.payload())?;
                if state.iteration != checkpoint.completed_units() {
                    return Err(error(
                        "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
                        "PageRank checkpoint cursor does not match its state",
                    ));
                }
                Ok(state.iteration)
            }
            _ => Err(error(
                "DTG-ANALYTICS-CHECKPOINT-UNSUPPORTED",
                "built-in provider does not restore this algorithm",
            )),
        }
    }

    fn execute_slice(
        &self,
        request: &AlgorithmRequest,
        start_unit: u64,
        max_units: u64,
        output: &mut dyn AnalyticsOutput,
    ) -> Result<ProviderSlice, ProviderError> {
        if max_units == 0 {
            return Err(error(
                "DTG-ANALYTICS-SLICE-LIMIT",
                "execution slice size must be positive",
            ));
        }
        if request.algorithm() == "dtg.graph.wcc" {
            return execute_wcc_slice(request, start_unit, max_units, output);
        }
        if request.algorithm() == "dtg.graph.pageRank" {
            return execute_page_rank_slice(request, start_unit, max_units, output);
        }
        if request.algorithm() != "dtg.graph.degree" {
            return Err(error(
                "DTG-ANALYTICS-SLICES-UNSUPPORTED",
                "built-in execution slices support Degree, WCC, and PageRank",
            ));
        }
        let degrees = match request.graph() {
            ProjectedGraph::Snapshot(graph) => degree_centrality(graph),
            ProjectedGraph::PartitionedSnapshot(graph) => partitioned_degree_centrality(graph),
            _ => {
                return Err(error(
                    "DTG-ANALYTICS-GRAPH-MODEL",
                    "snapshot graph is required",
                ));
            }
        };
        let start = usize::try_from(start_unit).map_err(|_| {
            error(
                "DTG-ANALYTICS-SLICE-STATE",
                "slice cursor overflows platform",
            )
        })?;
        let limit = usize::try_from(max_units)
            .map_err(|_| error("DTG-ANALYTICS-SLICE-LIMIT", "slice size overflows platform"))?;
        let vertices = degrees.into_iter().collect::<Vec<_>>();
        if start > vertices.len() {
            return Err(error(
                "DTG-ANALYTICS-SLICE-STATE",
                "slice cursor is beyond the deterministic vertex order",
            ));
        }
        let end = start.saturating_add(limit).min(vertices.len());
        table(
            output,
            &["vertexId", "inDegree", "outDegree", "degree"],
            vertices[start..end].iter().map(|(vertex, degree)| {
                vec![
                    AlgorithmValue::Vertex(*vertex),
                    integer(degree.incoming()),
                    integer(degree.outgoing()),
                    integer(degree.total()),
                ]
            }),
        )?;
        Ok(ProviderSlice::new(
            u64::try_from(end)
                .map_err(|_| error("DTG-ANALYTICS-SLICE-STATE", "slice cursor overflows u64"))?,
            end == vertices.len(),
        ))
    }

    fn execute_slice_from_checkpoint(
        &self,
        request: &AlgorithmRequest,
        checkpoint: &ProviderCheckpoint,
        max_units: u64,
        output: &mut dyn AnalyticsOutput,
    ) -> Result<ProviderSlice, ProviderError> {
        if request.algorithm() == "dtg.graph.wcc" {
            return execute_wcc_slice_from_checkpoint(request, checkpoint, max_units, output);
        }
        if request.algorithm() == "dtg.graph.pageRank" {
            return execute_page_rank_slice_from_checkpoint(request, checkpoint, max_units, output);
        }
        let start_unit = self.restore_checkpoint(request, checkpoint)?;
        self.execute_slice(request, start_unit, max_units, output)
    }

    fn execute_into(
        &self,
        request: AlgorithmRequest,
        output: &mut dyn AnalyticsOutput,
    ) -> Result<(), ProviderError> {
        ensure_not_canceled(&request)?;
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
        let request = gather_partitioned_snapshot_if_needed(request)?;
        ensure_not_canceled(&request)?;
        match request.algorithm() {
            "dtg.graph.bfs" => run_bfs(&request, output),
            "dtg.graph.dfs" => run_dfs(&request, output),
            "dtg.graph.sssp" => run_sssp(&request, output),
            "dtg.graph.allPairsShortestPath" => run_all_pairs_shortest_path(&request, output),
            "dtg.graph.wcc" => run_components(&request, false, output),
            "dtg.graph.scc" => run_components(&request, true, output),
            "dtg.graph.pageRank" => run_page_rank(&request, output),
            "dtg.graph.degree" => run_degree(&request, output),
            "dtg.graph.triangleCount" => run_triangle_count(&request, output),
            "dtg.graph.clusteringCoefficient" => run_clustering(&request, output),
            "dtg.graph.betweenness" => run_betweenness(&request, output),
            "dtg.graph.closeness" => run_closeness(&request, output),
            "dtg.graph.kCore" => run_k_core(&request, output),
            "dtg.graph.labelPropagation" => run_label_propagation(&request, output),
            "dtg.graph.louvain" => run_louvain(&request, output),
            "dtg.temporal.earliestArrival" => run_earliest(&request, output),
            "dtg.temporal.reachability" => run_reachability(&request, output),
            "dtg.temporal.minHop" => run_min_hop(&request, output),
            "dtg.temporal.latestDeparture" => run_latest_departure(&request, output),
            "dtg.temporal.fastestPath" => run_fastest_path(&request, output),
            "dtg.temporal.degree" => run_temporal_degree(&request, output),
            "dtg.temporal.closeness" => run_temporal_closeness(&request, output),
            "dtg.temporal.betweenness" => run_temporal_betweenness(&request, output),
            "dtg.temporal.pageRank" => run_temporal_page_rank(&request, output),
            "dtg.temporal.burstiness" => run_temporal_burstiness(&request, output),
            "dtg.temporal.clusteringCoefficient" => run_temporal_clustering(&request, output),
            "dtg.temporal.topologicalOverlap" => run_topological_overlap(&request, output),
            "dtg.temporal.windowedComponents" => run_windowed_components(&request, output),
            "dtg.temporal.windowedTriangleCount" => run_windowed_triangle_count(&request, output),
            "dtg.temporal.changePoint" => run_change_point(&request, output),
            "dtg.temporal.motifCount" => run_temporal_motif_count(&request, output),
            "dtg.temporal.intervalComponents" => run_interval_components(&request, output),
            "dtg.temporal.deltaSummary" => run_delta_summary(&request, output),
            _ => Err(error("DTG-ANALYTICS-UNKNOWN", "unknown algorithm")),
        }
    }
}

#[derive(Clone, Debug)]
struct WccState {
    iteration: u64,
    labels: BTreeMap<VertexId, VertexId>,
}

#[derive(Clone, Debug)]
struct PageRankState {
    iteration: u64,
    converged: bool,
    damping: f64,
    max_iterations: u64,
    tolerance: f64,
    ranks: BTreeMap<VertexId, f64>,
}

fn fnv_bytes(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x100000001b3)
    })
}

fn graph_fingerprint(request: &AlgorithmRequest) -> Result<u64, ProviderError> {
    let mut bytes = Vec::new();
    match request.graph() {
        ProjectedGraph::Snapshot(graph) => {
            bytes.push(u8::from(graph.directed()));
            for vertex in graph.vertices() {
                bytes.extend_from_slice(&vertex.value().to_be_bytes());
            }
            for edge in graph.edges() {
                bytes.extend_from_slice(&edge.source().value().to_be_bytes());
                bytes.extend_from_slice(&edge.destination().value().to_be_bytes());
                bytes.extend_from_slice(&edge.weight().to_bits().to_be_bytes());
            }
        }
        ProjectedGraph::PartitionedSnapshot(graph) => {
            bytes.push(u8::from(graph.directed()));
            for vertex in graph.vertices() {
                bytes.extend_from_slice(&vertex.value().to_be_bytes());
            }
            for partition in graph.partitions() {
                bytes.extend_from_slice(&partition.shard_id().to_be_bytes());
                for edge in partition.edges() {
                    bytes.extend_from_slice(&edge.source().value().to_be_bytes());
                    bytes.extend_from_slice(&edge.destination().value().to_be_bytes());
                    bytes.extend_from_slice(&edge.weight().to_bits().to_be_bytes());
                }
            }
        }
        _ => {
            return Err(error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "snapshot graph is required",
            ));
        }
    }
    Ok(fnv_bytes(&bytes))
}

fn initial_wcc_labels(
    request: &AlgorithmRequest,
) -> Result<BTreeMap<VertexId, VertexId>, ProviderError> {
    let vertices = match request.graph() {
        ProjectedGraph::Snapshot(graph) => graph.vertices(),
        ProjectedGraph::PartitionedSnapshot(graph) => graph.vertices(),
        _ => {
            return Err(error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "snapshot graph is required",
            ));
        }
    };
    Ok(vertices
        .iter()
        .copied()
        .map(|vertex| (vertex, vertex))
        .collect())
}

fn encode_wcc_state(
    request: &AlgorithmRequest,
    iteration: u64,
    labels: &BTreeMap<VertexId, VertexId>,
) -> Result<Vec<u8>, ProviderError> {
    let mut bytes = Vec::with_capacity(32 + labels.len() * 32);
    bytes.extend_from_slice(&WCC_STATE_MAGIC);
    bytes.extend_from_slice(&STATE_VERSION.to_be_bytes());
    bytes.extend_from_slice(&graph_fingerprint(request)?.to_be_bytes());
    bytes.extend_from_slice(&iteration.to_be_bytes());
    bytes.extend_from_slice(
        &u64::try_from(labels.len())
            .map_err(|_| error("DTG-ANALYTICS-CHECKPOINT-STATE", "WCC state is too large"))?
            .to_be_bytes(),
    );
    for (vertex, label) in labels {
        bytes.extend_from_slice(&vertex.value().to_be_bytes());
        bytes.extend_from_slice(&label.value().to_be_bytes());
    }
    let checksum = fnv_bytes(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    Ok(bytes)
}

fn decode_wcc_state(request: &AlgorithmRequest, bytes: &[u8]) -> Result<WccState, ProviderError> {
    if bytes.len() < 4 + 2 + 8 + 8 + 8 + 8 || bytes[..4] != WCC_STATE_MAGIC {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "invalid WCC checkpoint state",
        ));
    }
    if u16::from_be_bytes([bytes[4], bytes[5]]) != STATE_VERSION {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-VERSION",
            "unsupported WCC checkpoint state version",
        ));
    }
    let checksum_offset = bytes.len() - 8;
    let expected = u64::from_be_bytes(bytes[checksum_offset..].try_into().expect("fixed checksum"));
    if fnv_bytes(&bytes[..checksum_offset]) != expected {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "WCC checkpoint state checksum mismatch",
        ));
    }
    let graph_fp = u64::from_be_bytes(bytes[6..14].try_into().expect("fixed graph fingerprint"));
    if graph_fp != graph_fingerprint(request)? {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
            "WCC checkpoint graph does not match the request",
        ));
    }
    let iteration = u64::from_be_bytes(bytes[14..22].try_into().expect("fixed iteration"));
    let count = usize::try_from(u64::from_be_bytes(
        bytes[22..30].try_into().expect("fixed count"),
    ))
    .map_err(|_| {
        error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "WCC checkpoint state count overflows platform",
        )
    })?;
    let expected_len = 4 + 2 + 8 + 8 + 8 + count.saturating_mul(32) + 8;
    if expected_len != bytes.len() || count > MAX_GATHERED_SNAPSHOT_VERTICES {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "invalid WCC checkpoint state length",
        ));
    }
    let mut labels = BTreeMap::new();
    let mut offset = 30;
    for _ in 0..count {
        let vertex = VertexId::new(u128::from_be_bytes(
            bytes[offset..offset + 16].try_into().expect("fixed vertex"),
        ));
        let label = VertexId::new(u128::from_be_bytes(
            bytes[offset + 16..offset + 32]
                .try_into()
                .expect("fixed label"),
        ));
        offset += 32;
        if labels.insert(vertex, label).is_some() {
            return Err(error(
                "DTG-ANALYTICS-CHECKPOINT-STATE",
                "duplicate vertex in WCC checkpoint state",
            ));
        }
    }
    if labels.keys().copied().collect::<Vec<_>>()
        != match request.graph() {
            ProjectedGraph::Snapshot(graph) => graph.vertices().to_vec(),
            ProjectedGraph::PartitionedSnapshot(graph) => graph.vertices().to_vec(),
            _ => Vec::new(),
        }
    {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
            "WCC checkpoint vertices do not match the request",
        ));
    }
    Ok(WccState { iteration, labels })
}

fn wcc_iteration(
    request: &AlgorithmRequest,
    labels: &mut BTreeMap<VertexId, VertexId>,
) -> Result<bool, ProviderError> {
    let previous = labels.clone();
    match request.graph() {
        ProjectedGraph::Snapshot(graph) => {
            for edge in graph.edges() {
                let component = labels[&edge.source()].min(labels[&edge.destination()]);
                labels.insert(edge.source(), component);
                labels.insert(edge.destination(), component);
            }
            for vertex in graph.vertices() {
                let parent = labels[vertex];
                let root = labels[&parent];
                labels.insert(*vertex, root);
            }
        }
        ProjectedGraph::PartitionedSnapshot(graph) => {
            let mut updates = BTreeMap::<VertexId, VertexId>::new();
            for partition in graph.partitions() {
                for edge in partition.edges() {
                    let label = previous[&edge.source()].min(previous[&edge.destination()]);
                    updates
                        .entry(edge.source())
                        .and_modify(|current| *current = (*current).min(label))
                        .or_insert(label);
                    updates
                        .entry(edge.destination())
                        .and_modify(|current| *current = (*current).min(label))
                        .or_insert(label);
                }
            }
            for (vertex, label) in updates {
                labels
                    .entry(vertex)
                    .and_modify(|current| *current = (*current).min(label));
            }
        }
        _ => {
            return Err(error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "snapshot graph is required",
            ));
        }
    }
    Ok(*labels != previous)
}

fn emit_wcc(
    labels: &BTreeMap<VertexId, VertexId>,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    table(
        output,
        &["vertexId", "componentId"],
        labels.iter().map(|(vertex, component)| {
            vec![
                AlgorithmValue::Vertex(*vertex),
                AlgorithmValue::Vertex(*component),
            ]
        }),
    )
}

fn execute_wcc_slice(
    request: &AlgorithmRequest,
    start_unit: u64,
    max_units: u64,
    output: &mut dyn AnalyticsOutput,
) -> Result<ProviderSlice, ProviderError> {
    if start_unit != 0 {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "WCC continuation requires a provider checkpoint",
        ));
    }
    let labels = initial_wcc_labels(request)?;
    execute_wcc_state_slice(
        request,
        WccState {
            iteration: 0,
            labels,
        },
        max_units,
        output,
    )
}

fn execute_wcc_slice_from_checkpoint(
    request: &AlgorithmRequest,
    checkpoint: &ProviderCheckpoint,
    max_units: u64,
    output: &mut dyn AnalyticsOutput,
) -> Result<ProviderSlice, ProviderError> {
    let state = decode_wcc_state(request, checkpoint.payload())?;
    if state.iteration != checkpoint.completed_units() {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
            "WCC checkpoint cursor does not match its state",
        ));
    }
    execute_wcc_state_slice(request, state, max_units, output)
}

fn execute_wcc_state_slice(
    request: &AlgorithmRequest,
    mut state: WccState,
    max_units: u64,
    output: &mut dyn AnalyticsOutput,
) -> Result<ProviderSlice, ProviderError> {
    for _ in 0..max_units {
        ensure_not_canceled(request)?;
        let changed = wcc_iteration(request, &mut state.labels)?;
        state.iteration = state.iteration.saturating_add(1);
        if !changed {
            emit_wcc(&state.labels, output)?;
            return Ok(
                ProviderSlice::new(state.iteration, true).with_checkpoint_payload(
                    encode_wcc_state(request, state.iteration, &state.labels)?,
                ),
            );
        }
    }
    Ok(
        ProviderSlice::new(state.iteration, false).with_checkpoint_payload(encode_wcc_state(
            request,
            state.iteration,
            &state.labels,
        )?),
    )
}

fn page_rank_configuration(request: &AlgorithmRequest) -> Result<(f64, u64, f64), ProviderError> {
    let damping = float_parameter(request, "damping", 0.85)?;
    let max_iterations = u64::try_from(positive_integer(request, "maxIterations", 100)?)
        .map_err(|_| error("DTG-ANALYTICS-PARAMETER", "maxIterations is too large"))?;
    let tolerance = float_parameter(request, "tolerance", 1e-9)?;
    if !(0.0..1.0).contains(&damping) || !tolerance.is_finite() || tolerance <= 0.0 {
        return Err(error(
            "DTG-ANALYTICS-PARAMETER",
            "invalid PageRank configuration",
        ));
    }
    Ok((damping, max_iterations, tolerance))
}

fn page_rank_parameter_fingerprint(damping: f64, max_iterations: u64, tolerance: f64) -> u64 {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&damping.to_bits().to_be_bytes());
    bytes.extend_from_slice(&max_iterations.to_be_bytes());
    bytes.extend_from_slice(&tolerance.to_bits().to_be_bytes());
    fnv_bytes(&bytes)
}

fn initial_page_rank(request: &AlgorithmRequest) -> Result<BTreeMap<VertexId, f64>, ProviderError> {
    let vertices = match request.graph() {
        ProjectedGraph::Snapshot(graph) => graph.vertices(),
        ProjectedGraph::PartitionedSnapshot(graph) => graph.vertices(),
        _ => {
            return Err(error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "snapshot graph is required",
            ));
        }
    };
    if vertices.is_empty() {
        return Ok(BTreeMap::new());
    }
    let initial = 1.0 / vertices.len() as f64;
    Ok(vertices
        .iter()
        .copied()
        .map(|vertex| (vertex, initial))
        .collect())
}

fn encode_page_rank_state(
    request: &AlgorithmRequest,
    iteration: u64,
    converged: bool,
    damping: f64,
    max_iterations: u64,
    tolerance: f64,
    ranks: &BTreeMap<VertexId, f64>,
) -> Result<Vec<u8>, ProviderError> {
    let mut bytes = Vec::with_capacity(64 + ranks.len() * 24);
    bytes.extend_from_slice(&PAGERANK_STATE_MAGIC);
    bytes.extend_from_slice(&STATE_VERSION.to_be_bytes());
    bytes.extend_from_slice(&graph_fingerprint(request)?.to_be_bytes());
    bytes.extend_from_slice(
        &page_rank_parameter_fingerprint(damping, max_iterations, tolerance).to_be_bytes(),
    );
    bytes.extend_from_slice(&iteration.to_be_bytes());
    bytes.push(u8::from(converged));
    bytes.extend_from_slice(&damping.to_bits().to_be_bytes());
    bytes.extend_from_slice(&max_iterations.to_be_bytes());
    bytes.extend_from_slice(&tolerance.to_bits().to_be_bytes());
    bytes.extend_from_slice(
        &u64::try_from(ranks.len())
            .map_err(|_| {
                error(
                    "DTG-ANALYTICS-CHECKPOINT-STATE",
                    "PageRank state is too large",
                )
            })?
            .to_be_bytes(),
    );
    for (vertex, rank) in ranks {
        bytes.extend_from_slice(&vertex.value().to_be_bytes());
        bytes.extend_from_slice(&rank.to_bits().to_be_bytes());
    }
    let checksum = fnv_bytes(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    Ok(bytes)
}

fn decode_page_rank_state(
    request: &AlgorithmRequest,
    bytes: &[u8],
) -> Result<PageRankState, ProviderError> {
    const HEADER: usize = 4 + 2 + 8 + 8 + 8 + 1 + 8 + 8 + 8 + 8;
    if bytes.len() < HEADER + 8 || bytes[..4] != PAGERANK_STATE_MAGIC {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "invalid PageRank checkpoint state",
        ));
    }
    if u16::from_be_bytes([bytes[4], bytes[5]]) != STATE_VERSION {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-VERSION",
            "unsupported PageRank checkpoint state version",
        ));
    }
    let checksum_offset = bytes.len() - 8;
    let expected = u64::from_be_bytes(bytes[checksum_offset..].try_into().expect("fixed checksum"));
    if fnv_bytes(&bytes[..checksum_offset]) != expected {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "PageRank checkpoint state checksum mismatch",
        ));
    }
    let graph_fp = u64::from_be_bytes(bytes[6..14].try_into().expect("fixed graph fingerprint"));
    if graph_fp != graph_fingerprint(request)? {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
            "PageRank checkpoint graph does not match the request",
        ));
    }
    let parameter_fp = u64::from_be_bytes(
        bytes[14..22]
            .try_into()
            .expect("fixed parameter fingerprint"),
    );
    let (damping, max_iterations, tolerance) = page_rank_configuration(request)?;
    if parameter_fp != page_rank_parameter_fingerprint(damping, max_iterations, tolerance) {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
            "PageRank checkpoint parameters do not match the request",
        ));
    }
    let iteration = u64::from_be_bytes(bytes[22..30].try_into().expect("fixed iteration"));
    let converged = match bytes[30] {
        0 => false,
        1 => true,
        _ => {
            return Err(error(
                "DTG-ANALYTICS-CHECKPOINT-STATE",
                "invalid PageRank convergence flag",
            ));
        }
    };
    let encoded_damping = f64::from_bits(u64::from_be_bytes(
        bytes[31..39].try_into().expect("fixed damping"),
    ));
    let encoded_max = u64::from_be_bytes(bytes[39..47].try_into().expect("fixed max iterations"));
    let encoded_tolerance = f64::from_bits(u64::from_be_bytes(
        bytes[47..55].try_into().expect("fixed tolerance"),
    ));
    if encoded_damping.to_bits() != damping.to_bits()
        || encoded_max != max_iterations
        || encoded_tolerance.to_bits() != tolerance.to_bits()
    {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
            "PageRank checkpoint configuration does not match the request",
        ));
    }
    let count = usize::try_from(u64::from_be_bytes(
        bytes[55..63].try_into().expect("fixed count"),
    ))
    .map_err(|_| {
        error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "PageRank state count overflows platform",
        )
    })?;
    let expected_len = HEADER + count.saturating_mul(24) + 8;
    if bytes.len() != expected_len || count > MAX_GATHERED_SNAPSHOT_VERTICES {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "invalid PageRank checkpoint state length",
        ));
    }
    let mut ranks = BTreeMap::new();
    let mut offset = HEADER;
    for _ in 0..count {
        let vertex = VertexId::new(u128::from_be_bytes(
            bytes[offset..offset + 16].try_into().expect("fixed vertex"),
        ));
        let rank = f64::from_bits(u64::from_be_bytes(
            bytes[offset + 16..offset + 24]
                .try_into()
                .expect("fixed rank"),
        ));
        if !rank.is_finite() || ranks.insert(vertex, rank).is_some() {
            return Err(error(
                "DTG-ANALYTICS-CHECKPOINT-STATE",
                "invalid PageRank rank vector",
            ));
        }
        offset += 24;
    }
    let vertices = match request.graph() {
        ProjectedGraph::Snapshot(graph) => graph.vertices().to_vec(),
        ProjectedGraph::PartitionedSnapshot(graph) => graph.vertices().to_vec(),
        _ => Vec::new(),
    };
    if ranks.keys().copied().collect::<Vec<_>>() != vertices {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
            "PageRank checkpoint vertices do not match the request",
        ));
    }
    Ok(PageRankState {
        iteration,
        converged,
        damping,
        max_iterations,
        tolerance,
        ranks,
    })
}

fn page_rank_step(
    request: &AlgorithmRequest,
    ranks: &BTreeMap<VertexId, f64>,
    damping: f64,
) -> Result<(BTreeMap<VertexId, f64>, f64), ProviderError> {
    let vertices = match request.graph() {
        ProjectedGraph::Snapshot(graph) => graph.vertices(),
        ProjectedGraph::PartitionedSnapshot(graph) => graph.vertices(),
        _ => {
            return Err(error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "snapshot graph is required",
            ));
        }
    };
    let mut outgoing = BTreeMap::<VertexId, usize>::new();
    let mut edges = Vec::<(VertexId, VertexId, u64)>::new();
    match request.graph() {
        ProjectedGraph::Snapshot(graph) => {
            for vertex in vertices {
                for edge in graph.outgoing(*vertex) {
                    edges.push((edge.source(), edge.destination(), edge.weight().to_bits()));
                }
            }
        }
        ProjectedGraph::PartitionedSnapshot(graph) => {
            for partition in graph.partitions() {
                for edge in partition.edges() {
                    edges.push((edge.source(), edge.destination(), edge.weight().to_bits()));
                    if !graph.directed() && edge.source() != edge.destination() {
                        edges.push((edge.destination(), edge.source(), edge.weight().to_bits()));
                    }
                }
            }
        }
        _ => unreachable!(),
    }
    edges.sort_unstable();
    for (source, _, _) in &edges {
        *outgoing.entry(*source).or_default() += 1;
    }
    let count = vertices.len() as f64;
    let dangling = vertices
        .iter()
        .filter(|vertex| outgoing.get(vertex).copied().unwrap_or(0) == 0)
        .map(|vertex| ranks[vertex])
        .sum::<f64>();
    let base = (1.0 - damping) / count + damping * dangling / count;
    let mut next = vertices
        .iter()
        .copied()
        .map(|vertex| (vertex, base))
        .collect::<BTreeMap<_, _>>();
    for (source, destination, _) in edges {
        let contribution = damping * ranks[&source] / outgoing[&source] as f64;
        *next.get_mut(&destination).expect("validated endpoint") += contribution;
    }
    let delta = vertices
        .iter()
        .map(|vertex| (next[vertex] - ranks[vertex]).abs())
        .sum();
    Ok((next, delta))
}

fn emit_page_rank(
    ranks: &BTreeMap<VertexId, f64>,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    table(
        output,
        &["vertexId", "score"],
        ranks.iter().map(|(vertex, score)| {
            vec![
                AlgorithmValue::Vertex(*vertex),
                AlgorithmValue::FloatBits(score.to_bits()),
            ]
        }),
    )
}

fn execute_page_rank_slice(
    request: &AlgorithmRequest,
    start_unit: u64,
    max_units: u64,
    output: &mut dyn AnalyticsOutput,
) -> Result<ProviderSlice, ProviderError> {
    if start_unit != 0 {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-STATE",
            "PageRank continuation requires a provider checkpoint",
        ));
    }
    let (damping, max_iterations, tolerance) = page_rank_configuration(request)?;
    let ranks = initial_page_rank(request)?;
    execute_page_rank_state_slice(
        request,
        PageRankState {
            iteration: 0,
            converged: false,
            damping,
            max_iterations,
            tolerance,
            ranks,
        },
        max_units,
        output,
    )
}

fn execute_page_rank_slice_from_checkpoint(
    request: &AlgorithmRequest,
    checkpoint: &ProviderCheckpoint,
    max_units: u64,
    output: &mut dyn AnalyticsOutput,
) -> Result<ProviderSlice, ProviderError> {
    let state = decode_page_rank_state(request, checkpoint.payload())?;
    if state.iteration != checkpoint.completed_units() {
        return Err(error(
            "DTG-ANALYTICS-CHECKPOINT-MISMATCH",
            "PageRank checkpoint cursor does not match its state",
        ));
    }
    execute_page_rank_state_slice(request, state, max_units, output)
}

fn execute_page_rank_state_slice(
    request: &AlgorithmRequest,
    mut state: PageRankState,
    max_units: u64,
    output: &mut dyn AnalyticsOutput,
) -> Result<ProviderSlice, ProviderError> {
    if state.ranks.is_empty() {
        emit_page_rank(&state.ranks, output)?;
        return Ok(
            ProviderSlice::new(state.iteration, true).with_checkpoint_payload(
                encode_page_rank_state(
                    request,
                    state.iteration,
                    true,
                    state.damping,
                    state.max_iterations,
                    state.tolerance,
                    &state.ranks,
                )?,
            ),
        );
    }
    for _ in 0..max_units {
        ensure_not_canceled(request)?;
        if state.converged || state.iteration >= state.max_iterations {
            state.converged = true;
            emit_page_rank(&state.ranks, output)?;
            return Ok(
                ProviderSlice::new(state.iteration, true).with_checkpoint_payload(
                    encode_page_rank_state(
                        request,
                        state.iteration,
                        true,
                        state.damping,
                        state.max_iterations,
                        state.tolerance,
                        &state.ranks,
                    )?,
                ),
            );
        }
        let (next, delta) = page_rank_step(request, &state.ranks, state.damping)?;
        state.ranks = next;
        state.iteration = state.iteration.saturating_add(1);
        state.converged = delta <= state.tolerance || state.iteration >= state.max_iterations;
        if state.converged {
            emit_page_rank(&state.ranks, output)?;
            return Ok(
                ProviderSlice::new(state.iteration, true).with_checkpoint_payload(
                    encode_page_rank_state(
                        request,
                        state.iteration,
                        true,
                        state.damping,
                        state.max_iterations,
                        state.tolerance,
                        &state.ranks,
                    )?,
                ),
            );
        }
    }
    Ok(
        ProviderSlice::new(state.iteration, false).with_checkpoint_payload(encode_page_rank_state(
            request,
            state.iteration,
            false,
            state.damping,
            state.max_iterations,
            state.tolerance,
            &state.ranks,
        )?),
    )
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

fn partitioned_snapshot(
    request: &AlgorithmRequest,
) -> Result<&PartitionedSnapshotGraph, ProviderError> {
    if let ProjectedGraph::PartitionedSnapshot(graph) = request.graph() {
        Ok(graph)
    } else {
        Err(error(
            "DTG-ANALYTICS-GRAPH-MODEL",
            "partitioned snapshot graph is required",
        ))
    }
}

fn gather_partitioned_snapshot_if_needed(
    request: AlgorithmRequest,
) -> Result<AlgorithmRequest, ProviderError> {
    let native = matches!(
        request.algorithm(),
        "dtg.graph.degree" | "dtg.graph.wcc" | "dtg.graph.pageRank"
    );
    let ProjectedGraph::PartitionedSnapshot(graph) = request.graph() else {
        return Ok(request);
    };
    if native {
        return Ok(request);
    }
    let canonical = graph
        .canonical_snapshot_bounded(
            MAX_GATHERED_SNAPSHOT_VERTICES,
            MAX_GATHERED_SNAPSHOT_EDGES,
        )
        .map_err(|projection| {
            error(
                "DTG-ANALYTICS-CAPACITY",
                format!(
                    "algorithm requires a gathered snapshot bounded at {MAX_GATHERED_SNAPSHOT_VERTICES} vertices and {MAX_GATHERED_SNAPSHOT_EDGES} edges: {projection}"
                ),
            )
        })?;
    let cancellation = request.cancellation().clone();
    AlgorithmRequest::new(
        request.algorithm().to_owned(),
        ProjectedGraph::Snapshot(canonical),
        request.parameters().clone(),
    )
    .map(|request| request.with_cancellation(cancellation))
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

fn interval(request: &AlgorithmRequest) -> Result<&IntervalGraph, ProviderError> {
    if let ProjectedGraph::Interval(graph) = request.graph() {
        Ok(graph)
    } else {
        Err(error(
            "DTG-ANALYTICS-GRAPH-MODEL",
            "interval graph is required",
        ))
    }
}

fn delta(request: &AlgorithmRequest) -> Result<&DeltaGraph, ProviderError> {
    if let ProjectedGraph::Delta(graph) = request.graph() {
        Ok(graph)
    } else {
        Err(error(
            "DTG-ANALYTICS-GRAPH-MODEL",
            "delta graph is required",
        ))
    }
}

fn source(request: &AlgorithmRequest) -> Result<VertexId, ProviderError> {
    match request.parameters().get("source") {
        Some(AlgorithmValue::Vertex(vertex)) => Ok(*vertex),
        _ => Err(error("DTG-ANALYTICS-PARAMETER", "source must be a Vertex")),
    }
}

fn run_bfs(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let graph = snapshot(request)?;
    let result = bfs_cancellable(graph, source(request)?, || {
        request.cancellation().is_canceled()
    })
    .map_err(cancellation_aware_error)?;
    let rows = graph.vertices().iter().filter_map(|vertex| {
        result.distance(*vertex).map(|distance| {
            vec![
                AlgorithmValue::Vertex(*vertex),
                AlgorithmValue::Integer(i64::try_from(distance).unwrap_or(i64::MAX)),
                result
                    .predecessor(*vertex)
                    .map_or(AlgorithmValue::Null, AlgorithmValue::Vertex),
            ]
        })
    });
    table(output, &["vertexId", "distance", "predecessor"], rows)
}

fn run_dfs(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let graph = snapshot(request)?;
    let result = dfs_cancellable(graph, source(request)?, || {
        request.cancellation().is_canceled()
    })
    .map_err(cancellation_aware_error)?;
    let rows = graph.vertices().iter().filter_map(|vertex| {
        result.depth(*vertex).map(|depth| {
            vec![
                AlgorithmValue::Vertex(*vertex),
                AlgorithmValue::Integer(i64::try_from(depth).unwrap_or(i64::MAX)),
                result
                    .predecessor(*vertex)
                    .map_or(AlgorithmValue::Null, AlgorithmValue::Vertex),
            ]
        })
    });
    table(output, &["vertexId", "depth", "predecessor"], rows)
}

fn run_sssp(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let graph = snapshot(request)?;
    let result = sssp_cancellable(graph, source(request)?, || {
        request.cancellation().is_canceled()
    })
    .map_err(cancellation_aware_error)?;
    let rows = graph.vertices().iter().filter_map(|vertex| {
        result.distance(*vertex).map(|distance| {
            vec![
                AlgorithmValue::Vertex(*vertex),
                AlgorithmValue::FloatBits(distance.to_bits()),
                result
                    .predecessor(*vertex)
                    .map_or(AlgorithmValue::Null, AlgorithmValue::Vertex),
            ]
        })
    });
    table(output, &["vertexId", "distance", "predecessor"], rows)
}

fn run_all_pairs_shortest_path(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let distances =
        all_pairs_shortest_paths_cancellable(snapshot(request)?, MAX_ALL_PAIRS_WORK, || {
            request.cancellation().is_canceled()
        })
        .map_err(cancellation_aware_error)?;
    table(
        output,
        &["source", "target", "distance"],
        distances.into_iter().map(|((source, target), distance)| {
            vec![
                AlgorithmValue::Vertex(source),
                AlgorithmValue::Vertex(target),
                AlgorithmValue::FloatBits(distance.to_bits()),
            ]
        }),
    )
}

fn run_components(
    request: &AlgorithmRequest,
    strong: bool,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let components = if strong {
        scc_cancellable(snapshot(request)?, || request.cancellation().is_canceled())
            .map_err(cancellation_aware_error)?
    } else {
        match request.graph() {
            ProjectedGraph::Snapshot(graph) => {
                wcc_cancellable(graph, || request.cancellation().is_canceled())
                    .map_err(cancellation_aware_error)?
            }
            ProjectedGraph::PartitionedSnapshot(_) => {
                partitioned_wcc_cancellable(partitioned_snapshot(request)?, || {
                    request.cancellation().is_canceled()
                })
                .map_err(cancellation_aware_error)?
            }
            _ => {
                return Err(error(
                    "DTG-ANALYTICS-GRAPH-MODEL",
                    "snapshot graph is required",
                ));
            }
        }
    };
    table(
        output,
        &["vertexId", "componentId"],
        components.into_iter().map(|(vertex, component)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::Vertex(component),
            ]
        }),
    )
}

fn run_page_rank(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let damping = float_parameter(request, "damping", 0.85)?;
    let iterations = positive_integer(request, "maxIterations", 100)?;
    let tolerance = float_parameter(request, "tolerance", 1e-9)?;
    let iterations = usize::try_from(iterations)
        .map_err(|_| error("DTG-ANALYTICS-PARAMETER", "maxIterations is too large"))?;
    ensure_not_canceled(request)?;
    let ranks = match request.graph() {
        ProjectedGraph::Snapshot(graph) => {
            page_rank_cancellable(graph, damping, iterations, tolerance, || {
                request.cancellation().is_canceled()
            })
        }
        ProjectedGraph::PartitionedSnapshot(graph) => {
            partitioned_page_rank_cancellable(graph, damping, iterations, tolerance, || {
                request.cancellation().is_canceled()
            })
        }
        _ => {
            return Err(error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "snapshot graph is required",
            ));
        }
    }
    .map_err(cancellation_aware_error)?;
    ensure_not_canceled(request)?;
    table(
        output,
        &["vertexId", "score"],
        ranks.into_iter().map(|(vertex, score)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::FloatBits(score.to_bits()),
            ]
        }),
    )
}

fn run_degree(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let degrees = match request.graph() {
        ProjectedGraph::Snapshot(graph) => degree_centrality(graph),
        ProjectedGraph::PartitionedSnapshot(graph) => partitioned_degree_centrality(graph),
        _ => {
            return Err(error(
                "DTG-ANALYTICS-GRAPH-MODEL",
                "snapshot graph is required",
            ));
        }
    };
    table(
        output,
        &["vertexId", "inDegree", "outDegree", "degree"],
        degrees.into_iter().map(|(vertex, degree)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                integer(degree.incoming()),
                integer(degree.outgoing()),
                integer(degree.total()),
            ]
        }),
    )
}

fn run_triangle_count(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    table(
        output,
        &["triangleCount"],
        vec![vec![integer(triangle_count(snapshot(request)?) as u64)]],
    )
}

fn run_clustering(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    table(
        output,
        &["vertexId", "coefficient"],
        clustering_coefficient(snapshot(request)?)
            .into_iter()
            .map(|(vertex, value)| {
                vec![
                    AlgorithmValue::Vertex(vertex),
                    AlgorithmValue::FloatBits(value.to_bits()),
                ]
            }),
    )
}

fn run_betweenness(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let scores = betweenness_centrality_cancellable(snapshot(request)?, || {
        request.cancellation().is_canceled()
    })
    .map_err(cancellation_aware_error)?;
    table(
        output,
        &["vertexId", "score"],
        scores.into_iter().map(|(vertex, score)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::FloatBits(score.to_bits()),
            ]
        }),
    )
}

fn run_closeness(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let scores = closeness_centrality_cancellable(snapshot(request)?, MAX_CENTRALITY_WORK, || {
        request.cancellation().is_canceled()
    })
    .map_err(cancellation_aware_error)?;
    table(
        output,
        &["vertexId", "score"],
        scores.into_iter().map(|(vertex, score)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::FloatBits(score.to_bits()),
            ]
        }),
    )
}

fn run_k_core(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let k = positive_integer(request, "k", 2)?;
    let k = usize::try_from(k).map_err(|_| error("DTG-ANALYTICS-PARAMETER", "k is too large"))?;
    table(
        output,
        &["vertexId", "core"],
        k_core(snapshot(request)?, k)
            .into_iter()
            .map(|(vertex, core)| vec![AlgorithmValue::Vertex(vertex), integer(core as u64)]),
    )
}

fn run_label_propagation(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let iterations = positive_integer(request, "maxIterations", 20)?;
    let iterations = usize::try_from(iterations)
        .map_err(|_| error("DTG-ANALYTICS-PARAMETER", "maxIterations is too large"))?;
    table(
        output,
        &["vertexId", "label"],
        label_propagation_cancellable(snapshot(request)?, iterations, || {
            request.cancellation().is_canceled()
        })
        .map_err(cancellation_aware_error)?
        .into_iter()
        .map(|(vertex, label)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::Vertex(label),
            ]
        }),
    )
}

fn run_louvain(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let max_levels = usize::try_from(positive_integer(request, "maxLevels", 10)?)
        .map_err(|_| error("DTG-ANALYTICS-PARAMETER", "maxLevels is too large"))?;
    let max_iterations = usize::try_from(positive_integer(request, "maxIterations", 20)?)
        .map_err(|_| error("DTG-ANALYTICS-PARAMETER", "maxIterations is too large"))?;
    let communities = louvain_communities_cancellable(
        snapshot(request)?,
        max_levels,
        max_iterations,
        float_parameter(request, "resolution", 1.0)?,
        || request.cancellation().is_canceled(),
    )
    .map_err(cancellation_aware_error)?;
    table(
        output,
        &["vertexId", "communityId"],
        communities.into_iter().map(|(vertex, community)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::Vertex(community),
            ]
        }),
    )
}

fn run_earliest(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
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
        output,
        &["vertexId", "arrivalTime", "predecessor"],
        result.arrivals().iter().map(|(vertex, arrival)| {
            vec![
                AlgorithmValue::Vertex(*vertex),
                AlgorithmValue::Time(*arrival),
                result
                    .predecessor(*vertex)
                    .map_or(AlgorithmValue::Null, AlgorithmValue::Vertex),
            ]
        }),
    )
}

fn temporal_request(request: &AlgorithmRequest) -> Result<TemporalPathRequest, ProviderError> {
    TemporalPathRequest::new(
        source(request)?,
        time_parameter(request, "validFrom")?,
        time_parameter(request, "validTo")?,
        TimeOrder::NonDecreasing,
        WaitingPolicy::Allowed,
    )
    .map_err(execution_error)
}

fn run_reachability(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    table(
        output,
        &["vertexId", "reachable"],
        temporal_reachability(event(request)?, temporal_request(request)?)
            .map_err(execution_error)?
            .into_iter()
            .map(|(vertex, reachable)| {
                vec![
                    AlgorithmValue::Vertex(vertex),
                    AlgorithmValue::Boolean(reachable),
                ]
            }),
    )
}

fn run_min_hop(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    table(
        output,
        &["vertexId", "hops"],
        min_hop_temporal_path(event(request)?, temporal_request(request)?)
            .map_err(execution_error)?
            .into_iter()
            .map(|(vertex, hops)| vec![AlgorithmValue::Vertex(vertex), integer(hops)]),
    )
}

fn run_latest_departure(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let destination = match request.parameters().get("destination") {
        Some(AlgorithmValue::Vertex(vertex)) => *vertex,
        _ => {
            return Err(error(
                "DTG-ANALYTICS-PARAMETER",
                "destination must be a Vertex",
            ));
        }
    };
    let deadline = time_parameter(request, "deadline")?;
    table(
        output,
        &["vertexId", "latestDeparture"],
        latest_departure(event(request)?, destination, deadline)
            .map_err(execution_error)?
            .into_iter()
            .map(|(vertex, time)| vec![AlgorithmValue::Vertex(vertex), AlgorithmValue::Time(time)]),
    )
}

fn run_fastest_path(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let from = time_parameter(request, "validFrom")?;
    let result =
        earliest_arrival(event(request)?, temporal_request(request)?).map_err(execution_error)?;
    table(
        output,
        &["vertexId", "travelTime", "arrivalTime"],
        result.arrivals().iter().map(|(vertex, arrival)| {
            let travel = arrival.as_micros().saturating_sub(from.as_micros());
            vec![
                AlgorithmValue::Vertex(*vertex),
                AlgorithmValue::Integer(travel),
                AlgorithmValue::Time(*arrival),
            ]
        }),
    )
}

fn run_temporal_degree(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let from = time_parameter(request, "validFrom")?;
    let to = time_parameter(request, "validTo")?;
    let mut counts = BTreeMap::<VertexId, (u64, u64)>::new();
    for vertex in event(request)?.vertices() {
        counts.insert(*vertex, (0, 0));
    }
    for edge in event(request)?.events() {
        if edge.event_time() < from || edge.event_time() > to {
            continue;
        }
        let outgoing = counts
            .get(&edge.source())
            .map_or(1, |(_, outgoing)| outgoing.saturating_add(1));
        if let Some(entry) = counts.get_mut(&edge.source()) {
            entry.1 = outgoing;
        }
        let incoming = counts
            .get(&edge.destination())
            .map_or(1, |(incoming, _)| incoming.saturating_add(1));
        if let Some(entry) = counts.get_mut(&edge.destination()) {
            entry.0 = incoming;
        }
    }
    table(
        output,
        &["vertexId", "inDegree", "outDegree", "degree"],
        counts.into_iter().map(|(vertex, (incoming, outgoing))| {
            vec![
                AlgorithmValue::Vertex(vertex),
                integer(incoming),
                integer(outgoing),
                integer(incoming.saturating_add(outgoing)),
            ]
        }),
    )
}

fn run_temporal_closeness(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let path = temporal_request(request)?;
    let result = earliest_arrival(event(request)?, path).map_err(execution_error)?;
    let from = time_parameter(request, "validFrom")?;
    let mut total = 0_i64;
    let mut reachable = 0_i64;
    for arrival in result.arrivals().values() {
        if *arrival != from {
            total = total.saturating_add(arrival.as_micros().saturating_sub(from.as_micros()));
            reachable = reachable.saturating_add(1);
        }
    }
    let score = if total > 0 {
        reachable as f64 / total as f64
    } else {
        0.0
    };
    table(
        output,
        &["vertexId", "score"],
        vec![vec![
            AlgorithmValue::Vertex(source(request)?),
            AlgorithmValue::FloatBits(score.to_bits()),
        ]],
    )
}

fn run_temporal_betweenness(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let graph = event(request)?;
    let from = time_parameter(request, "validFrom")?;
    let to = time_parameter(request, "validTo")?;
    let mut counts = BTreeMap::<VertexId, u64>::new();
    for vertex in graph.vertices() {
        counts.insert(*vertex, 0);
    }
    for source in graph.vertices() {
        ensure_not_canceled(request)?;
        let path = TemporalPathRequest::new(
            *source,
            from,
            to,
            TimeOrder::NonDecreasing,
            WaitingPolicy::Allowed,
        )
        .map_err(execution_error)?;
        let result = earliest_arrival(graph, path).map_err(execution_error)?;
        for predecessor in result.predecessors().values() {
            if predecessor != source {
                *counts.entry(*predecessor).or_default() += 1;
            }
        }
    }
    table(
        output,
        &["vertexId", "score"],
        counts.into_iter().map(|(vertex, score)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::FloatBits((score as f64).to_bits()),
            ]
        }),
    )
}

fn run_temporal_page_rank(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let snapshot = event_window_snapshot(request, true)?;
    let iterations = positive_integer(request, "maxIterations", 100)?;
    let iterations = usize::try_from(iterations)
        .map_err(|_| error("DTG-ANALYTICS-PARAMETER", "maxIterations is too large"))?;
    let ranks = page_rank_cancellable(
        &snapshot,
        float_parameter(request, "damping", 0.85)?,
        iterations,
        float_parameter(request, "tolerance", 1e-9)?,
        || request.cancellation().is_canceled(),
    )
    .map_err(cancellation_aware_error)?;
    table(
        output,
        &["vertexId", "score"],
        ranks.into_iter().map(|(vertex, score)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::FloatBits(score.to_bits()),
            ]
        }),
    )
}

fn run_temporal_clustering(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    table(
        output,
        &["vertexId", "coefficient"],
        clustering_coefficient(&event_window_snapshot(request, false)?)
            .into_iter()
            .map(|(vertex, coefficient)| {
                vec![
                    AlgorithmValue::Vertex(vertex),
                    AlgorithmValue::FloatBits(coefficient.to_bits()),
                ]
            }),
    )
}

fn event_window_snapshot(
    request: &AlgorithmRequest,
    directed: bool,
) -> Result<SnapshotGraph, ProviderError> {
    let from = time_parameter(request, "validFrom")?;
    let to = time_parameter(request, "validTo")?;
    if from > to {
        return Err(error(
            "DTG-ANALYTICS-TEMPORAL-WINDOW",
            "validFrom must not exceed validTo",
        ));
    }
    let graph = event(request)?;
    let mut edges = Vec::new();
    for edge in graph.events() {
        ensure_not_canceled(request)?;
        if edge.event_time() < from || edge.event_time() > to {
            continue;
        }
        edges.push(
            analytics_api::SnapshotEdge::new(edge.source(), edge.destination(), edge.weight())
                .map_err(execution_error)?,
        );
    }
    SnapshotGraph::new(graph.vertices().to_vec(), edges, directed).map_err(execution_error)
}

fn run_topological_overlap(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let first_from = time_parameter(request, "firstFrom")?;
    let first_to = time_parameter(request, "firstTo")?;
    let second_from = time_parameter(request, "secondFrom")?;
    let second_to = time_parameter(request, "secondTo")?;
    if first_from > first_to || first_to >= second_from || second_from > second_to {
        return Err(error(
            "DTG-ANALYTICS-TEMPORAL-WINDOW",
            "topological overlap requires two ordered non-overlapping windows",
        ));
    }
    let graph = event(request)?;
    let first = temporal_neighbors(graph, first_from, first_to);
    let second = temporal_neighbors(graph, second_from, second_to);
    table(
        output,
        &["vertexId", "score"],
        graph.vertices().iter().map(|vertex| {
            let left = &first[vertex];
            let right = &second[vertex];
            let denominator = ((left.len() * right.len()) as f64).sqrt();
            let score = if denominator == 0.0 {
                0.0
            } else {
                left.intersection(right).count() as f64 / denominator
            };
            vec![
                AlgorithmValue::Vertex(*vertex),
                AlgorithmValue::FloatBits(score.to_bits()),
            ]
        }),
    )
}

fn temporal_neighbors(
    graph: &EventGraph,
    from: temporal_types::ValidTime,
    to: temporal_types::ValidTime,
) -> BTreeMap<VertexId, BTreeSet<VertexId>> {
    let mut neighbors = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, BTreeSet::new()))
        .collect::<BTreeMap<_, _>>();
    for edge in graph
        .events()
        .iter()
        .filter(|edge| edge.event_time() >= from && edge.event_time() <= to)
    {
        if edge.source() == edge.destination() {
            continue;
        }
        neighbors
            .get_mut(&edge.source())
            .expect("validated source")
            .insert(edge.destination());
        neighbors
            .get_mut(&edge.destination())
            .expect("validated destination")
            .insert(edge.source());
    }
    neighbors
}

fn run_temporal_burstiness(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let from = time_parameter(request, "validFrom")?;
    let to = time_parameter(request, "validTo")?;
    if from > to {
        return Err(error(
            "DTG-ANALYTICS-TEMPORAL-WINDOW",
            "validFrom must not exceed validTo",
        ));
    }
    let graph = event(request)?;
    let mut event_times = graph
        .vertices()
        .iter()
        .copied()
        .map(|vertex| (vertex, Vec::new()))
        .collect::<BTreeMap<_, _>>();
    for edge in graph
        .events()
        .iter()
        .filter(|edge| edge.event_time() >= from && edge.event_time() <= to)
    {
        event_times
            .get_mut(&edge.source())
            .expect("validated source")
            .push(edge.event_time().as_micros());
        if edge.destination() != edge.source() {
            event_times
                .get_mut(&edge.destination())
                .expect("validated destination")
                .push(edge.event_time().as_micros());
        }
    }
    table(
        output,
        &["vertexId", "score"],
        event_times.into_iter().map(|(vertex, mut times)| {
            times.sort_unstable();
            let intervals = times
                .windows(2)
                .map(|pair| pair[1].saturating_sub(pair[0]) as f64)
                .collect::<Vec<_>>();
            let score = if intervals.is_empty() {
                0.0
            } else {
                let mean = intervals.iter().sum::<f64>() / intervals.len() as f64;
                let variance = intervals
                    .iter()
                    .map(|interval| {
                        let delta = interval - mean;
                        delta * delta
                    })
                    .sum::<f64>()
                    / intervals.len() as f64;
                let deviation = variance.sqrt();
                let denominator = deviation + mean;
                if denominator == 0.0 {
                    0.0
                } else {
                    (deviation - mean) / denominator
                }
            };
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::FloatBits(score.to_bits()),
            ]
        }),
    )
}

fn run_windowed_components(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let components = windowed_components_cancellable(
        event(request)?,
        time_parameter(request, "validFrom")?,
        time_parameter(request, "validTo")?,
        || request.cancellation().is_canceled(),
    )
    .map_err(cancellation_aware_error)?;
    table(
        output,
        &["vertexId", "componentId"],
        components.into_iter().map(|(vertex, component)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::Vertex(component),
            ]
        }),
    )
}

fn run_windowed_triangle_count(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let count = windowed_triangle_count_cancellable(
        event(request)?,
        time_parameter(request, "validFrom")?,
        time_parameter(request, "validTo")?,
        || request.cancellation().is_canceled(),
    )
    .map_err(cancellation_aware_error)?;
    table(output, &["triangleCount"], vec![vec![integer(count)]])
}

fn run_change_point(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let scores = change_point_scores_cancellable(
        event(request)?,
        time_parameter(request, "firstFrom")?,
        time_parameter(request, "firstTo")?,
        time_parameter(request, "secondFrom")?,
        time_parameter(request, "secondTo")?,
        || request.cancellation().is_canceled(),
    )
    .map_err(cancellation_aware_error)?;
    table(
        output,
        &["vertexId", "score"],
        scores.into_iter().map(|(vertex, score)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::FloatBits(score.to_bits()),
            ]
        }),
    )
}

fn run_temporal_motif_count(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let motifs = temporal_motif_count_cancellable(
        event(request)?,
        time_parameter(request, "validFrom")?,
        time_parameter(request, "validTo")?,
        positive_integer(request, "deltaMicros", 1_000_000)?,
        || request.cancellation().is_canceled(),
    )
    .map_err(cancellation_aware_error)?;
    table(
        output,
        &["motif", "count"],
        motifs
            .into_iter()
            .map(|(motif, count)| vec![AlgorithmValue::String(motif), integer(count)]),
    )
}

fn run_interval_components(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let components = interval_components_cancellable(interval(request)?, || {
        request.cancellation().is_canceled()
    })
    .map_err(cancellation_aware_error)?;
    table(
        output,
        &["vertexId", "componentId"],
        components.into_iter().map(|(vertex, component)| {
            vec![
                AlgorithmValue::Vertex(vertex),
                AlgorithmValue::Vertex(component),
            ]
        }),
    )
}

fn run_delta_summary(
    request: &AlgorithmRequest,
    output: &mut dyn AnalyticsOutput,
) -> Result<(), ProviderError> {
    let summary =
        delta_summary_cancellable(delta(request)?, || request.cancellation().is_canceled())
            .map_err(cancellation_aware_error)?;
    table(
        output,
        &["entityType", "change", "count"],
        summary.into_iter().map(|((entity_type, change), count)| {
            vec![
                AlgorithmValue::String(delta_entity_type_name(entity_type).into()),
                AlgorithmValue::String(delta_kind_name(change).into()),
                integer(count),
            ]
        }),
    )
}

fn delta_entity_type_name(entity_type: DeltaEntityType) -> &'static str {
    match entity_type {
        DeltaEntityType::Vertex => "VERTEX",
        DeltaEntityType::Edge => "EDGE",
    }
}

fn delta_kind_name(change: DeltaKind) -> &'static str {
    match change {
        DeltaKind::Added => "ADDED",
        DeltaKind::Removed => "REMOVED",
        DeltaKind::Updated => "UPDATED",
    }
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
    output: &mut dyn AnalyticsOutput,
    columns: &[&str],
    rows: impl IntoIterator<Item = Vec<AlgorithmValue>>,
) -> Result<(), ProviderError> {
    output.declare_columns(columns.iter().map(|column| (*column).to_owned()).collect())?;
    for row in rows {
        output.push_row(row)?;
    }
    Ok(())
}

fn execution_error(error: impl ToString) -> ProviderError {
    ProviderError::new("DTG-ANALYTICS-EXECUTION", error.to_string())
}

fn cancellation_aware_error(algorithm_error: AlgorithmError) -> ProviderError {
    match algorithm_error {
        AlgorithmError::Canceled => error("DTG-ANALYTICS-CANCELED", "analytics job canceled"),
        AlgorithmError::InvalidTemporalWindow => error(
            "DTG-ANALYTICS-TEMPORAL-WINDOW",
            "temporal windows must be ordered, positive-length, and non-overlapping",
        ),
        AlgorithmError::InvalidTemporalMotifDelta => error(
            "DTG-ANALYTICS-PARAMETER",
            "deltaMicros must be a positive Integer",
        ),
        AlgorithmError::TemporalMotifCapacity => error(
            "DTG-ANALYTICS-CAPACITY",
            "temporal motif candidate-event capacity exceeded",
        ),
        AlgorithmError::AllPairsCapacity => error(
            "DTG-ANALYTICS-CAPACITY",
            "all-pairs shortest-path work budget exceeded",
        ),
        AlgorithmError::CentralityCapacity => {
            error("DTG-ANALYTICS-CAPACITY", "centrality work budget exceeded")
        }
        AlgorithmError::InvalidLouvainConfiguration => error(
            "DTG-ANALYTICS-PARAMETER",
            "Louvain requires positive maxLevels/maxIterations and resolution",
        ),
        AlgorithmError::InvalidBetweennessWeight => error(
            "DTG-ANALYTICS-PARAMETER",
            "betweenness requires strictly positive edge weights",
        ),
        other => execution_error(other),
    }
}

fn error(code: impl Into<String>, message: impl Into<String>) -> ProviderError {
    ProviderError::new(code, message)
}

fn ensure_not_canceled(request: &AlgorithmRequest) -> Result<(), ProviderError> {
    if request.cancellation().is_canceled() {
        Err(error("DTG-ANALYTICS-CANCELED", "analytics job canceled"))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod cancellation_tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use analytics_api::{
        AlgorithmRequest, AlgorithmValue, CancellationSignal, EventEdge, EventGraph,
        ProjectedGraph, VertexId,
    };
    use temporal_types::ValidTime;

    use super::event_window_snapshot;

    #[test]
    fn temporal_snapshot_projection_observes_cancellation_during_event_scan() {
        let graph = EventGraph::new(
            vec![VertexId::new(1)],
            (0..32)
                .map(|time| {
                    EventEdge::new(
                        VertexId::new(1),
                        VertexId::new(1),
                        ValidTime::from_micros(time),
                        0,
                        1.0,
                    )
                    .unwrap()
                })
                .collect(),
        )
        .unwrap();
        let canceled = Arc::new(AtomicBool::new(false));
        let request = AlgorithmRequest::new(
            "dtg.temporal.pageRank",
            ProjectedGraph::Event(graph),
            BTreeMap::from([
                (
                    "validFrom".into(),
                    AlgorithmValue::Time(ValidTime::from_micros(0)),
                ),
                (
                    "validTo".into(),
                    AlgorithmValue::Time(ValidTime::from_micros(31)),
                ),
            ]),
        )
        .unwrap()
        .with_cancellation(CancellationSignal::from_flag(Arc::clone(&canceled)));
        canceled.store(true, Ordering::Release);

        let error = event_window_snapshot(&request, true)
            .expect_err("event projection must observe cancellation");

        assert_eq!(error.code(), "DTG-ANALYTICS-CANCELED");
    }
}
