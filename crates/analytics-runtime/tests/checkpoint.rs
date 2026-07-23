use analytics_api::{
    AlgorithmRequest, AlgorithmResult, AlgorithmValue, AnalyticsProvider, ProjectedGraph,
    SnapshotEdge, SnapshotGraph, VertexId,
};
use analytics_runtime::BuiltInProvider;
use std::collections::BTreeMap;

fn request() -> AlgorithmRequest {
    AlgorithmRequest::new(
        "dtg.graph.degree",
        ProjectedGraph::Snapshot(SnapshotGraph::new(Vec::new(), Vec::new(), true).unwrap()),
        BTreeMap::<String, AlgorithmValue>::new(),
    )
    .unwrap()
}

#[test]
fn built_in_provider_exposes_versioned_checkpoint_round_trip() {
    let provider = BuiltInProvider::new();
    let checkpoint = provider.checkpoint(&request(), 7).unwrap();
    assert_eq!(checkpoint.algorithm(), "dtg.graph.degree");
    assert_eq!(checkpoint.completed_units(), 7);
    assert_eq!(
        provider
            .restore_checkpoint(&request(), &checkpoint)
            .unwrap(),
        7
    );
}

#[test]
fn providers_without_checkpoint_support_fail_closed() {
    struct Unsupported;
    impl AnalyticsProvider for Unsupported {
        fn descriptor(&self) -> analytics_api::ProviderDescriptor {
            analytics_api::ProviderDescriptor::new("test", "1", false, false)
        }
        fn algorithms(&self) -> Vec<analytics_api::AlgorithmDescriptor> {
            Vec::new()
        }
        fn execute_into(
            &self,
            _request: AlgorithmRequest,
            _output: &mut dyn analytics_api::AnalyticsOutput,
        ) -> Result<(), analytics_api::ProviderError> {
            Ok(())
        }
    }
    let error = Unsupported.checkpoint(&request(), 0).unwrap_err();
    assert_eq!(error.code(), "DTG-ANALYTICS-CHECKPOINT-UNSUPPORTED");
}

#[test]
fn degree_execution_slices_reconstruct_the_full_result() {
    let graph = SnapshotGraph::new(
        vec![VertexId::new(1), VertexId::new(2), VertexId::new(3)],
        vec![SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap()],
        true,
    )
    .unwrap();
    let request = AlgorithmRequest::new(
        "dtg.graph.degree",
        ProjectedGraph::Snapshot(graph),
        BTreeMap::new(),
    )
    .unwrap();
    let provider = BuiltInProvider::new();
    let expected = provider.execute(request.clone()).unwrap();
    let mut assembled: Option<AlgorithmResult> = None;
    let mut cursor = 0;
    loop {
        let mut slice = AlgorithmResult::empty();
        let result = provider
            .execute_slice(&request, cursor, 1, &mut slice)
            .unwrap();
        if let Some(assembled) = &mut assembled {
            assembled.append(slice).unwrap();
        } else {
            assembled = Some(slice);
        }
        if result.complete() {
            break;
        }
        cursor = result.next_unit();
    }
    assert_eq!(assembled.unwrap(), expected);
}

fn snapshot_request(
    algorithm: &str,
    graph: SnapshotGraph,
    parameters: BTreeMap<String, AlgorithmValue>,
) -> AlgorithmRequest {
    AlgorithmRequest::new(algorithm, ProjectedGraph::Snapshot(graph), parameters).unwrap()
}

fn checkpoint_graph() -> SnapshotGraph {
    SnapshotGraph::new(
        vec![
            VertexId::new(1),
            VertexId::new(2),
            VertexId::new(3),
            VertexId::new(4),
        ],
        vec![
            SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap(),
            SnapshotEdge::new(VertexId::new(2), VertexId::new(3), 1.0).unwrap(),
        ],
        true,
    )
    .unwrap()
}

#[test]
fn wcc_execution_slices_resume_from_native_checkpoint_state() {
    let request = snapshot_request("dtg.graph.wcc", checkpoint_graph(), BTreeMap::new());
    let provider = BuiltInProvider::new();
    let expected = provider.execute(request.clone()).unwrap();
    let mut assembled = AlgorithmResult::empty();
    let mut cursor = 0;
    let mut checkpoint = None;
    let mut resume_checkpoint = None;
    loop {
        let mut slice = AlgorithmResult::empty();
        let state = match checkpoint.as_ref() {
            Some(checkpoint) => provider
                .execute_slice_from_checkpoint(&request, checkpoint, 1, &mut slice)
                .unwrap(),
            None => provider
                .execute_slice(&request, cursor, 1, &mut slice)
                .unwrap(),
        };
        assembled.append(slice).unwrap();
        assert!(state.next_unit() > cursor || state.complete());
        assert!(!state.checkpoint_payload().is_empty());
        checkpoint = Some(
            analytics_api::ProviderCheckpoint::new(
                request.algorithm(),
                state.next_unit(),
                state.checkpoint_payload().to_vec(),
            )
            .unwrap(),
        );
        if !state.complete() && resume_checkpoint.is_none() {
            resume_checkpoint = checkpoint.clone();
        }
        if state.complete() {
            break;
        }
        cursor = state.next_unit();
    }
    assert_eq!(assembled, expected);

    let checkpoint = resume_checkpoint.expect("WCC must expose an intermediate checkpoint");
    let restored = provider.restore_checkpoint(&request, &checkpoint).unwrap();
    assert_eq!(restored, checkpoint.completed_units());
    let mut resumed = AlgorithmResult::empty();
    let mut checkpoint = checkpoint;
    loop {
        let mut slice = AlgorithmResult::empty();
        let state = provider
            .execute_slice_from_checkpoint(&request, &checkpoint, 1, &mut slice)
            .unwrap();
        resumed.append(slice).unwrap();
        checkpoint = analytics_api::ProviderCheckpoint::new(
            request.algorithm(),
            state.next_unit(),
            state.checkpoint_payload().to_vec(),
        )
        .unwrap();
        if state.complete() {
            break;
        }
    }
    assert_eq!(resumed, expected);
}

#[test]
fn page_rank_execution_slices_resume_from_native_checkpoint_state() {
    let mut parameters = BTreeMap::new();
    parameters.insert(
        "damping".to_owned(),
        AlgorithmValue::FloatBits(0.85f64.to_bits()),
    );
    parameters.insert("maxIterations".to_owned(), AlgorithmValue::Integer(4));
    parameters.insert(
        "tolerance".to_owned(),
        AlgorithmValue::FloatBits(1e-30f64.to_bits()),
    );
    let request = snapshot_request("dtg.graph.pageRank", checkpoint_graph(), parameters);
    let provider = BuiltInProvider::new();
    let expected = provider.execute(request.clone()).unwrap();
    let mut assembled = AlgorithmResult::empty();
    let mut cursor = 0;
    let mut last_checkpoint = None;
    let mut resume_checkpoint = None;
    loop {
        let mut slice = AlgorithmResult::empty();
        let state = match last_checkpoint.as_ref() {
            Some(checkpoint) => provider
                .execute_slice_from_checkpoint(&request, checkpoint, 1, &mut slice)
                .unwrap(),
            None => provider
                .execute_slice(&request, cursor, 1, &mut slice)
                .unwrap(),
        };
        assembled.append(slice).unwrap();
        assert!(state.next_unit() > cursor || state.complete());
        assert!(!state.checkpoint_payload().is_empty());
        last_checkpoint = Some(
            analytics_api::ProviderCheckpoint::new(
                request.algorithm(),
                state.next_unit(),
                state.checkpoint_payload().to_vec(),
            )
            .unwrap(),
        );
        if !state.complete() && resume_checkpoint.is_none() {
            resume_checkpoint = last_checkpoint.clone();
        }
        if state.complete() {
            break;
        }
        cursor = state.next_unit();
    }
    assert_eq!(assembled, expected);

    let mut checkpoint =
        resume_checkpoint.expect("PageRank must expose an intermediate checkpoint");
    let mut resumed = AlgorithmResult::empty();
    loop {
        let mut slice = AlgorithmResult::empty();
        let state = provider
            .execute_slice_from_checkpoint(&request, &checkpoint, 1, &mut slice)
            .unwrap();
        resumed.append(slice).unwrap();
        checkpoint = analytics_api::ProviderCheckpoint::new(
            request.algorithm(),
            state.next_unit(),
            state.checkpoint_payload().to_vec(),
        )
        .unwrap();
        if state.complete() {
            break;
        }
    }
    assert_eq!(resumed, expected);
}
