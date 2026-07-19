use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use analytics_api::{
    AlgorithmRequest, AlgorithmValue, ProjectedGraph, SnapshotEdge, SnapshotGraph, VertexId,
};
use analytics_runtime::{AnalyticsJobManager, BuiltInProvider, JobState};

#[test]
fn analytics_jobs_have_bounded_lifecycle_and_retrievable_results() {
    let graph = SnapshotGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        vec![SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).unwrap()],
        true,
    )
    .unwrap();
    let request = AlgorithmRequest::new(
        "dtg.graph.bfs",
        ProjectedGraph::Snapshot(graph),
        BTreeMap::from([("source".into(), AlgorithmValue::Vertex(VertexId::new(1)))]),
    )
    .unwrap();
    let manager = AnalyticsJobManager::new(Arc::new(BuiltInProvider::new()), 4).unwrap();
    let job = manager.submit(request).unwrap();
    for _ in 0..100 {
        if matches!(
            manager.status(job).unwrap().state(),
            JobState::Succeeded | JobState::Failed
        ) {
            break;
        }
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(manager.status(job).unwrap().state(), JobState::Succeeded);
    assert_eq!(manager.results(job).unwrap().unwrap().rows().len(), 2);
    assert!(manager.checkpoint(job).unwrap().is_some());
}

#[test]
fn analytics_jobs_can_be_canceled_and_limits_are_enforced() {
    let manager = AnalyticsJobManager::new(Arc::new(BuiltInProvider::new()), 1).unwrap();
    let graph = SnapshotGraph::new(vec![VertexId::new(1)], Vec::new(), true).unwrap();
    let request = AlgorithmRequest::new(
        "dtg.graph.pageRank",
        ProjectedGraph::Snapshot(graph),
        BTreeMap::new(),
    )
    .unwrap();
    let first = manager.submit(request.clone()).unwrap();
    assert!(manager.submit(request).is_err());
    manager.cancel(first).unwrap();
    assert_eq!(manager.status(first).unwrap().state(), JobState::Canceled);
}
