use std::collections::BTreeMap;

use analytics_api::{
    AlgorithmRequest, AlgorithmValue, AnalyticsProvider, ProjectedGraph, SnapshotEdge,
    SnapshotGraph, VertexId,
};
use analytics_runtime::BuiltInProvider;

#[test]
fn builtin_provider_catalog_and_typed_execution_share_one_spi() {
    let provider = BuiltInProvider::new();
    let names = provider
        .algorithms()
        .into_iter()
        .map(|descriptor| descriptor.name().to_owned())
        .collect::<Vec<_>>();
    assert!(names.contains(&"dtg.graph.bfs".to_owned()));
    assert!(names.contains(&"dtg.temporal.earliestArrival".to_owned()));
    assert!(!provider.descriptor().distributed());

    let graph = SnapshotGraph::new(
        vec![VertexId::new(1), VertexId::new(2)],
        vec![SnapshotEdge::new(VertexId::new(1), VertexId::new(2), 1.0).expect("edge")],
        true,
    )
    .expect("graph");
    let result = provider
        .execute(
            AlgorithmRequest::new(
                "dtg.graph.bfs",
                ProjectedGraph::Snapshot(graph),
                BTreeMap::from([("source".into(), AlgorithmValue::Vertex(VertexId::new(1)))]),
            )
            .expect("request"),
        )
        .expect("execute");
    assert_eq!(result.columns(), &["vertexId", "distance", "predecessor"]);
    assert_eq!(result.rows().len(), 2);
}
