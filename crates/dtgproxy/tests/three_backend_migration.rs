use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::{SystemTime, UNIX_EPOCH};

use adapter_neo4j::Neo4jAdapterFactory;
use adapter_postgres::PostgresAdapter;
use adapter_rocksdb::RocksAdapter;
use analytics_api::{SnapshotEdge, SnapshotGraph, VertexId};
use analytics_runtime::{DegreeCentrality, degree_centrality};
use storage_api::{
    KeySpan, KeyValue, Keyspace, LogicalSnapshotExportRequest, MappingBackedAdapter,
    MappingRequirement, StorageAdapter, TemporalBackendMapping,
};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[derive(Clone, Copy, Debug)]
enum Backend {
    RocksDb,
    PostgreSql,
    Neo4j,
}

impl Backend {
    const ALL_DIRECTIONS: [(Self, Self); 6] = [
        (Self::RocksDb, Self::PostgreSql),
        (Self::PostgreSql, Self::RocksDb),
        (Self::RocksDb, Self::Neo4j),
        (Self::Neo4j, Self::RocksDb),
        (Self::PostgreSql, Self::Neo4j),
        (Self::Neo4j, Self::PostgreSql),
    ];

    const fn name(self) -> &'static str {
        match self {
            Self::RocksDb => "rocksdb",
            Self::PostgreSql => "postgresql",
            Self::Neo4j => "neo4j",
        }
    }
}

struct Environment {
    postgres_url: String,
    neo4j_endpoint: String,
    neo4j_username: String,
    neo4j_password: String,
    neo4j_database: String,
    rocks_root: tempfile::TempDir,
    suffix: u128,
}

impl Environment {
    fn from_process() -> Self {
        Self {
            postgres_url: std::env::var("DTGPROXY_POSTGRES_URL")
                .expect("DTGPROXY_POSTGRES_URL must point to disposable PostgreSQL"),
            neo4j_endpoint: std::env::var("DTGPROXY_NEO4J_ENDPOINT")
                .expect("DTGPROXY_NEO4J_ENDPOINT must point to disposable Neo4j"),
            neo4j_username: std::env::var("DTGPROXY_NEO4J_USERNAME")
                .unwrap_or_else(|_| "neo4j".into()),
            neo4j_password: std::env::var("DTGPROXY_NEO4J_PASSWORD")
                .expect("DTGPROXY_NEO4J_PASSWORD must authenticate to disposable Neo4j"),
            neo4j_database: std::env::var("DTGPROXY_NEO4J_DATABASE")
                .unwrap_or_else(|_| "neo4j".into()),
            rocks_root: tempfile::tempdir().unwrap(),
            suffix: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        }
    }

    fn serving(&self, backend: Backend, direction: usize) -> Arc<dyn TemporalBackendMapping> {
        let id = self.instance_id(backend, direction, "source");
        match backend {
            Backend::RocksDb => Arc::new(
                RocksAdapter::open(self.rocks_root.path().join(&id)).expect("open RocksDB source"),
            ),
            Backend::PostgreSql => Arc::new(
                PostgresAdapter::open(&self.postgres_url, &id, 2).expect("open PostgreSQL source"),
            ),
            Backend::Neo4j => Neo4jAdapterFactory::open_mapping(
                &self.neo4j_endpoint,
                &self.neo4j_database,
                &self.neo4j_username,
                &self.neo4j_password,
                &id,
            )
            .expect("open Neo4j source"),
        }
    }

    fn restore_target(
        &self,
        backend: Backend,
        direction: usize,
    ) -> Arc<dyn TemporalBackendMapping> {
        let id = self.instance_id(backend, direction, "target");
        match backend {
            Backend::RocksDb => Arc::new(
                RocksAdapter::open(self.rocks_root.path().join(&id)).expect("open RocksDB target"),
            ),
            Backend::PostgreSql => Arc::new(
                PostgresAdapter::open_restore_target(&self.postgres_url, &id, 2)
                    .expect("open PostgreSQL restore target"),
            ),
            Backend::Neo4j => Neo4jAdapterFactory::open_restore_mapping(
                &self.neo4j_endpoint,
                &self.neo4j_database,
                &self.neo4j_username,
                &self.neo4j_password,
                &id,
            )
            .expect("open Neo4j restore target"),
        }
    }

    fn instance_id(&self, backend: Backend, direction: usize, role: &str) -> String {
        format!(
            "six-way-{}-{direction}-{role}-{}",
            backend.name(),
            self.suffix
        )
    }
}

#[derive(Clone, Copy)]
struct Fixture {
    vertices: [ElementRef; 3],
    edges: [(ElementRef, ElementRef, ElementRef); 2],
}

#[derive(Debug, Eq, PartialEq)]
struct QuerySignature {
    vertices: Vec<Option<CanonicalElement>>,
    edges: Vec<Option<CanonicalElement>>,
    outgoing: Vec<KeyValue>,
    incoming: Vec<KeyValue>,
}

#[test]
fn live_six_direction_temporal_graph_migration_matrix() {
    let environment = Environment::from_process();
    for (direction, (source_backend, target_backend)) in
        Backend::ALL_DIRECTIONS.into_iter().enumerate()
    {
        let source = environment.serving(source_backend, direction);
        let target = environment.restore_target(target_backend, direction);
        let fixture = populate(source.clone());
        let source_signature = query_signature(source.clone(), fixture);
        let source_degree = degree_at(source.clone(), fixture);
        let source_entries = migrate(source.as_ref(), target.as_ref());
        let target_entries = export_entries(target.as_ref());

        assert_eq!(
            target_entries, source_entries,
            "{source_backend:?} -> {target_backend:?} canonical snapshot"
        );
        assert_eq!(
            query_signature(target.clone(), fixture),
            source_signature,
            "{source_backend:?} -> {target_backend:?} query signature"
        );
        assert_eq!(
            degree_at(target.clone(), fixture),
            source_degree,
            "{source_backend:?} -> {target_backend:?} fixed-snapshot Degree"
        );
        continue_after_restore(target.clone(), fixture.vertices[2]);
        assert_eq!(
            target.applied_log_index().expect("target applied index"),
            6,
            "{source_backend:?} -> {target_backend:?} continued index"
        );
    }
}

fn populate(mapping: Arc<dyn TemporalBackendMapping>) -> Fixture {
    let store = store(mapping);
    let graph = GraphId::new(77);
    let vertices = [
        ElementRef::vertex(graph, PartitionId::new(1), ElementId::new(1)),
        ElementRef::vertex(graph, PartitionId::new(1), ElementId::new(2)),
        ElementRef::vertex(graph, PartitionId::new(2), ElementId::new(3)),
    ];
    let edges = [
        (
            ElementRef::edge(graph, PartitionId::new(1), ElementId::new(10)),
            vertices[0],
            vertices[1],
        ),
        (
            ElementRef::edge(graph, PartitionId::new(1), ElementId::new(11)),
            vertices[0],
            vertices[2],
        ),
    ];
    let forever = interval(1, None);
    let mut vertices_txn = TemporalTransaction::new();
    for (ordinal, vertex) in vertices.into_iter().enumerate() {
        vertices_txn = vertices_txn.with_vertex(
            VertexMutation::put(
                vertex,
                LabelId::new(1),
                forever,
                payload(1, &format!("vertex-{ordinal}")),
            )
            .unwrap(),
        );
    }
    block_on(store.commit_transaction(context(1, 1_001, 10, 20), vertices_txn)).unwrap();
    for (offset, (edge, source, destination)) in edges.into_iter().enumerate() {
        block_on(
            store.commit_edge(
                context(
                    u64::try_from(offset).unwrap() + 2,
                    u128::try_from(offset).unwrap() + 1_002,
                    20 + i64::try_from(offset).unwrap() * 10,
                    30 + i64::try_from(offset).unwrap() * 10,
                ),
                EdgeMutation::put_between(
                    edge,
                    EdgeTypeId::new(9),
                    source,
                    destination,
                    forever,
                    payload(2, &format!("edge-{offset}")),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }
    block_on(
        store.commit_vertex(
            context(4, 1_004, 40, 50),
            VertexMutation::put(
                vertices[0],
                LabelId::new(1),
                interval(4, Some(8)),
                payload(1, "vertex-updated"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
    block_on(
        store.commit_edge(
            context(5, 1_005, 50, 60),
            EdgeMutation::delete_between(
                edges[1].0,
                EdgeTypeId::new(9),
                edges[1].1,
                edges[1].2,
                interval(7, Some(9)),
            )
            .unwrap(),
        ),
    )
    .unwrap();

    Fixture { vertices, edges }
}

fn query_signature(mapping: Arc<dyn TemporalBackendMapping>, fixture: Fixture) -> QuerySignature {
    let store = store(mapping);
    QuerySignature {
        vertices: fixture
            .vertices
            .into_iter()
            .map(|vertex| {
                block_on(store.vertex_current(vertex, ValidTime::from_micros(5))).unwrap()
            })
            .collect(),
        edges: fixture
            .edges
            .into_iter()
            .map(|(edge, _, _)| {
                block_on(store.edge_current(edge, ValidTime::from_micros(5))).unwrap()
            })
            .collect(),
        outgoing: block_on(
            store
                .adapter()
                .scan(&KeySpan::prefix(Keyspace::AdjOut, Vec::new())),
        )
        .unwrap(),
        incoming: block_on(
            store
                .adapter()
                .scan(&KeySpan::prefix(Keyspace::AdjIn, Vec::new())),
        )
        .unwrap(),
    }
}

fn degree_at(
    mapping: Arc<dyn TemporalBackendMapping>,
    fixture: Fixture,
) -> BTreeMap<VertexId, DegreeCentrality> {
    let store = store(mapping);
    let vertices = fixture
        .vertices
        .into_iter()
        .filter(|vertex| {
            block_on(store.vertex_current(*vertex, ValidTime::from_micros(5)))
                .unwrap()
                .is_some()
        })
        .map(|vertex| VertexId::new(u128::from(vertex.id().value())))
        .collect::<Vec<_>>();
    let edges = fixture
        .edges
        .into_iter()
        .filter_map(|(edge, source, destination)| {
            block_on(store.edge_current(edge, ValidTime::from_micros(5)))
                .unwrap()
                .map(|_| {
                    SnapshotEdge::new(
                        VertexId::new(u128::from(source.id().value())),
                        VertexId::new(u128::from(destination.id().value())),
                        1.0,
                    )
                    .unwrap()
                })
        })
        .collect::<Vec<_>>();
    degree_centrality(&SnapshotGraph::new(vertices, edges, true).unwrap())
}

fn migrate(
    source: &dyn TemporalBackendMapping,
    target: &dyn TemporalBackendMapping,
) -> Vec<KeyValue> {
    let mut reader = block_on(source.export_canonical(LogicalSnapshotExportRequest::default()))
        .expect("begin source export");
    let mut restore =
        block_on(target.restore_canonical(reader.header().clone())).expect("begin target restore");
    let mut entries = Vec::new();
    while let Some(chunk) = block_on(reader.next_chunk()).expect("read source chunk") {
        entries.extend_from_slice(chunk.entries());
        block_on(restore.write_chunk(chunk)).expect("write target chunk");
    }
    let manifest = block_on(reader.finish()).expect("finish source export");
    block_on(restore.commit(manifest)).expect("publish target restore");
    entries
}

fn export_entries(mapping: &dyn TemporalBackendMapping) -> Vec<KeyValue> {
    let mut reader = block_on(mapping.export_canonical(LogicalSnapshotExportRequest::default()))
        .expect("begin target export");
    let mut entries = Vec::new();
    while let Some(chunk) = block_on(reader.next_chunk()).expect("read target chunk") {
        entries.extend_from_slice(chunk.entries());
    }
    block_on(reader.finish()).expect("finish target export");
    entries
}

fn continue_after_restore(mapping: Arc<dyn TemporalBackendMapping>, vertex: ElementRef) {
    let store = store(mapping);
    block_on(
        store.commit_vertex(
            context(6, 1_006, 60, 70),
            VertexMutation::put(
                vertex,
                LabelId::new(1),
                interval(10, Some(11)),
                payload(1, "continued"),
            )
            .unwrap(),
        ),
    )
    .unwrap();
}

fn store(mapping: Arc<dyn TemporalBackendMapping>) -> TemporalStore<Arc<dyn StorageAdapter>> {
    let adapter: Arc<dyn StorageAdapter> = Arc::new(
        MappingBackedAdapter::new(mapping, MappingRequirement::HotPluggableReplica).unwrap(),
    );
    TemporalStore::new(adapter)
}

fn context(
    log_index: u64,
    txn_id: u128,
    read_physical: i64,
    commit_physical: i64,
) -> CommitContext {
    CommitContext::new(
        1,
        log_index,
        txn_id,
        TransactionTime::new(read_physical, 0),
        TransactionTime::new(commit_physical, 0),
    )
}

fn interval(start: i64, end: Option<i64>) -> Interval<ValidTime> {
    Interval::new(
        ValidTime::from_micros(start),
        end.map(ValidTime::from_micros),
    )
    .unwrap()
}

fn payload(property: u32, value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(property, GraphValue::String(value.to_owned()))]),
    )
}

struct NoopWake;

impl Wake for NoopWake {
    fn wake(self: Arc<Self>) {}
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWake));
    let mut context = Context::from_waker(&waker);
    let mut future = std::pin::pin!(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
