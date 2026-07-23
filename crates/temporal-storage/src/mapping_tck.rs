use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use storage_api::{
    KeySpan, KeyValue, Keyspace, LogicalSnapshotChunkV1, LogicalSnapshotExportRequest,
    LogicalSnapshotHeaderV1, LogicalSnapshotManifestV1, MappingBackedAdapter, MappingRequirement,
    StorageAdapter, TemporalBackendMapping,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

use crate::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementRef, GraphId, LabelId, PartitionId,
    TemporalStore, TemporalTransaction, VertexMutation, cross_in_adjacency_key,
    cross_out_adjacency_key, current_edge_key, current_vertex_key, decode_canonical_graph_entry,
    edge_identity_key, history_prefix, vertex_identity_key,
};

/// Runs the backend-independent temporal graph portion of the Mapping TCK.
///
/// Both mappings must be fresh: `source` is a serving mapping at applied index zero and
/// `destination` is an unpublished restore target. The test exercises every graph key family,
/// cross-partition adjacency, temporal deletion/history retention, canonical export/restore and
/// continued writes after publication.
pub fn run_temporal_graph_mapping_tck(
    source: Arc<dyn TemporalBackendMapping>,
    destination: Arc<dyn TemporalBackendMapping>,
) {
    assert_eq!(source.applied_log_index().expect("source applied index"), 0);
    let source_adapter: Arc<dyn StorageAdapter> = Arc::new(
        MappingBackedAdapter::new(source.clone(), MappingRequirement::HotPluggableReplica)
            .expect("source Mapping adapter"),
    );
    let source_store = TemporalStore::new(source_adapter);
    let graph = GraphId::new(0x44);
    let source_vertex = ElementRef::vertex(graph, PartitionId::new(3), ElementId::new(11));
    let destination_vertex = ElementRef::vertex(graph, PartitionId::new(9), ElementId::new(12));
    let edge = ElementRef::edge(graph, PartitionId::new(3), ElementId::new(21));
    let edge_type = EdgeTypeId::new(5);
    let valid = Interval::new(ValidTime::from_micros(1), None).expect("valid interval");
    let source_payload = payload(1, "source");
    let destination_payload = payload(1, "destination");
    let edge_payload = payload(2, "edge");

    let vertices = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(
                source_vertex,
                LabelId::new(7),
                valid,
                source_payload.clone(),
            )
            .expect("source vertex mutation"),
        )
        .with_vertex(
            VertexMutation::put(
                destination_vertex,
                LabelId::new(8),
                valid,
                destination_payload.clone(),
            )
            .expect("destination vertex mutation"),
        );
    block_on(source_store.commit_transaction(context(1, 901, 10, 20), vertices))
        .expect("commit graph TCK vertices");
    block_on(
        source_store.commit_edge(
            context(2, 902, 20, 30),
            EdgeMutation::put_between(
                edge,
                edge_type,
                source_vertex,
                destination_vertex,
                valid,
                edge_payload.clone(),
            )
            .expect("cross-partition edge mutation"),
        ),
    )
    .expect("commit graph TCK edge");

    assert_eq!(
        block_on(source_store.vertex_current(source_vertex, ValidTime::from_micros(5)))
            .expect("source current vertex"),
        Some(source_payload.clone())
    );
    assert_eq!(
        block_on(source_store.edge_current(edge, ValidTime::from_micros(5)))
            .expect("source current edge"),
        Some(edge_payload)
    );
    assert_present(
        source.as_ref(),
        &[
            vertex_identity_key(source_vertex),
            vertex_identity_key(destination_vertex),
            edge_identity_key(edge),
            current_vertex_key(source_vertex),
            current_vertex_key(destination_vertex),
            current_edge_key(edge),
            cross_out_adjacency_key(
                graph,
                source_vertex.partition(),
                source_vertex.id(),
                edge_type,
                0,
                destination_vertex.partition(),
                destination_vertex.id(),
                edge.partition(),
                edge.id(),
            ),
            cross_in_adjacency_key(
                graph,
                destination_vertex.partition(),
                destination_vertex.id(),
                edge_type,
                0,
                source_vertex.partition(),
                source_vertex.id(),
                edge.partition(),
                edge.id(),
            ),
        ],
    );

    block_on(
        source_store.commit_edge(
            context(3, 903, 30, 40),
            EdgeMutation::delete_between(edge, edge_type, source_vertex, destination_vertex, valid)
                .expect("edge deletion mutation"),
        ),
    )
    .expect("commit graph TCK edge deletion");
    assert_eq!(
        block_on(source_store.edge_current(edge, ValidTime::from_micros(5)))
            .expect("deleted current edge"),
        None
    );
    let out_key = cross_out_adjacency_key(
        graph,
        source_vertex.partition(),
        source_vertex.id(),
        edge_type,
        0,
        destination_vertex.partition(),
        destination_vertex.id(),
        edge.partition(),
        edge.id(),
    );
    let in_key = cross_in_adjacency_key(
        graph,
        destination_vertex.partition(),
        destination_vertex.id(),
        edge_type,
        0,
        source_vertex.partition(),
        source_vertex.id(),
        edge.partition(),
        edge.id(),
    );
    assert_eq!(
        block_on(source.multi_get(&[out_key, in_key])).expect("deleted adjacency read"),
        vec![None, None]
    );
    assert_eq!(
        block_on(source.scan(&KeySpan::prefix(Keyspace::History, history_prefix(edge))))
            .expect("edge history scan")
            .len(),
        2
    );

    let (header, chunks, manifest, source_entries) = export(source.as_ref());
    for entry in &source_entries {
        decode_canonical_graph_entry(entry.key(), entry.value())
            .expect("exported graph entry must be canonical");
    }
    let mut restore = block_on(destination.restore_canonical(header))
        .expect("begin temporal graph canonical restore");
    for chunk in chunks {
        block_on(restore.write_chunk(chunk)).expect("write temporal graph restore chunk");
    }
    block_on(restore.commit(manifest)).expect("commit temporal graph restore");
    let (_, _, _, destination_entries) = export(destination.as_ref());
    assert_eq!(destination_entries, source_entries);

    let destination_adapter: Arc<dyn StorageAdapter> = Arc::new(
        MappingBackedAdapter::new(destination.clone(), MappingRequirement::HotPluggableReplica)
            .expect("restored Mapping adapter"),
    );
    let destination_store = TemporalStore::new(destination_adapter);
    block_on(
        destination_store.commit_vertex(
            context(4, 904, 40, 50),
            VertexMutation::put(
                source_vertex,
                LabelId::new(7),
                Interval::new(ValidTime::from_micros(2), Some(ValidTime::from_micros(3)))
                    .expect("continued valid interval"),
                payload(1, "continued"),
            )
            .expect("continued vertex mutation"),
        ),
    )
    .expect("continue writing after graph restore");
    assert_eq!(
        destination
            .applied_log_index()
            .expect("destination applied index"),
        4
    );
}

fn payload(property: u32, value: &str) -> CanonicalElement {
    CanonicalElement::new(
        1,
        BTreeMap::from([(property, GraphValue::String(value.to_owned()))]),
    )
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

fn assert_present(mapping: &dyn TemporalBackendMapping, keys: &[storage_api::LogicalKey]) {
    let values = block_on(mapping.multi_get(keys)).expect("canonical graph multi-get");
    assert_eq!(values.len(), keys.len());
    assert!(values.iter().all(Option::is_some));
}

type CanonicalExport = (
    LogicalSnapshotHeaderV1,
    Vec<LogicalSnapshotChunkV1>,
    LogicalSnapshotManifestV1,
    Vec<KeyValue>,
);

fn export(mapping: &dyn TemporalBackendMapping) -> CanonicalExport {
    let mut reader = block_on(mapping.export_canonical(
        LogicalSnapshotExportRequest::new(3, 4_096).expect("graph TCK snapshot limits"),
    ))
    .expect("begin graph TCK export");
    let header = reader.header().clone();
    let mut chunks = Vec::new();
    let mut entries = Vec::new();
    while let Some(chunk) = block_on(reader.next_chunk()).expect("read graph TCK export chunk") {
        entries.extend_from_slice(chunk.entries());
        chunks.push(chunk);
    }
    let manifest = block_on(reader.finish()).expect("finish graph TCK export");
    (header, chunks, manifest, entries)
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
