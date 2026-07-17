use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use adapter_memory::MemoryAdapter;
use query_executor::LocalExecutor;
use temporal_ir::{DiffOperator, GraphScope, PointOperator, TemporalPlan, TemporalSelector};
use temporal_storage::{
    CommitContext, EdgeMutation, EdgeTypeId, ElementId, ElementKind, ElementRef, GraphId, LabelId,
    PartitionId, TemporalStore, TemporalTransaction, VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

#[test]
fn canonical_json_preserves_typed_payloads_edge_identity_and_change_ranges() {
    let before = payload("before");
    let after = payload("after");
    let edge_payload = payload("edge");
    let store = TemporalStore::new(MemoryAdapter::new());
    let initial = TemporalTransaction::new()
        .with_vertex(
            VertexMutation::put(vertex(1), LabelId::new(1), interval(1, 10), before.clone())
                .unwrap(),
        )
        .with_vertex(
            VertexMutation::put(
                vertex(2),
                LabelId::new(1),
                interval(1, 10),
                payload("destination"),
            )
            .unwrap(),
        )
        .with_edge(
            EdgeMutation::put(
                edge(10),
                EdgeTypeId::new(9),
                ElementId::new(1),
                ElementId::new(2),
                interval(1, 10),
                edge_payload.clone(),
            )
            .unwrap(),
        );
    block_on(store.commit_transaction(context(1, 0, 100), initial)).unwrap();
    block_on(store.commit_vertex(
        context(2, 100, 200),
        VertexMutation::put(vertex(1), LabelId::new(1), interval(1, 10), after.clone()).unwrap(),
    ))
    .unwrap();
    let executor = LocalExecutor::new(store);

    let vertex_result = block_on(executor.execute(&TemporalPlan::point(
        scope(),
        PointOperator::VertexById(ElementId::new(1)),
        valid(5),
        TemporalSelector::Current,
        1,
    )))
    .unwrap();
    assert_eq!(
        vertex_result.to_canonical_json().unwrap(),
        format!(
            "{{\"version\":1,\"records\":[{{\"type\":\"vertex\",\"graph\":\"1\",\"partition\":\"0\",\"element_id\":\"1\",\"payload_dtp1\":\"{}\"}}]}}",
            hex(&after)
        )
    );

    let edge_result = block_on(executor.execute(&TemporalPlan::point(
        scope(),
        PointOperator::EdgeById(ElementId::new(10)),
        valid(5),
        TemporalSelector::Current,
        1,
    )))
    .unwrap();
    assert_eq!(
        edge_result.to_canonical_json().unwrap(),
        format!(
            "{{\"version\":1,\"records\":[{{\"type\":\"edge\",\"graph\":\"1\",\"partition\":\"0\",\"element_id\":\"10\",\"edge_type\":\"9\",\"source_id\":\"1\",\"destination_id\":\"2\",\"payload_dtp1\":\"{}\"}}]}}",
            hex(&edge_payload)
        )
    );

    let diff_result = block_on(executor.execute(&TemporalPlan::diff(
        scope(),
        DiffOperator::Element {
            kind: ElementKind::Vertex,
            id: ElementId::new(1),
        },
        tx(150),
        tx(250),
        8,
    )))
    .unwrap();
    assert_eq!(
        diff_result.to_canonical_json().unwrap(),
        format!(
            "{{\"version\":1,\"records\":[{{\"type\":\"change\",\"element_kind\":\"vertex\",\"graph\":\"1\",\"partition\":\"0\",\"element_id\":\"1\",\"valid_start_micros\":\"1\",\"valid_end_micros\":\"10\",\"change\":\"changed\",\"before_dtp1\":\"{}\",\"after_dtp1\":\"{}\"}}]}}",
            hex(&before),
            hex(&after)
        )
    );
}

fn hex(payload: &CanonicalElement) -> String {
    payload
        .encode()
        .unwrap()
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn scope() -> GraphScope {
    GraphScope::new(GraphId::new(1), PartitionId::new(0))
}

fn vertex(id: u128) -> ElementRef {
    ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(id))
}

fn edge(id: u128) -> ElementRef {
    ElementRef::edge(GraphId::new(1), PartitionId::new(0), ElementId::new(id))
}

fn context(log_index: u64, read: i64, commit: i64) -> CommitContext {
    CommitContext::new(3, log_index, u128::from(log_index), tx(read), tx(commit))
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn valid(value: i64) -> ValidTime {
    ValidTime::from_micros(value)
}

fn interval(start: i64, end: i64) -> Interval<ValidTime> {
    Interval::new(valid(start), Some(valid(end))).unwrap()
}

fn payload(value: &str) -> CanonicalElement {
    CanonicalElement::new(
        4,
        BTreeMap::from([
            (1, GraphValue::String(value.to_owned())),
            (2, GraphValue::FloatBits(f64::NAN.to_bits())),
            (3, GraphValue::Bytes(vec![0, 255])),
        ]),
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
