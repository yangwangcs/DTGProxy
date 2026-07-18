use std::process::Command;

use adapter_rocksdb::RocksAdapter;
use temporal_storage::{
    CommitContext, ElementId, ElementRef, GraphId, LabelId, PartitionId, TemporalStore,
    VertexMutation,
};
use temporal_types::{CanonicalElement, GraphValue, Interval, TransactionTime, ValidTime};

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

fn command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_dtgproxy"))
}

#[test]
fn version_reports_product_name_and_workspace_version() {
    let output = command().arg("--version").output().unwrap();

    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        "DTGProxy 1.0.0\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn no_arguments_reports_v1_commands_and_usage() {
    let output = command().output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("DTGProxy 1.0 prototype"));
    assert!(stdout.contains("dtgproxy init"));
    assert!(stdout.contains("dtgproxy serve"));
    assert!(stdout.contains("dtgproxy transaction"));
    assert!(stdout.contains("dtgproxy query --db <path> --text <query>"));
    assert!(output.stderr.is_empty());
}

#[test]
fn unknown_argument_exits_with_usage_error() {
    let output = command().arg("--unknown").output().unwrap();

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unknown argument: --unknown"));
    assert!(stderr.contains("dtgproxy query --db <path> --text <query>"));
}

#[test]
fn query_executes_against_rocksdb_and_prints_canonical_json() {
    let directory = tempfile::tempdir().unwrap();
    let payload = CanonicalElement::new(
        1,
        BTreeMap::from([(1, GraphValue::String("vertex".to_owned()))]),
    );
    {
        let store = TemporalStore::new(RocksAdapter::open(directory.path()).unwrap());
        block_on(
            store.commit_vertex(
                CommitContext::new(3, 1, 1, tx(0), tx(100)),
                VertexMutation::put(
                    ElementRef::vertex(GraphId::new(1), PartitionId::new(0), ElementId::new(7)),
                    LabelId::new(1),
                    Interval::new(ValidTime::from_micros(1), Some(ValidTime::from_micros(10)))
                        .unwrap(),
                    payload.clone(),
                )
                .unwrap(),
            ),
        )
        .unwrap();
    }

    let output = command()
        .args([
            "query",
            "--db",
            directory.path().to_str().unwrap(),
            "--text",
            "VERTEX 7 GRAPH 1 PARTITION 0 FOR VALID TIME 5 CURRENT LIMIT 1",
        ])
        .output()
        .unwrap();

    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    assert_eq!(
        String::from_utf8(output.stdout).unwrap(),
        format!(
            "{{\"version\":1,\"records\":[{{\"type\":\"vertex\",\"graph\":\"1\",\"partition\":\"0\",\"element_id\":\"7\",\"payload_dtp1\":\"{}\"}}]}}\n",
            hex(&payload)
        )
    );
}

#[test]
fn query_reports_parser_and_usage_errors_without_opening_a_backend() {
    let invalid_query = command()
        .args([
            "query",
            "--db",
            "/path/that/must/not/be/opened",
            "--text",
            "MATCH (n) RETURN n",
        ])
        .output()
        .unwrap();
    assert_eq!(invalid_query.status.code(), Some(2));
    assert!(invalid_query.stdout.is_empty());
    assert!(
        String::from_utf8(invalid_query.stderr)
            .unwrap()
            .contains("unsupported temporal query statement MATCH")
    );

    let missing_text = command()
        .args(["query", "--db", "/tmp/db"])
        .output()
        .unwrap();
    assert_eq!(missing_text.status.code(), Some(2));
    assert!(
        String::from_utf8(missing_text.stderr)
            .unwrap()
            .contains("dtgproxy query --db <path> --text <query>")
    );
}

fn tx(value: i64) -> TransactionTime {
    TransactionTime::new(value, 0)
}

fn hex(payload: &CanonicalElement) -> String {
    payload
        .encode()
        .unwrap()
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
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
