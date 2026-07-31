mod backend_e2e_support;

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use backend_e2e_support::{
    Backend, CellSpec, DiagnosticCluster, DiagnosticRuntime, Workload, measure_cell,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires release DTGProxy binaries"]
async fn fjall_cell_uses_real_four_process_bolt_path() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    let spec = CellSpec::one(Backend::Fjall, Workload::PointLookup, 1, 1);
    let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
    cluster.seed_read_dataset(4_096).await.unwrap();
    let observation = measure_cell(cluster.bolt_address(), spec).await.unwrap();
    assert_eq!(observation.errors, 0);
    assert_eq!(observation.row_count, 1);
    cluster.shutdown().await.unwrap();
}

#[test]
fn matrix_has_exact_diagnostic_cells() {
    let cells = backend_e2e_support::CellSpec::matrix(4_923_929_926_749_575_257);
    assert_eq!(cells.len(), 54);
    assert_eq!(
        cells.iter().filter(|cell| cell.concurrency == 8).count(),
        27
    );
}

#[test]
fn percentiles_use_nearest_rank() {
    let samples = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    assert_eq!(backend_e2e_support::percentile_ns(&samples, 50), 50);
    assert_eq!(backend_e2e_support::percentile_ns(&samples, 95), 100);
    assert_eq!(backend_e2e_support::percentile_ns(&samples, 99), 100);
}

#[tokio::test]
async fn bolt_session_reuses_a_single_socket_and_keeps_read_identity_stable() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted_connections = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::clone(&accepted_connections);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        accepted.fetch_add(1, Ordering::SeqCst);
        serve_fake_bolt_session(&mut socket, 2).await;
    });

    let mut session = backend_e2e_support::BoltSession::connect(address)
        .await
        .unwrap();
    let first: backend_e2e_support::BoltResult = session
        .run(
            "MATCH (n) WHERE n.id = $id RETURN n.id",
            BTreeMap::<String, backend_e2e_support::BoltValue>::new(),
        )
        .await
        .unwrap();
    let second = session
        .run("MATCH (n) WHERE n.id = $id RETURN n.id", BTreeMap::new())
        .await
        .unwrap();

    assert_eq!(accepted_connections.load(Ordering::SeqCst), 1);
    assert_eq!(first.fields, vec!["n.id"]);
    assert_eq!(first.rows.len(), 1);
    assert_eq!(first.result_digest, second.result_digest);
    drop(session);
    server.await.unwrap();
}

#[tokio::test]
async fn measure_cell_records_each_measured_read_on_one_worker_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        serve_fake_bolt_until_closed(&mut socket).await;
    });
    let cell = backend_e2e_support::CellSpec {
        backend: backend_e2e_support::Backend::Fjall,
        workload: backend_e2e_support::Workload::PointLookup,
        concurrency: 1,
        repetition: 0,
    };

    let warmup = Duration::from_millis(10);
    let observation = backend_e2e_support::measure_cell_with_durations(
        address,
        cell,
        warmup,
        Duration::from_millis(10),
    )
    .await
    .unwrap();

    assert!(observation.operations > 0);
    assert_eq!(observation.errors, 0);
    assert_eq!(observation.row_count, 1);
    assert_eq!(observation.result_digest.len(), 64);
    assert_eq!(observation.query_digest.len(), 64);
    let artifact = serde_json::to_value(&observation).unwrap();
    let warmup_finished_at_unix_ns = artifact["warmup_finished_at_unix_ns"].as_u64().unwrap();
    let measurement_started_at_unix_ns =
        artifact["measurement_started_at_unix_ns"].as_u64().unwrap();
    let measurement_finished_at_unix_ns = artifact["measurement_finished_at_unix_ns"]
        .as_u64()
        .unwrap();
    assert_eq!(warmup_finished_at_unix_ns, measurement_started_at_unix_ns);
    assert!(
        measurement_started_at_unix_ns
            > observation
                .started_at_unix_ns
                .saturating_add(warmup.as_nanos() as u64),
        "the measurement boundary must be sampled when warmup actually finishes"
    );
    assert!(measurement_finished_at_unix_ns >= measurement_started_at_unix_ns);
    server.await.unwrap();
}

#[tokio::test]
async fn measure_cell_returns_an_error_when_a_measurement_bolt_operation_fails() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        serve_fake_bolt_failure_during_measurement(&mut socket).await;
    });
    let cell = backend_e2e_support::CellSpec {
        backend: backend_e2e_support::Backend::Fjall,
        workload: backend_e2e_support::Workload::PointLookup,
        concurrency: 1,
        repetition: 0,
    };

    let error = backend_e2e_support::measure_cell_with_durations(
        address,
        cell,
        Duration::ZERO,
        Duration::from_millis(10),
    )
    .await
    .expect_err("a Bolt failure in the measurement window must reject the cell");

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .to_string()
            .contains("Neo.ClientError.Statement.SyntaxError")
    );
    server.await.unwrap();
}

#[test]
fn summary_groups_raw_observations_and_serializes_snake_case_enums() {
    let observation = backend_e2e_support::RawObservation {
        backend: backend_e2e_support::Backend::Fjall,
        workload: backend_e2e_support::Workload::PointLookup,
        concurrency: 8,
        repetition: 0,
        started_at_unix_ns: 10,
        finished_at_unix_ns: 20,
        warmup_finished_at_unix_ns: 15,
        measurement_started_at_unix_ns: 15,
        measurement_finished_at_unix_ns: 20,
        measured_duration_ns: 10,
        operations: 3,
        errors: 0,
        latency_samples_ns: vec![10, 20, 30],
        row_count: 1,
        result_digest: "result".into(),
        query_digest: "query".into(),
    };

    let summary: Vec<backend_e2e_support::Summary> =
        backend_e2e_support::summarize(&[observation.clone()]);
    assert_eq!(summary.len(), 1);
    assert_eq!(summary[0].p95_ns, 30);
    let artifact = serde_json::to_value(observation).unwrap();
    assert_eq!(artifact["backend"], "fjall");
    assert_eq!(artifact["workload"], "point_lookup");
}

async fn serve_fake_bolt_session(socket: &mut TcpStream, exchanges: usize) {
    let mut handshake = [0_u8; 20];
    socket.read_exact(&mut handshake).await.unwrap();
    assert_eq!(&handshake[..4], &[0x60, 0x60, 0xb0, 0x17]);
    socket.write_all(&[0, 0, 4, 5]).await.unwrap();

    let hello = read_bolt_message(socket).await;
    assert_eq!(hello[1], 0x01);
    write_bolt_message(socket, &[0xb1, 0x70, 0xa0]).await;

    for _ in 0..exchanges {
        let run = read_bolt_message(socket).await;
        assert_eq!(run[1], 0x10);
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x86, b'f', b'i', b'e', b'l', b'd', b's', 0x91, 0x84, b'n', b'.',
                b'i', b'd',
            ],
        )
        .await;

        let pull = read_bolt_message(socket).await;
        assert_eq!(pull, vec![0xb1, 0x3f, 0xa0]);
        write_bolt_message(socket, &[0xb1, 0x71, 0x91, 0xc9, 0x08, 0x00]).await;
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x88, b'h', b'a', b's', b'_', b'm', b'o', b'r', b'e', 0xc2,
            ],
        )
        .await;
    }
}

async fn serve_fake_bolt_until_closed(socket: &mut TcpStream) {
    let mut handshake = [0_u8; 20];
    socket.read_exact(&mut handshake).await.unwrap();
    socket.write_all(&[0, 0, 4, 5]).await.unwrap();
    if try_read_bolt_message(socket).await.is_err() {
        return;
    }
    write_bolt_message(socket, &[0xb1, 0x70, 0xa0]).await;
    loop {
        let Ok(run) = try_read_bolt_message(socket).await else {
            return;
        };
        assert_eq!(run[1], 0x10);
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x86, b'f', b'i', b'e', b'l', b'd', b's', 0x91, 0x84, b'n', b'.',
                b'i', b'd',
            ],
        )
        .await;
        let Ok(pull) = try_read_bolt_message(socket).await else {
            return;
        };
        assert_eq!(pull, vec![0xb1, 0x3f, 0xa0]);
        write_bolt_message(socket, &[0xb1, 0x71, 0x91, 0xc9, 0x08, 0x00]).await;
        write_bolt_message(
            socket,
            &[
                0xb1, 0x70, 0xa1, 0x88, b'h', b'a', b's', b'_', b'm', b'o', b'r', b'e', 0xc2,
            ],
        )
        .await;
    }
}

async fn serve_fake_bolt_failure_during_measurement(socket: &mut TcpStream) {
    let mut handshake = [0_u8; 20];
    socket.read_exact(&mut handshake).await.unwrap();
    socket.write_all(&[0, 0, 4, 5]).await.unwrap();
    let hello = read_bolt_message(socket).await;
    assert_eq!(hello[1], 0x01);
    write_bolt_message(socket, &[0xb1, 0x70, 0xa0]).await;

    let run = read_bolt_message(socket).await;
    assert_eq!(run[1], 0x10);
    write_bolt_message(
        socket,
        &[
            0xb1, 0x7f, 0xa2, 0x84, b'c', b'o', b'd', b'e', 0xd0, 0x25, b'N', b'e', b'o', b'.',
            b'C', b'l', b'i', b'e', b'n', b't', b'E', b'r', b'r', b'o', b'r', b'.', b'S', b't',
            b'a', b't', b'e', b'm', b'e', b'n', b't', b'.', b'S', b'y', b'n', b't', b'a', b'x',
            b'E', b'r', b'r', b'o', b'r', 0x87, b'm', b'e', b's', b's', b'a', b'g', b'e', 0x84,
            b'b', b'o', b'o', b'm',
        ],
    )
    .await;
}

async fn read_bolt_message(socket: &mut TcpStream) -> Vec<u8> {
    try_read_bolt_message(socket).await.unwrap()
}

async fn try_read_bolt_message(socket: &mut TcpStream) -> io::Result<Vec<u8>> {
    let mut message = Vec::new();
    loop {
        let mut size = [0_u8; 2];
        socket.read_exact(&mut size).await?;
        let size = usize::from(u16::from_be_bytes(size));
        if size == 0 {
            return Ok(message);
        }
        let start = message.len();
        message.resize(start + size, 0);
        socket.read_exact(&mut message[start..]).await?;
    }
}

async fn write_bolt_message(socket: &mut TcpStream, message: &[u8]) {
    socket
        .write_all(&(message.len() as u16).to_be_bytes())
        .await
        .unwrap();
    socket.write_all(message).await.unwrap();
    socket.write_all(&[0, 0]).await.unwrap();
}
