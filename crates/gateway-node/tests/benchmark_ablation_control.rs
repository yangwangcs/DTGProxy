use gateway_node::{BenchmarkAblationRuntime, BenchmarkControlError};
use query_executor::{AblationAxis, BenchmarkAblationConfig};

#[cfg(unix)]
use gateway_node::{
    BenchmarkAblationWireConfig, BenchmarkControlRequest, BenchmarkControlResponse,
    serve_benchmark_ablation_control,
};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
#[cfg(unix)]
use std::sync::Arc;
#[cfg(unix)]
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
#[cfg(unix)]
use tokio::net::UnixStream;

fn digest(character: char) -> String {
    std::iter::repeat_n(character, 64).collect()
}

#[test]
fn one_session_issues_a_token_and_audits_successful_query_leases() {
    let runtime = BenchmarkAblationRuntime::new();
    let config = BenchmarkAblationConfig {
        parallel_shard_fanout: false,
        ..BenchmarkAblationConfig::default()
    };
    let begun = runtime
        .begin_cell("run-1:42", &digest('a'), config)
        .unwrap();
    assert_eq!(begun.accepted_config(), config);
    assert_eq!(begun.session_token().len(), 64);

    let lease = runtime.acquire(begun.session_token()).unwrap();
    assert_eq!(lease.config(), config);
    lease.counters().record_serial_shard_open();
    lease.complete().unwrap();

    let finished = runtime.finish_cell(begun.session_token()).unwrap();
    assert_eq!(finished.cell_id(), "run-1:42");
    assert_eq!(finished.configuration_digest(), digest('a'));
    assert_eq!(finished.queries_started(), 1);
    assert_eq!(finished.queries_completed(), 1);
    assert_eq!(finished.queries_failed(), 0);
    assert_eq!(finished.queries_in_flight(), 0);
    assert_eq!(
        finished.counters().exercised_axes(),
        vec![AblationAxis::ParallelShardFanout]
    );
}

#[test]
fn session_token_is_mandatory_and_only_one_cell_can_be_active() {
    let runtime = BenchmarkAblationRuntime::new();
    assert!(!runtime.has_active_session().unwrap());
    let begun = runtime
        .begin_cell("run-1:1", &digest('b'), BenchmarkAblationConfig::default())
        .unwrap();
    assert!(runtime.has_active_session().unwrap());

    assert_eq!(
        runtime.acquire("wrong").unwrap_err(),
        BenchmarkControlError::InvalidSessionToken
    );
    assert_eq!(
        runtime
            .begin_cell("run-1:2", &digest('c'), BenchmarkAblationConfig::default(),)
            .unwrap_err(),
        BenchmarkControlError::SessionAlreadyActive
    );
    runtime
        .acquire(begun.session_token())
        .unwrap()
        .complete()
        .unwrap();
    runtime.finish_cell(begun.session_token()).unwrap();
    assert!(!runtime.has_active_session().unwrap());
}

#[test]
fn identical_begin_is_idempotent_until_the_first_query_starts() {
    let runtime = BenchmarkAblationRuntime::new();
    let config = BenchmarkAblationConfig::default();
    let first = runtime
        .begin_cell("run-idempotent:1", &digest('4'), config)
        .unwrap();
    let repeated = runtime
        .begin_cell("run-idempotent:1", &digest('4'), config)
        .unwrap();
    assert_eq!(repeated.session_token(), first.session_token());
    runtime
        .acquire(first.session_token())
        .unwrap()
        .complete()
        .unwrap();
    assert_eq!(
        runtime
            .begin_cell("run-idempotent:1", &digest('4'), config)
            .unwrap_err(),
        BenchmarkControlError::SessionAlreadyActive
    );
    runtime.finish_cell(first.session_token()).unwrap();
}

#[test]
fn dropped_or_explicitly_failed_leases_are_audited_and_block_a_successful_finish() {
    let runtime = BenchmarkAblationRuntime::new();
    let begun = runtime
        .begin_cell("run-2:1", &digest('d'), BenchmarkAblationConfig::default())
        .unwrap();
    drop(runtime.acquire(begun.session_token()).unwrap());
    runtime
        .acquire(begun.session_token())
        .unwrap()
        .fail()
        .unwrap();

    assert_eq!(
        runtime.finish_cell(begun.session_token()).unwrap_err(),
        BenchmarkControlError::QueriesFailed { count: 2 }
    );
    assert!(!runtime.has_active_session().unwrap());
}

#[test]
fn successful_finish_is_idempotent_for_the_same_session_token() {
    let runtime = BenchmarkAblationRuntime::new();
    let begun = runtime
        .begin_cell(
            "run-finish-idempotent:1",
            &digest('8'),
            BenchmarkAblationConfig::default(),
        )
        .unwrap();
    runtime
        .acquire(begun.session_token())
        .unwrap()
        .complete()
        .unwrap();

    let first = runtime.finish_cell(begun.session_token()).unwrap();
    let repeated = runtime.finish_cell(begun.session_token()).unwrap();

    assert_eq!(repeated, first);
    assert!(!runtime.has_active_session().unwrap());
}

#[test]
fn finish_rejects_in_flight_queries_and_unexercised_or_cross_axis_ablations() {
    let runtime = BenchmarkAblationRuntime::new();
    let config = BenchmarkAblationConfig {
        native_pushdown: false,
        ..BenchmarkAblationConfig::default()
    };
    let begun = runtime.begin_cell("run-3:1", &digest('e'), config).unwrap();
    let lease = runtime.acquire(begun.session_token()).unwrap();
    assert_eq!(
        runtime.finish_cell(begun.session_token()).unwrap_err(),
        BenchmarkControlError::QueriesInFlight { count: 1 }
    );
    assert!(runtime.has_active_session().unwrap());
    lease.complete().unwrap();
    assert_eq!(
        runtime.finish_cell(begun.session_token()).unwrap_err(),
        BenchmarkControlError::AblationNotExercised {
            axis: AblationAxis::NativePushdown
        }
    );

    let runtime = BenchmarkAblationRuntime::new();
    let begun = runtime.begin_cell("run-3:2", &digest('f'), config).unwrap();
    let lease = runtime.acquire(begun.session_token()).unwrap();
    lease.counters().record_canonical_residual_scan();
    lease.counters().record_eager_page_collection();
    lease.complete().unwrap();
    assert_eq!(
        runtime.finish_cell(begun.session_token()).unwrap_err(),
        BenchmarkControlError::UnexpectedAblationCounter {
            axis: AblationAxis::BoundedLazyPages
        }
    );
    assert!(!runtime.has_active_session().unwrap());
}

#[test]
fn oversized_cell_ids_are_rejected_without_activating_a_session() {
    let runtime = BenchmarkAblationRuntime::new();
    assert_eq!(
        runtime
            .begin_cell(
                &"x".repeat(257),
                &digest('2'),
                BenchmarkAblationConfig::default(),
            )
            .unwrap_err(),
        BenchmarkControlError::InvalidConfiguration
    );
    assert!(!runtime.has_active_session().unwrap());
}

#[test]
fn abort_clears_even_an_in_flight_cell_and_rejects_the_old_token() {
    let runtime = BenchmarkAblationRuntime::new();
    let begun = runtime
        .begin_cell(
            "run-abort:1",
            &digest('3'),
            BenchmarkAblationConfig::default(),
        )
        .unwrap();
    let lease = runtime.acquire(begun.session_token()).unwrap();
    runtime.abort_cell(begun.session_token()).unwrap();
    assert!(!runtime.has_active_session().unwrap());
    assert_eq!(
        runtime.acquire(begun.session_token()).unwrap_err(),
        BenchmarkControlError::NoActiveSession
    );
    drop(lease);
}

#[cfg(unix)]
#[tokio::test]
async fn unix_control_socket_round_trips_auditable_cell_evidence() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ablation.sock");
    let runtime = Arc::new(BenchmarkAblationRuntime::new());
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let server = tokio::spawn({
        let path = path.clone();
        let runtime = Arc::clone(&runtime);
        async move { serve_benchmark_ablation_control(&path, runtime, receiver).await }
    });
    for _ in 0..100 {
        if path.exists() {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(path.exists());
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );

    let begun = control_request(
        &path,
        BenchmarkControlRequest::BeginCell {
            schema_version: 1,
            cell_id: "paper-run:7".into(),
            configuration_digest: digest('7'),
            config: BenchmarkAblationWireConfig {
                native_pushdown: true,
                column_batches: true,
                bounded_lazy_pages: true,
                parallel_shard_fanout: false,
                batched_property_gather: true,
            },
        },
    )
    .await;
    let session_token = match begun {
        BenchmarkControlResponse::Begun {
            schema_version,
            gateway_pid,
            session_token,
            accepted_config,
        } => {
            assert_eq!(schema_version, 1);
            assert_eq!(gateway_pid, std::process::id());
            assert!(!accepted_config.parallel_shard_fanout);
            session_token
        }
        other => panic!("unexpected begin response: {other:?}"),
    };
    let lease = runtime.acquire(&session_token).unwrap();
    lease.counters().record_serial_shard_open();
    lease.complete().unwrap();

    let finished = control_request(
        &path,
        BenchmarkControlRequest::FinishCell {
            schema_version: 1,
            session_token,
        },
    )
    .await;
    match finished {
        BenchmarkControlResponse::Finished {
            gateway_pid,
            cell_id,
            configuration_digest,
            queries_started,
            queries_completed,
            queries_failed,
            queries_in_flight,
            counters,
            ..
        } => {
            assert_eq!(gateway_pid, std::process::id());
            assert_eq!(cell_id, "paper-run:7");
            assert_eq!(configuration_digest, digest('7'));
            assert_eq!((queries_started, queries_completed), (1, 1));
            assert_eq!((queries_failed, queries_in_flight), (0, 0));
            assert_eq!(counters.serial_shard_opens, 1);
        }
        other => panic!("unexpected finish response: {other:?}"),
    }

    shutdown.send(true).unwrap();
    server.await.unwrap().unwrap();
    assert!(!path.exists());
}

#[cfg(unix)]
#[tokio::test]
async fn disconnected_client_does_not_stop_the_control_socket() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ablation-disconnect.sock");
    let runtime = Arc::new(BenchmarkAblationRuntime::new());
    let (shutdown, receiver) = tokio::sync::watch::channel(false);
    let server = tokio::spawn({
        let path = path.clone();
        let runtime = Arc::clone(&runtime);
        async move { serve_benchmark_ablation_control(&path, runtime, receiver).await }
    });
    for _ in 0..100 {
        if path.exists() {
            break;
        }
        tokio::task::yield_now().await;
    }

    let mut abandoned = UnixStream::connect(&path).await.unwrap();
    abandoned.write_all(b"not-json\n").await.unwrap();
    drop(abandoned);
    tokio::task::yield_now().await;

    let response = control_request(
        &path,
        BenchmarkControlRequest::BeginCell {
            schema_version: 1,
            cell_id: "paper-run:after-disconnect".into(),
            configuration_digest: digest('9'),
            config: BenchmarkAblationWireConfig {
                native_pushdown: true,
                column_batches: true,
                bounded_lazy_pages: true,
                parallel_shard_fanout: true,
                batched_property_gather: true,
            },
        },
    )
    .await;
    assert!(matches!(response, BenchmarkControlResponse::Begun { .. }));

    shutdown.send(true).unwrap();
    server.await.unwrap().unwrap();
}

#[cfg(unix)]
async fn control_request(
    path: &std::path::Path,
    request: BenchmarkControlRequest,
) -> BenchmarkControlResponse {
    let mut stream = UnixStream::connect(path).await.unwrap();
    let mut encoded = serde_json::to_vec(&request).unwrap();
    encoded.push(b'\n');
    stream.write_all(&encoded).await.unwrap();
    let mut line = String::new();
    BufReader::new(stream).read_line(&mut line).await.unwrap();
    serde_json::from_str(&line).unwrap()
}

#[test]
fn production_rejects_every_alternate_counter() {
    let runtime = BenchmarkAblationRuntime::new();
    let begun = runtime
        .begin_cell("run-4:1", &digest('1'), BenchmarkAblationConfig::default())
        .unwrap();
    let lease = runtime.acquire(begun.session_token()).unwrap();
    lease.counters().record_row_column_conversion_boundary();
    lease.complete().unwrap();
    assert_eq!(
        runtime.finish_cell(begun.session_token()).unwrap_err(),
        BenchmarkControlError::UnexpectedAblationCounter {
            axis: AblationAxis::ColumnBatches
        }
    );
}
