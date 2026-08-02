# Gateway–Data Pipelined API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Gateway–Data read pipeline that batches ingress without batch-completion blocking, isolates per-request errors and cancellation, applies explicit bounded backpressure, and proves its performance with reproducible TCP/UDS experiments.

**Architecture:** Add an additive `ExecutePipelined` bidi RPC. Gateway owns batching, a bounded pending registry, credits and response dispatch; Data owns bounded execution, per-request cancellation handles and terminal responses. Existing `ExecuteSession` and unary `Execute` remain capability fallbacks, and Bolt remains unaware of the internal transport choice.

**Tech Stack:** Rust, Tokio, tonic/prost, existing `dtg-cluster-protocol`, `dtg-execution`, Data process service, four-process Bolt E2E harness.

## Global Constraints

- The new RPC is additive; numeric values of existing protobuf enums and fields must not change.
- Pipeline is only eligible for read-only single-fragment queries; writes, transactional semantics, Raft durability and snapshot fences retain their existing paths.
- A batch has at most 32 requests and at most 65,536 serialized bytes; Gateway queues at most 256 pending requests per Data endpoint.
- Initial Data execution credit is 32 requests. No queue or task collection may grow without a bound. Gateway opportunistically coalesces requests already queued at the writer and never waits to fill a batch; this keeps the aggregation delay bounded at zero.
- Every terminal response carries the original nonzero 16-byte request ID. A request must never wait for another request in its batch to finish.
- A single typed error, cancellation or deadline affects only its request. Transport failure is the only case that fails every pending request.
- Existing TCP, UDS, `ExecuteSession`, unary `Execute`, Bolt `RUN/PULL`, snapshot and Raft behavior remain backward-compatible.
- Benchmark output must report baseline/session/pipeline, TCP/UDS, concurrency 1/8/64, QPS, p50/p95/p99, errors, result digest, and Gateway/Data stage metrics.

---

## File Structure

- `crates/execution/dtg-cluster-protocol/proto/dtg_cluster_v2.proto` — additive wire messages, status codes and RPC.
- `crates/execution/dtg-execution/src/gateway.rs` — observable cancellation token, pipeline client, batcher, credits, fallback selection, and Gateway stage metrics.
- `crates/execution/dtg-execution/src/request_metrics.rs` — pipeline metric detail names.
- `crates/execution/dtg-execution/tests/gateway_process.rs` — deterministic fake protocol tests for batching, ordering, error isolation, cancellation, backpressure and fallback.
- `crates/processes/dtg-data/src/service.rs` — `ExecutePipelined` implementation, request validation, bounded per-request tasks and cancel registry.
- `crates/processes/dtg-data/tests/process.rs` — real service stream tests for protocol limits and independent completion.
- `crates/processes/dtg-gateway/src/main.rs` — opt-in pipeline feature flag passed to the transport factory.
- `crates/processes/dtg-gateway/tests/backend_e2e_support/{cluster.rs,artifact.rs}` — transport-mode configuration and artifact schema.
- `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs` — formal c1/c8/c64 TCP/UDS pipeline comparison.
- `docs/deployment.md` — pipeline feature flag, safe defaults and fallback behavior.

### Task 1: Add the additive pipeline wire contract

**Files:**
- Modify: `crates/execution/dtg-cluster-protocol/proto/dtg_cluster_v2.proto:118-163`
- Test: `crates/execution/dtg-cluster-protocol/src/lib.rs`

**Consumes:** Existing `GatewayRequest`, `GatewaySessionResponse`, `RequestContext`, `TypedStatus`.

**Produces:** Generated `proto::{GatewayPipelineRequestBatch, GatewayPipelineCancel, GatewayPipelineClientFrame, GatewayPipelineCredit, GatewayPipelineServerFrame}` and `GatewayServiceClient::execute_pipelined` / server trait method.

- [ ] **Step 1: Write failing contract tests**

```rust
#[test]
fn pipeline_frames_round_trip_without_reusing_existing_tags() {
    let frame = proto::GatewayPipelineClientFrame {
        payload: Some(proto::gateway_pipeline_client_frame::Payload::Cancel(
            proto::GatewayPipelineCancel { request: Some(request_context(1)) },
        )),
    };
    assert_eq!(proto::GatewayPipelineClientFrame::decode(frame.encode_to_vec().as_slice()).unwrap(), frame);
    assert!(proto::StatusCode::ResourceExhausted as i32 > proto::StatusCode::Internal as i32);
    assert!(proto::StatusCode::Cancelled as i32 > proto::StatusCode::Internal as i32);
}
```

- [ ] **Step 2: Run the focused test and confirm it fails because the generated types do not exist**

Run: `cargo test --locked -p dtg-cluster-protocol pipeline_frames_round_trip_without_reusing_existing_tags`

Expected: compile failure naming `GatewayPipelineClientFrame`.

- [ ] **Step 3: Add protobuf messages and RPC with stable numbering**

```protobuf
enum StatusCode {
  STATUS_CODE_UNSPECIFIED = 0;
  STATUS_CODE_OK = 1;
  STATUS_CODE_INVALID_REQUEST = 2;
  STATUS_CODE_STALE_FENCE = 3;
  STATUS_CODE_CONFLICT = 4;
  STATUS_CODE_UNAVAILABLE = 5;
  STATUS_CODE_INTERNAL = 6;
  STATUS_CODE_RESOURCE_EXHAUSTED = 7;
  STATUS_CODE_CANCELLED = 8;
}

message GatewayPipelineRequestBatch { repeated GatewayRequest requests = 1; }
message GatewayPipelineCancel { RequestContext request = 1; }
message GatewayPipelineClientFrame {
  oneof payload { GatewayPipelineRequestBatch batch = 1; GatewayPipelineCancel cancel = 2; }
}
message GatewayPipelineCredit { uint32 available_requests = 1; }
message GatewayPipelineServerFrame {
  oneof payload { GatewaySessionResponse response = 1; GatewayPipelineCredit credit = 2; }
}
```

Add `rpc ExecutePipelined(stream GatewayPipelineClientFrame) returns (stream GatewayPipelineServerFrame);` without changing `Execute` or `ExecuteSession`.

- [ ] **Step 4: Run focused and package protocol tests**

Run: `cargo test --locked -p dtg-cluster-protocol`

Expected: PASS.

- [ ] **Step 5: Commit the wire contract**

```bash
git add crates/execution/dtg-cluster-protocol/proto/dtg_cluster_v2.proto crates/execution/dtg-cluster-protocol/src/lib.rs
git commit -m "feat(protocol): add gateway pipeline frames"
```

### Task 2: Make Gateway cancellation observable and expose cancellable transport calls

**Files:**
- Modify: `crates/execution/dtg-execution/src/gateway.rs:359-379, 880-920, 1415-1510, 2035-2052`
- Test: `crates/execution/dtg-execution/tests/gateway_process.rs`

**Consumes:** `GatewayCancellationToken`, `GatewayExecutionTransport`, `GatewayProtocolV2Client`.

**Produces:** `GatewayCancellationToken::cancelled(&self) -> impl Future<Output = ()>`, cancellable query transport methods that accept `&GatewayCancellationToken`, and pipeline eligibility only for single-fragment planned reads.

- [ ] **Step 1: Write failing cancellation propagation test**

```rust
#[tokio::test]
async fn process_query_cancellation_reaches_protocol_client_before_remote_completion() {
    let client = Arc::new(CancellationRecordingProtocolClient::new());
    let token = GatewayCancellationToken::new();
    let query = execute_point_query(client.clone(), token.clone());
    client.wait_until_submitted().await;
    token.cancel();
    assert_eq!(query.await.unwrap_err().code(), "DTG-EXECUTION-CANCELLED");
    assert_eq!(client.cancelled_request_ids(), vec![point_request_id()]);
}
```

- [ ] **Step 2: Run it and confirm the fake client never observes cancellation**

Run: `cargo test --locked -p dtg-execution --test gateway_process process_query_cancellation_reaches_protocol_client_before_remote_completion`

Expected: FAIL because no cancellable protocol call exists.

- [ ] **Step 3: Add notification-backed token and cancellable extension points**

```rust
#[derive(Clone, Default)]
pub struct GatewayCancellationToken {
    cancelled: Arc<AtomicBool>,
    notified: Arc<tokio::sync::Notify>,
}

pub fn cancel(&self) {
    self.cancelled.store(true, Ordering::Release);
    self.notified.notify_waiters();
}

pub async fn cancelled(&self) {
    if !self.is_cancelled() { self.notified.notified().await; }
}
```

Add `execute_query_with_metrics_and_cancellation(request, metrics, cancellation)` defaulting to the old method on non-pipeline transports. Pass the existing `cancellation` argument at the process query callsite. Do not route writes or multi-fragment queries through it.

- [ ] **Step 4: Run execution library and focused process tests**

Run: `cargo test --locked -p dtg-execution --lib --test gateway_process`

Expected: PASS.

- [ ] **Step 5: Commit cancellable transport boundary**

```bash
git add crates/execution/dtg-execution/src/gateway.rs crates/execution/dtg-execution/tests/gateway_process.rs
git commit -m "feat(execution): propagate query cancellation to transport"
```

### Task 3: Implement the Gateway pipeline client and deterministic state-machine tests

**Files:**
- Modify: `crates/execution/dtg-execution/src/gateway.rs:1126-1375`
- Modify: `crates/execution/dtg-execution/src/request_metrics.rs`
- Test: `crates/execution/dtg-execution/tests/gateway_process.rs`

**Consumes:** Generated pipeline frames and the cancellable transport call from Tasks 1–2.

**Produces:** `TonicGatewayPipelineClient`, bounded 256-entry pending registry, opportunistic 32/65,536 batcher with zero fill wait, credit handling, request-ID dispatch and session/unary fallback.

- [ ] **Step 1: Write failing state-machine tests**

```rust
#[tokio::test]
async fn pipeline_emits_one_batch_but_resolves_requests_in_completion_order() {
    let pipe = PipelineHarness::with_initial_credit(3);
    let (first, second, third) = pipe.submit_three().await;
    assert_eq!(pipe.next_batch().await.request_ids(), vec![first, second, third]);
    pipe.respond(second, ok_response(second)).await;
    assert_eq!(pipe.await_response(second).await, Ok(ok_response(second)));
    assert!(pipe.response_pending(first));
}

#[tokio::test]
async fn pipeline_error_cancel_and_queue_limit_are_request_local() {
    let pipe = PipelineHarness::with_limits(1, 2, 65_536);
    let failed = pipe.submit().await;
    let live = pipe.submit().await;
    assert_eq!(pipe.submit().await.unwrap_err().code(), "DTG-CLUSTER-PIPELINE-BACKPRESSURE");
    pipe.respond(failed, invalid_response(failed)).await;
    assert!(pipe.cancel(live).await.is_ok());
    assert_eq!(pipe.await_response(live).await.unwrap_err().code(), "DTG-EXECUTION-CANCELLED");
}
```

- [ ] **Step 2: Run tests and confirm the pipeline harness/types are absent**

Run: `cargo test --locked -p dtg-execution --test gateway_process pipeline_`

Expected: compile failure naming `PipelineHarness` or `TonicGatewayPipelineClient`.

- [ ] **Step 3: Implement one writer, one reader and bounded state**

```rust
const PIPELINE_MAX_PENDING: usize = 256;
const PIPELINE_MAX_BATCH_REQUESTS: usize = 32;
const PIPELINE_MAX_BATCH_BYTES: usize = 65_536;

struct PendingPipelineRequest {
    completion: oneshot::Sender<GatewaySessionResponses>,
    state: PendingState,
}

enum PendingState { Active, Cancelling }
```

Writer behavior: reserve pending capacity before enqueue; await credit only until request deadline; form batches up to all three limits; send a `Batch` frame. Reader behavior: apply credit updates, remove and resolve matching active request, ignore terminal frames for cancelling/tombstoned IDs, and only fail all pending requests on stream failure. On cancellation, send one `Cancel` frame, resolve the caller as cancelled, and retain a tombstone until Data terminal response restores credit.

Add the five pipeline `RequestDetail` enum values and ensure timers record success/error/cancelled once. First attempt `ExecutePipelined`; on `Code::Unimplemented` mark pipeline inactive and call existing session/unary logic.

- [ ] **Step 4: Run formatting, focused tests and execution crate tests**

Run: `cargo fmt --all -- --check && cargo test --locked -p dtg-execution --lib --test gateway_process`

Expected: PASS.

- [ ] **Step 5: Commit Gateway pipeline state machine**

```bash
git add crates/execution/dtg-execution/src/gateway.rs crates/execution/dtg-execution/src/request_metrics.rs crates/execution/dtg-execution/tests/gateway_process.rs
git commit -m "feat(execution): pipeline gateway data requests"
```

### Task 4: Implement bounded Data pipeline execution and per-request cancellation

**Files:**
- Modify: `crates/processes/dtg-data/src/service.rs:1-48, 1378-1585`
- Test: `crates/processes/dtg-data/src/service.rs` test module
- Test: `crates/processes/dtg-data/tests/process.rs`

**Consumes:** Generated pipeline server trait and client frames.

**Produces:** `DataRpcService::execute_pipelined`, `PipelineExecutionRegistry`, per-request terminal frames and credits.

- [ ] **Step 1: Write failing service-stream tests**

```rust
#[tokio::test]
async fn pipeline_returns_fast_request_before_slow_peer_in_same_batch() {
    let mut stream = service.execute_pipelined(batch_of(slow_request(1), fast_request(2))).await.unwrap().into_inner();
    assert_eq!(response_id(stream.message().await.unwrap().unwrap()), 2);
    assert_eq!(response_id(stream.message().await.unwrap().unwrap()), 1);
}

#[tokio::test]
async fn pipeline_cancel_and_invalid_request_do_not_stop_sibling_requests() {
    let mut stream = service.execute_pipelined(frames(batch_of(valid_request(1), invalid_request(2)), cancel(1))).await.unwrap().into_inner();
    assert_eq!(terminal_status(&mut stream, 2), StatusCode::InvalidRequest);
    assert_eq!(terminal_status(&mut stream, 1), StatusCode::Cancelled);
}
```

- [ ] **Step 2: Run tests and confirm `execute_pipelined` is unimplemented**

Run: `cargo test --locked -p dtg-data --lib pipeline_`

Expected: compile failure because `GatewayService` lacks the new handler.

- [ ] **Step 3: Implement per-request task registry and bounded dispatcher**

```rust
const MAX_GATEWAY_PIPELINE_IN_FLIGHT: usize = 32;
const MAX_GATEWAY_PIPELINE_BATCH_REQUESTS: usize = 32;
const MAX_GATEWAY_PIPELINE_BATCH_BYTES: usize = 65_536;

struct PipelineExecutionRegistry {
    tasks: BTreeMap<u128, tokio_util::sync::CancellationToken>,
}
```

Send an initial `GatewayPipelineCredit { available_requests: 32 }`. Validate a batch before accepting each item; invalid item => request-local typed response; a bad frame/duplicate in-flight ID => stream error. Spawn each accepted request with its own cancellation token and permit. Feed `JoinSet` completions immediately to the output channel; emit that request’s response and one returned credit together, never after waiting for sibling tasks. A cancel frame resolves only the matching token; unknown/finished IDs are ignored and counted. All task exit paths remove their ID from the registry and emit exactly one terminal response.

- [ ] **Step 4: Run Data unit, process, and cancellation tests**

Run: `cargo test --locked -p dtg-data --lib --test process`

Expected: PASS.

- [ ] **Step 5: Commit Data pipeline service**

```bash
git add crates/processes/dtg-data/src/service.rs crates/processes/dtg-data/tests/process.rs
git commit -m "feat(data): execute pipelined gateway requests"
```

### Task 5: Wire capability gate, compatibility fallback and diagnostic artifacts

**Files:**
- Modify: `crates/processes/dtg-gateway/src/main.rs:20-25`
- Modify: `crates/execution/dtg-execution/src/gateway.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/cluster.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`
- Modify: `docs/deployment.md`

**Consumes:** Pipeline transport and Data handler from Tasks 3–4.

**Produces:** `DTG_GATEWAY_QUERY_PIPELINE` default-on feature gate, `session` / `pipeline` transport label, artifact-compatible formal matrix including c64.

- [ ] **Step 1: Write failing environment/artifact tests**

```rust
#[test]
fn diagnostic_transport_mode_and_c64_are_serialized() {
    let spec = CellSpec::pipeline(Backend::Fjall, Workload::PointLookup, 64, 0);
    let artifact = serde_json::to_value(spec).unwrap();
    assert_eq!(artifact["transport_mode"], "pipeline");
    assert_eq!(artifact["concurrency"], 64);
}

#[test]
fn disabled_pipeline_uses_session_without_changing_bolt_workload() {
    assert_eq!(pipeline_enabled_from("0"), false);
}
```

- [ ] **Step 2: Run tests and confirm the transport-mode field and c64 matrix are missing**

Run: `cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic diagnostic_transport_mode_and_c64_are_serialized`

Expected: FAIL because the mode is absent.

- [ ] **Step 3: Wire the feature gate and add artifact fields**

```rust
let query_pipeline_enabled = std::env::var("DTG_GATEWAY_QUERY_PIPELINE")
    .map_or(true, |value| value != "0");
let factory = TonicGatewayProtocolV2TransportFactory::new(query_sessions_enabled)
    .with_query_pipeline(query_pipeline_enabled);
```

Add `transport_mode` to every raw observation and pooled summary. Make the diagnostic driver explicitly run `session` and `pipeline`, then each at 1/8/64. Preserve the existing three-repetition artifact validation and include the named pipeline metric details in stage-window extraction. Document the environment flag, fallback order, bounds, and that all workloads remain standard Bolt traffic.

- [ ] **Step 4: Run Gateway artifact and diagnostic unit tests**

Run: `cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic`

Expected: PASS (ignored live tests remain ignored).

- [ ] **Step 5: Commit wiring and diagnostics**

```bash
git add crates/processes/dtg-gateway/src/main.rs crates/processes/dtg-gateway/tests/backend_e2e_support/cluster.rs crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs docs/deployment.md
git commit -m "feat(gateway): benchmark pipelined data transport"
```

### Task 6: Run formal performance experiment and completion gates

**Files:**
- Create: `target/backend-e2e-pipeline-formal.<run-id>/fjall-pipeline.json` (generated, never committed)
- Modify only if results prove an implementation defect: files from Tasks 1–5.

**Consumes:** Release binaries with pipeline transport; existing Data UDS listener and four-process E2E harness.

**Produces:** Reproducible formal artifact and a requirement-by-requirement completion record.

- [ ] **Step 1: Build release binaries once**

Run: `cargo build --locked --release -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway`

Expected: PASS and all four binaries exist under `target/release`.

- [ ] **Step 2: Run TCP formal artifact**

Run: `DTG_BACKEND_E2E_SELECTED_BACKEND=fjall DTG_BACKEND_E2E_QUICK_REPETITIONS=3 DTG_BACKEND_E2E_QUICK_OUTPUT="$PWD/target/backend-e2e-pipeline-formal.<run-id>/fjall-tcp.json" cargo test --locked --release -p dtg-gateway --test backend_e2e_diagnostic quick_selected_backend_e2e_comparison -- --ignored --exact --nocapture`

Expected: 0 errors for both `session` and `pipeline`, each read workload at c1/c8/c64.

- [ ] **Step 3: Run UDS formal artifact**

Run: `DTG_BACKEND_E2E_SELECTED_BACKEND=fjall DTG_BACKEND_E2E_QUICK_REPETITIONS=3 DTG_BACKEND_E2E_QUICK_OUTPUT="$PWD/target/backend-e2e-pipeline-formal.<run-id>/fjall-uds.json" DTG_DATA_GATEWAY_UNIX_SOCKET="$PWD/target/backend-e2e-pipeline-formal.<run-id>/data.sock" cargo test --locked --release -p dtg-gateway --test backend_e2e_diagnostic quick_selected_backend_e2e_comparison -- --ignored --exact --nocapture`

Expected: 0 errors; artifact contains pooled QPS, p50/p95/p99 and Gateway/Data stage windows.

- [ ] **Step 4: Verify correctness and quality gates**

Run: `cargo test --locked -p dtg-cluster-protocol -p dtg-execution -p dtg-data -p dtg-gateway && cargo clippy --locked -p dtg-cluster-protocol -p dtg-execution -p dtg-data -p dtg-gateway --all-targets -- -D warnings && cargo fmt --all -- --check && git diff --check && bash scripts/check-layered-architecture.sh && bash scripts/tests/check-layered-architecture-contract.sh`

Expected: every command exits 0. Compare every pipeline result against the session baseline in the same artifact; report QPS and p50/p95/p99 deltas, response-wait delta and Data execution delta. If pipeline is not faster at c8/c64, use the stage evidence to tune one bound at a time and rerun Steps 2–4.

- [ ] **Step 5: Commit source changes and retain generated evidence out of Git**

```bash
git add crates docs
git commit -m "feat: add bounded gateway data pipeline"
git status --short
```

Expected: source/doc changes are committed; `target/backend-e2e-pipeline-formal.*` remains untracked/ignored as reproducible local evidence.
