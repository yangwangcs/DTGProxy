# Bolt Read Pipeline Implementation Plan

> For agentic workers: use subagent-driven-development or executing-plans task by task. Steps use checkboxes.

Goal: allow a Bolt 5.4 connection to submit a bounded window of automatic read RUN/PULL pairs without changing client-visible ordering, errors, writes, or transactions.

Architecture: split Bolt handling into a bounded reader, independent read-job executors, and a single ordered writer. Statement classification admits only compiled query statements; every other command drains the read window and follows the original serial path.

Tech Stack: Rust, Tokio TCP split halves, existing Bolt PackStream codec, GatewayService, cancellation tokens, request metrics, and four-process Fjall E2E.

## Global constraints

- Existing sequential Bolt messages and response order remain byte-compatible.
- Only a compiled GatewayOperation::Query without an explicit transaction may execute concurrently.
- Each connection is bounded to 64 active reads, 64 KiB admitted request bytes, and 8 MiB completed-result bytes.
- A read error is request-local. RESET, disconnect, writes, transactions, analytics, unknown messages, and decode errors are serial barriers.
- DTG_GATEWAY_BOLT_READ_PIPELINE defaults disabled; existing sequential handling remains the fallback.
- Gateway-to-Data pipeline/session/unary behavior does not change.

---

### Task 1: Classify eligible reads and configure the gate

Files:
- Modify crates/processes/dtg-gateway/src/service.rs
- Modify crates/processes/dtg-gateway/src/config.rs
- Modify crates/processes/dtg-gateway/src/main.rs
- Test crates/processes/dtg-gateway/tests/tcypher.rs

Consumes: GatewayExecution::compile and LogicalStatement.

Produces: BoltStatementClass::{Read, Barrier}; GatewayConfig::bolt_read_pipeline_enabled().

- [ ] Step 1 — Write failing tests.

Test that the flag is false for absent or zero, true for one; test MATCH (n) RETURN n classifies Read, while CREATE (n) and BEGIN classify Barrier.

- [ ] Step 2 — Verify RED.

Run: cargo test --locked -p dtg-gateway --test tcypher bolt_read_pipeline

Expected: compile failure because the class and flag parser do not exist.

- [ ] Step 3 — Implement minimal classification.

Compile through the existing language interface and return Read only for LogicalStatement::Query(_); all other parsed statements return Barrier. Add the environment flag with default false and pass the resulting config to the listener.

- [ ] Step 4 — Verify GREEN.

Run: cargo test --locked -p dtg-gateway --lib --test tcypher

Expected: PASS.

- [ ] Step 5 — Commit classification.

Stage exactly the three production files and tcypher.rs; commit feat(gateway): classify Bolt read pipeline requests.

### Task 2: Build a pure bounded ordered-read state machine

Files:
- Modify crates/processes/dtg-gateway/src/bolt.rs
- Test crates/processes/dtg-gateway/src/bolt.rs

Consumes: GatewayCancellationToken, GatewayResponse, and BoltError.

Produces: BoltReadPipeline, BoltReadPipelineLimits, and request-local terminal state.

- [ ] Step 1 — Write failing state-machine tests.

Test two submitted reads where the second completes first: no output is ready until the first completes, then output IDs are first and second. Test request, inbound-byte, and completed-result bounds reject further admission without evicting existing jobs. Test reset cancels every active token and late completion is ignored.

- [ ] Step 2 — Verify RED.

Run: cargo test --locked -p dtg-gateway bolt::tests::reads_complete_out_of_order

Expected: compile failure naming BoltReadPipeline.

- [ ] Step 3 — Implement state only.

Use an arrival-ordered VecDeque. Each job stores its cancellation token, received-PULL flag, optional terminal result, and exact byte/result reservations. Only the completed queue front with PULL received is writable. Releasing it returns all reservations; reset drains and cancels all jobs.

- [ ] Step 4 — Verify GREEN.

Run: cargo test --locked -p dtg-gateway --lib --test tcypher

Expected: PASS.

- [ ] Step 5 — Commit state machine.

Stage bolt.rs only; commit feat(gateway): add bounded ordered Bolt read state.

### Task 3: Wire reader, executors, and ordered writer

Files:
- Modify crates/processes/dtg-gateway/src/bolt.rs
- Test crates/processes/dtg-gateway/tests/live_certification.rs
- Test crates/processes/dtg-gateway/tests/tcypher.rs

Consumes: Tasks 1 and 2.

Produces: feature-gated pipelined serve_connection with strict barriers.

- [ ] Step 1 — Write failing socket tests.

A client writes two complete read RUN/PULL pairs before reading either response; both ordered results must have the point digest. A read pair followed by a CREATE pair must return the read response before the write acknowledgement. Reset after submitted reads must produce only reset success and no late records.

- [ ] Step 2 — Verify RED.

Run: cargo test --locked -p dtg-gateway --test live_certification bolt_pipeline_accepts_two_reads

Expected: timeout or serial-only behavior.

- [ ] Step 3 — Implement socket roles.

After negotiation, split TcpStream. Reader stops when a state-machine bound is exhausted. Executor tasks return through mpsc::channel(64), never writing the socket. Writer solely owns the write half, applies completions, emits an entire front-job response, then releases its credits. A barrier stops admission and drains first; RESET cancels first, GOODBYE cancels before close.

- [ ] Step 4 — Verify GREEN.

Run: cargo test --locked -p dtg-gateway --test live_certification --test tcypher

Expected: PASS for enabled pipeline and disabled sequential behavior.

- [ ] Step 5 — Commit server path.

Stage bolt.rs plus the two test files; commit feat(gateway): pipeline bounded Bolt reads.

### Task 4: Measure real Bolt pipelining

Files:
- Modify crates/execution/dtg-execution/src/request_metrics.rs
- Modify crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs
- Modify crates/processes/dtg-gateway/tests/backend_e2e_support/bolt.rs
- Modify crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs
- Modify docs/deployment.md

Consumes: Task 3 and stage-metric artifact format.

Produces: additive schema-v9 Bolt details and a depth-aware BoltPipelineSession.

- [ ] Step 1 — Write failing metrics/client tests.

Test a depth-eight session returns all point results in submit order. Test a schema-v9 metrics snapshot missing any Bolt pipeline detail is rejected.

- [ ] Step 2 — Verify RED.

Run: cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic bolt_pipeline

Expected: compile failure naming BoltPipelineSession or schema-v9 detail.

- [ ] Step 3 — Implement metrics and client.

Add enqueue wait, execution wait, and ordered-write wait as additive request details. BoltPipelineSession sends up to depth 8 or 64 complete RUN/PULL pairs before reading their ordered response groups. Extend the matrix with c256 and depth 1/8/64; depth one remains the sequential baseline.

- [ ] Step 4 — Verify focused release behavior.

Run: cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic && cargo build --locked --release -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway

Then run the ignored Bolt pipeline release cells with DTG_BACKEND_E2E_BIN_DIR set to target/release and DTG_GATEWAY_BOLT_READ_PIPELINE=1.

Expected: zero errors and digests equal depth one.

- [ ] Step 5 — Commit measurements.

Stage the metrics source, benchmark support, diagnostic test, and deployment guide; commit test: measure pipelined Bolt read throughput.

### Task 5: Formal matrix and quality gates

Files:
- Create target/bolt-read-pipeline.<run-id>/fjall-{tcp,uds}.json; generated evidence is never committed.

- [ ] Step 1 — Build fresh release binaries.

Run: cargo build --locked --release -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway

Expected: PASS.

- [ ] Step 2 — Run three-repetition Fjall TCP and UDS matrices.

Execute c1/c8/c64/c256 at depths 1/8/64 with the feature enabled. Retain separate JSON artifacts and compare every digest and latency distribution to depth one.

- [ ] Step 3 — Run quality gates.

Run: cargo test --locked -p dtg-cluster-protocol -p dtg-execution -p dtg-data -p dtg-gateway && cargo clippy --locked -p dtg-cluster-protocol -p dtg-execution -p dtg-data -p dtg-gateway --all-targets -- -D warnings && cargo fmt --all -- --check && git diff --check && bash scripts/check-layered-architecture.sh && bash scripts/tests/check-layered-architecture-contract.sh

Expected: every command exits zero.

- [ ] Step 4 — Record outcome.

Report QPS, p50/p95/p99, and the three new detail means. Keep the gate off if a digest differs, an error occurs, or the matrix lacks material improvement.
