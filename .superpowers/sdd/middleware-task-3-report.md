# Middleware Task 3 Report

## Scope

Implemented stable middleware-stage evidence and durable quick-diagnostic enforcement.
Request-metrics schema v16 adds `gateway_plan_routing`, `gateway_transport_wait`,
`data_validation`, `data_execution`, `data_raft_queue`, `data_raft_apply`, and
`data_provider_apply`. The quick artifact requires the workload-specific subset, emits
`transport_mode: "session"`, and serializes Gateway/Data stage means per summary.

## TDD evidence

### Red

```text
$ cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic quick_artifact_requires_required_stage_set
test quick_artifact_requires_required_stage_set ... FAILED
called `Result::unwrap_err()` on an `Ok` value: QuickDiagnosticArtifact { ... }
```

The artifact accepted an observation whose `data_provider_apply` delta was removed.

```text
$ cargo test --locked -p dtg-execution --test gateway_process middleware_stage_
test middleware_stage_point_read_observes_gateway_boundaries_exactly_once ... FAILED
missing required middleware stage gateway_plan_routing
```

```text
$ cargo test --locked -p dtg-data --test process middleware_stage_
test middleware_stage_committed_write_observes_data_boundaries_exactly_once ... FAILED
missing required middleware stage DataValidation
```

The first version of this Data red test accidentally used an unavailable direct `serde_json`
test dependency and failed to compile. It was immediately changed to inspect the public metric
snapshot; the recorded red result above is the valid behavior failure.

### Green / final checks

```text
$ cargo test --locked -p dtg-execution --test gateway_process middleware_stage_
test middleware_stage_point_read_observes_gateway_boundaries_exactly_once ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 27 filtered out
```

```text
$ cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic quick_artifact_requires_required_stage_set
test quick_artifact_requires_required_stage_set ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 30 filtered out
```

```text
$ cargo check --locked -p dtg-data --test process
Finished `dev` profile [unoptimized + debuginfo] target(s) in 28.35s
```

```text
$ cargo test --locked -p dtg-execution --test gateway_process
test result: ok. 28 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

$ cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic
test result: ok. 23 passed; 0 failed; 8 ignored; 0 measured; 0 filtered out
```

```text
$ cargo fmt --all -- --check
exit 0

$ git diff --check
exit 0
```

The broader `cargo test --locked -p dtg-execution` library-unit invocation remains blocked by
pre-existing unresolved `ProcessWriteAccounting` symbols in `gateway.rs` test code
(`PROCESS_WRITE_ACCOUNTING_LIMIT`, `ProcessWriteAccounting`, and related helpers). The focused
`gateway_process` integration target compiles and passes independently.

## Files changed

- `crates/execution/dtg-execution/src/request_metrics.rs`
- `crates/execution/dtg-execution/src/gateway.rs`
- `crates/processes/dtg-data/src/service.rs`
- `crates/execution/dtg-execution/tests/gateway_process.rs`
- `crates/processes/dtg-data/tests/process.rs`
- `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`
- `crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs`
- `.superpowers/sdd/middleware-task-3-report.md`

## Concerns

The required executable `dtg-data` focused test could not be completed on this workstation.
One attempt reached the linker and failed with `ld: write() failed, errno=28 (No space left on
device)`; a subsequent attempt was stopped during an extended zero-CPU compile stall before it
reached test execution. This is an environment limitation, not an assertion result. The exact
`process` test source and production crate were checked successfully without linking. Re-run
`cargo test --locked -p dtg-data --test process middleware_stage_` on a host with sufficient
free build space before treating the Data runtime behavior as fully certified.

## Review-fix follow-up

The task review identified overlapping write-path stage boundaries and a hard-coded transport
label. The follow-up keeps `gateway_plan_routing` inside write command construction and finishes
it before `GatewayTransportWait`; it records `data_raft_queue` around bounded channel admission
and `data_raft_apply` only around the batcher's blocking Raft/provider apply. `RawObservation`
now carries the measured Bolt transport mode (`unary` at depth one, `pipeline` otherwise), and a
quick artifact rejects a mixed-mode run rather than labeling it as a fixed session.

Fresh verification after the follow-up:

```text
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic quick_artifact_rejects_mixed_transport_modes
1 passed

cargo test --locked -p dtg-execution --test gateway_process middleware_stage_
1 passed

cargo test --locked -p dtg-data --test process middleware_stage_
2 passed
```
