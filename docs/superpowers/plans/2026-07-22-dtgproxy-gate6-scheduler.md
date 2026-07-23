# DTGProxy Gate 6 Scheduler Implementation Plan

> **For agentic workers:** implement task-by-task with test-first verification. This supplements the approved temporal Cypher/analytics master plan and does not introduce a second API version.

**Goal:** complete asynchronous Degree execution from Meta job claim through canonical Shard Artifact publication, result pagination, lease fencing, and Gateway takeover.

**Architecture:** `analytics-ledger` remains the authoritative job state machine. A Gateway scheduler polls Meta, claims a lease, rebuilds the immutable JobSpec projection through the existing read-only Shard Adapter, executes the current `AnalyticsProvider`, stores canonical checkpoint/result bytes in the existing generation-fenced Shard Artifact API, and CAS-publishes only after pinning. `AnalyticsResultReader` is the sole result read path.

**Tech Stack:** Rust 2024 workspace, tonic Meta/Data RPC, `analytics-api` Provider SPI, `analytics-runtime` projection helpers, `analytics-ledger` codecs, `shard-client` Artifact API, BLAKE3 digests.

## Global Constraints

- Canonical Temporal KV and the current `TemporalBackendMapping` SPI remain unchanged.
- Only the current Cypher 25 and current analytics interfaces are supported; no V2 compatibility path.
- Every mutating Meta operation is revision/lease/topology fenced and replay-idempotent.
- A successful result is visible only after the Artifact generation is complete, pinned, digest-validated, and `PublishResult` commits.
- Stale Gateway owners must fail closed; they may not publish, checkpoint, or report success after fencing.

### Task 1: Result Artifact Reader

Files: `crates/analytics-ledger/src/lib.rs`, `crates/gateway-node/src/analytics_coordinator.rs`, `crates/gateway-node/src/service.rs`.

- Keep the `DTAR` codec canonical and bounded.
- Inject `AnalyticsResultReader` into `MetaClusterAnalyticsCoordinator`.
- Validate JobSpec graph/schema/topology/backend fences, stream the pinned generation, decode, and paginate.
- Tests: codec corruption/canonicality, deterministic pagination, stale routing fence, missing Shard, digest mismatch.

### Task 2: Meta Scheduler Client and Gateway Identity

Files: `crates/gateway-node/src/analytics_scheduler.rs`, `crates/gateway-node/src/service.rs`, `crates/gateway-node/Cargo.toml`.

- Add a stable nonzero `gateway_id` constructor path; retain existing constructors only as test-compatible defaults.
- Implement bounded polling of `ListClaimableAnalyticsJobs` and fenced `Claim`, `BeginRun`, `Renew`, `Fail`, `PublishResult` proposals.
- Derive command IDs from gateway ID, job ID, revision, lease epoch, and operation.
- Tests: two schedulers compete for one job; only one claim succeeds; stale publish is rejected.

### Task 3: Fixed Job Projection and Degree Provider Execution

Files: `crates/gateway-node/src/analytics_scheduler.rs`.

- Decode canonical parameters and rebuild the JobSpec snapshot projection across all current placements using `ShardClientStorageAdapter` and existing bounded projection helpers.
- Support `dtg.graph.degree` first, with deterministic `ProjectedGraph::PartitionedSnapshot`; reject unsupported algorithm/model combinations with stable errors.
- Capture the per-Shard applied-index vector and projection identity before execution.
- Tests: PrimaryReplica and Shared-Nothing projection identity, fixed applied-index vector, deterministic Degree output.

### Task 4: Checkpoint/Result CAS Publication

Files: `crates/gateway-node/src/analytics_scheduler.rs`, `crates/analytics-ledger/src/lib.rs`, `crates/shard-client/src/lib.rs`.

- Encode result bytes using `encode_algorithm_result_artifact`.
- Split into bounded digest-chain chunks, `put_artifact_chunk`, then `pin_artifact_generation`.
- Build an `ArtifactManifest` with provider/algorithm versions, projection identity, input index digest, and `Complete` result stage.
- Publish with expected job revision and current lease fence. On any stale response, stop and leave artifacts reclaimable.
- Tests: partial upload, duplicate upload, pin mismatch, stale publish, result read after pin.

### Task 5: Checkpoint Resume and Lease Renewal

Files: `crates/analytics-api/src/lib.rs`, `crates/analytics-runtime/src/provider.rs`, `crates/gateway-node/src/analytics_scheduler.rs`.

- Extend the current Provider SPI with a versioned deterministic execution checkpoint for Degree; do not add a V2 trait.
- Persist checkpoint state before long execution slices and renew the lease at a bounded interval.
- On takeover, restore only a compatible checkpoint; otherwise fail with an explicit incompatibility error.
- Tests: stop owner before/after checkpoint, takeover resumes, old owner cannot publish, result bytes are identical.

### Task 6: Real Two-Gateway Matrix

Files: `crates/gateway-node/tests/analytics_takeover.rs`, `.github/workflows/three-backend-migration.yml`, docs/progress.

- Run async Degree submit/status/results/cancel through RocksDB, PostgreSQL, and Neo4j in PrimaryReplica and Shared-Nothing.
- Stop the first Gateway at claim, checkpoint, upload, and publish boundaries.
- Verify one terminal outcome, stale fencing, canonical pagination, and identical results across all six backend/deployment combinations.
