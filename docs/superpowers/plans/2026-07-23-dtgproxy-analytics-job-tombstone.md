# DTGProxy Analytics Job Tombstone Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a durable Meta Job tombstone and GC acknowledgement protocol so latest pinned orphan Artifacts can be reclaimed without treating Meta absence as proof.

**Architecture:** Analytics Ledger owns the tombstone state machine and current-format codec. Meta exposes canonical tombstone pagination and validates GC-lease ownership before accepting reclamation acknowledgement. Gateway maintenance joins active Jobs, tombstones and Shard heads under one GC epoch, then advances fences, deletes generations and acknowledges only after a complete empty observation.

**Tech Stack:** Rust 2024, Tonic/Protobuf, Meta Raft state machine, Analytics Ledger, Remote ShardClient, Tokio integration tests.

## Global Constraints

- Current design only; no V2 or legacy compatibility path.
- Unknown and non-terminal latest pinned generations remain fail-closed.
- All production changes follow a red-green test cycle.
- Preserve the dirty worktree and do not create a commit.
- macOS RocksDB builds use the configured LLVM, libclang and deployment-target environment.

---

### Task 1: Analytics Ledger tombstone state machine

**Files:**
- Modify: `crates/analytics-ledger/src/lib.rs`
- Modify: `crates/analytics-ledger/tests/state_machine.rs`

**Interfaces:**
- Produces: `JobTombstone`, `LedgerState::tombstone`, `LedgerState::list_tombstones`.
- Produces: `JobCommand::prune_terminal(..., pruned_at_unix_ms)` and `JobCommand::acknowledge_artifacts_reclaimed(...)`.

- [ ] Add failing tests proving terminal prune creates a tombstone, active prune is rejected, unacknowledged tombstones cannot compact, acknowledgement is revision-fenced, and snapshot restore is exact.
- [ ] Run `cargo test -p analytics-ledger --test state_machine tombstone -- --nocapture`; expect failures because the tombstone API is absent.
- [ ] Implement the record, command variants, validation, canonical encoding/decoding and snapshot invariants.
- [ ] Re-run `cargo test -p analytics-ledger --test state_machine tombstone -- --nocapture` and `cargo test -p analytics-ledger`; expect all tests to pass.

### Task 2: Meta tombstone maintenance protocol

**Files:**
- Modify: `crates/cluster-protocol/proto/dtgproxy_cluster_v1.proto`
- Modify: `crates/cluster-protocol/tests/contracts.rs`
- Modify: `crates/meta-node/src/service.rs`
- Modify: `crates/meta-node/src/state_machine.rs`
- Modify: `crates/meta-node/tests/service.rs`
- Modify: `crates/meta-node/tests/process_restart.rs`

**Interfaces:**
- Produces: `ListAnalyticsJobTombstonesRequest/Response` and `AnalyticsJobTombstoneRecord`.
- Consumes: Ledger tombstone codec and acknowledgement command.

- [ ] Add failing wire-contract and Meta service tests for canonical pagination, checksums, malformed cursors and stale GC-owner acknowledgement.
- [ ] Run the focused Cluster Protocol and Meta tests; expect missing RPC/type failures.
- [ ] Extend the current protobuf service and implement Meta pagination plus GC-lease validation.
- [ ] Add restart evidence that an unacknowledged tombstone and its epoch survive Meta process restart.
- [ ] Re-run Cluster Protocol and Meta package tests.

### Task 3: Gateway tombstone-aware orphan GC

**Files:**
- Modify: `crates/gateway-node/src/analytics_scheduler.rs`

**Interfaces:**
- Consumes: Meta tombstone pages and current GC lease `(gateway_id, gc_epoch, expires_unix_ms)`.
- Produces: tombstone-aware `MetaArtifactProtection` and reclamation acknowledgement.

- [ ] Add failing planner tests: missing Job without tombstone is protected; terminal tombstone permits fence advance; active Job overrides any invalid duplicate tombstone; empty complete head scan produces an acknowledgement candidate.
- [ ] Run `cargo test -p gateway-node --lib analytics_scheduler::tests::tombstone`; expect failures.
- [ ] Implement canonical tombstone pagination, join logic, deletion fencing and next-tick empty-scan acknowledgement.
- [ ] Re-run Gateway unit tests and assert stale epochs remain rejected.

### Task 4: End-to-end failure recovery

**Files:**
- Modify: `crates/gateway-node/tests/process.rs`
- Modify: `crates/gateway-node/tests/primary_replica_analytics.rs`

**Interfaces:**
- Exercises the complete Meta → Gateway → Shard fence/delete → Meta acknowledgement loop.

- [ ] Add a failing test that prunes a terminal Job with a latest pinned Result, crashes the Gateway after fence advance or delete, restarts it, and verifies eventual acknowledgement without duplicate deletion.
- [ ] Add a Meta restart between delete and acknowledgement and prove the tombstone remains unacknowledged until an empty complete scan.
- [ ] Verify unknown and non-terminal Jobs remain protected in the same fixture.
- [ ] Run the focused process and analytics recovery tests serially.

### Task 5: Documentation and final gates

**Files:**
- Modify: `docs/superpowers/specs/2026-07-19-dtgproxy-temporal-cypher-analytics-design.md`
- Modify: `docs/backend-migration-runbook.md`
- Modify: `.superpowers/sdd/progress.md`

**Interfaces:**
- Records implemented, integration-certified and remaining production-certification boundaries.

- [ ] Update documents with the exact tombstone and acknowledgement lifecycle.
- [ ] Run `cargo fmt --all -- --check` and `git diff --check`.
- [ ] Run focused package tests and strict Clippy for Analytics Ledger, Cluster Protocol, Meta Node and Gateway Node.
- [ ] Run the full relevant recovery matrix and audit every Goal condition before claiming completion.
