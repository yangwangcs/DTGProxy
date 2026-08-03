# Snapshot-Isolation Ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make default single-shard T-Cypher writes snapshot-isolated `COMMITTED` operations without Meta round trips, and add bounded, retry-safe `ACCEPTED` Data ingestion.

**Architecture:** Gateway retains only a Data write transport for default writes. Data validates and commits those writes through its existing per-Shard Raft batcher. A separate Data ingestion batch RPC records a bounded in-memory receipt before a background worker invokes the same commit path; receipt polling exposes pending, committed and rejected states.

**Tech Stack:** Rust, tonic/prost, Tokio bounded channels, existing Shard/Raft and provider atomic batches.

## Global Constraints

- `COMMITTED` means Raft and provider atomic persistence complete; `ACCEPTED` never means durable or readable.
- Ingest limits are 64 items, 64 KiB per batch and 4,096 local receipts.
- Default T-Cypher writes must not synchronously call Meta.
- Cross-Shard coordinator and recovery semantics remain unchanged.

### Task 1: Add failing Data-ingest protocol tests

**Files:**
- Modify: `crates/execution/dtg-cluster-protocol/tests/contracts.rs`
- Modify: `crates/processes/dtg-data/tests/process.rs`

- [ ] Add round-trip and boundary tests for batch receipt IDs, empty/oversized batches and receipt-state values.
- [ ] Add Data tests for accepted-to-committed transition, duplicate receipt idempotency and full-queue rejection.

### Task 2: Add additive Data ingestion wire contract

**Files:**
- Modify: `crates/execution/dtg-cluster-protocol/proto/dtg_cluster_v2.proto`
- Modify: `crates/execution/dtg-cluster-protocol/src/{lib.rs,validate.rs}`

- [ ] Define additive ingest batch, receipt and lookup messages/RPCs with stable fields.
- [ ] Validate all bounds before Data admission.

### Task 3: Implement bounded Data receipt queue

**Files:**
- Modify: `crates/processes/dtg-data/src/service.rs`
- Modify: `crates/processes/dtg-data/tests/process.rs`

- [ ] Write failing lifecycle tests, then introduce bounded receipt state and a single worker.
- [ ] Reuse `apply_transaction_batched`; commit/reject workers update receipt once and never block ingress acknowledgement.

### Task 4: Replace default Gateway write transport

**Files:**
- Modify: `crates/execution/dtg-execution/src/{gateway.rs,request_metrics.rs,lib.rs}`
- Modify: `crates/execution/dtg-execution/tests/gateway_process.rs`
- Modify: `crates/processes/dtg-gateway/src/main.rs`

- [ ] Replace Meta/Data write transport with Data-only snapshot write commit.
- [ ] Delete default write accounting and Meta write metrics/tests.
- [ ] Prove a default create emits no Meta call and returns only after Data receipt.

### Task 5: Document, benchmark and verify

**Files:**
- Modify: `README.md`, `docs/architecture.md`, `docs/deployment.md`
- Modify: Data performance test/harness as required

- [ ] Record ingress-only p99 and committed latency separately.
- [ ] Run focused protocol/Data/Gateway/Shard/provider tests, formatting, diff and architecture gates.
- [ ] Commit and push the verified branch.
