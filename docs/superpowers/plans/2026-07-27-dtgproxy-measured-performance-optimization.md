# DTGProxy Measured PostgreSQL Canonical-Page Optimization Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the measured PostgreSQL Adapter canonical-scan N+1 query pattern, rerun the identical isolated diagnostic cells for all three backends, and report the verified before/after result.

**Architecture:** `POSTGRES_CANONICAL_SCAN_SQL` will return `logical_key` plus an optional already-canonical value. Native tables whose stored value is already the Storage SPI byte representation—Current projection, History value, and Opaque value—return it in the page query; keyspaces that require Rust encoding return `NULL` and keep the existing `load_canonical_entry` fallback. Page ordering, `max_items + 1` continuation, byte limits, repeatable-read snapshot, result identity, and public Adapter SPI remain unchanged.

**Tech Stack:** Rust 2024, postgres crate 0.19, PostgreSQL 17, StorageAdapter/ReadSnapshot SPI, Cargo integration tests, persistent diagnostic artifacts.

## Global Constraints

- Normal startup continues to load exactly one selected logical backend.
- Do not change T-Cypher semantics, temporal semantics, result identity, page ordering, continuation rules, or durability.
- Optimize only code supported by measured evidence; do not substitute a native count operation for the Adapter Direct canonical scan.
- Before values come from `target-codex-paper-diagnostics/2026-07-27-single-backend/<backend>/diagnostic-manifest.json` using 4,096 vertices, concurrency 1, one-second warmup, three-second measurement, and three repetitions.
- The baseline PostgreSQL Adapter Direct result is 0.333333 ops/s with p50 2,650.087042 ms; Backend Direct is 1,432.444444 ops/s with p50 0.682917 ms.
- RocksDB Adapter Direct baseline is 201.222222 ops/s with p50 4.906958 ms; Neo4j Adapter Direct baseline is 25.000000 ops/s with p50 39.480833 ms.
- All before/after comparisons require zero errors and identical result identity.
- Diagnostic results must not be described as formal 30-second warmup, 60-second measurement, five-repetition evidence.
- Preserve unrelated dirty-worktree changes and stage only exact task hunks.

---

### Task 1: Return Stored Canonical Values in the PostgreSQL Page Query

**Files:**
- Modify: `crates/adapter-postgres/tests/sql_contract.rs`
- Modify: `crates/adapter-postgres/src/lib.rs`

**Interfaces:**
- Consumes: `POSTGRES_CANONICAL_SCAN_SQL`, `PostgresReadSnapshot::scan_canonical_page`, `load_canonical_entry`.
- Produces: rows `(logical_key BYTEA, canonical_value BYTEA NULL)`; `Some(value)` bypasses per-key reload, while `None` retains the existing encoder.

- [ ] **Step 1: Write the failing SQL contract**

Require the canonical CTE to expose `canonical_value`, Current branches to select `projection`, History to select `history_value`, Opaque to select `value`, non-direct branches to use `NULL::bytea`, and the final query to select both columns.

- [ ] **Step 2: Run RED**

Run:

```bash
cargo test -p adapter-postgres --test sql_contract canonical_scan_sql_returns_stored_canonical_values_without_per_entry_reload -- --exact --nocapture
```

Expected: FAIL because the current SQL returns only `logical_key`.

- [ ] **Step 3: Implement the minimal SQL and row-decoding change**

Add the nullable value column to every UNION branch. In `scan_canonical_page` construct `KeyValue::new(key, value)` when column 1 is present; call `load_canonical_entry` only when it is `NULL`. Keep byte charging and continuation code unchanged.

- [ ] **Step 4: Run GREEN and live correctness**

Run:

```bash
cargo test -p adapter-postgres --test sql_contract -- --nocapture
DTGPROXY_POSTGRES_URL='host=127.0.0.1 port=55432 user=dtgproxy password=... dbname=dtgproxy sslmode=disable' \
  cargo test -p adapter-postgres --test live_postgres \
  live_read_snapshot_pins_paginated_reads_to_one_repeatable_read_transaction -- --exact --nocapture
```

Expected: contracts pass and the live page/continuation/result-byte test passes.

---

### Task 2: Identical Isolated Before/After Diagnostics and Final Report

**Files:**
- Modify: `docs/audit/performance/2026-07-27-single-backend-baseline.md`
- Create: `docs/audit/performance/2026-07-27-single-backend-optimization-report.md`

**Interfaces:**
- Consumes: persistent baseline manifests and the same `scripts/run-paper-diagnostic.sh` protocol.
- Produces: isolated `after/<backend>/diagnostic-manifest.json` artifacts and a report containing method, raw-data links, bottleneck evidence, code change, before/after values, correctness, limitations, and reproducible commands.

- [ ] **Step 1: Rerun all three backends in fixed order**

Stop both external services for RocksDB; then run only PostgreSQL; then run only Neo4j. Use the same 4,096-vertex dataset, concurrency 1, one-second warmup, three-second measurement, and three repetitions.

- [ ] **Step 2: Verify every after artifact**

For every manifest, recompute all raw SHA-256 values, require six observations, zero errors, one selected backend, and one identical result identity across Backend Direct and Adapter Direct.

- [ ] **Step 3: Evaluate the acceptance baseline**

The optimized PostgreSQL Adapter Direct median latency must be lower than 2,650.087042 ms and throughput higher than 0.333333 ops/s. Report the exact relative change; do not impose or claim a formal production threshold from this diagnostic.

- [ ] **Step 4: Run regressions and documentation checks**

Run adapter-postgres tests, paper-benchmark diagnostic contracts, relevant format checks, shell syntax checks, and `git diff --check`. Record any unavailable Proxy/formal remote evidence explicitly.

---
