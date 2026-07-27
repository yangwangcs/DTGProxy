# DTGProxy Formal Backend Lifecycle Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the formal three-backend runner start exactly one backend stack through a sealed lifecycle runner, bind the actual process identities to the verified artifact, prove all owned processes stopped, and only then advance.

**Architecture:** Each prepared bundle seals one executable lifecycle runner by absolute path and SHA-256. The isolated runner calls its `preflight` operation for every bundle before each backend, calls `run` for the selected bundle, validates the emitted actual runtime process evidence, verifies the artifact, independently probes every emitted exact PID/start identity after return, and publishes a combined isolation evidence file bound to each artifact's `SHA256SUMS`. PostgreSQL and Neo4j require an explicit `backend_service` process; RocksDB remains embedded in the Data Node.

**Tech Stack:** Bash, Python 3 standard library, jq-based shell contracts, immutable SHA-256-sealed benchmark bundles.

## Global Constraints

- The isolated runner never scans or kills processes by executable name.
- The lifecycle runner is the only component authorized to start or stop its exact owned processes.
- A formal success package is impossible unless all three lifecycle preflights pass, all three artifacts verify, actual runtime identities validate, and every exact process is absent or has a different start identity after its run.
- PostgreSQL and Neo4j runtime evidence must contain exactly one `backend_service`; RocksDB must contain none because it is embedded in the Data Node.
- Process identities captured during preparation are not accepted as runtime isolation evidence.
- The final combined package binds each normalized runtime evidence digest to the corresponding artifact `SHA256SUMS` digest and never contains credentials or connection strings.

---

### Task 1: Seal the Lifecycle Runner in Every Prepared Bundle

**Files:**
- Modify: `scripts/prepare-paper-performance.sh`
- Modify: `scripts/tests/prepare-paper-performance-contract.sh`

**Interfaces:**
- Consumes: new required CLI argument `--lifecycle-runner-bin /absolute/executable`.
- Produces: `READY.json.formal_run.lifecycle_runner` with absolute `path`, SHA-256, and protocol version `1`; preparation evidence marks backend service ownership as `lifecycle_runner`.

- [x] **Step 1: Add failing preparation contracts**

Require preparation without `--lifecycle-runner-bin` to fail, a non-absolute/non-executable runner to fail, and a valid bundle to contain:

```json
{
  "formal_run": {
    "lifecycle_runner": {
      "protocol_version": 1,
      "path": "/absolute/path/to/runner",
      "sha256": "<64 lowercase hex>"
    }
  }
}
```

Require PostgreSQL/Neo4j `backend_service` evidence to be
`{"ownership":"lifecycle_runner","runtime_role":"backend_service"}`.

- [x] **Step 2: Run RED**

Run: `bash scripts/tests/prepare-paper-performance-contract.sh`

Expected: FAIL because the CLI and `READY.json` do not contain lifecycle-runner fields.

- [x] **Step 3: Implement sealing**

Add the required CLI option, validate it with the same absolute executable rules as other binaries, include its SHA-256 in `READY.json`, and replace the external/unmanaged backend-service claim.

- [x] **Step 4: Run GREEN**

Run: `bash scripts/tests/prepare-paper-performance-contract.sh`

Expected: all preparation contract markers pass.

---

### Task 2: Execute Through the Sealed Lifecycle Protocol

**Files:**
- Modify: `scripts/run-isolated-paper-performance.sh`
- Modify: `scripts/tests/run-paper-performance-contract.sh`

**Interfaces:**
- Consumes lifecycle commands:

```text
<runner> preflight --prepared-bundle <bundle>
<runner> run --prepared-bundle <bundle> --output-root <root> --evidence-output <new-json>
```

- Produces runtime evidence schema:

```json
{
  "schema_version": 1,
  "selected_backend": "postgresql",
  "run_id": "...",
  "managed_processes": [
    {"backend":"postgresql","role":"gateway","identity":{},"probe":{}},
    {"backend":"postgresql","role":"data_node","identity":{},"probe":{}},
    {"backend":"postgresql","role":"backend_service","identity":{},"probe":{}}
  ]
}
```

- [x] **Step 1: Add failing lifecycle contracts**

The fake runner records invocations. Assert that all three bundles are preflighted before each run, formal execution occurs only through the lifecycle runner, and the sequence is RocksDB/PostgreSQL/Neo4j. Add rejection fixtures for a changed lifecycle-runner digest, missing PostgreSQL/Neo4j `backend_service`, a RocksDB external service, wrong backend/run ID, duplicate identities, and a still-live actual runtime identity.

- [x] **Step 2: Run RED**

Run: `bash scripts/tests/run-paper-performance-contract.sh`

Expected: FAIL because current code invokes `run-paper-performance.sh` directly and trusts preparation-time identities.

- [x] **Step 3: Implement runtime validation and release probing**

Load and verify the sealed lifecycle-runner identity from each bundle. Before every backend run, call `preflight` on all three bundles. Call `run` only for the selected bundle and require a newly created evidence file. Validate exact schema, backend/run ID, role cardinality, identity uniqueness, probe fields, and executable digest fields. Change `check_managed_processes_released` to consume this runtime evidence and allow the `backend_service` role. Re-run all three preflights after release. On any error, do not start the next backend and do not publish combined success output.

- [x] **Step 4: Run GREEN**

Run: `bash scripts/tests/run-paper-performance-contract.sh`

Expected: every lifecycle/isolation marker passes, including failure cleanup and no combined output.

---

### Task 3: Bind Runtime Isolation Evidence to the Combined Package

**Files:**
- Modify: `scripts/run-isolated-paper-performance.sh`
- Modify: `scripts/tests/run-paper-performance-contract.sh`
- Modify: `docs/paper-performance-artifact.md`
- Modify: `docs/audit/performance/2026-07-27-single-backend-optimization-report.md`

**Interfaces:**
- Consumes: normalized runtime evidence and each verified artifact's `SHA256SUMS`.
- Produces: `combined/isolation-evidence.json`, included in combined `SHA256SUMS`.

- [x] **Step 1: Add failing package-binding contracts**

Require exact fixed backend order and entries:

```json
{
  "schema_version": 1,
  "runs": [
    {
      "backend": "rocksdb",
      "run_id": "...",
      "artifact_sha256": "<sha256 of artifact/SHA256SUMS>",
      "runtime_evidence_sha256": "<sha256 of normalized evidence>"
    }
  ]
}
```

Reject a changed artifact checksum, changed evidence digest, missing backend, or extra file. Require `isolation-evidence.json` in the combined checksum inventory.

- [x] **Step 2: Run RED**

Run: `bash scripts/tests/run-paper-performance-contract.sh`

Expected: FAIL because the combined package has no isolation binding.

- [x] **Step 3: Implement deterministic binding**

Normalize validated runtime evidence into private files, hash them, hash each artifact's immutable `SHA256SUMS`, write `combined/isolation-evidence.json` with no-replace semantics, regenerate the combined checksum inventory deterministically, and extend combined verification to require all three files.

- [x] **Step 4: Document and verify**

Run:

```bash
bash scripts/tests/run-paper-performance-contract.sh
bash scripts/tests/prepare-paper-performance-contract.sh
bash scripts/tests/verify-paper-performance-contract.sh
bash -n scripts/prepare-paper-performance.sh scripts/run-isolated-paper-performance.sh
git diff --check
```

Expected: all pass. Documentation states that the lifecycle runner owns start/stop and that preparation-time PIDs are never used as runtime isolation proof.

- [x] **Step 5: Commit exact task files**

```bash
git add scripts/prepare-paper-performance.sh scripts/run-isolated-paper-performance.sh \
  scripts/tests/prepare-paper-performance-contract.sh scripts/tests/run-paper-performance-contract.sh \
  docs/paper-performance-artifact.md \
  docs/audit/performance/2026-07-27-single-backend-optimization-report.md \
  docs/superpowers/plans/2026-07-27-dtgproxy-formal-backend-lifecycle-isolation.md
git commit -m "fix(benchmark): bind formal runs to managed backend lifecycles"
```
