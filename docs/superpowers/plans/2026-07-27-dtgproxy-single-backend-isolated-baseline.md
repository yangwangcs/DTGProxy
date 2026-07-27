# DTGProxy Single-Backend Startup and Isolated Baseline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every normal DTGProxy process select and initialize exactly one logical backend, preserve lazy online migration, and produce isolated reproducible baseline artifacts for RocksDB, PostgreSQL, and Neo4j.

**Architecture:** Single-node startup constructs a Registry for only the active or migration-target provider. Distributed Data Nodes receive a process-level logical backend selection, validate every active Replica profile against it, and construct a one-provider Registry only when opening that profile; migration targets use a separate lazy path. The paper benchmark is split into three sealed one-backend runs and a deterministic combined report, so only one backend service family is live during each run.

**Tech Stack:** Rust 2024 workspace, Tokio, Serde/JSON, AdapterRegistry and StorageAdapter SPI, Bash/Python artifact sealing scripts, Cargo integration tests.

## Global Constraints

- Normal startup must load exactly one of `rocksdb`, `postgresql`, or `neo4j`.
- PostgreSQL and Neo4j remain distinct logical backends even though Data Nodes connect through the `sidecar` provider.
- A second backend may be loaded only while preparing, restoring, dual-applying, or recovering an online migration.
- Do not change T-Cypher semantics, temporal semantics, result identity, durability requirements, or public Adapter SPI behavior.
- Benchmark runs for the three backends must use separate process groups, ports, data directories, run IDs, and artifact roots.
- Formal protocol remains 30 seconds warmup, 60 seconds measurement, five repetitions, concurrency 1/8/32/64, and Proxy topology 1/4/8 Data Nodes.
- Simulated observations may verify artifact contracts but may not be reported as measured performance.
- Preserve unrelated dirty-worktree changes and stage only files owned by the current task.

---

## File Structure

- `crates/dtgproxy/src/gateway.rs`: select one Adapter factory for single-node startup and lazy migration; expose loaded providers in status.
- `crates/dtgproxy/tests/service_cli.rs`: single-node startup and migration lifecycle coverage.
- `crates/data-node/src/file_config.rs`: parse the process-level logical backend selection.
- `crates/data-node/src/backend.rs`: define `StartupBackend`, validate logical profiles, and create one-provider registries lazily.
- `crates/data-node/src/host.rs`: pass the selected backend into `BackendManager` and expose lifecycle evidence.
- `crates/data-node/src/bin/dtgproxy-data.rs`: wire runtime configuration into Data Node startup.
- `crates/data-node/tests/config.rs`: configuration contract tests.
- `crates/data-node/tests/backend_selection.rs`: focused backend selection and lazy migration tests.
- `crates/data-node/tests/process_restart.rs`: update process fixtures and verify restart selection.
- `config/examples/cluster-dev/data-1.json`, `config/examples/cluster-dev/data-2.json`: explicit example backend selection.
- `crates/paper-benchmark/src/artifact.rs`: one-backend formal matrix validation and combined-report model.
- `crates/paper-benchmark/src/bin/dtgproxy-paper-benchmark.rs`: `combine` command for three verified backend artifacts.
- `crates/paper-benchmark/tests/artifact_contract.rs`: one-backend and combined-report contracts.
- `scripts/prepare-paper-performance.sh`: seal one selected backend per prepared bundle.
- `scripts/run-paper-performance.sh`: preserve the selected backend in execution metadata.
- `scripts/verify-paper-performance.sh`: verify one-backend runs and combined reports.
- `scripts/run-isolated-paper-performance.sh`: run three prepared bundles sequentially and combine them.
- `scripts/tests/prepare-paper-performance-contract.sh`: preparation isolation contract.
- `scripts/tests/run-paper-performance-contract.sh`: sequential run contract.
- `scripts/tests/verify-paper-performance-contract.sh`: combined verification contract.
- `docs/paper-performance-artifact.md`: operator workflow for isolated runs.
- `docs/audit/performance/2026-07-27-single-backend-baseline.md`: measured/diagnostic baseline evidence and the optimization-plan input.

---

### Task 1: Single-Node One-Provider Registry

**Files:**
- Modify: `crates/dtgproxy/src/gateway.rs`
- Modify: `crates/dtgproxy/tests/service_cli.rs`

**Interfaces:**
- Consumes: `BackendProfile::provider()`, existing Adapter factories, `AdapterRegistry::register`.
- Produces: `fn backend_registry(provider: &str) -> Result<AdapterRegistry, GatewayError>` and `GatewayStatus.loaded_backend_providers: Vec<String>`.

- [ ] **Step 1: Write failing startup and migration tests**

Add assertions to `crates/dtgproxy/tests/service_cli.rs` that a RocksDB startup reports exactly one loaded provider and a completed RocksDB-to-RocksDB generation migration still reports only one provider. Add unit coverage beside `backend_registry` that constructs, but does not open, each production Registry and checks its provider list. Use these assertions:

```rust
let status = gateway.status().unwrap();
assert_eq!(status.loaded_backend_providers(), &["rocksdb"]);

let receipt = gateway
    .migrate_backend("rocksdb".into(), parameters, BTreeMap::new())
    .await
    .unwrap();
assert_eq!(receipt.target_provider(), "rocksdb");
assert_eq!(gateway.status().unwrap().loaded_backend_providers(), &["rocksdb"]);

for provider in ["rocksdb", "postgresql", "neo4j", "sidecar"] {
    assert_eq!(backend_registry(provider).unwrap().providers(), vec![provider]);
}
```

For an unsupported provider, assert that opening or migration returns an error containing `unsupported backend provider` before any target directory is created.

- [ ] **Step 2: Run the focused test and confirm failure**

Run:

```bash
cargo test -p dtgproxy --test service_cli -- --nocapture
```

Expected: compilation fails because `loaded_backend_providers` and its accessor do not exist, or the unsupported-provider assertion fails because all factories are registered.

- [ ] **Step 3: Replace all-provider registration with exact selection**

Change `crates/dtgproxy/src/gateway.rs` to construct only the requested factory:

```rust
fn backend_registry(provider: &str) -> Result<AdapterRegistry, GatewayError> {
    let mut registry = AdapterRegistry::new();
    match provider {
        "rocksdb" => registry.register(Arc::new(RocksAdapterFactory))?,
        "postgresql" => registry.register(Arc::new(PostgresAdapterFactory))?,
        "neo4j" => registry.register(Arc::new(Neo4jAdapterFactory))?,
        "sidecar" => registry.register(Arc::new(TcpSidecarAdapterFactory))?,
        provider => {
            return Err(GatewayError::Backend(format!(
                "unsupported backend provider {provider}"
            )));
        }
    }
    Ok(registry)
}
```

Call `backend_registry(graph.backend().provider())` during normal open and `backend_registry(target_profile.provider())` during migration. Store a sorted one-element provider vector in `GatewayService`, replace it after cutover, and serialize it in `GatewayStatus`.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cargo test -p dtgproxy --test service_cli -- --nocapture
```

Expected: all `service_cli` tests pass and status JSON includes exactly one loaded provider.

- [ ] **Step 5: Commit the task**

```bash
git add crates/dtgproxy/src/gateway.rs crates/dtgproxy/tests/service_cli.rs
git commit -m "feat(runtime): initialize only the selected single-node backend"
```

---

### Task 2: Data Node Logical Backend Configuration

**Files:**
- Modify: `crates/data-node/src/file_config.rs`
- Modify: `crates/data-node/src/lib.rs`
- Modify: `crates/data-node/tests/config.rs`
- Modify: `crates/data-node/tests/process_restart.rs`
- Modify: `config/examples/cluster-dev/data-1.json`
- Modify: `config/examples/cluster-dev/data-2.json`

**Interfaces:**
- Consumes: JSON field `backend`, existing `DataNodeRuntimeConfig`.
- Produces: public `StartupBackend` enum and `DataNodeRuntimeConfig::backend() -> StartupBackend`.

- [ ] **Step 1: Add failing configuration tests**

Update valid fixtures with `"backend": "rocksdb"`. Add one table-driven test covering the three values and invalid/missing values:

```rust
for (encoded, expected) in [
    ("rocksdb", StartupBackend::Rocksdb),
    ("postgresql", StartupBackend::Postgresql),
    ("neo4j", StartupBackend::Neo4j),
] {
    let path = write_config_with_backend(encoded);
    assert_eq!(DataNodeRuntimeConfig::load(path).unwrap().backend(), expected);
}

assert!(matches!(
    DataNodeRuntimeConfig::load(write_config_with_backend("memory")),
    Err(FileConfigError::UnsupportedBackend { .. })
));
```

Also assert that omitting `backend` is rejected rather than silently defaulting.

- [ ] **Step 2: Run tests and confirm failure**

Run:

```bash
cargo test -p data-node --test config -- --nocapture
```

Expected: compilation fails because `StartupBackend` and `backend()` do not exist.

- [ ] **Step 3: Implement the typed selection**

In `file_config.rs`, add:

```rust
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StartupBackend {
    Rocksdb,
    Postgresql,
    Neo4j,
}

impl StartupBackend {
    pub const fn name(self) -> &'static str {
        match self {
            Self::Rocksdb => "rocksdb",
            Self::Postgresql => "postgresql",
            Self::Neo4j => "neo4j",
        }
    }

    pub const fn provider(self) -> &'static str {
        match self {
            Self::Rocksdb => "rocksdb",
            Self::Postgresql | Self::Neo4j => "sidecar",
        }
    }
}
```

Deserialize a private `RawStartupBackend` with `#[serde(rename_all = "snake_case")]`, convert it to the public enum, store it in `DataNodeRuntimeConfig`, and export the type from `lib.rs`. Keep `deny_unknown_fields` and make the field required.

- [ ] **Step 4: Update process and example fixtures**

Add `"backend": "rocksdb"` to every Data Node JSON fixture in `process_restart.rs` and both cluster example files. Do not add defaults to production parsing.

- [ ] **Step 5: Run configuration and restart tests**

Run:

```bash
cargo test -p data-node --test config -- --nocapture
cargo test -p data-node --test process_restart -- --nocapture
```

Expected: both test binaries pass.

- [ ] **Step 6: Commit the task**

```bash
git add crates/data-node/src/file_config.rs crates/data-node/src/lib.rs crates/data-node/tests/config.rs crates/data-node/tests/process_restart.rs config/examples/cluster-dev/data-1.json config/examples/cluster-dev/data-2.json
git commit -m "feat(data-node): require an explicit logical backend"
```

---

### Task 3: Data Node Lazy Factory Loading and Profile Validation

**Files:**
- Modify: `crates/data-node/src/backend.rs`
- Modify: `crates/data-node/src/host.rs`
- Modify: `crates/data-node/src/bin/dtgproxy-data.rs`
- Create: `crates/data-node/tests/backend_selection.rs`

**Interfaces:**
- Consumes: `StartupBackend`, `BackendProfile::provider()`, Sidecar parameter `target_provider`.
- Produces: `BackendManager::production(startup_backend)` and separate active/migration profile open paths.

- [ ] **Step 1: Write failing BackendManager tests**

Create `backend_selection.rs` with these cases:

```rust
#[test]
fn rocksdb_startup_accepts_only_rocksdb_active_profiles() {
    let manager = BackendManager::production(StartupBackend::Rocksdb).unwrap();
    assert!(manager.validate_active_profile(&rocks_profile()).is_ok());
    assert!(manager.validate_active_profile(&sidecar_profile("postgresql")).is_err());
}

#[test]
fn logical_sidecar_backends_remain_distinct() {
    let postgres = BackendManager::production(StartupBackend::Postgresql).unwrap();
    assert!(postgres.validate_active_profile(&sidecar_profile("postgresql")).is_ok());
    assert!(postgres.validate_active_profile(&sidecar_profile("neo4j")).is_err());
}

#[test]
fn migration_target_is_validated_without_preloading_it() {
    let manager = BackendManager::production(StartupBackend::Rocksdb).unwrap();
    assert!(manager.validate_migration_target(&sidecar_profile("neo4j")).is_ok());
}
```

Use profile constructors with `target_provider` for Sidecar profiles.

- [ ] **Step 2: Run the new test and confirm failure**

Run:

```bash
cargo test -p data-node --test backend_selection -- --nocapture
```

Expected: compilation fails because the new constructor and validation methods do not exist.

- [ ] **Step 3: Replace the persistent all-provider Registry**

Change `BackendManager` to store the selected logical backend, not an all-provider Registry:

```rust
pub struct BackendManager {
    startup_backend: StartupBackend,
}

impl BackendManager {
    pub fn production(startup_backend: StartupBackend) -> Result<Self, BackendError> {
        Ok(Self {
            startup_backend,
        })
    }
}
```

Add `logical_backend(profile)` with exact rules:

```rust
fn logical_backend(profile: &BackendProfile) -> Result<StartupBackend, BackendError> {
    match profile.provider() {
        "rocksdb" => Ok(StartupBackend::Rocksdb),
        "sidecar" => match profile.public_parameters().get("target_provider").map(String::as_str) {
            Some("postgresql") => Ok(StartupBackend::Postgresql),
            Some("neo4j") => Ok(StartupBackend::Neo4j),
            _ => Err(BackendError::InvalidSidecarTargetProvider),
        },
        provider => Err(BackendError::UnsupportedProvider(provider.to_owned())),
    }
}
```

`open_profile` creates a fresh Registry containing only `RocksAdapterFactory` or `TcpSidecarAdapterFactory` for that profile. `open_slot` validates active profiles against `startup_backend`; dual-applying recovery accepts the configured source and the persisted target. `restore_target` calls the migration-target validator and opens only that target factory. Runtime lifecycle evidence is derived from successfully opened Replica slots in Task 4, avoiding stale global counters after cutover or abort.

- [ ] **Step 4: Wire the selection from the process entry point**

Change `DataNodeHost::open` to accept `StartupBackend`:

```rust
pub async fn open(
    config: NodeConfig,
    queue_capacity: usize,
    startup_backend: StartupBackend,
) -> Result<Self, HostError>
```

In `dtgproxy-data.rs`, save `let backend = runtime.backend();` before consuming the node config and pass it to `DataNodeHost::open`.

- [ ] **Step 5: Update all in-workspace call sites**

Use `rg -n "DataNodeHost::open\(" crates` and pass the fixture's intended backend explicitly. Tests that create RocksDB profiles use `StartupBackend::Rocksdb`; three-backend deployment helpers use their case backend.

- [ ] **Step 6: Run Data Node tests**

Run:

```bash
cargo test -p data-node --tests -- --nocapture
```

Expected: all Data Node tests pass; profile mismatch tests fail closed before opening an Adapter.

- [ ] **Step 7: Commit the task**

```bash
git add crates/data-node/src/backend.rs crates/data-node/src/host.rs crates/data-node/src/bin/dtgproxy-data.rs crates/data-node/tests/backend_selection.rs crates
git commit -m "feat(data-node): load active and migration backends on demand"
```

Before committing, inspect `git diff --cached --name-only` and unstage any unrelated file accidentally included by the broad call-site update.

---

### Task 4: Lifecycle Evidence and Three-Backend Certification

**Files:**
- Modify: `crates/data-node/src/host.rs`
- Modify: `crates/data-node/src/service.rs`
- Modify: `crates/cluster-protocol/proto/dtgproxy_cluster_v1.proto`
- Modify: `crates/gateway-node/tests/three_backend_deployment.rs`
- Modify: `crates/dtgproxy/tests/three_backend_migration.rs`

**Interfaces:**
- Consumes: successfully opened Replica specs and existing backend lifecycle status.
- Produces: status fields `logical_backend` and `loaded_backends` without credentials.

- [ ] **Step 1: Write failing lifecycle assertions**

Extend certification assertions so active replicas report one backend and dual-apply checkpoints report source plus target:

```rust
assert_eq!(status.logical_backend, backend.name());
assert_eq!(status.loaded_backends, vec![backend.name().to_owned()]);
```

For migration, assert the set is `{source, target}` during dual apply and `{target}` after cutover or `{source}` after abort.

- [ ] **Step 2: Regenerate protocol code after adding fields**

Add backward-compatible fields to the backend status response:

```proto
string logical_backend = 20;
repeated string loaded_backends = 21;
```

Run:

```bash
cargo test -p cluster-protocol --no-run
```

Expected: generated Protobuf bindings compile.

- [ ] **Step 3: Populate sanitized lifecycle evidence**

Derive `logical_backend` from the active profile. Build the sorted, deduplicated loaded set from each successfully opened Replica's `BackendSlotState`: `Active` contributes one logical backend and `DualApplying` contributes source plus target. Never serialize endpoint credentials, connection strings, usernames, or passwords.

- [ ] **Step 4: Run certification tests**

Run the in-process contracts first:

```bash
cargo test -p dtgproxy --features three-backend-certification --test three_backend_migration -- --nocapture
cargo test -p gateway-node --features three-backend-certification --test three_backend_deployment --no-run
```

Expected: migration tests pass; live deployment certification builds. Run live filtered cases only when their real backend environment variables are present.

- [ ] **Step 5: Commit the task**

```bash
git add crates/data-node/src/host.rs crates/data-node/src/service.rs crates/cluster-protocol/proto/dtgproxy_cluster_v1.proto crates/gateway-node/tests/three_backend_deployment.rs crates/dtgproxy/tests/three_backend_migration.rs
git commit -m "feat(runtime): expose single-backend lifecycle evidence"
```

---

### Task 5: One-Backend Formal Artifact Contract

**Files:**
- Modify: `crates/paper-benchmark/src/artifact.rs`
- Modify: `crates/paper-benchmark/src/manifest.rs`
- Modify: `crates/paper-benchmark/tests/artifact_contract.rs`
- Modify: `scripts/prepare-paper-performance.sh`
- Modify: `scripts/tests/prepare-paper-performance-contract.sh`

**Interfaces:**
- Consumes: existing `Backend` enum and formal suite definitions.
- Produces: `selected_backend: Backend` in a sealed run and validation that every suite contains exactly that backend.

- [ ] **Step 1: Write failing one-backend artifact tests**

Create a formal manifest whose suites all contain only RocksDB and assert it validates when `selected_backend` is RocksDB. Assert a manifest containing PostgreSQL in any RocksDB run fails with `suite backend differs from selected_backend`.

```rust
let manifest = formal_manifest_for(Backend::Rocksdb);
manifest.validate().unwrap();

let mut invalid = manifest.clone();
invalid.matrix.suites[0].backends = vec![Backend::Postgresql];
assert!(invalid.validate().unwrap_err().to_string().contains("selected_backend"));
```

- [ ] **Step 2: Run the contract test and confirm failure**

Run:

```bash
cargo test -p paper-benchmark --test artifact_contract -- --nocapture
```

Expected: the current validator rejects the one-backend formal matrix because it requires all three backends.

- [ ] **Step 3: Implement selected-backend validation**

Add `selected_backend: Backend` to the formal manifest identity. Replace the all-three-per-suite invariant with:

```rust
let expected = BTreeSet::from([manifest.selected_backend]);
for suite in &manifest.matrix.suites {
    let actual = suite.backends.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(ContractError::InvalidField("matrix.suites.backends"));
    }
}
```

Keep the comparison, scale, ablation, topology, concurrency, warmup, measurement, repetition, result-identity, and error-count rules unchanged.

- [ ] **Step 4: Add `--backend` to preparation**

Require one of `rocksdb|postgresql|neo4j`. In the Python validator, replace every equality against the three-backend set with equality against `{selected_backend}` and filter runtime targets, dataset evidence, topology evidence, and backend version evidence to that backend. Record the selection in `READY.json` and `formal-spec.json`.

- [ ] **Step 5: Run Rust and script contracts**

Run:

```bash
cargo test -p paper-benchmark --test artifact_contract -- --nocapture
bash scripts/tests/prepare-paper-performance-contract.sh
```

Expected: both pass, including explicit rejection of a mixed-backend prepared bundle.

- [ ] **Step 6: Commit the task**

```bash
git add crates/paper-benchmark/src/artifact.rs crates/paper-benchmark/src/manifest.rs crates/paper-benchmark/tests/artifact_contract.rs scripts/prepare-paper-performance.sh scripts/tests/prepare-paper-performance-contract.sh
git commit -m "feat(benchmark): seal one backend per formal run"
```

---

### Task 6: Sequential Three-Backend Runner and Combined Report

**Files:**
- Modify: `crates/paper-benchmark/src/bin/dtgproxy-paper-benchmark.rs`
- Modify: `crates/paper-benchmark/src/artifact.rs`
- Modify: `crates/paper-benchmark/tests/report_contract.rs`
- Create: `scripts/run-isolated-paper-performance.sh`
- Modify: `scripts/run-paper-performance.sh`
- Modify: `scripts/verify-paper-performance.sh`
- Modify: `scripts/tests/run-paper-performance-contract.sh`
- Modify: `scripts/tests/verify-paper-performance-contract.sh`

**Interfaces:**
- Consumes: three individually verified artifact directories.
- Produces: `dtgproxy-paper-benchmark combine --rocksdb DIR --postgresql DIR --neo4j DIR --output DIR` and a combined JSON/CSV report.

- [ ] **Step 1: Write failing combined-report tests**

Create three minimal verified artifact fixtures and assert that `combine` produces exactly one backend section per input, rejects duplicate or missing backends, and preserves each run's environment and SHA-256 root.

```rust
let report = combine_verified_runs([rocks, postgres, neo4j]).unwrap();
assert_eq!(report.backends.len(), 3);
assert_eq!(report.backends[0].backend, Backend::Rocksdb);
assert!(report.backends.iter().all(|entry| entry.verification.valid));
```

- [ ] **Step 2: Run report tests and confirm failure**

Run:

```bash
cargo test -p paper-benchmark --test report_contract -- --nocapture
```

Expected: compilation fails because the combine model and command do not exist.

- [ ] **Step 3: Implement deterministic combination**

Verify each input using the existing artifact verifier, require distinct backends in RocksDB/PostgreSQL/Neo4j order, and emit:

```json
{
  "schema_version": 1,
  "backends": [
    {"backend": "rocksdb", "artifact": "...", "artifact_sha256": "...", "summary": {}},
    {"backend": "postgresql", "artifact": "...", "artifact_sha256": "...", "summary": {}},
    {"backend": "neo4j", "artifact": "...", "artifact_sha256": "...", "summary": {}}
  ]
}
```

Generate a combined CSV by prefixing each existing summary row with `backend_run`. Do not average measurements across different backend families.

- [ ] **Step 4: Add the sequential shell runner**

`run-isolated-paper-performance.sh` accepts three prepared bundles and an output root. For each backend in fixed order, it verifies the sealed bundle's selected backend, runs `run-paper-performance.sh`, records the returned artifact directory, verifies that artifact, and only then advances. If a run fails, stop without launching the next backend and do not create a combined success report.

- [ ] **Step 5: Add process-isolation checks**

Prepared bundle metadata must list the exact managed PIDs and service identities for its selected backend. Before starting the next run, require every PID from the previous run to be absent or to have a different process-start identity. Never scan and kill arbitrary system processes by executable name.

- [ ] **Step 6: Run script and report contracts**

Run:

```bash
cargo test -p paper-benchmark --test report_contract -- --nocapture
bash scripts/tests/run-paper-performance-contract.sh
bash scripts/tests/verify-paper-performance-contract.sh
```

Expected: all pass; failure fixtures do not create combined output.

- [ ] **Step 7: Commit the task**

```bash
git add crates/paper-benchmark/src/bin/dtgproxy-paper-benchmark.rs crates/paper-benchmark/src/artifact.rs crates/paper-benchmark/tests/report_contract.rs scripts/run-isolated-paper-performance.sh scripts/run-paper-performance.sh scripts/verify-paper-performance.sh scripts/tests/run-paper-performance-contract.sh scripts/tests/verify-paper-performance-contract.sh
git commit -m "feat(benchmark): run and combine isolated backend experiments"
```

---

### Task 7: Documentation and Reproducible Baseline Capture

**Files:**
- Modify: `README.md`
- Modify: `docs/paper-performance-artifact.md`
- Create: `docs/audit/performance/2026-07-27-single-backend-baseline.md`

**Interfaces:**
- Consumes: verified isolated artifacts and combined report from Task 6.
- Produces: operator commands, baseline tables, bottleneck evidence, and the exact input for the optimization plan.

- [ ] **Step 1: Document startup selection**

Update the Data Node configuration examples and README to state that `backend` is required, normal startup loads one backend, and online migration may temporarily load a second backend. Include the logical-to-provider mapping:

```text
rocksdb -> embedded rocksdb
postgresql -> sidecar(target_provider=postgresql)
neo4j -> sidecar(target_provider=neo4j)
```

- [ ] **Step 2: Document isolated benchmark commands**

Add exact prepare commands for each backend and the sequential run command. Use distinct paths:

```bash
scripts/prepare-paper-performance.sh --backend rocksdb ... --output-dir /abs/prepared/rocksdb
scripts/prepare-paper-performance.sh --backend postgresql ... --output-dir /abs/prepared/postgresql
scripts/prepare-paper-performance.sh --backend neo4j ... --output-dir /abs/prepared/neo4j
scripts/run-isolated-paper-performance.sh \
  --rocksdb-bundle /abs/prepared/rocksdb \
  --postgresql-bundle /abs/prepared/postgresql \
  --neo4j-bundle /abs/prepared/neo4j \
  --output-root /abs/artifacts/paper-performance
```

- [ ] **Step 3: Run the smallest real local baseline available**

Inspect availability without starting unrelated services:

```bash
command -v pg_isready || true
curl --fail --silent --show-error http://127.0.0.1:7474/ >/dev/null || true
```

Run real RocksDB diagnostic cells unconditionally. Run PostgreSQL and Neo4j diagnostic cells only when their configured live endpoints pass the existing real-backend readiness checks. Label every shortened run `diagnostic`; do not place it under the formal artifact root.

- [ ] **Step 4: Capture baseline evidence**

Write the audit document with these sections and measured values copied from verified artifacts:

```markdown
## Environment
## Backend isolation evidence
## Workload and protocol
## RocksDB baseline
## PostgreSQL baseline
## Neo4j baseline
## Stage timing and bottleneck ranking
## Missing formal evidence
## Optimization target selection
```

For unavailable services, record the readiness check and mark the backend `not measured`; do not insert zeroes or estimates.

- [ ] **Step 5: Generate the evidence-driven optimization plan**

Rank middleware-controlled costs by measured absolute time and repeated impact across available backends. Select the largest cost whose fix preserves semantics and is covered by existing or addable tests. Write the second implementation plan to:

```text
docs/superpowers/plans/2026-07-27-dtgproxy-measured-performance-optimization.md
```

The second plan must name exact hot-path files and functions from profiles, include before-values as acceptance baselines, add an ablation or regression test, rerun the same cells, and finish the final performance report. Do not choose an optimization from intuition alone.

- [ ] **Step 6: Verify documentation and commit**

Run:

```bash
scripts/verify-paper-performance.sh --artifact /absolute/path/to/each-real-artifact
git diff --check
```

Expected: every referenced real artifact verifies, and the diff has no whitespace errors.

Commit:

```bash
git add README.md docs/paper-performance-artifact.md docs/audit/performance/2026-07-27-single-backend-baseline.md docs/superpowers/plans/2026-07-27-dtgproxy-measured-performance-optimization.md
git commit -m "docs(perf): record isolated backend baselines and optimization plan"
```

---

### Task 8: Phase-One Verification Gate

**Files:**
- Verify only; modify failing task-owned files as needed.

**Interfaces:**
- Consumes: Tasks 1-7.
- Produces: proof that startup isolation and baseline infrastructure are ready for measured optimization.

- [ ] **Step 1: Run formatting and focused workspace checks**

```bash
cargo fmt --all -- --check
cargo test -p dtgproxy --tests
cargo test -p data-node --tests
cargo test -p paper-benchmark --tests
```

Expected: all commands exit zero.

- [ ] **Step 2: Run shell contracts**

```bash
bash scripts/tests/prepare-paper-performance-contract.sh
bash scripts/tests/run-paper-performance-contract.sh
bash scripts/tests/verify-paper-performance-contract.sh
```

Expected: all contracts exit zero and report their success markers.

- [ ] **Step 3: Run static completion checks**

```bash
rg -n 'registry\.register\(Arc::new\((Neo4j|Postgres|Rocks|TcpSidecar)' crates/dtgproxy/src/gateway.rs crates/data-node/src/backend.rs
rg -n '"backend"' config/examples/cluster-dev/data-1.json config/examples/cluster-dev/data-2.json
git diff --check
```

Expected: registry calls occur only inside exact provider-selection branches; both configs contain the required backend field; no whitespace errors.

- [ ] **Step 4: Review requirement evidence**

Confirm with current files and test output:

1. Normal single-node and Data Node startup loads one logical backend.
2. PostgreSQL and Neo4j remain distinguishable through `target_provider`.
3. Migration loads the target lazily and returns to one backend after completion/abort.
4. Formal artifacts contain one selected backend.
5. Three verified backend artifacts combine deterministically.
6. Baseline documentation contains only measured values or explicit `not measured` entries.
7. The measured optimization plan exists and names a profile-proven bottleneck.

- [ ] **Step 5: Commit any verification-only fixes**

If verification required task-owned corrections, stage only those files and commit:

```bash
git commit -m "fix: close single-backend baseline verification gaps"
```

If no corrections were needed, do not create an empty commit.
