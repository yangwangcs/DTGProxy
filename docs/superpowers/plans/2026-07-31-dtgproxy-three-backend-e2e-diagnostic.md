# DTGProxy Three-Backend End-to-End Diagnostic Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build and run a release-mode local diagnostic comparing Fjall, PostgreSQL, and Neo4j through persistent Bolt, Gateway, Data Node, and the official in-process provider.

**Architecture:** Extend the Data process's existing environment-bootstrap profile so external official providers can be opened by the real `dtgproxy-data` executable. Keep benchmark mechanics in `dtg-gateway` integration-test support and a shell wrapper: Rust owns four-process cells, persistent Bolt sessions, observations, statistics, and immutable artifacts; Bash owns disposable PostgreSQL/Neo4j containers.

**Tech Stack:** Rust 1.93, Tokio 1.53, Tonic 0.14, Bolt 5.4 PackStream, Fjall 3.1.8, PostgreSQL 17, Neo4j 5.26 Community, Docker, Bash, Serde JSON, SHA-256.

## Global Constraints

- The approved design is `docs/superpowers/specs/2026-07-31-dtgproxy-three-backend-e2e-diagnostic-design.md`.
- The measured path is persistent Bolt client -> Gateway -> Data Node -> official in-process provider.
- Official backends are Fjall, PostgreSQL, and Neo4j; RocksDB and the removed Adapter SPI are forbidden.
- Run one backend at a time with a fresh Meta, Controller, Data, and Gateway cluster per measured repetition.
- Workloads are `create_vertex`, `point_lookup`, and `count_vertices` over 4,096 deterministic read vertices.
- Concurrency values are exactly 1 and 8; warmup is 1 second; measurement is 5 seconds; repetitions are 3.
- Every operation must have zero Bolt failures; read identity and write acknowledgements are correctness gates.
- Publish only complete immutable artifacts under `artifacts/backend-e2e-diagnostic/<run-id>/`.
- No benchmark-only branch may enter production query, transaction, storage, or process execution behavior.
- Every task follows red-green-refactor and commits only its own files.

---

### Task 1: Enable Data-process bootstrap for PostgreSQL and Neo4j

**Files:**
- Modify: `crates/processes/dtg-data/src/config.rs`
- Test: `crates/processes/dtg-data/src/config.rs`

**Interfaces:**
- Consumes: `DTG_DATA_ASSIGNMENTS` and fixed binding profile reference `environment-bootstrap`.
- Produces: endpoint and credential profiles used by `PostgresResolver` and `Neo4jResolver`.

- [ ] **Step 1: Write failing environment tests**

Add these tests to the private `config.rs` test module so credentials do not need a new public
inspection API:

```rust
#[test]
fn environment_bootstrap_loads_postgresql_profile() {
    let values = BTreeMap::from([
        ("DTG_DATA_CAPABILITIES", OsString::from("adjacency,immutable-read-view,logical-snapshot,point")),
        ("DTG_DATA_ASSIGNMENTS", OsString::from("7:11:13:17:19:23:postgresql:1:1:postgres-bench")),
        ("DTG_DATA_POSTGRES_ENDPOINT", OsString::from("host=127.0.0.1 port=55432 dbname=dtgproxy sslmode=disable")),
        ("DTG_DATA_POSTGRES_CREDENTIAL", OsString::from("user=dtgproxy password=secret")),
    ]);
    let config = DataProcessConfig::from_environment(|name| values.get(name).cloned()).unwrap();
    assert!(matches!(
        config.endpoint_profiles().get("environment-bootstrap"),
        Some(EndpointProfile::PostgreSql(value)) if value.contains("port=55432")
    ));
    assert!(matches!(
        config.credential_profiles().get("environment-bootstrap"),
        Some(CredentialProfile::PostgreSql(value)) if value == "user=dtgproxy password=secret"
    ));
}

#[test]
fn environment_bootstrap_loads_neo4j_profile_and_redacts_password() {
    let values = BTreeMap::from([
        ("DTG_DATA_CAPABILITIES", OsString::from("adjacency,immutable-read-view,logical-snapshot,point")),
        ("DTG_DATA_ASSIGNMENTS", OsString::from("7:11:13:17:19:23:neo4j:1:1:neo4j-bench")),
        ("DTG_DATA_NEO4J_ENDPOINT", OsString::from("http://127.0.0.1:57474")),
        ("DTG_DATA_NEO4J_DATABASE", OsString::from("neo4j")),
        ("DTG_DATA_NEO4J_USERNAME", OsString::from("neo4j")),
        ("DTG_DATA_NEO4J_PASSWORD", OsString::from("secret")),
    ]);
    let config = DataProcessConfig::from_environment(|name| values.get(name).cloned()).unwrap();
    let debug = format!("{:?}", config.credential_profiles());
    assert!(debug.contains("[redacted]"));
    assert!(!debug.contains("secret"));
}

#[test]
fn external_assignment_rejects_incomplete_profile() {
    let values = BTreeMap::from([
        ("DTG_DATA_CAPABILITIES", OsString::from("adjacency,immutable-read-view,logical-snapshot,point")),
        ("DTG_DATA_ASSIGNMENTS", OsString::from("7:11:13:17:19:23:postgresql:1:1:postgres-bench")),
        ("DTG_DATA_POSTGRES_ENDPOINT", OsString::from("host=127.0.0.1")),
    ]);
    let error = DataProcessConfig::from_environment(|name| values.get(name).cloned()).unwrap_err();
    assert!(error.to_string().contains("DTG_DATA_POSTGRES_CREDENTIAL"));
}
```

- [ ] **Step 2: Verify red**

Run: `cargo test --locked -p dtg-data environment_bootstrap_ -- --nocapture`

Expected: FAIL because external profiles are not populated.

- [ ] **Step 3: Implement provider-driven profile parsing**

After parsing assignments, inspect their provider kinds and call this helper:

```rust
fn configure_environment_bootstrap(
    mut config: DataProcessConfig,
    get: &impl Fn(&str) -> Option<OsString>,
) -> Result<DataProcessConfig, DataConfigError> {
    let has_postgresql = config
        .assignments
        .iter()
        .any(|binding| binding.provider_kind() == &ProviderKind::PostgreSql);
    let has_neo4j = config
        .assignments
        .iter()
        .any(|binding| binding.provider_kind() == &ProviderKind::Neo4j);
    if has_postgresql {
        let endpoint = required_environment_string(get, "DTG_DATA_POSTGRES_ENDPOINT")?;
        let credential = required_environment_string(get, "DTG_DATA_POSTGRES_CREDENTIAL")?;
        config = config
            .with_endpoint_profile("environment-bootstrap", EndpointProfile::PostgreSql(endpoint))
            .with_credential_profile("environment-bootstrap", CredentialProfile::PostgreSql(credential));
    }
    if has_neo4j {
        if config.endpoint_profiles.contains_key("environment-bootstrap") {
            return Err(DataConfigError::InvalidEnvironment(
                "environment bootstrap cannot mix PostgreSQL and Neo4j assignments".into(),
            ));
        }
        config = config
            .with_endpoint_profile(
                "environment-bootstrap",
                EndpointProfile::Neo4j {
                    endpoint: required_environment_string(get, "DTG_DATA_NEO4J_ENDPOINT")?,
                    database: required_environment_string(get, "DTG_DATA_NEO4J_DATABASE")?,
                },
            )
            .with_credential_profile(
                "environment-bootstrap",
                CredentialProfile::Neo4jBasic {
                    username: required_environment_string(get, "DTG_DATA_NEO4J_USERNAME")?,
                    password: required_environment_string(get, "DTG_DATA_NEO4J_PASSWORD")?,
                },
            );
    }
    Ok(config)
}
```

`required_environment_string` must name the missing variable in `DataConfigError::InvalidEnvironment`. Fjall-only parsing remains unchanged.

- [ ] **Step 4: Verify green**

Run:

```bash
cargo test --locked -p dtg-data environment_bootstrap_ -- --nocapture
cargo test --locked -p dtg-data --test process -- --test-threads=1
cargo fmt --check -- crates/processes/dtg-data/src/config.rs
```

- [ ] **Step 5: Commit**

```bash
git add crates/processes/dtg-data/src/config.rs
git commit -m "feat(data): bootstrap external provider profiles"
```

---

### Task 2: Add the persistent Bolt client and diagnostic model

**Files:**
- Create: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`
- Create: `crates/processes/dtg-gateway/tests/backend_e2e_support/mod.rs`
- Create: `crates/processes/dtg-gateway/tests/backend_e2e_support/bolt.rs`
- Create: `crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs`
- Modify: `crates/processes/dtg-gateway/Cargo.toml`
- Modify: `Cargo.lock`

**Interfaces:**
- Produces: `Backend`, `Workload`, `CellSpec`, `RawObservation`, `BoltSession`, `measure_cell`, `percentile_ns`, and `summarize`.

- [ ] **Step 1: Write failing matrix/statistics tests**

```rust
mod backend_e2e_support;

#[test]
fn matrix_has_exact_diagnostic_cells() {
    let cells = backend_e2e_support::CellSpec::matrix(4_923_929_926_749_575_257);
    assert_eq!(cells.len(), 54);
    assert_eq!(cells.iter().filter(|cell| cell.concurrency == 8).count(), 27);
}

#[test]
fn percentiles_use_nearest_rank() {
    let samples = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
    assert_eq!(backend_e2e_support::percentile_ns(&samples, 50), 50);
    assert_eq!(backend_e2e_support::percentile_ns(&samples, 95), 100);
    assert_eq!(backend_e2e_support::percentile_ns(&samples, 99), 100);
}
```

- [ ] **Step 2: Verify red**

Run: `cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic matrix_has_exact_diagnostic_cells -- --exact`

- [ ] **Step 3: Implement the exact matrix and raw schema**

Construct 3 backends x 3 workloads x 2 concurrency values x 3 repetitions, then apply a deterministic SplitMix64 Fisher-Yates shuffle. Serialize enums with snake_case. `RawObservation` stores backend, workload, concurrency, repetition, timestamps, measured duration, operations, errors, every latency sample, row count, result digest, and query digest.

- [ ] **Step 4: Write failing persistent-session tests**

Start a local fake Bolt server that counts accepted sockets and serves two RUN/PULL exchanges. Execute twice through one `BoltSession` and assert `accepted_connections == 1`, stable fields, row count, and digest.

Run: `cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic bolt_session_ -- --nocapture`

- [ ] **Step 5: Implement Bolt 5.4 load measurement**

Reuse the existing test PackStream encoding rules. Each worker opens one connection, negotiates once, sends HELLO once, then repeats RUN/PULL until the warmup or measurement deadline. Record latency only during measurement. Use exactly:

```rust
match workload {
    Workload::CreateVertex => ("CREATE (n:Bench {value: 1}) VALID FROM 1", BTreeMap::new()),
    Workload::PointLookup => (
        "MATCH (n) WHERE n.id = $id RETURN n.id",
        BTreeMap::from([("id".into(), BoltValue::Integer(2048))]),
    ),
    Workload::CountVertices => ("MATCH (n) RETURN COUNT(*)", BTreeMap::new()),
}
```

Use SHA-256 over canonical fields and values. All read operations in one cell must match the first identity. Write results require zero rows and a successful PULL summary.

- [ ] **Step 6: Verify green**

```bash
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
cargo fmt --check -- crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs crates/processes/dtg-gateway/tests/backend_e2e_support
```

- [ ] **Step 7: Commit**

```bash
git add Cargo.lock crates/processes/dtg-gateway/Cargo.toml \
  crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_support
git commit -m "test(perf): add persistent Bolt diagnostic client"
```

---

### Task 3: Add provider-specific four-process cell lifecycle

**Files:**
- Create: `crates/processes/dtg-gateway/tests/backend_e2e_support/cluster.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/mod.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`

**Interfaces:**
- Produces: `DiagnosticRuntime::from_env`, `DiagnosticCluster::start`, `seed_read_dataset`, `bolt_address`, and exact child retirement.

- [ ] **Step 1: Write a failing live Fjall cell**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires release DTGProxy binaries"]
async fn fjall_cell_uses_real_four_process_bolt_path() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    let spec = CellSpec::one(Backend::Fjall, Workload::PointLookup, 1, 1);
    let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
    cluster.seed_read_dataset(4_096).await.unwrap();
    let observation = measure_cell(cluster.bolt_address(), spec).await.unwrap();
    assert_eq!(observation.errors, 0);
    assert_eq!(observation.row_count, 1);
    cluster.shutdown().await.unwrap();
}
```

- [ ] **Step 2: Verify red**

```bash
cargo build --release --locked -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway
DTG_BACKEND_E2E_BIN_DIR="$PWD/target/release" \
  cargo test --release --locked -p dtg-gateway --test backend_e2e_diagnostic \
  fjall_cell_uses_real_four_process_bolt_path -- --ignored --exact --nocapture
```

- [ ] **Step 3: Implement exact cluster startup**

For every repetition create unique IDs, namespace, ports, and temporary directories. Start Meta, wait for readiness, start Controller and probe it, start Data, then seed reads when required and start Gateway with the observed applied index. Use `adjacency,immutable-read-view,logical-snapshot,point` capabilities.

Set provider-specific Data variables:

```rust
match spec.backend {
    Backend::Fjall => {}
    Backend::PostgreSql => {
        env.push(("DTG_DATA_POSTGRES_ENDPOINT", runtime.postgres_endpoint.as_str()));
        env.push(("DTG_DATA_POSTGRES_CREDENTIAL", runtime.postgres_credential.as_str()));
    }
    Backend::Neo4j => {
        env.push(("DTG_DATA_NEO4J_ENDPOINT", runtime.neo4j_endpoint.as_str()));
        env.push(("DTG_DATA_NEO4J_DATABASE", "neo4j"));
        env.push(("DTG_DATA_NEO4J_USERNAME", runtime.neo4j_username.as_str()));
        env.push(("DTG_DATA_NEO4J_PASSWORD", runtime.neo4j_password.as_str()));
    }
}
```

- [ ] **Step 4: Seed deterministic reads**

Send one `CommitSingleShard` containing vertex IDs 1..=4,096. Each vertex has property `id` equal to its numeric ID, valid interval `[1, 10_000)`, and transaction time 41. Obtain the applied index from the real response/observation; do not hard-code it.

- [ ] **Step 5: Implement exact cleanup**

Store only children started by the cell. On shutdown, terminate and wait for each exact child. Drop must perform the same cleanup best-effort. Never scan or kill by executable name. Keep one log file per process under the cell log directory.

- [ ] **Step 6: Verify green**

```bash
DTG_BACKEND_E2E_BIN_DIR="$PWD/target/release" \
  cargo test --release --locked -p dtg-gateway --test backend_e2e_diagnostic \
  fjall_cell_uses_real_four_process_bolt_path -- --ignored --exact --nocapture
```

Confirm the four exact child PIDs have exited.

- [ ] **Step 7: Commit**

```bash
git add crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_support
git commit -m "test(perf): add four-process diagnostic lifecycle"
```

---

### Task 4: Publish isolated backend artifacts and one combined summary

**Files:**
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_support/artifact.rs`
- Modify: `crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs`

**Interfaces:**
- Produces: three verified backend staging artifacts and one final `manifest.json`, `raw/*.json`,
  `summary.json`, `summary.csv`, `logs/`, and `SHA256SUMS` tree.

- [ ] **Step 1: Write failing artifact tests**

Use synthetic observations to assert grouping by backend/workload/concurrency, pooled nearest-rank percentiles, summed measured duration, no cross-backend average, exact file inventory, refusal to overwrite an existing run, and detection of a modified raw file.

- [ ] **Step 2: Verify red**

Run: `cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic artifact_ -- --nocapture`

- [ ] **Step 3: Implement summary validation**

Require three distinct repetitions for all 18 groups, zero errors, non-empty latency, matching read identities, and zero-row write acknowledgements. Calculate:

```rust
let throughput_ops_per_second =
    operations as f64 * 1_000_000_000.0 / measured_ns as f64;
```

Reject non-finite values.

- [ ] **Step 4: Implement immutable publication**

Write each final through a unique same-directory temporary file, `sync_all`, publish with a no-overwrite hard link, remove the temporary, and sync the parent. Generate byte-sorted SHA-256 entries for every regular file except `SHA256SUMS`, then re-read and verify them before returning.

- [ ] **Step 5: Add selected-backend capture and offline combine tests**

```rust
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "requires release binaries and the selected live backend"]
async fn capture_selected_backend_e2e_diagnostic() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    let selected = runtime.selected_backend;
    assert!(!runtime.backend_output_dir.exists());
    let mut observations = Vec::new();
    for spec in CellSpec::matrix_for_backend(selected, runtime.shuffle_seed) {
        let mut cluster = DiagnosticCluster::start(&runtime, spec).await.unwrap();
        if spec.workload.is_read() {
            cluster.seed_read_dataset(4_096).await.unwrap();
        }
        observations.push(measure_cell(cluster.bolt_address(), spec).await.unwrap());
        cluster.shutdown().await.unwrap();
    }
    write_backend_artifact(&runtime, &observations).unwrap();
    verify_backend_artifact(&runtime.backend_output_dir, selected).unwrap();
}

#[test]
#[ignore = "requires three verified backend staging artifacts"]
fn combine_three_backend_e2e_diagnostic() {
    let runtime = DiagnosticRuntime::from_env().unwrap();
    combine_backend_artifacts(&runtime.staging_root, &runtime.output_dir).unwrap();
    verify_artifact(&runtime.output_dir).unwrap();
}
```

`matrix_for_backend` returns exactly 18 cells. The combine step accepts exactly one verified Fjall,
one PostgreSQL, and one Neo4j staging artifact, copies their 54 raw files and logs into a new final
tree, recomputes all 18 summary groups, and refuses missing, duplicate, changed, or extra inputs.

- [ ] **Step 6: Verify green**

```bash
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
cargo fmt --check -- crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs crates/processes/dtg-gateway/tests/backend_e2e_support
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add crates/processes/dtg-gateway/tests/backend_e2e_diagnostic.rs \
  crates/processes/dtg-gateway/tests/backend_e2e_support
git commit -m "test(perf): publish immutable backend diagnostics"
```

---

### Task 5: Add disposable services, run the matrix, and report results

**Files:**
- Create: `scripts/run-backend-e2e-diagnostic.sh`
- Create: `scripts/tests/run-backend-e2e-diagnostic-contract.sh`
- Modify: `README.md`
- Create: `docs/audit/performance/2026-07-31-three-backend-e2e-diagnostic.md`

**Interfaces:**
- Produces: one user-facing command and the measured report.

- [ ] **Step 1: Write the failing shell contract**

With fake `docker`, `cargo`, `curl`, and `openssl`, require the deterministic backend order, release
build, three exact selected-backend test invocations, one final combine invocation, and removal of
each exact external container before the following backend starts. Repeat with injected Cargo
failure and require cleanup before exit.

Run: `bash scripts/tests/run-backend-e2e-diagnostic-contract.sh`

- [ ] **Step 2: Implement the lifecycle wrapper**

Accept only `--output-dir ABSOLUTE_NEW_DIR`; otherwise derive
`artifacts/backend-e2e-diagnostic/<UTC timestamp>-<git short revision>`. Compute the backend order
from seed `4923929926749575257` and record it in staging metadata. For Fjall, invoke the selected
capture without an external service. For PostgreSQL, start only `postgres:17`, wait with bounded
`docker exec ... pg_isready`, capture 18 cells, remove that exact container, and prove it is absent.
For Neo4j, start only `neo4j:5.26-community`, wait with bounded loopback `curl`, capture 18 cells,
remove that exact container, and prove it is absent. Never keep PostgreSQL and Neo4j alive at the
same time. Trap EXIT/INT/TERM and remove only the currently owned exact container name.

Export:

```text
DTG_BACKEND_E2E_BIN_DIR=<absolute target/release>
DTG_BACKEND_E2E_OUTPUT_DIR=<absolute final output>
DTG_BACKEND_E2E_STAGING_ROOT=<absolute temporary staging root>
DTG_BACKEND_E2E_SELECTED_BACKEND=fjall|postgresql|neo4j
DTG_BACKEND_E2E_POSTGRES_ENDPOINT=host=127.0.0.1 port=<port> dbname=dtgproxy sslmode=disable
DTG_BACKEND_E2E_POSTGRES_CREDENTIAL=user=dtgproxy password=<generated>
DTG_BACKEND_E2E_NEO4J_ENDPOINT=http://127.0.0.1:<port>
DTG_BACKEND_E2E_NEO4J_USERNAME=neo4j
DTG_BACKEND_E2E_NEO4J_PASSWORD=<generated>
DTG_BACKEND_E2E_SHUFFLE_SEED=4923929926749575257
```

Build the four release binaries once. Run `capture_selected_backend_e2e_diagnostic` once per
backend, clearing variables that do not belong to the selected backend. After all three staging
artifacts verify and all services retire, run `combine_three_backend_e2e_diagnostic` once.

- [ ] **Step 3: Verify wrapper contracts**

```bash
bash scripts/tests/run-backend-e2e-diagnostic-contract.sh
bash -n scripts/run-backend-e2e-diagnostic.sh scripts/tests/run-backend-e2e-diagnostic-contract.sh
```

- [ ] **Step 4: Update README**

Document prerequisites, invocation, several-minute expected duration after build/image availability, artifact location, and diagnostic-not-SLO limitation. Name Fjall as the embedded backend.

- [ ] **Step 5: Run correctness gates**

```bash
cargo test --locked -p dtg-data --test process -- --test-threads=1
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
cargo test --locked -p dtg-storage-fjall
cargo test --locked -p dtg-storage-postgres --lib
cargo test --locked -p dtg-storage-neo4j --lib
cargo build --release --locked -p dtg-meta -p dtg-controller -p dtg-data -p dtg-gateway
DTG_BACKEND_E2E_BIN_DIR="$PWD/target/release" \
  cargo test --release --locked -p dtg-gateway --test live_certification \
  four_process_functional_probes_and_real_bolt_query -- --exact --nocapture
```

- [ ] **Step 6: Execute the complete measured matrix**

Run: `scripts/run-backend-e2e-diagnostic.sh`

Expected: 54 raw observations, 18 summary rows, zero errors, three repetitions in every group, and verified checksums.

- [ ] **Step 7: Write the audit report**

Record host fingerprint, revision, invocation, artifact path, six metric rows per backend, variance and caveats, and the local-diagnostic-not-SLO boundary.

- [ ] **Step 8: Final verification**

```bash
bash scripts/tests/run-backend-e2e-diagnostic-contract.sh
cargo test --locked -p dtg-data --test process -- --test-threads=1
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic -- --test-threads=1
cargo fmt --check
bash scripts/check-layered-architecture.sh
bash scripts/check-clean-break-removal.sh
git diff --check
```

Re-run artifact verification and confirm exact managed containers and child PIDs are absent.

- [ ] **Step 9: Commit**

```bash
git add scripts/run-backend-e2e-diagnostic.sh \
  scripts/tests/run-backend-e2e-diagnostic-contract.sh README.md \
  docs/audit/performance/2026-07-31-three-backend-e2e-diagnostic.md
git commit -m "perf: measure three backend end-to-end paths"
```
