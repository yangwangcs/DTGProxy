# DTGProxy clean-break architecture verification

## Verdict

**PASS.** The runtime at commit `837bba0510d7b53a2c7876b723fb058e6413de8b` satisfies all ten
acceptance criteria in the approved clean-break architecture design. The buildable workspace
contains only the 21 approved kernel, language, execution, storage, and thin-process packages.

The authoritative live manifest is:

`target/clean-break-certification/20260729T120826Z-837bba0-live/manifest.json`

- Generated: `2026-07-29T12:11:54Z`
- Mode/status: `live` / `passed`
- Manifest content digest:
  `1f8c169e310735cdc50d7fce3b3f6ddd0d197674f0d81e4fbd7e88704d019660`
- Environment fingerprint digest:
  `1dde98091ed2c55b0cec1dbf9efc4ebacaaaf0fe8ef46a16180d95f3d7808fa0`
- Certified topology: four roles and five processes: Meta, Controller, two Data, and Gateway.

## Requirement-to-evidence matrix

| # | Requirement | Authoritative evidence | Result |
|---:|---|---|---|
| 1 | Minimal kernel, language/execution/storage DAG, and four thin process roots | `bash scripts/check-layered-architecture.sh`; manifest gates `architecture`, `process_contracts`, `process_binaries`, and `four_process_liveness`; `cargo metadata --locked --format-version 1 --no-deps` reports exactly 21 approved packages | PASS |
| 2 | Language stops at deterministic normalized logical IR | `crates/language/dtg-language-ir/tests/normalized_ir.rs`, `crates/language/dtg-language/tests/compile.rs`, and `crates/language/dtg-language/tests/no_user_procedures.rs`; covered by `process_contracts` and the full workspace run | PASS |
| 3 | Execution owns planning, query, TSI transactions, Shard/Raft, consistent reads, snapshots, analytics, control, and migration | Manifest gates `process_contracts` (78 passed), `raft_and_consistent_reads` (63), `temporal_transactions` (44), `snapshots_and_builtin_analytics` (38), and `control_and_six_migrations` (26); execution facade tests prove thin process delegation | PASS |
| 4 | Storage exposes typed logical contracts and capability-controlled pushdown | `crates/storage/dtg-storage/tests/contracts.rs` (22); `crates/execution/dtg-plan/tests/pushdown.rs` (5); `crates/execution/dtg-query/tests/residual.rs` (4); manifest gate `storage_and_remote_protocol` | PASS |
| 5 | Fjall, PostgreSQL, and Neo4j use native physical layouts and in-process official paths | Manifest gate `official_provider_contracts` (42 passed; seven live cases intentionally ignored there), plus `postgresql_live` (4/4) and `neo4j_live` (3/3); Data process provider composition test proves official providers are composed in-process | PASS |
| 6 | Third-party storage uses the versioned remote protocol | Remote conformance suite (10), protocol bounds suite (3), Data Remote-path process test, and manifest gate `storage_and_remote_protocol`; no official provider depends on Remote or a Sidecar | PASS |
| 7 | RocksDB and every named legacy runtime surface are absent | Manifest gate `legacy_removal`; `bash scripts/check-clean-break-removal.sh`; dependency-tree scan has no `rocksdb`, `adapter-sidecar`, or `procedure-runtime` match | PASS |
| 8 | Backend binding, epoch fencing, Raft-group homogeneity, exclusive namespaces, and heterogeneous Data hosting | Data `heterogeneous_shards` (3) and `namespace_isolation` (2), control `homogeneity` (5), Shard recovery/read/snapshot suites, and storage owner-fence/TCK suites | PASS |
| 9 | All six directed official-backend migrations pass crash and equivalence certification | `six_provider_directions` (1), `migration_crash_matrix` (11), `migration_state` (2), catalog/reconciliation suites, and manifest gate `control_and_six_migrations` | PASS |
| 10 | Old code, docs, scripts, tests, fixtures, workflows, and dependencies are removed or replaced | `legacy_removal`, the removal contract test, 21-package Cargo metadata, current README/docs/examples/workflows, and the 503-file Task 25 deletion commit `5ca21fe` | PASS |

## Complete verification record

The final live command was:

```bash
CARGO_TARGET_DIR="$PWD/target" scripts/certify-clean-break.sh --live
```

It ran from `2026-07-29T12:08:27Z` through `2026-07-29T12:11:54Z`. All 16 manifest gates
returned exit status zero:

| Gate | Scope | Result |
|---|---|---:|
| `format` | Rust formatting | PASS |
| `architecture` | Layer and dependency DAG | PASS |
| `legacy_removal` | Removed paths, symbols, packages, and dependencies | PASS |
| `live_backend_prerequisites` | PostgreSQL client and ready Docker daemon | PASS |
| `process_contracts` | Language, execution, Meta, Controller, Data, Gateway | 78 passed |
| `raft_and_consistent_reads` | Raft, authenticated follower proof, ReadIndex, recovery, replica snapshot | 63 passed |
| `temporal_transactions` | Timestamp authority, TSI, single-Shard and 2PC recovery | 44 passed |
| `snapshots_and_builtin_analytics` | Snapshot CSR, closed algorithms, durable async analytics | 38 passed |
| `control_and_six_migrations` | Catalog, homogeneity, crash matrix, six directions | 26 passed |
| `storage_and_remote_protocol` | Logical contract, Fjall, Remote conformance | 63 passed |
| `official_provider_contracts` | Fjall/PostgreSQL/Neo4j static and physical contracts | 42 passed, 7 live cases deferred to the dedicated live gates |
| `strict_clippy` | Thin runtime packages with warnings denied | PASS |
| `process_binaries` | Four process binaries | PASS |
| `four_process_liveness` | Four roles, five live processes | PASS |
| `postgresql_live` | Disposable PostgreSQL 17 native provider TCK | 4 passed |
| `neo4j_live` | Disposable Neo4j 5.26 native provider TCK | 3 passed |

The separate complete workspace verification used the same commit and evidence directory:

```bash
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
```

- Workspace tests: `2026-07-29T12:12:53Z` to `2026-07-29T12:15:23Z`; 378 passed,
  zero failed, seven ignored. The seven ignored cases are exactly the PostgreSQL and Neo4j live
  tests, which passed 4/4 and 3/3 in the live manifest.
- Workspace test log:
  `target/clean-break-certification/20260729T120826Z-837bba0-live/workspace-all-features.log`
- Workspace test log digest:
  `d96b16453be7c742dcc80cced1c06522ff91ab41c812e4b441882865b7074fef`
- Workspace Clippy: `2026-07-29T12:15:23Z` to `2026-07-29T12:15:24Z`; exit status zero.
- Workspace Clippy log:
  `target/clean-break-certification/20260729T120826Z-837bba0-live/workspace-clippy.log`
- Workspace Clippy log digest:
  `f40d437ad5b930a117bb62fe3dd4e7c379a46371fe9adcc689a01387bd2cf69f`

Environment: Darwin arm64, Rust `1.93.0`, Cargo `1.93.0`; PostgreSQL client at
`/opt/homebrew/bin/psql`; Docker daemon ready for the Neo4j container.

## Final-certification defect closed

The first live attempt exposed a genuine Neo4j read-view defect: an existing view observed an
apply committed after its fence. The failing evidence remains at
`target/clean-break-certification/20260729T115637Z-5ca21fe-live/neo4j_live.log`.

Commit `837bba0` fixed the provider by constraining every read-view query to the exact immutable
Raft applied prefix and reconstructing snapshot transaction/metadata state from fenced change
history. The direct Neo4j live TCK then passed 3/3, followed by the complete live manifest and full
workspace verification above.

## Conclusion

No compatibility wrapper, disabled acceptance path, source-only assertion, or legacy runtime is
used to satisfy these criteria. Static contracts, distributed behavioral tests, real process
liveness, and both official external-provider live TCKs jointly cover the approved architecture.
