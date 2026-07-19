# DTGProxy Boundary Certification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan.

**Goal:** After all prototype features pass their semantic suites, perform one unified license, provenance, protocol compatibility, security, resource-boundary, failure, and performance review; fix every stable-scope finding and publish reproducible evidence.

**Architecture:** Treat language/protocol input, distributed RPC, adapter pushdown, native/provider interfaces, snapshots/change feeds, caches, spill files, and operational configuration as trust boundaries. Audit the completed system as a whole so cross-layer assumptions are tested, not reviewed in isolation.

**Tech Stack:** cargo metadata/deny/audit tools, rustfmt/clippy, fuzz/property tests, official Cypher/Bolt vectors and drivers, existing fault harness, criterion/custom benchmarks, sanitizer/Miri where supported.

## Global Constraints

- Execute this plan last; minimal pre-import license checks do not replace it.
- Findings are fixed in code and regression tests, not waived by prose.
- Stable certification cannot rely on ignored tests or unavailable proprietary services.
- Record the broken local C++ toolchain separately from product failures and rerun the full matrix in a repaired environment.

---

### Task 1: Dependency, license, and source provenance

**Files:**
- Create: `deny.toml`
- Create: `docs/audit/dependency-provenance.md`
- Create: `docs/audit/third-party-notices.md`
- Modify: affected Cargo manifests/source headers

1. Enumerate direct/transitive Rust, copied, generated, test-only, native, WASM, and remote dependencies.
2. Verify versions, repository/revision, license expression, notices, modifications, and redistribution obligations.
3. Remove or replace incompatible/untraceable code; add minimal license gates to CI.
4. Run `cargo deny check` and provenance completeness script; expect pass.
5. Commit: `chore(audit): certify dependency licenses and provenance`.

### Task 2: Compatibility certification

**Files:**
- Create: `docs/audit/cypher-compatibility.md`
- Create: `docs/audit/bolt-compatibility.md`
- Create: `docs/audit/backend-compatibility.md`
- Add regression tests under corresponding crates/tests

1. Map every stable spec row to tests and implementation symbols.
2. Run licensed/open Cypher TCK subsets according to their terms, official Bolt vectors, and supported official drivers.
3. Run backend/deployment canonical equivalence matrices.
4. Fix every semantic deviation and record explicitly excluded administrative/proprietary features.
5. Commit: `fix(compat): close Cypher Bolt and backend deviations`.

### Task 3: Security and resource-boundary audit

**Files:**
- Create: `docs/audit/security-boundaries.md`
- Add tests/fuzz targets to syntax, Bolt, IR, physical plan, procedure, distributed query, projection, and provider crates

1. Threat-model authentication, authorization, tenant isolation, injection, deserialization, amplification, cache poisoning, spill/snapshot path handling, provider impersonation, stale epochs, and secrets.
2. Fuzz all external decoders and validate no panic/unbounded allocation/path escape.
3. Test every configured size/depth/time/memory/cardinality/concurrency limit at below/equal/above boundary.
4. Fix findings and rerun sanitizers/Miri on applicable pure-Rust crates.
5. Commit: `fix(security): close trust-boundary and resource findings`.

### Task 4: Failure and recovery audit

**Files:**
- Create: `docs/audit/failure-recovery.md`
- Add deterministic fault tests under `tests/`

1. Enumerate crash/retry/duplicate/reorder/partition/leader-transfer/rebalance/cancel/disk-full scenarios across query, transaction, analytics, cache, spill, snapshot, and incremental checkpoints.
2. Prove fail-closed identities, atomic visibility, idempotent recovery, bounded orphan cleanup, and actionable errors.
3. Fix every gap and run extended randomized schedules with recorded seeds.
4. Commit: `fix(recovery): close distributed failure matrix findings`.

### Task 5: Performance certification

**Files:**
- Create: `docs/audit/performance-report.md`
- Modify/add benches under `benches/` and crate benchmark targets

1. Establish reproducible hardware/config/dataset/query/algorithm profiles.
2. Measure compile latency/cache, point/expand/scan/joins, temporal AS OF/DIFF, Bolt streaming, distributed exchange/skew, writes/2PC, projection, every stable algorithm class, cache, incremental updates, and backend migration interaction.
3. Compare against specification §21 gates and pre-feature baselines; profile any regression.
4. Fix blocking regressions, rerun, and record confidence intervals plus peak memory/network/spill.
5. Commit: `perf: meet temporal query and analytics gates`.

### Task 6: Final clean-room verification

**Files:**
- Create: `docs/audit/1.0-certification.md`
- Modify: `README.md`

1. From a clean checkout with repaired C/C++ toolchain, run format, clippy with warnings denied, all features/targets, all tests, doc tests, fuzz smoke, compatibility, backend services, both deployments, fault matrix, and benchmarks.
2. Scan for `todo!`, `unimplemented!`, placeholders, skipped/ignored stable tests, unsafe core code, raw backend Cypher, leaked secrets, and undocumented feature gates.
3. Build the requirement-to-test evidence table and list only genuinely non-stable optional integrations as future work.
4. Commit: `docs: publish DTGProxy 1.0 certification evidence`.
