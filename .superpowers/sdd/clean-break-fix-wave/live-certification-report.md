# Clean-break live certification report

## Scope

This wave replaces process-liveness claims with behavioral evidence for the clean-break runtime and
adds physical provider-migration certification. The worktree is based on
`b84c2bf1cfbade43d4aae797b541dd3afa33d2c8` and certifies the v2 Meta, Controller, Data, Gateway,
storage, transaction, Raft, snapshot, analytics, and migration paths.

## RED evidence and corrections

### Runtime API compilation failures

- RED: the four-role composition test still called `ControllerProcess::open(config, catalog)` after
  the production entry point became `connect(config)`, so the certification target could not
  compile against the current process API. The new live path also required a Data-hosted Gateway v2
  service, but Data exposed only the lower-level Data service.
- GREEN: test composition now uses the explicit `ControllerProcess::open_for_test` seam. Data
  bootstraps replica assignments from bounded environment configuration, validates Gateway v2
  requests fail-closed, and serves planned query fragments through the Gateway v2 service. The
  four-process functional test compiles and executes the real Bolt query described below.

### Missing Fjall parent directories

- RED: the first process-level run reached Data startup with disposable nested Fjall business and
  consensus paths whose parent directories did not yet exist. Fjall open failed before a replica
  could be assigned, so PID liveness could not become a behavioral query proof.
- GREEN: Data startup creates the consensus root before opening replica consensus namespaces, and
  disposable certification fixtures provision the required Fjall roots before opening stores. The
  restart/WAL certification also reopens the same logical namespace and replays exactly one suffix
  entry.

### Certification contract exited 1

- RED: the strengthened contract test exited 1 because the certification script still accepted
  PID-only liveness and did not name the required behavioral evidence, third heterogeneous Data
  fixture, or physical migration runner.
- GREEN: `scripts/certify-clean-break.sh` now records functional Bolt evidence, deterministic
  runtime evidence, and live physical provider-migration evidence. The contract rejects any return
  to `record_success four_process_liveness` and requires every named evidence key.

### Neo4j candidate activation returned HTTP 400

- RED: Neo4j 5.26 rejected candidate activation with HTTP 400. Inside each `CALL`/`UNION` branch,
  the importing `WITH owner` was also used as a filtered `WITH`, which is not legal for a subquery
  import clause.
- GREEN: each affected branch now uses one importing `WITH owner`, followed by a separate filtered
  `WITH owner WHERE ...`. Source-contract assertions require both branch forms so this parser
  regression cannot silently return.

### Neo4j graph-history aggregation mismatch

- RED: staged graph-history comparison dropped `payload` from the aggregation key after the
  optional match. Distinct payload groups could collapse into one count, producing a false snapshot
  mismatch during physical migration.
- GREEN: both supplied-to-changes and changes-to-supplied comparisons preserve `payload` through
  aggregation and return a bounded mismatch row only when one group actually differs. The
  source-contract test requires both keyed aggregations.

## Behavioral evidence

The four-process functional probe executed:

```text
MATCH (n) FOR SYSTEM_TIME AS OF 41 RETURN n ORDER BY n
```

over Bolt v5.4 and returned a typed map row with vertex identifier `37`; the terminal summary had
`has_more=false`. The same evidence set records successful Meta catalog access, Controller
observation, and Data apply.

The deterministic runtime certification covers:

- RF=3 election, leader failover, ReadIndex, and follower historical read;
- single-shard snapshot-isolation commit and Home-Shard two-phase commit;
- logical snapshot install, restart, and WAL suffix continuation;
- Snapshot CSR construction, PageRank, and asynchronous T-Cypher analytics through the durable
  ledger;
- all six directed provider-migration control paths; and
- one Data node hosting independent Fjall, PostgreSQL, and Neo4j shards.

The live physical migration runner starts disposable PostgreSQL and Neo4j services together and
executes all six directed built-in provider pairs. Every direction copied five canonical records at
applied index 1, and every source content digest equalled its target canonical digest.

## Final manifests

### Local certification

- Manifest:
  `target/clean-break-certification/20260729T170914Z-b84c2bf-local/manifest.json`
- Status: `passed`
- Gates: 24, all exit status 0
- Content digest: `0fc032f1d3d277fa306771461568983a41af1a36e23edb6cbd80a31725f14c0b`

### Live certification

- Manifest:
  `target/clean-break-certification/20260729T172855Z-b84c2bf-live/manifest.json`
- Status: `passed`
- Gates: 28, all exit status 0
- Content digest: `b1de646131c5e12d2dfdeb6c657c37675dcbac024ddf7636d5db887bd08567b9`
- Physical migrations: 6/6 directions, with equal source and target canonical digests in every
  direction

## Verification record

- `bash scripts/tests/certify-clean-break-contract.sh`: passed.
- `scripts/certify-clean-break.sh --local`: passed with the 24-gate manifest above.
- `scripts/certify-clean-break.sh --live`: passed with the 28-gate manifest above.
- Live Neo4j storage TCK: 3/3 passed.
- Neo4j Cypher source contracts: 4/4 passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: passed.
- `cargo test --workspace --all-features`: passed with exit status 0; disposable-service tests
  remained ignored in this generic run and were exercised by the successful live certification.
- Layered architecture, clean-break removal, and `git diff --check` gates: passed.

## Residual concern

The Meta-Raft leader-change test failed transiently twice during earlier live attempts at
`meta_raft.rs:66`. It then passed three focused repetitions, five combined-mode repetitions, the
successful live certification, and the subsequent workspace run observed during this wave. No
reproducible root cause remained, so this wave does not change the test or election implementation.
