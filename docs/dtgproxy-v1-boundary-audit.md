# DTGProxy 1.0 prototype boundary audit

Audit date: 2026-07-18

## Verdict

The 1.0 main path is accepted as an executable research prototype. It now includes independently
deployable Meta, Data, Gateway, and Controller processes; shared multi-Raft Data transport;
crash-replayable Shard movement; joint-consensus leadership replacement; epoch lineage; and
durable cleanup pins. Both `PrimaryReplica` and `SharedNothing` use the same temporal/Raft core.
RocksDB runs in-process, while PostgreSQL and Neo4j are hot-pluggable stateful Sidecar targets with
durable cluster-wide migration. It is not approved for an untrusted or production network.

## Findings closed during this audit

| Finding | Resolution and evidence |
|---|---|
| A client could terminate the single Gateway accept loop with a malformed frame | Invalid/partial frames are isolated to their connection; accepted sockets have bounded read/write timeouts. `dtgproxy/tests/service_cli.rs` |
| A transaction could name a schema version different from the Catalog | Gateway now rejects stale and future schema versions before temporal preparation. `dtgproxy/tests/service_cli.rs` |
| Stateful Sidecar exhausted its worker capacity permanently after enough disconnected clients | It now uses a fixed reusable worker pool and bounded pending queue. `adapter-sidecar/tests/stateful_snapshot.rs` |
| Snapshot sessions had no resource bound or expiry | Sessions reserve one of 64 slots before export/restore and expire after five minutes of inactivity; abort/completion releases the slot. `adapter-sidecar/tests/stateful_snapshot.rs` |
| Sidecar timeout profile fields were accepted but ignored | Connect/read/write timeout parameters are parsed, validated, and installed on sockets. `adapter-sidecar/tests/stateful_snapshot.rs` |
| Durable Home decisions existed but orphan intent recovery was not wired into Gateway startup | Startup scans participant records and deterministically rolls committed transactions forward or expired undecided transactions back. Recovery commands reuse stable phase IDs and are idempotent. `dtgproxy/tests/distributed_transactions.rs` |
| Data binaries served gRPC but did not drive cross-process Raft traffic | `DataRaftRuntime` now ticks every local group, multiplexes routed messages over one private listener, and drains before host shutdown. `data-node/tests/raft_runtime.rs` |
| Shard migration existed only as Catalog states and Data receipts | The Controller now reconciles every level, installs resumable snapshots, adds learners, performs joint consensus and leadership transfer, commits an epoch fence, atomically publishes topology plus lineage, activates targets, and moves removed data to recoverable trash. `controller/tests/remote_migration.rs` |
| Joint membership was mistaken for its final incoming voter set | Completion now requires an empty outgoing voter set; an old Leader transfers leadership after Ready persistence and the new Leader commits auto-leave. `controller/tests/remote_migration.rs` |
| Cleanup had no durable transaction/backup/CDC fence | Versioned Catalog retention pins block cleanup until explicit release or lease expiry. `control-plane/tests/retention_pins.rs` |
| Backend hot swap stopped at local Adapter mechanics | Catalog now owns a crash-replayable per-graph workflow; Controller prepares every voter, starts Raft-replicated dual apply, verifies indices, publishes the generation, and retires the source. `control-plane/tests/backend_migration.rs`, `controller/tests/backend_reconciliation.rs`, `controller/tests/remote_backend_migration.rs` |
| Abort could race physical cutover while Catalog still said `Verified` | A durable, non-abortable `Committing` CAS now wins before any physical cutover RPC. Abort and commit using the same Catalog revision can no longer win in different layers. `control-plane/tests/backend_migration.rs` |
| Backend receipts did not prove one coherent replica set | Catalog now requires exactly the current voters, one resolved digest per Shard, per-replica digest binding, and monotonic applied indices through all receipt-bearing phases; snapshot recovery repeats the checks. `control-plane/tests/backend_migration.rs` |
| A crash before prepare receipts reached Catalog made abort re-prepare the target | Controller and Data share one canonical physical-profile digest. Abort no longer contacts the target backend; invalid legacy pre-receipt records safely skip shards that the same resolver could never have prepared. New migrations validate provider and every Shard endpoint before persistence. `controller/tests/backend_admin.rs`, `controller/tests/remote_backend_migration.rs` |

## Residual production blockers

These do not invalidate the prototype claim, but must be closed before a production release.

1. **P1 — Network trust boundary.** Data gRPC can load mTLS files, but Controller/Gateway clients,
   private Data Raft transport, and Sidecar remain loopback-plaintext implementations. There is no
   production certificate identity mapping, authorization/RBAC, tenant quota, rate limit, or
   secret-provider integration.
2. **P1 — Controller lease fencing.** Backend and Shard side effects are stable and idempotent, and
   Catalog state revisions fence conflicting decisions. However, `owner_term` is the Meta Raft term,
   not a unique persisted Controller lease generation; a long RPC does not re-check lease expiry
   before every side effect. Production needs an owner/lease epoch embedded in workflow commands and
   renewed or revalidated around long-running operations.
3. **P1 — Sidecar active-selector recovery.** PostgreSQL and Neo4j targets preserve restored data,
   and Data manifests preserve the selected backend generation. The Sidecar's own active-target
   selector still needs a separately durable, production-grade arbitration record for every crash
   window around backend publication.
4. **P1 — Recovery scheduling and topology changes.** Transaction recovery runs at Gateway startup.
   There is no periodic sweeper, participant-record garbage collection, or recovery protocol for an
   unresolved transaction whose Shard placement epoch changes.
5. **P1 — External-service qualification.** Disposable PostgreSQL 17.10 and Neo4j 5.26 Community
   live certification passed, including cross-backend restore, dual writes, Catalog publication,
   Data restart, and target reads. This is one-version functional evidence, not a support matrix.
   Failover, connection loss, throttling, rolling upgrades, large snapshots, disk-full behavior, and
   additional supported versions remain release gates.
6. **P2 — Streaming and backpressure.** The stateful Sidecar currently buffers an exported logical
   snapshot by chunks in memory, and graph-wide scans materialize Shard results before the global
   merge. Production work needs streaming cursors, byte budgets, cancellation, spill, and bounded
   fan-out concurrency.
7. **P2 — Discovery and liveness.** Controller and Gateway use static Data endpoint maps. Raft
   delivery failures are retried indirectly by Raft progress but are not surfaced as health or
   backpressure signals, and there is no dynamic peer discovery or automated placement repair.
8. **P2 — Pin producers and garbage collection.** Catalog pins and cleanup gates are durable, but
   Gateway transactions, backup jobs, and CDC consumers are not yet all wired to acquire/renew
   them automatically. Recoverable Replica trash, retired backend generations, and transaction
   records also need retention policies and garbage collection.
9. **P2 — Operations and release engineering.** Structured tracing/metrics, alerting,
   health/readiness separation, backup/restore drills, schema evolution tooling, rolling upgrades,
   SBOM/dependency policy, fuzzing in CI, and capacity/SLO qualification remain outside 1.0.
10. **Semantic boundary.** The transaction contract is temporal snapshot isolation for written
    element/valid-time intervals, not predicate-level temporal serializability. Query syntax and
    distributed execution are intentionally bounded; this is not a full Cypher/GQL implementation.

## Verification record

The audit gate was executed with Rust 1.93 and the Homebrew LLVM/libclang RocksDB toolchain:

```bash
cargo fmt --all -- --check
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
  cargo test --workspace --all-targets
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
  cargo clippy --workspace --all-targets --all-features -- -D warnings
git diff --check
```

The workspace suite includes deterministic network faults, Raft crash recovery, randomized temporal
storage TCKs, malformed codec/parser inputs, distributed commit/abort, process restart/failover,
remote Gateway execution, Shard migration, backend migration, Sidecar protocol limits, and global
merge epoch/snapshot fences. Environment-gated live suites were additionally executed against
disposable PostgreSQL 17.10 and Neo4j 5.26 Community services. Both full RocksDB-to-Sidecar
migrations preserved writes before restore and during dual apply, published generation 2,
survived Data restart, and returned both values from the target backend. Disposable test services
and their temporary data were removed after certification.

## Recommended 1.1 order

1. Persist a unique Controller lease generation and fence/revalidate long-running workflow effects.
2. Make Sidecar active-target arbitration durable across every publication crash window.
3. Wire transaction/backup/CDC pin producers and add background recovery plus garbage collection.
4. Replace buffered Sidecar snapshots and global scans with streaming, backpressured execution.
5. Add end-to-end mTLS/service identity, authorization, quotas, discovery, observability, and
   external backend CI/failover/version matrices.
6. Run service-level throughput, tail-latency, recovery-time, and scale-out benchmarks before making
   a high-performance production claim.
