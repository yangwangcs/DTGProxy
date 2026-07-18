# DTGProxy 1.0 prototype boundary audit

Audit date: 2026-07-18

## Verdict

The 1.0 main path is accepted as an executable research prototype. It now includes independently
deployable Meta, Data, Gateway, and Controller processes; shared multi-Raft Data transport;
crash-replayable Shard movement; joint-consensus leadership replacement; epoch lineage; and
durable cleanup pins. It is not approved for an untrusted or production network.

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

## Residual production blockers

These do not invalidate the prototype claim, but must be closed before a production release.

1. **P1 — Network trust boundary.** Data gRPC can load mTLS files, but Controller/Gateway clients,
   private Data Raft transport, and Sidecar remain loopback-plaintext implementations. There is no
   production certificate identity mapping, authorization, tenant quota, or secret-provider path.
2. **P1 — Cluster-wide backend hot swap.** Adapter SPI, RocksDB/PostgreSQL/Neo4j factories,
   Sidecar logical restore, dual apply, index verification, and generation publication are tested.
   The new remote Data-node/Controller migration path currently creates RocksDB Replicas only;
   backend generation switching across all remote Replicas is not yet a durable Meta workflow.
3. **P1 — External-service certification.** PostgreSQL and Neo4j contract tests pass, but their live
   suites are environment-gated and were not run without disposable services. Production
   certification needs version matrices, failover, connection loss, throttling, and disk-full
   tests against both products.
4. **P1 — Recovery scheduling and topology changes.** Transaction recovery runs at Gateway startup.
   There is no periodic sweeper, participant-record garbage collection, or recovery protocol for an
   unresolved transaction whose Shard placement epoch changes.
5. **P2 — Streaming and backpressure.** The stateful Sidecar currently buffers an exported logical
   snapshot by chunks in memory, and graph-wide scans materialize Shard results before the global
   merge. Production work needs streaming cursors, byte budgets, cancellation, spill, and bounded
   fan-out concurrency.
6. **P2 — Discovery and liveness.** Controller and Gateway use static Data endpoint maps. Raft
   delivery failures are retried indirectly by Raft progress but are not surfaced as health or
   backpressure signals, and there is no dynamic peer discovery or automated placement repair.
7. **P2 — Pin producers and garbage collection.** Catalog pins and cleanup gates are durable, but
   Gateway transactions, backup jobs, and CDC consumers are not yet all wired to acquire/renew
   them automatically. Recoverable Replica trash also needs retention and garbage collection.
8. **P2 — Operations.** Structured tracing/metrics, health/readiness separation, backup retention,
   separation, backup retention, schema evolution tooling, secret-provider integration, SBOM,
   fuzzing in CI, and capacity/SLO qualification remain outside 1.0.

## Verification record

The audit gate was executed with Rust 1.93 and the Homebrew LLVM/libclang RocksDB toolchain:

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

The workspace suite includes deterministic network faults, Raft crash recovery, randomized temporal
storage TCKs, malformed codec/parser inputs, distributed commit/abort, process restart/failover,
remote Gateway execution, Shard migration, backend migration, Sidecar protocol limits, and global
merge epoch/snapshot fences. One Neo4j and three PostgreSQL live tests remain explicitly ignored
unless their documented disposable-service variables are supplied.

## Recommended 1.1 order

1. Extend the durable Controller workflow from Shard movement to remote backend generations and
   run it across every Replica.
2. Wire transaction/backup/CDC pin producers and add background recovery plus garbage collection.
3. Replace buffered Sidecar snapshots and global scans with streaming, backpressured execution.
4. Add end-to-end mTLS/service identity, authorization, quotas, discovery, observability, and
   external backend CI matrices.
5. Run service-level throughput, tail-latency, recovery-time, and scale-out benchmarks before making
   a high-performance production claim.
