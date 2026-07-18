# DTGProxy 1.0 prototype boundary audit

Audit date: 2026-07-18

## Verdict

The 1.0 main path is accepted as an executable research prototype. It demonstrates backend-neutral
bitemporal persistence, Raft replication, distributed temporal transactions, both deployment
modes, global temporal scans, Sidecar isolation, and online logical backend migration. It is not
yet approved for an untrusted or production network.

## Findings closed during this audit

| Finding | Resolution and evidence |
|---|---|
| A client could terminate the single Gateway accept loop with a malformed frame | Invalid/partial frames are isolated to their connection; accepted sockets have bounded read/write timeouts. `dtgproxy/tests/service_cli.rs` |
| A transaction could name a schema version different from the Catalog | Gateway now rejects stale and future schema versions before temporal preparation. `dtgproxy/tests/service_cli.rs` |
| Stateful Sidecar exhausted its worker capacity permanently after enough disconnected clients | It now uses a fixed reusable worker pool and bounded pending queue. `adapter-sidecar/tests/stateful_snapshot.rs` |
| Snapshot sessions had no resource bound or expiry | Sessions reserve one of 64 slots before export/restore and expire after five minutes of inactivity; abort/completion releases the slot. `adapter-sidecar/tests/stateful_snapshot.rs` |
| Sidecar timeout profile fields were accepted but ignored | Connect/read/write timeout parameters are parsed, validated, and installed on sockets. `adapter-sidecar/tests/stateful_snapshot.rs` |
| Durable Home decisions existed but orphan intent recovery was not wired into Gateway startup | Startup scans participant records and deterministically rolls committed transactions forward or expired undecided transactions back. Recovery commands reuse stable phase IDs and are idempotent. `dtgproxy/tests/distributed_transactions.rs` |

## Residual production blockers

These do not invalidate the prototype claim, but must be closed before a production release.

1. **P1 — Network trust boundary.** Gateway, Raft transport, and Sidecar use plaintext TCP and have
   no authentication, authorization, tenant quotas, or TLS. Bind them only to a trusted local or
   isolated development network.
2. **P1 — Migration cutover transaction.** Logical copies and per-Replica index fences are durable,
   and the Catalog generation determines restart behavior. The multi-Replica in-process cutover is
   not itself a durable state machine, so injected backend/Catalog failures between individual
   cutovers still need a persisted prepare/commit/rollback protocol.
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
6. **P2 — Service concurrency.** The prototype Gateway serializes requests in one process. This
   makes Current global scans and backend cutover easy to reason about, but it is not the final
   throughput architecture. A production Gateway needs concurrent read execution and per-graph or
   per-Shard mutation/migration serialization.
7. **P2 — Operations.** Graceful signal handling, structured tracing/metrics, health/readiness
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
storage TCKs, malformed codec/parser inputs, distributed commit/abort, restart recovery, backend
migration, Sidecar protocol limits, a real CLI process restart, and global merge epoch/snapshot
fences. Environment-gated PostgreSQL/Neo4j live tests remain explicitly ignored unless their
documented variables are supplied.

## Recommended 1.1 order

1. Persist a migration state machine and add crash injection at every copy/dual-apply/cutover/Catalog
   boundary.
2. Add background transaction recovery plus transaction metadata/history garbage collection.
3. Replace buffered snapshots and global scans with streaming, backpressured execution.
4. Add mTLS/service identity, authorization, quotas, observability, and external backend CI matrices.
5. Run service-level throughput, tail-latency, recovery-time, and scale-out benchmarks before making
   a high-performance production claim.
