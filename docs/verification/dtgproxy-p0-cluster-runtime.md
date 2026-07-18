# DTGProxy P0 cluster runtime verification

Date: 2026-07-18  
Toolchain: Rust 1.93, Homebrew LLVM/libclang, RocksDB 0.24.0  
Profile: development tests and strict Clippy on macOS/Apple Silicon

## Executed release gates

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test --workspace --all-targets

CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo clippy --workspace --all-targets --all-features -- -D warnings

cargo fmt --all -- --check
```

All non-environment-gated tests passed. The all-target test command also executes the development
benchmark binaries; their numbers are diagnostic and are not service SLOs.

## Cluster evidence

- Three-process Meta quorum failover preserves Catalog and never reuses a timestamp lease.
- Data process restart restores durable Replicas and request replay receipts.
- Two real Data gRPC services connected by private Raft transport complete snapshot copy, learner
  catch-up, joint membership, leadership transfer, epoch activation, Catalog cutover, and cleanup.
- Gateway is stateless, owns no Replica directory, loads topology from Meta, and remotely commits
  cross-Shard temporal transactions.
- Controller crash injection before and after every Catalog transition converges to one topology
  epoch and lineage.
- PrimaryReplica and SharedNothing semantic suites both pass; SharedNothing cross-Shard execution
  is exercised through remote clients and deterministic fan-out/merge tests.

## Environment-gated exclusions

- `adapter-neo4j/tests/live_neo4j.rs`: 1 ignored test; requires a disposable Neo4j service.
- `adapter-postgres/tests/live_postgres.rs`: 3 ignored tests; require a disposable PostgreSQL
  service.

These exclusions block production backend certification, not the local research-prototype claim.
The exact remaining boundaries are recorded in `docs/dtgproxy-v1-boundary-audit.md`.
