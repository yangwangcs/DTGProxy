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

## Backend certification status

The exclusions recorded in the original 2026-07-18 verification run are closed. PostgreSQL 17 and
Neo4j 5.26 Community are provisioned as disposable GitHub Actions services. Their live Mapping
tests are not marked `#[ignore]`; an unavailable or incompatible service fails certification.

`.github/workflows/three-backend-migration.yml` runs all six migration directions and the complete
RocksDB/PostgreSQL/Neo4j x PrimaryReplica/Shared-Nothing distributed surface test, including
Temporal Cypher, temporal transactions, projections, synchronous/asynchronous Degree, result
pagination, and lease-expiry takeover by a second Gateway. This document remains the P0 runtime
record; current backend evidence is maintained in `docs/postgresql-adapter.md`,
`docs/adapter-spi.md`, and `.superpowers/sdd/progress.md`.
