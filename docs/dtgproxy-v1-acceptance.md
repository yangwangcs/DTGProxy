# DTGProxy 1.0 main-path acceptance

This document records the implemented prototype boundary before the separate final hardening audit.

| Capability | Executable evidence |
|---|---|
| PrimaryReplica routing and Raft replication | `dtgproxy/tests/deployment_modes.rs`, `shard-runtime/tests/raft_group.rs` |
| SharedNothing rendezvous routing | `dtgproxy/tests/deployment_modes.rs` |
| Durable timestamp allocation | `timestamp-oracle/tests/persistent.rs` |
| Distributed temporal transaction and durable Home decision | `dtgproxy/tests/distributed_transactions.rs` |
| Cross-partition edge OUT/IN projection | `dtgproxy/tests/distributed_transactions.rs` |
| Process reopen from persistent Adapter snapshot boundary | `shard-runtime/tests/raft_group.rs`, `dtgproxy/tests/service_cli.rs` |
| Durable control-plane fencing | `control-plane/tests/catalog.rs` |
| RocksDB backend | `adapter-rocksdb/tests/` |
| PostgreSQL backend and cross-backend snapshots | `adapter-postgres/tests/`; live tests are environment-gated |
| Neo4j Query API backend | `adapter-neo4j/`; live test is environment-gated |
| Stateful Sidecar export/restore and remote Factory | `adapter-sidecar/tests/stateful_snapshot.rs` |
| Online logical migration and catalog generation publish | `dtgproxy/tests/service_cli.rs` |
| Distributed global scan and deterministic merge | `dtgproxy/tests/distributed_query.rs`, `query-executor/tests/distributed.rs` |
| Runnable CLI/API and process restart recovery | `dtgproxy/tests/service_cli.rs`, `dtgproxy/tests/process_e2e.rs`, `examples/` |

## Verification commands

```bash
export CXX=/opt/homebrew/opt/llvm/bin/clang++
export LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets -- -D warnings
```

Ignored live tests require disposable services:

- `DTGPROXY_POSTGRES_URL` for PostgreSQL.
- `DTGPROXY_NEO4J_ENDPOINT`, `DTGPROXY_NEO4J_PASSWORD`, and optionally `DTGPROXY_NEO4J_USERNAME`/`DTGPROXY_NEO4J_DATABASE` for Neo4j.

## Deliberately deferred to the final boundary audit

Malformed Sidecar frames, session reaping/resource exhaustion, crash injection through every migration cutover window, disk-full/corruption, network packet-loss matrices, secret-redaction/SBOM review, and throughput/capacity characterization are audited only after the main code path is complete. They are not claimed by this acceptance record.
