# DTGProxy 1.0 main-path acceptance

This document records the implemented prototype main path. The separate
[final boundary audit](dtgproxy-v1-boundary-audit.md) records fixes and remaining production gates.

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
| Independent Meta/Data/Gateway/Controller processes | `meta-node/tests/process_quorum.rs`, `data-node/tests/process_restart.rs`, `gateway-node/tests/process.rs`, `controller/tests/config.rs` |
| Shared Data-node multi-Raft transport | `data-node/tests/raft_network.rs`, `data-node/tests/raft_runtime.rs` |
| Crash-replayable Shard migration and epoch lineage | `controller/tests/crash_matrix.rs`, `controller/tests/remote_migration.rs`, `control-plane/tests/lineage.rs` |
| Joint-consensus leader replacement | `data-node/tests/multi_raft_host.rs`, `controller/tests/remote_migration.rs` |
| Durable transaction/backup/CDC cleanup pins | `control-plane/tests/retention_pins.rs`, `controller/tests/reconciliation.rs` |

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

## Boundary status

Malformed Gateway isolation, Sidecar connection reuse/session bounds, schema fencing, timeout
handling, startup transaction recovery, durable Shard migration cutover, epoch lineage, and
recoverable source cleanup are closed. Cluster-wide backend-generation orchestration,
external-service matrices, untrusted-network security, streaming backpressure, disk-full testing,
SBOM review, and service capacity qualification remain production gates.
