# DTGProxy 1.0 Main-System Implementation Plan

> Execution priority: finish the runnable vertical product path first. Boundary audits,
> exhaustive fault injection, security hardening, and performance release gates are the final
> task, as explicitly requested by the project owner.

**Goal:** Deliver a runnable Rust DTGProxy 1.0 prototype with PrimaryReplica and SharedNothing,
distributed bitemporal transactions, deterministic routing and querying, RocksDB/PostgreSQL/
Neo4j backend profiles, and controlled hot-plug migration.

**Architecture:** Keep bitemporal meaning, transaction decisions, partitioning, Raft, and query
merge in DTGProxy. Backends receive deterministic logical KV mutations through `StorageAdapter`;
RocksDB is in-process, PostgreSQL and Neo4j are strict Sidecar-capable profiles. Every data Shard
is a Raft group in either deployment mode. SharedNothing adds routing and cross-Shard 2PC rather
than replacing replication.

**Technology:** Rust 2024, `raft-rs`, existing Storage Adapter SPI, RocksDB, PostgreSQL, DTAS
Sidecar frames, versioned binary codecs. Neo4j is the selected graph backend for 1.0 because its
transactional Cypher surface can express the required atomic canonical-record projection;
Memgraph remains an SPI-compatible follow-up profile.

---

## Delivery order and release definition

The 1.0 main-code gate is one executable deployment that can:

1. load a versioned configuration and durable topology;
2. start in PrimaryReplica or SharedNothing mode;
3. allocate globally monotonic transaction timestamps;
4. execute single- and multi-Shard bitemporal transactions;
5. recover an in-doubt transaction from its durable Home record;
6. query one or several Shards at one snapshot and canonically merge results;
7. open RocksDB, PostgreSQL, or Neo4j through one backend profile abstraction;
8. logically migrate a profile and continue from the fenced applied index;
9. expose those operations through the `dtgproxy` CLI/service entry point.

The final boundary gate then adds exhaustive malformed-input matrices, long-running TTL/Reaper
tests, disk-full/network-chaos/security audit, and performance envelope certification.

## Task 1: Persistent Timestamp Oracle and Snapshot Tokens

**Files:**

- Create: `crates/timestamp-oracle/Cargo.toml`
- Create: `crates/timestamp-oracle/src/lib.rs`
- Create: `crates/timestamp-oracle/tests/oracle.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

Implement a thread-safe Oracle with `next`, `next_after`, `snapshot`, and `closed_timestamp`.
Persist a reserved high-watermark before issuing any timestamp from a new block. A restart may
skip timestamps but must never reuse or regress one. Provide `MemoryTimestampStore` for tests and
`FileTimestampStore` for the runnable process; the file format is versioned, checksummed, synced,
and atomically replaced.

Test first: monotonic allocation under physical-clock regression, `next_after`, concurrent
uniqueness, durable restart beyond the reserved block, and corrupted-file rejection.

Commit: `feat: add persistent timestamp oracle`

## Task 2: Versioned Distributed Transaction Protocol

**Files:**

- Create: `crates/txn-protocol/Cargo.toml`
- Create: `crates/txn-protocol/src/lib.rs`
- Create: `crates/txn-protocol/tests/codec.rs`
- Create: `crates/txn-protocol/tests/participant.rs`
- Modify: `Cargo.toml`

Define canonical `TransactionId`, isolation, transaction state, participant proof, read span,
intent, Home transaction record, Prewrite, CommitDecision, Finalize, and Abort messages. Encode
them with an explicit magic/version/length/checksum format. Participant state uses `Keyspace::Txn`
keys and returns deterministic mutations for atomic application with Replica metadata.

The participant validates epoch/schema, frozen participant set, write conflicts after `start_ts`,
idempotent Prewrite, digest equality, valid-time overlap guards, and the irreversible
`Committed -> Applied` transition. Main-path recovery reads the Home record and deterministically
chooses roll-forward or rollback.

Commit: `feat: define distributed transaction protocol`

## Task 3: Replicate Transaction State Through Raft

**Files:**

- Modify: `crates/raft-command/src/lib.rs`
- Modify: `crates/raft-command/tests/codec.rs`
- Modify: `crates/shard-runtime/src/state_machine.rs`
- Modify: `crates/shard-runtime/src/metadata.rs`
- Modify: `crates/shard-runtime/src/lib.rs`
- Create: `crates/shard-runtime/tests/distributed_transaction.rs`

Add versioned Raft bodies for `Prewrite`, `RecordDecision`, `Finalize`, and `AbortIntent` without
renumbering existing tags. State-machine Apply never consults clocks or remote services: each
entry contains fixed timestamps, participant proofs, and deterministic mutations. Persist the
oldest unresolved Intent frontier and derive `resolved_ts` from replicated state.

Prove single-Shard 1PC remains one entry, multi-Shard Prewrite survives Replica restart, and replay
of every phase is byte-identical and idempotent.

Commit: `feat: replicate transaction intents and decisions`

## Task 4: Cross-Shard Coordinator and Temporal Rewrite

**Files:**

- Create: `crates/dtgproxy/src/transaction.rs`
- Create: `crates/dtgproxy/src/runtime.rs`
- Modify: `crates/dtgproxy/src/lib.rs`
- Create: `crates/dtgproxy/tests/distributed_transactions.rs`

Implement `TransactionCoordinator` over abstract TSO and Shard clients. Discover and freeze
participants, rewrite mutations at one `start_ts`, perform parallel Prewrite, allocate
`commit_ts > max(start_ts, min_commit_ts)`, replicate the Home decision, Finalize every
participant, and return a queryable final status for client retries. Preserve the 1PC fast path.

For cross-partition edges, route identity/out-adjacency to the source Shard and in-adjacency plus
endpoint guards to the destination Shard. A commit is acknowledged only after every participant
Adapter has applied it in 1.0.

Commit: `feat: coordinate cross-shard temporal transactions`

## Task 5: Durable Control Plane and Backend Profiles

**Files:**

- Create: `crates/control-plane/Cargo.toml`
- Create: `crates/control-plane/src/lib.rs`
- Create: `crates/control-plane/tests/catalog.rs`
- Modify: `Cargo.toml`
- Modify: `crates/dtgproxy/src/lib.rs`

Persist graph definitions, route seed, virtual partitions, placements, placement epoch, schema
version, and one backend profile per logical graph. Use a checksummed snapshot plus append-only
decision log for the single-process 1.0 control plane; expose a consensus-ready command API so it
can later become its own Meta Raft group without changing Gateway code.

Backend profiles contain provider name, public parameters, local secret references, required
capability level, and generation. Publishing a profile or placement increments its epoch; stale
requests are rejected.

Commit: `feat: add durable control-plane catalog`

## Task 6: Runnable Node/Gateway for Both Deployment Modes

**Files:**

- Modify: `crates/dtgproxy/src/main.rs`
- Create: `crates/dtgproxy/src/config.rs`
- Create: `crates/dtgproxy/src/gateway.rs`
- Create: `crates/dtgproxy/tests/service_cli.rs`
- Modify: `crates/dtgproxy/Cargo.toml`

Add versioned configuration parsing and commands for `init`, `serve`, `status`, `transaction`,
`query`, `backend verify`, and `backend migrate`. The service owns Control Plane, TSO, Shard
groups, Adapter registry, transaction coordinator, and query gateway. PrimaryReplica constructs
one Shard group; SharedNothing constructs and routes multiple independent groups.

The initial API may use the existing bounded TCP framing and canonical JSON command bodies; API
types remain separate from transport so gRPC can be added later. Remove Phase-specific messaging
from the user-facing CLI.

Commit: `feat: add runnable DTGProxy gateway service`

## Task 7: Complete the Three-Backend Matrix and Hot-Plug Main Path

**Files:**

- Create: `crates/adapter-neo4j/Cargo.toml`
- Create: `crates/adapter-neo4j/src/lib.rs`
- Create: `crates/adapter-neo4j/tests/contract.rs`
- Modify: `Cargo.toml`
- Modify: `crates/adapter-sidecar/src/service.rs`
- Modify: `crates/adapter-sidecar/src/lib.rs`
- Add focused Sidecar export/restore happy-path tests

Select Neo4j for the graph reference backend. Store canonical DTGProxy records as reserved graph
nodes keyed by `(instance, keyspace, order-preserving key encoding)` and update the canonical
payload, native Current projection, mutation fingerprints, and applied index in one database
transaction. Use parameterized Cypher; never depend on native property coercions for semantic
round-trip.

Complete the happy-path stateful Sidecar snapshot service and remote Factory/Reader so Registry
migration can copy RocksDB <-> PostgreSQL <-> Neo4j, validate the final descriptor/index, and
publish the new backend generation. Reaper, exhaustive replay-window, and network-chaos audits
remain Task 10.

Commit: `feat: add Neo4j backend and hot-plug migration`

## Task 8: Distributed Query Fan-Out and Canonical Merge

**Files:**

- Modify: `crates/query-executor/src/lib.rs`
- Create: `crates/query-executor/src/distributed.rs`
- Modify: `crates/dtgproxy/src/gateway.rs`
- Create: `crates/dtgproxy/tests/distributed_query.rs`

Execute point plans on one routed Shard. Fan out global scans and graph frontiers only to required
Shards, carrying one Snapshot Token, then merge by canonical record key with deterministic limit
application. Reject mixed snapshot/epoch streams. Preserve leader-required CURRENT reads and
safe-time-qualified follower historical reads.

Commit: `feat: execute and merge distributed temporal queries`

## Task 9: End-to-End 1.0 Product Acceptance

**Files:**

- Create: `examples/primary-replica/`
- Create: `examples/shared-nothing/`
- Create: `docs/dtgproxy-v1-quickstart.md`
- Create: `docs/dtgproxy-v1-acceptance.md`
- Add end-to-end tests under `crates/dtgproxy/tests/`

Exercise both modes from CLI initialization through transaction commit and query. Include a
cross-Shard edge, process restart with in-doubt recovery, PostgreSQL/Neo4j profile verification,
and one logical backend migration. Run the full workspace tests and strict Clippy.

Commit: `docs: certify DTGProxy 1.0 main path`

## Task 10: Final Boundary Audit and Hardening

Resume and finish the detailed Sidecar session plan, then run malformed protocol/property tests,
session TTL/Reaper/resource exhaustion, transaction crash windows, disk-full/corruption, packet
loss, security/secret redaction, dependency/SBOM, and throughput/capacity benchmarks. Findings
that affect correctness are fixed before the 1.0 release label; hardening that only affects a
future production SLO is recorded separately and never represented as implemented.

Commit series: focused fixes followed by `docs: record DTGProxy 1.0 boundary audit`.
