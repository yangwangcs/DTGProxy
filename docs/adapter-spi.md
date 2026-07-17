# DTGProxy Storage Adapter SPI v1

Status: SPI v1, capability gate, registry, RocksDB factory, and index-fenced hot-swap state machine are implemented. PostgreSQL and property-graph Sidecars are planned work and are not yet claimed as supported.

## Objective

The temporal model, Raft command, transaction protocol, and query IR remain backend-independent. A backend receives deterministic logical key/value mutations representing Identity, Current, History, outbound/inbound adjacency, temporal indexes, transaction records, and Replica metadata. Native graph/SQL layouts are projections and acceleration structures; they never become the only copy of DTGProxy transaction truth.

The core trait has five correctness operations:

- describe capabilities;
- atomically and idempotently apply one committed Raft batch;
- consistently read multiple logical keys;
- scan one keyspace in bytewise key order;
- return the durable `applied_log_index`.

Optional extensions cover checkpoint/logical export, predicate pushdown, adjacency pushdown, and change feeds.

## Backend matrix

| Backend | Family | Integration | Snapshot form | Current status |
|---|---|---|---|---|
| RocksDB | KV | in-process Rust Adapter | physical checkpoint | implemented and contract-tested |
| PostgreSQL | SQL | Rust Sidecar, parameterized SQL | repeatable logical export / database backup | design pending implementation |
| Neo4j | property graph | Sidecar over an official supported driver where possible | logical export; Enterprise backup is deployment-specific | design pending implementation |
| Memgraph | property graph | Rust/Bolt Sidecar | logical export or transactional snapshot | compatibility target, not yet certified |
| Memory | test | in-process | none | development only; production gate rejects it |

RocksDB documents atomic `WriteBatch`, including cross-column-family operations; DTGProxy additionally uses synchronous WAL writes and places business mutations, replay fingerprints, and `applied_log_index` in the same batch. [RocksDB overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview) [RocksDB atomic updates](https://github.com/facebook/rocksdb/wiki/Basic-Operations)

PostgreSQL `REPEATABLE READ` provides one transaction snapshot across statements, and `SERIALIZABLE` can reject non-serializable executions. DTGProxy will still own transaction timestamps and cross-Shard decisions; PostgreSQL isolation protects one Adapter transaction. [PostgreSQL SET TRANSACTION](https://www.postgresql.org/docs/current/sql-set-transaction.html) Synchronous commit behavior is an explicit deployment capability, not assumed from the product name. [PostgreSQL WAL settings](https://www.postgresql.org/docs/current/runtime-config-wal.html)

Neo4j documents ACID transactions and a write-ahead transaction log. Online backup and differential backup are edition/deployment capabilities, so an Adapter must report what the connected instance actually supports. [Neo4j transactional behavior](https://neo4j.com/docs/operations-manual/current/database-internals/) [Neo4j backup](https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/)

Memgraph documents Bolt compatibility and a Rust client path, but storage mode changes the guarantee. `IN_MEMORY_ANALYTICAL` explicitly lacks ACID and is rejected; only a transactional mode with verified durability may pass the production gate. [Memgraph Rust client](https://memgraph.com/docs/client-libraries/rust) [Memgraph transactions](https://memgraph.com/docs/fundamentals/transactions) [Memgraph storage modes](https://memgraph.com/docs/fundamentals/storage-memory-usage)

## Production capability gate

`AdapterDescriptorV1::validate(ManagedReplica)` checks, in order:

1. exact SPI version;
2. local atomic batch;
3. idempotent replay;
4. consistent multi-get;
5. ordered scan;
6. durable applied index;
7. synchronous durable acknowledgement;
8. physical checkpoint or logical export.

Product labels are not capabilities. For example, a Memgraph analytical instance, PostgreSQL with an unverified asynchronous durability profile, or a graph endpoint without portable export fails startup for a managed production Replica.

## Registration and isolation

Rust has no stable native plugin ABI, so DTGProxy does not `dlopen` arbitrary Rust dynamic libraries.

- RocksDB is a statically linked, registered in-process factory.
- SQL and graph drivers run in separately supervised Sidecars behind a versioned protocol.
- Provider names are canonical and duplicate/unknown names fail closed.
- Public parameters and secrets are separate; debug output always redacts secret values.
- The opened instance—not only its configuration—is described and validated before it can serve a Shard.

The Sidecar boundary must add request checksums, bounded frames, request IDs, deadlines, mTLS or same-host authenticated transport, health/readiness, backpressure, and structured retry classification. This wire protocol is the next SPI task.

## Index-fenced backend migration

“Hot plug” does not mean replacing a live database pointer without data movement. The implemented `HotSwapAdapter` protocol is:

1. create the target and restore/export all keyspaces through source index `N`;
2. verify source and target both durably report `applied_log_index = N`;
3. enter dual-apply generation `g+1`;
4. acknowledge each subsequent Raft entry only after source and target both apply it;
5. retry partial progress using the same idempotent batch;
6. cut over only with no apply in flight and equal durable indices;
7. retain the old generation for validation/rollback policy, then retire it explicitly.

The backend type is immutable within a placement epoch. Cutover is recorded as a control-plane compare-and-swap with a new epoch. Long-term migration additionally needs background snapshot transfer, WAL retention fences, checksum comparison across every keyspace, rollback deadlines, and operator APIs.

## Projection strategy

### PostgreSQL

The correctness layout uses byte-preserving tables keyed by `(shard_id, keyspace, logical_key)` plus transaction/log fingerprint and applied-index rows updated in the same SQL transaction. Typed relational columns and indexes are optional projections for pushdown. Every user value remains parameterized binary data; identifiers come only from validated static schema names.

### Neo4j / Memgraph

Native nodes and relationships accelerate graph traversal, while stable DTG identity, transaction intervals, valid intervals, adjacency mirrors, and replay metadata remain explicit properties/records. A single Adapter apply transaction updates native graph objects and the durable Replica frontier together. Bolt compatibility alone is insufficient: supported constraints, transaction isolation, durability, backup/export, value types, and Cypher differences are probed independently.

## Safety boundary

Applications must not write Adapter-owned tables, keys, nodes, or relationships directly. Out-of-band writes invalidate replay fingerprints and temporal history. The production package will use least-privilege credentials, reserved namespaces, schema fingerprints, periodic full-keyspace checksums, and drift alarms.
