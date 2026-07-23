# DTGProxy Temporal Backend Mapping and Storage Adapter SPI

Status vocabulary is exact: **implemented** means code and local contract tests exist; **compiled**
means an external backend path builds but has not run against a real backend; **real-backend
certified** means the Mapping TCK has passed against a disposable real backend; **distributed
certified** means the backend has additionally passed the two-mode cluster matrix. The current
`TemporalBackendMapping` SPI, lifecycle bridge, Registry schema fencing and shared Mapping TCK are
implemented. RocksDB is the canonical KV reference. PostgreSQL 17 and Neo4j 5.26 Community have
both passed the generic Mapping TCK and the shared full temporal-graph Mapping TCK against
disposable instances. The two-mode distributed matrix and six-direction migration matrix remain
are certified by the CI workflow and current local evidence.

## Objective

The temporal model, Raft command, transaction protocol, and query IR remain backend-independent.
Every backend receives deterministic canonical logical key/value mutations representing Identity,
Current, History, outbound/inbound adjacency, temporal indexes, transaction records, and Replica
metadata. Canonical KV is the logical interchange and verification contract; PostgreSQL and Neo4j
are not required to retain a duplicate raw-KV table once their native Mapping can reconstruct the
same canonical bytes exactly.

`TemporalBackendMapping` is the sole user-extensible backend interface. It provides:

- versioned schema description and nonzero schema fingerprint;
- configuration/schema validation before serving;
- `prepare`, `apply`, `commit`, and `abort` over one `CommittedMutationBatch`;
- canonical multi-get and bytewise ordered scan;
- canonical export and restore;
- durable `applied_log_index` and capability declarations.

`StorageAdapter` is the internal Shard runtime port. `MappingBackedAdapter` drives the Mapping
lifecycle so Raft, transactions, query execution and snapshot transfer do not depend on backend
layout. A failed apply or commit is explicitly aborted; acknowledgement follows only a durable
commit.

Optional extensions cover checkpoint/logical export, predicate pushdown, adjacency pushdown, and change feeds.

## Backend matrix

| Backend | Family | Integration | Snapshot form | Current status |
|---|---|---|---|---|
| RocksDB | KV | in-process Mapping | physical checkpoint + canonical logical export/restore | Mapping and two-mode distributed matrix certified |
| PostgreSQL | SQL | Rust Sidecar, parameterized SQL | repeatable logical export / logical restore | PostgreSQL 17 real-backend and two-mode distributed matrix certified |
| Neo4j | property graph | Rust Sidecar over Query API | logical export / logical restore | Neo4j 5.26 Community real-backend and two-mode distributed matrix certified |
| Memgraph | property graph | Rust/Bolt Sidecar | logical export or transactional snapshot | compatibility target, not yet certified |
| Memory | test | in-process | none | development only; production gate rejects it |

RocksDB documents atomic `WriteBatch`, including cross-column-family operations; DTGProxy additionally uses synchronous WAL writes and places business mutations, replay fingerprints, and `applied_log_index` in the same batch. [RocksDB overview](https://github.com/facebook/rocksdb/wiki/RocksDB-Overview) [RocksDB atomic updates](https://github.com/facebook/rocksdb/wiki/Basic-Operations)

PostgreSQL `REPEATABLE READ` provides one transaction snapshot across statements, and `SERIALIZABLE` can reject non-serializable executions. DTGProxy will still own transaction timestamps and cross-Shard decisions; PostgreSQL isolation protects one Adapter transaction. [PostgreSQL SET TRANSACTION](https://www.postgresql.org/docs/current/sql-set-transaction.html) Synchronous commit behavior is an explicit deployment capability, not assumed from the product name. [PostgreSQL WAL settings](https://www.postgresql.org/docs/current/runtime-config-wal.html)

The implemented PostgreSQL Mapping uses only parameterized values in a static reserved schema, a
bounded synchronous connection pool, a session advisory writer lease, Mapping fingerprint
fencing, `SERIALIZABLE` applies with `synchronous_commit=on`, and `REPEATABLE READ` reads/exports.
Its non-ignored live suite starts from a disposable PostgreSQL instance and includes the shared
Mapping TCK plus RocksDB↔PostgreSQL migration; see [PostgreSQL Mapping evidence](postgresql-adapter.md).

Neo4j documents ACID transactions and a write-ahead transaction log. Online backup and differential backup are edition/deployment capabilities, so an Adapter must report what the connected instance actually supports. [Neo4j transactional behavior](https://neo4j.com/docs/operations-manual/current/database-internals/) [Neo4j backup](https://neo4j.com/docs/operations-manual/current/backup-restore/online-backup/)

The implemented Neo4j Mapping materializes typed identity/current/history/adjacency record nodes,
stable endpoint nodes and native `DTG_EDGE` relationships. The Adapter instance, replay
fingerprints, applied index, Mapping version and Mapping fingerprint commit under the same atomic
Cypher statement as a business batch. Exact mutation replay is idempotent; divergent mutation
fingerprints are rejected inside the commit transaction rather than relying only on the prepare
precheck. Canonical getters and export reproduce byte-identical records, and restore publishes only
an unpublished instance after manifest verification. Its non-ignored suite covers native temporal
materialization, relationship deletion, atomic replay fencing, export/restore/continue, the generic
Mapping TCK and the shared temporal-graph Mapping TCK.

Memgraph documents Bolt compatibility and a Rust client path, but storage mode changes the guarantee. `IN_MEMORY_ANALYTICAL` explicitly lacks ACID and is rejected; only a transactional mode with verified durability may pass the production gate. [Memgraph Rust client](https://memgraph.com/docs/client-libraries/rust) [Memgraph transactions](https://memgraph.com/docs/fundamentals/transactions) [Memgraph storage modes](https://memgraph.com/docs/fundamentals/storage-memory-usage)

## Production capability gate

Before an Adapter capability check, Registry compares the Factory-declared Mapping descriptor with
the opened instance. Missing, unexpected, version/fingerprint-drifted, or backend-family-mismatched
descriptors fail closed. `MappingDescriptorV1::validate(ManagedReplica)` requires the current SPI,
atomic lifecycle, deterministic conversion, idempotent replay, canonical multi-get/ordered scan,
durable applied index, synchronous durability and snapshot recovery. `HotPluggableReplica` also
requires canonical export and restore.

`AdapterDescriptorV1::validate(ManagedReplica)` checks, in order:

1. exact SPI version;
2. local atomic batch;
3. idempotent replay;
4. consistent multi-get;
5. ordered scan;
6. durable applied index;
7. synchronous durable acknowledgement;
8. physical checkpoint or logical export.

`HotPluggableReplica` adds two independent requirements: canonical logical export and logical restore. A native checkpoint is not considered portable across backend families.

Product labels are not capabilities. For example, a Memgraph analytical instance, PostgreSQL with an unverified asynchronous durability profile, or a graph endpoint without portable export fails startup for a managed production Replica.

## Registration and isolation

Rust has no stable native plugin ABI, so DTGProxy does not `dlopen` arbitrary Rust dynamic libraries.

- RocksDB is a statically linked, registered in-process factory.
- SQL and graph drivers run in separately supervised Sidecars behind a versioned protocol.
- New backend integrations implement `TemporalBackendMapping`; they do not modify Raft,
  transactions, Temporal Cypher or analytics code.
- Provider names are canonical and duplicate/unknown names fail closed.
- Public parameters and secrets are separate; debug output always redacts secret values.
- The opened instance—not only its configuration—is described and validated before it can serve a Shard.

## Sidecar boundary

`adapter-sidecar` implements a canonical Protobuf payload inside a fixed `DTAS` binary frame. The frame has an explicit wire version, request/response kind, 128-bit request identifier, bounded 16 MiB payload length, and CRC32. Unknown versions, flags, message variants, enum values, non-canonical encodings, length mismatches, and checksum failures fail closed.

The deliberately small remote surface is `Describe`, `Apply`, `MultiGet`, `Scan`, `AppliedLogIndex`, and `Health`. The client checks the actual descriptor and readiness at connect time, caches the durable applied index, and rejects write acknowledgements behind the required index, index regression, wrong multi-get cardinality, out-of-range/unordered scans, unexpected response types, and remote structured errors.

The TCP implementation provides a fixed-size persistent connection pool, positive connect/read/write timeouts, `TCP_NODELAY`, request/response ID matching, and one reconnect retry with the original request ID. The bounded server uses a fixed worker count and finite pending-connection queue; it does not create a thread per connection and has explicit shutdown that interrupts active connections. A full queue sheds new connections and lets the client timeout/retry.

The Sidecar frame request ID is transport correlation, not the storage idempotency key. A response can be lost after an apply, so a retry may execute twice. Correctness therefore depends on the mandatory Adapter rule that the same committed `(shard_id, log_index, txn_id, mutation fingerprint)` is idempotent and a different replay at the same log index fails.

The current TCP transport is appropriate only on a trusted same-host/private test boundary. Unix-domain peer authentication or mTLS, authorization, per-tenant admission, protocol fuzzing, metrics/traces, and certificate rotation remain release gates before a network-exposed Sidecar is production-supported.

## Index-fenced backend migration

“Hot plug” does not mean replacing a live database pointer without data movement. The implemented `HotSwapAdapter` protocol is:

1. open one consistent source snapshot at applied index `N` and stream every Keyspace in strict logical-key order;
2. validate per-chunk ordinals/digests and the final content manifest while restoring into a hidden target generation;
3. atomically publish the target only after the complete manifest and its durable applied-index record match;
4. verify source and target both durably report `applied_log_index = N`;
5. enter dual-apply generation `g+1`;
6. acknowledge each subsequent Raft entry only after source and target both apply it;
7. retry partial progress using the same idempotent batch;
8. cut over only with no apply in flight and equal durable indices;
9. retain the old generation for validation/rollback policy, then retire it explicitly.

The implemented logical format binds format version, 128-bit snapshot ID, applied index, strict chunk ordinal, globally ordered `(keyspace, key, value)` entries, per-chunk BLAKE3, entry/chunk totals, and a whole-stream BLAKE3 manifest. Export limits are explicit and capped at 65,536 entries / 16 MiB per chunk. `AdapterRegistry::restore` drives a source reader into a target Factory restore session, then validates the opened target against the selected capability requirement.

The RocksDB target writes synchronous chunks into a non-visible sibling staging directory, defers publication of `applied_log_index` until final verification, syncs the tree, and renames the directory atomically. A failed or corrupt manifest removes the owned staging generation and never creates the target path.

The backend type is immutable within a placement epoch. Cutover is recorded as a control-plane compare-and-swap with a new epoch. Long-term migration additionally needs background snapshot transfer, WAL retention fences, checksum comparison across every keyspace, rollback deadlines, and operator APIs.

## Projection strategy

### PostgreSQL

The authoritative layout is native: stable vertex/edge identity tables, separate current tables,
vertex/edge history, outgoing/incoming adjacency, opaque non-graph records, replay fingerprints and
the applied-index control row. It does not retain a duplicate `canonical_kv` table. Fixed-width
typed identity columns and validated record payloads are converted back through the core codec for
byte-identical canonical getters and snapshots. Every user value remains a parameter; identifiers
come only from static schema names.

### Neo4j / Memgraph

Native nodes and relationships accelerate graph traversal, while stable DTG identity, transaction intervals, valid intervals, adjacency mirrors, and replay metadata remain explicit properties/records. A single Adapter apply transaction updates native graph objects and the durable Replica frontier together. Bolt compatibility alone is insufficient: supported constraints, transaction isolation, durability, backup/export, value types, and Cypher differences are probed independently.

## Safety boundary

Applications must not write Adapter-owned tables, keys, nodes, or relationships directly. Out-of-band writes invalidate replay fingerprints and temporal history. The production package will use least-privilege credentials, reserved namespaces, schema fingerprints, periodic full-keyspace checksums, and drift alarms.
