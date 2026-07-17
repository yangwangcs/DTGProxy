# DTGProxy PostgreSQL Adapter

Status: the Rust Adapter, Factory restore path, and loopback Sidecar executable are implemented. Static SQL/secret-boundary tests pass. Live PostgreSQL tests and cross-backend RocksDB↔PostgreSQL migration tests compile but are deliberately ignored because this development machine has neither a PostgreSQL server nor a running Docker daemon. This backend is not yet certified for release.

## Correctness layout

The Adapter owns the static `dtgproxy` schema. User data never becomes SQL identifiers or SQL text.

- `schema_meta` fences the global Adapter schema version.
- `adapter_instance` owns one instance ID and its 8-byte unsigned `applied_log_index`.
- `canonical_kv` stores `(instance_id, keyspace, logical_key, value)` with `BYTEA` keys and values and a primary-key B-tree in canonical order.
- committed-log fingerprints, mutation fingerprints, Replica metadata, bitemporal records, both adjacency directions, and transaction records use the same reserved logical keys as RocksDB.

An apply obtains the instance control row with `FOR UPDATE`, validates contiguous/replayed log position and mutation fingerprints, applies every put/delete, writes replay metadata and the canonical applied-index key, then updates the control row in one `SERIALIZABLE` transaction. `SET LOCAL synchronous_commit = on` is issued before mutation. The descriptor reports synchronous durability only when the connected server reports `fsync=on`; otherwise the managed-replica capability gate rejects startup. PostgreSQL documents that `synchronous_commit=on` includes a local durable commit and that `fsync=off` prevents WAL updates from being forced. [PostgreSQL WAL configuration](https://www.postgresql.org/docs/current/runtime-config-wal.html)

Reads use one `REPEATABLE READ READ ONLY` transaction per multi-get or scan. PostgreSQL documents that all statements in such a transaction see the same snapshot. [PostgreSQL transaction modes](https://www.postgresql.org/docs/current/sql-set-transaction.html) `BYTEA` is used because binary-string operations process the actual bytes rather than locale-dependent characters. [PostgreSQL binary data](https://www.postgresql.org/docs/current/datatype-binary.html)

## Process and connection safety

- A stable two-key session advisory lock makes one Sidecar the only DTGProxy writer for an instance ID. Lock collision fails closed.
- A separate global advisory lock serializes first-time DDL and schema-version checks.
- The fixed connection pool is bounded to 1..=128 connections.
- Every connection sets a 30-second statement timeout, 10-second lock timeout, and 60-second idle-in-transaction timeout.
- Connection strings enter the Factory only as `SecretString`; the Sidecar reads `DTGPROXY_POSTGRES_URL` and never prints it.
- The current driver uses `NoTls`; consequently the Sidecar executable rejects non-loopback listeners. Database TLS and authenticated Sidecar transport remain release gates.

## Logical migration

PostgreSQL exports all canonical rows from one long-lived `REPEATABLE READ` transaction in bounded, globally ordered chunks. Export queries use a streaming row iterator and enforce the chunk byte budget before retaining each row; concurrent export sessions are capped at `pool_size` and release their permit on drop. Restore creates a newly leased instance namespace with `published=false`, idempotently writes chunks, verifies the final BLAKE3 manifest, and commits the canonical/control applied index together with `published=true` only at the end. Ordinary open rejects an unpublished namespace, so a process crash cannot expose partial restore data. A later restore that obtains the same advisory lease reclaims an unpublished crash residue and restarts from an empty namespace; dropping a failed in-process restore deletes only that target namespace. The Registry validates the restore session's stable target descriptor before consuming data or allowing `finish` to publish it.

The ignored live tests cover:

- atomic apply, exact replay, multi-get, prefix scan, lease exclusion, restart, export, restore, and continued log apply;
- PostgreSQL→PostgreSQL logical restore;
- RocksDB→PostgreSQL and PostgreSQL→RocksDB byte-preserving migration.

Run against a disposable database whose role can create the `dtgproxy` schema:

```bash
DTGPROXY_POSTGRES_URL='host=127.0.0.1 user=dtgproxy password=... dbname=dtgproxy' \
CXX=/opt/homebrew/opt/llvm/bin/clang++ \
LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib \
cargo test -p adapter-postgres --test live_postgres -- --ignored
```

The loopback Sidecar requires `DTGPROXY_POSTGRES_URL` and `DTGPROXY_INSTANCE_ID`; optional variables are `DTGPROXY_LISTEN` (default `127.0.0.1:9711`) and `DTGPROXY_POSTGRES_POOL_SIZE` (default 8). The current Sidecar wire serves describe/apply/read/scan/health. Logical export/restore sessions must be added to the versioned wire before remote hot migration is considered complete.
