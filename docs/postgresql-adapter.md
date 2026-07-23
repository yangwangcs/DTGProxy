# DTGProxy PostgreSQL Native Temporal Mapping

Status: the PostgreSQL native `TemporalBackendMapping` is implemented and has passed the shared
Mapping TCK against a disposable PostgreSQL 17 instance. Real-instance tests are ordinary tests,
not `#[ignore]`. The full PostgreSQL × deployment-mode Temporal Cypher/transaction/analytics
matrix is also certified by the current three-backend CI workflow; this document separates the
native Mapping evidence from the distributed deployment evidence recorded in
`.superpowers/sdd/progress.md`.

## Native correctness layout

The Mapping owns one static `dtgproxy` schema. There is no `canonical_kv` shadow table.

- `schema_meta` stores the sole current Mapping name, version and 32-byte schema fingerprint.
- `adapter_instance` fences one instance ID, Mapping fingerprint, publication state and durable
  `applied_log_index`.
- `vertex_identity` and `edge_identity` store stable graph identities and edge endpoints.
- `vertex_current` and `edge_current` store current bitemporal projections.
- `history` stores vertex/edge transaction-time entries and their valid-time changes.
- `out_adjacency` and `in_adjacency` preserve local and cross-partition adjacency layouts.
- `opaque_records` stores only non-graph Meta, TemporalIndex and transaction-protocol records.
- `replay_log` and `replay_mutation` store deterministic idempotency fingerprints.

Unsigned canonical identifiers remain fixed-width, big-endian `BYTEA` columns, so every native row
can be converted back through the core temporal codec. Identity records are rebuilt from typed
columns; current, history and adjacency payload records are validated by their current canonical
record codecs. Export reconstructs exact `(keyspace, logical_key, value)` bytes and sorts them by
canonical key order before chunking.

## Atomic apply and recovery

The factory declares `postgresql-native-temporal/1.0.0` and returns a `MappingBackedAdapter`.
Factory and opened-instance Mapping descriptors must match exactly. Unknown name/version,
fingerprint drift, `fsync=off`, unpublished state or writer-lease conflict fails startup.

`prepare` validates the committed batch and stages no database-visible writes. `apply` marks the
prepared operation ready without publishing it. `commit` obtains the instance control row with
`FOR UPDATE`, validates contiguous/replayed log position and mutation fingerprints, applies every
native put/delete, writes replay state and advances the applied index in one `SERIALIZABLE`
transaction with `synchronous_commit=on`. `abort` publishes nothing.

Reads use one `REPEATABLE READ READ ONLY` transaction. Restore writes chunks only into a leased
`published=false` instance, verifies the canonical manifest, then atomically publishes the target
frontier. Failed or abandoned restore removes that target namespace; ordinary serving open never
exposes it.

## Real-instance certification

The non-ignored live suite covers:

- Mapping `prepare/apply/commit/abort`, exact replay, replay mismatch and non-contiguous index;
- canonical multi-get, ordered scan, delete/history retention and failed-batch invisibility;
- native vertex, edge, current, history and cross-partition in/out adjacency rows;
- PostgreSQL restart, canonical export/restore and continued writes;
- RocksDB → PostgreSQL and PostgreSQL → RocksDB canonical migration.

Run the self-contained local launcher. It uses an explicit `DTGPROXY_POSTGRES_URL` when supplied;
otherwise it creates a temporary SCRAM-authenticated local PostgreSQL cluster and removes it on
exit:

```bash
./scripts/test-postgres-live.sh
```

CI runs the same package against a disposable PostgreSQL 17 service in
`.github/workflows/postgres-mapping.yml`. Missing PostgreSQL infrastructure is a test failure, not
a successful skip.

The loopback Sidecar requires `DTGPROXY_POSTGRES_URL` and `DTGPROXY_INSTANCE_ID`; optional
variables are `DTGPROXY_LISTEN` (default `127.0.0.1:9711`) and
`DTGPROXY_POSTGRES_POOL_SIZE` (default 8). The current Sidecar wire serves
describe/apply/read/scan/health. Stateful remote export/restore remains a separate Sidecar protocol
gate and does not weaken the in-process Mapping certification above.
