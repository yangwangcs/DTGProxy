# Durable backend migration runbook

This runbook covers the independently deployed Meta, Data, and Controller path. Backend migration
is a Catalog-owned workflow; do not invoke a local Adapter cutover on an individual Data node.

## Workflow and invariants

The Controller reconciles this durable state machine:

```text
Preparing -> Restored -> DualApplying -> Verified -> CutOver
          -> Published -> SourceRetired

Preparing/Restored/DualApplying/Verified -> Aborting -> Aborted
```

`Restored`, `DualApplying`, `Verified`, `CutOver`, and `SourceRetired` carry one receipt per
`(shard, voter)`. A restored receipt binds that physical replica to the digest of its resolved Data
backend profile. This digest is intentionally different from the logical Catalog profile digest:
the Controller derives a unique instance and path for every Shard, and may resolve a logical
PostgreSQL/Neo4j target through a local Sidecar.

The target generation is always `source_generation + 1`. Data restores a logical snapshot at an
applied-index fence, Raft commits the start of dual apply, verification waits until every target is
at least as current as its source replica, and Raft commits cutover before Meta atomically publishes
the graph's new Backend Profile. Controller and Data restarts replay these steps idempotently.

## Controller and admin commands

Run the long-lived reconciler with the same loopback-only configuration used by the cluster:

```bash
cargo run -p controller --bin dtgproxy-controller -- controller.json
```

The configuration has this versioned shape:

```json
{
  "version": 1,
  "cluster_id": "11111111111111111111111111111111",
  "controller_id": 9,
  "meta_seeds": ["127.0.0.1:7001"],
  "data_nodes": {"1": "127.0.0.1:7101", "2": "127.0.0.1:7102"},
  "reconcile_interval_ms": 50,
  "request_timeout_ms": 5000,
  "security": "loopback_plaintext"
}
```

Choose a stable, nonzero 128-bit hexadecimal migration ID and retain it in the change ticket. The
same ID and target may be submitted again safely after a lost response; reuse with a different
target is rejected.

```bash
cargo run -p controller --bin dtgproxy-admin -- \
  backend-migrate controller.json 019f0000000000000000000000000001 7 target.json

cargo run -p controller --bin dtgproxy-admin -- \
  backend-status controller.json 019f0000000000000000000000000001
```

Before `CutOver`, request a durable rollback with:

```bash
cargo run -p controller --bin dtgproxy-admin -- \
  backend-abort controller.json 019f0000000000000000000000000001
```

Rollback after the cutover fence is rejected. At that point, finish reconciliation and, if needed,
start a new forward migration to another generation.

## RocksDB target

The Controller derives a replica-local `backend-generation-N` directory, so the target file needs
no path and contains no secret:

```json
{
  "version": 1,
  "provider": "rocksdb",
  "public_parameters": {},
  "secret_references": {}
}
```

## PostgreSQL target

Run one stateful Sidecar endpoint per Shard. The database credential stays in the Sidecar process
and is never sent over the Sidecar wire or persisted in Catalog:

```bash
DTGPROXY_POSTGRES_URL='postgresql://user:password@127.0.0.1/dtgproxy' \
DTGPROXY_INSTANCE_ID='bootstrap-graph-7-shard-10' \
DTGPROXY_LISTEN='127.0.0.1:9711' \
cargo run -p adapter-postgres --bin dtgproxy-postgres-sidecar
```

```json
{
  "version": 1,
  "provider": "postgresql",
  "public_parameters": {
    "sidecar_endpoint.shard.10": "127.0.0.1:9711",
    "pool_size": "8"
  },
  "secret_references": {}
}
```

For a multi-Shard graph, provide `sidecar_endpoint.shard.<shard-id>` for every Shard. A single
`sidecar_endpoint` is accepted only when all placements deliberately share one endpoint.

## Neo4j target

```bash
DTGPROXY_NEO4J_ENDPOINT='http://127.0.0.1:7474' \
DTGPROXY_NEO4J_USERNAME='neo4j' \
DTGPROXY_NEO4J_PASSWORD='change-me' \
DTGPROXY_NEO4J_DATABASE='neo4j' \
DTGPROXY_INSTANCE_ID='bootstrap-graph-7-shard-10' \
DTGPROXY_LISTEN='127.0.0.1:9712' \
cargo run -p adapter-neo4j --bin dtgproxy-neo4j-sidecar
```

```json
{
  "version": 1,
  "provider": "neo4j",
  "public_parameters": {
    "sidecar_endpoint.shard.10": "127.0.0.1:9712",
    "endpoint": "http://127.0.0.1:7474",
    "database": "neo4j",
    "username": "neo4j"
  },
  "secret_references": {}
}
```

The Sidecar owns the password and injects it when opening the generated target instance
`graph-<graph-id>-shard-<shard-id>-generation-<generation>`.

## Recovery checks

After `source_retired`, verify all of the following:

1. `backend-status` reports `source_retired` and the expected target generation.
2. Every Data replica reports `ready` with that generation.
3. Reads cover records written before snapshot restore and during dual apply.
4. Restart each Data process and repeat the reads.
5. Preserve the target Sidecar's active generated instance configuration before restarting that
   Sidecar. Version 1.0 Sidecar selection is process-local; automatic selector persistence is a
   boundary item, even though PostgreSQL/Neo4j data and applied indexes themselves are durable.
