# Durable backend migration runbook

The six canonical backend directions are continuously certified by
`.github/workflows/three-backend-migration.yml`. The non-ignored
`three_backend_migration` test writes a cross-partition temporal graph and verifies byte-identical
canonical snapshots, fixed-valid-time query signatures, fixed-snapshot Degree results and continued
writes for RocksDB ↔ PostgreSQL, RocksDB ↔ Neo4j and PostgreSQL ↔ Neo4j.

The same workflow certifies distributed analytics in two layers. The migration-and-surface job
runs the complete cross-backend semantic comparison. Six parallel takeover jobs select one backend
and one deployment mode through `DTGPROXY_CERT_BACKEND` and `DTGPROXY_CERT_MODE`. Each job runs
Degree, WCC and PageRank with a fail-once process stop at `Begin`; Degree also covers Claim,
LeaseRenew, ExecutionSlice, CheckpointUpload, CheckpointPin, CheckpointCas, ResultUpload, ResultPin
and Publish. Degree additionally restarts the Gateway runtime with the same stable Gateway ID at
ExecutionSlice and verifies that a new lease epoch is still required. Every case reads the pinned
Result Artifact directly from the storage Shard, verifies its manifest length and BLAKE3 digest,
and requires byte identity with an uninterrupted baseline. A narrower local diagnosis may set
`DTGPROXY_CERT_ALGORITHM` and/or `DTGPROXY_CERT_FAULT_POINT`.

`Publish` is a distinct recovery boundary. If the first Gateway uploads and pins a Result but stops
before Meta accepts the Result Manifest CAS, the replacement Gateway may reuse that pinned
generation only when the persisted chunk count, total byte count, and BLAKE3 content digest all
match its deterministic `DTAR` output exactly. A saturated bounded generation scan without such a
match fails closed; it never assumes absence and never creates a second visible Result generation.

Process-level recovery has a separate Gateway certification. It kills a real Gateway process only
after an asynchronous Degree job reaches `RUNNING`, restarts the same stable node ID, and requires
one pinned Result generation. A three-node Meta quorum case then stops the current Meta Leader
during an in-flight job and requires the surviving majority to elect a replacement without a
duplicate result. Status polling must use a fresh request ID for every observation; reusing one ID
correctly replays the first idempotent response and is not a valid state-progress check.

This runbook covers the independently deployed Meta, Data, and Controller path. Backend migration
is a Catalog-owned workflow; do not invoke a local Adapter cutover on an individual Data node.

## Workflow and invariants

The Controller reconciles this durable state machine:

```text
Preparing -> Restored -> DualApplying -> Verified -> Committing
          -> CutOver -> Published -> SourceRetired

Preparing/Restored/DualApplying/Verified -> Aborting -> Aborted
```

`Restored`, `DualApplying`, `Verified`, `CutOver`, and `SourceRetired` carry one receipt per
`(shard, voter)`. A restored receipt binds that physical replica to the digest of its resolved Data
backend profile. This digest is intentionally different from the logical Catalog profile digest:
the Controller derives a unique instance and path for every Shard, and may resolve a logical
PostgreSQL/Neo4j target through a local Sidecar. Catalog accepts only current voters, requires one
digest per Shard, and rejects applied-index regression between receipt phases.

The target generation is always `source_generation + 1`. Data restores a logical snapshot at an
applied-index fence, Raft commits the start of dual apply, and verification waits until every target
is at least as current as its source replica. Meta then CAS-commits the non-abortable `Committing`
fence before any physical cutover RPC; Raft commits cutover before Meta atomically publishes the
graph's new Backend Profile. Controller and Data restarts replay these steps idempotently.

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
target is rejected. Start validates the provider and resolves an endpoint for every Shard before
persisting the workflow.

```bash
cargo run -p controller --bin dtgproxy-admin -- \
  backend-migrate controller.json 019f0000000000000000000000000001 7 target.json

cargo run -p controller --bin dtgproxy-admin -- \
  backend-status controller.json 019f0000000000000000000000000001
```

Before `Committing`, request a durable rollback with:

```bash
cargo run -p controller --bin dtgproxy-admin -- \
  backend-abort controller.json 019f0000000000000000000000000001
```

Rollback at or after the commit fence is rejected. The abort command derives the prepared target
digest from Catalog and does not require the target backend to be reachable. After the fence, finish
reconciliation and, if needed, start a new forward migration to another generation.

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
   Sidecar. Current Sidecar selection is process-local; automatic selector persistence is a
   boundary item, even though PostgreSQL/Neo4j data and applied indexes themselves are durable.

For an in-flight analytics Job, a DataNode, Shard Leader, backend, or Sidecar outage is an
infrastructure-unavailable condition and must not be persisted as terminal `FAILED`. Keep Meta and
the unaffected cluster members alive, restart with the same durable directory,
node/shard/placement/backend identity and advertised address, and allow the current Gateway lease to
recover or expire into a higher-epoch takeover by another Gateway.
After recovery, compare the pinned Result Artifact digest and bytes with the uninterrupted baseline
and verify that exactly one Result generation is pinned.

For analytics Artifact reclamation, never treat absence from the active Meta Job list as deletion
proof. Prune a terminal Job first so Meta durably creates its Job tombstone. The current GC lease
owner may then advance the Shard fence and delete TTL-eligible generations. Reclamation is
acknowledged only on a later complete Shard-wide head scan that observes no generation for that
tombstone. Unknown Jobs and active non-terminal Jobs remain fail-closed, and an unacknowledged
tombstone must survive Meta or Gateway restart and must not be compacted.

Current certification level: this contract is implemented end-to-end and integration-certified for
RocksDB DataNode restart in PrimaryReplica and Shared-Nothing. Sidecar/backend restart is
real-backend certified for RocksDB, PostgreSQL 17 and Neo4j 5.26 Community in both modes, both with
the owning Gateway kept alive and with a distinct Gateway taking over after lease fencing. Every
successful case produces baseline-identical canonical `DTAR` bytes and exactly one pinned Result
generation. Ordered full-stack restart is real-backend certified for all three backends and both
modes: Meta and every DataNode/Shard/backend restart from fixed durable identities, the first
Gateway is destroyed, and a second Gateway completes the Job with baseline-identical bytes, one
pinned Result, and no ghost generation. PrimaryReplica uses two voters with independent backend
Sidecars; both members restart and the test waits for leader/follower applied-index convergence.
Isolated PostgreSQL/Neo4j DataNode-only restart remains a separate certification item.

Tombstone GC is also real-backend certified for all three backends and both modes at the
`after-fence`, `before-delete`, and `before-acknowledgement` crash boundaries. Each case starts two
replacement Gateways concurrently; Meta lease fencing permits exactly one successful deletion,
the later complete empty scan acknowledges reclamation, and Unknown/non-terminal Job Artifacts
remain protected. The matrix requires an observed Meta `ResourceExhausted` lease conflict, not just
an inferred single delete. Long scans renew the same owner epoch near expiry, and fence, delete and
acknowledgement each force a fresh Meta lease confirmation before proceeding.
