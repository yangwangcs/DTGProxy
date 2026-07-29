# Storage providers

`dtg-storage` defines the unified logical replica contract, immutable read views, atomic idempotent
apply, capability manifests, logical snapshots, activation receipts, and replica binding fences.

Official providers are linked and opened in process:

- `dtg-storage-fjall` uses isolated keyspaces and native ordered logical records.
- `dtg-storage-postgres` uses native relational tables, serializable activation, and indexed temporal
  access paths.
- `dtg-storage-neo4j` uses native graph labels, relationships, constraints, and snapshot markers.

Third-party providers implement `dtg-storage-remote-protocol`. The protocol is versioned, bounded,
authenticated, checksum protected, idempotent under lost-response retry, and fail-closed on unknown
versions or malformed payloads.

A provider may execute a pushdown only when its capability manifest satisfies the planner's exact
requirement. The execution layer retains residual predicates and validates applied index, temporal
scope, placement epoch, generation, and capability digest on every read.
