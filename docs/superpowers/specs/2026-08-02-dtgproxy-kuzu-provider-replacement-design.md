# DTGProxy Kuzu Native Graph Provider Replacement Design

**Status:** approved by user on 2026-08-02

## Goal

Replace Neo4j with Kuzu as DTGProxy's official native graph provider. The resulting official
provider set is Fjall for key-value-oriented access, PostgreSQL for relational access, and Kuzu
for native graph access. Kuzu must run in-process with one local database directory per Data
process namespace; no Docker image, network endpoint, or provider credential is required.

## Scope

This is a clean break. `dtg-storage-neo4j`, its process configuration, Docker launchers, CI job,
certification checks, migration-matrix entries, examples, and paper-facing provider references are
removed. `dtg-storage-kuzu` replaces them as the only official graph provider.

The change does not add raw text, image, audio, video, embedding, or LLM features. DTGProxy is
positioned as a distributed **multi-model** temporal graph database: it unifies key-value,
relational, and graph backends under one temporal graph contract.

## Architecture

### Provider boundary

`dtg-storage-kuzu` implements the existing provider-neutral storage interfaces:

- `ReplicaStateStore` for idempotent committed-batch application and replica metadata;
- `TemporalReadView` for bounded vertex, edge, history, change, and adjacency reads;
- logical snapshot source and sink interfaces for provider-independent migration; and
- `PushdownExecutor` for capabilities that have an exact declared guarantee.

The provider does not parse T-Cypher, decide distributed plans, assign timestamps, coordinate
transactions, own Raft, or construct Snapshot Fences. Those remain in the language and execution
layers. A Kuzu response is accepted only after the existing binding, generation, applied-index,
and capability checks pass.

### Local ownership and layout

`KuzuResolver` opens one database at
`DTG_DATA_KUZU_ROOT/<namespace-id>` for each assigned replica. Placement guarantees a single
active owner for that namespace. A Kuzu database contains the provider's private physical layout
for current vertex and edge records, immutable version history, adjacency access paths,
transaction metadata, and the durable applied Raft index.

Logical identifiers and property values use the existing DTG canonical encoding at the Kuzu
boundary. Kuzu-specific identifiers and property types must never appear in the language IR,
logical snapshot format, or cross-provider protocol.

### Writes, reads, and migration

Applying a committed shard batch uses one Kuzu write transaction. The provider writes all logical
effects and the corresponding applied index atomically; a failed transaction leaves neither
business data nor a newer applied index visible. Reads begin from a `ReadFence` and fail closed on
binding, capability, or applied-prefix mismatch.

Kuzu performs native node/edge candidate reads and bounded one-hop adjacency expansion through
the existing `TemporalReadView::expand` contract. The execution layer retains authority over
bi-temporal folding, residual predicates, cross-shard traversal, and path visibility. Kuzu's
physical graph model may change query cost but cannot change query results.

Logical snapshot export and restore continue to use `SnapshotRecord` and the existing staged
snapshot protocol. Database-file copying is prohibited. Fjall, PostgreSQL, and Kuzu therefore
continue to migrate through the same authenticated, provider-independent snapshot path.

## Configuration and public surface

`ProviderKind::Neo4j` becomes `ProviderKind::Kuzu`. Data-process configuration replaces Neo4j
endpoint and basic-credential profiles with a local Kuzu root. The Data process registers a
`KuzuResolver`; Gateway and Controller stay provider-neutral. Cluster examples advertise
`fjall`, `postgresql`, `kuzu`, and `remote` provider classes.

The public documentation and CIDR draft describe the third backend as Kuzu. They must not claim
that Neo4j remains an official, measured, or migration-certified provider.

## Verification

1. Add a Kuzu storage TCK factory and run the shared storage contract against a fresh local Kuzu
   directory.
2. Port the Neo4j provider's model, query, snapshot, typed-value, and live-contract coverage to
   Kuzu-specific tests without Docker.
3. Replace all six Fjall/PostgreSQL/Neo4j migration directions with the six Fjall/PostgreSQL/Kuzu
   directions; retain clean-break checks that reject stale Neo4j references.
4. Verify Data-process configuration creates isolated Kuzu namespaces and refuses incompatible
   bindings.
5. Run the focused workspace tests, clean-break architecture checks, and provider migration
   certification before declaring the replacement complete.

## Non-goals

- exposing Kuzu's native query language to clients;
- delegating T-Cypher semantics to Kuzu;
- sharing one Kuzu database across Data processes;
- preserving Neo4j as a compatibility or remote provider; and
- changing the Snapshot Fence, Raft, or temporal transaction protocol.
