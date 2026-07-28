# DTGProxy Clean-Break Layered Architecture Design

Status: approved

Date: 2026-07-28

## 1. Decision

DTGProxy will be rebuilt around a deliberately small kernel and three logical business layers:
language, execution, and storage. Gateway, Data, Meta, and Controller remain the four deployable
processes, but become thin composition roots rather than alternate homes for business logic.

This is a clean break, not an incremental compatibility refactor:

- the new dependency tree must not depend on any old runtime crate;
- old disk formats, Raft logs, snapshots, internal RPCs, IR payloads, and client compatibility
  bridges are not accepted by the new runtime;
- old data may move only through a one-shot offline logical export/import tool that is not linked
  into the new runtime;
- RocksDB, the old Adapter SPI and registry, official-backend Sidecars, old IR and executors,
  arbitrary user procedures, and their transitive compatibility code are removed before release.

The selected implementation strategy is a contract-first clean-room dependency tree. Existing
algorithms may be ported only after their behavior is covered by tests; their old public types and
dependency direction are not preserved.

## 2. Architectural invariants

The following invariants are release-blocking.

1. Dependencies point downward only:

   - process composition depends on execution and concrete storage providers;
   - execution depends on language IR, storage contracts, and kernel;
   - language and storage depend only on kernel;
   - kernel depends on no DTGProxy business crate.

2. Language ends at normalized logical IR. It contains no physical placement, Raft, transaction
   coordinator, storage capability, backend, network, or execution operator type.

3. Execution owns all distributed and temporal behavior: planning, query execution, temporal
   snapshot isolation, transactions, Shard/Raft, consistent reads, replica snapshots, Snapshot
   CSR, built-in graph algorithms, asynchronous T-Cypher analytics, control-plane state machines,
   and generational backend migration.

4. Storage owns physical persistence and exposes typed logical operations. It never parses
   T-Cypher, chooses distributed plans, decides transaction conflicts, owns routing, or implements
   Raft policy.

5. Fjall, PostgreSQL, and Neo4j are official in-process providers. No official provider requires a
   Sidecar or the third-party remote protocol.

6. A third-party backend is reachable only through the versioned remote storage protocol. That
   protocol exposes the same logical contract and bounded pushdown surface, not Rust traits or an
   arbitrary plugin ABI.

7. A Shard placement epoch has exactly one active backend generation. Migration may stage a
   candidate and retain a retiring generation, but reads and routing always name one active pair.

8. Every replica owns an exclusive namespace. Backend installations may be shared, but namespaces
   may not overlap or be opened by another replica.

9. Every voter in one Raft group has the same active backend class, contract version, layout
   version, and capability floor. Endpoints and credentials are replica-local.

10. A Data Node may host any number of assigned Shards with different backend classes. There is no
    node-global selected backend.

11. Arbitrary user code is impossible: no dynamic procedure registry, user-defined native
    library, WASM runtime, scripting runtime, or remote procedure provider exists.

## 3. Target dependency graph

~~~mermaid
flowchart TB
    subgraph Processes
        GW["Gateway process"]
        DN["Data process"]
        MN["Meta process"]
        CT["Controller process"]
    end

    subgraph Execution
        EF["execution facade"]
        PL["planning"]
        QE["query runtime"]
        TX["temporal transactions"]
        SH["Shard / Raft / reads"]
        AN["Snapshot CSR / algorithms / analytics"]
        CP["control plane / migration"]
    end

    subgraph Language
        LA["syntax + semantics + normalization"]
        IR["normalized logical IR"]
    end

    subgraph Storage
        SC["logical storage contracts"]
        SF["Fjall provider"]
        SP["PostgreSQL provider"]
        SN["Neo4j provider"]
        SR["third-party remote client"]
        RP["remote storage protocol"]
    end

    K["minimal kernel"]

    GW --> EF
    DN --> EF
    MN --> EF
    CT --> EF
    DN --> SF
    DN --> SP
    DN --> SN
    DN --> SR

    EF --> PL
    EF --> QE
    EF --> TX
    EF --> SH
    EF --> AN
    EF --> CP

    PL --> IR
    PL --> SC
    QE --> SC
    TX --> SC
    SH --> SC
    AN --> SC
    CP --> SC

    LA --> IR
    IR --> K
    SC --> K
    SF --> SC
    SP --> SC
    SN --> SC
    SR --> SC
    SR --> RP
    LA --> K
    EF --> K
    SF --> K
    SP --> K
    SN --> K
    RP --> K
~~~

There is no network hop between the three logical layers. They are library boundaries linked into
the four existing process roles. Network boundaries exist only between processes and between a
Data process and an explicitly configured third-party remote backend.

## 4. Target workspace structure

The names below are architectural names. The implementation plan may split large packages further,
but it must preserve these ownership boundaries.

~~~text
crates/
  kernel/
    dtg-kernel

  language/
    dtg-language-ir
    dtg-language

  execution/
    dtg-plan
    dtg-query
    dtg-transaction
    dtg-shard
    dtg-analytics
    dtg-control
    dtg-execution
    dtg-cluster-protocol

  storage/
    dtg-storage
    dtg-storage-fjall
    dtg-storage-postgres
    dtg-storage-neo4j
    dtg-storage-remote-protocol
    dtg-storage-remote

  processes/
    dtg-gateway
    dtg-data
    dtg-meta
    dtg-controller
~~~

The facade crates dtg-language and dtg-execution expose intentional stable surfaces. Process crates
must not reach around a facade into another layer's internal implementation crate. Architecture
tests inspect Cargo metadata and reject forbidden edges.

## 5. Minimal kernel

The kernel contains only stable, context-free data primitives shared by two or more layers:

- strongly typed cluster, graph, Shard, replica, placement epoch, backend generation, transaction,
  snapshot, request, and analytics job identifiers;
- valid-time intervals, transaction timestamps, hybrid logical time values where required, and
  checked interval algebra;
- backend-neutral scalar and property values;
- bounded sizes, digests, version identifiers, cancellation reason values, and deterministic
  ordering helpers;
- small error-class and retry-class enums used at protocol boundaries.

The kernel contains no async runtime, I/O, configuration loader, service, logging setup, RPC,
Raft, storage trait, query operator, logical plan, backend capability, provider code, or process
role. It may use only a reviewed allowlist of small data libraries. Tokio, Tonic, Raft libraries,
database drivers, and provider SDKs are forbidden.

Encoding is owned by the protocol or storage package that needs it. The kernel does not become a
dumping ground for wire formats.

## 6. Language layer

### 6.1 Responsibilities

The language layer owns:

- T-Cypher lexing, parsing, AST, name binding, type checking, and semantic diagnostics;
- temporal syntax such as current, AS OF, CHANGES, valid-time constraints, and transaction-time
  scopes;
- normalization of equivalent syntax into one canonical logical form;
- validation of read/write statement shape and statically known built-in analysis invocations;
- production of a versioned normalized logical program.

Its only cross-layer output is dtg-language-ir. AST and semantic-analysis implementation types are
private to dtg-language.

### 6.2 Normalized logical IR

The new IR is a fresh major version and is not wire-compatible with temporal-ir. It represents:

- graph scope and parameters;
- typed scans, point lookups, expansions, filters, projections, joins, aggregation, ordering,
  limits, and writes;
- explicit temporal read scope and valid-time predicates;
- transaction statement boundaries;
- a closed set of built-in graph and temporal analytics operations;
- an explicit asynchronous analytics submission statement and result schema.

It does not contain:

- physical operators, fragments, exchanges, placement, Shard IDs, replica choices, memory budgets,
  backend capabilities, raw backend predicates, Raft metadata, or RPC envelopes;
- procedure-provider identifiers, dynamic procedure names, arbitrary code references, or
  compatibility nodes for the old IR.

Unknown built-in analysis names fail semantic analysis. There is no runtime fallback to a procedure
registry.

## 7. Execution layer

### 7.1 Planning

Planning consumes normalized logical IR plus an immutable catalog and capability snapshot. It
performs logical optimization, Shard routing, physical operator selection, distributed
fragmentation, exchange planning, read-mode selection, resource budgeting, and bounded storage
pushdown.

Physical plans and worker fragments are execution-owned types. A fragment sent to a Data process
uses dtg-cluster-protocol and never serializes language AST or exposes a backend-native query.

The planner records the placement epoch, backend generation, capability digest, schema version, and
snapshot requirements used to create the plan. Execution rejects drift instead of silently
replanning under different semantics.

### 7.2 Query runtime

The query runtime is a bounded columnar pipeline with:

- vectorized record batches and explicit schemas;
- cancellation, deadline, memory, row, scan, network, and spill budgets;
- deterministic merge ordering and duplicate rules;
- residual evaluation for every non-exact pushdown;
- graph overlay support for read-your-own-writes;
- explicit retry boundaries above side-effect-free fragments.

Backend results are not trusted to satisfy semantics beyond the exact guarantees in the pinned
capability manifest.

### 7.3 Temporal snapshot isolation

The execution layer owns Temporal Snapshot Isolation.

- A transaction receives a durable start timestamp from Meta.
- Its read snapshot pins catalog version, placement epochs, backend generations, and per-Shard read
  fences.
- Reads observe one committed prefix plus the transaction's local overlay.
- Write conflicts are checked on entity and valid-time interval overlap against commits after the
  start timestamp.
- Disjoint valid-time corrections may commit concurrently.
- Referential and identity constraints are evaluated from the complete staged transaction.
- Storage providers apply deterministic committed mutations but never decide conflicts.

Single-Shard transactions commit through one replicated command. Multi-Shard transactions use
epoch-fenced participant intents, a durable Home-Shard decision, idempotent finalization, and
recovery of unresolved outcomes. A timeout never exposes a partial commit.

This design does not claim predicate-level serializability unless a later, separately approved
design adds predicate locking or SSI.

### 7.4 Shard and Raft

Each Shard is one independent Raft group. The replicated state machine owns deterministic command
validation, transaction intents and decisions, logical mutation production, applied indexes,
closed timestamps, snapshot metadata, backend migration state, and lineage.

Consensus durability is separate from the temporal graph provider contract. All Data and Meta Raft
WAL, hard state, membership state, and local snapshot bookkeeping use a Fjall-based consensus store
owned by the storage layer. PostgreSQL or Neo4j graph Shards still keep their consensus state in
their replica's exclusive local Fjall system namespace.

This separation prevents a remote graph backend outage from changing Raft's persistence semantics
and removes RocksDB without forcing consensus internals into PostgreSQL or Neo4j.

### 7.5 Consistent reads

Every read carries graph, Shard, placement epoch, backend generation, and requested read mode.

- Linearizable reads run on the leader after a quorum ReadIndex barrier and wait for local apply.
- Snapshot reads use a transaction snapshot token and a provider read view pinned to one applied
  prefix.
- Follower reads require a signed execution proof containing leader term, placement epoch, backend
  generation, applied index, and closed timestamp. All fields must still match local state.
- Historical time alone does not bypass epoch, generation, or safe-time validation.

Errors distinguish not leader, stale placement, stale generation, capability drift, snapshot too
old, unsafe follower read, deadline, overload, and terminal semantic failure.

### 7.6 Replica snapshots versus Snapshot CSR

Two snapshot concepts remain deliberately separate:

- Replica Snapshot is a versioned logical state-machine snapshot used for Raft recovery, Shard
  movement, backend migration, and disaster recovery.
- Snapshot CSR is an immutable graph projection at one transaction snapshot used by built-in graph
  algorithms.

Replica snapshots stream typed logical records and manifests. They are independent of every
provider's physical layout. Snapshot CSR uses stable vertex ordering, forward and optional reverse
offset arrays, typed edge/property columns, snapshot identity, partition provenance, and a content
digest. It may be partitioned and assembled incrementally under explicit memory and spill limits.

### 7.7 Built-in graph algorithms

Algorithms are compiled into dtg-analytics and selected by a closed BuiltInAlgorithmId. The initial
catalog retains the currently supported validated algorithms, including traversal, shortest path,
components, centrality, PageRank, community, triangle, k-core, and temporal path/change algorithms.

Each algorithm declares:

- required Snapshot CSR shape and property columns;
- deterministic ordering and tie-breaking;
- memory and iteration limits;
- cancellation points;
- whether distributed partial aggregation is supported;
- result and checkpoint schema versions.

No arbitrary user algorithm or procedure can be registered at runtime.

### 7.8 Asynchronous T-Cypher analytics

T-Cypher can submit only statically known built-in analytics as durable asynchronous jobs.

1. Gateway compiles and plans the statement, obtains a snapshot token, and submits a versioned job
   specification to Meta.
2. Meta Raft owns the job ledger, state transitions, leases, retries, cancellation, retention pins,
   and tombstones.
3. An execution scheduler claims the job, asks Data Shards for snapshot projection parts, and runs
   the built-in algorithm.
4. Checkpoints and results use versioned artifacts with generation numbers, digests, and retention
   ownership.
5. Gateway exposes status and result retrieval through T-Cypher/Bolt surfaces.

Process-local schedulers are composition details. Job semantics and state machines remain in the
execution layer.

### 7.9 Control plane

Meta is the authority for catalog state: graphs, Shards, replica sets, backend classes, replica
bindings, placement epochs, backend generations, namespace ownership, capability digests,
migration records, TSO state, analytics jobs, and leases.

Controller watches desired and observed state and executes idempotent reconciliation. It owns no
authoritative mutable state outside Meta. Data reports observed replica state; Gateway only caches
versioned catalog snapshots.

Shard movement and backend migration are separate workflows sharing the same fencing and lineage
model. A workflow may not change membership and backend generation in one unreviewed compound
transition.

## 8. Storage layer

### 8.1 Contract family

The old StorageAdapter and TemporalBackendMapping traits are replaced by a cohesive contract
family, not another universal god trait:

- ReplicaStateStore atomically applies one committed Shard batch and exposes the durable applied
  index;
- TemporalReadView provides point, temporal history, adjacency, changes, and bounded logical scans
  against one immutable prefix;
- LogicalSnapshotSource and LogicalSnapshotSink stream provider-independent replica snapshots;
- PushdownExecutor accepts only versioned bounded pushdown requests;
- ConsensusStore persists Raft WAL, hard state, membership metadata, and local snapshot metadata;
- ArtifactStore persists versioned analytics checkpoint and result chunks where configured.

The business contract uses typed logical identities, versions, intervals, properties, adjacency,
transaction records, and replica metadata. It exposes no canonical key bytes, column-family names,
SQL, Cypher text, or provider transaction handles.

Apply requests include the full binding identity, Raft term and index, command identifier, mutation
digest, and typed mutation batch. Providers must provide atomicity, monotonic applied index,
idempotent replay, durability before acknowledgement, and fail-closed binding validation.

### 8.2 Capability-controlled pushdown

A backend generation publishes an immutable capability manifest and digest at activation.
Capabilities are granular and semantic, for example:

- point and batched entity reads;
- valid-time and transaction-time range reads;
- directed adjacency expansion;
- CHANGES scanning;
- typed property predicates;
- ordering, limit, projection, and selected partial aggregates;
- immutable read views and logical snapshot streaming.

Each pushdown response is Exact, ResidualRequired, or Unsupported. Exact requires the provider to
meet ordering, null, temporal, duplicate, and snapshot semantics exactly. ResidualRequired names
the guarantees already provided and the execution layer evaluates the rest. Unsupported falls back
to logical reads when safe or fails planning when no bounded fallback exists.

Capabilities cannot change within a backend generation. A change requires a new generation and the
normal migration/cutover protocol.

### 8.3 Fjall physical organization

Fjall is the embedded official provider and the only local consensus-store implementation.
Each replica owns a directory and namespace marker containing its complete binding identity.

The temporal graph provider uses distinct Fjall keyspaces for logical record families, with
provider-private ordered encodings and checksums. It may optimize Current, History, adjacency,
temporal indexes, transactions, metadata, snapshot staging, artifacts, and consensus data
independently. These encodings are not part of the logical storage contract and are not shared with
other providers.

Opening a namespace with a mismatched identity, epoch, generation, or layout version fails before
serving.

### 8.4 PostgreSQL physical organization

PostgreSQL uses a replica-exclusive schema or database namespace with typed relational tables:
identity, current vertex/edge projections, version/history rows, directed adjacency, temporal
indexes, transaction state, replica metadata, snapshot staging, and optional artifacts.

Intervals use native range-compatible columns and indexes where they preserve DTGProxy semantics.
An applied Shard batch runs in one database transaction with an ownership-row fence and monotonic
Raft-index compare-and-set. Read views use a dedicated read-only repeatable snapshot. Provider SQL
is private implementation detail; there is no canonical-KV mirror table.

### 8.5 Neo4j physical organization

Neo4j uses native nodes, relationships, labels, properties, indexes, and explicit version/history
entities inside a replica-exclusive database or ownership-fenced namespace. Current graph
traversal uses native relationships; historical state and transaction metadata use versioned
entities with deterministic identifiers.

One Shard apply is one Neo4j transaction guarded by the namespace ownership marker and monotonic
applied index. Read views use one explicit backend transaction. There is no canonical-KV mirror and
no required Sidecar.

When a Neo4j edition cannot provide physical multi-database isolation, a shared database is allowed
only with an enforceable namespace discriminator on every entity, namespace-scoped constraints and
indexes, and contract tests proving that cross-namespace access is impossible.

### 8.6 Third-party remote protocol

dtg-storage-remote-protocol defines a versioned service independent of the internal cluster RPC.
It includes:

- protocol and logical-contract version negotiation;
- binding identity and namespace ownership handshake;
- immutable capability manifest and digest;
- atomic apply with idempotency key, Raft index, digest, deadline, and typed errors;
- begin/read/end immutable read sessions;
- bounded point, history, adjacency, changes, scan, and pushdown operations;
- logical snapshot export/import streams;
- health, readiness, draining, and observed applied-index reporting;
- bounded messages, checksums, flow control, authentication context, and request IDs.

Major-version mismatch fails closed. Minor versions add only optional fields or capabilities.
Official providers do not traverse this protocol. The old Sidecar protocol and processes are
deleted rather than adapted.

## 9. Binding, homogeneity, and namespace model

A replica binding contains:

~~~text
cluster_id
graph_id
shard_id
placement_epoch
replica_id
backend_generation
backend_class_digest
provider_kind
contract_version
layout_version
capability_digest
namespace_id
endpoint_profile_ref
credential_ref
role = candidate | active | retiring
~~~

The namespace identity is the complete tuple through backend_generation, not merely Shard ID.
Every physical provider stores an ownership marker and checks it on every open, apply, snapshot,
and destructive lifecycle operation.

BackendClassDigest excludes endpoint and credential material but includes provider kind, logical
contract version, layout version, durability policy, and required capability floor. All voters and
learners of one generation must match it. Replica-local endpoint profiles may differ.

The Data process maintains a ShardHost map keyed by Shard and replica identity. Each entry owns its
Raft runtime, consensus namespace, active graph-store binding, optional migration candidate, read
views, and lifecycle state. Adding a PostgreSQL Shard to a node already hosting Fjall and Neo4j
Shards does not restart or reconfigure the other entries.

This is the heterogeneous Data Node contract: heterogeneity is allowed between independent Shards
on one Data Node, while every individual Raft replica group remains homogeneous within each backend
generation.

## 10. Generational online backend migration

Migration is a durable control-plane and Shard state machine. Generation numbers never decrease or
get reused.

~~~mermaid
stateDiagram-v2
    [*] --> Stable
    Stable --> Allocating: create migration intent
    Allocating --> Backfilling: all candidate namespaces fenced
    Backfilling --> Mirroring: snapshot imported and WAL tail caught up
    Mirroring --> Prepared: all replicas prove equivalence
    Prepared --> Activating: reserve next placement epoch
    Activating --> Grace: new generation active
    Grace --> Stable: retire and delete old generation
    Allocating --> Aborted
    Backfilling --> Aborted
    Mirroring --> Aborted
    Prepared --> Aborted
    Grace --> Activating: rollback as a new generation
    Aborted --> Stable
~~~

The protocol is:

1. Meta records a migration ID, source generation, target backend class, and target generation.
   It validates that every replica can allocate the same target class.
2. Every replica creates an exclusive candidate namespace and persists an ownership marker.
3. A source logical snapshot at Raft index L is imported into each target. Commands after L are
   replayed, then every new committed command is durably mirrored to source and target.
4. A replica becomes prepared only after applied-index equality and provider-independent logical
   snapshot digest equivalence. Every voter must be prepared.
5. Controller reserves placement epoch E+1 in Meta. The Shard Raft group commits activation of
   target generation G+1 under E+1 and rejects E. Meta then publishes E+1/G+1. The short interval
   between Shard activation and catalog publication is retryable unavailability, not split-brain
   service.
6. Target serves reads. Both generations continue durable mirroring during a grace period.
7. Success seals the old namespace, releases pins, records lineage, and deletes it through a
   separately fenced lifecycle command.
8. A rollback activates an up-to-date old physical namespace as a new monotonic generation under
   another placement epoch. It never reuses the former identity.

Before activation, abort deletes only the candidate after verifying its exact binding identity.
After activation, failures resume from durable phase records. No process-local flag can complete or
skip a phase.

At every instant there is exactly one active generation. During migration, both source and target
classes are group-homogeneous within their own generation.

## 11. Thin process composition

### Gateway

Gateway owns listeners, Bolt session adaptation, authentication context, configuration, telemetry,
catalog-cache plumbing, and dependency injection. It invokes language and execution facades.
Compilation, planning, distributed coordination, transaction semantics, and analytics scheduling
are not implemented in the process crate.

### Data

Data owns listeners, node identity, transport, provider construction, secret resolution,
telemetry, and the ShardHost lifecycle. All Raft, query worker, read fencing, transaction
participant, snapshot, and migration semantics come from execution packages.

Data startup declares available provider drivers and named endpoint profiles. It does not select
one backend for the whole node.

### Meta

Meta owns listeners, bootstrap configuration, transport, and the Meta Raft host. Catalog, TSO,
leases, migration records, and analytics ledger state machines come from execution packages.

### Controller

Controller owns listeners if any, configuration, credentials, telemetry, and the reconciliation
loop host. Reconciliation rules and transition validation come from dtg-control.

The target is that process crates contain no domain data model duplicated from a layer and no
business state machine.

## 12. Protocol and version policy

The internal cluster protocol receives a new incompatible major version. It carries:

- common request identity, cluster identity, deadline, tracing context, and protocol version;
- graph, Shard, placement epoch, backend generation, and catalog version fences;
- execution fragments, column batches, transaction requests, Raft transport, snapshots, control
  observations, and typed errors;
- bounded lengths and checksums before allocation.

Old cluster messages are not decoded. Rolling upgrade from the old architecture is unsupported.
Rolling upgrades within the new architecture are supported only inside a documented compatible
minor-version window and never across a storage layout or logical-contract major change.

Provider physical layout versions, logical snapshot versions, execution fragment versions,
analytics artifact versions, and remote storage protocol versions are independent and explicit.

## 13. Failure and safety rules

- Every mutation is fenced by placement epoch, backend generation, namespace ownership, Raft term,
  and committed index where applicable.
- A provider may not acknowledge before durable commit.
- Unknown command, record, capability, protocol, or layout versions fail closed.
- Capability drift invalidates planning and read views.
- Partial snapshot imports remain invisible and are garbage-collected by generation.
- A migration target failure cannot corrupt or advance the source authority.
- A process crash cannot create two active bindings because activation authority is durable.
- Destructive namespace cleanup requires an exact identity match, a sealed state, expired retention
  pins, and an idempotent tombstone.
- Retryable errors never include ambiguous successful writes without an idempotency identity.
- Logs and metrics identify Shard, placement epoch, replica, backend generation, provider, and
  migration ID without exposing credentials.

## 14. Testing and verification

### 14.1 Architecture gates

- Cargo metadata proves the allowed dependency DAG and rejects all reverse edges.
- Kernel dependency and source scans enforce its allowlist and forbidden concepts.
- Process-crate source checks reject domain state machines and direct backend queries.
- New runtime crates have no dependency on old runtime crates.

### 14.2 Contract and semantic tests

- One shared logical storage TCK runs against Fjall, PostgreSQL, Neo4j, and the remote reference
  server.
- Provider-native layout tests prove no canonical-KV mirror is required.
- Namespace isolation tests attempt cross-replica reads, writes, snapshots, and deletion.
- Capability tests verify Exact and ResidualRequired semantics against execution evaluation.
- A semantic model and property tests cover temporal rewrites, history, adjacency, conflicts,
  constraints, and transaction recovery.

### 14.3 Distributed tests

- Deterministic Raft command and recovery tests.
- Leader ReadIndex, follower safe-time, stale epoch, stale generation, and capability-drift tests.
- Single- and multi-Shard transaction crash matrices.
- Replica snapshot create/install/recovery tests.
- Snapshot CSR equivalence and algorithm oracle tests.
- Durable asynchronous analytics lease, retry, cancellation, checkpoint, result, GC, and tombstone
  tests.

### 14.4 Migration tests

All directed provider pairs are exercised:

~~~text
Fjall -> PostgreSQL
Fjall -> Neo4j
PostgreSQL -> Fjall
PostgreSQL -> Neo4j
Neo4j -> Fjall
Neo4j -> PostgreSQL
~~~

Every phase has crash-before, crash-after, retry, abort, stale-controller, stale-Data-node,
namespace-collision, digest-mismatch, capability-mismatch, and rollback coverage. Tests prove one
active generation, group homogeneity, monotonic lineage, and no lost committed mutation.

### 14.5 Process and live-backend certification

- Four-process cluster tests cover mixed Shards on one Data Node and homogeneous replicas across
  multiple Data Nodes.
- Live PostgreSQL and Neo4j CI is mandatory, not environment-gated evidence for release.
- Remote protocol conformance runs against a reference third-party server.
- Performance and soak tests cover query budgets, backpressure, WAL, snapshots, algorithms,
  migration mirroring, failover, and restart.

## 15. Clean-break deletion gate

Before the architecture is considered implemented:

- Cargo workspace contains none of the old adapter, registry, Sidecar, temporal-ir, physical-plan,
  query-executor, query-optimizer, distributed-query, procedure-runtime, cypher-engine,
  temporal-storage, or RocksDB log-store packages;
- Cargo.lock contains no RocksDB crate or native RocksDB binding;
- production source and manifests contain no old Adapter SPI, Mapping SPI, Sidecar process,
  compatibility bridge, old IR decoder, old executor, or dynamic procedure provider;
- official PostgreSQL and Neo4j paths open their native providers directly;
- README, deployment docs, examples, scripts, benchmarks, and CI describe only the new
  architecture;
- obsolete tests and fixtures are deleted rather than disabled;
- any retained one-shot legacy export tool is built outside the runtime workspace and has no
  dependency edge into a process or new layer crate.

Historical architecture specifications may describe removed names for audit purposes, but they are
not normative and are excluded from runtime source gates.

## 16. Implementation decomposition

The architecture is one approved design but too large for one undifferentiated code change. The
implementation plan must use dependency-ordered gates:

1. architecture enforcement and clean-room skeleton;
2. kernel and new language IR/compiler;
3. logical storage contracts and Fjall provider, including Fjall consensus store;
4. Shard/Raft, read fencing, replica snapshot, and temporal transaction execution;
5. planning and query runtime;
6. PostgreSQL and Neo4j native providers;
7. Snapshot CSR, built-in algorithms, and asynchronous T-Cypher analytics;
8. Meta control plane, heterogeneous ShardHost, and generational migration;
9. thin four-process cutover and new protocols;
10. full certification, old architecture deletion, documentation, and release audit.

Each gate must leave the new dependency tree internally coherent. Temporary coexistence with old
code is allowed only during development; no new package may import an old package, and the final
gate deletes the entire old dependency closure.

## 17. Acceptance criteria

The design is complete only when evidence proves all of the following:

1. The dependency DAG contains minimal kernel plus language, execution, and storage layers with
   four thin process roots.
2. Language output stops at normalized logical IR.
3. Execution owns every capability named in this design.
4. Storage exposes typed logical contracts with controlled pushdown.
5. Fjall, PostgreSQL, and Neo4j use native physical layouts and official paths are in-process.
6. Third-party storage works through the versioned remote protocol.
7. RocksDB and every named legacy runtime surface are absent.
8. Backend binding, epoch fencing, group homogeneity, exclusive namespaces, and heterogeneous
   Data-node hosting pass distributed tests.
9. All six backend migration directions pass crash and equivalence certification.
10. Old architecture code, docs, scripts, tests, fixtures, and dependencies are removed or replaced.

No narrower prototype, compatibility wrapper, disabled test, or source scan without behavioral
coverage is sufficient evidence.
