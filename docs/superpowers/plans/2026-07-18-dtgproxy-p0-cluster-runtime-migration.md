# DTGProxy P0 Cluster Runtime and Durable Migration Implementation Plan

> **Execution note:** Follow this plan sequentially with red/green tests. Keep the existing in-process runtime as a test fixture until the final remote-path equivalence gate passes.

**Goal:** Replace the prototype's in-process cluster orchestration with independently deployable Meta, Data, Gateway, and Controller processes, then make Shard/backend migration a crash-replayable control-plane workflow.

**Architecture:** Add a versioned gRPC protocol at the process boundary while retaining the synchronous deterministic Raft/temporal state machines behind actor-style command queues. Meta owns authoritative, replicated Catalog and workflow records. Data nodes host many existing `DurableShardReplica`s, expose Shard RPC, and share a transport/runtime. Gateway depends on a `ShardClient` abstraction so the tested embedded client and the production remote client run identical transaction/query logic. Controller reconciles durable desired and observed states rather than storing continuations in memory.

**Technology:** Rust 1.93, Tokio, tonic/prost gRPC over HTTP/2, rustls mTLS, raft-rs, existing DTGProxy binary codecs and Adapter SPI. Tests use Rust child processes, ephemeral loopback ports, temporary directories, deterministic failpoints, and existing semantic TCKs.

**Authoritative design:** `docs/superpowers/specs/2026-07-18-dtgproxy-production-system-design.md`

---

## Delivery rules

- Every task begins with a failing test or compile-time contract and ends with focused tests, formatting, strict Clippy, and a small commit.
- Production binaries never construct peer replicas for another node in the same process.
- RPC handlers validate cluster identity, node identity, graph/shard identity, placement epoch, request ID, deadline, and frame limits before invoking a state machine.
- Durable formats are versioned, checksummed, size-bounded, and reject trailing bytes.
- No network retry may mint a new semantic phase ID.
- No migration state can be inferred solely from process memory.
- TLS may be explicitly disabled only for loopback test/development profiles until the security program installs production CA fixtures.

## Task 1: Versioned cluster protocol and bounded transport contract

**Files:**

- Create: `crates/cluster-protocol/Cargo.toml`
- Create: `crates/cluster-protocol/build.rs`
- Create: `crates/cluster-protocol/proto/dtgproxy_cluster_v1.proto`
- Create: `crates/cluster-protocol/src/lib.rs`
- Create: `crates/cluster-protocol/tests/contracts.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Step 1: Write the failing contract tests**

Cover non-zero IDs, 16-byte request IDs, non-zero epochs, deadline validation, maximum command/chunk sizes, unknown enum values, and round-trip conversion between protobuf envelopes and validated Rust domain types.

```bash
cargo test -p cluster-protocol --test contracts
```

Expected: FAIL because the crate and validated envelopes do not exist.

**Step 2: Define only the P0 wire surface**

Add protobuf packages/services for:

- `MetaService`: `GetCatalog`, server-streaming `WatchCatalog`, `Propose`, `AllocateTimestamp`, `Heartbeat`.
- `ShardService`: `Execute`, `Read`, server-streaming `Scan`, client-streaming `InstallSnapshot`, `ReplicaStatus`.
- `NodeAdminService`: `EnsureReplica`, `ChangeMembership`, `DeleteReplica`, `GetMigrationReceipt`.

Every request envelope includes `protocol_version`, `cluster_id`, `request_id`, and absolute deadline; Shard calls additionally include graph/shard/placement epoch. Large opaque payloads are chunks with ordinal, checksum, and bounded size.

**Step 3: Generate prost/tonic code and add validated wrappers**

`cluster-protocol/src/lib.rs` must expose validation/conversion functions and stable status metadata (`not_leader`, `stale_epoch`, `revision_compacted`, `resource_exhausted`) without leaking transport-specific errors into the core.

**Step 4: Run focused and workspace checks**

```bash
cargo test -p cluster-protocol --all-targets
cargo fmt --all -- --check
cargo clippy -p cluster-protocol --all-targets --all-features -- -D warnings
```

**Step 5: Commit**

```bash
git add Cargo.toml Cargo.lock crates/cluster-protocol
git commit -m "feat: define bounded DTGProxy cluster protocol"
```

## Task 2: Durable node identity and replica manifest

**Files:**

- Create: `crates/data-node/Cargo.toml`
- Create: `crates/data-node/src/config.rs`
- Create: `crates/data-node/src/identity.rs`
- Create: `crates/data-node/src/manifest.rs`
- Create: `crates/data-node/src/lib.rs`
- Create: `crates/data-node/tests/identity_manifest.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Step 1: Write failing restart/corruption tests**

Tests must prove first boot atomically creates `(cluster_id,node_id)` identity, restart rejects a different identity, two replicas cannot share a storage directory, manifest writes survive torn trailing data, and unknown format versions fail closed.

```bash
cargo test -p data-node --test identity_manifest
```

**Step 2: Implement validated static configuration**

Model listen/advertise addresses, data directory, node identity, Meta seeds, TLS references, capacity labels, and Adapter profiles. Reject non-loopback plaintext, duplicate addresses, relative secret ambiguity, zero IDs, and reuse of a data directory by a live process.

**Step 3: Implement atomic identity/manifest persistence**

Use temp-file + `sync_all` + rename + parent-directory sync. Include magic/version/length/CRC and explicit size limits. The manifest maps `(graph_id, shard_id)` to placement epoch, replica role, backend generation, and local directories.

**Step 4: Verify and commit**

```bash
cargo test -p data-node --all-targets
cargo clippy -p data-node --all-targets --all-features -- -D warnings
git add Cargo.toml Cargo.lock crates/data-node
git commit -m "feat: persist data node identity and replica manifests"
```

## Task 3: Multi-Raft DataNode host behind a deterministic actor boundary

**Files:**

- Create: `crates/data-node/src/host.rs`
- Create: `crates/data-node/src/replica_actor.rs`
- Create: `crates/data-node/tests/multi_raft_host.rs`
- Modify: `crates/data-node/src/lib.rs`
- Modify: `crates/shard-runtime/src/durable_replica.rs`

**Step 1: Write failing host tests**

Start two Shards on one host, route commands to the correct actor, saturate one Shard queue without starving the other, restart and recover both from separate WALs, and reject an epoch mismatch before apply.

```bash
cargo test -p data-node --test multi_raft_host
```

**Step 2: Introduce the actor command/result types**

The Tokio layer owns sockets and timers. Each Shard actor serializes calls into the existing synchronous `DurableShardReplica`; it returns messages-to-send and application results. Use bounded channels and explicit overloaded/shutting-down outcomes.

**Step 3: Implement lifecycle reconciliation**

`ensure_replica` is idempotent against the durable manifest. Starting a host reconstructs all actors before readiness. Stop removes admission first and flushes WAL without deleting data.

**Step 4: Verify and commit**

```bash
cargo test -p data-node --all-targets
cargo test -p shard-runtime --all-targets
cargo clippy -p data-node -p shard-runtime --all-targets --all-features -- -D warnings
git add crates/data-node crates/shard-runtime
git commit -m "feat: host durable multi-Raft shard actors"
```

## Task 4: Shared authenticated Raft transport

**Files:**

- Create: `crates/data-node/src/raft_network.rs`
- Create: `crates/data-node/tests/raft_network.rs`
- Modify: `crates/raft-transport/src/lib.rs`
- Modify: `crates/raft-transport/tests/wire.rs`
- Modify: `crates/data-node/src/host.rs`

**Step 1: Write failing network tests**

Prove one listener multiplexes multiple Shards, connections are reused, peer identity is checked, unknown cluster/shard/node frames are rejected, per-peer queues are bounded, reconnect preserves message identity, and a slow peer cannot block other peers.

**Step 2: Separate framing from the prototype socket implementation**

Keep existing deterministic codecs but extend the frame envelope with cluster ID and explicit protocol version. Add a transport trait consumed by DataNode; retain `TcpRaftTransport` only for compatibility tests until the new shared transport passes.

**Step 3: Implement pooled Tokio transport**

Use one authenticated HTTP/2 or dedicated mTLS stream per peer, bounded per-priority queues, heartbeat/data/snapshot traffic classes, and reconnect with jitter. Never silently drop committed-path messages; surface admission failure to the actor for retry.

**Step 4: Verify and commit**

```bash
cargo test -p raft-transport -p data-node --all-targets
cargo clippy -p raft-transport -p data-node --all-targets --all-features -- -D warnings
git add crates/raft-transport crates/data-node
git commit -m "feat: multiplex authenticated multi-Raft transport"
```

## Task 5: Shard and node-admin gRPC services

**Files:**

- Create: `crates/data-node/src/service.rs`
- Create: `crates/data-node/src/bin/dtgproxy-data.rs`
- Create: `crates/data-node/tests/service.rs`
- Create: `crates/data-node/tests/process_restart.rs`
- Modify: `crates/data-node/src/lib.rs`

**Step 1: Write failing service tests**

Cover request validation, not-Leader metadata, stale epoch, duplicate request IDs, deadline cancellation, queue saturation, chunk bounds, graceful shutdown, and process restart from persisted replicas.

**Step 2: Implement service translation only**

Handlers validate envelopes, perform authorization hooks, enqueue actor commands, and translate domain errors. Business rules stay in existing state machines. InstallSnapshot writes chunks to a bounded staging file and only calls restore after checksum/fsync verification.

**Step 3: Add the production Data binary**

Parse a versioned config file, acquire directory lock, load identity/manifest, restore all actors, connect to Meta, then publish readiness. Handle SIGTERM by draining and bounded flush.

**Step 4: Verify and commit**

```bash
cargo test -p data-node --all-targets
cargo clippy -p data-node --all-targets --all-features -- -D warnings
git add crates/data-node
git commit -m "feat: serve remote DTGProxy data nodes"
```

## Task 6: Replicated Meta state machine

**Files:**

- Create: `crates/meta-node/Cargo.toml`
- Create: `crates/meta-node/src/state_machine.rs`
- Create: `crates/meta-node/src/raft_host.rs`
- Create: `crates/meta-node/src/lib.rs`
- Create: `crates/meta-node/tests/quorum.rs`
- Modify: `crates/control-plane/src/lib.rs`
- Modify: `crates/control-plane/tests/catalog.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Step 1: Write failing Meta quorum tests**

Prove only committed Catalog commands become visible, command IDs remain idempotent across Leader changes, revision never regresses, snapshots restore exactly, a minority cannot serve authoritative mutations, and watches resume from revision.

**Step 2: Make Catalog a deterministic state machine**

Split codec/state transition from the prototype local append-log owner. Preserve existing command and snapshot formats where possible; add an event sequence for watch and a compacted-revision error.

**Step 3: Host Catalog in a dedicated Raft group**

Reuse `raft-logstore`, snapshot conventions, and shared network abstractions. Meta membership is bootstrapped explicitly and is independent of graph Shards.

**Step 4: Verify and commit**

```bash
cargo test -p control-plane -p meta-node --all-targets
cargo clippy -p control-plane -p meta-node --all-targets --all-features -- -D warnings
git add Cargo.toml Cargo.lock crates/control-plane crates/meta-node
git commit -m "feat: replicate the DTGProxy Meta catalog"
```

## Task 7: Durable TSO leases and Meta service

**Files:**

- Create: `crates/meta-node/src/tso.rs`
- Create: `crates/meta-node/src/service.rs`
- Create: `crates/meta-node/src/bin/dtgproxy-meta.rs`
- Create: `crates/meta-node/tests/tso_failover.rs`
- Create: `crates/meta-node/tests/service.rs`
- Modify: `crates/timestamp-oracle/src/lib.rs`

**Step 1: Write failing lease/failover tests**

Allocate concurrently, kill the Leader after reserving but before returning, elect a replacement, and prove timestamps are unique and strictly increasing. Test clock rollback, lease exhaustion, restart, and bounded future drift.

**Step 2: Implement replicated high-water lease allocation**

Meta commits a future timestamp range before serving it locally. A new Leader begins above the committed high-water and never reuses an abandoned range. Wall clock is a lower bound, not the uniqueness source.

**Step 3: Implement Meta RPC and binary**

Expose Catalog reads/watches/proposals, timestamp allocation, heartbeats, and Leader hints. Apply the same bounded message, mTLS, readiness, and graceful shutdown rules as Data.

**Step 4: Verify and commit**

```bash
cargo test -p timestamp-oracle -p meta-node --all-targets
cargo clippy -p timestamp-oracle -p meta-node --all-targets --all-features -- -D warnings
git add crates/timestamp-oracle crates/meta-node
git commit -m "feat: serve failover-safe Meta and TSO"
```

## Task 8: ShardClient seam and embedded equivalence

**Files:**

- Create: `crates/shard-client/Cargo.toml`
- Create: `crates/shard-client/src/lib.rs`
- Create: `crates/shard-client/src/embedded.rs`
- Create: `crates/shard-client/tests/embedded_tck.rs`
- Modify: `crates/dtgproxy/src/transaction.rs`
- Modify: `crates/dtgproxy/src/lib.rs`
- Modify: `crates/query-executor/src/distributed.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Step 1: Extract a compile-failing client contract**

Define operations at the same semantic granularity already used by transaction/query code: leader command, proven read, prepare/decide, participant scan, committed stream, replica status. Include routing context and stable idempotency IDs.

**Step 2: Adapt the existing in-process runtime**

Implement `EmbeddedShardClient` over `DistributedRuntime`. Move transaction/query coordinators to the trait without changing behavior.

**Step 3: Run all old suites as an equivalence gate**

```bash
cargo test -p shard-client --all-targets
cargo test -p dtgproxy -p query-executor --all-targets
```

Every existing test must remain green before adding the remote implementation.

**Step 4: Commit**

```bash
git add Cargo.toml Cargo.lock crates/shard-client crates/dtgproxy crates/query-executor
git commit -m "refactor: route coordinators through ShardClient"
```

## Task 9: Remote ShardClient and topology cache

**Files:**

- Create: `crates/shard-client/src/remote.rs`
- Create: `crates/shard-client/src/topology.rs`
- Create: `crates/shard-client/tests/remote_tck.rs`
- Create: `crates/shard-client/tests/topology_watch.rs`
- Modify: `crates/shard-client/src/lib.rs`

**Step 1: Run the same client TCK against real Data processes**

The initial failing test boots Data services and exercises exactly the embedded contract, including Leader changes, stale epoch refresh, duplicate delivery, unavailable minority, and deadlines.

**Step 2: Implement Meta watch and revisioned routing**

Build an immutable topology snapshot and atomically swap it after a complete Catalog revision. On stale epoch, apply a bounded refresh/retry only for idempotent phases. Never retry past deadline.

**Step 3: Implement pooled remote calls**

Reuse channels by node identity, validate server identity, preserve request/phase IDs, and map transport errors to retry classifications.

**Step 4: Verify and commit**

```bash
cargo test -p shard-client --all-targets
cargo clippy -p shard-client --all-targets --all-features -- -D warnings
git add crates/shard-client
git commit -m "feat: execute shard operations across data nodes"
```

## Task 10: Production Gateway process

**Files:**

- Create: `crates/gateway-node/Cargo.toml`
- Create: `crates/gateway-node/src/service.rs`
- Create: `crates/gateway-node/src/admission.rs`
- Create: `crates/gateway-node/src/lib.rs`
- Create: `crates/gateway-node/src/bin/dtgproxy-gateway.rs`
- Create: `crates/gateway-node/tests/service.rs`
- Create: `crates/gateway-node/tests/concurrency.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Step 1: Write failing remote Gateway tests**

Prove the process contains no local Replica directories, concurrent reads do not serialize, mutations for one Shard preserve ordering, deadlines cancel downstream work, and admission queues are bounded.

**Step 2: Assemble existing coordinators with RemoteShardClient**

The Gateway owns auth/admission hooks, Catalog/Schema cache, transaction coordinator, query planner, and result streams. Use per-graph/per-Shard mutation gates only where state-machine ordering requires them; reads remain concurrent.

**Step 3: Implement process lifecycle**

Readiness requires Meta watch catch-up and valid service identity. Shutdown withdraws readiness, stops admission, drains to deadline, and leaves durable recovery to Home Shards.

**Step 4: Verify and commit**

```bash
cargo test -p gateway-node --all-targets
cargo clippy -p gateway-node --all-targets --all-features -- -D warnings
git add Cargo.toml Cargo.lock crates/gateway-node
git commit -m "feat: serve stateless concurrent Gateways"
```

## Task 11: Migration records in the replicated Catalog

**Files:**

- Create: `crates/control-plane/src/migration.rs`
- Create: `crates/control-plane/tests/migration.rs`
- Modify: `crates/control-plane/src/lib.rs`
- Modify: `crates/control-plane/tests/catalog.rs`
- Modify: `crates/meta-node/src/state_machine.rs`

**Step 1: Write the failing state-machine matrix**

Test every legal edge, every illegal edge, CAS revision conflict, command replay, snapshot/log recovery from every state, one-active-workflow ownership, pre-publish abort, and post-publish no-rollback.

**Step 2: Add bounded durable model and commands**

Implement `MigrationRecord`, `MigrationState`, step receipts/fences, create/advance/fail/abort/finish commands, and Catalog query indexes by graph/shard/state. Extend command/snapshot codecs with backward-readable versioning.

**Step 3: Verify and commit**

```bash
cargo test -p control-plane -p meta-node --all-targets
cargo clippy -p control-plane -p meta-node --all-targets --all-features -- -D warnings
git add crates/control-plane crates/meta-node
git commit -m "feat: persist topology migration workflows"
```

## Task 12: Durable Data-side migration receipts and snapshot transfer

**Files:**

- Create: `crates/data-node/src/migration.rs`
- Create: `crates/data-node/tests/migration_receipts.rs`
- Create: `crates/data-node/tests/snapshot_transfer.rs`
- Modify: `crates/data-node/src/manifest.rs`
- Modify: `crates/data-node/src/service.rs`
- Modify: `crates/replica-snapshot/src/lib.rs`

**Step 1: Write failing crash-point tests**

Inject failure before/after staging creation, each chunk fsync, manifest fsync, Adapter import, actor start, learner catch-up fence, membership change, and delete. Repeating the same step must converge or return an immutable conflict.

**Step 2: Implement receipts and resumable staging**

Persist `(migration_id,step,input_digest,outcome)` records. Snapshot chunks write by ordinal to bounded files, validate CRC/digest, resume from acknowledged ordinal, and atomically install only a complete manifest.

**Step 3: Add learner and cutover fences**

Expose applied index, snapshot index, backend/schema generation, membership role, and epoch to the Controller. Do not mark Ready until all fences match.

**Step 4: Verify and commit**

```bash
cargo test -p replica-snapshot -p data-node --all-targets
cargo clippy -p replica-snapshot -p data-node --all-targets --all-features -- -D warnings
git add crates/replica-snapshot crates/data-node
git commit -m "feat: resume and verify shard migration steps"
```

## Task 13: Reconciliation Controller and epoch lineage

**Files:**

- Create: `crates/controller/Cargo.toml`
- Create: `crates/controller/src/reconciler.rs`
- Create: `crates/controller/src/lib.rs`
- Create: `crates/controller/src/bin/dtgproxy-controller.rs`
- Create: `crates/controller/tests/reconciliation.rs`
- Create: `crates/controller/tests/crash_matrix.rs`
- Modify: `crates/control-plane/src/lib.rs`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`

**Step 1: Write the failing complete crash matrix**

For each transition from Preparing through Cleaned, kill the Controller before and after the Meta commit and Data receipt. Restart a different Controller and assert convergence, one authoritative epoch, intact temporal history, and no premature cleanup.

**Step 2: Implement lease-owned level reconciliation**

Each loop reads durable desired state and observed node status, computes one idempotent action, executes it, then CAS-advances Meta. Ownership term prevents an expired Controller from advancing a workflow.

**Step 3: Publish topology and lineage atomically**

The same Meta command records the new topology epoch, Shard lineage, and migration cutover fence. Old-epoch calls receive the new epoch/leader hint. Cleanup waits for transaction/backup/CDC pins.

**Step 4: Verify and commit**

```bash
cargo test -p controller -p control-plane -p data-node --all-targets
cargo clippy -p controller -p control-plane -p data-node --all-targets --all-features -- -D warnings
git add Cargo.toml Cargo.lock crates/controller crates/control-plane
git commit -m "feat: reconcile crash-safe shard migrations"
```

## Task 14: Durable backend hot-swap workflow

**Files:**

- Create: `crates/controller/src/backend_migration.rs`
- Create: `crates/controller/tests/backend_migration.rs`
- Modify: `crates/control-plane/src/migration.rs`
- Modify: `crates/data-node/src/migration.rs`
- Modify: `crates/shard-runtime/src/state_machine.rs`
- Modify: `crates/adapter-sidecar/src/service.rs`

**Step 1: Write failing per-boundary crash tests**

Cover target prepare, snapshot copy, dual-apply start, verification, Catalog generation publish, source drain, and cleanup across all replicas. Inject Adapter/Catalog failures and prove no replica serves a mixed generation.

**Step 2: Drive Adapter slots through Raft commands**

All replicas apply identical start-dual/write/verify/cutover commands. Catalog generation is published only after quorum-observed fences. Source cleanup follows transaction and snapshot pins.

**Step 3: Run every Adapter TCK on both sides of a live switch**

```bash
cargo test -p adapter-rocksdb -p adapter-postgres -p adapter-neo4j -p adapter-sidecar --all-targets
cargo test -p controller --test backend_migration
```

**Step 4: Commit**

```bash
git add crates/controller crates/control-plane crates/data-node crates/shard-runtime crates/adapter-sidecar
git commit -m "feat: make backend hot-swap crash-replayable"
```

## Task 15: True multi-process P0 acceptance harness

**Files:**

- Create: `crates/cluster-tests/Cargo.toml`
- Create: `crates/cluster-tests/src/lib.rs`
- Create: `crates/cluster-tests/tests/primary_replica.rs`
- Create: `crates/cluster-tests/tests/shared_nothing.rs`
- Create: `crates/cluster-tests/tests/failover.rs`
- Create: `crates/cluster-tests/tests/migration.rs`
- Create: `crates/cluster-tests/tests/stale_gateway.rs`
- Create: `config/examples/cluster-dev/README.md`
- Create: `config/examples/cluster-dev/meta-1.toml`
- Create: `config/examples/cluster-dev/data-1.toml`
- Create: `config/examples/cluster-dev/gateway-1.toml`
- Modify: `Cargo.toml`
- Modify: `README.md`

**Step 1: Build a child-process-only harness**

Reserve ephemeral ports safely, generate isolated directories/certificates, start binaries, wait on readiness, capture structured logs, kill/restart individual processes, and always clean up children. The harness may communicate only over public RPC.

**Step 2: Implement all P0 acceptance scenarios**

Run both deployment modes, cross-Shard bitemporal transaction from one Gateway/read from another, Data and Meta Leader failover, Gateway loss after Home decision, stale Gateway epoch, migration crash matrix, and backend migration replay.

**Step 3: Prove no hidden in-process cluster path**

Inspect each child PID and data directory; assert a Data process only owns its declared local replicas and Gateway owns none.

**Step 4: Run the full release gate**

```bash
cargo fmt --all -- --check
cargo test --workspace --all-targets
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

Run environment-backed certification smoke when disposable services are configured:

```bash
cargo test -p adapter-postgres --test live_postgres -- --ignored
cargo test -p adapter-neo4j --test live_neo4j -- --ignored
```

**Step 5: Record evidence and commit**

Create `docs/verification/dtgproxy-p0-cluster-runtime.md` with exact commands, toolchain, test counts, failure scenarios, and any environment-gated exclusions. Do not call P0 complete if a required non-gated test is skipped.

```bash
git add Cargo.toml Cargo.lock README.md config crates/cluster-tests docs/verification
git commit -m "test: certify the DTGProxy multi-process cluster"
```

## P0 exit criteria

P0 exits only when:

- Tasks 1–15 and their strict checks pass on the workspace main path.
- The production Gateway contains no `InProcessShardGroup` construction.
- A real Meta quorum is the only authority for Catalog, topology epoch, migration state, and TSO high-water.
- Migration/backend cutover crash matrices cover every durable boundary.
- Both PrimaryReplica and SharedNothing pass through remote processes.
- The verification record explicitly lists external-service suites not executed; those remain blockers for P4 production certification, not silently accepted evidence.

After P0, execute P1–P4 from the production design, then perform the final boundary audit. Do not rename the current release as production at the P0 boundary.
