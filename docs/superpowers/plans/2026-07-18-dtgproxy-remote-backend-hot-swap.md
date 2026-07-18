# DTGProxy Remote Backend Hot-Swap Implementation Plan

> Scope: complete the production cluster path that is currently RocksDB-only. Raft WAL remains local and backend-independent; temporal graph state is stored in a durable, generation-fenced Adapter slot.

## Invariants

1. A committed Raft entry is acknowledged only after every backend required by the current backend phase has durably applied the same log index.
2. Backend generation and phase are replicated state, not Controller memory and not a process-local flag.
3. A target can enter dual-apply only after logical restore proves `target.applied_index == source.applied_index == fence_index`.
4. Cutover is legal only when no apply is in flight and source/target durable indices are equal.
5. Restart reconstructs the same active/shadow generations before replaying committed Raft entries.
6. Controller publication is the final catalog commit. Until publication, routing remains on the source generation; reconciliation is idempotent after every crash point.
7. Secrets are referenced by credential IDs. Replica manifests and catalog snapshots store only public, checksummed backend profiles.

## Phase A — Durable Data Replica Adapter Slot

### Task A1: Make the existing hot-swap slot restartable

Files:

- Modify `crates/adapter-registry/src/lib.rs`
- Modify `crates/adapter-registry/tests/registry.rs`

TDD cases:

- construct an active slot at generation greater than one;
- reject generation zero;
- expose active provider/instance identity without leaking the adapter;
- restore a dual-applying slot with an explicit target generation and synchronized index;
- reject non-consecutive target generations and target/source index mismatch.

Implementation:

- add validated constructors for `OpenedAdapter` and `HotSwapAdapter` recovery;
- add a serializable public `HotSwapRecoveryState` containing generation, active identity, optional shadow identity, and synchronized index;
- keep `HotSwapAdapter::new` as the generation-one convenience path.

### Task A2: Decouple `DurableRaftReplica` from concrete RocksDB state storage

Files:

- Modify `crates/shard-runtime/Cargo.toml`
- Modify `crates/shard-runtime/src/durable_replica.rs`
- Modify `crates/shard-runtime/tests/durable_replica.rs`

TDD cases:

- open a durable replica with an injected `Arc<HotSwapAdapter>`;
- recover Raft commit/apply invariants through an injected slot;
- prove reads and scans follow the active adapter after cutover;
- retain the RocksDB convenience constructor for existing callers.

Implementation:

- store `ShardStateMachine<Arc<HotSwapAdapter>>`;
- introduce `open_with_adapter_slot` while retaining `open` for the local RocksDB default;
- expose only the hot-swap control surface, not a concrete backend type.

### Task A3: Persist backend profiles and phase in the replica manifest

Files:

- Modify `crates/data-node/src/manifest.rs`
- Modify `crates/data-node/src/host.rs`
- Modify `crates/data-node/tests/identity_manifest.rs`

Data model:

- `BackendProfile { provider, instance_id, public_parameters, credential_refs, digest }`;
- `BackendSlotState::Active { generation, profile }`;
- `BackendSlotState::DualApplying { source_generation, source, target_generation, target, fence_index, synchronized_index }`.

TDD cases:

- v3 Rocks manifests decode into an equivalent generation-one Rocks profile;
- v4 round-trips each slot phase with checksum validation;
- invalid provider names, absolute paths, embedded secrets, zero/non-consecutive generations, and digest mismatches fail closed;
- manifest replacement permits only valid forward slot transitions.

Implementation:

- bump manifest format to v4 with backward-compatible v2/v3 decoding;
- persist a manifest transition before making its corresponding runtime state externally visible;
- reconstruct `ReplicaSpec` and the Adapter slot from the manifest at Data startup.

### Task A4: Register local and sidecar-backed providers in Data

Files:

- Modify `crates/data-node/Cargo.toml`
- Add `crates/data-node/src/backend.rs`
- Modify `crates/data-node/src/lib.rs`
- Modify `crates/data-node/src/config.rs`
- Modify `crates/data-node/src/host.rs`
- Modify `crates/data-node/src/replica_actor.rs`
- Add `crates/data-node/tests/backend_registry.rs`

Implementation:

- register `rocksdb` locally and `sidecar` for isolated PostgreSQL/Neo4j adapters;
- resolve credentials through a Data-local resolver interface;
- allow-list sidecar endpoints and enforce connect/request deadlines;
- open active and shadow adapters from persisted profiles before actor start;
- make backend-open failure keep the replica unavailable instead of silently falling back.

## Phase B — Replicated Backend Lifecycle

### Task B1: Add Raft commands and replicated backend metadata

Files:

- Modify `crates/raft-command/src/lib.rs`
- Modify `crates/raft-command/tests/codec.rs`
- Modify `crates/shard-runtime/src/lib.rs`
- Modify `crates/shard-runtime/src/durable_replica.rs`
- Add lifecycle cases to `crates/shard-runtime/tests/durable_replica.rs`

Commands:

- `BeginBackendDualApply { source_generation, target_generation, target_profile_digest, fence_index }`;
- `CutoverBackend { source_generation, target_generation, target_profile_digest }`;
- `AbortBackendMigration { source_generation, target_generation, target_profile_digest }`.

TDD cases:

- stale generation, wrong digest, wrong fence, and illegal phase transitions are rejected deterministically;
- duplicate commands return the original receipt;
- all replicas reach the same phase at the same applied index;
- crash before/after apply replays to one legal state;
- normal graph mutations dual-apply between begin and cutover.

Implementation:

- extend `ReplicaMetadata` with backend generation, phase, target digest, and fence;
- prepare target locally before proposing `BeginBackendDualApply`;
- execute slot transition while applying the replicated lifecycle entry;
- persist manifest phase and return a deterministic transition receipt.

### Task B2: Expose idempotent Data backend-management RPCs

Files:

- Modify `crates/cluster-protocol/proto/dtgproxy_cluster_v1.proto`
- Modify `crates/data-node/src/service.rs`
- Modify `crates/data-node/src/host.rs`
- Modify `crates/data-node/src/replica_actor.rs`
- Add `crates/data-node/tests/service_backend_migration.rs`

RPCs:

- `PrepareBackendTarget` streams/restores a logical snapshot into the requested target profile and returns a signed receipt containing fence, digest, and target descriptor;
- `BeginBackendDualApply`, `CutoverBackend`, and `AbortBackendMigration` propose the corresponding Raft command through the leader;
- `GetBackendStatus` reports replicated phase plus local active/shadow durable indices.

TDD cases:

- request replay is idempotent and conflicting replay is rejected;
- non-leader responses include leader hints;
- an unprepared target cannot enter dual-apply;
- a prepared target with a stale fence must be restored again;
- status survives process restart.

## Phase C — Durable Meta Orchestration

### Task C1: Add a catalog backend-migration record

Files:

- Modify `crates/control-plane/src/catalog.rs`
- Modify `crates/control-plane/src/command.rs`
- Modify `crates/control-plane/src/snapshot.rs`
- Modify `crates/control-plane/tests/catalog.rs`
- Modify `crates/control-plane/tests/durable_catalog.rs`

State machine:

`Preparing -> Restored -> DualApplying -> Verified -> CutOver -> Published -> SourceRetired`, with `Aborting -> Aborted` before publication.

TDD cases:

- exactly one migration per graph generation;
- target generation is exactly source + 1;
- profile digest is immutable;
- phase transitions require receipts from every replica in the placement;
- publication atomically advances the graph backend generation/profile;
- snapshot/restart preserves every phase and receipt.

### Task C2: Implement the Controller backend reconciler

Files:

- Modify `crates/controller/src/lib.rs`
- Modify `crates/controller/src/remote.rs`
- Add `crates/controller/src/backend_reconciler.rs`
- Add `crates/controller/tests/remote_backend_migration.rs`

Implementation:

- derive every operation ID from migration ID, phase, node ID, and profile digest;
- restore every replica target at a common snapshot fence;
- begin dual apply, verify equal durable indices, cut over, then publish catalog generation;
- retire the source only after publication and retention pins permit cleanup;
- on restart, read catalog phase and receipts and continue the first incomplete step.

TDD cases:

- inject a Controller crash after every remote effect and every catalog commit;
- lose/retry every RPC response;
- reject mixed target descriptors or indices;
- preserve source routing on pre-publication failure;
- converge without an index gap or double generation publication.

## Phase D — Backends and Acceptance

### Task D1: Certify the three representative providers

Files:

- Modify `crates/adapter-sidecar/src/service.rs`
- Modify `crates/adapter-postgres/src/lib.rs`
- Modify `crates/adapter-neo4j/src/lib.rs`
- Add provider recovery/restore tests in their test directories

Providers:

- KV: local RocksDB;
- SQL: PostgreSQL through sidecar;
- Graph: Neo4j through sidecar. Neo4j is the v1 graph representative; Memgraph remains an additional provider, not an alias with unproven protocol semantics.

Certification:

- descriptor capability compatibility;
- idempotent apply and monotonic applied index;
- logical snapshot export/restore and checksum;
- restart recovery and credential redaction;
- deadline/cancellation behavior and error classification.

### Task D2: Run the multi-process hot-swap matrix

Files:

- Add `crates/dtgproxy/tests/cluster_backend_hot_swap.rs`
- Add/update `scripts/certify-external-backends.sh`
- Update `docs/dtgproxy-v1-boundary-audit.md`

Matrix:

- PrimaryReplica and SharedNothing;
- RocksDB -> PostgreSQL, PostgreSQL -> Neo4j, Neo4j -> RocksDB;
- writes before restore, during dual apply, immediately around cutover, and after publication;
- Data/Meta/Controller/sidecar crash at each phase;
- historical point, interval, diff, and current reads before/after restart;
- no missing/duplicate version, identical digest, monotonic generation and applied index.

Final gates:

```bash
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo fmt --all -- --check
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo test --workspace --all-targets
CXX=/opt/homebrew/opt/llvm/bin/clang++ LIBCLANG_PATH=/opt/homebrew/opt/llvm/lib cargo clippy --workspace --all-targets --all-features -- -D warnings
./scripts/certify-external-backends.sh
```

The final boundary audit is updated only after this matrix passes. Missing external-service evidence is reported as an explicit release blocker, never converted into a success claim.
