# DTGProxy Phase 2 Replicated Shard Runtime Plan

**Goal:** Replace Phase 1C's caller-ordered local apply assumption with a real three-replica Raft
Shard Group, versioned deterministic commands, durable replica watermarks, epoch fencing, and safe
leader/follower read barriers.

**Dependency decision:** Use TiKV's Apache-2.0 `raft-rs` 0.7.0 as a library with `prost-codec` and
without its default logger. The official project explicitly supplies only the consensus module;
DTGProxy owns log persistence, state-machine application, transport, snapshots, and product
semantics. The in-process deterministic transport is implemented first, then isolated behind a
trait for later gRPC. NebulaGraph's KV/Multi-Raft split informs group ownership and batching, but no
Folly/Thrift/C++ source is copied. Sources: [raft-rs](https://github.com/tikv/raft-rs),
[official five-node example](https://github.com/tikv/raft-rs/tree/master/examples/five_mem_node),
[NebulaGraph](https://github.com/vesoft-inc/nebula).

## Invariants

- A client receives success only after a majority commits the entry and the leader Adapter applies
  it durably.
- Raft Entry bytes contain a versioned command with all deterministic logical mutations. Apply does
  not read business state, allocate time, generate IDs, or perform conflict decisions.
- Every Replica applies entries strictly by index. Business mutations, replica metadata, and the
  Adapter `applied_log_index` advance in one local atomic batch.
- A command is fenced by `(shard_id, placement_epoch)`; stale-epoch entries and requests fail
  closed.
- `closed_ts`, `resolved_ts`, and `adapter_applied_ts` are replicated/durable. Phase 2 has no
  distributed Intents, so a committed Closed-Timestamp Tick may advance `resolved_ts`; Phase 3 must
  replace this shortcut with Intent-aware resolution.
- `safe_ts = min(closed_ts, resolved_ts, adapter_applied_ts)`. Follower reads additionally require
  current placement authority and an applied ReadIndex/lease proof.
- Snapshot installation restores Raft membership/log position, watermarks, and an Adapter
  checkpoint representing the same applied index.

---

### Task 1: Separate Deterministic Prepare from Local Apply

**Files:** `storage-api`, `temporal-storage`, transaction TCK.

- [x] Add failing tests proving preparation returns byte-identical mutations without touching the
  Adapter and local convenience commit remains prepare-then-apply compatible.
- [x] Introduce `PreparedMutationBatch { shard_id, txn_id, mutations }`; assign the actual committed
  log index only at state-machine Apply.
- [x] Refactor `TemporalStore::prepare_transaction` to validate/stage the full graph transaction;
  keep `commit_transaction` as the Phase 1-compatible wrapper.
- [x] Preserve replay, conflict, endpoint, and deterministic ordering TCK results.
- [x] Commit as `refactor: separate temporal prepare and apply`.

### Task 2: Versioned Raft Command and Replica Metadata Codec

**Files:** new `raft-command` crate and corruption/golden tests.

- [x] Define `CommandEnvelopeV1 { shard_id, placement_epoch, request_id, body }` with bodies
  `ApplyPrepared { commit_ts, batch }` and `ClosedTimestampTick { closed_ts }`.
- [x] Implement explicit big-endian, length-delimited encoding with magic/version/body tags and
  checksum; do not use Rust default serialization for durable commands.
- [x] Decode with size/count limits, duplicate mutation-sequence rejection, canonical command
  ordering checks, and trailing/corruption rejection.
- [x] Add golden bytes, round-trip, arbitrary-byte smoke, and Phase 1 mutation compatibility tests.
- [x] Commit as `feat: define replicated shard commands`.

### Task 3: Durable Deterministic Shard State Machine

**Files:** new `shard-runtime` crate, runtime metadata keys/codecs, Memory/Rocks TCK.

- [x] Apply a decoded command at a supplied `(term, index)` and reject gaps, shard/epoch mismatch,
  non-monotonic commit time, and divergent replay before mutation.
- [x] Append deterministic Meta mutations so term/index/epoch/watermarks and business records share
  the Adapter's one atomic batch.
- [x] Implement replicated Closed-Timestamp Tick and calculate `safe_ts` from durable components.
- [x] Recover runtime metadata from Memory/RocksDB restart and prove duplicate Apply is idempotent.
- [x] Inject Adapter failure and prove the replica stops serving/advancing safe time until replay
  catches it up.
- [x] Commit as `feat: apply deterministic shard state`.

### Task 4: Three-node raft-rs Group with Deterministic Transport

**Files:** `shard-runtime` Raft node/group/transport modules and fault harness.

- [ ] Integrate `raft-rs` `RawNode` using a narrow wrapper; persist HardState, entries, and snapshot
  before sending persistence-dependent messages, following the official Ready/LightReady order.
- [ ] Implement an in-process transport supporting drop, delay, isolate, heal, reorder, and node
  stop/restart without shared replica state.
- [ ] Correlate proposals by request ID and acknowledge only after majority commit plus leader state
  apply; retry/replay must return the same outcome.
- [ ] Test election, 3/3 replication, one follower loss, leader loss/re-election, minority refusal,
  partition heal, duplicate delivery, and log convergence.
- [ ] Add `MultiRaftRuntime` ownership mapping `(node_id, shard_id) -> replica` and prove two groups
  progress independently on the same node.
- [ ] Commit as `feat: replicate temporal shard groups`.

### Task 5: Snapshot, Restart, and Log Catch-up

**Files:** Raft log store, checkpoint manifest, restore tests.

- [ ] Version a manifest binding shard/epoch/term/applied index/watermarks to an Adapter checkpoint
  and checksum.
- [ ] Create snapshots only after Adapter checkpoint completion at the matching applied index;
  compact logs only after the snapshot is durable.
- [ ] Install snapshot into an empty/lagging follower, then replay the suffix and compare Current,
  AS OF, DIFF, adjacency, metadata, and command fingerprints.
- [ ] Test crash points before/after checkpoint, manifest publish, log compact, and snapshot apply.
- [ ] Commit as `feat: recover replicated shard snapshots`.

### Task 6: Epoch Fencing and Consistent Read Barriers

**Files:** shard request API, ReadIndex/lease state, query-executor routing adapter.

- [ ] Define leader-linearizable and follower-snapshot read modes with explicit retryable
  `NotLeader`, `StaleEpoch`, `NotReady`, and `AdapterLagging` errors.
- [ ] Leader reads wait for ReadIndex apply. Follower reads require `safe_ts >= read_ts`, applied
  ReadIndex/lease proof, matching epoch, and healthy Adapter.
- [ ] Replicate ticks on idle shards and prove safe time advances without writes but never crosses
  an unapplied commit.
- [ ] Execute typed Temporal IR through the shard read API without direct Adapter access.
- [ ] Commit as `feat: enforce replicated temporal read barriers`.

### Task 7: Phase 2 Acceptance

- [ ] Run format, strict Clippy, full workspace/TCK/smoke suites and deterministic fault schedules.
- [ ] Run three-process loopback smoke after the in-process harness is stable; transport must remain
  replaceable without changing state-machine semantics.
- [ ] Benchmark proposal-to-commit, commit-to-apply, follower snapshot reads, tick overhead, and
  snapshot catch-up. Compare replication factor 1 versus 3.
- [ ] Document raft-rs attribution/SBOM, known Phase 3 Intent shortcut, operational metrics, and
  exact evidence.
- [ ] Do not mark the overall DTGProxy goal complete; Phase 3–6 remain.
