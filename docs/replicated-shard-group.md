# DTGProxy Replicated Shard Group

Phase 2 uses TiKV's Apache-2.0 `raft-rs` 0.7.0 consensus module with `prost-codec` and without its
default logger. DTGProxy owns storage, transport, command semantics, state-machine application,
safe time, and request completion. No NebulaGraph C++ source is copied.

## Ready processing

Every Replica owns an independent Raft log store and an independent graph Adapter. The `RawNode`
wrapper processes each Ready in this order:

1. take ordinary outbound messages but do not expose them to the transport yet;
2. persist an incoming snapshot, new log entries, and HardState;
3. release persistence-dependent messages;
4. apply committed normal entries to `ShardStateMachine` in index order;
5. call `advance`, durably update the LightReady commit index, process LightReady messages and
   committed entries, then call `advance_apply`.

Leader-election empty entries are not skipped: `apply_noop_entry` atomically advances the Adapter
and Replica position without changing graph data. Raft index and Adapter `applied_log_index`
therefore never diverge because of a leader no-op.

The deterministic harness uses a distinct `MemStorage` per Replica to inject precise network and
leadership schedules. The production-facing `DurableRaftReplica` uses `RocksRaftStorage`, persists
HardState/ConfState/entries/snapshot records with checksums and synchronous writes, and replays a
crash window in which Raft commit became durable before Adapter apply. Snapshot bundles install the
Adapter checkpoint and matching Raft snapshot as a hidden generation before publication, then
replay a retained committed suffix.

## Request completion

`request_id` is carried in both the command envelope and Raft proposal context. A request completes
only after Raft has committed its Entry and the Leader's own Adapter has applied it. Applied events
are retained per Replica, so a newly elected Leader can complete a request that it applied earlier
as a follower. Retrying an already completed request with identical bytes returns the original
receipt; reusing the ID with different bytes fails closed.

The in-process harness retains pending/completed request memory. Durable cross-leader request
deduplication is deliberately part of the Phase 3 transaction-status records; callers must not
assume an unacknowledged Phase 2 proposal has been durably deduplicated after a whole-process
restart.

## Deterministic network and Multi-Raft

`DeterministicTransport` can drop or duplicate messages, add per-link delay, isolate/heal a node,
block/heal a directed link, and reverse the delivery order. Tests cover 3/3 replication, one
follower stopped and caught up, Leader loss and re-election, minority refusal, partition heal,
request replay, and final state convergence.

`MultiRaftRuntime` maps `(node_id, shard_id)` ownership while keeping one independent Raft Group per
Shard. Tests run two groups with different Leaders on the same three logical nodes and prove that
both progress independently.

The replaceable acceptance transport uses versioned, checksummed TCP frames and a real
three-process restart smoke. It deliberately opens one TCP connection per message and is not the
final production network. Persistent pooled/multiplexed connections, TLS, admission control, and
backpressure remain release requirements.
