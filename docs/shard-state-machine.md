# DTGProxy Shard State Machine

`ShardStateMachine` is the only Phase 2 component allowed to turn a committed Raft Entry into an
Adapter write. It owns one `(shard_id, placement_epoch)` and applies entries strictly by
`(term,index)`.

## Atomic apply

For a new ApplyPrepared entry, one `CommittedMutationBatch` contains:

- every already validated business mutation from the command;
- an immutable BLAKE3 digest record for `(term,index,command bytes)`;
- the current Replica position `(shard,epoch,term,index)`;
- each changed closed, resolved, or Adapter-applied timestamp.

The Adapter commits these records and its own `applied_log_index` in one local atomic batch. A
process crash therefore cannot expose a new graph value with an old Replica watermark, or the
opposite. Reserved Replica Meta keys cannot be supplied by a business mutation.

The durable Raft command envelope `request_id` is the client/internal-proposal deduplication
namespace. The prepared batch `txn_id` remains the graph/distributed-transaction identity. A
Sidecar frame also has a request ID, but that value is transport correlation only: an ambiguous
network retry may execute the same call twice. The Adapter independently makes the committed
`(shard_id, log_index, txn_id, mutation fingerprint)` idempotent and rejects divergent replay.

## Replay and fencing

Before a new write the state machine rejects zero/gapped indices, decreasing terms, wrong shards,
stale placement epochs, commit timestamps at or before `closed_ts`, timestamp regressions, and
reserved metadata keys. Old-index replay reads the immutable entry digest: an exact command returns
a duplicate receipt without rewriting current metadata; a different term or command fails as
`DivergentReplay`.

Any Adapter error faults the Replica at that entry. The Replica exposes no servable safe timestamp
until the same entry is retried. Retry reloads durable metadata first, so it handles both a
pre-write failure and an ambiguous response after a successful atomic write.

## Safe time

Phase 2 has no distributed Intents. A committed ClosedTimestampTick advances `closed_ts` and
`resolved_ts` together. `adapter_applied_ts` is monotonic and is advanced by applied commits and
closed-time barriers. The public read frontier is:

```text
safe_ts = min(closed_ts, resolved_ts, adapter_applied_ts)
```

A tick may trail a newer applied commit; in that case `safe_ts` advances only to the tick. An idle
shard can advance all three components with a tick. Phase 3 replaces the `resolved_ts = closed_ts`
shortcut with Intent-aware resolution.

## Durable metadata

Replica records live under the reserved Meta prefix `0x01dtg/replica/v1/`. Position, timestamps,
and per-entry digests use explicit big-endian, versioned, checksummed codecs. On open, the durable
position must exactly equal the Adapter's applied log index. Missing, corrupted, wrong-shard, or
wrong-epoch metadata fails closed. Per-entry digest records are retained until Phase 5 snapshot/log
compaction proves an equivalent checkpoint durable.
