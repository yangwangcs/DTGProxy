# Consistent Shard Read Barriers

The current runtime exposes two explicit read modes. Neither mode accepts a raw Adapter reference
from the caller; the distributed fragment worker validates the typed Temporal IR, acquires a
`ReadPermit`, and executes the current temporal batch runtime over the selected Replica.

## Leader-linearizable reads

`leader_read_permit` rejects a stale placement epoch or a node that is not the current leader. It
submits a unique context through raft-rs `ReadIndex` in safe quorum mode, records the returned index
and leader term from `Ready.read_states`, and waits until the leader state machine's atomic Adapter
`applied_index` reaches that index. A leadership change, quorum timeout, stopped Replica, or
unhealthy/lagging Adapter fails closed.

The resulting permit can execute `CURRENT`, `AS OF`, or `DIFF` plans. `CURRENT` never runs on a
Follower because safe time describes immutable historical knowledge, not the latest linearizable
projection.

## Follower snapshot reads

A `FollowerReadProof` binds shard ID, placement epoch, leader ID, leader term, and quorum-confirmed
ReadIndex. It cannot be constructed outside `shard-runtime`. Before issuing a permit, the target
Follower verifies:

- it is a running non-leader Replica in the current placement epoch;
- the proof still names the current leader and term;
- its Adapter has applied through the proof's ReadIndex and the state machine is healthy;
- its durable `safe_ts` is at least the plan's required transaction time.

For an `AS OF` request, the required time is its transaction selector. For `DIFF`, it is the upper
transaction bound. A `CURRENT` request is rejected before Adapter access. The fragment worker also
fences graph, topology, snapshot, and target Shard before requesting ReadIndex.

Retryable failures are explicit: `NotLeader`, `StaleEpoch`, `NotReady`, and `AdapterLagging`.
`NotReady` covers missing quorum, a changed proof term, and a safe-time frontier below the request;
`AdapterLagging` identifies a local apply frontier below the quorum proof.

## Idle safe-time advancement

Raft heartbeats do not change temporal knowledge. `advance_closed_timestamp` therefore proposes a
versioned `ClosedTimestampTick` even on an otherwise idle Shard. Once committed and applied, the
state machine atomically advances `closed_ts` and `adapter_applied_ts`, then derives `resolved_ts`
from the oldest unresolved Intent, making `safe_ts` visible to Followers. An isolated Follower
cannot use the new timestamp: its applied index remains below the subsequent ReadIndex proof.

The deterministic three-node tests cover wrong leader/epoch, quorum loss, an idle safe-time tick,
Follower apply lag and catch-up, stale proof rejection after re-election, `CURRENT` rejection, and
authorized `AS OF`/`DIFF` execution. The current group harness uses Memory Adapter; the same permit
contract is the service boundary for the durable multi-process runtime.
