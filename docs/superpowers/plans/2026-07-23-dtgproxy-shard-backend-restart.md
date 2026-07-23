# DTGProxy Shard and Backend Restart Recovery Plan

**Goal:** certify that an in-flight analytics Job remains recoverable while its Gateway stays
alive and a DataNode, Shard, or backend is independently restarted. This extends the current-only
DTGProxy 1.1 scheduler; it introduces no compatibility API or legacy execution path.

## Recovery contract

- Restart with the same durable directory, cluster/node/graph/shard identity, placement epoch,
  backend identity, and fixed advertised address.
- Close the serving process and all existing Shard connections before bringing the same placement
  back. The Gateway and Meta quorum remain alive unless a combination case explicitly restarts
  them later.
- A transport, Leader-election, or backend-unavailable error is retryable infrastructure loss. It
  must not turn an owned `RUNNING` Job into `FAILED`.
- The current owner may continue only while it can renew the fenced lease. Otherwise the lease
  expires and the same or another Gateway claims a higher lease epoch and restores the latest
  committed checkpoint.
- Recovery may not silently restart completed slices. The final canonical `DTAR` Result bytes must
  equal an uninterrupted baseline byte-for-byte.
- A successful Job has exactly one pinned Result generation. Interrupted upload generations are
  reclaimable and must never become visible as a published result.

## Test-first sequence

1. Add a RocksDB process test that observes `RUNNING`, stops the DataNode while the Gateway remains
   alive, proves the Job never reaches `FAILED`, restarts the DataNode at the same address and from
   the same directory, then checks success, byte identity, and the single-pinned-Result invariant.
2. Use the failing test to classify scheduler errors at the Shard/backend boundary. Preserve
   fail-closed behavior for deterministic provider, checkpoint-corruption, semantic, and fencing
   errors; leave retryable infrastructure failures non-terminal so lease expiry can drive takeover.
3. Re-run the process case with a second Gateway to certify cross-Gateway takeover after the Shard
   outage, then cover PrimaryReplica and Shared-Nothing placement recovery.
4. Apply the same contract to RocksDB, PostgreSQL, and Neo4j backend/Sidecar restart fixtures. Each
   backend uses a stable namespace and durable identity across restart.
5. Add ordered combination cases for Gateway + Meta Leader + Shard + backend restart. Verify one
   terminal outcome, one pinned Result, no ghost Artifact, and baseline-identical bytes.

## Verification gates

- Focused process and scheduler tests pass after an observed red/green cycle.
- `cargo test -p gateway-node`, the real three-backend certification jobs, strict workspace Clippy,
  `cargo fmt --check`, and `git diff --check` pass.
- The design specification, Adapter documentation, restart runbook, and progress ledger label each
  result accurately as implemented, compiled, real-backend certified, or production-certified.
