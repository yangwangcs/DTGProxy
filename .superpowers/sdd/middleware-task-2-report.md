# Middleware Task 2 Report — Three-Data Static Routing Per Provider

## Scope

Modified `crates/processes/dtg-gateway/tests/four_process_cluster.rs` only. The topology
continues to use the existing Gateway/Data protocol and in-process Data RPC endpoint wrapper;
no production routing, migration, rebalance, follower-read, or cross-provider behavior changed.

## TDD record

1. Replaced the Fjall-only topology test with separate Fjall, Kuzu, and ignored PostgreSQL
   provider cases, before generalizing the binding/configuration helper.
2. Ran the required Kuzu red command:

   ```text
   cargo test --locked -p dtg-gateway --test four_process_cluster static_shard_snapshot_routes_kuzu
   ```

   It failed as expected because the original binding was `ProviderKind::Fjall` while the
   Kuzu `DataProcessConfig` was configured for Kuzu:

   ```text
   Build("assignment provider Fjall does not match configured backend Kuzu")
   ```

3. Generalized the binding provider, built each Data node from its own `DataProcessConfig`,
   and added assertions for the selected provider, unique namespaces, endpoint call deltas,
   and immutable fragment fences.

## Implementation

- `static_shard_case(backend, runtime)` starts three same-provider Data nodes.
- Each node has its own business, Kuzu, and Raft directories. PostgreSQL alone receives an
  endpoint/credential profile.
- Each run creates unique logical namespaces, so a disposable PostgreSQL instance can run the
  ignored case repeatedly without retaining a previous Raft-applied index.
- Dataset seeding uses `DataRpcService::apply_transaction`; query execution uses the existing
  `GatewayProtocolV2Transport` plus `ShardRoutedGatewayTransport` and calls the real Data RPC
  service implementation.
- The returned case evidence records point, adjacency, and count endpoint deltas plus the
  captured request fences. Provider-specific tests assert `[0, 0, 1]`, `[0, 0, 1]`,
  `[1, 1, 1]`, and immutable snapshots.
- The PostgreSQL test uses a multi-thread Tokio runtime. This is necessary because Raft apply
  invokes the provider through a blocking bridge and PostgreSQL requires async I/O. The whole
  topology is bounded to 20 seconds; dropping the case drops/stops its Data nodes on timeout.

## PostgreSQL hang diagnosis

The initial ignored PostgreSQL attempt stalled with one idle database connection. The cause was
the current-thread Tokio test runtime: the Raft blocking apply path could not drive the
PostgreSQL provider's async I/O. Switching just the PostgreSQL test to a four-worker multi-thread
runtime made the same real Data RPC flow complete. A second issue appeared when reusing the same
temporary PostgreSQL instance: fixed logical namespaces retained the first run's applied index
while fresh Raft directories began at index zero. Unique per-case namespaces resolved that state
collision.

## Verification

Passed:

```text
cargo fmt --package dtg-gateway -- --check
git diff --check
cargo test --locked -p dtg-gateway --test four_process_cluster static_shard_snapshot_routes_fjall
cargo test --locked -p dtg-gateway --test four_process_cluster static_shard_snapshot_routes_kuzu
cargo test --locked -p dtg-gateway --test backend_e2e_diagnostic --no-run
```

The ignored PostgreSQL topology also passed against a disposable loopback PostgreSQL 17 instance
created with `initdb`/`pg_ctl` and removed by a shell trap:

```text
cargo test --locked -p dtg-gateway --test four_process_cluster \
  static_shard_snapshot_routes_postgresql -- --ignored

1 passed; 0 failed; 0 ignored
```

## Self-review

- The topology keeps one provider kind across all three nodes and asserts it.
- Storage, Kuzu, consensus, and PostgreSQL logical namespaces are independent per node/case.
- No mock bypass was added: all reads and writes retain the normal Gateway/Data/Raft/provider
  path and immutable `ReadFence` evidence.
- PostgreSQL remains ignored without explicitly supplied test connection data; its live result is
  recorded above only because the disposable verification completed and asserted successfully.
